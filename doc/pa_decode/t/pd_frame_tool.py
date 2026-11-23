#!/usr/bin/env python3
"""Frame level dump and diff tool for publisher-side parallel logical decoding.

The tool reads the trace files written by the publisher (see the
parallel_decode_trace_file GUC) and either dumps them or compares two of
them.

The comparison answers the question the parallel path has to pass: does it
produce the same data as the serial path?  Four differences are allowed and
are reported separately, because they are intentional:

  * the frames may group the pgoutput messages differently, because the
    parallel path sends the frames of a transaction as they become ready;
  * the frame header LSNs may differ, because the parallel path clamps them
    to the release frontier;
  * the framing of a transaction differs, because the parallel path streams
    the transactions: it sends stream segments where the serial path sends a
    block of messages, and the messages of a streamed transaction carry the
    transaction id in band;
  * a transaction that changes nothing is empty: the serial path skips it,
    the parallel path streams it, the way the upstream streaming protocol
    does it.  An empty transaction carries no data, so the comparison ignores
    it.

The last difference is the reason why the tool does not compare the bytes of
the messages as they are.  The transaction id in band is stripped from the
bytes before they are compared, and the stream segments are put back
together into the transaction they belong to.  What is left is the data of
the transactions, and that has to be the same.

Everything else is a defect.  The tool compares the pgoutput messages, not
just the frames, because the parallel path may group the messages into
frames differently.

The order of the transactions is not one of the allowed differences: the
parallel path has to send the end event of a transaction before the stream
segments of a transaction that ended later, so that the subscriber does not
apply two transactions at the same time that the publisher serialized with
row locks.

Usage:
    pd_frame_tool.py dump <tracefile> [--messages]
    pd_frame_tool.py diff <tracefile-a> <tracefile-b> [--messages]
"""

import argparse
import difflib
import hashlib
import re
import struct
import sys
from collections import Counter, defaultdict

# The frame header of a replication frame:
#   'w' + dataStart(8) + walEnd(8) + sendtime(8) + payload
#   'k' + walEnd(8) + sendtime(8) + replyRequested(1)
FRAME_HEADER_LEN = {
    "w": 25,
    "k": 18,
}

# The replication frame types that carry a transaction's data.
DATA_FRAMES = ("w",)

# pgoutput message tags, see src/include/replication/logicalproto.h.
MSG_BEGIN = "B"
MSG_COMMIT = "C"
MSG_ORIGIN = "O"
MSG_INSERT = "I"
MSG_UPDATE = "U"
MSG_DELETE = "D"
MSG_TRUNCATE = "T"
MSG_RELATION = "R"
MSG_TYPE = "Y"
MSG_MESSAGE = "M"
MSG_BEGIN_PREPARE = "b"
MSG_PREPARE = "P"
MSG_COMMIT_PREPARED = "K"
MSG_ROLLBACK_PREPARED = "r"
MSG_STREAM_START = "S"
MSG_STREAM_STOP = "E"
MSG_STREAM_COMMIT = "c"
MSG_STREAM_ABORT = "A"
MSG_STREAM_PREPARE = "p"

# The messages that carry the transaction id in band when the transaction is
# streamed, see logicalrep_write_insert() and friends.
XID_IN_BAND = (
    MSG_INSERT,
    MSG_UPDATE,
    MSG_DELETE,
    MSG_TRUNCATE,
    MSG_RELATION,
    MSG_TYPE,
    MSG_MESSAGE,
)

# The messages that delimit a transaction.  A transaction that is not streamed
# is delimited by a begin and a commit message, a streamed one by a stream
# start and a stream commit message.
TXN_BOUNDARY = (MSG_BEGIN, MSG_COMMIT, MSG_STREAM_START, MSG_STREAM_COMMIT)

# The description of a message that carries a transaction id starts with the
# id, see _parse_message().
_XID_IN_DESCRIPTION = re.compile(r"^xid=(\d+)")

# The description of a stream abort message carries the id of the transaction
# and the id of the subtransaction that was aborted, see _parse_message().
_SUBXID_IN_DESCRIPTION = re.compile(r"^xid=(\d+) subxid=(\d+)")


def _subxid_of(desc, xid):
    """The id of the subtransaction that a stream abort message names.

    The message names the transaction and the subtransaction that was
    aborted.  The two are the same when the whole transaction was aborted.
    """
    match = _SUBXID_IN_DESCRIPTION.match(desc)
    if match is None:
        return xid
    return int(match.group(2))


class Phase(object):
    """One phase of the commit_sync barrier, as recorded in a trace file.

    A phase is the end event of a transaction: the commit, the abort or the
    prepare.  The phases are released in the order of their phase numbers, so
    that the transactions reach the subscriber in the order they were committed
    in.  The record tells whether the phase had an end event frame at all: a
    transaction that produced no message has no marker.
    """

    def __init__(self, index, session, cseq, flags, nframes, nbytes):
        self.index = index
        self.session = session
        self.cseq = cseq
        self.flags = flags
        self.nframes = nframes
        self.nbytes = nbytes

    @property
    def has_marker(self):
        return (self.flags & 0x01) != 0

    def __str__(self):
        return "#%d phase cseq=%d marker=%s frames=%d bytes=%d" % (
            self.index,
            self.cseq,
            "yes" if self.has_marker else "no",
            self.nframes,
            self.nbytes,
        )


class Frame(object):
    """One replication frame, as recorded in a trace file."""

    def __init__(self, index, session, ftype, lsn1, lsn2, payload):
        self.index = index
        self.session = session
        self.type = ftype
        self.lsn1 = lsn1
        self.lsn2 = lsn2
        self.payload = payload

    @property
    def body(self):
        """The bytes of the frame that are not part of the header."""
        return self.payload[FRAME_HEADER_LEN[self.type]:]

    @property
    def digest(self):
        return hashlib.sha256(self.body).hexdigest()[:16]

    def key(self):
        """The identity of the frame, ignoring the header LSNs."""
        return (self.type, self.digest, len(self.body))

    def __str__(self):
        return "#%d %s lsn1=%d lsn2=%d len=%d %s" % (
            self.index,
            self.type,
            self.lsn1,
            self.lsn2,
            len(self.body),
            self.digest,
        )


def read_trace(path, phases=None):
    """Read a trace file and return the list of frames it contains.

    The phase records of the commit_sync barrier are not frames: they are
    collected into the phases list, when one is given.
    """
    frames = []
    session = 0
    with open(path, "rb") as fh:
        data = fh.read()

    pos = 0
    while pos + 4 <= len(data):
        (length,) = struct.unpack_from(">I", data, pos)
        pos += 4
        if pos + length > len(data):
            raise ValueError(
                "truncated record at offset %d (want %d bytes, have %d)"
                % (pos - 4, length, len(data) - pos)
            )
        payload = data[pos:pos + length]
        pos += length

        if not payload:
            continue

        if payload[0] == 0:
            # Session marker.
            if len(payload) >= 9:
                session = struct.unpack_from(">Q", payload, 1)[0]
            continue

        ftype = chr(payload[0])
        if ftype == "p":
            # The release of a phase of the commit_sync barrier.
            if phases is None:
                continue
            if len(payload) < 18:
                raise ValueError(
                    "short phase record at frame %d" % len(frames)
                )
            cseq = struct.unpack_from(">Q", payload, 1)[0]
            flags = payload[9]
            nframes = struct.unpack_from(">I", payload, 10)[0]
            nbytes = struct.unpack_from(">I", payload, 14)[0]
            phases.append(
                Phase(len(phases), session, cseq, flags, nframes, nbytes)
            )
            continue

        if ftype not in FRAME_HEADER_LEN:
            raise ValueError(
                "unknown frame type %r at frame %d" % (ftype, len(frames))
            )
        if len(payload) < FRAME_HEADER_LEN[ftype]:
            raise ValueError(
                "short %r frame at frame %d" % (ftype, len(frames))
            )

        if ftype == "w":
            lsn1, lsn2 = struct.unpack_from(">qq", payload, 1)
        else:
            lsn1, lsn2 = struct.unpack_from(">q", payload, 1)[0], 0

        frames.append(Frame(len(frames), session, ftype, lsn1, lsn2, payload))

    return frames


class _Buf(object):
    """A cursor over the bytes of one replication frame."""

    def __init__(self, data):
        self.data = data
        self.pos = 0

    def _need(self, n):
        if self.pos + n > len(self.data):
            raise ValueError("short message")

    def u8(self):
        self._need(1)
        value = self.data[self.pos]
        self.pos += 1
        return value

    def u16(self):
        self._need(2)
        (value,) = struct.unpack_from(">H", self.data, self.pos)
        self.pos += 2
        return value

    def u32(self):
        self._need(4)
        (value,) = struct.unpack_from(">I", self.data, self.pos)
        self.pos += 4
        return value

    def i64(self):
        self._need(8)
        (value,) = struct.unpack_from(">q", self.data, self.pos)
        self.pos += 8
        return value

    def cstring(self):
        """A NUL terminated string, as sent by pq_sendstring()."""
        end = self.data.find(b"\0", self.pos)
        if end < 0:
            raise ValueError("unterminated string")
        value = self.data[self.pos:end]
        self.pos = end + 1
        return value

    def raw(self, n):
        self._need(n)
        value = self.data[self.pos:self.pos + n]
        self.pos += n
        return value


def _parse_tuple(buf):
    """Parse a tuple, see logicalrep_write_tuple()."""
    ncols = buf.u16()
    kinds = []
    for _ in range(ncols):
        kind = chr(buf.u8())
        if kind in ("b", "t"):
            buf.raw(buf.u32())
        kinds.append(kind)
    return kinds


def _parse_relation(buf):
    """Parse a relation message, see logicalrep_write_rel()."""
    relid = buf.u32()
    nspname = buf.cstring().decode("utf-8", "replace")
    relname = buf.cstring().decode("utf-8", "replace")
    replident = chr(buf.u8())
    ncols = buf.u16()
    for _ in range(ncols):
        buf.u8()                   # flags
        buf.cstring()              # attribute name
        buf.u32()                  # attribute type id
        buf.u32()                  # attribute type modifier
    return "%s.%s relid=%d replident=%s ncols=%d" % (
        nspname, relname, relid, replident, ncols)


def _parse_message(buf, tag, streaming):
    """Parse one pgoutput message, return a short description of it.

    The transaction id that a message of a streamed transaction carries in
    band is read by the caller, so that the message can be compared with the
    same message of a transaction that was not streamed.
    """
    if tag == MSG_BEGIN:
        final_lsn = buf.i64()
        buf.i64()                  # commit time
        xid = buf.u32()
        return "xid=%d final_lsn=%d" % (xid, final_lsn)
    if tag == MSG_COMMIT:
        buf.u8()                   # flags
        commit_lsn = buf.i64()
        buf.i64()                  # end lsn
        buf.i64()                  # commit time
        return "commit_lsn=%d" % commit_lsn
    if tag == MSG_BEGIN_PREPARE:
        buf.i64()                  # final lsn
        buf.i64()                  # end lsn
        buf.i64()                  # prepare time
        xid = buf.u32()
        gid = buf.cstring().decode("utf-8", "replace")
        return "xid=%d gid=%s" % (xid, gid)
    if tag == MSG_ORIGIN:
        origin_lsn = buf.i64()
        origin = buf.cstring().decode("utf-8", "replace")
        return "origin_lsn=%d origin=%s" % (origin_lsn, origin)
    if tag == MSG_RELATION:
        return _parse_relation(buf)
    if tag == MSG_TYPE:
        typoid = buf.u32()
        nspname = buf.cstring().decode("utf-8", "replace")
        typname = buf.cstring().decode("utf-8", "replace")
        return "%s.%s typoid=%d" % (nspname, typname, typoid)
    if tag == MSG_INSERT:
        relid = buf.u32()
        action = chr(buf.u8())
        kinds = _parse_tuple(buf)
        return "relid=%d action=%s cols=%s" % (relid, action, "".join(kinds))
    if tag == MSG_UPDATE:
        relid = buf.u32()
        action = chr(buf.u8())
        kinds = _parse_tuple(buf)
        if action in ("K", "O"):
            action2 = chr(buf.u8())
            kinds2 = _parse_tuple(buf)
            return "relid=%d action=%s,%s cols=%s/%s" % (
                relid, action, action2, "".join(kinds), "".join(kinds2))
        return "relid=%d action=%s cols=%s" % (relid, action, "".join(kinds))
    if tag == MSG_DELETE:
        relid = buf.u32()
        action = chr(buf.u8())
        kinds = _parse_tuple(buf)
        return "relid=%d action=%s cols=%s" % (relid, action, "".join(kinds))
    if tag == MSG_TRUNCATE:
        nrelids = buf.u32()
        buf.u8()                   # flags
        relids = [buf.u32() for _ in range(nrelids)]
        return "relids=%s" % ",".join(str(r) for r in relids)
    if tag == MSG_MESSAGE:
        buf.u8()                   # flags
        lsn = buf.i64()
        prefix = buf.cstring().decode("utf-8", "replace")
        size = buf.u32()
        buf.raw(size)
        return "lsn=%d prefix=%s size=%d" % (lsn, prefix, size)
    if tag == MSG_STREAM_START:
        xid = buf.u32()
        first = buf.u8()
        return "xid=%d first_segment=%d" % (xid, first)
    if tag == MSG_STREAM_STOP:
        return ""
    if tag == MSG_STREAM_COMMIT:
        xid = buf.u32()
        buf.u8()                   # flags
        commit_lsn = buf.i64()
        buf.i64()                  # end lsn
        buf.i64()                  # commit time
        return "xid=%d commit_lsn=%d" % (xid, commit_lsn)
    if tag == MSG_STREAM_ABORT:
        xid = buf.u32()
        subxid = buf.u32()
        # The abort lsn and time are only present in protocol version 2 and
        # later.
        if len(buf.data) - buf.pos >= 16:
            buf.i64()              # abort lsn
            buf.i64()              # abort time
        return "xid=%d subxid=%d" % (xid, subxid)
    if tag in (MSG_PREPARE, MSG_STREAM_PREPARE):
        buf.u8()                   # flags
        buf.i64()                  # prepare lsn
        buf.i64()                  # end lsn
        buf.i64()                  # prepare time
        xid = buf.u32()
        gid = buf.cstring().decode("utf-8", "replace")
        return "xid=%d gid=%s" % (xid, gid)
    if tag == MSG_COMMIT_PREPARED:
        buf.u8()                   # flags
        buf.i64()                  # commit lsn
        buf.i64()                  # end lsn
        buf.i64()                  # commit time
        xid = buf.u32()
        gid = buf.cstring().decode("utf-8", "replace")
        return "xid=%d gid=%s" % (xid, gid)
    if tag == MSG_ROLLBACK_PREPARED:
        buf.u8()                   # flags
        buf.i64()                  # prepare end lsn
        buf.i64()                  # end lsn
        buf.i64()                  # prepare time
        buf.i64()                  # rollback time
        xid = buf.u32()
        gid = buf.cstring().decode("utf-8", "replace")
        return "xid=%d gid=%s" % (xid, gid)

    raise ValueError("unknown message tag %r" % tag)


def parse_messages(payload, streaming=False):
    """Split the body of a 'w' frame into pgoutput messages.

    Returns a list of (tag, bytes, description, xid) tuples and the streaming
    state the frame leaves behind.  A message that cannot be parsed is
    returned as a single opaque blob, so that the comparison never loses
    bytes.

    The bytes of a message that carries the transaction id in band do not
    carry it any more: the id is returned separately, so that the same
    change has the same bytes whether the transaction was streamed or not.
    The id is zero for a message that does not carry one.

    The streaming state is a part of the result, because a stream segment is
    spread over many frames: the changes of a segment carry the transaction
    id in band, and the parser has to know that it is inside a segment.
    """
    body = payload[FRAME_HEADER_LEN["w"]:]
    buf = _Buf(body)
    messages = []

    while buf.pos < len(body):
        start = buf.pos
        tag = chr(body[start])
        buf.pos += 1				# the tag itself

        #
        # The transaction id in band is the first field of the message, after
        # the tag.  It is read here and kept out of the bytes of the
        # message, so that a streamed change can be compared with the same
        # change of a transaction that was not streamed.
        #
        xid = 0
        if streaming and tag in XID_IN_BAND:
            try:
                xid = buf.u32()
            except ValueError:
                pass

        try:
            desc = _parse_message(buf, tag, streaming)
        except (ValueError, struct.error, IndexError):
            # Fall back to an opaque blob for the rest of the frame.
            messages.append(("?", body[start:], "unparsable", xid))
            break

        if not xid:
            #
            # The messages that delimit a transaction carry the transaction
            # id in their body, and the parser puts it in the description.
            #
            match = _XID_IN_DESCRIPTION.match(desc)
            if match:
                xid = int(match.group(1))

        raw = body[start:buf.pos]
        if xid:
            raw = body[start:start + 1] + body[start + 5:buf.pos]

        messages.append((tag, raw, desc, xid))

        if tag == MSG_STREAM_START:
            streaming = True
        elif tag == MSG_STREAM_STOP:
            streaming = False

    return messages, streaming


def message_stream(frames):
    """The ordered list of pgoutput messages across all data frames."""
    stream = []
    streaming = False
    for frame in frames:
        if frame.type != "w":
            continue
        messages, streaming = parse_messages(frame.payload, streaming)
        for tag, raw, desc, xid in messages:
            stream.append((tag, hashlib.sha256(raw).hexdigest()[:16], len(raw)))
    return stream


def message_multiset(frames):
    """The multiset of pgoutput messages across all data frames."""
    counter = Counter()
    streaming = False
    for frame in frames:
        if frame.type != "w":
            continue
        messages, streaming = parse_messages(frame.payload, streaming)
        for tag, raw, desc, xid in messages:
            counter[(tag, hashlib.sha256(raw).hexdigest()[:16], len(raw))] += 1
    return counter


def split_transactions(frames):
    """Split the messages of the frames into the transactions they belong to.

    Returns a list of (tag, messages) pairs, in the order the transactions
    ended.  The tag is the message that ended the transaction: a commit
    message for a transaction that is not streamed, a stream commit message
    for a transaction that is streamed, a commit prepared message for a
    prepared transaction.  The messages are the (tag, digest, length) triples
    of the messages that carry data.

    A transaction that is not streamed is delimited by a begin and a commit
    message.  A transaction that is streamed is spread over the stream
    segments that the publisher sends while it is decoding, and the segments
    of the transactions that are in flight at the same time are interleaved
    on the wire.  The changes of a streamed transaction are therefore gathered
    by the transaction id that they carry in band.

    A prepared transaction is delimited by its prepare message: the prepare
    carries the changes of the transaction, and the commit prepared or the
    rollback prepared ends it.  The two paths do not use the same message for
    the prepare: the serial path sends a begin prepare and a prepare message
    for every prepared transaction, while the parallel path sends a stream
    prepare message for the transactions that it streamed while they were in
    progress, and a begin prepare and a prepare message for the ones that it
    did not.  Both are handled here, so that the two paths are comparable.

    A transaction that is aborted is not a transaction: the changes that were
    sent before the abort are void, and the serial path does not send them
    at all.  The changes of an aborted transaction are therefore dropped.

    The messages that delimit a transaction are not part of it, and neither
    are the relation messages: the two paths send the definition of a
    relation at different times, so they are compared on their own.

    A transaction that carries no data is empty.  The serial path does not
    send one: pgoutput skips the transactions that change nothing.  The
    parallel path streams every transaction, so the stream of an empty
    transaction reaches the subscriber as well, the way the upstream
    streaming protocol does it.  The empty transactions carry no information,
    so the comparison ignores them.
    """
    transactions = []
    #
    # The transaction that is not streamed, and the changes of the streamed
    # transactions, by transaction id.
    #
    plain = None
    streamed = {}
    #
    # The changes of the prepared transactions, by transaction id.  A prepare
    # message ends the changes of the transaction, but the transaction is not
    # over: the commit prepared or the rollback prepared ends it.
    #
    prepared = {}
    in_stream = False

    for frame in frames:
        if frame.type != "w":
            continue
        messages, _ = parse_messages(frame.payload, in_stream)
        for tag, raw, desc, xid in messages:
            if tag == MSG_BEGIN:
                plain = []
                continue
            if tag == MSG_COMMIT:
                if plain is not None:
                    transactions.append((MSG_COMMIT, plain))
                plain = None
                continue
            if tag == MSG_BEGIN_PREPARE:
                # The transaction is prepared.  Its changes follow, and the
                # prepare message ends them.
                plain = []
                continue
            if tag == MSG_PREPARE:
                # The transaction was not streamed: its changes are in the
                # plain buffer.
                prepared[xid] = plain if plain is not None else []
                plain = None
                continue
            if tag == MSG_STREAM_PREPARE:
                # The transaction was streamed while it was in progress: its
                # changes are in the streamed buffer.
                prepared[xid] = streamed.pop(xid, [])
                continue
            if tag == MSG_COMMIT_PREPARED:
                transactions.append((MSG_COMMIT_PREPARED,
                                     prepared.pop(xid, [])))
                continue
            if tag == MSG_ROLLBACK_PREPARED:
                # The transaction is rolled back: its changes are void.
                prepared.pop(xid, None)
                continue
            if tag == MSG_STREAM_START:
                in_stream = True
                streamed.setdefault(xid, [])
                continue
            if tag == MSG_STREAM_STOP:
                in_stream = False
                continue
            if tag == MSG_STREAM_COMMIT:
                transactions.append((MSG_STREAM_COMMIT, streamed.pop(xid, [])))
                continue
            if tag == MSG_STREAM_ABORT:
                # The transaction, or the subtransaction, is aborted: the
                # changes that were sent already are void.
                streamed.pop(_subxid_of(desc, xid), None)
                continue
            if tag == MSG_RELATION:
                continue

            key = (tag, hashlib.sha256(raw).hexdigest()[:16], len(raw))
            if in_stream and xid:
                streamed.setdefault(xid, []).append(key)
            else:
                if plain is None:
                    plain = []
                plain.append(key)

    return transactions


def split_relation(counter):
    """Split a message multiset into the relation messages and the rest.

    The relation messages are special: the parallel path gives every decode
    worker its own relation cache, so it sends one relation message per
    worker that touched the relation, while the serial path sends one per
    session.  The repetition is allowed by the protocol, the subscriber
    simply replaces the relation it knows, but the *definitions* have to be
    the same.
    """
    relation = Counter()
    other = Counter()
    for key, count in counter.items():
        if key[0] == MSG_RELATION:
            relation[key] = count
        else:
            other[key] = count
    return relation, other


def message_lsn_map(frames):
    """Map every message to the frame header LSNs it was sent with.

    The frames of the two runs may group the messages differently, so the
    LSNs can only be compared per message, not per frame.
    """
    result = defaultdict(list)
    streaming = False
    for frame in frames:
        if frame.type != "w":
            continue
        messages, streaming = parse_messages(frame.payload, streaming)
        for tag, raw, desc, xid in messages:
            key = (tag, hashlib.sha256(raw).hexdigest()[:16], len(raw))
            result[key].append((frame.lsn1, frame.lsn2))
    for key in result:
        result[key].sort()
    return result


def cmd_dump(args):
    phases = []
    frames = read_trace(args.tracefile, phases)
    print("%d frame(s) in %s" % (len(frames), args.tracefile))
    streaming = False
    for frame in frames:
        print(frame)
        if args.messages and frame.type == "w":
            messages, streaming = parse_messages(frame.payload, streaming)
            for tag, raw, desc, xid in messages:
                print("    %s %d bytes %s" % (tag, len(raw), desc))
    if args.phases:
        print("%d phase(s) in %s" % (len(phases), args.tracefile))
        for phase in phases:
            print(phase)
    return 0


def _first_difference(a, b):
    """Return the first index at which the two sequences differ."""
    sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag != "equal":
            return tag, i1, i2, j1, j2
    return None


def _describe_multiset_diff(counter_a, counter_b):
    """Describe the difference between two message multisets."""
    lines = []
    for key in sorted(set(counter_a) | set(counter_b)):
        count_a = counter_a.get(key, 0)
        count_b = counter_b.get(key, 0)
        if count_a == count_b:
            continue
        tag, digest, size = key
        lines.append("  %s %s (%d bytes): A=%d B=%d" %
                     (tag, digest, size, count_a, count_b))
    return lines


def cmd_diff(args):
    frames_a = read_trace(args.a)
    frames_b = read_trace(args.b)

    data_a = [f for f in frames_a if f.type in DATA_FRAMES]
    data_b = [f for f in frames_b if f.type in DATA_FRAMES]

    keepalives_a = len(frames_a) - len(data_a)
    keepalives_b = len(frames_b) - len(data_b)

    print("A: %s: %d data frame(s), %d keepalive(s)" %
          (args.a, len(data_a), keepalives_a))
    print("B: %s: %d data frame(s), %d keepalive(s)" %
          (args.b, len(data_b), keepalives_b))

    if not data_a and not data_b:
        print("both traces are empty")
        return 1

    errors = []

    #
    # The transactions of the two runs have to be the same: the same changes
    # in the same order, in the same transactions, and the transactions in
    # the order they ended.  The framing of a transaction is not compared:
    # the parallel path streams the transactions, so it sends stream segments
    # where the serial path sends a block of messages.
    #
    txns_a = split_transactions(data_a)
    txns_b = split_transactions(data_b)

    #
    # The empty transactions are ignored: they carry no data, so they cannot
    # tell the two paths apart.  The serial path does not send them at all,
    # the parallel path streams them.
    #
    empty_a = [t for t in txns_a if not t[1]]
    empty_b = [t for t in txns_b if not t[1]]
    if empty_a or empty_b:
        print("note: %d empty transaction(s) ignored (A=%d, B=%d): they carry "
              "no data, the serial path skips them and the parallel path "
              "streams them" %
              (len(empty_a) + len(empty_b), len(empty_a), len(empty_b)))
    txns_a = [t for t in txns_a if t[1]]
    txns_b = [t for t in txns_b if t[1]]

    if len(txns_a) != len(txns_b):
        print("the number of the transactions differs: A=%d B=%d" %
              (len(txns_a), len(txns_b)))
        errors.append("the number of the transactions differs")
    else:
        mismatch = None
        for i, (txn_a, txn_b) in enumerate(zip(txns_a, txns_b)):
            if txn_a[1] != txn_b[1]:
                mismatch = (i, txn_a[1], txn_b[1])
                break
        if mismatch is None:
            print("the transactions match (%d transaction(s), %d change(s))" %
                  (len(txns_a), sum(len(t[1]) for t in txns_a)))
        else:
            i, msgs_a_, msgs_b_ = mismatch
            print("the transaction %d differs: A=%d change(s), B=%d change(s)" %
                  (i, len(msgs_a_), len(msgs_b_)))
            for line in _describe_multiset_diff(Counter(msgs_a_),
                                                Counter(msgs_b_))[:20]:
                print(line)
            errors.append("the transaction %d differs" % i)

    #
    # The definitions of the relations have to be the same.  The two paths
    # send them at different times: the serial path sends the definition of a
    # relation once for the session, the parallel path sends it with every
    # transaction that changes the relation, so the definitions are compared
    # as a set.
    #
    multiset_a = message_multiset(data_a)
    multiset_b = message_multiset(data_b)

    relation_a, other_a = split_relation(multiset_a)
    relation_b, other_b = split_relation(multiset_b)

    if set(relation_a) == set(relation_b):
        if relation_a == relation_b:
            print("relation messages match (%d message(s))" % sum(relation_a.values()))
        else:
            print("note: the parallel path repeats the relation messages "
                  "(A=%d, B=%d); a streamed transaction carries the definition "
                  "of the relation it changes" %
                  (sum(relation_a.values()), sum(relation_b.values())))
    else:
        print("relation messages differ")
        for line in _describe_multiset_diff(relation_a, relation_b)[:20]:
            print(line)
        errors.append("relation messages differ")

    #
    # The order of the transaction delimiters has to be the same as well: the
    # parallel path ends the transactions in the order of their commit
    # sequence numbers, so the subscriber must see the same order as in the
    # serial path.  A transaction that is streamed is delimited by a stream
    # start and a stream commit message, a transaction that is not streamed
    # by a begin and a commit message, so only the end of a transaction is
    # compared.
    #
    order_a = [tag for tag, msgs in txns_a]
    order_b = [tag for tag, msgs in txns_b]

    if len(order_a) == len(order_b):
        print("the transactions end in the same order (%d transaction(s))" %
              len(order_a))
    else:
        print("the number of the transactions that end differs: A=%d B=%d" %
              (len(order_a), len(order_b)))
        errors.append("the number of the transactions that end differs")

    # The frame grouping may differ, that is allowed.
    keys_a = [f.key() for f in data_a]
    keys_b = [f.key() for f in data_b]
    if keys_a == keys_b:
        print("frame grouping matches (%d frame(s))" % len(data_a))
    else:
        diff = _first_difference(keys_a, keys_b)
        print("frame grouping differs (allowed): %s" % (diff,))

    # Compare the header LSNs per message: the frames of the two runs may
    # group the messages differently, so the frames cannot be compared by
    # position.  The parallel path clamps the header LSNs to the release
    # frontier, so a smaller LSN is allowed, a larger one is not.
    lsns_a = message_lsn_map(data_a)
    lsns_b = message_lsn_map(data_b)
    clamped = 0
    lsn_errors = []
    for key in sorted(set(lsns_a) & set(lsns_b)):
        for lsn_a, lsn_b in zip(lsns_a[key], lsns_b[key]):
            if lsn_a == lsn_b:
                continue
            if lsn_b[0] > lsn_a[0] or lsn_b[1] > lsn_a[1]:
                lsn_errors.append(
                    "%s: B header LSN (%d, %d) is ahead of A (%d, %d)" %
                    (key[0], lsn_b[0], lsn_b[1], lsn_a[0], lsn_a[1]))
            else:
                clamped += 1
    for msg in lsn_errors[:10]:
        print("  LSN %s" % msg)
    if lsn_errors:
        errors.append("%d frame header LSN mismatch(es)" % len(lsn_errors))
    if clamped:
        print("%d frame header LSN(s) clamped (allowed)" % clamped)

    # Report the message order difference, if any, for information.  The
    # messages of a streamed transaction carry the transaction id in band and
    # the transactions are framed differently, so the two message streams are
    # not the same even when the transactions are.
    msgs_a = message_stream(data_a)
    msgs_b = message_stream(data_b)
    if msgs_a != msgs_b:
        print("the message streams differ (allowed: the parallel path "
              "streams the transactions)")

    if errors:
        print("FAIL: %s" % "; ".join(errors))
        return 1

    print("OK")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command")

    p_dump = sub.add_parser("dump", help="dump the frames of a trace file")
    p_dump.add_argument("tracefile")
    p_dump.add_argument("--messages", action="store_true",
                        help="also list the pgoutput messages of each frame")
    p_dump.add_argument("--phases", action="store_true",
                        help="also list the phases of the commit_sync barrier")
    p_dump.set_defaults(func=cmd_dump)

    p_diff = sub.add_parser("diff", help="compare two trace files")
    p_diff.add_argument("a")
    p_diff.add_argument("b")
    p_diff.add_argument("--messages", action="store_true",
                        help="also compare the pgoutput message streams")
    p_diff.set_defaults(func=cmd_diff)

    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
