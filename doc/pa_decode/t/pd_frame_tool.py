#!/usr/bin/env python3
"""Frame level dump and diff tool for publisher-side parallel logical decoding.

The tool reads the trace files written by the publisher (see the
parallel_decode_trace_file GUC) and either dumps them or compares two of
them.

The comparison answers the question the parallel path has to pass: does it
produce the same bytes as the serial path?  Two differences are allowed and
are reported separately, because they are intentional:

  * the order of the frames may differ, because the parallel path may send a
    stream segment before the commit message of an older transaction;
  * the frame header LSNs may differ, because the parallel path clamps them
    to the release frontier.

Everything else is a defect.  The tool reports the first difference it
finds, with the frame number, the frame type and both int64 header values.

Usage:
    pd_frame_tool.py dump <tracefile> [--messages]
    pd_frame_tool.py diff <tracefile-a> <tracefile-b> [--messages]
"""

import argparse
import difflib
import hashlib
import struct
import sys

# The frame header of a replication frame:
#   'w' + dataStart(8) + walEnd(8) + sendtime(8) + payload
#   'k' + walEnd(8) + sendtime(8) + replyRequested(1)
#   'r' + walEnd(8) + sendtime(8) + replyRequested(1) + data
#   'c' + walEnd(8) + sendtime(8) + replyRequested(1) + data
FRAME_HEADER_LEN = {
    "w": 25,
    "k": 17,
    "r": 17,
    "c": 17,
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


def read_trace(path):
    """Read a trace file and return the list of frames it contains."""
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


def _need(buf, pos, n):
    if pos + n > len(buf):
        raise ValueError("short message")
    return pos + n


def parse_messages(payload):
    """Split the body of a 'w' frame into pgoutput messages.

    Returns a list of (tag, bytes) pairs.  A message that cannot be parsed
    is returned as a single opaque blob, so that the comparison never loses
    bytes.
    """
    body = payload[FRAME_HEADER_LEN["w"]:]
    messages = []
    pos = 0

    while pos < len(body):
        tag = chr(body[pos])
        start = pos
        pos += 1

        try:
            if tag in (MSG_BEGIN,):
                pos = _need(body, pos, 20)
            elif tag in (MSG_COMMIT,):
                pos = _need(body, pos, 24)
            elif tag in (MSG_BEGIN_PREPARE,):
                pos = _need(body, pos, 20)
                pos = _need(body, pos, 1)
                (glen,) = struct.unpack_from(">I", body, pos)
                pos = _need(body, pos + 4, glen)
            elif tag in (MSG_PREPARE, MSG_COMMIT_PREPARED, MSG_ROLLBACK_PREPARED,
                         MSG_STREAM_PREPARE):
                pos = _need(body, pos, 4)
                pos = _need(body, pos, 1)
                (glen,) = struct.unpack_from(">I", body, pos)
                pos = _need(body, pos + 4, glen)
            elif tag == MSG_ORIGIN:
                pos = _need(body, pos, 8)
                (nlen,) = struct.unpack_from(">I", body, pos)
                pos = _need(body, pos + 4, nlen)
            elif tag in (MSG_RELATION, MSG_TYPE, MSG_INSERT, MSG_UPDATE,
                         MSG_DELETE):
                (mlen,) = struct.unpack_from(">I", body, pos)
                pos = _need(body, pos + 4, mlen)
            elif tag == MSG_TRUNCATE:
                (nrelids,) = struct.unpack_from(">I", body, pos)
                pos = _need(body, pos + 4, 4 * nrelids)
            elif tag == MSG_MESSAGE:
                pos = _need(body, pos, 2)
                pos = _need(body, pos, body[pos - 1])
                (mlen,) = struct.unpack_from(">I", body, pos)
                pos = _need(body, pos + 4, mlen)
            elif tag == MSG_STREAM_START:
                pos = _need(body, pos, 5)
            elif tag == MSG_STREAM_STOP:
                pass
            elif tag == MSG_STREAM_COMMIT:
                pos = _need(body, pos, 33)
            elif tag == MSG_STREAM_ABORT:
                pos = _need(body, pos, 8)
                # The abort LSN and time are only present in protocol
                # version 2 and later; try to read them, and accept the
                # shorter form as well.
                if len(body) - pos >= 16:
                    pos = _need(body, pos, 16)
            else:
                raise ValueError("unknown message tag %r" % tag)
        except (ValueError, struct.error):
            # Fall back to an opaque blob for the rest of the frame.
            messages.append(("?", body[start:]))
            break

        messages.append((tag, body[start:pos]))

    return messages


def message_stream(frames):
    """The ordered list of pgoutput messages across all data frames."""
    stream = []
    for frame in frames:
        if frame.type != "w":
            continue
        for tag, raw in parse_messages(frame.payload):
            stream.append((tag, hashlib.sha256(raw).hexdigest()[:16], len(raw)))
    return stream


def cmd_dump(args):
    frames = read_trace(args.tracefile)
    print("%d frame(s) in %s" % (len(frames), args.tracefile))
    for frame in frames:
        print(frame)
        if args.messages and frame.type == "w":
            for tag, raw in parse_messages(frame.payload):
                print("    %s %d bytes" % (tag, len(raw)))
    return 0


def _first_difference(a, b):
    """Return the first index at which the two sequences differ."""
    sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag != "equal":
            return tag, i1, i2, j1, j2
    return None


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
        return 0

    keys_a = [f.key() for f in data_a]
    keys_b = [f.key() for f in data_b]

    diff = _first_difference(keys_a, keys_b)
    errors = []

    if diff is not None:
        tag, i1, i2, j1, j2 = diff
        print("frame sequence differs at %s: A[%d:%d] vs B[%d:%d]" %
              (tag, i1, i2, j1, j2))
        for f in data_a[i1:min(i2, i1 + 3)]:
            print("  A %s" % f)
        for f in data_b[j1:min(j2, j1 + 3)]:
            print("  B %s" % f)
        errors.append("frame sequence differs")
    else:
        print("frame sequence matches (%d frame(s))" % len(data_a))

    # Compare the header LSNs of the frames that matched.
    clamped = 0
    lsn_errors = []
    if diff is None:
        for fa, fb in zip(data_a, data_b):
            if fa.lsn1 == fb.lsn1 and fa.lsn2 == fb.lsn2:
                continue
            if fb.lsn1 > fa.lsn1 or fb.lsn2 > fa.lsn2:
                lsn_errors.append(
                    "frame #%d: B header LSN (%d, %d) is ahead of A (%d, %d)"
                    % (fa.index, fb.lsn1, fb.lsn2, fa.lsn1, fa.lsn2))
            else:
                clamped += 1
    for msg in lsn_errors[:10]:
        print("  LSN %s" % msg)
    if lsn_errors:
        errors.append("%d frame header LSN mismatch(es)" % len(lsn_errors))
    if clamped:
        print("%d frame header LSN(s) clamped (allowed)" % clamped)

    # Compare the message streams, which tolerates a different frame
    # boundary in the parallel path.
    if args.messages:
        msgs_a = message_stream(data_a)
        msgs_b = message_stream(data_b)
        msg_diff = _first_difference(msgs_a, msgs_b)
        if msg_diff is None:
            print("message stream matches (%d message(s))" % len(msgs_a))
        else:
            tag, i1, i2, j1, j2 = msg_diff
            print("message stream differs at %s: A[%d:%d] vs B[%d:%d]" %
                  (tag, i1, i2, j1, j2))
            print("  A %s" % (msgs_a[i1:i1 + 3],))
            print("  B %s" % (msgs_b[j1:j1 + 3],))
            if not errors:
                errors.append("message stream differs")

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
