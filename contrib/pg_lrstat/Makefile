# contrib/pg_lrstat/Makefile

MODULE_big = pg_lrstat
OBJS = \
  $(WIN32RES) \
  lrstat_remote.o \
  lrstat_shmem.o \
  lrstat_sql.o \
  lrstat_store.o \
  lrstat_worker.o \
  pg_lrstat.o
PGFILEDESC = "pg_lrstat - replication measurement sessions"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = pg_lrstat
DATA = pg_lrstat--2.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/pg_lrstat
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
