# src/test/modules/test_parallel_decode/Makefile

MODULE_big = test_parallel_decode
OBJS = \
	$(WIN32RES) \
	test_parallel_decode.o
PGFILEDESC = "test_parallel_decode - tests for parallel logical decoding infrastructure"

EXTENSION = test_parallel_decode
DATA = test_parallel_decode--1.0.sql

REGRESS = test_parallel_decode

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_parallel_decode
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
