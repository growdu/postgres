# Copyright (c) 2026, PostgreSQL Global Development Group

pg_lrstat_sources = files(
  'lrstat_remote.c',
  'lrstat_shmem.c',
  'lrstat_sql.c',
  'lrstat_worker.c',
  'pg_lrstat.c',
)

if host_system == 'windows'
  pg_lrstat_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_lrstat',
    '--FILEDESC', 'pg_lrstat - logical replication LSN and rate statistics',])
endif

pg_lrstat = shared_module('pg_lrstat',
  pg_lrstat_sources,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [libpq],
  },
)
contrib_targets += pg_lrstat

install_data(
  'pg_lrstat.control',
  'pg_lrstat--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_lrstat',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_lrstat',
    ],
    'regress_args': ['--temp-config', files('pg_lrstat.conf')],
    # The tests require "shared_preload_libraries=pg_lrstat", which
    # typical runningcheck users do not have.
    'runningcheck': false,
  },
  'tap': {
    'tests': [
      't/001_inject.pl',
      't/002_replication.pl',
    ],
  },
}
