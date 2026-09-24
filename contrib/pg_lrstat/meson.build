# Copyright (c) 2026, PostgreSQL Global Development Group

pg_lrstat_sources = files(
  'lrstat_remote.c',
  'lrstat_shmem.c',
  'lrstat_export.c',
  'lrstat_sql.c',
  'lrstat_store.c',
  'lrstat_worker.c',
  'pg_lrstat.c',
)

if host_system == 'windows'
  pg_lrstat_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_lrstat',
    '--FILEDESC', 'pg_lrstat - replication measurement sessions',])
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
  'pg_lrstat--2.0.sql',
  kwargs: contrib_data_args,
)
