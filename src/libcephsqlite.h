#ifndef LIBCEPHSQLITE_H
#define LIBCEPHSQLITE_H

#include "rados/librados.hpp"

struct sqlite3;

extern "C"
sqlite3 *ceph_sqlite3_open(
  librados::Rados &cluster,
  const char *dbname,           /* eg. "sql" instead of "sql.db" */
  const char *rados_namespace,
  int ceph_pool_id,
  bool must_create
);

extern "C"
void ceph_sqlite3_set_db_params(
  const char *dbname,           /* eg. "sql" instead of "sql.db" */
  int stripe_count,
  int obj_size
);

#endif
