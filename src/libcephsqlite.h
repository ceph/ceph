#ifndef LIBCEPHSQLITE_H
#define LIBCEPHSQLITE_H

#include "rados/librados.hpp"

struct sqlite3;

/* IMPORTANT
 * Remember to add the namespace name as a prefix to the actual database name
 * to uniquely identify the database within the cluster.
 * eg. "ACCOUNTS.receivable", "ACCOUNTS.payable", "INVENTORY.receivable", etc.
 * where ACCOUNTS is the namespace and receivable is the actual database name
 * The namespace prefix is required to uniquely identify the database by the
 * cephsqlite3 vfs due to the nature of the API which makes it impossible to
 * identify the namespace of the database when accessing the CephVFSContext
 * object for that datbase via the SQLite VFS methods.
 * The namespace prefix to the database name is required since the list of
 * CephVFSContext objects is global to the process and the presumption is that
 * a process may create smimilarly named databases in many namespaces.
 */
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

extern "C"
void ceph_sqlite3_set_db_page_size(sqlite3 *db, int page_size);

extern "C"
void ceph_sqlite3_dump_buffer_cache(sqlite3 *db, const char *file_name);

#endif
