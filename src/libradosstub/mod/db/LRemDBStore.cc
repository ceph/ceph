#include <iostream>
#include <string>
#include <vector>

#include "SQLiteCpp/SQLiteCpp.h"
#include "SQLiteCpp/Exception.h"

#include "LRemDBStore.h"
#include "LRemDBCluster.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "LRemDBStore: " << this << " " << __func__ \
                           << ": "
using namespace std;


static inline string quoted(const string& s) {
  return string("\"") + s + "\"";
}

static inline string join(std::vector<string> v, const string& sep = ", ")
{
  string result;
  for (auto& s : v) {
    if (!result.empty()) {
      result.append(sep);
    }
    result += s;
  }
  return result;
}

namespace librados {

LRemDBOps::LRemDBOps(const string& name, int flags) {
  db = std::make_unique<SQLite::Database>(name, flags);
}

SQLite::Transaction LRemDBOps::new_transaction() {
  return SQLite::Transaction(*db);
}

SQLite::Statement LRemDBOps::query(const string& sql) {
  cout << "Query: " << sql << std::endl;
  return SQLite::Statement(*db, sql);
}

void LRemDBOps::Transaction::commit() {
  trans.commit();
}

int LRemDBOps::create_table(const string& name, const string& defs)
{
  string s = string("CREATE TABLE ") + name + " (" + defs + ")"; 

  if (db->tableExists(name)) {
    return 0;
  }

  return exec(s);
}

int LRemDBOps::exec(const string& sql)
{
  try {
    cout << "SQL: " << sql << std::endl;
    db->exec(sql);
    /* return code is not interesting */
  } catch (SQLite::Exception& e) {
    std::cerr << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << std::endl;
    return -EIO;
  }
  return 0;
}

LRemDBStore::Cluster::Cluster(const string& cluster_name) {
  string dbname = string("cluster-") + cluster_name + ".db3";
  dbo = make_shared<LRemDBOps>(dbname, SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
}

int LRemDBStore::Cluster::init() {
  int r = dbo->create_table("pools", join( { "id INTEGER PRIMARY KEY",
                                           "name TEXT UNIQUE",
                                           "value TEXT" } ));
  if (r < 0) {
    return r;
  }

  return 0;
};

int LRemDBStore::Cluster::list_pools(std::map<string, PoolRef> *pools)
{
//ldout(g_ceph_context, 0) << __FILE__ << ":" << __LINE__ << ":" << __func__ << "()" << dendl;

  pools->clear();
  try {
    auto q = dbo->query("SELECT * from pools");

    while (q.executeStep()) {
      int         id      = q.getColumn(0);
      const char* name   = q.getColumn(1);
      const char* value   = q.getColumn(2);

      (*pools)[name] = std::make_shared<Pool>(dbo, id, name, value);
    }

  } catch (SQLite::Exception& e) {
    std::cout << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << std::endl;
    return -EIO;
  }

  return 0;
}

int LRemDBStore::Cluster::get_pool(const string& name, PoolRef *pool) {
//ldout(g_ceph_context, 0) << __FILE__ << ":" << __LINE__ << ":" << __func__ << "()" << dendl;

  try {
    auto q = dbo->query("SELECT * from pools WHERE name = ?");

    q.bind(1, name);

    if (q.executeStep()) {
      int         id      = q.getColumn(0);
      const char* name   = q.getColumn(1);
      const char* value   = q.getColumn(2);

      *pool = std::make_shared<Pool>(dbo, id, name, value);
      return id;
    }

  } catch (SQLite::Exception& e) {
    std::cout << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << std::endl;
    return -EIO;
  }

  return -ENOENT;
}

int LRemDBStore::Cluster::get_pool(int id, PoolRef *pool) {
//ldout(g_ceph_context, 0) << __FILE__ << ":" << __LINE__ << ":" << __func__ << "()" << dendl;

  try {
    auto q = dbo->query("SELECT * from pools WHERE id = ?");

    q.bind(1, id);

    if (q.executeStep()) {
      int         id      = q.getColumn(0);
      const char* name   = q.getColumn(1);
      const char* value   = q.getColumn(2);

      *pool = std::make_shared<Pool>(dbo, id, name, value);
      return id;
    }

  } catch (SQLite::Exception& e) {
    std::cout << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << std::endl;
    return -EIO;
  }

  return -ENOENT;
}

SQLite::Transaction LRemDBStore::Cluster::new_transaction() {
  return dbo->new_transaction();
}

int LRemDBStore::Cluster::create_pool(const string& name, const string& val) {
//ldout(g_ceph_context, 0) << __FILE__ << ":" << __LINE__ << ":" << __func__ << "()" << dendl;

  PoolRef pool = make_shared<Pool>(dbo);

  return pool->create(name, val);
}

int LRemDBStore::Pool::create(const string& _name, const string& _val) {
//ldout(g_ceph_context, 0) << __FILE__ << ":" << __LINE__ << ":" << __func__ << "()" << dendl;


  name = _name;
  value = _val;

  int r = dbo->exec(string("INSERT INTO pools VALUES (" + join( { "NULL", ::quoted(name), ::quoted(value) } ) + ")"));
  if (r < 0) {
    return r;
  }

  r = read();
  if (r < 0) {
    return r;
  }

  r = init_tables();
  if (r < 0) {
    return r;
  }

  return id;
}

int LRemDBStore::Pool::read() {
  try {
    auto q = dbo->query("SELECT * from pools WHERE name = ?");

    q.bind(1, name);

    if (q.executeStep()) {
      id      = q.getColumn(0);
      value   = q.getColumn(2).getString();

      return id;
    }

  } catch (SQLite::Exception& e) {
    std::cout << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << std::endl;
    return -EIO;
  }


  std::cout << "pool " << name << " not found" << std::endl;
  return -ENOENT;
}

int LRemDBStore::Pool::init_tables() {
  Obj obj_table(dbo, id);
  int r = obj_table.create_table();
  if (r < 0) {
    return r;
  }

  XAttrs xattrs_table(dbo, id);
  r = xattrs_table.create_table();
  if (r < 0) {
    return r;
  }

  OMap omap_table(dbo, id);
  r = omap_table.create_table();
  if (r < 0) {
    return r;
  }

  return 0;
}

LRemDBStore::ObjRef LRemDBStore::Pool::get_obj_handler(const std::string& nspace, const std::string& oid) {
  return std::make_shared<Obj>(dbo, id, nspace, oid);
}

LRemDBStore::XAttrsRef LRemDBStore::Pool::get_xattrs_handler(const std::string& nspace, const std::string& oid) {
  return std::make_shared<XAttrs>(dbo, id, nspace, oid);
}

LRemDBStore::OMapRef LRemDBStore::Pool::get_omap_handler(const std::string& nspace, const std::string& oid) {
  return std::make_shared<OMap>(dbo, id, nspace, oid);
}

void LRemDBStore::TableBase::init_table_name(const string& table_name_prefix) {
  char buf[32];
  snprintf(buf, sizeof(buf), "_%d", (int)pool_id);

  table_name = table_name_prefix + buf;
}

int LRemDBStore::Obj::create_table() {
  int r = dbo->create_table(table_name, join( { "nspace TEXT",
                                                "oid TEXT",
                                                "size INTEGER",
                                                "mtime TEXT",
                                                "objver INTEGER",
                                                "snap_id INTEGER",
                                                "snaps TEXT",
                                                "snap_overlap TEXT",
                                                "data BLOB",
                                                "PRIMARY KEY (nspace, oid)" } ));
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::KVTableBase::create_table() {
  int r = dbo->create_table(table_name, join( { "nspace TEXT",
                                                "oid TEXT",
                                                "key TEXT",
                                                "data BLOB",
                                                "PRIMARY KEY (nspace, oid)" } ));
  if (r < 0) {
    return r;
  }

  return 0;
}



}
