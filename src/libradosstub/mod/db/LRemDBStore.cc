#include <iostream>
#include <string>
#include <vector>

#include "SQLiteCpp/SQLiteCpp.h"
#include "SQLiteCpp/Exception.h"

#include "common/armor.h"
#include "common/ceph_time.h"
#include "common/debug.h"
#include "common/iso_8601.h"

#include "LRemDBStore.h"
#include "LRemDBCluster.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "LRemDBStore: " << this << " " << __func__ \
                           << ": "
#define dout_context g_ceph_context

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

template <class T>
string encode_base64(const T& t)
{
  bufferlist bl;
  ceph::encode(t, bl);

  char dst[bl.length() * 2];
  char *dend = dst + sizeof(dst);

  int r = ceph_armor(dst, dend, bl.c_str(), dend);
  assert (r >= 0);

  return string(dst);
}

template <class T>
int decode_base64(const string& s, T *t)
{
  int ssize = s.size();
  char dst[ssize];
  char *dend = dst + ssize;

  const char *src = s.c_str();
  const char *send = src + ssize;

  int r = ceph_unarmor(dst, dend, src, send);
  if (r < 0) {
    return r;
  }
  bufferptr bp(dst, ssize);
  bufferlist bl;
  bl.push_back(bp);

  try {
    ceph::decode(*t, bl);
  } catch (buffer::error& err) {
    return -EIO;
  }

  return 0;
}

namespace librados {

LRemDBOps::LRemDBOps(const string& name, int flags) {
  db = std::make_unique<SQLite::Database>(name, flags);
}

SQLite::Transaction LRemDBOps::new_transaction() {
  return SQLite::Transaction(*db);
}

SQLite::Statement LRemDBOps::statement(const string& sql) {
  dout(20) << "Statement: " << sql << dendl;
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
    dout(20) << "SQL: " << sql << dendl;
    db->exec(sql);
    /* return code is not interesting */
  } catch (SQLite::Exception& e) {
    std::cerr << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << std::endl;
    return -EIO;
  }
  return 0;
}

int LRemDBOps::exec(SQLite::Statement& stmt)
{
  int r;

  try {
    dout(20) << "SQL: " << stmt.getExpandedSQL() << dendl;
    r = stmt.exec();
    /* return code is not interesting */
  } catch (SQLite::Exception& e) {
    std::cerr << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << std::endl;
    return -EIO;
  }
  return r;
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
    auto q = dbo->statement("SELECT * from pools");

    while (q.executeStep()) {
      int         id      = q.getColumn(0);
      const char* name   = q.getColumn(1);
      const char* value   = q.getColumn(2);

      (*pools)[name] = std::make_shared<Pool>(dbo, id, name, value);
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  return 0;
}

int LRemDBStore::Cluster::get_pool(const string& name, PoolRef *pool) {
//ldout(g_ceph_context, 0) << __FILE__ << ":" << __LINE__ << ":" << __func__ << "()" << dendl;

  try {
    auto q = dbo->statement("SELECT * from pools WHERE name = ?");

    q.bind(1, name);

    if (q.executeStep()) {
      int         id      = q.getColumn(0);
      const char* name   = q.getColumn(1);
      const char* value   = q.getColumn(2);

      *pool = std::make_shared<Pool>(dbo, id, name, value);
      return id;
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  return -ENOENT;
}

int LRemDBStore::Cluster::get_pool(int id, PoolRef *pool) {
//ldout(g_ceph_context, 0) << __FILE__ << ":" << __LINE__ << ":" << __func__ << "()" << dendl;

  try {
    auto q = dbo->statement("SELECT * from pools WHERE id = ?");

    q.bind(1, id);

    if (q.executeStep()) {
      int         id      = q.getColumn(0);
      const char* name   = q.getColumn(1);
      const char* value   = q.getColumn(2);

      *pool = std::make_shared<Pool>(dbo, id, name, value);
      return id;
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
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
    auto q = dbo->statement("SELECT * from pools WHERE name = ?");

    q.bind(1, name);

    if (q.executeStep()) {
      id      = q.getColumn(0);
      value   = q.getColumn(2).getString();

      return id;
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  dout(20) << "pool " << name << " not found" << dendl;
  return -ENOENT;
}

int LRemDBStore::Pool::init_tables() {
  Obj obj_table(dbo, id);
  int r = obj_table.create_table();
  if (r < 0) {
    return r;
  }

  ObjData od_table(dbo, id);
  r = od_table.create_table();
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
                                                "epoch INTEGER",
                                                "PRIMARY KEY (nspace, oid)" } ));
  if (r < 0) {
    return r;
  }

  return 0;
}

void LRemDBStore::Obj::Meta::touch(uint64_t _epoch)
{
  epoch = _epoch;
  mtime = real_clock::now();
}

int LRemDBStore::Obj::read_meta(LRemDBStore::Obj::Meta *pmeta) {
  try {
    auto q = dbo->statement("SELECT size, mtime, objver, snap_id, snaps, snap_overlap, epoch from ? WHERE nspace = ? AND oid = ?");

    q.bind(1, table_name);
    q.bind(2, nspace);
    q.bind(3, oid);

    if (q.executeStep()) {
      pmeta->size = (long long)q.getColumn(0);
      string mtime_str = (const char *)q.getColumn(1);
      pmeta->mtime = ceph::from_iso_8601(mtime_str).value_or(ceph::real_time());
      pmeta->objver = (long long)q.getColumn(2);
      pmeta->snap_id = (long long)q.getColumn(3);
      string snaps_str = (const char *)q.getColumn(4);
      int r = decode_base64(snaps_str, &pmeta->snaps);
      if (r < 0) {
        dout(0) << "ERROR: failed to decode snaps for nspace=" << nspace << " oid=" << oid << dendl;
        return -EIO;
      }
      string snap_overlap_str = (const char *)q.getColumn(5);
      r = decode_base64(snap_overlap_str, &pmeta->snap_overlap);
      if (r < 0) {
        dout(0) << "ERROR: failed to decode snap_overlap for nspace=" << nspace << " oid=" << oid << dendl;
        return -EIO;
      }
      pmeta->epoch = (long long)q.getColumn(6);

      return 0;
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  return -ENOENT;
}

int LRemDBStore::Obj::write_meta(const LRemDBStore::Obj::Meta& meta) {
  auto q = dbo->statement("REPLACE INTO ? VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"); 

  q.bind(1, table_name);
  q.bind(2, nspace);
  q.bind(3, oid);

  q.bind(4, (long long)meta.size);
  q.bind(5, ceph::to_iso_8601(meta.mtime));
  q.bind(6, (long long)meta.objver);
  q.bind(7, (long long)meta.snap_id);
  q.bind(8, encode_base64(meta.snaps));
  q.bind(9, encode_base64(meta.snap_overlap));
  q.bind(10, (long long)meta.epoch);

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::Obj::read_data(uint64_t ofs, uint64_t len,
                                bufferlist *bl) {

  auto q = dbo->statement("SELECT size from ? WHERE nspace = ? AND oid = ?");

  q.bind(1, table_name);
  q.bind(2, nspace);
  q.bind(3, oid);

  if (!q.executeStep()) {
    return -ENOENT;
  }

  uint64_t size = (long long)q.getColumn(0);

  if (ofs >= size) {
    return 0;
  }

  if (ofs + len > size) {
    len = size - ofs;
  }

  if (len == 0) {
    len = size - ofs;
  }

  ObjData od(dbo, pool_id, nspace, oid);

  return od.read(ofs, len, bl);
}

int LRemDBStore::Obj::write_data(uint64_t ofs, uint64_t len,
                                 const bufferlist& bl) {

  ObjData od(dbo, pool_id, nspace, oid);

  int r = od.write(ofs, len, bl);
  if (r < 0) {
    return r;
  }
#if 0
  uint64_t size = ofs + len;
#warning update epoch, mtime
  auto q = dbo->statement("INSERT INTO ?(nspace, oid, size) VALUES (?, ?, ?)"
                          " ON CONFLICT(nspace, oid) DO UPDATE SET size=?"
                          " WHERE size < ?");

  q.bind(1, table_name);
  q.bind(2, nspace);
  q.bind(3, oid);
  q.bind(4, (long long)size);
  q.bind(5, (long long)size);

  r = dbo->exec(q);
  if (r < 0) {
    return r;
  }
#endif

  return 0;
}

int LRemDBStore::Obj::write(uint64_t ofs, uint64_t len,
                            const bufferlist& bl,
                            uint64_t epoch) {

  LRemDBStore::Obj::Meta meta;
  int r = read_meta(&meta);
  if (r < 0) {
    return r;
  }

  return write(ofs, len, bl, epoch);
}

int LRemDBStore::Obj::write(uint64_t ofs, uint64_t len,
                            const bufferlist& bl,
                            LRemDBStore::Obj::Meta& meta,
                            uint64_t epoch) {
  int r = write_data(ofs, len, bl);
  if (r < 0) {
    return r;
  }

  uint64_t size = ofs + r;
  if (size > meta.size) {
    meta.size = size;
  }

  meta.touch(epoch);

  return write_meta(meta);
}

int LRemDBStore::Obj::append(const bufferlist& bl,
                             uint64_t epoch) {

  LRemDBStore::Obj::Meta meta;
  int r = read_meta(&meta);
  if (r < 0) {
    return r;
  }

  return write(meta.size, bl.length(), bl, epoch);
}

int LRemDBStore::ObjData::create_table() {
  int r = dbo->create_table(table_name, join( { "nspace TEXT",
                                                "oid TEXT",
                                                "bid INTEGER",
                                                "data BLOB",
                                                "PRIMARY KEY (nspace, oid, bid)" } ));
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::ObjData::read_block(int bid, bufferlist *bl) {
  SQLite::Statement q = dbo->statement("SELECT data from ? WHERE nspace = ? AND oid = ? AND bid == ?");

  q.bind(1, table_name);
  q.bind(2, nspace);
  q.bind(3, oid);
  q.bind(4, bid);

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  if (!q.executeStep()) {
    return -ENOENT;
  }

  auto blob_col = q.getColumn(1);

  const char *data = (const char *)blob_col.getBlob();
  size_t len = blob_col.getBytes();

  bufferptr bp(data, len);
  bl->push_back(bp);

  return 0;
}

int LRemDBStore::ObjData::write_block(int bid, bufferlist& bl) {
  SQLite::Statement q = dbo->statement("REPLACE INTO ? VALUES ( ?, ?, ?, ?, ? )");

  q.bind(1, table_name);
  q.bind(2, nspace);
  q.bind(3, bid);
  q.bind(4, bl.c_str(), bl.length());

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::ObjData::read(uint64_t ofs, uint64_t len, bufferlist *bl) {
  int start_block = ofs / block_size;
  int end_block = (ofs + len - 1) / block_size;

  int cur_block = start_block;

  int block_ofs = ofs % block_size;

  while (cur_block <= end_block) {
    int block_len = std::min(block_size, block_ofs + (int)len);
    bufferlist bbl;
    int r = read_block(cur_block, &bbl);
    if (r == -ENOENT) {
      bl->append_zero(block_len);
    } else if (r < 0) {
      return r;
    }

    auto read_len = bbl.length();

    auto zero_len = block_len - read_len;

    bbl.splice(block_ofs, read_len - block_ofs, bl);

    if (zero_len) {
      bl->append_zero(zero_len);
    }

    block_ofs = 0;
    ++cur_block;
  }

  return bl->length();
}

int LRemDBStore::ObjData::write(uint64_t ofs, uint64_t len, const bufferlist& bl) {
  if (len > bl.length()) {
    len = bl.length();
  }
  uint64_t write_len = len;

  int start_block = ofs / block_size;
  int end_block = (ofs + len - 1) / block_size;

  int cur_block = start_block;

  int block_ofs = ofs % block_size;

  auto bliter = bl.cbegin();

  while (cur_block <= end_block) {
    int block_len = std::min((uint64_t)block_size, block_ofs + len);
    bufferlist bbl;
    int r = read_block(cur_block, &bbl);
    if (r < 0 && r != -ENOENT) {
      return r;
    }

    if ((uint64_t)block_len > bbl.length()) { /* pad with zeros to match new size */
      bbl.append_zero(block_len - bbl.length());
    }

    char *pdst = bbl.c_str();

    bliter.copy(block_len, pdst + block_ofs);
    bliter += block_ofs;

    bufferptr bp(pdst, block_len);
    bufferlist new_block;

    new_block.push_back(bp);

    r = write_block(cur_block, new_block);
    if (r < 0) {
      return r;
    }

    block_ofs = 0; /* next block starts from zero */
    len -= block_len;
    ++cur_block;
  }

  return (int)write_len;
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
