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
static inline string join_quoted(const T& v, const string& sep = ", ")
{
  string result;
  for (auto& s : v) {
    if (!result.empty()) {
      result.append(sep);
    }
    result += quoted(s);
  }
  return result;
}

string sprintf_int(const char *format, int val)
{
  char buf[32];
  snprintf(buf, sizeof(buf), format, val);
  return string(buf);
}

template <class T>
string encode_base64(const T& t)
{
  bufferlist bl;
  ceph::encode(t, bl);

  char dst[bl.length() * 2 + 16];
  char *dend = dst + sizeof(dst);

  char *src = bl.c_str();
  char *send = src + bl.length();

  int r = ceph_armor(dst, dend, src, send);
  assert (r >= 0);

  dst[r] = '\0';

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
  bufferptr bp(dst, r);
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
#define DB_TIMEOUT_SEC 20
  db = std::make_unique<SQLite::Database>(name, flags, DB_TIMEOUT_SEC * 1000);
}

LRemDBOps::Transaction LRemDBOps::new_transaction() {
  return Transaction(*this);
}

SQLite::Statement LRemDBOps::statement(const string& sql) {
  dout(20) << "Statement: " << sql << dendl;
  return SQLite::Statement(*db, sql);
}

void LRemDBOps::Transaction::complete_op(int r) {
  if (r < 0) {
    retcode = r;
  }
}

#warning remove me
map<void *, int> m;
ceph::mutex mlock = ceph::make_mutex("LRemDBCluster::m_lock");

LRemDBOps::Transaction::Transaction(LRemDBOps& dbo) {
  std::unique_lock locker{mlock};
  p = (void *)&dbo;
  int count = ++m[p];
  dout(20) << "LRemDBOps::Transaction() dbo=" << (void *)&dbo << " db=" << &dbo.get_db() << " count=" << count << dendl;
  assert(count == 1);

  trans = make_unique<SQLite::Transaction>(dbo.get_db());
}

LRemDBOps::Transaction::~Transaction() {
  std::unique_lock locker{mlock};
  --m[p];
  dout(20) << "LRemDBOps::~Transaction() dbo=" << p << " count=" << m[p] << dendl;
  if (retcode >= 0) {
    trans->commit();
  }
  trans.reset();
  dout(20) << "LRemDBOps::~Transaction() dbo=" << p << " count=" << m[p] << dendl;
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

  bool retry;

  do {

    try {
      retry = false;
      dout(20) << "SQL: " << stmt.getExpandedSQL() << dendl;
      r = stmt.exec();
      /* return code is not interesting */
    } catch (SQLite::Exception& e) {
      dout(0) << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
      if (e.getExtendedErrorCode() == 5) {
        retry = true;
        continue;
      }
      return -EIO;
    }
  } while (retry);
  return r;
}

int LRemDBOps::exec_step(SQLite::Statement& stmt)
{
  dout(20) << "SQL: " << stmt.getExpandedSQL() << dendl;
  return stmt.executeStep();
}

LRemDBStore::Cluster::Cluster(const string& cluster_name) {
  string dbname = string("cluster-") + cluster_name + ".db3";
  dbo = make_shared<LRemDBOps>(dbname, SQLite::OPEN_NOMUTEX|SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
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
  pools->clear();
  try {
    auto q = dbo->statement("SELECT * from pools");

    while (dbo->exec_step(q)) {
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
  try {
    auto q = dbo->statement("SELECT * from pools WHERE name = ?");

    q.bind(1, name);

    if (dbo->exec_step(q)) {
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
  try {
    auto q = dbo->statement("SELECT * from pools WHERE id = ?");

    q.bind(1, id);

    if (dbo->exec_step(q)) {
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

int LRemDBStore::Cluster::create_pool(const string& name, const string& val) {
  PoolRef pool = make_shared<Pool>(dbo);

  return pool->create(name, val);
}

int LRemDBStore::Pool::create(const string& _name, const string& _val) {

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

    if (dbo->exec_step(q)) {
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

LRemDBStore::ObjRef LRemDBStore::Pool::get_obj_handler(LRemTransactionStateRef& trans) {
  return std::make_shared<Obj>(dbo, id, trans);
}

LRemDBStore::XAttrsRef LRemDBStore::Pool::get_xattrs_handler(LRemTransactionStateRef& trans) {
  return std::make_shared<XAttrs>(dbo, id, trans);
}

LRemDBStore::OMapRef LRemDBStore::Pool::get_omap_handler(LRemTransactionStateRef& trans) {
  return std::make_shared<OMap>(dbo, id, trans);
}

void LRemDBStore::TableBase::init_table_name(const string& table_name_prefix) {
  table_name = table_name_prefix + sprintf_int("_%d", (int)pool_id);
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
    auto q = dbo->statement(string("SELECT size, mtime, objver, snap_id, snaps, snap_overlap, epoch from ") + table_name +
                            " WHERE nspace = ? AND oid = ?");

    q.bind(1, nspace);
    q.bind(2, oid);

    if (dbo->exec_step(q)) {
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
  auto q = dbo->statement(string("REPLACE INTO ") + table_name + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"); 

  q.bind(1, nspace);
  q.bind(2, oid);

  q.bind(3, (long long)meta.size);
  q.bind(4, ceph::to_iso_8601(meta.mtime));
  q.bind(5, (long long)meta.objver);
  q.bind(6, (long long)meta.snap_id);
  q.bind(7, encode_base64(meta.snaps));
  q.bind(8, encode_base64(meta.snap_overlap));
  q.bind(9, (long long)meta.epoch);

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::Obj::remove_meta() {
  auto q = dbo->statement(string("DELETE FROM ") + table_name +
                          " WHERE nspace = ? and oid = ?"); 

  q.bind(1, nspace);
  q.bind(2, oid);

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::Obj::read_data(uint64_t ofs, uint64_t len,
                                bufferlist *bl) {

  auto q = dbo->statement(string("SELECT size FROM ") + table_name +
                          " WHERE nspace = ? AND oid = ?");

  q.bind(1, nspace);
  q.bind(2, oid);

  try {
    if (!dbo->exec_step(q)) {
      return -ENOENT;
    }
  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
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

  ObjData od(dbo, pool_id, trans);

  return od.read(ofs, len, bl);
}

int LRemDBStore::Obj::write_data(uint64_t ofs, uint64_t len,
                                 const bufferlist& bl) {

  ObjData od(dbo, pool_id, trans);

  int r = od.write(ofs, len, bl);
  if (r < 0) {
    return r;
  }

  len = r;

  return len;
}

int LRemDBStore::Obj::write(uint64_t ofs, uint64_t len,
                            const bufferlist& bl,
                            uint64_t epoch) {

  LRemDBStore::Obj::Meta meta;
  int r = read_meta(&meta);
  if (r < 0) {
    return r;
  }

  meta.touch(epoch);

  return write(ofs, len, bl, meta);
}

int LRemDBStore::Obj::write(uint64_t ofs, uint64_t len,
                            const bufferlist& bl,
                            LRemDBStore::Obj::Meta& meta) {
  int r = write_data(ofs, len, bl);
  if (r < 0) {
    return r;
  }

  uint64_t size = ofs + r;
  if (size > meta.size) {
    meta.size = size;
  }

  return 0;
}

int LRemDBStore::Obj::remove() {
  ObjData od(dbo, pool_id, trans);

  int r = od.remove();
  if (r < 0) {
    return r;
  }

  return remove_meta();
}

int LRemDBStore::Obj::truncate(uint64_t ofs,
                               LRemDBStore::Obj::Meta& meta) {
  ObjData od(dbo, pool_id, trans);

  int r;

  if (ofs >= meta.size) {
    r = od.truncate(ofs);
    if (r < 0) {
      return r;
    }
  }

  meta.size = ofs;

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

void bl_from_blob_col(SQLite::Statement& q, int col_num, bufferlist *bl)
{
  auto blob_col = q.getColumn(col_num);

  const char *data = (const char *)blob_col.getBlob();
  size_t len = blob_col.getBytes();

  bufferptr bp(data, len);
  bl->push_back(bp);
}

int LRemDBStore::ObjData::read_block(int bid, bufferlist *bl) {
  SQLite::Statement q = dbo->statement(string("SELECT data FROM ") + table_name +
                                       " WHERE nspace = ? AND oid = ? AND bid == ?");

  q.bind(1, nspace);
  q.bind(2, oid);
  q.bind(3, bid);

  try {
    if (!dbo->exec_step(q)) {
      return -ENOENT;
    }
  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  bl_from_blob_col(q, 0, bl);

  return 0;
}

int LRemDBStore::ObjData::write_block(int bid, bufferlist& bl) {
  SQLite::Statement q = dbo->statement(string("REPLACE INTO ") + table_name +
                                       " VALUES ( ?, ?, ?, ? )");

  q.bind(1, nspace);
  q.bind(2, oid);
  q.bind(3, bid);
  q.bind(4, bl.c_str(), bl.length());

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::ObjData::truncate_block(int bid) {
  SQLite::Statement q = dbo->statement(string("DELETE FROM ") + table_name +
                                       " WHERE nspace = ? and oid = ? and bid >= ?");

  q.bind(1, nspace);
  q.bind(2, oid);
  q.bind(3, bid);

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

int LRemDBStore::ObjData::remove() {
  SQLite::Statement q = dbo->statement(string("DELETE FROM ") + table_name +
                                       " WHERE nspace = ? and oid = ?");

  q.bind(1, nspace);
  q.bind(2, oid);

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::ObjData::truncate(uint64_t ofs) {
  int start_block = (ofs + block_size - 1) / block_size;

  int r = truncate_block(start_block);
  if (r < 0) {
    return r;
  }

  if (start_block == 0) {
    return 0;
  }

  int block_ofs = ofs % block_size;
  if (block_ofs == 0) {
    return 0;
  }

  int bid = start_block - 1;

  bufferlist bl;
  r = read_block(bid, &bl);
  if (r < 0) {
    return r;
  }

  bufferlist newbl;
  bl.splice(0, block_ofs, &newbl);

  r = write_block(bid, newbl);
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
                                                "PRIMARY KEY (nspace, oid, key)" } ));
  if (r < 0) {
    return r;
  }

  return 0;
}

#define MAX_KEYS_DEFAULT 256

int LRemDBStore::KVTableBase::get_vals(const std::string& start_after,
                                       const std::string &filter_prefix,
                                       uint64_t max_return,
                                       std::map<std::string, bufferlist> *out_vals,
                                       bool *pmore) {
  string s = string("SELECT key, data from ") + table_name +
                    " WHERE nspace = ? AND oid = ? AND key > ''";
  string filt_val;
  if (!filter_prefix.empty()) {
    s += " AND key LIKE ?";
  }

  if (max_return == 0) {
    max_return = MAX_KEYS_DEFAULT;
  }
  max_return = std::min((int)max_return, MAX_KEYS_DEFAULT);

  auto max_req = max_return + 1;

  s += " LIMIT " + sprintf_int("%d", (int)max_req);

  SQLite::Statement q = dbo->statement(s);

  q.bind(1, nspace);
  q.bind(2, oid);
  if (!filter_prefix.empty()) {
    filt_val = filter_prefix + "%";
    q.bind(3, filt_val);
  }

  try {
    while (dbo->exec_step(q)) {
      --max_return;
      if (max_return == 0) {
        if (pmore) {
          *pmore = true;
        }
        return 0;
      }

      string key = q.getColumn(0).getString();

      bufferlist bl;
      bl_from_blob_col(q, 1, &bl);

      (*out_vals)[key] = bl;
    }
  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  if (pmore) {
    *pmore = false;
  }

  return 0;
}

int LRemDBStore::KVTableBase::get_all_vals(std::map<std::string, bufferlist> *out_vals) {
  string s = string("SELECT key, data from ") + table_name +
                    " WHERE nspace = ? AND oid = ? AND key > ''";

  SQLite::Statement q = dbo->statement(s);

  q.bind(1, nspace);
  q.bind(2, oid);

  try {
    while (dbo->exec_step(q)) {
      string key = q.getColumn(0).getString();

      bufferlist bl;
      bl_from_blob_col(q, 1, &bl);

      (*out_vals)[key] = bl;
    }
  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  return 0;
}

int LRemDBStore::KVTableBase::get_vals_by_keys(const std::set<std::string>& keys,
                                               std::map<std::string, bufferlist> *out_vals) {
  string s = string("SELECT key, data from ") + table_name +
                    " WHERE nspace = ? AND oid = ?"
                    " AND key IN (" + join_quoted(keys) + ")";

  SQLite::Statement q = dbo->statement(s);

  q.bind(1, nspace);
  q.bind(2, oid);

  try {
    while (dbo->exec_step(q)) {
      string key = q.getColumn(0).getString();

      bufferlist bl;
      bl_from_blob_col(q, 1, &bl);

      (*out_vals)[key] = bl;
    }
  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  return out_vals->size();
}

int LRemDBStore::KVTableBase::get_val(const std::string& key,
                                      bufferlist *bl)
{
  string s = string("SELECT data from ") + table_name +
                    " WHERE nspace = ? AND oid = ?"
                    " AND key = ?";

  SQLite::Statement q = dbo->statement(s);

  q.bind(1, nspace);
  q.bind(2, oid);
  q.bind(3, key);

  try {
    if (!dbo->exec_step(q)) {
      return -ENODATA;
    }
  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  bl_from_blob_col(q, 0, bl);

  return 0;
}

int LRemDBStore::KVTableBase::rm_keys(const std::set<std::string>& keys) {
  auto q = dbo->statement(string("DELETE FROM ") + table_name +
                          " WHERE nspace = ? and oid = ?"
                          " AND key IN (" + join_quoted(keys) + ")");

  q.bind(1, nspace);
  q.bind(2, oid);

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::KVTableBase::rm_range(const string& key_begin,
                              const string& key_end) {
  auto q = dbo->statement(string("DELETE FROM ") + table_name +
                          " WHERE nspace = ? and oid = ?"
                          " AND key >= ? AND key <= ?");

  q.bind(1, nspace);
  q.bind(2, oid);
  q.bind(3, key_begin);
  q.bind(4, key_end);

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::KVTableBase::clear() {
  auto q = dbo->statement(string("DELETE FROM ") + table_name +
                          " WHERE nspace = ? and oid = ?");
  q.bind(1, nspace);
  q.bind(2, oid);

  int r = dbo->exec(q);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::KVTableBase::set(const std::map<std::string, bufferlist>& m) {
  for (auto& iter : m) {
    auto& key = iter.first;
    auto bl = iter.second;

    SQLite::Statement q = dbo->statement(string("REPLACE INTO ") + table_name +
                                       " VALUES ( ?, ?, ?, ? )");

    q.bind(1, nspace);
    q.bind(2, oid);
    q.bind(3, key);
    q.bind(4, bl.c_str(), bl.length());

    int r = dbo->exec(q);
    if (r < 0) {
      return r;
    }
  }

  return m.size();
}

int LRemDBStore::KVTableBase::get_header(bufferlist *bl) {
  std::set<string> keys;
  keys.insert(string());

  std::map<string, bufferlist> out_vals;

  int r = get_vals_by_keys(keys, &out_vals);
  if (r < 0) {
    return r;
  }

  bl->clear();
  if (out_vals.empty()) {
    return 0;
  }

  auto bliter = out_vals.begin();
  if (bliter == out_vals.end()) { /* should never happen */
    return -EIO;
  }

  bl->claim_append(bliter->second);
  return 0;
}

int LRemDBStore::KVTableBase::set_header(const bufferlist& bl) {
  std::map<string, bufferlist> vals;

  vals[string()] = bl;

  return set(vals);
}

}
