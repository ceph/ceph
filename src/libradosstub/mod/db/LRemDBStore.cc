#include <iostream>
#include <string>
#include <vector>

#include <boost/tokenizer.hpp>

#include "SQLiteCpp/SQLiteCpp.h"
#include "SQLiteCpp/Exception.h"
#include "sqlite3.h"

#include "common/armor.h"
#include "common/ceph_time.h"
#include "common/debug.h"
#include "common/iso_8601.h"
#include "include/ceph_hash.h"

#include "LRemDBStore.h"
#include "LRemDBCluster.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "LRemDBStore: " << this << " " << __func__ \
                           << ": "
#define dout_context g_ceph_context

using namespace std;

#define NUM_DB_SHARDS 2


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

static string sprintf_int(const char *format, int val)
{
  char buf[32];
  snprintf(buf, sizeof(buf), format, val);
  return string(buf);
}

static string to_str(int val)
{
  return sprintf_int("%d", val);
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

class DBStatementCache {
  SQLite::Database *db;
  ceph::mutex lock = ceph::make_mutex("DBStatementCache::lock");
  map<string, list<SQLite::Statement *> > entries;

public:
  DBStatementCache(SQLite::Database *_db) : db(_db) {}

  SQLite::Statement *get(const string& sql) {
    std::unique_lock locker{lock};
    auto& l = entries[sql];
    if (l.empty()) {

      auto stmt = new SQLite::Statement(*db, sql);
      return stmt;
    }
    auto stmt = l.front();
    l.pop_front();
    return stmt;
  }

  void put(SQLite::Statement *stmt) {
    std::unique_lock locker{lock};
    stmt->clearBindings();
    stmt->reset();
    entries[stmt->getQuery()].push_back(stmt);
  }
};

LRemDBOps::LRemDBOps(const std::string& _name, int _flags, ceph::mutex *_trans_lock) : name(_name),
                                                                                       flags(_flags),
                                                                                       trans_lock(_trans_lock) {
#define DB_TIMEOUT_SEC 20
  db = std::make_unique<SQLite::Database>(name, flags, DB_TIMEOUT_SEC * 1000);
  stmt_cache = std::make_unique<DBStatementCache>(db.get());
}

LRemDBOps::Transaction LRemDBOps::new_transaction() {
  return Transaction(*db, trans_lock);
}

LRemDBOps::Transaction *LRemDBOps::alloc_transaction() {
  return new Transaction(*db, trans_lock);
}

SQLite::Statement LRemDBOps::statement(const string& sql) {
  dout(20) << "Statement: " << sql << dendl;
  return SQLite::Statement(*db, sql);
}

SQLite::Statement *LRemDBOps::deferred_statement(const string& sql) {
  dout(20) << "Statement: " << sql << dendl;
  return stmt_cache->get(sql);
}

void LRemDBOps::Transaction::complete_op(int r) {
  if (r < 0) {
    retcode = r;
  }
}

/* remove key, and drop refcount off the statement it points at,
 * if no other key points at it then remove statement
 */
void LRemDBOps::queue_remove_key(const std::string& key) {
  auto iter = statement_keys.find(key);
  if (iter == statement_keys.end()) {
    return;
  }

  statement_keys.erase(key);

  int num = iter->second;

  auto siter = deferred_statements.find(num);
  if (siter == deferred_statements.end()) {
    return;
  }

  auto& entry = siter->second;

  --entry.count;
  if (entry.count == 0) {
    deferred_statements.erase(siter);
  }
}

struct StatementCacheRef {
  DBStatementCache *cache;
  SQLite::Statement *stmt;

  StatementCacheRef(DBStatementCache *_cache,
                    SQLite::Statement *_stmt) : cache(_cache),
                                                stmt(_stmt) {}

  ~StatementCacheRef() {
    cache->put(stmt);
  }
};

StatementCacheRef *LRemDBOps::new_cache_ref(SQLite::Statement *statement) {
  return new StatementCacheRef(stmt_cache.get(), statement);
}

void LRemDBOps::queue_statement(SQLite::Statement *statement, const std::string& key) {
  int num = statement_num++;

  queue_remove_key(key);
  statement_keys[key] = num;
  deferred_statements[num] = {1, std::unique_ptr<StatementCacheRef>(new_cache_ref(statement))};
}

void LRemDBOps::queue_statement(SQLite::Statement *statement, std::vector<std::string>& keys) {
  int num = statement_num++;

  for (const auto& k : keys) {
    queue_remove_key(k);
    statement_keys[k] = num;
  }

  deferred_statements[num] = {(int)keys.size(), std::unique_ptr<StatementCacheRef>(new_cache_ref(statement))};
}

void LRemDBOps::queue_statement_range(SQLite::Statement *statement,
                                      const std::string& begin,
                                      const std::string& end) {
  int num = statement_num++;

  auto iter = statement_keys.lower_bound(begin);
  if (iter != statement_keys.end()) {
    auto eiter = statement_keys.upper_bound(end);

    for (; iter != eiter; ++iter) {
      queue_remove_key(iter->first);
    }
  }

  /* this operation isn't indexed by key, therefore can't be removed */
  deferred_statements[num] = {1, std::unique_ptr<StatementCacheRef>(new_cache_ref(statement))};
}

static ceph::mutex flush_lock = ceph::make_mutex("LRemDBOps::flush_lock");

void LRemDBOps::flush() {
  if (deferred_statements.empty()) {
    return;
  }

  std::unique_lock locker{flush_lock};

  Transaction t(*db, trans_lock);

  for (auto& i : deferred_statements) {
    auto s = i.second.ref->stmt;
    int r = exec(*s);
    if (r < 0) {
      t.complete_op(r);
      break;
    }
  }

  deferred_statements.clear();
  statement_keys.clear();
}

class DBOpsCache {
  ceph::mutex lock = ceph::make_mutex("DBOpsCache::lock");
  ceph::mutex trans_lock = ceph::make_mutex("DBOpsCache::trans_lock");
  map<string, list<LRemDBOpsRef> > entries;

public:
  DBOpsCache() {}

  LRemDBOpsRef get(const string& dbname) {
    std::unique_lock locker{lock};
    auto& l = entries[dbname];
    if (l.empty()) {
      return make_shared<LRemDBOps>(dbname, SQLite::OPEN_NOMUTEX|SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE, &trans_lock);
    }
    auto dbo = l.front();
    l.pop_front();
    return dbo;
  }

  void put(LRemDBOpsRef&& dbo) {
    std::unique_lock locker{lock};
    entries[dbo->get_name()].push_back(std::move(dbo));
  }
};

static DBOpsCache dbops_cache[NUM_DB_SHARDS + 1];

LRemDBOps::Transaction::Transaction(SQLite::Database& db,
                                    ceph::mutex *_lock) : lock(_lock) {
  trans = make_unique<SQLite::Transaction>(db, true);
}

LRemDBOps::Transaction::~Transaction() {
  std::unique_lock locker{*lock};

  if (!trans) {
    return;
  }

  if (retcode >= 0) {
    trans->commit();
  }
  trans.reset();
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
    dout(0) << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << " db=" << db.get() << dendl;
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
      dout(20) << "SQL: " << stmt.getQuery() << dendl;
      r = stmt.exec();
      /* return code is not interesting */
    } catch (SQLite::Exception& e) {
      dout(0) << "exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << " db=" << db.get() << dendl;
      if (e.getExtendedErrorCode() == SQLITE_BUSY) {
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
  dout(20) << "SQL: " << stmt.getQuery() << dendl;
  return stmt.executeStep();
}

static uint32_t do_hash(const string& s) {
  return ceph_str_hash_linux(s.c_str(), s.size());
}

static uint32_t loc_hash(const LRemCluster::ObjectLocator& loc) {
  return do_hash(loc.nspace) ^ do_hash(loc.name);
}


LRemDBTransactionState::LRemDBTransactionState(CephContext *_cct) : cct(_cct) {
  init();
}

LRemDBTransactionState::LRemDBTransactionState(CephContext *_cct,
                                               const LRemCluster::ObjectLocator& loc) : LRemTransactionState(loc), cct(_cct) {
  shard_id = loc_hash(loc) % 1021 % NUM_DB_SHARDS;
  init();
}

LRemDBTransactionState::~LRemDBTransactionState() {
  for (auto& trans : db_trans) {
    if (trans) {
      trans->abort();
      trans.reset();
    }
  }

  for (auto& op : all_ops) {
    op.dbo->flush();

    op.cache->put(std::move(op.dbo));
  }
}

void LRemDBTransactionState::init() {
  auto uuid = cct->_conf.get_val<uuid_d>("fsid");
  auto dir = cct->_conf.get_val<string>("librados_sqlite_data_dir");
  if (!dir.empty()) {
    db_name_prefix = dir + "/";
  }
  db_name_prefix += string("cluster-") + uuid.to_string();
  dbc = std::make_shared<LRemDBStore::Cluster>(cct, this);
}


LRemDBOpsRef& LRemDBTransactionState::dbroot(bool start_trans) {
  if (ops.dbroot) {
    return ops.dbroot;
  }

  auto dbname = db_name_prefix + ".db3";

  auto& ops_cache = dbops_cache[NUM_DB_SHARDS];

  ops.dbroot = ops_cache.get(dbname);
  all_ops.push_back({&ops_cache,
                     ops.dbroot});

  if (start_trans) {
    db_trans.push_back(std::unique_ptr<LRemDBOps::Transaction>(ops.dbroot->alloc_transaction()));
  }

  return ops.dbroot;
}

LRemDBOpsRef& LRemDBTransactionState::dbo(int sid, bool start_trans) {
  auto& _dbo = ops.dbo[sid];
  if (_dbo) {
    return _dbo;
  }

  auto dbname = db_name_prefix + "-" + to_str(sid) + ".db3";

  auto& ops_cache = dbops_cache[shard_id];

  _dbo = ops_cache.get(dbname);
  all_ops.push_back({&ops_cache,
                     _dbo});

  if (start_trans) {
    db_trans.push_back(std::unique_ptr<LRemDBOps::Transaction>(_dbo->alloc_transaction()));
  }

  return _dbo;
}

LRemDBOpsRef& LRemDBTransactionState::dbo() {
  return dbo(shard_id);
}

void LRemDBTransactionState::set_write(bool w) {
  if (write || !w) {
    return;
  }

  LRemTransactionState::set_write(w);
}

void LRemDBStore::TableBase::set_instance(LRemDBTransactionState *_trans) {
  trans = _trans;
  nspace = trans->nspace();
  oid = trans->oid();
}

LRemDBStore::Cluster::Cluster(CephContext *cct,
                              LRemDBTransactionState *_trans) : trans(_trans) {}

int LRemDBStore::Cluster::init_cluster() {
  auto db_conf = join({ "PRAGMA journal_mode = wal",
                        // "PRAGMA page_size = 32768",
                        "PRAGMA synchronous = NORMAL",
                        "PRAGMA temp_store = memory",
                       //  "PRAGMA wal_autocheckpoint=10"
                      },
                        ";");
  auto& dbroot = trans->dbroot(false);
  int r = dbroot->exec(db_conf);
  if (r < 0) {
    return r;
  }

  r = dbroot->create_table("pools", join( { "id INTEGER PRIMARY KEY",
                                            "name TEXT UNIQUE",
                                            "value TEXT" } ));
  if (r < 0) {
    return r;
  }

  for (int sid = 0; sid < NUM_DB_SHARDS; ++sid) {
    auto& dbo = trans->dbo(sid, false);
    int r = dbo->exec(db_conf);
    if (r < 0) {
      return r;
    }
  }

  trans->commit();

  return 0;
};

class PoolCache {
public:
  struct PoolInfo;
private:
  ceph::shared_mutex lock = ceph::make_shared_mutex("PoolCache::lock");
  map<string, PoolInfo> pools_by_name;
public:
  PoolCache() {}

  struct PoolInfo {
    int id;
    string name;
    string value;
  };

  bool get_pool(const string& name, PoolInfo *info) {
    std::shared_lock locker{lock};

    auto iter = pools_by_name.find(name);
    if (iter == pools_by_name.end()) {
      return false;
    }

    *info = iter->second;
    return true;
  }

  bool get_pool(int id, PoolInfo *info) {
    std::shared_lock locker{lock};

    for (auto& i : pools_by_name) {
      if (i.second.id == id) {
        *info = i.second;
        return true;
      }
    }
    return false;
  }

  bool list_pools(map<string, PoolInfo> *pm) {
    std::shared_lock locker{lock};
    *pm = pools_by_name;
    return (!pools_by_name.empty());
  }

  void set_pool(const PoolInfo& info) {
    std::unique_lock locker{lock};
    pools_by_name[info.name] = info;
  }

  void set_pools(map<string, PoolInfo>&& new_map) {
    std::unique_lock locker{lock};
    pools_by_name = std::move(new_map);
  }

  void clear() {
    std::unique_lock locker{lock};
    pools_by_name.clear();
  }
};

static PoolCache pool_cache;

int LRemDBStore::Cluster::list_pools(std::map<string, PoolRef> *pools)
{
  map<string, PoolCache::PoolInfo> m;

  if (pool_cache.list_pools(&m)) {
    for (auto& i : m) {
      auto& e = i.second;
      (*pools)[e.name] = std::make_shared<Pool>(trans, e.id, e.name, e.value);
    }
    return 0;
  }

  auto& dbroot = trans->dbroot();
  pools->clear();
  try {
    auto q = dbroot->statement("SELECT * from pools");

    while (dbroot->exec_step(q)) {
      int         id      = q.getColumn(0);
      const char* name   = q.getColumn(1);
      const char* value   = q.getColumn(2);

      (*pools)[name] = std::make_shared<Pool>(trans, id, name, value);
      m[name] = {id, name, value};
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  pool_cache.set_pools(std::move(m));

  return 0;
}

int LRemDBStore::Cluster::get_pool(const string& name, PoolRef *pool) {
  PoolCache::PoolInfo pi;
  if (pool_cache.get_pool(name, &pi)) {
    *pool = std::make_shared<Pool>(trans, pi.id, pi.name, pi.value);
    return pi.id;
  }
  auto& dbroot = trans->dbroot();
  try {
    auto q = dbroot->statement("SELECT * from pools WHERE name = ?");

    q.bind(1, name);

    if (dbroot->exec_step(q)) {
      int         id      = q.getColumn(0);
      const char* name   = q.getColumn(1);
      const char* value   = q.getColumn(2);

      *pool = std::make_shared<Pool>(trans, id, name, value);

      pool_cache.set_pool({id, name, value});

      return id;
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  return -ENOENT;
}

int LRemDBStore::Cluster::get_pool(int id, PoolRef *pool) {
  PoolCache::PoolInfo pi;
  if (pool_cache.get_pool(id, &pi)) {
    *pool = std::make_shared<Pool>(trans, pi.id, pi.name, pi.value);
    return pi.id;
  }
  auto& dbroot = trans->dbroot();
  try {
    auto q = dbroot->statement("SELECT * from pools WHERE id = ?");

    q.bind(1, id);

    if (dbroot->exec_step(q)) {
      int         id      = q.getColumn(0);
      const char* name   = q.getColumn(1);
      const char* value   = q.getColumn(2);

      *pool = std::make_shared<Pool>(trans, id, name, value);

      pool_cache.set_pool({id, name, value});

      return id;
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  return -ENOENT;
}

int LRemDBStore::Cluster::create_pool(const string& name, const string& val) {
  PoolRef pool = make_shared<Pool>(trans);

  return pool->create(name, val);
}

int LRemDBStore::Pool::create(const string& _name, const string& _val) {

  name = _name;
  value = _val;

  auto& dbroot = trans->dbroot();
  int r = dbroot->exec(string("INSERT INTO pools VALUES (" + join( { "NULL", ::quoted(name), ::quoted(value) } ) + ")"));
  if (r < 0) {
    return r;
  }

  trans->commit();

  pool_cache.clear();

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
  PoolCache::PoolInfo pi;
  if (pool_cache.get_pool(name, &pi)) {
    id = pi.id;
    value = pi.value;
    return id;
  }

  auto& dbroot = trans->dbroot();
  try {
    auto q = dbroot->statement("SELECT * from pools WHERE name = ?");

    q.bind(1, name);

    if (dbroot->exec_step(q)) {
      id      = q.getColumn(0);
      value   = q.getColumn(2).getString();

      pool_cache.set_pool({id, name, value});

      return id;
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  dout(20) << "pool " << name << " not found" << dendl;
  return -ENOENT;
}

static inline string escape_str(const std::string& s,
                                char esc_char,
                                char special_char)
{
  const char *src = s.c_str();
  char dest_buf[s.size() * 2 + 1];
  char *destp = dest_buf;

  for (size_t i = 0; i < s.size(); i++) {
    char c = src[i];
    if (c == esc_char || c == special_char) {
      *destp++ = esc_char;
    }
    *destp++ = c;
  }
  *destp++ = '\0';
  return string(dest_buf);
}

static inline string escape(std::vector<std::string> v, char esc = '\\', char sep = ':')
{
  string result;
  bool start = true;

  for (auto& s : v) {
    if (!start) {
      result += sep;
    }
    start = false;
    result.append(escape_str(s, esc, sep));
  }

  return result;
}

static inline std::vector<string> unescape(string s, char esc = '\\', char sep = ':')
{
  vector<string> v;
  boost::escaped_list_separator<char> els(esc, sep, '\0');
  boost::tokenizer<boost::escaped_list_separator<char>> tok(s, els);
  for (auto& s : tok) {
    v.push_back(s);
  }

  return v;
}


int LRemDBStore::Pool::list(std::optional<string> nspace,
             const string& _marker,
             std::optional<string> filter,
             int max,
             std::list<LRemDBStore::Pool::list_entry> *result,
             bool *more) {
  string marker = _marker;

  auto obj = get_obj_handler();

  const auto& table_name = obj->get_table_name();

  string marker_oid;
  string marker_nspace;
  int shard_id = 0;

  if (!marker.empty()) {
    auto v = unescape(marker);
    if (v.size() != 3) {
      return -EINVAL;
    }
    shard_id = atoi(v[0].c_str());
    marker_nspace = v[1];
    marker_oid = v[2];
  }

#define MAX_LIST_DEFAULT 256
  int limit = (max ? max : MAX_LIST_DEFAULT);
  if (limit > MAX_LIST_DEFAULT) {
    limit = MAX_LIST_DEFAULT;
  }

  result->clear();

  for (; shard_id < NUM_DB_SHARDS && limit > 0; ++shard_id) {
    auto& dbo = trans->dbo(shard_id);

    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", shard_id);
    string marker_prefix = buf;

    try {
      string s = string("SELECT nspace, oid from ") + table_name + " WHERE";

      string filter_str;

      bool complex_marker = (!nspace && !marker.empty());

      if (complex_marker) {
        s += " ((nspace == ? AND oid > ?) OR (nspace > ?))";
      } else {
        s += " oid > ?";
      }
      if (filter) {
        s += " AND oid LIKE ?";
      }

      if (nspace) {
        s += " AND nspace == ?";
      }

      s += " LIMIT " + to_str(limit + 1);

      auto q = dbo->statement(s);

      int n = 0;
      if (complex_marker) {
        q.bind(++n, marker_nspace);
        q.bind(++n, marker_oid);
        q.bind(++n, marker_nspace);
      } else {
        q.bind(++n, marker_oid);
      }
      if (filter) {
        q.bind(++n, *filter + "%");
      }
      if (nspace) {
        q.bind(++n, *nspace);
      }

      int count = 0;
      while (dbo->exec_step(q)) {
        if (++count > limit) {
          *more = true;
          break;
        }
        string result_nspace;
        string result_oid;

        result_nspace = q.getColumn(0).getString();
        result_oid = q.getColumn(1).getString();

        string marker = escape({ marker_prefix, result_nspace, result_oid });

        list_entry e = { marker,
                         {result_nspace, result_oid} };

        result->push_back(e);
      }

      limit -= count;
    } catch (SQLite::Exception& e) {
      dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
      return -EIO;
    }

    marker.clear();
    marker_nspace.clear();
    marker_oid.clear();
  }

  return result->size();
}

int LRemDBStore::Pool::init_tables() {
  LRemDBTransactionState trans2(trans->cct);
  for (int sid = 0; sid < NUM_DB_SHARDS; ++sid) {
    Obj obj_table(&trans2, id);
    int r = obj_table.create_table(sid);
    if (r < 0) {
      return r;
    }

    ObjData od_table(&trans2, id);
    r = od_table.create_table(sid);
    if (r < 0) {
      return r;
    }

    XAttrs xattrs_table(&trans2, id);
    r = xattrs_table.create_table(sid);
    if (r < 0) {
      return r;
    }

    OMap omap_table(&trans2, id);
    r = omap_table.create_table(sid);
    if (r < 0) {
      return r;
    }
  }

  trans2.commit();

  return 0;
}

LRemDBStore::ObjRef LRemDBStore::Pool::get_obj_handler() {
  return std::make_shared<Obj>(trans, id);
}

LRemDBStore::XAttrsRef LRemDBStore::Pool::get_xattrs_handler() {
  return std::make_shared<XAttrs>(trans, id);
}

LRemDBStore::OMapRef LRemDBStore::Pool::get_omap_handler() {
  return std::make_shared<OMap>(trans, id);
}

void LRemDBStore::TableBase::init_table_name(const string& table_name_prefix) {
  table_name = table_name_prefix + sprintf_int("_%d", (int)pool_id);
}

int LRemDBStore::Obj::create_table(int shard_id) {
  int r = trans->dbo(shard_id)->create_table(table_name, join( {
                                                "nspace TEXT",
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
  auto& dbo = trans->dbo();

  if (trans->cache.meta) {
    *pmeta = *trans->cache.meta;
    return 0;
  }

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

      trans->cache.meta = *pmeta;

      return 0;
    }

  } catch (SQLite::Exception& e) {
    dout(0) << "ERROR: SQL exception: " << e.what() << " ret=" << e.getExtendedErrorCode() << dendl;
    return -EIO;
  }

  return -ENOENT;
}

int LRemDBStore::Obj::write_meta(const LRemDBStore::Obj::Meta& meta) {
  auto& dbo = trans->dbo();
  auto q = dbo->deferred_statement(string("REPLACE INTO ") + table_name + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"); 

  q->bind(1, nspace);
  q->bind(2, oid);

  q->bind(3, (long long)meta.size);
  q->bind(4, ceph::to_iso_8601(meta.mtime));
  q->bind(5, (long long)meta.objver);
  q->bind(6, (long long)meta.snap_id);
  q->bind(7, encode_base64(meta.snaps));
  q->bind(8, encode_base64(meta.snap_overlap));
  q->bind(9, (long long)meta.epoch);

  dbo->queue_statement(q, join({table_name, nspace, oid}, ":"));

  trans->cache.meta = meta;

  return 0;
}

int LRemDBStore::Obj::remove_meta() {
  auto& dbo = trans->dbo();
  auto q = dbo->deferred_statement(string("DELETE FROM ") + table_name +
                          " WHERE nspace = ? and oid = ?"); 

  q->bind(1, nspace);
  q->bind(2, oid);

  dbo->queue_statement(q, join({table_name, nspace, oid}, ":"));

  return 0;
}

int LRemDBStore::Obj::read_data(uint64_t ofs, uint64_t len,
                                bufferlist *bl) {

  auto& dbo = trans->dbo();
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

  ObjData od(trans, pool_id);

  return od.read(ofs, len, bl);
}

int LRemDBStore::Obj::write_data(uint64_t ofs, uint64_t len,
                                 const bufferlist& bl) {

  ObjData od(trans, pool_id);

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
  ObjData od(trans, pool_id);

  int r = od.remove();
  if (r < 0) {
    return r;
  }

  return remove_meta();
}

int LRemDBStore::Obj::truncate(uint64_t ofs,
                               LRemDBStore::Obj::Meta& meta) {
  ObjData od(trans, pool_id);

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

int LRemDBStore::ObjData::create_table(int shard_id) {
  auto& dbo = trans->dbo(shard_id);
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
  auto iter = trans->data_blocks.find(bid);
  if (iter != trans->data_blocks.end()) {
    *bl = iter->second;
    return 0;
  }

  auto& dbo = trans->dbo();
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

  trans->data_blocks[bid] = *bl;

  return 0;
}

int LRemDBStore::ObjData::write_block(int bid, bufferlist& bl) {
  trans->data_blocks[bid] = bl;

  auto& dbo = trans->dbo();
  auto q = dbo->deferred_statement(string("REPLACE INTO ") + table_name +
                                       " VALUES ( ?, ?, ?, ? )");

  q->bind(1, nspace);
  q->bind(2, oid);
  q->bind(3, bid);
  q->bind(4, bl.c_str(), bl.length());

  dbo->queue_statement(q, join({table_name, nspace, oid, to_str(bid)}, ":"));

  return 0;
}

int LRemDBStore::ObjData::truncate_block(int bid) {
  auto& dbo = trans->dbo();
  auto q = dbo->deferred_statement(string("DELETE FROM ") + table_name +
                                       " WHERE nspace = ? and oid = ? and bid >= ?");

  q->bind(1, nspace);
  q->bind(2, oid);
  q->bind(3, bid);

  dbo->queue_statement(q, join({table_name, nspace, oid, to_str(bid)}, ":"));

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
      bbl.append_zero(block_len);
    } else if (r < 0) {
      return r;
    }

    auto read_len = bbl.length();

    auto zero_len = 0;
    if (block_len > read_len) {
      zero_len = block_len - read_len;
    }


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
    auto slen = block_len - block_ofs;
    bufferlist bbl;
    int r = read_block(cur_block, &bbl);
    if (r < 0 && r != -ENOENT) {
      return r;
    }

    if ((uint64_t)block_len > bbl.length()) { /* pad with zeros to match new size */
      bbl.append_zero(block_len - bbl.length());
    }

    char *pdst = bbl.c_str();

    bliter.copy(slen, pdst + block_ofs);

    bufferptr bp(pdst, bbl.length());
    bufferlist new_block;

    new_block.push_back(bp);

    r = write_block(cur_block, new_block);
    if (r < 0) {
      return r;
    }

    block_ofs = 0; /* next block starts from zero */
    len -= slen;
    ++cur_block;
  }

  return (int)write_len;
}

int LRemDBStore::ObjData::remove() {
  auto& dbo = trans->dbo();
  auto q = dbo->deferred_statement(string("DELETE FROM ") + table_name +
                                       " WHERE nspace = ? and oid = ?");

  q->bind(1, nspace);
  q->bind(2, oid);

  dbo->queue_statement(q, join({table_name, nspace, oid}, ":"));

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
  if (block_ofs > 0) {
    bl.splice(0, block_ofs, &newbl);
  }

  r = write_block(bid, newbl);
  if (r < 0) {
    return r;
  }

  return 0;
}

int LRemDBStore::KVTableBase::create_table(int shard_id) {
  auto& dbo = trans->dbo(shard_id);
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
  auto& dbo = trans->dbo();
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

  auto& dbo = trans->dbo();
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

  auto& dbo = trans->dbo();
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

  auto& dbo = trans->dbo();
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
  auto& dbo = trans->dbo();
  auto q = dbo->deferred_statement(string("DELETE FROM ") + table_name +
                          " WHERE nspace = ? and oid = ?"
                          " AND key IN (" + join_quoted(keys) + ")");

  q->bind(1, nspace);
  q->bind(2, oid);

  std::vector<string> qkeys;
  for (auto& k : keys) {
    qkeys.push_back(join({table_name, nspace, oid, k}, ":"));
  }

  dbo->queue_statement(q, qkeys);

  return 0;
}

int LRemDBStore::KVTableBase::rm_range(const string& key_begin,
                              const string& key_end) {
  auto& dbo = trans->dbo();
  auto q = dbo->deferred_statement(string("DELETE FROM ") + table_name +
                          " WHERE nspace = ? and oid = ?"
                          " AND key >= ? AND key <= ?");

  q->bind(1, nspace);
  q->bind(2, oid);
  q->bind(3, key_begin);
  q->bind(4, key_end);

  string qstart = join({table_name, nspace, oid, key_begin}, ":");
  string qend = join({table_name, nspace, oid, key_end}, ":");

  dbo->queue_statement_range(q, qstart, qend);

  return 0;
}

int LRemDBStore::KVTableBase::clear() {
  auto& dbo = trans->dbo();
  auto q = dbo->deferred_statement(string("DELETE FROM ") + table_name +
                          " WHERE nspace = ? and oid = ?");
  q->bind(1, nspace);
  q->bind(2, oid);

  dbo->queue_statement(q, join({table_name, nspace, oid}, ":"));

  return 0;
}

int LRemDBStore::KVTableBase::set(const std::map<std::string, bufferlist>& m) {
  for (auto& iter : m) {
    auto& key = iter.first;
    auto bl = iter.second;

    auto& dbo = trans->dbo();
    auto q = dbo->deferred_statement(string("REPLACE INTO ") + table_name +
                                       " VALUES ( ?, ?, ?, ? )");

    q->bind(1, nspace);
    q->bind(2, oid);
    q->bind(3, key);
    q->bind(4, bl.c_str(), bl.length());

    dbo->queue_statement(q, join({table_name, nspace, oid, key}, ":"));
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
