#include <expected>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <vector>

#include "common/debug.h"

#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "include/rados/librados.hpp"

#include "AuditDB.h"

#define dout_subsys ceph_subsys_audit_logging
#define adout(cct, lvl) ldout((cct), (lvl)) << "audit_logging: " << __func__ << ": "
#define aderr(cct) lderr((cct)) << "audit_logging: " << __func__ << ": "

namespace {
enum class PreparePhase : std::uint8_t {
  Insert,
  InsertAutoSeq,
  Update
};

std::once_flag sqlite_vfs_init_flag;

const char*
flag_to_op(int flags)
{
  if (flags & SQLITE_OPEN_READONLY)
    return "read";
  if ((flags & SQLITE_OPEN_READWRITE) && (flags & SQLITE_OPEN_CREATE))
    return "create";
  if (flags & SQLITE_OPEN_READWRITE)
    return "readwrite";
  return "unknown";
}

// allowed columns for ORDER BY to prevent SQL injection
bool
is_valid_order_column(const std::string& col)
{
  return col == "seq" || col == "init_time";
}

// `table_name` is concatenated as-is into every DDL/DML statement
// (create_table, prepare_sqlite3_stmt, query, count, trim,
// get_last_committed_seq). The only way to keep those statements safe
// is a strict check at the API boundary:
//   - 1..63 characters
//   - starts with a letter or underscore
//   - remaining characters in [A-Za-z0-9_]
//   - "schema_version" is reserved by this library's schema table
bool
is_valid_table_name(const std::string& name)
{
  if (name == "schema_version")
    return false;
  static const std::regex re(R"(^[A-Za-z_][A-Za-z0-9_]{0,62}$)");
  return std::regex_match(name, re);
}

std::expected<void, AuditDBError>
open_connection(
    CephContext* cct,
    const char* db_file,
    sqlite3*& db_handle,
    int flags)
{
  int rc = sqlite3_open_v2(db_file, &db_handle, flags, "ceph");
  if (rc != SQLITE_OK) {
    const char* err = nullptr;
    if (db_handle) {
      err = sqlite3_errmsg(db_handle);
      sqlite3_close(db_handle);
    } else {
      err = "no db handle";
    }
    db_handle = nullptr;
    aderr(cct) << "failed to create '" << db_file << "': rc=" << rc
               << " err=" << err << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_open_failed, rc});
  }

  // enable extended codes for more granular control over checks
  sqlite3_extended_result_codes(db_handle, 1);
  return {};
}

std::expected<void, AuditDBError>
apply_pragmas(
    CephContext* cct,
    sqlite3*& db_handle,
    const std::vector<const char*>& pragmas)
{
  char* err_msg = nullptr;
  for (const auto* pragma : pragmas) {
    int rc = sqlite3_exec(db_handle, pragma, nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
      aderr(cct) << "error while setting '" << pragma
                 << "': " << (err_msg ? err_msg : "unknown sqlite error")
                 << dendl;
      sqlite3_free(err_msg);
      err_msg = nullptr;
      return std::unexpected(AuditDBError{AuditDBErr::sqlite_exec_failed, rc});
    }
  }
  return {};
}

std::expected<void, AuditDBError>
create_table(CephContext* cct, sqlite3*& db_handle, const std::string& table)
{
  char* err_msg = nullptr;
  std::string table_sql;
  if (table == "schema_version") {
    table_sql =
        "CREATE TABLE IF NOT EXISTS schema_version ("
        "version INTEGER PRIMARY KEY"
        ");"
        "INSERT OR IGNORE INTO schema_version VALUES (1);";
  } else {
    table_sql =
        "CREATE TABLE IF NOT EXISTS " + table +
        " ("
        "seq INTEGER PRIMARY KEY, "
        "init_time INTEGER NOT NULL CHECK (init_time > 0), "
        "json_dump TEXT NOT NULL"
        ");";
  }
  int rc =
      sqlite3_exec(db_handle, table_sql.c_str(), nullptr, nullptr, &err_msg);
  if (rc != SQLITE_OK) {
    aderr(cct) << "cannot create " << table
               << " table: " << (err_msg ? err_msg : "unknown sqlite error")
               << dendl;
    sqlite3_free(err_msg);
    err_msg = nullptr;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_exec_failed, rc});
  }

  if (table != "schema_version") {
    const std::string indexes =
        "CREATE INDEX IF NOT EXISTS idx_audit_init_time ON " + table +
        "(init_time);";
    err_msg = nullptr;
    rc = sqlite3_exec(db_handle, indexes.c_str(), nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
      aderr(cct) << "error while setting indexes: "
                 << (err_msg ? err_msg : "unknown sqlite error") << dendl;
      sqlite3_free(err_msg);
      err_msg = nullptr;
      return std::unexpected(AuditDBError{AuditDBErr::sqlite_exec_failed, rc});
    }
  }
  return {};
}

std::expected<std::string, AuditDBError>
verify_pragma(CephContext* cct, sqlite3*& db_handle, const std::string& sql)
{
  sqlite3_stmt* stmt = nullptr;
  std::string result;
  int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    aderr(cct) << "pragma prepare failed: " << sqlite3_errmsg(db_handle)
               << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_prepare_failed, rc});
  }
  rc = sqlite3_step(stmt);
  if (rc == SQLITE_ROW) {
    if (const auto* t =
            reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))) {
      result = t;
    }
  }
  sqlite3_finalize(stmt);
  if (rc != SQLITE_ROW) {
    aderr(cct) << "pragma step failed: rc=" << rc << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }
  return result;
}

std::expected<void, AuditDBError>
prepare_sqlite3_stmt(
    CephContext* cct,
    sqlite3*& db_handle,
    sqlite3_stmt*& stmt,
    const std::string& table,
    PreparePhase phase)
{
  std::string sql;
  if (phase == PreparePhase::Insert) {
    sql = "INSERT INTO " + table +
          " (seq, init_time, json_dump) "
          "VALUES (?, ?, ?);";
  } else if (phase == PreparePhase::InsertAutoSeq) {
    sql = "INSERT INTO " + table +
          " (init_time, json_dump) "
          "VALUES (?, ?);";
  } else {
    sql = "UPDATE " + table +
          " SET json_dump = ? "
          "WHERE seq = ?;";
  }
  int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    std::string what;
    if (phase == PreparePhase::Insert || phase == PreparePhase::InsertAutoSeq) {
      what = "insert_stmt";
    } else {
      what = "update_stmt";
    }
    aderr(cct) << what << " prepare failed: " << sqlite3_errmsg(db_handle) << dendl;
    if (stmt) {
      sqlite3_finalize(stmt);
      stmt = nullptr;
    }
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_prepare_failed, rc});
  }

  return {};
}

// builds WHERE clause and returns bind values for query/count
std::string
build_where(
    const AuditQuery& q,
    std::vector<std::pair<int, int64_t>>& int_binds,
    std::vector<std::pair<int, std::string>>& text_binds)
{
  std::string where;
  int bind_idx = 1;

  auto add_condition = [&](const std::string& clause) {
    where += where.empty() ? " WHERE " : " AND ";
    where += clause;
  };

  if (q.after_seq) {
    add_condition("seq > ?");
    int_binds.push_back({bind_idx++, *q.after_seq});
  }
  if (q.before_seq) {
    add_condition("seq < ?");
    int_binds.push_back({bind_idx++, *q.before_seq});
  }
  if (q.since) {
    add_condition("init_time >= ?");
    int_binds.push_back({bind_idx++, static_cast<int64_t>(*q.since)});
  }
  if (q.until) {
    add_condition("init_time < ?");
    int_binds.push_back({bind_idx++, static_cast<int64_t>(*q.until)});
  }
  for (const auto& f : q.json_filters) {
    add_condition("CAST(json_extract(json_dump, ?) AS TEXT) = ?");
    text_binds.push_back({bind_idx++, "$." + f.field});
    text_binds.push_back({bind_idx++, f.value});
  }

  return where;
}

// bind numerical and textual values with the sqlite3_stmt, primarily used in
// the query/count after calling build_where()
int
bind_params(
    sqlite3_stmt* stmt,
    const std::vector<std::pair<int, int64_t>>& int_binds,
    const std::vector<std::pair<int, std::string>>& text_binds)
{
  for (auto& [idx, val] : int_binds) {
    int rc = sqlite3_bind_int64(stmt, idx, val);
    if (rc != SQLITE_OK)
      return rc;
  }
  for (auto& [idx, val] : text_binds) {
    int rc = sqlite3_bind_text(stmt, idx, val.c_str(), -1, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK)
      return rc;
  }
  return SQLITE_OK;
}

void vacuum(CephContext* cct, sqlite3*& db_handle)
{
  char* err_msg = nullptr;
  int rc = sqlite3_exec(db_handle, "VACUUM;", nullptr, nullptr, &err_msg);
  
  if (rc != SQLITE_OK) {
    adout(cct, 10) << "warning: " << (err_msg ? err_msg :
      sqlite3_errmsg(db_handle)) << dendl;
    sqlite3_free(err_msg);
  } else {
    adout(cct, 20) << "completed successfully" << dendl;
  }

}

}

namespace audit_db_detail {

struct InitFailureGuard {
  AuditDB& db;
  bool committed{false};

  explicit InitFailureGuard(AuditDB& d) :
    db(d)
  {}

  void
  commit()
  {
    committed = true;
  }

  ~InitFailureGuard()
  {
    if (!committed) {
      db.release_sqlite_handles();
    }
  }

  // copying is forbidden, allow only one guard per init()
  InitFailureGuard(const InitFailureGuard&) = delete;
  InitFailureGuard& operator=(const InitFailureGuard&) = delete;
};

}

AuditDB::AuditDB(
    CephContext* cct_,
    const char* db_file_or_uri,
    bool is_uri_,
    std::string table_name,
    bool is_standalone_) :
  cct(cct_),
  db_file(db_file_or_uri),
  is_uri(is_uri_),
  table_name(std::move(table_name)),
  is_standalone(is_standalone_)
{}

void
AuditDB::release_sqlite_handles()
{
  if (insert_stmt) {
    sqlite3_finalize(insert_stmt);
    insert_stmt = nullptr;
  }
  if (update_stmt) {
    sqlite3_finalize(update_stmt);
    update_stmt = nullptr;
  }
  if (db_handle) {
    sqlite3_close(db_handle);
    db_handle = nullptr;
  }
}

std::expected<void, AuditDBError>
AuditDB::init()
{
  // init() is documented as "call once before any other method"; all
  // other methods gate on `initialised` which we only flip at the end
  // of a successful init, so there is no contending caller to lock
  // against here.
  audit_db_detail::InitFailureGuard init_guard(*this);

  // reject bad table names before we touch sqlite; `table_name` is
  // interpolated directly into every statement this class issues.
  if (!is_valid_table_name(table_name)) {
    aderr(cct) << "invalid table name: '" << table_name << "'" << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::invalid_table_name, 0});
  }

  // ensure ceph vfs is initialized
  std::call_once(sqlite_vfs_init_flag, init_cephsqlite_vfs, cct);

  // SQLITE_OPEN_READWRITE is mandatory while creating a db
  int flags = 0;
  if (is_uri) {
    flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI;
  } else {
    flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  }

  // create db
  if (auto e = open_connection(cct, db_file.c_str(), db_handle, flags); !e) {
    return e;
  }

  // set a timeout for SQLITE_BUSY
  sqlite3_busy_timeout(db_handle, 5000); // 5s

  // add pragmas to the db
  const std::vector<const char*> pragmas = {
      "PRAGMA page_size = 65536", "PRAGMA cache_size = 4096",
      "PRAGMA locking_mode = NORMAL", "PRAGMA journal_mode = PERSIST",
      // would make some queries quicker to execute like ORDER_BY...
      "PRAGMA temp_store = memory",
      // link up tables from say mon table and mgr table?
      "PRAGMA foreign_keys = 1"};
  if (auto e = apply_pragmas(cct, db_handle, pragmas); !e) {
    return e;
  }

  // most pragmas fail loudly via sqlite3_exec() but journal_mode is a
  // documented exception.
  if (auto mode = verify_pragma(
    cct, db_handle, "PRAGMA journal_mode;"); !mode) {
    return std::unexpected(mode.error());
  } else if (*mode != "persist") {
    aderr(cct) << "expected journal_mode=persist, got '" << *mode << "'"
               << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_exec_failed, 0});
  }

  // schema versioning for future migrations
  if (auto e = create_table(cct, db_handle, "schema_version"); !e) {
    return e;
  }

  // default table
  if (auto e = create_table(cct, db_handle, table_name); !e) {
    return e;
  }

  // prepare phase-one statement
  if (auto e = prepare_sqlite3_stmt(cct, db_handle, insert_stmt, table_name,
    is_standalone ? PreparePhase::InsertAutoSeq : PreparePhase::Insert);
      !e) {
    return e;
  }

  // prepare phase-two statement
  if (auto e = prepare_sqlite3_stmt(
          cct, db_handle, update_stmt, table_name, PreparePhase::Update);
      !e) {
    return e;
  }

  initialised = true;
  init_guard.commit();
  return {};
}

AuditDB::~AuditDB() { release_sqlite_handles(); }

bool
AuditDB::is_initialised() const
{
  return initialised;
}

void
AuditDB::init_cephsqlite_vfs(CephContext* cct)
{ // static
  sqlite3_auto_extension((void (*)())sqlite3_cephsqlite_init);
  sqlite3* db = nullptr;
  if (int rc = sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE, nullptr);
      rc == SQLITE_OK) {
    sqlite3_close(db);
  } else {
    std::string err = "could not open sqlite3: " + std::to_string(rc);
    aderr(cct) << err << dendl;
    ceph_abort_msg(err);
  }
}

std::expected<void, AuditDBError>
AuditDB::ensure_audit_pool(
    CephContext* cct,
    librados::Rados& rados,
    bool create_pool)
{
  std::list<std::string> pools;
  int rc = rados.pool_list(pools);
  if (rc < 0) {
    aderr(cct) << "pool_list failed " << cpp_strerror(rc) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::rados_pool_list_failed, rc});
  }

  for (auto& pool : pools) {
    if (pool == AUDIT_POOL_NAME) {
      return {};
    }
  }

  if (create_pool) {
    ceph::bufferlist inbl, outbl;
    std::string outs;

    std::string cmd = R"({"prefix":"osd pool create",)"
                      R"("pool":")" +
                      std::string(AUDIT_POOL_NAME) +
                      R"(",)"
                      R"("pg_num":1,)"
                      R"("pg_num_min":1,)"
                      R"("pg_num_max":32,)"
                      R"("yes_i_really_mean_it":true})";
    rc = rados.mon_command(std::move(cmd), std::move(inbl), &outbl, &outs);
    if (rc < 0) {
      aderr(cct) << "failed to create " << AUDIT_POOL_NAME << " pool: " << outs
                 << " (" << cpp_strerror(rc) << ")" << dendl;
      return std::unexpected(
          AuditDBError{AuditDBErr::audit_pool_create_failed, rc});
    }

    outbl.clear();

    cmd = R"({"prefix":"osd pool application enable",)"
          R"("pool":")" +
          std::string(AUDIT_POOL_NAME) +
          R"(",)"
          R"("app":")" +
          std::string(AUDIT_APP_NAME) +
          R"(",)"
          R"("yes_i_really_mean_it":true})";
    rc = rados.mon_command(std::move(cmd), std::move(inbl), &outbl, &outs);
    if (rc < 0) {
      aderr(cct) << "failed to create application on " << AUDIT_POOL_NAME
                 << " pool: " << outs << " (" << cpp_strerror(rc) << ")"
                 << dendl;
      return std::unexpected(
          AuditDBError{AuditDBErr::audit_pool_app_enable_failed, rc});
    }
    return {};
  }

  return std::unexpected(
      AuditDBError{AuditDBErr::audit_pool_not_found, -ENOENT});
}

std::expected<void, AuditDBError>
AuditDB::delete_db_file(
    CephContext* cct,
    librados::Rados& rados,
    const char* db_path)
{ // static
  sqlite3_vfs* vfs = sqlite3_vfs_find("ceph");
  if (!vfs) {
    aderr(cct) << "could not find ceph vfs" << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::ceph_vfs_not_found, 0});
  }

  if (!vfs->xDelete) {
    aderr(cct) << "ceph vfs has not implemented xDelete, deleting the db file ("
               << db_path << ") is not possible" << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::vfs_xdelete_unsupported, 0});
  }

  if (auto pool_res = ensure_audit_pool(cct, rados, false); !pool_res) {
    aderr(cct) << ".audit pool not found" << dendl;
    return std::unexpected(pool_res.error());
  }

  const int rc = vfs->xDelete(vfs, db_path, 1);
  if (rc != SQLITE_OK) {
    aderr(cct) << "cannot delete " << db_path << " (" << rc << ")" << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::vfs_xdelete_failed, rc});
  }

  adout(cct, 20) << "successfully deleted " << db_path << dendl;
  return {};
}

std::expected<int64_t, AuditDBError>
AuditDB::do_commit(std::optional<int64_t> seq,
                   time_t init_time,
                   const std::string& json_dump)
{
  if (!initialised) {
    return std::unexpected(AuditDBError{AuditDBErr::not_initialised, 0});
  }
  if (seq.has_value() && *seq <= 0) {
    return std::unexpected(AuditDBError{AuditDBErr::invalid_seq, -EINVAL});
  }

  sqlite3_reset(insert_stmt);
  sqlite3_clear_bindings(insert_stmt);

  int idx = 1;
  int rc = 0;
  int64_t result_seq = 0;

  if (seq.has_value()) {
    rc = sqlite3_bind_int64(insert_stmt, idx++, *seq);
    if (rc != SQLITE_OK) goto bind_fail;
  }
  rc = sqlite3_bind_int64(insert_stmt, idx++, static_cast<int64_t>(init_time));
  if (rc != SQLITE_OK) goto bind_fail;
  rc = sqlite3_bind_text(insert_stmt, idx++, json_dump.c_str(), -1, SQLITE_TRANSIENT);
  if (rc != SQLITE_OK) goto bind_fail;

  rc = sqlite3_step(insert_stmt);
  if (rc != SQLITE_DONE) {
    aderr(cct) << "commit insert failed: rc=" << rc
               << " err=" << sqlite3_errmsg(db_handle) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }

  if (sqlite3_changes(db_handle) != 1) {
    if (seq.has_value()) {
      aderr(cct) << "audit log seq: " << *seq << " wasn't added" << dendl;
    } else {
      aderr(cct) << "audit log could not be added" << dendl;
    }
    return std::unexpected(AuditDBError{AuditDBErr::row_count_mismatch, 0});
  }

  result_seq = seq.value_or(sqlite3_last_insert_rowid(db_handle));
  adout(cct, 20) << "seq=" << result_seq << dendl;
  return result_seq;

bind_fail:
  aderr(cct) << "commit bind failed: rc=" << rc
             << " err=" << sqlite3_errmsg(db_handle) << dendl;
  return std::unexpected(AuditDBError{AuditDBErr::sqlite_bind_failed, rc});
}

std::expected<void, AuditDBError>
AuditDB::commit(int64_t seq, time_t init_time, const std::string& json_dump)
{
  std::lock_guard<ceph::mutex> l(lock);

  if (is_standalone) {
    return std::unexpected(
        AuditDBError{AuditDBErr::seq_not_allowed_standalone, -EINVAL});
  }
  if (auto r = do_commit(seq, init_time, json_dump); !r) {
    return std::unexpected(r.error());
  }
  return {};
}

std::expected<int64_t, AuditDBError>
AuditDB::commit(time_t init_time, const std::string& json_dump)
{
  std::lock_guard<ceph::mutex> l(lock);

  if (!is_standalone) {
    return std::unexpected(AuditDBError{AuditDBErr::seq_required, -EINVAL});
  }
  return do_commit(std::nullopt, init_time, json_dump);
}

std::expected<void, AuditDBError>
AuditDB::update(int64_t seq, const std::string& json_dump)
{
  std::lock_guard<ceph::mutex> l(lock);

  if (!initialised) {
    return std::unexpected(AuditDBError{AuditDBErr::not_initialised, 0});
  }
  if (seq <= 0) {
    return std::unexpected(AuditDBError{AuditDBErr::invalid_seq, 0});
  }

  sqlite3_reset(update_stmt);
  sqlite3_clear_bindings(update_stmt);

  int rc = sqlite3_bind_text(update_stmt, 1, json_dump.c_str(), -1, SQLITE_TRANSIENT);
  if (rc != SQLITE_OK) goto bind_fail;
  rc = sqlite3_bind_int64(update_stmt, 2, seq);
  if (rc != SQLITE_OK) goto bind_fail;

  rc = sqlite3_step(update_stmt);
  if (rc != SQLITE_DONE) {
    aderr(cct) << "update failed: rc=" << rc
               << " err=" << sqlite3_errmsg(db_handle) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }

  if (sqlite3_changes(db_handle) != 1) {
    aderr(cct) << "update: seq=" << seq << " not found" << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::row_count_mismatch, 0});
  }

  adout(cct, 20) << "seq=" << seq << dendl;
  return {};

bind_fail:
  aderr(cct) << "update bind failed: rc=" << rc
             << " err=" << sqlite3_errmsg(db_handle) << dendl;
  return std::unexpected(AuditDBError{AuditDBErr::sqlite_bind_failed, rc});
}

std::expected<std::vector<AuditEntry>, AuditDBError>
AuditDB::query(const AuditQuery& q)
{
  std::lock_guard<ceph::mutex> l(lock);

  if (!initialised) {
    return std::unexpected(AuditDBError{AuditDBErr::not_initialised, 0});
  }

  std::vector<AuditEntry> results;

  std::string order_col = is_valid_order_column(q.order_by) ? q.order_by
                                                            : "init_time";
  std::string direction = q.ascending ? "ASC" : "DESC";

  std::vector<std::pair<int, int64_t>> int_binds;
  std::vector<std::pair<int, std::string>> text_binds;
  std::string where = build_where(q, int_binds, text_binds);

  std::string sql =
      "SELECT seq, init_time, json_dump FROM " +
      table_name + where + " ORDER BY " + order_col + " " + direction +
      " LIMIT ?;";

  // limit bind index is after all WHERE binds
  int limit_idx = static_cast<int>(int_binds.size() + text_binds.size()) + 1;

  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    aderr(cct) << "query prepare failed: " << sqlite3_errmsg(db_handle)
               << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_prepare_failed, rc});
  }

  rc = bind_params(stmt, int_binds, text_binds);
  if (rc != SQLITE_OK) {
    aderr(cct) << "query bind failed: " << sqlite3_errmsg(db_handle) << dendl;
    sqlite3_finalize(stmt);
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_bind_failed, rc});
  }

  rc = sqlite3_bind_int64(stmt, limit_idx, q.limit);
  if (rc != SQLITE_OK) {
    aderr(cct) << "query limit bind failed: " << sqlite3_errmsg(db_handle)
               << dendl;
    sqlite3_finalize(stmt);
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_bind_failed, rc});
  }

  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    AuditEntry entry;
    entry.seq       = sqlite3_column_int64(stmt, 0);
    entry.init_time = static_cast<time_t>(sqlite3_column_int64(stmt, 1));
    if (const auto* p = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2))) {
      entry.json_dump = p;
    }
    results.push_back(std::move(entry));
  }

  if (rc != SQLITE_DONE) {
    aderr(cct) << "query step failed: rc=" << rc
               << " err=" << sqlite3_errmsg(db_handle) << dendl;
    sqlite3_finalize(stmt);
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }

  sqlite3_finalize(stmt);
  return results;
}

std::expected<int64_t, AuditDBError>
AuditDB::count(const AuditQuery& q)
{
  std::lock_guard<ceph::mutex> l(lock);

  if (!initialised) {
    return std::unexpected(AuditDBError{AuditDBErr::not_initialised, 0});
  }

  std::vector<std::pair<int, int64_t>> int_binds;
  std::vector<std::pair<int, std::string>> text_binds;
  std::string where = build_where(q, int_binds, text_binds);

  std::string sql = "SELECT COUNT(*) FROM " + table_name + where + ";";

  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    aderr(cct) << "count prepare failed: " << sqlite3_errmsg(db_handle)
               << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_prepare_failed, rc});
  }

  rc = bind_params(stmt, int_binds, text_binds);
  if (rc != SQLITE_OK) {
    aderr(cct) << "count bind failed: " << sqlite3_errmsg(db_handle) << dendl;
    sqlite3_finalize(stmt);
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_bind_failed, rc});
  }

  rc = sqlite3_step(stmt);
  if (rc != SQLITE_ROW) {
    aderr(cct) << "count step failed (expected ROW): rc=" << rc
               << " err=" << sqlite3_errmsg(db_handle) << dendl;
    sqlite3_finalize(stmt);
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }

  const int64_t result = sqlite3_column_int64(stmt, 0);

  rc = sqlite3_step(stmt);
  if (rc != SQLITE_DONE) {
    aderr(cct) << "count step failed (expected DONE): rc=" << rc
               << " err=" << sqlite3_errmsg(db_handle) << dendl;
    sqlite3_finalize(stmt);
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }

  sqlite3_finalize(stmt);
  return result;
}

std::expected<int64_t, AuditDBError>
AuditDB::get_last_committed_seq()
{
  std::lock_guard<ceph::mutex> l(lock);

  if (!initialised) {
    return std::unexpected(AuditDBError{AuditDBErr::not_initialised, 0});
  }

  const std::string sql = "SELECT MAX(seq) FROM " + table_name;
  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    aderr(cct) << "get_last_committed_seq prepare failed: "
               << sqlite3_errmsg(db_handle) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_prepare_failed, rc});
  }

  rc = sqlite3_step(stmt);
  if (rc != SQLITE_ROW) {
    aderr(cct) << "get_last_committed_seq step failed: rc=" << rc
               << " err=" << sqlite3_errmsg(db_handle) << dendl;
    sqlite3_finalize(stmt);
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }

  // on an empty table, "SELECT MAX(seq)" can still yield one SQLITE_ROW but
  // column 0 is SQL NULL
  if (sqlite3_column_type(stmt, 0) == SQLITE_NULL) {
    sqlite3_finalize(stmt);
    return std::unexpected(AuditDBError{AuditDBErr::empty_table, SQLITE_OK});
  }

  const int64_t max_seq = sqlite3_column_int64(stmt, 0);
  sqlite3_finalize(stmt);
  return max_seq;
}

std::expected<int, AuditDBError>
AuditDB::trim(time_t older_than, int64_t max_delete)
{
  std::lock_guard<ceph::mutex> l(lock);

  if (!initialised) {
    return std::unexpected(AuditDBError{AuditDBErr::not_initialised, 0});
  }

  std::string sql;
  // max_delete > 0: apply LIMIT; anything <= 0 deletes all matching rows
  if (max_delete > 0) {
    sql = "DELETE FROM " + table_name + " WHERE seq IN (SELECT seq FROM " +
          table_name + " WHERE init_time < ? ORDER BY seq ASC LIMIT ?);";
  } else {
    sql = "DELETE FROM " + table_name + " WHERE init_time < ?;";
  }

  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    aderr(cct) << "prepare failed: " << sqlite3_errmsg(db_handle) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_prepare_failed, rc});
  }

  rc = sqlite3_bind_int64(stmt, 1, static_cast<int64_t>(older_than));
  if (rc != SQLITE_OK) {
    aderr(cct) << "bind failed: " << sqlite3_errmsg(db_handle) << dendl;
    sqlite3_finalize(stmt);
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_bind_failed, rc});
  }

  if (max_delete > 0) {
    rc = sqlite3_bind_int64(stmt, 2, max_delete);
    if (rc != SQLITE_OK) {
      aderr(cct) << "limit bind failed: " << sqlite3_errmsg(db_handle)
                 << dendl;
      sqlite3_finalize(stmt);
      return std::unexpected(AuditDBError{AuditDBErr::sqlite_bind_failed, rc});
    }
  }

  rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);
  if (rc != SQLITE_DONE) {
    aderr(cct) << "failed: rc=" << rc
               << " err=" << sqlite3_errmsg(db_handle) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }

  int deleted = sqlite3_changes(db_handle);

  return deleted;
}

std::expected<int, AuditDBError>
AuditDB::clear_logs()
{
  std::lock_guard l(lock);

  if (!initialised) {
    return std::unexpected(AuditDBError{AuditDBErr::not_initialised, 0});
  }

  std::string sql = "DELETE FROM " + table_name + ";";
  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    aderr(cct) << "prepare failed: " << sqlite3_errmsg(db_handle) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_prepare_failed, rc});
  }

  rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  if (rc != SQLITE_DONE) {
    aderr(cct) << "failed: rc=" << rc
               << " err=" << sqlite3_errmsg(db_handle) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }

  int deleted = sqlite3_changes(db_handle);
  
  if (deleted > 0) {
    vacuum(cct, db_handle);
  }
  
  return deleted;
}

std::expected<int64_t, AuditDBError>
AuditDB::get_db_size()
{
  std::lock_guard l(lock);

  if (!initialised) {
    return std::unexpected(AuditDBError{AuditDBErr::not_initialised, 0});
  }

  std::string sql = "SELECT (page_count - freelist_count) * page_size"
                    " FROM pragma_page_count(), pragma_freelist_count(),"
                    " pragma_page_size();";
  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    aderr(cct) << "prepare failed: " << sqlite3_errmsg(db_handle) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_prepare_failed, rc});
  }

  rc = sqlite3_step(stmt);
  int64_t db_size = (rc == SQLITE_ROW) ? sqlite3_column_int64(stmt, 0) : 0;
  sqlite3_finalize(stmt);

  if (rc != SQLITE_ROW) {
    aderr(cct) << "step failed: rc=" << rc << " err="
               << sqlite3_errmsg(db_handle) << dendl;
    return std::unexpected(AuditDBError{AuditDBErr::sqlite_step_failed, rc});
  }

  return db_size;
}

std::optional<sqlite3_ptr>
AuditDB::open_db(const char* db_file_or_uri, int flags)
{
  std::lock_guard l(lock);

  sqlite3* db = nullptr;
  int rc = sqlite3_open_v2(db_file_or_uri, &db, flags, "ceph");
  if (rc != SQLITE_OK) {
    const char* err = nullptr;
    if (db) {
      err = sqlite3_errmsg(db);
      sqlite3_close(db);
    } else {
      err = "no db handle";
    }
    aderr(cct) << "failed to open '" << db_file_or_uri << "' in "
               << flag_to_op(flags) << " mode: rc=" << rc << " err=" << err
               << dendl;
    return std::nullopt;
  }
  return sqlite3_ptr(db, &sqlite3_close);
}
