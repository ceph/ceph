#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/debug.h"
#include "common/ceph_mutex.h"

#include "AuditDB.h"
#include <sqlite3.h>


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_audit_logging
#undef dout_prefix
#define dout_prefix *_dout << "audit_logging: " << __func__ << ": "

namespace {
    std::once_flag sqlite_vfs_init_flag;

    inline const char* flag_to_op(int flags) { 
        if (flags & SQLITE_OPEN_READONLY) return "read";
        if ((flags & SQLITE_OPEN_READWRITE) && 
            (flags & SQLITE_OPEN_CREATE)) return "create";
        if (flags & SQLITE_OPEN_READWRITE) return "readwrite";
        return "unknown";
    }

    // allowed columns for ORDER BY to prevent SQL injection
    inline bool is_valid_order_column(const std::string& col) {
        return col == "seq" || col == "init_time" || col == "comp_time"
            || col == "status" || col == "retval";
    }
}

AuditDB::AuditDB(const char *db_file_or_uri, bool is_uri,
                 std::string table_name): db_file(db_file_or_uri),
                 table_name(table_name) {
    
    // ensure ceph vfs is initialized
    std::call_once(sqlite_vfs_init_flag, init_cephsqlite_vfs);

    // SQLITE_OPEN_READWRITE is mandatory while creating a db
    if (is_uri) {
        flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI;
    } else {
        flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
    }

    // create db
    int rc = sqlite3_open_v2(db_file_or_uri, &db_handle, flags, "ceph");
    if (rc != SQLITE_OK) {
        const char* err = nullptr;
        if (db_handle) {
            err = sqlite3_errmsg(db_handle);
            sqlite3_close(db_handle);
        } else {
            err = "no db handle";
        }
        derr << "failed to create '" << db_file_or_uri << "': rc=" << rc
             << " err=" << err << dendl;
        ceph_abort();
    }

    // set a timeout for SQLITE_BUSY
    sqlite3_busy_timeout(db_handle, 50); // 50ms

    // setup the db
    static const char *k_pragma[] = {
        // TODO: might have to implement a retry logic for SQLITE_BUSY errors(?)
        "PRAGMA page_size = 65536",
        "PRAGMA cache_size = 4096",
        "PRAGMA journal_mode = PERSIST",
        // might be better to limit this since PERSIST mode is an optization 
        // not a requirement and it is a soft limit for an inactive trx journal
        "PRAGMA journal_size_limit = 1048576",
        // would make some queries quicker to execute like ORDER_BY...
        "PRAGMA temp_store = memory",
        // link up tables from say mon table and mgr table?
        "PRAGMA foreign_keys = 1",
        // enable incremental auto-vacuum to reclaim space after trimming
        "PRAGMA auto_vacuum = INCREMENTAL"
    };
    char *err_msg = nullptr;
    for (auto pragma: k_pragma) {
        rc = sqlite3_exec(db_handle, pragma, nullptr, nullptr, &err_msg);
        if (rc != SQLITE_OK) {
            derr << "error while setting '" << pragma << "': "
                 << (err_msg ? err_msg : "unknown sqlite error") << dendl;
            sqlite3_free(err_msg);
            err_msg = nullptr;
            ceph_abort();
        }
    }

    // schema versioning for future migrations
    static const char *k_schema_version_sql =
        "CREATE TABLE IF NOT EXISTS schema_version ("
        "version INTEGER PRIMARY KEY"
        ");"
        "INSERT OR IGNORE INTO schema_version VALUES (1);";
    rc = sqlite3_exec(db_handle, k_schema_version_sql, nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        derr << "cannot create schema_version table: "
             << (err_msg ? err_msg : "unknown sqlite error") << dendl;
        sqlite3_free(err_msg);
        err_msg = nullptr;
        ceph_abort();
    }

    // default table schema
    const std::string TABLE = "CREATE TABLE IF NOT EXISTS " + table_name + " ("
    // first-phase commit
    "seq INTEGER PRIMARY KEY, "
    "cmd TEXT NOT NULL, "
    "init_time INTEGER NOT NULL, "
    // second-phase commit therefore nullable until completion
    "comp_time INTEGER, "
    "status TEXT, "
    "retval INTEGER, "
    "CHECK (init_time >= 0 AND (comp_time IS NULL OR comp_time >= 0)"
    ");";
    // create table
    rc = sqlite3_exec(db_handle, TABLE.c_str(), nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        derr << "cannot create '" << table_name << "': "
             << (err_msg ? err_msg : "unknown sqlite error") << dendl;
        sqlite3_free(err_msg);
        err_msg = nullptr;
        ceph_abort();
    }

    // performance optimization for WHERE/ORDER BY queries
    const std::string INDEXES = 
    "CREATE INDEX IF NOT EXISTS idx_audit_init_time ON " + table_name+"(init_time);"
    "CREATE INDEX IF NOT EXISTS idx_audit_comp_time ON " + table_name+"(comp_time);"
    "CREATE INDEX IF NOT EXISTS idx_audit_status ON " + table_name+"(status);"
    "CREATE INDEX IF NOT EXISTS idx_audit_retval ON " + table_name+"(retval);";
    rc = sqlite3_exec(db_handle, INDEXES.c_str(), nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        derr << "error while setting indexes: " << (
            err_msg ? err_msg : "unknown sqlite error") << dendl;
        sqlite3_free(err_msg);
        err_msg = nullptr;
        ceph_abort();
    }

    // prepare phase-one statement
    const std::string INSERT = "INSERT INTO " + table_name + " (seq, cmd, init_time) "
    "VALUES (?, ?, ?);";
    rc = sqlite3_prepare_v2(db_handle, INSERT.c_str(), -1, &insert_stmt, nullptr);
    if (rc != SQLITE_OK) {
        derr << "insert_stmt prepare failed: " << sqlite3_errmsg(db_handle) << dendl;
        ceph_abort();
    }

    // prepare phase-two statement
    const std::string UPDATE = "UPDATE " + table_name + " SET "
    "comp_time = ?, status = ?, retval = ? "
    "WHERE seq = ?;";
    rc = sqlite3_prepare_v2(db_handle, UPDATE.c_str(), -1, &update_stmt, nullptr);
    if (rc != SQLITE_OK) {
        derr << "update_stmt prepare failed: " << sqlite3_errmsg(db_handle) << dendl;
        ceph_abort();
    }
}

AuditDB::~AuditDB() {
    if (insert_stmt) {
        sqlite3_finalize(insert_stmt);
    }
    if (update_stmt) {
        sqlite3_finalize(update_stmt);
    }
    if (db_handle) {
        sqlite3_close(db_handle);
    }
}

void AuditDB::init_cephsqlite_vfs() {  // static
    sqlite3_auto_extension((void (*)())sqlite3_cephsqlite_init);
    sqlite3* db = nullptr;
    if (int rc = sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE,
                                 nullptr);
        rc == SQLITE_OK) {
        sqlite3_close(db);
    } else {
        std::string err = "could not open sqlite3: " + std::to_string(rc);
        derr << err << dendl;
        ceph_abort_msg(err);
    }
}

int AuditDB::ensure_audit_pool(librados::Rados &rados, bool create_pool) {  // static
    std::list<std::string> pools;
    int rc = rados.pool_list(pools);
    if (rc < 0) {
        derr << "pool_list failed " << cpp_strerror(r) << dendl;
        return rc;
    }

    for (auto& pool: pools) {
        if (pool == AUDIT_POOL_NAME) {
            return 0;
        }
    }

    if (create_pool) {
        ceph::bufferlist inbl, outbl;
        std::string outs;
        
        std::string cmd = R"({"prefix":"osd pool create",)"
                R"("pool":")" + std::string(AUDIT_POOL_NAME) + R"(",)"
                R"("pg_num":1,)"
                R"("pg_num_min":1,)"
                R"("pg_num_max":32,)"
                R"("yes_i_really_mean_it":true})";
        rc = rados.mon_command(std::move(cmd), std::move(inbl), &outbl, &outs);
        if (rc < 0) {
            derr << "failed to create " << AUDIT_POOL_NAME << " pool: " << outs 
            << " (" << cpp_strerror(r) << ")" << dendl;
            return rc;
        }

        inbl.clear();
        outbl.clear();

        cmd = R"({"prefix":"osd pool application enable",)"
        R"("pool":")" + std::string(AUDIT_POOL_NAME) + R"(",)"
        R"("app":")" + std::string(AUDIT_APP_NAME) + R"(",)"
        R"("yes_i_really_mean_it":true})";
        rc = rados.mon_command(std::move(cmd), std::move(inbl), &outbl, &outs);
        if (rc < 0) {
            derr << "failed to create application on " << AUDIT_POOL_NAME
            << " pool: " << outs << " (" << cpp_strerror(r) << ")" << dendl;
            return rc;
        }
        return 0;
    }

    return -ENOENT;
}

int AuditDB::delete_db_file(librados::Rados &rados, const char* db_uri) {  // static
    sqlite3_vfs* vfs = sqlite3_vfs_find("ceph");
    if (!vfs) {
        derr << "could not find ceph vfs" << dendl;
        return -ENOENT;
    }

    if (!vfs->xDelete) {
        derr << "ceph vfs has not implemented xDelete, deleting the db file ("
        << db_uri << ") is not possible" << dendl;
        return -EPERM;
    }

    if (int rc = ensure_audit_pool(rados); rc < 0) {
        derr << ".audit pool not found" << dendl;
        return rc;
    }

    int rc = vfs->xDelete(vfs, db_uri, 1);
    if (rc == SQLITE_OK) {
        dout(10) << "successfully deleted " << db_uri << dendl;
    } else {
        dout(10) << "cannot delete " << db_uri << " (" << rc << ")" << dendl;
    }

    return rc;
}

std::optional<sqlite3_ptr> AuditDB::open_db(const char *db_file_or_uri,
                                            int flags) {
    std::lock_guard<ceph::mutex> l(lock);

    sqlite3 *db = nullptr;
    int rc = sqlite3_open_v2(db_file_or_uri, &db, flags, "ceph");
    if (rc != SQLITE_OK) {
        const char* err = nullptr;
        if (db) {
            err = sqlite3_errmsg(db);
            sqlite3_close(db);
        } else {
            err = "no db handle";
        }
        derr << "failed to open '" << db_file_or_uri << "' in "
             << flag_to_op(flags) << " mode: rc=" << rc << " err="
             << err << dendl;
        return std::nullopt;
    }
    return sqlite3_ptr(db, &sqlite3_close);
}

// NOTE: SQLITE_BUSY might arise at sqlite3_step(), need to ensure multiple retries at client side

bool AuditDB::first_phase_commit(int64_t seq, const std::string& cmd, time_t init_time) {
    std::lock_guard<ceph::mutex> l(lock);

    // TODO:: handle SQLITE_BUSY!!
    sqlite3_reset(insert_stmt);
    sqlite3_clear_bindings(insert_stmt);

    last_seq = seq;

    int rc = sqlite3_bind_int64(insert_stmt, 1, seq);
    if (rc != SQLITE_OK) goto bind_fail;
    rc = sqlite3_bind_text(insert_stmt, 2, cmd.c_str(), -1, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) goto bind_fail;
    rc = sqlite3_bind_int64(insert_stmt, 3, init_time);
    if (rc != SQLITE_OK) goto bind_fail;

    rc = sqlite3_step(insert_stmt);
    if (rc != SQLITE_DONE) {
        derr << "first_phase_commit insert failed: rc=" << rc << " err="
             << sqlite3_errmsg(db_handle) << dendl;
        return false;
    }

    // ensure the changes were reflected
    if (sqlite3_changes(db_handle) != 1) {
        derr << "audit log seq: " << seq << " wasn't added" << dendl;
        return false;
    }

    return true;

bind_fail:
    derr << "first_phase_commit bind failed: rc=" << rc << " err="
         << sqlite3_errmsg(db_handle) << dendl;
    return false;
}

bool AuditDB::second_phase_commit(int64_t seq, time_t comp_time, const std::string& status, int32_t retval) {
    std::lock_guard<ceph::mutex> l(lock);

    // TODO:: handle SQLITE_BUSY!!
    sqlite3_reset(update_stmt);
    sqlite3_clear_bindings(update_stmt);

    if (seq != last_seq) {
        // something horrible happened
        derr << "seq mismatch for second phase commit is not allowed" << dendl;
        ceph_abort();
    }

    int rc = sqlite3_bind_int64(update_stmt, 1, comp_time);
    if (rc != SQLITE_OK) goto bind_fail;
    rc = sqlite3_bind_text(update_stmt, 2, status.c_str(), -1, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) goto bind_fail;
    rc = sqlite3_bind_int(update_stmt, 3, retval);
    if (rc != SQLITE_OK) goto bind_fail;
    rc = sqlite3_bind_int64(update_stmt, 4, seq);
    if (rc != SQLITE_OK) goto bind_fail;

    rc = sqlite3_step(update_stmt);
    if (rc != SQLITE_DONE) {
        derr << "second_phase_commit insert failed: rc=" << rc << " err="
             << sqlite3_errmsg(db_handle) << dendl;
        return false;
    }

    // ensure the changes were reflected
    if (sqlite3_changes(db_handle) != 1) {
        derr << "audit log seq: " << seq << " didn't update" << dendl;
        return false;
    }

    return true;

bind_fail:
    derr << "second_phase_commit bind failed: rc=" << rc << " err="
         << sqlite3_errmsg(db_handle) << dendl;
    return false;
}

// builds WHERE clause and returns bind values for query/count
static std::string build_where(const AuditQuery& q,
                                std::vector<std::pair<int, int64_t>>& int_binds,
                                std::vector<std::pair<int, std::string>>& text_binds) {
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
        add_condition("init_time <= ?");
        int_binds.push_back({bind_idx++, static_cast<int64_t>(*q.until)});
    }
    if (q.status) {
        add_condition("status = ?");
        text_binds.push_back({bind_idx++, *q.status});
    }

    return where;
}

static int bind_params(sqlite3_stmt* stmt,
                       const std::vector<std::pair<int, int64_t>>& int_binds,
                       const std::vector<std::pair<int, std::string>>& text_binds) {
    for (auto& [idx, val] : int_binds) {
        int rc = sqlite3_bind_int64(stmt, idx, val);
        if (rc != SQLITE_OK) return rc;
    }
    for (auto& [idx, val] : text_binds) {
        int rc = sqlite3_bind_text(stmt, idx, val.c_str(), -1, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) return rc;
    }
    return SQLITE_OK;
}

std::vector<AuditEntry> AuditDB::query(const AuditQuery& q) {
    std::lock_guard<ceph::mutex> l(lock);
    std::vector<AuditEntry> results;

    std::string order_col = is_valid_order_column(q.order_by) ? q.order_by : "init_time";
    std::string direction = q.ascending ? "ASC" : "DESC";

    std::vector<std::pair<int, int64_t>> int_binds;
    std::vector<std::pair<int, std::string>> text_binds;
    std::string where = build_where(q, int_binds, text_binds);

    std::string sql = "SELECT seq, cmd, init_time, comp_time, status, retval FROM "
        + table_name + where
        + " ORDER BY " + order_col + " " + direction
        + " LIMIT ?;";

    // limit bind index is after all WHERE binds
    int limit_idx = static_cast<int>(int_binds.size() + text_binds.size()) + 1;

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        derr << "query prepare failed: " << sqlite3_errmsg(db_handle) << dendl;
        return results;
    }

    rc = bind_params(stmt, int_binds, text_binds);
    if (rc != SQLITE_OK) {
        derr << "query bind failed: " << sqlite3_errmsg(db_handle) << dendl;
        sqlite3_finalize(stmt);
        return results;
    }

    rc = sqlite3_bind_int64(stmt, limit_idx, q.limit);
    if (rc != SQLITE_OK) {
        derr << "query limit bind failed: " << sqlite3_errmsg(db_handle) << dendl;
        sqlite3_finalize(stmt);
        return results;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        AuditEntry entry;
        entry.seq = sqlite3_column_int64(stmt, 0);
        entry.cmd = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        entry.init_time = static_cast<time_t>(sqlite3_column_int64(stmt, 2));

        if (sqlite3_column_type(stmt, 3) != SQLITE_NULL) {
            entry.comp_time = static_cast<time_t>(sqlite3_column_int64(stmt, 3));
        }
        if (sqlite3_column_type(stmt, 4) != SQLITE_NULL) {
            entry.status = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4));
        }
        if (sqlite3_column_type(stmt, 5) != SQLITE_NULL) {
            entry.retval = sqlite3_column_int(stmt, 5);
        }

        results.push_back(std::move(entry));
    }

    if (rc != SQLITE_DONE) {
        derr << "query step failed: rc=" << rc << " err="
             << sqlite3_errmsg(db_handle) << dendl;
    }

    sqlite3_finalize(stmt);
    return results;
}

int64_t AuditDB::count(const AuditQuery& q) {
    std::lock_guard<ceph::mutex> l(lock);

    std::vector<std::pair<int, int64_t>> int_binds;
    std::vector<std::pair<int, std::string>> text_binds;
    std::string where = build_where(q, int_binds, text_binds);

    std::string sql = "SELECT COUNT(*) FROM " + table_name + where + ";";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        derr << "count prepare failed: " << sqlite3_errmsg(db_handle) << dendl;
        return -1;
    }

    rc = bind_params(stmt, int_binds, text_binds);
    if (rc != SQLITE_OK) {
        derr << "count bind failed: " << sqlite3_errmsg(db_handle) << dendl;
        sqlite3_finalize(stmt);
        return -1;
    }

    int64_t result = 0;
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        result = sqlite3_column_int64(stmt, 0);
    } else {
        derr << "count step failed: rc=" << rc << " err="
             << sqlite3_errmsg(db_handle) << dendl;
        result = -1;
    }

    sqlite3_finalize(stmt);
    return result;
}

int64_t AuditDB::trim(time_t older_than, int64_t max_delete) {
    std::lock_guard<ceph::mutex> l(lock);

    std::string sql;
    if (max_delete > 0) {
        sql = "DELETE FROM " + table_name
            + " WHERE seq IN (SELECT seq FROM " + table_name
            + " WHERE init_time < ? ORDER BY seq ASC LIMIT ?);";
    } else {
        sql = "DELETE FROM " + table_name + " WHERE init_time < ?;";
    }

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_handle, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        derr << "trim prepare failed: " << sqlite3_errmsg(db_handle) << dendl;
        return -1;
    }

    rc = sqlite3_bind_int64(stmt, 1, static_cast<int64_t>(older_than));
    if (rc != SQLITE_OK) {
        derr << "trim bind failed: " << sqlite3_errmsg(db_handle) << dendl;
        sqlite3_finalize(stmt);
        return -1;
    }

    if (max_delete > 0) {
        rc = sqlite3_bind_int64(stmt, 2, max_delete);
        if (rc != SQLITE_OK) {
            derr << "trim limit bind failed: " << sqlite3_errmsg(db_handle) << dendl;
            sqlite3_finalize(stmt);
            return -1;
        }
    }

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        derr << "trim delete failed: rc=" << rc << " err="
             << sqlite3_errmsg(db_handle) << dendl;
        return -1;
    }

    int64_t deleted = sqlite3_changes(db_handle);

    // reclaim space from deleted rows (uses PRAGMA auto_vacuum = INCREMENTAL)
    if (deleted > 0) {
        char *err_msg = nullptr;
        rc = sqlite3_exec(db_handle, "PRAGMA incremental_vacuum;",
                          nullptr, nullptr, &err_msg);
        if (rc != SQLITE_OK) {
            derr << "incremental_vacuum failed: "
                 << (err_msg ? err_msg : "unknown sqlite error") << dendl;
            sqlite3_free(err_msg);
        }
    }

    return deleted;
}
