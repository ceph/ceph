#include <mutex>
#include <optional>
#include <string>

#include "common/debug.h"
#include "common/ceph_mutex.h"

#include "audit_logging.h"
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
        "PRAGMA foreign_keys = 1"
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

void AuditDB::init_cephsqlite_vfs() {
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
