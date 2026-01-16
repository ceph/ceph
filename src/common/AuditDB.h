/**
 * TODO:
 * src/audit/
  ├── AuditDB.h
  ├── AuditDB.cc
  ├── AuditClient.h
  ├── AuditClient.cc
 */

#if defined(__linux__)
#pragma once
#endif

#if !defined(WITH_LIBCEPHSQLITE) 
#error "Audit logging APIs require libcephsqlite (build with -DWITH_LIBCEPHSQLITE=ON)"
#endif

#ifndef AUDIT_DB_H
#define AUDIT_DB_H

#include <optional>
#include <string>
#include <vector>

#include "include/libcephsqlite.h"
#include "include/rados/librados.hpp"
#include "common/errno.h"

inline constexpr auto AUDIT_POOL_NAME = ".audit";
inline constexpr auto AUDIT_APP_NAME = "audit";

using sqlite3_ptr = std::unique_ptr<sqlite3, decltype(&sqlite3_close)>;

struct AuditEntry {
    int64_t seq;
    std::string cmd;
    time_t init_time;
    std::optional<time_t> comp_time;
    std::optional<std::string> status;
    std::optional<int32_t> retval;
};

struct AuditQuery {
    // seq-based filtering
    std::optional<int64_t> before_seq;
    std::optional<int64_t> after_seq;

    // time-based filtering (seconds since epoch)
    std::optional<time_t> since;
    std::optional<time_t> until;

    // status filtering
    std::optional<std::string> status;

    // sorting
    std::string order_by = "init_time";
    bool ascending = false;

    // pagination
    int64_t limit = 100;
};

class AuditDB {
public:

    /**
     * Constructor
     * @param db_file_or_uri Database filename where filename
     * = file:///<*poolid|poolname>:/<dbname>.db?vfs=ceph
     * @param is_uri true if @p db_file_or_uri is a URI
     * @param table_name there is 1:1 db mapping for every entity making use
     * of audit db, so there exists only one table per db
     */
    explicit AuditDB(const char *db_file_or_uri, bool is_uri,
                     std::string table_name);

    ~AuditDB();

    int64_t last_seq{0};

    /**
     * @brief register ceph sqlite3 vfs before creating a db, this is a
     * no-op if it already is registered globally
     */
    static void init_cephsqlite_vfs();

    /**
     * @brief commit - log seq, the command and  command initiation time into
     * table in phase one
     *
     * @param seq log number from mgr/mon
     * @param cmd command executed
     * @param init_time cmd initiation timestamp in seconds since epoch
     *
     * @note this will run an INSERT SQL statement
     */
    bool first_phase_commit(int64_t seq, const std::string& cmd,
        time_t init_time);

    /**
     * @brief commit - command completion time, the status and its returned
     * value as part of phase two
     *
     * @param seq log number to be updated with completion info
     * @param comp_time cmd completion timestamp in seconds since epoch
     * @param status status reported by the daemon
     * @param retval command return value
     *
     * @note this will run an UPDATE SQL statement
     */
    bool second_phase_commit(int64_t seq, time_t comp_time,
        const std::string& status, int32_t retval);

    /**
     * @brief query audit log entries matching the given filters
     *
     * @param q query parameters (filters, sorting, limit)
     * @return matching entries ordered as specified
     */
    std::vector<AuditEntry> query(const AuditQuery& q);

    /**
     * @brief count audit log entries matching the given filters
     *
     * @param q query parameters (filters only, sorting/limit ignored)
     * @return number of matching entries
     */
    int64_t count(const AuditQuery& q);

    /**
     * @brief delete audit entries older than the given timestamp and
     * reclaim disk space via incremental vacuum
     *
     * @param older_than entries with init_time before this are deleted
     * @param max_delete max rows to delete (-1 for unlimited)
     * @return number of rows deleted, or -1 on error
     */
    int64_t trim(time_t older_than, int64_t max_delete = -1);

    /**
     * @brief ensure the .audit RADOS pool exists, if not create it via
     * mon command if needed.
     * 
     * @param rados an intialised and connected rados handle
     * @param create_pool allow creating the .audit pool if it doesn't exist
     * @return 0 on success (i.e. pool exists or was created), else ENOENT.
     * 
     * @note intended only for standalone tools, in cases when the cluster
     * wide setting is to not enable the audit pool, meaning auditing is
     * being done only for standalone tools.
     */
    static int ensure_audit_pool(librados::Rados &rados, bool create_pool = false);

    /**
     * @brief allows deleting the audit db file residing inside .audit pool
     * 
     * @param rados an intialised and connected rados handle to search for
     * .audit pool before commencing the db file deletion.
     * @param db_uri file:///.audit:/<dbname>.db?vfs=ceph
     * @return SQLITE_OK (0) if deletion is successful, SQLITE_NOTFOUND (12)
     * if the path does not exist, SQLITE_IOERR (10) for I/O failure and
     * SQLITE_IOERR_DELETE (2570) if the deletion failed.
     */
    static int delete_db_file(librados::Rados &rados, const char* db_uri);

private:
    mutable ceph::mutex lock = ceph::make_mutex("AuditDB::lock");
    std::string db_file;
    std::string table_name;
    sqlite3* db_handle = nullptr;
    sqlite3_stmt *insert_stmt = nullptr;
    sqlite3_stmt *update_stmt = nullptr;
    int flags{0};

    /**
     * @brief open a db connection.
     *
     * @param db_file_or_uri Database filename where filename
     * = file:///<*poolid|poolname>:/<dbname>.db?vfs=ceph
     * @param flags SQLITE_OPEN_READONLY or SQLITE_OPEN_READWRITE mode
     *
     * @note nothing should directly call this function
     */
    std::optional<sqlite3_ptr> open_db(const char *db_file_or_uri, int flags);
};

#endif
