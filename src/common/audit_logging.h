#if defined(__linux__)
#pragma once
#endif

#if !defined(WITH_LIBCEPHSQLITE) 
#error "Audit logging APIs require libcephsqlite (build with -DWITH_LIBCEPHSQLITE=ON)"
#endif

#ifndef AUDIT_LOGGING_H
#define AUDIT_LOGGING_H
#endif

#include <optional>

#include "include/libcephsqlite.h"

using sqlite3_ptr = std::unique_ptr<sqlite3, decltype(&sqlite3_close)>;

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
     * @brief ceph sqlite3 vfs before creating a db, this is a no-op if
     * it already is registered globally
     */
    void init_cephsqlite_vfs();
    
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
