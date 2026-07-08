#if defined(__linux__)
#pragma once
#endif

#if !defined(WITH_LIBCEPHSQLITE)
#error "Audit logging APIs require libcephsqlite (build with -DWITH_LIBCEPHSQLITE=ON)"
#endif

#ifndef AUDIT_DB_H
#define AUDIT_DB_H

#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "include/common_fwd.h"
#include "include/libcephsqlite.h"
#include "common/ceph_mutex.h"
#include "common/errno.h"
#include "include/rados/librados_fwd.hpp"

inline constexpr auto AUDIT_POOL_NAME = ".audit";
inline constexpr auto AUDIT_APP_NAME = "audit";

using sqlite3_ptr = std::unique_ptr<sqlite3, decltype(&sqlite3_close)>;

struct AuditEntry {
  int64_t seq{0};
  std::string cmd;
  std::string cmd_args;
  time_t init_time{0};
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
  bool ascending{false};

  // pagination
  int64_t limit{100};
};

/**
 * High-level failure reason for AuditDB operations that use std::expected.
 */
enum class AuditDBErr {
  not_initialised,
  invalid_seq,
  /** `sqlite3_open_v2` failed (`init` / open path) */
  sqlite_open_failed,
  /** `sqlite3_exec` failed (pragmas, DDL in `init`) */
  sqlite_exec_failed,
  sqlite_prepare_failed,
  sqlite_bind_failed,
  sqlite_step_failed,
  /** SELECT MAX(seq) had no rows / NULL (get_last_committed_seq only) */
  empty_table,
  /** INSERT/UPDATE step returned DONE but `sqlite3_changes` != 1 */
  row_count_mismatch,
  /** `sqlite3_vfs_find("ceph")` returned null (VFS not registered) */
  ceph_vfs_not_found,
  /** Ceph VFS has no `xDelete` implementation */
  vfs_xdelete_unsupported,
  /** `.audit` pool missing and `ensure_audit_pool(..., false)` failed */
  audit_pool_not_found,
  /** Ceph VFS `xDelete` returned non-`SQLITE_OK` */
  vfs_xdelete_failed,
  /** `rados.pool_list` failed */
  rados_pool_list_failed,
  /** `osd pool create` mon command failed */
  audit_pool_create_failed,
  /** `osd pool application enable` mon command failed */
  audit_pool_app_enable_failed,
  /** `table_name` fails validation in @ref AuditDB::init (empty, too long,
     *  characters outside `[A-Za-z0-9_]`, does not start with a letter or
     *  underscore, or equals the reserved name `schema_version`). */
  invalid_table_name,
  seq_not_allowed_standalone,
  seq_required
};

/**
 * Error value for std::expected. @p code classifies the failure; @p detail
 * is an optional raw code whose meaning depends on @p code (e.g. a SQLite
 * `SQLITE_*` result, a negative librados/mon return, `-errno`, or 0 if
 * unused).
 */
struct AuditDBError {
  AuditDBErr code = AuditDBErr::not_initialised;
  int detail = 0;
};

namespace audit_db_detail {
struct InitFailureGuard;
}

class AuditDB {
  friend struct audit_db_detail::InitFailureGuard;

public:
  /**
     * Constructor stores paths and table name only; does not open SQLite.
     *
     * @param cct Ceph context used for logging (must be non-null and must
     * outlive this object for the lifetime of operations that log).
     * @param db_file_or_uri Database filename where filename
     * = file:///<*poolid|poolname>:/<dbname>.db?vfs=ceph
     * @param is_uri true if @p db_file_or_uri is a URI
     * @param table_name there is 1:1 db mapping for every entity making use
     * of audit db, so there exists only one table per db. Validation runs in
     * @ref init; on failure @ref init returns `invalid_table_name`.
     * @param is_standalone this is mostly for setting up the insert_stmt, if it is
     * daemon, the first_phase_commit() will be provided a seq, for standalone
     * tools - sqlite would assign the next available integer
     */
  explicit AuditDB(
      CephContext* cct,
      const char* db_file_or_uri,
      bool is_uri,
      std::string table_name,
      bool is_standalone = false);

  /**
     * @brief Copying is disabled: each instance owns SQLite connection and
     * prepared-statement state; a copy would duplicate or double-release
     * those resources.
     *
     * @sa init, ~AuditDB, is_initialised
     */
  AuditDB(const AuditDB&) = delete;
  /** @copydoc AuditDB(const AuditDB&) */
  AuditDB& operator=(const AuditDB&) = delete;

  /**
     * @brief open the database, configure pragmas, create schema and
     * prepare statements. Must be called after construction and before
     * any commit/query operations.
     *
     * On failure the database handle is closed, prepared statements are
     * finalized, and the object remains uninitialised; other operations
     * return `not_initialised` without effect.
     */
  std::expected<void, AuditDBError> init();

  ~AuditDB();

  /**
     * @brief true after a successful @ref init; false before @ref init or
     * after a failed @ref init.
     */
  bool is_initialised() const;

  /**
     * @brief register ceph sqlite3 vfs before creating a db, this is a
     * no-op if it already is registered globally
     */
  static void init_cephsqlite_vfs(CephContext* cct);

  /**
     * @brief ensure the `.audit` RADOS pool exists; optionally create and
     * appify it via mon commands.
     *
     * @param rados an initialised and connected librados handle
     * @param create_pool if true, create the pool and enable the `audit`
     * application when missing; if false, only check existence (failure if
     * the pool is absent).
     *
     * @note intended for standalone tools when the cluster does not create
     * the pool; the mgr normally ensures the pool exists.
     */
  static std::expected<void, AuditDBError> ensure_audit_pool(
      CephContext* cct,
      librados::Rados& rados,
      bool create_pool = true);

  /**
     * @brief delete an audit DB file in the `.audit` pool via the Ceph SQLite
     * VFS.
     *
     * @param cct Ceph context for logging
     * @param rados connected librados handle
     * @param db_path Path in Ceph VFS format: `pool_name:namespace/filename.db`
     *                or `pool_name:/filename.db` (empty namespace).
     *                Example: `.audit:/my_audit.db`
     *
     * @note This function calls the VFS xDelete directly, so it expects a raw
     *       path, NOT a SQLite URI. Do not pass `file:///` prefix or `?vfs=ceph`
     *       suffix.
     *
     * @return void on success, AuditDBError on failure
     */
  static std::expected<void, AuditDBError> delete_db_file(
      CephContext* cct,
      librados::Rados& rados,
      const char* db_path);

      /**
    * @brief Phase-one commit (daemon mode): record cmd/args/init_time keyed by
    * a caller-supplied @p seq.
    *
    * Use this overload on instances constructed with `is_standalone == false`.
    * The daemon provides @p seq from its
    * own counter (e.g. `LogEntry::seq` for the log-audit subscription, or a
    * local monotonic id for mgr-side commands).
    *
    * @param seq monotonic command id; must be > 0 and unique within the
    * table. Uniqueness is enforced by the PRIMARY KEY constraint, not by
    * this method.
    * @param cmd command executed; must be non-empty after trimming.
    * @param cmd_args command arguments (may be empty).
    * @param init_time cmd initiation timestamp in seconds since epoch; must
    * be > 0.
    *
    * @return void on success; on failure @ref AuditDBError with one of:
    * - `not_initialised` — @ref init not called or failed.
    * - `seq_not_allowed_standalone` — called on a standalone instance.
    * - `invalid_seq` — @p seq <= 0.
    * - `sqlite_step_failed` — INSERT failed; `detail` carries the SQLite
    *   extended result code, e.g. `SQLITE_CONSTRAINT_PRIMARYKEY` for a
    *   duplicate seq, `SQLITE_CONSTRAINT_CHECK` for an empty cmd or
    *   non-positive init_time, or `SQLITE_BUSY` under contention.
    * - `sqlite_bind_failed` — parameter binding failed; `detail` carries
    *   the SQLite result code.
    * - `row_count_mismatch` — INSERT returned DONE but no row was added
    *   (should not occur in practice).
    */
   std::expected<void, AuditDBError> first_phase_commit(
      int64_t seq,
      const std::string& cmd,
      const std::string& cmd_args,
      time_t init_time);

   /**
    * @brief Phase-one commit (standalone mode): record cmd/args/init_time
    * with a SQLite-allocated seq, returned to the caller.
    *
    * Use this overload on instances constructed with `is_standalone == true`.
    * Standalone tools (e.g. CephFS DR utilities) cannot rely on a coordinated
    * seq source and must let SQLite assign one via the `seq INTEGER PRIMARY
    * KEY` rowid alias. Concurrent invocations of the same tool against the
    * same DB file are serialized by SQLite's file lock; each invocation
    * receives a unique seq.
    *
    * @param cmd command executed; must be non-empty after trimming.
    * @param cmd_args command arguments (may be empty).
    * @param init_time cmd initiation timestamp in seconds since epoch; must
    * be > 0.
    *
    * @return the seq assigned by SQLite on success (use this value when
    * later calling @ref second_phase_commit). On failure @ref AuditDBError
    * with one of:
    * - `not_initialised` — @ref init not called or failed.
    * - `seq_required` — called on a daemon instance (use the int64_t
    *   overload instead).
    * - `sqlite_step_failed` — INSERT failed; `detail` carries the SQLite
    *   extended result code (e.g. `SQLITE_CONSTRAINT_CHECK`,
    *   `SQLITE_BUSY`).
    * - `sqlite_bind_failed` — parameter binding failed; `detail` carries
    *   the SQLite result code.
    * - `row_count_mismatch` — INSERT returned DONE but no row was added.
    */
   std::expected<int64_t, AuditDBError> first_phase_commit(
      const std::string& cmd,
      const std::string& cmd_args,
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
     * @note See @ref first_phase_commit regarding `SQLITE_BUSY` / retries.
     */
  std::expected<void, AuditDBError> second_phase_commit(
      int64_t seq,
      time_t comp_time,
      const std::string& status,
      int32_t retval);

  /**
     * @brief query audit log entries matching the given filters
     *
     * @param q query parameters (filters, sorting, limit)
     * @return matching entries on success; on failure `AuditDBError` with
     * `code` and optional `detail` (see `AuditDBError`).
     */
  std::expected<std::vector<AuditEntry>, AuditDBError> query(
      const AuditQuery& q);

  /**
     * @brief count audit log entries matching the given filters
     *
     * @param q query parameters (filters only, sorting/limit ignored)
     * @return count on success; on failure `AuditDBError`.
     */
  std::expected<int64_t, AuditDBError> count(const AuditQuery& q);

  /**
     * @brief largest `seq` currently stored in the audit table (from the
     * database).
     *
     * @return the max sequence on success; on failure or empty table
     * (MAX NULL), `AuditDBError` with `empty_table` and `detail` 0, or other
     * codes on SQLite errors.
     */
  std::expected<int64_t, AuditDBError> get_last_committed_seq();

  /**
     * @brief delete audit entries older than the given timestamp and
     * reclaim disk space via incremental vacuum
     *
     * @param older_than entries with init_time before this are deleted
     * @param max_delete max rows to delete per call; anything <= 0 means no
     * limit on how many rows may be removed. Use > 0 to cap deletions.
     * @return rows deleted on success (including 0); on failure
     * `AuditDBError`.
     */
  std::expected<int, AuditDBError> trim(
      time_t older_than,
      int64_t max_delete = -1);

   /**
     * @brief delete all audit log entries from the table and reclaim disk space
     *
     * @return rows deleted on success (including 0); on failure `AuditDBError`
     * with appropriate code.
     *
     * @warning This operation is irreversible. All audit history will be lost.
     * Use @ref trim for selective deletion based on age.
     *
     * @note Vacuum failure does not cause clear_logs() to fail; it only logs
     * a warning. The entries are still deleted even if vacuum fails.
     */
  std::expected<int, AuditDBError> clear_logs();

    /**
     * @brief return the total size in bytes of all objects
     * (tables and indexes) in the audit database.
     *
     * @return size in bytes on success; on failure `AuditDBError`.
     */
    std::expected<int64_t, AuditDBError> get_db_size();

private:
  CephContext* cct;
  std::string db_file;
  bool is_uri{false};
  std::string table_name;
  bool is_standalone{false};

  ceph::mutex lock = ceph::make_mutex("AuditDB::lock");
  std::atomic<bool> initialised{false};
  sqlite3* db_handle = nullptr;
  sqlite3_stmt* insert_stmt = nullptr;
  sqlite3_stmt* update_stmt = nullptr;

  /**
     * Finalize prepared statements and close the DB handle. Safe when
     * pointers are already null.
     *
     * @sa AuditDB::~AuditDB
     */
  void release_sqlite_handles();

   /**
     * @brief this is a helper for first_phase_commit
     *
     * @param seq (optional) log number from mgr/mon/other daemon
     * @param cmd command executed
     * @param cmd_args command arguments
     * @param init_time cmd initiation timestamp in seconds since epoch
     *
     * @note this will run an INSERT SQL statement
     * @note caller must hold the lock
     */
  std::expected<int64_t, AuditDBError> do_first_phase_commit(
      std::optional<int64_t> seq,
      const std::string& cmd,
      const std::string& cmd_args,
      time_t init_time);

  /**
     * @brief open a db connection.
     *
     * @param db_file_or_uri Database filename where filename
     * = file:///<*poolid|poolname>:/<dbname>.db?vfs=ceph
     * @param flags SQLITE_OPEN_READONLY or SQLITE_OPEN_READWRITE mode
     *
     * @note nothing should directly call this function, this might get useful
     * to retry connecting to the db after some failure or connect to some
     * other db with read-only flags. the exact use case for this is yet to be
     * decided
     */
  std::optional<sqlite3_ptr> open_db(const char* db_file_or_uri, int flags);
};

#endif
