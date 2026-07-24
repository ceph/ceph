#if defined(__linux__)
#pragma once
#endif

#ifndef TOOLS_AUDIT_LOGGER_H
#define TOOLS_AUDIT_LOGGER_H

#include <ctime>
#include <cstdint>
#include <expected>
#include <memory>
#include <string>

#include "common/AuditDB.h"

/**
 * Helper for cephfs standalone tools: ensure `.audit` pool, open an audit DB in
 * standalone mode (SQLite-assigned seq), and pair begin/end commits.
 *
 * If @ref log_begin succeeded and @ref log_end was not called, the destructor
 * records a second-phase row with status `"aborted"` (process exit without
 * completion).
 *
 * The caller must keep @p rados connected for the lifetime of this object.
 * 
 * @note best-effort logging; tools should be prepared to skip audit on failure.
 */
class ToolsAuditLogger {
public:

  /*
   * Typical usage in a cephfs DR tool's main():
   *
   *   librados::Rados rados;
   *   rados.init_with_context(g_ceph_context);
   *   rados.connect();
   *
   *   auto logger_r = ToolsAuditLogger::create_for_tool(g_ceph_context, rados,
   *       "cephfs_data_scan");
   *   if (!logger_r) {
   *       // log warning; proceed without audit
   *   }
   *   auto& logger = *logger_r;  // unique_ptr<ToolsAuditLogger>
   *
   *   logger->log_begin(argv[0], joined_args, std::time(nullptr));
   *   int rc = run_tool();
   *   logger->log_end(std::time(nullptr), rc == 0 ? "completed" : "failed", rc);
   */

  /**
   * Ensure the .audit pool exists and create+open the audit DB in standalone
   * mode (SQLite-allocated seq). The cephsqlite VFS is initialised lazily
   * on the first call (process-wide).
   */
  static std::expected<std::unique_ptr<ToolsAuditLogger>, AuditDBError> create(
      CephContext* cct,
      librados::Rados& rados,
      std::string db_uri,
      std::string table_name);

  static std::expected<std::unique_ptr<ToolsAuditLogger>, AuditDBError> create_for_tool(
      CephContext* cct,
      librados::Rados& rados,
      std::string table_name);

  /**
   * A basic helper that takes a vector of C style strings and returns a string
   * which can be inserted directly as the cmd_args arg in log_end()
   * 
   * @note constructs string with args separated by spaces, this is enough since
   * the pythonic auditman module can scrap the data using its advaced set of
   * tools to represent data in a structured form
   */
  static std::string get_audit_cmd_args(const std::vector<const char*>& args);

  ~ToolsAuditLogger();

  ToolsAuditLogger(const ToolsAuditLogger&) = delete;
  ToolsAuditLogger& operator=(const ToolsAuditLogger&) = delete;

  /** Standalone first phase; stores seq for @ref log_end. Ignored if a begin is
   * already in flight without a matching @ref log_end. */
  void log_begin(
      const std::string& cmd,
      const std::string& cmd_args,
      time_t init_time);

  /** Second phase for the seq returned from the last @ref log_begin. */
  void log_end(time_t comp_time, const std::string& status, int32_t retval);

  bool is_ready() const;

private:
  ToolsAuditLogger(
      CephContext* cct,
      std::string db_uri,
      std::string table_name,
      std::unique_ptr<AuditDB> db);

  CephContext* const cct;
  std::string db_uri;
  std::string table_name;
  std::unique_ptr<AuditDB> db;
  int64_t seq_in_flight{0};
  bool begin_recorded{false};
};

#endif