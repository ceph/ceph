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
 * Both rows share the same JSON schema:
 *   { cmd, cmd_args, init_time, comp_time, status, retval }
 * The first row (from @ref log_begin) stores null for comp_time/status/retval.
 * The second row (from @ref log_end or the destructor abort) fills all fields.
 * Both rows carry the same event_id so they can be queried as a pair.
 *
 * If @ref log_begin succeeded and @ref log_end was not called, the destructor
 * inserts an abort row with status `"aborted"` and retval `-ECANCELED`.
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

  /**
   * Insert the first row for this invocation. JSON payload contains the full
   * schema with comp_time/status/retval as null. The returned event_id is
   * stored internally and passed to @ref log_end so both rows share the same
   * event. Ignored if a begin is already in flight.
   */
  void log_begin(
      const std::string& cmd,
      const std::string& cmd_args,
      time_t init_time);

  /**
   * Insert the completion row for this invocation. JSON payload contains the
   * full schema with all fields populated. Uses the event_id stored by
   * @ref log_begin so both rows belong to the same event.
   */
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
  bool        begin_recorded{false};
  // stored at log_begin time so log_end can pass the same event_id and
  // reconstruct the full JSON payload
  std::string event_id_in_flight;
  std::string cmd_in_flight;
  std::string cmd_args_in_flight;
  time_t      init_time_in_flight{0};
};

#endif