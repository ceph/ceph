#include "ToolsAuditLogger.h"

#include <ctime>

#include <fmt/format.h>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "include/ceph_assert.h"
#include "include/compat.h"
#include "include/rados/librados.hpp"

#define dout_subsys ceph_subsys_client
#undef dout_prefix
#define dout_prefix *_dout << "tools_audit_logger: " << __func__ << ": "

std::expected<std::unique_ptr<ToolsAuditLogger>, AuditDBError>
ToolsAuditLogger::create(
    CephContext* cct,
    librados::Rados& rados,
    std::string db_uri,
    std::string table_name)
{
  ceph_assert(cct);
  if (auto p = AuditDB::ensure_audit_pool(cct, rados); !p) {
    return std::unexpected(p.error());
  }

  auto db = std::make_unique<AuditDB>(cct, db_uri.c_str(), true, table_name, true);
  if (auto init_res = db->init(); !init_res) {
    return std::unexpected(init_res.error());
  }

  return std::unique_ptr<ToolsAuditLogger>(new ToolsAuditLogger(
      cct, std::move(db_uri), std::move(table_name), std::move(db)));
}

std::expected<std::unique_ptr<ToolsAuditLogger>, AuditDBError>
ToolsAuditLogger::create_for_tool(
      CephContext* cct,
      librados::Rados& rados,
      std::string table_name)
{
  std::string db_uri = fmt::format("file:///.audit:/{}_audit.db?vfs=ceph", table_name);
  return create(cct, rados, std::move(db_uri), std::move(table_name));
}

std::string ToolsAuditLogger::get_audit_cmd_args(const std::vector<const char*>& args)
{
  std::string result;

  for (size_t i = 0; i < args.size(); ++i) {
    result += args[i];

    if (i < args.size() - 1) {
      result += " ";
    }
  }
  return result;
}

ToolsAuditLogger::ToolsAuditLogger(
    CephContext* cct_,
    std::string db_uri_,
    std::string table_name_,
    std::unique_ptr<AuditDB> db_)
    : cct(cct_),
      db_uri(std::move(db_uri_)),
      table_name(std::move(table_name_)),
      db(std::move(db_))
{
}

ToolsAuditLogger::~ToolsAuditLogger()
{
  if (is_ready() && begin_recorded) {
    // comp_time must satisfy comp_time >= init_time (schema CHECK)
    log_end(std::time(nullptr), "aborted", -ECANCELED);
  }
}

bool ToolsAuditLogger::is_ready() const
{
  return db && db->is_initialised();
}

void ToolsAuditLogger::log_begin(
    const std::string& cmd,
    const std::string& cmd_args,
    time_t init_time)
{
  if (!is_ready()) {
    return;
  }
  if (begin_recorded) {
    lderr(cct) << "called while previous being (seq=" << seq_in_flight << ") still in flight; dropping" << dendl;
    return;
  }
  auto r = db->first_phase_commit(cmd, cmd_args, init_time);
  if (r.has_value()) {
    seq_in_flight = *r;
    begin_recorded = true;
  } else {
    lderr(cct) << "failed: code " << static_cast<int>(r.error().code)
               << " detail=" << r.error().detail << dendl;
  }
}

void ToolsAuditLogger::log_end(
    time_t comp_time,
    const std::string& status,
    int32_t retval)
{
  if (!is_ready() || !begin_recorded) {
    return;
  }
  auto r = db->second_phase_commit(seq_in_flight, comp_time, status, retval);
  if (r.has_value()) {
    begin_recorded = false;
    seq_in_flight = 0;
  } else {
    lderr(cct) << __func__ << "failed: seq=" << seq_in_flight
               << " code=" << static_cast<int>(r.error().code)
               << " detail=" << r.error().detail << dendl;
  }
}
