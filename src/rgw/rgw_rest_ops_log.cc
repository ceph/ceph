// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

#include "include/types.h"
#include "rgw_string.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_ops_log.h"
#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

int RGWRestOpsLog::verify_permission()
{
  return check_caps(s->user->caps);
}

void RGWRestOpsLog::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

void RGWGetOpsLog::execute() {

  RESTArgs::get_string(s, "bucket", "", &bucket);
  RESTArgs::get_string(s, "bucket_id", "", &bucket_id);
  RESTArgs::get_string(s, "date", "", &date);

  if (date.empty()) {
    op_ret = -EINVAL;
    std::stringstream message;
    message << "date expected";
    s->err.message = message.str();
    return;
  }

  if (!bucket.empty() && date.size() != 13) {
    op_ret = -EINVAL;
    std::stringstream message;
    message << "bad date format for '" << date << "', expect YYYY-MM-DD-HH";
    s->err.message = message.str();
    return;
  }

  if (!bucket.empty()) {
    RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
    int ret = store->get_bucket_info(obj_ctx, s->bucket_tenant,
                                     bucket, s->bucket_info, NULL,
                                     &s->bucket_attrs);
    if (ret < 0)
      op_ret = -ERR_NO_SUCH_BUCKET;

    oid = date;
    oid += "-";
    oid += s->bucket_info.bucket.bucket_id;
    oid += "-";
    oid += bucket;

    bufferlist bl;
    ret = rgw_get_system_obj(store, obj_ctx, store->get_zone_params().log_pool, oid, bl, NULL, NULL);
    if (ret < 0)
      op_ret = -ERR_NOT_FOUND;
  }
}
void RGWGetOpsLog::send_response()
{
  if (op_ret < 0) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
  if (op_ret < 0)
    return;

  RGWAccessHandle h;
  Formatter *formatter = s->formatter;
  if (bucket.empty() && !date.empty()) {
    //filter by date
    formatter->open_array_section("logs");
    int r = store->log_list_init(date, &h);
    if (r != -ENOENT) {
      while (true) {
        string name;
        int r = store->log_list_next(h, &name);
        if (r == -ENOENT)
          break;
        formatter->dump_string("object", name);
      }
    }
    formatter->close_section();
    rgw_flush_formatter_and_reset(s, formatter);
    return;
  }

  int r = store->log_show_init(oid, &h);

  formatter->open_object_section("log");   //log
  struct rgw_log_entry entry;
  r = store->log_show_next(h, &entry);
  formatter->dump_string("bucket_id", entry.bucket_id);
  formatter->dump_string("bucket_owner", entry.bucket_owner.to_str());
  formatter->dump_string("bucket", entry.bucket);

  uint64_t agg_time = 0;
  uint64_t agg_bytes_sent = 0;
  uint64_t agg_bytes_received = 0;
  uint64_t total_entries = 0;

  formatter->open_array_section("log_entries");

  do {
    using namespace std::chrono;
    uint64_t total_time = duration_cast<milliseconds>(entry.total_time).count();
    agg_time += total_time;
    agg_bytes_sent += entry.bytes_sent;
    agg_bytes_received += entry.bytes_received;
    total_entries++;
    rgw_format_ops_log_entry(entry, formatter);
    r = store->log_show_next(h, &entry);
  } while (r > 0);

  formatter->close_section();                //log_entries

  formatter->open_object_section("log_sum");
  formatter->dump_int("bytes_sent", agg_bytes_sent);
  formatter->dump_int("bytes_received", agg_bytes_received);
  formatter->dump_int("total_time", agg_time);
  formatter->dump_int("total_entries", total_entries);
  formatter->close_section();                //log_sum
  formatter->close_section();                //log
  rgw_flush_formatter_and_reset(s, formatter);
}

int RGWGetOpsLog::check_caps(RGWUserCaps& caps)
{
  return caps.check_cap("opslog", RGW_CAP_READ);
}

void RGWDeleteOpsLog::execute() {
  RESTArgs::get_string(s, "bucket", "", &bucket);
  RESTArgs::get_string(s, "bucket_id", "", &bucket_id);
  RESTArgs::get_string(s, "date", "", &date);

  if (date.empty()) {
    op_ret = -EINVAL;
    std::stringstream message;
    message << "date expected";
    s->err.message = message.str();
    return;
  }

  if (!bucket.empty() && date.size() != 13) {
    op_ret = -EINVAL;
    std::stringstream message;
    message << "bad date format for '" << date << "', expect YYYY-MM-DD-HH";
    s->err.message = message.str();
    return;
  }

  if (!bucket.empty()) {
    RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
    int ret = store->get_bucket_info(obj_ctx, s->bucket_tenant,
                                     bucket, s->bucket_info, NULL,
                                     &s->bucket_attrs);
    if (ret < 0)
      op_ret = -ERR_NO_SUCH_BUCKET;
  }

  string oid;
  oid = date;
  oid += "-";
  oid += s->bucket_info.bucket.bucket_id;
  oid += "-";
  oid += bucket;
  int r = store->log_remove(oid);
  op_ret = STATUS_NO_CONTENT;
  if (r < 0)
    op_ret = -ERR_INTERNAL_ERROR;
}

void RGWDeleteOpsLog::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWDeleteOpsLog::check_caps(RGWUserCaps& caps)
{
  return caps.check_cap("opslog", RGW_CAP_WRITE);
}

RGWOp *RGWHandler_OpsLog::op_get()
{
  return new RGWGetOpsLog;
}

RGWOp *RGWHandler_OpsLog::op_delete()
{
  return new RGWDeleteOpsLog;
}