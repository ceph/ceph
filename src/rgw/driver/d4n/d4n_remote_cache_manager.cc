/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#include "d4n_remote_cache_manager.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "common/errno.h"

#include <utility>

namespace rgw::d4n {

// System user (global tenant) whose S3 key signs remote cache requests.
// It must be created by the cluster admin on every RGW in the D4N cluster.
static constexpr std::string_view D4N_SYSTEM_USER = "rgw-d4n-system-user";

std::expected<RGWAccessKey, int> RemoteCacheOp::load_access_key(
    const DoutPrefixProvider *dpp, const optional_yield y)
{
  const std::string uid{D4N_SYSTEM_USER};
  auto user = driver->get_user(rgw_user{uid});
  if (!user) {
    ldpp_dout(dpp, 0) << "D4N could not obtain system user " << uid << dendl;
    return std::unexpected(-ENOENT);
  }
  if (const int ret = user->load_user(dpp, y); ret < 0) {
    ldpp_dout(dpp, 0) << "D4N failed to load system user " << uid
                      << ": " << cpp_strerror(ret) << dendl;
    return std::unexpected(ret);
  }

  const auto& info = user->get_info();
  if (!info.system || info.suspended) {
    ldpp_dout(dpp, 0) << "D4N user " << uid
                      << " must be an unsuspended system user" << dendl;
    return std::unexpected(-EACCES);
  }
  for (const auto& [id, candidate] : info.access_keys) {
    if (candidate.active && !candidate.id.empty() && !candidate.key.empty()) {
      return candidate;
    }
  }
  ldpp_dout(dpp, 0) << "D4N system user " << uid << " has no usable active S3 key" << dendl;
  return std::unexpected(-EACCES);
}

param_vec_t RemoteCacheOp::request_params() const
{
  param_vec_t params;
  if (!op.instance_id.empty()) {
    params.emplace_back("versionId", op.instance_id);
  }
  return params;
}

int RemoteCacheGetOp::send_request(const DoutPrefixProvider* dpp, optional_yield& y, bufferlist* bl)
{
  in_bl.clear();

  auto access_key = load_access_key(dpp, y);
  if (!access_key) {
    return access_key.error();
  }

  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;
  extra_headers["x-rgw-remote-cache-request"] = "true";
  extra_headers["x-rgw-cache-object-version"] = op.version;
  uint64_t end_offset = op.offset + op.len - 1;
  std::string range_val = "bytes=" + std::to_string(op.offset) + "-" + std::to_string(end_offset);
  extra_headers["RANGE"] = std::move(range_val);
  auto params = request_params();

  auto resource = get_resource(op.bucket_name, op.object_name, op.bucket_tenant);
  sender = std::make_unique<RGWRESTStreamRWRequest>(dpp->get_cct(), "GET", op.remote_addr, &cb, nullptr, &params, "", host_style);

  if (const auto ret = sender->send_request(dpp, &*access_key, extra_headers, resource, nullptr, bl); ret < 0) {
    return ret;
  }

  return 0;
}

rgw::AioResultList RemoteCacheGetOp::send_request(const DoutPrefixProvider* dpp, rgw::Aio* aio, uint64_t cost, uint64_t id, optional_yield& y)
{
  in_bl.clear();
  this->aio = aio;

  auto op_func = [this, dpp, y](Aio* aio, AioResult& r) mutable {
    this->r = &r;
    const int ret = send_request(dpp, y);
    if (ret < 0) {
      sender.reset();
      this->r = nullptr;
      r.result = ret;
      aio->put(r);
      return;
    }
  };

  return this->aio->get(rgw_raw_obj{}, std::move(op_func), cost, id);
}

int RemoteCacheGetOp::complete_request(const DoutPrefixProvider* dpp, optional_yield& y)
{
  if (!sender) {
    return -EINVAL;
  }

  int ret = sender->complete_request(dpp, y);
  sender.reset();

  if (this->aio && r) {
    auto* result = std::exchange(r, nullptr);
    result->result = ret;
    ldpp_dout(dpp, 20) << "RemoteCacheGetOp:: " << __func__ <<  " length of buffer received: " << in_bl.length() << dendl;
    result->data = std::move(in_bl);
    this->aio->put(*result);
  }
  return ret;
}

rgw::AioResultList RemoteCacheGetOp::send_and_complete_request(const DoutPrefixProvider* dpp, rgw::Aio* aio, uint64_t cost, uint64_t id, optional_yield& y)
{
  rgw::AioResultList results = send_request(dpp, aio, cost, id, y);
  // A failed submission has already completed its AioResult exactly once.
  if (r) {
    auto ret = complete_request(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "RemoteCacheGetOp:: " << __func__ << " complete_request failed with ret: " << ret << dendl;
    }
  }

  return results;
}

//placeholder for any initialization that is needed
int RemoteCacheOp::init(CephContext* cct, const DoutPrefixProvider* dpp)
{
  return 0;
}

int RemoteCacheOp::complete_request(const DoutPrefixProvider* dpp, optional_yield& y)
{
  if (!sender) {
    return -EINVAL;
  }

  int ret = sender->complete_request(dpp, y);
  sender.reset();
  return ret;
}

int RemoteCacheOp::send_and_complete_request(const DoutPrefixProvider* dpp, optional_yield& y, bufferlist* bl)
{
  auto ret = send_request(dpp, y, bl);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "RemoteCachePutOp:: " << __func__ <<  "(): send_request failed with ret: " << ret << dendl;
    return ret;
  }

  ret = complete_request(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "RemoteCachePutOp:: " << __func__ << "(): complete_request failed with ret: " << ret << dendl;
    return ret;
  }

  return 0;
}

int RemoteCacheDeleteOp::send_request(const DoutPrefixProvider* dpp, optional_yield& y, bufferlist* bl)
{
  in_bl.clear();

  auto access_key = load_access_key(dpp, y);
  if (!access_key) {
    return access_key.error();
  }

  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;
  extra_headers["x-rgw-remote-cache-request"] = "true";
  extra_headers["x-rgw-cache-object-version"] = op.version;
  extra_headers["x-rgw-cache-blk-offset"] = std::to_string(op.offset);
  extra_headers["x-rgw-cache-blk-len"] = std::to_string(op.len);
  extra_headers["x-rgw-cache-obj-size"] = std::to_string(op.obj_size);
  extra_headers["x-rgw-cache-object-dirty"] = op.dirty ? "true" : "false";
  // Empty when this delete does not create a delete marker.
  extra_headers["x-rgw-cache-delete-marker-version"] = op.delete_marker_version;
  if (!op.delete_marker_mtime.empty()) {
    extra_headers["x-rgw-cache-delete-marker-mtime"] = op.delete_marker_mtime;
  }

  auto resource = get_resource(op.bucket_name, op.object_name, op.bucket_tenant);
  sender = std::make_unique<RGWRESTStreamRWRequest>(dpp->get_cct(), "DELETE", op.remote_addr, &cb, nullptr, nullptr, "", host_style);

  if (const auto ret = sender->send_request(dpp, &*access_key, extra_headers, resource, nullptr, bl); ret < 0) {
    return ret;
  }

  return 0;
}

int RemoteCachePutOp::send_request(const DoutPrefixProvider* dpp, optional_yield& y, bufferlist* bl)
{
  in_bl.clear();

  auto access_key = load_access_key(dpp, y);
  if (!access_key) {
    return access_key.error();
  }

  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;
  extra_headers["x-rgw-remote-cache-request"] = "true";
  extra_headers["x-rgw-cache-object-version"] = op.version;
  if (!old_version.empty()) {
    extra_headers["x-rgw-cache-old-version"] = old_version;
  }
  if (!mtime.empty()) {
    extra_headers["x-rgw-cache-object-mtime"] = mtime;
  }
  extra_headers["x-rgw-cache-blk-offset"] = std::to_string(op.offset);
  extra_headers["x-rgw-cache-blk-len"] = std::to_string(op.len);
  extra_headers["x-rgw-cache-obj-size"] = std::to_string(op.obj_size);
  extra_headers["x-rgw-cache-object-dirty"] = op.dirty ? "true" : "false";

  if (block_only) {
    extra_headers["x-rgw-cache-block-only"] = "true";
  }

  auto resource = get_resource(op.bucket_name, op.object_name, op.bucket_tenant);
  sender = std::make_unique<RGWRESTStreamRWRequest>(dpp->get_cct(), "PUT", op.remote_addr, &cb, nullptr, nullptr, "", host_style);

  return sender->send_request(dpp, &*access_key, extra_headers, resource, nullptr, bl);
}

int RemoteCachePutBatch::send(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              RemoteCachePutOp::RemoteCachePutOpData& op,
                              bufferlist& bl)
{
  // Drain one if at capacity
  while (in_flight.size() >= max_in_flight) {
    if(int ret = complete_next(dpp, y); ret < 0) {
      ldpp_dout(dpp, 5) << "RemoteCachePutOp request failed for object_name=" << op.object_name << " ret=" << ret << dendl;
    }
  }

  // Create and initialize the put operation
  auto put_op = std::make_unique<RemoteCachePutOp>(driver, op);
  if(int ret = put_op->init(cct, dpp); ret < 0) {
    ldpp_dout(dpp, 0) << "Failed to init RemoteCachePutOp for object_name=" << op.object_name << " ret=" << ret << dendl;
    return ret;
  }

  // Send the request
  if(int ret = put_op->send_request(dpp, y, &bl); ret < 0) {
    ldpp_dout(dpp, 0) << "Failed to send RemoteCachePutOp for object_name=" << op.object_name << " ret=" << ret << dendl;
    return ret;
  }

  // Track it
  PutResult result {
    .put_op = std::move(put_op),
    .key = op.object_name,
    .op_info = op
  };
  in_flight.push_back(std::move(result));

  ldpp_dout(dpp, 20) << "RemoteCachePutOp queued: object_name=" << op.object_name << " offset=" << op.offset << " len=" << op.len << dendl;

  return 0;
}

int RemoteCachePutBatch::complete_next(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (in_flight.empty()) {
    return 0;
  }

  auto result = std::move(in_flight.front());
  in_flight.pop_front();

  // Complete the request
  result.status = result.put_op->complete_request(dpp, y);

  if (result.status < 0) {
    ldpp_dout(dpp, 5) << "RemoteCachePut completed with error: object_name=" << result.key << " ret=" << result.status << dendl;
  } else {
    ldpp_dout(dpp, 20) << "RemoteCachePut completed successfully: object_name=" << result.key << dendl;
  }

  result.put_op.reset();

  completed.push_back(std::move(result));
  return result.status;
}

int RemoteCachePutBatch::finish_all(const DoutPrefixProvider* dpp, optional_yield y)
{
  int first_error = 0;
  int error_count = 0;

  while (!in_flight.empty()) {
    int ret = complete_next(dpp, y);
    if (ret < 0) {
      error_count++;
      if (first_error == 0) {
        first_error = ret;
      }
    }
  }

  if (error_count > 0) {
    ldpp_dout(dpp, 5) << "RemoteCachePutBatch finished with " << error_count << " errors" << dendl;
  }

  return first_error;
}

} // namespace rgw::d4n
