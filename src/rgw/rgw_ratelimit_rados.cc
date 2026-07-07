#include "rgw_ratelimit_rados.h"

#include "common/ceph_context.h"
#include "include/rados/librados.hpp"

namespace rgw::ratelimit {

RadosRateLimitStore::RadosRateLimitStore(CephContext* cct,
                                         std::string pool,
                                         std::string oid_prefix,
                                         uint32_t num_shards)
  : DoutPrefix(cct, ceph_subsys_rgw, "rados rate limiter: "),
    pool_name(std::move(pool)),
    oid_prefix(std::move(oid_prefix)),
    num_shards(std::max(1u, num_shards)),
    interval(cct->_conf->rgw_ratelimit_interval),
    fail_open(cct->_conf->rgw_ratelimit_fail_open)
{
  fallback = std::make_unique<SharedCounterRateLimitStore>(cct, "rados-fallback");
}

int RadosRateLimitStore::init()
{
  if (pool_name.empty()) {
    ldpp_dout(this, 0) << "rados rate limit pool not configured, using fallback" << dendl;
    return 0;
  }
  rados_client = std::make_unique<librados::Rados>();
  rados = rados_client.get();
  int ret = rados->init_with_context(cct);
  if (ret < 0) {
    ldpp_dout(this, 0) << "rados init_with_context failed: " << cpp_strerror(ret) << dendl;
    return fail_open ? 0 : ret;
  }
  ret = rados->connect();
  if (ret < 0) {
    ldpp_dout(this, 0) << "rados connect failed: " << cpp_strerror(ret) << dendl;
    return fail_open ? 0 : ret;
  }
  ioctx = std::make_unique<librados::IoCtx>();
  ret = rados->ioctx_create(pool_name.c_str(), *ioctx);
  if (ret < 0) {
    ldpp_dout(this, 0) << "ioctx_create(" << pool_name << ") failed: "
                       << cpp_strerror(ret) << dendl;
    return fail_open ? 0 : ret;
  }
  return 0;
}

void RadosRateLimitStore::shutdown()
{
  ioctx.reset();
  if (rados) {
    rados->shutdown();
  }
  rados_client.reset();
  rados = nullptr;
}

void RadosRateLimitStore::set_test_ioctx(librados::IoCtx* ctx)
{
  test_ioctx = ctx;
}

librados::IoCtx* RadosRateLimitStore::get_ioctx()
{
  if (test_ioctx) {
    return test_ioctx;
  }
  return ioctx.get();
}

bool RadosRateLimitStore::use_fallback()
{
  return get_ioctx() == nullptr;
}

std::string RadosRateLimitStore::shard_oid(const std::string& key) const
{
  const std::hash<std::string> hasher;
  const uint32_t shard = hasher(key) % num_shards;
  return oid_prefix + "." + std::to_string(shard);
}

int64_t RadosRateLimitStore::should_rate_limit(
    const char* method,
    const std::string& key,
    ceph::coarse_real_time curr_timestamp,
    const RGWRateLimitInfo* ratelimit_info,
    const std::string& resource)
{
  if (key.empty() || key.length() == 1 || !ratelimit_info->enabled) {
    return 0;
  }
  if (use_fallback()) {
    return fallback->should_rate_limit(method, key, curr_timestamp,
                                       ratelimit_info, resource);
  }

  OpType type = rgw_ratelimit_op_type(method, resource, ratelimit_info);
  int64_t delay = 0;
  int ret = cls::rgw::ratelimit::consume(
      get_ioctx(),
      shard_oid(key),
      key,
      type,
      *ratelimit_info,
      curr_timestamp.time_since_epoch(),
      interval,
      &delay);
  if (ret < 0) {
    ldpp_dout(this, 5) << "cls consume failed: " << cpp_strerror(ret)
                       << " fail_open=" << fail_open << dendl;
    if (fail_open) {
      return fallback->should_rate_limit(method, key, curr_timestamp,
                                         ratelimit_info, resource);
    }
    return interval;
  }
  return delay;
}

void RadosRateLimitStore::giveback_tokens(
    const char* method,
    const std::string& key,
    const std::string& resource,
    const RGWRateLimitInfo* ratelimit_info)
{
  if (key.empty() || key.length() == 1 || !ratelimit_info->enabled) {
    return;
  }
  if (use_fallback()) {
    fallback->giveback_tokens(method, key, resource, ratelimit_info);
    return;
  }
  OpType type = rgw_ratelimit_op_type(method, resource, ratelimit_info);
  int ret = cls::rgw::ratelimit::giveback(get_ioctx(), shard_oid(key), key, type);
  if (ret < 0 && fail_open) {
    fallback->giveback_tokens(method, key, resource, ratelimit_info);
  }
}

void RadosRateLimitStore::decrease_bytes(
    const char* method,
    const std::string& key,
    int64_t amount,
    const RGWRateLimitInfo* info)
{
  if (key.empty() || key.length() == 1 || !info->enabled) {
    return;
  }
  if (use_fallback()) {
    fallback->decrease_bytes(method, key, amount, info);
    return;
  }
  OpType type = rgw_ratelimit_op_type(method, "", info);
  if ((type == OpType::Read && !info->max_read_bytes) ||
      (type == OpType::Write && !info->max_write_bytes)) {
    return;
  }
  const bool is_read = (type == OpType::Read);
  if (type != OpType::Read && type != OpType::Write) {
    return;
  }
  int ret = cls::rgw::ratelimit::decrease_bytes(
      get_ioctx(), shard_oid(key), key, is_read, amount, *info);
  if (ret < 0 && fail_open) {
    fallback->decrease_bytes(method, key, amount, info);
  }
}

} // namespace rgw::ratelimit
