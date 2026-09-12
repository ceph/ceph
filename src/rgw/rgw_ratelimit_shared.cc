#include "rgw_ratelimit_shared.h"

namespace rgw::ratelimit {

SharedCounterRegistry& SharedCounterRegistry::instance()
{
  static SharedCounterRegistry registry;
  return registry;
}

SharedCounterRegistry::entry_t& SharedCounterRegistry::get_or_create(const std::string& key)
{
  {
    std::shared_lock rlock(map_lock);
    auto it = counters.find(key);
    if (it != counters.end()) {
      return *it->second;
    }
  }
  std::unique_lock wlock(map_lock);
  auto& slot = counters[key];
  if (!slot) {
    slot = std::make_unique<entry_t>();
  }
  return *slot;
}

int64_t SharedCounterRegistry::consume_key(const std::string& key,
                                           OpType op_type,
                                           const RGWRateLimitInfo* info,
                                           ceph::timespan curr_timestamp,
                                           int64_t interval)
{
  auto& entry = get_or_create(key);
  std::lock_guard guard(entry.lock);
  return consume(entry.state, op_type, info, curr_timestamp, interval);
}

void SharedCounterRegistry::giveback_key(const std::string& key, OpType op_type)
{
  auto& entry = get_or_create(key);
  std::lock_guard guard(entry.lock);
  giveback(entry.state, op_type);
}

void SharedCounterRegistry::decrease_bytes_key(const std::string& key,
                                               bool is_read,
                                               int64_t amount,
                                               const RGWRateLimitInfo* info)
{
  auto& entry = get_or_create(key);
  std::lock_guard guard(entry.lock);
  decrease_bytes(entry.state, is_read, amount, info);
}

void SharedCounterRegistry::clear_for_tests()
{
  std::unique_lock wlock(map_lock);
  counters.clear();
}

SharedCounterRateLimitStore::SharedCounterRateLimitStore(CephContext* cct,
                                                         std::string prefix)
  : DoutPrefix(cct, ceph_subsys_rgw, "shared rate limiter: "),
    key_prefix(std::move(prefix)),
    interval(cct->_conf->rgw_ratelimit_interval)
{
}

std::string SharedCounterRateLimitStore::full_key(const std::string& key) const
{
  if (key_prefix.empty()) {
    return key;
  }
  return key_prefix + ":" + key;
}

int64_t SharedCounterRateLimitStore::should_rate_limit(
    const char* method,
    const std::string& key,
    ceph::coarse_real_time curr_timestamp,
    const RGWRateLimitInfo* ratelimit_info,
    const std::string& resource)
{
  ldpp_dout(this, 21) << "shared should_rate_limit: key=" << std::quoted(key)
                      << " enabled=" << ratelimit_info->enabled << dendl;
  if (key.empty() || key.length() == 1 || !ratelimit_info->enabled) {
    return 0;
  }
  OpType type = rgw_ratelimit_op_type(method, resource, ratelimit_info);
  return SharedCounterRegistry::instance().consume_key(
      full_key(key), type, ratelimit_info,
      curr_timestamp.time_since_epoch(), interval);
}

void SharedCounterRateLimitStore::giveback_tokens(
    const char* method,
    const std::string& key,
    const std::string& resource,
    const RGWRateLimitInfo* ratelimit_info)
{
  if (key.empty() || key.length() == 1 || !ratelimit_info->enabled) {
    return;
  }
  OpType type = rgw_ratelimit_op_type(method, resource, ratelimit_info);
  SharedCounterRegistry::instance().giveback_key(full_key(key), type);
}

void SharedCounterRateLimitStore::decrease_bytes(
    const char* method,
    const std::string& key,
    int64_t amount,
    const RGWRateLimitInfo* info)
{
  if (key.empty() || key.length() == 1 || !info->enabled) {
    return;
  }
  OpType type = rgw_ratelimit_op_type(method, "", info);
  if ((type == OpType::Read && !info->max_read_bytes) ||
      (type == OpType::Write && !info->max_write_bytes)) {
    return;
  }
  if (type == OpType::Read) {
    SharedCounterRegistry::instance().decrease_bytes_key(full_key(key), true, amount, info);
  } else if (type == OpType::Write) {
    SharedCounterRegistry::instance().decrease_bytes_key(full_key(key), false, amount, info);
  }
}

} // namespace rgw::ratelimit
