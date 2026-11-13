// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "cls/rgw/cls_rgw_usage.h"

#include "common/config_proxy.h"
#include "objclass/objclass.h"
#include "rgw/datum/adapters/ceph-datum.h"
#include "rgw/datum/adapters/cls/usage.h"

#include <cerrno>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <string>
#include <type_traits>
#include <utility>

#include <fmt/format.h>

using std::map;
using std::snprintf;
using std::string;

using ceph::bufferlist;
using ceph::encode;
using ceph::salad::datum::try_decode;

namespace usage_datum = cls::rgw::usage_datum;

static void usage_record_prefix_by_time(uint64_t epoch, string& key)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%011llu", (long long unsigned)epoch);
  key = buf;
}

static void usage_record_prefix_by_user_old(const string& user, uint64_t epoch, string& key)
{
  key = fmt::format("{}_{:011}_", user, epoch);
}

static void usage_record_prefix_by_user(const string& user, uint64_t epoch, string& key)
{
  usage_record_prefix_by_user_old(user, epoch, key);
  if (user.starts_with('0')) {
    // Use ~ prefix for name_by_user records that have keys that could potentially
    // put them among name_by_time records.
    key.insert(0, 1, '~');
  }
}

static void usage_record_name_by_time(uint64_t epoch, const string& user, const string& bucket, string& key)
{
  key = fmt::format("{:011}_{}_{}", epoch, user, bucket);
}

static void usage_record_name_by_user_old(const string& user, uint64_t epoch, const string& bucket, string& key)
{
  key = fmt::format("{}_{:011}_{}", user, epoch, bucket);
}

static void usage_record_name_by_user(const string& user, uint64_t epoch, const string& bucket, string& key)
{
  usage_record_name_by_user_old(user, epoch, bucket, key);
  if (user.starts_with('0')) {
    // Use ~ prefix for name_by_user records that have keys that could potentially
    // put them among name_by_time records.
    key.insert(0, 1, '~');
  }
}

static int usage_record_decode(const bufferlist& record_bl, rgw_usage_log_entry& e)
{
  auto kiter = record_bl.cbegin();
  if (!try_decode(e, kiter)) {
    CLS_LOG(1, "ERROR: usage_record_decode(): failed to decode record_bl\n");
    return -EINVAL;
  }

  return 0;
}

int rgw_user_usage_log_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "entered %s", __func__);

  auto in_iter = in->cbegin();
  usage_datum::add_request op;

  if (!try_decode(op, in_iter)) {
    CLS_LOG(1, "ERROR: rgw_user_usage_log_add(): failed to decode request\n");
    return -EINVAL;
  }

  const ConfigProxy& conf = cls_get_config(hctx);
  const bool key_transition = conf->rgw_usage_log_key_transition;

  auto& info = op.info;

  for (auto iter = info.entries.begin(); iter != info.entries.end(); ++iter) {
    rgw_usage_log_entry& entry = *iter;
    string key_by_time;

    rgw_user *puser = (entry.payer.empty() ? &entry.owner : &entry.payer);

    usage_record_name_by_time(entry.epoch, puser->to_str(), entry.bucket, key_by_time);

    CLS_LOG(10, "rgw_user_usage_log_add user=%s bucket=%s", puser->to_str().c_str(), entry.bucket.c_str());

    bufferlist record_bl;
    int ret = cls_cxx_map_get_val(hctx, key_by_time, &record_bl);
    if (ret < 0 && ret != -ENOENT) {
      CLS_LOG(1, "ERROR: rgw_user_usage_log_add(): cls_cxx_map_read_key returned %d", ret);
      return -EINVAL;
    }
    if (ret >= 0) {
      rgw_usage_log_entry e;
      ret = usage_record_decode(record_bl, e);
      if (ret < 0)
        return ret;
      CLS_LOG(10, "rgw_user_usage_log_add aggregating existing bucket\n");
      entry.aggregate(e);
    }

    bufferlist new_record_bl;
    encode(entry, new_record_bl);
    ret = cls_cxx_map_set_val(hctx, key_by_time, &new_record_bl);
    if (ret < 0)
      return ret;

    string key_by_user;
    usage_record_name_by_user(puser->to_str(), entry.epoch, entry.bucket, key_by_user);
    ret = cls_cxx_map_set_val(hctx, key_by_user, &new_record_bl);
    if (ret < 0)
      return ret;

    if (key_transition && puser->to_str().starts_with('0')) {
      string key_by_user_old;
      usage_record_name_by_user_old(puser->to_str(), entry.epoch, entry.bucket, key_by_user_old);
      (void)cls_cxx_map_remove_key(hctx, key_by_user_old);
    }
  }

  return 0;
}

// Consumers are borrowed for this call and invoked as lvalues; entries live only for each call.
template <typename ConsumerT>
concept usage_consumer = std::invocable<ConsumerT&, const rgw_usage_log_entry&> and
  (std::is_void_v<std::invoke_result_t<ConsumerT&, const rgw_usage_log_entry&>> or
   std::convertible_to<std::invoke_result_t<ConsumerT&, const rgw_usage_log_entry&>, int>);

template <usage_consumer ConsumerT>
static int usage_handle_range(cls_method_context_t hctx, uint64_t start_epoch, uint64_t end_epoch,
                              const string& start_key, const string& end_key, const string& user_key,
                              const string& bucket, string& key_iter, uint32_t max_entries, bool& truncated,
                              ConsumerT&& consumer)
{
  using consumer_result = std::invoke_result_t<ConsumerT&, const rgw_usage_log_entry&>;

  string filter_prefix;
  bool truncated_status = false;
  map<string, bufferlist> usage_logs;
  bool by_user = !user_key.empty();
  int cnt = 0;

  CLS_LOG(20, "usage_handle_range start_key=%s", start_key.c_str());
  int ret = cls_cxx_map_get_vals(hctx, start_key, filter_prefix, max_entries, &usage_logs, &truncated_status);
  if (ret < 0)
    return ret;

  truncated = truncated_status;

  for (const auto& log : usage_logs) {
    const string& key = log.first;
    rgw_usage_log_entry e;

    key_iter = key;
    if (!by_user && key.compare(end_key) >= 0) {
      CLS_LOG(20, "usage_handle_range reached key=%s, done", key.c_str());
      truncated = false;
      break;
    }

    if (by_user && key.compare(0, user_key.size(), user_key) != 0) {
      CLS_LOG(20, "usage_handle_range reached key=%s, done", key.c_str());
      truncated = false;
      break;
    }

    ret = usage_record_decode(log.second, e);
    if (ret < 0)
      return ret;

    if (!bucket.empty() && bucket.compare(e.bucket))
      continue;

    if (e.epoch < start_epoch)
      continue;

    /* keys are sorted by epoch, so once we're past end we're done */
    if (e.epoch >= end_epoch) {
      truncated = false;
      break;
    }

    if constexpr (std::is_void_v<consumer_result>) {
      std::invoke(consumer, std::as_const(e));
    }

    if constexpr (!std::is_void_v<consumer_result>) {
      const int status = std::invoke(consumer, std::as_const(e));
      if (status < 0) {
        return status;
      }
    }

    ++cnt;
  }

  return cnt;
}

template <usage_consumer ConsumerT>
static int usage_iterate_range_by_user(cls_method_context_t hctx, uint64_t start_epoch, uint64_t end_epoch,
                                       const string& user, const string& bucket, string& key_iter,
                                       uint32_t max_entries, bool& truncated, ConsumerT&& consumer)
{
  ceph_assert(!user.empty());

  const ConfigProxy& conf = cls_get_config(hctx);
  const bool key_transition = conf->rgw_usage_log_key_transition;

  if (key_transition &&
      user.starts_with('0') &&
      (key_iter.empty() || key_iter.starts_with('0'))) {
    // During key transition for records that could have keys that fall within name_by_time records,
    // we need to go 2 passes to cover both the old keys and the new keys.
    // Remove this block when key_transition is deprecated.
    const string user_key = user + "_";
    string start_key = key_iter;
    string old_key_iter = key_iter;
    if (start_key.empty()) {
      usage_record_prefix_by_user_old(user, start_epoch, start_key);
    }
    int ret = usage_handle_range(hctx, start_epoch, end_epoch, start_key, "", user_key,
                                 bucket, old_key_iter, max_entries, truncated, consumer);
    if (ret < 0) {
      return ret;
    }

    if (truncated) {
      key_iter = old_key_iter;
      return 0;
    }

    // All old keys have been handled, start fresh on the new keys.
    key_iter.clear();
    max_entries -= ret;
  }

  if (max_entries == 0) {
    return 0;
  }

  string start_key = key_iter;
  const string user_key = user.starts_with('0') ? (std::string("~") + user + "_") : (user + "_");
  if (start_key.empty()) {
    usage_record_prefix_by_user(user, start_epoch, start_key);
  }

  return usage_handle_range(hctx, start_epoch, end_epoch, start_key, "", user_key,
                            bucket, key_iter, max_entries, truncated, consumer);
}

template <usage_consumer ConsumerT>
static int usage_iterate_range_by_time(cls_method_context_t hctx, uint64_t start_epoch, uint64_t end_epoch,
                                       const string& bucket, string& key_iter, uint32_t max_entries,
                                       bool& truncated, ConsumerT&& consumer)
{
  CLS_LOG(10, "entered %s", __func__);

  string start_key = key_iter;
  string end_key;
  usage_record_prefix_by_time(end_epoch, end_key);

  if (start_key.empty()) {
    usage_record_prefix_by_time(start_epoch, start_key);
  }

  return usage_handle_range(hctx, start_epoch, end_epoch, start_key, end_key, "",
                            bucket, key_iter, max_entries, truncated, consumer);
}

template <usage_consumer ConsumerT>
static int usage_iterate_range(cls_method_context_t hctx, uint64_t start_epoch, uint64_t end_epoch,
                               const string& user, const string& bucket, string& key_iter,
                               uint32_t max_entries, bool& truncated, ConsumerT&& consumer)
{
  CLS_LOG(10, "entered %s", __func__);

  if (user.empty()) {
    return usage_iterate_range_by_time(hctx, start_epoch, end_epoch, bucket, key_iter,
                                       max_entries, truncated, consumer);
  }

  return usage_iterate_range_by_user(hctx, start_epoch, end_epoch, user, bucket,
                                     key_iter, max_entries, truncated, consumer);
}

int rgw_user_usage_log_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "entered %s", __func__);

  auto in_iter = in->cbegin();
  usage_datum::read_request op;

  if (!try_decode(op, in_iter)) {
    CLS_LOG(1, "ERROR: rgw_user_usage_log_read(): failed to decode request\n");
    return -EINVAL;
  }

  usage_datum::reply ret_info;
  auto consume = [&usage = ret_info.usage](const rgw_usage_log_entry& entry) {
    const auto& user = entry.payer.empty() ? entry.owner : entry.payer;
    usage[rgw_user_bucket {user.to_str(), entry.bucket}].aggregate(entry);
  };

  string iter = op.iter;
#define MAX_ENTRIES 1000
  uint32_t max_entries = (op.max_entries ? op.max_entries : MAX_ENTRIES);

  int ret = usage_iterate_range(hctx, op.start_epoch, op.end_epoch, op.owner, op.bucket,
                                iter, max_entries, ret_info.truncated, consume);
  if (ret < 0)
    return ret;

  if (ret_info.truncated)
    ret_info.next_iter = iter;

  encode(ret_info, *out);
  return 0;
}

int rgw_user_usage_log_trim(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "entered %s", __func__);

  /* only continue if object exists! */
  int ret = cls_cxx_stat(hctx, NULL, NULL);
  if (ret < 0)
    return ret;

  auto in_iter = in->cbegin();
  usage_datum::trim_request op;

  if (!try_decode(op, in_iter)) {
    CLS_LOG(1, "ERROR: rgw_user_log_usage_log_trim(): failed to decode request\n");
    return -EINVAL;
  }

  string iter;
  bool more;
  const ConfigProxy& conf = cls_get_config(hctx);
  const bool key_transition = conf->rgw_usage_log_key_transition;
  bool found = false;
  auto consume = [hctx, key_transition, &found](const rgw_usage_log_entry& entry) {
    found = true;

    string key_by_time, key_by_user;
    const string user = entry.payer.empty() ? entry.owner.to_str() : entry.payer.to_str();
    usage_record_name_by_time(entry.epoch, user, entry.bucket, key_by_time);
    usage_record_name_by_user(user, entry.epoch, entry.bucket, key_by_user);

    const int status = cls_cxx_map_remove_key(hctx, key_by_time);
    if (status < 0) {
      return status;
    }

    if (key_transition && user.starts_with('0')) {
      string old_key;
      usage_record_name_by_user_old(user, entry.epoch, entry.bucket, old_key);
      (void)cls_cxx_map_remove_key(hctx, old_key);
    }

    return cls_cxx_map_remove_key(hctx, key_by_user);
  };

#define MAX_USAGE_TRIM_ENTRIES 1000
  ret = usage_iterate_range(hctx, op.start_epoch, op.end_epoch, op.user, op.bucket,
                            iter, MAX_USAGE_TRIM_ENTRIES, more, consume);

  if (ret < 0)
    return ret;

  if (!more && !found)
    return -ENODATA;

  return 0;
}

int rgw_usage_log_clear(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "entered %s", __func__);

  int ret = cls_cxx_map_clear(hctx);
  /* if object doesn't exist all the logs are cleared anyway */
  if (ret == -ENOENT)
    ret = 0;

  return ret;
}
