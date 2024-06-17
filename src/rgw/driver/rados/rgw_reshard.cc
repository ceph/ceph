// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <limits>
#include <sstream>
#include <chrono>

#include "rgw_zone.h"
#include "driver/rados/rgw_bucket.h"
#include "rgw_reshard.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "common/errno.h"
#include "common/ceph_json.h"

#include "common/dout.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_tier_rados.h"
#include "services/svc_bilog_rados.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

const string reshard_oid_prefix = "reshard.";
const string reshard_lock_name = "reshard_process";
const string bucket_instance_lock_name = "bucket_instance_lock";

// key reduction values; NB maybe expose some in options
constexpr uint64_t min_objs_per_shard = 10000;
constexpr uint32_t min_dynamic_shards = 11;

/* All primes up to 2000 used to attempt to make dynamic sharding use
 * a prime numbers of shards. Note: this list also includes 1 for when
 * 1 shard is the most appropriate, even though 1 is not prime.
 */
const std::initializer_list<uint16_t> RGWBucketReshard::reshard_primes = {
  1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61,
  67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137,
  139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211,
  223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283,
  293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379,
  383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461,
  463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563,
  569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643,
  647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739,
  743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829,
  839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937,
  941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021,
  1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093,
  1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181,
  1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259,
  1277, 1279, 1283, 1289, 1291, 1297, 1301, 1303, 1307, 1319, 1321,
  1327, 1361, 1367, 1373, 1381, 1399, 1409, 1423, 1427, 1429, 1433,
  1439, 1447, 1451, 1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493,
  1499, 1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579,
  1583, 1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657,
  1663, 1667, 1669, 1693, 1697, 1699, 1709, 1721, 1723, 1733, 1741,
  1747, 1753, 1759, 1777, 1783, 1787, 1789, 1801, 1811, 1823, 1831,
  1847, 1861, 1867, 1871, 1873, 1877, 1879, 1889, 1901, 1907, 1913,
  1931, 1933, 1949, 1951, 1973, 1979, 1987, 1993, 1997, 1999
};


uint32_t RGWBucketReshard::get_prime_shard_count(
  uint32_t shard_count,
  uint32_t max_dynamic_shards,
  uint32_t min_dynamic_shards)
{
  uint32_t prime_shard_count =
    get_prime_shards_greater_or_equal(shard_count);

  // if we cannot find a larger prime number, then just use what was
  // passed in
  if (! prime_shard_count) {
    prime_shard_count = shard_count;
  }

  // keep within min/max bounds
  return std::min(max_dynamic_shards,
		  std::max(min_dynamic_shards, prime_shard_count));
}


// Given the current number of shards and objects (entries), we
// calculate whether resharding is called for and if so, how many
// shards we should have given a variety of considerations to be used
// as part of the dynamic resharding capability.
void RGWBucketReshard::calculate_preferred_shards(
  const DoutPrefixProvider* dpp,
  const uint32_t max_dynamic_shards,
  const uint64_t max_objs_per_shard,
//  const uint64_t min_objs_per_shard,
  const bool is_multisite,
//  const uint64_t min_dynamic_shards, FIX THIS!!!!!!
  const uint64_t num_objs,
  const uint32_t current_num_shards,
  bool& need_resharding,
  uint32_t* suggested_num_shards,
  bool prefer_prime)
{
  constexpr uint32_t regular_multiplier = 2;
  // to reduce number of reshards in multisite, increase number of shards more aggressively
  constexpr uint32_t multisite_multiplier = 8;
  const char* verb = "n/a";

  if (current_num_shards < max_dynamic_shards &&
      num_objs > current_num_shards * max_objs_per_shard) {
    need_resharding = true;
    verb = "expansion";
  } else if (current_num_shards > min_dynamic_shards &&
	     num_objs < current_num_shards * min_objs_per_shard) {
    need_resharding = true;
    verb = "reduction";
  } else {
    need_resharding = false;
    return;
  }

  const uint32_t multiplier =
    is_multisite ? multisite_multiplier : regular_multiplier;
  uint32_t calculated_num_shards =
    std::max(min_dynamic_shards,
	     std::min(max_dynamic_shards,
		      (uint32_t) (num_objs * multiplier / max_objs_per_shard)));
  if (calculated_num_shards == current_num_shards) {
    need_resharding = false;
    return;
  }

  if (prefer_prime) {
    calculated_num_shards = get_prime_shard_count(
      calculated_num_shards, max_dynamic_shards, min_dynamic_shards);
  }

  ldpp_dout(dpp, 20) << __func__ << ": reshard " << verb <<
    " suggested; current average (objects/shard) is " <<
    float(num_objs) / current_num_shards << ", which is not within " <<
    min_objs_per_shard << " and " << max_objs_per_shard <<
    "; suggesting " << calculated_num_shards << " shards" << dendl;

  if (suggested_num_shards) {
    *suggested_num_shards = calculated_num_shards;
  }
} // RGWBucketReshard::check_bucket_shards


class BucketReshardShard {
  rgw::sal::RadosStore* store;
  const RGWBucketInfo& bucket_info;
  int shard_id;
  RGWRados::BucketShard bs;
  vector<rgw_cls_bi_entry> entries;
  map<RGWObjCategory, rgw_bucket_category_stats> stats;
  deque<librados::AioCompletion *>& aio_completions;
  uint64_t max_aio_completions;
  uint64_t reshard_shard_batch_size;

  int wait_next_completion() {
    librados::AioCompletion *c = aio_completions.front();
    aio_completions.pop_front();

    c->wait_for_complete();

    int ret = c->get_return_value();
    c->release();

    if (ret < 0) {
      derr << "ERROR: reshard rados operation failed: " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    return 0;
  }

  int get_completion(librados::AioCompletion **c) {
    if (aio_completions.size() >= max_aio_completions) {
      int ret = wait_next_completion();
      if (ret < 0) {
        return ret;
      }
    }

    *c = librados::Rados::aio_create_completion(nullptr, nullptr);
    aio_completions.push_back(*c);

    return 0;
  }

public:
  BucketReshardShard(const DoutPrefixProvider *dpp,
		     rgw::sal::RadosStore *_store, const RGWBucketInfo& _bucket_info,
                     const rgw::bucket_index_layout_generation& index,
                     int shard_id, deque<librados::AioCompletion *>& _completions) :
    store(_store), bucket_info(_bucket_info), shard_id(shard_id),
    bs(store->getRados()), aio_completions(_completions)
  {
    bs.init(dpp, bucket_info, index, shard_id, null_yield);

    max_aio_completions =
      store->ctx()->_conf.get_val<uint64_t>("rgw_reshard_max_aio");
    reshard_shard_batch_size =
      store->ctx()->_conf.get_val<uint64_t>("rgw_reshard_batch_size");
  }

  int get_shard_id() const {
    return shard_id;
  }

  int add_entry(rgw_cls_bi_entry& entry, bool account, RGWObjCategory category,
                const rgw_bucket_category_stats& entry_stats) {
    entries.push_back(entry);
    if (account) {
      rgw_bucket_category_stats& target = stats[category];
      target.num_entries += entry_stats.num_entries;
      target.total_size += entry_stats.total_size;
      target.total_size_rounded += entry_stats.total_size_rounded;
      target.actual_size += entry_stats.actual_size;
    }
    if (entries.size() >= reshard_shard_batch_size) {
      int ret = flush();
      if (ret < 0) {
        return ret;
      }
    }

    return 0;
  }

  int flush() {
    if (entries.size() == 0) {
      return 0;
    }

    librados::ObjectWriteOperation op;
    for (auto& entry : entries) {
      store->getRados()->bi_put(op, bs, entry, null_yield);
    }
    cls_rgw_bucket_update_stats(op, false, stats);

    librados::AioCompletion *c;
    int ret = get_completion(&c);
    if (ret < 0) {
      return ret;
    }
    ret = bs.bucket_obj.aio_operate(c, &op);
    if (ret < 0) {
      derr << "ERROR: failed to store entries in target bucket shard (bs=" << bs.bucket << "/" << bs.shard_id << ") error=" << cpp_strerror(-ret) << dendl;
      return ret;
    }
    entries.clear();
    stats.clear();
    return 0;
  }

  int wait_all_aio() {
    int ret = 0;
    while (!aio_completions.empty()) {
      int r = wait_next_completion();
      if (r < 0) {
        ret = r;
      }
    }
    return ret;
  }
}; // class BucketReshardShard


class BucketReshardManager {
  rgw::sal::RadosStore *store;
  deque<librados::AioCompletion *> completions;
  vector<BucketReshardShard> target_shards;

public:
  BucketReshardManager(const DoutPrefixProvider *dpp,
		       rgw::sal::RadosStore *_store,
		       const RGWBucketInfo& bucket_info,
                       const rgw::bucket_index_layout_generation& target)
    : store(_store)
  {
    const uint32_t num_shards = rgw::num_shards(target.layout.normal);
    target_shards.reserve(num_shards);
    for (uint32_t i = 0; i < num_shards; ++i) {
      target_shards.emplace_back(dpp, store, bucket_info, target, i, completions);
    }
  }

  ~BucketReshardManager() {
    for (auto& shard : target_shards) {
      int ret = shard.wait_all_aio();
      if (ret < 0) {
        ldout(store->ctx(), 20) << __func__ <<
	  ": shard->wait_all_aio() returned ret=" << ret << dendl;
      }
    }
  }

  int add_entry(int shard_index,
                rgw_cls_bi_entry& entry, bool account, RGWObjCategory category,
                const rgw_bucket_category_stats& entry_stats) {
    int ret = target_shards[shard_index].add_entry(entry, account, category,
						   entry_stats);
    if (ret < 0) {
      derr << "ERROR: target_shards.add_entry(" << entry.idx <<
	") returned error: " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    return 0;
  }

  int finish() {
    int ret = 0;
    for (auto& shard : target_shards) {
      int r = shard.flush();
      if (r < 0) {
        derr << "ERROR: target_shards[" << shard.get_shard_id() << "].flush() returned error: " << cpp_strerror(-r) << dendl;
        ret = r;
      }
    }
    for (auto& shard : target_shards) {
      int r = shard.wait_all_aio();
      if (r < 0) {
        derr << "ERROR: target_shards[" << shard.get_shard_id() << "].wait_all_aio() returned error: " << cpp_strerror(-r) << dendl;
        ret = r;
      }
    }
    target_shards.clear();
    return ret;
  }
}; // class BucketReshardManager

RGWBucketReshard::RGWBucketReshard(rgw::sal::RadosStore* _store,
				   const RGWBucketInfo& _bucket_info,
				   const std::map<std::string, bufferlist>& _bucket_attrs,
				   RGWBucketReshardLock* _outer_reshard_lock) :
  store(_store), bucket_info(_bucket_info), bucket_attrs(_bucket_attrs),
  reshard_lock(store, bucket_info, true),
  outer_reshard_lock(_outer_reshard_lock)
{ }

// sets reshard status of bucket index shards for the current index layout
static int set_resharding_status(const DoutPrefixProvider *dpp,
				 rgw::sal::RadosStore* store,
				 const RGWBucketInfo& bucket_info,
                                 cls_rgw_reshard_status status)
{
  cls_rgw_bucket_instance_entry instance_entry;
  instance_entry.set_status(status);

  int ret = store->getRados()->bucket_set_reshard(dpp, bucket_info, instance_entry);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "RGWReshard::" << __func__ << " ERROR: error setting bucket resharding flag on bucket index: "
		  << cpp_strerror(-ret) << dendl;
    return ret;
  }
  return 0;
}

static int remove_old_reshard_instance(rgw::sal::RadosStore* store,
                                       const rgw_bucket& bucket,
                                       const DoutPrefixProvider* dpp, optional_yield y)
{
  RGWBucketInfo info;
  int r = store->getRados()->get_bucket_instance_info(bucket, info, nullptr,
                                                      nullptr, y, dpp);
  if (r < 0) {
    return r;
  }

  // delete its shard objects (ignore errors)
  store->svc()->bi->clean_index(dpp, info, info.layout.current_index);
  // delete the bucket instance metadata
  return store->ctl()->bucket->remove_bucket_instance_info(bucket, info, y, dpp);
}

// initialize the new bucket index shard objects
static int init_target_index(rgw::sal::RadosStore* store,
                             RGWBucketInfo& bucket_info,
                             const rgw::bucket_index_layout_generation& index,
                             const DoutPrefixProvider* dpp)
{
  int ret = store->svc()->bi->init_index(dpp, bucket_info, index);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to initialize "
       "target index shard objects: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  if (!bucket_info.datasync_flag_enabled()) {
    // if bucket sync is disabled, disable it on each of the new shards too
    auto log = rgw::log_layout_from_index(0, index);
    ret = store->svc()->bilog_rados->log_stop(dpp, bucket_info, log, -1);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to disable "
          "bucket sync on the target index shard objects: "
          << cpp_strerror(ret) << dendl;
      store->svc()->bi->clean_index(dpp, bucket_info, index);
      return ret;
    }
  }

  return ret;
}

// initialize a target index layout, create its bucket index shard objects, and
// write the target layout to the bucket instance metadata
static int init_target_layout(rgw::sal::RadosStore* store,
                              RGWBucketInfo& bucket_info,
			      std::map<std::string, bufferlist>& bucket_attrs,
                              ReshardFaultInjector& fault,
                              uint32_t new_num_shards,
                              const DoutPrefixProvider* dpp, optional_yield y)
{
  auto prev = bucket_info.layout; // make a copy for cleanup
  const auto current = prev.current_index;

  // initialize a new normal target index layout generation
  rgw::bucket_index_layout_generation target;
  target.layout.type = rgw::BucketIndexType::Normal;
  target.layout.normal.num_shards = new_num_shards;
  target.gen = current.gen + 1;

  if (bucket_info.reshard_status == cls_rgw_reshard_status::IN_PROGRESS) {
    // backward-compatible cleanup of old reshards, where the target was in a
    // different bucket instance
    if (!bucket_info.new_bucket_instance_id.empty()) {
      rgw_bucket new_bucket = bucket_info.bucket;
      new_bucket.bucket_id = bucket_info.new_bucket_instance_id;
      ldout(store->ctx(), 10) << __func__ << " removing target bucket instance "
          "from a previous reshard attempt" << dendl;
      // ignore errors
      remove_old_reshard_instance(store, new_bucket, dpp, y);
    }
    bucket_info.reshard_status = cls_rgw_reshard_status::NOT_RESHARDING;
  }

  if (bucket_info.layout.target_index) {
    // a previous reshard failed or stalled, and its reshard lock dropped
    ldpp_dout(dpp, 10) << __func__ << " removing existing target index "
        "objects from a previous reshard attempt" << dendl;
    // delete its existing shard objects (ignore errors)
    store->svc()->bi->clean_index(dpp, bucket_info, *bucket_info.layout.target_index);
    // don't reuse this same generation in the new target layout, in case
    // something is still trying to operate on its shard objects
    target.gen = bucket_info.layout.target_index->gen + 1;
  }

  // create the index shard objects
  int ret = init_target_index(store, bucket_info, target, dpp);
  if (ret < 0) {
    return ret;
  }

  // retry in case of racing writes to the bucket instance metadata
  static constexpr auto max_retries = 10;
  int tries = 0;
  do {
    // update resharding state
    bucket_info.layout.target_index = target;
    bucket_info.layout.resharding = rgw::BucketReshardState::InProgress;

    if (ret = fault.check("set_target_layout");
        ret == 0) { // no fault injected, write the bucket instance metadata
      ret = store->getRados()->put_bucket_instance_info(bucket_info, false,
                                                        real_time(), &bucket_attrs, dpp, y);
    } else if (ret == -ECANCELED) {
      fault.clear(); // clear the fault so a retry can succeed
    }

    if (ret == -ECANCELED) {
      // racing write detected, read the latest bucket info and try again
      int ret2 = store->getRados()->get_bucket_instance_info(
          bucket_info.bucket, bucket_info,
          nullptr, &bucket_attrs, y, dpp);
      if (ret2 < 0) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to read "
            "bucket info: " << cpp_strerror(ret2) << dendl;
        ret = ret2;
        break;
      }

      // check that we're still in the reshard state we started in
      if (bucket_info.layout.resharding != rgw::BucketReshardState::None ||
          bucket_info.layout.current_index != current) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "another reshard" << dendl;
        break;
      }

      prev = bucket_info.layout; // update the copy
    }
    ++tries;
  } while (ret == -ECANCELED && tries < max_retries);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to write "
        "target index layout to bucket info: " << cpp_strerror(ret) << dendl;

    bucket_info.layout = std::move(prev);  // restore in-memory layout

    // delete the target shard objects (ignore errors)
    store->svc()->bi->clean_index(dpp, bucket_info, target);
    return ret;
  }
  return 0;
} // init_target_layout

// delete the bucket index shards associated with the target layout and remove
// it from the bucket instance metadata
static int revert_target_layout(rgw::sal::RadosStore* store,
                                RGWBucketInfo& bucket_info,
				std::map<std::string, bufferlist>& bucket_attrs,
                                ReshardFaultInjector& fault,
                                const DoutPrefixProvider* dpp, optional_yield y)
{
  auto prev = bucket_info.layout; // make a copy for cleanup

  // remove target index shard objects
  int ret = store->svc()->bi->clean_index(dpp, bucket_info, *prev.target_index);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to remove "
        "target index with: " << cpp_strerror(ret) << dendl;
    ret = 0; // non-fatal error
  }

  // retry in case of racing writes to the bucket instance metadata
  static constexpr auto max_retries = 10;
  int tries = 0;
  do {
    // clear target_index and resharding state
    bucket_info.layout.target_index = std::nullopt;
    bucket_info.layout.resharding = rgw::BucketReshardState::None;

    if (ret = fault.check("revert_target_layout");
        ret == 0) { // no fault injected, revert the bucket instance metadata
      ret = store->getRados()->put_bucket_instance_info(bucket_info, false,
                                                        real_time(),
                                                        &bucket_attrs, dpp, y);
    } else if (ret == -ECANCELED) {
      fault.clear(); // clear the fault so a retry can succeed
    }

    if (ret == -ECANCELED) {
      // racing write detected, read the latest bucket info and try again
      int ret2 = store->getRados()->get_bucket_instance_info(
          bucket_info.bucket, bucket_info,
          nullptr, &bucket_attrs, y, dpp);
      if (ret2 < 0) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to read "
            "bucket info: " << cpp_strerror(ret2) << dendl;
        ret = ret2;
        break;
      }

      // check that we're still in the reshard state we started in
      if (bucket_info.layout.resharding == rgw::BucketReshardState::None) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "reshard cancel" << dendl;
        return -ECANCELED;
      }
      if (bucket_info.layout.current_index != prev.current_index ||
          bucket_info.layout.target_index != prev.target_index) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "another reshard" << dendl;
        return -ECANCELED;
      }

      prev = bucket_info.layout; // update the copy
    }
    ++tries;
  } while (ret == -ECANCELED && tries < max_retries);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to clear "
        "target index layout in bucket info: " << cpp_strerror(ret) << dendl;

    bucket_info.layout = std::move(prev);  // restore in-memory layout
    return ret;
  }
  return 0;
} // remove_target_layout

static int init_reshard(rgw::sal::RadosStore* store,
                        RGWBucketInfo& bucket_info,
			std::map<std::string, bufferlist>& bucket_attrs,
                        ReshardFaultInjector& fault,
                        uint32_t new_num_shards,
                        const DoutPrefixProvider *dpp, optional_yield y)
{
  if (new_num_shards == 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " got invalid new_num_shards=0" << dendl;
    return -EINVAL;
  }

  int ret = init_target_layout(store, bucket_info, bucket_attrs, fault, new_num_shards, dpp, y);
  if (ret < 0) {
    return ret;
  }

  if (ret = fault.check("block_writes");
      ret == 0) { // no fault injected, block writes to the current index shards
    ret = set_resharding_status(dpp, store, bucket_info,
                                cls_rgw_reshard_status::IN_PROGRESS);
  }

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to pause "
        "writes to the current index: " << cpp_strerror(ret) << dendl;
    // clean up the target layout (ignore errors)
    revert_target_layout(store, bucket_info, bucket_attrs, fault, dpp, y);
    return ret;
  }
  return 0;
} // init_reshard

static int cancel_reshard(rgw::sal::RadosStore* store,
                          RGWBucketInfo& bucket_info,
			  std::map<std::string, bufferlist>& bucket_attrs,
                          ReshardFaultInjector& fault,
                          const DoutPrefixProvider *dpp, optional_yield y)
{
  // unblock writes to the current index shard objects
  int ret = set_resharding_status(dpp, store, bucket_info,
                                  cls_rgw_reshard_status::NOT_RESHARDING);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to unblock "
        "writes to current index objects: " << cpp_strerror(ret) << dendl;
    ret = 0; // non-fatal error
  }

  if (bucket_info.layout.target_index) {
    return revert_target_layout(store, bucket_info, bucket_attrs, fault, dpp, y);
  }
  // there is nothing to revert
  return 0;
} // cancel_reshard

static int commit_target_layout(rgw::sal::RadosStore* store,
                                RGWBucketInfo& bucket_info,
                                std::map<std::string, bufferlist>& bucket_attrs,
                                ReshardFaultInjector& fault,
                                const DoutPrefixProvider *dpp, optional_yield y)
{
  auto& layout = bucket_info.layout;
  const auto next_log_gen = layout.logs.empty() ? 1 :
      layout.logs.back().gen + 1;

  if (!store->svc()->zone->need_to_log_data()) {
    // if we're not syncing data, we can drop any existing logs
    layout.logs.clear();
  }

  // use the new index layout as current
  ceph_assert(layout.target_index);
  layout.current_index = std::move(*layout.target_index);
  layout.target_index = std::nullopt;
  layout.resharding = rgw::BucketReshardState::None;
  // add the in-index log layout
  layout.logs.push_back(log_layout_from_index(next_log_gen, layout.current_index));

  int ret = fault.check("commit_target_layout");
  if (ret == 0) { // no fault injected, write the bucket instance metadata
    ret = store->getRados()->put_bucket_instance_info(
        bucket_info, false, real_time(), &bucket_attrs, dpp, y);
  } else if (ret == -ECANCELED) {
    fault.clear(); // clear the fault so a retry can succeed
  }
  return ret;
} // commit_target_layout

static int commit_reshard(rgw::sal::RadosStore* store,
                          RGWBucketInfo& bucket_info,
			  std::map<std::string, bufferlist>& bucket_attrs,
                          ReshardFaultInjector& fault,
                          const DoutPrefixProvider *dpp, optional_yield y)
{
  auto prev = bucket_info.layout; // make a copy for cleanup

  // retry in case of racing writes to the bucket instance metadata
  static constexpr auto max_retries = 10;
  int tries = 0;
  int ret = 0;
  do {
    ret = commit_target_layout(store, bucket_info, bucket_attrs, fault, dpp, y);
    if (ret == -ECANCELED) {
      // racing write detected, read the latest bucket info and try again
      int ret2 = store->getRados()->get_bucket_instance_info(
          bucket_info.bucket, bucket_info,
          nullptr, &bucket_attrs, y, dpp);
      if (ret2 < 0) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to read "
            "bucket info: " << cpp_strerror(ret2) << dendl;
        ret = ret2;
        break;
      }

      // check that we're still in the reshard state we started in
      if (bucket_info.layout.resharding != rgw::BucketReshardState::InProgress) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "reshard cancel" << dendl;
        return -ECANCELED; // whatever canceled us already did the cleanup
      }
      if (bucket_info.layout.current_index != prev.current_index ||
          bucket_info.layout.target_index != prev.target_index) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "another reshard" << dendl;
        return -ECANCELED; // whatever canceled us already did the cleanup
      }

      prev = bucket_info.layout; // update the copy
    }
    ++tries;
  } while (ret == -ECANCELED && tries < max_retries);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to commit "
        "target index layout: " << cpp_strerror(ret) << dendl;

    bucket_info.layout = std::move(prev); // restore in-memory layout

    // unblock writes to the current index shard objects
    int ret2 = set_resharding_status(dpp, store, bucket_info,
                                     cls_rgw_reshard_status::NOT_RESHARDING);
    if (ret2 < 0) {
      ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to unblock "
          "writes to current index objects: " << cpp_strerror(ret2) << dendl;
      // non-fatal error
    }
    return ret;
  }

  if (store->svc()->zone->need_to_log_data() && !prev.logs.empty() &&
      prev.current_index.layout.type == rgw::BucketIndexType::Normal) {
    // write a datalog entry for each shard of the previous index. triggering
    // sync on the old shards will force them to detect the end-of-log for that
    // generation, and eventually transition to the next
    // TODO: use a log layout to support types other than BucketLogType::InIndex
    for (uint32_t shard_id = 0; shard_id < rgw::num_shards(prev.current_index.layout.normal); ++shard_id) {
      // This null_yield can stay, for now, since we're in our own thread
      ret = store->svc()->datalog_rados->add_entry(dpp, bucket_info, prev.logs.back(), shard_id,
						   null_yield);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << "WARNING: failed writing data log (bucket_info.bucket="
        << bucket_info.bucket << ", shard_id=" << shard_id << "of generation="
        << prev.logs.back().gen << ")" << dendl;
      } // datalog error is not fatal
    }
  }

  // check whether the old index objects are still needed for bilogs
  const auto& logs = bucket_info.layout.logs;
  auto log = std::find_if(logs.begin(), logs.end(),
      [&prev] (const rgw::bucket_log_layout_generation& log) {
        return log.layout.type == rgw::BucketLogType::InIndex
            && log.layout.in_index.gen == prev.current_index.gen;
      });
  if (log == logs.end()) {
    // delete the index objects (ignore errors)
    store->svc()->bi->clean_index(dpp, bucket_info, prev.current_index);
  }
  return 0;
} // commit_reshard

int RGWBucketReshard::clear_resharding(rgw::sal::RadosStore* store,
                                       RGWBucketInfo& bucket_info,
				       std::map<std::string, bufferlist>& bucket_attrs,
                                       const DoutPrefixProvider* dpp, optional_yield y)
{
  ReshardFaultInjector no_fault;
  return cancel_reshard(store, bucket_info, bucket_attrs, no_fault, dpp, y);
}

int RGWBucketReshard::cancel(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = reshard_lock.lock(dpp);
  if (ret < 0) {
    return ret;
  }

  if (bucket_info.layout.resharding != rgw::BucketReshardState::InProgress) {
    ldpp_dout(dpp, -1) << "ERROR: bucket is not resharding" << dendl;
    ret = -EINVAL;
  } else {
    ret = clear_resharding(store, bucket_info, bucket_attrs, dpp, y);
  }

  reshard_lock.unlock();
  return ret;
}

RGWBucketReshardLock::RGWBucketReshardLock(rgw::sal::RadosStore* _store,
					   const std::string& reshard_lock_oid,
					   bool _ephemeral) :
  store(_store),
  lock_oid(reshard_lock_oid),
  ephemeral(_ephemeral),
  internal_lock(reshard_lock_name)
{
  const int lock_dur_secs = store->ctx()->_conf.get_val<uint64_t>(
    "rgw_reshard_bucket_lock_duration");
  duration = std::chrono::seconds(lock_dur_secs);

#define COOKIE_LEN 16
  char cookie_buf[COOKIE_LEN + 1];
  gen_rand_alphanumeric(store->ctx(), cookie_buf, sizeof(cookie_buf) - 1);
  cookie_buf[COOKIE_LEN] = '\0';

  internal_lock.set_cookie(cookie_buf);
  internal_lock.set_duration(duration);
}

int RGWBucketReshardLock::lock(const DoutPrefixProvider *dpp) {
  internal_lock.set_must_renew(false);

  int ret;
  if (ephemeral) {
    ret = internal_lock.lock_exclusive_ephemeral(&store->getRados()->reshard_pool_ctx,
						 lock_oid);
  } else {
    ret = internal_lock.lock_exclusive(&store->getRados()->reshard_pool_ctx, lock_oid);
  }

  if (ret == -EBUSY) {
    ldout(store->ctx(), 0) << "INFO: RGWReshardLock::" << __func__ <<
      " found lock on " << lock_oid <<
      " to be held by another RGW process; skipping for now" << dendl;
    return ret;
  } else if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: RGWReshardLock::" << __func__ <<
      " failed to acquire lock on " << lock_oid << ": " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  reset_time(Clock::now());

  return 0;
}

void RGWBucketReshardLock::unlock() {
  int ret = internal_lock.unlock(&store->getRados()->reshard_pool_ctx, lock_oid);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "WARNING: RGWBucketReshardLock::" << __func__ <<
      " failed to drop lock on " << lock_oid << " ret=" << ret << dendl;
  }
}

int RGWBucketReshardLock::renew(const Clock::time_point& now) {
  internal_lock.set_must_renew(true);
  int ret;
  if (ephemeral) {
    ret = internal_lock.lock_exclusive_ephemeral(&store->getRados()->reshard_pool_ctx,
						 lock_oid);
  } else {
    ret = internal_lock.lock_exclusive(&store->getRados()->reshard_pool_ctx, lock_oid);
  }
  if (ret < 0) { /* expired or already locked by another processor */
    std::stringstream error_s;
    if (-ENOENT == ret) {
      error_s << "ENOENT (lock expired or never initially locked)";
    } else {
      error_s << ret << " (" << cpp_strerror(-ret) << ")";
    }
    ldout(store->ctx(), 5) << __func__ << "(): failed to renew lock on " <<
      lock_oid << " with error " << error_s.str() << dendl;
    return ret;
  }
  internal_lock.set_must_renew(false);

  reset_time(now);
  ldout(store->ctx(), 20) << __func__ << "(): successfully renewed lock on " <<
    lock_oid << dendl;

  return 0;
}


int RGWBucketReshard::do_reshard(const rgw::bucket_index_layout_generation& current,
                                 const rgw::bucket_index_layout_generation& target,
                                 int max_entries,
				 bool verbose,
				 ostream *out,
				 Formatter *formatter,
                                 const DoutPrefixProvider *dpp, optional_yield y)
{
  if (out) {
    (*out) << "tenant: " << bucket_info.bucket.tenant << std::endl;
    (*out) << "bucket name: " << bucket_info.bucket.name << std::endl;
  }

  /* update bucket info -- in progress*/
  list<rgw_cls_bi_entry> entries;

  if (max_entries < 0) {
    ldpp_dout(dpp, 0) << __func__ <<
      ": can't reshard, negative max_entries" << dendl;
    return -EINVAL;
  }

  BucketReshardManager target_shards_mgr(dpp, store, bucket_info, target);

  bool verbose_json_out = verbose && (formatter != nullptr) && (out != nullptr);

  if (verbose_json_out) {
    formatter->open_array_section("entries");
  }

  uint64_t total_entries = 0;

  if (!verbose_json_out && out) {
    (*out) << "total entries:";
  }

  const uint32_t num_source_shards = rgw::num_shards(current.layout.normal);
  string marker;
  for (uint32_t i = 0; i < num_source_shards; ++i) {
    bool is_truncated = true;
    marker.clear();
    const std::string null_object_filter; // empty string since we're not filtering by object
    while (is_truncated) {
      entries.clear();
      int ret = store->getRados()->bi_list(dpp, bucket_info, i, null_object_filter, marker, max_entries, &entries, &is_truncated, y);
      if (ret == -ENOENT) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to find shard "
            << i << ", skipping" << dendl;
        // break out of the is_truncated loop and move on to the next shard
        break;
      } else if (ret < 0) {
        derr << "ERROR: bi_list(): " << cpp_strerror(-ret) << dendl;
        return ret;
      }

      for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
	rgw_cls_bi_entry& entry = *iter;
	if (verbose_json_out) {
	  formatter->open_object_section("entry");

	  encode_json("shard_id", i, formatter);
	  encode_json("num_entry", total_entries, formatter);
	  encode_json("entry", entry, formatter);
	}
	total_entries++;

	marker = entry.idx;

	int target_shard_id;
	cls_rgw_obj_key cls_key;
	RGWObjCategory category;
	rgw_bucket_category_stats stats;
	bool account = entry.get_info(&cls_key, &category, &stats);
	rgw_obj_key key(cls_key);
	if (entry.type == BIIndexType::OLH && key.empty()) {
	  // bogus entry created by https://tracker.ceph.com/issues/46456
	  // to fix, skip so it doesn't get include in the new bucket instance
	  total_entries--;
	  ldpp_dout(dpp, 10) << "Dropping entry with empty name, idx=" << marker << dendl;
	  continue;
	}
	rgw_obj obj(bucket_info.bucket, key);
	RGWMPObj mp;
	if (key.ns == RGW_OBJ_NS_MULTIPART && mp.from_meta(key.name)) {
	  // place the multipart .meta object on the same shard as its head object
	  obj.index_hash_source = mp.get_key();
	}
	ret = store->getRados()->get_target_shard_id(bucket_info.layout.target_index->layout.normal,
						     obj.get_hash_object(), &target_shard_id);
	if (ret < 0) {
	  ldpp_dout(dpp, -1) << "ERROR: get_target_shard_id() returned ret=" << ret << dendl;
	  return ret;
	}

	int shard_index = (target_shard_id > 0 ? target_shard_id : 0);

	ret = target_shards_mgr.add_entry(shard_index, entry, account,
					  category, stats);
	if (ret < 0) {
	  return ret;
	}

	Clock::time_point now = Clock::now();
	if (reshard_lock.should_renew(now)) {
	  // assume outer locks have timespans at least the size of ours, so
	  // can call inside conditional
	  if (outer_reshard_lock) {
	    ret = outer_reshard_lock->renew(now);
	    if (ret < 0) {
	      return ret;
	    }
	  }
	  ret = reshard_lock.renew(now);
	  if (ret < 0) {
	    ldpp_dout(dpp, -1) << "Error renewing bucket lock: " << ret << dendl;
	    return ret;
	  }
	}
	if (verbose_json_out) {
	  formatter->close_section();
	  formatter->flush(*out);
	} else if (out && !(total_entries % 1000)) {
	  (*out) << " " << total_entries;
	}
      } // entries loop
    }
  }

  if (verbose_json_out) {
    formatter->close_section();
    formatter->flush(*out);
  } else if (out) {
    (*out) << " " << total_entries << std::endl;
  }

  int ret = target_shards_mgr.finish();
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to reshard" << dendl;
    return -EIO;
  }
  return 0;
} // RGWBucketReshard::do_reshard

int RGWBucketReshard::get_status(const DoutPrefixProvider *dpp, list<cls_rgw_bucket_instance_entry> *status)
{
  return store->svc()->bi_rados->get_reshard_status(dpp, bucket_info, status);
}

int RGWBucketReshard::execute(int num_shards,
                              ReshardFaultInjector& fault,
                              int max_op_entries,
			      const cls_rgw_reshard_initiator initiator,
                              const DoutPrefixProvider *dpp,
			      optional_yield y,
                              bool verbose,
			      ostream *out,
                              Formatter *formatter,
                              RGWReshard* reshard_log)
{
  // take a reshard lock on the bucket
  int ret = reshard_lock.lock(dpp);
  if (ret < 0) {
    return ret;
  }
  // unlock when scope exits
  auto unlock = make_scope_guard([this] { reshard_lock.unlock(); });

  if (reshard_log) {
    ret = reshard_log->update(dpp, bucket_info, initiator, y);
    if (ret < 0) {
      return ret;
    }
  }

  // prepare the target index and add its layout the bucket info
  ret = init_reshard(store, bucket_info, bucket_attrs, fault, num_shards, dpp, y);
  if (ret < 0) {
    return ret;
  }

  if (ret = fault.check("do_reshard");
      ret == 0) { // no fault injected, do the reshard
    ret = do_reshard(bucket_info.layout.current_index,
                     *bucket_info.layout.target_index,
                     max_op_entries, verbose, out, formatter, dpp, y);
  }

  if (ret < 0) {
    cancel_reshard(store, bucket_info, bucket_attrs, fault, dpp, y);

    ldpp_dout(dpp, 1) << __func__ << " INFO: reshard of bucket \""
        << bucket_info.bucket.name << "\" canceled due to errors" << dendl;
    return ret;
  }

  ret = commit_reshard(store, bucket_info, bucket_attrs, fault, dpp, y);
  if (ret < 0) {
    return ret;
  }

  ldpp_dout(dpp, 1) << __func__ << " INFO: reshard of bucket \"" <<
    bucket_info.bucket.name << "\" from " <<
    rgw::num_shards(bucket_info.layout.current_index) << " shards to " << num_shards <<
    " shards completed successfully" << dendl;

  return 0;
} // execute

bool RGWBucketReshard::should_zone_reshard_now(const RGWBucketInfo& bucket,
					       const RGWSI_Zone* zone_svc)
{
  return !zone_svc->need_to_log_data() ||
    bucket.layout.logs.size() < max_bilog_history;
}


RGWReshard::RGWReshard(rgw::sal::RadosStore* _store, bool _verbose, ostream *_out,
                       Formatter *_formatter) :
  store(_store), instance_lock(bucket_instance_lock_name),
  verbose(_verbose), out(_out), formatter(_formatter)
{
  num_logshards = store->ctx()->_conf.get_val<uint64_t>("rgw_reshard_num_logs");
}

string RGWReshard::get_logshard_key(const string& tenant,
				    const string& bucket_name)
{
  return tenant + ":" + bucket_name;
}

#define MAX_RESHARD_LOGSHARDS_PRIME 7877

void RGWReshard::get_bucket_logshard_oid(const string& tenant, const string& bucket_name, string *oid)
{
  string key = get_logshard_key(tenant, bucket_name);

  uint32_t sid = ceph_str_hash_linux(key.c_str(), key.size());
  uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
  sid = sid2 % MAX_RESHARD_LOGSHARDS_PRIME % num_logshards;

  get_logshard_oid(int(sid), oid);
}

int RGWReshard::add(const DoutPrefixProvider *dpp, cls_rgw_reshard_entry& entry, optional_yield y)
{
  if (!store->svc()->zone->can_reshard()) {
    ldpp_dout(dpp, 20) << __func__ << " Resharding is disabled"  << dendl;
    return 0;
  }

  string logshard_oid;

  get_bucket_logshard_oid(entry.tenant, entry.bucket_name, &logshard_oid);

  librados::ObjectWriteOperation op;

  // if this is dynamic resharding and we're reducing, we don't want
  // to overwrite an existing entry in order to not interfere with the
  // reshard reduction wait period
  const bool create_only =
    entry.initiator == cls_rgw_reshard_initiator::Dynamic &&
    entry.new_num_shards < entry.old_num_shards;

  cls_rgw_reshard_add(op, entry, create_only);

  int ret = rgw_rados_operate(dpp, store->getRados()->reshard_pool_ctx, logshard_oid, &op, y);
  if (create_only && ret == -EEXIST) {
    ldpp_dout(dpp, 20) <<
      "INFO: did not write reshard queue entry for oid=" <<
      logshard_oid << " tenant=" << entry.tenant << " bucket=" <<
      entry.bucket_name <<
      ", because it's a dynamic reshard reduction and an entry for that "
      "bucket already exists" << dendl;
    // this is not an error so just fall through
  } else if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to add entry to reshard log, oid=" <<
      logshard_oid << " tenant=" << entry.tenant << " bucket=" <<
      entry.bucket_name << dendl;
    return ret;
  }
  return 0;
}

int RGWReshard::update(const DoutPrefixProvider *dpp,
		       const RGWBucketInfo& bucket_info,
		       const cls_rgw_reshard_initiator initiator,
		       optional_yield y)
{
  cls_rgw_reshard_entry entry;
  entry.bucket_name = bucket_info.bucket.name;
  entry.bucket_id = bucket_info.bucket.bucket_id;
  entry.tenant = bucket_info.bucket.tenant;
  entry.initiator = initiator;

  int ret = get(dpp, entry);
  if (ret < 0) {
    return ret;
  }

  ret = add(dpp, entry, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": Error in updating entry bucket " << entry.bucket_name << ": " <<
      cpp_strerror(-ret) << dendl;
  }

  return ret;
}


int RGWReshard::list(const DoutPrefixProvider *dpp, int logshard_num, string& marker, uint32_t max, std::list<cls_rgw_reshard_entry>& entries, bool *is_truncated)
{
  string logshard_oid;

  get_logshard_oid(logshard_num, &logshard_oid);

  int ret = cls_rgw_reshard_list(store->getRados()->reshard_pool_ctx, logshard_oid, marker, max, entries, is_truncated);

  if (ret == -ENOENT) {
    // these shard objects aren't created until we actually write something to
    // them, so treat ENOENT as a successful empty listing
    *is_truncated = false;
    ret = 0;
  } else if (ret == -EACCES) {
    ldpp_dout(dpp, -1) << "ERROR: access denied to pool " << store->svc()->zone->get_zone_params().reshard_pool
                      << ". Fix the pool access permissions of your client" << dendl;
  } else if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to list reshard log entries, oid="
        << logshard_oid << " marker=" << marker << " " << cpp_strerror(ret) << dendl;
  }

  return ret;
}

int RGWReshard::get(const DoutPrefixProvider *dpp, cls_rgw_reshard_entry& entry)
{
  string logshard_oid;

  get_bucket_logshard_oid(entry.tenant, entry.bucket_name, &logshard_oid);

  int ret = cls_rgw_reshard_get(store->getRados()->reshard_pool_ctx, logshard_oid, entry);
  if (ret < 0) {
    if (ret != -ENOENT) {
      ldpp_dout(dpp, -1) << "ERROR: failed to get entry from reshard log, oid=" << logshard_oid << " tenant=" << entry.tenant <<
	" bucket=" << entry.bucket_name << dendl;
    }
    return ret;
  }

  return 0;
}

int RGWReshard::remove(const DoutPrefixProvider *dpp, const cls_rgw_reshard_entry& entry, optional_yield y)
{
  string logshard_oid;

  get_bucket_logshard_oid(entry.tenant, entry.bucket_name, &logshard_oid);

  librados::ObjectWriteOperation op;
  cls_rgw_reshard_remove(op, entry);

  int ret = rgw_rados_operate(dpp, store->getRados()->reshard_pool_ctx, logshard_oid, &op, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to remove entry from reshard log, oid=" << logshard_oid << " tenant=" << entry.tenant << " bucket=" << entry.bucket_name << dendl;
    return ret;
  }

  return ret;
}

int RGWReshard::clear_bucket_resharding(const DoutPrefixProvider *dpp, const string& bucket_instance_oid, cls_rgw_reshard_entry& entry)
{
  int ret = cls_rgw_clear_bucket_resharding(store->getRados()->reshard_pool_ctx, bucket_instance_oid);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to clear bucket resharding, bucket_instance_oid=" << bucket_instance_oid << dendl;
    return ret;
  }

  return 0;
}

int RGWReshardWait::wait(optional_yield y)
{
  std::unique_lock lock(mutex);

  if (going_down) {
    return -ECANCELED;
  }

  if (y) {
    auto& yield = y.get_yield_context();

    Waiter waiter(yield.get_executor());
    waiters.push_back(waiter);
    lock.unlock();

    waiter.timer.expires_after(duration);

    boost::system::error_code ec;
    waiter.timer.async_wait(yield[ec]);

    lock.lock();
    waiters.erase(waiters.iterator_to(waiter));
    return -ec.value();
  }

  cond.wait_for(lock, duration);

  if (going_down) {
    return -ECANCELED;
  }

  return 0;
}

void RGWReshardWait::stop()
{
  std::scoped_lock lock(mutex);
  going_down = true;
  cond.notify_all();
  for (auto& waiter : waiters) {
    // unblock any waiters with ECANCELED
    waiter.timer.cancel();
  }
}

int RGWReshard::process_entry(const cls_rgw_reshard_entry& entry,
                              int max_entries,
			      const DoutPrefixProvider* dpp,
			      optional_yield y)
{
  ldpp_dout(dpp, 20) << __func__ << " resharding " <<
      entry.bucket_name  << dendl;

  rgw_bucket bucket;
  RGWBucketInfo bucket_info;
  std::map<std::string, bufferlist> bucket_attrs;

  // removes the entry and logs a message
  auto clean_up = [this, &dpp, &entry, &y](const std::string_view& reason = "") -> int {
    int ret = remove(dpp, entry, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) <<
	"ERROR removing bucket \"" << entry.bucket_name <<
	"\" from resharding queue, because " <<
	(reason.empty() ? "resharding complete" : reason) <<
	"; error is " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    }

    if (! reason.empty()) {
      ldpp_dout(dpp, 10) <<
	"WARNING: processing reshard reduction on bucket \"" <<
	entry.bucket_name << "\", but cancelling because " <<
	reason << dendl;
    }

    return 0;
  };

  int ret = store->getRados()->get_bucket_info(store->svc(),
                                               entry.tenant,
					       entry.bucket_name,
                                               bucket_info, nullptr,
                                               y, dpp,
					       &bucket_attrs);
  if (ret < 0 || bucket_info.bucket.bucket_id != entry.bucket_id) {
    if (ret < 0) {
      ldpp_dout(dpp, 0) <<  __func__ <<
          ": Error in get_bucket_info for bucket " << entry.bucket_name <<
          ": " << cpp_strerror(-ret) << dendl;
      if (ret != -ENOENT) {
        // any error other than ENOENT will abort
        return ret;
      }

      // we've encountered a reshard queue entry for an apparently
      // non-existent bucket; let's try to recover by cleaning up
      return clean_up("bucket does not currently exist");
    } else {
      return clean_up("bucket already resharded");
    }
  }

  // if *dynamic* reshard reduction, perform extra sanity checks in
  // part to prevent chasing constantly changing entry count. If
  // *admin*-initiated (or unknown-initiated) reshard reduction, skip
  // this step and proceed.
  if (entry.initiator == cls_rgw_reshard_initiator::Dynamic &&
      entry.new_num_shards < entry.old_num_shards) {
    const bool may_reduce =
      store->ctx()->_conf.get_val<bool>("rgw_dynamic_resharding_may_reduce");
    if (! may_reduce) {
      return clean_up("current configuration does not allow reshard reduction");
    }

    // determine how many entries there are in the bucket index
    std::map<RGWObjCategory, RGWStorageStats> stats;
    ret = store->getRados()->get_bucket_stats(dpp, bucket_info,
					      bucket_info.layout.current_index,
					      -1, nullptr, nullptr, stats, nullptr, nullptr);

    // determine current number of bucket entries across shards
    uint64_t num_entries = 0;
    for (const auto& s : stats) {
      num_entries += s.second.num_objects;
    }

    const uint32_t current_shard_count =
      rgw::num_shards(bucket_info.get_current_index().layout.normal);

    bool needs_resharding { false };
    uint32_t suggested_shard_count { 0 };
    // calling this rados function determines various rados values
    // needed to perform the calculation before calling
    // calculating_preferred_shards() in this class
    store->getRados()->calculate_preferred_shards(
      dpp, num_entries, current_shard_count,
      needs_resharding, &suggested_shard_count);

    // if we no longer need resharding or currently need to expand
    // number of shards, drop this request
    if (! needs_resharding || suggested_shard_count > current_shard_count) {
      return clean_up("reshard reduction no longer appropriate");
    }

    // see if it's been long enough since this reshard queue entry was
    // added to actually do the reshard reduction
    ceph::real_time when_queued = entry.time;
    ceph::real_time now = real_clock::now();

    // use double so we can handle fractions
    double reshard_reduction_wait_hours =
      uint32_t(store->ctx()->_conf.get_val<uint64_t>("rgw_dynamic_resharding_reduction_wait"));

    // see if we have to reduce the waiting interval due to debug
    // config
    int debug_interval = store->ctx()->_conf.get_val<int64_t>("rgw_reshard_debug_interval");
    if (debug_interval >= 1) {
      constexpr int secs_per_day = 60 * 60 * 24;
      reshard_reduction_wait_hours = reshard_reduction_wait_hours * debug_interval / secs_per_day;
    }

    auto timespan = std::chrono::seconds(int(60 * 60 * reshard_reduction_wait_hours));
    if (now < when_queued + timespan) {
      // too early to reshard; log and skip
      ldpp_dout(dpp, 20) <<  __func__ <<
	": INFO: reshard reduction for bucket \"" <<
	entry.bucket_name << "\" will not proceed until " <<
	(when_queued + timespan) << dendl;

      return 0;
    }

    // only if we allow the resharding logic to continue should we log
    // the fact that the reduction_wait_time was shortened due to
    // debugging mode
    if (debug_interval >= 1) {
      ldpp_dout(dpp, 0) << "DEBUG: since the rgw_reshard_debug_interval is set at " <<
	debug_interval << " the rgw_dynamic_resharding_reduction_wait is now " <<
	reshard_reduction_wait_hours << " hours (" <<
	int(reshard_reduction_wait_hours * 60 * 60) << " seconds) and bucket \"" <<
	entry.bucket_name << "\" has reached the reduction wait period" << dendl;
    }

    // all checks passed; we can drop through and proceed
  }

  if (!RGWBucketReshard::should_zone_reshard_now(bucket_info, store->svc()->zone)) {
    return clean_up("bucket not eligible for resharding until peer "
		    "zones finish syncing one or more of its old log "
		    "generations");
  }

  // all checkes passed; we can reshard...

  RGWBucketReshard br(store, bucket_info, bucket_attrs, nullptr);

  ReshardFaultInjector f; // no fault injected
  ret = br.execute(entry.new_num_shards, f, max_entries, entry.initiator,
		   dpp, y, false, nullptr, nullptr, this);
  if (ret < 0) {
    ldpp_dout(dpp, 0) <<  __func__ <<
        ": Error during resharding bucket " << entry.bucket_name << ":" <<
        cpp_strerror(-ret)<< dendl;
    return ret;
  }

  ldpp_dout(dpp, 20) << __func__ <<
      " removing reshard queue entry for bucket " << entry.bucket_name <<
      dendl;

  return clean_up();
} // RGWReshard::process_entry


int RGWReshard::process_single_logshard(int logshard_num, const DoutPrefixProvider *dpp, optional_yield y)
{
  string marker;
  bool truncated = true;

  constexpr uint32_t max_entries = 1000;

  string logshard_oid;
  get_logshard_oid(logshard_num, &logshard_oid);

  RGWBucketReshardLock logshard_lock(store, logshard_oid, false);

  int ret = logshard_lock.lock(dpp);
  if (ret < 0) { 
    ldpp_dout(dpp, 5) << __func__ << "(): failed to acquire lock on " <<
      logshard_oid << ", ret = " << ret <<dendl;
    return ret;
  }
  
  do {
    std::list<cls_rgw_reshard_entry> entries;
    ret = list(dpp, logshard_num, marker, max_entries, entries, &truncated);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "cannot list all reshards in logshard oid=" <<
	logshard_oid << dendl;
      continue;
    }

    for(auto& entry : entries) { // logshard entries
      process_entry(entry, max_entries, dpp, y);

      Clock::time_point now = Clock::now();
      if (logshard_lock.should_renew(now)) {
        ret = logshard_lock.renew(now);
        if (ret < 0) {
          return ret;
        }
      }

      entry.get_key(&marker);
    } // entry for loop
  } while (truncated);

  logshard_lock.unlock();
  return 0;
}


void RGWReshard::get_logshard_oid(int shard_num, string *logshard)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%010u", (unsigned)shard_num);

  string objname(reshard_oid_prefix);
  *logshard =  objname + buf;
}

int RGWReshard::process_all_logshards(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = 0;

  for (int i = 0; i < num_logshards; i++) {
    string logshard;
    get_logshard_oid(i, &logshard);

    ldpp_dout(dpp, 20) << "processing logshard = " << logshard << dendl;

    ret = process_single_logshard(i, dpp, y);

    ldpp_dout(dpp, 20) << "finish processing logshard = " << logshard << " , ret = " << ret << dendl;
  }

  return 0;
}

bool RGWReshard::going_down()
{
  return down_flag;
}

void RGWReshard::start_processor()
{
  worker = new ReshardWorker(store->ctx(), this);
  worker->create("rgw_reshard");
}

void RGWReshard::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = nullptr;
}

void *RGWReshard::ReshardWorker::entry() {
  const auto debug_interval = cct->_conf.get_val<int64_t>("rgw_reshard_debug_interval");
  double interval_factor = 1.0;
  if (debug_interval >= 1) {
    constexpr double secs_per_day = 60 * 60 * 24;
    interval_factor = debug_interval / secs_per_day;

    ldpp_dout(this, 0) << "DEBUG: since the rgw_reshard_debug_interval is set at " <<
      debug_interval << " the rgw_reshard_thread_interval will be "
      "multiplied by a factor of " << interval_factor << dendl;
  }

  do {
    utime_t start = ceph_clock_now();
    reshard->process_all_logshards(this, null_yield);

    if (reshard->going_down())
      break;

    utime_t end = ceph_clock_now();
    utime_t elapsed = end - start;

    int secs = cct->_conf.get_val<uint64_t>("rgw_reshard_thread_interval");
    secs = std::max(1, int(secs * interval_factor));

    if (secs <= elapsed.sec()) {
      continue; // next round
    }

    secs -= elapsed.sec();

    // note: this will likely wait for the intended period of
    // time, but could wait for less
    std::unique_lock locker{lock};
    cond.wait_for(locker, std::chrono::seconds(secs));
  } while (!reshard->going_down());

  return NULL;
}

void RGWReshard::ReshardWorker::stop()
{
  std::lock_guard l{lock};
  cond.notify_all();
}

CephContext *RGWReshard::ReshardWorker::get_cct() const
{
  return cct;
}

unsigned RGWReshard::ReshardWorker::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWReshard::ReshardWorker::gen_prefix(std::ostream& out) const
{
  return out << "rgw reshard worker thread: ";
}
