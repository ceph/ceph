// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <vector>
#include <initializer_list>
#include <functional>
#include <iterator>
#include <algorithm>

#include <boost/intrusive/list.hpp>
#include <boost/asio/basic_waitable_timer.hpp>

#include "include/common_fwd.h"
#include "include/rados/librados.hpp"
#include "common/ceph_time.h"
#include "common/async/yield_context.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/lock/cls_lock_client.h"

#include "rgw_common.h"
#include "common/fault_injector.h"


class RGWReshard;
namespace rgw { namespace sal {
  class RadosStore;
} }

using ReshardFaultInjector = FaultInjector<std::string_view>;

class RGWBucketReshardLock {
  using Clock = ceph::coarse_mono_clock;

  rgw::sal::RadosStore* store;
  const std::string lock_oid;
  const bool ephemeral;
  rados::cls::lock::Lock internal_lock;
  std::chrono::seconds duration;

  Clock::time_point start_time;
  Clock::time_point renew_thresh;

  void reset_time(const Clock::time_point& now) {
    start_time = now;
    renew_thresh = start_time + duration / 2;
  }

public:
  RGWBucketReshardLock(rgw::sal::RadosStore* _store,
		       const std::string& reshard_lock_oid,
		       bool _ephemeral);
  RGWBucketReshardLock(rgw::sal::RadosStore* _store,
		       const RGWBucketInfo& bucket_info,
		       bool _ephemeral) :
    RGWBucketReshardLock(_store, bucket_info.bucket.get_key(':'), _ephemeral)
  {}

  int lock(const DoutPrefixProvider *dpp);
  void unlock();
  int renew(const Clock::time_point&);

  bool should_renew(const Clock::time_point& now) const {
    return now >= renew_thresh;
  }
}; // class RGWBucketReshardLock

class RGWBucketReshard {
 public:
  using Clock = ceph::coarse_mono_clock;

 private:
  rgw::sal::RadosStore *store;
  RGWBucketInfo bucket_info;
  std::map<std::string, bufferlist> bucket_attrs;

  RGWBucketReshardLock reshard_lock;
  RGWBucketReshardLock* outer_reshard_lock;

  // using an initializer_list as an array in contiguous memory
  // allocated in at once
  static const std::initializer_list<uint16_t> reshard_primes;

  int do_reshard(const rgw::bucket_index_layout_generation& current,
                 const rgw::bucket_index_layout_generation& target,
                 int max_entries,
                 bool verbose,
                 std::ostream *os,
		 Formatter *formatter,
                 const DoutPrefixProvider *dpp, optional_yield y);
public:

  // pass nullptr for the final parameter if no outer reshard lock to
  // manage
  RGWBucketReshard(rgw::sal::RadosStore* _store,
		   const RGWBucketInfo& _bucket_info,
		   const std::map<std::string, bufferlist>& _bucket_attrs,
		   RGWBucketReshardLock* _outer_reshard_lock);
  int execute(int num_shards, ReshardFaultInjector& f,
              int max_op_entries, const DoutPrefixProvider *dpp, optional_yield y,
              bool verbose = false, std::ostream *out = nullptr,
              ceph::Formatter *formatter = nullptr,
	      RGWReshard *reshard_log = nullptr);
  int get_status(const DoutPrefixProvider *dpp, std::list<cls_rgw_bucket_instance_entry> *status);
  int cancel(const DoutPrefixProvider* dpp, optional_yield y);

  static int clear_resharding(rgw::sal::RadosStore* store,
			      RGWBucketInfo& bucket_info,
			      std::map<std::string, bufferlist>& bucket_attrs,
                              const DoutPrefixProvider* dpp, optional_yield y);

  static uint32_t get_max_prime_shards() {
    return *std::crbegin(reshard_primes);
  }

  // returns the prime in our list less than or equal to the
  // parameter; the lowest value that can be returned is 1
  static uint32_t get_prime_shards_less_or_equal(uint32_t requested_shards) {
    auto it = std::upper_bound(reshard_primes.begin(), reshard_primes.end(),
			       requested_shards);
    if (it == reshard_primes.begin()) {
      return 1;
    } else {
      return *(--it);
    }
  }

  // returns the prime in our list greater than or equal to the
  // parameter; if we do not have such a prime, 0 is returned
  static uint32_t get_prime_shards_greater_or_equal(
    uint32_t requested_shards)
  {
    auto it = std::lower_bound(reshard_primes.begin(), reshard_primes.end(),
			       requested_shards);
    if (it == reshard_primes.end()) {
      return 0;
    } else {
      return *it;
    }
  }

  // returns a preferred number of shards given a calculated number of
  // shards based on max_dynamic_shards and the list of prime values
  static uint32_t get_preferred_shards(uint32_t suggested_shards,
				       uint32_t max_dynamic_shards) {

    // use a prime if max is within our prime range, otherwise use
    // specified max
    const uint32_t absolute_max =
      max_dynamic_shards >= get_max_prime_shards() ?
      max_dynamic_shards :
      get_prime_shards_less_or_equal(max_dynamic_shards);

    // if we can use a prime number, use it, otherwise use suggested;
    // note get_prime_shards_greater_or_equal will return 0 if no prime in
    // prime range
    const uint32_t prime_ish_num_shards =
      std::max(get_prime_shards_greater_or_equal(suggested_shards),
	       suggested_shards);

    // dynamic sharding cannot reshard more than defined maximum
    const uint32_t final_num_shards =
      std::min(prime_ish_num_shards, absolute_max);

    return final_num_shards;
  }

  const std::map<std::string, bufferlist>& get_bucket_attrs() const {
    return bucket_attrs;
  }

  // for multisite, the RGWBucketInfo keeps a history of old log generations
  // until all peers are done with them. prevent this log history from growing
  // too large by refusing to reshard the bucket until the old logs get trimmed
  static constexpr size_t max_bilog_history = 4;

  static bool should_zone_reshard_now(const RGWBucketInfo& bucket,
				      const RGWSI_Zone* zone_svc);
}; // RGWBucketReshard


class RGWReshard {
public:
    using Clock = ceph::coarse_mono_clock;

private:
    rgw::sal::RadosStore* store;
    std::string lock_name;
    rados::cls::lock::Lock instance_lock;
    int num_logshards;

    bool verbose;
    std::ostream *out;
    Formatter *formatter;

    void get_logshard_oid(int shard_num, std::string *shard);
protected:
  class ReshardWorker : public Thread, public DoutPrefixProvider {
    CephContext *cct;
    RGWReshard *reshard;
    ceph::mutex lock = ceph::make_mutex("ReshardWorker");
    ceph::condition_variable cond;

  public:
    ReshardWorker(CephContext * const _cct,
		  RGWReshard * const _reshard)
      : cct(_cct),
        reshard(_reshard) {}

    void *entry() override;
    void stop();

    CephContext *get_cct() const override;
    unsigned get_subsys() const override;
    std::ostream& gen_prefix(std::ostream& out) const override;
  };

  ReshardWorker *worker = nullptr;
  std::atomic<bool> down_flag = { false };

  std::string get_logshard_key(const std::string& tenant, const std::string& bucket_name);
  void get_bucket_logshard_oid(const std::string& tenant, const std::string& bucket_name, std::string *oid);

public:
  RGWReshard(rgw::sal::RadosStore* _store, bool _verbose = false, std::ostream *_out = nullptr, Formatter *_formatter = nullptr);
  int add(const DoutPrefixProvider *dpp, cls_rgw_reshard_entry& entry, optional_yield y);
  int update(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, optional_yield y);
  int get(const DoutPrefixProvider *dpp, cls_rgw_reshard_entry& entry);
  int remove(const DoutPrefixProvider *dpp, const cls_rgw_reshard_entry& entry, optional_yield y);
  int list(const DoutPrefixProvider *dpp, int logshard_num, std::string& marker, uint32_t max, std::list<cls_rgw_reshard_entry>& entries, bool *is_truncated);
  int clear_bucket_resharding(const DoutPrefixProvider *dpp, const std::string& bucket_instance_oid, cls_rgw_reshard_entry& entry);

  /* reshard thread */
  int process_entry(const cls_rgw_reshard_entry& entry, int max_entries,
                    const DoutPrefixProvider *dpp, optional_yield y);
  int process_single_logshard(int logshard_num, const DoutPrefixProvider *dpp, optional_yield y);
  int process_all_logshards(const DoutPrefixProvider *dpp, optional_yield y);
  bool going_down();
  void start_processor();
  void stop_processor();
};

class RGWReshardWait {
 public:
  // the blocking wait uses std::condition_variable::wait_for(), which uses the
  // std::chrono::steady_clock. use that for the async waits as well
  using Clock = std::chrono::steady_clock;
 private:
  const ceph::timespan duration;
  ceph::mutex mutex = ceph::make_mutex("RGWReshardWait::lock");
  ceph::condition_variable cond;

  struct Waiter : boost::intrusive::list_base_hook<> {
    using Executor = boost::asio::io_context::executor_type;
    using Timer = boost::asio::basic_waitable_timer<Clock,
          boost::asio::wait_traits<Clock>, Executor>;
    Timer timer;
    explicit Waiter(boost::asio::io_context& ioc) : timer(ioc) {}
  };
  boost::intrusive::list<Waiter> waiters;

  bool going_down{false};

public:
  RGWReshardWait(ceph::timespan duration = std::chrono::seconds(5))
    : duration(duration) {}
  ~RGWReshardWait() {
    ceph_assert(going_down);
  }
  int wait(optional_yield y);
  // unblock any threads waiting on reshard
  void stop();
};
