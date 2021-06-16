// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_RESHARD_H
#define RGW_RESHARD_H

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


class RGWReshard;
namespace rgw { namespace sal {
  class RadosStore;
} }

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

  friend class RGWReshard;

  using Clock = ceph::coarse_mono_clock;

private:

  rgw::sal::RadosStore* store;
  RGWBucketInfo bucket_info;
  std::map<string, bufferlist> bucket_attrs;

  RGWBucketReshardLock reshard_lock;
  RGWBucketReshardLock* outer_reshard_lock;

  // using an initializer_list as an array in contiguous memory
  // allocated in at once
  static const std::initializer_list<uint16_t> reshard_primes;

  int create_new_bucket_instance(int new_num_shards,
				 RGWBucketInfo& new_bucket_info,
                                 const DoutPrefixProvider *dpp);
  int do_reshard(int num_shards,
		 RGWBucketInfo& new_bucket_info,
		 int max_entries,
                 bool verbose,
                 ostream *os,
		 Formatter *formatter,
                 const DoutPrefixProvider *dpp);
public:

  // pass nullptr for the final parameter if no outer reshard lock to
  // manage
  RGWBucketReshard(rgw::sal::RadosStore* _store,
		   const RGWBucketInfo& _bucket_info,
                   const std::map<string, bufferlist>& _bucket_attrs,
		   RGWBucketReshardLock* _outer_reshard_lock);
  int execute(int num_shards, int max_op_entries,
              const DoutPrefixProvider *dpp,
              bool verbose = false, ostream *out = nullptr,
              Formatter *formatter = nullptr,
	      RGWReshard *reshard_log = nullptr);
  int get_status(const DoutPrefixProvider *dpp, std::list<cls_rgw_bucket_instance_entry> *status);
  int cancel(const DoutPrefixProvider *dpp);
  static int clear_resharding(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store,
			      const RGWBucketInfo& bucket_info);
  int clear_resharding(const DoutPrefixProvider *dpp) {
    return clear_resharding(dpp, store, bucket_info);
  }
  static int clear_index_shard_reshard_status(const DoutPrefixProvider *dpp,
                                              rgw::sal::RadosStore* store,
					      const RGWBucketInfo& bucket_info);
  int clear_index_shard_reshard_status(const DoutPrefixProvider *dpp) {
    return clear_index_shard_reshard_status(dpp, store, bucket_info);
  }
  static int set_resharding_status(const DoutPrefixProvider *dpp,
                                   rgw::sal::RadosStore* store,
				   const RGWBucketInfo& bucket_info,
				   const string& new_instance_id,
				   int32_t num_shards,
				   cls_rgw_reshard_status status);
  int set_resharding_status(const DoutPrefixProvider *dpp, const string& new_instance_id,
			    int32_t num_shards,
			    cls_rgw_reshard_status status) {
    return set_resharding_status(dpp, store, bucket_info,
				 new_instance_id, num_shards, status);
  }

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
}; // RGWBucketReshard


class RGWReshard {
public:
    using Clock = ceph::coarse_mono_clock;

private:
    rgw::sal::RadosStore* store;
    string lock_name;
    rados::cls::lock::Lock instance_lock;
    int num_logshards;

    bool verbose;
    ostream *out;
    Formatter *formatter;

    void get_logshard_oid(int shard_num, string *shard);
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
    unsigned get_subsys() const;
    std::ostream& gen_prefix(std::ostream& out) const;
  };

  ReshardWorker *worker = nullptr;
  std::atomic<bool> down_flag = { false };

  string get_logshard_key(const string& tenant, const string& bucket_name);
  void get_bucket_logshard_oid(const string& tenant, const string& bucket_name, string *oid);

public:
  RGWReshard(rgw::sal::RadosStore* _store, bool _verbose = false, ostream *_out = nullptr, Formatter *_formatter = nullptr);
  int add(const DoutPrefixProvider *dpp, cls_rgw_reshard_entry& entry);
  int update(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const RGWBucketInfo& new_bucket_info);
  int get(const DoutPrefixProvider *dpp, cls_rgw_reshard_entry& entry);
  int remove(const DoutPrefixProvider *dpp, cls_rgw_reshard_entry& entry);
  int list(const DoutPrefixProvider *dpp, int logshard_num, string& marker, uint32_t max, std::list<cls_rgw_reshard_entry>& entries, bool *is_truncated);
  int clear_bucket_resharding(const DoutPrefixProvider *dpp, const string& bucket_instance_oid, cls_rgw_reshard_entry& entry);

  /* reshard thread */
  int process_single_logshard(int logshard_num, const DoutPrefixProvider *dpp);
  int process_all_logshards(const DoutPrefixProvider *dpp);
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

#endif
