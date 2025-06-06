// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <boost/asio/steady_timer.hpp>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/function2.hpp"

#include "common/async/async_cond.h"
#include "common/async/yield_context.h"

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "common/Formatter.h"
#include "common/lru_map.h"

#include "cls/log/cls_log_types.h"
#include "neorados/cls/sem_set.h"
#include "rgw_basic_types.h"
#include "rgw_log_backing.h"
#include "rgw_sync_policy.h"
#include "rgw_trim_bilog.h"
#include "rgw_zone.h"

#include "common/async/spawn_group.h"

namespace asio = boost::asio;
namespace bc = boost::container;

enum DataLogEntityType {
  ENTITY_TYPE_UNKNOWN = 0,
  ENTITY_TYPE_BUCKET = 1,
};
inline std::ostream& operator <<(std::ostream& m,
				 const DataLogEntityType& t) {
  switch (t) {
  case ENTITY_TYPE_BUCKET:
    return m << "bucket";
  default:
    return m << "unknown";
  }
}

struct rgw_data_change {
  DataLogEntityType entity_type;
  std::string key;
  ceph::real_time timestamp;
  uint64_t gen = 0;

  void encode(ceph::buffer::list& bl) const {
    // require decoders to recognize v2 when gen>0
    const uint8_t compat = (gen == 0) ? 1 : 2;
    ENCODE_START(2, compat, bl);
    auto t = std::uint8_t(entity_type);
    encode(t, bl);
    encode(key, bl);
    encode(timestamp, bl);
    encode(gen, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(2, bl);
     std::uint8_t t;
     decode(t, bl);
     entity_type = DataLogEntityType(t);
     decode(key, bl);
     decode(timestamp, bl);
     if (struct_v < 2) {
       gen = 0;
     } else {
       decode(gen, bl);
     }
     DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
  static void generate_test_instances(std::list<rgw_data_change *>& l);
};
WRITE_CLASS_ENCODER(rgw_data_change)
inline std::ostream& operator <<(std::ostream& m,
				 const rgw_data_change& c) {
  return m << "[entity_type: " << c.entity_type
	   << ", key: " << c.key
	   << ", timestamp: " << c.timestamp
	   << ", gen: " << c.gen << "]";
}

struct rgw_data_change_log_entry {
  std::string log_id;
  ceph::real_time log_timestamp;
  rgw_data_change entry;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(log_id, bl);
    encode(log_timestamp, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(log_id, bl);
     decode(log_timestamp, bl);
     decode(entry, bl);
     DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(rgw_data_change_log_entry)
inline std::ostream& operator <<(std::ostream& m,
				 const rgw_data_change_log_entry& e) {
  return m << "[log_id: " << e.log_id
	   << ", log_timestamp: " << e.log_timestamp
	   << ", entry: " << e.entry << "]";
}

struct RGWDataChangesLogInfo {
  std::string marker;
  ceph::real_time last_update;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct RGWDataChangesLogMarker {
  int shard = 0;
  std::string marker;

  RGWDataChangesLogMarker() = default;
  RGWDataChangesLogMarker(int shard, std::string marker)
    : shard(shard), marker(std::move(marker)) {}

  operator bool() const {
    return (shard > 0 || !marker.empty());
  }

  void clear() {
    shard = 0;
    marker.clear();
  }
};

class RGWDataChangesLog;

struct rgw_data_notify_entry {
  std::string key;
  uint64_t gen = 0;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);

  rgw_data_notify_entry& operator=(const rgw_data_notify_entry&) = default;

  bool operator <(const rgw_data_notify_entry& d) const {
    if (key < d.key) {
      return true;
    }
    if (d.key < key) {
      return false;
    }
    return gen < d.gen;
  }
  friend std::ostream& operator <<(std::ostream& m,
				   const rgw_data_notify_entry& e) {
    return m << "[key: " << e.key << ", gen: " << e.gen << "]";
  }
};

class RGWDataChangesBE;

class DataLogBackends final
  : public logback_generations,
    private bc::flat_map<uint64_t, boost::intrusive_ptr<RGWDataChangesBE>> {
  friend class logback_generations;
  friend class GenTrim;

  std::mutex m;
  RGWDataChangesLog& datalog;

  DataLogBackends(neorados::RADOS& rados,
		  const neorados::Object oid,
		  const neorados::IOContext& loc,
		  fu2::unique_function<std::string(
		    uint64_t, int) const>&& get_oid,
		  int shards, RGWDataChangesLog& datalog) noexcept
    : logback_generations(rados, oid, loc, std::move(get_oid),
			  shards), datalog(datalog) {}
public:

  boost::intrusive_ptr<RGWDataChangesBE> head() {
    std::unique_lock l(m);
    auto i = end();
    --i;
    return i->second;
  }
  asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			     std::string>>
  list(const DoutPrefixProvider *dpp, int shard,
       std::span<rgw_data_change_log_entry> entries,
       std::string marker);
  asio::awaitable<void> trim_entries(const DoutPrefixProvider *dpp, int shard_id,
				     std::string_view marker);
  void handle_init(entries_t e) override;
  void handle_new_gens(entries_t e) override;
  void handle_empty_to(uint64_t new_tail) override;

  asio::awaitable<void> trim_generations(const DoutPrefixProvider *dpp,
					 std::optional<uint64_t>& through);
};

struct BucketGen {
  rgw_bucket_shard shard;
  uint64_t gen = 0;

  BucketGen(const rgw_bucket_shard& shard, uint64_t gen)
    : shard(shard), gen(gen) {}

  BucketGen(rgw_bucket_shard&& shard, uint64_t gen)
    : shard(std::move(shard)), gen(gen) {}

  BucketGen(std::string_view key) {
    {
      auto genpos = key.rfind(':');
      if (genpos == key.npos) {
	throw sys::system_error{
	  EILSEQ, sys::generic_category(),
	  fmt::format("Missing delimeter for generation in: {}", key)};
      }
      auto gensub = key.substr(genpos + 1);
      auto maybegen = ceph::parse<decltype(gen)>(gensub);
      if (!maybegen) {
	throw sys::system_error{
	  EILSEQ, sys::generic_category(),
	  fmt::format("Invalid generation: {}", gensub)};
      }
      gen = *maybegen;
      key.remove_suffix(key.size() - genpos);
    }
    std::string_view name{key};
    std::string_view instance;

    // split tenant/name
    auto pos = name.find('/');
    if (pos != name.npos) {
      auto tenant = name.substr(0, pos);
      shard.bucket.tenant.assign(tenant.begin(), tenant.end());
      name = name.substr(pos + 1);
    } else {
      shard.bucket.tenant.clear();
    }

    // split name:instance
    pos = name.find(':');
    if (pos != name.npos) {
      instance = name.substr(pos + 1);
      name = name.substr(0, pos);
    }
    shard.bucket.name.assign(name.begin(), name.end());

    // split instance:shard
    pos = instance.find(':');
    if (pos == instance.npos) {
      auto maybeshard = ceph::parse<decltype(gen)>(instance);
      if (maybeshard) {
	shard.shard_id = *maybeshard;
      } else {
	shard.bucket.bucket_id.assign(instance.begin(), instance.end());
	shard.shard_id = 0;
      }
    } else {
      auto shardsub = instance.substr(pos + 1);
      auto maybeshard = ceph::parse<decltype(gen)>(shardsub);
      if (!maybeshard) {
	throw sys::system_error{
	  EILSEQ, sys::generic_category(),
	  fmt::format("Invalid shard: {}", shardsub)};
      }
      shard.shard_id = *maybeshard;
      instance = instance.substr(0, pos);
      shard.bucket.bucket_id.assign(instance.begin(), instance.end());
    }
  }

  BucketGen(const BucketGen&) = default;
  BucketGen(BucketGen&&) = default;
  BucketGen& operator =(const BucketGen&) = default;
  BucketGen& operator =(BucketGen&&) = default;

  ~BucketGen() = default;

  auto get_key() const {
    return fmt::format("{}:{:0>20}", shard.get_key(), gen);
  }
};

inline bool operator ==(const BucketGen& l, const BucketGen& r) {
  return (l.shard == r.shard) && (l.gen == r.gen);
}

inline bool operator <(const BucketGen& l, const BucketGen& r) {
  if (l.shard < r.shard) {
    return true;
  } else if (l.shard == r.shard) {
    return l.gen < r.gen;
  } else {
    return false;
  }
}

inline std::ostream& operator <<(std::ostream& m, const BucketGen& bg) {
  return m << "{" << bg.shard << ", " << bg.gen << "}";
}

namespace std {
template <>
struct hash<BucketGen> {
  std::size_t operator ()(const BucketGen& bg) const noexcept {
    return (hash<decltype(bg.shard)>{}(bg.shard) << 1)
          ^ hash<decltype(bg.gen)>{}(bg.gen);
  }
};
}

class RGWDataChangesLog {
  friend class DataLogTestBase;
  friend DataLogBackends;
  CephContext *cct;
  neorados::RADOS* rados;
  std::optional<asio::strand<asio::io_context::executor_type>> cancel_strand;
  neorados::IOContext loc;
  rgw::BucketChangeObserver *observer = nullptr;
  bool log_data = false;
  std::unique_ptr<DataLogBackends> bes;

  std::shared_ptr<asio::cancellation_signal> renew_signal =
    std::make_shared<asio::cancellation_signal>();
  std::shared_ptr<asio::cancellation_signal> watch_signal =
    std::make_shared<asio::cancellation_signal>();
  std::shared_ptr<asio::cancellation_signal> recovery_signal =
    std::make_shared<asio::cancellation_signal>();
  ceph::mono_time last_recovery = ceph::mono_clock::zero();

  const int num_shards;
  std::string get_prefix() { return "data_log"; }
  std::string metadata_log_oid() {
    return get_prefix() + "generations_metadata";
  }
  std::string prefix;

  std::mutex lock;
  std::shared_mutex modified_lock;
  bc::flat_map<int, bc::flat_set<rgw_data_notify_entry>> modified_shards;

  std::atomic<bool> down_flag = { true };
  bool ran_background = false;

  struct ChangeStatus {
    std::shared_ptr<const rgw_sync_policy_info> sync_policy;
    ceph::real_time cur_expiration;
    ceph::real_time cur_sent;
    bool pending = false;
    ceph::async::async_cond<boost::asio::io_context::executor_type> cond;
    std::mutex lock;

    ChangeStatus(boost::asio::io_context::executor_type executor)
      : cond(executor) {}
  };

  using ChangeStatusPtr = std::shared_ptr<ChangeStatus>;

  lru_map<BucketGen, ChangeStatusPtr> changes;
  const uint64_t sem_max_keys = neorados::cls::sem_set::max_keys;

  bc::flat_set<BucketGen> cur_cycle;
  std::vector<bc::flat_set<std::string>> semaphores{unsigned(num_shards)};

  ChangeStatusPtr _get_change(const rgw_bucket_shard& bs, uint64_t gen);
  bool register_renew(BucketGen bg);
  void update_renewed(const rgw_bucket_shard& bs,
		      uint64_t gen,
		      ceph::real_time expiration);

  std::optional<asio::steady_timer> renew_timer;
  asio::awaitable<void> renew_run(decltype(renew_signal) renew_signal);
  void renew_stop();

  std::function<bool(const rgw_bucket& bucket, optional_yield y,
                     const DoutPrefixProvider *dpp)> bucket_filter;
  bool going_down() const;
  bool filter_bucket(const DoutPrefixProvider* dpp,
		     const rgw_bucket& bucket,
		     asio::yield_context y) const;
  asio::awaitable<void> renew_entries(const DoutPrefixProvider *dpp);

  uint64_t watchcookie = 0;

public:

  RGWDataChangesLog(CephContext* cct);
  // For testing.
  RGWDataChangesLog(CephContext* cct, bool log_data,
		    neorados::RADOS* rados,
		    std::optional<int> num_shards = std::nullopt,
		    std::optional<uint64_t> sem_max_keys = std::nullopt);
  ~RGWDataChangesLog();

  asio::awaitable<void> start(const DoutPrefixProvider* dpp,
			      const rgw_pool& log_pool,
			      // Broken out for testing, in use
			      // they're either all on (radosgw) or
			      // all off (radosgw-admin)
			      bool recovery, bool watch, bool renew);

  int start(const DoutPrefixProvider *dpp, const RGWZone* _zone,
	    const RGWZoneParams& zoneparams, rgw::sal::RadosStore* store,
	    bool background_tasks);
  asio::awaitable<bool> establish_watch(const DoutPrefixProvider* dpp,
					std::string_view oid);
  asio::awaitable<void> process_notification(const DoutPrefixProvider* dpp,
					     std::string_view oid);
  asio::awaitable<void> watch_loop(decltype(watch_signal));
  int choose_oid(const rgw_bucket_shard& bs);
  asio::awaitable<void> add_entry(const DoutPrefixProvider *dpp,
				  const RGWBucketInfo& bucket_info,
				  const rgw::bucket_log_layout_generation& gen,
				  int shard_id);
  void add_entry(const DoutPrefixProvider *dpp,
		 const RGWBucketInfo& bucket_info,
		 const rgw::bucket_log_layout_generation& gen,
		 int shard_id, asio::yield_context y);
  int add_entry(const DoutPrefixProvider *dpp,
		const RGWBucketInfo& bucket_info,
		const rgw::bucket_log_layout_generation& gen,
		int shard_id, optional_yield y);
  int get_log_shard_id(rgw_bucket& bucket, int shard_id);
  asio::awaitable<std::tuple<std::vector<rgw_data_change_log_entry>,
			     std::string>>
  list_entries(const DoutPrefixProvider* dpp, int shard, int max_entries,
	       std::string marker);
  int list_entries(const DoutPrefixProvider *dpp, int shard, int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   std::string_view marker, std::string* out_marker,
		   bool* truncated, optional_yield y);
  asio::awaitable<std::tuple<std::vector<rgw_data_change_log_entry>,
			     RGWDataChangesLogMarker>>
  list_entries(const DoutPrefixProvider *dpp, int max_entries,
	       RGWDataChangesLogMarker marker);
  int list_entries(const DoutPrefixProvider *dpp, int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   RGWDataChangesLogMarker& marker, bool* ptruncated,
		   optional_yield y);

  int trim_entries(const DoutPrefixProvider *dpp, int shard_id,
		   std::string_view marker, optional_yield y);
  int trim_entries(const DoutPrefixProvider *dpp, int shard_id,
		    std::string_view marker, librados::AioCompletion* c);
  int get_info(const DoutPrefixProvider *dpp, int shard_id,
	       RGWDataChangesLogInfo *info, optional_yield y);

  void mark_modified(int shard_id, const rgw_bucket_shard& bs, uint64_t gen);
  auto read_clear_modified() {
    std::unique_lock wl{modified_lock};
    decltype(modified_shards) modified;
    modified.swap(modified_shards);
    modified_shards.clear();
    return modified;
  }

  void set_observer(rgw::BucketChangeObserver *observer) {
    this->observer = observer;
  }

  void set_bucket_filter(decltype(bucket_filter)&& f) {
    bucket_filter = std::move(f);
  }
  // a marker that compares greater than any other
  std::string max_marker() const;
  std::string get_oid(uint64_t gen_id, int shard_id) const;
  std::string get_sem_set_oid(int shard_id) const;


  int change_format(const DoutPrefixProvider *dpp, log_type type,
		    optional_yield y);
  int trim_generations(const DoutPrefixProvider *dpp,
		       std::optional<uint64_t>& through,
		       optional_yield y);
  asio::awaitable<std::pair<bc::flat_map<std::string, uint64_t>,
			    std::string>>
  read_sems(int index, std::string cursor);
  asio::awaitable<bool>
  synthesize_entries(const DoutPrefixProvider* dpp, int index,
		     const bc::flat_map<std::string, uint64_t>& semcount);
  asio::awaitable<bool>
  gather_working_sets(const DoutPrefixProvider* dpp,
		      int index,
		      bc::flat_map<std::string, uint64_t>& semcount);
  asio::awaitable<void>
  decrement_sems(int index,
		 ceph::mono_time fetch_time,
		 bc::flat_map<std::string, uint64_t>&& semcount);
  asio::awaitable<void> recover_shard(const DoutPrefixProvider* dpp, int index);
  asio::awaitable<void> recover(const DoutPrefixProvider* dpp,
				decltype(recovery_signal));
  asio::awaitable<void> shutdown();
  asio::awaitable<void> shutdown_or_timeout();
  void blocking_shutdown();

  asio::awaitable<void> admin_sem_list(std::optional<int> req_shard,
				       std::uint64_t max_entries,
				       std::string marker,
				       std::ostream& m,
				       ceph::Formatter& formatter);
  asio::awaitable<void> admin_sem_reset(std::string_view marker,
					std::uint64_t count);
};

class RGWDataChangesBE : public boost::intrusive_ref_counter<RGWDataChangesBE> {
protected:
  neorados::RADOS& r;
  neorados::IOContext loc;
  RGWDataChangesLog& datalog;

  CephContext* cct{r.cct()};

  std::string get_oid(int shard_id) {
    return datalog.get_oid(gen_id, shard_id);
  }
public:
  using entries = std::variant<std::vector<cls::log::entry>,
			       std::deque<ceph::buffer::list>>;

  const uint64_t gen_id;

  RGWDataChangesBE(neorados::RADOS& r,
		   neorados::IOContext loc,
		   RGWDataChangesLog& datalog,
		   uint64_t gen_id)
    : r(r), loc(std::move(loc)), datalog(datalog), gen_id(gen_id) {}
  virtual ~RGWDataChangesBE() = default;

  virtual void prepare(ceph::real_time now, const std::string& key,
		       ceph::buffer::list&& entry, entries& out) = 0;
  virtual asio::awaitable<void> push(const DoutPrefixProvider *dpp, int index,
				     entries&& items) = 0;
  virtual void push(const DoutPrefixProvider *dpp, int index,
		    ceph::real_time now,
		    const std::string& key,
		    ceph::buffer::list&& bl,
		    asio::yield_context y) = 0;
  virtual asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			  std::string>>
  list(const DoutPrefixProvider* dpp, int shard,
       std::span<rgw_data_change_log_entry> entries, std::string marker) = 0;
  virtual asio::awaitable<RGWDataChangesLogInfo>
  get_info(const DoutPrefixProvider *dpp, int index) = 0;
  virtual asio::awaitable<void> trim(const DoutPrefixProvider *dpp, int index,
				     std::string_view marker) = 0;
  virtual std::string_view max_marker() const = 0;
  // 1 on empty, 0 on non-empty, negative on error.
  virtual asio::awaitable<bool> is_empty(const DoutPrefixProvider *dpp) = 0;
};
