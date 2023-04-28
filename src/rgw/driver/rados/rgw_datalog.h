// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <fmt/format.h>

#include "common/async/yield_context.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/function2.hpp"

#include "include/rados/librados.hpp"

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "common/Formatter.h"
#include "common/lru_map.h"
#include "common/RefCountedObj.h"

#include "cls/log/cls_log_types.h"
#include "rgw_basic_types.h"
#include "rgw_log_backing.h"
#include "rgw_sync_policy.h"
#include "rgw_zone.h"
#include "rgw_trim_bilog.h"

namespace bc = boost::container;

enum DataLogEntityType {
  ENTITY_TYPE_UNKNOWN = 0,
  ENTITY_TYPE_BUCKET = 1,
};

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

  DataLogBackends(librados::IoCtx& ioctx,
		  std::string oid,
		  fu2::unique_function<std::string(
		    uint64_t, int) const>&& get_oid,
		  int shards, RGWDataChangesLog& datalog) noexcept
    : logback_generations(ioctx, oid, std::move(get_oid),
			  shards), datalog(datalog) {}
public:

  boost::intrusive_ptr<RGWDataChangesBE> head() {
    std::unique_lock l(m);
    auto i = end();
    --i;
    return i->second;
  }
  int list(const DoutPrefixProvider *dpp, int shard, int max_entries,
	   std::vector<rgw_data_change_log_entry>& entries,
	   std::string_view marker, std::string* out_marker, bool* truncated,
	   optional_yield y);
  int trim_entries(const DoutPrefixProvider *dpp, int shard_id,
		   std::string_view marker, optional_yield y);
  void trim_entries(const DoutPrefixProvider *dpp, int shard_id, std::string_view marker,
		    librados::AioCompletion* c);

  bs::error_code handle_init(entries_t e) noexcept override;
  bs::error_code handle_new_gens(entries_t e) noexcept override;
  bs::error_code handle_empty_to(uint64_t new_tail) noexcept override;

  int trim_generations(const DoutPrefixProvider *dpp,
		       std::optional<uint64_t>& through,
		       optional_yield y);
};

struct BucketGen {
  rgw_bucket_shard shard;
  uint64_t gen;

  BucketGen(const rgw_bucket_shard& shard, uint64_t gen)
    : shard(shard), gen(gen) {}

  BucketGen(rgw_bucket_shard&& shard, uint64_t gen)
    : shard(std::move(shard)), gen(gen) {}

  BucketGen(const BucketGen&) = default;
  BucketGen(BucketGen&&) = default;
  BucketGen& operator =(const BucketGen&) = default;
  BucketGen& operator =(BucketGen&&) = default;

  ~BucketGen() = default;
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

class RGWDataChangesLog {
  friend DataLogBackends;
  CephContext *cct;
  librados::IoCtx ioctx;
  rgw::BucketChangeObserver *observer = nullptr;
  const RGWZone* zone;
  std::unique_ptr<DataLogBackends> bes;

  const int num_shards;
  std::string get_prefix() {
    auto prefix = cct->_conf->rgw_data_log_obj_prefix;
    return prefix.empty() ? prefix : "data_log";
  }
  std::string metadata_log_oid() {
    return get_prefix() + "generations_metadata";
  }
  std::string prefix;

  ceph::mutex lock = ceph::make_mutex("RGWDataChangesLog::lock");
  ceph::shared_mutex modified_lock =
    ceph::make_shared_mutex("RGWDataChangesLog::modified_lock");
  bc::flat_map<int, bc::flat_set<rgw_data_notify_entry>> modified_shards;

  std::atomic<bool> down_flag = { false };

  struct ChangeStatus {
    std::shared_ptr<const rgw_sync_policy_info> sync_policy;
    ceph::real_time cur_expiration;
    ceph::real_time cur_sent;
    bool pending = false;
    RefCountedCond* cond = nullptr;
    ceph::mutex lock = ceph::make_mutex("RGWDataChangesLog::ChangeStatus");
  };

  using ChangeStatusPtr = std::shared_ptr<ChangeStatus>;

  lru_map<BucketGen, ChangeStatusPtr> changes;

  bc::flat_set<BucketGen> cur_cycle;

  ChangeStatusPtr _get_change(const rgw_bucket_shard& bs, uint64_t gen);
  void register_renew(const rgw_bucket_shard& bs,
		      const rgw::bucket_log_layout_generation& gen);
  void update_renewed(const rgw_bucket_shard& bs,
		      uint64_t gen,
		      ceph::real_time expiration);

  ceph::mutex renew_lock = ceph::make_mutex("ChangesRenewThread::lock");
  ceph::condition_variable renew_cond;
  void renew_run() noexcept;
  void renew_stop();
  std::thread renew_thread;

  std::function<bool(const rgw_bucket& bucket, optional_yield y, const DoutPrefixProvider *dpp)> bucket_filter;
  bool going_down() const;
  bool filter_bucket(const DoutPrefixProvider *dpp, const rgw_bucket& bucket, optional_yield y) const;
  int renew_entries(const DoutPrefixProvider *dpp);

public:

  RGWDataChangesLog(CephContext* cct);
  ~RGWDataChangesLog();

  int start(const DoutPrefixProvider *dpp, const RGWZone* _zone, const RGWZoneParams& zoneparams,
	    rgw::sal::RadosStore* store);
  int choose_oid(const rgw_bucket_shard& bs);
  int add_entry(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info,
		const rgw::bucket_log_layout_generation& gen, int shard_id,
		optional_yield y);
  int get_log_shard_id(rgw_bucket& bucket, int shard_id);
  int list_entries(const DoutPrefixProvider *dpp, int shard, int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   std::string_view marker, std::string* out_marker,
		   bool* truncated, optional_yield y);
  int trim_entries(const DoutPrefixProvider *dpp, int shard_id,
		   std::string_view marker, optional_yield y);
  int trim_entries(const DoutPrefixProvider *dpp, int shard_id, std::string_view marker,
		   librados::AioCompletion* c); // :(
  int get_info(const DoutPrefixProvider *dpp, int shard_id,
	       RGWDataChangesLogInfo *info, optional_yield y);

  using LogMarker = RGWDataChangesLogMarker;

  int list_entries(const DoutPrefixProvider *dpp, int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   LogMarker& marker, bool* ptruncated,
		   optional_yield y);

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


  int change_format(const DoutPrefixProvider *dpp, log_type type, optional_yield y);
  int trim_generations(const DoutPrefixProvider *dpp,
		       std::optional<uint64_t>& through,
		       optional_yield y);
};

class RGWDataChangesBE : public boost::intrusive_ref_counter<RGWDataChangesBE> {
protected:
  librados::IoCtx& ioctx;
  CephContext* const cct;
  RGWDataChangesLog& datalog;

  std::string get_oid(int shard_id) {
    return datalog.get_oid(gen_id, shard_id);
  }
public:
  using entries = std::variant<std::vector<cls::log::entry>,
			       std::vector<ceph::buffer::list>>;

  const uint64_t gen_id;

  RGWDataChangesBE(librados::IoCtx& ioctx,
		   RGWDataChangesLog& datalog,
		   uint64_t gen_id)
    : ioctx(ioctx), cct(static_cast<CephContext*>(ioctx.cct())),
      datalog(datalog), gen_id(gen_id) {}
  virtual ~RGWDataChangesBE() = default;

  virtual void prepare(ceph::real_time now,
		       const std::string& key,
		       ceph::buffer::list&& entry,
		       entries& out) = 0;
  virtual int push(const DoutPrefixProvider *dpp, int index, entries&& items,
		   optional_yield y) = 0;
  virtual int push(const DoutPrefixProvider *dpp, int index, ceph::real_time now,
		   const std::string& key, ceph::buffer::list&& bl,
		   optional_yield y) = 0;
  virtual int list(const DoutPrefixProvider *dpp, int shard, int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   std::optional<std::string_view> marker,
		   std::string* out_marker, bool* truncated,
		   optional_yield y) = 0;
  virtual int get_info(const DoutPrefixProvider *dpp, int index,
		       RGWDataChangesLogInfo *info, optional_yield y) = 0;
  virtual int trim(const DoutPrefixProvider *dpp, int index,
		   std::string_view marker, optional_yield y) = 0;
  virtual int trim(const DoutPrefixProvider *dpp, int index,
		   std::string_view marker, librados::AioCompletion* c) = 0;
  virtual std::string_view max_marker() const = 0;
  // 1 on empty, 0 on non-empty, negative on error.
  virtual int is_empty(const DoutPrefixProvider *dpp, optional_yield y) = 0;
};
