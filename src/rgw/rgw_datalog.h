// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DATALOG_H
#define CEPH_RGW_DATALOG_H

#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <boost/container/flat_map.hpp>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/buffer.h"
#include "include/encoding.h"

#include "include/rados/librados.hpp"

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "common/Formatter.h"
#include "common/lru_map.h"
#include "common/RefCountedObj.h"

#include "cls/log/cls_log_types.h"

#include "rgw_basic_types.h"
#include "rgw_sync_policy.h"
#include "rgw_zone.h"
#include "rgw_trim_bilog.h"

#include "services/svc_cls.h"

namespace bc = boost::container;

enum DataLogEntityType {
  ENTITY_TYPE_UNKNOWN = 0,
  ENTITY_TYPE_BUCKET = 1,
};

struct rgw_data_change {
  DataLogEntityType entity_type;
  std::string key;
  ceph::real_time timestamp;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    auto t = std::uint8_t(entity_type);
    encode(t, bl);
    encode(key, bl);
    encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     std::uint8_t t;
     decode(t, bl);
     entity_type = DataLogEntityType(t);
     decode(key, bl);
     decode(timestamp, bl);
     DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
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
  std::optional<std::string> marker;

  RGWDataChangesLogMarker() = default;
};

class RGWDataChangesBE {
protected:
  CephContext* const cct;
private:
  std::string prefix;
  static std::string_view get_prefix(CephContext* cct) {
    std::string_view prefix = cct->_conf->rgw_data_log_obj_prefix;
    if (prefix.empty()) {
      prefix = "data_log"sv;
    }
    return prefix;
  }
public:
  using entries = std::variant<std::list<cls_log_entry>,
			       std::vector<ceph::buffer::list>>;

  RGWDataChangesBE(CephContext* const cct)
    : cct(cct), prefix(get_prefix(cct)) {}
  virtual ~RGWDataChangesBE() = default;

  static std::string get_oid(CephContext* cct, int i) {
    return fmt::format("{}.{}", get_prefix(cct), i);
  }
  std::string get_oid(int i) {
    return fmt::format("{}.{}", prefix, i);
  }
  static int remove(CephContext* cct, librados::Rados* rados,
		    const rgw_pool& log_pool);


  virtual void prepare(ceph::real_time now,
		       const std::string& key,
		       ceph::buffer::list&& entry,
		       entries& out) = 0;
  virtual int push(int index, entries&& items) = 0;
  virtual int push(int index, ceph::real_time now,
		   const std::string& key,
		   ceph::buffer::list&& bl) = 0;
  virtual int list(int shard, int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   std::optional<std::string_view> marker,
		   std::string* out_marker, bool* truncated) = 0;
  virtual int get_info(int index, RGWDataChangesLogInfo *info) = 0;
  virtual int trim(int index, std::string_view marker) = 0;
  virtual int trim(int index, std::string_view marker,
		   librados::AioCompletion* c) = 0;
  virtual std::string_view max_marker() const = 0;
};

class RGWDataChangesLog {
  CephContext *cct;
  rgw::BucketChangeObserver *observer = nullptr;
  const RGWZone* zone;
  std::unique_ptr<RGWDataChangesBE> be;

  const int num_shards;

  ceph::mutex lock = ceph::make_mutex("RGWDataChangesLog::lock");
  ceph::shared_mutex modified_lock =
    ceph::make_shared_mutex("RGWDataChangesLog::modified_lock");
  bc::flat_map<int, bc::flat_set<std::string>> modified_shards;

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

  lru_map<rgw_bucket_shard, ChangeStatusPtr> changes;

  bc::flat_set<rgw_bucket_shard> cur_cycle;

  void _get_change(const rgw_bucket_shard& bs, ChangeStatusPtr& status);
  void register_renew(const rgw_bucket_shard& bs);
  void update_renewed(const rgw_bucket_shard& bs, ceph::real_time expiration);

  ceph::mutex renew_lock = ceph::make_mutex("ChangesRenewThread::lock");
  ceph::condition_variable renew_cond;
  void renew_run();
  void renew_stop();
  std::thread renew_thread;

  std::function<bool(const rgw_bucket& bucket, optional_yield y, const DoutPrefixProvider *dpp)> bucket_filter;
  int choose_oid(const rgw_bucket_shard& bs);
  bool going_down() const;
  bool filter_bucket(const DoutPrefixProvider *dpp, const rgw_bucket& bucket, optional_yield y) const;
  int renew_entries();

public:

  RGWDataChangesLog(CephContext* cct);
  ~RGWDataChangesLog();

  int start(const RGWZone* _zone, const RGWZoneParams& zoneparams,
	    RGWSI_Cls *cls_svc, librados::Rados* lr);

  int add_entry(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, int shard_id);
  int get_log_shard_id(rgw_bucket& bucket, int shard_id);
  int list_entries(int shard, int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   std::optional<std::string_view> marker,
		   std::string* out_marker, bool* truncated);
  int trim_entries(int shard_id, std::string_view marker);
  int trim_entries(int shard_id, std::string_view marker,
		   librados::AioCompletion* c); // :(
  int get_info(int shard_id, RGWDataChangesLogInfo *info);

  using LogMarker = RGWDataChangesLogMarker;

  int list_entries(int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   LogMarker& marker, bool* ptruncated);

  void mark_modified(int shard_id, const rgw_bucket_shard& bs);
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
  std::string_view max_marker() const;
  std::string get_oid(int shard_id) const;
};

#endif
