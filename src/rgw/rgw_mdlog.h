// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include "common/RWLock.h"
#include <list>
#include <string>
#include <string_view>

#include <boost/container/flat_set.hpp>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "common/Formatter.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"

#include "rgw_metadata.h"
#include "rgw_mdlog_types.h"
#include "rgw_coroutine.h"

#include "services/svc_rados.h"

namespace bc = boost::container;

inline constexpr auto META_LOG_OBJ_PREFIX = "meta.log."sv;

struct RGWMetadataLogInfo {
  std::string marker;
  ceph::real_time last_update;

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};

class RGWCompletionManager;

class RGWMetadataLogInfoCompletion : public RefCountedObject {
 public:
  using info_callback_t = std::function<void(int, const cls_log_header&)>;
 private:
  cls_log_header header;
  RGWSI_RADOS::Obj io_obj;
  librados::AioCompletion *completion;
  std::mutex mutex; //< protects callback between cancel/complete
  info_callback_t callback; //< cleared on cancel
 public:
  explicit RGWMetadataLogInfoCompletion(info_callback_t callback);
  ~RGWMetadataLogInfoCompletion() override;

  RGWSI_RADOS::Obj& get_io_obj() { return io_obj; }
  cls_log_header& get_header() { return header; }
  librados::AioCompletion* get_completion() { return completion; }

  void finish(librados::completion_t cb) {
    std::scoped_lock lock(mutex);
    if (callback) {
      callback(completion->get_return_value(), header);
    }
  }
  void cancel() {
    std::scoped_lock lock(mutex);
    callback = nullptr;
  }
};

class RGWMetadataLogBE {
protected:
  CephContext* const cct;
private:
  std::string prefix;
 
public:
  using entries = std::variant<std::list<cls_log_entry>,
			       std::vector<ceph::buffer::list>>;

  RGWMetadataLogBE(CephContext* const cct, std::string prefix);
  virtual ~RGWMetadataLogBE();

  std::string get_shard_oid(int id) const {
    return fmt::format("{}{}", prefix, id);
  }

  virtual int push(int index, std::vector<cls_log_entry>& items, librados::AioCompletion *&completion) { return 0; };
  virtual int push(int index, ceph::real_time now,
		   const std::string& section,
		   const std::string& key,
		   ceph::buffer::list& bl) { return 0; };
  virtual int list(int index, int max_entries,
		   std::vector<cls_log_entry>& items,
		   std::string_view marker,
		   std::string* out_marker, bool* truncated) { return 0; };
  virtual int get_info(int index, RGWMetadataLogInfo *info) { return 0; };
  virtual int get_info_async(int index, RGWMetadataLogInfoCompletion *completion) { return 0; };
  virtual int trim(int shard_id, std::string_view marker, bool exclusive) { return 0; };
  virtual int trim(int shard_id, std::string_view marker, librados::AioCompletion* c, bool exclusive) { return 0; };
  virtual std::string_view max_marker() { return NULL; };
  static int remove(CephContext* cct, std::string prefix,
	       		     librados::Rados* rados, const rgw_pool& log_pool);
};

class RGWMetadataLog {
  CephContext *cct;
  rgw::sal::RGWRadosStore* store;
  const std::string prefix;

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Cls *cls{nullptr};
  } svc;

  static std::string make_prefix(const std::string& period) {
    if (period.empty())
      return string(META_LOG_OBJ_PREFIX);
    return fmt::format("{}{}.", META_LOG_OBJ_PREFIX, period);
  }

  ceph::shared_mutex lock = ceph::make_shared_mutex("RGWMetaLog::lock");
  bc::flat_set<int> modified_shards;

  void mark_modified(int shard_id);
  std::unique_ptr<RGWMetadataLogBE> be;
public:
  RGWMetadataLog(CephContext *cct,
		 rgw::sal::RGWRadosStore* store,
                 RGWSI_Zone *_zone_svc,
                 RGWSI_Cls *_cls_svc,
                 const std::string& period)
    : cct(cct), store(store),
      prefix(make_prefix(period)) {
    svc.zone = _zone_svc;
    svc.cls = _cls_svc;
  }


  std::string get_shard_oid(int id) const {
    return fmt::format("{}{}", prefix, id);
  }

  int init(librados::Rados* lr);

  int add_entry(const DoutPrefixProvider *dpp, const std::string& hash_key, const std::string& section,
		const std::string& key, ceph::bufferlist& bl);
  int get_shard_id(const DoutPrefixProvider *dpp, std::string_view hash_key);
  int store_entries_in_shard(const DoutPrefixProvider *dpp, std::vector<cls_log_entry>& entries, int shard_id,
			     librados::AioCompletion *completion);

  struct LogListCtx {
    int cur_shard;
    std::string marker;

    std::string cur_oid;

    bool done;

    LogListCtx() : cur_shard(0), done(false) {}
  };

  int list_entries(const DoutPrefixProvider *dpp, int shard,
                   int max_entries,
		   std::string_view marker,
                   std::vector<cls_log_entry>& entries,
		   std::string *out_marker,
		   bool *truncated);

  int trim(const DoutPrefixProvider *dpp, nt shard_id, std::string_view marker, bool exclusive);
  int trim(const DoutPrefixProvider *dpp, int shard_id, std::string_view marker, librados::AioCompletion* c, bool exclusive);
  int get_info(const DoutPrefixProvider *dpp, int shard_id, RGWMetadataLogInfo *info);
  int get_info_async(const DoutPrefixProvider *dpp, int shard_id, RGWMetadataLogInfoCompletion *completion);
  int lock_exclusive(const DoutPrefixProvider *dpp, int shard_id, ceph::timespan duration, std::string& zone_id,
		     std::string& owner_id);
  int unlock(const DoutPrefixProvider *dpp, int shard_id, std::string& zone_id, string& owner_id);

  bc::flat_set<int> read_clear_modified();
  RGWCoroutine* purge_cr();
  RGWCoroutine* master_trim_cr(int shard_id, const std::string& marker,
	       			std::string* last_trim);
  RGWCoroutine* peer_trim_cr(int shard_id, std::string marker, bool exclusive);
  std::string_view max_marker() const;
};

class RGWMetadataLogTrimCR : public RGWSimpleCoroutine {
  RGWMetadataLog* mdlog;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
 protected:
  int shard_id;
  std::string marker;
  bool exclusive{false};
  std::string *last_trim_marker;

 public:
  RGWMetadataLogTrimCR(CephContext *cct,
                       RGWMetadataLog *mdlog, const int shard_id,
                       const std::string& marker, bool exclusive,
		       std::string *last_trim_marker);

  int send_request() override;
  int request_complete() override;
};

struct LogStatusDump {
  RGWMDLogStatus status;

  explicit LogStatusDump(RGWMDLogStatus status) : status(status) {}
  void dump(ceph::Formatter *f) const;
};

struct RGWMetadataLogData {
  obj_version read_version;
  obj_version write_version;
  RGWMDLogStatus status;

  RGWMetadataLogData() : status(MDLOG_STATUS_UNKNOWN) {}

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWMetadataLogData)

struct RGWMetadataLogHistory {
  epoch_t oldest_realm_epoch;
  std::string oldest_period_id;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(oldest_realm_epoch, bl);
    encode(oldest_period_id, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(oldest_realm_epoch, p);
    decode(oldest_period_id, p);
    DECODE_FINISH(p);
  }

  static const std::string oid;
};
WRITE_CLASS_ENCODER(RGWMetadataLogHistory)
