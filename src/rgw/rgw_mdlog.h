// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

#include "rgw_metadata.h"
#include "rgw_mdlog_types.h"

#define META_LOG_OBJ_PREFIX "meta.log."

struct RGWMetadataLogInfo {
  string marker;
  real_time last_update;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

class RGWCompletionManager;

class RGWMetadataLogInfoCompletion : public RefCountedObject {
 public:
  using info_callback_t = std::function<void(int, const cls_log_header&)>;
 private:
  cls_log_header header;
  librados::IoCtx io_ctx;
  librados::AioCompletion *completion;
  std::mutex mutex; //< protects callback between cancel/complete
  boost::optional<info_callback_t> callback; //< cleared on cancel
 public:
  explicit RGWMetadataLogInfoCompletion(info_callback_t callback);
  ~RGWMetadataLogInfoCompletion() override;

  librados::IoCtx& get_io_ctx() { return io_ctx; }
  cls_log_header& get_header() { return header; }
  librados::AioCompletion* get_completion() { return completion; }

  void finish(librados::completion_t cb) {
    std::lock_guard<std::mutex> lock(mutex);
    if (callback) {
      (*callback)(completion->get_return_value(), header);
    }
  }
  void cancel() {
    std::lock_guard<std::mutex> lock(mutex);
    callback = boost::none;
  }
};

class RGWMetadataLog {
  CephContext *cct;
  RGWRados *store;
  const string prefix;

  static std::string make_prefix(const std::string& period) {
    if (period.empty())
      return META_LOG_OBJ_PREFIX;
    return META_LOG_OBJ_PREFIX + period + ".";
  }

  RWLock lock;
  set<int> modified_shards;

  void mark_modified(int shard_id);
public:
  RGWMetadataLog(CephContext *_cct, RGWRados *_store, const std::string& period)
    : cct(_cct), store(_store),
      prefix(make_prefix(period)),
      lock("RGWMetaLog::lock") {}

  void get_shard_oid(int id, string& oid) const {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", id);
    oid = prefix + buf;
  }

  int add_entry(RGWSI_MetaBackend::Module *module, const string& section, const string& key, bufferlist& bl);
  int store_entries_in_shard(list<cls_log_entry>& entries, int shard_id, librados::AioCompletion *completion);

  struct LogListCtx {
    int cur_shard;
    string marker;
    real_time from_time;
    real_time end_time;

    string cur_oid;

    bool done;

    LogListCtx() : cur_shard(0), done(false) {}
  };

  void init_list_entries(int shard_id, const real_time& from_time, const real_time& end_time, string& marker, void **handle);
  void complete_list_entries(void *handle);
  int list_entries(void *handle,
                   int max_entries,
                   list<cls_log_entry>& entries,
		   string *out_marker,
		   bool *truncated);

  int trim(int shard_id, const real_time& from_time, const real_time& end_time, const string& start_marker, const string& end_marker);
  int get_info(int shard_id, RGWMetadataLogInfo *info);
  int get_info_async(int shard_id, RGWMetadataLogInfoCompletion *completion);
  int lock_exclusive(int shard_id, timespan duration, string&zone_id, string& owner_id);
  int unlock(int shard_id, string& zone_id, string& owner_id);

  int update_shards(list<int>& shards);

  void read_clear_modified(set<int> &modified);
};

struct LogStatusDump {
  RGWMDLogStatus status;

  explicit LogStatusDump(RGWMDLogStatus _status) : status(_status) {}
  void dump(Formatter *f) const;
};

struct RGWMetadataLogData {
  obj_version read_version;
  obj_version write_version;
  RGWMDLogStatus status;
  
  RGWMetadataLogData() : status(MDLOG_STATUS_UNKNOWN) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWMetadataLogData)

struct RGWMetadataLogHistory {
  epoch_t oldest_realm_epoch;
  std::string oldest_period_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(oldest_realm_epoch, bl);
    encode(oldest_period_id, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    decode(oldest_realm_epoch, p);
    decode(oldest_period_id, p);
    DECODE_FINISH(p);
  }

  static const std::string oid;
};
WRITE_CLASS_ENCODER(RGWMetadataLogHistory)

