// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_BL_H
#define CEPH_RGW_BL_H

#include <atomic>
#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include "common/debug.h"

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "cls/rgw/cls_rgw_types.h"

using namespace std;
#define BL_HASH_PRIME 7877
#define BL_UNIQUE_STRING_LEN 16
static string bl_oid_prefix = "bl";
static string bl_index_lock_name = "bl_process";

extern const char* BL_STATUS[];

typedef enum {
  bl_uninitial = 0,
  bl_processing,
  bl_failed,
  bl_perm_error,
  bl_acl_error,
  bl_complete,
}BL_BUCKET_STATUS;

class RGWBL {
  CephContext *cct;
  RGWRados *store;
  int max_objs;
  string *obj_names;
  std::atomic<bool> down_flag = { false };
  string cookie;

  class BLWorker : public Thread {
    CephContext *cct;
    RGWBL *bl;
    Mutex lock;
    Cond cond;

   public:
    BLWorker(CephContext *_cct, RGWBL *_bl) : cct(_cct), bl(_bl), lock("BLWorker") {}
    void *entry() override;
    void stop();
    bool should_work(utime_t& now);
    int schedule_next_start_time(utime_t& now);
  };

 public:
  BLWorker *worker = nullptr;
  RGWBL() : cct(nullptr), store(nullptr), worker(nullptr) {}
  ~RGWBL() {
    stop_processor();
    finalize();
  }

  void initialize(CephContext *_cct, RGWRados *_store);
  void finalize();

  int process();
  int process(int index, int max_secs);
  bool if_already_run_today(time_t& start_date);
  int list_bl_progress(const string& marker, uint32_t max_entries,
		       map<string, int> *progress_map);
  int bucket_bl_prepare(int index);
  int bucket_bl_process(string& shard_id);
  int bucket_bl_post(int index, int max_lock_sec,
		     pair<string, int >& entry, int& result);

  void format_opslog_entry(struct rgw_log_entry& entry, bufferlist *buffer);
  int bucket_bl_fetch(const string opslog_obj, bufferlist* opslog_entries);
  int bucket_bl_upload(bufferlist* opslog_buffer, rgw_obj target_object,
                       map<string, bufferlist> tobject_attrs);
  int bucket_bl_remove(const string opslog_obj);
  int bucket_bl_deliver(string opslog_obj, const rgw_bucket target_bucket,
                        const string target_prefix, map<string, bufferlist> tobject_attrs);

  bool going_down();
  void start_processor();
  void stop_processor();

};

class BLLoggingEnabled
{
protected:
  CephContext *cct;
  bool status;
  string target_bucket;
  string target_prefix;

public:
  bool target_bucket_specified;
  bool target_prefix_specified;

  BLLoggingEnabled() : cct(nullptr), status(false), target_bucket_specified(false),
                       target_prefix_specified(false) {};
  BLLoggingEnabled(CephContext *_cct) : cct(_cct), status(false), target_bucket_specified(false),
                       target_prefix_specified(false) {};
  ~BLLoggingEnabled(){};

  void set_true() {
    status = true;
  }

  void set_false() {
    status = false;
  }

  bool is_true() const {
    return status == true;
  }

  string get_target_bucket() const {
    return target_bucket;
  }

  string get_target_prefix() const {
    return target_prefix;
  }

  void set_target_bucket(string _bucket) {
    target_bucket =  _bucket;
  }

  void set_target_prefix(string _prefix) {
    target_prefix =  _prefix;
  }

  void encode(bufferlist& bl) const {
     ENCODE_START(1, 1, bl);
     ::encode(status, bl);
     ::encode(target_bucket, bl);
     ::encode(target_prefix, bl);
     ENCODE_FINISH(bl);
   }
   void decode(bufferlist::iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
     ::decode(status, bl);
     ::decode(target_bucket, bl);
     ::decode(target_prefix, bl);
     DECODE_FINISH(bl);
   }
};
WRITE_CLASS_ENCODER(BLLoggingEnabled)

class RGWBucketLoggingStatus
{
 protected:
  CephContext *cct;

 public:
  BLLoggingEnabled enabled;

  RGWBucketLoggingStatus(CephContext *_cct) : cct(_cct), enabled(_cct) {}
  RGWBucketLoggingStatus() : cct(nullptr), enabled(nullptr) {}

  void set_ctx(CephContext *ctx) {
    cct = ctx;
  }

  virtual ~RGWBucketLoggingStatus() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(enabled, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(enabled, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;

  bool is_enabled() const {
    return enabled.is_true();
  }

  string get_target_prefix() const {
    return enabled.get_target_prefix();
  }

  string get_target_bucket() const {
    return enabled.get_target_bucket();
  }

};
WRITE_CLASS_ENCODER(RGWBucketLoggingStatus)


#endif
