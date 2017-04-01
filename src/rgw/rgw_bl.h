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

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include "common/debug.h"

#include "include/types.h"
#include "include/atomic.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "cls/rgw/cls_rgw_types.h"

using namespace std;
#define BL_HASH_PRIME 7877
static string bl_oid_prefix = "bl";
static string bl_index_lock_name = "bl_process";

extern const char* BL_STATUS[];

typedef enum {
  bl_uninitial = 0,
  bl_processing,
  bl_failed,
  bl_complete,
}BL_BUCKET_STATUS;

class RGWBL {
  CephContext *cct;
  RGWRados *store;
  int max_objs;
  string *obj_names;
  atomic_t down_flag;
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
  bool going_down();
  void start_processor();
  void stop_processor();

};

#endif
