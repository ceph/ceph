// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Garbage Collector Classes for the CORTX Motr backend
 *
 * Copyright (C) 2022 Seagate Technology LLC and/or its Affiliates
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef __MOTR_GC_H__
#define __MOTR_GC_H__

#include "rgw_sal_motr.h"
#include "common/Thread.h"
#include <mutex>
#include <condition_variable>
#include <atomic>

const int64_t GC_MAX_SHARDS_PRIME = 65521;
static std::string gc_index_prefix = "gc.";
static std::string gc_thread_prefix = "gc_thread_";

class MotrGC : public DoutPrefixProvider {
 private:
  CephContext *cct;
  rgw::sal::Store *store;
  int max_indices = 0;
  std::vector<std::string> index_names;
  std::atomic<bool> down_flag = false;

 public:
  class GCWorker : public Thread {
   private:
    const DoutPrefixProvider *dpp;
    CephContext *cct;
    MotrGC *motr_gc;
    int worker_id;
    uint32_t gc_interval = 60*60;  // default: 24*60*60 sec
    std::mutex lock;
    std::condition_variable cv;
   public:
    GCWorker(const DoutPrefixProvider* _dpp, CephContext *_cct,
             MotrGC *_motr_gc, int _worker_id)
      : dpp(_dpp),
        cct(_cct),
        motr_gc(_motr_gc),
        worker_id(_worker_id) {};

    void *entry() override;
    void stop();
  };
  std::vector<std::unique_ptr<MotrGC::GCWorker>> workers;

  MotrGC(CephContext *_cct, rgw::sal::Store* _store)
    : cct(_cct), store(_store) {}

  ~MotrGC() {
    stop_processor();
    finalize();
  }

  void initialize();
  void finalize();

  void start_processor();
  void stop_processor();

  bool going_down();

  // Set Up logging prefix for GC
  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const;
  std::ostream& gen_prefix(std::ostream& out) const;
};

#endif
