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

const uint32_t GC_DEFAULT_QUEUES = 64;
const uint32_t GC_DEFAULT_COUNT = 256;
const uint32_t GC_MAX_QUEUES = 4096;
static const std::string gc_index_prefix = "motr.rgw.gc";
static const std::string gc_thread_prefix = "motr_gc_";
static const std::string obj_tag_prefix = "0_";
static const std::string obj_exp_time_prefix = "1_";

namespace rgw::sal {
  class MotrStore;
}

struct Meta {
  struct m0_uint128 oid = {};
  struct m0_fid pver = {};
  uint64_t layout_id = 0;

  void encode(bufferlist &bl) const {
    ENCODE_START(5, 5, bl);
    encode(oid.u_hi, bl);
    encode(oid.u_lo, bl);
    encode(pver.f_container, bl);
    encode(pver.f_key, bl);
    encode(layout_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(5, bl);
    decode(oid.u_hi, bl);
    decode(oid.u_lo, bl);
    decode(pver.f_container, bl);
    decode(pver.f_key, bl);
    decode(layout_id, bl);
    DECODE_FINISH(bl);
  }
};

struct motr_gc_obj_info {
  std::string tag;             // gc obj unique identifier
  std::string name;            // fully qualified object name
  Meta mobj;                   // motr obj
  std::time_t time;            // deletion time
  std::uint64_t size;          // size of obj
  std::uint64_t size_actual;   // size of disk
  bool is_multipart;           // flag to indicate if object is multipart
  std::string multipart_iname; // part index name

  motr_gc_obj_info() {}
  motr_gc_obj_info(std::string _tag, std::string _name, Meta _mobj,
                   std::time_t _time, std::uint64_t _size, std::uint64_t _size_actual,
                   bool _is_multipart, std::string _multipart_iname)
      : tag(std::move(_tag)), name(std::move(_name)), mobj(std::move(_mobj)),
        time(std::move(_time)), size(std::move(_size)), size_actual(std::move(_size_actual)),
        is_multipart(std::move(_is_multipart)), multipart_iname(std::move(_multipart_iname)) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(12, 2, bl);
    encode(tag, bl);
    encode(name, bl);
    encode(mobj.oid.u_hi, bl);
    encode(mobj.oid.u_lo, bl);
    encode(mobj.pver.f_container, bl);
    encode(mobj.pver.f_key, bl);
    encode(mobj.layout_id, bl);
    encode(time, bl);
    encode(size, bl);
    encode(size_actual, bl);
    encode(is_multipart, bl);
    encode(multipart_iname, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN_32(12, 2, 2, bl);
    decode(tag, bl);
    decode(name, bl);
    decode(mobj.oid.u_hi, bl);
    decode(mobj.oid.u_lo, bl);
    decode(mobj.pver.f_container, bl);
    decode(mobj.pver.f_key, bl);
    decode(mobj.layout_id, bl);
    decode(time, bl);
    decode(size, bl);
    decode(size_actual, bl);
    decode(is_multipart, bl);
    decode(multipart_iname, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(motr_gc_obj_info);

class MotrGC : public DoutPrefixProvider {
 private:
  CephContext *cct;
  rgw::sal::MotrStore *store;
  uint32_t max_indices = 0;
  uint32_t max_count = 0;
  std::atomic<uint32_t> enqueue_index;
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
    int get_id() { return worker_id; }
  };

  std::vector<std::unique_ptr<MotrGC::GCWorker>> workers;

  MotrGC(CephContext *_cct, rgw::sal::MotrStore* _store)
    : cct(_cct), store(_store) {}

  ~MotrGC() {
    stop_processor();
    finalize();
  }

  void initialize();
  void finalize();

  void start_processor();
  void stop_processor();

  int enqueue(motr_gc_obj_info obj);
  int dequeue(std::string iname, motr_gc_obj_info obj);
  int list(std::vector<motr_gc_obj_info>& gc_entries);
  int get_locked_gc_index(uint32_t& rand_ind);
  bool going_down();

  // Set Up logging prefix for GC
  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const;
  std::ostream& gen_prefix(std::ostream& out) const;
};

#endif
