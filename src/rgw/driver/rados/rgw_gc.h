// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_rados.h"
#include "cls/rgw/cls_rgw_types.h"

#include <atomic>

class RGWGCIOManager;

class RGWGC : public DoutPrefixProvider {
  CephContext *cct;
  RGWRados *store;
  int max_objs;
  std::string *obj_names;
  std::atomic<bool> down_flag = { false };

  static constexpr uint64_t seed = 8675309;

  int tag_index(const std::string& tag);
  int send_chain(const cls_rgw_obj_chain& chain, const std::string& tag, optional_yield y);

  class GCWorker : public Thread {
    const DoutPrefixProvider *dpp;
    CephContext *cct;
    RGWGC *gc;
    ceph::mutex lock = ceph::make_mutex("GCWorker");
    ceph::condition_variable cond;

  public:
    GCWorker(const DoutPrefixProvider *_dpp, CephContext *_cct, RGWGC *_gc) : dpp(_dpp), cct(_cct), gc(_gc) {}
    void *entry() override;
    void stop();
  };

  GCWorker *worker;
public:
  RGWGC() : cct(NULL), store(NULL), max_objs(0), obj_names(NULL), worker(NULL) {}
  ~RGWGC() {
    stop_processor();
    finalize();
  }
  std::vector<bool> transitioned_objects_cache;
  int get_max_objs() const { return max_objs; }
  std::tuple<int, std::optional<cls_rgw_obj_chain>> send_split_chain(const cls_rgw_obj_chain& chain, const std::string& tag, optional_yield y);

  int remove(int index, const std::vector<std::string>& tags, librados::AioCompletion **pc, optional_yield y);
  int remove(int index, int num_entries, optional_yield y);

  void initialize(CephContext *_cct, RGWRados *_store, optional_yield y);
  void finalize();

  int list(int& index, std::string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool& truncated, bool& processing_queue, std::optional<int> shard_id = std::nullopt);
  int process(int index, int process_max_secs, bool expired_only,
              RGWGCIOManager& io_manager, optional_yield y);
  int process(bool expired_only, optional_yield y, std::optional<int> shard_id = std::nullopt);

  bool going_down();
  void start_processor();
  void stop_processor();

  CephContext *get_cct() const override { return store->ctx(); }
  unsigned get_subsys() const;

  std::ostream& gen_prefix(std::ostream& out) const;

};
