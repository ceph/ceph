// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <array>
#include <string>
#include <iostream>

#include "common/debug.h"

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/iso_8601.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
//#include "rgw_tag.h"
#include "rgw_sal.h"

#include <atomic>
#include <tuple>

#define HASH_PRIME 7877
#define MAX_ID_LEN 255
static std::string restore_oid_prefix = "restore";
static std::string restore_index_lock_name = "restore_process";

class RGWRestore : public DoutPrefixProvider {
  CephContext *cct;
  rgw::sal::Driver* driver;
  std::unique_ptr<rgw::sal::Restore> sal_restore;
  int max_objs{0};
  std::string *obj_names{nullptr};
  std::atomic<bool> down_flag = { false };

  class RestoreWorker : public Thread
  {
    const DoutPrefixProvider *dpp;
    CephContext *cct;
    RGWRestore *restore;
    ceph::mutex lock = ceph::make_mutex("RestoreWorker");
    ceph::condition_variable cond;

  public:

    using lock_guard = std::lock_guard<std::mutex>;
    using unique_lock = std::unique_lock<std::mutex>;

    RestoreWorker(const DoutPrefixProvider* _dpp, CephContext *_cct, RGWRestore *_restore) : dpp(_dpp), cct(_cct), restore(_restore) {}
    RGWRestore* get_restore() { return restore; }

    void *entry() override;
    void stop();

    friend class RGWRados;
  }; // RestoreWorker

  std::unique_ptr<RGWRestore::RestoreWorker> worker;

public:
  ~RGWRestore() {
    stop_processor();
    finalize();
  }

  friend class RGWRados;

  RGWRestore() : cct(nullptr), driver(nullptr), max_objs(0), obj_names(NULL) {}

  void initialize(CephContext *_cct, rgw::sal::Driver* _driver);
  void finalize();

  bool going_down();
  void start_processor();
  void stop_processor();

  CephContext *get_cct() const override { return cct; }
  rgw::sal::Restore* get_restore() const { return sal_restore.get(); }
  unsigned get_subsys() const;

  std::ostream& gen_prefix(std::ostream& out) const;

  int process(RestoreWorker* worker,
              bool once, //is it needed for Restore?
              bool retry); // to retry in_progress request after restart

  time_t thread_stop_at();


  int set_cloud_restore_status(const DoutPrefixProvider* dpp, rgw::sal::Object* pobj,
		  	   optional_yield y, rgw::sal::RGWRestoreStatus restore_status);
  int restore_obj_from_cloud(rgw::sal::Bucket* pbucket, rgw::sal::Object* pobj,
         	 	     rgw::sal::PlacementTier* tier,
			     std::optional<uint64_t> days, optional_yield y);
};
