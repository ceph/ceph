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
#include "rgw_sal.h"

#include <atomic>
#include <tuple>

#define HASH_PRIME 7877
#define MAX_ID_LEN 255
static constexpr std::string_view restore_oid_prefix = "restore";
static constexpr std::string_view restore_index_lock_name = "restore_process";

namespace rgw::restore {

/** Single Restore entry state */
struct RestoreEntry {
  rgw_bucket bucket;
  rgw_obj_key obj_key;
  std::optional<uint64_t> days;
  std::string zone_id; // or should it be zone name?
  rgw::sal::RGWRestoreStatus status;

  RestoreEntry() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket, bl);
    encode(obj_key, bl);
    encode(days, bl);
    encode(zone_id, bl);
    encode(status, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket, bl);
    decode(obj_key, bl);
    decode(days, bl);
    decode(zone_id, bl);
    decode(status, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
  static void generate_test_instances(std::list<rgw::restore::RestoreEntry*>& l);
};
WRITE_CLASS_ENCODER(RestoreEntry)

class Restore : public DoutPrefixProvider {
  CephContext *cct;
  rgw::sal::Driver* driver;
  std::unique_ptr<rgw::sal::Restore> sal_restore;
  int max_objs{0};
  std::vector<std::string> obj_names;
  std::atomic<bool> down_flag = { false };

  class RestoreWorker : public Thread
  {
    const DoutPrefixProvider *dpp;
    CephContext *cct;
    rgw::restore::Restore *restore;
    ceph::mutex lock = ceph::make_mutex("RestoreWorker");
    ceph::condition_variable cond;

  public:

    using lock_guard = std::lock_guard<std::mutex>;
    using unique_lock = std::unique_lock<std::mutex>;

    RestoreWorker(const DoutPrefixProvider* _dpp, CephContext *_cct, rgw::restore::Restore *_restore) : dpp(_dpp), cct(_cct), restore(_restore) {}
    rgw::restore::Restore* get_restore() { return restore; }
    std::string thr_name() {
      return std::string{"restore_thrd: "}; // + std::to_string(ix);
    }
    void *entry() override;
    void stop();

    friend class RGWRados;
  }; // RestoreWorker

  std::unique_ptr<Restore::RestoreWorker> worker;

public:
  ~Restore() {
    stop_processor();
    finalize();
  }

  friend class RGWRados;

  Restore() : cct(nullptr), driver(nullptr), max_objs(0) {}

  int initialize(CephContext *_cct, rgw::sal::Driver* _driver);
  void finalize();

  bool going_down();
  void start_processor();
  void stop_processor();

  CephContext *get_cct() const override { return cct; }
  rgw::sal::Restore* get_restore() const { return sal_restore.get(); }
  unsigned get_subsys() const;

  std::ostream& gen_prefix(std::ostream& out) const;

  int process(RestoreWorker* worker, optional_yield y);
  int choose_oid(const rgw::restore::RestoreEntry& e);
  int process(int index, int max_secs, optional_yield y);
  int process_restore_entry(rgw::restore::RestoreEntry& entry, optional_yield y);
  time_t thread_stop_at();

  /** Set the restore status for the given object */
  int set_cloud_restore_status(const DoutPrefixProvider* dpp, rgw::sal::Object* pobj,
		  	   optional_yield y,
			   const rgw::sal::RGWRestoreStatus& restore_status);

  /** Given <bucket, obj>, restore the object from the cloud-tier. In case the
   * object cannot be restored immediately, save that restore state(/entry) 
   * to be procesed later by RestoreWorker thread. */
  int restore_obj_from_cloud(rgw::sal::Bucket* pbucket, rgw::sal::Object* pobj,
		  	     rgw::sal::PlacementTier* tier,
			     std::optional<uint64_t> days,
			     const DoutPrefixProvider* dpp,
			     optional_yield y);
};

} // namespace rgw::restore
