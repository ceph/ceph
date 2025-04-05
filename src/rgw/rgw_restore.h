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

extern const char* RESTORE_STATUS[];

typedef enum {
  restore_uninitial = 0,
  restore_in_progress,
  restore_failed,
  restore_complete
} RESTORE_ENTRY_STATUS;

struct rgw_restore_entry {
  rgw_bucket bucket;
  rgw_obj_key obj_key;
  std::optional<uint64_t> days;
  std::string zone_id; // or should it be zone name?
  uint32_t status;

  rgw_restore_entry() {}
  rgw_restore_entry(rgw::sal::RestoreEntry e): bucket(e.bucket), obj_key(e.obj_key), days(e.days),
       				zone_id(e.zone_id), status(e.status) {}
  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket, bl);
    encode(obj_key, bl);
    encode(days, bl);
    encode(zone_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(bucket, bl);
     decode(obj_key, bl);
     decode(days, bl);
     decode(zone_id, bl);
     DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(rgw_restore_entry)

struct rgw_restore_log_entry {
  std::string id;
  ceph::real_time mtime;
  rgw_restore_entry entry;

  rgw_restore_log_entry() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(mtime, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(id, bl);
     decode(mtime, bl);
     decode(entry, bl);
     DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(rgw_restore_log_entry)

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

    std::string thr_name() {
      return std::string{"restore_thrd: "}; // + std::to_string(ix);
    }


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

  int process(RestoreWorker* worker, optional_yield y,
		   bool once = false, //is it needed for CR?
		   bool retry = false); // to retry in_progress request after restart
  time_t thread_stop_at();

  int choose_oid(rgw::sal::RestoreEntry e);
  int process(int index, int max_secs, optional_yield y);
  int process_restore_entry(rgw::sal::RestoreEntry& entry, optional_yield y);

  int set_cloud_restore_status(const DoutPrefixProvider* dpp, rgw::sal::Object* pobj,
		  	   optional_yield y, rgw::sal::RGWRestoreStatus restore_status);
  int restore_obj_from_cloud(rgw::sal::Bucket* pbucket, rgw::sal::Object* pobj,
         	 	     rgw::sal::PlacementTier* tier,
			     std::optional<uint64_t> days, optional_yield y);
};
