// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <fmt/chrono.h>
#include <string.h>
#include <iostream>
#include <map>
#include <algorithm>
#include <tuple>
#include <functional>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/variant.hpp>

#include "include/scope_guard.h"
#include "include/function2.hpp"
#include "common/Formatter.h"
#include "common/containers.h"
#include "common/split.h"
#include <common/errno.h>
#include "include/random.h"
#include "cls/lock/cls_lock_client.h"
#include "rgw_perf_counters.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_restore.h"
#include "rgw_zone.h"
#include "rgw_string.h"
#include "rgw_multi.h"
#include "rgw_sal.h"
#include "rgw_lc_tier.h"
#include "rgw_notify.h"
#include "common/dout.h"

#include "fmt/format.h"

#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw_restore


constexpr int32_t hours_in_a_day = 24;
constexpr int32_t secs_in_a_day = hours_in_a_day * 60 * 60;

using namespace std;
using namespace rgw::sal;

const char* RESTORE_STATUS[] = {
      "UNINITIAL",
      "INPROGRESS",
      "FAILED",
      "COMPLETE"
};

//using namespace librados;

void RGWRestore::initialize(CephContext *_cct, rgw::sal::Driver* _driver) {
  cct = _cct;
  driver = _driver;
  max_objs = cct->_conf->rgw_restore_max_objs;
  if (max_objs > HASH_PRIME)
    max_objs = HASH_PRIME;
 
  obj_names = new string[max_objs]; // XXXXXX: maybe not needed

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = restore_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf);
  }
  sal_restore = driver->get_restore();
  sal_restore->init(this, driver, max_objs, restore_oid_prefix);
}

void RGWRestore::finalize()
{
  delete[] obj_names;
}

static inline std::ostream& operator<<(std::ostream &os, rgw::sal::RestoreEntry& ent) {
  os << "<ent: bucket=";
  os << ent.bucket;
  os << "; obj_key=";
  os << ent.obj_key;
  os << "; days=";
  os << ent.days;
  os << "; zone_id=";
  os << ent.zone_id;
  os << "; status=";
  os << RESTORE_STATUS[ent.status];
  os << ">";
  return os;
}

void RGWRestore::RestoreWorker::stop()
{
  std::lock_guard l{lock};
  cond.notify_all();
}

bool RGWRestore::going_down()
{
  return down_flag;
}

void RGWRestore::start_processor()
{
  worker = std::make_unique<RGWRestore::RestoreWorker>(this, cct, this);
  worker->create("rgw_restore");
}

void RGWRestore::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  worker.reset(nullptr);
}

unsigned RGWRestore::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWRestore::gen_prefix(std::ostream& out) const
{
  return out << "restore: ";
}

int RGWRestore::choose_oid(rgw::sal::RestoreEntry e) {
  int index;

  const auto& name = e.bucket.name + e.obj_key.name + e.obj_key.instance;
  index = ((ceph_str_hash_linux(name.data(), name.size())) % max_objs);

  return static_cast<int>(index);
}

void *RGWRestore::RestoreWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
//    ldpp_dout(dpp, 2) << "Restore worker: start" << dendl;
    int r = 0;
    restore->process(this, null_yield, true, false);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: restore process() returned error r=" << r << dendl;
    }

    if (restore->going_down())
      break;

    utime_t end = ceph_clock_now();
    end -= start;
    int secs = cct->_conf->rgw_restore_processor_period;

    if (secs <= end.sec())
      continue; // next round

    secs -= end.sec();

    std::unique_lock locker{lock};
//    ldpp_dout(dpp, -1) << "XXXXXXXXXXXX RGWRestore Reached RestoreWorker " << dendl;
    
  } while (!restore->going_down());

  return NULL;
}

int RGWRestore::process(RestoreWorker* worker,
		   optional_yield y,
		   bool once, //is it needed for CR?
		   bool retry) // to retry in_progress request after restart
{
  int max_secs = cct->_conf->rgw_restore_lock_max_time;

  const int start = ceph::util::generate_random_number(0, max_objs - 1);

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    int ret = process(index, max_secs, y);
    if (ret < 0)
      return ret;
  }
  return 0;
}

int RGWRestore::process(int index, int max_secs, optional_yield y)
{
  ldpp_dout(this, 20) << "RGWRestore::process entered with Restore index_shard=" <<
    index << ", max_secs=" << max_secs << dendl;

  std::list<RestoreEntry> r_entries;

  std::unique_ptr<rgw::sal::RestoreSerializer> serializer =
    sal_restore->get_serializer(restore_index_lock_name, obj_names[index],
			   worker->thr_name());
  if (max_secs <= 0) {
    return -EAGAIN;
  }

  utime_t end = ceph_clock_now();

  /* max_secs should be greater than zero. We don't want a zero max_secs
   * to be translated as no timeout, since we'd then need to break the
   * lock and that would require a manual intervention. In this case
   * we can just wait it out. */
  if (max_secs <= 0)
    return -EAGAIN;
 
  utime_t time(max_secs, 0);
  int ret = serializer->try_lock(this, time, null_yield);
  if (ret == -EBUSY || ret == -EEXIST) {
    /* already locked by another lc processor */
    ldpp_dout(this, 0) << "RGWRestore::process() failed to acquire lock on "
		       << obj_names[index] << dendl;
    return -EBUSY;
  }
  if (ret < 0)
    return 0;

  std::unique_lock<rgw::sal::RestoreSerializer> lock(
    *(serializer.get()), std::adopt_lock);

  std::string marker;
  std::string next_marker;
  bool truncated = false;

  do {
    int max = 100;
    std::vector<RestoreEntry> entries;

    int ret = 0;

    sal_restore->list(this, y, index, marker, &next_marker, max, entries, &truncated);
      ldpp_dout(this, 20) <<
      "RGWRestore::process cls_rgw_gc_list returned with returned:" << ret <<
      ", entries.size=" << entries.size() << ", truncated=" << truncated <<
      ", next_marker='" << next_marker << "'" << dendl;
 
      if (entries.size() == 0) {
        ret = 0;
        goto done;
      }

    if (ret < 0)
      goto done;

    marker = next_marker;

    std::vector<RestoreEntry>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      RestoreEntry entry = *iter;

      ret = process_restore_entry(entry, y);

      if (entry.status == restore_in_progress) {
	 r_entries.push_back(entry);
      }
      ///
      ///process all entries, trim and re-add
      utime_t now = ceph_clock_now();
      if (now >= end) {
        goto done;
      }
	  if (going_down()) {
	    // leave early, even if tag isn't removed, it's ok since it
	    // will be picked up next time around
	    goto done;
	  }
    }
    sal_restore->trim_entries(this, y, index, marker);
    if (!r_entries.empty()) {
	    sal_restore->add_entries(this, y, index, r_entries);
    }
    r_entries.clear();
  } while (truncated);

done:
    sal_restore->trim_entries(this, y, index, marker);
    if (!r_entries.empty()) {
	    sal_restore->add_entries(this, y, index, r_entries);
    }
    r_entries.clear();

  lock.unlock();

  return 0;
}

/* XXX: check how to use this routine */
time_t RGWRestore::thread_stop_at()
{
  uint64_t interval = (cct->_conf->rgw_restore_debug_interval > 0)
    ? cct->_conf->rgw_restore_debug_interval : secs_in_a_day;

  return time(nullptr) + interval;
}

int RGWRestore::set_cloud_restore_status(const DoutPrefixProvider* dpp,
			   rgw::sal::Object* pobj, optional_yield y,
	          	   rgw::sal::RGWRestoreStatus restore_status)
{
  int ret = -1;
  if (!pobj)
    return ret;

  pobj->set_atomic();

  bufferlist bl;
  using ceph::encode;
  encode(restore_status, bl);

  ret = pobj->modify_obj_attrs(RGW_ATTR_RESTORE_STATUS, bl, y, dpp);

  return ret;
}

int RGWRestore::process_restore_entry(RestoreEntry& entry, optional_yield y)
{
  int ret = 0;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::unique_ptr<rgw::sal::Object> obj;
  std::unique_ptr<rgw::sal::PlacementTier> tier;
  std::optional<uint64_t> days = entry.days;


  // Ensure its the same source zone processing temp entries as we do not
  // replicate temp restored copies
  if (days) { // temp copy
    auto& zone_id = entry.zone_id;
    if (driver->get_zone()->get_id() != zone_id) {
      // XXX: Do we need to check if the zone is still valid
      return 0; // skip processing this entry in this zone
    }
  }

  // fill in the details from entry
  // bucket, obj, days, state=in_progress
  ret = driver->load_bucket(this, entry.bucket, &bucket, null_yield);
  if (ret < 0) {
    ldpp_dout(this, 0) << "Restore:get_bucket for " << bucket->get_name()
		       << " failed" << dendl;
    return ret;
  }
  obj = bucket->get_object(entry.obj_key);

  ret = obj->load_obj_state(this, null_yield, true);

  if (ret < 0) {
    ldpp_dout(this, 0) << "Restore:get_object for " << entry.obj_key
		       << " failed" << dendl;
    return ret;
  }

  rgw_placement_rule target_placement;

      target_placement.inherit_from(bucket->get_placement_rule());

      auto& attrs = obj->get_attrs();
      auto attr_iter = attrs.find(RGW_ATTR_STORAGE_CLASS);
      if (attr_iter != attrs.end()) {
        target_placement.storage_class = attr_iter->second.to_str();
      }
      ret = driver->get_zone()->get_zonegroup().get_placement_tier(target_placement, &tier);

      if (ret < 0) {
        ldpp_dout(this, -1) << "failed to fetch tier placement handle, ret = " << ret << dendl;
        return ret;
      } else {
        ldpp_dout(this, 20) << "getting tier placement handle cloud tier for " <<
                         " storage class " << target_placement.storage_class << dendl;
      }

      if (!tier->is_tier_type_s3()) {
        ldpp_dout(this, -1) << "ERROR: not s3 tier type - " << tier->get_tier_type() << 
                       " for storage class " << target_placement.storage_class << dendl;
        return -EINVAL;
      }

   // now go ahead with restoring object
   bool in_progress = true;
   ret = obj->restore_obj_from_cloud(bucket.get(), tier.get(), cct, days, in_progress,
		  		      this, y);
   if (ret < 0) {
      ldpp_dout(this, -1) << "Restore of object(" << obj->get_key() << ") failed" << ret << dendl;

      auto reset_ret = set_cloud_restore_status(this, obj.get(), y, rgw::sal::RGWRestoreStatus::RestoreFailed);

      entry.status = restore_failed;
      if (reset_ret < 0) {
        ldpp_dout(this, -1) << "Setting restore status ad RestoreFailed failed for object(" << obj->get_key() << ") " << reset_ret << dendl;
      }
      return ret;
   }

   if (in_progress) {
     ldpp_dout(this, 20) << "Restore of object " << obj->get_key() << " still in progress" << dendl;
   entry.status = restore_in_progress;
   } else {
     ldpp_dout(this, 20) << "Restore of object " << obj->get_key() << " succeeded" << dendl;
   entry.status = restore_complete;
   }
   return ret;
}

int RGWRestore::restore_obj_from_cloud(rgw::sal::Bucket* pbucket, rgw::sal::Object* pobj,
				       rgw::sal::PlacementTier* tier,
				       std::optional<uint64_t> days, optional_yield y)
{
   int ret = 0;
   if (!pbucket || !pobj) {
      ldpp_dout(this, -1) << "ERROR: Invalid bucket/object. Restore failed" << dendl;
      return -EINVAL;
   }

   // set restore_status as RESTORE_ALREADY_IN_PROGRESS
   ret = set_cloud_restore_status(this, pobj, y, rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress);
   if (ret < 0) {
     ldpp_dout(this, 0) << " Setting cloud restore status to RESTORE_ALREADY_IN_PROGRESS for the object(" << pobj->get_key() << " failed, ret=" << ret << dendl;
     return ret;
   }

   // now go ahead with restoring object
   bool in_progress = false;
   ret = pobj->restore_obj_from_cloud(pbucket, tier, cct, days, in_progress,
		  		      this, y);
   if (ret < 0) {
      ldpp_dout(this, -1) << "Restore of object(" << pobj->get_key() << ") failed" << ret << dendl;

      auto reset_ret = set_cloud_restore_status(this, pobj, y, rgw::sal::RGWRestoreStatus::RestoreFailed);

      if (reset_ret < 0) {
        ldpp_dout(this, -1) << "Setting restore status ad RestoreFailed failed for object(" << pobj->get_key() << ") " << reset_ret << dendl;
      }
      return ret;
   }

   ldpp_dout(this, 20) << "Restore of object " << pobj->get_key() << " succeeded" << dendl;

   if (in_progress) {
     // add restore entry to the list
     RestoreEntry entry;
     entry.bucket = pbucket->get_key();
     entry.obj_key = pobj->get_key();
     entry.status = restore_in_progress;
     entry.days = days;
     entry.zone_id = driver->get_zone()->get_id(); 

     int index = choose_oid(entry);
     ret = sal_restore->add_entry(this, y, index, entry);
     if (ret < 0) {
        ldpp_dout(this, -1) << "Adding restore entry of object(" << pobj->get_key() << ") failed" << ret << dendl;
     }
   }

   return ret;
}
