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

void RGWRestoreEntry::dump(Formatter *f) const
{
  encode_json("Bucket", bucket, f);
  encode_json("Object", obj_key, f);
  if (days) {
    encode_json("Days", days, f);
  } else {
    encode_json("Days", 0, f);
  }
  encode_json("Zone_id", zone_id, f);
  encode_json("Status", static_cast<int>(status), f);
}

void RGWRestoreEntry::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("Bucket", bucket, obj);
  JSONDecoder::decode_json("Object", obj_key, obj);
  JSONDecoder::decode_json("Days", days, obj);
  JSONDecoder::decode_json("Zone_id", zone_id, obj);
  int st;
  JSONDecoder::decode_json("Status", st, obj);
  status = static_cast<rgw::sal::RGWRestoreStatus>(st);
}

void RGWRestoreEntry::generate_test_instances(std::list<RGWRestoreEntry*>& l)
{
  auto p = new RGWRestoreEntry;
  rgw_bucket bk("tenant1", "bucket1");
  rgw_obj_key obj("object1");
  rgw_obj_key obj2("object2");
  uint64_t days1 = 3;
  std::optional<uint64_t> days;
  std::string zone_id = "zone1";
  rgw::sal::RGWRestoreStatus status = rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress;
  
  p->bucket = bk;
  p->obj_key = obj;
  p->zone_id = zone_id;
  p->days = days;
  p->status = status;
  l.push_back(p);

  p = new RGWRestoreEntry;
  days = days1;
  status = rgw::sal::RGWRestoreStatus::CloudRestored;
  p->bucket = bk;
  p->obj_key = obj2;
  p->zone_id = zone_id;
  p->days = days;
  p->status = status;
  l.push_back(p);

  l.push_back(new RGWRestoreEntry);
}

void RGWRestore::initialize(CephContext *_cct, rgw::sal::Driver* _driver) {
  cct = _cct;
  driver = _driver;

  /* max_objs indicates the number of shards or objects
   * used to store Restore Entries */
  max_objs = cct->_conf->rgw_restore_max_objs;
  if (max_objs > HASH_PRIME)
    max_objs = HASH_PRIME;

  for (int i = 0; i < max_objs; i++) {
    obj_names.push_back(fmt::format("{}.{}", restore_oid_prefix, i));
  }
  sal_restore = driver->get_restore(max_objs, obj_names);
}

void RGWRestore::finalize()
{
  obj_names.clear();
}

static inline std::ostream& operator<<(std::ostream &os, RGWRestoreEntry& ent) {
  os << "<ent: bucket=";
  os << ent.bucket;
  os << "; obj_key=";
  os << ent.obj_key;
  os << "; days=";
  os << ent.days;
  os << "; zone_id=";
  os << ent.zone_id;
  os << "; status=";
  os << rgw_restore_status_dump(ent.status);
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

/* Hash based on both <bucket, obj> */
int RGWRestore::choose_oid(const RGWRestoreEntry& e) {
  int index;
  const auto& name = e.bucket.name + e.obj_key.name + e.obj_key.instance;
  index = ((ceph_str_hash_linux(name.data(), name.size())) % max_objs);
  return static_cast<int>(index);
}

void *RGWRestore::RestoreWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    int r = 0;
    r = restore->process(this, null_yield);
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
    cond.wait_for(locker, std::chrono::seconds(secs));
  } while (!restore->going_down());

  return NULL;

}

int RGWRestore::process(RestoreWorker* worker, optional_yield y)
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

/* 
 * Given an index, fetch a list of restore entries to process. After each
 * iteration, trim the list to the last marker read.
 *
 * While processing the entries, if any of their restore operation is still in
 * progress, such entries are added back to the list.
 */ 
int RGWRestore::process(int index, int max_secs, optional_yield y)
{
  ldpp_dout(this, 20) << "RGWRestore::process entered with Restore index_shard="
		      << index << ", max_secs=" << max_secs << dendl;

  /* list used to gather still IN_PROGRESS */
  std::list<RGWRestoreEntry> r_entries;

  std::unique_ptr<rgw::sal::RestoreSerializer> serializer =
  			sal_restore->get_serializer(std::string(restore_index_lock_name),
				       	std::string(obj_names[index]),
					worker->thr_name());
  utime_t end = ceph_clock_now();

  /* max_secs should be greater than zero. We don't want a zero max_secs
   * to be translated as no timeout, since we'd then need to break the
   * lock and that would require a manual intervention. In this case
   * we can just wait it out. */

  if (max_secs <= 0)
    return -EAGAIN;

  end += max_secs;
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

  std::unique_lock<rgw::sal::RestoreSerializer> lock(*(serializer.get()), std::adopt_lock);
  std::string marker;
  std::string next_marker;
  bool truncated = false;

  do {
    int max = 100;
    std::vector<RGWRestoreEntry> entries;

    ret = sal_restore->list(this, y, index, marker, &next_marker, max, entries, &truncated);
    ldpp_dout(this, 20) <<
      "RGWRestore::process sal_restore->list returned with returned:" << ret <<
      ", entries.size=" << entries.size() << ", truncated=" << truncated <<
      ", next_marker='" << next_marker << "'" << dendl;

    if (entries.size() == 0) {
      lock.unlock();
      return 0;
    }

    if (ret < 0)
      goto done;

    marker = next_marker;
    std::vector<RGWRestoreEntry>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      RGWRestoreEntry entry = *iter;

      ret = process_restore_entry(entry, y);

      if (entry.status == rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress) {
	 r_entries.push_back(entry);
      }

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
    ret = sal_restore->trim_entries(this, y, index, marker);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWRestore::process() failed to trim entries on "
  	  	         << obj_names[index] << dendl;
    }

    if (!r_entries.empty()) {
      ret = sal_restore->add_entries(this, y, index, r_entries);
      if (ret < 0) {
        ldpp_dout(this, 0) << "RGWRestore::process() failed to add entries on "
  	  	           << obj_names[index] << dendl;
      }
    }

    r_entries.clear();
  } while (truncated);

done:
  lock.unlock();

  return 0;
}

int RGWRestore::process_restore_entry(RGWRestoreEntry& entry, optional_yield y)
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
  auto attr_iter = attrs.find(RGW_ATTR_RESTORE_STATUS);
  rgw::sal::RGWRestoreStatus restore_status = rgw::sal::RGWRestoreStatus::None;
  if (attr_iter != attrs.end()) {
    bufferlist bl = attr_iter->second;
    auto iter = bl.cbegin();
    decode(restore_status, iter);
  }
  if (restore_status == rgw::sal::RGWRestoreStatus::CloudRestored) {
    // XXX: Check if expiry-date needs to be update
    ldpp_dout(this, 20) << "Restore of object " << obj->get_key() << " already done" << dendl;
    entry.status = rgw::sal::RGWRestoreStatus::CloudRestored;
    return 0;
  }

  attr_iter = attrs.find(RGW_ATTR_STORAGE_CLASS);
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
  // XXX: first check if its already restored?
  bool in_progress = true;
  ret = obj->restore_obj_from_cloud(bucket.get(), tier.get(), cct, days, in_progress,
		  		      this, y);
  if (ret < 0) {
    ldpp_dout(this, -1) << "Restore of object(" << obj->get_key() << ") failed" << ret << dendl;
    auto reset_ret = set_cloud_restore_status(this, obj.get(), y, rgw::sal::RGWRestoreStatus::RestoreFailed);
    entry.status = rgw::sal::RGWRestoreStatus::RestoreFailed;
    if (reset_ret < 0) {
      ldpp_dout(this, -1) << "Setting restore status ad RestoreFailed failed for object(" << obj->get_key() << ") " << reset_ret << dendl;
    }
    return ret;
  }

  if (in_progress) {
    ldpp_dout(this, 20) << "Restore of object " << obj->get_key() << " still in progress" << dendl;
    entry.status = rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress;
  } else {
    ldpp_dout(this, 20) << "Restore of object " << obj->get_key() << " succeeded" << dendl;
    entry.status = rgw::sal::RGWRestoreStatus::RestoreFailed;
  }
  return ret;
}

time_t RGWRestore::thread_stop_at()
{
  uint64_t interval = (cct->_conf->rgw_restore_debug_interval > 0)
    ? cct->_conf->rgw_restore_debug_interval : secs_in_a_day;

  return time(nullptr) + interval;
}

int RGWRestore::set_cloud_restore_status(const DoutPrefixProvider* dpp,
			   rgw::sal::Object* pobj, optional_yield y,
	          	   const rgw::sal::RGWRestoreStatus& restore_status)
{
  int ret = -1;

  if (!pobj)
    return ret;

  pobj->set_atomic();

  bufferlist bl;
  using ceph::encode;
  encode(restore_status, bl);

  ret = pobj->modify_obj_attrs(RGW_ATTR_RESTORE_STATUS, bl, y, dpp, false);

  return ret;
}

int RGWRestore::restore_obj_from_cloud(rgw::sal::Bucket* pbucket,
	       			       rgw::sal::Object* pobj,
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
  ret = pobj->restore_obj_from_cloud(pbucket, tier, cct, days, in_progress, this, y);

  if (ret < 0) {
    ldpp_dout(this, 0) << "object " << pobj->get_key() << " fetching failed" << ret << dendl;
    auto reset_ret = set_cloud_restore_status(this, pobj, y, rgw::sal::RGWRestoreStatus::RestoreFailed);

    if (reset_ret < 0) {
      ldpp_dout(this, -1) << "Setting restore status ad RestoreFailed failed for object(" << pobj->get_key() << ") " << reset_ret << dendl;
    }

    return ret;
  }

  ldpp_dout(this, 20) << "Restore of object " << pobj->get_key() << " succeeded" << dendl;
  if (in_progress) {
    // add restore entry to the list
    RGWRestoreEntry entry;
    entry.bucket = pbucket->get_key();
    entry.obj_key = pobj->get_key();
    entry.status = rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress;
    entry.days = days;
    entry.zone_id = driver->get_zone()->get_id(); 

    int index = choose_oid(entry);
    ret = sal_restore->add_entry(this, y, index, entry);

    if (ret < 0) {
      ldpp_dout(this, -1) << "Adding restore entry of object(" << pobj->get_key() << ") failed" << ret << dendl;
    }
  }

  ldpp_dout(this, 20) << "Restore of object " << pobj->get_key() << " succeeded" << dendl;
  return ret;
}
