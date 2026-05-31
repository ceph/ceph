// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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
#include "common/Clock.h" // for ceph_clock_now()
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
#include "rgw_restore_waiter.h"
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
#include "common/ceph_time.h"

#include <unordered_set>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include "common/async/spawn_throttle.h"
#include "rgw_asio_thread.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw_restore


constexpr int32_t hours_in_a_day = 24;
constexpr int32_t secs_in_a_day = hours_in_a_day * 60 * 60;
static constexpr size_t listing_max_entries = 1000;

using namespace std;
using namespace rgw::sal;

namespace rgw::restore {

void RestoreEntry::dump(Formatter *f) const
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

void RestoreEntry::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("Bucket", bucket, obj);
  JSONDecoder::decode_json("Object", obj_key, obj);
  JSONDecoder::decode_json("Days", days, obj);
  JSONDecoder::decode_json("Zone_id", zone_id, obj);
  int st;
  JSONDecoder::decode_json("Status", st, obj);
  status = static_cast<rgw::sal::RGWRestoreStatus>(st);
}

void RestoreEntry::generate_test_instances(std::list<RestoreEntry*>& l)
{
  auto p = new RestoreEntry;
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

  p = new RestoreEntry;
  days = days1;
  status = rgw::sal::RGWRestoreStatus::CloudRestored;
  p->bucket = bk;
  p->obj_key = obj2;
  p->zone_id = zone_id;
  p->days = days;
  p->status = status;
  l.push_back(p);

  l.push_back(new RestoreEntry);
}

static std::string restore_id = "rgw restore";
static std::string restore_req_id = "0";

void Restore::send_notification(const DoutPrefixProvider* dpp,
                              rgw::sal::Driver* driver,
                              rgw::sal::Object* obj,
                              rgw::sal::Bucket* bucket,
                              const std::string& etag,
                              uint64_t size,
                              const std::string& version_id,
                              const rgw::notify::EventTypeList& event_types,
                              optional_yield y) {
  // notification supported only for RADOS driver for now
  auto notify = driver->get_notification(
      dpp, obj, nullptr, event_types, bucket, restore_id,
      const_cast<std::string&>(bucket->get_tenant()), restore_req_id, y);

  if (!notify) {
    return;
  }

  int ret = notify->publish_reserve(dpp, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: notify publish_reserve failed with error: "
                      << ret << " for restore object: " << obj->get_name()
                      << " for event_types: " << event_types << dendl;
    return;
  }
  ret = notify->publish_commit(dpp, size, ceph::real_clock::now(), etag,
                               version_id);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "WARNING: notify publish_commit failed with error: "
                      << ret << " for lc object: " << obj->get_name()
                      << " for event_types: " << event_types << dendl;
  }
}

int Restore::initialize(CephContext *_cct, rgw::sal::Driver* _driver) {
  int ret = 0;
  cct = _cct;
  driver = _driver;

  // Initialize waiter registry
  if (!waiter_registry) {
    waiter_registry = std::make_shared<RestoreWaiterRegistry>();
  }

  ldpp_dout(this, 20) << __PRETTY_FUNCTION__ << ": initializing Restore handle" << dendl;
  /* max_objs indicates the number of shards or objects
   * used to store Restore Entries */
  max_objs = cct->_conf->rgw_restore_max_objs;
  if (max_objs > HASH_PRIME)
    max_objs = HASH_PRIME;

  obj_names.clear();
  for (int i = 0; i < max_objs; i++) {
    std::string s = fmt::format("{}.{}", restore_oid_prefix, i);
    obj_names.push_back(s); 
    ldpp_dout(this, 30) << __PRETTY_FUNCTION__ << ": obj_name_i=" << obj_names[i] << dendl;
  }

  sal_restore = driver->get_restore();

  if (!sal_restore) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": failed to create sal_restore" << dendl;
    return -EINVAL;
  }

  ret = sal_restore->initialize(this, null_yield, max_objs, obj_names);

  if (ret < 0) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": failed to initialize sal_restore" << dendl;
  }

  ldpp_dout(this, 20) << __PRETTY_FUNCTION__ << ": initializing Restore handle completed" << dendl;

  return ret;	  
}

void Restore::finalize()
{
  sal_restore.reset(nullptr);
  obj_names.clear();
  if (waiter_registry) {
    waiter_registry->shutdown();
    waiter_registry.reset();
  }
  ldpp_dout(this, 20) << __PRETTY_FUNCTION__ << ": finalize Restore handle" << dendl;
}

static inline std::ostream& operator<<(std::ostream &os, RestoreEntry& ent) {
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

void Restore::RestoreWorker::cancel_current_cycle_locked()
{
  if (cur_context && cur_signal) {
    // the signal lives on the worker thread's io_context; emit it from there
    auto* sig = cur_signal;
    boost::asio::post(*cur_context,
        [sig] { sig->emit(boost::asio::cancellation_type::terminal); });
  }
}

void Restore::RestoreWorker::stop()
{
  std::lock_guard l{lock};
  cancel_current_cycle_locked();
  cond.notify_all();   // also wake the inter-cycle sleep
}

void Restore::RestoreWorker::set_lock_lost_signal(rgw::sal::RestoreSerializer& serializer)
{
  std::lock_guard l{lock};
  serializer.set_lock_lost_signal(cur_signal);
}

bool Restore::going_down()
{
  return down_flag;
}

void Restore::start_processor()
{
  worker = std::make_unique<Restore::RestoreWorker>(this, cct, this);
  worker->create("rgw_restore");
}

void Restore::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  worker.reset(nullptr);
}

void Restore::wake_worker()
{
  if (worker) {
    std::lock_guard lock(worker->lock);
    worker->cond.notify_one();
  }
}

unsigned Restore::get_subsys() const
{
  return dout_subsys;
}

std::ostream& Restore::gen_prefix(std::ostream& out) const
{
  return out << "restore: ";
}

/* Hash based on both <bucket, obj> */
int Restore::choose_oid(const RestoreEntry& e) {
  int index;
  const auto& name = e.bucket.name + e.obj_key.name + e.obj_key.instance;
  index = ((ceph_str_hash_linux(name.data(), name.size())) % max_objs);
  return static_cast<int>(index);
}

void *Restore::RestoreWorker::entry() {
  do {
    utime_t start = ceph_clock_now();

    // run the cycle in a coroutine so process() gets a real yield_context
    int r = 0;
    boost::asio::io_context context;
    boost::asio::cancellation_signal cycle_sig;     // stop() emits this to cancel the cycle
    {
      // publish the active context + signal so stop() can reach this running cycle
      std::lock_guard l{lock};
      cur_context = &context;
      cur_signal = &cycle_sig;
    }
    boost::asio::spawn(context,
        [this] (boost::asio::yield_context yield) {
          return restore->process(this, optional_yield{yield});
        },
        // emitting cycle_sig cancels the coroutine tree; spawn_throttle propagates it
        boost::asio::bind_cancellation_slot(cycle_sig.slot(),
        [&r] (std::exception_ptr eptr, int result) {
          if (eptr) {
            // cancellation surfaces as operation_aborted -> clean exit
            try { std::rethrow_exception(eptr); }
            catch (const boost::system::system_error& e) {
              if (e.code() != boost::asio::error::operation_aborted) throw;
            }
          } else {
            r = result;
          }
        }));
    {
      auto warn = warn_about_blocking_in_scope{};   // flag accidental blocking calls
      context.run();
    }
    {
      std::lock_guard l{lock};
      cur_context = nullptr;
      cur_signal = nullptr;
    }
    if (r < 0 && r != -ECANCELED) {   // -ECANCELED == clean cancellation
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
                         << ": ERROR: restore process() returned error r=" << r << dendl;
    }
    if (restore->going_down())
      break;

    int secs = cct->_conf->rgw_restore_processor_period;
    if (secs <= 0)
      secs = secs_in_a_day;                         // avoid a busy-loop on misconfiguration

    // start-to-start period: sleep only the remainder; if we overran, run again now
    int elapsed = (int)(ceph_clock_now() - start).sec();
    if (elapsed >= secs)
      continue;
    std::unique_lock locker{lock};
    cond.wait_for(locker, std::chrono::seconds(secs - elapsed));
  } while (!restore->going_down());

  return NULL;
}

int Restore::process(RestoreWorker* worker, optional_yield y)
{
  int max_secs = cct->_conf->rgw_restore_lock_max_time;

  const int start = ceph::util::generate_random_number(0, max_objs - 1);
  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    int ret = process(index, max_secs, y);
    if (ret == -EBUSY)
      continue;                        // another RGW holds this shard; try the next one
    if (ret < 0)
      return ret;                      // -ECANCELED (cancellation) or a real error: stop the cycle
  }
  return 0;
}

// unique_lock expects an unlock() taking no arguments, but
// RadosRestoreSerializer::unlock() requires two. create an adapter that binds these
// additional args
struct RestoreLockAdapter {
  rgw::sal::RestoreSerializer& serializer;
  const DoutPrefixProvider* dpp = nullptr;
  optional_yield y;

  void unlock() {
    if (serializer.is_locked()) {
      serializer.unlock(dpp, y);
    }
  }
};

/* 
 * Given an index, fetch a list of restore entries to process. After each
 * iteration, trim the list to the last marker read.
 *
 * While processing the entries, if any of their restore operation is still in
 * progress, such entries are added back to the list.
 */ 
int Restore::process(int index, int max_secs, optional_yield y)
{
  ldpp_dout(this, 20) << __PRETTY_FUNCTION__ << ": process entered index=" << index << dendl;

  std::unique_ptr<rgw::sal::RestoreSerializer> serializer =
        sal_restore->get_serializer(std::string(restore_index_lock_name),
                                    std::string(obj_names[index]),
                                    worker->thr_name());
  worker->set_lock_lost_signal(*serializer);

  if (max_secs <= 0)
    return -EAGAIN;
  const ceph::timespan lock_dur = std::chrono::seconds(max_secs);

  // try_lock starts background renewal; the lock auto-renews until unlock()
  int ret = serializer->try_lock(this, lock_dur, y);
  if (ret == -EBUSY || ret == -EEXIST) {
    ldpp_dout(this, 0) << __PRETTY_FUNCTION__ << ": failed to acquire lock on "
                       << obj_names[index] << dendl;
    return -EBUSY;
  }
  if (ret < 0)
    return 0;

  utime_t end = ceph_clock_now();
  end += max_secs;

  auto lock_adapter = RestoreLockAdapter{*serializer, this, y};
  std::unique_lock<RestoreLockAdapter> lock(lock_adapter, std::adopt_lock);

  // clamp signed first: a negative value would wrap to a huge size_t (config min is 1)
  const size_t io_limit = static_cast<size_t>(
      std::max<int64_t>(1, cct->_conf->rgw_restore_max_concurrent_io));
  const int max_entries = std::max<int>(1,
      static_cast<int>(cct->_conf->rgw_restore_batch_size));
  auto yield = y.get_yield_context();

  std::string marker;
  std::string next_marker;
  bool truncated = false;

  /*
   * Keys re-queued this cycle (in-progress glacier entries re-appended to the FIFO head).
   * Re-encountering one means we wrapped -> stop. The key carries full identity because
   * choose_oid() hashes only name+obj.name+instance, so tenants/namespaces share a shard.
   */
  std::unordered_set<std::string> requeued;
  auto entry_key = [](const RestoreEntry& e) {
    const auto& b = e.bucket; const auto& k = e.obj_key;
    return b.tenant + '\0' + b.name + '\0' + b.bucket_id + '\0'
         + k.ns + '\0' + k.name + '\0' + k.instance;
  };

  try {
  do {
    std::vector<RestoreEntry> entries;

    ret = sal_restore->list(this, y, index, marker, &next_marker, max_entries, entries, &truncated);
    ldpp_dout(this, 20) << __PRETTY_FUNCTION__ <<
      ": list on shard:" << obj_names[index] << " returned:" << ret <<
      ", entries.size=" << entries.size() << ", truncated=" << truncated <<
      ", marker='" << marker << "'" << ", next_marker='" << next_marker << "'" << dendl;
    if (ret < 0)
      goto done;
    if (entries.size() == 0)
      break;                       // caught up

    {
      std::vector<RestoreEntry> batch_inprog;   // shared, no lock (single-threaded io_context)
      int batch_err = 0;                        // first hard (non-ENOENT) error in the batch
      bool wrapped = false;
      bool abort_batch = false;

      // process the batch concurrently, bounded by rgw_restore_max_concurrent_io
      {
        ceph::async::spawn_throttle workpool{yield, io_limit};
        std::unordered_set<std::string> seen_in_batch;   // coalesce same-object dups
        try {
          for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
            // check before spawning; spawn() can park, so at most one extra restore starts
            if (going_down() || !serializer->is_locked()) { abort_batch = true; break; }
            const std::string k = entry_key(*iter);
            if (requeued.count(k)) { wrapped = true; break; }
            // coalesce dups: the FIFO can hold an object twice; running both would race it
            if (!seen_in_batch.insert(k).second)
              continue;

            workpool.spawn(
              [this, index, &batch_inprog, &batch_err, e = *iter]
              (boost::asio::yield_context ey) mutable {
                int r;
                try {
                  r = process_restore_entry(e, optional_yield{ey});
                } catch (const boost::system::system_error& se) {
                  if (se.code() == boost::asio::error::operation_aborted) {
                    r = -ECANCELED;          // cancellation, not a failure -> batch_err below
                  } else {
                    if (!batch_err) batch_err = -EIO;
                    ldpp_dout(this, 0) << __PRETTY_FUNCTION__ << ": exception restoring "
                                       << e.obj_key << ": " << se.what() << dendl;
                    return;
                  }
                } catch (const std::exception& ex) {
                  if (!batch_err) batch_err = -EIO;
                  ldpp_dout(this, 0) << __PRETTY_FUNCTION__ << ": exception restoring "
                                     << e.obj_key << ": " << ex.what() << dendl;
                  return;
                }
                if (!r && e.status == rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress) {
                  batch_inprog.push_back(e);
                  ldpp_dout(this, 20) << __PRETTY_FUNCTION__ << ": re-queueing entry: '"
                                      << e << "' on shard:" << obj_names[index] << dendl;
                } else if (r < 0 && r != -ENOENT && !batch_err) {
                  batch_err = r;     // tolerated -ENOENT (missing bucket/obj) is ignored
                }
              });

          }
          if (abort_batch)
            workpool.cancel();   // abort in-flight restores (each aborts its cloud GET)
          workpool.wait();       // join all entries; cancelled children return -ECANCELED
        } catch (const boost::system::system_error& e) {
          /*
           * Cancellation only *signals* the children; their async_wait completes later.
           * Join them HERE, while batch_inprog/batch_err are still alive -- the workpool
           * destructor would only re-signal, not join. Real errors rethrow to done:.
           */
          if (e.code() != boost::asio::error::operation_aborted) throw;
          abort_batch = true;
          yield.reset_cancellation_state();   // re-arm, else the join's wait() aborts at once
          workpool.cancel();
          workpool.wait();       // children resume, return -ECANCELED, then complete -> joined
        }
      }

      // renewal failed -> lock may be lost; abort without trimming (retry next cycle)
      if (!serializer->is_locked()) {
        ldpp_dout(this, 0) << __PRETTY_FUNCTION__ << ": lock renewal failed on "
                           << obj_names[index] << "; aborting shard" << dendl;
        ret = -ECANCELED;
        goto done;
      }
      if (batch_err < 0) { ret = batch_err; goto done; }   // hard error: do NOT trim
      if (abort_batch) {
        ret = -ECANCELED;
        goto done;
      }
      if (wrapped)
        goto done;                 // wrapped batch: leave untrimmed for next cycle

      /*
       * Add-before-trim: re-queue in-progress entries before trimming, so a failure leaves
       * the originals in the FIFO (no loss; at worst a benign dup). Re-queued markers sort
       * beyond next_marker, so the inclusive trim won't remove them.
       */
      if (!batch_inprog.empty()) {
        ret = sal_restore->add_entries(this, y, index, batch_inprog);
        if (ret < 0) {
          ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": ERROR: failed to re-queue "
                              << batch_inprog.size() << " in-progress entries on "
                              << obj_names[index] << dendl;
          goto done;               // originals not trimmed -> no loss; surface error
        }
        if (!serializer->is_locked()) {
          ldpp_dout(this, 0) << __PRETTY_FUNCTION__ << ": lock renewal failed on "
                             << obj_names[index] << " after re-queue; aborting shard" << dendl;
          ret = -ECANCELED;
          goto done;
        }
        for (auto& e : batch_inprog)
          requeued.insert(entry_key(e));
      }

      marker = next_marker;        // trim the fully-processed batch
      ret = sal_restore->trim_entries(this, y, index, marker);
      if (ret < 0) {
        ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": ERROR: failed to trim entries on "
                            << obj_names[index] << dendl;
        goto done;
      }
      if (!serializer->is_locked()) {
        ldpp_dout(this, 0) << __PRETTY_FUNCTION__ << ": lock renewal failed on "
                           << obj_names[index] << " after trim; aborting shard" << dendl;
        ret = -ECANCELED;
        goto done;
      }
      ldpp_dout(this, 20) << __PRETTY_FUNCTION__ << ": trimmed till marker: '" << marker
                          << "' on shard:" << obj_names[index] << dendl;

      /*
       * Deadline checked at the batch boundary (after trim), never mid-batch, so a started
       * batch always makes progress. Worst-case overshoot is one batch of up to `max_entries`.
       */
      if (ceph_clock_now() >= end)
        break;
    }
  } while (truncated);
  } catch (const boost::system::system_error& e) {
    /*
     * Cancellation on a FIFO op (list/add/trim) lands here; the workpool is already joined
     * above. Catch it so done: runs in normal flow rather than unwinding unlock() on a
     * cancelled yield. Real errors rethrow.
     */
    if (e.code() != boost::asio::error::operation_aborted) throw;
    ret = -ECANCELED;
  }

done:
  /*
   * Re-arm the cancellation state so the unlock (cls-lock release + renewal stop, both
   * yielding ops) completes even when we got here by cancellation.
   */
  yield.reset_cancellation_state();
  lock.unlock();   // releases the cls_lock and stops the renewal coroutine
  return ret;
}

int Restore::process_restore_entry(RestoreEntry& entry, optional_yield y)
{
  int ret = 0;
  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::unique_ptr<rgw::sal::Object> obj;
  std::unique_ptr<rgw::sal::PlacementTier> tier;
  std::optional<uint64_t> days = entry.days;
  rgw::sal::RGWRestoreStatus restore_status = rgw::sal::RGWRestoreStatus::None;
  rgw_placement_rule target_placement;

  // mark in_progress as false if the entry is being processed first time
  bool in_progress = ((entry.status == rgw::sal::RGWRestoreStatus::None) ? false : true);

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
  ret = driver->load_bucket(this, entry.bucket, &bucket, y);
  if (ret < 0) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": ERROR: get_bucket for "
	   		<< bucket->get_name()	  
		       << " failed" << dendl;
    return ret;
  }
  obj = bucket->get_object(entry.obj_key);

  ret = obj->load_obj_state(this, y, true);

  if (ret < 0) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": ERROR: get_object for "
	   	       << entry.obj_key	  
		       << " failed" << dendl;
    return ret;
  }

  ldpp_dout(this, 10) << "Restore:: Processing restore entry of object(" << obj->get_key() << ") entry: " << entry << dendl;
  target_placement.inherit_from(bucket->get_placement_rule());

  auto& attrs = obj->get_attrs();
  auto attr_iter = attrs.find(RGW_ATTR_RESTORE_STATUS);
  if (attr_iter != attrs.end()) {
    bufferlist bl = attr_iter->second;
    auto iter = bl.cbegin();
    using ceph::decode;
    decode(restore_status, iter);
  }
  // check if its still in Progress state
  if (restore_status != rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress) {
    ldpp_dout(this, 5) << __PRETTY_FUNCTION__ << ": Restore of object " << obj->get_key()
	   	       << " not in progress state" << dendl;

    entry.status = restore_status;  
    return 0;
  }

  attr_iter = attrs.find(RGW_ATTR_STORAGE_CLASS);
  if (attr_iter != attrs.end()) {
    target_placement.storage_class = attr_iter->second.to_str();
  } else {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": ERROR: Attr RGW_ATTR_STORAGE_CLASS not found for object: " << obj->get_key() << dendl;
  }

  ret = driver->get_zone()->get_zonegroup().get_placement_tier(target_placement, &tier);

  if (ret < 0) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": ERROR: failed to fetch tier placement handle, target_placement = " << target_placement << ", for zonegroup = " << driver->get_zone()->get_zonegroup().get_name() << ", ret = " << ret << dendl;
    goto done;	  
  } else {
    ldpp_dout(this, 20) << __PRETTY_FUNCTION__ << ": getting tier placement handle"
	   		 << " cloud tier for " <<	  
                         " storage class " << target_placement.storage_class << dendl;
  }

  if (!tier->is_tier_type_s3()) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": ERROR: not s3 tier type - "
	   		<< tier->get_tier_type() << 	  
                      " for storage class " << target_placement.storage_class << dendl;
    goto done;
  }

  uint64_t size;
  // now go ahead with restoring object
  ret = obj->restore_obj_from_cloud(bucket.get(), tier.get(), cct, days, in_progress, size,
		  		      this, y);
  if (ret == -ECANCELED && (going_down() || yield_cancelled(y))) {
    // clean cancellation, not a failure: don't mark RestoreFailed; entry stays queued
    ldpp_dout(this, 10) << __PRETTY_FUNCTION__ << ": Restore of object(" << obj->get_key()
                        << ") canceled; will retry" << dendl;
    return ret;
  }
  if (ret < 0) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": Restore of object(" << obj->get_key() << ") failed" << ret << dendl;	  
    auto reset_ret = set_cloud_restore_status(this, obj.get(), y, rgw::sal::RGWRestoreStatus::RestoreFailed);
    if (reset_ret < 0) {
      ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": Setting restore status ad RestoreFailed failed for object(" << obj->get_key() << ") " << reset_ret << dendl;	    
    }
    goto done;
  }

  if (in_progress) {
    ldpp_dout(this, 15) << __PRETTY_FUNCTION__ << ": Restore of object " << obj->get_key() << " is still in progress" << dendl;
    entry.status = rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress;
  } else {
    ldpp_dout(this, 15) << __PRETTY_FUNCTION__ << ": Restore of object " << obj->get_key() << " succeeded" << dendl;
    entry.status = rgw::sal::RGWRestoreStatus::CloudRestored;
    
    string etag;
    attr_iter = attrs.find(RGW_ATTR_ETAG);
    if (attr_iter != attrs.end()) {
      etag = rgw_bl_str(attr_iter->second);
    }

    // send notification in case the restore is successfully completed
    send_notification(this, driver, obj.get(), bucket.get(), etag, size,
                      obj->get_key().instance,
                      {rgw::notify::ObjectRestoreCompleted}, y);
  }

done:
  if (ret < 0) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": Restore of entry:'" << entry << "' failed" << ret << dendl;	  
    entry.status = rgw::sal::RGWRestoreStatus::RestoreFailed;
  }

  // Notify any waiting GET requests
  if (waiter_registry && !in_progress) {
    bool success = (ret >= 0);
    waiter_registry->notify_completion(
      entry.bucket,
      entry.obj_key,
      success,
      ret
    );
  }

  return ret;
}

time_t Restore::thread_stop_at()
{
  uint64_t interval = (cct->_conf->rgw_restore_debug_interval > 0)
    ? cct->_conf->rgw_restore_debug_interval : secs_in_a_day;

  return time(nullptr) + interval;
}

int Restore::set_cloud_restore_status(const DoutPrefixProvider* dpp,
			   rgw::sal::Object* pobj, optional_yield y,
	          	   const rgw::sal::RGWRestoreStatus& restore_status)
{
  int ret = -1;

  if (!pobj)
    return ret;

  pobj->set_atomic(true);

  bufferlist bl;
  using ceph::encode;
  encode(restore_status, bl);

  ret = pobj->modify_obj_attrs(RGW_ATTR_RESTORE_STATUS, bl, y, dpp, false);

  return ret;
}

void Restore::get_expiration_date(const DoutPrefixProvider* dpp,
                               int expiry_days, ceph::real_time& exp_date) {
  constexpr int32_t secs_in_a_day = 24 * 60 * 60;
  ceph::real_time cur_time = real_clock::now();

  ldpp_dout(dpp, 5) << "Calculating expiration date for days:" << expiry_days << dendl;
  if (cct->_conf->rgw_restore_debug_interval > 0) {
    exp_date = cur_time + make_timespan(double(expiry_days)*cct->_conf->rgw_restore_debug_interval);
  } else {
    exp_date = cur_time + make_timespan(double(expiry_days) * secs_in_a_day);
  }
  ldpp_dout(dpp, 5) << "expiration date: " << exp_date << " , cur_time: " << cur_time << ", restore_interval: " << cct->_conf->rgw_restore_debug_interval  << dendl;
}

/*
 * As per AWS spec (https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html), 
 * After restoring an archived object, you can update the restoration period by reissuing the
 * request with a new period. Amazon S3 updates the restoration period relative to the current time.
 * You cannot update the restoration period when Amazon S3 is actively processing your current restore
 * request for the object.
 */
int Restore::update_cloud_restore_exp_date(rgw::sal::Bucket* pbucket,
	       			       rgw::sal::Object* pobj,
				       std::optional<uint64_t> days,
				       const DoutPrefixProvider* dpp,
				       optional_yield y)
{
  int ret = -1;
  ceph::real_time cur_time = real_clock::now();

  if (!pobj)
    return ret;

  if (!days) {// days should be present as we should update
              // expiry date only for temp copies
    return ret;
  }

  ret = pobj->get_obj_attrs(y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read object attrs " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ceph::real_time expiration_date;
  get_expiration_date(dpp, days.value(), expiration_date);

  auto& attrs = pobj->get_attrs();
  {
    bufferlist bl;
    using ceph::encode;
    encode(expiration_date, bl);
    attrs[RGW_ATTR_RESTORE_EXPIRY_DATE] = attrs[RGW_ATTR_DELETE_AT] = bl;
  }
  {
    bufferlist bl;
    using ceph::encode;
    encode(cur_time, bl);
    attrs[RGW_ATTR_INTERNAL_MTIME] = bl;
  }

  pobj->set_atomic(true);


  ret = pobj->set_obj_attrs(dpp, &attrs, nullptr, y, 0);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to update restore expiry date ret=" << ret << dendl;
    return ret;
  }

  ldpp_dout(dpp, 5) << "Updated Restore expiry time to: " << expiration_date << " , cur_time: " << cur_time << ", restore_interval: " << cct->_conf->rgw_restore_debug_interval  << dendl;
  pobj->set_atomic(false);


  return ret;
}

int Restore::restore_obj_from_cloud(rgw::sal::Bucket* pbucket,
	       			                      rgw::sal::Object* pobj,
                       			        rgw::sal::PlacementTier* tier,
                     				        std::optional<uint64_t> days,
                    				        const DoutPrefixProvider* dpp,
                    				        optional_yield y)
{
  int ret = 0;

  if (!pbucket || !pobj) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": ERROR: Invalid bucket/object. Restore failed" << dendl;	  
    return -EINVAL;
  }

  auto notify = driver->get_notification(
      dpp, pobj, nullptr,
      {rgw::notify::ObjectRestoreInitiated},
      pbucket, restore_id,
      const_cast<std::string&>(pbucket->get_tenant()), restore_req_id, y);

  if (notify) {
    int ret = notify->publish_reserve(dpp, nullptr);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: notify publish_reserve failed with error: "
      	                << ret << " for restore object: " << pobj->get_name()
        	              << " for event_types: rgw::notify::ObjectRestoreInitiated" << dendl;
      return ret;
    }
  }

  // set restore_status as RESTORE_ALREADY_IN_PROGRESS
  ret = set_cloud_restore_status(this, pobj, y, rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress);
  if (ret < 0) {
    ldpp_dout(this, 0) << __PRETTY_FUNCTION__ << ": Setting cloud restore status to RESTORE_ALREADY_IN_PROGRESS for the object(" << pobj->get_key() << " failed, ret=" << ret << dendl;	  
    return ret;
  }

  // now add the entry to the restore list to be processed by Restore worker thread
  // asynchronoudly
  RestoreEntry entry;
  entry.bucket = pbucket->get_key();
  entry.obj_key = pobj->get_key();
  // for first time mark status as None
  entry.status = rgw::sal::RGWRestoreStatus::None;
  entry.days = days;
  entry.zone_id = driver->get_zone()->get_id(); 
 
  ldpp_dout(this, 10) << "Restore:: Adding restore entry of object(" << pobj->get_key() << ") entry: " << entry << dendl;

  int index = choose_oid(entry);
  ldpp_dout(this, 10) << __PRETTY_FUNCTION__ << ": Adding restore entry of object(" << pobj->get_key() << ") entry: " << entry << ", to shard:" << obj_names[index] << dendl;

  std::vector<rgw::restore::RestoreEntry> r_entries;
  r_entries.push_back(entry);
  ret = sal_restore->add_entries(this, y, index, r_entries);

  if (ret < 0) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": ERROR: Adding restore entry of object(" << pobj->get_key() << ") failed" << ret << dendl;	    

    auto reset_ret = set_cloud_restore_status(this, pobj, y, rgw::sal::RGWRestoreStatus::RestoreFailed);
    if (reset_ret < 0) {
      ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": Setting restore status as RestoreFailed failed for object(" << pobj->get_key() << ") " << reset_ret << dendl;	      
    }

    return ret;
  }

  // For cloud-s3 tier (not glacier), wake the restore worker immediately
  // to start the download instead of waiting for the next periodic cycle
  if (tier && tier->get_tier_type() == "cloud-s3") {
    ldpp_dout(this, 10) << __PRETTY_FUNCTION__ << ": Waking restore worker for immediate processing (cloud-s3 tier)" << dendl;
    wake_worker();
  }

  ldpp_dout(this, 10) << __PRETTY_FUNCTION__ << ": Restore of object " << pobj->get_key() << " is in progress." << dendl;

  if (notify) {
    auto& attrs = pobj->get_attrs();
    string etag;
    auto attr_iter = attrs.find(RGW_ATTR_ETAG);
    if (attr_iter != attrs.end()) {
      etag = rgw_bl_str(attr_iter->second);
    }

    ret = notify->publish_commit(dpp, pobj->get_size(), ceph::real_clock::now(), etag,
		    		 pobj->get_key().instance);
    if (ret < 0) {
      ldpp_dout(dpp, 5) << "WARNING: notify publish_commit failed with error: "
                        << ret << " for lc object: " << pobj->get_name()
                        << " for event_types: rgw::notify::ObjectRestoreInitiated" << dendl;
    }
  }


  return ret;
}

int Restore::list(const DoutPrefixProvider* dpp, RestoreEntry& entry,
                  std::optional<string> restore_status_filter,
                  std::string& err_msg, RGWFormatterFlusher& flusher, optional_yield y)
{
  int ret = 0;
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = driver->load_bucket(dpp, entry.bucket, &bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not init bucket: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  params.list_versions = bucket->versioned();
  params.allow_unordered = true;
  flusher.start(0);
  auto f = flusher.get_formatter();
  f->open_object_section("restore_list");
  do {
    ret = bucket->list(dpp, params, listing_max_entries, results, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: driver->list_objects(): " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    for (vector<rgw_bucket_dir_entry>::iterator iter = results.objs.begin(); iter != results.objs.end(); ++iter) {
      std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(iter->key.name);
      if (obj) {
        ret = obj->get_obj_attrs(y, dpp);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << dendl;
          return -ret;
        }
        for (map<string, bufferlist>::iterator getattriter = obj->get_attrs().begin(); getattriter != obj->get_attrs().end(); ++getattriter) {
          bufferlist& bl = getattriter->second;
          if (getattriter->first == RGW_ATTR_RESTORE_STATUS) {
            rgw::sal::RGWRestoreStatus rs;
            {
              using ceph::decode;
              try {
                decode(rs, bl);
              } catch (const JSONDecoder::err& e) {
                ldpp_dout(dpp, 0) << "failed to decode JSON input: " << e.what() << dendl;
                return EINVAL;
              }
            }
            if (restore_status_filter) {
              if (restore_status_filter == rgw::sal::rgw_restore_status_dump(rs)) {
                f->dump_string(iter->key.name, rgw::sal::rgw_restore_status_dump(rs));
              }
            } else {
              f->dump_string(iter->key.name, rgw::sal::rgw_restore_status_dump(rs));
            }
          }
        }
      }
    }
  } while (results.is_truncated);
  f->close_section();
  flusher.flush();

  return ret;
}

int Restore::status(const DoutPrefixProvider* dpp, RestoreEntry& entry,
                    std::string& err_msg, RGWFormatterFlusher& flusher,
                    optional_yield y)
{
  int ret = 0;
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = driver->load_bucket(dpp, entry.bucket, &bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not init bucket: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  if (!entry.obj_key.name.empty()) {
    flusher.start(0);
    auto f = flusher.get_formatter();
    f->open_object_section("object restore status");
    f->dump_string("name", entry.obj_key.name);
    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(entry.obj_key);
    ret = obj->get_obj_attrs(y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << dendl;
      return -ret;
    }
    map<string, bufferlist>::iterator iter;
    for (iter = obj->get_attrs().begin(); iter != obj->get_attrs().end(); ++iter) {
      bufferlist& bl = iter->second;
      {
        using ceph::decode;
        if (iter->first == RGW_ATTR_RESTORE_STATUS) {
          rgw::sal::RGWRestoreStatus rs;
          try {
            decode(rs, bl);
          } catch (const JSONDecoder::err& e) {
            ldpp_dout(dpp, 0) << "failed to decode JSON input: " << e.what() << dendl;
            return EINVAL;
          }
          f->dump_string("RestoreStatus", rgw::sal::rgw_restore_status_dump(rs));
        } else if (iter->first == RGW_ATTR_RESTORE_TYPE) {
          rgw::sal::RGWRestoreType rt;
          try {
            decode(rt, bl);
          } catch (const JSONDecoder::err& e) {
            ldpp_dout(dpp, 0) << "failed to decode JSON input: " << e.what() << dendl;
            return EINVAL;
          }
          f->dump_string("RestoreType", rgw::sal::rgw_restore_type_dump(rt));
        } else if (iter->first == RGW_ATTR_RESTORE_EXPIRY_DATE) {
          ceph::real_time restore_expiry_date;
          try {
            decode(restore_expiry_date, bl);
          } catch (const JSONDecoder::err& e) {
            ldpp_dout(dpp, 0) << "failed to decode JSON input: " << e.what() << dendl;
            return EINVAL;
          }
          encode_json("RestoreExpiryDate", restore_expiry_date, f);
        } else if (iter->first == RGW_ATTR_RESTORE_TIME) {
          ceph::real_time restore_time;
          try {
            decode(restore_time, bl);
          } catch (const JSONDecoder::err& e) {
            ldpp_dout(dpp, 0) << "failed to decode JSON input: " << e.what() << dendl;
            return EINVAL;
          }
          encode_json("RestoreTime", restore_time, f);
        } else if (iter->first == RGW_ATTR_RESTORE_VERSIONED_EPOCH) {
          uint64_t versioned_epoch;
          try {
            decode(versioned_epoch, bl);
          } catch (const JSONDecoder::err& e) {
            ldpp_dout(dpp, 0) << "failed to decode JSON input: " << e.what() << dendl;
            return EINVAL;
          }
          f->dump_unsigned("RestoreVersionedEpoch", versioned_epoch);
        }
      }
    }
    f->close_section();
    flusher.flush();
  }

  return ret;
}

} // namespace rgw::restore
