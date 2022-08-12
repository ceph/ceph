// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Garbage Collector implementation for the CORTX Motr backend
 *
 * Copyright (C) 2022 Seagate Technology LLC and/or its Affiliates
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "gc.h"
#include <ctime>

void *MotrGC::GCWorker::entry() {
  std::unique_lock<std::mutex> lk(lock);
  ldpp_dout(dpp, 10) << __func__ << ": " << gc_thread_prefix
    << worker_id << " started." << dendl;

  // Get random number to lock the GC index.
  uint32_t my_index = \
    ceph::util::generate_random_number(0, motr_gc->max_indices - 1);
  uint32_t rgw_gc_processor_period = cct->_conf->rgw_gc_processor_period;
  // This is going to be endless loop
  do {
    std::string gc_log_prefix = "[" + gc_thread_prefix
                                    + std::to_string(worker_id) + "] ";
    ldpp_dout(dpp, 10) << gc_log_prefix << __func__ << " Iteration Started" << dendl;

    std::string iname = "";
    // Get lock on an GC index
    int rc = motr_gc->get_locked_gc_index(my_index);

    // Lock has been aquired, start the timer
    std::time_t start_time = std::time(nullptr);
    std::time_t end_time = start_time + \
			   cct->_conf->rgw_gc_processor_max_time - 10;
    std::time_t current_time = std::time(nullptr);

    if (rc == 0) {
      uint32_t processed_count = 0;
      // form the index name
      iname = motr_gc->index_names[my_index];
      ldpp_dout(dpp, 10) << gc_log_prefix
                      << __func__ << ": Working on GC Queue: " << iname << dendl;
      
      bufferlist bl;
      std::vector<std::string> keys(motr_gc->max_count + 1);
      std::vector<bufferlist> vals(motr_gc->max_count + 1);
      uint32_t rgw_gc_obj_min_wait = cct->_conf->rgw_gc_obj_min_wait;
      keys[0] = obj_exp_time_prefix;
      rc = motr_gc->store->next_query_by_name(iname,
                                  keys, vals, obj_exp_time_prefix);
      ldpp_dout(dpp, 0) << gc_log_prefix 
                         << __func__ <<": next_query_by_name() rc=" << rc << dendl;
      if (rc < 0) {
        // In case of failure, worker will keep retrying till end_time
        ldpp_dout(dpp, 0) << gc_log_prefix 
                        << __func__ <<": ERROR: NEXT query failed. rc=" << rc << dendl;
        continue;
      }

      // fetch entries as per defined in rgw_gc_max_trim_chunk from index iname
      for (uint32_t j = 0; j < motr_gc->max_count &&
                              !keys[j].empty() && keys[j] != obj_exp_time_prefix; j++) {
        bufferlist::const_iterator blitr = vals[j].cbegin();
        motr_gc_obj_info ginfo;
        ginfo.decode(blitr);
        // Check if the object is ready for deletion
        if(ginfo.deletion_time + rgw_gc_obj_min_wait > std::time(nullptr)) {
          // No more expired object for deletion
          break;
        }
        // delete motr object
        if(ginfo.is_multipart) {
          // handle multipart object deletion
        }
        else {
          // simple object
          rc = motr_gc->delete_motr_obj_from_gc(ginfo);
          if (rc < 0) {
            ldpp_dout(dpp, 0) << gc_log_prefix
                << __func__ <<"ERROR: Motr obj deletion failed for "
                << ginfo.tag << " with rc: " << rc << dendl;
            continue; // should continue deletion for next objects
          }
        }
        // delete entry from GC queue
        rc = motr_gc->dequeue(iname, ginfo);
        processed_count++;
        
        // Update current time
        current_time = std::time(nullptr);
        // Exit the loop if required work is complete
        if (processed_count >= motr_gc->max_count 
                              || current_time > end_time || motr_gc->going_down())
          break;
      }
      // unlock the GC queue
    }
    my_index = (my_index + 1) % motr_gc->max_indices;

    // sleep for remaining duration
    uint32_t unutilized_time = (rgw_gc_processor_period - (end_time - start_time));
    if (unutilized_time > 0) {
      cv.wait_for(lk, std::chrono::seconds(unutilized_time));
    }
  } while (!motr_gc->going_down());

  ldpp_dout(dpp, 0) << __func__ << ": Stop signal called for "
    << gc_thread_prefix << worker_id << dendl;
  return nullptr;
}

void MotrGC::initialize() {
  max_indices = get_max_indices();
  index_names.reserve(max_indices);
  ldpp_dout(this, 50) << __func__ << ": max_indices = " << max_indices << dendl;
  for (uint32_t ind_suf = 0; ind_suf < max_indices; ind_suf++) {
    std::string iname = gc_index_prefix + "." + std::to_string(ind_suf);
    int rc = store->create_motr_idx_by_name(iname);
    if (rc < 0 && rc != -EEXIST){
      ldout(cct, 0) << "ERROR: GC index creation failed with rc: " << rc << dendl;
      break;
    }
    index_names.push_back(iname);
  }
  // Get the max count of objects to be deleted in 1 processing cycle
  max_count = cct->_conf->rgw_gc_max_trim_chunk;
  if (max_count == 0) max_count = GC_DEFAULT_COUNT;

  // set random starting index for enqueue of delete requests
  enqueue_index = \
    ceph::util::generate_random_number(0, max_indices - 1);
}

void MotrGC::finalize() {
  // We do not delete GC queues or related lock entry.
  // GC queue & lock entries would be needed after RGW / cluster restart,
  // so do not delete those.
}

void MotrGC::start_processor() {
  // fetch max_concurrent_io i.e. max_threads to create from config.
  // start all the gc_worker threads
  auto max_workers = cct->_conf->rgw_gc_max_concurrent_io;
  ldpp_dout(this, 50) << __func__ << ": max_workers = "
    << max_workers << dendl;
  workers.reserve(max_workers);
  for (int ix = 0; ix < max_workers; ++ix) {
    auto worker = std::make_unique<MotrGC::GCWorker>(this /* dpp */,
                                                     cct, this, ix);
    worker->create((gc_thread_prefix + std::to_string(ix)).c_str());
    workers.push_back(std::move(worker));
  }
}

void MotrGC::stop_processor() {
  // gracefully shutdown all the gc threads.
  down_flag = true;
  for (auto& worker : workers) {
    ldout(cct, 20) << "stopping and joining "
      << gc_thread_prefix << worker->get_id() << dendl;
    worker->stop();
    worker->join();
  }
  workers.clear();
}

void MotrGC::GCWorker::stop() {
  std::lock_guard l{lock};
  cv.notify_all();
}

bool MotrGC::going_down() {
  return down_flag;
}

uint32_t MotrGC::get_max_indices() {
  // fetch max gc indices from config
  uint32_t rgw_gc_max_objs = cct->_conf->rgw_gc_max_objs;
  uint32_t gc_max_indices = 0;
  if (rgw_gc_max_objs) {
    rgw_gc_max_objs = pow(2, ceil(log2(rgw_gc_max_objs)));
    gc_max_indices = static_cast<uint32_t>(std::min(rgw_gc_max_objs,
                                            GC_MAX_QUEUES));
  }
  else {
    gc_max_indices = GC_DEFAULT_QUEUES;
  }
  return gc_max_indices;
}

int MotrGC::delete_motr_obj_from_gc(motr_gc_obj_info ginfo) {
  int rc;
  struct m0_op *op = nullptr;
  char fid_str[M0_FID_STR_LEN];
  snprintf(fid_str, ARRAY_SIZE(fid_str), U128X_F, U128_P(&ginfo.mobj.oid));

  if (!ginfo.mobj.oid.u_hi || !ginfo.mobj.oid.u_lo) {
    ldout(cct, 0) <<__func__<< ": invalid motr object oid=" << fid_str << dendl;
    return -EINVAL;
  }
  ldout(cct, 10) <<__func__<< ": deleting motr object oid=" << fid_str << dendl;

  // Open the object.
  if (ginfo.mobj.layout_id == 0) {
    return -ENOENT;
  }
  auto mobj = new m0_obj();
  memset(mobj, 0, sizeof *mobj);
  m0_obj_init(mobj, &store->container.co_realm, &ginfo.mobj.oid, store->conf.mc_layout_id);
  mobj->ob_attr.oa_layout_id = ginfo.mobj.layout_id;
  mobj->ob_attr.oa_pver      = ginfo.mobj.pver;
  mobj->ob_entity.en_flags  |= M0_ENF_META;
  rc = m0_entity_open(&mobj->ob_entity, &op);
  if (rc != 0) {
    ldout(cct, 0) <<__func__<< ": ERROR: m0_entity_open() failed: rc=" << rc << dendl;
    if (mobj != nullptr) {
      m0_obj_fini(mobj);
      delete mobj; mobj = nullptr;
    }
    return rc;
  }
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);
  if (rc < 0) {
    ldout(cct, 10) <<__func__<< ": ERROR: failed to open motr object: rc=" << rc << dendl;
    if (mobj != nullptr) {
      m0_obj_fini(mobj);
      delete mobj; mobj = nullptr;
    }
    return rc;
  }

  // Create an DELETE op and execute it.
  op = nullptr;
  mobj->ob_entity.en_flags |= M0_ENF_META;
  rc = m0_entity_delete(&mobj->ob_entity, &op);
  ldout(cct, 20) <<__func__<< ": m0_entity_delete() rc=" << rc << dendl;
  if (rc != 0) {
    ldout(cct, 0) <<__func__<< ": ERROR: m0_entity_delete() failed. rc=" << rc << dendl;
    return rc;
  }

  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc < 0) {
    ldout(cct, 0) <<__func__<< ": ERROR: failed to open motr object for deletion. rc="
                            << rc << dendl;
    return rc;
  }
  if (mobj != nullptr) {
    m0_obj_fini(mobj);
    delete mobj; mobj = nullptr;
  }
  ldout(cct, 10) <<__func__<< ": deleted motr object oid=" 
                            << fid_str << " for tag=" << ginfo.tag << dendl;
  return 0;
}

int MotrGC::enqueue(motr_gc_obj_info obj) {
  int rc = 0;
  // create 🔑's:
  //  - 1_{obj.deletion_time + min_wait_time}
  //  - 0_{obj.tag}
  std::string key1 = obj_exp_time_prefix +
                     std::to_string(obj.deletion_time + cct->_conf->rgw_gc_obj_min_wait);
  std::string key2 = obj_tag_prefix + obj.tag;

  bufferlist bl;
  obj.encode(bl);
  // push {1_ExpiryTime: motr_gc_obj_info} to the gc queue.📥
  rc = store->do_idx_op_by_name(index_names[enqueue_index],
                                M0_IC_PUT, key1, bl);
  if (rc < 0)
    return rc;
  // push {0_ObjTag: motr_gc_obj_info} to the gc queue.📥
  rc = store->do_idx_op_by_name(index_names[enqueue_index],
                                M0_IC_PUT, key2, bl);
  if (rc < 0) {
    // cleanup 🧹: pop key1 📤
    store->do_idx_op_by_name(index_names[enqueue_index],
                             M0_IC_DEL, key1, bl);
    // we are avoiding delete retry on failure since,
    // while processing the gc entry we will ignore the
    // 1_ExpiryTime entry if corresponding 0_ObjTag entry is absent.
    return rc;
  }

  // rolling increment the enqueue_index
  enqueue_index = (enqueue_index + 1) % max_indices;
  return rc;
}

int MotrGC::dequeue(std::string iname, motr_gc_obj_info obj) {
  int rc;
  bufferlist bl;
  std::string tag_key = obj_tag_prefix + obj.tag;
  std::string expiry_time_key = obj_exp_time_prefix +
                     std::to_string(obj.deletion_time + cct->_conf->rgw_gc_obj_min_wait);
  rc = store->do_idx_op_by_name(iname, M0_IC_DEL, tag_key, bl);
  if (rc < 0) {
    ldout(cct, 0) << "ERROR: failed to delete tag entry "
                        << tag_key << " rc: " << rc << dendl;
  }
  ldout(cct, 10) << "Deleted tag entry "<< tag_key << dendl;
  rc = store->do_idx_op_by_name(iname, M0_IC_DEL, expiry_time_key, bl);
  if (rc < 0 && rc != -EEXIST) {
    ldout(cct, 0) << "ERROR: failed to delete time entry "
                        << expiry_time_key << " rc: " << rc << dendl;
  }
  ldout(cct, 10) << "Deleted time entry "<< expiry_time_key << dendl;
  return rc;
}

int MotrGC::get_locked_gc_index(uint32_t& rand_ind) {
  int rc = -1;
  uint32_t new_index = 0;
  // attempt to lock GC starting with passed in index
  for (uint32_t ind = 1; ind < max_indices; ind++) {
    new_index = (ind + rand_ind) % max_indices;
    // try locking index
    // on sucess mark rc as 0
    rc = 0; // will be set by MotrLock.lock(gc_queue, exp_time);
    if (rc == 0)
      break;
  }
  rc = 0; // remove this line after lock implementation
  rand_ind = new_index;
  return rc;
}

int MotrGC::list(std::vector<std::unordered_map<std::string, std::string>> &gc_entries) {
  int rc = 0;
  int max_entries = 1000;
  max_indices = get_max_indices();
  for (uint32_t i = 0; i < max_indices; i++) {
    std::vector<std::string> keys(max_entries + 1);
    std::vector<bufferlist> vals(max_entries + 1);
    std::string marker = "";
    bool truncated = false;
    keys[0] = obj_tag_prefix;
    std::string iname = gc_index_prefix + "." + std::to_string(i);
    ldout(cct, 70) << "listing entries for " << iname << dendl;
    do {
      if (!marker.empty()) {
        keys[0] = marker;
      }
      rc = store->next_query_by_name(iname, keys, vals, obj_tag_prefix);
      if (rc < 0) {
        ldout(cct, 0) <<__func__<<": ERROR: NEXT query failed. rc="
          << rc << dendl;
        return rc;
      }
      if (rc == max_entries + 1) {
        truncated = true;
        marker = keys.back();
      }
      for (int j = 0; j < max_entries && 
                          !keys[j].empty() && keys[j] != obj_tag_prefix; j++) {
        bufferlist::const_iterator blitr = vals[j].cbegin();
        motr_gc_obj_info ginfo;
        ginfo.decode(blitr);
        std::unordered_map<std::string, std::string> mp;
        mp["tag"] = ginfo.tag;
        mp["name"] = ginfo.name;
        mp["deletion_time"] = std::to_string(ginfo.deletion_time);
        mp["size"] = std::to_string(ginfo.size);
        mp["size_actual"] = std::to_string(ginfo.size_actual);
        gc_entries.push_back(mp);
        ldout(cct, 70) << ginfo.tag << ", "
                       << ginfo.name << ", "
                       << ginfo.size << ", " << dendl;
      }
    } while (truncated);
  }
  return 0;
}

unsigned MotrGC::get_subsys() const {
  return dout_subsys;
}

std::ostream& MotrGC::gen_prefix(std::ostream& out) const {
  return out << "garbage_collector: ";
}
