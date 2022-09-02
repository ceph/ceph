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
#include "motr/sync/motr_sync_impl.h"

void *MotrGC::GCWorker::entry() {
  std::unique_lock<std::mutex> lk(lock);
  ldpp_dout(dpp, 10) << __func__ << ": " << gc_thread_prefix
    << worker_id << " started." << dendl;

  // Get random number to lock the GC index.
  uint32_t my_index = \
    ceph::util::generate_random_number(0, motr_gc->max_indices - 1);
  uint32_t rgw_gc_processor_period = cct->_conf->rgw_gc_processor_period;
  uint32_t rgw_gc_processing_time = cct->_conf->rgw_gc_processor_max_time;
  // This is going to be endless loop
  do {
    std::string gc_log_prefix = "[" + gc_thread_prefix
                                    + std::to_string(worker_id) + "] ";
    ldpp_dout(dpp, 10) << gc_log_prefix << __func__ << ": iteration started" << dendl;

    std::string iname = "";
    // Get lock on an GC index
    int rc = motr_gc->get_locked_gc_index(my_index,
      rgw_gc_processing_time);

    // Lock has been aquired, start the timer
    std::time_t start_time = std::time(nullptr);
    std::time_t end_time = start_time + \
			   rgw_gc_processing_time - 10;
    std::time_t current_time = std::time(nullptr);

    if (rc == 0) {
      uint32_t processed_count = 0;
      // form the index name
      iname = motr_gc->index_names[my_index];
      ldpp_dout(dpp, 10) << gc_log_prefix
                      << __func__ << ": working on GC queue: " << iname << dendl;

      bufferlist bl;
      std::vector<std::string> keys(motr_gc->max_count + 1);
      std::vector<bufferlist> vals(motr_gc->max_count + 1);
      uint32_t rgw_gc_obj_min_wait = cct->_conf->rgw_gc_obj_min_wait;
      keys[0] = obj_exp_time_prefix;
      rc = motr_gc->store->next_query_by_name(iname,
                                  keys, vals, obj_exp_time_prefix);
      ldpp_dout(dpp, 20) << gc_log_prefix
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
          // Handle distribution of parts of a multipart object across available 
          // GC queues. With large number of parts, a gc thread may exhaust its 
          // alloted time, in that case it will be preempted and remaining parts
          // will be processed in next cycle
          rc = motr_gc->process_parts(ginfo, end_time);
          if (rc < 0 && rc != -ETIMEDOUT) {
            ldpp_dout(dpp, 0) << gc_log_prefix << __func__
                    << ": ERROR: failed to distribute multipart object " 
                    << ginfo.name << " with tag " << ginfo.tag << " rc: " << rc << dendl;
            continue; // should continue processing next objects
          } else if (rc == -ETIMEDOUT) {
            ldpp_dout(dpp, 0) << gc_log_prefix << __func__
                    << ": processing time's up for current cycle" << dendl;
          } else {
            ldpp_dout(dpp, 20) << gc_log_prefix
                    << "successfully distributed multipart object "
                    << ginfo.name << " with tag " << ginfo.tag << dendl;
          }
        }
        else {
          // simple object
          rc = motr_gc->delete_obj_from_gc(ginfo);
          if (rc < 0) {
            ldpp_dout(dpp, 0) << gc_log_prefix
                << __func__ <<": ERROR: motr obj deletion failed for "
                << ginfo.tag << " with rc: " << rc << dendl;
            continue; // should continue deletion for next objects
          }
          ldpp_dout(dpp, 20) << gc_log_prefix
                << "successfully deleted object " << ginfo.name 
                << " with tag " << ginfo.tag << dendl;
        }
        // delete entry from GC queue unless there is a timeout
        if (rc != -ETIMEDOUT) {
          rc = motr_gc->dequeue(iname, ginfo);
          if (rc == 0) {
          ldpp_dout(dpp, 20) << gc_log_prefix 
                << "successfully deleted GC entry for " << ginfo.name 
                << " with tag " << ginfo.tag << dendl;
          }
          processed_count++;
        }

        // Update current time
        current_time = std::time(nullptr);
        // Exit the loop if required work is complete
        if (processed_count >= motr_gc->max_count
                              || current_time > end_time || motr_gc->going_down())
          break;
      }
      // unlock the GC queue
      motr_gc->un_lock_gc_index(my_index);
    } else {
      ldpp_dout(dpp, 0) << gc_log_prefix
          << __func__ << ": ERROR: no GC index is available for locking."
          << " Skipping GC processing" << " rc = " << rc << dendl;
    }
    my_index = (my_index + 1) % motr_gc->max_indices;

    // sleep for remaining duration
    uint32_t unutilized_time = (rgw_gc_processor_period - (end_time - start_time));
    if (unutilized_time > 0) {
      cv.wait_for(lk, std::chrono::seconds(unutilized_time));
    }
  } while (!motr_gc->going_down());

  ldpp_dout(dpp, 0) << __func__ << ": stop signal called for "
    << gc_thread_prefix << worker_id << dendl;
  return nullptr;
}

int MotrGC::initialize() {
  int rc = -1;
  max_indices = get_max_indices();
  index_names.reserve(max_indices);
  ldpp_dout(this, 50) << __func__ << ": max_indices = " << max_indices << dendl;
  for (uint32_t ind_suf = 0; ind_suf < max_indices; ind_suf++) {
    std::string iname = gc_index_prefix + "." + std::to_string(ind_suf);
    rc = store->create_motr_idx_by_name(iname);
    if (rc < 0 && rc != -EEXIST){
      ldout(cct, 0) << ": ERROR: GC index creation failed with rc: " << rc << dendl;
      break;
    }
    if (rc == -EEXIST) rc = 0;
    index_names.push_back(iname);
  }
  // Get the max count of objects to be deleted in 1 processing cycle
  max_count = cct->_conf->rgw_gc_max_trim_chunk;
  if (max_count == 0) max_count = GC_DEFAULT_COUNT;

  // set random starting index for enqueue of delete requests
  enqueue_index = \
    ceph::util::generate_random_number(0, max_indices - 1);
  // Init KV lock provider
  std::unique_ptr<MotrLockProvider> kv_lock_provider =
      std::make_unique<MotrKVLockProvider>();
  if (rc == 0) {
    // If everything above is success, initilaize KV lock provider
    rc = kv_lock_provider->initialize(
        this, store, global_lock_table);
    if (rc < 0) {
      ldout(cct, 0) << "ERROR: failed to initialize lock provider: "
        << rc << dendl;
      return rc;
    }
    // Create lock object
    get_lock_instance(kv_lock_provider);
    // Initilaize caller id
    caller_id = random_string(GC_CALLER_ID_STR_LEN);
  }
  return rc;
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

int MotrGC::delete_obj_from_gc(motr_gc_obj_info ginfo) {
  int rc = delete_motr_obj(ginfo.mobj);
  return rc;
}

int MotrGC::process_parts(motr_gc_obj_info ginfo, std::time_t end_time) {
  int rc = 0;
  int max_entries = 10000;
  int number_of_parts = 0;
  int processed_parts = 0;
  std::vector<std::string> keys(max_entries);
  std::vector<bufferlist> vals(max_entries);

  rc = store->next_query_by_name(ginfo.multipart_iname, keys, vals);
  if (rc < 0) {
    ldout(cct, 0) <<__func__<<": ERROR: next query failed. rc="
      << rc << dendl;
    return rc;  
  }
  number_of_parts = rc;
  for (const auto& bl: vals) {
    if (bl.length() == 0)
      break;

    RGWUploadPartInfo info;
    auto iter = bl.cbegin();
    info.decode(iter);
    rgw::sal::Attrs attrs_dummy;
    decode(attrs_dummy, iter);
    Meta mobj;
    mobj.decode(iter);
    ldout(cct, 20) <<__func__<< ": part_num=" << info.num
                             << " part_size=" << info.size << dendl;
    std::string tag = mobj.oid_str();
    std::string part_name = "part.";
    char buff[32];
    snprintf(buff, sizeof(buff), "%08d", (int)info.num);
    part_name.append(buff);

    std::string obj_fqdn = ginfo.name + "." + part_name;
    motr_gc_obj_info gc_obj(tag, obj_fqdn, mobj, ginfo.deletion_time,
                            info.size, false, "");
    rc = enqueue(gc_obj);
    if (rc < 0) {
      ldout(cct, 0) <<__func__<< ": ERROR: failed to push " 
                          << obj_fqdn << "into GC queue " << dendl;
      continue;
    }
    processed_parts++;
    bufferlist bl_del;
    rc = store->do_idx_op_by_name(ginfo.multipart_iname,
                                  M0_IC_DEL, part_name, bl_del);
    if (rc < 0) {
      ldout(cct, 0) <<__func__<< ": ERROR: failed to remove part " << part_name 
                      << " from part index " << ginfo.multipart_iname << dendl;
    }
    if (std::time(nullptr) > end_time || going_down()) {
      // processing time's up, so return now
      return -ETIMEDOUT;
    }
  }

  if (processed_parts == number_of_parts) {
    store->delete_motr_idx_by_name(ginfo.multipart_iname);
  }

  return rc;
}

int MotrGC::delete_motr_obj(Meta motr_obj) {
  int rc;
  struct m0_op *op = nullptr;

  if (!motr_obj.oid.u_hi || !motr_obj.oid.u_lo) {
    ldpp_dout(this, 0) <<__func__<< ": invalid motr object oid="
                       << motr_obj.oid_str() << dendl;
    return -EINVAL;
  }
  ldpp_dout(this, 10) <<__func__<< ": deleting motr object oid="
                      << motr_obj.oid_str() << dendl;

  // Open the object.
  if (motr_obj.layout_id == 0) {
    return -ENOENT;
  }
  auto mobj = new m0_obj();
  memset(mobj, 0, sizeof *mobj);
  m0_obj_init(mobj, &store->container.co_realm, &motr_obj.oid, store->conf.mc_layout_id);
  mobj->ob_attr.oa_layout_id = motr_obj.layout_id;
  mobj->ob_attr.oa_pver      = motr_obj.pver;
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
  return 0;
}

int MotrGC::enqueue(motr_gc_obj_info obj) {
  int rc = 0;
  // create 🔑's:
  //  - 1_{obj.deletion_time}_{obj.tag}
  //  - 0_{obj.tag}
  std::string key1 = obj_exp_time_prefix +
                     std::to_string(obj.deletion_time) + "_" +
                     obj.tag;
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
                                std::to_string(obj.deletion_time) +
                                "_" + obj.tag;
  rc = store->do_idx_op_by_name(iname, M0_IC_DEL, tag_key, bl);
  if (rc < 0) {
    ldout(cct, 0) << __func__ << ": ERROR: failed to delete tag entry "
                        << tag_key << " rc: " << rc << dendl;
  }
  ldout(cct, 10) << __func__ << ": deleted tag entry " << tag_key << dendl;
  rc = store->do_idx_op_by_name(iname, M0_IC_DEL, expiry_time_key, bl);
  if (rc < 0 && rc != -EEXIST) {
    ldout(cct, 0) << __func__ << ": ERROR: failed to delete time entry "
                        << expiry_time_key << " rc: " << rc << dendl;
  }
  ldout(cct, 10) << __func__ << ": deleted time entry "
                        << expiry_time_key << dendl;
  return rc;
}

int MotrGC::get_locked_gc_index(uint32_t& rand_ind,
                                uint32_t& lease_duration) {
  int rc = -1;
  uint32_t new_index = 0;
  std::shared_ptr<MotrSync>& gc_lock = get_lock_instance();

  // attempt to lock GC starting with passed in index
  for (uint32_t ind = 0; ind < max_indices; ind++) {
    new_index = (ind + rand_ind) % max_indices;
    if (gc_lock) {
      std::chrono::milliseconds lease_timeout{lease_duration * 1000};
      auto tv = ceph::to_timeval(lease_timeout);
      utime_t gc_lease_duration;
      gc_lease_duration.set_from_timeval(&tv);
      std::string iname = index_names[new_index];
      // try locking index
      rc = gc_lock->lock(iname, MotrLockType::EXCLUSIVE,
                         gc_lease_duration, caller_id);
      if (rc < 0) {
        ldout(cct, 10) << __func__ << ": failed to acquire lock: GC queue = [" << iname
            << "]" << "caller_id =[" << caller_id << "]" << "rc = " << rc << dendl;
      } else {
        ldout(cct, 10) << __func__ << ": acquired lock for GC queue = ["
            << iname << "]" << dendl;
      }
    }
    if (rc == 0)
      break;
  }
  rand_ind = new_index;
  return rc;
}

int MotrGC::un_lock_gc_index(uint32_t& index) {
  int rc = 0;
  std::shared_ptr<MotrSync>& gc_lock = get_lock_instance();
  if (gc_lock) {
    std::string iname = index_names[index];
    rc = gc_lock->unlock(iname, MotrLockType::EXCLUSIVE, caller_id);
  }
  return rc;
}

int MotrGC::list(std::vector<std::unordered_map<std::string, std::string>> &gc_entries) {
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
      int rc = store->next_query_by_name(iname, keys, vals, obj_tag_prefix);
      if (rc < 0) {
        ldout(cct, 0) << __func__ << ": ERROR: next query failed for " << iname
                      << " with rc=" << rc << dendl;
        continue;
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
        char t_str[100];
        std::string deletion_time;
        if (std::strftime(t_str, sizeof(t_str), "%Y-%m-%dT%H:%M:%S%z%Z", 
                                        std::localtime(&ginfo.deletion_time))) {
          deletion_time = t_str;
        } else {
          deletion_time = std::to_string(ginfo.deletion_time);
        }
        mp["tag"] = ginfo.tag;
        mp["name"] = ginfo.name;
        mp["deletion_time"] = deletion_time;
        mp["size"] = std::to_string(ginfo.size);
        mp["is_multipart"] = ginfo.is_multipart ? "true" : "false";
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
