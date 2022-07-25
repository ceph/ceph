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

#include "motr/gc/gc.h"

void *MotrGC::GCWorker::entry() {
  std::unique_lock<std::mutex> lk(lock);
  ldpp_dout(dpp, 10) << __func__ << ": " << gc_thread_prefix
    << worker_id << " started." << dendl;

  do {

    ldpp_dout(dpp, 10) << __func__ << ": " << gc_thread_prefix
      << worker_id << " iteration" << dendl;
    cv.wait_for(lk, std::chrono::milliseconds(gc_interval * 10));

  } while (! motr_gc->going_down());

  ldpp_dout(dpp, 0) << __func__ << ": Stop signalled called for "
    << gc_thread_prefix << worker_id << dendl;
  return nullptr;
}

void MotrGC::initialize() {
  // fetch max gc indices from config
  auto max_indices = std::min(cct->_conf->rgw_gc_max_objs,
                              GC_MAX_SHARDS_PRIME);
  ldpp_dout(this, 50) << __func__ << ": max_indices = " << max_indices << dendl;

  index_names.reserve(max_indices);
  for (int i = 0; i < max_indices; i++) {
    // Append index name to the gc index list
    index_names.push_back(gc_index_prefix + std::to_string(i));

    // [To be Implemented] create index in motr dix
  }

}

void MotrGC::finalize() {
  // [To be Implemented] undo steps from initialize stage
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

unsigned MotrGC::get_subsys() const {
  return dout_subsys;
}

std::ostream& MotrGC::gen_prefix(std::ostream& out) const {
  return out << "garbage_collector: ";
}
