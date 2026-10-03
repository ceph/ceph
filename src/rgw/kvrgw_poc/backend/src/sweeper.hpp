// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "kv_store.hpp"
#include "typed_ids.hpp"

#include <atomic>
#include <cstdint>

namespace kvrgw {

class Sweeper {
 public:
  Sweeper(KvStore& store, std::atomic<bool>& stop_flag, int interval_sec, int min_age_sec);

  void run();
  void process_bucket(bucket_id_t bucket_id, bool force);

 private:
  void sweep_once();

  KvStore& store_;
  std::atomic<bool>& stop_flag_;
  int interval_sec_;
  int min_age_sec_;
};

}  // namespace kvrgw
