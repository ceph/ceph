// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#ifdef __linux__

#include <atomic>
#include <string>
#include <thread>
#include "common/dout.h"
#include "user_cache.h"

namespace rgw::sal {

class UserDBWatcher {
  UserCache& user_cache;
  CephContext* cct;
  const DoutPrefix dp;
  std::string db_path;
  int inotify_fd{-1};
  int event_fd{-1};
  int watch_fd{-1};
  std::atomic<bool> shutdown{false};
  std::thread thrd;

  void watch_loop();

public:
  UserDBWatcher(UserCache& _cache, CephContext* _cct,
                const std::string& _db_path)
    : user_cache(_cache),
      cct(_cct),
      dp(_cct, ceph_subsys_rgw, "UserDBWatcher: "),
      db_path(_db_path)
  {}

  ~UserDBWatcher() { stop(); }

  int start(const DoutPrefixProvider* dpp);
  void stop();
};

} // namespace rgw::sal

#endif // __linux__
