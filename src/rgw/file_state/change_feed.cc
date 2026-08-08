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

#include "change_feed.h"

#include <utility>
#include <vector>

namespace rgw::file_state {

ChangeFeed::Handle InProcessChangeFeed::subscribe(Callback cb) {
  std::lock_guard lock(mu_);
  Handle h = next_++;
  subs_.emplace(h, std::move(cb));
  return h;
}

void InProcessChangeFeed::unsubscribe(Handle h) {
  std::lock_guard lock(mu_);
  subs_.erase(h);
}

void InProcessChangeFeed::fire() {
  // Snapshot under the lock so callbacks don't see a torn map
  // if someone subscribes/unsubscribes while we're iterating,
  // and so callbacks don't call back into the feed under our
  // mutex.
  std::vector<Callback> snapshot;
  {
    std::lock_guard lock(mu_);
    snapshot.reserve(subs_.size());
    for (const auto& [_, cb] : subs_) {
      snapshot.push_back(cb);
    }
  }
  for (const auto& cb : snapshot) {
    cb();
  }
}

}  // namespace rgw::file_state
