// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_PAXOS_FSMAP_H
#define CEPH_PAXOS_FSMAP_H

#include <chrono>

#include "mds/FSMap.h"
#include "mds/MDSMap.h"

#include "include/ceph_assert.h"

class PaxosFSMap {
public:
  virtual ~PaxosFSMap() {}

  const FSMap &get_pending_fsmap() const { ceph_assert(is_leader()); return pending_fsmap; }
  const FSMap &get_fsmap() const { return fsmap; }

  virtual bool is_leader() const = 0;

protected:
  FSMap &get_pending_fsmap_writeable() { ceph_assert(is_leader()); return pending_fsmap; }

  FSMap &create_pending() {
    ceph_assert(is_leader());
    pending_fsmap = fsmap;
    pending_fsmap.inc_epoch();
    return pending_fsmap;
  }

  void prune_fsmap_history() {
    auto now = real_clock::now();
    for (auto it = history.begin(); it != history.end(); ) {
      auto since = now - it->second.get_btime();
      /* Be sure to not make the map empty */
      auto itnext = std::next(it);
      if (itnext == history.end()) {
        break;
      }
      /* Keep the map just before the prune time threshold:
       * [ e-1             (lifetime > history_prune_time) | e (lifetime 1s) ]
       * If an mds was removed in (e), then we want to be able to say it was
       * last seen 1 second ago.
       */
      auto since2 = now - itnext->second.get_btime();
      if (since > history_prune_time && since2 > history_prune_time) {
        it = history.erase(it);
      } else {
        break;
      }
    }
  }

  void put_fsmap_history(const FSMap& _fsmap) {
    auto now = real_clock::now();
    auto since = now - _fsmap.get_btime();
    if (since < history_prune_time) {
      history.emplace(std::piecewise_construct, std::forward_as_tuple(_fsmap.get_epoch()), std::forward_as_tuple(_fsmap));
    }
  }

  void set_fsmap_history_threshold(std::chrono::seconds t) {
    history_prune_time = t;
  }
  std::chrono::seconds get_fsmap_history_threshold() const {
    return history_prune_time;
  }

  const auto& get_fsmap_history() const {
    return history;
  }

  void decode(ceph::buffer::list &bl) {
    fsmap.decode(bl);
    put_fsmap_history(fsmap);
    pending_fsmap = FSMap(); /* nuke it to catch invalid access */
  }

private:
  /* Keep these PRIVATE to prevent unprotected manipulation. */
  std::map<epoch_t, FSMap> history;
  std::chrono::seconds history_prune_time = std::chrono::seconds(0);
  FSMap fsmap; /* the current epoch */
  FSMap pending_fsmap; /* the next epoch */
};


#endif
