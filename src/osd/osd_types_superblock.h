// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "include/CompatSet.h"
#include "common/ceph_mutex.h"
#include "osd/osd_types_core.h"
// ---------------------------------------

class OSDSuperblock {
private:
  class GuardedMap {
private:
  mutable ceph::mutex map_lock = ceph::make_mutex("map_lock");
  interval_set<epoch_t> maps;

  epoch_t calc_newest_map() const {
    if (!maps.empty()) {
      // maps stores [oldest_map, newest_map) (exclusive)
      return maps.range_end() - 1;
    }
    return 0;
  }
  epoch_t calc_oldest_map() const {
    if (!maps.empty()) {
      return maps.range_start();
    }
    return 0;
  }
public:
  GuardedMap() = default;
  GuardedMap(const GuardedMap& other)
    : map_lock(ceph::make_mutex("map_lock"))
  {
    std::lock_guard l(other.map_lock);  // Lock before copying shared state
    maps = other.maps;
  }

  GuardedMap& operator=(const GuardedMap& other) {
    if (this != &other) {
      std::scoped_lock l(map_lock, other.map_lock);
      maps = other.maps;
    }
    return *this;
  }

  GuardedMap(GuardedMap&& other)
    :map_lock(ceph::make_mutex("map_lock"))
  {
    std::lock_guard l(other.map_lock);
    maps = std::move(other.maps);
  }

  GuardedMap& operator=(GuardedMap&& other) noexcept {
    if (this != &other) {
      std::scoped_lock l(map_lock, other.map_lock);
      maps = std::move(other.maps);
    }
    return *this;
  }

  bool is_maps_empty() const {
    std::lock_guard lock(map_lock);
    return maps.empty();
  }

  void erase_oldest_maps() {
    std::lock_guard lock(map_lock);
    maps.erase(calc_oldest_map());
  }

  interval_set<epoch_t>::size_type get_maps_num_intervals() const {
    std::lock_guard lock(map_lock);
    return maps.num_intervals();
  }

  interval_set<epoch_t> get_maps() const {
    std::lock_guard lock(map_lock);
    return maps;
  }

  epoch_t get_oldest_map() const {
    std::lock_guard lock(map_lock);
    return calc_oldest_map();
  }

  epoch_t get_newest_map() const {
    std::lock_guard lock(map_lock);
    return calc_newest_map();
  }

  void insert_osdmap_epochs(epoch_t first, epoch_t last) {
    ceph_assert(std::cmp_less_equal(first, last));
    interval_set<epoch_t> message_epochs;
    message_epochs.insert(first, last - first + 1);
    std::lock_guard lock(map_lock);
    maps.union_of(message_epochs);
    ceph_assert(last == calc_newest_map());
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);

};
  WRITE_CLASS_ENCODER(GuardedMap);
  GuardedMap mapc;
public:
  uuid_d cluster_fsid, osd_fsid;
  int32_t whoami = -1;    // my role in this fs.
  epoch_t current_epoch = 0;             // most recent epoch

  bool is_maps_empty() const {
    return mapc.is_maps_empty();
  }

  void erase_oldest_maps() {
    mapc.erase_oldest_maps();
  }

  interval_set<epoch_t>::size_type get_maps_num_intervals() const {
    return mapc.get_maps_num_intervals();
  }

  interval_set<epoch_t> get_maps() const {
    return mapc.get_maps();
  }

  epoch_t get_oldest_map() const {
    return mapc.get_oldest_map();
  }

  epoch_t get_newest_map() const {
    return mapc.get_newest_map();
  }

  void insert_osdmap_epochs(epoch_t first, epoch_t last) {
    mapc.insert_osdmap_epochs(first, last);
  }

  double weight = 0.0;

  CompatSet compat_features;

  // last interval over which i mounted and was then active
  epoch_t mounted = 0;     // last epoch i mounted
  epoch_t clean_thru = 0;  // epoch i was active and clean thru

  epoch_t purged_snaps_last = 0;
  utime_t last_purged_snaps_scrub;

  epoch_t cluster_osdmap_trim_lower_bound = 0;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static std::list<OSDSuperblock> generate_test_instances();

  // Allow default operators to avoid crimson related errors
  OSDSuperblock(OSDSuperblock&&) noexcept = default;
  OSDSuperblock& operator=(OSDSuperblock&&) noexcept = default;
  OSDSuperblock(const OSDSuperblock&) = default;
  OSDSuperblock& operator=(const OSDSuperblock&) = default;
  OSDSuperblock() = default;
};
WRITE_CLASS_ENCODER(OSDSuperblock)

inline std::ostream& operator<<(std::ostream& out, const OSDSuperblock& sb)
{
  return out << "sb(" << sb.cluster_fsid
             << " osd." << sb.whoami
	     << " " << sb.osd_fsid
             << " e" << sb.current_epoch
             << " maps " << sb.get_maps()
	     << " lci=[" << sb.mounted << "," << sb.clean_thru << "]"
             << " tlb=" << sb.cluster_osdmap_trim_lower_bound
             << ")";
}
