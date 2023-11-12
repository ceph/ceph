// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <functional>
#include <string>

#include "common/ceph_mutex.h"
#include "common/config_proxy.h"
#include "common/Formatter.h"
#include "osd/osd_types.h"

namespace Scrub {

/**
 * an interface allowing the ScrubResources to log directly into its
 * owner's log. This way, we do not need the full dout() mechanism
 * (prefix func, OSD id, etc.)
 */
using log_upwards_t = std::function<void(std::string msg)>;

/**
 * The number of concurrent scrub operations performed on an OSD is limited
 * by a configuration parameter. The 'ScrubResources' class is responsible for
 * maintaining a count of the number of scrubs currently performed, both
 * acting as primary and acting as a replica, and for enforcing the limit.
 */
class ScrubResources {
  /// the number of concurrent scrubs performed by Primaries on this OSD
  int scrubs_local{0};

  /// the set of PGs that have active scrub reservations as replicas
  /// \todo come C++23 - consider std::flat_set<pg_t>
  std::set<pg_t> granted_reservations;

  mutable ceph::mutex resource_lock =
      ceph::make_mutex("ScrubQueue::resource_lock");

  log_upwards_t log_upwards;  ///< access into the owner's dout()

  const ceph::common::ConfigProxy& conf;

 public:
  explicit ScrubResources(
      log_upwards_t log_access,
      const ceph::common::ConfigProxy& config);

  /**
   * \returns true if the number of concurrent scrubs is
   *  below osd_max_scrubs
   */
  bool can_inc_scrubs() const;

  /// increments the number of scrubs acting as a Primary
  bool inc_scrubs_local();

  /// decrements the number of scrubs acting as a Primary
  void dec_scrubs_local();

  /// increments the number of scrubs acting as a Replica
  bool inc_scrubs_remote(pg_t pgid);

  /// decrements the number of scrubs acting as a Replica
  void dec_scrubs_remote(pg_t pgid);

  void dump_scrub_reservations(ceph::Formatter* f) const;
};
}  // namespace Scrub
