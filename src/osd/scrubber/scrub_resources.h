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
class LocalResourceWrapper;

/**
 * The number of concurrent scrub operations performed on an OSD is limited
 * by a configuration parameter. The 'ScrubResources' class is responsible for
 * maintaining a count of the number of scrubs currently performed by primary
 * PGs on this OSD, and for enforcing the limit.
 */
class ScrubResources {
  friend class LocalResourceWrapper;

  /**
   * the number of concurrent scrubs performed by Primaries on this OSD.
   *
   * Note that, as high priority scrubs are always allowed to proceed, this
   * counter may exceed the configured limit. When in this state - no new
   * regular scrubs will be allowed to start.
   */
  int scrubs_local{0};

  mutable ceph::mutex resource_lock =
      ceph::make_mutex("ScrubQueue::resource_lock");

  log_upwards_t log_upwards;  ///< access into the owner's dout()

  const ceph::common::ConfigProxy& conf;

  /// an aux used to check available local scrubs. Must be called with
  /// the resource lock held.
  bool can_inc_local_scrubs_unlocked() const;

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
  std::unique_ptr<LocalResourceWrapper> inc_scrubs_local(bool is_high_priority);

  /// decrements the number of scrubs acting as a Primary
  void dec_scrubs_local();

  void dump_scrub_reservations(ceph::Formatter* f) const;
};


/**
 * a wrapper around a "local scrub resource". The resources bookkeeper
 * is handing these out to the PGs that acquired the local OSD's scrub
 * resources. The PGs use these to release the resources when they are
 * done scrubbing.
 */
class LocalResourceWrapper {
  ScrubResources& m_resource_bookkeeper;

 public:
  LocalResourceWrapper(
      ScrubResources& resource_bookkeeper);
  ~LocalResourceWrapper();
};

}  // namespace Scrub
