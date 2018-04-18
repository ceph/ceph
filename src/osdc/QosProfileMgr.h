// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "osd/osd_types.h"
#include "dmclock/src/dmclock_client.h"

namespace osdc {

  class QosProfile;

  using shared_qos_profile = std::shared_ptr<QosProfile>;
  using qos_profile_ref = const shared_qos_profile*;

  namespace dmc = crimson::dmclock;

  class QosProfile {

    friend class QosProfileMgr;
    friend const shared_qos_profile& get_default_qos_profile();

    qos_params_t params;
    dmc::ServiceTracker<int,dmc::OrigTracker> tracker;

  public:

    QosProfile(uint64_t r, uint64_t w, uint64_t l, uint64_t id) :
      params(r, w, l, id)
    {}

    uint64_t profile_id() const { return params.qos_profile_id; }
    qos_params_t& qos_params() { return params; }
    const qos_params_t& qos_params() const { return params; }
    dmc::ServiceTracker<int,dmc::OrigTracker>& service_tracker() {
      return tracker;
    }
  }; // class QosProfile

  class QosProfileMgr {

    std::set<shared_qos_profile> profiles;
    std::mutex m;
    uint64_t next_client_profile_id;

    qos_profile_ref create(uint64_t r, uint64_t w, uint64_t l,
			   uint64_t profile_id);

  public:

    QosProfileMgr() : next_client_profile_id(1) {}

    qos_profile_ref create(uint64_t r, uint64_t w, uint64_t l);

    int release(qos_profile_ref qpr);

    static uint64_t get_profile_id(qos_profile_ref qpr) {
      return (*qpr)->profile_id();
    }
  }; // QosProfileMgr

  const shared_qos_profile& get_default_qos_profile();
} // namespace osdc
