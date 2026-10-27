// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "test/osd/ReplicaPeeringTestFixture.h"

ReplicaPeeringTestFixture::ReplicaPeeringTestFixture()
  : PeeringTestFixture(REPLICATED) {
}

void ReplicaPeeringTestFixture::run_recovery(const std::string& obj_name)
{
  const hobject_t hoid = make_test_object(obj_name);
  const PeeringState* ps = get_primary_test_pg()->get_peering_state();

  auto recover = [&](bool to_primary) {
    eversion_t need;
    bool needs_recovery = ps->get_missing_loc().needs_recovery(hoid, &need);
    ceph_assert(needs_recovery);
    run_on_primary([this, hoid, need, to_primary] {
      // Pushing to replicas needs the object's context; a pull creates it.
      ObjectContextRef obc;
      if (!to_primary) {
        obc = get_object_context(hoid, false);
        ceph_assert(obc);
      }
      PGBackend* backend = get_primary_backend();
      PGBackend::RecoveryHandle* h = backend->open_recovery_op();
      int r = backend->recover_object(hoid, need, ObjectContextRef(), obc, h);
      ceph_assert(r == 0);
      backend->run_recovery_op(h, g_conf()->osd_recovery_op_priority);
    });
  };

  if (ps->get_pg_log().get_missing().is_missing(hoid)) {
    recover(true);
  }
  if (ps->get_missing_loc().needs_recovery(hoid)) {
    recover(false);
  }
  check_recovery_completion();
}
