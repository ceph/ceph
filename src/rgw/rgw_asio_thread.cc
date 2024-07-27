// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_asio_thread.h"

#include "common/BackTrace.h"
#include "common/dout.h"
#include "include/ceph_assert.h"

thread_local bool is_asio_thread = false;

void maybe_warn_about_blocking(const DoutPrefixProvider* dpp)
{
  // work on asio threads should be asynchronous, so warn when they block
  if (!is_asio_thread) {
    return;
  }

  // for validation, tests can assert that no requests block
  const auto& conf = dpp->get_cct()->_conf;
  ceph_assert_always(!conf->rgw_asio_assert_yielding);

  // otherwise just log the warning and optional backtrace
  ldpp_dout(dpp, 20) << "WARNING: blocking librados call" << dendl;
#ifdef _BACKTRACE_LOGGING
  ldpp_dout(dpp, 20) << "BACKTRACE: " << ClibBackTrace(0) << dendl;
#endif
}
