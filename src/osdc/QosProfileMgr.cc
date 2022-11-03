// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <memory>
#include <mutex>

#include "QosProfileMgr.h"

osdc::qos_profile_ref osdc::QosProfileMgr::create(uint64_t res,
                                                  uint64_t wgt,
                                                  uint64_t lim)
{
  std::lock_guard<decltype(m)> lock(m);
  return create_qos_profile(res, wgt, lim, next_client_profile_id++);
}

osdc::qos_profile_ref osdc::QosProfileMgr::create(uint64_t res,
                                                  uint64_t wgt,
                                                  uint64_t lim,
                                                  uint64_t profile_id)
{
  std::lock_guard<decltype(m)> lock(m);
  return create_qos_profile(res, wgt, lim, profile_id);
}

osdc::qos_profile_ref osdc::QosProfileMgr::create_qos_profile(uint64_t res,
                                                              uint64_t wgt,
                                                              uint64_t lim,
                                                              uint64_t cpid)
{
  auto ref = std::make_shared<QosProfile>(res, wgt, lim, cpid);
  auto ret = profiles.insert(ref);
  // ret is a pair where the first element is a const iterator to
  // the item just inserted.
  return &(*ret.first);
}

int osdc::QosProfileMgr::release(qos_profile_ref qpr)
{
  std::lock_guard<decltype(m)> lock(m);
  auto i = profiles.find(*qpr);
  if (profiles.end() == i) {
    return ENOENT;
  }
  profiles.erase(i);
  return 0;
}

const osdc::shared_qos_profile& osdc::get_default_qos_profile()
{
  // Set default QoS params [res:0, wgt:1, lim:0, profile_id:0]
  static const shared_qos_profile singleton =
    std::make_shared<QosProfile>(0u, 1u, 0u, 0u);
  return singleton;
}
