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


#include <memory>
#include <mutex>

#include "QosProfileMgr.h"


osdc::qos_profile_ref osdc::QosProfileMgr::create(uint64_t r,
						  uint64_t w,
						  uint64_t l)
{
  return create(r, w, l, next_client_profile_id++);
}

osdc::qos_profile_ref osdc::QosProfileMgr::create(uint64_t r,
						  uint64_t w,
						  uint64_t l,
						  uint64_t profile_id)
{
  std::lock_guard<decltype(m)> lock(m);
  auto ref =
    std::make_shared<QosProfile>(r, w, l, profile_id);
  auto ret = profiles.insert(ref);
  // ret is a pair where first element is a const iterator to item just inserted
  return &(*ret.first);
}

int osdc::QosProfileMgr::release(qos_profile_ref qpr)
{
  std::lock_guard<decltype(m)> lock(m);
  auto i = profiles.find(*qpr);
  if (profiles.end() == i) {
    return ENOENT;
  } else {
    profiles.erase(i);
    return 0;
  }
}

const osdc::shared_qos_profile& osdc::get_default_qos_profile() {
  static const shared_qos_profile singleton =
    std::make_shared<QosProfile>(0u, 0u, 0u, 0u);
  return singleton;
}
