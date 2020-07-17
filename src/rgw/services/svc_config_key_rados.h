

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include <atomic>

#include "rgw/rgw_service.h"

#include "svc_config_key.h"

class RGWSI_RADOS;

class RGWSI_ConfigKey_RADOS : public RGWSI_ConfigKey
{
  bool maybe_insecure_mon_conn{false};
  std::atomic_flag warned_insecure = ATOMIC_FLAG_INIT;

  int do_start() override;

  void warn_if_insecure();

public:
  struct Svc {
    RGWSI_RADOS *rados{nullptr};
  } svc;

  void init(RGWSI_RADOS *rados_svc) {
    svc.rados = rados_svc;
  }

  RGWSI_ConfigKey_RADOS(CephContext *cct) : RGWSI_ConfigKey(cct) {}

  int get(const string& key, bool secure, bufferlist *result) override;
};


