// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 * Copyright (C) 2020 Abutalib Aghayev
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_BLK_HMSMRDEVICE_H
#define CEPH_BLK_HMSMRDEVICE_H

#include <atomic>

#include "include/types.h"
#include "include/interval_set.h"
#include "common/Thread.h"
#include "include/utime.h"

#include "aio/aio.h"
#include "BlockDevice.h"
#include "../kernel/KernelDevice.h"


class HMSMRDevice final : public KernelDevice {
  int zbd_fd = -1;	///< fd for the zoned block device

public:
  HMSMRDevice(CephContext* cct, aio_callback_t cb, void *cbpriv,
              aio_callback_t d_cb, void *d_cbpriv);

  static bool support(const std::string& path);

  // open/close hooks for libzbd
  int _post_open() override;
  void _pre_close() override;

  // smr-specific methods
  bool is_smr() const final { return true; }
  void reset_all_zones() override;
  void reset_zone(uint64_t zone) override;
  std::vector<uint64_t> get_zones() override;

};

#endif //CEPH_BLK_HMSMRDEVICE_H
