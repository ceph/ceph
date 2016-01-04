// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
  *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "BlockDevice.h"
#if defined(HAVE_SPDK)
#include "NVMEDevice.h"
#endif

BlockDevice *BlockDevice::create(const string& type, aio_callback_t cb, void *cbpriv)
{
  if (type == "kernel") {
    return new KernelDevice(cb, cbpriv);
  }
#if defined(HAVE_SPDK)
  if (type == "ust-nvme") {
    return new NVMEDeivce(cb, cbpriv);
  }
#endif

  return NULL;
}

