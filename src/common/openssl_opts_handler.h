// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2020 Huawei Technologies Co., Ltd.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_OPENSSL_OPTS_HANDLER_H
#define CEPH_OPENSSL_OPTS_HANDLER_H

namespace ceph {
  namespace crypto {
    void init_openssl_engine_once();
  }
}

#endif
