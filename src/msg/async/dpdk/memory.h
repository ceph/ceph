// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */
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

#ifndef CEPH_MSG_DPDK_MEMORY_H
#define CEPH_MSG_DPDK_MEMORY_H

#include <stdint.h>
#include <stddef.h>

#include "include/page.h"

using physical_address = uint64_t;
static size_t huge_page_size = 512 * CEPH_PAGE_SIZE;

struct translation {
  translation() = default;
  translation(physical_address a, size_t s) : addr(a), size(s) {}
  physical_address addr = 0;
  size_t size = 0;
};

// Translate a virtual address range to a physical range.
//
// Can return a smaller range (in which case the reminder needs
// to be translated again), or a zero sized range in case the
// translation is not known.
translation translate(const void* addr, size_t size);

#endif //CEPH_MSG_DPDK_MEMORY_H_H
