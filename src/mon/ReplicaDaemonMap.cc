// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#include "ReplicaDaemonMap.h"

void ReplicaDaemonState::print_state(std::ostream& oss) const
{
  oss << "commit at epoch: " << commit_epoch << ", "
      << "replica addr: " << replica_route_addr
      << std::endl;
}
