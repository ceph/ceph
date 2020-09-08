// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <iostream>
#include <string>
#include <string_view>
#include <memory>

#include "include/ceph_features.h"
#include "include/ceph_assert.h"
#include "common/config.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/pick_address.h"
#include "common/Preforker.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "msg/Messenger.h"

#include "mon/MonMap.h"
#include "mon/MonClient.h"

#include "ReplicaDaemon.h"

#define FN_NAME (__CEPH_ASSERT_FUNCTION == nullptr ? __func__ : __CEPH_ASSERT_FUNCTION)
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cache_replica

using std::cerr;
using std::cout;
using std::map;
using std::ostringstream;
using std::string;
using std::string_view;
using std::vector;
using std::shared_ptr;

using ceph::bufferlist;

int main(int argc, const char **argv)
{
  ceph_pthread_setname(pthread_self(), "ceph-replica");

  return 0;
}
