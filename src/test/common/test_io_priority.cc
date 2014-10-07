// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <errno.h>
#include <gtest/gtest.h>

#include "common/io_priority.h"

TEST(io_priority, ceph_ioprio_string_to_class) {
  ASSERT_EQ(IOPRIO_CLASS_IDLE, ceph_ioprio_string_to_class("idle"));
  ASSERT_EQ(IOPRIO_CLASS_IDLE, ceph_ioprio_string_to_class("IDLE"));

  ASSERT_EQ(IOPRIO_CLASS_BE, ceph_ioprio_string_to_class("be"));
  ASSERT_EQ(IOPRIO_CLASS_BE, ceph_ioprio_string_to_class("BE"));
  ASSERT_EQ(IOPRIO_CLASS_BE, ceph_ioprio_string_to_class("besteffort"));
  ASSERT_EQ(IOPRIO_CLASS_BE, ceph_ioprio_string_to_class("BESTEFFORT"));
  ASSERT_EQ(IOPRIO_CLASS_BE, ceph_ioprio_string_to_class("best effort"));
  ASSERT_EQ(IOPRIO_CLASS_BE, ceph_ioprio_string_to_class("BEST EFFORT"));

  ASSERT_EQ(IOPRIO_CLASS_RT, ceph_ioprio_string_to_class("rt"));
  ASSERT_EQ(IOPRIO_CLASS_RT, ceph_ioprio_string_to_class("RT"));
  ASSERT_EQ(IOPRIO_CLASS_RT, ceph_ioprio_string_to_class("realtime"));
  ASSERT_EQ(IOPRIO_CLASS_RT, ceph_ioprio_string_to_class("REALTIME"));
  ASSERT_EQ(IOPRIO_CLASS_RT, ceph_ioprio_string_to_class("real time"));
  ASSERT_EQ(IOPRIO_CLASS_RT, ceph_ioprio_string_to_class("REAL TIME"));

  ASSERT_EQ(-EINVAL, ceph_ioprio_string_to_class("invalid"));
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; 
 *   make -j4 unittest_io_priority &&
 *   libtool --mode=execute valgrind --tool=memcheck --leak-check=full \
 *      ./unittest_io_priority
 *   "
 * End:
 */
