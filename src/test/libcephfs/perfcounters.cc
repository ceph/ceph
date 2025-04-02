// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/compat.h"
#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "common/ceph_json.h"
#include "include/utime.h"

#include <string>

using namespace std;

TEST(LibCephFS, ValidatePerfCounters) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  char *perf_dump;
  int len = ceph_get_perf_counters(cmount, &perf_dump);
  ASSERT_GT(len, 0);

  JSONParser jp;
  ASSERT_TRUE(jp.parse(perf_dump, len));

  JSONObj *jo = jp.find_obj("client");

  // basic verification to chek if we have (some) fields in
  // the json object.
  utime_t val;
  JSONDecoder::decode_json("mdavg", val, jo);
  JSONDecoder::decode_json("readavg", val, jo);
  JSONDecoder::decode_json("writeavg", val, jo);

  int count;
  JSONDecoder::decode_json("mdops", count, jo);
  JSONDecoder::decode_json("rdops", count, jo);
  JSONDecoder::decode_json("wrops", count, jo);

  free(perf_dump);
  ceph_shutdown(cmount);
}
