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

#include <string>

using namespace std;

TEST(LibCephFS, ValidatePerfCounters) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  struct ceph_perf_counters perf;
  ASSERT_EQ(0, ceph_get_perf_counters(cmount, &perf));

  if (int count = perf.num_values) {
    std::cout << " -- type value:" << count << " -- " << std::endl;
    while (count > 0) {
      std::cout << "  name         : \"" << perf.values->desc.name << "\"" << std::endl;
      std::cout << "  description  : \"" << perf.values->desc.description << "\"" << std::endl;
      std::cout << "  priority     : " << perf.values->desc.prio << std::endl;
      std::cout << "  value        : " << perf.values->value << std::endl;
      std::cout << "  ---------------\n" << std::endl;
      ++perf.values;
      --count;
    }

    std::cout << std::endl;
  }

  if (int count = perf.num_times) {
    std::cout << " -- type time:" << count << " -- " << std::endl;
    while (count > 0) {
      std::cout << "  name         : \"" << perf.times->desc.name << "\"" << std::endl;
      std::cout << "  description  : \"" << perf.times->desc.description << "\"" << std::endl;
      std::cout << "  priority     : " << perf.times->desc.prio << std::endl;
      std::cout << "  value        : " << perf.times->value << std::endl;
      std::cout << "  ---------------\n" << std::endl;
      ++perf.times;
      --count;
    }

    std::cout << std::endl;
  }

  if (int count = perf.num_averages) {
    std::cout << " -- type averages:" << count << " -- " << std::endl;
    while (count > 0) {
      std::cout << "  name         : \"" << perf.averages->desc.name << "\"" << std::endl;
      std::cout << "  description  : \"" << perf.averages->desc.description << "\"" << std::endl;
      std::cout << "  priority     : " << perf.averages->desc.prio << std::endl;
      std::cout << "  avgcount     : " << perf.averages->avgcount << std::endl;
      std::cout << "  sum          : " << perf.averages->sum << std::endl;
      std::cout << "  ---------------\n" << std::endl;
      ++perf.averages;
      --count;
    }

    std::cout << std::endl;
  }

  if (int count = perf.num_time_averages) {
    std::cout << " -- type time_averages:" << count << " -- " << std::endl;
    while (count > 0) {
      std::cout << "  name         : \"" << perf.time_averages->desc.name << "\"" << std::endl;
      std::cout << "  description  : \"" << perf.time_averages->desc.description << "\"" << std::endl;
      std::cout << "  priority     : " << perf.time_averages->desc.prio << std::endl;
      std::cout << "  avgcount     : " << perf.time_averages->avgcount << std::endl;
      std::cout << "  sum          : " << perf.time_averages->sum << std::endl;
      std::cout << "  avgtime      : " << perf.time_averages->avgtime << std::endl;
      std::cout << "  ---------------\n" << std::endl;
      ++perf.time_averages;
      --count;
    }

    std::cout << std::endl;
  }

  ceph_free_perf_counters(&perf);
  ceph_shutdown(cmount);
}
