// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LAZY_OMAP_STATS_TEST_H
#define CEPH_LAZY_OMAP_STATS_TEST_H

#include <map>
#include <boost/regex.hpp>
#include <string>

#include "include/compat.h"
#include "include/rados/librados.hpp"

struct index_t {
  unsigned byte_index = 0;
  unsigned key_index = 0;
};

class LazyOmapStatsTest
{
  librados::IoCtx io_ctx;
  librados::Rados rados;
  std::map<std::string, librados::bufferlist> payload;

  struct lazy_omap_test_t {
    unsigned payload_size = 0;
    unsigned replica_count = 3;
    unsigned keys = 2000;
    unsigned how_many = 50;
    std::string pool_name = "lazy_omap_test_pool";
    std::string pool_id;
    unsigned total_bytes = 0;
    unsigned total_keys = 0;
  } conf;

  typedef enum {
    TARGET_MON,
    TARGET_MGR
  } CommandTarget;

  LazyOmapStatsTest(LazyOmapStatsTest&) = delete;
  void operator=(LazyOmapStatsTest) = delete;
  void init(const int argc, const char** argv);
  void shutdown();
  void write_omap(const std::string& object_name);
  const std::string get_name() const;
  void create_payload();
  void write_many(const unsigned how_many);
  void scrub();
  const int find_matches(std::string& output, boost::regex& reg) const;
  void check_one();
  const int find_index(std::string& haystack, boost::regex& needle,
                       std::string label) const;
  const unsigned tally_column(const unsigned omap_bytes_index,
                          const std::string& table, bool header) const;
  void check_column(const int index, const std::string& table,
                    const std::string& type, bool header = true) const;
  index_t get_indexes(boost::regex& reg, std::string& output) const;
  void check_pg_dump();
  void check_pg_dump_summary();
  void check_pg_dump_pgs();
  void check_pg_dump_pools();
  void check_pg_ls();
  const std::string get_output(
      const std::string command = R"({"prefix": "pg dump"})",
      const bool silent = false,
      const CommandTarget target = CommandTarget::TARGET_MGR);
  void get_pool_id(const std::string& pool);
  std::map<std::string, std::string> get_scrub_stamps();
  void wait_for_active_clean();

 public:
  LazyOmapStatsTest() = default;
  const int run(const int argc, const char** argv);
};

#endif // CEPH_LAZY_OMAP_STATS_TEST_H
