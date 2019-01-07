// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat Inc.
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <gtest/gtest.h>

#include "common/ceph_json.h"

#include <sstream>

using namespace std;


TEST(formatter, bug_37706) {
  vector<std::string> pgs;

   string outstring = 
"{\"pg_ready\":true, \"pg_stats\":[ { \"pgid\":\"1.0\", \"version\":\"16'56\",\"reported_seq\":\"62\",\"reported_epoch\":\"20\",\"state\":\"active+clean+inconsistent\",\"last_fresh\":\"2018-12-18 15:21:22.173804\",\"last_change\":\"2018-12-18 15:21:22.173804\",\"last_active\":\"2018-12-18 15:21:22.173804\",\"last_peered\":\"2018-12-18 15:21:22.173804\",\"last_clean\":\"2018-12-18 15:21:22.173804\",\"last_became_active\":\"2018-12-18 15:21:21.347685\",\"last_became_peered\":\"2018-12-18 15:21:21.347685\",\"last_unstale\":\"2018-12-18 15:21:22.173804\",\"last_undegraded\":\"2018-12-18 15:21:22.173804\",\"last_fullsized\":\"2018-12-18 15:21:22.173804\",\"mapping_epoch\":19,\"log_start\":\"0'0\",\"ondisk_log_start\":\"0'0\",\"created\":7,\"last_epoch_clean\":20,\"parent\":\"0.0\",\"parent_split_bits\":0,\"last_scrub\":\"16'56\",\"last_scrub_stamp\":\"2018-12-18 15:21:22.173684\",\"last_deep_scrub\":\"0'0\",\"last_deep_scrub_stamp\":\"2018-12-18 15:21:06.514438\",\"last_clean_scrub_stamp\":\"2018-12-18 15:21:06.514438\",\"log_size\":56,\"ondisk_log_size\":56,\"stats_invalid\":false,\"dirty_stats_invalid\":false,\"omap_stats_invalid\":false,\"hitset_stats_invalid\":false,\"hitset_bytes_stats_invalid\":false,\"pin_stats_invalid\":false,\"manifest_stats_invalid\":false,\"snaptrimq_len\":0,\"stat_sum\":{\"num_bytes\":24448,\"num_objects\":36,\"num_object_clones\":20,\"num_object_copies\":36,\"num_objects_missing_on_primary\":0,\"num_objects_missing\":0,\"num_objects_degraded\":0,\"num_objects_misplaced\":0,\"num_objects_unfound\":0,\"num_objects_dirty\":36,\"num_whiteouts\":3,\"num_read\":0,\"num_read_kb\":0,\"num_write\":36,\"num_write_kb\":50,\"num_scrub_errors\":20,\"num_shallow_scrub_errors\":20,\"num_deep_scrub_errors\":0,\"num_objects_recovered\":0,\"num_bytes_recovered\":0,\"num_keys_recovered\":0,\"num_objects_omap\":0,\"num_objects_hit_set_archive\":0,\"num_bytes_hit_set_archive\":0,\"num_flush\":0,\"num_flush_kb\":0,\"num_evict\":0,\"num_evict_kb\":0,\"num_promote\":0,\"num_flush_mode_high\":0,\"num_flush_mode_low\":0,\"num_evict_mode_some\":0,\"num_evict_mode_full\":0,\"num_objects_pinned\":0,\"num_legacy_snapsets\":0,\"num_large_omap_objects\":0,\"num_objects_manifest\":0},\"up\":[0],\"acting\":[0],\"blocked_by\":[],\"up_primary\":0,\"acting_primary\":0,\"purged_snaps\":[] }]}";


   JSONParser parser;
   ASSERT_TRUE(parser.parse(outstring.c_str(), outstring.size()));

   vector<string> v;

   ASSERT_TRUE (!parser.is_array());

   JSONObj *pgstat_obj = parser.find_obj("pg_stats");
   ASSERT_TRUE (!!pgstat_obj);
   auto s = pgstat_obj->get_data();

   ASSERT_TRUE(!s.empty());
   JSONParser pg_stats;
   ASSERT_TRUE(pg_stats.parse(s.c_str(), s.length()));
   v = pg_stats.get_array_elements();

   for (auto i : v) {
     JSONParser pg_json;
     ASSERT_TRUE(pg_json.parse(i.c_str(), i.length()));
     string pgid;
     JSONDecoder::decode_json("pgid", pgid, &pg_json);
     pgs.emplace_back(std::move(pgid));
   }

   ASSERT_EQ(pgs.back(), "1.0");
}

