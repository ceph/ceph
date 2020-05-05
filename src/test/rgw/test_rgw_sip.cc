// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include <iostream>
#include "global/global_init.h"
#include "global/global_context.h"
#include "common/ceph_argparse.h"
// #include "rgw/rgw_common.h"
#include "rgw/rgw_sync_info.h"

#include <gtest/gtest.h>

using namespace std;

template <class T>
static string encode_marker(T t)
{
  bufferlist bl;
  encode(t, bl);

  bufferlist b64;
  bl.encode_base64(b64);
  return b64.to_str();
}

template <class T>
static bool decode_marker(const string& marker, T *t)
{
  if (marker.empty()) {
    *t = T();
    return true;
  }
  bufferlist b64;
  b64.append(marker.c_str(), marker.size());
  try {
    bufferlist bl;
    bl.decode_base64(b64);

    auto iter = bl.cbegin();
    decode(*t, iter);
  } catch (ceph::buffer::error& err) {
   cerr << "failed to decode marker" << std::endl;
   return false;
  }

  return true;
}

struct BasicMarker {
  uint32_t val{0};

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(val, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(val, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(BasicMarker)

struct BasicData {
  string val;
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(val, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(val, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(BasicData)

class BasicProvider : public SIProvider_SingleStage
{
  vector<uint32_t> max_entries;

  string gen_data(int shard_id, uint32_t val) {
    stringstream ss;
    ss << shard_id << ": basic test data -- " << val;
    return ss.str();
  }

public: 
  BasicProvider(int _num_shards, uint32_t _max_entries) : SIProvider_SingleStage(g_ceph_context,
                                                                                 "basic",
                                                                                 SIProvider::StageType::FULL,
                                                                                 _num_shards) {
    for (int i = 0; i < _num_shards; ++i) {
      max_entries.push_back(_max_entries);
    }
  }

  BasicProvider(vector<uint32_t> _max_entries) : SIProvider_SingleStage(g_ceph_context,
                                                                   "basic",
                                                                   SIProvider::StageType::FULL,
                                                                   _max_entries.size()),
                                            max_entries(_max_entries) {}

  int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) override {
    if (shard_id >= (int)max_entries.size()) {
      return -ERANGE;
    }

    BasicMarker pos;

    result->entries.clear();

    if (!decode_marker(marker, &pos)) {
      return -EINVAL;
    }

    uint32_t val = pos.val;

    if (!marker.empty()) {
      val++;
    }

    for (int i = 0; i < max && val < max_entries[shard_id]; ++i, ++val) {
      SIProvider::Entry e;

      BasicData d{gen_data(shard_id, val)};
      d.encode(e.data);
      e.key = encode_marker(BasicMarker{val});

      result->entries.push_back(std::move(e));
    }

    result->more = (max_entries[shard_id] > 0 && val < max_entries[shard_id]);
    result->done = !result->more; /* simple provider, no intermediate state */

    return 0;
  }

  int do_get_start_marker(int shard_id, std::string *marker) const override {
    if (shard_id >= (int)max_entries.size()) {
      return -ERANGE;
    }

    marker->clear();
    return 0;
  }

  int do_get_cur_state(int shard_id, std::string *marker) const override {
    /* non incremental, no cur state */
    return -EINVAL;
  }
};

class LogProvider : public SIProvider_SingleStage
{
  vector<uint32_t> min_val;
  vector<uint32_t> max_val;

  bool done;


  string gen_data(int shard_id, uint32_t val) {
    stringstream ss;
    ss << shard_id << ": log test data (" << min_val << "-" << max_val << ") -- " << val;
    return ss.str();
  }

public: 
  LogProvider(int _num_shards,
              uint32_t _min_val,
              uint32_t _max_val) : SIProvider_SingleStage(g_ceph_context,
                                                          "log",
                                                          SIProvider::StageType::INC,
                                                          _num_shards)
  {
    for (int i = 0; i < _num_shards; ++i) {
      min_val.push_back(_min_val);
      max_val.push_back(_max_val);
    }
  }

  LogProvider(int _num_shards,
              uint32_t _min_val,
              vector<uint32_t >_max_val) : SIProvider_SingleStage(g_ceph_context,
                                                          "log",
                                                          SIProvider::StageType::INC,
                                                          _num_shards), max_val(_max_val)
  {
    for (int i = 0; i < _num_shards; ++i) {
      min_val.push_back(_min_val);
    }
  }

  void add_entries(int shard_id, int count) {
    max_val[shard_id] += count;
  }

  void trim(int shard_id, int new_min) {
    min_val[shard_id] = new_min;
  }

  int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) override {
    if (shard_id >= (int)min_val.size()) {
      return -ERANGE;
    }

    BasicMarker pos;

    result->entries.clear();

    if (!decode_marker(marker, &pos)) {
      return -EINVAL;
    }

    uint32_t val = pos.val;

    if (marker.empty()) {
      val = min_val[shard_id];
    } else {
      if (val >= max_val[shard_id]) {
        result->more = false;
        result->done = done;
        return 0;
      }
      val++;
    }

    for (int i = 0; i < max && val <= max_val[shard_id]; ++i, ++val) {
      SIProvider::Entry e;

      BasicData d{gen_data(shard_id, val)};
      d.encode(e.data);
      e.key = encode_marker(BasicMarker{val});

      result->entries.push_back(std::move(e));
    }

    result->more = (val <= max_val[shard_id]);
    result->done = (!result->more && done);

    return 0;
  }

  int do_get_start_marker(int shard_id, std::string *marker) const override {
    marker->clear();
    return 0;
  }

  int do_get_cur_state(int shard_id, std::string *marker) const override {
    return max_val[shard_id];
  }
};

static bool handle_result(SIProvider::fetch_result& result, string *marker)
{
  for (auto& e : result.entries) {
    if (marker) {
      *marker = e.key;
    }

    BasicData d;
    try {
      decode(d, e.data);
    } catch (buffer::error& err) {
      cerr << "ERROR: failed to decode data" << std::endl;
      return false;
    }
    cout << "entry: " << d.val << std::endl;
  }

  return true;
}

class TestProviderClient : public SIProviderClient
{
public:
  TestProviderClient(SIProviderRef& provider) : SIProviderClient(provider) {}

  int load_state() override {
    return 0;
  }

  int save_state() override {
    return 0;
  }

};

TEST(TestRGWSIP, test_basic_provider)
{
  int max_entries = 25;

  BasicProvider bp(1, max_entries);

  auto snum = bp.get_first_stage();

  string marker;
  ASSERT_EQ(0, bp.get_start_marker(snum, 0, &marker));

  int total = 0;

  int chunk_size = 10;

  do {
    SIProvider::fetch_result result;
    ASSERT_EQ(0, bp.fetch(snum, 0, marker, chunk_size, &result));

    total += result.entries.size();

    ASSERT_TRUE(handle_result(result, &marker));
    ASSERT_TRUE((total < max_entries) != (result.done));
  } while (total < max_entries);

  ASSERT_TRUE(total == max_entries);
}

TEST(TestRGWSIP, test_log_provider)
{
  int min_val = 12;
  int max_val = 33;

  int max_entries = max_val - min_val + 1;

  LogProvider lp(1, min_val, max_val);

  auto snum = lp.get_first_stage();

  string marker;
  ASSERT_EQ(0, lp.get_start_marker(snum, 0, &marker));

  int total = 0;

  int chunk_size = 10;

  do {
    SIProvider::fetch_result result;
    ASSERT_EQ(0, lp.fetch(snum, 0, marker, chunk_size, &result));

    total += result.entries.size();

    ASSERT_TRUE(handle_result(result, &marker));
    ASSERT_TRUE((total < max_entries) == (result.more));

  } while (total < max_entries);

  ASSERT_TRUE(total == max_entries);
}

TEST(TestRGWSIP, test_basic_provider_client)
{
  int max_entries = 25;

  auto bp = std::make_shared<BasicProvider>(1, max_entries);
  auto base = std::static_pointer_cast<SIProvider>(bp);

  TestProviderClient pc(base);

  ASSERT_EQ(0, pc.init_markers());

  int total = 0;
  int chunk_size = 10;

  do {
    SIProvider::fetch_result result;
    ASSERT_EQ(0, pc.fetch(0, chunk_size, &result));

    total += result.entries.size();

    ASSERT_TRUE(handle_result(result, nullptr));
    ASSERT_TRUE((total < max_entries) != (result.done));
  } while (total < max_entries);

  ASSERT_TRUE(total == max_entries);
}

struct stage_info {
  int num_shards;
  int first_shard_size;
  int shard_entries_limit;
  vector<uint32_t> max_entries;
  SIProviderRef provider_base;
  int all_entries{0};

  stage_info() {}
  stage_info(int _num_shards,
             int _start,
             int _limit,
             std::function<SIProviderRef(vector<uint32_t>)> alloc) : num_shards(_num_shards),
                                                                                   first_shard_size(_start),
                                                                                   shard_entries_limit(_limit) {
    max_entries.resize(num_shards);

    init(alloc);
  }

  void init(std::function<std::shared_ptr<SIProvider>(vector<uint32_t>)> alloc) {
    all_entries = 0;

    for (int i = 0; i < num_shards; ++i) {
      int max = first_shard_size + (i * 7) % (shard_entries_limit + 1);
      all_entries += max;
      max_entries[i] = max;
      cout << "max_entries[" << i << "]=" << max_entries[i] << std::endl;
    }

    provider_base = alloc(max_entries);
  }

  SIClientRef alloc_client() {
    auto pc = std::make_shared<TestProviderClient>(provider_base);
    return std::static_pointer_cast<SIClient>(pc);
  }
};

TEST(TestRGWSIP, test_sharded_stage)
{
  stage_info si(20, 20, 40, [](vector<uint32_t> max_entries){ 
    auto bp = std::make_shared<BasicProvider>(max_entries);
    return std::static_pointer_cast<SIProvider>(bp);
  });

  auto client = si.alloc_client();

  ASSERT_EQ(0, client->init_markers());

  uint32_t total[si.num_shards];
  int chunk_size = 10;

  memset(total, 0, sizeof(total));

  int total_iter = 0;
  int all_count = 0;

  while (!client->stage_complete()) {
    for (int i = 0; i < client->stage_num_shards(); ++i) {
      if (client->is_shard_done(i)) {
        continue;
      }
      SIProvider::fetch_result result;
      ASSERT_EQ(0, client->fetch(i, chunk_size, &result));

      total[i] += result.entries.size();
      all_count += result.entries.size();

      ASSERT_TRUE(handle_result(result, nullptr));
      ASSERT_NE((total[i] < si.max_entries[i]), result.done);
    }

    ++total_iter;
    ASSERT_TRUE(total_iter < (si.num_shards * si.shard_entries_limit)); /* avoid infinite loop due to bug */
  }

  ASSERT_EQ(si.all_entries, all_count);
}

TEST(TestRGWSIP, test_multistage)
{
  stage_info sis[2];

  sis[0] = stage_info(1, 35, 40, [](vector<uint32_t> max_entries){ 
    auto bp = std::make_shared<BasicProvider>(max_entries);
    return std::static_pointer_cast<SIProvider>(bp);
  });

  sis[1] = stage_info(10, 5, 20, [](vector<uint32_t> max_entries){ 
    auto bp = std::make_shared<LogProvider>(max_entries.size(), 0, max_entries);
   
    for (int i = 0; i < (int)max_entries.size(); ++i) {
      /* simulate half read log */
      bp->trim(i, max_entries[i] + 1);
      bp->add_entries(i, max_entries[i]);
    }
    return std::static_pointer_cast<SIProvider>(bp);
  });

  vector<SIProviderRef> providers = { sis[0].provider_base, sis[1].provider_base };

  SIProviderRef pvd_container = std::make_shared<SIProvider_Container>(g_ceph_context, "container", providers);

  TestProviderClient client(pvd_container);

  ASSERT_EQ(0, client.init_markers());

  for (int stage_num = 0; stage_num < 2; ++stage_num) {
    auto& si = sis[stage_num];

    int total[si.num_shards];
    int chunk_size = 10;

    memset(total, 0, sizeof(total));

    int total_iter = 0;
    int all_count = 0;

    int fetched_now;

    do {

      fetched_now = 0;
      
      for (int i = 0; i < client.stage_num_shards(); ++i) {

        if (client.is_shard_done(i)) {
          continue;
        }
        SIProvider::fetch_result result;
        ASSERT_EQ(0, client.fetch(i, chunk_size, &result));

        fetched_now = result.entries.size();

        total[i] += fetched_now;
        all_count += fetched_now;

        ASSERT_TRUE(handle_result(result, nullptr));
        ASSERT_NE((total[i] < (int)si.max_entries[i]), !result.more);
      }

      ++total_iter;
      ASSERT_TRUE(total_iter < (si.num_shards * si.shard_entries_limit)); /* avoid infinite loop due to bug */
    } while (fetched_now > 0);

    client.promote_stage(nullptr);

    ASSERT_EQ(si.all_entries, all_count);
  }
}


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

