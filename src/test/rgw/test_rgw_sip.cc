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

class BasicProvider : public SIProvider
{
  int id;
  uint32_t max_entries;

  string gen_data(uint32_t val) {
    stringstream ss;
    ss << id << ": basic test data -- " << val;
    return ss.str();
  }

public: 
  BasicProvider(int _id, uint32_t _max_entries) : id(_id), max_entries(_max_entries) {}

  SIProvider::Info get_info() const {
    return { SIProvider::Type::FULL, 1 };
  }

  int fetch(int shard_id, std::string marker, int max, fetch_result *result) override {
    BasicMarker pos;

    result->entries.clear();

    if (!decode_marker(marker, &pos)) {
      return -EINVAL;
    }

    uint32_t val = pos.val;

    if (!marker.empty()) {
      val++;
    }

    for (int i = 0; i < max && val < max_entries; ++i, ++val) {
      SIProvider::Entry e;

      BasicData d{gen_data(val)};
      d.encode(e.data);
      e.key = encode_marker(BasicMarker{val});

      result->entries.push_back(std::move(e));
    }

    result->more = (max_entries > 0 && val < max_entries);
    result->done = !result->more; /* simple provider, no intermediate state */

    return 0;
  }

  int get_start_marker(int shard_id, std::string *marker) const override {
    marker->clear();
    return 0;
  }

  int get_cur_state(int shard_id, std::string *marker) const override {
    /* non incremental, no cur state */
    return -EINVAL;
  }
};

class LogProvider : public SIProvider
{
  int id;

  uint32_t min_val;
  uint32_t max_val;

  bool done;


  string gen_data(uint32_t val) {
    stringstream ss;
    ss << id << ": log test data (" << min_val << "-" << max_val << ") -- " << val;
    return ss.str();
  }

public: 
  LogProvider(int _id,
              uint32_t _min_val,
              uint32_t _max_val) : id(_id),
                                   min_val(_min_val),
                                   max_val(_max_val) {}

  SIProvider::Info get_info() const {
    return { SIProvider::Type::INC, 1 };
  }

  void add_entries(int count) {
    max_val += count;
  }

  void trim(int new_min) {
    min_val = new_min;
  }

  void set_done(bool _done) {
    done = _done;
  }

  int fetch(int shard_id, std::string marker, int max, fetch_result *result) override {
    BasicMarker pos;

    result->entries.clear();

    if (!decode_marker(marker, &pos)) {
      return -EINVAL;
    }

    uint32_t val = pos.val;

    if (marker.empty()) {
      val = min_val;
    } else {
      if (val >= max_val) {
        result->more = false;
        result->done = done;
        return 0;
      }
      val++;
    }

    for (int i = 0; i < max && val <= max_val; ++i, ++val) {
      SIProvider::Entry e;

      BasicData d{gen_data(val)};
      d.encode(e.data);
      e.key = encode_marker(BasicMarker{val});

      result->entries.push_back(std::move(e));
    }

    result->more = (val <= max_val);
    result->done = (!result->more && done);

    return 0;
  }

  int get_start_marker(int shard_id, std::string *marker) const override {
    marker->clear();
    return 0;
  }

  int get_cur_state(int shard_id, std::string *marker) const override {
    return max_val;
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

  BasicProvider bp(0, max_entries);

  string marker;
  ASSERT_EQ(0, bp.get_start_marker(0, &marker));

  int total = 0;

  int chunk_size = 10;

  do {
    SIProvider::fetch_result result;
    ASSERT_EQ(0, bp.fetch(0, marker, chunk_size, &result));

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

  LogProvider lp(0, min_val, max_val);

  string marker;
  ASSERT_EQ(0, lp.get_start_marker(0, &marker));

  int total = 0;

  int chunk_size = 10;

  do {
    SIProvider::fetch_result result;
    ASSERT_EQ(0, lp.fetch(0, marker, chunk_size, &result));

    total += result.entries.size();

    ASSERT_TRUE(handle_result(result, &marker));
    ASSERT_TRUE((total < max_entries) == (result.more));

  } while (total < max_entries);

  ASSERT_TRUE(total == max_entries);
}

TEST(TestRGWSIP, test_basic_provider_client)
{
  int max_entries = 25;

  auto bp = std::make_shared<BasicProvider>(0, max_entries);
  auto base = std::static_pointer_cast<SIProvider>(bp);

  TestProviderClient pc(base);

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
  vector<int> max_entries;
  int all_entries{0};
  vector<SIClientRef> shards;

  stage_info() {}
  stage_info(int _num_shards,
             int _start,
             int _limit,
             std::function<std::shared_ptr<SIProvider>(int, int)> alloc) : num_shards(_num_shards),
                                                                           first_shard_size(_start),
                                                                           shard_entries_limit(_limit) {
    max_entries.resize(num_shards);

    init(alloc);
  }

  void init(std::function<std::shared_ptr<SIProvider>(int, int)> alloc) {
    all_entries = 0;

    for (int i = 0; i < num_shards; ++i) {
      int max = first_shard_size + (i * 7) % (shard_entries_limit + 1);
      all_entries += max;
      max_entries[i] = max;
      cout << "max_entries[" << i << "]=" << max_entries[i] << std::endl;
  
      auto base = alloc(i, max_entries[i]);
      auto pc = std::make_shared<TestProviderClient>(base);

      shards.push_back(std::static_pointer_cast<SIClient>(pc));
    }
  }
};

TEST(TestRGWSIP, test_sharded_stage)
{
  stage_info si(20, 20, 40, [](int id, int max_entries){ 
    auto bp = std::make_shared<BasicProvider>(id, max_entries);
    return std::static_pointer_cast<SIProvider>(bp);
  });

  auto stage = make_shared<SIPShardedStage>(si.shards);

  ASSERT_EQ(0, stage->init_markers(true));

  int total[si.num_shards];
  int chunk_size = 10;

  memset(total, 0, sizeof(total));

  int total_iter = 0;
  int all_count = 0;

  while (!stage->is_complete()) {
    for (int i = 0; i < stage->num_shards(); ++i) {
      if (stage->is_shard_done(i)) {
        continue;
      }
      SIProvider::fetch_result result;
      ASSERT_EQ(0, stage->fetch(i, chunk_size, &result));

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
  vector<SIPShardedStageRef> stages;

  stage_info sis[2];

  sis[0] = stage_info(1, 35, 40, [](int id, int max_entries){ 
    auto bp = std::make_shared<BasicProvider>(id, max_entries);
    return std::static_pointer_cast<SIProvider>(bp);
  });

  auto stage1 = make_shared<SIPShardedStage>(sis[0].shards);

  sis[1] = stage_info(10, 5, 20, [](int id, int max_entries){ 
    auto bp = std::make_shared<LogProvider>(id, 0, max_entries);

    /* simulate half read log */
    bp->trim(max_entries + 1);
    bp->add_entries(max_entries);
    return std::static_pointer_cast<SIProvider>(bp);
  });

  auto stage2 = make_shared<SIPShardedStage>(sis[1].shards);

  stages.push_back(stage1);
  stages.push_back(stage2);

  SIPMultiStageClient client(stages);

  ASSERT_EQ(0, client.init_markers());

  for (int stage_num = 0; stage_num < 2; ++stage_num) {
    auto stage = client.cur_stage_ref();
    auto& si = sis[stage_num];

    int total[si.num_shards];
    int chunk_size = 10;

    memset(total, 0, sizeof(total));

    int total_iter = 0;
    int all_count = 0;

    int fetched_now;

    do {

      fetched_now = 0;
      
      for (int i = 0; i < client.num_shards(); ++i) {

        if (stage->is_shard_done(i)) {
          continue;
        }
        SIProvider::fetch_result result;
        ASSERT_EQ(0, stage->fetch(i, chunk_size, &result));

        fetched_now = result.entries.size();

        total[i] += fetched_now;
        all_count += fetched_now;

        ASSERT_TRUE(handle_result(result, nullptr));
        ASSERT_NE((total[i] < si.max_entries[i]), !result.more);
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

