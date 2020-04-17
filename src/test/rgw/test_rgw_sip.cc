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

  SIProvider::Type get_type() const {
    return SIProvider::Type::FULL;
  }

  int fetch(std::string marker, int max, fetch_result *result) override {
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
      SIProvider::info e;

      BasicData d{gen_data(val)};
      d.encode(e.data);
      e.key = encode_marker(BasicMarker{val});

      result->entries.push_back(std::move(e));
    }

    result->more = (max_entries > 0 && val < max_entries);
    result->done = !result->more; /* simple provider, no intermediate state */

    return 0;
  }

  int get_start_marker(std::string *marker) const override {
    marker->clear();
    return 0;
  }

  int get_cur_state(std::string *marker) const override {
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

  SIProvider::Type get_type() const {
    return SIProvider::Type::INC;
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

  int fetch(std::string marker, int max, fetch_result *result) override {
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
      SIProvider::info e;

      BasicData d{gen_data(val)};
      d.encode(e.data);
      e.key = encode_marker(BasicMarker{val});

      result->entries.push_back(std::move(e));
    }

    result->more = (val <= max_val);
    result->done = (!result->more && done);

    return 0;
  }

  int get_start_marker(std::string *marker) const override {
    marker->clear();
    return 0;
  }

  int get_cur_state(std::string *marker) const override {
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

TEST(TestRGWSIP, test_basic_provider)
{
  int max_entries = 25;

  BasicProvider bp(0, max_entries);

  string marker;
  ASSERT_EQ(0, bp.get_start_marker(&marker));

  int total = 0;

  int chunk_size = 10;

  do {
    SIProvider::fetch_result result;
    ASSERT_EQ(0, bp.fetch(marker, chunk_size, &result));

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
  ASSERT_EQ(0, lp.get_start_marker(&marker));

  int total = 0;

  int chunk_size = 10;

  do {
    SIProvider::fetch_result result;
    ASSERT_EQ(0, lp.fetch(marker, chunk_size, &result));

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

  SIProviderClient pc(base);

  int total = 0;
  int chunk_size = 10;

  do {
    SIProvider::fetch_result result;
    ASSERT_EQ(0, pc.fetch(chunk_size, &result));

    total += result.entries.size();

    ASSERT_TRUE(handle_result(result, nullptr));
    ASSERT_TRUE((total < max_entries) != (result.done));
  } while (total < max_entries);

  ASSERT_TRUE(total == max_entries);
}

TEST(TestRGWSIP, test_sharded_stage)
{
  int num_shards = 20;
  int shard_entries_limit = 40;
  int max_entries[num_shards];

  vector<SIProviderClientRef> shards;

  int all_entries = 0;

  for (int i = 0; i < num_shards; ++i) {
    max_entries[i] = (i * 7) % (shard_entries_limit + 1);
    cout << "max_entries[" << i << "]=" << max_entries[i] << std::endl;
    all_entries += max_entries[i];

    auto bp = std::make_shared<BasicProvider>(i, max_entries[i]);
    auto base = std::static_pointer_cast<SIProvider>(bp);
    auto pc = std::make_shared<SIProviderClient>(base);

    shards.push_back(pc);
  }

  SIPShardedStage stage(shards);

  int total[num_shards];
  int chunk_size = 10;

  memset(total, 0, sizeof(total));

  int total_iter = 0;
  int all_count = 0;

  while (!stage.is_complete()) {
    for (int i = 0; i < num_shards; ++i) {
      if (stage.is_shard_done(i)) {
        continue;
      }
      SIProvider::fetch_result result;
      ASSERT_EQ(0, stage.fetch(i, chunk_size, &result));

      total[i] += result.entries.size();
      all_count += result.entries.size();

      ASSERT_TRUE(handle_result(result, nullptr));
      ASSERT_NE((total[i] < max_entries[i]), result.done);
    }

    ++total_iter;
    ASSERT_TRUE(total_iter < (num_shards * shard_entries_limit)); /* avoid infinite loop due to bug */
  }

  ASSERT_EQ(all_entries, all_count);
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

