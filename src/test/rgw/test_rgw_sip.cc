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

class BasicProvider : public SIProvider
{
  uint32_t max_entries;

  static string gen_data(uint32_t val) {
    stringstream ss;
    ss << "basic test data -- " << val;
    return ss.str();
  }

public: 
  BasicProvider(uint32_t _max_entries) : max_entries(_max_entries) {}

  struct Marker {
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

  struct Data {
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

  SIProvider::Type get_type() const {
    return SIProvider::Type::FULL;
  }

  int fetch(std::string marker, int max, fetch_result *result) override {
    Marker pos;

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

      Data d{gen_data(val)};
      d.encode(e.data);
      e.key = encode_marker(Marker{val});

      result->entries.push_back(std::move(e));
    }

    result->more = (max_entries > 0 && val < max_entries - 1);
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
WRITE_CLASS_ENCODER(BasicProvider::Marker)
WRITE_CLASS_ENCODER(BasicProvider::Data)

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

  struct Marker {
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

  struct Data {
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


  int fetch(std::string marker, int max, fetch_result *result) override {
    Marker pos;

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

      Data d{gen_data(val)};
      d.encode(e.data);
      e.key = encode_marker(Marker{val});

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
WRITE_CLASS_ENCODER(LogProvider::Marker)
WRITE_CLASS_ENCODER(LogProvider::Data)



TEST(TestRGWSIP, test_basic_provider)
{
  int max_entries = 25;

  BasicProvider bp(max_entries);

  string marker;
  ASSERT_EQ(0, bp.get_start_marker(&marker));

  int total = 0;

  int chunk_size = 10;

  do {
    SIProvider::fetch_result result;
    ASSERT_EQ(0, bp.fetch(marker, chunk_size, &result));

    total += result.entries.size();

    for (auto& e : result.entries) {
      marker = e.key;

      BasicProvider::Data d;
      try {
        decode(d, e.data);
      } catch (buffer::error& err) {
        cerr << "ERROR: failed to decode data" << std::endl;
        ASSERT_TRUE(false);
      }
      cout << "entry: " << d.val << std::endl;
    }

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

    for (auto& e : result.entries) {
      marker = e.key;

      LogProvider::Data d;
      try {
        decode(d, e.data);
      } catch (buffer::error& err) {
        cerr << "ERROR: failed to decode data" << std::endl;
        ASSERT_TRUE(false);
      }
      cout << "entry: " << d.val << std::endl;
    }
    cout << "result.more=" << result.more << " total=" << total << std::endl;
    ASSERT_TRUE((total < max_entries) == (result.more));

  } while (total < max_entries);

  ASSERT_TRUE(total == max_entries);
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

