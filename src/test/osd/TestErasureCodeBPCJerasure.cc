// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CERN/Sitzerland
 *
 * Author: Andreas Joachim Peters <andreas.joachim.peters@cern.ch> 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "global/global_init.h"
#include "osd/ErasureCodePluginJerasure/ErasureCodeJerasure.h"
#include "common/ceph_argparse.h"
#include "common/Clock.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

typedef struct stopwatch {
  utime_t start;
  utime_t stop;

  utime_t realtime() const {
    return stop - start;
  }
} stopwatch_t;

typedef std::map<std::string, stopwatch_t> timing_t;
typedef std::map<std::string, timing_t > timing_map_t;

timing_map_t timing;
unsigned object_size = 4 * 1024 * 1024ll;
unsigned loops = 1;
bool benchmark = false;

template <typename T>
class ErasureCodeTest_82 : public ::testing::Test {
public:
};

template <typename T>
class ErasureCodeTest_BPC_822_Double_Failure : public ::testing::Test {
public:
};

template <typename T>
class ErasureCodeTest_BPC_822_Triple_Failure : public ::testing::Test {
public:
};

template <typename T>
class ErasureCodeTest_BPC_822_CRC32C : public ::testing::Test {
public:
};


typedef ::testing::Types<
ErasureCodeJerasureCauchyGood,
ErasureCodeJerasureLiber8tion
> JerasureTypes;
TYPED_TEST_CASE (ErasureCodeTest_82, JerasureTypes);
TYPED_TEST_CASE (ErasureCodeTest_BPC_822_Double_Failure, JerasureTypes);
TYPED_TEST_CASE (ErasureCodeTest_BPC_822_Triple_Failure, JerasureTypes);
TYPED_TEST_CASE (ErasureCodeTest_BPC_822_CRC32C, JerasureTypes);

TYPED_TEST (ErasureCodeTest_82, encode_decode) {
  TypeParam jerasure;
  map<std::string, std::string> parameters;
  parameters["erasure-code-k"] = "8";
  parameters["erasure-code-m"] = "2";
  parameters["erasure-code-w"] = "8";

  parameters["erasure-code-packetsize"] = "4096";
  jerasure.init(parameters);

  unsigned large_enough = (4 * object_size);

  bufferptr in_ptr(large_enough);
  in_ptr.zero();
  in_ptr.set_length(0);
  for (size_t i = 0; i < object_size; i++) {
    char c = random();
    in_ptr.append(&c, 1);
  }

  bufferlist in;
  in.push_front(in_ptr);
  int want_to_encode[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  map<int, bufferlist> encoded;

  timing[jerasure.technique]["encode"].start = ceph_clock_now(0);
  
  for (size_t i=0; i< loops; i++) 
  EXPECT_EQ(0, jerasure.encode(set<int>(want_to_encode, want_to_encode + 10),
                               in,
                               &encoded));

  timing[jerasure.technique]["encode"].stop = ceph_clock_now(0);
  EXPECT_EQ(10u, encoded.size());
  unsigned length = encoded[0].length();
  EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), length));
  EXPECT_EQ(0, memcmp(encoded[1].c_str(), in.c_str() + length, encoded[1].length()));


  // all chunks are available
  {
    int want_to_decode[] = {0, 1};
    map<int, bufferlist> decoded;
    for (size_t i=0; i< loops; i++)  
      EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode + 2),
				   encoded,
				   &decoded));

    
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(10u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[1].c_str(), in.c_str() + length, decoded[1].length()));
  }

  // two chunks are missing 
  {
    map<int, bufferlist> degraded = encoded;
    degraded.erase(0);
    degraded.erase(1);
    EXPECT_EQ(8u, degraded.size());
    int want_to_decode[] = {0, 1};
    map<int, bufferlist> decoded;
    timing[jerasure.technique]["reco"].start = ceph_clock_now(0);
    for (size_t i=0; i< loops; i++) 
      EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode + 2),
				   degraded,
				   &decoded));
    timing[jerasure.technique]["reco"].stop = ceph_clock_now(0);
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(10u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[1].c_str(), in.c_str() + length, decoded[1].length()));
  }
}

TYPED_TEST (ErasureCodeTest_BPC_822_Double_Failure, encode_decode) {
  TypeParam jerasure;
  map<std::string, std::string> parameters;
  parameters["erasure-code-k"] = "8";
  parameters["erasure-code-m"] = "2";
  parameters["erasure-code-lp"] = "2";

  parameters["erasure-code-w"] = "8";

  parameters["erasure-code-packetsize"] = "4096";
  jerasure.init(parameters);

  unsigned large_enough = (4 * object_size);

  bufferptr in_ptr(large_enough);
  in_ptr.zero();
  in_ptr.set_length(0);
  for (size_t i = 0; i < object_size; i++) {
    char c = random();
    in_ptr.append(&c, 1);
  }

  bufferlist in;
  in.push_front(in_ptr);
  int want_to_encode[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
  map<int, bufferlist> encoded;

  timing[jerasure.technique]["encode-bpc"].start = ceph_clock_now(0);
  for (size_t i=0; i< loops; i++) 
  EXPECT_EQ(0, jerasure.encode(set<int>(want_to_encode, want_to_encode + 12),
                               in,
                               &encoded));

  timing[jerasure.technique]["encode-bpc"].stop = ceph_clock_now(0);
  EXPECT_EQ(12u, encoded.size());
  unsigned length = encoded[0].length();
  EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), length));
  EXPECT_EQ(0, memcmp(encoded[1].c_str(), in.c_str() + length, encoded[1].length()));


  // all chunks are available
  {
    int want_to_decode[] = {0, 1};
    map<int, bufferlist> decoded;
    for (size_t i=0; i< loops; i++) 
      EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode + 2),
				   encoded,
				   &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(12u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[1].c_str(), in.c_str() + length, decoded[1].length()));
  }

  // two chunks are missing 
  {
    map<int, bufferlist> degraded = encoded;
    degraded.erase(0);
    degraded.erase(5);
    EXPECT_EQ(10u, degraded.size());
    int want_to_decode[] = {0, 5};
    map<int, bufferlist> decoded;
    timing[jerasure.technique]["reco-bpc"].start = ceph_clock_now(0);
    for (size_t i=0; i< loops; i++) 
      EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode + 2),
				   degraded,
				   &decoded));
    timing[jerasure.technique]["reco-bpc"].stop = ceph_clock_now(0);
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(12u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(length, decoded[1].length());
    EXPECT_EQ(length, decoded[2].length());
    EXPECT_EQ(length, decoded[3].length());
    EXPECT_EQ(length, decoded[4].length());
    EXPECT_EQ(length, decoded[5].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[5].c_str(), in.c_str() + 5 * length, decoded[5].length()));
  }
}

TYPED_TEST (ErasureCodeTest_BPC_822_Triple_Failure, encode_decode) {
  TypeParam jerasure;
  map<std::string, std::string> parameters;
  parameters["erasure-code-k"] = "8";
  parameters["erasure-code-m"] = "2";
  parameters["erasure-code-lp"] = "2";

  parameters["erasure-code-w"] = "8";

  parameters["erasure-code-packetsize"] = "4096";
  jerasure.init(parameters);

  unsigned large_enough = (4 * object_size);

  bufferptr in_ptr(large_enough);
  in_ptr.zero();
  in_ptr.set_length(0);
  for (size_t i = 0; i < object_size; i++) {
    char c = random();
    in_ptr.append(&c, 1);
  }

  bufferlist in;
  in.push_front(in_ptr);
  int want_to_encode[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
  map<int, bufferlist> encoded;

  timing[jerasure.technique]["encode-bpc-triple"].start = ceph_clock_now(0);
  for (size_t i=0; i< loops; i++) 
  EXPECT_EQ(0, jerasure.encode(set<int>(want_to_encode, want_to_encode + 12),
                               in,
                               &encoded));

  timing[jerasure.technique]["encode-bpc-triple"].stop = ceph_clock_now(0);
  EXPECT_EQ(12u, encoded.size());
  unsigned length = encoded[0].length();
  EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), length));
  EXPECT_EQ(0, memcmp(encoded[5].c_str(), in.c_str() + 5 * length, encoded[5].length()));


  // all chunks are available
  {
    int want_to_decode[] = {0, 5};
    map<int, bufferlist> decoded;

    EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode + 2),
                                 encoded,
                                 &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(12u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[5].c_str(), in.c_str() + 5 * length, decoded[5].length()));
  }

  // three chunks are missing 
  {
    map<int, bufferlist> degraded = encoded;
    degraded.erase(0); //=> use local parity
    degraded.erase(5); //=> erasure decoding
    degraded.erase(6); //=> erasure decoding
    EXPECT_EQ(9u, degraded.size());
    int want_to_decode[] = {0, 5, 6};
    map<int, bufferlist> decoded;
    timing[jerasure.technique]["reco-bpc-triple"].start = ceph_clock_now(0);
    for (size_t i=0; i< loops; i++) 
      EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode + 3),
				   degraded,
				   &decoded));
    timing[jerasure.technique]["reco-bpc-triple"].stop = ceph_clock_now(0);
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(12u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[5].c_str(), in.c_str() + 5 * length, decoded[5].length()));
    EXPECT_EQ(0, memcmp(decoded[6].c_str(), in.c_str() + 6 * length, decoded[6].length()));
  }

  // check minimum-to-decode functionality 
  {
    int want_to_decode[] = {0, 5, 6};
    map<int, bufferlist> degraded = encoded;
    degraded.erase(0); //=> use local parity
    {
      std::set<int> needed;
      std::set<int> available;
      for (std::map<int, bufferlist>::iterator it = degraded.begin(); it != degraded.end(); ++it)
        available.insert(it->first);
      int ret = jerasure.minimum_to_decode(set<int>(want_to_decode, want_to_decode + 3),
                                           available,
                                           &needed);
      std::ostringstream chunkstream;
      for (std::set<int>::iterator i = needed.begin(); i != needed.end(); i++) {
        if (i != needed.begin())
          chunkstream << ":";
        chunkstream << *i;
      }

      EXPECT_EQ(0, strcmp("1:2:3:5:6:10", chunkstream.str().c_str()));
      EXPECT_EQ(0, ret);
    }

    degraded = encoded;
    degraded.erase(5); //=> erasure decoding
    degraded.erase(6); //=> erasure decoding

    {
      std::set<int> needed;
      std::set<int> available;
      for (std::map<int, bufferlist>::iterator it = degraded.begin(); it != degraded.end(); ++it)
        available.insert(it->first);
      int ret = jerasure.minimum_to_decode(set<int>(want_to_decode, want_to_decode + 3),
                                           available,
                                           &needed);
      std::ostringstream chunkstream;
      for (std::set<int>::iterator i = needed.begin(); i != needed.end(); i++) {
        if (i != needed.begin())
          chunkstream << ":";
        chunkstream << *i;
      }

      EXPECT_EQ(0, strcmp("0:1:2:3:4:7:8:9", chunkstream.str().c_str()));
      EXPECT_EQ(0, ret);
    }

    degraded.erase(0); //=> add's local parity to the previous case
    {
      std::set<int> needed;
      std::set<int> available;
      for (std::map<int, bufferlist>::iterator it = degraded.begin(); it != degraded.end(); ++it)
        available.insert(it->first);
      int ret = jerasure.minimum_to_decode(set<int>(want_to_decode, want_to_decode + 3),
                                           available,
                                           &needed);
      std::ostringstream chunkstream;
      for (std::set<int>::iterator i = needed.begin(); i != needed.end(); i++) {
        if (i != needed.begin())
          chunkstream << ":";
        chunkstream << *i;
      }

      EXPECT_EQ(0, strcmp("1:2:3:4:7:8:9:10", chunkstream.str().c_str()));
      EXPECT_EQ(0, ret);
    }

    degraded.erase(1); //=> make's decoding impossible
    {
      std::set<int> needed;
      std::set<int> available;
      for (std::map<int, bufferlist>::iterator it = degraded.begin(); it != degraded.end(); ++it)
        available.insert(it->first);
      int ret = jerasure.minimum_to_decode(set<int>(want_to_decode, want_to_decode + 3),
                                           available,
                                           &needed);
      EXPECT_EQ(-5, ret);
    }
  }

  // four chunks are missing 
  {
    map<int, bufferlist> degraded = encoded;
    degraded.erase(0);
    degraded.erase(5);
    degraded.erase(6);
    degraded.erase(7);
    EXPECT_EQ(8u, degraded.size());
    int want_to_decode[] = {0, 5, 6, 7};
    map<int, bufferlist> decoded;
    EXPECT_EQ(-1, jerasure.decode(set<int>(want_to_decode, want_to_decode + 4),
                                  degraded,
                                  &decoded));
  }
}

TYPED_TEST (ErasureCodeTest_BPC_822_CRC32C, encode_decode) {
  TypeParam jerasure;
  map<std::string, std::string> parameters;
  parameters["erasure-code-k"] = "8";
  parameters["erasure-code-m"] = "2";
  parameters["erasure-code-lp"] = "2";
  parameters["erasure-code-w"] = "8";

  parameters["erasure-code-packetsize"] = "4096";
  jerasure.init(parameters);

  unsigned large_enough = (4 * object_size);

  bufferptr in_ptr(large_enough);
  in_ptr.zero();
  in_ptr.set_length(0);
  for (size_t i = 0; i < object_size; i++) {
    char c = random();
    in_ptr.append(&c, 1);
  }

  bufferlist in;
  in.push_front(in_ptr);
  int want_to_encode[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
  map<int, bufferlist> encoded;

  timing[jerasure.technique]["encode-bpc-crc32c"].start = ceph_clock_now(0);
  for (size_t i=0; i< loops; i++) {
    EXPECT_EQ(0, jerasure.encode(set<int>(want_to_encode, want_to_encode + 12),
				 in,
				 &encoded));

    for (map<int, bufferlist>::iterator it = encoded.begin(); it != encoded.end(); it++) {
      buffer::hash crc;
      crc.update(it->second);
    }
  }

  timing[jerasure.technique]["encode-bpc-crc32c"].stop = ceph_clock_now(0);
  EXPECT_EQ(12u, encoded.size());
  unsigned length = encoded[0].length();
  EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), length));
  EXPECT_EQ(0, memcmp(encoded[1].c_str(), in.c_str() + length, encoded[1].length()));


  // always decode all, regardless of want_to_decode
  // all chunks are available
  {
    int want_to_decode[] = {0, 1};
    map<int, bufferlist> decoded;
    for (size_t i=0; i< loops; i++) 
      EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode + 2),
				   encoded,
				   &decoded));

    // always decode all, regardless of want_to_decode
    EXPECT_EQ(12u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[1].c_str(), in.c_str() + length, decoded[1].length()));
  }

  // two chunks are missing 
  {
    map<int, bufferlist> degraded = encoded;
    degraded.erase(0);
    degraded.erase(5);
    EXPECT_EQ(10u, degraded.size());
    int want_to_decode[] = {0, 5};
    map<int, bufferlist> decoded;
    timing[jerasure.technique]["reco-bpc-crc32c"].start = ceph_clock_now(0);
    for (size_t i=0; i< loops; i++) {
      EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode + 2),
				   degraded,
				   &decoded));

      for (map<int, bufferlist>::iterator it = decoded.begin(); it != decoded.end(); it++) {
	buffer::hash crc;
	crc.update(it->second);
      }
    }

    timing[jerasure.technique]["reco-bpc-crc32c"].stop = ceph_clock_now(0);
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(12u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[5].c_str(), in.c_str() + 5 * length, decoded[5].length()));
  }
}

class ErasureCodeTiming : public ::testing::Test {
public:
};

TEST_F (ErasureCodeTiming, PropertyOutput) {
  for (timing_map_t::const_iterator techniqueit = timing.begin(); techniqueit != timing.end(); ++techniqueit) {
    for (timing_t::const_iterator modeit = techniqueit->second.begin(); modeit != techniqueit->second.end(); ++modeit) {
      char timingout[4096];
      double speed = loops * object_size / 1000000l / ((double) modeit->second.realtime()) / 1000.0;
      snprintf(timingout,
               sizeof (timingout) - 1,
               "[ -TIMING- ] technique=%-16s [ %18s ] speed=%02.03f [GB/s] latency=%02.03f ms\n",
               techniqueit->first.c_str(),
               modeit->first.c_str(),
               speed,
               object_size / (1000000 * speed)
               );

      if (benchmark)
	cout << timingout;

      std::string property = std::string("jerasure::") + techniqueit->first.c_str() + "::" + modeit->first.c_str();
      RecordProperty((property + "::speed::mb").c_str(), speed * 1000);
      RecordProperty((property + "::latency::mus").c_str(), object_size / (1000 * speed));
    }
  }
  RecordProperty("object-size", object_size);
  RecordProperty("iterations", loops);
}

int
main (int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **) argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);

  for (int i = 0; i < argc; i++) {
    std::string arg = argv[i];
    if (arg.substr(0, 14) == "--object-size=") {
      arg.erase(0, 14);
      object_size = atoi(arg.c_str())*1024 * 1024ll;
    }

    if (arg.substr(0, 13) == "--iterations=") {
      arg.erase(0, 13);
      loops = atoi(arg.c_str());
    }
    
    if (arg.substr(0,7) == "--bench") {
      arg.erase(0, 7);
      benchmark = true;
    }

    if (!object_size || (object_size > 2147483648ll)) {
      fprintf(stderr, "error: --object-size=MB ==> ( 0 < MB <= 2000 )\n");
      exit(EINVAL);
    }
  }
  return RUN_ALL_TESTS();
}

// Local Variables:
// compile-command: "cd ../.. ; make -j4 && make unittest_erasure_code_bpc_jerasure && valgrind --tool=memcheck ./unittest_erasure_code_bpc_jerasure --gtest_filter=*.* --log-to-stderr=true --debug-osd=20 [--object-size=4] [--iterations=100] [--bench]"
// End:
