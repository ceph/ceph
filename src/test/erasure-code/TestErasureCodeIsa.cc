// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <errno.h>
#include <stdlib.h>

#include "crush/CrushWrapper.h"
#include "include/stringify.h"
#include "erasure-code/isa/ErasureCodeIsa.h"
#include "erasure-code/isa/xor_op.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"
#include "gtest/gtest.h"
#include "test/unit.h"

ErasureCodeIsaTableCache tcache;

class IsaErasureCodeTest : public ::testing::Test {
public:
  void compare_chunks(bufferlist &in, map<int, bufferlist> &encoded);
  void encode_decode(unsigned object_size); 
};

void IsaErasureCodeTest::compare_chunks(bufferlist &in, map<int, bufferlist> &encoded)
{
  unsigned object_size = in.length();
  unsigned chunk_size = encoded[0].length();
  for (unsigned i = 0; i < encoded.size(); i++) {
    if (i * chunk_size >= object_size)
      break;
    int chunk_length = object_size > (i + 1) * chunk_size ? chunk_size : object_size - i * chunk_size;
    EXPECT_EQ(0, memcmp(encoded[i].c_str(), in.c_str() + i * chunk_size, chunk_length));
  }
}

void IsaErasureCodeTest::encode_decode(unsigned object_size)
{
  ErasureCodeIsaDefault Isa(tcache);

  ErasureCodeProfile profile;
  profile["k"] = "2";
  profile["m"] = "2";
  Isa.init(profile, &cerr);

  string payload(object_size, 'X');
  bufferlist in;
  // may be multiple bufferptr if object_size is larger than CEPH_PAGE_SIZE
  in.append(payload.c_str(), payload.length());
  int want_to_encode[] = {0, 1, 2, 3};
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, Isa.encode(set<int>(want_to_encode, want_to_encode + 4),
                          in,
                          &encoded));
  EXPECT_EQ(4u, encoded.size());
  unsigned chunk_size = encoded[0].length();
  EXPECT_EQ(chunk_size, Isa.get_chunk_size(object_size));
  compare_chunks(in, encoded);

  // all chunks are available
  {
    int want_to_decode[] = {0, 1};
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, Isa.decode(set<int>(want_to_decode, want_to_decode + 2),
                            encoded,
                            &decoded));
    EXPECT_EQ(2u, decoded.size());
    EXPECT_EQ(chunk_size, decoded[0].length());
    compare_chunks(in, decoded);
  }

  // one data chunk is missing
  {
    map<int, bufferlist> degraded = encoded;

    string enc1(encoded[1].c_str(), chunk_size);

    degraded.erase(1);
    EXPECT_EQ(3u, degraded.size());
    int want_to_decode[] = {1};
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, Isa.decode(set<int>(want_to_decode, want_to_decode + 1),
                            degraded,
                            &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(4u, decoded.size());
    EXPECT_EQ(chunk_size, decoded[1].length());
    EXPECT_EQ(0, memcmp(decoded[1].c_str(), enc1.c_str(), chunk_size));
  }

  // non-xor coding chunk is missing
  {
    map<int, bufferlist> degraded = encoded;

    string enc3(encoded[3].c_str(), chunk_size);

    degraded.erase(3);
    EXPECT_EQ(3u, degraded.size());
    int want_to_decode[] = {3};
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, Isa.decode(set<int>(want_to_decode, want_to_decode + 1),
                            degraded,
                            &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(4u, decoded.size());
    EXPECT_EQ(chunk_size, decoded[3].length());
    EXPECT_EQ(0, memcmp(decoded[3].c_str(), enc3.c_str(), chunk_size));
  }

  // xor coding chunk is missing
  {
    map<int, bufferlist> degraded = encoded;

    string enc2(encoded[2].c_str(), chunk_size);

    degraded.erase(2);
    EXPECT_EQ(3u, degraded.size());
    int want_to_decode[] = {2};
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, Isa.decode(set<int>(want_to_decode, want_to_decode + 1),
                            degraded,
                            &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(4u, decoded.size());
    EXPECT_EQ(chunk_size, decoded[2].length());
    EXPECT_EQ(0, memcmp(decoded[2].c_str(), enc2.c_str(), chunk_size));
  }

  // one data and one coding chunk is missing
  {
    map<int, bufferlist> degraded = encoded;

    string enc3(encoded[3].c_str(), chunk_size);

    degraded.erase(1);
    degraded.erase(3);
    EXPECT_EQ(2u, degraded.size());
    int want_to_decode[] = {1, 3};
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, Isa.decode(set<int>(want_to_decode, want_to_decode + 2),
                            degraded,
                            &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(4u, decoded.size());
    EXPECT_EQ(chunk_size, decoded[1].length());
    EXPECT_EQ(0, memcmp(decoded[3].c_str(), enc3.c_str(), chunk_size));
  }

  // two data chunks are missing
  {
    map<int, bufferlist> degraded = encoded;
    degraded.erase(0);
    degraded.erase(1);
    EXPECT_EQ(2u, degraded.size());
    int want_to_decode[] = {0, 1};
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, Isa.decode(set<int>(want_to_decode, want_to_decode + 2),
                            degraded,
                            &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(4u, decoded.size());
    EXPECT_EQ(chunk_size, decoded[0].length());
    compare_chunks(in, decoded);
  }

}

TEST_F(IsaErasureCodeTest, encode_decode)
{
  encode_decode(1);
  encode_decode(EC_ISA_ADDRESS_ALIGNMENT);
  encode_decode(EC_ISA_ADDRESS_ALIGNMENT + 1);
  encode_decode(2048);
  encode_decode(4096);
  encode_decode(4096 + 1);
}

TEST_F(IsaErasureCodeTest, minimum_to_decode)
{
  ErasureCodeIsaDefault Isa(tcache);
  ErasureCodeProfile profile;
  profile["k"] = "2";
  profile["m"] = "2";
  Isa.init(profile, &cerr);

  //
  // If trying to read nothing, the minimum is empty.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    EXPECT_EQ(0, Isa.minimum_to_decode(want_to_read,
                                       available_chunks,
                                       &minimum));
    EXPECT_TRUE(minimum.empty());
  }
  //
  // There is no way to read a chunk if none are available.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);

    EXPECT_EQ(-EIO, Isa.minimum_to_decode(want_to_read,
                                          available_chunks,
                                          &minimum));
  }
  //
  // Reading a subset of the available chunks is always possible.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);
    available_chunks.insert(0);

    EXPECT_EQ(0, Isa.minimum_to_decode(want_to_read,
                                       available_chunks,
                                       &minimum));
    EXPECT_EQ(want_to_read, minimum);
  }
  //
  // There is no way to read a missing chunk if there is less than k
  // chunks available.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);
    want_to_read.insert(1);
    available_chunks.insert(0);

    EXPECT_EQ(-EIO, Isa.minimum_to_decode(want_to_read,
                                          available_chunks,
                                          &minimum));
  }
  //
  // When chunks are not available, the minimum can be made of any
  // chunks. For instance, to read 1 and 3 below the minimum could be
  // 2 and 3 which may seem better because it contains one of the
  // chunks to be read. But it won't be more efficient than retrieving
  // 0 and 2 instead because, in both cases, the decode function will
  // need to run the same recovery operation and use the same amount
  // of CPU and memory.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(1);
    want_to_read.insert(3);
    available_chunks.insert(0);
    available_chunks.insert(2);
    available_chunks.insert(3);

    EXPECT_EQ(0, Isa.minimum_to_decode(want_to_read,
                                       available_chunks,
                                       &minimum));
    EXPECT_EQ(2u, minimum.size());
    EXPECT_EQ(0u, minimum.count(3));
  }
}

TEST_F(IsaErasureCodeTest, chunk_size)
{
  {
    ErasureCodeIsaDefault Isa(tcache);
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "1";
    Isa.init(profile, &cerr);
    int k = 2;

    ASSERT_EQ(EC_ISA_ADDRESS_ALIGNMENT, Isa.get_chunk_size(1));
    ASSERT_EQ(EC_ISA_ADDRESS_ALIGNMENT, Isa.get_chunk_size(EC_ISA_ADDRESS_ALIGNMENT * k - 1));
    ASSERT_EQ(EC_ISA_ADDRESS_ALIGNMENT * 2, Isa.get_chunk_size(EC_ISA_ADDRESS_ALIGNMENT * k + 1));
  }
  {
    ErasureCodeIsaDefault Isa(tcache);
    ErasureCodeProfile profile;
    profile["k"] = "3";
    profile["m"] = "1";
    Isa.init(profile, &cerr);
    int k = 3;

    ASSERT_EQ(EC_ISA_ADDRESS_ALIGNMENT, Isa.get_chunk_size(1));
    ASSERT_EQ(EC_ISA_ADDRESS_ALIGNMENT, Isa.get_chunk_size(EC_ISA_ADDRESS_ALIGNMENT * k - 1));
    ASSERT_EQ(EC_ISA_ADDRESS_ALIGNMENT * 2, Isa.get_chunk_size(EC_ISA_ADDRESS_ALIGNMENT * k + 1));
    unsigned object_size = EC_ISA_ADDRESS_ALIGNMENT * k * 1024 + 1;
    ASSERT_NE(0u, object_size % k);
    ASSERT_NE(0u, object_size % EC_ISA_ADDRESS_ALIGNMENT);
    unsigned chunk_size = Isa.get_chunk_size(object_size);
    ASSERT_EQ(0u, chunk_size % EC_ISA_ADDRESS_ALIGNMENT);
    ASSERT_GT(chunk_size, (chunk_size * k) - object_size);
  }
}

TEST_F(IsaErasureCodeTest, encode)
{
  ErasureCodeIsaDefault Isa(tcache);
  ErasureCodeProfile profile;
  profile["k"] = "2";
  profile["m"] = "2";
  Isa.init(profile, &cerr);

  unsigned aligned_object_size = Isa.get_alignment() * 2;
  {
    //
    // When the input bufferlist needs to be padded because
    // it is not properly aligned, it is padded with zeros.
    //
    bufferlist in;
    map<int,bufferlist> encoded;
    int want_to_encode[] = { 0, 1, 2, 3 };
    int trail_length = 1;
    in.append(string(aligned_object_size + trail_length, 'X'));
    EXPECT_EQ(0, Isa.encode(set<int>(want_to_encode, want_to_encode+4),
				 in,
				 &encoded));
    EXPECT_EQ(4u, encoded.size());
    char *last_chunk = encoded[1].c_str();
    int length =encoded[1].length();
    EXPECT_EQ('X', last_chunk[0]);
    EXPECT_EQ('\0', last_chunk[length - trail_length]);
  }

  {
    //
    // When only the first chunk is required, the encoded map only
    // contains the first chunk. Although the Isa encode
    // internally allocated a buffer because of padding requirements
    // and also computes the coding chunks, they are released before
    // the return of the method, as shown when running the tests thru
    // valgrind (there is no leak).
    //
    bufferlist in;
    map<int,bufferlist> encoded;
    set<int> want_to_encode;
    want_to_encode.insert(0);
    int trail_length = 1;
    in.append(string(aligned_object_size + trail_length, 'X'));
    EXPECT_EQ(0, Isa.encode(want_to_encode, in, &encoded));
    EXPECT_EQ(1u, encoded.size());
  }
}

TEST_F(IsaErasureCodeTest, sanity_check_k)
{
  ErasureCodeIsaDefault Isa(tcache);
  ErasureCodeProfile profile;
  profile["k"] = "1";
  profile["m"] = "1";
  ostringstream errors;
  EXPECT_EQ(-EINVAL, Isa.init(profile, &errors));
  EXPECT_NE(std::string::npos, errors.str().find("must be >= 2"));
}

bool
DecodeAndVerify(ErasureCodeIsaDefault& Isa, map<int, bufferlist> &degraded, set<int> want_to_decode, buffer::ptr* enc, int length)
{
  map<int, bufferlist> decoded;
  bool ok;

  // decode as requested
  ok = Isa.decode(want_to_decode,
                  degraded,
                  &decoded);

  for (int i = 0; i < (int) decoded.size(); i++) {
    // compare all the buffers with their original
    ok |= memcmp(decoded[i].c_str(), enc[i].c_str(), length);
  }

  return ok;
}

TEST_F(IsaErasureCodeTest, isa_vandermonde_exhaustive)
{
  // Test all possible failure scenarios and reconstruction cases for
  // a (12,4) configuration using the vandermonde matrix

  ErasureCodeIsaDefault Isa(tcache);
  ErasureCodeProfile profile;
  profile["k"] = "12";
  profile["m"] = "4";
  Isa.init(profile, &cerr);

  const int k = 12;
  const int m = 4;

#define LARGE_ENOUGH 2048
  bufferptr in_ptr(buffer::create_page_aligned(LARGE_ENOUGH));
  in_ptr.zero();
  in_ptr.set_length(0);
  const char *payload =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  in_ptr.append(payload, strlen(payload));
  bufferlist in;
  in.push_front(in_ptr);

  set<int>want_to_encode;

  map<int, bufferlist> encoded;
  for (int i = 0; i < (k + m); i++) {
    want_to_encode.insert(i);
  }


  EXPECT_EQ(0, Isa.encode(want_to_encode,
                          in,
                          &encoded));

  EXPECT_EQ((unsigned) (k + m), encoded.size());

  unsigned length = encoded[0].length();

  for (int i = 0; i < k; i++) {
    EXPECT_EQ(0, memcmp(encoded[i].c_str(), in.c_str() + (i * length), length));
  }

  buffer::ptr enc[k + m];
  // create buffers with a copy of the original data to be able to compare it after decoding
  {
    for (int i = 0; i < (k + m); i++) {
      buffer::ptr newenc(buffer::create_page_aligned(LARGE_ENOUGH));
      enc[i] = newenc;
      enc[i].zero();
      enc[i].set_length(0);
      enc[i].append(encoded[i].c_str(), length);
    }
  }

  // loop through all possible loss scenarios
  int cnt_cf = 0;

  for (int l1 = 0; l1 < (k + m); l1++) {
    map<int, bufferlist> degraded = encoded;
    set<int> want_to_decode;
    bool err;
    degraded.erase(l1);
    want_to_decode.insert(l1);
    err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
    EXPECT_EQ(0, err);
    cnt_cf++;
    for (int l2 = l1 + 1; l2 < (k + m); l2++) {
      degraded.erase(l2);
      want_to_decode.insert(l2);
      err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
      EXPECT_EQ(0, err);
      cnt_cf++;
      for (int l3 = l2 + 1; l3 < (k + m); l3++) {
        degraded.erase(l3);
        want_to_decode.insert(l3);
        err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
        EXPECT_EQ(0, err);
        cnt_cf++;
        for (int l4 = l3 + 1; l4 < (k + m); l4++) {
          degraded.erase(l4);
          want_to_decode.insert(l4);
          err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
          EXPECT_EQ(0, err);
          degraded[l4] = encoded[l4];
          want_to_decode.erase(l4);
          cnt_cf++;
        }
        degraded[l3] = encoded[l3];
        want_to_decode.erase(l3);
      }
      degraded[l2] = encoded[l2];
      want_to_decode.erase(l2);
    }
    degraded[l1] = encoded[l1];
    want_to_decode.erase(l1);
  }
  EXPECT_EQ(2516, cnt_cf);
  EXPECT_EQ(2506, tcache.getDecodingTableCacheSize()); // 3 entries from (2,2) test and 2503 from (12,4)
}

TEST_F(IsaErasureCodeTest, isa_cauchy_exhaustive)
{
  // Test all possible failure scenarios and reconstruction cases for
  // a (12,4) configuration using the cauchy matrix
  ErasureCodeIsaDefault Isa(tcache,ErasureCodeIsaDefault::kCauchy);
  ErasureCodeProfile profile;
  profile["k"] = "12";
  profile["m"] = "4";
  profile["technique"] = "cauchy";

  Isa.init(profile, &cerr);

  int k = 12;
  int m = 4;

#define LARGE_ENOUGH 2048
  bufferptr in_ptr(buffer::create_page_aligned(LARGE_ENOUGH));
  in_ptr.zero();
  in_ptr.set_length(0);
  const char *payload =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  in_ptr.append(payload, strlen(payload));
  bufferlist in;
  in.push_front(in_ptr);

  set<int>want_to_encode;

  map<int, bufferlist> encoded;
  for (int i = 0; i < (k + m); i++) {
    want_to_encode.insert(i);
  }


  EXPECT_EQ(0, Isa.encode(want_to_encode,
                          in,
                          &encoded));

  EXPECT_EQ((unsigned) (k + m), encoded.size());

  unsigned length = encoded[0].length();

  for (int i = 0; i < k; i++) {
    EXPECT_EQ(0, memcmp(encoded[i].c_str(), in.c_str() + (i * length), length));
  }

  buffer::ptr enc[k + m];
  // create buffers with a copy of the original data to be able to compare it after decoding
  {
    for (int i = 0; i < (k + m); i++) {
      buffer::ptr newenc(buffer::create_page_aligned(LARGE_ENOUGH));
      enc[i] = newenc;
      enc[i].zero();
      enc[i].set_length(0);
      enc[i].append(encoded[i].c_str(), length);
    }
  }

  // loop through all possible loss scenarios
  int cnt_cf = 0;

  for (int l1 = 0; l1 < (k + m); l1++) {
    map<int, bufferlist> degraded = encoded;
    set<int> want_to_decode;
    bool err;
    degraded.erase(l1);
    want_to_decode.insert(l1);
    err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
    EXPECT_EQ(0, err);
    cnt_cf++;
    for (int l2 = l1 + 1; l2 < (k + m); l2++) {
      degraded.erase(l2);
      want_to_decode.insert(l2);
      err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
      EXPECT_EQ(0, err);
      cnt_cf++;
      for (int l3 = l2 + 1; l3 < (k + m); l3++) {
        degraded.erase(l3);
        want_to_decode.insert(l3);
        err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
        EXPECT_EQ(0, err);
        cnt_cf++;
        for (int l4 = l3 + 1; l4 < (k + m); l4++) {
          degraded.erase(l4);
          want_to_decode.insert(l4);
          err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
          EXPECT_EQ(0, err);
          degraded[l4] = encoded[l4];
          want_to_decode.erase(l4);
          cnt_cf++;
        }
        degraded[l3] = encoded[l3];
        want_to_decode.erase(l3);
      }
      degraded[l2] = encoded[l2];
      want_to_decode.erase(l2);
    }
    degraded[l1] = encoded[l1];
    want_to_decode.erase(l1);
  }
  EXPECT_EQ(2516, cnt_cf);
  EXPECT_EQ(2516, tcache.getDecodingTableCacheSize(ErasureCodeIsaDefault::kCauchy));
}

TEST_F(IsaErasureCodeTest, isa_cauchy_cache_trash)
{
  // Test all possible failure scenarios and reconstruction cases for
  // a (12,4) configuration using the cauchy matrix
  ErasureCodeIsaDefault Isa(tcache,ErasureCodeIsaDefault::kCauchy);
  ErasureCodeProfile profile;
  profile["k"] = "16";
  profile["m"] = "4";
  profile["technique"] = "cauchy";

  Isa.init(profile, &cerr);

  int k = 16;
  int m = 4;

#define LARGE_ENOUGH 2048
  bufferptr in_ptr(buffer::create_page_aligned(LARGE_ENOUGH));
  in_ptr.zero();
  in_ptr.set_length(0);
  const char *payload =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  in_ptr.append(payload, strlen(payload));
  bufferlist in;
  in.push_front(in_ptr);

  set<int>want_to_encode;

  map<int, bufferlist> encoded;
  for (int i = 0; i < (k + m); i++) {
    want_to_encode.insert(i);
  }


  EXPECT_EQ(0, Isa.encode(want_to_encode,
                          in,
                          &encoded));

  EXPECT_EQ((unsigned) (k + m), encoded.size());

  unsigned length = encoded[0].length();

  for (int i = 0; i < k; i++) {
    EXPECT_EQ(0, memcmp(encoded[i].c_str(), in.c_str() + (i * length), length));
  }

  buffer::ptr enc[k + m];
  // create buffers with a copy of the original data to be able to compare it after decoding
  {
    for (int i = 0; i < (k + m); i++) {
      buffer::ptr newenc(buffer::create_page_aligned(LARGE_ENOUGH));
      enc[i] = newenc;
      enc[i].zero();
      enc[i].set_length(0);
      enc[i].append(encoded[i].c_str(), length);
    }
  }

  // loop through all possible loss scenarios
  int cnt_cf = 0;

  for (int l1 = 0; l1 < (k + m); l1++) {
    map<int, bufferlist> degraded = encoded;
    set<int> want_to_decode;
    bool err;
    degraded.erase(l1);
    want_to_decode.insert(l1);
    err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
    EXPECT_EQ(0, err);
    cnt_cf++;
    for (int l2 = l1 + 1; l2 < (k + m); l2++) {
      degraded.erase(l2);
      want_to_decode.insert(l2);
      err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
      EXPECT_EQ(0, err);
      cnt_cf++;
      for (int l3 = l2 + 1; l3 < (k + m); l3++) {
        degraded.erase(l3);
        want_to_decode.insert(l3);
        err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
        EXPECT_EQ(0, err);
        cnt_cf++;
        for (int l4 = l3 + 1; l4 < (k + m); l4++) {
          degraded.erase(l4);
          want_to_decode.insert(l4);
          err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
          EXPECT_EQ(0, err);
          degraded[l4] = encoded[l4];
          want_to_decode.erase(l4);
          cnt_cf++;
        }
        degraded[l3] = encoded[l3];
        want_to_decode.erase(l3);
      }
      degraded[l2] = encoded[l2];
      want_to_decode.erase(l2);
    }
    degraded[l1] = encoded[l1];
    want_to_decode.erase(l1);
  }
  EXPECT_EQ(6195, cnt_cf);
  EXPECT_EQ(2516, tcache.getDecodingTableCacheSize(ErasureCodeIsaDefault::kCauchy));
}

TEST_F(IsaErasureCodeTest, isa_xor_codec)
{
  // Test all possible failure scenarios and reconstruction cases for
  // a (4,1) RAID-5 like configuration 

  ErasureCodeIsaDefault Isa(tcache);
  ErasureCodeProfile profile;
  profile["k"] = "4";
  profile["m"] = "1";
  Isa.init(profile, &cerr);

  int k = 4;
  int m = 1;

#define LARGE_ENOUGH 2048
  bufferptr in_ptr(buffer::create_page_aligned(LARGE_ENOUGH));
  in_ptr.zero();
  in_ptr.set_length(0);
  const char *payload =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  in_ptr.append(payload, strlen(payload));
  bufferlist in;
  in.push_front(in_ptr);

  set<int>want_to_encode;

  map<int, bufferlist> encoded;
  for (int i = 0; i < (k + m); i++) {
    want_to_encode.insert(i);
  }


  EXPECT_EQ(0, Isa.encode(want_to_encode,
                          in,
                          &encoded));

  EXPECT_EQ((unsigned) (k + m), encoded.size());

  unsigned length = encoded[0].length();

  for (int i = 0; i < k; i++) {
    EXPECT_EQ(0, memcmp(encoded[i].c_str(), in.c_str() + (i * length), length));
  }

  buffer::ptr enc[k + m];
  // create buffers with a copy of the original data to be able to compare it after decoding
  {
    for (int i = 0; i < (k + m); i++) {
      buffer::ptr newenc(buffer::create_page_aligned(LARGE_ENOUGH));
      enc[i] = newenc;
      enc[i].zero();
      enc[i].set_length(0);
      enc[i].append(encoded[i].c_str(), length);
    }
  }

  // loop through all possible loss scenarios
  int cnt_cf = 0;

  for (int l1 = 0; l1 < (k + m); l1++) {
    map<int, bufferlist> degraded = encoded;
    set<int> want_to_decode;
    bool err;
    degraded.erase(l1);
    want_to_decode.insert(l1);
    err = DecodeAndVerify(Isa, degraded, want_to_decode, enc, length);
    EXPECT_EQ(0, err);
    cnt_cf++;
    degraded[l1] = encoded[l1];
    want_to_decode.erase(l1);
  }
  EXPECT_EQ(5, cnt_cf);
}

TEST_F(IsaErasureCodeTest, create_ruleset)
{
  CrushWrapper *c = new CrushWrapper;
  c->create();
  int root_type = 2;
  c->set_type_name(root_type, "root");
  int host_type = 1;
  c->set_type_name(host_type, "host");
  int osd_type = 0;
  c->set_type_name(osd_type, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		root_type, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  map<string,string> loc;
  loc["root"] = "default";

  int num_host = 4;
  int num_osd = 5;
  int osd = 0;
  for (int h=0; h<num_host; ++h) {
    loc["host"] = string("host-") + stringify(h);
    for (int o=0; o<num_osd; ++o, ++osd) {
      c->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
    }
  }

  {
    stringstream ss;
    ErasureCodeIsaDefault isa(tcache);
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "2";
    profile["w"] = "8";
    isa.init(profile, &cerr);
    int ruleset = isa.create_ruleset("myrule", *c, &ss);
    EXPECT_EQ(0, ruleset);
    EXPECT_EQ(-EEXIST, isa.create_ruleset("myrule", *c, &ss));
    //
    // the minimum that is expected from the created ruleset is to
    // successfully map get_chunk_count() devices from the crushmap,
    // at least once.
    //
    vector<__u32> weight(c->get_max_devices(), 0x10000);
    vector<int> out;
    int x = 0;
    c->do_rule(ruleset, x, out, isa.get_chunk_count(), weight);
    ASSERT_EQ(out.size(), isa.get_chunk_count());
    for (unsigned i=0; i<out.size(); ++i)
      ASSERT_NE(CRUSH_ITEM_NONE, out[i]);
  }
  {
    stringstream ss;
    ErasureCodeIsaDefault isa(tcache);
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "2";
    profile["w"] = "8";
    profile["ruleset-root"] = "BAD";
    isa.init(profile, &cerr);
    EXPECT_EQ(-ENOENT, isa.create_ruleset("otherrule", *c, &ss));
    EXPECT_EQ("root item BAD does not exist", ss.str());
  }
  {
    stringstream ss;
    ErasureCodeIsaDefault isa(tcache);
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "2";
    profile["w"] = "8";
    profile["ruleset-failure-domain"] = "WORSE";
    isa.init(profile, &cerr);
    EXPECT_EQ(-EINVAL, isa.create_ruleset("otherrule", *c, &ss));
    EXPECT_EQ("unknown type WORSE", ss.str());
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 unittest_erasure_code_isa &&
 *   libtool --mode=execute valgrind --tool=memcheck \
 *      ./unittest_erasure_code_isa \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
