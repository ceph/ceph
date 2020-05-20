// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/armor.h"
#include "common/config.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "common/Clock.h"

#include "gtest/gtest.h"

TEST(RoundTrip, SimpleRoundTrip) {
  static const int OUT_LEN = 4096;
  const char * const original = "abracadabra";
  const char * const correctly_encoded = "YWJyYWNhZGFicmE=";
  char out[OUT_LEN];
  memset(out, 0, sizeof(out));
  int alen = ceph_armor(out, out + OUT_LEN, original, original + strlen(original));
  ASSERT_STREQ(correctly_encoded, out);

  char out2[OUT_LEN];
  memset(out2, 0, sizeof(out2));
  ceph_unarmor(out2, out2 + OUT_LEN, out, out + alen);
  ASSERT_STREQ(original, out2);
}

TEST(RoundTrip, RandomRoundTrips) {
  static const int IN_MAX = 1024;
  static const int OUT_MAX = 4096;
  static const int ITERS = 1000;
  for (int i = 0; i < ITERS; ++i) {
    unsigned int seed = i;
    int in_len = rand_r(&seed) % IN_MAX;

    char in[IN_MAX];
    memset(in, 0, sizeof(in));
    for (int j = 0; j < in_len; ++j) {
      in[j] = rand_r(&seed) % 0xff;
    }
    char out[OUT_MAX];
    memset(out, 0, sizeof(out));
    int alen = ceph_armor(out, out + OUT_MAX, in, in + in_len);
    ASSERT_GE(alen, 0);

    char decoded[IN_MAX];
    memset(decoded, 0, sizeof(decoded));
    int blen = ceph_unarmor(decoded, decoded + IN_MAX, out, out + alen);
    ASSERT_GE(blen, 0);

    ASSERT_EQ(memcmp(in, decoded, in_len), 0);
  }
}

TEST(EdgeCase, EndsInNewline) {
  static const int OUT_MAX = 4096;

  char b64[] =
    "aaaa\n";

    char decoded[OUT_MAX];
    memset(decoded, 0, sizeof(decoded));
    int blen = ceph_unarmor(decoded, decoded + OUT_MAX, b64, b64 + sizeof(b64)-1);
    ASSERT_GE(blen, 0);
}

TEST(FuzzEncoding, BadDecode1) {
  static const int OUT_LEN = 4096;
  const char * const bad_encoded = "FAKEBASE64 foo";
  char out[OUT_LEN];
  memset(out, 0, sizeof(out));
  int alen = ceph_unarmor(out, out + OUT_LEN, bad_encoded, bad_encoded + strlen(bad_encoded));
  ASSERT_LT(alen, 0);
}

TEST(FuzzEncoding, BadDecode2) {
  string str("FAKEBASE64 foo");
  bool failed = false;
  try {
    bufferlist bl;
    bl.append(str);

    bufferlist cl;
    cl.decode_base64(bl);
    cl.hexdump(std::cerr);
  }
  catch (const buffer::error &err) {
    failed = true;
  }
  ASSERT_EQ(failed, true);
}

void bench_base64_encode(size_t len, int encode_count) {
  bufferlist bl;

  int fd = open("/dev/urandom", O_RDONLY);
  if (fd < 0) {
    std::cout << "open /dev/urandom failed" << std::endl;
    return;
  }
  bl.read_fd(fd, len);
  EXPECT_EQ(bl.length(), len);

  utime_t start = ceph_clock_now();
  for (int i = 0; i < encode_count; i++) {
    bufferlist rst;
    bl.encode_base64(rst);
  }
  utime_t end = ceph_clock_now();
  cout << "base64 encode len: " << setw(8) << len << ", "
       << "encode " << setw(4) << encode_count << " times, "
       << "total time(s): " << setw(8) << end - start << std::endl;
}

TEST(BenchBase64, BenchEncode) {
  bench_base64_encode(8388608, 10);
  bench_base64_encode(8192, 20);
  bench_base64_encode(4096, 20);
  bench_base64_encode(1024, 40);
  bench_base64_encode(256, 80);
  bench_base64_encode(128, 100);
  bench_base64_encode(40, 100);
}

void bench_base64_decode(size_t reverse_encode_len, int decode_count) {
  bufferlist bl;
  int fd = open("/dev/urandom", O_RDONLY);
  if (fd < 0) {
    std::cout << "open /dev/urandom failed" << std::endl;
    return;
  }
  bl.read_fd(fd, reverse_encode_len);
  EXPECT_EQ(bl.length(), reverse_encode_len);

  bufferlist bl_enc;
  bl.encode_base64(bl_enc);

  utime_t start = ceph_clock_now();
  for (int i = 0; i < decode_count; i++) {
    bufferlist rst;
    rst.decode_base64(bl_enc);
  }
  utime_t end = ceph_clock_now();
  cout << "base64 decode len: " << setw(8) << bl_enc.length() << ", "
       << "decode " << setw(4) << decode_count << " times, "
       << "total time(s): " << setw(8) << end - start << std::endl;
}

TEST(BenchBase64, BenchDecode) {
  bench_base64_decode(8388608, 10);
  bench_base64_decode(8192, 20);
  bench_base64_decode(4096, 20);
  bench_base64_decode(1024, 40);
  bench_base64_decode(256, 80);
  bench_base64_decode(128, 100);
  bench_base64_decode(40, 100);
}
