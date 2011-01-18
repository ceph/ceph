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
#include "config.h"
#include "include/buffer.h"
#include "include/encoding.h"

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
