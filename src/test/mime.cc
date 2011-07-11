// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/mime.h"
#include "gtest/gtest.h"

#include <stdint.h>
#include <string>

using std::string;

TEST(MimeTests, SimpleEncode) {
  char output[256];
  memset(output, 0, sizeof(output));
  int len;

  len = mime_encode_as_qp("abc", NULL, 0);
  ASSERT_EQ(len, 4);
  len = mime_encode_as_qp("abc", output, 4);
  ASSERT_EQ(len, 4);
  ASSERT_EQ(string("abc"), string(output));

  len = mime_encode_as_qp("a=b", NULL, 0);
  ASSERT_EQ(len, 6);
  len = mime_encode_as_qp("a=b", output, 6);
  ASSERT_EQ(len, 6);
  ASSERT_EQ(string("a=3Db"), string(output));

  len = mime_encode_as_qp("Libert\xc3\xa9", NULL, 0);
  ASSERT_EQ(len, 13);
  len = mime_encode_as_qp("Libert\xc3\xa9", output, 13);
  ASSERT_EQ(len, 13);
  ASSERT_EQ(string("Libert=C3=A9"), string(output));
}

TEST(MimeTests, EncodeOutOfSpace) {
  char output[256];
  memset(output, 0, sizeof(output));
  int len;

  len = mime_encode_as_qp("abcdefg", NULL, 0);
  ASSERT_EQ(len, 8);
  len = mime_encode_as_qp("abcdefg", output, 4);
  ASSERT_EQ(len, 8);
  ASSERT_EQ(string("abc"), string(output));
  len = mime_encode_as_qp("abcdefg", output, 1);
  ASSERT_EQ(len, 8);
  ASSERT_EQ(string(""), string(output));

  len = mime_encode_as_qp("a=b", output, 2);
  ASSERT_EQ(len, 6);
  ASSERT_EQ(string("a"), string(output));
  len = mime_encode_as_qp("a=b", output, 3);
  ASSERT_EQ(len, 6);
  ASSERT_EQ(string("a"), string(output));
}

TEST(MimeTests, SimpleDecode) {
  char output[256];
  memset(output, 0, sizeof(output));
  int len;

  len = mime_decode_from_qp("abc", NULL, 0);
  ASSERT_EQ(len, 4);
  len = mime_decode_from_qp("abc", output, 4);
  ASSERT_EQ(len, 4);
  ASSERT_EQ(string("abc"), string(output));

  len = mime_decode_from_qp("a=3Db", NULL, 0);
  ASSERT_EQ(len, 4);
  len = mime_decode_from_qp("a=3Db", output, 4);
  ASSERT_EQ(len, 4);
  ASSERT_EQ(string("a=b"), string(output));

  len = mime_decode_from_qp("Libert=C3=A9", NULL, 0);
  ASSERT_EQ(len, 9);
  len = mime_decode_from_qp("Libert=C3=A9", output, 9);
  ASSERT_EQ(len, 9);
  ASSERT_EQ(string("Libert\xc3\xa9"), string(output));
}

TEST(MimeTests, LowercaseDecode) {
  char output[256];
  memset(output, 0, sizeof(output));
  int len;

  len = mime_decode_from_qp("Libert=c3=a9", NULL, 0);
  ASSERT_EQ(len, 9);
  len = mime_decode_from_qp("Libert=c3=a9", output, 9);
  ASSERT_EQ(len, 9);
  ASSERT_EQ(string("Libert\xc3\xa9"), string(output));
}

TEST(MimeTests, DecodeOutOfSpace) {
  char output[256];
  memset(output, 0, sizeof(output));
  int len;

  len = mime_decode_from_qp("abcdefg", NULL, 0);
  ASSERT_EQ(len, 8);
  len = mime_decode_from_qp("abcdefg", output, 4);
  ASSERT_EQ(len, 8);
  ASSERT_EQ(string("abc"), string(output));
  len = mime_decode_from_qp("abcdefg", output, 1);
  ASSERT_EQ(len, 8);
  ASSERT_EQ(string(""), string(output));

  len = mime_decode_from_qp("a=3Db", output, 2);
  ASSERT_EQ(len, 4);
  ASSERT_EQ(string("a"), string(output));
  len = mime_decode_from_qp("a=3Db", output, 3);
  ASSERT_EQ(len, 4);
  ASSERT_EQ(string("a="), string(output));
}

TEST(MimeTests, DecodeErrors) {
  char output[128];
  memset(output, 0, sizeof(output));
  int len;

  // incomplete escape sequence
  len = mime_decode_from_qp("boo=", output, sizeof(output));
  ASSERT_LT(len, 0);

  // invalid escape sequences
  len = mime_decode_from_qp("boo=gg", output, sizeof(output));
  ASSERT_LT(len, 0);
  len = mime_decode_from_qp("boo=g", output, sizeof(output));
  ASSERT_LT(len, 0);
  len = mime_decode_from_qp("boo==", output, sizeof(output));
  ASSERT_LT(len, 0);
  len = mime_decode_from_qp("boo=44bar=z", output, sizeof(output));
  ASSERT_LT(len, 0);

  // high bit should not be set in quoted-printable mime output
  unsigned char bad_input2[] = { 0x81, 0x6a, 0x0 };
  len = mime_decode_from_qp(reinterpret_cast<const char*>(bad_input2),
			    output, sizeof(output));
  ASSERT_LT(len, 0);
}
