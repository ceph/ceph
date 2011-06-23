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

#include "include/rados/librgw.h"

#include "gtest/gtest.h"

#include <stdint.h>

static const char SAMPLE_XML_1[] = \
"<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
  <Owner>\n\
    <ID>foo</ID>\n\
    <DisplayName>MrFoo</DisplayName>\n\
  </Owner>\n\
  <AccessControlList>\n\
    <Grant>\n\
      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
xsi:type=\"CanonicalUser\">\n\
	<ID>bar</ID>\n\
	<DisplayName>display-name</DisplayName>\n\
      </Grantee>\n\
      <Permission>FULL_CONTROL</Permission>\n\
    </Grant>\n\
  </AccessControlList>\n\
</AccessControlPolicy>";

static const uint8_t VERSION1_BIN[] = {
  0x01, 0x01, 0x07, 0x00, 0x00, 0x00, 0x63, 0x6d,
  0x63, 0x63, 0x61, 0x62, 0x65, 0x07, 0x00, 0x00,
  0x00, 0x63, 0x6d, 0x63, 0x63, 0x61, 0x62, 0x65,
  0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
  0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x63, 0x6d,
  0x63, 0x63, 0x61, 0x62, 0x65, 0x01, 0x01, 0x00,
  0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x63,
  0x6d, 0x63, 0x63, 0x61, 0x62, 0x65, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x0f,
  0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x63,
  0x6d, 0x63, 0x63, 0x61, 0x62, 0x65, 0x0a, 0x0a
};

TEST(LibRGW, FromBin) {
  int ret;
  const char *bin = (const char*)VERSION1_BIN;
  int len = sizeof(VERSION1_BIN) / sizeof(VERSION1_BIN[0]);
  librgw_t rgw;

  ret = librgw_create(&rgw, NULL);
  ASSERT_EQ(ret, 0);

  char *xml = NULL;
  ret = librgw_acl_bin2xml(rgw, bin, len, &xml);
  ASSERT_EQ(ret, 0);

  librgw_shutdown(rgw);
}

TEST(LibRGW, RoundTrip) {

  int ret;
  char *bin = NULL;
  int bin_len = 0;
  librgw_t rgw;

  ret = librgw_create(&rgw, NULL);
  ASSERT_EQ(ret, 0);

  ret = librgw_acl_xml2bin(rgw, SAMPLE_XML_1, &bin, &bin_len);
  ASSERT_EQ(ret, 0);

  char *xml2 = NULL;
  ret = librgw_acl_bin2xml(rgw, bin, bin_len, &xml2);
  ASSERT_EQ(ret, 0);

  char *bin2 = NULL;
  int bin_len2 = 0;
  ret = librgw_acl_xml2bin(rgw, xml2, &bin2, &bin_len2);
  ASSERT_EQ(ret, 0);

  // the serialized representation should be the same.
  ASSERT_EQ(bin_len, bin_len2);
  ASSERT_EQ(memcmp(bin, bin2, bin_len), 0);

  // Free memory
  // As you can see, we ignore freeing memory on test failures
  // Don't do this in your real programs!
  librgw_free_bin(rgw, bin);
  librgw_free_xml(rgw, xml2);
  librgw_free_bin(rgw, bin2);

  librgw_shutdown(rgw);
}
