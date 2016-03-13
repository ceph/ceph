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
#include "common/escape.h"
#include "gtest/gtest.h"
#include <stdint.h>

static std::string escape_xml_attrs(const char *str)
{
  int len = escape_xml_attr_len(str);
  char out[len];
  escape_xml_attr(str, out);
  return out;
}

TEST(EscapeXml, PassThrough) {
  ASSERT_EQ(escape_xml_attrs("simplicity itself"), "simplicity itself");
  ASSERT_EQ(escape_xml_attrs(""), "");
  ASSERT_EQ(escape_xml_attrs("simple examples please!"), "simple examples please!");
}

TEST(EscapeXml, EntityRefs1) {
  ASSERT_EQ(escape_xml_attrs("The \"scare quotes\""), "The &quot;scare quotes&quot;");
  ASSERT_EQ(escape_xml_attrs("I <3 XML"), "I &lt;3 XML");
  ASSERT_EQ(escape_xml_attrs("Some 'single' \"quotes\" here"),
	    "Some &apos;single&apos; &quot;quotes&quot; here");
}

TEST(EscapeXml, ControlChars) {
  uint8_t cc1[] = { 0x01, 0x02, 0x03, 0x0 };
  ASSERT_EQ(escape_xml_attrs((char*)cc1), "&#x01;&#x02;&#x03;");

  uint8_t cc2[] = { 0x61, 0x62, 0x63, 0x7f, 0x0 };
  ASSERT_EQ(escape_xml_attrs((char*)cc2), "abc&#x7f;");
}

TEST(EscapeXml, Utf8) {
  uint8_t cc1[] = { 0xe6, 0xb1, 0x89, 0xe5, 0xad, 0x97, 0x0a, 0x0 };
  ASSERT_EQ(escape_xml_attrs((const char*)cc1), (const char*)cc1);

  uint8_t cc2[] = { 0x3c, 0xe6, 0xb1, 0x89, 0xe5, 0xad, 0x97, 0x3e, 0x0a, 0x0 };
  uint8_t cc2_out[] = { 0x26, 0x6c, 0x74, 0x3b, 0xe6, 0xb1, 0x89, 0xe5,
			0xad, 0x97, 0x26, 0x67, 0x74, 0x3b, 0x0a, 0x0 };
  ASSERT_EQ(escape_xml_attrs((const char*)cc2), (const char*)cc2_out);
}

static std::string escape_json_attrs(const char *str, int cooked = 0)
{
  int src_len = strlen(str);
  int len = escape_json_attr_len(str, src_len, cooked);
  char out[len];
  escape_json_attr(str, src_len, out, cooked);
  return out;
}

TEST(EscapeJson, PassThrough) {
  ASSERT_EQ(escape_json_attrs("simplicity itself"), "simplicity itself");
  ASSERT_EQ(escape_json_attrs(""), "");
  ASSERT_EQ(escape_json_attrs("simple examples please!"), "simple examples please!");
}

TEST(EscapeJson, Escapes1) {
  ASSERT_EQ(escape_json_attrs("The \"scare quotes\""),
			     "The \\\"scare quotes\\\"");
  ASSERT_EQ(escape_json_attrs("I <3 JSON"), "I <3 JSON");
  ASSERT_EQ(escape_json_attrs(
      "JSON calls a slash / backslash a solidus / reverse solidus"),
      "JSON calls a slash \\/ backslash a solidus \\/ reverse solidus");
  ASSERT_EQ(escape_json_attrs("Some 'single' \"quotes\" here"),
      "Some 'single' \\\"quotes\\\" here");
  ASSERT_EQ(escape_json_attrs("tabs\tand\tnewlines\n, oh my"),
      "tabs\\tand\\tnewlines\\n, oh my");
  ASSERT_EQ(escape_json_attrs(
      "JSON calls a slash / backslash a solidus / </br>reverse solidus", 1),
      "JSON calls a slash / backslash a solidus / </br>reverse solidus");
  ASSERT_EQ(escape_json_attrs(
      "JSON calls a slash / backslash a solidus / </br>reverse solidus", 2),
      "JSON calls a slash / backslash a solidus / <\\/br>reverse solidus");
}

TEST(EscapeJson, ControlChars) {
  uint8_t cc1[] = { 0x01, 0x02, 0x03, 0x0 };
  ASSERT_EQ(escape_json_attrs((char*)cc1), "\\u0001\\u0002\\u0003");

  uint8_t cc2[] = { 0x61, 0x62, 0x63, 0x7f, 0x0 };
  ASSERT_EQ(escape_json_attrs((char*)cc2), "abc\\u007f");
}

TEST(EscapeJson, Utf8) {
  uint8_t cc1[] = { 0xe6, 0xb1, 0x89, 0xe5, 0xad, 0x97, 0x0a, 0x0 };
  ASSERT_EQ(escape_xml_attrs((const char*)cc1), (const char*)cc1);
}
