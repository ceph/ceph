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
static std::string escape_xml_stream(const char *str)
{
  std::stringstream ss;
  ss << xml_stream_escaper(str);
  return ss.str();
}

TEST(EscapeXml, PassThrough) {
  ASSERT_EQ(escape_xml_attrs("simplicity itself"), "simplicity itself");
  ASSERT_EQ(escape_xml_stream("simplicity itself"), "simplicity itself");
  ASSERT_EQ(escape_xml_attrs(""), "");
  ASSERT_EQ(escape_xml_stream(""), "");
  ASSERT_EQ(escape_xml_attrs("simple examples please!"), "simple examples please!");
  ASSERT_EQ(escape_xml_stream("simple examples please!"), "simple examples please!");
}

TEST(EscapeXml, EntityRefs1) {
  ASSERT_EQ(escape_xml_attrs("The \"scare quotes\""), "The &quot;scare quotes&quot;");
  ASSERT_EQ(escape_xml_stream("The \"scare quotes\""), "The &quot;scare quotes&quot;");
  ASSERT_EQ(escape_xml_attrs("I <3 XML"), "I &lt;3 XML");
  ASSERT_EQ(escape_xml_stream("I <3 XML"), "I &lt;3 XML");
  ASSERT_EQ(escape_xml_attrs("Some 'single' \"quotes\" here"),
	    "Some &apos;single&apos; &quot;quotes&quot; here");
  ASSERT_EQ(escape_xml_stream("Some 'single' \"quotes\" here"),
	    "Some &apos;single&apos; &quot;quotes&quot; here");
}

TEST(EscapeXml, ControlChars) {
  ASSERT_EQ(escape_xml_attrs("\x01\x02\x03"), "&#x01;&#x02;&#x03;");
  ASSERT_EQ(escape_xml_stream("\x01\x02\x03"), "&#x01;&#x02;&#x03;");

  ASSERT_EQ(escape_xml_attrs("abc\x7f"), "abc&#x7f;");
  ASSERT_EQ(escape_xml_stream("abc\x7f"), "abc&#x7f;");
}

TEST(EscapeXml, Utf8) {
  const char *cc1 = "\xe6\xb1\x89\xe5\xad\x97\n";
  ASSERT_EQ(escape_xml_attrs(cc1), cc1);
  ASSERT_EQ(escape_xml_stream(cc1), cc1);

  ASSERT_EQ(escape_xml_attrs("<\xe6\xb1\x89\xe5\xad\x97>\n"), "&lt;\xe6\xb1\x89\xe5\xad\x97&gt;\n");
  ASSERT_EQ(escape_xml_stream("<\xe6\xb1\x89\xe5\xad\x97>\n"), "&lt;\xe6\xb1\x89\xe5\xad\x97&gt;\n");
}

static std::string escape_json_attrs(const char *str)
{
  int src_len = strlen(str);
  int len = escape_json_attr_len(str, src_len);
  char out[len];
  escape_json_attr(str, src_len, out);
  return out;
}
static std::string escape_json_stream(const char *str)
{
  std::stringstream ss;
  ss << json_stream_escaper(str);
  return ss.str();
}

TEST(EscapeJson, PassThrough) {
  ASSERT_EQ(escape_json_attrs("simplicity itself"), "simplicity itself");
  ASSERT_EQ(escape_json_stream("simplicity itself"), "simplicity itself");
  ASSERT_EQ(escape_json_attrs(""), "");
  ASSERT_EQ(escape_json_stream(""), "");
  ASSERT_EQ(escape_json_attrs("simple examples please!"), "simple examples please!");
  ASSERT_EQ(escape_json_stream("simple examples please!"), "simple examples please!");
}

TEST(EscapeJson, Escapes1) {
  ASSERT_EQ(escape_json_attrs("The \"scare quotes\""),
			     "The \\\"scare quotes\\\"");
  ASSERT_EQ(escape_json_stream("The \"scare quotes\""),
			      "The \\\"scare quotes\\\"");
  ASSERT_EQ(escape_json_attrs("I <3 JSON"), "I <3 JSON");
  ASSERT_EQ(escape_json_stream("I <3 JSON"), "I <3 JSON");
  ASSERT_EQ(escape_json_attrs("Some 'single' \"quotes\" here"),
      "Some 'single' \\\"quotes\\\" here");
  ASSERT_EQ(escape_json_stream("Some 'single' \"quotes\" here"),
      "Some 'single' \\\"quotes\\\" here");
  ASSERT_EQ(escape_json_attrs("tabs\tand\tnewlines\n, oh my"),
      "tabs\\tand\\tnewlines\\n, oh my");
  ASSERT_EQ(escape_json_stream("tabs\tand\tnewlines\n, oh my"),
      "tabs\\tand\\tnewlines\\n, oh my");
}

TEST(EscapeJson, ControlChars) {
  ASSERT_EQ(escape_json_attrs("\x01\x02\x03"), "\\u0001\\u0002\\u0003");
  ASSERT_EQ(escape_json_stream("\x01\x02\x03"), "\\u0001\\u0002\\u0003");

  ASSERT_EQ(escape_json_attrs("abc\x7f"), "abc\\u007f");
  ASSERT_EQ(escape_json_stream("abc\x7f"), "abc\\u007f");
}

TEST(EscapeJson, Utf8) {
  EXPECT_EQ(escape_json_attrs("\xe6\xb1\x89\xe5\xad\x97\n"), "\xe6\xb1\x89\xe5\xad\x97\\n");
  EXPECT_EQ(escape_json_stream("\xe6\xb1\x89\xe5\xad\x97\n"), "\xe6\xb1\x89\xe5\xad\x97\\n");
}
