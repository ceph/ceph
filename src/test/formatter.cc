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

#include "gtest/gtest.h"
#include "common/Formatter.h"
#include "common/HTMLFormatter.h"

#include <sstream>
#include <string>

using namespace ceph;
using std::ostringstream;

TEST(JsonFormatter, Simple1) {
  ostringstream oss;
  JSONFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.dump_int("a", 1);
  fmt.dump_int("b", 2);
  fmt.dump_int("c", 3);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "{\"a\":1,\"b\":2,\"c\":3}");
}

TEST(JsonFormatter, Simple2) {
  ostringstream oss;
  JSONFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.open_object_section("bar");
  fmt.dump_int("int", 0xf00000000000ll);
  fmt.dump_unsigned("unsigned", 0x8000000000000001llu);
  fmt.dump_float("float", 1.234);
  fmt.close_section();
  fmt.dump_string("string", "str");
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "{\"bar\":{\"int\":263882790666240,\
\"unsigned\":9223372036854775809,\"float\":1.234},\
\"string\":\"str\"}");
}

TEST(JsonFormatter, CunningFloats) {
  ostringstream oss;
  JSONFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.dump_float("long", 1.0 / 7);
  fmt.dump_float("big", 12345678901234567890.0);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "{\"long\":0.14285714285714285,\"big\":1.2345678901234567e+19}");
}

TEST(JsonFormatter, Empty) {
  ostringstream oss;
  JSONFormatter fmt(false);
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "");
}

TEST(XmlFormatter, Simple1) {
  ostringstream oss;
  XMLFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.dump_int("a", 1);
  fmt.dump_int("b", 2);
  fmt.dump_int("c", 3);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<foo><a>1</a><b>2</b><c>3</c></foo>");
}

TEST(XmlFormatter, Simple2) {
  ostringstream oss;
  XMLFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.open_object_section("bar");
  fmt.dump_int("int", 0xf00000000000ll);
  fmt.dump_unsigned("unsigned", 0x8000000000000001llu);
  fmt.dump_float("float", 1.234);
  fmt.close_section();
  fmt.dump_string("string", "str");
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<foo><bar>\
<int>263882790666240</int>\
<unsigned>9223372036854775809</unsigned>\
<float>1.234</float>\
</bar><string>str</string>\
</foo>");
}

TEST(XmlFormatter, Empty) {
  ostringstream oss;
  XMLFormatter fmt(false);
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "");
}

TEST(XmlFormatter, DumpStream1) {
  ostringstream oss;
  XMLFormatter fmt(false);
  fmt.dump_stream("blah") << "hithere";
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<blah>hithere</blah>");
}

TEST(XmlFormatter, DumpStream2) {
  ostringstream oss;
  XMLFormatter fmt(false);

  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<foo><blah>hithere</blah></foo>");
}

TEST(XmlFormatter, DumpStream3) {
  ostringstream oss;
  XMLFormatter fmt(false);

  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 0.128);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<foo><blah>hithere</blah><pi>0.128</pi></foo>");
}

TEST(XmlFormatter, DTD) {
  ostringstream oss;
  XMLFormatter fmt(false);

  fmt.output_header();
  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 0.128);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<foo><blah>hithere</blah><pi>0.128</pi></foo>");
}

TEST(XmlFormatter, Clear) {
  ostringstream oss;
  XMLFormatter fmt(false);

  fmt.output_header();
  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 0.128);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<foo><blah>hithere</blah><pi>0.128</pi></foo>");

  ostringstream oss2;
  fmt.flush(oss2);
  ASSERT_EQ(oss2.str(), "");

  ostringstream oss3;
  fmt.reset();
  fmt.flush(oss3);
  ASSERT_EQ(oss3.str(), "");
}

TEST(XmlFormatter, NamespaceTest) {
  ostringstream oss;
  XMLFormatter fmt(false);

  fmt.output_header();
  fmt.open_array_section_in_ns("foo",
			   "http://s3.amazonaws.com/doc/2006-03-01/");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 0.128);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<foo xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    "<blah>hithere</blah><pi>0.128</pi></foo>");
}

TEST(XmlFormatter, DumpFormatNameSpaceTest) {
  ostringstream oss1;
  XMLFormatter fmt(false);

  fmt.dump_format_ns("foo",
		     "http://s3.amazonaws.com/doc/2006-03-01/",
		     "%s","bar");
  fmt.flush(oss1);
  ASSERT_EQ(oss1.str(),
	    "<foo xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">bar</foo>");

  // Testing with a null ns..should be same as dump format
  ostringstream oss2;
  fmt.reset();
  fmt.dump_format_ns("foo",NULL,"%s","bar");
  fmt.flush(oss2);
  ASSERT_EQ(oss2.str(),"<foo>bar</foo>");
}

TEST(HtmlFormatter, Simple1) {
  ostringstream oss;
  HTMLFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.dump_int("a", 1);
  fmt.dump_int("b", 2);
  fmt.dump_int("c", 3);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<foo><li>a: 1</li><li>b: 2</li><li>c: 3</li></foo>");
}

TEST(HtmlFormatter, Simple2) {
  ostringstream oss;
  HTMLFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.open_object_section("bar");
  fmt.dump_int("int", 0xf00000000000ll);
  fmt.dump_unsigned("unsigned", 0x8000000000000001llu);
  fmt.dump_float("float", 1.234);
  fmt.close_section();
  fmt.dump_string("string", "str");
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<foo><bar>\
<li>int: 263882790666240</li>\
<li>unsigned: 9223372036854775809</li>\
<li>float: 1.234</li>\
</bar><li>string: str</li>\
</foo>");
}

TEST(HtmlFormatter, Empty) {
  ostringstream oss;
  HTMLFormatter fmt(false);
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "");
}

TEST(HtmlFormatter, DumpStream1) {
  ostringstream oss;
  HTMLFormatter fmt(false);
  fmt.dump_stream("blah") << "hithere";
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<li>blah: hithere</li>");
}

TEST(HtmlFormatter, DumpStream2) {
  ostringstream oss;
  HTMLFormatter fmt(false);

  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<foo><li>blah: hithere</li></foo>");
}

TEST(HtmlFormatter, DumpStream3) {
  ostringstream oss;
  HTMLFormatter fmt(false);

  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 0.128);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<foo><li>blah: hithere</li><li>pi: 0.128</li></foo>");
}

TEST(HtmlFormatter, DTD) {
  ostringstream oss;
  HTMLFormatter fmt(false);

  fmt.write_raw_data(HTMLFormatter::XML_1_DTD);
  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 0.128);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<foo><li>blah: hithere</li><li>pi: 0.128</li></foo>");
}

TEST(HtmlFormatter, Clear) {
  ostringstream oss;
  HTMLFormatter fmt(false);

  fmt.write_raw_data(HTMLFormatter::XML_1_DTD);
  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 0.128);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<foo><li>blah: hithere</li><li>pi: 0.128</li></foo>");

  ostringstream oss2;
  fmt.flush(oss2);
  ASSERT_EQ(oss2.str(), "");

  ostringstream oss3;
  fmt.reset();
  fmt.flush(oss3);
  ASSERT_EQ(oss3.str(), "");
}

TEST(HtmlFormatter, NamespaceTest) {
  ostringstream oss;
  HTMLFormatter fmt(false);

  fmt.write_raw_data(HTMLFormatter::XML_1_DTD);
  fmt.open_array_section_in_ns("foo",
			   "http://s3.amazonaws.com/doc/2006-03-01/");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 0.128);
  fmt.close_section();
  fmt.flush(oss);
  ASSERT_EQ(oss.str(), "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<foo xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    "<li>blah: hithere</li><li>pi: 0.128</li></foo>");
}

TEST(HtmlFormatter, DumpFormatNameSpaceTest) {
  ostringstream oss1;
  HTMLFormatter fmt(false);

  fmt.dump_format_ns("foo",
		     "http://s3.amazonaws.com/doc/2006-03-01/",
		     "%s","bar");
  fmt.flush(oss1);
  ASSERT_EQ(oss1.str(),
	    "<li xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">foo: bar</li>");

  // Testing with a null ns..should be same as dump format
  ostringstream oss2;
  fmt.reset();
  fmt.dump_format_ns("foo",NULL,"%s","bar");
  fmt.flush(oss2);
  ASSERT_EQ(oss2.str(),"<li>foo: bar</li>");
}
