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

#include "test/unit.h"
#include "common/Formatter.h"

#include <sstream>
#include <string>

using std::ostringstream;

TEST(JsonFormatter, Simple1) {
  ostringstream oss,oss1;
  JSONFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.dump_int("a", 1);
  fmt.dump_int("b", 2);
  fmt.dump_int("c", 3);
  fmt.close_section();
  fmt.flush(oss);
  oss1 << "{\"a\":1,\"b\":2,\"c\":3}" << '\n';
  ASSERT_EQ(oss.str(), oss1.str());
}

TEST(JsonFormatter, Simple2) {
  ostringstream oss,oss1;
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
  oss1 << "{\"bar\":{\"int\":263882790666240,\
\"unsigned\":9223372036854775809,\"float\":1.234000},\
\"string\":\"str\"}" << '\n';
  ASSERT_EQ(oss.str(), oss1.str()); 
}

TEST(JsonFormatter, Empty) {
  ostringstream oss,oss1;
  JSONFormatter fmt(false);
  fmt.flush(oss);
  oss1 << '\n';
  ASSERT_EQ(oss.str(), oss1.str());
}

TEST(XmlFormatter, Simple1) {
  ostringstream oss,oss1;
  XMLFormatter fmt(false);
  fmt.open_object_section("foo");
  fmt.dump_int("a", 1);
  fmt.dump_int("b", 2);
  fmt.dump_int("c", 3);
  fmt.close_section();
  fmt.flush(oss);
  oss1 << "<foo><a>1</a><b>2</b><c>3</c></foo>" << '\n';
  ASSERT_EQ(oss.str(), oss1.str());
}

TEST(XmlFormatter, Simple2) {
  ostringstream oss,oss1;
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
  oss1 << "<foo><bar>\
<int>263882790666240</int>\
<unsigned>9223372036854775809</unsigned>\
<float>1.234</float>\
</bar><string>str</string>\
</foo>" << '\n';
  ASSERT_EQ(oss.str(), oss1.str());
}

TEST(XmlFormatter, Empty) {
  ostringstream oss,oss1;
  XMLFormatter fmt(false);
  fmt.flush(oss);
  oss1 << '\n';
  ASSERT_EQ(oss.str(), oss1.str());
}

TEST(XmlFormatter, DumpStream1) {
  ostringstream oss,oss1;
  XMLFormatter fmt(false);
  fmt.dump_stream("blah") << "hithere";
  fmt.flush(oss);
  oss1 << "<blah>hithere</blah>" << '\n';
  ASSERT_EQ(oss.str(), oss1.str());
}

TEST(XmlFormatter, DumpStream2) {
  ostringstream oss,oss1;
  XMLFormatter fmt(false);

  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.close_section();
  fmt.flush(oss);
  oss1 << "<foo><blah>hithere</blah></foo>" << '\n';
  ASSERT_EQ(oss.str(), oss1.str());
}

TEST(XmlFormatter, DumpStream3) {
  ostringstream oss,oss1;
  XMLFormatter fmt(false);

  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 3.14);
  fmt.close_section();
  fmt.flush(oss);
  oss1 << "<foo><blah>hithere</blah><pi>3.14</pi></foo>" << '\n';
  ASSERT_EQ(oss.str(), oss1.str()); 
}

TEST(XmlFormatter, DTD) {
  ostringstream oss,oss1;
  XMLFormatter fmt(false);

  fmt.write_raw_data(XMLFormatter::XML_1_DTD);
  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 3.14);
  fmt.close_section();
  fmt.flush(oss);
  oss1 <<  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<foo><blah>hithere</blah><pi>3.14</pi></foo>" << '\n';
  ASSERT_EQ(oss.str(), oss1.str());
}

TEST(XmlFormatter, Clear) {
  ostringstream oss,oss1;
  XMLFormatter fmt(false);

  fmt.write_raw_data(XMLFormatter::XML_1_DTD);
  fmt.open_array_section("foo");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 3.14);
  fmt.close_section();
  fmt.flush(oss);
  oss1 << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<foo><blah>hithere</blah><pi>3.14</pi></foo>" << '\n';
  ASSERT_EQ(oss.str(), oss1.str());

  ostringstream oss2,oss4;
  fmt.flush(oss2);
  oss4 << '\n';
  ASSERT_EQ(oss2.str(), oss4.str());

  ostringstream oss3,oss5;
  fmt.reset();
  fmt.flush(oss3);
  oss5 << '\n';
  ASSERT_EQ(oss3.str(), oss5.str());
}

TEST(XmlFormatter, NamespaceTest) {
  ostringstream oss,oss1;
  XMLFormatter fmt(false);

  fmt.write_raw_data(XMLFormatter::XML_1_DTD);
  fmt.open_array_section_in_ns("foo",
			   "http://s3.amazonaws.com/doc/2006-03-01/");
  fmt.dump_stream("blah") << "hithere";
  fmt.dump_float("pi", 3.14);
  fmt.close_section();
  fmt.flush(oss);
  oss1 << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" 
    "<foo xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" 
    "<blah>hithere</blah><pi>3.14</pi></foo>" << '\n';
  ASSERT_EQ(oss.str(), oss1.str());
}

TEST(XmlFormatter, DumpFormatNameSpaceTest) {
  ostringstream oss1,oss3;
  XMLFormatter fmt(false);

  fmt.dump_format_ns("foo",
		     "http://s3.amazonaws.com/doc/2006-03-01/",
		     "%s","bar");
  fmt.flush(oss1);
  oss3 << "<foo xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">bar</foo>" << '\n';
  ASSERT_EQ(oss1.str(),oss3.str());

  // Testing with a null ns..should be same as dump format
  ostringstream oss2,oss4;
  fmt.reset();
  fmt.dump_format_ns("foo",NULL,"%s","bar");
  fmt.flush(oss2);
  oss4 << "<foo>bar</foo>" << '\n';
  ASSERT_EQ(oss2.str(),oss4.str());
}
