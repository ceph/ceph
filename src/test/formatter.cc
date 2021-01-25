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
#include "common/formatter_filter.h"

#include <sstream>
#include <string>

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/linear_congruential.hpp>
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

using namespace ceph::formatter_filter;
TEST(FormatterFilter, TokenTypes) {
  TokenSplitter tok;
  tok.set_input("! ) (.variable!>=  has \"text\"  0x1111 777   ");
  token_t t;
  t = tok.peek();
  ASSERT_EQ(t, eOperator);
  ASSERT_EQ(tok.get_operator(), neg);
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eRightBracket);
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eLeftBracket);
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eVariable);
  ASSERT_EQ(tok.get_variable(), ".variable");
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eOperator);
  ASSERT_EQ(tok.get_operator(), neg);
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eOperator);
  ASSERT_EQ(tok.get_operator(), ge);
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eOperator);
  ASSERT_EQ(tok.get_operator(), has);
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eLiteral);
  ASSERT_EQ(tok.get_literal(), "text");
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eNumber);
  ASSERT_EQ(tok.get_number(), 0x1111);
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eNumber);
  ASSERT_EQ(tok.get_number(), 777);
  tok.consume();

  t = tok.peek();
  ASSERT_EQ(t, eEnd);
}

TEST(FormatterFilter, ErrorMessages) {
  TokenSplitter token_src;
  VariableMap vars;
  std::string error;
  ExpRef exp;
  bool b;
  token_src.set_input("(1 + .var");
  b = parse(token_src, vars, error, &exp);
  EXPECT_EQ(b, false);
  token_src.show_error(std::cout, error);

  token_src.set_input("(1 + + .var)");
  b = parse(token_src, vars, error, &exp);
  EXPECT_EQ(b, false);
  token_src.show_error(std::cout, error);

  token_src.set_input("(1 + .var))");
  b = parse(token_src, vars, error, &exp);
  EXPECT_EQ(b, false);
  token_src.show_error(std::cout, error);

  token_src.set_input("has b");
  b = parse(token_src, vars, error, &exp);
  EXPECT_EQ(b, false);
  token_src.show_error(std::cout, error);
}


TEST(FormatterFilter, Expressions_basic) {
  TokenSplitter token_src;
  token_src.set_input("1 + .var");
  VariableMap vars;
  std::string error;
  ExpRef exp;
  bool b = parse(token_src, vars, error, &exp);
  ASSERT_EQ(b, true);
  auto it = vars.find(".var");
  ASSERT_NE(it, vars.end());
  ASSERT_NE(exp.get(), nullptr);
  Exp::Value v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::einv);
  vars[".var"].reset(new string("1"));
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::eint);
  ASSERT_EQ(std::get<int64_t>(v), 2);
}

TEST(FormatterFilter, Expressions_more) {
  TokenSplitter token_src;
  token_src.set_input("(1 + .var)*.var");
  VariableMap vars;
  std::string error;
  ExpRef exp;
  bool b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  {
  vars[".var"].reset(new string("1"));
  Exp::Value v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::eint);
  ASSERT_EQ(std::get<int64_t>(v), 2);
  }
  {
  vars[".var"].reset(new string("2"));
  Exp::Value v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::eint);
  ASSERT_EQ(std::get<int64_t>(v), 6);
  }
  {
  vars[".var"].reset();
  Exp::Value v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::einv);
  }
}

TEST(FormatterFilter, Expressions_priority) {
  TokenSplitter token_src;
  VariableMap vars;
  std::string error;
  ExpRef exp;
  bool b;
  Exp::Value v;
  token_src.set_input("1 + 2 * 3");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::eint);
  ASSERT_EQ(std::get<int64_t>(v), 7);

  token_src.set_input("1+2*3+4*5+6*7");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::eint);
  ASSERT_EQ(std::get<int64_t>(v), 1+2*3+4*5+6*7);

  token_src.set_input("6 == 2 * 3");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), true);

  token_src.set_input("(2 < 1) == (1 == 2)");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), true);

  token_src.set_input("2 < 1 * 3 && 1 + 1 == 2");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), true);
}

TEST(FormatterFilter, Expressions_has) {
  TokenSplitter token_src;
  VariableMap vars;
  std::string error;
  ExpRef exp;
  bool b;
  Exp::Value v;
  token_src.set_input("\"abcdefgh\" has \"def\"");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), true);

  token_src.set_input("\"ab1234cdefgh\" has (2+2)");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), true);

}

TEST(FormatterFilter, Expressions_exists) {
  TokenSplitter token_src;
  VariableMap vars;
  std::string error;
  ExpRef exp;
  bool b;
  Exp::Value v;
  token_src.set_input("exists .var");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), false);

  vars[".var"].reset(new string(""));
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), true);

  exp.reset();
  vars.clear();
  token_src.set_input("1 + .var");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::einv);

  token_src.set_input("exists (1 + .var)");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), false);

  vars[".var"].reset(new string("aaa"));
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), false);

  vars[".var"].reset(new string("1"));
  v = exp->get_value();
  ASSERT_EQ(v.index(), Exp::ebool);
  ASSERT_EQ(std::get<bool>(v), true);
}

TEST(FormatterFilter, Expressions_all_ops) {
  TokenSplitter token_src;
  VariableMap vars;
  std::string error;
  ExpRef exp;
  bool b;
  Exp::Value v;
  token_src.set_input("1 == 2 != 3 >= 4 <= 5 > 6 < 7 && 8 || 9 has exists 10 + 11 - 12 * 13 / 14 % !15");
  b = parse(token_src, vars, error, &exp);
  if (!b) token_src.show_error(std::cout, error);
  ASSERT_EQ(b, true);
  v = exp->get_value();
}

TEST(FormatterFilter, basic) {
  ostringstream oss;
  std::string error;
  Filter* ff = Filter::create();
  bool b = ff->setup(".foo.bar.int == 111", error);
  ASSERT_EQ(b, true);
  JSONFormatter fmt(false);
  bool matched = false;
  Formatter* f = ff->attach(&fmt, [&matched]() { matched = true;} );
  f->open_object_section("foo");
  f->open_object_section("bar");
  f->dump_int("int", 111);
  f->dump_unsigned("unsigned", 0x8000000000000001llu);
  f->dump_float("float", 1.234);
  f->close_section();
  f->dump_string("string", "str");
  f->close_section();
  f->flush(oss);
  ASSERT_EQ(matched, true);
  ff->detach();
  ASSERT_EQ(oss.str(), "{\"bar\":{\"int\":111,\
\"unsigned\":9223372036854775809,\"float\":1.234},\
\"string\":\"str\"}");
  delete ff;
}

TEST(BufferingFilter, basic) {
  ostringstream oss;
  JSONFormatter fmt(false);
  std::unique_ptr<Buffer> ff(Buffer::create());
  Formatter* f = ff->attach(&fmt);
  f->open_object_section("foo");
  f->open_object_section("bar");
  f->dump_int("int", 0xf00000000000ll);
  f->dump_unsigned("unsigned", 0x8000000000000001llu);
  f->dump_float("float", 1.234);
  f->close_section();
  f->dump_string("string", "str");
  f->close_section();
  ff->unbuffer();
  f->flush(oss);
  ff->detach();
  ASSERT_EQ(oss.str(), "{\"bar\":{\"int\":263882790666240,\
\"unsigned\":9223372036854775809,\"float\":1.234},\
\"string\":\"str\"}");
};

TEST(FilterTrimmer, basic_1) {
  ostringstream oss;
  JSONFormatter fmt(false);
  std::string error;
  std::unique_ptr<ceph::Trimmer> trimmer(ceph::Trimmer::create());
  bool b = trimmer->setup("-foo.bar.float", error);
  ASSERT_EQ(b, true);
  Formatter* f(trimmer->attach(&fmt));
  f->open_object_section("foo");
  f->open_object_section("bar");
  f->dump_int("int", 0xf00000000000ll);
  f->dump_unsigned("unsigned", 0x8000000000000001llu);
  f->dump_float("float", 1.234);
  f->close_section();
  f->dump_string("string", "str");
  f->close_section();
  f->flush(oss);
  trimmer->detach();
  ASSERT_EQ(oss.str(), "{\"bar\":{\"int\":263882790666240,\
\"unsigned\":9223372036854775809},\
\"string\":\"str\"}");
};

TEST(FilterTrimmer, basic_2) {
  ostringstream oss;
  XMLFormatter fmt(false);
  std::string error;
  std::unique_ptr<ceph::Trimmer> trimmer(ceph::Trimmer::create());
  bool b = trimmer->setup("-foo,+foo.bar.float", error);
  ASSERT_EQ(b, true);
  Formatter* f(trimmer->attach(&fmt));
  f->open_object_section("foo");
  f->open_object_section("bar");
  f->dump_int("int", 0xf00000000000ll);
  f->dump_unsigned("unsigned", 0x8000000000000001llu);
  f->dump_float("float", 1.234);
  f->close_section();
  f->dump_string("string", "str");
  f->close_section();
  f->flush(oss);
  ASSERT_EQ(oss.str(), "<foo><bar><float>1.234</float></bar></foo>");
};



/*
1. make random tree A
2. make +/- rule R that matches this tree
3. make tree B by interpreting rule R - copying parts A to B or deleting from B
4. stream tree A through filter R
5. stream tree B
6. 4 and 5 must match
 */
TEST(FilterTrimmer, big) {
  boost::rand48 rng;

  auto R = [&](size_t min, size_t max) {
    return boost::uniform_int<>(min, max)(rng);
  };

  auto random_id = [&]() {
    std::string n;
    size_t s = R(1,7);
    for (size_t i = 0; i < s; i++) {
      if (R(0,1)) {
	n.push_back(R('a', 'z'));
      } else {
	n.push_back(R('A', 'Z'));
      }
    }
    return n;
  };

  auto unique_random_id = [&](std::set<std::string>& ids) {
    while (true) {
      std::string n = random_id();
      if (ids.count(n) == 0) {
	ids.emplace(n);
	return n;
      }
    }
  };

  struct T {
    std::string value; //if value!="" branches must be empty
    std::map<std::string, T> branches;
    bool support_only = false;
    void dump(size_t depth) const {
      if (value != "") {
	std::cout << std::string(depth * 2, ' ') << ":" << value << std::endl;
      } else {
	for (auto& t : branches) {
	  std::cout << std::string(depth * 2, ' ') <<
	    "(" << t.second.support_only << ")" << t.first << std::endl;
	  t.second.dump(depth + 1);
	}
      }
    }
    void set_support(bool b) {
      support_only = b;
      for (auto& t : branches) {
	t.second.set_support(b);
      }
    }
    bool prune_support() {
      bool b = support_only;
      for (auto it = branches.begin(); it != branches.end(); ) {
	bool b1 = it->second.prune_support();
	if (b1 == true) {
	  it = branches.erase(it);
	} else {
	  it ++;
	}
	b = b && b1;
      }
      return b;
    }
  };

  std::set<std::string> used_ids;

  std::function<void(size_t, T&)>
  produce_tree = [&](size_t depth, T& t) {
    ssize_t elems = depth == 0 ? 1 : 6 - depth * 2 + R(0, 4);
    bool is_leaf = depth < 2 ? false : R(0, 3) == 0;

    if (is_leaf) {
      t.value = random_id();
      return;
    }
    for (ssize_t i = 0; i < elems; i++) {
      std::string branch = unique_random_id(used_ids);
      T& x = t.branches[branch];
      produce_tree(depth + 1, x);
    }
  };

  std::function<void(const T&, std::vector<std::string>& path)>
  select_path = [&](const T& t, std::vector<std::string>& path) {
    std::string s;
    if (R(0,3) > 0) {
      size_t br = t.branches.size();
      if (br != 0) {
	size_t pos = R(0, br - 1);
	auto it = t.branches.begin();
	while (pos > 0) {
	  it++;
	  pos--;
	};
	path.push_back(it->first);
	select_path(it->second, path);
      }
    }
  };

  auto copy = [&](const T& src, T& dst, const std::vector<std::string>& path) {
    const T* s = &src;
    T* d = &dst;
    //first check if anything gets copied
    bool matched = true;
    for (size_t i = 0; i < path.size(); i++) {
      auto it = s->branches.find(path[i]);
      if (it == s->branches.end()) {
	matched = false;
	break;
      }
      s = &it->second;
    }
    if (!matched) {
      return;
    }
    s = &src;
    for (auto& p : path) {
      auto it = s->branches.find(p);
      ceph_assert(it != s->branches.end());
      auto itd = d->branches.find(p);
      if (itd == d->branches.end()) {
	d = &d->branches[p];
	d->support_only = true;
      } else {
	d = &d->branches[p];
      }
      s = &it->second;
    }
    *d = *s;
    d->set_support(false);
  };

  auto cut = [&](T& dst, const std::vector<std::string>& path) {
    T* d = &dst;
    if (path.size() == 0) {
      d->branches.clear();
    }
    for (size_t i = 0; i < path.size(); i++) {
      auto it = d->branches.find(path[i]);
      if (it == d->branches.end()) {
	break;
      }
      if (i == path.size() - 1) {
	d->branches.erase(it);
	break;
      }
      d = &it->second;
    }
  };

  std::function<void(const T&, Formatter*)>
  print_tree = [&](const T& t, Formatter* f) {
    for (auto const& [n, b]: t.branches) {
      if (b.value != "") {
	f->dump_string(n.c_str(), b.value);
      } else {
	f->open_object_section(n.c_str());
	print_tree(b, f);
	f->close_section();
      }
    }
  };

  for (size_t i = 0; i < 10000; i++) {
    T t;
    T manual;
    produce_tree(0,t);
    //default +
    manual = t;
    size_t rct = R(1,5);
    std::string rule;
    for (size_t j = 0; j < rct; j++) {
      std::vector<std::string> path;
      select_path(t, path);

      std::string r;
      for (auto& x: path) {
	r = r + "." + x;
      }
      if (!rule.empty()) {
	rule += ",";
      }

      if (R(0, 2) > 0) {
	copy(t, manual, path);
	rule += "+" + r;
      } else {
	cut(manual, path);
	rule += "-" + r;
      }
    }
    manual.prune_support();
    std::unique_ptr<Formatter> f_actual(new XMLFormatter(false));
    ASSERT_NE(f_actual.get(), nullptr);
    print_tree(manual, &*f_actual);
    stringstream manual_ss;
    f_actual->flush(manual_ss);
    std::unique_ptr<ceph::Trimmer> trimmer(ceph::Trimmer::create());
    std::string error;
    bool b = trimmer->setup(rule, error);
    ASSERT_EQ(b, true);
    Formatter* f = trimmer->attach(f_actual.get());
    ASSERT_NE(f, nullptr);
    print_tree(t, f);
    stringstream trimmed_ss;
    f->flush(trimmed_ss);
    EXPECT_EQ(manual_ss.str(), trimmed_ss.str());
    trimmer->detach();
  }
};
