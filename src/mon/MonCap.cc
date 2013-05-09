// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/fusion/include/std_pair.hpp>
#include <boost/spirit/include/phoenix.hpp>
#include <boost/spirit/include/qi_uint.hpp>

#include "MonCap.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/Formatter.h"

using std::ostream;
using std::vector;

ostream& operator<<(ostream& out, mon_rwxa_t p)
{ 
  if (p == MON_CAP_ANY)
    return out << "*";

  if (p & MON_CAP_R)
    out << "r";
  if (p & MON_CAP_W)
    out << "w";
  if (p & MON_CAP_X)
    out << "x";
  return out;
}

ostream& operator<<(ostream& out, const MonCapSpec& s)
{
  return out << s.allow;
}

ostream& operator<<(ostream& out, const MonCapMatch& m)
{
  if (m.service.length()) {
    out << "service " << m.service << " ";
  }
  if (m.command.length()) {
    out << "command " << m.command << " ";
  }
  return out;
}

bool MonCapMatch::is_match(const std::string& s, const std::string& c,
			   const map<string,string>& c_args) const
{
  if (service.length()) {
    if (service != s)
      return false;
  }
  if (command.length()) {
    if (command != c)
      return false;
    for (map<string,string>::const_iterator p = command_args.begin(); p != command_args.end(); ++p) {
      map<string,string>::const_iterator q = c_args.find(p->first);
      if (q == c_args.end() || q->second != p->second)
	return false;
    }
  }
  return true;
}

ostream& operator<<(ostream& out, const MonCapGrant& g)
{
  return out << "grant(" << g.match << g.spec << ")";
}


bool MonCap::allow_all() const
{
  map<string,string> empty;
  for (vector<MonCapGrant>::const_iterator p = grants.begin(); p != grants.end(); ++p)
    if (p->match.is_match(string(), string(), empty) && p->spec.allow_all())
      return true;
  return false;
}

void MonCap::set_allow_all()
{
  grants.clear();
  grants.push_back(MonCapGrant(MonCapMatch(), MonCapSpec(MON_CAP_ANY)));
}

bool MonCap::is_capable(const string& service,
			const string& command, const map<string,string>& command_args,
			bool op_may_read, bool op_may_write, bool op_may_exec) const
{
  mon_rwxa_t allow = 0;
  for (vector<MonCapGrant>::const_iterator p = grants.begin();
       p != grants.end(); ++p) {
    if (p->match.is_match(service, command, command_args)) {
      allow = allow | p->spec.allow;
      if ((op_may_read && !(allow & MON_CAP_R)) ||
	  (op_may_write && !(allow & MON_CAP_W)) ||
	  (op_may_exec && !(allow & MON_CAP_X)))
	continue;
      return true;
    }
  }
  return false;
}

void MonCap::encode(bufferlist& bl) const
{
  ENCODE_START(4, 4, bl);   // legacy MonCaps was 3, 3
  ::encode(text, bl);
  ENCODE_FINISH(bl);
}

void MonCap::decode(bufferlist::iterator& bl)
{
  string s;
  DECODE_START(4, bl);
  ::decode(s, bl);
  DECODE_FINISH(bl);
  parse(s, NULL);
}

void MonCap::dump(Formatter *f) const
{
  f->dump_string("text", text);
}

void MonCap::generate_test_instances(list<MonCap*>& ls)
{
  ls.push_back(new MonCap);
  ls.push_back(new MonCap);
  ls.back()->parse("allow *");
  ls.push_back(new MonCap);
  ls.back()->parse("allow rwx");
  ls.push_back(new MonCap);
  ls.back()->parse("allow service foo x");
  ls.push_back(new MonCap);
  ls.back()->parse("allow command bar x");
  ls.push_back(new MonCap);
  ls.back()->parse("allow service foo r, allow command bar x");
  ls.push_back(new MonCap);
  ls.back()->parse("allow command bar with k1=v1 x");
  ls.push_back(new MonCap);
  ls.back()->parse("allow command bar with k1=v1 k2=v2 x");
}

// grammar
namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;
namespace phoenix = boost::phoenix;

template <typename Iterator>
struct MonCapParser : qi::grammar<Iterator, MonCap()>
{
  MonCapParser() : MonCapParser::base_type(moncap)
  {
    using qi::char_;
    using qi::int_;
    using qi::ulong_long;
    using qi::lexeme;
    using qi::alnum;
    using qi::_val;
    using qi::_1;
    using qi::_2;
    using qi::_3;
    using qi::eps;
    using qi::lit;

    quoted_string %=
      lexeme['"' >> +(char_ - '"') >> '"'] | 
      lexeme['\'' >> +(char_ - '\'') >> '\''];
    unquoted_word %= +char_("a-zA-Z0-9_-");
    str %= quoted_string | unquoted_word;

    spaces = +lit(' ');

    // command := command[=]cmd [k1=v1 k2=v2 ...]
    kv_pair = str >> '=' >> str;
    kv_map %= kv_pair >> *(spaces >> kv_pair);
    command_match =
      (-spaces >> lit("command") >> (lit('=') | spaces) >> str
       >> spaces >> lit("with") >> spaces >> kv_map)
             [_val = phoenix::construct<MonCapMatch>(_1, _2)] ||
      (-spaces >> lit("command") >> (lit('=') | spaces) >> str)
             [_val = phoenix::construct<MonCapMatch>(_1, map<string,string>())];

    // service
    service_match %= -spaces >> lit("service") >> (lit('=') | spaces) >> str
      [_val = phoenix::construct<MonCapMatch>(_1, string())];

    profile_match %= -spaces >> lit("profile") >> (lit('=') | spaces) >> str
      [_val = phoenix::construct<MonCapMatch>(string(), _1)];

    match = -(command_match || service_match || profile_match);

    // rwxa := * | [r][w][x]
    rwxa =
      (spaces >> lit("*")[_val = MON_CAP_ANY]) |
      ( eps[_val = 0] >>
	(
	 spaces >>
	 ( lit('r')[_val |= MON_CAP_R] ||
	   lit('w')[_val |= MON_CAP_W] ||
	   lit('x')[_val |= MON_CAP_X]
	   )
	 )
	);
	 
    // capspec := * | rwx
    capspec = rwxa                                          [_val = phoenix::construct<MonCapSpec>(_1)];

    // grant := allow match capspec
    grant = (*lit(' ') >> lit("allow") >>
	     ((capspec >> match)       [_val = phoenix::construct<MonCapGrant>(_2, _1)] |
	      (match >> capspec)       [_val = phoenix::construct<MonCapGrant>(_1, _2)]) >>
	     *lit(' '));
    // moncap := grant [grant ...]
    grants %= (grant % (*lit(' ') >> (lit(';') | lit(',')) >> *lit(' ')));
    moncap = grants  [_val = phoenix::construct<MonCap>(_1)]; 
  }
  qi::rule<Iterator> spaces;
  qi::rule<Iterator, unsigned()> rwxa;
  qi::rule<Iterator, string()> quoted_string;
  qi::rule<Iterator, string()> unquoted_word;
  qi::rule<Iterator, string()> str;

  qi::rule<Iterator, pair<string, string>()> kv_pair;
  qi::rule<Iterator, map<string, string>()> kv_map;

  qi::rule<Iterator, MonCapSpec()> capspec;
  qi::rule<Iterator, string()> pool_name;
  qi::rule<Iterator, string()> service_name;
  qi::rule<Iterator, string()> command_name;
  qi::rule<Iterator, MonCapMatch()> command_match;
  qi::rule<Iterator, MonCapMatch()> service_match;
  qi::rule<Iterator, MonCapMatch()> profile_match;
  qi::rule<Iterator, MonCapMatch()> match;
  qi::rule<Iterator, MonCapGrant()> grant;
  qi::rule<Iterator, std::vector<MonCapGrant>()> grants;
  qi::rule<Iterator, MonCap()> moncap;
};

bool MonCap::parse(const string& str, ostream *err)
{
  text = str;
  string::iterator iter = text.begin();
  string::iterator end = text.end();

  MonCapParser<string::iterator> g;
  //MonCapMatch mm;
  //bool r = qi::phrase_parse(iter, end, g, ascii::space, mm);
  bool r = qi::phrase_parse(iter, end, g, ascii::space, *this);
  if (r && iter == end)
    return true;

  // Make sure no grants are kept after parsing failed!
  grants.clear();

  if (err)
    *err << "moncap parse failed, stopped at '" << std::string(iter, end)
	 << "' of '" << str << "'\n";

  return false; 
}

