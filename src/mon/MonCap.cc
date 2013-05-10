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

#define dout_subsys ceph_subsys_mon

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

ostream& operator<<(ostream& out, const MonCapGrant& m)
{
  out << "allow";
  if (m.service.length()) {
    out << " service " << m.service;
  }
  if (m.command.length()) {
    out << " command " << m.command;
    if (m.command_args.size()) {
      out << " with " << m.command_args;
    }
  }
  if (m.profile.length()) {
    out << " profile " << m.profile;
  }
  if (m.allow != 0)
    out << " " << m.allow;
  return out;
}

bool MonCapGrant::is_match(const std::string& s, const std::string& c,
			   const map<string,string>& c_args) const
{
  if (profile.length())
    return false;
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

ostream& operator<<(ostream&out, const MonCap& m)
{
  for (vector<MonCapGrant>::const_iterator p = m.grants.begin(); p != m.grants.end(); ++p) {
    if (p != m.grants.begin())
      out << ", ";
    out << *p;
  }
  return out;
}

bool MonCap::is_allow_all() const
{
  for (vector<MonCapGrant>::const_iterator p = grants.begin(); p != grants.end(); ++p)
    if (p->is_allow_all())
      return true;
  return false;
}

void MonCap::set_allow_all()
{
  grants.clear();
  grants.push_back(MonCapGrant(MON_CAP_ANY));
}

bool MonCap::is_capable(CephContext *cct,
			const string& service,
			const string& command, const map<string,string>& command_args,
			bool op_may_read, bool op_may_write, bool op_may_exec) const
{
  if (cct)
    ldout(cct, 20) << "is_capable service=" << service << " command=" << command
		   << (op_may_read ? " read":"")
		   << (op_may_write ? " write":"")
		   << (op_may_exec ? " exec":"")
		   << " on cap " << *this
		   << dendl;
  mon_rwxa_t allow = 0;
  for (vector<MonCapGrant>::const_iterator p = grants.begin();
       p != grants.end(); ++p) {
    if (cct)
      ldout(cct, 20) << " allow so far " << allow << ", doing grant " << *p << dendl;

    if (p->is_allow_all())
      return true;

    // check enumerated caps
    if (p->is_match(service, command, command_args)) {
      allow = allow | p->allow;
      if (!((op_may_read && !(allow & MON_CAP_R)) ||
	    (op_may_write && !(allow & MON_CAP_W)) ||
	    (op_may_exec && !(allow & MON_CAP_X))))
	return true;
    }

    // match against profiles
    if (p->profile == "osd" || p->profile == "mds" || p->profile == "mon") {
      // daemons can log
      if (service == "log")
	return true;
      // everyone can read mon, osd maps
      if ((service == "mon" || service == "osd" || service == "pg") &&
	  !op_may_write && !op_may_exec)
	return true;
    }
    if (p->profile == "mds") {
      if (service == "mds")      // FIXME, this needs some refinement.
	return true;
    }
    if (p->profile == "osd") {
      if (service == "osd")      // FIXME, this needs some refinement.
	return true;
    }
    if (p->profile == "mon") {
      if (service == "mon")
	return true;
    }
    if (p->profile == "fs-client") {
      // can read mon, osd, mds maps
      if ((service == "mon" || service == "osd" || service == "mds" || service == "pg") &&
	  !op_may_write && !op_may_exec)
	return true;
    }
    if (p->profile == "simple-rados-client") {
      // can read mon, osd maps
      if ((service == "mon" || service == "osd" || service == "pg") &&
	  !op_may_write && !op_may_exec)
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
      (-spaces >> lit("allow") >> spaces >> lit("command") >> (lit('=') | spaces) >> str
       >> spaces >> lit("with") >> spaces >> kv_map)
             [_val = phoenix::construct<MonCapGrant>(_1, _2)] ||
      (-spaces >> lit("allow") >> spaces >> lit("command") >> (lit('=') | spaces) >> str)
             [_val = phoenix::construct<MonCapGrant>(_1, map<string,string>())];

    // service
    service_match %= -spaces >> lit("allow") >> spaces >> lit("service") >> (lit('=') | spaces) >>
      (str >> spaces >> rwxa)[_val = phoenix::construct<MonCapGrant>(_1, _2)];

    profile_match = -spaces >> lit("allow") >> spaces >> lit("profile") >> (lit('=') | spaces) >> str
      [_val = phoenix::construct<MonCapGrant>(_1)];

    rwxa_match %= -spaces >> lit("allow") >> spaces >> rwxa   [_val = phoenix::construct<MonCapGrant>(_1)];

    // rwxa := * | [r][w][x]
    rwxa =
      (lit("*")[_val = MON_CAP_ANY]) |
      ( eps[_val = 0] >>
	( lit('r')[_val |= MON_CAP_R] ||
	  lit('w')[_val |= MON_CAP_W] ||
	  lit('x')[_val |= MON_CAP_X]
	  )
	);

    // grant := allow ...
    grant = profile_match | rwxa_match | command_match | service_match;

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

  qi::rule<Iterator, MonCapGrant()> rwxa_match;
  qi::rule<Iterator, MonCapGrant()> command_match;
  qi::rule<Iterator, MonCapGrant()> service_match;
  qi::rule<Iterator, MonCapGrant()> profile_match;
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
  bool r = qi::phrase_parse(iter, end, g, ascii::space, *this);
  //MonCapGrant foo;
  //bool r = qi::phrase_parse(iter, end, g, ascii::space, foo);
  if (r && iter == end)
    return true;

  // Make sure no grants are kept after parsing failed!
  grants.clear();

  if (err)
    *err << "moncap parse failed, stopped at '" << std::string(iter, end)
	 << "' of '" << str << "'\n";

  return false; 
}

