// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix.hpp>

#include "OSDCap.h"
#include "common/config.h"
#include "common/debug.h"

using std::ostream;
using std::vector;

ostream& operator<<(ostream& out, rwxa_t p)
{ 
  if (p & OSD_CAP_ANY)
    return out << "*";

  if (p & OSD_CAP_R)
    out << "r";
  if (p & OSD_CAP_W)
    out << "w";
  if (p & OSD_CAP_X)
    out << "x";
  return out;
}

ostream& operator<<(ostream& out, const OSDCapSpec& s)
{
  if (s.allow & OSD_CAP_ANY)
    return out << "*";
  if (s.allow) {
    if (s.allow & OSD_CAP_R)
      out << 'r';
    if (s.allow & OSD_CAP_W)
      out << 'w';
    if (s.allow & OSD_CAP_X)
      out << 'x';
    return out;
  }
  if (s.class_name.length())
    return out << "class '" << s.class_name << "' '" << s.class_allow << "'";
  return out << s.allow;
}

ostream& operator<<(ostream& out, const OSDCapMatch& m)
{
  if (m.auid != -1LL) {
    out << "auid " << m.auid << " ";
  }
  if (m.object_prefix.length()) {
    out << "object_prefix " << m.object_prefix << " ";
  }
  if (m.pool_name.length()) {
    out << "pool " << m.pool_name << " ";
  }
  return out;
}

bool OSDCapMatch::is_match(const string& pn, int64_t pool_auid, const string& object) const
{
  if (auid >= 0) {
    if (auid != pool_auid)
      return false;
  }
  if (pool_name.length()) {
    if (pool_name != pn)
      return false;
  }
  if (object_prefix.length()) {
    if (object.find(object_prefix) != 0)
      return false;
  }
  return true;
}

ostream& operator<<(ostream& out, const OSDCapGrant& g)
{
  return out << "grant(" << g.match << g.spec << ")";
}


bool OSDCap::allow_all() const
{
  for (vector<OSDCapGrant>::const_iterator p = grants.begin(); p != grants.end(); ++p)
    if (p->spec.allow_all())
      return true;
  return false;
}

void OSDCap::set_allow_all()
{
  grants.clear();
  grants.push_back(OSDCapGrant(OSDCapMatch(), OSDCapSpec(OSD_CAP_ANY)));
}

bool OSDCap::is_capable(const string& pool_name, int64_t pool_auid, const string& object,
			bool op_may_read, bool op_may_write, bool op_may_exec) const
{
  rwxa_t allow = 0;
  for (vector<OSDCapGrant>::const_iterator p = grants.begin(); p != grants.end(); ++p) {
    if (p->match.is_match(pool_name, pool_auid, object)) {
      allow |= p->spec.allow;
      if (op_may_read && !(allow & OSD_CAP_R))
	continue;
      if (op_may_write && !(allow & OSD_CAP_W))
	continue;
      if (op_may_exec && !(allow & OSD_CAP_X))
	continue;
      return true;
    }
  }
  return false;
}


// grammar
namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;
namespace phoenix = boost::phoenix;

template <typename Iterator>
struct OSDCapParser : qi::grammar<Iterator, OSDCap(), ascii::space_type>
{
  OSDCapParser() : OSDCapParser::base_type(osdcap)
  {
    using qi::char_;
    using qi::int_;
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
    unquoted_word %= +(alnum | '_' | '-');
    str %= quoted_string | unquoted_word;

    // match := [pool <poolname> | uid <123>] [object_prefix <prefix>]
    pool_name %= -(lit("pool") >> str);
    object_prefix %= -(lit("object_prefix") >> str);
    match = ( (lit("auid") >> int_ >> object_prefix)   [_val = phoenix::construct<OSDCapMatch>(_1, _2)] |
	      (pool_name >> object_prefix)             [_val = phoenix::construct<OSDCapMatch>(_1, _2)]);
	      

    // rwxa := * | [r][w][x]
    rwxa =
      lit('*')[_val = OSD_CAP_ANY] |
      ( eps[_val = 0] >>
	( lit('r')[_val |= OSD_CAP_R] ||
	  lit('w')[_val |= OSD_CAP_W] ||
	  lit('x')[_val |= OSD_CAP_X] ));

    // capspec := * | rwx | class <name> [classcap]
    capspec =
      rwxa                                            [_val = phoenix::construct<OSDCapSpec>(_1)] |
      ( lit("class") >> ((str >> str)                 [_val = phoenix::construct<OSDCapSpec>(_1, _2)] |
			 str                          [_val = phoenix::construct<OSDCapSpec>(_1, string())] ));

    // grant := allow match capspec
    grant = lit("allow") >> ((capspec >> match)       [_val = phoenix::construct<OSDCapGrant>(_2, _1)] |
			     (match >> capspec)       [_val = phoenix::construct<OSDCapGrant>(_1, _2)]);

    // osdcap := grant [grant ...]
    grants %= (grant % (lit(';') | lit(',')));
    osdcap = grants  [_val = phoenix::construct<OSDCap>(_1)]; 
  }
  qi::rule<Iterator, unsigned(), ascii::space_type> rwxa;
  qi::rule<Iterator, string(), ascii::space_type> quoted_string;
  qi::rule<Iterator, string()> unquoted_word;
  qi::rule<Iterator, string(), ascii::space_type> str;
  qi::rule<Iterator, OSDCapSpec(), ascii::space_type> capspec;
  qi::rule<Iterator, string(), ascii::space_type> pool_name;
  qi::rule<Iterator, string(), ascii::space_type> object_prefix;
  qi::rule<Iterator, OSDCapMatch(), ascii::space_type> match;
  qi::rule<Iterator, OSDCapGrant(), ascii::space_type> grant;
  qi::rule<Iterator, std::vector<OSDCapGrant>(), ascii::space_type> grants;
  qi::rule<Iterator, OSDCap(), ascii::space_type> osdcap;
};

bool OSDCap::parse(const string& str, ostream *err)
{
  OSDCapParser<string::const_iterator> g;
  string::const_iterator iter = str.begin();
  string::const_iterator end = str.end();

  bool r = qi::phrase_parse(iter, end, g, ascii::space, *this);
  if (r && iter == end)
    return true;

  if (err)
    *err << "osdcap parse failed, stopped at '" << std::string(iter, end)
	 << "' of '" << str << "'\n";

  return false; 
}

