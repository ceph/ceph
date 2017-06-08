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

ostream& operator<<(ostream& out, osd_rwxa_t p)
{ 
  if (p == OSD_CAP_ANY)
    return out << "*";

  if (p & OSD_CAP_R)
    out << "r";
  if (p & OSD_CAP_W)
    out << "w";
  if ((p & OSD_CAP_X) == OSD_CAP_X) {
    out << "x";
  } else {
    if (p & OSD_CAP_CLS_R)
      out << " class-read";
    if (p & OSD_CAP_CLS_W)
      out << " class-write";
  }
  return out;
}

ostream& operator<<(ostream& out, const OSDCapSpec& s)
{
  if (s.allow)
    return out << s.allow;
  if (s.class_name.length())
    return out << "class '" << s.class_name << "' '" << s.class_allow << "'";
  return out;
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
  if (m.is_nspace) {
    out << "namespace ";
    if (m.nspace.length() == 0)
      out << "\"\"";
    else
      out << m.nspace;
    out << " ";
  }
  return out;
}

bool OSDCapMatch::is_match(const string& pn, const string& ns, int64_t pool_auid, const string& object) const
{
  if (auid >= 0) {
    if (auid != pool_auid)
      return false;
  }
  if (pool_name.length()) {
    if (pool_name != pn)
      return false;
  }
  if (is_nspace) {
    if (nspace != ns)
      return false;
  }
  if (object_prefix.length()) {
    if (object.find(object_prefix) != 0)
      return false;
  }
  return true;
}

bool OSDCapMatch::is_match_all() const
{
  if (auid >= 0)
    return false;
  if (pool_name.length())
    return false;
  if (is_nspace)
    return false;
  if (object_prefix.length())
    return false;
  return true;
}

ostream& operator<<(ostream& out, const OSDCapGrant& g)
{
  return out << "grant(" << g.match << g.spec << ")";
}


bool OSDCap::allow_all() const
{
  for (vector<OSDCapGrant>::const_iterator p = grants.begin(); p != grants.end(); ++p)
    if (p->match.is_match_all() && p->spec.allow_all())
      return true;
  return false;
}

void OSDCap::set_allow_all()
{
  grants.clear();
  grants.push_back(OSDCapGrant(OSDCapMatch(), OSDCapSpec(OSD_CAP_ANY)));
}

bool OSDCap::is_capable(const string& pool_name, const string& ns, int64_t pool_auid,
			const string& object, bool op_may_read,
			bool op_may_write, bool op_may_class_read,
			bool op_may_class_write) const
{
  osd_rwxa_t allow = 0;
  for (vector<OSDCapGrant>::const_iterator p = grants.begin();
       p != grants.end(); ++p) {
    if (p->match.is_match(pool_name, ns, pool_auid, object)) {
      allow = allow | p->spec.allow;
      if ((op_may_read && !(allow & OSD_CAP_R)) ||
	  (op_may_write && !(allow & OSD_CAP_W)) ||
	  (op_may_class_read && !(allow & OSD_CAP_CLS_R)) ||
	  (op_may_class_write && !(allow & OSD_CAP_CLS_W)))
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
struct OSDCapParser : qi::grammar<Iterator, OSDCap()>
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
    equoted_string %=
      lexeme['"' >> *(char_ - '"') >> '"'] |
      lexeme['\'' >> *(char_ - '\'') >> '\''];
    unquoted_word %= +char_("a-zA-Z0-9_.-");
    str %= quoted_string | unquoted_word;
    estr %= equoted_string | unquoted_word;

    spaces = +ascii::space;


    // match := [pool[=]<poolname> [namespace[=]<namespace>] | auid <123>] [object_prefix <prefix>]
    pool_name %= -(spaces >> lit("pool") >> (lit('=') | spaces) >> str);
    nspace %= (spaces >> lit("namespace") >> (lit('=') | spaces) >> estr);
    auid %= (spaces >> lit("auid") >> spaces >> int_);
    object_prefix %= -(spaces >> lit("object_prefix") >> spaces >> str);

    match = ( (auid >> object_prefix)                 [_val = phoenix::construct<OSDCapMatch>(_1, _2)] |
             (pool_name >> nspace >> object_prefix)   [_val = phoenix::construct<OSDCapMatch>(_1, _2, _3)] |
             (pool_name >> object_prefix)             [_val = phoenix::construct<OSDCapMatch>(_1, _2)]);

    // rwxa := * | [r][w][x] [class-read] [class-write]
    rwxa =
      (spaces >> lit("*")[_val = OSD_CAP_ANY]) |
      ( eps[_val = 0] >>
	(
	 spaces >>
	 ( lit('r')[_val |= OSD_CAP_R] ||
	   lit('w')[_val |= OSD_CAP_W] ||
	   lit('x')[_val |= OSD_CAP_X] )) ||
	( (spaces >> lit("class-read")[_val |= OSD_CAP_CLS_R]) ||
	  (spaces >> lit("class-write")[_val |= OSD_CAP_CLS_W]) ));

    // capspec := * | rwx | class <name> [classcap]
    capspec =
      rwxa                                            [_val = phoenix::construct<OSDCapSpec>(_1)] |
      ( spaces >> lit("class") >> spaces >> ((str >> spaces >> str)   [_val = phoenix::construct<OSDCapSpec>(_1, _2)] |
			 str                          [_val = phoenix::construct<OSDCapSpec>(_1, string())] ));

    // grant := allow match capspec
    grant = (*ascii::blank >> lit("allow") >>
	     ((capspec >> match)       [_val = phoenix::construct<OSDCapGrant>(_2, _1)] |
	      (match >> capspec)       [_val = phoenix::construct<OSDCapGrant>(_1, _2)]) >>
	     *ascii::blank);
    // osdcap := grant [grant ...]
    grants %= (grant % (lit(';') | lit(',')));
    osdcap = grants  [_val = phoenix::construct<OSDCap>(_1)]; 
  }
  qi::rule<Iterator> spaces;
  qi::rule<Iterator, unsigned()> rwxa;
  qi::rule<Iterator, string()> quoted_string, equoted_string;
  qi::rule<Iterator, string()> unquoted_word;
  qi::rule<Iterator, string()> str, estr;
  qi::rule<Iterator, int()> auid;
  qi::rule<Iterator, OSDCapSpec()> capspec;
  qi::rule<Iterator, string()> pool_name;
  qi::rule<Iterator, string()> nspace;
  qi::rule<Iterator, string()> object_prefix;
  qi::rule<Iterator, OSDCapMatch()> match;
  qi::rule<Iterator, OSDCapGrant()> grant;
  qi::rule<Iterator, std::vector<OSDCapGrant>()> grants;
  qi::rule<Iterator, OSDCap()> osdcap;
};

bool OSDCap::parse(const string& str, ostream *err)
{
  OSDCapParser<string::const_iterator> g;
  string::const_iterator iter = str.begin();
  string::const_iterator end = str.end();

  bool r = qi::phrase_parse(iter, end, g, ascii::space, *this);
  if (r && iter == end)
    return true;

  // Make sure no grants are kept after parsing failed!
  grants.clear();

  if (err)
    *err << "osdcap parse failed, stopped at '" << std::string(iter, end)
	 << "' of '" << str << "'\n";

  return false; 
}

