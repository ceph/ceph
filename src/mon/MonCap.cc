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
#include <boost/spirit/include/qi_uint.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/fusion/include/std_pair.hpp>
#include <boost/spirit/include/phoenix.hpp>
#include <boost/fusion/adapted/struct/adapt_struct.hpp>
#include <boost/fusion/include/adapt_struct.hpp>

#include "MonCap.h"
#include "include/stringify.h"
#include "include/ipaddr.h"
#include "common/debug.h"
#include "common/Formatter.h"

#include <algorithm>
#include <regex>

#include "include/ceph_assert.h"

using std::list;
using std::map;
using std::ostream;
using std::pair;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::Formatter;

static inline bool is_not_alnum_space(char c)
{
  return !(isalpha(c) || isdigit(c) || (c == '-') || (c == '_'));
}

static std::string maybe_quote_string(const std::string& str)
{
  if (find_if(str.begin(), str.end(), is_not_alnum_space) == str.end())
    return str;
  return string("\"") + str + string("\"");
}

#define dout_subsys ceph_subsys_mon

ostream& operator<<(ostream& out, const mon_rwxa_t& p)
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

ostream& operator<<(ostream& out, const StringConstraint& c)
{
  switch (c.match_type) {
  case StringConstraint::MATCH_TYPE_EQUAL:
    return out << "value " << c.value;
  case StringConstraint::MATCH_TYPE_PREFIX:
    return out << "prefix " << c.value;
  case StringConstraint::MATCH_TYPE_REGEX:
    return out << "regex " << c.value;
  default:
    break;
  }
  return out;
}

ostream& operator<<(ostream& out, const MonCapGrant& m)
{
  out << "allow";
  if (m.service.length()) {
    out << " service " << maybe_quote_string(m.service);
  }
  if (m.command.length()) {
    out << " command " << maybe_quote_string(m.command);
    if (!m.command_args.empty()) {
      out << " with";
      for (auto p = m.command_args.begin();
	   p != m.command_args.end();
	   ++p) {
        switch (p->second.match_type) {
        case StringConstraint::MATCH_TYPE_EQUAL:
	  out << " " << maybe_quote_string(p->first) << "="
              << maybe_quote_string(p->second.value);
          break;
        case StringConstraint::MATCH_TYPE_PREFIX:
	  out << " " << maybe_quote_string(p->first) << " prefix "
              << maybe_quote_string(p->second.value);
          break;
        case StringConstraint::MATCH_TYPE_REGEX:
	  out << " " << maybe_quote_string(p->first) << " regex "
              << maybe_quote_string(p->second.value);
          break;
        default:
          break;
        }
      }
    }
  }
  if (m.profile.length()) {
    out << " profile " << maybe_quote_string(m.profile);
  }
  if (m.allow != 0)
    out << " " << m.allow;
  if (m.network.size())
    out << " network " << m.network;
  return out;
}


// <magic>
//  fusion lets us easily populate structs via the qi parser.

typedef map<string,StringConstraint> kvmap;

BOOST_FUSION_ADAPT_STRUCT(MonCapGrant,
			  (std::string, service)
			  (std::string, profile)
			  (std::string, command)
			  (kvmap, command_args)
			  (mon_rwxa_t, allow)
			  (std::string, network))

BOOST_FUSION_ADAPT_STRUCT(StringConstraint,
                          (StringConstraint::MatchType, match_type)
			  (std::string, value))

// </magic>

void MonCapGrant::parse_network()
{
  network_valid = ::parse_network(network.c_str(), &network_parsed,
				  &network_prefix);
}

void MonCapGrant::expand_profile(int daemon_type, const EntityName& name) const
{
  // only generate this list once
  if (!profile_grants.empty())
    return;

  if (profile == "read-only") {
    // grants READ-ONLY caps monitor-wide
    // 'auth' requires MON_CAP_X even for RO, which we do not grant here.
    profile_grants.push_back(mon_rwxa_t(MON_CAP_R));
    return;
  }

  if (profile == "read-write") {
    // grants READ-WRITE caps monitor-wide
    // 'auth' requires MON_CAP_X for all operations, which we do not grant.
    profile_grants.push_back(mon_rwxa_t(MON_CAP_R | MON_CAP_W));
    return;
  }

  switch (daemon_type) {
  case CEPH_ENTITY_TYPE_MON:
    expand_profile_mon(name);
    return;
  case CEPH_ENTITY_TYPE_MGR:
    expand_profile_mgr(name);
    return;
  }
}

void MonCapGrant::expand_profile_mgr(const EntityName& name) const
{
}

void MonCapGrant::expand_profile_mon(const EntityName& name) const
{
  if (profile == "mon") {
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_ALL));
    profile_grants.push_back(MonCapGrant("log", MON_CAP_ALL));
  }
  if (profile == "osd") {
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_ALL));
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("pg", MON_CAP_R | MON_CAP_W));
    profile_grants.push_back(MonCapGrant("log", MON_CAP_W));
  }
  if (profile == "mds") {
    profile_grants.push_back(MonCapGrant("mds", MON_CAP_ALL));
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_R));
    // This command grant is checked explicitly in MRemoveSnaps handling
    profile_grants.push_back(MonCapGrant("osd pool rmsnap"));
    profile_grants.push_back(MonCapGrant("osd blacklist"));
    profile_grants.push_back(MonCapGrant("log", MON_CAP_W));
  }
  if (profile == "mgr") {
    profile_grants.push_back(MonCapGrant("mgr", MON_CAP_ALL));
    profile_grants.push_back(MonCapGrant("log", MON_CAP_R | MON_CAP_W));
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R | MON_CAP_W));
    profile_grants.push_back(MonCapGrant("mds", MON_CAP_R | MON_CAP_W));
    profile_grants.push_back(MonCapGrant("fs", MON_CAP_R | MON_CAP_W));
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_R | MON_CAP_W));
    profile_grants.push_back(MonCapGrant("auth", MON_CAP_R | MON_CAP_X));
    profile_grants.push_back(MonCapGrant("config-key", MON_CAP_R | MON_CAP_W));
    profile_grants.push_back(MonCapGrant("config", MON_CAP_R | MON_CAP_W));
  }
  if (profile == "osd" || profile == "mds" || profile == "mon" ||
      profile == "mgr") {
    StringConstraint constraint(StringConstraint::MATCH_TYPE_PREFIX,
                                string("daemon-private/") + stringify(name) +
                                string("/"));
    std::string prefix = string("daemon-private/") + stringify(name) + string("/");
    profile_grants.push_back(MonCapGrant("config-key get", "key", constraint));
    profile_grants.push_back(MonCapGrant("config-key put", "key", constraint));
    profile_grants.push_back(MonCapGrant("config-key set", "key", constraint));
    profile_grants.push_back(MonCapGrant("config-key exists", "key", constraint));
    profile_grants.push_back(MonCapGrant("config-key delete", "key", constraint));
  }
  if (profile == "bootstrap-osd") {
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));  // read monmap
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_R));  // read osdmap
    profile_grants.push_back(MonCapGrant("mon getmap"));
    profile_grants.push_back(MonCapGrant("osd new"));
    profile_grants.push_back(MonCapGrant("osd purge-new"));
  }
  if (profile == "bootstrap-mds") {
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));  // read monmap
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_R));  // read osdmap
    profile_grants.push_back(MonCapGrant("mon getmap"));
    profile_grants.push_back(MonCapGrant("auth get-or-create"));  // FIXME: this can expose other mds keys
    profile_grants.back().command_args["entity"] = StringConstraint(
      StringConstraint::MATCH_TYPE_PREFIX, "mds.");
    profile_grants.back().command_args["caps_mon"] = StringConstraint(
      StringConstraint::MATCH_TYPE_EQUAL, "allow profile mds");
    profile_grants.back().command_args["caps_osd"] = StringConstraint(
      StringConstraint::MATCH_TYPE_EQUAL, "allow rwx");
    profile_grants.back().command_args["caps_mds"] = StringConstraint(
      StringConstraint::MATCH_TYPE_EQUAL, "allow");
  }
  if (profile == "bootstrap-mgr") {
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));  // read monmap
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_R));  // read osdmap
    profile_grants.push_back(MonCapGrant("mon getmap"));
    profile_grants.push_back(MonCapGrant("auth get-or-create"));  // FIXME: this can expose other mgr keys
    profile_grants.back().command_args["entity"] = StringConstraint(
      StringConstraint::MATCH_TYPE_PREFIX, "mgr.");
    profile_grants.back().command_args["caps_mon"] = StringConstraint(
      StringConstraint::MATCH_TYPE_EQUAL, "allow profile mgr");
  }
  if (profile == "bootstrap-rgw") {
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));  // read monmap
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_R));  // read osdmap
    profile_grants.push_back(MonCapGrant("mon getmap"));
    profile_grants.push_back(MonCapGrant("auth get-or-create"));  // FIXME: this can expose other mds keys
    profile_grants.back().command_args["entity"] = StringConstraint(
      StringConstraint::MATCH_TYPE_PREFIX, "client.rgw.");
    profile_grants.back().command_args["caps_mon"] = StringConstraint(
      StringConstraint::MATCH_TYPE_EQUAL, "allow rw");
    profile_grants.back().command_args["caps_osd"] = StringConstraint(
      StringConstraint::MATCH_TYPE_EQUAL, "allow rwx");
  }
  if (profile == "bootstrap-rbd" || profile == "bootstrap-rbd-mirror") {
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));  // read monmap
    profile_grants.push_back(MonCapGrant("auth get-or-create"));  // FIXME: this can expose other rbd keys
    profile_grants.back().command_args["entity"] = StringConstraint(
      StringConstraint::MATCH_TYPE_PREFIX, "client.");
    profile_grants.back().command_args["caps_mon"] = StringConstraint(
      StringConstraint::MATCH_TYPE_EQUAL,
      (profile == "bootstrap-rbd-mirror" ? "profile rbd-mirror" :
                                           "profile rbd"));
    profile_grants.back().command_args["caps_osd"] = StringConstraint(
      StringConstraint::MATCH_TYPE_REGEX,
      "^([ ,]*profile(=|[ ]+)['\"]?rbd[^ ,'\"]*['\"]?([ ]+pool(=|[ ]+)['\"]?[^,'\"]+['\"]?)?)+$");
  }
  if (profile == "fs-client") {
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("mds", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("pg", MON_CAP_R));
  }
  if (profile == "simple-rados-client") {
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("pg", MON_CAP_R));
  }
  if (profile == "rbd" || profile == "rbd-mirror") {
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("osd", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("pg", MON_CAP_R));

    // exclusive lock dead-client blacklisting (IP+nonce required)
    profile_grants.push_back(MonCapGrant("osd blacklist"));
    profile_grants.back().command_args["blacklistop"] = StringConstraint(
      StringConstraint::MATCH_TYPE_EQUAL, "add");
    profile_grants.back().command_args["addr"] = StringConstraint(
      StringConstraint::MATCH_TYPE_REGEX, "^[^/]+/[0-9]+$");

  }
  if (profile == "rbd-mirror") {
    StringConstraint constraint(StringConstraint::MATCH_TYPE_PREFIX,
                                "rbd/mirror/");
    profile_grants.push_back(MonCapGrant("config-key get", "key", constraint));
  }

  if (profile == "role-definer") {
    // grants ALL caps to the auth subsystem, read-only on the
    // monitor subsystem and nothing else.
    profile_grants.push_back(MonCapGrant("mon", MON_CAP_R));
    profile_grants.push_back(MonCapGrant("auth", MON_CAP_ALL));
  }
}

mon_rwxa_t MonCapGrant::get_allowed(CephContext *cct,
				    int daemon_type,
				    EntityName name,
				    const std::string& s, const std::string& c,
				    const map<string,string>& c_args) const
{
  if (profile.length()) {
    expand_profile(daemon_type, name);
    mon_rwxa_t a;
    for (auto p = profile_grants.begin();
	 p != profile_grants.end(); ++p)
      a = a | p->get_allowed(cct, daemon_type, name, s, c, c_args);
    return a;
  }
  if (service.length()) {
    if (service != s)
      return 0;
    return allow;
  }
  if (command.length()) {
    if (command != c)
      return 0;
    for (map<string,StringConstraint>::const_iterator p = command_args.begin(); p != command_args.end(); ++p) {
      map<string,string>::const_iterator q = c_args.find(p->first);
      // argument must be present if a constraint exists
      if (q == c_args.end())
	return 0;
      switch (p->second.match_type) {
      case StringConstraint::MATCH_TYPE_EQUAL:
	if (p->second.value != q->second)
	  return 0;
        break;
      case StringConstraint::MATCH_TYPE_PREFIX:
	if (q->second.find(p->second.value) != 0)
	  return 0;
        break;
      case StringConstraint::MATCH_TYPE_REGEX:
        try {
	  std::regex pattern(
            p->second.value, std::regex::extended);
          if (!std::regex_match(q->second, pattern))
	    return 0;
        } catch(const std::regex_error&) {
	  return 0;
	}
        break;
      default:
        break;
      }
    }
    return MON_CAP_ALL;
  }
  // we don't allow config-key service to be accessed with blanket caps other
  // than '*' (i.e., 'any'), and that should have been checked by the caller
  // via 'is_allow_all()'.
  if (s == "config-key") {
    return 0;
  }
  return allow;
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
  text = "allow *";
}

bool MonCap::is_capable(
  CephContext *cct,
  int daemon_type,
  EntityName name,
  const string& service,
  const string& command, const map<string,string>& command_args,
  bool op_may_read, bool op_may_write, bool op_may_exec,
  const entity_addr_t& addr) const
{
  if (cct)
    ldout(cct, 20) << "is_capable service=" << service << " command=" << command
		   << (op_may_read ? " read":"")
		   << (op_may_write ? " write":"")
		   << (op_may_exec ? " exec":"")
		   << " addr " << addr
		   << " on cap " << *this
		   << dendl;

  mon_rwxa_t allow = 0;
  for (vector<MonCapGrant>::const_iterator p = grants.begin();
       p != grants.end(); ++p) {
    if (cct)
      ldout(cct, 20) << " allow so far " << allow << ", doing grant " << *p
		     << dendl;

    if (p->network.size() &&
	(!p->network_valid ||
	 !network_contains(p->network_parsed,
			   p->network_prefix,
			   addr))) {
      continue;
    }

    if (p->is_allow_all()) {
      if (cct)
	ldout(cct, 20) << " allow all" << dendl;
      return true;
    }

    // check enumerated caps
    allow = allow | p->get_allowed(cct, daemon_type, name, service, command,
				   command_args);
    if ((!op_may_read || (allow & MON_CAP_R)) &&
	(!op_may_write || (allow & MON_CAP_W)) &&
	(!op_may_exec || (allow & MON_CAP_X))) {
      if (cct)
	ldout(cct, 20) << " match" << dendl;
      return true;
    }
  }
  return false;
}

void MonCap::encode(bufferlist& bl) const
{
  ENCODE_START(4, 4, bl);   // legacy MonCaps was 3, 3
  encode(text, bl);
  ENCODE_FINISH(bl);
}

void MonCap::decode(bufferlist::const_iterator& bl)
{
  std::string s;
  DECODE_START(4, bl);
  decode(s, bl);
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
    unquoted_word %= +char_("a-zA-Z0-9_./-");
    str %= quoted_string | unquoted_word;
    network_str %= +char_("/.:a-fA-F0-9][");

    spaces = +(lit(' ') | lit('\n') | lit('\t'));

    // command := command[=]cmd [k1=v1 k2=v2 ...]
    str_match = '=' >> qi::attr(StringConstraint::MATCH_TYPE_EQUAL) >> str;
    str_prefix = spaces >> lit("prefix") >> spaces >>
                 qi::attr(StringConstraint::MATCH_TYPE_PREFIX) >> str;
    str_regex = spaces >> lit("regex") >> spaces >>
                 qi::attr(StringConstraint::MATCH_TYPE_REGEX) >> str;
    kv_pair = str >> (str_match | str_prefix | str_regex);
    kv_map %= kv_pair >> *(spaces >> kv_pair);
    command_match = -spaces >> lit("allow") >> spaces >> lit("command") >> (lit('=') | spaces)
			    >> qi::attr(string()) >> qi::attr(string())
			    >> str
			    >> -(spaces >> lit("with") >> spaces >> kv_map)
			    >> qi::attr(0)
			    >> -(spaces >> lit("network") >> spaces >> network_str);

    // service foo rwxa
    service_match %= -spaces >> lit("allow") >> spaces >> lit("service") >> (lit('=') | spaces)
			     >> str >> qi::attr(string()) >> qi::attr(string())
			     >> qi::attr(map<string,StringConstraint>())
                             >> spaces >> rwxa
			     >> -(spaces >> lit("network") >> spaces >> network_str);

    // profile foo
    profile_match %= -spaces >> -(lit("allow") >> spaces)
                             >> lit("profile") >> (lit('=') | spaces)
			     >> qi::attr(string())
			     >> str
			     >> qi::attr(string())
			     >> qi::attr(map<string,StringConstraint>())
			     >> qi::attr(0)
			     >> -(spaces >> lit("network") >> spaces >> network_str);

    // rwxa
    rwxa_match %= -spaces >> lit("allow") >> spaces
			  >> qi::attr(string()) >> qi::attr(string()) >> qi::attr(string())
			  >> qi::attr(map<string,StringConstraint>())
			  >> rwxa
			  >> -(spaces >> lit("network") >> spaces >> network_str);

    // rwxa := * | [r][w][x]
    rwxa =
      (lit("*")[_val = MON_CAP_ANY]) |
      (lit("all")[_val = MON_CAP_ANY]) |
      ( eps[_val = 0] >>
	( lit('r')[_val |= MON_CAP_R] ||
	  lit('w')[_val |= MON_CAP_W] ||
	  lit('x')[_val |= MON_CAP_X]
	  )
	);

    // grant := allow ...
    grant = -spaces >> (rwxa_match | profile_match | service_match | command_match) >> -spaces;

    // moncap := grant [grant ...]
    grants %= (grant % (*lit(' ') >> (lit(';') | lit(',')) >> *lit(' ')));
    moncap = grants  [_val = phoenix::construct<MonCap>(_1)]; 

  }
  qi::rule<Iterator> spaces;
  qi::rule<Iterator, unsigned()> rwxa;
  qi::rule<Iterator, string()> quoted_string;
  qi::rule<Iterator, string()> unquoted_word;
  qi::rule<Iterator, string()> str, network_str;

  qi::rule<Iterator, StringConstraint()> str_match, str_prefix, str_regex;
  qi::rule<Iterator, pair<string, StringConstraint>()> kv_pair;
  qi::rule<Iterator, map<string, StringConstraint>()> kv_map;

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
  auto iter = str.begin();
  auto end = str.end();

  MonCapParser<string::const_iterator> exp;
  bool r = qi::parse(iter, end, exp, *this);
  if (r && iter == end) {
    text = str;
    for (auto& g : grants) {
      g.parse_network();
    }
    return true;
  }

  // Make sure no grants are kept after parsing failed!
  grants.clear();

  if (err) {
    if (iter != end)
      *err << "mon capability parse failed, stopped at '"
	   << std::string(iter, end)
	   << "' of '" << str << "'";
    else
      *err << "mon capability parse failed, stopped at end of '" << str << "'";
  }

  return false; 
}

