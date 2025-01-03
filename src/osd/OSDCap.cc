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
#include <boost/phoenix/operator.hpp>
#include <boost/phoenix.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "OSDCap.h"
#include "common/config.h"
#include "common/debug.h"
#include "include/ipaddr.h"

#define dout_subsys ceph_subsys_osd

#undef dout_prefix
#define dout_prefix *_dout << "OSDCap "

using std::ostream;
using std::string;
using std::vector;

ostream& operator<<(ostream& out, const osd_rwxa_t& p)
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
  if (s.class_name.length()) {
    out << "class '" << s.class_name << "'";
    if (!s.method_name.empty()) {
      out << " '" << s.method_name << "'";
    }
  }
  return out;
}

ostream& operator<<(ostream& out, const OSDCapPoolNamespace& pns)
{
  if (!pns.pool_name.empty()) {
    out << "pool " << pns.pool_name << " ";
  }
  if (pns.nspace) {
    out << "namespace ";
    if (pns.nspace->empty()) {
      out << "\"\"";
    } else {
      out << *pns.nspace;
    }
    out << " ";
  }
  return out;
}

ostream& operator<<(ostream &out, const OSDCapPoolTag &pt)
{
  out << "app " << pt.application << " key " << pt.key << " val " << pt.value
      << " ";
  return out;
}

ostream& operator<<(ostream& out, const OSDCapMatch& m)
{
  if (!m.pool_namespace.pool_name.empty() || m.pool_namespace.nspace) {
    out << m.pool_namespace;
  }

  if (!m.pool_tag.application.empty()) {
    out << m.pool_tag;
  }

  if (m.object_prefix.length()) {
    out << "object_prefix " << m.object_prefix << " ";
  }
  return out;
}

ostream& operator<<(ostream& out, const OSDCapProfile& m)
{
  out << "profile " << m.name;
  out << m.pool_namespace;
  return out;
}

bool OSDCapPoolNamespace::is_match(const std::string& pn,
                                   const std::string& ns) const
{
  if (!pool_name.empty()) {
    if (pool_name != pn) {
      return false;
    }
  }
  if (nspace) {
    if (!nspace->empty() && nspace->back() == '*' &&
	boost::starts_with(ns, nspace->substr(0, nspace->length() - 1))) {
      return true;
    }

    if (*nspace != ns) {
      return false;
    }
  }
  return true;
}

bool OSDCapPoolNamespace::is_match_all() const
{
  if (!pool_name.empty())
    return false;
  if (nspace)
    return false;
  return true;
}

bool OSDCapPoolTag::is_match(const app_map_t& app_map) const
{
  if (application.empty()) {
    return true;
  }
  auto kv_map = app_map.find(application);
  if (kv_map == app_map.end()) {
    return false;
  }
  if (!key.compare("*") && !value.compare("*")) {
    return true;
  }
  if (!key.compare("*")) {
    for (auto it : kv_map->second) {
      if (it.second == value) {
	return true;
      }
    }
    return false;
  }
  auto kv_val = kv_map->second.find(key);
  if (kv_val == kv_map->second.end()) {
    return false;
  }
  if (!value.compare("*")) {
    return true;
  }
  return kv_val->second == value;
}

bool OSDCapPoolTag::is_match_all() const {
  return application.empty();
}

bool OSDCapMatch::is_match(const string& pn, const string& ns,
			   const OSDCapPoolTag::app_map_t& app_map,
			   const string& object) const
{
  if (!pool_namespace.is_match(pn, ns)) {
    return false;
  } else if (!pool_tag.is_match(app_map)) {
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
if (!pool_namespace.is_match_all()) {
    return false;
  } else if (!pool_tag.is_match_all()) {
    return false;
  }

  if (object_prefix.length()) {
    return false;
  }
  return true;
}

ostream& operator<<(ostream& out, const OSDCapGrant& g)
{
  out << "grant(";
  if (g.profile.is_valid()) {
    out << g.profile << " [";
    for (auto it = g.profile_grants.cbegin();
         it != g.profile_grants.cend(); ++it) {
      if (it != g.profile_grants.cbegin()) {
        out << ",";
      }
      out << *it;
    }
    out << "]";
  } else {
    out << g.match << g.spec;
  }
  if (g.network.size()) {
    out << " network " << g.network;
  }
  out << ")";
  return out;
}

void OSDCapGrant::set_network(const string& n)
{
  network = n;
  network_valid = ::parse_network(n.c_str(), &network_parsed, &network_prefix);
}

bool OSDCapGrant::allow_all() const
{
  if (profile.is_valid()) {
    return std::any_of(profile_grants.cbegin(), profile_grants.cend(),
                       [](const OSDCapGrant& grant) {
        return grant.allow_all();
      });
  }

  return (match.is_match_all() && spec.allow_all());
}

bool OSDCapGrant::is_capable(
  const string& pool_name,
  const string& ns,
  const OSDCapPoolTag::app_map_t& application_metadata,
  const string& object,
  bool op_may_read,
  bool op_may_write,
  const std::vector<OpInfo::ClassInfo>& classes,
  const entity_addr_t& addr,
  std::vector<bool>* class_allowed) const
{
  osd_rwxa_t allow = 0;

  if (network.size() &&
      (!network_valid ||
       !network_contains(network_parsed,
			 network_prefix,
			 addr))) {
    return false;
  }

  if (profile.is_valid()) {
    return std::any_of(profile_grants.cbegin(), profile_grants.cend(),
                       [&](const OSDCapGrant& grant) {
			   return grant.is_capable(pool_name, ns,
						   application_metadata,
						   object, op_may_read,
						   op_may_write, classes, addr,
						   class_allowed);
		       });
  } else {
    if (match.is_match(pool_name, ns, application_metadata, object)) {
      allow = allow | spec.allow;
      if ((op_may_read && !(allow & OSD_CAP_R)) ||
          (op_may_write && !(allow & OSD_CAP_W))) {
        return false;
      }
      if (!classes.empty()) {
        // check 'allow *'
        if (spec.allow_all()) {
          return true;
        }

        // compare this grant to each class in the operation
        for (size_t i = 0; i < classes.size(); ++i) {
          // check 'allow class foo [method_name]'
          if (!spec.class_name.empty() &&
              classes[i].class_name == spec.class_name &&
              (spec.method_name.empty() ||
               classes[i].method_name == spec.method_name)) {
            (*class_allowed)[i] = true;
            continue;
          }
          // check 'allow x | class-{rw}': must be on allow list
          if (!classes[i].allowed) {
            continue;
          }
          if ((classes[i].read && !(allow & OSD_CAP_CLS_R)) ||
              (classes[i].write && !(allow & OSD_CAP_CLS_W))) {
            continue;
          }
          (*class_allowed)[i] = true;
        }
        if (!std::all_of(class_allowed->cbegin(), class_allowed->cend(),
              [](bool v) { return v; })) {
          return false;
        }
      }
      return true;
    }
  }
  return false;
}

void OSDCapGrant::expand_profile()
{
  if (profile.name == "read-only") {
    // grants READ-ONLY caps to the OSD
    profile_grants.emplace_back(OSDCapMatch(profile.pool_namespace),
                                OSDCapSpec(osd_rwxa_t(OSD_CAP_R)));
    return;
  }
  if (profile.name == "read-write") {
    // grants READ-WRITE caps to the OSD
    profile_grants.emplace_back(OSDCapMatch(profile.pool_namespace),
                                OSDCapSpec(osd_rwxa_t(OSD_CAP_R | OSD_CAP_W)));
  }

  if (profile.name == "rbd") {
    // RBD read-write grant
    profile_grants.emplace_back(OSDCapMatch(string(), "rbd_info"),
                                OSDCapSpec(osd_rwxa_t(OSD_CAP_R)));
    profile_grants.emplace_back(OSDCapMatch(string(), "rbd_children"),
                                OSDCapSpec(osd_rwxa_t(OSD_CAP_CLS_R)));
    profile_grants.emplace_back(OSDCapMatch(string(), "rbd_mirroring"),
                                OSDCapSpec(osd_rwxa_t(OSD_CAP_CLS_R)));
    profile_grants.emplace_back(OSDCapMatch(profile.pool_namespace.pool_name,
                                            "", "rbd_info"),
                                OSDCapSpec("rbd", "metadata_list"));
    profile_grants.emplace_back(OSDCapMatch(profile.pool_namespace),
                                OSDCapSpec(osd_rwxa_t(OSD_CAP_R |
                                                      OSD_CAP_W |
                                                      OSD_CAP_X)));
  }
  if (profile.name == "rbd-read-only") {
    // RBD read-only grant
    profile_grants.emplace_back(OSDCapMatch(profile.pool_namespace.pool_name,
                                            "", "rbd_info"),
                                OSDCapSpec("rbd", "metadata_list"));
    profile_grants.emplace_back(OSDCapMatch(profile.pool_namespace),
                                OSDCapSpec(osd_rwxa_t(OSD_CAP_R |
                                                      OSD_CAP_CLS_R)));
    profile_grants.emplace_back(OSDCapMatch(profile.pool_namespace,
                                            "rbd_header."),
                                OSDCapSpec("rbd", "child_attach"));
    profile_grants.emplace_back(OSDCapMatch(profile.pool_namespace,
                                            "rbd_header."),
                                OSDCapSpec("rbd", "child_detach"));
  }
}

bool OSDCap::allow_all() const
{
  for (auto &grant : grants) {
    if (grant.allow_all()) {
      return true;
    }
  }
  return false;
}

void OSDCap::set_allow_all()
{
  grants.clear();
  grants.push_back(OSDCapGrant(OSDCapMatch(), OSDCapSpec(OSD_CAP_ANY)));
}

bool OSDCap::is_capable(const string& pool_name, const string& ns,
			const OSDCapPoolTag::app_map_t& application_metadata,
			const string& object,
                        bool op_may_read, bool op_may_write,
			const std::vector<OpInfo::ClassInfo>& classes,
			const entity_addr_t& addr) const
{
  std::vector<bool> class_allowed(classes.size(), false);
  for (auto &grant : grants) {
    if (grant.is_capable(pool_name, ns, application_metadata,
			 object, op_may_read, op_may_write, classes, addr,
			 &class_allowed)) {
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
    unquoted_word %= +char_("a-zA-Z0-9_./-");
    str %= quoted_string | unquoted_word;
    estr %= equoted_string | unquoted_word;
    network_str %= +char_("/.:a-fA-F0-9][");

    spaces = +ascii::space;

    wildcard = (lit('*') | lit("all")) [_val = "*"];

    pool_name %= -(spaces >> lit("pool") >> (lit('=') | spaces) >> str);
    nspace %= (spaces >> lit("namespace")
	       >> (lit('=') | spaces)
	       >> estr >> -char_('*'));

    // match := [pool[=]<poolname> [namespace[=]<namespace>]] [object_prefix <prefix>]
    object_prefix %= -(spaces >> lit("object_prefix") >> spaces >> str);
    pooltag %= (spaces >> lit("tag")
		>> spaces >> str // application
		>> spaces >> (wildcard | str) // key
		>> -spaces >> lit('=') >> -spaces >> (wildcard | str)); // value

    match = (
      pooltag                                 [_val = phoenix::construct<OSDCapMatch>(_1)] |
      (nspace >> pooltag)                     [_val = phoenix::construct<OSDCapMatch>(_1, _2)] |
      (pool_name >> nspace >> object_prefix)  [_val = phoenix::construct<OSDCapMatch>(_1, _2, _3)] |
      (pool_name >> object_prefix)            [_val = phoenix::construct<OSDCapMatch>(_1, _2)]
    );

    // rwxa := * | [r][w][x] [class-read] [class-write]
    rwxa =
      (spaces >> wildcard[_val = OSD_CAP_ANY]) |
      ( eps[_val = 0] >>
	(
	 spaces >>
	 ( lit('r')[_val |= OSD_CAP_R] ||
	   lit('w')[_val |= OSD_CAP_W] ||
	   lit('x')[_val |= OSD_CAP_X] )) ||
	( (spaces >> lit("class-read")[_val |= OSD_CAP_CLS_R]) ||
	  (spaces >> lit("class-write")[_val |= OSD_CAP_CLS_W]) ));

    // capspec := * | rwx | class <name> [<method name>]
    class_name %= (spaces >> lit("class") >> spaces >> str);
    method_name %= -(spaces >> str);
    capspec = (
      (rwxa)                      [_val = phoenix::construct<OSDCapSpec>(_1)] |
      (class_name >> method_name) [_val = phoenix::construct<OSDCapSpec>(_1, _2)]);

    // profile := profile <name> [pool[=]<pool> [namespace[=]<namespace>]]
    profile_name %= (lit("profile") >> (lit('=') | spaces) >> str);
    profile = (
      (profile_name >> pool_name >> nspace) [_val = phoenix::construct<OSDCapProfile>(_1, _2, _3)] |
      (profile_name >> pool_name)           [_val = phoenix::construct<OSDCapProfile>(_1, _2)]);

    // grant := allow match capspec
    grant = (*ascii::blank >>
	     ((lit("allow") >> capspec >> match >>
	       -(spaces >> lit("network") >> spaces >> network_str))
	       [_val = phoenix::construct<OSDCapGrant>(_2, _1, _3)] |
	      (lit("allow") >> match >> capspec >>
	       -(spaces >> lit("network") >> spaces >> network_str))
	       [_val = phoenix::construct<OSDCapGrant>(_1, _2, _3)] |
              (profile >> -(spaces >> lit("network") >> spaces >> network_str))
	       [_val = phoenix::construct<OSDCapGrant>(_1, _2)]
             ) >> *ascii::blank);
    // osdcap := grant [grant ...]
    grants %= (grant % (lit(';') | lit(',')));
    osdcap = grants  [_val = phoenix::construct<OSDCap>(_1)];
  }
  qi::rule<Iterator> spaces;
  qi::rule<Iterator, unsigned()> rwxa;
  qi::rule<Iterator, string()> quoted_string, equoted_string;
  qi::rule<Iterator, string()> unquoted_word;
  qi::rule<Iterator, string()> str, estr, network_str;
  qi::rule<Iterator, string()> wildcard;
  qi::rule<Iterator, string()> class_name;
  qi::rule<Iterator, string()> method_name;
  qi::rule<Iterator, OSDCapSpec()> capspec;
  qi::rule<Iterator, string()> pool_name;
  qi::rule<Iterator, string()> nspace;
  qi::rule<Iterator, string()> object_prefix;
  qi::rule<Iterator, OSDCapPoolTag()> pooltag;
  qi::rule<Iterator, OSDCapMatch()> match;
  qi::rule<Iterator, string()> profile_name;
  qi::rule<Iterator, OSDCapProfile()> profile;
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
    *err << "osd capability parse failed, stopped at '" << std::string(iter, end)
	 << "' of '" << str << "'";

  return false; 
}

bool OSDCap::merge(OSDCap newcap)
{
  ceph_assert(newcap.grants.size() == 1);
  auto ng = newcap.grants[0];

  for (auto& g : grants) {
    /* TODO: check case where cap is "allow rw tag cephfs *". */

    if (g.match.pool_tag.application == ng.match.pool_tag.application and
	g.match.pool_tag.key == ng.match.pool_tag.key and
	g.match.pool_tag.value == ng.match.pool_tag.value) {
      if (g.spec.allow == ng.spec.allow) {
	// no update required, maintaining idempotency.
	return false;
      } else if (g.spec.allow != ng.spec.allow) {
	// cap for given application is present, let's update it.
	g.spec.allow = ng.spec.allow;
	return true;
      }
    }
  }

  // cap for given application is absent, let's add a new cap for it.
  grants.push_back(OSDCapGrant(
    OSDCapMatch(OSDCapPoolTag(ng.match.pool_tag.application,
      ng.match.pool_tag.key, ng.match.pool_tag.value)),
    OSDCapSpec(ng.spec.allow)));
  return true;
}

string OSDCapGrant::to_string() {
  string str = "allow ";

  if (spec.allow & OSD_CAP_R)
    str += "r";
  if (spec.allow & OSD_CAP_W)
    str += "w";

  if ((spec.allow & OSD_CAP_X) == OSD_CAP_X)
    str += "x";
  else {
    if (spec.allow & OSD_CAP_CLS_R)
      str += " class-read";
    else if (spec.allow & OSD_CAP_CLS_W)
      str += " class-write";
  }

  if (not (match.pool_tag.application.empty() and match.pool_tag.key.empty()
	   and match.pool_tag.value.empty()))
    str += " tag " + match.pool_tag.application + " " + \
	   match.pool_tag.key + "=" + match.pool_tag.value;

  return str;
}

string OSDCap::to_string()
{
  string str;

  for (size_t i = 0; i < grants.size(); ++i) {
    str += grants[i].to_string();
    if (i < grants.size() - 1)
      str += ", ";
  }

  return str;
}
