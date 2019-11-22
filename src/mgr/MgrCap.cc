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
#include <boost/algorithm/string/predicate.hpp>

#include "MgrCap.h"
#include "include/stringify.h"
#include "include/ipaddr.h"
#include "common/debug.h"
#include "common/Formatter.h"

#include <algorithm>
#include <regex>

#include "include/ceph_assert.h"

static inline bool is_not_alnum_space(char c) {
  return !(isalpha(c) || isdigit(c) || (c == '-') || (c == '_'));
}

static std::string maybe_quote_string(const std::string& str) {
  if (find_if(str.begin(), str.end(), is_not_alnum_space) == str.end())
    return str;
  return std::string("\"") + str + std::string("\"");
}

#define dout_subsys ceph_subsys_mgr

ostream& operator<<(ostream& out, const mgr_rwxa_t& p) {
  if (p == MGR_CAP_ANY)
    return out << "*";

  if (p & MGR_CAP_R)
    out << "r";
  if (p & MGR_CAP_W)
    out << "w";
  if (p & MGR_CAP_X)
    out << "x";
  return out;
}

ostream& operator<<(ostream& out, const MgrCapGrantConstraint& c) {
  switch (c.match_type) {
  case MgrCapGrantConstraint::MATCH_TYPE_EQUAL:
    out << "=";
    break;
  case MgrCapGrantConstraint::MATCH_TYPE_PREFIX:
    out << " prefix ";
    break;
  case MgrCapGrantConstraint::MATCH_TYPE_REGEX:
    out << " regex ";
    break;
  default:
    break;
  }
  out << maybe_quote_string(c.value);
  return out;
}

ostream& operator<<(ostream& out, const MgrCapGrant& m) {
  if (!m.profile.empty()) {
    out << "profile " << maybe_quote_string(m.profile);
  } else {
    out << "allow";
    if (!m.service.empty()) {
      out << " service " << maybe_quote_string(m.service);
    } else if (!m.module.empty()) {
      out << " module " << maybe_quote_string(m.module);
    } else if (!m.command.empty()) {
      out << " command " << maybe_quote_string(m.command);
    }
  }

  if (!m.arguments.empty()) {
    out << (!m.profile.empty() ? "" : " with");
    for (auto& [key, constraint] : m.arguments) {
      out << " " << maybe_quote_string(key) << constraint;
    }
  }

  if (m.allow != 0) {
    out << " " << m.allow;
  }

  if (m.network.size()) {
    out << " network " << m.network;
  }
  return out;
}

// <magic>
//  fusion lets us easily populate structs via the qi parser.

typedef std::map<std::string, MgrCapGrantConstraint> kvmap;

BOOST_FUSION_ADAPT_STRUCT(MgrCapGrant,
                          (std::string, service)
                          (std::string, module)
                          (std::string, profile)
                          (std::string, command)
                          (kvmap, arguments)
                          (mgr_rwxa_t, allow)
                          (std::string, network))

BOOST_FUSION_ADAPT_STRUCT(MgrCapGrantConstraint,
                          (MgrCapGrantConstraint::MatchType, match_type)
                          (std::string, value))

// </magic>

void MgrCapGrant::parse_network() {
  network_valid = ::parse_network(network.c_str(), &network_parsed,
                                  &network_prefix);
}

void MgrCapGrant::expand_profile(std::ostream *err) const {
  // only generate this list once
  if (!profile_grants.empty()) {
    return;
  }

  if (profile == "read-only") {
    // grants READ-ONLY caps MGR-wide
    profile_grants.push_back({{}, {}, {}, {}, {}, mgr_rwxa_t{MGR_CAP_R}});
    return;
  }

  if (profile == "read-write") {
    // grants READ-WRITE caps MGR-wide
    profile_grants.push_back({{}, {}, {}, {}, {},
                              mgr_rwxa_t{MGR_CAP_R | MGR_CAP_W}});
    return;
  }

  if (profile == "crash") {
    profile_grants.push_back({{}, {}, {}, "crash post", {}, {}});
    return;
  }

  if (profile == "osd") {
    // this is a documented profile (so we need to accept it as valid), but it
    // currently doesn't do anything
    return;
  }

  if (profile == "mds") {
    // this is a documented profile (so we need to accept it as valid), but it
    // currently doesn't do anything
    return;
  }

  if (profile == "rbd" || profile == "rbd-read-only") {
    Arguments filtered_arguments;
    for (auto& [key, constraint] : arguments) {
      if (key == "pool" || key == "namespace") {
        filtered_arguments[key] = std::move(constraint);
      } else {
        if (err != nullptr) {
          *err << "profile '" << profile << "' does not recognize key '" << key
               << "'";
        }
        return;
      }
    }

    mgr_rwxa_t perms = mgr_rwxa_t{MGR_CAP_R};
    if (profile == "rbd") {
      perms = mgr_rwxa_t{MGR_CAP_R | MGR_CAP_W};
    }

    // whitelist all 'rbd_support' commands (restricted by optional
    // pool/namespace constraints)
    profile_grants.push_back({{}, "rbd_support", {}, {},
                              std::move(filtered_arguments), perms});
    return;
  }

  if (err != nullptr) {
    *err << "unrecognized profile '" << profile << "'";
  }
}

bool MgrCapGrant::validate_arguments(
      const std::map<std::string, std::string>& args) const {
  for (auto& [key, constraint] : arguments) {
    auto q = args.find(key);

    // argument must be present if a constraint exists
    if (q == args.end()) {
      return false;
    }

    switch (constraint.match_type) {
    case MgrCapGrantConstraint::MATCH_TYPE_EQUAL:
      if (constraint.value != q->second)
        return false;
      break;
    case MgrCapGrantConstraint::MATCH_TYPE_PREFIX:
      if (q->second.find(constraint.value) != 0)
        return false;
      break;
    case MgrCapGrantConstraint::MATCH_TYPE_REGEX:
      try {
        std::regex pattern(constraint.value, std::regex::extended);
        if (!std::regex_match(q->second, pattern)) {
          return false;
        }
      } catch(const std::regex_error&) {
        return false;
      }
      break;
    default:
      return false;
    }
  }

  return true;
}

mgr_rwxa_t MgrCapGrant::get_allowed(
    CephContext *cct, EntityName name, const std::string& s,
    const std::string& m, const std::string& c,
    const std::map<std::string, std::string>& args) const {
  if (!profile.empty()) {
    expand_profile(nullptr);
    mgr_rwxa_t a;
    for (auto& grant : profile_grants) {
      a = a | grant.get_allowed(cct, name, s, m, c, args);
    }
    return a;
  }

  if (!service.empty()) {
    if (service != s) {
      return mgr_rwxa_t{};
    }
    return allow;
  }

  if (!module.empty()) {
    if (module != m) {
      return mgr_rwxa_t{};
    }

    // don't test module arguments when validating a specific command
    if (c.empty() && !validate_arguments(args)) {
      return mgr_rwxa_t{};
    }
    return allow;
  }

  if (!command.empty()) {
    if (command != c) {
      return mgr_rwxa_t{};
    }
    if (!validate_arguments(args)) {
      return mgr_rwxa_t{};
    }
    return mgr_rwxa_t{MGR_CAP_ANY};
  }

  return allow;
}

ostream& operator<<(ostream&out, const MgrCap& m) {
  bool first = true;
  for (auto& grant : m.grants) {
    if (!first) {
      out << ", ";
    }
    first = false;

    out << grant;
  }
  return out;
}

bool MgrCap::is_allow_all() const {
  for (auto& grant : grants) {
    if (grant.is_allow_all()) {
      return true;
    }
  }
  return false;
}

void MgrCap::set_allow_all() {
  grants.clear();
  grants.push_back({{}, {}, {}, {}, {}, mgr_rwxa_t{MGR_CAP_ANY}});
  text = "allow *";
}

bool MgrCap::is_capable(
    CephContext *cct,
    EntityName name,
    const std::string& service,
    const std::string& module,
    const std::string& command,
    const std::map<std::string, std::string>& command_args,
    bool op_may_read, bool op_may_write, bool op_may_exec,
    const entity_addr_t& addr) const {
  if (cct) {
    ldout(cct, 20) << "is_capable service=" << service << " "
                   << "module=" << module << " "
                   << "command=" << command
                   << (op_may_read ? " read":"")
                   << (op_may_write ? " write":"")
                   << (op_may_exec ? " exec":"")
                   << " addr " << addr
                   << " on cap " << *this
                   << dendl;
  }

  mgr_rwxa_t allow;
  for (auto& grant : grants) {
    if (cct)
      ldout(cct, 20) << " allow so far " << allow << ", doing grant " << grant
                     << dendl;

    if (grant.network.size() &&
        (!grant.network_valid ||
         !network_contains(grant.network_parsed,
                           grant.network_prefix,
                           addr))) {
      continue;
    }

    if (grant.is_allow_all()) {
      if (cct) {
        ldout(cct, 20) << " allow all" << dendl;
      }
      return true;
    }

    // check enumerated caps
    allow = allow | grant.get_allowed(cct, name, service, module, command,
                                      command_args);
    if ((!op_may_read || (allow & MGR_CAP_R)) &&
        (!op_may_write || (allow & MGR_CAP_W)) &&
        (!op_may_exec || (allow & MGR_CAP_X))) {
      if (cct) {
        ldout(cct, 20) << " match" << dendl;
      }
      return true;
    }
  }
  return false;
}

void MgrCap::encode(bufferlist& bl) const {
  // remain backwards compatible w/ MgrCap
  ENCODE_START(4, 4, bl);
  encode(text, bl);
  ENCODE_FINISH(bl);
}

void MgrCap::decode(bufferlist::const_iterator& bl) {
  // remain backwards compatible w/ MgrCap
  std::string s;
  DECODE_START(4, bl);
  decode(s, bl);
  DECODE_FINISH(bl);
  parse(s, NULL);
}

void MgrCap::dump(Formatter *f) const {
  f->dump_string("text", text);
}

void MgrCap::generate_test_instances(list<MgrCap*>& ls) {
  ls.push_back(new MgrCap);
  ls.push_back(new MgrCap);
  ls.back()->parse("allow *");
  ls.push_back(new MgrCap);
  ls.back()->parse("allow rwx");
  ls.push_back(new MgrCap);
  ls.back()->parse("allow service foo x");
  ls.push_back(new MgrCap);
  ls.back()->parse("allow command bar x");
  ls.push_back(new MgrCap);
  ls.back()->parse("allow service foo r, allow command bar x");
  ls.push_back(new MgrCap);
  ls.back()->parse("allow command bar with k1=v1 x");
  ls.push_back(new MgrCap);
  ls.back()->parse("allow command bar with k1=v1 k2=v2 x");
  ls.push_back(new MgrCap);
  ls.back()->parse("allow module bar with k1=v1 k2=v2 x");
  ls.push_back(new MgrCap);
  ls.back()->parse("profile rbd pool=rbd");
}

// grammar
namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;
namespace phoenix = boost::phoenix;

template <typename Iterator>
struct MgrCapParser : qi::grammar<Iterator, MgrCap()> {
  MgrCapParser() : MgrCapParser::base_type(mgrcap) {
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

    // key <=|prefix|regex> value[ ...]
    str_match = -spaces >> lit('=') >> -spaces >>
                qi::attr(MgrCapGrantConstraint::MATCH_TYPE_EQUAL) >> str;
    str_prefix = spaces >> lit("prefix") >> spaces >>
                 qi::attr(MgrCapGrantConstraint::MATCH_TYPE_PREFIX) >> str;
    str_regex = spaces >> lit("regex") >> spaces >>
                 qi::attr(MgrCapGrantConstraint::MATCH_TYPE_REGEX) >> str;
    kv_pair = str >> (str_match | str_prefix | str_regex);
    kv_map %= kv_pair >> *(spaces >> kv_pair);

    // command := command[=]cmd [k1=v1 k2=v2 ...]
    command_match = -spaces >> lit("allow") >> spaces >> lit("command") >> (lit('=') | spaces)
                            >> qi::attr(std::string())
                            >> qi::attr(std::string())
                            >> qi::attr(std::string())
                            >> str
                            >> -(spaces >> lit("with") >> spaces >> kv_map)
                            >> qi::attr(0)
                            >> -(spaces >> lit("network") >> spaces >> network_str);

    // service foo rwxa
    service_match %= -spaces >> lit("allow") >> spaces >> lit("service") >> (lit('=') | spaces)
                             >> str
                             >> qi::attr(std::string())
                             >> qi::attr(std::string())
                             >> qi::attr(std::string())
                             >> qi::attr(map<std::string, MgrCapGrantConstraint>())
                             >> spaces >> rwxa
                             >> -(spaces >> lit("network") >> spaces >> network_str);

    // module foo rwxa
    module_match %= -spaces >> lit("allow") >> spaces >> lit("module") >> (lit('=') | spaces)
                            >> qi::attr(std::string())
                            >> str
                            >> qi::attr(std::string())
                            >> qi::attr(std::string())
                            >> -(spaces >> lit("with") >> spaces >> kv_map)
                            >> spaces >> rwxa
                            >> -(spaces >> lit("network") >> spaces >> network_str);

    // profile foo
    profile_match %= -spaces >> -(lit("allow") >> spaces)
                             >> lit("profile") >> (lit('=') | spaces)
                             >> qi::attr(std::string())
                             >> qi::attr(std::string())
                             >> str
                             >> qi::attr(std::string())
                             >> -(spaces >> kv_map)
                             >> qi::attr(0)
                             >> -(spaces >> lit("network") >> spaces >> network_str);

    // rwxa
    rwxa_match %= -spaces >> lit("allow") >> spaces
                          >> qi::attr(std::string())
                          >> qi::attr(std::string())
                          >> qi::attr(std::string())
                          >> qi::attr(std::string())
                          >> qi::attr(std::map<std::string,MgrCapGrantConstraint>())
                          >> rwxa
                          >> -(spaces >> lit("network") >> spaces >> network_str);

    // rwxa := * | [r][w][x]
    rwxa =
      (lit("*")[_val = MGR_CAP_ANY]) |
      (lit("all")[_val = MGR_CAP_ANY]) |
      ( eps[_val = 0] >>
        ( lit('r')[_val |= MGR_CAP_R] ||
          lit('w')[_val |= MGR_CAP_W] ||
          lit('x')[_val |= MGR_CAP_X]
          )
        );

    // grant := allow ...
    grant = -spaces >> (rwxa_match | profile_match | service_match |
                        module_match | command_match) >> -spaces;

    // mgrcap := grant [grant ...]
    grants %= (grant % (*lit(' ') >> (lit(';') | lit(',')) >> *lit(' ')));
    mgrcap = grants  [_val = phoenix::construct<MgrCap>(_1)];
  }

  qi::rule<Iterator> spaces;
  qi::rule<Iterator, unsigned()> rwxa;
  qi::rule<Iterator, std::string()> quoted_string;
  qi::rule<Iterator, std::string()> unquoted_word;
  qi::rule<Iterator, std::string()> str, network_str;

  qi::rule<Iterator, MgrCapGrantConstraint()> str_match, str_prefix, str_regex;
  qi::rule<Iterator, std::pair<std::string, MgrCapGrantConstraint>()> kv_pair;
  qi::rule<Iterator, std::map<std::string, MgrCapGrantConstraint>()> kv_map;

  qi::rule<Iterator, MgrCapGrant()> rwxa_match;
  qi::rule<Iterator, MgrCapGrant()> command_match;
  qi::rule<Iterator, MgrCapGrant()> service_match;
  qi::rule<Iterator, MgrCapGrant()> module_match;
  qi::rule<Iterator, MgrCapGrant()> profile_match;
  qi::rule<Iterator, MgrCapGrant()> grant;
  qi::rule<Iterator, std::vector<MgrCapGrant>()> grants;
  qi::rule<Iterator, MgrCap()> mgrcap;
};

bool MgrCap::parse(const std::string& str, ostream *err) {
  auto iter = str.begin();
  auto end = str.end();

  MgrCapParser<std::string::const_iterator> exp;
  bool r = qi::parse(iter, end, exp, *this);
  if (r && iter == end) {
    text = str;

    std::stringstream profile_err;
    for (auto& g : grants) {
      g.parse_network();

      if (!g.profile.empty()) {
        g.expand_profile(&profile_err);
      }
    }

    if (!profile_err.str().empty()) {
      if (err != nullptr) {
        *err << "mgr capability parse failed during profile evaluation: "
             << profile_err.str();
      }
      return false;
    }
    return true;
  }

  // Make sure no grants are kept after parsing failed!
  grants.clear();

  if (err) {
    if (iter != end)
      *err << "mgr capability parse failed, stopped at '"
           << std::string(iter, end) << "' of '" << str << "'";
    else
      *err << "mgr capability parse failed, stopped at end of '" << str << "'";
  }

  return false;
}
