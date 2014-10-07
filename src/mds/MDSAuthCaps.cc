// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include <errno.h>

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix.hpp>

#include "MDSAuthCaps.h"

using std::ostream;
using std::string;
namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;
namespace phoenix = boost::phoenix;

const std::string MDSCapMatch::MDS_AUTH_PATH_ROOT = "/";

template <typename Iterator>
struct MDSCapParser : qi::grammar<Iterator, MDSAuthCaps()>
{
  MDSCapParser() : MDSCapParser::base_type(mdscaps)
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

    spaces = +(lit(' ') | lit('\n') | lit('\t'));

    quoted_path %=
      lexeme[lit("\"") >> *(char_ - '"') >> '"'] | 
      lexeme[lit("'") >> *(char_ - '\'') >> '\''];
    unquoted_path %= +char_("a-zA-Z0-9_.-/");

    // match := [path=<path>] [uid=<uid>]
    uid %= (spaces >> lit("uid") >> lit('=') >> int_);
    path %= (spaces >> lit("path") >> lit('=') >> (quoted_path | unquoted_path));
    match = -(
             (uid)[_val = phoenix::construct<MDSCapMatch>(_1)] |
             (path >> uid)[_val = phoenix::construct<MDSCapMatch>(_1, _2)] | 
             (path)[_val = phoenix::construct<MDSCapMatch>(_1)]);

    // capspec = * | r[w]
    capspec = spaces >> (
        lit("*")[_val = MDSCapSpec(true, true, true)]
        |
        (lit("rw"))[_val = MDSCapSpec(true, true, false)]
        |
        (lit("r"))[_val = MDSCapSpec(true, false, false)]
        );

    grant = lit("allow") >> (capspec >> match)[_val = phoenix::construct<MDSCapGrant>(_1, _2)];
    grants %= (grant % (*lit(' ') >> (lit(';') | lit(',')) >> *lit(' ')));
    mdscaps = grants  [_val = phoenix::construct<MDSAuthCaps>(_1)]; 
  }
  qi::rule<Iterator> spaces;
  qi::rule<Iterator, string()> quoted_path, unquoted_path;
  qi::rule<Iterator, MDSCapSpec()> capspec;
  qi::rule<Iterator, string()> path;
  qi::rule<Iterator, int()> uid;
  qi::rule<Iterator, MDSCapMatch()> match;
  qi::rule<Iterator, MDSCapGrant()> grant;
  qi::rule<Iterator, std::vector<MDSCapGrant>()> grants;
  qi::rule<Iterator, MDSAuthCaps()> mdscaps;
};


/**
 * For a given filesystem path, query whether this capability carries`
 * authorization to read or write.
 *
 * This is true if any of the 'grant' clauses in the capability match the
 * requested path + op.
 *
 */
bool MDSAuthCaps::is_capable(const std::string &path, int uid, bool may_read, bool may_write) const
{
  for (std::vector<MDSCapGrant>::const_iterator i = grants.begin(); i != grants.end(); ++i) {
    if (i->match.match(path, uid)) {
      if ((may_read && !i->spec.read) ||
          (may_write && !i->spec.write)) {
        continue;
      } else {
        return true;
      }
    }
  }

  return false;
}

void MDSAuthCaps::set_allow_all()
{
    grants.clear();
    grants.push_back(MDSCapGrant(MDSCapSpec(true, true, true), MDSCapMatch()));
}

bool MDSAuthCaps::parse(const std::string& str, ostream *err)
{
  // Special case for legacy caps
  if (str == "allow") {
    grants.clear();
    grants.push_back(MDSCapGrant(MDSCapSpec(true, true, false), MDSCapMatch()));
    return true;
  }

  MDSCapParser<std::string::const_iterator> g;
  std::string::const_iterator iter = str.begin();
  std::string::const_iterator end = str.end();

  bool r = qi::phrase_parse(iter, end, g, ascii::space, *this);
  if (r && iter == end) {
    return true;
  } else {
    // Make sure no grants are kept after parsing failed!
    grants.clear();

    if (err)
      *err << "osdcap parse failed, stopped at '" << std::string(iter, end)
           << "' of '" << str << "'\n";
    return false; 
  }
}


bool MDSAuthCaps::allow_all() const
{
  for (std::vector<MDSCapGrant>::const_iterator i = grants.begin(); i != grants.end(); ++i) {
    if (i->match.is_match_all() && i->spec.allow_all()) {
      return true;
    }
  }

  return false;
}


ostream &operator<<(ostream &out, const MDSCapMatch &match)
{
  if (match.path != MDSCapMatch::MDS_AUTH_PATH_ROOT) {
    out << "path=\"" << match.path << "\"";
  }
  if (match.path != MDSCapMatch::MDS_AUTH_PATH_ROOT && match.uid != MDSCapMatch::MDS_AUTH_UID_ANY) {
    out << " ";
  }
  if (match.uid != MDSCapMatch::MDS_AUTH_UID_ANY) {
    out << "uid=" << match.uid;
  }

  return out;
}


ostream &operator<<(ostream &out, const MDSCapSpec &spec)
{
  if (spec.any) {
    out << "*";
  } else {
    if (spec.read) {
      out << "r";
    }
    if (spec.write) {
      out << "w";
    }
  }

  return out;
}


ostream &operator<<(ostream &out, const MDSCapGrant &grant)
{
  out << "allow ";
  out << grant.spec;
  if (!grant.match.is_match_all()) {
    out << " " << grant.match;
  }

  return out;
}


ostream &operator<<(ostream &out, const MDSAuthCaps &cap)
{
  out << "MDSAuthCaps[";
  for (size_t i = 0; i < cap.grants.size(); ++i) {
    out << cap.grants[i];
    if (i < cap.grants.size() - 1) {
      out << ", ";
    }
  }
  out << "]";

  return out;
}

