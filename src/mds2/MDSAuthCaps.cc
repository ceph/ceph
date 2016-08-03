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
#include <fcntl.h>

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix.hpp>

#include "common/debug.h"
#include "MDSAuthCaps.h"

#define dout_subsys ceph_subsys_mds

#undef dout_prefix
#define dout_prefix *_dout << "MDSAuthCap "

using std::ostream;
using std::string;
namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;
namespace phoenix = boost::phoenix;

template <typename Iterator>
struct MDSCapParser : qi::grammar<Iterator, MDSAuthCaps()>
{
  MDSCapParser() : MDSCapParser::base_type(mdscaps)
  {
    using qi::char_;
    using qi::int_;
    using qi::uint_;
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
    unquoted_path %= +char_("a-zA-Z0-9_./-");

    // match := [path=<path>] [uid=<uid> [gids=<gid>[,<gid>...]]
    path %= (spaces >> lit("path") >> lit('=') >> (quoted_path | unquoted_path));
    uid %= (spaces >> lit("uid") >> lit('=') >> uint_);
    uintlist %= (uint_ % lit(','));
    gidlist %= -(spaces >> lit("gids") >> lit('=') >> uintlist);
    match = -(
	     (uid >> gidlist)[_val = phoenix::construct<MDSCapMatch>(_1, _2)] |
	     (path >> uid >> gidlist)[_val = phoenix::construct<MDSCapMatch>(_1, _2, _3)] |
             (path)[_val = phoenix::construct<MDSCapMatch>(_1)]);

    // capspec = * | r[w]
    capspec = spaces >> (
        lit("*")[_val = MDSCapSpec(true, true, true, true)]
        |
        (lit("rwp"))[_val = MDSCapSpec(true, true, false, true)]
        |
        (lit("rw"))[_val = MDSCapSpec(true, true, false, false)]
        |
        (lit("r"))[_val = MDSCapSpec(true, false, false, false)]
        );

    grant = lit("allow") >> (capspec >> match)[_val = phoenix::construct<MDSCapGrant>(_1, _2)];
    grants %= (grant % (*lit(' ') >> (lit(';') | lit(',')) >> *lit(' ')));
    mdscaps = grants  [_val = phoenix::construct<MDSAuthCaps>(_1)]; 
  }
  qi::rule<Iterator> spaces;
  qi::rule<Iterator, string()> quoted_path, unquoted_path;
  qi::rule<Iterator, MDSCapSpec()> capspec;
  qi::rule<Iterator, string()> path;
  qi::rule<Iterator, uint32_t()> uid;
  qi::rule<Iterator, std::vector<uint32_t>() > uintlist;
  qi::rule<Iterator, std::vector<uint32_t>() > gidlist;
  qi::rule<Iterator, MDSCapMatch()> match;
  qi::rule<Iterator, MDSCapGrant()> grant;
  qi::rule<Iterator, std::vector<MDSCapGrant>()> grants;
  qi::rule<Iterator, MDSAuthCaps()> mdscaps;
};

void MDSCapMatch::normalize_path()
{
  // drop any leading /
  while (path.length() && path[0] == '/') {
    path = path.substr(1);
  }

  // drop dup //
  // drop .
  // drop ..
}

bool MDSCapMatch::match(const std::string &target_path,
			const int caller_uid,
			const int caller_gid) const
{
  if (uid != MDS_AUTH_UID_ANY) {
    if (uid != caller_uid)
      return false;
    if (std::find(gids.begin(), gids.end(), caller_gid) == gids.end())
      return false;
  }

  if (!match_path(target_path)) {
    return false;
  }

  return true;
}

bool MDSCapMatch::match_path(const std::string &target_path) const
{
  if (path.length()) {
    if (target_path.find(path) != 0)
      return false;
    // if path doesn't already have a trailing /, make sure the target
    // does so that path=/foo doesn't match target_path=/food
    if (target_path.length() > path.length() &&
	path[path.length()-1] != '/' &&
	target_path[path.length()] != '/')
      return false;
  }

  return true;
}

/**
 * Is the client *potentially* able to access this path?  Actual
 * permission will depend on uids/modes in the full is_capable.
 */
bool MDSAuthCaps::path_capable(const std::string &inode_path) const
{
  for (const auto &i : grants) {
    if (i.match.match_path(inode_path)) {
      return true;
    }
  }

  return false;
}

/**
 * For a given filesystem path, query whether this capability carries`
 * authorization to read or write.
 *
 * This is true if any of the 'grant' clauses in the capability match the
 * requested path + op.
 */
bool MDSAuthCaps::is_capable(const std::string &inode_path,
			     uid_t inode_uid, gid_t inode_gid,
			     unsigned inode_mode,
			     uid_t caller_uid, gid_t caller_gid,
			     unsigned mask,
			     uid_t new_uid, gid_t new_gid) const
{
  if (cct)
    ldout(cct, 10) << __func__ << " inode(path /" << inode_path
		   << " owner " << inode_uid << ":" << inode_gid
		   << " mode 0" << std::oct << inode_mode << std::dec
		   << ") by caller " << caller_uid << ":" << caller_gid
		   << " mask " << mask
		   << " new " << new_uid << ":" << new_gid
		   << " cap: " << *this << dendl;

  for (std::vector<MDSCapGrant>::const_iterator i = grants.begin();
       i != grants.end();
       ++i) {

    if (i->match.match(inode_path, caller_uid, caller_gid) &&
	i->spec.allows(mask & (MAY_READ|MAY_EXECUTE), mask & MAY_WRITE)) {

      // Spec is non-allowing if caller asked for set pool but spec forbids it
      if (mask & MAY_SET_POOL) {
        if (!i->spec.allows_set_pool()) {
          continue;
        }
      }

      // check unix permissions?
      if (i->match.uid == MDSCapMatch::MDS_AUTH_UID_ANY) {
        return true;
      }

      // chown/chgrp
      if (mask & MAY_CHOWN) {
	if (new_uid != caller_uid ||   // you can't chown to someone else
	    inode_uid != caller_uid) { // you can't chown from someone else
	  continue;
	}
      }
      if (mask & MAY_CHGRP) {
	// you can only chgrp *to* one of your groups... if you own the file.
	if (inode_uid != caller_uid ||
	    std::find(i->match.gids.begin(), i->match.gids.end(), new_gid) ==
	    i->match.gids.end()) {
	  continue;
	}
      }

      if (inode_uid == caller_uid) {
        if ((!(mask & MAY_READ) || (inode_mode & S_IRUSR)) &&
	    (!(mask & MAY_WRITE) || (inode_mode & S_IWUSR)) &&
	    (!(mask & MAY_EXECUTE) || (inode_mode & S_IXUSR))) {
          return true;
        }
      } else if (std::find(i->match.gids.begin(), i->match.gids.end(),
			   inode_gid) != i->match.gids.end()) {
        if ((!(mask & MAY_READ) || (inode_mode & S_IRGRP)) &&
	    (!(mask & MAY_WRITE) || (inode_mode & S_IWGRP)) &&
	    (!(mask & MAY_EXECUTE) || (inode_mode & S_IXGRP))) {
          return true;
        }
      } else {
        if ((!(mask & MAY_READ) || (inode_mode & S_IROTH)) &&
	    (!(mask & MAY_WRITE) || (inode_mode & S_IWOTH)) &&
	    (!(mask & MAY_EXECUTE) || (inode_mode & S_IXOTH))) {
          return true;
        }
      }
    }
  }

  return false;
}

void MDSAuthCaps::set_allow_all()
{
    grants.clear();
    grants.push_back(MDSCapGrant(
                       MDSCapSpec(true, true, true, true),
                       MDSCapMatch()));
}

bool MDSAuthCaps::parse(CephContext *c, const std::string& str, ostream *err)
{
  // Special case for legacy caps
  if (str == "allow") {
    grants.clear();
    grants.push_back(MDSCapGrant(MDSCapSpec(true, true, false, true), MDSCapMatch()));
    return true;
  }

  MDSCapParser<std::string::const_iterator> g;
  std::string::const_iterator iter = str.begin();
  std::string::const_iterator end = str.end();

  bool r = qi::phrase_parse(iter, end, g, ascii::space, *this);
  cct = c;  // set after parser self-assignment
  if (r && iter == end) {
    return true;
  } else {
    // Make sure no grants are kept after parsing failed!
    grants.clear();

    if (err)
      *err << "MDSAuthCaps parse failed, stopped at '" << std::string(iter, end)
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
  if (match.path.length()) {
    out << "path=\"/" << match.path << "\"";
    if (match.uid != MDSCapMatch::MDS_AUTH_UID_ANY) {
      out << " ";
    }
  }
  if (match.uid != MDSCapMatch::MDS_AUTH_UID_ANY) {
    out << "uid=" << match.uid;
    if (!match.gids.empty()) {
      out << " gids=";
      for (std::vector<gid_t>::const_iterator p = match.gids.begin();
	   p != match.gids.end();
	   ++p) {
	if (p != match.gids.begin())
	  out << ',';
	out << *p;
      }
    }
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

