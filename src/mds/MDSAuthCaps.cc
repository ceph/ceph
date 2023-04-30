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

#include <string_view>

#include <errno.h>

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix.hpp>

#include "common/debug.h"
#include "MDSAuthCaps.h"
#include "mdstypes.h"
#include "include/ipaddr.h"

#define dout_subsys ceph_subsys_mds

#undef dout_prefix
#define dout_prefix *_dout << "MDSAuthCap "

using std::ostream;
using std::string;
using std::vector;
namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;
namespace phoenix = boost::phoenix;

template <typename Iterator>
struct MDSCapParser : qi::grammar<Iterator, MDSAuthCaps()>
{
  MDSCapParser() : MDSCapParser::base_type(mdscaps)
  {
    using qi::attr;
    using qi::bool_;
    using qi::char_;
    using qi::int_;
    using qi::uint_;
    using qi::lexeme;
    using qi::alnum;
    using qi::_val;
    using qi::_1;
    using qi::_2;
    using qi::_3;
    using qi::_4;
    using qi::_5;
    using qi::eps;
    using qi::lit;

    spaces = +(lit(' ') | lit('\n') | lit('\t'));

    quoted_path %=
      lexeme[lit("\"") >> *(char_ - '"') >> '"'] | 
      lexeme[lit("'") >> *(char_ - '\'') >> '\''];
    unquoted_path %= +char_("a-zA-Z0-9_./-");
    network_str %= +char_("/.:a-fA-F0-9][");
    fs_name_str %= +char_("a-zA-Z0-9_.-");

    path %= -(spaces >> lit("path") >> lit('=') >> (quoted_path | unquoted_path));
    uid %= -(spaces >> lit("uid") >> lit('=') >> uint_);
    uintlist %= (uint_ % lit(','));
    gidlist %= -(spaces >> lit("gids") >> lit('=') >> uintlist);
    fs_name %= -(spaces >> lit("fsname") >> lit('=') >> fs_name_str);
    root_squash %= -(spaces >> lit("root_squash") >> attr(true));
    match = (fs_name >> path >> root_squash >> uid >> gidlist)[_val = phoenix::construct<MDSCapMatch>(_1, _2, _3, _4, _5)];

    // capspec = * | r[w][f][p][s]
    capspec = spaces >> (
        lit("*")[_val = MDSCapSpec(MDSCapSpec::ALL)]
        |
        lit("all")[_val = MDSCapSpec(MDSCapSpec::ALL)]
        |
        (lit("rwfps"))[_val = MDSCapSpec(MDSCapSpec::RWFPS)]
        |
        (lit("rwps"))[_val = MDSCapSpec(MDSCapSpec::RWPS)]
        |
        (lit("rwfp"))[_val = MDSCapSpec(MDSCapSpec::RWFP)]
        |
        (lit("rwfs"))[_val = MDSCapSpec(MDSCapSpec::RWFS)]
        |
        (lit("rwp"))[_val = MDSCapSpec(MDSCapSpec::RWP)]
        |
        (lit("rws"))[_val = MDSCapSpec(MDSCapSpec::RWS)]
        |
        (lit("rwf"))[_val = MDSCapSpec(MDSCapSpec::RWF)]
        |
        (lit("rw"))[_val = MDSCapSpec(MDSCapSpec::RW)]
        |
        (lit("r"))[_val = MDSCapSpec(MDSCapSpec::READ)]
        );

    grant = lit("allow") >> (capspec >> match >>
			     -(spaces >> lit("network") >> spaces >> network_str))
      [_val = phoenix::construct<MDSCapGrant>(_1, _2, _3)];
    grants %= (grant % (*lit(' ') >> (lit(';') | lit(',')) >> *lit(' ')));
    mdscaps = grants  [_val = phoenix::construct<MDSAuthCaps>(_1)]; 
  }
  qi::rule<Iterator> spaces;
  qi::rule<Iterator, string()> quoted_path, unquoted_path, network_str;
  qi::rule<Iterator, string()> fs_name_str, fs_name, path;
  qi::rule<Iterator, bool()> root_squash;
  qi::rule<Iterator, MDSCapSpec()> capspec;
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

bool MDSCapMatch::match(std::string_view target_path,
			const int caller_uid,
			const int caller_gid,
			const vector<uint64_t> *caller_gid_list) const
{
  if (uid != MDS_AUTH_UID_ANY) {
    if (uid != caller_uid)
      return false;
    if (!gids.empty()) {
      bool gid_matched = false;
      if (std::find(gids.begin(), gids.end(), caller_gid) != gids.end())
	gid_matched = true;
      if (caller_gid_list) {
	for (auto i = caller_gid_list->begin(); i != caller_gid_list->end(); ++i) {
	  if (std::find(gids.begin(), gids.end(), *i) != gids.end()) {
	    gid_matched = true;
	    break;
	  }
	}
      }
      if (!gid_matched)
	return false;
    }
  }

  if (!match_path(target_path)) {
    return false;
  }

  return true;
}

bool MDSCapMatch::match_path(std::string_view target_path) const
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

void MDSCapGrant::parse_network()
{
  network_valid = ::parse_network(network.c_str(), &network_parsed,
				  &network_prefix);
}

/**
 * Is the client *potentially* able to access this path?  Actual
 * permission will depend on uids/modes in the full is_capable.
 */
bool MDSAuthCaps::path_capable(std::string_view inode_path) const
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
bool MDSAuthCaps::is_capable(std::string_view inode_path,
			     uid_t inode_uid, gid_t inode_gid,
			     unsigned inode_mode,
			     uid_t caller_uid, gid_t caller_gid,
			     const vector<uint64_t> *caller_gid_list,
			     unsigned mask,
			     uid_t new_uid, gid_t new_gid,
			     const entity_addr_t& addr) const
{
  ldout(g_ceph_context, 10) << __func__ << " inode(path /" << inode_path
		 << " owner " << inode_uid << ":" << inode_gid
		 << " mode 0" << std::oct << inode_mode << std::dec
		 << ") by caller " << caller_uid << ":" << caller_gid
// << "[" << caller_gid_list << "]";
		 << " mask " << mask
		 << " new " << new_uid << ":" << new_gid
		 << " cap: " << *this << dendl;

  for (const auto& grant : grants) {
    if (grant.network.size() &&
	(!grant.network_valid ||
	 !network_contains(grant.network_parsed,
			   grant.network_prefix,
			   addr))) {
      continue;
    }

    if (grant.match.match(inode_path, caller_uid, caller_gid, caller_gid_list) &&
	grant.spec.allows(mask & (MAY_READ|MAY_EXECUTE), mask & MAY_WRITE)) {
      if (grant.match.root_squash && ((caller_uid == 0) || (caller_gid == 0)) &&
          (mask & MAY_WRITE)) {
	    continue;
      }
      // we have a match; narrow down GIDs to those specifically allowed here
      vector<uint64_t> gids;
      if (std::find(grant.match.gids.begin(), grant.match.gids.end(), caller_gid) !=
	  grant.match.gids.end()) {
	gids.push_back(caller_gid);
      }
      if (caller_gid_list) {
	std::set_intersection(grant.match.gids.begin(), grant.match.gids.end(),
			      caller_gid_list->begin(), caller_gid_list->end(),
			      std::back_inserter(gids));
	std::sort(gids.begin(), gids.end());
      }
      

      // Spec is non-allowing if caller asked for set pool but spec forbids it
      if (mask & MAY_SET_VXATTR) {
        if (!grant.spec.allow_set_vxattr()) {
          continue;
        }
      }

      if (mask & MAY_SNAPSHOT) {
        if (!grant.spec.allow_snapshot()) {
          continue;
        }
      }

      if (mask & MAY_FULL) {
        if (!grant.spec.allow_full()) {
          continue;
        }
      }

      // check unix permissions?
      if (grant.match.uid == MDSCapMatch::MDS_AUTH_UID_ANY) {
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
	    std::find(gids.begin(), gids.end(), new_gid) ==
	    gids.end()) {
	  continue;
	}
      }

      if (inode_uid == caller_uid) {
        if ((!(mask & MAY_READ) || (inode_mode & S_IRUSR)) &&
	    (!(mask & MAY_WRITE) || (inode_mode & S_IWUSR)) &&
	    (!(mask & MAY_EXECUTE) || (inode_mode & S_IXUSR))) {
          return true;
        }
      } else if (std::find(gids.begin(), gids.end(),
			   inode_gid) != gids.end()) {
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
    grants.push_back(MDSCapGrant(MDSCapSpec(MDSCapSpec::ALL), MDSCapMatch(),
				 {}));
}

bool MDSAuthCaps::parse(std::string_view str, ostream *err)
{
  // Special case for legacy caps
  if (str == "allow") {
    grants.clear();
    grants.push_back(MDSCapGrant(MDSCapSpec(MDSCapSpec::RWPS), MDSCapMatch(),
				 {}));
    return true;
  }

  auto iter = str.begin();
  auto end = str.end();
  MDSCapParser<decltype(iter)> g;

  bool r = qi::phrase_parse(iter, end, g, ascii::space, *this);
  if (r && iter == end) {
    for (auto& grant : grants) {
      std::sort(grant.match.gids.begin(), grant.match.gids.end());
      grant.parse_network();
    }
    return true;
  } else {
    // Make sure no grants are kept after parsing failed!
    grants.clear();

    if (err)
      *err << "mds capability parse failed, stopped at '"
	   << std::string(iter, end)
           << "' of '" << str << "'";
    return false; 
  }
}


bool MDSAuthCaps::allow_all() const
{
  for (const auto& grant : grants) {
    if (grant.match.is_match_all() && grant.spec.allow_all()) {
      return true;
    }
  }

  return false;
}


ostream &operator<<(ostream &out, const MDSCapMatch &match)
{
  if (!match.fs_name.empty()) {
    out << " fsname=" << match.fs_name;
  }
  if (match.path.length()) {
    out << " path=\"/" << match.path << "\"";
  }
  if (match.root_squash) {
    out << " root_squash";
  }
  if (match.uid != MDSCapMatch::MDS_AUTH_UID_ANY) {
    out << " uid=" << match.uid;
    if (!match.gids.empty()) {
      out << " gids=";
      bool first = true;
      for (const auto& gid : match.gids) {
	if (!first)
	  out << ',';
	out << gid;
        first = false;
      }
    }
  }

  return out;
}


ostream &operator<<(ostream &out, const MDSCapSpec &spec)
{
  if (spec.allow_all()) {
    out << "*";
  } else {
    if (spec.allow_read()) {
      out << "r";
    }
    if (spec.allow_write()) {
      out << "w";
    }
    if (spec.allow_full()) {
      out << "f";
    }
    if (spec.allow_set_vxattr()) {
      out << "p";
    }
    if (spec.allow_snapshot()) {
      out << "s";
    }
  }

  return out;
}


ostream &operator<<(ostream &out, const MDSCapGrant &grant)
{
  out << "allow ";
  out << grant.spec;
  out << grant.match;
  if (grant.network.size()) {
    out << " network " << grant.network;
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

