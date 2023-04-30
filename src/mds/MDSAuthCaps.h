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
#ifndef MDS_AUTH_CAPS_H
#define MDS_AUTH_CAPS_H

#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "include/common_fwd.h"
#include "include/types.h"
#include "common/debug.h"

#include "mdstypes.h"

// unix-style capabilities
enum {
  MAY_READ	= (1 << 0),
  MAY_WRITE 	= (1 << 1),
  MAY_EXECUTE	= (1 << 2),
  MAY_CHOWN	= (1 << 4),
  MAY_CHGRP	= (1 << 5),
  MAY_SET_VXATTR = (1 << 6),
  MAY_SNAPSHOT	= (1 << 7),
  MAY_FULL	= (1 << 8),
};

// what we can do
struct MDSCapSpec {
  static const unsigned ALL		= (1 << 0);
  static const unsigned READ		= (1 << 1);
  static const unsigned WRITE		= (1 << 2);
  // if the capability permits setting vxattrs (layout, quota, etc)
  static const unsigned SET_VXATTR	= (1 << 3);
  // if the capability permits mksnap/rmsnap
  static const unsigned SNAPSHOT	= (1 << 4);
  // if the capability permits to bypass osd full check
  static const unsigned FULL	        = (1 << 5);

  static const unsigned RW		= (READ|WRITE);
  static const unsigned RWF		= (READ|WRITE|FULL);
  static const unsigned RWP		= (READ|WRITE|SET_VXATTR);
  static const unsigned RWS		= (READ|WRITE|SNAPSHOT);
  static const unsigned RWFP		= (READ|WRITE|FULL|SET_VXATTR);
  static const unsigned RWFS		= (READ|WRITE|FULL|SNAPSHOT);
  static const unsigned RWPS		= (READ|WRITE|SET_VXATTR|SNAPSHOT);
  static const unsigned RWFPS		= (READ|WRITE|FULL|SET_VXATTR|SNAPSHOT);

  MDSCapSpec() = default;
  MDSCapSpec(unsigned _caps) : caps(_caps) {
    if (caps & ALL)
      caps |= RWFPS;
  }

  bool allow_all() const {
    return (caps & ALL);
  }
  bool allow_read() const {
    return (caps & READ);
  }
  bool allow_write() const {
    return (caps & WRITE);
  }

  bool allows(bool r, bool w) const {
    if (allow_all())
      return true;
    if (r && !allow_read())
      return false;
    if (w && !allow_write())
      return false;
    return true;
  }

  bool allow_snapshot() const {
    return (caps & SNAPSHOT);
  }
  bool allow_set_vxattr() const {
    return (caps & SET_VXATTR);
  }
  bool allow_full() const {
    return (caps & FULL);
  }
private:
  unsigned caps = 0;
};

// conditions before we are allowed to do it
struct MDSCapMatch {
  static const int64_t MDS_AUTH_UID_ANY = -1;

  MDSCapMatch() {}

  MDSCapMatch(const std::string& fsname_, const std::string& path_,
	      bool root_squash_, int64_t uid_=MDS_AUTH_UID_ANY,
	      const std::vector<gid_t>& gids_={}) {
    fs_name = std::move(fsname_);
    path = std::move(path_);
    root_squash = root_squash_;
    uid = (uid_ == 0) ? -1 : uid_;
    gids = gids_;

    normalize_path();
  }

  void normalize_path();
  
  bool is_match_all() const
  {
    return uid == MDS_AUTH_UID_ANY && path == "";
  }

  // check whether this grant matches against a given file and caller uid:gid
  bool match(std::string_view target_path,
	     const int caller_uid,
	     const int caller_gid,
	     const std::vector<uint64_t> *caller_gid_list) const;

  /**
   * Check whether this path *might* be accessible (actual permission
   * depends on the stronger check in match()).
   *
   * @param target_path filesystem path without leading '/'
   */
  bool match_path(std::string_view target_path) const;

  // Require UID to be equal to this, if !=MDS_AUTH_UID_ANY
  int64_t uid = MDS_AUTH_UID_ANY;
  std::vector<gid_t> gids;  // Use these GIDs
  std::string path;  // Require path to be child of this (may be "" or "/" for any)
  std::string fs_name;
  bool root_squash=false;
};

struct MDSCapGrant {
  MDSCapGrant(const MDSCapSpec &spec_, const MDSCapMatch &match_,
	      boost::optional<std::string> n)
    : spec(spec_), match(match_) {
    if (n) {
      network = *n;
      parse_network();
    }
  }
  MDSCapGrant() {}

  void parse_network();

  MDSCapSpec spec;
  MDSCapMatch match;

  std::string network;

  entity_addr_t network_parsed;
  unsigned network_prefix = 0;
  bool network_valid = true;
};

class MDSAuthCaps
{
public:
  MDSAuthCaps() = default;

  // this ctor is used by spirit/phoenix
  explicit MDSAuthCaps(const std::vector<MDSCapGrant>& grants_) : grants(grants_) {}

  void clear() {
    grants.clear();
  }

  void set_allow_all();
  bool parse(std::string_view str, std::ostream *err);

  bool allow_all() const;
  bool is_capable(std::string_view inode_path,
		  uid_t inode_uid, gid_t inode_gid, unsigned inode_mode,
		  uid_t uid, gid_t gid, const std::vector<uint64_t> *caller_gid_list,
		  unsigned mask, uid_t new_uid, gid_t new_gid,
		  const entity_addr_t& addr) const;
  bool path_capable(std::string_view inode_path) const;

  bool fs_name_capable(std::string_view fs_name, unsigned mask) const {
    if (allow_all()) {
      return true;
    }

    for (const MDSCapGrant &g : grants) {
      if (g.match.fs_name == fs_name || g.match.fs_name.empty() ||
	  g.match.fs_name == "*") {
	if (mask & MAY_READ && g.spec.allow_read()) {
	  return true;
	}

	if (mask & MAY_WRITE && g.spec.allow_write()) {
	  return true;
	}
      }
    }

    return false;
  }

  friend std::ostream &operator<<(std::ostream &out, const MDSAuthCaps &cap);
private:
  std::vector<MDSCapGrant> grants;
};

std::ostream &operator<<(std::ostream &out, const MDSCapMatch &match);
std::ostream &operator<<(std::ostream &out, const MDSCapSpec &spec);
std::ostream &operator<<(std::ostream &out, const MDSCapGrant &grant);
std::ostream &operator<<(std::ostream &out, const MDSAuthCaps &cap);

#endif // MDS_AUTH_CAPS_H
