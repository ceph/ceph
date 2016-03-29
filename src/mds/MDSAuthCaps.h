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

#include <vector>
#include <string>
#include <sstream>
#include "include/types.h"
#include "common/debug.h"

// unix-style capabilities
enum {
  MAY_READ = 1,
  MAY_WRITE = 2,
  MAY_EXECUTE = 4,
  MAY_CHOWN = 16,
  MAY_CHGRP = 32,
  MAY_SET_POOL = 64,
};

class CephContext;

// what we can do
struct MDSCapSpec {
  bool read, write, any;

  // True if the capability permits modifying the pool on file layouts
  bool layout_pool;

  MDSCapSpec() : read(false), write(false), any(false), layout_pool(false) {}
  MDSCapSpec(bool r, bool w, bool a, bool lop)
    : read(r), write(w), any(a), layout_pool(lop) {}

  bool allow_all() const {
    return any;
  }

  bool allows(bool r, bool w) const {
    if (any)
      return true;
    if (r && !read)
      return false;
    if (w && !write)
      return false;
    return true;
  }

  bool allows_set_pool() const {
    return layout_pool;
  }
};

// conditions before we are allowed to do it
struct MDSCapMatch {
  static const int64_t MDS_AUTH_UID_ANY = -1;

  int64_t uid;       // Require UID to be equal to this, if !=MDS_AUTH_UID_ANY
  std::vector<gid_t> gids;  // Use these GIDs
  std::string path;  // Require path to be child of this (may be "" or "/" for any)

  MDSCapMatch() : uid(MDS_AUTH_UID_ANY) {}
  MDSCapMatch(int64_t uid_, std::vector<gid_t>& gids_) : uid(uid_), gids(gids_) {}
  explicit MDSCapMatch(std::string path_)
    : uid(MDS_AUTH_UID_ANY), path(path_) {
    normalize_path();
  }
  MDSCapMatch(std::string path_, int64_t uid_, std::vector<gid_t>& gids_)
    : uid(uid_), gids(gids_), path(path_) {
    normalize_path();
  }

  void normalize_path();
  
  bool is_match_all() const
  {
    return uid == MDS_AUTH_UID_ANY && path == "";
  }

  // check whether this grant matches against a given file and caller uid:gid
  bool match(const std::string &target_path,
	     const int caller_uid,
	     const int caller_gid) const;

  /**
   * Check whether this path *might* be accessible (actual permission
   * depends on the stronger check in match()).
   *
   * @param target_path filesystem path without leading '/'
   */
  bool match_path(const std::string &target_path) const;
};

struct MDSCapGrant {
  MDSCapSpec spec;
  MDSCapMatch match;

  MDSCapGrant(const MDSCapSpec &spec_, const MDSCapMatch &match_)
    : spec(spec_), match(match_) {}
  MDSCapGrant() {}
};

class MDSAuthCaps
{
  CephContext *cct;
  std::vector<MDSCapGrant> grants;

public:
  explicit MDSAuthCaps(CephContext *cct_=NULL)
    : cct(cct_) { }

  // this ctor is used by spirit/phoenix; doesn't need cct.
  explicit MDSAuthCaps(const std::vector<MDSCapGrant> &grants_)
    : cct(NULL), grants(grants_) { }

  void set_allow_all();
  bool parse(CephContext *cct, const std::string &str, std::ostream *err);

  bool allow_all() const;
  bool is_capable(const std::string &inode_path,
		  uid_t inode_uid, gid_t inode_gid, unsigned inode_mode,
		  uid_t uid, gid_t gid, unsigned mask,
		  uid_t new_uid, gid_t new_gid) const;
  bool path_capable(const std::string &inode_path) const;

  friend std::ostream &operator<<(std::ostream &out, const MDSAuthCaps &cap);
};


std::ostream &operator<<(std::ostream &out, const MDSCapMatch &match);
std::ostream &operator<<(std::ostream &out, const MDSCapSpec &spec);
std::ostream &operator<<(std::ostream &out, const MDSCapGrant &grant);
std::ostream &operator<<(std::ostream &out, const MDSAuthCaps &cap);

#endif // MDS_AUTH_CAPS_H
