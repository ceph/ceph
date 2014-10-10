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


struct MDSCapSpec {
  bool read;
  bool write;
  bool any;

  MDSCapSpec() : read(false), write(false), any(false) {}
  MDSCapSpec(bool r_, bool w_, bool a_) : read(r_), write(w_), any(a_) {}

  bool allow_all() const {return any;}
};

struct MDSCapMatch {
  static const int MDS_AUTH_UID_ANY = -1;
  static const std::string MDS_AUTH_PATH_ROOT;

  int uid;  // Require UID to be equal to this, if !=MDS_AUTH_UID_ANY
  std::string path;  // Require path to be child of this (may be "/" for any)

  MDSCapMatch() : uid(MDS_AUTH_UID_ANY), path(MDS_AUTH_PATH_ROOT) {}
  MDSCapMatch(int uid_) : uid(uid_), path(MDS_AUTH_PATH_ROOT) {}
  MDSCapMatch(std::string path_) : uid(MDS_AUTH_UID_ANY), path(path_) {}
  MDSCapMatch(std::string path_, int uid_) : uid(uid_), path(path_) {}
  
  bool is_match_all() const
  {
    return uid == MDS_AUTH_UID_ANY && path == "/";
  }

  bool match(const std::string &target_path, const int target_uid) const {
    return (target_path.find(path) == 0 && (target_uid == uid || uid == MDS_AUTH_UID_ANY)); 
  }
};

struct MDSCapGrant {
  MDSCapSpec spec;
  MDSCapMatch match;

  MDSCapGrant(const MDSCapSpec &spec_, const MDSCapMatch &match_) : spec(spec_), match(match_) {}
  MDSCapGrant() {}
};

class MDSAuthCaps
{
    protected:
    std::vector<MDSCapGrant> grants;

    public:
    void set_allow_all();
    bool parse(const std::string &str, std::ostream *err);
    MDSAuthCaps() {}
    MDSAuthCaps(const std::vector<MDSCapGrant> &grants_) : grants(grants_) {}

    bool allow_all() const;
    bool is_capable(const std::string &path, int uid, bool may_read, bool may_write) const;
    friend std::ostream &operator<<(std::ostream &out, const MDSAuthCaps &cap);
};


std::ostream &operator<<(std::ostream &out, const MDSCapMatch &match);
std::ostream &operator<<(std::ostream &out, const MDSCapSpec &spec);
std::ostream &operator<<(std::ostream &out, const MDSCapGrant &grant);
std::ostream &operator<<(std::ostream &out, const MDSAuthCaps &cap);

#endif // MDS_AUTH_CAPS_H
