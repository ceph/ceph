// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 * OSDCaps: Hold the capabilities associated with a single authenticated 
 * user key. These are specified by text strings of the form
 * "allow r" (which allows reading anything on the OSD)
 * "allow rwx auid foo" (which allows full access to listed auids)
 *  "allow rwx pool foo" (which allows full access to listed pools)
 * "allow *" (which allows full access to EVERYTHING)
 *
 * The full grammar is documented in the parser in OSDCap.cc.
 *
 * The OSD assumes that anyone with * caps is an admin and has full
 * message permissions. This means that only the monitor and the OSDs
 * should get *
 */

#ifndef CEPH_OSDCAP_H
#define CEPH_OSDCAP_H

#include <ostream>
using std::ostream;

#include "include/types.h"

static const __u8 OSD_CAP_R     = (1 << 1);      // read
static const __u8 OSD_CAP_W     = (1 << 2);      // write
static const __u8 OSD_CAP_CLS_R = (1 << 3);      // class read
static const __u8 OSD_CAP_CLS_W = (1 << 4);      // class write
static const __u8 OSD_CAP_X     = (OSD_CAP_CLS_R | OSD_CAP_CLS_W); // execute
static const __u8 OSD_CAP_ANY   = 0xff;          // *

struct osd_rwxa_t {
  __u8 val;

  // cppcheck-suppress noExplicitConstructor
  osd_rwxa_t(__u8 v = 0) : val(v) {}
  osd_rwxa_t& operator=(__u8 v) {
    val = v;
    return *this;
  }
  operator __u8() const {
    return val;
  }
};

ostream& operator<<(ostream& out, osd_rwxa_t p);

struct OSDCapSpec {
  osd_rwxa_t allow;
  std::string class_name;
  std::string class_allow;

  OSDCapSpec() : allow(0) {}
  explicit OSDCapSpec(osd_rwxa_t v) : allow(v) {}
  explicit OSDCapSpec(std::string n) : allow(0), class_name(n) {}
  OSDCapSpec(std::string n, std::string a) : allow(0), class_name(n), class_allow(a) {}

  bool allow_all() const {
    return allow == OSD_CAP_ANY;
  }
};

ostream& operator<<(ostream& out, const OSDCapSpec& s);


struct OSDCapMatch {
  // auid and pool_name/nspace are mutually exclusive
  int64_t auid;
  std::string pool_name;
  bool is_nspace;      // true if nspace is defined; false if not constrained.
  std::string nspace;

  std::string object_prefix;

  OSDCapMatch() : auid(CEPH_AUTH_UID_DEFAULT), is_nspace(false) {}
  OSDCapMatch(std::string pl, std::string pre) :
	auid(CEPH_AUTH_UID_DEFAULT), pool_name(pl), is_nspace(false), object_prefix(pre) {}
  OSDCapMatch(std::string pl, std::string ns, std::string pre) :
	auid(CEPH_AUTH_UID_DEFAULT), pool_name(pl), is_nspace(true), nspace(ns), object_prefix(pre) {}
  OSDCapMatch(uint64_t auid, std::string pre) : auid(auid), is_nspace(false), object_prefix(pre) {}

  /**
   * check if given request parameters match our constraints
   *
   * @param pool_name pool name
   * @param nspace_name namespace name
   * @param pool_auid pool's auid
   * @param object object name
   * @return true if we match, false otherwise
   */
  bool is_match(const std::string& pool_name, const std::string& nspace_name, int64_t pool_auid, const std::string& object) const;
  bool is_match_all() const;
};

ostream& operator<<(ostream& out, const OSDCapMatch& m);


struct OSDCapGrant {
  OSDCapMatch match;
  OSDCapSpec spec;

  OSDCapGrant() {}
  OSDCapGrant(OSDCapMatch m, OSDCapSpec s) : match(m), spec(s) {}
};

ostream& operator<<(ostream& out, const OSDCapGrant& g);


struct OSDCap {
  std::vector<OSDCapGrant> grants;

  OSDCap() {}
  explicit OSDCap(std::vector<OSDCapGrant> g) : grants(g) {}

  bool allow_all() const;
  void set_allow_all();
  bool parse(const std::string& str, ostream *err=NULL);

  /**
   * check if we are capable of something
   *
   * This method actually checks a description of a particular operation against
   * what the capability has specified.  Currently that is just rwx with matches
   * against pool, pool auid, and object name prefix.
   *
   * @param pool_name name of the pool we are accessing
   * @param ns name of the namespace we are accessing
   * @param pool_auid owner of the pool we are accessing
   * @param object name of the object we are accessing
   * @param op_may_read whether the operation may need to read
   * @param op_may_write whether the operation may need to write
   * @param op_may_class_read whether the operation needs to call a
   *                          read class method
   * @param op_may_class_write whether the operation needs to call a
   *                          write class method
   * @return true if the operation is allowed, false otherwise
   */
  bool is_capable(const string& pool_name, const string& ns, int64_t pool_auid,
		  const string& object, bool op_may_read, bool op_may_write,
		  bool op_may_class_read, bool op_may_class_write) const;
};

static inline ostream& operator<<(ostream& out, const OSDCap& cap) 
{
  return out << "osdcap" << cap.grants;
}

#endif
