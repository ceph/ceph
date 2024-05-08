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

#include <list>
#include <vector>
#include <boost/optional.hpp>
#include <boost/fusion/include/adapt_struct.hpp>

#include "include/types.h"
#include "osd/osd_op_util.h"


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

ostream& operator<<(ostream& out, const osd_rwxa_t& p);

struct OSDCapSpec {
  osd_rwxa_t allow;
  std::string class_name;
  std::string method_name;

  OSDCapSpec() : allow(0) {}
  explicit OSDCapSpec(osd_rwxa_t v) : allow(v) {}
  OSDCapSpec(std::string class_name, std::string method_name)
    : allow(0), class_name(std::move(class_name)),
      method_name(std::move(method_name)) {}

  bool allow_all() const {
    return allow == OSD_CAP_ANY;
  }
};

ostream& operator<<(ostream& out, const OSDCapSpec& s);

struct OSDCapPoolNamespace {
  std::string pool_name;
  boost::optional<std::string> nspace = boost::none;

  OSDCapPoolNamespace() {
  }
  OSDCapPoolNamespace(const std::string& pool_name,
                      const boost::optional<std::string>& nspace = boost::none)
    : pool_name(pool_name), nspace(nspace) {
  }

  bool is_match(const std::string& pn, const std::string& ns) const;
  bool is_match_all() const;
};

ostream& operator<<(ostream& out, const OSDCapPoolNamespace& pns);

struct OSDCapPoolTag {
  typedef std::map<std::string, std::map<std::string, std::string> > app_map_t;
  std::string application;
  std::string key;
  std::string value;

  OSDCapPoolTag () {}
  OSDCapPoolTag(const std::string& application, const std::string& key,
		const std::string& value) :
    application(application), key(key), value(value) {}

  bool is_match(const app_map_t& app_map) const;
  bool is_match_all() const;
};
// adapt for parsing with boost::spirit::qi in OSDCapParser
BOOST_FUSION_ADAPT_STRUCT(OSDCapPoolTag,
			  (std::string, application)
			  (std::string, key)
			  (std::string, value))

ostream& operator<<(ostream& out, const OSDCapPoolTag& pt);

struct OSDCapMatch {
  typedef std::map<std::string, std::map<std::string, std::string> > app_map_t;
  OSDCapPoolNamespace pool_namespace;
  OSDCapPoolTag pool_tag;
  std::string object_prefix;

  OSDCapMatch() {}
  explicit OSDCapMatch(const OSDCapPoolTag& pt) : pool_tag(pt) {}
  explicit OSDCapMatch(const OSDCapPoolNamespace& pns) : pool_namespace(pns) {}
  OSDCapMatch(const OSDCapPoolNamespace& pns, const std::string& pre)
    : pool_namespace(pns), object_prefix(pre) {}
  OSDCapMatch(const std::string& pl, const std::string& pre)
    : pool_namespace(pl), object_prefix(pre) {}
  OSDCapMatch(const std::string& pl, const std::string& ns,
              const std::string& pre)
    : pool_namespace(pl, ns), object_prefix(pre) {}
  OSDCapMatch(const std::string& dummy, const std::string& app,
	      const std::string& key, const std::string& val)
    : pool_tag(app, key, val) {}
  OSDCapMatch(const std::string& ns, const OSDCapPoolTag& pt)
    : pool_namespace("", ns), pool_tag(pt) {}

  /**
   * check if given request parameters match our constraints
   *
   * @param pool_name pool name
   * @param nspace_name namespace name
   * @param object object name
   * @return true if we match, false otherwise
   */
  bool is_match(const std::string& pool_name, const std::string& nspace_name,
                const app_map_t& app_map,
		const std::string& object) const;
  bool is_match_all() const;
};

ostream& operator<<(ostream& out, const OSDCapMatch& m);


struct OSDCapProfile {
  std::string name;
  OSDCapPoolNamespace pool_namespace;

  OSDCapProfile() {
  }
  OSDCapProfile(const std::string& name,
                const std::string& pool_name,
                const boost::optional<std::string>& nspace = boost::none)
    : name(name), pool_namespace(pool_name, nspace) {
  }

  inline bool is_valid() const {
    return !name.empty();
  }
};

ostream& operator<<(ostream& out, const OSDCapProfile& m);

struct OSDCapGrant {
  OSDCapMatch match;
  OSDCapSpec spec;
  OSDCapProfile profile;
  std::string network;
  entity_addr_t network_parsed;
  unsigned network_prefix = 0;
  bool network_valid = true;

  // explicit grants that a profile grant expands to; populated as
  // needed by expand_profile() and cached here.
  std::list<OSDCapGrant> profile_grants;

  OSDCapGrant() {}
  OSDCapGrant(const OSDCapMatch& m, const OSDCapSpec& s,
	      boost::optional<std::string> n = {})
    : match(m), spec(s) {
    if (n) {
      set_network(*n);
    }
  }
  explicit OSDCapGrant(const OSDCapProfile& profile,
		       boost::optional<std::string> n = {})
    : profile(profile) {
    if (n) {
      set_network(*n);
    }
    expand_profile();
  }

  void set_network(const std::string& n);

  bool allow_all() const;
  bool is_capable(const std::string& pool_name, const std::string& ns,
		  const OSDCapPoolTag::app_map_t& application_metadata,
                  const std::string& object, bool op_may_read, bool op_may_write,
                  const std::vector<OpInfo::ClassInfo>& classes,
		  const entity_addr_t& addr,
                  std::vector<bool>* class_allowed) const;

  void expand_profile();
  std::string to_string();
};

ostream& operator<<(ostream& out, const OSDCapGrant& g);


struct OSDCap {
  std::vector<OSDCapGrant> grants;

  OSDCap() {}
  explicit OSDCap(std::vector<OSDCapGrant> g) : grants(std::move(g)) {}

  bool allow_all() const;
  void set_allow_all();
  bool parse(const std::string& str, ostream *err=NULL);
  bool merge(OSDCap newcap);
  std::string to_string();

  /**
   * check if we are capable of something
   *
   * This method actually checks a description of a particular operation against
   * what the capability has specified.  Currently that is just rwx with matches
   * against pool, and object name prefix.
   *
   * @param pool_name name of the pool we are accessing
   * @param ns name of the namespace we are accessing
   * @param object name of the object we are accessing
   * @param op_may_read whether the operation may need to read
   * @param op_may_write whether the operation may need to write
   * @param classes (class-name, rd, wr, allowed-flag) tuples
   * @return true if the operation is allowed, false otherwise
   */
  bool is_capable(const std::string& pool_name, const std::string& ns,
		  const OSDCapPoolTag::app_map_t& application_metadata,
		  const std::string& object, bool op_may_read, bool op_may_write,
		  const std::vector<OpInfo::ClassInfo>& classes,
		  const entity_addr_t& addr) const;
};

inline std::ostream& operator<<(std::ostream& out, const OSDCap& cap) 
{
  return out << "osdcap" << cap.grants;
}

#endif
