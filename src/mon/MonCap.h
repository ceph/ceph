// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MONCAP_H
#define CEPH_MONCAP_H

#include <ostream>

#include "include/common_fwd.h"
#include "include/types.h"
#include "common/entity_name.h"
#include "mds/mdstypes.h"

static const __u8 MON_CAP_R     = (1 << 1);      // read
static const __u8 MON_CAP_W     = (1 << 2);      // write
static const __u8 MON_CAP_X     = (1 << 3);      // execute
static const __u8 MON_CAP_ALL   = MON_CAP_R | MON_CAP_W | MON_CAP_X;
static const __u8 MON_CAP_ANY   = 0xff;          // *

struct mon_rwxa_t {
  __u8 val;

  // cppcheck-suppress noExplicitConstructor
  mon_rwxa_t(__u8 v = 0) : val(v) {}
  mon_rwxa_t& operator=(__u8 v) {
    val = v;
    return *this;
  }
  operator __u8() const {
    return val;
  }
};

std::ostream& operator<<(std::ostream& out, const mon_rwxa_t& p);

struct StringConstraint {
  enum MatchType {
    MATCH_TYPE_NONE,
    MATCH_TYPE_EQUAL,
    MATCH_TYPE_PREFIX,
    MATCH_TYPE_REGEX
  };

  MatchType match_type = MATCH_TYPE_NONE;
  std::string value;

  StringConstraint() {}
  StringConstraint(MatchType match_type, std::string value)
    : match_type(match_type), value(value) {
  }
};

std::ostream& operator<<(std::ostream& out, const StringConstraint& c);

struct MonCapGrant {
  /*
   * A grant can come in one of five forms:
   *
   *  - a blanket allow ('allow rw', 'allow *')
   *    - this will match against any service and the read/write/exec flags
   *      in the mon code.  semantics of what X means are somewhat ad hoc.
   *
   *  - a service allow ('allow service mds rw')
   *    - this will match against a specific service and the r/w/x flags.
   *
   *  - a profile ('allow profile osd')
   *    - this will match against specific monitor-enforced semantics of what
   *      this type of user should need to do.  examples include 'osd', 'mds',
   *      'bootstrap-osd'.
   *
   *  - a command ('allow command foo', 'allow command bar with arg1=val1 arg2 prefix val2')
   *      this includes the command name (the prefix string), and a set
   *      of key/value pairs that constrain use of that command.  if no pairs
   *      are specified, any arguments are allowed; if a pair is specified, that
   *      argument must be present and equal or match a prefix.
   *
   *  - an fs name ('allow fsname foo')
   *    - this will restrict access to MDSMaps in the FSMap to the provided
   *      fs name.
   */
  std::string service;
  std::string profile;
  std::string command;
  std::map<std::string, StringConstraint> command_args;
  std::string fs_name;

  // restrict by network
  std::string network;

  // these are filled in by parse_network(), called by MonCap::parse()
  entity_addr_t network_parsed;
  unsigned network_prefix = 0;
  bool network_valid = true;

  void parse_network();

  mon_rwxa_t allow;

  // explicit grants that a profile grant expands to; populated as
  // needed by expand_profile() (via is_match()) and cached here.
  mutable std::list<MonCapGrant> profile_grants;

  void expand_profile(const EntityName& name) const;

  MonCapGrant() : allow(0) {}
  // cppcheck-suppress noExplicitConstructor
  MonCapGrant(mon_rwxa_t a) : allow(a) {}
  MonCapGrant(std::string s, mon_rwxa_t a) : service(std::move(s)), allow(a) {}
  // cppcheck-suppress noExplicitConstructor
  MonCapGrant(std::string c) : command(std::move(c)) {}
  MonCapGrant(std::string c, std::string a, StringConstraint co) : command(std::move(c)) {
    command_args[a] = co;
  }
  MonCapGrant(mon_rwxa_t a, std::string fsname) : fs_name(fsname), allow(a) {}

  /**
   * check if given request parameters match our constraints
   *
   * @param cct context
   * @param name entity name
   * @param service service (if any)
   * @param command command (if any)
   * @param command_args command args (if any)
   * @return bits we allow
   */
  mon_rwxa_t get_allowed(CephContext *cct,
			 EntityName name,
			 const std::string& service,
			 const std::string& command,
			 const std::map<std::string, std::string>& command_args) const;

  bool is_allow_all() const {
    return
      allow == MON_CAP_ANY &&
      service.length() == 0 &&
      profile.length() == 0 &&
      command.length() == 0 &&
      fs_name.empty();
  }

  std::string to_string();
};

std::ostream& operator<<(std::ostream& out, const MonCapGrant& g);

struct MonCap {
  std::string text;
  std::vector<MonCapGrant> grants;

  MonCap() {}
  explicit MonCap(const std::vector<MonCapGrant> &g) : grants(g) {}

  std::string get_str() const {
    return text;
  }
  std::string to_string();

  bool is_allow_all() const;
  void set_allow_all();
  bool parse(const std::string& str, std::ostream *err=NULL);
  bool merge(MonCap newcap);

  /**
   * check if we are capable of something
   *
   * This method actually checks a description of a particular operation against
   * what the capability has specified.
   *
   * @param service service name
   * @param command command id
   * @param command_args
   * @param op_may_read whether the operation may need to read
   * @param op_may_write whether the operation may need to write
   * @param op_may_exec whether the operation may exec
   * @return true if the operation is allowed, false otherwise
   */
  bool is_capable(CephContext *cct,
		  EntityName name,
		  const std::string& service,
		  const std::string& command,
		  const std::map<std::string, std::string>& command_args,
		  bool op_may_read, bool op_may_write, bool op_may_exec,
		  const entity_addr_t& addr) const;

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<MonCap*>& ls);

  std::vector<std::string> allowed_fs_names() const {
    std::vector<std::string> ret;
    for (auto& g : grants) {
      if (not g.fs_name.empty()) {
	ret.push_back(g.fs_name);
      } else {
	return {};
      }
    }
    return ret;
  }

  bool fs_name_capable(const EntityName& ename, std::string_view fs_name,
		       __u8 mask) {
    for (auto& g : grants) {
      if (g.is_allow_all()) {
	return true;
      }

      if ((g.fs_name.empty() || g.fs_name == fs_name) && (mask & g.allow)) {
	  return true;
      }

      g.expand_profile(ename);
      for (auto& pg : g.profile_grants) {
	if ((pg.service == "fs" || pg.service == "mds") &&
	    (pg.fs_name.empty() || pg.fs_name == fs_name) &&
	    (pg.allow & mask)) {
	  return true;
	}
      }
    }

    return false;
  }

};
WRITE_CLASS_ENCODER(MonCap)

std::ostream& operator<<(std::ostream& out, const MonCap& cap);

#endif
