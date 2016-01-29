// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MONCAP_H
#define CEPH_MONCAP_H

#include <ostream>
using std::ostream;

#include "include/types.h"
#include "common/entity_name.h"

class CephContext;

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

ostream& operator<<(ostream& out, mon_rwxa_t p);

struct StringConstraint {
  string value;
  string prefix;

  StringConstraint() {}
  StringConstraint(string a, string b) : value(a), prefix(b) {}
};

ostream& operator<<(ostream& out, const StringConstraint& c);

struct MonCapGrant {
  /*
   * A grant can come in one of four forms:
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
   */
  std::string service;
  std::string profile;
  std::string command;
  map<std::string,StringConstraint> command_args;

  mon_rwxa_t allow;

  // explicit grants that a profile grant expands to; populated as
  // needed by expand_profile() (via is_match()) and cached here.
  mutable list<MonCapGrant> profile_grants;

  void expand_profile(EntityName name) const;

  MonCapGrant() : allow(0) {}
  // cppcheck-suppress noExplicitConstructor
  MonCapGrant(mon_rwxa_t a) : allow(a) {}
  MonCapGrant(string s, mon_rwxa_t a) : service(s), allow(a) {}
  // cppcheck-suppress noExplicitConstructor  
  MonCapGrant(string c) : command(c) {}
  MonCapGrant(string c, string a, StringConstraint co) : command(c) {
    command_args[a] = co;
  }

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
			 const map<string,string>& command_args) const;

  bool is_allow_all() const {
    return
      allow == MON_CAP_ANY &&
      service.length() == 0 &&
      profile.length() == 0 &&
      command.length() == 0;
  }
};

ostream& operator<<(ostream& out, const MonCapGrant& g);

struct MonCap {
  string text;
  std::vector<MonCapGrant> grants;

  MonCap() {}
  explicit MonCap(std::vector<MonCapGrant> g) : grants(g) {}

  string get_str() const {
    return text;
  }

  bool is_allow_all() const;
  void set_allow_all();
  bool parse(const std::string& str, ostream *err=NULL);

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
		  const string& service,
		  const string& command, const map<string,string>& command_args,
		  bool op_may_read, bool op_may_write, bool op_may_exec) const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<MonCap*>& ls);
};
WRITE_CLASS_ENCODER(MonCap)

ostream& operator<<(ostream& out, const MonCap& cap);

#endif
