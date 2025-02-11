// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGRCAP_H
#define CEPH_MGRCAP_H

#include <iosfwd>
#include <map>
#include <string>

#include "include/common_fwd.h"
#include "include/types.h"
#include "common/entity_name.h"
#include "msg/msg_types.h" // for struct entity_addr_t

static const __u8 MGR_CAP_R     = (1 << 1);      // read
static const __u8 MGR_CAP_W     = (1 << 2);      // write
static const __u8 MGR_CAP_X     = (1 << 3);      // execute
static const __u8 MGR_CAP_ANY   = 0xff;          // *

struct mgr_rwxa_t {
  __u8 val = 0U;

  mgr_rwxa_t() {}
  explicit mgr_rwxa_t(__u8 v) : val(v) {}

  mgr_rwxa_t& operator=(__u8 v) {
    val = v;
    return *this;
  }
  operator __u8() const {
    return val;
  }
};

std::ostream& operator<<(std::ostream& out, const mgr_rwxa_t& p);

struct MgrCapGrantConstraint {
  enum MatchType {
    MATCH_TYPE_NONE,
    MATCH_TYPE_EQUAL,
    MATCH_TYPE_PREFIX,
    MATCH_TYPE_REGEX
  };

  MatchType match_type = MATCH_TYPE_NONE;
  std::string value;

  MgrCapGrantConstraint() {}
  MgrCapGrantConstraint(MatchType match_type, std::string value)
    : match_type(match_type), value(value) {
  }
};

std::ostream& operator<<(std::ostream& out, const MgrCapGrantConstraint& c);

struct MgrCapGrant {
  /*
   * A grant can come in one of four forms:
   *
   *  - a blanket allow ('allow rw', 'allow *')
   *    - this will match against any service and the read/write/exec flags
   *      in the mgr code.  semantics of what X means are somewhat ad hoc.
   *
   *  - a service allow ('allow service mds rw')
   *    - this will match against a specific service and the r/w/x flags.
   *
   *  - a module allow ('allow module rbd_support rw, allow module rbd_support with pool=rbd rw')
   *    - this will match against a specific python add-on module and the r/w/x
   *      flags.
   *
   *  - a profile ('profile read-only, profile rbd pool=rbd')
   *    - this will match against specific MGR-enforced semantics of what
   *      this type of user should need to do.  examples include 'read-write',
   *      'read-only', 'crash'.
   *
   *  - a command ('allow command foo', 'allow command bar with arg1=val1 arg2 prefix val2')
   *      this includes the command name (the prefix string)
   *
   *  The command, module, and profile caps can also accept an optional
   *  key/value map. If not provided, all command arguments and module
   *  meta-arguments are allowed. If a key/value pair is specified, that
   *  argument must be present and must match the provided constraint.
   */
  typedef std::map<std::string, MgrCapGrantConstraint> Arguments;

  std::string service;
  std::string module;
  std::string profile;
  std::string command;
  Arguments arguments;

  // restrict by network
  std::string network;

  // these are filled in by parse_network(), called by MgrCap::parse()
  entity_addr_t network_parsed;
  unsigned network_prefix = 0;
  bool network_valid = true;

  void parse_network();

  mgr_rwxa_t allow;

  // explicit grants that a profile grant expands to; populated as
  // needed by expand_profile() (via is_match()) and cached here.
  mutable std::list<MgrCapGrant> profile_grants;

  void expand_profile(std::ostream *err=nullptr) const;

  MgrCapGrant() : allow(0) {}
  MgrCapGrant(std::string&& service,
              std::string&& module,
              std::string&& profile,
              std::string&& command,
              Arguments&& arguments,
              mgr_rwxa_t allow)
    : service(std::move(service)), module(std::move(module)),
      profile(std::move(profile)), command(std::move(command)),
      arguments(std::move(arguments)), allow(allow) {
  }

  bool validate_arguments(
      const std::map<std::string, std::string>& arguments) const;

  /**
   * check if given request parameters match our constraints
   *
   * @param cct context
   * @param name entity name
   * @param service service (if any)
   * @param module module (if any)
   * @param command command (if any)
   * @param arguments profile/module/command args (if any)
   * @return bits we allow
   */
  mgr_rwxa_t get_allowed(
      CephContext *cct,
      EntityName name,
      const std::string& service,
      const std::string& module,
      const std::string& command,
      const std::map<std::string, std::string>& arguments) const;

  bool is_allow_all() const {
    return (allow == MGR_CAP_ANY &&
            service.empty() &&
            module.empty() &&
            profile.empty() &&
            command.empty());
  }
};

std::ostream& operator<<(std::ostream& out, const MgrCapGrant& g);

struct MgrCap {
  std::string text;
  std::vector<MgrCapGrant> grants;

  MgrCap() {}
  explicit MgrCap(const std::vector<MgrCapGrant> &g) : grants(g) {}

  std::string get_str() const {
    return text;
  }

  bool is_allow_all() const;
  void set_allow_all();
  bool parse(const std::string& str, std::ostream *err=NULL);

  /**
   * check if we are capable of something
   *
   * This method actually checks a description of a particular operation against
   * what the capability has specified.
   *
   * @param service service name
   * @param module module name
   * @param command command id
   * @param arguments
   * @param op_may_read whether the operation may need to read
   * @param op_may_write whether the operation may need to write
   * @param op_may_exec whether the operation may exec
   * @return true if the operation is allowed, false otherwise
   */
  bool is_capable(CephContext *cct,
		  EntityName name,
		  const std::string& service,
		  const std::string& module,
		  const std::string& command,
		  const std::map<std::string, std::string>& arguments,
		  bool op_may_read, bool op_may_write, bool op_may_exec,
		  const entity_addr_t& addr) const;

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<MgrCap*>& ls);
};
WRITE_CLASS_ENCODER(MgrCap)

std::ostream& operator<<(std::ostream& out, const MgrCap& cap);

#endif // CEPH_MGRCAP_H
