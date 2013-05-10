// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MONCAP_H
#define CEPH_MONCAP_H

#include <ostream>
using std::ostream;

#include "include/types.h"
#include "msg/msg_types.h"

class CephContext;

static const __u8 MON_CAP_R     = (1 << 1);      // read
static const __u8 MON_CAP_W     = (1 << 2);      // write
static const __u8 MON_CAP_X     = (1 << 3);      // execute
static const __u8 MON_CAP_ANY   = 0xff;          // *

struct mon_rwxa_t {
  __u8 val;

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

struct MonCapGrant {
  std::string service;
  std::string profile;
  std::string command;
  map<std::string,std::string> command_args;

  mon_rwxa_t allow;

  MonCapGrant() : allow(0) {}
  MonCapGrant(mon_rwxa_t a) : allow(a) {}
  MonCapGrant(std::string s, mon_rwxa_t a) : service(s), allow(a) {}
  MonCapGrant(std::string p) : profile(p), allow(0) {}
  MonCapGrant(std::string c, map<std::string,std::string> m) : command(c), command_args(m), allow(0) {}

  /**
   * check if given request parameters match our constraints
   *
   * @param service
   * @param command
   * @param command_args
   * @return true if we match, false otherwise
   */
  bool is_match(const std::string& service,
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
  MonCap(std::vector<MonCapGrant> g) : grants(g) {}

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
		  entity_name_t name,
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
