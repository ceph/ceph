// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MONCAP_H
#define CEPH_MONCAP_H

#include <ostream>
using std::ostream;

#include "include/types.h"

static const __u8 MON_CAP_R     = (1 << 1);      // read
static const __u8 MON_CAP_W     = (1 << 2);      // write
static const __u8 MON_CAP_X     = (1 << 3);      // execute
static const __u8 MON_CAP_ANY   = 0xff;          // *

typedef __u8 mon_rwxa_t;

ostream& operator<<(ostream& out, mon_rwxa_t p);

struct MonCapSpec {
  mon_rwxa_t allow;

  MonCapSpec() : allow(0) {}
  MonCapSpec(mon_rwxa_t v) : allow(v) {}

  bool allow_all() const {
    return allow == MON_CAP_ANY;
  }
};

ostream& operator<<(ostream& out, const MonCapSpec& s);


struct MonCapMatch {
  std::string service;
  std::string command;
  map<std::string,std::string> command_args;

  MonCapMatch() {}
  MonCapMatch(std::string s) : service(s) {}
  MonCapMatch(std::string c, map<std::string,std::string> m) : command(c), command_args(m) {}

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
};

ostream& operator<<(ostream& out, const MonCapMatch& m);


struct MonCapGrant {
  MonCapMatch match;
  MonCapSpec spec;

  MonCapGrant() {}
  MonCapGrant(MonCapMatch m, MonCapSpec s) : match(m), spec(s) {}
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

  bool allow_all() const;
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
  bool is_capable(const string& service,
		  const string& command, const map<string,string>& command_args,
		  bool op_may_read, bool op_may_write, bool op_may_exec) const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<MonCap*>& ls);
};
WRITE_CLASS_ENCODER(MonCap)

static inline ostream& operator<<(ostream& out, const MonCap& cap)
{
  return out << "moncap" << cap.grants;
}

#endif
