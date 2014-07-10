// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_POLICY_H
#define CEPH_RGW_POLICY_H

#include <limits.h>

#include <map>
#include <list>
#include <string>

#include "include/utime.h"

#include "rgw_string.h"


class RGWPolicyEnv {
  std::map<std::string, std::string, ltstr_nocase> vars;

public:
  void add_var(const string& name, const string& value);
  bool get_var(const string& name, string& val);
  bool get_value(const string& s, string& val, std::map<std::string, bool, ltstr_nocase>& checked_vars);
  bool match_policy_vars(map<string, bool, ltstr_nocase>& policy_vars, string& err_msg);
};

class RGWPolicyCondition;


class RGWPolicy {
  uint64_t expires;
  string expiration_str;
  std::list<RGWPolicyCondition *> conditions;
  std::list<pair<std::string, std::string> > var_checks;
  std::map<std::string, bool, ltstr_nocase> checked_vars;

public:
  off_t min_length;
  off_t max_length;

  RGWPolicy() : expires(0), min_length(0), max_length(LLONG_MAX) {}
  ~RGWPolicy();

  int set_expires(const string& e);

  void set_var_checked(const std::string& var) {
    checked_vars[var] = true;
  }

  int add_condition(const std::string& op, const std::string& first, const std::string& second, string& err_msg);
  void add_simple_check(const std::string& var, const std::string& value) {
    var_checks.push_back(pair<string, string>(var, value));
  }

  int check(RGWPolicyEnv *env, string& err_msg);
  int from_json(bufferlist& bl, string& err_msg);
};
#endif
