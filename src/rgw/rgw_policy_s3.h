// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <limits.h>

#include <map>
#include <list>
#include <string>

#include "include/utime.h"

#include "rgw_string.h"


class RGWPolicyEnv {
  std::map<std::string, std::string, ltstr_nocase> vars;

public:
  void add_var(const std::string& name, const std::string& value);
  bool get_var(const std::string& name, std::string& val);
  bool get_value(const std::string& s, std::string& val, std::map<std::string, bool, ltstr_nocase>& checked_vars);
  bool match_policy_vars(std::map<std::string, bool, ltstr_nocase>& policy_vars, std::string& err_msg);
};

class RGWPolicyCondition;


class RGWPolicy {
  uint64_t expires;
  std::string expiration_str;
  std::list<RGWPolicyCondition *> conditions;
  std::list<std::pair<std::string, std::string> > var_checks;
  std::map<std::string, bool, ltstr_nocase> checked_vars;

public:
  off_t min_length;
  off_t max_length;

  RGWPolicy() : expires(0), min_length(0), max_length(LLONG_MAX) {}
  ~RGWPolicy();

  int set_expires(const std::string& e);

  void set_var_checked(const std::string& var) {
    checked_vars[var] = true;
  }

  int add_condition(const std::string& op, const std::string& first, const std::string& second, std::string& err_msg);
  void add_simple_check(const std::string& var, const std::string& value) {
    var_checks.emplace_back(var, value);
  }

  int check(RGWPolicyEnv *env, std::string& err_msg);
  int from_json(bufferlist& bl, std::string& err_msg);
};
