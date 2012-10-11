#ifndef CEPH_RGW_POLICY_H
#define CEPH_RGW_POLICY_H

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
  bool match_policy_vars(map<string, bool, ltstr_nocase>& policy_vars);
};

class RGWPolicyCondition;


class RGWPolicy {
  uint64_t expires;
  std::list<RGWPolicyCondition *> conditions;
  std::list<pair<std::string, std::string> > var_checks;
  std::map<std::string, bool, ltstr_nocase> checked_vars;

public:
  int min_length;
  int max_length;

  RGWPolicy() : expires(0), min_length(-1), max_length(-1) {}
  ~RGWPolicy();

  uint64_t get_current_epoch();
  void set_expires(utime_t& e);
  void set_expires(string& e);

  void set_var_checked(const std::string& var) {
    checked_vars[var] = true;
  }

  int add_condition(const std::string& op, const std::string& first, const std::string& second);
  void add_simple_check(const std::string& var, const std::string& value) {
    var_checks.push_back(pair<string, string>(var, value));
  }

  bool check(RGWPolicyEnv *env);
  int from_json(bufferlist& bl);
};
#endif
