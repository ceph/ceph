#ifndef CEPH_RGW_POLICY_H
#define CEPH_RGW_POLICY_H

#include <map>
#include <list>
#include <string>

#include "include/utime.h"


class RGWPolicyEnv {
  std::map<std::string, std::string> vars;

public:
  void add_var(const string& name, const string& value);
  bool get_var(const string& name, string& val);
  bool get_value(const string& s, string& val);
};

class RGWPolicyCondition;


class RGWPolicy {
  utime_t expires;
  std::list<RGWPolicyCondition *> conditions;
  std::list<pair<std::string, std::string> > var_checks;

public:
  RGWPolicy() {}
  ~RGWPolicy();
  void set_expires(utime_t& e) { expires = e; }

  int add_condition(const std::string& op, const std::string& first, const std::string& second);
  void add_simple_check(const std::string& var, const std::string& value) {
    var_checks.push_back(pair<string, string>(var, value));
  }

  bool check(RGWPolicyEnv *env);
  int from_json(bufferlist& bl);
};
#endif
