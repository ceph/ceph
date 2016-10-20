// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/ceph_json.h"
#include "rgw_policy_s3.h"
#include "rgw_common.h"


#define dout_subsys ceph_subsys_rgw

class RGWPolicyCondition {
protected:
  string v1;
  string v2;

  virtual bool check(const string& first, const string& second, string& err_msg) = 0;

public:
  virtual ~RGWPolicyCondition() {}

  void set_vals(const string& _v1, const string& _v2) {
    v1 = _v1;
    v2 = _v2;
  }

  bool check(RGWPolicyEnv *env, map<string, bool, ltstr_nocase>& checked_vars, string& err_msg) {
     string first, second;
     env->get_value(v1, first, checked_vars);
     env->get_value(v2, second, checked_vars);

     dout(1) << "policy condition check " << v1 << " [" << first << "] " << v2 << " [" << second << "]" << dendl;
     bool ret = check(first, second, err_msg);
     if (!ret) {
       err_msg.append(": ");
       err_msg.append(v1);
       err_msg.append(", ");
       err_msg.append(v2);
     }
     return ret;
  }

};


class RGWPolicyCondition_StrEqual : public RGWPolicyCondition {
protected:
  bool check(const string& first, const string& second, string& msg) {
    bool ret = first.compare(second) == 0;
    if (!ret) {
      msg = "Policy condition failed: eq";
    }
    return ret;
  }
};

class RGWPolicyCondition_StrStartsWith : public RGWPolicyCondition {
protected:
  bool check(const string& first, const string& second, string& msg) {
    bool ret = first.compare(0, second.size(), second) == 0;
    if (!ret) {
      msg = "Policy condition failed: starts-with";
    }
    return ret;
  }
};

void RGWPolicyEnv::add_var(const string& name, const string& value)
{
  vars[name] = value;
}

bool RGWPolicyEnv::get_var(const string& name, string& val)
{
  map<string, string, ltstr_nocase>::iterator iter = vars.find(name);
  if (iter == vars.end())
    return false;

  val = iter->second;

  return true;
}

bool RGWPolicyEnv::get_value(const string& s, string& val, map<string, bool, ltstr_nocase>& checked_vars)
{
  if (s.empty() || s[0] != '$') {
    val = s;
    return true;
  }

  const string& var = s.substr(1);
  checked_vars[var] = true;

  return get_var(var, val);
}


bool RGWPolicyEnv::match_policy_vars(map<string, bool, ltstr_nocase>& policy_vars, string& err_msg)
{
  map<string, string, ltstr_nocase>::iterator iter;
  string ignore_prefix = "x-ignore-";
  for (iter = vars.begin(); iter != vars.end(); ++iter) {
    const string& var = iter->first;
    if (strncasecmp(ignore_prefix.c_str(), var.c_str(), ignore_prefix.size()) == 0)
      continue;
    if (policy_vars.count(var) == 0) {
      err_msg = "Policy missing condition: ";
      err_msg.append(iter->first);
      dout(1) << "env var missing in policy: " << iter->first << dendl;
      return false;
    }
  }
  return true;
}

RGWPolicy::~RGWPolicy()
{
  list<RGWPolicyCondition *>::iterator citer;
  for (citer = conditions.begin(); citer != conditions.end(); ++citer) {
    RGWPolicyCondition *cond = *citer;
    delete cond;
  }
}

int RGWPolicy::set_expires(const string& e)
{
  struct tm t;
  if (!parse_iso8601(e.c_str(), &t))
      return -EINVAL;

  expires = timegm(&t);

  return 0;
}

int RGWPolicy::add_condition(const string& op, const string& first, const string& second, string& err_msg)
{
  RGWPolicyCondition *cond = NULL;
  if (stringcasecmp(op, "eq") == 0) {
    cond = new RGWPolicyCondition_StrEqual;
  } else if (stringcasecmp(op, "starts-with") == 0) {
    cond = new RGWPolicyCondition_StrStartsWith;
  } else if (stringcasecmp(op, "content-length-range") == 0) {
    off_t min, max;
    int r = stringtoll(first, &min);
    if (r < 0) {
      err_msg = "Bad content-length-range param";
      dout(0) << "bad content-length-range param: " << first << dendl;
      return r;
    }

    r = stringtoll(second, &max);
    if (r < 0) {
      err_msg = "Bad content-length-range param";
      dout(0) << "bad content-length-range param: " << second << dendl;
      return r;
    }

    if (min > min_length)
      min_length = min;

    if (max < max_length)
      max_length = max;

    return 0;
  }

  if (!cond) {
    err_msg = "Invalid condition: ";
    err_msg.append(op);
    dout(0) << "invalid condition: " << op << dendl;
    return -EINVAL;
  }

  cond->set_vals(first, second);
  
  conditions.push_back(cond);

  return 0;
}

int RGWPolicy::check(RGWPolicyEnv *env, string& err_msg)
{
  uint64_t now = ceph_clock_now(NULL).sec();
  if (expires <= now) {
    dout(0) << "NOTICE: policy calculated as expired: " << expiration_str << dendl;
    err_msg = "Policy expired";
    return -EACCES; // change to condition about expired policy following S3
  }

  list<pair<string, string> >::iterator viter;
  for (viter = var_checks.begin(); viter != var_checks.end(); ++viter) {
    pair<string, string>& p = *viter;
    const string& name = p.first;
    const string& check_val = p.second;
    string val;
    if (!env->get_var(name, val)) {
      dout(20) << " policy check failed, variable not found: '" << name << "'" << dendl;
      err_msg = "Policy check failed, variable not found: ";
      err_msg.append(name);
      return -EACCES;
    }

    set_var_checked(name);

    dout(20) << "comparing " << name << " [" << val << "], " << check_val << dendl;
    if (val.compare(check_val) != 0) {
      err_msg = "Policy check failed, variable not met condition: ";
      err_msg.append(name);
      dout(1) << "policy check failed, val=" << val << " != " << check_val << dendl;
      return -EACCES;
    }
  }

  list<RGWPolicyCondition *>::iterator citer;
  for (citer = conditions.begin(); citer != conditions.end(); ++citer) {
    RGWPolicyCondition *cond = *citer;
    if (!cond->check(env, checked_vars, err_msg)) {
      return -EACCES;
    }
  }

  if (!env->match_policy_vars(checked_vars, err_msg)) {
    dout(1) << "missing policy condition" << dendl;
    return -EACCES;
  }
  return 0;
}


int RGWPolicy::from_json(bufferlist& bl, string& err_msg)
{
  JSONParser parser;

  if (!parser.parse(bl.c_str(), bl.length())) {
    err_msg = "Malformed JSON";
    dout(0) << "malformed json" << dendl;
    return -EINVAL;
  }

  // as no time was included in the request, we hope that the user has included a short timeout
  JSONObjIter iter = parser.find_first("expiration");
  if (iter.end()) {
    err_msg = "Policy missing expiration";
    dout(0) << "expiration not found" << dendl;
    return -EINVAL; // change to a "no expiration" error following S3
  }

  JSONObj *obj = *iter;
  expiration_str = obj->get_data();
  int r = set_expires(expiration_str);
  if (r < 0) {
    err_msg = "Failed to parse policy expiration";
    return r;
  }

  iter = parser.find_first("conditions");
  if (iter.end()) {
    err_msg = "Policy missing conditions";
    dout(0) << "conditions not found" << dendl;
    return -EINVAL; // change to a "no conditions" error following S3
  }

  obj = *iter;

  iter = obj->find_first();
  for (; !iter.end(); ++iter) {
    JSONObj *child = *iter;
    dout(20) << "data=" << child->get_data() << dendl;
    dout(20) << "is_object=" << child->is_object() << dendl;
    dout(20) << "is_array=" << child->is_array() << dendl;
    JSONObjIter citer = child->find_first();
    if (child->is_array()) {
      vector<string> v;
      int i;
      for (i = 0; !citer.end() && i < 3; ++citer, ++i) {
	JSONObj *o = *citer;
        v.push_back(o->get_data());
      }
      if (i != 3 || !citer.end()) { /* we expect exactly 3 arguments here */
        err_msg = "Bad condition array, expecting 3 arguments";
        return -EINVAL;
      }

      int r = add_condition(v[0], v[1], v[2], err_msg);
      if (r < 0)
        return r;
    } else if (!citer.end()) {
      JSONObj *c = *citer;
      dout(0) << "adding simple_check: " << c->get_name() << " : " << c->get_data() << dendl;

      add_simple_check(c->get_name(), c->get_data());
    } else {
      return -EINVAL;
    }
  }
  return 0;
}
