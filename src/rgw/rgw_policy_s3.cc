
#include <errno.h>

#include "rgw_policy_s3.h"
#include "rgw_json.h"
#include "rgw_common.h"


#define dout_subsys ceph_subsys_rgw


class RGWPolicyCondition {
protected:
  string v1;
  string v2;

  virtual bool check(const string& first, const string& second) = 0;

public:
  virtual ~RGWPolicyCondition() {}

  void set_vals(const string& _v1, const string& _v2) {
    v1 = _v1;
    v2 = _v2;
  }

  bool check(RGWPolicyEnv *env, map<string, bool, ltstr_nocase>& checked_vars) {
     string first, second;
     env->get_value(v1, first, checked_vars);
     env->get_value(v2, second, checked_vars);

     dout(1) << "policy condition check " << v1 << " [" << first << "] " << v2 << " [" << second << "]" << dendl;
     return check(first, second);
  }

};


class RGWPolicyCondition_StrEqual : public RGWPolicyCondition {
protected:
  bool check(const string& first, const string& second) {
    return first.compare(second) == 0;
  }
};

class RGWPolicyCondition_StrStartsWith : public RGWPolicyCondition {
protected:
  bool check(const string& first, const string& second) {
    return first.compare(0, second.size(), second) == 0;
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


bool RGWPolicyEnv::match_policy_vars(map<string, bool, ltstr_nocase>& policy_vars)
{
  map<string, string, ltstr_nocase>::iterator iter;
  string ignore_prefix = "x-ignore-";
  for (iter = vars.begin(); iter != vars.end(); ++iter) {
    const string& var = iter->first;
    if (strncasecmp(ignore_prefix.c_str(), var.c_str(), ignore_prefix.size()) == 0)
      continue;
    if (policy_vars.count(var) == 0) {
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

uint64_t RGWPolicy::get_current_epoch()
{
  uint64_t current_epoch = 0;
  time_t local_time = ceph_clock_now(NULL).sec();
  struct tm *gmt_time_struct = gmtime(&local_time);

  current_epoch = timegm(gmt_time_struct);


//  string gmt_time_string = asctime(gmt_time_struct);

//  parse_date(gmt_time_string, &current_epoch, NULL, NULL);

  return current_epoch;
}

void RGWPolicy::set_expires(utime_t& e)
{
  time_t e_time = e.sec();
  struct tm *gmt_time_struct = gmtime(&e_time);
  string gmt_time_string = asctime(gmt_time_struct);
  parse_date(gmt_time_string, &expires, NULL, NULL);
}

void RGWPolicy::set_expires(string& e)
{
  parse_date(e, &expires, NULL, NULL);
}

int RGWPolicy::add_condition(const string& op, const string& first, const string& second)
{
  RGWPolicyCondition *cond = NULL;
  if (stringcasecmp(op, "eq") == 0) {
    cond = new RGWPolicyCondition_StrEqual;
  } else if (stringcasecmp(op, "starts-with") == 0) {
    cond = new RGWPolicyCondition_StrStartsWith;
  } else if (stringcasecmp(op, "content-length-range") == 0) {
    stringstream min_string(first);
    stringstream max_string(second);

    if ( !(min_string >> min_length) || !(max_string >> max_length) )
      return -EINVAL;
  }

  if (!cond)
    return -EINVAL;

  cond->set_vals(first, second);
  
  conditions.push_back(cond);

  return 0;
}

bool RGWPolicy::check(RGWPolicyEnv *env)
{
  list<pair<string, string> >::iterator viter;
  for (viter = var_checks.begin(); viter != var_checks.end(); ++viter) {
    pair<string, string>& p = *viter;
    const string& name = p.first;
    const string& check_val = p.second;
    string val;
    if (!env->get_var(name, val))
      return false;

    set_var_checked(name);

    dout(20) << "comparing " << name << " [" << val << "], " << check_val << dendl;
    if (val.compare(check_val) != 0) {
      dout(1) << "policy check failed, val=" << val << " != " << check_val << dendl;
      return false;
    }
  }

  list<RGWPolicyCondition *>::iterator citer;
  for (citer = conditions.begin(); citer != conditions.end(); ++citer) {
    RGWPolicyCondition *cond = *citer;
    if (!cond->check(env, checked_vars)) {
      return false;
    }
  }

  if (!env->match_policy_vars(checked_vars)) {
    dout(1) << "missing policy condition" << dendl;
    return false;
  }
  return true;
}


int RGWPolicy::from_json(bufferlist& bl)
{
  RGWJSONParser parser;

  if (!parser.parse(bl.c_str(), bl.length()))
    return -EINVAL;

  // as no time was included in the request, we hope that the user has included a short timeout
  JSONObjIter iter = parser.find_first("expiration");
  if (iter.end())
    return -EINVAL; // change to a "no expiration" error following S3

  JSONObj *obj = *iter;
  string expiration_string = obj->get_data();
  set_expires(expiration_string);

  uint64_t current_epoch = get_current_epoch();
  if (current_epoch == 0) {
    dout(0) << "NOTICE: failed to get current epoch!" << dendl;
    return -EINVAL;
  }

  if (expires < current_epoch) {
    dout(0) << "NOTICE: policy calculated as  expired: " << expiration_string << dendl;
    return -EINVAL; // change to condition about expired policy following S3
  }

  iter = parser.find_first("conditions");
  if (iter.end())
    return -EINVAL; // change to a "no conditions" error following S3

  obj = *iter;

  iter = obj->find_first();
  for (; !iter.end(); ++iter) {
    JSONObj *child = *iter;
    dout(20) << "is_object=" << child->is_object() << dendl;
    dout(20) << "is_array=" << child->is_array() << dendl;
    if (child->is_array()) {
      JSONObjIter aiter = child->find_first();
      vector<string> v;
      int i;
      for (i = 0; !aiter.end() && i < 3; ++aiter, ++i) {
	JSONObj *o = *aiter;
        v.push_back(o->get_data());
      }
      if (i != 3 || !aiter.end())  /* we expect exactly 3 arguments here */
        return -EINVAL;

      add_condition(v[0], v[1], v[2]);
    } else {
      add_simple_check(child->get_name(), child->get_data());
    }
  }
  return 0;
}
