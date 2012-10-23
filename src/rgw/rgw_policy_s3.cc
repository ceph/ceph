
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

  bool check(RGWPolicyEnv *env) {
     string first, second;
     env->get_value(v1, first);
     env->get_value(v2, second);
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
  map<string, string>::iterator iter = vars.find(name);
  if (iter == vars.end())
    return false;

  val = iter->second;

  return true;
}

bool RGWPolicyEnv::get_value(const string& s, string& val)
{
  if (s.empty() || s[0] != '$') {
    val = s;
    return true;
  }

  return get_var(s.substr(1), val);
}


RGWPolicy::~RGWPolicy()
{
  list<RGWPolicyCondition *>::iterator citer;
  for (citer = conditions.begin(); citer != conditions.end(); ++citer) {
    RGWPolicyCondition *cond = *citer;
    delete cond;
  }
}

int RGWPolicy::add_condition(const string& op, const string& first, const string& second)
{
  RGWPolicyCondition *cond = NULL;
  if (op.compare("eq") == 0)
    cond = new RGWPolicyCondition_StrEqual;
  else if (op.compare("starts-with") == 0)
    cond = new RGWPolicyCondition_StrStartsWith;

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

    dout(20) << "comparing " << name << " [" << val << "], " << check_val << dendl;
    if (val.compare(check_val) != 0) {
      dout(1) << "policy check failed, val=" << val << " != " << check_val << dendl;
      return false;
    }
  }

  list<RGWPolicyCondition *>::iterator citer;
  for (citer = conditions.begin(); citer != conditions.end(); ++citer) {
    RGWPolicyCondition *cond = *citer;
    if (!cond->check(env)) {
      return false;
    }
  }
  return true;
}


int RGWPolicy::from_json(bufferlist& bl)
{
  RGWJSONParser parser;

  if (!parser.parse(bl.c_str(), bl.length()))
    return -EINVAL;

  JSONObjIter iter = parser.find_first("conditions");
  if (iter.end())
    return 0;

  JSONObj *obj = *iter;

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
