#include "common/debug.h"
#include "common/ceph_json.h"

#include "acconfig.h"

#include <errno.h>
#include <string>
#include <list>
#include "include/types.h"
#include "rgw_website.h"

using namespace std;


bool RGWBWRoutingRuleCondition::check_key_condition(const string& key) {
  return (key.size() >= key_prefix_equals.size() &&
          key.compare(0, key_prefix_equals.size(), key_prefix_equals) == 0);
}


bool RGWBWRoutingRules::check_key_condition(const string& key, RGWBWRoutingRule **rule)
{
  for (list<RGWBWRoutingRule>::iterator iter = rules.begin(); iter != rules.end(); ++iter) {
    if (iter->check_key_condition(key)) {
      *rule = &(*iter);
      return true;
    }
  }
  return false;
}

bool RGWBWRoutingRules::check_error_code_condition(int error_code, RGWBWRoutingRule **rule)
{
  for (list<RGWBWRoutingRule>::iterator iter = rules.begin(); iter != rules.end(); ++iter) {
    if (iter->check_error_code_condition(error_code)) {
      *rule = &(*iter);
      return true;
    }
  }
  return false;
}

void RGWBucketWebsiteConf::get_effective_target(const string& key, string *effective_key, RGWRedirectInfo *redirect)
{
  RGWBWRoutingRule *rule;
  string new_key;

  if (routing_rules.check_key_condition(key, &rule)) {
    RGWBWRoutingRuleCondition& condition = rule->condition;
    RGWBWRedirectInfo& info = rule->redirect_info;

    if (!info.replace_key_prefix_with.empty()) {
      *effective_key = info.replace_key_prefix_with;
      *effective_key += key.substr(condition.key_prefix_equals.size());
    } else if (!info.replace_key_with.empty()) {
      *effective_key = info.replace_key_with;
    } else {
      *effective_key = key;
    }

    *redirect = info.redirect;
  }

  if (effective_key->empty()) {
    *effective_key = index_doc_suffix;
  } else if ((*effective_key)[effective_key->size() - 1] == '/') {
    *effective_key += index_doc_suffix;
  }
}
