// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <cerrno>
#include <errno.h>
#include <ctime>
#include <regex>
#include <boost/algorithm/string/replace.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "include/types.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_tools.h"
#include "rgw_customer_managed_policy.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;
namespace rgw::sal{
  const string RGWCustomerManagedPolicy::policy_arn_prefix = "arn::aws::iam::";

  RGWCustomerManagedPolicy::RGWCustomerManagedPolicy(std::string name,
                                                     std::string tenant,
                                                     rgw_account_id account_id,
                                                     std::string path,
                                                     std::string policy_document,
                                                     std::string description,
                                                     std::string default_version,
                                                     std::multimap<std::string, std::string> tags)
  {
    info.policy_name = std::move(name);
    info.tenant = std::move(tenant);
    info.account_id = std::move(account_id);
    info.path = std::move(path);
    info.policy_document = std::move(policy_document);
    info.description = std::move(description);
    info.default_version = std::move(default_version);
    info.tags = std::move(tags);
    info.mtime = real_time();
    if(this->info.path.empty())
      this->info.path = "/";
  }

  bool RGWCustomerManagedPolicy::validate_input(const DoutPrefixProvider *dpp) {
    if (info.policy_name.length() > MAX_POLICY_NAME_LEN) {
      ldpp_dout(dpp, 0) << "ERROR: Invalid name length " << dendl;
      return false;
    }

    if (info.path.length() > MAX_PATH_NAME_LEN) {
      ldpp_dout(dpp, 0) << "ERROR: Invalid path length " << dendl;
      return false;
    }

    std::regex regex_name("[A-Za-z0-9:=,.@-]+");
    if (! std::regex_match(info.policy_name, regex_name)) {
      ldpp_dout(dpp, 0) << "ERROR: Invalid chars in name " << dendl;
      return false;
    }

    std::regex regex_path("(/[!-~]+/)|(/)");
    if (! std::regex_match(info.path,regex_path)) {
      ldpp_dout(dpp, 0) << "ERROR: Invalid chars in path " << dendl;
      return false;
    }
    return true;
  }

  RGWCustomerManagedPolicy::RGWCustomerManagedPolicy(std::string id) { info.id = std::move(id); }

  int RGWCustomerManagedPolicy::set_tags(const DoutPrefixProvider *dpp, const std::multimap<std::string, std::string> &tags_map)
  {
    for (auto& it : tags_map) {
      this->info.tags.emplace(it.first, it.second);
    }
    if (this->info.tags.size() > 50) {
      ldpp_dout(dpp, 0) << "No. of tags is greater than 50" << dendl;
      return -EINVAL;
    }
    return 0;
  }
  boost::optional<std::multimap<std::string, std::string>> RGWCustomerManagedPolicy::get_tags()
  {
    if (this->info.tags.empty())
    {
      return boost::none;
    }
    return this->info.tags;
  }

  void RGWCustomerManagedPolicy::erase_tags(const std::vector<std::string> &tagKeys)
  {
    for (auto &it : tagKeys)
    {
      this->info.tags.erase(it);
    }
  }

  void RGWCustomerManagedPolicy::update_policy_document(std::string &policy_document) { this->info.policy_document = policy_document; }
  //TODO Define
  int RGWCustomerManagedPolicy::delete_policy(const DoutPrefixProvider *dpp, const rgw::ARN &arn) {}
  ManagedPolicyAttachment RGWCustomerManagedPolicy::get_attachment(const rgw::ARN &arn) const {}
  void RGWCustomerManagedPolicy::set_attachment(const rgw::ARN &arn, const ManagedPolicyAttachment &attachment) {}
  PolicyDocument RGWCustomerManagedPolicy::getVersion(const VersionId &versionId) const {}
  void RGWCustomerManagedPolicy::setVersion(const VersionId &versionId, const PolicyDocument &policy_document) {}

  int RGWCustomerManagedPolicy::create(const DoutPrefixProvider *dpp, std::string &policy_id, optional_yield y,
                                       const RGWAccountInfo &acc_info, std::map<std::string, bufferlist> &acc_attrs,
                                       RGWObjVersionTracker &objv)
  {
    if( !validate_input(dpp)) {
      return -EINVAL;
    }

    if(!policy_id.empty()){
      info.id = policy_id;
    }

    if (info.id.empty()) {
    /* create unique id */
      uuid_d new_uuid;
      char uuid_str[37];
      new_uuid.generate_random();
      new_uuid.print(uuid_str);
      info.id = uuid_str;
      policy_id = info.id;
    }

    //arn
    std::string_view account = !info.account_id.empty() ? info.account_id : info.tenant;
    info.arn = string_cat_reserve(policy_arn_prefix, account, ":policy", info.path, info.policy_name);

    if (info.creation_date.empty()) {
      // Creation time
      real_clock::time_point t = real_clock::now();

      struct timeval tv;
      real_clock::to_timeval(t, tv);

      char buf[30];
      struct tm result;
      gmtime_r(&tv.tv_sec, &result);
      strftime(buf,30,"%Y-%m-%dT%H:%M:%S", &result);
      sprintf(buf + strlen(buf),".%03dZ",(int)tv.tv_usec/1000);
      info.creation_date.assign(buf, strlen(buf));
    }

    constexpr bool exclusive = true;
    return store_info(dpp, exclusive, y, acc_info, acc_attrs, objv);
  }
}



