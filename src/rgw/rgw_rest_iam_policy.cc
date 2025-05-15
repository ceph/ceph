// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_rest_iam_policy.h"
#include "rgw_iam_managed_policy.h"
#include "rgw_rest_iam.h"
#include <regex>
#include <string>
#include "rgw_process_env.h"
#include <errno.h>


#define dout_subsys ceph_subsys_rgw

int RGWRestPolicy::verify_permission(optional_yield y)
{
  if (verify_user_permission(this, s, arn, action)) {
    return 0;
  }
  return -EACCES;
}

void RGWRestPolicy::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this);
}

static int parse_tags(const DoutPrefixProvider* dpp,
                      const std::map<std::string, std::string>& params,
                      std::multimap<std::string, std::string>& tags,
                      std::string& message)
{
  std::map<int, std::string> key_map;
  std::map<int, std::string> val_map;

  const std::regex pattern_key("Tags\\.member\\.([0-9]+)\\.Key");
  const std::regex pattern_value("Tags\\.member\\.([0-9]+)\\.Value");

  for (const auto& param : params) {
    std::smatch match;

    if (std::regex_match(param.first, match, pattern_key)) {
      int index = ceph::parse<int>(match[1].str()).value();
      key_map[index] = param.second;
    }

    if (std::regex_match(param.first, match, pattern_value)) {
      int index = ceph::parse<int>(match[1].str()).value();
      val_map[index] = param.second;
    }
  }

  if (key_map.size() != val_map.size()) {
    message = "Tags array found mismatched Keys/Values";
    return -EINVAL;
  }

  for (const auto& [index, key] : key_map) {
    auto it = val_map.find(index);
    if (it == val_map.end()) {
      message = "Tags array found mismatched Keys/Values";
      return -EINVAL;
    }
    tags.emplace(key, it->second);
  }

  return 0;
}

static rgw::ARN make_policy_arn(const std::string& path,
                              const std::string& name,
                              const std::string& account)
{
  return {string_cat_reserve(path, name), "policy", account, true};
}

static int check_policy_limit(const DoutPrefixProvider* dpp, optional_yield y,
                     rgw::sal::Driver* driver, const RGWAccountInfo& account,
                     std::string& err)
{

  uint32_t count = 0;
  int r = driver->count_account_policies(dpp, y, account.id, count);
  if (r < 0) {
    ldpp_dout(dpp, 4) << "failed to count policies for iam account " << account.id << ": " << r << dendl;
    return r;
  }
  if (std::cmp_greater_equal(count, account.max_policies)) {
    err = fmt::format("policy limit {} exceeded", account.max_policies);
    return -ERR_LIMIT_EXCEEDED;
  }
  return 0;
}

int RGWCreatePolicy::init_processing(optional_yield y)
{
  // unique id
  uuid_d new_uuid;
  char policy_id[37];
  new_uuid.generate_random();
  new_uuid.print(policy_id);
  info.id = policy_id;

  info.name = s->info.args.get("PolicyName");
  if(!validate_iam_policy_name(info.name, s->err.message)) {
    return -EINVAL;
  }

  info.path = s->info.args.get("Path");
  if(info.path.empty()) {
    info.path = "/";
  } else if (!validate_iam_path(info.path, s->err.message)) {
    return -EINVAL;
  }

  info.description = s->info.args.get("Description");
  if (info.description.size() > 1000) {
    s->err.message = "Description exceeds maximum length of 1000 characters.";
    return -EINVAL;
  }

  info.policy_document = s->info.args.get("PolicyDocument");
  if (info.policy_document.empty()) {
    s->err.message = "Missing required element PolicyDocument";
    return -EINVAL;
  }

  int ret = parse_tags(this, s->info.args.get_params(), info.tags, s->err.message);
  if(ret < 0) {
    return ret;
  }

  if (info.tags.size() > 50) {
    s->err.message = "Tags count cannot exceed 50";
    return -ERR_LIMIT_EXCEEDED;
  }

  if (const auto& account = s->auth.identity->get_account(); account) {
    info.account_id = account->id;
    ret = check_policy_limit(this, y, driver, *account, s->err.message);
    if (ret < 0) {
      return ret;
    }
    arn = make_policy_arn(info.path, info.name, info.account_id);
    info.arn = arn.to_string();
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  info.creation_date = real_clock::now();
  info.update_date = info.creation_date;
  return 0;
}

static void dump_ManagedPolicyInfo(const rgw::IAM::ManagedPolicyInfo& info, Formatter *f)
{
  encode_json("PolicyName", info.name, f);
  encode_json("DefaultVersionId", info.default_version, f);
  encode_json("PolicyId", info.id, f);
  encode_json("Path", info.path, f);
  encode_json("Arn", info.arn, f);
  encode_json("CreateDate", info.creation_date, f);
  encode_json("UpdateDate", info.update_date, f);
  encode_json("Description", info.description, f);
  f->open_array_section("Tags");
  for (const auto& tag : info.tags) {
    f->open_object_section("Tag");
    encode_json("Key", tag.first, f);
    encode_json("Value", tag.second, f);
    f->close_section();
  }
  f->close_section();
  encode_json("IsAttachable", info.is_attachable, f);
  encode_json("PermissionsBoundaryUsageCount", info.permissions_boundary_usage_count, f);
  encode_json("AttachmentCount", info.attachment_count, f);
}

int RGWCreatePolicy::forward_to_master(optional_yield y,
                                         const rgw::SiteConfig& site,
                                         std::string& uid)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("PolicyName");
  s->info.args.remove("Path");
  s->info.args.remove("PolicyDocument");
  s->info.args.remove("Description");
  auto& params = s->info.args.get_params();
  if (auto lower = params.lower_bound("Tags.member."); lower != params.end()) {
    auto upper = params.upper_bound("Tags.member.");
    params.erase(lower, upper);
  }

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, s->err, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }

  XMLObj* response = parser.find_first("CreatePolicyResponse");;
  if (!response) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: CreatePolicyResponse" << dendl;
    return -EINVAL;
  }

  XMLObj* result = response->find_first("CreatePolicyResult");
  if (!result) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: CreatePolicyResult" << dendl;
    return -EINVAL;
  }

  XMLObj* policy = result->find_first("Policy");
  if (!policy) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: Policy" << dendl;
    return -EINVAL;
  }

  try {
    RGWXMLDecoder::decode_xml("PolicyId", uid, policy, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: PolicyId" << dendl;
    return -EINVAL;
  }

  ldpp_dout(this, 4) << "policy_id decoded from forwarded response is " << uid << dendl;
  return 0;
}

void RGWCreatePolicy::execute(optional_yield y)
{

  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site, info.id);
    if (op_ret) {
      return;
    }
  }

  constexpr bool exclusive = true;
  op_ret = driver->store_customer_managed_policy(this, y, info, exclusive);
  if(op_ret < 0) {
    ldpp_dout(this, 20) << "failed to store managed policy info: " << strerror(op_ret) << dendl;
  } else {
    s->formatter->open_object_section_in_ns("CreatePolicyResponse", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("CreatePolicyResult");
    s->formatter->open_object_section("Policy");
    dump_ManagedPolicyInfo(info, s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

static int validate_policy_arn(const std::string& policy_arn,
                               std::string_view account_id,
                               rgw::ARN& arn,
                               std::string& message)
{
  if (policy_arn.empty()) {
    message = "Missing required element PolicyArn";
    return -EINVAL;
  }

  if (policy_arn.size() < 20 || policy_arn.size() > 2048) {
    message = "PolicyArn must be between 20 and 2048 characters";
    return -EINVAL;
  }

  std::string_view str = policy_arn;

  constexpr std::string_view arn_prefix = "arn:";
  if (!str.starts_with(arn_prefix)) {
    message = "PolicyArn must start with 'arn:'";
    return -EINVAL;
  }
  str.remove_prefix(arn_prefix.size());

  constexpr std::string_view partition = "aws:";
  if (!str.starts_with(partition)) {
    message = "PolicyArn partition must be 'aws'";
    return -EINVAL;
  }
  arn.partition = rgw::Partition::aws;
  str.remove_prefix(partition.size());

  constexpr std::string_view service = "iam::";
  if (!str.starts_with(service)) {
    message = "PolicyArn service must be 'iam'";
    return -EINVAL;
  }
  arn.service = rgw::Service::iam;
  str.remove_prefix(service.size());

  if (!str.starts_with(account_id)) {
    message = "PolicyArn account ID must match curent account ID";
    return -EINVAL;
  }
  arn.account = std::string(account_id);
  str.remove_prefix(account_id.size());

  if (!str.starts_with(":policy/")) {
    message = "PolicyArn must have ':policy/' after account ID";
    return -EINVAL;
  }

  if(str.find("//") != std::string_view::npos || str.back() == '/') {
    message = "Invalid policy path format";
    return -EINVAL;
  }
  arn.resource = std::string(str);

  return 0;
}

int RGWGetPolicy::init_processing(optional_yield y)
{
  
  std::string_view account;
  if (const auto& acc = s->auth.identity->get_account(); acc) {
    account = acc->id;
    std::string provider_arn = s->info.args.get("PolicyArn");
    return validate_policy_arn(provider_arn, account, arn, s->err.message);
  }
  return -ERR_METHOD_NOT_ALLOWED;
}

void RGWGetPolicy::execute(optional_yield y)
{
  std::string policy_name = arn.resource.substr(arn.resource.rfind('/') + 1);
  op_ret = driver->load_customer_managed_policy(this, y, arn.account, policy_name, info);
  if(op_ret < 0) {
    ldpp_dout(this, 20) << "failed to get managed policy info: " << strerror(op_ret) << dendl;
  } else {
    s->formatter->open_object_section_in_ns("GetPolicyResponse", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("GetPolicyResult");
    s->formatter->open_object_section("Policy");
    dump_ManagedPolicyInfo(info, s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWDeletePolicy::init_processing(optional_yield y)
{

  std::string_view account;
  if (const auto& acc = s->auth.identity->get_account(); acc) {
    account = acc->id;
    std::string provider_arn = s->info.args.get("PolicyArn");
    return validate_policy_arn(provider_arn, account, arn, s->err.message);
  }
  return -ERR_METHOD_NOT_ALLOWED;
}

void RGWDeletePolicy::execute(optional_yield y)
{
  std::string policy_name = arn.resource.substr(arn.resource.rfind('/') + 1);
  op_ret = driver->delete_customer_managed_policy(this, y, arn.account, policy_name);
  if(op_ret < 0) {
    ldpp_dout(this, 20) << "failed to delete managed policy info: " << strerror(op_ret) << dendl;
  } else {
    s->formatter->open_object_section_in_ns("DeletePolicyResponse  ", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWListPolicies::init_processing(optional_yield y)
{
  const std::string scope_str = s->info.args.get("Scope");
  if (scope_str.empty() || scope_str == "All") {
    scope = rgw::IAM::Scope::All;
  } else if (scope_str == "AWS") {
    scope = rgw::IAM::Scope::AWS;
  } else if (scope_str == "Local") {
    scope = rgw::IAM::Scope::Local;
  } else {
    s->err.message = "Invalid value for Scope";
    return -EINVAL;
  }

  s->info.args.get_bool("OnlyAttached", &only_attached, false);

  path_prefix = s->info.args.get("PathPrefix");
  if(path_prefix.empty()) {
    path_prefix = "/";
  }

  const std::string usage_filter_str = s->info.args.get("PolicyUsageFilter");
  if (usage_filter_str.empty() || usage_filter_str == "PermissionsPolicy") {
    policy_usage_filter = rgw::IAM::PolicyUsageFilter::PermissionsPolicy;
  } else if (usage_filter_str == "PermissionsBoundary") {
    policy_usage_filter = rgw::IAM::PolicyUsageFilter::PermissionsBoundary;
  } else {
    s->err.message = "Invalid value for PolicyUsageFilter";
    return -EINVAL;
  }

  marker = s->info.args.get("Marker");

  int r = s->info.args.get_int("MaxItems", &max_items, max_items);
  if (r < 0 || max_items > 1000) {
    s->err.message = "Invalid value for MaxItems";
    return -EINVAL;
  }

  if (const auto& acc = s->auth.identity->get_account(); acc) {
    account_id = acc->id;
  }

  return 0;
}

void RGWListPolicies::execute(optional_yield y)
{
  rgw::IAM::PolicyList listing;
  listing.next_marker = marker;
  op_ret = driver->list_customer_mananged_policies(this, y, account_id, scope,
                      only_attached, path_prefix, policy_usage_filter,
                      listing.next_marker, max_items, listing);
  if (op_ret == -ENOENT) {
    op_ret = 0;
  } else if (op_ret < 0) {
    return;
  }

  send_response_data(listing.policies);

  if (!started_response) {
    started_response = true;
    start_response();
  }
  end_response(listing.next_marker);
}

void RGWListPolicies::start_response()
{
  const int64_t proposed_content_length =
      op_ret ? NO_CONTENT_LENGTH : CHUNKED_TRANSFER_ENCODING;

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format), proposed_content_length);

  if (op_ret) {
    return;
  }

  dump_start(s); // <?xml block ?>
  s->formatter->open_object_section_in_ns("ListPoliciesResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ListPoliciesResult");
  s->formatter->open_array_section("Policies");
}

void RGWListPolicies::end_response(std::string_view next_marker)
{
  s->formatter->close_section(); // Policies

  const bool truncated = !next_marker.empty();
  s->formatter->dump_bool("IsTruncated", truncated);
  if (truncated) {
    s->formatter->dump_string("Marker", next_marker);
  }

  s->formatter->close_section(); // ListPoliciesResult
  s->formatter->close_section(); // ListPoliciesResponse
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWListPolicies::send_response_data(std::span<rgw::IAM::ManagedPolicyInfo> policies)
{
  if (!started_response) {
    started_response = true;
    start_response();
  }

  for (const auto& info : policies) {
    s->formatter->open_object_section("member");
    dump_ManagedPolicyInfo(info, s->formatter);
    s->formatter->close_section(); // member
  }

  // flush after each chunk
  rgw_flush_formatter(s, s->formatter);
}

void RGWListPolicies::send_response()
{
  if (!started_response) {
    start_response();
  }
}
