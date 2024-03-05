// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rest_iam_group.h"

#include <utility>
#include "include/buffer.h"
#include "common/errno.h"
#include "rgw_arn.h"
#include "rgw_common.h"
#include "rgw_iam_managed_policy.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_rest.h"
#include "rgw_rest_iam.h"


static std::string make_resource_name(const RGWGroupInfo& info)
{
  std::string_view path = info.path;
  if (path.empty()) {
    path = "/";
  }
  return string_cat_reserve(path, info.name);
}

static void dump_iam_group(const RGWGroupInfo& info, Formatter* f)
{
  encode_json("Path", info.path, f);
  encode_json("GroupName", info.name, f);
  encode_json("GroupId", info.id, f);
  encode_json("Arn", iam_group_arn(info), f);
}

static void dump_iam_user(const RGWUserInfo& info, Formatter* f)
{
  encode_json("Path", info.path, f);
  encode_json("UserName", info.display_name, f);
  encode_json("UserId", info.user_id, f);
  encode_json("Arn", iam_user_arn(info), f);
}


// CreateGroup
class RGWCreateGroup_IAM : public RGWOp {
  bufferlist post_body;
  RGWGroupInfo info;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site, std::string& uid);
 public:
  explicit RGWCreateGroup_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "create_group"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_GROUP; }
};

int RGWCreateGroup_IAM::init_processing(optional_yield y)
{
  // use account id from authenticated user/role. with AssumeRole, this may not
  // match the account of s->user
  if (const auto& account = s->auth.identity->get_account(); account) {
    info.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  info.path = s->info.args.get("Path");
  if (info.path.empty()) {
    info.path = "/";
  } else if (!validate_iam_path(info.path, s->err.message)) {
    return -EINVAL;
  }

  info.name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(info.name, s->err.message)) {
    return -EINVAL;
  }

  return 0;
}

int RGWCreateGroup_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamCreateGroup, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWCreateGroup_IAM::forward_to_master(optional_yield y,
                                         const rgw::SiteConfig& site,
                                         std::string& id)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("GroupName");
  s->info.args.remove("Path");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }

  XMLObj* response = parser.find_first("CreateGroupResponse");;
  if (!response) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: CreateGroupResponse" << dendl;
    return -EINVAL;
  }

  XMLObj* result = response->find_first("CreateGroupResult");
  if (!result) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: CreateGroupResult" << dendl;
    return -EINVAL;
  }

  XMLObj* group = result->find_first("Group");
  if (!group) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: Group" << dendl;
    return -EINVAL;
  }

  try {
    RGWXMLDecoder::decode_xml("GroupId", id, group, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: GroupId" << dendl;
    return -EINVAL;
  }

  ldpp_dout(this, 4) << "group id decoded from forwarded response is " << id << dendl;
  return 0;
}

void RGWCreateGroup_IAM::execute(optional_yield y)
{
  {
    // check the current group count against account limit
    RGWAccountInfo account;
    rgw::sal::Attrs attrs; // unused
    RGWObjVersionTracker objv; // unused
    op_ret = driver->load_account_by_id(this, y, info.account_id,
                                        account, attrs, objv);
    if (op_ret < 0) {
      ldpp_dout(this, 4) << "failed to load iam account "
          << info.account_id << ": " << cpp_strerror(op_ret) << dendl;
    }

    if (account.max_groups >= 0) { // max_groups < 0 means unlimited
      uint32_t count = 0;
      op_ret = driver->count_account_groups(this, y, info.account_id, count);
      if (op_ret < 0) {
        ldpp_dout(this, 4) << "failed to count groups for iam account "
            << info.account_id << ": " << cpp_strerror(op_ret) << dendl;
        return;
      }
      if (std::cmp_greater_equal(count, account.max_groups)) {
        s->err.message = fmt::format("Group limit {} exceeded",
                                     account.max_groups);
        op_ret = -ERR_LIMIT_EXCEEDED;
        return;
      }
    }
  }

  // generate group id. forward_to_master() may overwrite this
  uuid_d uuid;
  uuid.generate_random();
  info.id = uuid.to_string();
  info.tenant = s->auth.identity->get_tenant();

  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site, info.id);
    if (op_ret) {
      return;
    }
  }

  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;
  objv.generate_new_write_ver(get_cct());
  constexpr bool exclusive = true;
  op_ret = driver->store_group(this, y, info, attrs, objv, exclusive, nullptr);
}

void RGWCreateGroup_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "CreateGroupResponse", RGW_REST_IAM_XMLNS};
    {
      Formatter::ObjectSection result{*f, "CreateGroupResult"};
      Formatter::ObjectSection group{*f, "Group"};
      dump_iam_group(info, f);
      // /Group
      // /CreateGroupResult
    }
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /CreateGroupResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// GetGroup
class RGWGetGroup_IAM : public RGWOp {
  rgw_account_id account_id;
  RGWGroupInfo info;
  std::string marker;
  int max_items = 100;
  rgw::sal::UserList listing;
 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "get_group"; }
  RGWOpType get_type() override { return RGW_OP_GET_GROUP; }
};

int RGWGetGroup_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  marker = s->info.args.get("Marker");

  int r = s->info.args.get_int("MaxItems", &max_items, max_items);
  if (r < 0 || max_items > 1000) {
    s->err.message = "Invalid value for MaxItems";
    return -EINVAL;
  }

  rgw::sal::Attrs attrs_ignored;
  RGWObjVersionTracker objv_ignored;
  r = driver->load_group_by_name(this, y, account_id, name, info,
                                 attrs_ignored, objv_ignored);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWGetGroup_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamGetGroup, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWGetGroup_IAM::execute(optional_yield y)
{
  const auto& tenant = s->auth.identity->get_tenant();
  op_ret = driver->list_group_users(this, y, tenant, info.id,
                                    marker, max_items, listing);
}

void RGWGetGroup_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "GetGroupResponse", RGW_REST_IAM_XMLNS};
    {
      Formatter::ObjectSection result{*f, "GetGroupResult"};
      {
        Formatter::ObjectSection Group{*f, "Group"};
        dump_iam_group(info, f);
      } // /Group
      {
        Formatter::ArraySection users{*f, "Users"};
        for (const auto& user : listing.users) {
          Formatter::ObjectSection result{*f, "member"};
          dump_iam_user(user, f);
        } // /member
      } // /Users
      const bool is_truncated = !listing.next_marker.empty();
      f->dump_bool("IsTruncated", is_truncated);
      if (is_truncated) {
        f->dump_string("Marker", listing.next_marker);
      }
      // /GetGroupResult
    }
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /GetGroupResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// UpdateGroup
class RGWUpdateGroup_IAM : public RGWOp {
  bufferlist post_body;
  std::string new_path;
  std::string new_name;
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWUpdateGroup_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "update_group"; }
  RGWOpType get_type() override { return RGW_OP_UPDATE_GROUP; }
};

int RGWUpdateGroup_IAM::init_processing(optional_yield y)
{
  rgw_account_id account_id;
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  new_path = s->info.args.get("NewPath");
  if (!new_path.empty() && !validate_iam_path(new_path, s->err.message)) {
    return -EINVAL;
  }

  new_name = s->info.args.get("NewGroupName");
  if (!new_name.empty() &&
      !validate_iam_group_name(new_name, s->err.message)) {
    return -EINVAL;
  }

  const std::string name = s->info.args.get("GroupName");
  if (name.empty()) {
    s->err.message = "Missing required element GroupName";
    return -EINVAL;
  }

  int r = driver->load_group_by_name(this, y, account_id, name,
                                     info, attrs, objv);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWUpdateGroup_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamUpdateGroup, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWUpdateGroup_IAM::forward_to_master(optional_yield y, const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("NewPath");
  s->info.args.remove("NewGroupName");
  s->info.args.remove("GroupName");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWUpdateGroup_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_group_write(this, y, driver, info, attrs, objv,
      [this, y] {
        const RGWGroupInfo old_info = info;

        if (!new_path.empty()) {
          info.path = new_path;
        }
        if (!new_name.empty()) {
          info.name = new_name;
        }

        if (info.path == old_info.path &&
            info.name == old_info.name) {
          return 0; // nothing to do, return success
        }

        constexpr bool exclusive = false;
        return driver->store_group(this, y, info, attrs, objv,
                                   exclusive, &old_info);
      });
}

void RGWUpdateGroup_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "UpdateGroupResponse", RGW_REST_IAM_XMLNS};
    {
      Formatter::ObjectSection result{*f, "UpdateGroupResult"};
      Formatter::ObjectSection group{*f, "Group"};
      dump_iam_group(info, f);
      // /Group
      // /UpdateGroupResult
    }
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /UpdateGroupResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// DeleteGroup
class RGWDeleteGroup_IAM : public RGWOp {
  bufferlist post_body;
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
  int check_empty(optional_yield y);
 public:
  explicit RGWDeleteGroup_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "delete_group"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_GROUP; }
};

int RGWDeleteGroup_IAM::init_processing(optional_yield y)
{
  rgw_account_id account_id;
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (name.empty()) {
    s->err.message = "Missing required element GroupName";
    return -EINVAL;
  }

  int r = driver->load_group_by_name(this, y, account_id, name,
                                     info, attrs, objv);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWDeleteGroup_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamDeleteGroup, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWDeleteGroup_IAM::forward_to_master(optional_yield y, const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("GroupName");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

int RGWDeleteGroup_IAM::check_empty(optional_yield y)
{
  if (!s->penv.site->is_meta_master()) {
    // only check on the master zone. if a forwarded DeleteGroup request
    // succeeds on the master zone, it needs to succeed here too
    return 0;
  }

  // verify that all policies are removed first
  if (auto p = attrs.find(RGW_ATTR_IAM_POLICY); p != attrs.end()) {
    std::map<std::string, std::string> policies;
    try {
      decode(policies, p->second);
    } catch (const buffer::error&) {
      ldpp_dout(this, 0) << "ERROR: failed to decode group policies" << dendl;
      return -EIO;
    }

    if (!policies.empty()) {
      s->err.message = "The group cannot be deleted until all group policies are removed";
      return -ERR_DELETE_CONFLICT;
    }
  }
  if (auto p = attrs.find(RGW_ATTR_MANAGED_POLICY); p != attrs.end()) {
    rgw::IAM::ManagedPolicies policies;
    try {
      decode(policies, p->second);
    } catch (const buffer::error&) {
      ldpp_dout(this, 0) << "ERROR: failed to decode managed policies" << dendl;
      return -EIO;
    }

    if (!policies.arns.empty()) {
      s->err.message = "The group cannot be deleted until all managed policies are detached";
      return -ERR_DELETE_CONFLICT;
    }
  }

  // check that group has no users
  const std::string& tenant = s->auth.identity->get_tenant();
  rgw::sal::UserList listing;
  int r = driver->list_group_users(this, y, tenant, info.id, "", 1, listing);
  if (r < 0) {
    return r;
  }

  if (listing.users.size()) {
    s->err.message = "The group cannot be deleted until all users are removed";
    return -ERR_DELETE_CONFLICT;
  }

  return 0;
}

void RGWDeleteGroup_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_group_write(this, y, driver, info, attrs, objv,
      [this, y] {
        if (int r = check_empty(y); r < 0) {
          return r;
        }
        return driver->remove_group(this, y, info, objv);
      });

  if (op_ret == -ENOENT) {
    if (!site.is_meta_master()) {
      // delete succeeded on the master, return that success here too
      op_ret = 0;
    } else {
      s->err.message = "No such GroupName in the account";
      op_ret = -ERR_NO_SUCH_ENTITY;
    }
  }
}

void RGWDeleteGroup_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "DeleteGroupResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /DeleteGroupResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// ListGroups
class RGWListGroups_IAM : public RGWOp {
  rgw_account_id account_id;
  std::string marker;
  std::string path_prefix;
  int max_items = 100;

  bool started_response = false;
  void start_response();
  void end_response(std::string_view next_marker);
  void send_response_data(std::span<RGWGroupInfo> groups);
 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_groups"; }
  RGWOpType get_type() override { return RGW_OP_LIST_GROUPS; }
};

int RGWListGroups_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  marker = s->info.args.get("Marker");
  path_prefix = s->info.args.get("PathPrefix");

  int r = s->info.args.get_int("MaxItems", &max_items, max_items);
  if (r < 0 || max_items > 1000) {
    s->err.message = "Invalid value for MaxItems";
    return -EINVAL;
  }

  return 0;
}

int RGWListGroups_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = "";
  const rgw::ARN arn{resource_name, "group", account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamListGroups, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWListGroups_IAM::execute(optional_yield y)
{
  rgw::sal::GroupList listing;
  listing.next_marker = marker;

  op_ret = driver->list_account_groups(this, y, account_id,
                                       path_prefix, listing.next_marker,
                                       max_items, listing);
  if (op_ret == -ENOENT) {
    op_ret = 0;
  } else if (op_ret < 0) {
    return;
  }

  send_response_data(listing.groups);

  if (!started_response) {
    started_response = true;
    start_response();
  }
  end_response(listing.next_marker);
}

void RGWListGroups_IAM::start_response()
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
  s->formatter->open_object_section_in_ns("ListGroupsResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ListGroupsResult");
  s->formatter->open_array_section("Groups");
}

void RGWListGroups_IAM::end_response(std::string_view next_marker)
{
  s->formatter->close_section(); // Groups

  const bool truncated = !next_marker.empty();
  s->formatter->dump_bool("IsTruncated", truncated);
  if (truncated) {
    s->formatter->dump_string("Marker", next_marker);
  }

  s->formatter->close_section(); // ListGroupsResult
  s->formatter->close_section(); // ListGroupsResponse
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWListGroups_IAM::send_response_data(std::span<RGWGroupInfo> groups)
{
  if (!started_response) {
    started_response = true;
    start_response();
  }

  for (const auto& info : groups) {
    s->formatter->open_object_section("member");
    dump_iam_group(info, s->formatter);
    s->formatter->close_section(); // member
  }

  // flush after each chunk
  rgw_flush_formatter(s, s->formatter);
}

void RGWListGroups_IAM::send_response()
{
  if (!started_response) { // errored out before execute() wrote anything
    start_response();
  }
}


// AddUserToGroup
class RGWAddUserToGroup_IAM : public RGWOp {
  bufferlist post_body;
  RGWGroupInfo group;
  std::unique_ptr<rgw::sal::User> user;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWAddUserToGroup_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "add_user_to_group"; }
  RGWOpType get_type() override { return RGW_OP_ADD_USER_TO_GROUP; }
};

int RGWAddUserToGroup_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    group.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  const std::string username = s->info.args.get("UserName");
  if (!validate_iam_user_name(username, s->err.message)) {
    return -EINVAL;
  }

  // look up group by GroupName
  rgw::sal::Attrs attrs_ignored;
  RGWObjVersionTracker objv_ignored;
  int r = driver->load_group_by_name(this, y, group.account_id, name,
                                     group, attrs_ignored, objv_ignored);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  if (r < 0) {
    return r;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  r = driver->load_account_user_by_name(this, y, group.account_id,
                                        tenant, username, &user);
  if (r == -ENOENT) {
    s->err.message = "No such UserName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWAddUserToGroup_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(group);
  const rgw::ARN arn{resource_name, "group", group.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamAddUserToGroup, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWAddUserToGroup_IAM::forward_to_master(optional_yield y,
                                             const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("GroupName");
  s->info.args.remove("UserName");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWAddUserToGroup_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_user_write(this, y, user.get(),
      [this, y] {
        RGWUserInfo& info = user->get_info();
        RGWUserInfo old_info = info;

        if (!info.group_ids.insert(group.id).second) {
          return 0; // nothing to do, return success
        }

        constexpr bool exclusive = false;
        return user->store_user(this, y, exclusive, &old_info);
      });
}

void RGWAddUserToGroup_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "AddUserToGroupResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /AddUserToGroupResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// RemoveUserFromGroup
class RGWRemoveUserFromGroup_IAM : public RGWOp {
  bufferlist post_body;
  RGWGroupInfo group;
  std::unique_ptr<rgw::sal::User> user;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWRemoveUserFromGroup_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "remove_user_from_group"; }
  RGWOpType get_type() override { return RGW_OP_REMOVE_USER_FROM_GROUP; }
};

int RGWRemoveUserFromGroup_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    group.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  const std::string username = s->info.args.get("UserName");
  if (!validate_iam_user_name(username, s->err.message)) {
    return -EINVAL;
  }

  // look up group by GroupName
  rgw::sal::Attrs attrs_ignored;
  RGWObjVersionTracker objv_ignored;
  int r = driver->load_group_by_name(this, y, group.account_id, name,
                                     group, attrs_ignored, objv_ignored);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  if (r < 0) {
    return r;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  r = driver->load_account_user_by_name(this, y, group.account_id,
                                        tenant, username, &user);
  if (r == -ENOENT) {
    s->err.message = "No such UserName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWRemoveUserFromGroup_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(group);
  const rgw::ARN arn{resource_name, "group", group.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamRemoveUserFromGroup, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWRemoveUserFromGroup_IAM::forward_to_master(optional_yield y,
                                                  const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("GroupName");
  s->info.args.remove("UserName");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWRemoveUserFromGroup_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_user_write(this, y, user.get(),
      [this, y] {
        RGWUserInfo& info = user->get_info();
        RGWUserInfo old_info = info;

        auto id = info.group_ids.find(group.id);
        if (id == info.group_ids.end()) {
          return 0; // nothing to do, return success
        }
        info.group_ids.erase(id);

        constexpr bool exclusive = false;
        return user->store_user(this, y, exclusive, &old_info);
      });
}

void RGWRemoveUserFromGroup_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "RemoveUserFromGroupResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /RemoveUserFromGroupResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// ListGroupsForUser
class RGWListGroupsForUser_IAM : public RGWOp {
  rgw_account_id account_id;
  std::string marker;
  int max_items = 100;
  std::unique_ptr<rgw::sal::User> user;

 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_groups_for_user"; }
  RGWOpType get_type() override { return RGW_OP_LIST_GROUPS_FOR_USER; }
};

int RGWListGroupsForUser_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  marker = s->info.args.get("Marker");

  int r = s->info.args.get_int("MaxItems", &max_items, max_items);
  if (r < 0 || max_items > 1000) {
    s->err.message = "Invalid value for MaxItems";
    return -EINVAL;
  }

  const std::string username = s->info.args.get("UserName");
  if (!validate_iam_user_name(username, s->err.message)) {
    return -EINVAL;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  r = driver->load_account_user_by_name(this, y, account_id,
                                        tenant, username, &user);
  if (r == -ENOENT) {
    s->err.message = "No such UserName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWListGroupsForUser_IAM::verify_permission(optional_yield y)
{
  const RGWUserInfo& info = user->get_info();
  const std::string resource_name = string_cat_reserve(info.path, info.display_name);
  const rgw::ARN arn{resource_name, "user", account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamListGroupsForUser, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWListGroupsForUser_IAM::execute(optional_yield y)
{
  rgw::sal::GroupList listing;
  listing.next_marker = marker;

  op_ret = user->list_groups(this, y, marker, max_items, listing);
  if (op_ret == -ENOENT) {
    op_ret = 0;
  } else if (op_ret < 0) {
    return;
  }

  dump_start(s); // <?xml block ?>
  Formatter* f = s->formatter;
  Formatter::ObjectSection response{*f, "ListGroupsForUserResponse", RGW_REST_IAM_XMLNS};
  {
    Formatter::ObjectSection result{*f, "ListGroupsForUserResult"};
    {
      Formatter::ArraySection groups{*f, "Groups"};
      for (const auto& info : listing.groups) {
        Formatter::ObjectSection result{*f, "member"};
        dump_iam_group(info, s->formatter);
      } // /member
    } // /Groups
    const bool truncated = !listing.next_marker.empty();
    f->dump_bool("IsTruncated", truncated);
    if (truncated) {
      f->dump_string("Marker", listing.next_marker);
    }
  } // /ListGroupsForUserResult
  Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
  f->dump_string("RequestId", s->trans_id);
  // /ResponseMetadata
  // /ListGroupsForUserResponse
}

void RGWListGroupsForUser_IAM::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// PutGroupPolicy
class RGWPutGroupPolicy_IAM : public RGWOp {
  bufferlist post_body;
  std::string policy_name;
  std::string policy_document;
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWPutGroupPolicy_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "put_group_policy"; }
  RGWOpType get_type() override { return RGW_OP_PUT_GROUP_POLICY; }
};

int RGWPutGroupPolicy_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    info.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  policy_document = s->info.args.get("PolicyDocument");
  if (policy_document.empty()) {
    s->err.message = "Missing required element PolicyDocument";
    return -EINVAL;
  }

  // look up group by GroupName
  int r = driver->load_group_by_name(this, y, info.account_id, name,
                                     info, attrs, objv);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWPutGroupPolicy_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamPutGroupPolicy, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWPutGroupPolicy_IAM::forward_to_master(optional_yield y,
                                                const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("GroupName");
  s->info.args.remove("PolicyName");
  s->info.args.remove("PolicyDocument");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWPutGroupPolicy_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  try {
    // validate the document
    const rgw::IAM::Policy p(
      s->cct, nullptr, policy_document,
      s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
  } catch (rgw::IAM::PolicyParseException& e) {
    s->err.message = std::move(e.msg);
    op_ret = -ERR_MALFORMED_DOC;
    return;
  }

  op_ret = retry_raced_group_write(this, y, driver, info, attrs, objv,
      [this, y] {
        std::map<std::string, std::string> policies;
        if (auto p = attrs.find(RGW_ATTR_IAM_POLICY); p != attrs.end()) try {
          decode(policies, p->second);
        } catch (const buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode group policies" << dendl;
          return -EIO;
        }

        policies[policy_name] = policy_document;

        constexpr size_t GROUP_POLICIES_MAX_NUM = 100;
        if (policies.size() > GROUP_POLICIES_MAX_NUM) {
          s->err.message = fmt::format("Group policy limit {} exceeded",
                                       GROUP_POLICIES_MAX_NUM);
          return -ERR_LIMIT_EXCEEDED;
        }

        bufferlist bl;
        encode(policies, bl);
        attrs[RGW_ATTR_IAM_POLICY] = std::move(bl);

        constexpr bool exclusive = false;
        return driver->store_group(this, y, info, attrs, objv, exclusive, &info);
      });
}

void RGWPutGroupPolicy_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "PutGroupPolicyResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /PutGroupPolicyResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// GetGroupPolicy
class RGWGetGroupPolicy_IAM : public RGWOp {
  std::string policy_name;
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;

 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "get_group_policy"; }
  RGWOpType get_type() override { return RGW_OP_GET_GROUP_POLICY; }
};

int RGWGetGroupPolicy_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    info.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  // look up group by GroupName
  RGWObjVersionTracker objv_ignored;
  int r = driver->load_group_by_name(this, y, info.account_id, name,
                                     info, attrs, objv_ignored);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWGetGroupPolicy_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamGetGroupPolicy, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWGetGroupPolicy_IAM::execute(optional_yield y)
{
  std::map<std::string, std::string> policies;
  if (auto p = attrs.find(RGW_ATTR_IAM_POLICY); p != attrs.end()) try {
    decode(policies, p->second);
  } catch (const buffer::error& err) {
    ldpp_dout(this, 0) << "ERROR: failed to decode group policies" << dendl;
    op_ret = -EIO;
    return;
  }

  auto policy = policies.find(policy_name);
  if (policy == policies.end()) {
    s->err.message = "No such PolicyName on the group";
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }

  dump_start(s); // <?xml block ?>
  Formatter* f = s->formatter;
  Formatter::ObjectSection response{*f, "GetGroupPolicyResponse", RGW_REST_IAM_XMLNS};
  {
    Formatter::ObjectSection result{*f, "GetGroupPolicyResult"};
    encode_json("GroupName", info.name, f);
    encode_json("PolicyName", policy_name, f);
    encode_json("PolicyDocument", policy->second, f);
    // /GetGroupPolicyResult
  }
  Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
  f->dump_string("RequestId", s->trans_id);
  // /ResponseMetadata
  // /GetGroupPolicyResponse
}

void RGWGetGroupPolicy_IAM::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// DeleteGroupPolicy
class RGWDeleteGroupPolicy_IAM : public RGWOp {
  bufferlist post_body;
  std::string policy_name;
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWDeleteGroupPolicy_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "detach_group_policy"; }
  RGWOpType get_type() override { return RGW_OP_DETACH_GROUP_POLICY; }
};

int RGWDeleteGroupPolicy_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    info.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  // look up group by GroupName
  int r = driver->load_group_by_name(this, y, info.account_id, name,
                                     info, attrs, objv);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWDeleteGroupPolicy_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamDeleteGroupPolicy, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWDeleteGroupPolicy_IAM::forward_to_master(optional_yield y,
                                                const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("GroupName");
  s->info.args.remove("PolicyName");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWDeleteGroupPolicy_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_group_write(this, y, driver, info, attrs, objv,
      [this, y, &site] {
        std::map<std::string, std::string> policies;
        if (auto it = attrs.find(RGW_ATTR_IAM_POLICY); it != attrs.end()) try {
          decode(policies, it->second);
        } catch (buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
          return -EIO;
        }

        auto i = policies.find(policy_name);
        if (i == policies.end()) {
          if (!site.is_meta_master()) {
            return 0; // delete succeeded on the master
          }
          return -ERR_NO_SUCH_ENTITY;
        }
        policies.erase(i);

        bufferlist bl;
        encode(policies, bl);
        attrs[RGW_ATTR_IAM_POLICY] = std::move(bl);

        constexpr bool exclusive = false;
        return driver->store_group(this, y, info, attrs, objv, exclusive, &info);
      });
}

void RGWDeleteGroupPolicy_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "DeleteGroupPolicyResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /DeleteGroupPolicyResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// ListGroupPolicies
class RGWListGroupPolicies_IAM : public RGWOp {
  bufferlist post_body;
  std::string marker;
  int max_items = 100;
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;

 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_group_policies"; }
  RGWOpType get_type() override { return RGW_OP_LIST_GROUP_POLICIES; }
};

int RGWListGroupPolicies_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    info.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  marker = s->info.args.get("Marker");

  int r = s->info.args.get_int("MaxItems", &max_items, max_items);
  if (r < 0 || max_items > 1000) {
    s->err.message = "Invalid value for MaxItems";
    return -EINVAL;
  }

  // look up group by GroupName
  RGWObjVersionTracker objv_ignored;
  r = driver->load_group_by_name(this, y, info.account_id, name,
                                 info, attrs, objv_ignored);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWListGroupPolicies_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamListGroupPolicies, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWListGroupPolicies_IAM::execute(optional_yield y)
{
  std::map<std::string, std::string> policies;
  if (auto p = attrs.find(RGW_ATTR_IAM_POLICY); p != attrs.end()) try {
    decode(policies, p->second);
  } catch (const buffer::error& err) {
    ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
    op_ret = -EIO;
  }

  dump_start(s); // <?xml block ?>
  Formatter* f = s->formatter;
  Formatter::ObjectSection response{*f, "ListGroupPoliciesResponse", RGW_REST_IAM_XMLNS};
  {
    Formatter::ObjectSection result{*f, "ListGroupPoliciesResult"};
    auto policy = policies.lower_bound(marker);
    {
      Formatter::ArraySection names{*f, "PolicyNames"};
      for (; policy != policies.end() && max_items > 0; ++policy, --max_items) {
        encode_json("member", policy->first, f);
      }
    } // /PolicyNames
    const bool is_truncated = (policy != policies.end());
    encode_json("IsTruncated", is_truncated, f);
    if (is_truncated) {
      encode_json("Marker", policy->first, f);
    }
  } // /ListUserPoliciesResult
  Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
  f->dump_string("RequestId", s->trans_id);
  // /ResponseMetadata
  // /ListGroupPoliciesResponse
}

void RGWListGroupPolicies_IAM::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// AttachGroupPolicy
class RGWAttachGroupPolicy_IAM : public RGWOp {
  bufferlist post_body;
  std::string policy_arn;
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWAttachGroupPolicy_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "attach_group_policy"; }
  RGWOpType get_type() override { return RGW_OP_ATTACH_GROUP_POLICY; }
};

int RGWAttachGroupPolicy_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    info.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  policy_arn = s->info.args.get("PolicyArn");
  if (!validate_iam_policy_arn(policy_arn, s->err.message)) {
    return -EINVAL;
  }

  // look up group by GroupName
  int r = driver->load_group_by_name(this, y, info.account_id, name,
                                     info, attrs, objv);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWAttachGroupPolicy_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamAttachGroupPolicy, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWAttachGroupPolicy_IAM::forward_to_master(optional_yield y,
                                                const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("GroupName");
  s->info.args.remove("PolicyArn");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWAttachGroupPolicy_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  // validate the policy arn
  try {
    const auto p = rgw::IAM::get_managed_policy(s->cct, policy_arn);
    if (!p) {
      op_ret = -ERR_NO_SUCH_ENTITY;
      s->err.message = "The requested PolicyArn is not recognized";
      return;
    }
  } catch (const rgw::IAM::PolicyParseException& e) {
    ldpp_dout(this, 5) << "failed to parse policy: " << e.what() << dendl;
    s->err.message = e.what();
    op_ret = -ERR_MALFORMED_DOC;
    return;
  }

  op_ret = retry_raced_group_write(this, y, driver, info, attrs, objv,
      [this, y] {
        rgw::IAM::ManagedPolicies policies;
        if (auto it = attrs.find(RGW_ATTR_MANAGED_POLICY); it != attrs.end()) try {
          decode(policies, it->second);
        } catch (buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
          return -EIO;
        }

        if (!policies.arns.insert(policy_arn).second) {
          return 0; // nothing to do, return success
        }

        bufferlist bl;
        encode(policies, bl);
        attrs[RGW_ATTR_MANAGED_POLICY] = std::move(bl);

        constexpr bool exclusive = false;
        return driver->store_group(this, y, info, attrs, objv, exclusive, &info);
      });
}

void RGWAttachGroupPolicy_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "AttachGroupPolicyResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /AttachGroupPolicyResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// DetachGroupPolicy
class RGWDetachGroupPolicy_IAM : public RGWOp {
  bufferlist post_body;
  std::string policy_arn;
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWDetachGroupPolicy_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "detach_group_policy"; }
  RGWOpType get_type() override { return RGW_OP_DETACH_GROUP_POLICY; }
};

int RGWDetachGroupPolicy_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    info.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  policy_arn = s->info.args.get("PolicyArn");
  if (!validate_iam_policy_arn(policy_arn, s->err.message)) {
    return -EINVAL;
  }

  // look up group by GroupName
  int r = driver->load_group_by_name(this, y, info.account_id, name,
                                     info, attrs, objv);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWDetachGroupPolicy_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamDetachGroupPolicy, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWDetachGroupPolicy_IAM::forward_to_master(optional_yield y,
                                                const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("GroupName");
  s->info.args.remove("PolicyArn");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWDetachGroupPolicy_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_group_write(this, y, driver, info, attrs, objv,
      [this, y, &site] {
        rgw::IAM::ManagedPolicies policies;
        if (auto it = attrs.find(RGW_ATTR_MANAGED_POLICY); it != attrs.end()) try {
          decode(policies, it->second);
        } catch (const buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
          return -EIO;
        }

        auto i = policies.arns.find(policy_arn);
        if (i == policies.arns.end()) {
          if (!site.is_meta_master()) {
            return 0; // delete succeeded on the master
          }
          return -ERR_NO_SUCH_ENTITY;
        }
        policies.arns.erase(i);

        bufferlist bl;
        encode(policies, bl);
        attrs[RGW_ATTR_MANAGED_POLICY] = std::move(bl);

        constexpr bool exclusive = false;
        return driver->store_group(this, y, info, attrs, objv, exclusive, &info);
      });
}

void RGWDetachGroupPolicy_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "DetachGroupPolicyResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /DetachGroupPolicyResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// ListAttachedGroupPolicies
class RGWListAttachedGroupPolicies_IAM : public RGWOp {
  bufferlist post_body;
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;
  std::string marker;
  int max_items = 100;

 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_attached_group_policies"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ATTACHED_GROUP_POLICIES; }
};

int RGWListAttachedGroupPolicies_IAM::init_processing(optional_yield y)
{
  if (const auto& account = s->auth.identity->get_account(); account) {
    info.account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string name = s->info.args.get("GroupName");
  if (!validate_iam_group_name(name, s->err.message)) {
    return -EINVAL;
  }

  marker = s->info.args.get("Marker");

  int r = s->info.args.get_int("MaxItems", &max_items, max_items);
  if (r < 0 || max_items > 1000) {
    s->err.message = "Invalid value for MaxItems";
    return -EINVAL;
  }

  // look up group by GroupName
  RGWObjVersionTracker objv_ignored;
  r = driver->load_group_by_name(this, y, info.account_id, name,
                                 info, attrs, objv_ignored);
  if (r == -ENOENT) {
    s->err.message = "No such GroupName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWListAttachedGroupPolicies_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "group", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamListAttachedGroupPolicies, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWListAttachedGroupPolicies_IAM::execute(optional_yield y)
{
  rgw::IAM::ManagedPolicies policies;
  if (auto p = attrs.find(RGW_ATTR_MANAGED_POLICY); p != attrs.end()) try {
    decode(policies, p->second);
  } catch (const buffer::error& err) {
    ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
    op_ret = -EIO;
  }


  dump_start(s); // <?xml block ?>
  Formatter* f = s->formatter;
  Formatter::ObjectSection response{*f, "ListAttachedGroupPoliciesResponse", RGW_REST_IAM_XMLNS};
  {
    Formatter::ObjectSection result{*f, "ListAttachedGroupPoliciesResult"};

    auto policy = policies.arns.lower_bound(marker);
    {
      Formatter::ArraySection arr{*f, "AttachedPolicies"};
      for (; policy != policies.arns.end() && max_items > 0; ++policy, --max_items) {
        Formatter::ObjectSection result{*f, "member"};
        std::string_view arn = *policy;
        if (auto p = arn.find('/'); p != arn.npos) {
          encode_json("PolicyName", arn.substr(p + 1), f);
        }
        encode_json("PolicyArn", arn, f);
      }
    } // /AttachedPolicies
    const bool is_truncated = (policy != policies.arns.end());
    encode_json("IsTruncated", is_truncated, f);
    if (is_truncated) {
      encode_json("Marker", *policy, f);
    }
    // /ListAttachedUserPoliciesResult
  }
  Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
  f->dump_string("RequestId", s->trans_id);
  // /ResponseMetadata
  // /ListAttachedGroupPoliciesResponse
}

void RGWListAttachedGroupPolicies_IAM::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


RGWOp* make_iam_create_group_op(const ceph::bufferlist& post_body) {
  return new RGWCreateGroup_IAM(post_body);
}
RGWOp* make_iam_get_group_op(const ceph::bufferlist&) {
  return new RGWGetGroup_IAM;
}
RGWOp* make_iam_update_group_op(const ceph::bufferlist& post_body) {
  return new RGWUpdateGroup_IAM(post_body);
}
RGWOp* make_iam_delete_group_op(const ceph::bufferlist& post_body) {
  return new RGWDeleteGroup_IAM(post_body);
}
RGWOp* make_iam_list_groups_op(const ceph::bufferlist&) {
  return new RGWListGroups_IAM;
}

RGWOp* make_iam_add_user_to_group_op(const ceph::bufferlist& post_body) {
  return new RGWAddUserToGroup_IAM(post_body);
}
RGWOp* make_iam_remove_user_from_group_op(const ceph::bufferlist& post_body) {
  return new RGWRemoveUserFromGroup_IAM(post_body);
}
RGWOp* make_iam_list_groups_for_user_op(const ceph::bufferlist& unused) {
  return new RGWListGroupsForUser_IAM;
}

RGWOp* make_iam_put_group_policy_op(const ceph::bufferlist& post_body) {
  return new RGWPutGroupPolicy_IAM(post_body);
}
RGWOp* make_iam_get_group_policy_op(const ceph::bufferlist& unused) {
  return new RGWGetGroupPolicy_IAM;
}
RGWOp* make_iam_delete_group_policy_op(const ceph::bufferlist& post_body) {
  return new RGWDeleteGroupPolicy_IAM(post_body);
}
RGWOp* make_iam_list_group_policies_op(const ceph::bufferlist& unused) {
  return new RGWListGroupPolicies_IAM;
}
RGWOp* make_iam_attach_group_policy_op(const ceph::bufferlist& post_body) {
  return new RGWAttachGroupPolicy_IAM(post_body);
}
RGWOp* make_iam_detach_group_policy_op(const ceph::bufferlist& post_body) {
  return new RGWDetachGroupPolicy_IAM(post_body);
}
RGWOp* make_iam_list_attached_group_policies_op(const ceph::bufferlist& unused) {
  return new RGWListAttachedGroupPolicies_IAM();
}

