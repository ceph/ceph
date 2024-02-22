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

#include "rgw_rest_iam_user.h"

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


static std::string make_resource_name(const RGWUserInfo& info)
{
  std::string_view path = info.path;
  if (path.empty()) {
    path = "/";
  }
  return string_cat_reserve(path, info.display_name);
}

static void dump_iam_user(const RGWUserInfo& info, Formatter* f)
{
  encode_json("Path", info.path, f);
  encode_json("UserName", info.display_name, f);
  encode_json("UserId", info.user_id, f);
  encode_json("Arn", iam_user_arn(info), f);
  encode_json("CreateDate", info.create_date, f);
}


// CreateUser
class RGWCreateUser_IAM : public RGWOp {
  bufferlist post_body;
  RGWUserInfo info;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site, std::string& uid);
 public:
  explicit RGWCreateUser_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "create_user"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_USER; }
};

int RGWCreateUser_IAM::init_processing(optional_yield y)
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

  info.display_name = s->info.args.get("UserName");
  if (!validate_iam_user_name(info.display_name, s->err.message)) {
    return -EINVAL;
  }

  // TODO: Tags
  return 0;
}

int RGWCreateUser_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "user", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamCreateUser, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWCreateUser_IAM::forward_to_master(optional_yield y,
                                         const rgw::SiteConfig& site,
                                         std::string& uid)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("UserName");
  s->info.args.remove("Path");
  s->info.args.remove("PermissionsBoundary");
  s->info.args.remove("Action");
  s->info.args.remove("Version");
  auto& params = s->info.args.get_params();
  if (auto lower = params.lower_bound("Tags.member."); lower != params.end()) {
    auto upper = params.upper_bound("Tags.member.");
    params.erase(lower, upper);
  }

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }

  XMLObj* response = parser.find_first("CreateUserResponse");;
  if (!response) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: CreateUserResponse" << dendl;
    return -EINVAL;
  }

  XMLObj* result = response->find_first("CreateUserResult");
  if (!result) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: CreateUserResult" << dendl;
    return -EINVAL;
  }

  XMLObj* user = result->find_first("User");
  if (!user) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: User" << dendl;
    return -EINVAL;
  }

  try {
    RGWXMLDecoder::decode_xml("UserId", uid, user, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: UserId" << dendl;
    return -EINVAL;
  }

  ldpp_dout(this, 4) << "user_id decoded from forwarded response is " << uid << dendl;
  return 0;
}

void RGWCreateUser_IAM::execute(optional_yield y)
{
  // check the current user count against account limit
  RGWAccountInfo account;
  rgw::sal::Attrs attrs; // unused
  RGWObjVersionTracker objv; // unused
  op_ret = driver->load_account_by_id(this, y, info.account_id,
                                      account, attrs, objv);
  if (op_ret < 0) {
    ldpp_dout(this, 4) << "failed to load iam account "
        << info.account_id << ": " << cpp_strerror(op_ret) << dendl;
    return;
  }

  if (account.max_users >= 0) { // max_users < 0 means unlimited
    uint32_t count = 0;
    op_ret = driver->count_account_users(this, y, info.account_id, count);
    if (op_ret < 0) {
      ldpp_dout(this, 4) << "failed to count users for iam account "
          << info.account_id << ": " << cpp_strerror(op_ret) << dendl;
      return;
    }
    if (std::cmp_greater_equal(count, account.max_users)) {
      s->err.message = fmt::format("User limit {} exceeded",
                                   account.max_users);
      op_ret = ERR_LIMIT_EXCEEDED;
      return;
    }
  }

  // generate user id. forward_to_master() may overwrite this
  uuid_d uuid;
  uuid.generate_random();
  info.user_id.id = uuid.to_string();
  info.user_id.tenant = s->auth.identity->get_tenant();

  info.create_date = ceph::real_clock::now();

  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site, info.user_id.id);
    if (op_ret) {
      return;
    }
  }

  std::unique_ptr<rgw::sal::User> user = driver->get_user(info.user_id);
  user->get_info() = info;

  constexpr bool exclusive = true;
  op_ret = user->store_user(this, y, exclusive, nullptr);
}

void RGWCreateUser_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "CreateUserResponse", RGW_REST_IAM_XMLNS};
    {
      Formatter::ObjectSection result{*f, "CreateUserResult"};
      Formatter::ObjectSection user{*f, "User"};
      dump_iam_user(info, f);
      // /User
      // /CreateUserResult
    }
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /CreateUserResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// GetUser
class RGWGetUser_IAM : public RGWOp {
  rgw_account_id account_id;
  std::unique_ptr<rgw::sal::User> user;
 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "get_user"; }
  RGWOpType get_type() override { return RGW_OP_GET_USER; }
};

int RGWGetUser_IAM::init_processing(optional_yield y)
{
  // use account id from authenticated user/role. with AssumeRole, this may not
  // match the account of s->user
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string username = s->info.args.get("UserName");
  if (username.empty()) {
    // If you do not specify a user name, IAM determines the user name
    // implicitly based on the AWS access key ID signing the request.
    // This operation works for access keys under the AWS account.
    // Consequently, you can use this operation to manage AWS account
    // root user credentials.
    user = s->user->clone();
    return 0;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  int r = driver->load_account_user_by_name(this, y, account_id,
                                            tenant, username, &user);
  // root user is hidden from user apis
  const bool is_root = (user && user->get_type() == TYPE_ROOT);
  if (r == -ENOENT || is_root) {
    s->err.message = "No such UserName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWGetUser_IAM::verify_permission(optional_yield y)
{
  const RGWUserInfo& info = user->get_info();
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "user", account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamGetUser, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWGetUser_IAM::execute(optional_yield y)
{
}

void RGWGetUser_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "GetUserResponse", RGW_REST_IAM_XMLNS};
    {
      Formatter::ObjectSection result{*f, "GetUserResult"};
      Formatter::ObjectSection User{*f, "User"};
      dump_iam_user(user->get_info(), f);
      // /User
      // /GetUserResult
    }
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /GetUserResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// UpdateUser
class RGWUpdateUser_IAM : public RGWOp {
  bufferlist post_body;
  std::string new_path;
  std::string new_username;
  std::unique_ptr<rgw::sal::User> user;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWUpdateUser_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "update_user"; }
  RGWOpType get_type() override { return RGW_OP_UPDATE_USER; }
};

int RGWUpdateUser_IAM::init_processing(optional_yield y)
{
  // use account id from authenticated user/role. with AssumeRole, this may not
  // match the account of s->user
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

  new_username = s->info.args.get("NewUserName");
  if (!new_username.empty() &&
      !validate_iam_user_name(new_username, s->err.message)) {
    return -EINVAL;
  }

  const std::string username = s->info.args.get("UserName");
  if (username.empty()) {
    s->err.message = "Missing required element UserName";
    return -EINVAL;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  int r = driver->load_account_user_by_name(this, y, account_id,
                                            tenant, username, &user);
  // root user is hidden from user apis
  const bool is_root = (user && user->get_type() == TYPE_ROOT);
  if (r == -ENOENT || is_root) {
    s->err.message = "No such UserName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWUpdateUser_IAM::verify_permission(optional_yield y)
{
  const RGWUserInfo& info = user->get_info();
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "user", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamUpdateUser, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWUpdateUser_IAM::forward_to_master(optional_yield y, const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("NewPath");
  s->info.args.remove("NewUserName");
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

void RGWUpdateUser_IAM::execute(optional_yield y)
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

        if (!new_path.empty()) {
          info.path = new_path;
        }
        if (!new_username.empty()) {
          info.display_name = new_username;
        }

        if (info.path == old_info.path &&
            info.display_name == old_info.display_name) {
          return 0; // no changes to write
        }

        constexpr bool exclusive = false;
        return user->store_user(this, y, exclusive, &old_info);
      });
}

void RGWUpdateUser_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "UpdateUserResponse", RGW_REST_IAM_XMLNS};
    {
      Formatter::ObjectSection result{*f, "UpdateUserResult"};
      Formatter::ObjectSection User{*f, "User"};
      dump_iam_user(user->get_info(), f);
      // /User
      // /UpdateUserResult
    }
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /UpdateUserResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// DeleteUser
class RGWDeleteUser_IAM : public RGWOp {
  bufferlist post_body;
  std::unique_ptr<rgw::sal::User> user;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
  int check_empty();
 public:
  explicit RGWDeleteUser_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "delete_user"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_USER; }
};

int RGWDeleteUser_IAM::init_processing(optional_yield y)
{
  // use account id from authenticated user/role. with AssumeRole, this may not
  // match the account of s->user
  rgw_account_id account_id;
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string username = s->info.args.get("UserName");
  if (username.empty()) {
    s->err.message = "Missing required element UserName";
    return -EINVAL;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  int r = driver->load_account_user_by_name(this, y, account_id,
                                            tenant, username, &user);
  // root user is hidden from user apis
  const bool is_root = (user && user->get_type() == TYPE_ROOT);
  if (r == -ENOENT || is_root) {
    s->err.message = "No such UserName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWDeleteUser_IAM::verify_permission(optional_yield y)
{
  const RGWUserInfo& info = user->get_info();
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "user", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamDeleteUser, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWDeleteUser_IAM::forward_to_master(optional_yield y, const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

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

int RGWDeleteUser_IAM::check_empty()
{
  if (!s->penv.site->is_meta_master()) {
    // only check on the master zone. if a forwarded DeleteUser request
    // succeeds on the master zone, it needs to succeed here too
    return 0;
  }

  // verify that all user resources are removed first
  const RGWUserInfo& info = user->get_info();
  if (!info.access_keys.empty()) {
    s->err.message = "The user cannot be deleted until its AccessKeys are removed";
    return -ERR_DELETE_CONFLICT;
  }

  const auto& attrs = user->get_attrs();
  if (auto p = attrs.find(RGW_ATTR_USER_POLICY); p != attrs.end()) {
    std::map<std::string, std::string> policies;
    try {
      decode(policies, p->second);
    } catch (const buffer::error&) {
      ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
      return -EIO;
    }

    if (!policies.empty()) {
      s->err.message = "The user cannot be deleted until all user policies are removed";
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
      s->err.message = "The user cannot be deleted until all managed policies are detached";
      return -ERR_DELETE_CONFLICT;
    }
  }

  return 0;
}

void RGWDeleteUser_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
  } else {
    op_ret = check_empty();
  }
  if (op_ret) {
    return;
  }

  op_ret = user->remove_user(this, y);

  if (op_ret == -ENOENT) {
    if (!site.is_meta_master()) {
      // delete succeeded on the master, return that success here too
      op_ret = 0;
    } else {
      s->err.message = "No such UserName in the account";
      op_ret = -ERR_NO_SUCH_ENTITY;
    }
  }
}

void RGWDeleteUser_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "DeleteUserResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /DeleteUserResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// ListUsers
class RGWListUsers_IAM : public RGWOp {
  rgw_account_id account_id;
  std::string marker;
  std::string path_prefix;
  int max_items = 100;

  bool started_response = false;
  void start_response();
  void end_response(std::string_view next_marker);
  void send_response_data(std::span<RGWUserInfo> users);
 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_users"; }
  RGWOpType get_type() override { return RGW_OP_LIST_USERS; }
};

int RGWListUsers_IAM::init_processing(optional_yield y)
{
  // use account id from authenticated user/role. with AssumeRole, this may not
  // match the account of s->user
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

int RGWListUsers_IAM::verify_permission(optional_yield y)
{
  const std::string resource_name = "";
  const rgw::ARN arn{resource_name, "user", account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamListUsers, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWListUsers_IAM::execute(optional_yield y)
{
  const std::string& tenant = s->auth.identity->get_tenant();

  rgw::sal::UserList listing;
  listing.next_marker = marker;

  op_ret = driver->list_account_users(this, y, account_id, tenant,
                                      path_prefix, listing.next_marker,
                                      max_items, listing);
  if (op_ret == -ENOENT) {
    op_ret = 0;
  } else if (op_ret < 0) {
    return;
  }

  send_response_data(listing.users);

  if (!started_response) {
    started_response = true;
    start_response();
  }
  end_response(listing.next_marker);
}

void RGWListUsers_IAM::start_response()
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
  s->formatter->open_object_section_in_ns("ListUsersResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ListUsersResult");
  s->formatter->open_array_section("Users");
}

void RGWListUsers_IAM::end_response(std::string_view next_marker)
{
  s->formatter->close_section(); // Users

  const bool truncated = !next_marker.empty();
  s->formatter->dump_bool("IsTruncated", truncated);
  if (truncated) {
    s->formatter->dump_string("Marker", next_marker);
  }

  s->formatter->close_section(); // ListUsersResult
  s->formatter->close_section(); // ListUsersResponse
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWListUsers_IAM::send_response_data(std::span<RGWUserInfo> users)
{
  if (!started_response) {
    started_response = true;
    start_response();
  }

  for (const auto& info : users) {
    if (info.type == TYPE_ROOT) {
      continue; // root user is hidden from user apis
    }
    s->formatter->open_object_section("member");
    dump_iam_user(info, s->formatter);
    s->formatter->close_section(); // member
  }

  // flush after each chunk
  rgw_flush_formatter(s, s->formatter);
}

void RGWListUsers_IAM::send_response()
{
  if (!started_response) { // errored out before execute() wrote anything
    start_response();
  }
}


void dump_access_key(const RGWAccessKey& key, Formatter* f)
{
  encode_json("AccessKeyId", key.id, f);
  encode_json("Status", key.active ? "Active" : "Inactive", f);
  encode_json("CreateDate", key.create_date, f);
}

// CreateAccessKey
class RGWCreateAccessKey_IAM : public RGWOp {
  bufferlist post_body;
  std::unique_ptr<rgw::sal::User> user;
  RGWAccessKey key;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site,
                        RGWAccessKey& cred);
 public:
  explicit RGWCreateAccessKey_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "create_access_key"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_ACCESS_KEY; }
};

int RGWCreateAccessKey_IAM::init_processing(optional_yield y)
{
  // use account id from authenticated user/role. with AssumeRole, this may not
  // match the account of s->user
  rgw_account_id account_id;
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  const std::string username = s->info.args.get("UserName");
  if (username.empty()) {
    // If you do not specify a user name, IAM determines the user name
    // implicitly based on the AWS access key ID signing the request.
    // This operation works for access keys under the AWS account.
    // Consequently, you can use this operation to manage AWS account
    // root user credentials.
    user = s->user->clone();
    return 0;
  }
  if (!validate_iam_user_name(username, s->err.message)) {
    return -EINVAL;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  int r = driver->load_account_user_by_name(this, y, account_id,
                                            tenant, username, &user);
  // root user is hidden from user apis
  const bool is_root = (user && user->get_type() == TYPE_ROOT);
  if (r == -ENOENT || is_root) {
    s->err.message = "No such UserName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWCreateAccessKey_IAM::verify_permission(optional_yield y)
{
  const RGWUserInfo& info = user->get_info();
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "user", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamCreateAccessKey, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWCreateAccessKey_IAM::forward_to_master(optional_yield y,
                                              const rgw::SiteConfig& site,
                                              RGWAccessKey& cred)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("UserName");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }

  XMLObj* response = parser.find_first("CreateAccessKeyResponse");;
  if (!response) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: CreateAccessKeyResponse" << dendl;
    return -EINVAL;
  }

  XMLObj* result = response->find_first("CreateAccessKeyResult");
  if (!result) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: CreateAccessKeyResult" << dendl;
    return -EINVAL;
  }

  XMLObj* access_key = result->find_first("AccessKey");
  if (!user) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: AccessKey" << dendl;
    return -EINVAL;
  }

  try {
    RGWXMLDecoder::decode_xml("AccessKeyId", cred.id, access_key, true);
    RGWXMLDecoder::decode_xml("SecretAccessKey", cred.key, access_key, true);
    RGWXMLDecoder::decode_xml("CreateDate", cred.create_date, access_key);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "ERROR: unexpected xml: AccessKey" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWCreateAccessKey_IAM::execute(optional_yield y)
{
  std::optional<int> max_keys;
  {
    // read account's access key limit
    RGWAccountInfo account;
    rgw::sal::Attrs attrs; // unused
    RGWObjVersionTracker objv; // unused
    op_ret = driver->load_account_by_id(this, y, user->get_info().account_id,
                                        account, attrs, objv);
    if (op_ret < 0) {
      ldpp_dout(this, 4) << "failed to load iam account "
          << user->get_info().account_id << ": " << cpp_strerror(op_ret) << dendl;
      return;
    }
    if (account.max_access_keys >= 0) { // max < 0 means unlimited
      max_keys = account.max_access_keys;
    }
  }

  // generate the key. forward_to_master() may overwrite this
  if (rgw_generate_access_key(this, y, driver, key.id) < 0) {
    s->err.message = "failed to generate s3 access key";
    op_ret = -ERR_INTERNAL_ERROR;
    return;
  }
  rgw_generate_secret_key(get_cct(), key.key);
  key.create_date = ceph::real_clock::now();

  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site, key);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_user_write(this, y, user.get(),
      [this, y, &max_keys] {
        RGWUserInfo& info = user->get_info();
        RGWUserInfo old_info = info;

        info.access_keys[key.id] = key;

        // check the current count against account limit
        if (max_keys && std::cmp_greater(info.access_keys.size(), *max_keys)) {
          s->err.message = fmt::format("Access key limit {} exceeded", *max_keys);
          return -ERR_LIMIT_EXCEEDED;
        }

        constexpr bool exclusive = false;
        return user->store_user(this, y, exclusive, &old_info);
      });
}

void RGWCreateAccessKey_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "CreateAccessKeyResponse", RGW_REST_IAM_XMLNS};
    {
      Formatter::ObjectSection result{*f, "CreateAccessKeyResult"};
      Formatter::ObjectSection accesskey{*f, "AccessKey"};
      encode_json("UserName", user->get_display_name(), f);
      dump_access_key(key, f);
      encode_json("SecretAccessKey", key.key, f);
      // /AccessKey
      // /CreateAccessKeyResult
    }
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /CreateAccessKeyResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// UpdateAccessKey
class RGWUpdateAccessKey_IAM : public RGWOp {
  bufferlist post_body;
  std::string access_key_id;
  bool new_status = false;
  std::unique_ptr<rgw::sal::User> user;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWUpdateAccessKey_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "update_access_key"; }
  RGWOpType get_type() override { return RGW_OP_UPDATE_ACCESS_KEY; }
};

int RGWUpdateAccessKey_IAM::init_processing(optional_yield y)
{
  // use account id from authenticated user/role. with AssumeRole, this may not
  // match the account of s->user
  rgw_account_id account_id;
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  access_key_id = s->info.args.get("AccessKeyId");
  if (access_key_id.empty()) {
    s->err.message = "Missing required element AccessKeyId";
    return -EINVAL;
  }

  const std::string status = s->info.args.get("Status");
  if (status == "Active") {
    new_status = true;
  } else if (status == "Inactive") {
    new_status = false;
  } else {
    if (status.empty()) {
      s->err.message = "Missing required element Status";
    } else {
      s->err.message = "Invalid value for Status";
    }
    return -EINVAL;
  }

  const std::string username = s->info.args.get("UserName");
  if (username.empty()) {
    // If you do not specify a user name, IAM determines the user name
    // implicitly based on the AWS access key ID signing the request.
    // This operation works for access keys under the AWS account.
    // Consequently, you can use this operation to manage AWS account
    // root user credentials.
    user = s->user->clone();
    return 0;
  }
  if (!validate_iam_user_name(username, s->err.message)) {
    return -EINVAL;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  int r = driver->load_account_user_by_name(this, y, account_id,
                                            tenant, username, &user);
  // root user is hidden from user apis
  const bool is_root = (user && user->get_type() == TYPE_ROOT);
  if (r == -ENOENT || is_root) {
    s->err.message = "No such UserName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWUpdateAccessKey_IAM::verify_permission(optional_yield y)
{
  const RGWUserInfo& info = user->get_info();
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "user", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamUpdateAccessKey, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWUpdateAccessKey_IAM::forward_to_master(optional_yield y,
                                              const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("AccessKeyId");
  s->info.args.remove("Status");
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

void RGWUpdateAccessKey_IAM::execute(optional_yield y)
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

        auto key = info.access_keys.find(access_key_id);
        if (key == info.access_keys.end()) {
          s->err.message = "No such AccessKeyId in the user";
          return -ERR_NO_SUCH_ENTITY;
        }

        if (key->second.active == new_status) {
          return 0; // nothing to do, return success
        }

        key->second.active = new_status;

        constexpr bool exclusive = false;
        return user->store_user(this, y, exclusive, &old_info);
      });
}

void RGWUpdateAccessKey_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "UpdateAccessKeyResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /UpdateAccessKeyResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}

// DeleteAccessKey
class RGWDeleteAccessKey_IAM : public RGWOp {
  bufferlist post_body;
  std::string access_key_id;
  std::unique_ptr<rgw::sal::User> user;

  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWDeleteAccessKey_IAM(const ceph::bufferlist& post_body)
    : post_body(post_body) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "delete_access_key"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_ACCESS_KEY; }
};

int RGWDeleteAccessKey_IAM::init_processing(optional_yield y)
{
  // use account id from authenticated user/role. with AssumeRole, this may not
  // match the account of s->user
  rgw_account_id account_id;
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }

  access_key_id = s->info.args.get("AccessKeyId");
  if (access_key_id.empty()) {
    s->err.message = "Missing required element AccessKeyId";
    return -EINVAL;
  }

  const std::string username = s->info.args.get("UserName");
  if (username.empty()) {
    // If you do not specify a user name, IAM determines the user name
    // implicitly based on the AWS access key ID signing the request.
    // This operation works for access keys under the AWS account.
    // Consequently, you can use this operation to manage AWS account
    // root user credentials.
    user = s->user->clone();
    return 0;
  }
  if (!validate_iam_user_name(username, s->err.message)) {
    return -EINVAL;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  int r = driver->load_account_user_by_name(this, y, account_id,
                                            tenant, username, &user);
  // root user is hidden from user apis
  const bool is_root = (user && user->get_type() == TYPE_ROOT);
  if (r == -ENOENT || is_root) {
    s->err.message = "No such UserName in the account";
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWDeleteAccessKey_IAM::verify_permission(optional_yield y)
{
  const RGWUserInfo& info = user->get_info();
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "user", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamDeleteAccessKey, true)) {
    return 0;
  }
  return -EACCES;
}

int RGWDeleteAccessKey_IAM::forward_to_master(optional_yield y,
                                              const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("AccessKeyId");
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

void RGWDeleteAccessKey_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_user_write(this, y, user.get(),
      [this, y, &site] {
        RGWUserInfo& info = user->get_info();
        RGWUserInfo old_info = info;

        auto key = info.access_keys.find(access_key_id);
        if (key == info.access_keys.end()) {
          if (!site.is_meta_master()) {
            return 0; // delete succeeded on the master
          }
          s->err.message = "No such AccessKeyId in the user";
          return -ERR_NO_SUCH_ENTITY;
        }

        info.access_keys.erase(key);

        constexpr bool exclusive = false;
        return user->store_user(this, y, exclusive, &old_info);
      });
}

void RGWDeleteAccessKey_IAM::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    Formatter* f = s->formatter;
    Formatter::ObjectSection response{*f, "DeleteAccessKeyResponse", RGW_REST_IAM_XMLNS};
    Formatter::ObjectSection metadata{*f, "ResponseMetadata"};
    f->dump_string("RequestId", s->trans_id);
    // /ResponseMetadata
    // /DeleteAccessKeyResponse
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


// ListAccessKeys
class RGWListAccessKeys_IAM : public RGWOp {
  std::unique_ptr<rgw::sal::User> user;
  std::string marker;
  int max_items = 100;

  bool started_response = false;
  void start_response();
 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_access_keys"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ACCESS_KEYS; }
};

int RGWListAccessKeys_IAM::init_processing(optional_yield y)
{
  // use account id from authenticated user/role. with AssumeRole, this may not
  // match the account of s->user
  rgw_account_id account_id;
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
  if (username.empty()) {
    // If you do not specify a user name, IAM determines the user name
    // implicitly based on the AWS access key ID signing the request.
    // This operation works for access keys under the AWS account.
    // Consequently, you can use this operation to manage AWS account
    // root user credentials.
    user = s->user->clone();
    return 0;
  }
  if (!validate_iam_user_name(username, s->err.message)) {
    return -EINVAL;
  }

  // look up user by UserName
  const std::string& tenant = s->auth.identity->get_tenant();
  r = driver->load_account_user_by_name(this, y, account_id,
                                        tenant, username, &user);
  // root user is hidden from user apis
  const bool is_root = (user && user->get_type() == TYPE_ROOT);
  if (r == -ENOENT || is_root) {
    return -ERR_NO_SUCH_ENTITY;
  }
  return r;
}

int RGWListAccessKeys_IAM::verify_permission(optional_yield y)
{
  const RGWUserInfo& info = user->get_info();
  const std::string resource_name = make_resource_name(info);
  const rgw::ARN arn{resource_name, "user", info.account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamListAccessKeys, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWListAccessKeys_IAM::execute(optional_yield y)
{
  start_response();
  started_response = true;

  dump_start(s); // <?xml block ?>

  Formatter* f = s->formatter;
  f->open_object_section_in_ns("ListAccessKeysResponse", RGW_REST_IAM_XMLNS);
  f->open_object_section("ListAccessKeysResult");
  encode_json("UserName", user->get_display_name(), f);
  f->open_array_section("AccessKeyMetadata");

  const RGWUserInfo& info = user->get_info();

  auto key = info.access_keys.lower_bound(marker);
  for (int i = 0; i < max_items && key != info.access_keys.end(); ++i, ++key) {
    f->open_object_section("member");
    encode_json("UserName", user->get_display_name(), f);
    dump_access_key(key->second, f);
    f->close_section(); // member
  }

  f->close_section(); // AccessKeyMetadata

  const bool truncated = (key != info.access_keys.end());
  f->dump_bool("IsTruncated", truncated);
  if (truncated) {
    f->dump_string("Marker", key->second.id);
  }

  f->close_section(); // ListAccessKeysResult
  f->close_section(); // ListAccessKeysResponse
  rgw_flush_formatter_and_reset(s, f);
}

void RGWListAccessKeys_IAM::start_response()
{
  const int64_t proposed_content_length =
      op_ret ? NO_CONTENT_LENGTH : CHUNKED_TRANSFER_ENCODING;

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format), proposed_content_length);
}

void RGWListAccessKeys_IAM::send_response()
{
  if (!started_response) { // errored out before execute() wrote anything
    start_response();
  }
}


RGWOp* make_iam_create_user_op(const ceph::bufferlist& post_body) {
  return new RGWCreateUser_IAM(post_body);
}
RGWOp* make_iam_get_user_op(const ceph::bufferlist&) {
  return new RGWGetUser_IAM;
}
RGWOp* make_iam_update_user_op(const ceph::bufferlist& post_body) {
  return new RGWUpdateUser_IAM(post_body);
}
RGWOp* make_iam_delete_user_op(const ceph::bufferlist& post_body) {
  return new RGWDeleteUser_IAM(post_body);
}
RGWOp* make_iam_list_users_op(const ceph::bufferlist&) {
  return new RGWListUsers_IAM;
}

RGWOp* make_iam_create_access_key_op(const ceph::bufferlist& post_body) {
  return new RGWCreateAccessKey_IAM(post_body);
}
RGWOp* make_iam_update_access_key_op(const ceph::bufferlist& post_body) {
  return new RGWUpdateAccessKey_IAM(post_body);
}
RGWOp* make_iam_delete_access_key_op(const ceph::bufferlist& post_body) {
  return new RGWDeleteAccessKey_IAM(post_body);
}
RGWOp* make_iam_list_access_keys_op(const ceph::bufferlist& unused) {
  return new RGWListAccessKeys_IAM;
}
