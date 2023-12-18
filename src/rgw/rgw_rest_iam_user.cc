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
#include "rgw_op.h"
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
  RGWUserInfo info;
 public:
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
  if (const auto* id = std::get_if<rgw_account_id>(&s->owner.id); id) {
    info.account_id = *id;
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

  // generate user id
  uuid_d uuid;
  uuid.generate_random();
  info.user_id.id = uuid.to_string();
  info.user_id.tenant = s->auth.identity->get_tenant();

  info.create_date = ceph::real_clock::now();

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
  if (const auto* id = std::get_if<rgw_account_id>(&s->owner.id); id) {
    account_id = *id;
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
  if (r == -ENOENT) {
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
  std::string new_path;
  std::string new_username;
  std::unique_ptr<rgw::sal::User> user;
 public:
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
  if (const auto* id = std::get_if<rgw_account_id>(&s->owner.id); id) {
    account_id = *id;
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
  if (r == -ENOENT) {
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

void RGWUpdateUser_IAM::execute(optional_yield y)
{
  RGWUserInfo& info = user->get_info();
  RGWUserInfo old_info = info;

  if (!new_path.empty()) {
    info.path = new_path;
  }
  if (!new_username.empty()) {
    info.display_name = new_username;
  }

  constexpr bool exclusive = false;
  op_ret = user->store_user(this, y, exclusive, &old_info);
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
  std::unique_ptr<rgw::sal::User> user;
 public:
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
  if (const auto* id = std::get_if<rgw_account_id>(&s->owner.id); id) {
    account_id = *id;
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
  if (r == -ENOENT) {
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

void RGWDeleteUser_IAM::execute(optional_yield y)
{
  // verify that all user resources are removed first
  const RGWUserInfo& info = user->get_info();
  if (!info.access_keys.empty()) {
    s->err.message = "The user cannot be deleted until its AccessKeys are removed";
    op_ret = -ERR_DELETE_CONFLICT;
    return;
  }

  const auto& attrs = user->get_attrs();
  if (auto p = attrs.find(RGW_ATTR_USER_POLICY); p != attrs.end()) {
    std::map<std::string, std::string> policies;
    try {
      decode(policies, p->second);
    } catch (const buffer::error&) {
      ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
      op_ret = -EIO;
      return;
    }

    if (!policies.empty()) {
      s->err.message = "The user cannot be deleted until all user policies are removed";
      op_ret = -ERR_DELETE_CONFLICT;
      return;
    }
  }

  op_ret = user->remove_user(this, y);
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
  if (const auto* id = std::get_if<rgw_account_id>(&s->owner.id); id) {
    account_id = *id;
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


RGWOp* make_iam_create_user_op(const ceph::bufferlist&) {
  return new RGWCreateUser_IAM;
}
RGWOp* make_iam_get_user_op(const ceph::bufferlist&) {
  return new RGWGetUser_IAM;
}
RGWOp* make_iam_update_user_op(const ceph::bufferlist&) {
  return new RGWUpdateUser_IAM;
}
RGWOp* make_iam_delete_user_op(const ceph::bufferlist&) {
  return new RGWDeleteUser_IAM;
}
RGWOp* make_iam_list_users_op(const ceph::bufferlist&) {
  return new RGWListUsers_IAM;
}
