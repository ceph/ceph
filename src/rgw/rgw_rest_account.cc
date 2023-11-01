// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rest_account.h"
#include "rgw_account.h"
#include "rgw_process_env.h"

class RGWOp_Account_Create : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("accounts", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "create_account"; }
};

void RGWOp_Account_Create::execute(optional_yield y)
{
  rgw::account::AdminOpState op_state;
  RESTArgs::get_string(s, "id", "", &op_state.account_id);
  RESTArgs::get_string(s, "tenant", "", &op_state.tenant);
  RESTArgs::get_string(s, "name", "", &op_state.account_name);
  RESTArgs::get_string(s, "email", "", &op_state.email);

  uint32_t max_users = 0;
  bool has_max_users = false;
  RESTArgs::get_uint32(s, "max-users", 0, &max_users, &has_max_users);
  if (has_max_users) {
    op_state.max_users = max_users;
  }

  uint32_t max_roles = 0;
  bool has_max_roles = false;
  RESTArgs::get_uint32(s, "max-roles", 0, &max_roles, &has_max_roles);
  if (has_max_roles) {
    op_state.max_roles = max_roles;
  }

  uint32_t max_groups = 0;
  bool has_max_groups = false;
  RESTArgs::get_uint32(s, "max-groups", 0, &max_groups, &has_max_groups);
  if (has_max_groups) {
    op_state.max_groups = max_groups;
  }

  uint32_t max_access_keys = 0;
  bool has_max_access_keys = false;
  RESTArgs::get_uint32(s, "max-access-keys", 0, &max_access_keys, &has_max_access_keys);
  if (has_max_access_keys) {
    op_state.max_access_keys = max_access_keys;
  }

  uint32_t max_buckets = 0;
  bool has_max_buckets = false;
  RESTArgs::get_uint32(s, "max-buckets", 0, &max_buckets, &has_max_buckets);
  if (has_max_buckets) {
    op_state.max_buckets = max_buckets;
  }

  if (!driver->is_meta_master()) {
    bufferlist data;
    JSONParser parser;
    op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                           &data, &parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }

    // the master zone may have generated its own account id, use the same
    std::string meta_master_id;
    JSONDecoder::decode_json("id", meta_master_id, &parser);
    if (meta_master_id.empty()) {
      ldpp_dout(this, 4) << "forward_request_to_master returned empty account id" << dendl;
      op_ret = -EINVAL;
      return;
    }
    op_state.account_id = meta_master_id;
  }

  op_ret = rgw::account::create(this, driver, op_state,
                                s->err.message, flusher, y);
  if (op_ret < 0) {
    if (op_ret == -EEXIST) {
      op_ret = -ERR_ACCOUNT_EXISTS;
    }
  }
}

class RGWOp_Account_Modify : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("accounts", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "modify_account"; }
};

void RGWOp_Account_Modify::execute(optional_yield y)
{
  bufferlist data;
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  rgw::account::AdminOpState op_state;
  RESTArgs::get_string(s, "id", "", &op_state.account_id);
  RESTArgs::get_string(s, "tenant", "", &op_state.tenant);
  RESTArgs::get_string(s, "name", "", &op_state.account_name);
  RESTArgs::get_string(s, "email", "", &op_state.email);

  uint32_t max_users = 0;
  bool has_max_users = false;
  RESTArgs::get_uint32(s, "max-users", 0, &max_users, &has_max_users);
  if (has_max_users) {
    op_state.max_users = max_users;
  }

  uint32_t max_roles = 0;
  bool has_max_roles = false;
  RESTArgs::get_uint32(s, "max-roles", 0, &max_roles, &has_max_roles);
  if (has_max_roles) {
    op_state.max_roles = max_roles;
  }

  uint32_t max_groups = 0;
  bool has_max_groups = false;
  RESTArgs::get_uint32(s, "max-groups", 0, &max_groups, &has_max_groups);
  if (has_max_groups) {
    op_state.max_groups = max_groups;
  }

  uint32_t max_access_keys = 0;
  bool has_max_access_keys = false;
  RESTArgs::get_uint32(s, "max-access-keys", 0, &max_access_keys, &has_max_access_keys);
  if (has_max_access_keys) {
    op_state.max_access_keys = max_access_keys;
  }

  uint32_t max_buckets = 0;
  bool has_max_buckets = false;
  RESTArgs::get_uint32(s, "max-buckets", 0, &max_buckets, &has_max_buckets);
  if (has_max_buckets) {
    op_state.max_buckets = max_buckets;
  }

  op_ret = rgw::account::modify(this, driver, op_state,
                                s->err.message, flusher, y);
}


class RGWOp_Account_Get : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("account", RGW_CAP_READ);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "get_account"; }
};

void RGWOp_Account_Get::execute(optional_yield y)
{
  rgw::account::AdminOpState op_state;
  RESTArgs::get_string(s, "id", "", &op_state.account_id);
  RESTArgs::get_string(s, "tenant", "", &op_state.tenant);
  RESTArgs::get_string(s, "name", "", &op_state.account_name);

  op_ret = rgw::account::info(this, driver, op_state,
                              s->err.message, flusher, y);
}

class RGWOp_Account_Delete : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("account", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "delete_account"; }
};

void RGWOp_Account_Delete::execute(optional_yield y)
{
  bufferlist data;
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  rgw::account::AdminOpState op_state;
  RESTArgs::get_string(s, "id", "", &op_state.account_id);
  RESTArgs::get_string(s, "tenant", "", &op_state.tenant);
  RESTArgs::get_string(s, "name", "", &op_state.account_name);

  op_ret = rgw::account::remove(this, driver, op_state,
                                s->err.message, flusher, y);
}

RGWOp* RGWHandler_Account::op_post()
{
  return new RGWOp_Account_Create;
}

RGWOp* RGWHandler_Account::op_put()
{
  return new RGWOp_Account_Modify;
}

RGWOp* RGWHandler_Account::op_get()
{
  return new RGWOp_Account_Get;
}

RGWOp* RGWHandler_Account::op_delete()
{
  return new RGWOp_Account_Delete;
}
