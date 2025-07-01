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

#include "rgw_rest_s3control.h"

#include <concepts>

#include "rgw_auth_s3.h"
#include "rgw_process_env.h"
#include "common/errno.h"

template <std::invocable<> F>
int retry_raced_account_write(const DoutPrefixProvider* dpp, optional_yield y,
                              rgw::sal::Driver* driver, RGWAccountInfo& info,
                              RGWObjVersionTracker& objv, const F& f)
{
  int r = f();
  for (int i = 0; i < 10 && r == -ECANCELED; ++i) {
    objv.clear();
    r = driver->load_account_by_id(dpp, y, info.id, info, objv);
    if (r >= 0) {
      r = f();
    }
  }
  return r;
}

static int get_account_id(req_state* s, rgw_account_id& account_id)
{
  auto id = s->info.x_meta_map.find("x-amz-account-id");
  if (id == s->info.x_meta_map.end()) {
    s->err.message = "Missing required header x-amz-account-id";
    return -EINVAL;
  }
  account_id = id->second;
  if (account_id.empty()) {
    s->err.message = "Missing required value for x-amz-account-id";
    return -EINVAL;
  }

  const auto& account = s->auth.identity->get_account();
  if (!account) {
    return -ERR_METHOD_NOT_ALLOWED;
  }
  if (account_id != account->id) {
    s->err.message = "x-amz-account-id must match the requester";
    return -EINVAL;
  }
  return 0;
}

// GetPublicAccessBlock
class RGWGetPublicAccessBlock_S3Control : public RGWOp {
  rgw_account_id account_id;
  PublicAccessBlockConfiguration public_access_block;

 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "get_public_access_block"; }
  RGWOpType get_type() override { return RGW_OP_GET_PUBLIC_ACCESS_BLOCK; }
};

int RGWGetPublicAccessBlock_S3Control::init_processing(optional_yield y)
{
  return get_account_id(s, account_id);
}

int RGWGetPublicAccessBlock_S3Control::verify_permission(optional_yield y)
{
  const rgw::ARN arn{"", "root", account_id, false}; // XXX
  if (verify_user_permission(this, s, arn, rgw::IAM::s3GetAccountPublicAccessBlock, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWGetPublicAccessBlock_S3Control::execute(optional_yield y)
{
  RGWAccountInfo account;
  RGWObjVersionTracker objv;
  op_ret = driver->load_account_by_id(this, y, account_id, account, objv);
  if (op_ret < 0) {
    ldpp_dout(this, 4) << "failed to load account id "
        << account_id << ": " << cpp_strerror(op_ret) << dendl;
    return;
  }

  auto i = account.attrs.find(RGW_ATTR_PUBLIC_ACCESS);
  if (i == account.attrs.end()) {
    op_ret = -ERR_NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION;
    return;
  }

  try {
    auto p = i->second.cbegin();
    decode(public_access_block, p);
  } catch (const buffer::error&) {
    op_ret = -EIO;
    return;
  }
}

void RGWGetPublicAccessBlock_S3Control::send_response()
{
  if (!op_ret) {
    dump_start(s); // <?xml block ?>
    public_access_block.dump_xml(s->formatter);
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}

// PutPublicAccessBlock
class RGWPutPublicAccessBlock_S3Control : public RGWOp {
  rgw_account_id account_id;
  bufferlist data;
  PublicAccessBlockConfiguration public_access_block;

 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "put_public_access_block"; }
  RGWOpType get_type() override { return RGW_OP_PUT_PUBLIC_ACCESS_BLOCK; }
};

int RGWPutPublicAccessBlock_S3Control::init_processing(optional_yield y)
{
  int ret = get_account_id(s, account_id);
  if (ret < 0) {
    return ret;
  }

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(ret, data) = read_all_input(s, max_size, false);
  if (ret < 0) {
    return ret;
  }

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    return -EINVAL;
  }
  if (!parser.parse(data.c_str(), data.length(), 1)) {
    return -ERR_MALFORMED_XML;
  }
  try {
    RGWXMLDecoder::decode_xml("PublicAccessBlockConfiguration",
                              public_access_block, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    return -ERR_MALFORMED_XML;
  }
  return 0;
}

int RGWPutPublicAccessBlock_S3Control::verify_permission(optional_yield y)
{
  const rgw::ARN arn{"", "root", account_id, false}; // XXX
  if (verify_user_permission(this, s, arn, rgw::IAM::s3PutAccountPublicAccessBlock, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWPutPublicAccessBlock_S3Control::execute(optional_yield y)
{
  RGWAccountInfo account;
  RGWObjVersionTracker objv;
  op_ret = driver->load_account_by_id(this, y, account_id, account, objv);
  if (op_ret < 0) {
    ldpp_dout(this, 4) << "failed to load account id "
        << account_id << ": " << cpp_strerror(op_ret) << dendl;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                         &data, nullptr, s->info, s->err, y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  bufferlist conf_bl;
  encode(public_access_block, conf_bl);

  op_ret = retry_raced_account_write(this, y, driver, account, objv,
      [this, y, &conf_bl, &account, &objv] {
        const RGWAccountInfo old_info = account;
        account.attrs[RGW_ATTR_PUBLIC_ACCESS] = conf_bl;

        constexpr bool exclusive = false;
        return driver->store_account(this, y, exclusive, account,
                                     &old_info, objv);
      });
}

void RGWPutPublicAccessBlock_S3Control::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}

// DeletePublicAccessBlock
class RGWDeletePublicAccessBlock_S3Control : public RGWOp {
  rgw_account_id account_id;

 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "delete_public_access_block"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_PUBLIC_ACCESS_BLOCK; }
};

int RGWDeletePublicAccessBlock_S3Control::init_processing(optional_yield y)
{
  return get_account_id(s, account_id);
}

int RGWDeletePublicAccessBlock_S3Control::verify_permission(optional_yield y)
{
  const rgw::ARN arn{"", "root", account_id, false}; // XXX
  if (verify_user_permission(this, s, arn, rgw::IAM::s3PutAccountPublicAccessBlock, true)) {
    return 0;
  }
  return -EACCES;
}

void RGWDeletePublicAccessBlock_S3Control::execute(optional_yield y)
{
  RGWAccountInfo account;
  RGWObjVersionTracker objv;
  op_ret = driver->load_account_by_id(this, y, account_id, account, objv);
  if (op_ret < 0) {
    ldpp_dout(this, 4) << "failed to load account id "
        << account_id << ": " << cpp_strerror(op_ret) << dendl;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                         nullptr, nullptr, s->info, s->err, y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_account_write(this, y, driver, account, objv,
      [this, y, &account, &objv] {
        const RGWAccountInfo old_info = account;

        auto i = account.attrs.find(RGW_ATTR_PUBLIC_ACCESS);
        if (i == account.attrs.end()) {
          return 0;
        }
        account.attrs.erase(i);

        constexpr bool exclusive = false;
        return driver->store_account(this, y, exclusive, account,
                                     &old_info, objv);
      });
}

void RGWDeletePublicAccessBlock_S3Control::send_response()
{
  if (!op_ret) {
    op_ret = STATUS_NO_CONTENT;
  }
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}

class RGWHandler_PublicAccessBlock : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;

  RGWOp* op_delete() override { return new RGWDeletePublicAccessBlock_S3Control; }
  RGWOp* op_get() override { return new RGWGetPublicAccessBlock_S3Control; }
  RGWOp* op_put() override { return new RGWPutPublicAccessBlock_S3Control; }
public:
  RGWHandler_PublicAccessBlock(const rgw::auth::StrategyRegistry& auth_registry)
    : auth_registry(auth_registry)
  {}

  int init(rgw::sal::Driver* driver, req_state *s, rgw::io::BasicClient *cio) {
    s->dialect = "s3control";
    s->prot_flags = RGW_REST_S3CONTROL;
    int r = RGWHandler_REST::allocate_formatter(s, RGWFormat::XML, true);
    if (r < 0) {
      return r;
    }
    return RGWHandler_REST::init(driver, s, cio);
  }
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override {
    return RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
  }
  int postauth_init(optional_yield y) override { return 0; }
};

class RGWRESTMgr_S3Control_PublicAccessBlock : public RGWRESTMgr {
 public:
  RGWHandler_REST* get_handler(rgw::sal::Driver* driver, req_state* s,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& prefix) {
    return new RGWHandler_PublicAccessBlock(auth_registry);
  }
};

RGWRESTMgr_S3Control::RGWRESTMgr_S3Control()
{
  // register nested resources
  auto cfg = std::make_unique<RGWRESTMgr>();
  cfg->register_resource("publicAccessBlock",
                         new RGWRESTMgr_S3Control_PublicAccessBlock);
  register_resource("configuration", cfg.release());
}
