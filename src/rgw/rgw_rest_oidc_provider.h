// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_oidc_provider.h"

class RGWRestOIDCProvider : public RGWRESTOp {
protected:
  vector<string> client_ids;
  vector<string> thumbprints;
  string provider_url; //'iss' field in JWT
  string provider_arn;
public:
  int verify_permission() override;
  void send_response() override;
  virtual uint64_t get_op() = 0;
};

class RGWRestOIDCProviderRead : public RGWRestOIDCProvider {
public:
  RGWRestOIDCProviderRead() = default;
  int check_caps(const RGWUserCaps& caps) override;
};

class RGWRestOIDCProviderWrite : public RGWRestOIDCProvider {
public:
  RGWRestOIDCProviderWrite() = default;
  int check_caps(const RGWUserCaps& caps) override;
};

class RGWCreateOIDCProvider : public RGWRestOIDCProviderWrite {
public:
  RGWCreateOIDCProvider() = default;
  int verify_permission() override;
  void execute() override;
  int get_params();
  const char* name() const override { return "create_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_OIDC_PROVIDER; }
  uint64_t get_op() { return rgw::IAM::iamCreateOIDCProvider; }
};

class RGWDeleteOIDCProvider : public RGWRestOIDCProviderWrite {
public:
  RGWDeleteOIDCProvider() = default;
  void execute() override;
  const char* name() const override { return "delete_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_OIDC_PROVIDER; }
  uint64_t get_op() { return rgw::IAM::iamDeleteOIDCProvider; }
};

class RGWGetOIDCProvider : public RGWRestOIDCProviderRead {
public:
  RGWGetOIDCProvider() = default;
  void execute() override;
  const char* name() const override { return "get_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_GET_OIDC_PROVIDER; }
  uint64_t get_op() { return rgw::IAM::iamGetOIDCProvider; }
};

class RGWListOIDCProviders : public RGWRestOIDCProviderRead {
public:
  RGWListOIDCProviders() = default;
  int verify_permission() override;
  void execute() override;
  int get_params();
  const char* name() const override { return "list_oidc_providers"; }
  RGWOpType get_type() override { return RGW_OP_LIST_OIDC_PROVIDERS; }
  uint64_t get_op() { return rgw::IAM::iamListOIDCProviders; }
};
