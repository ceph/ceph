// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_oidc_provider.h"

class RGWRestOIDCProvider : public RGWRESTOp {
  const uint64_t action;
  const uint32_t perm;
protected:
  rgw::ARN resource; // must be initialized before verify_permission()

  int check_caps(const RGWUserCaps& caps) override;

  RGWRestOIDCProvider(uint64_t action, uint32_t perm)
    : action(action), perm(perm) {}
public:
  int verify_permission(optional_yield y) override;
  void send_response() override;
};

class RGWCreateOIDCProvider : public RGWRestOIDCProvider {
  std::vector<std::string> client_ids;
  std::vector<std::string> thumbprints;
  std::string provider_url; //'iss' field in JWT
 public:
  RGWCreateOIDCProvider();

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "create_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_OIDC_PROVIDER; }
};

class RGWDeleteOIDCProvider : public RGWRestOIDCProvider {
  std::string provider_arn;
 public:
  RGWDeleteOIDCProvider();

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "delete_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_OIDC_PROVIDER; }
};

class RGWGetOIDCProvider : public RGWRestOIDCProvider {
  std::string provider_arn;
 public:
  RGWGetOIDCProvider();

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_GET_OIDC_PROVIDER; }
};

class RGWListOIDCProviders : public RGWRestOIDCProvider {
 public:
  RGWListOIDCProviders();

  void execute(optional_yield y) override;
  const char* name() const override { return "list_oidc_providers"; }
  RGWOpType get_type() override { return RGW_OP_LIST_OIDC_PROVIDERS; }
};
