// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "rgw_arn.h"
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
  bufferlist bl_post_body;
  RGWOIDCProviderInfo info;
 public:
  explicit
  RGWCreateOIDCProvider(const bufferlist& bl_post_body)
    : RGWRestOIDCProvider(rgw::IAM::iamCreateOIDCProvider, RGW_CAP_WRITE),
      bl_post_body(bl_post_body)
  {
  }

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "create_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_CREATE_OIDC_PROVIDER; }
};

class RGWDeleteOIDCProvider : public RGWRestOIDCProvider {
  bufferlist bl_post_body;
  std::string url;
 public:
  explicit
  RGWDeleteOIDCProvider(const bufferlist& bl_post_body)
    : RGWRestOIDCProvider(rgw::IAM::iamDeleteOIDCProvider, RGW_CAP_WRITE),
      bl_post_body(bl_post_body)
  {
  }

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "delete_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_OIDC_PROVIDER; }
};

class RGWGetOIDCProvider : public RGWRestOIDCProvider {
  std::string url;

public:
  RGWGetOIDCProvider()
    : RGWRestOIDCProvider(rgw::IAM::iamGetOIDCProvider, RGW_CAP_READ)
  {
  }

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_GET_OIDC_PROVIDER; }
};

class RGWListOIDCProviders : public RGWRestOIDCProvider {
public:
  RGWListOIDCProviders()
    : RGWRestOIDCProvider(rgw::IAM::iamListOIDCProviders, RGW_CAP_READ) {}

  void execute(optional_yield y) override;
  const char* name() const override { return "list_oidc_providers"; }
  RGWOpType get_type() override { return RGW_OP_LIST_OIDC_PROVIDERS; }
};

class RGWAddClientIdToOIDCProvider : public RGWRestOIDCProvider {
  bufferlist bl_post_body;
  std::string url;
  std::string client_id;

public:
  explicit
  RGWAddClientIdToOIDCProvider(const bufferlist& bl_post_body)
    : RGWRestOIDCProvider(
          rgw::IAM::iamAddClientIdToOIDCProvider, RGW_CAP_WRITE),
      bl_post_body(bl_post_body)
  {
  }

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "add_client_id_to_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_ADD_CLIENTID_TO_OIDC_PROVIDER; }
};

class RGWRemoveClientIdFromOIDCProvider : public RGWRestOIDCProvider {
  bufferlist bl_post_body;
  std::string url;
  std::string client_id;

public:
  explicit
  RGWRemoveClientIdFromOIDCProvider(const bufferlist& bl_post_body)
    : RGWRestOIDCProvider(
          rgw::IAM::iamRemoveClientIdFromOIDCProvider, RGW_CAP_WRITE),
      bl_post_body(bl_post_body)
  {
  }

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "remove_client_id_from_oidc_provider"; }
  RGWOpType get_type() override { return RGW_OP_REMOVE_CLIENTID_FROM_OIDC_PROVIDER; }
};

class RGWUpdateOIDCProviderThumbprint : public RGWRestOIDCProvider {
  bufferlist bl_post_body;
  std::string url;
  std::vector<std::string> thumbprints;

public:
  explicit
  RGWUpdateOIDCProviderThumbprint(const bufferlist& bl_post_body)
    : RGWRestOIDCProvider(
          rgw::IAM::iamUpdateOIDCProviderThumbprint, RGW_CAP_WRITE),
      bl_post_body(bl_post_body)
  {
  }

  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "update_oidc_provider_thumbprint"; }
  RGWOpType get_type() override { return RGW_OP_UPDATE_OIDC_PROVIDER_THUMBPRINT; }
};
