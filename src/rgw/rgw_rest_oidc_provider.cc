// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_role.h"
#include "rgw_rest_oidc_provider.h"
#include "rgw_oidc_provider.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

int RGWRestOIDCProvider::verify_permission(optional_yield y)
{
  if (verify_user_permission(this, s, resource, action)) {
    return 0;
  }

  return RGWRESTOp::verify_permission(y);
}

int RGWRestOIDCProvider::check_caps(const RGWUserCaps& caps)
{
  return caps.check_cap("roles", perm);
}

void RGWRestOIDCProvider::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this);
}


RGWCreateOIDCProvider::RGWCreateOIDCProvider()
  : RGWRestOIDCProvider(rgw::IAM::iamCreateOIDCProvider, RGW_CAP_WRITE)
{
}

int RGWCreateOIDCProvider::init_processing(optional_yield y)
{
  provider_url = s->info.args.get("Url");
  if (provider_url.empty()) {
    s->err.message = "Missing required element Url";
    return -EINVAL;
  }

  auto val_map = s->info.args.get_params();
  for (auto& it : val_map) {
      if (it.first.find("ClientIDList.member.") != string::npos) {
          client_ids.emplace_back(it.second);
      }
      if (it.first.find("ThumbprintList.member.") != string::npos) {
          thumbprints.emplace_back(it.second);
      }
  }

  if (thumbprints.empty()) {
    s->err.message = "Missing required element ThumbprintList";
    return -EINVAL;
  }

  string idp_url = url_remove_prefix(provider_url);
  resource = rgw::ARN(idp_url, "oidc-provider/", s->user->get_tenant(), true);
  return 0;
}

void RGWCreateOIDCProvider::execute(optional_yield y)
{
  std::unique_ptr<rgw::sal::RGWOIDCProvider> provider = driver->get_oidc_provider();
  provider->set_url(provider_url);
  provider->set_tenant(s->user->get_tenant());
  provider->set_client_ids(client_ids);
  provider->set_thumbprints(thumbprints);
  op_ret = provider->create(s, true, y);

  if (op_ret == 0) {
    s->formatter->open_object_section("CreateOpenIDConnectProviderResponse");
    s->formatter->open_object_section("CreateOpenIDConnectProviderResult");
    provider->dump(s->formatter);
    s->formatter->close_section();
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}


static int validate_provider_arn(const std::string& provider_arn,
                                 rgw::ARN& resource, std::string& message)
{
  if (provider_arn.empty()) {
    message = "Missing required element OpenIDConnectProviderArn";
    return -EINVAL;
  }

  auto arn = rgw::ARN::parse(provider_arn, true);
  if (!arn) {
    message = "Invalid value for OpenIDConnectProviderArn";
    return -EINVAL;
  }
  resource = std::move(*arn);
  return 0;
}


RGWDeleteOIDCProvider::RGWDeleteOIDCProvider()
  : RGWRestOIDCProvider(rgw::IAM::iamDeleteOIDCProvider, RGW_CAP_WRITE)
{
}

int RGWDeleteOIDCProvider::init_processing(optional_yield y)
{
  provider_arn = s->info.args.get("OpenIDConnectProviderArn");
  return validate_provider_arn(provider_arn, resource, s->err.message);
}

void RGWDeleteOIDCProvider::execute(optional_yield y)
{
  std::unique_ptr<rgw::sal::RGWOIDCProvider> provider = driver->get_oidc_provider();
  provider->set_arn(provider_arn);
  provider->set_tenant(s->user->get_tenant());
  op_ret = provider->delete_obj(s, y);

  if (op_ret < 0 && op_ret != -ENOENT && op_ret != -EINVAL) {
    op_ret = ERR_INTERNAL_ERROR;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("DeleteOpenIDConnectProviderResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

RGWGetOIDCProvider::RGWGetOIDCProvider()
  : RGWRestOIDCProvider(rgw::IAM::iamGetOIDCProvider, RGW_CAP_READ)
{
}

int RGWGetOIDCProvider::init_processing(optional_yield y)
{
  provider_arn = s->info.args.get("OpenIDConnectProviderArn");
  return validate_provider_arn(provider_arn, resource, s->err.message);
}

void RGWGetOIDCProvider::execute(optional_yield y)
{
  std::unique_ptr<rgw::sal::RGWOIDCProvider> provider = driver->get_oidc_provider();
  provider->set_arn(provider_arn);
  provider->set_tenant(s->user->get_tenant());
  op_ret = provider->get(s, y);

  if (op_ret < 0 && op_ret != -ENOENT && op_ret != -EINVAL) {
    op_ret = ERR_INTERNAL_ERROR;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("GetOpenIDConnectProviderResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_object_section("GetOpenIDConnectProviderResult");
    provider->dump_all(s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}


RGWListOIDCProviders::RGWListOIDCProviders()
  : RGWRestOIDCProvider(rgw::IAM::iamListOIDCProviders, RGW_CAP_READ)
{
}

void RGWListOIDCProviders::execute(optional_yield y)
{
  vector<std::unique_ptr<rgw::sal::RGWOIDCProvider>> result;
  op_ret = driver->get_oidc_providers(s, s->user->get_tenant(), result, y);

  if (op_ret == 0) {
    s->formatter->open_array_section("ListOpenIDConnectProvidersResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_object_section("ListOpenIDConnectProvidersResult");
    s->formatter->open_array_section("OpenIDConnectProviderList");
    for (const auto& it : result) {
      s->formatter->open_object_section("member");
      auto& arn = it->get_arn();
      ldpp_dout(s, 0) << "ARN: " << arn << dendl;
      s->formatter->dump_string("Arn", arn);
      s->formatter->close_section();
    }
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

