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
#include "rgw_rest_iam.h"
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


static std::string format_creation_date(ceph::real_time now)
{
  struct timeval tv;
  real_clock::to_timeval(now, tv);

  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  char buf[30];
  strftime(buf,30,"%Y-%m-%dT%H:%M:%S", &result);
  sprintf(buf + strlen(buf),".%03dZ",(int)tv.tv_usec/1000);
  return buf;
}


RGWCreateOIDCProvider::RGWCreateOIDCProvider()
  : RGWRestOIDCProvider(rgw::IAM::iamCreateOIDCProvider, RGW_CAP_WRITE)
{
}

inline constexpr int MAX_OIDC_NUM_CLIENT_IDS = 100;
inline constexpr int MAX_OIDC_CLIENT_ID_LEN = 255;
inline constexpr int MAX_OIDC_NUM_THUMBPRINTS = 5;
inline constexpr int MAX_OIDC_THUMBPRINT_LEN = 40;
inline constexpr int MAX_OIDC_URL_LEN = 255;

int RGWCreateOIDCProvider::init_processing(optional_yield y)
{
  info.provider_url = s->info.args.get("Url");
  if (info.provider_url.empty()) {
    s->err.message = "Missing required element Url";
    return -EINVAL;
  }
  if (info.provider_url.size() > MAX_OIDC_URL_LEN) {
    s->err.message = "Url cannot exceed the maximum length of "
        + std::to_string(MAX_OIDC_URL_LEN);
    return -EINVAL;
  }

  auto val_map = s->info.args.get_params();
  for (auto& it : val_map) {
    if (it.first.find("ClientIDList.member.") != string::npos) {
      if (it.second.size() > MAX_OIDC_CLIENT_ID_LEN) {
        s->err.message = "ClientID cannot exceed the maximum length of "
            + std::to_string(MAX_OIDC_CLIENT_ID_LEN);
        return -EINVAL;
      }
      info.client_ids.emplace_back(it.second);
    }
    if (it.first.find("ThumbprintList.member.") != string::npos) {
      if (it.second.size() > MAX_OIDC_THUMBPRINT_LEN) {
        s->err.message = "Thumbprint cannot exceed the maximum length of "
            + std::to_string(MAX_OIDC_THUMBPRINT_LEN);
        return -EINVAL;
      }
      info.thumbprints.emplace_back(it.second);
    }
  }

  if (info.thumbprints.empty()) {
    s->err.message = "Missing required element ThumbprintList";
    return -EINVAL;
  }
  if (info.thumbprints.size() > MAX_OIDC_NUM_THUMBPRINTS) {
    s->err.message = "ThumbprintList cannot exceed the maximum size of "
        + std::to_string(MAX_OIDC_NUM_THUMBPRINTS);
    return -EINVAL;
  }

  if (info.client_ids.size() > MAX_OIDC_NUM_CLIENT_IDS) {
    s->err.message = "ClientIDList cannot exceed the maximum size of "
        + std::to_string(MAX_OIDC_NUM_CLIENT_IDS);
    return -EINVAL;
  }

  if (const auto& acc = s->auth.identity->get_account(); acc) {
    info.tenant = acc->id;
  } else {
    info.tenant = s->user->get_tenant();
  }
  resource = rgw::ARN(url_remove_prefix(info.provider_url),
                      "oidc-provider/", info.tenant, true);
  info.arn = resource.to_string();
  info.creation_date = format_creation_date(real_clock::now());

  return 0;
}

void RGWCreateOIDCProvider::execute(optional_yield y)
{
  constexpr bool exclusive = true;
  op_ret = driver->store_oidc_provider(this, y, info, exclusive);
  if (op_ret == 0) {
    s->formatter->open_object_section_in_ns("CreateOpenIDConnectProviderResponse", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("CreateOpenIDConnectProviderResult");
    encode_json("OpenIDConnectProviderArn", info.arn, s->formatter);
    s->formatter->close_section();
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}


static int validate_provider_arn(const std::string& provider_arn,
                                 std::string_view tenant,
                                 rgw::ARN& resource, std::string& url,
                                 std::string& message)
{
  if (provider_arn.empty()) {
    message = "Missing required element OpenIDConnectProviderArn";
    return -EINVAL;
  }

  // teuthology runs keycloak on localhost:8080, and rgw::ARN::parse() rejects
  // that extra colon. aws docs say "The URL should not contain a port number."
  // but we'll be less strict about parsing

  std::string_view str = provider_arn;

  constexpr std::string_view arn_prefix = "arn:";
  if (!str.starts_with(arn_prefix)) {
    message = "Invalid value for OpenIDConnectProviderArn";
    return -EINVAL;
  }
  str.remove_prefix(arn_prefix.size());

  constexpr std::string_view partition = "aws:";
  if (!str.starts_with(partition)) {
    message = "OpenIDConnectProviderArn partition must be aws";
    return -EINVAL;
  }
  resource.partition = rgw::Partition::aws;
  str.remove_prefix(partition.size());

  constexpr std::string_view service = "iam::";
  if (!str.starts_with(service)) {
    message = "OpenIDConnectProviderArn service must be iam";
    return -EINVAL;
  }
  resource.service = rgw::Service::iam;
  str.remove_prefix(service.size());

  if (!str.starts_with(tenant)) {
    message = "OpenIDConnectProviderArn account must match user tenant";
    return -EINVAL;
  }
  resource.account = tenant;
  str.remove_prefix(tenant.size());

  constexpr std::string_view resource_prefix = ":oidc-provider/";
  if (!str.starts_with(resource_prefix)) {
    message = "Invalid ARN resource for OpenIDConnectProviderArn";
    return -EINVAL;
  }
  resource.resource = str.substr(1); // trim leading :
  str.remove_prefix(resource_prefix.size());
  url = str;

  return 0;
}


RGWDeleteOIDCProvider::RGWDeleteOIDCProvider()
  : RGWRestOIDCProvider(rgw::IAM::iamDeleteOIDCProvider, RGW_CAP_WRITE)
{
}

int RGWDeleteOIDCProvider::init_processing(optional_yield y)
{
  std::string_view account;
  if (const auto& acc = s->auth.identity->get_account(); acc) {
    account = acc->id;
  } else {
    account = s->user->get_tenant();
  }
  std::string provider_arn = s->info.args.get("OpenIDConnectProviderArn");
  return validate_provider_arn(provider_arn, account,
                               resource, url, s->err.message);
}

void RGWDeleteOIDCProvider::execute(optional_yield y)
{
  op_ret = driver->delete_oidc_provider(this, y, resource.account, url);

  if (op_ret < 0 && op_ret != -ENOENT && op_ret != -EINVAL) {
    op_ret = ERR_INTERNAL_ERROR;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section_in_ns("DeleteOpenIDConnectProviderResponse", RGW_REST_IAM_XMLNS);
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
  std::string_view account;
  if (const auto& acc = s->auth.identity->get_account(); acc) {
    account = acc->id;
  } else {
    account = s->user->get_tenant();
  }
  std::string provider_arn = s->info.args.get("OpenIDConnectProviderArn");
  return validate_provider_arn(provider_arn, account,
                               resource, url, s->err.message);
}

static void dump_oidc_provider(const RGWOIDCProviderInfo& info, Formatter *f)
{
  f->open_object_section("ClientIDList");
  for (const auto& it : info.client_ids) {
    encode_json("member", it, f);
  }
  f->close_section();
  encode_json("CreateDate", info.creation_date, f);
  f->open_object_section("ThumbprintList");
  for (const auto& it : info.thumbprints) {
    encode_json("member", it, f);
  }
  f->close_section();
  encode_json("Url", info.provider_url, f);
}

void RGWGetOIDCProvider::execute(optional_yield y)
{
  RGWOIDCProviderInfo info;
  op_ret = driver->load_oidc_provider(this, y, resource.account, url, info);

  if (op_ret < 0 && op_ret != -ENOENT && op_ret != -EINVAL) {
    op_ret = ERR_INTERNAL_ERROR;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section_in_ns("GetOpenIDConnectProviderResponse", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_object_section("GetOpenIDConnectProviderResult");
    dump_oidc_provider(info, s->formatter);
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
  std::string_view account;
  if (const auto& acc = s->auth.identity->get_account(); acc) {
    account = acc->id;
  } else {
    account = s->user->get_tenant();
  }
  vector<RGWOIDCProviderInfo> result;
  op_ret = driver->get_oidc_providers(this, y, account, result);

  if (op_ret == 0) {
    s->formatter->open_object_section_in_ns("ListOpenIDConnectProvidersResponse", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_object_section("ListOpenIDConnectProvidersResult");
    s->formatter->open_array_section("OpenIDConnectProviderList");
    for (const auto& it : result) {
      s->formatter->open_object_section("member");
      s->formatter->dump_string("Arn", it.arn);
      s->formatter->close_section();
    }
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

RGWAddClientIdToOIDCProvider::RGWAddClientIdToOIDCProvider()
  : RGWRestOIDCProvider(rgw::IAM::iamAddClientIdToOIDCProvider, RGW_CAP_WRITE)
{
}

int RGWAddClientIdToOIDCProvider::init_processing(optional_yield y)
{
  std::string_view account;
  if (const auto& acc = s->auth.identity->get_account(); acc) {
    account = acc->id;
  } else {
    account = s->user->get_tenant();
  }
  std::string provider_arn = s->info.args.get("OpenIDConnectProviderArn");
  auto ret = validate_provider_arn(provider_arn, account,
                               resource, url, s->err.message);
  if (ret < 0) {
    return ret;
  }

  client_id = s->info.args.get("ClientID");

  if (client_id.empty()) {
    s->err.message = "Missing required element ClientID";
    ldpp_dout(this, 20) << "ERROR: ClientID is empty" << dendl;
    return -EINVAL;
  }

  if (client_id.size() > MAX_OIDC_CLIENT_ID_LEN) {
    s->err.message = "ClientID cannot exceed the maximum length of "
        + std::to_string(MAX_OIDC_CLIENT_ID_LEN);
    ldpp_dout(this, 20) << "ERROR: ClientID length exceeded " << MAX_OIDC_CLIENT_ID_LEN << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWAddClientIdToOIDCProvider::execute(optional_yield y)
{
  RGWOIDCProviderInfo info;
  op_ret = driver->load_oidc_provider(this, y, resource.account, url, info);

  if (op_ret < 0) {
    if (op_ret != -ENOENT && op_ret != -EINVAL) {
      op_ret = ERR_INTERNAL_ERROR;
    }
    return;
  }

  if(std::find(info.client_ids.begin(), info.client_ids.end(), client_id) != info.client_ids.end()) {
    op_ret = -EEXIST;
  } else {

    info.client_ids.emplace_back(client_id);

    constexpr bool exclusive = false;
    op_ret = driver->store_oidc_provider(this, y, info, exclusive);
  }
  if (op_ret == 0 || op_ret == -EEXIST) {
    op_ret = 0;
    s->formatter->open_object_section("AddClientIDToOpenIDConnectProviderResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_object_section("AddClientIDToOpenIDConnectProviderResponse");
    dump_oidc_provider(info, s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

RGWUpdateOIDCProviderThumbprint::RGWUpdateOIDCProviderThumbprint()
  : RGWRestOIDCProvider(rgw::IAM::iamUpdateOIDCProviderThumbprint, RGW_CAP_WRITE)
{
}

int RGWUpdateOIDCProviderThumbprint::init_processing(optional_yield y)
{
  std::string_view account;
  if (const auto& acc = s->auth.identity->get_account(); acc) {
    account = acc->id;
  } else {
    account = s->user->get_tenant();
  }
  std::string provider_arn = s->info.args.get("OpenIDConnectProviderArn");
  auto ret = validate_provider_arn(provider_arn, account,
                               resource, url, s->err.message);
  if (ret < 0) {
    return ret;
  }

  auto val_map = s->info.args.get_params();
  /* From AWS documentation here: https://docs.aws.amazon.com/IAM/latest/APIReference/API_UpdateOpenIDConnectProviderThumbprint.html
  The list that you pass with this operation completely replaces the existing list of thumbprints. (The lists are not merged.) */
  for (auto& it : val_map) {
    if (it.first.find("ThumbprintList.member.") != string::npos) {
        if (it.second.size() > MAX_OIDC_THUMBPRINT_LEN) {
          s->err.message = "Thumbprint cannot exceed the maximum length of "
              + std::to_string(MAX_OIDC_THUMBPRINT_LEN);
          ldpp_dout(this, 20) << "ERROR: Thumbprint exceeds maximum length of " << MAX_OIDC_THUMBPRINT_LEN << dendl;
          return -EINVAL;
        }
        thumbprints.emplace_back(it.second);
    }
  }

  if (thumbprints.empty()) {
    s->err.message = "Missing required element ThumbprintList";
    ldpp_dout(this, 20) << "ERROR: Thumbprints list is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWUpdateOIDCProviderThumbprint::execute(optional_yield y)
{
  RGWOIDCProviderInfo info;
  op_ret = driver->load_oidc_provider(this, y, resource.account, url, info);

  if (op_ret < 0) {
    if (op_ret != -ENOENT && op_ret != -EINVAL) {
      op_ret = ERR_INTERNAL_ERROR;
    }
    return;
  }

  info.thumbprints = std::move(thumbprints);

  constexpr bool exclusive = false;
  op_ret = driver->store_oidc_provider(this, y, info, exclusive);
  if (op_ret == 0) {
    s->formatter->open_object_section("AddClientIDToOpenIDConnectProviderResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_object_section("AddClientIDToOpenIDConnectProviderResponse");
    dump_oidc_provider(info, s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}
