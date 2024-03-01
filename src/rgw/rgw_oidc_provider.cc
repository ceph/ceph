// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <ctime>
#include <regex>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"
#include "rgw_zone.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_oidc_provider.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw { namespace sal {

const string RGWOIDCProvider::oidc_url_oid_prefix = "oidc_url.";
const string RGWOIDCProvider::oidc_arn_prefix = "arn:aws:iam::";

int RGWOIDCProvider::get_tenant_url_from_arn(string& tenant, string& url)
{
  auto provider_arn = rgw::ARN::parse(arn);
  if (!provider_arn) {
    return -EINVAL;
  }
  url = provider_arn->resource;
  tenant = provider_arn->account;
  auto pos = url.find("oidc-provider/");
  if (pos != std::string::npos) {
    url.erase(pos, 14);
  }
  return 0;
}

int RGWOIDCProvider::create(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  int ret;

  if (! validate_input(dpp)) {
    return -EINVAL;
  }

  string idp_url = url_remove_prefix(provider_url);

  /* check to see the name is not used */
  ret = read_url(dpp, idp_url, tenant, y);
  if (exclusive && ret == 0) {
    ldpp_dout(dpp, 0) << "ERROR: url " << provider_url << " already in use"
                    << id << dendl;
    return -EEXIST;
  } else if ( ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "failed reading provider url  " << provider_url << ": "
                  << cpp_strerror(-ret) << dendl;
    return ret;
  }

  //arn
  arn = oidc_arn_prefix + tenant + ":oidc-provider/" + idp_url;

  // Creation time
  real_clock::time_point t = real_clock::now();

  struct timeval tv;
  real_clock::to_timeval(t, tv);

  char buf[30];
  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  strftime(buf,30,"%Y-%m-%dT%H:%M:%S", &result);
  sprintf(buf + strlen(buf),".%dZ",(int)tv.tv_usec/1000);
  creation_date.assign(buf, strlen(buf));

  ret = store_url(dpp, idp_url, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing role info in OIDC pool: "
                  << provider_url << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

int RGWOIDCProvider::get(const DoutPrefixProvider *dpp, optional_yield y)
{
  string url, tenant;
  auto ret = get_tenant_url_from_arn(tenant, url);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to parse arn" << dendl;
    return -EINVAL;
  }

  if (this->tenant != tenant) {
    ldpp_dout(dpp, 0) << "ERROR: tenant in arn doesn't match that of user " << this->tenant << ", "
                  << tenant << ": " << dendl;
    return -EINVAL;
  }

  ret = read_url(dpp, url, tenant, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

void RGWOIDCProvider::dump(Formatter *f) const
{
  encode_json("OpenIDConnectProviderArn", arn, f);
}

void RGWOIDCProvider::dump_all(Formatter *f) const
{
  f->open_object_section("ClientIDList");
  for (auto it : client_ids) {
    encode_json("member", it, f);
  }
  f->close_section();
  encode_json("CreateDate", creation_date, f);
  f->open_object_section("ThumbprintList");
  for (auto it : thumbprints) {
    encode_json("member", it, f);
  }
  f->close_section();
  encode_json("Url", provider_url, f);
}

void RGWOIDCProvider::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("OpenIDConnectProviderArn", arn, obj);
}

bool RGWOIDCProvider::validate_input(const DoutPrefixProvider *dpp)
{
  if (provider_url.length() > MAX_OIDC_URL_LEN) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid length of url " << dendl;
    return false;
  }
  if (client_ids.size() > MAX_OIDC_NUM_CLIENT_IDS) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid number of client ids " << dendl;
    return false;
  }

  for (auto& it : client_ids) {
    if (it.length() > MAX_OIDC_CLIENT_ID_LEN) {
      return false;
    }
  }

  if (thumbprints.size() > MAX_OIDC_NUM_THUMBPRINTS) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid number of thumbprints " << thumbprints.size() << dendl;
    return false;
  }

  for (auto& it : thumbprints) {
    if (it.length() > MAX_OIDC_THUMBPRINT_LEN) {
      return false;
    }
  }
  
  return true;
}

const string& RGWOIDCProvider::get_url_oid_prefix()
{
  return oidc_url_oid_prefix;
}

} } // namespace rgw::sal
