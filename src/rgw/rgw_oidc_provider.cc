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

const string RGWOIDCProvider::oidc_url_oid_prefix = "oidc_url.";
const string RGWOIDCProvider::oidc_arn_prefix = "arn:aws:iam::";

int RGWOIDCProvider::store_url(const string& url, bool exclusive)
{
  using ceph::encode;
  string oid = tenant + get_url_oid_prefix() + url;

  auto svc = ctl->svc;

  bufferlist bl;
  encode(*this, bl);
  auto obj_ctx = svc->sysobj->init_obj_ctx();
  return rgw_put_system_obj(obj_ctx, svc->zone->get_zone_params().oidc_pool, oid,
                            bl, exclusive, NULL, real_time(), NULL);
}

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

int RGWOIDCProvider::create(bool exclusive)
{
  int ret;

  if (! validate_input()) {
    return -EINVAL;
  }

  string idp_url = provider_url;
  auto pos = idp_url.find("http://");
  if (pos == std::string::npos) {
    pos = idp_url.find("https://");
    if (pos != std::string::npos) {
      idp_url.erase(pos, 8);
    } else {
      pos = idp_url.find("www.");
      if (pos != std::string::npos) {
        idp_url.erase(pos, 4);
      }
    }
  } else {
    idp_url.erase(pos, 7);
  }

  /* check to see the name is not used */
  ret = read_url(idp_url, tenant);
  if (exclusive && ret == 0) {
    ldout(cct, 0) << "ERROR: url " << provider_url << " already in use"
                    << id << dendl;
    return -EEXIST;
  } else if ( ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "failed reading provider url  " << provider_url << ": "
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

  auto svc = ctl->svc;

  auto& pool = svc->zone->get_zone_params().oidc_pool;
  ret = store_url(idp_url, exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR:  storing role info in pool: " << pool.name << ": "
                  << provider_url << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

int RGWOIDCProvider::delete_obj()
{
  auto svc = ctl->svc;
  auto& pool = svc->zone->get_zone_params().oidc_pool;

  string url, tenant;
  auto ret = get_tenant_url_from_arn(tenant, url);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to parse arn" << dendl;
    return -EINVAL;
  }

  if (this->tenant != tenant) {
    ldout(cct, 0) << "ERROR: tenant in arn doesn't match that of user " << this->tenant << ", "
                  << tenant << ": " << dendl;
    return -EINVAL;
  }

  // Delete url
  string oid = tenant + get_url_oid_prefix() + url;
  ret = rgw_delete_system_obj(svc->sysobj, pool, oid, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting oidc url from pool: " << pool.name << ": "
                  << provider_url << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RGWOIDCProvider::get()
{
  string url, tenant;
  auto ret = get_tenant_url_from_arn(tenant, url);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to parse arn" << dendl;
    return -EINVAL;
  }

  if (this->tenant != tenant) {
    ldout(cct, 0) << "ERROR: tenant in arn doesn't match that of user " << this->tenant << ", "
                  << tenant << ": " << dendl;
    return -EINVAL;
  }

  ret = read_url(url, tenant);
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

int RGWOIDCProvider::read_url(const string& url, const string& tenant)
{
  auto svc = ctl->svc;
  auto& pool = svc->zone->get_zone_params().oidc_pool;
  string oid = tenant + get_url_oid_prefix() + url;
  bufferlist bl;
  auto obj_ctx = svc->sysobj->init_obj_ctx();

  int ret = rgw_get_system_obj(obj_ctx, pool, oid, bl, NULL, NULL, null_yield);
  if (ret < 0) {
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode oidc provider info from pool: " << pool.name <<
                  ": " << url << dendl;
    return -EIO;
  }

  return 0;
}

bool RGWOIDCProvider::validate_input()
{
  if (provider_url.length() > MAX_OIDC_URL_LEN) {
    ldout(cct, 0) << "ERROR: Invalid length of url " << dendl;
    return false;
  }
  if (client_ids.size() > MAX_OIDC_NUM_CLIENT_IDS) {
    ldout(cct, 0) << "ERROR: Invalid number of client ids " << dendl;
    return false;
  }

  for (auto& it : client_ids) {
    if (it.length() > MAX_OIDC_CLIENT_ID_LEN) {
      return false;
    }
  }

  if (thumbprints.size() > MAX_OIDC_NUM_THUMBPRINTS) {
    ldout(cct, 0) << "ERROR: Invalid number of thumbprints " << thumbprints.size() << dendl;
    return false;
  }

  for (auto& it : thumbprints) {
    if (it.length() > MAX_OIDC_THUMBPRINT_LEN) {
      return false;
    }
  }
  
  return true;
}

int RGWOIDCProvider::get_providers(RGWRados *store,
                                    const string& tenant,
                                    vector<RGWOIDCProvider>& providers)
{
  auto ctl = store->pctl;
  auto svc = ctl->svc;
  auto pool = store->svc.zone->get_zone_params().oidc_pool;
  string prefix = tenant + oidc_url_oid_prefix;

  //Get the filtered objects
  list<string> result;
  bool is_truncated;
  RGWListRawObjsCtx ctx;
  do {
    list<string> oids;
    int r = store->list_raw_objects(pool, prefix, 1000, ctx, oids, &is_truncated);
    if (r < 0) {
      ldout(ctl->cct, 0) << "ERROR: listing filtered objects failed: " << pool.name << ": "
                  << prefix << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    for (const auto& iter : oids) {
      RGWOIDCProvider provider(ctl->cct, store->pctl);
      bufferlist bl;
      auto obj_ctx = svc->sysobj->init_obj_ctx();

      int ret = rgw_get_system_obj(obj_ctx, pool, iter, bl, NULL, NULL, null_yield);
      if (ret < 0) {
        return ret;
      }

      try {
        using ceph::decode;
        auto iter = bl.cbegin();
        decode(provider, iter);
      } catch (buffer::error& err) {
        ldout(ctl->cct, 0) << "ERROR: failed to decode oidc provider info from pool: " << pool.name <<
                    ": " << iter << dendl;
        return -EIO;
      }

      providers.push_back(std::move(provider));
    }
  } while (is_truncated);

  return 0;
}

const string& RGWOIDCProvider::get_url_oid_prefix()
{
  return oidc_url_oid_prefix;
}
