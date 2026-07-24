// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_oidc_provider.h"

#include <cstring>
#include <ctime>

#define dout_subsys ceph_subsys_rgw

std::string format_creation_date(ceph::real_time now)
{
  struct timeval tv;
  ceph::real_clock::to_timeval(now, tv);

  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  char buf[30];
  strftime(buf, 30, "%Y-%m-%dT%H:%M:%S", &result);
  sprintf(buf + strlen(buf), ".%03dZ", (int)tv.tv_usec / 1000);
  return buf;
}

void RGWOIDCProviderInfo::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("provider_url", provider_url, f);
  if (is_global_oidc_provider(*this)) {
    encode_json("arn", provider_url, f);
  } else {
    encode_json("arn", arn, f);
  }
  encode_json("creation_date", creation_date, f);
  encode_json("tenant", tenant, f);
  encode_json("client_ids", client_ids, f);
  encode_json("thumbprints", thumbprints, f);
}

void RGWOIDCProviderInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("provider_url", provider_url, obj);
  JSONDecoder::decode_json("arn", arn, obj);
  JSONDecoder::decode_json("creation_date", creation_date, obj);
  JSONDecoder::decode_json("tenant", tenant, obj);
  JSONDecoder::decode_json("client_ids", client_ids, obj);
  JSONDecoder::decode_json("thumbprints", thumbprints, obj);
}

std::list<RGWOIDCProviderInfo> RGWOIDCProviderInfo::generate_test_instances()
{
  std::list<RGWOIDCProviderInfo> l;
  RGWOIDCProviderInfo p;
  p.id = "id";
  p.provider_url = "server.example.com";
  p.arn = "arn:aws:iam::acct:oidc-provider/server.example.com";
  p.creation_date = "someday";
  p.client_ids = {"a", "b"};
  p.thumbprints = {"c", "d"};
  l.push_back(std::move(p));
  l.emplace_back();
  return l;
}
