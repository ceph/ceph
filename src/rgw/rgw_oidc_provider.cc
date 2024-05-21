// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_oidc_provider.h"

#define dout_subsys ceph_subsys_rgw

void RGWOIDCProviderInfo::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("provider_url", provider_url, f);
  encode_json("arn", arn, f);
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

void RGWOIDCProviderInfo::generate_test_instances(std::list<RGWOIDCProviderInfo*>& l)
{
  auto p = new RGWOIDCProviderInfo;
  p->id = "id";
  p->provider_url = "server.example.com";
  p->arn = "arn:aws:iam::acct:oidc-provider/server.example.com";
  p->creation_date = "someday";
  p->client_ids = {"a", "b"};
  p->thumbprints = {"c", "d"};
  l.push_back(p);
  l.push_back(new RGWOIDCProviderInfo);
}
