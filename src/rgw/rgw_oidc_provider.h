// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_OIDC_PROVIDER_H
#define CEPH_RGW_OIDC_PROVIDER_H

#include <string>

#include "common/ceph_context.h"

class RGWCtl;

class RGWOIDCProvider
{
  using string = std::string;
  static const string oidc_url_oid_prefix;
  static const string oidc_arn_prefix;
  static constexpr int MAX_OIDC_NUM_CLIENT_IDS = 100;
  static constexpr int MAX_OIDC_CLIENT_ID_LEN = 255;
  static constexpr int MAX_OIDC_NUM_THUMBPRINTS = 5;
  static constexpr int MAX_OIDC_THUMBPRINT_LEN = 40;
  static constexpr int MAX_OIDC_URL_LEN = 255;

  CephContext *cct;
  RGWCtl *ctl;
  string id;
  string provider_url;
  string arn;
  string creation_date;
  string tenant;
  vector<string> client_ids;
  vector<string> thumbprints;

  int get_tenant_url_from_arn(string& tenant, string& url);
  int store_url(const string& url, bool exclusive);
  int read_url(const string& url, const string& tenant);
  bool validate_input();

public:
  RGWOIDCProvider(CephContext *cct,
                    RGWCtl *ctl,
                    string provider_url,
                    string tenant,
                    vector<string> client_ids,
                    vector<string> thumbprints)
  : cct(cct),
    ctl(ctl),
    provider_url(std::move(provider_url)),
    tenant(std::move(tenant)),
    client_ids(std::move(client_ids)),
    thumbprints(std::move(thumbprints)) {
  }

  RGWOIDCProvider(CephContext *cct,
                    RGWCtl *ctl,
                    string arn,
                    string tenant)
  : cct(cct),
    ctl(ctl),
    arn(std::move(arn)),
    tenant(std::move(tenant)) {
  }

  RGWOIDCProvider(CephContext *cct,
                    RGWCtl *ctl,
                    string tenant)
  : cct(cct),
    ctl(ctl),
    tenant(std::move(tenant)) {}

  RGWOIDCProvider(CephContext *cct,
          RGWCtl *ctl)
  : cct(cct),
    ctl(ctl) {}

  RGWOIDCProvider() {}

  ~RGWOIDCProvider() = default;

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    encode(id, bl);
    encode(provider_url, bl);
    encode(arn, bl);
    encode(creation_date, bl);
    encode(tenant, bl);
    encode(client_ids, bl);
    encode(thumbprints, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(id, bl);
    decode(provider_url, bl);
    decode(arn, bl);
    decode(creation_date, bl);
    decode(tenant, bl);
    decode(client_ids, bl);
    decode(thumbprints, bl);
    DECODE_FINISH(bl);
  }

  const string& get_provider_url() const { return provider_url; }
  const string& get_arn() const { return arn; }
  const string& get_create_date() const { return creation_date; }
  const vector<string>& get_client_ids() const { return client_ids;}
  const vector<string>& get_thumbprints() const { return thumbprints; }

  int create(bool exclusive);
  int delete_obj();
  int get();
  void dump(Formatter *f) const;
  void dump_all(Formatter *f) const;
  void decode_json(JSONObj *obj);

  static const string& get_url_oid_prefix();
  static int get_providers(RGWRados *store,
                            const string& tenant,
                            vector<RGWOIDCProvider>& providers);
};
WRITE_CLASS_ENCODER(RGWOIDCProvider)
#endif /* CEPH_RGW_OIDC_PROVIDER_H */

