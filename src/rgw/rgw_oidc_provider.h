// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>

#include "common/ceph_context.h"
#include "common/ceph_json.h"

#include "rgw/rgw_sal.h"

namespace rgw { namespace sal {

class RGWOIDCProvider
{
public:
  static const std::string oidc_url_oid_prefix;
  static const std::string oidc_arn_prefix;
  static constexpr int MAX_OIDC_NUM_CLIENT_IDS = 100;
  static constexpr int MAX_OIDC_CLIENT_ID_LEN = 255;
  static constexpr int MAX_OIDC_NUM_THUMBPRINTS = 5;
  static constexpr int MAX_OIDC_THUMBPRINT_LEN = 40;
  static constexpr int MAX_OIDC_URL_LEN = 255;

protected:
  std::string id;
  std::string provider_url;
  std::string arn;
  std::string creation_date;
  std::string tenant;
  std::vector<std::string> client_ids;
  std::vector<std::string> thumbprints;

  int get_tenant_url_from_arn(std::string& tenant, std::string& url);
  virtual int store_url(const DoutPrefixProvider *dpp, const std::string& url, bool exclusive, optional_yield y) = 0;
  virtual int read_url(const DoutPrefixProvider *dpp, const std::string& url, const std::string& tenant, optional_yield y) = 0;
  bool validate_input(const DoutPrefixProvider *dpp);

public:
  void set_arn(std::string _arn) {
    arn = _arn;
  }
  void set_url(std::string _provider_url) {
    provider_url = _provider_url;
  }
  void set_tenant(std::string _tenant) {
    tenant = _tenant;
  }
  void set_client_ids(std::vector<std::string>& _client_ids) {
    client_ids = std::move(_client_ids);
  }
  void set_thumbprints(std::vector<std::string>& _thumbprints) {
    thumbprints = std::move(_thumbprints);
  }

  RGWOIDCProvider(std::string provider_url,
                    std::string tenant,
                    std::vector<std::string> client_ids,
                    std::vector<std::string> thumbprints)
  : provider_url(std::move(provider_url)),
    tenant(std::move(tenant)),
    client_ids(std::move(client_ids)),
    thumbprints(std::move(thumbprints)) {
  }

  RGWOIDCProvider( std::string arn,
                    std::string tenant)
  : arn(std::move(arn)),
    tenant(std::move(tenant)) {
  }

  RGWOIDCProvider(std::string tenant)
  : tenant(std::move(tenant)) {}

  RGWOIDCProvider() {}

  virtual ~RGWOIDCProvider() = default;

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

  const std::string& get_provider_url() const { return provider_url; }
  const std::string& get_arn() const { return arn; }
  const std::string& get_create_date() const { return creation_date; }
  const std::vector<std::string>& get_client_ids() const { return client_ids;}
  const std::vector<std::string>& get_thumbprints() const { return thumbprints; }

  int create(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) = 0;
  int get(const DoutPrefixProvider *dpp, optional_yield y);
  void dump(Formatter *f) const;
  void dump_all(Formatter *f) const;
  void decode_json(JSONObj *obj);

  static const std::string& get_url_oid_prefix();
};
WRITE_CLASS_ENCODER(RGWOIDCProvider)

} } // namespace rgw::sal
