// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <list>
#include <string>
#include <string_view>
#include <vector>

#include "common/ceph_json.h"
#include "common/ceph_time.h"

struct RGWOIDCProviderInfo
{
  std::string id;
  std::string provider_url;
  std::string arn;
  std::string creation_date;
  std::string tenant; // tenant-name or account-id
  std::vector<std::string> client_ids;
  std::vector<std::string> thumbprints;

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
    DECODE_START(3, bl);
    decode(id, bl);
    decode(provider_url, bl);
    decode(arn, bl);
    decode(creation_date, bl);
    decode(tenant, bl);
    decode(client_ids, bl);
    decode(thumbprints, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static std::list<RGWOIDCProviderInfo> generate_test_instances();
};
WRITE_CLASS_ENCODER(RGWOIDCProviderInfo)

// Reserved identifier for global OIDC providers (not tied to any account).
// Uses '.' prefix which is invalid in account names and does not conflict with
// metadata separators ('$' for key, ':' for section).
static constexpr std::string_view global_oidc_id = ".global";

inline bool is_global_oidc_provider(const RGWOIDCProviderInfo& info) {
  return info.tenant == global_oidc_id;
}

// Format a timestamp as ISO 8601 for OIDC provider creation_date field.
std::string format_creation_date(ceph::real_time now);
