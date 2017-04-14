// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_AUTH_S3_H
#define CEPH_RGW_AUTH_S3_H

#include <string>
#include <tuple>

#include "rgw_common.h"
#include "rgw_rest_s3.h"

#include "rgw_auth.h"
#include "rgw_auth_filters.h"
#include "rgw_auth_keystone.h"


namespace rgw {
namespace auth {
namespace s3 {

class ExternalAuthStrategy : public rgw::auth::Strategy,
                             public rgw::auth::RemoteApplier::Factory {
  typedef rgw::auth::IdentityApplier::aplptr_t aplptr_t;
  RGWRados* const store;

  using keystone_config_t = rgw::keystone::CephCtxConfig;
  using keystone_cache_t = rgw::keystone::TokenCache;
  using EC2Engine = rgw::auth::keystone::EC2Engine;

  EC2Engine keystone_engine;
  LDAPEngine ldap_engine;

  aplptr_t create_apl_remote(CephContext* const cct,
                             const req_state* const s,
                             rgw::auth::RemoteApplier::acl_strategy_t&& acl_alg,
                             const rgw::auth::RemoteApplier::AuthInfo info
                            ) const override {
    auto apl = rgw::auth::add_sysreq(cct, store, s,
      rgw::auth::RemoteApplier(cct, store, std::move(acl_alg), info,
                               false /* no implicit tenants */));
    /* TODO(rzarzynski): replace with static_ptr. */
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

public:
  ExternalAuthStrategy(CephContext* const cct,
                       RGWRados* const store,
                       Version2ndEngine::Extractor* const extractor)
    : store(store),
      keystone_engine(cct, extractor,
                      static_cast<rgw::auth::RemoteApplier::Factory*>(this),
                      keystone_config_t::get_instance(),
                      keystone_cache_t::get_instance<keystone_config_t>()),
      ldap_engine(cct, store, *extractor,
                  static_cast<rgw::auth::RemoteApplier::Factory*>(this)) {

    if (cct->_conf->rgw_s3_auth_use_keystone &&
        ! cct->_conf->rgw_keystone_url.empty()) {
      add_engine(Control::SUFFICIENT, keystone_engine);
    }

    if (cct->_conf->rgw_s3_auth_use_ldap &&
        ! cct->_conf->rgw_ldap_uri.empty()) {
      add_engine(Control::SUFFICIENT, ldap_engine);
    }
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::AWSv2ExternalAuthStrategy";
  }
};


template <class ExtractorT>
class AWSv2AuthStrategy : public rgw::auth::Strategy,
                          public rgw::auth::LocalApplier::Factory {
  typedef rgw::auth::IdentityApplier::aplptr_t aplptr_t;

  static_assert(std::is_base_of<rgw::auth::s3::Version2ndEngine::Extractor,
                                ExtractorT>::value,
                "ExtractorT must be a subclass of rgw::auth::s3::ExtractorT");

  RGWRados* const store;
  ExtractorT extractor;

  ExternalAuthStrategy external_engines;
  LocalVersion2ndEngine local_engine;

  aplptr_t create_apl_local(CephContext* const cct,
                            const req_state* const s,
                            const RGWUserInfo& user_info,
                            const std::string& subuser) const override {
    auto apl = rgw::auth::add_sysreq(cct, store, s,
      rgw::auth::LocalApplier(cct, user_info, subuser));
    /* TODO(rzarzynski): replace with static_ptr. */
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

public:
  AWSv2AuthStrategy(CephContext* const cct,
                    RGWRados* const store)
    : store(store),
      extractor(cct),
      external_engines(cct, store, &extractor),
      local_engine(cct, store, extractor,
                   static_cast<rgw::auth::LocalApplier::Factory*>(this)) {

    Control local_engine_mode;
    if (! external_engines.is_empty()) {
      add_engine(Control::SUFFICIENT, external_engines);

      local_engine_mode = Control::FALLBACK;
    } else {
      local_engine_mode = Control::SUFFICIENT;
    }

    if (cct->_conf->rgw_s3_auth_use_rados) {
      add_engine(local_engine_mode, local_engine);
    }
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::AWSv2AuthStrategy";
  }
};

} /* namespace s3 */
} /* namespace auth */
} /* namespace rgw */

void rgw_create_s3_canonical_header(
  const char *method,
  const char *content_md5,
  const char *content_type,
  const char *date,
  const std::map<std::string, std::string>& meta_map,
  const char *request_uri,
  const std::map<std::string, std::string>& sub_resources,
  std::string& dest_str);
bool rgw_create_s3_canonical_header(const req_info& info,
                                    utime_t *header_time,       /* out */
                                    std::string& dest,          /* out */
                                    bool qsr);
static inline std::tuple<bool, std::string, utime_t>
rgw_create_s3_canonical_header(const req_info& info, const bool qsr) {
  std::string dest;
  utime_t header_time;

  const bool ok = rgw_create_s3_canonical_header(info, &header_time, dest, qsr);
  return std::make_tuple(ok, dest, header_time);
}

int rgw_get_s3_header_digest(const string& auth_hdr, const string& key,
			     string& dest);
int rgw_get_s3_header_digest(const string& auth_hdr, const string& key, string& dest);

namespace rgw {
namespace auth {
namespace s3 {


int parse_credentials(const req_info& info,             /* in */
                      std::string& credential,          /* out */
                      std::string& signedheaders,       /* out */
                      std::string& signature,           /* out */
                      std::string& date,                /* out */
                      bool& using_qs);                  /* out */

static inline std::string get_v4_canonical_uri(const req_info& info) {
  /* The code should normalize according to RFC 3986 but S3 does NOT do path
   * normalization that SigV4 typically does. This code follows the same
   * approach that boto library. See auth.py:canonical_uri(...). */

  std::string canonical_uri = info.request_uri_aws4;

  if (canonical_uri.empty()) {
    canonical_uri = "/";
  } else {
    boost::replace_all(canonical_uri, "+", "%20");
  }

  return canonical_uri;
}

std::string get_v4_canonical_qs(const req_info& info, bool using_qs);

boost::optional<std::string> get_v4_canonical_headers(const req_info& info,
                                                      const std::string& signedheaders,
                                                      bool using_qs,
                                                      bool force_boto2_compat);

std::string hash_string_sha256(const char* data, int len);

std::string get_v4_canonical_request_hash(CephContext* cct,
                                          const std::string& http_verb,
                                          const std::string& canonical_uri,
                                          const std::string& canonical_qs,
                                          const std::string& canonical_hdrs,
                                          const std::string& signed_hdrs,
                                          const std::string& request_payload_hash);

std::string get_v4_string_to_sign(CephContext* cct,
                                  const std::string& algorithm,
                                  const std::string& request_date,
                                  const std::string& credential_scope,
                                  const std::string& hashed_qr);

/* TODO(rzarzynski): split the SigningKey calculation into a separated func. */
std::string get_v4_signature(CephContext* cct,
                             const std::string& credential_scope,
                             const std::string& string_to_sign,
                             const std::string& access_key_secret,
                             /* This is a makeshift-only parameter. It'll be killed soon. */
                             char (&signing_key)[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE]);
} /* namespace s3 */
} /* namespace auth */
} /* namespace rgw */

#endif
