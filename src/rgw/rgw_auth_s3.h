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

void rgw_hash_s3_string_sha256(const char *data, int len, string& dest);
void rgw_create_s3_v4_canonical_request(struct req_state *s, const string& canonical_uri,
                                        const string& canonical_qs, const string& canonical_hdrs,
                                        const string& signed_hdrs, const string& request_payload,
                                        bool unsigned_payload,
                                        string& canonical_req, string& canonical_req_hash);
void rgw_create_s3_v4_string_to_sign(CephContext *cct, const string& algorithm,
                                     const string& request_date, const string& credential_scope,
                                     const string& hashed_qr, string& string_to_sign);
int rgw_calculate_s3_v4_aws_signature(struct req_state *s, const string& access_key_id,
                                      const string &date, const string& region,
                                      const string& service, const string& string_to_sign,
                                      string& signature);

#endif
