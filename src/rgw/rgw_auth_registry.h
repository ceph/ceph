// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <functional>
#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include "rgw_auth.h"
#include "rgw_auth_s3.h"
#include "rgw_swift_auth.h"
#include "rgw_rest_sts.h"

namespace rgw {
namespace auth {

/* A class aggregating the knowledge about all Strategies in RadosGW. It is
 * responsible for handling the dynamic reconfiguration on e.g. realm update. */
class StrategyRegistry {
  template <class AbstractorT,
            bool AllowAnonAccessT = false>
  using s3_strategy_t = \
    rgw::auth::s3::AWSAuthStrategy<AbstractorT, AllowAnonAccessT>;

  struct s3_main_strategy_t : public Strategy {
    using s3_main_strategy_plain_t = \
      s3_strategy_t<rgw::auth::s3::AWSGeneralAbstractor, true>;
    using s3_main_strategy_boto2_t = \
      s3_strategy_t<rgw::auth::s3::AWSGeneralBoto2Abstractor>;

    s3_main_strategy_plain_t s3_main_strategy_plain;
    s3_main_strategy_boto2_t s3_main_strategy_boto2;

    // Keep the existing plain/boto strategy order, but do not replay an
    // identical Keystone proof after a cold denial. A genuinely different
    // canonical request still gets the established Boto2 strategy attempt.
    struct boto2_fallback_t : public Engine {
      CephContext* const cct;
      const s3_main_strategy_boto2_t& strategy;
      const bool signing_key_mode;

      boto2_fallback_t(CephContext* cct, const s3_main_strategy_boto2_t& strategy)
        : cct(cct), strategy(strategy),
          signing_key_mode(cct->_conf->rgw_s3_auth_use_keystone &&
                          cct->_conf->rgw_keystone_s3_auth_mode == "signing_key") {}

      const char* get_name() const noexcept override {
        return "rgw::auth::StrategyRegistry::boto2_fallback_t";
      }
      result_t authenticate(const DoutPrefixProvider* dpp, const req_state* s,
                            optional_yield y) const override {
        if (signing_key_mode) {
          try {
            const auto plain = rgw::auth::s3::AWSGeneralAbstractor(cct).get_auth_data(s);
            const auto boto = rgw::auth::s3::AWSGeneralBoto2Abstractor(cct).get_auth_data(s);
            using Kind = rgw::auth::s3::AWSEngine::VersionAbstractor::ProofKind;
            bool strict = plain.proof_kind == Kind::s3_v2;
            if (plain.proof_kind == Kind::s3_v4 && plain.derived_key && boto.derived_key) {
              const auto& a = *plain.derived_key;
              const auto& b = *boto.derived_key;
              rgw::auth::keystone::SigningKeyCache::Key scope;
              const bool query = !s->info.env->get("HTTP_AUTHORIZATION", "")[0];
              strict = rgw::auth::keystone::validate_s3_v4_proof(
                  plain.string_to_sign, query, s->info.args.get("x-amz-expires"),
                  time(nullptr), scope) == 0 &&
                  a.date == scope.date && a.region == scope.region && a.service == scope.service &&
                  a.date == b.date && a.region == b.region && a.service == b.service &&
                  a.request_date == b.request_date && a.credential_scope == b.credential_scope;
            }
            if (strict && plain.proof_kind == boto.proof_kind &&
                !plain.access_key_id.empty() && !plain.client_signature.empty() &&
                plain.access_key_id == boto.access_key_id &&
                plain.client_signature == boto.client_signature &&
                plain.session_token.empty() && boto.session_token.empty() &&
                plain.string_to_sign == boto.string_to_sign) {
              return result_t::deny(); // FALLBACK preserves the original result
            }
          } catch (const int) {
            // The canonicalizers may differ in applicability too. Preserve
            // their normal behavior unless identical strict proofs were built.
          }
        }
        return strategy.authenticate(dpp, s, y);
      }
    } boto2_fallback;

    s3_main_strategy_t(CephContext* const cct,
		       const ImplicitTenants& implicit_tenant_context,
		       rgw::sal::Driver* driver)
      : s3_main_strategy_plain(cct, implicit_tenant_context, driver),
        s3_main_strategy_boto2(cct, implicit_tenant_context, driver),
        boto2_fallback(cct, s3_main_strategy_boto2) {
      add_engine(Strategy::Control::SUFFICIENT, s3_main_strategy_plain);
      add_engine(Strategy::Control::FALLBACK, boto2_fallback);
    }

    const char* get_name() const noexcept override {
      return "rgw::auth::StrategyRegistry::s3_main_strategy_t";
    }
  } s3_main_strategy;

  using s3_post_strategy_t = \
    s3_strategy_t<rgw::auth::s3::AWSBrowserUploadAbstractor>;
  s3_post_strategy_t s3_post_strategy;

  rgw::auth::swift::DefaultStrategy swift_strategy;

  rgw::auth::sts::DefaultStrategy sts_strategy;

public:
  StrategyRegistry(CephContext* const cct,
                   const ImplicitTenants& implicit_tenant_context,
                   rgw::sal::Driver* driver)
    : s3_main_strategy(cct, implicit_tenant_context, driver),
      s3_post_strategy(cct, implicit_tenant_context, driver),
      swift_strategy(cct, implicit_tenant_context, driver),
      sts_strategy(cct, implicit_tenant_context, driver) {
  }

  const s3_main_strategy_t& get_s3_main() const {
    return s3_main_strategy;
  }

  const s3_post_strategy_t& get_s3_post() const {
    return s3_post_strategy;
  }

  const rgw::auth::swift::DefaultStrategy& get_swift() const {
    return swift_strategy;
  }

  const rgw::auth::sts::DefaultStrategy& get_sts() const {
    return sts_strategy;
  }

  static std::unique_ptr<StrategyRegistry>
  create(CephContext* const cct,
         const ImplicitTenants& implicit_tenant_context,
         rgw::sal::Driver* driver) {
    return std::make_unique<StrategyRegistry>(cct, implicit_tenant_context, driver);
  }
};

} /* namespace auth */
} /* namespace rgw */

using rgw_auth_registry_t = rgw::auth::StrategyRegistry;
using rgw_auth_registry_ptr_t = std::unique_ptr<rgw_auth_registry_t>;
