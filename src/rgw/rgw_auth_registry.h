// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_RGW_AUTH_REGISTRY_H
#define CEPH_RGW_AUTH_REGISTRY_H

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

    s3_main_strategy_t(CephContext* const cct, RGWRados* const store)
      : s3_main_strategy_plain(cct, store),
        s3_main_strategy_boto2(cct, store) {
      add_engine(Strategy::Control::SUFFICIENT, s3_main_strategy_plain);
      add_engine(Strategy::Control::FALLBACK, s3_main_strategy_boto2);
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
                   RGWRados* const store)
    : s3_main_strategy(cct, store),
      s3_post_strategy(cct, store),
      swift_strategy(cct, store),
      sts_strategy(cct, store) {
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

  static std::shared_ptr<StrategyRegistry>
  create(CephContext* const cct,
         RGWRados* const store) {
    return std::make_shared<StrategyRegistry>(cct, store);
  }
};

} /* namespace auth */
} /* namespace rgw */

using rgw_auth_registry_t = rgw::auth::StrategyRegistry;
using rgw_auth_registry_ptr_t = std::shared_ptr<rgw_auth_registry_t>;

#endif /* CEPH_RGW_AUTH_REGISTRY_H */
