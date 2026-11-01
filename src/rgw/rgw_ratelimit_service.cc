#include "rgw_ratelimit_store.h"

#include "common/dout.h"
#include "rgw_ratelimit.h"
#include "rgw_ratelimit_rados.h"
#include "rgw_ratelimit_shared.h"

#define dout_subsys ceph_subsys_rgw

struct RateLimitService::Impl {
  CephContext* cct;
  std::unique_ptr<ActiveRateLimiter> local;
  std::shared_ptr<RateLimitStore> distributed;
  std::shared_ptr<rgw::ratelimit::RadosRateLimitStore> rados;
  std::string backend;

  explicit Impl(CephContext* _cct) : cct(_cct)
  {
    backend = cct->_conf->rgw_ratelimit_backend;
  }

  void start()
  {
    if (backend == "redis") {
      const std::string prefix = cct->_conf->rgw_ratelimit_redis_key_prefix;
      distributed = std::make_shared<rgw::ratelimit::RedisRateLimitStore>(cct, prefix);
      return;
    }
    if (backend == "rados") {
      rados = std::make_shared<rgw::ratelimit::RadosRateLimitStore>(
          cct,
          cct->_conf->rgw_ratelimit_rados_pool,
          cct->_conf->rgw_ratelimit_rados_oid_prefix,
          cct->_conf->rgw_ratelimit_rados_num_shards);
      int ret = rados->init();
      if (ret < 0) {
        ldout(cct, 0) << "rados rate limit init failed, using shared fallback"
                      << dendl;
        distributed = std::make_shared<rgw::ratelimit::SharedCounterRateLimitStore>(
            cct, cct->_conf->rgw_ratelimit_redis_key_prefix);
        rados.reset();
        return;
      }
      distributed = rados;
      return;
    }
    local = std::make_unique<ActiveRateLimiter>(cct);
    local->start();
  }

  void shutdown()
  {
    if (rados) {
      rados->shutdown();
      rados.reset();
    }
    local.reset();
    distributed.reset();
  }

  std::shared_ptr<RateLimitStore> get_active()
  {
    if (distributed) {
      return distributed;
    }
    return local->get_active();
  }
};

RateLimitService::RateLimitService(CephContext* cct)
  : impl(std::make_unique<Impl>(cct))
{
}

RateLimitService::~RateLimitService()
{
  if (impl) {
    impl->shutdown();
  }
}

std::shared_ptr<RateLimitStore> RateLimitService::get_active()
{
  return impl->get_active();
}

void RateLimitService::start()
{
  impl->start();
}
