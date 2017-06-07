#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"

#include "rgw_sync_module_log.h"
#include "rgw_sync_module_es.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

RGWStatRemoteObjCBCR::RGWStatRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                       RGWBucketInfo& _bucket_info, rgw_obj_key& _key) : RGWCoroutine(_sync_env->cct),
                                                          sync_env(_sync_env),
                                                          bucket_info(_bucket_info), key(_key) {
}

RGWCallStatRemoteObjCR::RGWCallStatRemoteObjCR(RGWDataSyncEnv *_sync_env,
                                               RGWBucketInfo& _bucket_info, rgw_obj_key& _key) : RGWCoroutine(_sync_env->cct),
                                                                                                 sync_env(_sync_env),
                                                                                                 bucket_info(_bucket_info), key(_key) {
}

int RGWCallStatRemoteObjCR::operate() {
  reenter(this) {
    yield {
      call(new RGWStatRemoteObjCR(sync_env->async_rados, sync_env->store,
                                  sync_env->source_zone,
                                  bucket_info, key, &mtime, &size, &attrs));
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 0) << "RGWStatRemoteObjCR() returned " << retcode << dendl;
      return set_cr_error(retcode);
    }
    ldout(sync_env->cct, 20) << "stat of remote obj: z=" << sync_env->source_zone
      << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
      << " attrs=" << attrs << dendl;
    yield {
      RGWStatRemoteObjCBCR *cb = allocate_callback();
      if (cb) {
        cb->set_result(mtime, size, std::move(attrs));
        call(cb);
      }
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 0) << "RGWStatRemoteObjCR() callback returned " << retcode << dendl;
      return set_cr_error(retcode);
    }
    return set_cr_done();
  }
  return 0;
}

void rgw_register_sync_modules(RGWSyncModulesManager *modules_manager)
{
  RGWSyncModuleRef default_module(std::make_shared<RGWDefaultSyncModule>());
  modules_manager->register_module("rgw", default_module, true);

  RGWSyncModuleRef log_module(std::make_shared<RGWLogSyncModule>());
  modules_manager->register_module("log", log_module);

  RGWSyncModuleRef es_module(std::make_shared<RGWElasticSyncModule>());
  modules_manager->register_module("elasticsearch", es_module);
}
