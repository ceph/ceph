// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_bucket.h"

#include "rgw_sync_module_log.h"
#include "rgw_sync_module_es.h"
#include "rgw_sync_module_aws.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

RGWMetadataHandler *RGWSyncModuleInstance::alloc_bucket_meta_handler(librados::Rados& rados)
{
  return RGWBucketMetaHandlerAllocator::alloc(rados);
}

RGWBucketInstanceMetadataHandlerBase* RGWSyncModuleInstance::alloc_bucket_instance_meta_handler(rgw::sal::Driver* driver)
{
  return RGWBucketInstanceMetaHandlerAllocator::alloc(driver);
}

RGWStatRemoteObjCBCR::RGWStatRemoteObjCBCR(RGWDataSyncCtx *_sc,
                       rgw_bucket& _src_bucket, rgw_obj_key& _key) : RGWCoroutine(_sc->cct),
                                                          sc(_sc), sync_env(_sc->env),
                                                          src_bucket(_src_bucket), key(_key) {
}

RGWCallStatRemoteObjCR::RGWCallStatRemoteObjCR(RGWDataSyncCtx *_sc,
                                               rgw_bucket& _src_bucket, rgw_obj_key& _key) : RGWCoroutine(_sc->cct),
                                                                                                 sc(_sc), sync_env(_sc->env),
                                                                                                 src_bucket(_src_bucket), key(_key) {
}

int RGWCallStatRemoteObjCR::operate(const DoutPrefixProvider *dpp) {
  reenter(this) {
    yield {
      call(new RGWStatRemoteObjCR(sync_env->async_rados, sync_env->driver,
                                  sc->source_zone,
                                  src_bucket, key, &mtime, &size, &etag, &attrs, &headers));
    }
    if (retcode < 0) {
      ldpp_dout(dpp, 10) << "RGWStatRemoteObjCR() returned " << retcode << dendl;
      return set_cr_error(retcode);
    }
    ldpp_dout(dpp, 20) << "stat of remote obj: z=" << sc->source_zone
                             << " b=" << src_bucket << " k=" << key
                             << " size=" << size << " mtime=" << mtime << dendl;
    yield {
      RGWStatRemoteObjCBCR *cb = allocate_callback();
      if (cb) {
        cb->set_result(mtime, size, etag, std::move(attrs), std::move(headers));
        call(cb);
      }
    }
    if (retcode < 0) {
      ldpp_dout(dpp, 10) << "RGWStatRemoteObjCR() callback returned " << retcode << dendl;
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

  RGWSyncModuleRef archive_module(std::make_shared<RGWArchiveSyncModule>());
  modules_manager->register_module("archive", archive_module);

  RGWSyncModuleRef log_module(std::make_shared<RGWLogSyncModule>());
  modules_manager->register_module("log", log_module);

  RGWSyncModuleRef es_module(std::make_shared<RGWElasticSyncModule>());
  modules_manager->register_module("elasticsearch", es_module);

  RGWSyncModuleRef aws_module(std::make_shared<RGWAWSSyncModule>());
  modules_manager->register_module("cloud", aws_module);
}
