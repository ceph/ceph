// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_bucket.h"

#include "rgw_sync_module_log.h"
#include "rgw_sync_module_es.h"
#include "rgw_sync_module_aws.h"
#include "rgw_sync_module_pubsub.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

RGWMetadataHandler *RGWSyncModuleInstance::alloc_bucket_meta_handler()
{
  return RGWBucketMetaHandlerAllocator::alloc();
}

RGWMetadataHandler *RGWSyncModuleInstance::alloc_bucket_instance_meta_handler()
{
  return RGWBucketInstanceMetaHandlerAllocator::alloc();
}

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
                                  bucket_info, key, &mtime, &size, &etag, &attrs, &headers));
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 10) << "RGWStatRemoteObjCR() returned " << retcode << dendl;
      return set_cr_error(retcode);
    }
    ldout(sync_env->cct, 20) << "stat of remote obj: z=" << sync_env->source_zone
                             << " b=" << bucket_info.bucket << " k=" << key
                             << " size=" << size << " mtime=" << mtime << dendl;
    yield {
      RGWStatRemoteObjCBCR *cb = allocate_callback();
      if (cb) {
        cb->set_result(mtime, size, etag, std::move(attrs), std::move(headers));
        call(cb);
      }
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 10) << "RGWStatRemoteObjCR() callback returned " << retcode << dendl;
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

  RGWSyncModuleRef pubsub_module(std::make_shared<RGWPSSyncModule>());
  modules_manager->register_module("pubsub", pubsub_module);
}
