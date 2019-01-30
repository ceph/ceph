// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_service.h"

#include "services/svc_finisher.h"
#include "services/svc_bi_rados.h"
#include "services/svc_bilog_rados.h"
#include "services/svc_bucket_sobj.h"
#include "services/svc_cls.h"
#include "services/svc_mdlog.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_meta_be_otp.h"
#include "services/svc_notify.h"
#include "services/svc_otp.h"
#include "services/svc_rados.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_quota.h"
#include "services/svc_sync_modules.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"
#include "services/svc_sys_obj_core.h"
#include "services/svc_user_rados.h"

#include "common/errno.h"

#include "rgw_metadata.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_otp.h"
#include "rgw_cr_rados.h"

#define dout_subsys ceph_subsys_rgw


RGWServices_Def::RGWServices_Def() = default;
RGWServices_Def::~RGWServices_Def()
{
  shutdown();
}

boost::system::error_code RGWServices_Def::init(CephContext *cct,
						boost::asio::io_context& ioc,
						RGWRados* _rr,
						bool have_cache,
						bool raw, bool run_sync)
{
  rr = _rr;
  async_processor = std::make_unique<RGWAsyncRadosProcessor>(
    cct, cct->_conf->rgw_num_async_rados_threads);
  finisher = std::make_unique<RGWSI_Finisher>(cct, ioc);
  bucket_sobj = std::make_unique<RGWSI_Bucket_SObj>(cct, ioc);
  bi_rados = std::make_unique<RGWSI_BucketIndex_RADOS>(cct, ioc);
  bilog_rados = std::make_unique<RGWSI_BILog_RADOS>(cct, ioc);
  cls = std::make_unique<RGWSI_Cls>(cct, ioc);
  mdlog = std::make_unique<RGWSI_MDLog>(cct, ioc, run_sync);
  meta = std::make_unique<RGWSI_Meta>(cct, ioc);
  meta_be_sobj = std::make_unique<RGWSI_MetaBackend_SObj>(cct, ioc);
  meta_be_otp = std::make_unique<RGWSI_MetaBackend_OTP>(cct, ioc);
  notify = std::make_unique<RGWSI_Notify>(cct, ioc);
  otp = std::make_unique<RGWSI_OTP>(cct, ioc);
  rados = std::make_unique<RGWSI_RADOS>(cct, ioc);
  zone = std::make_unique<RGWSI_Zone>(cct, ioc);
  zone_utils = std::make_unique<RGWSI_ZoneUtils>(cct, ioc);
  quota = std::make_unique<RGWSI_Quota>(cct, ioc);
  sync_modules = std::make_unique<RGWSI_SyncModules>(cct, ioc);
  sysobj = std::make_unique<RGWSI_SysObj>(cct, ioc);
  sysobj_core = std::make_unique<RGWSI_SysObj_Core>(cct, ioc);
  user_rados = std::make_unique<RGWSI_User_RADOS>(cct, ioc);

  if (have_cache) {
    sysobj_cache = std::make_unique<RGWSI_SysObj_Cache>(cct, ioc);
  }

  vector<RGWSI_MetaBackend *> meta_bes{meta_be_sobj.get(), meta_be_otp.get()};

  finisher->init();
  bi_rados->init(zone.get(), rados.get(), bilog_rados.get(), log.get());
  bilog_rados->init(rados.get(), bi_rados.get());
  bucket_sobj->init(zone.get(), sysobj.get(), sysobj_cache.get(),
                    bi_rados.get(), meta.get(), meta_be_sobj.get(),
                    sync_modules.get());
  cls->init(zone.get(), rados.get());
  mdlog->init(async_processor.get(), rr, rados.get(), zone.get(), sysobj.get(),
	      cls.get());
  meta->init(sysobj.get(), mdlog.get(), meta_bes);
  meta_be_sobj->init(sysobj.get(), mdlog.get());
  meta_be_otp->init(sysobj.get(), mdlog.get(), cls.get());
  notify->init(zone.get(), rados.get(), finisher.get());
  otp->init(zone.get(), meta.get(), meta_be_otp.get());
  rados->init();
  zone->init(sysobj.get(), rados.get(), sync_modules.get());
  zone_utils->init(rados.get(), zone.get());
  quota->init(zone.get());
  sync_modules->init(zone.get());
  sysobj_core->core_init(rados.get(), zone.get());
  if (have_cache) {
    sysobj_cache->init(rados.get(), zone.get(), notify.get());
    sysobj->init(rados.get(), sysobj_cache.get());
  } else {
    sysobj->init(rados.get(), sysobj_core.get());
  }
  user_rados->init(rados.get(), zone.get(), sysobj.get(), sysobj_cache.get(),
                   meta.get(), meta_be_sobj.get(), sync_modules.get());


  async_processor->start();

  can_shutdown = true;

  auto r = finisher->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start finisher service (" << r << dendl;
    return r;
  }

  if (!raw) {
    r = notify->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start notify service (" << r << dendl;
      return r;
    }
  }

  r = rados->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start rados service (" << r << dendl;
    return r;
  }
  log = std::make_unique<RGWDataChangesLog>(zone.get(), cls.get());

  if (!raw) {
    r = zone->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start zone service (" << r << dendl;
      return r;
    }

    r = mdlog->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start mdlog service (" << r << dendl;
      return r;
    }

    r = sync_modules->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start sync modules service (" << r << dendl;
      return r;
    }
  }

  r = cls->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start cls service (" << r << dendl;
    return r;
  }

  r = zone_utils->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start zone_utils service (" << r << dendl;
    return r;
  }

  r = quota->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start quota service (" << r << dendl;
    return r;
  }

  r = sysobj_core->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start sysobj_core service (" << r << dendl;
    return r;
  }

  if (have_cache) {
    r = sysobj_cache->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start sysobj_cache service (" << r << dendl;
      return r;
    }
  }

  r = sysobj->start();
  if (r) {
    ldout(cct, 0) << "ERROR: failed to start sysobj service (" << r << dendl;
    return r;
  }

  if (!raw) {
    r = meta_be_sobj->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start meta_be_sobj service (" << r << dendl;
      return r;
    }

    r = meta->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start meta service (" << r << dendl;
      return r;
    }

    r = bucket_sobj->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start bucket service (" << r << dendl;
      return r;
    }

    r = user_rados->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start user_rados service (" << r << dendl;
      return r;
    }

    r = otp->start();
    if (r) {
      ldout(cct, 0) << "ERROR: failed to start otp service (" << r << dendl;
      return r;
    }
  }

  /* cache or core services will be started by sysobj */

  return {};
}

void RGWServices_Def::shutdown()
{
  if (!can_shutdown) {
    return;
  }

  if (has_shutdown) {
    return;
  }

  sysobj->shutdown();
  sysobj_core->shutdown();
  notify->shutdown();
  if (sysobj_cache) {
    sysobj_cache->shutdown();
  }
  quota->shutdown();
  zone_utils->shutdown();
  zone->shutdown();
  rados->shutdown();

  if (async_processor)
    async_processor->stop();

  has_shutdown = true;

}


boost::system::error_code RGWServices::do_init(CephContext* _cct,
					       boost::asio::io_context& ioc,
					       RGWRados* rr,
					       bool have_cache, bool raw,
					       bool run_sync) {
  cct = _cct;
  auto r = svc.init(cct, ioc, rr, have_cache, raw, run_sync);
  if (r) {
    return r;
  }

  rr = svc.rr;
  async_processor = svc.async_processor.get();
  log = svc.log.get();
  finisher = svc.finisher.get();
  bi_rados = svc.bi_rados.get();
  bi = bi_rados;
  bilog_rados = svc.bilog_rados.get();
  bucket_sobj = svc.bucket_sobj.get();
  bucket = bucket_sobj;
  cls = svc.cls.get();
  mdlog = svc.mdlog.get();
  meta = svc.meta.get();
  meta_be_sobj = svc.meta_be_sobj.get();
  meta_be_otp = svc.meta_be_otp.get();
  notify = svc.notify.get();
  otp = svc.otp.get();
  rados = svc.rados.get();
  zone = svc.zone.get();
  zone_utils = svc.zone_utils.get();
  quota = svc.quota.get();
  sync_modules = svc.sync_modules.get();
  sysobj = svc.sysobj.get();
  cache = svc.sysobj_cache.get();
  core = svc.sysobj_core.get();
  user = svc.user_rados.get();

  return {};
}

boost::system::error_code RGWServiceInstance::start()
{
  if (start_state != StateInit) {
    return {};
  }

  start_state = StateStarting; /* setting started prior to do_start() on purpose so that circular
                                  references can call start() on each other */

  auto r = do_start();
  if (r) {
    return r;
  }

  start_state = StateStarted;

  return {};
}

RGWCtlDef::RGWCtlDef() {}
RGWCtlDef::~RGWCtlDef() {}
RGWCtlDef::_meta::_meta() {}
RGWCtlDef::_meta::~_meta() {}


int RGWCtlDef::init(RGWServices& svc)
{
  meta.mgr.reset(new RGWMetadataManager(svc.meta));

  meta.user.reset(RGWUserMetaHandlerAllocator::alloc(svc.user));

  auto sync_module = svc.sync_modules->get_sync_module();
  if (sync_module) {
    meta.bucket.reset(sync_module->alloc_bucket_meta_handler());
    meta.bucket_instance.reset(sync_module->alloc_bucket_instance_meta_handler());
  } else {
    meta.bucket.reset(RGWBucketMetaHandlerAllocator::alloc());
    meta.bucket_instance.reset(RGWBucketInstanceMetaHandlerAllocator::alloc());
  }

  meta.otp.reset(RGWOTPMetaHandlerAllocator::alloc());

  user.reset(new RGWUserCtl(svc.zone, svc.user, svc.rados, (RGWUserMetadataHandler *)meta.user.get()));
  bucket.reset(new RGWBucketCtl(svc.zone,
                                svc.bucket,
                                svc.bi));
  otp.reset(new RGWOTPCtl(svc.zone, svc.otp));

  RGWBucketMetadataHandlerBase *bucket_meta_handler = static_cast<RGWBucketMetadataHandlerBase *>(meta.bucket.get());
  RGWBucketInstanceMetadataHandlerBase *bi_meta_handler = static_cast<RGWBucketInstanceMetadataHandlerBase *>(meta.bucket_instance.get());

  bucket_meta_handler->init(svc.bucket, bucket.get());
  bi_meta_handler->init(svc.zone, svc.bucket, svc.bi);

  RGWOTPMetadataHandlerBase *otp_handler = static_cast<RGWOTPMetadataHandlerBase *>(meta.otp.get());
  otp_handler->init(svc.zone, svc.meta_be_otp, svc.otp);

  user->init(bucket.get());
  bucket->init(user.get(),
               (RGWBucketMetadataHandler *)bucket_meta_handler,
               (RGWBucketInstanceMetadataHandler *)bi_meta_handler);

  otp->init((RGWOTPMetadataHandler *)meta.otp.get());

  return 0;
}

int RGWCtl::init(RGWServices *_svc)
{
  svc = _svc;
  cct = svc->cct;

  int r = _ctl.init(*svc);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init ctls (" << cpp_strerror(-r) << dendl;
    return r;
  }

  meta.mgr = _ctl.meta.mgr.get();
  meta.user = _ctl.meta.user.get();
  meta.bucket = _ctl.meta.bucket.get();
  meta.bucket_instance = _ctl.meta.bucket_instance.get();
  meta.otp = _ctl.meta.otp.get();

  user = _ctl.user.get();
  bucket = _ctl.bucket.get();
  otp = _ctl.otp.get();

  r = meta.user->attach(meta.mgr);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init meta.user ctl (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = meta.bucket->attach(meta.mgr);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init meta.bucket ctl (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = meta.bucket_instance->attach(meta.mgr);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init meta.bucket_instance ctl (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = meta.otp->attach(meta.mgr);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init otp ctl (" << cpp_strerror(-r) << dendl;
    return r;
  }

  return 0;
}

