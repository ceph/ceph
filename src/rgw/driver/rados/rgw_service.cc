// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_service.h"

#include "services/svc_finisher.h"
#include "services/svc_bi_rados.h"
#include "services/svc_bilog_rados.h"
#include "services/svc_bucket_sobj.h"
#include "services/svc_bucket_sync_sobj.h"
#include "services/svc_cls.h"
#include "services/svc_config_key_rados.h"
#include "services/svc_mdlog.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_meta_be_otp.h"
#include "services/svc_notify.h"
#include "services/svc_otp.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_quota.h"
#include "services/svc_sync_modules.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"
#include "services/svc_sys_obj_core.h"
#include "services/svc_user_rados.h"
#include "services/svc_role_rados.h"
#include "services/svc_topic_rados.h"

#include "common/errno.h"

#include "rgw_bucket.h"
#include "rgw_cr_rados.h"
#include "rgw_datalog.h"
#include "rgw_metadata.h"
#include "rgw_otp.h"
#include "rgw_sal_rados.h"
#include "rgw_user.h"
#include "rgw_role.h"
#include "rgw_pubsub.h"
#include "topic.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

RGWServices_Def::RGWServices_Def() = default;
RGWServices_Def::~RGWServices_Def()
{
  shutdown();
}

int RGWServices_Def::init(CephContext *cct,
			  rgw::sal::RadosStore* driver,
			  bool have_cache,
                          bool raw,
			  bool run_sync,
			  optional_yield y,
                          const DoutPrefixProvider *dpp)
{
  finisher = std::make_unique<RGWSI_Finisher>(cct);
  bucket_sobj = std::make_unique<RGWSI_Bucket_SObj>(cct);
  bucket_sync_sobj = std::make_unique<RGWSI_Bucket_Sync_SObj>(cct);
  bi_rados = std::make_unique<RGWSI_BucketIndex_RADOS>(cct);
  bilog_rados = std::make_unique<RGWSI_BILog_RADOS>(cct);
  cls = std::make_unique<RGWSI_Cls>(cct);
  config_key_rados = std::make_unique<RGWSI_ConfigKey_RADOS>(cct);
  datalog_rados = std::make_unique<RGWDataChangesLog>(cct);
  mdlog = std::make_unique<RGWSI_MDLog>(cct, run_sync);
  meta = std::make_unique<RGWSI_Meta>(cct);
  meta_be_sobj = std::make_unique<RGWSI_MetaBackend_SObj>(cct);
  meta_be_otp = std::make_unique<RGWSI_MetaBackend_OTP>(cct);
  notify = std::make_unique<RGWSI_Notify>(cct);
  otp = std::make_unique<RGWSI_OTP>(cct);
  zone = std::make_unique<RGWSI_Zone>(cct);
  zone_utils = std::make_unique<RGWSI_ZoneUtils>(cct);
  quota = std::make_unique<RGWSI_Quota>(cct);
  sync_modules = std::make_unique<RGWSI_SyncModules>(cct);
  sysobj = std::make_unique<RGWSI_SysObj>(cct);
  sysobj_core = std::make_unique<RGWSI_SysObj_Core>(cct);
  user_rados = std::make_unique<RGWSI_User_RADOS>(cct);
  role_rados = std::make_unique<RGWSI_Role_RADOS>(cct);
  async_processor = std::make_unique<RGWAsyncRadosProcessor>(
    cct, cct->_conf->rgw_num_async_rados_threads);
  topic_rados = std::make_unique<RGWSI_Topic_RADOS>(cct);

  if (have_cache) {
    sysobj_cache = std::make_unique<RGWSI_SysObj_Cache>(dpp, cct);
  }

  vector<RGWSI_MetaBackend *> meta_bes{meta_be_sobj.get(), meta_be_otp.get()};

  async_processor->start();
  finisher->init();
  bi_rados->init(zone.get(), driver->getRados()->get_rados_handle(),
		 bilog_rados.get(), datalog_rados.get());
  bilog_rados->init(bi_rados.get());
  bucket_sobj->init(zone.get(), sysobj.get(), sysobj_cache.get(),
                    bi_rados.get(), meta.get(), meta_be_sobj.get(),
                    sync_modules.get(), bucket_sync_sobj.get());
  bucket_sync_sobj->init(zone.get(),
                         sysobj.get(),
                         sysobj_cache.get(),
                         bucket_sobj.get());
  cls->init(zone.get(), driver->getRados()->get_rados_handle());
  config_key_rados->init(driver->getRados()->get_rados_handle());
  mdlog->init(driver->getRados()->get_rados_handle(), zone.get(), sysobj.get(),
	      cls.get(), async_processor.get());
  meta->init(sysobj.get(), mdlog.get(), meta_bes);
  meta_be_sobj->init(sysobj.get(), mdlog.get());
  meta_be_otp->init(sysobj.get(), mdlog.get(), cls.get());
  notify->init(zone.get(), driver->getRados()->get_rados_handle(),
	       finisher.get());
  otp->init(zone.get(), meta.get(), meta_be_otp.get());
  zone->init(sysobj.get(), driver->getRados()->get_rados_handle(),
	     sync_modules.get(), bucket_sync_sobj.get());
  zone_utils->init(driver->getRados()->get_rados_handle(), zone.get());
  quota->init(zone.get());
  sync_modules->init(zone.get());
  sysobj_core->core_init(driver->getRados()->get_rados_handle(), zone.get());
  if (have_cache) {
    sysobj_cache->init(driver->getRados()->get_rados_handle(), zone.get(), notify.get());
    sysobj->init(driver->getRados()->get_rados_handle(), sysobj_cache.get());
  } else {
    sysobj->init(driver->getRados()->get_rados_handle(), sysobj_core.get());
  }
  user_rados->init(driver->getRados()->get_rados_handle(), zone.get(), sysobj.get(), sysobj_cache.get(),
                   meta.get(), meta_be_sobj.get(), sync_modules.get());
  role_rados->init(zone.get(), meta.get(), meta_be_sobj.get(), sysobj.get());
  topic_rados->init(zone.get(), meta.get(), meta_be_sobj.get(), sysobj.get());
  can_shutdown = true;

  int r = finisher->start(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to start finisher service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  if (!raw) {
    r = notify->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start notify service (" << cpp_strerror(-r) << dendl;
      return r;
    }
  }

  if (!raw) {
    r = zone->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start zone service (" << cpp_strerror(-r) << dendl;
      return r;
    }

    r = datalog_rados->start(dpp, &zone->get_zone(),
			     zone->get_zone_params(),
			     driver->getRados()->get_rados_handle());
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start datalog_rados service (" << cpp_strerror(-r) << dendl;
      return r;
    }

    r = mdlog->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start mdlog service (" << cpp_strerror(-r) << dendl;
      return r;
    }

    r = sync_modules->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start sync modules service (" << cpp_strerror(-r) << dendl;
      return r;
    }
  }

  r = cls->start(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to start cls service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = config_key_rados->start(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to start config_key service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = zone_utils->start(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to start zone_utils service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = quota->start(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to start quota service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = sysobj_core->start(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to start sysobj_core service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  if (have_cache) {
    r = sysobj_cache->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start sysobj_cache service (" << cpp_strerror(-r) << dendl;
      return r;
    }
  }

  r = sysobj->start(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to start sysobj service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  if (!raw) {
    r = meta_be_sobj->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start meta_be_sobj service (" << cpp_strerror(-r) << dendl;
      return r;
    }

    r = meta->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start meta service (" << cpp_strerror(-r) << dendl;
      return r;
    }

    r = bucket_sobj->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start bucket service (" << cpp_strerror(-r) << dendl;
      return r;
    }

    r = bucket_sync_sobj->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start bucket_sync service (" << cpp_strerror(-r) << dendl;
      return r;
    }

    r = user_rados->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start user_rados service (" << cpp_strerror(-r) << dendl;
      return r;
    }

    r = otp->start(y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start otp service (" << cpp_strerror(-r) << dendl;
      return r;
    }

    r = role_rados->start(y, dpp);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to start role_rados service (" << cpp_strerror(-r) << dendl;
      return r;
    }
    r = topic_rados->start(y, dpp);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to start topic_rados service ("
                    << cpp_strerror(-r) << dendl;
      return r;
    }
  }

  /* cache or core services will be started by sysobj */

  return  0;
}

void RGWServices_Def::shutdown()
{
  if (!can_shutdown) {
    return;
  }

  if (has_shutdown) {
    return;
  }

  topic_rados->shutdown();
  role_rados->shutdown();
  datalog_rados.reset();
  user_rados->shutdown();
  sync_modules->shutdown();
  otp->shutdown();
  notify->shutdown();
  meta_be_otp->shutdown();
  meta_be_sobj->shutdown();
  meta->shutdown();
  mdlog->shutdown();
  config_key_rados->shutdown();
  cls->shutdown();
  bilog_rados->shutdown();
  bi_rados->shutdown();
  bucket_sync_sobj->shutdown();
  bucket_sobj->shutdown();
  finisher->shutdown();

  sysobj->shutdown();
  sysobj_core->shutdown();
  notify->shutdown();
  if (sysobj_cache) {
    sysobj_cache->shutdown();
  }
  quota->shutdown();
  zone_utils->shutdown();
  zone->shutdown();
  async_processor->stop();

  has_shutdown = true;
}

int RGWServices::do_init(CephContext *_cct, rgw::sal::RadosStore* driver, bool have_cache, bool raw, bool run_sync, optional_yield y, const DoutPrefixProvider *dpp, const rgw::SiteConfig& _site)
{
  cct = _cct;
  site = &_site;

  int r = _svc.init(cct, driver, have_cache, raw, run_sync, y, dpp);
  if (r < 0) {
    return r;
  }

  finisher = _svc.finisher.get();
  bi_rados = _svc.bi_rados.get();
  bi = bi_rados;
  bilog_rados = _svc.bilog_rados.get();
  bucket_sobj = _svc.bucket_sobj.get();
  bucket = bucket_sobj;
  bucket_sync_sobj = _svc.bucket_sync_sobj.get();
  bucket_sync = bucket_sync_sobj;
  cls = _svc.cls.get();
  config_key_rados = _svc.config_key_rados.get();
  config_key = config_key_rados;
  datalog_rados = _svc.datalog_rados.get();
  mdlog = _svc.mdlog.get();
  meta = _svc.meta.get();
  meta_be_sobj = _svc.meta_be_sobj.get();
  meta_be_otp = _svc.meta_be_otp.get();
  notify = _svc.notify.get();
  otp = _svc.otp.get();
  zone = _svc.zone.get();
  zone_utils = _svc.zone_utils.get();
  quota = _svc.quota.get();
  sync_modules = _svc.sync_modules.get();
  sysobj = _svc.sysobj.get();
  cache = _svc.sysobj_cache.get();
  core = _svc.sysobj_core.get();
  user = _svc.user_rados.get();
  role = _svc.role_rados.get();
  async_processor = _svc.async_processor.get();
  topic = _svc.topic_rados.get();

  return 0;
}

RGWServiceInstance::~RGWServiceInstance() {}

int RGWServiceInstance::start(optional_yield y, const DoutPrefixProvider *dpp)
{
  if (start_state != StateInit) {
    return 0;
  }

  start_state = StateStarting;; /* setting started prior to do_start() on purpose so that circular
                                   references can call start() on each other */

  int r = do_start(y, dpp);
  if (r < 0) {
    return r;
  }

  start_state = StateStarted;

  return 0;
}

RGWCtlDef::RGWCtlDef() {}
RGWCtlDef::~RGWCtlDef() {}
RGWCtlDef::_meta::_meta() {}
RGWCtlDef::_meta::~_meta() {}


int RGWCtlDef::init(RGWServices& svc, rgw::sal::Driver* driver, const DoutPrefixProvider *dpp)
{
  meta.mgr.reset(new RGWMetadataManager(svc.meta));

  meta.user.reset(RGWUserMetaHandlerAllocator::alloc(svc.user));

  auto sync_module = svc.sync_modules->get_sync_module();
  if (sync_module) {
    meta.bucket.reset(sync_module->alloc_bucket_meta_handler());
    meta.bucket_instance.reset(sync_module->alloc_bucket_instance_meta_handler(driver));
  } else {
    meta.bucket.reset(RGWBucketMetaHandlerAllocator::alloc());
    meta.bucket_instance.reset(RGWBucketInstanceMetaHandlerAllocator::alloc(driver));
  }

  meta.otp.reset(RGWOTPMetaHandlerAllocator::alloc());
  meta.role = std::make_unique<rgw::sal::RGWRoleMetadataHandler>(driver, svc.role);
  meta.topic = std::make_unique<RGWTopicMetadataHandler>(driver, svc.topic);

  user.reset(new RGWUserCtl(svc.zone, svc.user, (RGWUserMetadataHandler *)meta.user.get()));
  bucket.reset(new RGWBucketCtl(svc.zone,
                                svc.bucket,
                                svc.bucket_sync,
                                svc.bi, svc.user));
  otp.reset(new RGWOTPCtl(svc.zone, svc.otp));

  RGWBucketMetadataHandlerBase *bucket_meta_handler = static_cast<RGWBucketMetadataHandlerBase *>(meta.bucket.get());
  RGWBucketInstanceMetadataHandlerBase *bi_meta_handler = static_cast<RGWBucketInstanceMetadataHandlerBase *>(meta.bucket_instance.get());

  bucket_meta_handler->init(svc.bucket, bucket.get());
  bi_meta_handler->init(svc.zone, svc.bucket, svc.bi);

  meta.topic_cache = std::make_unique<RGWChainedCacheImpl<rgwrados::topic::cache_entry>>();
  meta.topic_cache->init(svc.cache);

  meta.topic = rgwrados::topic::create_metadata_handler(
      *svc.sysobj, *svc.cache, *svc.mdlog, svc.zone->get_zone_params(),
      *meta.topic_cache);

  RGWOTPMetadataHandlerBase *otp_handler = static_cast<RGWOTPMetadataHandlerBase *>(meta.otp.get());
  otp_handler->init(svc.zone, svc.meta_be_otp, svc.otp);

  user->init(bucket.get());
  bucket->init(user.get(),
               (RGWBucketMetadataHandler *)bucket_meta_handler,
               (RGWBucketInstanceMetadataHandler *)bi_meta_handler,
	       svc.datalog_rados,
               dpp);

  otp->init((RGWOTPMetadataHandler *)meta.otp.get());

  return 0;
}

int RGWCtl::init(RGWServices *_svc, rgw::sal::Driver* driver, const DoutPrefixProvider *dpp)
{
  svc = _svc;
  cct = svc->cct;

  int r = _ctl.init(*svc, driver, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to start init ctls (" << cpp_strerror(-r) << dendl;
    return r;
  }

  meta.mgr = _ctl.meta.mgr.get();
  meta.user = _ctl.meta.user.get();
  meta.bucket = _ctl.meta.bucket.get();
  meta.bucket_instance = _ctl.meta.bucket_instance.get();
  meta.otp = _ctl.meta.otp.get();
  meta.role = _ctl.meta.role.get();
  meta.topic = _ctl.meta.topic.get();
  meta.topic_cache = _ctl.meta.topic_cache.get();

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

  r = meta.role->attach(meta.mgr);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init otp ctl (" << cpp_strerror(-r) << dendl;
    return r;
  }
  r = meta.topic->attach(meta.mgr);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init topic ctl ("
                  << cpp_strerror(-r) << dendl;
    return r;
  }
  return 0;
}

