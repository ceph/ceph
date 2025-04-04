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
#include "services/svc_notify.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_quota.h"
#include "services/svc_sync_modules.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"
#include "services/svc_sys_obj_core.h"
#include "services/svc_user_rados.h"

#include "common/errno.h"

#include "account.h"
#include "group.h"
#include "rgw_bucket.h"
#include "rgw_cr_rados.h"
#include "rgw_datalog.h"
#include "rgw_metadata.h"
#include "rgw_otp.h"
#include "rgw_sal_rados.h"
#include "rgw_user.h"
#include "role.h"
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
			  bool background_tasks,
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
  notify = std::make_unique<RGWSI_Notify>(cct);
  zone = std::make_unique<RGWSI_Zone>(cct);
  zone_utils = std::make_unique<RGWSI_ZoneUtils>(cct);
  quota = std::make_unique<RGWSI_Quota>(cct);
  sync_modules = std::make_unique<RGWSI_SyncModules>(cct);
  sysobj = std::make_unique<RGWSI_SysObj>(cct);
  sysobj_core = std::make_unique<RGWSI_SysObj_Core>(cct);
  user_rados = std::make_unique<RGWSI_User_RADOS>(cct);
  async_processor = std::make_unique<RGWAsyncRadosProcessor>(
    cct, cct->_conf->rgw_num_async_rados_threads);

  if (have_cache) {
    sysobj_cache = std::make_unique<RGWSI_SysObj_Cache>(dpp, cct);
  }

  async_processor->start();
  finisher->init();
  bi_rados->init(zone.get(), driver->getRados()->get_rados_handle(),
		 bilog_rados.get(), datalog_rados.get());
  bilog_rados->init(bi_rados.get());
  bucket_sobj->init(zone.get(), sysobj.get(), sysobj_cache.get(),
                    bi_rados.get(), mdlog.get(),
                    sync_modules.get(), bucket_sync_sobj.get());
  bucket_sync_sobj->init(zone.get(),
                         sysobj.get(),
                         sysobj_cache.get(),
                         bucket_sobj.get());
  cls->init(zone.get(), driver->getRados()->get_rados_handle());
  config_key_rados->init(driver->getRados()->get_rados_handle());
  mdlog->init(driver->getRados()->get_rados_handle(), zone.get(), sysobj.get(),
	      cls.get(), async_processor.get());
  notify->init(zone.get(), driver->getRados()->get_rados_handle(),
	       finisher.get());
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
  user_rados->init(driver->getRados()->get_rados_handle(), zone.get(),
                   mdlog.get(), sysobj.get(), sysobj_cache.get());

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
			     driver, background_tasks);
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

  datalog_rados.reset();
  user_rados->shutdown();
  sync_modules->shutdown();
  notify->shutdown();
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

int RGWServices::do_init(CephContext *_cct, rgw::sal::RadosStore* driver, bool have_cache, bool raw, bool run_sync, bool background_tasks, optional_yield y, const DoutPrefixProvider *dpp, const rgw::SiteConfig& _site)
{
  cct = _cct;
  site = &_site;

  int r = _svc.init(cct, driver, have_cache, raw, run_sync, background_tasks, y, dpp);
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
  notify = _svc.notify.get();
  zone = _svc.zone.get();
  zone_utils = _svc.zone_utils.get();
  quota = _svc.quota.get();
  sync_modules = _svc.sync_modules.get();
  sysobj = _svc.sysobj.get();
  cache = _svc.sysobj_cache.get();
  core = _svc.sysobj_core.get();
  user = _svc.user_rados.get();
  async_processor = _svc.async_processor.get();

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


int RGWCtlDef::init(RGWServices& svc, rgw::sal::Driver* driver,
                    librados::Rados& rados, const DoutPrefixProvider *dpp)
{
  meta.mgr.reset(new RGWMetadataManager());

  meta.user = create_user_metadata_handler(svc.user);

  bucket.reset(new RGWBucketCtl(svc.zone,
                                svc.bucket,
                                svc.bucket_sync,
                                svc.bi, svc.user,
                                svc.datalog_rados));

  auto sync_module = svc.sync_modules->get_sync_module();
  if (sync_module) {
    meta.bucket = sync_module->alloc_bucket_meta_handler(rados, svc.bucket, bucket.get());
    meta.bucket_instance = sync_module->alloc_bucket_instance_meta_handler(
        driver, svc.zone, svc.bucket, svc.bi, svc.datalog_rados);
  } else {
    meta.bucket = create_bucket_metadata_handler(rados, svc.bucket, bucket.get());
    meta.bucket_instance = create_bucket_instance_metadata_handler(
        driver, svc.zone, svc.bucket, svc.bi, svc.datalog_rados);
  }

  meta.otp = rgwrados::otp::create_metadata_handler(
      *svc.sysobj, *svc.cls, *svc.mdlog, svc.zone->get_zone_params());
  meta.role = rgwrados::role::create_metadata_handler(
      rados, *svc.sysobj, *svc.mdlog, svc.zone->get_zone_params());
  meta.account = rgwrados::account::create_metadata_handler(
      *svc.sysobj, svc.zone->get_zone_params());
  meta.group = rgwrados::group::create_metadata_handler(
      *svc.sysobj, rados, svc.zone->get_zone_params());

  user = std::make_unique<RGWUserCtl>(svc.zone, svc.user);

  meta.topic_cache = std::make_unique<RGWChainedCacheImpl<rgwrados::topic::cache_entry>>();
  meta.topic_cache->init(svc.cache);

  meta.topic = rgwrados::topic::create_metadata_handler(
      *svc.sysobj, svc.cache, *svc.mdlog, rados,
      svc.zone->get_zone_params(), *meta.topic_cache);

  user->init(bucket.get());
  bucket->init(user.get(), svc.datalog_rados, dpp);

  return 0;
}

int RGWCtl::init(RGWServices *_svc, rgw::sal::Driver* driver,
                 librados::Rados& rados, const DoutPrefixProvider *dpp)
{
  svc = _svc;
  cct = svc->cct;

  int r = _ctl.init(*svc, driver, rados, dpp);
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
    ldout(cct, 0) << "ERROR: failed to start init meta.role ctl (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = _ctl.meta.account->attach(meta.mgr);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init meta.account ctl (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = meta.topic->attach(meta.mgr);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init topic ctl ("
                  << cpp_strerror(-r) << dendl;
    return r;
  }

  r = _ctl.meta.group->attach(meta.mgr);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start init meta.group ctl (" << cpp_strerror(-r) << dendl;
    return r;
  }
  return 0;
}

