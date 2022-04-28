// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "common/RWLock.h"
#include "rgw/rgw_service.h"
#include "rgw/rgw_cache.h"

#include "svc_sys_obj_cache.h"
#include "svc_sys_obj_core_rados.h"

class RGWSI_Notify;

class RGWSI_SysObj_Cache_CB;

class RGWSI_SysObj_Cache_RADOS : public RGWSI_SysObj_Cache
{
  friend class RGWSI_SysObj_Cache_CB;
  friend class RGWServices_Def;

  RGWSI_Notify *notify_svc{nullptr};
  ObjectCache cache;

  std::shared_ptr<RGWSI_SysObj_Cache_CB> cb;

protected:
  void do_init(RGWSI_SysObj_Core_RADOS *_sysobj_core_svc,
               RGWSI_Zone *_zone_svc,
               RGWSI_Notify *_notify_svc) {
    RGWSI_SysObj_Cache::init(_sysobj_core_svc, _zone_svc);
    notify_svc = _notify_svc;
  }

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;

  int watch_cb(const DoutPrefixProvider *dpp,
               uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               bufferlist& bl);

  int distribute_cache(const DoutPrefixProvider *dpp, const std::string& normal_name, const rgw_raw_obj& obj,
                       ObjectCacheInfo& obj_info, int op,
                       optional_yield y) override;

public:
  RGWSI_SysObj_Cache_RADOS(const DoutPrefixProvider *dpp, CephContext *cct) : RGWSI_SysObj_Cache(dpp, cct) {
    cache.set_ctx(cct);
  }

  bool chain_cache_entry(const DoutPrefixProvider *dpp,
                         std::initializer_list<rgw_cache_entry_info *> cache_info_entries,
                         RGWChainedCache::Entry *chained_entry);
  void register_chained_cache(RGWChainedCache *cc);
  void unregister_chained_cache(RGWChainedCache *cc);
};

