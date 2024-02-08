// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_service.h"

#include "svc_sys_obj.h"
#include "svc_sys_obj_core_types.h"


class RGWSI_Zone;

struct rgw_cache_entry_info;

class RGWSI_SysObj_Core : public RGWServiceInstance
{
  friend struct RGWServices_Def;
  friend class RGWSI_SysObj;

protected:
  librados::Rados* rados{nullptr};
  RGWSI_Zone *zone_svc{nullptr};

  using GetObjState = RGWSI_SysObj_Core_GetObjState;
  using PoolListImplInfo = RGWSI_SysObj_Core_PoolListImplInfo;

  void core_init(librados::Rados* rados_,
                 RGWSI_Zone *_zone_svc) {
    rados = rados_;
    zone_svc = _zone_svc;
  }
  int get_rados_obj(const DoutPrefixProvider *dpp, RGWSI_Zone *zone_svc, const rgw_raw_obj& obj, rgw_rados_ref* pobj);

  virtual int raw_stat(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj,
                       uint64_t *psize, real_time *pmtime,
                       std::map<std::string, bufferlist> *attrs,
                       RGWObjVersionTracker *objv_tracker,
                       optional_yield y);

  virtual int read(const DoutPrefixProvider *dpp,
                   RGWSI_SysObj_Obj_GetObjState& read_state,
                   RGWObjVersionTracker *objv_tracker,
                   const rgw_raw_obj& obj,
                   bufferlist *bl, off_t ofs, off_t end,
                   ceph::real_time* pmtime, uint64_t* psize,
                   std::map<std::string, bufferlist> *attrs,
		   bool raw_attrs,
                   rgw_cache_entry_info *cache_info,
                   boost::optional<obj_version>,
                   optional_yield y);

  virtual int remove(const DoutPrefixProvider *dpp, 
                     RGWObjVersionTracker *objv_tracker,
                     const rgw_raw_obj& obj,
                     optional_yield y);

  virtual int write(const DoutPrefixProvider *dpp, 
                    const rgw_raw_obj& obj,
                    real_time *pmtime,
                    std::map<std::string, bufferlist>& attrs,
                    bool exclusive,
                    const bufferlist& data,
                    RGWObjVersionTracker *objv_tracker,
                    real_time set_mtime,
                    optional_yield y);

  virtual int write_data(const DoutPrefixProvider *dpp, 
                         const rgw_raw_obj& obj,
                         const bufferlist& bl,
                         bool exclusive,
                         RGWObjVersionTracker *objv_tracker,
                         optional_yield y);

  virtual int get_attr(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj,
                       const char *name, bufferlist *dest,
                       optional_yield y);

  virtual int set_attrs(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj,
                        std::map<std::string, bufferlist>& attrs,
                        std::map<std::string, bufferlist> *rmattrs,
                        RGWObjVersionTracker *objv_tracker,
                        bool exclusive, optional_yield y);

  virtual int omap_get_all(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, std::map<std::string, bufferlist> *m,
                           optional_yield y);
  virtual int omap_get_vals(const DoutPrefixProvider *dpp, 
                            const rgw_raw_obj& obj,
                            const std::string& marker,
                            uint64_t count,
                            std::map<std::string, bufferlist> *m,
                            bool *pmore,
                            optional_yield y);
  virtual int omap_set(const DoutPrefixProvider *dpp, 
                       const rgw_raw_obj& obj, const std::string& key,
                       bufferlist& bl, bool must_exist,
                       optional_yield y);
  virtual int omap_set(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj,
                       const std::map<std::string, bufferlist>& m, bool must_exist,
                       optional_yield y);
  virtual int omap_del(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, const std::string& key,
                       optional_yield y);

  virtual int notify(const DoutPrefixProvider *dpp, 
                     const rgw_raw_obj& obj, bufferlist& bl,
                     uint64_t timeout_ms, bufferlist *pbl,
                     optional_yield y);

  virtual int pool_list_prefixed_objs(const DoutPrefixProvider *dpp,
                                      const rgw_pool& pool,
                                      const std::string& prefix,
                                      std::function<void(const std::string&)> cb);

  virtual int pool_list_objects_init(const DoutPrefixProvider *dpp,
                                     const rgw_pool& pool,
                                     const std::string& marker,
                                     const std::string& prefix,
                                     RGWSI_SysObj::Pool::ListCtx *ctx);
  virtual int pool_list_objects_next(const DoutPrefixProvider *dpp,
                                     RGWSI_SysObj::Pool::ListCtx& ctx,
                                     int max,
                                     std::vector<std::string> *oids,
                                     bool *is_truncated);

  virtual int pool_list_objects_get_marker(RGWSI_SysObj::Pool::ListCtx& _ctx,
                                           std::string *marker);

  int stat(RGWSI_SysObj_Obj_GetObjState& state,
           const rgw_raw_obj& obj,
           std::map<std::string, bufferlist> *attrs,
	   bool raw_attrs,
           real_time *lastmod,
           uint64_t *obj_size,
           RGWObjVersionTracker *objv_tracker,
           optional_yield y,
           const DoutPrefixProvider *dpp);

public:
  RGWSI_SysObj_Core(CephContext *cct): RGWServiceInstance(cct) {}

  RGWSI_Zone *get_zone_svc() {
    return zone_svc;
  }
};
