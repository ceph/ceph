// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"

#include "svc_rados.h"
#include "svc_sys_obj.h"
#include "svc_sys_obj_core_types.h"


class RGWSI_Zone;

struct rgw_cache_entry_info;

class RGWSI_SysObj_Core : public RGWServiceInstance
{
  friend struct RGWServices_Def;
  friend class RGWSI_SysObj;

protected:
  RGWSI_RADOS *rados_svc{nullptr};
  RGWSI_Zone *zone_svc{nullptr};

  using GetObjState = RGWSI_SysObj_Obj_GetObjState;

  void core_init(RGWSI_RADOS *_rados_svc,
                 RGWSI_Zone *_zone_svc) {
    rados_svc = _rados_svc;
    zone_svc = _zone_svc;
  }
  tl::expected<RGWSI_RADOS::Obj, boost::system::error_code>
  get_rados_obj(RGWSI_Zone *zone_svc, const rgw_raw_obj& obj, optional_yield y);

  virtual boost::system::error_code
  raw_stat(const rgw_raw_obj& obj, uint64_t *psize,
           real_time *pmtime, uint64_t *epoch,
           boost::container::flat_map<std::string, ceph::buffer::list>* attrs,
           ceph::buffer::list *first_chunk,
           RGWObjVersionTracker *objv_tracker,
           optional_yield y);

  virtual boost::system::error_code read(RGWSysObjectCtxBase& obj_ctx,
                                         GetObjState& read_state,
                                         RGWObjVersionTracker *objv_tracker,
                                         const rgw_raw_obj& obj,
                                         bufferlist *bl, off_t ofs, off_t end,
                                         boost::container::flat_map<string, bufferlist> *attrs,
                                         bool raw_attrs,
                                         rgw_cache_entry_info *cache_info,
                                         boost::optional<obj_version>,
                                         optional_yield y);

  virtual boost::system::error_code remove(RGWSysObjectCtxBase& obj_ctx,
                                           RGWObjVersionTracker *objv_tracker,
                                           const rgw_raw_obj& obj,
                                           optional_yield y);

  virtual boost::system::error_code write(const rgw_raw_obj& obj,
                                          real_time *pmtime,
                                          boost::container::flat_map<std::string, bufferlist>& attrs,
                                          bool exclusive,
                                          const bufferlist& data,
                                          RGWObjVersionTracker *objv_tracker,
                                          real_time set_mtime,
                                          optional_yield y);

  virtual boost::system::error_code write_data(const rgw_raw_obj& obj,
                                               const bufferlist& bl,
                                               bool exclusive,
                                               RGWObjVersionTracker *objv_tracker,
                                               optional_yield y);

  virtual boost::system::error_code get_attr(const rgw_raw_obj& obj, const char *name, bufferlist *dest,
                                             optional_yield y);

  virtual boost::system::error_code set_attrs(const rgw_raw_obj& obj,
                                              boost::container::flat_map<string, bufferlist>& attrs,
                                              boost::container::flat_map<string, bufferlist> *rmattrs,
                                              RGWObjVersionTracker *objv_tracker,
                                              optional_yield y);

  virtual boost::system::error_code omap_get_all(const rgw_raw_obj& obj,
                                                 boost::container::flat_map<string, bufferlist> *m,
                                                 optional_yield y);
  virtual boost::system::error_code omap_get_vals(const rgw_raw_obj& obj,
                                                  const string& marker,
                                                  uint64_t count,
                                                  boost::container::flat_map<string, bufferlist> *m,
                                                  bool *pmore,
                                                  optional_yield y);
  virtual boost::system::error_code omap_set(const rgw_raw_obj& obj, const std::string& key,
                                             bufferlist& bl, bool must_exist,
                                             optional_yield y);
  virtual boost::system::error_code omap_set(const rgw_raw_obj& obj,
                                             const boost::container::flat_map<std::string, bufferlist>& m, bool must_exist,
                                             optional_yield y);
  virtual boost::system::error_code omap_del(const rgw_raw_obj& obj, const std::string& key,
                                             optional_yield y);

  virtual boost::system::error_code notify(const rgw_raw_obj& obj, bufferlist& bl,
                                           std::optional<std::chrono::milliseconds> timeout,
                                           bufferlist *pbl, optional_yield y);

  /* wrappers */
  boost::system::error_code get_system_obj_state_impl(RGWSysObjectCtxBase *rctx,
                                                      const rgw_raw_obj& obj, RGWSysObjState **state,
                                                      RGWObjVersionTracker *objv_tracker,
                                                      optional_yield y);
  boost::system::error_code get_system_obj_state(RGWSysObjectCtxBase *rctx, const rgw_raw_obj& obj,
                                                 RGWSysObjState **state,
                                                 RGWObjVersionTracker *objv_tracker,
                                                 optional_yield y);

  boost::system::error_code
  stat(RGWSysObjectCtxBase& obj_ctx,
       GetObjState& state,
       const rgw_raw_obj& obj,
       boost::container::flat_map<std::string, ceph::bufferlist>* attrs,
       bool raw_attrs,
       real_time *lastmod,
       uint64_t *obj_size,
       RGWObjVersionTracker *objv_tracker,
       optional_yield y);

public:
  RGWSI_SysObj_Core(CephContext *cct, boost::asio::io_context& ioc)
    : RGWServiceInstance(cct, ioc) {}

  RGWSI_Zone *get_zone_svc() {
    return zone_svc;
  }
};
