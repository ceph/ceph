

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include "rgw_service.h"

#include "svc_meta_be.h"
#include "svc_sys_obj.h"


class RGWSI_MBSObj_Handler_Module : public RGWSI_MetaBackend::Module {
protected:
  std::string section;
public:
  RGWSI_MBSObj_Handler_Module(const std::string& _section) : section(_section) {}
  virtual void get_pool_and_oid(const std::string& key, rgw_pool *pool, std::string *oid) = 0;
  virtual const std::string& get_oid_prefix() = 0;
  virtual std::string key_to_oid(const std::string& key) = 0;
  virtual bool is_valid_oid(const std::string& oid) = 0;
  virtual std::string oid_to_key(const std::string& oid) = 0;

  const std::string& get_section() {
    return section;
  }

  /* key to use for hashing entries for log shard placement */
  virtual std::string get_hash_key(const std::string& key) {
    return section + ":" + key;
  }
};

struct RGWSI_MBSObj_GetParams : public RGWSI_MetaBackend::GetParams {
  bufferlist *pbl{nullptr};
  std::map<std::string, bufferlist> *pattrs{nullptr};
  rgw_cache_entry_info *cache_info{nullptr};
  boost::optional<obj_version> refresh_version;

  RGWSI_MBSObj_GetParams() {}
  RGWSI_MBSObj_GetParams(bufferlist *_pbl,
                         std::map<std::string, bufferlist> *_pattrs,
                         ceph::real_time *_pmtime) : RGWSI_MetaBackend::GetParams(_pmtime),
                                                     pbl(_pbl),
                                                     pattrs(_pattrs) {}

  RGWSI_MBSObj_GetParams& set_cache_info(rgw_cache_entry_info *_cache_info) {
    cache_info = _cache_info;
    return *this;
  }
  RGWSI_MBSObj_GetParams& set_refresh_version(boost::optional<obj_version>& _refresh_version) {
    refresh_version = _refresh_version;
    return *this;
  }
};

struct RGWSI_MBSObj_PutParams : public RGWSI_MetaBackend::PutParams {
  bufferlist bl;
  const std::map<std::string, bufferlist> *pattrs{nullptr};
  bool exclusive{false};

  RGWSI_MBSObj_PutParams() {}
  RGWSI_MBSObj_PutParams(const std::map<std::string, bufferlist> *_pattrs,
                         const ceph::real_time& _mtime) : RGWSI_MetaBackend::PutParams(_mtime),
                                                          pattrs(_pattrs) {}
  RGWSI_MBSObj_PutParams(bufferlist& _bl,
                         const std::map<std::string, bufferlist> *_pattrs,
                         const ceph::real_time& _mtime,
                         bool _exclusive) : RGWSI_MetaBackend::PutParams(_mtime),
                                            bl(_bl),
                                            pattrs(_pattrs),
                                            exclusive(_exclusive) {}
};

struct RGWSI_MBSObj_RemoveParams : public RGWSI_MetaBackend::RemoveParams {
};

class RGWSI_MetaBackend_SObj : public RGWSI_MetaBackend
{
protected:
  RGWSI_SysObj *sysobj_svc{nullptr};

public:
  struct Context_SObj : public RGWSI_MetaBackend::Context {
    RGWSI_MBSObj_Handler_Module *module{nullptr};
    struct _list {
      std::optional<RGWSI_SysObj::Pool> pool;
      std::optional<RGWSI_SysObj::Pool::Op> op;
    } list;

    void init(RGWSI_MetaBackend_Handler *h) override;
  };

  RGWSI_MetaBackend_SObj(CephContext *cct);
  virtual ~RGWSI_MetaBackend_SObj();

  RGWSI_MetaBackend::Type get_type() {
    return MDBE_SOBJ;
  }

  void init(RGWSI_SysObj *_sysobj_svc,
            RGWSI_MDLog *_mdlog_svc) {
    base_init(_mdlog_svc);
    sysobj_svc = _sysobj_svc;
  }

  RGWSI_MetaBackend_Handler *alloc_be_handler() override;
  RGWSI_MetaBackend::Context *alloc_ctx() override;


  int call_with_get_params(ceph::real_time *pmtime, std::function<int(RGWSI_MetaBackend::GetParams&)> cb) override;

  int pre_modify(const DoutPrefixProvider *dpp, 
                 RGWSI_MetaBackend::Context *ctx,
                 const std::string& key,
                 RGWMetadataLogData& log_data,
                 RGWObjVersionTracker *objv_tracker,
                 RGWMDLogStatus op_type,
                 optional_yield y);
  int post_modify(const DoutPrefixProvider *dpp, 
                  RGWSI_MetaBackend::Context *ctx,
                  const std::string& key,
                  RGWMetadataLogData& log_data,
                  RGWObjVersionTracker *objv_tracker, int ret,
                  optional_yield y);

  int get_entry(RGWSI_MetaBackend::Context *ctx,
                const std::string& key,
                RGWSI_MetaBackend::GetParams& params,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y,
                const DoutPrefixProvider *dpp,
                bool get_raw_attrs=false) override;
  int put_entry(const DoutPrefixProvider *dpp, 
                RGWSI_MetaBackend::Context *ctx,
                const std::string& key,
                RGWSI_MetaBackend::PutParams& params,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y) override;
  int remove_entry(const DoutPrefixProvider *dpp, 
                   RGWSI_MetaBackend::Context *ctx,
                   const std::string& key,
                   RGWSI_MetaBackend::RemoveParams& params,
                   RGWObjVersionTracker *objv_tracker,
                   optional_yield y) override;

  int list_init(const DoutPrefixProvider *dpp, RGWSI_MetaBackend::Context *_ctx, const std::string& marker) override;
  int list_next(const DoutPrefixProvider *dpp,
                RGWSI_MetaBackend::Context *_ctx,
                int max, std::list<std::string> *keys,
                bool *truncated) override;
  int list_get_marker(RGWSI_MetaBackend::Context *ctx,
                      std::string *marker) override;

  int get_shard_id(RGWSI_MetaBackend::Context *ctx,
		   const std::string& key,
		   int *shard_id) override;

  int call(std::optional<RGWSI_MetaBackend_CtxParams> opt,
           std::function<int(RGWSI_MetaBackend::Context *)> f) override;
};


class RGWSI_MetaBackend_Handler_SObj : public RGWSI_MetaBackend_Handler {
  friend class RGWSI_MetaBackend_SObj::Context_SObj;

  RGWSI_MBSObj_Handler_Module *module{nullptr};

public:
  RGWSI_MetaBackend_Handler_SObj(RGWSI_MetaBackend *be) : 
                                            RGWSI_MetaBackend_Handler(be) {}
  
  void set_module(RGWSI_MBSObj_Handler_Module *_module) {
    module = _module;
  }

  RGWSI_MBSObj_Handler_Module *get_module() {
    return module;
  }
};
