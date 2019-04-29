

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

#include "rgw/rgw_service.h"

#include "svc_meta_be.h"
#include "svc_sys_obj.h"


class RGWSI_MBSObj_Handler_Module : public RGWSI_MetaBackend::Module {
public:
  virtual void get_pool_and_oid(const string& key, rgw_pool *pool, string *oid) = 0;
  virtual void key_to_oid(string& key) {}
  virtual void oid_to_key(string& oid) {}

  /* key to use for hashing entries for log shard placement */
  virtual void get_hash_key(const std::string& section, const std::string& key, std::string& hash_key) {
    hash_key = section + ":" + key;
  }
};

struct RGWSI_MBSObj_GetParams : public RGWSI_MetaBackend::GetParams {
  std::optional<bufferlist> _bl;
  bufferlist *pbl{nullptr};
  map<string, bufferlist> *pattrs{nullptr};
  rgw_cache_entry_info *cache_info{nullptr};
  boost::optional<obj_version> refresh_version;

  RGWSI_MBSObj_GetParams() {}
  RGWSI_MBSObj_GetParams(bufferlist *_pbl,
                         std::map<string, bufferlist> *_pattrs,
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
  map<string, bufferlist> *pattrs{nullptr};
  bool exclusive{false};

  RGWSI_MBSObj_PutParams() {}
  RGWSI_MBSObj_PutParams(std::map<string, bufferlist> *_pattrs,
                         const ceph::real_time& _mtime) : RGWSI_MetaBackend::PutParams(_mtime),
                                                          pattrs(_pattrs) {}
  RGWSI_MBSObj_PutParams(bufferlist& _bl,
                         std::map<string, bufferlist> *_pattrs,
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
    RGWSI_SysObj *sysobj_svc{nullptr};

    RGWSI_MBSObj_Handler_Module *module{nullptr};
    optional<RGWSysObjectCtx> obj_ctx;

    Context_SObj(RGWSI_SysObj *_sysobj_svc) : sysobj_svc(_sysobj_svc) {}

    void init(RGWSI_MetaBackend_Handler *h) override;
  };

  RGWSI_MetaBackend_SObj(CephContext *cct);
  virtual ~RGWSI_MetaBackend_SObj();

  RGWSI_MetaBackend::Type get_type() {
    return MDBE_SOBJ;
  }

  void init(RGWSI_SysObj *_sysobj_svc,
            RGWSI_MDLog *_mdlog_svc) {
    base_init(mdlog_svc);
    sysobj_svc = _sysobj_svc;
  }

  RGWSI_MetaBackend_Handler *alloc_be_handler() override;

  RGWSI_MetaBackend::GetParams *alloc_default_get_params(ceph::real_time *pmtime) override;

  int get_entry(RGWSI_MetaBackend::Context *ctx,
                const string& key,
                RGWSI_MetaBackend::GetParams& params,
                RGWObjVersionTracker *objv_tracker) override;
  int put_entry(RGWSI_MetaBackend::Context *ctx,
                const string& key,
                RGWSI_MetaBackend::PutParams& params,
                RGWObjVersionTracker *objv_tracker) override;
  int remove_entry(RGWSI_MetaBackend::Context *ctx,
                   const string& key,
                   RGWSI_MetaBackend::RemoveParams& params,
                   RGWObjVersionTracker *objv_tracker) override;

  int call(std::function<int(RGWSI_MetaBackend::Context *)> f) override;
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
