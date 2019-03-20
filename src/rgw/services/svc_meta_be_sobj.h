

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


class RGWSI_MBSObj_Handler_Module;

struct rgwsi_meta_be_sobj_handler_info {
  RGWSI_MetaBackend::ModuleRef _module;
  RGWSI_MBSObj_Handler_Module *module;
  string section;
};


class RGWSI_MBSObj_Handler_Module : public RGWSI_MetaBackend::Module {
public:
  virtual void get_pool_and_oid(const string& key, rgw_pool& pool, string& oid) = 0;
  virtual void key_to_oid(string& key) {}
  virtual void oid_to_key(string& oid) {}
};

struct RGWSI_MBSObj_GetParams : public RGWSI_MetaBackend::GetParams {
  std::optional<bufferlist> _bl;
  bufferlist *pbl{nullptr};
  map<string, bufferlist> *pattrs{nullptr};
  rgw_cache_entry_info *cache_info{nullptr};
  boost::optional<obj_version> refresh_version;
  optional_yield y{null_yield};
};

struct RGWSI_MBSObj_PutParams : public RGWSI_MetaBackend::PutParams {
  bufferlist bl;
  map<string, bufferlist> *pattrs{nullptr};
  bool exclusive{false};
};

struct RGWSI_MBSObj_RemoveParams : public RGWSI_MetaBackend::RemoveParams {
  // nothing in here
};

class RGWSI_MetaBackend_SObj : public RGWSI_MetaBackend
{
protected:
  RGWSI_SysObj *sysobj_svc{nullptr};

  map<string, rgwsi_meta_be_sobj_handler_info> handlers;

  int init_handler(RGWMetadataHandler *handler, RGWSI_MetaBackend_Handle *phandle) override;

public:
  struct Context_SObj : public RGWSI_MetaBackend::Context {
    std::optional<RGWSysObjectCtx> obj_ctx;
    RGWMetadataObject *obj;
    rgw_pool pool;
    string oid;
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

  void init_ctx(RGWSI_MetaBackend_Handle handle, const string& key, RGWMetadataObject *obj, RGWSI_MetaBackend::Context *ctx) override;

  RGWSI_MetaBackend::GetParams *alloc_default_get_params(ceph::real_time *pmtime) override;

  int get_entry(RGWSI_MetaBackend::Context *ctx,
                RGWSI_MetaBackend::GetParams& params,
                RGWObjVersionTracker *objv_tracker) override;
  int put_entry(RGWSI_MetaBackend::Context *ctx,
                RGWSI_MetaBackend::PutParams& params,
                RGWObjVersionTracker *objv_tracker) override;
  int remove_entry(RGWSI_MetaBackend::Context *ctx,
                   RGWSI_MetaBackend::RemoveParams& params,
                   RGWObjVersionTracker *objv_tracker) override;
};


