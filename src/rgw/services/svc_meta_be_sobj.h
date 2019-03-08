

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


struct rgwsi_meta_be_sobj_handler_info;

class RGWSI_MBSObj_Handler_Module : public RGWSI_MetaBackend::Module {
public:
  virtual void get_pool_and_oid(const string& key, rgw_pool& pool, string& oid) = 0;
  virtual void key_to_oid(string& key) {}
  virtual void oid_to_key(string& oid) {}
};


class RGWSI_MetaBackend_SObj : public RGWSI_MetaBackend
{
  RGWSI_SysObj *sysobj_svc{nullptr};

  map<string, rgwsi_meta_be_sobj_handler_info> handlers;

protected:
  int init_handler(RGWMetadataHandler *handler, RGWSI_MetaBackend_Handle *phandle) override;

public:
  struct Context_SObj : public RGWSI_MetaBackend::Context {
    std::optional<RGWSysObjectCtx> obj_ctx;
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

  void init_ctx(RGWSI_MetaBackend_Handle handle, const string& key, RGWSI_MetaBackend::Context *ctx) override;

  virtual int get_entry(RGWSI_MetaBackend::Context *ctx,
                        bufferlist *pbl,
                        RGWObjVersionTracker *objv_tracker,
                        real_time *pmtime,
                        map<string, bufferlist> *pattrs = nullptr,
                        rgw_cache_entry_info *cache_info = nullptr,
                        boost::optional<obj_version> refresh_version = boost::none) = 0;
  virtual int put_entry(RGWSI_MetaBackend::Context *ctx, bufferlist& bl, bool exclusive,
                        RGWObjVersionTracker *objv_tracker, real_time mtime, map<string, bufferlist> *pattrs = nullptr) = 0;
  virtual int remove_entry(RGWSI_MetaBackend::Context *ctx,
                           RGWObjVersionTracker *objv_tracker) = 0;
};


