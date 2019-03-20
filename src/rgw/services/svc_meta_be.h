
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
#include "rgw/rgw_mdlog_types.h"

class RGWMetadataHandler;
class RGWMetadataLogData;
class RGWMetadataObject;

class RGWSI_MDLog;
class RGWSI_Meta;

typedef void *RGWSI_MetaBackend_Handle;

class RGWSI_MetaBackend : public RGWServiceInstance
{
  friend class RGWSI_Meta;
public:
  class Module;
  class Context;
protected:
  map<string, RGWMetadataHandler *> handlers;

  RGWSI_MDLog *mdlog_svc{nullptr};

  int find_handler(const string& metadata_key, RGWMetadataHandler **handler, string& entry);

  void base_init(RGWSI_MDLog *_mdlog_svc) {
    mdlog_svc = _mdlog_svc;
  }

  virtual int init_handler(RGWMetadataHandler *handler, RGWSI_MetaBackend_Handle *phandle) { return 0; }

  int prepare_mutate(RGWSI_MetaBackend::Context *ctx,
                     const real_time& mtime,
                     RGWObjVersionTracker *objv_tracker,
                     RGWMDLogSyncType sync_mode);

  virtual int mutate(Context *ctx,
                     const ceph::real_time& mtime, RGWObjVersionTracker *objv_tracker,
                     RGWMDLogStatus op_type,
                     RGWMDLogSyncType sync_mode,
                     std::function<int()> f,
                     bool generic_prepare);

  virtual int pre_modify(Context *ctx,
                         RGWMetadataLogData& log_data,
                         RGWObjVersionTracker *objv_tracker,
                         RGWMDLogStatus op_type);
  virtual int post_modify(Context *ctx,
                          RGWMetadataLogData& log_data,
                          RGWObjVersionTracker *objv_tracker, int ret);
public:
  class Module {
    /*
     * Backend specialization module
     */
  public:
    virtual ~Module() = 0;
    /* key to use for hashing entries for log shard placement */
    virtual void get_hash_key(const string& section, const string& key, string& hash_key) {
      hash_key = section + ":" + key;
    }
  };

  using ModuleRef = std::shared_ptr<Module>;

  struct Context { /*
                    * A single metadata operation context. Will be holding info about
                    * backend and operation itself; operation might span multiple backend
                    * calls.
                    */
    virtual ~Context() = 0;

    RGWSI_MetaBackend_Handle handle;
    Module *module{nullptr};
    std::string section;
    std::string key;
  };

  struct PutParams {
    virtual ~PutParams() = 0;

    ceph::real_time mtime;
  };

  struct GetParams {
    virtual ~GetParams();

    ceph::real_time *pmtime{nullptr};
  };

  struct RemoveParams {
    virtual ~RemoveParams() = 0;

    ceph::real_time mtime;
  };

  enum Type {
    MDBE_SOBJ = 0,
    MDBE_OTP  = 1,
  };

  RGWSI_MetaBackend(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_MetaBackend() {}

  virtual Type get_type() = 0;

  virtual void init_ctx(RGWSI_MetaBackend_Handle handle, const string& key, RGWMetadataObject *obj, Context *ctx) = 0;

  virtual GetParams *alloc_default_get_params(ceph::real_time *pmtime) = 0;

  /* these should be implemented by backends */
  virtual int get_entry(RGWSI_MetaBackend::Context *ctx,
                        RGWSI_MetaBackend::GetParams& params,
                        RGWObjVersionTracker *objv_tracker) = 0;
  virtual int put_entry(RGWSI_MetaBackend::Context *ctx,
                        RGWSI_MetaBackend::PutParams& params,
                        RGWObjVersionTracker *objv_tracker) = 0;
  virtual int remove_entry(Context *ctx,
                           RGWSI_MetaBackend::RemoveParams& params,
                           RGWObjVersionTracker *objv_tracker) = 0;

  /* these should be called by handlers */
  virtual int get(Context *ctx,
                  GetParams &params,
                  RGWObjVersionTracker *objv_tracker);

  virtual int put(Context *ctx,
                  PutParams& params,
                  RGWObjVersionTracker *objv_tracker,
                  RGWMDLogSyncType sync_mode);

  virtual int remove(Context *ctx,
                     RemoveParams& params,
                     RGWObjVersionTracker *objv_tracker,
                     RGWMDLogSyncType sync_mode);
};

