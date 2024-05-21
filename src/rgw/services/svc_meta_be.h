
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

#include "svc_meta_be_params.h"

#include "rgw_mdlog_types.h"

#include "driver/rados/rgw_service.h" // FIXME: subclass dependency

class RGWMetadataLogData;

class RGWSI_MDLog;
class RGWSI_Meta;
class RGWObjVersionTracker;
class RGWSI_MetaBackend_Handler;

class RGWSI_MetaBackend : public RGWServiceInstance
{
  friend class RGWSI_Meta;
public:
  class Module;
  class Context;
protected:
  RGWSI_MDLog *mdlog_svc{nullptr};

  void base_init(RGWSI_MDLog *_mdlog_svc) {
    mdlog_svc = _mdlog_svc;
  }

  int prepare_mutate(RGWSI_MetaBackend::Context *ctx,
                     const std::string& key,
                     const ceph::real_time& mtime,
                     RGWObjVersionTracker *objv_tracker,
                     optional_yield y,
                     const DoutPrefixProvider *dpp);

  virtual int do_mutate(Context *ctx,
                     const std::string& key,
                     const ceph::real_time& mtime, RGWObjVersionTracker *objv_tracker,
                     RGWMDLogStatus op_type,
                     optional_yield y,
                     std::function<int()> f,
                     bool generic_prepare,
                     const DoutPrefixProvider *dpp);

  virtual int pre_modify(const DoutPrefixProvider *dpp, 
                         Context *ctx,
                         const std::string& key,
                         RGWMetadataLogData& log_data,
                         RGWObjVersionTracker *objv_tracker,
                         RGWMDLogStatus op_type,
                         optional_yield y);
  virtual int post_modify(const DoutPrefixProvider *dpp, 
                          Context *ctx,
                          const std::string& key,
                          RGWMetadataLogData& log_data,
                          RGWObjVersionTracker *objv_tracker, int ret,
                          optional_yield y);
public:
  class Module {
    /*
     * Backend specialization module
     */
  public:
    virtual ~Module() = 0;
  };

  using ModuleRef = std::shared_ptr<Module>;

  struct Context { /*
                    * A single metadata operation context. Will be holding info about
                    * backend and operation itself; operation might span multiple backend
                    * calls.
                    */
    virtual ~Context() = 0;

    virtual void init(RGWSI_MetaBackend_Handler *h) = 0;
  };

  virtual Context *alloc_ctx() = 0;

  struct PutParams {
    ceph::real_time mtime;

    PutParams() {}
    PutParams(const ceph::real_time& _mtime) : mtime(_mtime) {}
    virtual ~PutParams() = 0;
  };

  struct GetParams {
    GetParams() {}
    GetParams(ceph::real_time *_pmtime) : pmtime(_pmtime) {}
    virtual ~GetParams();

    ceph::real_time *pmtime{nullptr};
  };

  struct RemoveParams {
    virtual ~RemoveParams() = 0;

    ceph::real_time mtime;
  };

  struct MutateParams {
    ceph::real_time mtime;
    RGWMDLogStatus op_type;

    MutateParams() {}
    MutateParams(const ceph::real_time& _mtime,
		 RGWMDLogStatus _op_type) : mtime(_mtime), op_type(_op_type) {}
    virtual ~MutateParams() {}
  };

  enum Type {
    MDBE_SOBJ = 0,
    MDBE_OTP  = 1,
  };

  RGWSI_MetaBackend(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_MetaBackend() {}

  virtual Type get_type() = 0;

  virtual RGWSI_MetaBackend_Handler *alloc_be_handler() = 0;
  virtual int call_with_get_params(ceph::real_time *pmtime, std::function<int(RGWSI_MetaBackend::GetParams&)>) = 0;

  /* these should be implemented by backends */
  virtual int get_entry(RGWSI_MetaBackend::Context *ctx,
                        const std::string& key,
                        RGWSI_MetaBackend::GetParams& params,
                        RGWObjVersionTracker *objv_tracker,
                        optional_yield y,
                        const DoutPrefixProvider *dpp,
                        bool get_raw_attrs=false) = 0;
  virtual int put_entry(const DoutPrefixProvider *dpp, 
                        RGWSI_MetaBackend::Context *ctx,
                        const std::string& key,
                        RGWSI_MetaBackend::PutParams& params,
                        RGWObjVersionTracker *objv_tracker,
                        optional_yield y) = 0;
  virtual int remove_entry(const DoutPrefixProvider *dpp, 
                           Context *ctx,
                           const std::string& key,
                           RGWSI_MetaBackend::RemoveParams& params,
                           RGWObjVersionTracker *objv_tracker,
                           optional_yield y) = 0;

  virtual int list_init(const DoutPrefixProvider *dpp, RGWSI_MetaBackend::Context *ctx, const std::string& marker) = 0;
  virtual int list_next(const DoutPrefixProvider *dpp,
                        RGWSI_MetaBackend::Context *ctx,
                        int max, std::list<std::string> *keys,
                        bool *truncated)  = 0;
  virtual int list_get_marker(RGWSI_MetaBackend::Context *ctx,
                              std::string *marker) = 0;

  int call(std::function<int(RGWSI_MetaBackend::Context *)> f) {
    return call(std::nullopt, f);
  }

  virtual int call(std::optional<RGWSI_MetaBackend_CtxParams> opt,
                   std::function<int(RGWSI_MetaBackend::Context *)> f) = 0;

  virtual int get_shard_id(RGWSI_MetaBackend::Context *ctx,
			   const std::string& key,
			   int *shard_id) = 0;

  /* higher level */
  virtual int get(Context *ctx,
                  const std::string& key,
                  GetParams &params,
                  RGWObjVersionTracker *objv_tracker,
                  optional_yield y,
                  const DoutPrefixProvider *dpp,
                  bool get_raw_attrs=false);

  virtual int put(Context *ctx,
                  const std::string& key,
                  PutParams& params,
                  RGWObjVersionTracker *objv_tracker,
                  optional_yield y,
                  const DoutPrefixProvider *dpp);

  virtual int remove(Context *ctx,
                     const std::string& key,
                     RemoveParams& params,
                     RGWObjVersionTracker *objv_tracker,
                     optional_yield y,
                     const DoutPrefixProvider *dpp);

  virtual int mutate(Context *ctx,
                     const std::string& key,
                     MutateParams& params,
		     RGWObjVersionTracker *objv_tracker,
                     optional_yield y,
                     std::function<int()> f,
                     const DoutPrefixProvider *dpp);
};

class RGWSI_MetaBackend_Handler {
  RGWSI_MetaBackend *be{nullptr};

public:
  class Op {
    friend class RGWSI_MetaBackend_Handler;

    RGWSI_MetaBackend *be;
    RGWSI_MetaBackend::Context *be_ctx;

    Op(RGWSI_MetaBackend *_be,
       RGWSI_MetaBackend::Context *_ctx) : be(_be), be_ctx(_ctx) {}

  public:
    RGWSI_MetaBackend::Context *ctx() {
      return be_ctx;
    }

    int get(const std::string& key,
            RGWSI_MetaBackend::GetParams &params,
            RGWObjVersionTracker *objv_tracker,
            optional_yield y, const DoutPrefixProvider *dpp) {
      return be->get(be_ctx, key, params, objv_tracker, y, dpp);
    }

    int put(const std::string& key,
            RGWSI_MetaBackend::PutParams& params,
            RGWObjVersionTracker *objv_tracker,
            optional_yield y, const DoutPrefixProvider *dpp) {
      return be->put(be_ctx, key, params, objv_tracker, y, dpp);
    }

    int remove(const std::string& key,
               RGWSI_MetaBackend::RemoveParams& params,
               RGWObjVersionTracker *objv_tracker,
               optional_yield y, const DoutPrefixProvider *dpp) {
      return be->remove(be_ctx, key, params, objv_tracker, y, dpp);
    }

    int mutate(const std::string& key,
	       RGWSI_MetaBackend::MutateParams& params,
	       RGWObjVersionTracker *objv_tracker,
               optional_yield y,
	       std::function<int()> f,
               const DoutPrefixProvider *dpp) {
      return be->mutate(be_ctx, key, params, objv_tracker, y, f, dpp);
    }

    int list_init(const DoutPrefixProvider *dpp, const std::string& marker) {
      return be->list_init(dpp, be_ctx, marker);
    }
    int list_next(const DoutPrefixProvider *dpp, int max, std::list<std::string> *keys,
                  bool *truncated) {
      return be->list_next(dpp, be_ctx, max, keys, truncated);
    }
    int list_get_marker(std::string *marker) {
      return be->list_get_marker(be_ctx, marker);
    }

    int get_shard_id(const std::string& key, int *shard_id) {
      return be->get_shard_id(be_ctx, key, shard_id);
    }
  };

  class Op_ManagedCtx : public Op {
    std::unique_ptr<RGWSI_MetaBackend::Context> pctx;
  public:
    Op_ManagedCtx(RGWSI_MetaBackend_Handler *handler);
  };

  RGWSI_MetaBackend_Handler(RGWSI_MetaBackend *_be) : be(_be) {}
  virtual ~RGWSI_MetaBackend_Handler() {}

  int call(std::function<int(Op *)> f) {
    return call(std::nullopt, f);
  }

  virtual int call(std::optional<RGWSI_MetaBackend_CtxParams> bectx_params,
                   std::function<int(Op *)> f);
};

