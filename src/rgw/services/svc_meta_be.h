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

#include "rgw/rgw_service.h"
#include "rgw/rgw_mdlog_types.h"

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
  struct Context;
protected:
  RGWSI_MDLog *mdlog_svc{nullptr};

  void base_init(RGWSI_MDLog *_mdlog_svc) {
    mdlog_svc = _mdlog_svc;
  }

  boost::system::error_code prepare_mutate(RGWSI_MetaBackend::Context *ctx,
                     const std::string& key,
                     const ceph::real_time& mtime,
                     RGWObjVersionTracker *objv_tracker,
                     optional_yield y);

  virtual boost::system::error_code do_mutate(Context *ctx,
                     const std::string& key,
                     const ceph::real_time& mtime, RGWObjVersionTracker *objv_tracker,
                     RGWMDLogStatus op_type,
                     optional_yield y,
                     std::function<boost::system::error_code()> f,
                     bool generic_prepare);

  virtual boost::system::error_code pre_modify(Context *ctx,
                         const std::string& key,
                         RGWMetadataLogData& log_data,
                         RGWObjVersionTracker *objv_tracker,
                         RGWMDLogStatus op_type,
                         optional_yield y);
  virtual boost::system::error_code
  post_modify(Context *ctx,
              const std::string& key,
              RGWMetadataLogData& log_data,
              RGWObjVersionTracker *objv_tracker, boost::system::error_code ret,
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

  RGWSI_MetaBackend(CephContext *cct, boost::asio::io_context& ioctx)
    : RGWServiceInstance(cct, ioctx) {}
  virtual ~RGWSI_MetaBackend() {}

  virtual Type get_type() = 0;

  virtual RGWSI_MetaBackend_Handler *alloc_be_handler() = 0;
  virtual boost::system::error_code
  call_with_get_params(ceph::real_time *pmtime,
                       std::function<boost::system::error_code(RGWSI_MetaBackend::GetParams&)>) = 0;

  /* these should be implemented by backends */
  virtual boost::system::error_code get_entry(RGWSI_MetaBackend::Context *ctx,
                        const std::string& key,
                        RGWSI_MetaBackend::GetParams& params,
                        RGWObjVersionTracker *objv_tracker,
                        optional_yield y) = 0;
  virtual boost::system::error_code put_entry(RGWSI_MetaBackend::Context *ctx,
                        const std::string& key,
                        RGWSI_MetaBackend::PutParams& params,
                        RGWObjVersionTracker *objv_tracker,
                        optional_yield y) = 0;
  virtual boost::system::error_code list_init(RGWSI_MetaBackend::Context *ctx,
					      std::optional<string> marker) = 0;
  virtual boost::system::error_code list_next(RGWSI_MetaBackend::Context *ctx,
					      int max,
					      std::vector<string> *keys,
					      bool *truncated)  = 0;
  virtual std::string list_get_marker(RGWSI_MetaBackend::Context *ctx) = 0;
  virtual boost::system::error_code
  remove_entry(Context *ctx, const std::string& key,
               RGWSI_MetaBackend::RemoveParams& params,
               RGWObjVersionTracker *objv_tracker,
               optional_yield y) = 0;

  boost::system::error_code call(std::function<boost::system::error_code(RGWSI_MetaBackend::Context *)> f) {
    return call(nullopt, f);
  }

  virtual boost::system::error_code call(std::optional<RGWSI_MetaBackend_CtxParams> opt,
                   std::function<boost::system::error_code(RGWSI_MetaBackend::Context *)> f) = 0;

  virtual int get_shard_id(RGWSI_MetaBackend::Context *ctx,
                           const std::string& key) = 0;

  /* higher level */
  virtual boost::system::error_code get(Context *ctx,
                  const std::string& key,
                  GetParams &params,
                  RGWObjVersionTracker *objv_tracker,
                  optional_yield y);

  virtual boost::system::error_code put(Context *ctx,
                  const std::string& key,
                  PutParams& params,
                  RGWObjVersionTracker *objv_tracker,
                  optional_yield y);

  virtual boost::system::error_code remove(Context *ctx,
                     const std::string& key,
                     RemoveParams& params,
                     RGWObjVersionTracker *objv_tracker,
                     optional_yield y);

  virtual boost::system::error_code mutate(Context *ctx,
                     const std::string& key,
                     MutateParams& params,
		     RGWObjVersionTracker *objv_tracker,
                     optional_yield y,
                     std::function<boost::system::error_code()> f);
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

    boost::system::error_code get(const std::string& key,
            RGWSI_MetaBackend::GetParams &params,
            RGWObjVersionTracker *objv_tracker,
            optional_yield y) {
      return be->get(be_ctx, key, params, objv_tracker, y);
    }

    boost::system::error_code put(const std::string& key,
            RGWSI_MetaBackend::PutParams& params,
            RGWObjVersionTracker *objv_tracker,
            optional_yield y) {
      return be->put(be_ctx, key, params, objv_tracker, y);
    }

    boost::system::error_code remove(const std::string& key,
               RGWSI_MetaBackend::RemoveParams& params,
               RGWObjVersionTracker *objv_tracker,
               optional_yield y) {
      return be->remove(be_ctx, key, params, objv_tracker, y);
    }

    boost::system::error_code mutate(const std::string& key,
	       RGWSI_MetaBackend::MutateParams& params,
	       RGWObjVersionTracker *objv_tracker,
               optional_yield y,
	       std::function<boost::system::error_code()> f) {
      return be->mutate(be_ctx, key, params, objv_tracker, y, f);
    }

    boost::system::error_code list_init(std::optional<string> marker) {
      return be->list_init(be_ctx, marker);
    }
    boost::system::error_code list_next(int max, std::vector <string> *keys,
					bool *truncated) {
      return be->list_next(be_ctx, max, keys, truncated);
    }
    std::string list_get_marker() {
      return be->list_get_marker(be_ctx);
    }
    int get_shard_id(const std::string& key) {
      return be->get_shard_id(be_ctx, key);
    }
  };

  class Op_ManagedCtx : public Op {
    std::unique_ptr<RGWSI_MetaBackend::Context> pctx;
  public:
    Op_ManagedCtx(RGWSI_MetaBackend_Handler *handler);
  };

  RGWSI_MetaBackend_Handler(RGWSI_MetaBackend *_be) : be(_be) {}
  virtual ~RGWSI_MetaBackend_Handler() {}

  boost::system::error_code call(std::function<boost::system::error_code(Op *)> f) {
    return call(nullopt, f);
  }

  virtual boost::system::error_code
  call(std::optional<RGWSI_MetaBackend_CtxParams> bectx_params,
       std::function<boost::system::error_code(Op *)> f);
};
