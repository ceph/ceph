
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

#define RGW_BUCKET_INSTANCE_MD_PREFIX ".bucket.meta."

class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;

struct rgw_cache_entry_info;

template <class T>
class RGWChainedCacheImpl;

class RGWSI_Bucket : public RGWServiceInstance
{
  friend class Instance;

  RGWSI_Zone *zone_svc{nullptr};
  RGWSI_SysObj *sysobj_svc{nullptr};
  RGWSI_SysObj_Cache *cache_svc{nullptr};

  struct bucket_info_cache_entry {
    RGWBucketInfo info;
    real_time mtime;
    map<string, bufferlist> attrs;
  };

  using RGWChainedCacheImpl_bucket_info_cache_entry = RGWChainedCacheImpl<bucket_info_cache_entry>;
  unique_ptr<RGWChainedCacheImpl_bucket_info_cache_entry> binfo_cache;

  int do_start() override;
public:
  RGWSI_Bucket(CephContext *cct);
  ~RGWSI_Bucket();

  void init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc, RGWSI_SysObj_Cache *_cache_svc);

  class Instance {
    friend class Op;

    RGWSI_Bucket *bucket_svc;
    RGWSysObjectCtx& ctx;
    rgw_bucket bucket;
    RGWBucketInfo bucket_info;

    static string get_meta_oid(const rgw_bucket& bucket) {
      return RGW_BUCKET_INSTANCE_MD_PREFIX + bucket.get_key(':');
    }

    int read_bucket_entrypoint_info(const rgw_bucket& bucket,
                                    RGWBucketEntryPoint *entry_point,
                                    RGWObjVersionTracker *objv_tracker,
                                    real_time *pmtime,
                                    map<string, bufferlist> *pattrs,
                                    rgw_cache_entry_info *cache_info,
                                    boost::optional<obj_version> refresh_version);

    int read_bucket_info(const rgw_raw_obj& obj,
                         RGWBucketInfo *info,
                         real_time *pmtime, map<string, bufferlist> *pattrs,
                         rgw_cache_entry_info *cache_info,
                         boost::optional<obj_version> refresh_version);

    int read_bucket_instance_info(const rgw_bucket& bucket,
                                  RGWBucketInfo *info,
                                  real_time *pmtime,
                                  map<string, bufferlist> *pattrs,
                                  rgw_cache_entry_info *cache_info,
                                  boost::optional<obj_version> refresh_version);

    int read_bucket_info(const rgw_bucket& bucket,
                         RGWBucketInfo *info,
                         real_time *pmtime,
                         map<string, bufferlist> *pattrs,
                         boost::optional<obj_version> refresh_version);

  public:
    Instance(RGWSI_Bucket *_bucket_svc,
         RGWSysObjectCtx& _ctx,
         const string& _tenant,
         const string& _bucket_name) : bucket_svc(_bucket_svc),
                                       ctx(_ctx) {
      bucket.tenant = _tenant;
      bucket.name = _bucket_name;
    }

    Instance(RGWSI_Bucket *_bucket_svc,
             RGWSysObjectCtx& _ctx,
             const rgw_bucket& _bucket) : bucket_svc(_bucket_svc),
                                          ctx(_ctx),
                                          bucket(_bucket) {}

    RGWSysObjectCtx& get_ctx() {
      return ctx;
    }

    rgw_bucket& get_bucket() {
      return bucket;
    }

    RGWBucketInfo& get_bucket_info() {
      return bucket_info;
    }

    struct GetOp {
      Instance& source;

      ceph::real_time *pmtime{nullptr};
      map<string, bufferlist> *pattrs{nullptr};
      boost::optional<obj_version> refresh_version;
      RGWBucketInfo *pinfo{nullptr};


      GetOp& set_mtime(ceph::real_time *_mtime) {
        pmtime = _mtime;
        return *this;
      }

      GetOp& set_attrs(map<string, bufferlist> *_attrs) {
        pattrs = _attrs;
        return *this;
      }

      GetOp& set_refresh_version(const obj_version& rf) {
        refresh_version = rf;
        return *this;
      }

      GetOp& set_pinfo(RGWBucketInfo *_pinfo) {
        pinfo = _pinfo;
        return *this;
      }

      GetOp(Instance& _source) : source(_source) {}

      int exec();
    };

    GetOp get_op() {
      return GetOp(*this);
    }
  };

  Instance instance(RGWSysObjectCtx& _ctx,
                    const string& _tenant,
                    const string& _bucket_name);

  Instance instance(RGWSysObjectCtx& _ctx,
                    const rgw_bucket& _bucket);
};
