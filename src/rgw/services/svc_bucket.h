
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

#include "rgw/rgw_service.h"

#include "svc_bucket_types.h"

class RGWSI_Bucket : public RGWServiceInstance
{
public:
  RGWSI_Bucket(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_Bucket() {}

  static string get_entrypoint_meta_key(const rgw_bucket& bucket);
  static string get_bi_meta_key(const rgw_bucket& bucket);

  virtual RGWSI_Bucket_BE_Handler& get_ep_be_handler() = 0;
  virtual RGWSI_BucketInstance_BE_Handler& get_bi_be_handler() = 0;

  virtual int read_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                          const string& key,
                                          RGWBucketEntryPoint *entry_point,
                                          RGWObjVersionTracker *objv_tracker,
                                          real_time *pmtime,
                                          map<string, bufferlist> *pattrs,
                                          optional_yield y,
                                          rgw_cache_entry_info *cache_info = nullptr,
                                          boost::optional<obj_version> refresh_version = boost::none) = 0;

  virtual int store_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                   const string& key,
                                   RGWBucketEntryPoint& info,
                                   bool exclusive,
                                   real_time mtime,
                                   map<string, bufferlist> *pattrs,
                                   RGWObjVersionTracker *objv_tracker,
                                   optional_yield y, const Span& parent_span = nullptr) = 0;

  virtual int remove_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                    const string& key,
                                    RGWObjVersionTracker *objv_tracker,
                                    optional_yield y) = 0;

  virtual int read_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                const string& key,
                                RGWBucketInfo *info,
                                real_time *pmtime,
                                map<string, bufferlist> *pattrs,
                                optional_yield y,
                                rgw_cache_entry_info *cache_info = nullptr,
                                boost::optional<obj_version> refresh_version = boost::none) = 0;

  virtual int read_bucket_info(RGWSI_Bucket_X_Ctx& ep_ctx,
                       const rgw_bucket& bucket,
                       RGWBucketInfo *info,
                       real_time *pmtime,
                       map<string, bufferlist> *pattrs,
                       boost::optional<obj_version> refresh_version,
                       optional_yield y) = 0;

  virtual int store_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                 const string& key,
                                 RGWBucketInfo& info,
                                 std::optional<RGWBucketInfo *> orig_info, /* nullopt: orig_info was not fetched,
                                                                              nullptr: orig_info was not found (new bucket instance */
                                 bool exclusive,
                                 real_time mtime,
                                 map<string, bufferlist> *pattrs,
                                 optional_yield y, const Span& parent_span = nullptr) = 0;

  virtual int remove_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                  const string& key,
				  const RGWBucketInfo& bucket_info,
                                  RGWObjVersionTracker *objv_tracker,
                                  optional_yield y) = 0;

  virtual int read_bucket_stats(RGWSI_Bucket_X_Ctx& ctx,
                        const rgw_bucket& bucket,
                        RGWBucketEnt *ent,
                        optional_yield y) = 0;

  virtual int read_buckets_stats(RGWSI_Bucket_X_Ctx& ctx,
                                 map<string, RGWBucketEnt>& m,
                                 optional_yield y) = 0;
};

