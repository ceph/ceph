
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

#include <memory>
#include "rgw_service.h"

class RGWMetadataLister;

class RGWSI_Bucket : public RGWServiceInstance
{
public:
  RGWSI_Bucket(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_Bucket() {}

  static std::string get_entrypoint_meta_key(const rgw_bucket& bucket);
  static std::string get_bi_meta_key(const rgw_bucket& bucket);

  virtual int create_entrypoint_lister(const DoutPrefixProvider* dpp,
                                       const std::string& marker,
                                       std::unique_ptr<RGWMetadataLister>& lister) = 0;
  virtual int create_instance_lister(const DoutPrefixProvider* dpp,
                                     const std::string& marker,
                                     std::unique_ptr<RGWMetadataLister>& lister) = 0;

  virtual int read_bucket_entrypoint_info(const std::string& key,
                                          RGWBucketEntryPoint *entry_point,
                                          RGWObjVersionTracker *objv_tracker,
                                          real_time *pmtime,
                                          std::map<std::string, bufferlist> *pattrs,
                                          optional_yield y,
                                          const DoutPrefixProvider *dpp,
                                          rgw_cache_entry_info *cache_info = nullptr,
                                          boost::optional<obj_version> refresh_version = boost::none) = 0;

  virtual int store_bucket_entrypoint_info(const std::string& key,
                                   RGWBucketEntryPoint& info,
                                   bool exclusive,
                                   real_time mtime,
                                   const std::map<std::string, bufferlist> *pattrs,
                                   RGWObjVersionTracker *objv_tracker,
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp) = 0;

  virtual int remove_bucket_entrypoint_info(const std::string& key,
                                    RGWObjVersionTracker *objv_tracker,
                                    optional_yield y,
                                    const DoutPrefixProvider *dpp) = 0;

  virtual int read_bucket_instance_info(const std::string& key,
                                RGWBucketInfo *info,
                                real_time *pmtime,
                                std::map<std::string, bufferlist> *pattrs,
                                optional_yield y,
                                const DoutPrefixProvider *dpp,
                                rgw_cache_entry_info *cache_info = nullptr,
                                boost::optional<obj_version> refresh_version = boost::none) = 0;

  virtual int read_bucket_info(const rgw_bucket& bucket,
                       RGWBucketInfo *info,
                       real_time *pmtime,
                       std::map<std::string, bufferlist> *pattrs,
                       boost::optional<obj_version> refresh_version,
                       optional_yield y,
                       const DoutPrefixProvider *dpp) = 0;

  virtual int store_bucket_instance_info(const std::string& key,
                                 RGWBucketInfo& info,
                                 std::optional<RGWBucketInfo *> orig_info, /* nullopt: orig_info was not fetched,
                                                                              nullptr: orig_info was not found (new bucket instance */
                                 bool exclusive,
                                 real_time mtime,
                                 const std::map<std::string, bufferlist> *pattrs,
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp) = 0;

  virtual int remove_bucket_instance_info(const std::string& key,
				  const RGWBucketInfo& bucket_info,
                                  RGWObjVersionTracker *objv_tracker,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp) = 0;

  virtual int read_bucket_stats(const rgw_bucket& bucket,
                        RGWBucketEnt *ent,
                        optional_yield y,
                        const DoutPrefixProvider *dpp) = 0;

  virtual int read_buckets_stats(std::vector<RGWBucketEnt>& buckets,
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp) = 0;
};

