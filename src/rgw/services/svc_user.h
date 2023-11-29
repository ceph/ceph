
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

#include "svc_meta_be.h"

#include "rgw_service.h"
#include "rgw_sal_fwd.h"

class RGWUserBuckets;

class RGWSI_User : public RGWServiceInstance
{
public:
  RGWSI_User(CephContext *cct);
  virtual ~RGWSI_User();

  static std::string get_meta_key(const rgw_user& user) {
    return user.to_str();
  }

  static rgw_user user_from_meta_key(const std::string& key) {
    return rgw_user(key);
  }

  virtual RGWSI_MetaBackend_Handler *get_be_handler() = 0;

  /* base svc_user interfaces */

  virtual int read_user_info(RGWSI_MetaBackend::Context *ctx,
                             const rgw_user& user,
                             RGWUserInfo *info,
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             rgw_cache_entry_info * const cache_info,
                             std::map<std::string, bufferlist> * const pattrs,
                             optional_yield y,
                             const DoutPrefixProvider *dpp) = 0;

  virtual int store_user_info(RGWSI_MetaBackend::Context *ctx,
                              const RGWUserInfo& info,
                              RGWUserInfo *old_info,
                              RGWObjVersionTracker *objv_tracker,
                              const real_time& mtime,
                              bool exclusive,
                              std::map<std::string, bufferlist> *attrs,
                              optional_yield y,
                              const DoutPrefixProvider *dpp) = 0;

  virtual int remove_user_info(RGWSI_MetaBackend::Context *ctx,
                               const RGWUserInfo& info,
                               RGWObjVersionTracker *objv_tracker,
                               optional_yield y,
                               const DoutPrefixProvider *dpp) = 0;

  virtual int get_user_info_by_email(RGWSI_MetaBackend::Context *ctx,
                             const std::string& email, RGWUserInfo *info,
                             RGWObjVersionTracker *objv_tracker,
                             real_time *pmtime,
                             optional_yield y,
                             const DoutPrefixProvider *dpp) = 0;
  virtual int get_user_info_by_swift(RGWSI_MetaBackend::Context *ctx,
                             const std::string& swift_name,
                             RGWUserInfo *info,        /* out */
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             optional_yield y,
                             const DoutPrefixProvider *dpp) = 0;
  virtual int get_user_info_by_access_key(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& access_key,
                                  RGWUserInfo *info,
                                  RGWObjVersionTracker* objv_tracker,
                                  real_time *pmtime,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp) = 0;

  virtual int add_bucket(const DoutPrefixProvider *dpp, 
                         const rgw_user& user,
                         const rgw_bucket& bucket,
                         ceph::real_time creation_time,
                         optional_yield y) = 0;
  virtual int remove_bucket(const DoutPrefixProvider *dpp, 
                            const rgw_user& user,
                            const rgw_bucket& _bucket, optional_yield) = 0;
  virtual int list_buckets(const DoutPrefixProvider *dpp, 
                           const rgw_user& user,
                           const std::string& marker,
                           const std::string& end_marker,
                           uint64_t max,
                           RGWUserBuckets *buckets,
                           bool *is_truncated,
                           optional_yield y) = 0;

  virtual int flush_bucket_stats(const DoutPrefixProvider *dpp, 
                                 const rgw_user& user,
                                 const RGWBucketEnt& ent, optional_yield y) = 0;
  virtual int complete_flush_stats(const DoutPrefixProvider *dpp,
				   const rgw_user& user, optional_yield y) = 0;
  virtual int reset_bucket_stats(const DoutPrefixProvider *dpp, 
				 const rgw_user& user,
                                 optional_yield y) = 0;
  virtual int read_stats(const DoutPrefixProvider *dpp, 
                         RGWSI_MetaBackend::Context *ctx,
			 const rgw_user& user, RGWStorageStats *stats,
			 ceph::real_time *last_stats_sync,         /* last time a full stats sync completed */
			 ceph::real_time *last_stats_update,
                         optional_yield y) = 0;  /* last time a stats update was done */

  virtual int read_stats_async(const DoutPrefixProvider *dpp,
			       const rgw_user& user,
			       boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb) = 0;
};

