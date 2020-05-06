
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

#include "rgw/rgw_service.h"

class RGWUserBuckets;
class RGWGetUserStats_CB;

class RGWSI_User : public RGWServiceInstance
{
public:
  RGWSI_User(CephContext *cct);
  virtual ~RGWSI_User();

  static string get_meta_key(const rgw_user& user) {
    return user.to_str();
  }

  static rgw_user user_from_meta_key(const string& key) {
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
                             map<string, bufferlist> * const pattrs,
                             optional_yield y) = 0;

  virtual int store_user_info(RGWSI_MetaBackend::Context *ctx,
                              const RGWUserInfo& info,
                              RGWUserInfo *old_info,
                              RGWObjVersionTracker *objv_tracker,
                              const real_time& mtime,
                              bool exclusive,
                              map<string, bufferlist> *attrs,
                              optional_yield y) = 0;

  virtual int remove_user_info(RGWSI_MetaBackend::Context *ctx,
                               const RGWUserInfo& info,
                               RGWObjVersionTracker *objv_tracker,
                               optional_yield y) = 0;

  virtual int get_user_info_by_email(RGWSI_MetaBackend::Context *ctx,
                             const string& email, RGWUserInfo *info,
                             RGWObjVersionTracker *objv_tracker,
                             real_time *pmtime,
                             optional_yield y) = 0;
  virtual int get_user_info_by_swift(RGWSI_MetaBackend::Context *ctx,
                             const string& swift_name,
                             RGWUserInfo *info,        /* out */
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             optional_yield y) = 0;
  virtual int get_user_info_by_access_key(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& access_key,
                                  RGWUserInfo *info,
                                  RGWObjVersionTracker* objv_tracker,
                                  real_time *pmtime,
                                  optional_yield y) = 0;

  virtual int add_bucket(RGWSI_MetaBackend::Context *ctx,
                         const rgw_user& user,
                         const rgw_bucket& bucket,
                         ceph::real_time creation_time,
                         bool identity_type_role = false) = 0;
  virtual int remove_bucket(RGWSI_MetaBackend::Context *ctx,
                            const rgw_user& user,
                            const rgw_bucket& _bucket,
                            bool identity_type_role = false) = 0;
  virtual int list_buckets(RGWSI_MetaBackend::Context *ctx,
                           const rgw_user& user,
                           const string& marker,
                           const string& end_marker,
                           uint64_t max,
                           RGWUserBuckets *buckets,
                           bool *is_truncated,
                           bool identity_type_role = false) = 0;

  virtual int flush_bucket_stats(RGWSI_MetaBackend::Context *ctx,
                                 const rgw_user& user,
                                 const RGWBucketEnt& ent,
                                 bool identity_type_role = false) = 0;
  virtual int complete_flush_stats(RGWSI_MetaBackend::Context *ctx,
				   const rgw_user& user) = 0;
  virtual int reset_bucket_stats(RGWSI_MetaBackend::Context *ctx,
				 const rgw_user& user) = 0;
  virtual int read_stats(RGWSI_MetaBackend::Context *ctx,
			 const rgw_user& user, RGWStorageStats *stats,
			 ceph::real_time *last_stats_sync,         /* last time a full stats sync completed */
			 ceph::real_time *last_stats_update) = 0;  /* last time a stats update was done */

  virtual int read_stats_async(RGWSI_MetaBackend::Context *ctx,
			       const rgw_user& user, RGWGetUserStats_CB *cb) = 0;
};

