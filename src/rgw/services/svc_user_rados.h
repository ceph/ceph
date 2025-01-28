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

#include "rgw_service.h"

#include "svc_user.h"

#include "driver/rados/rgw_bucket.h" // FIXME: subclass dependency

class RGWSI_MDLog;
class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWSI_SyncModules;

struct rgw_cache_entry_info;

class RGWGetUserHeader_CB;

template <class T>
class RGWChainedCacheImpl;

class RGWSI_User_RADOS : public RGWSI_User
{
  friend class PutOperation;

  struct user_info_cache_entry {
    RGWUserInfo info;
    RGWObjVersionTracker objv_tracker;
    std::map<std::string, bufferlist> attrs;
    real_time mtime;
  };

  using RGWChainedCacheImpl_user_info_cache_entry = RGWChainedCacheImpl<user_info_cache_entry>;
  std::unique_ptr<RGWChainedCacheImpl_user_info_cache_entry> uinfo_cache;

  rgw_raw_obj get_buckets_obj(const rgw_user& user_id) const override;

  int get_user_info_from_index(const std::string& key,
                               const rgw_pool& pool,
                               RGWUserInfo *info,
                               RGWObjVersionTracker * const objv_tracker,
                               std::map<std::string, bufferlist>* pattrs,
                               real_time * const pmtime,
                               optional_yield y,
                               const DoutPrefixProvider *dpp);

  int remove_uid_index(const RGWUserInfo& user_info, RGWObjVersionTracker *objv_tracker,
                       optional_yield y, const DoutPrefixProvider *dpp);

  int remove_key_index(const DoutPrefixProvider *dpp, const RGWAccessKey& access_key, optional_yield y);
  int remove_email_index(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y);
  int remove_swift_name_index(const DoutPrefixProvider *dpp, const std::string& swift_name, optional_yield y);

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;
public:
  librados::Rados* rados{nullptr};

  struct Svc {
    RGWSI_User_RADOS *user{nullptr};
    RGWSI_Zone *zone{nullptr};
    RGWSI_MDLog *mdlog{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
  } svc;

  RGWSI_User_RADOS(CephContext *cct);
  ~RGWSI_User_RADOS();

  void init(librados::Rados* rados_,
            RGWSI_Zone *_zone_svc,
            RGWSI_MDLog *mdlog_svc,
            RGWSI_SysObj *_sysobj_svc,
            RGWSI_SysObj_Cache *_cache_svc);

  int create_lister(const DoutPrefixProvider* dpp,
                    const std::string& marker,
                    std::unique_ptr<RGWMetadataLister>& lister) override;

  int read_user_info(const rgw_user& user,
                     RGWUserInfo *info,
                     RGWObjVersionTracker * const objv_tracker,
                     real_time * const pmtime,
                     rgw_cache_entry_info * const cache_info,
                     std::map<std::string, bufferlist> * const pattrs,
                     optional_yield y,
                     const DoutPrefixProvider *dpp) override;

  int store_user_info(const RGWUserInfo& info,
                      RGWUserInfo *old_info,
                      RGWObjVersionTracker *objv_tracker,
                      const real_time& mtime,
                      bool exclusive,
                      std::map<std::string, bufferlist> *attrs,
                      optional_yield y,
                      const DoutPrefixProvider *dpp) override;

  int remove_user_info(const RGWUserInfo& info,
                       RGWObjVersionTracker *objv_tracker,
                       optional_yield y,
                       const DoutPrefixProvider *dpp) override;

  int get_user_info_by_email(const std::string& email, RGWUserInfo *info,
                             RGWObjVersionTracker *objv_tracker,
                             std::map<std::string, bufferlist>* pattrs,
                             real_time *pmtime,
                             optional_yield y,
                             const DoutPrefixProvider *dpp) override;
  int get_user_info_by_swift(const std::string& swift_name,
                             RGWUserInfo *info,        /* out */
                             RGWObjVersionTracker * const objv_tracker,
                             std::map<std::string, bufferlist>* pattrs,
                             real_time * const pmtime,
                             optional_yield y,
                             const DoutPrefixProvider *dpp) override;
  int get_user_info_by_access_key(const std::string& access_key,
                                  RGWUserInfo *info,
                                  RGWObjVersionTracker* objv_tracker,
                                  std::map<std::string, bufferlist>* pattrs,
                                  real_time *pmtime,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp) override;

  int read_email_index(const DoutPrefixProvider* dpp, optional_yield y,
                       std::string_view email, RGWUID& uid) override;
};
