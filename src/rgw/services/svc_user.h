
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

struct RGWUID;

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

  virtual rgw_raw_obj get_buckets_obj(const rgw_user& user_id) const = 0;

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
                             std::map<std::string, bufferlist>* pattrs,
                             real_time *pmtime,
                             optional_yield y,
                             const DoutPrefixProvider *dpp) = 0;
  virtual int get_user_info_by_swift(RGWSI_MetaBackend::Context *ctx,
                             const std::string& swift_name,
                             RGWUserInfo *info,        /* out */
                             RGWObjVersionTracker * const objv_tracker,
                             std::map<std::string, bufferlist>* pattrs,
                             real_time * const pmtime,
                             optional_yield y,
                             const DoutPrefixProvider *dpp) = 0;
  virtual int get_user_info_by_access_key(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& access_key,
                                  RGWUserInfo *info,
                                  RGWObjVersionTracker* objv_tracker,
                                  std::map<std::string, bufferlist>* pattrs,
                                  real_time *pmtime,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp) = 0;
  virtual int read_email_index(const DoutPrefixProvider* dpp, optional_yield y,
                               std::string_view email, RGWUID& uid) = 0;
};

