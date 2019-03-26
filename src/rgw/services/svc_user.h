
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

class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWSI_Meta;
class RGWSI_SyncModules;
class RGWMetadataHandler;

struct rgw_cache_entry_info;

template <class T>
class RGWChainedCacheImpl;

class RGWSI_User : public RGWServiceInstance
{
  friend class User;

  struct Svc {
    RGWSI_User *user{nullptr};
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
    RGWSI_Meta *meta{nullptr};
  } svc;

  RGWMetadataHandler *user_meta_handler;

  struct user_info_cache_entry {
    RGWUserInfo info;
    RGWObjVersionTracker objv_tracker;
    real_time mtime;
  };

  using RGWChainedCacheImpl_user_info_cache_entry = RGWChainedCacheImpl<user_info_cache_entry>;
  unique_ptr<RGWChainedCacheImpl_user_info_cache_entry> uinfo_cache;

  int do_start() override;
public:
  RGWSI_User(CephContext *cct);
  ~RGWSI_User();

  void init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
	    RGWSI_SysObj_Cache *_cache_svc, RGWSI_Meta *_meta_svc,
	    RGWSI_SyncModules *_sync_modules);

  class User {
    friend class Op;

    RGWSI_User::Svc& svc;
    RGWSysObjectCtx& ctx;
    rgw_user user;
    RGWUserInfo user_info;

    static string get_meta_key(const rgw_user& user) {
      return user.to_str();
    }

  public:
    User(RGWSI_User *_user_svc,
         RGWSysObjectCtx& _ctx,
         const rgw_user& _user) : svc(_user_svc->svc),
                                  ctx(_ctx), user(_user) {}

    RGWSysObjectCtx& get_ctx() {
      return ctx;
    }

    rgw_user& get_user() {
      return user;
    }

    RGWUserInfo& get_user_info() {
      return user_info;
    }

    struct GetOp {
      User& source;

      ceph::real_time *pmtime{nullptr};
      map<string, bufferlist> *pattrs{nullptr};
      boost::optional<obj_version> refresh_version;
      RGWUserInfo *pinfo{nullptr};


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

      GetOp& set_pinfo(RGWUserInfo *_pinfo) {
        pinfo = _pinfo;
        return *this;
      }

      GetOp(User& _source) : source(_source) {}

      int exec();
    };

    struct SetOp {
      User& source;

      ceph::real_time mtime;
      map<string, bufferlist> *pattrs{nullptr};
      bool exclusive{false};


      SetOp& set_mtime(const ceph::real_time& _mtime) {
        mtime = _mtime;
        return *this;
      }

      SetOp& set_attrs(map<string, bufferlist> *_attrs) {
        pattrs = _attrs;
        return *this;
      }

      SetOp& set_exclusive(bool _exclusive) {
        exclusive = _exclusive;
        return *this;
      }

      SetOp(User& _source) : source(_source) {}

      int exec();
    };

    GetOp get_op() {
      return GetOp(*this);
    }

    SetOp set_op() {
      return SetOp(*this);
    }
  };

  User user(RGWSysObjectCtx& _ctx,
            const rgw_user& _user);

  int read_user_info(const rgw_user& user,
                     RGWUserInfo *info,
                     real_time *pmtime,
                     RGWObjVersionTracker *objv_tracker,
                     map<string, bufferlist> *pattrs,
                     rgw_cache_entry_info *cache_info);
  int store_user_info(RGWUserInfo& user_info, bool exclusive,
                      map<string, bufferlist>& attrs,
                      RGWObjVersionTracker *objv_tracker,
                      real_time mtime);
  int remove_user_info(const rgw_user& user,
                       RGWObjVersionTracker *objv_tracker);
};

