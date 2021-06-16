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

#include "cls/otp/cls_otp_types.h"
#include "cls/log/cls_log_types.h"

#include "rgw/rgw_service.h"

#include "svc_rados.h"


class RGWSI_Cls : public RGWServiceInstance
{
  RGWSI_Zone *zone_svc{nullptr};
  RGWSI_RADOS *rados_svc{nullptr};

  class ClsSubService : public RGWServiceInstance {
    friend class RGWSI_Cls;

    RGWSI_Cls *cls_svc{nullptr};
    RGWSI_Zone *zone_svc{nullptr};
    RGWSI_RADOS *rados_svc{nullptr};

    void init(RGWSI_Cls *_cls_svc, RGWSI_Zone *_zone_svc, RGWSI_RADOS *_rados_svc) {
      cls_svc = _cls_svc;
      zone_svc = _cls_svc->zone_svc;
      rados_svc = _cls_svc->rados_svc;
    }

  public:
    ClsSubService(CephContext *cct) : RGWServiceInstance(cct) {}
  };

public:
  class MFA : public ClsSubService {
    int get_mfa_obj(const DoutPrefixProvider *dpp, const rgw_user& user, std::optional<RGWSI_RADOS::Obj> *obj);
    int get_mfa_ref(const DoutPrefixProvider *dpp, const rgw_user& user, rgw_rados_ref *ref);

    void prepare_mfa_write(librados::ObjectWriteOperation *op,
			   RGWObjVersionTracker *objv_tracker,
			   const ceph::real_time& mtime);

  public:
    MFA(CephContext *cct): ClsSubService(cct) {}

    string get_mfa_oid(const rgw_user& user) {
      return string("user:") + user.to_str();
    }

    int check_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, const string& otp_id, const string& pin, optional_yield y);
    int create_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, const rados::cls::otp::otp_info_t& config,
		   RGWObjVersionTracker *objv_tracker, const ceph::real_time& mtime, optional_yield y);
    int remove_mfa(const DoutPrefixProvider *dpp, 
                   const rgw_user& user, const string& id,
		   RGWObjVersionTracker *objv_tracker,
		   const ceph::real_time& mtime,
		   optional_yield y);
    int get_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, const string& id, rados::cls::otp::otp_info_t *result, optional_yield y);
    int list_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, list<rados::cls::otp::otp_info_t> *result, optional_yield y);
    int otp_get_current_time(const DoutPrefixProvider *dpp, const rgw_user& user, ceph::real_time *result, optional_yield y);
    int set_mfa(const DoutPrefixProvider *dpp, const string& oid, const list<rados::cls::otp::otp_info_t>& entries,
		bool reset_obj, RGWObjVersionTracker *objv_tracker,
		const real_time& mtime, optional_yield y);
    int list_mfa(const DoutPrefixProvider *dpp, const string& oid, list<rados::cls::otp::otp_info_t> *result,
		 RGWObjVersionTracker *objv_tracker, ceph::real_time *pmtime, optional_yield y);
  } mfa;

  class TimeLog : public ClsSubService {
    int init_obj(const DoutPrefixProvider *dpp, const string& oid, RGWSI_RADOS::Obj& obj);
  public:
    TimeLog(CephContext *cct): ClsSubService(cct) {}

    void prepare_entry(cls_log_entry& entry,
                       const real_time& ut,
                       const string& section,
                       const string& key,
                       bufferlist& bl);
    int add(const DoutPrefixProvider *dpp, 
            const string& oid,
            const real_time& ut,
            const string& section,
            const string& key,
            bufferlist& bl,
            optional_yield y);
    int add(const DoutPrefixProvider *dpp, 
            const string& oid,
            std::list<cls_log_entry>& entries,
            librados::AioCompletion *completion,
            bool monotonic_inc,
            optional_yield y);
    int list(const DoutPrefixProvider *dpp, 
             const string& oid,
             const real_time& start_time,
             const real_time& end_time,
             int max_entries, list<cls_log_entry>& entries,
             const string& marker,
             string *out_marker,
             bool *truncated,
             optional_yield y);
    int info(const DoutPrefixProvider *dpp, 
             const string& oid,
             cls_log_header *header,
             optional_yield y);
    int info_async(const DoutPrefixProvider *dpp,
                   RGWSI_RADOS::Obj& obj,
                   const string& oid,
                   cls_log_header *header,
                   librados::AioCompletion *completion);
    int trim(const DoutPrefixProvider *dpp, 
             const string& oid,
             const real_time& start_time,
             const real_time& end_time,
             const string& from_marker,
             const string& to_marker,
             librados::AioCompletion *completion,
             optional_yield y);
  } timelog;

  class Lock : public ClsSubService {
    int init_obj(const string& oid, RGWSI_RADOS::Obj& obj);
    public:
    Lock(CephContext *cct): ClsSubService(cct) {}
    int lock_exclusive(const DoutPrefixProvider *dpp,
                       const rgw_pool& pool,
                       const string& oid,
                       timespan& duration,
                       string& zone_id,
                       string& owner_id,
                       std::optional<string> lock_name = std::nullopt);
    int unlock(const DoutPrefixProvider *dpp,
               const rgw_pool& pool,
               const string& oid,
               string& zone_id,
               string& owner_id,
               std::optional<string> lock_name = std::nullopt);
  } lock;

  RGWSI_Cls(CephContext *cct): RGWServiceInstance(cct), mfa(cct), timelog(cct), lock(cct) {}

  void init(RGWSI_Zone *_zone_svc, RGWSI_RADOS *_rados_svc) {
    rados_svc = _rados_svc;
    zone_svc = _zone_svc;

    mfa.init(this, zone_svc, rados_svc);
    timelog.init(this, zone_svc, rados_svc);
    lock.init(this, zone_svc, rados_svc);
  }

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;
};

