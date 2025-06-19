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

#include "rgw_service.h"

#include "driver/rados/rgw_tools.h"


class RGWSI_Cls : public RGWServiceInstance
{
  RGWSI_Zone *zone_svc{nullptr};
  librados::Rados* rados{nullptr};

  class ClsSubService : public RGWServiceInstance {
    friend class RGWSI_Cls;

    RGWSI_Cls *cls{nullptr};

    void init(RGWSI_Cls *cls_) {
      cls = cls_;
    }

  public:
    ClsSubService(CephContext *cct) : RGWServiceInstance(cct) {}
  };

public:
  class MFA : public ClsSubService {
    int get_mfa_ref(const DoutPrefixProvider *dpp, const rgw_user& user, rgw_rados_ref *ref);

    void prepare_mfa_write(librados::ObjectWriteOperation *op,
			   RGWObjVersionTracker *objv_tracker,
			   const ceph::real_time& mtime);

  public:
    MFA(CephContext *cct): ClsSubService(cct) {}

    std::string get_mfa_oid(const rgw_user& user) {
      return std::string("user:") + user.to_str();
    }

    int check_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, const std::string& otp_id, const std::string& pin, optional_yield y);
    int create_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, const rados::cls::otp::otp_info_t& config,
		   RGWObjVersionTracker *objv_tracker, const ceph::real_time& mtime, optional_yield y);
    int remove_mfa(const DoutPrefixProvider *dpp, 
                   const rgw_user& user, const std::string& id,
		   RGWObjVersionTracker *objv_tracker,
		   const ceph::real_time& mtime,
		   optional_yield y);
    int get_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, const std::string& id, rados::cls::otp::otp_info_t *result, optional_yield y);
    int list_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, std::list<rados::cls::otp::otp_info_t> *result, optional_yield y);
    int otp_get_current_time(const DoutPrefixProvider *dpp, const rgw_user& user, ceph::real_time *result, optional_yield y);
    int set_mfa(const DoutPrefixProvider *dpp, const std::string& oid, const std::list<rados::cls::otp::otp_info_t>& entries,
		bool reset_obj, RGWObjVersionTracker *objv_tracker,
		const real_time& mtime, optional_yield y);
    int list_mfa(const DoutPrefixProvider *dpp, const std::string& oid, std::list<rados::cls::otp::otp_info_t> *result,
		 RGWObjVersionTracker *objv_tracker, ceph::real_time *pmtime, optional_yield y);
  } mfa;

  class TimeLog : public ClsSubService {
    int init_obj(const DoutPrefixProvider *dpp, const std::string& oid, rgw_rados_ref& obj);
  public:
    TimeLog(CephContext *cct): ClsSubService(cct) {}

    void prepare_entry(cls::log::entry& entry,
                       const real_time& ut,
                       const std::string& section,
                       const std::string& key,
                       bufferlist& bl);
    int add(const DoutPrefixProvider *dpp, 
            const std::string& oid,
            const real_time& ut,
            const std::string& section,
            const std::string& key,
            bufferlist& bl,
            optional_yield y);
    int add(const DoutPrefixProvider *dpp, 
            const std::string& oid,
            std::vector<cls::log::entry>& entries,
            librados::AioCompletion *completion,
            bool monotonic_inc,
            optional_yield y);
    int list(const DoutPrefixProvider *dpp, 
             const std::string& oid,
             const real_time& start_time,
             const real_time& end_time,
             int max_entries, std::vector<cls::log::entry>& entries,
             const std::string& marker,
             std::string *out_marker,
             bool *truncated,
             optional_yield y);
    int info(const DoutPrefixProvider *dpp, 
             const std::string& oid,
             cls::log::header *header,
             optional_yield y);
    int info_async(const DoutPrefixProvider *dpp,
                   rgw_rados_ref& obj,
                   const std::string& oid,
                   cls::log::header *header,
                   librados::AioCompletion *completion);
    int trim(const DoutPrefixProvider *dpp, 
             const std::string& oid,
             const real_time& start_time,
             const real_time& end_time,
             const std::string& from_marker,
             const std::string& to_marker,
             librados::AioCompletion *completion,
             optional_yield y);
  } timelog;

  class Lock : public ClsSubService {
    int init_obj(const std::string& oid, rgw_rados_ref& obj);
    public:
    Lock(CephContext *cct): ClsSubService(cct) {}
    int lock_exclusive(const DoutPrefixProvider *dpp,
                       const rgw_pool& pool,
                       const std::string& oid,
                       timespan& duration,
                       std::string& zone_id,
                       std::string& owner_id,
                       std::optional<std::string> lock_name = std::nullopt);
    int unlock(const DoutPrefixProvider *dpp,
               const rgw_pool& pool,
               const std::string& oid,
               std::string& zone_id,
               std::string& owner_id,
               std::optional<std::string> lock_name = std::nullopt);
  } lock;

  RGWSI_Cls(CephContext *cct): RGWServiceInstance(cct), mfa(cct), timelog(cct), lock(cct) {}

  void init(RGWSI_Zone *_zone_svc, librados::Rados* rados_) {
    rados = rados_;
    zone_svc = _zone_svc;

    mfa.init(this);
    timelog.init(this);
    lock.init(this);
  }

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;
};
