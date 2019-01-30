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

namespace rgw {
inline auto log_lock_name = "rgw_log_lock"sv;
}

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
    ClsSubService(CephContext *cct, boost::asio::io_context& ioc)
      : RGWServiceInstance(cct, ioc) {}
  };

public:
  class MFA : public ClsSubService {
    tl::expected<RGWSI_RADOS::Obj, boost::system::error_code>
    get_mfa_obj(const rgw_user& user, optional_yield y);

    void prepare_mfa_write(RADOS::WriteOp& op,
			   RGWObjVersionTracker *objv_tracker,
			   ceph::real_time mtime);


  public:
    MFA(CephContext *cct, boost::asio::io_context& ioc)
      : ClsSubService(cct, ioc) {}

    std::string get_mfa_oid(const rgw_user& user) {
      return string("user:") + user.to_str();
    }

    boost::system::error_code
    check_mfa(const rgw_user& user, std::string_view otp_id,
	      std::string_view pin, optional_yield y);
    boost::system::error_code
    create_mfa(const rgw_user& user, const rados::cls::otp::otp_info_t& config,
	       RGWObjVersionTracker *objv_tracker, ceph::real_time mtime,
	       optional_yield y);
    boost::system::error_code
    remove_mfa(const rgw_user& user, const string& id,
	       RGWObjVersionTracker *objv_tracker,
	       ceph::real_time mtime, optional_yield y);
    tl::expected<rados::cls::otp::otp_info_t,
		 boost::system::error_code>
    get_mfa(const rgw_user& user, std::string_view id, optional_yield y);
    tl::expected<std::vector<rados::cls::otp::otp_info_t>,
		 boost::system::error_code>
    list_mfa(const rgw_user& user, optional_yield y);
    tl::expected<ceph::real_time, boost::system::error_code>
    otp_get_current_time(const rgw_user& user, optional_yield y);

    boost::system::error_code
    set_mfa(string_view oid,
	    const std::vector<rados::cls::otp::otp_info_t>& entries,
	    bool reset_obj, RGWObjVersionTracker *objv_tracker,
	    const real_time& mtime, optional_yield y);

    tl::expected<std::pair<std::vector<rados::cls::otp::otp_info_t>,
			   ceph::real_time>, boost::system::error_code>
    list_mfa(std::string_view oid, RGWObjVersionTracker *objv_tracker,
	     optional_yield y);

  } mfa;

  class TimeLog : public ClsSubService {
    tl::expected<RGWSI_RADOS::Obj, boost::system::error_code>
    init_obj(std::string_view oid, optional_yield y);

  public:
    TimeLog(CephContext *cct, boost::asio::io_context& ioc)
      : ClsSubService(cct, ioc) {}

    boost::system::error_code
    add(std::string_view oid, real_time t,
	std::string_view section,
	std::string_view key, bufferlist&& bl,
	optional_yield y);

    boost::system::error_code
    add(std::string_view oid,
	std::vector<cls_log_entry>&& entries,
	bool monotonic_inc, optional_yield y);

    tl::expected<std::tuple<std::vector<cls_log_entry>, std::string, bool>,
		 boost::system::error_code>
    list(std::string_view oid, ceph::real_time start_time,
	 ceph::real_time end_time, int max_entries,
	 std::string_view marker, optional_yield y);

    tl::expected<cls_log_header, boost::system::error_code>
    info(std::string_view oid, optional_yield y);

    boost::system::error_code
    trim(std::string_view oid, ceph::real_time start_time,
	 ceph::real_time end_time, std::string_view from_marker,
	 std::string_view to_marker, optional_yield y);
  } timelog;

  RGWSI_Cls(CephContext *cct, boost::asio::io_context& ioc)
    : RGWServiceInstance(cct, ioc), mfa(cct, ioc), timelog(cct, ioc) {}

  void init(RGWSI_Zone *_zone_svc, RGWSI_RADOS *_rados_svc) {
    rados_svc = _rados_svc;
    zone_svc = _zone_svc;

    mfa.init(this, zone_svc, rados_svc);
    timelog.init(this, zone_svc, rados_svc);
  }

  boost::system::error_code do_start() override;
  RGWSI_RADOS* rados() {
    return rados_svc;
  }
};
