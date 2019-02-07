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

#include "cls/otp/cls_otp_types.h"

#include "rgw/rgw_service.h"

#include "svc_rados.h"


class RGWSI_MFA : public RGWServiceInstance
{
  RGWSI_Zone *zone_svc{nullptr};
  RGWSI_RADOS *rados_svc{nullptr};

  int get_mfa_obj(const rgw_user& user, std::optional<RGWSI_RADOS::Obj> *obj);
  int get_mfa_ref(const rgw_user& user, rgw_rados_ref *ref);

  void prepare_mfa_write(librados::ObjectWriteOperation *op,
                         RGWObjVersionTracker *objv_tracker,
                         const ceph::real_time& mtime);

public:
  RGWSI_MFA(CephContext *cct): RGWServiceInstance(cct) {}

  void init(RGWSI_Zone *_zone_svc, RGWSI_RADOS *_rados_svc) {
    rados_svc = _rados_svc;
    zone_svc = _zone_svc;
  }

  string get_mfa_oid(const rgw_user& user) {
    return string("user:") + user.to_str();
  }

  int check_mfa(const rgw_user& user, const string& otp_id, const string& pin, optional_yield y);
  int create_mfa(const rgw_user& user, const rados::cls::otp::otp_info_t& config,
                 RGWObjVersionTracker *objv_tracker, const ceph::real_time& mtime, optional_yield y);
  int remove_mfa(const rgw_user& user, const string& id,
                 RGWObjVersionTracker *objv_tracker,
                 const ceph::real_time& mtime,
                 optional_yield y);
  int get_mfa(const rgw_user& user, const string& id, rados::cls::otp::otp_info_t *result, optional_yield y);
  int list_mfa(const rgw_user& user, list<rados::cls::otp::otp_info_t> *result, optional_yield y);
  int otp_get_current_time(const rgw_user& user, ceph::real_time *result, optional_yield y);
  int set_mfa(const string& oid, const list<rados::cls::otp::otp_info_t>& entries,
              bool reset_obj, RGWObjVersionTracker *objv_tracker,
              const real_time& mtime, optional_yield y);
  int list_mfa(const string& oid, list<rados::cls::otp::otp_info_t> *result,
               RGWObjVersionTracker *objv_tracker, ceph::real_time *pmtime, optional_yield y);
};



