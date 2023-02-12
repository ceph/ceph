// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_sal_fwd.h"
#include "cls/otp/cls_otp_types.h"
#include "services/svc_meta_be_otp.h"

#include "rgw_basic_types.h"
#include "rgw_metadata.h"


class RGWObjVersionTracker;
class RGWMetadataHandler;
class RGWOTPMetadataHandler;
class RGWSI_Zone;
class RGWSI_OTP;
class RGWSI_MetaBackend;

class RGWOTPMetadataHandlerBase : public RGWMetadataHandler_GenericMetaBE {
public:
  virtual ~RGWOTPMetadataHandlerBase() {}
  virtual int init(RGWSI_Zone *zone,
		   RGWSI_MetaBackend *_meta_be,
		   RGWSI_OTP *_otp) = 0;
};

class RGWOTPMetaHandlerAllocator {
public:
  static RGWMetadataHandler *alloc();
};

struct RGWOTPInfo {
  rgw_user uid;
  otp_devices_list_t devices;
};


class RGWOTPCtl
{
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_OTP *otp{nullptr};
  } svc;

  RGWOTPMetadataHandler *meta_handler;
  RGWSI_MetaBackend_Handler *be_handler;
  
public:
  RGWOTPCtl(RGWSI_Zone *zone_svc,
	    RGWSI_OTP *otp_svc);

  void init(RGWOTPMetadataHandler *_meta_handler);

  struct GetParams {
    RGWObjVersionTracker *objv_tracker{nullptr};
    ceph::real_time *mtime{nullptr};

    GetParams() {}

    GetParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
      objv_tracker = _objv_tracker;
      return *this;
    }

    GetParams& set_mtime(ceph::real_time *_mtime) {
      mtime = _mtime;
      return *this;
    }
  };

  struct PutParams {
    RGWObjVersionTracker *objv_tracker{nullptr};
    ceph::real_time mtime;

    PutParams() {}

    PutParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
      objv_tracker = _objv_tracker;
      return *this;
    }

    PutParams& set_mtime(const ceph::real_time& _mtime) {
      mtime = _mtime;
      return *this;
    }
  };

  struct RemoveParams {
    RGWObjVersionTracker *objv_tracker{nullptr};

    RemoveParams() {}

    RemoveParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
      objv_tracker = _objv_tracker;
      return *this;
    }
  };

  int read_all(const rgw_user& uid, RGWOTPInfo *info, optional_yield y,
               const DoutPrefixProvider *dpp,
               const GetParams& params = {});
  int store_all(const DoutPrefixProvider *dpp, 
                const RGWOTPInfo& info, optional_yield y,
                const PutParams& params = {});
  int remove_all(const DoutPrefixProvider *dpp, 
                 const rgw_user& user, optional_yield y,
                 const RemoveParams& params = {});
};
