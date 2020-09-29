// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_OTP_H
#define CEPH_RGW_OTP_H

namespace rgw { namespace sal {
class RGWRadosStore;
} }

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

  struct GetParams : public BaseGetParams<GetParams> {};
  struct PutParams : public  BasePutParams<PutParams> {};
  struct RemoveParams : public BaseRemoveParams<RemoveParams> {};

  int read_all(const rgw_user& uid, RGWOTPInfo *info, optional_yield y,
               const GetParams& params = {});
  int store_all(const RGWOTPInfo& info, optional_yield y,
                const PutParams& params = {});
  int remove_all(const rgw_user& user, optional_yield y,
                 const RemoveParams& params = {});
};

#endif

