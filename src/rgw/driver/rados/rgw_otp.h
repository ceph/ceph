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
