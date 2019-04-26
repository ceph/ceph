// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_OTP_H
#define CEPH_RGW_OTP_H

class RGWRados;

class RGWMetadataHandler;

RGWMetadataHandler *rgw_otp_get_handler(void);

class RGWOTPMetaHandlerAllocator {
public:
  static RGWMetadataHandler *alloc(RGWSI_Zone *zone_svc, RGWSI_MetaBackend *meta_be_svc);
};


#endif

