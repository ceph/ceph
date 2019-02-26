// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_OTP_H
#define CEPH_RGW_OTP_H

class RGWRados;

class RGWMetadataHandler;

RGWMetadataHandler *rgw_otp_get_handler(void);
void rgw_otp_init(RGWRados *store);

#endif

