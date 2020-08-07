// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"

#include "svc_sip_marker.h"

class RGWSI_SIP_Marker_SObj : public RGWSI_SIP_Marker
{
  struct {
    RGWSI_Zone *zone;
    RGWSI_SysObj *sysobj;
  } svc;

public:
  RGWSI_SIP_Marker_SObj(CephContext *cct) : RGWSI_SIP_Marker(cct) {}

  void init(RGWSI_Zone *_zone_svc,
            RGWSI_SysObj *_sysobj_svc);

  RGWSI_SIP_Marker::HandlerRef get_handler(SIProviderRef& sip) override;
};


