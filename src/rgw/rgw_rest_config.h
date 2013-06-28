// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#ifndef CEPH_RGW_REST_CONFIG_H
#define CEPH_RGW_REST_CONFIG_H

class RGWOp_RegionMap_Get : public RGWRESTOp {
  RGWRegionMap regionmap;
public:
  RGWOp_RegionMap_Get() {}
  ~RGWOp_RegionMap_Get() {}

  int verify_permission() {
    return 0; 
  }
  void execute();
  virtual void send_response();
  virtual const char *name() {
    return "get_region_map";
  }
};

class RGWHandler_Config : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get();

  int read_permissions(RGWOp*) {
    return 0;
  }
public:
  RGWHandler_Config() : RGWHandler_Auth_S3() {}
  virtual ~RGWHandler_Config() {}
};

class RGWRESTMgr_Config : public RGWRESTMgr {
public:
  RGWRESTMgr_Config() {}
  virtual ~RGWRESTMgr_Config() {}

  virtual RGWHandler *get_handler(struct req_state *s){
    return new RGWHandler_Config;
  }
};

#endif
