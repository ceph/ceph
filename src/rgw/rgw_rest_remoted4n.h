// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw/rgw_rest.h"
#include "rgw/rgw_auth_s3.h"

class RGWOp_RemoteD4N_Get : public RGWGetObj {
public:
  RGWOp_RemoteD4N_Get() {}
  ~RGWOp_RemoteD4N_Get() override {}

  int get_params(optional_yield y) override {return 0; }
  int send_response_data_error(optional_yield y) override;
  int send_response_data(bufferlist& bl, off_t ofs, off_t len) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_remoted4n"; }
};

/*
class RGWOp_RemoteD4N_Get_Myself : public RGWOp_RemoteD4N_Get {
public:
  RGWOp_RemoteD4N_Get_Myself() {}
  ~RGWOp_RemoteD4N_Get_Myself() override {}

  void execute(optional_yield y) override;
};
*/

class RGWOp_RemoteD4N_Put : public RGWRESTOp {
  int get_data(bufferlist& bl);
  std::string update_status;
  obj_version ondisk_version;
public:
  RGWOp_RemoteD4N_Put() {}
  ~RGWOp_RemoteD4N_Put() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("remoted4n", RGW_CAP_WRITE);
  }
  void execute(optional_yield y) override;
  void send_response() override;
  const char* name() const override { return "set_remoted4n"; }
  RGWOpType get_type() override { return RGW_OP_PUT_OBJ; } // Sam: is this correct? 
};

class RGWOp_RemoteD4N_Delete : public RGWRESTOp {
public:
  RGWOp_RemoteD4N_Delete() {}
  ~RGWOp_RemoteD4N_Delete() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("remoted4n", RGW_CAP_WRITE);
  }
  void execute(optional_yield y) override;
  const char* name() const override { return "remove_remoted4n"; }
};

class RGWHandler_RemoteD4N : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
  RGWOp *op_put() override;
  RGWOp *op_delete() override;

  int read_permissions(RGWOp*, optional_yield y) override {
    return 0;
  }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_RemoteD4N() override = default;
};

class RGWRESTMgr_RemoteD4N : public RGWRESTMgr {
public:
  RGWRESTMgr_RemoteD4N() = default;
  ~RGWRESTMgr_RemoteD4N() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state* const s,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& frontend_prefix) override;
};
