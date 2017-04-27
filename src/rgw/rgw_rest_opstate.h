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

#ifndef RGW_REST_OPSTATE_H
#define RGW_REST_OPSTATE_H

class RGWOp_Opstate_List : public RGWRESTOp {
  bool sent_header;
public:
  RGWOp_Opstate_List() : sent_header(false) {}
  ~RGWOp_Opstate_List() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("opstate", RGW_CAP_READ);
  }
  int verify_permission() override {
    return check_caps(s->user->caps);
  }
  void execute() override;
  void send_response() override;
  virtual void send_response(list<cls_statelog_entry> entries);
  virtual void send_response_end();
  const string name() override {
    return "opstate_list";
  }
};

class RGWOp_Opstate_Set : public RGWRESTOp {
public:
  RGWOp_Opstate_Set() {}
  ~RGWOp_Opstate_Set() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("opstate", RGW_CAP_WRITE);
  }
  void execute() override;
  const string name() override {
    return "set_opstate";
  }
};

class RGWOp_Opstate_Renew : public RGWRESTOp {
public:
  RGWOp_Opstate_Renew() {}
  ~RGWOp_Opstate_Renew() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("opstate", RGW_CAP_WRITE);
  }
  void execute() override;
  const string name() override {
    return "renew_opstate";
  }
};

class RGWOp_Opstate_Delete : public RGWRESTOp {
public:
  RGWOp_Opstate_Delete() {}
  ~RGWOp_Opstate_Delete() override {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("opstate", RGW_CAP_WRITE);
  }
  void execute() override;
  const string name() override {
    return "delete_opstate";
  }
};

class RGWHandler_Opstate : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override {
    return new RGWOp_Opstate_List;
  }
  RGWOp *op_delete() override {
    return new RGWOp_Opstate_Delete;
  }
  RGWOp *op_post() override;

  int read_permissions(RGWOp*) override {
    return 0;
  }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_Opstate() override = default;
};

class RGWRESTMgr_Opstate : public RGWRESTMgr {
public:
  RGWRESTMgr_Opstate() = default;
  ~RGWRESTMgr_Opstate() override = default;

  RGWHandler_REST* get_handler(struct req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_Opstate(auth_registry);
  }
};

#endif /*!RGW_REST_OPSTATE_H*/
