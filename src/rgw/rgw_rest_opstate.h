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
  ~RGWOp_Opstate_List() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("opstate", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  void execute();
  virtual void send_response();
  virtual void send_response(list<cls_statelog_entry> entries);
  virtual void send_response_end();
  virtual const string name() {
    return "opstate_list";
  }
};

class RGWOp_Opstate_Set : public RGWRESTOp {
public:
  RGWOp_Opstate_Set() {}
  ~RGWOp_Opstate_Set() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("opstate", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "set_opstate";
  }
};

class RGWOp_Opstate_Renew : public RGWRESTOp {
public:
  RGWOp_Opstate_Renew() {}
  ~RGWOp_Opstate_Renew() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("opstate", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "renew_opstate";
  }
};

class RGWOp_Opstate_Delete : public RGWRESTOp {
public:
  RGWOp_Opstate_Delete() {}
  ~RGWOp_Opstate_Delete() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("opstate", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "delete_opstate";
  }
};

class RGWHandler_Opstate : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() {
    return new RGWOp_Opstate_List;
  }
  RGWOp *op_delete() {
    return new RGWOp_Opstate_Delete;
  }
  RGWOp *op_post();

  int read_permissions(RGWOp*) {
    return 0;
  }
public:
  RGWHandler_Opstate() : RGWHandler_Auth_S3() {}
  virtual ~RGWHandler_Opstate() {}
};

class RGWRESTMgr_Opstate : public RGWRESTMgr {
public:
  RGWRESTMgr_Opstate() = default;
  virtual ~RGWRESTMgr_Opstate() = default;

  RGWHandler_REST* get_handler(struct req_state*,
                               const std::string&) override {
    return new RGWHandler_Opstate;
  }
};

#endif /*!RGW_REST_OPSTATE_H*/
