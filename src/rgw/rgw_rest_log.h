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
#ifndef CEPH_RGW_REST_LOG_H
#define CEPH_RGW_REST_LOG_H

class RGWOp_BILog_List : public RGWRESTOp {
  int http_ret;
  bool sent_header;
public:
  RGWOp_BILog_List() : http_ret(0), sent_header(false) {}
  ~RGWOp_BILog_List() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("bilog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user.caps);
  }
  virtual void send_response();
  virtual void send_response(list<rgw_bi_log_entry>& entries, string& marker);
  virtual void send_response_end();
  void execute();
  virtual const char *name() {
    return "list bucket index log";
  }
};

class RGWOp_BILog_Delete : public RGWRESTOp {
public:
  RGWOp_BILog_Delete() {}
  ~RGWOp_BILog_Delete() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("bilog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const char *name() {
    return "trim_bucket_index_log";
  }
};

class RGWOp_MDLog_List : public RGWRESTOp {
  list<cls_log_entry> entries;
  int http_ret;
public:
  RGWOp_MDLog_List() : http_ret(0) {}
  ~RGWOp_MDLog_List() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user.caps);
  }
  void execute();
  virtual void send_response();
  virtual const char *name() {
    return "list_metadata_log";
  }
};

class RGWOp_MDLog_GetShardsInfo : public RGWRESTOp {
  unsigned num_objects;
  int http_ret;
public:
  RGWOp_MDLog_GetShardsInfo() : num_objects(0), http_ret(0) {}
  ~RGWOp_MDLog_GetShardsInfo() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user.caps);
  }
  void execute();
  virtual void send_response();
  virtual const char *name() {
    return "get_metadata_log_shards_info";
  }
};

class RGWOp_MDLog_Post : public RGWRESTOp {
  enum {
    MDLOG_POST_INVALID = 0,
    MDLOG_POST_LOCK,
    MDLOG_POST_UNLOCK
  };
  int get_post_type() {
    bool exists;
    s->info.args.get("lock", &exists);
    if (exists) 
      return MDLOG_POST_LOCK;
    s->info.args.get("unlock", &exists);
    if (exists)
      return MDLOG_POST_UNLOCK;
    return MDLOG_POST_INVALID;
  }
public:
  RGWOp_MDLog_Post() {}
  ~RGWOp_MDLog_Post() {}

  int check_caps(RGWUserCaps& caps);
  void execute();
  virtual const char *name();
};

class RGWOp_MDLog_Delete : public RGWRESTOp {
public:
  RGWOp_MDLog_Delete() {}
  ~RGWOp_MDLog_Delete() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const char *name() {
    return "trim_metadata_log";
  }
};

class RGWHandler_Log : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get();
  RGWOp *op_delete();
  RGWOp *op_post();

  int read_permissions(RGWOp*) {
    return 0;
  }
public:
  RGWHandler_Log() : RGWHandler_Auth_S3() {}
  virtual ~RGWHandler_Log() {}
};

class RGWRESTMgr_Log : public RGWRESTMgr {
public:
  RGWRESTMgr_Log() {}
  virtual ~RGWRESTMgr_Log() {}

  virtual RGWHandler *get_handler(struct req_state *s){
    return new RGWHandler_Log;
  }
};

#endif

