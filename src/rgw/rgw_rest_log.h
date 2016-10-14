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

#ifndef RGW_REST_LOG_H
#define RGW_REST_LOG_H

#include "rgw_metadata.h"

class RGWOp_BILog_List : public RGWRESTOp {
  bool sent_header;
public:
  RGWOp_BILog_List() : sent_header(false) {}
  ~RGWOp_BILog_List() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("bilog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  virtual void send_response();
  virtual void send_response(list<rgw_bi_log_entry>& entries, string& marker);
  virtual void send_response_end();
  void execute();
  virtual const string name() {
    return "list_bucket_index_log";
  }
};

class RGWOp_BILog_Info : public RGWRESTOp {
  string bucket_ver;
  string master_ver;
  string max_marker;
public:
  RGWOp_BILog_Info() : bucket_ver(), master_ver() {}
  ~RGWOp_BILog_Info() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("bilog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  virtual void send_response();
  void execute();
  virtual const string name() {
    return "bucket_index_log_info";
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
  virtual const string name() {
    return "trim_bucket_index_log";
  }
};

class RGWOp_MDLog_List : public RGWRESTOp {
  list<cls_log_entry> entries;
  string last_marker;
  bool truncated;
public:
  RGWOp_MDLog_List() : truncated(false) {}
  ~RGWOp_MDLog_List() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  void execute();
  virtual void send_response();
  virtual const string name() {
    return "list_metadata_log";
  }
};

class RGWOp_MDLog_Info : public RGWRESTOp {
  unsigned num_objects;
  RGWPeriodHistory::Cursor period;
public:
  RGWOp_MDLog_Info() : num_objects(0) {}
  ~RGWOp_MDLog_Info() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  void execute();
  virtual void send_response();
  virtual const string name() {
    return "get_metadata_log_info";
  }
};

class RGWOp_MDLog_ShardInfo : public RGWRESTOp {
  RGWMetadataLogInfo info;
public:
  RGWOp_MDLog_ShardInfo() {}
  ~RGWOp_MDLog_ShardInfo() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  void execute();
  virtual void send_response();
  virtual const string name() {
    return "get_metadata_log_shard_info";
  }
};

class RGWOp_MDLog_Lock : public RGWRESTOp {
public:
  RGWOp_MDLog_Lock() {}
  ~RGWOp_MDLog_Lock() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "lock_mdlog_object";
  }
};

class RGWOp_MDLog_Unlock : public RGWRESTOp {
public:
  RGWOp_MDLog_Unlock() {}
  ~RGWOp_MDLog_Unlock() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "unlock_mdlog_object";
  }
};

class RGWOp_MDLog_Notify : public RGWRESTOp {
public:
  RGWOp_MDLog_Notify() {}
  ~RGWOp_MDLog_Notify() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "mdlog_notify";
  }
};

class RGWOp_MDLog_Delete : public RGWRESTOp {
public:
  RGWOp_MDLog_Delete() {}
  ~RGWOp_MDLog_Delete() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "trim_metadata_log";
  }
};

class RGWOp_DATALog_List : public RGWRESTOp {
  list<rgw_data_change_log_entry> entries;
  string last_marker;
  bool truncated;
  bool extra_info;
public:
  RGWOp_DATALog_List() : truncated(false), extra_info(false) {}
  ~RGWOp_DATALog_List() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("datalog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  void execute();
  virtual void send_response();
  virtual const string name() {
    return "list_data_changes_log";
  }
};

class RGWOp_DATALog_Info : public RGWRESTOp {
  unsigned num_objects;
public:
  RGWOp_DATALog_Info() : num_objects(0) {}
  ~RGWOp_DATALog_Info() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("datalog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  void execute();
  virtual void send_response();
  virtual const string name() {
    return "get_data_changes_log_info";
  }
};

class RGWOp_DATALog_ShardInfo : public RGWRESTOp {
  RGWDataChangesLogInfo info;
public:
  RGWOp_DATALog_ShardInfo() {}
  ~RGWOp_DATALog_ShardInfo() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("datalog", RGW_CAP_READ);
  }
  int verify_permission() {
    return check_caps(s->user->caps);
  }
  void execute();
  virtual void send_response();
  virtual const string name() {
    return "get_data_changes_log_shard_info";
  }
};

class RGWOp_DATALog_Lock : public RGWRESTOp {
public:
  RGWOp_DATALog_Lock() {}
  ~RGWOp_DATALog_Lock() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("datalog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "lock_datalog_object";
  }
};

class RGWOp_DATALog_Unlock : public RGWRESTOp {
public:
  RGWOp_DATALog_Unlock() {}
  ~RGWOp_DATALog_Unlock() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("datalog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "unlock_datalog_object";
  }
};

class RGWOp_DATALog_Notify : public RGWRESTOp {
public:
  RGWOp_DATALog_Notify() {}
  ~RGWOp_DATALog_Notify() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("datalog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "datalog_notify";
  }
};

class RGWOp_DATALog_Delete : public RGWRESTOp {
public:
  RGWOp_DATALog_Delete() {}
  ~RGWOp_DATALog_Delete() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("datalog", RGW_CAP_WRITE);
  }
  void execute();
  virtual const string name() {
    return "trim_data_changes_log";
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

  virtual RGWHandler_REST* get_handler(struct req_state *s){
    return new RGWHandler_Log;
  }
};

#endif /* RGW_REST_LOG_H */
