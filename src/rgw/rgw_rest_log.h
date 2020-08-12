// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

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

#pragma once

#include "rgw_metadata.h"
#include "rgw_mdlog.h"

class RGWOp_BILog_List : public RGWRESTOp {
  bool sent_header;
public:
  RGWOp_BILog_List() : sent_header(false) {}
  ~RGWOp_BILog_List() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("bilog", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void send_response(const Span& parent_span = nullptr) override;
  virtual void send_response(list<rgw_bi_log_entry>& entries, string& marker);
  virtual void send_response_end();
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "list_bucket_index_log";
  }
};

class RGWOp_BILog_Info : public RGWRESTOp {
  string bucket_ver;
  string master_ver;
  string max_marker;
  bool syncstopped;
public:
  RGWOp_BILog_Info() : bucket_ver(), master_ver(), syncstopped(false) {}
  ~RGWOp_BILog_Info() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("bilog", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void send_response(const Span& parent_span = nullptr) override;
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "bucket_index_log_info";
  }
};

class RGWOp_BILog_Delete : public RGWRESTOp {
public:
  RGWOp_BILog_Delete() {}
  ~RGWOp_BILog_Delete() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("bilog", RGW_CAP_WRITE);
  }
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "trim_bucket_index_log";
  }
};

class RGWOp_MDLog_List : public RGWRESTOp {
  list<cls_log_entry> entries;
  string last_marker;
  bool truncated;
public:
  RGWOp_MDLog_List() : truncated(false) {}
  ~RGWOp_MDLog_List() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("mdlog", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void execute(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "list_metadata_log";
  }
};

class RGWOp_MDLog_Info : public RGWRESTOp {
  unsigned num_objects;
  RGWPeriodHistory::Cursor period;
public:
  RGWOp_MDLog_Info() : num_objects(0) {}
  ~RGWOp_MDLog_Info() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("mdlog", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void execute(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "get_metadata_log_info";
  }
};

class RGWOp_MDLog_ShardInfo : public RGWRESTOp {
  RGWMetadataLogInfo info;
public:
  RGWOp_MDLog_ShardInfo() {}
  ~RGWOp_MDLog_ShardInfo() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("mdlog", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void execute(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "get_metadata_log_shard_info";
  }
};

class RGWOp_MDLog_Lock : public RGWRESTOp {
public:
  RGWOp_MDLog_Lock() {}
  ~RGWOp_MDLog_Lock() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "lock_mdlog_object";
  }
};

class RGWOp_MDLog_Unlock : public RGWRESTOp {
public:
  RGWOp_MDLog_Unlock() {}
  ~RGWOp_MDLog_Unlock() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "unlock_mdlog_object";
  }
};

class RGWOp_MDLog_Notify : public RGWRESTOp {
public:
  RGWOp_MDLog_Notify() {}
  ~RGWOp_MDLog_Notify() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "mdlog_notify";
  }
};

class RGWOp_MDLog_Delete : public RGWRESTOp {
public:
  RGWOp_MDLog_Delete() {}
  ~RGWOp_MDLog_Delete() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("mdlog", RGW_CAP_WRITE);
  }
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override {
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
  ~RGWOp_DATALog_List() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("datalog", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void execute(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "list_data_changes_log";
  }
};

class RGWOp_DATALog_Info : public RGWRESTOp {
  unsigned num_objects;
public:
  RGWOp_DATALog_Info() : num_objects(0) {}
  ~RGWOp_DATALog_Info() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("datalog", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void execute(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "get_data_changes_log_info";
  }
};

class RGWOp_DATALog_ShardInfo : public RGWRESTOp {
  RGWDataChangesLogInfo info;
public:
  RGWOp_DATALog_ShardInfo() {}
  ~RGWOp_DATALog_ShardInfo() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("datalog", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void execute(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "get_data_changes_log_shard_info";
  }
};

class RGWOp_DATALog_Notify : public RGWRESTOp {
public:
  RGWOp_DATALog_Notify() {}
  ~RGWOp_DATALog_Notify() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("datalog", RGW_CAP_WRITE);
  }
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "datalog_notify";
  }
};

class RGWOp_DATALog_Delete : public RGWRESTOp {
public:
  RGWOp_DATALog_Delete() {}
  ~RGWOp_DATALog_Delete() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("datalog", RGW_CAP_WRITE);
  }
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override {
    return "trim_data_changes_log";
  }
};

class RGWHandler_Log : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
  RGWOp *op_delete() override;
  RGWOp *op_post() override;

  int read_permissions(RGWOp*, const Span& parent_span = nullptr) override {
    return 0;
  }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_Log() override = default;
};

class RGWRESTMgr_Log : public RGWRESTMgr {
public:
  RGWRESTMgr_Log() = default;
  ~RGWRESTMgr_Log() override = default;

  RGWHandler_REST* get_handler(rgw::sal::RGWRadosStore *store,
			       struct req_state* const,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& frontend_prefixs) override {
    return new RGWHandler_Log(auth_registry);
  }
};
