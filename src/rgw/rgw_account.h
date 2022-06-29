// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <atomic>
#include <vector>

#include "include/types.h"
#include "rgw_metadata.h"

class optional_yield;
class RGWRole;

class RGWSI_Account_RADOS;
class RGWFormatterFlusher;

std::string rgw_generate_account_id(CephContext* cct);
bool rgw_validate_account_id(std::string_view id, std::string& err_msg);
bool rgw_validate_account_name(std::string_view name, std::string& err_msg);

struct RGWAccountInfo {
  std::string id;
  std::string name;
  std::string tenant;

  static constexpr uint32_t DEFAULT_QUOTA_LIMIT = 1000;
  uint32_t max_users = DEFAULT_QUOTA_LIMIT;

  void encode(bufferlist& bl) const {
    ENCODE_START(1,1,bl);
    encode(id, bl);
    encode(name, bl);
    encode(tenant, bl);
    encode(max_users, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(name, bl);
    decode(tenant, bl);
    decode(max_users, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter * const f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWAccountInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWAccountInfo)

struct RGWAccountNameToId {
  std::string id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1,1,bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter * const f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWAccountNameToId*>& o);
};
WRITE_CLASS_ENCODER(RGWAccountNameToId)

class RGWAccountMetadataHandler;

using RGWAccountCompleteInfo = CompleteInfo<RGWAccountInfo>;

class RGWAccountMetadataObject : public RGWMetadataObject {
  RGWAccountCompleteInfo aci;
public:
  RGWAccountMetadataObject() = default;
  RGWAccountMetadataObject(const RGWAccountCompleteInfo& _aci,
			   const obj_version& v,
			   real_time m) : aci(_aci) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    aci.dump(f);
  }

  RGWAccountCompleteInfo& get_aci() {
    return aci;
  }
};

class RGWAccountMetadataHandler : public RGWMetadataHandler {
  RGWSI_Account_RADOS* svc_account = nullptr;
  RGWSI_SysObj* svc_sysobj = nullptr;
 public:
  RGWAccountMetadataHandler(RGWSI_Account_RADOS* svc_account,
                            RGWSI_SysObj* svc_sysobj);

  std::string get_type() override { return "account"; }

  RGWMetadataObject *get_meta_obj(JSONObj *jo,
                                  const obj_version& objv,
                                  const ceph::real_time& mtime) override;

  int get(std::string& entry, RGWMetadataObject** obj,
          optional_yield y, const DoutPrefixProvider* dpp) override;
  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv, optional_yield y,
          const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override;
  int remove(std::string& entry, RGWObjVersionTracker& objv,
             optional_yield y, const DoutPrefixProvider* dpp) override;

  int mutate(const std::string& entry,
             const ceph::real_time& mtime,
             RGWObjVersionTracker* objv,
             optional_yield y,
             const DoutPrefixProvider* dpp,
             RGWMDLogStatus op_type,
             std::function<int()> f) override;

  int list_keys_init(const DoutPrefixProvider* dpp,
                     const std::string& marker, void** phandle) override;
  int list_keys_next(const DoutPrefixProvider* dpp, void* handle, int max,
                     std::list<std::string>& keys, bool* truncated) override;
  void list_keys_complete(void* handle) override;

  std::string get_marker(void* handle) override;
};

struct RGWAccountAdminOpState
{
  RGWAccountInfo info;
  std::string account_id;
  std::string tenant;
  std::string account_name;
  uint32_t max_users = 0;
  RGWObjVersionTracker objv_tracker;

  std::string marker;
  std::optional<int> max_entries;

  bool has_account_id() const { return !account_id.empty(); }
  bool has_account_name() const { return !account_name.empty(); }

  void set_max_users(uint32_t _max_users) { max_users = _max_users;}
};

/*
 Wrappers to unify all the admin ops of creating & removing accounts
*/
class RGWAdminOp_Account
{
public:
  static int create(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
                    RGWAccountAdminOpState& op_state, std::string& err_msg,
                    RGWFormatterFlusher& flusher, optional_yield y);

  static int modify(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
                    RGWAccountAdminOpState& op_state, std::string& err_msg,
                    RGWFormatterFlusher& flusher, optional_yield y);

  static int remove(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
		    RGWAccountAdminOpState& op_state, std::string& err_msg,
		    RGWFormatterFlusher& flusher, optional_yield y);

  static int info(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
		  RGWAccountAdminOpState& op_state, std::string& err_msg,
		  RGWFormatterFlusher& flusher, optional_yield y);

  static int list_users(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
                        RGWAccountAdminOpState& op_state, std::string& err_msg,
                        RGWFormatterFlusher& flusher, optional_yield y);
};
