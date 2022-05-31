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

#include "include/types.h"
#include "rgw_metadata.h"

class optional_yield;
class RGWRole;

class RGWSI_Zone;
class RGWSI_Account;
class RGWSI_MetaBackend_Handler;
class RGWFormatterFlusher;

static constexpr uint32_t DEFAULT_QUOTA_LIMIT = 1000;

struct AccountQuota {
  uint32_t max_users {DEFAULT_QUOTA_LIMIT};
  uint32_t max_roles {DEFAULT_QUOTA_LIMIT};

  void encode(bufferlist& bl) const {
    ENCODE_START(1,1,bl);
    encode(max_users, bl);
    encode(max_roles, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1,bl);
    decode(max_users, bl);
    decode(max_roles, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter * const f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(AccountQuota)

class RGWAccountInfo {
  std::string id;
  std::string tenant;
  AccountQuota account_quota;

public:
  RGWAccountInfo() = default;
  explicit RGWAccountInfo(std::string&& _id) : id(std::move(_id)) {}
  explicit RGWAccountInfo(const std::string& _id): id(_id) {}

  RGWAccountInfo(const std::string& _id,
		 const std::string& _tenant) : id(_id),
					       tenant(_tenant)
  {}

  RGWAccountInfo(std::string&& _id,
		 std::string&& _tenant) : id(std::move(_id)),
					  tenant(std::move(_tenant))
  {}

  ~RGWAccountInfo() = default;

  void encode(bufferlist& bl) const {
    ENCODE_START(1,1,bl);
    encode(id, bl);
    encode(tenant, bl);
    encode(account_quota, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(tenant, bl);
    decode(account_quota, bl);
    DECODE_FINISH(bl);
  }

  const std::string& get_id() const { return id; }
  const std::string& get_tenant() { return tenant; }
  uint32_t get_max_users() const
  {
    return account_quota.max_users == 0 ? UINT32_MAX: account_quota.max_users;
  }

  void set_max_users(uint32_t _max_users) {
    account_quota.max_users = _max_users;
  }

  void set_max_roles(uint32_t _max_roles) {
    account_quota.max_roles = _max_roles;
  }

  void dump(Formatter * const f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWAccountInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWAccountInfo)

class RGWAccountMetadataHandler;

class RGWAccountCtl
{
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Account *account {nullptr};
  } svc;

  RGWAccountMetadataHandler *am_handler{nullptr};
  RGWSI_MetaBackend_Handler *be_handler{nullptr};
public:
  RGWAccountCtl(RGWSI_Zone *zone_svc,
		RGWSI_Account *account_svc,
		RGWAccountMetadataHandler *_am_handler);

  ~RGWAccountCtl() = default;

  int add_user(const std::string& account_id,
	       const rgw_user& user,
	       optional_yield);
  int add_role(const RGWRole& role);

  int list_users(const std::string& account_id,
                 const std::string& marker,
                 bool *more,
                 vector<rgw_user>& results,
                 optional_yield y);

  int remove_user(const std::string& account_id,
                  const rgw_user& user,
                  optional_yield y);
  int remove_role(const RGWRole& role);

  int store_info(const RGWAccountInfo& info,
		 RGWObjVersionTracker *objv_tracker,
		 const real_time& mtime,
		 bool exclusive,
		 map<string, bufferlist> *pattrs,
		 optional_yield y);

  int read_info(const std::string& account_id,
		RGWAccountInfo* info,
		RGWObjVersionTracker * const objv_tracker,
		real_time * const pmtime,
		map<std::string, bufferlist> * pattrs,
		optional_yield y);

  int remove_info(const std::string& account_id,
		  RGWObjVersionTracker *objv_tracker,
		  optional_yield y);
  // TODO
  int get_info_by_tenant(const std::string& tenant,
			 RGWAccountInfo* info,
			 optional_yield y);
};

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

class RGWAccountMetadataHandler: public RGWMetadataHandler_GenericMetaBE {
public:
  struct Svc {
    RGWSI_Account *account {nullptr};
  } svc;

  explicit RGWAccountMetadataHandler(RGWSI_Account *account_svc);

  std::string get_type() override { return "account"; }

  int do_get(RGWSI_MetaBackend_Handler::Op *op,
             std::string& entry,
             RGWMetadataObject **obj,
             optional_yield y) override;

  int do_put(RGWSI_MetaBackend_Handler::Op *op,
             std::string& entry,
             RGWMetadataObject *obj,
             RGWObjVersionTracker& objv_tracker,
             optional_yield y,
             RGWMDLogSyncType type) override;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op,
                std::string& entry,
                RGWObjVersionTracker& objv_tracker,
                optional_yield y) override;

  RGWMetadataObject *get_meta_obj(JSONObj *jo,
				  const obj_version& objv,
				  const ceph::real_time& mtime) override;
};

struct RGWAccountAdminOpState
{
  RGWAccountInfo info;
  std::string account_id;
  std::string tenant;
  uint32_t max_users;
  uint32_t max_roles;
  RGWObjVersionTracker objv_tracker;

  bool has_account_id() {
    return !account_id.empty();
  }

  void set_max_users(uint32_t _max_users) { max_users = _max_users;}
  void set_max_roles(uint32_t _max_roles) { max_roles = _max_roles; }
  RGWAccountAdminOpState(const std::string& _account_id): account_id(_account_id) {};
  RGWAccountAdminOpState(const std::string& _account_id,
			 const std::string& _tenant) : account_id(_account_id),
						       tenant(_tenant) {}
};

/*
 Wrappers to unify all the admin ops of creating & removing accounts
*/
class RGWAdminOp_Account
{
public:
  static int add(rgw::sal::RGWRadosStore *store,
		 RGWAccountAdminOpState& op_state,
		 RGWFormatterFlusher& flusher,
		 optional_yield y);

  static int remove(rgw::sal::RGWRadosStore *store,
		    RGWAccountAdminOpState& op_state,
		    RGWFormatterFlusher& flusher,
		    optional_yield y);

  static int info(rgw::sal::RGWRadosStore *store,
		  RGWAccountAdminOpState& op_state,
		  RGWFormatterFlusher& flusher,
		  optional_yield y);

  static int list(rgw::sal::RGWRadosStore *store,
		  RGWAccountAdminOpState& op_state,
		  RGWFormatterFlusher& flusher,
		  optional_yield y);
};