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

class RGWSI_Zone;
class RGWSI_Account;
class RGWSI_MetaBackend_Handler;
class RGWFormatterFlusher;

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

  int add_user(const DoutPrefixProvider* dpp,
	       const std::string& account_id,
	       const rgw_user& user,
	       optional_yield);
  int add_role(const RGWRole& role);

  int list_users(const DoutPrefixProvider* dpp,
                 const std::string& account_id,
                 const std::string& marker,
                 bool *more,
                 std::vector<rgw_user>& results,
                 optional_yield y);

  int remove_user(const DoutPrefixProvider* dpp,
                  const std::string& account_id,
                  const rgw_user& user,
                  optional_yield y);
  int remove_role(const RGWRole& role);

  int store_info(const DoutPrefixProvider* dpp,
		 const RGWAccountInfo& info,
		 const RGWAccountInfo* old_info,
		 RGWObjVersionTracker& objv,
		 const real_time& mtime,
		 bool exclusive,
		 std::map<std::string, bufferlist> *pattrs,
		 optional_yield y);

  int read_info(const DoutPrefixProvider* dpp,
		const std::string& account_id,
		RGWAccountInfo& info,
		RGWObjVersionTracker& objv,
		real_time* pmtime,
		std::map<std::string, bufferlist>* pattrs,
		optional_yield y);

  int remove_info(const DoutPrefixProvider* dpp,
                  const RGWAccountInfo& info,
		  RGWObjVersionTracker& objv,
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
             optional_yield y, const DoutPrefixProvider* dpp) override;

  int do_put(RGWSI_MetaBackend_Handler::Op *op,
             std::string& entry,
             RGWMetadataObject *obj,
             RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider* dpp,
             RGWMDLogSyncType type, bool from_remote_zone) override;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op,
                std::string& entry,
                RGWObjVersionTracker& objv_tracker,
                optional_yield y, const DoutPrefixProvider* dpp) override;

  RGWMetadataObject *get_meta_obj(JSONObj *jo,
				  const obj_version& objv,
				  const ceph::real_time& mtime) override;
};

struct RGWAccountAdminOpState
{
  RGWAccountInfo info;
  std::string account_id;
  std::string tenant;
  std::string account_name;
  uint32_t max_users;
  RGWObjVersionTracker objv_tracker;

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
  static int add(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
		 RGWAccountAdminOpState& op_state,
		 RGWFormatterFlusher& flusher,
		 optional_yield y);

  static int remove(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
		    RGWAccountAdminOpState& op_state,
		    RGWFormatterFlusher& flusher,
		    optional_yield y);

  static int info(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
		  RGWAccountAdminOpState& op_state,
		  RGWFormatterFlusher& flusher,
		  optional_yield y);

  static int list(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
		  RGWAccountAdminOpState& op_state,
		  RGWFormatterFlusher& flusher,
		  optional_yield y);
};
