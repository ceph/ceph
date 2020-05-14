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

class RGWRole;
class rgw_user;
class optional_yield;

struct AccountQuota {
  uint16_t max_users {1000};
  uint16_t max_roles {1000};

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
};
WRITE_CLASS_ENCODER(AccountQuota)

class RGWAccountInfo {
  std::string id;
  std::string tenant;
  AccountQuota account_quota;

  std::atomic<uint16_t> users_count = {0};
  std::atomic<uint16_t> roles_count = {0};
public:
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
};
WRITE_CLASS_ENCODER(RGWAccountInfo)

class RGWAccountCtl
{
public:
  int add_user(const std::string& account_id,
	       const rgw_user& user);
  int add_role(const RGWRole& role);

  int list_users();

  int remove_user(const rgw_user& user);
  int remove_role(const RGWRole& role);

  int get_info_by_id(const std::string& id,
		     RGWAccountInfo* info,
		     optional_yield y);

  int store_info(const RGWAccountInfo& info,
		 optional_yield y);

  int read_info(RGWAccountInfo* info,
		optional_yield y);

  int remove_info(const RGWAccountInfo& info,
		  optional_yield y);
  // TODO
  // get_info_by_tenant()
};
