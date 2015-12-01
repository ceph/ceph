// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_RGW_SWIFT_H
#define CEPH_RGW_SWIFT_H

#include "rgw_common.h"
#include "common/Cond.h"
#include "rgw_keystone.h"

class RGWRados;

struct rgw_swift_auth_info {
  int status;
  string auth_groups;
  string user;
  string display_name;
  long long ttl;

  rgw_swift_auth_info() : status(0), ttl(0) {}
};

class RGWSwift {
  CephContext *cct;
  Keystone *keystone;

  int validate_token(const char *token, struct rgw_swift_auth_info *info);
  int validate_keystone_token(RGWRados *store, const string& token, struct rgw_swift_auth_info *info,
			      RGWUserInfo& rgw_user);

  int update_user_info(RGWRados *store, struct rgw_swift_auth_info *info, RGWUserInfo& user_info);

  bool do_verify_swift_token(RGWRados *store, req_state *s);
public:

  RGWSwift(CephContext *_cct, Keystone *_keystone) : cct(_cct), keystone(_keystone) { }

  bool verify_swift_token(RGWRados *store, req_state *s);
};

extern RGWSwift *rgw_swift;
void swift_init(CephContext *cct);
void swift_finalize();

#endif

