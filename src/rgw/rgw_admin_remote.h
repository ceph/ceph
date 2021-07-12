// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "common/async/yield_context.h"

#include "rgw_basic_types.h"

class CephContext;
class RGWSI_Zone;
class RGWRESTConn;

class RGWRemoteAdminOp {
  CephContext *cct;

  struct {
    RGWSI_Zone *zone;
  } svc;

 public:
  RGWRemoteAdminOp(RGWSI_Zone *zone_svc);

  struct CreateUserParams {
    rgw_user user;
    std::string display_name;
    std::optional<std::string> email;
    std::optional<std::string> access_key;
    std::optional<std::string> secret_key;
    std::optional<std::string> key_type;
    std::optional<std::string> user_caps;
    std::optional<bool> generate_key{true};
    std::optional<bool> suspended{false};
    std::optional<int32_t> max_buckets;
    std::optional<bool> system;
    std::optional<bool> exclusive;
    std::optional<std::string> op_mask;
    std::optional<std::string> default_placement;
    std::optional<std::string> placement_tags;
  };

  /* explicit conn */
  int create_user(RGWRESTConn *conn, const CreateUserParams& params, optional_yield y);

  /* implicit conn, using conn to primary */
  int create_user(const CreateUserParams& params, optional_yield y);
};
