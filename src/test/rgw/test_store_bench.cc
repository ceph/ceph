// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/async/yield_context.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/dout.h"
#include "include/buffer_fwd.h"
#include "include/msgr.h"
#include "include/types.h"
#include "rgw_basic_types.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_sal.h"
#include "rgw_tools.h"
#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>
#include <chrono>
#include <climits>
#include <cstdint>
#include <map>
#include <memory>
#include <string>

#define CONF Config::instance()

struct Range {
  int start, end;
  /*
    A range is a string written like int..int or int alone.
    Example:
    2..32 is a range from 2 to 32
    32 is a range from 0 to 32
   */
  Range() : start(0), end(0) {}

  Range(std::string range) {
    size_t dot_index = range.find('.');
    start = 0;
    end = 0;

    // no points let's assume it's a single int
    if (dot_index == std::string::npos || dot_index + 2 >= range.length() || range[dot_index+1] != '.') {
      if(is_number(range)) {
        end = std::stoi(range);
      }
    } else {
      std::string lh = range.substr(0, dot_index);
      std::string rh = range.substr(dot_index + 2, range.length());
      if (is_number(lh)) {
        start = std::stoi(lh);
      }
      if (is_number(rh)) {
        end = std::stoi(rh);
      }
    }
  }

  friend std::ostream& operator<< (std::ostream &out, const Range &range) {
    out << range.start << ".." << range.end;
    return out;
  }


private:
  bool is_number(std::string &n) {
    for (auto c : n) {
      if (!std::isdigit(c)) {
        return false;
      }
    }
    return true;
  }
};

class Config {
public:
  DoutPrefixProvider *dpp;
  CephContext *cct;
  rgw::sal::Store *store;
  rgw_user uid;
  rgw_bucket bucket;
  std::string tenant;

  Config() : tenant("tenant1") {
    uid = rgw_user(tenant, "user1");
    bucket.name = "bucket1";
    bucket.tenant = tenant;
  }
  static Config &instance() {
    static Config _instance;
    return _instance;
  }
};

void create_user() {
  std::string display_name("user1");
  std::string email("foo@bar.com");
  std::string access_key;
  std::string secret_key;
  std::string key_type_str;
  std::string caps;
  std::string tenant_name;
  std::string op_mask_str;
  std::string default_placement_str;
  std::string placement_tags_str;

  RGWUserAdminOpState op_state(CONF.store);

  op_state.set_user_id(Config::instance().uid);
  op_state.set_display_name(display_name);
  op_state.set_user_email(email);
  op_state.set_caps(caps);
  op_state.set_access_key(access_key);
  op_state.set_secret_key(secret_key);

  RGWNullFlusher flusher;
  optional_yield y = null_yield;
  RGWEnv env;
  struct req_state s(CONF.cct, &env, 1);
  int err = RGWUserAdminOp_User::create(&s, CONF.store, op_state, flusher, y);
  if (err == -ERR_USER_EXIST) {
    std::cout << "create user: " << Config::instance().uid << " already exists!"
              << std::endl;
  }
  std::cout << "create user: " << err << std::endl;
}

void list_users() {
  RGWUserAdminOpState op_state(CONF.store);
  JSONFormatter jf;
  std::stringstream ss;
  RGWStreamFlusher flusher(&jf, ss);
  int err = RGWUserAdminOp_User::list(CONF.dpp, CONF.store, op_state, flusher);
  std::cout << "list user: " << err << std::endl;
  std::cout << "list " << ss.str() << std::endl;
}

void create_bucket() {
  Config c = Config::instance();
  RGWBucketInfo info;
  RGWUserInfo owner;
  obj_version objv;
  rgw_placement_rule rule(
      c.store->get_zone()->get_zonegroup().get_default_placement_name(),
      RGW_STORAGE_CLASS_STANDARD);
  std::map<std::string, bufferlist> attrs;
  owner.user_id.id = Config::instance().uid.id;

  // idk what this is
  objv.ver = 2;
  objv.tag = "write_tag";
  rule.name = "rule1";
  rule.storage_class = "sc1";
  auto user = c.store->get_user(Config::instance().uid);
  rgw_placement_rule r(
      c.store->get_zone()->get_zonegroup().get_default_placement_name(),
      RGW_STORAGE_CLASS_STANDARD);
  owner.default_placement = r;
  RGWQuotaInfo qi;
  auto zone = c.store->get_zone();
  RGWAccessControlPolicy cp;
  RGWEnv env;
  req_info req_i(c.cct, &env);
  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::string swift_var_location;
  bool existed = false;
  int ret =
      user->create_bucket(c.dpp, c.bucket, zone->get_zonegroup().get_id(), r,
                          swift_var_location, &qi, cp, attrs, info, objv, false,
                          false, &existed, req_i, &bucket, null_yield);
  std::cout << "create bucket: " << ret << std::endl;
  std::cout << "existed: " << existed << std::endl;
}

void list_buckets() {
  auto c = Config::instance();
  auto user = c.store->get_user(c.uid);
  rgw::sal::BucketList buckets;
  std::string marker, end_marker;
  user->list_buckets(c.dpp, marker, end_marker, 10, 0, buckets, null_yield);
  std::cout << "Buckets: ";
  for (auto &[name, bucket] : buckets.get_buckets()) {
    std::cout << name << " ";
  }
  std::cout << std::endl;
}

static bufferlist *bl = nullptr;
// bl->length() doesn't work?????
static uint32_t bufferlist_length = 0;

uint64_t create_object(rgw_obj_key key, uint32_t obj_size) {
  // For now let's have one bufferlist as append might be too expensive
  if (bl == nullptr) {
    bl = new bufferlist();
    bl->append_zero(obj_size);
    bufferlist_length = obj_size;
  }
  if (bufferlist_length != obj_size) {
    bl->clear();
    bl->append_zero(obj_size);
    bufferlist_length = obj_size;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket{nullptr};
  auto ruser = CONF.store->get_user(CONF.uid);
  CONF.store->get_bucket(CONF.dpp, ruser.get(), CONF.bucket, &bucket,
                         null_yield);
  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);
  std::unique_ptr<rgw::sal::Writer> processor;
  rgw_placement_rule pr("p1", "");

  std::string etag = "";
  rgw::sal::Attrs attrs;
  bufferlist etag_bl;

  // time prepare->process->complete
  std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
  etag_bl.append(etag.c_str(), etag.size());
  attrs.emplace(RGW_ATTR_ETAG, etag_bl);
  obj->set_obj_attrs(CONF.dpp, &attrs, nullptr, null_yield);
  processor = CONF.store->get_atomic_writer(
      CONF.dpp, null_yield, std::move(obj), Config::instance().uid, &pr, 1, "");
  processor->prepare(null_yield);
  auto proc_ret = processor->process(std::move(*bl), 0);
  if (proc_ret < 0) {
    std::cerr << "Error processor process " << proc_ret << std::endl;
  }
  auto time = real_time();
  char *if_match = NULL;
  char *if_nomatch = NULL;
  std::string user_data;
  rgw_zone_set zs;
  bool pcanceled = false;
  int op_ret = processor->complete(obj_size, "", &time, real_time(), attrs,
                                   real_time(), if_match, if_nomatch,
                                   &user_data, &zs, &pcanceled, null_yield);
  if (op_ret < 0) {
    std::cerr << "Error processor complete " << op_ret << std::endl;
  }
  std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>((t2 - t1))
      .count();
}

int main(int argc, char **argv) {
  std::map<std::string, std::string> defaults = {
      {"debug_rgw", "1/5"},
      {"keyring", "$rgw_data/keyring"},
      {"objecter_inflight_ops", "24576"},
      // require a secure mon connection by default
      {"ms_mon_client_mode", "secure"},
      {"auth_client_required", "cephx"}};
  int flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS;
  flags |= CINIT_FLAG_DEFER_DROP_PRIVILEGES;
  auto args = argv_to_vec(argc, argv);
  auto cct = rgw_global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_DAEMON, flags);

  auto args_copy = args;
  std::string val;
  Range pow_2_increments;
  size_t iterations = 1;
  for (std::vector<const char *>::iterator i = args_copy.begin();
       i != args_copy.end();) {
    if (ceph_argparse_witharg(args_copy, i, &val, "--pow-2-increments",
                              (char *)NULL)) {
      std::cout << val << std::endl;
      pow_2_increments = Range{val};
    } else if (ceph_argparse_witharg(args_copy, i, &val, "--iterations",
                                     (char *)NULL)) {
      iterations = std::stoul(val);
    } else {
      ++i;
    }
  }
  if (pow_2_increments.start < 0) {
    std::cerr << "Wrong number of increments: " << pow_2_increments.start
              << std::endl;
    exit(1);
  }

  // obj_size is unsigned int, 2^32 append_zero fails in assertion of length
  if (pow_2_increments.end > 31) {
    pow_2_increments.end = 31;
  }

  std::cout << "number of power of 2 increments = " << pow_2_increments
            << std::endl;
  std::cout << "iterations = " << iterations << std::endl;
  DoutPrefix dp(cct.get(), 1, "test");
  DoutPrefixProvider *dpp = dynamic_cast<DoutPrefixProvider *>(&dp);
  rgw::sal::Store *store = StoreManager::get_storage(
      dpp, cct.get(), "rados", false, false, false, false, false);

  if (store == nullptr) {
    std::cerr << "get raw storage failed" << std::endl;
    exit(1);
  }

  Config::instance().store = store;
  Config::instance().dpp = dpp;
  Config::instance().cct = cct.get();

  create_user();
  list_users();

  create_bucket();
  list_buckets();

  for (int p2 = pow_2_increments.start; p2 <= pow_2_increments.end; p2++) {
    uint64_t time = 0;
    uint32_t obj_size = (1 << p2);
    for (size_t i = 0; i < iterations; i++) {
      rgw_obj_key key("foo", std::to_string(i));
      time += create_object(key, obj_size);
    }
    if (iterations > 0) {
      std::cout << "Average time per object size of " << obj_size
                << " bytes: " << (time / iterations) << "[ms]" << std::endl;
    }
  }

  return 0;
}
