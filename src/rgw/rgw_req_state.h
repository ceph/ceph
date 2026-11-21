// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 * Copyright (C) 2015 Yehuda Sadeh <yehuda@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_REQ_STATE_H
#define CEPH_RGW_REQ_STATE_H

#include "common/dout.h"
#include "common/tracer.h"
#include "rgw_common.h"
#include "rgw_iam_policy.h"

/* Namespaced forward declarations. */
namespace rgw {
  namespace auth {
    namespace s3 {
      class AWSBrowserUploadAbstractor;
      class STSEngine;
    }
    class Completer;
    class Identity;
  }
  namespace io {
    class BasicClient;
  }
}

struct req_init_state {
  /* Keeps [[tenant]:]bucket until we parse the token. */
  std::string url_bucket;
  std::string src_bucket;
};

/** Store all the state necessary to complete and respond to an HTTP request*/
struct req_state : DoutPrefixProvider {
  CephContext *cct;
  const RGWProcessEnv& penv;
  rgw::io::BasicClient *cio{nullptr};
  http_op op{OP_UNKNOWN};
  RGWOpType op_type{};
  std::shared_ptr<RateLimiter> ratelimit_data;
  RGWRateLimitInfo user_ratelimit;
  RGWRateLimitInfo bucket_ratelimit;
  RGWRateLimitInfo account_ratelimit;
  int64_t ratelimit_retry_after{0};
  std::string ratelimit_bucket_marker;
  std::string ratelimit_user_name;
  bool content_started{false};
  RGWFormat format{RGWFormat::PLAIN};
  ceph::Formatter *formatter{nullptr};
  std::string decoded_uri;
  std::string relative_uri;
  const char *length{nullptr};
  int64_t content_length{0};
  std::map<std::string, std::string> generic_attrs;
  rgw_err err;
  bool expect_cont{false};
  uint64_t obj_size{0};
  uint64_t rdma_bytes_transferred{0};
  bool enable_ops_log;
  bool enable_usage_log;
  rgw_s3select_usage_data s3select_usage;
  uint8_t defer_to_bucket_acls;
  uint32_t perm_mask{0};

  /* Set once when url_bucket is parsed and not violated thereafter. */
  std::string account_name;

  std::string bucket_tenant;
  std::string bucket_name;
  rgw_obj_key object_key; // requested object name and version id

  /* bucket is only created in rgw_build_bucket_policies() and should never be
   * overwritten */
  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::unique_ptr<rgw::sal::Object> object;
  std::string src_tenant_name;
  std::string src_bucket_name;
  rgw_obj_key src_object_key; // requested source object name and version id
  std::unique_ptr<rgw::sal::Object> src_object;
  ACLOwner bucket_owner;
  // Resource owner for the authenticated identity, initialized in authorize()
  ACLOwner owner;

  std::string zonegroup_name;
  std::string zonegroup_endpoint;
  std::string bucket_instance_id;
  int bucket_instance_shard_id{-1};
  std::string redirect_zone_endpoint;

  std::string redirect;

  real_time bucket_mtime;
  std::map<std::string, ceph::bufferlist> bucket_attrs;
  bool bucket_exists{false};
  rgw_placement_rule dest_placement;

  bool has_bad_meta{false};

  std::unique_ptr<rgw::sal::User> user;

  struct {
    /* TODO(rzarzynski): switch out to the static_ptr for both members. */

    /* Object having the knowledge about an authenticated identity and allowing
     * to apply it during the authorization phase (verify_permission() methods
     * of a given RGWOp). Thus, it bounds authentication and authorization steps
     * through a well-defined interface. For more details, see rgw_auth.h. */
    std::unique_ptr<rgw::auth::Identity> identity;

    std::shared_ptr<rgw::auth::Completer> completer;

    /* A container for credentials of the S3's browser upload. It's necessary
     * because: 1) the ::authenticate() method of auth engines and strategies
     * take req_state only; 2) auth strategies live much longer than RGWOps -
     * there is no way to pass additional data dependencies through ctors. */
    class {
      /* Writer. */
      friend class RGWPostObj_ObjStore_S3;
      /* Reader. */
      friend class rgw::auth::s3::AWSBrowserUploadAbstractor;
      friend class rgw::auth::s3::STSEngine;

      std::string access_key;
      std::string signature;
      std::string x_amz_algorithm;
      std::string x_amz_credential;
      std::string x_amz_date;
      std::string x_amz_security_token;
      ceph::bufferlist encoded_policy;
    } s3_postobj_creds;
  } auth;

  RGWAccessControlPolicy user_acl;
  RGWAccessControlPolicy bucket_acl;
  RGWAccessControlPolicy object_acl;

  rgw::IAM::Environment env;
  boost::optional<rgw::IAM::Policy> iam_policy;
  // PublicAccessBlock configuration that applies to this request
  PublicAccessBlockConfiguration public_access_block;
  rgw::s3::ObjectOwnership bucket_object_ownership = rgw::s3::ObjectOwnership::ObjectWriter;
  std::vector<rgw::IAM::Policy> iam_identity_policies;

  /* Is the request made by an user marked as a system one?
   * Being system user means we also have the admin status. */
  bool system_request{false};

  std::string canned_acl;
  bool has_acl_header{false};
  bool granted_by_acl{false};
  bool local_source{false}; /* source is local */

  int prot_flags{0};

  /* Content-Disposition override for TempURL of Swift API. */
  struct {
    std::string override;
    std::string fallback;
  } content_disp;

  std::string host_id;

  req_info info;
  req_init_state init_state;

  using Clock = ceph::coarse_real_clock;
  Clock::time_point time;

  Clock::duration time_elapsed() const { return Clock::now() - time; }

  std::string dialect;
  std::string req_id;
  std::string trans_id;
  uint64_t id;

  RGWObjTags tagset;

  bool mfa_verified{false};

  /// optional coroutine context
  optional_yield yield{null_yield};

  //token claims from STS token for ops log (can be used for Keystone token also)
  std::vector<std::string> token_claims;

  std::vector<rgw::IAM::Policy> session_policies;

  jspan_ptr trace;
  bool trace_enabled = false;

  //Principal tags that come in as part of AssumeRoleWithWebIdentity
  std::vector<std::pair<std::string, std::string>> principal_tags;

  req_state(CephContext* _cct, const RGWProcessEnv& penv, RGWEnv* e, uint64_t id);
  ~req_state();


  void set_user(std::unique_ptr<rgw::sal::User>& u) { user.swap(u); }
  bool is_err() const { return err.is_err(); }

  // implements DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const override;
  CephContext* get_cct() const override { return cct; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }
};

void set_req_state_err(req_state*, int);
void set_req_state_err(req_state*, int, const std::string&);
void dump(req_state*);

#endif // CEPH_RGW_REQ_STATE_H
