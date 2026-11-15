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

#ifndef CEPH_RGW_PERMISSION_H
#define CEPH_RGW_PERMISSION_H

#include "rgw_common.h"
#include "rgw_iam_policy.h"

struct perm_state_base {
  CephContext *cct;
  const rgw::IAM::Environment& env;
  rgw::auth::Identity *identity;
  const RGWBucketInfo bucket_info;
  rgw::s3::ObjectOwnership bucket_object_ownership;
  int perm_mask;
  bool defer_to_bucket_acls;
  PublicAccessBlockConfiguration public_access_block;

  perm_state_base(CephContext *_cct,
                  const rgw::IAM::Environment& _env,
                  rgw::auth::Identity *_identity,
                  const RGWBucketInfo& _bucket_info,
                  rgw::s3::ObjectOwnership bucket_object_ownership,
                  int _perm_mask,
                  bool _defer_to_bucket_acls,
                  PublicAccessBlockConfiguration _public_access_block = {}) :
                                                cct(_cct),
                                                env(_env),
                                                identity(_identity),
                                                bucket_info(_bucket_info),
                                                bucket_object_ownership(bucket_object_ownership),
                                                perm_mask(_perm_mask),
                                                defer_to_bucket_acls(_defer_to_bucket_acls),
                                                public_access_block(_public_access_block)
  {}

  virtual ~perm_state_base() {}

  virtual const char *get_referer() const = 0;
  virtual std::optional<bool> get_request_payer() const = 0; /*
                                                              * empty state means that request_payer param was not passed in
                                                              */

};

struct perm_state : public perm_state_base {
  const char *referer;
  bool request_payer;

  perm_state(CephContext *_cct,
             const rgw::IAM::Environment& _env,
             rgw::auth::Identity *_identity,
             const RGWBucketInfo& _bucket_info,
             rgw::s3::ObjectOwnership bucket_object_ownership,
             int _perm_mask,
             bool _defer_to_bucket_acls,
             const char *_referer,
             bool _request_payer) : perm_state_base(_cct,
                                                    _env,
                                                    _identity,
                                                    _bucket_info,
                                                    bucket_object_ownership,
                                                    _perm_mask,
                                                    _defer_to_bucket_acls),
                                    referer(_referer),
                                    request_payer(_request_payer) {}

  const char *get_referer() const override {
    return referer;
  }

  std::optional<bool> get_request_payer() const override {
    return request_payer;
  }
};

/* A Keystone identity whose role tier caps its implicit permissions
 * (a project reader or an implicit-deny role). The identity-type check
 * matters: Swift read-only subusers of local users also carry a reduced
 * perm_mask but must not be treated as capped Keystone identities. */
bool is_capped_keystone_identity(uint32_t perm_mask,
                                 const rgw::auth::Identity& identity);

/* Can this owner perform this operation?
 *
 * Returns true when the identity owns the resource and its permission mask
 * allows the requested permission. A normal owner has full control, so
 * anything passes. A capped identity (a Keystone reader or implicit-deny
 * role) is limited to what its mask allows: reads pass, writes do not.
 *
 * This is the one place an owner grant checks the cap. A policy that allows
 * the user runs earlier and can still grant more. */
bool verify_owner_permission(const rgw::auth::Identity& identity,
                             uint32_t perm_mask,
                             const rgw_owner& owner, uint32_t perm);

/** Check if the req_state's user has the necessary permissions
 * to do the requested action  */
bool verify_bucket_permission_no_policy(
  const DoutPrefixProvider* dpp,
  const perm_state_base * const s,
  const RGWAccessControlPolicy& user_acl,
  const RGWAccessControlPolicy& bucket_acl,
  const int perm, bool* granted_by_acl = nullptr);

bool verify_user_permission_no_policy(const DoutPrefixProvider* dpp,
                                      struct perm_state_base * const s,
                                      const RGWAccessControlPolicy& user_acl,
                                      const int perm);

bool verify_object_permission_no_policy(const DoutPrefixProvider* dpp,
                                        struct perm_state_base * const s,
					const RGWAccessControlPolicy& user_acl,
					const RGWAccessControlPolicy& bucket_acl,
					const RGWAccessControlPolicy& object_acl,
					const int perm,
                                        bool *granted_by_acl = nullptr);

// determine whether a request is allowed or denied within an account
rgw::IAM::Effect evaluate_iam_policies(
    const DoutPrefixProvider* dpp,
    const rgw::IAM::Environment& env,
    const rgw::auth::Identity& identity,
    bool account_root, uint64_t op,
    const rgw::ARN& identity_arn,
    boost::optional<const rgw::ARN&> resource_arn,
    const boost::optional<rgw::IAM::Policy>& resource_policy,
    const std::vector<rgw::IAM::Policy>& identity_policies,
    const std::vector<rgw::IAM::Policy>& session_policies,
    bool cross_account);

bool verify_user_permission(const DoutPrefixProvider* dpp,
                            req_state * const s,
                            const rgw::ARN& res,
                            const uint64_t op,
                            bool mandatory_policy=true);
bool verify_user_permission_no_policy(const DoutPrefixProvider* dpp,
                                      req_state * const s,
                                      int perm);
// evaluate permissions on a non-s3 resource like a notification topic
// or role trust policy
rgw::IAM::Effect evaluate_resource_permission(
    const DoutPrefixProvider* dpp,
    const rgw::IAM::Environment& env,
    const rgw::auth::Identity& identity,
    uint64_t op,
    const rgw::ARN& identity_arn,
    boost::optional<const rgw::ARN&> resource_arn,
    const rgw_owner& resource_owner,
    const boost::optional<rgw::IAM::Policy>& resource_policy,
    const std::vector<rgw::IAM::Policy>& identity_policies,
    const std::vector<rgw::IAM::Policy>& session_policies);

// verify permissions on a non-s3 resource like a notification topic
// or role trust policy
bool verify_resource_permission(
    const DoutPrefixProvider* dpp,
    const rgw::IAM::Environment& env,
    const rgw::auth::Identity& identity,
    uint64_t op,
    const rgw::ARN& identity_arn,
    boost::optional<const rgw::ARN&> resource_arn,
    const rgw_owner& resource_owner,
    const boost::optional<rgw::IAM::Policy>& resource_policy,
    const std::vector<rgw::IAM::Policy>& identity_policies,
    const std::vector<rgw::IAM::Policy>& session_policies);

bool verify_bucket_permission(const DoutPrefixProvider* dpp,
                              const perm_state_base * const s,
                              const rgw::ARN& arn,
                              bool account_root,
                              const RGWAccessControlPolicy& user_acl,
                              const RGWAccessControlPolicy& bucket_acl,
			      const boost::optional<rgw::IAM::Policy>& bucket_policy,
                              const std::vector<rgw::IAM::Policy>& identity_policies,
                              const std::vector<rgw::IAM::Policy>& session_policies,
                              const uint64_t op,
                              bool cross_account,
                              bool *granted_by_acl = nullptr);
bool verify_bucket_permission(
  const DoutPrefixProvider* dpp,
  req_state * const s,
  const rgw::ARN& arn,
  const RGWAccessControlPolicy& user_acl,
  const RGWAccessControlPolicy& bucket_acl,
  const boost::optional<rgw::IAM::Policy>& bucket_policy,
  const std::vector<rgw::IAM::Policy>& identity_policies,
  const std::vector<rgw::IAM::Policy>& session_policies,
  const uint64_t op);
bool verify_bucket_permission(const DoutPrefixProvider* dpp, req_state* s,
                              const rgw::ARN& arn, uint64_t op);
bool verify_bucket_permission(const DoutPrefixProvider* dpp,
                              req_state* s, uint64_t op);
bool verify_bucket_permission_no_policy(
  const DoutPrefixProvider* dpp,
  req_state * const s,
  const RGWAccessControlPolicy& user_acl,
  const RGWAccessControlPolicy& bucket_acl,
  const int perm);
bool verify_bucket_permission_no_policy(const DoutPrefixProvider* dpp,
                                        req_state * const s,
					const int perm);
extern bool verify_object_permission(
  const DoutPrefixProvider* dpp,
  req_state * const s,
  const rgw_obj& obj,
  const RGWAccessControlPolicy& user_acl,
  const RGWAccessControlPolicy& bucket_acl,
  const RGWAccessControlPolicy& object_acl,
  const boost::optional<rgw::IAM::Policy>& bucket_policy,
  const std::vector<rgw::IAM::Policy>& identity_policies,
  const std::vector<rgw::IAM::Policy>& session_policies,
  const uint64_t op);
extern bool verify_object_permission(const DoutPrefixProvider* dpp, req_state *s, uint64_t op);
extern bool verify_object_permission_no_policy(const DoutPrefixProvider* dpp, req_state *s,
					       int perm);
extern int verify_object_lock(
  const DoutPrefixProvider* dpp,
  const rgw::sal::Attrs& attrs,
  const bool bypass_perm,
  const bool bypass_governance_mode);

extern boost::optional<rgw::IAM::Policy>
get_iam_policy_from_attr(CephContext* cct,
                         const std::map<std::string, bufferlist>& attrs,
                         const std::string& tenant);

#endif // CEPH_RGW_PERMISSION_H
