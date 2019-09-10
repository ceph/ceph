// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RGW_ERROR_CODE_H
#define RGW_ERROR_CODE_H

#include <string>

#include "common/error_code.h"
#include "rgw_common.h"
#include "rgw_error_code.h"

namespace bs = boost::system;

class rgw_error_category : public ceph::converting_category {
public:
  rgw_error_category(){}
  const char* name() const noexcept override;
  std::string message(int ev) const override;
  bs::error_condition default_error_condition(int ev) const noexcept
    override;
  bool equivalent(int ev, const bs::error_condition& c) const
    noexcept override;
  using ceph::converting_category::equivalent;
  int from_code(int ev) const noexcept override;
};

const char* rgw_error_category::name() const noexcept {
  return "monc";
}

std::string rgw_error_category::message(int ev) const {
  if (0)
    return "No error";

  switch (static_cast<rgw_errc>(ev)) {
  case rgw_errc::zonegroup_missing_zone:
    return "Zonegroup missing zone";
  case rgw_errc::cannot_find_zone:
    return "Cannot find zone";
  case rgw_errc::tier_type_not_found:
    return "Tier type not found";
  case rgw_errc::invalid_bucket_name:
    return "Invalid bucket name";
  case rgw_errc::invalid_object_name:
    return "Invalid object name.";
  case rgw_errc::no_such_bucket:
    return "No such bucket";
  case rgw_errc::method_not_allowed:
    return "Method not allowed";
  case rgw_errc::invalid_digest:
    return "Invalid digest.";
  case rgw_errc::bad_digest:
    return "Bad digest.";
  case rgw_errc::unresolvable_email:
    return "Unresolvable email.";
  case rgw_errc::invalid_part:
    return "Invalid part.";
  case rgw_errc::invalid_part_order:
    return "Invalid part order.";
  case rgw_errc::no_such_upload:
    return "No such upload";
  case rgw_errc::request_timeout:
    return "Request timeout";
  case rgw_errc::length_required:
    return "Length required";
  case rgw_errc::request_time_skewed:
    return "Request time skewed";
  case rgw_errc::bucket_exists:
    return "Bucket exists";
  case rgw_errc::bad_url:
    return "Bad URL";
  case rgw_errc::precondition_failed:
    return "Precondition failed";
  case rgw_errc::not_modified:
    return "Not modified.";
  case rgw_errc::invalid_utf8:
    return "Invalid UTF-8";
  case rgw_errc::unprocessable_entity:
    return "Unprocessable entity";
  case rgw_errc::too_large:
    return "Too large";
  case rgw_errc::too_many_buckets:
    return "Too many buckets";
  case rgw_errc::invalid_request:
    return "Invalid request";
  case rgw_errc::too_small:
    return "Too small";
  case rgw_errc::not_found:
    return "Not found";
  case rgw_errc::permanent_redirect:
    return "Permanent redirect";
  case rgw_errc::locked:
    return "Locked";
  case rgw_errc::quota_exceeded:
    return "Quota exceeded";
  case rgw_errc::signature_no_match:
    return "Signature no match";
  case rgw_errc::invalid_access_key:
    return "Invalid access key";
  case rgw_errc::malformed_xml:
    return "Malformed XML";
  case rgw_errc::user_exist:
    return "User exists";
  case rgw_errc::not_slo_manifest:
    return "Not SLO manifest";
  case rgw_errc::email_exist:
    return "Email exists";
  case rgw_errc::key_exist:
    return "Key exists";
  case rgw_errc::invalid_secret_key:
    return "Invalid secret key";
  case rgw_errc::invalid_key_type:
    return "Invalid key type";
  case rgw_errc::invalid_cap:
    return "Invalid cap";
  case rgw_errc::invalid_tenant_name:
    return "Invalid tenant name";
  case rgw_errc::website_redirect:
    return "Website redirect";
  case rgw_errc::no_such_website_configuration:
    return "No such website configuration";
  case rgw_errc::amz_content_sha256_mismatch:
    return "amz_content_sha256 mismatch";
  case rgw_errc::no_such_lc:
    return "No such lifecycle.";
  case rgw_errc::no_such_user:
    return "No such user";
  case rgw_errc::no_such_subuser:
    return "No such subuser";
  case rgw_errc::mfa_required:
    return "MFA required";
  case rgw_errc::no_such_cors_configuration:
    return "No such CORS configuration";
  case rgw_errc::user_suspended:
    return "User suspended";
  case rgw_errc::internal_error:
    return "Internal error";
  case rgw_errc::not_implemented:
    return "Not implemented";
  case rgw_errc::service_unavailable:
    return "Service unavailable";
  case rgw_errc::role_exists:
    return "Role Exists";
  case rgw_errc::malformed_doc:
    return "Malformed document";
  case rgw_errc::no_role_found:
    return "No role found";
  case rgw_errc::delete_conflict:
    return "Delete conflict";
  case rgw_errc::no_such_bucket_policy:
    return "No such bucket policy";
  case rgw_errc::invalid_location_constraint:
    return "Invalid location constraint";
  case rgw_errc::tag_conflict:
    return "Tag conflict";
  case rgw_errc::invalid_tag:
    return "Invalid tag";
  case rgw_errc::zero_in_url:
    return "Zero in URL";
  case rgw_errc::malformed_acl_error:
    return "Malformed ACL error";
  case rgw_errc::zonegroup_default_placement_misconfiguration:
    return "Zonegroup default placement misconfiguration";
  case rgw_errc::invalid_encryption_algorithm:
    return "Invalid encryption algorithm";
  case rgw_errc::invalid_cors_rules_error:
    return "Invalid CORS rules error";
  case rgw_errc::no_cors_found:
    return "No CORS found";
  case rgw_errc::invalid_website_routing_rules_error:
    return "Invalid website routing rules error";
  case rgw_errc::rate_limited:
    return "Rate limited";
  case rgw_errc::position_not_equal_to_length:
    return "Position not equal to length";
  case rgw_errc::object_not_appendable:
    return "Object not appendable";
  case rgw_errc::invalid_bucket_state:
    return "Invalid bucket state";
  case rgw_errc::busy_resharding:
    return "Busy resharding";
  case rgw_errc::no_such_entity:
    return "No such entity";
  case rgw_errc::packed_policy_too_large:
    return "Packed policy too large";
  case rgw_errc::invalid_identity_token:
    return "Invalid identity token";
  case rgw_errc::user_not_permitted_to_use_placement_rule:
    return "User not permitted to use placement rule";
  case rgw_errc::zone_does_not_contain_placement_rule_present_in_zone_group:
    return "Zone does not contain placement rule present in zone group";
  case rgw_errc::storage_class_dne:
    return "Requested storage class does not exist";
  case rgw_errc::requirement_exceeds_limit:
    return "Requirement for operation exceeds allowed capacity";
  case rgw_errc::placement_pool_missing:
    return "Placement pool missing";
  case rgw_errc::no_bucket_for_bucket_operation:
    return "No bucket for bucket operation";
  case rgw_errc::unsupported_hash_type:
    return "Unsupported hash type";
  case rgw_errc::mfa_failed:
    return "Multifactor Authentication Failed";
  case rgw_errc::no_apply:
    return "No Apply";
  }

  return "Unknown error";
}

bs::error_condition rgw_error_category::default_error_condition(int ev) const noexcept {
  switch (static_cast<rgw_errc>(ev)) {
  case rgw_errc::zonegroup_missing_zone: [[fallthrough]];
  case rgw_errc::cannot_find_zone: [[fallthrough]];
  case rgw_errc::tier_type_not_found: [[fallthrough]];
  case rgw_errc::invalid_bucket_name: [[fallthrough]];
  case rgw_errc::invalid_object_name: [[fallthrough]];
  case rgw_errc::method_not_allowed: [[fallthrough]];
  case rgw_errc::invalid_digest: [[fallthrough]];
  case rgw_errc::bad_digest: [[fallthrough]];
  case rgw_errc::unresolvable_email: [[fallthrough]];
  case rgw_errc::invalid_part: [[fallthrough]];
  case rgw_errc::invalid_part_order: [[fallthrough]];
  case rgw_errc::length_required: [[fallthrough]];
  case rgw_errc::request_time_skewed: [[fallthrough]];
  case rgw_errc::bad_url: [[fallthrough]];
  case rgw_errc::invalid_utf8: [[fallthrough]];
  case rgw_errc::unprocessable_entity: [[fallthrough]];
  case rgw_errc::too_large: [[fallthrough]];
  case rgw_errc::invalid_request: [[fallthrough]];
  case rgw_errc::too_small: [[fallthrough]];
  case rgw_errc::signature_no_match: [[fallthrough]];
  case rgw_errc::invalid_access_key: [[fallthrough]];
  case rgw_errc::malformed_xml: [[fallthrough]];
  case rgw_errc::not_slo_manifest: [[fallthrough]];
  case rgw_errc::invalid_secret_key: [[fallthrough]];
  case rgw_errc::invalid_key_type: [[fallthrough]];
  case rgw_errc::invalid_cap: [[fallthrough]];
  case rgw_errc::invalid_tenant_name: [[fallthrough]];
  case rgw_errc::packed_policy_too_large: [[fallthrough]];
  case rgw_errc::invalid_identity_token: [[fallthrough]];
  case rgw_errc::malformed_doc: [[fallthrough]];
  case rgw_errc::invalid_location_constraint: [[fallthrough]];
  case rgw_errc::invalid_tag: [[fallthrough]];
  case rgw_errc::zero_in_url: [[fallthrough]];
  case rgw_errc::invalid_website_routing_rules_error: [[fallthrough]];
  case rgw_errc::invalid_bucket_state: [[fallthrough]];
  case rgw_errc::object_not_appendable: [[fallthrough]];
  case rgw_errc::position_not_equal_to_length: [[fallthrough]];
  case rgw_errc::zone_does_not_contain_placement_rule_present_in_zone_group:
    return bs::errc::invalid_argument;

  case rgw_errc::no_such_bucket: [[fallthrough]];
  case rgw_errc::no_such_upload: [[fallthrough]];
  case rgw_errc::not_found: [[fallthrough]];
  case rgw_errc::no_such_website_configuration: [[fallthrough]];
  case rgw_errc::no_such_lc: [[fallthrough]];
  case rgw_errc::no_such_user: [[fallthrough]];
  case rgw_errc::no_such_subuser: [[fallthrough]];
  case rgw_errc::no_such_cors_configuration: [[fallthrough]];
  case rgw_errc::no_role_found: [[fallthrough]];
  case rgw_errc::no_such_bucket_policy: [[fallthrough]];
  case rgw_errc::no_cors_found: [[fallthrough]];
  case rgw_errc::no_such_entity: [[fallthrough]];
  case rgw_errc::storage_class_dne: [[fallthrough]];
  case rgw_errc::placement_pool_missing:
    return ceph::errc::does_not_exist;

  case rgw_errc::request_timeout:
    return bs::errc::timed_out;

  case rgw_errc::bucket_exists: [[fallthrough]];
  case rgw_errc::not_modified: [[fallthrough]];
  case rgw_errc::permanent_redirect: [[fallthrough]];
  case rgw_errc::user_exist: [[fallthrough]];
  case rgw_errc::email_exist: [[fallthrough]];
  case rgw_errc::key_exist: [[fallthrough]];
  case rgw_errc::website_redirect: [[fallthrough]];
  case rgw_errc::role_exists:
    return ceph::errc::exists;

  case rgw_errc::too_many_buckets: [[fallthrough]];
  case rgw_errc::quota_exceeded:
    return ceph::errc::limit_exceeded;

  case rgw_errc::locked: [[fallthrough]];
  case rgw_errc::service_unavailable: [[fallthrough]];
  case rgw_errc::busy_resharding: [[fallthrough]];
  case rgw_errc::rate_limited:
    return bs::errc::resource_unavailable_try_again;

  case rgw_errc::amz_content_sha256_mismatch: [[fallthrough]];
  case rgw_errc::mfa_required: [[fallthrough]];
  case rgw_errc::user_suspended: [[fallthrough]];
  case rgw_errc::malformed_acl_error: [[fallthrough]];
  case rgw_errc::invalid_encryption_algorithm: [[fallthrough]];
  case rgw_errc::invalid_cors_rules_error:
    return ceph::errc::auth;

  case rgw_errc::internal_error: [[fallthrough]];
  case rgw_errc::zonegroup_default_placement_misconfiguration:
    return ceph::errc::failure;

  case rgw_errc::not_implemented:
    return bs::errc::operation_not_supported;

  case rgw_errc::precondition_failed: [[fallthrough]];
  case rgw_errc::delete_conflict: [[fallthrough]];
  case rgw_errc::tag_conflict:
    return ceph::errc::conflict;

  case rgw_errc::user_not_permitted_to_use_placement_rule:
    return bs::errc::operation_not_permitted;

  case rgw_errc::requirement_exceeds_limit:
    return bs::errc::resource_deadlock_would_occur;

  case rgw_errc::unsupported_hash_type:
    return bs::errc::operation_not_supported;

  case rgw_errc::mfa_failed:
    return bs::errc::permission_denied;

  case rgw_errc::no_bucket_for_bucket_operation:
    return bs::errc::io_error;

  case rgw_errc::no_apply:
    return bs::error_condition{};
  }
  return { ev, *this };
}

bool rgw_error_category::equivalent(int ev, const bs::error_condition& c) const noexcept {
  switch (static_cast<rgw_errc>(ev)) {
  case rgw_errc::malformed_acl_error: [[fallthrough]];
  case rgw_errc::invalid_encryption_algorithm: [[fallthrough]];
  case rgw_errc::invalid_cors_rules_error:
    return c == bs::errc::invalid_argument;

  case rgw_errc::requirement_exceeds_limit:
    return c == ceph::errc::limit_exceeded;

  case rgw_errc::placement_pool_missing: [[fallthrough]];
  case rgw_errc::no_bucket_for_bucket_operation:
  case rgw_errc::no_apply:
    return c == bs::errc::io_error;

  default:
    return default_error_condition(ev) == c;
  }
}

int rgw_error_category::from_code(int ev) const noexcept {
  if (0)
    return 0;

  switch (static_cast<rgw_errc>(ev)) {
  case rgw_errc::zonegroup_missing_zone: [[fallthrough]];
  case rgw_errc::cannot_find_zone: [[fallthrough]];
  case rgw_errc::tier_type_not_found: [[fallthrough]];
  case rgw_errc::zone_does_not_contain_placement_rule_present_in_zone_group: [[fallthrough]];
  case rgw_errc::storage_class_dne:
    return -EINVAL;
  case rgw_errc::invalid_bucket_name:
    return -ERR_INVALID_BUCKET_NAME;
  case rgw_errc::invalid_object_name:
    return -ERR_INVALID_OBJECT_NAME;
  case rgw_errc::no_such_bucket:
    return -ERR_NO_SUCH_BUCKET;
  case rgw_errc::method_not_allowed:
    return -ERR_METHOD_NOT_ALLOWED;
  case rgw_errc::invalid_digest:
    return -ERR_INVALID_DIGEST;
  case rgw_errc::bad_digest:
    return -ERR_BAD_DIGEST;
  case rgw_errc::unresolvable_email:
    return -ERR_UNRESOLVABLE_EMAIL;
  case rgw_errc::invalid_part:
    return -ERR_INVALID_PART;
  case rgw_errc::invalid_part_order:
    return -ERR_INVALID_PART_ORDER;
  case rgw_errc::no_such_upload:
    return -ERR_NO_SUCH_UPLOAD;
  case rgw_errc::request_timeout:
    return -ERR_REQUEST_TIMEOUT;
  case rgw_errc::length_required:
    return -ERR_LENGTH_REQUIRED;
  case rgw_errc::request_time_skewed:
    return -ERR_REQUEST_TIME_SKEWED;
  case rgw_errc::bucket_exists:
    return -ERR_BUCKET_EXISTS;
  case rgw_errc::bad_url:
    return -ERR_BAD_URL;
  case rgw_errc::precondition_failed:
    return -ERR_PRECONDITION_FAILED;
  case rgw_errc::not_modified:
    return -ERR_NOT_MODIFIED;
  case rgw_errc::invalid_utf8:
    return -ERR_INVALID_UTF8;
  case rgw_errc::unprocessable_entity:
    return -ERR_UNPROCESSABLE_ENTITY;
  case rgw_errc::too_large:
    return -ERR_TOO_LARGE;
  case rgw_errc::too_many_buckets:
    return -ERR_TOO_MANY_BUCKETS;
  case rgw_errc::invalid_request:
    return -ERR_INVALID_REQUEST;
  case rgw_errc::too_small:
    return -ERR_TOO_SMALL;
  case rgw_errc::not_found:
    return -ERR_NOT_FOUND;
  case rgw_errc::permanent_redirect:
    return -ERR_PERMANENT_REDIRECT;
  case rgw_errc::locked:
    return -ERR_LOCKED;
  case rgw_errc::quota_exceeded:
    return -ERR_QUOTA_EXCEEDED;
  case rgw_errc::signature_no_match:
    return -ERR_SIGNATURE_NO_MATCH;
  case rgw_errc::invalid_access_key:
    return -ERR_INVALID_ACCESS_KEY;
  case rgw_errc::malformed_xml:
    return -ERR_MALFORMED_XML;
  case rgw_errc::user_exist:
    return -ERR_USER_EXIST;
  case rgw_errc::not_slo_manifest:
    return -ERR_NOT_SLO_MANIFEST;
  case rgw_errc::email_exist:
    return -ERR_EMAIL_EXIST;
  case rgw_errc::key_exist:
    return -ERR_KEY_EXIST;
  case rgw_errc::invalid_secret_key:
    return -ERR_INVALID_SECRET_KEY;
  case rgw_errc::invalid_key_type:
    return -ERR_INVALID_KEY_TYPE;
  case rgw_errc::invalid_cap:
    return -ERR_INVALID_CAP;
  case rgw_errc::invalid_tenant_name:
    return -ERR_INVALID_TENANT_NAME;
  case rgw_errc::website_redirect:
    return -ERR_WEBSITE_REDIRECT;
  case rgw_errc::no_such_website_configuration:
    return -ERR_NO_SUCH_WEBSITE_CONFIGURATION;
  case rgw_errc::amz_content_sha256_mismatch:
    return -ERR_AMZ_CONTENT_SHA256_MISMATCH;
  case rgw_errc::no_such_lc:
    return -ERR_NO_SUCH_LC;
  case rgw_errc::no_such_user:
    return -ERR_NO_SUCH_USER;
  case rgw_errc::no_such_subuser:
    return -ERR_NO_SUCH_SUBUSER;
  case rgw_errc::mfa_required:
    return -ERR_MFA_REQUIRED;
  case rgw_errc::no_such_cors_configuration:
    return -ERR_NO_SUCH_CORS_CONFIGURATION;
  case rgw_errc::user_suspended:
    return -ERR_USER_SUSPENDED;
  case rgw_errc::internal_error:
    return -ERR_INTERNAL_ERROR;
  case rgw_errc::not_implemented:
    return -ERR_NOT_IMPLEMENTED;
  case rgw_errc::service_unavailable:
    return -ERR_SERVICE_UNAVAILABLE;
  case rgw_errc::role_exists:
    return -ERR_ROLE_EXISTS;
  case rgw_errc::malformed_doc:
    return -ERR_MALFORMED_DOC;
  case rgw_errc::no_role_found:
    return -ERR_NO_ROLE_FOUND;
  case rgw_errc::delete_conflict:
    return -ERR_DELETE_CONFLICT;
  case rgw_errc::no_such_bucket_policy:
    return -ERR_NO_SUCH_BUCKET_POLICY;
  case rgw_errc::invalid_location_constraint:
    return -ERR_INVALID_LOCATION_CONSTRAINT;
  case rgw_errc::tag_conflict:
    return -ERR_TAG_CONFLICT;
  case rgw_errc::invalid_tag:
    return -ERR_INVALID_TAG;
  case rgw_errc::zero_in_url:
    return -ERR_ZERO_IN_URL;
  case rgw_errc::malformed_acl_error:
    return -ERR_MALFORMED_ACL_ERROR;
  case rgw_errc::zonegroup_default_placement_misconfiguration:
    return -ERR_ZONEGROUP_DEFAULT_PLACEMENT_MISCONFIGURATION;
  case rgw_errc::invalid_encryption_algorithm:
    return -ERR_INVALID_ENCRYPTION_ALGORITHM;
  case rgw_errc::invalid_cors_rules_error:
    return -ERR_INVALID_CORS_RULES_ERROR;
  case rgw_errc::no_cors_found:
    return -ERR_NO_CORS_FOUND;
  case rgw_errc::invalid_website_routing_rules_error:
    return -ERR_INVALID_WEBSITE_ROUTING_RULES_ERROR;
  case rgw_errc::rate_limited:
    return -ERR_RATE_LIMITED;
  case rgw_errc::position_not_equal_to_length:
    return -ERR_POSITION_NOT_EQUAL_TO_LENGTH;
  case rgw_errc::object_not_appendable:
    return -ERR_OBJECT_NOT_APPENDABLE;
  case rgw_errc::invalid_bucket_state:
    return -ERR_INVALID_BUCKET_STATE;
  case rgw_errc::busy_resharding:
    return -ERR_BUSY_RESHARDING;
  case rgw_errc::no_such_entity:
    return -ERR_NO_SUCH_ENTITY;
  case rgw_errc::packed_policy_too_large:
    return -ERR_PACKED_POLICY_TOO_LARGE;
  case rgw_errc::invalid_identity_token:
    return -ERR_INVALID_IDENTITY_TOKEN;
  case rgw_errc::user_not_permitted_to_use_placement_rule:
    return -EPERM;
  case rgw_errc::requirement_exceeds_limit:
    return -EDEADLK;
  case rgw_errc::placement_pool_missing: [[fallthrough]];
  case rgw_errc::no_bucket_for_bucket_operation:
    return -EIO;
  case rgw_errc::unsupported_hash_type:
    return -ENOTSUP;
  case rgw_errc::mfa_failed:
    return -EACCES;
  case rgw_errc::no_apply:
    return STATUS_NO_APPLY;
  }
  return -EDOM;
}

const bs::error_category& monc_category() noexcept {
  static const rgw_error_category c;
  return c;
}

const bs::error_category& rgw_category() noexcept {
  static rgw_error_category c;
  return c;
}

#endif // RGW_ERROR_CODE_H
