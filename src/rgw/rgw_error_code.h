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

#include <boost/system/error_code.hpp>

const boost::system::error_category& rgw_category() noexcept;

enum class rgw_errc {
  zonegroup_missing_zone = 1,
  cannot_find_zone,
  tier_type_not_found,
  invalid_bucket_name,
  invalid_object_name,
  no_such_bucket,
  method_not_allowed,
  invalid_digest,
  bad_digest,
  unresolvable_email,
  invalid_part,
  invalid_part_order,
  no_such_upload,
  request_timeout,
  length_required,
  request_time_skewed,
  bucket_exists,
  bad_url,
  precondition_failed,
  not_modified,
  invalid_utf8,
  unprocessable_entity,
  too_large,
  too_many_buckets,
  invalid_request,
  too_small,
  not_found,
  permanent_redirect,
  locked,
  quota_exceeded,
  signature_no_match,
  invalid_access_key,
  malformed_xml,
  user_exist,
  not_slo_manifest,
  email_exist,
  key_exist,
  invalid_secret_key,
  invalid_key_type,
  invalid_cap,
  invalid_tenant_name,
  website_redirect,
  no_such_website_configuration,
  amz_content_sha256_mismatch,
  no_such_lc,
  no_such_user,
  no_such_subuser,
  mfa_required,
  no_such_cors_configuration,
  user_suspended,
  internal_error,
  not_implemented,
  service_unavailable,
  role_exists,
  malformed_doc,
  no_role_found,
  delete_conflict,
  no_such_bucket_policy,
  invalid_location_constraint,
  tag_conflict,
  invalid_tag,
  zero_in_url,
  malformed_acl_error,
  zonegroup_default_placement_misconfiguration,
  invalid_encryption_algorithm,
  invalid_cors_rules_error,
  no_cors_found,
  invalid_website_routing_rules_error,
  rate_limited,
  position_not_equal_to_length,
  object_not_appendable,
  invalid_bucket_state,
  busy_resharding,
  no_such_entity,
  packed_policy_too_large,
  invalid_identity_token,
  user_not_permitted_to_use_placement_rule,
  zone_does_not_contain_placement_rule_present_in_zone_group,
  storage_class_dne,
  requirement_exceeds_limit,
  placement_pool_missing,
  no_bucket_for_bucket_operation,
  unsupported_hash_type,
  mfa_failed,
  no_apply // If they do allow us to specify some codes as 'status
	   // with success', this should be a success, apparently.
};

namespace boost {
namespace system {
template<>
struct is_error_code_enum<::rgw_errc> {
  static const bool value = true;
};
}
}

//  explicit conversion:
inline boost::system::error_code make_error_code(rgw_errc e) noexcept {
  return { int(e), rgw_category() };
}

// implicit conversion:
inline boost::system::error_condition make_error_condition(rgw_errc e)
  noexcept {
  return { int(e), rgw_category() };
}
