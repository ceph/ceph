// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <array>
#include <charconv>
#include <optional>
#include <tuple>

#include "common/ceph_json.h"

#include "noobaa_schema.h"

namespace rgw { namespace sal { namespace nsfs { namespace noobaa_schema {

namespace {

/* `{}`;  no schema at the pin is one, and a later release's may be */
[[maybe_unused]] constexpr Node open{.kind = Node::Kind::Open};
constexpr Node scalar{.kind = Node::Kind::Scalar};

using Kind = Node::Kind;

/* ------------------------------------------------------------------
 * NooBaa's record schemas at noobaa-core `08d063f86` (5.23.0), as its
 * validator compiles them:  `nsfs_account_schema.js`,
 * `nsfs_bucket_schema.js`, and the `common_api.js` definitions they
 * reach, each a `def_` node shared by every field that refers to it.
 * An arm is named for its position in NooBaa's oneOf/anyOf/allOf.
 * ------------------------------------------------------------------ */

constexpr Field account_access_keys_item_fields[] = {
  {"access_key", "5.23.0", Disposition::Inherit, &scalar},
  {"encrypted_secret_key", "5.23.0", Disposition::Inherit, &scalar},
  {"creation_date", "5.23.0", Disposition::Inherit, &scalar},
  {"deactivated", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node account_access_keys_item{.kind = Kind::Object, .fields = account_access_keys_item_fields};
constexpr Node account_access_keys{.kind = Kind::Array, .each = &account_access_keys_item};
constexpr Node def_supplemental_groups{.kind = Kind::Array, .each = &scalar};
constexpr Field account_nsfs_account_config_arm0_fields[] = {
  {"uid", "5.23.0", Disposition::Inherit, &scalar},
  {"gid", "5.23.0", Disposition::Inherit, &scalar},
  {"supplemental_groups", "5.23.0", Disposition::Inherit, &def_supplemental_groups},
  {"new_buckets_path", "5.23.0", Disposition::Inherit, &scalar},
  {"fs_backend", "5.23.0", Disposition::Inherit, &scalar},
  {"custom_bucket_path_allowed_list", "5.23.0", Disposition::Inherit, &scalar},
  {"allow_bypass_governance", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node account_nsfs_account_config_arm0{.kind = Kind::Object, .fields = account_nsfs_account_config_arm0_fields};
constexpr Field account_nsfs_account_config_arm1_fields[] = {
  {"distinguished_name", "5.23.0", Disposition::Inherit, &scalar},
  {"new_buckets_path", "5.23.0", Disposition::Inherit, &scalar},
  {"fs_backend", "5.23.0", Disposition::Inherit, &scalar},
  {"custom_bucket_path_allowed_list", "5.23.0", Disposition::Inherit, &scalar},
  {"allow_bypass_governance", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node account_nsfs_account_config_arm1{.kind = Kind::Object, .fields = account_nsfs_account_config_arm1_fields};
constexpr const Node* account_nsfs_account_config_arms[] = {&account_nsfs_account_config_arm0, &account_nsfs_account_config_arm1};
constexpr Node account_nsfs_account_config{.kind = Kind::AnyOf, .arms = account_nsfs_account_config_arms};
constexpr Node def_assume_role_policy_statement_item_action{.kind = Kind::Array, .each = &scalar};
constexpr Node def_assume_role_policy_statement_item_principal{.kind = Kind::Array, .each = &scalar};
constexpr Field def_assume_role_policy_statement_item_fields[] = {
  {"effect", "5.23.0", Disposition::Inherit, &scalar},
  {"action", "5.23.0", Disposition::Inherit, &def_assume_role_policy_statement_item_action},
  {"principal", "5.23.0", Disposition::Inherit, &def_assume_role_policy_statement_item_principal},
};
constexpr Node def_assume_role_policy_statement_item{.kind = Kind::Object, .fields = def_assume_role_policy_statement_item_fields};
constexpr Node def_assume_role_policy_statement{.kind = Kind::Array, .each = &def_assume_role_policy_statement_item};
constexpr Field def_assume_role_policy_fields[] = {
  {"version", "5.23.0", Disposition::Inherit, &scalar},
  {"statement", "5.23.0", Disposition::Inherit, &def_assume_role_policy_statement},
};
constexpr Node def_assume_role_policy{.kind = Kind::Object, .fields = def_assume_role_policy_fields};
constexpr Field def_role_config_fields[] = {
  {"role_name", "5.23.0", Disposition::Inherit, &scalar},
  {"assume_role_policy", "5.23.0", Disposition::Inherit, &def_assume_role_policy},
};
constexpr Node def_role_config{.kind = Kind::Object, .fields = def_role_config_fields};
constexpr Field account_record_fields[] = {
  {"_id", "5.23.0", Disposition::Translated, &scalar},
  {"name", "5.23.0", Disposition::Translated, &scalar},
  {"email", "5.23.0", Disposition::Translated, &scalar},
  {"creation_date", "5.23.0", Disposition::Translated, &scalar},
  {"owner", "5.23.0", Disposition::Translated, &scalar},
  {"creator", "5.23.0", Disposition::NotCarried, &scalar},
  {"iam_path", "5.23.0", Disposition::Translated, &scalar},
  {"master_key_id", "5.23.0", Disposition::Translated, &scalar},
  {"allow_bucket_creation", "5.23.0", Disposition::Translated, &scalar},
  {"force_md5_etag", "5.23.0", Disposition::Carried, &scalar},
  {"iam_operate_on_root_account", "5.23.0", Disposition::NotCarried, &scalar},
  {"access_keys", "5.23.0", Disposition::Translated, &account_access_keys},
  {"nsfs_account_config", "5.23.0", Disposition::Translated, &account_nsfs_account_config},
  {"default_connection", "5.23.0", Disposition::NotCarried, &scalar},
  {"role_config", "5.23.0", Disposition::Translated, &def_role_config},
};
constexpr Node account_record{.kind = Kind::Object, .fields = account_record_fields};
constexpr Field def_tag_fields[] = {
  {"key", "5.23.0", Disposition::Inherit, &scalar},
  {"value", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_tag{.kind = Kind::Object, .fields = def_tag_fields};
constexpr Node def_tagging{.kind = Kind::Array, .each = &def_tag};
constexpr Node def_bucket_policy_principal_arm1_AWS_arm1{.kind = Kind::Array, .each = &scalar};
constexpr const Node* def_bucket_policy_principal_arm1_AWS_arms[] = {&scalar, &def_bucket_policy_principal_arm1_AWS_arm1};
constexpr Node def_bucket_policy_principal_arm1_AWS{.kind = Kind::AnyOf, .arms = def_bucket_policy_principal_arm1_AWS_arms};
constexpr Field def_bucket_policy_principal_arm1_fields[] = {
  {"AWS", "5.23.0", Disposition::Inherit, &def_bucket_policy_principal_arm1_AWS},
};
constexpr Node def_bucket_policy_principal_arm1{.kind = Kind::Object, .fields = def_bucket_policy_principal_arm1_fields};
constexpr const Node* def_bucket_policy_principal_arms[] = {&scalar, &def_bucket_policy_principal_arm1};
constexpr Node def_bucket_policy_principal{.kind = Kind::AnyOf, .arms = def_bucket_policy_principal_arms};
constexpr Node def_string_or_string_array_arm1{.kind = Kind::Array, .each = &scalar};
constexpr const Node* def_string_or_string_array_arms[] = {&scalar, &def_string_or_string_array_arm1};
constexpr Node def_string_or_string_array{.kind = Kind::AnyOf, .arms = def_string_or_string_array_arms};
constexpr Node def_bucket_policy_string_condition{.kind = Kind::Map, .each = &scalar};
constexpr Node def_bucket_policy_null_condition{.kind = Kind::Map, .each = &scalar};
constexpr Field def_bucket_policy_Statement_item_arm0_Condition_fields[] = {
  {"StringEquals", "5.23.0", Disposition::Inherit, &def_bucket_policy_string_condition},
  {"StringNotEquals", "5.23.0", Disposition::Inherit, &def_bucket_policy_string_condition},
  {"StringEqualsIgnoreCase", "5.23.0", Disposition::Inherit, &def_bucket_policy_string_condition},
  {"StringNotEqualsIgnoreCase", "5.23.0", Disposition::Inherit, &def_bucket_policy_string_condition},
  {"StringLike", "5.23.0", Disposition::Inherit, &def_bucket_policy_string_condition},
  {"StringNotLike", "5.23.0", Disposition::Inherit, &def_bucket_policy_string_condition},
  {"Null", "5.23.0", Disposition::Inherit, &def_bucket_policy_null_condition},
};
constexpr Node def_bucket_policy_Statement_item_arm0_Condition{.kind = Kind::Object, .fields = def_bucket_policy_Statement_item_arm0_Condition_fields};
constexpr Field def_bucket_policy_Statement_item_arm0_fields[] = {
  {"Sid", "5.23.0", Disposition::Inherit, &scalar},
  {"Principal", "5.23.0", Disposition::Inherit, &def_bucket_policy_principal},
  {"NotPrincipal", "5.23.0", Disposition::Inherit, &def_bucket_policy_principal},
  {"Action", "5.23.0", Disposition::Inherit, &def_string_or_string_array},
  {"NotAction", "5.23.0", Disposition::Inherit, &def_string_or_string_array},
  {"Resource", "5.23.0", Disposition::Inherit, &def_string_or_string_array},
  {"NotResource", "5.23.0", Disposition::Inherit, &def_string_or_string_array},
  {"Effect", "5.23.0", Disposition::Inherit, &scalar},
  {"Condition", "5.23.0", Disposition::Inherit, &def_bucket_policy_Statement_item_arm0_Condition},
};
constexpr Node def_bucket_policy_Statement_item_arm0{.kind = Kind::Object, .fields = def_bucket_policy_Statement_item_arm0_fields};
constexpr Node def_bucket_policy_Statement_item_arm1_arm0{.kind = Kind::Object, .open = true};
constexpr Node def_bucket_policy_Statement_item_arm1_arm1{.kind = Kind::Object, .open = true};
constexpr const Node* def_bucket_policy_Statement_item_arm1_arms[] = {&def_bucket_policy_Statement_item_arm1_arm0, &def_bucket_policy_Statement_item_arm1_arm1};
constexpr Node def_bucket_policy_Statement_item_arm1{.kind = Kind::AnyOf, .arms = def_bucket_policy_Statement_item_arm1_arms};
constexpr Node def_bucket_policy_Statement_item_arm2_arm0{.kind = Kind::Object, .open = true};
constexpr Node def_bucket_policy_Statement_item_arm2_arm1{.kind = Kind::Object, .open = true};
constexpr const Node* def_bucket_policy_Statement_item_arm2_arms[] = {&def_bucket_policy_Statement_item_arm2_arm0, &def_bucket_policy_Statement_item_arm2_arm1};
constexpr Node def_bucket_policy_Statement_item_arm2{.kind = Kind::AnyOf, .arms = def_bucket_policy_Statement_item_arm2_arms};
constexpr Node def_bucket_policy_Statement_item_arm3_arm0{.kind = Kind::Object, .open = true};
constexpr Node def_bucket_policy_Statement_item_arm3_arm1{.kind = Kind::Object, .open = true};
constexpr const Node* def_bucket_policy_Statement_item_arm3_arms[] = {&def_bucket_policy_Statement_item_arm3_arm0, &def_bucket_policy_Statement_item_arm3_arm1};
constexpr Node def_bucket_policy_Statement_item_arm3{.kind = Kind::AnyOf, .arms = def_bucket_policy_Statement_item_arm3_arms};
constexpr const Node* def_bucket_policy_Statement_item_arms[] = {&def_bucket_policy_Statement_item_arm0, &def_bucket_policy_Statement_item_arm1, &def_bucket_policy_Statement_item_arm2, &def_bucket_policy_Statement_item_arm3};
constexpr Node def_bucket_policy_Statement_item{.kind = Kind::AllOf, .arms = def_bucket_policy_Statement_item_arms};
constexpr Node def_bucket_policy_Statement{.kind = Kind::Array, .each = &def_bucket_policy_Statement_item};
constexpr Field def_bucket_policy_fields[] = {
  {"Version", "5.23.0", Disposition::Inherit, &scalar},
  {"Statement", "5.23.0", Disposition::Inherit, &def_bucket_policy_Statement},
};
constexpr Node def_bucket_policy{.kind = Kind::Object, .fields = def_bucket_policy_fields};
constexpr Field def_bucket_encryption_fields[] = {
  {"algorithm", "5.23.0", Disposition::Inherit, &scalar},
  {"kms_key_id", "5.23.0", Disposition::Inherit, &scalar},
  {"bucket_key_enabled", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_encryption{.kind = Kind::Object, .fields = def_bucket_encryption_fields};
constexpr Field def_bucket_website_website_configuration_arm0_redirect_all_requests_to_fields[] = {
  {"host_name", "5.23.0", Disposition::Inherit, &scalar},
  {"protocol", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_website_website_configuration_arm0_redirect_all_requests_to{.kind = Kind::Object, .fields = def_bucket_website_website_configuration_arm0_redirect_all_requests_to_fields};
constexpr Field def_bucket_website_website_configuration_arm0_fields[] = {
  {"redirect_all_requests_to", "5.23.0", Disposition::Inherit, &def_bucket_website_website_configuration_arm0_redirect_all_requests_to},
};
constexpr Node def_bucket_website_website_configuration_arm0{.kind = Kind::Object, .fields = def_bucket_website_website_configuration_arm0_fields};
constexpr Field def_bucket_website_website_configuration_arm1_index_document_fields[] = {
  {"suffix", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_website_website_configuration_arm1_index_document{.kind = Kind::Object, .fields = def_bucket_website_website_configuration_arm1_index_document_fields};
constexpr Field def_bucket_website_website_configuration_arm1_error_document_fields[] = {
  {"key", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_website_website_configuration_arm1_error_document{.kind = Kind::Object, .fields = def_bucket_website_website_configuration_arm1_error_document_fields};
constexpr Field def_bucket_website_website_configuration_arm1_routing_rules_item_condition_fields[] = {
  {"key_prefix_equals", "5.23.0", Disposition::Inherit, &scalar},
  {"http_error_code_returned_equals", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_website_website_configuration_arm1_routing_rules_item_condition{.kind = Kind::Object, .fields = def_bucket_website_website_configuration_arm1_routing_rules_item_condition_fields};
constexpr Field def_bucket_website_website_configuration_arm1_routing_rules_item_redirect_fields[] = {
  {"protocol", "5.23.0", Disposition::Inherit, &scalar},
  {"host_name", "5.23.0", Disposition::Inherit, &scalar},
  {"replace_key_prefix_with", "5.23.0", Disposition::Inherit, &scalar},
  {"replace_key_with", "5.23.0", Disposition::Inherit, &scalar},
  {"http_redirect_code", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_website_website_configuration_arm1_routing_rules_item_redirect{.kind = Kind::Object, .fields = def_bucket_website_website_configuration_arm1_routing_rules_item_redirect_fields};
constexpr Field def_bucket_website_website_configuration_arm1_routing_rules_item_fields[] = {
  {"condition", "5.23.0", Disposition::Inherit, &def_bucket_website_website_configuration_arm1_routing_rules_item_condition},
  {"redirect", "5.23.0", Disposition::Inherit, &def_bucket_website_website_configuration_arm1_routing_rules_item_redirect},
};
constexpr Node def_bucket_website_website_configuration_arm1_routing_rules_item{.kind = Kind::Object, .fields = def_bucket_website_website_configuration_arm1_routing_rules_item_fields};
constexpr Node def_bucket_website_website_configuration_arm1_routing_rules{.kind = Kind::Array, .each = &def_bucket_website_website_configuration_arm1_routing_rules_item};
constexpr Field def_bucket_website_website_configuration_arm1_fields[] = {
  {"index_document", "5.23.0", Disposition::Inherit, &def_bucket_website_website_configuration_arm1_index_document},
  {"error_document", "5.23.0", Disposition::Inherit, &def_bucket_website_website_configuration_arm1_error_document},
  {"routing_rules", "5.23.0", Disposition::Inherit, &def_bucket_website_website_configuration_arm1_routing_rules},
};
constexpr Node def_bucket_website_website_configuration_arm1{.kind = Kind::Object, .fields = def_bucket_website_website_configuration_arm1_fields};
constexpr const Node* def_bucket_website_website_configuration_arms[] = {&def_bucket_website_website_configuration_arm0, &def_bucket_website_website_configuration_arm1};
constexpr Node def_bucket_website_website_configuration{.kind = Kind::AnyOf, .arms = def_bucket_website_website_configuration_arms};
constexpr Field def_bucket_website_fields[] = {
  {"website_configuration", "5.23.0", Disposition::Inherit, &def_bucket_website_website_configuration},
};
constexpr Node def_bucket_website{.kind = Kind::Object, .fields = def_bucket_website_fields};
constexpr Field def_bucket_logging_fields[] = {
  {"log_bucket", "5.23.0", Disposition::Inherit, &scalar},
  {"log_prefix", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_logging{.kind = Kind::Object, .fields = def_bucket_logging_fields};
constexpr Node def_bucket_lifecycle_rule_filter_tags{.kind = Kind::Array, .each = &def_tag};
constexpr Field def_bucket_lifecycle_rule_filter_fields[] = {
  {"prefix", "5.23.0", Disposition::Inherit, &scalar},
  {"tags", "5.23.0", Disposition::Inherit, &def_bucket_lifecycle_rule_filter_tags},
  {"object_size_greater_than", "5.23.0", Disposition::Inherit, &scalar},
  {"object_size_less_than", "5.23.0", Disposition::Inherit, &scalar},
  {"and", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_lifecycle_rule_filter{.kind = Kind::Object, .fields = def_bucket_lifecycle_rule_filter_fields};
constexpr Field def_bucket_lifecycle_rule_expiration_fields[] = {
  {"days", "5.23.0", Disposition::Inherit, &scalar},
  {"date", "5.23.0", Disposition::Inherit, &scalar},
  {"expired_object_delete_marker", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_lifecycle_rule_expiration{.kind = Kind::Object, .fields = def_bucket_lifecycle_rule_expiration_fields};
constexpr Field def_bucket_lifecycle_rule_abort_incomplete_multipart_upload_fields[] = {
  {"days_after_initiation", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_lifecycle_rule_abort_incomplete_multipart_upload{.kind = Kind::Object, .fields = def_bucket_lifecycle_rule_abort_incomplete_multipart_upload_fields};
constexpr Field def_bucket_lifecycle_rule_transition_fields[] = {
  {"date", "5.23.0", Disposition::Inherit, &scalar},
  {"days", "5.23.0", Disposition::Inherit, &scalar},
  {"storage_class", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_lifecycle_rule_transition{.kind = Kind::Object, .fields = def_bucket_lifecycle_rule_transition_fields};
constexpr Field def_bucket_lifecycle_rule_noncurrent_version_expiration_fields[] = {
  {"noncurrent_days", "5.23.0", Disposition::Inherit, &scalar},
  {"newer_noncurrent_versions", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_lifecycle_rule_noncurrent_version_expiration{.kind = Kind::Object, .fields = def_bucket_lifecycle_rule_noncurrent_version_expiration_fields};
constexpr Field def_bucket_lifecycle_rule_noncurrent_version_transition_fields[] = {
  {"noncurrent_days", "5.23.0", Disposition::Inherit, &scalar},
  {"newer_noncurrent_versions", "5.23.0", Disposition::Inherit, &scalar},
  {"storage_class", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_lifecycle_rule_noncurrent_version_transition{.kind = Kind::Object, .fields = def_bucket_lifecycle_rule_noncurrent_version_transition_fields};
constexpr Field def_bucket_lifecycle_rule_fields[] = {
  {"id", "5.23.0", Disposition::Inherit, &scalar},
  {"status", "5.23.0", Disposition::Inherit, &scalar},
  {"filter", "5.23.0", Disposition::Inherit, &def_bucket_lifecycle_rule_filter},
  {"uses_prefix", "5.23.0", Disposition::Inherit, &scalar},
  {"expiration", "5.23.0", Disposition::Inherit, &def_bucket_lifecycle_rule_expiration},
  {"last_sync", "5.23.0", Disposition::Inherit, &scalar},
  {"abort_incomplete_multipart_upload", "5.23.0", Disposition::Inherit, &def_bucket_lifecycle_rule_abort_incomplete_multipart_upload},
  {"transition", "5.23.0", Disposition::Inherit, &def_bucket_lifecycle_rule_transition},
  {"noncurrent_version_expiration", "5.23.0", Disposition::Inherit, &def_bucket_lifecycle_rule_noncurrent_version_expiration},
  {"noncurrent_version_transition", "5.23.0", Disposition::Inherit, &def_bucket_lifecycle_rule_noncurrent_version_transition},
};
constexpr Node def_bucket_lifecycle_rule{.kind = Kind::Object, .fields = def_bucket_lifecycle_rule_fields};
constexpr Node def_bucket_lifecycle_configuration{.kind = Kind::Array, .each = &def_bucket_lifecycle_rule};
constexpr Node def_bucket_notification_id{.kind = Kind::Array, .each = &scalar};
constexpr Node def_bucket_notification_topic{.kind = Kind::Array, .each = &scalar};
constexpr Node def_bucket_notification_event{.kind = Kind::Array, .each = &scalar};
constexpr Field def_bucket_notification_fields[] = {
  {"id", "5.23.0", Disposition::Inherit, &def_bucket_notification_id},
  {"topic", "5.23.0", Disposition::Inherit, &def_bucket_notification_topic},
  {"event", "5.23.0", Disposition::Inherit, &def_bucket_notification_event},
};
constexpr Node def_bucket_notification{.kind = Kind::Object, .fields = def_bucket_notification_fields};
constexpr Node bucket_notifications{.kind = Kind::Array, .each = &def_bucket_notification};
constexpr Node def_bucket_cors_rule_allowed_methods{.kind = Kind::Array, .each = &scalar};
constexpr Node def_bucket_cors_rule_allowed_origins{.kind = Kind::Array, .each = &scalar};
constexpr Node def_bucket_cors_rule_allowed_headers{.kind = Kind::Array, .each = &scalar};
constexpr Node def_bucket_cors_rule_expose_headers{.kind = Kind::Array, .each = &scalar};
constexpr Field def_bucket_cors_rule_fields[] = {
  {"id", "5.23.0", Disposition::Inherit, &scalar},
  {"allowed_methods", "5.23.0", Disposition::Inherit, &def_bucket_cors_rule_allowed_methods},
  {"allowed_origins", "5.23.0", Disposition::Inherit, &def_bucket_cors_rule_allowed_origins},
  {"allowed_headers", "5.23.0", Disposition::Inherit, &def_bucket_cors_rule_allowed_headers},
  {"expose_headers", "5.23.0", Disposition::Inherit, &def_bucket_cors_rule_expose_headers},
  {"max_age_seconds", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_bucket_cors_rule{.kind = Kind::Object, .fields = def_bucket_cors_rule_fields};
constexpr Node def_bucket_cors_configuration{.kind = Kind::Array, .each = &def_bucket_cors_rule};
constexpr Field def_public_access_block_fields[] = {
  {"block_public_acls", "5.23.0", Disposition::Inherit, &scalar},
  {"ignore_public_acls", "5.23.0", Disposition::Inherit, &scalar},
  {"block_public_policy", "5.23.0", Disposition::Inherit, &scalar},
  {"restrict_public_buckets", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_public_access_block{.kind = Kind::Object, .fields = def_public_access_block_fields};
constexpr Field def_object_lock_configuration_rule_default_retention_arm0_fields[] = {
  {"years", "5.23.0", Disposition::Inherit, &scalar},
  {"mode", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_object_lock_configuration_rule_default_retention_arm0{.kind = Kind::Object, .fields = def_object_lock_configuration_rule_default_retention_arm0_fields};
constexpr Field def_object_lock_configuration_rule_default_retention_arm1_fields[] = {
  {"days", "5.23.0", Disposition::Inherit, &scalar},
  {"mode", "5.23.0", Disposition::Inherit, &scalar},
};
constexpr Node def_object_lock_configuration_rule_default_retention_arm1{.kind = Kind::Object, .fields = def_object_lock_configuration_rule_default_retention_arm1_fields};
constexpr const Node* def_object_lock_configuration_rule_default_retention_arms[] = {&def_object_lock_configuration_rule_default_retention_arm0, &def_object_lock_configuration_rule_default_retention_arm1};
constexpr Node def_object_lock_configuration_rule_default_retention{.kind = Kind::AnyOf, .arms = def_object_lock_configuration_rule_default_retention_arms};
constexpr Field def_object_lock_configuration_rule_fields[] = {
  {"default_retention", "5.23.0", Disposition::Inherit, &def_object_lock_configuration_rule_default_retention},
};
constexpr Node def_object_lock_configuration_rule{.kind = Kind::Object, .fields = def_object_lock_configuration_rule_fields};
constexpr Field def_object_lock_configuration_fields[] = {
  {"object_lock_enabled", "5.23.0", Disposition::Inherit, &scalar},
  {"rule", "5.23.0", Disposition::Inherit, &def_object_lock_configuration_rule},
};
constexpr Node def_object_lock_configuration{.kind = Kind::Object, .fields = def_object_lock_configuration_fields};
constexpr Field bucket_record_fields[] = {
  {"_id", "5.23.0", Disposition::Translated, &scalar},
  {"owner_account", "5.23.0", Disposition::Translated, &scalar},
  {"creator", "5.23.0", Disposition::NotCarried, &scalar},
  {"name", "5.23.0", Disposition::NotCarried, &scalar},
  {"system_owner", "5.23.0", Disposition::NotCarried, &scalar},
  {"bucket_owner", "5.23.0", Disposition::NotCarried, &scalar},
  {"tag", "5.23.0", Disposition::Translated, &def_tagging},
  {"versioning", "5.23.0", Disposition::Translated, &scalar},
  {"path", "5.23.0", Disposition::Translated, &scalar},
  {"should_create_underlying_storage", "5.23.0", Disposition::Translated, &scalar},
  {"creation_date", "5.23.0", Disposition::Translated, &scalar},
  {"fs_backend", "5.23.0", Disposition::NotCarried, &scalar},
  {"s3_policy", "5.23.0", Disposition::Translated, &def_bucket_policy},
  {"encryption", "5.23.0", Disposition::Translated, &def_bucket_encryption},
  {"website", "5.23.0", Disposition::Translated, &def_bucket_website},
  {"force_md5_etag", "5.23.0", Disposition::Carried, &scalar},
  {"logging", "5.23.0", Disposition::Translated, &def_bucket_logging},
  {"lifecycle_configuration_rules", "5.23.0", Disposition::Translated, &def_bucket_lifecycle_configuration},
  {"notifications", "5.23.0", Disposition::NotCarried, &bucket_notifications},
  {"cors_configuration_rules", "5.23.0", Disposition::Translated, &def_bucket_cors_configuration},
  {"public_access_block", "5.23.0", Disposition::Translated, &def_public_access_block},
  {"object_lock_configuration", "5.23.0", Disposition::Translated, &def_object_lock_configuration},
};
constexpr Node bucket_record{.kind = Kind::Object, .fields = bucket_record_fields};

constexpr std::string_view releases[] = {"5.23.0"};

using Release = std::tuple<int, int, int>;

std::optional<Release> parse(std::string_view v)
{
  int part[3];
  const char* p = v.data();
  const char* end = v.data() + v.size();
  for (int i = 0; i < 3; ++i) {
    auto [next, ec] = std::from_chars(p, end, part[i]);
    if ((ec != std::errc{}) || (next == p)) {
      return std::nullopt;
    }
    p = next;
    if (i < 2) {
      if ((p == end) || (*p != '.')) {
        return std::nullopt;
      }
      ++p;
    }
  }
  if (p != end) {
    return std::nullopt;
  }
  return Release{part[0], part[1], part[2]};
}

const char* shape(JSONObj* v)
{
  return v->is_object() ? "an object" : v->is_array() ? "an array" : "a value";
}

/* Whether `v` holds to `n`.  On false, `why` says where it does not. */
bool holds(const Node& n, JSONObj* v, const Release& at,
           const std::string& path, std::string& why)
{
  switch (n.kind) {
  case Kind::Open:
    return true;

  case Kind::Scalar:
    if (v->is_object() || v->is_array()) {
      why = path + " is " + shape(v) + " where NooBaa writes a value";
      return false;
    }
    return true;

  case Kind::Object:
  case Kind::Map:
    if (!v->is_object()) {
      why = path + " is " + shape(v) + " where NooBaa writes an object";
      return false;
    }
    for (auto i = v->find_first(); !i.end(); ++i) {
      JSONObj* c = *i;
      const std::string at_key = path + "." + c->get_name();
      if (n.kind == Kind::Map) {
        if (!holds(*n.each, c, at, at_key, why)) {
          return false;
        }
        continue;
      }
      const Field* f = nullptr;
      for (const auto& candidate : n.fields) {
        if (c->get_name() == candidate.name) {
          f = &candidate;
          break;
        }
      }
      if (!f) {
        if (n.open) {
          continue;
        }
        why = at_key + " is not a field NooBaa writes";
        return false;
      }
      if (*parse(f->since) > at) {
        why = at_key + " is written from NooBaa " + f->since;
        return false;
      }
      if (!holds(*f->value, c, at, at_key, why)) {
        return false;
      }
    }
    return true;

  case Kind::Array:
    if (!v->is_array()) {
      why = path + " is " + shape(v) + " where NooBaa writes an array";
      return false;
    }
    {
      size_t idx = 0;
      for (auto i = v->find_first(); !i.end(); ++i, ++idx) {
        if (!holds(*n.each, *i, at, path + "[" + std::to_string(idx) + "]",
                   why)) {
          return false;
        }
      }
    }
    return true;

  case Kind::AnyOf:
    {
      std::string all;
      for (const Node* arm : n.arms) {
        std::string one;
        if (holds(*arm, v, at, path, one)) {
          return true;
        }
        all += (all.empty() ? "" : ";  ") + one;
      }
      why = path + " matches none of the forms NooBaa writes (" + all + ")";
      return false;
    }

  case Kind::AllOf:
    for (const Node* arm : n.arms) {
      if (!holds(*arm, v, at, path, why)) {
        return false;
      }
    }
    return true;
  }
  return false;
}

} // namespace

const Node& account = account_record;
const Node& bucket = bucket_record;
const std::span<const std::string_view> reviewed_releases{releases};

std::string unknown_field(const Node& schema, JSONObj* record,
                          std::string_view release)
{
  const Release oldest = *parse(releases[0]);
  const Release newest = *parse(releases[std::size(releases) - 1]);
  Release at = parse(release).value_or(newest);
  if (at < oldest) {
    at = oldest;
  }
  std::string why;
  if (holds(schema, record, at, "", why)) {
    return {};
  }
  return why;
}

}}}} // namespace rgw::sal::nsfs::noobaa_schema
