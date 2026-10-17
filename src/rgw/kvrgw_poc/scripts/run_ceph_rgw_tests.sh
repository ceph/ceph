#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@redhat.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

S3TESTS_DIR="${S3TESTS_DIR:-../../test/rgw/s3-tests}"
S3TEST_CONF="${S3TEST_CONF:-$ROOT/s3tests.conf}"

CATEGORY=""
if [[ "${1:-}" == "--category" || "${1:-}" == "-c" ]]; then
    CATEGORY="${2:-}"
    if [[ -z "$CATEGORY" ]]; then
        echo "Usage: $0 [--category CATEGORY]"
        echo "Categories: list, list-delimiter, list-prefix-delimiter, list-special,"
        echo "  list-auth, list-buckets, bucket, bucket-naming, object, atomic,"
        echo "  delete, range, conditional-get, conditional-put, conditional-delete,"
        echo "  conditional-copy, copy, versioning, versioning-concurrent, tagging,"
        echo "  post, object-lock"
        exit 1
    fi
fi

if [ ! -d "$S3TESTS_DIR" ]; then
    echo "ERROR: s3-tests not found at $S3TESTS_DIR"
    echo "Set S3TESTS_DIR to point to your ceph s3-tests checkout"
    exit 1
fi

declare -A TESTS_BY_CATEGORY
TESTS_BY_CATEGORY[list]="test_bucket_list_empty or test_bucket_list_distinct or test_bucket_list_many or test_bucket_listv2_many or test_basic_key_count or test_bucket_list_prefix_basic or test_bucket_listv2_prefix_basic or test_bucket_list_prefix_alt or test_bucket_listv2_prefix_alt or test_bucket_list_prefix_empty or test_bucket_listv2_prefix_empty or test_bucket_list_prefix_none or test_bucket_listv2_prefix_none or test_bucket_list_prefix_not_exist or test_bucket_listv2_prefix_not_exist or test_bucket_list_maxkeys_one or test_bucket_listv2_maxkeys_one or test_bucket_list_maxkeys_zero or test_bucket_listv2_maxkeys_zero or test_bucket_list_maxkeys_none or test_bucket_listv2_maxkeys_none or test_bucket_list_maxkeys_invalid or test_bucket_list_marker_none or test_bucket_list_marker_empty or test_bucket_list_marker_not_in_list or test_bucket_list_marker_after_list or test_bucket_list_marker_unreadable or test_bucket_listv2_continuationtoken_empty or test_bucket_listv2_continuationtoken or test_bucket_listv2_both_continuationtoken_startafter or test_bucket_listv2_startafter_not_in_list or test_bucket_listv2_startafter_after_list or test_bucket_listv2_startafter_unreadable or test_bucket_listv2_fetchowner_defaultempty or test_bucket_listv2_fetchowner_empty"

TESTS_BY_CATEGORY[list-delimiter]="test_bucket_list_delimiter_basic or test_bucket_list_delimiter_alt or test_bucket_listv2_delimiter_alt or test_bucket_list_delimiter_dot or test_bucket_listv2_delimiter_dot or test_bucket_list_delimiter_whitespace or test_bucket_listv2_delimiter_whitespace or test_bucket_list_delimiter_percentage or test_bucket_listv2_delimiter_percentage or test_bucket_list_delimiter_none or test_bucket_listv2_delimiter_none or test_bucket_list_delimiter_empty or test_bucket_listv2_delimiter_empty or test_bucket_list_delimiter_prefix_ends_with_delimiter or test_bucket_listv2_delimiter_prefix_ends_with_delimiter or test_bucket_list_delimiter_not_exist or test_bucket_listv2_delimiter_not_exist or test_bucket_list_delimiter_not_skip_special or test_bucket_list_delimiter_unreadable or test_bucket_listv2_delimiter_unreadable"

TESTS_BY_CATEGORY[list-prefix-delimiter]="test_bucket_list_prefix_delimiter_basic or test_bucket_listv2_prefix_delimiter_basic or test_bucket_list_prefix_delimiter_alt or test_bucket_listv2_prefix_delimiter_alt or test_bucket_list_prefix_delimiter_delimiter_not_exist or test_bucket_listv2_prefix_delimiter_delimiter_not_exist or test_bucket_list_prefix_delimiter_prefix_not_exist or test_bucket_listv2_prefix_delimiter_prefix_not_exist or test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist or test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist or test_bucket_list_prefix_unreadable or test_bucket_listv2_prefix_unreadable"

TESTS_BY_CATEGORY[list-special]="test_bucket_list_special_prefix or test_bucket_create_special_key_names or test_bucket_list_long_name"

TESTS_BY_CATEGORY[list-auth]="test_bucket_list_objects_anonymous_fail or test_bucket_listv2_objects_anonymous_fail or test_list_buckets_bad_auth or test_list_buckets_invalid_auth"

TESTS_BY_CATEGORY[list-buckets]="test_list_buckets_paginated or test_buckets_list_ctime or test_buckets_create_then_list"

TESTS_BY_CATEGORY[bucket]="test_bucket_notexist or test_bucketv2_notexist or test_bucket_create_delete or test_bucket_create_exists or test_bucket_delete_notexist or test_bucket_delete_nonowner or test_bucket_delete_nonempty or test_bucket_head or test_bucket_head_notexist or test_bucket_recreate_not_overriding"

TESTS_BY_CATEGORY[bucket-naming]="test_bucket_create_naming_good_starts_alpha or test_bucket_create_naming_good_starts_digit or test_bucket_create_naming_good_contains_period or test_bucket_create_naming_good_contains_hyphen or test_bucket_create_naming_good_long_60 or test_bucket_create_naming_good_long_61 or test_bucket_create_naming_good_long_62 or test_bucket_create_naming_good_long_63 or test_bucket_create_naming_bad_short_one or test_bucket_create_naming_bad_short_two or test_bucket_create_naming_bad_starts_nonalpha or test_bucket_create_naming_bad_ip or test_bucket_create_naming_dns_underscore or test_bucket_create_naming_dns_dash_at_end or test_bucket_create_naming_dns_dash_dot or test_bucket_create_naming_dns_dot_dash or test_bucket_create_naming_dns_dot_dot or test_bucket_create_naming_dns_long"

TESTS_BY_CATEGORY[object]="test_object_write_read_update_read_delete or test_object_write_file or test_object_write_check_etag or test_object_head_zero_bytes or test_object_metadata_replaced_on_put or test_object_read_not_exist or test_object_write_to_nonexist_bucket or test_object_put_authenticated or test_object_anon_put or test_object_raw_authenticated_bucket_acl or test_object_raw_response_headers or test_object_requestid_matches_header_on_error or test_object_delete_key_bucket_gone or test_object_presigned_put_object_with_acl or test_object_presigned_put_object_with_acl_tenant or test_object_put_acl_mtime or test_object_raw_authenticated or test_object_raw_authenticated_object_acl or test_object_raw_authenticated_object_gone or test_object_raw_get_bucket_gone or test_object_raw_get_object_acl or test_object_raw_get_x_amz_expires_not_expired or test_object_raw_get_x_amz_expires_not_expired_tenant or test_object_raw_get_x_amz_expires_out_range_zero"

TESTS_BY_CATEGORY[atomic]="test_atomic_write_1mb or test_atomic_write_4mb or test_atomic_write_8mb or test_atomic_read_1mb or test_atomic_read_4mb or test_atomic_read_8mb or test_atomic_dual_write_1mb or test_atomic_dual_write_4mb or test_atomic_dual_write_8mb or test_atomic_conditional_write_1mb or test_atomic_write_bucket_gone"

TESTS_BY_CATEGORY[delete]="test_multi_object_delete or test_multi_objectv2_delete or test_multi_object_delete_key_limit or test_multi_objectv2_delete_key_limit"

TESTS_BY_CATEGORY[range]="test_ranged_request_response_code or test_ranged_big_request_response_code or test_ranged_request_skip_leading_bytes_response_code or test_ranged_request_return_trailing_bytes_response_code or test_ranged_request_empty_object or test_ranged_request_invalid_range"

TESTS_BY_CATEGORY[conditional-get]="test_get_object_ifmatch_good or test_get_object_ifnonematch_failed or test_get_object_ifmodifiedsince_good or test_get_object_ifunmodifiedsince_failed"

TESTS_BY_CATEGORY[conditional-put]="test_put_object_ifmatch_good or test_put_object_ifnonmatch_good or test_put_object_ifmatch_overwrite_existed_good or test_put_object_ifnonmatch_nonexisted_good or test_put_object_if_match or test_put_current_object_if_match or test_put_current_object_if_none_match or test_put_object_current_if_match or test_put_object_ifmatch_failed or test_put_object_ifmatch_nonexisted_failed or test_put_object_ifnonmatch_failed or test_put_object_ifnonmatch_overwrite_existed_failed"

TESTS_BY_CATEGORY[conditional-delete]="test_delete_object_current_if_match_last_modified_time or test_delete_object_current_if_match_size or test_delete_object_version_if_match or test_delete_object_version_if_match_last_modified_time or test_delete_object_version_if_match_size"

TESTS_BY_CATEGORY[conditional-copy]="test_copy_object_ifmatch_good or test_copy_object_ifmatch_failed or test_copy_object_ifnonematch_good or test_copy_object_ifnonematch_failed"

TESTS_BY_CATEGORY[copy]="test_object_copy_zero_size or test_object_copy_16m or test_object_copy_same_bucket or test_object_copy_diff_bucket or test_object_copy_verify_contenttype or test_object_copy_to_itself or test_object_copy_to_itself_replacing_tagging or test_object_copy_to_itself_with_metadata or test_object_copy_bucket_not_found or test_object_copy_key_not_found or test_object_copy_versioned_bucket or test_object_copy_replacing_metadata_and_copying_tagging or test_object_copy_replacing_tagging or test_object_copy_retaining_tagging or test_object_copy_tagging_directive_copy or test_object_copy_tagging_ignored_without_directive"

TESTS_BY_CATEGORY[versioning]="test_versioning_bucket_create_suspend or test_versioning_obj_create_read_remove or test_versioning_obj_create_read_remove_head or test_versioning_stack_delete_markers or test_versioning_obj_plain_null_version_removal or test_versioning_obj_plain_null_version_overwrite or test_versioning_obj_plain_null_version_overwrite_suspended or test_versioning_obj_suspend_versions or test_versioning_obj_create_versions_remove_all or test_versioning_obj_create_versions_remove_special_names or test_versioning_obj_list_marker or test_versioning_multi_object_delete or test_versioning_multi_object_delete_with_marker or test_versioning_multi_object_delete_with_marker_create or test_versioning_bucket_atomic_upload_return_version_id or test_versioning_obj_suspended_copy or test_versioning_copy_obj_version or test_delete_marker_versioned or test_delete_marker_suspended"

TESTS_BY_CATEGORY[versioning-concurrent]="test_versioned_concurrent_object_create_and_remove or test_versioned_concurrent_object_create_concurrent_remove or test_versioning_concurrent_multi_object_delete"

TESTS_BY_CATEGORY[bucket-policy]="test_bucket_policy or test_bucketv2_policy or test_set_get_del_bucket_policy or test_bucket_policy_put_obj_kms_s3 or test_bucket_policy_put_obj_s3_kms"

TESTS_BY_CATEGORY[metadata]="test_object_set_get_metadata_none_to_good or test_object_set_get_metadata_none_to_empty or test_object_set_get_metadata_overwrite_to_empty or test_object_copy_retaining_metadata or test_object_copy_replacing_metadata"

TESTS_BY_CATEGORY[tagging]="test_get_obj_tagging or test_get_obj_head_tagging or test_put_max_tags or test_put_excess_tags or test_put_max_kvsize_tags or test_put_excess_key_tags or test_put_excess_val_tags or test_put_modify_tags or test_put_delete_tags or test_put_obj_with_tags or test_get_tags_acl_public or test_put_tags_acl_public or test_delete_tags_obj_public"

TESTS_BY_CATEGORY[post]="test_post_object_condition_is_case_sensitive or test_post_object_empty_conditions or test_post_object_expires_is_case_sensitive or test_post_object_invalid_content_length_argument or test_post_object_invalid_date_format or test_post_object_missing_conditions_list or test_post_object_missing_content_length_argument or test_post_object_missing_expires_condition or test_post_object_missing_signature or test_post_object_no_key_specified or test_post_object_upload_size_below_minimum or test_post_object_upload_size_limit_exceeded"

TESTS_BY_CATEGORY[object-lock]="test_object_lock_get_obj_lock_invalid_bucket or test_object_lock_put_legal_hold_invalid_status or test_object_lock_put_obj_lock_invalid_mode or test_object_lock_put_obj_lock_invalid_status or test_object_lock_put_obj_lock_with_days_and_years or test_object_lock_put_obj_retention_invalid_mode"

DESELECTS="\
--deselect s3tests/functional/test_s3.py::test_object_copy_replacing_tagging_with_none \
--deselect s3tests/functional/test_s3.py::test_bucket_head_extended \
--deselect s3tests/functional/test_s3.py::test_bucket_header_acl_grants \
--deselect s3tests/functional/test_s3.py::test_bucket_create_delete_bucket_ownership \
--deselect s3tests/functional/test_s3.py::test_object_anon_put_write_access \
--deselect s3tests/functional/test_s3.py::test_object_copy_canned_acl \
--deselect s3tests/functional/test_s3.py::test_bucket_create_exists_nonowner \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_acl \
--deselect s3tests/functional/test_s3.py::test_bucketv2_policy_acl \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_different_tenant \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_multipart \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_tenanted_bucket \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_another_bucket \
--deselect s3tests/functional/test_s3.py::test_bucketv2_policy_another_bucket \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_set_condition_operator_end_with_IfExists \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_get_obj_existing_tag \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_get_obj_tagging_existing_tag \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_put_obj_tagging_existing_tag \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_upload_part_copy \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_put_obj_copy_source \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_put_obj_copy_source_meta \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_put_obj_acl \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_put_obj_grant \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_put_obj_s3_noenc \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_put_obj_s3_incorrect_algo_sse_s3 \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_put_obj_kms_noenc \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_put_obj_request_obj_tag \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_get_obj_acl_existing_tag \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_allow_notprincipal \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_deny_self_denied_policy \
--deselect s3tests/functional/test_s3.py::test_bucket_policy_deny_self_denied_policy_confirm_header"

if [[ -n "$CATEGORY" ]]; then
    if [[ -z "${TESTS_BY_CATEGORY[$CATEGORY]:-}" ]]; then
        echo "ERROR: Unknown category '$CATEGORY'"
        echo "Available: ${!TESTS_BY_CATEGORY[*]}"
        exit 1
    fi
    TEST_EXPR="${TESTS_BY_CATEGORY[$CATEGORY]}"
else
    # All categories combined
    TEST_EXPR=""
    for cat in "${!TESTS_BY_CATEGORY[@]}"; do
        if [[ -n "$TEST_EXPR" ]]; then
            TEST_EXPR="$TEST_EXPR or ${TESTS_BY_CATEGORY[$cat]}"
        else
            TEST_EXPR="${TESTS_BY_CATEGORY[$cat]}"
        fi
    done
fi

cd "$S3TESTS_DIR"

# shellcheck disable=SC2086
S3TEST_CONF="$S3TEST_CONF" python3.11 -m pytest s3tests/functional/test_s3.py \
    -k "$TEST_EXPR" \
    $DESELECTS \
    --tb=line -q
