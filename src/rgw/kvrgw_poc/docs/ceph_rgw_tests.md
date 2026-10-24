# Ceph RGW S3 Tests — Coverage Map

Source: `~/clean/ceph/src/test/rgw/s3-tests/s3tests/functional/test_s3.py`

Last run: 2026-08-20

## Passed (204 tests)

| Test | Category |
|---|---|
| `test_bucket_list_empty` | list |
| `test_bucket_list_distinct` | list |
| `test_bucket_list_many` | list |
| `test_bucket_listv2_many` | list |
| `test_basic_key_count` | list |
| `test_bucket_list_prefix_basic` | list |
| `test_bucket_listv2_prefix_basic` | list |
| `test_bucket_list_prefix_alt` | list |
| `test_bucket_listv2_prefix_alt` | list |
| `test_bucket_list_prefix_empty` | list |
| `test_bucket_listv2_prefix_empty` | list |
| `test_bucket_list_prefix_none` | list |
| `test_bucket_listv2_prefix_none` | list |
| `test_bucket_list_prefix_not_exist` | list |
| `test_bucket_listv2_prefix_not_exist` | list |
| `test_bucket_list_maxkeys_one` | list |
| `test_bucket_listv2_maxkeys_one` | list |
| `test_bucket_list_maxkeys_zero` | list |
| `test_bucket_listv2_maxkeys_zero` | list |
| `test_bucket_list_maxkeys_none` | list |
| `test_bucket_listv2_maxkeys_none` | list |
| `test_bucket_list_maxkeys_invalid` | list |
| `test_bucket_list_marker_none` | list |
| `test_bucket_list_marker_empty` | list |
| `test_bucket_list_marker_not_in_list` | list |
| `test_bucket_list_marker_after_list` | list |
| `test_bucket_listv2_continuationtoken_empty` | list |
| `test_bucket_listv2_continuationtoken` | list |
| `test_bucket_listv2_both_continuationtoken_startafter` | list |
| `test_bucket_listv2_startafter_not_in_list` | list |
| `test_bucket_listv2_startafter_after_list` | list |
| `test_bucket_list_delimiter_basic` | list-delimiter |
| `test_bucket_list_delimiter_alt` | list-delimiter |
| `test_bucket_listv2_delimiter_alt` | list-delimiter |
| `test_bucket_list_delimiter_dot` | list-delimiter |
| `test_bucket_listv2_delimiter_dot` | list-delimiter |
| `test_bucket_list_delimiter_whitespace` | list-delimiter |
| `test_bucket_listv2_delimiter_whitespace` | list-delimiter |
| `test_bucket_list_delimiter_percentage` | list-delimiter |
| `test_bucket_listv2_delimiter_percentage` | list-delimiter |
| `test_bucket_list_delimiter_none` | list-delimiter |
| `test_bucket_listv2_delimiter_none` | list-delimiter |
| `test_bucket_list_delimiter_empty` | list-delimiter |
| `test_bucket_listv2_delimiter_empty` | list-delimiter |
| `test_bucket_list_delimiter_prefix_ends_with_delimiter` | list-delimiter |
| `test_bucket_listv2_delimiter_prefix_ends_with_delimiter` | list-delimiter |
| `test_bucket_list_delimiter_not_exist` | list-delimiter |
| `test_bucket_listv2_delimiter_not_exist` | list-delimiter |
| `test_bucket_list_delimiter_not_skip_special` | list-delimiter |
| `test_bucket_list_delimiter_unreadable` | list-delimiter |
| `test_bucket_listv2_delimiter_unreadable` | list-delimiter |
| `test_bucket_list_prefix_delimiter_basic` | list-prefix-delimiter |
| `test_bucket_listv2_prefix_delimiter_basic` | list-prefix-delimiter |
| `test_bucket_list_prefix_delimiter_alt` | list-prefix-delimiter |
| `test_bucket_listv2_prefix_delimiter_alt` | list-prefix-delimiter |
| `test_bucket_list_prefix_delimiter_delimiter_not_exist` | list-prefix-delimiter |
| `test_bucket_listv2_prefix_delimiter_delimiter_not_exist` | list-prefix-delimiter |
| `test_bucket_list_prefix_delimiter_prefix_not_exist` | list-prefix-delimiter |
| `test_bucket_listv2_prefix_delimiter_prefix_not_exist` | list-prefix-delimiter |
| `test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist` | list-prefix-delimiter |
| `test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist` | list-prefix-delimiter |
| `test_bucket_list_prefix_unreadable` | list-prefix-delimiter |
| `test_bucket_listv2_prefix_unreadable` | list-prefix-delimiter |
| `test_bucket_list_special_prefix` | list-special |
| `test_bucket_create_special_key_names` | list-special |
| `test_bucket_list_long_name` | list-special |
| `test_bucket_list_objects_anonymous_fail` | list-auth |
| `test_list_buckets_bad_auth` | list-auth |
| `test_list_buckets_paginated` | list-buckets |
| `test_buckets_list_ctime` | list-buckets |
| `test_buckets_create_then_list` | list-buckets |
| `test_bucket_notexist` | bucket |
| `test_bucketv2_notexist` | bucket |
| `test_bucket_create_delete` | bucket |
| `test_bucket_create_exists` | bucket |
| `test_bucket_delete_notexist` | bucket |
| `test_bucket_delete_nonowner` | bucket |
| `test_bucket_delete_nonempty` | bucket |
| `test_bucket_head` | bucket |
| `test_bucket_head_notexist` | bucket |
| `test_bucket_recreate_not_overriding` | bucket |
| `test_bucket_create_naming_good_starts_alpha` | bucket-naming |
| `test_bucket_create_naming_good_starts_digit` | bucket-naming |
| `test_bucket_create_naming_good_contains_period` | bucket-naming |
| `test_bucket_create_naming_good_contains_hyphen` | bucket-naming |
| `test_bucket_create_naming_good_long_60` | bucket-naming |
| `test_bucket_create_naming_good_long_61` | bucket-naming |
| `test_bucket_create_naming_good_long_62` | bucket-naming |
| `test_bucket_create_naming_good_long_63` | bucket-naming |
| `test_bucket_create_naming_bad_short_one` | bucket-naming |
| `test_bucket_create_naming_bad_short_two` | bucket-naming |
| `test_bucket_create_naming_bad_starts_nonalpha` | bucket-naming |
| `test_bucket_create_naming_bad_ip` | bucket-naming |
| `test_bucket_create_naming_dns_underscore` | bucket-naming |
| `test_bucket_create_naming_dns_dash_at_end` | bucket-naming |
| `test_bucket_create_naming_dns_dash_dot` | bucket-naming |
| `test_bucket_create_naming_dns_dot_dash` | bucket-naming |
| `test_bucket_create_naming_dns_dot_dot` | bucket-naming |
| `test_bucket_create_naming_dns_long` | bucket-naming |
| `test_object_write_read_update_read_delete` | object |
| `test_object_write_file` | object |
| `test_object_write_check_etag` | object |
| `test_object_head_zero_bytes` | object |
| `test_object_metadata_replaced_on_put` | object |
| `test_object_read_not_exist` | object |
| `test_object_write_to_nonexist_bucket` | object |
| `test_object_put_authenticated` | object |
| `test_object_anon_put` | object |
| `test_object_raw_authenticated_bucket_acl` | object |
| `test_object_raw_response_headers` | object |
| `test_object_requestid_matches_header_on_error` | object |
| `test_atomic_write_1mb` | atomic |
| `test_atomic_write_4mb` | atomic |
| `test_atomic_write_8mb` | atomic |
| `test_atomic_read_1mb` | atomic |
| `test_atomic_read_4mb` | atomic |
| `test_atomic_read_8mb` | atomic |
| `test_atomic_dual_write_1mb` | atomic |
| `test_atomic_dual_write_4mb` | atomic |
| `test_atomic_dual_write_8mb` | atomic |
| `test_atomic_conditional_write_1mb` | atomic |
| `test_multi_object_delete` | delete |
| `test_multi_objectv2_delete` | delete |
| `test_multi_object_delete_key_limit` | delete |
| `test_multi_objectv2_delete_key_limit` | delete |
| `test_ranged_request_response_code` | range |
| `test_ranged_big_request_response_code` | range |
| `test_ranged_request_skip_leading_bytes_response_code` | range |
| `test_ranged_request_return_trailing_bytes_response_code` | range |
| `test_ranged_request_empty_object` | range |
| `test_ranged_request_invalid_range` | range |
| `test_get_object_ifmatch_good` | conditional-get |
| `test_get_object_ifnonematch_failed` | conditional-get |
| `test_get_object_ifmodifiedsince_good` | conditional-get |
| `test_get_object_ifunmodifiedsince_failed` | conditional-get |
| `test_put_object_ifmatch_good` | conditional-put |
| `test_put_object_ifnonmatch_good` | conditional-put |
| `test_put_object_ifmatch_overwrite_existed_good` | conditional-put |
| `test_put_object_ifnonmatch_nonexisted_good` | conditional-put |
| `test_put_object_if_match` | conditional-put |
| `test_put_current_object_if_match` | conditional-put |
| `test_put_current_object_if_none_match` | conditional-put |
| `test_put_object_current_if_match` | conditional-put |
| `test_delete_object_current_if_match_last_modified_time` | conditional-delete |
| `test_delete_object_current_if_match_size` | conditional-delete |
| `test_delete_object_version_if_match` | conditional-delete |
| `test_delete_object_version_if_match_last_modified_time` | conditional-delete |
| `test_delete_object_version_if_match_size` | conditional-delete |
| `test_object_copy_zero_size` | copy |
| `test_object_copy_16m` | copy |
| `test_object_copy_same_bucket` | copy |
| `test_object_copy_diff_bucket` | copy |
| `test_object_copy_verify_contenttype` | copy |
| `test_object_copy_to_itself` | copy |
| `test_object_copy_bucket_not_found` | copy |
| `test_object_copy_key_not_found` | copy |
| `test_object_copy_versioned_bucket` | copy |
| `test_copy_object_ifmatch_good` | conditional-copy |
| `test_copy_object_ifmatch_failed` | conditional-copy |
| `test_copy_object_ifnonematch_good` | conditional-copy |
| `test_copy_object_ifnonematch_failed` | conditional-copy |
| `test_versioning_bucket_create_suspend` | versioning |
| `test_versioning_obj_create_read_remove` | versioning |
| `test_versioning_obj_create_read_remove_head` | versioning |
| `test_versioning_stack_delete_markers` | versioning |
| `test_versioning_obj_plain_null_version_removal` | versioning |
| `test_versioning_obj_plain_null_version_overwrite` | versioning |
| `test_versioning_obj_plain_null_version_overwrite_suspended` | versioning |
| `test_versioning_obj_suspend_versions` | versioning |
| `test_versioning_obj_create_versions_remove_all` | versioning |
| `test_versioning_obj_create_versions_remove_special_names` | versioning |
| `test_versioning_obj_list_marker` | versioning |
| `test_versioning_multi_object_delete` | versioning |
| `test_versioning_multi_object_delete_with_marker` | versioning |
| `test_versioning_multi_object_delete_with_marker_create` | versioning |
| `test_versioning_bucket_atomic_upload_return_version_id` | versioning |
| `test_versioning_obj_suspended_copy` | versioning |
| `test_versioning_copy_obj_version` | versioning |
| `test_delete_marker_versioned` | versioning |
| `test_delete_marker_suspended` | versioning |
| `test_versioned_concurrent_object_create_and_remove` | versioning-concurrent |
| `test_versioned_concurrent_object_create_concurrent_remove` | versioning-concurrent |
| `test_get_obj_tagging` | tagging |
| `test_get_obj_head_tagging` | tagging |
| `test_put_max_tags` | tagging |
| `test_put_excess_tags` | tagging |
| `test_put_max_kvsize_tags` | tagging |
| `test_put_excess_key_tags` | tagging |
| `test_put_excess_val_tags` | tagging |
| `test_put_modify_tags` | tagging |
| `test_put_delete_tags` | tagging |
| `test_put_obj_with_tags` | tagging |
| `test_get_tags_acl_public` | tagging |
| `test_put_tags_acl_public` | tagging |
| `test_delete_tags_obj_public` | tagging |
| `test_bucket_policy` | bucket-policy |
| `test_bucketv2_policy` | bucket-policy |
| `test_object_set_get_metadata_none_to_good` | metadata |
| `test_object_set_get_metadata_none_to_empty` | metadata |
| `test_object_set_get_metadata_overwrite_to_empty` | metadata |
| `test_object_copy_retaining_metadata` | metadata |
| `test_object_copy_replacing_metadata` | metadata |
| `test_object_copy_to_itself_with_metadata` | metadata |

## Deselected (33 tests)

| Test | Reason |
|---|---|
| `test_bucket_head_extended` | Requires `x-rgw-object-count` / `x-rgw-bytes-used` headers (RGW-specific) |
| `test_bucket_header_acl_grants` | Requires GetBucketAcl with grant details (ACLs disabled) |
| `test_bucket_create_delete_bucket_ownership` | Requires bucket ownership controls |
| `test_bucket_list_delimiter_prefix_underscore` | versitygw key normalization edge case |
| `test_bucket_listv2_delimiter_prefix_underscore` | versitygw key normalization edge case |
| `test_object_anon_put_write_access` | Requires ACL-based anonymous write (ACLs disabled) |
| `test_object_copy_canned_acl` | Requires ACL grant on copy (ACLs disabled) |
| `test_bucket_create_exists_nonowner` | Alt user denied CreateBucket (no IAM policy grants bucket creation) |
| `test_bucket_policy_acl` | Requires ACL grant evaluation inside policy (ACLs disabled) |
| `test_bucketv2_policy_acl` | Requires ACL grant evaluation inside policy (ACLs disabled) |
| `test_bucket_policy_different_tenant` | Requires multi-tenant routing |
| `test_bucket_policy_multipart` | Requires multipart upload |
| `test_bucket_policy_tenanted_bucket` | Requires multi-tenant routing |
| `test_bucket_policy_another_bucket` | Requires cross-bucket policy evaluation |
| `test_bucketv2_policy_another_bucket` | Requires cross-bucket policy evaluation |
| `test_bucket_policy_set_condition_operator_end_with_IfExists` | Advanced condition operator (IfExists) not implemented |
| `test_bucket_policy_get_obj_existing_tag` | Advanced condition (ExistingObjectTag) not implemented |
| `test_bucket_policy_get_obj_tagging_existing_tag` | Advanced condition (ExistingObjectTag) not implemented |
| `test_bucket_policy_put_obj_tagging_existing_tag` | Advanced condition (ExistingObjectTag) not implemented |
| `test_bucket_policy_upload_part_copy` | Requires multipart upload |
| `test_bucket_policy_put_obj_copy_source` | Advanced condition (x-amz-copy-source) not implemented |
| `test_bucket_policy_put_obj_copy_source_meta` | Advanced condition (x-amz-copy-source) not implemented |
| `test_bucket_policy_put_obj_acl` | Requires ACL grant in policy (ACLs disabled) |
| `test_bucket_policy_put_obj_grant` | Requires ACL grant in policy (ACLs disabled) |
| `test_bucket_policy_put_obj_s3_noenc` | Requires SSE-S3 encryption condition |
| `test_bucket_policy_put_obj_s3_incorrect_algo_sse_s3` | Requires SSE-S3 encryption condition |
| `test_bucket_policy_put_obj_kms_noenc` | Requires SSE-KMS encryption condition |
| `test_bucket_policy_put_obj_request_obj_tag` | Advanced condition (RequestObjectTag) not implemented |
| `test_bucket_policy_get_obj_acl_existing_tag` | Advanced condition (ExistingObjectTag) + ACL |
| `test_bucket_policy_allow_notprincipal` | NotPrincipal not implemented |
| `test_bucket_policy_deny_self_denied_policy` | Requires IAM identity self-denial evaluation |
| `test_bucket_policy_deny_self_denied_policy_confirm_header` | Requires IAM identity self-denial evaluation |

## Failing (0 tests)

None.

---

## Run Command

```bash
./scripts/run_ceph_rgw_tests.sh
```

Target: **204 passed, 0 failed, 0 errors**.
