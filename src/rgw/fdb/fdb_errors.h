// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025-2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_FDB_ERRORS_H
 #define CEPH_FDB_ERRORS_H

// The API version we're writing against, which can (and probably does) differ
// from the installed version. This must be defined before the FoundationDB header
// is included (see fdb_c_apiversion.g.h):
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

namespace ceph::libfdb {

/*
 * Native error names and values from FoundationDB 7.3.79:
 * flow/include/flow/error_definitions.h, commit
 * 0074653ee0113fc39da0fa088b056c7210ab3191.
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this table except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The fixed int representation also preserves unknown native codes through
 * static_cast<fdb_error_code>(code) and std::to_underlying(). Messages and
 * retry predicates come from the loaded C client, not this table.
 */
enum struct fdb_error_code : int {
 success                                           = 0,
 end_of_stream                                     = 1,
 operation_failed                                  = 1000,
 wrong_shard_server                                = 1001,
 operation_obsolete                                = 1002,
 cold_cache_server                                 = 1003,
 timed_out                                         = 1004,
 coordinated_state_conflict                        = 1005,
 all_alternatives_failed                           = 1006,
 transaction_too_old                               = 1007,
 no_more_servers                                   = 1008,
 future_version                                    = 1009,
 movekeys_conflict                                 = 1010,
 tlog_stopped                                      = 1011,
 server_request_queue_full                         = 1012,
 not_committed                                     = 1020,
 commit_unknown_result                             = 1021,
 commit_unknown_result_fatal                       = 1022,
 transaction_cancelled                             = 1025,
 connection_failed                                 = 1026,
 coordinators_changed                              = 1027,
 new_coordinators_timed_out                        = 1028,
 watch_cancelled                                   = 1029,
 request_maybe_delivered                           = 1030,
 transaction_timed_out                             = 1031,
 too_many_watches                                  = 1032,
 locality_information_unavailable                  = 1033,
 watches_disabled                                  = 1034,
 default_error_or                                  = 1035,
 accessed_unreadable                               = 1036,
 process_behind                                    = 1037,
 database_locked                                   = 1038,
 cluster_version_changed                           = 1039,
 external_client_already_loaded                    = 1040,
 lookup_failed                                     = 1041,
 commit_proxy_memory_limit_exceeded                = 1042,
 shutdown_in_progress                              = 1043,
 serialization_failed                              = 1044,
 connection_unreferenced                           = 1048,
 connection_idle                                   = 1049,
 disk_adapter_reset                                = 1050,
 batch_transaction_throttled                       = 1051,
 dd_cancelled                                      = 1052,
 dd_not_found                                      = 1053,
 wrong_connection_file                             = 1054,
 version_already_compacted                         = 1055,
 local_config_changed                              = 1056,
 failed_to_reach_quorum                            = 1057,
 unsupported_format_version                        = 1058,
 unknown_change_feed                               = 1059,
 change_feed_not_registered                        = 1060,
 granule_assignment_conflict                       = 1061,
 change_feed_cancelled                             = 1062,
 blob_granule_file_load_error                      = 1063,
 blob_granule_transaction_too_old                  = 1064,
 blob_manager_replaced                             = 1065,
 change_feed_popped                                = 1066,
 remote_kvs_cancelled                              = 1067,
 page_header_wrong_page_id                         = 1068,
 page_header_checksum_failed                       = 1069,
 page_header_version_not_supported                 = 1070,
 page_encoding_not_supported                       = 1071,
 page_decoding_failed                              = 1072,
 unexpected_encoding_type                          = 1073,
 encryption_key_not_found                          = 1074,
 data_move_cancelled                               = 1075,
 data_move_dest_team_not_found                     = 1076,
 blob_worker_full                                  = 1077,
 grv_proxy_memory_limit_exceeded                   = 1078,
 blob_granule_request_failed                       = 1079,
 storage_too_many_feed_streams                     = 1080,
 storage_engine_not_initialized                    = 1081,
 unknown_storage_engine                            = 1082,
 duplicate_snapshot_request                        = 1083,
 dd_config_changed                                 = 1084,
 consistency_check_urgent_task_failed              = 1085,
 data_move_conflict                                = 1086,
 consistency_check_urgent_duplicate_request        = 1087,
 broken_promise                                    = 1100,
 operation_cancelled                               = 1101,
 future_released                                   = 1102,
 connection_leaked                                 = 1103,
 never_reply                                       = 1104,
 retry                                             = 1105,
 recruitment_failed                                = 1200,
 move_to_removed_server                            = 1201,
 worker_removed                                    = 1202,
 cluster_recovery_failed                           = 1203,
 master_max_versions_in_flight                     = 1204,
 tlog_failed                                       = 1205,
 worker_recovery_failed                            = 1206,
 please_reboot                                     = 1207,
 please_reboot_delete                              = 1208,
 commit_proxy_failed                               = 1209,
 resolver_failed                                   = 1210,
 server_overloaded                                 = 1211,
 backup_worker_failed                              = 1212,
 tag_throttled                                     = 1213,
 grv_proxy_failed                                  = 1214,
 dd_tracker_cancelled                              = 1215,
 failed_to_progress                                = 1216,
 invalid_cluster_id                                = 1217,
 restart_cluster_controller                        = 1218,
 please_reboot_kv_store                            = 1219,
 incompatible_software_version                     = 1220,
 audit_storage_failed                              = 1221,
 audit_storage_exceeded_request_limit              = 1222,
 proxy_tag_throttled                               = 1223,
 key_value_store_deadline_exceeded                 = 1224,
 storage_quota_exceeded                            = 1225,
 audit_storage_error                               = 1226,
 master_failed                                     = 1227,
 test_failed                                       = 1228,
 retry_clean_up_datamove_tombstone_added           = 1229,
 persist_new_audit_metadata_error                  = 1230,
 cancel_audit_storage_failed                       = 1231,
 audit_storage_cancelled                           = 1232,
 location_metadata_corruption                      = 1233,
 audit_storage_task_outdated                       = 1234,
 finish_move_keys_too_many_retries                 = 1235,
 platform_error                                    = 1500,
 large_alloc_failed                                = 1501,
 performance_counter_error                         = 1502,
 bad_allocator                                     = 1503,
 io_error                                          = 1510,
 file_not_found                                    = 1511,
 bind_failed                                       = 1512,
 file_not_readable                                 = 1513,
 file_not_writable                                 = 1514,
 no_cluster_file_found                             = 1515,
 file_too_large                                    = 1516,
 non_sequential_op                                 = 1517,
 http_bad_response                                 = 1518,
 http_not_accepted                                 = 1519,
 checksum_failed                                   = 1520,
 io_timeout                                        = 1521,
 file_corrupt                                      = 1522,
 http_request_failed                               = 1523,
 http_auth_failed                                  = 1524,
 http_bad_request_id                               = 1525,
 rest_invalid_uri                                  = 1526,
 rest_invalid_rest_client_knob                     = 1527,
 rest_connectpool_key_not_found                    = 1528,
 lock_file_failure                                 = 1529,
 rest_unsupported_protocol                         = 1530,
 rest_malformed_response                           = 1531,
 rest_max_base_cipher_len                          = 1532,
 client_invalid_operation                          = 2000,
 commit_read_incomplete                            = 2002,
 test_specification_invalid                        = 2003,
 key_outside_legal_range                           = 2004,
 inverted_range                                    = 2005,
 invalid_option_value                              = 2006,
 invalid_option                                    = 2007,
 network_not_setup                                 = 2008,
 network_already_setup                             = 2009,
 read_version_already_set                          = 2010,
 version_invalid                                   = 2011,
 range_limits_invalid                              = 2012,
 invalid_database_name                             = 2013,
 attribute_not_found                               = 2014,
 future_not_set                                    = 2015,
 future_not_error                                  = 2016,
 used_during_commit                                = 2017,
 invalid_mutation_type                             = 2018,
 attribute_too_large                               = 2019,
 transaction_invalid_version                       = 2020,
 no_commit_version                                 = 2021,
 environment_variable_network_option_failed        = 2022,
 transaction_read_only                             = 2023,
 invalid_cache_eviction_policy                     = 2024,
 network_cannot_be_restarted                       = 2025,
 blocked_from_network_thread                       = 2026,
 invalid_config_db_range_read                      = 2027,
 invalid_config_db_key                             = 2028,
 invalid_config_path                               = 2029,
 mapper_bad_index                                  = 2030,
 mapper_no_such_key                                = 2031,
 mapper_bad_range_decriptor                        = 2032,
 quick_get_key_values_has_more                     = 2033,
 quick_get_value_miss                              = 2034,
 quick_get_key_values_miss                         = 2035,
 blob_granule_no_ryw                               = 2036,
 blob_granule_not_materialized                     = 2037,
 get_mapped_key_values_has_more                    = 2038,
 get_mapped_range_reads_your_writes                = 2039,
 checkpoint_not_found                              = 2040,
 key_not_tuple                                     = 2041,
 value_not_tuple                                   = 2042,
 mapper_not_tuple                                  = 2043,
 invalid_checkpoint_format                         = 2044,
 invalid_throttle_quota_value                      = 2045,
 failed_to_create_checkpoint                       = 2046,
 failed_to_restore_checkpoint                      = 2047,
 failed_to_create_checkpoint_shard_metadata        = 2048,
 address_parse_error                               = 2049,
 incompatible_protocol_version                     = 2100,
 transaction_too_large                             = 2101,
 key_too_large                                     = 2102,
 value_too_large                                   = 2103,
 connection_string_invalid                         = 2104,
 address_in_use                                    = 2105,
 invalid_local_address                             = 2106,
 tls_error                                         = 2107,
 unsupported_operation                             = 2108,
 too_many_tags                                     = 2109,
 tag_too_long                                      = 2110,
 too_many_tag_throttles                            = 2111,
 special_keys_cross_module_read                    = 2112,
 special_keys_no_module_found                      = 2113,
 special_keys_write_disabled                       = 2114,
 special_keys_no_write_module_found                = 2115,
 special_keys_cross_module_clear                   = 2116,
 special_keys_api_failure                          = 2117,
 client_lib_invalid_metadata                       = 2118,
 client_lib_already_exists                         = 2119,
 client_lib_not_found                              = 2120,
 client_lib_not_available                          = 2121,
 client_lib_invalid_binary                         = 2122,
 no_external_client_provided                       = 2123,
 all_external_clients_failed                       = 2124,
 incompatible_client                               = 2125,
 tenant_name_required                              = 2130,
 tenant_not_found                                  = 2131,
 tenant_already_exists                             = 2132,
 tenant_not_empty                                  = 2133,
 invalid_tenant_name                               = 2134,
 tenant_prefix_allocator_conflict                  = 2135,
 tenants_disabled                                  = 2136,
 illegal_tenant_access                             = 2138,
 invalid_tenant_group_name                         = 2139,
 invalid_tenant_configuration                      = 2140,
 cluster_no_capacity                               = 2141,
 tenant_removed                                    = 2142,
 invalid_tenant_state                              = 2143,
 tenant_locked                                     = 2144,
 invalid_cluster_name                              = 2160,
 invalid_metacluster_operation                     = 2161,
 cluster_already_exists                            = 2162,
 cluster_not_found                                 = 2163,
 cluster_not_empty                                 = 2164,
 cluster_already_registered                        = 2165,
 metacluster_no_capacity                           = 2166,
 management_cluster_invalid_access                 = 2167,
 tenant_creation_permanently_failed                = 2168,
 cluster_removed                                   = 2169,
 cluster_restoring                                 = 2170,
 invalid_data_cluster                              = 2171,
 metacluster_mismatch                              = 2172,
 conflicting_restore                               = 2173,
 invalid_metacluster_configuration                 = 2174,
 api_version_unset                                 = 2200,
 api_version_already_set                           = 2201,
 api_version_invalid                               = 2202,
 api_version_not_supported                         = 2203,
 api_function_missing                              = 2204,
 exact_mode_without_limits                         = 2210,
 invalid_tuple_data_type                           = 2250,
 invalid_tuple_index                               = 2251,
 key_not_in_subspace                               = 2252,
 manual_prefixes_not_enabled                       = 2253,
 prefix_in_partition                               = 2254,
 cannot_open_root_directory                        = 2255,
 directory_already_exists                          = 2256,
 directory_does_not_exist                          = 2257,
 parent_directory_does_not_exist                   = 2258,
 mismatched_layer                                  = 2259,
 invalid_directory_layer_metadata                  = 2260,
 cannot_move_directory_between_partitions          = 2261,
 cannot_use_partition_as_subspace                  = 2262,
 incompatible_directory_version                    = 2263,
 directory_prefix_not_empty                        = 2264,
 directory_prefix_in_use                           = 2265,
 invalid_destination_directory                     = 2266,
 cannot_modify_root_directory                      = 2267,
 invalid_uuid_size                                 = 2268,
 invalid_versionstamp_size                         = 2269,
 backup_error                                      = 2300,
 restore_error                                     = 2301,
 backup_duplicate                                  = 2311,
 backup_unneeded                                   = 2312,
 backup_bad_block_size                             = 2313,
 backup_invalid_url                                = 2314,
 backup_invalid_info                               = 2315,
 backup_cannot_expire                              = 2316,
 backup_auth_missing                               = 2317,
 backup_auth_unreadable                            = 2318,
 backup_does_not_exist                             = 2319,
 backup_not_filterable_with_key_ranges             = 2320,
 backup_not_overlapped_with_keys_filter            = 2321,
 restore_invalid_version                           = 2361,
 restore_corrupted_data                            = 2362,
 restore_missing_data                              = 2363,
 restore_duplicate_tag                             = 2364,
 restore_unknown_tag                               = 2365,
 restore_unknown_file_type                         = 2366,
 restore_unsupported_file_version                  = 2367,
 restore_bad_read                                  = 2368,
 restore_corrupted_data_padding                    = 2369,
 restore_destination_not_empty                     = 2370,
 restore_duplicate_uid                             = 2371,
 task_invalid_version                              = 2381,
 task_interrupted                                  = 2382,
 invalid_encryption_key_file                       = 2383,
 blob_restore_missing_logs                         = 2384,
 blob_restore_corrupted_logs                       = 2385,
 blob_restore_invalid_manifest_url                 = 2386,
 blob_restore_corrupted_manifest                   = 2387,
 blob_restore_missing_manifest                     = 2388,
 key_not_found                                     = 2400,
 json_malformed                                    = 2401,
 json_eof_expected                                 = 2402,
 snap_disable_tlog_pop_failed                      = 2500,
 snap_storage_failed                               = 2501,
 snap_tlog_failed                                  = 2502,
 snap_coord_failed                                 = 2503,
 snap_enable_tlog_pop_failed                       = 2504,
 snap_path_not_whitelisted                         = 2505,
 snap_not_fully_recovered_unsupported              = 2506,
 snap_log_anti_quorum_unsupported                  = 2507,
 snap_with_recovery_unsupported                    = 2508,
 snap_invalid_uid_string                           = 2509,
 encrypt_ops_error                                 = 2700,
 encrypt_header_metadata_mismatch                  = 2701,
 encrypt_key_not_found                             = 2702,
 encrypt_key_ttl_expired                           = 2703,
 encrypt_header_authtoken_mismatch                 = 2704,
 encrypt_update_cipher                             = 2705,
 encrypt_invalid_id                                = 2706,
 encrypt_keys_fetch_failed                         = 2707,
 encrypt_invalid_kms_config                        = 2708,
 encrypt_unsupported                               = 2709,
 encrypt_mode_mismatch                             = 2710,
 encrypt_key_check_value_mismatch                  = 2711,
 encrypt_max_base_cipher_len                       = 2712,
 unknown_error                                     = 4000,
 internal_error                                    = 4100,
 not_implemented                                   = 4200,
 permission_denied                                 = 6000,
 unauthorized_attempt                              = 6001,
 digital_signature_ops_error                       = 6002,
 authorization_token_verify_failed                 = 6003,
 pkey_decode_error                                 = 6004,
 pkey_encode_error                                 = 6005
};

enum struct libfdb_error_code : int {
 invalid_result = 1,
 client_shutdown = 2,
 operation_failed = 3
};

namespace detail {

struct fdb_error_category final : std::error_category
{
 static const fdb_error_category instance;

 private:
 constexpr fdb_error_category() noexcept = default;

 public:
 const char *name() const noexcept override { return "FoundationDB"; }

 std::string message(const int value) const override
 {
  return "FoundationDB error " + std::to_string(value) + ": " + fdb_get_error(value);
 }

};

struct libfdb_error_category final : std::error_category
{
 static const libfdb_error_category instance;

 private:
 constexpr libfdb_error_category() noexcept = default;

 public:
 const char *name() const noexcept override { return "libfdb"; }

 std::string message(const int value) const override
 {
  switch (static_cast<libfdb_error_code>(value)) {
    default:
           return "Unknown libfdb error " + std::to_string(value);
    case libfdb_error_code::invalid_result:
           return "Invalid result returned by FoundationDB";
    case libfdb_error_code::client_shutdown:
           return "FoundationDB client has shut down";
    case libfdb_error_code::operation_failed:
           return "libfdb operation failed";
  }
 }

};

// Inline instances preserve category identity across translation units and
// RGW's shared-library boundaries, without initialization on each access:
inline constinit const fdb_error_category fdb_error_category::instance;
inline constinit const libfdb_error_category libfdb_error_category::instance;

} // namespace detail

[[nodiscard]] inline const std::error_category& fdb_error_category() noexcept
{
 return detail::fdb_error_category::instance;
}

[[nodiscard]] inline const std::error_category& libfdb_error_category() noexcept
{
 return detail::libfdb_error_category::instance;
}

[[nodiscard]] inline std::error_code make_error_code(const fdb_error_code code) noexcept
{
 return {std::to_underlying(code), fdb_error_category()};
}

[[nodiscard]] inline std::error_code make_error_code(const libfdb_error_code code) noexcept
{
 return {std::to_underlying(code), libfdb_error_category()};
}

[[nodiscard]] inline bool retryable(const std::error_code code) noexcept
{
 return fdb_error_category() == code.category() and
        fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, code.value());
}

[[nodiscard]] inline bool maybe_committed(const std::error_code code) noexcept
{
 return fdb_error_category() == code.category() and
        fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED, code.value());
}

[[nodiscard]] inline bool retryable_not_committed(const std::error_code code) noexcept
{
 return fdb_error_category() == code.category() and
        fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED, code.value());
}

} // namespace ceph::libfdb

namespace std {

template <>
struct is_error_code_enum<::ceph::libfdb::fdb_error_code> : true_type {};

template <>
struct is_error_code_enum<::ceph::libfdb::libfdb_error_code> : true_type {};

} // namespace std

namespace ceph::libfdb {

struct libfdb_exception final : std::system_error
{
 using std::system_error::system_error;

 libfdb_exception(const std::string_view context)
  : std::system_error(make_error_code(libfdb_error_code::operation_failed),
                      std::string(context))
 {}

 libfdb_exception(const std::errc code, const std::string_view context)
  : std::system_error(std::make_error_code(code), std::string(context))
 {}

 public:
 [[nodiscard]] bool retryable() const noexcept
 {
  return ::ceph::libfdb::retryable(code());
 }

};

[[nodiscard]] inline bool retryable(const libfdb_exception& error) noexcept
{
 return retryable(error.code());
}

} // namespace ceph::libfdb

#endif // CEPH_FDB_ERRORS_H
