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

// rgw::file_state::Store — backend interface for the S3 Files API.
//
// REST handlers consume this interface; concrete backends (MemoryStore
// for development and tests, FdbStore for production) implement it.
// See doc/dev/radosgw/s3_files_api.rst for the design.
//
// Errors are returned as StoreResult<T> (a variant of T or StoreError).
// StoreError carries an errorCode string from src/rgw/rgw_s3files_errors.h
// — REST handlers map these to AWS-shape exception types.

#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace rgw::file_state {

// ---------------------------------------------------------------- Lifecycle

enum class LifecycleState : std::uint8_t {
  Available,
  Creating,
  Updating,
  Deleting,
  Deleted,
  Error,
};

inline std::string_view to_string(LifecycleState s) {
  switch (s) {
    case LifecycleState::Available: return "AVAILABLE";
    case LifecycleState::Creating:  return "CREATING";
    case LifecycleState::Updating:  return "UPDATING";
    case LifecycleState::Deleting:  return "DELETING";
    case LifecycleState::Deleted:   return "DELETED";
    case LifecycleState::Error:     return "ERROR";
  }
  return "UNKNOWN";
}

// ---------------------------------------------------------------- Errors

struct StoreError {
  enum class Kind : std::uint8_t {
    NotFound,            // → ResourceNotFoundException (HTTP 404)
    Conflict,            // → ConflictException (HTTP 409)
    InvalidArgument,     // → ValidationException (HTTP 400)
    QuotaExceeded,       // → ServiceQuotaExceededException (HTTP 402)
    Throttled,           // → ThrottlingException (HTTP 429)
    Internal,            // → InternalServerException (HTTP 500)
  };
  Kind kind;
  std::string error_code;     // from rgw_s3files_errors.h, e.g. "FILE_SYSTEM_NOT_FOUND"
  std::string message;        // human-readable, surfaced in the AWS message field
};

// Placeholder for void-returning methods — keeps StoreResult uniform.
struct Unit {};

template <typename T>
using StoreResult = std::variant<T, StoreError>;

template <typename T>
inline bool ok(const StoreResult<T>& r) {
  return std::holds_alternative<T>(r);
}

template <typename T>
inline const T& value(const StoreResult<T>& r) {
  return std::get<T>(r);
}

template <typename T>
inline T&& take(StoreResult<T>&& r) {
  return std::get<T>(std::move(r));
}

template <typename T>
inline const StoreError& error(const StoreResult<T>& r) {
  return std::get<StoreError>(r);
}

// ---------------------------------------------------------------- Tags

struct Tag {
  std::string key;
  std::string value;

  bool operator==(const Tag& o) const = default;
};

namespace detail {
inline std::optional<std::string> name_from_tags(const std::vector<Tag>& tags) {
  for (const auto& t : tags) {
    if (t.key == "Name") {
      return t.value;
    }
  }
  return std::nullopt;
}
}  // namespace detail

// ---------------------------------------------------------------- POSIX

struct PosixUser {
  std::int64_t uid = 0;
  std::int64_t gid = 0;
  std::vector<std::int64_t> secondary_gids;

  bool operator==(const PosixUser& o) const = default;
};

struct CreationPermissions {
  std::int64_t owner_uid = 0;
  std::int64_t owner_gid = 0;
  std::string permissions;  // e.g. "0755"

  bool operator==(const CreationPermissions& o) const = default;
};

struct RootDirectory {
  std::string path;
  std::optional<CreationPermissions> creation_permissions;

  bool operator==(const RootDirectory& o) const = default;
};

// ---------------------------------------------------------------- FileSystem

struct FileSystemSpec {
  std::string id;                 // server-assigned, e.g. "fs-<17-hex>"
  std::string arn;                // server-assigned
  std::string owner_account_id;
  std::string bucket_arn;
  std::string prefix;             // captured at create
  std::string role_arn;
  std::string kms_key_id;
  std::string placement;          // captured Files-placement name
  std::string client_token;
  bool accept_bucket_warning = false;
  std::vector<Tag> tags;
  std::int64_t creation_time_unix_seconds = 0;
};

struct FileSystemStatus {
  LifecycleState state = LifecycleState::Creating;
  std::string status_message;
};

struct FileSystemView {
  FileSystemSpec spec;
  FileSystemStatus status;

  // Convenience: the value of the `Name` tag if present
  // (mirrors the AWS response field of the same name).
  std::optional<std::string> name() const {
    return detail::name_from_tags(spec.tags);
  }
};

struct CreateFileSystemRequest {
  std::string owner_account_id;
  std::string bucket_arn;
  std::string prefix;
  std::string role_arn;
  std::string kms_key_id;
  std::string client_token;
  std::string placement;          // optional; resolved from default if empty
  std::vector<Tag> tags;
  bool accept_bucket_warning = false;
};

// ---------------------------------------------------------------- AccessPoint

struct AccessPointSpec {
  std::string id;                 // server-assigned, e.g. "fsap-<17-hex>"
  std::string arn;
  std::string parent_filesystem_id;
  std::string owner_account_id;
  std::string client_token;
  std::optional<PosixUser> posix_user;
  std::optional<RootDirectory> root_directory;
  std::vector<Tag> tags;
};

struct AccessPointStatus {
  LifecycleState state = LifecycleState::Creating;
  std::string status_message;
};

struct AccessPointView {
  AccessPointSpec spec;
  AccessPointStatus status;

  std::optional<std::string> name() const {
    return detail::name_from_tags(spec.tags);
  }
};

struct CreateAccessPointRequest {
  std::string owner_account_id;
  std::string filesystem_id;
  std::string client_token;
  std::optional<PosixUser> posix_user;
  std::optional<RootDirectory> root_directory;
  std::vector<Tag> tags;
};

// ---------------------------------------------------------------- MountTarget

struct MountTargetSpec {
  std::string id;                 // server-assigned, e.g. "fsmt-<17-hex>"
  std::string parent_filesystem_id;
  std::string owner_account_id;
  std::string zone_id;            // decoded from subnetId at the API layer
  std::string ip_address_type;    // IPV4_ONLY etc.
  std::vector<std::string> security_groups;
};

struct MountTargetStatus {
  LifecycleState state = LifecycleState::Creating;
  std::string status_message;
  std::string ipv4_address;       // resolved by reconciler from placement
  std::string ipv6_address;
  std::string network_interface_id;
  std::string vpc_id;
};

struct MountTargetView {
  MountTargetSpec spec;
  MountTargetStatus status;
};

struct CreateMountTargetRequest {
  std::string owner_account_id;
  std::string filesystem_id;
  std::string zone_id;            // already decoded from subnetId
  std::string ip_address_type;
  std::vector<std::string> security_groups;
};

struct UpdateMountTargetRequest {
  std::string id;
  std::vector<std::string> security_groups;
};

// ---------------------------------------------------------------- Sync config

struct ImportDataRule {
  std::string prefix;
  std::string trigger;            // ON_FILE_ACCESS or ON_DIRECTORY_FIRST_ACCESS
  std::int64_t size_less_than = 0;

  bool operator==(const ImportDataRule& o) const = default;
};

struct ExpirationDataRule {
  std::int32_t days_after_last_access = 0;

  bool operator==(const ExpirationDataRule& o) const = default;
};

struct SyncConfig {
  std::vector<ImportDataRule> import_rules;
  std::vector<ExpirationDataRule> expiration_rules;
  std::int64_t latest_version_number = 0;
};

// ---------------------------------------------------------------- Listing

struct ListOptions {
  std::int32_t max_results = 100;
  std::string next_token;
};

template <typename T>
struct PagedResult {
  std::vector<T> items;
  std::string next_token;        // empty if no more pages
};

// ---------------------------------------------------------------- Store

class Store {
 public:
  virtual ~Store() = default;

  // FileSystem CRUD ------------------------------------------------

  virtual StoreResult<FileSystemView> create_file_system(
      const CreateFileSystemRequest& req) = 0;

  virtual StoreResult<FileSystemView> get_file_system(
      std::string_view account_id,
      std::string_view filesystem_id) = 0;

  virtual StoreResult<PagedResult<FileSystemView>> list_file_systems(
      std::string_view account_id,
      const ListOptions& opts) = 0;

  virtual StoreResult<Unit> delete_file_system(
      std::string_view account_id,
      std::string_view filesystem_id) = 0;

  // FileSystem resource policy --------------------------------------

  virtual StoreResult<Unit> put_file_system_policy(
      std::string_view account_id,
      std::string_view filesystem_id,
      std::string_view policy_json) = 0;

  virtual StoreResult<std::string> get_file_system_policy(
      std::string_view account_id,
      std::string_view filesystem_id) = 0;

  virtual StoreResult<Unit> delete_file_system_policy(
      std::string_view account_id,
      std::string_view filesystem_id) = 0;

  // FileSystem synchronization configuration ------------------------

  virtual StoreResult<Unit> put_synchronization_configuration(
      std::string_view account_id,
      std::string_view filesystem_id,
      const SyncConfig& cfg,
      std::optional<std::int64_t> expected_version) = 0;

  virtual StoreResult<SyncConfig> get_synchronization_configuration(
      std::string_view account_id,
      std::string_view filesystem_id) = 0;

  // AccessPoint CRUD -----------------------------------------------

  virtual StoreResult<AccessPointView> create_access_point(
      const CreateAccessPointRequest& req) = 0;

  virtual StoreResult<AccessPointView> get_access_point(
      std::string_view account_id,
      std::string_view access_point_id) = 0;

  virtual StoreResult<PagedResult<AccessPointView>> list_access_points(
      std::string_view account_id,
      std::string_view filesystem_id,
      const ListOptions& opts) = 0;

  virtual StoreResult<Unit> delete_access_point(
      std::string_view account_id,
      std::string_view access_point_id) = 0;

  // MountTarget CRUD + Update ---------------------------------------

  virtual StoreResult<MountTargetView> create_mount_target(
      const CreateMountTargetRequest& req) = 0;

  virtual StoreResult<MountTargetView> get_mount_target(
      std::string_view account_id,
      std::string_view mount_target_id) = 0;

  virtual StoreResult<PagedResult<MountTargetView>> list_mount_targets(
      std::string_view account_id,
      std::optional<std::string_view> filesystem_id,
      std::optional<std::string_view> access_point_id,
      const ListOptions& opts) = 0;

  virtual StoreResult<MountTargetView> update_mount_target(
      std::string_view account_id,
      const UpdateMountTargetRequest& req) = 0;

  virtual StoreResult<Unit> delete_mount_target(
      std::string_view account_id,
      std::string_view mount_target_id) = 0;

  // Tagging — FileSystem and AccessPoint only (per AWS ResourceId pattern)

  virtual StoreResult<PagedResult<Tag>> list_tags_for_resource(
      std::string_view account_id,
      std::string_view resource_id,
      const ListOptions& opts) = 0;

  virtual StoreResult<Unit> tag_resource(
      std::string_view account_id,
      std::string_view resource_id,
      const std::vector<Tag>& tags) = 0;

  virtual StoreResult<Unit> untag_resource(
      std::string_view account_id,
      std::string_view resource_id,
      const std::vector<std::string>& tag_keys) = 0;

  // ---- admin / reconciler scan ------------------------------
  //
  // Read every record in the store, regardless of owner. These
  // methods exist for the reconciler — which composes the
  // desired Ganesha export set across all accounts known to
  // the store — and should NOT be invoked from request-scoped
  // handler code (which is always account-scoped).
  //
  // Implementations are expected to return a consistent
  // snapshot: each scan_* method completes against state as it
  // existed at some point during the call. There is no
  // cross-method snapshot guarantee — the reconciler always
  // runs the three scans back to back and must tolerate
  // momentary inconsistencies (a FS that's about to gain an AP
  // shows up before the AP does). The safety-net timer + post-
  // mutation change-feed signal converge any lag.

  virtual std::vector<FileSystemView> scan_file_systems() = 0;
  virtual std::vector<AccessPointView> scan_access_points() = 0;
  virtual std::vector<MountTargetView> scan_mount_targets() = 0;

  // Register a callback to fire after every successful mutation
  // — used by in-process change-feed wiring (typically with
  // MemoryStore + InProcessChangeFeed, where the reconciler
  // lives in the same process). Backends that have native
  // change-feed mechanisms (RADOS watch/notify, FDB watches)
  // ignore this hook; it's an opt-in extension for stores that
  // don't carry their own out-of-band notification path.
  //
  // Default: no-op. MemoryStore overrides.
  virtual void set_on_change(std::function<void()> /*cb*/) {}
};

}  // namespace rgw::file_state
