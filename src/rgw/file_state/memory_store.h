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

// In-memory backend for rgw::file_state::Store.
//
// Used during day-1 development (before the FDB-backed
// implementation lands) and as the test backend for the gtest
// suite. Mirrors the eventual FdbStore semantics — spec/status
// separation, client-token idempotency on creates, lifecycle
// state transitions, cascade-rejection on FileSystem delete with
// active children, optimistic concurrency on
// SynchronizationConfiguration via latest_version_number — so
// that handlers written against this backend behave the same
// against FdbStore.

#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>

#include "store.h"

namespace rgw::file_state {

class MemoryStore : public Store {
 public:
  MemoryStore();
  ~MemoryStore() override = default;

  MemoryStore(const MemoryStore&) = delete;
  MemoryStore& operator=(const MemoryStore&) = delete;

  // FileSystem ------------------------------------------------------

  StoreResult<FileSystemView> create_file_system(
      const CreateFileSystemRequest& req) override;

  StoreResult<FileSystemView> get_file_system(
      std::string_view account_id,
      std::string_view filesystem_id) override;

  StoreResult<PagedResult<FileSystemView>> list_file_systems(
      std::string_view account_id,
      const ListOptions& opts) override;

  StoreResult<Unit> delete_file_system(
      std::string_view account_id,
      std::string_view filesystem_id) override;

  // FileSystem policy ----------------------------------------------

  StoreResult<Unit> put_file_system_policy(
      std::string_view account_id,
      std::string_view filesystem_id,
      std::string_view policy_json) override;

  StoreResult<std::string> get_file_system_policy(
      std::string_view account_id,
      std::string_view filesystem_id) override;

  StoreResult<Unit> delete_file_system_policy(
      std::string_view account_id,
      std::string_view filesystem_id) override;

  // FileSystem synchronization configuration -----------------------

  StoreResult<Unit> put_synchronization_configuration(
      std::string_view account_id,
      std::string_view filesystem_id,
      const SyncConfig& cfg,
      std::optional<std::int64_t> expected_version) override;

  StoreResult<SyncConfig> get_synchronization_configuration(
      std::string_view account_id,
      std::string_view filesystem_id) override;

  // AccessPoint -----------------------------------------------------

  StoreResult<AccessPointView> create_access_point(
      const CreateAccessPointRequest& req) override;

  StoreResult<AccessPointView> get_access_point(
      std::string_view account_id,
      std::string_view access_point_id) override;

  StoreResult<PagedResult<AccessPointView>> list_access_points(
      std::string_view account_id,
      std::string_view filesystem_id,
      const ListOptions& opts) override;

  StoreResult<Unit> delete_access_point(
      std::string_view account_id,
      std::string_view access_point_id) override;

  // MountTarget -----------------------------------------------------

  StoreResult<MountTargetView> create_mount_target(
      const CreateMountTargetRequest& req) override;

  StoreResult<MountTargetView> get_mount_target(
      std::string_view account_id,
      std::string_view mount_target_id) override;

  StoreResult<PagedResult<MountTargetView>> list_mount_targets(
      std::string_view account_id,
      std::optional<std::string_view> filesystem_id,
      std::optional<std::string_view> access_point_id,
      const ListOptions& opts) override;

  StoreResult<MountTargetView> update_mount_target(
      std::string_view account_id,
      const UpdateMountTargetRequest& req) override;

  StoreResult<Unit> delete_mount_target(
      std::string_view account_id,
      std::string_view mount_target_id) override;

  // Tagging --------------------------------------------------------

  StoreResult<PagedResult<Tag>> list_tags_for_resource(
      std::string_view account_id,
      std::string_view resource_id,
      const ListOptions& opts) override;

  StoreResult<Unit> tag_resource(
      std::string_view account_id,
      std::string_view resource_id,
      const std::vector<Tag>& tags) override;

  StoreResult<Unit> untag_resource(
      std::string_view account_id,
      std::string_view resource_id,
      const std::vector<std::string>& tag_keys) override;

 private:
  struct FileSystemRecord {
    FileSystemSpec spec;
    FileSystemStatus status;
    std::optional<std::string> policy;
    std::optional<SyncConfig> sync_config;
  };

  struct AccessPointRecord {
    AccessPointSpec spec;
    AccessPointStatus status;
  };

  struct MountTargetRecord {
    MountTargetSpec spec;
    MountTargetStatus status;
  };

  // Storage. All access goes through `mu_`.
  mutable std::mutex mu_;
  std::unordered_map<std::string, FileSystemRecord> file_systems_;
  std::unordered_map<std::string, AccessPointRecord> access_points_;
  std::unordered_map<std::string, MountTargetRecord> mount_targets_;

  // Idempotency mappings: (account_id, client_token) -> existing id.
  std::map<std::pair<std::string, std::string>, std::string>
      fs_client_tokens_;
  std::map<std::pair<std::string, std::string>, std::string>
      ap_client_tokens_;

  // ID generation: monotonically incrementing 64-bit counter,
  // formatted as 32 hex chars to match the AWS Smithy patterns.
  std::atomic<std::uint64_t> id_counter_;
  std::string make_id(std::string_view prefix);

  // ARN format mirrors AWS:
  //   arn:aws:s3files:<region>:<account>:file-system/<fs>
  //   arn:aws:s3files:<region>:<account>:file-system/<fs>/access-point/<ap>
  static std::string make_fs_arn(
      std::string_view account_id, std::string_view fs_id);
  static std::string make_ap_arn(
      std::string_view account_id, std::string_view fs_id,
      std::string_view ap_id);

  // Locate either a FileSystem or an AccessPoint record by id and
  // owner. Returns a pointer to the underlying tag list, or
  // nullptr when the resource doesn't exist or isn't owned by
  // `account_id`. Caller must hold `mu_`.
  std::vector<Tag>* tags_target(
      std::string_view account_id, std::string_view resource_id);
};

}  // namespace rgw::file_state
