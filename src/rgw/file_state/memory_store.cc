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

#include "memory_store.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <random>

#include "rgw_s3files_errors.h"

namespace rgw::file_state {

namespace {

using namespace ::rgw::s3files;  // ERR_* errorCode constants

constexpr std::string_view kRegion = "default";

std::int64_t now_unix_seconds() {
  using namespace std::chrono;
  return duration_cast<seconds>(
      system_clock::now().time_since_epoch()).count();
}

// Apply ListOptions {max_results, next_token} to an already-collected
// + sorted vector of items. Returns a PagedResult containing at most
// `max_results` items, with `next_token` set to the id of the last
// item in the page when more items remain.
//
// `id_of` extracts the sort key / token from an item; the same key
// is used for ordering and for the next-token boundary, so callers
// must hand in a vector already sorted ascending by id_of.
template <typename T, typename IdFn>
PagedResult<T> apply_pagination(
    std::vector<T>&& sorted, const ListOptions& opts, IdFn id_of) {
  PagedResult<T> out;
  // start_after: skip past entries whose id_of(item) <= next_token.
  auto it = sorted.begin();
  if (!opts.next_token.empty()) {
    it = std::upper_bound(
        sorted.begin(), sorted.end(), opts.next_token,
        [&](const std::string& tok, const T& item) {
          return tok < id_of(item);
        });
  }
  const std::int32_t cap = opts.max_results > 0 ? opts.max_results : 100;
  for (; it != sorted.end() && static_cast<std::int32_t>(out.items.size()) < cap;
       ++it) {
    out.items.push_back(std::move(*it));
  }
  if (it != sorted.end() && !out.items.empty()) {
    out.next_token = std::string(id_of(out.items.back()));
  }
  return out;
}

StoreError not_found(std::string_view code, std::string message) {
  return StoreError{
      .kind = StoreError::Kind::NotFound,
      .error_code = std::string(code),
      .message = std::move(message),
  };
}

StoreError conflict(std::string_view code, std::string message) {
  return StoreError{
      .kind = StoreError::Kind::Conflict,
      .error_code = std::string(code),
      .message = std::move(message),
  };
}

StoreError invalid(std::string_view code, std::string message) {
  return StoreError{
      .kind = StoreError::Kind::InvalidArgument,
      .error_code = std::string(code),
      .message = std::move(message),
  };
}

}  // namespace

MemoryStore::MemoryStore() = default;

void MemoryStore::set_on_change(std::function<void()> cb) {
  std::lock_guard lock(mu_);
  on_change_ = std::move(cb);
}

std::string MemoryStore::make_id(std::string_view prefix) {
  // AWS EFS-style resource id: `<prefix><17 random hex chars>`
  // (e.g. "fs-0123456789abcdef0").  17 hex = 68 bits of entropy,
  // which is plenty for global uniqueness within an account.
  // A thread-local PRNG seeded from random_device keeps this
  // self-contained -- the file_state library doesn't need to
  // drag in libcommon for uuid_d.
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  const std::uint64_t lo = rng();
  const std::uint64_t hi = rng() & 0xfu;
  char buf[18];
  std::snprintf(buf, sizeof(buf), "%016llx%llx",
                static_cast<unsigned long long>(lo),
                static_cast<unsigned long long>(hi));
  return std::string(prefix) + buf;
}

std::string MemoryStore::make_fs_arn(
    std::string_view account_id, std::string_view fs_id) {
  std::string arn;
  arn.reserve(64 + account_id.size() + fs_id.size());
  arn.append("arn:aws:s3files:");
  arn.append(kRegion);
  arn.append(":");
  arn.append(account_id);
  arn.append(":file-system/");
  arn.append(fs_id);
  return arn;
}

std::string MemoryStore::make_ap_arn(
    std::string_view account_id, std::string_view fs_id,
    std::string_view ap_id) {
  std::string arn = make_fs_arn(account_id, fs_id);
  arn.append("/access-point/");
  arn.append(ap_id);
  return arn;
}

// =================================================================
// FileSystem
// =================================================================

StoreResult<FileSystemView> MemoryStore::create_file_system(
    const CreateFileSystemRequest& req) {
  if (req.bucket_arn.empty()) {
    return invalid(ERR_INVALID_BUCKET_ARN, "bucket is required");
  }
  if (req.role_arn.empty()) {
    return invalid(ERR_INVALID_ROLE_ARN, "roleArn is required");
  }

  ChangeNotify notify(*this);  // fires after lock releases, only if commit()ed
  std::lock_guard lock(mu_);

  // Idempotency by client_token.
  if (!req.client_token.empty()) {
    auto it = fs_client_tokens_.find({req.owner_account_id, req.client_token});
    if (it != fs_client_tokens_.end()) {
      auto fs_it = file_systems_.find(it->second);
      if (fs_it != file_systems_.end()) {
        return FileSystemView{fs_it->second.spec, fs_it->second.status};
      }
    }
  }

  // Bucket-already-in-use check.
  for (const auto& [_, rec] : file_systems_) {
    if (rec.spec.owner_account_id == req.owner_account_id &&
        rec.spec.bucket_arn == req.bucket_arn) {
      return conflict(ERR_BUCKET_ALREADY_IN_USE,
                       "bucket is already bound to a file system");
    }
  }

  FileSystemRecord rec;
  rec.spec.id = make_id("fs-");
  rec.spec.arn = make_fs_arn(req.owner_account_id, rec.spec.id);
  rec.spec.owner_account_id = req.owner_account_id;
  rec.spec.bucket_arn = req.bucket_arn;
  rec.spec.prefix = req.prefix;
  rec.spec.role_arn = req.role_arn;
  rec.spec.kms_key_id = req.kms_key_id;
  rec.spec.placement = req.placement;
  rec.spec.client_token = req.client_token;
  rec.spec.accept_bucket_warning = req.accept_bucket_warning;
  rec.spec.tags = req.tags;
  rec.spec.creation_time_unix_seconds = now_unix_seconds();
  // Memory backend transitions to Available immediately.
  rec.status.state = LifecycleState::Available;

  if (!req.client_token.empty()) {
    fs_client_tokens_.emplace(
        std::make_pair(req.owner_account_id, req.client_token),
        rec.spec.id);
  }

  std::string id = rec.spec.id;
  auto [it, _] = file_systems_.emplace(id, std::move(rec));
  notify.commit();
  return FileSystemView{it->second.spec, it->second.status};
}

StoreResult<FileSystemView> MemoryStore::get_file_system(
    std::string_view account_id, std::string_view filesystem_id) {
  std::lock_guard lock(mu_);
  auto it = file_systems_.find(std::string(filesystem_id));
  if (it == file_systems_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }
  return FileSystemView{it->second.spec, it->second.status};
}

StoreResult<PagedResult<FileSystemView>> MemoryStore::list_file_systems(
    std::string_view account_id, const ListOptions& opts) {
  std::lock_guard lock(mu_);
  std::vector<FileSystemView> all;
  for (const auto& [_, rec] : file_systems_) {
    if (rec.spec.owner_account_id == account_id) {
      all.push_back(FileSystemView{rec.spec, rec.status});
    }
  }
  std::sort(all.begin(), all.end(), [](const auto& a, const auto& b) {
    return a.spec.id < b.spec.id;
  });
  return apply_pagination(std::move(all), opts,
      [](const FileSystemView& v) { return v.spec.id; });
}

StoreResult<Unit> MemoryStore::delete_file_system(
    std::string_view account_id, std::string_view filesystem_id) {
  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);
  auto it = file_systems_.find(std::string(filesystem_id));
  if (it == file_systems_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }

  // Cascade rejection: refuse if any AP or MT references this FS.
  for (const auto& [_, ap] : access_points_) {
    if (ap.spec.parent_filesystem_id == filesystem_id) {
      return conflict(ERR_FILE_SYSTEM_HAS_CHILDREN,
                       "file system has active access points");
    }
  }
  for (const auto& [_, mt] : mount_targets_) {
    if (mt.spec.parent_filesystem_id == filesystem_id) {
      return conflict(ERR_FILE_SYSTEM_HAS_CHILDREN,
                       "file system has active mount targets");
    }
  }

  // Drop client_token mapping if any.
  if (!it->second.spec.client_token.empty()) {
    fs_client_tokens_.erase(
        {it->second.spec.owner_account_id, it->second.spec.client_token});
  }
  file_systems_.erase(it);
  notify.commit();
  return Unit{};
}

// =================================================================
// FileSystem policy
// =================================================================

StoreResult<Unit> MemoryStore::put_file_system_policy(
    std::string_view account_id, std::string_view filesystem_id,
    std::string_view policy_json) {
  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);
  auto it = file_systems_.find(std::string(filesystem_id));
  if (it == file_systems_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }
  // Minimal JSON-shape check: must start with '{'.
  if (policy_json.empty() || policy_json.front() != '{') {
    return invalid(ERR_INVALID_POLICY_DOCUMENT,
                    "policy must be a JSON object");
  }
  it->second.policy = std::string(policy_json);
  notify.commit();
  return Unit{};
}

StoreResult<std::string> MemoryStore::get_file_system_policy(
    std::string_view account_id, std::string_view filesystem_id) {
  std::lock_guard lock(mu_);
  auto it = file_systems_.find(std::string(filesystem_id));
  if (it == file_systems_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }
  if (!it->second.policy) {
    return not_found(ERR_POLICY_NOT_FOUND, "no policy set");
  }
  return *it->second.policy;
}

StoreResult<Unit> MemoryStore::delete_file_system_policy(
    std::string_view account_id, std::string_view filesystem_id) {
  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);
  auto it = file_systems_.find(std::string(filesystem_id));
  if (it == file_systems_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }
  if (!it->second.policy) {
    return not_found(ERR_POLICY_NOT_FOUND, "no policy set");
  }
  it->second.policy.reset();
  notify.commit();
  return Unit{};
}

// =================================================================
// FileSystem synchronization configuration
// =================================================================

StoreResult<Unit> MemoryStore::put_synchronization_configuration(
    std::string_view account_id, std::string_view filesystem_id,
    const SyncConfig& cfg, std::optional<std::int64_t> expected_version) {
  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);
  auto it = file_systems_.find(std::string(filesystem_id));
  if (it == file_systems_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }

  std::int64_t current_version = it->second.sync_config
      ? it->second.sync_config->latest_version_number
      : 0;
  if (expected_version && *expected_version != current_version) {
    return conflict(ERR_FILE_SYSTEM_IN_INVALID_STATE,
                     "latestVersionNumber does not match current configuration");
  }

  SyncConfig stored = cfg;
  stored.latest_version_number = current_version + 1;
  it->second.sync_config = std::move(stored);
  notify.commit();
  return Unit{};
}

StoreResult<SyncConfig> MemoryStore::get_synchronization_configuration(
    std::string_view account_id, std::string_view filesystem_id) {
  std::lock_guard lock(mu_);
  auto it = file_systems_.find(std::string(filesystem_id));
  if (it == file_systems_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }
  if (!it->second.sync_config) {
    return not_found(ERR_FILE_SYSTEM_IN_INVALID_STATE,
                      "no synchronization configuration set");
  }
  return *it->second.sync_config;
}

// =================================================================
// AccessPoint
// =================================================================

StoreResult<AccessPointView> MemoryStore::create_access_point(
    const CreateAccessPointRequest& req) {
  if (req.filesystem_id.empty()) {
    return invalid(ERR_INVALID_ROOT_DIRECTORY, "fileSystemId is required");
  }
  if (req.posix_user) {
    // posixUser must include both uid and gid (>= 0).
    if (req.posix_user->uid < 0 || req.posix_user->gid < 0) {
      return invalid(ERR_INVALID_POSIX_USER,
                      "posixUser uid and gid must be non-negative");
    }
  }

  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);

  // Verify parent FS exists and belongs to caller.
  auto fs_it = file_systems_.find(req.filesystem_id);
  if (fs_it == file_systems_.end() ||
      fs_it->second.spec.owner_account_id != req.owner_account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }

  // Idempotency by client_token.
  if (!req.client_token.empty()) {
    auto it = ap_client_tokens_.find({req.owner_account_id, req.client_token});
    if (it != ap_client_tokens_.end()) {
      auto ap_it = access_points_.find(it->second);
      if (ap_it != access_points_.end()) {
        return AccessPointView{ap_it->second.spec, ap_it->second.status};
      }
    }
  }

  AccessPointRecord rec;
  rec.spec.id = make_id("fsap-");
  rec.spec.arn = make_ap_arn(
      req.owner_account_id, req.filesystem_id, rec.spec.id);
  rec.spec.parent_filesystem_id = req.filesystem_id;
  rec.spec.owner_account_id = req.owner_account_id;
  rec.spec.client_token = req.client_token;
  rec.spec.posix_user = req.posix_user;
  rec.spec.root_directory = req.root_directory;
  rec.spec.tags = req.tags;
  rec.status.state = LifecycleState::Available;

  if (!req.client_token.empty()) {
    ap_client_tokens_.emplace(
        std::make_pair(req.owner_account_id, req.client_token),
        rec.spec.id);
  }

  std::string id = rec.spec.id;
  auto [it, _] = access_points_.emplace(id, std::move(rec));
  notify.commit();
  return AccessPointView{it->second.spec, it->second.status};
}

StoreResult<AccessPointView> MemoryStore::get_access_point(
    std::string_view account_id, std::string_view access_point_id) {
  std::lock_guard lock(mu_);
  auto it = access_points_.find(std::string(access_point_id));
  if (it == access_points_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_ACCESS_POINT_NOT_FOUND, "access point not found");
  }
  return AccessPointView{it->second.spec, it->second.status};
}

StoreResult<PagedResult<AccessPointView>> MemoryStore::list_access_points(
    std::string_view account_id, std::string_view filesystem_id,
    const ListOptions& opts) {
  std::lock_guard lock(mu_);

  // The parent FS must exist.
  auto fs_it = file_systems_.find(std::string(filesystem_id));
  if (fs_it == file_systems_.end() ||
      fs_it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }

  std::vector<AccessPointView> all;
  for (const auto& [_, rec] : access_points_) {
    if (rec.spec.parent_filesystem_id == filesystem_id &&
        rec.spec.owner_account_id == account_id) {
      all.push_back(AccessPointView{rec.spec, rec.status});
    }
  }
  std::sort(all.begin(), all.end(), [](const auto& a, const auto& b) {
    return a.spec.id < b.spec.id;
  });
  return apply_pagination(std::move(all), opts,
      [](const AccessPointView& v) { return v.spec.id; });
}

StoreResult<Unit> MemoryStore::delete_access_point(
    std::string_view account_id, std::string_view access_point_id) {
  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);
  auto it = access_points_.find(std::string(access_point_id));
  if (it == access_points_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_ACCESS_POINT_NOT_FOUND, "access point not found");
  }
  if (!it->second.spec.client_token.empty()) {
    ap_client_tokens_.erase(
        {it->second.spec.owner_account_id, it->second.spec.client_token});
  }
  access_points_.erase(it);
  notify.commit();
  return Unit{};
}

// =================================================================
// MountTarget
// =================================================================

StoreResult<MountTargetView> MemoryStore::create_mount_target(
    const CreateMountTargetRequest& req) {
  if (req.filesystem_id.empty()) {
    return invalid(ERR_INVALID_ZONE_ID, "fileSystemId is required");
  }
  if (req.zone_id.empty()) {
    return invalid(ERR_INVALID_ZONE_ID, "zone_id (subnetId) is required");
  }

  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);

  auto fs_it = file_systems_.find(req.filesystem_id);
  if (fs_it == file_systems_.end() ||
      fs_it->second.spec.owner_account_id != req.owner_account_id) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
  }

  // One MT per (FS, zone).
  for (const auto& [_, mt] : mount_targets_) {
    if (mt.spec.parent_filesystem_id == req.filesystem_id &&
        mt.spec.zone_id == req.zone_id) {
      return conflict(ERR_MOUNT_TARGET_ALREADY_IN_ZONE,
                       "mount target already exists in this zone");
    }
  }

  MountTargetRecord rec;
  rec.spec.id = make_id("fsmt-");
  rec.spec.parent_filesystem_id = req.filesystem_id;
  rec.spec.owner_account_id = req.owner_account_id;
  rec.spec.zone_id = req.zone_id;
  rec.spec.ip_address_type = req.ip_address_type.empty()
      ? "IPV4_ONLY"
      : req.ip_address_type;
  rec.spec.security_groups = req.security_groups;
  rec.status.state = LifecycleState::Available;
  // ipv4_address remains empty until a reconciler populates it.

  std::string id = rec.spec.id;
  auto [it, _] = mount_targets_.emplace(id, std::move(rec));
  notify.commit();
  return MountTargetView{it->second.spec, it->second.status};
}

StoreResult<MountTargetView> MemoryStore::get_mount_target(
    std::string_view account_id, std::string_view mount_target_id) {
  std::lock_guard lock(mu_);
  auto it = mount_targets_.find(std::string(mount_target_id));
  if (it == mount_targets_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_MOUNT_TARGET_NOT_FOUND, "mount target not found");
  }
  return MountTargetView{it->second.spec, it->second.status};
}

StoreResult<PagedResult<MountTargetView>> MemoryStore::list_mount_targets(
    std::string_view account_id,
    std::optional<std::string_view> filesystem_id,
    std::optional<std::string_view> access_point_id,
    const ListOptions& opts) {
  std::lock_guard lock(mu_);

  // Resolve the filter target. If filesystem_id is supplied, that FS
  // must exist (and belong to the caller). access_point_id, if
  // supplied, is resolved to its parent FS.
  std::optional<std::string> required_fs_id;
  if (filesystem_id) {
    auto fs_it = file_systems_.find(std::string(*filesystem_id));
    if (fs_it == file_systems_.end() ||
        fs_it->second.spec.owner_account_id != account_id) {
      return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "file system not found");
    }
    required_fs_id = std::string(*filesystem_id);
  }
  if (access_point_id) {
    auto ap_it = access_points_.find(std::string(*access_point_id));
    if (ap_it == access_points_.end() ||
        ap_it->second.spec.owner_account_id != account_id) {
      return not_found(ERR_ACCESS_POINT_NOT_FOUND, "access point not found");
    }
    if (required_fs_id && *required_fs_id != ap_it->second.spec.parent_filesystem_id) {
      // Both filters supplied but inconsistent → empty result.
      return PagedResult<MountTargetView>{};
    }
    required_fs_id = ap_it->second.spec.parent_filesystem_id;
  }

  std::vector<MountTargetView> all;
  for (const auto& [_, rec] : mount_targets_) {
    if (rec.spec.owner_account_id != account_id) continue;
    if (required_fs_id && rec.spec.parent_filesystem_id != *required_fs_id) {
      continue;
    }
    all.push_back(MountTargetView{rec.spec, rec.status});
  }
  std::sort(all.begin(), all.end(), [](const auto& a, const auto& b) {
    return a.spec.id < b.spec.id;
  });
  return apply_pagination(std::move(all), opts,
      [](const MountTargetView& v) { return v.spec.id; });
}

StoreResult<MountTargetView> MemoryStore::update_mount_target(
    std::string_view account_id, const UpdateMountTargetRequest& req) {
  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);
  auto it = mount_targets_.find(req.id);
  if (it == mount_targets_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_MOUNT_TARGET_NOT_FOUND, "mount target not found");
  }
  it->second.spec.security_groups = req.security_groups;
  notify.commit();
  return MountTargetView{it->second.spec, it->second.status};
}

StoreResult<Unit> MemoryStore::delete_mount_target(
    std::string_view account_id, std::string_view mount_target_id) {
  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);
  auto it = mount_targets_.find(std::string(mount_target_id));
  if (it == mount_targets_.end() ||
      it->second.spec.owner_account_id != account_id) {
    return not_found(ERR_MOUNT_TARGET_NOT_FOUND, "mount target not found");
  }
  mount_targets_.erase(it);
  notify.commit();
  return Unit{};
}

// =================================================================
// Tagging
// =================================================================

std::vector<Tag>* MemoryStore::tags_target(
    std::string_view account_id, std::string_view resource_id) {
  if (auto it = file_systems_.find(std::string(resource_id));
      it != file_systems_.end()) {
    if (it->second.spec.owner_account_id != account_id) return nullptr;
    return &it->second.spec.tags;
  }
  if (auto it = access_points_.find(std::string(resource_id));
      it != access_points_.end()) {
    if (it->second.spec.owner_account_id != account_id) return nullptr;
    return &it->second.spec.tags;
  }
  return nullptr;
}

StoreResult<PagedResult<Tag>> MemoryStore::list_tags_for_resource(
    std::string_view account_id, std::string_view resource_id,
    const ListOptions& opts) {
  std::lock_guard lock(mu_);
  std::vector<Tag>* tags = tags_target(account_id, resource_id);
  if (!tags) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "resource not found");
  }
  std::vector<Tag> all = *tags;
  std::sort(all.begin(), all.end(), [](const Tag& a, const Tag& b) {
    return a.key < b.key;
  });
  return apply_pagination(std::move(all), opts,
      [](const Tag& t) { return t.key; });
}

StoreResult<Unit> MemoryStore::tag_resource(
    std::string_view account_id, std::string_view resource_id,
    const std::vector<Tag>& tags) {
  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);
  std::vector<Tag>* existing = tags_target(account_id, resource_id);
  if (!existing) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "resource not found");
  }
  for (const auto& t : tags) {
    auto it = std::find_if(existing->begin(), existing->end(),
                            [&](const Tag& e) { return e.key == t.key; });
    if (it != existing->end()) {
      it->value = t.value;
    } else {
      existing->push_back(t);
    }
  }
  notify.commit();
  return Unit{};
}

StoreResult<Unit> MemoryStore::untag_resource(
    std::string_view account_id, std::string_view resource_id,
    const std::vector<std::string>& tag_keys) {
  ChangeNotify notify(*this);
  std::lock_guard lock(mu_);
  std::vector<Tag>* existing = tags_target(account_id, resource_id);
  if (!existing) {
    return not_found(ERR_FILE_SYSTEM_NOT_FOUND, "resource not found");
  }
  existing->erase(
      std::remove_if(existing->begin(), existing->end(),
                      [&](const Tag& t) {
                        return std::find(tag_keys.begin(), tag_keys.end(),
                                          t.key) != tag_keys.end();
                      }),
      existing->end());
  notify.commit();
  return Unit{};
}

// =================================================================
// Admin / reconciler scans
// =================================================================

std::vector<FileSystemView> MemoryStore::scan_file_systems() {
  std::lock_guard lock(mu_);
  std::vector<FileSystemView> out;
  out.reserve(file_systems_.size());
  for (const auto& [_, rec] : file_systems_) {
    out.push_back(FileSystemView{rec.spec, rec.status});
  }
  return out;
}

std::vector<AccessPointView> MemoryStore::scan_access_points() {
  std::lock_guard lock(mu_);
  std::vector<AccessPointView> out;
  out.reserve(access_points_.size());
  for (const auto& [_, rec] : access_points_) {
    out.push_back(AccessPointView{rec.spec, rec.status});
  }
  return out;
}

std::vector<MountTargetView> MemoryStore::scan_mount_targets() {
  std::lock_guard lock(mu_);
  std::vector<MountTargetView> out;
  out.reserve(mount_targets_.size());
  for (const auto& [_, rec] : mount_targets_) {
    out.push_back(MountTargetView{rec.spec, rec.status});
  }
  return out;
}

}  // namespace rgw::file_state
