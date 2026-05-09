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

#include "desired_export.h"

#include <algorithm>
#include <unordered_map>

namespace rgw::file_state {

namespace {

// Compose `fs.prefix + ap.root_directory.path` into a single S3
// object-key prefix usable by the FSAL. The AccessPoint's
// rootDirectory is a path-like string; AWS allows it to start
// with `/` ("/scoped"), but S3 prefixes don't have a leading
// slash, so we strip it. Always returns a string that is either
// empty or ends with `/`.
std::string compose_prefix(std::string_view fs_prefix,
                            std::string_view ap_path) {
  std::string out{fs_prefix};
  while (!ap_path.empty() && ap_path.front() == '/') {
    ap_path.remove_prefix(1);
  }
  out.append(ap_path);
  if (!out.empty() && out.back() != '/') {
    out.push_back('/');
  }
  return out;
}

}  // namespace

std::vector<DesiredExport> compose_exports(Store& store,
                                           BootstrapResolver& bootstrap) {
  // Read all three resource types. No cross-method consistency
  // is required: any inconsistency is repaired by the next
  // reconciliation cycle, and the safety-net timer guarantees
  // bounded staleness.
  auto fs_views = store.scan_file_systems();
  auto ap_views = store.scan_access_points();
  auto mt_views = store.scan_mount_targets();

  // Index FSes by id for O(1) lookup during the join.
  std::unordered_map<std::string, const FileSystemSpec*> fs_by_id;
  fs_by_id.reserve(fs_views.size());
  for (const auto& fs : fs_views) {
    fs_by_id.emplace(fs.spec.id, &fs.spec);
  }

  // Index APs by parent FS id, since each (FS, MT) tuple may
  // correspond to multiple APs and each AP × MT combination
  // produces one export.
  std::unordered_map<std::string, std::vector<const AccessPointSpec*>>
      aps_by_fs;
  for (const auto& ap : ap_views) {
    aps_by_fs[ap.spec.parent_filesystem_id].push_back(&ap.spec);
  }

  // Cache bootstrap creds per owner account so we hit the resolver
  // once per account, not once per export.  std::optional models
  // "resolution attempted, returned nothing"; we still cache the
  // miss so a misconfigured account doesn't get hammered every
  // reconciliation cycle within a single compose_exports call.
  std::unordered_map<std::string, std::optional<BootstrapCredentials>>
      bootstrap_cache;

  std::vector<DesiredExport> out;
  // For each MountTarget, find its parent FS and every AP under
  // that FS; emit one DesiredExport per (FS, AP, MT) triple. A
  // FS without APs produces no exports — the AP is what carries
  // the path/POSIX scope, so without one there's nothing for the
  // FSAL to render.
  for (const auto& mt : mt_views) {
    const auto fs_it = fs_by_id.find(mt.spec.parent_filesystem_id);
    if (fs_it == fs_by_id.end()) {
      continue;  // orphaned MT; reconciliation will drop it
    }
    const FileSystemSpec& fs = *fs_it->second;

    const auto aps_it = aps_by_fs.find(fs.id);
    if (aps_it == aps_by_fs.end()) continue;

    /* Resolve bootstrap creds for the FS's owner account once. */
    auto cache_it = bootstrap_cache.find(fs.owner_account_id);
    if (cache_it == bootstrap_cache.end()) {
      cache_it = bootstrap_cache.emplace(
          fs.owner_account_id,
          bootstrap.resolve(fs.owner_account_id)).first;
    }
    const auto& creds = cache_it->second;
    if (!creds) {
      /* Skip exports we can't bootstrap. The reconciler logs at
       * its own layer; here we just don't render. */
      continue;
    }

    for (const AccessPointSpec* ap : aps_it->second) {
      DesiredExport e;
      e.fs_id = fs.id;
      e.ap_id = ap->id;
      e.mt_id = mt.spec.id;
      e.bucket_arn = fs.bucket_arn;
      e.composed_prefix = compose_prefix(
          fs.prefix,
          ap->root_directory ? ap->root_directory->path : "");
      e.role_arn = fs.role_arn;
      e.owner_account_id = fs.owner_account_id;
      e.bootstrap_user_id = creds->user_id;
      e.bootstrap_access_key = creds->access_key;
      e.bootstrap_secret_key = creds->secret_key;
      e.posix_user = ap->posix_user;
      e.zone_id = mt.spec.zone_id;
      e.ipv4_address = mt.status.ipv4_address;
      e.ipv6_address = mt.status.ipv6_address;
      e.security_groups = mt.spec.security_groups;
      out.push_back(std::move(e));
    }
  }

  // Stable ordering for tests + diff stability.
  std::sort(out.begin(), out.end(),
            [](const DesiredExport& a, const DesiredExport& b) {
              return std::tie(a.fs_id, a.ap_id, a.mt_id) <
                     std::tie(b.fs_id, b.ap_id, b.mt_id);
            });
  return out;
}

}  // namespace rgw::file_state
