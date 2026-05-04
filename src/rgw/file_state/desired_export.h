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

// Plain-data record describing one Ganesha NFS export the
// reconciler wants the local Ganesha to be serving. The
// Reconciler emits a vector<DesiredExport> per reconciliation
// cycle; the GaneshaSink applies it.
//
// One DesiredExport per (FileSystem, AccessPoint, MountTarget)
// triple — that's the granularity of an NFS export. A FileSystem
// without an AccessPoint or without a MountTarget produces no
// exports.
//
// All fields are populated from the Store records joined together
// in compose_exports(). The struct is wire-format-agnostic — it
// carries enough information for any sink (RecordingGaneshaSink,
// DbusGaneshaSink, future GrpcGaneshaSink) to render or apply.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "store.h"

namespace rgw::file_state {

struct DesiredExport {
  // Stable identity of the export. Same `(fs_id, ap_id, mt_id)`
  // tuple across reconciliation cycles maps to the same Ganesha
  // export; sinks track the (tuple → Ganesha export_id) mapping
  // internally.
  std::string fs_id;
  std::string ap_id;
  std::string mt_id;

  // Bucket bound to the FS (full ARN).
  std::string bucket_arn;

  // S3 object-key prefix the export should appear to be rooted
  // at: `fs.prefix + ap.root_directory.path` with the leading
  // slash on `root_directory.path` stripped, and a trailing
  // slash guaranteed unless empty. Computed by compose_exports.
  std::string composed_prefix;

  // The FS-level role assumed by the FSAL per mount session.
  std::string role_arn;

  // Owner account-id (informational; sinks may include in
  // exported metadata for debugging).
  std::string owner_account_id;

  // The AccessPoint's POSIX squash identity, if any. When the
  // mounting principal lacks ClientRootAccess, NFS ops are
  // mapped to this uid/gid.
  std::optional<PosixUser> posix_user;

  // The MountTarget's zone-id and (eventually) resolved IP
  // address. ipv4_address may be empty before the placement
  // resolver attaches a VIP.
  std::string zone_id;
  std::string ipv4_address;
  std::string ipv6_address;

  // Security groups recorded on the MT spec — currently stored
  // but not enforced at the data plane. Ganesha config can
  // surface them as documentation comments.
  std::vector<std::string> security_groups;

  bool operator==(const DesiredExport& o) const = default;
};

// Read all (FS, AP, MT) tuples from the store and return the
// flattened set of desired exports. Pure: no side effects, no
// I/O beyond Store reads. Output is sorted lexicographically by
// `(fs_id, ap_id, mt_id)` so callers can rely on stable ordering
// for diffing.
//
// `account_filter` narrows to a single account when set; empty
// means scan every account the store knows about. (For RGW v1
// the s3files API is account-scoped at request time so this is
// usually omitted in production code paths.)
std::vector<DesiredExport> compose_exports(Store& store);

}  // namespace rgw::file_state
