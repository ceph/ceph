// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace kvrgw {

inline constexpr uint32_t kDefaultTenantId = 1;
inline constexpr uint16_t kShardCount = 1;
inline constexpr uint16_t kShardId = 0;
inline constexpr char kNamespaceBucket = 'B';
inline constexpr char kNamespaceTenant = 'T';
inline constexpr char kNamespaceObject = 'S';
inline constexpr char kNamespacePending = 'P';
inline constexpr char kNamespaceGc = 'G';
inline constexpr char kNamespaceData = 'D';
inline constexpr char kNamespaceLocal = 'L';
inline constexpr char kLocalTypeNumeric = 'N';
inline constexpr char kLocalTypeIdMap = 'I';
inline constexpr char kCategoryObject = 'O';
inline constexpr char kCategoryVersion = 'V';
inline constexpr char kCategoryMultipart = 'M';      // S: namespace, multipart head row only
inline constexpr char kCategoryMultipartPart = 'U';  // S: namespace, multipart part rows
inline constexpr char kOpTypeObject = 'O';
inline constexpr char kOpTypeGroup = 'G';
inline constexpr char kOpTypeMultipart = 'M';    // G: namespace, G:M whole-upload directive
inline constexpr char kOpTypeGcU = 'U';          // G: namespace, G:U per-part cleanup group (1-20 entries)

inline constexpr uint32_t kNullVersionRaw    = 0xFFFFFFFF;
inline constexpr uint32_t kFirstVersionIdRaw = 0xFFFFFFFE;

enum VersioningState : uint8_t {
  VERSIONING_DISABLED  = 0,
  VERSIONING_ENABLED   = 1,
  VERSIONING_SUSPENDED = 2,
};

inline constexpr uint32_t kDefaultMaxInline = 256;
inline constexpr uint32_t kDefaultMaxKvStore = 4096;
inline constexpr bool kDefaultKvStoreCoalescing = false;

inline constexpr size_t kRefTagSize = 12;
inline constexpr size_t kEtagSize = 16;

// NUL delimiter used between a variable-length raw field (e.g. object_name)
// and an immediately-following fixed-size binary field, to prevent two
// different variable-length values from producing colliding raw key bytes
// (e.g. S:V's object_name/version_id, S:M's object_name/ref_tag).
inline constexpr char kDelimChar = '\0';
inline constexpr size_t kBucketIdSize = sizeof(uint64_t);
inline constexpr size_t kTenantIdSize = sizeof(uint32_t);
inline constexpr size_t kMaxContentTypeLen = 255;

inline constexpr char kCategoryChild = 'C';
inline constexpr char kChildTypeTags = 'T';
inline constexpr char kChildTypeAnnotation = 'A';
inline constexpr char kChildTypeExtended = 'E';

inline constexpr size_t kMaxTags = 10;
inline constexpr size_t kMaxTagKeyLen = 128;
inline constexpr size_t kMaxTagValueLen = 256;
inline constexpr size_t kMaxTagPayload = 5120;
inline constexpr size_t kMaxInlineTagPayload = 256;
inline constexpr size_t kMaxOValueBytes = 1024;
inline constexpr uint8_t kFlagExternalTags = 0x08;

inline constexpr std::string_view kLocalCounterTenantId = "tenant_id";
inline constexpr std::string_view kLocalCounterBucketId = "bucket_id";
inline constexpr std::string_view kLocalCounterRgwId = "rgw_id";

inline constexpr int kMaxBatchSize = 20;
inline constexpr int kMaxGroupPoSize = 8;

inline constexpr size_t AWS_MaxMetadataBytes = 2048;
inline constexpr size_t AWS_MaxObjectNameLen = 1024;
inline constexpr uint32_t AWS_MinBucketNameLen = 3;
inline constexpr uint32_t AWS_MaxBucketNameLen = 63;
inline constexpr uint32_t AWS_MaxKeys = 1000;
inline constexpr uint32_t AWS_MaxBuckets = 10000;
inline constexpr int kListBucketsFdbPage = 1000;

inline constexpr uint32_t kSweeperMaxKeys = 1000;
inline constexpr uint32_t kGcMaxKeys = 1000;
inline constexpr uint32_t kFdbMaxKeys = 5000;

// Multipart upload (AWS limits + POC-locked design constants).

// when scanning provide headroom for older gen upload-part retries.
inline constexpr uint16_t kListPartsGenHeadroom=100;
inline constexpr uint16_t kMinPartNumber = 1;
inline constexpr uint16_t kMaxPartNumber = 10000;
inline constexpr uint64_t kMinPartSizeBytes = 5ULL * 1024 * 1024;         // 5 MiB
inline constexpr uint64_t kMaxPartSizeBytes = 5ULL * 1024 * 1024 * 1024;  // 5 GiB

// N-bound on total entries (PENDING + COMMITTED) per (bucket_id, ref_tag,
// part_number) row -- see plans/fixing_agent_mess.md Section 3's N-bound
// eviction rule. A new UploadPart arrival that finds the row already at N:
//   - evicts the oldest COMMITTED entry (never a PENDING one) to make
//     room, if at least one COMMITTED entry exists; or
//   - is rejected outright (KVRGW_ERR_UPLOAD_PART_BUSY) if all N entries
//     are PENDING.
// This bound is what makes multiple COMMITTED generations for the same
// part reachable at once (Phase 3's unconditional-commit rule, Complete's
// ETag-based selection among them) while still keeping a row's size small.
inline constexpr uint32_t kMaxGenerationsPerPart = 3;

// IN_TRANSIENT wait/poll rule (see plans/fixing_agent_mess.md Section 5):
// an UploadPart arriving while the head is IN_TRANSIENT re-reads the head
// every kInTransientPollIntervalMs, for up to kInTransientPollMaxMs total,
// before giving up and returning a retriable (SlowDown) error.
inline constexpr int kInTransientPollIntervalMs = 10;
inline constexpr int kInTransientPollMaxMs = 1000;

}  // namespace kvrgw
