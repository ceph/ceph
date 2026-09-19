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
inline constexpr char kOpTypeObject = 'O';
inline constexpr char kOpTypeGroup = 'G';

inline constexpr uint32_t kNullVersionRaw    = 0xFFFFFFFF;
inline constexpr uint32_t kFirstVersionIdRaw = 0xFFFFFFFE;

enum VersioningState : uint8_t {
  VERSIONING_DISABLED  = 0,
  VERSIONING_ENABLED   = 1,
  VERSIONING_SUSPENDED = 2,
};

inline constexpr uint32_t kDefaultMaxInline = 0;
inline constexpr uint32_t kDefaultMaxKvStore = 0;
inline constexpr bool kDefaultKvStoreCoalescing = false;

inline constexpr size_t kRefTagSize = 12;
inline constexpr size_t kEtagSize = 16;
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

inline constexpr int kMaxBatchSize = 16;

inline constexpr uint32_t AWS_MaxKeys = 1000;
inline constexpr uint32_t AWS_MaxBuckets = 10000;
inline constexpr int kListBucketsFdbPage = 1000;

inline constexpr uint32_t kSweeperMaxKeys = 1000;
inline constexpr uint32_t kGcMaxKeys = 1000;
inline constexpr uint32_t kFdbMaxKeys = 5000;
}  // namespace kvrgw
