// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
//
// Canonical errorCode string values returned in S3 Files API error
// responses.
//
// Smithy declares the six exception types (ValidationException,
// ConflictException, ResourceNotFoundException, ThrottlingException,
// ServiceQuotaExceededException, InternalServerException) and their
// HTTP status codes, but the inner `errorCode` member of each
// exception is typed only as a non-empty string. AWS does not publish
// its canonical errorCode values. This header is the single source of
// truth for the values RGW returns; the Python mirror at
// src/test/rgw/s3files/errors.py must be kept in sync.

#pragma once

#include <string_view>

namespace rgw::s3files {

// Validation (HTTP 400, ValidationException)
inline constexpr std::string_view ERR_INVALID_BUCKET_ARN              = "INVALID_BUCKET_ARN";
inline constexpr std::string_view ERR_INVALID_BUCKET_REGION           = "INVALID_BUCKET_REGION";
inline constexpr std::string_view ERR_INVALID_ROLE_ARN                = "INVALID_ROLE_ARN";
inline constexpr std::string_view ERR_INVALID_PREFIX                  = "INVALID_PREFIX";
inline constexpr std::string_view ERR_INVALID_KMS_KEY                 = "INVALID_KMS_KEY";
inline constexpr std::string_view ERR_INVALID_ROOT_DIRECTORY          = "INVALID_ROOT_DIRECTORY";
inline constexpr std::string_view ERR_INVALID_POSIX_USER              = "INVALID_POSIX_USER";
inline constexpr std::string_view ERR_INVALID_ZONE_ID                 = "INVALID_ZONE_ID";
inline constexpr std::string_view ERR_ZONE_NOT_IN_ZONEGROUP           = "ZONE_NOT_IN_ZONEGROUP";
inline constexpr std::string_view ERR_INVALID_POLICY_DOCUMENT         = "INVALID_POLICY_DOCUMENT";
inline constexpr std::string_view ERR_INVALID_TAG_KEY                 = "INVALID_TAG_KEY";
inline constexpr std::string_view ERR_INVALID_TAG_VALUE               = "INVALID_TAG_VALUE";
inline constexpr std::string_view ERR_BUCKET_CONFIGURATION_WARNING    = "BUCKET_CONFIGURATION_WARNING";
inline constexpr std::string_view ERR_INVALID_MAX_RESULTS             = "INVALID_MAX_RESULTS";
inline constexpr std::string_view ERR_INVALID_SYNC_RULES              = "INVALID_SYNC_RULES";

// Not-found (HTTP 404, ResourceNotFoundException)
inline constexpr std::string_view ERR_BUCKET_NOT_FOUND                = "BUCKET_NOT_FOUND";
inline constexpr std::string_view ERR_ROLE_NOT_FOUND                  = "ROLE_NOT_FOUND";
inline constexpr std::string_view ERR_FILE_SYSTEM_NOT_FOUND           = "FILE_SYSTEM_NOT_FOUND";
inline constexpr std::string_view ERR_ACCESS_POINT_NOT_FOUND          = "ACCESS_POINT_NOT_FOUND";
inline constexpr std::string_view ERR_MOUNT_TARGET_NOT_FOUND          = "MOUNT_TARGET_NOT_FOUND";
inline constexpr std::string_view ERR_POLICY_NOT_FOUND                = "POLICY_NOT_FOUND";

// Conflict (HTTP 409, ConflictException)
inline constexpr std::string_view ERR_FILE_SYSTEM_ALREADY_EXISTS      = "FILE_SYSTEM_ALREADY_EXISTS";
inline constexpr std::string_view ERR_BUCKET_ALREADY_IN_USE           = "BUCKET_ALREADY_IN_USE";
inline constexpr std::string_view ERR_FILE_SYSTEM_HAS_CHILDREN        = "FILE_SYSTEM_HAS_CHILDREN";
inline constexpr std::string_view ERR_FILE_SYSTEM_IN_INVALID_STATE    = "FILE_SYSTEM_IN_INVALID_STATE";
inline constexpr std::string_view ERR_MOUNT_TARGET_ALREADY_IN_ZONE    = "MOUNT_TARGET_ALREADY_EXISTS_IN_ZONE";
inline constexpr std::string_view ERR_PLACEMENT_NOT_BOUND_IN_ZONE     = "PLACEMENT_NOT_BOUND_IN_ZONE";
inline constexpr std::string_view ERR_UNRESOLVED_NFS_SERVICE          = "UNRESOLVED_NFS_SERVICE";

// Quota (HTTP 402, ServiceQuotaExceededException)
inline constexpr std::string_view ERR_TOO_MANY_FILE_SYSTEMS           = "TOO_MANY_FILE_SYSTEMS";
inline constexpr std::string_view ERR_TOO_MANY_ACCESS_POINTS          = "TOO_MANY_ACCESS_POINTS";
inline constexpr std::string_view ERR_TOO_MANY_MOUNT_TARGETS          = "TOO_MANY_MOUNT_TARGETS";
inline constexpr std::string_view ERR_TOO_MANY_TAGS                   = "TOO_MANY_TAGS";
inline constexpr std::string_view ERR_POLICY_TOO_LARGE                = "POLICY_TOO_LARGE";

// Throttling (HTTP 429, ThrottlingException)
inline constexpr std::string_view ERR_THROTTLED                       = "THROTTLED";

// Server-side (HTTP 500, InternalServerException)
inline constexpr std::string_view ERR_INTERNAL                        = "INTERNAL";
inline constexpr std::string_view ERR_BACKEND_UNAVAILABLE             = "BACKEND_UNAVAILABLE";

}  // namespace rgw::s3files
