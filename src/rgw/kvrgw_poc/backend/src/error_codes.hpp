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

#include <foundationdb/fdb_c.h>
#include <cstdint>

namespace kvrgw {

enum KvrgwErrorCode : int32_t {
  KVRGW_ERR_OK = 0,
  KVRGW_ERR_NO_SUCH_KEY = 100,
  KVRGW_ERR_NO_SUCH_BUCKET = 101,
  KVRGW_ERR_BUCKET_ALREADY_EXISTS = 102,
  KVRGW_ERR_BUCKET_NOT_EMPTY = 103,
  KVRGW_ERR_PRECONDITION_FAILED = 104,
  KVRGW_ERR_ACCESS_DENIED = 105,
  KVRGW_ERR_INVALID_ARGUMENT = 106,
  KVRGW_ERR_INVALID_BUCKET_NAME = 107,
  KVRGW_ERR_NO_SUCH_VERSION = 108,
  KVRGW_ERR_NO_SUCH_TENANT = 109,
  KVRGW_ERR_INVALID_RANGE = 110,
  KVRGW_ERR_INVALID_REQUEST = 111,
  KVRGW_ERR_INVALID_TAG = 112,
  KVRGW_ERR_TENANT_ALREADY_EXISTS = 113,
  KVRGW_ERR_FDB_CONFLICT = 200,
  KVRGW_ERR_FDB_PROCESS_BEHIND = 201,
  KVRGW_ERR_FDB_FUTURE_VERSION = 202,
  KVRGW_ERR_FDB_TRANSACTION_TOO_OLD = 203,
  KVRGW_ERR_FDB_NOT_COMMITTED = 204,
  KVRGW_ERR_FDB_COMMIT_UNKNOWN = 205,
  KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED = 206,
  KVRGW_ERR_FDB_KEY_TOO_LARGE = 300,
  KVRGW_ERR_FDB_VALUE_TOO_LARGE = 301,
  KVRGW_ERR_FDB_TRANSACTION_TOO_LARGE = 302,
  KVRGW_ERR_INTERNAL = 400,
  KVRGW_ERR_CORRUPT_VALUE = 401,
  KVRGW_ERR_BUCKET_ID_MISMATCH = 402,
  KVRGW_ERR_VALUE_TOO_LARGE = 403,
  KVRGW_ERR_TRANSACTION_CONFLICT = 404,
  KVRGW_ERR_MAX_RETRIES_EXCEEDED = 405,
};

// Was `KvrgwErrorCode_MAX + 1` (protobuf convention); KVRGW_ERR_MAX_RETRIES_EXCEEDED=405 is
// the highest value.
inline constexpr int KvrgwErrorCode_ARRAYSIZE = 406;

// Returns the bare enumerator name (e.g. "KVRGW_ERR_OK"), matching protobuf's Name().
// Implemented in error_codes.cpp (mirrors the existing kvrgw_strerror() switch below it).
const char* KvrgwErrorCode_Name(KvrgwErrorCode code);

namespace fdb {
  constexpr fdb_error_t kConflict              = 1020;
  constexpr fdb_error_t kProcessBehind         = 1037;
  constexpr fdb_error_t kFutureVersion         = 1009;
  constexpr fdb_error_t kTransactionTooOld     = 1007;
  constexpr fdb_error_t kCommitUnknownResult   = 1021;
  constexpr fdb_error_t kClusterVersionChanged = 1039;
  constexpr fdb_error_t kKeyTooLarge           = 2102;
  constexpr fdb_error_t kValueTooLarge         = 2103;
  constexpr fdb_error_t kTransactionTooLarge   = 2101;
}

inline KvrgwErrorCode fdb_to_error(fdb_error_t err) {
  switch (err) {
    case fdb::kConflict:              return KVRGW_ERR_FDB_CONFLICT;
    case fdb::kProcessBehind:         return KVRGW_ERR_FDB_PROCESS_BEHIND;
    case fdb::kFutureVersion:         return KVRGW_ERR_FDB_FUTURE_VERSION;
    case fdb::kTransactionTooOld:     return KVRGW_ERR_FDB_TRANSACTION_TOO_OLD;
    case fdb::kCommitUnknownResult:   return KVRGW_ERR_FDB_COMMIT_UNKNOWN;
    case fdb::kClusterVersionChanged: return KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED;
    case fdb::kKeyTooLarge:           return KVRGW_ERR_FDB_KEY_TOO_LARGE;
    case fdb::kValueTooLarge:         return KVRGW_ERR_FDB_VALUE_TOO_LARGE;
    case fdb::kTransactionTooLarge:   return KVRGW_ERR_FDB_TRANSACTION_TOO_LARGE;
    default:                          return KVRGW_ERR_INTERNAL;
  }
}

inline bool is_retriable_idempotent(KvrgwErrorCode c) {
  switch (c) {
    case KVRGW_ERR_FDB_CONFLICT:
    case KVRGW_ERR_FDB_PROCESS_BEHIND:
    case KVRGW_ERR_FDB_FUTURE_VERSION:
    case KVRGW_ERR_FDB_TRANSACTION_TOO_OLD:
    case KVRGW_ERR_FDB_NOT_COMMITTED:
    case KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED:
      return true;
    default:
      return false;
  }
}

inline bool is_retriable_not_idempotent(KvrgwErrorCode c) {
  return c == KVRGW_ERR_FDB_COMMIT_UNKNOWN;
}

inline bool is_retriable(KvrgwErrorCode c) {
  return is_retriable_idempotent(c) || is_retriable_not_idempotent(c);
}

const char* kvrgw_strerror(KvrgwErrorCode code);

}  // namespace kvrgw
