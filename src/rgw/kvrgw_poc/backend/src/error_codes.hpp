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

#include "kvrgw.pb.h"

#include <foundationdb/fdb_c.h>

namespace kvrgw {

using ::kvrgw::v1::KvrgwErrorCode;
using ::kvrgw::v1::KVRGW_ERR_OK;
using ::kvrgw::v1::KVRGW_ERR_NO_SUCH_KEY;
using ::kvrgw::v1::KVRGW_ERR_NO_SUCH_BUCKET;
using ::kvrgw::v1::KVRGW_ERR_BUCKET_ALREADY_EXISTS;
using ::kvrgw::v1::KVRGW_ERR_BUCKET_NOT_EMPTY;
using ::kvrgw::v1::KVRGW_ERR_PRECONDITION_FAILED;
using ::kvrgw::v1::KVRGW_ERR_ACCESS_DENIED;
using ::kvrgw::v1::KVRGW_ERR_INVALID_ARGUMENT;
using ::kvrgw::v1::KVRGW_ERR_INVALID_BUCKET_NAME;
using ::kvrgw::v1::KVRGW_ERR_NO_SUCH_VERSION;
using ::kvrgw::v1::KVRGW_ERR_NO_SUCH_TENANT;
using ::kvrgw::v1::KVRGW_ERR_INVALID_RANGE;
using ::kvrgw::v1::KVRGW_ERR_INVALID_REQUEST;
using ::kvrgw::v1::KVRGW_ERR_INVALID_TAG;
using ::kvrgw::v1::KVRGW_ERR_FDB_CONFLICT;
using ::kvrgw::v1::KVRGW_ERR_FDB_PROCESS_BEHIND;
using ::kvrgw::v1::KVRGW_ERR_FDB_FUTURE_VERSION;
using ::kvrgw::v1::KVRGW_ERR_FDB_TRANSACTION_TOO_OLD;
using ::kvrgw::v1::KVRGW_ERR_FDB_NOT_COMMITTED;
using ::kvrgw::v1::KVRGW_ERR_FDB_COMMIT_UNKNOWN;
using ::kvrgw::v1::KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED;
using ::kvrgw::v1::KVRGW_ERR_FDB_KEY_TOO_LARGE;
using ::kvrgw::v1::KVRGW_ERR_FDB_VALUE_TOO_LARGE;
using ::kvrgw::v1::KVRGW_ERR_FDB_TRANSACTION_TOO_LARGE;
using ::kvrgw::v1::KVRGW_ERR_INTERNAL;
using ::kvrgw::v1::KVRGW_ERR_CORRUPT_VALUE;
using ::kvrgw::v1::KVRGW_ERR_BUCKET_ID_MISMATCH;
using ::kvrgw::v1::KVRGW_ERR_VALUE_TOO_LARGE;
using ::kvrgw::v1::KVRGW_ERR_TRANSACTION_CONFLICT;
using ::kvrgw::v1::KVRGW_ERR_MAX_RETRIES_EXCEEDED;

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
