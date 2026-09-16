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

#include "error_codes.hpp"

namespace kvrgw {

const char *kvrgw_strerror(KvrgwErrorCode code)
{
  switch (code) {
  case KVRGW_ERR_OK:
    return "Success";
  case KVRGW_ERR_NO_SUCH_KEY:
    return "No such key";
  case KVRGW_ERR_NO_SUCH_BUCKET:
    return "No such bucket";
  case KVRGW_ERR_BUCKET_ALREADY_EXISTS:
    return "Bucket already exists";
  case KVRGW_ERR_BUCKET_NOT_EMPTY:
    return "Bucket not empty";
  case KVRGW_ERR_PRECONDITION_FAILED:
    return "Precondition failed";
  case KVRGW_ERR_ACCESS_DENIED:
    return "Access denied";
  case KVRGW_ERR_INVALID_ARGUMENT:
    return "Invalid argument";
  case KVRGW_ERR_INVALID_REQUEST:
    return "Invalid request";
  case KVRGW_ERR_INVALID_TAG:
    return "Invalid tag";
  case KVRGW_ERR_INVALID_BUCKET_NAME:
    return "Invalid bucket name";
  case KVRGW_ERR_NO_SUCH_VERSION:
    return "No such version";
  case KVRGW_ERR_NO_SUCH_TENANT:
    return "No such tenant";
  case KVRGW_ERR_FDB_CONFLICT:
    return "FDB transaction conflict";
  case KVRGW_ERR_FDB_PROCESS_BEHIND:
    return "FDB storage server behind";
  case KVRGW_ERR_FDB_FUTURE_VERSION:
    return "FDB future version requested";
  case KVRGW_ERR_FDB_TRANSACTION_TOO_OLD:
    return "FDB transaction too old";
  case KVRGW_ERR_FDB_NOT_COMMITTED:
    return "FDB transaction not committed";
  case KVRGW_ERR_FDB_COMMIT_UNKNOWN:
    return "FDB commit result unknown";
  case KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED:
    return "FDB cluster version changed";
  case KVRGW_ERR_FDB_KEY_TOO_LARGE:
    return "FDB key too large";
  case KVRGW_ERR_FDB_VALUE_TOO_LARGE:
    return "FDB value too large";
  case KVRGW_ERR_FDB_TRANSACTION_TOO_LARGE:
    return "FDB transaction too large";
  case KVRGW_ERR_INTERNAL:
    return "Internal error";
  case KVRGW_ERR_CORRUPT_VALUE:
    return "Corrupt KV value";
  case KVRGW_ERR_BUCKET_ID_MISMATCH:
    return "Bucket ID mismatch";
  case KVRGW_ERR_VALUE_TOO_LARGE:
    return "Value too large for tier";
  case KVRGW_ERR_TRANSACTION_CONFLICT:
    return "Transaction conflict limit exceeded";
  case KVRGW_ERR_MAX_RETRIES_EXCEEDED:
    return "Max retries exceeded";
  default:
    return "Unknown error";
  }
}

} // namespace kvrgw
