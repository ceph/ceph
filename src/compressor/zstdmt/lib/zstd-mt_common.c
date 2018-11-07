
/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * Copyright (c) 2016 Tino Reichardt
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "zstd.h"
#include "zstd-mt.h"

/* ****************************************
 * ZSTDMT Error Management
 ******************************************/

size_t zstdmt_errcode;

/**
 * ZSTDMT_isError() - tells if a return value is an error code
 */
unsigned ZSTDMT_isError(size_t code)
{
  return (code > ZSTDMT_ERROR(maxCode));
}

/**
 * LZ4MT_getErrorString() - give error code string from function result
 */
const char *ZSTDMT_getErrorString(size_t code)
{
  static const char *noErrorCode = "Unspecified zstmt error code";

  if (ZSTD_isError(zstdmt_errcode))
    return ZSTD_getErrorName(zstdmt_errcode);

  switch ((ZSTDMT_ErrorCode) (0 - code)) {
    case ZSTDMT_PREFIX(no_error):
      return "No error detected";
    case ZSTDMT_PREFIX(memory_allocation):
      return "Allocation error : not enough memory";
    case ZSTDMT_PREFIX(read_fail):
      return "Read failure";
    case ZSTDMT_PREFIX(write_fail):
      return "Write failure";
    case ZSTDMT_PREFIX(data_error):
      return "Malformed input";
    case ZSTDMT_PREFIX(frame_compress):
      return "Could not compress frame at once";
    case ZSTDMT_PREFIX(frame_decompress):
      return "Could not decompress frame at once";
    case ZSTDMT_PREFIX(compressionParameter_unsupported):
      return "Compression parameter is out of bound";
    case ZSTDMT_PREFIX(compression_library):
      return "Compression library reports failure";
    case ZSTDMT_PREFIX(maxCode):
    default:
      return noErrorCode;
  }
}
