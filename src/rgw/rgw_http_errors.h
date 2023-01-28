// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_common.h"

typedef const std::map<int,const std::pair<int, const char*>> rgw_http_errors;

extern rgw_http_errors rgw_http_s3_errors;

extern rgw_http_errors rgw_http_swift_errors;

extern rgw_http_errors rgw_http_sts_errors;

extern rgw_http_errors rgw_http_iam_errors;

static inline int rgw_http_error_to_errno(int http_err)
{
  if (http_err >= 200 && http_err <= 299)
    return 0;
  switch (http_err) {
    case 304:
      return -ERR_NOT_MODIFIED;
    case 400:
      return -EINVAL;
    case 401:
      return -EPERM;
    case 403:
        return -EACCES;
    case 404:
        return -ENOENT;
    case 405:
        return -ERR_METHOD_NOT_ALLOWED;
    case 409:
        return -ENOTEMPTY;
    case 503:
        return -EBUSY;
    default:
        return -EIO;
  }

  return 0; /* unreachable */
}
