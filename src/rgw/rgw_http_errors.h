// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_HTTP_ERRORS_H_
#define RGW_HTTP_ERRORS_H_

#include "rgw_common.h"

struct rgw_http_errors {
  int err_no;
  int http_ret;
  const char *s3_code;
};

const static struct rgw_http_errors RGW_HTTP_ERRORS[] = {
    { 0, 200, "" },
    { STATUS_CREATED, 201, "Created" },
    { STATUS_ACCEPTED, 202, "Accepted" },
    { STATUS_NO_CONTENT, 204, "NoContent" },
    { STATUS_PARTIAL_CONTENT, 206, "" },
    { ERR_PERMANENT_REDIRECT, 301, "PermanentRedirect" },
    { ERR_WEBSITE_REDIRECT, 301, "WebsiteRedirect" },
    { STATUS_REDIRECT, 303, "" },
    { ERR_NOT_MODIFIED, 304, "NotModified" },
    { EINVAL, 400, "InvalidArgument" },
    { ERR_INVALID_REQUEST, 400, "InvalidRequest" },
    { ERR_INVALID_DIGEST, 400, "InvalidDigest" },
    { ERR_BAD_DIGEST, 400, "BadDigest" },
    { ERR_INVALID_BUCKET_NAME, 400, "InvalidBucketName" },
    { ERR_INVALID_OBJECT_NAME, 400, "InvalidObjectName" },
    { ERR_UNRESOLVABLE_EMAIL, 400, "UnresolvableGrantByEmailAddress" },
    { ERR_INVALID_PART, 400, "InvalidPart" },
    { ERR_INVALID_PART_ORDER, 400, "InvalidPartOrder" },
    { ERR_REQUEST_TIMEOUT, 400, "RequestTimeout" },
    { ERR_TOO_LARGE, 400, "EntityTooLarge" },
    { ERR_TOO_SMALL, 400, "EntityTooSmall" },
    { ERR_TOO_MANY_BUCKETS, 400, "TooManyBuckets" },
    { ERR_MALFORMED_XML, 400, "MalformedXML" },
    { ERR_AMZ_CONTENT_SHA256_MISMATCH, 400, "XAmzContentSHA256Mismatch" },
    { ERR_LENGTH_REQUIRED, 411, "MissingContentLength" },
    { EACCES, 403, "AccessDenied" },
    { EPERM, 403, "AccessDenied" },
    { ERR_SIGNATURE_NO_MATCH, 403, "SignatureDoesNotMatch" },
    { ERR_INVALID_ACCESS_KEY, 403, "InvalidAccessKeyId" },
    { ERR_USER_SUSPENDED, 403, "UserSuspended" },
    { ERR_REQUEST_TIME_SKEWED, 403, "RequestTimeTooSkewed" },
    { ERR_QUOTA_EXCEEDED, 403, "QuotaExceeded" },
    { ENOENT, 404, "NoSuchKey" },
    { ERR_NO_SUCH_BUCKET, 404, "NoSuchBucket" },
    { ERR_NO_SUCH_WEBSITE_CONFIGURATION, 404, "NoSuchWebsiteConfiguration" },
    { ERR_NO_SUCH_UPLOAD, 404, "NoSuchUpload" },
    { ERR_NOT_FOUND, 404, "Not Found"},
    { ERR_METHOD_NOT_ALLOWED, 405, "MethodNotAllowed" },
    { ETIMEDOUT, 408, "RequestTimeout" },
    { EEXIST, 409, "BucketAlreadyExists" },
    { ERR_USER_EXIST, 409, "UserAlreadyExists" },
    { ERR_EMAIL_EXIST, 409, "EmailExists" },
    { ERR_KEY_EXIST, 409, "KeyExists"},
    { ERR_INVALID_SECRET_KEY, 400, "InvalidSecretKey"},
    { ERR_INVALID_KEY_TYPE, 400, "InvalidKeyType"},
    { ERR_INVALID_CAP, 400, "InvalidCapability"},
    { ERR_INVALID_TENANT_NAME, 400, "InvalidTenantName" },
    { ENOTEMPTY, 409, "BucketNotEmpty" },
    { ERR_PRECONDITION_FAILED, 412, "PreconditionFailed" },
    { ERANGE, 416, "InvalidRange" },
    { ERR_UNPROCESSABLE_ENTITY, 422, "UnprocessableEntity" },
    { ERR_LOCKED, 423, "Locked" },
    { ERR_INTERNAL_ERROR, 500, "InternalError" },
    { ERR_NOT_IMPLEMENTED, 501, "NotImplemented" },
};

const static struct rgw_http_errors RGW_HTTP_SWIFT_ERRORS[] = {
    { EACCES, 401, "AccessDenied" },
    { EPERM, 401, "AccessDenied" },
    { ERR_USER_SUSPENDED, 401, "UserSuspended" },
    { ERR_INVALID_UTF8, 412, "Invalid UTF8" },
    { ERR_BAD_URL, 412, "Bad URL" },
    { ERR_NOT_SLO_MANIFEST, 400, "Not an SLO manifest" }
};

struct rgw_http_status_code {
  int code;
  const char *name;
};

const static struct rgw_http_status_code http_codes[] = {
  { 100, "Continue" },
  { 200, "OK" },
  { 201, "Created" },
  { 202, "Accepted" },
  { 204, "No Content" },
  { 205, "Reset Content" },
  { 206, "Partial Content" },
  { 207, "Multi Status" },
  { 208, "Already Reported" },
  { 300, "Multiple Choices" },
  { 301, "Moved Permanently" },
  { 302, "Found" },
  { 303, "See Other" },
  { 304, "Not Modified" },
  { 305, "User Proxy" },
  { 306, "Switch Proxy" },
  { 307, "Temporary Redirect" },
  { 308, "Permanent Redirect" },
  { 400, "Bad Request" },
  { 401, "Unauthorized" },
  { 402, "Payment Required" },
  { 403, "Forbidden" },
  { 404, "Not Found" },
  { 405, "Method Not Allowed" },
  { 406, "Not Acceptable" },
  { 407, "Proxy Authentication Required" },
  { 408, "Request Timeout" },
  { 409, "Conflict" },
  { 410, "Gone" },
  { 411, "Length Required" },
  { 412, "Precondition Failed" },
  { 413, "Request Entity Too Large" },
  { 414, "Request-URI Too Long" },
  { 415, "Unsupported Media Type" },
  { 416, "Requested Range Not Satisfiable" },
  { 417, "Expectation Failed" },
  { 422, "Unprocessable Entity" },
  { 500, "Internal Server Error" },
  { 501, "Not Implemented" },
  { 0, NULL },
};

#define ARRAY_LEN(arr) (sizeof(arr) / sizeof(arr[0]))

static inline const struct rgw_http_errors *search_err(int err_no, const struct rgw_http_errors *errs, int len)
{
  for (int i = 0; i < len; ++i, ++errs) {
    if (err_no == errs->err_no)
      return errs;
  }
  return NULL;
}


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
    default:
        return -EIO;
  }

  return 0; /* unreachable */
}


#endif
