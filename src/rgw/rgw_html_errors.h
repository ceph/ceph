#ifndef RGW_HTML_ERRORS_H_
#define RGW_HTML_ERRORS_H_

#include "rgw_common.h"

struct rgw_html_errors {
  int err_no;
  int http_ret;
  const char *s3_code;
};

const static struct rgw_html_errors RGW_HTML_ERRORS[] = {
    { 0, 200, "" },
    { STATUS_CREATED, 201, "Created" },
    { STATUS_ACCEPTED, 202, "Accepted" },
    { STATUS_NO_CONTENT, 204, "NoContent" },
    { STATUS_PARTIAL_CONTENT, 206, "" },
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
    { ERR_LENGTH_REQUIRED, 411, "MissingContentLength" },
    { EACCES, 403, "AccessDenied" },
    { EPERM, 403, "AccessDenied" },
    { ERR_USER_SUSPENDED, 403, "UserSuspended" },
    { ERR_REQUEST_TIME_SKEWED, 403, "RequestTimeTooSkewed" },
    { ENOENT, 404, "NoSuchKey" },
    { ERR_NO_SUCH_BUCKET, 404, "NoSuchBucket" },
    { ERR_NO_SUCH_UPLOAD, 404, "NoSuchUpload" },
    { ERR_NOT_FOUND, 404, "Not Found"},
    { ERR_METHOD_NOT_ALLOWED, 405, "MethodNotAllowed" },
    { ETIMEDOUT, 408, "RequestTimeout" },
    { EEXIST, 409, "BucketAlreadyExists" },
    { ENOTEMPTY, 409, "BucketNotEmpty" },
    { ERR_PRECONDITION_FAILED, 412, "PreconditionFailed" },
    { ERANGE, 416, "InvalidRange" },
    { ERR_UNPROCESSABLE_ENTITY, 422, "UnprocessableEntity" },
    { ERR_INTERNAL_ERROR, 500, "InternalError" },
};

const static struct rgw_html_errors RGW_HTML_SWIFT_ERRORS[] = {
    { EACCES, 401, "AccessDenied" },
    { EPERM, 401, "AccessDenied" },
    { ERR_USER_SUSPENDED, 401, "UserSuspended" },
    { ERR_INVALID_UTF8, 412, "Invalid UTF8" },
    { ERR_BAD_URL, 412, "Bad URL" },
};

#define ARRAY_LEN(arr) (sizeof(arr) / sizeof(arr[0]))

static inline const struct rgw_html_errors *search_err(int err_no, const struct rgw_html_errors *errs, int len)
{
  for (int i = 0; i < len; ++i, ++errs) {
    if (err_no == errs->err_no)
      return errs;
  }
  return NULL;
}



#endif
