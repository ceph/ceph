// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_HTTP_ERRORS_H_
#define RGW_HTTP_ERRORS_H_

#include "rgw_common.h"

struct rgw_http_errors {
  int err_no;
  int http_ret;
  const char *s3_code;
  const char *s3_message; // error message in DeleteMultiObjects.
};

const static struct rgw_http_errors RGW_HTTP_ERRORS[] = {
    { 0, 200, "", "" },
    { STATUS_CREATED, 201, "Created", "Created" },
    { STATUS_ACCEPTED, 202, "Accepted", "Accepted" },
    { STATUS_NO_CONTENT, 204, "NoContent", "No content" },
    { STATUS_PARTIAL_CONTENT, 206, "", ""  },
    { ERR_WEBSITE_REDIRECT, 301, "WebsiteRedirect", "Website redirect" },
    { STATUS_REDIRECT, 303, "", "Status redirect" },
    { ERR_NOT_MODIFIED, 304, "NotModified", "Not modified" },
    { EINVAL, 400, "InvalidArgument", "Invalied Argument" },
    { ERR_INVALID_REQUEST, 400, "InvalidRequest", "The request or one or more of the params or headers are invalid." },
    { ERR_INVALID_DIGEST, 400, "InvalidDigest", "The Content-MD5 you specified is not valid." },
    { ERR_BAD_DIGEST, 400, "BadDigest", "The Content-MD5 you specified did not match what we received." },
    { ERR_INVALID_BUCKET_NAME, 400, "InvalidBucketName", "The specified bucket is not valid." },
    { ERR_INVALID_OBJECT_NAME, 400, "InvalidObjectName", "The specified object is not valid." },
    { ERR_UNRESOLVABLE_EMAIL, 400, "UnresolvableGrantByEmailAddress", "The email address you provided does not match any account on record." },
    { ERR_INVALID_PART, 400, "InvalidPart", "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag." },
    { ERR_INVALID_PART_ORDER, 400, "InvalidPartOrder", "The list of parts was not in ascending order. Parts list must specified in order by part number." },
    { ERR_REQUEST_TIMEOUT, 400, "RequestTimeout", "Your socket connection to the server was not read from or written to within the timeout period." },
    { ERR_TOO_LARGE, 400, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed object size." },
    { ERR_TOO_SMALL, 400, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size." },
    { ERR_TOO_MANY_BUCKETS, 400, "TooManyBuckets", "You have attempted to create more buckets than allowed." },
    { ERR_MALFORMED_XML, 400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema." },
    { ERR_AMZ_CONTENT_SHA256_MISMATCH, 400, "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed." },
    { ERR_MALFORMED_DOC, 400, "MalformedPolicyDocument", "The policy document you provided was not well-formed or did not validate against our published schema." },
    { ERR_LENGTH_REQUIRED, 411, "MissingContentLength", "You must provide the Content-Length HTTP header." },
    { EACCES, 403, "AccessDenied", "Access Denied" },
    { EPERM, 403, "AccessDenied", "Access Denied" },
    { ERR_SIGNATURE_NO_MATCH, 403, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided. Check your key and signing method." },
    { ERR_INVALID_ACCESS_KEY, 403, "InvalidAccessKeyId", "The access key ID you provided does not exist in our records." },
    { ERR_USER_SUSPENDED, 403, "UserSuspended", "User Suspended" },
    { ERR_REQUEST_TIME_SKEWED, 403, "RequestTimeTooSkewed", "The difference between the request time and the server's time is too large." },
    { ERR_QUOTA_EXCEEDED, 403, "QuotaExceeded", "Quota Exceeded" }, 
    { ENOENT, 404, "NoSuchKey", "The resource you requested does not exist" },
    { ERR_NO_SUCH_BUCKET, 404, "NoSuchBucket", "The specified bucket does not exist." },
    { ERR_NO_SUCH_WEBSITE_CONFIGURATION, 404, "NoSuchWebsiteConfiguration", "No such website configuration" },
    { ERR_NO_SUCH_UPLOAD, 404, "NoSuchUpload", "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed." },
    { ERR_NOT_FOUND, 404, "Not Found", "Not Found"},
    { ERR_NO_SUCH_LC, 404, "NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist." },
    { ERR_NO_ROLE_FOUND, 404, "NoSuchEntity", "The role does not exist." },
    { ERR_METHOD_NOT_ALLOWED, 405, "MethodNotAllowed", "The specified method is not allowed against this resource." },
    { ETIMEDOUT, 408, "RequestTimeout", "Your socket connection to the server was not read from or written to within the timeout period." },
    { EEXIST, 409, "BucketAlreadyExists", "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again." },
    { ERR_USER_EXIST, 409, "UserAlreadyExists", "The requested user name is not available. Please select a different name and try again." }, // doesn't exist in s3 error codes
    { ERR_EMAIL_EXIST, 409, "EmailExists", "The request email is not available. Please select a different email and try again." }, // doesn't exist in s3 error codes
    { ERR_KEY_EXIST, 409, "KeyExists", "The request key is not available. Please select a different key and try again." },  // doesn't exist in s3 error codes
    { ERR_ROLE_EXISTS, 409, "EntityAlreadyExists", ""},
    { ERR_DELETE_CONFLICT, 409, "DeleteConflict", ""},
    { ERR_INVALID_SECRET_KEY, 400, "InvalidSecretKey", "The provided security credentials are not valid."},
    { ERR_INVALID_KEY_TYPE, 400, "InvalidKeyType", "Invalid Key Type"},
    { ERR_INVALID_CAP, 400, "InvalidCapability", "Invalid Capability"},
    { ERR_INVALID_TENANT_NAME, 400, "InvalidTenantName", "Invalid Tenant Name" },
    { ENOTEMPTY, 409, "BucketNotEmpty", "The bucket you tried to delete is not empty." },
    { ERR_PRECONDITION_FAILED, 412, "PreconditionFailed", "At least one of the preconditions you specified did not hold." },
    { ERANGE, 416, "InvalidRange", "The requested range cannot be satisfied." },
    { ERR_UNPROCESSABLE_ENTITY, 422, "UnprocessableEntity", "Unprocessable Entity" },
    { ERR_LOCKED, 423, "Locked", "Locked" },
    { ERR_INTERNAL_ERROR, 500, "InternalError", "We encountered an internal error. Please try again." },
    { ERR_NOT_IMPLEMENTED, 501, "NotImplemented", "A header you provided implies functionality that is not implemented." },
    { ERR_SERVICE_UNAVAILABLE, 503, "ServiceUnavailable", "Reduce your request rate."}
};

const static struct rgw_http_errors RGW_HTTP_SWIFT_ERRORS[] = {
    { EACCES, 403, "AccessDenied" },
    { EPERM, 401, "AccessDenied" },
    { ERR_USER_SUSPENDED, 401, "UserSuspended" },
    { ERR_INVALID_UTF8, 412, "Invalid UTF8" },
    { ERR_BAD_URL, 412, "Bad URL" },
    { ERR_NOT_SLO_MANIFEST, 400, "Not an SLO manifest" },
    { ERR_QUOTA_EXCEEDED, 413, "QuotaExceeded" }
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
    case 409:
        return -ENOTEMPTY;
    default:
        return -EIO;
  }

  return 0; /* unreachable */
}


#endif
