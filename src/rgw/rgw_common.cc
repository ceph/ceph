// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <vector>
#include <algorithm>
#include <string>
#include <boost/tokenizer.hpp>

#include "json_spirit/json_spirit.h"
#include "common/ceph_json.h"

#include "rgw_op.h"
#include "rgw_common.h"
#include "rgw_acl.h"
#include "rgw_string.h"
#include "rgw_rados.h"
#include "rgw_http_errors.h"
#include "rgw_arn.h"
#include "rgw_data_sync.h"

#include "common/ceph_crypto.h"
#include "common/armor.h"
#include "common/errno.h"
#include "common/Clock.h"
#include "common/Formatter.h"
#include "common/convenience.h"
#include "common/strtol.h"
#include "include/str_list.h"
#include "rgw_crypt_sanitize.h"
#include "rgw_bucket_sync.h"
#include "rgw_sync_policy.h"

#include "services/svc_zone.h"

#include <sstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using rgw::ARN;
using rgw::IAM::Effect;
using rgw::IAM::op_to_perm;
using rgw::IAM::Policy;

const uint32_t RGWBucketInfo::NUM_SHARDS_BLIND_BUCKET(UINT32_MAX);

rgw_http_errors rgw_http_s3_errors({
    { 0, {200, "" }},
    { STATUS_CREATED, {201, "Created" }},
    { STATUS_ACCEPTED, {202, "Accepted" }},
    { STATUS_NO_CONTENT, {204, "NoContent" }},
    { STATUS_PARTIAL_CONTENT, {206, "" }},
    { ERR_PERMANENT_REDIRECT, {301, "PermanentRedirect" }},
    { ERR_WEBSITE_REDIRECT, {301, "WebsiteRedirect" }},
    { STATUS_REDIRECT, {303, "" }},
    { ERR_NOT_MODIFIED, {304, "NotModified" }},
    { EINVAL, {400, "InvalidArgument" }},
    { ERR_INVALID_REQUEST, {400, "InvalidRequest" }},
    { ERR_INVALID_DIGEST, {400, "InvalidDigest" }},
    { ERR_BAD_DIGEST, {400, "BadDigest" }},
    { ERR_INVALID_LOCATION_CONSTRAINT, {400, "InvalidLocationConstraint" }},
    { ERR_ZONEGROUP_DEFAULT_PLACEMENT_MISCONFIGURATION, {400, "ZonegroupDefaultPlacementMisconfiguration" }},
    { ERR_INVALID_BUCKET_NAME, {400, "InvalidBucketName" }},
    { ERR_INVALID_OBJECT_NAME, {400, "InvalidObjectName" }},
    { ERR_UNRESOLVABLE_EMAIL, {400, "UnresolvableGrantByEmailAddress" }},
    { ERR_INVALID_PART, {400, "InvalidPart" }},
    { ERR_INVALID_PART_ORDER, {400, "InvalidPartOrder" }},
    { ERR_REQUEST_TIMEOUT, {400, "RequestTimeout" }},
    { ERR_TOO_LARGE, {400, "EntityTooLarge" }},
    { ERR_TOO_SMALL, {400, "EntityTooSmall" }},
    { ERR_TOO_MANY_BUCKETS, {400, "TooManyBuckets" }},
    { ERR_MALFORMED_XML, {400, "MalformedXML" }},
    { ERR_AMZ_CONTENT_SHA256_MISMATCH, {400, "XAmzContentSHA256Mismatch" }},
    { ERR_MALFORMED_DOC, {400, "MalformedPolicyDocument"}},
    { ERR_INVALID_TAG, {400, "InvalidTag"}},
    { ERR_MALFORMED_ACL_ERROR, {400, "MalformedACLError" }},
    { ERR_INVALID_CORS_RULES_ERROR, {400, "InvalidRequest" }},
    { ERR_INVALID_WEBSITE_ROUTING_RULES_ERROR, {400, "InvalidRequest" }},
    { ERR_INVALID_ENCRYPTION_ALGORITHM, {400, "InvalidEncryptionAlgorithmError" }},
    { ERR_INVALID_RETENTION_PERIOD,{400, "InvalidRetentionPeriod"}},
    { ERR_LIMIT_EXCEEDED, {400, "LimitExceeded" }},
    { ERR_LENGTH_REQUIRED, {411, "MissingContentLength" }},
    { EACCES, {403, "AccessDenied" }},
    { EPERM, {403, "AccessDenied" }},
    { ERR_SIGNATURE_NO_MATCH, {403, "SignatureDoesNotMatch" }},
    { ERR_INVALID_ACCESS_KEY, {403, "InvalidAccessKeyId" }},
    { ERR_USER_SUSPENDED, {403, "UserSuspended" }},
    { ERR_REQUEST_TIME_SKEWED, {403, "RequestTimeTooSkewed" }},
    { ERR_QUOTA_EXCEEDED, {403, "QuotaExceeded" }},
    { ERR_MFA_REQUIRED, {403, "AccessDenied" }},
    { ENOENT, {404, "NoSuchKey" }},
    { ERR_NO_SUCH_BUCKET, {404, "NoSuchBucket" }},
    { ERR_NO_SUCH_WEBSITE_CONFIGURATION, {404, "NoSuchWebsiteConfiguration" }},
    { ERR_NO_SUCH_UPLOAD, {404, "NoSuchUpload" }},
    { ERR_NOT_FOUND, {404, "Not Found"}},
    { ERR_NO_SUCH_LC, {404, "NoSuchLifecycleConfiguration"}},
    { ERR_NO_SUCH_BUCKET_POLICY, {404, "NoSuchBucketPolicy"}},
    { ERR_NO_SUCH_USER, {404, "NoSuchUser"}},
    { ERR_NO_ROLE_FOUND, {404, "NoSuchEntity"}},
    { ERR_NO_CORS_FOUND, {404, "NoSuchCORSConfiguration"}},
    { ERR_NO_SUCH_SUBUSER, {404, "NoSuchSubUser"}},
    { ERR_NO_SUCH_ENTITY, {404, "NoSuchEntity"}},
    { ERR_NO_SUCH_CORS_CONFIGURATION, {404, "NoSuchCORSConfiguration"}},
    { ERR_NO_SUCH_OBJECT_LOCK_CONFIGURATION, {404, "ObjectLockConfigurationNotFoundError"}},
    { ERR_METHOD_NOT_ALLOWED, {405, "MethodNotAllowed" }},
    { ETIMEDOUT, {408, "RequestTimeout" }},
    { EEXIST, {409, "BucketAlreadyExists" }},
    { ERR_USER_EXIST, {409, "UserAlreadyExists" }},
    { ERR_EMAIL_EXIST, {409, "EmailExists" }},
    { ERR_KEY_EXIST, {409, "KeyExists"}},
    { ERR_TAG_CONFLICT, {409, "OperationAborted"}},
    { ERR_POSITION_NOT_EQUAL_TO_LENGTH, {409, "PositionNotEqualToLength"}},
    { ERR_OBJECT_NOT_APPENDABLE, {409, "ObjectNotAppendable"}},
    { ERR_INVALID_BUCKET_STATE, {409, "InvalidBucketState"}},
    { ERR_INVALID_SECRET_KEY, {400, "InvalidSecretKey"}},
    { ERR_INVALID_KEY_TYPE, {400, "InvalidKeyType"}},
    { ERR_INVALID_CAP, {400, "InvalidCapability"}},
    { ERR_INVALID_TENANT_NAME, {400, "InvalidTenantName" }},
    { ENOTEMPTY, {409, "BucketNotEmpty" }},
    { ERR_PRECONDITION_FAILED, {412, "PreconditionFailed" }},
    { ERANGE, {416, "InvalidRange" }},
    { ERR_UNPROCESSABLE_ENTITY, {422, "UnprocessableEntity" }},
    { ERR_LOCKED, {423, "Locked" }},
    { ERR_INTERNAL_ERROR, {500, "InternalError" }},
    { ERR_NOT_IMPLEMENTED, {501, "NotImplemented" }},
    { ERR_SERVICE_UNAVAILABLE, {503, "ServiceUnavailable"}},
    { ERR_RATE_LIMITED, {503, "SlowDown"}},
    { ERR_ZERO_IN_URL, {400, "InvalidRequest" }},
    { ERR_NO_SUCH_TAG_SET, {404, "NoSuchTagSetError"}},
});

rgw_http_errors rgw_http_swift_errors({
    { EACCES, {403, "AccessDenied" }},
    { EPERM, {401, "AccessDenied" }},
    { ENAMETOOLONG, {400, "Metadata name too long" }},
    { ERR_USER_SUSPENDED, {401, "UserSuspended" }},
    { ERR_INVALID_UTF8, {412, "Invalid UTF8" }},
    { ERR_BAD_URL, {412, "Bad URL" }},
    { ERR_NOT_SLO_MANIFEST, {400, "Not an SLO manifest" }},
    { ERR_QUOTA_EXCEEDED, {413, "QuotaExceeded" }},
    { ENOTEMPTY, {409, "There was a conflict when trying "
                       "to complete your request." }},
    /* FIXME(rzarzynski): we need to find a way to apply Swift's error handling
     * procedures also for ERR_ZERO_IN_URL. This make a problem as the validation
     * is performed very early, even before setting the req_state::proto_flags. */
    { ERR_ZERO_IN_URL, {412, "Invalid UTF8 or contains NULL"}},
    { ERR_RATE_LIMITED, {498, "Rate Limited"}},
});

rgw_http_errors rgw_http_sts_errors({
    { ERR_PACKED_POLICY_TOO_LARGE, {400, "PackedPolicyTooLarge" }},
    { ERR_INVALID_IDENTITY_TOKEN, {400, "InvalidIdentityToken" }},
});

rgw_http_errors rgw_http_iam_errors({
    { EINVAL, {400, "InvalidInput" }},
    { ENOENT, {404, "NoSuchEntity"}},
    { ERR_ROLE_EXISTS, {409, "EntityAlreadyExists"}},
    { ERR_DELETE_CONFLICT, {409, "DeleteConflict"}},
    { EEXIST, {409, "EntityAlreadyExists"}},
    { ERR_INTERNAL_ERROR, {500, "ServiceFailure" }},
});

using namespace ceph::crypto;

rgw_err::
rgw_err()
{
  clear();
}

void rgw_err::
clear()
{
  http_ret = 200;
  ret = 0;
  err_code.clear();
}

bool rgw_err::
is_clear() const
{
  return (http_ret == 200);
}

bool rgw_err::
is_err() const
{
  return !(http_ret >= 200 && http_ret <= 399);
}

// The requestURI transferred from the frontend can be abs_path or absoluteURI
// If it is absoluteURI, we should adjust it to abs_path for the following 
// S3 authorization and some other processes depending on the requestURI
// The absoluteURI can start with "http://", "https://", "ws://" or "wss://"
static string get_abs_path(const string& request_uri) {
  const static string ABS_PREFIXS[] = {"http://", "https://", "ws://", "wss://"};
  bool isAbs = false;
  for (int i = 0; i < 4; ++i) {
    if (boost::algorithm::starts_with(request_uri, ABS_PREFIXS[i])) {
      isAbs = true;
      break;
    } 
  }
  if (!isAbs) {  // it is not a valid absolute uri
    return request_uri;
  }
  size_t beg_pos = request_uri.find("://") + 3;
  size_t len = request_uri.size();
  beg_pos = request_uri.find('/', beg_pos);
  if (beg_pos == string::npos) return request_uri;
  return request_uri.substr(beg_pos, len - beg_pos);
}

req_info::req_info(CephContext *cct, const class RGWEnv *env) : env(env) {
  method = env->get("REQUEST_METHOD", "");
  script_uri = env->get("SCRIPT_URI", cct->_conf->rgw_script_uri.c_str());
  request_uri = env->get("REQUEST_URI", cct->_conf->rgw_request_uri.c_str());
  if (request_uri[0] != '/') {
    request_uri = get_abs_path(request_uri);
  }
  auto pos = request_uri.find('?');
  if (pos != string::npos) {
    request_params = request_uri.substr(pos + 1);
    request_uri = request_uri.substr(0, pos);
  } else {
    request_params = env->get("QUERY_STRING", "");
  }
  host = env->get("HTTP_HOST", "");

  // strip off any trailing :port from host (added by CrossFTP and maybe others)
  size_t colon_offset = host.find_last_of(':');
  if (colon_offset != string::npos) {
    bool all_digits = true;
    for (unsigned i = colon_offset + 1; i < host.size(); ++i) {
      if (!isdigit(host[i])) {
	all_digits = false;
	break;
      }
    }
    if (all_digits) {
      host.resize(colon_offset);
    }
  }
}

void req_info::rebuild_from(req_info& src)
{
  method = src.method;
  script_uri = src.script_uri;
  args = src.args;
  if (src.effective_uri.empty()) {
    request_uri = src.request_uri;
  } else {
    request_uri = src.effective_uri;
  }
  effective_uri.clear();
  host = src.host;

  x_meta_map = src.x_meta_map;
  x_meta_map.erase("x-amz-date");
}


req_state::req_state(CephContext* _cct, RGWEnv* e, rgw::sal::RGWUser* u, uint64_t id)
  : cct(_cct), user(u),
    info(_cct, e), id(id)
{
  enable_ops_log = e->get_enable_ops_log();
  enable_usage_log = e->get_enable_usage_log();
  defer_to_bucket_acls = e->get_defer_to_bucket_acls();

  time = Clock::now();
}

req_state::~req_state() {
  delete formatter;
}

std::ostream& req_state::gen_prefix(std::ostream& out) const
{
  auto p = out.precision();
  return out << "req " << id << ' '
      << std::setprecision(3) << std::fixed << time_elapsed() // '0.123s'
      << std::setprecision(p) << std::defaultfloat << ' ';
}

bool search_err(rgw_http_errors& errs, int err_no, int& http_ret, string& code)
{
  auto r = errs.find(err_no);
  if (r != errs.end()) {
    http_ret = r->second.first;
    code = r->second.second;
    return true;
  }
  return false;
}

void set_req_state_err(struct rgw_err& err,	/* out */
			int err_no,		/* in  */
			const int prot_flags)	/* in  */
{
  if (err_no < 0)
    err_no = -err_no;

  err.ret = -err_no;

  if (prot_flags & RGW_REST_SWIFT) {
    if (search_err(rgw_http_swift_errors, err_no, err.http_ret, err.err_code))
      return;
  }

  if (prot_flags & RGW_REST_STS) {
    if (search_err(rgw_http_sts_errors, err_no, err.http_ret, err.err_code))
      return;
  }

  if (prot_flags & RGW_REST_IAM) {
    if (search_err(rgw_http_iam_errors, err_no, err.http_ret, err.err_code))
      return;
  }

  //Default to searching in s3 errors
  if (search_err(rgw_http_s3_errors, err_no, err.http_ret, err.err_code))
      return;
  dout(0) << "WARNING: set_req_state_err err_no=" << err_no
	<< " resorting to 500" << dendl;

  err.http_ret = 500;
  err.err_code = "UnknownError";
}

void set_req_state_err(struct req_state* s, int err_no, const string& err_msg)
{
  if (s) {
    set_req_state_err(s, err_no);
    if (s->prot_flags & RGW_REST_SWIFT && !err_msg.empty()) {
      /* TODO(rzarzynski): there never ever should be a check like this one.
       * It's here only for the sake of the patch's backportability. Further
       * commits will move the logic to a per-RGWHandler replacement of
       * the end_header() function. Alternativaly, we might consider making
       * that just for the dump(). Please take a look on @cbodley's comments
       * in PR #10690 (https://github.com/ceph/ceph/pull/10690). */
      s->err.err_code = err_msg;
    } else {
      s->err.message = err_msg;
    }
  }
}

void set_req_state_err(struct req_state* s, int err_no)
{
  if (s) {
    set_req_state_err(s->err, err_no, s->prot_flags);
  }
}

void dump(struct req_state* s)
{
  if (s->format != RGW_FORMAT_HTML)
    s->formatter->open_object_section("Error");
  if (!s->err.err_code.empty())
    s->formatter->dump_string("Code", s->err.err_code);
  if (!s->err.message.empty())
    s->formatter->dump_string("Message", s->err.message);
  if (!s->bucket_name.empty())	// TODO: connect to expose_bucket
    s->formatter->dump_string("BucketName", s->bucket_name);
  if (!s->trans_id.empty())	// TODO: connect to expose_bucket or another toggle
    s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->dump_string("HostId", s->host_id);
  if (s->format != RGW_FORMAT_HTML)
    s->formatter->close_section();
}

struct str_len {
  const char *str;
  int len;
};

#define STR_LEN_ENTRY(s) { s, sizeof(s) - 1 }

struct str_len meta_prefixes[] = { STR_LEN_ENTRY("HTTP_X_AMZ"),
                                   STR_LEN_ENTRY("HTTP_X_GOOG"),
                                   STR_LEN_ENTRY("HTTP_X_DHO"),
                                   STR_LEN_ENTRY("HTTP_X_RGW"),
                                   STR_LEN_ENTRY("HTTP_X_OBJECT"),
                                   STR_LEN_ENTRY("HTTP_X_CONTAINER"),
                                   STR_LEN_ENTRY("HTTP_X_ACCOUNT"),
                                   {NULL, 0} };

void req_info::init_meta_info(bool *found_bad_meta)
{
  x_meta_map.clear();

  for (const auto& kv: env->get_map()) {
    const char *prefix;
    const string& header_name = kv.first;
    const string& val = kv.second;
    for (int prefix_num = 0; (prefix = meta_prefixes[prefix_num].str) != NULL; prefix_num++) {
      int len = meta_prefixes[prefix_num].len;
      const char *p = header_name.c_str();
      if (strncmp(p, prefix, len) == 0) {
        dout(10) << "meta>> " << p << dendl;
        const char *name = p+len; /* skip the prefix */
        int name_len = header_name.size() - len;

        if (found_bad_meta && strncmp(name, "_META_", name_len) == 0)
          *found_bad_meta = true;

        char name_low[meta_prefixes[0].len + name_len + 1];
        snprintf(name_low, meta_prefixes[0].len - 5 + name_len + 1, "%s%s", meta_prefixes[0].str + 5 /* skip HTTP_ */, name); // normalize meta prefix
        int j;
        for (j = 0; name_low[j]; j++) {
          if (name_low[j] != '_')
            name_low[j] = tolower(name_low[j]);
          else
            name_low[j] = '-';
        }
        name_low[j] = 0;

        auto it = x_meta_map.find(name_low);
        if (it != x_meta_map.end()) {
          string old = it->second;
          boost::algorithm::trim_right(old);
          old.append(",");
          old.append(val);
          x_meta_map[name_low] = old;
        } else {
          x_meta_map[name_low] = val;
        }
      }
    }
  }
  for (const auto& kv: x_meta_map) {
    dout(10) << "x>> " << kv.first << ":" << rgw::crypt_sanitize::x_meta_map{kv.first, kv.second} << dendl;
  }
}

std::ostream& operator<<(std::ostream& oss, const rgw_err &err)
{
  oss << "rgw_err(http_ret=" << err.http_ret << ", err_code='" << err.err_code << "') ";
  return oss;
}

void rgw_add_amz_meta_header(
  meta_map_t& x_meta_map,
  const std::string& k,
  const std::string& v)
{
  auto it = x_meta_map.find(k);
  if (it != x_meta_map.end()) {
    std::string old = it->second;
    boost::algorithm::trim_right(old);
    old.append(",");
    old.append(v);
    x_meta_map[k] = old;
  } else {
    x_meta_map[k] = v;
  }
}

string rgw_string_unquote(const string& s)
{
  if (s[0] != '"' || s.size() < 2)
    return s;

  int len;
  for (len = s.size(); len > 2; --len) {
    if (s[len - 1] != ' ')
      break;
  }

  if (s[len-1] != '"')
    return s;

  return s.substr(1, len - 2);
}

static bool check_str_end(const char *s)
{
  if (!s)
    return false;

  while (*s) {
    if (!isspace(*s))
      return false;
    s++;
  }
  return true;
}

static bool check_gmt_end(const char *s)
{
  if (!s || !*s)
    return false;

  while (isspace(*s)) {
    ++s;
  }

  /* check for correct timezone */
  if ((strncmp(s, "GMT", 3) != 0) &&
      (strncmp(s, "UTC", 3) != 0)) {
    return false;
  }

  return true;
}

static bool parse_rfc850(const char *s, struct tm *t)
{
  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(t, 0, sizeof(*t));
  return check_gmt_end(strptime(s, "%A, %d-%b-%y %H:%M:%S ", t));
}

static bool parse_asctime(const char *s, struct tm *t)
{
  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(t, 0, sizeof(*t));
  return check_str_end(strptime(s, "%a %b %d %H:%M:%S %Y", t));
}

static bool parse_rfc1123(const char *s, struct tm *t)
{
  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(t, 0, sizeof(*t));
  return check_gmt_end(strptime(s, "%a, %d %b %Y %H:%M:%S ", t));
}

static bool parse_rfc1123_alt(const char *s, struct tm *t)
{
  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(t, 0, sizeof(*t));
  return check_str_end(strptime(s, "%a, %d %b %Y %H:%M:%S %z", t));
}

bool parse_rfc2616(const char *s, struct tm *t)
{
  return parse_rfc850(s, t) || parse_asctime(s, t) || parse_rfc1123(s, t) || parse_rfc1123_alt(s,t);
}

bool parse_iso8601(const char *s, struct tm *t, uint32_t *pns, bool extended_format)
{
  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(t, 0, sizeof(*t));
  const char *p;

  if (!s)
    s = "";

  if (extended_format) {
    p = strptime(s, "%Y-%m-%dT%T", t);
    if (!p) {
      p = strptime(s, "%Y-%m-%d %T", t);
    }
  } else {
    p = strptime(s, "%Y%m%dT%H%M%S", t);
  }
  if (!p) {
    dout(0) << "parse_iso8601 failed" << dendl;
    return false;
  }
  const std::string_view str = rgw_trim_whitespace(std::string_view(p));
  int len = str.size();

  if (len == 0 || (len == 1 && str[0] == 'Z'))
    return true;

  if (str[0] != '.' ||
      str[len - 1] != 'Z')
    return false;

  uint32_t ms;
  std::string_view nsstr = str.substr(1,  len - 2);
  int r = stringtoul(std::string(nsstr), &ms);
  if (r < 0)
    return false;

  if (!pns) {
    return true;
  }

  if (nsstr.size() > 9) {
    nsstr = nsstr.substr(0, 9);
  }

  uint64_t mul_table[] = { 0,
    100000000LL,
    10000000LL,
    1000000LL,
    100000LL,
    10000LL,
    1000LL,
    100LL,
    10LL,
    1 };


  *pns = ms * mul_table[nsstr.size()];

  return true;
}

int parse_key_value(string& in_str, const char *delim, string& key, string& val)
{
  if (delim == NULL)
    return -EINVAL;

  auto pos = in_str.find(delim);
  if (pos == string::npos)
    return -EINVAL;

  key = rgw_trim_whitespace(in_str.substr(0, pos));
  val = rgw_trim_whitespace(in_str.substr(pos + 1));

  return 0;
}

int parse_key_value(string& in_str, string& key, string& val)
{
  return parse_key_value(in_str, "=", key,val);
}

boost::optional<std::pair<std::string_view, std::string_view>>
parse_key_value(const std::string_view& in_str,
                const std::string_view& delim)
{
  const size_t pos = in_str.find(delim);
  if (pos == std::string_view::npos) {
    return boost::none;
  }

  const auto key = rgw_trim_whitespace(in_str.substr(0, pos));
  const auto val = rgw_trim_whitespace(in_str.substr(pos + 1));

  return std::make_pair(key, val);
}

boost::optional<std::pair<std::string_view, std::string_view>>
parse_key_value(const std::string_view& in_str)
{
  return parse_key_value(in_str, "=");
}

int parse_time(const char *time_str, real_time *time)
{
  struct tm tm;
  uint32_t ns = 0;

  if (!parse_rfc2616(time_str, &tm) && !parse_iso8601(time_str, &tm, &ns)) {
    return -EINVAL;
  }

  time_t sec = internal_timegm(&tm);
  *time = utime_t(sec, ns).to_real_time();

  return 0;
}

#define TIME_BUF_SIZE 128

void rgw_to_iso8601(const real_time& t, char *dest, int buf_size)
{
  utime_t ut(t);

  char buf[TIME_BUF_SIZE];
  struct tm result;
  time_t epoch = ut.sec();
  struct tm *tmp = gmtime_r(&epoch, &result);
  if (tmp == NULL)
    return;

  if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T", tmp) == 0)
    return;

  snprintf(dest, buf_size, "%s.%03dZ", buf, (int)(ut.usec() / 1000));
}

void rgw_to_iso8601(const real_time& t, string *dest)
{
  char buf[TIME_BUF_SIZE];
  rgw_to_iso8601(t, buf, sizeof(buf));
  *dest = buf;
}


string rgw_to_asctime(const utime_t& t)
{
  stringstream s;
  t.asctime(s);
  return s.str();
}

/*
 * calculate the sha1 value of a given msg and key
 */
void calc_hmac_sha1(const char *key, int key_len,
                    const char *msg, int msg_len, char *dest)
/* destination should be CEPH_CRYPTO_HMACSHA1_DIGESTSIZE bytes long */
{
  HMACSHA1 hmac((const unsigned char *)key, key_len);
  hmac.Update((const unsigned char *)msg, msg_len);
  hmac.Final((unsigned char *)dest);
}

/*
 * calculate the sha256 value of a given msg and key
 */
void calc_hmac_sha256(const char *key, int key_len,
                      const char *msg, int msg_len, char *dest)
{
  char hash_sha256[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];

  HMACSHA256 hmac((const unsigned char *)key, key_len);
  hmac.Update((const unsigned char *)msg, msg_len);
  hmac.Final((unsigned char *)hash_sha256);

  memcpy(dest, hash_sha256, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE);
}

using ceph::crypto::SHA256;

/*
 * calculate the sha256 hash value of a given msg
 */
sha256_digest_t calc_hash_sha256(const std::string_view& msg)
{
  sha256_digest_t hash;

  SHA256 hasher;
  hasher.Update(reinterpret_cast<const unsigned char*>(msg.data()), msg.size());
  hasher.Final(hash.v);

  return hash;
}

SHA256* calc_hash_sha256_open_stream()
{
  return new SHA256;
}

void calc_hash_sha256_update_stream(SHA256 *hash, const char *msg, int len)
{
  hash->Update((const unsigned char *)msg, len);
}

string calc_hash_sha256_close_stream(SHA256 **phash)
{
  SHA256 *hash = *phash;
  if (!hash) {
    hash = calc_hash_sha256_open_stream();
  }
  char hash_sha256[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];

  hash->Final((unsigned char *)hash_sha256);

  char hex_str[(CEPH_CRYPTO_SHA256_DIGESTSIZE * 2) + 1];
  buf_to_hex((unsigned char *)hash_sha256, CEPH_CRYPTO_SHA256_DIGESTSIZE, hex_str);

  delete hash;
  *phash = NULL;
  
  return std::string(hex_str);
}

std::string calc_hash_sha256_restart_stream(SHA256 **phash)
{
  const auto hash = calc_hash_sha256_close_stream(phash);
  *phash = calc_hash_sha256_open_stream();

  return hash;
}

int NameVal::parse()
{
  auto delim_pos = str.find('=');
  int ret = 0;

  if (delim_pos == string::npos) {
    name = str;
    val = "";
    ret = 1;
  } else {
    name = str.substr(0, delim_pos);
    val = str.substr(delim_pos + 1);
  }

  return ret; 
}

int RGWHTTPArgs::parse()
{
  int pos = 0;
  bool end = false;

  if (str.empty())
    return 0;

  if (str[pos] == '?')
    pos++;

  while (!end) {
    int fpos = str.find('&', pos);
    if (fpos  < pos) {
       end = true;
       fpos = str.size(); 
    }
    std::string nameval = url_decode(str.substr(pos, fpos - pos), true);
    NameVal nv(std::move(nameval));
    int ret = nv.parse();
    if (ret >= 0) {
      string& name = nv.get_name();
      string& val = nv.get_val();

      append(name, val);
    }

    pos = fpos + 1;  
  }

  return 0;
}

void RGWHTTPArgs::append(const string& name, const string& val)
{
  if (name.compare(0, sizeof(RGW_SYS_PARAM_PREFIX) - 1, RGW_SYS_PARAM_PREFIX) == 0) {
    sys_val_map[name] = val;
  } else {
    val_map[name] = val;
  }

  if ((name.compare("acl") == 0) ||
      (name.compare("cors") == 0) ||
      (name.compare("notification") == 0) ||
      (name.compare("location") == 0) ||
      (name.compare("logging") == 0) ||
      (name.compare("usage") == 0) ||
      (name.compare("lifecycle") == 0) ||
      (name.compare("delete") == 0) ||
      (name.compare("uploads") == 0) ||
      (name.compare("partNumber") == 0) ||
      (name.compare("uploadId") == 0) ||
      (name.compare("versionId") == 0) ||
      (name.compare("start-date") == 0) ||
      (name.compare("end-date") == 0) ||
      (name.compare("versions") == 0) ||
      (name.compare("versioning") == 0) ||
      (name.compare("website") == 0) ||
      (name.compare("requestPayment") == 0) ||
      (name.compare("torrent") == 0) ||
      (name.compare("tagging") == 0) ||
      (name.compare("append") == 0) ||
      (name.compare("position") == 0) ||
      (name.compare("policyStatus") == 0) ||
      (name.compare("publicAccessBlock") == 0)) {
    sub_resources[name] = val;
  } else if (name[0] == 'r') { // root of all evil
    if ((name.compare("response-content-type") == 0) ||
        (name.compare("response-content-language") == 0) ||
        (name.compare("response-expires") == 0) ||
        (name.compare("response-cache-control") == 0) ||
        (name.compare("response-content-disposition") == 0) ||
        (name.compare("response-content-encoding") == 0)) {
      sub_resources[name] = val;
      has_resp_modifier = true;
    }
  } else if  ((name.compare("subuser") == 0) ||
              (name.compare("key") == 0) ||
              (name.compare("caps") == 0) ||
              (name.compare("index") == 0) ||
              (name.compare("policy") == 0) ||
              (name.compare("quota") == 0) ||
              (name.compare("list") == 0) ||
              (name.compare("object") == 0) ||
              (name.compare("sync") == 0)) {
    if (!admin_subresource_added) {
      sub_resources[name] = "";
      admin_subresource_added = true;
    }
  }
}

const string& RGWHTTPArgs::get(const string& name, bool *exists) const
{
  auto iter = val_map.find(name);
  bool e = (iter != std::end(val_map));
  if (exists)
    *exists = e;
  if (e)
    return iter->second;
  return empty_str;
}

boost::optional<const std::string&>
RGWHTTPArgs::get_optional(const std::string& name) const
{
  bool exists;
  const std::string& value = get(name, &exists);
  if (exists) {
    return value;
  } else {
    return boost::none;
  }
}

int RGWHTTPArgs::get_bool(const string& name, bool *val, bool *exists)
{
  map<string, string>::iterator iter;
  iter = val_map.find(name);
  bool e = (iter != val_map.end());
  if (exists)
    *exists = e;

  if (e) {
    const char *s = iter->second.c_str();

    if (strcasecmp(s, "false") == 0) {
      *val = false;
    } else if (strcasecmp(s, "true") == 0) {
      *val = true;
    } else {
      return -EINVAL;
    }
  }

  return 0;
}

int RGWHTTPArgs::get_bool(const char *name, bool *val, bool *exists)
{
  string s(name);
  return get_bool(s, val, exists);
}

void RGWHTTPArgs::get_bool(const char *name, bool *val, bool def_val)
{
  bool exists = false;
  if ((get_bool(name, val, &exists) < 0) ||
      !exists) {
    *val = def_val;
  }
}

int RGWHTTPArgs::get_int(const char *name, int *val, int def_val)
{
  bool exists = false;
  string val_str;
  val_str = get(name, &exists);
  if (!exists) {
    *val = def_val;
    return 0;
  }

  string err;

  *val = (int)strict_strtol(val_str.c_str(), 10, &err);
  if (!err.empty()) {
    *val = def_val;
    return -EINVAL;
  }
  return 0;
}

string RGWHTTPArgs::sys_get(const string& name, bool * const exists) const
{
  const auto iter = sys_val_map.find(name);
  const bool e = (iter != sys_val_map.end());

  if (exists) {
    *exists = e;
  }

  return e ? iter->second : string();
}

bool rgw_transport_is_secure(CephContext *cct, const RGWEnv& env)
{
  const auto& m = env.get_map();
  // frontend connected with ssl
  if (m.count("SERVER_PORT_SECURE")) {
    return true;
  }
  // ignore proxy headers unless explicitly enabled
  if (!cct->_conf->rgw_trust_forwarded_https) {
    return false;
  }
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Forwarded
  // Forwarded: by=<identifier>; for=<identifier>; host=<host>; proto=<http|https>
  auto i = m.find("HTTP_FORWARDED");
  if (i != m.end() && i->second.find("proto=https") != std::string::npos) {
    return true;
  }
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-Proto
  i = m.find("HTTP_X_FORWARDED_PROTO");
  if (i != m.end() && i->second == "https") {
    return true;
  }
  return false;
}


namespace {

struct perm_state_from_req_state : public perm_state_base {
  req_state * const s;
  perm_state_from_req_state(req_state * const _s) : perm_state_base(_s->cct,
                                                                    _s->env,
                                                                    _s->auth.identity.get(),
                                                                    _s->bucket_info,
                                                                    _s->perm_mask,
                                                                    _s->defer_to_bucket_acls,
                                                                    _s->bucket_access_conf),
                                                                    s(_s) {}
  std::optional<bool> get_request_payer() const override {
    const char *request_payer = s->info.env->get("HTTP_X_AMZ_REQUEST_PAYER");
    if (!request_payer) {
      bool exists;
      request_payer = s->info.args.get("x-amz-request-payer", &exists).c_str();
      if (!exists) {
        return false;
      }
    }

    if (strcasecmp(request_payer, "requester") == 0) {
      return true;
    }

    return std::nullopt;
  }

  const char *get_referer() const override {
    return s->info.env->get("HTTP_REFERER");
  }
};

Effect eval_or_pass(const boost::optional<Policy>& policy,
		    const rgw::IAM::Environment& env,
		    boost::optional<const rgw::auth::Identity&> id,
		    const uint64_t op,
		    const ARN& arn) {
  if (!policy)
    return Effect::Pass;
  else
    return policy->eval(env, id, op, arn);
}

}

Effect eval_user_policies(const vector<Policy>& user_policies,
                          const rgw::IAM::Environment& env,
                          boost::optional<const rgw::auth::Identity&> id,
                          const uint64_t op,
                          const ARN& arn) {
  auto usr_policy_res = Effect::Pass, prev_res = Effect::Pass;
  for (auto& user_policy : user_policies) {
    if (usr_policy_res = eval_or_pass(user_policy, env, id, op, arn); usr_policy_res == Effect::Deny)
      return usr_policy_res;
    else if (usr_policy_res == Effect::Allow)
      prev_res = Effect::Allow;
    else if (usr_policy_res == Effect::Pass && prev_res == Effect::Allow)
      usr_policy_res = Effect::Allow;
  }
  return usr_policy_res;
}

bool verify_user_permission(const DoutPrefixProvider* dpp,
                            perm_state_base * const s,
                            RGWAccessControlPolicy * const user_acl,
                            const vector<rgw::IAM::Policy>& user_policies,
                            const rgw::ARN& res,
                            const uint64_t op)
{
  auto usr_policy_res = eval_user_policies(user_policies, s->env, boost::none, op, res);
  if (usr_policy_res == Effect::Deny) {
    return false;
  }

  if (usr_policy_res == Effect::Allow) {
    return true;
  }

  if (op == rgw::IAM::s3CreateBucket || op == rgw::IAM::s3ListAllMyBuckets) {
    auto perm = op_to_perm(op);

    return verify_user_permission_no_policy(dpp, s, user_acl, perm);
  }

  return false;
}

bool verify_user_permission_no_policy(const DoutPrefixProvider* dpp,
                                      struct perm_state_base * const s,
                                      RGWAccessControlPolicy * const user_acl,
                                      const int perm)
{
  if (s->identity->get_identity_type() == TYPE_ROLE)
    return false;

  /* S3 doesn't support account ACLs. */
  if (!user_acl)
    return true;

  if ((perm & (int)s->perm_mask) != perm)
    return false;

  return user_acl->verify_permission(dpp, *s->identity, perm, perm);
}

bool verify_user_permission(const DoutPrefixProvider* dpp,
                            struct req_state * const s,
                            const rgw::ARN& res,
                            const uint64_t op)
{
  perm_state_from_req_state ps(s);
  return verify_user_permission(dpp, &ps, s->user_acl.get(), s->iam_user_policies, res, op);
}

bool verify_user_permission_no_policy(const DoutPrefixProvider* dpp, 
                                      struct req_state * const s,
                                      const int perm)
{
  perm_state_from_req_state ps(s);
  return verify_user_permission_no_policy(dpp, &ps, s->user_acl.get(), perm);
}

bool verify_requester_payer_permission(struct perm_state_base *s)
{
  if (!s->bucket_info.requester_pays)
    return true;

  if (s->identity->is_owner_of(s->bucket_info.owner))
    return true;
  
  if (s->identity->is_anonymous()) {
    return false;
  }

  auto request_payer = s->get_request_payer();
  if (request_payer) {
    return *request_payer;
  }

  return false;
}

bool verify_bucket_permission(const DoutPrefixProvider* dpp,
                              struct perm_state_base * const s,
			      const rgw_bucket& bucket,
                              RGWAccessControlPolicy * const user_acl,
                              RGWAccessControlPolicy * const bucket_acl,
			      const boost::optional<Policy>& bucket_policy,
                              const vector<Policy>& user_policies,
                              const uint64_t op)
{
  if (!verify_requester_payer_permission(s))
    return false;

  auto usr_policy_res = eval_user_policies(user_policies, s->env, boost::none, op, ARN(bucket));
  if (usr_policy_res == Effect::Deny)
    return false;

  auto r = eval_or_pass(bucket_policy, s->env, *s->identity,
			op, ARN(bucket));
  if (r == Effect::Allow)
    // It looks like S3 ACLs only GRANT permissions rather than
    // denying them, so this should be safe.
    return true;
  else if (r == Effect::Deny)
    return false;
  else if (usr_policy_res == Effect::Allow) // r is Effect::Pass at this point
    return true;

  const auto perm = op_to_perm(op);

  return verify_bucket_permission_no_policy(dpp, s, user_acl, bucket_acl, perm);
}

bool verify_bucket_permission(const DoutPrefixProvider* dpp,
                              struct req_state * const s,
			      const rgw_bucket& bucket,
                              RGWAccessControlPolicy * const user_acl,
                              RGWAccessControlPolicy * const bucket_acl,
			      const boost::optional<Policy>& bucket_policy,
                              const vector<Policy>& user_policies,
                              const uint64_t op)
{
  perm_state_from_req_state ps(s);
  return verify_bucket_permission(dpp, &ps, bucket,
                                  user_acl, bucket_acl,
                                  bucket_policy, user_policies,
                                  op);
}

bool verify_bucket_permission_no_policy(const DoutPrefixProvider* dpp, struct perm_state_base * const s,
					RGWAccessControlPolicy * const user_acl,
					RGWAccessControlPolicy * const bucket_acl,
					const int perm)
{
  if (!bucket_acl)
    return false;

  if ((perm & (int)s->perm_mask) != perm)
    return false;

  if (bucket_acl->verify_permission(dpp, *s->identity, perm, perm,
                                    s->get_referer(),
                                    s->bucket_access_conf &&
                                    s->bucket_access_conf->ignore_public_acls()))
    return true;

  if (!user_acl)
    return false;

  return user_acl->verify_permission(dpp, *s->identity, perm, perm);
}

bool verify_bucket_permission_no_policy(const DoutPrefixProvider* dpp, struct req_state * const s,
					RGWAccessControlPolicy * const user_acl,
					RGWAccessControlPolicy * const bucket_acl,
					const int perm)
{
  perm_state_from_req_state ps(s);
  return verify_bucket_permission_no_policy(dpp,
                                            &ps,
                                            user_acl,
                                            bucket_acl,
                                            perm);
}

bool verify_bucket_permission_no_policy(const DoutPrefixProvider* dpp, struct req_state * const s, const int perm)
{
  perm_state_from_req_state ps(s);

  if (!verify_requester_payer_permission(&ps))
    return false;

  return verify_bucket_permission_no_policy(dpp,
                                            &ps,
                                            s->user_acl.get(),
                                            s->bucket_acl.get(),
                                            perm);
}

bool verify_bucket_permission(const DoutPrefixProvider* dpp, struct req_state * const s, const uint64_t op)
{
  perm_state_from_req_state ps(s);

  return verify_bucket_permission(dpp, 
                                  &ps,
                                  s->bucket,
                                  s->user_acl.get(),
                                  s->bucket_acl.get(),
                                  s->iam_policy,
                                  s->iam_user_policies,
                                  op);
}

// Authorize anyone permitted by the policy and the bucket owner
// unless explicitly denied by the policy.

int verify_bucket_owner_or_policy(struct req_state* const s,
				  const uint64_t op)
{
  auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env, boost::none, op, ARN(s->bucket));
  if (usr_policy_res == Effect::Deny) {
    return -EACCES;
  }

  auto e = eval_or_pass(s->iam_policy,
			s->env, *s->auth.identity,
			op, ARN(s->bucket));
  if (e == Effect::Deny) {
    return -EACCES;
  }

  if (e == Effect::Allow ||
      usr_policy_res == Effect::Allow ||
      (e == Effect::Pass &&
       usr_policy_res == Effect::Pass &&
       s->auth.identity->is_owner_of(s->bucket_owner.get_id()))) {
    return 0;
  } else {
    return -EACCES;
  }
}


static inline bool check_deferred_bucket_perms(const DoutPrefixProvider* dpp,
                                               struct perm_state_base * const s,
					       const rgw_bucket& bucket,
					       RGWAccessControlPolicy * const user_acl,
					       RGWAccessControlPolicy * const bucket_acl,
					       const boost::optional<Policy>& bucket_policy,
                 const vector<Policy>& user_policies,
					       const uint8_t deferred_check,
					       const uint64_t op)
{
  return (s->defer_to_bucket_acls == deferred_check \
	  && verify_bucket_permission(dpp, s, bucket, user_acl, bucket_acl, bucket_policy, user_policies,op));
}

static inline bool check_deferred_bucket_only_acl(const DoutPrefixProvider* dpp,
                                                  struct perm_state_base * const s,
						  RGWAccessControlPolicy * const user_acl,
						  RGWAccessControlPolicy * const bucket_acl,
						  const uint8_t deferred_check,
						  const int perm)
{
  return (s->defer_to_bucket_acls == deferred_check \
	  && verify_bucket_permission_no_policy(dpp, s, user_acl, bucket_acl, perm));
}

bool verify_object_permission(const DoutPrefixProvider* dpp, struct perm_state_base * const s,
			      const rgw_obj& obj,
                              RGWAccessControlPolicy * const user_acl,
                              RGWAccessControlPolicy * const bucket_acl,
                              RGWAccessControlPolicy * const object_acl,
                              const boost::optional<Policy>& bucket_policy,
                              const vector<Policy>& user_policies,
                              const uint64_t op)
{
  if (!verify_requester_payer_permission(s))
    return false;

  auto usr_policy_res = eval_user_policies(user_policies, s->env, boost::none, op, ARN(obj));
  if (usr_policy_res == Effect::Deny)
    return false;

  auto r = eval_or_pass(bucket_policy, s->env, *s->identity, op, ARN(obj));
  if (r == Effect::Allow)
    // It looks like S3 ACLs only GRANT permissions rather than
    // denying them, so this should be safe.
    return true;
  else if (r == Effect::Deny)
    return false;
  else if (usr_policy_res == Effect::Allow)
    return true;

  const auto perm = op_to_perm(op);

  if (check_deferred_bucket_perms(dpp, s, obj.bucket, user_acl, bucket_acl, bucket_policy,
				  user_policies, RGW_DEFER_TO_BUCKET_ACLS_RECURSE, op) ||
      check_deferred_bucket_perms(dpp, s, obj.bucket, user_acl, bucket_acl, bucket_policy,
				  user_policies, RGW_DEFER_TO_BUCKET_ACLS_FULL_CONTROL, rgw::IAM::s3All)) {
    return true;
  }

  if (!object_acl) {
    return false;
  }

  bool ret = object_acl->verify_permission(dpp, *s->identity, s->perm_mask, perm,
					   nullptr, /* http_referrer */
					   s->bucket_access_conf &&
					   s->bucket_access_conf->ignore_public_acls());
  if (ret) {
    return true;
  }

  if (!s->cct->_conf->rgw_enforce_swift_acls)
    return ret;

  if ((perm & (int)s->perm_mask) != perm)
    return false;

  int swift_perm = 0;
  if (perm & (RGW_PERM_READ | RGW_PERM_READ_ACP))
    swift_perm |= RGW_PERM_READ_OBJS;
  if (perm & RGW_PERM_WRITE)
    swift_perm |= RGW_PERM_WRITE_OBJS;

  if (!swift_perm)
    return false;

  /* we already verified the user mask above, so we pass swift_perm as the mask here,
     otherwise the mask might not cover the swift permissions bits */
  if (bucket_acl->verify_permission(dpp, *s->identity, swift_perm, swift_perm,
                                    s->get_referer()))
    return true;

  if (!user_acl)
    return false;

  return user_acl->verify_permission(dpp, *s->identity, swift_perm, swift_perm);
}

bool verify_object_permission(const DoutPrefixProvider* dpp, struct req_state * const s,
			      const rgw_obj& obj,
                              RGWAccessControlPolicy * const user_acl,
                              RGWAccessControlPolicy * const bucket_acl,
                              RGWAccessControlPolicy * const object_acl,
                              const boost::optional<Policy>& bucket_policy,
                              const vector<Policy>& user_policies,
                              const uint64_t op)
{
  perm_state_from_req_state ps(s);
  return verify_object_permission(dpp, &ps, obj,
                                  user_acl, bucket_acl,
                                  object_acl, bucket_policy,
                                  user_policies, op);
}

bool verify_object_permission_no_policy(const DoutPrefixProvider* dpp,
                                        struct perm_state_base * const s,
					RGWAccessControlPolicy * const user_acl,
					RGWAccessControlPolicy * const bucket_acl,
					RGWAccessControlPolicy * const object_acl,
					const int perm)
{
  if (check_deferred_bucket_only_acl(dpp, s, user_acl, bucket_acl, RGW_DEFER_TO_BUCKET_ACLS_RECURSE, perm) ||
      check_deferred_bucket_only_acl(dpp, s, user_acl, bucket_acl, RGW_DEFER_TO_BUCKET_ACLS_FULL_CONTROL, RGW_PERM_FULL_CONTROL)) {
    return true;
  }

  if (!object_acl) {
    return false;
  }

  bool ret = object_acl->verify_permission(dpp, *s->identity, s->perm_mask, perm,
					   nullptr, /* http referrer */
					   s->bucket_access_conf &&
					   s->bucket_access_conf->ignore_public_acls());
  if (ret) {
    return true;
  }

  if (!s->cct->_conf->rgw_enforce_swift_acls)
    return ret;

  if ((perm & (int)s->perm_mask) != perm)
    return false;

  int swift_perm = 0;
  if (perm & (RGW_PERM_READ | RGW_PERM_READ_ACP))
    swift_perm |= RGW_PERM_READ_OBJS;
  if (perm & RGW_PERM_WRITE)
    swift_perm |= RGW_PERM_WRITE_OBJS;

  if (!swift_perm)
    return false;

  /* we already verified the user mask above, so we pass swift_perm as the mask here,
     otherwise the mask might not cover the swift permissions bits */
  if (bucket_acl->verify_permission(dpp, *s->identity, swift_perm, swift_perm,
                                    s->get_referer()))
    return true;

  if (!user_acl)
    return false;

  return user_acl->verify_permission(dpp, *s->identity, swift_perm, swift_perm);
}

bool verify_object_permission_no_policy(const DoutPrefixProvider* dpp, struct req_state *s, int perm)
{
  perm_state_from_req_state ps(s);

  if (!verify_requester_payer_permission(&ps))
    return false;

  return verify_object_permission_no_policy(dpp,
                                            &ps,
                                            s->user_acl.get(),
                                            s->bucket_acl.get(),
                                            s->object_acl.get(),
                                            perm);
}

bool verify_object_permission(const DoutPrefixProvider* dpp, struct req_state *s, uint64_t op)
{
  perm_state_from_req_state ps(s);

  return verify_object_permission(dpp,
                                  &ps,
                                  rgw_obj(s->bucket, s->object),
                                  s->user_acl.get(),
                                  s->bucket_acl.get(),
                                  s->object_acl.get(),
                                  s->iam_policy,
                                  s->iam_user_policies,
                                  op);
}

class HexTable
{
  char table[256];

public:
  HexTable() {
    // FIPS zeroization audit 20191115: this memset is not security related.
    memset(table, -1, sizeof(table));
    int i;
    for (i = '0'; i<='9'; i++)
      table[i] = i - '0';
    for (i = 'A'; i<='F'; i++)
      table[i] = i - 'A' + 0xa;
    for (i = 'a'; i<='f'; i++)
      table[i] = i - 'a' + 0xa;
  }

  char to_num(char c) {
    return table[(int)c];
  }
};

static char hex_to_num(char c)
{
  static HexTable hex_table;
  return hex_table.to_num(c);
}

std::string url_decode(const std::string_view& src_str, bool in_query)
{
  std::string dest_str;
  dest_str.reserve(src_str.length() + 1);

  for (auto src = std::begin(src_str); src != std::end(src_str); ++src) {
    if (*src != '%') {
      if (!in_query || *src != '+') {
        if (*src == '?') {
          in_query = true;
        }
        dest_str.push_back(*src);
      } else {
        dest_str.push_back(' ');
      }
    } else {
      /* 3 == strlen("%%XX") */
      if (std::distance(src, std::end(src_str)) < 3) {
        break;
      }

      src++;
      const char c1 = hex_to_num(*src++);
      const char c2 = hex_to_num(*src);
      if (c1 < 0 || c2 < 0) {
        return std::string();
      } else {
        dest_str.push_back(c1 << 4 | c2);
      }
    }
  }

  return dest_str;
}

void rgw_uri_escape_char(char c, string& dst)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%%%.2X", (int)(unsigned char)c);
  dst.append(buf);
}

static bool char_needs_url_encoding(char c)
{
  if (c <= 0x20 || c >= 0x7f)
    return true;

  switch (c) {
    case 0x22:
    case 0x23:
    case 0x25:
    case 0x26:
    case 0x2B:
    case 0x2C:
    case 0x2F:
    case 0x3A:
    case 0x3B:
    case 0x3C:
    case 0x3E:
    case 0x3D:
    case 0x3F:
    case 0x40:
    case 0x5B:
    case 0x5D:
    case 0x5C:
    case 0x5E:
    case 0x60:
    case 0x7B:
    case 0x7D:
      return true;
  }
  return false;
}

void url_encode(const string& src, string& dst, bool encode_slash)
{
  const char *p = src.c_str();
  for (unsigned i = 0; i < src.size(); i++, p++) {
    if ((!encode_slash && *p == 0x2F) || !char_needs_url_encoding(*p)) {
      dst.append(p, 1);
    }else {
      rgw_uri_escape_char(*p, dst);
    }
  }
}

std::string url_encode(const std::string& src, bool encode_slash)
{
  std::string dst;
  url_encode(src, dst, encode_slash);

  return dst;
}

std::string url_remove_prefix(const std::string& url)
{
  std::string dst = url;
  auto pos = dst.find("http://");
  if (pos == std::string::npos) {
    pos = dst.find("https://");
    if (pos != std::string::npos) {
      dst.erase(pos, 8);
    } else {
      pos = dst.find("www.");
      if (pos != std::string::npos) {
        dst.erase(pos, 4);
      }
    }
  } else {
    dst.erase(pos, 7);
  }

  return dst;
}

string rgw_trim_whitespace(const string& src)
{
  if (src.empty()) {
    return string();
  }

  int start = 0;
  for (; start != (int)src.size(); start++) {
    if (!isspace(src[start]))
      break;
  }

  int end = src.size() - 1;
  if (end < start) {
    return string();
  }

  for (; end > start; end--) {
    if (!isspace(src[end]))
      break;
  }

  return src.substr(start, end - start + 1);
}

std::string_view rgw_trim_whitespace(const std::string_view& src)
{
  std::string_view res = src;

  while (res.size() > 0 && std::isspace(res.front())) {
    res.remove_prefix(1);
  }
  while (res.size() > 0 && std::isspace(res.back())) {
    res.remove_suffix(1);
  }
  return res;
}

string rgw_trim_quotes(const string& val)
{
  string s = rgw_trim_whitespace(val);
  if (s.size() < 2)
    return s;

  int start = 0;
  int end = s.size() - 1;
  int quotes_count = 0;

  if (s[start] == '"') {
    start++;
    quotes_count++;
  }
  if (s[end] == '"') {
    end--;
    quotes_count++;
  }
  if (quotes_count == 2) {
    return s.substr(start, end - start + 1);
  }
  return s;
}

static struct rgw_name_to_flag cap_names[] = { {"*",     RGW_CAP_ALL},
                  {"read",  RGW_CAP_READ},
		  {"write", RGW_CAP_WRITE},
		  {NULL, 0} };

int RGWUserCaps::parse_cap_perm(const string& str, uint32_t *perm)
{
  return rgw_parse_list_of_flags(cap_names, str, perm);
}

int RGWUserCaps::get_cap(const string& cap, string& type, uint32_t *pperm)
{
  int pos = cap.find('=');
  if (pos >= 0) {
    type = rgw_trim_whitespace(cap.substr(0, pos));
  }

  if (!is_valid_cap_type(type))
    return -ERR_INVALID_CAP;

  string cap_perm;
  uint32_t perm = 0;
  if (pos < (int)cap.size() - 1) {
    cap_perm = cap.substr(pos + 1);
    int r = RGWUserCaps::parse_cap_perm(cap_perm, &perm);
    if (r < 0)
      return r;
  }

  *pperm = perm;

  return 0;
}

int RGWUserCaps::add_cap(const string& cap)
{
  uint32_t perm;
  string type;

  int r = get_cap(cap, type, &perm);
  if (r < 0)
    return r;

  caps[type] |= perm;

  return 0;
}

int RGWUserCaps::remove_cap(const string& cap)
{
  uint32_t perm;
  string type;

  int r = get_cap(cap, type, &perm);
  if (r < 0)
    return r;

  map<string, uint32_t>::iterator iter = caps.find(type);
  if (iter == caps.end())
    return 0;

  uint32_t& old_perm = iter->second;
  old_perm &= ~perm;
  if (!old_perm)
    caps.erase(iter);

  return 0;
}

int RGWUserCaps::add_from_string(const string& str)
{
  int start = 0;
  do {
    auto end = str.find(';', start);
    if (end == string::npos)
      end = str.size();

    int r = add_cap(str.substr(start, end - start));
    if (r < 0)
      return r;

    start = end + 1;
  } while (start < (int)str.size());

  return 0;
}

int RGWUserCaps::remove_from_string(const string& str)
{
  int start = 0;
  do {
    auto end = str.find(';', start);
    if (end == string::npos)
      end = str.size();

    int r = remove_cap(str.substr(start, end - start));
    if (r < 0)
      return r;

    start = end + 1;
  } while (start < (int)str.size());

  return 0;
}

void RGWUserCaps::dump(Formatter *f) const
{
  dump(f, "caps");
}

void RGWUserCaps::dump(Formatter *f, const char *name) const
{
  f->open_array_section(name);
  map<string, uint32_t>::const_iterator iter;
  for (iter = caps.begin(); iter != caps.end(); ++iter)
  {
    f->open_object_section("cap");
    f->dump_string("type", iter->first);
    uint32_t perm = iter->second;
    string perm_str;
    for (int i=0; cap_names[i].type_name; i++) {
      if ((perm & cap_names[i].flag) == cap_names[i].flag) {
	if (perm_str.size())
	  perm_str.append(", ");

	perm_str.append(cap_names[i].type_name);
	perm &= ~cap_names[i].flag;
      }
    }
    if (perm_str.empty())
      perm_str = "<none>";

    f->dump_string("perm", perm_str);
    f->close_section();
  }

  f->close_section();
}

struct RGWUserCap {
  string type;
  uint32_t perm;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("type", type, obj);
    string perm_str;
    JSONDecoder::decode_json("perm", perm_str, obj);
    if (RGWUserCaps::parse_cap_perm(perm_str, &perm) < 0) {
      throw JSONDecoder::err("failed to parse permissions");
    }
  }
};

void RGWUserCaps::decode_json(JSONObj *obj)
{
  list<RGWUserCap> caps_list;
  decode_json_obj(caps_list, obj);

  list<RGWUserCap>::iterator iter;
  for (iter = caps_list.begin(); iter != caps_list.end(); ++iter) {
    RGWUserCap& cap = *iter;
    caps[cap.type] = cap.perm;
  }
}

int RGWUserCaps::check_cap(const string& cap, uint32_t perm) const
{
  auto iter = caps.find(cap);

  if ((iter == caps.end()) ||
      (iter->second & perm) != perm) {
    return -EPERM;
  }

  return 0;
}

bool RGWUserCaps::is_valid_cap_type(const string& tp)
{
  static const char *cap_type[] = { "user",
                                    "users",
                                    "buckets",
                                    "metadata",
                                    "usage",
                                    "zone",
                                    "bilog",
                                    "mdlog",
                                    "datalog",
                                    "roles",
                                    "user-policy",
                                    "amz-cache",
                                    "oidc-provider"};

  for (unsigned int i = 0; i < sizeof(cap_type) / sizeof(char *); ++i) {
    if (tp.compare(cap_type[i]) == 0) {
      return true;
    }
  }

  return false;
}

void rgw_pool::from_str(const string& s)
{
  size_t pos = rgw_unescape_str(s, 0, '\\', ':', &name);
  if (pos != string::npos) {
    pos = rgw_unescape_str(s, pos, '\\', ':', &ns);
    /* ignore return; if pos != string::npos it means that we had a colon
     * in the middle of ns that wasn't escaped, we're going to stop there
     */
  }
}

string rgw_pool::to_str() const
{
  string esc_name;
  rgw_escape_str(name, '\\', ':', &esc_name);
  if (ns.empty()) {
    return esc_name;
  }
  string esc_ns;
  rgw_escape_str(ns, '\\', ':', &esc_ns);
  return esc_name + ":" + esc_ns;
}

void rgw_raw_obj::decode_from_rgw_obj(bufferlist::const_iterator& bl)
{
  using ceph::decode;
  rgw_obj old_obj;
  decode(old_obj, bl);

  get_obj_bucket_and_oid_loc(old_obj, oid, loc);
  pool = old_obj.get_explicit_data_pool();
}

static struct rgw_name_to_flag op_type_mapping[] = { {"*",  RGW_OP_TYPE_ALL},
                  {"read",  RGW_OP_TYPE_READ},
		  {"write", RGW_OP_TYPE_WRITE},
		  {"delete", RGW_OP_TYPE_DELETE},
		  {NULL, 0} };


int rgw_parse_op_type_list(const string& str, uint32_t *perm)
{
  return rgw_parse_list_of_flags(op_type_mapping, str, perm);
}

bool match_policy(std::string_view pattern, std::string_view input,
                  uint32_t flag)
{
  const uint32_t flag2 = flag & (MATCH_POLICY_ACTION|MATCH_POLICY_ARN) ?
      MATCH_CASE_INSENSITIVE : 0;
  const bool colonblocks = !(flag & (MATCH_POLICY_RESOURCE |
				     MATCH_POLICY_STRING));

  const auto npos = std::string_view::npos;
  std::string_view::size_type last_pos_input = 0, last_pos_pattern = 0;
  while (true) {
    auto cur_pos_input = colonblocks ? input.find(":", last_pos_input) : npos;
    auto cur_pos_pattern =
      colonblocks ? pattern.find(":", last_pos_pattern) : npos;

    auto substr_input = input.substr(last_pos_input, cur_pos_input);
    auto substr_pattern = pattern.substr(last_pos_pattern, cur_pos_pattern);

    if (!match_wildcards(substr_pattern, substr_input, flag2))
      return false;

    if (cur_pos_pattern == npos)
      return cur_pos_input == npos;
    if (cur_pos_input == npos)
      return false;

    last_pos_pattern = cur_pos_pattern + 1;
    last_pos_input = cur_pos_input + 1;
  }
}

/*
 * make attrs look-like-this
 * converts underscores to dashes
 */
string lowercase_dash_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '_':
        buf[i] = '-';
        break;
      default:
        buf[i] = tolower(*s);
    }
  }
  return string(buf);
}

/*
 * make attrs Look-Like-This
 * converts underscores to dashes
 */
string camelcase_dash_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  bool last_sep = true;

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '_':
      case '-':
        buf[i] = '-';
        last_sep = true;
        break;
      default:
        if (last_sep) {
          buf[i] = toupper(*s);
        } else {
          buf[i] = tolower(*s);
        }
        last_sep = false;
    }
  }
  return string(buf);
}

RGWBucketInfo::RGWBucketInfo()
{
}

RGWBucketInfo::~RGWBucketInfo()
{
}

void RGWBucketInfo::encode(bufferlist& bl) const {
  ENCODE_START(22, 4, bl);
  encode(bucket, bl);
  encode(owner.id, bl);
  encode(flags, bl);
  encode(zonegroup, bl);
  uint64_t ct = real_clock::to_time_t(creation_time);
  encode(ct, bl);
  encode(placement_rule, bl);
  encode(has_instance_obj, bl);
  encode(quota, bl);
  encode(requester_pays, bl);
  encode(owner.tenant, bl);
  encode(has_website, bl);
  if (has_website) {
    encode(website_conf, bl);
  }
  encode(swift_versioning, bl);
  if (swift_versioning) {
    encode(swift_ver_location, bl);
  }
  encode(creation_time, bl);
  encode(mdsearch_config, bl);
  encode(reshard_status, bl);
  encode(new_bucket_instance_id, bl);
  if (obj_lock_enabled()) {
    encode(obj_lock, bl);
  }
  bool has_sync_policy = !empty_sync_policy();
  encode(has_sync_policy, bl);
  if (has_sync_policy) {
    encode(*sync_policy, bl);
  }
  encode(layout, bl);
  ENCODE_FINISH(bl);
}

void RGWBucketInfo::decode(bufferlist::const_iterator& bl) {
  DECODE_START_LEGACY_COMPAT_LEN_32(22, 4, 4, bl);
  decode(bucket, bl);
  if (struct_v >= 2) {
    string s;
    decode(s, bl);
    owner.from_str(s);
  }
  if (struct_v >= 3)
    decode(flags, bl);
  if (struct_v >= 5)
    decode(zonegroup, bl);
  if (struct_v >= 6) {
    uint64_t ct;
    decode(ct, bl);
    if (struct_v < 17)
      creation_time = ceph::real_clock::from_time_t((time_t)ct);
  }
  if (struct_v >= 7)
    decode(placement_rule, bl);
  if (struct_v >= 8)
    decode(has_instance_obj, bl);
  if (struct_v >= 9)
    decode(quota, bl);
  static constexpr uint8_t new_layout_v = 22;
  if (struct_v >= 10 && struct_v < new_layout_v)
    decode(layout.current_index.layout.normal.num_shards, bl);
  if (struct_v >= 11 && struct_v < new_layout_v)
    decode(layout.current_index.layout.normal.hash_type, bl);
  if (struct_v >= 12)
    decode(requester_pays, bl);
  if (struct_v >= 13)
    decode(owner.tenant, bl);
  if (struct_v >= 14) {
    decode(has_website, bl);
    if (has_website) {
      decode(website_conf, bl);
    } else {
      website_conf = RGWBucketWebsiteConf();
    }
  }
  if (struct_v >= 15 && struct_v < new_layout_v) {
    uint32_t it;
    decode(it, bl);
    layout.current_index.layout.type = (rgw::BucketIndexType)it;
  } else {
    layout.current_index.layout.type = rgw::BucketIndexType::Normal;
  }
  swift_versioning = false;
  swift_ver_location.clear();
  if (struct_v >= 16) {
    decode(swift_versioning, bl);
    if (swift_versioning) {
      decode(swift_ver_location, bl);
    }
  }
  if (struct_v >= 17) {
    decode(creation_time, bl);
  }
  if (struct_v >= 18) {
    decode(mdsearch_config, bl);
  }
  if (struct_v >= 19) {
    decode(reshard_status, bl);
    decode(new_bucket_instance_id, bl);
  }
  if (struct_v >= 20 && obj_lock_enabled()) {
    decode(obj_lock, bl);
  }
  if (struct_v >= 21) {
    decode(sync_policy, bl);
  }
  if (struct_v >= 22) {
    decode(layout, bl);
  }
  
  DECODE_FINISH(bl);
}

void RGWBucketInfo::set_sync_policy(rgw_sync_policy_info&& policy)
{
  sync_policy = std::move(policy);
}

bool RGWBucketInfo::empty_sync_policy() const
{
  if (!sync_policy) {
    return true;
  }

  return sync_policy->empty();
}

