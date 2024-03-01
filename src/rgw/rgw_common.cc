// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <vector>
#include <algorithm>
#include <string>
#include <boost/tokenizer.hpp>

#include "json_spirit/json_spirit.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"

#include "rgw_op.h"
#include "rgw_common.h"
#include "rgw_acl.h"
#include "rgw_string.h"
#include "rgw_http_errors.h"
#include "rgw_arn.h"
#include "rgw_data_sync.h"

#include "global/global_init.h"
#include "common/ceph_crypto.h"
#include "common/armor.h"
#include "common/errno.h"
#include "common/Clock.h"
#include "common/convenience.h"
#include "common/strtol.h"
#include "include/str_list.h"
#include "rgw_crypt_sanitize.h"
#include "rgw_bucket_sync.h"
#include "rgw_sync_policy.h"

#include "services/svc_zone.h"

#include <sstream>

#define dout_context g_ceph_context

static constexpr auto dout_subsys = ceph_subsys_rgw;

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
    { ERR_INVALID_OBJECT_STATE, {403, "InvalidObjectState"}},
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
    { ERR_NO_SUCH_TAG_SET, {404, "NoSuchTagSet"}},
    { ERR_NO_SUCH_BUCKET_ENCRYPTION_CONFIGURATION, {404, "ServerSideEncryptionConfigurationNotFoundError"}},
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

using namespace std;
using namespace ceph::crypto;

thread_local bool is_asio_thread = false;

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
  const static string ABS_PREFIXES[] = {"http://", "https://", "ws://", "wss://"};
  bool isAbs = false;
  for (int i = 0; i < 4; ++i) {
    if (boost::algorithm::starts_with(request_uri, ABS_PREFIXES[i])) {
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

void req_info::rebuild_from(const req_info& src)
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


req_state::req_state(CephContext* _cct, const RGWProcessEnv& penv,
                     RGWEnv* e, uint64_t id)
  : cct(_cct), penv(penv), info(_cct, e), id(id)
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
  std::ios oldState(nullptr);
  oldState.copyfmt(out);

  out << "req " << id << ' '
      << std::setprecision(3) << std::fixed << time_elapsed() // '0.123s'
      << ' ';

  out.copyfmt(oldState);
  return out;

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

void set_req_state_err(req_state* s, int err_no, const string& err_msg)
{
  if (s) {
    set_req_state_err(s, err_no);
    if (s->prot_flags & RGW_REST_SWIFT && !err_msg.empty()) {
      /* TODO(rzarzynski): there never ever should be a check like this one.
       * It's here only for the sake of the patch's backportability. Further
       * commits will move the logic to a per-RGWHandler replacement of
       * the end_header() function. Alternatively, we might consider making
       * that just for the dump(). Please take a look on @cbodley's comments
       * in PR #10690 (https://github.com/ceph/ceph/pull/10690). */
      s->err.err_code = err_msg;
    } else {
      s->err.message = err_msg;
    }
  }
}

void set_req_state_err(req_state* s, int err_no)
{
  if (s) {
    set_req_state_err(s->err, err_no, s->prot_flags);
  }
}

void dump(req_state* s)
{
  if (s->format != RGWFormat::HTML)
    s->formatter->open_object_section("Error");
  if (!s->err.err_code.empty())
    s->formatter->dump_string("Code", s->err.err_code);
  s->formatter->dump_string("Message", s->err.message);
  if (!s->bucket_name.empty())	// TODO: connect to expose_bucket
    s->formatter->dump_string("BucketName", s->bucket_name);
  if (!s->trans_id.empty())	// TODO: connect to expose_bucket or another toggle
    s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->dump_string("HostId", s->host_id);
  if (s->format != RGWFormat::HTML)
    s->formatter->close_section();
}

struct str_len {
  const char *str;
  int len;
};

#define STR_LEN_ENTRY(s) { s, sizeof(s) - 1 }

struct str_len meta_prefixes[] = { STR_LEN_ENTRY("HTTP_X_AMZ_"),
                                   STR_LEN_ENTRY("HTTP_X_GOOG_"),
                                   STR_LEN_ENTRY("HTTP_X_DHO_"),
                                   STR_LEN_ENTRY("HTTP_X_RGW_"),
                                   STR_LEN_ENTRY("HTTP_X_OBJECT_"),
                                   STR_LEN_ENTRY("HTTP_X_CONTAINER_"),
                                   STR_LEN_ENTRY("HTTP_X_ACCOUNT_"),
                                   {NULL, 0} };

void req_info::init_meta_info(const DoutPrefixProvider *dpp, bool *found_bad_meta, const int prot_flags)
{
  x_meta_map.clear();
  crypt_attribute_map.clear();

  for (const auto& kv: env->get_map()) {
    const char *prefix;
    const string& header_name = kv.first;
    const string& val = kv.second;
    for (int prefix_num = 0; (prefix = meta_prefixes[prefix_num].str) != NULL; prefix_num++) {
      int len = meta_prefixes[prefix_num].len;
      const char *p = header_name.c_str();
      if (strncmp(p, prefix, len) == 0) {
        ldpp_dout(dpp, 10) << "meta>> " << p << dendl;
        const char *name = p+len; /* skip the prefix */
        int name_len = header_name.size() - len;

        if (found_bad_meta && strncmp(name, "META_", name_len) == 0)
          *found_bad_meta = true;

        string name_low = lowercase_dash_http_attr(string(meta_prefixes[0].str + 5) + name,
                                                   !(prot_flags & RGW_REST_SWIFT));

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
        if (strncmp(name_low.c_str(), "x-amz-server-side-encryption", 20) == 0) {
          crypt_attribute_map[name_low] = val;
        }
      }
    }
  }
  for (const auto& kv: x_meta_map) {
    ldpp_dout(dpp, 10) << "x>> " << kv.first << ":" << rgw::crypt_sanitize::x_meta_map{kv.first, kv.second} << dendl;
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

bool rgw_set_amz_meta_header(
  meta_map_t& x_meta_map,
  const std::string& k,
  const std::string& v,
  rgw_set_action_if_set a)
{
  auto it { x_meta_map.find(k) };
  bool r { it != x_meta_map.end() };
  switch(a) {
  default:
    ceph_assert(a == 0);
  case DISCARD:
    break;
  case APPEND:
    if (r) {
	std::string old { it->second };
	boost::algorithm::trim_right(old);
	old.append(",");
	old.append(v);
	x_meta_map[k] = old;
	break;
    }
    /* fall through */
  case OVERWRITE:
    x_meta_map[k] = v;
  }
  return r;
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

int parse_key_value(const string& in_str, const char *delim, string& key, string& val)
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

int parse_key_value(const string& in_str, string& key, string& val)
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

int RGWHTTPArgs::parse(const DoutPrefixProvider *dpp)
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
      if (name.find("X-Amz-") != string::npos) {
        std::for_each(name.begin(),
          name.end(),
          [](char &c){
            if (c != '-') {
              c = ::tolower(static_cast<unsigned char>(c));
            }
        });
      }
      string& val = nv.get_val();
      ldpp_dout(dpp, 10) << "name: " << name << " val: " << val << dendl;
      append(name, val);
    }

    pos = fpos + 1;  
  }

  return 0;
}

void RGWHTTPArgs::remove(const string& name)
{
  auto val_iter = val_map.find(name);
  if (val_iter != std::end(val_map)) {
    val_map.erase(val_iter);
  }

  auto sys_val_iter = sys_val_map.find(name);
  if (sys_val_iter != std::end(sys_val_map)) {
    sys_val_map.erase(sys_val_iter);
  }

  auto subres_iter = sub_resources.find(name);
  if (subres_iter != std::end(sub_resources)) {
    sub_resources.erase(subres_iter);
  }
}

void RGWHTTPArgs::append(const string& name, const string& val)
{
  if (name.compare(0, sizeof(RGW_SYS_PARAM_PREFIX) - 1, RGW_SYS_PARAM_PREFIX) == 0) {
    sys_val_map[name] = val;
  } else {
    val_map[name] = val;
  }

// when sub_resources exclusive by object are added, please remember to update obj_sub_resource in RGWHTTPArgs::exist_obj_excl_sub_resource().
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

int RGWHTTPArgs::get_bool(const string& name, bool *val, bool *exists) const
{
  map<string, string>::const_iterator iter;
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

int RGWHTTPArgs::get_bool(const char *name, bool *val, bool *exists) const
{
  string s(name);
  return get_bool(s, val, exists);
}

void RGWHTTPArgs::get_bool(const char *name, bool *val, bool def_val) const
{
  bool exists = false;
  if ((get_bool(name, val, &exists) < 0) ||
      !exists) {
    *val = def_val;
  }
}

int RGWHTTPArgs::get_int(const char *name, int *val, int def_val) const
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
  perm_state_from_req_state(req_state * const _s)
    : perm_state_base(_s->cct,
		      _s->env,
		      _s->auth.identity.get(),
		      _s->bucket.get() ? _s->bucket->get_info() : RGWBucketInfo(),
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

Effect eval_or_pass(const DoutPrefixProvider* dpp,
		    const boost::optional<Policy>& policy,
		    const rgw::IAM::Environment& env,
		    boost::optional<const rgw::auth::Identity&> id,
		    const uint64_t op,
		    const ARN& resource,
				boost::optional<rgw::IAM::PolicyPrincipal&> princ_type=boost::none) {
  if (!policy)
    return Effect::Pass;
  else
    return policy->eval(env, id, op, resource, princ_type);
}

}

Effect eval_identity_or_session_policies(const DoutPrefixProvider* dpp,
			  const vector<Policy>& policies,
                          const rgw::IAM::Environment& env,
                          const uint64_t op,
                          const ARN& arn) {
  auto policy_res = Effect::Pass, prev_res = Effect::Pass;
  for (auto& policy : policies) {
    if (policy_res = eval_or_pass(dpp, policy, env, boost::none, op, arn); policy_res == Effect::Deny)
      return policy_res;
    else if (policy_res == Effect::Allow)
      prev_res = Effect::Allow;
    else if (policy_res == Effect::Pass && prev_res == Effect::Allow)
      policy_res = Effect::Allow;
  }
  return policy_res;
}

bool verify_user_permission(const DoutPrefixProvider* dpp,
                            perm_state_base * const s,
                            const RGWAccessControlPolicy& user_acl,
                            const vector<rgw::IAM::Policy>& user_policies,
                            const vector<rgw::IAM::Policy>& session_policies,
                            const rgw::ARN& res,
                            const uint64_t op,
                            bool mandatory_policy)
{
  auto identity_policy_res = eval_identity_or_session_policies(dpp, user_policies, s->env, op, res);
  if (identity_policy_res == Effect::Deny) {
    return false;
  }

  if (! session_policies.empty()) {
    auto session_policy_res = eval_identity_or_session_policies(dpp, session_policies, s->env, op, res);
    if (session_policy_res == Effect::Deny) {
      return false;
    }
    //Intersection of identity policies and session policies
    if (identity_policy_res == Effect::Allow && session_policy_res == Effect::Allow) {
      return true;
    }
    return false;
  }

  if (identity_policy_res == Effect::Allow) {
    return true;
  }

  if (mandatory_policy) {
    // no policies, and policy is mandatory
    ldpp_dout(dpp, 20) << "no policies for a policy mandatory op " << op << dendl;
    return false;
  }

  auto perm = op_to_perm(op);

  return verify_user_permission_no_policy(dpp, s, user_acl, perm);
}

bool verify_user_permission_no_policy(const DoutPrefixProvider* dpp,
                                      struct perm_state_base * const s,
                                      const RGWAccessControlPolicy& user_acl,
                                      const int perm)
{
  if (s->identity->get_identity_type() == TYPE_ROLE)
    return false;

  /* S3 doesn't support account ACLs, so user_acl will be uninitialized. */
  if (user_acl.get_owner().id.empty())
    return true;

  if ((perm & (int)s->perm_mask) != perm)
    return false;

  return user_acl.verify_permission(dpp, *s->identity, perm, perm);
}

bool verify_user_permission(const DoutPrefixProvider* dpp,
                            req_state * const s,
                            const rgw::ARN& res,
                            const uint64_t op,
                            bool mandatory_policy)
{
  perm_state_from_req_state ps(s);
  return verify_user_permission(dpp, &ps, s->user_acl, s->iam_user_policies, s->session_policies, res, op, mandatory_policy);
}

bool verify_user_permission_no_policy(const DoutPrefixProvider* dpp, 
                                      req_state * const s,
                                      const int perm)
{
  perm_state_from_req_state ps(s);
  return verify_user_permission_no_policy(dpp, &ps, s->user_acl, perm);
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
                              const RGWAccessControlPolicy& user_acl,
                              const RGWAccessControlPolicy& bucket_acl,
			      const boost::optional<Policy>& bucket_policy,
                              const vector<Policy>& identity_policies,
                              const vector<Policy>& session_policies,
                              const uint64_t op)
{
  if (!verify_requester_payer_permission(s))
    return false;

  auto identity_policy_res = eval_identity_or_session_policies(dpp, identity_policies, s->env, op, ARN(bucket));
  if (identity_policy_res == Effect::Deny)
    return false;

  rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
  if (bucket_policy) {
    ldpp_dout(dpp, 16) << __func__ << ": policy: " << bucket_policy.get()
		       << "resource: " << ARN(bucket) << dendl;
  }
  auto r = eval_or_pass(dpp, bucket_policy, s->env, *s->identity,
			op, ARN(bucket), princ_type);
  if (r == Effect::Deny)
    return false;

  //Take into account session policies, if the identity making a request is a role
  if (!session_policies.empty()) {
    auto session_policy_res = eval_identity_or_session_policies(dpp, session_policies, s->env, op, ARN(bucket));
    if (session_policy_res == Effect::Deny) {
        return false;
    }
    if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
      //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
      if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
          (session_policy_res == Effect::Allow && r == Effect::Allow))
        return true;
    } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
      //Intersection of session policy and identity policy plus bucket policy
      if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || r == Effect::Allow)
        return true;
    } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
      if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow)
        return true;
    }
    return false;
  }

  if (r == Effect::Allow || identity_policy_res == Effect::Allow)
    // It looks like S3 ACLs only GRANT permissions rather than
    // denying them, so this should be safe.
    return true;

  const auto perm = op_to_perm(op);

  return verify_bucket_permission_no_policy(dpp, s, user_acl, bucket_acl, perm);
}

bool verify_bucket_permission(const DoutPrefixProvider* dpp,
                              req_state * const s,
			      const rgw_bucket& bucket,
                              const RGWAccessControlPolicy& user_acl,
                              const RGWAccessControlPolicy& bucket_acl,
			      const boost::optional<Policy>& bucket_policy,
                              const vector<Policy>& user_policies,
                              const vector<Policy>& session_policies,
                              const uint64_t op)
{
  perm_state_from_req_state ps(s);
  return verify_bucket_permission(dpp, &ps, bucket,
                                  user_acl, bucket_acl,
                                  bucket_policy, user_policies,
                                  session_policies, op);
}

bool verify_bucket_permission_no_policy(const DoutPrefixProvider* dpp, struct perm_state_base * const s,
					const RGWAccessControlPolicy& user_acl,
					const RGWAccessControlPolicy& bucket_acl,
					const int perm)
{
  if ((perm & (int)s->perm_mask) != perm)
    return false;

  if (bucket_acl.verify_permission(dpp, *s->identity, perm, perm,
                                   s->get_referer(),
                                   s->bucket_access_conf &&
                                   s->bucket_access_conf->ignore_public_acls()))
    return true;

  return user_acl.verify_permission(dpp, *s->identity, perm, perm);
}

bool verify_bucket_permission_no_policy(const DoutPrefixProvider* dpp, req_state * const s,
					const RGWAccessControlPolicy& user_acl,
					const RGWAccessControlPolicy& bucket_acl,
					const int perm)
{
  perm_state_from_req_state ps(s);
  return verify_bucket_permission_no_policy(dpp,
                                            &ps,
                                            user_acl,
                                            bucket_acl,
                                            perm);
}

bool verify_bucket_permission_no_policy(const DoutPrefixProvider* dpp, req_state * const s, const int perm)
{
  perm_state_from_req_state ps(s);

  if (!verify_requester_payer_permission(&ps))
    return false;

  return verify_bucket_permission_no_policy(dpp,
                                            &ps,
                                            s->user_acl,
                                            s->bucket_acl,
                                            perm);
}

bool verify_bucket_permission(const DoutPrefixProvider* dpp, req_state * const s, const uint64_t op)
{
  if (rgw::sal::Bucket::empty(s->bucket)) {
    // request is missing a bucket name
    return false;
  }

  perm_state_from_req_state ps(s);

  return verify_bucket_permission(dpp, 
                                  &ps,
                                  s->bucket->get_key(),
                                  s->user_acl,
                                  s->bucket_acl,
                                  s->iam_policy,
                                  s->iam_user_policies,
                                  s->session_policies,
                                  op);
}

// Authorize anyone permitted by the bucket policy, identity policies, session policies and the bucket owner
// unless explicitly denied by the policy.

int verify_bucket_owner_or_policy(req_state* const s,
				  const uint64_t op)
{
  auto identity_policy_res = eval_identity_or_session_policies(s, s->iam_user_policies, s->env, op, ARN(s->bucket->get_key()));
  if (identity_policy_res == Effect::Deny) {
    return -EACCES;
  }

  rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
  auto e = eval_or_pass(s, s->iam_policy,
			s->env, *s->auth.identity,
			op, ARN(s->bucket->get_key()), princ_type);
  if (e == Effect::Deny) {
    return -EACCES;
  }

  if (!s->session_policies.empty()) {
    auto session_policy_res = eval_identity_or_session_policies(s, s->session_policies, s->env, op,
								ARN(s->bucket->get_key()));
    if (session_policy_res == Effect::Deny) {
        return -EACCES;
    }
    if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
      //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
      if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
          (session_policy_res == Effect::Allow && e == Effect::Allow))
        return 0;
    } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
      //Intersection of session policy and identity policy plus bucket policy
      if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || e == Effect::Allow)
        return 0;
    } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
      if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow)
        return 0;
    }
    return -EACCES;
  }

  if (e == Effect::Allow ||
      identity_policy_res == Effect::Allow ||
      (e == Effect::Pass &&
       identity_policy_res == Effect::Pass &&
       s->auth.identity->is_owner_of(s->bucket_owner.id))) {
    return 0;
  } else {
    return -EACCES;
  }
}


static inline bool check_deferred_bucket_perms(const DoutPrefixProvider* dpp,
                                               struct perm_state_base * const s,
					       const rgw_bucket& bucket,
					       const RGWAccessControlPolicy& user_acl,
					       const RGWAccessControlPolicy& bucket_acl,
					       const boost::optional<Policy>& bucket_policy,
                 const vector<Policy>& identity_policies,
                 const vector<Policy>& session_policies,
					       const uint8_t deferred_check,
					       const uint64_t op)
{
  return (s->defer_to_bucket_acls == deferred_check \
	  && verify_bucket_permission(dpp, s, bucket, user_acl, bucket_acl, bucket_policy, identity_policies, session_policies,op));
}

static inline bool check_deferred_bucket_only_acl(const DoutPrefixProvider* dpp,
                                                  struct perm_state_base * const s,
						  const RGWAccessControlPolicy& user_acl,
						  const RGWAccessControlPolicy& bucket_acl,
						  const uint8_t deferred_check,
						  const int perm)
{
  return (s->defer_to_bucket_acls == deferred_check \
	  && verify_bucket_permission_no_policy(dpp, s, user_acl, bucket_acl, perm));
}

bool verify_object_permission(const DoutPrefixProvider* dpp, struct perm_state_base * const s,
			      const rgw_obj& obj,
                              const RGWAccessControlPolicy& user_acl,
                              const RGWAccessControlPolicy& bucket_acl,
                              const RGWAccessControlPolicy& object_acl,
                              const boost::optional<Policy>& bucket_policy,
                              const vector<Policy>& identity_policies,
                              const vector<Policy>& session_policies,
                              const uint64_t op)
{
  if (!verify_requester_payer_permission(s))
    return false;

  auto identity_policy_res = eval_identity_or_session_policies(dpp, identity_policies, s->env, op, ARN(obj));
  if (identity_policy_res == Effect::Deny)
    return false;

  rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
  auto r = eval_or_pass(dpp, bucket_policy, s->env, *s->identity, op, ARN(obj), princ_type);
  if (r == Effect::Deny)
    return false;

  if (!session_policies.empty()) {
    auto session_policy_res = eval_identity_or_session_policies(dpp, session_policies, s->env, op, ARN(obj));
    if (session_policy_res == Effect::Deny) {
        return false;
    }
    if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
      //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
      if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
          (session_policy_res == Effect::Allow && r == Effect::Allow))
        return true;
    } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
      //Intersection of session policy and identity policy plus bucket policy
      if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || r == Effect::Allow)
        return true;
    } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
      if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow)
        return true;
    }
    return false;
  }

  if (r == Effect::Allow || identity_policy_res == Effect::Allow)
    // It looks like S3 ACLs only GRANT permissions rather than
    // denying them, so this should be safe.
    return true;

  const auto perm = op_to_perm(op);

  if (check_deferred_bucket_perms(dpp, s, obj.bucket, user_acl, bucket_acl, bucket_policy,
				  identity_policies, session_policies, RGW_DEFER_TO_BUCKET_ACLS_RECURSE, op) ||
      check_deferred_bucket_perms(dpp, s, obj.bucket, user_acl, bucket_acl, bucket_policy,
				  identity_policies, session_policies, RGW_DEFER_TO_BUCKET_ACLS_FULL_CONTROL, rgw::IAM::s3All)) {
    return true;
  }

  bool ret = object_acl.verify_permission(dpp, *s->identity, s->perm_mask, perm,
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
  if (bucket_acl.verify_permission(dpp, *s->identity, swift_perm, swift_perm,
                                   s->get_referer()))
    return true;

  return user_acl.verify_permission(dpp, *s->identity, swift_perm, swift_perm);
}

bool verify_object_permission(const DoutPrefixProvider* dpp, req_state * const s,
			      const rgw_obj& obj,
                              const RGWAccessControlPolicy& user_acl,
                              const RGWAccessControlPolicy& bucket_acl,
                              const RGWAccessControlPolicy& object_acl,
                              const boost::optional<Policy>& bucket_policy,
                              const vector<Policy>& identity_policies,
                              const vector<Policy>& session_policies,
                              const uint64_t op)
{
  perm_state_from_req_state ps(s);
  return verify_object_permission(dpp, &ps, obj,
                                  user_acl, bucket_acl,
                                  object_acl, bucket_policy,
                                  identity_policies, session_policies, op);
}

bool verify_object_permission_no_policy(const DoutPrefixProvider* dpp,
                                        struct perm_state_base * const s,
					const RGWAccessControlPolicy& user_acl,
					const RGWAccessControlPolicy& bucket_acl,
					const RGWAccessControlPolicy& object_acl,
					const int perm)
{
  if (check_deferred_bucket_only_acl(dpp, s, user_acl, bucket_acl, RGW_DEFER_TO_BUCKET_ACLS_RECURSE, perm) ||
      check_deferred_bucket_only_acl(dpp, s, user_acl, bucket_acl, RGW_DEFER_TO_BUCKET_ACLS_FULL_CONTROL, RGW_PERM_FULL_CONTROL)) {
    return true;
  }

  bool ret = object_acl.verify_permission(dpp, *s->identity, s->perm_mask, perm,
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
  if (bucket_acl.verify_permission(dpp, *s->identity, swift_perm, swift_perm,
                                   s->get_referer()))
    return true;

  return user_acl.verify_permission(dpp, *s->identity, swift_perm, swift_perm);
}

bool verify_object_permission_no_policy(const DoutPrefixProvider* dpp, req_state *s, int perm)
{
  perm_state_from_req_state ps(s);

  if (!verify_requester_payer_permission(&ps))
    return false;

  return verify_object_permission_no_policy(dpp,
                                            &ps,
                                            s->user_acl,
                                            s->bucket_acl,
                                            s->object_acl,
                                            perm);
}

bool verify_object_permission(const DoutPrefixProvider* dpp, req_state *s, uint64_t op)
{
  perm_state_from_req_state ps(s);

  return verify_object_permission(dpp,
                                  &ps,
                                  rgw_obj(s->bucket->get_key(), s->object->get_key()),
                                  s->user_acl,
                                  s->bucket_acl,
                                  s->object_acl,
                                  s->iam_policy,
                                  s->iam_user_policies,
                                  s->session_policies,
                                  op);
}


int verify_object_lock(const DoutPrefixProvider* dpp, const rgw::sal::Attrs& attrs, const bool bypass_perm, const bool bypass_governance_mode) {
  auto aiter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
  if (aiter != attrs.end()) {
    RGWObjectRetention obj_retention;
    try {
      decode(obj_retention, aiter->second);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode RGWObjectRetention" << dendl;
      return -EIO;
    }
    if (ceph::real_clock::to_time_t(obj_retention.get_retain_until_date()) > ceph_clock_now()) {
      if (obj_retention.get_mode().compare("GOVERNANCE") != 0 || !bypass_perm || !bypass_governance_mode) {
        return -EACCES;
      }
    }
  }
  aiter = attrs.find(RGW_ATTR_OBJECT_LEGAL_HOLD);
  if (aiter != attrs.end()) {
    RGWObjectLegalHold obj_legal_hold;
    try {
      decode(obj_legal_hold, aiter->second);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode RGWObjectLegalHold" << dendl;
      return -EIO;
    }
    if (obj_legal_hold.is_enabled()) {
      return -EACCES;
    }
  }
  
  return 0;
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

static int rgw_parse_list_of_flags(struct rgw_name_to_flag *mapping,
			    const string& str, uint32_t *perm)
{
  list<string> strs;
  get_str_list(str, strs);
  list<string>::iterator iter;
  uint32_t v = 0;
  for (iter = strs.begin(); iter != strs.end(); ++iter) {
    string& s = *iter;
    for (int i = 0; mapping[i].type_name; i++) {
      if (s.compare(mapping[i].type_name) == 0)
        v |= mapping[i].flag;
    }
  }

  *perm = v;
  return 0;
}

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

void RGWUserCaps::generate_test_instances(list<RGWUserCaps*>& o)
{
  o.push_back(new RGWUserCaps);
  RGWUserCaps *caps = new RGWUserCaps;
  caps->add_cap("read");
  caps->add_cap("write");
  o.push_back(caps);
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
                                    "info",
                                    "usage",
                                    "zone",
                                    "bilog",
                                    "mdlog",
                                    "datalog",
                                    "roles",
                                    "user-policy",
                                    "amz-cache",
                                    "oidc-provider",
                                    "user-info-without-keys",
				                            "ratelimit"};

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

void rgw_raw_obj::generate_test_instances(std::list<rgw_raw_obj*>& o)
{
  rgw_raw_obj *r = new rgw_raw_obj;
  r->oid = "foo";
  r->loc = "bar";
  r->pool.name = "baz";
  r->pool.ns = "ns";
  o.push_back(r);
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

bool match_policy(const std::string& pattern, const std::string& input,
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
string lowercase_dash_http_attr(const string& orig, bool bidirection)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '_':
        buf[i] = '-';
        break;
      case '-':
        if (bidirection)
          buf[i] = '_';
        else
          buf[i] = tolower(*s);
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
string camelcase_dash_http_attr(const string& orig, bool convert2dash)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  bool last_sep = true;

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '_':
      case '-':
        buf[i] = convert2dash ? '-' : *s;
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
  ENCODE_START(23, 4, bl);
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
  encode(owner.ns, bl);
  ENCODE_FINISH(bl);
}

void RGWBucketInfo::decode(bufferlist::const_iterator& bl) {
  DECODE_START_LEGACY_COMPAT_LEN_32(23, 4, 4, bl);
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
  if (struct_v >= 23) {
    decode(owner.ns, bl);
  }

  if (layout.logs.empty() &&
      layout.current_index.layout.type == rgw::BucketIndexType::Normal) {
    layout.logs.push_back(rgw::log_layout_from_index(0, layout.current_index));
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

struct rgw_pool;
struct rgw_placement_rule;
class RGWUserCaps;

void decode_json_obj(rgw_pool& pool, JSONObj *obj)
{
  string s;
  decode_json_obj(s, obj);
  pool = rgw_pool(s);
}

void encode_json(const char *name, const rgw_placement_rule& r, Formatter *f)
{
  encode_json(name, r.to_str(), f);
}

void encode_json(const char *name, const rgw_pool& pool, Formatter *f)
{
  f->dump_string(name, pool.to_str());
}

void encode_json(const char *name, const RGWUserCaps& val, Formatter *f)
{
  val.dump(f, name);
}

void RGWBucketEnt::generate_test_instances(list<RGWBucketEnt*>& o)
{
  RGWBucketEnt *e = new RGWBucketEnt;
  init_bucket(&e->bucket, "tenant", "bucket", "pool", ".index_pool", "marker", "10");
  e->size = 1024;
  e->size_rounded = 4096;
  e->count = 1;
  o.push_back(e);
  o.push_back(new RGWBucketEnt);
}

void RGWBucketEnt::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("size", size, f);
  encode_json("size_rounded", size_rounded, f);
  utime_t ut(creation_time);
  encode_json("mtime", ut, f); /* mtime / creation time discrepancy needed for backward compatibility */
  encode_json("count", count, f);
  encode_json("placement_rule", placement_rule.to_str(), f);
}

void rgw_obj::generate_test_instances(list<rgw_obj*>& o)
{
  rgw_bucket b;
  init_bucket(&b, "tenant", "bucket", "pool", ".index_pool", "marker", "10");
  rgw_obj *obj = new rgw_obj(b, "object");
  o.push_back(obj);
  o.push_back(new rgw_obj);
}

void rgw_bucket_placement::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("placement_rule", placement_rule, f);
}

void RGWBucketInfo::generate_test_instances(list<RGWBucketInfo*>& o)
{
  // Since things without a log will have one synthesized on decode,
  // ensure the things we attempt to encode will have one added so we
  // round-trip properly.
  auto gen_layout = [](rgw::BucketLayout& l) {
    l.current_index.gen = 0;
    l.current_index.layout.normal.hash_type = rgw::BucketHashType::Mod;
    l.current_index.layout.type = rgw::BucketIndexType::Normal;
    l.current_index.layout.normal.num_shards = 11;
    l.logs.push_back(log_layout_from_index(
                       l.current_index.gen,
                       l.current_index));
  };


  RGWBucketInfo *i = new RGWBucketInfo;
  init_bucket(&i->bucket, "tenant", "bucket", "pool", ".index_pool", "marker", "10");
  i->owner = "owner";
  i->flags = BUCKET_SUSPENDED;
  gen_layout(i->layout);
  o.push_back(i);
  i = new RGWBucketInfo;
  gen_layout(i->layout);
  o.push_back(i);
}

void RGWBucketInfo::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  utime_t ut(creation_time);
  encode_json("creation_time", ut, f);
  encode_json("owner", owner.to_str(), f);
  encode_json("flags", flags, f);
  encode_json("zonegroup", zonegroup, f);
  encode_json("placement_rule", placement_rule, f);
  encode_json("has_instance_obj", has_instance_obj, f);
  encode_json("quota", quota, f);
  encode_json("num_shards", layout.current_index.layout.normal.num_shards, f);
  encode_json("bi_shard_hash_type", (uint32_t)layout.current_index.layout.normal.hash_type, f);
  encode_json("requester_pays", requester_pays, f);
  encode_json("has_website", has_website, f);
  if (has_website) {
    encode_json("website_conf", website_conf, f);
  }
  encode_json("swift_versioning", swift_versioning, f);
  encode_json("swift_ver_location", swift_ver_location, f);
  encode_json("index_type", (uint32_t)layout.current_index.layout.type, f);
  encode_json("mdsearch_config", mdsearch_config, f);
  encode_json("reshard_status", (int)reshard_status, f);
  encode_json("new_bucket_instance_id", new_bucket_instance_id, f);
  if (!empty_sync_policy()) {
    encode_json("sync_policy", *sync_policy, f);
  }
}

void RGWBucketInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("bucket", bucket, obj);
  utime_t ut;
  JSONDecoder::decode_json("creation_time", ut, obj);
  creation_time = ut.to_real_time();
  JSONDecoder::decode_json("owner", owner, obj);
  JSONDecoder::decode_json("flags", flags, obj);
  JSONDecoder::decode_json("zonegroup", zonegroup, obj);
  /* backward compatability with region */
  if (zonegroup.empty()) {
    JSONDecoder::decode_json("region", zonegroup, obj);
  }
  string pr;
  JSONDecoder::decode_json("placement_rule", pr, obj);
  placement_rule.from_str(pr);
  JSONDecoder::decode_json("has_instance_obj", has_instance_obj, obj);
  JSONDecoder::decode_json("quota", quota, obj);
  JSONDecoder::decode_json("num_shards", layout.current_index.layout.normal.num_shards, obj);
  uint32_t hash_type;
  JSONDecoder::decode_json("bi_shard_hash_type", hash_type, obj);
  layout.current_index.layout.normal.hash_type = static_cast<rgw::BucketHashType>(hash_type);
  JSONDecoder::decode_json("requester_pays", requester_pays, obj);
  JSONDecoder::decode_json("has_website", has_website, obj);
  if (has_website) {
    JSONDecoder::decode_json("website_conf", website_conf, obj);
  }
  JSONDecoder::decode_json("swift_versioning", swift_versioning, obj);
  JSONDecoder::decode_json("swift_ver_location", swift_ver_location, obj);
  uint32_t it;
  JSONDecoder::decode_json("index_type", it, obj);
  layout.current_index.layout.type = (rgw::BucketIndexType)it;
  JSONDecoder::decode_json("mdsearch_config", mdsearch_config, obj);
  int rs;
  JSONDecoder::decode_json("reshard_status", rs, obj);
  reshard_status = (cls_rgw_reshard_status)rs;

  rgw_sync_policy_info sp;
  JSONDecoder::decode_json("sync_policy", sp, obj);
  if (!sp.empty()) {
    set_sync_policy(std::move(sp));
  }
}

void RGWUserInfo::generate_test_instances(list<RGWUserInfo*>& o)
{
  RGWUserInfo *i = new RGWUserInfo;
  i->user_id = "user_id";
  i->display_name =  "display_name";
  i->user_email = "user@email";
  RGWAccessKey k1, k2;
  k1.id = "id1";
  k1.key = "key1";
  k2.id = "id2";
  k2.subuser = "subuser";
  RGWSubUser u;
  u.name = "id2";
  u.perm_mask = 0x1;
  i->access_keys[k1.id] = k1;
  i->swift_keys[k2.id] = k2;
  i->subusers[u.name] = u;
  o.push_back(i);

  o.push_back(new RGWUserInfo);
}

static void user_info_dump_subuser(const char *name, const RGWSubUser& subuser, Formatter *f, void *parent)
{
  RGWUserInfo *info = static_cast<RGWUserInfo *>(parent);
  subuser.dump(f, info->user_id.to_str());
}

static void user_info_dump_key(const char *name, const RGWAccessKey& key, Formatter *f, void *parent)
{
  RGWUserInfo *info = static_cast<RGWUserInfo *>(parent);
  key.dump(f, info->user_id.to_str(), false);
}

static void user_info_dump_swift_key(const char *name, const RGWAccessKey& key, Formatter *f, void *parent)
{
  RGWUserInfo *info = static_cast<RGWUserInfo *>(parent);
  key.dump(f, info->user_id.to_str(), true);
}

static void decode_access_keys(map<string, RGWAccessKey>& m, JSONObj *o)
{
  RGWAccessKey k;
  k.decode_json(o);
  m[k.id] = k;
}

static void decode_swift_keys(map<string, RGWAccessKey>& m, JSONObj *o)
{
  RGWAccessKey k;
  k.decode_json(o, true);
  m[k.id] = k;
}

static void decode_subusers(map<string, RGWSubUser>& m, JSONObj *o)
{
  RGWSubUser u;
  u.decode_json(o);
  m[u.name] = u;
}


struct rgw_flags_desc {
  uint32_t mask;
  const char *str;
};

static struct rgw_flags_desc rgw_perms[] = {
 { RGW_PERM_FULL_CONTROL, "full-control" },
 { RGW_PERM_READ | RGW_PERM_WRITE, "read-write" },
 { RGW_PERM_READ, "read" },
 { RGW_PERM_WRITE, "write" },
 { RGW_PERM_READ_ACP, "read-acp" },
 { RGW_PERM_WRITE_ACP, "write-acp" },
 { 0, NULL }
};

void rgw_perm_to_str(uint32_t mask, char *buf, int len)
{
  const char *sep = "";
  int pos = 0;
  if (!mask) {
    snprintf(buf, len, "<none>");
    return;
  }
  while (mask) {
    uint32_t orig_mask = mask;
    for (int i = 0; rgw_perms[i].mask; i++) {
      struct rgw_flags_desc *desc = &rgw_perms[i];
      if ((mask & desc->mask) == desc->mask) {
        pos += snprintf(buf + pos, len - pos, "%s%s", sep, desc->str);
        if (pos == len)
          return;
        sep = ", ";
        mask &= ~desc->mask;
        if (!mask)
          return;
      }
    }
    if (mask == orig_mask) // no change
      break;
  }
}

uint32_t rgw_str_to_perm(const char *str)
{
  if (strcasecmp(str, "") == 0)
    return RGW_PERM_NONE;
  else if (strcasecmp(str, "read") == 0)
    return RGW_PERM_READ;
  else if (strcasecmp(str, "write") == 0)
    return RGW_PERM_WRITE;
  else if (strcasecmp(str, "readwrite") == 0)
    return RGW_PERM_READ | RGW_PERM_WRITE;
  else if (strcasecmp(str, "full") == 0)
    return RGW_PERM_FULL_CONTROL;

  return RGW_PERM_INVALID;
}

template <class T>
static void mask_to_str(T *mask_list, uint32_t mask, char *buf, int len)
{
  const char *sep = "";
  int pos = 0;
  if (!mask) {
    snprintf(buf, len, "<none>");
    return;
  }
  while (mask) {
    uint32_t orig_mask = mask;
    for (int i = 0; mask_list[i].mask; i++) {
      T *desc = &mask_list[i];
      if ((mask & desc->mask) == desc->mask) {
        pos += snprintf(buf + pos, len - pos, "%s%s", sep, desc->str);
        if (pos == len)
          return;
        sep = ", ";
        mask &= ~desc->mask;
        if (!mask)
          return;
      }
    }
    if (mask == orig_mask) // no change
      break;
  }
}

static void perm_to_str(uint32_t mask, char *buf, int len)
{
  return mask_to_str(rgw_perms, mask, buf, len);
}

static struct rgw_flags_desc op_type_flags[] = {
 { RGW_OP_TYPE_READ, "read" },
 { RGW_OP_TYPE_WRITE, "write" },
 { RGW_OP_TYPE_DELETE, "delete" },
 { 0, NULL }
};

void op_type_to_str(uint32_t mask, char *buf, int len)
{
  return mask_to_str(op_type_flags, mask, buf, len);
}

void RGWRateLimitInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("max_read_ops", max_read_ops, obj);
  JSONDecoder::decode_json("max_write_ops", max_write_ops, obj);
  JSONDecoder::decode_json("max_read_bytes", max_read_bytes, obj);
  JSONDecoder::decode_json("max_write_bytes", max_write_bytes, obj);
  JSONDecoder::decode_json("enabled", enabled, obj);
}

void RGWRateLimitInfo::dump(Formatter *f) const
{
  f->dump_int("max_read_ops", max_read_ops);
  f->dump_int("max_write_ops", max_write_ops);
  f->dump_int("max_read_bytes", max_read_bytes);
  f->dump_int("max_write_bytes", max_write_bytes);
  f->dump_bool("enabled", enabled);
}

void RGWUserInfo::dump(Formatter *f) const
{

  encode_json("user_id", user_id.to_str(), f);
  encode_json("display_name", display_name, f);
  encode_json("email", user_email, f);
  encode_json("suspended", (int)suspended, f);
  encode_json("max_buckets", (int)max_buckets, f);

  encode_json_map("subusers", NULL, "subuser", NULL, user_info_dump_subuser,(void *)this, subusers, f);
  encode_json_map("keys", NULL, "key", NULL, user_info_dump_key,(void *)this, access_keys, f);
  encode_json_map("swift_keys", NULL, "key", NULL, user_info_dump_swift_key,(void *)this, swift_keys, f);

  encode_json("caps", caps, f);

  char buf[256];
  op_type_to_str(op_mask, buf, sizeof(buf));
  encode_json("op_mask", (const char *)buf, f);

  if (system) { /* no need to show it for every user */
    encode_json("system", (bool)system, f);
  }
  if (admin) {
    encode_json("admin", (bool)admin, f);
  }
  encode_json("default_placement", default_placement.name, f);
  encode_json("default_storage_class", default_placement.storage_class, f);
  encode_json("placement_tags", placement_tags, f);
  encode_json("bucket_quota", quota.bucket_quota, f);
  encode_json("user_quota", quota.user_quota, f);
  encode_json("temp_url_keys", temp_url_keys, f);

  string user_source_type;
  switch ((RGWIdentityType)type) {
  case TYPE_RGW:
    user_source_type = "rgw";
    break;
  case TYPE_KEYSTONE:
    user_source_type = "keystone";
    break;
  case TYPE_LDAP:
    user_source_type = "ldap";
    break;
  case TYPE_NONE:
    user_source_type = "none";
    break;
  default:
    user_source_type = "none";
    break;
  }
  encode_json("type", user_source_type, f);
  encode_json("mfa_ids", mfa_ids, f);
}

void RGWUserInfo::decode_json(JSONObj *obj)
{
  string uid;

  JSONDecoder::decode_json("user_id", uid, obj, true);
  user_id.from_str(uid);

  JSONDecoder::decode_json("display_name", display_name, obj);
  JSONDecoder::decode_json("email", user_email, obj);
  bool susp = false;
  JSONDecoder::decode_json("suspended", susp, obj);
  suspended = (__u8)susp;
  JSONDecoder::decode_json("max_buckets", max_buckets, obj);

  JSONDecoder::decode_json("keys", access_keys, decode_access_keys, obj);
  JSONDecoder::decode_json("swift_keys", swift_keys, decode_swift_keys, obj);
  JSONDecoder::decode_json("subusers", subusers, decode_subusers, obj);

  JSONDecoder::decode_json("caps", caps, obj);

  string mask_str;
  JSONDecoder::decode_json("op_mask", mask_str, obj);
  rgw_parse_op_type_list(mask_str, &op_mask);

  bool sys = false;
  JSONDecoder::decode_json("system", sys, obj);
  system = (__u8)sys;
  bool ad = false;
  JSONDecoder::decode_json("admin", ad, obj);
  admin = (__u8)ad;
  JSONDecoder::decode_json("default_placement", default_placement.name, obj);
  JSONDecoder::decode_json("default_storage_class", default_placement.storage_class, obj);
  JSONDecoder::decode_json("placement_tags", placement_tags, obj);
  JSONDecoder::decode_json("bucket_quota", quota.bucket_quota, obj);
  JSONDecoder::decode_json("user_quota", quota.user_quota, obj);
  JSONDecoder::decode_json("temp_url_keys", temp_url_keys, obj);

  string user_source_type;
  JSONDecoder::decode_json("type", user_source_type, obj);
  if (user_source_type == "rgw") {
    type = TYPE_RGW;
  } else if (user_source_type == "keystone") {
    type = TYPE_KEYSTONE;
  } else if (user_source_type == "ldap") {
    type = TYPE_LDAP;
  } else if (user_source_type == "none") {
    type = TYPE_NONE;
  }
  JSONDecoder::decode_json("mfa_ids", mfa_ids, obj);
}


void RGWSubUser::generate_test_instances(list<RGWSubUser*>& o)
{
  RGWSubUser *u = new RGWSubUser;
  u->name = "name";
  u->perm_mask = 0xf;
  o.push_back(u);
  o.push_back(new RGWSubUser);
}

void RGWSubUser::dump(Formatter *f) const
{
  encode_json("id", name, f);
  char buf[256];
  perm_to_str(perm_mask, buf, sizeof(buf));
  encode_json("permissions", (const char *)buf, f);
}

void RGWSubUser::dump(Formatter *f, const string& user) const
{
  string s = user;
  s.append(":");
  s.append(name);
  encode_json("id", s, f);
  char buf[256];
  perm_to_str(perm_mask, buf, sizeof(buf));
  encode_json("permissions", (const char *)buf, f);
}

uint32_t str_to_perm(const string& s)
{
  if (s.compare("read") == 0)
    return RGW_PERM_READ;
  else if (s.compare("write") == 0)
    return RGW_PERM_WRITE;
  else if (s.compare("read-write") == 0)
    return RGW_PERM_READ | RGW_PERM_WRITE;
  else if (s.compare("full-control") == 0)
    return RGW_PERM_FULL_CONTROL;
  return 0;
}

void RGWSubUser::decode_json(JSONObj *obj)
{
  string uid;
  JSONDecoder::decode_json("id", uid, obj);
  int pos = uid.find(':');
  if (pos >= 0)
    name = uid.substr(pos + 1);
  string perm_str;
  JSONDecoder::decode_json("permissions", perm_str, obj);
  perm_mask = str_to_perm(perm_str);
}

void RGWAccessKey::generate_test_instances(list<RGWAccessKey*>& o)
{
  RGWAccessKey *k = new RGWAccessKey;
  k->id = "id";
  k->key = "key";
  k->subuser = "subuser";
  o.push_back(k);
  o.push_back(new RGWAccessKey);
}

void RGWAccessKey::dump(Formatter *f) const
{
  encode_json("access_key", id, f);
  encode_json("secret_key", key, f);
  encode_json("subuser", subuser, f);
  encode_json("active", active, f);
}

void RGWAccessKey::dump_plain(Formatter *f) const
{
  encode_json("access_key", id, f);
  encode_json("secret_key", key, f);
}

void RGWAccessKey::dump(Formatter *f, const string& user, bool swift) const
{
  string u = user;
  if (!subuser.empty()) {
    u.append(":");
    u.append(subuser);
  }
  encode_json("user", u, f);
  if (!swift) {
    encode_json("access_key", id, f);
  }
  encode_json("secret_key", key, f);
  encode_json("active", active, f);
}

void RGWAccessKey::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("access_key", id, obj, true);
  JSONDecoder::decode_json("secret_key", key, obj, true);
  if (!JSONDecoder::decode_json("subuser", subuser, obj)) {
    string user;
    JSONDecoder::decode_json("user", user, obj);
    int pos = user.find(':');
    if (pos >= 0) {
      subuser = user.substr(pos + 1);
    }
  }
  JSONDecoder::decode_json("active", active, obj);
}

void RGWAccessKey::decode_json(JSONObj *obj, bool swift) {
  if (!swift) {
    decode_json(obj);
    return;
  }

  if (!JSONDecoder::decode_json("subuser", subuser, obj)) {
    JSONDecoder::decode_json("user", id, obj, true);
    int pos = id.find(':');
    if (pos >= 0) {
      subuser = id.substr(pos + 1);
    }
  }
  JSONDecoder::decode_json("secret_key", key, obj, true);
  JSONDecoder::decode_json("active", active, obj);
}

void RGWStorageStats::dump(Formatter *f) const
{
  encode_json("size", size, f);
  encode_json("size_actual", size_rounded, f);
  if (dump_utilized) {
    encode_json("size_utilized", size_utilized, f);
  }
  encode_json("size_kb", rgw_rounded_kb(size), f);
  encode_json("size_kb_actual", rgw_rounded_kb(size_rounded), f);
  if (dump_utilized) {
    encode_json("size_kb_utilized", rgw_rounded_kb(size_utilized), f);
  }
  encode_json("num_objects", num_objects, f);
}

void rgw_obj_key::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("instance", instance, f);
  encode_json("ns", ns, f);
}

void rgw_obj_key::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("instance", instance, obj);
  JSONDecoder::decode_json("ns", ns, obj);
}

void rgw_raw_obj::dump(Formatter *f) const
{
  encode_json("pool", pool, f);
  encode_json("oid", oid, f);
  encode_json("loc", loc, f);
}

void rgw_raw_obj::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("oid", oid, obj);
  JSONDecoder::decode_json("loc", loc, obj);
}

void rgw_obj::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("key", key, f);
}

int rgw_bucket_parse_bucket_instance(const string& bucket_instance, string *bucket_name, string *bucket_id, int *shard_id)
{
  auto pos = bucket_instance.rfind(':');
  if (pos == string::npos) {
    return -EINVAL;
  }

  string first = bucket_instance.substr(0, pos);
  string second = bucket_instance.substr(pos + 1);

  pos = first.find(':');

  if (pos == string::npos) {
    *shard_id = -1;
    *bucket_name = first;
    *bucket_id = second;
    return 0;
  }

  *bucket_name = first.substr(0, pos);
  *bucket_id = first.substr(pos + 1);

  string err;
  *shard_id = strict_strtol(second.c_str(), 10, &err);
  if (!err.empty()) {
    return -EINVAL;
  }

  return 0;
}

boost::intrusive_ptr<CephContext>
rgw_global_init(const std::map<std::string,std::string> *defaults,
		    std::vector < const char* >& args,
		    uint32_t module_type, code_environment_t code_env,
		    int flags)
{
  // Load the config from the files, but not the mon
  global_pre_init(defaults, args, module_type, code_env, flags);

  // Get the store backend
  const auto& config_store = g_conf().get_val<std::string>("rgw_backend_store");

  if ((config_store == "dbstore") ||
      (config_store == "motr") || 
      (config_store == "daos")) {
    // These stores don't use the mon
    flags |= CINIT_FLAG_NO_MON_CONFIG;
  }

  // Finish global init, indicating we already ran pre-init
  return global_init(defaults, args, module_type, code_env, flags, false);
}

void RGWObjVersionTracker::generate_new_write_ver(CephContext *cct)
{
  write_version.ver = 1;
#define TAG_LEN 24

  write_version.tag.clear();
  append_rand_alpha(cct, write_version.tag, write_version.tag, TAG_LEN);
}

