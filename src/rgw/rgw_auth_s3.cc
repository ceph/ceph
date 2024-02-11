// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>
#include <map>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include "common/armor.h"
#include "common/utf8.h"
#include "rgw_rest_s3.h"
#include "rgw_auth_s3.h"
#include "rgw_common.h"
#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "rgw_crypt_sanitize.h"

#include <boost/container/small_vector.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim_all.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

static const auto signed_subresources = {
  "acl",
  "cors",
  "delete",
  "encryption",
  "lifecycle",
  "location",
  "logging",
  "notification",
  "partNumber",
  "policy",
  "policyStatus",
  "publicAccessBlock",
  "requestPayment",
  "response-cache-control",
  "response-content-disposition",
  "response-content-encoding",
  "response-content-language",
  "response-content-type",
  "response-expires",
  "tagging",
  "torrent",
  "uploadId",
  "uploads",
  "versionId",
  "versioning",
  "versions",
  "website",
  "object-lock"
};

/*
 * ?get the canonical amazon-style header for something?
 */

static std::string
get_canon_amz_hdr(const meta_map_t& meta_map)
{
  std::string dest;

  for (const auto& kv : meta_map) {
    dest.append(kv.first);
    dest.append(":");
    dest.append(kv.second);
    dest.append("\n");
  }

  return dest;
}

/*
 * ?get the canonical representation of the object's location
 */
static std::string
get_canon_resource(const DoutPrefixProvider *dpp, const char* const request_uri,
                   const std::map<std::string, std::string>& sub_resources)
{
  std::string dest;

  if (request_uri) {
    dest.append(request_uri);
  }

  bool initial = true;
  for (const auto& subresource : signed_subresources) {
    const auto iter = sub_resources.find(subresource);
    if (iter == std::end(sub_resources)) {
      continue;
    }
    
    if (initial) {
      dest.append("?");
      initial = false;
    } else {
      dest.append("&");
    }

    dest.append(iter->first);
    if (! iter->second.empty()) {
      dest.append("=");
      dest.append(iter->second);
    }
  }

  ldpp_dout(dpp, 10) << "get_canon_resource(): dest=" << dest << dendl;
  return dest;
}

/*
 * get the header authentication  information required to
 * compute a request's signature
 */
void rgw_create_s3_canonical_header(
  const DoutPrefixProvider *dpp,
  const char* const method,
  const char* const content_md5,
  const char* const content_type,
  const char* const date,
  const meta_map_t& meta_map,
  const meta_map_t& qs_map,
  const char* const request_uri,
  const std::map<std::string, std::string>& sub_resources,
  std::string& dest_str)
{
  std::string dest;

  if (method) {
    dest = method;
  }
  dest.append("\n");
  
  if (content_md5) {
    dest.append(content_md5);
  }
  dest.append("\n");

  if (content_type) {
    dest.append(content_type);
  }
  dest.append("\n");

  if (date) {
    dest.append(date);
  }
  dest.append("\n");

  dest.append(get_canon_amz_hdr(meta_map));
  dest.append(get_canon_amz_hdr(qs_map));
  dest.append(get_canon_resource(dpp, request_uri, sub_resources));

  dest_str = dest;
}

static inline bool is_base64_for_content_md5(unsigned char c) {
  return (isalnum(c) || isspace(c) || (c == '+') || (c == '/') || (c == '='));
}

static inline void get_v2_qs_map(const req_info& info,
				 meta_map_t& qs_map) {
  const auto& params = const_cast<RGWHTTPArgs&>(info.args).get_params();
  for (const auto& elt : params) {
    std::string k = boost::algorithm::to_lower_copy(elt.first);
    if (k.find("x-amz-meta-") == /* offset */ 0) {
      rgw_add_amz_meta_header(qs_map, k, elt.second);
    }
    if (k == "x-amz-security-token") {
      qs_map[k] = elt.second;
    }
  }
}

/*
 * get the header authentication  information required to
 * compute a request's signature
 */
bool rgw_create_s3_canonical_header(const DoutPrefixProvider *dpp,
                                    const req_info& info,
                                    utime_t* const header_time,
                                    std::string& dest,
                                    const bool qsr)
{
  const char* const content_md5 = info.env->get("HTTP_CONTENT_MD5");
  if (content_md5) {
    for (const char *p = content_md5; *p; p++) {
      if (!is_base64_for_content_md5(*p)) {
        ldpp_dout(dpp, 0) << "NOTICE: bad content-md5 provided (not base64),"
                << " aborting request p=" << *p << " " << (int)*p << dendl;
        return false;
      }
    }
  }

  const char *content_type = info.env->get("CONTENT_TYPE");

  std::string date;
  meta_map_t qs_map;

  if (qsr) {
    get_v2_qs_map(info, qs_map); // handle qs metadata
    date = info.args.get("Expires");
  } else {
    const char *str = info.env->get("HTTP_X_AMZ_DATE");
    const char *req_date = str;
    if (str == NULL) {
      req_date = info.env->get("HTTP_DATE");
      if (!req_date) {
        ldpp_dout(dpp, 0) << "NOTICE: missing date for auth header" << dendl;
        return false;
      }
      date = req_date;
    }

    if (header_time) {
      struct tm t;
      uint32_t ns = 0;
      if (!parse_rfc2616(req_date, &t) && !parse_iso8601(req_date, &t, &ns, false)) {
        ldpp_dout(dpp, 0) << "NOTICE: failed to parse date <" << req_date << "> for auth header" << dendl;
        return false;
      }
      if (t.tm_year < 70) {
        ldpp_dout(dpp, 0) << "NOTICE: bad date (predates epoch): " << req_date << dendl;
        return false;
      }
      *header_time = utime_t(internal_timegm(&t), 0);
      *header_time -= t.tm_gmtoff;
    }
  }

  const auto& meta_map = info.x_meta_map;
  const auto& sub_resources = info.args.get_sub_resources();

  std::string request_uri;
  if (info.effective_uri.empty()) {
    request_uri = info.request_uri;
  } else {
    request_uri = info.effective_uri;
  }

  rgw_create_s3_canonical_header(dpp, info.method, content_md5, content_type,
                                 date.c_str(), meta_map, qs_map,
				 request_uri.c_str(), sub_resources, dest);
  return true;
}


namespace rgw::auth::s3 {

bool is_time_skew_ok(time_t t)
{
  auto req_tp = ceph::coarse_real_clock::from_time_t(t);
  auto cur_tp = ceph::coarse_real_clock::now();

  if (std::chrono::abs(cur_tp - req_tp) > RGW_AUTH_GRACE) {
    dout(10) << "NOTICE: request time skew too big." << dendl;
    using ceph::operator<<;
    dout(10) << "req_tp=" << req_tp << ", cur_tp=" << cur_tp << dendl;
    return false;
  }

  return true;
}

static inline int parse_v4_query_string(const req_info& info,              /* in */
                                        std::string_view& credential,    /* out */
                                        std::string_view& signedheaders, /* out */
                                        std::string_view& signature,     /* out */
                                        std::string_view& date,          /* out */
                                        std::string_view& sessiontoken)  /* out */
{
  /* auth ships with req params ... */

  /* look for required params */
  credential = info.args.get("x-amz-credential");
  if (credential.size() == 0) {
    return -EPERM;
  }

  date = info.args.get("x-amz-date");
  struct tm date_t;
  if (!parse_iso8601(sview2cstr(date).data(), &date_t, nullptr, false)) {
    return -EPERM;
  }

  std::string_view expires = info.args.get("x-amz-expires");
  if (expires.empty()) {
    return -EPERM;
  }
  /* X-Amz-Expires provides the time period, in seconds, for which
     the generated presigned URL is valid. The minimum value
     you can set is 1, and the maximum is 604800 (seven days) */
  time_t exp = atoll(expires.data());
  if ((exp < 1) || (exp > 7*24*60*60)) {
    dout(10) << "ERROR: exp out of range, exp = " << exp << dendl;
    return -EPERM;
  }
  /* handle expiration in epoch time */
  uint64_t req_sec = (uint64_t)internal_timegm(&date_t);
  uint64_t now = ceph_clock_now();
  if (now >= req_sec + exp) {
    dout(10) << "ERROR: presigned URL has expired, now = " << now << ", req_sec = " << req_sec << ", exp = " << exp << dendl;
    return -ERR_PRESIGNED_URL_EXPIRED;
  }

  signedheaders = info.args.get("x-amz-signedheaders");
  if (signedheaders.size() == 0) {
    return -EPERM;
  }

  signature = info.args.get("x-amz-signature");
  if (signature.size() == 0) {
    return -EPERM;
  }

  if (info.args.exists("x-amz-security-token")) {
    sessiontoken = info.args.get("x-amz-security-token");
    if (sessiontoken.size() == 0) {
      return -EPERM;
    }
  }

  return 0;
}

static bool get_next_token(const std::string_view& s,
                           size_t& pos,
                           const char* const delims,
                           std::string_view& token)
{
  const size_t start = s.find_first_not_of(delims, pos);
  if (start == std::string_view::npos) {
    pos = s.size();
    return false;
  }

  size_t end = s.find_first_of(delims, start);
  if (end != std::string_view::npos)
    pos = end + 1;
  else {
    pos = end = s.size();
  }

  token = s.substr(start, end - start);
  return true;
}

template<std::size_t ExpectedStrNum>
boost::container::small_vector<std::string_view, ExpectedStrNum>
get_str_vec(const std::string_view& str, const char* const delims)
{
  boost::container::small_vector<std::string_view, ExpectedStrNum> str_vec;

  size_t pos = 0;
  std::string_view token;
  while (pos < str.size()) {
    if (get_next_token(str, pos, delims, token)) {
      if (token.size() > 0) {
        str_vec.push_back(token);
      }
    }
  }

  return str_vec;
}

template<std::size_t ExpectedStrNum>
boost::container::small_vector<std::string_view, ExpectedStrNum>
get_str_vec(const std::string_view& str)
{
  const char delims[] = ";,= \t";
  return get_str_vec<ExpectedStrNum>(str, delims);
}

static inline int parse_v4_auth_header(const req_info& info,               /* in */
                                       std::string_view& credential,     /* out */
                                       std::string_view& signedheaders,  /* out */
                                       std::string_view& signature,      /* out */
                                       std::string_view& date,           /* out */
                                       std::string_view& sessiontoken,   /* out */
                                       const DoutPrefixProvider *dpp)
{
  std::string_view input(info.env->get("HTTP_AUTHORIZATION", ""));
  try {
    input = input.substr(::strlen(AWS4_HMAC_SHA256_STR) + 1);
  } catch (std::out_of_range&) {
    /* We should never ever run into this situation as the presence of
     * AWS4_HMAC_SHA256_STR had been verified earlier. */
    ldpp_dout(dpp, 10) << "credentials string is too short" << dendl;
    return -EINVAL;
  }

  std::map<std::string_view, std::string_view> kv;
  for (const auto& s : get_str_vec<4>(input, ",")) {
    const auto parsed_pair = parse_key_value(s);
    if (parsed_pair) {
      kv[parsed_pair->first] = parsed_pair->second;
    } else {
      ldpp_dout(dpp, 10) << "NOTICE: failed to parse auth header (s=" << s << ")"
               << dendl;
      return -EINVAL;
    }
  }

  static const std::array<std::string_view, 3> required_keys = {
    "Credential",
    "SignedHeaders",
    "Signature"
  };

  /* Ensure that the presigned required keys are really there. */
  for (const auto& k : required_keys) {
    if (kv.find(k) == std::end(kv)) {
      ldpp_dout(dpp, 10) << "NOTICE: auth header missing key: " << k << dendl;
      return -EINVAL;
    }
  }

  credential = kv["Credential"];
  signedheaders = kv["SignedHeaders"];
  signature = kv["Signature"];

  /* sig hex str */
  ldpp_dout(dpp, 10) << "v4 signature format = " << signature << dendl;

  /* ------------------------- handle x-amz-date header */

  /* grab date */

  const char *d = info.env->get("HTTP_X_AMZ_DATE");

  struct tm t;
  if (unlikely(d == NULL)) {
    d = info.env->get("HTTP_DATE");
  }
  if (!d || !parse_iso8601(d, &t, NULL, false)) {
    ldpp_dout(dpp, 10) << "error reading date via http_x_amz_date and http_date" << dendl;
    return -EACCES;
  }
  date = d;

  if (!is_time_skew_ok(internal_timegm(&t))) {
    return -ERR_REQUEST_TIME_SKEWED;
  }

  auto token = info.env->get_optional("HTTP_X_AMZ_SECURITY_TOKEN");
  if (token) {
    sessiontoken = *token;
  }

  return 0;
}

bool is_non_s3_op(RGWOpType op_type)
{
  if (op_type == RGW_STS_GET_SESSION_TOKEN ||
      op_type == RGW_STS_ASSUME_ROLE ||
      op_type == RGW_STS_ASSUME_ROLE_WEB_IDENTITY ||
      op_type == RGW_OP_CREATE_ROLE ||
      op_type == RGW_OP_DELETE_ROLE ||
      op_type == RGW_OP_GET_ROLE ||
      op_type == RGW_OP_MODIFY_ROLE_TRUST_POLICY ||
      op_type == RGW_OP_LIST_ROLES ||
      op_type == RGW_OP_PUT_ROLE_POLICY ||
      op_type == RGW_OP_GET_ROLE_POLICY ||
      op_type == RGW_OP_LIST_ROLE_POLICIES ||
      op_type == RGW_OP_DELETE_ROLE_POLICY ||
      op_type == RGW_OP_PUT_USER_POLICY ||
      op_type == RGW_OP_GET_USER_POLICY ||
      op_type == RGW_OP_LIST_USER_POLICIES ||
      op_type == RGW_OP_DELETE_USER_POLICY ||
      op_type == RGW_OP_CREATE_OIDC_PROVIDER ||
      op_type == RGW_OP_DELETE_OIDC_PROVIDER ||
      op_type == RGW_OP_GET_OIDC_PROVIDER ||
      op_type == RGW_OP_LIST_OIDC_PROVIDERS ||
      op_type == RGW_OP_PUBSUB_TOPIC_CREATE ||
      op_type == RGW_OP_PUBSUB_TOPICS_LIST ||
      op_type == RGW_OP_PUBSUB_TOPIC_GET ||
      op_type == RGW_OP_PUBSUB_TOPIC_SET ||
      op_type == RGW_OP_PUBSUB_TOPIC_DELETE ||
      op_type == RGW_OP_TAG_ROLE ||
      op_type == RGW_OP_LIST_ROLE_TAGS ||
      op_type == RGW_OP_UNTAG_ROLE ||
      op_type == RGW_OP_UPDATE_ROLE) {
    return true;
  }
  return false;
}

int parse_v4_credentials(const req_info& info,                     /* in */
			 std::string_view& access_key_id,        /* out */
			 std::string_view& credential_scope,     /* out */
			 std::string_view& signedheaders,        /* out */
			 std::string_view& signature,            /* out */
			 std::string_view& date,                 /* out */
			 std::string_view& session_token,        /* out */
			 const bool using_qs,                    /* in */
                         const DoutPrefixProvider *dpp)
{
  std::string_view credential;
  int ret;
  if (using_qs) {
    ret = parse_v4_query_string(info, credential, signedheaders,
                                signature, date, session_token);
  } else {
    ret = parse_v4_auth_header(info, credential, signedheaders,
                               signature, date, session_token, dpp);
  }

  if (ret < 0) {
    return ret;
  }

  /* access_key/YYYYMMDD/region/service/aws4_request */
  ldpp_dout(dpp, 10) << "v4 credential format = " << credential << dendl;

  if (std::count(credential.begin(), credential.end(), '/') != 4) {
    return -EINVAL;
  }

  /* credential must end with 'aws4_request' */
  if (credential.find("aws4_request") == std::string::npos) {
    return -EINVAL;
  }

  /* grab access key id */
  const size_t pos = credential.find("/");
  access_key_id = credential.substr(0, pos);
  ldpp_dout(dpp, 10) << "access key id = " << access_key_id << dendl;

  /* grab credential scope */
  credential_scope = credential.substr(pos + 1);
  ldpp_dout(dpp, 10) << "credential scope = " << credential_scope << dendl;

  return 0;
}

string gen_v4_scope(const ceph::real_time& timestamp,
                    const string& region,
                    const string& service)
{

  auto sec = real_clock::to_time_t(timestamp);

  struct tm bt;
  gmtime_r(&sec, &bt);

  auto year = 1900 + bt.tm_year;
  auto mon = bt.tm_mon + 1;
  auto day = bt.tm_mday;

  return fmt::format(FMT_STRING("{:d}{:02d}{:02d}/{:s}/{:s}/aws4_request"),
                     year, mon, day, region, service);
}

std::string get_v4_canonical_qs(const req_info& info, const bool using_qs)
{
  const std::string *params = &info.request_params;
  std::string copy_params;
  if (params->empty()) {
    /* Optimize the typical flow. */
    return std::string();
  }
  if (params->find_first_of('+') != std::string::npos) {
    copy_params = *params;
    boost::replace_all(copy_params, "+", "%20");
    params = &copy_params;
  }

  /* Handle case when query string exists. Step 3 described in: http://docs.
   * aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html */
  std::multimap<std::string, std::string> canonical_qs_map;
  for (const auto& s : get_str_vec<5>(*params, "&")) {
    std::string_view key, val;
    const auto parsed_pair = parse_key_value(s);
    if (parsed_pair) {
      std::tie(key, val) = *parsed_pair;
    } else {
      /* Handling a parameter without any value (even the empty one). That's
       * it, we've encountered something like "this_param&other_param=val"
       * which is used by S3 for subresources. */
      key = s;
    }

    if (using_qs && boost::iequals(key, "X-Amz-Signature")) {
      /* Preserving the original behaviour of get_v4_canonical_qs() here. */
      continue;
    }

    // while awsv4 specs ask for all slashes to be encoded, s3 itself is relaxed
    // in its implementation allowing non-url-encoded slashes to be present in
    // presigned urls for instance
    canonical_qs_map.insert({{aws4_uri_recode(key, true), aws4_uri_recode(val, true)}});
  }

  /* Thanks to the early exist we have the guarantee that canonical_qs_map has
   * at least one element. */
  auto iter = std::begin(canonical_qs_map);
  std::string canonical_qs;
  canonical_qs.append(iter->first)
              .append("=", ::strlen("="))
              .append(iter->second);

  for (iter++; iter != std::end(canonical_qs_map); iter++) {
    canonical_qs.append("&", ::strlen("&"))
                .append(iter->first)
                .append("=", ::strlen("="))
                .append(iter->second);
  }

  return canonical_qs;
}

static void add_v4_canonical_params_from_map(const map<string, string>& m,
                                        std::map<string, string> *result,
                                        bool is_non_s3_op)
{
  for (auto& entry : m) {
    const auto& key = entry.first;
    if (key.empty() || (is_non_s3_op && key == "PayloadHash")) {
      continue;
    }

    (*result)[aws4_uri_recode(key, true)] = aws4_uri_recode(entry.second, true);
  }
}

std::string gen_v4_canonical_qs(const req_info& info, bool is_non_s3_op)
{
  std::map<std::string, std::string> canonical_qs_map;

  add_v4_canonical_params_from_map(info.args.get_params(), &canonical_qs_map, is_non_s3_op);
  add_v4_canonical_params_from_map(info.args.get_sys_params(), &canonical_qs_map, false);

  if (canonical_qs_map.empty()) {
    return string();
  }

  /* Thanks to the early exit we have the guarantee that canonical_qs_map has
   * at least one element. */
  auto iter = std::begin(canonical_qs_map);
  std::string canonical_qs;
  canonical_qs.append(iter->first)
              .append("=", ::strlen("="))
              .append(iter->second);

  for (iter++; iter != std::end(canonical_qs_map); iter++) {
    canonical_qs.append("&", ::strlen("&"))
                .append(iter->first)
                .append("=", ::strlen("="))
                .append(iter->second);
  }

  return canonical_qs;
}

std::string get_v4_canonical_method(const req_state* s)
{
  /* If this is a OPTIONS request we need to compute the v4 signature for the
   * intended HTTP method and not the OPTIONS request itself. */
  if (s->op_type == RGW_OP_OPTIONS_CORS) {
    const char *cors_method = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_METHOD");

    if (cors_method) {
      /* Validate request method passed in access-control-request-method is valid. */
      auto cors_flags = get_cors_method_flags(cors_method);
      if (!cors_flags) {
          ldpp_dout(s, 1) << "invalid access-control-request-method header = "
                          << cors_method << dendl;
          throw -EINVAL;
      }

      ldpp_dout(s, 10) << "canonical req method = " << cors_method
                       << ", due to access-control-request-method header" << dendl;
      return cors_method;
    } else {
      ldpp_dout(s, 1) << "invalid http options req missing "
                      << "access-control-request-method header" << dendl;
      throw -EINVAL;
    }
  }

  return s->info.method;
}

boost::optional<std::string>
get_v4_canonical_headers(const req_info& info,
                         const std::string_view& signedheaders,
                         const bool using_qs,
                         const bool force_boto2_compat)
{
  std::map<std::string_view, std::string> canonical_hdrs_map;
  for (const auto& token : get_str_vec<5>(signedheaders, ";")) {
    /* TODO(rzarzynski): we'd like to switch to sstring here but it should
     * get push_back() and reserve() first. */
    std::string token_env = "HTTP_";
    token_env.reserve(token.length() + std::strlen("HTTP_") + 1);

    std::transform(std::begin(token), std::end(token),
                   std::back_inserter(token_env), [](const int c) {
                     return c == '-' ? '_' : c == '_' ? '-' : std::toupper(c);
                   });

    if (token_env == "HTTP_CONTENT_LENGTH") {
      token_env = "CONTENT_LENGTH";
    } else if (token_env == "HTTP_CONTENT_TYPE") {
      token_env = "CONTENT_TYPE";
    }
    const char* const t = info.env->get(token_env.c_str());
    if (!t) {
      dout(10) << "warning env var not available " << token_env.c_str() << dendl;
      continue;
    }

    std::string token_value(t);
    if (token_env == "HTTP_CONTENT_MD5" &&
        !std::all_of(std::begin(token_value), std::end(token_value),
                     is_base64_for_content_md5)) {
      dout(0) << "NOTICE: bad content-md5 provided (not base64)"
            << ", aborting request" << dendl;
      return boost::none;
    }

    if (force_boto2_compat && using_qs && token == "host") {
      std::string_view port = info.env->get("SERVER_PORT", "");
      std::string_view secure_port = info.env->get("SERVER_PORT_SECURE", "");

      if (!secure_port.empty()) {
	if (secure_port != "443")
	  token_value.append(":", std::strlen(":"))
                     .append(secure_port.data(), secure_port.length());
      } else if (!port.empty()) {
	if (port != "80")
	  token_value.append(":", std::strlen(":"))
                     .append(port.data(), port.length());
      }
    }

    canonical_hdrs_map[token] = rgw_trim_whitespace(token_value);
  }

  std::string canonical_hdrs;
  for (const auto& header : canonical_hdrs_map) {
    const std::string_view& name = header.first;
    std::string value = header.second;
    boost::trim_all<std::string>(value);

    canonical_hdrs.append(name.data(), name.length())
                  .append(":", std::strlen(":"))
                  .append(value)
                  .append("\n", std::strlen("\n"));
  }
  return canonical_hdrs;
}

static void handle_header(const string& header, const string& val,
                          std::map<std::string, std::string> *canonical_hdrs_map)
{
  /* TODO(rzarzynski): we'd like to switch to sstring here but it should
   * get push_back() and reserve() first. */

  std::string token;
  token.reserve(header.length());

  if (header == "HTTP_CONTENT_LENGTH") {
    token = "content-length";
  } else if (header == "HTTP_CONTENT_TYPE") {
    token = "content-type";
  } else {
    auto start = std::begin(header);
    if (boost::algorithm::starts_with(header, "HTTP_")) {
      start += 5; /* len("HTTP_") */
    }

    std::transform(start, std::end(header),
                   std::back_inserter(token), [](const int c) {
                   return c == '_' ? '-' : std::tolower(c);
                   });
  }

  (*canonical_hdrs_map)[token] = rgw_trim_whitespace(val);
}

std::string gen_v4_canonical_headers(const req_info& info,
                                     const map<string, string>& extra_headers,
                                     string *signed_hdrs)
{
  std::map<std::string, std::string> canonical_hdrs_map;
  for (auto& entry : info.env->get_map()) {
    handle_header(entry.first, entry.second, &canonical_hdrs_map);
  }
  for (auto& entry : extra_headers) {
    handle_header(entry.first, entry.second, &canonical_hdrs_map);
  }

  std::string canonical_hdrs;
  signed_hdrs->clear();
  for (const auto& header : canonical_hdrs_map) {
    const auto& name = header.first;
    std::string value = header.second;
    boost::trim_all<std::string>(value);

    if (!signed_hdrs->empty()) {
      signed_hdrs->append(";");
    }
    signed_hdrs->append(name);

    canonical_hdrs.append(name.data(), name.length())
                  .append(":", std::strlen(":"))
                  .append(value)
                  .append("\n", std::strlen("\n"));
  }

  return canonical_hdrs;
}

/*
 * create canonical request for signature version 4
 *
 * http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
 */
sha256_digest_t
get_v4_canon_req_hash(CephContext* cct,
                      const std::string_view& http_verb,
                      const std::string& canonical_uri,
                      const std::string& canonical_qs,
                      const std::string& canonical_hdrs,
                      const std::string_view& signed_hdrs,
                      const std::string_view& request_payload_hash,
                      const DoutPrefixProvider *dpp)
{
  ldpp_dout(dpp, 10) << "payload request hash = " << request_payload_hash << dendl;

  const auto canonical_req = string_join_reserve("\n",
    http_verb,
    canonical_uri,
    canonical_qs,
    canonical_hdrs,
    signed_hdrs,
    request_payload_hash);

  const auto canonical_req_hash = calc_hash_sha256(canonical_req);

  using sanitize = rgw::crypt_sanitize::log_content;
  ldpp_dout(dpp, 10) << "canonical request = " << sanitize{canonical_req} << dendl;
  ldpp_dout(dpp, 10) << "canonical request hash = "
                 << canonical_req_hash << dendl;

  return canonical_req_hash;
}

/*
 * create string to sign for signature version 4
 *
 * http://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
 */
AWSEngine::VersionAbstractor::string_to_sign_t
get_v4_string_to_sign(CephContext* const cct,
                      const std::string_view& algorithm,
                      const std::string_view& request_date,
                      const std::string_view& credential_scope,
                      const sha256_digest_t& canonreq_hash,
                      const DoutPrefixProvider *dpp)
{
  const auto hexed_cr_hash = canonreq_hash.to_str();
  const std::string_view hexed_cr_hash_str(hexed_cr_hash);

  const auto string_to_sign = string_join_reserve("\n",
    algorithm,
    request_date,
    credential_scope,
    hexed_cr_hash_str);

  ldpp_dout(dpp, 10) << "string to sign = "
                 << rgw::crypt_sanitize::log_content{string_to_sign}
                 << dendl;

  return string_to_sign;
}


static inline std::tuple<std::string_view,            /* date */
                         std::string_view,            /* region */
                         std::string_view>            /* service */
parse_cred_scope(std::string_view credential_scope)
{
  /* date cred */
  size_t pos = credential_scope.find("/");
  const auto date_cs = credential_scope.substr(0, pos);
  credential_scope = credential_scope.substr(pos + 1);

  /* region cred */
  pos = credential_scope.find("/");
  const auto region_cs = credential_scope.substr(0, pos);
  credential_scope = credential_scope.substr(pos + 1);

  /* service cred */
  pos = credential_scope.find("/");
  const auto service_cs = credential_scope.substr(0, pos);

  return std::make_tuple(date_cs, region_cs, service_cs);
}

static inline std::vector<unsigned char>
transform_secret_key(const std::string_view& secret_access_key)
{
  /* TODO(rzarzynski): switch to constexpr when C++14 becomes available. */
  static const std::initializer_list<unsigned char> AWS4 { 'A', 'W', 'S', '4' };

  /* boost::container::small_vector might be used here if someone wants to
   * optimize out even more dynamic allocations. */
  std::vector<unsigned char> secret_key_utf8;
  secret_key_utf8.reserve(AWS4.size() + secret_access_key.size());
  secret_key_utf8.assign(AWS4);

  for (const auto c : secret_access_key) {
    std::array<unsigned char, MAX_UTF8_SZ> buf;
    const size_t n = encode_utf8(c, buf.data());
    secret_key_utf8.insert(std::end(secret_key_utf8),
                           std::begin(buf), std::begin(buf) + n);
  }

  return secret_key_utf8;
}

/*
 * calculate the SigningKey of AWS auth version 4
 */
static sha256_digest_t
get_v4_signing_key(CephContext* const cct,
                   const std::string_view& credential_scope,
                   const std::string_view& secret_access_key,
                   const DoutPrefixProvider *dpp)
{
  std::string_view date, region, service;
  std::tie(date, region, service) = parse_cred_scope(credential_scope);

  const auto utfed_sec_key = transform_secret_key(secret_access_key);
  const auto date_k = calc_hmac_sha256(utfed_sec_key, date);
  const auto region_k = calc_hmac_sha256(date_k, region);
  const auto service_k = calc_hmac_sha256(region_k, service);

  /* aws4_request */
  const auto signing_key = calc_hmac_sha256(service_k,
                                            std::string_view("aws4_request"));

  ldpp_dout(dpp, 10) << "date_k    = " << date_k << dendl;
  ldpp_dout(dpp, 10) << "region_k  = " << region_k << dendl;
  ldpp_dout(dpp, 10) << "service_k = " << service_k << dendl;
  ldpp_dout(dpp, 10) << "signing_k = " << signing_key << dendl;

  return signing_key;
}

/*
 * calculate the AWS signature version 4
 *
 * http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
 *
 * srv_signature_t is an alias over Ceph's basic_sstring. We're using
 * it to keep everything within the stack boundaries instead of doing
 * dynamic allocations.
 */
AWSEngine::VersionAbstractor::server_signature_t
get_v4_signature(const std::string_view& credential_scope,
                 CephContext* const cct,
                 const std::string_view& secret_key,
                 const AWSEngine::VersionAbstractor::string_to_sign_t& string_to_sign,
                 const DoutPrefixProvider *dpp)
{
  auto signing_key = get_v4_signing_key(cct, credential_scope, secret_key, dpp);

  /* The server-side generated digest for comparison. */
  const auto digest = calc_hmac_sha256(signing_key, string_to_sign);

  /* TODO(rzarzynski): I would love to see our sstring having reserve() and
   * the non-const data() variant like C++17's std::string. */
  using srv_signature_t = AWSEngine::VersionAbstractor::server_signature_t;
  srv_signature_t signature(srv_signature_t::initialized_later(),
                            digest.SIZE * 2);
  buf_to_hex(digest.v, digest.SIZE, signature.begin());

  ldpp_dout(dpp, 10) << "generated signature = " << signature << dendl;

  return signature;
}

AWSEngine::VersionAbstractor::server_signature_t
get_v2_signature(CephContext* const cct,
                 const std::string& secret_key,
                 const AWSEngine::VersionAbstractor::string_to_sign_t& string_to_sign)
{
  if (secret_key.empty()) {
    throw -EINVAL;
  }

  const auto digest = calc_hmac_sha1(secret_key, string_to_sign);

  /* 64 is really enough */;
  char buf[64];
  const int ret = ceph_armor(std::begin(buf),
                             std::begin(buf) + 64,
                             reinterpret_cast<const char *>(digest.v),
                             reinterpret_cast<const char *>(digest.v + digest.SIZE));
  if (ret < 0) {
    ldout(cct, 10) << "ceph_armor failed" << dendl;
    throw ret;
  } else {
    buf[ret] = '\0';
    using srv_signature_t = AWSEngine::VersionAbstractor::server_signature_t;
    return srv_signature_t(buf, ret);
  }
}

bool AWSv4ComplMulti::ChunkMeta::is_new_chunk_in_stream(size_t stream_pos) const
{
  return stream_pos >= (data_offset_in_stream + data_length);
}

size_t AWSv4ComplMulti::ChunkMeta::get_data_size(size_t stream_pos) const
{
  if (stream_pos > (data_offset_in_stream + data_length)) {
    /* Data in parsing_buf. */
    return data_length;
  } else {
    return data_offset_in_stream + data_length - stream_pos;
  }
}


/* AWSv4 completers begin. */
std::pair<AWSv4ComplMulti::ChunkMeta, size_t /* consumed */>
AWSv4ComplMulti::ChunkMeta::create_next(CephContext* const cct,
                                        ChunkMeta&& old,
                                        const char* const metabuf,
                                        const size_t metabuf_len)
{
  std::string_view metastr(metabuf, metabuf_len);

  const size_t semicolon_pos = metastr.find(";");
  if (semicolon_pos == std::string_view::npos) {
    ldout(cct, 20) << "AWSv4ComplMulti cannot find the ';' separator"
                   << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  char* data_field_end;
  /* strtoull ignores the "\r\n" sequence after each non-first chunk. */
  const size_t data_length = std::strtoull(metabuf, &data_field_end, 16);
  if (data_length == 0 && data_field_end == metabuf) {
    ldout(cct, 20) << "AWSv4ComplMulti: cannot parse the data size"
                   << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  /* Parse the chunk_signature=... part. */
  const auto signature_part = metastr.substr(semicolon_pos + 1);
  const size_t eq_sign_pos = signature_part.find("=");
  if (eq_sign_pos == std::string_view::npos) {
    ldout(cct, 20) << "AWSv4ComplMulti: cannot find the '=' separator"
                   << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  /* OK, we have at least the beginning of a signature. */
  const size_t data_sep_pos = signature_part.find("\r\n");
  if (data_sep_pos == std::string_view::npos) {
    ldout(cct, 20) << "AWSv4ComplMulti: no new line at signature end"
                   << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  const auto signature = \
    signature_part.substr(eq_sign_pos + 1, data_sep_pos - 1 - eq_sign_pos);
  if (signature.length() != SIG_SIZE) {
    ldout(cct, 20) << "AWSv4ComplMulti: signature.length() != 64"
                   << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  const size_t data_starts_in_stream = \
    + semicolon_pos + strlen(";") + data_sep_pos  + strlen("\r\n")
    + old.data_offset_in_stream + old.data_length;

  ldout(cct, 20) << "parsed new chunk; signature=" << signature
                 << ", data_length=" << data_length
                 << ", data_starts_in_stream=" << data_starts_in_stream
                 << dendl;

  return std::make_pair(ChunkMeta(data_starts_in_stream,
                                  data_length,
                                  signature),
                        semicolon_pos + 83);
}

std::string
AWSv4ComplMulti::calc_chunk_signature(const std::string& payload_hash) const
{
  const auto string_to_sign = string_join_reserve("\n",
    AWS4_HMAC_SHA256_PAYLOAD_STR,
    date,
    credential_scope,
    prev_chunk_signature,
    AWS4_EMPTY_PAYLOAD_HASH,
    payload_hash);

  ldout(cct, 20) << "AWSv4ComplMulti: string_to_sign=\n" << string_to_sign
                 << dendl;

  /* new chunk signature */
  const auto sig = calc_hmac_sha256(signing_key, string_to_sign);
  /* FIXME(rzarzynski): std::string here is really unnecessary. */
  return sig.to_str();
}


bool AWSv4ComplMulti::is_signature_mismatched()
{
  /* The validity of previous chunk can be verified only after getting meta-
   * data of the next one. */
  const auto payload_hash = calc_hash_sha256_restart_stream(&sha256_hash);
  const auto calc_signature = calc_chunk_signature(payload_hash);

  if (chunk_meta.get_signature() != calc_signature) {
    ldout(cct, 20) << "AWSv4ComplMulti: ERROR: chunk signature mismatch"
                   << dendl;
    ldout(cct, 20) << "AWSv4ComplMulti: declared signature="
                   << chunk_meta.get_signature() << dendl;
    ldout(cct, 20) << "AWSv4ComplMulti: calculated signature="
                   << calc_signature << dendl;

    return true;
  } else {
    prev_chunk_signature = chunk_meta.get_signature();
    return false;
  }
}

size_t AWSv4ComplMulti::recv_chunk(char* const buf, const size_t buf_max, bool& eof)
{
  /* Buffer stores only parsed stream. Raw values reflect the stream
   * we're getting from a client. */
  size_t buf_pos = 0;

  if (chunk_meta.is_new_chunk_in_stream(stream_pos)) {
    /* Verify signature of the previous chunk. We aren't doing that for new
     * one as the procedure requires calculation of payload hash. This code
     * won't be triggered for the last, zero-length chunk. Instead, is will
     * be checked in the complete() method.  */
    if (stream_pos >= ChunkMeta::META_MAX_SIZE && is_signature_mismatched()) {
      throw rgw::io::Exception(ERR_SIGNATURE_NO_MATCH, std::system_category());
    }

    /* We don't have metadata for this range. This means a new chunk, so we
     * need to parse a fresh portion of the stream. Let's start. */
    size_t to_extract = parsing_buf.capacity() - parsing_buf.size();
    do {
      const size_t orig_size = parsing_buf.size();
      parsing_buf.resize(parsing_buf.size() + to_extract);
      const size_t received = io_base_t::recv_body(parsing_buf.data() + orig_size,
                                                   to_extract);
      parsing_buf.resize(parsing_buf.size() - (to_extract - received));
      if (received == 0) {
        eof = true;
        break;
      }

      stream_pos += received;
      to_extract -= received;
    } while (to_extract > 0);

    size_t consumed;
    std::tie(chunk_meta, consumed) = \
      ChunkMeta::create_next(cct, std::move(chunk_meta),
                             parsing_buf.data(), parsing_buf.size());

    /* We can drop the bytes consumed during metadata parsing. The remainder
     * can be chunk's data plus possibly beginning of next chunks' metadata. */
    parsing_buf.erase(std::begin(parsing_buf),
                      std::begin(parsing_buf) + consumed);
  }

  size_t stream_pos_was = stream_pos - parsing_buf.size();

  size_t to_extract = \
    std::min(chunk_meta.get_data_size(stream_pos_was), buf_max);
  dout(30) << "AWSv4ComplMulti: stream_pos_was=" << stream_pos_was << ", to_extract=" << to_extract << dendl;
  
  /* It's quite probable we have a couple of real data bytes stored together
   * with meta-data in the parsing_buf. We need to extract them and move to
   * the final buffer. This is a trade-off between frontend's read overhead
   * and memcpy. */
  if (to_extract > 0 && parsing_buf.size() > 0) {
    const auto data_len = std::min(to_extract, parsing_buf.size());
    const auto data_end_iter = std::begin(parsing_buf) + data_len;
    dout(30) << "AWSv4ComplMulti: to_extract=" << to_extract << ", data_len=" << data_len << dendl;

    std::copy(std::begin(parsing_buf), data_end_iter, buf);
    parsing_buf.erase(std::begin(parsing_buf), data_end_iter);

    calc_hash_sha256_update_stream(sha256_hash, buf, data_len);

    to_extract -= data_len;
    buf_pos += data_len;
  }

  /* Now we can do the bulk read directly from RestfulClient without any extra
   * buffering. */
  while (to_extract > 0) {
    const size_t received = io_base_t::recv_body(buf + buf_pos, to_extract);
    dout(30) << "AWSv4ComplMulti: to_extract=" << to_extract << ", received=" << received << dendl;

    if (received == 0) {
      eof = true;
      break;
    }

    calc_hash_sha256_update_stream(sha256_hash, buf + buf_pos, received);

    buf_pos += received;
    stream_pos += received;
    to_extract -= received;
  }

  dout(20) << "AWSv4ComplMulti: filled=" << buf_pos << dendl;
  return buf_pos;
}

size_t AWSv4ComplMulti::recv_body(char* const buf, const size_t buf_max)
{
  bool eof = false;
  size_t total = 0;

  while (total < buf_max && !eof) {
    const size_t received = recv_chunk(buf + total, buf_max - total, eof);
    total += received;
  }
  dout(20) << "AWSv4ComplMulti: received=" << total << dendl;
  return total;
}

void AWSv4ComplMulti::modify_request_state(const DoutPrefixProvider* dpp, req_state* const s_rw)
{
  const char* const decoded_length = \
    s_rw->info.env->get("HTTP_X_AMZ_DECODED_CONTENT_LENGTH");

  if (!decoded_length) {
    throw -EINVAL;
  } else {
    s_rw->length = decoded_length;
    s_rw->content_length = parse_content_length(decoded_length);

    if (s_rw->content_length < 0) {
      ldpp_dout(dpp, 10) << "negative AWSv4's content length, aborting" << dendl;
      throw -EINVAL;
    }
  }

  /* Install the filter over rgw::io::RestfulClient. */
  AWS_AUTHv4_IO(s_rw)->add_filter(
    std::static_pointer_cast<io_base_t>(shared_from_this()));
}

bool AWSv4ComplMulti::complete()
{
  /* Now it's time to verify the signature of the last, zero-length chunk. */
  if (is_signature_mismatched()) {
    ldout(cct, 10) << "ERROR: signature of last chunk does not match"
                   << dendl;
    return false;
  } else {
    return true;
  }
}

rgw::auth::Completer::cmplptr_t
AWSv4ComplMulti::create(const req_state* const s,
                        std::string_view date,
                        std::string_view credential_scope,
                        std::string_view seed_signature,
                        const boost::optional<std::string>& secret_key)
{
  if (!secret_key) {
    /* Some external authorizers (like Keystone) aren't fully compliant with
     * AWSv4. They do not provide the secret_key which is necessary to handle
     * the streamed upload. */
    throw -ERR_NOT_IMPLEMENTED;
  }

  const auto signing_key = \
    rgw::auth::s3::get_v4_signing_key(s->cct, credential_scope, *secret_key, s);

  return std::make_shared<AWSv4ComplMulti>(s,
                                           std::move(date),
                                           std::move(credential_scope),
                                           std::move(seed_signature),
                                           signing_key);
}

size_t AWSv4ComplSingle::recv_body(char* const buf, const size_t max)
{
  const auto received = io_base_t::recv_body(buf, max);
  calc_hash_sha256_update_stream(sha256_hash, buf, received);

  return received;
}

void AWSv4ComplSingle::modify_request_state(const DoutPrefixProvider* dpp, req_state* const s_rw)
{
  /* Install the filter over rgw::io::RestfulClient. */
  AWS_AUTHv4_IO(s_rw)->add_filter(
    std::static_pointer_cast<io_base_t>(shared_from_this()));
}

bool AWSv4ComplSingle::complete()
{
  /* The completer is only for the cases where signed payload has been
   * requested. It won't be used, for instance, during the query string-based
   * authentication. */
  const auto payload_hash = calc_hash_sha256_close_stream(&sha256_hash);

  /* Validate x-amz-sha256 */
  if (payload_hash.compare(expected_request_payload_hash) == 0) {
    return true;
  } else {
    ldout(cct, 10) << "ERROR: x-amz-content-sha256 does not match"
                   << dendl;
    ldout(cct, 10) << "ERROR:   grab_aws4_sha256_hash()="
                   << payload_hash << dendl;
    ldout(cct, 10) << "ERROR:   expected_request_payload_hash="
                   << expected_request_payload_hash << dendl;
    return false;
  }
}

AWSv4ComplSingle::AWSv4ComplSingle(const req_state* const s)
  : io_base_t(nullptr),
    cct(s->cct),
    expected_request_payload_hash(get_v4_exp_payload_hash(s->info)),
    sha256_hash(calc_hash_sha256_open_stream()) {
}

rgw::auth::Completer::cmplptr_t
AWSv4ComplSingle::create(const req_state* const s,
                         const boost::optional<std::string>&)
{
  return std::make_shared<AWSv4ComplSingle>(s);
}

} // namespace rgw::auth::s3
