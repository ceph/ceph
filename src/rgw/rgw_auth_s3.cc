// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <map>
#include <string>

#include "common/armor.h"
#include "common/utf8.h"
#include "rgw_common.h"
#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "rgw_crypt_sanitize.h"

#include "include/str_list.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

static const auto signed_subresources = {
  "acl",
  "cors",
  "delete",
  "lifecycle",
  "location",
  "logging",
  "notification",
  "partNumber",
  "policy",
  "requestPayment",
  "response-cache-control",
  "response-content-disposition",
  "response-content-encoding",
  "response-content-language",
  "response-content-type",
  "response-expires",
  "torrent",
  "uploadId",
  "uploads",
  "start-date",
  "end-date",
  "versionId",
  "versioning",
  "versions",
  "website"
};

/*
 * ?get the canonical amazon-style header for something?
 */

static std::string
get_canon_amz_hdr(const std::map<std::string, std::string>& meta_map)
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
get_canon_resource(const char* const request_uri,
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

  dout(10) << "get_canon_resource(): dest=" << dest << dendl;
  return dest;
}

/*
 * get the header authentication  information required to
 * compute a request's signature
 */
void rgw_create_s3_canonical_header(
  const char* const method,
  const char* const content_md5,
  const char* const content_type,
  const char* const date,
  const std::map<std::string, std::string>& meta_map,
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
  dest.append(get_canon_resource(request_uri, sub_resources));

  dest_str = dest;
}

int rgw_get_s3_header_digest(const std::string& auth_hdr,
                             const std::string& key,
                             std::string& dest)
{
  if (key.empty())
    return -EINVAL;

  char hmac_sha1[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  calc_hmac_sha1(key.c_str(), key.size(), auth_hdr.c_str(), auth_hdr.size(), hmac_sha1);

  char b64[64]; /* 64 is really enough */
  int ret = ceph_armor(b64, b64 + 64, hmac_sha1,
		       hmac_sha1 + CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
  if (ret < 0) {
    dout(10) << "ceph_armor failed" << dendl;
    return ret;
  }
  b64[ret] = '\0';

  dest = b64;

  return 0;
}

static inline bool is_base64_for_content_md5(unsigned char c) {
  return (isalnum(c) || isspace(c) || (c == '+') || (c == '/') || (c == '='));
}

/*
 * get the header authentication  information required to
 * compute a request's signature
 */
bool rgw_create_s3_canonical_header(const req_info& info,
                                    utime_t* const header_time,
                                    std::string& dest,
                                    const bool qsr)
{
  const char* const content_md5 = info.env->get("HTTP_CONTENT_MD5");
  if (content_md5) {
    for (const char *p = content_md5; *p; p++) {
      if (!is_base64_for_content_md5(*p)) {
        dout(0) << "NOTICE: bad content-md5 provided (not base64),"
                << " aborting request p=" << *p << " " << (int)*p << dendl;
        return false;
      }
    }
  }

  const char *content_type = info.env->get("CONTENT_TYPE");

  std::string date;
  if (qsr) {
    date = info.args.get("Expires");
  } else {
    const char *str = info.env->get("HTTP_DATE");
    const char *req_date = str;
    if (str) {
      date = str;
    } else {
      req_date = info.env->get("HTTP_X_AMZ_DATE");
      if (!req_date) {
        dout(0) << "NOTICE: missing date for auth header" << dendl;
        return false;
      }
    }

    if (header_time) {
      struct tm t;
      if (!parse_rfc2616(req_date, &t)) {
        dout(0) << "NOTICE: failed to parse date for auth header" << dendl;
        return false;
      }
      if (t.tm_year < 70) {
        dout(0) << "NOTICE: bad date (predates epoch): " << req_date << dendl;
        return false;
      }
      *header_time = utime_t(internal_timegm(&t), 0);
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

  rgw_create_s3_canonical_header(info.method, content_md5, content_type,
                                 date.c_str(), meta_map, request_uri.c_str(),
                                 sub_resources, dest);
  return true;
}


namespace rgw {
namespace auth {
namespace s3 {

/* FIXME(rzarzynski): duplicated from rgw_rest_s3.h. */
#define RGW_AUTH_GRACE_MINS 15

static inline int parse_v4_credentials_qs(const req_info& info,           /* in */
                                          std::string& credential,        /* out */
                                          std::string& signedheaders,     /* out */
                                          std::string& signature,         /* out */
                                          std::string& date)              /* out */
{
  /* auth ships with req params ... */

  /* look for required params */
  credential = info.args.get("X-Amz-Credential");
  if (credential.size() == 0) {
    return -EPERM;
  }

  date = info.args.get("X-Amz-Date");
  struct tm date_t;
  if (!parse_iso8601(date.c_str(), &date_t, NULL, false))
    return -EPERM;

  /* Used for pre-signatured url, We shouldn't return -ERR_REQUEST_TIME_SKEWED
   * when current time <= X-Amz-Expires */
  bool qsr = false;

  uint64_t now_req = 0;
  uint64_t now = ceph_clock_now();

  std::string expires = info.args.get("X-Amz-Expires");
  if (!expires.empty()) {
    /* X-Amz-Expires provides the time period, in seconds, for which
       the generated presigned URL is valid. The minimum value
       you can set is 1, and the maximum is 604800 (seven days) */
    time_t exp = atoll(expires.c_str());
    if ((exp < 1) || (exp > 7*24*60*60)) {
      dout(10) << "NOTICE: exp out of range, exp = " << exp << dendl;
      return -EPERM;
    }
    /* handle expiration in epoch time */
    now_req = (uint64_t)internal_timegm(&date_t);
    if (now >= now_req + exp) {
      dout(10) << "NOTICE: now = " << now << ", now_req = " << now_req << ", exp = " << exp << dendl;
      return -EPERM;
    }
    qsr = true;
  }

  if ((now_req < now - RGW_AUTH_GRACE_MINS * 60 ||
     now_req > now + RGW_AUTH_GRACE_MINS * 60) && !qsr) {
    dout(10) << "NOTICE: request time skew too big." << dendl;
    dout(10) << "now_req = " << now_req << " now = " << now
             << "; now - RGW_AUTH_GRACE_MINS="
             << now - RGW_AUTH_GRACE_MINS * 60
             << "; now + RGW_AUTH_GRACE_MINS="
             << now + RGW_AUTH_GRACE_MINS * 60 << dendl;
    return -ERR_REQUEST_TIME_SKEWED;
  }

  signedheaders = info.args.get("X-Amz-SignedHeaders");
  if (signedheaders.size() == 0) {
    return -EPERM;
  }

  signature = info.args.get("X-Amz-Signature");
  if (signature.size() == 0) {
    return -EPERM;
  }

  return 0;
}

static inline int parse_v4_credentials_hdrs(const req_info& info,           /* in */
                                            std::string& credential,        /* out */
                                            std::string& signedheaders,     /* out */
                                            std::string& signature,         /* out */
                                            std::string& date)              /* out */
{
  /* auth ships in headers ... */

  /* ------------------------- handle Credential header */

  const char* const http_auth = info.env->get("HTTP_AUTHORIZATION");
  string auth_str = http_auth;

#define AWS4_HMAC_SHA256_STR "AWS4-HMAC-SHA256"
#define CREDENTIALS_PREFIX_LEN (sizeof(AWS4_HMAC_SHA256_STR) - 1)
  uint64_t min_len = CREDENTIALS_PREFIX_LEN + 1;
  if (auth_str.length() < min_len) {
    dout(10) << "credentials string is too short" << dendl;
    return -EINVAL;
  }

  list<string> auth_list;
  get_str_list(auth_str.substr(min_len), ",", auth_list);

  map<string, string> kv;

  for (string& s : auth_list) {
    string key, val;
    int ret = parse_key_value(s, key, val);
    if (ret < 0) {
      dout(10) << "NOTICE: failed to parse auth header (s=" << s << ")" << dendl;
      return -EINVAL;
    }
    kv[key] = std::move(val);
  }

  static std::array<string, 3> aws4_presigned_required_keys = {
    "Credential",
    "SignedHeaders",
    "Signature"
  };

  for (string& k : aws4_presigned_required_keys) {
    if (kv.find(k) == kv.end()) {
      dout(10) << "NOTICE: auth header missing key: " << k << dendl;
      return -EINVAL;
    }
  }

  credential = std::move(kv["Credential"]);
  signedheaders = std::move(kv["SignedHeaders"]);
  signature = std::move(kv["Signature"]);

  /* sig hex str */
  dout(10) << "v4 signature format = " << signature << dendl;

  /* ------------------------- handle x-amz-date header */

  /* grab date */

  const char *d = info.env->get("HTTP_X_AMZ_DATE");
  struct tm t;
  if (!parse_iso8601(d, &t, NULL, false)) {
    dout(10) << "error reading date via http_x_amz_date" << dendl;
    return -EACCES;
  }
  date = d;

  return 0;
}

int parse_credentials(const req_info& info,             /* in */
                      std::string& credential,          /* out */
                      std::string& signedheaders,       /* out */
                      std::string& signature,           /* out */
                      std::string& date,                /* out */
                      bool& using_qs)                   /* out */
{
  int ret;
  const char* const http_auth = info.env->get("HTTP_AUTHORIZATION");

  using_qs = http_auth == nullptr || http_auth[0] == '\0';
  if (using_qs) {
    ret = parse_v4_credentials_qs(info, credential, signedheaders,
                                  signature, date);
  } else {
    ret = parse_v4_credentials_hdrs(info, credential, signedheaders,
                                    signature, date);
  }

  if (ret < 0) {
    return ret;
  }

  /* AKIAIVKTAZLOCF43WNQD/AAAAMMDD/region/host/aws4_request */
  dout(10) << "v4 credential format = " << credential << dendl;

  if (std::count(credential.begin(), credential.end(), '/') != 4) {
    return -EINVAL;
  }

  /* credential must end with 'aws4_request' */
  if (credential.find("aws4_request") == std::string::npos) {
    return -EINVAL;
  }

  return 0;
}

static inline bool char_needs_aws4_escaping(const char c)
{
  if ((c >= 'a' && c <= 'z') ||
      (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9')) {
    return false;
  }

  switch (c) {
    case '-':
    case '_':
    case '.':
    case '~':
      return false;
  }
  return true;
}

static inline void aws4_uri_encode(const string& src, string& dst)
{
  const char *p = src.c_str();
  for (unsigned i = 0; i < src.size(); i++, p++) {
    if (char_needs_aws4_escaping(*p)) {
      rgw_uri_escape_char(*p, dst);
      continue;
    }

    dst.append(p, 1);
  }
}

std::string get_v4_canonical_qs(const req_info& info, const bool using_qs)
{
  std::string canonical_qs = info.request_params;

  if (!canonical_qs.empty()) {

    /* Handle case when query string exists. Step 3 in
     * http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html */
    map<string, string> canonical_qs_map;
    istringstream cqs(canonical_qs);
    string keyval;

    while (getline(cqs, keyval, '&')) {
      string key, val;
      istringstream kv(keyval);
      getline(kv, key, '=');
      getline(kv, val, '=');
      if (!using_qs || key != "X-Amz-Signature") {
        string encoded_key;
        string encoded_val;
        if (key != "X-Amz-Credential") {
          string key_decoded;
          url_decode(key, key_decoded);
          if (key.length() != key_decoded.length()) {
            encoded_key = key;
          } else {
            aws4_uri_encode(key, encoded_key);
          }
          string val_decoded;
          url_decode(val, val_decoded);
          if (val.length() != val_decoded.length()) {
            encoded_val = val;
          } else {
            aws4_uri_encode(val, encoded_val);
          }
        } else {
          encoded_key = key;
          encoded_val = val;
        }
        canonical_qs_map[encoded_key] = encoded_val;
      }
    }

    canonical_qs = "";

    map<string, string>::iterator last = canonical_qs_map.end();
    --last;

    for (map<string, string>::iterator it = canonical_qs_map.begin();
        it != canonical_qs_map.end(); ++it) {
      canonical_qs.append(it->first + "=" + it->second);
      if (it != last) {
        canonical_qs.append("&");
      }
    }
  }

  return canonical_qs;
}

boost::optional<std::string>
get_v4_canonical_headers(const req_info& info,
                         const std::string& signedheaders,
                         const bool using_qs,
                         const bool force_boto2_compat)
{
  map<string, string> canonical_hdrs_map;
  istringstream sh(signedheaders);
  string token;
  string port = info.env->get("SERVER_PORT", "");
  string secure_port = info.env->get("SERVER_PORT_SECURE", "");

  while (getline(sh, token, ';')) {
    string token_env = "HTTP_" + token;
    transform(token_env.begin(), token_env.end(), token_env.begin(), ::toupper);
    replace(token_env.begin(), token_env.end(), '-', '_');
    if (token_env == "HTTP_CONTENT_LENGTH") {
      token_env = "CONTENT_LENGTH";
    }
    if (token_env == "HTTP_CONTENT_TYPE") {
      token_env = "CONTENT_TYPE";
    }
    const char *t = info.env->get(token_env.c_str());
    if (!t) {
      dout(10) << "warning env var not available" << dendl;
      continue;
    }
    if (token_env == "HTTP_CONTENT_MD5") {
      for (const char *p = t; *p; p++) {
	if (!is_base64_for_content_md5(*p)) {
	  dout(0) << "NOTICE: bad content-md5 provided (not base64), aborting request p=" << *p << " " << (int)*p << dendl;
	  return boost::none;
	}
      }
    }
    string token_value = string(t);
    if (force_boto2_compat && using_qs && (token == "host")) {
      if (!secure_port.empty()) {
	if (secure_port != "443")
	  token_value = token_value + ":" + secure_port;
      } else if (!port.empty()) {
	if (port != "80")
	  token_value = token_value + ":" + port;
      }
    }
    canonical_hdrs_map[token] = rgw_trim_whitespace(token_value);
  }

  std::string canonical_hdrs;
  for (map<string, string>::iterator it = canonical_hdrs_map.begin();
      it != canonical_hdrs_map.end(); ++it) {
    canonical_hdrs.append(it->first + ":" + it->second + "\n");
  }

  return canonical_hdrs;
}

std::string hash_string_sha256(const char* const data, const int len)
{
  std::string dest;
  calc_hash_sha256(data, len, dest);
  return dest;
}

/*
 * assemble canonical request for signature version 4
 */
static std::string assemble_v4_canonical_request(
  const char* const method,
  const char* const canonical_uri,
  const char* const canonical_qs,
  const char* const canonical_hdrs,
  const char* const signed_hdrs,
  const char* const request_payload_hash)
{
  std::string dest;

  if (method)
    dest = method;
  dest.append("\n");

  if (canonical_uri) {
    dest.append(canonical_uri);
  }
  dest.append("\n");

  if (canonical_qs) {
    dest.append(canonical_qs);
  }
  dest.append("\n");

  if (canonical_hdrs)
    dest.append(canonical_hdrs);
  dest.append("\n");

  if (signed_hdrs)
    dest.append(signed_hdrs);
  dest.append("\n");

  if (request_payload_hash)
    dest.append(request_payload_hash);

  return dest;
}

/*
 * create canonical request for signature version 4
 */
std::string get_v4_canonical_request_hash(CephContext* cct,
                                          const std::string& http_verb,
                                          const std::string& canonical_uri,
                                          const std::string& canonical_qs,
                                          const std::string& canonical_hdrs,
                                          const std::string& signed_hdrs,
                                          const std::string& request_payload_hash)
{
  ldout(cct, 10) << "payload request hash = " << request_payload_hash << dendl;

  std::string canonical_req = \
    assemble_v4_canonical_request(http_verb.c_str(),
                                  canonical_uri.c_str(),
                                  canonical_qs.c_str(),
                                  canonical_hdrs.c_str(),
                                  signed_hdrs.c_str(),
                                  request_payload_hash.c_str());

  std::string canonical_req_hash = \
    hash_string_sha256(canonical_req.c_str(), canonical_req.size());

  ldout(cct, 10) << "canonical request = " << canonical_req << dendl;
  ldout(cct, 10) << "canonical request hash = " << canonical_req_hash << dendl;

  return canonical_req_hash;
}

/*
 * assemble string to sign for signature version 4
 */
static std::string rgw_assemble_s3_v4_string_to_sign(
  const char* const algorithm,
  const char* const request_date,
  const char* const credential_scope,
  const char* const hashed_qr
) {
  std::string dest;

  if (algorithm)
    dest = algorithm;
  dest.append("\n");

  if (request_date)
    dest.append(request_date);
  dest.append("\n");

  if (credential_scope)
    dest.append(credential_scope);
  dest.append("\n");

  if (hashed_qr)
    dest.append(hashed_qr);

  return dest;
}

/*
 * create string to sign for signature version 4
 *
 * http://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
 */
std::string get_v4_string_to_sign(CephContext* const cct,
                                  const std::string& algorithm,
                                  const std::string& request_date,
                                  const std::string& credential_scope,
                                  const std::string& hashed_qr)
{
  const auto string_to_sign = \
    rgw_assemble_s3_v4_string_to_sign(
      algorithm.c_str(),
      request_date.c_str(),
      credential_scope.c_str(),
      hashed_qr.c_str());

  ldout(cct, 10) << "string to sign = "
                 << rgw::crypt_sanitize::log_content{string_to_sign.c_str()}
                 << dendl;
  return string_to_sign;
}


static inline std::tuple<boost::string_ref,             /* date */
                         boost::string_ref,             /* region */
                         boost::string_ref>             /* service */
parse_cred_scope(boost::string_ref credential_scope)
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

/*
 * calculate the SigningKey of AWS auth version 4
 */
std::array<unsigned char, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE>
get_v4_signing_key(CephContext* const cct,
                   const std::string& credential_scope,
                   const std::string& access_key_secret)
{
  std::string secret_key = "AWS4" + access_key_secret;
  char secret_k[secret_key.size() * MAX_UTF8_SZ];

  size_t n = 0;

  for (size_t i = 0; i < secret_key.size(); i++) {
    n += encode_utf8(secret_key[i], (unsigned char *) (secret_k + n));
  }

  string secret_key_utf8_k(secret_k, n);

  boost::string_ref date, region, service;
  std::tie(date, region, service) = parse_cred_scope(credential_scope);

  const auto date_k = calc_hmac_sha256(secret_key_utf8_k.c_str(),
                                       secret_key_utf8_k.size(),
                                       date.data(), date.size());
  const auto region_k = calc_hmac_sha256(date_k, region);
  const auto service_k = calc_hmac_sha256(region_k, service);

  /* aws4_request */
  const auto signing_key = calc_hmac_sha256(service_k,
                                            boost::string_ref("aws4_request"));

  ldout(cct, 10) << "date_k    = " << buf_to_hex(date_k).data() << dendl;
  ldout(cct, 10) << "region_k  = " << buf_to_hex(region_k).data() << dendl;
  ldout(cct, 10) << "service_k = " << buf_to_hex(service_k).data() << dendl;
  ldout(cct, 10) << "signing_k = " << buf_to_hex(signing_key).data() << dendl;

  return signing_key;
}

/*
 * calculate the AWS signature version 4

 * http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
 */
std::string get_v4_signature(CephContext* const cct,
                             const std::array<unsigned char, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE>& signing_key,
                             const std::string& string_to_sign)
{

  /* new signature */

  char signature_k[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  /* FIXME(rzarzynski): eradicate the reinterpret_cast. */
  calc_hmac_sha256(reinterpret_cast<const char*>(signing_key.data()), CEPH_CRYPTO_HMACSHA256_DIGESTSIZE,
                   string_to_sign.c_str(), string_to_sign.size(),
                   signature_k);

  char aux[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE * 2 + 1];
  buf_to_hex((unsigned char *) signature_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, aux);

  ldout(cct, 10) << "signature_k   = " << string(aux) << dendl;

  std::string signature = string(aux);

  ldout(cct, 10) << "new signature = " << signature << dendl;

  return signature;
}

} /* namespace s3 */
} /* namespace auth */
} /* namespace rgw */
