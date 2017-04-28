// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <map>
#include <string>

#include "common/armor.h"
#include "common/utf8.h"
#include "rgw_auth_s3.h"
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
                      std::string& access_key_id,       /* out */
                      std::string& credential_scope,    /* out */
                      std::string& signedheaders,       /* out */
                      std::string& signature,           /* out */
                      std::string& date,                /* out */
                      bool& using_qs)                   /* out */
{
  const char* const http_auth = info.env->get("HTTP_AUTHORIZATION");
  using_qs = http_auth == nullptr || http_auth[0] == '\0';

  int ret;
  std::string credential;
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

  /* grab access key id */
  const size_t pos = credential.find("/");
  access_key_id = credential.substr(0, pos);
  dout(10) << "access key id = " << access_key_id << dendl;

  /* grab credential scope */
  credential_scope = credential.substr(pos + 1);
  dout(10) << "credential scope = " << credential_scope << dendl;

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
 *
 * http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
 */
std::string get_v4_canon_req_hash(CephContext* cct,
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

bool AWSv4Completer::ChunkMeta::is_new_chunk_in_stream(size_t stream_pos) const
{
  return stream_pos >= (data_offset_in_stream + data_length);
}

size_t AWSv4Completer::ChunkMeta::get_data_size(size_t stream_pos) const
{
  if (stream_pos > (data_offset_in_stream + data_length)) {
    /* Data in parsing_buf. */
    return data_length;
  } else {
    return data_offset_in_stream + data_length - stream_pos;
  }
}

std::pair<AWSv4Completer::ChunkMeta, size_t /* consumed */>
AWSv4Completer::ChunkMeta::create_next(ChunkMeta&& old,
                                       const char* const metabuf,
                                       const size_t metabuf_len)
{
  boost::string_ref metastr(metabuf, metabuf_len);

  const size_t semicolon_pos = metastr.find(";");
  if (semicolon_pos == boost::string_ref::npos) {
    dout(20) << "AWSv4Completer cannot find the ';' separator" << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  char* data_field_end;
  /* strtoull ignores the "\r\n" sequence after each non-first chunk. */
  const size_t data_length = std::strtoull(metabuf, &data_field_end, 16);
  if (data_length == 0 && data_field_end == metabuf) {
    dout(20) << "AWSv4Completer: cannot parse the data size" << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  /* Parse the chunk_signature=... part. */
  const auto signature_part = metastr.substr(semicolon_pos + 1);
  const size_t eq_sign_pos = signature_part.find("=");
  if (eq_sign_pos == boost::string_ref::npos) {
    dout(20) << "AWSv4Completer: cannot find the '=' separator" << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  /* OK, we have at least the beginning of a signature. */
  const size_t data_sep_pos = signature_part.find("\r\n");
  if (data_sep_pos == boost::string_ref::npos) {
    dout(20) << "AWSv4Completer: no new line at signature end" << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  const auto signature = \
    signature_part.substr(eq_sign_pos + 1, data_sep_pos - 1 - eq_sign_pos);
  if (signature.length() != SIG_SIZE) {
    dout(20) << "AWSv4Completer: signature.length() != 64" << dendl;
    throw rgw::io::Exception(EINVAL, std::system_category());
  }

  const size_t data_starts_in_stream = \
    + semicolon_pos + strlen(";") + data_sep_pos  + strlen("\r\n")
    + old.data_offset_in_stream + old.data_length;

  dout(20) << "parsed new chunk; signature=" << signature
           << ", data_length=" << data_length
           << ", data_starts_in_stream=" << data_starts_in_stream
           << dendl;

  return std::make_pair(ChunkMeta(data_starts_in_stream,
                                  data_length,
                                  signature),
                        semicolon_pos + 83);
}

std::string
AWSv4Completer::calc_chunk_signature(const std::string& payload_hash) const
{
  if (!signing_key) {
    return std::string();
  }

  std::string string_to_sign = "AWS4-HMAC-SHA256-PAYLOAD\n";

  string_to_sign.append(date + "\n");
  string_to_sign.append(credential_scope + "\n");
  string_to_sign.append(seed_signature + "\n");
  string_to_sign.append(std::string(AWS4_EMPTY_PAYLOAD_HASH) + "\n");
  string_to_sign.append(payload_hash);

  dout(20) << "AWSv4Completer: string_to_sign=\n" << string_to_sign << dendl;
  /* new chunk signature */
  const auto sighex = buf_to_hex(calc_hmac_sha256(*signing_key,
                                                  string_to_sign));
  /* FIXME(rzarzynski): std::string here is really unnecessary. */
  return std::string(sighex.data(), sighex.size() - 1);
}


bool AWSv4Completer::is_signature_mismatched()
{
  /* The validity of previous chunk can be verified only after getting meta-
   * data of the next one. */
  const auto payload_hash = calc_hash_sha256_restart_stream(&sha256_hash);
  const auto calc_signature = calc_chunk_signature(payload_hash);

  if (chunk_meta.get_signature() != calc_signature) {
    dout(20) << "AWSv4Completer: chunk signature mismatch" << dendl;
    dout(20) << "AWSv4Completer: declared signature="
             << chunk_meta.get_signature() << dendl;
    dout(20) << "AWSv4Completer: calculated signature="
             << calc_signature << dendl;

    return true;
  } else {
    seed_signature = chunk_meta.get_signature();
    return false;
  }
}

size_t AWSv4Completer::recv_chunked(char* const buf, const size_t buf_max)
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
        break;
      }

      stream_pos += received;
      to_extract -= received;
    } while (to_extract > 0);

    size_t consumed;
    std::tie(chunk_meta, consumed) = \
      ChunkMeta::create_next(std::move(chunk_meta),
                                 parsing_buf.data(), parsing_buf.size());

    /* We can drop the bytes consumed during metadata parsing. The remainder
     * can be chunk's data plus possibly beginning of next chunks' metadata. */
    parsing_buf.erase(std::begin(parsing_buf),
                      std::begin(parsing_buf) + consumed);
  }

  size_t to_extract = \
    std::min(chunk_meta.get_data_size(stream_pos), buf_max);
  
  /* It's quite probable we have a couple of real data bytes stored together
   * with meta-data in the parsing_buf. We need to extract them and move to
   * the final buffer. This is a trade-off between frontend's read overhead
   * and memcpy. */
  if (to_extract > 0 && parsing_buf.size() > 0) {
    const auto data_len = std::min(to_extract, parsing_buf.size());
    const auto data_end_iter = std::begin(parsing_buf) + data_len;

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

    if (received == 0) {
      break;
    }

    calc_hash_sha256_update_stream(sha256_hash, buf + buf_pos, received);

    buf_pos += received;
    stream_pos += received;
    to_extract -= received;
  }

  dout(20) << "AWSv4Completer: filled=" << buf_pos << dendl;
  return buf_pos;
}

size_t AWSv4Completer::recv_body(char* const buf, const size_t max)
{
  if (aws4_auth_streaming_mode) {
    return recv_chunked(buf, max);
  }

  const auto received = io_base_t::recv_body(buf, max);

  if (sha256_hash) {
    calc_hash_sha256_update_stream(sha256_hash, buf, received);
  }

  return received;
}

void AWSv4Completer::modify_request_state(req_state* const s_rw)
{
  /* TODO(rzarzynski): switch to the dedicated filter over RestfulClient. */
  //s_rw->aws4_auth_streaming_mode = aws4_auth_streaming_mode;

  s_rw->aws4_auth = std::unique_ptr<rgw_aws4_auth>(new rgw_aws4_auth);

  s_rw->aws4_auth->date = date;
  s_rw->aws4_auth->credential_scope = credential_scope;

  /* If we're here, the provided signature has been successfully validated
   * earlier. */
  s_rw->aws4_auth->seed_signature = seed_signature;

  if (signing_key) {
    s_rw->aws4_auth->signing_key = std::move(*signing_key);
  } else if (aws4_auth_streaming_mode) {
    /* Some external authorizers (like Keystone) aren't fully compliant with
     * AWSv4. They do not provide the secret_key which is necessary to handle
     * the streamed upload. */
    throw -ERR_NOT_IMPLEMENTED;
  }

  /* Install the filter over rgw::io::RestfulClient. */
  AWS_AUTHv4_IO(s_rw)->add_filter(
    std::static_pointer_cast<io_base_t>(shared_from_this()));
}

bool AWSv4Completer::complete()
{
  /* Now it's time to verify the signature of the last, zero-length chunk. */
  if (aws4_auth_streaming_mode && is_signature_mismatched()) {
    return false;
  }

  if (!aws4_auth_needs_complete) {
    return true;
  }

  /* In AWSv4 the hash of real, transfered payload IS NOT necessary to form
   * a Canonical Request, and thus verify a Signature. x-amz-content-sha256
   * header lets get the information very early -- before seeing first byte
   * of HTTP body. As a consequence, we can decouple Signature verification
   * from payload's fingerprint check. */
  const char *expected_request_payload_hash = \
    rgw::auth::s3::get_v4_exp_payload_hash(s->info);

  /* The completer is only for the cases where signed payload has been
   * requested. It won't be used, for instance, during the query string-based
   * authentication. */
  const auto payload_hash = calc_hash_sha256_close_stream(&sha256_hash);

  /* Validate x-amz-sha256 */
  if (payload_hash.compare(expected_request_payload_hash) == 0) {
    return true;
  } else {
    ldout(s->cct, 10) << "ERROR: x-amz-content-sha256 does not match"
                      << dendl;
    ldout(s->cct, 10) << "ERROR:   grab_aws4_sha256_hash()="
                      << payload_hash << dendl;
    ldout(s->cct, 10) << "ERROR:   expected_request_payload_hash="
                      << expected_request_payload_hash << dendl;
    return false;
  }
}

rgw::auth::Completer::cmplptr_t
AWSv4Completer::create_for_single_chunk(const req_state* const s,
                                        const boost::optional<std::string>&)
{
  return rgw::auth::Completer::cmplptr_t(new AWSv4Completer(s));
}

rgw::auth::Completer::cmplptr_t
AWSv4Completer::create_for_multi_chunk(const req_state* const s,
                                       std::string date,
                                       std::string credential_scope,
                                       std::string seed_signature,
                                       const boost::optional<std::string>& secret_key)
{
  boost::optional<std::array<unsigned char,
                  CEPH_CRYPTO_HMACSHA256_DIGESTSIZE>> signing_key;
  if (secret_key) {
    signing_key = rgw::auth::s3::get_v4_signing_key(s->cct, credential_scope,
                                                    *secret_key);
  }

  return rgw::auth::Completer::cmplptr_t(
    new AWSv4Completer(s,
                       std::move(date),
                       std::move(credential_scope),
                       std::move(seed_signature),
                       signing_key));
}

} /* namespace s3 */
} /* namespace auth */
} /* namespace rgw */
