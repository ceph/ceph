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

int rgw_get_s3_header_digest(const string& auth_hdr, const string& key, string& dest)
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

void rgw_hash_s3_string_sha256(const char *data, int len, string& dest)
{
  calc_hash_sha256(data, len, dest);
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

/*
 * assemble canonical request for signature version 4
 */
void rgw_assemble_s3_v4_canonical_request(const char *method, const char *canonical_uri, const char *canonical_qs,
                                          const char *canonical_hdrs, const char *signed_hdrs, const char *request_payload_hash,
                                          string& dest_str)
{
  string dest;

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

  dest_str = dest;
}

/*
 * create canonical request for signature version 4
 */
void rgw_create_s3_v4_canonical_request(struct req_state *s, const string& canonical_uri, const string& canonical_qs,
                                        const string& canonical_hdrs, const string& signed_hdrs, const string& request_payload,
                                        bool unsigned_payload, string& canonical_req, string& canonical_req_hash)
{
  string request_payload_hash;

  if (unsigned_payload) {
    request_payload_hash = "UNSIGNED-PAYLOAD";
  } else {
    if (s->aws4_auth_needs_complete) {
      request_payload_hash = AWS_AUTHv4_IO(s)->grab_aws4_sha256_hash();
    } else {
      if (s->aws4_auth_streaming_mode) {
        request_payload_hash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
      } else {
        rgw_hash_s3_string_sha256(request_payload.c_str(), request_payload.size(), request_payload_hash);
      }
    }
  }

  s->aws4_auth->payload_hash = request_payload_hash;

  ldout(s->cct, 10) << "payload request hash = " << request_payload_hash << dendl;

  rgw_assemble_s3_v4_canonical_request(s->info.method, canonical_uri.c_str(),
      canonical_qs.c_str(), canonical_hdrs.c_str(), signed_hdrs.c_str(),
      request_payload_hash.c_str(), canonical_req);

  rgw_hash_s3_string_sha256(canonical_req.c_str(), canonical_req.size(), canonical_req_hash);

  ldout(s->cct, 10) << "canonical request = " << canonical_req << dendl;
  ldout(s->cct, 10) << "canonical request hash = " << canonical_req_hash << dendl;
}

/*
 * assemble string to sign for signature version 4
 */
void rgw_assemble_s3_v4_string_to_sign(const char *algorithm, const char *request_date,
                                       const char *credential_scope, const char *hashed_qr, string& dest_str)
{
  string dest;

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

  dest_str = dest;
}

/*
 * create string to sign for signature version 4
 */
void rgw_create_s3_v4_string_to_sign(CephContext *cct, const string& algorithm, const string& request_date,
                                     const string& credential_scope, const string& hashed_qr,
                                     string& string_to_sign) {

  rgw_assemble_s3_v4_string_to_sign(algorithm.c_str(), request_date.c_str(),
      credential_scope.c_str(), hashed_qr.c_str(), string_to_sign);

  ldout(cct, 10) << "string to sign = " << rgw::crypt_sanitize::log_content{string_to_sign.c_str()} << dendl;
}

/*
 * calculate the AWS signature version 4
 */
int rgw_calculate_s3_v4_aws_signature(struct req_state *s,
    const string& access_key_id, const string &date, const string& region,
    const string& service, const string& string_to_sign, string& signature) {

  map<string, RGWAccessKey>::iterator iter = s->user->access_keys.find(access_key_id);
  if (iter == s->user->access_keys.end()) {
    ldout(s->cct, 10) << "ERROR: access key not encoded in user info" << dendl;
    return -EPERM;
  }

  RGWAccessKey& k = iter->second;

  string secret_key = "AWS4" + k.key;

  char secret_k[secret_key.size() * MAX_UTF8_SZ];

  size_t n = 0;

  for (size_t i = 0; i < secret_key.size(); i++) {
    n += encode_utf8(secret_key[i], (unsigned char *) (secret_k + n));
  }

  string secret_key_utf8_k(secret_k, n);

  /* date */

  char date_k[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  calc_hmac_sha256(secret_key_utf8_k.c_str(), secret_key_utf8_k.size(),
      date.c_str(), date.size(), date_k);

  char aux[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE * 2 + 1];
  buf_to_hex((unsigned char *) date_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, aux);

  ldout(s->cct, 10) << "date_k        = " << string(aux) << dendl;

  /* region */

  char region_k[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  calc_hmac_sha256(date_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, region.c_str(), region.size(), region_k);

  buf_to_hex((unsigned char *) region_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, aux);

  ldout(s->cct, 10) << "region_k      = " << string(aux) << dendl;

  /* service */

  char service_k[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  calc_hmac_sha256(region_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, service.c_str(), service.size(), service_k);

  buf_to_hex((unsigned char *) service_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, aux);

  ldout(s->cct, 10) << "service_k     = " << string(aux) << dendl;

  /* aws4_request */

  char *signing_k = s->aws4_auth->signing_k;

  calc_hmac_sha256(service_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, "aws4_request", 12, signing_k);

  buf_to_hex((unsigned char *) signing_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, aux);

  ldout(s->cct, 10) << "signing_k     = " << string(aux) << dendl;

  s->aws4_auth->signing_key = aux;

  /* new signature */

  char signature_k[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  calc_hmac_sha256(signing_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, string_to_sign.c_str(), string_to_sign.size(), signature_k);

  buf_to_hex((unsigned char *) signature_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, aux);

  ldout(s->cct, 10) << "signature_k   = " << string(aux) << dendl;

  signature = string(aux);

  ldout(s->cct, 10) << "new signature = " << signature << dendl;

  return 0;
}
