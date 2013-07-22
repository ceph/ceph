
#include "common/armor.h"
#include "rgw_common.h"

#define dout_subsys ceph_subsys_rgw

static const char *signed_subresources[] = {
  "acl",
  "delete",
  "lifecycle",
  "location",
  "logging",
  "notification",
  "partNumber",
  "policy",
  "requestPayment",
  "torrent",
  "uploadId",
  "uploads",
  "versionId",
  "versioning",
  "versions",
  "website",
  NULL
};

/*
 * ?get the canonical amazon-style header for something?
 */

static void get_canon_amz_hdr(map<string, string>& meta_map, string& dest)
{
  dest = "";
  map<string, string>::iterator iter;
  for (iter = meta_map.begin(); iter != meta_map.end(); ++iter) {
    dest.append(iter->first);
    dest.append(":");
    dest.append(iter->second);
    dest.append("\n");
  }
}

/*
 * ?get the canonical representation of the object's location
 */
static void get_canon_resource(const char *request_uri, map<string, string>& sub_resources, string& dest)
{
  string s;

  if (request_uri)
    s.append(request_uri);

  string append_str;

  const char **p = signed_subresources;

  for (; *p; ++p) {
    map<string, string>::iterator iter = sub_resources.find(*p);
    if (iter == sub_resources.end())
      continue;
    
    if (append_str.empty())
      append_str.append("?");
    else
      append_str.append("&");     
    append_str.append(iter->first);
    if (!iter->second.empty()) {
      append_str.append("=");
      append_str.append(iter->second);
    }
  }
  if (!append_str.empty()) {
    s.append(append_str);
  }
  dout(10) << "get_canon_resource(): dest=" << dest << dendl;

  dest = s;
}

/*
 * get the header authentication  information required to
 * compute a request's signature
 */
void rgw_create_s3_canonical_header(const char *method, const char *content_md5, const char *content_type, const char *date,
                            map<string, string>& meta_map, const char *request_uri, map<string, string>& sub_resources,
                            string& dest_str)
{
  string dest;

  if (method)
    dest = method;
  dest.append("\n");
  
  if (content_md5) {
    dest.append(content_md5);
  }
  dest.append("\n");

  if (content_type)
    dest.append(content_type);
  dest.append("\n");

  if (date)
    dest.append(date);
  dest.append("\n");

  string canon_amz_hdr;
  get_canon_amz_hdr(meta_map, canon_amz_hdr);
  dest.append(canon_amz_hdr);

  string canon_resource;
  get_canon_resource(request_uri, sub_resources, canon_resource);

  dest.append(canon_resource);

  dest_str = dest;
}

int rgw_get_s3_header_digest(const string& auth_hdr, const string& key, string& dest)
{
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
bool rgw_create_s3_canonical_header(req_info& info, utime_t *header_time, string& dest, bool qsr)
{
  const char *content_md5 = info.env->get("HTTP_CONTENT_MD5");
  if (content_md5) {
    for (const char *p = content_md5; *p; p++) {
      if (!is_base64_for_content_md5(*p)) {
        dout(0) << "NOTICE: bad content-md5 provided (not base64), aborting request p=" << *p << " " << (int)*p << dendl;
        return false;
      }
    }
  }

  const char *content_type = info.env->get("CONTENT_TYPE");

  string date;
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
      *header_time = utime_t(timegm(&t), 0);
    }
  }

  map<string, string>& meta_map = info.x_meta_map;
  map<string, string>& sub_resources = info.args.get_sub_resources();

  string request_uri;
  if (info.effective_uri.empty())
    request_uri = info.request_uri;
  else
    request_uri = info.effective_uri;

  rgw_create_s3_canonical_header(info.method, content_md5, content_type, date.c_str(),
                            meta_map, request_uri.c_str(), sub_resources,
                            dest);

  return true;
}
