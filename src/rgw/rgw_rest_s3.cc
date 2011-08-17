#include <errno.h>
#include <string.h>

#include "common/ceph_crypto.h"
#include "common/Formatter.h"

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_acl.h"

#include "common/armor.h"


using namespace ceph::crypto;

void list_all_buckets_start(struct req_state *s)
{
  s->formatter->open_array_section_in_ns("ListAllMyBucketsResult",
			      "http://doc.s3.amazonaws.com/2006-03-01");
}

void list_all_buckets_end(struct req_state *s)
{
  s->formatter->close_section();
}

void dump_bucket(struct req_state *s, RGWBucketEnt& obj)
{
  s->formatter->open_object_section("Bucket");
  s->formatter->dump_format("Name", obj.name.c_str());
  dump_time(s, "CreationDate", &obj.mtime);
  s->formatter->close_section();
}

int RGWGetObj_REST_S3::send_response(void *handle)
{
  const char *content_type = NULL;
  int orig_ret = ret;

  if (ret)
    goto done;

  if (sent_header)
    goto send_data;

  if (range_str)
    dump_range(s, start, end, s->obj_size);

  dump_content_length(s, total_len);
  dump_last_modified(s, lastmod);

  if (!ret) {
    map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_ETAG);
    if (iter != attrs.end()) {
      bufferlist& bl = iter->second;
      if (bl.length()) {
        char *etag = bl.c_str();
        dump_etag(s, etag);
      }
    }

    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
       const char *name = iter->first.c_str();
       if (strncmp(name, RGW_ATTR_META_PREFIX, sizeof(RGW_ATTR_META_PREFIX)-1) == 0) {
         name += sizeof(RGW_ATTR_PREFIX) - 1;
         CGI_PRINTF(s,"%s: %s\r\n", name, iter->second.c_str());
       } else if (!content_type && strcmp(name, RGW_ATTR_CONTENT_TYPE) == 0) {
         content_type = iter->second.c_str();
       }
    }
  }

  if (range_str && !ret)
    ret = 206; /* partial content */
done:
  set_req_state_err(s, ret);

  dump_errno(s);
  if (!content_type)
    content_type = "binary/octet-stream";
  end_header(s, content_type);
  sent_header = true;

send_data:
  if (get_data && !orig_ret) {
    CGI_PutStr(s, data, len);
  }

  return 0;
}

void RGWListBuckets_REST_S3::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  dump_start(s);

  list_all_buckets_start(s);
  dump_owner(s, s->user.user_id, s->user.display_name);

  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  map<string, RGWBucketEnt>::iterator iter;

  s->formatter->open_array_section("Buckets");
  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt obj = iter->second;
    dump_bucket(s, obj);
  }
  s->formatter->close_section();
  list_all_buckets_end(s);
  dump_content_length(s, s->formatter->get_len());
  end_header(s, "application/xml");
  flush_formatter_to_req_state(s, s->formatter);
}

void RGWListBucket_REST_S3::send_response()
{
  if (ret < 0)
    set_req_state_err(s, ret);
  dump_errno(s);

  end_header(s, "application/xml");
  dump_start(s);
  if (ret < 0)
    return;

  s->formatter->open_object_section("ListBucketResult");
  s->formatter->dump_format("Name", s->bucket);
  if (!prefix.empty())
    s->formatter->dump_format("Prefix", prefix.c_str());
  s->formatter->dump_format("Marker", marker.c_str());
  s->formatter->dump_int("MaxKeys", max);
  if (!delimiter.empty())
    s->formatter->dump_format("Delimiter", delimiter.c_str());

  s->formatter->dump_format("IsTruncated", (max && is_truncated ? "true" : "false"));

  if (ret >= 0) {
    vector<RGWObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      s->formatter->open_array_section("Contents");
      s->formatter->dump_format("Key", iter->name.c_str());
      dump_time(s, "LastModified", &iter->mtime);
      s->formatter->dump_format("ETag", "\"%s\"", iter->etag);
      s->formatter->dump_int("Size", iter->size);
      s->formatter->dump_format("StorageClass", "STANDARD");
      dump_owner(s, iter->owner, iter->owner_display_name);
      s->formatter->close_section();
    }
    if (common_prefixes.size() > 0) {
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin(); pref_iter != common_prefixes.end(); ++pref_iter) {
        s->formatter->open_array_section("CommonPrefixes");
        s->formatter->dump_format("Prefix", pref_iter->first.c_str());
        s->formatter->close_section();
      }
    }
  }
  s->formatter->close_section();
  flush_formatter_to_req_state(s, s->formatter);
}

void RGWCreateBucket_REST_S3::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
}

void RGWDeleteBucket_REST_S3::send_response()
{
  int r = ret;
  if (!r)
    r = 204;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
}

void RGWPutObj_REST_S3::send_response()
{
  if (ret) {
    set_req_state_err(s, ret);
  } else {
    dump_etag(s, etag.c_str());
    dump_content_length(s, 0);
  }
  dump_errno(s);
  end_header(s);
}

void RGWDeleteObj_REST_S3::send_response()
{
  int r = ret;
  if (!r)
    r = 204;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
}

void RGWCopyObj_REST_S3::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);

  end_header(s, "binary/octet-stream");
  if (ret == 0) {
    s->formatter->open_object_section("CopyObjectResult");
    dump_time(s, "LastModified", &mtime);
    map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_ETAG);
    if (iter != attrs.end()) {
      bufferlist& bl = iter->second;
      if (bl.length()) {
        char *etag = bl.c_str();
        s->formatter->dump_format("ETag", etag);
      }
    }
    s->formatter->close_section();
    flush_formatter_to_req_state(s, s->formatter);
  }
}

void RGWGetACLs_REST_S3::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s, "application/xml");
  dump_start(s);
  CGI_PutStr(s, acls.c_str(), acls.size());
}

void RGWPutACLs_REST_S3::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s, "application/xml");
  dump_start(s);
}

void RGWInitMultipart_REST_S3::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s, "application/xml");
  if (ret == 0) { 
    dump_start(s);
    s->formatter->open_object_section_in_ns("InitiateMultipartUploadResult",
		  "http://s3.amazonaws.com/doc/2006-03-01/");
    s->formatter->dump_format("Bucket", s->bucket);
    s->formatter->dump_format("Key", s->object);
    s->formatter->dump_format("UploadId", upload_id.c_str());
    s->formatter->close_section();
    flush_formatter_to_req_state(s, s->formatter);
  }
}

void RGWCompleteMultipart_REST_S3::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s, "application/xml");
  if (ret == 0) { 
    dump_start(s);
    s->formatter->open_object_section_in_ns("CompleteMultipartUploadResult",
			  "http://s3.amazonaws.com/doc/2006-03-01/");
    const char *gateway_dns_name = s->env->get("RGW_DNS_NAME");
    if (gateway_dns_name)
      s->formatter->dump_format("Location", "%s.%s", s->bucket, gateway_dns_name);
    s->formatter->dump_format("Bucket", s->bucket);
    s->formatter->dump_format("Key", s->object);
    s->formatter->dump_format("ETag", etag.c_str());
    s->formatter->close_section();
    flush_formatter_to_req_state(s, s->formatter);
  }
}

void RGWAbortMultipart_REST_S3::send_response()
{
  int r = ret;
  if (!r)
    r = 204;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
}

void RGWListMultipart_REST_S3::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s, "application/xml");

  if (ret == 0) { 
    dump_start(s);
    s->formatter->open_object_section_in_ns("ListMultipartUploadResult",
		    "http://s3.amazonaws.com/doc/2006-03-01/");
    map<uint32_t, RGWUploadPartInfo>::iterator iter, test_iter;
    int i, cur_max = 0;

    iter = parts.upper_bound(marker);
    for (i = 0, test_iter = iter; test_iter != parts.end() && i < max_parts; ++test_iter, ++i) {
      cur_max = test_iter->first;
    }
    s->formatter->dump_format("Bucket", s->bucket);
    s->formatter->dump_format("Key", s->object);
    s->formatter->dump_format("UploadId", upload_id.c_str());
    s->formatter->dump_format("StorageClass", "STANDARD");
    s->formatter->dump_format("PartNumberMarker", "%d", marker);
    s->formatter->dump_format("NextPartNumberMarker", "%d", cur_max + 1);
    s->formatter->dump_format("MaxParts", "%d", max_parts);
    s->formatter->dump_format("IsTruncated", "%s", (test_iter == parts.end() ? "false" : "true"));

    ACLOwner& owner = policy.get_owner();
    dump_owner(s, owner.get_id(), owner.get_display_name());

    for (; iter != parts.end(); ++iter) {
      RGWUploadPartInfo& info = iter->second;

      time_t sec = info.modified.sec();
      struct tm tmp;
      gmtime_r(&sec, &tmp);
      char buf[TIME_BUF_SIZE];
      if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", &tmp) > 0) {
        s->formatter->dump_format("LastModified", buf);
      }

      s->formatter->open_object_section("Part");
      s->formatter->dump_unsigned("PartNumber", info.num);
      s->formatter->dump_format("ETag", "%s", info.etag.c_str());
      s->formatter->dump_unsigned("Size", info.size);
      s->formatter->close_section();
    }
    s->formatter->close_section();
    flush_formatter_to_req_state(s, s->formatter);
  }
}

void RGWListBucketMultiparts_REST_S3::send_response()
{
  if (ret < 0)
    set_req_state_err(s, ret);
  dump_errno(s);

  end_header(s, "application/xml");
  dump_start(s);
  if (ret < 0)
    return;

  s->formatter->open_object_section("ListMultipartUploadsResult");
  s->formatter->dump_format("Bucket", s->bucket);
  if (!prefix.empty())
    s->formatter->dump_format("ListMultipartUploadsResult.Prefix", prefix.c_str());
  string& key_marker = marker.get_key();
  if (!key_marker.empty())
    s->formatter->dump_format("KeyMarker", key_marker.c_str());
  string& upload_id_marker = marker.get_upload_id();
  if (!upload_id_marker.empty())
    s->formatter->dump_format("UploadIdMarker", upload_id_marker.c_str());
  string next_key = next_marker.mp.get_key();
  if (!next_key.empty())
    s->formatter->dump_format("NextKeyMarker", next_key.c_str());
  string next_upload_id = next_marker.mp.get_upload_id();
  if (!next_upload_id.empty())
    s->formatter->dump_format("NextUploadIdMarker", next_upload_id.c_str());
  s->formatter->dump_format("MaxUploads", "%d", max_uploads);
  if (!delimiter.empty())
    s->formatter->dump_format("Delimiter", delimiter.c_str());
  s->formatter->dump_format("IsTruncated", (is_truncated ? "true" : "false"));

  if (ret >= 0) {
    vector<RGWMultipartUploadEntry>::iterator iter;
    for (iter = uploads.begin(); iter != uploads.end(); ++iter) {
      RGWMPObj& mp = iter->mp;
      s->formatter->open_array_section("Upload");
      s->formatter->dump_format("Key", mp.get_key().c_str());
      s->formatter->dump_format("UploadId", mp.get_upload_id().c_str());
      dump_owner(s, s->user.user_id, s->user.display_name, "Initiator");
      dump_owner(s, s->user.user_id, s->user.display_name);
      s->formatter->dump_format("StorageClass", "STANDARD");
      dump_time(s, "Initiated", &iter->obj.mtime);
      s->formatter->close_section();
    }
    if (common_prefixes.size() > 0) {
      s->formatter->open_array_section("CommonPrefixes");
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin(); pref_iter != common_prefixes.end(); ++pref_iter) {
        s->formatter->dump_format("CommonPrefixes.Prefix", pref_iter->first.c_str());
      }
      s->formatter->close_section();
    }
  }
  s->formatter->close_section();
  flush_formatter_to_req_state(s, s->formatter);
}

RGWOp *RGWHandler_REST_S3::get_retrieve_obj_op(bool get_data)
{
  if (is_acl_op()) {
    return new RGWGetACLs_REST_S3;
  }
  if (s->object) {
    RGWGetObj_REST_S3 *get_obj_op = new RGWGetObj_REST_S3;
    get_obj_op->set_get_data(get_data);
    return get_obj_op;
  } else if (!s->bucket) {
    return NULL;
  }

  if (s->args.exists("uploads"))
    return new RGWListBucketMultiparts_REST_S3;

  return new RGWListBucket_REST_S3;
}

RGWOp *RGWHandler_REST_S3::get_retrieve_op(bool get_data)
{
  if (s->bucket) {
    if (is_acl_op()) {
      return new RGWGetACLs_REST_S3;
    } else if (s->args.exists("uploadId")) {
      return new RGWListMultipart_REST_S3;
    }
    return get_retrieve_obj_op(get_data);
  }

  return new RGWListBuckets_REST_S3;
}

RGWOp *RGWHandler_REST_S3::get_create_op()
{
  if (is_acl_op()) {
    return new RGWPutACLs_REST_S3;
  } else if (s->object) {
    if (!s->copy_source)
      return new RGWPutObj_REST_S3;
    else
      return new RGWCopyObj_REST_S3;
  } else if (s->bucket) {
    return new RGWCreateBucket_REST_S3;
  }

  return NULL;
}

RGWOp *RGWHandler_REST_S3::get_delete_op()
{
  string upload_id = s->args.get("uploadId");

  if (s->object) {
    if (upload_id.empty())
      return new RGWDeleteObj_REST_S3;
    else
      return new RGWAbortMultipart_REST_S3;
  } else if (s->bucket)
    return new RGWDeleteBucket_REST_S3;

  return NULL;
}

RGWOp *RGWHandler_REST_S3::get_post_op()
{
  if (s->object) {
    if (s->args.exists("uploadId"))
      return new RGWCompleteMultipart_REST_S3;
    else
      return new RGWInitMultipart_REST_S3;
  }

  return NULL;
}

/*
 * ?get the canonical amazon-style header for something?
 */

static void get_canon_amz_hdr(struct req_state *s, string& dest)
{
  dest = "";
  map<string, string>::iterator iter;
  for (iter = s->x_amz_map.begin(); iter != s->x_amz_map.end(); ++iter) {
    dest.append(iter->first);
    dest.append(":");
    dest.append(iter->second);
    dest.append("\n");
  }
}

/*
 * ?get the canonical representation of the object's location
 */
static void get_canon_resource(struct req_state *s, string& dest)
{
  if (s->host_bucket) {
    dest = "/";
    dest.append(s->host_bucket);
  }

  dest.append(s->path_name_url.c_str());

  map<string, string>& sub = s->args.get_sub_resources();
  map<string, string>::iterator iter;
  for (iter = sub.begin(); iter != sub.end(); ++iter) {
    if (iter == sub.begin())
      dest.append("?");
    else
      dest.append("&");     
    dest.append(iter->first);
    if (!iter->second.empty()) {
      dest.append("=");
      dest.append(iter->second);
    }
  }
  RGW_LOG(10) << "get_canon_resource(): dest=" << dest << dendl;
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

static bool parse_rfc850(const char *s, struct tm *t)
{
  return check_str_end(strptime(s, "%A, %d-%b-%y %H:%M:%S GMT", t));
}

static bool parse_asctime(const char *s, struct tm *t)
{
  return check_str_end(strptime(s, "%a %b %d %H:%M:%S %Y", t));
}

static bool parse_rfc1123(const char *s, struct tm *t)
{
  return check_str_end(strptime(s, "%a, %d %b %Y %H:%M:%S GMT", t));
}

static bool parse_rfc2616(const char *s, struct tm *t)
{
  return parse_rfc850(s, t) || parse_asctime(s, t) || parse_rfc1123(s, t);
}

static inline bool is_base64_for_content_md5(unsigned char c) {
  return (isalnum(c) || isspace(c) || (c == '+') || (c == '/') || (c == '='));
}

/*
 * get the header authentication  information required to
 * compute a request's signature
 */
static bool get_auth_header(struct req_state *s, string& dest, bool qsr)
{
  dest = "";
  if (s->method)
    dest = s->method;
  dest.append("\n");
  
  const char *md5 = s->env->get("HTTP_CONTENT_MD5");
  if (md5) {
    for (const char *p = md5; *p; p++) {
      if (!is_base64_for_content_md5(*p)) {
        RGW_LOG(0) << "bad content-md5 provided (not base64), aborting request p=" << *p << " " << (int)*p << dendl;
        return false;
      }
    }
    dest.append(md5);
  }
  dest.append("\n");

  const char *type = s->env->get("CONTENT_TYPE");
  if (type)
    dest.append(type);
  dest.append("\n");

  string date;
  if (qsr) {
    date = s->args.get("Expires");
  } else {
    const char *str = s->env->get("HTTP_DATE");
    const char *req_date = str;
    if (str) {
      date = str;
    } else {
      req_date = s->env->get("HTTP_X_AMZ_DATE");
      if (!req_date) {
        RGW_LOG(0) << "missing date for auth header" << dendl;
        return false;
      }
    }

    struct tm t;
    if (!parse_rfc2616(req_date, &t)) {
      RGW_LOG(0) << "failed to parse date for auth header" << dendl;
      return false;
    }
    if (t.tm_year < 70) {
      RGW_LOG(0) << "bad date (predates epoch): " << req_date << dendl;
      return false;
    }
    s->header_time = utime_t(timegm(&t), 0);
  }

  if (date.size())
      dest.append(date);
  dest.append("\n");

  string canon_amz_hdr;
  get_canon_amz_hdr(s, canon_amz_hdr);
  dest.append(canon_amz_hdr);

  string canon_resource;
  get_canon_resource(s, canon_resource);
  dest.append(canon_resource);

  return true;
}

/*
 * verify that a signed request comes from the keyholder
 * by checking the signature against our locally-computed version
 */
int RGWHandler_REST_S3::authorize()
{
  bool qsr = false;
  string auth_id;
  string auth_sign;

  time_t now;
  time(&now);

  if (!s->http_auth || !(*s->http_auth)) {
    auth_id = s->args.get("AWSAccessKeyId");
    if (auth_id.size()) {
      url_decode(s->args.get("Signature"), auth_sign);

      string date = s->args.get("Expires");
      time_t exp = atoll(date.c_str());
      if (now >= exp)
        return -EPERM;

      qsr = true;
    } else {
      /* anonymous access */
      rgw_get_anon_user(s->user);
      s->perm_mask = RGW_PERM_FULL_CONTROL;
      return 0;
    }
  } else {
    if (strncmp(s->http_auth, "AWS ", 4))
      return -EINVAL;
    string auth_str(s->http_auth + 4);
    int pos = auth_str.find(':');
    if (pos < 0)
      return -EINVAL;

    auth_id = auth_str.substr(0, pos);
    auth_sign = auth_str.substr(pos + 1);
  }

  /* first get the user info */
  if (rgw_get_user_info_by_access_key(auth_id, s->user) < 0) {
    RGW_LOG(5) << "error reading user info, uid=" << auth_id << " can't authenticate" << dendl;
    return -EPERM;
  }

  /* now verify signature */
   
  string auth_hdr;
  if (!get_auth_header(s, auth_hdr, qsr)) {
    RGW_LOG(10) << "failed to create auth header\n" << auth_hdr << dendl;
    return -EPERM;
  }
  RGW_LOG(10) << "auth_hdr:\n" << auth_hdr << dendl;

  time_t req_sec = s->header_time.sec();
  if (req_sec < now - RGW_AUTH_GRACE_MINS * 60 ||
      req_sec > now + RGW_AUTH_GRACE_MINS * 60) {
    RGW_LOG(10) << "req_sec=" << req_sec << " now=" << now << "; now - RGW_AUTH_GRACE_MINS=" << now - RGW_AUTH_GRACE_MINS * 60 << "; now + RGW_AUTH_GRACE_MINS=" << now + RGW_AUTH_GRACE_MINS * 60 << dendl;
    RGW_LOG(0) << "request time skew too big now=" << utime_t(now, 0) << " req_time=" << s->header_time << dendl;
    return -ERR_REQUEST_TIME_SKEWED;
  }

  map<string, RGWAccessKey>::iterator iter = s->user.access_keys.find(auth_id);
  if (iter == s->user.access_keys.end()) {
    RGW_LOG(0) << "ERROR: access key not encoded in user info" << dendl;
    return -EPERM;
  }
  RGWAccessKey& k = iter->second;
  const char *key = k.key.c_str();
  int key_len = k.key.size();

  if (!k.subuser.empty()) {
    map<string, RGWSubUser>::iterator uiter = s->user.subusers.find(k.subuser);
    if (uiter == s->user.subusers.end()) {
      RGW_LOG(0) << "ERROR: could not find subuser: " << k.subuser << dendl;
      return -EPERM;
    }
    RGWSubUser& subuser = uiter->second;
    s->perm_mask = subuser.perm_mask;
  } else
    s->perm_mask = RGW_PERM_FULL_CONTROL;

  char hmac_sha1[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  calc_hmac_sha1(key, key_len, auth_hdr.c_str(), auth_hdr.size(), hmac_sha1);

  char b64[64]; /* 64 is really enough */
  int ret = ceph_armor(b64, b64 + 64, hmac_sha1,
		       hmac_sha1 + CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
  if (ret < 0) {
    RGW_LOG(10) << "ceph_armor failed" << dendl;
    return -EPERM;
  }
  b64[ret] = '\0';

  RGW_LOG(15) << "b64=" << b64 << dendl;
  RGW_LOG(15) << "auth_sign=" << auth_sign << dendl;
  RGW_LOG(15) << "compare=" << auth_sign.compare(b64) << dendl;
  if (auth_sign.compare(b64) != 0)
    return -EPERM;

  return  0;
}


