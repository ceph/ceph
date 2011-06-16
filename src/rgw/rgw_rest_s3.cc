#include <errno.h>
#include <string.h>

#include "common/ceph_crypto.h"

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_acl.h"

#include "common/armor.h"


using namespace ceph::crypto;

void list_all_buckets_start(struct req_state *s)
{
  s->formatter->open_array_section("ListAllMyBucketsResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\"");
}

void list_all_buckets_end(struct req_state *s)
{
  s->formatter->close_section("ListAllMyBucketsResult");
}

void dump_bucket(struct req_state *s, RGWBucketEnt& obj)
{
  s->formatter->open_obj_section("Bucket");
  s->formatter->dump_value_str("Name", obj.name.c_str());
  dump_time(s, "CreationDate", &obj.mtime);
  s->formatter->close_section("Bucket");
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
    dump_range(s, ofs, end);

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
  if (orig_ret)
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
  s->formatter->close_section("Buckets");
  list_all_buckets_end(s);
  dump_content_length(s, s->formatter->get_len());
  end_header(s, "application/xml");
  s->formatter->flush();
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

  s->formatter->open_obj_section("ListBucketResult");
  s->formatter->dump_value_str("Name", s->bucket);
  if (!prefix.empty())
    s->formatter->dump_value_str("Prefix", prefix.c_str());
  if (!marker.empty())
    s->formatter->dump_value_str("Marker", marker.c_str());
  if (!max_keys.empty()) {
    s->formatter->dump_value_str("MaxKeys", max_keys.c_str());
  }
  if (!delimiter.empty())
    s->formatter->dump_value_str("Delimiter", delimiter.c_str());

  if (ret >= 0) {
    vector<RGWObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      s->formatter->open_array_section("Contents");
      s->formatter->dump_value_str("Key", iter->name.c_str());
      dump_time(s, "LastModified", &iter->mtime);
      s->formatter->dump_value_str("ETag", "\"%s\"", iter->etag);
      s->formatter->dump_value_int("Size", "%lld", iter->size);
      s->formatter->dump_value_str("StorageClass", "STANDARD");
      dump_owner(s, s->user.user_id, s->user.display_name);
      s->formatter->close_section("Contents");
    }
    if (common_prefixes.size() > 0) {
      s->formatter->open_array_section("CommonPrefixes");
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin(); pref_iter != common_prefixes.end(); ++pref_iter) {
        s->formatter->dump_value_str("Prefix", pref_iter->first.c_str());
      }
      s->formatter->close_section("CommonPrefixes");
    }
  }
  s->formatter->close_section("ListBucketResult");
  s->formatter->flush();
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
  dump_etag(s, etag.c_str());
  if (ret)
    set_req_state_err(s, ret);
  else
    dump_content_length(s, 0);
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
    s->formatter->open_obj_section("CopyObjectResult");
    dump_time(s, "LastModified", &mtime);
    map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_ETAG);
    if (iter != attrs.end()) {
      bufferlist& bl = iter->second;
      if (bl.length()) {
        char *etag = bl.c_str();
        s->formatter->dump_value_str("ETag", etag);
      }
    }
    s->formatter->close_section("CopyObjectResult");
    s->formatter->flush();
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
    s->formatter->open_obj_section("InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"");
    s->formatter->dump_value_str("Bucket", s->bucket);
    s->formatter->dump_value_str("Key", s->object);
    s->formatter->dump_value_str("UploadId", upload_id.c_str());
    s->formatter->close_section("InitiateMultipartUploadResult");
    s->formatter->flush();
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
    s->formatter->open_obj_section("CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"");
    const char *gateway_dns_name = FCGX_GetParam("RGW_DNS_NAME", s->fcgx->envp);
    if (gateway_dns_name)
      s->formatter->dump_value_str("Location", "%s.%s", s->bucket, gateway_dns_name);
    s->formatter->dump_value_str("Bucket", s->bucket);
    s->formatter->dump_value_str("Key", s->object);
    s->formatter->dump_value_str("ETag", etag.c_str());
    s->formatter->close_section("CompleteMultipartUploadResult");
    s->formatter->flush();
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
    s->formatter->open_obj_section("ListMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"");
    map<uint32_t, RGWUploadPartInfo>::iterator iter, test_iter;
    int i, cur_max = 0;

    iter = parts.upper_bound(marker);
    for (i = 0, test_iter = iter; test_iter != parts.end() && i < max_parts; ++test_iter, ++i) {
      cur_max = test_iter->first;
    }
    s->formatter->dump_value_str("Bucket", s->bucket);
    s->formatter->dump_value_str("Key", s->object);
    s->formatter->dump_value_str("UploadId", upload_id.c_str());
    s->formatter->dump_value_str("StorageClass", "STANDARD");
    s->formatter->dump_value_str("PartNumberMarker", "%d", marker);
    s->formatter->dump_value_str("NextPartNumberMarker", "%d", cur_max + 1);
    s->formatter->dump_value_str("MaxParts", "%d", max_parts);
    s->formatter->dump_value_str("IsTruncated", "%s", (test_iter == parts.end() ? "false" : "true"));

    ACLOwner& owner = policy.get_owner();
    dump_owner(s, owner.get_id(), owner.get_display_name());

    for (; iter != parts.end(); ++iter) {
      RGWUploadPartInfo& info = iter->second;

      time_t sec = info.modified.sec();
      struct tm tmp;
      localtime_r(&sec, &tmp);
      char buf[TIME_BUF_SIZE];
      if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", &tmp) > 0) {
        s->formatter->dump_value_str("LastModified", buf);
      }

      s->formatter->open_obj_section("Part");
      s->formatter->dump_value_int("PartNumber", "%u", info.num);
      s->formatter->dump_value_str("ETag", "%s", info.etag.c_str());
      s->formatter->dump_value_int("Size", "%llu", info.size);
      s->formatter->close_section("Part");
    }
    s->formatter->close_section("ListMultipartUploadResult");
    s->formatter->flush();
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

  s->formatter->open_obj_section("ListMultipartUploadsResult");
  s->formatter->dump_value_str("Bucket", s->bucket);
  if (!prefix.empty())
    s->formatter->dump_value_str("ListMultipartUploadsResult.Prefix", prefix.c_str());
  if (!key_marker.empty())
    s->formatter->dump_value_str("KeyMarker", key_marker.c_str());
  if (!uploadid_marker.empty())
    s->formatter->dump_value_str("UploadIdMarker", uploadid_marker.c_str());
  if (!next_marker.key.empty())
    s->formatter->dump_value_str("NextKeyMarker", next_marker.key.c_str());
  if (!next_marker.upload_id.empty())
    s->formatter->dump_value_str("NextUploadIdMarker", next_marker.upload_id.c_str());
  if (!max_keys.empty()) {
    s->formatter->dump_value_str("MaxUploads", max_keys.c_str());
  }
  if (!delimiter.empty())
    s->formatter->dump_value_str("Delimiter", delimiter.c_str());
  s->formatter->dump_value_str("IsTruncated", (is_truncated ? "true" : "false"));

  if (ret >= 0) {
    vector<RGWMultipartUploadEntry>::iterator iter;
    for (iter = uploads.begin(); iter != uploads.end(); ++iter) {
      s->formatter->open_array_section("Upload");
      s->formatter->dump_value_str("Key", iter->key.c_str());
      s->formatter->dump_value_str("UploadId", iter->upload_id.c_str());
      dump_owner(s, s->user.user_id, s->user.display_name, "Initiator");
      dump_owner(s, s->user.user_id, s->user.display_name);
      s->formatter->dump_value_str("StorageClass", "STANDARD");
      dump_time(s, "Initiated", &iter->obj.mtime);
      s->formatter->close_section("Upload");
    }
    if (common_prefixes.size() > 0) {
      s->formatter->open_array_section("CommonPrefixes");
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin(); pref_iter != common_prefixes.end(); ++pref_iter) {
        s->formatter->dump_value_str("CommonPrefixes.Prefix", pref_iter->first.c_str());
      }
      s->formatter->close_section("CommonPrefixes");
    }
  }
  s->formatter->close_section("ListMultipartUploadsResult");
  s->formatter->flush();
}

RGWOp *RGWHandler_REST_S3::get_retrieve_obj_op(struct req_state *s, bool get_data)
{
  if (is_acl_op(s)) {
    return &get_acls_op;
  }
  if (s->object) {
    get_obj_op.set_get_data(get_data);
    return &get_obj_op;
  } else if (!s->bucket) {
    return NULL;
  }

  if (s->args.exists("uploads"))
    return &list_bucket_multiparts;

  return &list_bucket_op;
}

RGWOp *RGWHandler_REST_S3::get_retrieve_op(struct req_state *s, bool get_data)
{
  if (s->bucket) {
    if (is_acl_op(s)) {
      return &get_acls_op;
    } else if (s->args.exists("uploadId")) {
      return &list_multipart;
    }
    return get_retrieve_obj_op(s, get_data);
  }

  return &list_buckets_op;
}

RGWOp *RGWHandler_REST_S3::get_create_op(struct req_state *s)
{
  if (is_acl_op(s)) {
    return &put_acls_op;
  } else if (s->object) {
    if (!s->copy_source)
      return &put_obj_op;
    else
      return &copy_obj_op;
  } else if (s->bucket) {
    return &create_bucket_op;
  }

  return NULL;
}

RGWOp *RGWHandler_REST_S3::get_delete_op(struct req_state *s)
{
  string upload_id = s->args.get("uploadId");

  if (s->object) {
    if (upload_id.empty())
      return &delete_obj_op;
    else
      return &abort_multipart;
  } else if (s->bucket)
    return &delete_bucket_op;

  return NULL;
}

RGWOp *RGWHandler_REST_S3::get_post_op(struct req_state *s)
{
  if (s->object) {
    if (s->args.exists("uploadId"))
      return &complete_multipart;
    else
      return &init_multipart;
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
  RGW_LOG(0) << "dest=" << dest << dendl;
}

/*
 * get the header authentication  information required to
 * compute a request's signature
 */
static void get_auth_header(struct req_state *s, string& dest, bool qsr)
{
  dest = "";
  if (s->method)
    dest = s->method;
  dest.append("\n");
  
  const char *md5 = FCGX_GetParam("HTTP_CONTENT_MD5", s->fcgx->envp);
  if (md5)
    dest.append(md5);
  dest.append("\n");

  const char *type = FCGX_GetParam("CONTENT_TYPE", s->fcgx->envp);
  if (type)
    dest.append(type);
  dest.append("\n");

  string date;
  if (qsr) {
    date = s->args.get("Expires");
  } else {
    const char *str = FCGX_GetParam("HTTP_DATE", s->fcgx->envp);
    if (str)
      date = str;
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
}

/*
 * verify that a signed request comes from the keyholder
 * by checking the signature against our locally-computed version
 */
bool RGWHandler_REST_S3::authorize(struct req_state *s)
{
  bool qsr = false;
  string auth_id;
  string auth_sign;

  if (!s->http_auth || !(*s->http_auth)) {
    auth_id = s->args.get("AWSAccessKeyId");
    if (auth_id.size()) {
      url_decode(s->args.get("Signature"), auth_sign);

      string date = s->args.get("Expires");
      time_t exp = atoll(date.c_str());
      time_t now;
      time(&now);
      if (now >= exp)
        return false;

      qsr = true;
    } else {
      /* anonymous access */
      rgw_get_anon_user(s->user);
      s->perm_mask = RGW_PERM_FULL_CONTROL;
      return true;
    }
  } else {
    if (strncmp(s->http_auth, "AWS ", 4))
      return false;
    string auth_str(s->http_auth + 4);
    int pos = auth_str.find(':');
    if (pos < 0)
      return false;

    auth_id = auth_str.substr(0, pos);
    auth_sign = auth_str.substr(pos + 1);
  }

  /* first get the user info */
  if (rgw_get_user_info_by_access_key(auth_id, s->user) < 0) {
    RGW_LOG(5) << "error reading user info, uid=" << auth_id << " can't authenticate" << dendl;
    return false;
  }

  /* now verify signature */
   
  string auth_hdr;
  get_auth_header(s, auth_hdr, qsr);
  RGW_LOG(10) << "auth_hdr:\n" << auth_hdr << dendl;

  map<string, RGWAccessKey>::iterator iter = s->user.access_keys.find(auth_id);
  if (iter == s->user.access_keys.end()) {
    RGW_LOG(0) << "ERROR: access key not encoded in user info" << dendl;
    return false;
  }
  RGWAccessKey& k = iter->second;
  const char *key = k.key.c_str();
  int key_len = k.key.size();

  if (!k.subuser.empty()) {
    map<string, RGWSubUser>::iterator uiter = s->user.subusers.find(k.subuser);
    if (uiter == s->user.subusers.end()) {
      RGW_LOG(0) << "ERROR: could not find subuser: " << k.subuser << dendl;
      return false;
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
    return false;
  }
  b64[ret] = '\0';

  RGW_LOG(15) << "b64=" << b64 << dendl;
  RGW_LOG(15) << "auth_sign=" << auth_sign << dendl;
  RGW_LOG(15) << "compare=" << auth_sign.compare(b64) << dendl;
  return (auth_sign.compare(b64) == 0);
}


