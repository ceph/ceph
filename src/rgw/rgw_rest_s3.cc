#include <errno.h>
#include <string.h>

#include <cryptopp/sha.h>
#include <cryptopp/hmac.h>

#include "rgw_rest.h"
#include "rgw_rest_s3.h"

#include "common/armor.h"


using namespace CryptoPP;

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

  dump_errno(s, ret, &err);
  if (!content_type)
    content_type = "binary/octet-stream";
  end_header(s, content_type);

  sent_header = true;

send_data:
  if (get_data && !orig_ret) {
    FCGX_PutStr(data, len, s->fcgx->out); 
  }

  return 0;
}

void RGWListBuckets_REST_S3::send_response()
{
  dump_errno(s, ret);
  end_header(s, "application/xml");
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
}

void RGWListBucket_REST_S3::send_response()
{
  dump_errno(s, (ret < 0 ? ret : 0));

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
      s->formatter->dump_value_str("ETag", "&quot;%s&quot;", iter->etag);
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
}

void RGWCreateBucket_REST_S3::send_response()
{
  dump_errno(s, ret);
  end_header(s);
}

void RGWDeleteBucket_REST_S3::send_response()
{
  int r = ret;
  if (!r)
    r = 204;

  dump_errno(s, r);
  end_header(s);
}

void RGWPutObj_REST_S3::send_response()
{
  dump_etag(s, etag.c_str());
  dump_errno(s, ret, &err);
  end_header(s);
}

void RGWDeleteObj_REST_S3::send_response()
{
  int r = ret;
  if (!r)
    r = 204;

  dump_errno(s, r);
  end_header(s);
}

void RGWCopyObj_REST_S3::send_response()
{
  dump_errno(s, ret, &err);

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
  }
}

void RGWGetACLs_REST_S3::send_response()
{
  if (ret) dump_errno(s, ret);
  end_header(s, "application/xml");
  dump_start(s);
  FCGX_PutStr(acls.c_str(), acls.size(), s->fcgx->out); 
}

void RGWPutACLs_REST_S3::send_response()
{
  dump_errno(s, ret);
  end_header(s, "application/xml");
  dump_start(s);
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

  return &list_bucket_op;
}

RGWOp *RGWHandler_REST_S3::get_retrieve_op(struct req_state *s, bool get_data)
{
  if (s->bucket) {
    if (is_acl_op(s)) {
      return &get_acls_op;
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
  if (s->object)
    return &delete_obj_op;
  else if (s->bucket)
    return &delete_bucket_op;

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

  string& sub = s->args.get_sub_resource();
  if (sub.size() > 0) {
    dest.append("?");
    dest.append(sub);
  }
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
 * calculate the sha1 value of a given msg and key
 */
static int calc_hmac_sha1(const char *key, int key_len,
                           const char *msg, int msg_len,
                           char *dest, int *len) /* dest should be large enough to hold result */
{
  if (*len < HMAC<SHA1>::DIGESTSIZE)
    return -EINVAL;

  char hex_str[HMAC<SHA1>::DIGESTSIZE * 2 + 1];

  HMAC<SHA1> hmac((const unsigned char *)key, key_len);
  hmac.Update((const unsigned char *)msg, msg_len);
  hmac.Final((unsigned char *)dest);
  *len = HMAC<SHA1>::DIGESTSIZE;
  
  buf_to_hex((unsigned char *)dest, *len, hex_str);

  RGW_LOG(15) << "hmac=" << hex_str << endl;

  return 0;
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
  if (rgw_get_user_info(auth_id, s->user) < 0) {
    RGW_LOG(5) << "error reading user info, uid=" << auth_id << " can't authenticate" << endl;
    return false;
  }

  /* now verify signature */
   
  string auth_hdr;
  get_auth_header(s, auth_hdr, qsr);
  RGW_LOG(10) << "auth_hdr:" << endl << auth_hdr << endl;

  const char *key = s->user.secret_key.c_str();
  int key_len = strlen(key);

  char hmac_sha1[HMAC<SHA1>::DIGESTSIZE];
  int len = sizeof(hmac_sha1);
  if (calc_hmac_sha1(key, key_len, auth_hdr.c_str(), auth_hdr.size(), hmac_sha1, &len) < 0)
    return false;

  char b64[64]; /* 64 is really enough */
  int ret = ceph_armor(b64, &b64[sizeof(b64)], hmac_sha1, &hmac_sha1[len]);
  if (ret < 0) {
    RGW_LOG(10) << "ceph_armor failed" << endl;
    return false;
  }
  b64[ret] = '\0';

  RGW_LOG(15) << "b64=" << b64 << endl;
  RGW_LOG(15) << "auth_sign=" << auth_sign << endl;
  RGW_LOG(15) << "compare=" << auth_sign.compare(b64) << endl;
  return (auth_sign.compare(b64) == 0);
}


