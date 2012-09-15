#include <errno.h>
#include <string.h>

#include "common/ceph_crypto.h"
#include "common/Formatter.h"

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_acl.h"

#include "common/armor.h"

#ifdef FASTCGI_INCLUDE_DIR
# include "fastcgi/fcgiapp.h"
#else
# include "fcgiapp.h"
#endif

#define dout_subsys ceph_subsys_rgw

using namespace ceph::crypto;

void list_all_buckets_start(struct req_state *s)
{
  s->formatter->open_array_section_in_ns("ListAllMyBucketsResult",
			      "http://s3.amazonaws.com/doc/2006-03-01/");
}

void list_all_buckets_end(struct req_state *s)
{
  s->formatter->close_section();
}

void dump_bucket(struct req_state *s, RGWBucketEnt& obj)
{
  s->formatter->open_object_section("Bucket");
  s->formatter->dump_string("Name", obj.bucket.name);
  dump_time(s, "CreationDate", &obj.mtime);
  s->formatter->close_section();
}

void rgw_get_errno_s3(rgw_html_errors *e , int err_no)
{
  const struct rgw_html_errors *r;
  r = search_err(err_no, RGW_HTML_ERRORS, ARRAY_LEN(RGW_HTML_ERRORS));

  if (r) {
    e->http_ret = r->http_ret;
    e->s3_code = r->s3_code;
  } else {
    e->http_ret = 500;
    e->s3_code = "UnknownError";
  }
}

int RGWGetObj_REST_S3::send_response(bufferlist& bl)
{
  string content_type_str;
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

    if (s->args.has_response_modifier()) {
      bool exists;
      content_type_str = s->args.get("response-content-type", &exists);
      if (exists)
	content_type = content_type_str.c_str();
      string val = s->args.get("response-content-language", &exists);
      if (exists)
        CGI_PRINTF(s, "Content-Language: %s\n", val.c_str());
      val = s->args.get("response-expires", &exists);
      if (exists)
        CGI_PRINTF(s, "Expires: %s\n", val.c_str());
      val = s->args.get("response-cache-control", &exists);
      if (exists)
        CGI_PRINTF(s, "Cache-Control: %s\n", val.c_str());
      val = s->args.get("response-content-disposition", &exists);
      if (exists)
        CGI_PRINTF(s, "Content-Disposition: %s\n", val.c_str());
      val = s->args.get("response-content-encoding", &exists);
      if (exists)
        CGI_PRINTF(s, "Content-Encoding: %s\n", val.c_str());
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

  if (partial_content && !ret)
    ret = STATUS_PARTIAL_CONTENT;
done:
  set_req_state_err(s, ret);

  dump_errno(s);
  if (!content_type)
    content_type = "binary/octet-stream";
  end_header(s, content_type);
  sent_header = true;

send_data:
  if (get_data && !orig_ret) {
    CGI_PutStr(s, bl.c_str(), len);
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
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWListBucket_REST_S3::get_params()
{
  prefix = s->args.get("prefix");
  marker = s->args.get("marker");
  max_keys = s->args.get("max-keys");
  ret = parse_max_keys();
  if (ret < 0) {
    return ret;
  }
  delimiter = s->args.get("delimiter");
  return 0;
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

  s->formatter->open_object_section_in_ns("ListBucketResult",
					  "http://s3.amazonaws.com/doc/2006-03-01/");
  s->formatter->dump_string("Name", s->bucket_name);
  if (!prefix.empty())
    s->formatter->dump_string("Prefix", prefix);
  s->formatter->dump_string("Marker", marker);
  s->formatter->dump_int("MaxKeys", max);
  if (!delimiter.empty())
    s->formatter->dump_string("Delimiter", delimiter);

  s->formatter->dump_string("IsTruncated", (max && is_truncated ? "true" : "false"));

  if (ret >= 0) {
    vector<RGWObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      s->formatter->open_array_section("Contents");
      s->formatter->dump_string("Key", iter->name);
      dump_time(s, "LastModified", &iter->mtime);
      s->formatter->dump_format("ETag", "\"%s\"", iter->etag.c_str());
      s->formatter->dump_int("Size", iter->size);
      s->formatter->dump_string("StorageClass", "STANDARD");
      dump_owner(s, iter->owner, iter->owner_display_name);
      s->formatter->close_section();
    }
    if (common_prefixes.size() > 0) {
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin(); pref_iter != common_prefixes.end(); ++pref_iter) {
        s->formatter->open_array_section("CommonPrefixes");
        s->formatter->dump_string("Prefix", pref_iter->first);
        s->formatter->close_section();
      }
    }
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

static void dump_bucket_metadata(struct req_state *s, RGWBucketEnt& bucket)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", (long long)bucket.count);
  CGI_PRINTF(s,"X-RGW-Object-Count: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)bucket.size);
  CGI_PRINTF(s,"X-RGW-Bytes-Used: %s\n", buf);
}

void RGWStatBucket_REST_S3::send_response()
{
  if (ret >= 0) {
    dump_bucket_metadata(s, bucket);
  }

  set_req_state_err(s, ret);
  dump_errno(s);

  end_header(s);
  dump_start(s);
}

int RGWCreateBucket_REST_S3::get_params()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);
  int r = s3policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
  if (r < 0)
    return r;

  policy = s3policy;

  return 0;
}

void RGWCreateBucket_REST_S3::send_response()
{
  if (ret == -ERR_BUCKET_EXISTS)
    ret = 0;
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
}

void RGWDeleteBucket_REST_S3::send_response()
{
  int r = ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
}

int RGWPutObj_REST_S3::get_params()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);
  if (!s->length)
    return -ERR_LENGTH_REQUIRED;

  int r = s3policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
  if (!r)
     return -EINVAL;

  policy = s3policy;

  return RGWPutObj_REST::get_params();
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
  if (r == -ENOENT)
    r = 0;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
}

int RGWCopyObj_REST_S3::init_dest_policy()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);

  /* build a policy for the target object */
  ret = s3policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
  if (!ret)
     return -EINVAL;

  dest_policy = s3policy;

  return 0;
}

int RGWCopyObj_REST_S3::get_params()
{
  if_mod = s->env->get("HTTP_X_AMZ_COPY_IF_MODIFIED_SINCE");
  if_unmod = s->env->get("HTTP_X_AMZ_COPY_IF_UNMODIFIED_SINCE");
  if_match = s->env->get("HTTP_X_AMZ_COPY_IF_MATCH");
  if_nomatch = s->env->get("HTTP_X_AMZ_COPY_IF_NONE_MATCH");

  const char *req_src = s->copy_source;
  if (!req_src || !req_src)
    return -EINVAL;

  ret = parse_copy_location(req_src, src_bucket_name, src_object);
  if (!ret)
     return -EINVAL;

  dest_bucket_name = s->bucket.name;
  dest_object = s->object_str;

  const char *md_directive = s->env->get("HTTP_X_AMZ_METADATA_DIRECTIVE");
  if (md_directive) {
    if (strcasecmp(md_directive, "COPY") == 0) {
      replace_attrs = false;
    } else if (strcasecmp(md_directive, "REPLACE") == 0) {
      replace_attrs = true;
    } else {
      return -EINVAL;
    }
  }

  if ((dest_bucket_name.compare(src_bucket_name) == 0) &&
      (dest_object.compare(src_object) == 0) &&
      !replace_attrs) {
    /* can only copy object into itself if replacing attrs */
    return -ERR_INVALID_REQUEST;
  }
  return 0;
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
        s->formatter->dump_string("ETag", etag);
      }
    }
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
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

int RGWPutACLs_REST_S3::get_canned_policy(ACLOwner& owner, stringstream& ss)
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);
  bool r = s3policy.create_canned(owner.get_id(), owner.get_display_name(), s->canned_acl);
  if (!r)
    return -EINVAL;

  s3policy.to_xml(ss);

  return 0;
}

void RGWPutACLs_REST_S3::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s, "application/xml");
  dump_start(s);
}

int RGWInitMultipart_REST_S3::get_params()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);
  ret = s3policy.create_canned(s->user.user_id, s->user.display_name, s->canned_acl);
  if (!ret)
     return -EINVAL;

  policy = s3policy;

  return RGWInitMultipart_REST::get_params();
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
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object);
    s->formatter->dump_string("UploadId", upload_id);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
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
    if (g_conf->rgw_dns_name.length())
      s->formatter->dump_format("Location", "%s.%s", s->bucket_name, g_conf->rgw_dns_name.c_str());
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object);
    s->formatter->dump_string("ETag", etag);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWAbortMultipart_REST_S3::send_response()
{
  int r = ret;
  if (!r)
    r = STATUS_NO_CONTENT;

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
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object);
    s->formatter->dump_string("UploadId", upload_id);
    s->formatter->dump_string("StorageClass", "STANDARD");
    s->formatter->dump_int("PartNumberMarker", marker);
    s->formatter->dump_int("NextPartNumberMarker", cur_max + 1);
    s->formatter->dump_int("MaxParts", max_parts);
    s->formatter->dump_string("IsTruncated", (test_iter == parts.end() ? "false" : "true"));

    ACLOwner& owner = policy.get_owner();
    dump_owner(s, owner.get_id(), owner.get_display_name());

    for (; iter != parts.end(); ++iter) {
      RGWUploadPartInfo& info = iter->second;

      time_t sec = info.modified.sec();
      struct tm tmp;
      gmtime_r(&sec, &tmp);
      char buf[TIME_BUF_SIZE];

      s->formatter->open_object_section("Part");

      if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", &tmp) > 0) {
        s->formatter->dump_string("LastModified", buf);
      }

      s->formatter->dump_unsigned("PartNumber", info.num);
      s->formatter->dump_string("ETag", info.etag);
      s->formatter->dump_unsigned("Size", info.size);
      s->formatter->close_section();
    }
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
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
  s->formatter->dump_string("Bucket", s->bucket_name);
  if (!prefix.empty())
    s->formatter->dump_string("ListMultipartUploadsResult.Prefix", prefix);
  string& key_marker = marker.get_key();
  if (!key_marker.empty())
    s->formatter->dump_string("KeyMarker", key_marker);
  string& upload_id_marker = marker.get_upload_id();
  if (!upload_id_marker.empty())
    s->formatter->dump_string("UploadIdMarker", upload_id_marker);
  string next_key = next_marker.mp.get_key();
  if (!next_key.empty())
    s->formatter->dump_string("NextKeyMarker", next_key);
  string next_upload_id = next_marker.mp.get_upload_id();
  if (!next_upload_id.empty())
    s->formatter->dump_string("NextUploadIdMarker", next_upload_id);
  s->formatter->dump_int("MaxUploads", max_uploads);
  if (!delimiter.empty())
    s->formatter->dump_string("Delimiter", delimiter);
  s->formatter->dump_string("IsTruncated", (is_truncated ? "true" : "false"));

  if (ret >= 0) {
    vector<RGWMultipartUploadEntry>::iterator iter;
    for (iter = uploads.begin(); iter != uploads.end(); ++iter) {
      RGWMPObj& mp = iter->mp;
      s->formatter->open_array_section("Upload");
      s->formatter->dump_string("Key", mp.get_key());
      s->formatter->dump_string("UploadId", mp.get_upload_id());
      dump_owner(s, s->user.user_id, s->user.display_name, "Initiator");
      dump_owner(s, s->user.user_id, s->user.display_name);
      s->formatter->dump_string("StorageClass", "STANDARD");
      dump_time(s, "Initiated", &iter->obj.mtime);
      s->formatter->close_section();
    }
    if (common_prefixes.size() > 0) {
      s->formatter->open_array_section("CommonPrefixes");
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin(); pref_iter != common_prefixes.end(); ++pref_iter) {
        s->formatter->dump_string("CommonPrefixes.Prefix", pref_iter->first);
      }
      s->formatter->close_section();
    }
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWDeleteMultiObj_REST_S3::send_status()
{
  if (!status_dumped) {
    if (ret < 0)
      set_req_state_err(s, ret);
    dump_errno(s);
    status_dumped = true;
  }
}

void RGWDeleteMultiObj_REST_S3::begin_response()
{

  if (!status_dumped) {
    send_status();
  }

  dump_start(s);
  end_header(s, "application/xml");
  s->formatter->open_object_section_in_ns("DeleteResult",
                                            "http://s3.amazonaws.com/doc/2006-03-01/");

  rgw_flush_formatter(s, s->formatter);
}

void RGWDeleteMultiObj_REST_S3::send_partial_response(pair<string,int>& result)
{
  if (!result.first.empty()) {
    if (result.second == 0 && !quiet) {
      s->formatter->open_object_section("Deleted");
      s->formatter->dump_string("Key", result.first);
      s->formatter->close_section();
    } else if (result.first < 0) {
      struct rgw_html_errors *r = new rgw_html_errors;
      int err_no;

      s->formatter->open_object_section("Error");

      err_no = -(result.second);
      rgw_get_errno_s3(r, err_no);

      s->formatter->dump_string("Key", result.first);
      s->formatter->dump_int("Code", r->http_ret);
      s->formatter->dump_string("Message", r->s3_code);
      s->formatter->close_section();
    }

    rgw_flush_formatter(s, s->formatter);
  }
}

void RGWDeleteMultiObj_REST_S3::end_response()
{

  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
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
  } else if (!s->bucket_name) {
    return NULL;
  }

  if (s->args.exists("uploads"))
    return new RGWListBucketMultiparts_REST_S3;

  if (get_data)
    return new RGWListBucket_REST_S3;
  else
    return new RGWStatBucket_REST_S3;
}

RGWOp *RGWHandler_REST_S3::get_retrieve_op(bool get_data)
{
  if (s->bucket_name) {
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
  } else if (s->bucket_name) {
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
  } else if (s->bucket_name)
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
  else if ( s->request_params == "delete" ) {
    return new RGWDeleteMultiObj_REST_S3;
  }

  return NULL;
}

int RGWHandler_REST_S3::init(RGWRados *store, struct req_state *state, FCGX_Request *fcgx)
{
  const char *cacl = state->env->get("HTTP_X_AMZ_ACL");
  if (cacl)
    state->canned_acl = cacl;

  state->copy_source = state->env->get("HTTP_X_AMZ_COPY_SOURCE");

  state->dialect = "s3";

  return RGWHandler_REST::init(store, state, fcgx);
}

/*
 * ?get the canonical amazon-style header for something?
 */

static void get_canon_amz_hdr(struct req_state *s, string& dest)
{
  dest = "";
  map<string, string>::iterator iter;
  for (iter = s->x_meta_map.begin(); iter != s->x_meta_map.end(); ++iter) {
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

  dest.append(s->request_uri.c_str());

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
  dout(10) << "get_canon_resource(): dest=" << dest << dendl;
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
        dout(0) << "NOTICE: bad content-md5 provided (not base64), aborting request p=" << *p << " " << (int)*p << dendl;
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
        dout(0) << "NOTICE: missing date for auth header" << dendl;
        return false;
      }
    }

    struct tm t;
    if (!parse_rfc2616(req_date, &t)) {
      dout(0) << "NOTICE: failed to parse date for auth header" << dendl;
      return false;
    }
    if (t.tm_year < 70) {
      dout(0) << "NOTICE: bad date (predates epoch): " << req_date << dendl;
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
      auth_sign = s->args.get("Signature");

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
  if (rgw_get_user_info_by_access_key(store, auth_id, s->user) < 0) {
    dout(5) << "error reading user info, uid=" << auth_id << " can't authenticate" << dendl;
    return -EPERM;
  }

  /* now verify signature */
   
  string auth_hdr;
  if (!get_auth_header(s, auth_hdr, qsr)) {
    dout(10) << "failed to create auth header\n" << auth_hdr << dendl;
    return -EPERM;
  }
  dout(10) << "auth_hdr:\n" << auth_hdr << dendl;

  time_t req_sec = s->header_time.sec();
  if ((req_sec < now - RGW_AUTH_GRACE_MINS * 60 ||
      req_sec > now + RGW_AUTH_GRACE_MINS * 60) && !qsr) {
    dout(10) << "req_sec=" << req_sec << " now=" << now << "; now - RGW_AUTH_GRACE_MINS=" << now - RGW_AUTH_GRACE_MINS * 60 << "; now + RGW_AUTH_GRACE_MINS=" << now + RGW_AUTH_GRACE_MINS * 60 << dendl;
    dout(0) << "NOTICE: request time skew too big now=" << utime_t(now, 0) << " req_time=" << s->header_time << dendl;
    return -ERR_REQUEST_TIME_SKEWED;
  }

  map<string, RGWAccessKey>::iterator iter = s->user.access_keys.find(auth_id);
  if (iter == s->user.access_keys.end()) {
    dout(0) << "ERROR: access key not encoded in user info" << dendl;
    return -EPERM;
  }
  RGWAccessKey& k = iter->second;
  const char *key = k.key.c_str();
  int key_len = k.key.size();

  if (!k.subuser.empty()) {
    map<string, RGWSubUser>::iterator uiter = s->user.subusers.find(k.subuser);
    if (uiter == s->user.subusers.end()) {
      dout(0) << "NOTICE: could not find subuser: " << k.subuser << dendl;
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
    dout(10) << "ceph_armor failed" << dendl;
    return -EPERM;
  }
  b64[ret] = '\0';

  dout(15) << "b64=" << b64 << dendl;
  dout(15) << "auth_sign=" << auth_sign << dendl;
  dout(15) << "compare=" << auth_sign.compare(b64) << dendl;

  if (auth_sign.compare(b64) != 0)
    return -EPERM;

  return  0;
}


