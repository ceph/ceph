#include <errno.h>

#include "rgw_rest.h"
#include "rgw_rest_s3.h"

void list_all_buckets_start(struct req_state *s)
{
  open_section(s, "ListAllMyBucketsResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\"");
}

void list_all_buckets_end(struct req_state *s)
{
  close_section(s, "ListAllMyBucketsResult");
}

void dump_bucket(struct req_state *s, RGWObjEnt& obj)
{
  open_section(s, "Bucket");
  dump_value(s, "Name", obj.name.c_str());
  dump_time(s, "CreationDate", &obj.mtime);
  close_section(s, "Bucket");
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
         CGI_PRINTF(s->fcgx->out,"%s: %s\r\n", name, iter->second.c_str());
       } else if (!content_type && strcmp(name, RGW_ATTR_CONTENT_TYPE) == 0) {
         content_type = iter->second.c_str();
       }
    }
  }

  if (range_str && !ret)
    ret = 206; /* partial content */

  dump_errno(s, ret, &err);
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
  dump_start_xml(s);

  list_all_buckets_start(s);
  dump_owner(s, s->user.user_id, s->user.display_name);

  map<string, RGWObjEnt>& m = buckets.get_buckets();
  map<string, RGWObjEnt>::iterator iter;

  open_section(s, "Buckets");
  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWObjEnt obj = iter->second;
    dump_bucket(s, obj);
  }
  close_section(s, "Buckets");
  list_all_buckets_end(s);
}

void RGWListBucket_REST_S3::send_response()
{
  dump_errno(s, (ret < 0 ? ret : 0));

  end_header(s, "application/xml");
  dump_start_xml(s);
  if (ret < 0)
    return;

  open_section(s, "ListBucketResult");
  dump_value(s, "Name", s->bucket);
  if (!prefix.empty())
    dump_value(s, "Prefix", prefix.c_str());
  if (!marker.empty())
    dump_value(s, "Marker", marker.c_str());
  if (!max_keys.empty()) {
    dump_value(s, "MaxKeys", max_keys.c_str());
  }
  if (!delimiter.empty())
    dump_value(s, "Delimiter", delimiter.c_str());

  if (ret >= 0) {
    vector<RGWObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      open_section(s, "Contents");
      dump_value(s, "Key", iter->name.c_str());
      dump_time(s, "LastModified", &iter->mtime);
      dump_value(s, "ETag", "&quot;%s&quot;", iter->etag);
      dump_value(s, "Size", "%lld", iter->size);
      dump_value(s, "StorageClass", "STANDARD");
      dump_owner(s, s->user.user_id, s->user.display_name);
      close_section(s, "Contents");
    }
    if (common_prefixes.size() > 0) {
      open_section(s, "CommonPrefixes");
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin(); pref_iter != common_prefixes.end(); ++pref_iter) {
        dump_value(s, "Prefix", pref_iter->first.c_str());
      }
      close_section(s, "CommonPrefixes");
    }
  }
  close_section(s, "ListBucketResult");
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

  end_header(s);
  if (ret == 0) {
    open_section(s, "CopyObjectResult");
    dump_time(s, "LastModified", &mtime);
    map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_ETAG);
    if (iter != attrs.end()) {
      bufferlist& bl = iter->second;
      if (bl.length()) {
        char *etag = bl.c_str();
        dump_value(s, "ETag", etag);
      }
    }
    close_section(s, "CopyObjectResult");
  }
}

void RGWGetACLs_REST_S3::send_response()
{
  if (ret) dump_errno(s, ret);
  end_header(s, "application/xml");
  dump_start_xml(s);
  FCGX_PutStr(acls.c_str(), acls.size(), s->fcgx->out); 
}

void RGWPutACLs_REST_S3::send_response()
{
  dump_errno(s, ret);
  end_header(s, "application/xml");
  dump_start_xml(s);
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


