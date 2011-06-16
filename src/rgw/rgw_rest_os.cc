
#include "rgw_os.h"
#include "rgw_rest_os.h"

void RGWListBuckets_REST_OS::send_response()
{
  set_req_state_err(s, ret);
  dump_errno(s);

  dump_start(s);

  if (ret < 0) {
    end_header(s);
    return;
  }

  s->formatter->open_array_section("account");

  // dump_owner(s, s->user.user_id, s->user.display_name);

  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  map<string, RGWBucketEnt>::iterator iter;

  string marker = s->args.get("marker");
  if (marker.empty())
    iter = m.begin();
  else
    iter = m.upper_bound(marker);

  int limit = 10000;
  string limit_str = s->args.get("limit");
  if (!limit_str.empty())
    limit = atoi(limit_str.c_str());

  for (int i = 0; i < limit && iter != m.end(); ++iter, ++i) {
    RGWBucketEnt obj = iter->second;
    s->formatter->open_obj_section("container");
    s->formatter->dump_value_str("name", obj.name.c_str());
    s->formatter->dump_value_int("count", "%lld", obj.count);
    s->formatter->dump_value_int("bytes", "%lld", obj.size);
    s->formatter->close_section("container");
  }
  s->formatter->close_section("account");

  RGW_LOG(10) << "formatter->get_len=" << s->formatter->get_len() << dendl;

  dump_content_length(s, s->formatter->get_len());
  end_header(s);
  s->formatter->flush(s);
}

void RGWListBucket_REST_OS::send_response()
{
  set_req_state_err(s, (ret < 0 ? ret : 0));
  dump_errno(s);

  dump_start(s);
  if (ret < 0) {
    end_header(s);
    return;
  }

  vector<RGWObjEnt>::iterator iter = objs.begin();
  map<string, bool>::iterator pref_iter = common_prefixes.begin();

  s->formatter->open_array_section("container");

  while (iter != objs.end() || pref_iter != common_prefixes.end()) {
    bool do_pref = false;
    bool do_objs = false;
    if (pref_iter == common_prefixes.end())
      do_objs = true;
    else if (iter == objs.end())
      do_pref = true;
    else if (iter->name.compare(pref_iter->first) == 0) {
      do_objs = true;
      pref_iter++;
    } else if (iter->name.compare(pref_iter->first) <= 0)
      do_objs = true;
    else
      do_pref = true;

    if (do_objs && (marker.empty() || iter->name.compare(marker) > 0)) {
      s->formatter->open_obj_section("object");
      s->formatter->dump_value_str("name", iter->name.c_str());
      s->formatter->dump_value_str("hash", "\"%s\"", iter->etag);
      s->formatter->dump_value_int("bytes", "%lld", iter->size);
      if (iter->content_type.size())
        s->formatter->dump_value_str("content_type", iter->content_type.c_str());
      dump_time(s, "last_modified", &iter->mtime);
      s->formatter->close_section("object");
    }

    if (do_pref &&  (marker.empty() || pref_iter->first.compare(marker) > 0)) {
      s->formatter->open_obj_section("object");
      s->formatter->dump_value_str("name", pref_iter->first.c_str());
      s->formatter->close_section("object");
    }
    if (do_objs)
      iter++;
    else
      pref_iter++;
  }

  s->formatter->close_section("container");

  end_header(s);
  s->formatter->flush(s);
}

static void dump_container_metadata(struct req_state *s, RGWBucketEnt& bucket)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%lld", (long long)bucket.count);
  CGI_PRINTF(s,"X-Container-Object-Count: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)bucket.size);
  CGI_PRINTF(s,"X-Container-Bytes-Used: %s\n", buf);
}

void RGWStatBucket_REST_OS::send_response()
{
  if (ret >= 0)
    dump_container_metadata(s, bucket);

  if (ret < 0)
    set_req_state_err(s, ret);
  dump_errno(s);

  end_header(s);
  dump_start(s);
  s->formatter->flush(s);
}

void RGWCreateBucket_REST_OS::send_response()
{
  if (ret)
    set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
  s->formatter->flush(s);
}

void RGWDeleteBucket_REST_OS::send_response()
{
  int r = ret;
  if (!r)
    r = 204;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
  s->formatter->flush(s);
}

void RGWPutObj_REST_OS::send_response()
{
  if (!ret)
    ret = 201; // "created"
  dump_etag(s, etag.c_str());
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
  s->formatter->flush(s);
}

void RGWDeleteObj_REST_OS::send_response()
{
  int r = ret;
  if (!r)
    r = 204;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
  s->formatter->flush(s);
}

int RGWGetObj_REST_OS::send_response(void *handle)
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

  if (ret)
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
  s->formatter->flush(s);

  return 0;
}

RGWOp *RGWHandler_REST_OS::get_retrieve_obj_op(struct req_state *s, bool get_data)
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

  if (get_data)
    return &list_bucket_op;
  else
    return &stat_bucket_op;
}

RGWOp *RGWHandler_REST_OS::get_retrieve_op(struct req_state *s, bool get_data)
{
  if (s->bucket) {
    if (is_acl_op(s)) {
      return &get_acls_op;
    }
    return get_retrieve_obj_op(s, get_data);
  }

  return &list_buckets_op;
}

RGWOp *RGWHandler_REST_OS::get_create_op(struct req_state *s)
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

RGWOp *RGWHandler_REST_OS::get_delete_op(struct req_state *s)
{
  if (s->object)
    return &delete_obj_op;
  else if (s->bucket)
    return &delete_bucket_op;

  return NULL;
}

bool RGWHandler_REST_OS::authorize(struct req_state *s)
{
  return rgw_verify_os_token(s);
}
