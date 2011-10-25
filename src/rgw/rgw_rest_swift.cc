
#include "common/Formatter.h"
#include "rgw_swift.h"
#include "rgw_rest_swift.h"

#include <sstream>

#define DOUT_SUBSYS rgw

void RGWListBuckets_REST_SWIFT::send_response()
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
    s->formatter->open_object_section("container");
    s->formatter->dump_format("name", obj.bucket.name.c_str());
    s->formatter->dump_int("count", obj.count);
    s->formatter->dump_int("bytes", obj.size);
    s->formatter->close_section();
  }
  s->formatter->close_section();

  ostringstream oss;
  s->formatter->flush(oss);
  std::string outs(oss.str());
  string::size_type outs_size = outs.size();
  dump_content_length(s, outs_size);
  end_header(s);
  if (!outs.empty()) {
    CGI_PutStr(s, outs.c_str(), outs_size);
  }
  s->formatter->reset();
}

void RGWListBucket_REST_SWIFT::send_response()
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
      s->formatter->open_object_section("object");
      s->formatter->dump_format("name", iter->name.c_str());
      s->formatter->dump_format("hash", "\"%s\"", iter->etag.c_str());
      s->formatter->dump_int("bytes", iter->size);
      if (iter->content_type.size())
        s->formatter->dump_format("content_type", iter->content_type.c_str());
      dump_time(s, "last_modified", &iter->mtime);
      s->formatter->close_section();
    }

    if (do_pref &&  (marker.empty() || pref_iter->first.compare(marker) > 0)) {
      s->formatter->open_object_section("object");
      s->formatter->dump_format("name", pref_iter->first.c_str());
      s->formatter->close_section();
    }
    if (do_objs)
      iter++;
    else
      pref_iter++;
  }

  s->formatter->close_section();

  end_header(s);
  flush_formatter_to_req_state(s, s->formatter);
}

static void dump_container_metadata(struct req_state *s, RGWBucketEnt& bucket)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", (long long)bucket.count);
  CGI_PRINTF(s,"X-Container-Object-Count: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)bucket.size);
  CGI_PRINTF(s,"X-Container-Bytes-Used: %s\n", buf);
}

static void dump_account_metadata(struct req_state *s, uint32_t buckets_count,
                                  uint64_t buckets_object_count, uint64_t buckets_size)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", (long long)buckets_count);
  CGI_PRINTF(s,"X-Account-Container-Count: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)buckets_object_count);
  CGI_PRINTF(s,"X-Account-Object-Count: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)buckets_size);
  CGI_PRINTF(s,"X-Account-Bytes-Used: %s\n", buf);
}

void RGWStatAccount_REST_SWIFT::send_response()
{
  if (ret >= 0) {
    ret = 204;
    dump_account_metadata(s, buckets_count, buckets_objcount, buckets_size);
  }

  set_req_state_err(s, ret);
  dump_errno(s);

  end_header(s);
  dump_start(s);
}

void RGWStatBucket_REST_SWIFT::send_response()
{
  if (ret >= 0) {
    ret = 204;
    dump_container_metadata(s, bucket);
  }

  set_req_state_err(s, ret);
  dump_errno(s);

  end_header(s);
  dump_start(s);
}

void RGWCreateBucket_REST_SWIFT::send_response()
{
  if (!ret)
    ret = 201; // "created"
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
  flush_formatter_to_req_state(s, s->formatter);
}

void RGWDeleteBucket_REST_SWIFT::send_response()
{
  int r = ret;
  if (!r)
    r = 204;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
  flush_formatter_to_req_state(s, s->formatter);
}

void RGWPutObj_REST_SWIFT::send_response()
{
  if (!ret)
    ret = 201; // "created"
  dump_etag(s, etag.c_str());
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
  flush_formatter_to_req_state(s, s->formatter);
}

void RGWDeleteObj_REST_SWIFT::send_response()
{
  int r = ret;
  if (!r)
    r = 204;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
  flush_formatter_to_req_state(s, s->formatter);
}

int RGWGetObj_REST_SWIFT::send_response(void *handle)
{
  const char *content_type = NULL;
  int orig_ret = ret;

  if (sent_header)
    goto send_data;

  if (range_str)
    dump_range(s, ofs, start, s->obj_size);

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
  flush_formatter_to_req_state(s, s->formatter);

  return 0;
}

RGWOp *RGWHandler_REST_SWIFT::get_retrieve_obj_op(bool get_data)
{
  if (is_acl_op()) {
    return new RGWGetACLs_REST_SWIFT;
  }

  if (s->object) {
    RGWGetObj_REST_SWIFT *get_obj_op = new RGWGetObj_REST_SWIFT;
    get_obj_op->set_get_data(get_data);
    return get_obj_op;
  } else if (!s->bucket_name) {
    return NULL;
  }

  if (get_data)
    return new RGWListBucket_REST_SWIFT;
  else
    return new RGWStatBucket_REST_SWIFT;
}

RGWOp *RGWHandler_REST_SWIFT::get_retrieve_op(bool get_data)
{
  if (s->bucket_name) {
    if (is_acl_op()) {
      return new RGWGetACLs_REST_SWIFT;
    }
    return get_retrieve_obj_op(get_data);
  }

  if (get_data)
    return new RGWListBuckets_REST_SWIFT;
  else
    return new RGWStatAccount_REST_SWIFT;
}

RGWOp *RGWHandler_REST_SWIFT::get_create_op()
{
  if (is_acl_op()) {
    return new RGWPutACLs_REST_SWIFT;
  } else if (s->object) {
    if (!s->copy_source)
      return new RGWPutObj_REST_SWIFT;
    else
      return new RGWCopyObj_REST_SWIFT;
  } else if (s->bucket_name) {
    return new RGWCreateBucket_REST_SWIFT;
  }

  return NULL;
}

RGWOp *RGWHandler_REST_SWIFT::get_delete_op()
{
  if (s->object)
    return new RGWDeleteObj_REST_SWIFT;
  else if (s->bucket_name)
    return new RGWDeleteBucket_REST_SWIFT;

  return NULL;
}

int RGWHandler_REST_SWIFT::authorize()
{
  bool authorized = rgw_verify_os_token(s);
  if (!authorized)
    return -EPERM;

  s->perm_mask = RGW_PERM_FULL_CONTROL;

  return 0;
}
