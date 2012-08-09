
#include "common/Formatter.h"
#include "rgw_swift.h"
#include "rgw_rest_swift.h"
#include "rgw_acl_swift.h"

#ifdef FASTCGI_INCLUDE_DIR
# include "fastcgi/fcgiapp.h"
#else
# include "fcgiapp.h"
#endif

#include <sstream>

#define dout_subsys ceph_subsys_rgw

int RGWListBuckets_REST_SWIFT::get_params()
{
  marker = s->args.get("marker");
  string limit_str;
  limit_str = s->args.get("limit");
  limit = strtol(limit_str.c_str(), NULL, 10);
  if (limit > limit_max || limit < 0)
    return -ERR_PRECONDITION_FAILED;

  if (limit == 0)
    limit = limit_max;

  return 0;
}

void RGWListBuckets_REST_SWIFT::send_response()
{
  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  map<string, RGWBucketEnt>::iterator iter;

  if (ret < 0)
    goto done;

  dump_start(s);

  s->formatter->open_array_section("account");

  if (marker.empty())
    iter = m.begin();
  else
    iter = m.upper_bound(marker);

  for (int i = 0; i < limit && iter != m.end(); ++iter, ++i) {
    RGWBucketEnt obj = iter->second;
    s->formatter->open_object_section("container");
    s->formatter->dump_string("name", obj.bucket.name);
    s->formatter->dump_int("count", obj.count);
    s->formatter->dump_int("bytes", obj.size);
    s->formatter->close_section();
  }
  s->formatter->close_section();

  if (!ret && s->formatter->get_len() == 0)
    ret = STATUS_NO_CONTENT;
done:
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);

  if (ret < 0) {
    return;
  }

  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWListBucket_REST_SWIFT::get_params()
{
  prefix = s->args.get("prefix");
  marker = s->args.get("marker");
  max_keys = s->args.get("limit");
  ret = parse_max_keys();
  if (ret < 0) {
    return ret;
  }
  if (max > default_max)
    return -ERR_PRECONDITION_FAILED;

  delimiter = s->args.get("delimiter");

  string path_args;
  if (s->args.exists("path")) { // should handle empty path
    path_args = s->args.get("path");
    if (!delimiter.empty() || !prefix.empty()) {
      return -EINVAL;
    }
    prefix = path_args;
    delimiter="/";

    path = prefix;
    if (path.size() && path[path.size() - 1] != '/')
      path.append("/");
  }

  int len = prefix.size();
  int delim_size = delimiter.size();
  if (len >= delim_size) {
    if (prefix.substr(len - delim_size).compare(delimiter) != 0)
      prefix.append(delimiter);
  }

  return 0;
}

void RGWListBucket_REST_SWIFT::send_response()
{
  vector<RGWObjEnt>::iterator iter = objs.begin();
  map<string, bool>::iterator pref_iter = common_prefixes.begin();

  dump_start(s);

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
      if (iter->name.compare(path) == 0)
        goto next;

      s->formatter->open_object_section("object");
      s->formatter->dump_string("name", iter->name);
      s->formatter->dump_string("hash", iter->etag);
      s->formatter->dump_int("bytes", iter->size);
      string single_content_type = iter->content_type;
      if (iter->content_type.size()) {
        // content type might hold multiple values, just dump the last one
        size_t pos = iter->content_type.rfind(',');
        if (pos > 0) {
          ++pos;
          while (single_content_type[pos] == ' ')
            ++pos;
          single_content_type = single_content_type.substr(pos);
        }
        s->formatter->dump_string("content_type", single_content_type);
      }
      dump_time(s, "last_modified", &iter->mtime);
      s->formatter->close_section();
    }

    if (do_pref &&  (marker.empty() || pref_iter->first.compare(marker) > 0)) {
      const string& name = pref_iter->first;
      if (name.compare(delimiter) == 0)
        goto next;

      s->formatter->open_object_section("object");
      s->formatter->dump_string("name", pref_iter->first);
      s->formatter->close_section();
    }
next:
    if (do_objs)
      iter++;
    else
      pref_iter++;
  }

  s->formatter->close_section();

  if (!ret && s->formatter->get_len() == 0)
    ret = STATUS_NO_CONTENT;
  else if (ret > 0)
    ret = 0;

  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
  if (ret < 0) {
    return;
  }

  rgw_flush_formatter_and_reset(s, s->formatter);
}

static void dump_container_metadata(struct req_state *s, RGWBucketEnt& bucket)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", (long long)bucket.count);
  CGI_PRINTF(s,"X-Container-Object-Count: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)bucket.size);
  CGI_PRINTF(s,"X-Container-Bytes-Used: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)bucket.size_rounded);
  CGI_PRINTF(s,"X-Container-Bytes-Used-Actual: %s\n", buf);

  if (!s->object) {
    RGWAccessControlPolicy_SWIFT *swift_policy = static_cast<RGWAccessControlPolicy_SWIFT *>(s->bucket_acl);
    string read_acl, write_acl;
    swift_policy->to_str(read_acl, write_acl);
    if (read_acl.size()) {
      CGI_PRINTF(s, "X-Container-Read: %s\r\n", read_acl.c_str());
    }
    if (write_acl.size()) {
      CGI_PRINTF(s, "X-Container-Write: %s\r\n", write_acl.c_str());
    }
  }
}

static void dump_account_metadata(struct req_state *s, uint32_t buckets_count,
                                  uint64_t buckets_object_count, uint64_t buckets_size, uint64_t buckets_size_rounded)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", (long long)buckets_count);
  CGI_PRINTF(s,"X-Account-Container-Count: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)buckets_object_count);
  CGI_PRINTF(s,"X-Account-Object-Count: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)buckets_size);
  CGI_PRINTF(s,"X-Account-Bytes-Used: %s\n", buf);
  snprintf(buf, sizeof(buf), "%lld", (long long)buckets_size_rounded);
  CGI_PRINTF(s,"X-Account-Bytes-Used-Actual: %s\n", buf);
}

void RGWStatAccount_REST_SWIFT::send_response()
{
  if (ret >= 0) {
    ret = STATUS_NO_CONTENT;
    dump_account_metadata(s, buckets_count, buckets_objcount, buckets_size, buckets_size_rounded);
  }

  set_req_state_err(s, ret);
  dump_errno(s);

  end_header(s);
  dump_start(s);
}

void RGWStatBucket_REST_SWIFT::send_response()
{
  if (ret >= 0) {
    ret = STATUS_NO_CONTENT;
    dump_container_metadata(s, bucket);
  }

  set_req_state_err(s, ret);
  dump_errno(s);

  end_header(s);
  dump_start(s);
}

int RGWCreateBucket_REST_SWIFT::get_params()
{
  policy.create_default(s->user.user_id, s->user.display_name);

  return 0;
}

void RGWCreateBucket_REST_SWIFT::send_response()
{
  if (!ret)
    ret = STATUS_CREATED;
  else if (ret == -ERR_BUCKET_EXISTS)
    ret = STATUS_ACCEPTED;
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWDeleteBucket_REST_SWIFT::send_response()
{
  int r = ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWPutObj_REST_SWIFT::get_params()
{
  if (s->has_bad_meta)
    return -EINVAL;

  if (!s->length) {
    const char *encoding = s->env->get("HTTP_TRANSFER_ENCODING");
    if (!encoding || strcmp(encoding, "chunked") != 0)
      return -ERR_LENGTH_REQUIRED;

    chunked_upload = true;
  }

  supplied_etag = s->env->get("HTTP_ETAG");

  if (!s->content_type) {
    dout(5) << "content type wasn't provided, trying to guess" << dendl;
    const char *suffix = strrchr(s->object, '.');
    if (suffix) {
      suffix++;
      if (*suffix) {
        string suffix_str(suffix);
        s->content_type = rgw_find_mime_by_ext(suffix_str);
      }
    }
  }

  policy.create_default(s->user.user_id, s->user.display_name);

  return RGWPutObj_REST::get_params();
}

void RGWPutObj_REST_SWIFT::send_response()
{
  if (!ret)
    ret = STATUS_CREATED;
  dump_etag(s, etag.c_str());
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWPutMetadata_REST_SWIFT::get_params()
{
  if (s->has_bad_meta)
    return -EINVAL;

  if (!s->object) {
    string read_list, write_list;

    const char *read_attr = s->env->get("HTTP_X_CONTAINER_READ");
    if (read_attr) {
      read_list = read_attr;
    }
    const char *write_attr = s->env->get("HTTP_X_CONTAINER_WRITE");
    if (write_attr) {
      write_list = write_attr;
    }

    if (read_attr || write_attr) {
      RGWAccessControlPolicy_SWIFT swift_policy(s->cct);
      int r = swift_policy.create(store, s->user.user_id, s->user.display_name, read_list, write_list);
      if (r < 0)
        return r;

      policy = swift_policy;
      has_policy = true;
    }
  }

  return 0;
}

void RGWPutMetadata_REST_SWIFT::send_response()
{
  if (!ret)
    ret = STATUS_ACCEPTED;
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWDeleteObj_REST_SWIFT::send_response()
{
  int r = ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWCopyObj_REST_SWIFT::init_dest_policy()
{
  dest_policy.create_default(s->user.user_id, s->user.display_name);

  return 0;
}

int RGWCopyObj_REST_SWIFT::get_params()
{
  if_mod = s->env->get("HTTP_IF_MODIFIED_SINCE");
  if_unmod = s->env->get("HTTP_IF_UNMODIFIED_SINCE");
  if_match = s->env->get("HTTP_COPY_IF_MATCH");
  if_nomatch = s->env->get("HTTP_COPY_IF_NONE_MATCH");

  if (s->op == OP_COPY) {
    const char *req_dest = s->env->get("HTTP_DESTINATION");
    if (!req_dest)
      return -ERR_BAD_URL;

    ret = parse_copy_location(req_dest, dest_bucket_name, dest_object);
    if (!ret)
       return -ERR_BAD_URL;
    src_bucket_name = s->bucket_name;
    src_object = s->object_str;
  } else {
    const char *req_src = s->copy_source;
    if (!req_src)
      return -ERR_BAD_URL;

    ret = parse_copy_location(req_src, src_bucket_name, src_object);
    if (!ret)
       return -ERR_BAD_URL;

    dest_bucket_name = s->bucket_name;
    dest_object = s->object_str;
  }

  return 0;
}

void RGWCopyObj_REST_SWIFT::send_response()
{
  if (!ret)
    ret = STATUS_CREATED;
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
}

int RGWGetObj_REST_SWIFT::send_response(bufferlist& bl)
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
         name += sizeof(RGW_ATTR_META_PREFIX) - 1;
         CGI_PRINTF(s, "X-%s-Meta-%s: %s\r\n", (s->object ? "Object" : "Container"), name, iter->second.c_str());
       } else if (!content_type && strcmp(name, RGW_ATTR_CONTENT_TYPE) == 0) {
         content_type = iter->second.c_str();
       }
    }
  }

  if (partial_content && !ret)
    ret = -STATUS_PARTIAL_CONTENT;

  if (ret)
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
  rgw_flush_formatter_and_reset(s, s->formatter);

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

RGWOp *RGWHandler_REST_SWIFT::get_post_op()
{
  return new RGWPutMetadata_REST_SWIFT;
}

RGWOp *RGWHandler_REST_SWIFT::get_copy_op()
{
  if (s->object)
    return new RGWCopyObj_REST_SWIFT;

  return NULL;
}

int RGWHandler_REST_SWIFT::authorize()
{
  bool authorized = rgw_verify_swift_token(store, s);
  if (!authorized)
    return -EPERM;

  s->perm_mask = RGW_PERM_FULL_CONTROL;

  return 0;
}

int RGWHandler_REST_SWIFT::init(RGWRados *store, struct req_state *state, FCGX_Request *fcgx)
{
  state->copy_source = state->env->get("HTTP_X_COPY_FROM");

  state->dialect = "swift";

  return RGWHandler_REST::init(store, state, fcgx);
}
