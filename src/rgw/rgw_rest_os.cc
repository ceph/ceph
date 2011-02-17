
#include "rgw_os.h"
#include "rgw_rest_os.h"

void RGWListBuckets_REST_OS::send_response()
{
  dump_errno(s, ret);
  end_header(s);
  dump_start(s);

  s->formatter->open_array_section("account");

  // dump_owner(s, s->user.user_id, s->user.display_name);

  map<string, RGWObjEnt>& m = buckets.get_buckets();
  map<string, RGWObjEnt>::iterator iter;

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
    RGWObjEnt obj = iter->second;
    s->formatter->open_obj_section("container");
    s->formatter->dump_value_str("name", obj.name.c_str());
    /* FIXME: missing count, bytes */
    s->formatter->close_section("container");
  }
  s->formatter->close_section("account");
}

void RGWListBucket_REST_OS::send_response()
{
  dump_errno(s, (ret < 0 ? ret : 0));

  end_header(s);
  dump_start(s);
  if (ret < 0)
    return;

  vector<RGWObjEnt>::iterator iter = objs.begin();

  s->formatter->open_array_section("container");

  for (; iter != objs.end(); ++iter) {
    if (!marker.empty() && iter->name.compare(marker) <= 0) {
      /* we expect marker to be upper-bount, whereas lower layer uses marker
         as the starting point (like S3) */
      continue;
    }
    s->formatter->open_obj_section("object");
    s->formatter->dump_value_str("name", iter->name.c_str());
    s->formatter->dump_value_str("hash", "&quot;%s&quot;", iter->etag);
    s->formatter->dump_value_int("bytes", "%lld", iter->size);
    dump_time(s, "last_modified", &iter->mtime);
    s->formatter->close_section("object");
  }
#if 0
    if (common_prefixes.size() > 0) {
      s->formatter->open_array_section("CommonPrefixes");
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin(); pref_iter != common_prefixes.end(); ++pref_iter) {
        s->formatter->dump_value_str("Prefix", pref_iter->first.c_str());
      }
      s->formatter->close_section("CommonPrefixes");
    }
#endif
  s->formatter->close_section("container");
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

  return &list_bucket_op;
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
