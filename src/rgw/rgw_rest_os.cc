
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

  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWObjEnt obj = iter->second;
    s->formatter->open_obj_section("container");
    s->formatter->dump_value_str("name", obj.name.c_str());
    /* FIXME: missing count, bytes */
    s->formatter->close_section("container");
  }
  s->formatter->close_section("account");
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
