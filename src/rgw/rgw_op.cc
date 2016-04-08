// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <stdlib.h>

#include <sstream>

#include "common/Clock.h"
#include "common/armor.h"
#include "common/mime.h"
#include "common/utf8.h"
#include "common/ceph_json.h"

#include "rgw_rados.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_acl_swift.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_multi.h"
#include "rgw_multi_del.h"
#include "rgw_cors.h"
#include "rgw_cors_s3.h"
#include "rgw_rest_conn.h"
#include "rgw_rest_s3.h"
#include "rgw_client_io.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;
using ceph::crypto::MD5;

static string mp_ns = RGW_OBJ_NS_MULTIPART;
static string shadow_ns = RGW_OBJ_NS_SHADOW;

static int forward_request_to_master(struct req_state *s, obj_version *objv, RGWRados *store, bufferlist& in_data, JSONParser *jp);

#define MULTIPART_UPLOAD_ID_PREFIX_LEGACY "2/"
#define MULTIPART_UPLOAD_ID_PREFIX "2~" // must contain a unique char that may not come up in gen_rand_alpha()

class MultipartMetaFilter : public RGWAccessListFilter {
public:
  MultipartMetaFilter() {}
  bool filter(string& name, string& key) {
    int len = name.size();
    if (len < 6)
      return false;

    int pos = name.find(MP_META_SUFFIX, len - 5);
    if (pos <= 0)
      return false;

    pos = name.rfind('.', pos - 1);
    if (pos < 0)
      return false;

    key = name.substr(0, pos);

    return true;
  }
};

static MultipartMetaFilter mp_filter;

static int parse_range(const char *range, off_t& ofs, off_t& end, bool *partial_content)
{
  int r = -ERANGE;
  string s(range);
  string ofs_str;
  string end_str;

  *partial_content = false;

  int pos = s.find("bytes=");
  if (pos < 0) {
    pos = 0;
    while (isspace(s[pos]))
      pos++;
    int end = pos;
    while (isalpha(s[end]))
      end++;
    if (strncasecmp(s.c_str(), "bytes", end - pos) != 0)
      return 0;
    while (isspace(s[end]))
      end++;
    if (s[end] != '=')
      return 0;
    s = s.substr(end + 1);
  } else {
    s = s.substr(pos + 6); /* size of("bytes=")  */
  }
  pos = s.find('-');
  if (pos < 0)
    goto done;

  *partial_content = true;

  ofs_str = s.substr(0, pos);
  end_str = s.substr(pos + 1);
  if (end_str.length()) {
    end = atoll(end_str.c_str());
    if (end < 0)
      goto done;
  }

  if (ofs_str.length()) {
    ofs = atoll(ofs_str.c_str());
  } else { // RFC2616 suffix-byte-range-spec
    ofs = -end;
    end = -1;
  }

  if (end >= 0 && end < ofs)
    goto done;

  r = 0;
done:
  return r;
}

static int decode_policy(CephContext *cct, bufferlist& bl, RGWAccessControlPolicy *policy)
{
  bufferlist::iterator iter = bl.begin();
  try {
    policy->decode(iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(policy);
    ldout(cct, 15) << __func__ << " Read AccessControlPolicy";
    s3policy->to_xml(*_dout);
    *_dout << dendl;
  }
  return 0;
}

static int get_bucket_policy_from_attr(CephContext *cct, RGWRados *store, void *ctx,
                                       RGWBucketInfo& bucket_info, map<string, bufferlist>& bucket_attrs,
                                       RGWAccessControlPolicy *policy, rgw_obj& obj)
{
  map<string, bufferlist>::iterator aiter = bucket_attrs.find(RGW_ATTR_ACL);

  if (aiter != bucket_attrs.end()) {
    int ret = decode_policy(cct, aiter->second, policy);
    if (ret < 0)
      return ret;
  } else {
    ldout(cct, 0) << "WARNING: couldn't find acl header for bucket, generating default" << dendl;
    RGWUserInfo uinfo;
    /* object exists, but policy is broken */
    int r = rgw_get_user_info_by_uid(store, bucket_info.owner, uinfo);
    if (r < 0)
      return r;

    policy->create_default(bucket_info.owner, uinfo.display_name);
  }
  return 0;
}

static int get_obj_policy_from_attr(CephContext *cct, RGWRados *store, RGWObjectCtx& obj_ctx,
                                    RGWBucketInfo& bucket_info, map<string, bufferlist>& bucket_attrs,
                                    RGWAccessControlPolicy *policy, rgw_obj& obj)
{
  bufferlist bl;
  int ret = 0;

  RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read rop(&op_target);

  ret = rop.get_attr(RGW_ATTR_ACL, bl);
  if (ret >= 0) {
    ret = decode_policy(cct, bl, policy);
    if (ret < 0)
      return ret;
  } else if (ret == -ENODATA) {
    /* object exists, but policy is broken */
    ldout(cct, 0) << "WARNING: couldn't find acl header for object, generating default" << dendl;
    RGWUserInfo uinfo;
    ret = rgw_get_user_info_by_uid(store, bucket_info.owner, uinfo);
    if (ret < 0)
      return ret;

    policy->create_default(bucket_info.owner, uinfo.display_name);
  }
  return ret;
}


/**
 * Get the AccessControlPolicy for an object off of disk.
 * policy: must point to a valid RGWACL, and will be filled upon return.
 * bucket: name of the bucket containing the object.
 * object: name of the object to get the ACL for.
 * Returns: 0 on success, -ERR# otherwise.
 */
static int get_policy_from_attr(CephContext *cct, RGWRados *store, void *ctx,
                                RGWBucketInfo& bucket_info, map<string, bufferlist>& bucket_attrs,
                                RGWAccessControlPolicy *policy, rgw_obj& obj)
{
  if (obj.bucket.name.empty()) {
    return 0;
  }

  if (obj.get_object().empty()) {
    rgw_obj instance_obj;
    store->get_bucket_instance_obj(bucket_info.bucket, instance_obj);
    return get_bucket_policy_from_attr(cct, store, ctx, bucket_info, bucket_attrs,
                                       policy, instance_obj);
  }
  return get_obj_policy_from_attr(cct, store, *static_cast<RGWObjectCtx *>(ctx), bucket_info, bucket_attrs,
                                  policy, obj);
}

static int get_obj_attrs(RGWRados *store, struct req_state *s, rgw_obj& obj, map<string, bufferlist>& attrs)
{
  RGWRados::Object op_target(store, s->bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;
  read_op.params.perr = &s->err;

  return read_op.prepare(NULL, NULL);
}

static int get_system_obj_attrs(RGWRados *store, struct req_state *s, rgw_obj& obj, map<string, bufferlist>& attrs,
                         uint64_t *obj_size, RGWObjVersionTracker *objv_tracker)
{
  RGWRados::SystemObject src(store, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  RGWRados::SystemObject::Read rop(&src);

  rop.stat_params.attrs = &attrs;
  rop.stat_params.obj_size = obj_size;

  int ret = rop.stat(objv_tracker);
  return ret;
}

static int read_policy(RGWRados *store, struct req_state *s,
                       RGWBucketInfo& bucket_info, map<string, bufferlist>& bucket_attrs,
                       RGWAccessControlPolicy *policy, rgw_bucket& bucket, rgw_obj_key& object)
{
  string upload_id;
  upload_id = s->info.args.get("uploadId");
  rgw_obj obj;

  if (!s->system_request && bucket_info.flags & BUCKET_SUSPENDED) {
    ldout(s->cct, 0) << "NOTICE: bucket " << bucket_info.bucket.name << " is suspended" << dendl;
    return -ERR_USER_SUSPENDED;
  }

  if (!object.empty() && !upload_id.empty()) {
    /* multipart upload */
    RGWMPObj mp(object.name, upload_id);
    string oid = mp.get_meta();
    obj.init_ns(bucket, oid, mp_ns);
    obj.set_in_extra_data(true);
  } else {
    obj = rgw_obj(bucket, object);
  }
  int ret = get_policy_from_attr(s->cct, store, s->obj_ctx, bucket_info, bucket_attrs, policy, obj);
  if (ret == -ENOENT && !object.empty()) {
    /* object does not exist checking the bucket's ACL to make sure
       that we send a proper error code */
    RGWAccessControlPolicy bucket_policy(s->cct);
    string no_object;
    rgw_obj no_obj(bucket, no_object);
    ret = get_policy_from_attr(s->cct, store, s->obj_ctx, bucket_info, bucket_attrs, &bucket_policy, no_obj);
    if (ret < 0)
      return ret;
    rgw_user& owner = bucket_policy.get_owner().get_id();
    if (!s->system_request && owner.compare(s->user->user_id) != 0 &&
        !bucket_policy.verify_permission(s->user->user_id, s->perm_mask,
					RGW_PERM_READ))
      ret = -EACCES;
    else
      ret = -ENOENT;

  } else if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_BUCKET;
  }

  return ret;
}

/**
 * Get the AccessControlPolicy for a bucket or object off of disk.
 * s: The req_state to draw information from.
 * only_bucket: If true, reads the bucket ACL rather than the object ACL.
 * Returns: 0 on success, -ERR# otherwise.
 */
int rgw_build_bucket_policies(RGWRados* store, struct req_state* s)
{
  int ret = 0;
  rgw_obj_key obj;
  RGWUserInfo bucket_owner_info;
  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);

  string bi = s->info.args.get(RGW_SYS_PARAM_PREFIX "bucket-instance");
  if (!bi.empty()) {
    ret = rgw_bucket_parse_bucket_instance(bi, &s->bucket_instance_id, &s->bucket_instance_shard_id);
    if (ret < 0) {
      return ret;
    }
  }

  if(s->dialect.compare("s3") == 0) {
    s->bucket_acl = new RGWAccessControlPolicy_S3(s->cct);
  } else if(s->dialect.compare("swift")  == 0) {
    s->bucket_acl = new RGWAccessControlPolicy_SWIFT(s->cct);
  } else {
    s->bucket_acl = new RGWAccessControlPolicy(s->cct);
  }

  /* check if copy source is within the current domain */
  if (!s->src_bucket_name.empty()) {
    RGWBucketInfo source_info;

    if (s->bucket_instance_id.empty()) {
      ret = store->get_bucket_info(obj_ctx, s->src_tenant_name, s->src_bucket_name, source_info, NULL);
    } else {
      ret = store->get_bucket_instance_info(obj_ctx, s->bucket_instance_id, source_info, NULL, NULL);
    }
    if (ret == 0) {
      string& zonegroup = source_info.zonegroup;
      s->local_source = store->get_zonegroup().equals(zonegroup);
    }
  }

  if (!s->bucket_name.empty()) {
    s->bucket_exists = true;
    if (s->bucket_instance_id.empty()) {
      ret = store->get_bucket_info(obj_ctx, s->bucket_tenant, s->bucket_name, s->bucket_info, NULL, &s->bucket_attrs);
    } else {
      ret = store->get_bucket_instance_info(obj_ctx, s->bucket_instance_id, s->bucket_info, NULL, &s->bucket_attrs);
    }
    if (ret < 0) {
      if (ret != -ENOENT) {
        string bucket_log;
        rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name, bucket_log);
        ldout(s->cct, 0) << "NOTICE: couldn't get bucket from bucket_name (name=" << bucket_log << ")" << dendl;
        return ret;
      }
      s->bucket_exists = false;
    }
    s->bucket = s->bucket_info.bucket;

    if (s->bucket_exists) {
      rgw_obj_key no_obj;
      ret = read_policy(store, s, s->bucket_info, s->bucket_attrs, s->bucket_acl, s->bucket, no_obj);
    } else {
      s->bucket_acl->create_default(s->user->user_id, s->user->display_name);
      ret = -ERR_NO_SUCH_BUCKET;
    }

    s->bucket_owner = s->bucket_acl->get_owner();

    RGWZoneGroup zonegroup;
    int r = store->get_zonegroup(s->bucket_info.zonegroup, zonegroup);
    if (!r) {
      if (!zonegroup.endpoints.empty()) {
	s->zonegroup_endpoint = zonegroup.endpoints.front();
      }
      s->zonegroup_name = zonegroup.get_name();
    }
    if (r < 0 && ret == 0) {
      ret = r;
    }

    if (s->bucket_exists && !store->get_zonegroup().equals(s->bucket_info.zonegroup)) {
      ldout(s->cct, 0) << "NOTICE: request for data in a different zonegroup (" << s->bucket_info.zonegroup << " != " << store->get_zonegroup().get_id() << ")" << dendl;
      /* we now need to make sure that the operation actually requires copy source, that is
       * it's a copy operation
       */
      if (store->get_zonegroup().is_master && s->op == OP_DELETE && s->system_request) {
        /*If the operation is delete and if this is the master, don't redirect*/
      } else if (!s->local_source ||
          (s->op != OP_PUT && s->op != OP_COPY) ||
          s->object.empty()) {
        return -ERR_PERMANENT_REDIRECT;
      }
    }
  }

  return ret;
}

/**
 * Get the AccessControlPolicy for a bucket or object off of disk.
 * s: The req_state to draw information from.
 * only_bucket: If true, reads the bucket ACL rather than the object ACL.
 * Returns: 0 on success, -ERR# otherwise.
 */
int rgw_build_object_policies(RGWRados *store, struct req_state *s,
			      bool prefetch_data)
{
  int ret = 0;

  if (!s->object.empty()) {
    if (!s->bucket_exists) {
      return -ERR_NO_SUCH_BUCKET;
    }
    s->object_acl = new RGWAccessControlPolicy(s->cct);

    rgw_obj obj(s->bucket, s->object);
    store->set_atomic(s->obj_ctx, obj);
    if (prefetch_data) {
      store->set_prefetch_data(s->obj_ctx, obj);
    }
    ret = read_policy(store, s, s->bucket_info, s->bucket_attrs, s->object_acl, s->bucket, s->object);
  }

  return ret;
}

static void rgw_bucket_object_pre_exec(struct req_state *s)
{
  if (s->expect_cont)
    dump_continue(s);

  dump_bucket_from_state(s);
}

int RGWGetObj::verify_permission()
{
  obj = rgw_obj(s->bucket, s->object);
  store->set_atomic(s->obj_ctx, obj);
  if (get_data)
    store->set_prefetch_data(s->obj_ctx, obj);

  if (!verify_object_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}


int RGWOp::verify_op_mask()
{
  uint32_t required_mask = op_mask();

  ldout(s->cct, 20) << "required_mask= " << required_mask
		    << " user.op_mask=" << s->user->op_mask << dendl;

  if ((s->user->op_mask & required_mask) != required_mask) {
    return -EPERM;
  }

  if (!s->system_request && (required_mask & RGW_OP_TYPE_MODIFY) && store->get_zone().is_read_only()) {
    ldout(s->cct, 5) << "NOTICE: modify request to a read-only zone by a non-system user, permission denied"  << dendl;
    return -EPERM;
  }

  return 0;
}

int RGWOp::do_aws4_auth_completion()
{
  int ret;

  if (s->aws4_auth_needs_complete) {
    /* complete */
    ret = RGW_Auth_S3::authorize_aws4_auth_complete(store, s);
    s->aws4_auth_needs_complete = false;
    if (ret) {
      return ret;
    }
    /* verify signature */
    if (s->aws4_auth->signature != s->aws4_auth->new_signature) {
      ret = -ERR_SIGNATURE_NO_MATCH;
      ldout(s->cct, 20) << "delayed aws4 auth failed" << dendl;
      return ret;
    }
    /* authorization ok */
    dout(10) << "v4 auth ok" << dendl;
  }

  return 0;
}

int RGWOp::init_quota()
{
  /* no quota enforcement for system requests */
  if (s->system_request)
    return 0;

  /* init quota related stuff */
  if (!(s->user->op_mask & RGW_OP_TYPE_MODIFY)) {
    return 0;
  }

  /* only interested in object related ops */
  if (s->object.empty()) {
    return 0;
  }

  RGWUserInfo owner_info;
  RGWUserInfo *uinfo;

  if (s->user->user_id == s->bucket_owner.get_id()) {
    uinfo = s->user;
  } else {
    int r = rgw_get_user_info_by_uid(store, s->bucket_info.owner, owner_info);
    if (r < 0)
      return r;
    uinfo = &owner_info;
  }

  if (s->bucket_info.quota.enabled) {
    bucket_quota = s->bucket_info.quota;
  } else if (uinfo->bucket_quota.enabled) {
    bucket_quota = uinfo->bucket_quota;
  } else {
    bucket_quota = store->get_bucket_quota();
  }

  if (uinfo->user_quota.enabled) {
    user_quota = uinfo->user_quota;
  } else {
    user_quota = store->get_user_quota();
  }

  return 0;
}

static bool validate_cors_rule_method(RGWCORSRule *rule, const char *req_meth) {
  uint8_t flags = 0;

  if (!req_meth) {
    dout(5) << "req_meth is null" << dendl;
    return false;
  }

  if (strcmp(req_meth, "GET") == 0) flags = RGW_CORS_GET;
  else if (strcmp(req_meth, "POST") == 0) flags = RGW_CORS_POST;
  else if (strcmp(req_meth, "PUT") == 0) flags = RGW_CORS_PUT;
  else if (strcmp(req_meth, "DELETE") == 0) flags = RGW_CORS_DELETE;
  else if (strcmp(req_meth, "HEAD") == 0) flags = RGW_CORS_HEAD;

  if ((rule->get_allowed_methods() & flags) == flags) {
    dout(10) << "Method " << req_meth << " is supported" << dendl;
  } else {
    dout(5) << "Method " << req_meth << " is not supported" << dendl;
    return false;
  }

  return true;
}

int RGWOp::read_bucket_cors()
{
  bufferlist bl;

  map<string, bufferlist>::iterator aiter = s->bucket_attrs.find(RGW_ATTR_CORS);
  if (aiter == s->bucket_attrs.end()) {
    ldout(s->cct, 20) << "no CORS configuration attr found" << dendl;
    cors_exist = false;
    return 0; /* no CORS configuration found */
  }

  cors_exist = true;

  bl = aiter->second;

  bufferlist::iterator iter = bl.begin();
  try {
    bucket_cors.decode(iter);
  } catch (buffer::error& err) {
    ldout(s->cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    RGWCORSConfiguration_S3 *s3cors = static_cast<RGWCORSConfiguration_S3 *>(&bucket_cors);
    ldout(s->cct, 15) << "Read RGWCORSConfiguration";
    s3cors->to_xml(*_dout);
    *_dout << dendl;
  }
  return 0;
}

/** CORS 6.2.6.
 * If any of the header field-names is not a ASCII case-insensitive match for
 * any of the values in list of headers do not set any additional headers and
 * terminate this set of steps.
 * */
static void get_cors_response_headers(RGWCORSRule *rule, const char *req_hdrs, string& hdrs, string& exp_hdrs, unsigned *max_age) {
  if (req_hdrs) {
    list<string> hl;
    get_str_list(req_hdrs, hl);
    for(list<string>::iterator it = hl.begin(); it != hl.end(); ++it) {
      if (!rule->is_header_allowed((*it).c_str(), (*it).length())) {
        dout(5) << "Header " << (*it) << " is not registered in this rule" << dendl;
      } else {
        if (hdrs.length() > 0) hdrs.append(",");
        hdrs.append((*it));
      }
    }
  }
  rule->format_exp_headers(exp_hdrs);
  *max_age = rule->get_max_age();
}

/**
 * Generate the CORS header response
 *
 * This is described in the CORS standard, section 6.2.
 */
bool RGWOp::generate_cors_headers(string& origin, string& method, string& headers, string& exp_headers, unsigned *max_age)
{
  /* CORS 6.2.1. */
  const char *orig = s->info.env->get("HTTP_ORIGIN");
  if (!orig) {
    return false;
  }

  /* Custom: */
  origin = orig;
  op_ret = read_bucket_cors();
  if (op_ret < 0) {
    return false;
  }

  if (!cors_exist) {
    dout(2) << "No CORS configuration set yet for this bucket" << dendl;
    return false;
  }

  /* CORS 6.2.2. */
  RGWCORSRule *rule = bucket_cors.host_name_rule(orig);
  if (!rule)
    return false;

  /* CORS 6.2.3. */
  const char *req_meth = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_METHOD");
  if (!req_meth) {
    req_meth = s->info.method;
  }

  if (req_meth) {
    method = req_meth;
    /* CORS 6.2.5. */
    if (!validate_cors_rule_method(rule, req_meth)) {
     return false;
    }
  }

  /* CORS 6.2.4. */
  const char *req_hdrs = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_HEADERS");

  /* CORS 6.2.6. */
  get_cors_response_headers(rule, req_hdrs, headers, exp_headers, max_age);

  return true;
}

int RGWGetObj::read_user_manifest_part(rgw_bucket& bucket,
                                       const RGWObjEnt& ent,
                                       RGWAccessControlPolicy * const bucket_policy,
                                       const off_t start_ofs,
                                       const off_t end_ofs)
{
  ldout(s->cct, 20) << "user manifest obj=" << ent.key.name << "[" << ent.key.instance << "]" << dendl;

  int64_t cur_ofs = start_ofs;
  int64_t cur_end = end_ofs;
  utime_t start_time = s->time;

  rgw_obj part(bucket, ent.key);

  map<string, bufferlist> attrs;

  uint64_t obj_size;
  RGWObjectCtx obj_ctx(store);
  RGWAccessControlPolicy obj_policy(s->cct);

  ldout(s->cct, 20) << "reading obj=" << part << " ofs=" << cur_ofs << " end=" << cur_end << dendl;

  obj_ctx.set_atomic(part);
  store->set_prefetch_data(&obj_ctx, part);

  RGWRados::Object op_target(store, s->bucket_info, obj_ctx, part);
  RGWRados::Object::Read read_op(&op_target);

  read_op.conds.if_match = ent.etag.c_str();
  read_op.params.attrs = &attrs;
  read_op.params.obj_size = &obj_size;
  read_op.params.perr = &s->err;

  op_ret = read_op.prepare(&cur_ofs, &cur_end);
  if (op_ret < 0)
    return op_ret;

  if (obj_size != ent.size) {
    // hmm.. something wrong, object not as expected, abort!
    ldout(s->cct, 0) << "ERROR: expected obj_size=" << obj_size << ", actual read size=" << ent.size << dendl;
    return -EIO;
  }

  op_ret = rgw_policy_from_attrset(s->cct, attrs, &obj_policy);
  if (op_ret < 0)
    return op_ret;

  if (!verify_object_permission(s, bucket_policy, &obj_policy, RGW_PERM_READ)) {
    return -EPERM;
  }

  perfcounter->inc(l_rgw_get_b, cur_end - cur_ofs);
  while (cur_ofs <= cur_end) {
    bufferlist bl;
    op_ret = read_op.read(cur_ofs, cur_end, bl);
    if (op_ret < 0)
      return op_ret;

    off_t len = bl.length();
    cur_ofs += len;
    op_ret = 0; /* XXX redundant? */
    perfcounter->tinc(l_rgw_get_lat,
                      (ceph_clock_now(s->cct) - start_time));
    send_response_data(bl, 0, len);

    start_time = ceph_clock_now(s->cct);
  }

  return 0;
}

static int iterate_user_manifest_parts(CephContext * const cct,
                                       RGWRados * const store,
                                       const off_t ofs,
                                       const off_t end,
                                       RGWBucketInfo *pbucket_info,
                                       const string& obj_prefix,
                                       RGWAccessControlPolicy * const bucket_policy,
                                       uint64_t * const ptotal_len,
                                       uint64_t * const pobj_size,
                                       string * const pobj_sum,
                                       int (*cb)(rgw_bucket& bucket,
                                                 const RGWObjEnt& ent,
                                                 RGWAccessControlPolicy * const bucket_policy,
                                                 off_t start_ofs,
                                                 off_t end_ofs,
                                                 void *param),
                                       void * const cb_param)
{
  rgw_bucket& bucket = pbucket_info->bucket;
  uint64_t obj_ofs = 0, len_count = 0;
  bool found_start = false, found_end = false, handled_end = false;
  string delim;
  bool is_truncated;
  vector<RGWObjEnt> objs;

  utime_t start_time = ceph_clock_now(cct);

  RGWRados::Bucket target(store, *pbucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.prefix = obj_prefix;
  list_op.params.delim = delim;

  MD5 etag_sum;
  do {
#define MAX_LIST_OBJS 100
    int r = list_op.list_objects(MAX_LIST_OBJS, &objs, NULL, &is_truncated);
    if (r < 0) {
      return r;
    }

    for (RGWObjEnt& ent : objs) {
      uint64_t cur_total_len = obj_ofs;
      uint64_t start_ofs = 0, end_ofs = ent.size;

      if (!found_start && cur_total_len + ent.size > (uint64_t)ofs) {
	start_ofs = ofs - obj_ofs;
	found_start = true;
      }

      obj_ofs += ent.size;
      if (pobj_sum) {
        etag_sum.Update((const byte *)ent.etag.c_str(),
                        ent.etag.length());
      }

      if (!found_end && obj_ofs > (uint64_t)end) {
	end_ofs = end - cur_total_len + 1;
	found_end = true;
      }

      perfcounter->tinc(l_rgw_get_lat,
                       (ceph_clock_now(cct) - start_time));

      if (found_start && !handled_end) {
        len_count += end_ofs - start_ofs;

        if (cb) {
          r = cb(bucket, ent, bucket_policy, start_ofs, end_ofs, cb_param);
          if (r < 0) {
            return r;
          }
        }
      }

      handled_end = found_end;
      start_time = ceph_clock_now(cct);
    }
  } while (is_truncated);

  if (ptotal_len) {
    *ptotal_len = len_count;
  }
  if (pobj_size) {
    *pobj_size = obj_ofs;
  }
  if (pobj_sum) {
    complete_etag(etag_sum, pobj_sum);
  }

  return 0;
}

struct rgw_slo_part {
  RGWAccessControlPolicy *bucket_policy;
  rgw_bucket bucket;
  string obj_name;
  uint64_t size;
  string etag;

  rgw_slo_part() : bucket_policy(NULL), size(0) {}
};

static int iterate_slo_parts(CephContext *cct,
                             RGWRados *store,
                             off_t ofs,
                             off_t end,
                             map<uint64_t, rgw_slo_part>& slo_parts,
                             int (*cb)(rgw_bucket& bucket,
                                       const RGWObjEnt& ent,
                                       RGWAccessControlPolicy *bucket_policy,
                                       off_t start_ofs,
                                       off_t end_ofs,
                                       void *param),
                             void *cb_param)
{
  bool found_start = false, found_end = false;

  if (slo_parts.empty()) {
    return 0;
  }

  utime_t start_time = ceph_clock_now(cct);

  map<uint64_t, rgw_slo_part>::iterator iter = slo_parts.upper_bound(ofs);
  if (iter != slo_parts.begin()) {
    --iter;
  }

  uint64_t obj_ofs = iter->first;

  for (; iter != slo_parts.end() && !found_end; ++iter) {
    rgw_slo_part& part = iter->second;
    RGWObjEnt ent;

    ent.key.name = part.obj_name;
    ent.size = part.size;
    ent.etag = part.etag;

    uint64_t cur_total_len = obj_ofs;
    uint64_t start_ofs = 0, end_ofs = ent.size;

    if (!found_start && cur_total_len + ent.size > (uint64_t)ofs) {
      start_ofs = ofs - obj_ofs;
      found_start = true;
    }

    obj_ofs += ent.size;

    if (!found_end && obj_ofs > (uint64_t)end) {
      end_ofs = end - cur_total_len + 1;
      found_end = true;
    }

    perfcounter->tinc(l_rgw_get_lat,
                      (ceph_clock_now(cct) - start_time));

    if (found_start) {
      if (cb) {
        int r = cb(part.bucket, ent, part.bucket_policy, start_ofs, end_ofs, cb_param);
        if (r < 0)
          return r;
      }
    }

    start_time = ceph_clock_now(cct);
  }

  return 0;
}

static int get_obj_user_manifest_iterate_cb(rgw_bucket& bucket,
                                            const RGWObjEnt& ent,
                                            RGWAccessControlPolicy * const bucket_policy,
                                            const off_t start_ofs,
                                            const off_t end_ofs,
                                            void * const param)
{
  RGWGetObj *op = static_cast<RGWGetObj *>(param);
  return op->read_user_manifest_part(bucket, ent, bucket_policy, start_ofs, end_ofs);
}

int RGWGetObj::handle_user_manifest(const char *prefix)
{
  ldout(s->cct, 2) << "RGWGetObj::handle_user_manifest() prefix=" << prefix << dendl;

  string prefix_str = prefix;
  int pos = prefix_str.find('/');
  if (pos < 0)
    return -EINVAL;

  string bucket_name_raw, bucket_name;
  bucket_name_raw = prefix_str.substr(0, pos);
  url_decode(bucket_name_raw, bucket_name);

  string obj_prefix_raw, obj_prefix;
  obj_prefix_raw = prefix_str.substr(pos + 1);
  url_decode(obj_prefix_raw, obj_prefix);

  rgw_bucket bucket;

  RGWAccessControlPolicy _bucket_policy(s->cct);
  RGWAccessControlPolicy *bucket_policy;
  RGWBucketInfo bucket_info;
  RGWBucketInfo *pbucket_info;

  if (bucket_name.compare(s->bucket.name) != 0) {
    map<string, bufferlist> bucket_attrs;
    RGWObjectCtx obj_ctx(store);
    int r = store->get_bucket_info(obj_ctx, s->user->user_id.tenant,
				  bucket_name, bucket_info, NULL,
				  &bucket_attrs);
    if (r < 0) {
      ldout(s->cct, 0) << "could not get bucket info for bucket="
		       << bucket_name << dendl;
      return r;
    }
    bucket = bucket_info.bucket;
    pbucket_info = &bucket_info;
    rgw_obj_key no_obj;
    bucket_policy = &_bucket_policy;
    r = read_policy(store, s, bucket_info, bucket_attrs, bucket_policy, bucket, no_obj);
    if (r < 0) {
      ldout(s->cct, 0) << "failed to read bucket policy" << dendl;
      return r;
    }
  } else {
    bucket = s->bucket;
    pbucket_info = &s->bucket_info;
    bucket_policy = s->bucket_acl;
  }

  /* dry run to find out:
   * - total length (of the parts we are going to send to client),
   * - overall DLO's content size,
   * - md5 sum of overall DLO's content (for etag of Swift API). */
  int r = iterate_user_manifest_parts(s->cct, store, ofs, end,
        pbucket_info, obj_prefix, bucket_policy,
        &total_len, &s->obj_size, &lo_etag,
        nullptr /* cb */, nullptr /* cb arg */);
  if (r < 0) {
    return r;
  }

  if (!get_data) {
    bufferlist bl;
    send_response_data(bl, 0, 0);
    return 0;
  }

  r = iterate_user_manifest_parts(s->cct, store, ofs, end,
        pbucket_info, obj_prefix, bucket_policy,
        nullptr, nullptr, nullptr,
        get_obj_user_manifest_iterate_cb, (void *)this);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWGetObj::handle_slo_manifest(bufferlist& bl)
{
  RGWSLOInfo slo_info;
  bufferlist::iterator bliter = bl.begin();
  try {
    ::decode(slo_info, bliter);
  } catch (buffer::error& err) {
    ldout(s->cct, 0) << "ERROR: failed to decode slo manifest" << dendl;
    return -EIO;
  }
  ldout(s->cct, 2) << "RGWGetObj::handle_slo_manifest()" << dendl;

  list<RGWAccessControlPolicy> allocated_policies;
  map<string, RGWAccessControlPolicy *> policies;
  map<string, rgw_bucket> buckets;

  map<uint64_t, rgw_slo_part> slo_parts;

  MD5 etag_sum;
  total_len = 0;

  for (const auto& entry : slo_info.entries) {
    const string& path = entry.path;
    const size_t pos = path.find('/', 1); /* skip first / */
    if (pos == string::npos) {
      return -EINVAL;
    }

    string bucket_name = path.substr(1, pos - 1);
    string obj_name = path.substr(pos + 1);

    rgw_bucket bucket;
    RGWAccessControlPolicy *bucket_policy;

    if (bucket_name.compare(s->bucket.name) != 0) {
      const auto& piter = policies.find(bucket_name);
      if (piter != policies.end()) {
        bucket_policy = piter->second;
        bucket = buckets[bucket_name];
      } else {
        allocated_policies.push_back(RGWAccessControlPolicy(s->cct));
        RGWAccessControlPolicy& _bucket_policy = allocated_policies.back();

        RGWBucketInfo bucket_info;
        map<string, bufferlist> bucket_attrs;
        RGWObjectCtx obj_ctx(store);
        int r = store->get_bucket_info(obj_ctx, s->user->user_id.tenant,
                                       bucket_name, bucket_info, nullptr,
                                       &bucket_attrs);
        if (r < 0) {
          ldout(s->cct, 0) << "could not get bucket info for bucket="
			   << bucket_name << dendl;
          return r;
        }
        bucket = bucket_info.bucket;
        rgw_obj_key no_obj;
        bucket_policy = &_bucket_policy;
        r = read_policy(store, s, bucket_info, bucket_attrs, bucket_policy,
                        bucket, no_obj);
        if (r < 0) {
          ldout(s->cct, 0) << "failed to read bucket policy for bucket "
                           << bucket << dendl;
          return r;
        }
        buckets[bucket_name] = bucket;
        policies[bucket_name] = bucket_policy;
      }
    } else {
      bucket = s->bucket;
      bucket_policy = s->bucket_acl;
    }

    rgw_slo_part part;
    part.bucket_policy = bucket_policy;
    part.bucket = bucket;
    part.obj_name = obj_name;
    part.size = entry.size_bytes;
    part.etag = entry.etag;
    ldout(s->cct, 20) << "slo_part: ofs=" << ofs
                      << " bucket=" << part.bucket
                      << " obj=" << part.obj_name
                      << " size=" << part.size
                      << " etag=" << part.etag
                      << dendl;

    etag_sum.Update((const byte *)entry.etag.c_str(),
                    entry.etag.length());

    slo_parts[total_len] = part;
    total_len += part.size;
  }

  complete_etag(etag_sum, &lo_etag);

  s->obj_size = slo_info.total_size;
  ldout(s->cct, 20) << "s->obj_size=" << s->obj_size << dendl;

  if (ofs < 0) {
    ofs = total_len - std::min(-ofs, static_cast<off_t>(total_len));
  }

  if (end < 0 || end >= static_cast<off_t>(total_len)) {
    end = total_len - 1;
  }

  total_len = end - ofs + 1;

  int r = iterate_slo_parts(s->cct, store, ofs, end, slo_parts,
        get_obj_user_manifest_iterate_cb, (void *)this);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWGetObj::get_data_cb(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  /* garbage collection related handling */
  utime_t start_time = ceph_clock_now(s->cct);
  if (start_time > gc_invalidate_time) {
    int r = store->defer_gc(s->obj_ctx, obj);
    if (r < 0) {
      dout(0) << "WARNING: could not defer gc entry for obj" << dendl;
    }
    gc_invalidate_time = start_time;
    gc_invalidate_time += (s->cct->_conf->rgw_gc_obj_min_wait / 2);
  }
  return send_response_data(bl, bl_ofs, bl_len);
}

bool RGWGetObj::prefetch_data()
{
  /* HEAD request, stop prefetch*/
  if (!get_data) {
    return false;
  }

  bool prefetch_first_chunk = true;
  range_str = s->info.env->get("HTTP_RANGE");

  if(range_str) {
    int r = parse_range(range_str, ofs, end, &partial_content);
    /* error on parsing the range, stop prefetch and will fail in execte() */
    if (r < 0) {
      range_parsed = false;
      return false;
    } else {
      range_parsed = true;
    }
    /* range get goes to shadown objects, stop prefetch */
    if (ofs >= s->cct->_conf->rgw_max_chunk_size) {
      prefetch_first_chunk = false;
    }
  }

  return get_data && prefetch_first_chunk;
}
void RGWGetObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

static bool object_is_expired(map<string, bufferlist>& attrs) {
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_DELETE_AT);
  if (iter != attrs.end()) {
    utime_t delete_at;
    try {
      ::decode(delete_at, iter->second);
    } catch (buffer::error& err) {
      dout(0) << "ERROR: " << __func__ << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
      return false;
    }

    if (delete_at <= ceph_clock_now(g_ceph_context)) {
      return true;
    }
  }

  return false;
}

void RGWGetObj::execute()
{
  utime_t start_time = s->time;
  bufferlist bl;
  gc_invalidate_time = ceph_clock_now(s->cct);
  gc_invalidate_time += (s->cct->_conf->rgw_gc_obj_min_wait / 2);

  RGWGetObj_CB cb(this);

  map<string, bufferlist>::iterator attr_iter;

  perfcounter->inc(l_rgw_get);
  int64_t new_ofs, new_end;

  RGWRados::Object op_target(store, s->bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target);

  op_ret = get_params();
  if (op_ret < 0)
    goto done_err;

  op_ret = init_common();
  if (op_ret < 0)
    goto done_err;

  new_ofs = ofs;
  new_end = end;

  read_op.conds.mod_ptr = mod_ptr;
  read_op.conds.unmod_ptr = unmod_ptr;
  read_op.conds.high_precision_time = s->system_request; /* system request need to use high precision time */
  read_op.conds.mod_zone_id = mod_zone_id;
  read_op.conds.mod_pg_ver = mod_pg_ver;
  read_op.conds.if_match = if_match;
  read_op.conds.if_nomatch = if_nomatch;
  read_op.params.attrs = &attrs;
  read_op.params.lastmod = &lastmod;
  read_op.params.read_size = &total_len;
  read_op.params.obj_size = &s->obj_size;
  read_op.params.perr = &s->err;

  op_ret = read_op.prepare(&new_ofs, &new_end);
  if (op_ret < 0)
    goto done_err;

  attr_iter = attrs.find(RGW_ATTR_USER_MANIFEST);
  if (attr_iter != attrs.end() && !skip_manifest) {
    op_ret = handle_user_manifest(attr_iter->second.c_str());
    if (op_ret < 0) {
      ldout(s->cct, 0) << "ERROR: failed to handle user manifest ret="
		       << op_ret << dendl;
    }
    return;
  }
  attr_iter = attrs.find(RGW_ATTR_SLO_MANIFEST);
  if (attr_iter != attrs.end()) {
    is_slo = true;
    op_ret = handle_slo_manifest(attr_iter->second);
    if (op_ret < 0) {
      ldout(s->cct, 0) << "ERROR: failed to handle slo manifest ret=" << op_ret
		       << dendl;
      goto done_err;
    }
    return;
  }

  /* Check whether the object has expired. Swift API documentation
   * stands that we should return 404 Not Found in such case. */
  if (need_object_expiration() && object_is_expired(attrs)) {
    op_ret = -ENOENT;
    goto done_err;
  }

  ofs = new_ofs;
  end = new_end;

  start = ofs;

  /* STAT ops don't need data, and do no i/o */
  if (get_type() == RGW_OP_STAT_OBJ)
    return;

  if (!get_data || ofs > end)
    goto done_err;

  perfcounter->inc(l_rgw_get_b, end - ofs);

  op_ret = read_op.iterate(ofs, end, &cb);

  perfcounter->tinc(l_rgw_get_lat,
                   (ceph_clock_now(s->cct) - start_time));
  if (op_ret < 0) {
    goto done_err;
  }

  op_ret = send_response_data(bl, 0, 0);
  if (op_ret < 0) {
    goto done_err;
  }
  return;

done_err:
  send_response_data_error();
}

int RGWGetObj::init_common()
{
  if (range_str) {
    /* range parsed error when prefetch*/
    if (!range_parsed) {
      int r = parse_range(range_str, ofs, end, &partial_content);
      if (r < 0)
        return r;
    }
  }
  if (if_mod) {
    if (parse_time(if_mod, &mod_time) < 0)
      return -EINVAL;
    mod_ptr = &mod_time;
  }

  if (if_unmod) {
    if (parse_time(if_unmod, &unmod_time) < 0)
      return -EINVAL;
    unmod_ptr = &unmod_time;
  }

  return 0;
}

int RGWListBuckets::verify_permission()
{
  return 0;
}

int RGWGetUsage::verify_permission()
{
  if (!rgw_user_is_authenticated(*s->user))
    return -EACCES;
  return 0;
}

void RGWListBuckets::execute()
{
  bool done;
  bool started = false;
  uint64_t total_count = 0;

  uint64_t max_buckets = s->cct->_conf->rgw_list_buckets_max_chunk;

  op_ret = get_params();
  if (op_ret < 0) {
    goto send_end;
  }

  if (supports_account_metadata()) {
    op_ret = rgw_get_user_attrs_by_uid(store, s->user->user_id, attrs);
    if (op_ret < 0) {
      goto send_end;
    }
  }

  do {
    RGWUserBuckets buckets;
    uint64_t read_count;
    if (limit >= 0) {
      read_count = min(limit - total_count, (uint64_t)max_buckets);
    } else {
      read_count = max_buckets;
    }

    op_ret = rgw_read_user_buckets(store, s->user->user_id, buckets,
                                   marker, end_marker, read_count,
                                   should_get_stats(), &is_truncated,
                                   get_default_max());
    if (op_ret < 0) {
      /* hmm.. something wrong here.. the user was authenticated, so it
         should exist */
      ldout(s->cct, 10) << "WARNING: failed on rgw_get_user_buckets uid="
			<< s->user->user_id << dendl;
      break;
    }
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    map<string, RGWBucketEnt>::iterator iter;
    for (iter = m.begin(); iter != m.end(); ++iter) {
      RGWBucketEnt& bucket = iter->second;
      buckets_size += bucket.size;
      buckets_size_rounded += bucket.size_rounded;
      buckets_objcount += bucket.count;

      marker = iter->first;
    }
    buckets_count += m.size();
    total_count += m.size();

    done = (m.size() < read_count || (limit >= 0 && total_count >= (uint64_t)limit));

    if (!started) {
      send_response_begin(buckets.count() > 0);
      started = true;
    }

    if (!m.empty()) {
      send_response_data(buckets);

      map<string, RGWBucketEnt>::reverse_iterator riter = m.rbegin();
      marker = riter->first;
    }
  } while (!done);

send_end:
  if (!started) {
    send_response_begin(false);
  }
  send_response_end();
}

void RGWGetUsage::execute()
{
  uint64_t start_epoch = 0;
  uint64_t end_epoch = (uint64_t)-1;
  op_ret = get_params();
  if (op_ret < 0)
    return;
    
  if (!start_date.empty()) {
    op_ret = utime_t::parse_date(start_date, &start_epoch, NULL);
    if (op_ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to parse start date" << dendl;
      return;
    }
  }
    
  if (!end_date.empty()) {
    op_ret = utime_t::parse_date(end_date, &end_epoch, NULL);
    if (op_ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to parse end date" << dendl;
      return;
    }
  }
     
  uint32_t max_entries = 1000;

  bool is_truncated = true;

  RGWUsageIter usage_iter;
  
  while (is_truncated) {
    op_ret = store->read_usage(s->user->user_id, start_epoch, end_epoch, max_entries,
                                &is_truncated, usage_iter, usage);

    if (op_ret == -ENOENT) {
      op_ret = 0;
      is_truncated = false;
    }

    if (op_ret < 0) {
      return;
    }    
  }

  op_ret = rgw_user_sync_all_stats(store, s->user->user_id);
  if (op_ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to sync user stats: " << dendl;
    return ;
  }
  
  string user_str = s->user->user_id.to_str();
  op_ret = store->cls_user_get_header(user_str, &header);
  if (op_ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: can't read user header: "  << dendl;
    return ;
  }
  
  return;
}

int RGWStatAccount::verify_permission()
{
  return 0;
}

void RGWStatAccount::execute()
{
  string marker;
  bool done;
  bool is_truncated;
  uint64_t max_buckets = s->cct->_conf->rgw_list_buckets_max_chunk;

  do {
    RGWUserBuckets buckets;

    op_ret = rgw_read_user_buckets(store, s->user->user_id, buckets, marker,
				   string(), max_buckets, true, &is_truncated);
    if (op_ret < 0) {
      /* hmm.. something wrong here.. the user was authenticated, so it
         should exist */
      ldout(s->cct, 10) << "WARNING: failed on rgw_get_user_buckets uid="
			<< s->user->user_id << dendl;
      break;
    } else {
      map<string, RGWBucketEnt>& m = buckets.get_buckets();
      map<string, RGWBucketEnt>::iterator iter;
      for (iter = m.begin(); iter != m.end(); ++iter) {
        RGWBucketEnt& bucket = iter->second;
        buckets_size += bucket.size;
        buckets_size_rounded += bucket.size_rounded;
        buckets_objcount += bucket.count;

        marker = iter->first;
      }
      buckets_count += m.size();

      done = (m.size() < max_buckets);
    }
  } while (!done);
}

int RGWGetBucketVersioning::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWGetBucketVersioning::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketVersioning::execute()
{
  versioned = s->bucket_info.versioned();
  versioning_enabled = s->bucket_info.versioning_enabled();
}

int RGWSetBucketVersioning::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWSetBucketVersioning::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWSetBucketVersioning::execute()
{
  if (!store->is_meta_master()) {
    bufferlist in_data;
    JSONParser jp;
    op_ret = forward_request_to_master(s, NULL, store, in_data, &jp);
    if (op_ret < 0) {
      ldout(s->cct, 20) << __func__ << "forward_request_to_master returned ret=" << op_ret << dendl;
    }
    return;
  }
  
  op_ret = get_params();

  if (op_ret < 0)
    return;

  if (enable_versioning) {
    s->bucket_info.flags |= BUCKET_VERSIONED;
    s->bucket_info.flags &= ~BUCKET_VERSIONS_SUSPENDED;
  } else {
    s->bucket_info.flags |= (BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED);
  }

  op_ret = store->put_bucket_instance_info(s->bucket_info, false, real_time(),
					  &s->bucket_attrs);
  if (op_ret < 0) {
    ldout(s->cct, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name
		     << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWGetBucketWebsite::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWGetBucketWebsite::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketWebsite::execute()
{
  if (!s->bucket_info.has_website) {
    op_ret = -ENOENT;
  }
}

int RGWSetBucketWebsite::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWSetBucketWebsite::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWSetBucketWebsite::execute()
{
  op_ret = get_params();

  if (op_ret < 0)
    return;

  s->bucket_info.has_website = true;
  s->bucket_info.website_conf = website_conf;

  op_ret = store->put_bucket_instance_info(s->bucket_info, false, real_time(), &s->bucket_attrs);
  if (op_ret < 0) {
    ldout(s->cct, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWDeleteBucketWebsite::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWDeleteBucketWebsite::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteBucketWebsite::execute()
{
  s->bucket_info.has_website = false;
  s->bucket_info.website_conf = RGWBucketWebsiteConf();

  op_ret = store->put_bucket_instance_info(s->bucket_info, false, real_time(), &s->bucket_attrs);
  if (op_ret < 0) {
    ldout(s->cct, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWStatBucket::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

void RGWStatBucket::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWStatBucket::execute()
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  RGWUserBuckets buckets;
  bucket.bucket = s->bucket;
  buckets.add(bucket);
  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  op_ret = store->update_containers_stats(m);
  if (! op_ret)
    op_ret = -EEXIST;
  if (op_ret > 0) {
    op_ret = 0;
    map<string, RGWBucketEnt>::iterator iter = m.find(bucket.bucket.name);
    if (iter != m.end()) {
      bucket = iter->second;
    } else {
      op_ret = -EINVAL;
    }
  }
}

int RGWListBucket::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

int RGWListBucket::parse_max_keys()
{
  if (!max_keys.empty()) {
    char *endptr;
    max = strtol(max_keys.c_str(), &endptr, 10);
    if (endptr) {
      while (*endptr && isspace(*endptr)) // ignore white space
        endptr++;
      if (*endptr) {
        return -EINVAL;
      }
    }
  } else {
    max = default_max;
  }

  return 0;
}

void RGWListBucket::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWListBucket::execute()
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  op_ret = get_params();
  if (op_ret < 0)
    return;

  if (need_container_stats()) {
    map<string, RGWBucketEnt> m;
    m[s->bucket.name] = RGWBucketEnt();
    m.begin()->second.bucket = s->bucket;
    op_ret = store->update_containers_stats(m);
    if (op_ret > 0) {
      bucket = m.begin()->second;
    }
  }

  RGWRados::Bucket target(store, s->bucket_info);
  if (shard_id >= 0) {
    target.set_shard_id(shard_id);
  }
  RGWRados::Bucket::List list_op(&target);

  list_op.params.prefix = prefix;
  list_op.params.delim = delimiter;
  list_op.params.marker = marker;
  list_op.params.end_marker = end_marker;
  list_op.params.list_versions = list_versions;

  op_ret = list_op.list_objects(max, &objs, &common_prefixes, &is_truncated);
  if (op_ret >= 0 && !delimiter.empty()) {
    next_marker = list_op.get_next_marker();
  }
}

int RGWGetBucketLogging::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

int RGWGetBucketLocation::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

int RGWCreateBucket::verify_permission()
{
  if (!rgw_user_is_authenticated(*(s->user)))
    return -EACCES;

  if (s->user->user_id.tenant != s->bucket_tenant) {
    ldout(s->cct, 10)
      << "user cannot create a bucket in a different tenant (user_id.tenant="
      << s->user->user_id.tenant << " requested=" << s->bucket_tenant << ")"
      << dendl;
    return -EACCES;
  }

  if (s->user->max_buckets) {
    RGWUserBuckets buckets;
    string marker;
    bool is_truncated;
    op_ret = rgw_read_user_buckets(store, s->user->user_id, buckets,
				   marker, string(), s->user->max_buckets,
				   false, &is_truncated);
    if (op_ret < 0)
      return op_ret;

    if (buckets.count() >= s->user->max_buckets) {
      return -ERR_TOO_MANY_BUCKETS;
    }
  }

  return 0;
}

static int forward_request_to_master(struct req_state *s, obj_version *objv,
				    RGWRados *store, bufferlist& in_data,
				    JSONParser *jp)
{
  if (!store->rest_master_conn) {
    ldout(s->cct, 0) << "rest connection is invalid" << dendl;
    return -EINVAL;
  }
  ldout(s->cct, 0) << "sending create_bucket request to master zonegroup" << dendl;
  bufferlist response;
  string uid_str = s->user->user_id.to_str();
#define MAX_REST_RESPONSE (128 * 1024) // we expect a very small response
  int ret = store->rest_master_conn->forward(uid_str, s->info, objv,
					    MAX_REST_RESPONSE, &in_data,
					    &response);
  if (ret < 0)
    return ret;

  ldout(s->cct, 20) << "response: " << response.c_str() << dendl;
  if (jp && !jp->parse(response.c_str(), response.length())) {
    ldout(s->cct, 0) << "failed parsing response from master zonegroup" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWCreateBucket::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

static void prepare_add_del_attrs(const map<string, bufferlist>& orig_attrs,
                                  map<string, bufferlist>& out_attrs,
                                  map<string, bufferlist>& out_rmattrs)
{
  for (const auto kv : orig_attrs) {
    const string& name = kv.first;

    /* Check if the attr is user-defined metadata item. */
    if (name.compare(0, sizeof(RGW_ATTR_META_PREFIX) - 1,
                     RGW_ATTR_META_PREFIX) == 0) {
      /* For the objects all existing meta attrs have to be removed. */
      out_rmattrs[name] = kv.second;
    } else if (out_attrs.find(name) == std::end(out_attrs)) {
      out_attrs[name] = kv.second;
    }
  }
}

static void prepare_add_del_attrs(const map<string, bufferlist>& orig_attrs,
                                  const set<string>& rmattr_names,
                                  map<string, bufferlist>& out_attrs)
{
  for (const auto kv : orig_attrs) {
    const string& name = kv.first;

    /* Check if the attr is user-defined metadata item. */
    if (name.compare(0, strlen(RGW_ATTR_META_PREFIX),
                     RGW_ATTR_META_PREFIX) == 0) {
      /* For the buckets all existing meta attrs are preserved,
         except those that are listed in rmattr_names. */
      if (rmattr_names.find(name) != std::end(rmattr_names)) {
        const auto aiter = out_attrs.find(name);

        if (aiter != std::end(out_attrs)) {
          out_attrs.erase(aiter);
        }
      }
    } else if (out_attrs.find(name) == std::end(out_attrs)) {
      out_attrs[name] = kv.second;
    }
  }
}


static void populate_with_generic_attrs(const req_state * const s,
                                        map<string, bufferlist>& out_attrs)
{
  for (auto kv : s->generic_attrs) {
    bufferlist& attrbl = out_attrs[kv.first];
    const string& val = kv.second;
    attrbl.clear();
    attrbl.append(val.c_str(), val.size() + 1);
  }
}


void RGWCreateBucket::execute()
{
  RGWAccessControlPolicy old_policy(s->cct);
  map<string, bufferlist> attrs;
  bufferlist aclbl;
  bufferlist corsbl;
  bool existed;
  string bucket_name;
  rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name, bucket_name);
  rgw_obj obj(store->get_zone_params().domain_root, bucket_name);
  obj_version objv, *pobjv = NULL;

  op_ret = get_params();
  if (op_ret < 0)
    return;

  if (!store->get_zonegroup().is_master &&
      store->get_zonegroup().api_name != location_constraint) {
    ldout(s->cct, 0) << "location constraint (" << location_constraint << ") doesn't match zonegroup" << " (" << store->get_zonegroup().api_name << ")" << dendl;
    op_ret = -EINVAL;
    return;
  }

  /* we need to make sure we read bucket info, it's not read before for this
   * specific request */
  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  op_ret = store->get_bucket_info(obj_ctx, s->bucket_tenant, s->bucket_name,
				  s->bucket_info, NULL, &s->bucket_attrs);
  if (op_ret < 0 && op_ret != -ENOENT)
    return;
  s->bucket_exists = (op_ret != -ENOENT);

  s->bucket_owner.set_id(s->user->user_id);
  s->bucket_owner.set_name(s->user->display_name);
  if (s->bucket_exists) {
    int r = get_policy_from_attr(s->cct, store, s->obj_ctx, s->bucket_info,
				s->bucket_attrs, &old_policy, obj);
    if (r >= 0)  {
      if (old_policy.get_owner().get_id().compare(s->user->user_id) != 0) {
        op_ret = -EEXIST;
        return;
      }
    }
  }

  RGWBucketInfo master_info;
  rgw_bucket *pmaster_bucket;
  real_time creation_time;

  if (!store->is_meta_master()) {
    JSONParser jp;
    op_ret = forward_request_to_master(s, NULL, store, in_data, &jp);
    if (op_ret < 0)
      return;

    JSONDecoder::decode_json("entry_point_object_ver", ep_objv, &jp);
    JSONDecoder::decode_json("object_ver", objv, &jp);
    JSONDecoder::decode_json("bucket_info", master_info, &jp);
    ldout(s->cct, 20) << "parsed: objv.tag=" << objv.tag << " objv.ver=" << objv.ver << dendl;
    ldout(s->cct, 20) << "got creation time: << " << master_info.creation_time << dendl;
    pmaster_bucket= &master_info.bucket;
    creation_time = master_info.creation_time;
    pobjv = &objv;
  } else {
    pmaster_bucket = NULL;
  }

  string zonegroup_id;

  if (s->system_request) {
    zonegroup_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "zonegroup");
    if (zonegroup_id.empty()) {
      zonegroup_id = store->get_zonegroup().get_id();
    }
  } else {
    zonegroup_id = store->get_zonegroup().get_id();
  }

  if (s->bucket_exists) {
    string selected_placement_rule;
    rgw_bucket bucket;
    op_ret = store->select_bucket_placement(*(s->user), zonegroup_id,
					    placement_rule,
					    s->bucket_tenant, s->bucket_name,
					    bucket, &selected_placement_rule, nullptr);
    if (selected_placement_rule != s->bucket_info.placement_rule) {
      op_ret = -EEXIST;
      return;
    }
  }

  if (need_metadata_upload()) {
    rgw_get_request_metadata(s->cct, s->info, attrs, false);
    prepare_add_del_attrs(s->bucket_attrs, rmattr_names, attrs);
    populate_with_generic_attrs(s, attrs);
  }

  policy.encode(aclbl);
  attrs[RGW_ATTR_ACL] = aclbl;

  if (has_cors) {
    cors_config.encode(corsbl);
    attrs[RGW_ATTR_CORS] = corsbl;
  }
  s->bucket.tenant = s->bucket_tenant; /* ignored if bucket exists */
  s->bucket.name = s->bucket_name;
  op_ret = store->create_bucket(*(s->user), s->bucket, zonegroup_id, placement_rule,
                                swift_ver_location,
				attrs, info, pobjv, &ep_objv, creation_time,
				pmaster_bucket, true);
  /* continue if EEXIST and create_bucket will fail below.  this way we can
   * recover from a partial create by retrying it. */
  ldout(s->cct, 20) << "rgw_create_bucket returned ret=" << op_ret << " bucket=" << s->bucket << dendl;

  if (op_ret && op_ret != -EEXIST)
    return;

  existed = (op_ret == -EEXIST);

  if (existed) {
    /* bucket already existed, might have raced with another bucket creation, or
     * might be partial bucket creation that never completed. Read existing bucket
     * info, verify that the reported bucket owner is the current user.
     * If all is ok then update the user's list of buckets.
     * Otherwise inform client about a name conflict.
     */
    if (info.owner.compare(s->user->user_id) != 0) {
      op_ret = -EEXIST;
      return;
    }
    s->bucket = info.bucket;
  }

  op_ret = rgw_link_bucket(store, s->user->user_id, s->bucket,
			   info.creation_time, false);
  if (op_ret && !existed && op_ret != -EEXIST) {
    /* if it exists (or previously existed), don't remove it! */
    op_ret = rgw_unlink_bucket(store, s->user->user_id, s->bucket.tenant,
			       s->bucket.name);
    if (op_ret < 0) {
      ldout(s->cct, 0) << "WARNING: failed to unlink bucket: ret=" << op_ret
		       << dendl;
    }
  } else if (op_ret == -EEXIST || (op_ret == 0 && existed)) {
    op_ret = -ERR_BUCKET_EXISTS;
  }

  if (need_metadata_upload() && existed) {
    /* OK, it looks we lost race with another request. As it's required to
     * handle metadata fusion and upload, the whole operation becomes very
     * similar in nature to PutMetadataBucket. However, as the attrs may
     * changed in the meantime, we have to refresh. */
    short tries = 0;
    do {
      RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
      RGWBucketInfo binfo;
      map<string, bufferlist> battrs;

      op_ret = store->get_bucket_info(obj_ctx, s->bucket_tenant, s->bucket_name,
                                      binfo, nullptr, &battrs);
      if (op_ret < 0) {
        return;
      } else if (binfo.owner.compare(s->user->user_id) != 0) {
        /* New bucket doesn't belong to the account we're operating on. */
        op_ret = -EEXIST;
        return;
      } else {
        s->bucket_info = binfo;
        s->bucket_attrs = battrs;
      }

      attrs.clear();

      rgw_get_request_metadata(s->cct, s->info, attrs, false);
      prepare_add_del_attrs(s->bucket_attrs, rmattr_names, attrs);
      populate_with_generic_attrs(s, attrs);

      op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs,
                                    &s->bucket_info.objv_tracker);
    } while (op_ret == -ECANCELED && tries++ < 20);

    /* Restore the proper return code. */
    if (op_ret >= 0) {
      op_ret = -ERR_BUCKET_EXISTS;
    }
  }
}

int RGWDeleteBucket::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWDeleteBucket::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteBucket::execute()
{
  op_ret = -EINVAL;

  if (s->bucket_name.empty())
    return;

  if (!s->bucket_exists) {
    ldout(s->cct, 0) << "ERROR: bucket " << s->bucket_name << " not found" << dendl;
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }
  RGWObjVersionTracker ot;
  ot.read_version = s->bucket_info.ep_objv;

  if (s->system_request) {
    string tag = s->info.args.get(RGW_SYS_PARAM_PREFIX "tag");
    string ver_str = s->info.args.get(RGW_SYS_PARAM_PREFIX "ver");
    if (!tag.empty()) {
      ot.read_version.tag = tag;
      uint64_t ver;
      string err;
      ver = strict_strtol(ver_str.c_str(), 10, &err);
      if (!err.empty()) {
        ldout(s->cct, 0) << "failed to parse ver param" << dendl;
        op_ret = -EINVAL;
        return;
      }
      ot.read_version.ver = ver;
    }
  }

  op_ret = rgw_bucket_sync_user_stats(store, s->user->user_id, s->bucket);
  if ( op_ret < 0) {
     ldout(s->cct, 1) << "WARNING: failed to sync user stats before bucket delete: op_ret= " << op_ret << dendl;
  }

  op_ret = store->delete_bucket(s->bucket, ot);
  if (op_ret == 0) {
    op_ret = rgw_unlink_bucket(store, s->user->user_id, s->bucket.tenant,
			       s->bucket.name, false);
    if (op_ret < 0) {
      ldout(s->cct, 0) << "WARNING: failed to unlink bucket: ret=" << op_ret
		       << dendl;
    }
  }

  if (op_ret < 0) {
    return;
  }

  if (!store->is_meta_master()) {
    bufferlist in_data;
    op_ret = forward_request_to_master(s, &ot.read_version, store, in_data,
				       NULL);
    if (op_ret < 0) {
      if (op_ret == -ENOENT) {
        /* adjust error, we want to return with NoSuchBucket and not
	 * NoSuchKey */
        op_ret = -ERR_NO_SUCH_BUCKET;
      }
      return;
    }
  }

}

int RGWPutObj::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

class RGWPutObjProcessor_Multipart : public RGWPutObjProcessor_Atomic
{
  string part_num;
  RGWMPObj mp;
  req_state *s;
  string upload_id;

protected:
  int prepare(RGWRados *store, string *oid_rand);
  int do_complete(string& etag, real_time *mtime, real_time set_mtime,
                  map<string, bufferlist>& attrs, real_time delete_at,
                  const char *if_match = NULL, const char *if_nomatch = NULL);

public:
  bool immutable_head() { return true; }
  RGWPutObjProcessor_Multipart(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, uint64_t _p, req_state *_s) :
                   RGWPutObjProcessor_Atomic(obj_ctx, bucket_info, _s->bucket, _s->object.name, _p, _s->req_id, false), s(_s) {}
};

int RGWPutObjProcessor_Multipart::prepare(RGWRados *store, string *oid_rand)
{
  int r = prepare_init(store, NULL);
  if (r < 0) {
    return r;
  }

  string oid = obj_str;
  upload_id = s->info.args.get("uploadId");
  if (!oid_rand) {
    mp.init(oid, upload_id);
  } else {
    mp.init(oid, upload_id, *oid_rand);
  }

  part_num = s->info.args.get("partNumber");
  if (part_num.empty()) {
    ldout(s->cct, 10) << "part number is empty" << dendl;
    return -EINVAL;
  }

  string err;
  uint64_t num = (uint64_t)strict_strtol(part_num.c_str(), 10, &err);

  if (!err.empty()) {
    ldout(s->cct, 10) << "bad part number: " << part_num << ": " << err << dendl;
    return -EINVAL;
  }

  string upload_prefix = oid + ".";

  if (!oid_rand) {
    upload_prefix.append(upload_id);
  } else {
    upload_prefix.append(*oid_rand);
  }

  rgw_obj target_obj;
  target_obj.init(bucket, oid);

  manifest.set_prefix(upload_prefix);

  manifest.set_multipart_part_rule(store->ctx()->_conf->rgw_obj_stripe_size, num);

  r = manifest_gen.create_begin(store->ctx(), &manifest, bucket, target_obj);
  if (r < 0) {
    return r;
  }

  head_obj = manifest_gen.get_cur_obj();
  head_obj.index_hash_source = obj_str;
  cur_obj = head_obj;

  return 0;
}

static bool is_v2_upload_id(const string& upload_id)
{
  const char *uid = upload_id.c_str();

  return (strncmp(uid, MULTIPART_UPLOAD_ID_PREFIX, sizeof(MULTIPART_UPLOAD_ID_PREFIX) - 1) == 0) ||
         (strncmp(uid, MULTIPART_UPLOAD_ID_PREFIX_LEGACY, sizeof(MULTIPART_UPLOAD_ID_PREFIX_LEGACY) - 1) == 0);
}

int RGWPutObjProcessor_Multipart::do_complete(string& etag, real_time *mtime, real_time set_mtime,
                                              map<string, bufferlist>& attrs, real_time delete_at,
                                              const char *if_match, const char *if_nomatch)
{
  complete_writing_data();

  RGWRados::Object op_target(store, s->bucket_info, obj_ctx, head_obj);
  RGWRados::Object::Write head_obj_op(&op_target);

  head_obj_op.meta.set_mtime = set_mtime;
  head_obj_op.meta.mtime = mtime;
  head_obj_op.meta.owner = s->owner.get_id();
  head_obj_op.meta.delete_at = delete_at;

  int r = head_obj_op.write_meta(s->obj_size, attrs);
  if (r < 0)
    return r;

  bufferlist bl;
  RGWUploadPartInfo info;
  string p = "part.";
  bool sorted_omap = is_v2_upload_id(upload_id);

  if (sorted_omap) {
    string err;
    int part_num_int = strict_strtol(part_num.c_str(), 10, &err);
    if (!err.empty()) {
      dout(10) << "bad part number specified: " << part_num << dendl;
      return -EINVAL;
    }
    char buf[32];
    snprintf(buf, sizeof(buf), "%08d", part_num_int);
    p.append(buf);
  } else {
    p.append(part_num);
  }
  info.num = atoi(part_num.c_str());
  info.etag = etag;
  info.size = s->obj_size;
  info.modified = real_clock::now();
  info.manifest = manifest;
  ::encode(info, bl);

  string multipart_meta_obj = mp.get_meta();

  rgw_obj meta_obj;
  meta_obj.init_ns(bucket, multipart_meta_obj, mp_ns);
  meta_obj.set_in_extra_data(true);

  r = store->omap_set(meta_obj, p, bl);

  return r;
}


RGWPutObjProcessor *RGWPutObj::select_processor(RGWObjectCtx& obj_ctx, bool *is_multipart)
{
  RGWPutObjProcessor *processor;

  bool multipart = s->info.args.exists("uploadId");

  uint64_t part_size = s->cct->_conf->rgw_obj_stripe_size;

  if (!multipart) {
    processor = new RGWPutObjProcessor_Atomic(obj_ctx, s->bucket_info, s->bucket, s->object.name, part_size, s->req_id, s->bucket_info.versioning_enabled());
    (static_cast<RGWPutObjProcessor_Atomic *>(processor))->set_olh_epoch(olh_epoch);
    (static_cast<RGWPutObjProcessor_Atomic *>(processor))->set_version_id(version_id);
  } else {
    processor = new RGWPutObjProcessor_Multipart(obj_ctx, s->bucket_info, part_size, s);
  }

  if (is_multipart) {
    *is_multipart = multipart;
  }

  return processor;
}

void RGWPutObj::dispose_processor(RGWPutObjProcessor *processor)
{
  delete processor;
}

void RGWPutObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPutObj::execute()
{
  RGWPutObjProcessor *processor = NULL;
  char supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1];
  char supplied_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  bufferlist bl, aclbl;
  map<string, bufferlist> attrs;
  int len;
  map<string, string>::iterator iter;
  bool multipart;

  bool need_calc_md5 = (dlo_manifest == NULL) && (slo_info == NULL);


  perfcounter->inc(l_rgw_put);
  op_ret = -EINVAL;
  if (s->object.empty()) {
    goto done;
  }

  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  op_ret = get_params();
  if (op_ret < 0) {
    ldout(s->cct, 20) << "get_params() returned ret=" << op_ret << dendl;
    goto done;
  }

  op_ret = get_system_versioning_params(s, &olh_epoch, &version_id);
  if (op_ret < 0) {
    ldout(s->cct, 20) << "get_system_versioning_params() returned ret="
		      << op_ret << dendl;
    goto done;
  }

  if (supplied_md5_b64) {
    need_calc_md5 = true;

    ldout(s->cct, 15) << "supplied_md5_b64=" << supplied_md5_b64 << dendl;
    op_ret = ceph_unarmor(supplied_md5_bin, &supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1],
                       supplied_md5_b64, supplied_md5_b64 + strlen(supplied_md5_b64));
    ldout(s->cct, 15) << "ceph_armor ret=" << op_ret << dendl;
    if (op_ret != CEPH_CRYPTO_MD5_DIGESTSIZE) {
      op_ret = -ERR_INVALID_DIGEST;
      goto done;
    }

    buf_to_hex((const unsigned char *)supplied_md5_bin, CEPH_CRYPTO_MD5_DIGESTSIZE, supplied_md5);
    ldout(s->cct, 15) << "supplied_md5=" << supplied_md5 << dendl;
  }

  if (!chunked_upload) { /* with chunked upload we don't know how big is the upload.
                            we also check sizes at the end anyway */
    op_ret = store->check_quota(s->bucket_owner.get_id(), s->bucket,
				user_quota, bucket_quota, s->content_length);
    if (op_ret < 0) {
      ldout(s->cct, 20) << "check_quota() returned ret=" << op_ret << dendl;
      goto done;
    }
  }

  if (supplied_etag) {
    strncpy(supplied_md5, supplied_etag, sizeof(supplied_md5) - 1);
    supplied_md5[sizeof(supplied_md5) - 1] = '\0';
  }

  processor = select_processor(*static_cast<RGWObjectCtx *>(s->obj_ctx), &multipart);

  op_ret = processor->prepare(store, NULL);
  if (op_ret < 0) {
    ldout(s->cct, 20) << "processor->prepare() returned ret=" << op_ret
		      << dendl;
    goto done;
  }

  do {
    bufferlist data;
    len = get_data(data);
    if (len < 0) {
      op_ret = len;
      goto done;
    }
    if (!len)
      break;

    /* do we need this operation to be synchronous? if we're dealing with an object with immutable
     * head, e.g., multipart object we need to make sure we're the first one writing to this object
     */
    bool need_to_wait = (ofs == 0) && multipart;

    bufferlist orig_data;

    if (need_to_wait) {
      orig_data = data;
    }

    op_ret = put_data_and_throttle(processor, data, ofs,
				  (need_calc_md5 ? &hash : NULL), need_to_wait);
    if (op_ret < 0) {
      if (!need_to_wait || op_ret != -EEXIST) {
        ldout(s->cct, 20) << "processor->thottle_data() returned ret="
			  << op_ret << dendl;
        goto done;
      }

      ldout(s->cct, 5) << "NOTICE: processor->throttle_data() returned -EEXIST, need to restart write" << dendl;

      /* restore original data */
      data.swap(orig_data);

      /* restart processing with different oid suffix */

      dispose_processor(processor);
      processor = select_processor(*static_cast<RGWObjectCtx *>(s->obj_ctx), &multipart);

      string oid_rand;
      char buf[33];
      gen_rand_alphanumeric(store->ctx(), buf, sizeof(buf) - 1);
      oid_rand.append(buf);

      op_ret = processor->prepare(store, &oid_rand);
      if (op_ret < 0) {
        ldout(s->cct, 0) << "ERROR: processor->prepare() returned "
			 << op_ret << dendl;
        goto done;
      }

      op_ret = put_data_and_throttle(processor, data, ofs, NULL, false);
      if (op_ret < 0) {
        goto done;
      }
    }

    ofs += len;
  } while (len > 0);

  if (!chunked_upload && ofs != s->content_length) {
    op_ret = -ERR_REQUEST_TIMEOUT;
    goto done;
  }
  s->obj_size = ofs;

  perfcounter->inc(l_rgw_put_b, s->obj_size);

  if (s->aws4_auth_needs_complete) {

    /* complete aws4 auth */

    op_ret = RGW_Auth_S3::authorize_aws4_auth_complete(store, s);
    if (op_ret) {
      goto done;
    }

    s->aws4_auth_needs_complete = false;

    /* verify signature */

    if (s->aws4_auth->signature != s->aws4_auth->new_signature) {
      op_ret = -ERR_SIGNATURE_NO_MATCH;
      ldout(s->cct, 20) << "delayed aws4 auth failed" << dendl;
      goto done;
    }

    /* authorization ok */

    dout(10) << "v4 auth ok" << dendl;

  }

  op_ret = store->check_quota(s->bucket_owner.get_id(), s->bucket,
                              user_quota, bucket_quota, s->obj_size);
  if (op_ret < 0) {
    ldout(s->cct, 20) << "second check_quota() returned op_ret=" << op_ret << dendl;
    goto done;
  }

  if (need_calc_md5) {
    processor->complete_hash(&hash);
  }
  hash.Final(m);

  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);
  etag = calc_md5;

  if (supplied_md5_b64 && strcmp(calc_md5, supplied_md5)) {
    op_ret = -ERR_BAD_DIGEST;
    goto done;
  }

  policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;

  if (dlo_manifest) {
    op_ret = encode_dlo_manifest_attr(dlo_manifest, attrs);
    if (op_ret < 0) {
      ldout(s->cct, 0) << "bad user manifest: " << dlo_manifest << dendl;
      goto done;
    }
    complete_etag(hash, &etag);
    ldout(s->cct, 10) << __func__ << ": calculated md5 for user manifest: " << etag << dendl;
  }

  if (slo_info) {
    bufferlist manifest_bl;
    ::encode(*slo_info, manifest_bl);
    attrs[RGW_ATTR_SLO_MANIFEST] = manifest_bl;

    hash.Update((byte *)slo_info->raw_data, slo_info->raw_data_len);
    complete_etag(hash, &etag);
    ldout(s->cct, 10) << __func__ << ": calculated md5 for user manifest: " << etag << dendl;
  }

  if (supplied_etag && etag.compare(supplied_etag) != 0) {
    op_ret = -ERR_UNPROCESSABLE_ENTITY;
    goto done;
  }
  bl.append(etag.c_str(), etag.size() + 1);
  attrs[RGW_ATTR_ETAG] = bl;

  for (iter = s->generic_attrs.begin(); iter != s->generic_attrs.end();
       ++iter) {
    bufferlist& attrbl = attrs[iter->first];
    const string& val = iter->second;
    attrbl.append(val.c_str(), val.size() + 1);
  }

  rgw_get_request_metadata(s->cct, s->info, attrs);
  encode_delete_at_attr(delete_at, attrs);

  /* Add a custom metadata to expose the information whether an object
   * is an SLO or not. Appending the attribute must be performed AFTER
   * processing any input from user in order to prohibit overwriting. */
  if (slo_info) {
    bufferlist slo_userindicator_bl;
    ::encode("True", slo_userindicator_bl);
    attrs[RGW_ATTR_SLO_UINDICATOR] = slo_userindicator_bl;
  }

  op_ret = processor->complete(etag, &mtime, real_time(), attrs, delete_at, if_match,
			       if_nomatch);

done:
  dispose_processor(processor);
  perfcounter->tinc(l_rgw_put_lat,
                   (ceph_clock_now(s->cct) - s->time));
}

int RGWPostObj::verify_permission()
{
  return 0;
}

RGWPutObjProcessor *RGWPostObj::select_processor(RGWObjectCtx& obj_ctx)
{
  RGWPutObjProcessor *processor;

  uint64_t part_size = s->cct->_conf->rgw_obj_stripe_size;

  processor = new RGWPutObjProcessor_Atomic(obj_ctx, s->bucket_info, s->bucket, s->object.name, part_size, s->req_id, s->bucket_info.versioning_enabled());

  return processor;
}

void RGWPostObj::dispose_processor(RGWPutObjProcessor *processor)
{
  delete processor;
}

void RGWPostObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPostObj::execute()
{
  RGWPutObjProcessor *processor = NULL;
  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  bufferlist bl, aclbl;
  int len = 0;

  // read in the data from the POST form
  op_ret = get_params();
  if (op_ret < 0)
    goto done;

  op_ret = verify_params();
  if (op_ret < 0)
    goto done;

  if (!verify_bucket_permission(s, RGW_PERM_WRITE)) {
    op_ret = -EACCES;
    goto done;
  }

  op_ret = store->check_quota(s->bucket_owner.get_id(), s->bucket,
			      user_quota, bucket_quota, s->content_length);
  if (op_ret < 0) {
    goto done;
  }

  processor = select_processor(*static_cast<RGWObjectCtx *>(s->obj_ctx));

  op_ret = processor->prepare(store, NULL);
  if (op_ret < 0)
    goto done;

  while (data_pending) {
     bufferlist data;
     len = get_data(data);

     if (len < 0) {
       op_ret = len;
       goto done;
     }

     if (!len)
       break;

     op_ret = put_data_and_throttle(processor, data, ofs, &hash, false);

     ofs += len;

     if (ofs > max_len) {
       op_ret = -ERR_TOO_LARGE;
       goto done;
     }
   }

  if (len < min_len) {
    op_ret = -ERR_TOO_SMALL;
    goto done;
  }

  s->obj_size = ofs;

  op_ret = store->check_quota(s->bucket_owner.get_id(), s->bucket,
			      user_quota, bucket_quota, s->obj_size);
  if (op_ret < 0) {
    goto done;
  }

  processor->complete_hash(&hash);
  hash.Final(m);
  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);

  policy.encode(aclbl);
  etag = calc_md5;

  bl.append(etag.c_str(), etag.size() + 1);
  attrs[RGW_ATTR_ETAG] = bl;
  attrs[RGW_ATTR_ACL] = aclbl;

  if (content_type.size()) {
    bufferlist ct_bl;
    ct_bl.append(content_type.c_str(), content_type.size() + 1);
    attrs[RGW_ATTR_CONTENT_TYPE] = ct_bl;
  }

  op_ret = processor->complete(etag, NULL, real_time(), attrs, delete_at);

done:
  dispose_processor(processor);
}

int RGWPutMetadataAccount::handle_temp_url_update(
  const map<int, string>& temp_url_keys) {
  RGWUserAdminOpState user_op;
  user_op.set_user_id(s->user->user_id);

  map<int, string>::const_iterator iter;
  for (iter = temp_url_keys.begin(); iter != temp_url_keys.end(); ++iter) {
    user_op.set_temp_url_key(iter->second, iter->first);
  }

  RGWUser user;
  op_ret = user.init(store, user_op);
  if (op_ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: could not init user ret=" << op_ret
			   << dendl;
    return op_ret;
  }

  string err_msg;
  op_ret = user.modify(user_op, &err_msg);
  if (op_ret < 0) {
    ldout(store->ctx(), 10) << "user.modify() returned " << op_ret << ": "
			    << err_msg << dendl;
    return op_ret;
  }
  return 0;
}

int RGWPutMetadataAccount::verify_permission()
{
  if (!rgw_user_is_authenticated(*(s->user))) {
    return -EACCES;
  }
  // if ((s->perm_mask & RGW_PERM_WRITE) == 0) {
  //   return -EACCES;
  // }
  return 0;
}

void RGWPutMetadataAccount::filter_out_temp_url(map<string, bufferlist>& add_attrs,
                                                const set<string>& rmattr_names,
                                                map<int, string>& temp_url_keys)
{
  map<string, bufferlist>::iterator iter;

  iter = add_attrs.find(RGW_ATTR_TEMPURL_KEY1);
  if (iter != add_attrs.end()) {
    temp_url_keys[0] = iter->second.c_str();
    add_attrs.erase(iter);
  }

  iter = add_attrs.find(RGW_ATTR_TEMPURL_KEY2);
  if (iter != add_attrs.end()) {
    temp_url_keys[1] = iter->second.c_str();
    add_attrs.erase(iter);
  }

  set<string>::const_iterator riter;
  for(riter = rmattr_names.begin(); riter != rmattr_names.end(); ++riter) {
    const string& name = *riter;

    if (name.compare(RGW_ATTR_TEMPURL_KEY1) == 0) {
      temp_url_keys[0] = string();
    }
    if (name.compare(RGW_ATTR_TEMPURL_KEY2) == 0) {
      temp_url_keys[1] = string();
    }
  }
}

void RGWPutMetadataAccount::execute()
{
  map<string, bufferlist> attrs, orig_attrs, rmattrs;
  RGWObjVersionTracker acct_op_tracker;

  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  rgw_get_request_metadata(s->cct, s->info, attrs, false);
  RGWUserInfo orig_uinfo;
  rgw_get_user_info_by_uid(store, s->user->user_id, orig_uinfo, &acct_op_tracker);
  populate_with_generic_attrs(s, attrs);

  /* Handle the TempURL-related stuff. */
  map<int, string> temp_url_keys;
  filter_out_temp_url(attrs, rmattr_names, temp_url_keys);
  if (!temp_url_keys.empty()) {
    if (s->perm_mask != RGW_PERM_FULL_CONTROL) {
      op_ret = -EPERM;
      return;
    }
  }

  /* XXX tenant needed? */
  op_ret = rgw_store_user_info(store, *(s->user), &orig_uinfo,
                               &acct_op_tracker, real_time(), false, &attrs);
  if (op_ret < 0) {
    return;
  }

  if (!temp_url_keys.empty()) {
    op_ret = handle_temp_url_update(temp_url_keys);
    if (op_ret < 0) {
      return;
    }
  }
}

int RGWPutMetadataBucket::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

  return 0;
}

void RGWPutMetadataBucket::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPutMetadataBucket::execute()
{
  map<string, bufferlist> attrs, orig_attrs;

  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  rgw_get_request_metadata(s->cct, s->info, attrs, false);

  if (!placement_rule.empty() &&
      placement_rule != s->bucket_info.placement_rule) {
    op_ret = -EEXIST;
    return;
  }

  orig_attrs = s->bucket_attrs;
  prepare_add_del_attrs(orig_attrs, rmattr_names, attrs);
  populate_with_generic_attrs(s, attrs);

  if (has_policy) {
    bufferlist bl;
    policy.encode(bl);
    attrs[RGW_ATTR_ACL] = bl;
  }

  if (has_cors) {
    bufferlist bl;
    cors_config.encode(bl);
    attrs[RGW_ATTR_CORS] = bl;
  }

  s->bucket_info.swift_ver_location = swift_ver_location;
  s->bucket_info.swift_versioning = (!swift_ver_location.empty());

  op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs, &s->bucket_info.objv_tracker);
}

int RGWPutMetadataObject::verify_permission()
{
  if (!verify_object_permission(s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

  return 0;
}

void RGWPutMetadataObject::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPutMetadataObject::execute()
{
  rgw_obj obj(s->bucket, s->object);
  map<string, bufferlist> attrs, orig_attrs, rmattrs;

  store->set_atomic(s->obj_ctx, obj);

  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  rgw_get_request_metadata(s->cct, s->info, attrs);
  /* check if obj exists, read orig attrs */
  op_ret = get_obj_attrs(store, s, obj, orig_attrs);
  if (op_ret < 0) {
    return;
  }

  /* Check whether the object has expired. Swift API documentation
   * stands that we should return 404 Not Found in such case. */
  if (need_object_expiration() && object_is_expired(orig_attrs)) {
    op_ret = -ENOENT;
    return;
  }

  /* Filter currently existing attributes. */
  prepare_add_del_attrs(orig_attrs, attrs, rmattrs);
  populate_with_generic_attrs(s, attrs);
  encode_delete_at_attr(delete_at, attrs);

  if (dlo_manifest) {
    op_ret = encode_dlo_manifest_attr(dlo_manifest, attrs);
    if (op_ret < 0) {
      ldout(s->cct, 0) << "bad user manifest: " << dlo_manifest << dendl;
      return;
    }
  }

  op_ret = store->set_attrs(s->obj_ctx, obj, attrs, &rmattrs);
}

int RGWDeleteObj::handle_slo_manifest(bufferlist& bl)
{
  RGWSLOInfo slo_info;
  bufferlist::iterator bliter = bl.begin();
  try {
    ::decode(slo_info, bliter);
  } catch (buffer::error& err) {
    ldout(s->cct, 0) << "ERROR: failed to decode slo manifest" << dendl;
    return -EIO;
  }

  try {
    deleter = std::unique_ptr<RGWBulkDelete::Deleter>(\
          new RGWBulkDelete::Deleter(store, s));
  } catch (std::bad_alloc) {
    return -ENOMEM;
  }

  list<RGWBulkDelete::acct_path_t> items;
  for (const auto& iter : slo_info.entries) {
    const string& path_str = iter.path;

    const size_t sep_pos = path_str.find('/', 1 /* skip first slash */);
    if (string::npos == sep_pos) {
      return -EINVAL;
    }

    RGWBulkDelete::acct_path_t path;

    string bucket_name;
    url_decode(path_str.substr(1, sep_pos - 1), bucket_name);

    string obj_name;
    url_decode(path_str.substr(sep_pos + 1), obj_name);

    path.bucket_name = bucket_name;
    path.obj_key = obj_name;

    items.push_back(path);
  }

  /* Request removal of the manifest object itself. */
  RGWBulkDelete::acct_path_t path;
  path.bucket_name = s->bucket_name;
  path.obj_key = s->object;
  items.push_back(path);

  int ret = deleter->delete_chunk(items);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWDeleteObj::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWDeleteObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteObj::execute()
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  rgw_obj obj(s->bucket, s->object.name);
  obj.set_instance(s->object.instance);
  map<string, bufferlist> attrs;


  if (!s->object.empty()) {
    if (need_object_expiration() || multipart_delete) {
      /* check if obj exists, read orig attrs */
      op_ret = get_obj_attrs(store, s, obj, attrs);
      if (op_ret < 0) {
        return;
      }
    }

    if (multipart_delete) {
      const auto slo_attr = attrs.find(RGW_ATTR_SLO_MANIFEST);

      if (slo_attr != attrs.end()) {
        op_ret = handle_slo_manifest(slo_attr->second);
        if (op_ret < 0) {
          ldout(s->cct, 0) << "ERROR: failed to handle slo manifest ret=" << op_ret << dendl;
        }
      } else {
        op_ret = -ERR_NOT_SLO_MANIFEST;
      }

      return;
    }

    RGWObjectCtx *obj_ctx = static_cast<RGWObjectCtx *>(s->obj_ctx);

    obj_ctx->set_atomic(obj);

    RGWRados::Object del_target(store, s->bucket_info, *obj_ctx, obj);
    RGWRados::Object::Delete del_op(&del_target);

    op_ret = get_system_versioning_params(s, &del_op.params.olh_epoch,
					  &del_op.params.marker_version_id);
    if (op_ret < 0) {
      return;
    }

    del_op.params.bucket_owner = s->bucket_owner.get_id();
    del_op.params.versioning_status = s->bucket_info.versioning_status();
    del_op.params.obj_owner = s->owner;
    del_op.params.unmod_since = unmod_since;
    del_op.params.high_precision_time = s->system_request; /* system request uses high precision time */

    op_ret = del_op.delete_obj();
    if (op_ret >= 0) {
      delete_marker = del_op.result.delete_marker;
      version_id = del_op.result.version_id;
    }

    /* Check whether the object has expired. Swift API documentation
     * stands that we should return 404 Not Found in such case. */
    if (need_object_expiration() && object_is_expired(attrs)) {
      op_ret = -ENOENT;
      return;
    }

    if (op_ret == -ERR_PRECONDITION_FAILED && no_precondition_error) {
      op_ret = 0;
    }
  } else {
    op_ret = -EINVAL;
  }
}


bool RGWCopyObj::parse_copy_location(const string& url_src, string& bucket_name, rgw_obj_key& key)
{
  string name_str;
  string params_str;

  int pos = url_src.find('?');
  if (pos < 0) {
    name_str = url_src;
  } else {
    name_str = url_src.substr(0, pos);
    params_str = url_src.substr(pos + 1);
  }

  string dec_src;

  url_decode(name_str, dec_src);
  const char *src = dec_src.c_str();

  if (*src == '/') ++src;

  string str(src);

  pos = str.find('/');
  if (pos <= 0)
    return false;

  bucket_name = str.substr(0, pos);
  key.name = str.substr(pos + 1);

  if (key.name.empty()) {
    return false;
  }

  if (!params_str.empty()) {
    RGWHTTPArgs args;
    args.set(params_str);
    args.parse();

    key.instance = args.get("versionId", NULL);
  }

  return true;
}

int RGWCopyObj::verify_permission()
{
  RGWAccessControlPolicy src_policy(s->cct);
  op_ret = get_params();
  if (op_ret < 0)
    return op_ret;

  op_ret = get_system_versioning_params(s, &olh_epoch, &version_id);
  if (op_ret < 0) {
    return op_ret;
  }
  map<string, bufferlist> src_attrs;

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);

  if (s->bucket_instance_id.empty()) {
    op_ret = store->get_bucket_info(obj_ctx, src_tenant_name, src_bucket_name, src_bucket_info, NULL, &src_attrs);
  } else {
    /* will only happen in intra region sync where the source and dest bucket is the same */
    op_ret = store->get_bucket_instance_info(obj_ctx, s->bucket_instance_id, src_bucket_info, NULL, &src_attrs);
  }
  if (op_ret < 0) {
    if (op_ret == -ENOENT) {
      op_ret = -ERR_NO_SUCH_BUCKET;
    }
    return op_ret;
  }

  src_bucket = src_bucket_info.bucket;

  /* get buckets info (source and dest) */
  if (s->local_source &&  source_zone.empty()) {
    rgw_obj src_obj(src_bucket, src_object);
    store->set_atomic(s->obj_ctx, src_obj);
    store->set_prefetch_data(s->obj_ctx, src_obj);

    /* check source object permissions */
    op_ret = read_policy(store, s, src_bucket_info, src_attrs, &src_policy,
			 src_bucket, src_object);
    if (op_ret < 0)
      return op_ret;

    if (!s->system_request && /* system request overrides permission checks */
        !src_policy.verify_permission(s->user->user_id, s->perm_mask,
				      RGW_PERM_READ))
      return -EACCES;
  }

  RGWAccessControlPolicy dest_bucket_policy(s->cct);
  map<string, bufferlist> dest_attrs;

  if (src_bucket_name.compare(dest_bucket_name) == 0) { /* will only happen if s->local_source
                                                           or intra region sync */
    dest_bucket_info = src_bucket_info;
    dest_attrs = src_attrs;
  } else {
    op_ret = store->get_bucket_info(obj_ctx, dest_tenant_name, dest_bucket_name,
				    dest_bucket_info, NULL, &dest_attrs);
    if (op_ret < 0) {
      if (op_ret == -ENOENT) {
        op_ret = -ERR_NO_SUCH_BUCKET;
      }
      return op_ret;
    }
  }

  dest_bucket = dest_bucket_info.bucket;

  rgw_obj dest_obj(dest_bucket, dest_object);
  store->set_atomic(s->obj_ctx, dest_obj);

  rgw_obj_key no_obj;

  /* check dest bucket permissions */
  op_ret = read_policy(store, s, dest_bucket_info, dest_attrs,
		       &dest_bucket_policy, dest_bucket, no_obj);
  if (op_ret < 0)
    return op_ret;

  if (!s->system_request && /* system request overrides permission checks */
      !dest_bucket_policy.verify_permission(s->user->user_id, s->perm_mask,
					    RGW_PERM_WRITE))
    return -EACCES;

  op_ret = init_dest_policy();
  if (op_ret < 0)
    return op_ret;

  return 0;
}


int RGWCopyObj::init_common()
{
  if (if_mod) {
    if (parse_time(if_mod, &mod_time) < 0) {
      op_ret = -EINVAL;
      return op_ret;
    }
    mod_ptr = &mod_time;
  }

  if (if_unmod) {
    if (parse_time(if_unmod, &unmod_time) < 0) {
      op_ret = -EINVAL;
      return op_ret;
    }
    unmod_ptr = &unmod_time;
  }

  bufferlist aclbl;
  dest_policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;
  rgw_get_request_metadata(s->cct, s->info, attrs);

  map<string, string>::iterator iter;
  for (iter = s->generic_attrs.begin(); iter != s->generic_attrs.end(); ++iter) {
    bufferlist& attrbl = attrs[iter->first];
    const string& val = iter->second;
    attrbl.append(val.c_str(), val.size() + 1);
  }

  return 0;
}

static void copy_obj_progress_cb(off_t ofs, void *param)
{
  RGWCopyObj *op = static_cast<RGWCopyObj *>(param);
  op->progress_cb(ofs);
}

void RGWCopyObj::progress_cb(off_t ofs)
{
  if (!s->cct->_conf->rgw_copy_obj_progress)
    return;

  if (ofs - last_ofs < s->cct->_conf->rgw_copy_obj_progress_every_bytes)
    return;

  send_partial_response(ofs);

  last_ofs = ofs;
}

void RGWCopyObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWCopyObj::execute()
{
  if (init_common() < 0)
    return;

  rgw_obj src_obj(src_bucket, src_object);
  rgw_obj dst_obj(dest_bucket, dest_object);

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  obj_ctx.set_atomic(src_obj);
  obj_ctx.set_atomic(dst_obj);

  encode_delete_at_attr(delete_at, attrs);

  bool high_precision_time = (s->system_request);

  op_ret = store->copy_obj(obj_ctx,
			   s->user->user_id,
			   client_id,
			   op_id,
			   &s->info,
			   source_zone,
			   dst_obj,
			   src_obj,
			   dest_bucket_info,
			   src_bucket_info,
			   &src_mtime,
			   &mtime,
			   mod_ptr,
			   unmod_ptr,
                           high_precision_time,
			   if_match,
			   if_nomatch,
			   attrs_mod,
                           copy_if_newer,
			   attrs, RGW_OBJ_CATEGORY_MAIN,
			   olh_epoch,
			   delete_at,
			   (version_id.empty() ? NULL : &version_id),
			   &s->req_id, /* use req_id as tag */
			   &etag,
			   &s->err,
			   copy_obj_progress_cb, (void *)this
    );
}

int RGWGetACLs::verify_permission()
{
  bool perm;
  if (!s->object.empty()) {
    perm = verify_object_permission(s, RGW_PERM_READ_ACP);
  } else {
    perm = verify_bucket_permission(s, RGW_PERM_READ_ACP);
  }
  if (!perm)
    return -EACCES;

  return 0;
}

void RGWGetACLs::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetACLs::execute()
{
  stringstream ss;
  RGWAccessControlPolicy *acl = (!s->object.empty() ? s->object_acl : s->bucket_acl);
  RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(acl);
  s3policy->to_xml(ss);
  acls = ss.str();
}



int RGWPutACLs::verify_permission()
{
  bool perm;
  if (!s->object.empty()) {
    perm = verify_object_permission(s, RGW_PERM_WRITE_ACP);
  } else {
    perm = verify_bucket_permission(s, RGW_PERM_WRITE_ACP);
  }
  if (!perm)
    return -EACCES;

  return 0;
}

void RGWPutACLs::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPutACLs::execute()
{
  bufferlist bl;

  RGWAccessControlPolicy_S3 *policy = NULL;
  RGWACLXMLParser_S3 parser(s->cct);
  RGWAccessControlPolicy_S3 new_policy(s->cct);
  stringstream ss;
  char *new_data = NULL;
  rgw_obj obj;

  op_ret = 0; /* XXX redundant? */

  if (!parser.init()) {
    op_ret = -EINVAL;
    return;
  }


  RGWAccessControlPolicy *existing_policy = (s->object.empty() ? s->bucket_acl : s->object_acl);

  owner = existing_policy->get_owner();

  op_ret = get_params();
  if (op_ret < 0)
    return;

  ldout(s->cct, 15) << "read len=" << len << " data=" << (data ? data : "") << dendl;

  if (!s->canned_acl.empty() && len) {
    op_ret = -EINVAL;
    return;
  }

  if (!s->canned_acl.empty() || s->has_acl_header) {
    op_ret = get_policy_from_state(store, s, ss);
    if (op_ret < 0)
      return;

    new_data = strdup(ss.str().c_str());
    free(data);
    data = new_data;
    len = ss.str().size();
  }

  if (!parser.parse(data, len, 1)) {
    op_ret = -EACCES;
    return;
  }
  policy = static_cast<RGWAccessControlPolicy_S3 *>(parser.find_first("AccessControlPolicy"));
  if (!policy) {
    op_ret = -EINVAL;
    return;
  }

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    ldout(s->cct, 15) << "Old AccessControlPolicy";
    policy->to_xml(*_dout);
    *_dout << dendl;
  }

  op_ret = policy->rebuild(store, &owner, new_policy);
  if (op_ret < 0)
    return;

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    ldout(s->cct, 15) << "New AccessControlPolicy:";
    new_policy.to_xml(*_dout);
    *_dout << dendl;
  }

  new_policy.encode(bl);
  obj = rgw_obj(s->bucket, s->object);
  map<string, bufferlist> attrs;

  store->set_atomic(s->obj_ctx, obj);

  if (!s->object.empty()) {
    op_ret = get_obj_attrs(store, s, obj, attrs);
    if (op_ret < 0)
      return;
  
    attrs[RGW_ATTR_ACL] = bl;
    op_ret = store->set_attrs(s->obj_ctx, obj, attrs, NULL);
  } else {
    attrs = s->bucket_attrs;
    attrs[RGW_ATTR_ACL] = bl;
    op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs, &s->bucket_info.objv_tracker);
  }
}

int RGWGetCORS::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWGetCORS::execute()
{
  op_ret = read_bucket_cors();
  if (op_ret < 0)
    return ;

  if (!cors_exist) {
    dout(2) << "No CORS configuration set yet for this bucket" << dendl;
    op_ret = -ENOENT;
    return;
  }
}

int RGWPutCORS::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWPutCORS::execute()
{
  rgw_obj obj;

  op_ret = get_params();
  if (op_ret < 0)
    return;

  bool is_object_op = (!s->object.empty());
  if (is_object_op) {
    store->get_bucket_instance_obj(s->bucket, obj);
    store->set_atomic(s->obj_ctx, obj);
    op_ret = store->set_attr(s->obj_ctx, obj, RGW_ATTR_CORS, cors_bl);
  } else {
    map<string, bufferlist> attrs = s->bucket_attrs;
    attrs[RGW_ATTR_CORS] = cors_bl;
    op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs, &s->bucket_info.objv_tracker);
  }
}

int RGWDeleteCORS::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWDeleteCORS::execute()
{
  op_ret = read_bucket_cors();
  if (op_ret < 0)
    return;

  bufferlist bl;
  rgw_obj obj;
  if (!cors_exist) {
    dout(2) << "No CORS configuration set yet for this bucket" << dendl;
    op_ret = -ENOENT;
    return;
  }
  store->get_bucket_instance_obj(s->bucket, obj);
  store->set_atomic(s->obj_ctx, obj);
  map<string, bufferlist> orig_attrs, attrs, rmattrs;
  map<string, bufferlist>::iterator iter;

  bool is_object_op = (!s->object.empty());


  if (is_object_op) {
    /* check if obj exists, read orig attrs */
    op_ret = get_obj_attrs(store, s, obj, orig_attrs);
    if (op_ret < 0)
      return;
  } else {
    op_ret = get_system_obj_attrs(store, s, obj, orig_attrs, NULL, &s->bucket_info.objv_tracker);
    if (op_ret < 0)
      return;
  }

  /* only remove meta attrs */
  for (iter = orig_attrs.begin(); iter != orig_attrs.end(); ++iter) {
    const string& name = iter->first;
    dout(10) << "DeleteCORS : attr: " << name << dendl;
    if (name.compare(0, (sizeof(RGW_ATTR_CORS) - 1), RGW_ATTR_CORS) == 0) {
      rmattrs[name] = iter->second;
    } else if (attrs.find(name) == attrs.end()) {
      attrs[name] = iter->second;
    }
  }
  if (is_object_op) {
    op_ret = store->set_attrs(s->obj_ctx, obj, attrs, &rmattrs);
  } else {
    op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs, &s->bucket_info.objv_tracker);
  }
}

void RGWOptionsCORS::get_response_params(string& hdrs, string& exp_hdrs, unsigned *max_age) {
  get_cors_response_headers(rule, req_hdrs, hdrs, exp_hdrs, max_age);
}

int RGWOptionsCORS::validate_cors_request(RGWCORSConfiguration *cc) {
  rule = cc->host_name_rule(origin);
  if (!rule) {
    dout(10) << "There is no cors rule present for " << origin << dendl;
    return -ENOENT;
  }

  if (!validate_cors_rule_method(rule, req_meth)) {
    return -ENOENT;
  }
  return 0;
}

void RGWOptionsCORS::execute()
{
  op_ret = read_bucket_cors();
  if (op_ret < 0)
    return;

  origin = s->info.env->get("HTTP_ORIGIN");
  if (!origin) {
    dout(0) <<
    "Preflight request without mandatory Origin header"
    << dendl;
    op_ret = -EINVAL;
    return;
  }
  req_meth = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_METHOD");
  if (!req_meth) {
    dout(0) <<
    "Preflight request without mandatory Access-control-request-method header"
    << dendl;
    op_ret = -EINVAL;
    return;
  }
  if (!cors_exist) {
    dout(2) << "No CORS configuration set yet for this bucket" << dendl;
    op_ret = -ENOENT;
    return;
  }
  req_hdrs = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_HEADERS");
  op_ret = validate_cors_request(&bucket_cors);
  if (!rule) {
    origin = req_meth = NULL;
    return;
  }
  return;
}

int RGWGetRequestPayment::verify_permission()
{
  return 0;
}

void RGWGetRequestPayment::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetRequestPayment::execute()
{
  requester_pays = s->bucket_info.requester_pays;
}

int RGWSetRequestPayment::verify_permission()
{
  if (s->user->user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWSetRequestPayment::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWSetRequestPayment::execute()
{
  op_ret = get_params();

  if (op_ret < 0)
    return;

  s->bucket_info.requester_pays = requester_pays;
  op_ret = store->put_bucket_instance_info(s->bucket_info, false, real_time(),
					   &s->bucket_attrs);
  if (op_ret < 0) {
    ldout(s->cct, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name
		     << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWInitMultipart::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWInitMultipart::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWInitMultipart::execute()
{
  bufferlist aclbl;
  map<string, bufferlist> attrs;
  rgw_obj obj;
  map<string, string>::iterator iter;

  if (get_params() < 0)
    return;
  op_ret = -EINVAL;
  if (s->object.empty())
    return;

  policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;

  for (iter = s->generic_attrs.begin(); iter != s->generic_attrs.end(); ++iter) {
    bufferlist& attrbl = attrs[iter->first];
    const string& val = iter->second;
    attrbl.append(val.c_str(), val.size() + 1);
  }

  rgw_get_request_metadata(s->cct, s->info, attrs);

  do {
    char buf[33];
    gen_rand_alphanumeric(s->cct, buf, sizeof(buf) - 1);
    upload_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
    upload_id.append(buf);

    string tmp_obj_name;
    RGWMPObj mp(s->object.name, upload_id);
    tmp_obj_name = mp.get_meta();

    obj.init_ns(s->bucket, tmp_obj_name, mp_ns);
    // the meta object will be indexed with 0 size, we c
    obj.set_in_extra_data(true);
    obj.index_hash_source = s->object.name;

    RGWRados::Object op_target(store, s->bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
    op_target.set_versioning_disabled(true); /* no versioning for multipart meta */

    RGWRados::Object::Write obj_op(&op_target);

    obj_op.meta.owner = s->owner.get_id();
    obj_op.meta.category = RGW_OBJ_CATEGORY_MULTIMETA;
    obj_op.meta.flags = PUT_OBJ_CREATE_EXCL;

    op_ret = obj_op.write_meta(0, attrs);
  } while (op_ret == -EEXIST);
}

static int get_multipart_info(RGWRados *store, struct req_state *s,
			      string& meta_oid,
                              RGWAccessControlPolicy *policy,
			      map<string, bufferlist>& attrs)
{
  map<string, bufferlist>::iterator iter;
  bufferlist header;

  rgw_obj obj;
  obj.init_ns(s->bucket, meta_oid, mp_ns);
  obj.set_in_extra_data(true);

  int op_ret = get_obj_attrs(store, s, obj, attrs);
  if (op_ret < 0) {
    if (op_ret == -ENOENT) {
      return -ERR_NO_SUCH_UPLOAD;
    }
    return op_ret;
  }

  if (policy) {
    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
      string name = iter->first;
      if (name.compare(RGW_ATTR_ACL) == 0) {
        bufferlist& bl = iter->second;
        bufferlist::iterator bli = bl.begin();
        try {
          ::decode(*policy, bli);
        } catch (buffer::error& err) {
          ldout(s->cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
          return -EIO;
        }
        break;
      }
    }
  }

  return 0;
}

static int list_multipart_parts(RGWRados *store, struct req_state *s,
                                const string& upload_id,
                                string& meta_oid, int num_parts,
                                int marker, map<uint32_t, RGWUploadPartInfo>& parts,
                                int *next_marker, bool *truncated,
                                bool assume_unsorted = false)
{
  map<string, bufferlist> parts_map;
  map<string, bufferlist>::iterator iter;
  bufferlist header;

  rgw_obj obj;
  obj.init_ns(s->bucket, meta_oid, mp_ns);
  obj.set_in_extra_data(true);

  bool sorted_omap = is_v2_upload_id(upload_id) && !assume_unsorted;

  int ret;

  parts.clear();

  if (sorted_omap) {
    string p;
    p = "part.";
    char buf[32];

    snprintf(buf, sizeof(buf), "%08d", marker);
    p.append(buf);

    ret = store->omap_get_vals(obj, header, p, num_parts + 1, parts_map);
  } else {
    ret = store->omap_get_all(obj, header, parts_map);
  }
  if (ret < 0)
    return ret;

  int i;
  int last_num = 0;

  uint32_t expected_next = marker + 1;

  for (i = 0, iter = parts_map.begin(); (i < num_parts || !sorted_omap) && iter != parts_map.end(); ++iter, ++i) {
    bufferlist& bl = iter->second;
    bufferlist::iterator bli = bl.begin();
    RGWUploadPartInfo info;
    try {
      ::decode(info, bli);
    } catch (buffer::error& err) {
      ldout(s->cct, 0) << "ERROR: could not part info, caught buffer::error" << dendl;
      return -EIO;
    }
    if (sorted_omap) {
      if (info.num != expected_next) {
        /* ouch, we expected a specific part num here, but we got a different one. Either
         * a part is missing, or it could be a case of mixed rgw versions working on the same
         * upload, where one gateway doesn't support correctly sorted omap keys for multipart
         * upload just assume data is unsorted.
         */
        return list_multipart_parts(store, s, upload_id, meta_oid, num_parts, marker, parts, next_marker, truncated, true);
      }
      expected_next++;
    }
    if (sorted_omap ||
      (int)info.num > marker) {
      parts[info.num] = info;
      last_num = info.num;
    }
  }

  if (sorted_omap) {
    if (truncated)
      *truncated = (iter != parts_map.end());
  } else {
    /* rebuild a map with only num_parts entries */

    map<uint32_t, RGWUploadPartInfo> new_parts;
    map<uint32_t, RGWUploadPartInfo>::iterator piter;

    for (i = 0, piter = parts.begin(); i < num_parts && piter != parts.end(); ++i, ++piter) {
      new_parts[piter->first] = piter->second;
      last_num = piter->first;
    }

    if (truncated)
      *truncated = (piter != parts.end());

    parts.swap(new_parts);
  }

  if (next_marker) {
    *next_marker = last_num;
  }

  return 0;
}

int RGWCompleteMultipart::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWCompleteMultipart::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWCompleteMultipart::execute()
{
  RGWMultiCompleteUpload *parts;
  map<int, string>::iterator iter;
  RGWMultiXMLParser parser;
  string meta_oid;
  map<uint32_t, RGWUploadPartInfo> obj_parts;
  map<uint32_t, RGWUploadPartInfo>::iterator obj_iter;
  map<string, bufferlist> attrs;
  off_t ofs = 0;
  MD5 hash;
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  bufferlist etag_bl;
  rgw_obj meta_obj;
  rgw_obj target_obj;
  RGWMPObj mp;
  RGWObjManifest manifest;
  uint64_t olh_epoch = 0;
  string version_id;

  op_ret = get_params();
  if (op_ret < 0)
    return;

  op_ret = get_system_versioning_params(s, &olh_epoch, &version_id);
  if (op_ret < 0) {
    return;
  }

  if (!data || !len) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  if (!parser.init()) {
    op_ret = -EIO;
    return;
  }

  if (!parser.parse(data, len, 1)) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  parts = static_cast<RGWMultiCompleteUpload *>(parser.find_first("CompleteMultipartUpload"));
  if (!parts || parts->parts.empty()) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  if ((int)parts->parts.size() >
      s->cct->_conf->rgw_multipart_part_upload_limit) {
    op_ret = -ERANGE;
    return;
  }

  mp.init(s->object.name, upload_id);
  meta_oid = mp.get_meta();

  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  bool truncated;

  uint64_t min_part_size = s->cct->_conf->rgw_multipart_min_part_size;

  list<rgw_obj_key> remove_objs; /* objects to be removed from index listing */

  bool versioned_object = s->bucket_info.versioning_enabled();

  iter = parts->parts.begin();

  meta_obj.init_ns(s->bucket, meta_oid, mp_ns);
  meta_obj.set_in_extra_data(true);
  meta_obj.index_hash_source = s->object.name;

  op_ret = get_obj_attrs(store, s, meta_obj, attrs);
  if (op_ret < 0) {
    ldout(s->cct, 0) << "ERROR: failed to get obj attrs, obj=" << meta_obj
		     << " ret=" << op_ret << dendl;
    return;
  }

  do {
    op_ret = list_multipart_parts(store, s, upload_id, meta_oid, max_parts,
				  marker, obj_parts, &marker, &truncated);
    if (op_ret == -ENOENT) {
      op_ret = -ERR_NO_SUCH_UPLOAD;
    }
    if (op_ret < 0)
      return;

    total_parts += obj_parts.size();
    if (!truncated && total_parts != (int)parts->parts.size()) {
      op_ret = -ERR_INVALID_PART;
      return;
    }

    for (obj_iter = obj_parts.begin(); iter != parts->parts.end() && obj_iter != obj_parts.end(); ++iter, ++obj_iter, ++handled_parts) {
      uint64_t part_size = obj_iter->second.size;
      if (handled_parts < (int)parts->parts.size() - 1 &&
          part_size < min_part_size) {
        op_ret = -ERR_TOO_SMALL;
        return;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (iter->first != (int)obj_iter->first) {
        ldout(s->cct, 0) << "NOTICE: parts num mismatch: next requested: "
			 << iter->first << " next uploaded: "
			 << obj_iter->first << dendl;
        op_ret = -ERR_INVALID_PART;
        return;
      }
      string part_etag = rgw_string_unquote(iter->second);
      if (part_etag.compare(obj_iter->second.etag) != 0) {
        ldout(s->cct, 0) << "NOTICE: etag mismatch: part: " << iter->first
			 << " etag: " << iter->second << dendl;
        op_ret = -ERR_INVALID_PART;
        return;
      }

      hex_to_buf(obj_iter->second.etag.c_str(), petag,
		CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const byte *)petag, sizeof(petag));

      RGWUploadPartInfo& obj_part = obj_iter->second;

      /* update manifest for part */
      string oid = mp.get_part(obj_iter->second.num);
      rgw_obj src_obj;
      src_obj.init_ns(s->bucket, oid, mp_ns);

      if (obj_part.manifest.empty()) {
        ldout(s->cct, 0) << "ERROR: empty manifest for object part: obj="
			 << src_obj << dendl;
        op_ret = -ERR_INVALID_PART;
        return;
      } else {
        manifest.append(obj_part.manifest);
      }

      rgw_obj_key remove_key;
      src_obj.get_index_key(&remove_key);

      remove_objs.push_back(remove_key);

      ofs += obj_part.size;
    }
  } while (truncated);
  hash.Final((byte *)final_etag);

  buf_to_hex((unsigned char *)final_etag, sizeof(final_etag), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],  sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)parts->parts.size());
  etag = final_etag_str;
  ldout(s->cct, 10) << "calculated etag: " << final_etag_str << dendl;

  etag_bl.append(final_etag_str, strlen(final_etag_str) + 1);

  attrs[RGW_ATTR_ETAG] = etag_bl;

  target_obj.init(s->bucket, s->object.name);
  if (versioned_object) {
    store->gen_rand_obj_instance_name(&target_obj);
  }

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);

  obj_ctx.set_atomic(target_obj);

  RGWRados::Object op_target(store, s->bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), target_obj);
  RGWRados::Object::Write obj_op(&op_target);

  obj_op.meta.manifest = &manifest;
  obj_op.meta.remove_objs = &remove_objs;

  obj_op.meta.ptag = &s->req_id; /* use req_id as operation tag */
  obj_op.meta.owner = s->owner.get_id();
  obj_op.meta.flags = PUT_OBJ_CREATE;

  op_ret = obj_op.write_meta(ofs, attrs);
  if (op_ret < 0)
    return;

  // remove the upload obj
  int r = store->delete_obj(*static_cast<RGWObjectCtx *>(s->obj_ctx),
			    s->bucket_info, meta_obj, 0);
  if (r < 0) {
    ldout(store->ctx(), 0) << "WARNING: failed to remove object " << meta_obj << dendl;
  }
}

int RGWAbortMultipart::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWAbortMultipart::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWAbortMultipart::execute()
{
  op_ret = -EINVAL;
  string upload_id;
  string meta_oid;
  upload_id = s->info.args.get("uploadId");
  map<uint32_t, RGWUploadPartInfo> obj_parts;
  map<uint32_t, RGWUploadPartInfo>::iterator obj_iter;
  map<string, bufferlist> attrs;
  rgw_obj meta_obj;
  RGWMPObj mp;

  if (upload_id.empty() || s->object.empty())
    return;

  mp.init(s->object.name, upload_id);
  meta_oid = mp.get_meta();

  op_ret = get_multipart_info(store, s, meta_oid, NULL, attrs);
  if (op_ret < 0)
    return;

  bool truncated;
  int marker = 0;
  int max_parts = 1000;


  RGWObjectCtx *obj_ctx = static_cast<RGWObjectCtx *>(s->obj_ctx);

  meta_obj.init_ns(s->bucket, meta_oid, mp_ns);
  meta_obj.set_in_extra_data(true);
  meta_obj.index_hash_source = s->object.name;

  cls_rgw_obj_chain chain;
  list<rgw_obj_key> remove_objs;

  do {
    op_ret = list_multipart_parts(store, s, upload_id, meta_oid, max_parts,
				  marker, obj_parts, &marker, &truncated);
    if (op_ret < 0)
      return;

    for (obj_iter = obj_parts.begin();
	 obj_iter != obj_parts.end(); ++obj_iter) {
      RGWUploadPartInfo& obj_part = obj_iter->second;

      if (obj_part.manifest.empty()) {
        string oid = mp.get_part(obj_iter->second.num);
        rgw_obj obj;
        obj.init_ns(s->bucket, oid, mp_ns);
        obj.index_hash_source = s->object.name;
        op_ret = store->delete_obj(*obj_ctx, s->bucket_info, obj, 0);
        if (op_ret < 0 && op_ret != -ENOENT)
          return;
      } else {
        store->update_gc_chain(meta_obj, obj_part.manifest, &chain);
        RGWObjManifest::obj_iterator oiter = obj_part.manifest.obj_begin();
        if (oiter != obj_part.manifest.obj_end()) {
          rgw_obj head = oiter.get_location();
          rgw_obj_key key;
          head.get_index_key(&key);
          remove_objs.push_back(key);
        }
      }
    }
  } while (truncated);

  /* use upload id as tag */
  op_ret = store->send_chain_to_gc(chain, upload_id , false);  // do it async
  if (op_ret < 0) {
    ldout(store->ctx(), 5) << "gc->send_chain() returned " << op_ret << dendl;
    return;
  }

  RGWRados::Object del_target(store, s->bucket_info, *obj_ctx, meta_obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = s->bucket_info.owner;
  del_op.params.versioning_status = 0;
  if (!remove_objs.empty()) {
    del_op.params.remove_objs = &remove_objs;
  }

  // and also remove the metadata obj
  op_ret = del_op.delete_obj();
  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_SUCH_BUCKET;
  }
}

int RGWListMultipart::verify_permission()
{
  if (!verify_object_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

void RGWListMultipart::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWListMultipart::execute()
{
  map<string, bufferlist> xattrs;
  string meta_oid;
  RGWMPObj mp;

  op_ret = get_params();
  if (op_ret < 0)
    return;

  mp.init(s->object.name, upload_id);
  meta_oid = mp.get_meta();

  op_ret = get_multipart_info(store, s, meta_oid, &policy, xattrs);
  if (op_ret < 0)
    return;

  op_ret = list_multipart_parts(store, s, upload_id, meta_oid, max_parts,
				marker, parts, NULL, &truncated);
}

int RGWListBucketMultiparts::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}

void RGWListBucketMultiparts::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWListBucketMultiparts::execute()
{
  vector<RGWObjEnt> objs;
  string marker_meta;

  op_ret = get_params();
  if (op_ret < 0)
    return;

  if (s->prot_flags & RGW_REST_SWIFT) {
    string path_args;
    path_args = s->info.args.get("path");
    if (!path_args.empty()) {
      if (!delimiter.empty() || !prefix.empty()) {
        op_ret = -EINVAL;
        return;
      }
      prefix = path_args;
      delimiter="/";
    }
  }
  marker_meta = marker.get_meta();

  RGWRados::Bucket target(store, s->bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.prefix = prefix;
  list_op.params.delim = delimiter;
  list_op.params.marker = marker_meta;
  list_op.params.ns = mp_ns;
  list_op.params.filter = &mp_filter;

  op_ret = list_op.list_objects(max_uploads, &objs, &common_prefixes,
				&is_truncated);
  if (!objs.empty()) {
    vector<RGWObjEnt>::iterator iter;
    RGWMultipartUploadEntry entry;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      rgw_obj_key& key = iter->key;
      if (!entry.mp.from_meta(key.name))
        continue;
      entry.obj = *iter;
      uploads.push_back(entry);
    }
    next_marker = entry;
  }
}

int RGWDeleteMultiObj::verify_permission()
{
  if (!verify_bucket_permission(s, RGW_PERM_WRITE))
    return -EACCES;

  return 0;
}

void RGWDeleteMultiObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteMultiObj::execute()
{
  RGWMultiDelDelete *multi_delete;
  vector<rgw_obj_key>::iterator iter;
  RGWMultiDelXMLParser parser;
  int num_processed = 0;
  RGWObjectCtx *obj_ctx = static_cast<RGWObjectCtx *>(s->obj_ctx);

  op_ret = get_params();
  if (op_ret < 0) {
    goto error;
  }

  if (!data) {
    op_ret = -EINVAL;
    goto error;
  }

  if (!parser.init()) {
    op_ret = -EINVAL;
    goto error;
  }

  if (!parser.parse(data, len, 1)) {
    op_ret = -EINVAL;
    goto error;
  }

  multi_delete = static_cast<RGWMultiDelDelete *>(parser.find_first("Delete"));
  if (!multi_delete) {
    op_ret = -EINVAL;
    goto error;
  }

  if (multi_delete->is_quiet())
    quiet = true;

  begin_response();
  if (multi_delete->objects.empty()) {
    goto done;
  }

  for (iter = multi_delete->objects.begin();
        iter != multi_delete->objects.end() && num_processed < max_to_delete;
        ++iter, num_processed++) {
    rgw_obj obj(bucket, *iter);

    obj_ctx->set_atomic(obj);

    RGWRados::Object del_target(store, s->bucket_info, *obj_ctx, obj);
    RGWRados::Object::Delete del_op(&del_target);

    del_op.params.bucket_owner = s->bucket_owner.get_id();
    del_op.params.versioning_status = s->bucket_info.versioning_status();
    del_op.params.obj_owner = s->owner;

    op_ret = del_op.delete_obj();
    if (op_ret == -ENOENT) {
      op_ret = 0;
    }

    send_partial_response(*iter, del_op.result.delete_marker,
			  del_op.result.version_id, op_ret);
  }

  /*  set the return code to zero, errors at this point will be
  dumped to the response */
  op_ret = 0;

done:
  // will likely segfault if begin_response() has not been called
  end_response();
  free(data);
  return;

error:
  send_status();
  free(data);
  return;

}

bool RGWBulkDelete::Deleter::verify_permission(RGWBucketInfo& binfo,
                                               map<string, bufferlist>& battrs,
                                               rgw_obj& obj,
                                               ACLOwner& bucket_owner /* out */)
{
  RGWAccessControlPolicy bacl(store->ctx());
  rgw_obj_key no_obj;
  int ret = read_policy(store, s, binfo, battrs, &bacl, binfo.bucket, no_obj);
  if (ret < 0) {
    return false;
  }

  RGWAccessControlPolicy oacl(s->cct);
  ret = read_policy(store, s, binfo, battrs, &oacl, binfo.bucket, s->object);
  if (ret < 0) {
    return false;
  }

  bucket_owner = bacl.get_owner();

  return verify_object_permission(s, &bacl, &oacl, RGW_PERM_WRITE);
}

bool RGWBulkDelete::Deleter::verify_permission(RGWBucketInfo& binfo,
                                               map<string, bufferlist>& battrs)
{
  RGWAccessControlPolicy bacl(store->ctx());
  rgw_obj_key no_obj;
  int ret = read_policy(store, s, binfo, battrs, &bacl, binfo.bucket, no_obj);
  if (ret < 0) {
    return false;
  }

  return verify_bucket_permission(s, &bacl, RGW_PERM_WRITE);
}

bool RGWBulkDelete::Deleter::delete_single(const acct_path_t& path)
{
  auto& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);

  RGWBucketInfo binfo;
  map<string, bufferlist> battrs;
  int ret = store->get_bucket_info(obj_ctx, s->user->user_id.tenant,
				  path.bucket_name, binfo, NULL, &battrs);
  if (ret < 0) {
    goto binfo_fail;
  }

  if (!path.obj_key.empty()) {
    rgw_obj obj(binfo.bucket, path.obj_key);
    obj_ctx.set_atomic(obj);

    RGWRados::Object del_target(store, binfo, obj_ctx, obj);
    RGWRados::Object::Delete del_op(&del_target);

    ACLOwner owner;
    if (!verify_permission(binfo, battrs, obj, owner)) {
      ret = -EACCES;
      goto auth_fail;
    }

    del_op.params.bucket_owner = binfo.owner;
    del_op.params.versioning_status = binfo.versioning_status();
    del_op.params.obj_owner = owner;

    ret = del_op.delete_obj();
    if (ret < 0) {
      goto delop_fail;
    }
  } else {
    RGWObjVersionTracker ot;
    ot.read_version = binfo.ep_objv;

    if (!verify_permission(binfo, battrs)) {
      ret = -EACCES;
      goto auth_fail;
    }

    ret = store->delete_bucket(binfo.bucket, ot);
    if (0 == ret) {
      ret = rgw_unlink_bucket(store, binfo.owner, binfo.bucket.tenant,
			      binfo.bucket.name, false);
      if (ret < 0) {
        ldout(s->cct, 0) << "WARNING: failed to unlink bucket: ret=" << ret
			 << dendl;
      }
    }
    if (ret < 0) {
      goto delop_fail;
    }

    if (!store->get_zonegroup().is_master) {
      bufferlist in_data;
      ret = forward_request_to_master(s, &ot.read_version, store, in_data,
				      NULL);
      if (ret < 0) {
        if (ret == -ENOENT) {
          /* adjust error, we want to return with NoSuchBucket and not
	   * NoSuchKey */
          ret = -ERR_NO_SUCH_BUCKET;
        }
        goto delop_fail;
      }
    }
  }

  num_deleted++;
  return true;


binfo_fail:
    if (-ENOENT == ret) {
      ldout(store->ctx(), 20) << "cannot find bucket = " << path.bucket_name << dendl;
      num_unfound++;
    } else {
      ldout(store->ctx(), 20) << "cannot get bucket info, ret = " << ret
			      << dendl;

      fail_desc_t failed_item = {
        .err  = ret,
        .path = path
      };
      failures.push_back(failed_item);
    }
    return false;

auth_fail:
    ldout(store->ctx(), 20) << "wrong auth for " << path << dendl;
    {
      fail_desc_t failed_item = {
        .err  = ret,
        .path = path
      };
      failures.push_back(failed_item);
    }
    return false;

delop_fail:
    if (-ENOENT == ret) {
      ldout(store->ctx(), 20) << "cannot find entry " << path << dendl;
      num_unfound++;
    } else {
      fail_desc_t failed_item = {
        .err  = ret,
        .path = path
      };
      failures.push_back(failed_item);
    }
    return false;
}

bool RGWBulkDelete::Deleter::delete_chunk(const std::list<acct_path_t>& paths)
{
  ldout(store->ctx(), 20) << "in delete_chunk" << dendl;
  for (auto path : paths) {
    ldout(store->ctx(), 20) << "bulk deleting path: " << path << dendl;
    delete_single(path);
  }

  return true;
}

int RGWBulkDelete::verify_permission()
{
  return 0;
}

void RGWBulkDelete::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWBulkDelete::execute()
{
  deleter = std::unique_ptr<Deleter>(new Deleter(store, s));

  bool is_truncated = false;
  do {
    list<RGWBulkDelete::acct_path_t> items;

    int ret = get_data(items, &is_truncated);
    if (ret < 0) {
      return;
    }

    ret = deleter->delete_chunk(items);
  } while (!op_ret && is_truncated);

  return;
}

RGWHandler::~RGWHandler()
{
}

int RGWHandler::init(RGWRados *_store, struct req_state *_s, RGWClientIO *cio)
{
  store = _store;
  s = _s;

  return 0;
}

int RGWHandler::do_init_permissions()
{
  int ret = rgw_build_bucket_policies(store, s);

  if (ret < 0) {
    ldout(s->cct, 10) << "read_permissions on " << s->bucket << " ret=" << ret << dendl;
    if (ret == -ENODATA)
      ret = -EACCES;
  }

  return ret;
}

int RGWHandler::do_read_permissions(RGWOp *op, bool only_bucket)
{
  if (only_bucket) {
    /* already read bucket info */
    return 0;
  }
  int ret = rgw_build_object_policies(store, s, op->prefetch_data());

  if (ret < 0) {
    ldout(s->cct, 10) << "read_permissions on " << s->bucket << ":"
		      << s->object << " only_bucket=" << only_bucket
		      << " ret=" << ret << dendl;
    if (ret == -ENODATA)
      ret = -EACCES;
  }

  return ret;
}

int RGWOp::error_handler(int err_no, string *error_content) {
  return dialect_handler->error_handler(err_no, error_content);
}

int RGWHandler::error_handler(int err_no, string *error_content) {
  // This is the do-nothing error handler
  return err_no;
}
