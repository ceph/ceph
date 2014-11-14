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
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_multi.h"
#include "rgw_multi_del.h"
#include "rgw_cors.h"
#include "rgw_cors_s3.h"

#include "rgw_client_io.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;
using ceph::crypto::MD5;

static string mp_ns = RGW_OBJ_NS_MULTIPART;
static string shadow_ns = RGW_OBJ_NS_SHADOW;

#define MULTIPART_UPLOAD_ID_PREFIX "2/" // must contain a unique char that may not come up in gen_rand_alpha()

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

static void format_xattr(std::string &xattr)
{
  /* If the extended attribute is not valid UTF-8, we encode it using quoted-printable
   * encoding.
   */
  if ((check_utf8(xattr.c_str(), xattr.length()) != 0) ||
      (check_for_control_characters(xattr.c_str(), xattr.length()) != 0)) {
    static const char MIME_PREFIX_STR[] = "=?UTF-8?Q?";
    static const int MIME_PREFIX_LEN = sizeof(MIME_PREFIX_STR) - 1;
    static const char MIME_SUFFIX_STR[] = "?=";
    static const int MIME_SUFFIX_LEN = sizeof(MIME_SUFFIX_STR) - 1;
    int mlen = mime_encode_as_qp(xattr.c_str(), NULL, 0);
    char *mime = new char[MIME_PREFIX_LEN + mlen + MIME_SUFFIX_LEN + 1];
    strcpy(mime, MIME_PREFIX_STR);
    mime_encode_as_qp(xattr.c_str(), mime + MIME_PREFIX_LEN, mlen);
    strcpy(mime + MIME_PREFIX_LEN + (mlen - 1), MIME_SUFFIX_STR);
    xattr.assign(mime);
    delete [] mime;
  }
}

/**
 * Get the HTTP request metadata out of the req_state as a
 * map(<attr_name, attr_contents>, where attr_name is RGW_ATTR_PREFIX.HTTP_NAME)
 * s: The request state
 * attrs: will be filled up with attrs mapped as <attr_name, attr_contents>
 *
 */
static void rgw_get_request_metadata(CephContext *cct, struct req_info& info, map<string, bufferlist>& attrs)
{
  map<string, string>::iterator iter;
  for (iter = info.x_meta_map.begin(); iter != info.x_meta_map.end(); ++iter) {
    const string &name(iter->first);
    string &xattr(iter->second);
    ldout(cct, 10) << "x>> " << name << ":" << xattr << dendl;
    format_xattr(xattr);
    string attr_name(RGW_ATTR_PREFIX);
    attr_name.append(name);
    map<string, bufferlist>::value_type v(attr_name, bufferlist());
    std::pair < map<string, bufferlist>::iterator, bool > rval(attrs.insert(v));
    bufferlist& bl(rval.first->second);
    bl.append(xattr.c_str(), xattr.size() + 1);
  }
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
    ldout(cct, 15) << "Read AccessControlPolicy";
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

static int get_obj_policy_from_attr(CephContext *cct, RGWRados *store, void *ctx,
                                    RGWBucketInfo& bucket_info, map<string, bufferlist>& bucket_attrs,
                                    RGWAccessControlPolicy *policy, rgw_obj& obj)
{
  bufferlist bl;
  int ret = 0;

  ret = store->get_attr(ctx, obj, RGW_ATTR_ACL, bl);
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

  if (obj.object.empty()) {
    rgw_obj instance_obj;
    store->get_bucket_instance_obj(bucket_info.bucket, instance_obj);
    return get_bucket_policy_from_attr(cct, store, ctx, bucket_info, bucket_attrs,
                                       policy, instance_obj);
  }
  return get_obj_policy_from_attr(cct, store, ctx, bucket_info, bucket_attrs,
                                  policy, obj);
}

static int get_obj_attrs(RGWRados *store, struct req_state *s, rgw_obj& obj, map<string, bufferlist>& attrs,
                         uint64_t *obj_size, RGWObjVersionTracker *objv_tracker)
{
  void *handle;
  int ret = store->prepare_get_obj(s->obj_ctx, obj, NULL, NULL, &attrs, NULL,
                                      NULL, NULL, NULL, NULL, NULL, obj_size, objv_tracker, &handle, &s->err);
  store->finish_get_obj(&handle);
  return ret;
}

static int read_policy(RGWRados *store, struct req_state *s,
                       RGWBucketInfo& bucket_info, map<string, bufferlist>& bucket_attrs,
                       RGWAccessControlPolicy *policy, rgw_bucket& bucket, string& object)
{
  string upload_id;
  upload_id = s->info.args.get("uploadId");
  string oid = object;
  rgw_obj obj;

  if (!s->system_request && bucket_info.flags & BUCKET_SUSPENDED) {
    ldout(s->cct, 0) << "NOTICE: bucket " << bucket_info.bucket.name << " is suspended" << dendl;
    return -ERR_USER_SUSPENDED;
  }

  if (!oid.empty() && !upload_id.empty()) {
    RGWMPObj mp(oid, upload_id);
    oid = mp.get_meta();
    obj.init_ns(bucket, oid, mp_ns);
    obj.set_in_extra_data(true);
  } else {
    obj.init(bucket, oid);
  }
  int ret = get_policy_from_attr(s->cct, store, s->obj_ctx, bucket_info, bucket_attrs, policy, obj);
  if (ret == -ENOENT && object.size()) {
    /* object does not exist checking the bucket's ACL to make sure
       that we send a proper error code */
    RGWAccessControlPolicy bucket_policy(s->cct);
    string no_object;
    rgw_obj no_obj(bucket, no_object);
    ret = get_policy_from_attr(s->cct, store, s->obj_ctx, bucket_info, bucket_attrs, &bucket_policy, no_obj);
    if (ret < 0)
      return ret;
    string& owner = bucket_policy.get_owner().get_id();
    if (!s->system_request && owner.compare(s->user.user_id) != 0 &&
        !bucket_policy.verify_permission(s->user.user_id, s->perm_mask, RGW_PERM_READ))
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
static int rgw_build_policies(RGWRados *store, struct req_state *s, bool only_bucket, bool prefetch_data)
{
  int ret = 0;
  string obj_str;
  RGWUserInfo bucket_owner_info;

  s->bucket_instance_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "bucket-instance");

  s->bucket_acl = new RGWAccessControlPolicy(s->cct);

  if (s->copy_source) { /* check if copy source is within the current domain */
    const char *src = s->copy_source;
    if (*src == '/')
      ++src;
    string copy_source_str(src);

    int pos = copy_source_str.find('/');
    if (pos > 0)
      copy_source_str = copy_source_str.substr(0, pos);

    RGWBucketInfo source_info;

    ret = store->get_bucket_info(s->obj_ctx, copy_source_str, source_info, NULL);
    if (ret == 0) {
      string& region = source_info.region;
      s->local_source = store->region.equals(region);
    }
  }

  if (!s->bucket_name_str.empty()) {
    s->bucket_exists = true;
    if (s->bucket_instance_id.empty()) {
      ret = store->get_bucket_info(s->obj_ctx, s->bucket_name_str, s->bucket_info, NULL, &s->bucket_attrs);
    } else {
      ret = store->get_bucket_instance_info(s->obj_ctx, s->bucket_instance_id, s->bucket_info, NULL, &s->bucket_attrs);
    }
    if (ret < 0) {
      if (ret != -ENOENT) {
        ldout(s->cct, 0) << "NOTICE: couldn't get bucket from bucket_name (name=" << s->bucket_name_str << ")" << dendl;
        return ret;
      }
      s->bucket_exists = false;
    }
    s->bucket = s->bucket_info.bucket;

    if (s->bucket_exists) {
      string no_obj;
      ret = read_policy(store, s, s->bucket_info, s->bucket_attrs, s->bucket_acl, s->bucket, no_obj);
    } else {
      s->bucket_acl->create_default(s->user.user_id, s->user.display_name);
      ret = -ERR_NO_SUCH_BUCKET;
    }

    s->bucket_owner = s->bucket_acl->get_owner();

    string& region = s->bucket_info.region;
    map<string, RGWRegion>::iterator dest_region = store->region_map.regions.find(region);
    if (dest_region != store->region_map.regions.end()) {
      s->region_endpoint = dest_region->second.endpoints.front();
    }
    if (s->bucket_exists && !store->region.equals(region)) {
      ldout(s->cct, 0) << "NOTICE: request for data in a different region (" << region << " != " << store->region.name << ")" << dendl;
      /* we now need to make sure that the operation actually requires copy source, that is
       * it's a copy operation
       */
      if (store->region.is_master && s->op == OP_DELETE && s->system_request) {
        /*If the operation is delete and if this is the master, don't redirect*/
      } else if (!s->local_source ||
          (s->op != OP_PUT && s->op != OP_COPY) ||
          s->object_str.empty()) {
        return -ERR_PERMANENT_REDIRECT;
      }
    }
  }

  /* we're passed only_bucket = true when we specifically need the bucket's
     acls, that happens on write operations */
  if (!only_bucket && !s->object_str.empty()) {
    if (!s->bucket_exists) {
      return -ERR_NO_SUCH_BUCKET;
    }
    s->object_acl = new RGWAccessControlPolicy(s->cct);

    obj_str = s->object_str;
    rgw_obj obj(s->bucket, obj_str);
    store->set_atomic(s->obj_ctx, obj);
    if (prefetch_data) {
      store->set_prefetch_data(s->obj_ctx, obj);
    }
    ret = read_policy(store, s, s->bucket_info, s->bucket_attrs, s->object_acl, s->bucket, obj_str);
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
  obj.init(s->bucket, s->object_str);
  store->set_atomic(s->obj_ctx, obj);
  store->set_prefetch_data(s->obj_ctx, obj);

  if (!verify_object_permission(s, RGW_PERM_READ))
    return -EACCES;

  return 0;
}


int RGWOp::verify_op_mask()
{
  uint32_t required_mask = op_mask();

  ldout(s->cct, 20) << "required_mask= " << required_mask << " user.op_mask=" << s->user.op_mask << dendl;

  if ((s->user.op_mask & required_mask) != required_mask) {
    return -EPERM;
  }

  if (!s->system_request && (required_mask & RGW_OP_TYPE_MODIFY) && !store->zone.is_master)  {
    ldout(s->cct, 5) << "NOTICE: modify request to a non-master zone by a non-system user, permission denied"  << dendl;
    return -EPERM;
  }

  return 0;
}

int RGWOp::init_quota()
{
  /* no quota enforcement for system requests */
  if (s->system_request)
    return 0;

  /* init quota related stuff */
  if (!(s->user.op_mask & RGW_OP_TYPE_MODIFY)) {
    return 0;
  }

  /* only interested in object related ops */
  if (s->object_str.empty()) {
    return 0;
  }

  RGWUserInfo owner_info;
  RGWUserInfo *uinfo;

  if (s->user.user_id == s->bucket_owner.get_id()) {
    uinfo = &s->user;
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
    bucket_quota = store->region_map.bucket_quota;
  }

  if (uinfo->user_quota.enabled) {
    user_quota = uinfo->user_quota;
  } else {
    user_quota = store->region_map.user_quota;
  }

  return 0;
}

static bool validate_cors_rule_method(RGWCORSRule *rule, const char *req_meth) {
  uint8_t flags = 0;
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
  int ret = read_bucket_cors();
  if (ret < 0) {
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

  if (req_meth)
    method = req_meth;
  /* CORS 6.2.5. */
  if (!validate_cors_rule_method(rule, req_meth)) {
    return false;
  }

  /* CORS 6.2.4. */
  const char *req_hdrs = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_HEADERS");

  /* CORS 6.2.6. */
  get_cors_response_headers(rule, req_hdrs, headers, exp_headers, max_age);

  return true;
}

int RGWGetObj::read_user_manifest_part(rgw_bucket& bucket, RGWObjEnt& ent, RGWAccessControlPolicy *bucket_policy, off_t start_ofs, off_t end_ofs)
{
  ldout(s->cct, 0) << "user manifest obj=" << ent.name << dendl;

  void *handle = NULL;
  off_t cur_ofs = start_ofs;
  off_t cur_end = end_ofs;
  utime_t start_time = s->time;

  rgw_obj part(bucket, ent.name);

  map<string, bufferlist> attrs;

  uint64_t obj_size;
  void *obj_ctx = store->create_context(s);
  RGWAccessControlPolicy obj_policy(s->cct);

  ldout(s->cct, 20) << "reading obj=" << part << " ofs=" << cur_ofs << " end=" << cur_end << dendl;

  store->set_atomic(obj_ctx, part);
  store->set_prefetch_data(obj_ctx, part);
  ret = store->prepare_get_obj(obj_ctx, part, &cur_ofs, &cur_end, &attrs, NULL,
                                  NULL, NULL, NULL, NULL, NULL, &obj_size, NULL, &handle, &s->err);
  if (ret < 0)
    goto done_err;

  if (obj_size != ent.size) {
    // hmm.. something wrong, object not as expected, abort!
    ldout(s->cct, 0) << "ERROR: expected obj_size=" << obj_size << ", actual read size=" << ent.size << dendl;
    ret = -EIO;
    goto done_err;
  }

  ret = rgw_policy_from_attrset(s->cct, attrs, &obj_policy);
  if (ret < 0)
    goto done_err;

  if (!verify_object_permission(s, bucket_policy, &obj_policy, RGW_PERM_READ)) {
    ret = -EPERM;
    goto done_err;
  }

  perfcounter->inc(l_rgw_get_b, cur_end - cur_ofs);
  while (cur_ofs <= cur_end) {
    bufferlist bl;
    ret = store->get_obj(obj_ctx, NULL, &handle, part, bl, cur_ofs, cur_end, NULL);
    if (ret < 0)
      goto done_err;

    off_t len = bl.length();
    cur_ofs += len;
    ofs += len;
    ret = 0;
    perfcounter->tinc(l_rgw_get_lat,
                      (ceph_clock_now(s->cct) - start_time));
    send_response_data(bl, 0, len);

    start_time = ceph_clock_now(s->cct);
  }

  store->destroy_context(obj_ctx);
  obj_ctx = NULL;

  store->finish_get_obj(&handle);

  return 0;

done_err:
  if (obj_ctx)
    store->destroy_context(obj_ctx);
  return ret;
}

static int iterate_user_manifest_parts(CephContext *cct, RGWRados *store, off_t ofs, off_t end,
                                       rgw_bucket& bucket, string& obj_prefix, RGWAccessControlPolicy *bucket_policy,
                                       uint64_t *ptotal_len,
                                       int (*cb)(rgw_bucket& bucket, RGWObjEnt& ent, RGWAccessControlPolicy *bucket_policy,
                                                 off_t start_ofs, off_t end_ofs, void *param), void *cb_param)
{
  uint64_t obj_ofs = 0, len_count = 0;
  bool found_start = false, found_end = false;
  string delim;
  string marker;
  bool is_truncated;
  string no_ns;
  map<string, bool> common_prefixes;
  vector<RGWObjEnt> objs;

  utime_t start_time = ceph_clock_now(cct);

  do {
#define MAX_LIST_OBJS 100
    int r = store->list_objects(bucket, MAX_LIST_OBJS, obj_prefix, delim, marker, NULL,
                                objs, common_prefixes,
                                true, no_ns, true, &is_truncated, NULL);
    if (r < 0)
      return r;

    vector<RGWObjEnt>::iterator viter;

    for (viter = objs.begin(); viter != objs.end() && !found_end; ++viter) {
      RGWObjEnt& ent = *viter;
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
        len_count += end_ofs - start_ofs;

        if (cb) {
          r = cb(bucket, ent, bucket_policy, start_ofs, end_ofs, cb_param);
          if (r < 0)
            return r;
        }
      }
      marker = ent.name;

      start_time = ceph_clock_now(cct);
    }
  } while (is_truncated && !found_end);

  if (ptotal_len)
    *ptotal_len = len_count;

  return 0;
}

static int get_obj_user_manifest_iterate_cb(rgw_bucket& bucket, RGWObjEnt& ent, RGWAccessControlPolicy *bucket_policy, off_t start_ofs, off_t end_ofs,
                                       void *param)
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

  string bucket_name = prefix_str.substr(0, pos);
  string obj_prefix = prefix_str.substr(pos + 1);

  rgw_bucket bucket;

  RGWAccessControlPolicy _bucket_policy(s->cct);
  RGWAccessControlPolicy *bucket_policy;

  if (bucket_name.compare(s->bucket.name) != 0) {
    RGWBucketInfo bucket_info;
    map<string, bufferlist> bucket_attrs;
    int r = store->get_bucket_info(NULL, bucket_name, bucket_info, NULL, &bucket_attrs);
    if (r < 0) {
      ldout(s->cct, 0) << "could not get bucket info for bucket=" << bucket_name << dendl;
      return r;
    }
    bucket = bucket_info.bucket;
    string no_obj;
    bucket_policy = &_bucket_policy;
    r = read_policy(store, s, bucket_info, bucket_attrs, bucket_policy, bucket, no_obj);
    if (r < 0) {
      ldout(s->cct, 0) << "failed to read bucket policy" << dendl;
      return r;
    }
  } else {
    bucket = s->bucket;
    bucket_policy = s->bucket_acl;
  }

  /* dry run to find out total length */
  int r = iterate_user_manifest_parts(s->cct, store, ofs, end, bucket, obj_prefix, bucket_policy, &total_len, NULL, NULL);
  if (r < 0)
    return r;

  s->obj_size = total_len;

  r = iterate_user_manifest_parts(s->cct, store, ofs, end, bucket, obj_prefix, bucket_policy, NULL, get_obj_user_manifest_iterate_cb, (void *)this);
  if (r < 0)
    return r;

  return 0;
}

class RGWGetObj_CB : public RGWGetDataCB
{
  RGWGetObj *op;
public:
  RGWGetObj_CB(RGWGetObj *_op) : op(_op) {}
  virtual ~RGWGetObj_CB() {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
    return op->get_data_cb(bl, bl_ofs, bl_len);
  }
};

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

void RGWGetObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetObj::execute()
{
  void *handle = NULL;
  utime_t start_time = s->time;
  bufferlist bl;
  gc_invalidate_time = ceph_clock_now(s->cct);
  gc_invalidate_time += (s->cct->_conf->rgw_gc_obj_min_wait / 2);

  RGWGetObj_CB cb(this);

  map<string, bufferlist>::iterator attr_iter;

  perfcounter->inc(l_rgw_get);
  off_t new_ofs, new_end;

  ret = get_params();
  if (ret < 0)
    goto done_err;

  ret = init_common();
  if (ret < 0)
    goto done_err;

  new_ofs = ofs;
  new_end = end;

  ret = store->prepare_get_obj(s->obj_ctx, obj, &new_ofs, &new_end, &attrs, mod_ptr,
                               unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &s->obj_size, NULL, &handle, &s->err);
  if (ret < 0)
    goto done_err;

  attr_iter = attrs.find(RGW_ATTR_USER_MANIFEST);
  if (attr_iter != attrs.end()) {
    ret = handle_user_manifest(attr_iter->second.c_str());
    if (ret < 0) {
      ldout(s->cct, 0) << "ERROR: failed to handle user manifest ret=" << ret << dendl;
    }
    return;
  }

  ofs = new_ofs;
  end = new_end;

  start = ofs;

  if (!get_data || ofs > end)
    goto done_err;

  perfcounter->inc(l_rgw_get_b, end - ofs);

  ret = store->get_obj_iterate(s->obj_ctx, &handle, obj, ofs, end, &cb);

  perfcounter->tinc(l_rgw_get_lat,
                   (ceph_clock_now(s->cct) - start_time));
  if (ret < 0) {
    goto done_err;
  }

  store->finish_get_obj(&handle);

done_err:
  send_response_data(bl, 0, 0);
  store->finish_get_obj(&handle);
}

int RGWGetObj::init_common()
{
  if (range_str) {
    int r = parse_range(range_str, ofs, end, &partial_content);
    if (r < 0)
      return r;
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

void RGWListBuckets::execute()
{
  bool done;
  bool started = false;
  uint64_t total_count = 0;

  uint64_t max_buckets = s->cct->_conf->rgw_list_buckets_max_chunk;

  ret = get_params();
  if (ret < 0)
    goto send_end;

  do {
    RGWUserBuckets buckets;
    uint64_t read_count;
    if (limit > 0)
      read_count = min(limit - total_count, (uint64_t)max_buckets);
    else
      read_count = max_buckets;

    ret = rgw_read_user_buckets(store, s->user.user_id, buckets,
                                marker, read_count, should_get_stats());

    if (!started) {
      send_response_begin(buckets.count() > 0);
      started = true;
    }

    if (ret < 0) {
      /* hmm.. something wrong here.. the user was authenticated, so it
         should exist */
      ldout(s->cct, 10) << "WARNING: failed on rgw_get_user_buckets uid=" << s->user.user_id << dendl;
      break;
    }
    map<string, RGWBucketEnt>& m = buckets.get_buckets();

    total_count += m.size();

    done = (m.size() < read_count || (limit > 0 && total_count == limit));

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

int RGWStatAccount::verify_permission()
{
  return 0;
}

void RGWStatAccount::execute()
{
  string marker;
  bool done;
  uint64_t max_buckets = s->cct->_conf->rgw_list_buckets_max_chunk;

  do {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(store, s->user.user_id, buckets, marker, max_buckets, true);
    if (ret < 0) {
      /* hmm.. something wrong here.. the user was authenticated, so it
         should exist */
      ldout(s->cct, 10) << "WARNING: failed on rgw_get_user_buckets uid=" << s->user.user_id << dendl;
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
  RGWUserBuckets buckets;
  bucket.bucket = s->bucket;
  buckets.add(bucket);
  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  ret = store->update_containers_stats(m);
  if (!ret)
    ret = -EEXIST;
  if (ret > 0) {
    ret = 0;
    map<string, RGWBucketEnt>::iterator iter = m.find(bucket.bucket.name);
    if (iter != m.end()) {
      bucket = iter->second;
    } else {
      ret = -EINVAL;
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
  string no_ns;

  ret = get_params();
  if (ret < 0)
    return;

  string *pnext_marker = (delimiter.empty() ? NULL : &next_marker);

  ret = store->list_objects(s->bucket, max, prefix, delimiter, marker, pnext_marker, objs, common_prefixes,
                               !!(s->prot_flags & RGW_REST_SWIFT), no_ns, true, &is_truncated, NULL);
}

int RGWGetBucketLogging::verify_permission()
{
  if (s->user.user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

int RGWGetBucketLocation::verify_permission()
{
  if (s->user.user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

int RGWCreateBucket::verify_permission()
{
  if (!rgw_user_is_authenticated(s->user))
    return -EACCES;

  if (s->user.max_buckets) {
    RGWUserBuckets buckets;
    string marker;
    int ret = rgw_read_user_buckets(store, s->user.user_id, buckets, marker, s->user.max_buckets, false);
    if (ret < 0)
      return ret;

    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    if (m.size() >= s->user.max_buckets) {
      return -ERR_TOO_MANY_BUCKETS;
    }
  }

  return 0;
}

static int forward_request_to_master(struct req_state *s, obj_version *objv, RGWRados *store, bufferlist& in_data, JSONParser *jp)
{
  if (!store->rest_master_conn) {
    ldout(s->cct, 0) << "rest connection is invalid" << dendl;
    return -EINVAL;
  }
  ldout(s->cct, 0) << "sending create_bucket request to master region" << dendl;
  bufferlist response;
#define MAX_REST_RESPONSE (128 * 1024) // we expect a very small response
  int ret = store->rest_master_conn->forward(s->user.user_id, s->info, objv, MAX_REST_RESPONSE, &in_data, &response);
  if (ret < 0)
    return ret;

  ldout(s->cct, 20) << "response: " << response.c_str() << dendl;
  ret = jp->parse(response.c_str(), response.length());
  if (ret < 0) {
    ldout(s->cct, 0) << "failed parsing response from master region" << dendl;
    return ret;
  }

  return 0;
}

void RGWCreateBucket::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWCreateBucket::execute()
{
  RGWAccessControlPolicy old_policy(s->cct);
  map<string, bufferlist> attrs;
  bufferlist aclbl;
  bufferlist corsbl;
  bool existed;
  rgw_obj obj(store->zone.domain_root, s->bucket_name_str);
  obj_version objv, *pobjv = NULL;

  ret = get_params();
  if (ret < 0)
    return;

  if (!store->region.is_master &&
      store->region.api_name != location_constraint) {
    ldout(s->cct, 0) << "location constraint (" << location_constraint << ") doesn't match region" << " (" << store->region.api_name << ")" << dendl;
    ret = -EINVAL;
    return;
  }

  /* we need to make sure we read bucket info, it's not read before for this specific request */
  ret = store->get_bucket_info(s->obj_ctx, s->bucket_name_str, s->bucket_info, NULL, &s->bucket_attrs);
  if (ret < 0 && ret != -ENOENT)
    return;
  s->bucket_exists = (ret != -ENOENT);

  s->bucket_owner.set_id(s->user.user_id);
  s->bucket_owner.set_name(s->user.display_name);
  if (s->bucket_exists) {
    int r = get_policy_from_attr(s->cct, store, s->obj_ctx, s->bucket_info, s->bucket_attrs,
                                 &old_policy, obj);
    if (r >= 0)  {
      if (old_policy.get_owner().get_id().compare(s->user.user_id) != 0) {
        ret = -EEXIST;
        return;
      }
    }
  }

  RGWBucketInfo master_info;
  rgw_bucket *pmaster_bucket;
  time_t creation_time;

  if (!store->region.is_master) {
    JSONParser jp;
    ret = forward_request_to_master(s, NULL, store, in_data, &jp);
    if (ret < 0)
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
    creation_time = 0;
  }

  string region_name;

  if (s->system_request) {
    region_name = s->info.args.get(RGW_SYS_PARAM_PREFIX "region");
    if (region_name.empty()) {
      region_name = store->region.name;
    }
  } else {
    region_name = store->region.name;
  }

  policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;

  if (has_cors) {
    cors_config.encode(corsbl);
    attrs[RGW_ATTR_CORS] = corsbl;
  }
  s->bucket.name = s->bucket_name_str;
  ret = store->create_bucket(s->user, s->bucket, region_name, placement_rule, attrs, info, pobjv,
                             &ep_objv, creation_time, pmaster_bucket, true);
  /* continue if EEXIST and create_bucket will fail below.  this way we can recover
   * from a partial create by retrying it. */
  ldout(s->cct, 20) << "rgw_create_bucket returned ret=" << ret << " bucket=" << s->bucket << dendl;

  if (ret && ret != -EEXIST)
    return;

  existed = (ret == -EEXIST);

  if (existed) {
    /* bucket already existed, might have raced with another bucket creation, or
     * might be partial bucket creation that never completed. Read existing bucket
     * info, verify that the reported bucket owner is the current user.
     * If all is ok then update the user's list of buckets
     */
    if (info.owner.compare(s->user.user_id) != 0) {
      ret = -ERR_BUCKET_EXISTS;
      return;
    }
    s->bucket = info.bucket;
  }

  ret = rgw_link_bucket(store, s->user.user_id, s->bucket, info.creation_time, false);
  if (ret && !existed && ret != -EEXIST) {  /* if it exists (or previously existed), don't remove it! */
    ret = rgw_unlink_bucket(store, s->user.user_id, s->bucket.name);
    if (ret < 0) {
      ldout(s->cct, 0) << "WARNING: failed to unlink bucket: ret=" << ret << dendl;
    }
  }

  if (ret == -EEXIST)
    ret = -ERR_BUCKET_EXISTS;
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
  ret = -EINVAL;

  if (s->bucket_name_str.empty())
    return;

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
        ret = -EINVAL;
        return;
      }
      ot.read_version.ver = ver;
    }
  }

  ret = store->delete_bucket(s->bucket, ot);

  if (ret == 0) {
    ret = rgw_unlink_bucket(store, s->user.user_id, s->bucket.name, false);
    if (ret < 0) {
      ldout(s->cct, 0) << "WARNING: failed to unlink bucket: ret=" << ret << dendl;
    }
  }

  if (ret < 0) {
    return;
  }

  if (!store->region.is_master) {
    bufferlist in_data;
    JSONParser jp;
    ret = forward_request_to_master(s, &ot.read_version, store, in_data, &jp);
    if (ret < 0) {
      if (ret == -ENOENT) { /* adjust error,
                               we want to return with NoSuchBucket and not NoSuchKey */
        ret = -ERR_NO_SUCH_BUCKET;
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
  int prepare(RGWRados *store, void *obj_ctx, string *oid_rand);
  int do_complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs);

public:
  bool immutable_head() { return true; }
  RGWPutObjProcessor_Multipart(const string& bucket_owner, uint64_t _p, req_state *_s) :
                   RGWPutObjProcessor_Atomic(bucket_owner, _s->bucket, _s->object_str, _p, _s->req_id), s(_s) {}
};

int RGWPutObjProcessor_Multipart::prepare(RGWRados *store, void *obj_ctx, string *oid_rand)
{
  int r = prepare_init(store, obj_ctx, NULL);
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
  cur_obj = head_obj;
  add_obj(cur_obj);

  return 0;
}

static bool is_v2_upload_id(const string& upload_id)
{
  const char *uid = upload_id.c_str();

  return (strncmp(uid, MULTIPART_UPLOAD_ID_PREFIX, sizeof(MULTIPART_UPLOAD_ID_PREFIX) - 1) == 0);
}

int RGWPutObjProcessor_Multipart::do_complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs)
{
  complete_writing_data();

  RGWRados::PutObjMetaExtraParams params;
  params.set_mtime = set_mtime;
  params.mtime = mtime;
  params.owner = s->owner.get_id();

  int r = store->put_obj_meta(obj_ctx, head_obj, s->obj_size, attrs, RGW_OBJ_CATEGORY_MAIN, 0, params);
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
  info.modified = ceph_clock_now(store->ctx());
  info.manifest = manifest;
  ::encode(info, bl);

  string multipart_meta_obj = mp.get_meta();

  rgw_obj meta_obj;
  meta_obj.init_ns(bucket, multipart_meta_obj, mp_ns);
  meta_obj.set_in_extra_data(true);

  r = store->omap_set(meta_obj, p, bl);

  return r;
}


RGWPutObjProcessor *RGWPutObj::select_processor(bool *is_multipart)
{
  RGWPutObjProcessor *processor;

  bool multipart = s->info.args.exists("uploadId");

  uint64_t part_size = s->cct->_conf->rgw_obj_stripe_size;

  const string& bucket_owner = s->bucket_owner.get_id();

  if (!multipart) {
    processor = new RGWPutObjProcessor_Atomic(bucket_owner, s->bucket, s->object_str, part_size, s->req_id);
  } else {
    processor = new RGWPutObjProcessor_Multipart(bucket_owner, part_size, s);
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

static int put_data_and_throttle(RGWPutObjProcessor *processor, bufferlist& data, off_t ofs,
                                 MD5 *hash, bool need_to_wait)
{
  bool again;

  do {
    void *handle;

    int ret = processor->handle_data(data, ofs, hash, &handle, &again);
    if (ret < 0)
      return ret;

    ret = processor->throttle_data(handle, need_to_wait);
    if (ret < 0)
      return ret;

    need_to_wait = false; /* the need to wait only applies to the first iteration */
  } while (again);

  return 0;
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

  bool need_calc_md5 = (obj_manifest == NULL);


  perfcounter->inc(l_rgw_put);
  ret = -EINVAL;
  if (!s->object) {
    goto done;
  }

  ret = get_params();
  if (ret < 0)
    goto done;

  if (supplied_md5_b64) {
    need_calc_md5 = true;

    ldout(s->cct, 15) << "supplied_md5_b64=" << supplied_md5_b64 << dendl;
    ret = ceph_unarmor(supplied_md5_bin, &supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1],
                       supplied_md5_b64, supplied_md5_b64 + strlen(supplied_md5_b64));
    ldout(s->cct, 15) << "ceph_armor ret=" << ret << dendl;
    if (ret != CEPH_CRYPTO_MD5_DIGESTSIZE) {
      ret = -ERR_INVALID_DIGEST;
      goto done;
    }

    buf_to_hex((const unsigned char *)supplied_md5_bin, CEPH_CRYPTO_MD5_DIGESTSIZE, supplied_md5);
    ldout(s->cct, 15) << "supplied_md5=" << supplied_md5 << dendl;
  }

  if (!chunked_upload) { /* with chunked upload we don't know how big is the upload.
                            we also check sizes at the end anyway */
    ret = store->check_quota(s->bucket_owner.get_id(), s->bucket,
                             user_quota, bucket_quota, s->content_length);
    if (ret < 0) {
      goto done;
    }
  }

  if (supplied_etag) {
    strncpy(supplied_md5, supplied_etag, sizeof(supplied_md5) - 1);
    supplied_md5[sizeof(supplied_md5) - 1] = '\0';
  }

  processor = select_processor(&multipart);

  ret = processor->prepare(store, s->obj_ctx, NULL);
  if (ret < 0)
    goto done;

  do {
    bufferlist data;
    len = get_data(data);
    if (len < 0) {
      ret = len;
      goto done;
    }
    if (!len)
      break;

    /* do we need this operation to be synchronous? if we're dealing with an object with immutable
     * head, e.g., multipart object we need to make sure we're the first one writing to this object
     */
    bool need_to_wait = (ofs == 0) && multipart;

    ret = put_data_and_throttle(processor, data, ofs, (need_calc_md5 ? &hash : NULL), need_to_wait);
    if (ret < 0) {
      if (!need_to_wait || ret != -EEXIST) {
        ldout(s->cct, 20) << "processor->thottle_data() returned ret=" << ret << dendl;
        goto done;
      }

      ldout(s->cct, 5) << "NOTICE: processor->throttle_data() returned -EEXIST, need to restart write" << dendl;

      /* restart processing with different oid suffix */

      dispose_processor(processor);
      processor = select_processor(&multipart);

      string oid_rand;
      char buf[33];
      gen_rand_alphanumeric(store->ctx(), buf, sizeof(buf) - 1);
      oid_rand.append(buf);

      ret = processor->prepare(store, s->obj_ctx, &oid_rand);
      if (ret < 0) {
        ldout(s->cct, 0) << "ERROR: processor->prepare() returned " << ret << dendl;
        goto done;
      }

      ret = put_data_and_throttle(processor, data, ofs, NULL, false);
      if (ret < 0) {
        goto done;
      }
    }

    ofs += len;
  } while (len > 0);

  if (!chunked_upload && ofs != s->content_length) {
    ret = -ERR_REQUEST_TIMEOUT;
    goto done;
  }
  s->obj_size = ofs;
  perfcounter->inc(l_rgw_put_b, s->obj_size);

  ret = store->check_quota(s->bucket_owner.get_id(), s->bucket,
                           user_quota, bucket_quota, s->obj_size);
  if (ret < 0) {
    goto done;
  }

  if (need_calc_md5) {
    processor->complete_hash(&hash);
    hash.Final(m);

    buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);
    etag = calc_md5;

    if (supplied_md5_b64 && strcmp(calc_md5, supplied_md5)) {
      ret = -ERR_BAD_DIGEST;
      goto done;
    }
  }

  policy.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;
  if (obj_manifest) {
    bufferlist manifest_bl;
    string manifest_obj_prefix;
    string manifest_bucket;

    char etag_buf[CEPH_CRYPTO_MD5_DIGESTSIZE];
    char etag_buf_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];

    manifest_bl.append(obj_manifest, strlen(obj_manifest) + 1);
    attrs[RGW_ATTR_USER_MANIFEST] = manifest_bl;
    user_manifest_parts_hash = &hash;
    string prefix_str = obj_manifest;
    int pos = prefix_str.find('/');
    if (pos < 0) {
      ldout(s->cct, 0) << "bad user manifest, missing slash separator: " << obj_manifest << dendl;
      goto done;
    }

    manifest_bucket = prefix_str.substr(0, pos);
    manifest_obj_prefix = prefix_str.substr(pos + 1);

    hash.Final((byte *)etag_buf);
    buf_to_hex((const unsigned char *)etag_buf, CEPH_CRYPTO_MD5_DIGESTSIZE, etag_buf_str);

    ldout(s->cct, 0) << __func__ << ": calculated md5 for user manifest: " << etag_buf_str << dendl;

    etag = etag_buf_str;
  }
  if (supplied_etag && etag.compare(supplied_etag) != 0) {
    ret = -ERR_UNPROCESSABLE_ENTITY;
    goto done;
  }
  bl.append(etag.c_str(), etag.size() + 1);
  attrs[RGW_ATTR_ETAG] = bl;

  for (iter = s->generic_attrs.begin(); iter != s->generic_attrs.end(); ++iter) {
    bufferlist& attrbl = attrs[iter->first];
    const string& val = iter->second;
    attrbl.append(val.c_str(), val.size() + 1);
  }

  rgw_get_request_metadata(s->cct, s->info, attrs);

  ret = processor->complete(etag, &mtime, 0, attrs);
done:
  dispose_processor(processor);
  perfcounter->tinc(l_rgw_put_lat,
                   (ceph_clock_now(s->cct) - s->time));
}

int RGWPostObj::verify_permission()
{
  return 0;
}

RGWPutObjProcessor *RGWPostObj::select_processor()
{
  RGWPutObjProcessor *processor;

  uint64_t part_size = s->cct->_conf->rgw_obj_stripe_size;

  processor = new RGWPutObjProcessor_Atomic(s->bucket_owner.get_id(), s->bucket, s->object_str, part_size, s->req_id);

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
  ret = get_params();
  if (ret < 0)
    goto done;

  ret = verify_params();
  if (ret < 0)
    goto done;

  if (!verify_bucket_permission(s, RGW_PERM_WRITE)) {
    ret = -EACCES;
    goto done;
  }

  processor = select_processor();

  ret = processor->prepare(store, s->obj_ctx, NULL);
  if (ret < 0)
    goto done;

  while (data_pending) {
     bufferlist data;
     len = get_data(data);

     if (len < 0) {
       ret = len;
       goto done;
     }

     if (!len)
       break;

     ret = put_data_and_throttle(processor, data, ofs, &hash, false);

     ofs += len;

     if (ofs > max_len) {
       ret = -ERR_TOO_LARGE;
       goto done;
     }
   }

  if (len < min_len) {
    ret = -ERR_TOO_SMALL;
    goto done;
  }

  s->obj_size = ofs;

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

  ret = processor->complete(etag, NULL, 0, attrs);

done:
  dispose_processor(processor);
}


int RGWPutMetadata::verify_permission()
{
  if (s->object) {
    if (!verify_object_permission(s, RGW_PERM_WRITE))
      return -EACCES;
  } else {
    if (!verify_bucket_permission(s, RGW_PERM_WRITE))
      return -EACCES;
  }

  return 0;
}

void RGWPutMetadata::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPutMetadata::execute()
{
  const char *meta_prefix = RGW_ATTR_META_PREFIX;
  int meta_prefix_len = sizeof(RGW_ATTR_META_PREFIX) - 1;
  map<string, bufferlist> attrs, orig_attrs, rmattrs;
  map<string, bufferlist>::iterator iter;
  bufferlist bl, cors_bl;

  rgw_obj obj(s->bucket, s->object_str);

  store->set_atomic(s->obj_ctx, obj);

  ret = get_params();
  if (ret < 0)
    return;

  rgw_get_request_metadata(s->cct, s->info, attrs);

  /* no need to track object versioning, need it for bucket's data only */
  RGWObjVersionTracker *ptracker = (s->object ? NULL : &s->bucket_info.objv_tracker);

  /* check if obj exists, read orig attrs */
  ret = get_obj_attrs(store, s, obj, orig_attrs, NULL, ptracker);
  if (ret < 0)
    return;

  /* only remove meta attrs */
  for (iter = orig_attrs.begin(); iter != orig_attrs.end(); ++iter) {
    const string& name = iter->first;
    if (name.compare(0, meta_prefix_len, meta_prefix) == 0) {
      rmattrs[name] = iter->second;
    } else if (attrs.find(name) == attrs.end()) {
      attrs[name] = iter->second;
    }
  }

  map<string, string>::iterator giter;
  for (giter = s->generic_attrs.begin(); giter != s->generic_attrs.end(); ++giter) {
    bufferlist& attrbl = attrs[giter->first];
    const string& val = giter->second;
    attrbl.clear();
    attrbl.append(val.c_str(), val.size() + 1);
  }

  if (has_policy) {
    policy.encode(bl);
    attrs[RGW_ATTR_ACL] = bl;
  }
  if (has_cors) {
    cors_config.encode(cors_bl);
    attrs[RGW_ATTR_CORS] = cors_bl;
  }
  if (s->object) {
    ret = store->set_attrs(s->obj_ctx, obj, attrs, &rmattrs, ptracker);
  } else {
    ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs, &rmattrs, ptracker);
  }
}

int RGWSetTempUrl::verify_permission()
{
  if (s->perm_mask != RGW_PERM_FULL_CONTROL)
    return -EACCES;

  return 0;
}

void RGWSetTempUrl::execute()
{
  ret = get_params();
  if (ret < 0)
    return;

  RGWUserAdminOpState user_op;
  user_op.set_user_id(s->user.user_id);
  map<int, string>::iterator iter;
  for (iter = temp_url_keys.begin(); iter != temp_url_keys.end(); ++iter) {
    user_op.set_temp_url_key(iter->second, iter->first);
  }

  RGWUser user;
  ret = user.init(store, user_op);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: could not init user ret=" << ret << dendl;
    return;
  }
  string err_msg;
  ret = user.modify(user_op, &err_msg);
  if (ret < 0) {
    ldout(store->ctx(), 10) << "user.modify() returned " << ret << ": " << err_msg << dendl;
    return;
  }
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
  ret = -EINVAL;
  rgw_obj obj(s->bucket, s->object_str);
  if (s->object) {
    store->set_atomic(s->obj_ctx, obj);
    ret = store->delete_obj(s->obj_ctx, s->bucket_owner.get_id(), obj);
  }
}

bool RGWCopyObj::parse_copy_location(const char *src, string& bucket_name, string& object)
{
  string url_src(src);
  string dec_src;

  url_decode(url_src, dec_src);
  src = dec_src.c_str();

  if (*src == '/') ++src;

  string str(src);

  int pos = str.find("/");
  if (pos <= 0)
    return false;

  bucket_name = str.substr(0, pos);
  object = str.substr(pos + 1);

  if (object.size() == 0)
    return false;

  return true;
}

int RGWCopyObj::verify_permission()
{
  string empty_str;
  RGWAccessControlPolicy src_policy(s->cct);
  ret = get_params();
  if (ret < 0)
    return ret;

  map<string, bufferlist> src_attrs;

  ret = store->get_bucket_info(s->obj_ctx, src_bucket_name, src_bucket_info, NULL, &src_attrs);
  if (ret < 0)
    return ret;

  src_bucket = src_bucket_info.bucket;

  /* get buckets info (source and dest) */
  if (s->local_source &&  source_zone.empty()) {
    rgw_obj src_obj(src_bucket, src_object);
    store->set_atomic(s->obj_ctx, src_obj);
    store->set_prefetch_data(s->obj_ctx, src_obj);

    /* check source object permissions */
    ret = read_policy(store, s, src_bucket_info, src_attrs, &src_policy, src_bucket, src_object);
    if (ret < 0)
      return ret;

    if (!s->system_request && /* system request overrides permission checks */
        !src_policy.verify_permission(s->user.user_id, s->perm_mask, RGW_PERM_READ))
      return -EACCES;
  }

  RGWAccessControlPolicy dest_bucket_policy(s->cct);
  map<string, bufferlist> dest_attrs;

  if (src_bucket_name.compare(dest_bucket_name) == 0) { /* will only happen if s->local_source */
    dest_bucket_info = src_bucket_info;
  } else {
    ret = store->get_bucket_info(s->obj_ctx, dest_bucket_name, dest_bucket_info, NULL, &dest_attrs);
    if (ret < 0)
      return ret;
  }

  dest_bucket = dest_bucket_info.bucket;

  rgw_obj dest_obj(dest_bucket, dest_object);
  store->set_atomic(s->obj_ctx, dest_obj);

  /* check dest bucket permissions */
  ret = read_policy(store, s, dest_bucket_info, dest_attrs, &dest_bucket_policy, dest_bucket, empty_str);
  if (ret < 0)
    return ret;

  if (!s->system_request && /* system request overrides permission checks */
      !dest_bucket_policy.verify_permission(s->user.user_id, s->perm_mask, RGW_PERM_WRITE))
    return -EACCES;

  ret = init_dest_policy();
  if (ret < 0)
    return ret;

  return 0;
}


int RGWCopyObj::init_common()
{
  if (if_mod) {
    if (parse_time(if_mod, &mod_time) < 0) {
      ret = -EINVAL;
      return ret;
    }
    mod_ptr = &mod_time;
  }

  if (if_unmod) {
    if (parse_time(if_unmod, &unmod_time) < 0) {
      ret = -EINVAL;
      return ret;
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
  rgw_obj src_obj, dst_obj;

  if (init_common() < 0)
    return;

  src_obj.init(src_bucket, src_object);
  dst_obj.init(dest_bucket, dest_object);
  store->set_atomic(s->obj_ctx, src_obj);

  store->set_atomic(s->obj_ctx, dst_obj);

  ret = store->copy_obj(s->obj_ctx,
                        s->user.user_id,
                        client_id,
                        op_id,
                        &s->info,
                        source_zone,
                        dst_obj,
                        src_obj,
                        dest_bucket_info,
                        src_bucket_info,
                        &mtime,
                        mod_ptr,
                        unmod_ptr,
                        if_match,
                        if_nomatch,
                        replace_attrs,
                        attrs, RGW_OBJ_CATEGORY_MAIN,
                        &s->req_id, /* use req_id as tag */
                        &etag,
                        &s->err,
                        copy_obj_progress_cb, (void *)this
                        );
}

int RGWGetACLs::verify_permission()
{
  bool perm;
  if (s->object) {
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
  RGWAccessControlPolicy *acl = (s->object ? s->object_acl : s->bucket_acl);
  RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(acl);
  s3policy->to_xml(ss);
  acls = ss.str();
}



int RGWPutACLs::verify_permission()
{
  bool perm;
  if (s->object) {
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
  ACLOwner owner;
  rgw_obj obj;

  ret = 0;

  if (!parser.init()) {
    ret = -EINVAL;
    return;
  }

  owner.set_id(s->user.user_id);
  owner.set_name(s->user.display_name);

  ret = get_params();
  if (ret < 0)
    return;

  ldout(s->cct, 15) << "read len=" << len << " data=" << (data ? data : "") << dendl;

  if (!s->canned_acl.empty() && len) {
    ret = -EINVAL;
    return;
  }

  if (!s->canned_acl.empty() || s->has_acl_header) {
    ret = get_policy_from_state(store, s, ss);
    if (ret < 0)
      return;

    new_data = strdup(ss.str().c_str());
    free(data);
    data = new_data;
    len = ss.str().size();
  }

  if (!parser.parse(data, len, 1)) {
    ret = -EACCES;
    return;
  }
  policy = static_cast<RGWAccessControlPolicy_S3 *>(parser.find_first("AccessControlPolicy"));
  if (!policy) {
    ret = -EINVAL;
    return;
  }

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    ldout(s->cct, 15) << "Old AccessControlPolicy";
    policy->to_xml(*_dout);
    *_dout << dendl;
  }

  ret = policy->rebuild(store, &owner, new_policy);
  if (ret < 0)
    return;

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    ldout(s->cct, 15) << "New AccessControlPolicy:";
    new_policy.to_xml(*_dout);
    *_dout << dendl;
  }

  RGWObjVersionTracker *ptracker = (s->object ? NULL : &s->bucket_info.objv_tracker);

  new_policy.encode(bl);
  obj.init(s->bucket, s->object_str);
  map<string, bufferlist> attrs;
  attrs[RGW_ATTR_ACL] = bl;
  store->set_atomic(s->obj_ctx, obj);
  if (s->object) {
    ret = store->set_attrs(s->obj_ctx, obj, attrs, NULL, ptracker);
  } else {
    ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs, NULL, ptracker);
  }
}

int RGWGetCORS::verify_permission()
{
  if (s->user.user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWGetCORS::execute()
{
  ret = read_bucket_cors();
  if (ret < 0)
    return ;

  if (!cors_exist) {
    dout(2) << "No CORS configuration set yet for this bucket" << dendl;
    ret = -ENOENT;
    return;
  }
}

int RGWPutCORS::verify_permission()
{
  if (s->user.user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWPutCORS::execute()
{
  rgw_obj obj;

  ret = get_params();
  if (ret < 0)
    return;

  RGWObjVersionTracker *ptracker = (s->object ? NULL : &s->bucket_info.objv_tracker);

  store->get_bucket_instance_obj(s->bucket, obj);
  store->set_atomic(s->obj_ctx, obj);
  ret = store->set_attr(s->obj_ctx, obj, RGW_ATTR_CORS, cors_bl, ptracker);
}

int RGWDeleteCORS::verify_permission()
{
  if (s->user.user_id.compare(s->bucket_owner.get_id()) != 0)
    return -EACCES;

  return 0;
}

void RGWDeleteCORS::execute()
{
  ret = read_bucket_cors();
  if (ret < 0)
    return;

  bufferlist bl;
  rgw_obj obj;
  if (!cors_exist) {
    dout(2) << "No CORS configuration set yet for this bucket" << dendl;
    ret = -ENOENT;
    return;
  }
  store->get_bucket_instance_obj(s->bucket, obj);
  store->set_atomic(s->obj_ctx, obj);
  map<string, bufferlist> orig_attrs, attrs, rmattrs;
  map<string, bufferlist>::iterator iter;

  RGWObjVersionTracker *ptracker = (s->object ? NULL : &s->bucket_info.objv_tracker);

  /* check if obj exists, read orig attrs */
  ret = get_obj_attrs(store, s, obj, orig_attrs, NULL, ptracker);
  if (ret < 0)
    return;

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
  ret = store->set_attrs(s->obj_ctx, obj, attrs, &rmattrs, ptracker);
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
  ret = read_bucket_cors();
  if (ret < 0)
    return;

  origin = s->info.env->get("HTTP_ORIGIN");
  if (!origin) {
    dout(0) <<
    "Preflight request without mandatory Origin header"
    << dendl;
    ret = -EINVAL;
    return;
  }
  req_meth = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_METHOD");
  if (!req_meth) {
    dout(0) <<
    "Preflight request without mandatory Access-control-request-method header"
    << dendl;
    ret = -EINVAL;
    return;
  }
  if (!cors_exist) {
    dout(2) << "No CORS configuration set yet for this bucket" << dendl;
    ret = -ENOENT;
    return;
  }
  req_hdrs = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_HEADERS");
  ret = validate_cors_request(&bucket_cors);
  if (!rule) {
    origin = req_meth = NULL;
    return;
  }
  return;
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
  ret = -EINVAL;
  if (!s->object)
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
    upload_id = "2/"; /* v2 upload id */
    upload_id.append(buf);

    string tmp_obj_name;
    RGWMPObj mp(s->object_str, upload_id);
    tmp_obj_name = mp.get_meta();

    obj.init_ns(s->bucket, tmp_obj_name, mp_ns);
    // the meta object will be indexed with 0 size, we c
    obj.set_in_extra_data(true);
    ret = store->put_obj_meta(s->obj_ctx, obj, 0, NULL, attrs, RGW_OBJ_CATEGORY_MULTIMETA, PUT_OBJ_CREATE_EXCL, s->owner.get_id());
  } while (ret == -EEXIST);
}

static int get_multipart_info(RGWRados *store, struct req_state *s, string& meta_oid,
                              RGWAccessControlPolicy *policy, map<string, bufferlist>& attrs)
{
  map<string, bufferlist>::iterator iter;
  bufferlist header;

  rgw_obj obj;
  obj.init_ns(s->bucket, meta_oid, mp_ns);
  obj.set_in_extra_data(true);

  int ret = get_obj_attrs(store, s, obj, attrs, NULL, NULL);
  if (ret < 0)
    return ret;

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

  ret = get_params();
  if (ret < 0)
    return;

  if (!data) {
    ret = -EINVAL;
    return;
  }

  if (!parser.init()) {
    ret = -EINVAL;
    return;
  }

  if (!parser.parse(data, len, 1)) {
    ret = -EINVAL;
    return;
  }

  parts = static_cast<RGWMultiCompleteUpload *>(parser.find_first("CompleteMultipartUpload"));
  if (!parts) {
    ret = -EINVAL;
    return;
  }

  mp.init(s->object_str, upload_id);
  meta_oid = mp.get_meta();

  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  bool truncated;

  uint64_t min_part_size = s->cct->_conf->rgw_multipart_min_part_size;

  list<string> remove_objs; /* objects to be removed from index listing */

  iter = parts->parts.begin();

  meta_obj.init_ns(s->bucket, meta_oid, mp_ns);
  meta_obj.set_in_extra_data(true);

  ret = get_obj_attrs(store, s, meta_obj, attrs, NULL, NULL);
  if (ret < 0) {
    ldout(s->cct, 0) << "ERROR: failed to get obj attrs, obj=" << meta_obj << " ret=" << ret << dendl;
    return;
  }

  do {
    ret = list_multipart_parts(store, s, upload_id, meta_oid, max_parts, marker, obj_parts, &marker, &truncated);
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    if (ret < 0)
      return;

    total_parts += obj_parts.size();
    if (!truncated && total_parts != (int)parts->parts.size()) {
      ret = -ERR_INVALID_PART;
      return;
    }

    for (obj_iter = obj_parts.begin(); iter != parts->parts.end() && obj_iter != obj_parts.end(); ++iter, ++obj_iter, ++handled_parts) {
      uint64_t part_size = obj_iter->second.size;
      if (handled_parts < (int)parts->parts.size() - 1 &&
          part_size < min_part_size) {
        ret = -ERR_TOO_SMALL;
        return;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (iter->first != (int)obj_iter->first) {
        ldout(s->cct, 0) << "NOTICE: parts num mismatch: next requested: " << iter->first << " next uploaded: " << obj_iter->first << dendl;
        ret = -ERR_INVALID_PART;
        return;
      }
      string part_etag = rgw_string_unquote(iter->second);
      if (part_etag.compare(obj_iter->second.etag) != 0) {
        ldout(s->cct, 0) << "NOTICE: etag mismatch: part: " << iter->first << " etag: " << iter->second << dendl;
        ret = -ERR_INVALID_PART;
        return;
      }

      hex_to_buf(obj_iter->second.etag.c_str(), petag, CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const byte *)petag, sizeof(petag));

      RGWUploadPartInfo& obj_part = obj_iter->second;

      /* update manifest for part */
      string oid = mp.get_part(obj_iter->second.num);
      rgw_obj src_obj;
      src_obj.init_ns(s->bucket, oid, mp_ns);

      if (obj_part.manifest.empty()) {
        ldout(s->cct, 0) << "ERROR: empty manifest for object part: obj=" << src_obj << dendl;
        ret = -ERR_INVALID_PART;
        return;
      } else {
        manifest.append(obj_part.manifest);
      }

      remove_objs.push_back(src_obj.object);

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

  target_obj.init(s->bucket, s->object_str);

  store->set_atomic(s->obj_ctx, target_obj);

  RGWRados::PutObjMetaExtraParams extra_params;

  extra_params.manifest = &manifest;
  extra_params.remove_objs = &remove_objs;

  extra_params.ptag = &s->req_id; /* use req_id as operation tag */
  extra_params.owner = s->owner.get_id();

  ret = store->put_obj_meta(s->obj_ctx, target_obj, ofs, attrs,
                            RGW_OBJ_CATEGORY_MAIN, PUT_OBJ_CREATE,
			    extra_params);
  if (ret < 0)
    return;

  // remove the upload obj
  store->delete_obj(s->obj_ctx, s->bucket_owner.get_id(), meta_obj);
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
  ret = -EINVAL;
  string upload_id;
  string meta_oid;
  upload_id = s->info.args.get("uploadId");
  map<uint32_t, RGWUploadPartInfo> obj_parts;
  map<uint32_t, RGWUploadPartInfo>::iterator obj_iter;
  map<string, bufferlist> attrs;
  rgw_obj meta_obj;
  RGWMPObj mp;
  const string& owner = s->bucket_owner.get_id();

  if (upload_id.empty() || s->object_str.empty())
    return;

  mp.init(s->object_str, upload_id);
  meta_oid = mp.get_meta();

  ret = get_multipart_info(store, s, meta_oid, NULL, attrs);
  if (ret < 0)
    return;

  bool truncated;
  int marker = 0;
  int max_parts = 1000;

  do {
    ret = list_multipart_parts(store, s, upload_id, meta_oid, max_parts, marker, obj_parts, &marker, &truncated);
    if (ret < 0)
      return;

    for (obj_iter = obj_parts.begin(); obj_iter != obj_parts.end(); ++obj_iter) {
      RGWUploadPartInfo& obj_part = obj_iter->second;

      if (obj_part.manifest.empty()) {
        string oid = mp.get_part(obj_iter->second.num);
        rgw_obj obj;
        obj.init_ns(s->bucket, oid, mp_ns);
        ret = store->delete_obj(s->obj_ctx, owner, obj);
        if (ret < 0 && ret != -ENOENT)
          return;
      } else {
        RGWObjManifest& manifest = obj_part.manifest;
        RGWObjManifest::obj_iterator oiter;
        for (oiter = manifest.obj_begin(); oiter != manifest.obj_end(); ++oiter) {
          rgw_obj loc = oiter.get_location();
          ret = store->delete_obj(s->obj_ctx, owner, loc);
          if (ret < 0 && ret != -ENOENT)
            return;
        }
      }
    }
  } while (truncated);

  // and also remove the metadata obj
  meta_obj.init_ns(s->bucket, meta_oid, mp_ns);
  meta_obj.set_in_extra_data(true);
  ret = store->delete_obj(s->obj_ctx, owner, meta_obj);
  if (ret == -ENOENT) {
    ret = -ERR_NO_SUCH_BUCKET;
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

  ret = get_params();
  if (ret < 0)
    return;

  mp.init(s->object_str, upload_id);
  meta_oid = mp.get_meta();

  ret = get_multipart_info(store, s, meta_oid, &policy, xattrs);
  if (ret < 0)
    return;

  ret = list_multipart_parts(store, s, upload_id, meta_oid, max_parts, marker, parts, NULL, &truncated);
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

  ret = get_params();
  if (ret < 0)
    return;

  if (s->prot_flags & RGW_REST_SWIFT) {
    string path_args;
    path_args = s->info.args.get("path");
    if (!path_args.empty()) {
      if (!delimiter.empty() || !prefix.empty()) {
        ret = -EINVAL;
        return;
      }
      prefix = path_args;
      delimiter="/";
    }
  }
  marker_meta = marker.get_meta();
  ret = store->list_objects(s->bucket, max_uploads, prefix, delimiter, marker_meta, NULL, objs, common_prefixes,
                               !!(s->prot_flags & RGW_REST_SWIFT), mp_ns, true, &is_truncated, &mp_filter);
  if (!objs.empty()) {
    vector<RGWObjEnt>::iterator iter;
    RGWMultipartUploadEntry entry;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      string name = iter->name;
      if (!entry.mp.from_meta(name))
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
  vector<string>::iterator iter;
  RGWMultiDelXMLParser parser;
  pair<string,int> result;
  int num_processed = 0;

  ret = get_params();
  if (ret < 0) {
    goto error;
  }

  if (!data) {
    ret = -EINVAL;
    goto error;
  }

  if (!parser.init()) {
    ret = -EINVAL;
    goto error;
  }

  if (!parser.parse(data, len, 1)) {
    ret = -EINVAL;
    goto error;
  }

  multi_delete = static_cast<RGWMultiDelDelete *>(parser.find_first("Delete"));
  if (!multi_delete) {
    ret = -EINVAL;
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

    rgw_obj obj(bucket,(*iter));
    store->set_atomic(s->obj_ctx, obj);
    ret = store->delete_obj(s->obj_ctx, s->bucket_owner.get_id(), obj);
    if (ret == -ENOENT) {
      ret = 0;
    }
    result = make_pair(*iter, ret);

    send_partial_response(result);
  }

  /*  set the return code to zero, errors at this point will be
  dumped to the response */
  ret = 0;

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

RGWHandler::~RGWHandler()
{
}

int RGWHandler::init(RGWRados *_store, struct req_state *_s, RGWClientIO *cio)
{
  store = _store;
  s = _s;

  return 0;
}

int RGWHandler::do_read_permissions(RGWOp *op, bool only_bucket)
{
  int ret = rgw_build_policies(store, s, only_bucket, op->prefetch_data());

  if (ret < 0) {
    ldout(s->cct, 10) << "read_permissions on " << s->bucket << ":" <<s->object_str << " only_bucket=" << only_bucket << " ret=" << ret << dendl;
    if (ret == -ENODATA)
      ret = -EACCES;
  }

  return ret;
}


RGWOp *RGWHandler::get_op(RGWRados *store)
{
  RGWOp *op;
  switch (s->op) {
   case OP_GET:
     op = op_get();
     break;
   case OP_PUT:
     op = op_put();
     break;
   case OP_DELETE:
     op = op_delete();
     break;
   case OP_HEAD:
     op = op_head();
     break;
   case OP_POST:
     op = op_post();
     break;
   case OP_COPY:
     op = op_copy();
     break;
   case OP_OPTIONS:
     op = op_options();
     break;
   default:
     return NULL;
  }

  if (op) {
    op->init(store, s, this);
  }
  return op;
}

void RGWHandler::put_op(RGWOp *op)
{
  delete op;
}

