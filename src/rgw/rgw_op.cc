// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <stdlib.h>
#include <system_error>
#include <unistd.h>

#include <sstream>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/bind.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>
#include <boost/utility/string_view.hpp>

#include "include/scope_guard.h"
#include "common/Clock.h"
#include "common/armor.h"
#include "common/errno.h"
#include "common/mime.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/static_ptr.h"

#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_acl_swift.h"
#include "rgw_aio_throttle.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_multi.h"
#include "rgw_multi_del.h"
#include "rgw_cors.h"
#include "rgw_cors_s3.h"
#include "rgw_rest_conn.h"
#include "rgw_rest_s3.h"
#include "rgw_tar.h"
#include "rgw_client_io.h"
#include "rgw_compression.h"
#include "rgw_role.h"
#include "rgw_tag_s3.h"
#include "rgw_putobj_processor.h"
#include "rgw_crypt.h"
#include "rgw_perf_counters.h"

#include "services/svc_zone.h"
#include "services/svc_quota.h"
#include "services/svc_sys_obj.h"

#include "cls/lock/cls_lock_client.h"
#include "cls/rgw/cls_rgw_client.h"


#include "include/ceph_assert.h"

#include "compressor/Compressor.h"

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/rgw_op.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace librados;
using ceph::crypto::MD5;
using boost::optional;
using boost::none;

using rgw::IAM::ARN;
using rgw::IAM::Effect;
using rgw::IAM::Policy;

using rgw::IAM::Policy;

static string mp_ns = RGW_OBJ_NS_MULTIPART;
static string shadow_ns = RGW_OBJ_NS_SHADOW;

static void forward_req_info(CephContext *cct, req_info& info, const std::string& bucket_name);
static int forward_request_to_master(struct req_state *s, obj_version *objv, RGWRados *store,
                                     bufferlist& in_data, JSONParser *jp, req_info *forward_info = nullptr);

static MultipartMetaFilter mp_filter;

// this probably should belong in the rgw_iam_policy_keywords, I'll get it to it
// at some point
static constexpr auto S3_EXISTING_OBJTAG = "s3:ExistingObjectTag";

int RGWGetObj::parse_range(void)
{
  int r = -ERANGE;
  string rs(range_str);
  string ofs_str;
  string end_str;

  ignore_invalid_range = s->cct->_conf->rgw_ignore_get_invalid_range;
  partial_content = false;

  size_t pos = rs.find("bytes=");
  if (pos == string::npos) {
    pos = 0;
    while (isspace(rs[pos]))
      pos++;
    int end = pos;
    while (isalpha(rs[end]))
      end++;
    if (strncasecmp(rs.c_str(), "bytes", end - pos) != 0)
      return 0;
    while (isspace(rs[end]))
      end++;
    if (rs[end] != '=')
      return 0;
    rs = rs.substr(end + 1);
  } else {
    rs = rs.substr(pos + 6); /* size of("bytes=")  */
  }
  pos = rs.find('-');
  if (pos == string::npos)
    goto done;

  partial_content = true;

  ofs_str = rs.substr(0, pos);
  end_str = rs.substr(pos + 1);
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

  range_parsed = true;
  return 0;

done:
  if (ignore_invalid_range) {
    partial_content = false;
    ofs = 0;
    end = -1;
    range_parsed = false; // allow retry
    r = 0;
  }

  return r;
}

static int decode_policy(CephContext *cct,
                         bufferlist& bl,
                         RGWAccessControlPolicy *policy)
{
  auto iter = bl.cbegin();
  try {
    policy->decode(iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    ldout(cct, 15) << __func__ << " Read AccessControlPolicy";
    RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(policy);
    s3policy->to_xml(*_dout);
    *_dout << dendl;
  }
  return 0;
}


static int get_user_policy_from_attr(CephContext * const cct,
				     RGWRados * const store,
				     map<string, bufferlist>& attrs,
				     RGWAccessControlPolicy& policy    /* out */)
{
  auto aiter = attrs.find(RGW_ATTR_ACL);
  if (aiter != attrs.end()) {
    int ret = decode_policy(cct, aiter->second, &policy);
    if (ret < 0) {
      return ret;
    }
  } else {
    return -ENOENT;
  }

  return 0;
}

static int get_bucket_instance_policy_from_attr(CephContext *cct,
						RGWRados *store,
						RGWBucketInfo& bucket_info,
						map<string, bufferlist>& bucket_attrs,
						RGWAccessControlPolicy *policy)
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

static int get_obj_policy_from_attr(CephContext *cct,
				    RGWRados *store,
				    RGWObjectCtx& obj_ctx,
				    RGWBucketInfo& bucket_info,
				    map<string, bufferlist>& bucket_attrs,
				    RGWAccessControlPolicy *policy,
                                    string *storage_class,
				    rgw_obj& obj)
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

  if (storage_class) {
    bufferlist scbl;
    int r = rop.get_attr(RGW_ATTR_STORAGE_CLASS, scbl);
    if (r >= 0) {
      *storage_class = scbl.to_str();
    } else {
      storage_class->clear();
    }
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
int rgw_op_get_bucket_policy_from_attr(CephContext *cct,
                                       RGWRados *store,
                                       RGWBucketInfo& bucket_info,
                                       map<string, bufferlist>& bucket_attrs,
                                       RGWAccessControlPolicy *policy)
{
  return get_bucket_instance_policy_from_attr(cct, store, bucket_info, bucket_attrs, policy);
}

static boost::optional<Policy> get_iam_policy_from_attr(CephContext* cct,
							RGWRados* store,
							map<string, bufferlist>& attrs,
							const string& tenant) {
  auto i = attrs.find(RGW_ATTR_IAM_POLICY);
  if (i != attrs.end()) {
    return Policy(cct, tenant, i->second);
  } else {
    return none;
  }
}

vector<Policy> get_iam_user_policy_from_attr(CephContext* cct,
                        RGWRados* store,
                        map<string, bufferlist>& attrs,
                        const string& tenant) {
  vector<Policy> policies;
  if (auto it = attrs.find(RGW_ATTR_USER_POLICY); it != attrs.end()) {
   bufferlist out_bl = attrs[RGW_ATTR_USER_POLICY];
   map<string, string> policy_map;
   decode(policy_map, out_bl);
   for (auto& it : policy_map) {
     bufferlist bl = bufferlist::static_from_string(it.second);
     Policy p(cct, tenant, bl);
     policies.push_back(std::move(p));
   }
  }
  return policies;
}

static int get_obj_attrs(RGWRados *store, struct req_state *s, const rgw_obj& obj, map<string, bufferlist>& attrs)
{
  RGWRados::Object op_target(store, s->bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;

  return read_op.prepare();
}

static int get_obj_head(RGWRados *store, struct req_state *s,
                        const rgw_obj& obj,
                        map<string, bufferlist> *attrs,
                         bufferlist *pbl)
{
  store->set_prefetch_data(s->obj_ctx, obj);

  RGWRados::Object op_target(store, s->bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = attrs;

  int ret = read_op.prepare();
  if (ret < 0) {
    return ret;
  }

  if (!pbl) {
    return 0;
  }

  ret = read_op.read(0, s->cct->_conf->rgw_max_chunk_size, *pbl);

  return 0;
}

struct multipart_upload_info
{
  rgw_placement_rule dest_placement;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(dest_placement, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(dest_placement, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(multipart_upload_info)

static int get_multipart_info(RGWRados *store, struct req_state *s,
			      const rgw_obj& obj,
                              RGWAccessControlPolicy *policy,
			      map<string, bufferlist> *attrs,
                              multipart_upload_info *upload_info)
{
  bufferlist header;

  bufferlist headbl;
  bufferlist *pheadbl = (upload_info ? &headbl : nullptr);

  int op_ret = get_obj_head(store, s, obj, attrs, pheadbl);
  if (op_ret < 0) {
    if (op_ret == -ENOENT) {
      return -ERR_NO_SUCH_UPLOAD;
    }
    return op_ret;
  }

  if (upload_info && headbl.length() > 0) {
    auto hiter = headbl.cbegin();
    try {
      decode(*upload_info, hiter);
    } catch (buffer::error& err) {
      ldpp_dout(s, 0) << "ERROR: failed to decode multipart upload info" << dendl;
      return -EIO;
    }
  }

  if (policy && attrs) {
    for (auto& iter : *attrs) {
      string name = iter.first;
      if (name.compare(RGW_ATTR_ACL) == 0) {
        bufferlist& bl = iter.second;
        auto bli = bl.cbegin();
        try {
          decode(*policy, bli);
        } catch (buffer::error& err) {
          ldpp_dout(s, 0) << "ERROR: could not decode policy" << dendl;
          return -EIO;
        }
        break;
      }
    }
  }

  return 0;
}

static int get_multipart_info(RGWRados *store, struct req_state *s,
			      const string& meta_oid,
                              RGWAccessControlPolicy *policy,
			      map<string, bufferlist> *attrs,
                              multipart_upload_info *upload_info)
{
  map<string, bufferlist>::iterator iter;
  bufferlist header;

  rgw_obj meta_obj;
  meta_obj.init_ns(s->bucket, meta_oid, mp_ns);
  meta_obj.set_in_extra_data(true);

  return get_multipart_info(store, s, meta_obj, policy, attrs, upload_info);
}

static int modify_obj_attr(RGWRados *store, struct req_state *s, const rgw_obj& obj, const char* attr_name, bufferlist& attr_val)
{
  map<string, bufferlist> attrs;
  RGWRados::Object op_target(store, s->bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;
  
  int r = read_op.prepare();
  if (r < 0) {
    return r;
  }
  store->set_atomic(s->obj_ctx, read_op.state.obj);
  attrs[attr_name] = attr_val;
  return store->set_attrs(s->obj_ctx, s->bucket_info, read_op.state.obj, attrs, NULL);
}

static int read_bucket_policy(RGWRados *store,
                              struct req_state *s,
                              RGWBucketInfo& bucket_info,
                              map<string, bufferlist>& bucket_attrs,
                              RGWAccessControlPolicy *policy,
                              rgw_bucket& bucket)
{
  if (!s->system_request && bucket_info.flags & BUCKET_SUSPENDED) {
    ldpp_dout(s, 0) << "NOTICE: bucket " << bucket_info.bucket.name
        << " is suspended" << dendl;
    return -ERR_USER_SUSPENDED;
  }

  if (bucket.name.empty()) {
    return 0;
  }

  int ret = rgw_op_get_bucket_policy_from_attr(s->cct, store, bucket_info, bucket_attrs, policy);
  if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_BUCKET;
  }

  return ret;
}

static int read_obj_policy(RGWRados *store,
                           struct req_state *s,
                           RGWBucketInfo& bucket_info,
                           map<string, bufferlist>& bucket_attrs,
                           RGWAccessControlPolicy* acl,
                           string *storage_class,
			   boost::optional<Policy>& policy,
                           rgw_bucket& bucket,
                           rgw_obj_key& object)
{
  string upload_id;
  upload_id = s->info.args.get("uploadId");
  rgw_obj obj;

  if (!s->system_request && bucket_info.flags & BUCKET_SUSPENDED) {
    ldpp_dout(s, 0) << "NOTICE: bucket " << bucket_info.bucket.name
        << " is suspended" << dendl;
    return -ERR_USER_SUSPENDED;
  }

  if (!upload_id.empty()) {
    /* multipart upload */
    RGWMPObj mp(object.name, upload_id);
    string oid = mp.get_meta();
    obj.init_ns(bucket, oid, mp_ns);
    obj.set_in_extra_data(true);
  } else {
    obj = rgw_obj(bucket, object);
  }
  policy = get_iam_policy_from_attr(s->cct, store, bucket_attrs, bucket.tenant);

  RGWObjectCtx *obj_ctx = static_cast<RGWObjectCtx *>(s->obj_ctx);
  int ret = get_obj_policy_from_attr(s->cct, store, *obj_ctx,
                                     bucket_info, bucket_attrs, acl, storage_class, obj);
  if (ret == -ENOENT) {
    /* object does not exist checking the bucket's ACL to make sure
       that we send a proper error code */
    RGWAccessControlPolicy bucket_policy(s->cct);
    ret = rgw_op_get_bucket_policy_from_attr(s->cct, store, bucket_info, bucket_attrs, &bucket_policy);
    if (ret < 0) {
      return ret;
    }

    const rgw_user& bucket_owner = bucket_policy.get_owner().get_id();
    if (bucket_owner.compare(s->user->user_id) != 0 &&
        ! s->auth.identity->is_admin_of(bucket_owner) &&
        ! bucket_policy.verify_permission(s, *s->auth.identity, s->perm_mask,
                                          RGW_PERM_READ)) {
      ret = -EACCES;
    } else {
      ret = -ENOENT;
    }
  }

  return ret;
}

/**
 * Get the AccessControlPolicy for an user, bucket or object off of disk.
 * s: The req_state to draw information from.
 * only_bucket: If true, reads the user and bucket ACLs rather than the object ACL.
 * Returns: 0 on success, -ERR# otherwise.
 */
int rgw_build_bucket_policies(RGWRados* store, struct req_state* s)
{
  int ret = 0;
  rgw_obj_key obj;
  RGWUserInfo bucket_owner_info;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  string bi = s->info.args.get(RGW_SYS_PARAM_PREFIX "bucket-instance");
  if (!bi.empty()) {
    ret = rgw_bucket_parse_bucket_instance(bi, &s->bucket_instance_id, &s->bucket_instance_shard_id);
    if (ret < 0) {
      return ret;
    }
  }

  if(s->dialect.compare("s3") == 0) {
    s->bucket_acl = std::make_unique<RGWAccessControlPolicy_S3>(s->cct);
  } else if(s->dialect.compare("swift")  == 0) {
    /* We aren't allocating the account policy for those operations using
     * the Swift's infrastructure that don't really need req_state::user.
     * Typical example here is the implementation of /info. */
    if (!s->user->user_id.empty()) {
      s->user_acl = std::make_unique<RGWAccessControlPolicy_SWIFTAcct>(s->cct);
    }
    s->bucket_acl = std::make_unique<RGWAccessControlPolicy_SWIFT>(s->cct);
  } else {
    s->bucket_acl = std::make_unique<RGWAccessControlPolicy>(s->cct);
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
      s->local_source = store->svc.zone->get_zonegroup().equals(zonegroup);
    }
  }

  struct {
    rgw_user uid;
    std::string display_name;
  } acct_acl_user = {
    s->user->user_id,
    s->user->display_name,
  };

  if (!s->bucket_name.empty()) {
    s->bucket_exists = true;
    if (s->bucket_instance_id.empty()) {
      ret = store->get_bucket_info(obj_ctx, s->bucket_tenant, s->bucket_name,
                                   s->bucket_info, &s->bucket_mtime,
                                   &s->bucket_attrs);
    } else {
      ret = store->get_bucket_instance_info(obj_ctx, s->bucket_instance_id,
                                            s->bucket_info, &s->bucket_mtime,
                                            &s->bucket_attrs);
    }
    if (ret < 0) {
      if (ret != -ENOENT) {
        string bucket_log;
        rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name, bucket_log);
        ldpp_dout(s, 0) << "NOTICE: couldn't get bucket from bucket_name (name="
            << bucket_log << ")" << dendl;
        return ret;
      }
      s->bucket_exists = false;
    }
    s->bucket = s->bucket_info.bucket;

    if (s->bucket_exists) {
      ret = read_bucket_policy(store, s, s->bucket_info, s->bucket_attrs,
                               s->bucket_acl.get(), s->bucket);
      acct_acl_user = {
        s->bucket_info.owner,
        s->bucket_acl->get_owner().get_display_name(),
      };
    } else {
      return -ERR_NO_SUCH_BUCKET;
    }

    s->bucket_owner = s->bucket_acl->get_owner();

    RGWZoneGroup zonegroup;
    int r = store->svc.zone->get_zonegroup(s->bucket_info.zonegroup, zonegroup);
    if (!r) {
      if (!zonegroup.endpoints.empty()) {
	s->zonegroup_endpoint = zonegroup.endpoints.front();
      } else {
        // use zonegroup's master zone endpoints
        auto z = zonegroup.zones.find(zonegroup.master_zone);
        if (z != zonegroup.zones.end() && !z->second.endpoints.empty()) {
          s->zonegroup_endpoint = z->second.endpoints.front();
        }
      }
      s->zonegroup_name = zonegroup.get_name();
    }
    if (r < 0 && ret == 0) {
      ret = r;
    }

    if (s->bucket_exists && !store->svc.zone->get_zonegroup().equals(s->bucket_info.zonegroup)) {
      ldpp_dout(s, 0) << "NOTICE: request for data in a different zonegroup ("
          << s->bucket_info.zonegroup << " != "
          << store->svc.zone->get_zonegroup().get_id() << ")" << dendl;
      /* we now need to make sure that the operation actually requires copy source, that is
       * it's a copy operation
       */
      if (store->svc.zone->get_zonegroup().is_master_zonegroup() && s->system_request) {
        /*If this is the master, don't redirect*/
      } else if (s->op_type == RGW_OP_GET_BUCKET_LOCATION ) {
        /* If op is get bucket location, don't redirect */
      } else if (!s->local_source ||
          (s->op != OP_PUT && s->op != OP_COPY) ||
          s->object.empty()) {
        return -ERR_PERMANENT_REDIRECT;
      }
    }

    /* init dest placement -- only if bucket exists, otherwise request is either not relevant, or
     * it's a create_bucket request, in which case the op will deal with the placement later */
    if (s->bucket_exists) {
      s->dest_placement.storage_class = s->info.storage_class;
      s->dest_placement.inherit_from(s->bucket_info.placement_rule);

      if (!store->svc.zone->get_zone_params().valid_placement(s->dest_placement)) {
        ldpp_dout(s, 0) << "NOTICE: invalid dest placement: " << s->dest_placement.to_str() << dendl;
        return -EINVAL;
      }
    }
  }

  /* handle user ACL only for those APIs which support it */
  if (s->user_acl) {
    map<string, bufferlist> uattrs;
    ret = rgw_get_user_attrs_by_uid(store, acct_acl_user.uid, uattrs);
    if (!ret) {
      ret = get_user_policy_from_attr(s->cct, store, uattrs, *s->user_acl);
    }
    if (-ENOENT == ret) {
      /* In already existing clusters users won't have ACL. In such case
       * assuming that only account owner has the rights seems to be
       * reasonable. That allows to have only one verification logic.
       * NOTE: there is small compatibility kludge for global, empty tenant:
       *  1. if we try to reach an existing bucket, its owner is considered
       *     as account owner.
       *  2. otherwise account owner is identity stored in s->user->user_id.  */
      s->user_acl->create_default(acct_acl_user.uid,
                                  acct_acl_user.display_name);
      ret = 0;
    } else if (ret < 0) {
      ldpp_dout(s, 0) << "NOTICE: couldn't get user attrs for handling ACL "
          "(user_id=" << s->user->user_id << ", ret=" << ret << ")" << dendl;
      return ret;
    }
  }
  // We don't need user policies in case of STS token returned by AssumeRole,
  // hence the check for user type
  if (! s->user->user_id.empty() && s->auth.identity->get_identity_type() != TYPE_ROLE) {
    try {
      map<string, bufferlist> uattrs;
      if (ret = rgw_get_user_attrs_by_uid(store, s->user->user_id, uattrs); ! ret) {
        if (s->iam_user_policies.empty()) {
          s->iam_user_policies = get_iam_user_policy_from_attr(s->cct, store, uattrs, s->user->user_id.tenant);
        } else {
          // This scenario can happen when a STS token has a policy, then we need to append other user policies
          // to the existing ones. (e.g. token returned by GetSessionToken)
          auto user_policies = get_iam_user_policy_from_attr(s->cct, store, uattrs, s->user->user_id.tenant);
          s->iam_user_policies.insert(s->iam_user_policies.end(), user_policies.begin(), user_policies.end());
        }
      } else {
        if (ret == -ENOENT)
          ret = 0;
        else ret = -EACCES;
      }
    } catch (const std::exception& e) {
      lderr(s->cct) << "Error reading IAM User Policy: " << e.what() << dendl;
      ret = -EACCES;
    }
  }

  try {
    s->iam_policy = get_iam_policy_from_attr(s->cct, store, s->bucket_attrs,
					     s->bucket_tenant);
  } catch (const std::exception& e) {
    // Really this is a can't happen condition. We parse the policy
    // when it's given to us, so perhaps we should abort or otherwise
    // raise bloody murder.
    ldpp_dout(s, 0) << "Error reading IAM Policy: " << e.what() << dendl;
    ret = -EACCES;
  }

  bool success = store->svc.zone->get_redirect_zone_endpoint(&s->redirect_zone_endpoint);
  if (success) {
    ldpp_dout(s, 20) << "redirect_zone_endpoint=" << s->redirect_zone_endpoint << dendl;
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
    s->object_acl = std::make_unique<RGWAccessControlPolicy>(s->cct);
    rgw_obj obj(s->bucket, s->object);
      
    store->set_atomic(s->obj_ctx, obj);
    if (prefetch_data) {
      store->set_prefetch_data(s->obj_ctx, obj);
    }
    ret = read_obj_policy(store, s, s->bucket_info, s->bucket_attrs,
			  s->object_acl.get(), nullptr, s->iam_policy, s->bucket,
                          s->object);
  }

  return ret;
}

void rgw_add_to_iam_environment(rgw::IAM::Environment& e, std::string_view key, std::string_view val){
  // This variant just adds non empty key pairs to IAM env., values can be empty
  // in certain cases like tagging
  if (!key.empty())
    e.emplace(key,val);
}

static int rgw_iam_add_tags_from_bl(struct req_state* s, bufferlist& bl){
  RGWObjTags tagset;
  try {
    auto bliter = bl.cbegin();
    tagset.decode(bliter);
  } catch (buffer::error& err) {
    ldpp_dout(s, 0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
    return -EIO;
  }

  for (const auto& tag: tagset.get_tags()){
    rgw_add_to_iam_environment(s->env, "s3:ExistingObjectTag/" + tag.first, tag.second);
  }
  return 0;
}

static int rgw_iam_add_existing_objtags(RGWRados* store, struct req_state* s, rgw_obj& obj, std::uint64_t action){
  map <string, bufferlist> attrs;
  store->set_atomic(s->obj_ctx, obj);
  int op_ret = get_obj_attrs(store, s, obj, attrs);
  if (op_ret < 0)
    return op_ret;
  auto tags = attrs.find(RGW_ATTR_TAGS);
  if (tags != attrs.end()){
    return rgw_iam_add_tags_from_bl(s, tags->second);
  }
  return 0;
}

static void rgw_add_grant_to_iam_environment(rgw::IAM::Environment& e, struct req_state *s){

  using header_pair_t = std::pair <const char*, const char*>;
  static const std::initializer_list <header_pair_t> acl_header_conditionals {
    {"HTTP_X_AMZ_GRANT_READ", "s3:x-amz-grant-read"},
    {"HTTP_X_AMZ_GRANT_WRITE", "s3:x-amz-grant-write"},
    {"HTTP_X_AMZ_GRANT_READ_ACP", "s3:x-amz-grant-read-acp"},
    {"HTTP_X_AMZ_GRANT_WRITE_ACP", "s3:x-amz-grant-write-acp"},
    {"HTTP_X_AMZ_GRANT_FULL_CONTROL", "s3:x-amz-grant-full-control"}
  };

  if (s->has_acl_header){
    for (const auto& c: acl_header_conditionals){
      auto hdr = s->info.env->get(c.first);
      if(hdr) {
	e[c.second] = hdr;
      }
    }
  }
}

void rgw_build_iam_environment(RGWRados* store,
	                              struct req_state* s)
{
  const auto& m = s->info.env->get_map();
  auto t = ceph::real_clock::now();
  s->env.emplace("aws:CurrentTime", std::to_string(ceph::real_clock::to_time_t(t)));
  s->env.emplace("aws:EpochTime", ceph::to_iso_8601(t));
  // TODO: This is fine for now, but once we have STS we'll need to
  // look and see. Also this won't work with the IdentityApplier
  // model, since we need to know the actual credential.
  s->env.emplace("aws:PrincipalType", "User");

  auto i = m.find("HTTP_REFERER");
  if (i != m.end()) {
    s->env.emplace("aws:Referer", i->second);
  }

  if (rgw_transport_is_secure(s->cct, *s->info.env)) {
    s->env.emplace("aws:SecureTransport", "true");
  }

  const auto remote_addr_param = s->cct->_conf->rgw_remote_addr_param;
  if (remote_addr_param.length()) {
    i = m.find(remote_addr_param);
  } else {
    i = m.find("REMOTE_ADDR");
  }
  if (i != m.end()) {
    const string* ip = &(i->second);
    string temp;
    if (remote_addr_param == "HTTP_X_FORWARDED_FOR") {
      const auto comma = ip->find(',');
      if (comma != string::npos) {
	temp.assign(*ip, 0, comma);
	ip = &temp;
      }
    }
    s->env.emplace("aws:SourceIp", *ip);
  }

  i = m.find("HTTP_USER_AGENT"); {
  if (i != m.end())
    s->env.emplace("aws:UserAgent", i->second);
  }

  if (s->user) {
    // What to do about aws::userid? One can have multiple access
    // keys so that isn't really suitable. Do we have a durable
    // identifier that can persist through name changes?
    s->env.emplace("aws:username", s->user->user_id.id);
  }

  i = m.find("HTTP_X_AMZ_SECURITY_TOKEN");
  if (i != m.end()) {
    s->env.emplace("sts:authentication", "true");
  } else {
    s->env.emplace("sts:authentication", "false");
  }
}

void rgw_bucket_object_pre_exec(struct req_state *s)
{
  if (s->expect_cont)
    dump_continue(s);

  dump_bucket_from_state(s);
}

// So! Now and then when we try to update bucket information, the
// bucket has changed during the course of the operation. (Or we have
// a cache consistency problem that Watch/Notify isn't ruling out
// completely.)
//
// When this happens, we need to update the bucket info and try
// again. We have, however, to try the right *part* again.  We can't
// simply re-send, since that will obliterate the previous update.
//
// Thus, callers of this function should include everything that
// merges information to be changed into the bucket information as
// well as the call to set it.
//
// The called function must return an integer, negative on error. In
// general, they should just return op_ret.
namespace {
template<typename F>
int retry_raced_bucket_write(RGWRados* g, req_state* s, const F& f) {
  auto r = f();
  for (auto i = 0u; i < 15u && r == -ECANCELED; ++i) {
    r = g->try_refresh_bucket_info(s->bucket_info, nullptr,
				   &s->bucket_attrs);
    if (r >= 0) {
      r = f();
    }
  }
  return r;
}
}


int RGWGetObj::verify_permission()
{
  obj = rgw_obj(s->bucket, s->object);
  store->set_atomic(s->obj_ctx, obj);
  if (get_data) {
    store->set_prefetch_data(s->obj_ctx, obj);
  }

  if (torrent.get_flag()) {
    if (obj.key.instance.empty()) {
      action = rgw::IAM::s3GetObjectTorrent;
    } else {
      action = rgw::IAM::s3GetObjectVersionTorrent;
    }
  } else {
    if (obj.key.instance.empty()) {
      action = rgw::IAM::s3GetObject;
    } else {
      action = rgw::IAM::s3GetObjectVersion;
    }
    if (s->iam_policy && s->iam_policy->has_partial_conditional(S3_EXISTING_OBJTAG))
      rgw_iam_add_existing_objtags(store, s, obj, action);
    if (! s->iam_user_policies.empty()) {
      for (auto& user_policy : s->iam_user_policies) {
        if (user_policy.has_partial_conditional(S3_EXISTING_OBJTAG))
          rgw_iam_add_existing_objtags(store, s, obj, action);
      }
    }
  }

  if (!verify_object_permission(this, s, action)) {
    return -EACCES;
  }

  return 0;
}


int RGWOp::verify_op_mask()
{
  uint32_t required_mask = op_mask();

  ldpp_dout(this, 20) << "required_mask= " << required_mask
      << " user.op_mask=" << s->user->op_mask << dendl;

  if ((s->user->op_mask & required_mask) != required_mask) {
    return -EPERM;
  }

  if (!s->system_request && (required_mask & RGW_OP_TYPE_MODIFY) && !store->svc.zone->zone_is_writeable()) {
    ldpp_dout(this, 5) << "NOTICE: modify request to a read-only zone by a "
        "non-system user, permission denied"  << dendl;
    return -EPERM;
  }

  return 0;
}

int RGWGetObjTags::verify_permission()
{
  auto iam_action = s->object.instance.empty()?
    rgw::IAM::s3GetObjectTagging:
    rgw::IAM::s3GetObjectVersionTagging;
  // TODO since we are parsing the bl now anyway, we probably change
  // the send_response function to accept RGWObjTag instead of a bl
  if (s->iam_policy && s->iam_policy->has_partial_conditional(S3_EXISTING_OBJTAG)){
    rgw_obj obj = rgw_obj(s->bucket, s->object);
    rgw_iam_add_existing_objtags(store, s, obj, iam_action);
  }
  if (! s->iam_user_policies.empty()) {
    for (auto& user_policy : s->iam_user_policies) {
      if (user_policy.has_partial_conditional(S3_EXISTING_OBJTAG)) {
        rgw_obj obj = rgw_obj(s->bucket, s->object);
        rgw_iam_add_existing_objtags(store, s, obj, iam_action);
      }
    }
  }
  if (!verify_object_permission(this, s,iam_action))
    return -EACCES;

  return 0;
}

void RGWGetObjTags::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetObjTags::execute()
{
  rgw_obj obj;
  map<string,bufferlist> attrs;

  obj = rgw_obj(s->bucket, s->object);

  store->set_atomic(s->obj_ctx, obj);

  op_ret = get_obj_attrs(store, s, obj, attrs);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get obj attrs, obj=" << obj
        << " ret=" << op_ret << dendl;
    return;
  }

  auto tags = attrs.find(RGW_ATTR_TAGS);
  if(tags != attrs.end()){
    has_tags = true;
    tags_bl.append(tags->second);
  }
  send_response_data(tags_bl);
}

int RGWPutObjTags::verify_permission()
{
  auto iam_action = s->object.instance.empty() ?
    rgw::IAM::s3PutObjectTagging:
    rgw::IAM::s3PutObjectVersionTagging;

  if(s->iam_policy && s->iam_policy->has_partial_conditional(S3_EXISTING_OBJTAG)){
    auto obj = rgw_obj(s->bucket, s->object);
    rgw_iam_add_existing_objtags(store, s, obj, iam_action);
  }
  if (! s->iam_user_policies.empty()) {
    for (auto& user_policy : s->iam_user_policies) {
      if (user_policy.has_partial_conditional(S3_EXISTING_OBJTAG)) {
        rgw_obj obj = rgw_obj(s->bucket, s->object);
        rgw_iam_add_existing_objtags(store, s, obj, iam_action);
      }
    }
  }
  if (!verify_object_permission(this, s,iam_action))
    return -EACCES;
  return 0;
}

void RGWPutObjTags::execute()
{
  op_ret = get_params();
  if (op_ret < 0)
    return;

  if (s->object.empty()){
    op_ret= -EINVAL; // we only support tagging on existing objects
    return;
  }

  rgw_obj obj;
  obj = rgw_obj(s->bucket, s->object);
  store->set_atomic(s->obj_ctx, obj);
  op_ret = modify_obj_attr(store, s, obj, RGW_ATTR_TAGS, tags_bl);
  if (op_ret == -ECANCELED){
    op_ret = -ERR_TAG_CONFLICT;
  }
}

void RGWDeleteObjTags::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}


int RGWDeleteObjTags::verify_permission()
{
  if (!s->object.empty()) {
    auto iam_action = s->object.instance.empty() ?
      rgw::IAM::s3DeleteObjectTagging:
      rgw::IAM::s3DeleteObjectVersionTagging;

    if (s->iam_policy && s->iam_policy->has_partial_conditional(S3_EXISTING_OBJTAG)){
      auto obj = rgw_obj(s->bucket, s->object);
      rgw_iam_add_existing_objtags(store, s, obj, iam_action);
    }
    if (! s->iam_user_policies.empty()) {
    for (auto& user_policy : s->iam_user_policies) {
      if (user_policy.has_partial_conditional(S3_EXISTING_OBJTAG)) {
        auto obj = rgw_obj(s->bucket, s->object);
        rgw_iam_add_existing_objtags(store, s, obj, iam_action);
      }
    }
  }
    if (!verify_object_permission(this, s, iam_action))
      return -EACCES;
  }
  return 0;
}

void RGWDeleteObjTags::execute()
{
  if (s->object.empty())
    return;

  rgw_obj obj;
  obj = rgw_obj(s->bucket, s->object);
  store->set_atomic(s->obj_ctx, obj);
  map <string, bufferlist> attrs;
  map <string, bufferlist> rmattr;
  bufferlist bl;
  rmattr[RGW_ATTR_TAGS] = bl;
  op_ret = store->set_attrs(s->obj_ctx, s->bucket_info, obj, attrs, &rmattr);
}

int RGWOp::do_aws4_auth_completion()
{
  ldpp_dout(this, 5) << "NOTICE: call to do_aws4_auth_completion"  << dendl;
  if (s->auth.completer) {
    if (!s->auth.completer->complete()) {
      return -ERR_AMZ_CONTENT_SHA256_MISMATCH;
    } else {
      ldpp_dout(this, 10) << "v4 auth ok -- do_aws4_auth_completion" << dendl;
    }

    /* TODO(rzarzynski): yes, we're really called twice on PUTs. Only first
     * call passes, so we disable second one. This is old behaviour, sorry!
     * Plan for tomorrow: seek and destroy. */
    s->auth.completer = nullptr;
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
    bucket_quota = store->svc.quota->get_bucket_quota();
  }

  if (uinfo->user_quota.enabled) {
    user_quota = uinfo->user_quota;
  } else {
    user_quota = store->svc.quota->get_user_quota();
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

  if (rule->get_allowed_methods() & flags) {
    dout(10) << "Method " << req_meth << " is supported" << dendl;
  } else {
    dout(5) << "Method " << req_meth << " is not supported" << dendl;
    return false;
  }

  return true;
}

static bool validate_cors_rule_header(RGWCORSRule *rule, const char *req_hdrs) {
  if (req_hdrs) {
    vector<string> hdrs;
    get_str_vec(req_hdrs, hdrs);
    for (const auto& hdr : hdrs) {
      if (!rule->is_header_allowed(hdr.c_str(), hdr.length())) {
        dout(5) << "Header " << hdr << " is not registered in this rule" << dendl;
        return false;
      }
    }
  }
  return true;
}

int RGWOp::read_bucket_cors()
{
  bufferlist bl;

  map<string, bufferlist>::iterator aiter = s->bucket_attrs.find(RGW_ATTR_CORS);
  if (aiter == s->bucket_attrs.end()) {
    ldpp_dout(this, 20) << "no CORS configuration attr found" << dendl;
    cors_exist = false;
    return 0; /* no CORS configuration found */
  }

  cors_exist = true;

  bl = aiter->second;

  auto iter = bl.cbegin();
  try {
    bucket_cors.decode(iter);
  } catch (buffer::error& err) {
    ldpp_dout(this, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    RGWCORSConfiguration_S3 *s3cors = static_cast<RGWCORSConfiguration_S3 *>(&bucket_cors);
    ldpp_dout(this, 15) << "Read RGWCORSConfiguration";
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
    ldpp_dout(this, 2) << "No CORS configuration set yet for this bucket" << dendl;
    return false;
  }

  /* CORS 6.2.2. */
  RGWCORSRule *rule = bucket_cors.host_name_rule(orig);
  if (!rule)
    return false;

  /*
   * Set the Allowed-Origin header to a asterisk if this is allowed in the rule
   * and no Authorization was send by the client
   *
   * The origin parameter specifies a URI that may access the resource.  The browser must enforce this.
   * For requests without credentials, the server may specify "*" as a wildcard,
   * thereby allowing any origin to access the resource.
   */
  const char *authorization = s->info.env->get("HTTP_AUTHORIZATION");
  if (!authorization && rule->has_wildcard_origin())
    origin = "*";

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
                                       const rgw_bucket_dir_entry& ent,
                                       RGWAccessControlPolicy * const bucket_acl,
                                       const boost::optional<Policy>& bucket_policy,
                                       const off_t start_ofs,
                                       const off_t end_ofs,
                                       bool swift_slo)
{
  ldpp_dout(this, 20) << "user manifest obj=" << ent.key.name
      << "[" << ent.key.instance << "]" << dendl;
  RGWGetObj_CB cb(this);
  RGWGetObj_Filter* filter = &cb;
  boost::optional<RGWGetObj_Decompress> decompress;

  int64_t cur_ofs = start_ofs;
  int64_t cur_end = end_ofs;

  rgw_obj part(bucket, ent.key);

  map<string, bufferlist> attrs;

  uint64_t obj_size;
  RGWObjectCtx obj_ctx(store);
  RGWAccessControlPolicy obj_policy(s->cct);

  ldpp_dout(this, 20) << "reading obj=" << part << " ofs=" << cur_ofs
      << " end=" << cur_end << dendl;

  obj_ctx.set_atomic(part);
  store->set_prefetch_data(&obj_ctx, part);

  RGWRados::Object op_target(store, s->bucket_info, obj_ctx, part);
  RGWRados::Object::Read read_op(&op_target);

  if (!swift_slo) {
    /* SLO etag is optional */
    read_op.conds.if_match = ent.meta.etag.c_str();
  }
  read_op.params.attrs = &attrs;
  read_op.params.obj_size = &obj_size;

  op_ret = read_op.prepare();
  if (op_ret < 0)
    return op_ret;
  op_ret = read_op.range_to_ofs(ent.meta.accounted_size, cur_ofs, cur_end);
  if (op_ret < 0)
    return op_ret;
  bool need_decompress;
  op_ret = rgw_compression_info_from_attrset(attrs, need_decompress, cs_info);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to decode compression info" << dendl;
    return -EIO;
  }

  if (need_decompress)
  {
    if (cs_info.orig_size != ent.meta.accounted_size) {
      // hmm.. something wrong, object not as expected, abort!
      ldpp_dout(this, 0) << "ERROR: expected cs_info.orig_size=" << cs_info.orig_size
          << ", actual read size=" << ent.meta.size << dendl;
      return -EIO;
    }
    decompress.emplace(s->cct, &cs_info, partial_content, filter);
    filter = &*decompress;
  }
  else
  {
    if (obj_size != ent.meta.size) {
      // hmm.. something wrong, object not as expected, abort!
      ldpp_dout(this, 0) << "ERROR: expected obj_size=" << obj_size
          << ", actual read size=" << ent.meta.size << dendl;
      return -EIO;
	  }
  }

  op_ret = rgw_policy_from_attrset(s->cct, attrs, &obj_policy);
  if (op_ret < 0)
    return op_ret;

  /* We can use global user_acl because LOs cannot have segments
   * stored inside different accounts. */
  if (s->system_request) {
    ldpp_dout(this, 2) << "overriding permissions due to system operation" << dendl;
  } else if (s->auth.identity->is_admin_of(s->user->user_id)) {
    ldpp_dout(this, 2) << "overriding permissions due to admin operation" << dendl;
  } else if (!verify_object_permission(this, s, part, s->user_acl.get(), bucket_acl,
				       &obj_policy, bucket_policy, s->iam_user_policies, action)) {
    return -EPERM;
  }
  if (ent.meta.size == 0) {
    return 0;
  }

  perfcounter->inc(l_rgw_get_b, cur_end - cur_ofs);
  filter->fixup_range(cur_ofs, cur_end);
  op_ret = read_op.iterate(cur_ofs, cur_end, filter);
  if (op_ret >= 0)
	  op_ret = filter->flush();
  return op_ret;
}

static int iterate_user_manifest_parts(CephContext * const cct,
                                       RGWRados * const store,
                                       const off_t ofs,
                                       const off_t end,
                                       RGWBucketInfo *pbucket_info,
                                       const string& obj_prefix,
                                       RGWAccessControlPolicy * const bucket_acl,
                                       const boost::optional<Policy>& bucket_policy,
                                       uint64_t * const ptotal_len,
                                       uint64_t * const pobj_size,
                                       string * const pobj_sum,
                                       int (*cb)(rgw_bucket& bucket,
                                                 const rgw_bucket_dir_entry& ent,
                                                 RGWAccessControlPolicy * const bucket_acl,
                                                 const boost::optional<Policy>& bucket_policy,
                                                 off_t start_ofs,
                                                 off_t end_ofs,
                                                 void *param,
                                                 bool swift_slo),
                                       void * const cb_param)
{
  rgw_bucket& bucket = pbucket_info->bucket;
  uint64_t obj_ofs = 0, len_count = 0;
  bool found_start = false, found_end = false, handled_end = false;
  string delim;
  bool is_truncated;
  vector<rgw_bucket_dir_entry> objs;

  utime_t start_time = ceph_clock_now();

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

    for (rgw_bucket_dir_entry& ent : objs) {
      const uint64_t cur_total_len = obj_ofs;
      const uint64_t obj_size = ent.meta.accounted_size;
      uint64_t start_ofs = 0, end_ofs = obj_size;

      if ((ptotal_len || cb) && !found_start && cur_total_len + obj_size > (uint64_t)ofs) {
	start_ofs = ofs - obj_ofs;
	found_start = true;
      }

      obj_ofs += obj_size;
      if (pobj_sum) {
        etag_sum.Update((const unsigned char *)ent.meta.etag.c_str(),
                        ent.meta.etag.length());
      }

      if ((ptotal_len || cb) && !found_end && obj_ofs > (uint64_t)end) {
	end_ofs = end - cur_total_len + 1;
	found_end = true;
      }

      perfcounter->tinc(l_rgw_get_lat,
			(ceph_clock_now() - start_time));

      if (found_start && !handled_end) {
        len_count += end_ofs - start_ofs;

        if (cb) {
          r = cb(bucket, ent, bucket_acl, bucket_policy, start_ofs, end_ofs,
		 cb_param, false /* swift_slo */);
          if (r < 0) {
            return r;
          }
        }
      }

      handled_end = found_end;
      start_time = ceph_clock_now();
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
  RGWAccessControlPolicy *bucket_acl = nullptr;
  Policy* bucket_policy = nullptr;
  rgw_bucket bucket;
  string obj_name;
  uint64_t size = 0;
  string etag;
};

static int iterate_slo_parts(CephContext *cct,
                             RGWRados *store,
                             off_t ofs,
                             off_t end,
                             map<uint64_t, rgw_slo_part>& slo_parts,
                             int (*cb)(rgw_bucket& bucket,
                                       const rgw_bucket_dir_entry& ent,
                                       RGWAccessControlPolicy *bucket_acl,
                                       const boost::optional<Policy>& bucket_policy,
                                       off_t start_ofs,
                                       off_t end_ofs,
                                       void *param,
                                       bool swift_slo),
                             void *cb_param)
{
  bool found_start = false, found_end = false;

  if (slo_parts.empty()) {
    return 0;
  }

  utime_t start_time = ceph_clock_now();

  map<uint64_t, rgw_slo_part>::iterator iter = slo_parts.upper_bound(ofs);
  if (iter != slo_parts.begin()) {
    --iter;
  }

  uint64_t obj_ofs = iter->first;

  for (; iter != slo_parts.end() && !found_end; ++iter) {
    rgw_slo_part& part = iter->second;
    rgw_bucket_dir_entry ent;

    ent.key.name = part.obj_name;
    ent.meta.accounted_size = ent.meta.size = part.size;
    ent.meta.etag = part.etag;

    uint64_t cur_total_len = obj_ofs;
    uint64_t start_ofs = 0, end_ofs = ent.meta.size;

    if (!found_start && cur_total_len + ent.meta.size > (uint64_t)ofs) {
      start_ofs = ofs - obj_ofs;
      found_start = true;
    }

    obj_ofs += ent.meta.size;

    if (!found_end && obj_ofs > (uint64_t)end) {
      end_ofs = end - cur_total_len + 1;
      found_end = true;
    }

    perfcounter->tinc(l_rgw_get_lat,
		      (ceph_clock_now() - start_time));

    if (found_start) {
      if (cb) {
	// SLO is a Swift thing, and Swift has no knowledge of S3 Policies.
        int r = cb(part.bucket, ent, part.bucket_acl,
		   (part.bucket_policy ?
		    boost::optional<Policy>(*part.bucket_policy) : none),
		   start_ofs, end_ofs, cb_param, true /* swift_slo */);
	if (r < 0)
          return r;
      }
    }

    start_time = ceph_clock_now();
  }

  return 0;
}

static int get_obj_user_manifest_iterate_cb(rgw_bucket& bucket,
                                            const rgw_bucket_dir_entry& ent,
                                            RGWAccessControlPolicy * const bucket_acl,
                                            const boost::optional<Policy>& bucket_policy,
                                            const off_t start_ofs,
                                            const off_t end_ofs,
                                            void * const param,
                                            bool swift_slo = false)
{
  RGWGetObj *op = static_cast<RGWGetObj *>(param);
  return op->read_user_manifest_part(
    bucket, ent, bucket_acl, bucket_policy, start_ofs, end_ofs, swift_slo);
}

int RGWGetObj::handle_user_manifest(const char *prefix)
{
  const boost::string_view prefix_view(prefix);
  ldpp_dout(this, 2) << "RGWGetObj::handle_user_manifest() prefix="
                   << prefix_view << dendl;

  const size_t pos = prefix_view.find('/');
  if (pos == string::npos) {
    return -EINVAL;
  }

  const std::string bucket_name = url_decode(prefix_view.substr(0, pos));
  const std::string obj_prefix = url_decode(prefix_view.substr(pos + 1));

  rgw_bucket bucket;

  RGWAccessControlPolicy _bucket_acl(s->cct);
  RGWAccessControlPolicy *bucket_acl;
  boost::optional<Policy> _bucket_policy;
  boost::optional<Policy>* bucket_policy;
  RGWBucketInfo bucket_info;
  RGWBucketInfo *pbucket_info;

  if (bucket_name.compare(s->bucket.name) != 0) {
    map<string, bufferlist> bucket_attrs;
    auto obj_ctx = store->svc.sysobj->init_obj_ctx();
    int r = store->get_bucket_info(obj_ctx, s->user->user_id.tenant,
				  bucket_name, bucket_info, NULL,
				  &bucket_attrs);
    if (r < 0) {
      ldpp_dout(this, 0) << "could not get bucket info for bucket="
		       << bucket_name << dendl;
      return r;
    }
    bucket = bucket_info.bucket;
    pbucket_info = &bucket_info;
    bucket_acl = &_bucket_acl;
    r = read_bucket_policy(store, s, bucket_info, bucket_attrs, bucket_acl, bucket);
    if (r < 0) {
      ldpp_dout(this, 0) << "failed to read bucket policy" << dendl;
      return r;
    }
    _bucket_policy = get_iam_policy_from_attr(s->cct, store, bucket_attrs,
					      bucket_info.bucket.tenant);
    bucket_policy = &_bucket_policy;
  } else {
    bucket = s->bucket;
    pbucket_info = &s->bucket_info;
    bucket_acl = s->bucket_acl.get();
    bucket_policy = &s->iam_policy;
  }

  /* dry run to find out:
   * - total length (of the parts we are going to send to client),
   * - overall DLO's content size,
   * - md5 sum of overall DLO's content (for etag of Swift API). */
  int r = iterate_user_manifest_parts(s->cct, store, ofs, end,
        pbucket_info, obj_prefix, bucket_acl, *bucket_policy,
        nullptr, &s->obj_size, &lo_etag,
        nullptr /* cb */, nullptr /* cb arg */);
  if (r < 0) {
    return r;
  }

  r = RGWRados::Object::Read::range_to_ofs(s->obj_size, ofs, end);
  if (r < 0) {
    return r;
  }

  r = iterate_user_manifest_parts(s->cct, store, ofs, end,
        pbucket_info, obj_prefix, bucket_acl, *bucket_policy,
        &total_len, nullptr, nullptr,
        nullptr, nullptr);
  if (r < 0) {
    return r;
  }

  if (!get_data) {
    bufferlist bl;
    send_response_data(bl, 0, 0);
    return 0;
  }

  r = iterate_user_manifest_parts(s->cct, store, ofs, end,
        pbucket_info, obj_prefix, bucket_acl, *bucket_policy,
        nullptr, nullptr, nullptr,
        get_obj_user_manifest_iterate_cb, (void *)this);
  if (r < 0) {
    return r;
  }

  if (!total_len) {
    bufferlist bl;
    send_response_data(bl, 0, 0);
  }

  return 0;
}

int RGWGetObj::handle_slo_manifest(bufferlist& bl)
{
  RGWSLOInfo slo_info;
  auto bliter = bl.cbegin();
  try {
    decode(slo_info, bliter);
  } catch (buffer::error& err) {
    ldpp_dout(this, 0) << "ERROR: failed to decode slo manifest" << dendl;
    return -EIO;
  }
  ldpp_dout(this, 2) << "RGWGetObj::handle_slo_manifest()" << dendl;

  vector<RGWAccessControlPolicy> allocated_acls;
  map<string, pair<RGWAccessControlPolicy *, boost::optional<Policy>>> policies;
  map<string, rgw_bucket> buckets;

  map<uint64_t, rgw_slo_part> slo_parts;

  MD5 etag_sum;
  total_len = 0;

  for (const auto& entry : slo_info.entries) {
    const string& path = entry.path;

    /* If the path starts with slashes, strip them all. */
    const size_t pos_init = path.find_first_not_of('/');
    /* According to the documentation of std::string::find following check
     * is not necessary as we should get the std::string::npos propagation
     * here. This might be true with the accuracy to implementation's bugs.
     * See following question on SO:
     * http://stackoverflow.com/questions/1011790/why-does-stdstring-findtext-stdstringnpos-not-return-npos
     */
    if (pos_init == string::npos) {
      return -EINVAL;
    }

    const size_t pos_sep = path.find('/', pos_init);
    if (pos_sep == string::npos) {
      return -EINVAL;
    }

    string bucket_name = path.substr(pos_init, pos_sep - pos_init);
    string obj_name = path.substr(pos_sep + 1);

    rgw_bucket bucket;
    RGWAccessControlPolicy *bucket_acl;
    Policy* bucket_policy;

    if (bucket_name.compare(s->bucket.name) != 0) {
      const auto& piter = policies.find(bucket_name);
      if (piter != policies.end()) {
        bucket_acl = piter->second.first;
        bucket_policy = piter->second.second.get_ptr();
	bucket = buckets[bucket_name];
      } else {
	allocated_acls.push_back(RGWAccessControlPolicy(s->cct));
	RGWAccessControlPolicy& _bucket_acl = allocated_acls.back();

        RGWBucketInfo bucket_info;
        map<string, bufferlist> bucket_attrs;
        auto obj_ctx = store->svc.sysobj->init_obj_ctx();
        int r = store->get_bucket_info(obj_ctx, s->user->user_id.tenant,
                                       bucket_name, bucket_info, nullptr,
                                       &bucket_attrs);
        if (r < 0) {
          ldpp_dout(this, 0) << "could not get bucket info for bucket="
			   << bucket_name << dendl;
          return r;
        }
        bucket = bucket_info.bucket;
        bucket_acl = &_bucket_acl;
        r = read_bucket_policy(store, s, bucket_info, bucket_attrs, bucket_acl,
                               bucket);
        if (r < 0) {
          ldpp_dout(this, 0) << "failed to read bucket ACL for bucket "
                           << bucket << dendl;
          return r;
	}
	auto _bucket_policy = get_iam_policy_from_attr(
	  s->cct, store, bucket_attrs, bucket_info.bucket.tenant);
        bucket_policy = _bucket_policy.get_ptr();
	buckets[bucket_name] = bucket;
        policies[bucket_name] = make_pair(bucket_acl, _bucket_policy);
      }
    } else {
      bucket = s->bucket;
      bucket_acl = s->bucket_acl.get();
      bucket_policy = s->iam_policy.get_ptr();
    }

    rgw_slo_part part;
    part.bucket_acl = bucket_acl;
    part.bucket_policy = bucket_policy;
    part.bucket = bucket;
    part.obj_name = obj_name;
    part.size = entry.size_bytes;
    part.etag = entry.etag;
    ldpp_dout(this, 20) << "slo_part: ofs=" << ofs
                      << " bucket=" << part.bucket
                      << " obj=" << part.obj_name
                      << " size=" << part.size
                      << " etag=" << part.etag
                      << dendl;

    etag_sum.Update((const unsigned char *)entry.etag.c_str(),
                    entry.etag.length());

    slo_parts[total_len] = part;
    total_len += part.size;
  } /* foreach entry */

  complete_etag(etag_sum, &lo_etag);

  s->obj_size = slo_info.total_size;
  ldpp_dout(this, 20) << "s->obj_size=" << s->obj_size << dendl;

  int r = RGWRados::Object::Read::range_to_ofs(total_len, ofs, end);
  if (r < 0) {
    return r;
  }

  total_len = end - ofs + 1;

  r = iterate_slo_parts(s->cct, store, ofs, end, slo_parts,
        get_obj_user_manifest_iterate_cb, (void *)this);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWGetObj::get_data_cb(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  /* garbage collection related handling */
  utime_t start_time = ceph_clock_now();
  if (start_time > gc_invalidate_time) {
    int r = store->defer_gc(s->obj_ctx, s->bucket_info, obj);
    if (r < 0) {
      ldpp_dout(this, 0) << "WARNING: could not defer gc entry for obj" << dendl;
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

  if (range_str) {
    int r = parse_range();
    /* error on parsing the range, stop prefetch and will fail in execute() */
    if (r < 0) {
      return false; /* range_parsed==false */
    }
    /* range get goes to shadow objects, stop prefetch */
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
      decode(delete_at, iter->second);
    } catch (buffer::error& err) {
      dout(0) << "ERROR: " << __func__ << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
      return false;
    }

    if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
      return true;
    }
  }

  return false;
}

void RGWGetObj::execute()
{
  bufferlist bl;
  gc_invalidate_time = ceph_clock_now();
  gc_invalidate_time += (s->cct->_conf->rgw_gc_obj_min_wait / 2);

  bool need_decompress;
  int64_t ofs_x, end_x;

  RGWGetObj_CB cb(this);
  RGWGetObj_Filter* filter = (RGWGetObj_Filter *)&cb;
  boost::optional<RGWGetObj_Decompress> decompress;
  std::unique_ptr<RGWGetObj_Filter> decrypt;
  map<string, bufferlist>::iterator attr_iter;

  perfcounter->inc(l_rgw_get);

  RGWRados::Object op_target(store, s->bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target);

  op_ret = get_params();
  if (op_ret < 0)
    goto done_err;

  op_ret = init_common();
  if (op_ret < 0)
    goto done_err;

  read_op.conds.mod_ptr = mod_ptr;
  read_op.conds.unmod_ptr = unmod_ptr;
  read_op.conds.high_precision_time = s->system_request; /* system request need to use high precision time */
  read_op.conds.mod_zone_id = mod_zone_id;
  read_op.conds.mod_pg_ver = mod_pg_ver;
  read_op.conds.if_match = if_match;
  read_op.conds.if_nomatch = if_nomatch;
  read_op.params.attrs = &attrs;
  read_op.params.lastmod = &lastmod;
  read_op.params.obj_size = &s->obj_size;

  op_ret = read_op.prepare();
  if (op_ret < 0)
    goto done_err;
  version_id = read_op.state.obj.key.instance;

  /* STAT ops don't need data, and do no i/o */
  if (get_type() == RGW_OP_STAT_OBJ) {
    return;
  }

  /* start gettorrent */
  if (torrent.get_flag())
  {
    attr_iter = attrs.find(RGW_ATTR_CRYPT_MODE);
    if (attr_iter != attrs.end() && attr_iter->second.to_str() == "SSE-C-AES256") {
      ldpp_dout(this, 0) << "ERROR: torrents are not supported for objects "
          "encrypted with SSE-C" << dendl;
      op_ret = -EINVAL;
      goto done_err;
    }
    torrent.init(s, store);
    op_ret = torrent.get_torrent_file(read_op, total_len, bl, obj);
    if (op_ret < 0)
    {
      ldpp_dout(this, 0) << "ERROR: failed to get_torrent_file ret= " << op_ret
                       << dendl;
      goto done_err;
    }
    op_ret = send_response_data(bl, 0, total_len);
    if (op_ret < 0)
    {
      ldpp_dout(this, 0) << "ERROR: failed to send_response_data ret= " << op_ret << dendl;
      goto done_err;
    }
    return;
  }
  /* end gettorrent */

  op_ret = rgw_compression_info_from_attrset(attrs, need_decompress, cs_info);
  if (op_ret < 0) {
    ldpp_dout(s, 0) << "ERROR: failed to decode compression info, cannot decompress" << dendl;
    goto done_err;
  }
  if (need_decompress) {
      s->obj_size = cs_info.orig_size;
      decompress.emplace(s->cct, &cs_info, partial_content, filter);
      filter = &*decompress;
  }

  attr_iter = attrs.find(RGW_ATTR_USER_MANIFEST);
  if (attr_iter != attrs.end() && !skip_manifest) {
    op_ret = handle_user_manifest(attr_iter->second.c_str());
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "ERROR: failed to handle user manifest ret="
		       << op_ret << dendl;
      goto done_err;
    }
    return;
  }

  attr_iter = attrs.find(RGW_ATTR_SLO_MANIFEST);
  if (attr_iter != attrs.end() && !skip_manifest) {
    is_slo = true;
    op_ret = handle_slo_manifest(attr_iter->second);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "ERROR: failed to handle slo manifest ret=" << op_ret
		       << dendl;
      goto done_err;
    }
    return;
  }

  // for range requests with obj size 0
  if (range_str && !(s->obj_size)) {
    total_len = 0;
    op_ret = -ERANGE;
    goto done_err;
  }

  op_ret = read_op.range_to_ofs(s->obj_size, ofs, end);
  if (op_ret < 0)
    goto done_err;
  total_len = (ofs <= end ? end + 1 - ofs : 0);

  /* Check whether the object has expired. Swift API documentation
   * stands that we should return 404 Not Found in such case. */
  if (need_object_expiration() && object_is_expired(attrs)) {
    op_ret = -ENOENT;
    goto done_err;
  }

  start = ofs;

  attr_iter = attrs.find(RGW_ATTR_MANIFEST);
  op_ret = this->get_decrypt_filter(&decrypt, filter,
                                    attr_iter != attrs.end() ? &(attr_iter->second) : nullptr);
  if (decrypt != nullptr) {
    filter = decrypt.get();
  }
  if (op_ret < 0) {
    goto done_err;
  }

  if (!get_data || ofs > end) {
    send_response_data(bl, 0, 0);
    return;
  }

  perfcounter->inc(l_rgw_get_b, end - ofs);

  ofs_x = ofs;
  end_x = end;
  filter->fixup_range(ofs_x, end_x);
  op_ret = read_op.iterate(ofs_x, end_x, filter);

  if (op_ret >= 0)
    op_ret = filter->flush();

  perfcounter->tinc(l_rgw_get_lat, s->time_elapsed());
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
    /* range parsed error when prefetch */
    if (!range_parsed) {
      int r = parse_range();
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
  rgw::IAM::Partition partition = rgw::IAM::Partition::aws;
  rgw::IAM::Service service = rgw::IAM::Service::s3;

  if (!verify_user_permission(this, s, ARN(partition, service, "", s->user->user_id.tenant, "*"), rgw::IAM::s3ListAllMyBuckets)) {
    return -EACCES;
  }

  return 0;
}

int RGWGetUsage::verify_permission()
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  return 0;
}

void RGWListBuckets::execute()
{
  bool done;
  bool started = false;
  uint64_t total_count = 0;

  const uint64_t max_buckets = s->cct->_conf->rgw_list_buckets_max_chunk;

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

  is_truncated = false;
  do {
    RGWUserBuckets buckets;
    uint64_t read_count;
    if (limit >= 0) {
      read_count = min(limit - total_count, max_buckets);
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
      ldpp_dout(this, 10) << "WARNING: failed on rgw_get_user_buckets uid="
			<< s->user->user_id << dendl;
      break;
    }

    /* We need to have stats for all our policies - even if a given policy
     * isn't actually used in a given account. In such situation its usage
     * stats would be simply full of zeros. */
    for (const auto& policy : store->svc.zone->get_zonegroup().placement_targets) {
      policies_stats.emplace(policy.second.name,
                             decltype(policies_stats)::mapped_type());
    }

    std::map<std::string, RGWBucketEnt>& m = buckets.get_buckets();
    for (const auto& kv : m) {
      const auto& bucket = kv.second;

      global_stats.bytes_used += bucket.size;
      global_stats.bytes_used_rounded += bucket.size_rounded;
      global_stats.objects_count += bucket.count;

      /* operator[] still can create a new entry for storage policy seen
       * for first time. */
      auto& policy_stats = policies_stats[bucket.placement_rule.to_str()];
      policy_stats.bytes_used += bucket.size;
      policy_stats.bytes_used_rounded += bucket.size_rounded;
      policy_stats.buckets_count++;
      policy_stats.objects_count += bucket.count;
    }
    global_stats.buckets_count += m.size();
    total_count += m.size();

    done = (m.size() < read_count || (limit >= 0 && total_count >= (uint64_t)limit));

    if (!started) {
      send_response_begin(buckets.count() > 0);
      started = true;
    }

    if (!m.empty()) {
      map<string, RGWBucketEnt>::reverse_iterator riter = m.rbegin();
      marker = riter->first;

      handle_listing_chunk(std::move(buckets));
    }
  } while (is_truncated && !done);

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
      ldpp_dout(this, 0) << "ERROR: failed to parse start date" << dendl;
      return;
    }
  }
    
  if (!end_date.empty()) {
    op_ret = utime_t::parse_date(end_date, &end_epoch, NULL);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "ERROR: failed to parse end date" << dendl;
      return;
    }
  }
     
  uint32_t max_entries = 1000;

  bool is_truncated = true;

  RGWUsageIter usage_iter;
  
  while (is_truncated) {
    op_ret = store->read_usage(s->user->user_id, s->bucket_name, start_epoch, end_epoch, max_entries,
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
    ldpp_dout(this, 0) << "ERROR: failed to sync user stats" << dendl;
    return;
  }

  op_ret = rgw_user_get_all_buckets_stats(store, s->user->user_id, buckets_usage);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get user's buckets stats" << dendl;
    return;
  }

  string user_str = s->user->user_id.to_str();
  op_ret = store->cls_user_get_header(user_str, &header);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: can't read user header"  << dendl;
    return;
  }
  
  return;
}

int RGWStatAccount::verify_permission()
{
  if (!verify_user_permission_no_policy(this, s, RGW_PERM_READ)) {
    return -EACCES;
  }

  return 0;
}

void RGWStatAccount::execute()
{
  string marker;
  bool is_truncated = false;
  uint64_t max_buckets = s->cct->_conf->rgw_list_buckets_max_chunk;

  do {
    RGWUserBuckets buckets;

    op_ret = rgw_read_user_buckets(store, s->user->user_id, buckets, marker,
				   string(), max_buckets, true, &is_truncated);
    if (op_ret < 0) {
      /* hmm.. something wrong here.. the user was authenticated, so it
         should exist */
      ldpp_dout(this, 10) << "WARNING: failed on rgw_get_user_buckets uid="
			<< s->user->user_id << dendl;
      break;
    } else {
      /* We need to have stats for all our policies - even if a given policy
       * isn't actually used in a given account. In such situation its usage
       * stats would be simply full of zeros. */
      for (const auto& policy : store->svc.zone->get_zonegroup().placement_targets) {
        policies_stats.emplace(policy.second.name,
                               decltype(policies_stats)::mapped_type());
      }

      std::map<std::string, RGWBucketEnt>& m = buckets.get_buckets();
      for (const auto& kv : m) {
        const auto& bucket = kv.second;

        global_stats.bytes_used += bucket.size;
        global_stats.bytes_used_rounded += bucket.size_rounded;
        global_stats.objects_count += bucket.count;

        /* operator[] still can create a new entry for storage policy seen
         * for first time. */
        auto& policy_stats = policies_stats[bucket.placement_rule.to_str()];
        policy_stats.bytes_used += bucket.size;
        policy_stats.bytes_used_rounded += bucket.size_rounded;
        policy_stats.buckets_count++;
        policy_stats.objects_count += bucket.count;
      }
      global_stats.buckets_count += m.size();

    }
  } while (is_truncated);
}

int RGWGetBucketVersioning::verify_permission()
{
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketVersioning);
}

void RGWGetBucketVersioning::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketVersioning::execute()
{
  versioned = s->bucket_info.versioned();
  versioning_enabled = s->bucket_info.versioning_enabled();
  mfa_enabled = s->bucket_info.mfa_enabled();
}

int RGWSetBucketVersioning::verify_permission()
{
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketVersioning);
}

void RGWSetBucketVersioning::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWSetBucketVersioning::execute()
{
  op_ret = get_params();
  if (op_ret < 0)
    return;

  bool cur_mfa_status = (s->bucket_info.flags & BUCKET_MFA_ENABLED) != 0;

  mfa_set_status &= (mfa_status != cur_mfa_status);

  if (mfa_set_status &&
      !s->mfa_verified) {
    op_ret = -ERR_MFA_REQUIRED;
    return;
  }

  if (!store->svc.zone->is_meta_master()) {
    op_ret = forward_request_to_master(s, NULL, store, in_data, nullptr);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
  }

  bool modified = mfa_set_status;

  op_ret = retry_raced_bucket_write(store, s, [&] {
      if (mfa_set_status) {
        if (mfa_status) {
          s->bucket_info.flags |= BUCKET_MFA_ENABLED;
        } else {
          s->bucket_info.flags &= ~BUCKET_MFA_ENABLED;
        }
      }

      if (versioning_status == VersioningEnabled) {
	s->bucket_info.flags |= BUCKET_VERSIONED;
	s->bucket_info.flags &= ~BUCKET_VERSIONS_SUSPENDED;
        modified = true;
      } else if (versioning_status == VersioningSuspended) {
	s->bucket_info.flags |= (BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED);
        modified = true;
      } else {
	return op_ret;
      }
      return store->put_bucket_instance_info(s->bucket_info, false, real_time(),
                                             &s->bucket_attrs);
    });

  if (!modified) {
    return;
  }

  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name
		     << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWGetBucketWebsite::verify_permission()
{
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketWebsite);
}

void RGWGetBucketWebsite::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketWebsite::execute()
{
  if (!s->bucket_info.has_website) {
    op_ret = -ERR_NO_SUCH_WEBSITE_CONFIGURATION;
  }
}

int RGWSetBucketWebsite::verify_permission()
{
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketWebsite);
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

  if (!store->svc.zone->is_meta_master()) {
    op_ret = forward_request_to_master(s, NULL, store, in_data, nullptr);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << " forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
  }

  op_ret = retry_raced_bucket_write(store, s, [this] {
      s->bucket_info.has_website = true;
      s->bucket_info.website_conf = website_conf;
      op_ret = store->put_bucket_instance_info(s->bucket_info, false,
					       real_time(), &s->bucket_attrs);
      return op_ret;
    });

  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name
        << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWDeleteBucketWebsite::verify_permission()
{
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3DeleteBucketWebsite);
}

void RGWDeleteBucketWebsite::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteBucketWebsite::execute()
{
  op_ret = retry_raced_bucket_write(store, s, [this] {
      s->bucket_info.has_website = false;
      s->bucket_info.website_conf = RGWBucketWebsiteConf();
      op_ret = store->put_bucket_instance_info(s->bucket_info, false,
					       real_time(), &s->bucket_attrs);
      return op_ret;
    });
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name
        << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWStatBucket::verify_permission()
{
  // This (a HEAD request on a bucket) is governed by the s3:ListBucket permission.
  if (!verify_bucket_permission(this, s, rgw::IAM::s3ListBucket)) {
    return -EACCES;
  }

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
  op_ret = get_params();
  if (op_ret < 0) {
    return op_ret;
  }
  if (!prefix.empty())
    s->env.emplace("s3:prefix", prefix);

  if (!delimiter.empty())
    s->env.emplace("s3:delimiter", delimiter);

  s->env.emplace("s3:max-keys", std::to_string(max));

  if (!verify_bucket_permission(this,
                                s,
				list_versions ?
				rgw::IAM::s3ListBucketVersions :
				rgw::IAM::s3ListBucket)) {
    return -EACCES;
  }

  return 0;
}

int RGWListBucket::parse_max_keys()
{
  // Bound max value of max-keys to configured value for security
  // Bound min value of max-keys to '0'
  // Some S3 clients explicitly send max-keys=0 to detect if the bucket is
  // empty without listing any items.
  return parse_value_and_bound(max_keys, max, 0,
			g_conf().get_val<uint64_t>("rgw_max_listing_results"),
			default_max);
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

  if (allow_unordered && !delimiter.empty()) {
    ldpp_dout(this, 0) <<
      "ERROR: unordered bucket listing requested with a delimiter" << dendl;
    op_ret = -EINVAL;
    return;
  }

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
  list_op.params.allow_unordered = allow_unordered;

  op_ret = list_op.list_objects(max, &objs, &common_prefixes, &is_truncated);
  if (op_ret >= 0) {
    next_marker = list_op.get_next_marker();
  }
}

int RGWGetBucketLogging::verify_permission()
{
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketLogging);
}

int RGWGetBucketLocation::verify_permission()
{
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketLocation);
}

int RGWCreateBucket::verify_permission()
{
  /* This check is mostly needed for S3 that doesn't support account ACL.
   * Swift doesn't allow to delegate any permission to an anonymous user,
   * so it will become an early exit in such case. */
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  rgw_bucket bucket;
  bucket.name = s->bucket_name;
  bucket.tenant = s->bucket_tenant;
  ARN arn = ARN(bucket);
  if (!verify_user_permission(this, s, arn, rgw::IAM::s3CreateBucket)) {
    return -EACCES;
  }

  if (s->user->user_id.tenant != s->bucket_tenant) {
    ldpp_dout(this, 10) << "user cannot create a bucket in a different tenant"
                      << " (user_id.tenant=" << s->user->user_id.tenant
                      << " requested=" << s->bucket_tenant << ")"
                      << dendl;
    return -EACCES;
  }
  if (s->user->max_buckets < 0) {
    return -EPERM;
  }

  if (s->user->max_buckets) {
    RGWUserBuckets buckets;
    string marker;
    bool is_truncated = false;
    op_ret = rgw_read_user_buckets(store, s->user->user_id, buckets,
				   marker, string(), s->user->max_buckets,
				   false, &is_truncated);
    if (op_ret < 0) {
      return op_ret;
    }

    if ((int)buckets.count() >= s->user->max_buckets) {
      return -ERR_TOO_MANY_BUCKETS;
    }
  }

  return 0;
}

static int forward_request_to_master(struct req_state *s, obj_version *objv,
				    RGWRados *store, bufferlist& in_data,
				    JSONParser *jp, req_info *forward_info)
{
  if (!store->svc.zone->get_master_conn()) {
    ldpp_dout(s, 0) << "rest connection is invalid" << dendl;
    return -EINVAL;
  }
  ldpp_dout(s, 0) << "sending request to master zonegroup" << dendl;
  bufferlist response;
  string uid_str = s->user->user_id.to_str();
#define MAX_REST_RESPONSE (128 * 1024) // we expect a very small response
  int ret = store->svc.zone->get_master_conn()->forward(uid_str, (forward_info ? *forward_info : s->info),
                                                        objv, MAX_REST_RESPONSE, &in_data, &response);
  if (ret < 0)
    return ret;

  ldpp_dout(s, 20) << "response: " << response.c_str() << dendl;
  if (jp && !jp->parse(response.c_str(), response.length())) {
    ldpp_dout(s, 0) << "failed parsing response from master zonegroup" << dendl;
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
  for (const auto& kv : orig_attrs) {
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

/* Fuse resource metadata basing on original attributes in @orig_attrs, set
 * of _custom_ attribute names to remove in @rmattr_names and attributes in
 * @out_attrs. Place results in @out_attrs.
 *
 * NOTE: it's supposed that all special attrs already present in @out_attrs
 * will be preserved without any change. Special attributes are those which
 * names start with RGW_ATTR_META_PREFIX. They're complement to custom ones
 * used for X-Account-Meta-*, X-Container-Meta-*, X-Amz-Meta and so on.  */
static void prepare_add_del_attrs(const map<string, bufferlist>& orig_attrs,
                                  const set<string>& rmattr_names,
                                  map<string, bufferlist>& out_attrs)
{
  for (const auto& kv : orig_attrs) {
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
      } else {
        /* emplace() won't alter the map if the key is already present.
         * This behaviour is fully intensional here. */
        out_attrs.emplace(kv);
      }
    } else if (out_attrs.find(name) == std::end(out_attrs)) {
      out_attrs[name] = kv.second;
    }
  }
}


static void populate_with_generic_attrs(const req_state * const s,
                                        map<string, bufferlist>& out_attrs)
{
  for (const auto& kv : s->generic_attrs) {
    bufferlist& attrbl = out_attrs[kv.first];
    const string& val = kv.second;
    attrbl.clear();
    attrbl.append(val.c_str(), val.size() + 1);
  }
}


static int filter_out_quota_info(std::map<std::string, bufferlist>& add_attrs,
                                 const std::set<std::string>& rmattr_names,
                                 RGWQuotaInfo& quota,
                                 bool * quota_extracted = nullptr)
{
  bool extracted = false;

  /* Put new limit on max objects. */
  auto iter = add_attrs.find(RGW_ATTR_QUOTA_NOBJS);
  std::string err;
  if (std::end(add_attrs) != iter) {
    quota.max_objects =
      static_cast<int64_t>(strict_strtoll(iter->second.c_str(), 10, &err));
    if (!err.empty()) {
      return -EINVAL;
    }
    add_attrs.erase(iter);
    extracted = true;
  }

  /* Put new limit on bucket (container) size. */
  iter = add_attrs.find(RGW_ATTR_QUOTA_MSIZE);
  if (iter != add_attrs.end()) {
    quota.max_size =
      static_cast<int64_t>(strict_strtoll(iter->second.c_str(), 10, &err));
    if (!err.empty()) {
      return -EINVAL;
    }
    add_attrs.erase(iter);
    extracted = true;
  }

  for (const auto& name : rmattr_names) {
    /* Remove limit on max objects. */
    if (name.compare(RGW_ATTR_QUOTA_NOBJS) == 0) {
      quota.max_objects = -1;
      extracted = true;
    }

    /* Remove limit on max bucket size. */
    if (name.compare(RGW_ATTR_QUOTA_MSIZE) == 0) {
      quota.max_size = -1;
      extracted = true;
    }
  }

  /* Swift requries checking on raw usage instead of the 4 KiB rounded one. */
  quota.check_on_raw = true;
  quota.enabled = quota.max_size > 0 || quota.max_objects > 0;

  if (quota_extracted) {
    *quota_extracted = extracted;
  }

  return 0;
}


static void filter_out_website(std::map<std::string, ceph::bufferlist>& add_attrs,
                               const std::set<std::string>& rmattr_names,
                               RGWBucketWebsiteConf& ws_conf)
{
  std::string lstval;

  /* Let's define a mapping between each custom attribute and the memory where
   * attribute's value should be stored. The memory location is expressed by
   * a non-const reference. */
  const auto mapping  = {
    std::make_pair(RGW_ATTR_WEB_INDEX,     std::ref(ws_conf.index_doc_suffix)),
    std::make_pair(RGW_ATTR_WEB_ERROR,     std::ref(ws_conf.error_doc)),
    std::make_pair(RGW_ATTR_WEB_LISTINGS,  std::ref(lstval)),
    std::make_pair(RGW_ATTR_WEB_LIST_CSS,  std::ref(ws_conf.listing_css_doc)),
    std::make_pair(RGW_ATTR_SUBDIR_MARKER, std::ref(ws_conf.subdir_marker))
  };

  for (const auto& kv : mapping) {
    const char * const key = kv.first;
    auto& target = kv.second;

    auto iter = add_attrs.find(key);

    if (std::end(add_attrs) != iter) {
      /* The "target" is a reference to ws_conf. */
      target = iter->second.c_str();
      add_attrs.erase(iter);
    }

    if (rmattr_names.count(key)) {
      target = std::string();
    }
  }

  if (! lstval.empty()) {
    ws_conf.listing_enabled = boost::algorithm::iequals(lstval, "true");
  }
}


void RGWCreateBucket::execute()
{
  RGWAccessControlPolicy old_policy(s->cct);
  buffer::list aclbl;
  buffer::list corsbl;
  bool existed;
  string bucket_name;
  rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name, bucket_name);
  rgw_raw_obj obj(store->svc.zone->get_zone_params().domain_root, bucket_name);
  obj_version objv, *pobjv = NULL;

  op_ret = get_params();
  if (op_ret < 0)
    return;

  if (!relaxed_region_enforcement &&
      !location_constraint.empty() &&
      !store->svc.zone->has_zonegroup_api(location_constraint)) {
      ldpp_dout(this, 0) << "location constraint (" << location_constraint << ")"
                       << " can't be found." << dendl;
      op_ret = -ERR_INVALID_LOCATION_CONSTRAINT;
      s->err.message = "The specified location-constraint is not valid";
      return;
  }

  if (!relaxed_region_enforcement && !store->svc.zone->get_zonegroup().is_master_zonegroup() && !location_constraint.empty() &&
      store->svc.zone->get_zonegroup().api_name != location_constraint) {
    ldpp_dout(this, 0) << "location constraint (" << location_constraint << ")"
                     << " doesn't match zonegroup" << " (" << store->svc.zone->get_zonegroup().api_name << ")"
                     << dendl;
    op_ret = -ERR_INVALID_LOCATION_CONSTRAINT;
    s->err.message = "The specified location-constraint is not valid";
    return;
  }

  const auto& zonegroup = store->svc.zone->get_zonegroup();
  if (!placement_rule.name.empty() &&
      !zonegroup.placement_targets.count(placement_rule.name)) {
    ldpp_dout(this, 0) << "placement target (" << placement_rule.name << ")"
                     << " doesn't exist in the placement targets of zonegroup"
                     << " (" << store->svc.zone->get_zonegroup().api_name << ")" << dendl;
    op_ret = -ERR_INVALID_LOCATION_CONSTRAINT;
    s->err.message = "The specified placement target does not exist";
    return;
  }

  /* we need to make sure we read bucket info, it's not read before for this
   * specific request */
  op_ret = store->get_bucket_info(*s->sysobj_ctx, s->bucket_tenant, s->bucket_name,
				  s->bucket_info, nullptr, &s->bucket_attrs);
  if (op_ret < 0 && op_ret != -ENOENT)
    return;
  s->bucket_exists = (op_ret != -ENOENT);

  s->bucket_owner.set_id(s->user->user_id);
  s->bucket_owner.set_name(s->user->display_name);
  if (s->bucket_exists) {
    int r = rgw_op_get_bucket_policy_from_attr(s->cct, store, s->bucket_info,
                                               s->bucket_attrs, &old_policy);
    if (r >= 0)  {
      if (old_policy.get_owner().get_id().compare(s->user->user_id) != 0) {
        op_ret = -EEXIST;
        return;
      }
    }
  }

  RGWBucketInfo master_info;
  rgw_bucket *pmaster_bucket;
  uint32_t *pmaster_num_shards;
  real_time creation_time;

  if (!store->svc.zone->is_meta_master()) {
    JSONParser jp;
    op_ret = forward_request_to_master(s, NULL, store, in_data, &jp);
    if (op_ret < 0) {
      return;
    }

    JSONDecoder::decode_json("entry_point_object_ver", ep_objv, &jp);
    JSONDecoder::decode_json("object_ver", objv, &jp);
    JSONDecoder::decode_json("bucket_info", master_info, &jp);
    ldpp_dout(this, 20) << "parsed: objv.tag=" << objv.tag << " objv.ver=" << objv.ver << dendl;
    ldpp_dout(this, 20) << "got creation time: << " << master_info.creation_time << dendl;
    pmaster_bucket= &master_info.bucket;
    creation_time = master_info.creation_time;
    pmaster_num_shards = &master_info.num_shards;
    pobjv = &objv;
  } else {
    pmaster_bucket = NULL;
    pmaster_num_shards = NULL;
  }

  string zonegroup_id;

  if (s->system_request) {
    zonegroup_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "zonegroup");
    if (zonegroup_id.empty()) {
      zonegroup_id = store->svc.zone->get_zonegroup().get_id();
    }
  } else {
    zonegroup_id = store->svc.zone->get_zonegroup().get_id();
  }

  if (s->bucket_exists) {
    rgw_placement_rule selected_placement_rule;
    rgw_bucket bucket;
    bucket.tenant = s->bucket_tenant;
    bucket.name = s->bucket_name;
    op_ret = store->svc.zone->select_bucket_placement(*(s->user), zonegroup_id,
					    placement_rule,
					    &selected_placement_rule, nullptr);
    if (selected_placement_rule != s->bucket_info.placement_rule) {
      op_ret = -EEXIST;
      return;
    }
  }

  /* Encode special metadata first as we're using std::map::emplace under
   * the hood. This method will add the new items only if the map doesn't
   * contain such keys yet. */
  policy.encode(aclbl);
  emplace_attr(RGW_ATTR_ACL, std::move(aclbl));

  if (has_cors) {
    cors_config.encode(corsbl);
    emplace_attr(RGW_ATTR_CORS, std::move(corsbl));
  }

  RGWQuotaInfo quota_info;
  const RGWQuotaInfo * pquota_info = nullptr;
  if (need_metadata_upload()) {
    /* It's supposed that following functions WILL NOT change any special
     * attributes (like RGW_ATTR_ACL) if they are already present in attrs. */
    op_ret = rgw_get_request_metadata(s->cct, s->info, attrs, false);
    if (op_ret < 0) {
      return;
    }
    prepare_add_del_attrs(s->bucket_attrs, rmattr_names, attrs);
    populate_with_generic_attrs(s, attrs);

    op_ret = filter_out_quota_info(attrs, rmattr_names, quota_info);
    if (op_ret < 0) {
      return;
    } else {
      pquota_info = &quota_info;
    }

    /* Web site of Swift API. */
    filter_out_website(attrs, rmattr_names, s->bucket_info.website_conf);
    s->bucket_info.has_website = !s->bucket_info.website_conf.is_empty();
  }

  s->bucket.tenant = s->bucket_tenant; /* ignored if bucket exists */
  s->bucket.name = s->bucket_name;

  /* Handle updates of the metadata for Swift's object versioning. */
  if (swift_ver_location) {
    s->bucket_info.swift_ver_location = *swift_ver_location;
    s->bucket_info.swift_versioning = (! swift_ver_location->empty());
  }

  op_ret = store->create_bucket(*(s->user), s->bucket, zonegroup_id,
                                placement_rule, s->bucket_info.swift_ver_location,
                                pquota_info, attrs,
                                info, pobjv, &ep_objv, creation_time,
                                pmaster_bucket, pmaster_num_shards, true);
  /* continue if EEXIST and create_bucket will fail below.  this way we can
   * recover from a partial create by retrying it. */
  ldpp_dout(this, 20) << "rgw_create_bucket returned ret=" << op_ret << " bucket=" << s->bucket << dendl;

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
      ldpp_dout(this, 0) << "WARNING: failed to unlink bucket: ret=" << op_ret
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
      RGWBucketInfo binfo;
      map<string, bufferlist> battrs;

      op_ret = store->get_bucket_info(*s->sysobj_ctx, s->bucket_tenant, s->bucket_name,
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

      op_ret = rgw_get_request_metadata(s->cct, s->info, attrs, false);
      if (op_ret < 0) {
        return;
      }
      prepare_add_del_attrs(s->bucket_attrs, rmattr_names, attrs);
      populate_with_generic_attrs(s, attrs);
      op_ret = filter_out_quota_info(attrs, rmattr_names, s->bucket_info.quota);
      if (op_ret < 0) {
        return;
      }

      /* Handle updates of the metadata for Swift's object versioning. */
      if (swift_ver_location) {
        s->bucket_info.swift_ver_location = *swift_ver_location;
        s->bucket_info.swift_versioning = (! swift_ver_location->empty());
      }

      /* Web site of Swift API. */
      filter_out_website(attrs, rmattr_names, s->bucket_info.website_conf);
      s->bucket_info.has_website = !s->bucket_info.website_conf.is_empty();

      /* This will also set the quota on the bucket. */
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
  if (!verify_bucket_permission(this, s, rgw::IAM::s3DeleteBucket)) {
    return -EACCES;
  }

  return 0;
}

void RGWDeleteBucket::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteBucket::execute()
{
  if (s->bucket_name.empty()) {
    op_ret = -EINVAL;
    return;
  }

  if (!s->bucket_exists) {
    ldpp_dout(this, 0) << "ERROR: bucket " << s->bucket_name << " not found" << dendl;
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
        ldpp_dout(this, 0) << "failed to parse ver param" << dendl;
        op_ret = -EINVAL;
        return;
      }
      ot.read_version.ver = ver;
    }
  }

  op_ret = rgw_bucket_sync_user_stats(store, s->user->user_id, s->bucket_info);
  if ( op_ret < 0) {
     ldpp_dout(this, 1) << "WARNING: failed to sync user stats before bucket delete: op_ret= " << op_ret << dendl;
  }
  
  op_ret = store->check_bucket_empty(s->bucket_info);
  if (op_ret < 0) {
    return;
  }

  if (!store->svc.zone->is_meta_master()) {
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

  string prefix, delimiter;

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

  op_ret = abort_bucket_multiparts(store, s->cct, s->bucket_info, prefix, delimiter);

  if (op_ret < 0) {
    return;
  }

  op_ret = store->delete_bucket(s->bucket_info, ot, false);

  if (op_ret == -ECANCELED) {
    // lost a race, either with mdlog sync or another delete bucket operation.
    // in either case, we've already called rgw_unlink_bucket()
    op_ret = 0;
    return;
  }

  if (op_ret == 0) {
    op_ret = rgw_unlink_bucket(store, s->bucket_info.owner, s->bucket.tenant,
			       s->bucket.name, false);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "WARNING: failed to unlink bucket: ret=" << op_ret
		       << dendl;
    }
  }
}

int RGWPutObj::verify_permission()
{
  if (! copy_source.empty()) {

    RGWAccessControlPolicy cs_acl(s->cct);
    boost::optional<Policy> policy;
    map<string, bufferlist> cs_attrs;
    rgw_bucket cs_bucket(copy_source_bucket_info.bucket);
    rgw_obj_key cs_object(copy_source_object_name, copy_source_version_id);

    rgw_obj obj(cs_bucket, cs_object);
    store->set_atomic(s->obj_ctx, obj);
    store->set_prefetch_data(s->obj_ctx, obj);

    /* check source object permissions */
    if (read_obj_policy(store, s, copy_source_bucket_info, cs_attrs, &cs_acl, nullptr,
			policy, cs_bucket, cs_object) < 0) {
      return -EACCES;
    }

    /* admin request overrides permission checks */
    if (! s->auth.identity->is_admin_of(cs_acl.get_owner().get_id())) {
      if (policy || ! s->iam_user_policies.empty()) {
        auto usr_policy_res = Effect::Pass;
        for (auto& user_policy : s->iam_user_policies) {
          if (usr_policy_res = user_policy.eval(s->env, *s->auth.identity,
			      cs_object.instance.empty() ?
			      rgw::IAM::s3GetObject :
			      rgw::IAM::s3GetObjectVersion,
			      rgw::IAM::ARN(obj)); usr_policy_res == Effect::Deny)
            return -EACCES;
          else if (usr_policy_res == Effect::Allow)
            break;
        }
  rgw::IAM::Effect e = Effect::Pass;
  if (policy) {
	  e = policy->eval(s->env, *s->auth.identity,
			      cs_object.instance.empty() ?
			      rgw::IAM::s3GetObject :
			      rgw::IAM::s3GetObjectVersion,
			      rgw::IAM::ARN(obj));
  }
	if (e == Effect::Deny) {
	  return -EACCES; 
	} else if (usr_policy_res == Effect::Pass && e == Effect::Pass &&
		   !cs_acl.verify_permission(this, *s->auth.identity, s->perm_mask,
						RGW_PERM_READ)) {
	  return -EACCES;
	}
      } else if (!cs_acl.verify_permission(this, *s->auth.identity, s->perm_mask,
					   RGW_PERM_READ)) {
	return -EACCES;
      }
    }
  }

  auto op_ret = get_params();
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "get_params() returned ret=" << op_ret << dendl;
    return op_ret;
  }

  if (s->iam_policy || ! s->iam_user_policies.empty()) {
    rgw_add_grant_to_iam_environment(s->env, s);

    rgw_add_to_iam_environment(s->env, "s3:x-amz-acl", s->canned_acl);

    if (obj_tags != nullptr && obj_tags->count() > 0){
      auto tags = obj_tags->get_tags();
      for (const auto& kv: tags){
        rgw_add_to_iam_environment(s->env, "s3:RequestObjectTag/"+kv.first, kv.second);
      }
    }

    constexpr auto encrypt_attr = "x-amz-server-side-encryption";
    constexpr auto s3_encrypt_attr = "s3:x-amz-server-side-encryption";
    auto enc_header = s->info.x_meta_map.find(encrypt_attr);
    if (enc_header != s->info.x_meta_map.end()){
      rgw_add_to_iam_environment(s->env, s3_encrypt_attr, enc_header->second);
    }

    constexpr auto kms_attr = "x-amz-server-side-encryption-aws-kms-key-id";
    constexpr auto s3_kms_attr = "s3:x-amz-server-side-encryption-aws-kms-key-id";
    auto kms_header = s->info.x_meta_map.find(kms_attr);
    if (kms_header != s->info.x_meta_map.end()){
      rgw_add_to_iam_environment(s->env, s3_kms_attr, kms_header->second);
    }

    auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env,
                                            boost::none,
                                            rgw::IAM::s3PutObject,
                                            rgw_obj(s->bucket, s->object));
    if (usr_policy_res == Effect::Deny)
      return -EACCES;

    rgw::IAM::Effect e = Effect::Pass;
    if (s->iam_policy) {
      e = s->iam_policy->eval(s->env, *s->auth.identity,
          rgw::IAM::s3PutObject,
          rgw_obj(s->bucket, s->object));
    }
    if (e == Effect::Allow) {
      return 0;
    } else if (e == Effect::Deny) {
      return -EACCES;
    } else if (usr_policy_res == Effect::Allow) {
      return 0;
    }
  }

  if (!verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

  return 0;
}


void RGWPutObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

class RGWPutObj_CB : public RGWGetObj_Filter
{
  RGWPutObj *op;
public:
  explicit RGWPutObj_CB(RGWPutObj *_op) : op(_op) {}
  ~RGWPutObj_CB() override {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    return op->get_data_cb(bl, bl_ofs, bl_len);
  }
};

int RGWPutObj::get_data_cb(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  bufferlist bl_tmp;
  bl.copy(bl_ofs, bl_len, bl_tmp);

  bl_aux.append(bl_tmp);

  return bl_len;
}

int RGWPutObj::get_data(const off_t fst, const off_t lst, bufferlist& bl)
{
  RGWPutObj_CB cb(this);
  RGWGetObj_Filter* filter = &cb;
  boost::optional<RGWGetObj_Decompress> decompress;
  std::unique_ptr<RGWGetObj_Filter> decrypt;
  RGWCompressionInfo cs_info;
  map<string, bufferlist> attrs;
  map<string, bufferlist>::iterator attr_iter;
  int ret = 0;

  uint64_t obj_size;
  int64_t new_ofs, new_end;

  new_ofs = fst;
  new_end = lst;

  rgw_obj_key obj_key(copy_source_object_name, copy_source_version_id);
  rgw_obj obj(copy_source_bucket_info.bucket, obj_key);

  RGWRados::Object op_target(store, copy_source_bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target);
  read_op.params.obj_size = &obj_size;
  read_op.params.attrs = &attrs;

  ret = read_op.prepare();
  if (ret < 0)
    return ret;

  bool need_decompress;
  op_ret = rgw_compression_info_from_attrset(attrs, need_decompress, cs_info);
  if (op_ret < 0) {
    ldpp_dout(s, 0) << "ERROR: failed to decode compression info" << dendl;
    return -EIO;
  }

  bool partial_content = true;
  if (need_decompress)
  {
    obj_size = cs_info.orig_size;
    decompress.emplace(s->cct, &cs_info, partial_content, filter);
    filter = &*decompress;
  }

  attr_iter = attrs.find(RGW_ATTR_MANIFEST);
  op_ret = this->get_decrypt_filter(&decrypt,
                                    filter,
                                    attrs,
                                    attr_iter != attrs.end() ? &(attr_iter->second) : nullptr);
  if (decrypt != nullptr) {
    filter = decrypt.get();
  }
  if (op_ret < 0) {
    return ret;
  }

  ret = read_op.range_to_ofs(obj_size, new_ofs, new_end);
  if (ret < 0)
    return ret;

  filter->fixup_range(new_ofs, new_end);
  ret = read_op.iterate(new_ofs, new_end, filter);

  if (ret >= 0)
    ret = filter->flush();

  bl.claim_append(bl_aux);

  return ret;
}

// special handling for compression type = "random" with multipart uploads
static CompressorRef get_compressor_plugin(const req_state *s,
                                           const std::string& compression_type)
{
  if (compression_type != "random") {
    return Compressor::create(s->cct, compression_type);
  }

  bool is_multipart{false};
  const auto& upload_id = s->info.args.get("uploadId", &is_multipart);

  if (!is_multipart) {
    return Compressor::create(s->cct, compression_type);
  }

  // use a hash of the multipart upload id so all parts use the same plugin
  const auto alg = std::hash<std::string>{}(upload_id) % Compressor::COMP_ALG_LAST;
  if (alg == Compressor::COMP_ALG_NONE) {
    return nullptr;
  }
  return Compressor::create(s->cct, alg);
}

void RGWPutObj::execute()
{
  char supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1];
  char supplied_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  bufferlist bl, aclbl, bs;
  int len;
  
  off_t fst;
  off_t lst;

  bool need_calc_md5 = (dlo_manifest == NULL) && (slo_info == NULL);
  perfcounter->inc(l_rgw_put);
  // report latency on return
  auto put_lat = make_scope_guard([&] {
      perfcounter->tinc(l_rgw_put_lat, s->time_elapsed());
    });

  op_ret = -EINVAL;
  if (s->object.empty()) {
    return;
  }

  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }


  op_ret = get_system_versioning_params(s, &olh_epoch, &version_id);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "get_system_versioning_params() returned ret="
		      << op_ret << dendl;
    return;
  }

  if (supplied_md5_b64) {
    need_calc_md5 = true;

    ldpp_dout(this, 15) << "supplied_md5_b64=" << supplied_md5_b64 << dendl;
    op_ret = ceph_unarmor(supplied_md5_bin, &supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1],
                       supplied_md5_b64, supplied_md5_b64 + strlen(supplied_md5_b64));
    ldpp_dout(this, 15) << "ceph_armor ret=" << op_ret << dendl;
    if (op_ret != CEPH_CRYPTO_MD5_DIGESTSIZE) {
      op_ret = -ERR_INVALID_DIGEST;
      return;
    }

    buf_to_hex((const unsigned char *)supplied_md5_bin, CEPH_CRYPTO_MD5_DIGESTSIZE, supplied_md5);
    ldpp_dout(this, 15) << "supplied_md5=" << supplied_md5 << dendl;
  }

  if (!chunked_upload) { /* with chunked upload we don't know how big is the upload.
                            we also check sizes at the end anyway */
    op_ret = store->check_quota(s->bucket_owner.get_id(), s->bucket,
				user_quota, bucket_quota, s->content_length);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "check_quota() returned ret=" << op_ret << dendl;
      return;
    }
    op_ret = store->check_bucket_shards(s->bucket_info, s->bucket, bucket_quota);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "check_bucket_shards() returned ret=" << op_ret << dendl;
      return;
    }
  }

  if (supplied_etag) {
    strncpy(supplied_md5, supplied_etag, sizeof(supplied_md5) - 1);
    supplied_md5[sizeof(supplied_md5) - 1] = '\0';
  }

  const bool multipart = !multipart_upload_id.empty();
  auto& obj_ctx = *static_cast<RGWObjectCtx*>(s->obj_ctx);
  rgw_obj obj{s->bucket, s->object};

  /* Handle object versioning of Swift API. */
  if (! multipart) {
    op_ret = store->swift_versioning_copy(obj_ctx,
                                          s->bucket_owner.get_id(),
                                          s->bucket_info,
                                          obj);
    if (op_ret < 0) {
      return;
    }
  }

  // create the object processor
  rgw::AioThrottle aio(store->ctx()->_conf->rgw_put_obj_min_window_size);
  using namespace rgw::putobj;
  constexpr auto max_processor_size = std::max({sizeof(MultipartObjectProcessor),
                                               sizeof(AtomicObjectProcessor),
                                               sizeof(AppendObjectProcessor)});
  ceph::static_ptr<ObjectProcessor, max_processor_size> processor;

  rgw_placement_rule *pdest_placement;

  if (multipart) {
    RGWMPObj mp(s->object.name, multipart_upload_id);

    multipart_upload_info upload_info;
    op_ret = get_multipart_info(store, s, mp.get_meta(), nullptr, nullptr, &upload_info);
    if (op_ret < 0) {
      if (op_ret != -ENOENT) {
        ldpp_dout(this, 0) << "ERROR: get_multipart_info returned " << op_ret << ": " << cpp_strerror(-op_ret) << dendl;
      } else {// -ENOENT: raced with upload complete/cancel, no need to spam log
        ldpp_dout(this, 20) << "failed to get multipart info (returned " << op_ret << ": " << cpp_strerror(-op_ret) << "): probably raced with upload complete / cancel" << dendl;
      }
      return;
    }
    pdest_placement = &upload_info.dest_placement;
    ldpp_dout(this, 20) << "dest_placement for part=" << upload_info.dest_placement << dendl;
    processor.emplace<MultipartObjectProcessor>(
        &aio, store, s->bucket_info, pdest_placement,
        s->owner.get_id(), obj_ctx, obj,
        multipart_upload_id, multipart_part_num, multipart_part_str);
  } else if(append) {
    if (s->bucket_info.versioned()) {
      op_ret = -ERR_INVALID_BUCKET_STATE;
      return;
    }
    pdest_placement = &s->dest_placement;
    processor.emplace<AppendObjectProcessor>(
            &aio, store, s->bucket_info, pdest_placement, s->bucket_owner.get_id(),obj_ctx, obj,
            s->req_id, position, &cur_accounted_size);
  } else {
    if (s->bucket_info.versioning_enabled()) {
      if (!version_id.empty()) {
        obj.key.set_instance(version_id);
      } else {
        store->gen_rand_obj_instance_name(&obj);
        version_id = obj.key.instance;
      }
    }
    pdest_placement = &s->dest_placement;
    processor.emplace<AtomicObjectProcessor>(
        &aio, store, s->bucket_info, pdest_placement,
        s->bucket_owner.get_id(), obj_ctx, obj, olh_epoch, s->req_id);
  }

  op_ret = processor->prepare();
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "processor->prepare() returned ret=" << op_ret
		      << dendl;
    return;
  }

  if ((! copy_source.empty()) && !copy_source_range) {
    rgw_obj_key obj_key(copy_source_object_name, copy_source_version_id);
    rgw_obj obj(copy_source_bucket_info.bucket, obj_key.name);

    RGWObjState *astate;
    op_ret = store->get_obj_state(&obj_ctx, copy_source_bucket_info, obj,
                                  &astate, true, false);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "ERROR: get copy source obj state returned with error" << op_ret << dendl;
      return;
    }
    if (!astate->exists){
      op_ret = -ENOENT;
      return;
    }
    lst = astate->accounted_size - 1;
  } else {
    lst = copy_source_range_lst;
  }

  fst = copy_source_range_fst;

  // no filters by default
  DataProcessor *filter = processor.get();

  const auto& compression_type = store->svc.zone->get_zone_params().get_compression_type(*pdest_placement);
  CompressorRef plugin;
  boost::optional<RGWPutObj_Compress> compressor;

  std::unique_ptr<DataProcessor> encrypt;
  op_ret = get_encrypt_filter(&encrypt, filter);
  if (op_ret < 0) {
    return;
  }
  if (encrypt != nullptr) {
    filter = &*encrypt;
  } else if (compression_type != "none") {
    plugin = get_compressor_plugin(s, compression_type);
    if (!plugin) {
      ldpp_dout(this, 1) << "Cannot load plugin for compression type "
          << compression_type << dendl;
    } else {
      compressor.emplace(s->cct, plugin, filter);
      filter = &*compressor;
    }
  }
  tracepoint(rgw_op, before_data_transfer, s->req_id.c_str());
  do {
    bufferlist data;
    if (fst > lst)
      break;
    if (copy_source.empty()) {
      len = get_data(data);
    } else {
      uint64_t cur_lst = min(fst + s->cct->_conf->rgw_max_chunk_size - 1, lst);
      op_ret = get_data(fst, cur_lst, data);
      if (op_ret < 0)
        return;
      len = data.length();
      s->content_length += len;
      fst += len;
    }
    if (len < 0) {
      op_ret = len;
      ldpp_dout(this, 20) << "get_data() returned ret=" << op_ret << dendl;
      return;
    } else if (len == 0) {
      break;
    }

    if (need_calc_md5) {
      hash.Update((const unsigned char *)data.c_str(), data.length());
    }

    /* update torrrent */
    torrent.update(data);

    op_ret = filter->process(std::move(data), ofs);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "processor->process() returned ret="
          << op_ret << dendl;
      return;
    }

    ofs += len;
  } while (len > 0);
  tracepoint(rgw_op, after_data_transfer, s->req_id.c_str(), ofs);

  // flush any data in filters
  op_ret = filter->process({}, ofs);
  if (op_ret < 0) {
    return;
  }

  if (!chunked_upload && ofs != s->content_length) {
    op_ret = -ERR_REQUEST_TIMEOUT;
    return;
  }
  s->obj_size = ofs;

  perfcounter->inc(l_rgw_put_b, s->obj_size);

  op_ret = do_aws4_auth_completion();
  if (op_ret < 0) {
    return;
  }

  op_ret = store->check_quota(s->bucket_owner.get_id(), s->bucket,
                              user_quota, bucket_quota, s->obj_size);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "second check_quota() returned op_ret=" << op_ret << dendl;
    return;
  }

  op_ret = store->check_bucket_shards(s->bucket_info, s->bucket, bucket_quota);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "check_bucket_shards() returned ret=" << op_ret << dendl;
    return;
  }

  hash.Final(m);

  if (compressor && compressor->is_compressed()) {
    bufferlist tmp;
    RGWCompressionInfo cs_info;
    cs_info.compression_type = plugin->get_type_name();
    cs_info.orig_size = s->obj_size;
    cs_info.blocks = move(compressor->get_compression_blocks());
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
    ldpp_dout(this, 20) << "storing " << RGW_ATTR_COMPRESSION
        << " with type=" << cs_info.compression_type
        << ", orig_size=" << cs_info.orig_size
        << ", blocks=" << cs_info.blocks.size() << dendl;
  }

  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);

  etag = calc_md5;

  if (supplied_md5_b64 && strcmp(calc_md5, supplied_md5)) {
    op_ret = -ERR_BAD_DIGEST;
    return;
  }

  policy.encode(aclbl);
  emplace_attr(RGW_ATTR_ACL, std::move(aclbl));

  if (dlo_manifest) {
    op_ret = encode_dlo_manifest_attr(dlo_manifest, attrs);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "bad user manifest: " << dlo_manifest << dendl;
      return;
    }
    complete_etag(hash, &etag);
    ldpp_dout(this, 10) << __func__ << ": calculated md5 for user manifest: " << etag << dendl;
  }

  if (slo_info) {
    bufferlist manifest_bl;
    encode(*slo_info, manifest_bl);
    emplace_attr(RGW_ATTR_SLO_MANIFEST, std::move(manifest_bl));

    hash.Update((unsigned char *)(slo_info->raw_data.c_str()), slo_info->raw_data.length());
    complete_etag(hash, &etag);
    ldpp_dout(this, 10) << __func__ << ": calculated md5 for user manifest: " << etag << dendl;
  }

  if (supplied_etag && etag.compare(supplied_etag) != 0) {
    op_ret = -ERR_UNPROCESSABLE_ENTITY;
    return;
  }
  bl.append(etag.c_str(), etag.size());
  emplace_attr(RGW_ATTR_ETAG, std::move(bl));

  populate_with_generic_attrs(s, attrs);
  op_ret = rgw_get_request_metadata(s->cct, s->info, attrs);
  if (op_ret < 0) {
    return;
  }
  encode_delete_at_attr(delete_at, attrs);
  encode_obj_tags_attr(obj_tags.get(), attrs);

  /* Add a custom metadata to expose the information whether an object
   * is an SLO or not. Appending the attribute must be performed AFTER
   * processing any input from user in order to prohibit overwriting. */
  if (slo_info) {
    bufferlist slo_userindicator_bl;
    slo_userindicator_bl.append("True", 4);
    emplace_attr(RGW_ATTR_SLO_UINDICATOR, std::move(slo_userindicator_bl));
  }

  tracepoint(rgw_op, processor_complete_enter, s->req_id.c_str());
  op_ret = processor->complete(s->obj_size, etag, &mtime, real_time(), attrs,
                               (delete_at ? *delete_at : real_time()), if_match, if_nomatch,
                               (user_data.empty() ? nullptr : &user_data), nullptr, nullptr);
  tracepoint(rgw_op, processor_complete_exit, s->req_id.c_str());

  /* produce torrent */
  if (s->cct->_conf->rgw_torrent_flag && (ofs == torrent.get_data_len()))
  {
    torrent.init(s, store);
    torrent.set_create_date(mtime);
    op_ret =  torrent.complete();
    if (0 != op_ret)
    {
      ldpp_dout(this, 0) << "ERROR: torrent.handle_data() returned " << op_ret << dendl;
      return;
    }
  }
}

int RGWPostObj::verify_permission()
{
  return 0;
}

void RGWPostObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPostObj::execute()
{
  boost::optional<RGWPutObj_Compress> compressor;
  CompressorRef plugin;
  char supplied_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];

  /* Read in the data from the POST form. */
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  op_ret = verify_params();
  if (op_ret < 0) {
    return;
  }

  if (s->iam_policy || ! s->iam_user_policies.empty()) {
    auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env,
                                            boost::none,
                                            rgw::IAM::s3PutObject,
                                            rgw_obj(s->bucket, s->object));
    if (usr_policy_res == Effect::Deny) {
      op_ret = -EACCES;
      return;
    }

    rgw::IAM::Effect e = Effect::Pass;
    if (s->iam_policy) {
      e = s->iam_policy->eval(s->env, *s->auth.identity,
				 rgw::IAM::s3PutObject,
				 rgw_obj(s->bucket, s->object));
    }
    if (e == Effect::Deny) {
      op_ret = -EACCES;
      return;
    } else if (usr_policy_res == Effect::Pass && e == Effect::Pass && !verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
      op_ret = -EACCES;
      return;
    }
  } else if (!verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    op_ret = -EACCES;
    return;
  }

  /* Start iteration over data fields. It's necessary as Swift's FormPost
   * is capable to handle multiple files in single form. */
  do {
    char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
    unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
    MD5 hash;
    ceph::buffer::list bl, aclbl;
    int len = 0;

    op_ret = store->check_quota(s->bucket_owner.get_id(),
                                s->bucket,
                                user_quota,
                                bucket_quota,
                                s->content_length);
    if (op_ret < 0) {
      return;
    }

    op_ret = store->check_bucket_shards(s->bucket_info, s->bucket, bucket_quota);
    if (op_ret < 0) {
      return;
    }

    if (supplied_md5_b64) {
      char supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1];
      ldpp_dout(this, 15) << "supplied_md5_b64=" << supplied_md5_b64 << dendl;
      op_ret = ceph_unarmor(supplied_md5_bin, &supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1],
                            supplied_md5_b64, supplied_md5_b64 + strlen(supplied_md5_b64));
      ldpp_dout(this, 15) << "ceph_armor ret=" << op_ret << dendl;
      if (op_ret != CEPH_CRYPTO_MD5_DIGESTSIZE) {
        op_ret = -ERR_INVALID_DIGEST;
        return;
      }

      buf_to_hex((const unsigned char *)supplied_md5_bin, CEPH_CRYPTO_MD5_DIGESTSIZE, supplied_md5);
      ldpp_dout(this, 15) << "supplied_md5=" << supplied_md5 << dendl;
    }

    rgw_obj obj(s->bucket, get_current_filename());
    if (s->bucket_info.versioning_enabled()) {
      store->gen_rand_obj_instance_name(&obj);
    }

    rgw::AioThrottle aio(s->cct->_conf->rgw_put_obj_min_window_size);

    using namespace rgw::putobj;
    AtomicObjectProcessor processor(&aio, store, s->bucket_info,
                                    &s->dest_placement,
                                    s->bucket_owner.get_id(),
                                    *static_cast<RGWObjectCtx*>(s->obj_ctx),
                                    obj, 0, s->req_id);
    op_ret = processor.prepare();
    if (op_ret < 0) {
      return;
    }

    /* No filters by default. */
    DataProcessor *filter = &processor;

    std::unique_ptr<DataProcessor> encrypt;
    op_ret = get_encrypt_filter(&encrypt, filter);
    if (op_ret < 0) {
      return;
    }
    if (encrypt != nullptr) {
      filter = encrypt.get();
    } else {
      const auto& compression_type = store->svc.zone->get_zone_params().get_compression_type(
          s->dest_placement);
      if (compression_type != "none") {
        plugin = Compressor::create(s->cct, compression_type);
        if (!plugin) {
          ldpp_dout(this, 1) << "Cannot load plugin for compression type "
                           << compression_type << dendl;
        } else {
          compressor.emplace(s->cct, plugin, filter);
          filter = &*compressor;
        }
      }
    }

    bool again;
    do {
      ceph::bufferlist data;
      len = get_data(data, again);

      if (len < 0) {
        op_ret = len;
        return;
      }

      if (!len) {
        break;
      }

      hash.Update((const unsigned char *)data.c_str(), data.length());
      op_ret = filter->process(std::move(data), ofs);

      ofs += len;

      if (ofs > max_len) {
        op_ret = -ERR_TOO_LARGE;
        return;
      }
    } while (again);

    // flush
    op_ret = filter->process({}, ofs);
    if (op_ret < 0) {
      return;
    }

    if (len < min_len) {
      op_ret = -ERR_TOO_SMALL;
      return;
    }

    s->obj_size = ofs;


    op_ret = store->check_quota(s->bucket_owner.get_id(), s->bucket,
                                user_quota, bucket_quota, s->obj_size);
    if (op_ret < 0) {
      return;
    }

    op_ret = store->check_bucket_shards(s->bucket_info, s->bucket, bucket_quota);
    if (op_ret < 0) {
      return;
    }

    hash.Final(m);
    buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);

    etag = calc_md5;
    
    if (supplied_md5_b64 && strcmp(calc_md5, supplied_md5)) {
      op_ret = -ERR_BAD_DIGEST;
      return;
    }

    bl.append(etag.c_str(), etag.size());
    emplace_attr(RGW_ATTR_ETAG, std::move(bl));

    policy.encode(aclbl);
    emplace_attr(RGW_ATTR_ACL, std::move(aclbl));

    const std::string content_type = get_current_content_type();
    if (! content_type.empty()) {
      ceph::bufferlist ct_bl;
      ct_bl.append(content_type.c_str(), content_type.size() + 1);
      emplace_attr(RGW_ATTR_CONTENT_TYPE, std::move(ct_bl));
    }

    if (compressor && compressor->is_compressed()) {
      ceph::bufferlist tmp;
      RGWCompressionInfo cs_info;
      cs_info.compression_type = plugin->get_type_name();
      cs_info.orig_size = s->obj_size;
      cs_info.blocks = move(compressor->get_compression_blocks());
      encode(cs_info, tmp);
      emplace_attr(RGW_ATTR_COMPRESSION, std::move(tmp));
    }

    op_ret = processor.complete(s->obj_size, etag, nullptr, real_time(), attrs,
                                (delete_at ? *delete_at : real_time()),
                                nullptr, nullptr, nullptr, nullptr, nullptr);
    if (op_ret < 0) {
      return;
    }
  } while (is_next_file_to_upload());
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

  for (const string& name : rmattr_names) {
    if (name.compare(RGW_ATTR_TEMPURL_KEY1) == 0) {
      temp_url_keys[0] = string();
    }
    if (name.compare(RGW_ATTR_TEMPURL_KEY2) == 0) {
      temp_url_keys[1] = string();
    }
  }
}

int RGWPutMetadataAccount::init_processing()
{
  /* First, go to the base class. At the time of writing the method was
   * responsible only for initializing the quota. This isn't necessary
   * here as we are touching metadata only. I'm putting this call only
   * for the future. */
  op_ret = RGWOp::init_processing();
  if (op_ret < 0) {
    return op_ret;
  }

  op_ret = get_params();
  if (op_ret < 0) {
    return op_ret;
  }

  op_ret = rgw_get_user_attrs_by_uid(store, s->user->user_id, orig_attrs,
                                     &acct_op_tracker);
  if (op_ret < 0) {
    return op_ret;
  }

  if (has_policy) {
    bufferlist acl_bl;
    policy.encode(acl_bl);
    attrs.emplace(RGW_ATTR_ACL, std::move(acl_bl));
  }

  op_ret = rgw_get_request_metadata(s->cct, s->info, attrs, false);
  if (op_ret < 0) {
    return op_ret;
  }
  prepare_add_del_attrs(orig_attrs, rmattr_names, attrs);
  populate_with_generic_attrs(s, attrs);

  /* Try extract the TempURL-related stuff now to allow verify_permission
   * evaluate whether we need FULL_CONTROL or not. */
  filter_out_temp_url(attrs, rmattr_names, temp_url_keys);

  /* The same with quota except a client needs to be reseller admin. */
  op_ret = filter_out_quota_info(attrs, rmattr_names, new_quota,
                                 &new_quota_extracted);
  if (op_ret < 0) {
    return op_ret;
  }

  return 0;
}

int RGWPutMetadataAccount::verify_permission()
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if (!verify_user_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

  /* Altering TempURL keys requires FULL_CONTROL. */
  if (!temp_url_keys.empty() && s->perm_mask != RGW_PERM_FULL_CONTROL) {
    return -EPERM;
  }

  /* We are failing this intensionally to allow system user/reseller admin
   * override in rgw_process.cc. This is the way to specify a given RGWOp
   * expect extra privileges.  */
  if (new_quota_extracted) {
    return -EACCES;
  }

  return 0;
}

void RGWPutMetadataAccount::execute()
{
  /* Params have been extracted earlier. See init_processing(). */
  RGWUserInfo new_uinfo;
  op_ret = rgw_get_user_info_by_uid(store, s->user->user_id, new_uinfo,
                                    &acct_op_tracker);
  if (op_ret < 0) {
    return;
  }

  /* Handle the TempURL-related stuff. */
  if (!temp_url_keys.empty()) {
    for (auto& pair : temp_url_keys) {
      new_uinfo.temp_url_keys[pair.first] = std::move(pair.second);
    }
  }

  /* Handle the quota extracted at the verify_permission step. */
  if (new_quota_extracted) {
    new_uinfo.user_quota = std::move(new_quota);
  }

  /* We are passing here the current (old) user info to allow the function
   * optimize-out some operations. */
  op_ret = rgw_store_user_info(store, new_uinfo, s->user,
                               &acct_op_tracker, real_time(), false, &attrs);
}

int RGWPutMetadataBucket::verify_permission()
{
  if (!verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
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
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  op_ret = rgw_get_request_metadata(s->cct, s->info, attrs, false);
  if (op_ret < 0) {
    return;
  }

  if (!placement_rule.empty() &&
      placement_rule != s->bucket_info.placement_rule) {
    op_ret = -EEXIST;
    return;
  }

  op_ret = retry_raced_bucket_write(store, s, [this] {
      /* Encode special metadata first as we're using std::map::emplace under
       * the hood. This method will add the new items only if the map doesn't
       * contain such keys yet. */
      if (has_policy) {
	if (s->dialect.compare("swift") == 0) {
	  auto old_policy =						\
	    static_cast<RGWAccessControlPolicy_SWIFT*>(s->bucket_acl.get());
	  auto new_policy = static_cast<RGWAccessControlPolicy_SWIFT*>(&policy);
	  new_policy->filter_merge(policy_rw_mask, old_policy);
	  policy = *new_policy;
	}
	buffer::list bl;
	policy.encode(bl);
	emplace_attr(RGW_ATTR_ACL, std::move(bl));
      }

      if (has_cors) {
	buffer::list bl;
	cors_config.encode(bl);
	emplace_attr(RGW_ATTR_CORS, std::move(bl));
      }

      /* It's supposed that following functions WILL NOT change any
       * special attributes (like RGW_ATTR_ACL) if they are already
       * present in attrs. */
      prepare_add_del_attrs(s->bucket_attrs, rmattr_names, attrs);
      populate_with_generic_attrs(s, attrs);

      /* According to the Swift's behaviour and its container_quota
       * WSGI middleware implementation: anyone with write permissions
       * is able to set the bucket quota. This stays in contrast to
       * account quotas that can be set only by clients holding
       * reseller admin privileges. */
      op_ret = filter_out_quota_info(attrs, rmattr_names, s->bucket_info.quota);
      if (op_ret < 0) {
	return op_ret;
      }

      if (swift_ver_location) {
	s->bucket_info.swift_ver_location = *swift_ver_location;
	s->bucket_info.swift_versioning = (!swift_ver_location->empty());
      }

      /* Web site of Swift API. */
      filter_out_website(attrs, rmattr_names, s->bucket_info.website_conf);
      s->bucket_info.has_website = !s->bucket_info.website_conf.is_empty();

      /* Setting attributes also stores the provided bucket info. Due
       * to this fact, the new quota settings can be serialized with
       * the same call. */
      op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs,
				    &s->bucket_info.objv_tracker);
      return op_ret;
    });
}

int RGWPutMetadataObject::verify_permission()
{
  // This looks to be something specific to Swift. We could add
  // operations like swift:PutMetadataObject to the Policy Engine.
  if (!verify_object_permission_no_policy(this, s, RGW_PERM_WRITE)) {
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

  op_ret = rgw_get_request_metadata(s->cct, s->info, attrs);
  if (op_ret < 0) {
    return;
  }

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
      ldpp_dout(this, 0) << "bad user manifest: " << dlo_manifest << dendl;
      return;
    }
  }

  op_ret = store->set_attrs(s->obj_ctx, s->bucket_info, obj, attrs, &rmattrs);
}

int RGWDeleteObj::handle_slo_manifest(bufferlist& bl)
{
  RGWSLOInfo slo_info;
  auto bliter = bl.cbegin();
  try {
    decode(slo_info, bliter);
  } catch (buffer::error& err) {
    ldpp_dout(this, 0) << "ERROR: failed to decode slo manifest" << dendl;
    return -EIO;
  }

  try {
    deleter = std::unique_ptr<RGWBulkDelete::Deleter>(\
          new RGWBulkDelete::Deleter(this, store, s));
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  }

  list<RGWBulkDelete::acct_path_t> items;
  for (const auto& iter : slo_info.entries) {
    const string& path_str = iter.path;

    const size_t sep_pos = path_str.find('/', 1 /* skip first slash */);
    if (boost::string_view::npos == sep_pos) {
      return -EINVAL;
    }

    RGWBulkDelete::acct_path_t path;

    path.bucket_name = url_decode(path_str.substr(1, sep_pos - 1));
    path.obj_key = url_decode(path_str.substr(sep_pos + 1));

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
  if (s->iam_policy || ! s->iam_user_policies.empty()) {
    auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env,
                                              boost::none,
                                              s->object.instance.empty() ?
                                              rgw::IAM::s3DeleteObject :
                                              rgw::IAM::s3DeleteObjectVersion,
                                              ARN(s->bucket, s->object.name));
    if (usr_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect r = Effect::Pass;
    if (s->iam_policy) {
      r = s->iam_policy->eval(s->env, *s->auth.identity,
				 s->object.instance.empty() ?
				 rgw::IAM::s3DeleteObject :
				 rgw::IAM::s3DeleteObjectVersion,
				 ARN(s->bucket, s->object.name));
    }
    if (r == Effect::Allow)
      return 0;
    else if (r == Effect::Deny)
      return -EACCES;
    else if (usr_policy_res == Effect::Allow)
      return 0;
  }

  if (!verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

  if (s->bucket_info.mfa_enabled() &&
      !s->object.instance.empty() &&
      !s->mfa_verified) {
    ldpp_dout(this, 5) << "NOTICE: object delete request with a versioned object, mfa auth not provided" << dendl;
    return -ERR_MFA_REQUIRED;
  }

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

  rgw_obj obj(s->bucket, s->object);
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
          ldpp_dout(this, 0) << "ERROR: failed to handle slo manifest ret=" << op_ret << dendl;
        }
      } else {
        op_ret = -ERR_NOT_SLO_MANIFEST;
      }

      return;
    }

    RGWObjectCtx *obj_ctx = static_cast<RGWObjectCtx *>(s->obj_ctx);
    obj_ctx->set_atomic(obj);

    bool ver_restored = false;
    op_ret = store->swift_versioning_restore(*s->sysobj_ctx, *obj_ctx, s->bucket_owner.get_id(),
                                             s->bucket_info, obj, ver_restored);
    if (op_ret < 0) {
      return;
    }

    if (!ver_restored) {
      /* Swift's versioning mechanism hasn't found any previous version of
       * the object that could be restored. This means we should proceed
       * with the regular delete path. */
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
    }

    if (op_ret == -ECANCELED) {
      op_ret = 0;
    }
    if (op_ret == -ERR_PRECONDITION_FAILED && no_precondition_error) {
      op_ret = 0;
    }
  } else {
    op_ret = -EINVAL;
  }
}

bool RGWCopyObj::parse_copy_location(const boost::string_view& url_src,
				     string& bucket_name,
				     rgw_obj_key& key)
{
  boost::string_view name_str;
  boost::string_view params_str;

  size_t pos = url_src.find('?');
  if (pos == string::npos) {
    name_str = url_src;
  } else {
    name_str = url_src.substr(0, pos);
    params_str = url_src.substr(pos + 1);
  }

  boost::string_view dec_src{name_str};
  if (dec_src[0] == '/')
    dec_src.remove_prefix(1);

  pos = dec_src.find('/');
  if (pos ==string::npos)
    return false;

  boost::string_view bn_view{dec_src.substr(0, pos)};
  bucket_name = std::string{bn_view.data(), bn_view.size()};

  boost::string_view kn_view{dec_src.substr(pos + 1)};
  key.name = std::string{kn_view.data(), kn_view.size()};

  if (key.name.empty()) {
    return false;
  }

  if (! params_str.empty()) {
    RGWHTTPArgs args;
    args.set(params_str.to_string());
    args.parse();

    key.instance = args.get("versionId", NULL);
  }

  return true;
}

int RGWCopyObj::verify_permission()
{
  RGWAccessControlPolicy src_acl(s->cct);
  boost::optional<Policy> src_policy;
  op_ret = get_params();
  if (op_ret < 0)
    return op_ret;

  op_ret = get_system_versioning_params(s, &olh_epoch, &version_id);
  if (op_ret < 0) {
    return op_ret;
  }
  map<string, bufferlist> src_attrs;

  if (s->bucket_instance_id.empty()) {
    op_ret = store->get_bucket_info(*s->sysobj_ctx, src_tenant_name, src_bucket_name, src_bucket_info, NULL, &src_attrs);
  } else {
    /* will only happen in intra region sync where the source and dest bucket is the same */
    op_ret = store->get_bucket_instance_info(*s->sysobj_ctx, s->bucket_instance_id, src_bucket_info, NULL, &src_attrs);
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

    rgw_placement_rule src_placement;

    /* check source object permissions */
    op_ret = read_obj_policy(store, s, src_bucket_info, src_attrs, &src_acl, &src_placement.storage_class,
			     src_policy, src_bucket, src_object);
    if (op_ret < 0) {
      return op_ret;
    }

    /* follow up on previous checks that required reading source object head */
    if (need_to_check_storage_class) {
      src_placement.inherit_from(src_bucket_info.placement_rule);

      op_ret  = check_storage_class(src_placement);
      if (op_ret < 0) {
        return op_ret;
      }
    }

    /* admin request overrides permission checks */
    if (!s->auth.identity->is_admin_of(src_acl.get_owner().get_id())) {
      if (src_policy) {
	auto e = src_policy->eval(s->env, *s->auth.identity,
				  src_object.instance.empty() ?
				  rgw::IAM::s3GetObject :
				  rgw::IAM::s3GetObjectVersion,
				  ARN(src_obj));
	if (e == Effect::Deny) {
	  return -EACCES;
	} else if (e == Effect::Pass &&
		   !src_acl.verify_permission(this, *s->auth.identity, s->perm_mask,
					      RGW_PERM_READ)) { 
	  return -EACCES;
	}
      } else if (!src_acl.verify_permission(this, *s->auth.identity,
					       s->perm_mask,
					    RGW_PERM_READ)) {
	return -EACCES;
      }
    }
  }

  RGWAccessControlPolicy dest_bucket_policy(s->cct);
  map<string, bufferlist> dest_attrs;

  if (src_bucket_name.compare(dest_bucket_name) == 0) { /* will only happen if s->local_source
                                                           or intra region sync */
    dest_bucket_info = src_bucket_info;
    dest_attrs = src_attrs;
  } else {
    op_ret = store->get_bucket_info(*s->sysobj_ctx, dest_tenant_name, dest_bucket_name,
                                    dest_bucket_info, nullptr, &dest_attrs);
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

  /* check dest bucket permissions */
  op_ret = read_bucket_policy(store, s, dest_bucket_info, dest_attrs,
                              &dest_bucket_policy, dest_bucket);
  if (op_ret < 0) {
    return op_ret;
  }
  auto dest_iam_policy = get_iam_policy_from_attr(s->cct, store, dest_attrs, dest_bucket.tenant);
  /* admin request overrides permission checks */
  if (! s->auth.identity->is_admin_of(dest_policy.get_owner().get_id())){
    if (dest_iam_policy != boost::none) {
      rgw_add_to_iam_environment(s->env, "s3:x-amz-copy-source", copy_source);
      rgw_add_to_iam_environment(s->env, "s3:x-amz-metadata-directive", md_directive);

      auto e = dest_iam_policy->eval(s->env, *s->auth.identity,
                                     rgw::IAM::s3PutObject,
                                     ARN(dest_obj));
      if (e == Effect::Deny) {
        return -EACCES;
      } else if (e == Effect::Pass &&
                 ! dest_bucket_policy.verify_permission(this,
                                                        *s->auth.identity,
                                                        s->perm_mask,
                                                        RGW_PERM_WRITE)){
        return -EACCES;
      }
    }
  } else if (! dest_bucket_policy.verify_permission(this, *s->auth.identity, s->perm_mask,
                                                    RGW_PERM_WRITE)) {
    return -EACCES;
  }

  op_ret = init_dest_policy();
  if (op_ret < 0) {
    return op_ret;
  }

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
  emplace_attr(RGW_ATTR_ACL, std::move(aclbl));

  op_ret = rgw_get_request_metadata(s->cct, s->info, attrs);
  if (op_ret < 0) {
    return op_ret;
  }
  populate_with_generic_attrs(s, attrs);

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
  if ( ! version_id.empty()) {
    dst_obj.key.set_instance(version_id);
  } else if (dest_bucket_info.versioning_enabled()) {
    store->gen_rand_obj_instance_name(&dst_obj);
  }

  obj_ctx.set_atomic(src_obj);
  obj_ctx.set_atomic(dst_obj);

  encode_delete_at_attr(delete_at, attrs);

  bool high_precision_time = (s->system_request);

  /* Handle object versioning of Swift API. In case of copying to remote this
   * should fail gently (op_ret == 0) as the dst_obj will not exist here. */
  op_ret = store->swift_versioning_copy(obj_ctx,
                                        dest_bucket_info.owner,
                                        dest_bucket_info,
                                        dst_obj);
  if (op_ret < 0) {
    return;
  }

  op_ret = store->copy_obj(obj_ctx,
			   s->user->user_id,
			   &s->info,
			   source_zone,
			   dst_obj,
			   src_obj,
			   dest_bucket_info,
			   src_bucket_info,
                           s->dest_placement,
			   &src_mtime,
			   &mtime,
			   mod_ptr,
			   unmod_ptr,
                           high_precision_time,
			   if_match,
			   if_nomatch,
			   attrs_mod,
                           copy_if_newer,
			   attrs, RGWObjCategory::Main,
			   olh_epoch,
			   (delete_at ? *delete_at : real_time()),
			   (version_id.empty() ? NULL : &version_id),
			   &s->req_id, /* use req_id as tag */
			   &etag,
			   copy_obj_progress_cb, (void *)this
    );
}

int RGWGetACLs::verify_permission()
{
  bool perm;
  if (!s->object.empty()) {
    auto iam_action = s->object.instance.empty() ?
      rgw::IAM::s3GetObjectAcl :
      rgw::IAM::s3GetObjectVersionAcl;

    if (s->iam_policy && s->iam_policy->has_partial_conditional(S3_EXISTING_OBJTAG)){
      rgw_obj obj = rgw_obj(s->bucket, s->object);
      rgw_iam_add_existing_objtags(store, s, obj, iam_action);
    }
    if (! s->iam_user_policies.empty()) {
      for (auto& user_policy : s->iam_user_policies) {
        if (user_policy.has_partial_conditional(S3_EXISTING_OBJTAG)) {
          rgw_obj obj = rgw_obj(s->bucket, s->object);
          rgw_iam_add_existing_objtags(store, s, obj, iam_action);
        }
      }
    }
    perm = verify_object_permission(this, s, iam_action);
  } else {
    if (!s->bucket_exists) {
      return -ERR_NO_SUCH_BUCKET;
    }
    perm = verify_bucket_permission(this, s, rgw::IAM::s3GetBucketAcl);
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
  RGWAccessControlPolicy* const acl = \
    (!s->object.empty() ? s->object_acl.get() : s->bucket_acl.get());
  RGWAccessControlPolicy_S3* const s3policy = \
    static_cast<RGWAccessControlPolicy_S3*>(acl);
  s3policy->to_xml(ss);
  acls = ss.str();
}



int RGWPutACLs::verify_permission()
{
  bool perm;

  rgw_add_to_iam_environment(s->env, "s3:x-amz-acl", s->canned_acl);

  rgw_add_grant_to_iam_environment(s->env, s);
  if (!s->object.empty()) {
    auto iam_action = s->object.instance.empty() ? rgw::IAM::s3PutObjectAcl : rgw::IAM::s3PutObjectVersionAcl;
    auto obj = rgw_obj(s->bucket, s->object);
    op_ret = rgw_iam_add_existing_objtags(store, s, obj, iam_action);
    perm = verify_object_permission(this, s, iam_action);
  } else {
    perm = verify_bucket_permission(this, s, rgw::IAM::s3PutBucketAcl);
  }
  if (!perm)
    return -EACCES;

  return 0;
}

int RGWGetLC::verify_permission()
{
  bool perm;
  perm = verify_bucket_permission(this, s, rgw::IAM::s3GetLifecycleConfiguration);
  if (!perm)
    return -EACCES;

  return 0;
}

int RGWPutLC::verify_permission()
{
  bool perm;
  perm = verify_bucket_permission(this, s, rgw::IAM::s3PutLifecycleConfiguration);
  if (!perm)
    return -EACCES;

  return 0;
}

int RGWDeleteLC::verify_permission()
{
  bool perm;
  perm = verify_bucket_permission(this, s, rgw::IAM::s3PutLifecycleConfiguration);
  if (!perm)
    return -EACCES;

  return 0;
}

void RGWPutACLs::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetLC::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPutLC::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteLC::pre_exec()
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
  rgw_obj obj;

  op_ret = 0; /* XXX redundant? */

  if (!parser.init()) {
    op_ret = -EINVAL;
    return;
  }


  RGWAccessControlPolicy* const existing_policy = \
    (s->object.empty() ? s->bucket_acl.get() : s->object_acl.get());

  owner = existing_policy->get_owner();

  op_ret = get_params();
  if (op_ret < 0) {
    if (op_ret == -ERANGE) {
      ldpp_dout(this, 4) << "The size of request xml data is larger than the max limitation, data size = "
                       << s->length << dendl;
      op_ret = -ERR_MALFORMED_XML;
      s->err.message = "The XML you provided was larger than the maximum " +
                       std::to_string(s->cct->_conf->rgw_max_put_param_size) +
                       " bytes allowed.";
    }
    return;
  }

  char* buf = data.c_str();
  ldpp_dout(this, 15) << "read len=" << data.length() << " data=" << (buf ? buf : "") << dendl;

  if (!s->canned_acl.empty() && data.length() > 0) {
    op_ret = -EINVAL;
    return;
  }

  if (!s->canned_acl.empty() || s->has_acl_header) {
    op_ret = get_policy_from_state(store, s, ss);
    if (op_ret < 0)
      return;

    data.clear();
    data.append(ss.str());
  }

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    op_ret = -EINVAL;
    return;
  }
  policy = static_cast<RGWAccessControlPolicy_S3 *>(parser.find_first("AccessControlPolicy"));
  if (!policy) {
    op_ret = -EINVAL;
    return;
  }

  const RGWAccessControlList& req_acl = policy->get_acl();
  const multimap<string, ACLGrant>& req_grant_map = req_acl.get_grant_map();
#define ACL_GRANTS_MAX_NUM      100
  int max_num = s->cct->_conf->rgw_acl_grants_max_num;
  if (max_num < 0) {
    max_num = ACL_GRANTS_MAX_NUM;
  }

  int grants_num = req_grant_map.size();
  if (grants_num > max_num) {
    ldpp_dout(this, 4) << "An acl can have up to " << max_num
        << " grants, request acl grants num: " << grants_num << dendl;
    op_ret = -ERR_MALFORMED_ACL_ERROR;
    s->err.message = "The request is rejected, because the acl grants number you requested is larger than the maximum "
                     + std::to_string(max_num)
                     + " grants allowed in an acl.";
    return;
  }

  // forward bucket acl requests to meta master zone
  if (s->object.empty() && !store->svc.zone->is_meta_master()) {
    bufferlist in_data;
    // include acl data unless it was generated from a canned_acl
    if (s->canned_acl.empty()) {
      in_data.append(data);
    }
    op_ret = forward_request_to_master(s, NULL, store, in_data, NULL);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
  }

  if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    ldpp_dout(this, 15) << "Old AccessControlPolicy";
    policy->to_xml(*_dout);
    *_dout << dendl;
  }

  op_ret = policy->rebuild(store, &owner, new_policy);
  if (op_ret < 0)
    return;

  if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    ldpp_dout(this, 15) << "New AccessControlPolicy:";
    new_policy.to_xml(*_dout);
    *_dout << dendl;
  }

  new_policy.encode(bl);
  map<string, bufferlist> attrs;

  if (!s->object.empty()) {
    obj = rgw_obj(s->bucket, s->object);
    store->set_atomic(s->obj_ctx, obj);
    //if instance is empty, we should modify the latest object
    op_ret = modify_obj_attr(store, s, obj, RGW_ATTR_ACL, bl);
  } else {
    attrs = s->bucket_attrs;
    attrs[RGW_ATTR_ACL] = bl;
    op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs, &s->bucket_info.objv_tracker);
  }
  if (op_ret == -ECANCELED) {
    op_ret = 0; /* lost a race, but it's ok because acls are immutable */
  }
}

void RGWPutLC::execute()
{
  bufferlist bl;
  
  RGWLifecycleConfiguration_S3 config(s->cct);
  RGWXMLParser parser;
  RGWLifecycleConfiguration_S3 new_config(s->cct);

  content_md5 = s->info.env->get("HTTP_CONTENT_MD5");
  if (content_md5 == nullptr) {
    op_ret = -ERR_INVALID_REQUEST;
    s->err.message = "Missing required header for this request: Content-MD5";
    ldpp_dout(this, 5) << s->err.message << dendl;
    return;
  }

  std::string content_md5_bin;
  try {
    content_md5_bin = rgw::from_base64(boost::string_view(content_md5));
  } catch (...) {
    s->err.message = "Request header Content-MD5 contains character "
                     "that is not base64 encoded.";
    ldpp_dout(this, 5) << s->err.message << dendl;
    op_ret = -ERR_BAD_DIGEST;
    return;
  }

  if (!parser.init()) {
    op_ret = -EINVAL;
    return;
  }

  op_ret = get_params();
  if (op_ret < 0)
    return;

  char* buf = data.c_str();
  ldpp_dout(this, 15) << "read len=" << data.length() << " data=" << (buf ? buf : "") << dendl;

  MD5 data_hash;
  unsigned char data_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
  data_hash.Update(reinterpret_cast<const unsigned char*>(buf), data.length());
  data_hash.Final(data_hash_res);

  if (memcmp(data_hash_res, content_md5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) {
    op_ret = -ERR_BAD_DIGEST;
    s->err.message = "The Content-MD5 you specified did not match what we received.";
    ldpp_dout(this, 5) << s->err.message
                     << " Specified content md5: " << content_md5
                     << ", calculated content md5: " << data_hash_res
                     << dendl;
    return;
  }

  if (!parser.parse(buf, data.length(), 1)) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("LifecycleConfiguration", config, &parser);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "Bad lifecycle configuration: " << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  op_ret = config.rebuild(store, new_config);
  if (op_ret < 0)
    return;

  if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    XMLFormatter xf;
    new_config.dump_xml(&xf);
    stringstream ss;
    xf.flush(ss);
    ldpp_dout(this, 15) << "New LifecycleConfiguration:" << ss.str() << dendl;
  }

  op_ret = store->get_lc()->set_bucket_config(s->bucket_info, s->bucket_attrs, &new_config);
  if (op_ret < 0) {
    return;
  }
  return;
}

void RGWDeleteLC::execute()
{
  map<string, bufferlist> attrs = s->bucket_attrs;
  attrs.erase(RGW_ATTR_LC);
  op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs,
				&s->bucket_info.objv_tracker);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "RGWLC::RGWDeleteLC() failed to set attrs on bucket="
        << s->bucket.name << " returned err=" << op_ret << dendl;
    return;
  }

  op_ret = store->get_lc()->remove_bucket_config(s->bucket_info, s->bucket_attrs);
  if (op_ret < 0) {
    return;
  }
  return;
}

int RGWGetCORS::verify_permission()
{
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketCORS);
}

void RGWGetCORS::execute()
{
  op_ret = read_bucket_cors();
  if (op_ret < 0)
    return ;

  if (!cors_exist) {
    ldpp_dout(this, 2) << "No CORS configuration set yet for this bucket" << dendl;
    op_ret = -ERR_NO_CORS_FOUND;
    return;
  }
}

int RGWPutCORS::verify_permission()
{
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketCORS);
}

void RGWPutCORS::execute()
{
  rgw_raw_obj obj;

  op_ret = get_params();
  if (op_ret < 0)
    return;

  if (!store->svc.zone->is_meta_master()) {
    op_ret = forward_request_to_master(s, NULL, store, in_data, nullptr);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
  }

  op_ret = retry_raced_bucket_write(store, s, [this] {
      map<string, bufferlist> attrs = s->bucket_attrs;
      attrs[RGW_ATTR_CORS] = cors_bl;
      return rgw_bucket_set_attrs(store, s->bucket_info, attrs, &s->bucket_info.objv_tracker);
    });
}

int RGWDeleteCORS::verify_permission()
{
  // No separate delete permission
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketCORS);
}

void RGWDeleteCORS::execute()
{
  op_ret = retry_raced_bucket_write(store, s, [this] {
      op_ret = read_bucket_cors();
      if (op_ret < 0)
	return op_ret;

      if (!cors_exist) {
	ldpp_dout(this, 2) << "No CORS configuration set yet for this bucket" << dendl;
	op_ret = -ENOENT;
	return op_ret;
      }

      map<string, bufferlist> attrs = s->bucket_attrs;
      attrs.erase(RGW_ATTR_CORS);
      op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs,
				&s->bucket_info.objv_tracker);
      if (op_ret < 0) {
	ldpp_dout(this, 0) << "RGWLC::RGWDeleteCORS() failed to set attrs on bucket=" << s->bucket.name
			 << " returned err=" << op_ret << dendl;
      }
      return op_ret;
    });
}

void RGWOptionsCORS::get_response_params(string& hdrs, string& exp_hdrs, unsigned *max_age) {
  get_cors_response_headers(rule, req_hdrs, hdrs, exp_hdrs, max_age);
}

int RGWOptionsCORS::validate_cors_request(RGWCORSConfiguration *cc) {
  rule = cc->host_name_rule(origin);
  if (!rule) {
    ldpp_dout(this, 10) << "There is no cors rule present for " << origin << dendl;
    return -ENOENT;
  }

  if (!validate_cors_rule_method(rule, req_meth)) {
    return -ENOENT;
  }

  if (!validate_cors_rule_header(rule, req_hdrs)) {
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
    ldpp_dout(this, 0) << "Missing mandatory Origin header" << dendl;
    op_ret = -EINVAL;
    return;
  }
  req_meth = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_METHOD");
  if (!req_meth) {
    ldpp_dout(this, 0) << "Missing mandatory Access-control-request-method header" << dendl;
    op_ret = -EINVAL;
    return;
  }
  if (!cors_exist) {
    ldpp_dout(this, 2) << "No CORS configuration set yet for this bucket" << dendl;
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
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketRequestPayment);
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
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketRequestPayment);
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
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name
		     << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWInitMultipart::verify_permission()
{
  if (s->iam_policy || ! s->iam_user_policies.empty()) {
    auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env,
                                              boost::none,
                                              rgw::IAM::s3PutObject,
                                              rgw_obj(s->bucket, s->object));
    if (usr_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect e = Effect::Pass;
    if (s->iam_policy) {
      e = s->iam_policy->eval(s->env, *s->auth.identity,
				 rgw::IAM::s3PutObject,
				 rgw_obj(s->bucket, s->object));
    }
    if (e == Effect::Allow) {
      return 0;
    } else if (e == Effect::Deny) {
      return -EACCES;
    } else if (usr_policy_res == Effect::Allow) {
      return 0;
    }
  }

  if (!verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

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

  if (get_params() < 0)
    return;

  if (s->object.empty())
    return;

  policy.encode(aclbl);
  attrs[RGW_ATTR_ACL] = aclbl;

  populate_with_generic_attrs(s, attrs);

  /* select encryption mode */
  op_ret = prepare_encryption(attrs);
  if (op_ret != 0)
    return;

  op_ret = rgw_get_request_metadata(s->cct, s->info, attrs);
  if (op_ret < 0) {
    return;
  }

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
    obj_op.meta.category = RGWObjCategory::MultiMeta;
    obj_op.meta.flags = PUT_OBJ_CREATE_EXCL;

    multipart_upload_info upload_info;
    upload_info.dest_placement = s->dest_placement;

    bufferlist bl;
    encode(upload_info, bl);
    obj_op.meta.data = &bl;

    op_ret = obj_op.write_meta(bl.length(), 0, attrs);
  } while (op_ret == -EEXIST);
}

int RGWCompleteMultipart::verify_permission()
{
  if (s->iam_policy || ! s->iam_user_policies.empty()) {
    auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env,
                                              boost::none,
                                              rgw::IAM::s3PutObject,
                                              rgw_obj(s->bucket, s->object));
    if (usr_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect e = Effect::Pass;
    if (s->iam_policy) {
      e = s->iam_policy->eval(s->env, *s->auth.identity,
				 rgw::IAM::s3PutObject,
				 rgw_obj(s->bucket, s->object));
    }
    if (e == Effect::Allow) {
      return 0;
    } else if (e == Effect::Deny) {
      return -EACCES;
    } else if (usr_policy_res == Effect::Allow) {
      return 0;
    }
  }

  if (!verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

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

  op_ret = get_params();
  if (op_ret < 0)
    return;
  op_ret = get_system_versioning_params(s, &olh_epoch, &version_id);
  if (op_ret < 0) {
    return;
  }

  if (!data.length()) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  if (!parser.init()) {
    op_ret = -EIO;
    return;
  }

  if (!parser.parse(data.c_str(), data.length(), 1)) {
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
  RGWCompressionInfo cs_info;
  bool compressed = false;
  uint64_t accounted_size = 0;

  uint64_t min_part_size = s->cct->_conf->rgw_multipart_min_part_size;

  list<rgw_obj_index_key> remove_objs; /* objects to be removed from index listing */

  bool versioned_object = s->bucket_info.versioning_enabled();

  iter = parts->parts.begin();

  meta_obj.init_ns(s->bucket, meta_oid, mp_ns);
  meta_obj.set_in_extra_data(true);
  meta_obj.index_hash_source = s->object.name;

  /*take a cls lock on meta_obj to prevent racing completions (or retries)
    from deleting the parts*/
  rgw_pool meta_pool;
  rgw_raw_obj raw_obj;
  int max_lock_secs_mp =
    s->cct->_conf.get_val<int64_t>("rgw_mp_lock_max_time");
  utime_t dur(max_lock_secs_mp, 0);

  store->obj_to_raw((s->bucket_info).placement_rule, meta_obj, &raw_obj);
  store->get_obj_data_pool((s->bucket_info).placement_rule,
			   meta_obj,&meta_pool);
  store->open_pool_ctx(meta_pool, serializer.ioctx);

  op_ret = serializer.try_lock(raw_obj.oid, dur);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "failed to acquire lock" << dendl;
    op_ret = -ERR_INTERNAL_ERROR;
    s->err.message = "This multipart completion is already in progress";
    return;
  }

  op_ret = get_obj_attrs(store, s, meta_obj, attrs);

  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get obj attrs, obj=" << meta_obj
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
      ldpp_dout(this, 0) << "NOTICE: total parts mismatch: have: " << total_parts
		       << " expected: " << parts->parts.size() << dendl;
      op_ret = -ERR_INVALID_PART;
      return;
    }

    for (obj_iter = obj_parts.begin(); iter != parts->parts.end() && obj_iter != obj_parts.end(); ++iter, ++obj_iter, ++handled_parts) {
      uint64_t part_size = obj_iter->second.accounted_size;
      if (handled_parts < (int)parts->parts.size() - 1 &&
          part_size < min_part_size) {
        op_ret = -ERR_TOO_SMALL;
        return;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (iter->first != (int)obj_iter->first) {
        ldpp_dout(this, 0) << "NOTICE: parts num mismatch: next requested: "
			 << iter->first << " next uploaded: "
			 << obj_iter->first << dendl;
        op_ret = -ERR_INVALID_PART;
        return;
      }
      string part_etag = rgw_string_unquote(iter->second);
      if (part_etag.compare(obj_iter->second.etag) != 0) {
        ldpp_dout(this, 0) << "NOTICE: etag mismatch: part: " << iter->first
			 << " etag: " << iter->second << dendl;
        op_ret = -ERR_INVALID_PART;
        return;
      }

      hex_to_buf(obj_iter->second.etag.c_str(), petag,
		CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const unsigned char *)petag, sizeof(petag));

      RGWUploadPartInfo& obj_part = obj_iter->second;

      /* update manifest for part */
      string oid = mp.get_part(obj_iter->second.num);
      rgw_obj src_obj;
      src_obj.init_ns(s->bucket, oid, mp_ns);

      if (obj_part.manifest.empty()) {
        ldpp_dout(this, 0) << "ERROR: empty manifest for object part: obj="
			 << src_obj << dendl;
        op_ret = -ERR_INVALID_PART;
        return;
      } else {
        manifest.append(obj_part.manifest, store->svc.zone);
      }

      bool part_compressed = (obj_part.cs_info.compression_type != "none");
      if ((obj_iter != obj_parts.begin()) &&
          ((part_compressed != compressed) ||
            (cs_info.compression_type != obj_part.cs_info.compression_type))) {
          ldpp_dout(this, 0) << "ERROR: compression type was changed during multipart upload ("
                           << cs_info.compression_type << ">>" << obj_part.cs_info.compression_type << ")" << dendl;
          op_ret = -ERR_INVALID_PART;
          return; 
      }
      
      if (part_compressed) {
        int64_t new_ofs; // offset in compression data for new part
        if (cs_info.blocks.size() > 0)
          new_ofs = cs_info.blocks.back().new_ofs + cs_info.blocks.back().len;
        else
          new_ofs = 0;
        for (const auto& block : obj_part.cs_info.blocks) {
          compression_block cb;
          cb.old_ofs = block.old_ofs + cs_info.orig_size;
          cb.new_ofs = new_ofs;
          cb.len = block.len;
          cs_info.blocks.push_back(cb);
          new_ofs = cb.new_ofs + cb.len;
        } 
        if (!compressed)
          cs_info.compression_type = obj_part.cs_info.compression_type;
        cs_info.orig_size += obj_part.cs_info.orig_size;
        compressed = true;
      }

      rgw_obj_index_key remove_key;
      src_obj.key.get_index_key(&remove_key);

      remove_objs.push_back(remove_key);

      ofs += obj_part.size;
      accounted_size += obj_part.accounted_size;
    }
  } while (truncated);
  hash.Final((unsigned char *)final_etag);

  buf_to_hex((unsigned char *)final_etag, sizeof(final_etag), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],  sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)parts->parts.size());
  etag = final_etag_str;
  ldpp_dout(this, 10) << "calculated etag: " << final_etag_str << dendl;

  etag_bl.append(final_etag_str, strlen(final_etag_str));

  attrs[RGW_ATTR_ETAG] = etag_bl;

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  target_obj.init(s->bucket, s->object.name);
  if (versioned_object) {
    if (!version_id.empty()) {
      target_obj.key.set_instance(version_id);
    } else {
      store->gen_rand_obj_instance_name(&target_obj);
      version_id = target_obj.key.get_instance();
    }
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
  obj_op.meta.modify_tail = true;
  obj_op.meta.completeMultipart = true;
  obj_op.meta.olh_epoch = olh_epoch;
  op_ret = obj_op.write_meta(ofs, accounted_size, attrs);
  if (op_ret < 0)
    return;

  // remove the upload obj
  int r = store->delete_obj(*static_cast<RGWObjectCtx *>(s->obj_ctx),
			    s->bucket_info, meta_obj, 0);
  if (r >= 0)  {
    /* serializer's exclusive lock is released */
    serializer.clear_locked();
  } else {
    ldpp_dout(this, 0) << "WARNING: failed to remove object " << meta_obj << dendl;
  }
}

int RGWCompleteMultipart::MPSerializer::try_lock(
  const std::string& _oid,
  utime_t dur)
{
  oid = _oid;
  op.assert_exists();
  lock.set_duration(dur);
  lock.lock_exclusive(&op);
  int ret = ioctx.operate(oid, &op);
  if (! ret) {
    locked = true;
  }
  return ret;
}

void RGWCompleteMultipart::complete()
{
  /* release exclusive lock iff not already */
  if (unlikely(serializer.locked)) {
    int r = serializer.unlock();
    if (r < 0) {
      ldpp_dout(this, 0) << "WARNING: failed to unlock " << serializer.oid << dendl;
    }
  }
  send_response();
}

int RGWAbortMultipart::verify_permission()
{
  if (s->iam_policy || ! s->iam_user_policies.empty()) {
    auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env,
                                              boost::none,
                                              rgw::IAM::s3AbortMultipartUpload,
                                              rgw_obj(s->bucket, s->object));
    if (usr_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect e = Effect::Pass;
    if (s->iam_policy) {
      e = s->iam_policy->eval(s->env, *s->auth.identity,
				 rgw::IAM::s3AbortMultipartUpload,
				 rgw_obj(s->bucket, s->object));
    }
    if (e == Effect::Allow) {
      return 0;
    } else if (e == Effect::Deny) {
      return -EACCES;
    } else if (usr_policy_res == Effect::Allow)
      return 0;
  }

  if (!verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

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
  rgw_obj meta_obj;
  RGWMPObj mp;

  if (upload_id.empty() || s->object.empty())
    return;

  mp.init(s->object.name, upload_id);
  meta_oid = mp.get_meta();

  op_ret = get_multipart_info(store, s, meta_oid, nullptr, nullptr, nullptr);
  if (op_ret < 0)
    return;

  RGWObjectCtx *obj_ctx = static_cast<RGWObjectCtx *>(s->obj_ctx);
  op_ret = abort_multipart_upload(store, s->cct, obj_ctx, s->bucket_info, mp);
}

int RGWListMultipart::verify_permission()
{
  if (!verify_object_permission(this, s, rgw::IAM::s3ListMultipartUploadParts))
    return -EACCES;

  return 0;
}

void RGWListMultipart::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWListMultipart::execute()
{
  string meta_oid;
  RGWMPObj mp;

  op_ret = get_params();
  if (op_ret < 0)
    return;

  mp.init(s->object.name, upload_id);
  meta_oid = mp.get_meta();

  op_ret = get_multipart_info(store, s, meta_oid, &policy, nullptr, nullptr);
  if (op_ret < 0)
    return;

  op_ret = list_multipart_parts(store, s, upload_id, meta_oid, max_parts,
				marker, parts, NULL, &truncated);
}

int RGWListBucketMultiparts::verify_permission()
{
  if (!verify_bucket_permission(this,
                                s,
				rgw::IAM::s3ListBucketMultipartUploads))
    return -EACCES;

  return 0;
}

void RGWListBucketMultiparts::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWListBucketMultiparts::execute()
{
  vector<rgw_bucket_dir_entry> objs;
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

  op_ret = list_bucket_multiparts(store, s->bucket_info, prefix, marker_meta, delimiter,
                                  max_uploads, &objs, &common_prefixes, &is_truncated);
  if (op_ret < 0) {
    return;
  }

  if (!objs.empty()) {
    vector<rgw_bucket_dir_entry>::iterator iter;
    RGWMultipartUploadEntry entry;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      rgw_obj_key key(iter->key);
      if (!entry.mp.from_meta(key.name))
        continue;
      entry.obj = *iter;
      uploads.push_back(entry);
    }
    next_marker = entry;
  }
}

void RGWGetHealthCheck::execute()
{
  if (!g_conf()->rgw_healthcheck_disabling_path.empty() &&
      (::access(g_conf()->rgw_healthcheck_disabling_path.c_str(), F_OK) == 0)) {
    /* Disabling path specified & existent in the filesystem. */
    op_ret = -ERR_SERVICE_UNAVAILABLE; /* 503 */
  } else {
    op_ret = 0; /* 200 OK */
  }
}

int RGWDeleteMultiObj::verify_permission()
{
  if (s->iam_policy || ! s->iam_user_policies.empty()) {
    auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env,
                                              boost::none,
                                              s->object.instance.empty() ?
                                              rgw::IAM::s3DeleteObject :
                                              rgw::IAM::s3DeleteObjectVersion,
                                              ARN(s->bucket));
    if (usr_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect r = Effect::Pass;
    if (s->iam_policy) {
      r = s->iam_policy->eval(s->env, *s->auth.identity,
				 s->object.instance.empty() ?
				 rgw::IAM::s3DeleteObject :
				 rgw::IAM::s3DeleteObjectVersion,
				 ARN(s->bucket));
    }
    if (r == Effect::Allow)
      return 0;
    else if (r == Effect::Deny)
      return -EACCES;
    else if (usr_policy_res == Effect::Allow)
      return 0;
  }

  acl_allowed = verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE);
  if (!acl_allowed)
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
  RGWObjectCtx *obj_ctx = static_cast<RGWObjectCtx *>(s->obj_ctx);
  char* buf;

  op_ret = get_params();
  if (op_ret < 0) {
    goto error;
  }

  buf = data.c_str();
  if (!buf) {
    op_ret = -EINVAL;
    goto error;
  }

  if (!parser.init()) {
    op_ret = -EINVAL;
    goto error;
  }

  if (!parser.parse(buf, data.length(), 1)) {
    op_ret = -EINVAL;
    goto error;
  }

  multi_delete = static_cast<RGWMultiDelDelete *>(parser.find_first("Delete"));
  if (!multi_delete) {
    op_ret = -EINVAL;
    goto error;
  } else {
#define DELETE_MULTI_OBJ_MAX_NUM      1000
    int max_num = s->cct->_conf->rgw_delete_multi_obj_max_num;
    if (max_num < 0) {
      max_num = DELETE_MULTI_OBJ_MAX_NUM;
    }
    int multi_delete_object_num = multi_delete->objects.size();
    if (multi_delete_object_num > max_num) {
      op_ret = -ERR_MALFORMED_XML;
      goto error;
    }
  }

  if (multi_delete->is_quiet())
    quiet = true;

  if (s->bucket_info.mfa_enabled()) {
    bool has_versioned = false;
    for (auto i : multi_delete->objects) {
      if (!i.instance.empty()) {
        has_versioned = true;
        break;
      }
    }
    if (has_versioned && !s->mfa_verified) {
      ldpp_dout(this, 5) << "NOTICE: multi-object delete request with a versioned object, mfa auth not provided" << dendl;
      op_ret = -ERR_MFA_REQUIRED;
      goto error;
    }
  }

  begin_response();
  if (multi_delete->objects.empty()) {
    goto done;
  }

  for (iter = multi_delete->objects.begin();
        iter != multi_delete->objects.end();
        ++iter) {
    rgw_obj obj(bucket, *iter);
    if (s->iam_policy || ! s->iam_user_policies.empty()) {
      auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env,
                                              boost::none,
                                              iter->instance.empty() ?
                                              rgw::IAM::s3DeleteObject :
                                              rgw::IAM::s3DeleteObjectVersion,
                                              ARN(obj));
      if (usr_policy_res == Effect::Deny) {
        send_partial_response(*iter, false, "", -EACCES);
        continue;
      }

      rgw::IAM::Effect e = Effect::Pass;
      if (s->iam_policy) {
        e = s->iam_policy->eval(s->env,
				   *s->auth.identity,
				   iter->instance.empty() ?
				   rgw::IAM::s3DeleteObject :
				   rgw::IAM::s3DeleteObjectVersion,
				   ARN(obj));
      }
      if ((e == Effect::Deny) ||
	  (usr_policy_res == Effect::Pass && e == Effect::Pass && !acl_allowed)) {
	send_partial_response(*iter, false, "", -EACCES);
	continue;
      }
    }

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
  return;

error:
  send_status();
  return;

}

bool RGWBulkDelete::Deleter::verify_permission(RGWBucketInfo& binfo,
                                               map<string, bufferlist>& battrs,
                                               ACLOwner& bucket_owner /* out */)
{
  RGWAccessControlPolicy bacl(store->ctx());
  int ret = read_bucket_policy(store, s, binfo, battrs, &bacl, binfo.bucket);
  if (ret < 0) {
    return false;
  }

  auto policy = get_iam_policy_from_attr(s->cct, store, battrs, binfo.bucket.tenant);

  bucket_owner = bacl.get_owner();

  /* We can use global user_acl because each BulkDelete request is allowed
   * to work on entities from a single account only. */
  return verify_bucket_permission(dpp, s, binfo.bucket, s->user_acl.get(),
				  &bacl, policy, s->iam_user_policies, rgw::IAM::s3DeleteBucket);
}

bool RGWBulkDelete::Deleter::delete_single(const acct_path_t& path)
{
  auto& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);

  RGWBucketInfo binfo;
  map<string, bufferlist> battrs;
  ACLOwner bowner;

  int ret = store->get_bucket_info(*s->sysobj_ctx, s->user->user_id.tenant,
                                   path.bucket_name, binfo, nullptr,
                                   &battrs);
  if (ret < 0) {
    goto binfo_fail;
  }

  if (!verify_permission(binfo, battrs, bowner)) {
    ret = -EACCES;
    goto auth_fail;
  }

  if (!path.obj_key.empty()) {
    rgw_obj obj(binfo.bucket, path.obj_key);
    obj_ctx.set_atomic(obj);

    RGWRados::Object del_target(store, binfo, obj_ctx, obj);
    RGWRados::Object::Delete del_op(&del_target);

    del_op.params.bucket_owner = binfo.owner;
    del_op.params.versioning_status = binfo.versioning_status();
    del_op.params.obj_owner = bowner;

    ret = del_op.delete_obj();
    if (ret < 0) {
      goto delop_fail;
    }
  } else {
    RGWObjVersionTracker ot;
    ot.read_version = binfo.ep_objv;

    ret = store->delete_bucket(binfo, ot);
    if (0 == ret) {
      ret = rgw_unlink_bucket(store, binfo.owner, binfo.bucket.tenant,
                              binfo.bucket.name, false);
      if (ret < 0) {
        ldpp_dout(s, 0) << "WARNING: failed to unlink bucket: ret=" << ret << dendl;
      }
    }
    if (ret < 0) {
      goto delop_fail;
    }

    if (!store->svc.zone->is_meta_master()) {
      bufferlist in_data;
      ret = forward_request_to_master(s, &ot.read_version, store, in_data,
                                      nullptr);
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
      ldpp_dout(s, 20) << "cannot find bucket = " << path.bucket_name << dendl;
      num_unfound++;
    } else {
      ldpp_dout(s, 20) << "cannot get bucket info, ret = " << ret << dendl;

      fail_desc_t failed_item = {
        .err  = ret,
        .path = path
      };
      failures.push_back(failed_item);
    }
    return false;

auth_fail:
    ldpp_dout(s, 20) << "wrong auth for " << path << dendl;
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
      ldpp_dout(s, 20) << "cannot find entry " << path << dendl;
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
  ldpp_dout(s, 20) << "in delete_chunk" << dendl;
  for (auto path : paths) {
    ldpp_dout(s, 20) << "bulk deleting path: " << path << dendl;
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
  deleter = std::unique_ptr<Deleter>(new Deleter(this, store, s));

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


constexpr std::array<int, 2> RGWBulkUploadOp::terminal_errors;

int RGWBulkUploadOp::verify_permission()
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if (! verify_user_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

  if (s->user->user_id.tenant != s->bucket_tenant) {
    ldpp_dout(this, 10) << "user cannot create a bucket in a different tenant"
        << " (user_id.tenant=" << s->user->user_id.tenant
        << " requested=" << s->bucket_tenant << ")" << dendl;
    return -EACCES;
  }

  if (s->user->max_buckets < 0) {
    return -EPERM;
  }

  return 0;
}

void RGWBulkUploadOp::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

boost::optional<std::pair<std::string, rgw_obj_key>>
RGWBulkUploadOp::parse_path(const boost::string_ref& path)
{
  /* We need to skip all slashes at the beginning in order to preserve
   * compliance with Swift. */
  const size_t start_pos = path.find_first_not_of('/');

  if (boost::string_ref::npos != start_pos) {
    /* Seperator is the first slash after the leading ones. */
    const size_t sep_pos = path.substr(start_pos).find('/');

    if (boost::string_ref::npos != sep_pos) {
      const auto bucket_name = path.substr(start_pos, sep_pos - start_pos);
      const auto obj_name = path.substr(sep_pos + 1);

      return std::make_pair(bucket_name.to_string(),
                            rgw_obj_key(obj_name.to_string()));
    } else {
      /* It's guaranteed here that bucket name is at least one character
       * long and is different than slash. */
      return std::make_pair(path.substr(start_pos).to_string(),
                            rgw_obj_key());
    }
  }

  return none;
}

std::pair<std::string, std::string>
RGWBulkUploadOp::handle_upload_path(struct req_state *s)
{
  std::string bucket_path, file_prefix;
  if (! s->init_state.url_bucket.empty()) {
    file_prefix = bucket_path = s->init_state.url_bucket + "/";
    if (! s->object.empty()) {
      std::string& object_name = s->object.name;

      /* As rgw_obj_key::empty() already verified emptiness of s->object.name,
       * we can safely examine its last element. */
      if (object_name.back() == '/') {
        file_prefix.append(object_name);
      } else {
        file_prefix.append(object_name).append("/");
      }
    }
  }
  return std::make_pair(bucket_path, file_prefix);
}

int RGWBulkUploadOp::handle_dir_verify_permission()
{
  if (s->user->max_buckets > 0) {
    RGWUserBuckets buckets;
    std::string marker;
    bool is_truncated = false;
    op_ret = rgw_read_user_buckets(store, s->user->user_id, buckets,
                                   marker, std::string(), s->user->max_buckets,
                                   false, &is_truncated);
    if (op_ret < 0) {
      return op_ret;
    }

    if (buckets.count() >= static_cast<size_t>(s->user->max_buckets)) {
      return -ERR_TOO_MANY_BUCKETS;
    }
  }

  return 0;
}

static void forward_req_info(CephContext *cct, req_info& info, const std::string& bucket_name)
{
  /* the request of container or object level will contain bucket name.
   * only at account level need to append the bucket name */
  if (info.script_uri.find(bucket_name) != std::string::npos) {
    return;
  }

  ldout(cct, 20) << "append the bucket: "<< bucket_name << " to req_info" << dendl;
  info.script_uri.append("/").append(bucket_name);
  info.request_uri_aws4 = info.request_uri = info.script_uri;
  info.effective_uri = "/" + bucket_name;
}

void RGWBulkUploadOp::init(RGWRados* const store,
                           struct req_state* const s,
                           RGWHandler* const h)
{
  RGWOp::init(store, s, h);
  dir_ctx.emplace(store->svc.sysobj->init_obj_ctx());
}

int RGWBulkUploadOp::handle_dir(const boost::string_ref path)
{
  ldpp_dout(this, 20) << "got directory=" << path << dendl;

  op_ret = handle_dir_verify_permission();
  if (op_ret < 0) {
    return op_ret;
  }

  std::string bucket_name;
  rgw_obj_key object_junk;
  std::tie(bucket_name, object_junk) =  *parse_path(path);

  rgw_raw_obj obj(store->svc.zone->get_zone_params().domain_root,
                  rgw_make_bucket_entry_name(s->bucket_tenant, bucket_name));

  /* we need to make sure we read bucket info, it's not read before for this
   * specific request */
  RGWBucketInfo binfo;
  std::map<std::string, ceph::bufferlist> battrs;
  op_ret = store->get_bucket_info(*dir_ctx, s->bucket_tenant, bucket_name,
                                  binfo, nullptr, &battrs);
  if (op_ret < 0 && op_ret != -ENOENT) {
    return op_ret;
  }
  const bool bucket_exists = (op_ret != -ENOENT);

  if (bucket_exists) {
    RGWAccessControlPolicy old_policy(s->cct);
    int r = rgw_op_get_bucket_policy_from_attr(s->cct, store, binfo,
                                               battrs, &old_policy);
    if (r >= 0)  {
      if (old_policy.get_owner().get_id().compare(s->user->user_id) != 0) {
        op_ret = -EEXIST;
        return op_ret;
      }
    }
  }

  RGWBucketInfo master_info;
  rgw_bucket *pmaster_bucket = nullptr;
  uint32_t *pmaster_num_shards = nullptr;
  real_time creation_time;
  obj_version objv, ep_objv, *pobjv = nullptr;

  if (! store->svc.zone->is_meta_master()) {
    JSONParser jp;
    ceph::bufferlist in_data;
    req_info info = s->info;
    forward_req_info(s->cct, info, bucket_name);
    op_ret = forward_request_to_master(s, nullptr, store, in_data, &jp, &info);
    if (op_ret < 0) {
      return op_ret;
    }

    JSONDecoder::decode_json("entry_point_object_ver", ep_objv, &jp);
    JSONDecoder::decode_json("object_ver", objv, &jp);
    JSONDecoder::decode_json("bucket_info", master_info, &jp);

    ldpp_dout(this, 20) << "parsed: objv.tag=" << objv.tag << " objv.ver=" << objv.ver << dendl;
    ldpp_dout(this, 20) << "got creation_time="<< master_info.creation_time << dendl;

    pmaster_bucket= &master_info.bucket;
    creation_time = master_info.creation_time;
    pmaster_num_shards = &master_info.num_shards;
    pobjv = &objv;
  } else {
    pmaster_bucket = nullptr;
    pmaster_num_shards = nullptr;
  }

  rgw_placement_rule placement_rule(binfo.placement_rule, s->info.storage_class);

  if (bucket_exists) {
    rgw_placement_rule selected_placement_rule;
    rgw_bucket bucket;
    bucket.tenant = s->bucket_tenant;
    bucket.name = s->bucket_name;
    op_ret = store->svc.zone->select_bucket_placement(*(s->user),
                                            store->svc.zone->get_zonegroup().get_id(),
                                            placement_rule,
                                            &selected_placement_rule,
                                            nullptr);
    if (selected_placement_rule != binfo.placement_rule) {
      op_ret = -EEXIST;
      ldpp_dout(this, 20) << "non-coherent placement rule" << dendl;
      return op_ret;
    }
  }

  /* Create metadata: ACLs. */
  std::map<std::string, ceph::bufferlist> attrs;
  RGWAccessControlPolicy policy;
  policy.create_default(s->user->user_id, s->user->display_name);
  ceph::bufferlist aclbl;
  policy.encode(aclbl);
  attrs.emplace(RGW_ATTR_ACL, std::move(aclbl));

  RGWQuotaInfo quota_info;
  const RGWQuotaInfo * pquota_info = nullptr;

  rgw_bucket bucket;
  bucket.tenant = s->bucket_tenant; /* ignored if bucket exists */
  bucket.name = bucket_name;


  RGWBucketInfo out_info;
  op_ret = store->create_bucket(*(s->user),
                                bucket,
                                store->svc.zone->get_zonegroup().get_id(),
                                placement_rule, binfo.swift_ver_location,
                                pquota_info, attrs,
                                out_info, pobjv, &ep_objv, creation_time,
                                pmaster_bucket, pmaster_num_shards, true);
  /* continue if EEXIST and create_bucket will fail below.  this way we can
   * recover from a partial create by retrying it. */
  ldpp_dout(this, 20) << "rgw_create_bucket returned ret=" << op_ret
      << ", bucket=" << bucket << dendl;

  if (op_ret && op_ret != -EEXIST) {
    return op_ret;
  }

  const bool existed = (op_ret == -EEXIST);
  if (existed) {
    /* bucket already existed, might have raced with another bucket creation, or
     * might be partial bucket creation that never completed. Read existing bucket
     * info, verify that the reported bucket owner is the current user.
     * If all is ok then update the user's list of buckets.
     * Otherwise inform client about a name conflict.
     */
    if (out_info.owner.compare(s->user->user_id) != 0) {
      op_ret = -EEXIST;
      ldpp_dout(this, 20) << "conflicting bucket name" << dendl;
      return op_ret;
    }
    bucket = out_info.bucket;
  }

  op_ret = rgw_link_bucket(store, s->user->user_id, bucket,
                           out_info.creation_time, false);
  if (op_ret && !existed && op_ret != -EEXIST) {
    /* if it exists (or previously existed), don't remove it! */
    op_ret = rgw_unlink_bucket(store, s->user->user_id,
                               bucket.tenant, bucket.name);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "WARNING: failed to unlink bucket: ret=" << op_ret << dendl;
    }
  } else if (op_ret == -EEXIST || (op_ret == 0 && existed)) {
    ldpp_dout(this, 20) << "containers already exists" << dendl;
    op_ret = -ERR_BUCKET_EXISTS;
  }

  return op_ret;
}


bool RGWBulkUploadOp::handle_file_verify_permission(RGWBucketInfo& binfo,
						    const rgw_obj& obj,
                                                    std::map<std::string, ceph::bufferlist>& battrs,
                                                    ACLOwner& bucket_owner /* out */)
{
  RGWAccessControlPolicy bacl(store->ctx());
  op_ret = read_bucket_policy(store, s, binfo, battrs, &bacl, binfo.bucket);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "cannot read_policy() for bucket" << dendl;
    return false;
  }

  auto policy = get_iam_policy_from_attr(s->cct, store, battrs, binfo.bucket.tenant);

  bucket_owner = bacl.get_owner();
  if (policy || ! s->iam_user_policies.empty()) {
    auto usr_policy_res = eval_user_policies(s->iam_user_policies, s->env,
                                              boost::none,
                                              rgw::IAM::s3PutObject, obj);
    if (usr_policy_res == Effect::Deny) {
      return false;
    }
    auto e = policy->eval(s->env, *s->auth.identity,
			  rgw::IAM::s3PutObject, obj);
    if (e == Effect::Allow) {
      return true;
    } else if (e == Effect::Deny) {
      return false;
    } else if (usr_policy_res == Effect::Allow) {
      return true;
    }
  }
    
  return verify_bucket_permission_no_policy(this, s, s->user_acl.get(),
					    &bacl, RGW_PERM_WRITE);
}

int RGWBulkUploadOp::handle_file(const boost::string_ref path,
                                 const size_t size,
                                 AlignedStreamGetter& body)
{

  ldpp_dout(this, 20) << "got file=" << path << ", size=" << size << dendl;

  if (size > static_cast<size_t>(s->cct->_conf->rgw_max_put_size)) {
    op_ret = -ERR_TOO_LARGE;
    return op_ret;
  }

  std::string bucket_name;
  rgw_obj_key object;
  std::tie(bucket_name, object) = *parse_path(path);

  auto& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  RGWBucketInfo binfo;
  std::map<std::string, ceph::bufferlist> battrs;
  ACLOwner bowner;
  op_ret = store->get_bucket_info(*s->sysobj_ctx, s->user->user_id.tenant,
                                  bucket_name, binfo, nullptr, &battrs);
  if (op_ret == -ENOENT) {
    ldpp_dout(this, 20) << "non existent directory=" << bucket_name << dendl;
  } else if (op_ret < 0) {
    return op_ret;
  }

  if (! handle_file_verify_permission(binfo,
				      rgw_obj(binfo.bucket, object),
				      battrs, bowner)) {
    ldpp_dout(this, 20) << "object creation unauthorized" << dendl;
    op_ret = -EACCES;
    return op_ret;
  }

  op_ret = store->check_quota(bowner.get_id(), binfo.bucket,
                              user_quota, bucket_quota, size);
  if (op_ret < 0) {
    return op_ret;
  }

  op_ret = store->check_bucket_shards(s->bucket_info, s->bucket, bucket_quota);
  if (op_ret < 0) {
    return op_ret;
  }

  rgw_obj obj(binfo.bucket, object);
  if (s->bucket_info.versioning_enabled()) {
    store->gen_rand_obj_instance_name(&obj);
  }

  rgw_placement_rule dest_placement = s->dest_placement;
  dest_placement.inherit_from(binfo.placement_rule);

  rgw::AioThrottle aio(store->ctx()->_conf->rgw_put_obj_min_window_size);

  using namespace rgw::putobj;

  AtomicObjectProcessor processor(&aio, store, binfo, &s->dest_placement, bowner.get_id(),
                                  obj_ctx, obj, 0, s->req_id);

  op_ret = processor.prepare();
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "cannot prepare processor due to ret=" << op_ret << dendl;
    return op_ret;
  }

  /* No filters by default. */
  DataProcessor *filter = &processor;

  const auto& compression_type = store->svc.zone->get_zone_params().get_compression_type(
      dest_placement);
  CompressorRef plugin;
  boost::optional<RGWPutObj_Compress> compressor;
  if (compression_type != "none") {
    plugin = Compressor::create(s->cct, compression_type);
    if (! plugin) {
      ldpp_dout(this, 1) << "Cannot load plugin for rgw_compression_type "
          << compression_type << dendl;
    } else {
      compressor.emplace(s->cct, plugin, filter);
      filter = &*compressor;
    }
  }

  /* Upload file content. */
  ssize_t len = 0;
  size_t ofs = 0;
  MD5 hash;
  do {
    ceph::bufferlist data;
    len = body.get_at_most(s->cct->_conf->rgw_max_chunk_size, data);

    ldpp_dout(this, 20) << "body=" << data.c_str() << dendl;
    if (len < 0) {
      op_ret = len;
      return op_ret;
    } else if (len > 0) {
      hash.Update((const unsigned char *)data.c_str(), data.length());
      op_ret = filter->process(std::move(data), ofs);
      if (op_ret < 0) {
        ldpp_dout(this, 20) << "filter->process() returned ret=" << op_ret << dendl;
        return op_ret;
      }

      ofs += len;
    }

  } while (len > 0);

  // flush
  op_ret = filter->process({}, ofs);
  if (op_ret < 0) {
    return op_ret;
  }

  if (ofs != size) {
    ldpp_dout(this, 10) << "real file size different from declared" << dendl;
    op_ret = -EINVAL;
    return op_ret;
  }

  op_ret = store->check_quota(bowner.get_id(), binfo.bucket,
			      user_quota, bucket_quota, size);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "quota exceeded for path=" << path << dendl;
    return op_ret;
  }

  op_ret = store->check_bucket_shards(s->bucket_info, s->bucket, bucket_quota);
  if (op_ret < 0) {
    return op_ret;
  }

  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  hash.Final(m);
  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);

  /* Create metadata: ETAG. */
  std::map<std::string, ceph::bufferlist> attrs;
  std::string etag = calc_md5;
  ceph::bufferlist etag_bl;
  etag_bl.append(etag.c_str(), etag.size() + 1);
  attrs.emplace(RGW_ATTR_ETAG, std::move(etag_bl));

  /* Create metadata: ACLs. */
  RGWAccessControlPolicy policy;
  policy.create_default(s->user->user_id, s->user->display_name);
  ceph::bufferlist aclbl;
  policy.encode(aclbl);
  attrs.emplace(RGW_ATTR_ACL, std::move(aclbl));

  /* Create metadata: compression info. */
  if (compressor && compressor->is_compressed()) {
    ceph::bufferlist tmp;
    RGWCompressionInfo cs_info;
    cs_info.compression_type = plugin->get_type_name();
    cs_info.orig_size = s->obj_size;
    cs_info.blocks = std::move(compressor->get_compression_blocks());
    encode(cs_info, tmp);
    attrs.emplace(RGW_ATTR_COMPRESSION, std::move(tmp));
  }

  /* Complete the transaction. */
  op_ret = processor.complete(size, etag, nullptr, ceph::real_time(),
                              attrs, ceph::real_time() /* delete_at */,
                              nullptr, nullptr, nullptr, nullptr, nullptr);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "processor::complete returned op_ret=" << op_ret << dendl;
  }

  return op_ret;
}

void RGWBulkUploadOp::execute()
{
  ceph::bufferlist buffer(64 * 1024);

  ldpp_dout(this, 20) << "start" << dendl;

  /* Create an instance of stream-abstracting class. Having this indirection
   * allows for easy introduction of decompressors like gzip and bzip2. */
  auto stream = create_stream();
  if (! stream) {
    return;
  }

  /* Handling the $UPLOAD_PATH accordingly to the Swift's Bulk middleware. See: 
   * https://github.com/openstack/swift/blob/2.13.0/swift/common/middleware/bulk.py#L31-L41 */
  std::string bucket_path, file_prefix;
  std::tie(bucket_path, file_prefix) = handle_upload_path(s);

  auto status = rgw::tar::StatusIndicator::create();
  do {
    op_ret = stream->get_exactly(rgw::tar::BLOCK_SIZE, buffer);
    if (op_ret < 0) {
      ldpp_dout(this, 2) << "cannot read header" << dendl;
      return;
    }

    /* We need to re-interpret the buffer as a TAR block. Exactly two blocks
     * must be tracked to detect out end-of-archive. It occurs when both of
     * them are empty (zeroed). Tracing this particular inter-block dependency
     * is responsibility of the rgw::tar::StatusIndicator class. */
    boost::optional<rgw::tar::HeaderView> header;
    std::tie(status, header) = rgw::tar::interpret_block(status, buffer);

    if (! status.empty() && header) {
      /* This specific block isn't empty (entirely zeroed), so we can parse
       * it as a TAR header and dispatch. At the moment we do support only
       * regular files and directories. Everything else (symlinks, devices)
       * will be ignored but won't cease the whole upload. */
      switch (header->get_filetype()) {
        case rgw::tar::FileType::NORMAL_FILE: {
          ldpp_dout(this, 2) << "handling regular file" << dendl;

          boost::string_ref filename = bucket_path.empty() ? header->get_filename() : \
                            file_prefix + header->get_filename().to_string();
          auto body = AlignedStreamGetter(0, header->get_filesize(),
                                          rgw::tar::BLOCK_SIZE, *stream);
          op_ret = handle_file(filename,
                               header->get_filesize(),
                               body);
          if (! op_ret) {
            /* Only regular files counts. */
            num_created++;
          } else {
            failures.emplace_back(op_ret, filename.to_string());
          }
          break;
        }
        case rgw::tar::FileType::DIRECTORY: {
          ldpp_dout(this, 2) << "handling regular directory" << dendl;

          boost::string_ref dirname = bucket_path.empty() ? header->get_filename() : bucket_path;
          op_ret = handle_dir(dirname);
          if (op_ret < 0 && op_ret != -ERR_BUCKET_EXISTS) {
            failures.emplace_back(op_ret, dirname.to_string());
          }
          break;
        }
        default: {
          /* Not recognized. Skip. */
          op_ret = 0;
          break;
        }
      }

      /* In case of any problems with sub-request authorization Swift simply
       * terminates whole upload immediately. */
      if (boost::algorithm::contains(std::initializer_list<int>{ op_ret },
                                     terminal_errors)) {
        ldpp_dout(this, 2) << "terminating due to ret=" << op_ret << dendl;
        break;
      }
    } else {
      ldpp_dout(this, 2) << "an empty block" << dendl;
      op_ret = 0;
    }

    buffer.clear();
  } while (! status.eof());

  return;
}

RGWBulkUploadOp::AlignedStreamGetter::~AlignedStreamGetter()
{
  const size_t aligned_legnth = length + (-length % alignment);
  ceph::bufferlist junk;

  DecoratedStreamGetter::get_exactly(aligned_legnth - position, junk);
}

ssize_t RGWBulkUploadOp::AlignedStreamGetter::get_at_most(const size_t want,
                                                          ceph::bufferlist& dst)
{
  const size_t max_to_read = std::min(want, length - position);
  const auto len = DecoratedStreamGetter::get_at_most(max_to_read, dst);
  if (len > 0) {
    position += len;
  }
  return len;
}

ssize_t RGWBulkUploadOp::AlignedStreamGetter::get_exactly(const size_t want,
                                                          ceph::bufferlist& dst)
{
  const auto len = DecoratedStreamGetter::get_exactly(want, dst);
  if (len > 0) {
    position += len;
  }
  return len;
}

int RGWSetAttrs::verify_permission()
{
  // This looks to be part of the RGW-NFS machinery and has no S3 or
  // Swift equivalent.
  bool perm;
  if (!s->object.empty()) {
    perm = verify_object_permission_no_policy(this, s, RGW_PERM_WRITE);
  } else {
    perm = verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE);
  }
  if (!perm)
    return -EACCES;

  return 0;
}

void RGWSetAttrs::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWSetAttrs::execute()
{
  op_ret = get_params();
  if (op_ret < 0)
    return;

  rgw_obj obj(s->bucket, s->object);

  if (!s->object.empty()) {
    store->set_atomic(s->obj_ctx, obj);
    op_ret = store->set_attrs(s->obj_ctx, s->bucket_info, obj, attrs, nullptr);
  } else {
    for (auto& iter : attrs) {
      s->bucket_attrs[iter.first] = std::move(iter.second);
    }
    op_ret = rgw_bucket_set_attrs(store, s->bucket_info, s->bucket_attrs,
				  &s->bucket_info.objv_tracker);
  }
}

void RGWGetObjLayout::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetObjLayout::execute()
{
  rgw_obj obj(s->bucket, s->object);
  RGWRados::Object target(store,
                          s->bucket_info,
                          *static_cast<RGWObjectCtx *>(s->obj_ctx),
                          rgw_obj(s->bucket, s->object));
  RGWRados::Object::Read stat_op(&target);

  op_ret = stat_op.prepare();
  if (op_ret < 0) {
    return;
  }

  head_obj = stat_op.state.head_obj;

  op_ret = target.get_manifest(&manifest);
}


int RGWConfigBucketMetaSearch::verify_permission()
{
  if (!s->auth.identity->is_owner_of(s->bucket_owner.get_id())) {
    return -EACCES;
  }

  return 0;
}

void RGWConfigBucketMetaSearch::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWConfigBucketMetaSearch::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "NOTICE: get_params() returned ret=" << op_ret << dendl;
    return;
  }

  s->bucket_info.mdsearch_config = mdsearch_config;

  op_ret = store->put_bucket_instance_info(s->bucket_info, false, real_time(), &s->bucket_attrs);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name
        << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWGetBucketMetaSearch::verify_permission()
{
  if (!s->auth.identity->is_owner_of(s->bucket_owner.get_id())) {
    return -EACCES;
  }

  return 0;
}

void RGWGetBucketMetaSearch::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

int RGWDelBucketMetaSearch::verify_permission()
{
  if (!s->auth.identity->is_owner_of(s->bucket_owner.get_id())) {
    return -EACCES;
  }

  return 0;
}

void RGWDelBucketMetaSearch::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDelBucketMetaSearch::execute()
{
  s->bucket_info.mdsearch_config.clear();

  op_ret = store->put_bucket_instance_info(s->bucket_info, false, real_time(), &s->bucket_attrs);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name
        << " returned err=" << op_ret << dendl;
    return;
  }
}


RGWHandler::~RGWHandler()
{
}

int RGWHandler::init(RGWRados *_store,
                     struct req_state *_s,
                     rgw::io::BasicClient *cio)
{
  store = _store;
  s = _s;

  return 0;
}

int RGWHandler::do_init_permissions()
{
  int ret = rgw_build_bucket_policies(store, s);
  if (ret < 0) {
    ldpp_dout(s, 10) << "init_permissions on " << s->bucket
        << " failed, ret=" << ret << dendl;
    return ret==-ENODATA ? -EACCES : ret;
  }

  rgw_build_iam_environment(store, s);
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
    ldpp_dout(op, 10) << "read_permissions on " << s->bucket << ":"
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

std::ostream& RGWOp::gen_prefix(std::ostream& out) const
{
  // append <dialect>:<op name> to the prefix
  return s->gen_prefix(out) << s->dialect << ':' << name() << ' ';
}

void RGWDefaultResponseOp::send_response() {
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

void RGWPutBucketPolicy::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWPutBucketPolicy::verify_permission()
{
  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketPolicy)) {
    return -EACCES;
  }

  return 0;
}

int RGWPutBucketPolicy::get_params()
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  // At some point when I have more time I want to make a version of
  // rgw_rest_read_all_input that doesn't use malloc.
  std::tie(op_ret, data) = rgw_rest_read_all_input(s, max_size, false);

  // And throws exceptions.
  return op_ret;
}

void RGWPutBucketPolicy::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  if (!store->svc.zone->is_meta_master()) {
    op_ret = forward_request_to_master(s, NULL, store, data, nullptr);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
  }

  try {
    const Policy p(s->cct, s->bucket_tenant, data);
    op_ret = retry_raced_bucket_write(store, s, [&p, this] {
	auto attrs = s->bucket_attrs;
	attrs[RGW_ATTR_IAM_POLICY].clear();
	attrs[RGW_ATTR_IAM_POLICY].append(p.text);
	op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs,
				      &s->bucket_info.objv_tracker);
	return op_ret;
      });
  } catch (rgw::IAM::PolicyParseException& e) {
    ldpp_dout(this, 20) << "failed to parse policy: " << e.what() << dendl;
    op_ret = -EINVAL;
  }
}

void RGWGetBucketPolicy::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, "application/json");
  dump_body(s, policy);
}

int RGWGetBucketPolicy::verify_permission()
{
  if (!verify_bucket_permission(this, s, rgw::IAM::s3GetBucketPolicy)) {
    return -EACCES;
  }

  return 0;
}

void RGWGetBucketPolicy::execute()
{
  auto attrs = s->bucket_attrs;
  map<string, bufferlist>::iterator aiter = attrs.find(RGW_ATTR_IAM_POLICY);
  if (aiter == attrs.end()) {
    ldpp_dout(this, 0) << "can't find bucket IAM POLICY attr bucket_name = "
        << s->bucket_name << dendl;
    op_ret = -ERR_NO_SUCH_BUCKET_POLICY;
    s->err.message = "The bucket policy does not exist";
    return;
  } else {
    policy = attrs[RGW_ATTR_IAM_POLICY];

    if (policy.length() == 0) {
      ldpp_dout(this, 10) << "The bucket policy does not exist, bucket: "
          << s->bucket_name << dendl;
      op_ret = -ERR_NO_SUCH_BUCKET_POLICY;
      s->err.message = "The bucket policy does not exist";
      return;
    }
  } 
}

void RGWDeleteBucketPolicy::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWDeleteBucketPolicy::verify_permission()
{
  if (!verify_bucket_permission(this, s, rgw::IAM::s3DeleteBucketPolicy)) {
    return -EACCES;
  }

  return 0;
}

void RGWDeleteBucketPolicy::execute()
{
  op_ret = retry_raced_bucket_write(store, s, [this] {
      auto attrs = s->bucket_attrs;
      attrs.erase(RGW_ATTR_IAM_POLICY);
      op_ret = rgw_bucket_set_attrs(store, s->bucket_info, attrs,
				    &s->bucket_info.objv_tracker);
      return op_ret;
    });
}

void RGWGetClusterStat::execute()
{
  op_ret = this->store->get_rados_handle()->cluster_stat(stats_op);
}


