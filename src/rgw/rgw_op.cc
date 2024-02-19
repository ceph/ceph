// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <stdlib.h>
#include <system_error>
#include <unistd.h>

#include <sstream>
#include <string_view>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>

#include "include/scope_guard.h"
#include "common/Clock.h"
#include "common/armor.h"
#include "common/errno.h"
#include "common/mime.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/static_ptr.h"
#include "common/perf_counters_key.h"
#include "rgw_tracer.h"

#include "rgw_rados.h"
#include "rgw_zone.h"
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
#include "rgw_tar.h"
#include "rgw_client_io.h"
#include "rgw_compression.h"
#include "rgw_role.h"
#include "rgw_tag_s3.h"
#include "rgw_putobj_processor.h"
#include "rgw_crypt.h"
#include "rgw_perf_counters.h"
#include "rgw_process_env.h"
#include "rgw_notify.h"
#include "rgw_notify_event_type.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_torrent.h"
#include "rgw_lua_data_filter.h"
#include "rgw_lua.h"

#include "services/svc_zone.h"
#include "services/svc_quota.h"
#include "services/svc_sys_obj.h"

#include "cls/lock/cls_lock_client.h"
#include "cls/rgw/cls_rgw_client.h"


#include "include/ceph_assert.h"

#include "compressor/Compressor.h"

#ifdef WITH_ARROW_FLIGHT
#include "rgw_flight.h"
#include "rgw_flight_frontend.h"
#endif

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

using namespace std;
using namespace librados;
using ceph::crypto::MD5;
using boost::optional;
using boost::none;

using rgw::ARN;
using rgw::IAM::Effect;
using rgw::IAM::Policy;

static string mp_ns = RGW_OBJ_NS_MULTIPART;
static string shadow_ns = RGW_OBJ_NS_SHADOW;

static void forward_req_info(const DoutPrefixProvider *dpp, CephContext *cct, req_info& info, const std::string& bucket_name);

// this probably should belong in the rgw_iam_policy_keywords, I'll get it to it
// at some point
static constexpr auto S3_EXISTING_OBJTAG = "s3:ExistingObjectTag";
static constexpr auto S3_RESOURCE_TAG = "s3:ResourceTag";
static constexpr auto S3_RUNTIME_RESOURCE_VAL = "${s3:ResourceTag";

int rgw_forward_request_to_master(const DoutPrefixProvider* dpp,
                                  const rgw::SiteConfig& site,
                                  const rgw_user& uid,
                                  bufferlist* indata, JSONParser* jp,
                                  req_info& req, optional_yield y)
{
  const auto& period = site.get_period();
  if (!period) {
    return 0; // not multisite
  }
  if (site.is_meta_master()) {
    return 0; // don't need to forward metadata requests
  }
  const auto& pmap = period->period_map;
  auto zg = pmap.zonegroups.find(pmap.master_zonegroup);
  if (zg == pmap.zonegroups.end()) {
    return -EINVAL;
  }
  auto z = zg->second.zones.find(zg->second.master_zone);
  if (z == zg->second.zones.end()) {
    return -EINVAL;
  }
  const RGWAccessKey& creds = site.get_zone_params().system_key;

  bufferlist data;
  if (indata == nullptr) {
    // forward() needs an input bufferlist to set the content-length
    indata = &data;
  }

  // use the master zone's endpoints
  auto conn = RGWRESTConn{dpp->get_cct(), z->second.id, z->second.endpoints,
                          creds, zg->second.id, zg->second.api_name};
  bufferlist outdata;
  constexpr size_t max_response_size = 128 * 1024; // we expect a very small response
  int ret = conn.forward(dpp, uid, req, nullptr, max_response_size,
                         indata, &outdata, y);
  if (ret < 0) {
    return ret;
  }
  if (jp && !jp->parse(outdata.c_str(), outdata.length())) {
    ldpp_dout(dpp, 0) << "failed parsing response from master zonegroup" << dendl;
    return -EINVAL;
  }
  return 0;
}

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

static int decode_policy(const DoutPrefixProvider *dpp,
                         CephContext *cct,
                         bufferlist& bl,
                         RGWAccessControlPolicy& policy)
{
  auto iter = bl.cbegin();
  try {
    policy.decode(iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    ldpp_dout(dpp, 15) << __func__ << " Read AccessControlPolicy";
    rgw::s3::write_policy_xml(policy, *_dout);
    *_dout << dendl;
  }
  return 0;
}


static int get_user_policy_from_attr(const DoutPrefixProvider *dpp,
                                     CephContext * const cct,
				     map<string, bufferlist>& attrs,
				     RGWAccessControlPolicy& policy    /* out */)
{
  auto i = attrs.find(RGW_ATTR_ACL);
  if (i == attrs.end()) {
    return -ENOENT;
  }
  return decode_policy(dpp, cct, i->second, policy);
}

/**
 * Get the AccessControlPolicy for an object off of disk.
 * policy: must point to a valid RGWACL, and will be filled upon return.
 * bucket: name of the bucket containing the object.
 * object: name of the object to get the ACL for.
 * Returns: 0 on success, -ERR# otherwise.
 */
int rgw_op_get_bucket_policy_from_attr(const DoutPrefixProvider *dpp, 
                                       CephContext *cct,
				       rgw::sal::Driver* driver,
				       const rgw_user& bucket_owner,
				       map<string, bufferlist>& bucket_attrs,
				       RGWAccessControlPolicy& policy,
				       optional_yield y)
{
  map<string, bufferlist>::iterator aiter = bucket_attrs.find(RGW_ATTR_ACL);

  if (aiter != bucket_attrs.end()) {
    int ret = decode_policy(dpp, cct, aiter->second, policy);
    if (ret < 0)
      return ret;
  } else {
    ldpp_dout(dpp, 0) << "WARNING: couldn't find acl header for bucket, generating default" << dendl;
    std::unique_ptr<rgw::sal::User> user = driver->get_user(bucket_owner);
    /* object exists, but policy is broken */
    int r = user->load_user(dpp, y);
    if (r < 0)
      return r;

    policy.create_default(user->get_id(), user->get_display_name());
  }
  return 0;
}

static int get_obj_policy_from_attr(const DoutPrefixProvider *dpp, 
                                    CephContext *cct,
				    rgw::sal::Driver* driver,
				    RGWBucketInfo& bucket_info,
				    map<string, bufferlist>& bucket_attrs,
				    RGWAccessControlPolicy& policy,
                                    string *storage_class,
				    rgw::sal::Object* obj,
                                    optional_yield y)
{
  bufferlist bl;
  int ret = 0;

  std::unique_ptr<rgw::sal::Object::ReadOp> rop = obj->get_read_op();

  ret = rop->get_attr(dpp, RGW_ATTR_ACL, bl, y);
  if (ret >= 0) {
    ret = decode_policy(dpp, cct, bl, policy);
    if (ret < 0)
      return ret;
  } else if (ret == -ENODATA) {
    /* object exists, but policy is broken */
    ldpp_dout(dpp, 0) << "WARNING: couldn't find acl header for object, generating default" << dendl;
    std::unique_ptr<rgw::sal::User> user = driver->get_user(bucket_info.owner);
    ret = user->load_user(dpp, y);
    if (ret < 0)
      return ret;

    policy.create_default(bucket_info.owner, user->get_display_name());
  }

  if (storage_class) {
    bufferlist scbl;
    int r = rop->get_attr(dpp, RGW_ATTR_STORAGE_CLASS, scbl, y);
    if (r >= 0) {
      *storage_class = scbl.to_str();
    } else {
      storage_class->clear();
    }
  }

  return ret;
}


static boost::optional<Policy> get_iam_policy_from_attr(CephContext* cct,
							map<string, bufferlist>& attrs,
							const string& tenant) {
  auto i = attrs.find(RGW_ATTR_IAM_POLICY);
  if (i != attrs.end()) {
    return Policy(cct, tenant, i->second, false);
  } else {
    return none;
  }
}

static boost::optional<PublicAccessBlockConfiguration>
get_public_access_conf_from_attr(const map<string, bufferlist>& attrs)
{
  if (auto aiter = attrs.find(RGW_ATTR_PUBLIC_ACCESS);
      aiter != attrs.end()) {
    bufferlist::const_iterator iter{&aiter->second};
    PublicAccessBlockConfiguration access_conf;
    try {
      access_conf.decode(iter);
    } catch (const buffer::error& e) {
      return boost::none;
    }
    return access_conf;
  }
  return boost::none;
}

vector<Policy> get_iam_user_policy_from_attr(CephContext* cct,
                        map<string, bufferlist>& attrs,
                        const string& tenant) {
  vector<Policy> policies;
  if (auto it = attrs.find(RGW_ATTR_USER_POLICY); it != attrs.end()) {
   bufferlist out_bl = attrs[RGW_ATTR_USER_POLICY];
   map<string, string> policy_map;
   decode(policy_map, out_bl);
   for (auto& it : policy_map) {
     bufferlist bl = bufferlist::static_from_string(it.second);
     Policy p(cct, tenant, bl, false);
     policies.push_back(std::move(p));
   }
  }
  return policies;
}

static int read_bucket_policy(const DoutPrefixProvider *dpp, 
                              rgw::sal::Driver* driver,
                              req_state *s,
                              RGWBucketInfo& bucket_info,
                              map<string, bufferlist>& bucket_attrs,
                              RGWAccessControlPolicy& policy,
                              rgw_bucket& bucket,
			      optional_yield y)
{
  if (!s->system_request && bucket_info.flags & BUCKET_SUSPENDED) {
    ldpp_dout(dpp, 0) << "NOTICE: bucket " << bucket_info.bucket.name
        << " is suspended" << dendl;
    return -ERR_USER_SUSPENDED;
  }

  if (bucket.name.empty()) {
    return 0;
  }

  int ret = rgw_op_get_bucket_policy_from_attr(dpp, s->cct, driver, bucket_info.owner,
                                               bucket_attrs, policy, y);
  if (ret == -ENOENT) {
    ret = -ERR_NO_SUCH_BUCKET;
  }

  return ret;
}

static int read_obj_policy(const DoutPrefixProvider *dpp, 
                           rgw::sal::Driver* driver,
                           req_state *s,
                           RGWBucketInfo& bucket_info,
                           map<string, bufferlist>& bucket_attrs,
                           RGWAccessControlPolicy& acl,
                           string *storage_class,
                           boost::optional<Policy>& policy,
                           rgw::sal::Bucket* bucket,
                           rgw::sal::Object* object,
                           optional_yield y,
                           bool copy_src=false)
{
  string upload_id;
  upload_id = s->info.args.get("uploadId");
  std::unique_ptr<rgw::sal::Object> mpobj;
  rgw_obj obj;

  if (!s->system_request && bucket_info.flags & BUCKET_SUSPENDED) {
    ldpp_dout(dpp, 0) << "NOTICE: bucket " << bucket_info.bucket.name
        << " is suspended" << dendl;
    return -ERR_USER_SUSPENDED;
  }

  // when getting policy info for copy-source obj, upload_id makes no sense.
  // 'copy_src' is used to make this function backward compatible.
  if (!upload_id.empty() && !copy_src) {
    /* multipart upload */
    std::unique_ptr<rgw::sal::MultipartUpload> upload;
    upload = bucket->get_multipart_upload(object->get_name(), upload_id);
    mpobj = upload->get_meta_obj();
    mpobj->set_in_extra_data(true);
    object = mpobj.get();
  }
  policy = get_iam_policy_from_attr(s->cct, bucket_attrs, bucket->get_tenant());

  int ret = get_obj_policy_from_attr(dpp, s->cct, driver, bucket_info,
				     bucket_attrs, acl, storage_class, object,
				     s->yield);
  if (ret == -ENOENT) {
    /* object does not exist checking the bucket's ACL to make sure
       that we send a proper error code */
    RGWAccessControlPolicy bucket_policy;
    ret = rgw_op_get_bucket_policy_from_attr(dpp, s->cct, driver, bucket_info.owner,
                                             bucket_attrs, bucket_policy, y);
    if (ret < 0) {
      return ret;
    }
    const rgw_user& bucket_owner = bucket_policy.get_owner().id;
    if (bucket_owner != s->user->get_id() &&
        ! s->auth.identity->is_admin_of(bucket_owner)) {
      auto r = eval_identity_or_session_policies(dpp, s->iam_user_policies, s->env,
                                  rgw::IAM::s3ListBucket, ARN(bucket->get_key()));
      if (r == Effect::Allow)
        return -ENOENT;
      if (r == Effect::Deny)
        return -EACCES;
      if (policy) {
        ARN b_arn(bucket->get_key());
        r = policy->eval(s->env, *s->auth.identity, rgw::IAM::s3ListBucket, b_arn);
        if (r == Effect::Allow)
          return -ENOENT;
        if (r == Effect::Deny)
          return -EACCES;
      }
      if (! s->session_policies.empty()) {
        r = eval_identity_or_session_policies(dpp, s->session_policies, s->env,
                                  rgw::IAM::s3ListBucket, ARN(bucket->get_key()));
        if (r == Effect::Allow)
          return -ENOENT;
        if (r == Effect::Deny)
          return -EACCES;
      }
      if (! bucket_policy.verify_permission(s, *s->auth.identity, s->perm_mask, RGW_PERM_READ))
        ret = -EACCES;
      else
        ret = -ENOENT;
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
int rgw_build_bucket_policies(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, req_state* s, optional_yield y)
{
  int ret = 0;

  string bi = s->info.args.get(RGW_SYS_PARAM_PREFIX "bucket-instance");
  if (!bi.empty()) {
    // note: overwrites s->bucket_name, may include a tenant/
    ret = rgw_bucket_parse_bucket_instance(bi, &s->bucket_name, &s->bucket_instance_id, &s->bucket_instance_shard_id);
    if (ret < 0) {
      return ret;
    }
  }

  const RGWZoneGroup& zonegroup = s->penv.site->get_zonegroup();

  /* check if copy source is within the current domain */
  if (!s->src_bucket_name.empty()) {
    std::unique_ptr<rgw::sal::Bucket> src_bucket;
    ret = driver->load_bucket(dpp, rgw_bucket(s->src_tenant_name,
                                              s->src_bucket_name),
                              &src_bucket, y);
    if (ret == 0) {
      s->local_source = zonegroup.equals(src_bucket->get_info().zonegroup);
    }
  }

  struct {
    rgw_user uid;
    std::string display_name;
  } acct_acl_user = {
    s->user->get_id(),
    s->user->get_display_name(),
  };

  if (!s->bucket_name.empty()) {
    s->bucket_exists = true;

    /* This is the only place that s->bucket is created.  It should never be
     * overwritten. */
    ret = driver->load_bucket(dpp, rgw_bucket(s->bucket_tenant, s->bucket_name,
                                              s->bucket_instance_id),
                              &s->bucket, y);
    if (ret < 0) {
      if (ret != -ENOENT) {
	string bucket_log;
	bucket_log = rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name);
	ldpp_dout(dpp, 0) << "NOTICE: couldn't get bucket from bucket_name (name="
	  << bucket_log << ")" << dendl;
	return ret;
      }
      s->bucket_exists = false;
      return -ERR_NO_SUCH_BUCKET;
    }
    if (!rgw::sal::Object::empty(s->object.get())) {
      s->object->set_bucket(s->bucket.get());
    }
    
    s->bucket_mtime = s->bucket->get_modification_time();
    s->bucket_attrs = s->bucket->get_attrs();
    ret = read_bucket_policy(dpp, driver, s, s->bucket->get_info(),
			     s->bucket->get_attrs(),
			     s->bucket_acl, s->bucket->get_key(), y);
    acct_acl_user = {
      s->bucket->get_info().owner,
      s->bucket_acl.get_owner().display_name,
    };

    s->bucket_owner = s->bucket_acl.get_owner();

    s->zonegroup_endpoint = rgw::get_zonegroup_endpoint(zonegroup);
    s->zonegroup_name = zonegroup.get_name();

    if (!zonegroup.equals(s->bucket->get_info().zonegroup)) {
      ldpp_dout(dpp, 0) << "NOTICE: request for data in a different zonegroup ("
          << s->bucket->get_info().zonegroup << " != "
          << zonegroup.get_id() << ")" << dendl;
      /* we now need to make sure that the operation actually requires copy source, that is
       * it's a copy operation
       */
      if (driver->get_zone()->get_zonegroup().is_master_zonegroup() && s->system_request) {
        /*If this is the master, don't redirect*/
      } else if (s->op_type == RGW_OP_GET_BUCKET_LOCATION ) {
        /* If op is get bucket location, don't redirect */
      } else if (!s->local_source ||
          (s->op != OP_PUT && s->op != OP_COPY) ||
          rgw::sal::Object::empty(s->object.get())) {
        return -ERR_PERMANENT_REDIRECT;
      }
    }

    /* init dest placement */
    s->dest_placement.storage_class = s->info.storage_class;
    s->dest_placement.inherit_from(s->bucket->get_placement_rule());

    if (!driver->valid_placement(s->dest_placement)) {
      ldpp_dout(dpp, 0) << "NOTICE: invalid dest placement: " << s->dest_placement.to_str() << dendl;
      return -EINVAL;
    }

    s->bucket_access_conf = get_public_access_conf_from_attr(s->bucket->get_attrs());
  }

  /* handle user ACL only for those APIs which support it */
  if (s->dialect == "swift" && !s->user->get_id().empty()) {
    std::unique_ptr<rgw::sal::User> acl_user = driver->get_user(acct_acl_user.uid);

    ret = acl_user->read_attrs(dpp, y);
    if (!ret) {
      ret = get_user_policy_from_attr(dpp, s->cct, acl_user->get_attrs(), s->user_acl);
    }
    if (-ENOENT == ret) {
      /* In already existing clusters users won't have ACL. In such case
       * assuming that only account owner has the rights seems to be
       * reasonable. That allows to have only one verification logic.
       * NOTE: there is small compatibility kludge for global, empty tenant:
       *  1. if we try to reach an existing bucket, its owner is considered
       *     as account owner.
       *  2. otherwise account owner is identity stored in s->user->user_id.  */
      s->user_acl.create_default(acct_acl_user.uid,
                                 acct_acl_user.display_name);
      ret = 0;
    } else if (ret < 0) {
      ldpp_dout(dpp, 0) << "NOTICE: couldn't get user attrs for handling ACL "
          "(user_id=" << s->user->get_id() << ", ret=" << ret << ")" << dendl;
      return ret;
    }
  }
  // We don't need user policies in case of STS token returned by AssumeRole,
  // hence the check for user type
  if (! s->user->get_id().empty() && s->auth.identity->get_identity_type() != TYPE_ROLE) {
    try {
      ret = s->user->read_attrs(dpp, y);
      if (ret == 0) {
	auto user_policies = get_iam_user_policy_from_attr(s->cct,
							   s->user->get_attrs(),
							   s->user->get_tenant());
          s->iam_user_policies.insert(s->iam_user_policies.end(),
                                      std::make_move_iterator(user_policies.begin()),
                                      std::make_move_iterator(user_policies.end()));
      } else {
        if (ret == -ENOENT)
          ret = 0;
        else ret = -EACCES;
      }
    } catch (const std::exception& e) {
      ldpp_dout(dpp, -1) << "Error reading IAM User Policy: " << e.what() << dendl;
      if (!s->system_request) {
        ret = -EACCES;
      }
    }
  }

  try {
    s->iam_policy = get_iam_policy_from_attr(s->cct, s->bucket_attrs, s->bucket_tenant);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "Error reading IAM Policy: " << e.what() << dendl;

    // This really shouldn't happen. We parse the policy when it's given to us,
    // so a parsing failure here means we broke backward compatibility. The only
    // sensible thing to do in this case is to deny access, because the policy
    // may have.
    //
    // However, the only way for an administrator to repair such a bucket is to
    // send a PutBucketPolicy or DeleteBucketPolicy request as an admin/system
    // user. We can allow such requests, because even if the policy denied
    // access, admin/system users override that error from verify_permission().
    if (!s->system_request) {
      ret = -EACCES;
    }
  }

  bool success = driver->get_zone()->get_redirect_endpoint(&s->redirect_zone_endpoint);
  if (success) {
    ldpp_dout(dpp, 20) << "redirect_zone_endpoint=" << s->redirect_zone_endpoint << dendl;
  }

  return ret;
}

/**
 * Get the AccessControlPolicy for a bucket or object off of disk.
 * s: The req_state to draw information from.
 * only_bucket: If true, reads the bucket ACL rather than the object ACL.
 * Returns: 0 on success, -ERR# otherwise.
 */
int rgw_build_object_policies(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
			      req_state *s, bool prefetch_data, optional_yield y)
{
  if (rgw::sal::Object::empty(s->object)) {
    return 0;
  }
  if (!s->bucket_exists) {
    return -ERR_NO_SUCH_BUCKET;
  }

  s->object->set_atomic();
  if (prefetch_data) {
    s->object->set_prefetch_data();
  }

  return read_obj_policy(dpp, driver, s, s->bucket->get_info(), s->bucket_attrs,
                         s->object_acl, nullptr, s->iam_policy, s->bucket.get(),
                         s->object.get(), y);
}

static int rgw_iam_remove_objtags(const DoutPrefixProvider *dpp, req_state* s, rgw::sal::Object* object, bool has_existing_obj_tag, bool has_resource_tag) {
  object->set_atomic();
  int op_ret = object->get_obj_attrs(s->yield, dpp);
  if (op_ret < 0)
    return op_ret;
  rgw::sal::Attrs attrs = object->get_attrs();
  auto tags = attrs.find(RGW_ATTR_TAGS);
  if (tags != attrs.end()) {
    RGWObjTags tagset;
    try {
      auto bliter = tags->second.cbegin();
      tagset.decode(bliter);
    } catch (buffer::error& err) {
      ldpp_dout(s, 0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
      return -EIO;
    }
    for (auto& tag: tagset.get_tags()) {
      if (has_existing_obj_tag) {
        vector<std::unordered_multimap<string, string>::iterator> iters;
        string key = "s3:ExistingObjectTag/" + tag.first;
        auto result = s->env.equal_range(key);
        for (auto& it = result.first; it != result.second; ++it)
        {
          if (tag.second == it->second) {
            iters.emplace_back(it);
          }
        }
        for (auto& it : iters) {
          s->env.erase(it);
        }
      }//end if has_existing_obj_tag
      if (has_resource_tag) {
        vector<std::unordered_multimap<string, string>::iterator> iters;
        string key = "s3:ResourceTag/" + tag.first;
        auto result = s->env.equal_range(key);
        for (auto& it = result.first; it != result.second; ++it)
        {
          if (tag.second == it->second) {
            iters.emplace_back(it);
          }
        }
        for (auto& it : iters) {
          s->env.erase(it);
        }
      }//end if has_resource_tag
    }
  }
  return 0;
}

void rgw_add_to_iam_environment(rgw::IAM::Environment& e, std::string_view key, std::string_view val){
  // This variant just adds non empty key pairs to IAM env., values can be empty
  // in certain cases like tagging
  if (!key.empty())
    e.emplace(key,val);
}

static int rgw_iam_add_tags_from_bl(req_state* s, bufferlist& bl, bool has_existing_obj_tag=false, bool has_resource_tag=false){
  RGWObjTags& tagset = s->tagset;
  try {
    auto bliter = bl.cbegin();
    tagset.decode(bliter);
  } catch (buffer::error& err) {
    ldpp_dout(s, 0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
    return -EIO;
  }

  for (const auto& tag: tagset.get_tags()){
    if (has_existing_obj_tag)
      rgw_add_to_iam_environment(s->env, "s3:ExistingObjectTag/" + tag.first, tag.second);
    if (has_resource_tag)
      rgw_add_to_iam_environment(s->env, "s3:ResourceTag/" + tag.first, tag.second);
  }
  return 0;
}

static int rgw_iam_add_objtags(const DoutPrefixProvider *dpp, req_state* s, rgw::sal::Object* object, bool has_existing_obj_tag, bool has_resource_tag) {
  object->set_atomic();
  int op_ret = object->get_obj_attrs(s->yield, dpp);
  if (op_ret < 0)
    return op_ret;
  rgw::sal::Attrs attrs = object->get_attrs();
  auto tags = attrs.find(RGW_ATTR_TAGS);
  if (tags != attrs.end()){
    return rgw_iam_add_tags_from_bl(s, tags->second, has_existing_obj_tag, has_resource_tag);
  }
  return 0;
}

static int rgw_iam_add_objtags(const DoutPrefixProvider *dpp, req_state* s, bool has_existing_obj_tag, bool has_resource_tag) {
  if (!rgw::sal::Object::empty(s->object.get())) {
    return rgw_iam_add_objtags(dpp, s, s->object.get(), has_existing_obj_tag, has_resource_tag);
  }
  return 0;
}

static int rgw_iam_add_buckettags(const DoutPrefixProvider *dpp, req_state* s, rgw::sal::Bucket* bucket) {
  rgw::sal::Attrs attrs = bucket->get_attrs();
  auto tags = attrs.find(RGW_ATTR_TAGS);
  if (tags != attrs.end()) {
    return rgw_iam_add_tags_from_bl(s, tags->second, false, true);
  }
  return 0;
}

static int rgw_iam_add_buckettags(const DoutPrefixProvider *dpp, req_state* s) {
  return rgw_iam_add_buckettags(dpp, s, s->bucket.get());
}

static void rgw_iam_add_crypt_attrs(rgw::IAM::Environment& e,
                                    const meta_map_t& attrs)
{
  constexpr auto encrypt_attr = "x-amz-server-side-encryption";
  constexpr auto s3_encrypt_attr = "s3:x-amz-server-side-encryption";
  if (auto h = attrs.find(encrypt_attr); h != attrs.end()) {
    rgw_add_to_iam_environment(e, s3_encrypt_attr, h->second);
  }

  constexpr auto kms_attr = "x-amz-server-side-encryption-aws-kms-key-id";
  constexpr auto s3_kms_attr = "s3:x-amz-server-side-encryption-aws-kms-key-id";
  if (auto h = attrs.find(kms_attr); h != attrs.end()) {
    rgw_add_to_iam_environment(e, s3_kms_attr, h->second);
  }
}

static std::tuple<bool, bool> rgw_check_policy_condition(const DoutPrefixProvider *dpp,
                                                          boost::optional<rgw::IAM::Policy> iam_policy,
                                                          boost::optional<vector<rgw::IAM::Policy>> identity_policies,
                                                          boost::optional<vector<rgw::IAM::Policy>> session_policies,
                                                          bool check_obj_exist_tag=true) {
  bool has_existing_obj_tag = false, has_resource_tag = false;
  bool iam_policy_s3_exist_tag = false, iam_policy_s3_resource_tag = false;
  if (iam_policy) {
    if (check_obj_exist_tag) {
      iam_policy_s3_exist_tag = iam_policy->has_partial_conditional(S3_EXISTING_OBJTAG);
    }
    iam_policy_s3_resource_tag = iam_policy->has_partial_conditional(S3_RESOURCE_TAG) || iam_policy->has_partial_conditional_value(S3_RUNTIME_RESOURCE_VAL);
  }

  bool identity_policy_s3_exist_tag = false, identity_policy_s3_resource_tag = false;
  if (identity_policies) {
    for (auto& identity_policy : identity_policies.get()) {
      if (check_obj_exist_tag) {
        if (identity_policy.has_partial_conditional(S3_EXISTING_OBJTAG))
          identity_policy_s3_exist_tag = true;
      }
      if (identity_policy.has_partial_conditional(S3_RESOURCE_TAG) || identity_policy.has_partial_conditional_value(S3_RUNTIME_RESOURCE_VAL))
        identity_policy_s3_resource_tag = true;
      if (identity_policy_s3_exist_tag && identity_policy_s3_resource_tag) // check all policies till both are set to true
        break;
    }
  }

  bool session_policy_s3_exist_tag = false, session_policy_s3_resource_flag = false;
  if (session_policies) {
    for (auto& session_policy : session_policies.get()) {
      if (check_obj_exist_tag) {
        if (session_policy.has_partial_conditional(S3_EXISTING_OBJTAG))
          session_policy_s3_exist_tag = true;
      }
      if (session_policy.has_partial_conditional(S3_RESOURCE_TAG) || session_policy.has_partial_conditional_value(S3_RUNTIME_RESOURCE_VAL))
        session_policy_s3_resource_flag = true;
      if (session_policy_s3_exist_tag && session_policy_s3_resource_flag)
        break;
    }
  }

  has_existing_obj_tag = iam_policy_s3_exist_tag || identity_policy_s3_exist_tag || session_policy_s3_exist_tag;
  has_resource_tag = iam_policy_s3_resource_tag || identity_policy_s3_resource_tag || session_policy_s3_resource_flag;
  return make_tuple(has_existing_obj_tag, has_resource_tag);
}

static std::tuple<bool, bool> rgw_check_policy_condition(const DoutPrefixProvider *dpp, req_state* s, bool check_obj_exist_tag=true) {
  return rgw_check_policy_condition(dpp, s->iam_policy, s->iam_user_policies, s->session_policies, check_obj_exist_tag);
}

static void rgw_add_grant_to_iam_environment(rgw::IAM::Environment& e, req_state *s){

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
        e.emplace(c.second, hdr);
      }
    }
  }
}

void rgw_build_iam_environment(rgw::sal::Driver* driver,
	                              req_state* s)
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
    s->env.emplace("aws:username", s->user->get_id().id);
  }

  if (s->auth.identity) {
    s->env.emplace("rgw:subuser", s->auth.identity->get_subuser().c_str());
  }

  i = m.find("HTTP_X_AMZ_SECURITY_TOKEN");
  if (i != m.end()) {
    s->env.emplace("sts:authentication", "true");
  } else {
    s->env.emplace("sts:authentication", "false");
  }
}

/*
 * GET on CloudTiered objects is processed only when sent from the sync client.
 * In all other cases, fail with `ERR_INVALID_OBJECT_STATE`.
 */
int handle_cloudtier_obj(rgw::sal::Attrs& attrs, bool sync_cloudtiered) {
  int op_ret = 0;
  auto attr_iter = attrs.find(RGW_ATTR_MANIFEST);
  if (attr_iter != attrs.end()) {
    RGWObjManifest m;
    try {
      decode(m, attr_iter->second);
      if (m.get_tier_type() == "cloud-s3") {
        if (!sync_cloudtiered) {
          /* XXX: Instead send presigned redirect or read-through */
          op_ret = -ERR_INVALID_OBJECT_STATE;
        } else { // fetch object for sync and set cloud_tier attrs
          bufferlist t, t_tier;
          RGWObjTier tier_config;
          m.get_tier_config(&tier_config);

          t.append("cloud-s3");
          attrs[RGW_ATTR_CLOUD_TIER_TYPE] = t;
          encode(tier_config, t_tier);
          attrs[RGW_ATTR_CLOUD_TIER_CONFIG] = t_tier;
        }
      }
    } catch (const buffer::end_of_buffer&) {
      // ignore empty manifest; it's not cloud-tiered
    } catch (const std::exception& e) {
    }
  }

  return op_ret;
}

void rgw_bucket_object_pre_exec(req_state *s)
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
int retry_raced_bucket_write(const DoutPrefixProvider *dpp, rgw::sal::Bucket* b, const F& f, optional_yield y) {
  auto r = f();
  for (auto i = 0u; i < 15u && r == -ECANCELED; ++i) {
    r = b->try_refresh_info(dpp, nullptr, y);
    if (r >= 0) {
      r = f();
    }
  }
  return r;
}
}


int RGWGetObj::verify_permission(optional_yield y)
{
  s->object->set_atomic();

  if (prefetch_data()) {
    s->object->set_prefetch_data();
  }

  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
    if (has_s3_existing_tag || has_s3_resource_tag)
      rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  if (get_torrent) {
    if (s->object->get_instance().empty()) {
      action = rgw::IAM::s3GetObjectTorrent;
    } else {
      action = rgw::IAM::s3GetObjectVersionTorrent;
    }
  } else {
    if (s->object->get_instance().empty()) {
      action = rgw::IAM::s3GetObject;
    } else {
      action = rgw::IAM::s3GetObjectVersion;
    }
  }

  if (!verify_object_permission(this, s, action)) {
    return -EACCES;
  }

  if (s->bucket->get_info().obj_lock_enabled()) {
    get_retention = verify_object_permission(this, s, rgw::IAM::s3GetObjectRetention);
    get_legal_hold = verify_object_permission(this, s, rgw::IAM::s3GetObjectLegalHold);
  }

  return 0;
}

RGWOp::~RGWOp(){};

int RGWOp::verify_op_mask()
{
  uint32_t required_mask = op_mask();

  ldpp_dout(this, 20) << "required_mask= " << required_mask
      << " user.op_mask=" << s->user->get_info().op_mask << dendl;

  if ((s->user->get_info().op_mask & required_mask) != required_mask) {
    return -EPERM;
  }

  if (!s->system_request && (required_mask & RGW_OP_TYPE_MODIFY) && !driver->get_zone()->is_writeable()) {
    ldpp_dout(this, 5) << "NOTICE: modify request to a read-only zone by a "
        "non-system user, permission denied"  << dendl;
    return -EPERM;
  }

  return 0;
}

int RGWGetObjTags::verify_permission(optional_yield y)
{
  auto iam_action = s->object->get_instance().empty()?
    rgw::IAM::s3GetObjectTagging:
    rgw::IAM::s3GetObjectVersionTagging;

  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
  if (has_s3_existing_tag || has_s3_resource_tag)
    rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);
  if (!verify_object_permission(this, s,iam_action))
    return -EACCES;

  return 0;
}

void RGWGetObjTags::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetObjTags::execute(optional_yield y)
{
  rgw::sal::Attrs attrs;

  s->object->set_atomic();

  op_ret = s->object->get_obj_attrs(y, this);

  if (op_ret == 0){
    attrs = s->object->get_attrs();
    auto tags = attrs.find(RGW_ATTR_TAGS);
    if(tags != attrs.end()){
      has_tags = true;
      tags_bl.append(tags->second);
    }
  }
  send_response_data(tags_bl);
}

int RGWPutObjTags::verify_permission(optional_yield y)
{
  auto iam_action = s->object->get_instance().empty() ?
    rgw::IAM::s3PutObjectTagging:
    rgw::IAM::s3PutObjectVersionTagging;

  //Using buckets tags for authorization makes more sense.
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, true);
  if (has_s3_existing_tag)
    rgw_iam_add_objtags(this, s, true, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);
  if (!verify_object_permission(this, s,iam_action))
    return -EACCES;
  return 0;
}

void RGWPutObjTags::execute(optional_yield y)
{
  op_ret = get_params(y);
  if (op_ret < 0)
    return;

  if (rgw::sal::Object::empty(s->object.get())){
    op_ret= -EINVAL; // we only support tagging on existing objects
    return;
  }

  s->object->set_atomic();
  op_ret = s->object->modify_obj_attrs(RGW_ATTR_TAGS, tags_bl, y, this);
  if (op_ret == -ECANCELED){
    op_ret = -ERR_TAG_CONFLICT;
  }
}

void RGWDeleteObjTags::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}


int RGWDeleteObjTags::verify_permission(optional_yield y)
{
  if (!rgw::sal::Object::empty(s->object.get())) {
    auto iam_action = s->object->get_instance().empty() ?
      rgw::IAM::s3DeleteObjectTagging:
      rgw::IAM::s3DeleteObjectVersionTagging;

  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
  if (has_s3_existing_tag || has_s3_resource_tag)
    rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);
  if (!verify_object_permission(this, s, iam_action))
    return -EACCES;
  }
  return 0;
}

void RGWDeleteObjTags::execute(optional_yield y)
{
  if (rgw::sal::Object::empty(s->object.get()))
    return;

  op_ret = s->object->delete_obj_attrs(this, RGW_ATTR_TAGS, y);
}

int RGWGetBucketTags::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3GetBucketTagging)) {
    return -EACCES;
  }

  return 0;
}

void RGWGetBucketTags::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketTags::execute(optional_yield y)
{
  auto iter = s->bucket_attrs.find(RGW_ATTR_TAGS);
  if (iter != s->bucket_attrs.end()) {
    has_tags = true;
    tags_bl.append(iter->second);
  } else {
    op_ret = -ERR_NO_SUCH_TAG_SET;
  }
  send_response_data(tags_bl);
}

int RGWPutBucketTags::verify_permission(optional_yield y) {
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketTagging);
}

void RGWPutBucketTags::execute(optional_yield y)
{

  op_ret = get_params(this, y);
  if (op_ret < 0) 
    return;

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &in_data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
    rgw::sal::Attrs attrs = s->bucket->get_attrs();
    attrs[RGW_ATTR_TAGS] = tags_bl;
    return s->bucket->merge_and_store_attrs(this, attrs, y);
  }, y);

}

void RGWDeleteBucketTags::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

int RGWDeleteBucketTags::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketTagging);
}

void RGWDeleteBucketTags::execute(optional_yield y)
{
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
    rgw::sal::Attrs attrs = s->bucket->get_attrs();
    attrs.erase(RGW_ATTR_TAGS);
    op_ret = s->bucket->merge_and_store_attrs(this, attrs, y);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "RGWDeleteBucketTags() failed to remove RGW_ATTR_TAGS on bucket="
			 << s->bucket->get_name()
			 << " returned err= " << op_ret << dendl;
    }
    return op_ret;
  }, y);
}

int RGWGetBucketReplication::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3GetReplicationConfiguration)) {
    return -EACCES;
  }

  return 0;
}

void RGWGetBucketReplication::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketReplication::execute(optional_yield y)
{
  send_response_data();
}

int RGWPutBucketReplication::verify_permission(optional_yield y) {
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutReplicationConfiguration);
}

void RGWPutBucketReplication::execute(optional_yield y) {

  op_ret = get_params(y);
  if (op_ret < 0) 
    return;

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &in_data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
    auto sync_policy = (s->bucket->get_info().sync_policy ? *s->bucket->get_info().sync_policy : rgw_sync_policy_info());

    for (auto& group : sync_policy_groups) {
      sync_policy.groups[group.id] = group;
    }

    s->bucket->get_info().set_sync_policy(std::move(sync_policy));

    int ret = s->bucket->put_info(this, false, real_time(), y);
    if (ret < 0) {
      ldpp_dout(this, 0) << "ERROR: put_bucket_instance_info (bucket=" << s->bucket << ") returned ret=" << ret << dendl;
      return ret;
    }

    return 0;
  }, y);
}

void RGWDeleteBucketReplication::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

int RGWDeleteBucketReplication::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3DeleteReplicationConfiguration);
}

void RGWDeleteBucketReplication::execute(optional_yield y)
{
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
    if (!s->bucket->get_info().sync_policy) {
      return 0;
    }

    rgw_sync_policy_info sync_policy = *s->bucket->get_info().sync_policy;

    update_sync_policy(&sync_policy);

    s->bucket->get_info().set_sync_policy(std::move(sync_policy));

    int ret = s->bucket->put_info(this, false, real_time(), y);
    if (ret < 0) {
      ldpp_dout(this, 0) << "ERROR: put_bucket_instance_info (bucket=" << s->bucket << ") returned ret=" << ret << dendl;
      return ret;
    }

    return 0;
  }, y);
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
  if (!(s->user->get_info().op_mask & RGW_OP_TYPE_MODIFY)) {
    return 0;
  }

  /* Need a bucket to get quota */
  if (rgw::sal::Bucket::empty(s->bucket.get())) {
    return 0;
  }

  std::unique_ptr<rgw::sal::User> owner_user =
			driver->get_user(s->bucket->get_info().owner);
  rgw::sal::User* user;

  if (s->user->get_id() == s->bucket_owner.id) {
    user = s->user.get();
  } else {
    int r = owner_user->load_user(this, s->yield);
    if (r < 0)
      return r;
    user = owner_user.get();
    
  }

  driver->get_quota(quota);

  if (s->bucket->get_info().quota.enabled) {
    quota.bucket_quota = s->bucket->get_info().quota;
  } else if (user->get_info().quota.bucket_quota.enabled) {
    quota.bucket_quota = user->get_info().quota.bucket_quota;
  }

  if (user->get_info().quota.user_quota.enabled) {
    quota.user_quota = user->get_info().quota.user_quota;
  }

  return 0;
}

static bool validate_cors_rule_method(const DoutPrefixProvider *dpp, RGWCORSRule *rule, const char *req_meth) {
  if (!req_meth) {
    ldpp_dout(dpp, 5) << "req_meth is null" << dendl;
    return false;
  }

  uint8_t flags = get_cors_method_flags(req_meth);

  if (rule->get_allowed_methods() & flags) {
    ldpp_dout(dpp, 10) << "Method " << req_meth << " is supported" << dendl;
  } else {
    ldpp_dout(dpp, 5) << "Method " << req_meth << " is not supported" << dendl;
    return false;
  }

  return true;
}

static bool validate_cors_rule_header(const DoutPrefixProvider *dpp, RGWCORSRule *rule, const char *req_hdrs) {
  if (req_hdrs) {
    vector<string> hdrs;
    get_str_vec(req_hdrs, hdrs);
    for (const auto& hdr : hdrs) {
      if (!rule->is_header_allowed(hdr.c_str(), hdr.length())) {
        ldpp_dout(dpp, 5) << "Header " << hdr << " is not registered in this rule" << dendl;
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
    ldpp_dout(this, 0) << "ERROR: could not decode CORS, caught buffer::error" << dendl;
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
static void get_cors_response_headers(const DoutPrefixProvider *dpp, RGWCORSRule *rule, const char *req_hdrs, string& hdrs, string& exp_hdrs, unsigned *max_age) {
  if (req_hdrs) {
    list<string> hl;
    get_str_list(req_hdrs, hl);
    for(list<string>::iterator it = hl.begin(); it != hl.end(); ++it) {
      if (!rule->is_header_allowed((*it).c_str(), (*it).length())) {
        ldpp_dout(dpp, 5) << "Header " << (*it) << " is not registered in this rule" << dendl;
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
  int temp_op_ret = read_bucket_cors();
  if (temp_op_ret < 0) {
    op_ret = temp_op_ret;
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
    if (!validate_cors_rule_method(this, rule, req_meth)) {
     return false;
    }
  }

  /* CORS 6.2.4. */
  const char *req_hdrs = s->info.env->get("HTTP_ACCESS_CONTROL_REQUEST_HEADERS");

  /* CORS 6.2.6. */
  get_cors_response_headers(this, rule, req_hdrs, headers, exp_headers, max_age);

  return true;
}

int rgw_policy_from_attrset(const DoutPrefixProvider *dpp, CephContext *cct, map<string, bufferlist>& attrset, RGWAccessControlPolicy *policy)
{
  map<string, bufferlist>::iterator aiter = attrset.find(RGW_ATTR_ACL);
  if (aiter == attrset.end())
    return -EIO;

  bufferlist& bl = aiter->second;
  auto iter = bl.cbegin();
  try {
    policy->decode(iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    ldpp_dout(dpp, 15) << __func__ << " Read AccessControlPolicy";
    rgw::s3::write_policy_xml(*policy, *_dout);
    *_dout << dendl;
  }
  return 0;
}

int RGWGetObj::read_user_manifest_part(rgw::sal::Bucket* bucket,
                                       const rgw_bucket_dir_entry& ent,
                                       const RGWAccessControlPolicy& bucket_acl,
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

  std::unique_ptr<rgw::sal::Object> part = bucket->get_object(ent.key);

  RGWAccessControlPolicy obj_policy;

  ldpp_dout(this, 20) << "reading obj=" << part << " ofs=" << cur_ofs
      << " end=" << cur_end << dendl;

  part->set_atomic();
  part->set_prefetch_data();

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op = part->get_read_op();

  if (!swift_slo) {
    /* SLO etag is optional */
    read_op->params.if_match = ent.meta.etag.c_str();
  }

  op_ret = read_op->prepare(s->yield, this);
  if (op_ret < 0)
    return op_ret;
  op_ret = part->range_to_ofs(ent.meta.accounted_size, cur_ofs, cur_end);
  if (op_ret < 0)
    return op_ret;
  bool need_decompress;
  op_ret = rgw_compression_info_from_attrset(part->get_attrs(), need_decompress, cs_info);
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
    if (part->get_obj_size() != ent.meta.size) {
      // hmm.. something wrong, object not as expected, abort!
      ldpp_dout(this, 0) << "ERROR: expected obj_size=" << part->get_obj_size()
          << ", actual read size=" << ent.meta.size << dendl;
      return -EIO;
	  }
  }

  op_ret = rgw_policy_from_attrset(s, s->cct, part->get_attrs(), &obj_policy);
  if (op_ret < 0)
    return op_ret;

  /* We can use global user_acl because LOs cannot have segments
   * stored inside different accounts. */
  if (s->system_request) {
    ldpp_dout(this, 2) << "overriding permissions due to system operation" << dendl;
  } else if (s->auth.identity->is_admin_of(s->user->get_id())) {
    ldpp_dout(this, 2) << "overriding permissions due to admin operation" << dendl;
  } else if (!verify_object_permission(this, s, part->get_obj(), s->user_acl,
				       bucket_acl, obj_policy, bucket_policy,
				       s->iam_user_policies, s->session_policies, action)) {
    return -EPERM;
  }
  if (ent.meta.size == 0) {
    return 0;
  }

  auto counters = rgw::op_counters::get(s);
  rgw::op_counters::inc(counters, l_rgw_op_get_obj_b, cur_end - cur_ofs);
  filter->fixup_range(cur_ofs, cur_end);
  op_ret = read_op->iterate(this, cur_ofs, cur_end, filter, s->yield);
  if (op_ret >= 0)
	  op_ret = filter->flush();
  return op_ret;
}

static int iterate_user_manifest_parts(const DoutPrefixProvider *dpp, 
                                       CephContext * const cct,
                                       rgw::sal::Driver* const driver,
                                       const off_t ofs,
                                       const off_t end,
                                       rgw::sal::Bucket* bucket,
                                       const string& obj_prefix,
                                       const RGWAccessControlPolicy& bucket_acl,
                                       const boost::optional<Policy>& bucket_policy,
                                       uint64_t * const ptotal_len,
                                       uint64_t * const pobj_size,
                                       string * const pobj_sum,
                                       int (*cb)(rgw::sal::Bucket* bucket,
                                                 const rgw_bucket_dir_entry& ent,
                                                 const RGWAccessControlPolicy& bucket_acl,
                                                 const boost::optional<Policy>& bucket_policy,
                                                 off_t start_ofs,
                                                 off_t end_ofs,
                                                 void *param,
                                                 bool swift_slo),
                                       void * const cb_param,
				       optional_yield y)
{
  uint64_t obj_ofs = 0, len_count = 0;
  bool found_start = false, found_end = false, handled_end = false;
  string delim;

  utime_t start_time = ceph_clock_now();

  rgw::sal::Bucket::ListParams params;
  params.prefix = obj_prefix;
  params.delim = delim;

  rgw::sal::Bucket::ListResults results;
  MD5 etag_sum;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  etag_sum.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  do {
    static constexpr auto MAX_LIST_OBJS = 100u;
    int r = bucket->list(dpp, params, MAX_LIST_OBJS, results, y);
    if (r < 0) {
      return r;
    }

    for (rgw_bucket_dir_entry& ent : results.objs) {
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

      rgw::op_counters::CountersContainer counters;
      rgw::op_counters::tinc(counters, l_rgw_op_get_obj_lat,
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
  } while (results.is_truncated);

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
  rgw::sal::Bucket* bucket;
  string obj_name;
  uint64_t size = 0;
  string etag;
};

static int iterate_slo_parts(const DoutPrefixProvider *dpp,
                             CephContext *cct,
                             rgw::sal::Driver* driver,
                             off_t ofs,
                             off_t end,
                             map<uint64_t, rgw_slo_part>& slo_parts,
                             int (*cb)(rgw::sal::Bucket* bucket,
                                       const rgw_bucket_dir_entry& ent,
                                       const RGWAccessControlPolicy& bucket_acl,
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
    uint64_t start_ofs = 0, end_ofs = ent.meta.size - 1;

    if (!found_start && cur_total_len + ent.meta.size > (uint64_t)ofs) {
      start_ofs = ofs - obj_ofs;
      found_start = true;
    }

    obj_ofs += ent.meta.size;

    if (!found_end && obj_ofs > (uint64_t)end) {
      end_ofs = end - cur_total_len;
      found_end = true;
    }

    rgw::op_counters::CountersContainer counters;
    rgw::op_counters::tinc(counters, l_rgw_op_get_obj_lat,
                          (ceph_clock_now() - start_time));

    if (found_start) {
      if (cb) {
        ldpp_dout(dpp, 20) << "iterate_slo_parts()"
                          << " obj=" << part.obj_name
                          << " start_ofs=" << start_ofs
                          << " end_ofs=" << end_ofs
                          << dendl;

	// SLO is a Swift thing, and Swift has no knowledge of S3 Policies.
        int r = cb(part.bucket, ent, *part.bucket_acl,
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

static int get_obj_user_manifest_iterate_cb(rgw::sal::Bucket* bucket,
                                            const rgw_bucket_dir_entry& ent,
                                            const RGWAccessControlPolicy& bucket_acl,
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

int RGWGetObj::handle_user_manifest(const char *prefix, optional_yield y)
{
  const std::string_view prefix_view(prefix);
  ldpp_dout(this, 2) << "RGWGetObj::handle_user_manifest() prefix="
                   << prefix_view << dendl;

  const size_t pos = prefix_view.find('/');
  if (pos == string::npos) {
    return -EINVAL;
  }

  const std::string bucket_name = url_decode(prefix_view.substr(0, pos));
  const std::string obj_prefix = url_decode(prefix_view.substr(pos + 1));

  RGWAccessControlPolicy _bucket_acl;
  RGWAccessControlPolicy *bucket_acl;
  boost::optional<Policy> _bucket_policy;
  boost::optional<Policy>* bucket_policy;
  RGWBucketInfo bucket_info;
  std::unique_ptr<rgw::sal::Bucket> ubucket;
  rgw::sal::Bucket* pbucket = NULL;
  int r = 0;

  if (bucket_name.compare(s->bucket->get_name()) != 0) {
    map<string, bufferlist> bucket_attrs;
    r = driver->load_bucket(this, rgw_bucket(s->user->get_tenant(), bucket_name),
                            &ubucket, y);
    if (r < 0) {
      ldpp_dout(this, 0) << "could not get bucket info for bucket="
		       << bucket_name << dendl;
      return r;
    }
    bucket_acl = &_bucket_acl;
    r = read_bucket_policy(this, driver, s, ubucket->get_info(), bucket_attrs, *bucket_acl, ubucket->get_key(), y);
    if (r < 0) {
      ldpp_dout(this, 0) << "failed to read bucket policy" << dendl;
      return r;
    }
    _bucket_policy = get_iam_policy_from_attr(s->cct, bucket_attrs, s->user->get_tenant());
    bucket_policy = &_bucket_policy;
    pbucket = ubucket.get();
  } else {
    pbucket = s->bucket.get();
    bucket_acl = &s->bucket_acl;
    bucket_policy = &s->iam_policy;
  }

  /* dry run to find out:
   * - total length (of the parts we are going to send to client),
   * - overall DLO's content size,
   * - md5 sum of overall DLO's content (for etag of Swift API). */
  r = iterate_user_manifest_parts(this, s->cct, driver, ofs, end,
        pbucket, obj_prefix, *bucket_acl, *bucket_policy,
        nullptr, &s->obj_size, &lo_etag,
	nullptr /* cb */, nullptr /* cb arg */, y);
  if (r < 0) {
    return r;
  }
  s->object->set_obj_size(s->obj_size);

  r = s->object->range_to_ofs(s->obj_size, ofs, end);
  if (r < 0) {
    return r;
  }

  r = iterate_user_manifest_parts(this, s->cct, driver, ofs, end,
        pbucket, obj_prefix, *bucket_acl, *bucket_policy,
        &total_len, nullptr, nullptr,
	nullptr, nullptr, y);
  if (r < 0) {
    return r;
  }

  if (!get_data) {
    bufferlist bl;
    send_response_data(bl, 0, 0);
    return 0;
  }

  r = iterate_user_manifest_parts(this, s->cct, driver, ofs, end,
        pbucket, obj_prefix, *bucket_acl, *bucket_policy,
        nullptr, nullptr, nullptr,
	get_obj_user_manifest_iterate_cb, (void *)this, y);
  if (r < 0) {
    return r;
  }

  if (!total_len) {
    bufferlist bl;
    send_response_data(bl, 0, 0);
  }

  return r;
}

int RGWGetObj::handle_slo_manifest(bufferlist& bl, optional_yield y)
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
  map<string, std::unique_ptr<rgw::sal::Bucket>> buckets;

  map<uint64_t, rgw_slo_part> slo_parts;

  MD5 etag_sum;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  etag_sum.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
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

    rgw::sal::Bucket* bucket;
    RGWAccessControlPolicy *bucket_acl;
    Policy* bucket_policy;

    if (bucket_name.compare(s->bucket->get_name()) != 0) {
      const auto& piter = policies.find(bucket_name);
      if (piter != policies.end()) {
        bucket_acl = piter->second.first;
        bucket_policy = piter->second.second.get_ptr();
	bucket = buckets[bucket_name].get();
      } else {
	RGWAccessControlPolicy& _bucket_acl = allocated_acls.emplace_back();

	std::unique_ptr<rgw::sal::Bucket> tmp_bucket;
	int r = driver->load_bucket(this, rgw_bucket(s->user->get_tenant(),
                                                     bucket_name),
                                    &tmp_bucket, y);
        if (r < 0) {
          ldpp_dout(this, 0) << "could not get bucket info for bucket="
			   << bucket_name << dendl;
          return r;
        }
        bucket = tmp_bucket.get();
        bucket_acl = &_bucket_acl;
        r = read_bucket_policy(this, driver, s, tmp_bucket->get_info(), tmp_bucket->get_attrs(), *bucket_acl,
                               tmp_bucket->get_key(), y);
        if (r < 0) {
          ldpp_dout(this, 0) << "failed to read bucket ACL for bucket "
                           << bucket << dendl;
          return r;
	}
	auto _bucket_policy = get_iam_policy_from_attr(
	  s->cct, tmp_bucket->get_attrs(), tmp_bucket->get_tenant());
        bucket_policy = _bucket_policy.get_ptr();
	buckets[bucket_name].swap(tmp_bucket);
        policies[bucket_name] = make_pair(bucket_acl, _bucket_policy);
      }
    } else {
      bucket = s->bucket.get();
      bucket_acl = &s->bucket_acl;
      bucket_policy = s->iam_policy.get_ptr();
    }

    rgw_slo_part part;
    part.bucket_acl = bucket_acl;
    part.bucket_policy = bucket_policy;
    part.bucket = bucket;
    part.obj_name = obj_name;
    part.size = entry.size_bytes;
    part.etag = entry.etag;
    ldpp_dout(this, 20) << "slo_part: bucket=" << part.bucket
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
  s->object->set_obj_size(slo_info.total_size);
  ldpp_dout(this, 20) << "s->obj_size=" << s->obj_size << dendl;

  int r = s->object->range_to_ofs(total_len, ofs, end);
  if (r < 0) {
    return r;
  }

  total_len = end - ofs + 1;
  ldpp_dout(this, 20) << "Requested: ofs=" << ofs
                    << " end=" << end
                    << " total=" << total_len
                    << dendl;

  r = iterate_slo_parts(this, s->cct, driver, ofs, end, slo_parts,
        get_obj_user_manifest_iterate_cb, (void *)this);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWGetObj::get_data_cb(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  /* garbage collection related handling:
   * defer_gc disabled for https://tracker.ceph.com/issues/47866 */
  return send_response_data(bl, bl_ofs, bl_len);
}

int RGWGetObj::get_lua_filter(std::unique_ptr<RGWGetObj_Filter>* filter, RGWGetObj_Filter* cb) {
  std::string script;
  const auto rc = rgw::lua::read_script(s, s->penv.lua.manager.get(), s->bucket_tenant, s->yield, rgw::lua::context::getData, script);
  if (rc == -ENOENT) {
    // no script, nothing to do
    return 0;
  } else if (rc < 0) {
    ldpp_dout(this, 5) << "WARNING: failed to read data script. error: " << rc << dendl;
    return rc;
  }
  filter->reset(new rgw::lua::RGWGetObjFilter(s, script, cb));
  return 0;
}

bool RGWGetObj::prefetch_data()
{
  /* HEAD request, stop prefetch*/
  if (!get_data || s->info.env->exists("HTTP_X_RGW_AUTH")) {
    return false;
  }

  range_str = s->info.env->get("HTTP_RANGE");
  // TODO: add range prefetch
  if (range_str) {
    parse_range();
    return false;
  }

  return get_data;
}

void RGWGetObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

static inline void rgw_cond_decode_objtags(
  req_state *s,
  const std::map<std::string, buffer::list> &attrs)
{
  const auto& tags = attrs.find(RGW_ATTR_TAGS);
  if (tags != attrs.end()) {
    try {
      bufferlist::const_iterator iter{&tags->second};
      s->tagset.decode(iter);
    } catch (buffer::error& err) {
      ldpp_dout(s, 0)
	<< "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
    }
  }
}

void RGWGetObj::execute(optional_yield y)
{
  bufferlist bl;
  gc_invalidate_time = ceph_clock_now();
  gc_invalidate_time += (s->cct->_conf->rgw_gc_obj_min_wait / 2);

  bool need_decompress = false;
  int64_t ofs_x = 0, end_x = 0;
  bool encrypted = false;

  RGWGetObj_CB cb(this);
  RGWGetObj_Filter* filter = (RGWGetObj_Filter *)&cb;
  boost::optional<RGWGetObj_Decompress> decompress;
#ifdef WITH_ARROW_FLIGHT
  boost::optional<rgw::flight::FlightGetObj_Filter> flight_filter;
#endif
  std::unique_ptr<RGWGetObj_Filter> decrypt;
  std::unique_ptr<RGWGetObj_Filter> run_lua;
  map<string, bufferlist>::iterator attr_iter;

  auto counters = rgw::op_counters::get(s);
  rgw::op_counters::inc(counters, l_rgw_op_get_obj, 1);

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(s->object->get_read_op());

  op_ret = get_params(y);
  if (op_ret < 0)
    goto done_err;

  op_ret = init_common();
  if (op_ret < 0)
    goto done_err;

  read_op->params.mod_ptr = mod_ptr;
  read_op->params.unmod_ptr = unmod_ptr;
  read_op->params.high_precision_time = s->system_request; /* system request need to use high precision time */
  read_op->params.mod_zone_id = mod_zone_id;
  read_op->params.mod_pg_ver = mod_pg_ver;
  read_op->params.if_match = if_match;
  read_op->params.if_nomatch = if_nomatch;
  read_op->params.lastmod = &lastmod;
  if (multipart_part_num) {
    read_op->params.part_num = &*multipart_part_num;
  }

  op_ret = read_op->prepare(s->yield, this);
  version_id = s->object->get_instance();
  s->obj_size = s->object->get_obj_size();
  attrs = s->object->get_attrs();
  multipart_parts_count = read_op->params.parts_count;
  if (op_ret < 0)
    goto done_err;

  /* STAT ops don't need data, and do no i/o */
  if (get_type() == RGW_OP_STAT_OBJ) {
    return;
  }
  if (s->info.env->exists("HTTP_X_RGW_AUTH")) {
    op_ret = 0;
    goto done_err;
  }
  /* start gettorrent */
  if (get_torrent) {
    attr_iter = attrs.find(RGW_ATTR_CRYPT_MODE);
    if (attr_iter != attrs.end() && attr_iter->second.to_str() == "SSE-C-AES256") {
      ldpp_dout(this, 0) << "ERROR: torrents are not supported for objects "
          "encrypted with SSE-C" << dendl;
      op_ret = -EINVAL;
      goto done_err;
    }
    // read torrent info from attr
    bufferlist torrentbl;
    op_ret = rgw_read_torrent_file(this, s->object.get(), torrentbl, y);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "ERROR: failed to get_torrent_file ret= " << op_ret
                       << dendl;
      goto done_err;
    }
    op_ret = send_response_data(torrentbl, 0, torrentbl.length());
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "ERROR: failed to send_response_data ret= " << op_ret << dendl;
      goto done_err;
    }
    return;
  }
  /* end gettorrent */

  // run lua script on decompressed and decrypted data - first filter runs last
  op_ret = get_lua_filter(&run_lua, filter);
  if (run_lua != nullptr) {
    filter = run_lua.get();
  }
  if (op_ret < 0) {
    goto done_err;
  }

#ifdef WITH_ARROW_FLIGHT
  if (s->penv.flight_store) {
    if (ofs == 0) {
      // insert a GetObj_Filter to monitor and create flight
      flight_filter.emplace(s, filter);
      filter = &*flight_filter;
    }
  } else {
    ldpp_dout(this, 0) << "ERROR: flight_store not created in " << __func__ << dendl;
  }
#endif

  op_ret = rgw_compression_info_from_attrset(attrs, need_decompress, cs_info);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to decode compression info, cannot decompress" << dendl;
    goto done_err;
  }

  // where encryption and compression are combined, compression was applied to
  // the data before encryption. if the system header rgwx-skip-decrypt is
  // present, we have to skip the decompression filter too
  encrypted = attrs.count(RGW_ATTR_CRYPT_MODE);

  if (need_decompress && (!encrypted || !skip_decrypt)) {
    s->obj_size = cs_info.orig_size;
    s->object->set_obj_size(cs_info.orig_size);
    decompress.emplace(s->cct, &cs_info, partial_content, filter);
    filter = &*decompress;
  }

  attr_iter = attrs.find(RGW_ATTR_OBJ_REPLICATION_TRACE);
  if (attr_iter != attrs.end()) {
    try {
      std::vector<rgw_zone_set_entry> zones;
      auto p = attr_iter->second.cbegin();
      decode(zones, p);
      for (const auto& zone: zones) {
        if (zone == dst_zone_trace) {
          op_ret = -ERR_NOT_MODIFIED;
          ldpp_dout(this, 4) << "Object already has been copied to this destination. Returning "
            << op_ret << dendl;
          goto done_err;
        }
      }
    } catch (const buffer::error&) {}
  }

  if (get_type() == RGW_OP_GET_OBJ && get_data) {
    op_ret = handle_cloudtier_obj(attrs, sync_cloudtiered);
    if (op_ret < 0) {
      ldpp_dout(this, 4) << "Cannot get cloud tiered object: " << *s->object
          <<". Failing with " << op_ret << dendl;
      if (op_ret == -ERR_INVALID_OBJECT_STATE) {
        s->err.message = "This object was transitioned to cloud-s3";
      }
      goto done_err;
    }
  }

  attr_iter = attrs.find(RGW_ATTR_USER_MANIFEST);
  if (attr_iter != attrs.end() && !skip_manifest) {
    op_ret = handle_user_manifest(attr_iter->second.c_str(), y);
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
    op_ret = handle_slo_manifest(attr_iter->second, y);
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

  op_ret = s->object->range_to_ofs(s->obj_size, ofs, end);
  if (op_ret < 0)
    goto done_err;
  total_len = (ofs <= end ? end + 1 - ofs : 0);

  ofs_x = ofs;
  end_x = end;
  filter->fixup_range(ofs_x, end_x);

  /* Check whether the object has expired. Swift API documentation
   * stands that we should return 404 Not Found in such case. */
  if (need_object_expiration() && s->object->is_expired()) {
    op_ret = -ENOENT;
    goto done_err;
  }

  /* Decode S3 objtags, if any */
  rgw_cond_decode_objtags(s, attrs);

  start = ofs;

  attr_iter = attrs.find(RGW_ATTR_MANIFEST);
  op_ret = this->get_decrypt_filter(&decrypt, filter,
                                    attr_iter != attrs.end() ? &(attr_iter->second) : nullptr);
  if (decrypt != nullptr) {
    filter = decrypt.get();
    filter->fixup_range(ofs_x, end_x);
  }
  if (op_ret < 0) {
    goto done_err;
  }


  if (!get_data || ofs > end) {
    send_response_data(bl, 0, 0);
    return;
  }

  rgw::op_counters::inc(counters, l_rgw_op_get_obj_b, end-ofs);

  op_ret = read_op->iterate(this, ofs_x, end_x, filter, s->yield);

  if (op_ret >= 0)
    op_ret = filter->flush();

  rgw::op_counters::tinc(counters, l_rgw_op_get_obj_lat, s->time_elapsed());

  if (op_ret < 0) {
    goto done_err;
  }

  op_ret = send_response_data(bl, 0, 0);
  if (op_ret < 0) {
    goto done_err;
  }
  return;

done_err:
  send_response_data_error(y);
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

int RGWListBuckets::verify_permission(optional_yield y)
{
  rgw::Partition partition = rgw::Partition::aws;
  rgw::Service service = rgw::Service::s3;

  string tenant;
  if (s->auth.identity->get_identity_type() == TYPE_ROLE) {
    tenant = s->auth.identity->get_role_tenant();
  } else {
    tenant = s->user->get_tenant();
  }

  if (!verify_user_permission(this, s, ARN(partition, service, "", tenant, "*"), rgw::IAM::s3ListAllMyBuckets, false)) {
    return -EACCES;
  }

  return 0;
}

int RGWGetUsage::verify_permission(optional_yield y)
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  return 0;
}

void RGWListBuckets::execute(optional_yield y)
{
  bool done;
  bool started = false;
  uint64_t total_count = 0;

  const uint64_t max_buckets = s->cct->_conf->rgw_list_buckets_max_chunk;

  auto counters = rgw::op_counters::get(s);
  rgw::op_counters::inc(counters, l_rgw_op_list_buckets, 1);

  auto g = make_scope_guard([this, &started] {
      if (!started) {
        send_response_begin(false);
      }
      send_response_end();
    });

  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  if (supports_account_metadata()) {
    op_ret = s->user->read_attrs(this, s->yield);
    if (op_ret < 0) {
      return;
    }
  }

  /* We need to have stats for all our policies - even if a given policy
   * isn't actually used in a given account. In such situation its usage
   * stats would be simply full of zeros. */
  std::set<std::string> targets;
  driver->get_zone()->get_zonegroup().get_placement_target_names(targets);
  for (const auto& policy : targets) {
    policies_stats[policy] = {};
  }

  rgw::sal::BucketList listing;
  do {
    uint64_t read_count;
    if (limit == 0) {
      break;
    } else if (limit > 0) {
      read_count = min(limit - total_count, max_buckets);
    } else {
      read_count = max_buckets;
    }

    op_ret = s->user->list_buckets(this, marker, end_marker, read_count, should_get_stats(), listing, y);

    if (op_ret < 0) {
      /* hmm.. something wrong here.. the user was authenticated, so it
         should exist */
      ldpp_dout(this, 10) << "WARNING: failed on rgw_get_user_buckets uid="
			<< s->user->get_id() << dendl;
      break;
    }

    marker = listing.next_marker;

    for (const auto& ent : listing.buckets) {
      global_stats.bytes_used += ent.size;
      global_stats.bytes_used_rounded += ent.size_rounded;
      global_stats.objects_count += ent.count;

      /* operator[] still can create a new entry for storage policy seen
       * for first time. */
      auto& policy_stats = policies_stats[ent.placement_rule.to_str()];
      policy_stats.bytes_used += ent.size;
      policy_stats.bytes_used_rounded += ent.size_rounded;
      policy_stats.buckets_count++;
      policy_stats.objects_count += ent.count;
    }
    global_stats.buckets_count += listing.buckets.size();
    total_count += listing.buckets.size();

    done = (limit >= 0 && std::cmp_greater_equal(total_count, limit));

    if (!started) {
      send_response_begin(!listing.buckets.empty());
      started = true;
    }

    handle_listing_chunk(listing.buckets);
  } while (!marker.empty() && !done);
  
  rgw::op_counters::tinc(counters, l_rgw_op_list_buckets_lat, s->time_elapsed());
}

void RGWGetUsage::execute(optional_yield y)
{
  uint64_t start_epoch = 0;
  uint64_t end_epoch = (uint64_t)-1;
  op_ret = get_params(y);
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
  
  while (s->bucket && is_truncated) {
    op_ret = s->bucket->read_usage(this, start_epoch, end_epoch, max_entries, &is_truncated,
				   usage_iter, usage);
    if (op_ret == -ENOENT) {
      op_ret = 0;
      is_truncated = false;
    }

    if (op_ret < 0) {
      return;
    }    
  }

  op_ret = rgw_user_sync_all_stats(this, driver, s->user.get(), y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to sync user stats" << dendl;
    return;
  }

  op_ret = rgw_user_get_all_buckets_stats(this, driver, s->user.get(), buckets_usage, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get user's buckets stats" << dendl;
    return;
  }

  op_ret = s->user->read_stats(this, y, &stats);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: can't read user header"  << dendl;
    return;
  }
  
  return;
}

int RGWStatAccount::verify_permission(optional_yield y)
{
  if (!verify_user_permission_no_policy(this, s, RGW_PERM_READ)) {
    return -EACCES;
  }

  return 0;
}

void RGWStatAccount::execute(optional_yield y)
{
  uint64_t max_buckets = s->cct->_conf->rgw_list_buckets_max_chunk;

  /* We need to have stats for all our policies - even if a given policy
   * isn't actually used in a given account. In such situation its usage
   * stats would be simply full of zeros. */
  std::set<std::string> names;
  driver->get_zone()->get_zonegroup().get_placement_target_names(names);
  for (const auto& policy : names) {
    policies_stats.emplace(policy, decltype(policies_stats)::mapped_type());
  }

  rgw::sal::BucketList listing;
  do {
    op_ret = s->user->list_buckets(this, listing.next_marker, string(),
                                   max_buckets, true, listing, y);
    if (op_ret < 0) {
      /* hmm.. something wrong here.. the user was authenticated, so it
         should exist */
      ldpp_dout(this, 10) << "WARNING: failed on list_buckets uid="
			<< s->user->get_id() << " ret=" << op_ret << dendl;
      return;
    }

    for (const auto& ent : listing.buckets) {
      global_stats.bytes_used += ent.size;
      global_stats.bytes_used_rounded += ent.size_rounded;
      global_stats.objects_count += ent.count;

      /* operator[] still can create a new entry for storage policy seen
       * for first time. */
      auto& policy_stats = policies_stats[ent.placement_rule.to_str()];
      policy_stats.bytes_used += ent.size;
      policy_stats.bytes_used_rounded += ent.size_rounded;
      policy_stats.buckets_count++;
      policy_stats.objects_count += ent.count;
    }
    global_stats.buckets_count += listing.buckets.size();
  } while (!listing.next_marker.empty());
}

int RGWGetBucketVersioning::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketVersioning);
}

void RGWGetBucketVersioning::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketVersioning::execute(optional_yield y)
{
  if (! s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  versioned = s->bucket->versioned();
  versioning_enabled = s->bucket->versioning_enabled();
  mfa_enabled = s->bucket->get_info().mfa_enabled();
}

int RGWSetBucketVersioning::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketVersioning);
}

void RGWSetBucketVersioning::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWSetBucketVersioning::execute(optional_yield y)
{
  op_ret = get_params(y);
  if (op_ret < 0)
    return;

  if (! s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  if (s->bucket->get_info().obj_lock_enabled() && versioning_status != VersioningEnabled) {
    s->err.message = "bucket versioning cannot be disabled on buckets with object lock enabled";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_BUCKET_STATE;
    return;
  }

  bool cur_mfa_status = s->bucket->get_info().mfa_enabled();

  mfa_set_status &= (mfa_status != cur_mfa_status);

  if (mfa_set_status &&
      !s->mfa_verified) {
    op_ret = -ERR_MFA_REQUIRED;
    return;
  }
  //if mfa is enabled for bucket, make sure mfa code is validated in case versioned status gets changed
  if (cur_mfa_status) {
    bool req_versioning_status = false;
    //if requested versioning status is not the same as the one set for the bucket, return error
    if (versioning_status == VersioningEnabled) {
      req_versioning_status = (s->bucket->get_info().flags & BUCKET_VERSIONS_SUSPENDED) != 0;
    } else if (versioning_status == VersioningSuspended) {
      req_versioning_status = (s->bucket->get_info().flags & BUCKET_VERSIONS_SUSPENDED) == 0;
    }
    if (req_versioning_status && !s->mfa_verified) {
      op_ret = -ERR_MFA_REQUIRED;
      return;
    }
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &in_data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  bool modified = mfa_set_status;

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [&] {
      if (mfa_set_status) {
        if (mfa_status) {
          s->bucket->get_info().flags |= BUCKET_MFA_ENABLED;
        } else {
          s->bucket->get_info().flags &= ~BUCKET_MFA_ENABLED;
        }
      }

      if (versioning_status == VersioningEnabled) {
	s->bucket->get_info().flags |= BUCKET_VERSIONED;
	s->bucket->get_info().flags &= ~BUCKET_VERSIONS_SUSPENDED;
        modified = true;
      } else if (versioning_status == VersioningSuspended) {
	s->bucket->get_info().flags |= (BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED);
        modified = true;
      } else {
	return op_ret;
      }
      s->bucket->set_attrs(rgw::sal::Attrs(s->bucket_attrs));
      return s->bucket->put_info(this, false, real_time(), y);
    }, y);

  if (!modified) {
    return;
  }

  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket->get_name()
		     << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWGetBucketWebsite::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketWebsite);
}

void RGWGetBucketWebsite::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketWebsite::execute(optional_yield y)
{
  if (!s->bucket->get_info().has_website) {
    op_ret = -ERR_NO_SUCH_WEBSITE_CONFIGURATION;
  }
}

int RGWSetBucketWebsite::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketWebsite);
}

void RGWSetBucketWebsite::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWSetBucketWebsite::execute(optional_yield y)
{
  op_ret = get_params(y);

  if (op_ret < 0)
    return;

  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &in_data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << " forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
      s->bucket->get_info().has_website = true;
      s->bucket->get_info().website_conf = website_conf;
      op_ret = s->bucket->put_info(this, false, real_time(), y);
      return op_ret;
    }, y);

  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket->get_name()
        << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWDeleteBucketWebsite::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3DeleteBucketWebsite);
}

void RGWDeleteBucketWebsite::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteBucketWebsite::execute(optional_yield y)
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: forward_to_master failed on bucket=" << s->bucket->get_name()
      << "returned err=" << op_ret << dendl;
    return;
  }
  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
      s->bucket->get_info().has_website = false;
      s->bucket->get_info().website_conf = RGWBucketWebsiteConf();
      op_ret = s->bucket->put_info(this, false, real_time(), y);
      return op_ret;
    }, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket
        << " returned err=" << op_ret << dendl;
    return;
  }
}

int RGWStatBucket::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

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

// read the bucket's stats for RGWObjCategory::Main
static int load_bucket_stats(const DoutPrefixProvider* dpp, optional_yield y,
                             rgw::sal::Bucket& bucket, RGWStorageStats& stats)
{
  const auto& index = bucket.get_info().layout.current_index;
  std::string bver, mver; // ignored
  std::map<RGWObjCategory, RGWStorageStats> categories;

  int r = bucket.read_stats(dpp, index, -1, &bver, &mver, categories);
  if (r < 0) {
    return r;
  }
  stats = categories[RGWObjCategory::Main];
  return 0;
}

void RGWStatBucket::execute(optional_yield y)
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  op_ret = driver->load_bucket(this, s->bucket->get_key(), &bucket, y);
  if (op_ret) {
    return;
  }

  op_ret = load_bucket_stats(this, y, *s->bucket, stats);
}

int RGWListBucket::verify_permission(optional_yield y)
{
  op_ret = get_params(y);
  if (op_ret < 0) {
    return op_ret;
  }
  if (!prefix.empty())
    s->env.emplace("s3:prefix", prefix);

  if (!delimiter.empty())
    s->env.emplace("s3:delimiter", delimiter);

  s->env.emplace("s3:max-keys", std::to_string(max));

  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

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

void RGWListBucket::execute(optional_yield y)
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
    stats.emplace();
    if (int ret = load_bucket_stats(this, y, *s->bucket, *stats); ret < 0) {
      stats = std::nullopt; // just don't return stats
    }
  }

  rgw::sal::Bucket::ListParams params;
  params.prefix = prefix;
  params.delim = delimiter;
  params.marker = marker;
  params.end_marker = end_marker;
  params.list_versions = list_versions;
  params.allow_unordered = allow_unordered;
  params.shard_id = shard_id;

  rgw::sal::Bucket::ListResults results;

  op_ret = s->bucket->list(this, params, max, results, y);
  if (op_ret >= 0) {
    next_marker = results.next_marker;
    is_truncated = results.is_truncated;
    objs = std::move(results.objs);
    common_prefixes = std::move(results.common_prefixes);
  }

  auto counters = rgw::op_counters::get(s);
  rgw::op_counters::inc(counters, l_rgw_op_list_obj, 1);
  rgw::op_counters::tinc(counters, l_rgw_op_list_obj_lat, s->time_elapsed());
}

int RGWGetBucketLogging::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketLogging);
}

int RGWGetBucketLocation::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketLocation);
}

// list the user's buckets to check whether they're at their maximum
static int check_user_max_buckets(const DoutPrefixProvider* dpp,
                                  rgw::sal::User& user, optional_yield y)
{
  int32_t remaining = user.get_max_buckets();
  if (!remaining) { // unlimited
    return 0;
  }

  uint64_t max_buckets = dpp->get_cct()->_conf->rgw_list_buckets_max_chunk;

  rgw::sal::BucketList listing;
  do {
    size_t to_read = std::max<size_t>(max_buckets, remaining);

    int ret = user.list_buckets(dpp, listing.next_marker, string(),
                                to_read, false, listing, y);
    if (ret < 0) {
      return ret;
    }

    remaining -= listing.buckets.size();
    if (remaining <= 0) {
      return -ERR_TOO_MANY_BUCKETS;
    }
  } while (!listing.next_marker.empty());

  return 0;
}

int RGWCreateBucket::verify_permission(optional_yield y)
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
  if (!verify_user_permission(this, s, arn, rgw::IAM::s3CreateBucket, false)) {
    return -EACCES;
  }

  if (s->user->get_tenant() != s->bucket_tenant) {
    //AssumeRole is meant for cross account access
    if (s->auth.identity->get_identity_type() != TYPE_ROLE) {
      ldpp_dout(this, 10) << "user cannot create a bucket in a different tenant"
                        << " (user_id.tenant=" << s->user->get_tenant()
                        << " requested=" << s->bucket_tenant << ")"
                        << dendl;
      return -EACCES;
    }
  }

  if (s->user->get_max_buckets() < 0) {
    return -EPERM;
  }

  return check_user_max_buckets(this, *s->user, y);
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

  /* Swift requires checking on raw usage instead of the 4 KiB rounded one. */
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

static int select_bucket_placement(const DoutPrefixProvider* dpp,
                                   const RGWZoneGroup& zonegroup,
                                   const RGWUserInfo& user,
                                   rgw_placement_rule& rule)
{
  std::string_view selected = "requested";

  // select placement: requested rule > user default > zonegroup default
  if (rule.name.empty()) {
    selected = "user-default";
    rule.inherit_from(user.default_placement);
    if (rule.name.empty()) {
      selected = "zonegroup-default";
      rule.inherit_from(zonegroup.default_placement);
      if (rule.name.empty()) {
        ldpp_dout(dpp, 0) << "ERROR: misconfigured zonegroup " << zonegroup.id
            << ", default placement should not be empty" << dendl;
        return -ERR_ZONEGROUP_DEFAULT_PLACEMENT_MISCONFIGURATION;
      }
    }
  }

  // look up the zonegroup placement target
  auto target = zonegroup.placement_targets.find(rule.name);
  if (target == zonegroup.placement_targets.end()) {
    ldpp_dout(dpp, 0) << "could not find " << selected << " placement target "
        << rule.name << " within zonegroup" << dendl;
    return -ERR_INVALID_LOCATION_CONSTRAINT;
  }

  // check the user's permission tags
  if (!target->second.user_permitted(user.placement_tags)) {
    ldpp_dout(dpp, 0) << "user not permitted to use placement rule "
        << target->first << dendl;
    return -EPERM;
  }

  ldpp_dout(dpp, 20) << "using " << selected << " placement target "
      << rule.name << dendl;
  return 0;
}

void RGWCreateBucket::execute(optional_yield y)
{
  op_ret = get_params(y);
  if (op_ret < 0)
    return;

  const rgw::SiteConfig& site = *s->penv.site;
  const std::optional<RGWPeriod>& period = site.get_period();
  const RGWZoneGroup& my_zonegroup = site.get_zonegroup();

  if (s->system_request) {
    // allow system requests to override the target zonegroup. for forwarded
    // requests, we'll create the bucket for the originating zonegroup
    createparams.zonegroup_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "zonegroup");
  }

  const RGWZoneGroup* bucket_zonegroup = &my_zonegroup;
  if (createparams.zonegroup_id.empty()) {
    // default to the local zonegroup
    createparams.zonegroup_id = my_zonegroup.id;
  } else if (period) {
    auto z = period->period_map.zonegroups.find(createparams.zonegroup_id);
    if (z == period->period_map.zonegroups.end()) {
      ldpp_dout(this, 0) << "could not find zonegroup "
          << createparams.zonegroup_id << " in current period" << dendl;
      op_ret = -ENOENT;
      return;
    }
    bucket_zonegroup = &z->second;
  } else if (createparams.zonegroup_id != my_zonegroup.id) {
    ldpp_dout(this, 0) << "zonegroup does not match current zonegroup "
        << createparams.zonegroup_id << dendl;
    op_ret = -ENOENT;
    return;
  }

  // validate the LocationConstraint
  if (!location_constraint.empty() && !relaxed_region_enforcement) {
    // on the master zonegroup, allow any valid api_name. otherwise it has to
    // match the bucket's zonegroup
    if (period && my_zonegroup.is_master) {
      if (!period->period_map.zonegroups_by_api.count(location_constraint)) {
        ldpp_dout(this, 0) << "location constraint (" << location_constraint
            << ") can't be found." << dendl;
        op_ret = -ERR_INVALID_LOCATION_CONSTRAINT;
        s->err.message = "The specified location-constraint is not valid";
        return;
      }
    } else if (bucket_zonegroup->api_name != location_constraint) {
      ldpp_dout(this, 0) << "location constraint (" << location_constraint
          << ") doesn't match zonegroup (" << bucket_zonegroup->api_name
          << ')' << dendl;
      op_ret = -ERR_INVALID_LOCATION_CONSTRAINT;
      s->err.message = "The specified location-constraint is not valid";
      return;
    }
  }

  // select and validate the placement target
  op_ret = select_bucket_placement(this, *bucket_zonegroup, s->user->get_info(),
                                   createparams.placement_rule);
  if (op_ret < 0) {
    return;
  }

  if (bucket_zonegroup == &my_zonegroup) {
    // look up the zone placement pool
    createparams.zone_placement = rgw::find_zone_placement(
        this, site.get_zone_params(), createparams.placement_rule);
    if (!createparams.zone_placement) {
      op_ret = -ERR_INVALID_LOCATION_CONSTRAINT;
      return;
    }
  }

  // read the bucket info if it exists
  op_ret = driver->load_bucket(this, rgw_bucket(s->bucket_tenant, s->bucket_name),
                               &s->bucket, y);
  if (op_ret < 0 && op_ret != -ENOENT)
    return;
  s->bucket_exists = (op_ret != -ENOENT);
  ceph_assert(s->bucket); // creates handle even on ENOENT

  if (s->bucket_exists) {
    const RGWBucketInfo& info = s->bucket->get_info();

    if (!s->system_request && createparams.zonegroup_id != info.zonegroup) {
      s->err.message = "Cannot modify existing bucket's zonegroup";
      op_ret = -EEXIST;
      return;
    }

    if (!createparams.swift_ver_location) {
      createparams.swift_ver_location = info.swift_ver_location;
    }

    // don't allow changes to placement
    if (createparams.placement_rule != info.placement_rule) {
      s->err.message = "Cannot modify existing bucket's placement rule";
      op_ret = -EEXIST;
      return;
    }

    // don't allow changes to the acl policy
    RGWAccessControlPolicy old_policy;
    int r = rgw_op_get_bucket_policy_from_attr(this, s->cct, driver, info.owner,
                                               s->bucket->get_attrs(),
                                               old_policy, y);
    if (r >= 0 && old_policy != policy) {
      s->err.message = "Cannot modify existing access control policy";
      op_ret = -EEXIST;
      return;
    }
  }

  s->bucket_owner.id = s->user->get_id();
  s->bucket_owner.display_name = s->user->get_display_name();
  createparams.owner = s->user->get_id();

  buffer::list aclbl;
  policy.encode(aclbl);
  createparams.attrs[RGW_ATTR_ACL] = std::move(aclbl);

  if (has_cors) {
    buffer::list corsbl;
    cors_config.encode(corsbl);
    createparams.attrs[RGW_ATTR_CORS] = std::move(corsbl);
  }

  if (need_metadata_upload()) {
    /* It's supposed that following functions WILL NOT change any special
     * attributes (like RGW_ATTR_ACL) if they are already present in attrs. */
    op_ret = rgw_get_request_metadata(this, s->cct, s->info,
                                      createparams.attrs, false);
    if (op_ret < 0) {
      return;
    }
    prepare_add_del_attrs(s->bucket_attrs, rmattr_names, createparams.attrs);
    populate_with_generic_attrs(s, createparams.attrs);

    RGWQuotaInfo quota;
    op_ret = filter_out_quota_info(createparams.attrs, rmattr_names, quota);
    if (op_ret < 0) {
      return;
    }
    createparams.quota = quota;

    /* Web site of Swift API. */
    RGWBucketInfo& info = s->bucket->get_info();
    filter_out_website(createparams.attrs, rmattr_names, info.website_conf);
    info.has_website = !info.website_conf.is_empty();
  }

  if (!driver->is_meta_master()) {
    // apply bucket creation on the master zone first
    bufferlist in_data;
    JSONParser jp;
    op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                           &in_data, &jp, s->info, y);
    if (op_ret < 0) {
      return;
    }

    RGWBucketInfo master_info;
    JSONDecoder::decode_json("bucket_info", master_info, &jp);

    // update params with info from the master
    createparams.marker = master_info.bucket.marker;
    createparams.bucket_id = master_info.bucket.bucket_id;
    createparams.zonegroup_id = master_info.zonegroup;
    createparams.obj_lock_enabled = master_info.obj_lock_enabled();
    createparams.quota = master_info.quota;
    createparams.creation_time = master_info.creation_time;
  }

  ldpp_dout(this, 10) << "user=" << s->user << " bucket=" << s->bucket << dendl;
  op_ret = s->bucket->create(this, createparams, y);

  /* continue if EEXIST and create_bucket will fail below.  this way we can
   * recover from a partial create by retrying it. */
  ldpp_dout(this, 20) << "Bucket::create() returned ret=" << op_ret << " bucket=" << s->bucket << dendl;

  if (op_ret < 0 && op_ret != -EEXIST && op_ret != -ERR_BUCKET_EXISTS)
    return;

  const bool existed = s->bucket_exists;
  if (need_metadata_upload() && existed) {
    /* OK, it looks we lost race with another request. As it's required to
     * handle metadata fusion and upload, the whole operation becomes very
     * similar in nature to PutMetadataBucket. However, as the attrs may
     * changed in the meantime, we have to refresh. */
    short tries = 0;
    do {
      map<string, bufferlist> battrs;

      op_ret = s->bucket->load_bucket(this, y);
      if (op_ret < 0) {
        return;
      } else if (s->bucket->get_owner() != s->user->get_id()) {
        /* New bucket doesn't belong to the account we're operating on. */
        op_ret = -EEXIST;
        return;
      } else {
        s->bucket_attrs = s->bucket->get_attrs();
      }

      createparams.attrs.clear();

      op_ret = rgw_get_request_metadata(this, s->cct, s->info, createparams.attrs, false);
      if (op_ret < 0) {
        return;
      }
      prepare_add_del_attrs(s->bucket_attrs, rmattr_names, createparams.attrs);
      populate_with_generic_attrs(s, createparams.attrs);
      op_ret = filter_out_quota_info(createparams.attrs, rmattr_names,
                                     s->bucket->get_info().quota);
      if (op_ret < 0) {
        return;
      }

      /* Handle updates of the metadata for Swift's object versioning. */
      if (createparams.swift_ver_location) {
        s->bucket->get_info().swift_ver_location = *createparams.swift_ver_location;
        s->bucket->get_info().swift_versioning = !createparams.swift_ver_location->empty();
      }

      /* Web site of Swift API. */
      filter_out_website(createparams.attrs, rmattr_names,
                         s->bucket->get_info().website_conf);
      s->bucket->get_info().has_website = !s->bucket->get_info().website_conf.is_empty();

      /* This will also set the quota on the bucket. */
      op_ret = s->bucket->merge_and_store_attrs(this, createparams.attrs, y);
    } while (op_ret == -ECANCELED && tries++ < 20);

    /* Restore the proper return code. */
    if (op_ret >= 0) {
      op_ret = -ERR_BUCKET_EXISTS;
    }
  }
}

int RGWDeleteBucket::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3DeleteBucket)) {
    return -EACCES;
  }

  return 0;
}

void RGWDeleteBucket::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteBucket::execute(optional_yield y)
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
  ot.read_version = s->bucket->get_version();

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

  op_ret = s->bucket->sync_user_stats(this, y, nullptr);
  if ( op_ret < 0) {
     ldpp_dout(this, 1) << "WARNING: failed to sync user stats before bucket delete: op_ret= " << op_ret << dendl;
  }

  op_ret = s->bucket->check_empty(this, y);
  if (op_ret < 0) {
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    if (op_ret == -ENOENT) {
      /* adjust error, we want to return with NoSuchBucket and not
       * NoSuchKey */
      op_ret = -ERR_NO_SUCH_BUCKET;
    }
    return;
  }

  op_ret = rgw_remove_sse_s3_bucket_key(s, y);
  if (op_ret != 0) {
      // do nothing; it will already have been logged
  }

  op_ret = s->bucket->remove(this, false, y);
  if (op_ret < 0 && op_ret == -ECANCELED) {
      // lost a race, either with mdlog sync or another delete bucket operation.
      // in either case, we've already called ctl.bucket->unlink_bucket()
      op_ret = 0;
  }

  auto counters = rgw::op_counters::get(s);
  rgw::op_counters::inc(counters, l_rgw_op_del_bucket, 1);
  rgw::op_counters::tinc(counters, l_rgw_op_del_bucket_lat, s->time_elapsed());

  return;
}

int RGWPutObj::init_processing(optional_yield y) {
  copy_source = url_decode(s->info.env->get("HTTP_X_AMZ_COPY_SOURCE", ""));
  copy_source_range = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_RANGE");
  size_t pos;
  int ret;

  /* handle x-amz-copy-source */
  std::string_view cs_view(copy_source);
  if (! cs_view.empty()) {
    if (cs_view[0] == '/')
      cs_view.remove_prefix(1);
    copy_source_bucket_name = std::string(cs_view);
    pos = copy_source_bucket_name.find("/");
    if (pos == std::string::npos) {
      ret = -EINVAL;
      ldpp_dout(this, 5) << "x-amz-copy-source bad format" << dendl;
      return ret;
    }
    copy_source_object_name =
      copy_source_bucket_name.substr(pos + 1, copy_source_bucket_name.size());
    copy_source_bucket_name = copy_source_bucket_name.substr(0, pos);
#define VERSION_ID_STR "?versionId="
    pos = copy_source_object_name.find(VERSION_ID_STR);
    if (pos == std::string::npos) {
      copy_source_object_name = url_decode(copy_source_object_name);
    } else {
      copy_source_version_id =
        copy_source_object_name.substr(pos + sizeof(VERSION_ID_STR) - 1);
      copy_source_object_name =
        url_decode(copy_source_object_name.substr(0, pos));
    }
    pos = copy_source_bucket_name.find(":");
    if (pos == std::string::npos) {
      // if tenant is not specified in x-amz-copy-source, use tenant of the requester
      copy_source_tenant_name = s->user->get_tenant();
    } else {
      copy_source_tenant_name = copy_source_bucket_name.substr(0, pos);
      copy_source_bucket_name = copy_source_bucket_name.substr(pos + 1, copy_source_bucket_name.size());
      if (copy_source_bucket_name.empty()) {
        ret = -EINVAL;
        ldpp_dout(this, 5) << "source bucket name is empty" << dendl;
        return ret;
      }
    }
    std::unique_ptr<rgw::sal::Bucket> bucket;
    ret = driver->load_bucket(this, rgw_bucket(copy_source_tenant_name,
                                               copy_source_bucket_name),
                              &bucket, y);
    if (ret < 0) {
      ldpp_dout(this, 5) << __func__ << "(): load_bucket() returned ret=" << ret << dendl;
      if (ret == -ENOENT) {
        ret = -ERR_NO_SUCH_BUCKET;
      }
      return ret;
    }
    copy_source_bucket_info = bucket->get_info();

    /* handle x-amz-copy-source-range */
    if (copy_source_range) {
      string range = copy_source_range;
      pos = range.find("bytes=");
      if (pos == std::string::npos || pos != 0) {
        ret = -EINVAL;
        ldpp_dout(this, 5) << "x-amz-copy-source-range bad format" << dendl;
        return ret;
      }
      /* 6 is the length of "bytes=" */
      range = range.substr(pos + 6);
      pos = range.find("-");
      if (pos == std::string::npos) {
        ret = -EINVAL;
        ldpp_dout(this, 5) << "x-amz-copy-source-range bad format" << dendl;
        return ret;
      }
      string first = range.substr(0, pos);
      string last = range.substr(pos + 1);
      if (first.find_first_not_of("0123456789") != std::string::npos ||
	  last.find_first_not_of("0123456789") != std::string::npos) {
        ldpp_dout(this, 5) << "x-amz-copy-source-range bad format not an integer" << dendl;
        ret = -EINVAL;
        return ret;
      }
      copy_source_range_fst = strtoull(first.c_str(), NULL, 10);
      copy_source_range_lst = strtoull(last.c_str(), NULL, 10);
      if (copy_source_range_fst > copy_source_range_lst) {
        ret = -ERANGE;
        ldpp_dout(this, 5) << "x-amz-copy-source-range bad format first number bigger than second" << dendl;
        return ret;
      }
    }

  } /* copy_source */
  return RGWOp::init_processing(y);
}

int RGWPutObj::verify_permission(optional_yield y)
{
  if (! copy_source.empty()) {

    RGWAccessControlPolicy cs_acl;
    boost::optional<Policy> policy;
    map<string, bufferlist> cs_attrs;
    auto cs_bucket = driver->get_bucket(copy_source_bucket_info);
    auto cs_object = cs_bucket->get_object(rgw_obj_key(copy_source_object_name,
                                                       copy_source_version_id));
    cs_object->set_atomic();
    cs_object->set_prefetch_data();

    /* check source object permissions */
    int ret = read_obj_policy(this, driver, s, copy_source_bucket_info, cs_attrs, cs_acl, nullptr,
                              policy, cs_bucket.get(), cs_object.get(), y, true);
    if (ret < 0) {
      return ret;
    }

    /* admin request overrides permission checks */
    if (! s->auth.identity->is_admin_of(cs_acl.get_owner().id)) {
      if (policy || ! s->iam_user_policies.empty() || !s->session_policies.empty()) {
        //add source object tags for permission evaluation
        auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, policy, s->iam_user_policies, s->session_policies);
        if (has_s3_existing_tag || has_s3_resource_tag)
          rgw_iam_add_objtags(this, s, cs_object.get(), has_s3_existing_tag, has_s3_resource_tag);
        auto usr_policy_res = Effect::Pass;
        rgw::ARN obj_arn(cs_object->get_obj());
        for (auto& user_policy : s->iam_user_policies) {
          if (usr_policy_res = user_policy.eval(s->env, boost::none,
			      cs_object->get_instance().empty() ?
			      rgw::IAM::s3GetObject :
			      rgw::IAM::s3GetObjectVersion,
			      obj_arn); usr_policy_res == Effect::Deny)
            return -EACCES;
          else if (usr_policy_res == Effect::Allow)
            break;
        }
  rgw::IAM::Effect e = Effect::Pass;
  if (policy) {
    rgw::ARN obj_arn(cs_object->get_obj());
	  e = policy->eval(s->env, *s->auth.identity,
			      cs_object->get_instance().empty() ?
			      rgw::IAM::s3GetObject :
			      rgw::IAM::s3GetObjectVersion,
			      obj_arn);
  }
	if (e == Effect::Deny) {
	  return -EACCES; 
	} else if (usr_policy_res == Effect::Pass && e == Effect::Pass &&
		   !cs_acl.verify_permission(this, *s->auth.identity, s->perm_mask,
						RGW_PERM_READ)) {
	  return -EACCES;
	}
      rgw_iam_remove_objtags(this, s, cs_object.get(), has_s3_existing_tag, has_s3_resource_tag);
      } else if (!cs_acl.verify_permission(this, *s->auth.identity, s->perm_mask,
					   RGW_PERM_READ)) {
	return -EACCES;
      }
    }
  }

  if (s->bucket_access_conf && s->bucket_access_conf->block_public_acls()) {
    if (s->canned_acl.compare("public-read") ||
        s->canned_acl.compare("public-read-write") ||
        s->canned_acl.compare("authenticated-read"))
      return -EACCES;
  }

  int ret = get_params(y);
  if (ret < 0) {
    ldpp_dout(this, 20) << "get_params() returned ret=" << ret << dendl;
    return ret;
  }

  if (s->iam_policy || ! s->iam_user_policies.empty() || !s->session_policies.empty()) {
    rgw_add_grant_to_iam_environment(s->env, s);

    rgw_add_to_iam_environment(s->env, "s3:x-amz-acl", s->canned_acl);

    if (obj_tags != nullptr && obj_tags->count() > 0){
      auto tags = obj_tags->get_tags();
      for (const auto& kv: tags){
        rgw_add_to_iam_environment(s->env, "s3:RequestObjectTag/"+kv.first, kv.second);
      }
    }

    // add server-side encryption headers
    rgw_iam_add_crypt_attrs(s->env, s->info.crypt_attribute_map);

    // Add bucket tags for authorization
    auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
    if (has_s3_resource_tag)
      rgw_iam_add_buckettags(this, s);

    auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                            rgw::IAM::s3PutObject,
                                            s->object->get_obj());
    if (identity_policy_res == Effect::Deny)
      return -EACCES;

    rgw::IAM::Effect e = Effect::Pass;
    rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
    if (s->iam_policy) {
      ARN obj_arn(s->object->get_obj());
      e = s->iam_policy->eval(s->env, *s->auth.identity,
          rgw::IAM::s3PutObject,
          obj_arn,
          princ_type);
    }
    if (e == Effect::Deny) {
      return -EACCES;
    }

    if (!s->session_policies.empty()) {
      auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                              rgw::IAM::s3PutObject,
                                              s->object->get_obj());
      if (session_policy_res == Effect::Deny) {
          return -EACCES;
      }
      if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
        //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
            (session_policy_res == Effect::Allow && e == Effect::Allow))
          return 0;
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
        //Intersection of session policy and identity policy plus bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || e == Effect::Allow)
          return 0;
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
        if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow)
          return 0;
      }
      return -EACCES;
    }
    if (e == Effect::Allow || identity_policy_res == Effect::Allow) {
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
  bl.begin(bl_ofs).copy(bl_len, bl_tmp);

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
  int ret = 0;

  uint64_t obj_size;
  int64_t new_ofs, new_end;

  new_ofs = fst;
  new_end = lst;

  auto bucket = driver->get_bucket(copy_source_bucket_info);
  auto obj = bucket->get_object(rgw_obj_key(copy_source_object_name,
                                            copy_source_version_id));
  auto read_op = obj->get_read_op();

  ret = read_op->prepare(s->yield, this);
  if (ret < 0)
    return ret;

  obj_size = obj->get_obj_size();

  bool need_decompress;
  op_ret = rgw_compression_info_from_attrset(obj->get_attrs(), need_decompress, cs_info);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to decode compression info" << dendl;
    return -EIO;
  }

  bool partial_content = true;
  if (need_decompress)
  {
    obj_size = cs_info.orig_size;
    decompress.emplace(s->cct, &cs_info, partial_content, filter);
    filter = &*decompress;
  }

  auto attr_iter = obj->get_attrs().find(RGW_ATTR_MANIFEST);
  op_ret = this->get_decrypt_filter(&decrypt,
                                    filter,
                                    obj->get_attrs(),
                                    attr_iter != obj->get_attrs().end() ? &(attr_iter->second) : nullptr);
  if (decrypt != nullptr) {
    filter = decrypt.get();
  }
  if (op_ret < 0) {
    return op_ret;
  }

  ret = obj->range_to_ofs(obj_size, new_ofs, new_end);
  if (ret < 0)
    return ret;

  filter->fixup_range(new_ofs, new_end);
  ret = read_op->iterate(this, new_ofs, new_end, filter, s->yield);

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

auto RGWPutObj::get_torrent_filter(rgw::sal::DataProcessor* cb)
    -> std::optional<RGWPutObj_Torrent>
{
  auto& conf = get_cct()->_conf;
  if (!conf->rgw_torrent_flag) {
    return std::nullopt; // torrent generation disabled
  }
  const auto max_len = conf->rgw_torrent_max_size;
  const auto piece_len = conf->rgw_torrent_sha_unit;
  if (!max_len || !piece_len) {
    return std::nullopt; // invalid configuration
  }
  if (crypt_http_responses.count("x-amz-server-side-encryption-customer-algorithm")) {
    return std::nullopt; // downloading the torrent would require customer keys
  }
  return RGWPutObj_Torrent{cb, max_len, piece_len};
}

int RGWPutObj::get_lua_filter(std::unique_ptr<rgw::sal::DataProcessor>* filter, rgw::sal::DataProcessor* cb) {
  std::string script;
  const auto rc = rgw::lua::read_script(s, s->penv.lua.manager.get(), s->bucket_tenant, s->yield, rgw::lua::context::putData, script);
  if (rc == -ENOENT) {
    // no script, nothing to do
    return 0;
  } else if (rc < 0) {
    ldpp_dout(this, 5) << "WARNING: failed to read data script. error: " << rc << dendl;
    return rc;
  }
  filter->reset(new rgw::lua::RGWPutObjFilter(s, script, cb));
  return 0;
}

void RGWPutObj::execute(optional_yield y)
{
  char supplied_md5_bin[CEPH_CRYPTO_MD5_DIGESTSIZE + 1];
  char supplied_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  bufferlist bl, aclbl, bs;
  int len;
  
  off_t fst;
  off_t lst;

  auto counters = rgw::op_counters::get(s);

  bool need_calc_md5 = (dlo_manifest == NULL) && (slo_info == NULL);
  rgw::op_counters::inc(counters, l_rgw_op_put_obj, 1);

  // report latency on return
  auto put_lat = make_scope_guard([&] {
      rgw::op_counters::tinc(counters, l_rgw_op_put_obj_lat, s->time_elapsed());
    });

  op_ret = -EINVAL;
  if (rgw::sal::Object::empty(s->object.get())) {
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
    op_ret = s->bucket->check_quota(this, quota, s->content_length, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "check_quota() returned ret=" << op_ret << dendl;
      return;
    }
  }

  if (supplied_etag) {
    strncpy(supplied_md5, supplied_etag, sizeof(supplied_md5) - 1);
    supplied_md5[sizeof(supplied_md5) - 1] = '\0';
  }

  const bool multipart = !multipart_upload_id.empty();

  /* Handle object versioning of Swift API. */
  if (! multipart) {
    op_ret = s->object->swift_versioning_copy(this, s->yield);
    if (op_ret < 0) {
      return;
    }
  }

  // make reservation for notification if needed
  std::unique_ptr<rgw::sal::Notification> res
		     = driver->get_notification(
		       s->object.get(), s->src_object.get(), s,
		       rgw::notify::ObjectCreatedPut, y);
  if(!multipart) {
    op_ret = res->publish_reserve(this, obj_tags.get());
    if (op_ret < 0) {
      return;
    }
  }

  // create the object processor
  std::unique_ptr<rgw::sal::Writer> processor;

  rgw_placement_rule *pdest_placement = &s->dest_placement;

  if (multipart) {
    std::unique_ptr<rgw::sal::MultipartUpload> upload;
    upload = s->bucket->get_multipart_upload(s->object->get_name(),
					 multipart_upload_id);
    op_ret = upload->get_info(this, s->yield, &pdest_placement);

    s->trace->SetAttribute(tracing::rgw::UPLOAD_ID, multipart_upload_id);
    multipart_trace = tracing::rgw::tracer.add_span(name(), upload->get_trace());

    if (op_ret < 0) {
      if (op_ret != -ENOENT) {
        ldpp_dout(this, 0) << "ERROR: get_multipart_info returned " << op_ret << ": " << cpp_strerror(-op_ret) << dendl;
      } else {// -ENOENT: raced with upload complete/cancel, no need to spam log
        ldpp_dout(this, 20) << "failed to get multipart info (returned " << op_ret << ": " << cpp_strerror(-op_ret) << "): probably raced with upload complete / cancel" << dendl;
      }
      return;
    }
    /* upload will go out of scope, so copy the dest placement for later use */
    s->dest_placement = *pdest_placement;
    pdest_placement = &s->dest_placement;
    ldpp_dout(this, 20) << "dest_placement for part=" << *pdest_placement << dendl;
    processor = upload->get_writer(this, s->yield, s->object.get(),
				   s->user->get_id(), pdest_placement,
				   multipart_part_num, multipart_part_str);
  } else if(append) {
    if (s->bucket->versioned()) {
      op_ret = -ERR_INVALID_BUCKET_STATE;
      return;
    }
    processor = driver->get_append_writer(this, s->yield, s->object.get(),
					 s->bucket_owner.id,
					 pdest_placement, s->req_id, position,
					 &cur_accounted_size);
  } else {
    if (s->bucket->versioning_enabled()) {
      if (!version_id.empty()) {
        s->object->set_instance(version_id);
      } else {
	s->object->gen_rand_obj_instance_name();
        version_id = s->object->get_instance();
      }
    }
    processor = driver->get_atomic_writer(this, s->yield, s->object.get(),
					 s->bucket_owner.id,
					 pdest_placement, olh_epoch, s->req_id);
  }

  op_ret = processor->prepare(s->yield);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "processor->prepare() returned ret=" << op_ret
		      << dendl;
    return;
  }
  if ((! copy_source.empty()) && !copy_source_range) {
    auto bucket = driver->get_bucket(copy_source_bucket_info);
    auto obj = bucket->get_object(rgw_obj_key(copy_source_object_name,
                                              copy_source_version_id));

    RGWObjState *astate;
    op_ret = obj->get_obj_state(this, &astate, s->yield);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "ERROR: get copy source obj state returned with error" << op_ret << dendl;
      return;
    }
    bufferlist bl;
    if (astate->get_attr(RGW_ATTR_MANIFEST, bl)) {
      RGWObjManifest m;
      try{
        decode(m, bl);
        if (m.get_tier_type() == "cloud-s3") {
          op_ret = -ERR_INVALID_OBJECT_STATE;
          s->err.message = "This object was transitioned to cloud-s3";
          ldpp_dout(this, 4) << "Cannot copy cloud tiered object. Failing with "
                         << op_ret << dendl;
          return;
        }
      } catch (const buffer::end_of_buffer&) {
        // ignore empty manifest; it's not cloud-tiered
      } catch (const std::exception& e) {
        ldpp_dout(this, 1) << "WARNING: failed to decode object manifest for "
            << *s->object << ": " << e.what() << dendl;
      }
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
  rgw::sal::DataProcessor *filter = processor.get();

  const auto& compression_type = driver->get_compression_type(*pdest_placement);
  CompressorRef plugin;
  std::optional<RGWPutObj_Compress> compressor;
  std::optional<RGWPutObj_Torrent> torrent;

  std::unique_ptr<rgw::sal::DataProcessor> encrypt;
  std::unique_ptr<rgw::sal::DataProcessor> run_lua;

  if (!append) { // compression and encryption only apply to full object uploads
    op_ret = get_encrypt_filter(&encrypt, filter);
    if (op_ret < 0) {
      return;
    }
    if (encrypt != nullptr) {
      filter = &*encrypt;
    }
    // a zonegroup feature is required to combine compression and encryption
    const RGWZoneGroup& zonegroup = s->penv.site->get_zonegroup();
    const bool compress_encrypted = zonegroup.supports(rgw::zone_features::compress_encrypted);
    if (compression_type != "none" &&
        (encrypt == nullptr || compress_encrypted)) {
      plugin = get_compressor_plugin(s, compression_type);
      if (!plugin) {
        ldpp_dout(this, 1) << "Cannot load plugin for compression type "
            << compression_type << dendl;
      } else {
        compressor.emplace(s->cct, plugin, filter);
        filter = &*compressor;
        // always send incompressible hint when rgw is itself doing compression
        s->object->set_compressed();
      }
    }
    if (torrent = get_torrent_filter(filter); torrent) {
      filter = &*torrent;
    }
    // run lua script before data is compressed and encrypted - last filter runs first
    op_ret = get_lua_filter(&run_lua, filter);
    if (op_ret < 0) {
      return;
    }
    if (run_lua) {
      filter = &*run_lua;
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
      off_t cur_lst = min<off_t>(fst + s->cct->_conf->rgw_max_chunk_size - 1, lst);
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
  s->object->set_obj_size(ofs);

  rgw::op_counters::inc(counters, l_rgw_op_put_obj_b, s->obj_size);

  op_ret = do_aws4_auth_completion();
  if (op_ret < 0) {
    return;
  }

  op_ret = s->bucket->check_quota(this, quota, s->obj_size, y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "second check_quota() returned op_ret=" << op_ret << dendl;
    return;
  }

  hash.Final(m);

  if (compressor && compressor->is_compressed()) {
    bufferlist tmp;
    RGWCompressionInfo cs_info;      
    assert(plugin != nullptr);  
    // plugin exists when the compressor does
    // coverity[dereference:SUPPRESS]
    cs_info.compression_type = plugin->get_type_name();
    cs_info.orig_size = s->obj_size;
    cs_info.compressor_message = compressor->get_compressor_message();
    cs_info.blocks = std::move(compressor->get_compression_blocks());
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
    ldpp_dout(this, 20) << "storing " << RGW_ATTR_COMPRESSION
        << " with type=" << cs_info.compression_type
        << ", orig_size=" << cs_info.orig_size
        << ", blocks=" << cs_info.blocks.size() << dendl;
  }
  if (torrent) {
    auto bl = torrent->bencode_torrent(s->object->get_name());
    if (bl.length()) {
      ldpp_dout(this, 20) << "storing " << bl.length()
         << " bytes of torrent info in " << RGW_ATTR_TORRENT << dendl;
      attrs[RGW_ATTR_TORRENT] = std::move(bl);
    }
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
  }

  if (slo_info) {
    bufferlist manifest_bl;
    encode(*slo_info, manifest_bl);
    emplace_attr(RGW_ATTR_SLO_MANIFEST, std::move(manifest_bl));
  }

  if (supplied_etag && etag.compare(supplied_etag) != 0) {
    op_ret = -ERR_UNPROCESSABLE_ENTITY;
    return;
  }
  bl.append(etag.c_str(), etag.size());
  emplace_attr(RGW_ATTR_ETAG, std::move(bl));

  populate_with_generic_attrs(s, attrs);
  op_ret = rgw_get_request_metadata(this, s->cct, s->info, attrs);
  if (op_ret < 0) {
    return;
  }
  encode_delete_at_attr(delete_at, attrs);
  encode_obj_tags_attr(obj_tags.get(), attrs);
  rgw_cond_decode_objtags(s, attrs);

  /* Add a custom metadata to expose the information whether an object
   * is an SLO or not. Appending the attribute must be performed AFTER
   * processing any input from user in order to prohibit overwriting. */
  if (slo_info) {
    bufferlist slo_userindicator_bl;
    slo_userindicator_bl.append("True", 4);
    emplace_attr(RGW_ATTR_SLO_UINDICATOR, std::move(slo_userindicator_bl));
  }
  if (obj_legal_hold) {
    bufferlist obj_legal_hold_bl;
    obj_legal_hold->encode(obj_legal_hold_bl);
    emplace_attr(RGW_ATTR_OBJECT_LEGAL_HOLD, std::move(obj_legal_hold_bl));
  }
  if (obj_retention) {
    bufferlist obj_retention_bl;
    obj_retention->encode(obj_retention_bl);
    emplace_attr(RGW_ATTR_OBJECT_RETENTION, std::move(obj_retention_bl));
  }

  // don't track the individual parts of multipart uploads. they replicate in
  // full after CompleteMultipart
  const uint32_t complete_flags = multipart ? 0 : rgw::sal::FLAG_LOG_OP;

  tracepoint(rgw_op, processor_complete_enter, s->req_id.c_str());
  const req_context rctx{this, s->yield, s->trace.get()};
  op_ret = processor->complete(s->obj_size, etag, &mtime, real_time(), attrs,
                               (delete_at ? *delete_at : real_time()), if_match, if_nomatch,
                               (user_data.empty() ? nullptr : &user_data), nullptr, nullptr,
                               rctx, complete_flags);
  tracepoint(rgw_op, processor_complete_exit, s->req_id.c_str());
  if (op_ret < 0) {
    return;
  }

  // send request to notification manager
  int ret = res->publish_commit(this, s->obj_size, mtime, etag, s->object->get_instance());
  if (ret < 0) {
    ldpp_dout(this, 1) << "ERROR: publishing notification failed, with error: " << ret << dendl;
    // too late to rollback operation, hence op_ret is not set here
  }
}

int RGWPostObj::verify_permission(optional_yield y)
{
  return 0;
}

void RGWPostObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPostObj::execute(optional_yield y)
{
  boost::optional<RGWPutObj_Compress> compressor;
  CompressorRef plugin;
  char supplied_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];

  /* Read in the data from the POST form. */
  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  op_ret = verify_params();
  if (op_ret < 0) {
    return;
  }

  // add server-side encryption headers
  rgw_iam_add_crypt_attrs(s->env, s->info.crypt_attribute_map);

  if (s->iam_policy || ! s->iam_user_policies.empty() || !s->session_policies.empty()) {
    auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                            rgw::IAM::s3PutObject,
                                            s->object->get_obj());
    if (identity_policy_res == Effect::Deny) {
      op_ret = -EACCES;
      return;
    }

    rgw::IAM::Effect e = Effect::Pass;
    rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
    if (s->iam_policy) {
      ARN obj_arn(s->object->get_obj());
      e = s->iam_policy->eval(s->env, *s->auth.identity,
				 rgw::IAM::s3PutObject,
				 obj_arn,
         princ_type);
    }
    if (e == Effect::Deny) {
      op_ret = -EACCES;
      return;
    }

    if (!s->session_policies.empty()) {
      auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                              rgw::IAM::s3PutObject,
                                              s->object->get_obj());
      if (session_policy_res == Effect::Deny) {
          op_ret = -EACCES;
          return;
      }
      if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
        //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
            (session_policy_res == Effect::Allow && e == Effect::Allow)) {
          op_ret = 0;
          return;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
        //Intersection of session policy and identity policy plus bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || e == Effect::Allow) {
          op_ret = 0;
          return;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
        if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) {
          op_ret = 0;
          return;
        }
      }
      op_ret = -EACCES;
      return;
    }
    if (identity_policy_res == Effect::Pass && e == Effect::Pass && !verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
      op_ret = -EACCES;
      return;
    }
  } else if (!verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    op_ret = -EACCES;
    return;
  }

  // make reservation for notification if needed
  std::unique_ptr<rgw::sal::Notification> res
    = driver->get_notification(s->object.get(), s->src_object.get(), s, rgw::notify::ObjectCreatedPost, y);
  op_ret = res->publish_reserve(this);
  if (op_ret < 0) {
    return;
  }

  /* Start iteration over data fields. It's necessary as Swift's FormPost
   * is capable to handle multiple files in single form. */
  do {
    char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
    unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
    MD5 hash;
    // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
    hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
    ceph::buffer::list bl, aclbl;

    op_ret = s->bucket->check_quota(this, quota, s->content_length, y);
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

    std::unique_ptr<rgw::sal::Object> obj =
		     s->bucket->get_object(rgw_obj_key(get_current_filename()));
    if (s->bucket->versioning_enabled()) {
      obj->gen_rand_obj_instance_name();
    }

    std::unique_ptr<rgw::sal::Writer> processor;
    processor = driver->get_atomic_writer(this, s->yield, obj.get(),
					 s->bucket_owner.id,
					 &s->dest_placement, 0, s->req_id);
    op_ret = processor->prepare(s->yield);
    if (op_ret < 0) {
      return;
    }

    /* No filters by default. */
    rgw::sal::DataProcessor *filter = processor.get();

    std::unique_ptr<rgw::sal::DataProcessor> encrypt;
    op_ret = get_encrypt_filter(&encrypt, filter);
    if (op_ret < 0) {
      return;
    }
    if (encrypt != nullptr) {
      filter = encrypt.get();
    } else {
      const auto& compression_type = driver->get_compression_type(s->dest_placement);
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
      int len = get_data(data, again);

      if (len < 0) {
        op_ret = len;
        return;
      }

      if (!len) {
        break;
      }

      hash.Update((const unsigned char *)data.c_str(), data.length());
      op_ret = filter->process(std::move(data), ofs);
      if (op_ret < 0) {
        return;
      }

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

    if (ofs < min_len) {
      op_ret = -ERR_TOO_SMALL;
      return;
    }

    s->obj_size = ofs;
    s->object->set_obj_size(ofs);


    op_ret = s->bucket->check_quota(this, quota, s->obj_size, y);
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
      assert(plugin != nullptr);
      // plugin exists when the compressor does
      // coverity[dereference:SUPPRESS]
      cs_info.compression_type = plugin->get_type_name();
      cs_info.orig_size = s->obj_size;
      cs_info.compressor_message = compressor->get_compressor_message();
      cs_info.blocks = std::move(compressor->get_compression_blocks());
      encode(cs_info, tmp);
      emplace_attr(RGW_ATTR_COMPRESSION, std::move(tmp));
    }

    const req_context rctx{this, s->yield, s->trace.get()};
    op_ret = processor->complete(s->obj_size, etag, nullptr, real_time(), attrs,
                                (delete_at ? *delete_at : real_time()),
                                nullptr, nullptr, nullptr, nullptr, nullptr,
                                rctx, rgw::sal::FLAG_LOG_OP);
    if (op_ret < 0) {
      return;
    }
  } while (is_next_file_to_upload());

  // send request to notification manager
  int ret = res->publish_commit(this, ofs, s->object->get_mtime(), etag, s->object->get_instance());
  if (ret < 0) {
    ldpp_dout(this, 1) << "ERROR: publishing notification failed, with error: " << ret << dendl;
    // too late to rollback operation, hence op_ret is not set here
  }
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

int RGWPutMetadataAccount::init_processing(optional_yield y)
{
  /* First, go to the base class. At the time of writing the method was
   * responsible only for initializing the quota. This isn't necessary
   * here as we are touching metadata only. I'm putting this call only
   * for the future. */
  op_ret = RGWOp::init_processing(y);
  if (op_ret < 0) {
    return op_ret;
  }

  op_ret = get_params(y);
  if (op_ret < 0) {
    return op_ret;
  }

  op_ret = s->user->read_attrs(this, y);
  if (op_ret < 0) {
    return op_ret;
  }
  orig_attrs = s->user->get_attrs();

  if (has_policy) {
    bufferlist acl_bl;
    policy.encode(acl_bl);
    attrs.emplace(RGW_ATTR_ACL, std::move(acl_bl));
  }

  op_ret = rgw_get_request_metadata(this, s->cct, s->info, attrs, false);
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

int RGWPutMetadataAccount::verify_permission(optional_yield y)
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

void RGWPutMetadataAccount::execute(optional_yield y)
{
  /* Params have been extracted earlier. See init_processing(). */
  op_ret = s->user->load_user(this, y);
  if (op_ret < 0) {
    return;
  }

  /* Handle the TempURL-related stuff. */
  if (!temp_url_keys.empty()) {
    for (auto& pair : temp_url_keys) {
      s->user->get_info().temp_url_keys[pair.first] = std::move(pair.second);
    }
  }

  /* Handle the quota extracted at the verify_permission step. */
  if (new_quota_extracted) {
    s->user->get_info().quota.user_quota = std::move(new_quota);
  }

  /* We are passing here the current (old) user info to allow the function
   * optimize-out some operations. */
  s->user->set_attrs(attrs);
  op_ret = s->user->store_user(this, y, false, &s->user->get_info());
}

int RGWPutMetadataBucket::verify_permission(optional_yield y)
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

void RGWPutMetadataBucket::execute(optional_yield y)
{
  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  op_ret = rgw_get_request_metadata(this, s->cct, s->info, attrs, false);
  if (op_ret < 0) {
    return;
  }

  if (!placement_rule.empty() &&
      placement_rule != s->bucket->get_placement_rule()) {
    op_ret = -EEXIST;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this] {
      /* Encode special metadata first as we're using std::map::emplace under
       * the hood. This method will add the new items only if the map doesn't
       * contain such keys yet. */
      if (has_policy) {
	if (s->dialect.compare("swift") == 0) {
	  rgw::swift::merge_policy(policy_rw_mask, s->bucket_acl, policy);
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
      op_ret = filter_out_quota_info(attrs, rmattr_names, s->bucket->get_info().quota);
      if (op_ret < 0) {
	return op_ret;
      }

      if (swift_ver_location) {
	s->bucket->get_info().swift_ver_location = *swift_ver_location;
	s->bucket->get_info().swift_versioning = (!swift_ver_location->empty());
      }

      /* Web site of Swift API. */
      filter_out_website(attrs, rmattr_names, s->bucket->get_info().website_conf);
      s->bucket->get_info().has_website = !s->bucket->get_info().website_conf.is_empty();

      /* Setting attributes also stores the provided bucket info. Due
       * to this fact, the new quota settings can be serialized with
       * the same call. */
      op_ret = s->bucket->merge_and_store_attrs(this, attrs, s->yield);
      return op_ret;
    }, y);
}

int RGWPutMetadataObject::verify_permission(optional_yield y)
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

void RGWPutMetadataObject::execute(optional_yield y)
{
  rgw::sal::Attrs attrs, rmattrs;

  s->object->set_atomic();

  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  op_ret = rgw_get_request_metadata(this, s->cct, s->info, attrs);
  if (op_ret < 0) {
    return;
  }

  /* check if obj exists, read orig attrs */
  op_ret = s->object->get_obj_attrs(s->yield, s);
  if (op_ret < 0) {
    return;
  }

  /* Check whether the object has expired. Swift API documentation
   * stands that we should return 404 Not Found in such case. */
  if (need_object_expiration() && s->object->is_expired()) {
    op_ret = -ENOENT;
    return;
  }

  /* Filter currently existing attributes. */
  prepare_add_del_attrs(s->object->get_attrs(), attrs, rmattrs);
  populate_with_generic_attrs(s, attrs);
  encode_delete_at_attr(delete_at, attrs);

  if (dlo_manifest) {
    op_ret = encode_dlo_manifest_attr(dlo_manifest, attrs);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "bad user manifest: " << dlo_manifest << dendl;
      return;
    }
  }

  op_ret = s->object->set_obj_attrs(this, &attrs, &rmattrs, s->yield);
}

int RGWDeleteObj::handle_slo_manifest(bufferlist& bl, optional_yield y)
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
          new RGWBulkDelete::Deleter(this, driver, s));
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  }

  list<RGWBulkDelete::acct_path_t> items;
  for (const auto& iter : slo_info.entries) {
    const string& path_str = iter.path;

    const size_t pos_init = path_str.find_first_not_of('/');
    if (std::string_view::npos == pos_init) {
      return -EINVAL;
    }

    const size_t sep_pos = path_str.find('/', pos_init);
    if (std::string_view::npos == sep_pos) {
      return -EINVAL;
    }

    RGWBulkDelete::acct_path_t path;

    path.bucket_name = url_decode(path_str.substr(pos_init, sep_pos - pos_init));
    path.obj_key = url_decode(path_str.substr(sep_pos + 1));

    items.push_back(path);
  }

  /* Request removal of the manifest object itself. */
  RGWBulkDelete::acct_path_t path;
  path.bucket_name = s->bucket_name;
  path.obj_key = s->object->get_key();
  items.push_back(path);

  int ret = deleter->delete_chunk(items, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWDeleteObj::verify_permission(optional_yield y)
{
  int op_ret = get_params(y);
  if (op_ret) {
    return op_ret;
  }

  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
    if (has_s3_existing_tag || has_s3_resource_tag)
      rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  if (s->iam_policy || ! s->iam_user_policies.empty() || ! s->session_policies.empty()) {
    if (s->bucket->get_info().obj_lock_enabled() && bypass_governance_mode) {
      auto r = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                               rgw::IAM::s3BypassGovernanceRetention, ARN(s->bucket->get_key(), s->object->get_name()));
      if (r == Effect::Deny) {
        bypass_perm = false;
      } else if (r == Effect::Pass && s->iam_policy) {
        ARN obj_arn(ARN(s->bucket->get_key(), s->object->get_name()));
        r = s->iam_policy->eval(s->env, *s->auth.identity, rgw::IAM::s3BypassGovernanceRetention, obj_arn);
        if (r == Effect::Deny) {
          bypass_perm = false;
        }
      } else if (r == Effect::Pass && !s->session_policies.empty()) {
        r = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                               rgw::IAM::s3BypassGovernanceRetention, ARN(s->bucket->get_key(), s->object->get_name()));
        if (r == Effect::Deny) {
          bypass_perm = false;
        }
      }
    }
    auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                              s->object->get_instance().empty() ?
                                              rgw::IAM::s3DeleteObject :
                                              rgw::IAM::s3DeleteObjectVersion,
                                              ARN(s->bucket->get_key(), s->object->get_name()));
    if (identity_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect r = Effect::Pass;
    rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
    ARN obj_arn(ARN(s->bucket->get_key(), s->object->get_name()));
    if (s->iam_policy) {
      r = s->iam_policy->eval(s->env, *s->auth.identity,
				 s->object->get_instance().empty() ?
				 rgw::IAM::s3DeleteObject :
				 rgw::IAM::s3DeleteObjectVersion,
				 obj_arn,
         princ_type);
    }
    if (r == Effect::Deny)
      return -EACCES;

    if (!s->session_policies.empty()) {
      auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                              s->object->get_instance().empty() ?
                                              rgw::IAM::s3DeleteObject :
                                              rgw::IAM::s3DeleteObjectVersion,
                                              obj_arn);
      if (session_policy_res == Effect::Deny) {
          return -EACCES;
      }
      if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
        //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
            (session_policy_res == Effect::Allow && r == Effect::Allow)) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
        //Intersection of session policy and identity policy plus bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || r == Effect::Allow) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
        if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) {
          return 0;
        }
      }
      return -EACCES;
    }
    if (r == Effect::Allow || identity_policy_res == Effect::Allow)
      return 0;
  }

  if (!verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

  if (s->bucket->get_info().mfa_enabled() &&
      !s->object->get_instance().empty() &&
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

void RGWDeleteObj::execute(optional_yield y)
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  if (!rgw::sal::Object::empty(s->object.get())) {
    uint64_t obj_size = 0;
    std::string etag;
    {
      RGWObjState* astate = nullptr;
      bool check_obj_lock = s->object->have_instance() && s->bucket->get_info().obj_lock_enabled();

      op_ret = s->object->get_obj_state(this, &astate, s->yield, true);
      if (op_ret < 0) {
        if (need_object_expiration() || multipart_delete) {
          return;
        }

        if (check_obj_lock) {
          /* check if obj exists, read orig attrs */
          if (op_ret == -ENOENT) {
            /* object maybe delete_marker, skip check_obj_lock*/
            check_obj_lock = false;
          } else {
            return;
          }
        }
      } else {
        obj_size = astate->size;
        etag = astate->attrset[RGW_ATTR_ETAG].to_str();
      }

      // ignore return value from get_obj_attrs in all other cases
      op_ret = 0;

      if (check_obj_lock) {
        ceph_assert(astate);
        int object_lock_response = verify_object_lock(this, astate->attrset, bypass_perm, bypass_governance_mode);
        if (object_lock_response != 0) {
          op_ret = object_lock_response;
          if (op_ret == -EACCES) {
            s->err.message = "forbidden by object lock";
          }
          return;
        }
      }

      if (multipart_delete) {
        if (!astate) {
          op_ret = -ERR_NOT_SLO_MANIFEST;
          return;
        }

        const auto slo_attr = astate->attrset.find(RGW_ATTR_SLO_MANIFEST);

        if (slo_attr != astate->attrset.end()) {
          op_ret = handle_slo_manifest(slo_attr->second, y);
          if (op_ret < 0) {
            ldpp_dout(this, 0) << "ERROR: failed to handle slo manifest ret=" << op_ret << dendl;
          }
        } else {
          op_ret = -ERR_NOT_SLO_MANIFEST;
        }

        return;
      }
    }

    // make reservation for notification if needed
    const auto versioned_object = s->bucket->versioning_enabled();
    const auto event_type = versioned_object &&
      s->object->get_instance().empty() ?
      rgw::notify::ObjectRemovedDeleteMarkerCreated :
      rgw::notify::ObjectRemovedDelete;
    std::unique_ptr<rgw::sal::Notification> res
      = driver->get_notification(s->object.get(), s->src_object.get(), s,
				event_type, y);
    op_ret = res->publish_reserve(this);
    if (op_ret < 0) {
      return;
    }

    s->object->set_atomic();
    
    bool ver_restored = false;
    op_ret = s->object->swift_versioning_restore(ver_restored, this, y);
    if (op_ret < 0) {
      return;
    }

    if (!ver_restored) {
      uint64_t epoch = 0;

      /* Swift's versioning mechanism hasn't found any previous version of
       * the object that could be restored. This means we should proceed
       * with the regular delete path. */
      op_ret = get_system_versioning_params(s, &epoch, &version_id);
      if (op_ret < 0) {
	return;
      }

      std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = s->object->get_delete_op();
      del_op->params.obj_owner = s->owner;
      del_op->params.bucket_owner = s->bucket_owner;
      del_op->params.versioning_status = s->bucket->get_info().versioning_status();
      del_op->params.unmod_since = unmod_since;
      del_op->params.high_precision_time = s->system_request;
      del_op->params.olh_epoch = epoch;
      del_op->params.marker_version_id = version_id;

      op_ret = del_op->delete_obj(this, y, rgw::sal::FLAG_LOG_OP);
      if (op_ret >= 0) {
	delete_marker = del_op->result.delete_marker;
	version_id = del_op->result.version_id;
      }

      /* Check whether the object has expired. Swift API documentation
       * stands that we should return 404 Not Found in such case. */
      if (need_object_expiration() && s->object->is_expired()) {
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

    auto counters = rgw::op_counters::get(s);
    rgw::op_counters::inc(counters, l_rgw_op_del_obj, 1);
    rgw::op_counters::inc(counters, l_rgw_op_del_obj_b, obj_size);
    rgw::op_counters::tinc(counters, l_rgw_op_del_obj_lat, s->time_elapsed());

    if (op_ret < 0) {
      return;
    }

    // send request to notification manager
    int ret = res->publish_commit(this, obj_size, ceph::real_clock::now(), etag, version_id);
    if (ret < 0) {
      ldpp_dout(this, 1) << "ERROR: publishing notification failed, with error: " << ret << dendl;
      // too late to rollback operation, hence op_ret is not set here
    }
  } else {
    op_ret = -EINVAL;
  }
}

bool RGWCopyObj::parse_copy_location(const std::string_view& url_src,
				     string& bucket_name,
				     rgw_obj_key& key,
                                     req_state* s)
{
  std::string_view name_str;
  std::string_view params_str;

  // search for ? before url-decoding so we don't accidentally match %3F
  size_t pos = url_src.find('?');
  if (pos == string::npos) {
    name_str = url_src;
  } else {
    name_str = url_src.substr(0, pos);
    params_str = url_src.substr(pos + 1);
  }

  if (name_str[0] == '/') // trim leading slash
    name_str.remove_prefix(1);

  std::string dec_src = url_decode(name_str);

  pos = dec_src.find('/');
  if (pos == string::npos)
    return false;

  bucket_name = dec_src.substr(0, pos);
  key.name = dec_src.substr(pos + 1);

  if (key.name.empty()) {
    return false;
  }

  if (! params_str.empty()) {
    RGWHTTPArgs args;
    args.set(std::string(params_str));
    args.parse(s);

    key.instance = args.get("versionId", NULL);
  }

  return true;
}

int RGWCopyObj::init_processing(optional_yield y)
{
  op_ret = RGWOp::init_processing(y);
  if (op_ret < 0) {
    return op_ret;
  }

  op_ret = get_params(y);
  if (op_ret < 0)
    return op_ret;

  op_ret = get_system_versioning_params(s, &olh_epoch, &version_id);
  if (op_ret < 0) {
    return op_ret;
  }

  op_ret = driver->load_bucket(this, rgw_bucket(s->src_tenant_name,
                                                s->src_bucket_name),
                               &src_bucket, y);
  if (op_ret < 0) {
    if (op_ret == -ENOENT) {
      op_ret = -ERR_NO_SUCH_BUCKET;
    }
    return op_ret;
  }

  /* This is the only place the bucket is set on src_object */
  s->src_object->set_bucket(src_bucket.get());
  return 0;
}

int RGWCopyObj::verify_permission(optional_yield y)
{
  RGWAccessControlPolicy src_acl;
  boost::optional<Policy> src_policy;

  /* get buckets info (source and dest) */
  if (s->local_source &&  source_zone.empty()) {
    s->src_object->set_atomic();
    s->src_object->set_prefetch_data();

    rgw_placement_rule src_placement;

    /* check source object permissions */
    op_ret = read_obj_policy(this, driver, s, src_bucket->get_info(), src_bucket->get_attrs(), src_acl, &src_placement.storage_class,
			     src_policy, src_bucket.get(), s->src_object.get(), y);
    if (op_ret < 0) {
      return op_ret;
    }

    /* follow up on previous checks that required reading source object head */
    if (need_to_check_storage_class) {
      src_placement.inherit_from(src_bucket->get_placement_rule());

      op_ret  = check_storage_class(src_placement);
      if (op_ret < 0) {
        return op_ret;
      }
    }

    /* admin request overrides permission checks */
    if (!s->auth.identity->is_admin_of(src_acl.get_owner().id)) {
      if (src_policy || ! s->iam_user_policies.empty() || !s->session_policies.empty()) {
        auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, src_policy, s->iam_user_policies, s->session_policies);
        if (has_s3_existing_tag || has_s3_resource_tag)
          rgw_iam_add_objtags(this, s, s->src_object.get(), has_s3_existing_tag, has_s3_resource_tag);

        ARN obj_arn(s->src_object->get_obj());
        auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                                  s->src_object->get_instance().empty() ?
                                                  rgw::IAM::s3GetObject :
                                                  rgw::IAM::s3GetObjectVersion,
                                                  obj_arn);
        if (identity_policy_res == Effect::Deny) {
          return -EACCES;
        }
        auto e = Effect::Pass;
        rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
        if (src_policy) {
	        e = src_policy->eval(s->env, *s->auth.identity,
            s->src_object->get_instance().empty() ?
            rgw::IAM::s3GetObject :
            rgw::IAM::s3GetObjectVersion,
            obj_arn,
            princ_type);
        }
	if (e == Effect::Deny) {
	  return -EACCES;
	}
        if (!s->session_policies.empty()) {
	  auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                                  s->src_object->get_instance().empty() ?
                                                  rgw::IAM::s3GetObject :
                                                  rgw::IAM::s3GetObjectVersion,
                                                  obj_arn);
        if (session_policy_res == Effect::Deny) {
            return -EACCES;
        }
        if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
          //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
          if ((session_policy_res != Effect::Allow || identity_policy_res != Effect::Allow) &&
              (session_policy_res != Effect::Allow || e != Effect::Allow)) {
            return -EACCES;
          }
        } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
          //Intersection of session policy and identity policy plus bucket policy
          if ((session_policy_res != Effect::Allow || identity_policy_res != Effect::Allow) && e != Effect::Allow) {
            return -EACCES;
          }
        } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
          if (session_policy_res != Effect::Allow || identity_policy_res != Effect::Allow) {
            return -EACCES;
          }
        }
      }
  if (identity_policy_res == Effect::Pass && e == Effect::Pass &&
		   !src_acl.verify_permission(this, *s->auth.identity, s->perm_mask,
					      RGW_PERM_READ)) { 
	  return -EACCES;
	}
      //remove src object tags as it may interfere with policy evaluation of destination obj
      if (has_s3_existing_tag || has_s3_resource_tag)
        rgw_iam_remove_objtags(this, s, s->src_object.get(), has_s3_existing_tag, has_s3_resource_tag);

      } else if (!src_acl.verify_permission(this, *s->auth.identity,
					       s->perm_mask,
					    RGW_PERM_READ)) {
	return -EACCES;
      }
    }
  }

  RGWAccessControlPolicy dest_bucket_policy;

  s->object->set_atomic();

  /* check dest bucket permissions */
  op_ret = read_bucket_policy(this, driver, s, s->bucket->get_info(),
			      s->bucket->get_attrs(),
                              dest_bucket_policy, s->bucket->get_key(), y);
  if (op_ret < 0) {
    return op_ret;
  }
  auto dest_iam_policy = get_iam_policy_from_attr(s->cct, s->bucket->get_attrs(), s->bucket->get_tenant());
  /* admin request overrides permission checks */
  if (! s->auth.identity->is_admin_of(dest_policy.get_owner().id)){
    if (dest_iam_policy != boost::none || ! s->iam_user_policies.empty() || !s->session_policies.empty()) {
      //Add destination bucket tags for authorization
      auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, dest_iam_policy, s->iam_user_policies, s->session_policies);
      if (has_s3_resource_tag)
        rgw_iam_add_buckettags(this, s, s->bucket.get());

      rgw_add_to_iam_environment(s->env, "s3:x-amz-copy-source", copy_source);
      if (md_directive)
	rgw_add_to_iam_environment(s->env, "s3:x-amz-metadata-directive",
				   *md_directive);

      ARN obj_arn(s->object->get_obj());
      auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies,
                                                                  s->env,
                                                                  rgw::IAM::s3PutObject,
                                                                  obj_arn);
      if (identity_policy_res == Effect::Deny) {
        return -EACCES;
      }
      auto e = Effect::Pass;
      rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
      if (dest_iam_policy) {
        e = dest_iam_policy->eval(s->env, *s->auth.identity,
                                      rgw::IAM::s3PutObject,
                                      obj_arn,
                                      princ_type);
      }
      if (e == Effect::Deny) {
        return -EACCES;
      }
      if (!s->session_policies.empty()) {
        auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
								    rgw::IAM::s3PutObject, obj_arn);
        if (session_policy_res == Effect::Deny) {
            return false;
        }
        if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
          //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
          if ((session_policy_res != Effect::Allow || identity_policy_res != Effect::Allow) &&
              (session_policy_res != Effect::Allow || e == Effect::Allow)) {
            return -EACCES;
          }
        } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
          //Intersection of session policy and identity policy plus bucket policy
          if ((session_policy_res != Effect::Allow || identity_policy_res != Effect::Allow) && e != Effect::Allow) {
            return -EACCES;
          }
        } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
          if (session_policy_res != Effect::Allow || identity_policy_res != Effect::Allow) {
            return -EACCES;
          }
        }
      }
      if (identity_policy_res == Effect::Pass && e == Effect::Pass &&
                 ! dest_bucket_policy.verify_permission(this,
                                                        *s->auth.identity,
                                                        s->perm_mask,
                                                        RGW_PERM_WRITE)){
        return -EACCES;
      }
    } else if (! dest_bucket_policy.verify_permission(this, *s->auth.identity, s->perm_mask,
                                                      RGW_PERM_WRITE)) {
      return -EACCES;
    }

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

  op_ret = rgw_get_request_metadata(this, s->cct, s->info, attrs);
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

  if (ofs - last_ofs <
      static_cast<off_t>(s->cct->_conf->rgw_copy_obj_progress_every_bytes)) {
    return;
  }

  send_partial_response(ofs);

  last_ofs = ofs;
}

void RGWCopyObj::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWCopyObj::execute(optional_yield y)
{
  if (init_common() < 0)
    return;

  // make reservation for notification if needed
  std::unique_ptr<rgw::sal::Notification> res
				   = driver->get_notification(
				     s->object.get(), s->src_object.get(),
				     s, rgw::notify::ObjectCreatedCopy, y);
  op_ret = res->publish_reserve(this);
  if (op_ret < 0) {
    return;
  }

  if ( ! version_id.empty()) {
    s->object->set_instance(version_id);
  } else if (s->bucket->versioning_enabled()) {
    s->object->gen_rand_obj_instance_name();
  }

  s->src_object->set_atomic();
  s->object->set_atomic();

  encode_delete_at_attr(delete_at, attrs);

  if (obj_retention) {
    bufferlist obj_retention_bl;
    obj_retention->encode(obj_retention_bl);
    emplace_attr(RGW_ATTR_OBJECT_RETENTION, std::move(obj_retention_bl));
  }
  if (obj_legal_hold) {
    bufferlist obj_legal_hold_bl;
    obj_legal_hold->encode(obj_legal_hold_bl);
    emplace_attr(RGW_ATTR_OBJECT_LEGAL_HOLD, std::move(obj_legal_hold_bl));
  }

  uint64_t obj_size = 0;
  {
    // get src object size (cached in obj_ctx from verify_permission())
    RGWObjState* astate = nullptr;
    op_ret = s->src_object->get_obj_state(this, &astate, s->yield, true);
    if (op_ret < 0) {
      return;
    }

    /* Check if the src object is cloud-tiered */
    bufferlist bl;
    if (astate->get_attr(RGW_ATTR_MANIFEST, bl)) {
      RGWObjManifest m;
      try{
        decode(m, bl);
        if (m.get_tier_type() == "cloud-s3") {
          op_ret = -ERR_INVALID_OBJECT_STATE;
          s->err.message = "This object was transitioned to cloud-s3";
          ldpp_dout(this, 4) << "Cannot copy cloud tiered object. Failing with "
                         << op_ret << dendl;
          return;
        }
      } catch (const buffer::end_of_buffer&) {
        // ignore empty manifest; it's not cloud-tiered
      } catch (const std::exception& e) {
        ldpp_dout(this, 1) << "WARNING: failed to decode object manifest for "
            << *s->object << ": " << e.what() << dendl;
      }
    }

    obj_size = astate->size;
  
    if (!s->system_request) { // no quota enforcement for system requests
      if (astate->accounted_size > static_cast<size_t>(s->cct->_conf->rgw_max_put_size)) {
        op_ret = -ERR_TOO_LARGE;
        return;
      }
      // enforce quota against the destination bucket owner
      op_ret = s->bucket->check_quota(this, quota, astate->accounted_size, y);
      if (op_ret < 0) {
        return;
      }
    }
  }

  bool high_precision_time = (s->system_request);

  /* Handle object versioning of Swift API. In case of copying to remote this
   * should fail gently (op_ret == 0) as the dst_obj will not exist here. */
  op_ret = s->object->swift_versioning_copy(this, s->yield);
  if (op_ret < 0) {
    return;
  }

  op_ret = s->src_object->copy_object(s->user.get(),
	   &s->info,
	   source_zone,
	   s->object.get(),
	   s->bucket.get(),
	   src_bucket.get(),
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
	   attrs,
	   RGWObjCategory::Main,
	   olh_epoch,
	   delete_at,
	   (version_id.empty() ? NULL : &version_id),
	   &s->req_id, /* use req_id as tag */
	   &etag,
	   copy_obj_progress_cb, (void *)this,
	   this,
	   s->yield);

  if (op_ret < 0) {
    return;
  }

  // send request to notification manager
  int ret = res->publish_commit(this, obj_size, mtime, etag, s->object->get_instance());
  if (ret < 0) {
    ldpp_dout(this, 1) << "ERROR: publishing notification failed, with error: " << ret << dendl;
    // too late to rollback operation, hence op_ret is not set here
  }

  auto counters = rgw::op_counters::get(s);
  rgw::op_counters::inc(counters, l_rgw_op_copy_obj, 1);
  rgw::op_counters::inc(counters, l_rgw_op_copy_obj_b, obj_size);
  rgw::op_counters::tinc(counters, l_rgw_op_copy_obj_lat, s->time_elapsed());
}

int RGWGetACLs::verify_permission(optional_yield y)
{
  bool perm;
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
  if (!rgw::sal::Object::empty(s->object.get())) {
    auto iam_action = s->object->get_instance().empty() ?
      rgw::IAM::s3GetObjectAcl :
      rgw::IAM::s3GetObjectVersionAcl;
    if (has_s3_existing_tag || has_s3_resource_tag)
      rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);
    perm = verify_object_permission(this, s, iam_action);
  } else {
    if (!s->bucket_exists) {
      return -ERR_NO_SUCH_BUCKET;
    }
    if (has_s3_resource_tag)
      rgw_iam_add_buckettags(this, s);
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

void RGWGetACLs::execute(optional_yield y)
{
  stringstream ss;
  if (rgw::sal::Object::empty(s->object.get())) {
    rgw::s3::write_policy_xml(s->bucket_acl, ss);
  } else {
    rgw::s3::write_policy_xml(s->object_acl, ss);
  }
  acls = ss.str();
}



int RGWPutACLs::verify_permission(optional_yield y)
{
  bool perm;

  rgw_add_to_iam_environment(s->env, "s3:x-amz-acl", s->canned_acl);

  rgw_add_grant_to_iam_environment(s->env, s);
  if (!rgw::sal::Object::empty(s->object.get())) {
    auto iam_action = s->object->get_instance().empty() ? rgw::IAM::s3PutObjectAcl : rgw::IAM::s3PutObjectVersionAcl;
    op_ret = rgw_iam_add_objtags(this, s, true, true);
    perm = verify_object_permission(this, s, iam_action);
  } else {
    op_ret = rgw_iam_add_buckettags(this, s);
    perm = verify_bucket_permission(this, s, rgw::IAM::s3PutBucketAcl);
  }
  if (!perm)
    return -EACCES;

  return 0;
}

int RGWGetLC::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  bool perm;
  perm = verify_bucket_permission(this, s, rgw::IAM::s3GetLifecycleConfiguration);
  if (!perm)
    return -EACCES;

  return 0;
}

int RGWPutLC::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  bool perm;
  perm = verify_bucket_permission(this, s, rgw::IAM::s3PutLifecycleConfiguration);
  if (!perm)
    return -EACCES;

  return 0;
}

int RGWDeleteLC::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

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

void RGWPutACLs::execute(optional_yield y)
{
  const RGWAccessControlPolicy& existing_policy = \
    (rgw::sal::Object::empty(s->object.get()) ? s->bucket_acl : s->object_acl);

  const ACLOwner& existing_owner = existing_policy.get_owner();

  op_ret = get_params(y);
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

  RGWAccessControlPolicy new_policy;
  if (!s->canned_acl.empty() || s->has_acl_header) {
    op_ret = get_policy_from_state(existing_owner, new_policy);
  } else {
    op_ret = rgw::s3::parse_policy(this, y, driver, {data.c_str(), data.length()},
                                   new_policy, s->err.message);
  }
  if (op_ret < 0)
    return;

  if (!existing_owner.id.empty() &&
      existing_owner.id != new_policy.get_owner().id) {
    s->err.message = "Cannot modify ACL Owner";
    op_ret = -EPERM;
    return;
  }

  const RGWAccessControlList& req_acl = new_policy.get_acl();
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
    op_ret = -ERR_LIMIT_EXCEEDED;
    s->err.message = "The request is rejected, because the acl grants number you requested is larger than the maximum "
                     + std::to_string(max_num)
                     + " grants allowed in an acl.";
    return;
  }

  // forward bucket acl requests to meta master zone
  if ((rgw::sal::Object::empty(s->object.get()))) {
    op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                           &data, nullptr, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
  }

  if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    ldpp_dout(this, 15) << "Old AccessControlPolicy";
    rgw::s3::write_policy_xml(existing_policy, *_dout);
    *_dout << dendl;

    ldpp_dout(this, 15) << "New AccessControlPolicy:";
    rgw::s3::write_policy_xml(new_policy, *_dout);
    *_dout << dendl;
  }

  if (s->bucket_access_conf &&
      s->bucket_access_conf->block_public_acls() &&
      new_policy.is_public(this)) {
    op_ret = -EACCES;
    return;
  }

  bufferlist bl;
  new_policy.encode(bl);
  map<string, bufferlist> attrs;

  if (!rgw::sal::Object::empty(s->object.get())) {
    s->object->set_atomic();
    //if instance is empty, we should modify the latest object
    op_ret = s->object->modify_obj_attrs(RGW_ATTR_ACL, bl, s->yield, this);
  } else {
    map<string,bufferlist> attrs = s->bucket_attrs;
    attrs[RGW_ATTR_ACL] = bl;
    op_ret = s->bucket->merge_and_store_attrs(this, attrs, y);
  }
  if (op_ret == -ECANCELED) {
    op_ret = 0; /* lost a race, but it's ok because acls are immutable */
  }
}

void RGWPutLC::execute(optional_yield y)
{
  bufferlist bl;
  
  RGWLifecycleConfiguration_S3 config(s->cct);
  RGWXMLParser parser;
  RGWLifecycleConfiguration_S3 new_config(s->cct);

  // amazon says that Content-MD5 is required for this op specifically, but MD5
  // is not a security primitive and FIPS mode makes it difficult to use. if the
  // client provides the header we'll try to verify its checksum, but the header
  // itself is no longer required
  std::optional<std::string> content_md5_bin;

  content_md5 = s->info.env->get("HTTP_CONTENT_MD5");
  if (content_md5 != nullptr) {
    try {
      content_md5_bin = rgw::from_base64(std::string_view(content_md5));
    } catch (...) {
      s->err.message = "Request header Content-MD5 contains character "
                       "that is not base64 encoded.";
      ldpp_dout(this, 5) << s->err.message << dendl;
      op_ret = -ERR_BAD_DIGEST;
      return;
    }
  }

  if (!parser.init()) {
    op_ret = -EINVAL;
    return;
  }

  op_ret = get_params(y);
  if (op_ret < 0)
    return;

  char* buf = data.c_str();
  ldpp_dout(this, 15) << "read len=" << data.length() << " data=" << (buf ? buf : "") << dendl;

  if (content_md5_bin) {
    MD5 data_hash;
    // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
    data_hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
    unsigned char data_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
    data_hash.Update(reinterpret_cast<const unsigned char*>(buf), data.length());
    data_hash.Final(data_hash_res);

    if (memcmp(data_hash_res, content_md5_bin->c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) {
      op_ret = -ERR_BAD_DIGEST;
      s->err.message = "The Content-MD5 you specified did not match what we received.";
      ldpp_dout(this, 5) << s->err.message
                       << " Specified content md5: " << content_md5
                       << ", calculated content md5: " << data_hash_res
                       << dendl;
      return;
    }
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

  op_ret = config.rebuild(new_config);
  if (op_ret < 0)
    return;

  if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    XMLFormatter xf;
    new_config.dump_xml(&xf);
    stringstream ss;
    xf.flush(ss);
    ldpp_dout(this, 15) << "New LifecycleConfiguration:" << ss.str() << dendl;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = driver->get_rgwlc()->set_bucket_config(s->bucket.get(), s->bucket_attrs, &new_config);
  if (op_ret < 0) {
    return;
  }
  return;
}

void RGWDeleteLC::execute(optional_yield y)
{
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = driver->get_rgwlc()->remove_bucket_config(s->bucket.get(), s->bucket_attrs);
  if (op_ret < 0) {
    return;
  }
  return;
}

int RGWGetCORS::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketCORS);
}

void RGWGetCORS::execute(optional_yield y)
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

int RGWPutCORS::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketCORS);
}

void RGWPutCORS::execute(optional_yield y)
{
  rgw_raw_obj obj;

  op_ret = get_params(y);
  if (op_ret < 0)
    return;

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &in_data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this] {
      rgw::sal::Attrs attrs(s->bucket_attrs);
      attrs[RGW_ATTR_CORS] = cors_bl;
      return s->bucket->merge_and_store_attrs(this, attrs, s->yield);
    }, y);
}

int RGWDeleteCORS::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  // No separate delete permission
  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketCORS);
}

void RGWDeleteCORS::execute(optional_yield y)
{
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this] {
      op_ret = read_bucket_cors();
      if (op_ret < 0)
	return op_ret;

      if (!cors_exist) {
	ldpp_dout(this, 2) << "No CORS configuration set yet for this bucket" << dendl;
	op_ret = -ENOENT;
	return op_ret;
      }

      rgw::sal::Attrs attrs(s->bucket_attrs);
      attrs.erase(RGW_ATTR_CORS);
      op_ret = s->bucket->merge_and_store_attrs(this, attrs, s->yield);
      if (op_ret < 0) {
	ldpp_dout(this, 0) << "RGWLC::RGWDeleteCORS() failed to set attrs on bucket=" << s->bucket->get_name()
			 << " returned err=" << op_ret << dendl;
      }
      return op_ret;
    }, y);
}

void RGWOptionsCORS::get_response_params(string& hdrs, string& exp_hdrs, unsigned *max_age) {
  get_cors_response_headers(this, rule, req_hdrs, hdrs, exp_hdrs, max_age);
}

int RGWOptionsCORS::validate_cors_request(RGWCORSConfiguration *cc) {
  rule = cc->host_name_rule(origin);
  if (!rule) {
    ldpp_dout(this, 10) << "There is no cors rule present for " << origin << dendl;
    return -ENOENT;
  }

  if (!validate_cors_rule_method(this, rule, req_meth)) {
    return -ENOENT;
  }

  if (!validate_cors_rule_header(this, rule, req_hdrs)) {
    return -ENOENT;
  }

  return 0;
}

void RGWOptionsCORS::execute(optional_yield y)
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

int RGWGetRequestPayment::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketRequestPayment);
}

void RGWGetRequestPayment::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetRequestPayment::execute(optional_yield y)
{
  requester_pays = s->bucket->get_info().requester_pays;
}

int RGWSetRequestPayment::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketRequestPayment);
}

void RGWSetRequestPayment::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWSetRequestPayment::execute(optional_yield y)
{

  op_ret = get_params(y);
  if (op_ret < 0)
    return;
  
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &in_data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  s->bucket->get_info().requester_pays = requester_pays;
  op_ret = s->bucket->put_info(this, false, real_time(), y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket->get_name()
		     << " returned err=" << op_ret << dendl;
    return;
  }
  s->bucket_attrs = s->bucket->get_attrs();
}

int RGWInitMultipart::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
  if (has_s3_existing_tag || has_s3_resource_tag)
    rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  // add server-side encryption headers
  rgw_iam_add_crypt_attrs(s->env, s->info.crypt_attribute_map);

  if (s->iam_policy || ! s->iam_user_policies.empty() || !s->session_policies.empty()) {
    auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                              rgw::IAM::s3PutObject,
                                              s->object->get_obj());
    if (identity_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect e = Effect::Pass;
    rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
    ARN obj_arn(s->object->get_obj());
    if (s->iam_policy) {
      e = s->iam_policy->eval(s->env, *s->auth.identity,
				 rgw::IAM::s3PutObject,
				 obj_arn,
         princ_type);
    }
    if (e == Effect::Deny) {
      return -EACCES;
    }

    if (!s->session_policies.empty()) {
      auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                              rgw::IAM::s3PutObject,
                                              s->object->get_obj());
      if (session_policy_res == Effect::Deny) {
          return -EACCES;
      }
      if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
        //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
            (session_policy_res == Effect::Allow && e == Effect::Allow)) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
        //Intersection of session policy and identity policy plus bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || e == Effect::Allow) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
        if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) {
          return 0;
        }
      }
      return -EACCES;
    }
    if (e == Effect::Allow || identity_policy_res == Effect::Allow) {
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

void RGWInitMultipart::execute(optional_yield y)
{
  multipart_trace = tracing::rgw::tracer.start_trace(tracing::rgw::MULTIPART, s->trace_enabled);
  bufferlist aclbl, tracebl;
  rgw::sal::Attrs attrs;

  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  if (rgw::sal::Object::empty(s->object.get()))
    return;

  if (multipart_trace) {
    tracing::encode(multipart_trace->GetContext(), tracebl);
    attrs[RGW_ATTR_TRACE] = tracebl;
  }

  policy.encode(aclbl);
  attrs[RGW_ATTR_ACL] = aclbl;

  populate_with_generic_attrs(s, attrs);

  /* select encryption mode */
  op_ret = prepare_encryption(attrs);
  if (op_ret != 0)
    return;

  op_ret = rgw_get_request_metadata(this, s->cct, s->info, attrs);
  if (op_ret < 0) {
    return;
  }

  std::unique_ptr<rgw::sal::MultipartUpload> upload;
  upload = s->bucket->get_multipart_upload(s->object->get_name(),
				       upload_id);
  op_ret = upload->init(this, s->yield, s->owner, s->dest_placement, attrs);

  if (op_ret == 0) {
    upload_id = upload->get_upload_id();
  }
  s->trace->SetAttribute(tracing::rgw::UPLOAD_ID, upload_id);
  multipart_trace->UpdateName(tracing::rgw::MULTIPART + upload_id);

}

int RGWCompleteMultipart::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
  if (has_s3_existing_tag || has_s3_resource_tag)
    rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  // add server-side encryption headers
  rgw_iam_add_crypt_attrs(s->env, s->info.crypt_attribute_map);

  if (s->iam_policy || ! s->iam_user_policies.empty() || ! s->session_policies.empty()) {
    auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                              rgw::IAM::s3PutObject,
                                              s->object->get_obj());
    if (identity_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect e = Effect::Pass;
    rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
    rgw::ARN obj_arn(s->object->get_obj());
    if (s->iam_policy) {
      e = s->iam_policy->eval(s->env, *s->auth.identity,
				 rgw::IAM::s3PutObject,
				 obj_arn,
         princ_type);
    }
    if (e == Effect::Deny) {
      return -EACCES;
    }

    if (!s->session_policies.empty()) {
      auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                              rgw::IAM::s3PutObject,
                                              s->object->get_obj());
      if (session_policy_res == Effect::Deny) {
          return -EACCES;
      }
      if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
        //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
            (session_policy_res == Effect::Allow && e == Effect::Allow)) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
        //Intersection of session policy and identity policy plus bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || e == Effect::Allow) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
        if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) {
          return 0;
        }
      }
      return -EACCES;
    }
    if (e == Effect::Allow || identity_policy_res == Effect::Allow) {
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

void RGWCompleteMultipart::execute(optional_yield y)
{
  RGWMultiCompleteUpload *parts;
  RGWMultiXMLParser parser;
  std::unique_ptr<rgw::sal::MultipartUpload> upload;
  uint64_t olh_epoch = 0;

  op_ret = get_params(y);
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
    // CompletedMultipartUpload is incorrect but some versions of some libraries use it, see PR #41700
    parts = static_cast<RGWMultiCompleteUpload *>(parser.find_first("CompletedMultipartUpload"));
  }

  if (!parts || parts->parts.empty()) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }


  if ((int)parts->parts.size() >
      s->cct->_conf->rgw_multipart_part_upload_limit) {
    op_ret = -ERANGE;
    return;
  }

  upload = s->bucket->get_multipart_upload(s->object->get_name(), upload_id);

  RGWCompressionInfo cs_info;
  bool compressed = false;
  uint64_t accounted_size = 0;

  list<rgw_obj_index_key> remove_objs; /* objects to be removed from index listing */

  meta_obj = upload->get_meta_obj();
  meta_obj->set_in_extra_data(true);
  meta_obj->set_hash_source(s->object->get_name());

  /*take a cls lock on meta_obj to prevent racing completions (or retries)
    from deleting the parts*/
  int max_lock_secs_mp =
    s->cct->_conf.get_val<int64_t>("rgw_mp_lock_max_time");
  utime_t dur(max_lock_secs_mp, 0);

  serializer = meta_obj->get_serializer(this, "RGWCompleteMultipart");
  op_ret = serializer->try_lock(this, dur, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "failed to acquire lock" << dendl;
    if (op_ret == -ENOENT && check_previously_completed(parts)) {
      ldpp_dout(this, 1) << "NOTICE: This multipart completion is already completed" << dendl;
      op_ret = 0;
      return;
    }
    op_ret = -ERR_INTERNAL_ERROR;
    s->err.message = "This multipart completion is already in progress";
    return;
  }

  op_ret = meta_obj->get_obj_attrs(s->yield, this);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get obj attrs, obj=" << meta_obj
		     << " ret=" << op_ret << dendl;
    return;
  }
  s->trace->SetAttribute(tracing::rgw::UPLOAD_ID, upload_id);
  jspan_context trace_ctx(false, false);
  extract_span_context(meta_obj->get_attrs(), trace_ctx);
  multipart_trace = tracing::rgw::tracer.add_span(name(), trace_ctx);
  

  // make reservation for notification if needed
  res = driver->get_notification(meta_obj.get(), nullptr, s, rgw::notify::ObjectCreatedCompleteMultipartUpload, y,
                                 &s->object->get_name());
  op_ret = res->publish_reserve(this);
  if (op_ret < 0) {
    return;
  }

  target_obj = s->bucket->get_object(rgw_obj_key(s->object->get_name()));
  if (s->bucket->versioning_enabled()) {
    if (!version_id.empty()) {
      target_obj->set_instance(version_id);
    } else {
      target_obj->gen_rand_obj_instance_name();
      version_id = target_obj->get_instance();
    }
  }
  target_obj->set_attrs(meta_obj->get_attrs());

  op_ret = upload->complete(this, y, s->cct, parts->parts, remove_objs, accounted_size, compressed, cs_info, ofs, s->req_id, s->owner, olh_epoch, target_obj.get());
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: upload complete failed ret=" << op_ret << dendl;
    return;
  }

  upload_time = upload->get_mtime();
  int r = serializer->unlock();
  if (r < 0) {
    ldpp_dout(this, 0) << "WARNING: failed to unlock " << *serializer.get() << dendl;
  }
} // RGWCompleteMultipart::execute

bool RGWCompleteMultipart::check_previously_completed(const RGWMultiCompleteUpload* parts)
{
  // re-calculate the etag from the parts and compare to the existing object
  int ret = s->object->get_obj_attrs(s->yield, this);
  if (ret < 0) {
    ldpp_dout(this, 0) << __func__ << "() ERROR: get_obj_attrs() returned ret=" << ret << dendl;
    return false;
  }
  rgw::sal::Attrs sattrs = s->object->get_attrs();
  string oetag = sattrs[RGW_ATTR_ETAG].to_str();

  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  for (const auto& [index, part] : parts->parts) {
    std::string partetag = rgw_string_unquote(part);
    char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
    hex_to_buf(partetag.c_str(), petag, CEPH_CRYPTO_MD5_DIGESTSIZE);
    hash.Update((const unsigned char *)petag, sizeof(petag));
    ldpp_dout(this, 20) << __func__ << "() re-calculating multipart etag: part: "
                                   << index << ", etag: " << partetag << dendl;
  }

  unsigned char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  hash.Final(final_etag);
  buf_to_hex(final_etag, CEPH_CRYPTO_MD5_DIGESTSIZE, final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2], sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)parts->parts.size());

  if (oetag.compare(final_etag_str) != 0) {
    ldpp_dout(this, 1) << __func__ << "() NOTICE: etag mismatch: object etag:"
                                  << oetag << ", re-calculated etag:" << final_etag_str << dendl;
    return false;
  }
  ldpp_dout(this, 5) << __func__ << "() object etag and re-calculated etag match, etag: " << oetag << dendl;
  return true;
}

void RGWCompleteMultipart::complete()
{
  /* release exclusive lock iff not already */
  if (unlikely(serializer.get() && serializer->is_locked())) {
    int r = serializer->unlock();
    if (r < 0) {
      ldpp_dout(this, 0) << "WARNING: failed to unlock " << *serializer.get() << dendl;
    }
  }

  if (op_ret >= 0 && target_obj.get() != nullptr) {
    s->object->set_attrs(target_obj->get_attrs());
    etag = s->object->get_attrs()[RGW_ATTR_ETAG].to_str();
    // send request to notification manager
    if (res.get() != nullptr) {
      int ret = res->publish_commit(this, ofs, upload_time, etag, target_obj->get_instance());
      if (ret < 0) {
        ldpp_dout(this, 1) << "ERROR: publishing notification failed, with error: " << ret << dendl;
        // too late to rollback operation, hence op_ret is not set here
      }
    } else {
      ldpp_dout(this, 1) << "ERROR: reservation is null" << dendl;
    }
  } else {
    ldpp_dout(this, 1) << "ERROR: either op_ret is negative (execute failed) or target_obj is null, op_ret: "
                       << op_ret << dendl;
  }

  // remove the upload meta object ; the meta object is not versioned
  // when the bucket is, as that would add an unneeded delete marker
  // moved to complete to prevent segmentation fault in publish commit
  if (meta_obj.get() != nullptr) {
    int ret = meta_obj->delete_object(this, null_yield, rgw::sal::FLAG_PREVENT_VERSIONING);
    if (ret >= 0) {
      /* serializer's exclusive lock is released */
      serializer->clear_locked();
    } else {
      ldpp_dout(this, 0) << "WARNING: failed to remove object " << meta_obj << ", ret: " << ret << dendl;
    }
  } else {
    ldpp_dout(this, 0) << "WARNING: meta_obj is null" << dendl;
  }

  res.reset();
  meta_obj.reset();
  target_obj.reset();

  send_response();
}

int RGWAbortMultipart::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
  if (has_s3_existing_tag || has_s3_resource_tag)
    rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  if (s->iam_policy || ! s->iam_user_policies.empty() || !s->session_policies.empty()) {
    auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                              rgw::IAM::s3AbortMultipartUpload,
                                              s->object->get_obj());
    if (identity_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect e = Effect::Pass;
    rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
    ARN obj_arn(s->object->get_obj());
    if (s->iam_policy) {
      e = s->iam_policy->eval(s->env, *s->auth.identity,
				 rgw::IAM::s3AbortMultipartUpload,
				 obj_arn, princ_type);
    }

    if (e == Effect::Deny) {
      return -EACCES;
    }

    if (!s->session_policies.empty()) {
      auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                              rgw::IAM::s3PutObject,
                                              s->object->get_obj());
      if (session_policy_res == Effect::Deny) {
          return -EACCES;
      }
      if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
        //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
            (session_policy_res == Effect::Allow && e == Effect::Allow)) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
        //Intersection of session policy and identity policy plus bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || e == Effect::Allow) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
        if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) {
          return 0;
        }
      }
      return -EACCES;
    }
    if (e == Effect::Allow || identity_policy_res == Effect::Allow) {
      return 0;
    }
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

void RGWAbortMultipart::execute(optional_yield y)
{
  op_ret = -EINVAL;
  string upload_id;
  upload_id = s->info.args.get("uploadId");
  std::unique_ptr<rgw::sal::Object> meta_obj;
  std::unique_ptr<rgw::sal::MultipartUpload> upload;

  if (upload_id.empty() || rgw::sal::Object::empty(s->object.get()))
    return;

  upload = s->bucket->get_multipart_upload(s->object->get_name(), upload_id);
  jspan_context trace_ctx(false, false);
  if (tracing::rgw::tracer.is_enabled()) {
    // read meta object attributes for trace info
    meta_obj = upload->get_meta_obj();
    meta_obj->set_in_extra_data(true);
    meta_obj->get_obj_attrs(s->yield, this);
    extract_span_context(meta_obj->get_attrs(), trace_ctx);
  }
  multipart_trace = tracing::rgw::tracer.add_span(name(), trace_ctx);

  op_ret = upload->abort(this, s->cct, y);
}

int RGWListMultipart::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
  if (has_s3_existing_tag || has_s3_resource_tag)
    rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  if (!verify_object_permission(this, s, rgw::IAM::s3ListMultipartUploadParts))
    return -EACCES;

  return 0;
}

void RGWListMultipart::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWListMultipart::execute(optional_yield y)
{
  op_ret = get_params(y);
  if (op_ret < 0)
    return;

  upload = s->bucket->get_multipart_upload(s->object->get_name(), upload_id);

  rgw::sal::Attrs attrs;
  op_ret = upload->get_info(this, s->yield, &placement, &attrs);
  /* decode policy */
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_ACL);
  if (iter != attrs.end()) {
    auto bliter = iter->second.cbegin();
    try {
      policy.decode(bliter);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
      op_ret = -EIO;
    }
  }
  if (op_ret < 0)
    return;

  op_ret = upload->list_parts(this, s->cct, max_parts, marker, NULL, &truncated, y);
}

int RGWListBucketMultiparts::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

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

void RGWListBucketMultiparts::execute(optional_yield y)
{
  op_ret = get_params(y);
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

  op_ret = s->bucket->list_multiparts(this, prefix, marker_meta,
				      delimiter, max_uploads, uploads,
				      &common_prefixes, &is_truncated, y);
  if (op_ret < 0) {
    return;
  }

  if (!uploads.empty()) {
    next_marker_key = uploads.back()->get_key();
    next_marker_upload_id = uploads.back()->get_upload_id();
  }
}

void RGWGetHealthCheck::execute(optional_yield y)
{
  if (!g_conf()->rgw_healthcheck_disabling_path.empty() &&
      (::access(g_conf()->rgw_healthcheck_disabling_path.c_str(), F_OK) == 0)) {
    /* Disabling path specified & existent in the filesystem. */
    op_ret = -ERR_SERVICE_UNAVAILABLE; /* 503 */
  } else {
    op_ret = 0; /* 200 OK */
  }
}

int RGWDeleteMultiObj::verify_permission(optional_yield y)
{
  int op_ret = get_params(y);
  if (op_ret) {
    return op_ret;
  }

  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
    if (has_s3_existing_tag || has_s3_resource_tag)
      rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  if (s->iam_policy || ! s->iam_user_policies.empty() || ! s->session_policies.empty()) {
    if (s->bucket->get_info().obj_lock_enabled() && bypass_governance_mode) {
      ARN bucket_arn(s->bucket->get_key());
      auto r = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                               rgw::IAM::s3BypassGovernanceRetention, ARN(s->bucket->get_key()));
      if (r == Effect::Deny) {
        bypass_perm = false;
      } else if (r == Effect::Pass && s->iam_policy) {
        r = s->iam_policy->eval(s->env, *s->auth.identity, rgw::IAM::s3BypassGovernanceRetention,
                                     bucket_arn);
        if (r == Effect::Deny) {
          bypass_perm = false;
        }
      } else if (r == Effect::Pass && !s->session_policies.empty()) {
        r = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                               rgw::IAM::s3BypassGovernanceRetention, ARN(s->bucket->get_key()));
        if (r == Effect::Deny) {
          bypass_perm = false;
        }
      }
    }

    bool not_versioned = rgw::sal::Object::empty(s->object.get()) || s->object->get_instance().empty();

    auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                              not_versioned ?
                                              rgw::IAM::s3DeleteObject :
                                              rgw::IAM::s3DeleteObjectVersion,
                                              ARN(s->bucket->get_key()));
    if (identity_policy_res == Effect::Deny) {
      return -EACCES;
    }

    rgw::IAM::Effect r = Effect::Pass;
    rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
    rgw::ARN bucket_arn(s->bucket->get_key());
    if (s->iam_policy) {
      r = s->iam_policy->eval(s->env, *s->auth.identity,
				 not_versioned ?
				 rgw::IAM::s3DeleteObject :
				 rgw::IAM::s3DeleteObjectVersion,
				 bucket_arn,
         princ_type);
    }
    if (r == Effect::Deny)
      return -EACCES;

    if (!s->session_policies.empty()) {
      auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                              not_versioned ?
                                              rgw::IAM::s3DeleteObject :
                                              rgw::IAM::s3DeleteObjectVersion,
                                              ARN(s->bucket->get_key()));
      if (session_policy_res == Effect::Deny) {
          return -EACCES;
      }
      if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
        //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
            (session_policy_res == Effect::Allow && r == Effect::Allow)) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
        //Intersection of session policy and identity policy plus bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || r == Effect::Allow) {
          return 0;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
        if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) {
          return 0;
        }
      }
      return -EACCES;
    }
    if (r == Effect::Allow || identity_policy_res == Effect::Allow)
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

void RGWDeleteMultiObj::write_ops_log_entry(rgw_log_entry& entry) const {
  int num_err = 0;
  int num_ok = 0;
  for (auto iter = ops_log_entries.begin();
       iter != ops_log_entries.end();
       ++iter) {
    if (iter->error) {
      num_err++;
    } else {
      num_ok++;
    }
  }
  entry.delete_multi_obj_meta.num_err = num_err;
  entry.delete_multi_obj_meta.num_ok = num_ok;
  entry.delete_multi_obj_meta.objects = std::move(ops_log_entries);
}

void RGWDeleteMultiObj::wait_flush(optional_yield y,
                                   boost::asio::deadline_timer *formatter_flush_cond,
		                   std::function<bool()> predicate)
{
  if (y && formatter_flush_cond) {
    auto yc = y.get_yield_context();
    while (!predicate()) {
      boost::system::error_code error;
      formatter_flush_cond->async_wait(yc[error]);
      rgw_flush_formatter(s, s->formatter);
    }
  }
}

void RGWDeleteMultiObj::handle_individual_object(const rgw_obj_key& o, optional_yield y,
                                                 boost::asio::deadline_timer *formatter_flush_cond)
{
  std::string version_id;
  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(o);
  if (s->iam_policy || ! s->iam_user_policies.empty() || !s->session_policies.empty()) {
    auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                                                 o.instance.empty() ?
                                                                 rgw::IAM::s3DeleteObject :
                                                                 rgw::IAM::s3DeleteObjectVersion,
                                                                 ARN(obj->get_obj()));
    if (identity_policy_res == Effect::Deny) {
      send_partial_response(o, false, "", -EACCES, formatter_flush_cond);
      return;
    }

    rgw::IAM::Effect e = Effect::Pass;
    rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
    if (s->iam_policy) {
      ARN obj_arn(obj->get_obj());
      e = s->iam_policy->eval(s->env,
                              *s->auth.identity,
                              o.instance.empty() ?
                              rgw::IAM::s3DeleteObject :
                              rgw::IAM::s3DeleteObjectVersion,
                              obj_arn,
                              princ_type);
    }
    if (e == Effect::Deny) {
      send_partial_response(o, false, "", -EACCES, formatter_flush_cond);
      return;
    }

    if (!s->session_policies.empty()) {
      auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                                                  o.instance.empty() ?
                                                                  rgw::IAM::s3DeleteObject :
                                                                  rgw::IAM::s3DeleteObjectVersion,
                                                                  ARN(obj->get_obj()));
      if (session_policy_res == Effect::Deny) {
        send_partial_response(o, false, "", -EACCES, formatter_flush_cond);
        return;
      }
      if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
        //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
        if ((session_policy_res != Effect::Allow || identity_policy_res != Effect::Allow) &&
            (session_policy_res != Effect::Allow || e != Effect::Allow)) {
          send_partial_response(o, false, "", -EACCES, formatter_flush_cond);
          return;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
        //Intersection of session policy and identity policy plus bucket policy
        if ((session_policy_res != Effect::Allow || identity_policy_res != Effect::Allow) && e != Effect::Allow) {
          send_partial_response(o, false, "", -EACCES, formatter_flush_cond);
          return;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
        if (session_policy_res != Effect::Allow || identity_policy_res != Effect::Allow) {
          send_partial_response(o, false, "", -EACCES, formatter_flush_cond);
          return;
        }
      }
      send_partial_response(o, false, "", -EACCES, formatter_flush_cond);
      return;
    }

    if ((identity_policy_res == Effect::Pass && e == Effect::Pass && !acl_allowed)) {
      send_partial_response(o, false, "", -EACCES, formatter_flush_cond);
      return;
    }
  }

  uint64_t obj_size = 0;
  std::string etag;

  if (!rgw::sal::Object::empty(obj.get())) {
    RGWObjState* astate = nullptr;
    bool check_obj_lock = obj->have_instance() && bucket->get_info().obj_lock_enabled();
    const auto ret = obj->get_obj_state(this, &astate, y, true);

    if (ret < 0) {
      if (ret == -ENOENT) {
        // object maybe delete_marker, skip check_obj_lock
        check_obj_lock = false;
      } else {
        // Something went wrong.
        send_partial_response(o, false, "", ret, formatter_flush_cond);
        return;
      }
    } else {
      obj_size = astate->size;
      etag = astate->attrset[RGW_ATTR_ETAG].to_str();
    }

    if (check_obj_lock) {
      ceph_assert(astate);
      int object_lock_response = verify_object_lock(this, astate->attrset, bypass_perm, bypass_governance_mode);
      if (object_lock_response != 0) {
        send_partial_response(o, false, "", object_lock_response, formatter_flush_cond);
        return;
      }
    }
  }

  // make reservation for notification if needed
  const auto versioned_object = s->bucket->versioning_enabled();
  const auto event_type = versioned_object && obj->get_instance().empty() ?
                          rgw::notify::ObjectRemovedDeleteMarkerCreated :
                          rgw::notify::ObjectRemovedDelete;
  std::unique_ptr<rgw::sal::Notification> res
          = driver->get_notification(obj.get(), s->src_object.get(), s, event_type, y);
  op_ret = res->publish_reserve(this);
  if (op_ret < 0) {
    send_partial_response(o, false, "", op_ret, formatter_flush_cond);
    return;
  }

  obj->set_atomic();

  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op();
  del_op->params.versioning_status = obj->get_bucket()->get_info().versioning_status();
  del_op->params.obj_owner = s->owner;
  del_op->params.bucket_owner = s->bucket_owner;
  del_op->params.marker_version_id = version_id;

  op_ret = del_op->delete_obj(this, y, rgw::sal::FLAG_LOG_OP);
  if (op_ret == -ENOENT) {
    op_ret = 0;
  }
  if (op_ret == 0) {
    // send request to notification manager
    int ret = res->publish_commit(this, obj_size, ceph::real_clock::now(), etag, version_id);
    if (ret < 0) {
      ldpp_dout(this, 1) << "ERROR: publishing notification failed, with error: " << ret << dendl;
      // too late to rollback operation, hence op_ret is not set here
    }
  }
  
  send_partial_response(o, del_op->result.delete_marker, del_op->result.version_id, op_ret, formatter_flush_cond);
}

void RGWDeleteMultiObj::execute(optional_yield y)
{
  RGWMultiDelDelete *multi_delete;
  vector<rgw_obj_key>::iterator iter;
  RGWMultiDelXMLParser parser;
  uint32_t aio_count = 0;
  const uint32_t max_aio = std::max<uint32_t>(1, s->cct->_conf->rgw_multi_obj_del_max_aio);
  char* buf;
  std::optional<boost::asio::deadline_timer> formatter_flush_cond;
  if (y) {
    formatter_flush_cond = std::make_optional<boost::asio::deadline_timer>(y.get_io_context());  
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

  if (s->bucket->get_info().mfa_enabled()) {
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
    rgw_obj_key obj_key = *iter;
    if (y) {
      wait_flush(y, &*formatter_flush_cond, [&aio_count, max_aio] {
        return aio_count < max_aio;
      });
      aio_count++;
      spawn::spawn(y.get_yield_context(), [this, &y, &aio_count, obj_key, &formatter_flush_cond] (spawn::yield_context yield) {
        handle_individual_object(obj_key, optional_yield { y.get_io_context(), yield }, &*formatter_flush_cond); 
        aio_count--;
      }); 
    } else {
      handle_individual_object(obj_key, y, nullptr);
    }
  }
  if (formatter_flush_cond) {
    wait_flush(y, &*formatter_flush_cond, [this, n=multi_delete->objects.size()] {
      return n == ops_log_entries.size();
    });
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
                                               ACLOwner& bucket_owner /* out */,
					       optional_yield y)
{
  RGWAccessControlPolicy bacl;
  int ret = read_bucket_policy(dpp, driver, s, binfo, battrs, bacl, binfo.bucket, y);
  if (ret < 0) {
    return false;
  }

  auto policy = get_iam_policy_from_attr(s->cct, battrs, binfo.bucket.tenant);

  bucket_owner = bacl.get_owner();

  /* We can use global user_acl because each BulkDelete request is allowed
   * to work on entities from a single account only. */
  return verify_bucket_permission(dpp, s, binfo.bucket, s->user_acl,
				  bacl, policy, s->iam_user_policies, s->session_policies, rgw::IAM::s3DeleteBucket);
}

bool RGWBulkDelete::Deleter::delete_single(const acct_path_t& path, optional_yield y)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ACLOwner bowner;
  RGWObjVersionTracker ot;

  int ret = driver->load_bucket(dpp, rgw_bucket(s->user->get_tenant(),
                                                path.bucket_name),
                                &bucket, y);
  if (ret < 0) {
    goto binfo_fail;
  }

  if (!verify_permission(bucket->get_info(), bucket->get_attrs(), bowner, y)) {
    ret = -EACCES;
    goto auth_fail;
  }

  if (!path.obj_key.empty()) { // object deletion
    ACLOwner bucket_owner;

    bucket_owner.id = bucket->get_info().owner;
    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(path.obj_key);
    obj->set_atomic();

    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op();
    del_op->params.versioning_status = obj->get_bucket()->get_info().versioning_status();
    del_op->params.obj_owner = bowner;
    del_op->params.bucket_owner = bucket_owner;

    ret = del_op->delete_obj(dpp, y, rgw::sal::FLAG_LOG_OP);
    if (ret < 0) {
      goto delop_fail;
    }
  } else { // bucket deletion
    if (!driver->is_meta_master()) {
      // apply bucket deletion on the master zone first
      req_info req = s->info;
      forward_req_info(dpp, s->cct, req, path.bucket_name);

      ret = rgw_forward_request_to_master(dpp, *s->penv.site, s->user->get_id(),
                                          nullptr, nullptr, req, y);
      if (ret < 0) {
        goto delop_fail;
      }
    }
    ret = bucket->remove(dpp, false, s->yield);
    if (ret < 0) {
      goto delop_fail;
    }
  }

  num_deleted++;
  return true;

binfo_fail:
    if (-ENOENT == ret) {
      ldpp_dout(dpp, 20) << "cannot find bucket = " << path.bucket_name << dendl;
      num_unfound++;
    } else {
      ldpp_dout(dpp, 20) << "cannot get bucket info, ret = " << ret << dendl;

      fail_desc_t failed_item = {
        .err  = ret,
        .path = path
      };
      failures.push_back(failed_item);
    }
    return false;

auth_fail:
    ldpp_dout(dpp, 20) << "wrong auth for " << path << dendl;
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
      ldpp_dout(dpp, 20) << "cannot find entry " << path << dendl;
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

bool RGWBulkDelete::Deleter::delete_chunk(const std::list<acct_path_t>& paths, optional_yield y)
{
  ldpp_dout(dpp, 20) << "in delete_chunk" << dendl;
  for (auto path : paths) {
    ldpp_dout(dpp, 20) << "bulk deleting path: " << path << dendl;
    delete_single(path, y);
  }

  return true;
}

int RGWBulkDelete::verify_permission(optional_yield y)
{
  return 0;
}

void RGWBulkDelete::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWBulkDelete::execute(optional_yield y)
{
  deleter = std::unique_ptr<Deleter>(new Deleter(this, driver, s));

  bool is_truncated = false;
  do {
    list<RGWBulkDelete::acct_path_t> items;

    int ret = get_data(items, &is_truncated);
    if (ret < 0) {
      return;
    }

    ret = deleter->delete_chunk(items, y);
  } while (!op_ret && is_truncated);

  return;
}


constexpr std::array<int, 2> RGWBulkUploadOp::terminal_errors;

int RGWBulkUploadOp::verify_permission(optional_yield y)
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if (! verify_user_permission_no_policy(this, s, RGW_PERM_WRITE)) {
    return -EACCES;
  }

  if (s->user->get_tenant() != s->bucket_tenant) {
    ldpp_dout(this, 10) << "user cannot create a bucket in a different tenant"
        << " (user_id.tenant=" << s->user->get_tenant()
        << " requested=" << s->bucket_tenant << ")" << dendl;
    return -EACCES;
  }

  if (s->user->get_max_buckets() < 0) {
    return -EPERM;
  }

  return 0;
}

void RGWBulkUploadOp::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

boost::optional<std::pair<std::string, rgw_obj_key>>
RGWBulkUploadOp::parse_path(const std::string_view& path)
{
  /* We need to skip all slashes at the beginning in order to preserve
   * compliance with Swift. */
  const size_t start_pos = path.find_first_not_of('/');

  if (std::string_view::npos != start_pos) {
    /* Separator is the first slash after the leading ones. */
    const size_t sep_pos = path.substr(start_pos).find('/');

    if (std::string_view::npos != sep_pos) {
      const auto bucket_name = path.substr(start_pos, sep_pos - start_pos);
      const auto obj_name = path.substr(sep_pos + 1);

      return std::make_pair(std::string(bucket_name),
                            rgw_obj_key(std::string(obj_name)));
    } else {
      /* It's guaranteed here that bucket name is at least one character
       * long and is different than slash. */
      return std::make_pair(std::string(path.substr(start_pos)),
                            rgw_obj_key());
    }
  }

  return none;
}

std::pair<std::string, std::string>
RGWBulkUploadOp::handle_upload_path(req_state *s)
{
  std::string bucket_path, file_prefix;
  if (! s->init_state.url_bucket.empty()) {
    file_prefix = bucket_path = s->init_state.url_bucket + "/";
    if (!rgw::sal::Object::empty(s->object.get())) {
      const std::string& object_name = s->object->get_name();

      /* As rgw_obj_key::empty() already verified emptiness of s->object->get_name(),
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

int RGWBulkUploadOp::handle_dir_verify_permission(optional_yield y)
{
  return check_user_max_buckets(this, *s->user, y);
}

static void forward_req_info(const DoutPrefixProvider *dpp, CephContext *cct, req_info& info, const std::string& bucket_name)
{
  /* the request of container or object level will contain bucket name.
   * only at account level need to append the bucket name */
  if (info.script_uri.find(bucket_name) != std::string::npos) {
    return;
  }

  ldpp_dout(dpp, 20) << "append the bucket: "<< bucket_name << " to req_info" << dendl;
  info.script_uri.append("/").append(bucket_name);
  info.request_uri_aws4 = info.request_uri = info.script_uri;
  info.effective_uri = "/" + bucket_name;
}

void RGWBulkUploadOp::init(rgw::sal::Driver* const driver,
                           req_state* const s,
                           RGWHandler* const h)
{
  RGWOp::init(driver, s, h);
}

int RGWBulkUploadOp::handle_dir(const std::string_view path, optional_yield y)
{
  ldpp_dout(this, 20) << "got directory=" << path << dendl;

  int ret = handle_dir_verify_permission(y);
  if (ret < 0) {
    return ret;
  }

  std::string bucket_name;
  rgw_obj_key object_junk;
  std::tie(bucket_name, object_junk) =  *parse_path(path);

  rgw_bucket new_bucket;
  new_bucket.tenant = s->bucket_tenant; /* ignored if bucket exists */
  new_bucket.name = bucket_name;

  // load the bucket
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = driver->load_bucket(this, new_bucket, &bucket, y);

  // return success if it exists
  if (ret != -ENOENT) {
    return ret;
  }
  ceph_assert(bucket); // creates handle even on ENOENT

  const auto& zonegroup = s->penv.site->get_zonegroup();

  rgw::sal::Bucket::CreateParams createparams;
  createparams.owner = s->user->get_id();
  createparams.zonegroup_id = zonegroup.id;
  createparams.placement_rule.storage_class = s->info.storage_class;
  op_ret = select_bucket_placement(this, zonegroup, s->user->get_info(),
                                   createparams.placement_rule);
  createparams.zone_placement = rgw::find_zone_placement(
      this, s->penv.site->get_zone_params(), createparams.placement_rule);

  {
    // create a default acl
    RGWAccessControlPolicy policy;
    policy.create_default(s->user->get_id(), s->user->get_display_name());
    ceph::bufferlist aclbl;
    policy.encode(aclbl);
    createparams.attrs[RGW_ATTR_ACL] = std::move(aclbl);
  }

  if (!driver->is_meta_master()) {
    // apply bucket creation on the master zone first
    bufferlist in_data;
    JSONParser jp;
    req_info req = s->info;
    forward_req_info(this, s->cct, req, bucket_name);

    ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                        &in_data, &jp, req, y);
    if (ret < 0) {
      return ret;
    }

    RGWBucketInfo master_info;
    JSONDecoder::decode_json("bucket_info", master_info, &jp);

    // update params with info from the master
    createparams.marker = master_info.bucket.marker;
    createparams.bucket_id = master_info.bucket.bucket_id;
    createparams.obj_lock_enabled = master_info.obj_lock_enabled();
    createparams.quota = master_info.quota;
    createparams.creation_time = master_info.creation_time;
  }

  return bucket->create(this, createparams, y);
}


bool RGWBulkUploadOp::handle_file_verify_permission(RGWBucketInfo& binfo,
						    const rgw_obj& obj,
                                                    std::map<std::string, ceph::bufferlist>& battrs,
                                                    ACLOwner& bucket_owner /* out */,
						    optional_yield y)
{
  RGWAccessControlPolicy bacl;
  op_ret = read_bucket_policy(this, driver, s, binfo, battrs, bacl, binfo.bucket, y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "cannot read_policy() for bucket" << dendl;
    return false;
  }

  auto policy = get_iam_policy_from_attr(s->cct, battrs, binfo.bucket.tenant);

  bucket_owner = bacl.get_owner();
  if (policy || ! s->iam_user_policies.empty() || !s->session_policies.empty()) {
    auto identity_policy_res = eval_identity_or_session_policies(this, s->iam_user_policies, s->env,
                                              rgw::IAM::s3PutObject, obj);
    if (identity_policy_res == Effect::Deny) {
      return false;
    }

    rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
    ARN obj_arn(obj);
    auto e = policy->eval(s->env, *s->auth.identity,
			  rgw::IAM::s3PutObject, obj_arn, princ_type);
    if (e == Effect::Deny) {
      return false;
    }
  
    if (!s->session_policies.empty()) {
      auto session_policy_res = eval_identity_or_session_policies(this, s->session_policies, s->env,
                                              rgw::IAM::s3PutObject, obj);
      if (session_policy_res == Effect::Deny) {
          return false;
      }
      if (princ_type == rgw::IAM::PolicyPrincipal::Role) {
        //Intersection of session policy and identity policy plus intersection of session policy and bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) ||
            (session_policy_res == Effect::Allow && e == Effect::Allow)) {
          return true;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Session) {
        //Intersection of session policy and identity policy plus bucket policy
        if ((session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) || e == Effect::Allow) {
          return true;
        }
      } else if (princ_type == rgw::IAM::PolicyPrincipal::Other) {// there was no match in the bucket policy
        if (session_policy_res == Effect::Allow && identity_policy_res == Effect::Allow) {
          return true;
        }
      }
      return false;
    }
    if (e == Effect::Allow || identity_policy_res == Effect::Allow) {
      return true;
    }
  }
    
  return verify_bucket_permission_no_policy(this, s, s->user_acl,
					    bacl, RGW_PERM_WRITE);
}

int RGWBulkUploadOp::handle_file(const std::string_view path,
                                 const size_t size,
                                 AlignedStreamGetter& body, optional_yield y)
{

  ldpp_dout(this, 20) << "got file=" << path << ", size=" << size << dendl;

  if (size > static_cast<size_t>(s->cct->_conf->rgw_max_put_size)) {
    op_ret = -ERR_TOO_LARGE;
    return op_ret;
  }

  std::string bucket_name;
  rgw_obj_key object;
  std::tie(bucket_name, object) = *parse_path(path);

  std::unique_ptr<rgw::sal::Bucket> bucket;
  ACLOwner bowner;

  op_ret = driver->load_bucket(this, rgw_bucket(s->user->get_tenant(),
                                                bucket_name),
                               &bucket, y);
  if (op_ret < 0) {
    if (op_ret == -ENOENT) {
      ldpp_dout(this, 20) << "non existent directory=" << bucket_name << dendl;
    }
    return op_ret;
  }

  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);

  if (! handle_file_verify_permission(bucket->get_info(),
				      obj->get_obj(),
				      bucket->get_attrs(), bowner, y)) {
    ldpp_dout(this, 20) << "object creation unauthorized" << dendl;
    op_ret = -EACCES;
    return op_ret;
  }

  op_ret = bucket->check_quota(this, quota, size, y);
  if (op_ret < 0) {
    return op_ret;
  }

  if (bucket->versioning_enabled()) {
    obj->gen_rand_obj_instance_name();
  }

  rgw_placement_rule dest_placement = s->dest_placement;
  dest_placement.inherit_from(bucket->get_placement_rule());

  std::unique_ptr<rgw::sal::Writer> processor;
  processor = driver->get_atomic_writer(this, s->yield, obj.get(),
				       bowner.id,
				       &s->dest_placement, 0, s->req_id);
  op_ret = processor->prepare(s->yield);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "cannot prepare processor due to ret=" << op_ret << dendl;
    return op_ret;
  }

  /* No filters by default. */
  rgw::sal::DataProcessor *filter = processor.get();

  const auto& compression_type = driver->get_compression_type(dest_placement);
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
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
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

  op_ret = bucket->check_quota(this, quota, size, y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "quota exceeded for path=" << path << dendl;
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
  policy.create_default(s->user->get_id(), s->user->get_display_name());
  ceph::bufferlist aclbl;
  policy.encode(aclbl);
  attrs.emplace(RGW_ATTR_ACL, std::move(aclbl));

  /* Create metadata: compression info. */
  if (compressor && compressor->is_compressed()) {
    ceph::bufferlist tmp;
    RGWCompressionInfo cs_info;
    assert(plugin != nullptr);
    // plugin exists when the compressor does
    // coverity[dereference:SUPPRESS]
    cs_info.compression_type = plugin->get_type_name();
    cs_info.orig_size = size;
    cs_info.compressor_message = compressor->get_compressor_message();
    cs_info.blocks = std::move(compressor->get_compression_blocks());
    encode(cs_info, tmp);
    attrs.emplace(RGW_ATTR_COMPRESSION, std::move(tmp));
  }

  /* Complete the transaction. */
  const req_context rctx{this, s->yield, s->trace.get()};
  op_ret = processor->complete(size, etag, nullptr, ceph::real_time(),
                              attrs, ceph::real_time() /* delete_at */,
                              nullptr, nullptr, nullptr, nullptr, nullptr,
                              rctx, rgw::sal::FLAG_LOG_OP);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "processor::complete returned op_ret=" << op_ret << dendl;
  }

  return op_ret;
}

void RGWBulkUploadOp::execute(optional_yield y)
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

          std::string filename;
	  if (bucket_path.empty())
	    filename = header->get_filename();
	  else
	    filename = file_prefix + std::string(header->get_filename());
	  auto body = AlignedStreamGetter(0, header->get_filesize(),
                                          rgw::tar::BLOCK_SIZE, *stream);
          op_ret = handle_file(filename,
                               header->get_filesize(),
                               body, y);
          if (! op_ret) {
            /* Only regular files counts. */
            num_created++;
          } else {
            failures.emplace_back(op_ret, std::string(filename));
          }
          break;
        }
        case rgw::tar::FileType::DIRECTORY: {
          ldpp_dout(this, 2) << "handling regular directory" << dendl;

          std::string_view dirname = bucket_path.empty() ? header->get_filename() : bucket_path;
          op_ret = handle_dir(dirname, y);
          if (op_ret < 0 && op_ret != -ERR_BUCKET_EXISTS) {
            failures.emplace_back(op_ret, std::string(dirname));
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
  const size_t aligned_length = length + (-length % alignment);
  ceph::bufferlist junk;

  DecoratedStreamGetter::get_exactly(aligned_length - position, junk);
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

int RGWGetAttrs::verify_permission(optional_yield y)
{
  s->object->set_atomic();

  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
    if (has_s3_existing_tag || has_s3_resource_tag)
      rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  auto iam_action = s->object->get_instance().empty() ?
    rgw::IAM::s3GetObject :
    rgw::IAM::s3GetObjectVersion;

  if (!verify_object_permission(this, s, iam_action)) {
    return -EACCES;
  }

  return 0;
}

void RGWGetAttrs::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetAttrs::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0)
    return;

  s->object->set_atomic();

  op_ret = s->object->get_obj_attrs(s->yield, this);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get obj attrs, obj=" << s->object
        << " ret=" << op_ret << dendl;
    return;
  }

  /* XXX RGWObject::get_obj_attrs() does not support filtering (yet) */
  auto& obj_attrs = s->object->get_attrs();
  if (attrs.size() != 0) {
    /* return only attrs requested */
    for (auto& att : attrs) {
      auto iter = obj_attrs.find(att.first);
      if (iter != obj_attrs.end()) {
	att.second = iter->second;
      }
    }
  } else {
    /* return all attrs */
    for  (auto& att : obj_attrs) {
      attrs.insert(get_attrs_t::value_type(att.first, att.second));;
    }
  }

  return;
 }

int RGWRMAttrs::verify_permission(optional_yield y)
{
  // This looks to be part of the RGW-NFS machinery and has no S3 or
  // Swift equivalent.
  bool perm;
  if (!rgw::sal::Object::empty(s->object.get())) {
    perm = verify_object_permission_no_policy(this, s, RGW_PERM_WRITE);
  } else {
    perm = verify_bucket_permission_no_policy(this, s, RGW_PERM_WRITE);
  }
  if (!perm)
    return -EACCES;

  return 0;
}

void RGWRMAttrs::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWRMAttrs::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0)
    return;

  s->object->set_atomic();

  op_ret = s->object->set_obj_attrs(this, nullptr, &attrs, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to delete obj attrs, obj=" << s->object
		       << " ret=" << op_ret << dendl;
  }
  return;
}

int RGWSetAttrs::verify_permission(optional_yield y)
{
  // This looks to be part of the RGW-NFS machinery and has no S3 or
  // Swift equivalent.
  bool perm;
  if (!rgw::sal::Object::empty(s->object.get())) {
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

void RGWSetAttrs::execute(optional_yield y)
{
  op_ret = get_params(y);
  if (op_ret < 0)
    return;

  if (!rgw::sal::Object::empty(s->object.get())) {
    rgw::sal::Attrs a(attrs);
    op_ret = s->object->set_obj_attrs(this, &a, nullptr, y);
  } else {
    op_ret = s->bucket->merge_and_store_attrs(this, attrs, y);
  }

} /* RGWSetAttrs::execute() */

void RGWGetObjLayout::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetObjLayout::execute(optional_yield y)
{
}


int RGWConfigBucketMetaSearch::verify_permission(optional_yield y)
{
  if (!s->auth.identity->is_owner_of(s->bucket_owner.id)) {
    return -EACCES;
  }

  return 0;
}

void RGWConfigBucketMetaSearch::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWConfigBucketMetaSearch::execute(optional_yield y)
{
  op_ret = get_params(y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "NOTICE: get_params() returned ret=" << op_ret << dendl;
    return;
  }

  s->bucket->get_info().mdsearch_config = mdsearch_config;

  op_ret = s->bucket->put_info(this, false, real_time(), y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket->get_name()
        << " returned err=" << op_ret << dendl;
    return;
  }
  s->bucket_attrs = s->bucket->get_attrs();
}

int RGWGetBucketMetaSearch::verify_permission(optional_yield y)
{
  if (!s->auth.identity->is_owner_of(s->bucket_owner.id)) {
    return -EACCES;
  }

  return 0;
}

void RGWGetBucketMetaSearch::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

int RGWDelBucketMetaSearch::verify_permission(optional_yield y)
{
  if (!s->auth.identity->is_owner_of(s->bucket_owner.id)) {
    return -EACCES;
  }

  return 0;
}

void RGWDelBucketMetaSearch::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDelBucketMetaSearch::execute(optional_yield y)
{
  s->bucket->get_info().mdsearch_config.clear();

  op_ret = s->bucket->put_info(this, false, real_time(), y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket->get_name()
        << " returned err=" << op_ret << dendl;
    return;
  }
  s->bucket_attrs = s->bucket->get_attrs();
}


RGWHandler::~RGWHandler()
{
}

int RGWHandler::init(rgw::sal::Driver* _driver,
                     req_state *_s,
                     rgw::io::BasicClient *cio)
{
  driver = _driver;
  s = _s;

  return 0;
}

int RGWHandler::do_init_permissions(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = rgw_build_bucket_policies(dpp, driver, s, y);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "init_permissions on " << s->bucket
        << " failed, ret=" << ret << dendl;
    return ret==-ENODATA ? -EACCES : ret;
  }

  rgw_build_iam_environment(driver, s);
  return ret;
}

int RGWHandler::do_read_permissions(RGWOp *op, bool only_bucket, optional_yield y)
{
  if (only_bucket) {
    /* already read bucket info */
    return 0;
  }
  int ret = rgw_build_object_policies(op, driver, s, op->prefetch_data(), y);

  if (ret < 0) {
    ldpp_dout(op, 10) << "read_permissions on " << s->bucket << ":"
		      << s->object << " only_bucket=" << only_bucket
		      << " ret=" << ret << dendl;
    if (ret == -ENODATA)
      ret = -EACCES;
    if (s->auth.identity->is_anonymous() && ret == -EACCES)
      ret = -EPERM;
  }

  return ret;
}

int RGWOp::error_handler(int err_no, string *error_content, optional_yield y) {
  return dialect_handler->error_handler(err_no, error_content, y);
}

int RGWHandler::error_handler(int err_no, string *error_content, optional_yield) {
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
  if (!op_ret) {
    /* A successful Put Bucket Policy should return a 204 on success */
    op_ret = STATUS_NO_CONTENT;
  }
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWPutBucketPolicy::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketPolicy)) {
    return -EACCES;
  }

  return 0;
}

int RGWPutBucketPolicy::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  // At some point when I have more time I want to make a version of
  // rgw_rest_read_all_input that doesn't use malloc.
  std::tie(op_ret, data) = read_all_input(s, max_size, false);

  // And throws exceptions.
  return op_ret;
}

void RGWPutBucketPolicy::execute(optional_yield y)
{
  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  try {
    const Policy p(
      s->cct, s->bucket_tenant, data,
      s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
    rgw::sal::Attrs attrs(s->bucket_attrs);
    if (s->bucket_access_conf &&
        s->bucket_access_conf->block_public_policy() &&
        rgw::IAM::is_public(p)) {
      op_ret = -EACCES;
      return;
    }

    op_ret = retry_raced_bucket_write(this, s->bucket.get(), [&p, this, &attrs] {
	attrs[RGW_ATTR_IAM_POLICY].clear();
	attrs[RGW_ATTR_IAM_POLICY].append(p.text);
	op_ret = s->bucket->merge_and_store_attrs(this, attrs, s->yield);
	return op_ret;
      }, y);
  } catch (rgw::IAM::PolicyParseException& e) {
    ldpp_dout(this, 5) << "failed to parse policy: " << e.what() << dendl;
    op_ret = -EINVAL;
    s->err.message = e.what();
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

int RGWGetBucketPolicy::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3GetBucketPolicy)) {
    return -EACCES;
  }

  return 0;
}

void RGWGetBucketPolicy::execute(optional_yield y)
{
  rgw::sal::Attrs attrs(s->bucket_attrs);
  auto aiter = attrs.find(RGW_ATTR_IAM_POLICY);
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

int RGWDeleteBucketPolicy::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3DeleteBucketPolicy)) {
    return -EACCES;
  }

  return 0;
}

void RGWDeleteBucketPolicy::execute(optional_yield y)
{
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this] {
      rgw::sal::Attrs attrs(s->bucket_attrs);
      attrs.erase(RGW_ATTR_IAM_POLICY);
      op_ret = s->bucket->merge_and_store_attrs(this, attrs, s->yield);
      return op_ret;
    }, y);
}

void RGWPutBucketObjectLock::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

int RGWPutBucketObjectLock::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketObjectLockConfiguration);
}

void RGWPutBucketObjectLock::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    s->err.message = "object lock configuration can't be set if bucket object lock not enabled";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_BUCKET_STATE;
    return;
  }

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    op_ret = -EINVAL;
    return;
  }
  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }
  if (!parser.parse(data.c_str(), data.length(), 1)) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("ObjectLockConfiguration", obj_lock, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }
  if (obj_lock.has_rule() && !obj_lock.retention_period_valid()) {
    s->err.message = "retention period must be a positive integer value";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_RETENTION_PERIOD;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << __func__ << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
    s->bucket->get_info().obj_lock = obj_lock;
    op_ret = s->bucket->put_info(this, false, real_time(), y);
    return op_ret;
  }, y);
  return;
}

void RGWGetBucketObjectLock::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

int RGWGetBucketObjectLock::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketObjectLockConfiguration);
}

void RGWGetBucketObjectLock::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    op_ret = -ERR_NO_SUCH_OBJECT_LOCK_CONFIGURATION;
    return;
  }
}

int RGWPutObjRetention::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
    if (has_s3_existing_tag || has_s3_resource_tag)
      rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  if (!verify_object_permission(this, s, rgw::IAM::s3PutObjectRetention)) {
    return -EACCES;
  }
  op_ret = get_params(y);
  if (op_ret) {
    return op_ret;
  }
  if (bypass_governance_mode) {
    bypass_perm = verify_object_permission(this, s, rgw::IAM::s3BypassGovernanceRetention);
  }
  return 0;
}

void RGWPutObjRetention::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPutObjRetention::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    s->err.message = "object retention can't be set if bucket object lock not configured";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_REQUEST;
    return;
  }

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    op_ret = -EINVAL;
    return;
  }

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("Retention", obj_retention, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  if (ceph::real_clock::to_time_t(obj_retention.get_retain_until_date()) < ceph_clock_now()) {
    s->err.message = "the retain-until date must be in the future";
    ldpp_dout(this, 0) << "ERROR: " << s->err.message << dendl;
    op_ret = -EINVAL;
    return;
  }
  bufferlist bl;
  obj_retention.encode(bl);

  //check old retention
  op_ret = s->object->get_obj_attrs(s->yield, this);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: get obj attr error"<< dendl;
    return;
  }
  rgw::sal::Attrs attrs = s->object->get_attrs();
  auto aiter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
  if (aiter != attrs.end()) {
    RGWObjectRetention old_obj_retention;
    try {
      decode(old_obj_retention, aiter->second);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "ERROR: failed to decode RGWObjectRetention" << dendl;
      op_ret = -EIO;
      return;
    }
    if (ceph::real_clock::to_time_t(obj_retention.get_retain_until_date()) < ceph::real_clock::to_time_t(old_obj_retention.get_retain_until_date())) {
      if (old_obj_retention.get_mode().compare("GOVERNANCE") != 0 || !bypass_perm || !bypass_governance_mode) {
	  s->err.message = "proposed retain-until date shortens an existing retention period and governance bypass check failed";
        op_ret = -EACCES;
        return;
      }
    } else if (old_obj_retention.get_mode() == obj_retention.get_mode()) {
      // ok if retention mode doesn't change
    } else if (obj_retention.get_mode() == "GOVERNANCE") {
      s->err.message = "can't change retention mode from COMPLIANCE to GOVERNANCE";
      op_ret = -EACCES;
      return;
    } else if (!bypass_perm || !bypass_governance_mode) {
      s->err.message = "can't change retention mode from GOVERNANCE without governance bypass";
      op_ret = -EACCES;
      return;
    }
  }

  op_ret = s->object->modify_obj_attrs(RGW_ATTR_OBJECT_RETENTION, bl, s->yield, this);

  return;
}

int RGWGetObjRetention::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
    if (has_s3_existing_tag || has_s3_resource_tag)
      rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  if (!verify_object_permission(this, s, rgw::IAM::s3GetObjectRetention)) {
    return -EACCES;
  }
  return 0;
}

void RGWGetObjRetention::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetObjRetention::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    s->err.message = "bucket object lock not configured";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_REQUEST;
    return;
  }
  op_ret = s->object->get_obj_attrs(s->yield, this);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get obj attrs, obj=" << s->object
                       << " ret=" << op_ret << dendl;
    return;
  }
  rgw::sal::Attrs attrs = s->object->get_attrs();
  auto aiter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
  if (aiter == attrs.end()) {
    op_ret = -ERR_NO_SUCH_OBJECT_LOCK_CONFIGURATION;
    return;
  }

  bufferlist::const_iterator iter{&aiter->second};
  try {
    obj_retention.decode(iter);
  } catch (const buffer::error& e) {
    ldpp_dout(this, 0) << __func__ <<  "decode object retention config failed" << dendl;
    op_ret = -EIO;
    return;
  }
  return;
}

int RGWPutObjLegalHold::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
    if (has_s3_existing_tag || has_s3_resource_tag)
      rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  if (!verify_object_permission(this, s, rgw::IAM::s3PutObjectLegalHold)) {
    return -EACCES;
  }
  return 0;
}

void RGWPutObjLegalHold::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWPutObjLegalHold::execute(optional_yield y) {
  if (!s->bucket->get_info().obj_lock_enabled()) {
    s->err.message = "object legal hold can't be set if bucket object lock not enabled";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_REQUEST;
    return;
  }

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    op_ret = -EINVAL;
    return;
  }

  op_ret = get_params(y);
  if (op_ret < 0)
    return;

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("LegalHold", obj_legal_hold, &parser, true);
  } catch (RGWXMLDecoder::err &err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }
  bufferlist bl;
  obj_legal_hold.encode(bl);
  //if instance is empty, we should modify the latest object
  op_ret = s->object->modify_obj_attrs(RGW_ATTR_OBJECT_LEGAL_HOLD, bl, s->yield, this);
  return;
}

int RGWGetObjLegalHold::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s);
    if (has_s3_existing_tag || has_s3_resource_tag)
      rgw_iam_add_objtags(this, s, has_s3_existing_tag, has_s3_resource_tag);

  if (!verify_object_permission(this, s, rgw::IAM::s3GetObjectLegalHold)) {
    return -EACCES;
  }
  return 0;
}

void RGWGetObjLegalHold::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetObjLegalHold::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    s->err.message = "bucket object lock not configured";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_REQUEST;
    return;
  }
  map<string, bufferlist> attrs;
  op_ret = s->object->get_obj_attrs(s->yield, this);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get obj attrs, obj=" << s->object
                       << " ret=" << op_ret << dendl;
    return;
  }
  auto aiter = s->object->get_attrs().find(RGW_ATTR_OBJECT_LEGAL_HOLD);
  if (aiter == s->object->get_attrs().end()) {
    op_ret = -ERR_NO_SUCH_OBJECT_LOCK_CONFIGURATION;
    return;
  }

  bufferlist::const_iterator iter{&aiter->second};
  try {
    obj_legal_hold.decode(iter);
  } catch (const buffer::error& e) {
    ldpp_dout(this, 0) << __func__ <<  "decode object legal hold config failed" << dendl;
    op_ret = -EIO;
    return;
  }
  return;
}

void RGWGetClusterStat::execute(optional_yield y)
{
  op_ret = driver->cluster_stat(stats_op);
}

int RGWGetBucketPolicyStatus::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3GetBucketPolicyStatus)) {
    return -EACCES;
  }

  return 0;
}

void RGWGetBucketPolicyStatus::execute(optional_yield y)
{
  isPublic = (s->iam_policy && rgw::IAM::is_public(*s->iam_policy)) || s->bucket_acl.is_public(this);
}

int RGWPutBucketPublicAccessBlock::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketPublicAccessBlock)) {
    return -EACCES;
  }

  return 0;
}

int RGWPutBucketPublicAccessBlock::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);
  return op_ret;
}

void RGWPutBucketPublicAccessBlock::execute(optional_yield y)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    op_ret = -EINVAL;
    return;
  }

  op_ret = get_params(y);
  if (op_ret < 0)
    return;

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    ldpp_dout(this, 0) << "ERROR: malformed XML" << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("PublicAccessBlockConfiguration", access_conf, &parser, true);
  } catch (RGWXMLDecoder::err &err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  bufferlist bl;
  access_conf.encode(bl);
  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, &bl] {
      rgw::sal::Attrs attrs(s->bucket_attrs);
      attrs[RGW_ATTR_PUBLIC_ACCESS] = bl;
      return s->bucket->merge_and_store_attrs(this, attrs, s->yield);
    }, y);

}

int RGWGetBucketPublicAccessBlock::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3GetBucketPublicAccessBlock)) {
    return -EACCES;
  }

  return 0;
}

void RGWGetBucketPublicAccessBlock::execute(optional_yield y)
{
  auto attrs = s->bucket_attrs;
  if (auto aiter = attrs.find(RGW_ATTR_PUBLIC_ACCESS);
      aiter == attrs.end()) {
    ldpp_dout(this, 0) << "can't find bucket IAM POLICY attr bucket_name = "
		       << s->bucket_name << dendl;
    // return the default;
    return;
  } else {
    bufferlist::const_iterator iter{&aiter->second};
    try {
      access_conf.decode(iter);
    } catch (const buffer::error& e) {
      ldpp_dout(this, 0) << __func__ <<  "decode access_conf failed" << dendl;
      op_ret = -EIO;
      return;
    }
  }
}


void RGWDeleteBucketPublicAccessBlock::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWDeleteBucketPublicAccessBlock::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketPublicAccessBlock)) {
    return -EACCES;
  }

  return 0;
}

void RGWDeleteBucketPublicAccessBlock::execute(optional_yield y)
{
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this] {
      rgw::sal::Attrs attrs(s->bucket_attrs);
      attrs.erase(RGW_ATTR_PUBLIC_ACCESS);
      op_ret = s->bucket->merge_and_store_attrs(this, attrs, s->yield);
      return op_ret;
    }, y);
}

int RGWPutBucketEncryption::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);
  return op_ret;
}

int RGWPutBucketEncryption::verify_permission(optional_yield y)
{
  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketEncryption)) {
    return -EACCES;
  }
  return 0;
}

void RGWPutBucketEncryption::execute(optional_yield y)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    op_ret = -EINVAL;
    return;
  }
  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }
  if (!parser.parse(data.c_str(), data.length(), 1)) {
    ldpp_dout(this, 0) << "ERROR: malformed XML" << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("ServerSideEncryptionConfiguration", bucket_encryption_conf, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         &data, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  bufferlist conf_bl;
  bucket_encryption_conf.encode(conf_bl);
  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y, &conf_bl] {
    rgw::sal::Attrs attrs = s->bucket->get_attrs();
    attrs[RGW_ATTR_BUCKET_ENCRYPTION_POLICY] = conf_bl;
    return s->bucket->merge_and_store_attrs(this, attrs, y);
  }, y);
}

int RGWGetBucketEncryption::verify_permission(optional_yield y)
{
  if (!verify_bucket_permission(this, s, rgw::IAM::s3GetBucketEncryption)) {
    return -EACCES;
  }
  return 0;
}

void RGWGetBucketEncryption::execute(optional_yield y)
{
  const auto& attrs = s->bucket_attrs;
  if (auto aiter = attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY);
      aiter == attrs.end()) {
    ldpp_dout(this, 0) << "can't find BUCKET ENCRYPTION attr for bucket_name = " << s->bucket_name << dendl;
    op_ret = -ENOENT;
    s->err.message = "The server side encryption configuration was not found";
    return;
  } else {
    bufferlist::const_iterator iter{&aiter->second};
    try {
      bucket_encryption_conf.decode(iter);
    } catch (const buffer::error& e) {
      ldpp_dout(this, 0) << __func__ <<  "decode bucket_encryption_conf failed" << dendl;
      op_ret = -EIO;
      return;
    }
  }
}

int RGWDeleteBucketEncryption::verify_permission(optional_yield y)
{
  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketEncryption)) {
    return -EACCES;
  }
  return 0;
}

void RGWDeleteBucketEncryption::execute(optional_yield y)
{
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
    rgw::sal::Attrs attrs = s->bucket->get_attrs();
    attrs.erase(RGW_ATTR_BUCKET_ENCRYPTION_POLICY);
    attrs.erase(RGW_ATTR_BUCKET_ENCRYPTION_KEY_ID);
    op_ret = s->bucket->merge_and_store_attrs(this, attrs, y);
    return op_ret;
  }, y);
}

void rgw_slo_entry::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("path", path, obj);
  JSONDecoder::decode_json("etag", etag, obj);
  JSONDecoder::decode_json("size_bytes", size_bytes, obj);
};

