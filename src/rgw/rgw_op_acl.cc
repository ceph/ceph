// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <map>
#include <sstream>
#include <string>

#include "common/dout.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_bucket_logging.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_op_internal.h"
#include "rgw_process_env.h"
#include "rgw_sal.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int RGWGetACLs::verify_permission(optional_yield y)
{
  return rgw_verify_get_acl_permission(this, s);
}

void RGWGetACLs::execute(optional_yield y)
{
  std::stringstream ss;
  const RGWAccessControlPolicy& policy =
    rgw_acl_targets_object(s) ? s->object_acl : s->bucket_acl;

  rgw::s3::write_policy_xml(policy, ss);
  acls = ss.str();
}

int RGWPutACLs::verify_permission(optional_yield y)
{
  return rgw_verify_put_acl_permission(this, s, op_ret);
}

void RGWPutACLs::execute(optional_yield y)
{
  if (s->bucket_object_ownership == rgw::s3::ObjectOwnership::BucketOwnerEnforced) {
    s->err.message = "Cannot set ACLs when ObjectOwnership is BucketOwnerEnforced.";
    op_ret = -ERR_ACLS_NOT_SUPPORTED;
    return;
  }

  const bool targets_object = rgw_acl_targets_object(s);
  const RGWAccessControlPolicy& existing_policy =
    targets_object ? s->object_acl : s->bucket_acl;

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

  const char *buf = data.c_str();
  ldpp_dout(this, 15) << "read len=" << data.length() << " data=" << (buf ? buf : "") << dendl;

  if (!s->canned_acl.empty() && data.length() > 0) {
    op_ret = -EINVAL;
    return;
  }

  RGWAccessControlPolicy new_policy;
  const bool has_acl_input = !s->canned_acl.empty() || s->has_acl_header;
  if (has_acl_input) {
    op_ret = get_policy_from_state(existing_owner, new_policy);
  }
  if (!has_acl_input) {
    op_ret = rgw::s3::parse_policy(this, y, driver, {data.c_str(), data.length()},
                                   new_policy, s->err.message);
  }
  if (op_ret < 0) {
    return;
  }

  // only allow acl owner to change if the requester views them as equivalent.
  // the requester may change between their user id and account id.
  if (!existing_owner.empty() &&
      existing_owner.id != new_policy.get_owner().id &&
      !(s->auth.identity->is_owner_of(existing_owner.id) &&
        s->auth.identity->is_owner_of(new_policy.get_owner().id))) {
    s->err.message = "Cannot modify ACL Owner";
    op_ret = -EPERM;
    return;
  }

  const RGWAccessControlList& req_acl = new_policy.get_acl();
  const std::multimap<std::string, ACLGrant>& req_grant_map = req_acl.get_grant_map();
  constexpr int acl_grants_default_max_num = 100;
  int max_num = s->cct->_conf->rgw_acl_grants_max_num;
  if (max_num < 0) {
    max_num = acl_grants_default_max_num;
  }

  const int grants_num = req_grant_map.size();
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
  if (!targets_object) {
    op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                           &data, nullptr, s->info, s->err, y);
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

  if (s->public_access_block.BlockPublicAcls &&
      new_policy.is_public(this)) {
    op_ret = -EACCES;
    return;
  }

  if (targets_object) {
    // in journal mode we log only object ACLs
    const auto etag = s->object->get_attrs()[RGW_ATTR_ETAG].to_str();
    op_ret = rgw::bucketlogging::log_record(driver,
        rgw::bucketlogging::LoggingType::Journal,
        s->object.get(),
        s,
        canonical_name(),
        etag,
        s->object->get_size(),
        this, y, false, false);
    if (op_ret < 0) {
      return;
    }
  }

  bufferlist bl;
  new_policy.encode(bl);

  if (targets_object) {
    s->object->set_atomic(true);
    // if instance is empty, we should modify the latest object
    op_ret = s->object->modify_obj_attrs(RGW_ATTR_ACL, bl, s->yield, this);
  }
  if (!targets_object) {
    std::map<std::string, bufferlist> attrs = s->bucket_attrs;
    attrs[RGW_ATTR_ACL] = bl;
    op_ret = s->bucket->merge_and_store_attrs(this, attrs, y);
  }
  if (op_ret == -ECANCELED) {
    op_ret = 0; /* lost a race, but it's ok because acls are immutable */
  }
}
