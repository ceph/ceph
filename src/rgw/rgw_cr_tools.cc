#include "rgw_cr_tools.h"
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_acl_s3.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

template<>
int RGWUserCreateCR::Request::_send_request()
{
  CephContext *cct = store->ctx();

  int32_t default_max_buckets = cct->_conf->rgw_user_max_buckets;

  RGWUserAdminOpState op_state;

  rgw_user uid(params.uid);

  uid.tenant = params.tenant_name;

  op_state.set_user_id(uid);
  op_state.set_display_name(params.display_name);
  op_state.set_user_email(params.email);
  op_state.set_caps(params.caps);
  op_state.set_access_key(params.access_key);
  op_state.set_secret_key(params.secret_key);

  if (!params.key_type.empty()) {
    int32_t key_type = KEY_TYPE_S3;
    if (params.key_type == "swift") {
      key_type = KEY_TYPE_SWIFT;
    }

    op_state.set_key_type(key_type);
  }

  op_state.set_max_buckets(params.max_buckets.value_or(default_max_buckets));
  op_state.set_suspension(params.suspended);
  op_state.set_system(params.system);
  op_state.set_exclusive(params.exclusive);

  if (params.generate_key) {
    op_state.set_generate_key();
  }


  if (params.apply_quota) {
    RGWQuotaInfo bucket_quota;
    RGWQuotaInfo user_quota;

    if (cct->_conf->rgw_bucket_default_quota_max_objects >= 0) {
      bucket_quota.max_objects = cct->_conf->rgw_bucket_default_quota_max_objects;
      bucket_quota.enabled = true;
    }

    if (cct->_conf->rgw_bucket_default_quota_max_size >= 0) {
      bucket_quota.max_size = cct->_conf->rgw_bucket_default_quota_max_size;
      bucket_quota.enabled = true;
    }

    if (cct->_conf->rgw_user_default_quota_max_objects >= 0) {
      user_quota.max_objects = cct->_conf->rgw_user_default_quota_max_objects;
      user_quota.enabled = true;
    }

    if (cct->_conf->rgw_user_default_quota_max_size >= 0) {
      user_quota.max_size = cct->_conf->rgw_user_default_quota_max_size;
      user_quota.enabled = true;
    }

    if (bucket_quota.enabled) {
      op_state.set_bucket_quota(bucket_quota);
    }

    if (user_quota.enabled) {
      op_state.set_user_quota(user_quota);
    }
  }

  RGWNullFlusher flusher;
  return RGWUserAdminOp_User::create(store, op_state, flusher);
}

template<>
int RGWGetUserInfoCR::Request::_send_request()
{
  rgw_user user(params.tenant, params.uid);
  return rgw_get_user_info_by_uid(store, user, result->user_info);
}

template<>
int RGWBucketCreateLocalCR::Request::_send_request()
{
  CephContext *cct = store->ctx();

  const auto& user_info = params.user_info.get();
  const auto& user = user_info->user_id;
  const auto& bucket_name = params.bucket_name;
  auto& placement_rule = params.placement_rule;

  const auto& zonegroup = store->get_zonegroup();

  if (!placement_rule.empty() &&
      !zonegroup.placement_targets.count(placement_rule)) {
    ldout(cct, 0) << "placement target (" << placement_rule << ")"
      << " doesn't exist in the placement targets of zonegroup"
      << " (" << store->get_zonegroup().api_name << ")" << dendl;
    return -ERR_INVALID_LOCATION_CONSTRAINT;
  }

  /* we need to make sure we read bucket info, it's not read before for this
   * specific request */
  RGWObjectCtx obj_ctx(store);
  RGWBucketInfo bucket_info;
  map<string, bufferlist> bucket_attrs;

  int ret = store->get_bucket_info(obj_ctx, user.tenant, bucket_name,
				  bucket_info, nullptr, &bucket_attrs);
  if (ret < 0 && ret != -ENOENT)
    return ret;
  bool bucket_exists = (ret != -ENOENT);

  RGWAccessControlPolicy old_policy(cct);
  ACLOwner bucket_owner;
  bucket_owner.set_id(user);
  bucket_owner.set_name(user_info->display_name);
  if (bucket_exists) {
    ret = rgw_op_get_bucket_policy_from_attr(cct, store, bucket_info,
                                             bucket_attrs, &old_policy);
    if (ret >= 0)  {
      if (old_policy.get_owner().get_id().compare(user) != 0) {
        return -EEXIST;
      }
    }
  }

  RGWBucketInfo master_info;
  rgw_bucket *pmaster_bucket = nullptr;
  uint32_t *pmaster_num_shards = nullptr;
  real_time creation_time;

  string zonegroup_id = store->get_zonegroup().get_id();

  if (bucket_exists) {
    string selected_placement_rule;
    rgw_bucket bucket;
    bucket.tenant = user.tenant;
    bucket.name = bucket_name;
    ret = store->select_bucket_placement(*user_info, zonegroup_id,
                                         placement_rule,
                                         &selected_placement_rule, nullptr);
    if (selected_placement_rule != bucket_info.placement_rule) {
      ldout(cct, 0) << "bucket already exists on a different placement rule: "
        << " selected_rule= " << selected_placement_rule
        << " existing_rule= " << bucket_info.placement_rule << dendl;
      return -EEXIST;
    }
  }

  /* Encode special metadata first as we're using std::map::emplace under
   * the hood. This method will add the new items only if the map doesn't
   * contain such keys yet. */
  RGWAccessControlPolicy_S3 policy(cct);
  policy.create_canned(bucket_owner, bucket_owner, string()); /* default private policy */
  bufferlist aclbl;
  policy.encode(aclbl);
  map<string, buffer::list> attrs;
  attrs.emplace(std::move(RGW_ATTR_ACL), std::move(aclbl));

  RGWQuotaInfo quota_info;
  const RGWQuotaInfo * pquota_info = nullptr;

  rgw_bucket bucket;
  bucket.tenant = user.tenant;
  bucket.name = bucket_name;

  RGWBucketInfo info;
  obj_version ep_objv;

  ret = store->create_bucket(*user_info, bucket, zonegroup_id,
                                placement_rule, bucket_info.swift_ver_location,
                                pquota_info, attrs,
                                info, nullptr, &ep_objv, creation_time,
                                pmaster_bucket, pmaster_num_shards, true);


  if (ret && ret != -EEXIST)
    return ret;

  bool existed = (ret == -EEXIST);

  if (existed) {
    if (info.owner != user) {
      ldout(cct, 20) << "NOTICE: bucket already exists under a different user (bucket=" << bucket << " user=" << user << " bucket_owner=" << info.owner << dendl;
      return -EEXIST;
    }
    bucket = info.bucket;
  }

  ret = rgw_link_bucket(store, user, bucket,
                        info.creation_time, false);
  if (ret && !existed && ret != -EEXIST) {
    /* if it exists (or previously existed), don't remove it! */
    int r = rgw_unlink_bucket(store, user, bucket.tenant, bucket.name);
    if (r < 0) {
      ldout(cct, 0) << "WARNING: failed to unlink bucket: ret=" << r << dendl;
    }
  } else if (ret == -EEXIST || (ret == 0 && existed)) {
    ret = -ERR_BUCKET_EXISTS;
  }

  if (ret < 0) {
    ldout(cct, 0) << "ERROR: bucket creation (bucket=" << bucket << ") return ret=" << ret << dendl;
  }

  return ret;
}
