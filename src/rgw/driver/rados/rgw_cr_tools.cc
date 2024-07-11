// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"

#include "rgw_cr_tools.h"
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_acl_s3.h"
#include "rgw_zone.h"

#include "services/svc_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

template<>
int RGWUserCreateCR::Request::_send_request(const DoutPrefixProvider *dpp)
{
  CephContext *cct = store->ctx();

  const int32_t default_max_buckets =
    cct->_conf.get_val<int64_t>("rgw_user_max_buckets");

  RGWUserAdminOpState op_state(store);

  auto& user = params.user;

  op_state.set_user_id(user);
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
    RGWQuota quota;

    if (cct->_conf->rgw_bucket_default_quota_max_objects >= 0) {
      quota.bucket_quota.max_objects = cct->_conf->rgw_bucket_default_quota_max_objects;
      quota.bucket_quota.enabled = true;
    }

    if (cct->_conf->rgw_bucket_default_quota_max_size >= 0) {
      quota.bucket_quota.max_size = cct->_conf->rgw_bucket_default_quota_max_size;
      quota.bucket_quota.enabled = true;
    }

    if (cct->_conf->rgw_user_default_quota_max_objects >= 0) {
      quota.user_quota.max_objects = cct->_conf->rgw_user_default_quota_max_objects;
      quota.user_quota.enabled = true;
    }

    if (cct->_conf->rgw_user_default_quota_max_size >= 0) {
      quota.user_quota.max_size = cct->_conf->rgw_user_default_quota_max_size;
      quota.user_quota.enabled = true;
    }

    if (quota.bucket_quota.enabled) {
      op_state.set_bucket_quota(quota.bucket_quota);
    }

    if (quota.user_quota.enabled) {
      op_state.set_user_quota(quota.user_quota);
    }
  }

  RGWNullFlusher flusher;
  return RGWUserAdminOp_User::create(dpp, store, op_state, flusher, null_yield);
}

template<>
int RGWGetUserInfoCR::Request::_send_request(const DoutPrefixProvider *dpp)
{
  return store->ctl()->user->get_info_by_uid(dpp, params.user, result.get(), null_yield);
}

template<>
int RGWGetBucketInfoCR::Request::_send_request(const DoutPrefixProvider *dpp)
{
  return store->load_bucket(dpp, rgw_bucket(params.tenant, params.bucket_name),
                            &result->bucket, null_yield);
}

template<>
int RGWBucketLifecycleConfigCR::Request::_send_request(const DoutPrefixProvider *dpp)
{
  CephContext *cct = store->ctx();

  RGWLC *lc = store->getRados()->get_lc();
  if (!lc) {
    lderr(cct) << "ERROR: lifecycle object is not initialized!" << dendl;
    return -EIO;
  }

  int ret = lc->set_bucket_config(params.bucket,
                                  params.bucket_attrs,
                                  &params.config);
  if (ret < 0) {
    lderr(cct) << "ERROR: failed to set lifecycle on bucke: " << cpp_strerror(-ret) << dendl;
    return -ret;
  }

  return 0;
}

template<>
int RGWBucketGetSyncPolicyHandlerCR::Request::_send_request(const DoutPrefixProvider *dpp)
{
  int r = store->ctl()->bucket->get_sync_policy_handler(params.zone,
                                                        params.bucket,
                                                        &result->policy_handler,
                                                        null_yield,
                                                        dpp);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "ERROR: " << __func__ << "(): get_sync_policy_handler() returned " << r << dendl;
    return  r;
  }

  return 0;
}
