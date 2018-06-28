#include "rgw_cr_tools.h"
#include "rgw_user.h"

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

