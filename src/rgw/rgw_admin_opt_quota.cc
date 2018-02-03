#include "rgw_admin_opt_quota.h"

#include <common/errno.h>

#include "rgw_admin_common.h"

static void set_quota_info(RGWQuotaInfo& quota, RgwAdminCommand opt_cmd, int64_t max_size, int64_t max_objects,
                           bool have_max_size, bool have_max_objects)
{
  switch (opt_cmd) {
    case OPT_QUOTA_ENABLE:
    case OPT_GLOBAL_QUOTA_ENABLE:
      quota.enabled = true;

      // falling through on purpose

    case OPT_QUOTA_SET:
    case OPT_GLOBAL_QUOTA_SET:
      if (have_max_objects) {
        if (max_objects < 0) {
          quota.max_objects = -1;
        } else {
          quota.max_objects = max_objects;
        }
      }
      if (have_max_size) {
        if (max_size < 0) {
          quota.max_size = -1;
        } else {
          quota.max_size = rgw_rounded_kb(max_size) * 1024;
        }
      }
      break;
    case OPT_QUOTA_DISABLE:
    case OPT_GLOBAL_QUOTA_DISABLE:
      quota.enabled = false;
      break;
    default: break;
  }
}

static int set_bucket_quota(RGWRados *store, RgwAdminCommand opt_cmd,
                            const string& tenant_name, const string& bucket_name,
                            int64_t max_size, int64_t max_objects,
                            bool have_max_size, bool have_max_objects)
{
  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
  RGWObjectCtx obj_ctx(store);
  int r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, nullptr, &attrs);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  set_quota_info(bucket_info.quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  r = store->put_bucket_instance_info(bucket_info, false, real_time(), &attrs);
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

static int set_user_bucket_quota(RgwAdminCommand opt_cmd, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                                 bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.bucket_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_bucket_quota(user_info.bucket_quota);

  string err;
  int r = user.modify(op_state, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

static int set_user_quota(RgwAdminCommand opt_cmd, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                          bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.user_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_user_quota(user_info.user_quota);

  string err;
  int r = user.modify(op_state, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

int handle_opt_global_quota(string& realm_id, const string& realm_name, bool have_max_size, int64_t max_size,
                            bool have_max_objects, int64_t max_objects, RgwAdminCommand opt_cmd, const string& quota_scope,
                            RGWRados *store, Formatter *formatter)
{
  if (realm_id.empty()) {
    RGWRealm realm(g_ceph_context, store);
    if (!realm_name.empty()) {
      // look up realm_id for the given realm_name
      int ret = realm.read_id(realm_name, realm_id);
      if (ret < 0) {
        cerr << "ERROR: failed to read realm for " << realm_name
             << ": " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      // use default realm_id when none is given
      int ret = realm.read_default_id(realm_id);
      if (ret < 0 && ret != -ENOENT) { // on ENOENT, use empty realm_id
        cerr << "ERROR: failed to read default realm: "
             << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  RGWPeriodConfig period_config;
  int ret = period_config.read(store, realm_id);
  if (ret < 0 && ret != -ENOENT) {
    cerr << "ERROR: failed to read period config: "
         << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  formatter->open_object_section("period_config");
  if (quota_scope == "bucket") {
    set_quota_info(period_config.bucket_quota, opt_cmd,
                   max_size, max_objects,
                   have_max_size, have_max_objects);
    encode_json("bucket quota", period_config.bucket_quota, formatter);
  } else if (quota_scope == "user") {
    set_quota_info(period_config.user_quota, opt_cmd,
                   max_size, max_objects,
                   have_max_size, have_max_objects);
    encode_json("user quota", period_config.user_quota, formatter);
  } else if (quota_scope.empty() && opt_cmd == OPT_GLOBAL_QUOTA_GET) {
    // if no scope is given for GET, print both
    encode_json("bucket quota", period_config.bucket_quota, formatter);
    encode_json("user quota", period_config.user_quota, formatter);
  } else {
    cerr << "ERROR: invalid quota scope specification. Please specify "
        "either --quota-scope=bucket, or --quota-scope=user" << std::endl;
    return EINVAL;
  }
  formatter->close_section();

  if (opt_cmd != OPT_GLOBAL_QUOTA_GET) {
    // write the modified period config
    ret = period_config.write(store, realm_id);
    if (ret < 0) {
      cerr << "ERROR: failed to write period config: "
           << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    if (!realm_id.empty()) {
      cout << "Global quota changes saved. Use 'period update' to apply "
          "them to the staging period, and 'period commit' to commit the "
          "new period." << std::endl;
    } else {
      cout << "Global quota changes saved. They will take effect as "
          "the gateways are restarted." << std::endl;
    }
  }

  formatter->flush(cout);
  return 0;
}

int handle_opt_quota(const rgw_user& user_id, const string& bucket_name, const string& tenant, bool have_max_size,
                     int64_t max_size, bool have_max_objects, int64_t max_objects, RgwAdminCommand opt_cmd, const string& quota_scope,
                     RGWUser& user, RGWUserAdminOpState& user_op,
                     RGWRados *store) {
  if (bucket_name.empty() && user_id.empty()) {
    cerr << "ERROR: bucket name or uid is required for quota operation" << std::endl;
    return EINVAL;
  }

  if (!bucket_name.empty()) {
    if (!quota_scope.empty() && quota_scope != "bucket") {
      cerr << "ERROR: invalid quota scope specification." << std::endl;
      return EINVAL;
    }
    set_bucket_quota(store, opt_cmd, tenant, bucket_name,
                     max_size, max_objects, have_max_size, have_max_objects);
  } else if (!user_id.empty()) {
    if (quota_scope == "bucket") {
      return set_user_bucket_quota(opt_cmd, user, user_op, max_size, max_objects, have_max_size, have_max_objects);
    } else if (quota_scope == "user") {
      return set_user_quota(opt_cmd, user, user_op, max_size, max_objects, have_max_size, have_max_objects);
    } else {
      cerr << "ERROR: invalid quota scope specification. Please specify either --quota-scope=bucket, or --quota-scope=user" << std::endl;
      return EINVAL;
    }
  }
  return 0;
}