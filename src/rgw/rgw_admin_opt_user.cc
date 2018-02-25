#include "rgw_admin_opt_user.h"

#include "rgw_bucket.h"

int handle_opt_user_create(const std::string& subuser, RGWUserAdminOpState& user_op, RGWUser& user)
{
  if (!user_op.has_existing_user()) {
    user_op.set_generate_key(); // generate a new key by default
  }
  std::string err_msg;
  int ret = user.add(user_op, &err_msg);
  if (ret < 0) {
    cerr << "could not create user: " << err_msg << std::endl;
    if (ret == -ERR_INVALID_TENANT_NAME)
      ret = -EINVAL;

    return -ret;
  }
  if (!subuser.empty()) {
    ret = user.subusers.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create subuser: " << err_msg << std::endl;
      return -ret;
    }
  }
  return 0;
}

int handle_opt_user_stats(bool sync_stats, const std::string& bucket_name, const std::string& tenant,
                          rgw_user& user_id, RGWRados *store, Formatter *formatter)
{
  if (sync_stats) {
    if (!bucket_name.empty()) {
      int ret = rgw_bucket_sync_user_stats(store, tenant, bucket_name);
      if (ret < 0) {
        cerr << "ERROR: could not sync bucket stats: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      int ret = rgw_user_sync_all_stats(store, user_id);
      if (ret < 0) {
        cerr << "ERROR: failed to sync user stats: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  if (user_id.empty()) {
    cerr << "ERROR: uid not specified" << std::endl;
    return EINVAL;
  }
  cls_user_header header;
  string user_str = user_id.to_str();
  int ret = store->cls_user_get_header(user_str, &header);
  if (ret < 0) {
    if (ret == -ENOENT) { /* in case of ENOENT */
      cerr << "User has not been initialized or user does not exist" << std::endl;
    } else {
      cerr << "ERROR: can't read user: " << cpp_strerror(ret) << std::endl;
    }
    return -ret;
  }

  encode_json("header", header, formatter);
  formatter->flush(cout);
  return 0;
}
