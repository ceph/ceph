// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rgw/librgw_admin_user.h"
#include "rgw_admin_user.h"
#include "rgw_user.h"
#include "common/errno.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace rgw;

namespace rgw {

  extern RGWLibAdmin rgw_lib_admin;

}

extern "C" {

  int rgw_admin_create_user(librgw_admin_user_t librgw_admin_user, const char *uid,
			    const char *display_name,  const char *access_key, const char* secret_key,
			    const char *email, const char *caps,
			    const char *access, bool admin, bool system)
  {
    RGWUserAdminOpState user_op;
    rgw_user user_id;
    user_id.from_str(uid);
    user_op.set_user_id(user_id);
    user_op.set_display_name(display_name);
    user_op.user_email = email;
    user_op.user_email_specified=true;
    user_op.set_access_key(access_key);
    user_op.set_secret_key(secret_key);
    user_op.set_caps(caps);
    if (access) {
      uint32_t perm_mask = rgw_str_to_perm(access);
      user_op.set_perm(perm_mask);
    }
    user_op.set_admin(admin);
    user_op.set_system(system);

    RGWUser user;
    int ret = 0;
    ret = user.init(rgw_lib_admin.get_store(), user_op);
    if (ret < 0) {
      cerr << "user.init failed: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    std::string err_msg;
    ret = user.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create user: " << err_msg << std::endl;
      if (ret == -ERR_INVALID_TENANT_NAME)
	ret = -EINVAL;

      return -ret;
    }

    return 0;
  }

  int rgw_admin_user_info(librgw_admin_user_t librgw_admin_user, const char *uid, rgw_user_info* user_info)
  {
    RGWUserAdminOpState user_op;
    rgw_user user_id;
    user_id.from_str(uid);
    user_op.set_user_id(user_id);

    RGWUser user;
    int ret = 0;
    ret = user.init(rgw_lib_admin.get_store(), user_op);
    if (ret < 0) {
      cerr << "user.init failed: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    std::string err_msg;
    RGWUserInfo info;
    ret = user.info(info, &err_msg);
    if (ret < 0) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return -ret;
    }

    return 0;
  }

}
