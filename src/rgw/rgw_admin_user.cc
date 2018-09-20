// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

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

  /* transfer object */
  struct RGWLA_User
  {
    RGWUserInfo info;
    std::string uid;
    std::string display_name;
    std::string access_key;
    std::string secret_key;
    std::string caps;
    std::string access;

    void make_info(rgw_user_info* user_info) {
      user_info->priv = this;
      uid = info.user_id.to_str();
      user_info->uid = uid.c_str();
      user_info->display_name = info.display_name.c_str();
      // unsure if this is the intent
      auto ac = info.access_keys.begin();
      if (ac != info.access_keys.end()) {
	access_key = ac->second.id;
	secret_key = ac->second.key;
      }
      user_info->access_key = access_key.c_str();
      user_info->secret_key = secret_key.c_str();
      user_info->email = info.user_email.c_str();
      // TODO: finish
      user_info->caps = caps.c_str();
      user_info->access = access.c_str();

      user_info->admin = info.admin;
      user_info->system = info.system;
    }
  };

  int rgw_admin_user_info(librgw_admin_user_t librgw_admin_user,
			  const char *uid, rgw_user_info* user_info)
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
    RGWLA_User* la_user = new RGWLA_User();
    ret = user.info(la_user->info, &err_msg);
    if (ret < 0) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return -ret;
    }

    la_user->make_info(user_info);

    return 0;
  }

  void rgw_admin_user_release_info(rgw_user_info* user_info)
  {
    RGWLA_User* la_user = reinterpret_cast<RGWLA_User*>(user_info->priv);
    delete la_user;
  }

} /* extern "C" */
