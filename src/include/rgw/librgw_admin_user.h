// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * create rgw admin user
 *
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef LIB_RGW_ADMIN_USER_H
#define LIB_RGW_ADMIN_USER_H

#ifdef __cplusplus
extern "C" {
#endif

#define LIBRGW_ADMIN_USER_VER_MAJOR 1
#define LIBRGW_ADMIN_USER_VER_MINOR 0
#define LIBRGW_ADMIN_USER_VER_EXTRA 0

#define LIBRGW_ADMIN_USER_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)
#define LIBRGW_ADMIN_USER_VERSION_CODE LIBRGW_ADMIN_USER_VERSION(LIBRGW_ADMIN_USER_VER_MAJOR, LIBRGW_ADMIN_USER_VER_MINOR, LIBRGW_ADMIN_USER_VER_EXTRA)

typedef void* librgw_admin_user_t;
int librgw_admin_user_create(librgw_admin_user_t *rgw_admin_user, int argc, char **argv);
void librgw_admin_user_shutdown(librgw_admin_user_t rgw_admin_user);

struct rgw_user_info
{
  const char *uid;
  const char *display_name;
  const char *access_key;
  const char* secret_key;
  const char* email;
  const char *caps;
  const char *access;
  bool admin;
  bool system;
};

 /*
 * create a new rgw user
 */
int rgw_admin_create_user(librgw_admin_user_t rgw_admin_user, const char *uid,
			  const char *display_name,  const char *access_key, const char* secret_key,
			  const char *email, const char *caps,
			  const char *access, bool admin, bool system);

/*
 * get rgw user info
 */
int rgw_admin_user_info(librgw_admin_user_t rgw_admin_user,const char * uid, rgw_user_info* user_info);

#ifdef __cplusplus
}
#endif

#endif /* LIBRGW_ADMIN_USER */
