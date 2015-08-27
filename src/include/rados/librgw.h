// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_LIBRGW_H
#define CEPH_LIBRGW_H

#ifdef __cplusplus
extern "C" {
#endif

class CephContext;
typedef CephContext* librgw_t;
int librgw_create(librgw_t* rgw, const char* const id);
int librgw_acl_bin2xml(librgw_t rgw, const char* bin, int bin_len, char** xml);
void librgw_free_xml(librgw_t rgw, char* xml);
int librgw_acl_xml2bin(librgw_t rgw, const char *xml, char** bin,
		       int* bin_len);
void librgw_free_bin(librgw_t rgw, char* bin);
void librgw_shutdown(librgw_t rgw);

/* librgw external interface */
int librgw_init();
int librgw_stop();

/* User interface */
int get_userinfo_by_uid(const string& uid);
int get_user_acl();
int set_user_permissions();
int set_user_quota();
int get_user_quota();

/* buckets */
int get_user_buckets_list();
int get_bucket_objects_list();
int create_bucket();
int delete_bucket();
int get_bucket_attributes();
int set_bucket_attributes();

/* objects */
int create_object ();
int delete_object();
int rgw_write();
int rgw_read();
int get_object_attributes();
int set_object_attributes();

#ifdef __cplusplus
}
#endif

#endif /* CEPH_LIBRGW_H */
