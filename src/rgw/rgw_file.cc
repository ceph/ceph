// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/rgw_file.h"
#include "include/rados/librgw.h"

#include "rgw_lib.h"
#include "rgw_rados.h"
#include "rgw_resolve.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_rest_user.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_lib.h"
#include "rgw_auth_s3.h"
#include "rgw_lib.h"


extern RGWLib librgw;

bool is_root(const string& uri)
{
  return (uri == "");
}

bool is_bucket(const string& uri)
{
  /* XXX */
  int pos = uri.find('/');
  return (pos < 0);
}

/*
  get generate rgw_file_handle
*/
int rgw_get_handle(const char* uri, struct rgw_file_handle* handle)
{
  handle->handle = librgw.get_handle(uri);
  return 0;
}

/*
  check rgw_file_handle
*/
int rgw_check_handle(const struct rgw_file_handle* handle)
{
  return librgw.check_handle(handle->handle);
}

int rgw_mount(const char* uid, const char* key, const char* _secret,
	      struct rgw_file_handle* handle)
{
  int rc;
  string uri(uid);
  uri += "\\";
  string secret(_secret);
  string auth_hdr;
  map<string, string> meta_map;
  map<string, string> sub_resources;

  rgw_create_s3_canonical_header("GET",
				 NULL, /* const char *content_md5 */
				 "text/html",
				 "",
				 meta_map,
				 uri.c_str(),
				 sub_resources,
				 auth_hdr);

  /* check key */
  rc = rgw_get_s3_header_digest(auth_hdr, key, secret);
  if (rc < 0 ) {
    return rc;
  }

  return rgw_get_handle(uri.c_str(), handle);
}

/*
  get filesystem attributes
*/
int rgw_statfs(const struct rgw_file_handle *parent_handle,
	       struct rgw_statvfs *vfs_st)
{
  memset(vfs_st, 0, sizeof(struct rgw_statvfs));
  return 0;
}

/*
  generic create
*/
int rgw_create(const struct rgw_file_handle *parent_handle,
	       const char *name, mode_t mode, struct stat *st,
	       struct rgw_file_handle *handle)
{
  string uri;
  int rc;

  rc = librgw.get_uri(parent_handle->handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  uri += "\\";
  uri += name;

  /* TODO: implement */

  return rgw_get_handle(uri.c_str(), handle);
}

/*
  create a new directory
*/
int rgw_mkdir(const struct rgw_file_handle *parent_handle,
	      const char *name, mode_t mode, struct stat *st,
	      struct rgw_file_handle *handle)
{
  string uri;
  int rc;

  rc = librgw.get_uri(parent_handle->handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  /* cannot create a bucket in a bucket */
  if (! is_root(uri)) {
    return EINVAL;
  }

  /* TODO: implement */

  return 0;
}

/*
  rename object
*/
int rgw_rename(const struct rgw_file_handle* parent_handle,
	       const char* old_name,
	       const char* new_name)
{
  return 0;
}

/*
  remove file or directory
*/
int rgw_unlink(const struct rgw_file_handle* parent_handle, const char* path)
{
  return 0;
}

/*
  lookup a directory or file
*/
int rgw_lookup(const struct rgw_file_handle *parent_handle, const char* path,
	       struct rgw_file_handle *handle)
{
  string uri;
  int rc;

  rc = librgw.get_uri(parent_handle->handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  #warning get_bucket and ?get_object? unimplemented
  /* TODO: implement */
  if (is_root(uri)) {
    //librgw.get_bucket(uri);
  } else if (0 /* is_bucket(uri) */) {
    /* get the object */
  } else { /* parent cannot be an object */
    return -1;
  }

  uri += "/";
  uri += path;

  /* find or create an handle for the object or bucket */
  handle->handle = librgw.get_handle(uri);
  return 0;
}

/*
   get unix attributes for object
*/
int rgw_getattr(const struct rgw_file_handle *handle, struct stat *st)
{
  string uri;
  int rc;

  rc = librgw.get_uri(handle->handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  return 0;
}

/*
  set unix attributes for object
*/
int rgw_setattr(const struct rgw_file_handle *handle, struct stat *st,
		uint32_t mask)
{
  /* XXX no-op */
  return 0;
}

/*
  read directory content
*/
int rgw_readdir(const struct rgw_file_handle *parent_handle, uint64_t *offset,
		rgw_readdir_cb rcb, void *cb_arg, int *eof)
{
  string uri;
  int rc;

  rc = librgw.get_uri(parent_handle->handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

#if 0 /* TODO: implement */
  if (is_root(uri)) {
    /* get the bucket list */
    return librgw.get_user_buckets_list();
  } else if (is_bucket(uri)) {
    /* get the object list */
    return librgw.get_bucket_objects_list();
  } else { /* parent cannot be an object */
    return -1;
  }
#endif

  /* XXXX */
  (void) rcb("test1", cb_arg, 1);
  (void) rcb("test2", cb_arg, 2);
  (void) rcb("test3", cb_arg, 3);

  *eof = true;

  return 0;
}
