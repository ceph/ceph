// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/rgw_file.h"

#include "rgw_lib.h"
#include "rgw_rados.h"
#include "rgw_resolve.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_rest_user.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_lib.h"
#include "rgw_auth_s3.h"
#include "rgw_user.h"
#include "rgw_bucket.h"

#include "rgw_file.h"

#define dout_subsys ceph_subsys_rgw

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

/* librgw */
extern "C" {

/*
 attach rgw namespace
*/
int rgw_mount(librgw_t rgw, const char* uid, const char* key,
	      const char* _secret, struct rgw_fs **rgw_fs)
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

  /* stash access data for "mount" */
  struct rgw_fs *new_fs =
    static_cast<struct rgw_fs*>(operator new (sizeof(struct rgw_fs)));
  new_fs->rgw = rgw;
  new_fs->user_id = strdup(uid);
  new_fs->access_key_id = strdup(key);
  new_fs->secret_access_key = strdup(_secret);

  /* stash the root */
  rc = rgw_get_handle("", &new_fs->root_fh);

  *rgw_fs = new_fs;

  return rc;
}

/*
 detach rgw namespace
*/
int rgw_umount(struct rgw_fs *rgw_fs)
{
  free(rgw_fs->user_id);
  free(rgw_fs->access_key_id);
  free(rgw_fs->secret_access_key);
  operator delete (rgw_fs);

  return 0;
}

/*
  get filesystem attributes
*/
int rgw_statfs(struct rgw_fs *rgw_fs,
	       const struct rgw_file_handle *parent_handle,
	       struct rgw_statvfs *vfs_st)
{
  memset(vfs_st, 0, sizeof(struct rgw_statvfs));
  return 0;
}

/*
  generic create
*/
int rgw_create(struct rgw_fs *rgw_fs,
	       const struct rgw_file_handle *parent_handle,
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
int rgw_mkdir(struct rgw_fs *rgw_fs,
	      const struct rgw_file_handle *parent_handle,
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
int rgw_rename(struct rgw_fs *rgw_fs,
	       const struct rgw_file_handle *olddir, const char* old_name,
	       const struct rgw_file_handle *newdir, const char* new_name)
{
  return 0;
}

/*
  remove file or directory
*/
int rgw_unlink(struct rgw_fs *rgw_fs,
	       const struct rgw_file_handle* parent_handle, const char* path)
{
  return 0;
}

/*
  lookup a directory or file
*/
int rgw_lookup(struct rgw_fs *rgw_fs,
	       const struct rgw_file_handle *parent_handle, const char* path,
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

  /* find or create a handle for the object or bucket */
  handle->handle = librgw.get_handle(uri);
  return 0;
}

/*
   get unix attributes for object
*/
int rgw_getattr(struct rgw_fs *rgw_fs,
		const struct rgw_file_handle *handle, struct stat *st)
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
int rgw_setattr(struct rgw_fs *rgw_fs,
		const struct rgw_file_handle *handle, struct stat *st,
		uint32_t mask)
{
  /* XXX no-op */
  return 0;
}

/*
   truncate file
*/
int rgw_truncate(struct rgw_fs *rgw_fs,
		 const struct rgw_file_handle *handle, uint64_t size)
{
  return 0;
}

/*
   open file
*/
int rgw_open(struct rgw_fs *rgw_fs,
	     const struct rgw_file_handle *handle, uint32_t flags)
{
  return 0;
}

/*
   close file
*/
int rgw_close(struct rgw_fs *rgw_fs,
	      const struct rgw_file_handle *handle, uint32_t flags)
{
  return 0;
}

int rgw_readdir(struct rgw_fs *rgw_fs,
		const struct rgw_file_handle *parent_handle, uint64_t *offset,
		rgw_readdir_cb rcb, void *cb_arg, bool *eof)
{
  string uri;
  int rc;

  rc = librgw.get_uri(parent_handle->handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);

  /* TODO:
   * deal with markers (continuation)
   * deal with authorization
   * consider non-default tenancy/user and bucket layouts
   */
  if (is_root(uri)) {
    /* for now, root always contains one user's bucket namespace */
    RGWListBucketsRequest req(cct, rgw_fs->user_id, rcb, cb_arg, offset);
    (void) librgw.get_fe()->execute_req(&req);
  } else {
    /*
     * bucket?
     */
    uri += "/";

    RGWListBucketRequest req(cct, rgw_fs->user_id, uri, rcb, cb_arg, offset);
    (void) librgw.get_fe()->execute_req(&req);

  }

  *eof = true; // XXX move into RGGWListBucket(s)Request

  return 0;
}

/*
   read data from file
*/
int rgw_read(struct rgw_fs *rgw_fs,
	     const struct rgw_file_handle *handle, uint64_t offset,
	     size_t length, void *buffer)
{
  return 0;
}

/*
   write data to file
*/
int rgw_write(struct rgw_fs *rgw_fs,
	      const struct rgw_file_handle *handle, uint64_t offset,
	      size_t length, void *buffer)
{
  return 0;
}

/*
   sync written data
*/
int rgw_fsync(struct rgw_fs *rgw_fs, const struct rgw_file_handle *handle)
{
  return 0;
}

} /* extern "C" */
