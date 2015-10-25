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
#include "rgw_os_lib.h"
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
  int rgw_mount(librgw_t rgw, const char *uid, const char *acc_key,
		const char *sec_key, struct rgw_fs **rgw_fs)
{
  int rc = 0;

  /* stash access data for "mount" */
  RGWLibFS* new_fs = new RGWLibFS(uid, acc_key, sec_key);
  assert(new_fs);

  rc = new_fs->authorize(librgw.get_store());
  if (rc != 0) {
    delete new_fs;
    return -EINVAL;
  }

  struct rgw_fs *fs = new_fs->get_fs();;
  fs->rgw = rgw;

  /* stash the root */
  rc = rgw_get_handle("", &fs->root_fh);
  if (rc != 0) {
    delete new_fs;
    return -EINVAL;
  }

  *rgw_fs = fs;

  return 0;
}

/*
 detach rgw namespace
*/
int rgw_umount(struct rgw_fs *rgw_fs)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  delete fs;
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
  int rc;
  string uri;

  rc = librgw.get_uri(parent_handle->handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);

  /* cannot create a bucket in a bucket */
  if (! is_root(uri)) {
    return EINVAL;
  }

  // fix this
  uri += "/";
  uri += name;
  RGWCreateBucketRequest req(cct, fs->get_user(), uri);
  rc = librgw.get_fe()->execute_req(&req);

  return rc;
}

/*
  rename object
*/
int rgw_rename(struct rgw_fs *rgw_fs,
	       const struct rgw_file_handle *olddir, const char* old_name,
	       const struct rgw_file_handle *newdir, const char* new_name)
{
  /* -ENOTSUP */
  return -EINVAL;
}

/*
  remove file or directory
*/
int rgw_unlink(struct rgw_fs *rgw_fs, const struct rgw_file_handle* parent,
	      const char* path)
{
  string uri;
  int rc = 0;

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);

  if (fs->is_root(rgw_fs)) {
    /* for now, root always contains one user's bucket namespace */
    uri = "/";
    uri += path;
    RGWDeleteBucketRequest req(cct, fs->get_user(), uri);
    rc = librgw.get_fe()->execute_req(&req);
  } else {
    /*
     * object
     */
      rc = librgw.get_uri(parent->handle, uri);
      if (rc < 0 ) { /* invalid parent */
	return rc;
      }
      uri += path;
      /* TODO: implement
       * RGWDeleteObjectRequest req(cct, fs->get_user(), uri);
      */
  }

  return rc;
}

/*
  lookup a directory or file
*/
int rgw_lookup(struct rgw_fs *rgw_fs,
	      const struct rgw_file_handle *parent_fh, const char* path,
	      struct rgw_file_handle **fh, uint32_t flags)
{
  string uri;
  int rc;

  rc = librgw.get_uri(parent_fh->handle, uri);
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

  RGWFileHandle* rgw_fh = new RGWFileHandle();
  struct rgw_file_handle *rfh = rgw_fh->get_fh();

  /* find or create a handle for the object or bucket */
  rfh->handle = librgw.get_handle(uri);
  *fh = rfh;

  return 0;
} /* rgw_lookup */

/*
 * release file handle
 */
int rgw_fh_rele(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		uint32_t flags)
{
  RGWFileHandle* rgw_fh = get_rgwfh(fh);
  rgw_fh->rele();

  return 0;
}

/*
   get unix attributes for object
*/
int rgw_getattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st)
{
  string uri;
  int rc;

  rc = librgw.get_uri(fh->handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  return 0;
}

/*
  set unix attributes for object
*/
int rgw_setattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st,
		uint32_t mask)
{
  /* XXX no-op */
  return 0;
}

/*
   truncate file
*/
int rgw_truncate(struct rgw_fs *rgw_fs,
		 struct rgw_file_handle *fh, uint64_t size)
{
  return 0;
}

/*
   open file
*/
int rgw_open(struct rgw_fs *rgw_fs,
	     struct rgw_file_handle *fh, uint32_t flags)
{
  RGWFileHandle* rgw_fh = get_rgwfh(fh);
  rgw_fh->open(/* XXX */);

  return 0;
}

/*
   close file
*/
int rgw_close(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, uint32_t flags)
{
  RGWFileHandle* rgw_fh = get_rgwfh(fh);
  rgw_fh->close(/* XXX */);

  if (flags & RGW_CLOSE_FLAG_RELE)
    rgw_fh->rele();

  return 0;
}

int rgw_readdir(struct rgw_fs *rgw_fs,
		const struct rgw_file_handle *parent_fh, uint64_t *offset,
		rgw_readdir_cb rcb, void *cb_arg, bool *eof)
{
  int rc;
  string uri;

  rc = librgw.get_uri(parent_fh->handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);

  /* TODO:
   * deal with markers (continuation)
   * deal with authorization
   * consider non-default tenancy/user and bucket layouts
   */
  if (is_root(uri)) {
    /* for now, root always contains one user's bucket namespace */
    RGWListBucketsRequest req(cct, fs->get_user(), rcb, cb_arg, offset);
    rc = librgw.get_fe()->execute_req(&req);
  } else {
    /*
     * bucket?
     */
    uri += "/";
    RGWListBucketRequest req(cct, fs->get_user(), uri, rcb, cb_arg, offset);
    rc = librgw.get_fe()->execute_req(&req);

  }

  /* XXXX request MUST set this */
  *eof = true; // XXX move into RGGWListBucket(s)Request

  return rc;
}

/*
   read data from file
*/
int rgw_read(struct rgw_fs *rgw_fs,
	     struct rgw_file_handle *fh, uint64_t offset,
	     size_t length, void *buffer)
{
  return 0;
}

/*
   write data to file
*/
int rgw_write(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, uint64_t offset,
	      size_t length, void *buffer)
{
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  return 0;
}

/*
   sync written data
*/
int rgw_fsync(struct rgw_fs *rgw_fs, struct rgw_file_handle *handle)
{
  return 0;
}

} /* extern "C" */
