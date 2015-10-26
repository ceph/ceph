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

const string RGWFileHandle::root_name = "/";

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

  /* XXX we no longer assume "/" is unique, but we aren't tracking the
   * roots atm */

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
	       struct rgw_file_handle *parent_fh,
	       struct rgw_statvfs *vfs_st)
{
  memset(vfs_st, 0, sizeof(struct rgw_statvfs));
  return 0;
}

/*
  generic create
*/
int rgw_create(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh,
	       const char *name, mode_t mode, struct stat *st,
	       struct rgw_file_handle *handle)
{
  return 0;
}

/*
  create a new directory
*/
int rgw_mkdir(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh,
	      const char *name, mode_t mode, struct stat *st,
	      struct rgw_file_handle *handle)
{
  int rc;

  /* XXXX remove uri, deal with bucket names */
  string uri;

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);

  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if (! parent) {
    /* bad parent */
    return EINVAL;
  }

  if (! parent->is_root()) {
    /* cannot create a bucket in a bucket */
    return ENOTDIR;
  }

  // XXXX fix this
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
	       struct rgw_file_handle *olddir, const char* old_name,
	       struct rgw_file_handle *newdir, const char* new_name)
{
  /* -ENOTSUP */
  return -EINVAL;
}

/*
  remove file or directory
*/
int rgw_unlink(struct rgw_fs *rgw_fs, struct rgw_file_handle* parent,
	      const char* path)
{
  /* XXXX remove uri and deal with bucket and object names */
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
	      struct rgw_file_handle *parent_fh, const char* path,
	      struct rgw_file_handle **fh, uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);

  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if (! parent) {
    /* bad parent */
    return EINVAL;
  }

  RGWFileHandle* rgw_fh = new RGWFileHandle(fs, parent, path);
  if (! rgw_fh) {
    /* not found */
    return ENOENT;
  }

  struct rgw_file_handle *rfh = rgw_fh->get_fh();
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
		struct rgw_file_handle *parent_fh, uint64_t *offset,
		rgw_readdir_cb rcb, void *cb_arg, bool *eof)
{
  int rc;

  /* XXXX remove uri, deal with bucket and object names */
  string uri;

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);

  /* TODO:
   * deal with markers (continuation)
   * deal with authorization
   * consider non-default tenancy/user and bucket layouts
   */
  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if (! parent) {
    /* bad parent */
    return EINVAL;
  }

  if (parent->is_root()) {
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
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  if (! rgw_fh->is_object())
    return EINVAL;

  /* XXXX testing only */
  buffer::list bl;
  bl.push_back(
    buffer::create_static(length /* XXX size */, static_cast<char*>(buffer)));

  /* XXX */
  RGWPutObjRequest req(cct, fs->get_user(), rgw_fh->bucket_name(),
		      rgw_fh->object_name(), bl);
  int rc = librgw.get_fe()->execute_req(&req);

  return rc;
}

/*
   sync written data
*/
int rgw_fsync(struct rgw_fs *rgw_fs, struct rgw_file_handle *handle)
{
  return 0;
}

} /* extern "C" */
