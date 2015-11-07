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

using namespace rgw;

extern RGWLib librgw;

const string RGWFileHandle::root_name = "/";

atomic<uint32_t> RGWLibFS::fs_inst;

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

  struct rgw_fs *fs = new_fs->get_fs();
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
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);

  /* XXX for now, just publish a huge capacity and
   * limited utiliztion */
  vfs_st->f_bsize = 1024*1024 /* 1M */;
  vfs_st->f_frsize = 1024;    /* minimal allocation unit (who cares) */
  vfs_st->f_blocks = UINT64_MAX;
  vfs_st->f_bfree = UINT64_MAX;
  vfs_st->f_bavail = UINT64_MAX;
  vfs_st->f_files = 1024; /* object count, do we have an est? */
  vfs_st->f_ffree = UINT64_MAX;
  vfs_st->f_fsid[0] = fs->get_inst();
  vfs_st->f_fsid[1] = fs->get_inst();
  vfs_st->f_flag = 0;
  vfs_st->f_namemax = 4096;
  return 0;
}

/*
  generic create
*/
int rgw_create(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh,
	       const char *name, mode_t mode, struct stat *st,
	       struct rgw_file_handle **fh)
{
  return EINVAL;
}

/*
  create a new directory
*/
int rgw_mkdir(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh,
	      const char *name, mode_t mode, struct stat *st,
	      struct rgw_file_handle **fh)
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

  /* XXX: atomicity */
  RGWFileHandle* rgw_fh = fs->lookup_fh(parent, name);

  struct rgw_file_handle *rfh = rgw_fh->get_fh();
  *fh = rfh;

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
  int rc = 0;

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);

  if (fs->is_root(rgw_fs)) {
    /* XXXX remove uri and deal with bucket and object names */
    string uri = "/";
    uri += path;
    RGWDeleteBucketRequest req(cct, fs->get_user(), uri);
    rc = librgw.get_fe()->execute_req(&req);
  } else {
    /*
     * object
     */
    RGWDeleteObjRequest req(cct, fs->get_user(), "/", path);
    rc = librgw.get_fe()->execute_req(&req);
  }

  return rc;
}

void dump_buckets(void) {
  /* get the bucket list */
  string marker; // XXX need to match offset
  string end_marker;
  uint64_t bucket_count, bucket_objcount;

  RGWUserBuckets buckets;
  uint64_t max_buckets = g_ceph_context->_conf->rgw_list_buckets_max_chunk;

  RGWRados* store = librgw.get_store();

  /* XXX check offsets */
  uint64_t ix = 3;
  rgw_user uid("testuser");
  int rc = rgw_read_user_buckets(store, uid, buckets, marker, end_marker,
				 max_buckets, true);
  if (rc == 0) {
    bucket_count = 0;
    bucket_objcount = 0;
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    for (auto& ib : m) {
      RGWBucketEnt& bent = ib.second;
      bucket_objcount += bent.count;
      marker = ib.first;
      std::cout << bent.bucket.name.c_str() << " ix: " << ix++ << std::endl;
    }
    bucket_count += m.size();
  }
} /* dump_buckets */

/*
  lookup object by name (POSIX style)
*/
int rgw_lookup(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh, const char* path,
	      struct rgw_file_handle **fh, uint32_t flags)
{
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);

  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if ((! parent) ||
      (parent->is_object())) {
    /* bad parent */
    return EINVAL;
  }

  RGWStatObjRequest req(cct, fs->get_user(), parent->bucket_name(), path,
			RGWStatObjRequest::FLAG_NONE);

  int rc = librgw.get_fe()->execute_req(&req);
  if (rc != 0)
    return ENOENT;

  RGWFileHandle* rgw_fh = fs->lookup_fh(parent, path);
  if (! rgw_fh)
    return ENOENT;

  struct rgw_file_handle *rfh = rgw_fh->get_fh();
  *fh = rfh;

  return 0;
} /* rgw_lookup */

/*
  lookup object by handle (NFS style)
*/
int rgw_lookup_handle(struct rgw_fs *rgw_fs, struct rgw_fh_hk *fh_hk,
		      struct rgw_file_handle **fh, uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);

  RGWFileHandle* rgw_fh = fs->lookup_handle(*fh_hk);
  if (! rgw_fh) {
    /* not found */
    return ENOENT;
  }

  struct rgw_file_handle *rfh = rgw_fh->get_fh();
  *fh = rfh;

  return 0;
}

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
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  RGWStatObjRequest req(cct, fs->get_user(),
			rgw_fh->bucket_name(), rgw_fh->object_name(),
			RGWStatObjRequest::FLAG_NONE);

  int rc = librgw.get_fe()->execute_req(&req);
  if (rc != 0)
    return EINVAL;

  /* fill in stat data */
  memset(st, 0, sizeof(struct stat));
  st->st_dev = fs->get_inst();
  st->st_ino = rgw_fh->get_fh()->fh_hk.object; // XXX
  st->st_mode = 666;
  st->st_nlink = 1;
  st->st_uid = 0; // XXX
  st->st_gid = 0; // XXX
  st->st_size = req.size();
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size) / 512;
  st->st_atim.tv_sec = req.mtime();
  st->st_mtim.tv_sec = req.mtime();
  st->st_ctim.tv_sec = req.ctime();

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
    /* XXXX remove uri, deal with bucket and object names */
    string uri = "/" + parent->bucket_name() + "/";
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
	    size_t length, size_t *bytes_read, void *buffer)
{
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  if (! rgw_fh->is_object())
    return EINVAL;

  size_t nread = 0;

  /* XXX testing only */
  buffer::list bl;
  RGWGetObjRequest req(cct, fs->get_user(), rgw_fh->bucket_name(),
		      rgw_fh->object_name(), offset, length, bl);

  int rc = librgw.get_fe()->execute_req(&req);

  if (! rc) {
    uint64_t off = 0;
    for (auto& bp : bl.buffers()) {
      size_t bytes = std::min(length, size_t(bp.length()));
      memcpy(static_cast<char*>(buffer)+off, bp.c_str(), bytes);
      nread += bytes;
      off += bytes;
      if (off >= length)
	break;
    }
  }

  *bytes_read = nread;

  return rc;
}

/*
   write data to file
*/
int rgw_write(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, uint64_t offset,
	      size_t length, size_t *bytes_written, void *buffer)
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

  *bytes_written = (rc == 0) ? req.bytes_written : 0;

  return rc;
}

/*
   read data from file (vector)
*/
class RGWReadV
{
  buffer::list bl;
  struct rgw_vio* vio;

public:
  RGWReadV(buffer::list& _bl, rgw_vio* _vio) : vio(_vio) {
    bl.claim(_bl);
  }

  struct rgw_vio* get_vio() { return vio; }

  const std::list<buffer::ptr>& buffers() { return bl.buffers(); }

  unsigned /* XXX */ length() { return bl.length(); }

};

void rgw_readv_rele(struct rgw_uio *uio, uint32_t flags)
{
  RGWReadV* rdv = static_cast<RGWReadV*>(uio->uio_p1);
  rdv->~RGWReadV();
  ::operator delete(rdv);
}

int rgw_readv(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, rgw_uio *uio)
{
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  if (! rgw_fh->is_object())
    return EINVAL;

  dout(15) << "test dout" << dendl;

  buffer::list bl;
  RGWGetObjRequest req(cct, fs->get_user(), rgw_fh->bucket_name(),
		      rgw_fh->object_name(), uio->uio_offset, uio->uio_resid,
		      bl);
  req.do_hexdump = false;

  int rc = librgw.get_fe()->execute_req(&req);

  if (! rc) {
    RGWReadV* rdv = static_cast<RGWReadV*>(
      ::operator new(sizeof(RGWReadV) +
		    (bl.buffers().size() * sizeof(struct rgw_vio))));

    (void) new (rdv)
      RGWReadV(bl, reinterpret_cast<rgw_vio*>(rdv+sizeof(RGWReadV)));

    uio->uio_p1 = rdv;
    uio->uio_cnt = rdv->buffers().size();
    uio->uio_resid = rdv->length();
    uio->uio_vio = rdv->get_vio();
    uio->uio_rele = rgw_readv_rele;

    int ix = 0;
    auto& buffers = rdv->buffers();
    for (auto& bp : buffers) {
      rgw_vio *vio = &(uio->uio_vio[ix]);
      vio->vio_base = const_cast<char*>(bp.c_str());
      vio->vio_len = bp.length();
      vio->vio_u1 = nullptr;
      vio->vio_p1 = nullptr;
      ++ix;
    }
  }

  return rc;
}

/*
   write data to file (vector)
*/
  int rgw_writev(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		rgw_uio *uio)
{
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  if (! rgw_fh->is_object())
    return EINVAL;

  buffer::list bl;
  for (unsigned int ix = 0; ix < uio->uio_cnt; ++ix) {
    rgw_vio *vio = &(uio->uio_vio[ix]);
    bl.push_back(
      buffer::create_static(vio->vio_len,
			    static_cast<char*>(vio->vio_base)));
  }

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
