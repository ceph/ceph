// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "include/compat.h"
#include "include/rados/rgw_file.h"

#include <sys/types.h>
#include <sys/stat.h>

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
#include "rgw_zone.h"
#include "rgw_file.h"
#include "rgw_lib_frontend.h"
#include "rgw_perf_counters.h"
#include "common/errno.h"

#include "services/svc_zone.h"

#include <atomic>

#define dout_subsys ceph_subsys_rgw

using namespace rgw;

namespace rgw {

  extern RGWLib rgwlib;

  const string RGWFileHandle::root_name = "/";

  std::atomic<uint32_t> RGWLibFS::fs_inst_counter;

  uint32_t RGWLibFS::write_completion_interval_s = 10;

  ceph::timer<ceph::mono_clock> RGWLibFS::write_timer{
    ceph::construct_suspended};

  inline int valid_fs_bucket_name(const string& name) {
    int rc = valid_s3_bucket_name(name, false /* relaxed */);
    if (rc != 0) {
      if (name.size() > 255)
        return -ENAMETOOLONG;
      return -EINVAL;
    }
    return 0;
  }

  inline int valid_fs_object_name(const string& name) {
    int rc = valid_s3_object_name(name);
    if (rc != 0) {
      if (name.size() > 1024)
        return -ENAMETOOLONG;
      return -EINVAL;
    }
    return 0;
  }

  LookupFHResult RGWLibFS::stat_bucket(RGWFileHandle* parent, const char *path,
				       RGWLibFS::BucketStats& bs,
				       uint32_t flags)
  {
    LookupFHResult fhr{nullptr, 0};
    std::string bucket_name{path};
    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);
    RGWStatBucketRequest req(cct, &ruser, bucket_name, bs);

    int rc = rgwlib.get_fe()->execute_req(&req);
    if ((rc == 0) &&
	(req.get_ret() == 0) &&
	(req.matched())) {
      fhr = lookup_fh(parent, path,
		      (flags & RGWFileHandle::FLAG_LOCKED)|
		      RGWFileHandle::FLAG_CREATE|
		      RGWFileHandle::FLAG_BUCKET);
      if (get<0>(fhr)) {
	RGWFileHandle* rgw_fh = get<0>(fhr);
	if (! (flags & RGWFileHandle::FLAG_LOCKED)) {
	  rgw_fh->mtx.lock();
	}
	rgw_fh->set_times(req.get_ctime());
	/* restore attributes */
	auto ux_key = req.get_attr(RGW_ATTR_UNIX_KEY1);
	auto ux_attrs = req.get_attr(RGW_ATTR_UNIX1);
	if (ux_key && ux_attrs) {
	  DecodeAttrsResult dar = rgw_fh->decode_attrs(ux_key, ux_attrs);
	  if (get<0>(dar) || get<1>(dar)) {
	    update_fh(rgw_fh);
          }
	}
	if (! (flags & RGWFileHandle::FLAG_LOCKED)) {
	  rgw_fh->mtx.unlock();
	}
      }
    }
    return fhr;
  }

  LookupFHResult RGWLibFS::fake_leaf(RGWFileHandle* parent,
				     const char *path,
				     enum rgw_fh_type type,
				     struct stat *st, uint32_t st_mask,
				     uint32_t flags)
  {
    /* synthesize a minimal handle from parent, path, type, and st */
    using std::get;

    flags |= RGWFileHandle::FLAG_CREATE;

    switch (type) {
    case RGW_FS_TYPE_DIRECTORY:
      flags |= RGWFileHandle::FLAG_DIRECTORY;
      break;
    default:
      /* file */
      break;
    };

    LookupFHResult fhr = lookup_fh(parent, path, flags);
    if (get<0>(fhr)) {
      RGWFileHandle* rgw_fh = get<0>(fhr);
      if (st) {	
	lock_guard guard(rgw_fh->mtx);
	if (st_mask & RGW_SETATTR_SIZE) {
	  rgw_fh->set_size(st->st_size);
	}
	if (st_mask & RGW_SETATTR_MTIME) {
	  rgw_fh->set_times(st->st_mtim);
	}
      } /* st */
    } /* rgw_fh */
    return fhr;
  } /* RGWLibFS::fake_leaf */

  LookupFHResult RGWLibFS::stat_leaf(RGWFileHandle* parent,
				     const char *path,
				     enum rgw_fh_type type,
				     uint32_t flags)
  {
    /* find either-of <object_name>, <object_name/>, only one of
     * which should exist;  atomicity? */
    using std::get;

    LookupFHResult fhr{nullptr, 0};

    /* XXX the need for two round-trip operations to identify file or
     * directory leaf objects is unecessary--the current proposed
     * mechanism to avoid this is to store leaf object names with an
     * object locator w/o trailing slash */

    std::string obj_path = parent->format_child_name(path, false);
    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);

    for (auto ix : { 0, 1, 2 }) {
      switch (ix) {
      case 0:
      {
	/* type hint */
	if (type == RGW_FS_TYPE_DIRECTORY)
	  continue;

	RGWStatObjRequest req(cct, &ruser,
			      parent->bucket_name(), obj_path,
			      RGWStatObjRequest::FLAG_NONE);
	int rc = rgwlib.get_fe()->execute_req(&req);
	if ((rc == 0) &&
	    (req.get_ret() == 0)) {
	  fhr = lookup_fh(parent, path, RGWFileHandle::FLAG_CREATE);
	  if (get<0>(fhr)) {
	    RGWFileHandle* rgw_fh = get<0>(fhr);
	    lock_guard guard(rgw_fh->mtx);
	    rgw_fh->set_size(req.get_size());
	    rgw_fh->set_times(req.get_mtime());
	    /* restore attributes */
	    auto ux_key = req.get_attr(RGW_ATTR_UNIX_KEY1);
	    auto ux_attrs = req.get_attr(RGW_ATTR_UNIX1);
            rgw_fh->set_etag(*(req.get_attr(RGW_ATTR_ETAG)));
            rgw_fh->set_acls(*(req.get_attr(RGW_ATTR_ACL)));
	    if (ux_key && ux_attrs) {
              DecodeAttrsResult dar = rgw_fh->decode_attrs(ux_key, ux_attrs);
              if (get<0>(dar) || get<1>(dar)) {
                update_fh(rgw_fh);
              }
	    }
	  }
	  goto done;
	}
      }
      break;
      case 1:
      {
	/* try dir form */
	/* type hint */
	if (type == RGW_FS_TYPE_FILE)
	  continue;

	obj_path += "/";
	RGWStatObjRequest req(cct, &ruser,
			      parent->bucket_name(), obj_path,
			      RGWStatObjRequest::FLAG_NONE);
	int rc = rgwlib.get_fe()->execute_req(&req);
	if ((rc == 0) &&
	    (req.get_ret() == 0)) {
	  fhr = lookup_fh(parent, path, RGWFileHandle::FLAG_DIRECTORY);
	  if (get<0>(fhr)) {
	    RGWFileHandle* rgw_fh = get<0>(fhr);
	    lock_guard guard(rgw_fh->mtx);
	    rgw_fh->set_size(req.get_size());
	    rgw_fh->set_times(req.get_mtime());
	    /* restore attributes */
	    auto ux_key = req.get_attr(RGW_ATTR_UNIX_KEY1);
	    auto ux_attrs = req.get_attr(RGW_ATTR_UNIX1);
            rgw_fh->set_etag(*(req.get_attr(RGW_ATTR_ETAG)));
            rgw_fh->set_acls(*(req.get_attr(RGW_ATTR_ACL)));
	    if (ux_key && ux_attrs) {
              DecodeAttrsResult dar = rgw_fh->decode_attrs(ux_key, ux_attrs);
              if (get<0>(dar) || get<1>(dar)) {
                update_fh(rgw_fh);
              }
	    }
	  }
	  goto done;
	}
      }
      break;
      case 2:
      {
	std::string object_name{path};
	RGWStatLeafRequest req(cct, &ruser, parent, object_name);
	int rc = rgwlib.get_fe()->execute_req(&req);
	if ((rc == 0) &&
	    (req.get_ret() == 0)) {
	  if (req.matched) {
	    /* we need rgw object's key name equal to file name, if
	     * not return NULL */
	    if ((flags & RGWFileHandle::FLAG_EXACT_MATCH) &&
		!req.exact_matched) {
	      lsubdout(get_context(), rgw, 15)
	        << __func__
		<< ": stat leaf not exact match file name = "
		<< path << dendl;
	      goto done;
	    }
	    fhr = lookup_fh(parent, path,
			    RGWFileHandle::FLAG_CREATE|
			    ((req.is_dir) ?
			      RGWFileHandle::FLAG_DIRECTORY :
			      RGWFileHandle::FLAG_NONE));
	    /* XXX we don't have an object--in general, there need not
	     * be one (just a path segment in some other object).  In
	     * actual leaf an object exists, but we'd need another round
	     * trip to get attrs */
	    if (get<0>(fhr)) {
	      /* for now use the parent object's mtime */
	      RGWFileHandle* rgw_fh = get<0>(fhr);
	      lock_guard guard(rgw_fh->mtx);
	      rgw_fh->set_mtime(parent->get_mtime());
	    }
	  }
	}
      }
      break;
      default:
	/* not reached */
	break;
      }
    }
  done:
    return fhr;
  } /* RGWLibFS::stat_leaf */

  int RGWLibFS::read(RGWFileHandle* rgw_fh, uint64_t offset, size_t length,
		     size_t* bytes_read, void* buffer, uint32_t flags)
  {
    if (! rgw_fh->is_file())
      return -EINVAL;

    if (rgw_fh->deleted())
      return -ESTALE;

    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);
    RGWReadRequest req(get_context(), &ruser, rgw_fh, offset, length,
		       buffer);

    int rc = rgwlib.get_fe()->execute_req(&req);
    if ((rc == 0) &&
	(req.get_ret() == 0)) {
      lock_guard guard(rgw_fh->mtx);
      rgw_fh->set_atime(real_clock::to_timespec(real_clock::now()));
      *bytes_read = req.nread;
    }

    return rc;
  }

  int RGWLibFS::readlink(RGWFileHandle* rgw_fh, uint64_t offset, size_t length,
                     size_t* bytes_read, void* buffer, uint32_t flags)
  {
    if (! rgw_fh->is_link())
      return -EINVAL;

    if (rgw_fh->deleted())
      return -ESTALE;

    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);
    RGWReadRequest req(get_context(), &ruser, rgw_fh, offset, length,
                       buffer);

    int rc = rgwlib.get_fe()->execute_req(&req);
    if ((rc == 0) &&
        (req.get_ret() == 0)) {
      lock_guard(rgw_fh->mtx);
      rgw_fh->set_atime(real_clock::to_timespec(real_clock::now()));
      *bytes_read = req.nread;
    }

    return rc;
  }

  int RGWLibFS::unlink(RGWFileHandle* rgw_fh, const char* name, uint32_t flags)
  {
    int rc = 0;
    BucketStats bs;
    RGWFileHandle* parent = nullptr;
    RGWFileHandle* bkt_fh = nullptr;

    if (unlikely(flags & RGWFileHandle::FLAG_UNLINK_THIS)) {
      /* LOCKED */
      parent = rgw_fh->get_parent();
    } else {
      /* atomicity */
      parent = rgw_fh;
      LookupFHResult fhr = lookup_fh(parent, name, RGWFileHandle::FLAG_LOCK);
      rgw_fh = get<0>(fhr);
      /* LOCKED */
    }

    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);
    if (parent->is_root()) {
      /* a bucket may have an object storing Unix attributes, check
       * for and delete it */
      LookupFHResult fhr;
      fhr = stat_bucket(parent, name, bs, (rgw_fh) ?
			RGWFileHandle::FLAG_LOCKED :
			RGWFileHandle::FLAG_NONE);
      bkt_fh = get<0>(fhr);
      if (unlikely(! bkt_fh)) {
	/* implies !rgw_fh, so also !LOCKED */
	return -ENOENT;
      }

      if (bs.num_entries > 1) {
	unref(bkt_fh); /* return stat_bucket ref */
	if (likely(!! rgw_fh)) { /* return lock and ref from
				  * lookup_fh (or caller in the
				  * special case of
				  * RGWFileHandle::FLAG_UNLINK_THIS) */
	  rgw_fh->mtx.unlock();
	  unref(rgw_fh);
	}
	return -ENOTEMPTY;
      } else {
	/* delete object w/key "<bucket>/" (uxattrs), if any */
	string oname{"/"};
	RGWDeleteObjRequest req(cct, &ruser, bkt_fh->bucket_name(), oname);
	rc = rgwlib.get_fe()->execute_req(&req);
	/* don't care if ENOENT */
	unref(bkt_fh);
      }

      string bname{name};
      RGWDeleteBucketRequest req(cct, &ruser, bname);
      rc = rgwlib.get_fe()->execute_req(&req);
      if (! rc) {
	rc = req.get_ret();
      }
    } else {
      /*
       * leaf object
       */
      if (! rgw_fh) {
	/* XXX for now, peform a hard lookup to deduce the type of
	 * object to be deleted ("foo" vs. "foo/")--also, ensures
	 * atomicity at this endpoint */
	struct rgw_file_handle *fh;
	rc = rgw_lookup(get_fs(), parent->get_fh(), name, &fh,
			nullptr /* st */, 0 /* mask */,
			RGW_LOOKUP_FLAG_NONE);
	if (!! rc)
	  return rc;

	/* rgw_fh ref+ */
	rgw_fh = get_rgwfh(fh);
	rgw_fh->mtx.lock(); /* LOCKED */
      }

      std::string oname = rgw_fh->relative_object_name();
      if (rgw_fh->is_dir()) {
	/* for the duration of our cache timer, trust positive
	 * child cache */
	if (rgw_fh->has_children()) {
	  rgw_fh->mtx.unlock();
	  unref(rgw_fh);
	  return(-ENOTEMPTY);
	}
	oname += "/";
      }
      RGWDeleteObjRequest req(cct, &ruser, parent->bucket_name(),
			      oname);
      rc = rgwlib.get_fe()->execute_req(&req);
      if (! rc) {
	rc = req.get_ret();
      }
    }

    /* ENOENT when raced with other s3 gateway */
    if (! rc || rc == -ENOENT) {
      rgw_fh->flags |= RGWFileHandle::FLAG_DELETED;
      fh_cache.remove(rgw_fh->fh.fh_hk.object, rgw_fh,
		      RGWFileHandle::FHCache::FLAG_LOCK);
    }

    if (! rc) {
      real_time t = real_clock::now();
      parent->set_mtime(real_clock::to_timespec(t));
      parent->set_ctime(real_clock::to_timespec(t));
    }

    rgw_fh->mtx.unlock();
    unref(rgw_fh);

    return rc;
  } /* RGWLibFS::unlink */

  int RGWLibFS::rename(RGWFileHandle* src_fh, RGWFileHandle* dst_fh,
		       const char *_src_name, const char *_dst_name)

  {
    /* XXX initial implementation: try-copy, and delete if copy
     * succeeds */
    int rc = -EINVAL;
    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);

    real_time t;

    std::string src_name{_src_name};
    std::string dst_name{_dst_name};

    /* atomicity */
    LookupFHResult fhr = lookup_fh(src_fh, _src_name, RGWFileHandle::FLAG_LOCK);
    RGWFileHandle* rgw_fh = get<0>(fhr);

    /* should not happen */
    if (! rgw_fh) {
      ldout(get_context(), 0) << __func__
		       << " BUG no such src renaming path="
		       << src_name
		       << dendl;
      goto out;
    }

    /* forbid renaming of directories (unreasonable at scale) */
    if (rgw_fh->is_dir()) {
      ldout(get_context(), 12) << __func__
			<< " rejecting attempt to rename directory path="
			<< rgw_fh->full_object_name()
			<< dendl;
      rc = -EPERM;
      goto unlock;
    }

    /* forbid renaming open files (violates intent, for now) */
    if (rgw_fh->is_open()) {
      ldout(get_context(), 12) << __func__
			<< " rejecting attempt to rename open file path="
			<< rgw_fh->full_object_name()
			<< dendl;
      rc = -EPERM;
      goto unlock;
    }

    t = real_clock::now();

    for (int ix : {0, 1}) {
      switch (ix) {
      case 0:
      {
	RGWCopyObjRequest req(cct, &ruser, src_fh, dst_fh, src_name,
			      dst_name);
	int rc = rgwlib.get_fe()->execute_req(&req);
	if ((rc != 0) ||
	    ((rc = req.get_ret()) != 0)) {
	  ldout(get_context(), 1)
	    << __func__
	    << " rename step 0 failed src="
	    << src_fh->full_object_name() << " " << src_name
	    << " dst=" << dst_fh->full_object_name()
	    << " " << dst_name
	    << "rc " << rc
	    << dendl;
	  goto unlock;
	}
	ldout(get_context(), 12)
	  << __func__
	  << " rename step 0 success src="
	  << src_fh->full_object_name() << " " << src_name
	  << " dst=" << dst_fh->full_object_name()
	  << " " << dst_name
	  << " rc " << rc
	  << dendl;
	/* update dst change id */
	dst_fh->set_times(t);
      }
      break;
      case 1:
      {
	rc = this->unlink(rgw_fh /* LOCKED */, _src_name,
			  RGWFileHandle::FLAG_UNLINK_THIS);
	/* !LOCKED, -ref */
	if (! rc) {
	  ldout(get_context(), 12)
	    << __func__
	    << " rename step 1 success src="
	    << src_fh->full_object_name() << " " << src_name
	    << " dst=" << dst_fh->full_object_name()
	    << " " << dst_name
	    << " rc " << rc
	    << dendl;
	  /* update src change id */
	  src_fh->set_times(t);
	} else {
	  ldout(get_context(), 1)
	    << __func__
	    << " rename step 1 failed src="
	    << src_fh->full_object_name() << " " << src_name
	    << " dst=" << dst_fh->full_object_name()
	    << " " << dst_name
	    << " rc " << rc
	    << dendl;
	}
      }
      goto out;
      default:
	ceph_abort();
      } /* switch */
    } /* ix */
  unlock:
    rgw_fh->mtx.unlock(); /* !LOCKED */
    unref(rgw_fh); /* -ref */

  out:
    return rc;
  } /* RGWLibFS::rename */

  MkObjResult RGWLibFS::mkdir(RGWFileHandle* parent, const char *name,
			      struct stat *st, uint32_t mask, uint32_t flags)
  {
    int rc, rc2;
    rgw_file_handle *lfh;
    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);

    rc = rgw_lookup(get_fs(), parent->get_fh(), name, &lfh,
		    nullptr /* st */, 0 /* mask */,
		    RGW_LOOKUP_FLAG_NONE);
    if (! rc) {
      /* conflict! */
      rc = rgw_fh_rele(get_fs(), lfh, RGW_FH_RELE_FLAG_NONE);
      return MkObjResult{nullptr, -EEXIST};
    }

    MkObjResult mkr{nullptr, -EINVAL};
    LookupFHResult fhr;
    RGWFileHandle* rgw_fh = nullptr;
    buffer::list ux_key, ux_attrs;

    fhr = lookup_fh(parent, name,
		    RGWFileHandle::FLAG_CREATE|
		    RGWFileHandle::FLAG_DIRECTORY|
		    RGWFileHandle::FLAG_LOCK);
    rgw_fh = get<0>(fhr);
    if (rgw_fh) {
      rgw_fh->create_stat(st, mask);
      rgw_fh->set_times(real_clock::now());
      /* save attrs */
      rgw_fh->encode_attrs(ux_key, ux_attrs);
      if (st)
        rgw_fh->stat(st, RGWFileHandle::FLAG_LOCKED);
      get<0>(mkr) = rgw_fh;
    } else {
      get<1>(mkr) = -EIO;
      return mkr;
    }

    if (parent->is_root()) {
      /* bucket */
      string bname{name};
      /* enforce S3 name restrictions */
      rc = valid_fs_bucket_name(bname);
      if (rc != 0) {
	rgw_fh->flags |= RGWFileHandle::FLAG_DELETED;
	fh_cache.remove(rgw_fh->fh.fh_hk.object, rgw_fh,
			RGWFileHandle::FHCache::FLAG_LOCK);
	rgw_fh->mtx.unlock();
	unref(rgw_fh);
	get<0>(mkr) = nullptr;
	get<1>(mkr) = rc;
	return mkr;
      }

      RGWCreateBucketRequest req(get_context(), &ruser, bname);

      /* save attrs */
      req.emplace_attr(RGW_ATTR_UNIX_KEY1, std::move(ux_key));
      req.emplace_attr(RGW_ATTR_UNIX1, std::move(ux_attrs));

      rc = rgwlib.get_fe()->execute_req(&req);
      rc2 = req.get_ret();
    } else {
      /* create an object representing the directory */
      buffer::list bl;
      string dir_name = parent->format_child_name(name, true);

      /* need valid S3 name (characters, length <= 1024, etc) */
      rc = valid_fs_object_name(dir_name);
      if (rc != 0) {
	rgw_fh->flags |= RGWFileHandle::FLAG_DELETED;
	fh_cache.remove(rgw_fh->fh.fh_hk.object, rgw_fh,
			RGWFileHandle::FHCache::FLAG_LOCK);
	rgw_fh->mtx.unlock();
	unref(rgw_fh);
	get<0>(mkr) = nullptr;
	get<1>(mkr) = rc;
	return mkr;
      }

      RGWPutObjRequest req(get_context(), &ruser, parent->bucket_name(),
			  dir_name, bl);

      /* save attrs */
      req.emplace_attr(RGW_ATTR_UNIX_KEY1, std::move(ux_key));
      req.emplace_attr(RGW_ATTR_UNIX1, std::move(ux_attrs));

      rc = rgwlib.get_fe()->execute_req(&req);
      rc2 = req.get_ret();
    }

    if (! ((rc == 0) &&
	   (rc2 == 0))) {
      /* op failed */
      rgw_fh->flags |= RGWFileHandle::FLAG_DELETED;
      rgw_fh->mtx.unlock(); /* !LOCKED */
      unref(rgw_fh);
      get<0>(mkr) = nullptr;
      /* fixup rc */
      if (!rc)
	rc = rc2;
    } else {
      real_time t = real_clock::now();
      parent->set_mtime(real_clock::to_timespec(t));
      parent->set_ctime(real_clock::to_timespec(t));
      rgw_fh->mtx.unlock(); /* !LOCKED */
    }

    get<1>(mkr) = rc;

    return mkr;
  } /* RGWLibFS::mkdir */

  MkObjResult RGWLibFS::create(RGWFileHandle* parent, const char *name,
			      struct stat *st, uint32_t mask, uint32_t flags)
  {
    int rc, rc2;

    using std::get;

    rgw_file_handle *lfh;
    rc = rgw_lookup(get_fs(), parent->get_fh(), name, &lfh,
		    nullptr /* st */, 0 /* mask */,
		    RGW_LOOKUP_FLAG_NONE);
    if (! rc) {
      /* conflict! */
      rc = rgw_fh_rele(get_fs(), lfh, RGW_FH_RELE_FLAG_NONE);
      return MkObjResult{nullptr, -EEXIST};
    }

    /* expand and check name */
    std::string obj_name = parent->format_child_name(name, false);
    rc = valid_fs_object_name(obj_name);
    if (rc != 0) {
      return MkObjResult{nullptr, rc};
    }

    /* create it */
    buffer::list bl;
    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);
    RGWPutObjRequest req(cct, &ruser, parent->bucket_name(), obj_name, bl);
    MkObjResult mkr{nullptr, -EINVAL};

    rc = rgwlib.get_fe()->execute_req(&req);
    rc2 = req.get_ret();

    if ((rc == 0) &&
	(rc2 == 0)) {
      /* XXX atomicity */
      LookupFHResult fhr = lookup_fh(parent, name, RGWFileHandle::FLAG_CREATE |
                                                   RGWFileHandle::FLAG_LOCK);
      RGWFileHandle* rgw_fh = get<0>(fhr);
      if (rgw_fh) {
	if (get<1>(fhr) & RGWFileHandle::FLAG_CREATE) {
	  /* fill in stat data */
	  real_time t = real_clock::now();
	  rgw_fh->create_stat(st, mask);
	  rgw_fh->set_times(t);

	  parent->set_mtime(real_clock::to_timespec(t));
	  parent->set_ctime(real_clock::to_timespec(t));
	}
        if (st)
	  (void) rgw_fh->stat(st, RGWFileHandle::FLAG_LOCKED);

        rgw_fh->set_etag(*(req.get_attr(RGW_ATTR_ETAG)));
        rgw_fh->set_acls(*(req.get_attr(RGW_ATTR_ACL))); 

	get<0>(mkr) = rgw_fh;
	rgw_fh->mtx.unlock();
      } else
	rc = -EIO;
    }

    get<1>(mkr) = rc;
   
    /* case like : quota exceed will be considered as fail too*/
    if(rc2 < 0)
       get<1>(mkr) = rc2;

    return mkr;
  } /* RGWLibFS::create */

  MkObjResult RGWLibFS::symlink(RGWFileHandle* parent, const char *name,
            const char* link_path, struct stat *st, uint32_t mask, uint32_t flags)
  {
    int rc, rc2;

    using std::get;

    rgw_file_handle *lfh;
    rc = rgw_lookup(get_fs(), parent->get_fh(), name, &lfh,
		    nullptr /* st */, 0 /* mask */,
                    RGW_LOOKUP_FLAG_NONE);
    if (! rc) {
      /* conflict! */
      rc = rgw_fh_rele(get_fs(), lfh, RGW_FH_RELE_FLAG_NONE);
      return MkObjResult{nullptr, -EEXIST};
    }

    MkObjResult mkr{nullptr, -EINVAL};
    LookupFHResult fhr;
    RGWFileHandle* rgw_fh = nullptr;
    buffer::list ux_key, ux_attrs;

    fhr = lookup_fh(parent, name,
      RGWFileHandle::FLAG_CREATE|
      RGWFileHandle::FLAG_SYMBOLIC_LINK|
      RGWFileHandle::FLAG_LOCK);
    rgw_fh = get<0>(fhr);
    if (rgw_fh) {
      rgw_fh->create_stat(st, mask);
      rgw_fh->set_times(real_clock::now());
      /* save attrs */
      rgw_fh->encode_attrs(ux_key, ux_attrs);
      if (st)
        rgw_fh->stat(st);
      get<0>(mkr) = rgw_fh;
    } else {
      get<1>(mkr) = -EIO;
      return mkr;
    }

    /* need valid S3 name (characters, length <= 1024, etc) */
    rc = valid_fs_object_name(name);
    if (rc != 0) {
      rgw_fh->flags |= RGWFileHandle::FLAG_DELETED;
      fh_cache.remove(rgw_fh->fh.fh_hk.object, rgw_fh,
        RGWFileHandle::FHCache::FLAG_LOCK);
      rgw_fh->mtx.unlock();
      unref(rgw_fh);
      get<0>(mkr) = nullptr;
      get<1>(mkr) = rc;
      return mkr;
    }

    string obj_name = std::string(name);
    /* create an object representing the directory */
    buffer::list bl;

    /* XXXX */
#if 0
    bl.push_back(
      buffer::create_static(len, static_cast<char*>(buffer)));
#else

    bl.push_back(
      buffer::copy(link_path, strlen(link_path)));
#endif

    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);
    RGWPutObjRequest req(get_context(), &ruser, parent->bucket_name(),
                         obj_name, bl);

    /* save attrs */
    req.emplace_attr(RGW_ATTR_UNIX_KEY1, std::move(ux_key));
    req.emplace_attr(RGW_ATTR_UNIX1, std::move(ux_attrs));

    rc = rgwlib.get_fe()->execute_req(&req);
    rc2 = req.get_ret();
    if (! ((rc == 0) &&
        (rc2 == 0))) {
      /* op failed */
      rgw_fh->flags |= RGWFileHandle::FLAG_DELETED;
      rgw_fh->mtx.unlock(); /* !LOCKED */
      unref(rgw_fh);
      get<0>(mkr) = nullptr;
      /* fixup rc */
      if (!rc)
        rc = rc2;
    } else {
      real_time t = real_clock::now();
      parent->set_mtime(real_clock::to_timespec(t));
      parent->set_ctime(real_clock::to_timespec(t));
      rgw_fh->mtx.unlock(); /* !LOCKED */
    }

    get<1>(mkr) = rc;

    return mkr;
  } /* RGWLibFS::symlink */

  int RGWLibFS::getattr(RGWFileHandle* rgw_fh, struct stat* st)
  {
    switch(rgw_fh->fh.fh_type) {
    case RGW_FS_TYPE_FILE:
    {
      if (rgw_fh->deleted())
	return -ESTALE;
    }
    break;
    default:
      break;
    };
    /* if rgw_fh is a directory, mtime will be advanced */
    return rgw_fh->stat(st);
  } /* RGWLibFS::getattr */

  int RGWLibFS::setattr(RGWFileHandle* rgw_fh, struct stat* st, uint32_t mask,
			uint32_t flags)
  {
    int rc, rc2;
    buffer::list ux_key, ux_attrs;
    buffer::list etag = rgw_fh->get_etag();
    buffer::list acls = rgw_fh->get_acls();

    lock_guard guard(rgw_fh->mtx);

    switch(rgw_fh->fh.fh_type) {
    case RGW_FS_TYPE_FILE:
    {
      if (rgw_fh->deleted())
	return -ESTALE;
    }
    break;
    default:
      break;
    };

    string obj_name{rgw_fh->relative_object_name()};

    if (rgw_fh->is_dir() &&
	(likely(! rgw_fh->is_bucket()))) {
      obj_name += "/";
    }

    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);
    RGWSetAttrsRequest req(cct, &ruser, rgw_fh->bucket_name(), obj_name);

    rgw_fh->create_stat(st, mask);
    rgw_fh->encode_attrs(ux_key, ux_attrs);

    /* save attrs */
    req.emplace_attr(RGW_ATTR_UNIX_KEY1, std::move(ux_key));
    req.emplace_attr(RGW_ATTR_UNIX1, std::move(ux_attrs));
    req.emplace_attr(RGW_ATTR_ETAG, std::move(etag));
    req.emplace_attr(RGW_ATTR_ACL, std::move(acls));

    rc = rgwlib.get_fe()->execute_req(&req);
    rc2 = req.get_ret();

    if (rc == -ENOENT) {
      /* special case:  materialize placeholder dir */
      buffer::list bl;
      RGWPutObjRequest req(get_context(), &ruser, rgw_fh->bucket_name(),
			   obj_name, bl);

      rgw_fh->encode_attrs(ux_key, ux_attrs); /* because std::moved */

      /* save attrs */
      req.emplace_attr(RGW_ATTR_UNIX_KEY1, std::move(ux_key));
      req.emplace_attr(RGW_ATTR_UNIX1, std::move(ux_attrs));

      rc = rgwlib.get_fe()->execute_req(&req);
      rc2 = req.get_ret();
    }

    if ((rc != 0) || (rc2 != 0)) {
      return -EIO;
    }

    rgw_fh->set_ctime(real_clock::to_timespec(real_clock::now()));

    return 0;
  } /* RGWLibFS::setattr */

  /* called under rgw_fh->mtx held */
  void RGWLibFS::update_fh(RGWFileHandle *rgw_fh)
  {
    int rc, rc2;
    string obj_name{rgw_fh->relative_object_name()};
    buffer::list ux_key, ux_attrs;

    if (rgw_fh->is_dir() &&
	(likely(! rgw_fh->is_bucket()))) {
      obj_name += "/";
    }

    lsubdout(get_context(), rgw, 17)
      << __func__
      << " update old versioned fh : " << obj_name
      << dendl;

    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), user);
    RGWSetAttrsRequest req(cct, &ruser, rgw_fh->bucket_name(), obj_name);

    rgw_fh->encode_attrs(ux_key, ux_attrs);

    req.emplace_attr(RGW_ATTR_UNIX_KEY1, std::move(ux_key));
    req.emplace_attr(RGW_ATTR_UNIX1, std::move(ux_attrs));

    rc = rgwlib.get_fe()->execute_req(&req);
    rc2 = req.get_ret();

    if ((rc != 0) || (rc2 != 0)) {
      lsubdout(get_context(), rgw, 17)
	<< __func__
	<< " update fh failed : " << obj_name
	<< dendl;
    }
  } /* RGWLibFS::update_fh */

  void RGWLibFS::close()
  {
    state.flags |= FLAG_CLOSED;

    class ObjUnref
    {
      RGWLibFS* fs;
    public:
      explicit ObjUnref(RGWLibFS* _fs) : fs(_fs) {}
      void operator()(RGWFileHandle* fh) const {
	lsubdout(fs->get_context(), rgw, 5)
	  << __func__
	  << fh->name
	  << " before ObjUnref refs=" << fh->get_refcnt()
	  << dendl;
	fs->unref(fh);
      }
    };

    /* force cache drain, forces objects to evict */
    fh_cache.drain(ObjUnref(this),
		  RGWFileHandle::FHCache::FLAG_LOCK);
    rgwlib.get_fe()->get_process()->unregister_fs(this);
    rele();
  } /* RGWLibFS::close */

  inline std::ostream& operator<<(std::ostream &os, fh_key const &fhk) {
    os << "<fh_key: bucket=";
    os << fhk.fh_hk.bucket;
    os << "; object=";
    os << fhk.fh_hk.object;
    os << ">";
    return os;
  }

  inline std::ostream& operator<<(std::ostream &os, struct timespec const &ts) {
      os << "<timespec: tv_sec=";
      os << ts.tv_sec;
      os << "; tv_nsec=";
      os << ts.tv_nsec;
      os << ">";
    return os;
  }

  std::ostream& operator<<(std::ostream &os, RGWLibFS::event const &ev) {
    os << "<event:";
      switch (ev.t) {
      case RGWLibFS::event::type::READDIR:
	os << "type=READDIR;";
	break;
      default:
	os << "type=UNKNOWN;";
	break;
      };
    os << "fid=" << ev.fhk.fh_hk.bucket << ":" << ev.fhk.fh_hk.object
       << ";ts=" << ev.ts << ">";
    return os;
  }

  void RGWLibFS::gc()
  {
    using std::get;
    using directory = RGWFileHandle::directory;

    /* dirent invalidate timeout--basically, the upper-bound on
     * inconsistency with the S3 namespace */
    auto expire_s
      = get_context()->_conf->rgw_nfs_namespace_expire_secs;

    /* max events to gc in one cycle */
    uint32_t max_ev = get_context()->_conf->rgw_nfs_max_gc;

    struct timespec now, expire_ts;
    event_vector ve;
    bool stop = false;
    std::deque<event> &events = state.events;

    do {
      (void) clock_gettime(CLOCK_MONOTONIC_COARSE, &now);
      lsubdout(get_context(), rgw, 15)
	<< "GC: top of expire loop"
	<< " now=" << now
	<< " expire_s=" << expire_s
	<< dendl;
      {
	lock_guard guard(state.mtx); /* LOCKED */
	lsubdout(get_context(), rgw, 15)
	  << "GC: processing"
	  << " count=" << events.size()
	  << " events"
	  << dendl;
        /* just return if no events */
	if (events.empty()) {
	  return;
	}
	uint32_t _max_ev =
	  (events.size() < 500) ? max_ev : (events.size() / 4);
	for (uint32_t ix = 0; (ix < _max_ev) && (events.size() > 0); ++ix) {
	  event& ev = events.front();
	  expire_ts = ev.ts;
	  expire_ts.tv_sec += expire_s;
	  if (expire_ts > now) {
	    stop = true;
	    break;
	  }
	  ve.push_back(ev);
	  events.pop_front();
	}
      } /* anon */
      /* !LOCKED */
      for (auto& ev : ve) {
	lsubdout(get_context(), rgw, 15)
	  << "try-expire ev: " << ev << dendl;
	if (likely(ev.t == event::type::READDIR)) {
	  RGWFileHandle* rgw_fh = lookup_handle(ev.fhk.fh_hk);
	  lsubdout(get_context(), rgw, 15)
	    << "ev rgw_fh: " << rgw_fh << dendl;
	  if (rgw_fh) {
	    RGWFileHandle::directory* d;
	    if (unlikely(! rgw_fh->is_dir())) {
	      lsubdout(get_context(), rgw, 0)
		<< __func__
		<< " BUG non-directory found with READDIR event "
		<< "(" << rgw_fh->bucket_name() << ","
		<< rgw_fh->object_name() << ")"
		<< dendl;
	      goto rele;
	    }
	    /* maybe clear state */
	    d = get<directory>(&rgw_fh->variant_type);
	    if (d) {
	      struct timespec ev_ts = ev.ts;
	      lock_guard guard(rgw_fh->mtx);
	      struct timespec d_last_readdir = d->last_readdir;
	      if (unlikely(ev_ts < d_last_readdir)) {
		/* readdir cycle in progress, don't invalidate */
		lsubdout(get_context(), rgw, 15)
		  << "GC: delay expiration for "
		  << rgw_fh->object_name()
		  << " ev.ts=" << ev_ts
		  << " last_readdir=" << d_last_readdir
		  << dendl;
		continue;
	      } else {
		lsubdout(get_context(), rgw, 15)
		  << "GC: expiring "
		  << rgw_fh->object_name()
		  << dendl;
		rgw_fh->clear_state();
		rgw_fh->invalidate();
	      }
	    }
	  rele:
	    unref(rgw_fh);
	  } /* rgw_fh */
	} /* event::type::READDIR */
      } /* ev */
      ve.clear();
    } while (! (stop || shutdown));
  } /* RGWLibFS::gc */

  std::ostream& operator<<(std::ostream &os,
			   RGWFileHandle const &rgw_fh)
  {
    const auto& fhk = rgw_fh.get_key();
    const auto& fh = const_cast<RGWFileHandle&>(rgw_fh).get_fh();
    os << "<RGWFileHandle:";
    os << "addr=" << &rgw_fh << ";";
    switch (fh->fh_type) {
    case RGW_FS_TYPE_DIRECTORY:
	os << "type=DIRECTORY;";
	break;
    case RGW_FS_TYPE_FILE:
	os << "type=FILE;";
	break;
    default:
	os << "type=UNKNOWN;";
	break;
      };
    os << "fid=" << fhk.fh_hk.bucket << ":" << fhk.fh_hk.object << ";";
    os << "name=" << rgw_fh.object_name() << ";";
    os << "refcnt=" << rgw_fh.get_refcnt() << ";";
    os << ">";
    return os;
  }

  RGWFileHandle::~RGWFileHandle() {
    /* !recycle case, handle may STILL be in handle table, BUT
     * the partition lock is not held in this path */
    if (fh_hook.is_linked()) {
      fs->fh_cache.remove(fh.fh_hk.object, this, FHCache::FLAG_LOCK);
    }
    /* cond-unref parent */
    if (parent && (! parent->is_mount())) {
      /* safe because if parent->unref causes its deletion,
       * there are a) by refcnt, no other objects/paths pointing
       * to it and b) by the semantics of valid iteration of
       * fh_lru (observed, e.g., by cohort_lru<T,...>::drain())
       * no unsafe iterators reaching it either--n.b., this constraint
       * is binding oncode which may in future attempt to e.g.,
       * cause the eviction of objects in LRU order */
      (void) get_fs()->unref(parent);
    }
  }

  fh_key RGWFileHandle::make_fhk(const std::string& name)
  {
    std::string tenant = get_fs()->get_user()->user_id.to_str();
    if (depth == 0) {
      /* S3 bucket -- assert mount-at-bucket case reaches here */
      return fh_key(name, name, tenant);
    } else {
      std::string key_name = make_key_name(name.c_str());
      return fh_key(fhk.fh_hk.bucket, key_name.c_str(), tenant);
    }
  }

  void RGWFileHandle::encode_attrs(ceph::buffer::list& ux_key1,
				   ceph::buffer::list& ux_attrs1)
  {
    using ceph::encode;
    fh_key fhk(this->fh.fh_hk);
    encode(fhk, ux_key1);
    encode(*this, ux_attrs1);
  } /* RGWFileHandle::encode_attrs */

  DecodeAttrsResult RGWFileHandle::decode_attrs(const ceph::buffer::list* ux_key1,
                                                const ceph::buffer::list* ux_attrs1)
  {
    using ceph::decode;
    DecodeAttrsResult dar { false, false };
    fh_key fhk;
    auto bl_iter_key1 = ux_key1->cbegin();
    decode(fhk, bl_iter_key1);
    get<0>(dar) = true;

    auto bl_iter_unix1 = ux_attrs1->cbegin();
    decode(*this, bl_iter_unix1);
    if (this->state.version < 2) {
      get<1>(dar) = true;
    }

    return dar;
  } /* RGWFileHandle::decode_attrs */

  bool RGWFileHandle::reclaim() {
    lsubdout(fs->get_context(), rgw, 17)
      << __func__ << " " << *this
      << dendl;
    /* in the non-delete case, handle may still be in handle table */
    if (fh_hook.is_linked()) {
      /* in this case, we are being called from a context which holds
       * the partition lock */
      fs->fh_cache.remove(fh.fh_hk.object, this, FHCache::FLAG_NONE);
    }
    return true;
  } /* RGWFileHandle::reclaim */

  bool RGWFileHandle::has_children() const
  {
    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), *fs->get_user());
    if (unlikely(! is_dir()))
      return false;

    RGWRMdirCheck req(fs->get_context(), &ruser, this);
    int rc = rgwlib.get_fe()->execute_req(&req);
    if (! rc) {
      return req.valid && req.has_children;
    }

    return false;
  }

  std::ostream& operator<<(std::ostream &os,
			   RGWFileHandle::readdir_offset const &offset)
  {
    using boost::get;
    if (unlikely(!! get<uint64_t*>(&offset))) {
      uint64_t* ioff = get<uint64_t*>(offset);
      os << *ioff;
    }
    else
      os << get<const char*>(offset);
    return os;
  }

  int RGWFileHandle::readdir(rgw_readdir_cb rcb, void *cb_arg,
			     readdir_offset offset,
			     bool *eof, uint32_t flags)
  {
    using event = RGWLibFS::event;
    using boost::get;
    int rc = 0;
    struct timespec now;
    CephContext* cct = fs->get_context();
    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), *fs->get_user());

    lsubdout(cct, rgw, 10)
      << __func__ << " readdir called on "
      << object_name()
      << dendl;

    directory* d = get<directory>(&variant_type);
    if (d) {
      (void) clock_gettime(CLOCK_MONOTONIC_COARSE, &now); /* !LOCKED */
      lock_guard guard(mtx);
      d->last_readdir = now;
    }

    bool initial_off;
    char* mk{nullptr};

    if (likely(!! get<const char*>(&offset))) {
      mk = const_cast<char*>(get<const char*>(offset));
      initial_off = !mk;
    } else {
      initial_off = (*get<uint64_t*>(offset) == 0);
    }

    if (is_root()) {
      RGWListBucketsRequest req(cct, &ruser, this, rcb, cb_arg,
				offset);
      rc = rgwlib.get_fe()->execute_req(&req);
      if (! rc) {
	(void) clock_gettime(CLOCK_MONOTONIC_COARSE, &now); /* !LOCKED */
	lock_guard guard(mtx);
	state.atime = now;
	if (initial_off)
	  set_nlink(2);
	inc_nlink(req.d_count);
	*eof = req.eof();
      }
    } else {
      RGWReaddirRequest req(cct, &ruser, this, rcb, cb_arg, offset);
      rc = rgwlib.get_fe()->execute_req(&req);
      if (! rc) {
	(void) clock_gettime(CLOCK_MONOTONIC_COARSE, &now); /* !LOCKED */
	lock_guard guard(mtx);
	state.atime = now;
	if (initial_off)
	  set_nlink(2);
	inc_nlink(req.d_count);
	*eof = req.eof();
      }
    }

    event ev(event::type::READDIR, get_key(), state.atime);
    lock_guard sguard(fs->state.mtx);
    fs->state.push_event(ev);

    lsubdout(fs->get_context(), rgw, 15)
      << __func__
      << " final link count=" << state.nlink
      << dendl;

    return rc;
  } /* RGWFileHandle::readdir */

  int RGWFileHandle::write(uint64_t off, size_t len, size_t *bytes_written,
			   void *buffer)
  {
    using std::get;
    using WriteCompletion = RGWLibFS::WriteCompletion;
    rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), *fs->get_user());

    lock_guard guard(mtx);

    int rc = 0;

    file* f = get<file>(&variant_type);
    if (! f)
      return -EISDIR;

    if (deleted()) {
      lsubdout(fs->get_context(), rgw, 5)
	<< __func__
	<< " write attempted on deleted object "
	<< this->object_name()
	<< dendl;
      /* zap write transaction, if any */
      if (f->write_req) {
	delete f->write_req;
	f->write_req = nullptr;
      }
      return -ESTALE;
    }

    if (! f->write_req) {
      /* guard--we do not support (e.g., COW-backed) partial writes */
      if (off != 0) {
	lsubdout(fs->get_context(), rgw, 5)
	  << __func__
	  << " " << object_name()
	  << " non-0 initial write position " << off
	  << " (mounting with -o sync required)"
	  << dendl;
	return -EIO;
      }

      /* start */
      std::string object_name = relative_object_name();
      f->write_req =
	new RGWWriteRequest(fs->get_context(), &ruser, this,
			    bucket_name(), object_name);
      rc = rgwlib.get_fe()->start_req(f->write_req);
      if (rc < 0) {
	lsubdout(fs->get_context(), rgw, 5)
	  << __func__
	  << this->object_name()
	  << " write start failed " << off
	  << " (" << rc << ")"
	  << dendl;
	/* zap failed write transaction */
	delete f->write_req;
	f->write_req = nullptr;
        return -EIO;
      } else {
	if (stateless_open())  {
	  /* start write timer */
	  f->write_req->timer_id =
	    RGWLibFS::write_timer.add_event(
	      std::chrono::seconds(RGWLibFS::write_completion_interval_s),
	      WriteCompletion(*this));
	}
      }
    }

    int overlap = 0;
    if ((static_cast<off_t>(off) < f->write_req->real_ofs) &&
        ((f->write_req->real_ofs - off) <= len)) {
      overlap = f->write_req->real_ofs - off;
      off = f->write_req->real_ofs;
      buffer = static_cast<char*>(buffer) + overlap;
      len -= overlap;
    }

    buffer::list bl;
    /* XXXX */
#if 0
    bl.push_back(
      buffer::create_static(len, static_cast<char*>(buffer)));
#else
    bl.push_back(
      buffer::copy(static_cast<char*>(buffer), len));
#endif

    f->write_req->put_data(off, bl);
    rc = f->write_req->exec_continue();

    if (rc == 0) {
      size_t min_size = off + len;
      if (min_size > get_size())
	set_size(min_size);
      if (stateless_open()) {
	/* bump write timer */
	RGWLibFS::write_timer.adjust_event(
	  f->write_req->timer_id, std::chrono::seconds(10));
      }
    } else {
      /* continuation failed (e.g., non-contiguous write position) */
      lsubdout(fs->get_context(), rgw, 5)
	<< __func__
	<< object_name()
	<< " failed write at position " << off
	<< " (fails write transaction) "
	<< dendl;
      /* zap failed write transaction */
      delete f->write_req;
      f->write_req = nullptr;
      rc = -EIO;
    }

    *bytes_written = (rc == 0) ? (len + overlap) : 0;
    return rc;
  } /* RGWFileHandle::write */

  int RGWFileHandle::write_finish(uint32_t flags)
  {
    unique_lock guard{mtx, std::defer_lock};
    int rc = 0;

    if (! (flags & FLAG_LOCKED)) {
      guard.lock();
    }

    file* f = get<file>(&variant_type);
    if (f && (f->write_req)) {
      lsubdout(fs->get_context(), rgw, 10)
	<< __func__
	<< " finishing write trans on " << object_name()
	<< dendl;
      rc = rgwlib.get_fe()->finish_req(f->write_req);
      if (! rc) {
	rc = f->write_req->get_ret();
      }
      delete f->write_req;
      f->write_req = nullptr;
    }

    return rc;
  } /* RGWFileHandle::write_finish */

  int RGWFileHandle::close()
  {
    lock_guard guard(mtx);

    int rc = write_finish(FLAG_LOCKED);

    flags &= ~FLAG_OPEN;
    flags &= ~FLAG_STATELESS_OPEN;

    return rc;
  } /* RGWFileHandle::close */

  RGWFileHandle::file::~file()
  {
    delete write_req;
  }

  void RGWFileHandle::clear_state()
  {
    directory* d = get<directory>(&variant_type);
    if (d) {
      state.nlink = 2;
      d->last_marker = rgw_obj_key{};
    }
  }

  void RGWFileHandle::advance_mtime(uint32_t flags) {
    /* intended for use on directories, fast-forward mtime so as to
     * ensure a new, higher value for the change attribute */
    unique_lock uniq(mtx, std::defer_lock);
    if (likely(! (flags & RGWFileHandle::FLAG_LOCKED))) {
      uniq.lock();
    }

    /* advance mtime only if stored mtime is older than the
     * configured namespace expiration */
    auto now = real_clock::now();
    auto cmptime = state.mtime;
    cmptime.tv_sec +=
      fs->get_context()->_conf->rgw_nfs_namespace_expire_secs;
    if (cmptime < real_clock::to_timespec(now)) {
      /* sets ctime as well as mtime, to avoid masking updates should
       * ctime inexplicably hold a higher value */
      set_times(now);
    }
  }

  void RGWFileHandle::invalidate() {
    RGWLibFS *fs = get_fs();
    if (fs->invalidate_cb) {
      fs->invalidate_cb(fs->invalidate_arg, get_key().fh_hk);
    }
  }

  int RGWWriteRequest::exec_start() {
    struct req_state* s = get_state();

    auto compression_type =
      get_store()->svc()->zone->get_zone_params().get_compression_type(
	s->bucket_info.placement_rule);

    /* not obviously supportable */
    ceph_assert(! dlo_manifest);
    ceph_assert(! slo_info);

    perfcounter->inc(l_rgw_put);
    op_ret = -EINVAL;
    rgw_obj obj{s->bucket, s->object};

    if (s->object.empty()) {
      ldout(s->cct, 0) << __func__ << " called on empty object" << dendl;
      goto done;
    }

    op_ret = get_params();
    if (op_ret < 0)
      goto done;

    op_ret = get_system_versioning_params(s, &olh_epoch, &version_id);
    if (op_ret < 0) {
      goto done;
    }

    /* user-supplied MD5 check skipped (not supplied) */
    /* early quota check skipped--we don't have size yet */
    /* skipping user-supplied etag--we might have one in future, but
     * like data it and other attrs would arrive after open */

    aio.emplace(s->cct->_conf->rgw_put_obj_min_window_size);

    if (s->bucket_info.versioning_enabled()) {
      if (!version_id.empty()) {
        obj.key.set_instance(version_id);
      } else {
        get_store()->getRados()->gen_rand_obj_instance_name(&obj);
        version_id = obj.key.instance;
      }
    }
    processor.emplace(&*aio, get_store(), s->bucket_info,
                      &s->dest_placement,
                      s->bucket_owner.get_id(),
                      *static_cast<RGWObjectCtx *>(s->obj_ctx),
                      obj, olh_epoch, s->req_id, this, s->yield);

    op_ret = processor->prepare(s->yield);
    if (op_ret < 0) {
      ldout(s->cct, 20) << "processor->prepare() returned ret=" << op_ret
			<< dendl;
      goto done;
    }
    filter = &*processor;
    if (compression_type != "none") {
      plugin = Compressor::create(s->cct, compression_type);
      if (! plugin) {
        ldout(s->cct, 1) << "Cannot load plugin for rgw_compression_type "
                         << compression_type << dendl;
      } else {
        compressor.emplace(s->cct, plugin, filter);
        filter = &*compressor;
      }
    }

  done:
    return op_ret;
  } /* exec_start */

  int RGWWriteRequest::exec_continue()
  {
    struct req_state* s = get_state();
    op_ret = 0;

    /* check guards (e.g., contig write) */
    if (eio) {
      ldout(s->cct, 5)
        << " chunks arrived in wrong order"
        << " (mounting with -o sync required)"
        << dendl;
      return -EIO;
    }

    op_ret = get_store()->getRados()->check_quota(s->bucket_owner.get_id(), s->bucket,
                                      user_quota, bucket_quota, real_ofs, true);
    /* max_size exceed */
    if (op_ret < 0)
      return -EIO;

    size_t len = data.length();
    if (! len)
      return 0;

    hash.Update((const unsigned char *)data.c_str(), data.length());
    op_ret = filter->process(std::move(data), ofs);
    if (op_ret < 0) {
      goto done;
    }
    bytes_written += len;

  done:
    return op_ret;
  } /* exec_continue */

  int RGWWriteRequest::exec_finish()
  {
    buffer::list bl, aclbl, ux_key, ux_attrs;
    map<string, string>::iterator iter;
    char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
    unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
    struct req_state* s = get_state();

    size_t osize = rgw_fh->get_size();
    struct timespec octime = rgw_fh->get_ctime();
    struct timespec omtime = rgw_fh->get_mtime();
    real_time appx_t = real_clock::now();

    s->obj_size = bytes_written;
    perfcounter->inc(l_rgw_put_b, s->obj_size);

    // flush data in filters
    op_ret = filter->process({}, s->obj_size);
    if (op_ret < 0) {
      goto done;
    }

    op_ret = get_store()->getRados()->check_quota(s->bucket_owner.get_id(), s->bucket,
				      user_quota, bucket_quota, s->obj_size, true);
    /* max_size exceed */
    if (op_ret < 0) {
      goto done;
    }

    hash.Final(m);

    if (compressor && compressor->is_compressed()) {
      bufferlist tmp;
      RGWCompressionInfo cs_info;
      cs_info.compression_type = plugin->get_type_name();
      cs_info.orig_size = s->obj_size;
      cs_info.blocks = std::move(compressor->get_compression_blocks());
      encode(cs_info, tmp);
      attrs[RGW_ATTR_COMPRESSION] = tmp;
      ldout(s->cct, 20) << "storing " << RGW_ATTR_COMPRESSION
			<< " with type=" << cs_info.compression_type
			<< ", orig_size=" << cs_info.orig_size
			<< ", blocks=" << cs_info.blocks.size() << dendl;
    }

    buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);
    etag = calc_md5;

    bl.append(etag.c_str(), etag.size() + 1);
    emplace_attr(RGW_ATTR_ETAG, std::move(bl));

    policy.encode(aclbl);
    emplace_attr(RGW_ATTR_ACL, std::move(aclbl));

    /* unix attrs */
    rgw_fh->set_mtime(real_clock::to_timespec(appx_t));
    rgw_fh->set_ctime(real_clock::to_timespec(appx_t));
    rgw_fh->set_size(bytes_written);
    rgw_fh->encode_attrs(ux_key, ux_attrs);

    emplace_attr(RGW_ATTR_UNIX_KEY1, std::move(ux_key));
    emplace_attr(RGW_ATTR_UNIX1, std::move(ux_attrs));

    for (iter = s->generic_attrs.begin(); iter != s->generic_attrs.end();
	 ++iter) {
      buffer::list& attrbl = attrs[iter->first];
      const string& val = iter->second;
      attrbl.append(val.c_str(), val.size() + 1);
    }

    op_ret = rgw_get_request_metadata(s->cct, s->info, attrs);
    if (op_ret < 0) {
      goto done;
    }
    encode_delete_at_attr(delete_at, attrs);

    /* Add a custom metadata to expose the information whether an object
     * is an SLO or not. Appending the attribute must be performed AFTER
     * processing any input from user in order to prohibit overwriting. */
    if (unlikely(!! slo_info)) {
      buffer::list slo_userindicator_bl;
      using ceph::encode;
      encode("True", slo_userindicator_bl);
      emplace_attr(RGW_ATTR_SLO_UINDICATOR, std::move(slo_userindicator_bl));
    }

    op_ret = processor->complete(s->obj_size, etag, &mtime, real_time(), attrs,
                                 (delete_at ? *delete_at : real_time()),
                                if_match, if_nomatch, nullptr, nullptr, nullptr,
                                s->yield);
    if (op_ret != 0) {
      /* revert attr updates */
      rgw_fh->set_mtime(omtime);
      rgw_fh->set_ctime(octime);
      rgw_fh->set_size(osize);
    }

  done:
    perfcounter->tinc(l_rgw_put_lat, s->time_elapsed());
    return op_ret;
  } /* exec_finish */

} /* namespace rgw */

/* librgw */
extern "C" {

void rgwfile_version(int *major, int *minor, int *extra)
{
  if (major)
    *major = LIBRGW_FILE_VER_MAJOR;
  if (minor)
    *minor = LIBRGW_FILE_VER_MINOR;
  if (extra)
    *extra = LIBRGW_FILE_VER_EXTRA;
}

/*
 attach rgw namespace
*/
  int rgw_mount(librgw_t rgw, const char *uid, const char *acc_key,
		const char *sec_key, struct rgw_fs **rgw_fs,
		uint32_t flags)
{
  int rc = 0;

  /* stash access data for "mount" */
  RGWLibFS* new_fs = new RGWLibFS(static_cast<CephContext*>(rgw), uid, acc_key,
				  sec_key, "/");
  ceph_assert(new_fs);

  rc = new_fs->authorize(rgwlib.get_store());
  if (rc != 0) {
    delete new_fs;
    return -EINVAL;
  }

  /* register fs for shared gc */
  rgwlib.get_fe()->get_process()->register_fs(new_fs);

  struct rgw_fs *fs = new_fs->get_fs();
  fs->rgw = rgw;

  /* XXX we no longer assume "/" is unique, but we aren't tracking the
   * roots atm */

  *rgw_fs = fs;

  return 0;
}

int rgw_mount2(librgw_t rgw, const char *uid, const char *acc_key,
               const char *sec_key, const char *root, struct rgw_fs **rgw_fs,
               uint32_t flags)
{
  int rc = 0;

  /* stash access data for "mount" */
  RGWLibFS* new_fs = new RGWLibFS(static_cast<CephContext*>(rgw), uid, acc_key,
				  sec_key, root);
  ceph_assert(new_fs);

  rc = new_fs->authorize(rgwlib.get_store());
  if (rc != 0) {
    delete new_fs;
    return -EINVAL;
  }

  /* register fs for shared gc */
  rgwlib.get_fe()->get_process()->register_fs(new_fs);

  struct rgw_fs *fs = new_fs->get_fs();
  fs->rgw = rgw;

  /* XXX we no longer assume "/" is unique, but we aren't tracking the
   * roots atm */

  *rgw_fs = fs;

  return 0;
}

/*
 register invalidate callbacks
*/
int rgw_register_invalidate(struct rgw_fs *rgw_fs, rgw_fh_callback_t cb,
			    void *arg, uint32_t flags)

{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  return fs->register_invalidate(cb, arg, flags);
}

/*
 detach rgw namespace
*/
int rgw_umount(struct rgw_fs *rgw_fs, uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  fs->close();
  return 0;
}

/*
  get filesystem attributes
*/
int rgw_statfs(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh,
	       struct rgw_statvfs *vfs_st, uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  struct rados_cluster_stat_t stats;
  rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), *fs->get_user());

  RGWGetClusterStatReq req(fs->get_context(), &ruser, stats);
  int rc = rgwlib.get_fe()->execute_req(&req);
  if (rc < 0) {
    lderr(fs->get_context()) << "ERROR: getting total cluster usage"
                             << cpp_strerror(-rc) << dendl;
    return rc;
  }

  //Set block size to 1M.
  constexpr uint32_t CEPH_BLOCK_SHIFT = 20;
  vfs_st->f_bsize = 1 << CEPH_BLOCK_SHIFT;
  vfs_st->f_frsize = 1 << CEPH_BLOCK_SHIFT;
  vfs_st->f_blocks = stats.kb >> (CEPH_BLOCK_SHIFT - 10);
  vfs_st->f_bfree = stats.kb_avail >> (CEPH_BLOCK_SHIFT - 10);
  vfs_st->f_bavail = stats.kb_avail >> (CEPH_BLOCK_SHIFT - 10);
  vfs_st->f_files = stats.num_objects;
  vfs_st->f_ffree = -1;
  vfs_st->f_fsid[0] = fs->get_fsid();
  vfs_st->f_fsid[1] = fs->get_fsid();
  vfs_st->f_flag = 0;
  vfs_st->f_namemax = 4096;
  return 0;
}

/*
  generic create -- create an empty regular file
*/
int rgw_create(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
	       const char *name, struct stat *st, uint32_t mask,
	       struct rgw_file_handle **fh, uint32_t posix_flags,
	       uint32_t flags)
{
  using std::get;

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* parent = get_rgwfh(parent_fh);

  if ((! parent) ||
      (parent->is_root()) ||
      (parent->is_file())) {
    /* bad parent */
    return -EINVAL;
  }

  MkObjResult fhr = fs->create(parent, name, st, mask, flags);
  RGWFileHandle *nfh = get<0>(fhr); // nullptr if !success

  if (nfh)
    *fh = nfh->get_fh();

  return get<1>(fhr);
} /* rgw_create */

/*
  create a symbolic link
 */
int rgw_symlink(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
        const char *name, const char *link_path, struct stat *st, uint32_t mask,
        struct rgw_file_handle **fh, uint32_t posix_flags,
        uint32_t flags)
{
  using std::get;

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* parent = get_rgwfh(parent_fh);

  if ((! parent) ||
      (parent->is_root()) ||
      (parent->is_file())) {
    /* bad parent */
    return -EINVAL;
  }

  MkObjResult fhr = fs->symlink(parent, name, link_path, st, mask, flags);
  RGWFileHandle *nfh = get<0>(fhr); // nullptr if !success

  if (nfh)
    *fh = nfh->get_fh();

  return get<1>(fhr);
} /* rgw_symlink */

/*
  create a new directory
*/
int rgw_mkdir(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh,
	      const char *name, struct stat *st, uint32_t mask,
	      struct rgw_file_handle **fh, uint32_t flags)
{
  using std::get;

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* parent = get_rgwfh(parent_fh);

  if (! parent) {
    /* bad parent */
    return -EINVAL;
  }

  MkObjResult fhr = fs->mkdir(parent, name, st, mask, flags);
  RGWFileHandle *nfh = get<0>(fhr); // nullptr if !success

  if (nfh)
    *fh = nfh->get_fh();

  return get<1>(fhr);
} /* rgw_mkdir */

/*
  rename object
*/
int rgw_rename(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *src, const char* src_name,
	       struct rgw_file_handle *dst, const char* dst_name,
	       uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);

  RGWFileHandle* src_fh = get_rgwfh(src);
  RGWFileHandle* dst_fh = get_rgwfh(dst);

  return fs->rename(src_fh, dst_fh, src_name, dst_name);
}

/*
  remove file or directory
*/
int rgw_unlink(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
	       const char *name, uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* parent = get_rgwfh(parent_fh);

  return fs->unlink(parent, name);
}

/*
  lookup object by name (POSIX style)
*/
int rgw_lookup(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh, const char* path,
	      struct rgw_file_handle **fh,
	      struct stat *st, uint32_t mask, uint32_t flags)
{
  //CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);

  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if ((! parent) ||
      (! parent->is_dir())) {
    /* bad parent */
    return -EINVAL;
  }

  RGWFileHandle* rgw_fh;
  LookupFHResult fhr;

  if (parent->is_root()) {
    /* special: parent lookup--note lack of ref()! */
    if (unlikely((strcmp(path, "..") == 0) ||
		 (strcmp(path, "/") == 0))) {
      rgw_fh = parent;
    } else {
      RGWLibFS::BucketStats bstat;
      fhr = fs->stat_bucket(parent, path, bstat, RGWFileHandle::FLAG_NONE);
      rgw_fh = get<0>(fhr);
      if (! rgw_fh)
	return -ENOENT;
    }
  } else {
    /* special: after readdir--note extra ref()! */
    if (unlikely((strcmp(path, "..") == 0))) {
      rgw_fh = parent;
      lsubdout(fs->get_context(), rgw, 17)
	<< __func__ << " BANG"<< *rgw_fh
	<< dendl;
      fs->ref(rgw_fh);
    } else {
      enum rgw_fh_type fh_type = fh_type_of(flags);

      uint32_t sl_flags = (flags & RGW_LOOKUP_FLAG_RCB)
	? RGWFileHandle::FLAG_NONE
	: RGWFileHandle::FLAG_EXACT_MATCH;

      bool fast_attrs= fs->get_context()->_conf->rgw_nfs_s3_fast_attrs;

      if ((flags & RGW_LOOKUP_FLAG_RCB) && fast_attrs) {
	/* FAKE STAT--this should mean, interpolate special
	 * owner, group, and perms masks */
	fhr = fs->fake_leaf(parent, path, fh_type, st, mask, sl_flags);
      } else {
	if ((fh_type == RGW_FS_TYPE_DIRECTORY) && fast_attrs) {
	  /* trust cached dir, if present */
	  fhr = fs->lookup_fh(parent, path, RGWFileHandle::FLAG_DIRECTORY);
	  if (get<0>(fhr)) {
	    rgw_fh = get<0>(fhr);
	    goto done;
	  }
	}
	fhr = fs->stat_leaf(parent, path, fh_type, sl_flags);
      }
      if (! get<0>(fhr)) {
	if (! (flags & RGW_LOOKUP_FLAG_CREATE))
	  return -ENOENT;
	else
	  fhr = fs->lookup_fh(parent, path, RGWFileHandle::FLAG_CREATE);
      }
      rgw_fh = get<0>(fhr);
    }
  } /* !root */

done:
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
    return -ENOENT;
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
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  lsubdout(fs->get_context(), rgw, 17)
    << __func__ << " " << *rgw_fh
    << dendl;

  fs->unref(rgw_fh);
  return 0;
}

/*
   get unix attributes for object
*/
int rgw_getattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st, uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  return fs->getattr(rgw_fh, st);
}

/*
  set unix attributes for object
*/
int rgw_setattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st,
		uint32_t mask, uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  return fs->setattr(rgw_fh, st, mask, flags);
}

/*
   truncate file
*/
int rgw_truncate(struct rgw_fs *rgw_fs,
		 struct rgw_file_handle *fh, uint64_t size, uint32_t flags)
{
  return 0;
}

/*
   open file
*/
int rgw_open(struct rgw_fs *rgw_fs,
	     struct rgw_file_handle *fh, uint32_t posix_flags, uint32_t flags)
{
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  /* XXX
   * need to track specific opens--at least read opens and
   * a write open;  we need to know when a write open is returned,
   * that closes a write transaction
   *
   * for now, we will support single-open only, it's preferable to
   * anything we can otherwise do without access to the NFS state
   */
  if (! rgw_fh->is_file())
    return -EISDIR;

  return rgw_fh->open(flags);
}

/*
   close file
*/
int rgw_close(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);
  int rc = rgw_fh->close(/* XXX */);

  if (flags & RGW_CLOSE_FLAG_RELE)
    fs->unref(rgw_fh);

  return rc;
}

int rgw_readdir(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *parent_fh, uint64_t *offset,
		rgw_readdir_cb rcb, void *cb_arg, bool *eof,
		uint32_t flags)
{
  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if (! parent) {
    /* bad parent */
    return -EINVAL;
  }

  lsubdout(parent->get_fs()->get_context(), rgw, 15)
    << __func__
    << " offset=" << *offset
    << dendl;

  if ((*offset == 0) &&
      (flags & RGW_READDIR_FLAG_DOTDOT)) {
    /* send '.' and '..' with their NFS-defined offsets */
    rcb(".", cb_arg, 1, nullptr, 0, RGW_LOOKUP_FLAG_DIR);
    rcb("..", cb_arg, 2, nullptr, 0, RGW_LOOKUP_FLAG_DIR);
  }

  int rc = parent->readdir(rcb, cb_arg, offset, eof, flags);
  return rc;
} /* rgw_readdir */

/* enumeration continuing from name */
int rgw_readdir2(struct rgw_fs *rgw_fs,
		 struct rgw_file_handle *parent_fh, const char *name,
		 rgw_readdir_cb rcb, void *cb_arg, bool *eof,
		 uint32_t flags)
{
  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if (! parent) {
    /* bad parent */
    return -EINVAL;
  }

  lsubdout(parent->get_fs()->get_context(), rgw, 15)
    << __func__
    << " offset=" << ((name) ? name : "(nil)")
    << dendl;

  if ((! name) &&
      (flags & RGW_READDIR_FLAG_DOTDOT)) {
    /* send '.' and '..' with their NFS-defined offsets */
    rcb(".", cb_arg, 1, nullptr, 0, RGW_LOOKUP_FLAG_DIR);
    rcb("..", cb_arg, 2, nullptr, 0, RGW_LOOKUP_FLAG_DIR);
  }

  int rc = parent->readdir(rcb, cb_arg, name, eof, flags);
  return rc;
} /* rgw_readdir2 */

/* project offset of dirent name */
int rgw_dirent_offset(struct rgw_fs *rgw_fs,
		      struct rgw_file_handle *parent_fh,
		      const char *name, int64_t *offset,
		      uint32_t flags)
{
  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if ((! parent)) {
    /* bad parent */
    return -EINVAL;
  }
  std::string sname{name};
  int rc = parent->offset_of(sname, offset, flags);
  return rc;
}

/*
   read data from file
*/
int rgw_read(struct rgw_fs *rgw_fs,
	     struct rgw_file_handle *fh, uint64_t offset,
	     size_t length, size_t *bytes_read, void *buffer,
	     uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  return fs->read(rgw_fh, offset, length, bytes_read, buffer, flags);
}

/*
   read symbolic link
*/
int rgw_readlink(struct rgw_fs *rgw_fs,
	     struct rgw_file_handle *fh, uint64_t offset,
	     size_t length, size_t *bytes_read, void *buffer,
	     uint32_t flags)
{
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  return fs->readlink(rgw_fh, offset, length, bytes_read, buffer, flags);
}

/*
   write data to file
*/
int rgw_write(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, uint64_t offset,
	      size_t length, size_t *bytes_written, void *buffer,
	      uint32_t flags)
{
  RGWFileHandle* rgw_fh = get_rgwfh(fh);
  int rc;

  *bytes_written = 0;

  if (! rgw_fh->is_file())
    return -EISDIR;

  if (! rgw_fh->is_open()) {
    if (flags & RGW_OPEN_FLAG_V3) {
      rc = rgw_fh->open(flags);
      if (!! rc)
	return rc;
    } else
      return -EPERM;
  }

  rc = rgw_fh->write(offset, length, bytes_written, buffer);

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
    bl = std::move(_bl);
  }

  struct rgw_vio* get_vio() { return vio; }

  const auto& buffers() { return bl.buffers(); }

  unsigned /* XXX */ length() { return bl.length(); }

};

void rgw_readv_rele(struct rgw_uio *uio, uint32_t flags)
{
  RGWReadV* rdv = static_cast<RGWReadV*>(uio->uio_p1);
  rdv->~RGWReadV();
  ::operator delete(rdv);
}

int rgw_readv(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, rgw_uio *uio, uint32_t flags)
{
#if 0 /* XXX */
  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  if (! rgw_fh->is_file())
    return -EINVAL;

  int rc = 0;

  buffer::list bl;
  RGWGetObjRequest req(cct, fs->get_user(), rgw_fh->bucket_name(),
		      rgw_fh->object_name(), uio->uio_offset, uio->uio_resid,
		      bl);
  req.do_hexdump = false;

  rc = rgwlib.get_fe()->execute_req(&req);

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
#else
  return 0;
#endif
}

/*
   write data to file (vector)
*/
int rgw_writev(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	      rgw_uio *uio, uint32_t flags)
{

  return -ENOTSUP;

  CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* rgw_fh = get_rgwfh(fh);
  rgw::sal::RGWRadosUser ruser(rgwlib.get_store(), *fs->get_user());

  if (! rgw_fh->is_file())
    return -EINVAL;

  buffer::list bl;
  for (unsigned int ix = 0; ix < uio->uio_cnt; ++ix) {
    rgw_vio *vio = &(uio->uio_vio[ix]);
    bl.push_back(
      buffer::create_static(vio->vio_len,
			    static_cast<char*>(vio->vio_base)));
  }

  std::string oname = rgw_fh->relative_object_name();
  RGWPutObjRequest req(cct, &ruser, rgw_fh->bucket_name(),
		       oname, bl);

  int rc = rgwlib.get_fe()->execute_req(&req);

  /* XXX update size (in request) */

  return rc;
}

/*
   sync written data
*/
int rgw_fsync(struct rgw_fs *rgw_fs, struct rgw_file_handle *handle,
	      uint32_t flags)
{
  return 0;
}

int rgw_commit(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	       uint64_t offset, uint64_t length, uint32_t flags)
{
  RGWFileHandle* rgw_fh = get_rgwfh(fh);

  return rgw_fh->commit(offset, length, RGWFileHandle::FLAG_NONE);
}

} /* extern "C" */
