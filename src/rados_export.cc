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

#include "rados_sync.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "include/rados/librados.hpp"

#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <inttypes.h>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "include/compat.h"
#include "common/xattr.h"

using namespace librados;

class ExportLocalFileWQ : public RadosSyncWQ {
public:
  ExportLocalFileWQ(IoCtxDistributor *io_ctx_dist, time_t ti,
		    ThreadPool *tp, ExportDir *export_dir, bool force)
    : RadosSyncWQ(io_ctx_dist, ti, 0, tp),
      m_export_dir(export_dir),
      m_force(force)
  {
  }
private:
  void _process(std::string *s) {
    IoCtx &io_ctx(m_io_ctx_dist->get_ioctx());
    int flags = 0;
    auto_ptr <BackedUpObject> sobj;
    auto_ptr <BackedUpObject> dobj;
    const std::string &rados_name(*s);
    std::list < std::string > only_in_a;
    std::list < std::string > only_in_b;
    std::list < std::string > diff;
    int ret = BackedUpObject::from_rados(io_ctx, rados_name.c_str(), sobj);
    if (ret) {
      cerr << ERR_PREFIX << "couldn't get '" << rados_name << "' from rados: error "
	   << ret << std::endl;
      _exit(ret);
    }
    std::string obj_path(sobj->get_fs_path(m_export_dir));
    if (m_force) {
      flags |= (CHANGED_CONTENTS | CHANGED_XATTRS);
    }
    else {
      ret = BackedUpObject::from_path(obj_path.c_str(), dobj);
      if (ret == ENOENT) {
	sobj->get_xattrs(only_in_a);
	flags |= CHANGED_CONTENTS;
      }
      else if (ret) {
	cerr << ERR_PREFIX << "BackedUpObject::from_path returned "
	     << ret << std::endl;
	_exit(ret);
      }
      else {
	sobj->xattr_diff(dobj.get(), only_in_a, only_in_b, diff);
	if ((sobj->get_rados_size() == dobj->get_rados_size()) &&
	    (sobj->get_mtime() == dobj->get_mtime())) {
	  flags |= CHANGED_CONTENTS;
	}
      }
    }
    if (flags & CHANGED_CONTENTS) {
      ret = sobj->download(io_ctx, obj_path.c_str());
      if (ret) {
	cerr << ERR_PREFIX << "download error: " << ret << std::endl;
	_exit(ret);
      }
    }
    diff.splice(diff.begin(), only_in_a);
    for (std::list < std::string >::const_iterator x = diff.begin();
	 x != diff.end(); ++x) {
      flags |= CHANGED_XATTRS;
      const Xattr *xattr = sobj->get_xattr(*x);
      if (xattr == NULL) {
	cerr << ERR_PREFIX << "internal error on line: " << __LINE__ << std::endl;
	_exit(ret);
      }
      std::string xattr_fs_name(USER_XATTR_PREFIX);
      xattr_fs_name += x->c_str();
      ret = ceph_os_setxattr(obj_path.c_str(), xattr_fs_name.c_str(),
		     xattr->data, xattr->len);
      if (ret) {
	ret = errno;
	cerr << ERR_PREFIX << "setxattr error: " << cpp_strerror(ret) << std::endl;
	_exit(ret);
      }
    }
    for (std::list < std::string >::const_iterator x = only_in_b.begin();
	 x != only_in_b.end(); ++x) {
      flags |= CHANGED_XATTRS;
      ret = ceph_os_removexattr(obj_path.c_str(), x->c_str());
      if (ret) {
	ret = errno;
	cerr << ERR_PREFIX << "removexattr error: " << cpp_strerror(ret) << std::endl;
	_exit(ret);
      }
    }
    if (m_force) {
      cout << "[force]        " << rados_name << std::endl;
    }
    else if (flags & CHANGED_CONTENTS) {
      cout << "[exported]     " << rados_name << std::endl;
    }
    else if (flags & CHANGED_XATTRS) {
      cout << "[xattr]        " << rados_name << std::endl;
    }
  }
  ExportDir *m_export_dir;
  bool m_force;
};

class ExportValidateExistingWQ : public RadosSyncWQ {
public:
  ExportValidateExistingWQ(IoCtxDistributor *io_ctx_dist, time_t ti,
			   ThreadPool *tp, const char *dir_name)
    : RadosSyncWQ(io_ctx_dist, ti, 0, tp),
      m_dir_name(dir_name)
  {
  }
private:
  void _process(std::string *s) {
    IoCtx &io_ctx(m_io_ctx_dist->get_ioctx());
    auto_ptr <BackedUpObject> lobj;
    const std::string &local_name(*s);
    int ret = BackedUpObject::from_file(local_name.c_str(), m_dir_name, lobj);
    if (ret) {
      cout << ERR_PREFIX << "BackedUpObject::from_file: delete loop: "
	   << "got error " << ret << std::endl;
      _exit(ret);
    }
    auto_ptr <BackedUpObject> robj;
    ret = BackedUpObject::from_rados(io_ctx, lobj->get_rados_name(), robj);
    if (ret == -ENOENT) {
      // The entry doesn't exist on the remote server; delete it locally
      char path[strlen(m_dir_name) + local_name.size() + 2];
      snprintf(path, sizeof(path), "%s/%s", m_dir_name, local_name.c_str());
      if (unlink(path)) {
	ret = errno;
	cerr << ERR_PREFIX << "error unlinking '" << path << "': "
	     << cpp_strerror(ret) << std::endl;
	_exit(ret);
      }
      cout << "[deleted]      " << "removed '" << local_name << "'" << std::endl;
    }
    else if (ret) {
      cerr << ERR_PREFIX << "BackedUpObject::from_rados: delete loop: "
	   << "got error " << ret << std::endl;
      _exit(ret);
    }
  }
  const char *m_dir_name;
};

int do_rados_export(ThreadPool *tp, IoCtx& io_ctx,
      IoCtxDistributor *io_ctx_dist, const char *dir_name,
      bool create, bool force, bool delete_after)
{
  librados::ObjectIterator oi = io_ctx.objects_begin();
  librados::ObjectIterator oi_end = io_ctx.objects_end();
  auto_ptr <ExportDir> export_dir;
  export_dir.reset(ExportDir::create_for_writing(dir_name, 1, create));
  if (!export_dir.get())
    return -EIO;
  ExportLocalFileWQ export_object_wq(io_ctx_dist, time(NULL),
				     tp, export_dir.get(), force);
  for (; oi != oi_end; ++oi) {
    export_object_wq.queue(new std::string((*oi).first));
  }
  export_object_wq.drain();

  if (delete_after) {
    ExportValidateExistingWQ export_val_wq(io_ctx_dist, time(NULL),
					   tp, dir_name);
    DirHolder dh;
    int err = dh.opendir(dir_name);
    if (err) {
      cerr << ERR_PREFIX << "opendir(" << dir_name << ") error: "
	   << cpp_strerror(err) << std::endl;
      return err;
    }
    while (true) {
      struct dirent *de = readdir(dh.dp);
      if (!de)
	break;
      if ((strcmp(de->d_name, ".") == 0) || (strcmp(de->d_name, "..") == 0))
	continue;
      if (is_suffix(de->d_name, RADOS_SYNC_TMP_SUFFIX)) {
	char path[strlen(dir_name) + strlen(de->d_name) + 2];
	snprintf(path, sizeof(path), "%s/%s", dir_name, de->d_name);
	if (unlink(path)) {
	  int ret = errno;
	  cerr << ERR_PREFIX << "error unlinking temporary file '" << path << "': "
	       << cpp_strerror(ret) << std::endl;
	  return ret;
	}
	cout << "[deleted]      " << "removed temporary file '" << de->d_name << "'" << std::endl;
	continue;
      }
      export_val_wq.queue(new std::string(de->d_name));
    }
    export_val_wq.drain();
  }
  cout << "[done]" << std::endl;
  return 0;
}
