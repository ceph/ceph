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
#include "include/int_types.h"

#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "rados_sync.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "include/rados/librados.hpp"

using namespace librados;
using std::auto_ptr;

class ImportLocalFileWQ : public RadosSyncWQ {
public:
  ImportLocalFileWQ(const char *dir_name, bool force,
		    IoCtxDistributor *io_ctx_dist, time_t ti, ThreadPool *tp)
    : RadosSyncWQ(io_ctx_dist, ti, 0, tp),
      m_dir_name(dir_name),
      m_force(force)
  {
  }
private:
  void _process(std::string *s) {
    IoCtx &io_ctx(m_io_ctx_dist->get_ioctx());
    const std::string &local_name(*s);
    auto_ptr <BackedUpObject> sobj;
    auto_ptr <BackedUpObject> dobj;
    std::list < std::string > only_in_a;
    std::list < std::string > only_in_b;
    std::list < std::string > diff;
    int flags = 0;

    int ret = BackedUpObject::from_file(local_name.c_str(),
					m_dir_name.c_str(), sobj);
    if (ret) {
      cerr << ERR_PREFIX << "BackedUpObject::from_file: got error "
	   << ret << std::endl;
      _exit(ret);
    }
    const char *rados_name(sobj->get_rados_name());
    if (m_force) {
      flags |= (CHANGED_CONTENTS | CHANGED_XATTRS);
    }
    else {
      ret = BackedUpObject::from_rados(io_ctx, rados_name, dobj);
      if (ret == -ENOENT) {
	flags |= CHANGED_CONTENTS;
	sobj->get_xattrs(only_in_a);
      }
      else if (ret) {
	cerr << ERR_PREFIX << "BackedUpObject::from_rados returned "
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
      ret = sobj->upload(io_ctx, local_name.c_str(), m_dir_name.c_str());
      if (ret) {
	cerr << ERR_PREFIX << "upload error: " << ret << std::endl;
	_exit(ret);
      }
    }
    for (std::list < std::string >::const_iterator x = only_in_a.begin();
	 x != only_in_a.end(); ++x) {
      flags |= CHANGED_XATTRS;
      const Xattr *xattr = sobj->get_xattr(*x);
      if (xattr == NULL) {
	cerr << ERR_PREFIX << "internal error on line: " << __LINE__ << std::endl;
	_exit(ret);
      }
      bufferlist bl;
      bl.append(xattr->data, xattr->len);
      ret = io_ctx.setxattr(rados_name, x->c_str(), bl);
      if (ret < 0) {
	ret = errno;
	cerr << ERR_PREFIX << "io_ctx.setxattr(rados_name='" << rados_name
	     << "', xattr_name='" << x->c_str() << "'): " << cpp_strerror(ret)
	     << std::endl;
	_exit(ret);
      }
    }
    for (std::list < std::string >::const_iterator x = diff.begin();
	 x != diff.end(); ++x) {
      flags |= CHANGED_XATTRS;
      const Xattr *xattr = sobj->get_xattr(*x);
      if (xattr == NULL) {
	cerr << ERR_PREFIX << "internal error on line: " << __LINE__ << std::endl;
	_exit(ret);
      }
      bufferlist bl;
      bl.append(xattr->data, xattr->len);
      ret = io_ctx.rmxattr(rados_name, x->c_str());
      if (ret < 0) {
	cerr << ERR_PREFIX << "io_ctx.rmxattr error2: " << cpp_strerror(ret)
	     << std::endl;
	_exit(ret);
      }
      ret = io_ctx.setxattr(rados_name, x->c_str(), bl);
      if (ret < 0) {
	ret = errno;
	cerr << ERR_PREFIX << "io_ctx.setxattr(rados_name='" << rados_name
	     << "', xattr='" << x->c_str() << "'): " << cpp_strerror(ret) << std::endl;
	_exit(ret);
      }
    }
    for (std::list < std::string >::const_iterator x = only_in_b.begin();
	 x != only_in_b.end(); ++x) {
      flags |= CHANGED_XATTRS;
      ret = io_ctx.rmxattr(rados_name, x->c_str());
      if (ret < 0) {
	ret = errno;
	cerr << ERR_PREFIX << "rmxattr error3: " << cpp_strerror(ret) << std::endl;
	_exit(ret);
      }
    }
    if (m_force) {
      cout << "[force]        " << rados_name << std::endl;
    }
    else if (flags & CHANGED_CONTENTS) {
      cout << "[imported]     " << rados_name << std::endl;
    }
    else if (flags & CHANGED_XATTRS) {
      cout << "[xattr]        " << rados_name << std::endl;
    }
  }
  std::string m_dir_name;
  bool m_force;
};

class ImportValidateExistingWQ : public RadosSyncWQ {
public:
  ImportValidateExistingWQ(ExportDir *export_dir,
		 IoCtxDistributor *io_ctx_dist, time_t ti, ThreadPool *tp)
    : RadosSyncWQ(io_ctx_dist, ti, 0, tp),
      m_export_dir(export_dir)
  {
  }
private:
  void _process(std::string *s) {
    IoCtx &io_ctx(m_io_ctx_dist->get_ioctx());
    const std::string &rados_name(*s);
    auto_ptr <BackedUpObject> robj;
    int ret = BackedUpObject::from_rados(io_ctx, rados_name.c_str(), robj);
    if (ret) {
      cerr << ERR_PREFIX << "BackedUpObject::from_rados in delete loop "
	   << "returned " << ret << std::endl;
      _exit(ret);
    }
    std::string obj_path(robj->get_fs_path(m_export_dir));
    auto_ptr <BackedUpObject> lobj;
    ret = BackedUpObject::from_path(obj_path.c_str(), lobj);
    if (ret == ENOENT) {
      ret = io_ctx.remove(rados_name);
      if (ret && ret != -ENOENT) {
	cerr << ERR_PREFIX << "io_ctx.remove(" << obj_path << ") failed "
	    << "with error " << ret << std::endl;
	_exit(ret);
      }
      cout << "[deleted]      " << "removed '" << rados_name << "'" << std::endl;
    }
    else if (ret) {
      cerr << ERR_PREFIX << "BackedUpObject::from_path in delete loop "
	   << "returned " << ret << std::endl;
      _exit(ret);
    }
  }
  ExportDir *m_export_dir;
};

int do_rados_import(ThreadPool *tp, IoCtx &io_ctx, IoCtxDistributor* io_ctx_dist,
	   const char *dir_name, bool force, bool delete_after)
{
  auto_ptr <ExportDir> export_dir;
  export_dir.reset(ExportDir::from_file_system(dir_name));
  if (!export_dir.get())
    return -EIO;
  DirHolder dh;
  int ret = dh.opendir(dir_name);
  if (ret) {
    cerr << ERR_PREFIX << "opendir(" << dir_name << ") error: "
	 << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ImportLocalFileWQ import_file_wq(dir_name, force,
				   io_ctx_dist, time(NULL), tp);
  while (true) {
    struct dirent *de = readdir(dh.dp);
    if (!de)
      break;
    if ((strcmp(de->d_name, ".") == 0) || (strcmp(de->d_name, "..") == 0))
      continue;
    if (is_suffix(de->d_name, RADOS_SYNC_TMP_SUFFIX))
      continue;
    import_file_wq.queue(new std::string(de->d_name));
  }
  import_file_wq.drain();

  if (delete_after) {
    ImportValidateExistingWQ import_val_wq(export_dir.get(), io_ctx_dist,
					   time(NULL), tp);
    librados::NObjectIterator oi = io_ctx.nobjects_begin();
    librados::NObjectIterator oi_end = io_ctx.nobjects_end();
    for (; oi != oi_end; ++oi) {
      import_val_wq.queue(new std::string((*oi).get_oid()));
    }
    import_val_wq.drain();
  }
  cout << "[done]" << std::endl;
  return 0;
}
