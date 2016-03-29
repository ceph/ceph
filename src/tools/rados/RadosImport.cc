// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "common/errno.h"

#include "osd/PGLog.h"
#include "RadosImport.h"

#define dout_subsys ceph_subsys_rados

int RadosImport::import(std::string pool, bool no_overwrite)
{
  librados::IoCtx ioctx;
  librados::Rados cluster;

  char *id = getenv("CEPH_CLIENT_ID");
  if (id) cerr << "Client id is: " << id << std::endl;
  int ret = cluster.init(id);
  if (ret) {
    cerr << "Error " << ret << " in cluster.init" << std::endl;
    return ret;
  }
  ret = cluster.conf_read_file(NULL);
  if (ret) {
    cerr << "Error " << ret << " in cluster.conf_read_file" << std::endl;
    return ret;
  }
  ret = cluster.conf_parse_env(NULL);
  if (ret) {
    cerr << "Error " << ret << " in cluster.conf_read_env" << std::endl;
    return ret;
  }
  cluster.connect();

  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  if (ret < 0) {
    cerr << "ioctx_create " << pool << " failed with " << ret << std::endl;
    return ret;
  }

  return import(ioctx, no_overwrite);
}

int RadosImport::import(librados::IoCtx &io_ctx, bool no_overwrite)
{
  bufferlist ebl;
  pg_info_t info;
  PGLog::IndexedLog log;

  int ret = read_super();
  if (ret)
    return ret;

  if (sh.magic != super_header::super_magic) {
    cerr << "Invalid magic number: 0x"
      << std::hex << sh.magic << " vs. 0x" << super_header::super_magic
      << std::dec << std::endl;
    return -EFAULT;
  }

  if (sh.version > super_header::super_ver) {
    cerr << "Can't handle export format version=" << sh.version << std::endl;
    return -EINVAL;
  }

  //First section must be TYPE_PG_BEGIN
  sectiontype_t type;
  ret = read_section(&type, &ebl);
  if (ret)
    return ret;

  bool pool_mode = false;
  if (type == TYPE_POOL_BEGIN) {
    pool_mode = true;
    cout << "Importing pool" << std::endl;
  } else if (type == TYPE_PG_BEGIN) {
    bufferlist::iterator ebliter = ebl.begin();
    pg_begin pgb;
    pgb.decode(ebliter);
    spg_t pgid = pgb.pgid;;
    if (!pgid.is_no_shard()) {
      cerr << "Importing Erasure Coded shard is not supported" << std::endl;
      return -EOPNOTSUPP;
    }
    dout(10) << "Exported features: " << pgb.superblock.compat_features << dendl;
    cout << "Importing from pgid " << pgid << std::endl;
  } else {
    cerr << "Invalid initial section code " << type << std::endl;
    return -EFAULT;
  }

  // XXX: How to check export features?
#if 0
  if (sb.compat_features.compare(pgb.superblock.compat_features) == -1) {
    cerr << "Export has incompatible features set "
      << pgb.superblock.compat_features << std::endl;
    return -EINVAL;
  }
#endif

#if defined(__linux__)
  if (file_fd != STDIN_FILENO)
    posix_fadvise(file_fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif

  bool done = false;
  bool found_metadata = false;
  while(!done) {
    ret = read_section(&type, &ebl);
    if (ret)
      return ret;

    //cout << "do_import: Section type " << hex << type << dec << std::endl;
    if (type >= END_OF_TYPES) {
      cout << "Skipping unknown section type" << std::endl;
      continue;
    }
    switch(type) {
    case TYPE_OBJECT_BEGIN:
      ret = get_object_rados(io_ctx, ebl, no_overwrite);
      if (ret) {
        cerr << "Error inserting object: " << ret << std::endl;
        return ret;
      }
      break;
    case TYPE_PG_METADATA:
      dout(10) << "Don't care about the old metadata" << dendl;
      found_metadata = true;
      break;
    case TYPE_PG_END:
      done = true;
      break;
    case TYPE_POOL_END:
      done = true;
      break;
    default:
      return -EFAULT;
    }
  }

  if (!(pool_mode || found_metadata)) {
    cerr << "Missing metadata section!" << std::endl;
  }

#if defined(__linux__)
  if (file_fd != STDIN_FILENO)
    posix_fadvise(file_fd, 0, 0, POSIX_FADV_DONTNEED);
#endif
  return 0;
}

int RadosImport::get_object_rados(librados::IoCtx &ioctx, bufferlist &bl, bool no_overwrite)
{
  bufferlist::iterator ebliter = bl.begin();
  object_begin ob;
  ob.decode(ebliter);
  map<string,bufferlist>::iterator i;
  bufferlist abl;
  bool skipping;

  data_section ds;
  attr_section as;
  omap_hdr_section oh;
  omap_section os;

  assert(g_ceph_context);
  if (ob.hoid.hobj.nspace == g_ceph_context->_conf->osd_hit_set_namespace) {
    cout << "Skipping internal object " << ob.hoid << std::endl;
    skip_object(bl);
    return 0;
  }

  if (!ob.hoid.hobj.is_head()) {
    cout << "Skipping non-head for " << ob.hoid << std::endl;
    skip_object(bl);
    return 0;
  }

  ioctx.set_namespace(ob.hoid.hobj.get_namespace());

  string msg("Write");
  skipping = false;
  if (dry_run) {
    uint64_t psize;
    time_t pmtime;
    int ret = ioctx.stat(ob.hoid.hobj.oid.name, &psize, &pmtime);
    if (ret == 0) {
      if (no_overwrite)
        // Could set skipping, but dry-run doesn't change anything either
        msg = "Skipping existing";
      else
        msg = "***Overwrite***";
    }
  } else {
    int ret = ioctx.create(ob.hoid.hobj.oid.name, true);
    if (ret && ret != -EEXIST) {
      cerr << "create failed: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    if (ret == -EEXIST) {
      if (no_overwrite) {
        msg = "Skipping existing";
        skipping = true;
      } else {
        msg = "***Overwrite***";
        ret = ioctx.remove(ob.hoid.hobj.oid.name);
        if (ret < 0) {
          cerr << "remove failed: " << cpp_strerror(ret) << std::endl;
          return ret;
        }
        ret = ioctx.create(ob.hoid.hobj.oid.name, true);
        // If object re-appeared after removal, let's just skip it
        if (ret == -EEXIST) {
          skipping = true;
          msg = "Skipping in-use object";
          ret = 0;
        }
        if (ret < 0) {
          cerr << "create failed: " << cpp_strerror(ret) << std::endl;
          return ret;
        }
      }
    }
  }

  cout << msg << " " << ob.hoid << std::endl;

  bool need_align = false;
  uint64_t alignment = 0;
  if (align) {
    need_align = true;
    alignment = align;
  } else {
    int ret = ioctx.pool_requires_alignment2(&need_align);
    if (ret < 0) {
      cerr << "pool_requires_alignment2 failed: " << cpp_strerror(ret)
        << std::endl;
      return ret;
    }

    if (need_align) {
      ret = ioctx.pool_required_alignment2(&alignment);
      if (ret < 0) {
        cerr << "pool_required_alignment2 failed: " << cpp_strerror(ret)
	  << std::endl;
	return ret;
      }
      assert(alignment != 0);
    }
  }

  if (need_align) {
    dout(10) << "alignment = " << alignment << dendl;
  }

  bufferlist ebl, databl;
  uint64_t in_offset = 0, out_offset = 0;
  bool done = false;
  while(!done) {
    sectiontype_t type;
    int ret = read_section(&type, &ebl);
    if (ret) {
      cerr << "Error reading section: " << ret << std::endl;
      return ret;
    }

    ebliter = ebl.begin();
    //cout << "\tdo_object: Section type " << hex << type << dec << std::endl;
    //cout << "\t\tsection size " << ebl.length() << std::endl;
    if (type >= END_OF_TYPES) {
      cout << "Skipping unknown object section type" << std::endl;
      continue;
    }
    switch(type) {
    case TYPE_DATA:
      ds.decode(ebliter);
      dout(10) << "\tdata: offset " << ds.offset << " len " << ds.len << dendl;
      if (need_align) {
        if (ds.offset != in_offset) {
          cerr << "Discontiguous object data in export" << std::endl;
          return -EFAULT;
        }
        assert(ds.databl.length() == ds.len);
        databl.claim_append(ds.databl);
        in_offset += ds.len;
        if (databl.length() >= alignment) {
          uint64_t rndlen = uint64_t(databl.length() / alignment) * alignment;
          dout(10) << "write offset=" << out_offset << " len=" << rndlen << dendl;
          if (!dry_run && !skipping) {
            ret = ioctx.write(ob.hoid.hobj.oid.name, databl, rndlen, out_offset);
            if (ret) {
              cerr << "write failed: " << cpp_strerror(ret) << std::endl;
              return ret;
            }
          }
          out_offset += rndlen;
          bufferlist n;
          if (databl.length() > rndlen) {
            assert(databl.length() - rndlen < alignment);
	    n.substr_of(databl, rndlen, databl.length() - rndlen);
          }
          databl = n;
        }
        break;
      }
      if (!dry_run && !skipping) {
        ret = ioctx.write(ob.hoid.hobj.oid.name, ds.databl, ds.len, ds.offset);
        if (ret) {
          cerr << "write failed: " << cpp_strerror(ret) << std::endl;
          return ret;
        }
      }
      break;
    case TYPE_ATTRS:
      as.decode(ebliter);

      dout(10) << "\tattrs: len " << as.data.size() << dendl;
      if (dry_run || skipping)
        break;
      for (std::map<string,bufferlist>::iterator i = as.data.begin();
          i != as.data.end(); ++i) {
	// The user xattrs that we want all begin with "_" with length > 1.
        // Drop key "_" and all attributes that do not start with '_'
        if (i->first == "_" || i->first[0] != '_')
          continue;
        ret = ioctx.setxattr(ob.hoid.hobj.oid.name, i->first.substr(1).c_str(), i->second);
        if (ret) {
          cerr << "setxattr failed: " << cpp_strerror(ret) << std::endl;
          if (ret != -EOPNOTSUPP)
            return ret;
        }
      }
      break;
    case TYPE_OMAP_HDR:
      oh.decode(ebliter);

      dout(10) << "\tomap header: " << string(oh.hdr.c_str(), oh.hdr.length())
        << dendl;
      if (dry_run || skipping)
        break;
      ret = ioctx.omap_set_header(ob.hoid.hobj.oid.name, oh.hdr);
      if (ret) {
        cerr << "omap_set_header failed: " << cpp_strerror(ret) << std::endl;
        if (ret != -EOPNOTSUPP)
          return ret;
      }
      break;
    case TYPE_OMAP:
      os.decode(ebliter);

      dout(10) << "\tomap: size " << os.omap.size() << dendl;
      if (dry_run || skipping)
        break;
      ret = ioctx.omap_set(ob.hoid.hobj.oid.name, os.omap);
      if (ret) {
        cerr << "omap_set failed: " << cpp_strerror(ret) << std::endl;
        if (ret != -EOPNOTSUPP)
          return ret;
      }
      break;
    case TYPE_OBJECT_END:
      done = true;
      if (need_align && databl.length() > 0) {
        assert(databl.length() < alignment);
        dout(10) << "END write offset=" << out_offset << " len=" << databl.length() << dendl;
        if (dry_run || skipping)
          break;
        ret = ioctx.write(ob.hoid.hobj.oid.name, databl, databl.length(), out_offset);
        if (ret) {
          cerr << "write failed: " << cpp_strerror(ret) << std::endl;
          return ret;
        }
      }
      break;
    default:
      cerr << "Unexpected section type " << type << std::endl;
      return -EFAULT;
    }
  }
  return 0;
}
