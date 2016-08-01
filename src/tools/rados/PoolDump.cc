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

#include "include/rados/librados.hpp"
#include "common/errno.h"

#include "PoolDump.h"

using namespace librados;

#define dout_subsys ceph_subsys_rados

/**
 * Export RADOS objects from a live cluster
 * to a serialized format via a file descriptor.
 *
 * @returns 0 on success, else error code
 */
int PoolDump::dump(IoCtx *io_ctx)
{
  assert(io_ctx != NULL);

  int r = 0;
  write_super();

  r = write_simple(TYPE_POOL_BEGIN, file_fd);
  if (r != 0) {
    return r;
  }

  io_ctx->set_namespace(all_nspaces);
  librados::NObjectIterator i = io_ctx->nobjects_begin();

  librados::NObjectIterator i_end = io_ctx->nobjects_end();
  for (; i != i_end; ++i) {
    const std::string oid = i->get_oid();
    dout(10) << "OID '" << oid << "'" << dendl;

    // Compose OBJECT_BEGIN
    // ====================
    object_begin obj_begin;
    obj_begin.hoid.hobj.oid = i->get_oid();
    obj_begin.hoid.hobj.nspace = i->get_nspace();
    obj_begin.hoid.hobj.set_key(i->get_locator());

    // Only output head, RadosImport only wants that
    obj_begin.hoid.hobj.snap = CEPH_NOSNAP;

    // Skip setting object_begin.oi, RadosImport doesn't care

    r = write_section(TYPE_OBJECT_BEGIN, obj_begin, file_fd);
    if (r != 0) {
      return r;
    }

    // Compose TYPE_DATA chunks
    // ========================
    const uint32_t op_size = 4096 * 1024;
    uint64_t offset = 0;
    io_ctx->set_namespace(i->get_nspace());
    while (true) {
      bufferlist outdata;
      r = io_ctx->read(oid, outdata, op_size, offset);
      if (r <= 0) {
        // Error or no data
        break;
      }

      r = write_section(TYPE_DATA,
          data_section(offset, outdata.length(), outdata), file_fd);
      if (r != 0) {
        // Output stream error
        return r;
      }

      if (outdata.length() < op_size) {
        // No more data
        r = 0;
        break;
      }
      offset += outdata.length();
    }

    // Compose TYPE_ATTRS chunk
    // ========================
    std::map<std::string, bufferlist> raw_xattrs;
    std::map<std::string, bufferlist> xattrs;
    r = io_ctx->getxattrs(oid, raw_xattrs);
    if (r < 0) {
      cerr << "error getting xattr set " << oid << ": " << cpp_strerror(r)
           << std::endl;
      return r;
    }
    // Prepend "_" to mimic how user keys are represented in a pg export
    for (std::map<std::string, bufferlist>::iterator i = raw_xattrs.begin();
         i != raw_xattrs.end(); ++i) {
      std::pair< std::string, bufferlist> item(std::string("_") + std::string(i->first.c_str()), i->second);
      xattrs.insert(item);
    }
    r = write_section(TYPE_ATTRS, attr_section(xattrs), file_fd);
    if (r != 0) {
      return r;
    }

    // Compose TYPE_OMAP_HDR section
    // =============================
    bufferlist omap_header;
    r = io_ctx->omap_get_header(oid, &omap_header);
    if (r < 0) {
      cerr << "error getting omap header " << oid
	   << ": " << cpp_strerror(r) << std::endl;
      return r;
    }
    r = write_section(TYPE_OMAP_HDR, omap_hdr_section(omap_header), file_fd);
    if (r != 0) {
      return r;
    }

    // Compose TYPE_OMAP
    int MAX_READ = 512;
    string last_read = "";
    do {
      map<string, bufferlist> values;
      r = io_ctx->omap_get_vals(oid, last_read, MAX_READ, &values);
      if (r < 0) {
	cerr << "error getting omap keys " << oid << ": "
	     << cpp_strerror(r) << std::endl;
	return r;
      }
      if (values.size()) {
        last_read = values.rbegin()->first;
      } else {
        break;
      }

      r = write_section(TYPE_OMAP, omap_section(values), file_fd);
      if (r != 0) {
        return r;
      }
      r = values.size();
    } while (r == MAX_READ);
    r = 0;

    // Close object
    // =============
    r = write_simple(TYPE_OBJECT_END, file_fd);
    if (r != 0) {
      return r;
    }
  }

  r = write_simple(TYPE_POOL_END, file_fd);
#if defined(__linux__)
  if (file_fd != STDOUT_FILENO)
    posix_fadvise(file_fd, 0, 0, POSIX_FADV_DONTNEED);
#endif
  return r;
}
