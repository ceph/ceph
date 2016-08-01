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


#include "cls_cephfs_client.h"

#include "mds/CInode.h"

#define XATTR_CEILING "scan_ceiling"
#define XATTR_MAX_MTIME "scan_max_mtime"
#define XATTR_MAX_SIZE "scan_max_size"

int ClsCephFSClient::accumulate_inode_metadata(
  librados::IoCtx &ctx,
  inodeno_t inode_no,
  const uint64_t obj_index,
  const uint64_t obj_size,
  const time_t mtime)
{
  AccumulateArgs args(
      obj_index,
      obj_size,
      mtime,
      XATTR_CEILING,
      XATTR_MAX_MTIME,
      XATTR_MAX_SIZE);

  // Generate 0th object name, where we will accumulate sizes/mtimes
  object_t zeroth_object = InodeStore::get_object_name(inode_no, frag_t(), "");

  // Construct a librados operation invoking our class method
  librados::ObjectReadOperation op;
  bufferlist inbl;
  args.encode(inbl);
  op.exec("cephfs", "accumulate_inode_metadata", inbl);

  // Execute op
  bufferlist outbl;
  return ctx.operate(zeroth_object.name, &op, &outbl);
}

int ClsCephFSClient::fetch_inode_accumulate_result(
  librados::IoCtx &ctx,
  const std::string &oid,
  inode_backtrace_t *backtrace,
  file_layout_t *layout,
  AccumulateResult *result)
{
  assert(backtrace != NULL);
  assert(result != NULL);

  librados::ObjectReadOperation op;

  int scan_ceiling_r = 0;
  bufferlist scan_ceiling_bl;
  op.getxattr(XATTR_CEILING, &scan_ceiling_bl, &scan_ceiling_r);

  int scan_max_size_r = 0;
  bufferlist scan_max_size_bl;
  op.getxattr(XATTR_MAX_SIZE, &scan_max_size_bl, &scan_max_size_r);

  int scan_max_mtime_r = 0;
  bufferlist scan_max_mtime_bl;
  op.getxattr(XATTR_MAX_MTIME, &scan_max_mtime_bl, &scan_max_mtime_r);

  int parent_r = 0;
  bufferlist parent_bl;
  op.getxattr("parent", &parent_bl, &parent_r);
  op.set_op_flags2(librados::OP_FAILOK);

  int layout_r = 0;
  bufferlist layout_bl;
  op.getxattr("layout", &layout_bl, &layout_r);
  op.set_op_flags2(librados::OP_FAILOK);

  bufferlist op_bl;
  int r = ctx.operate(oid, &op, &op_bl);
  if (r < 0) {
    return r;
  }

  // Load scan_ceiling
  try {
    bufferlist::iterator scan_ceiling_bl_iter = scan_ceiling_bl.begin();
    ObjCeiling ceiling;
    ceiling.decode(scan_ceiling_bl_iter);
    result->ceiling_obj_index = ceiling.id;
    result->ceiling_obj_size = ceiling.size;
  } catch (const buffer::error &err) {
    //dout(4) << "Invalid size attr on '" << oid << "'" << dendl;
    return -EINVAL;
  }

  // Load scan_max_size
  try {
    bufferlist::iterator scan_max_size_bl_iter = scan_max_size_bl.begin();
    ::decode(result->max_obj_size, scan_max_size_bl_iter);
  } catch (const buffer::error &err) {
    //dout(4) << "Invalid size attr on '" << oid << "'" << dendl;
    return -EINVAL;
  }

  // Load scan_max_mtime
  try {
    bufferlist::iterator scan_max_mtime_bl_iter = scan_max_mtime_bl.begin();
    ::decode(result->max_mtime, scan_max_mtime_bl_iter);
  } catch (const buffer::error &err) {
    //dout(4) << "Invalid size attr on '" << oid << "'" << dendl;
    return -EINVAL;
  }

  // Deserialize backtrace
  if (parent_bl.length()) {
    try {
      bufferlist::iterator q = parent_bl.begin();
      backtrace->decode(q);
    } catch (buffer::error &e) {
      //dout(4) << "Corrupt backtrace on '" << oid << "': " << e << dendl;
      return -EINVAL;
    }
  }

  // Deserialize layout
  if (layout_bl.length()) {
    try {
      bufferlist::iterator q = layout_bl.begin();
      ::decode(*layout, q);
    } catch (buffer::error &e) {
      return -EINVAL;
    }
  }

  return 0;
}

void ClsCephFSClient::build_tag_filter(
          const std::string &scrub_tag,
          bufferlist *out_bl)
{
  assert(out_bl != NULL);

  // Leading part of bl is un-versioned string naming the filter
  ::encode(std::string("cephfs.inode_tag"), *out_bl);

  // Filter-specific part of the bl: in our case this is a versioned structure
  InodeTagFilterArgs args;
  args.scrub_tag = scrub_tag;
  args.encode(*out_bl);
}

