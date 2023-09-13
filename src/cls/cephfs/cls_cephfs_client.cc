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
#include "mds/CInode.h"

#include "cls_cephfs_client.h"

using ceph::bufferlist;
using ceph::decode;

#define XATTR_CEILING "scan_ceiling"
#define XATTR_MAX_MTIME "scan_max_mtime"
#define XATTR_MAX_SIZE "scan_max_size"
#define XATTR_POOL_ID "scan_pool_id"

int ClsCephFSClient::accumulate_inode_metadata(
  librados::IoCtx &ctx,
  inodeno_t inode_no,
  const uint64_t obj_index,
  const uint64_t obj_size,
  const int64_t obj_pool_id,
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
  librados::ObjectWriteOperation op;
  bufferlist inbl;
  args.encode(inbl);
  op.exec("cephfs", "accumulate_inode_metadata", inbl);

  if (obj_pool_id != -1) {
    bufferlist bl;
    encode(obj_pool_id, bl);
    op.setxattr(XATTR_POOL_ID, bl);
  }

  // Execute op
  return ctx.operate(zeroth_object.name, &op);
}

int ClsCephFSClient::delete_inode_accumulate_result(
    librados::IoCtx &ctx,
    const std::string &oid)
{
  librados::ObjectWriteOperation op;

  // Remove xattrs from object
  //
  op.rmxattr(XATTR_CEILING);
  op.rmxattr(XATTR_MAX_SIZE);
  op.rmxattr(XATTR_MAX_MTIME);
  op.rmxattr(XATTR_POOL_ID);
  op.set_op_flags2(librados::OP_FAILOK);

  return (ctx.operate(oid, &op));
}

int ClsCephFSClient::fetch_inode_accumulate_result(
  librados::IoCtx &ctx,
  const std::string &oid,
  inode_backtrace_t *backtrace,
  file_layout_t *layout,
  std::string *symlink,
  AccumulateResult *result)
{
  ceph_assert(backtrace != NULL);
  ceph_assert(result != NULL);

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

  int scan_pool_id_r = 0;
  bufferlist scan_pool_id_bl;
  op.getxattr(XATTR_POOL_ID, &scan_pool_id_bl, &scan_pool_id_r);
  op.set_op_flags2(librados::OP_FAILOK);

  int parent_r = 0;
  bufferlist parent_bl;
  op.getxattr("parent", &parent_bl, &parent_r);
  op.set_op_flags2(librados::OP_FAILOK);

  int layout_r = 0;
  bufferlist layout_bl;
  op.getxattr("layout", &layout_bl, &layout_r);
  op.set_op_flags2(librados::OP_FAILOK);

  int symlink_r = 0;
  bufferlist symlink_bl;
  op.getxattr("symlink", &symlink_bl, &symlink_r);
  op.set_op_flags2(librados::OP_FAILOK);

  bufferlist op_bl;
  int r = ctx.operate(oid, &op, &op_bl);
  if (r < 0) {
    return r;
  }

  // Load scan_ceiling
  try {
    auto scan_ceiling_bl_iter = scan_ceiling_bl.cbegin();
    ObjCeiling ceiling;
    ceiling.decode(scan_ceiling_bl_iter);
    result->ceiling_obj_index = ceiling.id;
    result->ceiling_obj_size = ceiling.size;
  } catch (const ceph::buffer::error &err) {
    //dout(4) << "Invalid ceiling attr on '" << oid << "'" << dendl;
    return -EINVAL;
  }

  // Load scan_max_size
  try {
    auto scan_max_size_bl_iter = scan_max_size_bl.cbegin();
    decode(result->max_obj_size, scan_max_size_bl_iter);
  } catch (const ceph::buffer::error &err) {
    //dout(4) << "Invalid size attr on '" << oid << "'" << dendl;
    return -EINVAL;
  }

  // Load scan_pool_id
  if (scan_pool_id_bl.length()) {
    try {
      auto scan_pool_id_bl_iter = scan_pool_id_bl.cbegin();
      decode(result->obj_pool_id, scan_pool_id_bl_iter);
    } catch (const ceph::buffer::error &err) {
      //dout(4) << "Invalid pool_id attr on '" << oid << "'" << dendl;
      return -EINVAL;
    }
  }

  // Load scan_max_mtime
  try {
    auto scan_max_mtime_bl_iter = scan_max_mtime_bl.cbegin();
    decode(result->max_mtime, scan_max_mtime_bl_iter);
  } catch (const ceph::buffer::error &err) {
    //dout(4) << "Invalid mtime attr on '" << oid << "'" << dendl;
    return -EINVAL;
  }

  // Deserialize backtrace
  if (parent_bl.length()) {
    try {
      auto q = parent_bl.cbegin();
      backtrace->decode(q);
    } catch (ceph::buffer::error &e) {
      //dout(4) << "Corrupt backtrace on '" << oid << "': " << e << dendl;
      return -EINVAL;
    }
  }

  // Deserialize layout
  if (layout_bl.length()) {
    try {
      auto q = layout_bl.cbegin();
      decode(*layout, q);
    } catch (ceph::buffer::error &e) {
      return -EINVAL;
    }
  }

  // Deserialize symlink
  if (symlink_bl.length()) {
    try {
      auto q = symlink_bl.cbegin();
      decode(*symlink, q);
    } catch (ceph::buffer::error &e) {
      return -EINVAL;
    }
  }

  return 0;
}

void ClsCephFSClient::build_tag_filter(
          const std::string &scrub_tag,
          bufferlist *out_bl)
{
  ceph_assert(out_bl != NULL);

  // Leading part of bl is un-versioned string naming the filter
  encode(std::string("cephfs.inode_tag"), *out_bl);

  // Filter-specific part of the bl: in our case this is a versioned structure
  InodeTagFilterArgs args;
  args.scrub_tag = scrub_tag;
  args.encode(*out_bl);
}
