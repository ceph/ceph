// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados_fwd.hpp"
#include "mds/mdstypes.h"
#include "cls_cephfs.h"

class AccumulateArgs;

class ClsCephFSClient
{
  public:
  static int accumulate_inode_metadata(
      librados::IoCtx &ctx,
      inodeno_t inode_no,
      const uint64_t obj_index,
      const uint64_t obj_size,
      const int64_t obj_pool_id,
      const time_t mtime);

  static int fetch_inode_accumulate_result(
      librados::IoCtx &ctx,
      const std::string &oid,
      inode_backtrace_t *backtrace,
      file_layout_t *layout,
      std::string *symlink,
      AccumulateResult *result);

  static int delete_inode_accumulate_result(
      librados::IoCtx &ctx,
      const std::string &oid);

  static void build_tag_filter(
      const std::string &scrub_tag,
      ceph::buffer::list *out_bl);
};
