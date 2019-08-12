
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
      const time_t mtime);

  static int fetch_inode_accumulate_result(
      librados::IoCtx &ctx,
      const std::string &oid,
      inode_backtrace_t *backtrace,
      file_layout_t *layout,
      AccumulateResult *result);

  static int delete_inode_accumulate_result(
      librados::IoCtx &ctx,
      const std::string &oid);

  static void build_tag_filter(
      const std::string &scrub_tag,
      bufferlist *out_bl);
};

