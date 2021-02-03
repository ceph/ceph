#include "librados_util.h"

uint8_t get_checksum_op_type(rados_checksum_type_t type) {
  switch (type) {
  case LIBRADOS_CHECKSUM_TYPE_XXHASH32:
    return CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH32;
  case LIBRADOS_CHECKSUM_TYPE_XXHASH64:
    return CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64;
  case LIBRADOS_CHECKSUM_TYPE_CRC32C:
    return CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C;
  default:
    return -1;
  }
}

int get_op_flags(int flags)
{
  int rados_flags = 0;
  if (flags & LIBRADOS_OP_FLAG_EXCL)
    rados_flags |= CEPH_OSD_OP_FLAG_EXCL;
  if (flags & LIBRADOS_OP_FLAG_FAILOK)
    rados_flags |= CEPH_OSD_OP_FLAG_FAILOK;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_RANDOM)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_RANDOM;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_WILLNEED)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_WILLNEED;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;
  if (flags & LIBRADOS_OP_FLAG_FADVISE_NOCACHE)
    rados_flags |= CEPH_OSD_OP_FLAG_FADVISE_NOCACHE;
  return rados_flags;
}

int translate_flags(int flags)
{
  int op_flags = 0;
  if (flags & librados::OPERATION_BALANCE_READS)
    op_flags |= CEPH_OSD_FLAG_BALANCE_READS;
  if (flags & librados::OPERATION_LOCALIZE_READS)
    op_flags |= CEPH_OSD_FLAG_LOCALIZE_READS;
  if (flags & librados::OPERATION_ORDER_READS_WRITES)
    op_flags |= CEPH_OSD_FLAG_RWORDERED;
  if (flags & librados::OPERATION_IGNORE_CACHE)
    op_flags |= CEPH_OSD_FLAG_IGNORE_CACHE;
  if (flags & librados::OPERATION_SKIPRWLOCKS)
    op_flags |= CEPH_OSD_FLAG_SKIPRWLOCKS;
  if (flags & librados::OPERATION_IGNORE_OVERLAY)
    op_flags |= CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (flags & librados::OPERATION_FULL_TRY)
    op_flags |= CEPH_OSD_FLAG_FULL_TRY;
  if (flags & librados::OPERATION_FULL_FORCE)
    op_flags |= CEPH_OSD_FLAG_FULL_FORCE;
  if (flags & librados::OPERATION_IGNORE_REDIRECT)
    op_flags |= CEPH_OSD_FLAG_IGNORE_REDIRECT;
  if (flags & librados::OPERATION_ORDERSNAP)
    op_flags |= CEPH_OSD_FLAG_ORDERSNAP;
  if (flags & librados::OPERATION_RETURNVEC)
    op_flags |= CEPH_OSD_FLAG_RETURNVEC;

  return op_flags;
}
