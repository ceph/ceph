// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_TYPES_H
#define CEPH_LIBRBD_IO_TYPES_H

#include "include/int_types.h"
#include "include/rados/rados_types.hpp"
#include "common/interval_map.h"
#include "osdc/StriperTypes.h"
#include <iosfwd>
#include <map>
#include <vector>

struct Context;

namespace librbd {
namespace io {

typedef enum {
  AIO_TYPE_NONE = 0,
  AIO_TYPE_GENERIC,
  AIO_TYPE_OPEN,
  AIO_TYPE_CLOSE,
  AIO_TYPE_READ,
  AIO_TYPE_WRITE,
  AIO_TYPE_DISCARD,
  AIO_TYPE_FLUSH,
  AIO_TYPE_WRITESAME,
  AIO_TYPE_COMPARE_AND_WRITE,
} aio_type_t;

enum FlushSource {
  FLUSH_SOURCE_USER,
  FLUSH_SOURCE_INTERNAL,
  FLUSH_SOURCE_SHUTDOWN,
  FLUSH_SOURCE_EXCLUSIVE_LOCK,
  FLUSH_SOURCE_EXCLUSIVE_LOCK_SKIP_REFRESH,
  FLUSH_SOURCE_REFRESH,
  FLUSH_SOURCE_WRITEBACK,
  FLUSH_SOURCE_WRITE_BLOCK,
};

enum Direction {
  DIRECTION_READ,
  DIRECTION_WRITE,
  DIRECTION_BOTH
};

enum DispatchResult {
  DISPATCH_RESULT_INVALID,
  DISPATCH_RESULT_RESTART,
  DISPATCH_RESULT_CONTINUE,
  DISPATCH_RESULT_COMPLETE
};

enum ImageDispatchLayer {
  IMAGE_DISPATCH_LAYER_NONE = 0,
  IMAGE_DISPATCH_LAYER_API_START = IMAGE_DISPATCH_LAYER_NONE,
  IMAGE_DISPATCH_LAYER_QUEUE,
  IMAGE_DISPATCH_LAYER_QOS,
  IMAGE_DISPATCH_LAYER_EXCLUSIVE_LOCK,
  IMAGE_DISPATCH_LAYER_REFRESH,
  IMAGE_DISPATCH_LAYER_INTERNAL_START = IMAGE_DISPATCH_LAYER_REFRESH,
  IMAGE_DISPATCH_LAYER_MIGRATION,
  IMAGE_DISPATCH_LAYER_JOURNAL,
  IMAGE_DISPATCH_LAYER_WRITE_BLOCK,
  IMAGE_DISPATCH_LAYER_WRITEBACK_CACHE,
  IMAGE_DISPATCH_LAYER_CORE,
  IMAGE_DISPATCH_LAYER_LAST
};

enum {
  IMAGE_DISPATCH_FLAG_QOS_IOPS_THROTTLE       = 1 << 0,
  IMAGE_DISPATCH_FLAG_QOS_BPS_THROTTLE        = 1 << 1,
  IMAGE_DISPATCH_FLAG_QOS_READ_IOPS_THROTTLE  = 1 << 2,
  IMAGE_DISPATCH_FLAG_QOS_WRITE_IOPS_THROTTLE = 1 << 3,
  IMAGE_DISPATCH_FLAG_QOS_READ_BPS_THROTTLE   = 1 << 4,
  IMAGE_DISPATCH_FLAG_QOS_WRITE_BPS_THROTTLE  = 1 << 5,
  IMAGE_DISPATCH_FLAG_QOS_BPS_MASK            = (
    IMAGE_DISPATCH_FLAG_QOS_BPS_THROTTLE |
    IMAGE_DISPATCH_FLAG_QOS_READ_BPS_THROTTLE |
    IMAGE_DISPATCH_FLAG_QOS_WRITE_BPS_THROTTLE),
  IMAGE_DISPATCH_FLAG_QOS_IOPS_MASK           = (
    IMAGE_DISPATCH_FLAG_QOS_IOPS_THROTTLE |
    IMAGE_DISPATCH_FLAG_QOS_READ_IOPS_THROTTLE |
    IMAGE_DISPATCH_FLAG_QOS_WRITE_IOPS_THROTTLE),
  IMAGE_DISPATCH_FLAG_QOS_READ_MASK           = (
    IMAGE_DISPATCH_FLAG_QOS_READ_IOPS_THROTTLE |
    IMAGE_DISPATCH_FLAG_QOS_READ_BPS_THROTTLE),
  IMAGE_DISPATCH_FLAG_QOS_WRITE_MASK          = (
    IMAGE_DISPATCH_FLAG_QOS_WRITE_IOPS_THROTTLE |
    IMAGE_DISPATCH_FLAG_QOS_WRITE_BPS_THROTTLE),
  IMAGE_DISPATCH_FLAG_QOS_MASK                = (
    IMAGE_DISPATCH_FLAG_QOS_BPS_MASK |
    IMAGE_DISPATCH_FLAG_QOS_IOPS_MASK),

  // TODO: pass area through ImageDispatchInterface and remove
  // this flag
  IMAGE_DISPATCH_FLAG_CRYPTO_HEADER           = 1 << 6
};

enum {
  RBD_IO_OPERATIONS_DEFAULT             = 0,
  RBD_IO_OPERATION_READ                 = 1 << 0,
  RBD_IO_OPERATION_WRITE                = 1 << 1,
  RBD_IO_OPERATION_DISCARD              = 1 << 2,
  RBD_IO_OPERATION_WRITE_SAME           = 1 << 3,
  RBD_IO_OPERATION_COMPARE_AND_WRITE    = 1 << 4,
  RBD_IO_OPERATIONS_ALL                 = (
    RBD_IO_OPERATION_READ |
    RBD_IO_OPERATION_WRITE |
    RBD_IO_OPERATION_DISCARD |
    RBD_IO_OPERATION_WRITE_SAME |
    RBD_IO_OPERATION_COMPARE_AND_WRITE)
};

enum ObjectDispatchLayer {
  OBJECT_DISPATCH_LAYER_NONE = 0,
  OBJECT_DISPATCH_LAYER_CACHE,
  OBJECT_DISPATCH_LAYER_CRYPTO,
  OBJECT_DISPATCH_LAYER_JOURNAL,
  OBJECT_DISPATCH_LAYER_PARENT_CACHE,
  OBJECT_DISPATCH_LAYER_SCHEDULER,
  OBJECT_DISPATCH_LAYER_CORE,
  OBJECT_DISPATCH_LAYER_LAST
};

enum {
  READ_FLAG_DISABLE_READ_FROM_PARENT            = 1UL << 0,
  READ_FLAG_DISABLE_CLIPPING                    = 1UL << 1,
};

enum {
  OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE            = 1UL << 0
};

enum {
  OBJECT_DISCARD_FLAG_DISABLE_CLONE_REMOVE      = 1UL << 0,
  OBJECT_DISCARD_FLAG_DISABLE_OBJECT_MAP_UPDATE = 1UL << 1
};

enum {
  OBJECT_DISPATCH_FLAG_FLUSH                    = 1UL << 0,
  OBJECT_DISPATCH_FLAG_WILL_RETRY_ON_ERROR      = 1UL << 1
};

enum {
  LIST_SNAPS_FLAG_DISABLE_LIST_FROM_PARENT      = 1UL << 0,
  LIST_SNAPS_FLAG_WHOLE_OBJECT                  = 1UL << 1,
  LIST_SNAPS_FLAG_IGNORE_ZEROED_EXTENTS         = 1UL << 2,
};

enum SparseExtentState {
  SPARSE_EXTENT_STATE_DNE,    /* does not exist */
  SPARSE_EXTENT_STATE_ZEROED,
  SPARSE_EXTENT_STATE_DATA
};

std::ostream& operator<<(std::ostream& os, SparseExtentState state);

struct SparseExtent {
  SparseExtentState state;
  uint64_t length;

  SparseExtent(SparseExtentState state, uint64_t length)
    : state(state), length(length) {
  }

  operator SparseExtentState() const {
    return state;
  }

  bool operator==(const SparseExtent& rhs) const {
    return state == rhs.state && length == rhs.length;
  }
};

std::ostream& operator<<(std::ostream& os, const SparseExtent& state);

struct SparseExtentSplitMerge {
  SparseExtent split(uint64_t offset, uint64_t length,
                     const SparseExtent& se) const {
    return SparseExtent(se.state, length);
  }

  bool can_merge(const SparseExtent& left, const SparseExtent& right) const {
    return left.state == right.state;
  }

  SparseExtent merge(SparseExtent&& left, SparseExtent&& right) const {
    SparseExtent se(left);
    se.length += right.length;
    return se;
  }

  uint64_t length(const SparseExtent& se) const {
    return se.length;
  }
};

typedef interval_map<uint64_t,
                     SparseExtent,
                     SparseExtentSplitMerge> SparseExtents;

typedef std::vector<uint64_t> SnapIds;

typedef std::pair<librados::snap_t, librados::snap_t> WriteReadSnapIds;
extern const WriteReadSnapIds INITIAL_WRITE_READ_SNAP_IDS;

typedef std::map<WriteReadSnapIds, SparseExtents> SnapshotDelta;

struct SparseBufferlistExtent : public SparseExtent {
  ceph::bufferlist bl;

  SparseBufferlistExtent(SparseExtentState state, uint64_t length)
    : SparseExtent(state, length) {
    ceph_assert(state != SPARSE_EXTENT_STATE_DATA);
  }
  SparseBufferlistExtent(SparseExtentState state, uint64_t length,
                         ceph::bufferlist&& bl_)
    : SparseExtent(state, length), bl(std::move(bl_)) {
    ceph_assert(state != SPARSE_EXTENT_STATE_DATA || length == bl.length());
  }

  bool operator==(const SparseBufferlistExtent& rhs) const {
    return (state == rhs.state &&
            length == rhs.length &&
            bl.contents_equal(rhs.bl));
  }
};

struct SparseBufferlistExtentSplitMerge {
  SparseBufferlistExtent split(uint64_t offset, uint64_t length,
                               const SparseBufferlistExtent& sbe) const {
    ceph::bufferlist bl;
    if (sbe.state == SPARSE_EXTENT_STATE_DATA) {
      bl.substr_of(sbe.bl, offset, length);
    }
    return SparseBufferlistExtent(sbe.state, length, std::move(bl));
  }

  bool can_merge(const SparseBufferlistExtent& left,
                 const SparseBufferlistExtent& right) const {
    return left.state == right.state;
  }

  SparseBufferlistExtent merge(SparseBufferlistExtent&& left,
                               SparseBufferlistExtent&& right) const {
    if (left.state == SPARSE_EXTENT_STATE_DATA) {
      ceph::bufferlist bl{std::move(left.bl)};
      bl.claim_append(std::move(right.bl));
      return SparseBufferlistExtent(SPARSE_EXTENT_STATE_DATA,
                                    bl.length(), std::move(bl));
    } else {
      return SparseBufferlistExtent(left.state, left.length + right.length, {});
    }
  }

  uint64_t length(const SparseBufferlistExtent& sbe) const {
    return sbe.length;
  }
};

typedef interval_map<uint64_t,
                     SparseBufferlistExtent,
                     SparseBufferlistExtentSplitMerge> SparseBufferlist;
typedef std::map<uint64_t, SparseBufferlist> SnapshotSparseBufferlist;

using striper::LightweightBufferExtents;
using striper::LightweightObjectExtent;
using striper::LightweightObjectExtents;

typedef std::pair<uint64_t,uint64_t> Extent;
typedef std::vector<Extent> Extents;

enum class ImageArea {
  DATA,
  CRYPTO_HEADER
};

std::ostream& operator<<(std::ostream& os, ImageArea area);

struct ReadExtent {
    const uint64_t offset;
    const uint64_t length;
    const LightweightBufferExtents buffer_extents;
    ceph::bufferlist bl;
    Extents extent_map;

    ReadExtent(uint64_t offset,
               uint64_t length) : offset(offset), length(length) {};
    ReadExtent(uint64_t offset,
               uint64_t length,
               const LightweightBufferExtents&& buffer_extents)
               : offset(offset),
                 length(length),
                 buffer_extents(buffer_extents) {}
    ReadExtent(uint64_t offset,
               uint64_t length,
               const LightweightBufferExtents&& buffer_extents,
               ceph::bufferlist&& bl,
               Extents&& extent_map) : offset(offset),
                                       length(length),
                                       buffer_extents(buffer_extents),
                                       bl(bl),
                                       extent_map(extent_map) {};

    friend inline std::ostream& operator<<(
            std::ostream& os,
            const ReadExtent &extent) {
      os << "offset=" << extent.offset << ", "
         << "length=" << extent.length << ", "
         << "buffer_extents=" << extent.buffer_extents << ", "
         << "bl.length=" << extent.bl.length() << ", "
         << "extent_map=" << extent.extent_map;
      return os;
    }
};

typedef std::vector<ReadExtent> ReadExtents;

typedef std::map<uint64_t, uint64_t> ExtentMap;

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_TYPES_H
