// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_READ_RESULT_H
#define CEPH_LIBRBD_IO_READ_RESULT_H

#include "include/common_fwd.h"
#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "librbd/io/Types.h"
#include "osdc/Striper.h"
#include <sys/uio.h>
#include <variant>


namespace librbd {

struct ImageCtx;

namespace io {

struct AioCompletion;
template <typename> struct ObjectReadRequest;

class ReadResult {
public:
  struct C_ImageReadRequest : public Context {
    AioCompletion *aio_completion;
    uint64_t buffer_offset = 0;
    Extents image_extents;
    bufferlist bl;
    bool ignore_enoent = false;

    C_ImageReadRequest(AioCompletion *aio_completion,
                       uint64_t buffer_offset,
                       const Extents& image_extents);

    void finish(int r) override;
  };

  struct C_ObjectReadRequest : public Context {
    AioCompletion *aio_completion;
    ReadExtents extents;

    C_ObjectReadRequest(AioCompletion *aio_completion, ReadExtents&& extents);

    void finish(int r) override;
  };

  struct C_ObjectReadMergedExtents : public Context {
      CephContext* cct;
      ReadExtents* extents;
      Context *on_finish;
      bufferlist bl;

      C_ObjectReadMergedExtents(CephContext* cct, ReadExtents* extents,
                                Context* on_finish);

      void finish(int r) override;
  };

  ReadResult() = default;
  ReadResult(char *buf, size_t buf_len);
  ReadResult(const struct iovec *iov, int iov_count);
  ReadResult(ceph::bufferlist *bl);
  ReadResult(Extents* extent_map, ceph::bufferlist* bl);

  void set_image_extents(const Extents& image_extents);

  void assemble_result(CephContext *cct);

private:
  struct Linear {
    char *buf;
    size_t buf_len;

    Linear(char *buf, size_t buf_len) : buf(buf), buf_len(buf_len) {
    }
  };

  struct Vector {
    const struct iovec *iov;
    int iov_count;

    Vector(const struct iovec *iov, int iov_count)
      : iov(iov), iov_count(iov_count) {
    }
  };

  struct Bufferlist {
    ceph::bufferlist *bl;

    Bufferlist(ceph::bufferlist *bl) : bl(bl) {
    }
  };

  struct SparseBufferlist {
    Extents *extent_map;
    ceph::bufferlist *bl;

    Extents image_extents;

    SparseBufferlist(Extents* extent_map, ceph::bufferlist* bl)
      : extent_map(extent_map), bl(bl) {
    }
  };

  typedef std::variant<std::monostate,
		       Linear,
		       Vector,
		       Bufferlist,
		       SparseBufferlist> Buffer;
  struct SetImageExtentsVisitor;
  struct AssembleResultVisitor;

  Buffer m_buffer;
  Striper::StripedReadResult m_destriper;

};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_READ_RESULT_H

