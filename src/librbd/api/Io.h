// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_IO_H
#define LIBRBD_API_IO_H

#include "include/int_types.h"
#include "librbd/io/ReadResult.h"

namespace librbd {

struct ImageCtx;
namespace io { struct AioCompletion; }

namespace api {

template<typename ImageCtxT = ImageCtx>
struct Io {
  static ssize_t read(ImageCtxT &image_ctx, uint64_t off, uint64_t len,
                      io::ReadResult &&read_result, int op_flags);
  static ssize_t write(ImageCtxT &image_ctx, uint64_t off, uint64_t len,
                       bufferlist &&bl, int op_flags);
  static ssize_t discard(ImageCtxT &image_ctx, uint64_t off, uint64_t len,
                         uint32_t discard_granularity_bytes);
  static ssize_t write_same(ImageCtxT &image_ctx, uint64_t off, uint64_t len,
                            bufferlist &&bl, int op_flags);
  static ssize_t write_zeroes(ImageCtxT &image_ctx, uint64_t off, uint64_t len,
                              int zero_flags, int op_flags);
  static ssize_t compare_and_write(ImageCtxT &image_ctx, uint64_t off,
                                   uint64_t len, bufferlist &&cmp_bl,
                                   bufferlist &&bl, uint64_t *mismatch_off,
                                   int op_flags);
  static int flush(ImageCtxT &image_ctx);

  static void aio_read(ImageCtxT &image_ctx, io::AioCompletion *c, uint64_t off,
                       uint64_t len, io::ReadResult &&read_result, int op_flags,
                       bool native_async);
  static void aio_write(ImageCtxT &image_ctx, io::AioCompletion *c,
                        uint64_t off, uint64_t len, bufferlist &&bl,
                        int op_flags, bool native_async);
  static void aio_discard(ImageCtxT &image_ctx, io::AioCompletion *c,
                          uint64_t off, uint64_t len,
                          uint32_t discard_granularity_bytes,
                          bool native_async);
  static void aio_write_same(ImageCtxT &image_ctx, io::AioCompletion *c,
                             uint64_t off, uint64_t len, bufferlist &&bl,
                             int op_flags, bool native_async);
  static void aio_write_zeroes(ImageCtxT &image_ctx, io::AioCompletion *c,
                               uint64_t off, uint64_t len, int zero_flags,
                               int op_flags, bool native_async);
  static void aio_compare_and_write(ImageCtxT &image_ctx, io::AioCompletion *c,
                                    uint64_t off, uint64_t len,
                                    bufferlist &&cmp_bl, bufferlist &&bl,
                                    uint64_t *mismatch_off, int op_flags,
                                    bool native_async);
  static void aio_flush(ImageCtxT &image_ctx, io::AioCompletion *c,
                        bool native_async);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Io<librbd::ImageCtx>;

#endif // LIBRBD_API_IO_H
