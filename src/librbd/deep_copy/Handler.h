// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_HANDLER_H
#define CEPH_LIBRBD_DEEP_COPY_HANDLER_H

#include "include/int_types.h"
#include "include/rbd/librbd.hpp"

namespace librbd {
namespace deep_copy {

struct Handler {
  virtual ~Handler() {}

  virtual void handle_read(uint64_t bytes_read) = 0;

  virtual int update_progress(uint64_t object_number,
                              uint64_t object_count) = 0;
};

struct NoOpHandler : public Handler {
  void handle_read(uint64_t bytes_read) override {
  }

  int update_progress(uint64_t object_number,
                      uint64_t object_count) override {
    return 0;
  }
};

class ProgressHandler : public NoOpHandler {
public:
  ProgressHandler(ProgressContext* progress_ctx)
    : m_progress_ctx(progress_ctx) {
  }

  int update_progress(uint64_t object_number,
                      uint64_t object_count) override {
    return m_progress_ctx->update_progress(object_number, object_count);
  }

private:
  librbd::ProgressContext* m_progress_ctx;
};

} // namespace deep_copy
} // namespace librbd

#endif // CEPH_LIBRBD_DEEP_COPY_HANDLER_H
