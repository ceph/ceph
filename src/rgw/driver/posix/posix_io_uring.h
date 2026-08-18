// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include "include/buffer.h"
#include "common/async/yield_context.h"
#include "common/dout.h"

namespace rgw::sal {

constexpr int64_t POSIX_DIRECT_ALIGN = 4096;
constexpr unsigned POSIX_URING_MAX_IODEPTH = 128;

inline int64_t posix_align_down(int64_t x, int64_t a = POSIX_DIRECT_ALIGN) {
  return x & ~(a - 1);
}
inline int64_t posix_align_up(int64_t x, int64_t a = POSIX_DIRECT_ALIGN) {
  return (x + a - 1) & ~(a - 1);
}

/* O_DIRECT pwrite helper (no io_uring).  Never toggles O_DIRECT on the fd. */
int posix_direct_write(int fd, int64_t ofs, bufferlist& bl,
                       const DoutPrefixProvider* dpp);

bool posix_io_engine_is_uring(CephContext* cct, const char* engine_opt);

/* engine=io_uring, yield available, and the thread-local ring is ready. */
bool posix_try_use_uring(const DoutPrefixProvider* dpp, optional_yield y,
                         bool nsfs);

/* Clamp QD to 2 for the sync engine; log if the configured value is higher. */
unsigned posix_sync_clamp_iodepth(const DoutPrefixProvider* dpp, unsigned qd,
                                  const char* iodepth_opt);

/* Init this thread's ring from knobs latched by the first
 * posix_try_use_uring(). Beast strands do not pin a worker: after yield
 * the coroutine may resume on a different thread, so this must be called
 * on the current thread before every io_uring_get_sqe / io_uring_submit.
 * Returns true if the ring is ready. */
bool posix_uring_ensure_ring(const DoutPrefixProvider* dpp);

class UringReadWindow {
  struct Impl;
  std::unique_ptr<Impl> impl;

public:
  UringReadWindow(const DoutPrefixProvider* dpp, optional_yield y,
                  int fd, unsigned qd, int64_t chunk_size, bool direct_io);
  ~UringReadWindow();
  UringReadWindow(const UringReadWindow&) = delete;
  UringReadWindow& operator=(const UringReadWindow&) = delete;
  UringReadWindow(UringReadWindow&&) = delete;
  UringReadWindow& operator=(UringReadWindow&&) = delete;

  /* Submit up to QD reads, wait oldest, invoke on_chunk(bl, len) in order. */
  int iterate(int64_t ofs, int64_t left,
              std::function<int(bufferlist&, int)> on_chunk);
};

class UringWriteWindow {
  struct Impl;
  std::unique_ptr<Impl> impl;

public:
  UringWriteWindow(const DoutPrefixProvider* dpp, optional_yield y,
                   int fd, unsigned qd, bool direct_io);
  ~UringWriteWindow();
  UringWriteWindow(const UringWriteWindow&) = delete;
  UringWriteWindow& operator=(const UringWriteWindow&) = delete;
  UringWriteWindow(UringWriteWindow&&) = delete;
  UringWriteWindow& operator=(UringWriteWindow&&) = delete;

  int process(bufferlist&& data, uint64_t offset);
  int drain();
};

} // namespace rgw::sal
