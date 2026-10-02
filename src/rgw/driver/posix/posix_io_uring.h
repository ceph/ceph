// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "include/buffer.h"
#include "include/function2.hpp"
#include "common/async/yield_context.h"
#include "common/dout.h"

namespace rgw::sal {

constexpr int64_t POSIX_DIRECT_ALIGN = 4096;
constexpr unsigned POSIX_URING_MAX_IODEPTH = 128;

/* Entries for a ring that carries no data transfer.
 *
 * With the synchronous data engine a ring exists only so that an
 * impersonated open can name a personality;  those submissions are
 * issued one at a time and waited on individually.  Ring memory is
 * charged against RLIMIT_MEMLOCK and multiplied by the frontend's
 * thread count, so the difference between this and a data-sized
 * ring decides whether a gateway with a few hundred threads can
 * create rings at all. */
constexpr unsigned POSIX_URING_CONTROL_ENTRIES = 8;

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

/* Init this thread's ring from rgw_{posix,nsfs}_io_uring_* knobs.
 * Returns true if the ring is ready. */
bool posix_uring_ensure_ring(const DoutPrefixProvider* dpp, bool nsfs);

/* No personality:  the SQE carries the submitting task's own
 * credentials, which is what every caller did before impersonation
 * existed and remains the default. */
constexpr uint16_t POSIX_URING_NO_PERSONALITY = 0;

/* Register the calling task's *current* credentials on this thread's
 * ring, and hand back the id that names them on an SQE.
 *
 * IORING_REGISTER_PERSONALITY copies `current`'s credentials and
 * takes no argument saying whose -- so a caller wanting somebody
 * else's must become them for the duration of this call.  Doing that
 * is the caller's business;  this knows only about the ring.
 *
 *   >= 0  the personality id
 *   < 0   -errno
 */
int posix_uring_register_personality(const DoutPrefixProvider* dpp);

/* Give one back.  Safe to call with an id this ring never issued,
 * which is what makes teardown simple. */
void posix_uring_unregister_personality(const DoutPrefixProvider* dpp,
                                        uint16_t id);

/* openat2(2) through the ring, under `personality`.
 *
 * This is where impersonation is actually enforced.  POSIX decides
 * access at open, not at read, so a personality named on a data SQE
 * applies to a descriptor whose permission check has already
 * passed;  the open has to carry it or nothing is enforced.
 *
 * Blocks until the completion arrives rather than suspending a
 * coroutine.  That is not a regression -- it replaces a blocking
 * openat(2) -- and it is what lets librgw, which has no yield
 * context, use the same path.
 *
 *   >= 0  the descriptor
 *   < 0   -errno, including -EACCES when the identity is refused,
 *         which is the answer the whole mechanism exists to get
 */
int posix_uring_openat2(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t flags, uint64_t mode,
                        uint16_t personality);

/* mkdirat(2) through the ring, under `personality`.
 *
 * A directory the gateway creates as itself is `S_IRWXU` and owned
 * by the gateway, so no impersonated identity can ever write inside
 * it.  Creating it as the identity is what makes the rest usable.
 *
 *   0     created
 *   < 0   -errno, -EEXIST included
 */
int posix_uring_mkdirat(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t mode,
                        uint16_t personality);

/* fsetxattr(2) and fgetxattr(2) through the ring, under
 * `personality`.
 *
 * Their siblings listxattr and removexattr have no opcode, so those
 * go through nsfs::run_as() instead -- see
 * docs/IMPERSONATED_XATTR.md.
 */
int posix_uring_fsetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, const void* value, size_t len,
                          int flags, uint16_t personality);
int posix_uring_fgetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, void* value, size_t len,
                          uint16_t personality);

class UringReadWindow {
  struct Impl;
  std::unique_ptr<Impl> impl;

public:
  /* `personality` names a credential registered on this thread's
   * ring;  POSIX_URING_NO_PERSONALITY means the submitting task's
   * own, which is the default and what posix passes. */
  UringReadWindow(const DoutPrefixProvider* dpp, optional_yield y,
                  int fd, unsigned qd, int64_t chunk_size, bool direct_io,
                  uint16_t personality = POSIX_URING_NO_PERSONALITY);
  ~UringReadWindow();
  UringReadWindow(const UringReadWindow&) = delete;
  UringReadWindow& operator=(const UringReadWindow&) = delete;
  UringReadWindow(UringReadWindow&&) = delete;
  UringReadWindow& operator=(UringReadWindow&&) = delete;

  /* Submit up to QD reads, wait oldest, invoke on_chunk(bl, len) in order. */
  int iterate(int64_t ofs, int64_t left,
              fu2::unique_function<int(bufferlist&, int)> on_chunk);
};

class UringWriteWindow {
  struct Impl;
  std::unique_ptr<Impl> impl;

public:
  UringWriteWindow(const DoutPrefixProvider* dpp, optional_yield y,
                   int fd, unsigned qd, bool direct_io,
                   uint16_t personality = POSIX_URING_NO_PERSONALITY);
  ~UringWriteWindow();
  UringWriteWindow(const UringWriteWindow&) = delete;
  UringWriteWindow& operator=(const UringWriteWindow&) = delete;
  UringWriteWindow(UringWriteWindow&&) = delete;
  UringWriteWindow& operator=(UringWriteWindow&&) = delete;

  int process(bufferlist&& data, uint64_t offset);
  int drain();
};

} // namespace rgw::sal
