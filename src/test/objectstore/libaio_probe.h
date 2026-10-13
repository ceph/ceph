// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <gtest/gtest.h>

#include "acconfig.h"
#include "common/errno.h"

#if defined(HAVE_LIBAIO)
#include <libaio.h>
#endif

// KernelDevice's aio thread aborts on any io_getevents(2) error it does not
// recognise.  Ubuntu 24.04 build containers let io_setup(2) succeed and deny
// io_getevents(2) with EPERM, so any test that opens a block device crashes
// there.  https://tracker.ceph.com/issues/78144
//
// Returns 0 if libaio is usable, else the negative errno that proved it is not.
inline int probe_libaio()
{
#if defined(HAVE_LIBAIO)
  io_context_t ctx = 0;
  // io_setup(2) failing is not "unusable": -EAGAIN only means aio-max-nr is
  // exhausted, which a parallel make check can do.  Let the test run and fail.
  // (No io_destroy: the kernel does not write ctx unless io_setup succeeds.)
  if (io_setup(1, &ctx) < 0) {
    return 0;
  }
  io_event event{};
  // Must be non-zero: libaio answers a zero timeout from the completion ring
  // in userspace, without issuing the syscall that gets denied.
  struct timespec timeout = {0, 1000 * 1000};
  int r;
  do {
    r = io_getevents(ctx, 1, 1, &event, &timeout);
  } while (r == -EINTR);            // as aio_queue_t::get_next_completed()
  io_destroy(ctx);
  if (r < 0) {
    return r;
  }
#endif
  return 0;
}

// First statement of any test that opens a block device.  Per test, not a
// global ::testing::Environment: skipping from Environment::SetUp() stops the
// tests but googletest still counts them PASSED.
#define SKIP_IF_NO_LIBAIO()                                              \
  do {                                                                   \
    int libaio_r_ = probe_libaio();                                      \
    if (libaio_r_ < 0) {                                                 \
      GTEST_SKIP() << "libaio is unusable in this environment ("         \
                   << cpp_strerror(libaio_r_)                            \
                   << "); a BlueStore block device cannot be opened here"; \
    }                                                                    \
  } while (0)
