// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <condition_variable>

#include "posix_io_uring.h"

#include "acconfig.h"
#include "common/errno.h"
#include "common/dout.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <linux/openat2.h>
#include <sys/stat.h>
#include <unistd.h>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

#if defined(HAVE_LIBURING)
#include <liburing.h>
#include <sys/eventfd.h>
#include "common/async/yield_waiter.h"
#endif

namespace rgw::sal {

namespace {

int pwrite_full(int fd, const char* buf, int64_t len, int64_t ofs,
                const DoutPrefixProvider* dpp)
{
  int64_t left = len;
  int64_t off = ofs;
  const char* p = buf;
  while (left > 0) {
    ssize_t n = ::pwrite(fd, p, left, off);
    if (n < 0) {
      int err = errno;
      ldpp_dout(dpp, 0) << "ERROR: pwrite failed: " << cpp_strerror(err)
                        << dendl;
      return -err;
    }
    if (n == 0) {
      return -EIO;
    }
    p += n;
    off += n;
    left -= n;
  }
  return 0;
}

unsigned round_up_pow2(unsigned n)
{
  if (n <= 1) {
    return 1;
  }
  --n;
  for (unsigned s = 1; s < sizeof(unsigned) * 8; s <<= 1) {
    n |= n >> s;
  }
  return n + 1;
}

} // anonymous namespace

int posix_direct_write(int fd, int64_t ofs, bufferlist& bl,
                       const DoutPrefixProvider* dpp)
{
  int64_t len = bl.length();
  if (len == 0) {
    return 0;
  }

  if ((ofs % POSIX_DIRECT_ALIGN) == 0 && (len % POSIX_DIRECT_ALIGN) == 0) {
    bl.rebuild_aligned_size_and_memory(POSIX_DIRECT_ALIGN, POSIX_DIRECT_ALIGN);
    return pwrite_full(fd, bl.c_str(), len, ofs, dpp);
  }

  if ((ofs % POSIX_DIRECT_ALIGN) == 0) {
    int64_t alen = posix_align_up(len);
    bufferptr bp(buffer::create_small_page_aligned(alen));
    memcpy(bp.c_str(), bl.c_str(), len);
    memset(bp.c_str() + len, 0, alen - len);
    int r = pwrite_full(fd, bp.c_str(), alen, ofs, dpp);
    if (r < 0) {
      return r;
    }
    struct stat st;
    if (::fstat(fd, &st) < 0) {
      return -errno;
    }
    int64_t logical = ofs + len;
    if (st.st_size <= ofs + alen) {
      if (::ftruncate(fd, logical) < 0) {
        return -errno;
      }
    }
    return 0;
  }

  /* Unaligned offset: read-modify-write of an aligned window. */
  int64_t aofs = posix_align_down(ofs);
  int64_t front = ofs - aofs;
  int64_t alen = posix_align_up(front + len);
  bufferptr bp(buffer::create_small_page_aligned(alen));
  memset(bp.c_str(), 0, alen);
  ssize_t n = ::pread(fd, bp.c_str(), alen, aofs);
  if (n < 0) {
    int err = errno;
    ldpp_dout(dpp, 0) << "ERROR: pread for O_DIRECT RMW failed: "
                      << cpp_strerror(err) << dendl;
    return -err;
  }
  memcpy(bp.c_str() + front, bl.c_str(), len);
  int r = pwrite_full(fd, bp.c_str(), alen, aofs, dpp);
  if (r < 0) {
    return r;
  }
  struct stat st;
  if (::fstat(fd, &st) < 0) {
    return -errno;
  }
  int64_t logical = ofs + len;
  if (st.st_size <= aofs + alen) {
    if (::ftruncate(fd, logical) < 0) {
      return -errno;
    }
  }
  return 0;
}

bool posix_io_engine_is_uring(CephContext* cct, const char* engine_opt)
{
  return cct->_conf.get_val<std::string>(engine_opt) == "io_uring";
}

bool posix_try_use_uring(const DoutPrefixProvider* dpp, optional_yield y,
                         bool nsfs)
{
  if (!y) {
    return false;
  }
  const char* engine_opt = nsfs ? "rgw_nsfs_io_engine" : "rgw_posix_io_engine";
  if (!posix_io_engine_is_uring(dpp->get_cct(), engine_opt)) {
    return false;
  }
  if (!posix_uring_ensure_ring(dpp, nsfs)) {
    ldpp_dout(dpp, 0) << "WARNING: io_uring requested but ring init failed; "
                      << "falling back to sync engine" << dendl;
    return false;
  }
  return true;
}

unsigned posix_sync_clamp_iodepth(const DoutPrefixProvider* dpp, unsigned qd,
                                  const char* iodepth_opt)
{
  if (qd > 2) {
    ldpp_dout(dpp, 1) << "clamping " << iodepth_opt << "=" << qd
                      << " to 2 for sync IO engine" << dendl;
    return 2;
  }
  if (qd < 1) {
    return 1;
  }
  return qd;
}

#if defined(HAVE_LIBURING)

namespace {

struct CqeDispatch {
  void (*fn)(io_uring_cqe* cqe, void* arg) = nullptr;
  void* arg = nullptr;
};

struct ThreadIoUringState {
  io_uring ring{};
  bool initialized = false;
  int init_error = 0;
  int event_fd = -1;
  std::thread reaper_thread;
  std::atomic<bool> shutdown{false};

  int init(const DoutPrefixProvider* dpp, unsigned queue_depth, unsigned flags,
           unsigned sq_thread_idle_ms) {
    if (initialized) {
      return 0;
    }
    if (init_error < 0) {
      return init_error;
    }

#ifdef IORING_SETUP_SINGLE_ISSUER
    flags |= IORING_SETUP_SINGLE_ISSUER;
#endif
    /* COOP_TASKRUN / DEFER_TASKRUN require the submitter to io_uring_enter
     * to flush CQEs, which fights the eventfd reaper. */

    auto queue_init = [&](unsigned f) {
      struct io_uring_params params{};
      params.flags = f;
      if (f & IORING_SETUP_SQPOLL) {
        params.sq_thread_idle = sq_thread_idle_ms;
      }
      return io_uring_queue_init_params(queue_depth, &ring, &params);
    };

    int ret = queue_init(flags);
    if (ret < 0 && ret == -EINVAL) {
      unsigned basic = flags & IORING_SETUP_SQPOLL;
      ret = queue_init(basic);
      flags = basic;
    }
    if (ret < 0 && (flags & IORING_SETUP_SQPOLL) &&
        (ret == -EPERM || ret == -EINVAL)) {
      ldpp_dout(dpp, 1) << "URING: SQPOLL rejected (" << cpp_strerror(-ret)
                        << "), retrying without it" << dendl;
      ret = queue_init(0);
      flags = 0;
    }
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: URING: io_uring_queue_init_params failed: "
                        << cpp_strerror(-ret) << dendl;
      init_error = ret;
      return ret;
    }

    event_fd = ::eventfd(0, EFD_CLOEXEC);
    if (event_fd < 0) {
      int err = errno;
      ldpp_dout(dpp, 0) << "ERROR: URING: eventfd() failed: "
                        << cpp_strerror(err) << dendl;
      io_uring_queue_exit(&ring);
      init_error = -err;
      return -err;
    }

    ret = io_uring_register_eventfd(&ring, event_fd);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: URING: io_uring_register_eventfd failed: "
                        << cpp_strerror(-ret) << dendl;
      ::close(event_fd);
      event_fd = -1;
      io_uring_queue_exit(&ring);
      init_error = ret;
      return ret;
    }

    shutdown.store(false, std::memory_order_relaxed);
    reaper_thread = std::thread([this]() { reap_loop(); });
    initialized = true;
    ldpp_dout(dpp, 10) << "URING: ring ready flags=0x" << std::hex << flags
                       << std::dec << " entries=" << queue_depth << dendl;
    return 0;
  }

  void reap_loop() {
    while (true) {
      uint64_t val = 0;
      ssize_t n = ::read(event_fd, &val, sizeof(val));
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        break;
      }
      struct io_uring_cqe* cqe;
      while (io_uring_peek_cqe(&ring, &cqe) == 0) {
        auto* disp = static_cast<CqeDispatch*>(io_uring_cqe_get_data(cqe));
        if (disp && disp->fn) {
          disp->fn(cqe, disp->arg);
        }
        io_uring_cqe_seen(&ring, cqe);
      }
      if (shutdown.load(std::memory_order_relaxed)) {
        break;
      }
    }
  }

  ~ThreadIoUringState() {
    if (initialized) {
      shutdown.store(true, std::memory_order_relaxed);
      if (event_fd >= 0) {
        uint64_t val = 1;
        [[maybe_unused]] auto _ = ::write(event_fd, &val, sizeof(val));
      }
      if (reaper_thread.joinable()) {
        reaper_thread.join();
      }
      io_uring_queue_exit(&ring);
      if (event_fd >= 0) {
        ::close(event_fd);
      }
    }
  }
};

thread_local ThreadIoUringState thread_uring_state;

struct IoSlot {
  CqeDispatch disp;
  bufferptr buf;
  bufferlist hold; /* keep PUT payload alive until CQE */
  int64_t user_ofs = 0;
  int64_t user_len = 0;
  int64_t aligned_ofs = 0;
  int64_t aligned_len = 0;
  int result = 0;
  bool done = false;
  bool is_write = false;
  std::mutex mutex;
  ceph::async::yield_waiter<int> waiter;
  /* For callers with no coroutine to suspend.  An open replaces a
   * blocking openat(2), so blocking here is what it already did;
   * and librgw has no yield context at all, so a yield-only wait
   * would exclude the NFS gateway. */
  std::condition_variable blocking_waiter;

  static void handle_cqe(io_uring_cqe* cqe, void* arg) {
    auto* slot = static_cast<IoSlot*>(arg);
    std::unique_lock lock(slot->mutex);
    slot->result = cqe->res;
    slot->done = true;
    const bool waiting = static_cast<bool>(slot->waiter);
    lock.unlock();
    if (waiting) {
      slot->waiter.complete(boost::system::error_code{}, slot->result);
    }
    /* harmless when nobody is blocked;  the reaper is a separate
     * thread, so it can complete a blocked submitter */
    slot->blocking_waiter.notify_all();
  }

  void reset_for_reuse() {
    buf = bufferptr();
    hold.clear();
    user_ofs = 0;
    user_len = 0;
    aligned_ofs = 0;
    aligned_len = 0;
    result = 0;
    done = false;
    is_write = false;
  }
};

int wait_slot(IoSlot& slot, optional_yield y)
{
  std::unique_lock lock(slot.mutex);
  if (!slot.done) {
    if (!y) {
      return -EAGAIN;
    }
    slot.result = slot.waiter.async_wait(lock, y.get_yield_context());
  }
  return slot.result;
}

/* Wait without a coroutine.  Used by the open path, which replaces
 * a blocking syscall and so blocks exactly as that did. */
int wait_slot_blocking(IoSlot& slot)
{
  std::unique_lock lock(slot.mutex);
  slot.blocking_waiter.wait(lock, [&slot] { return slot.done; });
  return slot.result;
}

int submit_prepared(const DoutPrefixProvider* dpp)
{
  int submitted = io_uring_submit(&thread_uring_state.ring);
  if (submitted < 0) {
    ldpp_dout(dpp, 0) << "ERROR: URING: io_uring_submit failed: "
                      << cpp_strerror(-submitted) << dendl;
    return submitted;
  }
  return 0;
}

io_uring_sqe* get_sqe_retry(const DoutPrefixProvider* dpp)
{
  for (int i = 0; i < 8; ++i) {
    io_uring_sqe* sqe = io_uring_get_sqe(&thread_uring_state.ring);
    if (sqe) {
      return sqe;
    }
    submit_prepared(dpp);
  }
  return nullptr;
}

int prep_rw(const DoutPrefixProvider* dpp, IoSlot& slot, int fd, bool write,
            uint16_t personality)
{
  io_uring_sqe* sqe = get_sqe_retry(dpp);
  if (!sqe) {
    ldpp_dout(dpp, 0) << "ERROR: URING: failed to get SQE" << dendl;
    return -EAGAIN;
  }
  /* The whole of impersonation on the data path:  the kernel applies
   * this slot's registered credentials to this one operation.  Zero
   * means the submitting task's own, which is what every caller did
   * before this existed. */
  sqe->personality = personality;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;
  if (write) {
    io_uring_prep_write(sqe, fd, slot.buf.c_str(), slot.aligned_len,
                        slot.aligned_ofs);
  } else {
    io_uring_prep_read(sqe, fd, slot.buf.c_str(), slot.aligned_len,
                       slot.aligned_ofs);
  }
  io_uring_sqe_set_data(sqe, &slot.disp);
  return 0;
}

} // anonymous namespace

int posix_uring_register_personality(const DoutPrefixProvider* dpp)
{
  /* Create the ring if this thread has not needed one yet.
   *
   * Registration happens in prepare(), before any data transfer has
   * asked for a ring -- and it is needed even when the data engine
   * is sync, because the open goes through the ring regardless of
   * how the bytes afterwards travel. */
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }
  const int id = io_uring_register_personality(&thread_uring_state.ring);
  if (id < 0) {
    ldpp_dout(dpp, 0) << "ERROR: URING: io_uring_register_personality: "
                      << cpp_strerror(-id) << dendl;
  }
  return id;
}

void posix_uring_unregister_personality(const DoutPrefixProvider* dpp,
                                        uint16_t id)
{
  if (!thread_uring_state.initialized) {
    return;
  }
  const int r = io_uring_unregister_personality(&thread_uring_state.ring, id);
  if (r < 0) {
    ldpp_dout(dpp, 5) << "URING: io_uring_unregister_personality(" << id
                      << "): " << cpp_strerror(-r) << dendl;
  }
}

bool posix_uring_ensure_ring(const DoutPrefixProvider* dpp, bool nsfs)
{
  if (thread_uring_state.initialized) {
    return true;
  }
  if (thread_uring_state.init_error < 0) {
    return false;
  }

  CephContext* cct = dpp->get_cct();
  const char* prefix = nsfs ? "rgw_nsfs" : "rgw_posix";
  unsigned entries = cct->_conf.get_val<uint64_t>(
      std::string(prefix) + "_io_uring_queue_depth");
  if (entries < POSIX_URING_MAX_IODEPTH) {
    entries = POSIX_URING_MAX_IODEPTH;
  }
  entries = round_up_pow2(entries);

  unsigned flags = 0;
  unsigned idle_ms = 0;
  if (cct->_conf.get_val<bool>(std::string(prefix) + "_io_uring_sqpoll")) {
    flags |= IORING_SETUP_SQPOLL;
    idle_ms = cct->_conf.get_val<uint64_t>(
        std::string(prefix) + "_io_uring_sq_thread_idle_ms");
  }

  int ret = thread_uring_state.init(dpp, entries, flags, idle_ms);
  return ret == 0;
}

int posix_uring_openat2(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t flags, uint64_t mode,
                        uint16_t personality)
{
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }

  /* RESOLVE_NO_MAGICLINKS keeps a symlink to /proc out of the
   * resolution.  Not RESOLVE_BENEATH yet:  the callers still pass
   * names that may legitimately traverse, and tightening that
   * belongs with the per-directory work. */
  struct open_how how = {};
  how.flags = flags;
  how.mode = (flags & (O_CREAT | O_TMPFILE)) ? mode : 0;
  how.resolve = RESOLVE_NO_MAGICLINKS;

  IoSlot slot;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;

  io_uring_sqe* sqe = get_sqe_retry(dpp);
  if (!sqe) {
    ldpp_dout(dpp, 0) << "ERROR: URING: no SQE for openat2" << dendl;
    return -EAGAIN;
  }
  io_uring_prep_openat2(sqe, dirfd, path, &how);
  sqe->personality = personality;
  io_uring_sqe_set_data(sqe, &slot.disp);

  const int sub = submit_prepared(dpp);
  if (sub < 0) {
    return sub;
  }
  return wait_slot_blocking(slot);
}

int posix_uring_fsetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, const void* value, size_t len,
                          int flags, uint16_t personality)
{
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }
  IoSlot slot;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;

  io_uring_sqe* sqe = get_sqe_retry(dpp);
  if (!sqe) {
    return -EAGAIN;
  }
  io_uring_prep_fsetxattr(sqe, fd, name, static_cast<const char*>(value),
                          flags, len);
  sqe->personality = personality;
  io_uring_sqe_set_data(sqe, &slot.disp);

  const int sub = submit_prepared(dpp);
  if (sub < 0) {
    return sub;
  }
  return wait_slot_blocking(slot);
}

int posix_uring_fgetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, void* value, size_t len,
                          uint16_t personality)
{
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }
  IoSlot slot;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;

  io_uring_sqe* sqe = get_sqe_retry(dpp);
  if (!sqe) {
    return -EAGAIN;
  }
  io_uring_prep_fgetxattr(sqe, fd, name, static_cast<char*>(value), len);
  sqe->personality = personality;
  io_uring_sqe_set_data(sqe, &slot.disp);

  const int sub = submit_prepared(dpp);
  if (sub < 0) {
    return sub;
  }
  return wait_slot_blocking(slot);
}

int posix_uring_mkdirat(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t mode,
                        uint16_t personality)
{
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }

  IoSlot slot;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;

  io_uring_sqe* sqe = get_sqe_retry(dpp);
  if (!sqe) {
    ldpp_dout(dpp, 0) << "ERROR: URING: no SQE for mkdirat" << dendl;
    return -EAGAIN;
  }
  io_uring_prep_mkdirat(sqe, dirfd, path, mode);
  sqe->personality = personality;
  io_uring_sqe_set_data(sqe, &slot.disp);

  const int sub = submit_prepared(dpp);
  if (sub < 0) {
    return sub;
  }
  return wait_slot_blocking(slot);
}


struct UringReadWindow::Impl {
  const DoutPrefixProvider* dpp;
  optional_yield y;
  int fd;
  unsigned qd;
  int64_t chunk_size;
  bool direct_io;
  std::deque<std::unique_ptr<IoSlot>> inflight;
  unsigned pending_sqes = 0;

  uint16_t personality{POSIX_URING_NO_PERSONALITY};

  Impl(const DoutPrefixProvider* dpp, optional_yield y, int fd, unsigned qd,
       int64_t chunk_size, bool direct_io, uint16_t personality)
    : dpp(dpp), y(y), fd(fd),
      qd(std::clamp(qd, 1u, POSIX_URING_MAX_IODEPTH)),
      chunk_size(chunk_size > 0 ? chunk_size : 128 * 1024),
      direct_io(direct_io), personality(personality) {}

  int flush_submit() {
    if (pending_sqes == 0) {
      return 0;
    }
    int r = submit_prepared(dpp);
    pending_sqes = 0;
    return r;
  }

  int submit_one(int64_t ofs, int64_t len) {
    auto slot = std::make_unique<IoSlot>();
    slot->user_ofs = ofs;
    slot->user_len = len;
    if (direct_io) {
      slot->aligned_ofs = posix_align_down(ofs);
      int64_t front = ofs - slot->aligned_ofs;
      slot->aligned_len = posix_align_up(front + len);
    } else {
      slot->aligned_ofs = ofs;
      slot->aligned_len = len;
    }
    slot->buf = bufferptr(
        buffer::create_small_page_aligned(slot->aligned_len));
    int r = prep_rw(dpp, *slot, fd, /*write=*/false, personality);
    if (r < 0) {
      return r;
    }
    ++pending_sqes;
    inflight.push_back(std::move(slot));
    return 0;
  }

  int wait_oldest(bufferlist& out) {
    int r = flush_submit();
    if (r < 0) {
      return r;
    }
    if (inflight.empty()) {
      return 0;
    }
    auto& slot = *inflight.front();
    int res = wait_slot(slot, y);
    if (res < 0) {
      inflight.pop_front();
      return res;
    }
    int64_t front = slot.user_ofs - slot.aligned_ofs;
    int64_t got = std::min<int64_t>(
        std::max<int64_t>(res - front, 0), slot.user_len);
    if (got > 0) {
      out.append(slot.buf, front, got);
    }
    inflight.pop_front();
    return got;
  }

  int drain() {
    int first_err = 0;
    while (!inflight.empty()) {
      bufferlist discard;
      int r = wait_oldest(discard);
      if (r < 0 && first_err == 0) {
        first_err = r;
      }
    }
    return first_err;
  }
};

UringReadWindow::UringReadWindow(const DoutPrefixProvider* dpp,
                                 optional_yield y, int fd, unsigned qd,
                                 int64_t chunk_size, bool direct_io,
                                 uint16_t personality)
  : impl(std::make_unique<Impl>(dpp, y, fd, qd, chunk_size, direct_io,
                                personality))
{}

UringReadWindow::~UringReadWindow()
{
  if (impl) {
    try {
      impl->drain();
    } catch (...) {}
  }
}

int UringReadWindow::iterate(int64_t ofs, int64_t left,
                             std::function<int(bufferlist&, int)> on_chunk)
{
  int64_t submit_ofs = ofs;
  int64_t submit_left = left;

  auto fill = [&]() -> int {
    while (submit_left > 0 && impl->inflight.size() < impl->qd) {
      int64_t n = std::min(submit_left, impl->chunk_size);
      int r = impl->submit_one(submit_ofs, n);
      if (r < 0) {
        impl->flush_submit();
        return r;
      }
      submit_ofs += n;
      submit_left -= n;
    }
    return impl->flush_submit();
  };

  while (left > 0) {
    int r = fill();
    if (r < 0) {
      impl->drain();
      return r;
    }
    if (impl->inflight.empty()) {
      break;
    }
    bufferlist bl;
    int len = impl->wait_oldest(bl);
    if (len < 0) {
      impl->drain();
      return len;
    }
    if (len == 0) {
      impl->drain();
      break;
    }
    r = on_chunk(bl, len);
    if (r < 0) {
      impl->drain();
      return r;
    }
    left -= len;
  }
  return impl->drain();
}

struct UringWriteWindow::Impl {
  const DoutPrefixProvider* dpp;
  optional_yield y;
  int fd;
  unsigned qd;
  bool direct_io;
  bool chmod_done = false;
  int64_t logical_end = 0;
  bool needs_truncate = false;
  std::deque<std::unique_ptr<IoSlot>> inflight;

  uint16_t personality{POSIX_URING_NO_PERSONALITY};

  Impl(const DoutPrefixProvider* dpp, optional_yield y, int fd, unsigned qd,
       bool direct_io, uint16_t personality)
    : dpp(dpp), y(y), fd(fd),
      qd(std::clamp(qd, 1u, POSIX_URING_MAX_IODEPTH)),
      direct_io(direct_io), personality(personality) {}

  int wait_oldest() {
    if (inflight.empty()) {
      return 0;
    }
    auto& slot = *inflight.front();
    int res = wait_slot(slot, y);
    inflight.pop_front();
    if (res < 0) {
      return res;
    }
    if (slot.is_write &&
        static_cast<int64_t>(res) != slot.aligned_len) {
      ldpp_dout(dpp, 0) << "ERROR: URING: short write wrote=" << res
                        << " expected=" << slot.aligned_len << dendl;
      return -EIO;
    }
    return 0;
  }

  int drain_all() {
    int first_err = 0;
    while (!inflight.empty()) {
      int r = wait_oldest();
      if (r < 0 && first_err == 0) {
        first_err = r;
      }
    }
    if (first_err == 0 && needs_truncate) {
      if (::ftruncate(fd, logical_end) < 0) {
        first_err = -errno;
        ldpp_dout(dpp, 0) << "ERROR: URING: ftruncate to " << logical_end
                          << " failed: " << cpp_strerror(-first_err) << dendl;
      }
      needs_truncate = false;
    }
    return first_err;
  }

  int submit_write(bufferlist&& data, uint64_t offset) {
    int64_t len = data.length();
    int64_t ofs = static_cast<int64_t>(offset);
    auto slot = std::make_unique<IoSlot>();
    slot->is_write = true;
    slot->user_ofs = ofs;
    slot->user_len = len;
    slot->hold = std::move(data);

    if (direct_io) {
      if ((ofs % POSIX_DIRECT_ALIGN) == 0 &&
          (len % POSIX_DIRECT_ALIGN) == 0) {
        slot->hold.rebuild_aligned_size_and_memory(POSIX_DIRECT_ALIGN,
                                                   POSIX_DIRECT_ALIGN);
        slot->aligned_ofs = ofs;
        slot->aligned_len = len;
      } else if ((ofs % POSIX_DIRECT_ALIGN) == 0) {
        slot->aligned_ofs = ofs;
        slot->aligned_len = posix_align_up(len);
        needs_truncate = true;
      } else {
        /* Unaligned offset: drain then RMW with blocking pwrite. */
        int r = drain_all();
        if (r < 0) {
          return r;
        }
        int wr = posix_direct_write(fd, ofs, slot->hold, dpp);
        if (wr < 0) {
          return wr;
        }
        logical_end = std::max(logical_end, ofs + len);
        return 0;
      }
    } else {
      slot->hold.rebuild();
      slot->aligned_ofs = ofs;
      slot->aligned_len = len;
    }

    slot->buf = bufferptr(
        buffer::create_small_page_aligned(slot->aligned_len));
    memcpy(slot->buf.c_str(), slot->hold.c_str(), len);
    if (slot->aligned_len > len) {
      memset(slot->buf.c_str() + len, 0, slot->aligned_len - len);
    }

    logical_end = std::max(logical_end, ofs + len);

    int r = prep_rw(dpp, *slot, fd, /*write=*/true, personality);
    if (r < 0) {
      return r;
    }
    inflight.push_back(std::move(slot));
    return submit_prepared(dpp);
  }
};

UringWriteWindow::UringWriteWindow(const DoutPrefixProvider* dpp,
                                   optional_yield y, int fd, unsigned qd,
                                   bool direct_io, uint16_t personality)
  : impl(std::make_unique<Impl>(dpp, y, fd, qd, direct_io, personality))
{}

UringWriteWindow::~UringWriteWindow()
{
  if (impl) {
    try {
      impl->drain_all();
    } catch (...) {}
  }
}

int UringWriteWindow::process(bufferlist&& data, uint64_t offset)
{
  if (data.length() == 0) {
    return impl->drain_all();
  }
  if (!impl->chmod_done) {
    int ret = ::fchmod(impl->fd, S_IRUSR | S_IWUSR);
    if (ret < 0) {
      ret = -errno;
      ldpp_dout(impl->dpp, 0) << "ERROR: URING: fchmod failed: "
                              << cpp_strerror(-ret) << dendl;
      return ret;
    }
    impl->chmod_done = true;
  }
  while (impl->inflight.size() >= impl->qd) {
    int r = impl->wait_oldest();
    if (r < 0) {
      return r;
    }
  }
  return impl->submit_write(std::move(data), offset);
}

int UringWriteWindow::drain()
{
  return impl->drain_all();
}

#else /* !HAVE_LIBURING */

bool posix_uring_ensure_ring(const DoutPrefixProvider* dpp, bool nsfs)
{
  (void)dpp;
  (void)nsfs;
  return false;
}

struct UringReadWindow::Impl {};
struct UringWriteWindow::Impl {};

int posix_uring_register_personality(const DoutPrefixProvider* dpp)
{
  (void)dpp;
  return -EOPNOTSUPP;
}

int posix_uring_openat2(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t flags, uint64_t mode,
                        uint16_t personality)
{
  (void)dpp; (void)dirfd; (void)path; (void)flags; (void)mode;
  (void)personality;
  return -EOPNOTSUPP;
}

int posix_uring_mkdirat(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t mode,
                        uint16_t personality)
{
  (void)dpp; (void)dirfd; (void)path; (void)mode; (void)personality;
  return -EOPNOTSUPP;
}

int posix_uring_fsetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, const void* value, size_t len,
                          int flags, uint16_t personality)
{
  (void)dpp; (void)fd; (void)name; (void)value; (void)len; (void)flags;
  (void)personality;
  return -EOPNOTSUPP;
}

int posix_uring_fgetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, void* value, size_t len,
                          uint16_t personality)
{
  (void)dpp; (void)fd; (void)name; (void)value; (void)len;
  (void)personality;
  return -EOPNOTSUPP;
}


void posix_uring_unregister_personality(const DoutPrefixProvider* dpp,
                                        uint16_t id)
{
  (void)dpp; (void)id;
}

UringReadWindow::UringReadWindow(const DoutPrefixProvider* dpp,
                                 optional_yield y, int fd, unsigned qd,
                                 int64_t chunk_size, bool direct_io,
                                 uint16_t personality)
  : impl(std::make_unique<Impl>())
{
  (void)dpp; (void)y; (void)fd; (void)qd; (void)chunk_size; (void)direct_io;
  (void)personality;
}

UringReadWindow::~UringReadWindow() = default;

int UringReadWindow::iterate(int64_t ofs, int64_t left,
                             std::function<int(bufferlist&, int)> on_chunk)
{
  (void)ofs; (void)left; (void)on_chunk;
  return -EOPNOTSUPP;
}

UringWriteWindow::UringWriteWindow(const DoutPrefixProvider* dpp,
                                   optional_yield y, int fd, unsigned qd,
                                   bool direct_io, uint16_t personality)
  : impl(std::make_unique<Impl>())
{
  (void)dpp; (void)y; (void)fd; (void)qd; (void)direct_io;
  (void)personality;
}

UringWriteWindow::~UringWriteWindow() = default;

int UringWriteWindow::process(bufferlist&& data, uint64_t offset)
{
  (void)data; (void)offset;
  return -EOPNOTSUPP;
}

int UringWriteWindow::drain()
{
  return -EOPNOTSUPP;
}

#endif /* HAVE_LIBURING */

} // namespace rgw::sal
