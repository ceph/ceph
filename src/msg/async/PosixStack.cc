// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#include <algorithm>
#include <deque>
#include <map>

// SO_ZEROCOPY comes from the kernel uapi headers while MSG_ZEROCOPY /
// MSG_ERRQUEUE come from libc, so a toolchain can define one without
// the other. Require both before compiling any of the send path.
#if defined(SO_ZEROCOPY) && defined(MSG_ZEROCOPY)
#define CEPH_HAVE_MSG_ZEROCOPY
#include <linux/errqueue.h>
#endif

#include "PosixStack.h"

#include "include/buffer.h"
#include "include/str_list.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "common/dout.h"
#include "msg/Messenger.h"
#include "include/compat.h"
#include "include/sock_compat.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "PosixStack "

class PosixConnectedSocketImpl final : public ConnectedSocketImpl {
  ceph::NetHandler &handler;
  int _fd;
  entity_addr_t sa;
  bool connected;
  CephContext *cct;
  PerfCounters *zc_logger;          // Worker l_msgr_* logger (may be null)
#ifdef CEPH_HAVE_MSG_ZEROCOPY
  // MSG_ZEROCOPY send state. Touched only on the owning worker
  // thread (send / drain / close), so no lock is needed. The zerocopy
  // perfcounters are driven HERE, at each event site, so they are
  // exact regardless of which teardown path closes the socket.
  //
  // Ids are tracked in a 64-bit space whose low 32 bits mirror the
  // kernel's per-socket counter; widen() maps a reported id back into
  // it. That keeps the wrap handling in one place and lets completed
  // ranges be ordered/merged with plain comparisons.
  bool zc_socket_enabled = false;   // SO_ZEROCOPY active on _fd
  bool zc_eligible = true;          // cleared for secure connections
  uint64_t zc_min_size = 0;         // ms_tcp_zerocopy_min_size
  uint64_t zc_next_id = 0;          // next id we will submit
  uint64_t zc_retire_id = 0;        // oldest id not yet known complete
  // Completed-but-not-yet-applied ranges (lo -> hi, inclusive, merged).
  // The kernel may deliver ranges out of order - a locally delivered
  // (looped-back) skb completes via skb_copy_ubufs() while an earlier
  // id is still unacked - so a pin may only retire once every id up to
  // and including its own has been reported.
  std::map<uint64_t, uint64_t> zc_done_ranges;
  uint64_t zc_pinned = 0;           // bytes awaiting completion
  size_t zc_last_bytes = 0;         // bytes of the last send() sent zero-copy
  unsigned zc_last_submitted = 0;   // zc sendmsg()s of the last send()
  struct ZeroCopyPin {
    uint64_t last_id;               // highest id covering these bytes
    uint64_t bytes;
    ceph::buffer::list held;        // refcounted: keeps the raws alive
  };
  std::deque<ZeroCopyPin> zc_pending;

  // Map a kernel-reported 32-bit id into the 64-bit space, resolving
  // wrap against the oldest outstanding id. Correct while fewer than
  // 2^32 ids are outstanding, which the pin queue bounds far below.
  uint64_t widen_zc_id(uint32_t id) const {
    uint64_t v = (zc_retire_id & ~0xffffffffULL) | id;
    if (v < zc_retire_id)
      v += 0x100000000ULL;
    return v;
  }
  // Merge [lo,hi] into zc_done_ranges, coalescing with neighbours.
  void record_zc_range(uint64_t lo, uint64_t hi) {
    auto [it, inserted] = zc_done_ranges.emplace(lo, hi);
    if (!inserted)
      it->second = std::max(it->second, hi);
    auto nx = std::next(it);
    while (nx != zc_done_ranges.end() && nx->first <= it->second + 1) {
      it->second = std::max(it->second, nx->second);
      nx = zc_done_ranges.erase(nx);
    }
    if (it != zc_done_ranges.begin()) {
      auto pv = std::prev(it);
      if (pv->second + 1 >= it->first) {
        pv->second = std::max(pv->second, it->second);
        zc_done_ranges.erase(it);
      }
    }
  }
#endif

 public:
  explicit PosixConnectedSocketImpl(ceph::NetHandler &h, const entity_addr_t &sa,
				    int f, bool connected, CephContext *c,
				    PerfCounters *plog)
      : handler(h), _fd(f), sa(sa), connected(connected), cct(c),
        zc_logger(plog) {
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    // Authoritative: did SO_ZEROCOPY actually take on this fd?
    // set_socket_options sets it only when ms_tcp_zerocopy is on.
    int zc = 0;
    socklen_t zl = sizeof(zc);
    if (cct &&
        ::getsockopt(_fd, SOL_SOCKET, SO_ZEROCOPY, &zc, &zl) == 0 && zc) {
      zc_socket_enabled = true;
      // type:size option: read via the with_legacy member (the
      // established ms_tcp_* idiom, e.g. ms_tcp_prefetch_max_size);
      // get_val<uint64_t> on a size option throws bad_variant_access.
      zc_min_size = cct->_conf->ms_tcp_zerocopy_min_size;
    }
#endif
  }

  int is_connected() override {
    if (connected)
      return 1;

    int r = handler.reconnect(sa, _fd);
    if (r == 0) {
      connected = true;
      return 1;
    } else if (r < 0) {
      return r;
    } else {
      return 0;
    }
  }

  ssize_t read(char *buf, size_t len) override {
    #ifdef _WIN32
    ssize_t r = ::recv(_fd, buf, len, 0);
    #else
    ssize_t r = ::read(_fd, buf, len);
    #endif
    if (r < 0)
      r = -ceph_sock_errno();
    return r;
  }

  // return the sent length
  // < 0 means error occurred
  #ifndef _WIN32
  // Per-send zero-copy accounting filled in by do_sendmsg().
  struct zc_send_stats {
    size_t bytes = 0;        // bytes handed to the kernel with MSG_ZEROCOPY
    unsigned submitted = 0;  // MSG_ZEROCOPY sendmsg()s == kernel ids consumed
    unsigned fallback = 0;   // ENOBUFS degradations to a copying send
  };

  static ssize_t do_sendmsg(int fd, struct msghdr &msg, unsigned len, bool more,
                            bool zerocopy, zc_send_stats *zc_)
  {
    size_t sent = 0;
    while (1) {
      MSGR_SIGPIPE_STOPPER;
      ssize_t r;
      int flags = MSG_NOSIGNAL | (more ? MSG_MORE : 0);
      bool zc = false;
#ifdef CEPH_HAVE_MSG_ZEROCOPY
      if (zerocopy) {
        flags |= MSG_ZEROCOPY;
        zc = true;
      }
#endif
      r = ::sendmsg(fd, &msg, flags);
      if (r < 0) {
        int err = ceph_sock_errno();
        if (err == EINTR) {
          continue;
#ifdef CEPH_HAVE_MSG_ZEROCOPY
        } else if (zc && err == ENOBUFS) {
          // optmem_max exhausted: transparent fallback to a copying
          // send for this syscall (not counted as a zero-copy submit,
          // and no kernel id is consumed - msg_zerocopy_alloc() fails
          // before sk_zckey is bumped).
          r = ::sendmsg(fd, &msg, flags & ~MSG_ZEROCOPY);
          zc = false;
          if (zc_)
            zc_->fallback++;
          if (r < 0) {
            int e2 = ceph_sock_errno();
            if (e2 == EINTR) continue;
            if (e2 == EAGAIN) break;
            // Report the bytes the kernel already accepted; the caller
            // must still pin them. The error resurfaces on the next
            // send() once there is no progress left to report.
            if (sent) break;
            return -e2;
          }
#endif
        } else if (err == EAGAIN) {
          break;
        } else {
          if (sent) break;
          return -err;
        }
      }

#ifdef CEPH_HAVE_MSG_ZEROCOPY
      if (zc && zc_) {
        zc_->submitted++;
        zc_->bytes += r;
      }
#endif
      sent += r;
      if (len == sent) break;

      while (r > 0) {
        if (msg.msg_iov[0].iov_len <= (size_t)r) {
          // drain this whole item
          r -= msg.msg_iov[0].iov_len;
          msg.msg_iov++;
          msg.msg_iovlen--;
        } else {
          msg.msg_iov[0].iov_base = (char *)msg.msg_iov[0].iov_base + r;
          msg.msg_iov[0].iov_len -= r;
          break;
        }
      }
    }
    return (ssize_t)sent;
  }

  ssize_t send(ceph::buffer::list &bl, bool more) override {
    size_t sent_bytes = 0;
    zc_send_stats zc_stats;
    int send_err = 0;
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    zc_last_bytes = 0;
    zc_last_submitted = 0;
#endif
    auto pb = std::cbegin(bl.buffers());
    uint64_t left_pbrs = bl.get_num_buffers();
    while (left_pbrs) {
      struct msghdr msg;
      struct iovec msgvec[IOV_MAX];
      uint64_t size = std::min<uint64_t>(left_pbrs, IOV_MAX);
      left_pbrs -= size;
      // FIPS zeroization audit 20191115: this memset is not security related.
      memset(&msg, 0, sizeof(msg));
      msg.msg_iovlen = size;
      msg.msg_iov = msgvec;
      unsigned msglen = 0;
      for (auto iov = msgvec; iov != msgvec + size; iov++) {
	iov->iov_base = (void*)(pb->c_str());
	iov->iov_len = pb->length();
	msglen += pb->length();
	++pb;
      }
      bool zerocopy = false;
#ifdef CEPH_HAVE_MSG_ZEROCOPY
      // Per-chunk: only large plaintext segments on a zero-copy-capable,
      // non-secure socket. Sub-threshold framing/control stays plain.
      // A zero-length chunk must never take this path: the kernel
      // consumes no id for it, which would desync our id mirror.
      zerocopy = zc_socket_enabled && zc_eligible &&
                 msglen && msglen >= zc_min_size;
#endif
      ssize_t r = do_sendmsg(_fd, msg, msglen, left_pbrs || more,
                             zerocopy, &zc_stats);
      if (r < 0) {
        // Nothing was accepted anywhere in this send(): report the
        // error. If an earlier chunk did make progress we must fall
        // through to pin it - the kernel owns those pages until it
        // says otherwise, even though the connection is about to fault.
        if (!sent_bytes) {
          send_err = r;
          break;
        }
        break;
      }

      // "r" is the remaining length
      sent_bytes += r;
      if (static_cast<unsigned>(r) < msglen)
        break;
      // only "r" == 0 continue
    }

    if (sent_bytes) {
      ceph::buffer::list sent_prefix;
      if (sent_bytes < bl.length()) {
        bl.splice(0, sent_bytes, &sent_prefix);
      } else {
        sent_prefix.swap(bl);
      }
#ifdef CEPH_HAVE_MSG_ZEROCOPY
      if (zc_stats.submitted) {
        // The kernel still references these pages until it posts a
        // MSG_ERRQUEUE completion; keep the (refcounted) buffers
        // pinned instead of releasing them here. The pin covers the
        // whole sent prefix even if some sub-threshold framing bytes
        // rode along - conservative for lifetime, never premature.
        // Byte *accounting* is not conservative in the same direction,
        // so it uses the measured zero-copy subset, not the prefix.
        const uint64_t last = zc_next_id + zc_stats.submitted - 1;
        zc_next_id += zc_stats.submitted;
        zc_pinned += sent_bytes;
        zc_pending.push_back(
          ZeroCopyPin{last, (uint64_t)sent_bytes, std::move(sent_prefix)});
        zc_last_bytes = zc_stats.bytes;
        zc_last_submitted = zc_stats.submitted;
        if (zc_logger) {
          // One pin == one zero-copy send operation. submitted counts
          // pins (the lifecycle unit), matching completed/force_dropped
          // which are also per-pin; the kernel-id stride may be >1 for
          // a partial/multi-chunk send and must NOT be the unit here.
          zc_logger->inc(l_msgr_zerocopy_submitted, 1);
          zc_logger->inc(l_msgr_send_bytes_zerocopy, zc_stats.bytes);
          zc_logger->inc(l_msgr_zerocopy_pinned_bytes, sent_bytes);
        }
      }
      // else sent_prefix is released here - identical to the old path.
      if (zc_stats.fallback && zc_logger)
        zc_logger->inc(l_msgr_zerocopy_fallback, zc_stats.fallback);
#endif
    }

    if (!sent_bytes && send_err)
      return send_err;
    return static_cast<ssize_t>(sent_bytes);
  }
  #else
  ssize_t send(bufferlist &bl, bool more) override
  {
    size_t total_sent_bytes = 0;
    auto pb = std::cbegin(bl.buffers());
    uint64_t left_pbrs = bl.get_num_buffers();
    while (left_pbrs) {
      WSABUF msgvec[IOV_MAX];
      uint64_t size = std::min<uint64_t>(left_pbrs, IOV_MAX);
      left_pbrs -= size;
      unsigned msglen = 0;
      for (auto iov = msgvec; iov != msgvec + size; iov++) {
        iov->buf = const_cast<char*>(pb->c_str());
        iov->len = pb->length();
        msglen += pb->length();
        ++pb;
      }
      DWORD sent_bytes = 0;
      DWORD flags = 0;
      if (more)
        flags |= MSG_PARTIAL;

      int ret_val = WSASend(_fd, msgvec, size, &sent_bytes, flags, NULL, NULL);
      if (ret_val)
        return -ret_val;

      total_sent_bytes += sent_bytes;
      if (static_cast<unsigned>(sent_bytes) < msglen)
        break;
    }

    if (total_sent_bytes) {
      bufferlist swapped;
      if (total_sent_bytes < bl.length()) {
        bl.splice(total_sent_bytes, bl.length()-total_sent_bytes, &swapped);
        bl.swap(swapped);
      } else {
        bl.clear();
      }
    }

    return static_cast<ssize_t>(total_sent_bytes);
  }
  #endif
  void shutdown() override {
    ::shutdown(_fd, SHUT_RDWR);
  }
  void close() override {
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    // Best-effort final retire of real completions first.
    drain_zerocopy_completions();
    if (!zc_pending.empty()) {
      // Pins are still outstanding, so the kernel holds page
      // references for data it has not finished sending. A graceful
      // close does NOT drop those: the socket lives on in the
      // background retransmitting from exactly the pages we are about
      // to hand back to the allocator. Force an abortive close - the
      // RST discards the retransmit queue - before releasing them.
      struct linger lg = {1, 0};
      ::setsockopt(_fd, SOL_SOCKET, SO_LINGER,
                   (SOCKOPT_VAL_TYPE)&lg, sizeof(lg));
      // Counted separately from real completions: force_dropped != 0
      // is the signal that pins are leaking or the peer stalled, and
      // is what makes submitted == completed + force_dropped a real
      // invariant instead of one that holds by construction.
      if (zc_logger)
        zc_logger->inc(l_msgr_zerocopy_force_dropped, zc_pending.size());
    }
    if (zc_pinned && zc_logger)
      zc_logger->dec(l_msgr_zerocopy_pinned_bytes, zc_pinned);
#endif
    if (_fd >= 0) {
      compat_closesocket(_fd);
      _fd = -1;
    }
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    // Only after the fd is gone: the kernel can no longer reference
    // these pages once the RST has torn the connection down.
    zc_pending.clear();
    zc_pinned = 0;
    zc_done_ranges.clear();
#endif
  }
  void set_priority(int sd, int prio, int domain) override {
    handler.set_priority(sd, prio, domain);
  }
  int fd() const override {
    return _fd;
  }

  void set_zerocopy_eligible(bool e) override {
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    zc_eligible = e;
#endif
  }
  size_t last_send_zerocopy_bytes() const override {
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    return zc_last_bytes;
#else
    return 0;
#endif
  }
  unsigned last_send_zerocopy_submitted() const override {
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    return zc_last_submitted;
#else
    return 0;
#endif
  }
  uint64_t pinned_zerocopy_bytes() const override {
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    return zc_pinned;
#else
    return 0;
#endif
  }
  unsigned pending_zerocopy_count() const override {
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    return zc_pending.size();
#else
    return 0;
#endif
  }
  ConnectedSocketImpl::zerocopy_drain drain_zerocopy_completions() override {
    ConnectedSocketImpl::zerocopy_drain out;
#ifdef CEPH_HAVE_MSG_ZEROCOPY
    if (zc_pending.empty() || _fd < 0)
      return out;   // nothing pinned: skip the recvmsg syscall
    char ctrl[512];
    while (true) {
      struct msghdr msg;
      // FIPS zeroization audit: not security related.
      memset(&msg, 0, sizeof(msg));
      msg.msg_control = ctrl;
      msg.msg_controllen = sizeof(ctrl);
      ssize_t r = ::recvmsg(_fd, &msg, MSG_ERRQUEUE);
      if (r < 0) {
        if (ceph_sock_errno() == EINTR)
          continue;   // not "drained" - retry
        break;        // EAGAIN: error queue drained
      }
      if (msg.msg_flags & MSG_CTRUNC) {
        // Completions were dropped on the floor; without them the
        // matching pins can never retire. Nothing to do here but keep
        // going - the buffer is sized for far more than the kernel
        // coalesces in practice, and close() force-drops the residue.
        ldout(cct, 1) << __func__
                      << " MSG_ERRQUEUE control data truncated" << dendl;
      }
      for (cmsghdr *cm = CMSG_FIRSTHDR(&msg); cm;
           cm = CMSG_NXTHDR(&msg, cm)) {
        if (!((cm->cmsg_level == SOL_IP && cm->cmsg_type == IP_RECVERR) ||
              (cm->cmsg_level == SOL_IPV6 && cm->cmsg_type == IPV6_RECVERR)))
          continue;
        auto *serr =
          reinterpret_cast<struct sock_extended_err *>(CMSG_DATA(cm));
        if (serr->ee_origin != SO_EE_ORIGIN_ZEROCOPY)
          continue;
        // A notification covers the inclusive id range [ee_info,
        // ee_data]. Both ends matter: ranges can arrive out of order,
        // so the high end alone must not be taken as "everything below
        // is done" - that would retire pins the kernel still owns.
        uint64_t lo = widen_zc_id(serr->ee_info);
        uint64_t hi = widen_zc_id(serr->ee_data);
        if (hi < lo)
          continue;                   // malformed; ignore
        if (serr->ee_code & SO_EE_CODE_ZEROCOPY_COPIED) {
          // The kernel copied after all (e.g. the egress path cannot
          // do scatter/gather). Count every id in the range, not the
          // notification, so this stays on the same unit as submitted.
          const uint64_t n = hi - lo + 1;
          out.fallback += n;
          if (zc_logger)
            zc_logger->inc(l_msgr_zerocopy_fallback, n);
        }
        if (hi < zc_retire_id)
          continue;                   // wholly retired already
        record_zc_range(std::max(lo, zc_retire_id), hi);

        // Apply only a run that is contiguous from the oldest
        // outstanding id; a range past a gap waits in the map.
        auto it = zc_done_ranges.begin();
        if (it != zc_done_ranges.end() && it->first <= zc_retire_id) {
          zc_retire_id = it->second + 1;
          zc_done_ranges.erase(it);
        }
        while (!zc_pending.empty() &&
               zc_pending.front().last_id < zc_retire_id) {
          const uint64_t b = zc_pending.front().bytes;
          out.completed++;
          out.retired_bytes += b;
          zc_pinned -= b;
          zc_pending.pop_front();
          if (zc_logger) {
            zc_logger->inc(l_msgr_zerocopy_completed, 1);
            zc_logger->dec(l_msgr_zerocopy_pinned_bytes, b);
          }
        }
      }
    }
#endif
    return out;
  }
  friend class PosixServerSocketImpl;
  friend class PosixNetworkStack;
};

class PosixServerSocketImpl : public ServerSocketImpl {
  ceph::NetHandler &handler;
  int _fd;

 public:
  explicit PosixServerSocketImpl(ceph::NetHandler &h, int f,
				 const entity_addr_t& listen_addr, unsigned slot)
    : ServerSocketImpl(listen_addr.get_type(), slot),
      handler(h), _fd(f) {}
  int accept(ConnectedSocket *sock, const SocketOptions &opts, entity_addr_t *out, Worker *w) override;
  void abort_accept() override {
    ::close(_fd);
    _fd = -1;
  }
  int fd() const override {
    return _fd;
  }
};

int PosixServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) {
  ceph_assert(sock);
  sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  int sd = accept_cloexec(_fd, (sockaddr*)&ss, &slen);
  if (sd < 0) {
    return -ceph_sock_errno();
  }

  int r = handler.set_nonblock(sd);
  if (r < 0) {
    ::close(sd);
    return -ceph_sock_errno();
  }

  r = handler.set_socket_options(sd, opt.nodelay, opt.rcbuf_size);
  if (r < 0) {
    ::close(sd);
    return -ceph_sock_errno();
  }

  ceph_assert(NULL != out); //out should not be NULL in accept connection

  out->set_type(addr_type);
  out->set_sockaddr((sockaddr*)&ss);
  handler.set_priority(sd, opt.priority, out->get_family());

  std::unique_ptr<PosixConnectedSocketImpl> csi(new PosixConnectedSocketImpl(handler, *out, sd, true, w->cct, w->get_perf_counter()));
  *sock = ConnectedSocket(std::move(csi));
  return 0;
}

void PosixWorker::initialize()
{
}

int PosixWorker::listen(entity_addr_t &sa,
			unsigned addr_slot,
			const SocketOptions &opt,
                        ServerSocket *sock)
{
  int listen_sd = net.create_socket(sa.get_family(), true);
  if (listen_sd < 0) {
    return -ceph_sock_errno();
  }

  int r = net.set_nonblock(listen_sd);
  if (r < 0) {
    ::close(listen_sd);
    return -ceph_sock_errno();
  }

  r = net.set_socket_options(listen_sd, opt.nodelay, opt.rcbuf_size);
  if (r < 0) {
    ::close(listen_sd);
    return -ceph_sock_errno();
  }

  r = ::bind(listen_sd, sa.get_sockaddr(), sa.get_sockaddr_len());
  if (r < 0) {
    r = -ceph_sock_errno();
    ldout(cct, 10) << __func__ << " unable to bind to " << sa.get_sockaddr()
                   << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  r = ::listen(listen_sd, cct->_conf->ms_tcp_listen_backlog);
  if (r < 0) {
    r = -ceph_sock_errno();
    lderr(cct) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  *sock = ServerSocket(
          std::unique_ptr<PosixServerSocketImpl>(
	    new PosixServerSocketImpl(net, listen_sd, sa, addr_slot)));
  return 0;
}

int PosixWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) {
  int sd;

  if (opts.nonblock) {
    sd = net.nonblock_connect(addr, opts.connect_bind_addr);
  } else {
    sd = net.connect(addr, opts.connect_bind_addr);
  }

  if (sd < 0) {
    return -ceph_sock_errno();
  }

  net.set_priority(sd, opts.priority, addr.get_family());
  *socket = ConnectedSocket(
      std::unique_ptr<PosixConnectedSocketImpl>(new PosixConnectedSocketImpl(net, addr, sd, !opts.nonblock, cct, get_perf_counter())));
  return 0;
}

PosixNetworkStack::PosixNetworkStack(CephContext *c, bool try_smc)
    : NetworkStack(c), try_smc(try_smc)
{
}
