// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "include/Context.h"
#include "common/errno.h"
#include "AsyncMessenger.h"
#include "AsyncConnection.h"

#include "include/sock_compat.h"

// Constant to limit starting sequence number to 2^31.  Nothing special about it, just a big number.  PLR
#define SEQ_MASK  0x7fffffff 

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _conn_prefix(_dout)
ostream& AsyncConnection::_conn_prefix(std::ostream *_dout) {
  return *_dout << "-- " << async_msgr->get_myinst().addr << " >> " << peer_addr << " conn(" << this
                << " sd=" << sd << " :" << port
                << " s=" << get_state_name(state)
                << " pgs=" << peer_global_seq
                << " cs=" << connect_seq
                << " l=" << policy.lossy
                << ").";
}

// Notes:
// 1. Don't dispatch any event when closed! It may cause AsyncConnection alive even if AsyncMessenger dead

const int AsyncConnection::TCP_PREFETCH_MIN_SIZE = 512;
const int ASYNC_COALESCE_THRESHOLD = 256;

class C_time_wakeup : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_time_wakeup(AsyncConnectionRef c): conn(c) {}
  void do_request(int fd_or_id) {
    conn->wakeup_from(fd_or_id);
  }
};

class C_handle_read : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_handle_read(AsyncConnectionRef c): conn(c) {}
  void do_request(int fd_or_id) {
    conn->process();
  }
};

class C_handle_write : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_handle_write(AsyncConnectionRef c): conn(c) {}
  void do_request(int fd) {
    conn->handle_write();
  }
};

class C_handle_reset : public EventCallback {
  AsyncMessenger *msgr;
  AsyncConnectionRef conn;

 public:
  C_handle_reset(AsyncMessenger *m, AsyncConnectionRef c): msgr(m), conn(c) {}
  void do_request(int id) {
    msgr->ms_deliver_handle_reset(conn.get());
  }
};

class C_handle_remote_reset : public EventCallback {
  AsyncMessenger *msgr;
  AsyncConnectionRef conn;

 public:
  C_handle_remote_reset(AsyncMessenger *m, AsyncConnectionRef c): msgr(m), conn(c) {}
  void do_request(int id) {
    msgr->ms_deliver_handle_remote_reset(conn.get());
  }
};

class C_handle_dispatch : public EventCallback {
  AsyncMessenger *msgr;
  Message *m;

 public:
  C_handle_dispatch(AsyncMessenger *msgr, Message *m): msgr(msgr), m(m) {}
  void do_request(int id) {
    msgr->ms_deliver_dispatch(m);
    delete this;
  }
};

class C_deliver_connect : public EventCallback {
  AsyncMessenger *msgr;
  AsyncConnectionRef conn;

 public:
  C_deliver_connect(AsyncMessenger *msgr, AsyncConnectionRef c): msgr(msgr), conn(c) {}
  void do_request(int id) {
    msgr->ms_deliver_handle_connect(conn.get());
  }
};

class C_deliver_accept : public EventCallback {
  AsyncMessenger *msgr;
  AsyncConnectionRef conn;

 public:
  C_deliver_accept(AsyncMessenger *msgr, AsyncConnectionRef c): msgr(msgr), conn(c) {}
  void do_request(int id) {
    msgr->ms_deliver_handle_accept(conn.get());
    delete this;
  }
};

class C_local_deliver : public EventCallback {
  AsyncConnectionRef conn;
 public:
  explicit C_local_deliver(AsyncConnectionRef c): conn(c) {}
  void do_request(int id) {
    conn->local_deliver();
  }
};


class C_clean_handler : public EventCallback {
  AsyncConnectionRef conn;
 public:
  explicit C_clean_handler(AsyncConnectionRef c): conn(c) {}
  void do_request(int id) {
    conn->cleanup_handler();
    delete this;
  }
};

static void alloc_aligned_buffer(bufferlist& data, unsigned len, unsigned off)
{
  // create a buffer to read into that matches the data alignment
  unsigned left = len;
  if (off & ~CEPH_PAGE_MASK) {
    // head
    unsigned head = 0;
    head = MIN(CEPH_PAGE_SIZE - (off & ~CEPH_PAGE_MASK), left);
    data.push_back(buffer::create(head));
    left -= head;
  }
  unsigned middle = left & CEPH_PAGE_MASK;
  if (middle > 0) {
    data.push_back(buffer::create_page_aligned(middle));
    left -= middle;
  }
  if (left) {
    data.push_back(buffer::create(left));
  }
}

AsyncConnection::AsyncConnection(CephContext *cct, AsyncMessenger *m, EventCenter *c, PerfCounters *p)
  : Connection(cct, m), delay_state(NULL), async_msgr(m), logger(p), global_seq(0), connect_seq(0), 
    peer_global_seq(0), out_seq(0), ack_left(0), in_seq(0), state(STATE_NONE), state_after_send(0), sd(-1),
    port(-1), write_lock("AsyncConnection::write_lock"), can_write(WriteStatus::NOWRITE),
    open_write(false), keepalive(false), lock("AsyncConnection::lock"), recv_buf(NULL),
    recv_max_prefetch(MIN(msgr->cct->_conf->ms_tcp_prefetch_max_size, TCP_PREFETCH_MIN_SIZE)),
    recv_start(0), recv_end(0), got_bad_auth(false), authorizer(NULL), replacing(false),
    is_reset_from_peer(false), once_ready(false), state_buffer(NULL), state_offset(0), net(cct), center(c)
{
  read_handler = new C_handle_read(this);
  write_handler = new C_handle_write(this);
  reset_handler = new C_handle_reset(async_msgr, this);
  remote_reset_handler = new C_handle_remote_reset(async_msgr, this);
  connect_handler = new C_deliver_connect(async_msgr, this);
  local_deliver_handler = new C_local_deliver(this);
  wakeup_handler = new C_time_wakeup(this);
  memset(msgvec, 0, sizeof(msgvec));
  // double recv_max_prefetch see "read_until"
  recv_buf = new char[2*recv_max_prefetch];
  state_buffer = new char[4096];
  logger->inc(l_msgr_created_connections);
}

AsyncConnection::~AsyncConnection()
{
  assert(out_q.empty());
  assert(sent.empty());
  delete authorizer;
  if (recv_buf)
    delete[] recv_buf;
  if (state_buffer)
    delete[] state_buffer;
  assert(!delay_state);
}

void AsyncConnection::maybe_start_delay_thread()
{
  if (!delay_state &&
      async_msgr->cct->_conf->ms_inject_delay_type.find(ceph_entity_type_name(peer_type)) != string::npos) {
    ldout(msgr->cct, 1) << __func__ << " setting up a delay queue" << dendl;
    delay_state = new DelayedDelivery(async_msgr, center);
  }
}

/* return -1 means `fd` occurs error or closed, it should be closed
 * return 0 means EAGAIN or EINTR */
ssize_t AsyncConnection::read_bulk(int fd, char *buf, unsigned len)
{
  ssize_t nread = ::read(fd, buf, len);
  if (nread == -1) {
    if (errno == EAGAIN || errno == EINTR) {
      nread = 0;
    } else {
      ldout(async_msgr->cct, 1) << __func__ << " reading from fd=" << fd
                          << " : "<< strerror(errno) << dendl;
      return -1;
    }
  } else if (nread == 0) {
    ldout(async_msgr->cct, 1) << __func__ << " peer close file descriptor "
                              << fd << dendl;
    return -1;
  }
  return nread;
}

/* 
 SIGPIPE suppression - for platforms without SO_NOSIGPIPE or MSG_NOSIGNAL
  http://krokisplace.blogspot.in/2010/02/suppressing-sigpipe-in-library.html 
  http://www.microhowto.info/howto/ignore_sigpipe_without_affecting_other_threads_in_a_process.html 
*/
void AsyncConnection::suppress_sigpipe()
{
#if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE)
  /*
    We want to ignore possible SIGPIPE that we can generate on write.
    SIGPIPE is delivered *synchronously* and *only* to the thread
    doing the write.  So if it is reported as already pending (which
    means the thread blocks it), then we do nothing: if we generate
    SIGPIPE, it will be merged with the pending one (there's no
    queuing), and that suits us well.  If it is not pending, we block
    it in this thread (and we avoid changing signal action, because it
    is per-process).
  */
  sigset_t pending;
  sigemptyset(&pending);
  sigpending(&pending);
  sigpipe_pending = sigismember(&pending, SIGPIPE);
  if (!sigpipe_pending) {
    sigset_t blocked;
    sigemptyset(&blocked);
    pthread_sigmask(SIG_BLOCK, &sigpipe_mask, &blocked);

    /* Maybe is was blocked already?  */
    sigpipe_unblock = ! sigismember(&blocked, SIGPIPE);
  }
#endif /* !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE) */
}


void AsyncConnection::restore_sigpipe()
{
#if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE)
  /*
    If SIGPIPE was pending already we do nothing.  Otherwise, if it
    become pending (i.e., we generated it), then we sigwait() it (thus
    clearing pending status).  Then we unblock SIGPIPE, but only if it
    were us who blocked it.
  */
  if (!sigpipe_pending) {
    sigset_t pending;
    sigemptyset(&pending);
    sigpending(&pending);
    if (sigismember(&pending, SIGPIPE)) {
      /*
        Protect ourselves from a situation when SIGPIPE was sent
        by the user to the whole process, and was delivered to
        other thread before we had a chance to wait for it.
      */
      static const struct timespec nowait = { 0, 0 };
      TEMP_FAILURE_RETRY(sigtimedwait(&sigpipe_mask, NULL, &nowait));
    }

    if (sigpipe_unblock)
      pthread_sigmask(SIG_UNBLOCK, &sigpipe_mask, NULL);
  }
#endif  /* !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE) */
}

// return the length of msg needed to be sent,
// < 0 means error occured
ssize_t AsyncConnection::do_sendmsg(struct msghdr &msg, unsigned len, bool more)
{
  suppress_sigpipe();

  while (len > 0) {
    ssize_t r;
#if defined(MSG_NOSIGNAL)
    r = ::sendmsg(sd, &msg, MSG_NOSIGNAL | (more ? MSG_MORE : 0));
#else
    r = ::sendmsg(sd, &msg, (more ? MSG_MORE : 0));
#endif /* defined(MSG_NOSIGNAL) */

    if (r == 0) {
      ldout(async_msgr->cct, 10) << __func__ << " sendmsg got r==0!" << dendl;
    } else if (r < 0) {
      if (errno == EINTR) {
        continue;
      } else if (errno == EAGAIN) {
        break;
      } else {
        ldout(async_msgr->cct, 1) << __func__ << " sendmsg error: " << cpp_strerror(errno) << dendl;
        restore_sigpipe();
        return r;
      }
    }

    len -= r;
    if (len == 0) break;

    // hrmph. drain r bytes from the front of our message.
    ldout(async_msgr->cct, 20) << __func__ << " short write did " << r << ", still have " << len << dendl;
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
  restore_sigpipe();
  return (ssize_t)len;
}

// return the remaining bytes, it may larger than the length of ptr
// else return < 0 means error
ssize_t AsyncConnection::_try_send(bool more)
{
  if (async_msgr->cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % async_msgr->cct->_conf->ms_inject_socket_failures == 0) {
      ldout(async_msgr->cct, 0) << __func__ << " injecting socket failure" << dendl;
      ::shutdown(sd, SHUT_RDWR);
    }
  }

  uint64_t sent_bytes = 0;
  list<bufferptr>::const_iterator pb = outcoming_bl.buffers().begin();
  uint64_t left_pbrs = outcoming_bl.buffers().size();
  while (left_pbrs) {
    struct msghdr msg;
    uint64_t size = MIN(left_pbrs, ASYNC_IOV_MAX);
    left_pbrs -= size;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iovlen = 0;
    msg.msg_iov = msgvec;
    unsigned msglen = 0;
    while (size > 0) {
      msgvec[msg.msg_iovlen].iov_base = (void*)(pb->c_str());
      msgvec[msg.msg_iovlen].iov_len = pb->length();
      msg.msg_iovlen++;
      msglen += pb->length();
      ++pb;
      size--;
    }

    ssize_t r = do_sendmsg(msg, msglen, left_pbrs || more);
    if (r < 0)
      return r;

    // "r" is the remaining length
    sent_bytes += msglen - r;
    if (r > 0) {
      ldout(async_msgr->cct, 5) << __func__ << " remaining " << r
                          << " needed to be sent, creating event for writing"
                          << dendl;
      break;
    }
    // only "r" == 0 continue
  }

  // trim already sent for outcoming_bl
  if (sent_bytes) {
    if (sent_bytes < outcoming_bl.length()) {
      outcoming_bl.splice(0, sent_bytes);
    } else {
      outcoming_bl.clear();
    }
  }

  ldout(async_msgr->cct, 20) << __func__ << " sent bytes " << sent_bytes
                             << " remaining bytes " << outcoming_bl.length() << dendl;

  if (!open_write && is_queued()) {
    center->create_file_event(sd, EVENT_WRITABLE, write_handler);
    open_write = true;
  }

  if (open_write && !is_queued()) {
    center->delete_file_event(sd, EVENT_WRITABLE);
    open_write = false;
  }

  return outcoming_bl.length();
}

// Because this func will be called multi times to populate
// the needed buffer, so the passed in bufferptr must be the same.
// Normally, only "read_message" will pass existing bufferptr in
//
// And it will uses readahead method to reduce small read overhead,
// "recv_buf" is used to store read buffer
//
// return the remaining bytes, 0 means this buffer is finished
// else return < 0 means error
ssize_t AsyncConnection::read_until(unsigned len, char *p)
{
  ldout(async_msgr->cct, 25) << __func__ << " len is " << len << " state_offset is "
                             << state_offset << dendl;

  if (async_msgr->cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % async_msgr->cct->_conf->ms_inject_socket_failures == 0) {
      ldout(async_msgr->cct, 0) << __func__ << " injecting socket failure" << dendl;
      ::shutdown(sd, SHUT_RDWR);
    }
  }

  ssize_t r = 0;
  uint64_t left = len - state_offset;
  if (recv_end > recv_start) {
    uint64_t to_read = MIN(recv_end - recv_start, left);
    memcpy(p, recv_buf+recv_start, to_read);
    recv_start += to_read;
    left -= to_read;
    ldout(async_msgr->cct, 25) << __func__ << " got " << to_read << " in buffer "
                               << " left is " << left << " buffer still has "
                               << recv_end - recv_start << dendl;
    if (left == 0) {
      return 0;
    }
    state_offset += to_read;
  }

  recv_end = recv_start = 0;
  /* nothing left in the prefetch buffer */
  if (len > recv_max_prefetch) {
    /* this was a large read, we don't prefetch for these */
    do {
      r = read_bulk(sd, p+state_offset, left);
      ldout(async_msgr->cct, 25) << __func__ << " read_bulk left is " << left << " got " << r << dendl;
      if (r < 0) {
        ldout(async_msgr->cct, 1) << __func__ << " read failed" << dendl;
        return -1;
      } else if (r == static_cast<int>(left)) {
        state_offset = 0;
        return 0;
      }
      state_offset += r;
      left -= r;
    } while (r > 0);
  } else {
    do {
      r = read_bulk(sd, recv_buf+recv_end, recv_max_prefetch);
      ldout(async_msgr->cct, 25) << __func__ << " read_bulk recv_end is " << recv_end
                                 << " left is " << left << " got " << r << dendl;
      if (r < 0) {
        ldout(async_msgr->cct, 1) << __func__ << " read failed" << dendl;
        return -1;
      }
      recv_end += r;
      if (r >= static_cast<int>(left)) {
        recv_start = len - state_offset;
        memcpy(p+state_offset, recv_buf, recv_start);
        state_offset = 0;
        return 0;
      }
      left -= r;
    } while (r > 0);
    memcpy(p+state_offset, recv_buf, recv_end-recv_start);
    state_offset += (recv_end - recv_start);
    recv_end = recv_start = 0;
  }
  ldout(async_msgr->cct, 25) << __func__ << " need len " << len << " remaining "
                             << len - state_offset << " bytes" << dendl;
  return len - state_offset;
}

void AsyncConnection::inject_delay() {
  if (async_msgr->cct->_conf->ms_inject_internal_delays) {
    ldout(async_msgr->cct, 10) << __func__ << " sleep for " << 
      async_msgr->cct->_conf->ms_inject_internal_delays << dendl;
    utime_t t;
    t.set_from_double(async_msgr->cct->_conf->ms_inject_internal_delays);
    t.sleep();
  }
}

void AsyncConnection::process()
{
  ssize_t r = 0;
  int prev_state = state;
  bool already_dispatch_writer = false;
  Mutex::Locker l(lock);
  do {
    ldout(async_msgr->cct, 20) << __func__ << " prev state is " << get_state_name(prev_state) << dendl;
    prev_state = state;
    switch (state) {
      case STATE_OPEN:
        {
          char tag = -1;
          r = read_until(sizeof(tag), &tag);
          if (r < 0) {
            ldout(async_msgr->cct, 1) << __func__ << " read tag failed" << dendl;
            goto fail;
          } else if (r > 0) {
            break;
          }

          if (tag == CEPH_MSGR_TAG_KEEPALIVE) {
            ldout(async_msgr->cct, 20) << __func__ << " got KEEPALIVE" << dendl;
	    set_last_keepalive(ceph_clock_now(NULL));
          } else if (tag == CEPH_MSGR_TAG_KEEPALIVE2) {
            state = STATE_OPEN_KEEPALIVE2;
          } else if (tag == CEPH_MSGR_TAG_KEEPALIVE2_ACK) {
            state = STATE_OPEN_KEEPALIVE2_ACK;
          } else if (tag == CEPH_MSGR_TAG_ACK) {
            state = STATE_OPEN_TAG_ACK;
          } else if (tag == CEPH_MSGR_TAG_MSG) {
            state = STATE_OPEN_MESSAGE_HEADER;
          } else if (tag == CEPH_MSGR_TAG_CLOSE) {
            state = STATE_OPEN_TAG_CLOSE;
          } else {
            ldout(async_msgr->cct, 0) << __func__ << " bad tag " << (int)tag << dendl;
            goto fail;
          }

          break;
        }

      case STATE_OPEN_KEEPALIVE2:
        {
          ceph_timespec *t;
          r = read_until(sizeof(*t), state_buffer);
          if (r < 0) {
            ldout(async_msgr->cct, 1) << __func__ << " read keeplive timespec failed" << dendl;
            goto fail;
          } else if (r > 0) {
            break;
          }

          ldout(async_msgr->cct, 30) << __func__ << " got KEEPALIVE2 tag ..." << dendl;
          t = (ceph_timespec*)state_buffer;
          utime_t kp_t = utime_t(*t);
          write_lock.Lock();
          _send_keepalive_or_ack(true, &kp_t);
	  write_lock.Unlock();
          ldout(async_msgr->cct, 20) << __func__ << " got KEEPALIVE2 " << kp_t << dendl;
	  set_last_keepalive(ceph_clock_now(NULL));
          state = STATE_OPEN;
          break;
        }

      case STATE_OPEN_KEEPALIVE2_ACK:
        {
          ceph_timespec *t;
          r = read_until(sizeof(*t), state_buffer);
          if (r < 0) {
            ldout(async_msgr->cct, 1) << __func__ << " read keeplive timespec failed" << dendl;
            goto fail;
          } else if (r > 0) {
            break;
          }

          t = (ceph_timespec*)state_buffer;
          set_last_keepalive_ack(utime_t(*t));
          ldout(async_msgr->cct, 20) << __func__ << " got KEEPALIVE_ACK" << dendl;
          state = STATE_OPEN;
          break;
        }

      case STATE_OPEN_TAG_ACK:
        {
          ceph_le64 *seq;
          r = read_until(sizeof(seq), state_buffer);
          if (r < 0) {
            ldout(async_msgr->cct, 1) << __func__ << " read ack seq failed" << dendl;
            goto fail;
          } else if (r > 0) {
            break;
          }

          seq = (ceph_le64*)state_buffer;
          ldout(async_msgr->cct, 20) << __func__ << " got ACK" << dendl;
          handle_ack(*seq);
          state = STATE_OPEN;
          break;
        }

      case STATE_OPEN_MESSAGE_HEADER:
        {
          ldout(async_msgr->cct, 20) << __func__ << " begin MSG" << dendl;
          ceph_msg_header header;
          ceph_msg_header_old oldheader;
          __u32 header_crc = 0;
          unsigned len;
          if (has_feature(CEPH_FEATURE_NOSRCADDR))
            len = sizeof(header);
          else
            len = sizeof(oldheader);

          r = read_until(len, state_buffer);
          if (r < 0) {
            ldout(async_msgr->cct, 1) << __func__ << " read message header failed" << dendl;
            goto fail;
          } else if (r > 0) {
            break;
          }

          ldout(async_msgr->cct, 20) << __func__ << " got MSG header" << dendl;

          if (has_feature(CEPH_FEATURE_NOSRCADDR)) {
            header = *((ceph_msg_header*)state_buffer);
            if (msgr->crcflags & MSG_CRC_HEADER)
              header_crc = ceph_crc32c(0, (unsigned char *)&header,
                                       sizeof(header) - sizeof(header.crc));
          } else {
            oldheader = *((ceph_msg_header_old*)state_buffer);
            // this is fugly
            memcpy(&header, &oldheader, sizeof(header));
            header.src = oldheader.src.name;
            header.reserved = oldheader.reserved;
            if (msgr->crcflags & MSG_CRC_HEADER) {
              header.crc = oldheader.crc;
              header_crc = ceph_crc32c(0, (unsigned char *)&oldheader, sizeof(oldheader) - sizeof(oldheader.crc));
            }
          }

          ldout(async_msgr->cct, 20) << __func__ << " got envelope type=" << header.type
                              << " src " << entity_name_t(header.src)
                              << " front=" << header.front_len
                              << " data=" << header.data_len
                              << " off " << header.data_off << dendl;

          // verify header crc
          if (msgr->crcflags & MSG_CRC_HEADER && header_crc != header.crc) {
            ldout(async_msgr->cct,0) << __func__ << " got bad header crc "
                                     << header_crc << " != " << header.crc << dendl;
            goto fail;
          }

          // Reset state
          data_buf.clear();
          front.clear();
          middle.clear();
          data.clear();
          recv_stamp = ceph_clock_now(async_msgr->cct);
          current_header = header;
          state = STATE_OPEN_MESSAGE_THROTTLE_MESSAGE;
          break;
        }

      case STATE_OPEN_MESSAGE_THROTTLE_MESSAGE:
        {
          if (policy.throttler_messages) {
            ldout(async_msgr->cct, 10) << __func__ << " wants " << 1 << " message from policy throttler "
                                       << policy.throttler_messages->get_current() << "/"
                                       << policy.throttler_messages->get_max() << dendl;
            if (!policy.throttler_messages->get_or_fail()) {
              ldout(async_msgr->cct, 10) << __func__ << " wants 1 message from policy throttle "
					 << policy.throttler_messages->get_current() << "/"
					 << policy.throttler_messages->get_max() << " failed, just wait." << dendl;
              // following thread pool deal with th full message queue isn't a
              // short time, so we can wait a ms.
              if (register_time_events.empty())
                register_time_events.insert(center->create_time_event(1000, wakeup_handler));
              break;
            }
          }

          state = STATE_OPEN_MESSAGE_THROTTLE_BYTES;
          break;
        }

      case STATE_OPEN_MESSAGE_THROTTLE_BYTES:
        {
          uint64_t message_size = current_header.front_len + current_header.middle_len + current_header.data_len;
          if (message_size) {
            if (policy.throttler_bytes) {
              ldout(async_msgr->cct, 10) << __func__ << " wants " << message_size << " bytes from policy throttler "
                                         << policy.throttler_bytes->get_current() << "/"
                                         << policy.throttler_bytes->get_max() << dendl;
              if (!policy.throttler_bytes->get_or_fail(message_size)) {
                ldout(async_msgr->cct, 10) << __func__ << " wants " << message_size << " bytes from policy throttler "
                                           << policy.throttler_bytes->get_current() << "/"
                                           << policy.throttler_bytes->get_max() << " failed, just wait." << dendl;
                // following thread pool deal with th full message queue isn't a
                // short time, so we can wait a ms.
                if (register_time_events.empty())
                  register_time_events.insert(center->create_time_event(1000, wakeup_handler));
                break;
              }
            }
          }

          throttle_stamp = ceph_clock_now(msgr->cct);
          state = STATE_OPEN_MESSAGE_READ_FRONT;
          break;
        }

      case STATE_OPEN_MESSAGE_READ_FRONT:
        {
          // read front
          unsigned front_len = current_header.front_len;
          if (front_len) {
            if (!front.length())
              front.push_back(buffer::create(front_len));

            r = read_until(front_len, front.c_str());
            if (r < 0) {
              ldout(async_msgr->cct, 1) << __func__ << " read message front failed" << dendl;
              goto fail;
            } else if (r > 0) {
              break;
            }

            ldout(async_msgr->cct, 20) << __func__ << " got front " << front.length() << dendl;
          }
          state = STATE_OPEN_MESSAGE_READ_MIDDLE;
        }

      case STATE_OPEN_MESSAGE_READ_MIDDLE:
        {
          // read middle
          unsigned middle_len = current_header.middle_len;
          if (middle_len) {
            if (!middle.length())
              middle.push_back(buffer::create(middle_len));

            r = read_until(middle_len, middle.c_str());
            if (r < 0) {
              ldout(async_msgr->cct, 1) << __func__ << " read message middle failed" << dendl;
              goto fail;
            } else if (r > 0) {
              break;
            }
            ldout(async_msgr->cct, 20) << __func__ << " got middle " << middle.length() << dendl;
          }

          state = STATE_OPEN_MESSAGE_READ_DATA_PREPARE;
        }

      case STATE_OPEN_MESSAGE_READ_DATA_PREPARE:
        {
          // read data
          unsigned data_len = le32_to_cpu(current_header.data_len);
          unsigned data_off = le32_to_cpu(current_header.data_off);
          if (data_len) {
            // get a buffer
            map<ceph_tid_t,pair<bufferlist,int> >::iterator p = rx_buffers.find(current_header.tid);
            if (p != rx_buffers.end()) {
              ldout(async_msgr->cct,10) << __func__ << " seleting rx buffer v " << p->second.second
                                  << " at offset " << data_off
                                  << " len " << p->second.first.length() << dendl;
              data_buf = p->second.first;
              // make sure it's big enough
              if (data_buf.length() < data_len)
                data_buf.push_back(buffer::create(data_len - data_buf.length()));
              data_blp = data_buf.begin();
            } else {
              ldout(async_msgr->cct,20) << __func__ << " allocating new rx buffer at offset " << data_off << dendl;
              alloc_aligned_buffer(data_buf, data_len, data_off);
              data_blp = data_buf.begin();
            }
          }

          msg_left = data_len;
          state = STATE_OPEN_MESSAGE_READ_DATA;
        }

      case STATE_OPEN_MESSAGE_READ_DATA:
        {
          while (msg_left > 0) {
            bufferptr bp = data_blp.get_current_ptr();
            unsigned read = MIN(bp.length(), msg_left);
            r = read_until(read, bp.c_str());
            if (r < 0) {
              ldout(async_msgr->cct, 1) << __func__ << " read data error " << dendl;
              goto fail;
            } else if (r > 0) {
              break;
            }

            data_blp.advance(read);
            data.append(bp, 0, read);
            msg_left -= read;
          }

          if (msg_left > 0)
            break;

          state = STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH;
        }

      case STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH:
        {
          ceph_msg_footer footer;
          ceph_msg_footer_old old_footer;
          unsigned len;
          // footer
          if (has_feature(CEPH_FEATURE_MSG_AUTH))
            len = sizeof(footer);
          else
            len = sizeof(old_footer);

          r = read_until(len, state_buffer);
          if (r < 0) {
            ldout(async_msgr->cct, 1) << __func__ << " read footer data error " << dendl;
            goto fail;
          } else if (r > 0) {
            break;
          }

          if (has_feature(CEPH_FEATURE_MSG_AUTH)) {
            footer = *((ceph_msg_footer*)state_buffer);
          } else {
            old_footer = *((ceph_msg_footer_old*)state_buffer);
            footer.front_crc = old_footer.front_crc;
            footer.middle_crc = old_footer.middle_crc;
            footer.data_crc = old_footer.data_crc;
            footer.sig = 0;
            footer.flags = old_footer.flags;
          }
          int aborted = (footer.flags & CEPH_MSG_FOOTER_COMPLETE) == 0;
          ldout(async_msgr->cct, 10) << __func__ << " aborted = " << aborted << dendl;
          if (aborted) {
            ldout(async_msgr->cct, 0) << __func__ << " got " << front.length() << " + " << middle.length() << " + " << data.length()
                                << " byte message.. ABORTED" << dendl;
            goto fail;
          }

          ldout(async_msgr->cct, 20) << __func__ << " got " << front.length() << " + " << middle.length()
                              << " + " << data.length() << " byte message" << dendl;
          Message *message = decode_message(async_msgr->cct, async_msgr->crcflags, current_header, footer, front, middle, data);
          if (!message) {
            ldout(async_msgr->cct, 1) << __func__ << " decode message failed " << dendl;
            goto fail;
          }

          //
          //  Check the signature if one should be present.  A zero return indicates success. PLR
          //

          if (session_security.get() == NULL) {
            ldout(async_msgr->cct, 10) << __func__ << " no session security set" << dendl;
          } else {
            if (session_security->check_message_signature(message)) {
              ldout(async_msgr->cct, 0) << __func__ << " Signature check failed" << dendl;
              message->put();
              goto fail;
            }
          }
          message->set_byte_throttler(policy.throttler_bytes);
          message->set_message_throttler(policy.throttler_messages);

          // store reservation size in message, so we don't get confused
          // by messages entering the dispatch queue through other paths.
          uint64_t message_size = current_header.front_len + current_header.middle_len + current_header.data_len;
          message->set_dispatch_throttle_size(message_size);

          message->set_recv_stamp(recv_stamp);
          message->set_throttle_stamp(throttle_stamp);
          message->set_recv_complete_stamp(ceph_clock_now(async_msgr->cct));

          // check received seq#.  if it is old, drop the message.  
          // note that incoming messages may skip ahead.  this is convenient for the client
          // side queueing because messages can't be renumbered, but the (kernel) client will
          // occasionally pull a message out of the sent queue to send elsewhere.  in that case
          // it doesn't matter if we "got" it or not.
          uint64_t cur_seq = in_seq.read();
          if (message->get_seq() <= cur_seq) {
            ldout(async_msgr->cct,0) << __func__ << " got old message "
                    << message->get_seq() << " <= " << cur_seq << " " << message << " " << *message
                    << ", discarding" << dendl;
            message->put();
            if (has_feature(CEPH_FEATURE_RECONNECT_SEQ) && async_msgr->cct->_conf->ms_die_on_old_message)
              assert(0 == "old msgs despite reconnect_seq feature");
            break;
          }
          if (message->get_seq() > cur_seq + 1) {
            ldout(async_msgr->cct, 0) << __func__ << " missed message?  skipped from seq "
                                      << cur_seq << " to " << message->get_seq() << dendl;
            if (async_msgr->cct->_conf->ms_die_on_skipped_message)
              assert(0 == "skipped incoming seq");
          }

          message->set_connection(this);

          // note last received message.
          in_seq.set(message->get_seq());
	  ldout(async_msgr->cct, 1) << " == rx == " << message->get_source() << " seq "
                                    << message->get_seq() << " " << message << " " << *message << dendl;

          ack_left.inc();
          // if send_message always send inline, it may have no
          // opportunity to send seq ack.
          if (!already_dispatch_writer) {
            center->dispatch_event_external(write_handler);
            already_dispatch_writer = true;
          }

          state = STATE_OPEN;

          logger->inc(l_msgr_recv_messages);
          logger->inc(l_msgr_recv_bytes, message_size + sizeof(ceph_msg_header) + sizeof(ceph_msg_footer));

          async_msgr->ms_fast_preprocess(message);
          if (delay_state) {
            utime_t release = message->get_recv_stamp();
            double delay_period = 0;
            if (rand() % 10000 < async_msgr->cct->_conf->ms_inject_delay_probability * 10000.0) {
              delay_period = async_msgr->cct->_conf->ms_inject_delay_max * (double)(rand() % 10000) / 10000.0;
              release += delay_period;
              ldout(async_msgr->cct, 1) << "queue_received will delay until " << release << " on "
                                        << message << " " << *message << dendl;
            }
            delay_state->queue(delay_period, release, message);
          } else if (async_msgr->ms_can_fast_dispatch(message)) {
            lock.Unlock();
            async_msgr->ms_fast_dispatch(message);
            lock.Lock();
          } else {
            center->dispatch_event_external(EventCallbackRef(new C_handle_dispatch(async_msgr, message)));
          }
          break;
        }

      case STATE_OPEN_TAG_CLOSE:
        {
          ldout(async_msgr->cct, 20) << __func__ << " got CLOSE" << dendl;
          _stop();
          return ;
        }

      case STATE_STANDBY:
        {
          ldout(async_msgr->cct, 20) << __func__ << " enter STANDY" << dendl;

          break;
        }

      case STATE_CLOSED:
        {
          if (sd >= 0)
            center->delete_file_event(sd, EVENT_READABLE);
          ldout(async_msgr->cct, 20) << __func__ << " socket closed" << dendl;
          break;
        }

      case STATE_WAIT:
        {
          ldout(async_msgr->cct, 20) << __func__ << " enter wait state" << dendl;
          break;
        }

      default:
        {
          if (_process_connection() < 0)
            goto fail;
          break;
        }
    }
  } while (prev_state != state);

  return;

 fail:
  // clean up state internal variables and states
  if (state >= STATE_CONNECTING_SEND_CONNECT_MSG &&
      state <= STATE_CONNECTING_READY) {
    delete authorizer;
    authorizer = NULL;
    got_bad_auth = false;
  }

  if (state > STATE_OPEN_MESSAGE_THROTTLE_MESSAGE &&
      state <= STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH
      && policy.throttler_messages) {
    ldout(async_msgr->cct,10) << __func__ << " releasing " << 1
                        << " message to policy throttler "
                        << policy.throttler_messages->get_current() << "/"
                        << policy.throttler_messages->get_max() << dendl;
    policy.throttler_messages->put();
  }
  if (state > STATE_OPEN_MESSAGE_THROTTLE_BYTES &&
      state <= STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH) {
    uint64_t message_size = current_header.front_len + current_header.middle_len + current_header.data_len;
    if (policy.throttler_bytes) {
      ldout(async_msgr->cct,10) << __func__ << " releasing " << message_size
                          << " bytes to policy throttler "
                          << policy.throttler_bytes->get_current() << "/"
                          << policy.throttler_bytes->get_max() << dendl;
      policy.throttler_bytes->put(message_size);
    }
  }
  fault();
}

ssize_t AsyncConnection::_process_connection()
{
  ssize_t r = 0;

  switch(state) {
    case STATE_WAIT_SEND:
      {
        Mutex::Locker l(write_lock);
        if (!outcoming_bl.length()) {
          assert(state_after_send);
          state = state_after_send;
          state_after_send = 0;
        }
        break;
      }

    case STATE_CONNECTING:
      {
        assert(!policy.server);

        // reset connect state variables
        got_bad_auth = false;
        delete authorizer;
        authorizer = NULL;
        memset(&connect_msg, 0, sizeof(connect_msg));
        memset(&connect_reply, 0, sizeof(connect_reply));

        global_seq = async_msgr->get_global_seq();
        // close old socket.  this is safe because we stopped the reader thread above.
        if (sd >= 0) {
          center->delete_file_event(sd, EVENT_READABLE|EVENT_WRITABLE);
          ::close(sd);
        }

        sd = net.nonblock_connect(get_peer_addr());
        if (sd < 0) {
          goto fail;
        }

        center->create_file_event(sd, EVENT_READABLE, read_handler);
        state = STATE_CONNECTING_RE;
        break;
      }

    case STATE_CONNECTING_RE:
      {
        r = net.reconnect(get_peer_addr(), sd);
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " reconnect failed " << dendl;
          goto fail;
        } else if (r > 0) {
          break;
        }

        state = STATE_CONNECTING_WAIT_BANNER;
        break;
      }

    case STATE_CONNECTING_WAIT_BANNER:
      {
        r = read_until(strlen(CEPH_BANNER), state_buffer);
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " read banner failed" << dendl;
          goto fail;
        } else if (r > 0) {
          break;
        }

        if (memcmp(state_buffer, CEPH_BANNER, strlen(CEPH_BANNER))) {
          ldout(async_msgr->cct, 0) << __func__ << " connect protocol error (bad banner) on peer "
                              << get_peer_addr() << dendl;
          goto fail;
        }

        ldout(async_msgr->cct, 10) << __func__ << " get banner, ready to send banner" << dendl;

        bufferlist bl;
        bl.append(state_buffer, strlen(CEPH_BANNER));
        r = try_send(bl);
        if (r == 0) {
          state = STATE_CONNECTING_WAIT_IDENTIFY_PEER;
          ldout(async_msgr->cct, 10) << __func__ << " connect write banner done: "
                               << get_peer_addr() << dendl;
        } else if (r > 0) {
          state = STATE_WAIT_SEND;
          state_after_send = STATE_CONNECTING_WAIT_IDENTIFY_PEER;
          ldout(async_msgr->cct, 10) << __func__ << " connect wait for write banner: "
                               << get_peer_addr() << dendl;
        } else {
          goto fail;
        }
        break;
      }

    case STATE_CONNECTING_WAIT_IDENTIFY_PEER:
      {
        entity_addr_t paddr, peer_addr_for_me;
        bufferlist myaddrbl;

        r = read_until(sizeof(paddr)*2, state_buffer);
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " read identify peeraddr failed" << dendl;
          goto fail;
        } else if (r > 0) {
          break;
        }

        bufferlist bl;
        bl.append(state_buffer, sizeof(paddr)*2);
        bufferlist::iterator p = bl.begin();
        try {
          ::decode(paddr, p);
          ::decode(peer_addr_for_me, p);
        } catch (const buffer::error& e) {
          lderr(async_msgr->cct) << __func__ <<  " decode peer addr failed " << dendl;
          goto fail;
        }
        ldout(async_msgr->cct, 20) << __func__ <<  " connect read peer addr "
                             << paddr << " on socket " << sd << dendl;
        if (peer_addr != paddr) {
          if (paddr.is_blank_ip() && peer_addr.get_port() == paddr.get_port() &&
              peer_addr.get_nonce() == paddr.get_nonce()) {
            ldout(async_msgr->cct, 0) << __func__ <<  " connect claims to be " << paddr
                                << " not " << peer_addr
                                << " - presumably this is the same node!" << dendl;
          } else {
            ldout(async_msgr->cct, 0) << __func__ << " connect claims to be "
                                << paddr << " not " << peer_addr << " - wrong node!" << dendl;
            goto fail;
          }
        }

        ldout(async_msgr->cct, 20) << __func__ << " connect peer addr for me is " << peer_addr_for_me << dendl;
        lock.Unlock();
        async_msgr->learned_addr(peer_addr_for_me);
        if (async_msgr->cct->_conf->ms_inject_internal_delays) {
          if (rand() % async_msgr->cct->_conf->ms_inject_socket_failures == 0) {
            ldout(msgr->cct, 10) << __func__ << " sleep for "
                                 << async_msgr->cct->_conf->ms_inject_internal_delays << dendl;
            utime_t t;
            t.set_from_double(async_msgr->cct->_conf->ms_inject_internal_delays);
            t.sleep();
          }
        }

        lock.Lock();
        if (state != STATE_CONNECTING_WAIT_IDENTIFY_PEER) {
          ldout(async_msgr->cct, 1) << __func__ << " state changed while learned_addr, mark_down or "
                                    << " replacing must be happened just now" << dendl;
          return 0;
        }

        ::encode(async_msgr->get_myaddr(), myaddrbl);
        r = try_send(myaddrbl);
        if (r == 0) {
          state = STATE_CONNECTING_SEND_CONNECT_MSG;
          ldout(async_msgr->cct, 10) << __func__ << " connect sent my addr "
              << async_msgr->get_myaddr() << dendl;
        } else if (r > 0) {
          state = STATE_WAIT_SEND;
          state_after_send = STATE_CONNECTING_SEND_CONNECT_MSG;
          ldout(async_msgr->cct, 10) << __func__ << " connect send my addr done: "
              << async_msgr->get_myaddr() << dendl;
        } else {
          ldout(async_msgr->cct, 2) << __func__ << " connect couldn't write my addr, "
              << cpp_strerror(errno) << dendl;
          goto fail;
        }

        break;
      }

    case STATE_CONNECTING_SEND_CONNECT_MSG:
      {
        if (!got_bad_auth) {
          delete authorizer;
          authorizer = async_msgr->get_authorizer(peer_type, false);
        }
        bufferlist bl;

        connect_msg.features = policy.features_supported;
        connect_msg.host_type = async_msgr->get_myinst().name.type();
        connect_msg.global_seq = global_seq;
        connect_msg.connect_seq = connect_seq;
        connect_msg.protocol_version = async_msgr->get_proto_version(peer_type, true);
        connect_msg.authorizer_protocol = authorizer ? authorizer->protocol : 0;
        connect_msg.authorizer_len = authorizer ? authorizer->bl.length() : 0;
        if (authorizer)
          ldout(async_msgr->cct, 10) << __func__ <<  " connect_msg.authorizer_len="
                                     << connect_msg.authorizer_len << " protocol="
                                     << connect_msg.authorizer_protocol << dendl;
        connect_msg.flags = 0;
        if (policy.lossy)
          connect_msg.flags |= CEPH_MSG_CONNECT_LOSSY;  // this is fyi, actually, server decides!
        bl.append((char*)&connect_msg, sizeof(connect_msg));
        if (authorizer) {
          bl.append(authorizer->bl.c_str(), authorizer->bl.length());
        }
        ldout(async_msgr->cct, 10) << __func__ << " connect sending gseq=" << global_seq << " cseq="
            << connect_seq << " proto=" << connect_msg.protocol_version << dendl;

        r = try_send(bl);
        if (r == 0) {
          state = STATE_CONNECTING_WAIT_CONNECT_REPLY;
          ldout(async_msgr->cct,20) << __func__ << " connect wrote (self +) cseq, waiting for reply" << dendl;
        } else if (r > 0) {
          state = STATE_WAIT_SEND;
          state_after_send = STATE_CONNECTING_WAIT_CONNECT_REPLY;
          ldout(async_msgr->cct, 10) << __func__ << " continue send reply " << dendl;
        } else {
          ldout(async_msgr->cct, 2) << __func__ << " connect couldn't send reply "
              << cpp_strerror(errno) << dendl;
          goto fail;
        }

        break;
      }

    case STATE_CONNECTING_WAIT_CONNECT_REPLY:
      {
        r = read_until(sizeof(connect_reply), state_buffer);
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " read connect reply failed" << dendl;
          goto fail;
        } else if (r > 0) {
          break;
        }

        connect_reply = *((ceph_msg_connect_reply*)state_buffer);
        connect_reply.features = ceph_sanitize_features(connect_reply.features);

        ldout(async_msgr->cct, 20) << __func__ << " connect got reply tag " << (int)connect_reply.tag
                             << " connect_seq " << connect_reply.connect_seq << " global_seq "
                             << connect_reply.global_seq << " proto " << connect_reply.protocol_version
                             << " flags " << (int)connect_reply.flags << " features "
                             << connect_reply.features << dendl;
        state = STATE_CONNECTING_WAIT_CONNECT_REPLY_AUTH;

        break;
      }

    case STATE_CONNECTING_WAIT_CONNECT_REPLY_AUTH:
      {
        bufferlist authorizer_reply;
        if (connect_reply.authorizer_len) {
          ldout(async_msgr->cct, 10) << __func__ << " reply.authorizer_len=" << connect_reply.authorizer_len << dendl;
          assert(connect_reply.authorizer_len < 4096);
          r = read_until(connect_reply.authorizer_len, state_buffer);
          if (r < 0) {
            ldout(async_msgr->cct, 1) << __func__ << " read connect reply authorizer failed" << dendl;
            goto fail;
          } else if (r > 0) {
            break;
          }

          authorizer_reply.append(state_buffer, connect_reply.authorizer_len);
          bufferlist::iterator iter = authorizer_reply.begin();
          if (authorizer && !authorizer->verify_reply(iter)) {
            ldout(async_msgr->cct, 0) << __func__ << " failed verifying authorize reply" << dendl;
            goto fail;
          }
        }
        r = handle_connect_reply(connect_msg, connect_reply);
        if (r < 0)
          goto fail;

        // state must be changed!
        assert(state != STATE_CONNECTING_WAIT_CONNECT_REPLY_AUTH);
        break;
      }

    case STATE_CONNECTING_WAIT_ACK_SEQ:
      {
        uint64_t newly_acked_seq = 0;

        r = read_until(sizeof(newly_acked_seq), state_buffer);
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " read connect ack seq failed" << dendl;
          goto fail;
        } else if (r > 0) {
          break;
        }

        newly_acked_seq = *((uint64_t*)state_buffer);
        ldout(async_msgr->cct, 2) << __func__ << " got newly_acked_seq " << newly_acked_seq
                            << " vs out_seq " << out_seq.read() << dendl;
        discard_requeued_up_to(newly_acked_seq);
        //while (newly_acked_seq > out_seq.read()) {
        //  Message *m = _get_next_outgoing(NULL);
        //  assert(m);
        //  ldout(async_msgr->cct, 2) << __func__ << " discarding previously sent " << m->get_seq()
        //                      << " " << *m << dendl;
        //  assert(m->get_seq() <= newly_acked_seq);
        //  m->put();
        //  out_seq.inc();
        //}

        bufferlist bl;
        uint64_t s = in_seq.read();
        bl.append((char*)&s, sizeof(s));
        r = try_send(bl);
        if (r == 0) {
          state = STATE_CONNECTING_READY;
          ldout(async_msgr->cct, 10) << __func__ << " send in_seq done " << dendl;
        } else if (r > 0) {
          state_after_send = STATE_CONNECTING_READY;
          state = STATE_WAIT_SEND;
          ldout(async_msgr->cct, 10) << __func__ << " continue send in_seq " << dendl;
        } else {
          goto fail;
        }
        break;
      }

    case STATE_CONNECTING_READY:
      {
        // hooray!
        peer_global_seq = connect_reply.global_seq;
        policy.lossy = connect_reply.flags & CEPH_MSG_CONNECT_LOSSY;
        state = STATE_OPEN;
        once_ready = true;
        connect_seq += 1;
        assert(connect_seq == connect_reply.connect_seq);
        backoff = utime_t();
        set_features((uint64_t)connect_reply.features & (uint64_t)connect_msg.features);
        ldout(async_msgr->cct, 10) << __func__ << " connect success " << connect_seq
                                   << ", lossy = " << policy.lossy << ", features "
                                   << get_features() << dendl;

        // If we have an authorizer, get a new AuthSessionHandler to deal with ongoing security of the
        // connection.  PLR
        if (authorizer != NULL) {
          session_security.reset(
              get_auth_session_handler(async_msgr->cct,
                                       authorizer->protocol,
                                       authorizer->session_key,
                                       get_features()));
        } else {
          // We have no authorizer, so we shouldn't be applying security to messages in this AsyncConnection.  PLR
          session_security.reset();
        }

        center->dispatch_event_external(connect_handler);
        async_msgr->ms_deliver_handle_fast_connect(this);

        // message may in queue between last _try_send and connection ready
        // write event may already notify and we need to force scheduler again
        write_lock.Lock();
        can_write = WriteStatus::CANWRITE;
        if (is_queued())
          center->dispatch_event_external(write_handler);
        write_lock.Unlock();
        maybe_start_delay_thread();
        break;
      }

    case STATE_ACCEPTING:
      {
        bufferlist bl;

        if (net.set_nonblock(sd) < 0)
          goto fail;

        net.set_socket_options(sd);

        bl.append(CEPH_BANNER, strlen(CEPH_BANNER));

        ::encode(async_msgr->get_myaddr(), bl);
        port = async_msgr->get_myaddr().get_port();
        // and peer's socket addr (they might not know their ip)
	sockaddr_storage ss;
        socklen_t len = sizeof(ss);
        r = ::getpeername(sd, (sockaddr*)&ss, &len);
        if (r < 0) {
          ldout(async_msgr->cct, 0) << __func__ << " failed to getpeername "
                              << cpp_strerror(errno) << dendl;
          goto fail;
        }
	socket_addr.set_sockaddr((sockaddr*)&ss);
        ::encode(socket_addr, bl);
        ldout(async_msgr->cct, 1) << __func__ << " sd=" << sd << " " << socket_addr << dendl;

        r = try_send(bl);
        if (r == 0) {
          state = STATE_ACCEPTING_WAIT_BANNER_ADDR;
          ldout(async_msgr->cct, 10) << __func__ << " write banner and addr done: "
            << get_peer_addr() << dendl;
        } else if (r > 0) {
          state = STATE_WAIT_SEND;
          state_after_send = STATE_ACCEPTING_WAIT_BANNER_ADDR;
          ldout(async_msgr->cct, 10) << __func__ << " wait for write banner and addr: "
                              << get_peer_addr() << dendl;
        } else {
          goto fail;
        }

        break;
      }
    case STATE_ACCEPTING_WAIT_BANNER_ADDR:
      {
        bufferlist addr_bl;
        entity_addr_t peer_addr;

        r = read_until(strlen(CEPH_BANNER) + sizeof(peer_addr), state_buffer);
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " read peer banner and addr failed" << dendl;
          goto fail;
        } else if (r > 0) {
          break;
        }

        if (memcmp(state_buffer, CEPH_BANNER, strlen(CEPH_BANNER))) {
          ldout(async_msgr->cct, 1) << __func__ << " accept peer sent bad banner '" << state_buffer
                                    << "' (should be '" << CEPH_BANNER << "')" << dendl;
          goto fail;
        }

        addr_bl.append(state_buffer+strlen(CEPH_BANNER), sizeof(peer_addr));
        {
          bufferlist::iterator ti = addr_bl.begin();
          ::decode(peer_addr, ti);
        }

        ldout(async_msgr->cct, 10) << __func__ << " accept peer addr is " << peer_addr << dendl;
        if (peer_addr.is_blank_ip()) {
          // peer apparently doesn't know what ip they have; figure it out for them.
          int port = peer_addr.get_port();
          peer_addr.addr = socket_addr.addr;
          peer_addr.set_port(port);
          ldout(async_msgr->cct, 0) << __func__ << " accept peer addr is really " << peer_addr
                             << " (socket is " << socket_addr << ")" << dendl;
        }
        set_peer_addr(peer_addr);  // so that connection_state gets set up
        state = STATE_ACCEPTING_WAIT_CONNECT_MSG;
        break;
      }

    case STATE_ACCEPTING_WAIT_CONNECT_MSG:
      {
        r = read_until(sizeof(connect_msg), state_buffer);
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " read connect msg failed" << dendl;
          goto fail;
        } else if (r > 0) {
          break;
        }

        connect_msg = *((ceph_msg_connect*)state_buffer);
        // sanitize features
        connect_msg.features = ceph_sanitize_features(connect_msg.features);
        state = STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH;
        break;
      }

    case STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH:
      {
        bufferlist authorizer_bl, authorizer_reply;

        if (connect_msg.authorizer_len) {
          r = read_until(connect_msg.authorizer_len, state_buffer);
          if (r < 0) {
            ldout(async_msgr->cct, 1) << __func__ << " read connect msg failed" << dendl;
            goto fail;
          } else if (r > 0) {
            break;
          }
          authorizer_bl.append(state_buffer, connect_msg.authorizer_len);
        }

        ldout(async_msgr->cct, 20) << __func__ << " accept got peer connect_seq "
                             << connect_msg.connect_seq << " global_seq "
                             << connect_msg.global_seq << dendl;
        set_peer_type(connect_msg.host_type);
        policy = async_msgr->get_policy(connect_msg.host_type);
        ldout(async_msgr->cct, 10) << __func__ << " accept of host_type " << connect_msg.host_type
                                   << ", policy.lossy=" << policy.lossy << " policy.server="
                                   << policy.server << " policy.standby=" << policy.standby
                                   << " policy.resetcheck=" << policy.resetcheck << dendl;

        r = handle_connect_msg(connect_msg, authorizer_bl, authorizer_reply);
        if (r < 0)
          goto fail;

        // state is changed by "handle_connect_msg"
        assert(state != STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH);
        break;
      }

    case STATE_ACCEPTING_WAIT_SEQ:
      {
        uint64_t newly_acked_seq;
        r = read_until(sizeof(newly_acked_seq), state_buffer);
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " read ack seq failed" << dendl;
          goto fail_registered;
        } else if (r > 0) {
          break;
        }

        newly_acked_seq = *((uint64_t*)state_buffer);
        ldout(async_msgr->cct, 2) << __func__ << " accept get newly_acked_seq " << newly_acked_seq << dendl;
        discard_requeued_up_to(newly_acked_seq);
        state = STATE_ACCEPTING_READY;
        break;
      }

    case STATE_ACCEPTING_READY:
      {
        ldout(async_msgr->cct, 20) << __func__ << " accept done" << dendl;
        state = STATE_OPEN;
        memset(&connect_msg, 0, sizeof(connect_msg));
        write_lock.Lock();
        can_write = WriteStatus::CANWRITE;
        if (is_queued())
          center->dispatch_event_external(write_handler);
        write_lock.Unlock();
        maybe_start_delay_thread();
        break;
      }

    default:
      {
        lderr(async_msgr->cct) << __func__ << " bad state: " << state << dendl;
        assert(0);
      }
  }

  return 0;

fail_registered:
  ldout(async_msgr->cct, 10) << "accept fault after register" << dendl;
  inject_delay();

fail:
  return -1;
}

int AsyncConnection::handle_connect_reply(ceph_msg_connect &connect, ceph_msg_connect_reply &reply)
{
  uint64_t feat_missing;
  if (reply.tag == CEPH_MSGR_TAG_FEATURES) {
    ldout(async_msgr->cct, 0) << __func__ << " connect protocol feature mismatch, my "
                        << std::hex << connect.features << " < peer "
                        << reply.features << " missing "
                        << (reply.features & ~policy.features_supported)
                        << std::dec << dendl;
    goto fail;
  }

  if (reply.tag == CEPH_MSGR_TAG_BADPROTOVER) {
    ldout(async_msgr->cct, 0) << __func__ << " connect protocol version mismatch, my "
                        << connect.protocol_version << " != " << reply.protocol_version
                        << dendl;
    goto fail;
  }

  if (reply.tag == CEPH_MSGR_TAG_BADAUTHORIZER) {
    ldout(async_msgr->cct,0) << __func__ << " connect got BADAUTHORIZER" << dendl;
    if (got_bad_auth)
      goto fail;
    got_bad_auth = true;
    delete authorizer;
    authorizer = async_msgr->get_authorizer(peer_type, true);  // try harder
    state = STATE_CONNECTING_SEND_CONNECT_MSG;
  }
  if (reply.tag == CEPH_MSGR_TAG_RESETSESSION) {
    ldout(async_msgr->cct, 0) << __func__ << " connect got RESETSESSION" << dendl;
    was_session_reset();
    state = STATE_CONNECTING_SEND_CONNECT_MSG;
  }
  if (reply.tag == CEPH_MSGR_TAG_RETRY_GLOBAL) {
    global_seq = async_msgr->get_global_seq(reply.global_seq);
    ldout(async_msgr->cct, 10) << __func__ << " connect got RETRY_GLOBAL "
                         << reply.global_seq << " chose new "
                         << global_seq << dendl;
    state = STATE_CONNECTING_SEND_CONNECT_MSG;
  }
  if (reply.tag == CEPH_MSGR_TAG_RETRY_SESSION) {
    assert(reply.connect_seq > connect_seq);
    ldout(async_msgr->cct, 10) << __func__ << " connect got RETRY_SESSION "
                               << connect_seq << " -> "
                               << reply.connect_seq << dendl;
    connect_seq = reply.connect_seq;
    state = STATE_CONNECTING_SEND_CONNECT_MSG;
  }
  if (reply.tag == CEPH_MSGR_TAG_WAIT) {
    ldout(async_msgr->cct, 3) << __func__ << " connect got WAIT (connection race)" << dendl;
    state = STATE_WAIT;
  }

  feat_missing = policy.features_required & ~(uint64_t)connect_reply.features;
  if (feat_missing) {
    ldout(async_msgr->cct, 1) << __func__ << " missing required features " << std::hex
                              << feat_missing << std::dec << dendl;
    goto fail;
  }

  if (reply.tag == CEPH_MSGR_TAG_SEQ) {
    ldout(async_msgr->cct, 10) << __func__ << " got CEPH_MSGR_TAG_SEQ, reading acked_seq and writing in_seq" << dendl;
    state = STATE_CONNECTING_WAIT_ACK_SEQ;
  }
  if (reply.tag == CEPH_MSGR_TAG_READY) {
    ldout(async_msgr->cct, 10) << __func__ << " got CEPH_MSGR_TAG_READY " << dendl;
    state = STATE_CONNECTING_READY;
  }

  return 0;

 fail:
  return -1;
}

ssize_t AsyncConnection::handle_connect_msg(ceph_msg_connect &connect, bufferlist &authorizer_bl,
                                            bufferlist &authorizer_reply)
{
  ssize_t r = 0;
  ceph_msg_connect_reply reply;
  bufferlist reply_bl;

  memset(&reply, 0, sizeof(reply));
  reply.protocol_version = async_msgr->get_proto_version(peer_type, false);

  // mismatch?
  ldout(async_msgr->cct, 10) << __func__ << " accept my proto " << reply.protocol_version
                      << ", their proto " << connect.protocol_version << dendl;
  if (connect.protocol_version != reply.protocol_version) {
    return _reply_accept(CEPH_MSGR_TAG_BADPROTOVER, connect, reply, authorizer_reply);
  }
  // require signatures for cephx?
  if (connect.authorizer_protocol == CEPH_AUTH_CEPHX) {
    if (peer_type == CEPH_ENTITY_TYPE_OSD ||
        peer_type == CEPH_ENTITY_TYPE_MDS) {
      if (async_msgr->cct->_conf->cephx_require_signatures ||
          async_msgr->cct->_conf->cephx_cluster_require_signatures) {
        ldout(async_msgr->cct, 10) << __func__ << " using cephx, requiring MSG_AUTH feature bit for cluster" << dendl;
        policy.features_required |= CEPH_FEATURE_MSG_AUTH;
      }
    } else {
      if (async_msgr->cct->_conf->cephx_require_signatures ||
          async_msgr->cct->_conf->cephx_service_require_signatures) {
        ldout(async_msgr->cct, 10) << __func__ << " using cephx, requiring MSG_AUTH feature bit for service" << dendl;
        policy.features_required |= CEPH_FEATURE_MSG_AUTH;
      }
    }
  }
  uint64_t feat_missing = policy.features_required & ~(uint64_t)connect.features;
  if (feat_missing) {
    ldout(async_msgr->cct, 1) << __func__ << " peer missing required features "
                        << std::hex << feat_missing << std::dec << dendl;
    return _reply_accept(CEPH_MSGR_TAG_FEATURES, connect, reply, authorizer_reply);
  }

  bool authorizer_valid;
  if (!async_msgr->verify_authorizer(this, peer_type, connect.authorizer_protocol, authorizer_bl,
                               authorizer_reply, authorizer_valid, session_key) || !authorizer_valid) {
    ldout(async_msgr->cct,0) << __func__ << ": got bad authorizer" << dendl;
    session_security.reset();
    return _reply_accept(CEPH_MSGR_TAG_BADAUTHORIZER, connect, reply, authorizer_reply);
  }

  // We've verified the authorizer for this AsyncConnection, so set up the session security structure.  PLR
  ldout(async_msgr->cct, 10) << __func__ << " accept setting up session_security." << dendl;

  // existing?
  lock.Unlock();
  AsyncConnectionRef existing = async_msgr->lookup_conn(peer_addr);

  inject_delay();

  lock.Lock();
  if (state != STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH) {
    ldout(async_msgr->cct, 1) << __func__ << " state changed while accept, it must be mark_down" << dendl;
    assert(state == STATE_CLOSED);
    goto fail;
  }

  if (existing == this)
    existing = NULL;
  if (existing) {
    // There is no possible that existing connection will acquire this
    // connection's lock
    existing->lock.Lock(true);  // skip lockdep check (we are locking a second AsyncConnection here)

    if (existing->replacing || existing->state == STATE_CLOSED) {
      ldout(async_msgr->cct, 1) << __func__ << " existing racing replace or mark_down happened while replacing."
                                << " existing_state=" << get_state_name(existing->state) << dendl;
      reply.global_seq = existing->peer_global_seq;
      r = _reply_accept(CEPH_MSGR_TAG_RETRY_GLOBAL, connect, reply, authorizer_reply);
      existing->lock.Unlock();
      if (r < 0)
        goto fail;
      return 0;
    }

    if (connect.global_seq < existing->peer_global_seq) {
      ldout(async_msgr->cct, 10) << __func__ << " accept existing " << existing
                           << ".gseq " << existing->peer_global_seq << " > "
                           << connect.global_seq << ", RETRY_GLOBAL" << dendl;
      reply.global_seq = existing->peer_global_seq;  // so we can send it below..
      existing->lock.Unlock();
      return _reply_accept(CEPH_MSGR_TAG_RETRY_GLOBAL, connect, reply, authorizer_reply);
    } else {
      ldout(async_msgr->cct, 10) << __func__ << " accept existing " << existing
                           << ".gseq " << existing->peer_global_seq
                           << " <= " << connect.global_seq << ", looks ok" << dendl;
    }

    if (existing->policy.lossy) {
      ldout(async_msgr->cct, 0) << __func__ << " accept replacing existing (lossy) channel (new one lossy="
                          << policy.lossy << ")" << dendl;
      existing->was_session_reset();
      goto replace;
    }

    ldout(async_msgr->cct, 0) << __func__ << " accept connect_seq " << connect.connect_seq
                              << " vs existing csq=" << existing->connect_seq << " existing_state="
                              << get_state_name(existing->state) << dendl;

    if (connect.connect_seq == 0 && existing->connect_seq > 0) {
      ldout(async_msgr->cct,0) << __func__ << " accept peer reset, then tried to connect to us, replacing" << dendl;
      // this is a hard reset from peer
      is_reset_from_peer = true;
      if (policy.resetcheck)
        existing->was_session_reset(); // this resets out_queue, msg_ and connect_seq #'s
      goto replace;
    }

    if (connect.connect_seq < existing->connect_seq) {
      // old attempt, or we sent READY but they didn't get it.
      ldout(async_msgr->cct, 10) << __func__ << " accept existing " << existing << ".cseq "
                           << existing->connect_seq << " > " << connect.connect_seq
                           << ", RETRY_SESSION" << dendl;
      reply.connect_seq = existing->connect_seq + 1;
      existing->lock.Unlock();
      return _reply_accept(CEPH_MSGR_TAG_RETRY_SESSION, connect, reply, authorizer_reply);
    }

    if (connect.connect_seq == existing->connect_seq) {
      // if the existing connection successfully opened, and/or
      // subsequently went to standby, then the peer should bump
      // their connect_seq and retry: this is not a connection race
      // we need to resolve here.
      if (existing->state == STATE_OPEN ||
          existing->state == STATE_STANDBY) {
        ldout(async_msgr->cct, 10) << __func__ << " accept connection race, existing " << existing
                             << ".cseq " << existing->connect_seq << " == "
                             << connect.connect_seq << ", OPEN|STANDBY, RETRY_SESSION" << dendl;
        reply.connect_seq = existing->connect_seq + 1;
        existing->lock.Unlock();
        return _reply_accept(CEPH_MSGR_TAG_RETRY_SESSION, connect, reply, authorizer_reply);
      }

      // connection race?
      if (peer_addr < async_msgr->get_myaddr() || existing->policy.server) {
        // incoming wins
        ldout(async_msgr->cct, 10) << __func__ << " accept connection race, existing " << existing
                             << ".cseq " << existing->connect_seq << " == " << connect.connect_seq
                             << ", or we are server, replacing my attempt" << dendl;
        goto replace;
      } else {
        // our existing outgoing wins
        ldout(async_msgr->cct,10) << __func__ << " accept connection race, existing "
                            << existing << ".cseq " << existing->connect_seq
                            << " == " << connect.connect_seq << ", sending WAIT" << dendl;
        assert(peer_addr > async_msgr->get_myaddr());
        existing->lock.Unlock();
        return _reply_accept(CEPH_MSGR_TAG_WAIT, connect, reply, authorizer_reply);
      }
    }

    assert(connect.connect_seq > existing->connect_seq);
    assert(connect.global_seq >= existing->peer_global_seq);
    if (policy.resetcheck &&   // RESETSESSION only used by servers; peers do not reset each other
        existing->connect_seq == 0) {
      ldout(async_msgr->cct, 0) << __func__ << " accept we reset (peer sent cseq "
                          << connect.connect_seq << ", " << existing << ".cseq = "
                          << existing->connect_seq << "), sending RESETSESSION" << dendl;
      existing->lock.Unlock();
      return _reply_accept(CEPH_MSGR_TAG_RESETSESSION, connect, reply, authorizer_reply);
    }

    // reconnect
    ldout(async_msgr->cct, 10) << __func__ << " accept peer sent cseq " << connect.connect_seq
                         << " > " << existing->connect_seq << dendl;
    goto replace;
  } // existing
  else if (!replacing && connect.connect_seq > 0) {
    // we reset, and they are opening a new session
    ldout(async_msgr->cct, 0) << __func__ << " accept we reset (peer sent cseq "
                        << connect.connect_seq << "), sending RESETSESSION" << dendl;
    return _reply_accept(CEPH_MSGR_TAG_RESETSESSION, connect, reply, authorizer_reply);
  } else {
    // new session
    ldout(async_msgr->cct, 10) << __func__ << " accept new session" << dendl;
    existing = NULL;
    goto open;
  }
  assert(0);

 replace:
  ldout(async_msgr->cct, 10) << __func__ << " accept replacing " << existing << dendl;

  inject_delay();
  if (existing->policy.lossy) {
    // disconnect from the Connection
    existing->center->dispatch_event_external(existing->reset_handler);
    ldout(async_msgr->cct, 1) << __func__ << " replacing on lossy channel, failing existing" << dendl;
    existing->_stop();
  } else {
    assert(can_write == WriteStatus::NOWRITE);
    existing->write_lock.Lock(true);
    // queue a reset on the new connection, which we're dumping for the old
    center->dispatch_event_external(reset_handler);

    // reset the in_seq if this is a hard reset from peer,
    // otherwise we respect our original connection's value
    if (is_reset_from_peer) {
      existing->is_reset_from_peer = true;
    }

    // Now existing connection will be alive and the current connection will
    // exchange socket with existing connection because we want to maintain
    // original "connection_state"
    if (existing->sd >= 0)
      existing->center->delete_file_event(existing->sd, EVENT_READABLE|EVENT_WRITABLE);
    center->delete_file_event(sd, EVENT_READABLE|EVENT_WRITABLE);
    existing->center->create_file_event(sd, EVENT_READABLE, existing->read_handler);

    reply.global_seq = existing->peer_global_seq;

    // Clean up output buffer
    existing->outcoming_bl.clear();
    if (existing->delay_state) {
      existing->delay_state->flush();
      assert(!delay_state);
    }
    existing->requeue_sent();

    swap(existing->sd, sd);
    existing->can_write = WriteStatus::NOWRITE;
    existing->open_write = false;
    existing->replacing = true;
    existing->state_offset = 0;
    existing->state = STATE_ACCEPTING_WAIT_CONNECT_MSG;
    // Discard existing prefetch buffer in `recv_buf`
    existing->recv_start = existing->recv_end = 0;
    // there shouldn't exist any buffer
    assert(recv_start == recv_end);

    existing->write_lock.Unlock();
    if (existing->_reply_accept(CEPH_MSGR_TAG_RETRY_GLOBAL, connect, reply, authorizer_reply) < 0) {
      // handle error
      ldout(async_msgr->cct, 0) << __func__ << " reply fault for existing connection." << dendl;
      existing->fault();
    }

    ldout(async_msgr->cct, 1) << __func__ << " stop myself to swap existing" << dendl;
    _stop();
    existing->lock.Unlock();
    return 0;
  }
  existing->lock.Unlock();

 open:
  connect_seq = connect.connect_seq + 1;
  peer_global_seq = connect.global_seq;
  ldout(async_msgr->cct, 10) << __func__ << " accept success, connect_seq = "
                             << connect_seq << " in_seq=" << in_seq.read() << ", sending READY" << dendl;

  int next_state;

  // if it is a hard reset from peer, we don't need a round-trip to negotiate in/out sequence
  if ((connect.features & CEPH_FEATURE_RECONNECT_SEQ) && !is_reset_from_peer) {
    reply.tag = CEPH_MSGR_TAG_SEQ;
    next_state = STATE_ACCEPTING_WAIT_SEQ;
  } else {
    reply.tag = CEPH_MSGR_TAG_READY;
    next_state = STATE_ACCEPTING_READY;
    discard_requeued_up_to(0);
    is_reset_from_peer = false;
    in_seq.set(0);
  }

  // send READY reply
  reply.features = policy.features_supported;
  reply.global_seq = async_msgr->get_global_seq();
  reply.connect_seq = connect_seq;
  reply.flags = 0;
  reply.authorizer_len = authorizer_reply.length();
  if (policy.lossy)
    reply.flags = reply.flags | CEPH_MSG_CONNECT_LOSSY;

  set_features((uint64_t)reply.features & (uint64_t)connect.features);
  ldout(async_msgr->cct, 10) << __func__ << " accept features " << get_features() << dendl;

  session_security.reset(
      get_auth_session_handler(async_msgr->cct, connect.authorizer_protocol,
                               session_key, get_features()));

  reply_bl.append((char*)&reply, sizeof(reply));

  if (reply.authorizer_len)
    reply_bl.append(authorizer_reply.c_str(), authorizer_reply.length());

  if (reply.tag == CEPH_MSGR_TAG_SEQ) {
    uint64_t s = in_seq.read();
    reply_bl.append((char*)&s, sizeof(s));
  }

  lock.Unlock();
  // Because "replacing" will prevent other connections preempt this addr,
  // it's safe that here we don't acquire Connection's lock
  r = async_msgr->accept_conn(this);

  inject_delay();
  
  lock.Lock();
  replacing = false;
  if (r < 0) {
    ldout(async_msgr->cct, 1) << __func__ << " existing race replacing process for addr=" << peer_addr
                              << " just fail later one(this)" << dendl;
    goto fail_registered;
  }
  if (state != STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH) {
    ldout(async_msgr->cct, 1) << __func__ << " state changed while accept_conn, it must be mark_down" << dendl;
    assert(state == STATE_CLOSED);
    goto fail_registered;
  }

  r = try_send(reply_bl);
  if (r < 0)
    goto fail_registered;

  // notify
  center->dispatch_event_external(EventCallbackRef(new C_deliver_accept(async_msgr, this)));
  async_msgr->ms_deliver_handle_fast_accept(this);
  once_ready = true;

  if (r == 0) {
    state = next_state;
    ldout(async_msgr->cct, 2) << __func__ << " accept write reply msg done" << dendl;
  } else {
    state = STATE_WAIT_SEND;
    state_after_send = next_state;
  }

  return 0;

 fail_registered:
  ldout(async_msgr->cct, 10) << __func__ << " accept fault after register" << dendl;
  inject_delay();

 fail:
  ldout(async_msgr->cct, 10) << __func__ << " failed to accept." << dendl;
  return -1;
}

void AsyncConnection::_connect()
{
  ldout(async_msgr->cct, 10) << __func__ << " csq=" << connect_seq << dendl;

  state = STATE_CONNECTING;
  // rescheduler connection in order to avoid lock dep
  // may called by external thread(send_message)
  center->dispatch_event_external(read_handler);
}

void AsyncConnection::accept(int incoming)
{
  ldout(async_msgr->cct, 10) << __func__ << " sd=" << incoming << dendl;
  assert(sd < 0);

  Mutex::Locker l(lock);
  sd = incoming;
  state = STATE_ACCEPTING;
  center->create_file_event(sd, EVENT_READABLE, read_handler);
  // rescheduler connection in order to avoid lock dep
  center->dispatch_event_external(read_handler);
}

int AsyncConnection::send_message(Message *m)
{
  ldout(async_msgr->cct, 1) << " == tx == " << m << " " << *m << dendl;

  // optimistic think it's ok to encode(actually may broken now)
  if (!m->get_priority())
    m->set_priority(async_msgr->get_default_send_priority());

  m->get_header().src = async_msgr->get_myname();
  m->set_connection(this);

  if (async_msgr->get_myaddr() == get_peer_addr()) { //loopback connection
    ldout(async_msgr->cct, 20) << __func__ << " " << *m << " local" << dendl;
    Mutex::Locker l(write_lock);
    if (can_write != WriteStatus::CLOSED) {
      local_messages.push_back(m);
      center->dispatch_event_external(local_deliver_handler);
    } else {
      ldout(async_msgr->cct, 10) << __func__ << " loopback connection closed."
                                 << " Drop message " << m << dendl;
      m->put();
    }
    return 0;
  }

  // we don't want to consider local message here, it's too lightweight which
  // may disturb users
  logger->inc(l_msgr_send_messages);

  bufferlist bl;
  uint64_t f = get_features();

  // TODO: Currently not all messages supports reencode like MOSDMap, so here
  // only let fast dispatch support messages prepare message
  bool can_fast_prepare = async_msgr->ms_can_fast_dispatch(m);
  if (can_fast_prepare)
    prepare_send_message(f, m, bl);

  Mutex::Locker l(write_lock);
  // "features" changes will change the payload encoding
  if (can_fast_prepare && (can_write == WriteStatus::NOWRITE || get_features() != f)) {
    // ensure the correctness of message encoding
    bl.clear();
    m->get_payload().clear();
    ldout(async_msgr->cct, 5) << __func__ << " clear encoded buffer previous "
                              << f << " != " << get_features() << dendl;
  }
  if (!is_queued() && can_write == WriteStatus::CANWRITE && async_msgr->cct->_conf->ms_async_send_inline) {
    if (!can_fast_prepare)
      prepare_send_message(get_features(), m, bl);
    logger->inc(l_msgr_send_messages_inline);
    if (write_message(m, bl, false) < 0) {
      ldout(async_msgr->cct, 1) << __func__ << " send msg failed" << dendl;
      // we want to handle fault within internal thread
      center->dispatch_event_external(write_handler);
    }
  } else if (can_write == WriteStatus::CLOSED) {
    ldout(async_msgr->cct, 10) << __func__ << " connection closed."
                               << " Drop message " << m << dendl;
    m->put();
  } else {
    out_q[m->get_priority()].emplace_back(std::move(bl), m);
    ldout(async_msgr->cct, 15) << __func__ << " inline write is denied, reschedule m=" << m << dendl;
    center->dispatch_event_external(write_handler);
  }
  return 0;
}

void AsyncConnection::requeue_sent()
{
  assert(write_lock.is_locked());
  if (sent.empty())
    return;

  list<pair<bufferlist, Message*> >& rq = out_q[CEPH_MSG_PRIO_HIGHEST];
  while (!sent.empty()) {
    Message* m = sent.back();
    sent.pop_back();
    ldout(async_msgr->cct, 10) << __func__ << " " << *m << " for resend "
                               << " (" << m->get_seq() << ")" << dendl;
    rq.push_front(make_pair(bufferlist(), m));
  }
}

void AsyncConnection::discard_requeued_up_to(uint64_t seq)
{
  ldout(async_msgr->cct, 10) << __func__ << " " << seq << dendl;
  Mutex::Locker l(write_lock);
  if (out_q.count(CEPH_MSG_PRIO_HIGHEST) == 0)
    return;
  list<pair<bufferlist, Message*> >& rq = out_q[CEPH_MSG_PRIO_HIGHEST];
  while (!rq.empty()) {
    pair<bufferlist, Message*> p = rq.front();
    if (p.second->get_seq() == 0 || p.second->get_seq() > seq)
      break;
    ldout(async_msgr->cct, 10) << __func__ << " " << *(p.second) << " for resend seq " << p.second->get_seq()
                         << " <= " << seq << ", discarding" << dendl;
    p.second->put();
    rq.pop_front();
  }
  if (rq.empty())
    out_q.erase(CEPH_MSG_PRIO_HIGHEST);
}

/*
 * Tears down the AsyncConnection's message queues, and removes them from the DispatchQueue
 * Must hold pipe_lock prior to calling.
 */
void AsyncConnection::discard_out_queue()
{
  ldout(async_msgr->cct, 10) << __func__ << " started" << dendl;
  assert(write_lock.is_locked());

  for (list<Message*>::iterator p = sent.begin(); p != sent.end(); ++p) {
    ldout(async_msgr->cct, 20) << __func__ << " discard " << *p << dendl;
    (*p)->put();
  }
  sent.clear();
  for (map<int, list<pair<bufferlist, Message*> > >::iterator p = out_q.begin(); p != out_q.end(); ++p)
    for (list<pair<bufferlist, Message*> >::iterator r = p->second.begin(); r != p->second.end(); ++r) {
      ldout(async_msgr->cct, 20) << __func__ << " discard " << r->second << dendl;
      r->second->put();
    }
  out_q.clear();
  outcoming_bl.clear();
}

int AsyncConnection::randomize_out_seq()
{
  if (get_features() & CEPH_FEATURE_MSG_AUTH) {
    // Set out_seq to a random value, so CRC won't be predictable.   Don't bother checking seq_error
    // here.  We'll check it on the call.  PLR
    uint64_t rand_seq;
    int seq_error = get_random_bytes((char *)&rand_seq, sizeof(rand_seq));
    rand_seq &= SEQ_MASK;
    lsubdout(async_msgr->cct, ms, 10) << __func__ << " randomize_out_seq " << rand_seq << dendl;
    out_seq.set(rand_seq);
    return seq_error;
  } else {
    // previously, seq #'s always started at 0.
    out_seq.set(0);
    return 0;
  }
}

void AsyncConnection::fault()
{
  if (state == STATE_CLOSED) {
    ldout(async_msgr->cct, 10) << __func__ << " connection is already closed" << dendl;
    return ;
  }

  if (policy.lossy && !(state >= STATE_CONNECTING && state < STATE_CONNECTING_READY)) {
    ldout(async_msgr->cct, 1) << __func__ << " on lossy channel, failing" << dendl;
    center->dispatch_event_external(reset_handler);
    _stop();
    return ;
  }

  write_lock.Lock();
  if (sd >= 0) {
    shutdown_socket();
    center->delete_file_event(sd, EVENT_READABLE|EVENT_WRITABLE);
    ::close(sd);
    sd = -1;
  }
  can_write = WriteStatus::NOWRITE;
  open_write = false;

  // queue delayed items immediately
  if (delay_state)
    delay_state->flush();
  // requeue sent items
  requeue_sent();
  recv_start = recv_end = 0;
  state_offset = 0;
  replacing = false;
  is_reset_from_peer = false;
  outcoming_bl.clear();
  if (!once_ready && !is_queued() &&
      state >=STATE_ACCEPTING && state <= STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH) {
    ldout(async_msgr->cct, 0) << __func__ << " with nothing to send and in the half "
                              << " accept state just closed" << dendl;
    center->dispatch_event_external(reset_handler);

    write_lock.Unlock();
    _stop();
    return ;
  }
  if (policy.standby && !is_queued()) {
    ldout(async_msgr->cct,0) << __func__ << " with nothing to send, going to standby" << dendl;
    state = STATE_STANDBY;
    write_lock.Unlock();
    return;
  }

  write_lock.Unlock();
  if (!(state >= STATE_CONNECTING && state < STATE_CONNECTING_READY)) {
    // policy maybe empty when state is in accept
    if (policy.server) {
      ldout(async_msgr->cct, 0) << __func__ << " server, going to standby" << dendl;
      state = STATE_STANDBY;
    } else {
      ldout(async_msgr->cct, 0) << __func__ << " initiating reconnect" << dendl;
      connect_seq++;
      state = STATE_CONNECTING;
    }
    backoff = utime_t();
  } else {
    if (backoff == utime_t()) {
      backoff.set_from_double(async_msgr->cct->_conf->ms_initial_backoff);
    } else {
      backoff += backoff;
      if (backoff > async_msgr->cct->_conf->ms_max_backoff)
        backoff.set_from_double(async_msgr->cct->_conf->ms_max_backoff);
    }

    state = STATE_CONNECTING;
    ldout(async_msgr->cct, 10) << __func__ << " waiting " << backoff << dendl;
  }

  // woke up again;
  register_time_events.insert(center->create_time_event(
          backoff.to_nsec()/1000, wakeup_handler));
}

void AsyncConnection::was_session_reset()
{
  ldout(async_msgr->cct,10) << __func__ << " started" << dendl;
  assert(lock.is_locked());
  Mutex::Locker l(write_lock);
  if (delay_state)
    delay_state->discard();
  discard_out_queue();

  center->dispatch_event_external(remote_reset_handler);

  if (randomize_out_seq()) {
    ldout(async_msgr->cct, 15) << __func__ << " could not get random bytes to set seq number for session reset; set seq number to " << out_seq.read() << dendl;
  }

  in_seq.set(0);
  connect_seq = 0;
  // it's safe to directly set 0, double locked
  ack_left.set(0);
  once_ready = false;
  can_write = WriteStatus::NOWRITE;
}

void AsyncConnection::_stop()
{
  assert(lock.is_locked());
  if (state == STATE_CLOSED)
    return ;

  if (delay_state)
    delay_state->flush();

  ldout(async_msgr->cct, 1) << __func__ << dendl;
  Mutex::Locker l(write_lock);
  if (sd >= 0)
    center->delete_file_event(sd, EVENT_READABLE|EVENT_WRITABLE);

  discard_out_queue();
  async_msgr->unregister_conn(this);

  state = STATE_CLOSED;
  open_write = false;
  can_write = WriteStatus::CLOSED;
  state_offset = 0;
  if (sd >= 0) {
    shutdown_socket();
    ::close(sd);
  }
  sd = -1;
  for (set<uint64_t>::iterator it = register_time_events.begin();
       it != register_time_events.end(); ++it)
    center->delete_time_event(*it);
  // Make sure in-queue events will been processed
  center->dispatch_event_external(EventCallbackRef(new C_clean_handler(this)));

}

void AsyncConnection::prepare_send_message(uint64_t features, Message *m, bufferlist &bl)
{
  ldout(async_msgr->cct, 20) << __func__ << " m" << " " << *m << dendl;

  // associate message with Connection (for benefit of encode_payload)
  if (m->empty_payload())
    ldout(async_msgr->cct, 20) << __func__ << " encoding features "
                               << features << " " << m << " " << *m << dendl;
  else
    ldout(async_msgr->cct, 20) << __func__ << " half-reencoding features "
                               << features << " " << m << " " << *m << dendl;

  // encode and copy out of *m
  m->encode(features, msgr->crcflags);

  bl.append(m->get_payload());
  bl.append(m->get_middle());
  bl.append(m->get_data());
}

ssize_t AsyncConnection::write_message(Message *m, bufferlist& bl, bool more)
{
  assert(can_write == WriteStatus::CANWRITE);
  m->set_seq(out_seq.inc());

  if (!policy.lossy) {
    // put on sent list
    sent.push_back(m);
    m->get();
  }

  if (msgr->crcflags & MSG_CRC_HEADER)
    m->calc_header_crc();

  ceph_msg_header& header = m->get_header();
  ceph_msg_footer& footer = m->get_footer();

  // TODO: let sign_message could be reentry?
  // Now that we have all the crcs calculated, handle the
  // digital signature for the message, if the AsyncConnection has session
  // security set up.  Some session security options do not
  // actually calculate and check the signature, but they should
  // handle the calls to sign_message and check_signature.  PLR
  if (session_security.get() == NULL) {
    ldout(async_msgr->cct, 20) << __func__ << " no session security" << dendl;
  } else {
    if (session_security->sign_message(m)) {
      ldout(async_msgr->cct, 20) << __func__ << " failed to sign m="
                                 << m << "): sig = " << footer.sig << dendl;
    } else {
      ldout(async_msgr->cct, 20) << __func__ << " signed m=" << m
                                 << "): sig = " << footer.sig << dendl;
    }
  }
  
  unsigned original_bl_len = outcoming_bl.length();

  outcoming_bl.append(CEPH_MSGR_TAG_MSG);

  if (has_feature(CEPH_FEATURE_NOSRCADDR)) {
    outcoming_bl.append((char*)&header, sizeof(header));
  } else {
    ceph_msg_header_old oldheader;
    memcpy(&oldheader, &header, sizeof(header));
    oldheader.src.name = header.src;
    oldheader.src.addr = get_peer_addr();
    oldheader.orig_src = oldheader.src;
    oldheader.reserved = header.reserved;
    oldheader.crc = ceph_crc32c(0, (unsigned char*)&oldheader,
                                sizeof(oldheader) - sizeof(oldheader.crc));
    outcoming_bl.append((char*)&oldheader, sizeof(oldheader));
  }

  ldout(async_msgr->cct, 20) << __func__ << " sending message type=" << header.type
                             << " src " << entity_name_t(header.src)
                             << " front=" << header.front_len
                             << " data=" << header.data_len
                             << " off " << header.data_off << dendl;

  if ((bl.length() <= ASYNC_COALESCE_THRESHOLD) && (bl.buffers().size() > 1)) {
    std::list<buffer::ptr>::const_iterator pb;
    for (pb = bl.buffers().begin(); pb != bl.buffers().end(); ++pb) {
      outcoming_bl.append((char*)pb->c_str(), pb->length());
    }
  } else {
    outcoming_bl.claim_append(bl);  
  }

  // send footer; if receiver doesn't support signatures, use the old footer format
  ceph_msg_footer_old old_footer;
  if (has_feature(CEPH_FEATURE_MSG_AUTH)) {
    outcoming_bl.append((char*)&footer, sizeof(footer));
  } else {
    if (msgr->crcflags & MSG_CRC_HEADER) {
      old_footer.front_crc = footer.front_crc;
      old_footer.middle_crc = footer.middle_crc;
      old_footer.data_crc = footer.data_crc;
    } else {
       old_footer.front_crc = old_footer.middle_crc = 0;
    }
    old_footer.data_crc = msgr->crcflags & MSG_CRC_DATA ? footer.data_crc : 0;
    old_footer.flags = footer.flags;
    outcoming_bl.append((char*)&old_footer, sizeof(old_footer));
  }

  logger->inc(l_msgr_send_bytes, outcoming_bl.length() - original_bl_len);
  ldout(async_msgr->cct, 20) << __func__ << " sending " << m->get_seq()
                             << " " << m << dendl;
  ssize_t rc = _try_send(more);
  if (rc < 0) {
    ldout(async_msgr->cct, 1) << __func__ << " error sending " << m << ", "
                              << cpp_strerror(errno) << dendl;
  } else if (rc == 0) {
    ldout(async_msgr->cct, 10) << __func__ << " sending " << m << " done." << dendl;
  } else {
    ldout(async_msgr->cct, 10) << __func__ << " sending " << m << " continuely." << dendl;
  }
  m->put();

  return rc;
}

void AsyncConnection::handle_ack(uint64_t seq)
{
  ldout(async_msgr->cct, 15) << __func__ << " got ack seq " << seq << dendl;
  // trim sent list
  Mutex::Locker l(write_lock);
  while (!sent.empty() && sent.front()->get_seq() <= seq) {
    Message* m = sent.front();
    sent.pop_front();
    ldout(async_msgr->cct, 10) << __func__ << " got ack seq "
                               << seq << " >= " << m->get_seq() << " on "
                               << m << " " << *m << dendl;
    m->put();
  }
}

void AsyncConnection::DelayedDelivery::do_request(int id)
{
  Message *m = nullptr;
  {
    Mutex::Locker l(delay_lock);
    register_time_events.erase(id);
    if (delay_queue.empty())
      return ;
    utime_t release = delay_queue.front().first;
    m = delay_queue.front().second;
    string delay_msg_type = msgr->cct->_conf->ms_inject_delay_msg_type;
    utime_t now = ceph_clock_now(msgr->cct);
    if ((release > now &&
        (delay_msg_type.empty() || m->get_type_name() == delay_msg_type))) {
      utime_t t = release - now;
      t.sleep();
    }
    delay_queue.pop_front();
  }
  if (msgr->ms_can_fast_dispatch(m)) {
    msgr->ms_fast_dispatch(m);
  } else {
    msgr->ms_deliver_dispatch(m);
  }
}

class C_flush_messages : public EventCallback {
  std::deque<std::pair<utime_t, Message*> > delay_queue;
  AsyncMessenger *msgr;
 public:
  C_flush_messages(std::deque<std::pair<utime_t, Message*> > &&q, AsyncMessenger *m): delay_queue(std::move(q)), msgr(m) {}
  void do_request(int id) {
    while (!delay_queue.empty()) {
      Message *m = delay_queue.front().second;
      if (msgr->ms_can_fast_dispatch(m)) {
        msgr->ms_fast_dispatch(m);
      } else {
        msgr->ms_deliver_dispatch(m);
      }
      delay_queue.pop_front();
    }
    delete this;
  }
};

void AsyncConnection::DelayedDelivery::flush() {
  Mutex::Locker l(delay_lock);
  center->dispatch_event_external(new C_flush_messages(std::move(delay_queue), msgr));
  for (auto i : register_time_events)
    center->delete_time_event(i);
  register_time_events.clear();
}

void AsyncConnection::send_keepalive()
{
  ldout(async_msgr->cct, 10) << __func__ << " started." << dendl;
  Mutex::Locker l(write_lock);
  if (can_write != WriteStatus::CLOSED) {
    keepalive = true;
    center->dispatch_event_external(write_handler);
  }
}

void AsyncConnection::mark_down()
{
  ldout(async_msgr->cct, 1) << __func__ << " started." << dendl;
  Mutex::Locker l(lock);
  _stop();
}

void AsyncConnection::_send_keepalive_or_ack(bool ack, utime_t *tp)
{
  assert(write_lock.is_locked());

  if (ack) {
    assert(tp);
    struct ceph_timespec ts;
    tp->encode_timeval(&ts);
    outcoming_bl.append(CEPH_MSGR_TAG_KEEPALIVE2_ACK);
    outcoming_bl.append((char*)&ts, sizeof(ts));
  } else if (has_feature(CEPH_FEATURE_MSGR_KEEPALIVE2)) {
    struct ceph_timespec ts;
    utime_t t = ceph_clock_now(async_msgr->cct);
    t.encode_timeval(&ts);
    outcoming_bl.append(CEPH_MSGR_TAG_KEEPALIVE2);
    outcoming_bl.append((char*)&ts, sizeof(ts));
  } else {
    outcoming_bl.append(CEPH_MSGR_TAG_KEEPALIVE);
  }

  ldout(async_msgr->cct, 10) << __func__ << " try send keepalive or ack" << dendl;
}

void AsyncConnection::handle_write()
{
  ldout(async_msgr->cct, 10) << __func__ << " started." << dendl;
  ssize_t r = 0;

  write_lock.Lock();
  if (can_write == WriteStatus::CANWRITE) {
    if (keepalive) {
      _send_keepalive_or_ack();
      keepalive = false;
    }

    while (1) {
      bufferlist data;
      Message *m = _get_next_outgoing(&data);
      if (!m)
        break;

      // send_message or requeue messages may not encode message
      if (!data.length())
        prepare_send_message(get_features(), m, data);

      r = write_message(m, data, _has_next_outgoing());
      if (r < 0) {
        ldout(async_msgr->cct, 1) << __func__ << " send msg failed" << dendl;
        write_lock.Unlock();
        goto fail;
      } else if (r > 0) {
        break;
      }
    }

    uint64_t left = ack_left.read();
    if (left) {
      ceph_le64 s;
      s = in_seq.read();
      outcoming_bl.append(CEPH_MSGR_TAG_ACK);
      outcoming_bl.append((char*)&s, sizeof(s));
      ldout(async_msgr->cct, 10) << __func__ << " try send msg ack, acked " << left << " messages" << dendl;
      ack_left.sub(left);
      left = ack_left.read();
      r = _try_send(left);
    } else if (is_queued()) {
      r = _try_send();
    }

    write_lock.Unlock();
    if (r < 0) {
      ldout(async_msgr->cct, 1) << __func__ << " send msg failed" << dendl;
      goto fail;
    }
  } else {
    write_lock.Unlock();
    lock.Lock();
    write_lock.Lock();
    if (state == STATE_STANDBY && !policy.server && is_queued()) {
      ldout(async_msgr->cct, 10) << __func__ << " policy.server is false" << dendl;
      _connect();
    } else if (sd >= 0 && state != STATE_CONNECTING && state != STATE_CONNECTING_RE && state != STATE_CLOSED) {
      r = _try_send();
      if (r < 0) {
        ldout(async_msgr->cct, 1) << __func__ << " send outcoming bl failed" << dendl;
        write_lock.Unlock();
        fault();
        lock.Unlock();
        return ;
      }
    }
    write_lock.Unlock();
    lock.Unlock();
  }

  return ;

 fail:
  lock.Lock();
  fault();
  lock.Unlock();
}

void AsyncConnection::wakeup_from(uint64_t id)
{
  lock.Lock();
  register_time_events.erase(id);
  lock.Unlock();
  process();
}

void AsyncConnection::local_deliver()
{
  ldout(async_msgr->cct, 10) << __func__ << dendl;
  Mutex::Locker l(write_lock);
  while (!local_messages.empty()) {
    Message *m = local_messages.front();
    local_messages.pop_front();
    m->set_connection(this);
    m->set_recv_stamp(ceph_clock_now(async_msgr->cct));
    ldout(async_msgr->cct, 10) << __func__ << " " << *m << " local deliver " << dendl;
    async_msgr->ms_fast_preprocess(m);
    write_lock.Unlock();
    if (async_msgr->ms_can_fast_dispatch(m)) {
      async_msgr->ms_fast_dispatch(m);
    } else {
      msgr->ms_deliver_dispatch(m);
    }
    write_lock.Lock();
  }
}
