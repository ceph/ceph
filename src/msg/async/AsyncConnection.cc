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
#include <urcu.h>
#include <unistd.h>

#include "include/Context.h"
#include "common/errno.h"
#include "common/RCU.h"
#include "AsyncMessenger.h"
#include "AsyncConnection.h"

// Constant to limit starting sequence number to 2^31.  Nothing special about it, just a big number.  PLR
#define SEQ_MASK  0x7fffffff 

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _conn_prefix(_dout)
ostream& AsyncConnection::_conn_prefix(std::ostream *_dout) {
  return *_dout << "-- " << async_msgr->get_myinst().addr << " >> " << peer_addr << " conn(" << this
                << " :" << port
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

class C_clean_handler : public EventCallback {
  AsyncConnectionRef conn;
 public:
  explicit C_clean_handler(AsyncConnectionRef c): conn(c) {}
  void do_request(int id) {
    conn->cleanup();
    delete this;
  }
};

class C_tick_wakeup : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_tick_wakeup(AsyncConnectionRef c): conn(c) {}
  void do_request(int fd_or_id) {
    conn->tick(fd_or_id);
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

AsyncConnection::AsyncConnection(CephContext *cct, AsyncMessenger *m, DispatchQueue *q,
                                 Worker *w)
  : Connection(cct, m), delay_state(NULL), async_msgr(m), conn_id(q->get_id()),
    logger(w->get_perf_counter()), global_seq(0), connect_seq(0), peer_global_seq(0),
    out_seq(0), ack_left(0), in_seq(0), state(STATE_NONE), state_after_send(STATE_NONE), port(-1),
    dispatch_queue(q), can_write(WriteStatus::NOWRITE),
    open_write(false), keepalive(false), recv_buf(NULL),
    recv_max_prefetch(MAX(msgr->cct->_conf->ms_tcp_prefetch_max_size, TCP_PREFETCH_MIN_SIZE)),
    recv_start(0), recv_end(0),
    last_active(ceph::coarse_mono_clock::now()),
    inactive_timeout_us(cct->_conf->ms_tcp_read_timeout*1000*1000),
    got_bad_auth(false), authorizer(NULL), replacing(false),
    is_reset_from_peer(false), once_ready(false), state_buffer(NULL), state_offset(0),
    worker(w), center(&w->center)
{
  read_handler = new C_handle_read(this);
  write_handler = new C_handle_write(this);
  wakeup_handler = new C_time_wakeup(this);
  tick_handler = new C_tick_wakeup(this);
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
    delay_state = new DelayedDelivery(async_msgr, center, dispatch_queue, conn_id);
  }
}

/* return -1 means `fd` occurs error or closed, it should be closed
 * return 0 means EAGAIN or EINTR */
ssize_t AsyncConnection::read_bulk(char *buf, unsigned len)
{
  ssize_t nread;
 again:
  nread = cs.read(buf, len);
  if (nread < 0) {
    if (nread == -EAGAIN) {
      nread = 0;
    } else if (nread == -EINTR) {
      goto again;
    } else {
      ldout(async_msgr->cct, 1) << __func__ << " reading from fd=" << cs.fd()
                          << " : "<< strerror(nread) << dendl;
      return -1;
    }
  } else if (nread == 0) {
    ldout(async_msgr->cct, 1) << __func__ << " peer close file descriptor "
                              << cs.fd() << dendl;
    return -1;
  }
  return nread;
}

// return the remaining bytes, it may larger than the length of ptr
// else return < 0 means error
ssize_t AsyncConnection::_try_send(bool more)
{
  if (async_msgr->cct->_conf->ms_inject_socket_failures && cs) {
    if (rand() % async_msgr->cct->_conf->ms_inject_socket_failures == 0) {
      ldout(async_msgr->cct, 0) << __func__ << " injecting socket failure" << dendl;
      cs.shutdown();
    }
  }

  ssize_t r = cs.send(outcoming_bl, more);
  if (r < 0) {
    ldout(async_msgr->cct, 1) << __func__ << " send error: " << cpp_strerror(r) << dendl;
    return r;
  }

  ldout(async_msgr->cct, 10) << __func__ << " sent bytes " << r
                             << " remaining bytes " << outcoming_bl.length() << dendl;

  if (!open_write && is_queued()) {
    if (center->in_thread()) {
      center->create_file_event(cs.fd(), EVENT_WRITABLE, write_handler);
      open_write = true;
    } else {
      center->dispatch_event_external(write_handler);
    }
  }

  if (open_write && !is_queued()) {
    if (center->in_thread()) {
      center->delete_file_event(cs.fd(), EVENT_WRITABLE);
      open_write = false;
    } else {
      center->dispatch_event_external(write_handler);
    }
    if (state_after_send != STATE_NONE)
      center->dispatch_event_external(read_handler);
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

  if (async_msgr->cct->_conf->ms_inject_socket_failures && cs) {
    if (rand() % async_msgr->cct->_conf->ms_inject_socket_failures == 0) {
      ldout(async_msgr->cct, 0) << __func__ << " injecting socket failure" << dendl;
      cs.shutdown();
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
      r = read_bulk(p+state_offset, left);
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
      r = read_bulk(recv_buf+recv_end, recv_max_prefetch);
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
  bool need_dispatch_writer = false;
  std::lock_guard<std::mutex> l(lock);
  last_active = ceph::coarse_mono_clock::now();

  RCU<>::RCU_quiescent();
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
          write_lock.lock();
          _append_keepalive_or_ack(true, &kp_t);
	  write_lock.unlock();
          ldout(async_msgr->cct, 20) << __func__ << " got KEEPALIVE2 " << kp_t << dendl;
	  set_last_keepalive(ceph_clock_now(NULL));
          need_dispatch_writer = true;
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
          cur_msg_size = current_header.front_len + current_header.middle_len + current_header.data_len;
          if (cur_msg_size) {
            if (policy.throttler_bytes) {
              ldout(async_msgr->cct, 10) << __func__ << " wants " << cur_msg_size << " bytes from policy throttler "
                                         << policy.throttler_bytes->get_current() << "/"
                                         << policy.throttler_bytes->get_max() << dendl;
              if (!policy.throttler_bytes->get_or_fail(cur_msg_size)) {
                ldout(async_msgr->cct, 10) << __func__ << " wants " << cur_msg_size << " bytes from policy throttler "
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

          state = STATE_OPEN_MESSAGE_THROTTLE_DISPATCH_QUEUE;
          break;
        }

      case STATE_OPEN_MESSAGE_THROTTLE_DISPATCH_QUEUE:
        {
          if (cur_msg_size) {
            if (!dispatch_queue->dispatch_throttler.get_or_fail(cur_msg_size)) {
              ldout(async_msgr->cct, 10) << __func__ << " wants " << cur_msg_size << " bytes from dispatch throttle "
                                         << dispatch_queue->dispatch_throttler.get_current() << "/"
                                         << dispatch_queue->dispatch_throttler.get_max() << " failed, just wait." << dendl;
              // following thread pool deal with th full message queue isn't a
              // short time, so we can wait a ms.
              if (register_time_events.empty())
                register_time_events.insert(center->create_time_event(1000, wakeup_handler));
              break;
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
          message->set_dispatch_throttle_size(cur_msg_size);

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
	  ldout(async_msgr->cct, 5) << " rx " << message->get_source() << " seq "
                                    << message->get_seq() << " " << message
				    << " " << *message << dendl;

          ack_left.inc();
          need_dispatch_writer = true;
          state = STATE_OPEN;

          logger->inc(l_msgr_recv_messages);
          logger->inc(l_msgr_recv_bytes, cur_msg_size + sizeof(ceph_msg_header) + sizeof(ceph_msg_footer));

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
            lock.unlock();
            dispatch_queue->fast_dispatch(message);
            lock.lock();
          } else {
            dispatch_queue->enqueue(message, message->get_priority(), conn_id);
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

      case STATE_NONE:
        {
          ldout(async_msgr->cct, 20) << __func__ << " enter none state" << dendl;
          break;
        }

      case STATE_CLOSED:
        {
          ldout(async_msgr->cct, 20) << __func__ << " socket closed" << dendl;
          break;
        }

      case STATE_WAIT:
        {
          ldout(async_msgr->cct, 1) << __func__ << " enter wait state, failing" << dendl;
          goto fail;
        }

      default:
        {
          if (_process_connection() < 0)
            goto fail;
          break;
        }
    }
  } while (prev_state != state);

  if (need_dispatch_writer && is_connected())
    center->dispatch_event_external(write_handler);
  return;

 fail:
  fault();
}

ssize_t AsyncConnection::_process_connection()
{
  ssize_t r = 0;

  switch(state) {
    case STATE_WAIT_SEND:
      {
        std::lock_guard<std::mutex> l(write_lock);
        if (!outcoming_bl.length()) {
          assert(state_after_send);
          state = state_after_send;
          state_after_send = STATE_NONE;
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
        authorizer_buf.clear();
        memset(&connect_msg, 0, sizeof(connect_msg));
        memset(&connect_reply, 0, sizeof(connect_reply));

        global_seq = async_msgr->get_global_seq();
        // close old socket.  this is safe because we stopped the reader thread above.
        if (cs) {
          center->delete_file_event(cs.fd(), EVENT_READABLE|EVENT_WRITABLE);
          cs.close();
        }

        SocketOptions opts;
        opts.priority = async_msgr->get_socket_priority();
        r = worker->connect(get_peer_addr(), opts, &cs);
        if (r < 0)
          goto fail;

        center->create_file_event(cs.fd(), EVENT_READABLE, read_handler);
        state = STATE_CONNECTING_RE;
        break;
      }

    case STATE_CONNECTING_RE:
      {
        r = cs.is_connected();
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " reconnect failed " << dendl;
          if (r == -ECONNREFUSED) {
            ldout(async_msgr->cct, 2) << __func__ << " connection refused!" << dendl;
            dispatch_queue->queue_refused(this);
          }
          goto fail;
        } else if (r == 0) {
          ldout(async_msgr->cct, 10) << __func__ << " nonblock connect inprogress" << dendl;
          if (async_msgr->get_stack()->nonblock_connect_need_writable_event())
            center->create_file_event(cs.fd(), EVENT_WRITABLE, read_handler);
          break;
        }

        center->delete_file_event(cs.fd(), EVENT_WRITABLE);
        ldout(async_msgr->cct, 10) << __func__ << " connect successfully, ready to send banner" << dendl;

        bufferlist bl;
        bl.append(CEPH_BANNER, strlen(CEPH_BANNER));
        r = try_send(bl);
        if (r == 0) {
          state = STATE_CONNECTING_WAIT_BANNER_AND_IDENTIFY;
          ldout(async_msgr->cct, 10) << __func__ << " connect write banner done: "
                                     << get_peer_addr() << dendl;
        } else if (r > 0) {
          state = STATE_WAIT_SEND;
          state_after_send = STATE_CONNECTING_WAIT_BANNER_AND_IDENTIFY;
          ldout(async_msgr->cct, 10) << __func__ << " connect wait for write banner: "
                               << get_peer_addr() << dendl;
        } else {
          goto fail;
        }

        break;
      }

    case STATE_CONNECTING_WAIT_BANNER_AND_IDENTIFY:
      {
        entity_addr_t paddr, peer_addr_for_me;
        bufferlist myaddrbl;
        unsigned banner_len = strlen(CEPH_BANNER);
        unsigned need_len = banner_len + sizeof(ceph_entity_addr)*2;
        r = read_until(need_len, state_buffer);
        if (r < 0) {
          ldout(async_msgr->cct, 1) << __func__ << " read banner and identify addresses failed" << dendl;
          goto fail;
        } else if (r > 0) {
          break;
        }

        if (memcmp(state_buffer, CEPH_BANNER, banner_len)) {
          ldout(async_msgr->cct, 0) << __func__ << " connect protocol error (bad banner) on peer "
                                    << get_peer_addr() << dendl;
          goto fail;
        }

        bufferlist bl;
        bl.append(state_buffer+banner_len, sizeof(ceph_entity_addr)*2);
        bufferlist::iterator p = bl.begin();
        try {
          ::decode(paddr, p);
          ::decode(peer_addr_for_me, p);
        } catch (const buffer::error& e) {
          lderr(async_msgr->cct) << __func__ <<  " decode peer addr failed " << dendl;
          goto fail;
        }
        ldout(async_msgr->cct, 20) << __func__ <<  " connect read peer addr "
                             << paddr << " on socket " << cs.fd() << dendl;
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
        lock.unlock();
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

        lock.lock();
        if (state != STATE_CONNECTING_WAIT_BANNER_AND_IDENTIFY) {
          ldout(async_msgr->cct, 1) << __func__ << " state changed while learned_addr, mark_down or "
                                    << " replacing must be happened just now" << dendl;
          return 0;
        }

        ::encode(async_msgr->get_myaddr(), myaddrbl, 0); // legacy
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
              << cpp_strerror(r) << dendl;
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
              << cpp_strerror(r) << dendl;
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

        if (delay_state)
          assert(delay_state->ready());
        dispatch_queue->queue_connect(this);
        async_msgr->ms_deliver_handle_fast_connect(this);

        // make sure no pending tick timer
        if (last_tick_id)
          center->delete_time_event(last_tick_id);
        last_tick_id = center->create_time_event(inactive_timeout_us, tick_handler);

        // message may in queue between last _try_send and connection ready
        // write event may already notify and we need to force scheduler again
        write_lock.lock();
        can_write = WriteStatus::CANWRITE;
        if (is_queued())
          center->dispatch_event_external(write_handler);
        write_lock.unlock();
        maybe_start_delay_thread();
        break;
      }

    case STATE_ACCEPTING:
      {
        bufferlist bl;
        center->create_file_event(cs.fd(), EVENT_READABLE, read_handler);

        bl.append(CEPH_BANNER, strlen(CEPH_BANNER));

        ::encode(async_msgr->get_myaddr(), bl, 0); // legacy
        port = async_msgr->get_myaddr().get_port();
        ::encode(socket_addr, bl, 0); // legacy
        ldout(async_msgr->cct, 1) << __func__ << " sd=" << cs.fd() << " " << socket_addr << dendl;

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

        r = read_until(strlen(CEPH_BANNER) + sizeof(ceph_entity_addr), state_buffer);
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

        addr_bl.append(state_buffer+strlen(CEPH_BANNER), sizeof(ceph_entity_addr));
        {
          bufferlist::iterator ti = addr_bl.begin();
          ::decode(peer_addr, ti);
        }

        ldout(async_msgr->cct, 10) << __func__ << " accept peer addr is " << peer_addr << dendl;
        if (peer_addr.is_blank_ip()) {
          // peer apparently doesn't know what ip they have; figure it out for them.
          int port = peer_addr.get_port();
          peer_addr.u = socket_addr.u;
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
        bufferlist authorizer_reply;

        if (connect_msg.authorizer_len) {
          if (!authorizer_buf.length())
            authorizer_buf.push_back(buffer::create(connect_msg.authorizer_len));

          r = read_until(connect_msg.authorizer_len, authorizer_buf.c_str());
          if (r < 0) {
            ldout(async_msgr->cct, 1) << __func__ << " read connect authorizer failed" << dendl;
            goto fail;
          } else if (r > 0) {
            break;
          }
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

        r = handle_connect_msg(connect_msg, authorizer_buf, authorizer_reply);
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

        if (delay_state)
          assert(delay_state->ready());
        // make sure no pending tick timer
        if (last_tick_id)
          center->delete_time_event(last_tick_id);
        last_tick_id = center->create_time_event(inactive_timeout_us, tick_handler);

        write_lock.lock();
        can_write = WriteStatus::CANWRITE;
        if (is_queued())
          center->dispatch_event_external(write_handler);
        write_lock.unlock();
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
    ldout(async_msgr->cct, 5) << __func__ << " connect got RETRY_GLOBAL "
                              << reply.global_seq << " chose new "
                              << global_seq << dendl;
    state = STATE_CONNECTING_SEND_CONNECT_MSG;
  }
  if (reply.tag == CEPH_MSGR_TAG_RETRY_SESSION) {
    assert(reply.connect_seq > connect_seq);
    ldout(async_msgr->cct, 5) << __func__ << " connect got RETRY_SESSION "
                              << connect_seq << " -> "
                              << reply.connect_seq << dendl;
    connect_seq = reply.connect_seq;
    state = STATE_CONNECTING_SEND_CONNECT_MSG;
  }
  if (reply.tag == CEPH_MSGR_TAG_WAIT) {
    ldout(async_msgr->cct, 1) << __func__ << " connect got WAIT (connection race)" << dendl;
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

  lock.unlock();

  bool authorizer_valid;
  if (!async_msgr->verify_authorizer(this, peer_type, connect.authorizer_protocol, authorizer_bl,
                               authorizer_reply, authorizer_valid, session_key) || !authorizer_valid) {
    lock.lock();
    ldout(async_msgr->cct,0) << __func__ << ": got bad authorizer" << dendl;
    session_security.reset();
    return _reply_accept(CEPH_MSGR_TAG_BADAUTHORIZER, connect, reply, authorizer_reply);
  }

  // We've verified the authorizer for this AsyncConnection, so set up the session security structure.  PLR
  ldout(async_msgr->cct, 10) << __func__ << " accept setting up session_security." << dendl;

  // existing?
  AsyncConnectionRef existing = async_msgr->lookup_conn(peer_addr);

  inject_delay();

  lock.lock();
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
    existing->lock.lock();  // skip lockdep check (we are locking a second AsyncConnection here)

    if (existing->replacing || existing->state == STATE_CLOSED) {
      ldout(async_msgr->cct, 1) << __func__ << " existing racing replace or mark_down happened while replacing."
                                << " existing_state=" << get_state_name(existing->state) << dendl;
      reply.global_seq = existing->peer_global_seq;
      r = _reply_accept(CEPH_MSGR_TAG_RETRY_GLOBAL, connect, reply, authorizer_reply);
      existing->lock.unlock();
      if (r < 0)
        goto fail;
      return 0;
    }

    if (connect.global_seq < existing->peer_global_seq) {
      ldout(async_msgr->cct, 10) << __func__ << " accept existing " << existing
                           << ".gseq " << existing->peer_global_seq << " > "
                           << connect.global_seq << ", RETRY_GLOBAL" << dendl;
      reply.global_seq = existing->peer_global_seq;  // so we can send it below..
      existing->lock.unlock();
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
      existing->lock.unlock();
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
        existing->lock.unlock();
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
        existing->lock.unlock();
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
      existing->lock.unlock();
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
    ldout(async_msgr->cct, 1) << __func__ << " replacing on lossy channel, failing existing" << dendl;
    existing->_stop();
    existing->dispatch_queue->queue_reset(existing.get());
  } else {
    assert(can_write == WriteStatus::NOWRITE);
    existing->write_lock.lock();

    // reset the in_seq if this is a hard reset from peer,
    // otherwise we respect our original connection's value
    if (is_reset_from_peer) {
      existing->is_reset_from_peer = true;
    }

    center->delete_file_event(cs.fd(), EVENT_READABLE|EVENT_WRITABLE);

    // Clean up output buffer
    existing->outcoming_bl.clear();
    if (existing->delay_state) {
      existing->delay_state->flush();
      assert(!delay_state);
    }
    existing->requeue_sent();
    existing->reset_recv_state();

    auto temp_cs = std::move(cs);
    EventCenter *new_center = center;
    Worker *new_worker = worker;
    // avoid _stop shutdown replacing socket
    // queue a reset on the new connection, which we're dumping for the old
    _stop();

    dispatch_queue->queue_reset(this);
    ldout(async_msgr->cct, 1) << __func__ << " stop myself to swap existing" << dendl;
    existing->can_write = WriteStatus::REPLACING;
    existing->open_write = false;
    existing->replacing = true;
    existing->state_offset = 0;
    // avoid previous thread modify event
    existing->state = STATE_NONE;
    // Discard existing prefetch buffer in `recv_buf`
    existing->recv_start = existing->recv_end = 0;
    // there shouldn't exist any buffer
    assert(recv_start == recv_end);

    auto deactivate_existing = std::bind(
        [existing, new_worker, new_center, connect, reply, authorizer_reply](ConnectedSocket &cs) mutable {
      // we need to delete time event in original thread
      {
        std::lock_guard<std::mutex> l(existing->lock);
        if (existing->state == STATE_NONE) {
          existing->shutdown_socket();
          existing->cs = std::move(cs);
          existing->worker->references--;
          new_worker->references++;
          existing->logger = new_worker->get_perf_counter();
          existing->worker = new_worker;
          existing->center = new_center;
          if (existing->delay_state)
            existing->delay_state->set_center(new_center);
        } else if (existing->state == STATE_CLOSED) {
          cs.close();
          return ;
        } else {
          assert(0);
        }
      }

      // Before changing existing->center, it may already exists some events in existing->center's queue.
      // Then if we mark down `existing`, it will execute in another thread and clean up connection.
      // Previous event will result in segment fault
      auto transfer_existing = [existing, connect, reply, authorizer_reply]() mutable {
        std::lock_guard<std::mutex> l(existing->lock);
        if (existing->state == STATE_CLOSED)
          return ;
        assert(existing->state == STATE_NONE);
  
        existing->state = STATE_ACCEPTING_WAIT_CONNECT_MSG;
        existing->center->create_file_event(existing->cs.fd(), EVENT_READABLE, existing->read_handler);
        reply.global_seq = existing->peer_global_seq;
        if (existing->_reply_accept(CEPH_MSGR_TAG_RETRY_GLOBAL, connect, reply, authorizer_reply) < 0) {
          // handle error
          existing->fault();
        }
      };
      if (existing->center->in_thread())
        transfer_existing();
      else
        existing->center->submit_to(
            existing->center->get_id(), std::move(transfer_existing), true);
    }, std::move(temp_cs));

    existing->center->submit_to(
        existing->center->get_id(), std::move(deactivate_existing), true);
    existing->write_lock.unlock();
    existing->lock.unlock();
    return 0;
  }
  existing->lock.unlock();

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

  lock.unlock();
  // Because "replacing" will prevent other connections preempt this addr,
  // it's safe that here we don't acquire Connection's lock
  r = async_msgr->accept_conn(this);

  inject_delay();
  
  lock.lock();
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
  dispatch_queue->queue_accept(this);
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

void AsyncConnection::accept(ConnectedSocket socket, entity_addr_t &addr)
{
  ldout(async_msgr->cct, 10) << __func__ << " sd=" << socket.fd() << dendl;
  assert(socket.fd() > 0);

  std::lock_guard<std::mutex> l(lock);
  cs = std::move(socket);
  socket_addr = addr;
  state = STATE_ACCEPTING;
  // rescheduler connection in order to avoid lock dep
  center->dispatch_event_external(read_handler);
}

int AsyncConnection::send_message(Message *m)
{
  lgeneric_subdout(async_msgr->cct, ms,
		   1) << "-- " << async_msgr->get_myaddr() << " --> "
		      << get_peer_addr() << " -- "
		      << *m << " -- " << m << " con "
		      << m->get_connection().get()
		      << dendl;

  // optimistic think it's ok to encode(actually may broken now)
  if (!m->get_priority())
    m->set_priority(async_msgr->get_default_send_priority());

  m->get_header().src = async_msgr->get_myname();
  m->set_connection(this);

  if (async_msgr->get_myaddr() == get_peer_addr()) { //loopback connection
    ldout(async_msgr->cct, 20) << __func__ << " " << *m << " local" << dendl;
    std::lock_guard<std::mutex> l(write_lock);
    if (can_write != WriteStatus::CLOSED) {
      dispatch_queue->local_delivery(m, m->get_priority());
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

  std::lock_guard<std::mutex> l(write_lock);
  // "features" changes will change the payload encoding
  if (can_fast_prepare && (can_write == WriteStatus::NOWRITE || get_features() != f)) {
    // ensure the correctness of message encoding
    bl.clear();
    m->get_payload().clear();
    ldout(async_msgr->cct, 5) << __func__ << " clear encoded buffer previous "
                              << f << " != " << get_features() << dendl;
  }
  if (!is_queued() && can_write == WriteStatus::CANWRITE && async_msgr->cct->_conf->ms_async_send_inline) {
    if (!bl.length())
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
    if (can_write != WriteStatus::REPLACING)
      center->dispatch_event_external(write_handler);
  }
  return 0;
}

void AsyncConnection::requeue_sent()
{
  if (sent.empty())
    return;

  list<pair<bufferlist, Message*> >& rq = out_q[CEPH_MSG_PRIO_HIGHEST];
  while (!sent.empty()) {
    Message* m = sent.back();
    sent.pop_back();
    ldout(async_msgr->cct, 10) << __func__ << " " << *m << " for resend "
                               << " (" << m->get_seq() << ")" << dendl;
    rq.push_front(make_pair(bufferlist(), m));
    out_seq.dec();
  }
}

void AsyncConnection::discard_requeued_up_to(uint64_t seq)
{
  ldout(async_msgr->cct, 10) << __func__ << " " << seq << dendl;
  std::lock_guard<std::mutex> l(write_lock);
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
    out_seq.inc();
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
  if (state == STATE_CLOSED || state == STATE_NONE) {
    ldout(async_msgr->cct, 10) << __func__ << " connection is already closed" << dendl;
    return ;
  }

  if (policy.lossy && !(state >= STATE_CONNECTING && state < STATE_CONNECTING_READY)) {
    ldout(async_msgr->cct, 1) << __func__ << " on lossy channel, failing" << dendl;
    _stop();
    dispatch_queue->queue_reset(this);
    return ;
  }

  write_lock.lock();
  can_write = WriteStatus::NOWRITE;
  shutdown_socket();
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
    write_lock.unlock();
    _stop();
    dispatch_queue->queue_reset(this);
    return ;
  }
  reset_recv_state();
  if (policy.standby && !is_queued() && state != STATE_WAIT) {
    ldout(async_msgr->cct,0) << __func__ << " with nothing to send, going to standby" << dendl;
    state = STATE_STANDBY;
    write_lock.unlock();
    return;
  }

  write_lock.unlock();
  if (!(state >= STATE_CONNECTING && state < STATE_CONNECTING_READY) &&
      state != STATE_WAIT) { // STATE_WAIT is coming from STATE_CONNECTING_*
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
    center->dispatch_event_external(read_handler);
  } else {
    if (state == STATE_WAIT) {
      backoff.set_from_double(async_msgr->cct->_conf->ms_max_backoff);
    } else if (backoff == utime_t()) {
      backoff.set_from_double(async_msgr->cct->_conf->ms_initial_backoff);
    } else {
      backoff += backoff;
      if (backoff > async_msgr->cct->_conf->ms_max_backoff)
        backoff.set_from_double(async_msgr->cct->_conf->ms_max_backoff);
    }

    state = STATE_CONNECTING;
    ldout(async_msgr->cct, 10) << __func__ << " waiting " << backoff << dendl;
    // woke up again;
    register_time_events.insert(center->create_time_event(
            backoff.to_nsec()/1000, wakeup_handler));
  }
}

void AsyncConnection::was_session_reset()
{
  ldout(async_msgr->cct,10) << __func__ << " started" << dendl;
  std::lock_guard<std::mutex> l(write_lock);
  if (delay_state)
    delay_state->discard();
  dispatch_queue->discard_queue(conn_id);
  discard_out_queue();

  dispatch_queue->queue_remote_reset(this);

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
  if (state == STATE_CLOSED)
    return ;

  if (delay_state)
    delay_state->flush();

  ldout(async_msgr->cct, 2) << __func__ << dendl;
  std::lock_guard<std::mutex> l(write_lock);

  reset_recv_state();
  dispatch_queue->discard_queue(conn_id);
  discard_out_queue();
  async_msgr->unregister_conn(this);
  worker->release_worker();

  state = STATE_CLOSED;
  open_write = false;
  can_write = WriteStatus::CLOSED;
  state_offset = 0;
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
                              << cpp_strerror(rc) << dendl;
  } else if (rc == 0) {
    ldout(async_msgr->cct, 10) << __func__ << " sending " << m << " done." << dendl;
  } else {
    ldout(async_msgr->cct, 10) << __func__ << " sending " << m << " continuely." << dendl;
  }
  m->put();

  return rc;
}

void AsyncConnection::reset_recv_state()
{
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
    ldout(async_msgr->cct, 10) << __func__ << " releasing " << 1
                               << " message to policy throttler "
                               << policy.throttler_messages->get_current() << "/"
                               << policy.throttler_messages->get_max() << dendl;
    policy.throttler_messages->put();
  }
  if (state > STATE_OPEN_MESSAGE_THROTTLE_BYTES &&
      state <= STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH) {
    if (policy.throttler_bytes) {
      ldout(async_msgr->cct, 10) << __func__ << " releasing " << cur_msg_size
                                 << " bytes to policy throttler "
                                 << policy.throttler_bytes->get_current() << "/"
                                 << policy.throttler_bytes->get_max() << dendl;
      policy.throttler_bytes->put(cur_msg_size);
    }
  }
  if (state > STATE_OPEN_MESSAGE_THROTTLE_DISPATCH_QUEUE &&
      state <= STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH) {
    ldout(async_msgr->cct, 10) << __func__ << " releasing " << cur_msg_size
                               << " bytes to dispatch_queue throttler "
                               << dispatch_queue->dispatch_throttler.get_current() << "/"
                               << dispatch_queue->dispatch_throttler.get_max() << dendl;
    dispatch_queue->dispatch_throttle_release(cur_msg_size);
  }
}

void AsyncConnection::handle_ack(uint64_t seq)
{
  ldout(async_msgr->cct, 15) << __func__ << " got ack seq " << seq << dendl;
  // trim sent list
  std::lock_guard<std::mutex> l(write_lock);
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
    std::lock_guard<std::mutex> l(delay_lock);
    register_time_events.erase(id);
    if (stop_dispatch)
      return ;
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
    dispatch_queue->fast_dispatch(m);
  } else {
    dispatch_queue->enqueue(m, m->get_priority(), conn_id);
  }
}

void AsyncConnection::DelayedDelivery::flush() {
  stop_dispatch = true;
  center->submit_to(
      center->get_id(), [this] () mutable {
    std::lock_guard<std::mutex> l(delay_lock);
    while (!delay_queue.empty()) {
      Message *m = delay_queue.front().second;
      if (msgr->ms_can_fast_dispatch(m)) {
        dispatch_queue->fast_dispatch(m);
      } else {
        dispatch_queue->enqueue(m, m->get_priority(), conn_id);
      }
      delay_queue.pop_front();
    }
    for (auto i : register_time_events)
      center->delete_time_event(i);
    register_time_events.clear();
    stop_dispatch = false;
  }, true);
}

void AsyncConnection::send_keepalive()
{
  ldout(async_msgr->cct, 10) << __func__ << dendl;
  std::lock_guard<std::mutex> l(write_lock);
  if (can_write != WriteStatus::CLOSED) {
    keepalive = true;
    center->dispatch_event_external(write_handler);
  }
}

void AsyncConnection::mark_down()
{
  ldout(async_msgr->cct, 1) << __func__ << dendl;
  std::lock_guard<std::mutex> l(lock);
  _stop();
}

void AsyncConnection::_append_keepalive_or_ack(bool ack, utime_t *tp)
{
  ldout(async_msgr->cct, 10) << __func__ << dendl;
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
}

void AsyncConnection::handle_write()
{
  ldout(async_msgr->cct, 10) << __func__ << dendl;
  ssize_t r = 0;

  write_lock.lock();
  if (can_write == WriteStatus::CANWRITE) {
    if (keepalive) {
      _append_keepalive_or_ack();
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
        write_lock.unlock();
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

    write_lock.unlock();
    if (r < 0) {
      ldout(async_msgr->cct, 1) << __func__ << " send msg failed" << dendl;
      goto fail;
    }
  } else {
    write_lock.unlock();
    lock.lock();
    write_lock.lock();
    if (state == STATE_STANDBY && !policy.server && is_queued()) {
      ldout(async_msgr->cct, 10) << __func__ << " policy.server is false" << dendl;
      _connect();
    } else if (cs && state != STATE_NONE && state != STATE_CONNECTING && state != STATE_CONNECTING_RE && state != STATE_CLOSED) {
      r = _try_send();
      if (r < 0) {
        ldout(async_msgr->cct, 1) << __func__ << " send outcoming bl failed" << dendl;
        write_lock.unlock();
        fault();
        lock.unlock();
        return ;
      }
    }
    write_lock.unlock();
    lock.unlock();
  }

  return ;

 fail:
  lock.lock();
  fault();
  lock.unlock();
}

void AsyncConnection::wakeup_from(uint64_t id)
{
  lock.lock();
  register_time_events.erase(id);
  lock.unlock();
  process();
}

void AsyncConnection::tick(uint64_t id)
{
  auto now = ceph::coarse_mono_clock::now();
  ldout(async_msgr->cct, 20) << __func__ << " last_id=" << last_tick_id
                             << " last_active" << last_active << dendl;
  assert(last_tick_id == id);
  std::lock_guard<std::mutex> l(lock);
  last_tick_id = 0;
  auto idle_period = std::chrono::duration_cast<std::chrono::microseconds>(now - last_active).count();
  if (inactive_timeout_us < (uint64_t)idle_period) {
    ldout(async_msgr->cct, 1) << __func__ << " idle(" << idle_period << ") more than "
                              << inactive_timeout_us
                              << " us, mark self fault." << dendl;
    fault();
  } else if (is_connected()) {
    last_tick_id = center->create_time_event(inactive_timeout_us, tick_handler);
  }
}
