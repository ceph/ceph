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

#include <unistd.h>

#include "include/Context.h"
#include "include/random.h"
#include "common/errno.h"
#include "AsyncMessenger.h"
#include "AsyncConnection.h"

#include "ProtocolV1.h"
#include "ProtocolV2.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "common/EventTrace.h"

// Constant to limit starting sequence number to 2^31.  Nothing special about it, just a big number.  PLR
#define SEQ_MASK  0x7fffffff

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _conn_prefix(_dout)
ostream& AsyncConnection::_conn_prefix(std::ostream *_dout) {
  return *_dout << "-- " << async_msgr->get_myaddrs() << " >> "
		<< *peer_addrs << " conn(" << this
		<< (msgr2 ? " msgr2=" : " legacy=")
		<< protocol.get()
                << " :" << port
                << " s=" << get_state_name(state)
                << " l=" << policy.lossy
                << ").";
}

// Notes:
// 1. Don't dispatch any event when closed! It may cause AsyncConnection alive even if AsyncMessenger dead

const uint32_t AsyncConnection::TCP_PREFETCH_MIN_SIZE = 512;

class C_time_wakeup : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_time_wakeup(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t fd_or_id) override {
    conn->wakeup_from(fd_or_id);
  }
};

class C_handle_read : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_handle_read(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t fd_or_id) override {
    conn->process();
  }
};

class C_handle_write : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_handle_write(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t fd) override {
    conn->handle_write();
  }
};

class C_handle_write_callback : public EventCallback {
  AsyncConnectionRef conn;

public:
  explicit C_handle_write_callback(AsyncConnectionRef c) : conn(c) {}
  void do_request(uint64_t fd) override { conn->handle_write_callback(); }
};

class C_clean_handler : public EventCallback {
  AsyncConnectionRef conn;
 public:
  explicit C_clean_handler(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t id) override {
    conn->cleanup();
    delete this;
  }
};

class C_tick_wakeup : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_tick_wakeup(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t fd_or_id) override {
    conn->tick(fd_or_id);
  }
};


AsyncConnection::AsyncConnection(CephContext *cct, AsyncMessenger *m, DispatchQueue *q,
                                 Worker *w, bool m2, bool local)
  : Connection(cct, m), delay_state(NULL), async_msgr(m), conn_id(q->get_id()),
    logger(w->get_perf_counter()),
    state(STATE_NONE), port(-1),
    dispatch_queue(q), recv_buf(NULL),
    recv_max_prefetch(std::max<int64_t>(msgr->cct->_conf->ms_tcp_prefetch_max_size, TCP_PREFETCH_MIN_SIZE)),
    recv_start(0), recv_end(0),
    last_active(ceph::coarse_mono_clock::now()),
    inactive_timeout_us(cct->_conf->ms_tcp_read_timeout*1000*1000),
    msgr2(m2), state_offset(0),
    worker(w), center(&w->center),read_buffer(nullptr)
{
  read_handler = new C_handle_read(this);
  write_handler = new C_handle_write(this);
  write_callback_handler = new C_handle_write_callback(this);
  wakeup_handler = new C_time_wakeup(this);
  tick_handler = new C_tick_wakeup(this);
  // double recv_max_prefetch see "read_until"
  recv_buf = new char[2*recv_max_prefetch];
  if (local) {
    protocol = std::unique_ptr<Protocol>(new LoopbackProtocolV1(this));
  } else if (m2) {
    protocol = std::unique_ptr<Protocol>(new ProtocolV2(this));
  } else {
    protocol = std::unique_ptr<Protocol>(new ProtocolV1(this));
  }
  logger->inc(l_msgr_created_connections);
}

AsyncConnection::~AsyncConnection()
{
  if (recv_buf)
    delete[] recv_buf;
  ceph_assert(!delay_state);
}

void AsyncConnection::maybe_start_delay_thread()
{
  if (!delay_state) {
    async_msgr->cct->_conf.with_val<std::string>(
      "ms_inject_delay_type",
      [this](const string& s) {
	if (s.find(ceph_entity_type_name(peer_type)) != string::npos) {
	  ldout(msgr->cct, 1) << __func__ << " setting up a delay queue"
			      << dendl;
	  delay_state = new DelayedDelivery(async_msgr, center, dispatch_queue,
					    conn_id);
	}
      });
  }
}


ssize_t AsyncConnection::read(unsigned len, char *buffer,
                              std::function<void(char *, ssize_t)> callback) {
  ldout(async_msgr->cct, 20) << __func__
                             << (pendingReadLen ? " continue" : " start")
                             << " len=" << len << dendl;
  ssize_t r = read_until(len, buffer);
  if (r > 0) {
    readCallback = callback;
    pendingReadLen = len;
    read_buffer = buffer;
  }
  return r;
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
    uint64_t to_read = std::min<uint64_t>(recv_end - recv_start, left);
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
  if (left > (uint64_t)recv_max_prefetch) {
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

ssize_t AsyncConnection::write(bufferlist &bl,
                               std::function<void(ssize_t)> callback,
                               bool more) {

    std::unique_lock<std::mutex> l(write_lock);
    outcoming_bl.claim_append(bl);
    ssize_t r = _try_send(more);
    if (r > 0) {
      writeCallback = callback;
    }
    return r;
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

  ceph_assert(center->in_thread());
  ldout(async_msgr->cct, 25) << __func__ << " cs.send " << outcoming_bl.length()
                             << " bytes" << dendl;
  ssize_t r = cs.send(outcoming_bl, more);
  if (r < 0) {
    ldout(async_msgr->cct, 1) << __func__ << " send error: " << cpp_strerror(r) << dendl;
    return r;
  }

  ldout(async_msgr->cct, 10) << __func__ << " sent bytes " << r
                             << " remaining bytes " << outcoming_bl.length() << dendl;

  if (!open_write && is_queued()) {
    center->create_file_event(cs.fd(), EVENT_WRITABLE, write_handler);
    open_write = true;
  }

  if (open_write && !is_queued()) {
    center->delete_file_event(cs.fd(), EVENT_WRITABLE);
    open_write = false;
    if (writeCallback) {
      center->dispatch_event_external(write_callback_handler);
    }
  }

  return outcoming_bl.length();
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

void AsyncConnection::process() {
  std::lock_guard<std::mutex> l(lock);
  last_active = ceph::coarse_mono_clock::now();
  recv_start_time = ceph::mono_clock::now();

  ldout(async_msgr->cct, 20) << __func__ << dendl;

  switch (state) {
    case STATE_NONE: {
      ldout(async_msgr->cct, 20) << __func__ << " enter none state" << dendl;
      return;
    }
    case STATE_CLOSED: {
      ldout(async_msgr->cct, 20) << __func__ << " socket closed" << dendl;
      return;
    }
    case STATE_CONNECTING: {
      ceph_assert(!policy.server);

      if (cs) {
        center->delete_file_event(cs.fd(), EVENT_READABLE | EVENT_WRITABLE);
        cs.close();
      }

      SocketOptions opts;
      opts.priority = async_msgr->get_socket_priority();
      opts.connect_bind_addr = msgr->get_myaddrs().front();
      ssize_t r = worker->connect(target_addr, opts, &cs);
      if (r < 0) {
        protocol->fault();
        return;
      }

      center->create_file_event(cs.fd(), EVENT_READABLE, read_handler);
      state = STATE_CONNECTING_RE;
    }
    case STATE_CONNECTING_RE: {
      ssize_t r = cs.is_connected();
      if (r < 0) {
        ldout(async_msgr->cct, 1) << __func__ << " reconnect failed to "
                                  << target_addr << dendl;
        if (r == -ECONNREFUSED) {
          ldout(async_msgr->cct, 2)
              << __func__ << " connection refused!" << dendl;
          dispatch_queue->queue_refused(this);
        }
        protocol->fault();
        return;
      } else if (r == 0) {
        ldout(async_msgr->cct, 10)
            << __func__ << " nonblock connect inprogress" << dendl;
        if (async_msgr->get_stack()->nonblock_connect_need_writable_event()) {
          center->create_file_event(cs.fd(), EVENT_WRITABLE,
                                    read_handler);
        }
        logger->tinc(l_msgr_running_recv_time,
               ceph::mono_clock::now() - recv_start_time);
        return;
      }

      center->delete_file_event(cs.fd(), EVENT_WRITABLE);
      ldout(async_msgr->cct, 10)
          << __func__ << " connect successfully, ready to send banner" << dendl;
      state = STATE_CONNECTION_ESTABLISHED;
      break;
    }

    case STATE_ACCEPTING: {
      center->create_file_event(cs.fd(), EVENT_READABLE, read_handler);
      state = STATE_CONNECTION_ESTABLISHED;

      break;
    }

    case STATE_CONNECTION_ESTABLISHED: {
      if (pendingReadLen) {
        ssize_t r = read(*pendingReadLen, read_buffer, readCallback);
        if (r <= 0) { // read all bytes, or an error occured
          pendingReadLen.reset();
          char *buf_tmp = read_buffer;
          read_buffer = nullptr;
          readCallback(buf_tmp, r);
        }
        return;
      }
      break;
    }
  }

  protocol->read_event();

  logger->tinc(l_msgr_running_recv_time,
               ceph::mono_clock::now() - recv_start_time);
}

bool AsyncConnection::is_connected() {
  return protocol->is_connected();
}

void AsyncConnection::connect(const entity_addrvec_t &addrs, int type,
                              entity_addr_t &target) {

  std::lock_guard<std::mutex> l(lock);
  set_peer_type(type);
  set_peer_addrs(addrs);
  policy = msgr->get_policy(type);
  target_addr = target;
  _connect();
}

void AsyncConnection::_connect()
{
  ldout(async_msgr->cct, 10) << __func__ << dendl;

  state = STATE_CONNECTING;
  protocol->connect();
  // rescheduler connection in order to avoid lock dep
  // may called by external thread(send_message)
  center->dispatch_event_external(read_handler);
}

void AsyncConnection::accept(ConnectedSocket socket,
			     const entity_addr_t &listen_addr,
			     const entity_addr_t &peer_addr)
{
  ldout(async_msgr->cct, 10) << __func__ << " sd=" << socket.fd()
			     << " listen_addr " << listen_addr
			     << " peer_addr " << peer_addr << dendl;
  ceph_assert(socket.fd() >= 0);

  std::lock_guard<std::mutex> l(lock);
  cs = std::move(socket);
  socket_addr = listen_addr;
  target_addr = peer_addr; // until we know better
  state = STATE_ACCEPTING;
  protocol->accept();
  // rescheduler connection in order to avoid lock dep
  center->dispatch_event_external(read_handler);
}

int AsyncConnection::send_message(Message *m)
{
  FUNCTRACE(async_msgr->cct);
  lgeneric_subdout(async_msgr->cct, ms,
		   1) << "-- " << async_msgr->get_myaddrs() << " --> "
		      << get_peer_addrs() << " -- "
		      << *m << " -- " << m << " con "
		      << this
		      << dendl;

  // optimistic think it's ok to encode(actually may broken now)
  if (!m->get_priority())
    m->set_priority(async_msgr->get_default_send_priority());

  m->get_header().src = async_msgr->get_myname();
  m->set_connection(this);

  if (m->get_type() == CEPH_MSG_OSD_OP)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OP_BEGIN", true);
  else if (m->get_type() == CEPH_MSG_OSD_OPREPLY)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OPREPLY_BEGIN", true);

  if (async_msgr->get_myaddrs() == get_peer_addrs()) { //loopback connection
    ldout(async_msgr->cct, 20) << __func__ << " " << *m << " local" << dendl;
    std::lock_guard<std::mutex> l(write_lock);
    if (protocol->is_connected()) {
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

  protocol->send_message(m);
  return 0;
}

entity_addr_t AsyncConnection::_infer_target_addr(const entity_addrvec_t& av)
{
  // pick the first addr of the same address family as socket_addr.  it could be
  // an any: or v2: addr, we don't care.  it should not be a v1 addr.
  for (auto& i : av.v) {
    if (i.is_legacy()) {
      continue;
    }
    if (i.get_family() == socket_addr.get_family()) {
      ldout(async_msgr->cct,10) << __func__ << " " << av << " -> " << i << dendl;
      return i;
    }
  }
  ldout(async_msgr->cct,10) << __func__ << " " << av << " -> nothing to match "
			    << socket_addr << dendl;
  return {};
}

void AsyncConnection::fault()
{
  shutdown_socket();
  open_write = false;

  // queue delayed items immediately
  if (delay_state)
    delay_state->flush();

  recv_start = recv_end = 0;
  state_offset = 0;
  outcoming_bl.clear();
}

void AsyncConnection::_stop() {
  writeCallback.reset();
  dispatch_queue->discard_queue(conn_id);
  async_msgr->unregister_conn(this);
  worker->release_worker();

  state = STATE_CLOSED;
  open_write = false;

  state_offset = 0;
  // Make sure in-queue events will been processed
  center->dispatch_event_external(EventCallbackRef(new C_clean_handler(this)));
}

bool AsyncConnection::is_queued() const {
  return outcoming_bl.length();
}

void AsyncConnection::shutdown_socket() {
  for (auto &&t : register_time_events) center->delete_time_event(t);
  register_time_events.clear();
  if (last_tick_id) {
    center->delete_time_event(last_tick_id);
    last_tick_id = 0;
  }
  if (cs) {
    center->delete_file_event(cs.fd(), EVENT_READABLE | EVENT_WRITABLE);
    cs.shutdown();
    cs.close();
  }
}

void AsyncConnection::DelayedDelivery::do_request(uint64_t id)
{
  Message *m = nullptr;
  {
    std::lock_guard<std::mutex> l(delay_lock);
    register_time_events.erase(id);
    if (stop_dispatch)
      return ;
    if (delay_queue.empty())
      return ;
    m = delay_queue.front();
    delay_queue.pop_front();
  }
  if (msgr->ms_can_fast_dispatch(m)) {
    dispatch_queue->fast_dispatch(m);
  } else {
    dispatch_queue->enqueue(m, m->get_priority(), conn_id);
  }
}

void AsyncConnection::DelayedDelivery::discard() {
  stop_dispatch = true;
  center->submit_to(center->get_id(),
                    [this]() mutable {
                      std::lock_guard<std::mutex> l(delay_lock);
                      while (!delay_queue.empty()) {
                        Message *m = delay_queue.front();
                        dispatch_queue->dispatch_throttle_release(
                            m->get_dispatch_throttle_size());
                        m->put();
                        delay_queue.pop_front();
                      }
                      for (auto i : register_time_events)
                        center->delete_time_event(i);
                      register_time_events.clear();
                      stop_dispatch = false;
                    },
                    true);
}

void AsyncConnection::DelayedDelivery::flush() {
  stop_dispatch = true;
  center->submit_to(
      center->get_id(), [this] () mutable {
    std::lock_guard<std::mutex> l(delay_lock);
    while (!delay_queue.empty()) {
      Message *m = delay_queue.front();
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
  protocol->send_keepalive();
}

void AsyncConnection::mark_down()
{
  ldout(async_msgr->cct, 1) << __func__ << dendl;
  std::lock_guard<std::mutex> l(lock);
  protocol->stop();
}

void AsyncConnection::handle_write()
{
  ldout(async_msgr->cct, 10) << __func__ << dendl;
  protocol->write_event();
}

void AsyncConnection::handle_write_callback() {
  std::lock_guard<std::mutex> l(lock);
  last_active = ceph::coarse_mono_clock::now();
  recv_start_time = ceph::mono_clock::now();
  write_lock.lock();
  if (writeCallback) {
    auto callback = *writeCallback;
    writeCallback.reset();
    write_lock.unlock();
    callback(0);
    return;
  }
  write_lock.unlock();
}

void AsyncConnection::stop(bool queue_reset) {
  lock.lock();
  bool need_queue_reset = (state != STATE_CLOSED) && queue_reset;
  protocol->stop();
  lock.unlock();
  if (need_queue_reset) dispatch_queue->queue_reset(this);
}

void AsyncConnection::cleanup() {
  shutdown_socket();
  delete read_handler;
  delete write_handler;
  delete write_callback_handler;
  delete wakeup_handler;
  delete tick_handler;
  if (delay_state) {
    delete delay_state;
    delay_state = NULL;
  }
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
                             << " last_active=" << last_active << dendl;
  std::lock_guard<std::mutex> l(lock);
  last_tick_id = 0;
  auto idle_period = std::chrono::duration_cast<std::chrono::microseconds>(now - last_active).count();
  if (inactive_timeout_us < (uint64_t)idle_period) {
    ldout(async_msgr->cct, 1) << __func__ << " idle(" << idle_period << ") more than "
                              << inactive_timeout_us
                              << " us, mark self fault." << dendl;
    protocol->fault();
  } else if (is_connected()) {
    last_tick_id = center->create_time_event(inactive_timeout_us, tick_handler);
  }
}
