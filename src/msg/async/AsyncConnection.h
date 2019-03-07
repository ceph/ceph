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

#ifndef CEPH_MSG_ASYNCCONNECTION_H
#define CEPH_MSG_ASYNCCONNECTION_H

#include <atomic>
#include <pthread.h>
#include <climits>
#include <list>
#include <mutex>
#include <map>
#include <functional>
#include <optional>

#include "auth/AuthSessionHandler.h"
#include "common/ceph_time.h"
#include "common/perf_counters.h"
#include "include/buffer.h"
#include "msg/Connection.h"
#include "msg/Messenger.h"

#include "Event.h"
#include "Stack.h"

class AsyncConnection;
class AsyncMessenger;
class DispatchQueue;
class Worker;
class Protocol;

struct QueuedMessage {
  Message *msg;
  unsigned int length;
  unsigned int payload_len = 0; /* Cache length, buffers can be claimed */
  unsigned int middle_len = 0;
  unsigned int data_len = 0;
  bool encoded;
  char tag;
  union {
    ceph_le64 s;
    struct ceph_timespec ts;
    ceph_msg_footer_old old_footer;
  } static_payload;  /* 13 bytes */

  QueuedMessage(Message *msg_, bool encoded_) :
    msg(msg_),
    length(0),
    encoded(encoded_),
    tag(CEPH_MSGR_TAG_MSG) {
  }

  QueuedMessage(char tag_, const void *data_, unsigned int len_) :
    msg(nullptr),
    length(len_ + sizeof(tag)),
    encoded(true),
    tag(tag_) {
    ceph_assert(len_ <= sizeof(static_payload));
    if (len_)
      memcpy(&static_payload, data_, len_);
  }

  ~QueuedMessage() {
    if (msg)
      msg->put();
  }
};

struct WriteQueue {
  AsyncConnection *con;

  /*
   * Before changing values, please, do appropriate performance
   * measurements for all possible sets of block sizes. Values
   * below were not chosen by chance. @IOV_NUM is used for both
   * iovec and bufferlist.
   */
  static const unsigned int IOV_NUM  = 64;
  static const unsigned int MAX_SIZE = 512<<10;

  std::array<struct iovec, IOV_NUM> iovec;
  decltype(iovec)::iterator iovec_beg_it;
  decltype(iovec)::iterator iovec_end_it;
  unsigned int iovec_len;

  /* Either iovec, either bufferlist. */
  bufferlist outbl;

  /*
   * @msgs list holds all queued messages, but among all of them there are
   * messages which are:
   *
   *    1. sent, but not acked yet (!lossy mode):
   *        [msgs.begin(), msgs_beg_it)
   *
   *    2. mapped to outcoming @iovec or @outbl, i.e. are ready to be sent:
   *        [msgs_beg_it, msgs_end_it)
   *
   *    3. picked up from the out_q, not yet mapped:
   *        [msgs_end_it, msgs.end())
   *
   */
  std::list<QueuedMessage> msgs;
  decltype(msgs)::iterator msgs_beg_it;
  decltype(msgs)::iterator msgs_end_it;
  unsigned int msg_beg_pos;
  unsigned int msg_end_pos;

  WriteQueue(AsyncConnection *con_) :
    con(con_),
    iovec_beg_it(iovec.begin()),
    iovec_end_it(iovec.begin()),
    iovec_len(0),
    msgs_beg_it(msgs.end()),
    msgs_end_it(msgs.end()),
    msg_beg_pos(0),
    msg_end_pos(0)
  {}

  bool is_outcoming_full() const {
    return iovec_end_it == iovec.end() ||
	   iovec_len >= MAX_SIZE ||
	   outbl.buffers().size() >= IOV_NUM ||
	   outbl.length() >= MAX_SIZE;
  }

  bool has_msgs_to_send() const {
    return msgs_beg_it != msgs.end();
  }

  bool has_msgs_in_outcoming() const {
    return msg_beg_pos != msg_end_pos ||
           msgs_beg_it != msgs_end_it;
  }

  void enqueue(std::list<QueuedMessage> &list) {
    std::list<QueuedMessage>::iterator beg_it;

    ceph_assert(!list.empty());
    beg_it = list.begin();
    msgs.splice(msgs.end(), list);
    if (msgs_beg_it == msgs.end())
      msgs_beg_it = beg_it;
    if (msgs_end_it == msgs.end())
      msgs_end_it = beg_it;
  }

  void enqueue(char tag, void *data, size_t len) {
    msgs.emplace_back(tag, data, len);
    if (msgs_beg_it == msgs.end())
      msgs_beg_it = --msgs.end();
    if (msgs_end_it == msgs.end())
      msgs_end_it = --msgs.end();
  }

  bufferlist::buffers_t::const_iterator
  find_buf(const bufferlist::buffers_t &bufs,
	   unsigned int &pos);

  bool fillin_iovec_from_mem(void *mem, unsigned int beg, unsigned int end);
  bool fillin_iovec_from_bufl(bufferlist &bufl, unsigned int beg);
  bool fillin_bufl_from_mem(void *mem, unsigned int beg, unsigned int end);
  bool fillin_bufl_from_bufl(bufferlist &bufl, unsigned int beg);

  void fillin_iovec();
  void fillin_bufferlist();
  void advance(unsigned int size);

private:
  void advance_iovec(unsigned int size);
  void advance_msgs(unsigned int size);
};

/*
 * AsyncConnection maintains a logic session between two endpoints. In other
 * word, a pair of addresses can find the only AsyncConnection. AsyncConnection
 * will handle with network fault or read/write transactions. If one file
 * descriptor broken, AsyncConnection will maintain the message queue and
 * sequence, try to reconnect peer endpoint.
 */
class AsyncConnection : public Connection {

  ssize_t read(unsigned len, char *buffer,
               std::function<void(char *, ssize_t)> callback);
  ssize_t read_until(unsigned needed, char *p);
  ssize_t read_bulk(char *buf, unsigned len);

  ssize_t write(bufferlist &bl, std::function<void(ssize_t)> callback,
                bool more=false);
  ssize_t _try_send(bool more=false);

  void _connect();
  void _stop();
  void fault();
  void inject_delay();

  bool is_queued() const;
  void shutdown_socket();

   /**
   * The DelayedDelivery is for injecting delays into Message delivery off
   * the socket. It is only enabled if delays are requested, and if they
   * are then it pulls Messages off the DelayQueue and puts them into the
   * AsyncMessenger event queue.
   */
  class DelayedDelivery : public EventCallback {
    std::set<uint64_t> register_time_events; // need to delete it if stop
    std::deque<Message*> delay_queue;
    std::mutex delay_lock;
    AsyncMessenger *msgr;
    EventCenter *center;
    DispatchQueue *dispatch_queue;
    uint64_t conn_id;
    std::atomic_bool stop_dispatch;

   public:
    explicit DelayedDelivery(AsyncMessenger *omsgr, EventCenter *c,
                             DispatchQueue *q, uint64_t cid)
      : msgr(omsgr), center(c), dispatch_queue(q), conn_id(cid),
        stop_dispatch(false) { }
    ~DelayedDelivery() override {
      ceph_assert(register_time_events.empty());
      ceph_assert(delay_queue.empty());
    }
    void set_center(EventCenter *c) { center = c; }
    void do_request(uint64_t id) override;
    void queue(double delay_period, Message *m) {
      std::lock_guard<std::mutex> l(delay_lock);
      delay_queue.push_back(m);
      register_time_events.insert(center->create_time_event(delay_period*1000000, this));
    }
    void discard();
    bool ready() const { return !stop_dispatch && delay_queue.empty() && register_time_events.empty(); }
    void flush();
  } *delay_state;

 public:
  AsyncConnection(CephContext *cct, AsyncMessenger *m, DispatchQueue *q,
		  Worker *w, bool is_msgr2, bool local);
  ~AsyncConnection() override;
  void maybe_start_delay_thread();

  ostream& _conn_prefix(std::ostream *_dout);

  bool is_connected() override;

  // Only call when AsyncConnection first construct
  void connect(const entity_addrvec_t& addrs, int type, entity_addr_t& target);

  // Only call when AsyncConnection first construct
  void accept(ConnectedSocket socket,
	      const entity_addr_t &listen_addr,
	      const entity_addr_t &peer_addr);
  int send_message(Message *m) override;

  void send_keepalive() override;
  void mark_down() override;
  void mark_disposable() override {
    std::lock_guard<std::mutex> l(lock);
    policy.lossy = true;
  }

  entity_addr_t get_peer_socket_addr() const override {
    return target_addr;
  }

  int get_con_mode() const override;

 private:
  enum {
    STATE_NONE,
    STATE_CONNECTING,
    STATE_CONNECTING_RE,
    STATE_ACCEPTING,
    STATE_CONNECTION_ESTABLISHED,
    STATE_CLOSED
  };

  static const uint32_t TCP_PREFETCH_MIN_SIZE;
  static const char *get_state_name(int state) {
      const char* const statenames[] = {"STATE_NONE",
                                        "STATE_CONNECTING",
                                        "STATE_CONNECTING_RE",
                                        "STATE_ACCEPTING",
                                        "STATE_CONNECTION_ESTABLISHED",
                                        "STATE_CLOSED"};
      return statenames[state];
  }

  AsyncMessenger *async_msgr;
  uint64_t conn_id;
  PerfCounters *logger;
  int state;
  ConnectedSocket cs;
  int port;
  Messenger::Policy policy;

  DispatchQueue *dispatch_queue;

  WriteQueue *wqueue;
  bufferlist outcoming_bl;
  bool open_write = false;

  std::mutex write_lock;

  std::mutex lock;
  EventCallbackRef read_handler;
  EventCallbackRef write_handler;
  EventCallbackRef write_callback_handler;
  EventCallbackRef wakeup_handler;
  EventCallbackRef tick_handler;
  char *recv_buf;
  uint32_t recv_max_prefetch;
  uint32_t recv_start;
  uint32_t recv_end;
  set<uint64_t> register_time_events; // need to delete it if stop
  ceph::coarse_mono_clock::time_point last_connect_started;
  ceph::coarse_mono_clock::time_point last_active;
  ceph::mono_clock::time_point recv_start_time;
  uint64_t last_tick_id = 0;
  const uint64_t connect_timeout_us;
  const uint64_t inactive_timeout_us;

  // Tis section are temp variables used by state transition

  // Accepting state
  bool msgr2 = false;
  entity_addr_t socket_addr;  ///< local socket addr
  entity_addr_t target_addr;  ///< which of the peer_addrs we're connecting to (as clienet) or should reconnect to (as peer)

  entity_addr_t _infer_target_addr(const entity_addrvec_t& av);

  // used only by "read_until"
  uint64_t state_offset;
  Worker *worker;
  EventCenter *center;

  std::unique_ptr<Protocol> protocol;

  std::optional<std::function<void(ssize_t)>> writeCallback;
  std::function<void(char *, ssize_t)> readCallback;
  std::optional<unsigned> pendingReadLen;
  char *read_buffer;

 public:
  // used by eventcallback
  void handle_write();
  void handle_write_callback();
  void process();
  void wakeup_from(uint64_t id);
  void tick(uint64_t id);
  void local_deliver();
  void stop(bool queue_reset);
  void cleanup();
  PerfCounters *get_perf_counter() {
    return logger;
  }

  friend class WriteQueue;
  friend class Protocol;
  friend class ProtocolV1;
  friend class ProtocolV2;
}; /* AsyncConnection */

typedef boost::intrusive_ptr<AsyncConnection> AsyncConnectionRef;

#endif
