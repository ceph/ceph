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
#include <signal.h>
#include <climits>
#include <list>
#include <mutex>
#include <map>
using namespace std;

#include "auth/AuthSessionHandler.h"
#include "common/ceph_time.h"
#include "common/perf_counters.h"
#include "include/buffer.h"
#include "msg/Connection.h"
#include "msg/Messenger.h"

#include "Event.h"
#include "Stack.h"

class AsyncMessenger;
class Worker;

static const int ASYNC_IOV_MAX = (IOV_MAX >= 1024 ? IOV_MAX / 4 : IOV_MAX);

/*
 * AsyncConnection maintains a logic session between two endpoints. In other
 * word, a pair of addresses can find the only AsyncConnection. AsyncConnection
 * will handle with network fault or read/write transactions. If one file
 * descriptor broken, AsyncConnection will maintain the message queue and
 * sequence, try to reconnect peer endpoint.
 */
class AsyncConnection : public Connection {

  ssize_t read_bulk(char *buf, unsigned len);
  ssize_t do_sendmsg(struct msghdr &msg, unsigned len, bool more);
  ssize_t try_send(bufferlist &bl, bool more=false) {
    std::lock_guard<std::mutex> l(write_lock);
    outcoming_bl.claim_append(bl);
    return _try_send(more);
  }
  ssize_t _try_send(bool more=false);
  ssize_t _send(Message *m);
  void prepare_send_message(uint64_t features, Message *m, bufferlist &bl);
  ssize_t read_until(unsigned needed, char *p);
  ssize_t _process_connection();
  void _connect();
  void _stop();
  int handle_connect_reply(ceph_msg_connect &connect, ceph_msg_connect_reply &r);
  ssize_t handle_connect_msg(ceph_msg_connect &m, bufferlist &aubl, bufferlist &bl);
  void was_session_reset();
  void fault();
  void discard_out_queue();
  void discard_requeued_up_to(uint64_t seq);
  void requeue_sent();
  int randomize_out_seq();
  void handle_ack(uint64_t seq);
  void _append_keepalive_or_ack(bool ack=false, utime_t *t=NULL);
  ssize_t write_message(Message *m, bufferlist& bl, bool more);
  void inject_delay();
  ssize_t _reply_accept(char tag, ceph_msg_connect &connect, ceph_msg_connect_reply &reply,
                    bufferlist &authorizer_reply) {
    bufferlist reply_bl;
    reply.tag = tag;
    reply.features = ((uint64_t)connect.features & policy.features_supported) | policy.features_required;
    reply.authorizer_len = authorizer_reply.length();
    reply_bl.append((char*)&reply, sizeof(reply));
    if (reply.authorizer_len) {
      reply_bl.append(authorizer_reply.c_str(), authorizer_reply.length());
    }
    ssize_t r = try_send(reply_bl);
    if (r < 0) {
      inject_delay();
      return -1;
    }

    state = STATE_ACCEPTING_WAIT_CONNECT_MSG;
    return 0;
  }
  bool is_queued() const {
    return !out_q.empty() || outcoming_bl.length();
  }
  void shutdown_socket() {
    for (auto &&t : register_time_events)
      center->delete_time_event(t);
    register_time_events.clear();
    if (last_tick_id) {
      center->delete_time_event(last_tick_id);
      last_tick_id = 0;
    }
    if (cs) {
      center->delete_file_event(cs.fd(), EVENT_READABLE|EVENT_WRITABLE);
      cs.shutdown();
      cs.close();
    }
  }
  Message *_get_next_outgoing(bufferlist *bl) {
    Message *m = 0;
    while (!m && !out_q.empty()) {
      map<int, list<pair<bufferlist, Message*> > >::reverse_iterator it = out_q.rbegin();
      if (!it->second.empty()) {
        list<pair<bufferlist, Message*> >::iterator p = it->second.begin();
        m = p->second;
        if (bl)
          bl->swap(p->first);
        it->second.erase(p);
      }
      if (it->second.empty())
        out_q.erase(it->first);
    }
    return m;
  }
  bool _has_next_outgoing() const {
    return !out_q.empty();
  }
  void reset_recv_state();

   /**
   * The DelayedDelivery is for injecting delays into Message delivery off
   * the socket. It is only enabled if delays are requested, and if they
   * are then it pulls Messages off the DelayQueue and puts them into the
   * AsyncMessenger event queue.
   */
  class DelayedDelivery : public EventCallback {
    std::set<uint64_t> register_time_events; // need to delete it if stop
    std::deque<std::pair<utime_t, Message*> > delay_queue;
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
      assert(register_time_events.empty());
      assert(delay_queue.empty());
    }
    void set_center(EventCenter *c) { center = c; }
    void do_request(int id) override;
    void queue(double delay_period, utime_t release, Message *m) {
      std::lock_guard<std::mutex> l(delay_lock);
      delay_queue.push_back(std::make_pair(release, m));
      register_time_events.insert(center->create_time_event(delay_period*1000000, this));
    }
    void discard() {
      stop_dispatch = true;
      center->submit_to(center->get_id(), [this] () mutable {
        std::lock_guard<std::mutex> l(delay_lock);
        while (!delay_queue.empty()) {
          Message *m = delay_queue.front().second;
          dispatch_queue->dispatch_throttle_release(m->get_dispatch_throttle_size());
          m->put();
          delay_queue.pop_front();
        }
        for (auto i : register_time_events)
          center->delete_time_event(i);
        register_time_events.clear();
        stop_dispatch = false;
      }, true);
    }
    bool ready() const { return !stop_dispatch && delay_queue.empty() && register_time_events.empty(); }
    void flush();
  } *delay_state;

 public:
  AsyncConnection(CephContext *cct, AsyncMessenger *m, DispatchQueue *q, Worker *w);
  ~AsyncConnection() override;
  void maybe_start_delay_thread();

  ostream& _conn_prefix(std::ostream *_dout);

  bool is_connected() override {
    return can_write.load() == WriteStatus::CANWRITE;
  }

  // Only call when AsyncConnection first construct
  void connect(const entity_addr_t& addr, int type) {
    set_peer_type(type);
    set_peer_addr(addr);
    policy = msgr->get_policy(type);
    _connect();
  }
  // Only call when AsyncConnection first construct
  void accept(ConnectedSocket socket, entity_addr_t &addr);
  int send_message(Message *m) override;

  void send_keepalive() override;
  void mark_down() override;
  void mark_disposable() override {
    std::lock_guard<std::mutex> l(lock);
    policy.lossy = true;
  }
  
 private:
  enum {
    STATE_NONE,
    STATE_OPEN,
    STATE_OPEN_KEEPALIVE2,
    STATE_OPEN_KEEPALIVE2_ACK,
    STATE_OPEN_TAG_ACK,
    STATE_OPEN_MESSAGE_HEADER,
    STATE_OPEN_MESSAGE_THROTTLE_MESSAGE,
    STATE_OPEN_MESSAGE_THROTTLE_BYTES,
    STATE_OPEN_MESSAGE_THROTTLE_DISPATCH_QUEUE,
    STATE_OPEN_MESSAGE_READ_FRONT,
    STATE_OPEN_MESSAGE_READ_MIDDLE,
    STATE_OPEN_MESSAGE_READ_DATA_PREPARE,
    STATE_OPEN_MESSAGE_READ_DATA,
    STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH,
    STATE_OPEN_TAG_CLOSE,
    STATE_WAIT_SEND,
    STATE_CONNECTING,
    STATE_CONNECTING_RE,
    STATE_CONNECTING_WAIT_BANNER_AND_IDENTIFY,
    STATE_CONNECTING_SEND_CONNECT_MSG,
    STATE_CONNECTING_WAIT_CONNECT_REPLY,
    STATE_CONNECTING_WAIT_CONNECT_REPLY_AUTH,
    STATE_CONNECTING_WAIT_ACK_SEQ,
    STATE_CONNECTING_READY,
    STATE_ACCEPTING,
    STATE_ACCEPTING_WAIT_BANNER_ADDR,
    STATE_ACCEPTING_WAIT_CONNECT_MSG,
    STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH,
    STATE_ACCEPTING_WAIT_SEQ,
    STATE_ACCEPTING_READY,
    STATE_STANDBY,
    STATE_CLOSED,
    STATE_WAIT,       // just wait for racing connection
  };

  static const int TCP_PREFETCH_MIN_SIZE;
  static const char *get_state_name(int state) {
      const char* const statenames[] = {"STATE_NONE",
                                        "STATE_OPEN",
                                        "STATE_OPEN_KEEPALIVE2",
                                        "STATE_OPEN_KEEPALIVE2_ACK",
                                        "STATE_OPEN_TAG_ACK",
                                        "STATE_OPEN_MESSAGE_HEADER",
                                        "STATE_OPEN_MESSAGE_THROTTLE_MESSAGE",
                                        "STATE_OPEN_MESSAGE_THROTTLE_BYTES",
                                        "STATE_OPEN_MESSAGE_THROTTLE_DISPATCH_QUEUE",
                                        "STATE_OPEN_MESSAGE_READ_FRONT",
                                        "STATE_OPEN_MESSAGE_READ_MIDDLE",
                                        "STATE_OPEN_MESSAGE_READ_DATA_PREPARE",
                                        "STATE_OPEN_MESSAGE_READ_DATA",
                                        "STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH",
                                        "STATE_OPEN_TAG_CLOSE",
                                        "STATE_WAIT_SEND",
                                        "STATE_CONNECTING",
                                        "STATE_CONNECTING_RE",
                                        "STATE_CONNECTING_WAIT_BANNER_AND_IDENTIFY",
                                        "STATE_CONNECTING_SEND_CONNECT_MSG",
                                        "STATE_CONNECTING_WAIT_CONNECT_REPLY",
                                        "STATE_CONNECTING_WAIT_CONNECT_REPLY_AUTH",
                                        "STATE_CONNECTING_WAIT_ACK_SEQ",
                                        "STATE_CONNECTING_READY",
                                        "STATE_ACCEPTING",
                                        "STATE_ACCEPTING_WAIT_BANNER_ADDR",
                                        "STATE_ACCEPTING_WAIT_CONNECT_MSG",
                                        "STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH",
                                        "STATE_ACCEPTING_WAIT_SEQ",
                                        "STATE_ACCEPTING_READY",
                                        "STATE_STANDBY",
                                        "STATE_CLOSED",
                                        "STATE_WAIT"};
      return statenames[state];
  }

  AsyncMessenger *async_msgr;
  uint64_t conn_id;
  PerfCounters *logger;
  int global_seq;
  __u32 connect_seq, peer_global_seq;
  std::atomic<uint64_t> out_seq{0};
  std::atomic<uint64_t> ack_left{0}, in_seq{0};
  int state;
  int state_after_send;
  ConnectedSocket cs;
  int port;
  Messenger::Policy policy;

  DispatchQueue *dispatch_queue;

  // lockfree, only used in own thread
  bufferlist outcoming_bl;
  bool open_write = false;

  std::mutex write_lock;
  enum class WriteStatus {
    NOWRITE,
    REPLACING,
    CANWRITE,
    CLOSED
  };
  std::atomic<WriteStatus> can_write;
  list<Message*> sent; // the first bufferlist need to inject seq
  map<int, list<pair<bufferlist, Message*> > > out_q;  // priority queue for outbound msgs
  bool keepalive;

  std::mutex lock;
  utime_t backoff;         // backoff time
  EventCallbackRef read_handler;
  EventCallbackRef write_handler;
  EventCallbackRef wakeup_handler;
  EventCallbackRef tick_handler;
  struct iovec msgvec[ASYNC_IOV_MAX];
  char *recv_buf;
  uint32_t recv_max_prefetch;
  uint32_t recv_start;
  uint32_t recv_end;
  set<uint64_t> register_time_events; // need to delete it if stop
  ceph::coarse_mono_clock::time_point last_active;
  uint64_t last_tick_id = 0;
  const uint64_t inactive_timeout_us;

  // Tis section are temp variables used by state transition

  // Open state
  utime_t recv_stamp;
  utime_t throttle_stamp;
  unsigned msg_left;
  uint64_t cur_msg_size;
  ceph_msg_header current_header;
  bufferlist data_buf;
  bufferlist::iterator data_blp;
  bufferlist front, middle, data;
  ceph_msg_connect connect_msg;
  // Connecting state
  bool got_bad_auth;
  AuthAuthorizer *authorizer;
  bufferlist authorizer_buf;
  ceph_msg_connect_reply connect_reply;
  // Accepting state
  entity_addr_t socket_addr;
  CryptoKey session_key;
  bool replacing;    // when replacing process happened, we will reply connect
                     // side with RETRY tag and accept side will clear replaced
                     // connection. So when connect side reissue connect_msg,
                     // there won't exists conflicting connection so we use
                     // "replacing" to skip RESETSESSION to avoid detect wrong
                     // presentation
  bool is_reset_from_peer;
  bool once_ready;

  // used only for local state, it will be overwrite when state transition
  char *state_buffer;
  // used only by "read_until"
  uint64_t state_offset;
  Worker *worker;
  EventCenter *center;
  ceph::shared_ptr<AuthSessionHandler> session_security;

 public:
  // used by eventcallback
  void handle_write();
  void process();
  void wakeup_from(uint64_t id);
  void tick(uint64_t id);
  void local_deliver();
  void stop(bool queue_reset) {
    lock.lock();
    bool need_queue_reset = (state != STATE_CLOSED) && queue_reset;
    _stop();
    lock.unlock();
    if (need_queue_reset)
      dispatch_queue->queue_reset(this);
  }
  void cleanup() {
    shutdown_socket();
    delete read_handler;
    delete write_handler;
    delete wakeup_handler;
    delete tick_handler;
    if (delay_state) {
      delete delay_state;
      delay_state = NULL;
    }
  }
  PerfCounters *get_perf_counter() {
    return logger;
  }
}; /* AsyncConnection */

typedef boost::intrusive_ptr<AsyncConnection> AsyncConnectionRef;

#endif
