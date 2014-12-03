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

#include <list>
#include <map>
using namespace std;

#include "common/Mutex.h"
#include "include/buffer.h"

#include "auth/AuthSessionHandler.h"
#include "include/buffer.h"
#include "msg/Connection.h"
#include "net_handler.h"
#include "Event.h"
#include "msg/Messenger.h"

class AsyncMessenger;

/*
 * AsyncConnection maintains a logic session between two endpoints. In other
 * word, a pair of addresses can find the only AsyncConnection. AsyncConnection
 * will handle with network fault or read/write transactions. If one file
 * descriptor broken, AsyncConnection will maintain the message queue and
 * sequence, try to reconnect peer endpoint.
 */
class AsyncConnection : public Connection {
  const static uint64_t IOV_LEN = 1024;

  int read_bulk(int fd, char *buf, int len);
  int do_sendmsg(struct msghdr &msg, int len, bool more);
  // if "send" is false, it will only append bl to send buffer
  // the main usage is avoid error happen outside messenger threads
  int _try_send(bufferlist bl, bool send=true);
  int _send(Message *m);
  int read_until(uint64_t needed, bufferptr &p);
  int _process_connection();
  void _connect();
  void _stop(bool external=false);
  int handle_connect_reply(ceph_msg_connect &connect, ceph_msg_connect_reply &r);
  int handle_connect_msg(ceph_msg_connect &m, bufferlist &aubl, bufferlist &bl);
  void was_session_reset();
  void fault();
  void discard_out_queue();
  void discard_requeued_up_to(uint64_t seq);
  void requeue_sent();
  int randomize_out_seq();
  void handle_ack(uint64_t seq);
  void _send_keepalive_or_ack(bool ack=false, utime_t *t=NULL);
  int write_message(ceph_msg_header& header, ceph_msg_footer& footer, bufferlist& blist);
  int _reply_accept(char tag, ceph_msg_connect &connect, ceph_msg_connect_reply &reply,
                    bufferlist authorizer_reply) {
    bufferlist reply_bl;
    reply.tag = tag;
    reply.features = ((uint64_t)connect.features & policy.features_supported) | policy.features_required;
    reply.authorizer_len = authorizer_reply.length();
    reply_bl.append((char*)&reply, sizeof(reply));
    if (reply.authorizer_len) {
      reply_bl.append(authorizer_reply.c_str(), authorizer_reply.length());
    }
    int r = _try_send(reply_bl);
    if (r < 0)
      return -1;

    state = STATE_ACCEPTING_WAIT_CONNECT_MSG;
    return 0;
  }
  bool is_queued() {
    return !out_q.empty() || outcoming_bl.length();
  }
  void shutdown_socket() {
    if (sd >= 0)
      ::shutdown(sd, SHUT_RDWR);
  }
  Message *_get_next_outgoing() {
    Message *m = 0;
    while (!m && !out_q.empty()) {
      map<int, list<Message*> >::reverse_iterator p = out_q.rbegin();
      if (!p->second.empty()) {
        m = p->second.front();
        p->second.pop_front();
      }
      if (p->second.empty())
        out_q.erase(p->first);
    }
    return m;
  }
 public:
  AsyncConnection(CephContext *cct, AsyncMessenger *m, EventCenter *c);
  ~AsyncConnection();

  ostream& _conn_prefix(std::ostream *_dout);

  bool is_connected() {
    // FIXME?
    return state != STATE_CLOSED;
  }

  // Only call when AsyncConnection first construct
  void connect(const entity_addr_t& addr, int type) {
    set_peer_type(type);
    set_peer_addr(addr);
    policy = msgr->get_policy(type);
    _connect();
  }
  // Only call when AsyncConnection first construct
  void accept(int sd);
  int send_message(Message *m);

  void send_keepalive();
  // Don't call it from AsyncConnection
  void mark_down() {
    Mutex::Locker l(lock);
    _stop(true);
  }
  void mark_disposable() {
    Mutex::Locker l(lock);
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
    STATE_OPEN_MESSAGE_READ_FRONT,
    STATE_OPEN_MESSAGE_READ_MIDDLE,
    STATE_OPEN_MESSAGE_READ_DATA_PREPARE,
    STATE_OPEN_MESSAGE_READ_DATA,
    STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH,
    STATE_OPEN_TAG_CLOSE,
    STATE_WAIT_SEND,
    STATE_CONNECTING,
    STATE_CONNECTING_WAIT_BANNER,
    STATE_CONNECTING_WAIT_IDENTIFY_PEER,
    STATE_CONNECTING_SEND_CONNECT_MSG,
    STATE_CONNECTING_WAIT_CONNECT_REPLY,
    STATE_CONNECTING_WAIT_CONNECT_REPLY_AUTH,
    STATE_CONNECTING_WAIT_ACK_SEQ,
    STATE_CONNECTING_READY,
    STATE_ACCEPTING,
    STATE_ACCEPTING_HANDLE_CONNECT,
    STATE_ACCEPTING_WAIT_BANNER_ADDR,
    STATE_ACCEPTING_WAIT_CONNECT_MSG,
    STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH,
    STATE_ACCEPTING_WAIT_SEQ,
    STATE_ACCEPTING_READY,
    STATE_STANDBY,
    STATE_CLOSED,
    STATE_WAIT,       // just wait for racing connection
  };

  static const char *get_state_name(int state) {
      const char* const statenames[] = {"STATE_NONE",
                                        "STATE_OPEN",
                                        "STATE_OPEN_KEEPALIVE2",
                                        "STATE_OPEN_KEEPALIVE2_ACK",
                                        "STATE_OPEN_TAG_ACK",
                                        "STATE_OPEN_MESSAGE_HEADER",
                                        "STATE_OPEN_MESSAGE_THROTTLE_MESSAGE",
                                        "STATE_OPEN_MESSAGE_THROTTLE_BYTES",
                                        "STATE_OPEN_MESSAGE_READ_FRONT",
                                        "STATE_OPEN_MESSAGE_READ_MIDDLE",
                                        "STATE_OPEN_MESSAGE_READ_DATA_PREPARE",
                                        "STATE_OPEN_MESSAGE_READ_DATA",
                                        "STATE_OPEN_MESSAGE_READ_FOOTER_AND_DISPATCH",
                                        "STATE_OPEN_TAG_CLOSE",
                                        "STATE_WAIT_SEND",
                                        "STATE_CONNECTING",
                                        "STATE_CONNECTING_WAIT_BANNER",
                                        "STATE_CONNECTING_WAIT_IDENTIFY_PEER",
                                        "STATE_CONNECTING_SEND_CONNECT_MSG",
                                        "STATE_CONNECTING_WAIT_CONNECT_REPLY",
                                        "STATE_CONNECTING_WAIT_CONNECT_REPLY_AUTH",
                                        "STATE_CONNECTING_WAIT_ACK_SEQ",
                                        "STATE_CONNECTING_READY",
                                        "STATE_ACCEPTING",
                                        "STATE_ACCEPTING_HANDLE_CONNECT",
                                        "STATE_ACCEPTING_WAIT_BANNER_ADDR",
                                        "STATE_ACCEPTING_WAIT_CONNECT_MSG",
                                        "STATE_ACCEPTING_WAIT_CONNECT_MSG_AUTH",
                                        "STATE_ACCEPTING_WAIT_SEQ",
                                        "STATE_ACCEPTING_READY",
                                        "STATE_STANDBY",
                                        "STATE_CLOSED",
                                        "STATE_WAIT",
                                        "STATE_FAULT"};
      return statenames[state];
  }

  CephContext *cc;
  AsyncMessenger *async_msgr;
  int global_seq;
  __u32 connect_seq, peer_global_seq;
  uint64_t out_seq;
  uint64_t in_seq, in_seq_acked;
  int state;
  int state_after_send;
  int sd;
  int port;
  Messenger::Policy policy;
  map<int, list<Message*> > out_q;  // priority queue for outbound msgs
  list<Message*> sent;
  Mutex lock;
  utime_t backoff;         // backoff time
  bool open_write;
  EventCallbackRef read_handler;
  EventCallbackRef write_handler;
  EventCallbackRef reset_handler;
  EventCallbackRef remote_reset_handler;
  bool keepalive;
  struct iovec msgvec[IOV_LEN];

  // Tis section are temp variables used by state transition

  // Open state
  utime_t recv_stamp;
  utime_t throttle_stamp;
  uint64_t msg_left;
  ceph_msg_header current_header;
  bufferlist data_buf;
  bufferlist::iterator data_blp;
  bufferlist front, middle, data;
  ceph_msg_connect connect_msg;
  // Connecting state
  bool got_bad_auth;
  AuthAuthorizer *authorizer;
  ceph_msg_connect_reply connect_reply;
  // Accepting state
  entity_addr_t socket_addr;
  CryptoKey session_key;

  // used only for local state, it will be overwrite when state transition
  bufferptr state_buffer;
  // used only by "read_until"
  uint64_t state_offset;
  bufferlist outcoming_bl;
  NetHandler net;
  EventCenter *center;
  ceph::shared_ptr<AuthSessionHandler> session_security;

 public:
  // used by eventcallback
  void handle_write();
  void process();
}; /* AsyncConnection */

typedef boost::intrusive_ptr<AsyncConnection> AsyncConnectionRef;

#endif
