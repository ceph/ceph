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

#ifndef CEPH_ASYNCMESSENGER_H
#define CEPH_ASYNCMESSENGER_H

#include <map>
#include <mutex>

#include "include/types.h"
#include "include/xlist.h"
#include "include/spinlock.h"
#include "include/unordered_map.h"
#include "include/unordered_set.h"

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

#include "msg/SimplePolicyMessenger.h"
#include "msg/DispatchQueue.h"
#include "AsyncConnection.h"
#include "Event.h"

#include "include/ceph_assert.h"

class AsyncMessenger;

/**
 * If the Messenger binds to a specific address, the Processor runs
 * and listens for incoming connections.
 */
class Processor {
  AsyncMessenger *msgr;
  NetHandler net;
  Worker *worker;
  vector<ServerSocket> listen_sockets;
  EventCallbackRef listen_handler;

  class C_processor_accept;

 public:
  Processor(AsyncMessenger *r, Worker *w, CephContext *c);
  ~Processor() { delete listen_handler; };

  void stop();
  int bind(const entity_addrvec_t &bind_addrs,
	   const set<int>& avoid_ports,
	   entity_addrvec_t* bound_addrs);
  void start();
  void accept();
};

/*
 * AsyncMessenger is represented for maintaining a set of asynchronous connections,
 * it may own a bind address and the accepted connections will be managed by
 * AsyncMessenger.
 *
 */

class AsyncMessenger : public SimplePolicyMessenger {
  // First we have the public Messenger interface implementation...
public:
  /**
   * Initialize the AsyncMessenger!
   *
   * @param cct The CephContext to use
   * @param name The name to assign ourselves
   * _nonce A unique ID to use for this AsyncMessenger. It should not
   * be a value that will be repeated if the daemon restarts.
   */
  AsyncMessenger(CephContext *cct, entity_name_t name, const std::string &type,
                 string mname, uint64_t _nonce);

  /**
   * Destroy the AsyncMessenger. Pretty simple since all the work is done
   * elsewhere.
   */
  ~AsyncMessenger() override;

  /** @defgroup Accessors
   * @{
   */
  bool set_addr_unknowns(const entity_addrvec_t &addr) override;
  void set_addrs(const entity_addrvec_t &addrs) override;

  int get_dispatch_queue_len() override {
    return dispatch_queue.get_queue_len();
  }

  double get_dispatch_queue_max_age(utime_t now) override {
    return dispatch_queue.get_max_age(now);
  }
  /** @} Accessors */

  /**
   * @defgroup Configuration functions
   * @{
   */
  void set_cluster_protocol(int p) override {
    ceph_assert(!started && !did_bind);
    cluster_protocol = p;
  }

  int bind(const entity_addr_t& bind_addr) override;
  int rebind(const set<int>& avoid_ports) override;
  int client_bind(const entity_addr_t& bind_addr) override;

  int bindv(const entity_addrvec_t& bind_addrs) override;

  bool should_use_msgr2() override;

  /** @} Configuration functions */

  /**
   * @defgroup Startup/Shutdown
   * @{
   */
  int start() override;
  void wait() override;
  int shutdown() override;

  /** @} // Startup/Shutdown */

  /**
   * @defgroup Messaging
   * @{
   */
  int send_to(Message *m, int type, const entity_addrvec_t& addrs) override;

  /** @} // Messaging */

  /**
   * @defgroup Connection Management
   * @{
   */
  ConnectionRef connect_to(int type,
			   const entity_addrvec_t& addrs) override;
  ConnectionRef get_loopback_connection() override;
  void mark_down(const entity_addr_t& addr) override {
    mark_down_addrs(entity_addrvec_t(addr));
  }
  void mark_down_addrs(const entity_addrvec_t& addrs) override;
  void mark_down_all() override {
    shutdown_connections(true);
  }
  /** @} // Connection Management */

  /**
   * @defgroup Inner classes
   * @{
   */

  /**
   * @} // Inner classes
   */

protected:
  /**
   * @defgroup Messenger Interfaces
   * @{
   */
  /**
   * Start up the DispatchQueue thread once we have somebody to dispatch to.
   */
  void ready() override;
  /** @} // Messenger Interfaces */

private:

  /**
   * @defgroup Utility functions
   * @{
   */

  /**
   * Create a connection associated with the given entity (of the given type).
   * Initiate the connection. (This function returning does not guarantee
   * connection success.)
   *
   * @param addrs The address(es) of the entity to connect to.
   * @param type The peer type of the entity at the address.
   *
   * @return a pointer to the newly-created connection. Caller does not own a
   * reference; take one if you need it.
   */
  AsyncConnectionRef create_connect(const entity_addrvec_t& addrs, int type);

  /**
   * Queue up a Message for delivery to the entity specified
   * by addr and dest_type.
   * submit_message() is responsible for creating
   * new AsyncConnection (and closing old ones) as necessary.
   *
   * @param m The Message to queue up. This function eats a reference.
   * @param con The existing Connection to use, or NULL if you don't know of one.
   * @param dest_addr The address to send the Message to.
   * @param dest_type The peer type of the address we're sending to
   * just drop silently under failure.
   */
  void submit_message(Message *m, AsyncConnectionRef con,
                      const entity_addrvec_t& dest_addrs, int dest_type);

  void _finish_bind(const entity_addrvec_t& bind_addrs,
		    const entity_addrvec_t& listen_addrs);

  entity_addrvec_t _filter_addrs(const entity_addrvec_t& addrs);

 private:
  static const uint64_t ReapDeadConnectionThreshold = 5;

  NetworkStack *stack;
  std::vector<Processor*> processors;
  friend class Processor;
  DispatchQueue dispatch_queue;

  // the worker run messenger's cron jobs
  Worker *local_worker;

  std::string ms_type;

  /// overall lock used for AsyncMessenger data structures
  Mutex lock;
  // AsyncMessenger stuff
  /// approximately unique ID set by the Constructor for use in entity_addr_t
  uint64_t nonce;

  /// true, specifying we haven't learned our addr; set false when we find it.
  // maybe this should be protected by the lock?
  bool need_addr;

  /**
   * set to bind addresses if bind was called before NetworkStack was ready to
   * bind
   */
  entity_addrvec_t pending_bind_addrs;

  /**
   * false; set to true if a pending bind exists
   */
  bool pending_bind = false;

  /**
   *  The following aren't lock-protected since you shouldn't be able to race
   *  the only writers.
   */

  /**
   *  false; set to true if the AsyncMessenger bound to a specific address;
   *  and set false again by Accepter::stop().
   */
  bool did_bind;
  /// counter for the global seq our connection protocol uses
  __u32 global_seq;
  /// lock to protect the global_seq
  ceph::spinlock global_seq_lock;

  /**
   * hash map of addresses to Asyncconnection
   *
   * NOTE: a Asyncconnection* with state CLOSED may still be in the map but is considered
   * invalid and can be replaced by anyone holding the msgr lock
   */
  ceph::unordered_map<entity_addrvec_t, AsyncConnectionRef> conns;

  /**
   * list of connection are in the process of accepting
   *
   * These are not yet in the conns map.
   */
  set<AsyncConnectionRef> accepting_conns;

  /**
   * list of connection are closed which need to be clean up
   *
   * Because AsyncMessenger and AsyncConnection follow a lock rule that
   * we can lock AsyncMesenger::lock firstly then lock AsyncConnection::lock
   * but can't reversed. This rule is aimed to avoid dead lock.
   * So if AsyncConnection want to unregister itself from AsyncMessenger,
   * we pick up this idea that just queue itself to this set and do lazy
   * deleted for AsyncConnection. "_lookup_conn" must ensure not return a
   * AsyncConnection in this set.
   */
  Mutex deleted_lock;
  set<AsyncConnectionRef> deleted_conns;

  EventCallbackRef reap_handler;

  /// internal cluster protocol version, if any, for talking to entities of the same type.
  int cluster_protocol;

  Cond  stop_cond;
  bool stopped;

  AsyncConnectionRef _lookup_conn(const entity_addrvec_t& k) {
    ceph_assert(lock.is_locked());
    auto p = conns.find(k);
    if (p == conns.end())
      return NULL;

    // lazy delete, see "deleted_conns"
    Mutex::Locker l(deleted_lock);
    if (deleted_conns.erase(p->second)) {
      p->second->get_perf_counter()->dec(l_msgr_active_connections);
      conns.erase(p);
      return NULL;
    }

    return p->second;
  }

  void _init_local_connection() {
    ceph_assert(lock.is_locked());
    local_connection->peer_addrs = *my_addrs;
    local_connection->peer_type = my_name.type();
    local_connection->set_features(CEPH_FEATURES_ALL);
    ms_deliver_handle_fast_connect(local_connection.get());
  }

  void shutdown_connections(bool queue_reset);

public:

  /// con used for sending messages to ourselves
  AsyncConnectionRef local_connection;

  /**
   * @defgroup AsyncMessenger internals
   * @{
   */
  /**
   * This wraps _lookup_conn.
   */
  AsyncConnectionRef lookup_conn(const entity_addrvec_t& k) {
    Mutex::Locker l(lock);
    return _lookup_conn(k);
  }

  int accept_conn(AsyncConnectionRef conn);
  bool learned_addr(const entity_addr_t &peer_addr_for_me);
  void add_accept(Worker *w, ConnectedSocket cli_socket,
		  const entity_addr_t &listen_addr,
		  const entity_addr_t &peer_addr);
  NetworkStack *get_stack() {
    return stack;
  }

  uint64_t get_nonce() const {
    return nonce;
  }

  /**
   * Increment the global sequence for this AsyncMessenger and return it.
   * This is for the connect protocol, although it doesn't hurt if somebody
   * else calls it.
   *
   * @return a global sequence ID that nobody else has seen.
   */
  __u32 get_global_seq(__u32 old=0) {
    std::lock_guard<ceph::spinlock> lg(global_seq_lock);

    if (old > global_seq)
      global_seq = old;
    __u32 ret = ++global_seq;

    return ret;
  }
  /**
   * Get the protocol version we support for the given peer type: either
   * a peer protocol (if it matches our own), the protocol version for the
   * peer (if we're connecting), or our protocol version (if we're accepting).
   */
  int get_proto_version(int peer_type, bool connect) const;

  /**
   * Fill in the address and peer type for the local connection, which
   * is used for delivering messages back to ourself.
   */
  void init_local_connection() {
    Mutex::Locker l(lock);
    local_connection->is_loopback = true;
    _init_local_connection();
  }

  /**
   * Unregister connection from `conns`
   *
   * See "deleted_conns"
   */
  void unregister_conn(AsyncConnectionRef conn) {
    Mutex::Locker l(deleted_lock);
    deleted_conns.insert(conn);

    if (deleted_conns.size() >= ReapDeadConnectionThreshold) {
      local_worker->center.dispatch_event_external(reap_handler);
    }
  }

  /**
   * Reap dead connection from `deleted_conns`
   *
   * @return the number of dead connections
   *
   * See "deleted_conns"
   */
  int reap_dead();

  /**
   * @} // AsyncMessenger Internals
   */
} ;

#endif /* CEPH_ASYNCMESSENGER_H */
