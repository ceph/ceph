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

#include "include/types.h"
#include "include/xlist.h"

#include <list>
#include <map>
using namespace std;
#include "include/unordered_map.h"
#include "include/unordered_set.h"

#include "common/Mutex.h"
#include "include/atomic.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "common/Throttle.h"

#include "msg/SimplePolicyMessenger.h"
#include "include/assert.h"
#include "AsyncConnection.h"
#include "Event.h"


class AsyncMessenger;

/**
 * If the Messenger binds to a specific address, the Processor runs
 * and listens for incoming connections.
 */
class Processor : public Thread {
  AsyncMessenger *msgr;
  bool done;
  int listen_sd;
  uint64_t nonce;

  public:
  Processor(AsyncMessenger *r, uint64_t n) : msgr(r), done(false), listen_sd(-1), nonce(n) {}

  void *entry();
  void stop();
  int bind(const entity_addr_t &bind_addr, const set<int>& avoid_ports);
  int rebind(const set<int>& avoid_port);
  int start();
  void accept();
};

class Worker : public Thread {
  AsyncMessenger *msgr;
  bool done;

 public:
  EventCenter center;
  Worker(AsyncMessenger *m, CephContext *c): msgr(m), done(false), center(c) {
    center.init(5000);
  }
  void *entry();
  void stop();
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
  AsyncMessenger(CephContext *cct, entity_name_t name,
                 string mname, uint64_t _nonce);

  /**
   * Destroy the AsyncMessenger. Pretty simple since all the work is done
   * elsewhere.
   */
  virtual ~AsyncMessenger();

  /** @defgroup Accessors
   * @{
   */
  void set_addr_unknowns(entity_addr_t& addr);

  int get_dispatch_queue_len() {
    return 0;
  }

  double get_dispatch_queue_max_age(utime_t now) {
    return 0;
  }
  /** @} Accessors */

  /**
   * @defgroup Configuration functions
   * @{
   */
  void set_cluster_protocol(int p) {
    assert(!started && !did_bind);
    cluster_protocol = p;
  }

  int bind(const entity_addr_t& bind_addr);
  int rebind(const set<int>& avoid_ports);

  /** @} Configuration functions */

  /**
   * @defgroup Startup/Shutdown
   * @{
   */
  virtual int start();
  virtual void wait();
  virtual int shutdown();

  /** @} // Startup/Shutdown */

  /**
   * @defgroup Messaging
   * @{
   */
  virtual int send_message(Message *m, const entity_inst_t& dest) {
          Mutex::Locker l(lock);

    return _send_message(m, dest);
  }

  /** @} // Messaging */

  /**
   * @defgroup Connection Management
   * @{
   */
  virtual ConnectionRef get_connection(const entity_inst_t& dest);
  virtual ConnectionRef get_loopback_connection();
  int send_keepalive(Connection *con);
  virtual void mark_down(const entity_addr_t& addr);
  virtual void mark_down_all();
  /** @} // Connection Management */

  /**
   * @defgroup Inner classes
   * @{
   */

  Connection *create_anon_connection() {
    Mutex::Locker l(lock);
    Worker *w = workers[conn_id % workers.size()];
    conn_id++;
    return new AsyncConnection(cct, this, &w->center);
  }

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
  virtual void ready();
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
   * @param addr The address of the entity to connect to.
   * @param type The peer type of the entity at the address.
   * @param con An existing Connection to associate with the new connection. If
   * NULL, it creates a new Connection.
   * @param msg an initial message to queue on the new connection
   *
   * @return a pointer to the newly-created connection. Caller does not own a
   * reference; take one if you need it.
   */
  AsyncConnectionRef create_connect(const entity_addr_t& addr, int type);

  /**
   * Queue up a Message for delivery to the entity specified
   * by addr and dest_type.
   * submit_message() is responsible for creating
   * new AsyncConnection (and closing old ones) as necessary.
   *
   * @param m The Message to queue up. This function eats a reference.
   * @param con The existing Connection to use, or NULL if you don't know of one.
   * @param addr The address to send the Message to.
   * @param dest_type The peer type of the address we're sending to
   * just drop silently under failure.
   */
  void submit_message(Message *m, AsyncConnectionRef con,
                      const entity_addr_t& dest_addr, int dest_type);

  int _send_message(Message *m, const entity_inst_t& dest);

 private:
  vector<Worker*> workers;
  int conn_id;

  Processor processor;
  friend class Processor;

  /// overall lock used for AsyncMessenger data structures
  Mutex lock;
  // AsyncMessenger stuff
  /// approximately unique ID set by the Constructor for use in entity_addr_t
  uint64_t nonce;

  /**
   *  The following aren't lock-protected since you shouldn't be able to race
   *  the only writers.
   */

  int listen_sd;
  /**
   *  false; set to true if the AsyncMessenger bound to a specific address;
   *  and set false again by Accepter::stop().
   */
  bool did_bind;
  /// counter for the global seq our connection protocol uses
  __u32 global_seq;
  /// lock to protect the global_seq
  ceph_spinlock_t global_seq_lock;

  /**
   * hash map of addresses to Asyncconnection
   *
   * NOTE: a Asyncconnection* with state CLOSED may still be in the map but is considered
   * invalid and can be replaced by anyone holding the msgr lock
   */
  ceph::unordered_map<entity_addr_t, AsyncConnectionRef> conns;

  /**
   * list of connection are in teh process of accepting
   *
   * These are not yet in the conns map.
   */
  // FIXME clear up
  set<AsyncConnectionRef> accepting_conns;

  /// internal cluster protocol version, if any, for talking to entities of the same type.
  int cluster_protocol;

  Cond  stop_cond;
  bool stopped;

  AsyncConnectionRef _lookup_conn(const entity_addr_t& k) {
    assert(lock.is_locked());
    ceph::unordered_map<entity_addr_t, AsyncConnectionRef>::iterator p = conns.find(k);
    if (p == conns.end())
      return NULL;

    assert(p->second->is_connected());
    return p->second;
  }

  void _stop_conn(AsyncConnectionRef c) {
    assert(lock.is_locked());
    if (c) {
      c->mark_down();
      conns.erase(c->peer_addr);
    }
  }

  void _init_local_connection() {
    assert(lock.is_locked());
    local_connection->peer_addr = my_inst.addr;
    local_connection->peer_type = my_inst.name.type();
    ms_deliver_handle_fast_connect(local_connection.get());
  }


public:

  /// con used for sending messages to ourselves
  ConnectionRef local_connection;

  /**
   * @defgroup AsyncMessenger internals
   * @{
   */
  /**
   * This wraps _lookup_conn.
   */
  AsyncConnectionRef lookup_conn(const entity_addr_t& k) {
    Mutex::Locker l(lock);
    return _lookup_conn(k);
  }

  void accept_conn(AsyncConnectionRef conn) {
    Mutex::Locker l(lock);
    conns[conn->peer_addr] = conn;
    accepting_conns.erase(conn);
  }

  void learned_addr(const entity_addr_t &peer_addr_for_me);
  AsyncConnectionRef add_accept(int sd);

  /**
   * This wraps ms_deliver_get_authorizer. We use it for AsyncConnection.
   */
  AuthAuthorizer *get_authorizer(int peer_type, bool force_new) {
    return ms_deliver_get_authorizer(peer_type, force_new);
  }

  /**
   * This wraps ms_deliver_verify_authorizer; we use it for AsyncConnection.
   */
  bool verify_authorizer(Connection *con, int peer_type, int protocol, bufferlist& auth, bufferlist& auth_reply,
                         bool& isvalid, CryptoKey& session_key) {
    return ms_deliver_verify_authorizer(con, peer_type, protocol, auth,
                                        auth_reply, isvalid, session_key);
  }
  /**
   * Increment the global sequence for this AsyncMessenger and return it.
   * This is for the connect protocol, although it doesn't hurt if somebody
   * else calls it.
   *
   * @return a global sequence ID that nobody else has seen.
   */
  __u32 get_global_seq(__u32 old=0) {
    ceph_spin_lock(&global_seq_lock);
    if (old > global_seq)
      global_seq = old;
    __u32 ret = ++global_seq;
    ceph_spin_unlock(&global_seq_lock);
    return ret;
  }
  /**
   * Get the protocol version we support for the given peer type: either
   * a peer protocol (if it matches our own), the protocol version for the
   * peer (if we're connecting), or our protocol version (if we're accepting).
   */
  int get_proto_version(int peer_type, bool connect);

  /**
   * Fill in the address and peer type for the local connection, which
   * is used for delivering messages back to ourself.
   */
  void init_local_connection() {
    Mutex::Locker l(lock);
    _init_local_connection();
  }

  /**
   * Unregister connection from `conns`
   * `external` is used to indicate whether need to lock AsyncMessenger::lock,
   * it may call. If external is false, it means that AsyncConnection take the
   * initiative to unregister
   */
  void unregister_conn(const entity_addr_t &addr, bool external) {
    if (!external)
      lock.Lock();
    conns.erase(addr);
    if (!external)
      lock.Unlock();
  }
  /**
   * @} // AsyncMessenger Internals
   */
} ;

#endif /* CEPH_ASYNCMESSENGER_H */
