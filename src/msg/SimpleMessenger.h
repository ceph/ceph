// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_SIMPLEMESSENGER_H
#define CEPH_SIMPLEMESSENGER_H

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

#include "Messenger.h"
#include "Message.h"
#include "include/assert.h"
#include "DispatchQueue.h"

#include "Pipe.h"
#include "Accepter.h"
#include "include/Spinlock.h"

/*
 * This class handles transmission and reception of messages. Generally
 * speaking, there are several major components:
 *
 * - Connection
 *    Each logical session is associated with a Connection.
 * - Pipe
 *    Each network connection is handled through a pipe, which handles
 *    the input and output of each message.  There is normally a 1:1
 *    relationship between Pipe and Connection, but logical sessions may
 *    get handed off between Pipes when sockets reconnect or during
 *    connection races.
 * - IncomingQueue
 *    Incoming messages are associated with an IncomingQueue, and there
 *    is one such queue associated with each Pipe.
 * - DispatchQueue
 *    IncomingQueues get queued in the DIspatchQueue, which is responsible
 *    for doing a round-robin sweep and processing them via a worker thread.
 * - SimpleMessenger
 *    It's the exterior class passed to the external message handler and
 *    most of the API details.
 *
 * Lock ordering:
 *
 *   SimpleMessenger::lock
 *       Pipe::pipe_lock
 *           DispatchQueue::lock
 *               IncomingQueue::lock
 */

class SimpleMessenger : public Messenger {
  // First we have the public Messenger interface implementation...
public:
  /**
   * Initialize the SimpleMessenger!
   *
   * @param cct The CephContext to use
   * @param name The name to assign ourselves
   * _nonce A unique ID to use for this SimpleMessenger. It should not
   * be a value that will be repeated if the daemon restarts.
   */
  SimpleMessenger(CephContext *cct, entity_name_t name,
		  string mname, uint64_t _nonce);

  /**
   * Destroy the SimpleMessenger. Pretty simple since all the work is done
   * elsewhere.
   */
  virtual ~SimpleMessenger();

  /** @defgroup Accessors
   * @{
   */
  void set_addr_unknowns(entity_addr_t& addr);

  int get_dispatch_queue_len() {
    return dispatch_queue.get_queue_len();
  }

  double get_dispatch_queue_max_age(utime_t now) {
    return dispatch_queue.get_max_age(now);
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

  void set_default_policy(Policy p) {
    Mutex::Locker l(policy_lock);
    default_policy = p;
  }

  void set_policy(int type, Policy p) {
    Mutex::Locker l(policy_lock);
    policy_map[type] = p;
  }

  void set_policy_throttlers(int type, Throttle *byte_throttle, Throttle *msg_throttle) {
    Mutex::Locker l(policy_lock);
    if (policy_map.count(type)) {
      policy_map[type].throttler_bytes = byte_throttle;
      policy_map[type].throttler_messages = msg_throttle;
    } else {
      default_policy.throttler_bytes = byte_throttle;
      default_policy.throttler_messages = msg_throttle;
    }
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
    return _send_message(m, dest, false);
  }

  virtual int send_message(Message *m, Connection *con) {
    return _send_message(m, con, false);
  }

  virtual int lazy_send_message(Message *m, const entity_inst_t& dest) {
    return _send_message(m, dest, true);
  }

  virtual int lazy_send_message(Message *m, Connection *con) {
    return _send_message(m, con, true);
  }
  /** @} // Messaging */

  /**
   * @defgroup Connection Management
   * @{
   */
  virtual ConnectionRef get_connection(const entity_inst_t& dest);
  virtual ConnectionRef get_loopback_connection();
  virtual int send_keepalive(const entity_inst_t& addr);
  virtual int send_keepalive(Connection *con);
  virtual void mark_down(const entity_addr_t& addr);
  virtual void mark_down(Connection *con);
  virtual void mark_down_on_empty(Connection *con);
  virtual void mark_disposable(Connection *con);
  virtual void mark_down_all();
  /** @} // Connection Management */
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
   * @defgroup Inner classes
   * @{
   */

public:
  Accepter accepter;
  DispatchQueue dispatch_queue;

  friend class Accepter;

  /**
   * Register a new pipe for accept
   *
   * @param sd socket
   */
  Pipe *add_accept_pipe(int sd);

private:

  /**
   * A thread used to tear down Pipes when they're complete.
   */
  class ReaperThread : public Thread {
    SimpleMessenger *msgr;
  public:
    ReaperThread(SimpleMessenger *m) : msgr(m) {}
    void *entry() {
      msgr->reaper_entry();
      return 0;
    }
  } reaper_thread;

  /**
   * @} // Inner classes
   */

  /**
   * @defgroup Utility functions
   * @{
   */

  /**
   * Create a Pipe associated with the given entity (of the given type).
   * Initiate the connection. (This function returning does not guarantee
   * connection success.)
   *
   * @param addr The address of the entity to connect to.
   * @param type The peer type of the entity at the address.
   * @param con An existing Connection to associate with the new Pipe. If
   * NULL, it creates a new Connection.
   * @param msg an initial message to queue on the new pipe
   *
   * @return a pointer to the newly-created Pipe. Caller does not own a
   * reference; take one if you need it.
   */
  Pipe *connect_rank(const entity_addr_t& addr, int type, Connection *con, Message *first);
  /**
   * Send a message, lazily or not.
   * This just glues [lazy_]send_message together and passes
   * the input on to submit_message.
   */
  int _send_message(Message *m, const entity_inst_t& dest, bool lazy);
  /**
   * Same as above, but for the Connection-based variants.
   */
  int _send_message(Message *m, Connection *con, bool lazy);
  /**
   * Queue up a Message for delivery to the entity specified
   * by addr and dest_type.
   * submit_message() is responsible for creating
   * new Pipes (and closing old ones) as necessary.
   *
   * @param m The Message to queue up. This function eats a reference.
   * @param con The existing Connection to use, or NULL if you don't know of one.
   * @param addr The address to send the Message to.
   * @param dest_type The peer type of the address we're sending to
   * @param lazy If true, do not establish or fix a Connection to send the Message;
   * just drop silently under failure.
   */
  void submit_message(Message *m, Connection *con,
                      const entity_addr_t& addr, int dest_type, bool lazy);
  /**
   * Look through the pipes in the pipe_reap_queue and tear them down.
   */
  void reaper();
  /**
   * @} // Utility functions
   */

  // SimpleMessenger stuff
  /// the peer type of our endpoint
  int my_type;
  /// approximately unique ID set by the Constructor for use in entity_addr_t
  uint64_t nonce;
  /// overall lock used for SimpleMessenger data structures
  Mutex lock;
  /// true, specifying we haven't learned our addr; set false when we find it.
  // maybe this should be protected by the lock?
  bool need_addr;

public:
  bool get_need_addr() const { return need_addr; }

private:
  /**
   *  false; set to true if the SimpleMessenger bound to a specific address;
   *  and set false again by Accepter::stop(). This isn't lock-protected
   *  since you shouldn't be able to race the only writers.
   */
  bool did_bind;
  /// counter for the global seq our connection protocol uses
  __u32 global_seq;
  /// lock to protect the global_seq
  ceph_spinlock_t global_seq_lock;

  /**
   * hash map of addresses to Pipes
   *
   * NOTE: a Pipe* with state CLOSED may still be in the map but is considered
   * invalid and can be replaced by anyone holding the msgr lock
   */
  ceph::unordered_map<entity_addr_t, Pipe*> rank_pipe;
  /**
   * list of pipes are in teh process of accepting
   *
   * These are not yet in the rank_pipe map.
   */
  set<Pipe*> accepting_pipes;
  /// a set of all the Pipes we have which are somehow active
  set<Pipe*>      pipes;
  /// a list of Pipes we want to tear down
  list<Pipe*>     pipe_reap_queue;

  /// internal cluster protocol version, if any, for talking to entities of the same type.
  int cluster_protocol;

  /// lock protecting policy
  Mutex policy_lock;
  /// the default Policy we use for Pipes
  Policy default_policy;
  /// map specifying different Policies for specific peer types
  map<int, Policy> policy_map; // entity_name_t::type -> Policy

  /// Throttle preventing us from building up a big backlog waiting for dispatch
  Throttle dispatch_throttler;

  bool reaper_started, reaper_stop;
  Cond reaper_cond;

  /// This Cond is slept on by wait() and signaled by dispatch_entry()
  Cond  wait_cond;

  friend class Pipe;

  Pipe *_lookup_pipe(const entity_addr_t& k) {
    ceph::unordered_map<entity_addr_t, Pipe*>::iterator p = rank_pipe.find(k);
    if (p == rank_pipe.end())
      return NULL;
    // see lock cribbing in Pipe::fault()
    if (p->second->state_closed.read())
      return NULL;
    return p->second;
  }

public:

  int timeout;

  /// con used for sending messages to ourselves
  ConnectionRef local_connection;

  /**
   * @defgroup SimpleMessenger internals
   * @{
   */

  /**
   * This wraps ms_deliver_get_authorizer. We use it for Pipe.
   */
  AuthAuthorizer *get_authorizer(int peer_type, bool force_new);
  /**
   * This wraps ms_deliver_verify_authorizer; we use it for Pipe.
   */
  bool verify_authorizer(Connection *con, int peer_type, int protocol, bufferlist& auth, bufferlist& auth_reply,
                         bool& isvalid,CryptoKey& session_key);
  /**
   * Increment the global sequence for this SimpleMessenger and return it.
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
  void init_local_connection();
  /**
   * Tell the SimpleMessenger its full IP address.
   *
   * This is used by Pipes when connecting to other endpoints, and
   * probably shouldn't be called by anybody else.
   */
  void learned_addr(const entity_addr_t& peer_addr_for_me);

  /**
   * Tell the SimpleMessenger its address is no longer known
   *
   * This happens when we rebind to a new port.
   */
  void unlearn_addr();

  /**
   * Get the Policy associated with a type of peer.
   * @param t The peer type to get the default policy for.
   *
   * @return A const Policy reference.
   */
  Policy get_policy(int t) {
    Mutex::Locker l(policy_lock);
    if (policy_map.count(t))
      return policy_map[t];
    else
      return default_policy;
  }
  Policy get_default_policy() {
    Mutex::Locker l(policy_lock);
    return default_policy;
  }

  /**
   * Release memory accounting back to the dispatch throttler.
   *
   * @param msize The amount of memory to release.
   */
  void dispatch_throttle_release(uint64_t msize);

  /**
   * This function is used by the reaper thread. As long as nobody
   * has set reaper_stop, it calls the reaper function, then
   * waits to be signaled when it needs to reap again (or when it needs
   * to stop).
   */
  void reaper_entry();
  /**
   * Add a pipe to the pipe_reap_queue, to be torn down on
   * the next call to reaper().
   * It should really only be the Pipe calling this, in our current
   * implementation.
   *
   * @param pipe A Pipe which has stopped its threads and is
   * ready to be torn down.
   */
  void queue_reap(Pipe *pipe);
  /**
   * @} // SimpleMessenger Internals
   */
} ;

#endif
