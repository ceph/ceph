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



#ifndef CEPH_MESSENGER_H
#define CEPH_MESSENGER_H

#include <map>
using namespace std;

#include "Message.h"
#include "Dispatcher.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/Context.h"
#include "include/types.h"
#include "include/ceph_features.h"
#include "auth/Crypto.h"

#include <errno.h>
#include <sstream>

#define SOCKET_PRIORITY_MIN_DELAY 6

class Timer;


class Messenger {
private:
  list<Dispatcher*> dispatchers;
  list <Dispatcher*> fast_dispatchers;

protected:
  /// the "name" of the local daemon. eg client.99
  entity_inst_t my_inst;
  int default_send_priority;
  /// set to true once the Messenger has started, and set to false on shutdown
  bool started;
  uint32_t magic;
  int socket_priority;

public:
  /**
   *  The CephContext this Messenger uses. Many other components initialize themselves
   *  from this value.
   */
  CephContext *cct;
  int crcflags;

  /**
   * A Policy describes the rules of a Connection. Is there a limit on how
   * much data this Connection can have locally? When the underlying connection
   * experiences an error, does the Connection disappear? Can this Messenger
   * re-establish the underlying connection?
   */
  struct Policy {
    /// If true, the Connection is tossed out on errors.
    bool lossy;
    /// If true, the underlying connection can't be re-established from this end.
    bool server;
    /// If true, we will standby when idle
    bool standby;
    /// If true, we will try to detect session resets
    bool resetcheck;
    /**
     *  The throttler is used to limit how much data is held by Messages from
     *  the associated Connection(s). When reading in a new Message, the Messenger
     *  will call throttler->throttle() for the size of the new Message.
     */
    Throttle *throttler_bytes;
    Throttle *throttler_messages;

    /// Specify features supported locally by the endpoint.
    uint64_t features_supported;
    /// Specify features any remotes must have to talk to this endpoint.
    uint64_t features_required;

    Policy()
      : lossy(false), server(false), standby(false), resetcheck(true),
	throttler_bytes(NULL),
	throttler_messages(NULL),
	features_supported(CEPH_FEATURES_SUPPORTED_DEFAULT),
	features_required(0) {}
  private:
    Policy(bool l, bool s, bool st, bool r, uint64_t sup, uint64_t req)
      : lossy(l), server(s), standby(st), resetcheck(r),
	throttler_bytes(NULL),
	throttler_messages(NULL),
	features_supported(sup | CEPH_FEATURES_SUPPORTED_DEFAULT),
	features_required(req) {}

  public:
    static Policy stateful_server(uint64_t sup, uint64_t req) {
      return Policy(false, true, true, true, sup, req);
    }
    static Policy stateless_server(uint64_t sup, uint64_t req) {
      return Policy(true, true, false, false, sup, req);
    }
    static Policy lossless_peer(uint64_t sup, uint64_t req) {
      return Policy(false, false, true, false, sup, req);
    }
    static Policy lossless_peer_reuse(uint64_t sup, uint64_t req) {
      return Policy(false, false, true, true, sup, req);
    }
    static Policy lossy_client(uint64_t sup, uint64_t req) {
      return Policy(true, false, false, false, sup, req);
    }
    static Policy lossless_client(uint64_t sup, uint64_t req) {
      return Policy(false, false, false, true, sup, req);
    }
  };

  /**
   * Messenger constructor. Call this from your implementation.
   * Messenger users should construct full implementations directly,
   * or use the create() function.
   */
  Messenger(CephContext *cct_, entity_name_t w)
    : my_inst(),
      default_send_priority(CEPH_MSG_PRIO_DEFAULT), started(false),
      magic(0),
      socket_priority(-1),
      cct(cct_),
      crcflags(get_default_crc_flags(cct->_conf))
  {
    my_inst.name = w;
  }
  virtual ~Messenger() {}

  /**
   * create a new messenger
   *
   * Create a new messenger instance, with whatever implementation is
   * available or specified via the configuration in cct.
   *
   * @param cct context
   * @param type name of messenger type
   * @param name entity name to register
   * @param lname logical name of the messenger in this process (e.g., "client")
   * @param nonce nonce value to uniquely identify this instance on the current host
   * @param features bits for the local connection
   */
  static Messenger *create(CephContext *cct,
                           const string &type,
                           entity_name_t name,
			   string lname,
                           uint64_t nonce,
			   uint64_t features = 0);

  /**
   * create a new messenger
   *
   * Create a new messenger instance.
   * Same as the above, but a slightly simpler interface for clients:
   * - Generate a random nonce
   * - use the default feature bits
   * - get the messenger type from cct
   * - use the client entity_type
   *
   * @param cct context
   * @param lname logical name of the messenger in this process (e.g., "client")
   */
  static Messenger *create_client_messenger(CephContext *cct, string lname);

  /**
   * @defgroup Accessors
   * @{
   */
  /**
   * Retrieve the Messenger's instance.
   *
   * @return A const reference to the instance this Messenger
   * currently believes to be its own.
   */
  const entity_inst_t& get_myinst() { return my_inst; }
  /**
   * set messenger's instance
   */
  void set_myinst(entity_inst_t i) { my_inst = i; }

  uint32_t get_magic() { return magic; }
  void set_magic(int _magic) { magic = _magic; }

  /**
   * Retrieve the Messenger's address.
   *
   * @return A const reference to the address this Messenger
   * currently believes to be its own.
   */
  const entity_addr_t& get_myaddr() { return my_inst.addr; }
protected:
  /**
   * set messenger's address
   */
  virtual void set_myaddr(const entity_addr_t& a) { my_inst.addr = a; }
public:
  /**
   * Retrieve the Messenger's name.
   *
   * @return A const reference to the name this Messenger
   * currently believes to be its own.
   */
  const entity_name_t& get_myname() { return my_inst.name; }
  /**
   * Set the name of the local entity. The name is reported to others and
   * can be changed while the system is running, but doing so at incorrect
   * times may have bad results.
   *
   * @param m The name to set.
   */
  void set_myname(const entity_name_t& m) { my_inst.name = m; }
  /**
   * Set the unknown address components for this Messenger.
   * This is useful if the Messenger doesn't know its full address just by
   * binding, but another Messenger on the same interface has already learned
   * its full address. This function does not fill in known address elements,
   * cause a rebind, or do anything of that sort.
   *
   * @param addr The address to use as a template.
   */
  virtual void set_addr_unknowns(entity_addr_t &addr) = 0;
  /// Get the default send priority.
  int get_default_send_priority() { return default_send_priority; }
  /**
   * Get the number of Messages which the Messenger has received
   * but not yet dispatched.
   */
  virtual int get_dispatch_queue_len() = 0;

  /**
   * Get age of oldest undelivered message
   * (0 if the queue is empty)
   */
  virtual double get_dispatch_queue_max_age(utime_t now) = 0;
  /**
   * Get the default crc flags for this messenger.
   * but not yet dispatched.
   */
  static int get_default_crc_flags(md_config_t *);

  /**
   * @} // Accessors
   */

  /**
   * @defgroup Configuration
   * @{
   */
  /**
   * Set the cluster protocol in use by this daemon.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param p The cluster protocol to use. Defined externally.
   */
  virtual void set_cluster_protocol(int p) = 0;
  /**
   * Set a policy which is applied to all peers who do not have a type-specific
   * Policy.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param p The Policy to apply.
   */
  virtual void set_default_policy(Policy p) = 0;
  /**
   * Set a policy which is applied to all peers of the given type.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param type The peer type this policy applies to.
   * @param p The policy to apply.
   */
  virtual void set_policy(int type, Policy p) = 0;
  /**
   * Set the Policy associated with a type of peer.
   *
   * This can be called either on initial setup, or after connections
   * are already established.  However, the policies for existing
   * connections will not be affected; the new policy will only apply
   * to future connections.
   *
   * @param t The peer type to get the default policy for.
   * @return A const Policy reference.
   */
  virtual Policy get_policy(int t) = 0;
  /**
   * Get the default Policy
   *
   * @return A const Policy reference.
   */
  virtual Policy get_default_policy() = 0;
  /**
   * Set Throttlers applied to all Messages from the given type of peer
   *
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param type The peer type the Throttlers will apply to.
   * @param bytes The Throttle for the number of bytes carried by the message
   * @param msgs The Throttle for the number of messages for this @p type
   * @note The Messenger does not take ownership of the Throttle pointers, but
   * you must not destroy them before you destroy the Messenger.
   */
  virtual void set_policy_throttlers(int type, Throttle *bytes, Throttle *msgs=NULL) = 0;
  /**
   * Set the default send priority
   *
   * This is an init-time function and must be called *before* calling
   * start().
   *
   * @param p The cluster protocol to use. Defined externally.
   */
  void set_default_send_priority(int p) {
    assert(!started);
    default_send_priority = p;
  }
  /**
   * Set the priority(SO_PRIORITY) for all packets to be sent on this socket.
   *
   * Linux uses this value to order the networking queues: packets with a higher
   * priority may be processed first depending on the selected device queueing
   * discipline.
   *
   * @param prio The priority. Setting a priority outside the range 0 to 6
   * requires the CAP_NET_ADMIN capability.
   */
  void set_socket_priority(int prio) {
    socket_priority = prio;
  }
  /**
   * Get the socket priority
   *
   * @return the socket priority
   */
  int get_socket_priority() {
    return socket_priority;
  }
  /**
   * Add a new Dispatcher to the front of the list. If you add
   * a Dispatcher which is already included, it will get a duplicate
   * entry. This will reduce efficiency but not break anything.
   *
   * @param d The Dispatcher to insert into the list.
   */
  void add_dispatcher_head(Dispatcher *d) { 
    bool first = dispatchers.empty();
    dispatchers.push_front(d);
    if (d->ms_can_fast_dispatch_any())
      fast_dispatchers.push_front(d);
    if (first)
      ready();
  }
  /**
   * Add a new Dispatcher to the end of the list. If you add
   * a Dispatcher which is already included, it will get a duplicate
   * entry. This will reduce efficiency but not break anything.
   *
   * @param d The Dispatcher to insert into the list.
   */
  void add_dispatcher_tail(Dispatcher *d) { 
    bool first = dispatchers.empty();
    dispatchers.push_back(d);
    if (d->ms_can_fast_dispatch_any())
      fast_dispatchers.push_back(d);
    if (first)
      ready();
  }
  /**
   * Bind the Messenger to a specific address. If bind_addr
   * is not completely filled in the system will use the
   * valid portions and cycle through the unset ones (eg, the port)
   * in an unspecified order.
   *
   * @param bind_addr The address to bind to.
   * @return 0 on success, or -1 on error, or -errno if
   * we can be more specific about the failure.
   */
  virtual int bind(const entity_addr_t& bind_addr) = 0;
  /**
   * This function performs a full restart of the Messenger component,
   * whatever that means.  Other entities who connect to this
   * Messenger post-rebind() should perceive it as a new entity which
   * they have not previously contacted, and it MUST bind to a
   * different address than it did previously.
   *
   * @param avoid_ports Additional port to avoid binding to.
   */
  virtual int rebind(const set<int>& avoid_ports) { return -EOPNOTSUPP; }
  /**
   * @return 1 on unlinked, or 0 on linked, or -errno if
   * we can be more specific about the failure.
   */
  virtual int is_linked() { return -EOPNOTSUPP; }
  /**
   * @} // Configuration
   */

  /**
   * @defgroup Startup/Shutdown
   * @{
   */
  /**
   * Perform any resource allocation, thread startup, etc
   * that is required before attempting to connect to other
   * Messengers or transmit messages.
   * Once this function completes, started shall be set to true.
   *
   * @return 0 on success; -errno on failure.
   */
  virtual int start() { started = true; return 0; }

  // shutdown
  /**
   * Block until the Messenger has finished shutting down (according
   * to the shutdown() function).
   * It is valid to call this after calling shutdown(), but it must
   * be called before deleting the Messenger.
   */
  virtual void wait() = 0;
  /**
   * Initiate a shutdown of the Messenger.
   *
   * @return 0 on success, -errno otherwise.
   */
  virtual int shutdown() { started = false; return 0; }
  /**
   * @} // Startup/Shutdown
   */

  /**
   * @defgroup Messaging
   * @{
   */
  /**
   * Queue the given Message for the given entity.
   * Success in this function does not guarantee Message delivery, only
   * success in queueing the Message. Other guarantees may be provided based
   * on the Connection policy associated with the dest.
   *
   * @param m The Message to send. The Messenger consumes a single reference
   * when you pass it in.
   * @param dest The entity to send the Message to.
   *
   * DEPRECATED: please do not use this interface for any new code;
   * use the Connection* variant.
   *
   * @return 0 on success, or -errno on failure.
   */
  virtual int send_message(Message *m, const entity_inst_t& dest) = 0;

  /**
   * @} // Messaging
   */
  /**
   * @defgroup Connection Management
   * @{
   */
  /**
   * Get the Connection object associated with a given entity. If a
   * Connection does not exist, create one and establish a logical connection.
   * The caller owns a reference when this returns. Call ->put() when you're
   * done!
   *
   * @param dest The entity to get a connection for.
   */
  virtual ConnectionRef get_connection(const entity_inst_t& dest) = 0;
  /**
   * Get the Connection object associated with ourselves.
   */
  virtual ConnectionRef get_loopback_connection() = 0;
  /**
   * Mark down a Connection to a remote.
   *
   * This will cause us to discard our outgoing queue for them, and if
   * reset detection is enabled in the policy and the endpoint tries
   * to reconnect they will discard their queue when we inform them of
   * the session reset.
   *
   * If there is no Connection to the given dest, it is a no-op.
   *
   * This generates a RESET notification to the Dispatcher.
   *
   * DEPRECATED: please do not use this interface for any new code;
   * use the Connection* variant.
   *
   * @param a The address to mark down.
   */
  virtual void mark_down(const entity_addr_t& a) = 0;
  /**
   * Mark all the existing Connections down. This is equivalent
   * to iterating over all Connections and calling mark_down()
   * on each.
   *
   * This will generate a RESET event for each closed connections.
   */
  virtual void mark_down_all() = 0;
  /**
   * @} // Connection Management
   */
protected:
  /**
   * @defgroup Subclass Interfacing
   * @{
   */
  /**
   * A courtesy function for Messenger implementations which
   * will be called when we receive our first Dispatcher.
   */
  virtual void ready() { }
  /**
   * @} // Subclass Interfacing
   */
  /**
   * @defgroup Dispatcher Interfacing
   * @{
   */
public:
  /**
   * Determine whether a message can be fast-dispatched. We will
   * query each Dispatcher in sequence to determine if they are
   * capable of handling a particular message via "fast dispatch".
   *
   * @param m The Message we are testing.
   */
  bool ms_can_fast_dispatch(Message *m) {
    for (list<Dispatcher*>::iterator p = fast_dispatchers.begin();
	 p != fast_dispatchers.end();
	 ++p) {
      if ((*p)->ms_can_fast_dispatch(m))
	return true;
    }
    return false;
  }

  /**
   * Deliver a single Message via "fast dispatch".
   *
   * @param m The Message we are fast dispatching. We take ownership
   * of one reference to it.
   */
  void ms_fast_dispatch(Message *m) {
    m->set_dispatch_stamp(ceph_clock_now(cct));
    for (list<Dispatcher*>::iterator p = fast_dispatchers.begin();
	 p != fast_dispatchers.end();
	 ++p) {
      if ((*p)->ms_can_fast_dispatch(m)) {
	(*p)->ms_fast_dispatch(m);
	return;
      }
    }
    assert(0);
  }
  /**
   *
   */
  void ms_fast_preprocess(Message *m) {
    for (list<Dispatcher*>::iterator p = fast_dispatchers.begin();
	 p != fast_dispatchers.end();
	 ++p) {
      (*p)->ms_fast_preprocess(m);
    }
  }
  /**
   *  Deliver a single Message. Send it to each Dispatcher
   *  in sequence until one of them handles it.
   *  If none of our Dispatchers can handle it, assert(0).
   *
   *  @param m The Message to deliver. We take ownership of
   *  one reference to it.
   */
  void ms_deliver_dispatch(Message *m) {
    m->set_dispatch_stamp(ceph_clock_now(cct));
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 ++p) {
      if ((*p)->ms_dispatch(m))
	return;
    }
    lsubdout(cct, ms, 0) << "ms_deliver_dispatch: unhandled message " << m << " " << *m << " from "
			 << m->get_source_inst() << dendl;
    assert(!cct->_conf->ms_die_on_unhandled_msg);
    m->put();
  }
  /**
   * Notify each Dispatcher of a new Connection. Call
   * this function whenever a new Connection is initiated or
   * reconnects.
   *
   * @param con Pointer to the new Connection.
   */
  void ms_deliver_handle_connect(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 ++p)
      (*p)->ms_handle_connect(con);
  }

  /**
   * Notify each fast Dispatcher of a new Connection. Call
   * this function whenever a new Connection is initiated or
   * reconnects.
   *
   * @param con Pointer to the new Connection.
   */
  void ms_deliver_handle_fast_connect(Connection *con) {
    for (list<Dispatcher*>::iterator p = fast_dispatchers.begin();
         p != fast_dispatchers.end();
         ++p)
      (*p)->ms_handle_fast_connect(con);
  }

  /**
   * Notify each Dispatcher of a new incomming Connection. Call
   * this function whenever a new Connection is accepted.
   *
   * @param con Pointer to the new Connection.
   */
  void ms_deliver_handle_accept(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 ++p)
      (*p)->ms_handle_accept(con);
  }

  /**
   * Notify each fast Dispatcher of a new incoming Connection. Call
   * this function whenever a new Connection is accepted.
   *
   * @param con Pointer to the new Connection.
   */
  void ms_deliver_handle_fast_accept(Connection *con) {
    for (list<Dispatcher*>::iterator p = fast_dispatchers.begin();
         p != fast_dispatchers.end();
         ++p)
      (*p)->ms_handle_fast_accept(con);
  }

  /**
   * Notify each Dispatcher of a Connection which may have lost
   * Messages. Call this function whenever you detect that a lossy Connection
   * has been disconnected.
   *
   * @param con Pointer to the broken Connection.
   */
  void ms_deliver_handle_reset(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 ++p) {
      if ((*p)->ms_handle_reset(con))
	return;
    }
  }
  /**
   * Notify each Dispatcher of a Connection which has been "forgotten" about
   * by the remote end, implying that messages have probably been lost.
   * Call this function whenever you detect a reset.
   *
   * @param con Pointer to the broken Connection.
   */
  void ms_deliver_handle_remote_reset(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 ++p)
      (*p)->ms_handle_remote_reset(con);
  }
  /**
   * Get the AuthAuthorizer for a new outgoing Connection.
   *
   * @param peer_type The peer type for the new Connection
   * @param force_new True if we want to wait for new keys, false otherwise.
   * @return A pointer to the AuthAuthorizer, if we have one; NULL otherwise
   */
  AuthAuthorizer *ms_deliver_get_authorizer(int peer_type, bool force_new) {
    AuthAuthorizer *a = 0;
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 ++p) {
      if ((*p)->ms_get_authorizer(peer_type, &a, force_new))
	return a;
    }
    return NULL;
  }
  /**
   * Verify that the authorizer on a new incoming Connection is correct.
   *
   * @param con The new incoming Connection
   * @param peer_type The type of the endpoint on the new Connection
   * @param protocol The ID of the protocol in use (at time of writing, cephx or none)
   * @param authorizer The authorization string supplied by the remote
   * @param authorizer_reply Output param: The string we should send back to
   * the remote to authorize ourselves. Only filled in if isvalid
   * @param isvalid Output param: True if authorizer is valid, false otherwise
   *
   * @return True if we were able to prove or disprove correctness of
   * authorizer, false otherwise.
   */
  bool ms_deliver_verify_authorizer(Connection *con, int peer_type,
				    int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
				    bool& isvalid, CryptoKey& session_key) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 ++p) {
      if ((*p)->ms_verify_authorizer(con, peer_type, protocol, authorizer, authorizer_reply, isvalid, session_key))
	return true;
    }
    return false;
  }

  /**
   * @} // Dispatcher Interfacing
   */
};



#endif
