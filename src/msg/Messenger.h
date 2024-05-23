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
#include <optional>
#include <vector>

#include <errno.h>
#include <sstream>
#include <memory>

#include "Message.h"
#include "Dispatcher.h"
#include "Policy.h"
#include "common/Throttle.h"
#include "include/Context.h"
#include "include/types.h"
#include "include/ceph_features.h"
#include "auth/Crypto.h"
#include "common/item_history.h"
#include "auth/AuthRegistry.h"
#include "compressor_registry.h"
#include "include/ceph_assert.h"

#include <errno.h>
#include <sstream>
#include <signal.h>

#define SOCKET_PRIORITY_MIN_DELAY 6

class Timer;

class AuthClient;
class AuthServer;

#ifdef UNIT_TESTS_BUILT

struct Interceptor {
  std::mutex lock;
  std::condition_variable cond_var;

  enum ACTION : uint32_t {
    CONTINUE = 0,
    FAIL,
    STOP
  };

  enum STEP {
    START_CLIENT_BANNER_EXCHANGE = 1,
    START_SERVER_BANNER_EXCHANGE,
    BANNER_EXCHANGE_BANNER_CONNECTING,
    BANNER_EXCHANGE,
    HANDLE_PEER_BANNER_BANNER_CONNECTING,
    HANDLE_PEER_BANNER,
    HANDLE_PEER_BANNER_PAYLOAD_HELLO_CONNECTING,
    HANDLE_PEER_BANNER_PAYLOAD,
    SEND_AUTH_REQUEST,
    HANDLE_AUTH_REQUEST_ACCEPTING_SIGN,
    SEND_CLIENT_IDENTITY,
    SEND_SERVER_IDENTITY,
    SEND_RECONNECT,
    SEND_RECONNECT_OK,
    READY,
    HANDLE_MESSAGE,
    READ_MESSAGE_COMPLETE,
    SESSION_RETRY,
    SEND_COMPRESSION_REQUEST,
    HANDLE_COMPRESSION_REQUEST
  };

  virtual ~Interceptor() {}
  virtual ACTION intercept(Connection *conn, uint32_t step) = 0;
};

#endif

class Messenger {
private:
  struct PriorityDispatcher {
    using priority_t = Dispatcher::priority_t;
    priority_t priority;
    Dispatcher* dispatcher;

    bool operator<(const PriorityDispatcher& other) const {
      return priority < other.priority;
    }
  };
  std::vector<PriorityDispatcher> dispatchers;
  std::vector<PriorityDispatcher> fast_dispatchers;

  ZTracer::Endpoint trace_endpoint;

protected:
  void set_endpoint_addr(const entity_addr_t& a,
                         const entity_name_t &name);

protected:
  /// the "name" of the local daemon. eg client.99
  entity_name_t my_name;

  /// my addr
  safe_item_history<entity_addrvec_t> my_addrs;

  int default_send_priority;
  /// std::set to true once the Messenger has started, and std::set to false on shutdown
  bool started;
  uint32_t magic;
  int socket_priority;

public:
  AuthClient *auth_client = 0;
  AuthServer *auth_server = 0;

#ifdef UNIT_TESTS_BUILT
  Interceptor *interceptor = nullptr;
#endif

  /**
   *  The CephContext this Messenger uses. Many other components initialize themselves
   *  from this value.
   */
  CephContext *cct;
  int crcflags;

  using Policy = ceph::net::Policy<Throttle>;

public:
  // allow unauthenticated connections.  This is needed for
  // compatibility with pre-nautilus OSDs, which do not authenticate
  // the heartbeat sessions.
  bool require_authorizer = true;

protected:
  // for authentication
  AuthRegistry auth_registry;

public:
  /**
   * Messenger constructor. Call this from your implementation.
   * Messenger users should construct full implementations directly,
   * or use the create() function.
   */
  Messenger(CephContext *cct_, entity_name_t w);
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
   */
  static Messenger *create(CephContext *cct,
                           const std::string &type,
                           entity_name_t name,
			   std::string lname,
                           uint64_t nonce);

  static uint64_t get_random_nonce();

  /**
   * create a new messenger
   *
   * Create a new messenger instance.
   * Same as the above, but a slightly simpler interface for clients:
   * - Generate a random nonce
   * - get the messenger type from cct
   * - use the client entity_type
   *
   * @param cct context
   * @param lname logical name of the messenger in this process (e.g., "client")
   */
  static Messenger *create_client_messenger(CephContext *cct, std::string lname);

  /**
   * @defgroup Accessors
   * @{
   */
  int get_mytype() const { return my_name.type(); }

  /**
   * Retrieve the Messenger's name
   *
   * @return A const reference to the name this Messenger
   * currently believes to be its own.
   */
  const entity_name_t& get_myname() { return my_name; }

  /**
   * Retrieve the Messenger's address.
   *
   * @return A const reference to the address this Messenger
   * currently believes to be its own.
   */
  const entity_addrvec_t& get_myaddrs() {
    return *my_addrs;
  }

  /**
   * get legacy addr for myself, suitable for protocol v1
   *
   * Note that myaddrs might be a proper addrvec with v1 in it, or it might be an
   * ANY addr (if i am a pure client).
   */
  entity_addr_t get_myaddr_legacy() {
    return my_addrs->as_legacy_addr();
  }


  /**
   * std::set messenger's instance
   */
  uint32_t get_magic() { return magic; }
  void set_magic(int _magic) { magic = _magic; }

  void set_auth_client(AuthClient *ac) {
    auth_client = ac;
  }
  void set_auth_server(AuthServer *as) {
    auth_server = as;
  }

  // for compression
  CompressorRegistry comp_registry;

protected:
  /**
   * std::set messenger's address
   */
  virtual void set_myaddrs(const entity_addrvec_t& a) {
    my_addrs = a;
    set_endpoint_addr(a.front(), my_name);
  }
public:
  /**
   * @return the zipkin trace endpoint
   */
  const ZTracer::Endpoint* get_trace_endpoint() const {
    return &trace_endpoint;
  }

  /**
   * set the name of the local entity. The name is reported to others and
   * can be changed while the system is running, but doing so at incorrect
   * times may have bad results.
   *
   * @param m The name to std::set.
   */
  void set_myname(const entity_name_t& m) { my_name = m; }

  /**
   * set the unknown address components for this Messenger.
   * This is useful if the Messenger doesn't know its full address just by
   * binding, but another Messenger on the same interface has already learned
   * its full address. This function does not fill in known address elements,
   * cause a rebind, or do anything of that sort.
   *
   * @param addr The address to use as a template.
   */
  virtual bool set_addr_unknowns(const entity_addrvec_t &addrs) = 0;

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
   * @} // Accessors
   */

  /**
   * @defgroup Configuration
   * @{
   */
  /**
   * set the cluster protocol in use by this daemon.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param p The cluster protocol to use. Defined externally.
   */
  virtual void set_cluster_protocol(int p) = 0;
  /**
   * set a policy which is applied to all peers who do not have a type-specific
   * Policy.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param p The Policy to apply.
   */
  virtual void set_default_policy(Policy p) = 0;
  /**
   * set a policy which is applied to all peers of the given type.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param type The peer type this policy applies to.
   * @param p The policy to apply.
   */
  virtual void set_policy(int type, Policy p) = 0;
  /**
   * set the Policy associated with a type of peer.
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
   * set Throttlers applied to all Messages from the given type of peer
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
   * set the default send priority
   *
   * This is an init-time function and must be called *before* calling
   * start().
   *
   * @param p The cluster protocol to use. Defined externally.
   */
  void set_default_send_priority(int p) {
    ceph_assert(!started);
    default_send_priority = p;
  }
  /**
   * set the priority(SO_PRIORITY) for all packets to be sent on this socket.
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
  void add_dispatcher_head(Dispatcher *d, PriorityDispatcher::priority_t priority=Dispatcher::PRIORITY_DEFAULT) {
    bool first = dispatchers.empty();
    dispatchers.insert(dispatchers.begin(), PriorityDispatcher{priority, d});
    std::stable_sort(dispatchers.begin(), dispatchers.end());
    if (d->ms_can_fast_dispatch_any()) {
      fast_dispatchers.insert(fast_dispatchers.begin(), PriorityDispatcher{priority, d});
      std::stable_sort(fast_dispatchers.begin(), fast_dispatchers.end());
    }
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
  void add_dispatcher_tail(Dispatcher *d, PriorityDispatcher::priority_t priority=Dispatcher::PRIORITY_DEFAULT) {
    bool first = dispatchers.empty();
    dispatchers.push_back(PriorityDispatcher{priority, d});
    std::stable_sort(dispatchers.begin(), dispatchers.end());
    if (d->ms_can_fast_dispatch_any()) {
      fast_dispatchers.push_back(PriorityDispatcher{priority, d});
      std::stable_sort(fast_dispatchers.begin(), fast_dispatchers.end());
    }
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
   * @patam public_addrs The addresses to announce over the network
   * @return 0 on success, or -1 on error, or -errno if
   * we can be more specific about the failure.
   */
  virtual int bind(const entity_addr_t& bind_addr,
		   std::optional<entity_addrvec_t> public_addrs=std::nullopt) = 0;

  virtual int bindv(const entity_addrvec_t& bind_addrs,
                    std::optional<entity_addrvec_t> public_addrs=std::nullopt);

  /**
   * This function performs a full restart of the Messenger component,
   * whatever that means.  Other entities who connect to this
   * Messenger post-rebind() should perceive it as a new entity which
   * they have not previously contacted, and it MUST bind to a
   * different address than it did previously.
   *
   * @param avoid_ports Additional port to avoid binding to.
   */
  virtual int rebind(const std::set<int>& avoid_ports) { return -EOPNOTSUPP; }
  /**
   * Bind the 'client' Messenger to a specific address.Messenger will bind
   * the address before connect to others when option ms_bind_before_connect
   * is true.
   * @param bind_addr The address to bind to.
   * @return 0 on success, or -1 on error, or -errno if
   * we can be more specific about the failure.
   */
  virtual int client_bind(const entity_addr_t& bind_addr) = 0;

  /**
   * reset the 'client' Messenger. Mark all the existing Connections down
   * and update 'nonce'.
   */
  virtual int client_reset() = 0;


  virtual bool should_use_msgr2() {
    return false;
  }

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
  virtual int send_to(
    Message *m,
    int type,
    const entity_addrvec_t& addr) = 0;
  int send_to_mon(
    Message *m, const entity_addrvec_t& addrs) {
    return send_to(m, CEPH_ENTITY_TYPE_MON, addrs);
  }
  int send_to_mds(
    Message *m, const entity_addrvec_t& addrs) {
    return send_to(m, CEPH_ENTITY_TYPE_MDS, addrs);
  }

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
  virtual ConnectionRef connect_to(
    int type, const entity_addrvec_t& dest,
    bool anon=false, bool not_local_dest=false) = 0;
  ConnectionRef connect_to_mon(const entity_addrvec_t& dest,
      bool anon=false, bool not_local_dest=false) {
	return connect_to(CEPH_ENTITY_TYPE_MON, dest, anon, not_local_dest);
  }
  ConnectionRef connect_to_mds(const entity_addrvec_t& dest,
      bool anon=false, bool not_local_dest=false) {
	return connect_to(CEPH_ENTITY_TYPE_MDS, dest, anon, not_local_dest);
  }
  ConnectionRef connect_to_osd(const entity_addrvec_t& dest,
      bool anon=false, bool not_local_dest=false) {
	return connect_to(CEPH_ENTITY_TYPE_OSD, dest, anon, not_local_dest);
  }
  ConnectionRef connect_to_mgr(const entity_addrvec_t& dest,
      bool anon=false, bool not_local_dest=false) {
	return connect_to(CEPH_ENTITY_TYPE_MGR, dest, anon, not_local_dest);
  }

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
  virtual void mark_down_addrs(const entity_addrvec_t& a) {
    mark_down(a.legacy_addr());
  }
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
public:
#ifdef CEPH_USE_SIGPIPE_BLOCKER
  /**
   * We need to disable SIGPIPE on all platforms, and if they
   * don't give us a better mechanism (read: are on Solaris) that
   * means blocking the signal whenever we do a send or sendmsg...
   * That means any implementations must invoke MSGR_SIGPIPE_STOPPER in-scope
   * whenever doing so. On most systems that's blank, but on systems where
   * it's needed we construct an RAII object to plug and un-plug the SIGPIPE.
   * See http://www.microhowto.info/howto/ignore_sigpipe_without_affecting_other_threads_in_a_process.html
   */
  struct sigpipe_stopper {
    bool blocked;
    sigset_t existing_mask;
    sigset_t pipe_mask;
    sigpipe_stopper() {
      sigemptyset(&pipe_mask);
      sigaddset(&pipe_mask, SIGPIPE);
      sigset_t signals;
      sigemptyset(&signals);
      sigpending(&signals);
      if (sigismember(&signals, SIGPIPE)) {
	blocked = false;
      } else {
	blocked = true;
	int r = pthread_sigmask(SIG_BLOCK, &pipe_mask, &existing_mask);
	ceph_assert(r == 0);
      }
    }
    ~sigpipe_stopper() {
      if (blocked) {
	struct timespec nowait{0};
	int r = sigtimedwait(&pipe_mask, 0, &nowait);
	ceph_assert(r == EAGAIN || r == 0);
	r = pthread_sigmask(SIG_SETMASK, &existing_mask, 0);
	ceph_assert(r == 0);
      }
    }
  };
#  define MSGR_SIGPIPE_STOPPER Messenger::sigpipe_stopper stopper();
#else
#  define MSGR_SIGPIPE_STOPPER
#endif
  /**
   * @defgroup Dispatcher Interfacing
   * @{
   */
  /**
   * Determine whether a message can be fast-dispatched. We will
   * query each Dispatcher in sequence to determine if they are
   * capable of handling a particular message via "fast dispatch".
   *
   * @param m The Message we are testing.
   */
  bool ms_can_fast_dispatch(const ceph::cref_t<Message>& m) {
    for ([[maybe_unused]] const auto& [priority, dispatcher] : fast_dispatchers) {
      if (dispatcher->ms_can_fast_dispatch2(m)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Deliver a single Message via "fast dispatch".
   *
   * @param m The Message we are fast dispatching.
   * If none of our Dispatchers can handle it, ceph_abort().
   */
  void ms_fast_dispatch(const ceph::ref_t<Message> &m) {
    m->set_dispatch_stamp(ceph_clock_now());
    for ([[maybe_unused]] const auto& [priority, dispatcher] : fast_dispatchers) {
      if (dispatcher->ms_can_fast_dispatch2(m)) {
        dispatcher->ms_fast_dispatch2(m);
        return;
      }
    }
    ceph_abort();
  }
  void ms_fast_dispatch(Message *m) {
    return ms_fast_dispatch(ceph::ref_t<Message>(m, false)); /* consume ref */
  }
  /**
   *
   */
  void ms_fast_preprocess(const ceph::ref_t<Message> &m) {
    for ([[maybe_unused]] const auto& [priority, dispatcher] : fast_dispatchers) {
      dispatcher->ms_fast_preprocess2(m);
    }
  }
  /**
   *  Deliver a single Message. Send it to each Dispatcher
   *  in sequence until one of them handles it.
   *  If none of our Dispatchers can handle it, ceph_abort().
   *
   *  @param m The Message to deliver.
   */
  void ms_deliver_dispatch(const ceph::ref_t<Message> &m) {
    m->set_dispatch_stamp(ceph_clock_now());
    for ([[maybe_unused]] const auto& [priority, dispatcher] : dispatchers) {
      if (dispatcher->ms_dispatch2(m)) {
        return;
      }
    }
    lsubdout(cct, ms, 0) << "ms_deliver_dispatch: unhandled message " << m << " " << *m << " from "
			 << m->get_source_inst() << dendl;
    ceph_assert(!cct->_conf->ms_die_on_unhandled_msg);
  }
  void ms_deliver_dispatch(Message *m) {
    return ms_deliver_dispatch(ceph::ref_t<Message>(m, false)); /* consume ref */
  }
  /**
   * Notify each Dispatcher of a new Connection. Call
   * this function whenever a new Connection is initiated or
   * reconnects.
   *
   * @param con Pointer to the new Connection.
   */
  void ms_deliver_handle_connect(Connection *con) {
    for ([[maybe_unused]] const auto& [priority, dispatcher] : dispatchers) {
      dispatcher->ms_handle_connect(con);
    }
  }

  /**
   * Notify each fast Dispatcher of a new Connection. Call
   * this function whenever a new Connection is initiated or
   * reconnects.
   *
   * @param con Pointer to the new Connection.
   */
  void ms_deliver_handle_fast_connect(Connection *con) {
    for ([[maybe_unused]] const auto& [priority, dispatcher] : fast_dispatchers) {
      dispatcher->ms_handle_fast_connect(con);
    }
  }

  /**
   * Notify each Dispatcher of a new incoming Connection. Call
   * this function whenever a new Connection is accepted.
   *
   * @param con Pointer to the new Connection.
   */
  void ms_deliver_handle_accept(Connection *con) {
    for ([[maybe_unused]] const auto& [priority, dispatcher] : dispatchers) {
      dispatcher->ms_handle_accept(con);
    }
  }

  /**
   * Notify each fast Dispatcher of a new incoming Connection. Call
   * this function whenever a new Connection is accepted.
   *
   * @param con Pointer to the new Connection.
   */
  void ms_deliver_handle_fast_accept(Connection *con) {
    for ([[maybe_unused]] const auto& [priority, dispatcher] : fast_dispatchers) {
      dispatcher->ms_handle_fast_accept(con);
    }
  }

  /**
   * Notify each Dispatcher of a Connection which may have lost
   * Messages. Call this function whenever you detect that a lossy Connection
   * has been disconnected.
   *
   * @param con Pointer to the broken Connection.
   */
  void ms_deliver_handle_reset(Connection *con) {
    for ([[maybe_unused]] const auto& [priority, dispatcher] : dispatchers) {
      if (dispatcher->ms_handle_reset(con)) {
        return;
      }
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
    for ([[maybe_unused]] const auto& [priority, dispatcher] : dispatchers) {
      dispatcher->ms_handle_remote_reset(con);
    }
  }

  /**
   * Notify each Dispatcher of a Connection for which reconnection
   * attempts are being refused. Call this function whenever you
   * detect that a lossy Connection has been disconnected and it's
   * impossible to reconnect.
   *
   * @param con Pointer to the broken Connection.
   */
  void ms_deliver_handle_refused(Connection *con) {
    for ([[maybe_unused]] const auto& [priority, dispatcher] : dispatchers) {
      if (dispatcher->ms_handle_refused(con)) {
        return;
      }
    }
  }

  void set_require_authorizer(bool b) {
    require_authorizer = b;
  }

  /**
   * @} // Dispatcher Interfacing
   */
};



#endif
