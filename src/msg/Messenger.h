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

#include <errno.h>
#include <sstream>

class MDS;
class Timer;


class Messenger {
public:
  struct Policy {
    bool lossy;
    bool server;
    Throttle *throttler;

    uint64_t features_supported;
    uint64_t features_required;

    Policy()
      : lossy(false), server(false), throttler(NULL),
	features_supported(CEPH_FEATURES_SUPPORTED_DEFAULT),
	features_required(0) {}
    Policy(bool l, bool s, uint64_t sup, uint64_t req)
      : lossy(l), server(s), throttler(NULL),
	features_supported(sup | CEPH_FEATURES_SUPPORTED_DEFAULT),
	features_required(req) {}

    static Policy stateful_server(uint64_t sup, uint64_t req) {
      return Policy(false, true, sup, req);
    }
    static Policy stateless_server(uint64_t sup, uint64_t req) {
      return Policy(true, true, sup, req);
    }
    static Policy lossless_peer(uint64_t sup, uint64_t req) {
      return Policy(false, false, sup, req);
    }
    static Policy client(uint64_t sup, uint64_t req) {
      return Policy(false, false, sup, req);
    }
  };


private:
  list<Dispatcher*> dispatchers;

protected:
  /// the "name" of the local daemon. eg client.99
  entity_inst_t my_inst;
  /// the name for the messenger in the context of this process (e.g., "client", "backend", "heartbeat")
  string name;
  int default_send_priority;
  /// set to true once the Messenger has started, and set to false on shutdown
  bool started;

 public:
  CephContext *cct;
  Messenger(CephContext *cct_, entity_name_t w, string name)
    : my_inst(), name(name),
      default_send_priority(CEPH_MSG_PRIO_DEFAULT), started(false),
      cct(cct_)
  {
    my_inst.name = w;
  }
  virtual ~Messenger() {}

  virtual void destroy() {
  }

  // accessors
  const entity_name_t& get_myname() { return my_inst.name; }
  /**
   * Retrieve the Messenger's address.
   *
   * @return A copy of the address this Messenger currently
   * believes to be its own.
   */
  const entity_addr_t& get_myaddr() { return my_inst.addr; }
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
  const entity_inst_t& get_myinst() { return my_inst; }
  
  /**
   * Set the name of the local entity. The name is reported to others and
   * can be changed while the system is running, but doing so at incorrect
   * times may have bad results.
   *
   * @param m The name to set.
   */
  void set_myname(const entity_name_t m) { my_inst.name = m; }

  /**
   * Set the default send priority
   * This is an init-time function and must be called *before* calling
   * start().
   *
   * @param p The cluster protocol to use. Defined externally.
   */
  void set_default_send_priority(int p) {
    assert(!started);
    default_send_priority = p;
  }
  int get_default_send_priority() { return default_send_priority; }
  
  // hrmpf.
  virtual int get_dispatch_queue_len() { return 0; };

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
    if (first)
      ready();
  }

  virtual void ready() { }
  bool is_ready() { return !dispatchers.empty(); }

  // dispatch incoming messages
  void ms_deliver_dispatch(Message *m) {
    m->set_dispatch_stamp(ceph_clock_now(cct));
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      if ((*p)->ms_dispatch(m))
	return;
    std::ostringstream oss;
    oss << "ms_deliver_dispatch: fatal error: unhandled message "
	<< m << " " << *m << " from " << m->get_source_inst();
    dout_emergency(oss.str());
    assert(0);
  }
  void ms_deliver_handle_connect(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      (*p)->ms_handle_connect(con);
  }
  void ms_deliver_handle_reset(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      if ((*p)->ms_handle_reset(con))
	return;
  }
  void ms_deliver_handle_remote_reset(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      (*p)->ms_handle_remote_reset(con);
  }

  AuthAuthorizer *ms_deliver_get_authorizer(int peer_type, bool force_new) {
    AuthAuthorizer *a = 0;
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      if ((*p)->ms_get_authorizer(peer_type, &a, force_new))
	return a;
    return NULL;
  }
  bool ms_deliver_verify_authorizer(Connection *con, int peer_type,
				    int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
				    bool& isvalid) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      if ((*p)->ms_verify_authorizer(con, peer_type, protocol, authorizer, authorizer_reply, isvalid))
	return true;
    return false;
  }

  // setup
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
  virtual void suicide() = 0;

  // send message
  virtual void prepare_dest(const entity_inst_t& inst) {}
  virtual int send_message(Message *m, const entity_inst_t& dest) = 0;
  virtual int send_message(Message *m, Connection *con) = 0;
  virtual int lazy_send_message(Message *m, const entity_inst_t& dest) {
    return send_message(m, dest);
  }
  virtual int lazy_send_message(Message *m, Connection *con) = 0;
  virtual int send_keepalive(const entity_inst_t& dest) = 0;
  virtual int send_keepalive(Connection *con) = 0;
  /**
   * Mark down a connection to a remote. This will cause us to
   * discard our outgoing queue for them, and if they try
   * to reconnect they will discard their queue when we
   * inform them of the session reset.
   * It does not generate any notifications to he Dispatcher.
   */
  virtual void mark_down(const entity_addr_t& a) = 0;
  virtual void mark_down(Connection *con) = 0;
  /**
   * Unlike mark_down, this function will try and deliver
   * all messages before ending the connection. But the
   * messages are not delivered reliably, and once they've
   * all been sent out the Connection will be closed and
   * generate an ms_handle_reset notification to the
   * Dispatcher.
   */
  virtual void mark_down_on_empty(Connection *con) = 0;
  virtual void mark_disposable(Connection *con) = 0;
  virtual void mark_down_all() = 0;

  /**
   * Get the Connection object associated with a given entity. If a
   * Connection does not exist, create one and establish a logical connection.
   *
   * @param dest The entity to get a connection for.
   */
  virtual Connection *get_connection(const entity_inst_t& dest) = 0;

  virtual int rebind(int avoid_port) { return -EOPNOTSUPP; }

  virtual int bind(entity_addr_t bind_addr) = 0;

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
   * Set a Throttler which is applied to all Messages from the given
   * type of peer.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param type The peer type this Throttler will apply to.
   * @param t The Throttler to apply. SimpleMessenger does not take
   * ownership of this pointer, but you must not destroy it before
   * you destroy SimpleMessenger.
   */
  virtual void set_policy_throttler(int type, Throttle *t) = 0;

  /**
   * create a new messenger
   *
   * Create a new messenger instance, with whatever implementation is
   * available or specified via the configuration in cct.
   *
   * @param cct context
   * @param ename entity name to register
   * @param lname logical name of the messenger in this process (e.g., "client")
   * @param nonce nonce value to uniquely identify this instance on the current host
   */
  static Messenger *create(CephContext *cct,
			   entity_name_t name,
			   string lname,
			   uint64_t nonce);
};



#endif
