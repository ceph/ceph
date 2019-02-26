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

#include "acconfig.h"

#include <iostream>
#include <fstream>

#include "AsyncMessenger.h"

#include "common/config.h"
#include "common/Timer.h"
#include "common/errno.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "common/EventTrace.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, AsyncMessenger *m) {
  return *_dout << "-- " << m->get_myaddrs() << " ";
}

static ostream& _prefix(std::ostream *_dout, Processor *p) {
  return *_dout << " Processor -- ";
}


/*******************
 * Processor
 */

class Processor::C_processor_accept : public EventCallback {
  Processor *pro;

 public:
  explicit C_processor_accept(Processor *p): pro(p) {}
  void do_request(uint64_t id) override {
    pro->accept();
  }
};

Processor::Processor(AsyncMessenger *r, Worker *w, CephContext *c)
  : msgr(r), net(c), worker(w),
    listen_handler(new C_processor_accept(this)) {}

int Processor::bind(const entity_addrvec_t &bind_addrs,
		    const set<int>& avoid_ports,
		    entity_addrvec_t* bound_addrs)
{
  const auto& conf = msgr->cct->_conf;
  // bind to socket(s)
  ldout(msgr->cct, 10) << __func__ << " " << bind_addrs << dendl;

  SocketOptions opts;
  opts.nodelay = msgr->cct->_conf->ms_tcp_nodelay;
  opts.rcbuf_size = msgr->cct->_conf->ms_tcp_rcvbuf;

  listen_sockets.resize(bind_addrs.v.size());
  *bound_addrs = bind_addrs;

  for (unsigned k = 0; k < bind_addrs.v.size(); ++k) {
    auto& listen_addr = bound_addrs->v[k];

    /* bind to port */
    int r = -1;

    for (int i = 0; i < conf->ms_bind_retry_count; i++) {
      if (i > 0) {
	lderr(msgr->cct) << __func__ << " was unable to bind. Trying again in "
			 << conf->ms_bind_retry_delay << " seconds " << dendl;
	sleep(conf->ms_bind_retry_delay);
      }

      if (listen_addr.get_port()) {
	worker->center.submit_to(
	  worker->center.get_id(),
	  [this, k, &listen_addr, &opts, &r]() {
	    r = worker->listen(listen_addr, k, opts, &listen_sockets[k]);
	  }, false);
	if (r < 0) {
	  lderr(msgr->cct) << __func__ << " unable to bind to " << listen_addr
			   << ": " << cpp_strerror(r) << dendl;
	  continue;
	}
      } else {
	// try a range of ports
	for (int port = msgr->cct->_conf->ms_bind_port_min;
	     port <= msgr->cct->_conf->ms_bind_port_max;
	     port++) {
	  if (avoid_ports.count(port))
	    continue;

	  listen_addr.set_port(port);
	  worker->center.submit_to(
	    worker->center.get_id(),
	    [this, k, &listen_addr, &opts, &r]() {
	      r = worker->listen(listen_addr, k, opts, &listen_sockets[k]);
	    }, false);
	  if (r == 0)
	    break;
	}
	if (r < 0) {
	  lderr(msgr->cct) << __func__ << " unable to bind to " << listen_addr
			   << " on any port in range "
			   << msgr->cct->_conf->ms_bind_port_min
			   << "-" << msgr->cct->_conf->ms_bind_port_max << ": "
			   << cpp_strerror(r) << dendl;
	  listen_addr.set_port(0); // Clear port before retry, otherwise we shall fail again.
	  continue;
	}
	ldout(msgr->cct, 10) << __func__ << " bound on random port "
			     << listen_addr << dendl;
      }
      if (r == 0) {
	break;
      }
    }

    // It seems that binding completely failed, return with that exit status
    if (r < 0) {
      lderr(msgr->cct) << __func__ << " was unable to bind after "
		       << conf->ms_bind_retry_count
		       << " attempts: " << cpp_strerror(r) << dendl;
      for (unsigned j = 0; j < k; ++j) {
	// clean up previous bind
	listen_sockets[j].abort_accept();
      }
      return r;
    }
  }

  ldout(msgr->cct, 10) << __func__ << " bound to " << *bound_addrs << dendl;
  return 0;
}

void Processor::start()
{
  ldout(msgr->cct, 1) << __func__ << dendl;

  // start thread
  worker->center.submit_to(worker->center.get_id(), [this]() {
      for (auto& l : listen_sockets) {
	if (l) {
	  worker->center.create_file_event(l.fd(), EVENT_READABLE,
					   listen_handler); }
      }
    }, false);
}

void Processor::accept()
{
  SocketOptions opts;
  opts.nodelay = msgr->cct->_conf->ms_tcp_nodelay;
  opts.rcbuf_size = msgr->cct->_conf->ms_tcp_rcvbuf;
  opts.priority = msgr->get_socket_priority();

  for (auto& listen_socket : listen_sockets) {
    ldout(msgr->cct, 10) << __func__ << " listen_fd=" << listen_socket.fd()
			 << dendl;
    unsigned accept_error_num = 0;

    while (true) {
      entity_addr_t addr;
      ConnectedSocket cli_socket;
      Worker *w = worker;
      if (!msgr->get_stack()->support_local_listen_table())
	w = msgr->get_stack()->get_worker();
      else
	++w->references;
      int r = listen_socket.accept(&cli_socket, opts, &addr, w);
      if (r == 0) {
	ldout(msgr->cct, 10) << __func__ << " accepted incoming on sd "
			     << cli_socket.fd() << dendl;

	msgr->add_accept(
	  w, std::move(cli_socket),
	  msgr->get_myaddrs().v[listen_socket.get_addr_slot()],
	  addr);
	accept_error_num = 0;
	continue;
      } else {
	if (r == -EINTR) {
	  continue;
	} else if (r == -EAGAIN) {
	  break;
	} else if (r == -EMFILE || r == -ENFILE) {
	  lderr(msgr->cct) << __func__ << " open file descriptions limit reached sd = " << listen_socket.fd()
			   << " errno " << r << " " << cpp_strerror(r) << dendl;
	  if (++accept_error_num > msgr->cct->_conf->ms_max_accept_failures) {
	    lderr(msgr->cct) << "Proccessor accept has encountered enough error numbers, just do ceph_abort()." << dendl;
	    ceph_abort();
	  }
	  continue;
	} else if (r == -ECONNABORTED) {
	  ldout(msgr->cct, 0) << __func__ << " it was closed because of rst arrived sd = " << listen_socket.fd()
			      << " errno " << r << " " << cpp_strerror(r) << dendl;
	  continue;
	} else {
	  lderr(msgr->cct) << __func__ << " no incoming connection?"
			   << " errno " << r << " " << cpp_strerror(r) << dendl;
	  if (++accept_error_num > msgr->cct->_conf->ms_max_accept_failures) {
	    lderr(msgr->cct) << "Proccessor accept has encountered enough error numbers, just do ceph_abort()." << dendl;
	    ceph_abort();
	  }
	  continue;
	}
      }
    }
  }
}

void Processor::stop()
{
  ldout(msgr->cct,10) << __func__ << dendl;

  worker->center.submit_to(worker->center.get_id(), [this]() {
      for (auto& listen_socket : listen_sockets) {
	if (listen_socket) {
	  worker->center.delete_file_event(listen_socket.fd(), EVENT_READABLE);
	  listen_socket.abort_accept();
	}
      }
    }, false);
}


struct StackSingleton {
  CephContext *cct;
  std::shared_ptr<NetworkStack> stack;

  explicit StackSingleton(CephContext *c): cct(c) {}
  void ready(std::string &type) {
    if (!stack)
      stack = NetworkStack::create(cct, type);
  }
  ~StackSingleton() {
    stack->stop();
  }
};


class C_handle_reap : public EventCallback {
  AsyncMessenger *msgr;

  public:
  explicit C_handle_reap(AsyncMessenger *m): msgr(m) {}
  void do_request(uint64_t id) override {
    // judge whether is a time event
    msgr->reap_dead();
  }
};

/*******************
 * AsyncMessenger
 */

AsyncMessenger::AsyncMessenger(CephContext *cct, entity_name_t name,
                               const std::string &type, string mname, uint64_t _nonce)
  : SimplePolicyMessenger(cct, name,mname, _nonce),
    dispatch_queue(cct, this, mname),
    lock("AsyncMessenger::lock"),
    nonce(_nonce), need_addr(true), did_bind(false),
    global_seq(0), deleted_lock("AsyncMessenger::deleted_lock"),
    cluster_protocol(0), stopped(true)
{
  std::string transport_type = "posix";
  if (type.find("rdma") != std::string::npos)
    transport_type = "rdma";
  else if (type.find("dpdk") != std::string::npos)
    transport_type = "dpdk";

  auto single = &cct->lookup_or_create_singleton_object<StackSingleton>(
    "AsyncMessenger::NetworkStack::" + transport_type, true, cct);
  single->ready(transport_type);
  stack = single->stack.get();
  stack->start();
  local_worker = stack->get_worker();
  local_connection = new AsyncConnection(cct, this, &dispatch_queue,
					 local_worker, true, true);
  init_local_connection();
  reap_handler = new C_handle_reap(this);
  unsigned processor_num = 1;
  if (stack->support_local_listen_table())
    processor_num = stack->get_num_worker();
  for (unsigned i = 0; i < processor_num; ++i)
    processors.push_back(new Processor(this, stack->get_worker(i), cct));
}

/**
 * Destroy the AsyncMessenger. Pretty simple since all the work is done
 * elsewhere.
 */
AsyncMessenger::~AsyncMessenger()
{
  delete reap_handler;
  ceph_assert(!did_bind); // either we didn't bind or we shut down the Processor
  local_connection->mark_down();
  for (auto &&p : processors)
    delete p;
}

void AsyncMessenger::ready()
{
  ldout(cct,10) << __func__ << " " << get_myaddrs() << dendl;

  stack->ready();
  if (pending_bind) {
    int err = bindv(pending_bind_addrs);
    if (err) {
      lderr(cct) << __func__ << " postponed bind failed" << dendl;
      ceph_abort();
    }
  }

  Mutex::Locker l(lock);
  for (auto &&p : processors)
    p->start();
  dispatch_queue.start();
}

int AsyncMessenger::shutdown()
{
  ldout(cct,10) << __func__ << " " << get_myaddrs() << dendl;

  // done!  clean up.
  for (auto &&p : processors)
    p->stop();
  mark_down_all();
  // break ref cycles on the loopback connection
  local_connection->set_priv(NULL);
  did_bind = false;
  lock.Lock();
  stop_cond.Signal();
  stopped = true;
  lock.Unlock();
  stack->drain();
  return 0;
}

int AsyncMessenger::bind(const entity_addr_t &bind_addr)
{
  ldout(cct,10) << __func__ << " " << bind_addr << dendl;
  // old bind() can take entity_addr_t(). new bindv() can take a
  // 0.0.0.0-like address but needs type and family to be set.
  auto a = bind_addr;
  if (a == entity_addr_t()) {
    a.set_type(entity_addr_t::TYPE_LEGACY);
    if (cct->_conf->ms_bind_ipv6) {
      a.set_family(AF_INET6);
    } else {
      a.set_family(AF_INET);
    }
  }
  return bindv(entity_addrvec_t(a));
}

int AsyncMessenger::bindv(const entity_addrvec_t &bind_addrs)
{
  lock.Lock();

  if (!pending_bind && started) {
    ldout(cct,10) << __func__ << " already started" << dendl;
    lock.Unlock();
    return -1;
  }

  ldout(cct,10) << __func__ << " " << bind_addrs << dendl;

  if (!stack->is_ready()) {
    ldout(cct, 10) << __func__ << " Network Stack is not ready for bind yet - postponed" << dendl;
    pending_bind_addrs = bind_addrs;
    pending_bind = true;
    lock.Unlock();
    return 0;
  }

  lock.Unlock();

  // bind to a socket
  set<int> avoid_ports;
  entity_addrvec_t bound_addrs;
  unsigned i = 0;
  for (auto &&p : processors) {
    int r = p->bind(bind_addrs, avoid_ports, &bound_addrs);
    if (r) {
      // Note: this is related to local tcp listen table problem.
      // Posix(default kernel implementation) backend shares listen table
      // in the kernel, so all threads can use the same listen table naturally
      // and only one thread need to bind. But other backends(like dpdk) uses local
      // listen table, we need to bind/listen tcp port for each worker. So if the
      // first worker failed to bind, it could be think the normal error then handle
      // it, like port is used case. But if the first worker successfully to bind
      // but the second worker failed, it's not expected and we need to assert
      // here
      ceph_assert(i == 0);
      return r;
    }
    ++i;
  }
  _finish_bind(bind_addrs, bound_addrs);
  return 0;
}

int AsyncMessenger::rebind(const set<int>& avoid_ports)
{
  ldout(cct,1) << __func__ << " rebind avoid " << avoid_ports << dendl;
  ceph_assert(did_bind);

  for (auto &&p : processors)
    p->stop();
  mark_down_all();

  // adjust the nonce; we want our entity_addr_t to be truly unique.
  nonce += 1000000;
  ldout(cct, 10) << __func__ << " new nonce " << nonce
		 << " and addr " << get_myaddrs() << dendl;

  entity_addrvec_t bound_addrs;
  entity_addrvec_t bind_addrs = get_myaddrs();
  set<int> new_avoid(avoid_ports);
  for (auto& a : bind_addrs.v) {
    new_avoid.insert(a.get_port());
    a.set_port(0);
  }
  ldout(cct, 10) << __func__ << " will try " << bind_addrs
		 << " and avoid ports " << new_avoid << dendl;
  unsigned i = 0;
  for (auto &&p : processors) {
    int r = p->bind(bind_addrs, avoid_ports, &bound_addrs);
    if (r) {
      ceph_assert(i == 0);
      return r;
    }
    ++i;
  }
  _finish_bind(bind_addrs, bound_addrs);
  for (auto &&p : processors) {
    p->start();
  }
  return 0;
}

int AsyncMessenger::client_bind(const entity_addr_t &bind_addr)
{
  if (!cct->_conf->ms_bind_before_connect)
    return 0;
  Mutex::Locker l(lock);
  if (did_bind) {
    return 0;
  }
  if (started) {
    ldout(cct, 10) << __func__ << " already started" << dendl;
    return -1;
  }
  ldout(cct, 10) << __func__ << " " << bind_addr << dendl;

  set_myaddrs(entity_addrvec_t(bind_addr));
  return 0;
}

void AsyncMessenger::_finish_bind(const entity_addrvec_t& bind_addrs,
				  const entity_addrvec_t& listen_addrs)
{
  set_myaddrs(bind_addrs);
  for (auto& a : bind_addrs.v) {
    if (!a.is_blank_ip()) {
      learned_addr(a);
    }
  }

  if (get_myaddrs().front().get_port() == 0) {
    set_myaddrs(listen_addrs);
  }
  entity_addrvec_t newaddrs = *my_addrs;
  for (auto& a : newaddrs.v) {
    a.set_nonce(nonce);
  }
  set_myaddrs(newaddrs);

  init_local_connection();

  ldout(cct,1) << __func__ << " bind my_addrs is " << get_myaddrs() << dendl;
  did_bind = true;
}

int AsyncMessenger::start()
{
  lock.Lock();
  ldout(cct,1) << __func__ << " start" << dendl;

  // register at least one entity, first!
  ceph_assert(my_name.type() >= 0);

  ceph_assert(!started);
  started = true;
  stopped = false;

  if (!did_bind) {
    entity_addrvec_t newaddrs = *my_addrs;
    for (auto& a : newaddrs.v) {
      a.nonce = nonce;
    }
    set_myaddrs(newaddrs);
    _init_local_connection();
  }

  lock.Unlock();
  return 0;
}

void AsyncMessenger::wait()
{
  lock.Lock();
  if (!started) {
    lock.Unlock();
    return;
  }
  if (!stopped)
    stop_cond.Wait(lock);

  lock.Unlock();

  dispatch_queue.shutdown();
  if (dispatch_queue.is_started()) {
    ldout(cct, 10) << __func__ << ": waiting for dispatch queue" << dendl;
    dispatch_queue.wait();
    dispatch_queue.discard_local();
    ldout(cct, 10) << __func__ << ": dispatch queue is stopped" << dendl;
  }

  // close all connections
  shutdown_connections(false);
  stack->drain();

  ldout(cct, 10) << __func__ << ": done." << dendl;
  ldout(cct, 1) << __func__ << " complete." << dendl;
  started = false;
}

void AsyncMessenger::add_accept(Worker *w, ConnectedSocket cli_socket,
				const entity_addr_t &listen_addr,
				const entity_addr_t &peer_addr)
{
  lock.Lock();
  AsyncConnectionRef conn = new AsyncConnection(cct, this, &dispatch_queue, w,
						listen_addr.is_msgr2(), false);
  conn->accept(std::move(cli_socket), listen_addr, peer_addr);
  accepting_conns.insert(conn);
  lock.Unlock();
}

AsyncConnectionRef AsyncMessenger::create_connect(
  const entity_addrvec_t& addrs, int type)
{
  ceph_assert(lock.is_locked());

  ldout(cct, 10) << __func__ << " " << addrs
      << ", creating connection and registering" << dendl;

  // here is where we decide which of the addrs to connect to.  always prefer
  // the first one, if we support it.
  entity_addr_t target;
  for (auto& a : addrs.v) {
    if (!a.is_msgr2() && !a.is_legacy()) {
      continue;
    }
    // FIXME: for ipv4 vs ipv6, check whether local host can handle ipv6 before
    // trying it?  for now, just pick whichever is listed first.
    target = a;
    break;
  }

  // create connection
  Worker *w = stack->get_worker();
  AsyncConnectionRef conn = new AsyncConnection(cct, this, &dispatch_queue, w,
						target.is_msgr2(), false);
  conn->connect(addrs, type, target);
  ceph_assert(!conns.count(addrs));
  ldout(cct, 10) << __func__ << " " << conn << " " << addrs << " "
		 << *conn->peer_addrs << dendl;
  conns[addrs] = conn;
  w->get_perf_counter()->inc(l_msgr_active_connections);

  return conn;
}


ConnectionRef AsyncMessenger::get_loopback_connection()
{
  return local_connection;
}

bool AsyncMessenger::should_use_msgr2()
{
  // if we are bound to v1 only, and we are connecting to a v2 peer,
  // we cannot use the peer's v2 address. otherwise the connection
  // is assymetrical, because they would have to use v1 to connect
  // to us, and we would use v2, and connection race detection etc
  // would totally break down (among other things).  or, the other
  // end will be confused that we advertise ourselve with a v1
  // address only (that we bound to) but connected with protocol v2.
  return !did_bind || get_myaddrs().has_msgr2();
}

entity_addrvec_t AsyncMessenger::_filter_addrs(int type,
					       const entity_addrvec_t& addrs)
{
  if (!should_use_msgr2()) {
    ldout(cct, 10) << __func__ << " " << addrs << " type " << type
		   << " limiting to v1 ()" << dendl;
    entity_addrvec_t r;
    for (auto& i : addrs.v) {
      if (i.is_msgr2()) {
	continue;
      }
      r.v.push_back(i);
    }
    return r;
  } else {
    return addrs;
  }
}

int AsyncMessenger::send_to(Message *m, int type, const entity_addrvec_t& addrs)
{
  Mutex::Locker l(lock);

  FUNCTRACE(cct);
  ceph_assert(m);

  if (m->get_type() == CEPH_MSG_OSD_OP)
    OID_EVENT_TRACE(((MOSDOp *)m)->get_oid().name.c_str(), "SEND_MSG_OSD_OP");
  else if (m->get_type() == CEPH_MSG_OSD_OPREPLY)
    OID_EVENT_TRACE(((MOSDOpReply *)m)->get_oid().name.c_str(), "SEND_MSG_OSD_OP_REPLY");

  ldout(cct, 1) << __func__ << "--> " << ceph_entity_type_name(type) << " "
      << addrs << " -- " << *m << " -- ?+"
      << m->get_data().length() << " " << m << dendl;

  if (addrs.empty()) {
    ldout(cct,0) << __func__ <<  " message " << *m
        << " with empty dest " << addrs << dendl;
    m->put();
    return -EINVAL;
  }

  auto av = _filter_addrs(type, addrs);
  AsyncConnectionRef conn = _lookup_conn(av);
  submit_message(m, conn, av, type);
  return 0;
}

ConnectionRef AsyncMessenger::connect_to(int type, const entity_addrvec_t& addrs)
{
  Mutex::Locker l(lock);
  if (*my_addrs == addrs ||
      (addrs.v.size() == 1 &&
       my_addrs->contains(addrs.front()))) {
    // local
    return local_connection;
  }

  auto av = _filter_addrs(type, addrs);

  AsyncConnectionRef conn = _lookup_conn(av);
  if (conn) {
    ldout(cct, 10) << __func__ << " " << av << " existing " << conn << dendl;
  } else {
    conn = create_connect(av, type);
    ldout(cct, 10) << __func__ << " " << av << " new " << conn << dendl;
  }

  return conn;
}

void AsyncMessenger::submit_message(Message *m, AsyncConnectionRef con,
                                    const entity_addrvec_t& dest_addrs,
				    int dest_type)
{
  if (cct->_conf->ms_dump_on_send) {
    m->encode(-1, MSG_CRC_ALL);
    ldout(cct, 0) << __func__ << " submit_message " << *m << "\n";
    m->get_payload().hexdump(*_dout);
    if (m->get_data().length() > 0) {
      *_dout << " data:\n";
      m->get_data().hexdump(*_dout);
    }
    *_dout << dendl;
    m->clear_payload();
  }

  // existing connection?
  if (con) {
    con->send_message(m);
    return ;
  }

  // local?
  if (*my_addrs == dest_addrs ||
      (dest_addrs.v.size() == 1 &&
       my_addrs->contains(dest_addrs.front()))) {
    // local
    local_connection->send_message(m);
    return ;
  }

  // remote, no existing connection.
  const Policy& policy = get_policy(dest_type);
  if (policy.server) {
    ldout(cct, 20) << __func__ << " " << *m << " remote, " << dest_addrs
        << ", lossy server for target type "
        << ceph_entity_type_name(dest_type) << ", no session, dropping." << dendl;
    m->put();
  } else {
    ldout(cct,20) << __func__ << " " << *m << " remote, " << dest_addrs
		  << ", new connection." << dendl;
    con = create_connect(dest_addrs, dest_type);
    con->send_message(m);
  }
}

/**
 * If my_addr doesn't have an IP set, this function
 * will fill it in from the passed addr. Otherwise it does nothing and returns.
 */
bool AsyncMessenger::set_addr_unknowns(const entity_addrvec_t &addrs)
{
  ldout(cct,1) << __func__ << " " << addrs << dendl;
  bool ret = false;
  Mutex::Locker l(lock);

  entity_addrvec_t newaddrs = *my_addrs;
  for (auto& a : newaddrs.v) {
    if (a.is_blank_ip()) {
      int type = a.get_type();
      int port = a.get_port();
      uint32_t nonce = a.get_nonce();
      for (auto& b : addrs.v) {
	if (a.get_family() == b.get_family()) {
	  ldout(cct,1) << __func__ << " assuming my addr " << a
		       << " matches provided addr " << b << dendl;
	  a = b;
	  a.set_nonce(nonce);
	  a.set_type(type);
	  a.set_port(port);
	  ret = true;
	  break;
	}
      }
    }
  }
  set_myaddrs(newaddrs);
  if (ret) {
    _init_local_connection();
  }
  ldout(cct,1) << __func__ << " now " << *my_addrs << dendl;
  return ret;
}

void AsyncMessenger::set_addrs(const entity_addrvec_t &addrs)
{
  Mutex::Locker l(lock);
  auto t = addrs;
  for (auto& a : t.v) {
    a.set_nonce(nonce);
  }
  set_myaddrs(t);
  _init_local_connection();
}

void AsyncMessenger::shutdown_connections(bool queue_reset)
{
  ldout(cct,1) << __func__ << " " << dendl;
  lock.Lock();
  for (set<AsyncConnectionRef>::iterator q = accepting_conns.begin();
       q != accepting_conns.end(); ++q) {
    AsyncConnectionRef p = *q;
    ldout(cct, 5) << __func__ << " accepting_conn " << p.get() << dendl;
    p->stop(queue_reset);
  }
  accepting_conns.clear();

  while (!conns.empty()) {
    auto it = conns.begin();
    AsyncConnectionRef p = it->second;
    ldout(cct, 5) << __func__ << " mark down " << it->first << " " << p << dendl;
    conns.erase(it);
    p->get_perf_counter()->dec(l_msgr_active_connections);
    p->stop(queue_reset);
  }

  {
    Mutex::Locker l(deleted_lock);
    while (!deleted_conns.empty()) {
      set<AsyncConnectionRef>::iterator it = deleted_conns.begin();
      AsyncConnectionRef p = *it;
      ldout(cct, 5) << __func__ << " delete " << p << dendl;
      deleted_conns.erase(it);
    }
  }
  lock.Unlock();
}

void AsyncMessenger::mark_down_addrs(const entity_addrvec_t& addrs)
{
  lock.Lock();
  AsyncConnectionRef p = _lookup_conn(addrs);
  if (p) {
    ldout(cct, 1) << __func__ << " " << addrs << " -- " << p << dendl;
    p->stop(true);
  } else {
    ldout(cct, 1) << __func__ << " " << addrs << " -- connection dne" << dendl;
  }
  lock.Unlock();
}

int AsyncMessenger::get_proto_version(int peer_type, bool connect) const
{
  int my_type = my_name.type();

  // set reply protocol version
  if (peer_type == my_type) {
    // internal
    return cluster_protocol;
  } else {
    // public
    switch (connect ? peer_type : my_type) {
      case CEPH_ENTITY_TYPE_OSD: return CEPH_OSDC_PROTOCOL;
      case CEPH_ENTITY_TYPE_MDS: return CEPH_MDSC_PROTOCOL;
      case CEPH_ENTITY_TYPE_MON: return CEPH_MONC_PROTOCOL;
    }
  }
  return 0;
}

int AsyncMessenger::accept_conn(AsyncConnectionRef conn)
{
  Mutex::Locker l(lock);
  auto it = conns.find(*conn->peer_addrs);
  if (it != conns.end()) {
    AsyncConnectionRef existing = it->second;

    // lazy delete, see "deleted_conns"
    // If conn already in, we will return 0
    Mutex::Locker l(deleted_lock);
    if (deleted_conns.erase(existing)) {
      existing->get_perf_counter()->dec(l_msgr_active_connections);
      conns.erase(it);
    } else if (conn != existing) {
      return -1;
    }
  }
  ldout(cct, 10) << __func__ << " " << conn << " " << *conn->peer_addrs << dendl;
  conns[*conn->peer_addrs] = conn;
  conn->get_perf_counter()->inc(l_msgr_active_connections);
  accepting_conns.erase(conn);
  return 0;
}


bool AsyncMessenger::learned_addr(const entity_addr_t &peer_addr_for_me)
{
  // be careful here: multiple threads may block here, and readers of
  // my_addr do NOT hold any lock.

  // this always goes from true -> false under the protection of the
  // mutex.  if it is already false, we need not retake the mutex at
  // all.
  if (!need_addr)
    return false;
  std::lock_guard l(lock);
  if (need_addr) {
    if (my_addrs->empty()) {
      auto a = peer_addr_for_me;
      a.set_type(entity_addr_t::TYPE_ANY);
      a.set_nonce(nonce);
      if (!did_bind) {
	a.set_port(0);
      }
      set_myaddrs(entity_addrvec_t(a));
      ldout(cct,10) << __func__ << " had no addrs" << dendl;
    } else {
      // fix all addrs of the same family, regardless of type (msgr2 vs legacy)
      entity_addrvec_t newaddrs = *my_addrs;
      for (auto& a : newaddrs.v) {
	if (a.is_blank_ip() &&
	    a.get_family() == peer_addr_for_me.get_family()) {
	  entity_addr_t t = peer_addr_for_me;
	  if (!did_bind) {
	    t.set_type(entity_addr_t::TYPE_ANY);
	    t.set_port(0);
	  } else {	  
	    t.set_type(a.get_type());
	    t.set_port(a.get_port());
	  }
	  t.set_nonce(a.get_nonce());
	  ldout(cct,10) << __func__ << " " << a << " -> " << t << dendl;
	  a = t;
	}
      }
      set_myaddrs(newaddrs);
    }
    ldout(cct, 1) << __func__ << " learned my addr " << *my_addrs
		  << " (peer_addr_for_me " << peer_addr_for_me << ")" << dendl;
    _init_local_connection();
    need_addr = false;
    return true;
  }
  return false;
}

int AsyncMessenger::reap_dead()
{
  ldout(cct, 1) << __func__ << " start" << dendl;
  int num = 0;

  Mutex::Locker l1(lock);
  Mutex::Locker l2(deleted_lock);

  while (!deleted_conns.empty()) {
    auto it = deleted_conns.begin();
    AsyncConnectionRef p = *it;
    ldout(cct, 5) << __func__ << " delete " << p << dendl;
    auto conns_it = conns.find(*p->peer_addrs);
    if (conns_it != conns.end() && conns_it->second == p)
      conns.erase(conns_it);
    accepting_conns.erase(p);
    deleted_conns.erase(it);
    ++num;
  }

  return num;
}
