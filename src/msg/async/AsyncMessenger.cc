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

#include <errno.h>
#include <iostream>
#include <fstream>

#include "AsyncMessenger.h"

#include "include/str_list.h"
#include "common/strtol.h"
#include "common/config.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "auth/Crypto.h"
#include "include/Spinlock.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, AsyncMessenger *m) {
  return *_dout << "-- " << m->get_myaddr() << " ";
}

static ostream& _prefix(std::ostream *_dout, Processor *p) {
  return *_dout << " Processor -- ";
}

static ostream& _prefix(std::ostream *_dout, Worker *w) {
  return *_dout << " Worker -- ";
}

static ostream& _prefix(std::ostream *_dout, WorkerPool *p) {
  return *_dout << " WorkerPool -- ";
}



/*******************
 * Processor
 */

int Processor::bind(const entity_addr_t &bind_addr, const set<int>& avoid_ports)
{
  const md_config_t *conf = msgr->cct->_conf;
  // bind to a socket
  ldout(msgr->cct, 10) << __func__ << dendl;

  int family;
  switch (bind_addr.get_family()) {
    case AF_INET:
    case AF_INET6:
      family = bind_addr.get_family();
      break;

    default:
      // bind_addr is empty
      family = conf->ms_bind_ipv6 ? AF_INET6 : AF_INET;
  }

  /* socket creation */
  listen_sd = ::socket(family, SOCK_STREAM, 0);
  if (listen_sd < 0) {
    lderr(msgr->cct) << __func__ << " unable to create socket: "
        << cpp_strerror(errno) << dendl;
    return -errno;
  }

  int r = net.set_nonblock(listen_sd);
  if (r < 0) {
    ::close(listen_sd);
    listen_sd = -1;
    return r;
  }

  net.set_socket_options(listen_sd);

  // use whatever user specified (if anything)
  entity_addr_t listen_addr = bind_addr;
  listen_addr.set_family(family);

  /* bind to port */
  int rc = -1;
  r = -1;

  for (int i = 0; i < conf->ms_bind_retry_count; i++) {
    if (i > 0) {
      lderr(msgr->cct) << __func__ << " was unable to bind. Trying again in "
                       << conf->ms_bind_retry_delay << " seconds " << dendl;
      sleep(conf->ms_bind_retry_delay);
    }

    if (listen_addr.get_port()) {
      // specific port
      // reuse addr+port when possible
      int on = 1;
      rc = ::setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
      if (rc < 0) {
        lderr(msgr->cct) << __func__ << " unable to setsockopt: " << cpp_strerror(errno) << dendl;
        r = -errno;
        continue;
      }

      rc = ::bind(listen_sd, (struct sockaddr *) &listen_addr.ss_addr(), listen_addr.addr_size());
      if (rc < 0) {
        lderr(msgr->cct) << __func__ << " unable to bind to " << listen_addr.ss_addr()
                         << ": " << cpp_strerror(errno) << dendl;
        r = -errno;
        continue;
      }
    } else {
      // try a range of ports
      for (int port = msgr->cct->_conf->ms_bind_port_min; port <= msgr->cct->_conf->ms_bind_port_max; port++) {
        if (avoid_ports.count(port))
          continue;

        listen_addr.set_port(port);
        rc = ::bind(listen_sd, (struct sockaddr *) &listen_addr.ss_addr(), listen_addr.addr_size());
        if (rc == 0)
          break;
      }
      if (rc < 0) {
        lderr(msgr->cct) << __func__ << " unable to bind to " << listen_addr.ss_addr()
                         << " on any port in range " << msgr->cct->_conf->ms_bind_port_min
                         << "-" << msgr->cct->_conf->ms_bind_port_max << ": "
                         << cpp_strerror(errno) << dendl;
        r = -errno;
        listen_addr.set_port(0); // Clear port before retry, otherwise we shall fail again.
        continue;
      }
      ldout(msgr->cct, 10) << __func__ << " bound on random port " << listen_addr << dendl;
    }
    if (rc == 0)
      break;
  }
  // It seems that binding completely failed, return with that exit status
  if (rc < 0) {
    lderr(msgr->cct) << __func__ << " was unable to bind after " << conf->ms_bind_retry_count
                     << " attempts: " << cpp_strerror(errno) << dendl;
    ::close(listen_sd);
    listen_sd = -1;
    return r;
  }

  // what port did we get?
  socklen_t llen = sizeof(listen_addr.ss_addr());
  rc = getsockname(listen_sd, (sockaddr*)&listen_addr.ss_addr(), &llen);
  if (rc < 0) {
    rc = -errno;
    lderr(msgr->cct) << __func__ << " failed getsockname: " << cpp_strerror(rc) << dendl;
    ::close(listen_sd);
    listen_sd = -1;
    return rc;
  }

  ldout(msgr->cct, 10) << __func__ << " bound to " << listen_addr << dendl;

  // listen!
  rc = ::listen(listen_sd, 128);
  if (rc < 0) {
    rc = -errno;
    lderr(msgr->cct) << __func__ << " unable to listen on " << listen_addr
        << ": " << cpp_strerror(rc) << dendl;
    ::close(listen_sd);
    listen_sd = -1;
    return rc;
  }

  msgr->set_myaddr(bind_addr);
  if (bind_addr != entity_addr_t())
    msgr->learned_addr(bind_addr);

  if (msgr->get_myaddr().get_port() == 0) {
    msgr->set_myaddr(listen_addr);
  }
  entity_addr_t addr = msgr->get_myaddr();
  addr.nonce = nonce;
  msgr->set_myaddr(addr);

  msgr->init_local_connection();

  ldout(msgr->cct,1) << __func__ << " bind my_inst.addr is " << msgr->get_myaddr() << dendl;
  return 0;
}

int Processor::rebind(const set<int>& avoid_ports)
{
  ldout(msgr->cct, 1) << __func__ << " rebind avoid " << avoid_ports << dendl;

  entity_addr_t addr = msgr->get_myaddr();
  set<int> new_avoid = avoid_ports;
  new_avoid.insert(addr.get_port());
  addr.set_port(0);

  // adjust the nonce; we want our entity_addr_t to be truly unique.
  nonce += 1000000;
  msgr->my_inst.addr.nonce = nonce;
  ldout(msgr->cct, 10) << __func__ << " new nonce " << nonce << " and inst " << msgr->my_inst << dendl;

  ldout(msgr->cct, 10) << __func__ << " will try " << addr << " and avoid ports " << new_avoid << dendl;
  return bind(addr, new_avoid);
}

int Processor::start(Worker *w)
{
  ldout(msgr->cct, 1) << __func__ << " " << dendl;

  // start thread
  if (listen_sd >= 0) {
    worker = w;
    w->center.create_file_event(listen_sd, EVENT_READABLE, listen_handler);
  }

  return 0;
}

void Processor::accept()
{
  ldout(msgr->cct, 10) << __func__ << " listen_sd=" << listen_sd << dendl;
  int errors = 0;
  while (errors < 4) {
    entity_addr_t addr;
    socklen_t slen = sizeof(addr.ss_addr());
    int sd = ::accept(listen_sd, (sockaddr*)&addr.ss_addr(), &slen);
    if (sd >= 0) {
      errors = 0;
      ldout(msgr->cct, 10) << __func__ << " accepted incoming on sd " << sd << dendl;

      msgr->add_accept(sd);
      continue;
    } else {
      if (errno == EINTR) {
        continue;
      } else if (errno == EAGAIN) {
        break;
      } else {
        errors++;
        ldout(msgr->cct, 20) << __func__ << " no incoming connection?  sd = " << sd
                             << " errno " << errno << " " << cpp_strerror(errno) << dendl;
      }
    }
  }
}

void Processor::stop()
{
  ldout(msgr->cct,10) << __func__ << dendl;

  if (listen_sd >= 0) {
    worker->center.delete_file_event(listen_sd, EVENT_READABLE);
    ::shutdown(listen_sd, SHUT_RDWR);
    ::close(listen_sd);
    listen_sd = -1;
  }
}

void Worker::stop()
{
  ldout(cct, 10) << __func__ << dendl;
  done = true;
  center.wakeup();
}

void *Worker::entry()
{
  ldout(cct, 10) << __func__ << " starting" << dendl;
  if (cct->_conf->ms_async_set_affinity) {
    int cid = pool->get_cpuid(id);
    if (cid >= 0 && set_affinity(cid)) {
      ldout(cct, 0) << __func__ << " sched_setaffinity failed: "
                    << cpp_strerror(errno) << dendl;
    }
  }

  center.set_owner();
  while (!done) {
    ldout(cct, 20) << __func__ << " calling event process" << dendl;

    int r = center.process_events(EventMaxWaitUs);
    if (r < 0) {
      ldout(cct, 20) << __func__ << " process events failed: "
          << cpp_strerror(errno) << dendl;
      // TODO do something?
    }
  }

  return 0;
}

/*******************
 * WorkerPool
 *******************/
const string WorkerPool::name = "AsyncMessenger::WorkerPool";

WorkerPool::WorkerPool(CephContext *c): cct(c), started(false),
                                        barrier_lock("WorkerPool::WorkerPool::barrier_lock"),
                                        barrier_count(0)
{
  assert(cct->_conf->ms_async_op_threads > 0);
  // make sure user won't try to force some crazy number of worker threads
  assert(cct->_conf->ms_async_max_op_threads >= cct->_conf->ms_async_op_threads && 
         cct->_conf->ms_async_op_threads <= 32);
  for (int i = 0; i < cct->_conf->ms_async_op_threads; ++i) {
    Worker *w = new Worker(cct, this, i);
    workers.push_back(w);
  }
  vector<string> corestrs;
  get_str_vec(cct->_conf->ms_async_affinity_cores, corestrs);
  for (vector<string>::iterator it = corestrs.begin();
       it != corestrs.end(); ++it) {
    string err;
    int coreid = strict_strtol(it->c_str(), 10, &err);
    if (err == "")
      coreids.push_back(coreid);
    else
      lderr(cct) << __func__ << " failed to parse " << *it << " in " << cct->_conf->ms_async_affinity_cores << dendl;
  }

}

WorkerPool::~WorkerPool()
{
  for (uint64_t i = 0; i < workers.size(); ++i) {
    if (workers[i]->is_started()) {
      workers[i]->stop();
      workers[i]->join();
    }
    delete workers[i];
  }
}

void WorkerPool::start()
{
  if (!started) {
    for (uint64_t i = 0; i < workers.size(); ++i) {
      workers[i]->create("ms_async_worker");
    }
    started = true;
  }
}

Worker* WorkerPool::get_worker()
{
  ldout(cct, 10) << __func__ << dendl;

   // start with some reasonably large number
  unsigned min_load = std::numeric_limits<int>::max();
  Worker* current_best = nullptr;

  simple_spin_lock(&pool_spin);
  // find worker with least references
  // tempting case is returning on references == 0, but in reality
  // this will happen so rarely that there's no need for special case.
  for (auto p = workers.begin(); p != workers.end(); ++p) {
    unsigned worker_load = (*p)->references.load();
    ldout(cct, 20) << __func__ << " Worker " << *p << " load: " << worker_load << dendl;
    if (worker_load < min_load) {
      current_best = *p;
      min_load = worker_load;
    }
  }

  // if minimum load exceeds amount of workers, make a new worker
  // logic behind this is that we're not going to create new worker
  // just because others have *some* load, we'll defer worker creation
  // until others have *plenty* of load. This will cause new worker
  // to get assigned to all new connections *unless* one or more
  // of workers get their load reduced - in that case, this worker
  // will be assigned to new connection.
  // TODO: add more logic and heuristics, so connections known to be
  // of light workload (heartbeat service, etc.) won't overshadow
  // heavy workload (clients, etc).
  if (!current_best || ((workers.size() < (unsigned)cct->_conf->ms_async_max_op_threads)
      && (min_load > workers.size()))) {
     ldout(cct, 20) << __func__ << " creating worker" << dendl;
     current_best = new Worker(cct, this, workers.size());
     workers.push_back(current_best);
     current_best->create("ms_async_worker");
  } else {
    ldout(cct, 20) << __func__ << " picked " << current_best 
                   << " as best worker with load " << min_load << dendl;
  }

  ++current_best->references;
  simple_spin_unlock(&pool_spin);

  assert(current_best);
  return current_best;
}

void WorkerPool::release_worker(EventCenter* c)
{
  ldout(cct, 10) << __func__ << dendl;
  simple_spin_lock(&pool_spin);
  for (auto p = workers.begin(); p != workers.end(); ++p) {
    if (&((*p)->center) == c) {
      ldout(cct, 10) << __func__ << " found worker, releasing" << dendl;
      int oldref = (*p)->references.fetch_sub(1);
      assert(oldref > 0);
      break;
    }
  }
  simple_spin_unlock(&pool_spin);
}

void WorkerPool::barrier()
{
  ldout(cct, 10) << __func__ << " started." << dendl;
  pthread_t cur = pthread_self();
  for (vector<Worker*>::iterator it = workers.begin(); it != workers.end(); ++it) {
    assert(cur != (*it)->center.get_owner());
    barrier_count.inc();
    (*it)->center.dispatch_event_external(EventCallbackRef(new C_barrier(this)));
  }
  ldout(cct, 10) << __func__ << " wait for " << barrier_count.read() << " barrier" << dendl;
  Mutex::Locker l(barrier_lock);
  while (barrier_count.read())
    barrier_cond.Wait(barrier_lock);

  ldout(cct, 10) << __func__ << " end." << dendl;
}


/*******************
 * AsyncMessenger
 */

AsyncMessenger::AsyncMessenger(CephContext *cct, entity_name_t name,
                               string mname, uint64_t _nonce, uint64_t features)
  : SimplePolicyMessenger(cct, name,mname, _nonce),
    processor(this, cct, _nonce),
    lock("AsyncMessenger::lock"),
    nonce(_nonce), need_addr(true), did_bind(false),
    global_seq(0), deleted_lock("AsyncMessenger::deleted_lock"),
    cluster_protocol(0), stopped(true)
{
  ceph_spin_init(&global_seq_lock);
  cct->lookup_or_create_singleton_object<WorkerPool>(pool, WorkerPool::name);
  local_worker = pool->get_worker();
  local_connection = new AsyncConnection(cct, this, &local_worker->center, local_worker->get_perf_counter());
  local_features = features;
  init_local_connection();
  reap_handler = new C_handle_reap(this);
}

/**
 * Destroy the AsyncMessenger. Pretty simple since all the work is done
 * elsewhere.
 */
AsyncMessenger::~AsyncMessenger()
{
  delete reap_handler;
  assert(!did_bind); // either we didn't bind or we shut down the Processor
  local_connection->mark_down();
}

void AsyncMessenger::ready()
{
  ldout(cct,10) << __func__ << " " << get_myaddr() << dendl;

  Mutex::Locker l(lock);
  Worker *w = pool->get_worker();
  processor.start(w);
}

int AsyncMessenger::shutdown()
{
  ldout(cct,10) << __func__ << " " << get_myaddr() << dendl;

  // break ref cycles on the loopback connection
  processor.stop();
  mark_down_all();
  local_connection->set_priv(NULL);
  pool->barrier();
  lock.Lock();
  stop_cond.Signal();
  lock.Unlock();
  stopped = true;
  return 0;
}


int AsyncMessenger::bind(const entity_addr_t &bind_addr)
{
  lock.Lock();
  if (started) {
    ldout(cct,10) << __func__ << " already started" << dendl;
    lock.Unlock();
    return -1;
  }
  ldout(cct,10) << __func__ << " bind " << bind_addr << dendl;
  lock.Unlock();

  // bind to a socket
  set<int> avoid_ports;
  int r = processor.bind(bind_addr, avoid_ports);
  if (r >= 0)
    did_bind = true;
  return r;
}

int AsyncMessenger::rebind(const set<int>& avoid_ports)
{
  ldout(cct,1) << __func__ << " rebind avoid " << avoid_ports << dendl;
  assert(did_bind);

  processor.stop();
  mark_down_all();
  int r = processor.rebind(avoid_ports);
  if (r == 0) {
    Worker *w = pool->get_worker();
    processor.start(w);
  }
  return r;
}

int AsyncMessenger::start()
{
  lock.Lock();
  ldout(cct,1) << __func__ << " start" << dendl;

  // register at least one entity, first!
  assert(my_inst.name.type() >= 0);

  assert(!started);
  started = true;
  stopped = false;

  if (!did_bind) {
    my_inst.addr.nonce = nonce;
    _init_local_connection();
  }
  pool->start();

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

  // done!  clean up.
  ldout(cct,20) << __func__ << ": stopping processor thread" << dendl;
  processor.stop();
  did_bind = false;
  ldout(cct,20) << __func__ << ": stopped processor thread" << dendl;

  // close all connections
  mark_down_all();

  ldout(cct, 10) << __func__ << ": done." << dendl;
  ldout(cct, 1) << __func__ << " complete." << dendl;
  started = false;
}

AsyncConnectionRef AsyncMessenger::add_accept(int sd)
{
  lock.Lock();
  Worker *w = pool->get_worker();
  AsyncConnectionRef conn = new AsyncConnection(cct, this, &w->center, w->get_perf_counter());
  conn->accept(sd);
  accepting_conns.insert(conn);
  lock.Unlock();
  return conn;
}

AsyncConnectionRef AsyncMessenger::create_connect(const entity_addr_t& addr, int type)
{
  assert(lock.is_locked());
  assert(addr != my_inst.addr);

  ldout(cct, 10) << __func__ << " " << addr
      << ", creating connection and registering" << dendl;

  // create connection
  Worker *w = pool->get_worker();
  AsyncConnectionRef conn = new AsyncConnection(cct, this, &w->center, w->get_perf_counter());
  conn->connect(addr, type);
  assert(!conns.count(addr));
  conns[addr] = conn;
  w->get_perf_counter()->inc(l_msgr_active_connections);

  return conn;
}

ConnectionRef AsyncMessenger::get_connection(const entity_inst_t& dest)
{
  Mutex::Locker l(lock);
  if (my_inst.addr == dest.addr) {
    // local
    return local_connection;
  }

  AsyncConnectionRef conn = _lookup_conn(dest.addr);
  if (conn) {
    ldout(cct, 10) << __func__ << " " << dest << " existing " << conn << dendl;
  } else {
    conn = create_connect(dest.addr, dest.name.type());
    ldout(cct, 10) << __func__ << " " << dest << " new " << conn << dendl;
  }

  return conn;
}

ConnectionRef AsyncMessenger::get_loopback_connection()
{
  return local_connection;
}

int AsyncMessenger::_send_message(Message *m, const entity_inst_t& dest)
{
  ldout(cct, 1) << __func__ << "--> " << dest.name << " "
      << dest.addr << " -- " << *m << " -- ?+"
      << m->get_data().length() << " " << m << dendl;

  if (dest.addr == entity_addr_t()) {
    ldout(cct,0) << __func__ <<  " message " << *m
        << " with empty dest " << dest.addr << dendl;
    m->put();
    return -EINVAL;
  }

  AsyncConnectionRef conn = _lookup_conn(dest.addr);
  submit_message(m, conn, dest.addr, dest.name.type());
  return 0;
}

void AsyncMessenger::submit_message(Message *m, AsyncConnectionRef con,
                                    const entity_addr_t& dest_addr, int dest_type)
{
  if (cct->_conf->ms_dump_on_send) {
    m->encode(-1, MSG_CRC_ALL);
    ldout(cct, 0) << __func__ << "submit_message " << *m << "\n";
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
  if (my_inst.addr == dest_addr) {
    // local
    static_cast<AsyncConnection*>(local_connection.get())->send_message(m);
    return ;
  }

  // remote, no existing connection.
  const Policy& policy = get_policy(dest_type);
  if (policy.server) {
    ldout(cct, 20) << __func__ << " " << *m << " remote, " << dest_addr
        << ", lossy server for target type "
        << ceph_entity_type_name(dest_type) << ", no session, dropping." << dendl;
    m->put();
  } else {
    ldout(cct,20) << __func__ << " " << *m << " remote, " << dest_addr << ", new connection." << dendl;
    con = create_connect(dest_addr, dest_type);
    con->send_message(m);
  }
}

/**
 * If my_inst.addr doesn't have an IP set, this function
 * will fill it in from the passed addr. Otherwise it does nothing and returns.
 */
void AsyncMessenger::set_addr_unknowns(entity_addr_t &addr)
{
  Mutex::Locker l(lock);
  if (my_inst.addr.is_blank_ip()) {
    int port = my_inst.addr.get_port();
    my_inst.addr.addr = addr.addr;
    my_inst.addr.set_port(port);
    _init_local_connection();
  }
}

int AsyncMessenger::send_keepalive(Connection *con)
{
  con->send_keepalive();
  return 0;
}

void AsyncMessenger::mark_down_all()
{
  ldout(cct,1) << __func__ << " " << dendl;
  lock.Lock();
  for (set<AsyncConnectionRef>::iterator q = accepting_conns.begin();
       q != accepting_conns.end(); ++q) {
    AsyncConnectionRef p = *q;
    ldout(cct, 5) << __func__ << " accepting_conn " << p.get() << dendl;
    p->stop();
  }
  accepting_conns.clear();

  while (!conns.empty()) {
    ceph::unordered_map<entity_addr_t, AsyncConnectionRef>::iterator it = conns.begin();
    AsyncConnectionRef p = it->second;
    ldout(cct, 5) << __func__ << " mark down " << it->first << " " << p << dendl;
    conns.erase(it);
    p->get_perf_counter()->dec(l_msgr_active_connections);
    p->stop();
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

void AsyncMessenger::mark_down(const entity_addr_t& addr)
{
  lock.Lock();
  AsyncConnectionRef p = _lookup_conn(addr);
  if (p) {
    ldout(cct, 1) << __func__ << " " << addr << " -- " << p << dendl;
    p->stop();
  } else {
    ldout(cct, 1) << __func__ << " " << addr << " -- connection dne" << dendl;
  }
  lock.Unlock();
}

int AsyncMessenger::get_proto_version(int peer_type, bool connect)
{
  int my_type = my_inst.name.type();

  // set reply protocol version
  if (peer_type == my_type) {
    // internal
    return cluster_protocol;
  } else {
    // public
    if (connect) {
      switch (peer_type) {
        case CEPH_ENTITY_TYPE_OSD: return CEPH_OSDC_PROTOCOL;
        case CEPH_ENTITY_TYPE_MDS: return CEPH_MDSC_PROTOCOL;
        case CEPH_ENTITY_TYPE_MON: return CEPH_MONC_PROTOCOL;
      }
    } else {
      switch (my_type) {
        case CEPH_ENTITY_TYPE_OSD: return CEPH_OSDC_PROTOCOL;
        case CEPH_ENTITY_TYPE_MDS: return CEPH_MDSC_PROTOCOL;
        case CEPH_ENTITY_TYPE_MON: return CEPH_MONC_PROTOCOL;
      }
    }
  }
  return 0;
}

void AsyncMessenger::learned_addr(const entity_addr_t &peer_addr_for_me)
{
  // be careful here: multiple threads may block here, and readers of
  // my_inst.addr do NOT hold any lock.

  // this always goes from true -> false under the protection of the
  // mutex.  if it is already false, we need not retake the mutex at
  // all.
  if (!need_addr)
    return ;
  lock.Lock();
  if (need_addr) {
    need_addr = false;
    entity_addr_t t = peer_addr_for_me;
    t.set_port(my_inst.addr.get_port());
    my_inst.addr.addr = t.addr;
    ldout(cct, 1) << __func__ << " learned my addr " << my_inst.addr << dendl;
    _init_local_connection();
  }
  lock.Unlock();
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
    auto conns_it = conns.find(p->peer_addr);
    if (conns_it != conns.end() && conns_it->second == p)
      conns.erase(conns_it);
    accepting_conns.erase(p);
    deleted_conns.erase(it);
    ++num;
  }

  return num;
}
