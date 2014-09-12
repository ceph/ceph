// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <iostream>
#include <fstream>


#include "AsyncMessenger.h"

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
  return *_dout << "-- ";
}

/*******************
 * EventCallBack
 */

class C_handle_accept : public EventCallback {
  Processor *p;

 public:
  C_handle_accept(Processor *p): p(p) {}
  void do_request(int fd, int mask) {
    p->accept();
  }
};


/*******************
 * Processor
 */

int Processor::bind(const entity_addr_t &bind_addr, const set<int>& avoid_ports)
{
  const md_config_t *conf = msgr->cct->_conf;
  // bind to a socket
  ldout(msgr->cct, 10) << __func__ << dendl;

  int family, flags;
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

  // use whatever user specified (if anything)
  entity_addr_t listen_addr = bind_addr;
  listen_addr.set_family(family);

  /* bind to port */
  int rc = -1;
  if (listen_addr.get_port()) {
    // specific port

    // reuse addr+port when possible
    int on = 1;
    rc = ::setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    if (rc < 0) {
      lderr(msgr->cct) << __func__ << " unable to setsockopt: "
                       << cpp_strerror(errno) << dendl;
      return -errno;
    }

    rc = ::bind(listen_sd, (struct sockaddr *) &listen_addr.ss_addr(), listen_addr.addr_size());
    if (rc < 0) {
      lderr(msgr->cct) << __func__ << " unable to bind to " << listen_addr.ss_addr()
                       << ": " << cpp_strerror(errno) << dendl;
      return -errno;
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
                       << "-" << msgr->cct->_conf->ms_bind_port_max
                       << ": " << cpp_strerror(errno) << dendl;
      return -errno;
    }
    ldout(msgr->cct,10) << __func__ << " bound on random port " << listen_addr << dendl;
  }

  // what port did we get?
  socklen_t llen = sizeof(listen_addr.ss_addr());
  rc = getsockname(listen_sd, (sockaddr*)&listen_addr.ss_addr(), &llen);
  if (rc < 0) {
    rc = -errno;
    lderr(msgr->cct) << __func__ << " failed getsockname: " << cpp_strerror(rc) << dendl;
    return rc;
  }

  ldout(msgr->cct, 10) << __func__ << " bound to " << listen_addr << dendl;

  // listen!
  rc = ::listen(listen_sd, 128);
  if (rc < 0) {
    rc = -errno;
    lderr(msgr->cct) << __func__ << " unable to listen on " << listen_addr
                     << ": " << cpp_strerror(rc) << dendl;
    return rc;
  }

  if ((flags = fcntl(listen_sd, F_GETFL, 0)) < 0 ||
      fcntl(listen_sd, F_SETFL, flags | O_NONBLOCK) < 0) {
    rc = -errno;
    lderr(msgr->cct) << __func__ << " unable to setnonblock on " << listen_addr
                     << ": " << cpp_strerror(rc) << dendl;
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
  int r = bind(addr, new_avoid);
  if (r == 0)
    start();
  return r;
}

int Processor::start()
{
  ldout(msgr->cct, 1) << __func__ << " start" << dendl;

  // start thread
  create();
  center->create_event(listen_sd, EVENT_READABLE, new C_handle_accept(this));

  return 0;
}

void Processor::accept()
{
  ldout(msgr->cct, 10) << __func__ << " starting" << dendl;
  // accept
  entity_addr_t addr;
  socklen_t slen = sizeof(addr.ss_addr());
  int sd = ::accept(listen_sd, (sockaddr*)&addr.ss_addr(), &slen);
  if (sd >= 0) {
    ldout(msgr->cct,10) << __func__ << " incoming on sd " << sd << dendl;

    msgr->add_accept(sd);
  } else {
    ldout(msgr->cct,0) << __func__ << " no incoming connection?  sd = " << sd
                       << " errno " << errno << " " << cpp_strerror(errno) << dendl;
  }
}

void *Processor::entry()
{
  ldout(msgr->cct, 10) << __func__ << " starting" << dendl;
  int r;

  while (!done) {
    ldout(msgr->cct,20) << __func__ << " calling poll" << dendl;

    r = center->process_events(500);
    if (r < 0) {
      ldout(msgr->cct,20) << __func__ << " process events failed: "
                          << cpp_strerror(errno) << dendl;
      // TODO do something?
    }
  }

  ldout(msgr->cct,20) << __func__ << " closing" << dendl;
  // don't close socket, in case we start up again?  blech.
  if (listen_sd >= 0) {
    ::close(listen_sd);
    listen_sd = -1;
  }
  ldout(msgr->cct,10) << __func__ << " stopping" << dendl;
  return 0;
}

void Processor::stop()
{
  done = true;
  ldout(msgr->cct, 10) << __func__ << " processor" << dendl;

  center->delete_event(listen_sd, EVENT_READABLE);
  if (listen_sd >= 0) {
    ::shutdown(listen_sd, SHUT_RDWR);
  }

  // wait for thread to stop before closing the socket, to avoid
  // racing against fd re-use.
  if (is_started()) {
    join();
  }

  if (listen_sd >= 0) {
    ::close(listen_sd);
    listen_sd = -1;
  }
  done = false;
}


/*******************
 * AsyncMessenger
 */

AsyncMessenger::AsyncMessenger(CephContext *cct, entity_name_t name,
                               string mname, uint64_t _nonce)
  : SimplePolicyMessenger(cct, name,mname, _nonce),
    processor(this, _nonce, &center),
    lock("AsyncMessenger::lock"),
    nonce(_nonce), did_bind(false),
    global_seq(0),
    cluster_protocol(0),
    local_connection(new AsyncConnection(cct, this)),
    center(cct)
{
  ceph_spin_init(&global_seq_lock);
  _init_local_connection();
}

/**
 * Destroy the AsyncMessenger. Pretty simple since all the work is done
 * elsewhere.
 */
AsyncMessenger::~AsyncMessenger()
{
  assert(!did_bind); // either we didn't bind or we shut down the Processor
}

void AsyncMessenger::ready()
{
  ldout(cct,10) << __func__ << " " << get_myaddr() << dendl;

  lock.Lock();
  if (did_bind)
    processor.start();
  lock.Unlock();
}

int AsyncMessenger::shutdown()
{
  ldout(cct,10) << __func__ << "shutdown " << get_myaddr() << dendl;
  mark_down_all();

  // break ref cycles on the loopback connection
  local_connection->set_priv(NULL);
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
  return processor.rebind(avoid_ports);
}

int AsyncMessenger::start()
{
  lock.Lock();
  ldout(cct,1) << __func__ << " start" << dendl;

  // register at least one entity, first!
  assert(my_inst.name.type() >= 0);

  assert(!started);
  started = true;

  if (!did_bind) {
    my_inst.addr.nonce = nonce;
    _init_local_connection();
  }

  lock.Unlock();

  // FIXME
  center.init(5000);
  return 0;
}

void AsyncMessenger::wait()
{
  lock.Lock();
  if (!started) {
    lock.Unlock();
    return;
  }
  lock.Unlock();

  // done!  clean up.
  if (did_bind) {
    ldout(cct,20) << __func__ << ": stopping processor thread" << dendl;
    processor.stop();
    did_bind = false;
    ldout(cct,20) << __func__ << ": stopped processor thread" << dendl;
  }

  // close all pipes
  lock.Lock();
  {
    ldout(cct, 10) << __func__ << ": closing pipes" << dendl;

    while (!conns.empty()) {
      AsyncConnection *p = conns.begin()->second;
      _stop_conn(p);
    }
  }
  lock.Unlock();

  ldout(cct, 10) << __func__ << ": done." << dendl;
  ldout(cct, 1) << __func__ << " complete." << dendl;
  started = false;
}

AsyncConnection *AsyncMessenger::add_accept(int sd)
{
  lock.Lock();
  AsyncConnection *conn = new AsyncConnection(cct, this);
  conn->accept(sd);
  accepting_conns.insert(conn);
  lock.Unlock();
  return conn;
}

AsyncConnection *AsyncMessenger::create_connect(const entity_addr_t& addr, int type)
{
  assert(lock.is_locked());
  assert(addr != my_inst.addr);

  ldout(cct, 10) << __func__ << " " << addr
                 << ", creating connection and registering" << dendl;

  // create connection
  AsyncConnection *conn = new AsyncConnection(cct, this);
  conn->connect(addr, type);
  assert(!conns.count(addr));
  conns[addr] = conn;

  return conn;
}

ConnectionRef AsyncMessenger::get_connection(const entity_inst_t& dest)
{
  Mutex::Locker l(lock);
  if (my_inst.addr == dest.addr) {
    // local
    return local_connection;
  }

  AsyncConnection *conn = _lookup_conn(dest.addr);
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
  // set envelope
  m->get_header().src = get_myname();

  if (!m->get_priority())
    m->set_priority(get_default_send_priority());

  ldout(cct, 1) << __func__ << "--> " << dest.name << " "
                << dest.addr << " -- " << *m << " -- ?+"
                << m->get_data().length() << " " << m << dendl;

  if (dest.addr == entity_addr_t()) {
    ldout(cct,0) << __func__ <<  " message " << *m
                 << " with empty dest " << dest.addr << dendl;
    m->put();
    return -EINVAL;
  }

  AsyncConnection *conn = _lookup_conn(dest.addr);
  submit_message(m, conn, dest.addr, dest.name.type());
  return 0;
}

void AsyncMessenger::submit_message(Message *m, AsyncConnection *con,
                                    const entity_addr_t& dest_addr, int dest_type)
{
  if (cct->_conf->ms_dump_on_send) {
    m->encode(-1, true);
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
    ldout(cct, 20) << __func__ << " " << *m << " local" << dendl;
    m->set_connection(local_connection.get());
    m->set_recv_stamp(ceph_clock_now(cct));
    ms_fast_preprocess(m);
    if (ms_can_fast_dispatch(m)) {
      ms_fast_dispatch(m);
    } else {
      if (m->get_priority() >= CEPH_MSG_PRIO_LOW) {
        ms_fast_dispatch(m);
      } else {
        ms_deliver_dispatch(m);
      }
    }

    return;
  }

  // remote, no existing pipe.
  const Policy& policy = get_policy(dest_type);
  if (policy.server) {
    ldout(cct, 20) << __func__ << " " << *m << " remote, " << dest_addr
                   << ", lossy server for target type "
                   << ceph_entity_type_name(dest_type) << ", no session, dropping." << dendl;
    m->put();
  } else {
    ldout(cct,20) << __func__ << " " << *m << " remote, " << dest_addr << ", new pipe." << dendl;
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
  for (set<AsyncConnection*>::iterator q = accepting_conns.begin();
       q != accepting_conns.end(); ++q) {
    AsyncConnection *p = *q;
    ldout(cct, 5) << __func__ << " accepting_conn " << p << dendl;
    p->mark_down();
    p->get();
    ms_deliver_handle_reset(p);
  }
  accepting_conns.clear();

  while (!conns.empty()) {
    ceph::unordered_map<entity_addr_t, AsyncConnection*>::iterator it = conns.begin();
    AsyncConnection *p = it->second;
    ldout(cct, 5) << __func__ << " " << it->first << " " << p << dendl;
    conns.erase(it);
    p->mark_down();
    p->get();
    ms_deliver_handle_reset(p);
  }
  lock.Unlock();
}

void AsyncMessenger::mark_down(const entity_addr_t& addr)
{
  lock.Lock();
  AsyncConnection *p = _lookup_conn(addr);
  if (p) {
    ldout(cct, 1) << __func__ << " " << addr << " -- " << p << dendl;
    _stop_conn(p);
    p->get();
    ms_deliver_handle_reset(p);
  } else {
    ldout(cct, 1) << __func__ << " " << addr << " -- pipe dne" << dendl;
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
  lock.Lock();
  entity_addr_t t = peer_addr_for_me;
  t.set_port(my_inst.addr.get_port());
  my_inst.addr.addr = t.addr;
  ldout(cct, 1) << __func__ << " learned my addr " << my_inst.addr << dendl;
  _init_local_connection();
  lock.Unlock();
}
