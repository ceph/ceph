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

#include <sys/socket.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <limits.h>
#include <poll.h>

#include "msg/Message.h"

#include "Accepter.h"
#include "Pipe.h"
#include "SimpleMessenger.h"

#include "common/debug.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "accepter."


/********************************************
 * Accepter
 */

int Accepter::bind(const entity_addr_t &bind_addr, const set<int>& avoid_ports)
{
  const md_config_t *conf = msgr->cct->_conf;
  // bind to a socket
  ldout(msgr->cct,10) << "accepter.bind" << dendl;
  
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
    lderr(msgr->cct) << "accepter.bind unable to create socket: "
		     << cpp_strerror(errno) << dendl;
    return -errno;
  }

  // use whatever user specified (if anything)
  entity_addr_t listen_addr = bind_addr;
  listen_addr.set_family(family);

  /* bind to port */
  int rc = -1;
  int r = -1;

  for (int i = 0; i < conf->ms_bind_retry_count; i++) {

    if (i > 0) {
        lderr(msgr->cct) << "accepter.bind was unable to bind. Trying again in " << conf->ms_bind_retry_delay << " seconds " << dendl;
        sleep(conf->ms_bind_retry_delay);
    }

    if (listen_addr.get_port()) {
        // specific port

        // reuse addr+port when possible
        int on = 1;
        rc = ::setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        if (rc < 0) {
            lderr(msgr->cct) << "accepter.bind unable to setsockopt: "
                             << cpp_strerror(errno) << dendl;
            r = -errno;
            continue;
        }

        rc = ::bind(listen_sd, listen_addr.get_sockaddr(),
		    listen_addr.get_sockaddr_len());
        if (rc < 0) {
            lderr(msgr->cct) << "accepter.bind unable to bind to " << listen_addr
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
            rc = ::bind(listen_sd, listen_addr.get_sockaddr(),
			listen_addr.get_sockaddr_len());
            if (rc == 0)
                break;
        }
        if (rc < 0) {
            lderr(msgr->cct) << "accepter.bind unable to bind to " << listen_addr
                             << " on any port in range " << msgr->cct->_conf->ms_bind_port_min
                             << "-" << msgr->cct->_conf->ms_bind_port_max
                             << ": " << cpp_strerror(errno)
                             << dendl;
            r = -errno;
            listen_addr.set_port(0); //Clear port before retry, otherwise we shall fail again.
            continue;
        }
        ldout(msgr->cct,10) << "accepter.bind bound on random port " << listen_addr << dendl;
    }

    if (rc == 0)
        break;
  }

  // It seems that binding completely failed, return with that exit status
  if (rc < 0) {
      lderr(msgr->cct) << "accepter.bind was unable to bind after " << conf->ms_bind_retry_count << " attempts: " << cpp_strerror(errno) << dendl;
      return r;
  }

  // what port did we get?
  sockaddr_storage ss;
  socklen_t llen = sizeof(ss);
  rc = getsockname(listen_sd, (sockaddr*)&ss, &llen);
  if (rc < 0) {
    rc = -errno;
    lderr(msgr->cct) << "accepter.bind failed getsockname: " << cpp_strerror(rc) << dendl;
    return rc;
  }
  listen_addr.set_sockaddr((sockaddr*)&ss);
  
  if (msgr->cct->_conf->ms_tcp_rcvbuf) {
    int size = msgr->cct->_conf->ms_tcp_rcvbuf;
    rc = ::setsockopt(listen_sd, SOL_SOCKET, SO_RCVBUF, (void*)&size, sizeof(size));
    if (rc < 0)  {
      rc = -errno;
      lderr(msgr->cct) << "accepter.bind failed to set SO_RCVBUF to " << size << ": " << cpp_strerror(r) << dendl;
      return rc;
    }
  }

  ldout(msgr->cct,10) << "accepter.bind bound to " << listen_addr << dendl;

  // listen!
  rc = ::listen(listen_sd, 128);
  if (rc < 0) {
    rc = -errno;
    lderr(msgr->cct) << "accepter.bind unable to listen on " << listen_addr
		     << ": " << cpp_strerror(rc) << dendl;
    return rc;
  }
  
  msgr->set_myaddr(bind_addr);
  if (bind_addr != entity_addr_t())
    msgr->learned_addr(bind_addr);
  else
    assert(msgr->get_need_addr());  // should still be true.

  if (msgr->get_myaddr().get_port() == 0) {
    msgr->set_myaddr(listen_addr);
  }
  entity_addr_t addr = msgr->get_myaddr();
  addr.nonce = nonce;
  msgr->set_myaddr(addr);

  msgr->init_local_connection();

  ldout(msgr->cct,1) << "accepter.bind my_inst.addr is " << msgr->get_myaddr()
		     << " need_addr=" << msgr->get_need_addr() << dendl;
  return 0;
}

int Accepter::rebind(const set<int>& avoid_ports)
{
  ldout(msgr->cct,1) << "accepter.rebind avoid " << avoid_ports << dendl;
  
  entity_addr_t addr = msgr->get_myaddr();
  set<int> new_avoid = avoid_ports;
  new_avoid.insert(addr.get_port());
  addr.set_port(0);

  // adjust the nonce; we want our entity_addr_t to be truly unique.
  nonce += 1000000;
  msgr->my_inst.addr.nonce = nonce;
  ldout(msgr->cct,10) << " new nonce " << nonce << " and inst " << msgr->my_inst << dendl;

  ldout(msgr->cct,10) << " will try " << addr << " and avoid ports " << new_avoid << dendl;
  int r = bind(addr, new_avoid);
  if (r == 0)
    start();
  return r;
}

int Accepter::start()
{
  ldout(msgr->cct,1) << "accepter.start" << dendl;

  // start thread
  create("ms_accepter");

  return 0;
}

void *Accepter::entry()
{
  ldout(msgr->cct,10) << "accepter starting" << dendl;
  
  int errors = 0;

  struct pollfd pfd;
  pfd.fd = listen_sd;
  pfd.events = POLLIN | POLLERR | POLLNVAL | POLLHUP;
  while (!done) {
    ldout(msgr->cct,20) << "accepter calling poll" << dendl;
    int r = poll(&pfd, 1, -1);
    if (r < 0)
      break;
    ldout(msgr->cct,20) << "accepter poll got " << r << dendl;

    if (pfd.revents & (POLLERR | POLLNVAL | POLLHUP))
      break;

    ldout(msgr->cct,10) << "pfd.revents=" << pfd.revents << dendl;
    if (done) break;

    // accept
    sockaddr_storage ss;
    socklen_t slen = sizeof(ss);
    int sd = ::accept(listen_sd, (sockaddr*)&ss, &slen);
    if (sd >= 0) {
      errors = 0;
      ldout(msgr->cct,10) << "accepted incoming on sd " << sd << dendl;
      
      msgr->add_accept_pipe(sd);
    } else {
      ldout(msgr->cct,0) << "accepter no incoming connection?  sd = " << sd
	      << " errno " << errno << " " << cpp_strerror(errno) << dendl;
      if (++errors > 4)
	break;
    }
  }

  ldout(msgr->cct,20) << "accepter closing" << dendl;
  // don't close socket, in case we start up again?  blech.
  if (listen_sd >= 0) {
    ::close(listen_sd);
    listen_sd = -1;
  }
  ldout(msgr->cct,10) << "accepter stopping" << dendl;
  return 0;
}

void Accepter::stop()
{
  done = true;
  ldout(msgr->cct,10) << "stop accepter" << dendl;

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




