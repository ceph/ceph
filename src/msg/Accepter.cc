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

#include "Accepter.h"
#include "SimpleMessenger.h"

#include "Message.h"
#include "Pipe.h"

#include "common/debug.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "accepter."


/********************************************
 * Accepter
 */

int Accepter::bind(entity_addr_t &bind_addr, int avoid_port1, int avoid_port2)
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
    char buf[80];
    ldout(msgr->cct,0) << "accepter.bind unable to create socket: "
	    << strerror_r(errno, buf, sizeof(buf)) << dendl;
    cerr << "accepter.bind unable to create socket: "
	 << strerror_r(errno, buf, sizeof(buf)) << std::endl;
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
    ::setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    rc = ::bind(listen_sd, (struct sockaddr *) &listen_addr.ss_addr(), listen_addr.addr_size());
    if (rc < 0) {
      char buf[80];
      ldout(msgr->cct,0) << "accepter.bind unable to bind to " << bind_addr.ss_addr()
	      << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      cerr << "accepter.bind unable to bind to " << bind_addr.ss_addr()
	   << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
      return -errno;
    }
  } else {
    // try a range of ports
    for (int port = CEPH_PORT_START; port <= CEPH_PORT_LAST; port++) {
      if (port == avoid_port1 || port == avoid_port2)
	continue;
      listen_addr.set_port(port);
      rc = ::bind(listen_sd, (struct sockaddr *) &listen_addr.ss_addr(), listen_addr.addr_size());
      if (rc == 0)
	break;
    }
    if (rc < 0) {
      char buf[80];
      ldout(msgr->cct,0) << "accepter.bind unable to bind to " << bind_addr.ss_addr()
	      << " on any port in range " << CEPH_PORT_START << "-" << CEPH_PORT_LAST
	      << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      cerr << "accepter.bind unable to bind to " << bind_addr.ss_addr()
	   << " on any port in range " << CEPH_PORT_START << "-" << CEPH_PORT_LAST
	   << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
      return -errno;
    }
    ldout(msgr->cct,10) << "accepter.bind bound on random port " << listen_addr << dendl;
  }

  // what port did we get?
  socklen_t llen = sizeof(listen_addr.ss_addr());
  getsockname(listen_sd, (sockaddr*)&listen_addr.ss_addr(), &llen);
  
  ldout(msgr->cct,10) << "accepter.bind bound to " << listen_addr << dendl;

  // listen!
  rc = ::listen(listen_sd, 128);
  if (rc < 0) {
    char buf[80];
    ldout(msgr->cct,0) << "accepter.bind unable to listen on " << listen_addr
	    << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    cerr << "accepter.bind unable to listen on " << listen_addr
	 << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
    return -errno;
  }
  
  msgr->set_myaddr(bind_addr);
  if (bind_addr != entity_addr_t())
    msgr->set_need_addr(false);
  else 
    msgr->set_need_addr(true);

  if (msgr->get_myaddr().get_port() == 0) {
    listen_addr.nonce = msgr->get_nonce();
    msgr->set_myaddr(listen_addr);
  }

  msgr->init_local_connection();

  ldout(msgr->cct,1) << "accepter.bind my_inst.addr is " << msgr->get_myaddr()
		     << " need_addr=" << msgr->get_need_addr() << dendl;
  return 0;
}

int Accepter::rebind(int avoid_port)
{
  ldout(msgr->cct,1) << "accepter.rebind avoid " << avoid_port << dendl;
  
  stop();

  entity_addr_t addr = msgr->get_myaddr();
  int old_port = addr.get_port();
  addr.set_port(0);

  ldout(msgr->cct,10) << " will try " << addr << dendl;
  int r = bind(addr, old_port, avoid_port);
  if (r == 0)
    start();
  return r;
}

int Accepter::start()
{
  ldout(msgr->cct,1) << "accepter.start" << dendl;

  // start thread
  create();

  return 0;
}

void *Accepter::entry()
{
  const md_config_t *conf = msgr->cct->_conf;
  ldout(msgr->cct,10) << "accepter starting" << dendl;
  
  int errors = 0;

  char buf[80];

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
    entity_addr_t addr;
    socklen_t slen = sizeof(addr.ss_addr());
    int sd = ::accept(listen_sd, (sockaddr*)&addr.ss_addr(), &slen);
    if (sd >= 0) {
      errors = 0;
      ldout(msgr->cct,10) << "accepted incoming on sd " << sd << dendl;
      
      // disable Nagle algorithm?
      if (conf->ms_tcp_nodelay) {
	int flag = 1;
	int r = ::setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
	if (r < 0)
	  ldout(msgr->cct,0) << "accepter could't set TCP_NODELAY: " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      }
      
      msgr->add_accept_pipe(sd);
    } else {
      ldout(msgr->cct,0) << "accepter no incoming connection?  sd = " << sd
	      << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
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
    ::close(listen_sd);
    listen_sd = -1;
  }
  join();
  done = false;
}




