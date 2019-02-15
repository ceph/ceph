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

#include "include/compat.h"
#include "include/sock_compat.h"
#include <iterator>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <limits.h>
#include <poll.h>

#include "msg/msg_types.h"
#include "msg/Message.h"

#include "Accepter.h"
#include "Pipe.h"
#include "SimpleMessenger.h"

#include "common/debug.h"
#include "common/errno.h"
#include "common/safe_io.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "accepter."


/********************************************
 * Accepter
 */

int Accepter::create_selfpipe(int *pipe_rd, int *pipe_wr) {
  int selfpipe[2];
  if (pipe_cloexec(selfpipe) < 0) {
    int e = errno;
    lderr(msgr->cct) << __func__ << " unable to create the selfpipe: "
                    << cpp_strerror(e) << dendl;
    return -e;
  }
  for (size_t i = 0; i < std::size(selfpipe); i++) {
    int rc = fcntl(selfpipe[i], F_GETFL);
    ceph_assert(rc != -1);
    rc = fcntl(selfpipe[i], F_SETFL, rc | O_NONBLOCK);
    ceph_assert(rc != -1);
  }
  *pipe_rd = selfpipe[0];
  *pipe_wr = selfpipe[1];
  return 0;
}

int Accepter::bind(const entity_addr_t &bind_addr, const set<int>& avoid_ports)
{
  const auto& conf = msgr->cct->_conf;
  // bind to a socket
  ldout(msgr->cct,10) <<  __func__ << dendl;
  
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
  listen_sd = socket_cloexec(family, SOCK_STREAM, 0);
  if (listen_sd < 0) {
    int e = errno;
    lderr(msgr->cct) << __func__ << " unable to create socket: "
		     << cpp_strerror(e) << dendl;
    return -e;
  }
  ldout(msgr->cct,10) <<  __func__ << " socket sd: " << listen_sd << dendl;

  // use whatever user specified (if anything)
  entity_addr_t listen_addr = bind_addr;
  if (listen_addr.get_type() == entity_addr_t::TYPE_NONE) {
    listen_addr.set_type(entity_addr_t::TYPE_LEGACY);
  }
  listen_addr.set_family(family);

  /* bind to port */
  int rc = -1;
  int r = -1;

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
            lderr(msgr->cct) << __func__ << " unable to setsockopt: "
                             << cpp_strerror(errno) << dendl;
            r = -errno;
            continue;
        }

        rc = ::bind(listen_sd, listen_addr.get_sockaddr(),
		    listen_addr.get_sockaddr_len());
        if (rc < 0) {
            lderr(msgr->cct) << __func__ << " unable to bind to " << listen_addr
			     << ": " << cpp_strerror(errno) << dendl;
            r = -errno;
            continue;
        }
    } else {
        // try a range of ports
        for (int port = msgr->cct->_conf->ms_bind_port_min; 
		port <= msgr->cct->_conf->ms_bind_port_max; port++) {
            if (avoid_ports.count(port))
                continue;

            listen_addr.set_port(port);
            rc = ::bind(listen_sd, listen_addr.get_sockaddr(),
			listen_addr.get_sockaddr_len());
            if (rc == 0)
                break;
        }
        if (rc < 0) {
            lderr(msgr->cct) <<  __func__ << "  unable to bind to " << listen_addr
                             << " on any port in range " << msgr->cct->_conf->ms_bind_port_min
                             << "-" << msgr->cct->_conf->ms_bind_port_max
                             << ": " << cpp_strerror(errno)
                             << dendl;
            r = -errno;
            // Clear port before retry, otherwise we shall fail again.
            listen_addr.set_port(0); 
            continue;
        }
        ldout(msgr->cct,10) << __func__ << " bound on random port " 
			    << listen_addr << dendl;
    }

    if (rc == 0)
        break;
  }

  // It seems that binding completely failed, return with that exit status
  if (rc < 0) {
      lderr(msgr->cct) << __func__ << " was unable to bind after " 
		       << conf->ms_bind_retry_count << " attempts: " 
		       << cpp_strerror(errno) << dendl;
      ::close(listen_sd);
      listen_sd = -1;
      return r;
  }

  // what port did we get?
  sockaddr_storage ss;
  socklen_t llen = sizeof(ss);
  rc = getsockname(listen_sd, (sockaddr*)&ss, &llen);
  if (rc < 0) {
    rc = -errno;
    lderr(msgr->cct) << __func__ << " failed getsockname: " 
		     << cpp_strerror(rc) << dendl;
    ::close(listen_sd);
    listen_sd = -1;
    return rc;
  }
  listen_addr.set_sockaddr((sockaddr*)&ss);
  
  if (msgr->cct->_conf->ms_tcp_rcvbuf) {
    int size = msgr->cct->_conf->ms_tcp_rcvbuf;
    rc = ::setsockopt(listen_sd, SOL_SOCKET, SO_RCVBUF, 
			(void*)&size, sizeof(size));
    if (rc < 0)  {
      rc = -errno;
      lderr(msgr->cct) <<  __func__ << "  failed to set SO_RCVBUF to " 
		       << size << ": " << cpp_strerror(rc) << dendl;
      ::close(listen_sd);
      listen_sd = -1;
      return rc;
    }
  }

  ldout(msgr->cct,10) <<  __func__ << " bound to " << listen_addr << dendl;

  // listen!
  rc = ::listen(listen_sd, msgr->cct->_conf->ms_tcp_listen_backlog);
  if (rc < 0) {
    rc = -errno;
    lderr(msgr->cct) <<  __func__ << " unable to listen on " << listen_addr
		     << ": " << cpp_strerror(rc) << dendl;
    ::close(listen_sd);
    listen_sd = -1;
    return rc;
  }
  
  msgr->set_myaddrs(entity_addrvec_t(bind_addr));
  if (bind_addr != entity_addr_t() &&
      !bind_addr.is_blank_ip())
    msgr->learned_addr(bind_addr);
  else
    ceph_assert(msgr->get_need_addr());  // should still be true.

  if (msgr->get_myaddr_legacy().get_port() == 0) {
    msgr->set_myaddrs(entity_addrvec_t(listen_addr));
  }
  entity_addr_t addr = msgr->get_myaddr_legacy();
  addr.nonce = nonce;
  msgr->set_myaddrs(entity_addrvec_t(addr));

  msgr->init_local_connection();

  rc = create_selfpipe(&shutdown_rd_fd, &shutdown_wr_fd);
  if (rc < 0) {
    lderr(msgr->cct) <<  __func__ << " unable to create signalling pipe " << listen_addr
		     << ": " << cpp_strerror(rc) << dendl;
    return rc;
  }

  ldout(msgr->cct,1) <<  __func__ << " my_addrs " << *msgr->my_addrs
		     << " my_addr " << msgr->my_addr
		     << " need_addr=" << msgr->get_need_addr() << dendl;
  return 0;
}

int Accepter::rebind(const set<int>& avoid_ports)
{
  ldout(msgr->cct,1) << __func__ << " avoid " << avoid_ports << dendl;
  
  entity_addr_t addr = msgr->get_myaddr_legacy();
  set<int> new_avoid = avoid_ports;
  new_avoid.insert(addr.get_port());
  addr.set_port(0);

  // adjust the nonce; we want our entity_addr_t to be truly unique.
  nonce += 1000000;
  entity_addrvec_t newaddrs = *msgr->my_addrs;
  newaddrs.v[0].nonce = nonce;
  msgr->set_myaddrs(newaddrs);
  ldout(msgr->cct,10) << __func__ << " new nonce " << nonce << " and addr "
			<< msgr->my_addr << dendl;

  ldout(msgr->cct,10) << " will try " << addr << " and avoid ports " << new_avoid << dendl;
  int r = bind(addr, new_avoid);
  if (r == 0)
    start();
  return r;
}

int Accepter::start()
{
  ldout(msgr->cct,1) << __func__ << dendl;

  // start thread
  create("ms_accepter");

  return 0;
}

void *Accepter::entry()
{
  ldout(msgr->cct,1) << __func__ << " start" << dendl;
  
  int errors = 0;

  struct pollfd pfd[2];
  memset(pfd, 0, sizeof(pfd));

  pfd[0].fd = listen_sd;
  pfd[0].events = POLLIN | POLLERR | POLLNVAL | POLLHUP;
  pfd[1].fd = shutdown_rd_fd;
  pfd[1].events = POLLIN | POLLERR | POLLNVAL | POLLHUP;
  while (!done) {
    ldout(msgr->cct,20) << __func__ << " calling poll for sd:" << listen_sd << dendl;
    int r = poll(pfd, 2, -1);
    if (r < 0) {
      if (errno == EINTR) {
        continue;
      }
      ldout(msgr->cct,1) << __func__ << " poll got error"  
 			  << " errno " << errno << " " << cpp_strerror(errno) << dendl;
      ceph_abort();
    }
    ldout(msgr->cct,10) << __func__ << " poll returned oke: " << r << dendl;
    ldout(msgr->cct,20) << __func__ <<  " pfd.revents[0]=" << pfd[0].revents << dendl;
    ldout(msgr->cct,20) << __func__ <<  " pfd.revents[1]=" << pfd[1].revents << dendl;

    if (pfd[0].revents & (POLLERR | POLLNVAL | POLLHUP)) {
      ldout(msgr->cct,1) << __func__ << " poll got errors in revents "  
 			 <<  pfd[0].revents << dendl;
      ceph_abort();
    }
    if (pfd[1].revents & (POLLIN | POLLERR | POLLNVAL | POLLHUP)) {
      // We got "signaled" to exit the poll
      // clean the selfpipe
      char ch;
      if (::read(shutdown_rd_fd, &ch, sizeof(ch)) == -1) {
        if (errno != EAGAIN)
          ldout(msgr->cct,1) << __func__ << " Cannot read selfpipe: "
 			      << " errno " << errno << " " << cpp_strerror(errno) << dendl;
        }
      break;
    }
    if (done) break;

    // accept
    sockaddr_storage ss;
    socklen_t slen = sizeof(ss);
    int sd = accept_cloexec(listen_sd, (sockaddr*)&ss, &slen);
    if (sd >= 0) {
      errors = 0;
      ldout(msgr->cct,10) << __func__ << " incoming on sd " << sd << dendl;
      
      msgr->add_accept_pipe(sd);
    } else {
      int e = errno;
      ldout(msgr->cct,0) << __func__ << " no incoming connection?  sd = " << sd
	      << " errno " << e << " " << cpp_strerror(e) << dendl;
      if (++errors > msgr->cct->_conf->ms_max_accept_failures) {
        lderr(msgr->cct) << "accetper has encoutered enough errors, just do ceph_abort()." << dendl;
        ceph_abort();
      }
    }
  }

  ldout(msgr->cct,20) << __func__ << " closing" << dendl;
  // socket is closed right after the thread has joined.
  // closing it here might race
  if (shutdown_rd_fd >= 0) {
    ::close(shutdown_rd_fd);
    shutdown_rd_fd = -1;
  }

  ldout(msgr->cct,10) << __func__ << " stopping" << dendl;
  return 0;
}

void Accepter::stop()
{
  done = true;
  ldout(msgr->cct,10) << __func__ << " accept listening on: " << listen_sd << dendl;

  if (shutdown_wr_fd < 0)
    return;

  // Send a byte to the shutdown pipe that the thread is listening to
  char ch = 0x0;
  int ret = safe_write(shutdown_wr_fd, &ch, sizeof(ch));
  if (ret < 0) {
    ldout(msgr->cct,1) << __func__ << " close failed: "
             << " errno " << errno << " " << cpp_strerror(errno) << dendl;
  } else {
    ldout(msgr->cct,15) << __func__ << " signaled poll" << dendl;
  }
  VOID_TEMP_FAILURE_RETRY(close(shutdown_wr_fd));
  shutdown_wr_fd = -1;

  // wait for thread to stop before closing the socket, to avoid
  // racing against fd re-use.
  if (is_started()) {
    ldout(msgr->cct,5) << __func__ << " wait for thread to join." << dendl;
    join();
  }

  if (listen_sd >= 0) {
    if (::close(listen_sd) < 0) {
      ldout(msgr->cct,1) << __func__ << " close listen_sd failed: "
	      << " errno " << errno << " " << cpp_strerror(errno) << dendl;
    }
    listen_sd = -1;
  }
  if (shutdown_rd_fd >= 0) {
    if (::close(shutdown_rd_fd) < 0) {
      ldout(msgr->cct,1) << __func__ << " close shutdown_rd_fd failed: "
	      << " errno " << errno << " " << cpp_strerror(errno) << dendl;
    }
    shutdown_rd_fd = -1;
  }
  done = false;
}




