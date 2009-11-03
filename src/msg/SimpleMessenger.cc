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

#include "SimpleMessenger.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <limits.h>
#include <sys/user.h>

#include "config.h"

#include "messages/MGenericMessage.h"

#include <netdb.h>

#include <iostream>
#include <fstream>

#include "common/Timer.h"

#define DOUT_SUBSYS ms
#undef dout_prefix
#define dout_prefix _prefix(rank)
static ostream& _prefix(SimpleMessenger *rank) {
  return *_dout << dbeginl << pthread_self() << " -- " << rank->rank_addr << " ";
}


#include "tcp.cc"


// help find socket resource leaks
//static int sockopen = 0;
#define closed_socket() //dout(20) << "closed_socket " << --sockopen << dendl;
#define opened_socket() //dout(20) << "opened_socket " << ++sockopen << dendl;


#ifdef DARWIN
sig_t old_sigint_handler = 0;
#else
sighandler_t old_sigint_handler = 0;
#endif

/********************************************
 * Accepter
 */

void noop_signal_handler(int s)
{
  //dout(0) << "blah_handler got " << s << dendl;
}

int SimpleMessenger::Accepter::bind(int64_t force_nonce)
{
  // bind to a socket
  dout(10) << "accepter.bind" << dendl;
  
  // use whatever user specified (if anything)
  sockaddr_in listen_addr = g_my_addr.in4_addr();
  listen_addr.sin_family = AF_INET;

  /* socket creation */
  listen_sd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_sd < 0) {
    char buf[80];
    derr(0) << "accepter.bind unable to create socket: "
	    << strerror_r(errno, buf, sizeof(buf)) << dendl;
    return -errno;
  }
  opened_socket();

  int on = 1;
  ::setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  
  /* bind to port */
  int rc;
  if (listen_addr.sin_port) {
    // specific port
    rc = ::bind(listen_sd, (struct sockaddr *) &listen_addr, sizeof(listen_addr));
    if (rc < 0) {
      char buf[80];
      derr(0) << "accepter.bind unable to bind to " << g_my_addr.ss_addr()
	      << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      return -errno;
    }
  } else {
    // try a range of ports
    for (int port = CEPH_PORT_START; port <= CEPH_PORT_LAST; port++) {
      listen_addr.sin_port = htons(port);
      rc = ::bind(listen_sd, (struct sockaddr *) &listen_addr, sizeof(listen_addr));
      if (rc == 0)
	break;
    }
    if (rc < 0) {
      char buf[80];
      derr(0) << "accepter.bind unable to bind to " << g_my_addr.ss_addr()
	      << " on any port in range " << CEPH_PORT_START << "-" << CEPH_PORT_LAST
	      << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      return -errno;
    }
  }

  // what port did we get?
  socklen_t llen = sizeof(listen_addr);
  getsockname(listen_sd, (sockaddr*)&listen_addr, &llen);
  
  dout(10) << "accepter.bind bound to " << listen_addr << dendl;

  // listen!
  rc = ::listen(listen_sd, 128);
  if (rc < 0) {
    char buf[80];
    derr(0) << "accepter.bind unable to listen on " << g_my_addr.ss_addr()
	    << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    return -errno;
  }
  
  rank->rank_addr = g_my_addr;
  if (rank->rank_addr != entity_addr_t())
    rank->need_addr = false;
  else 
    rank->need_addr = true;

  if (rank->rank_addr.get_port() == 0) {
    rank->rank_addr.in4_addr() = listen_addr;
    if (force_nonce >= 0)
      rank->rank_addr.nonce = force_nonce;
    else
      rank->rank_addr.nonce = getpid(); // FIXME: pid might not be best choice here.
  }
  rank->rank_addr.erank = 0;

  dout(1) << "accepter.bind rank_addr is " << rank->rank_addr << " need_addr=" << rank->need_addr << dendl;
  rank->did_bind = true;
  return 0;
}

int SimpleMessenger::Accepter::start()
{
  dout(1) << "accepter.start" << dendl;

  // set a harmless handle for SIGUSR1 (we'll use it to stop the accepter)
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = noop_signal_handler;
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGUSR1, &sa, NULL);

  // start thread
  create();

  return 0;
}

void *SimpleMessenger::Accepter::entry()
{
  dout(10) << "accepter starting" << dendl;
  
  fd_set fds;
  int errors = 0;

  sigset_t sigmask, sigempty;
  sigemptyset(&sigmask);
  sigaddset(&sigmask, SIGUSR1);
  sigemptyset(&sigempty);

  // block SIGUSR1
  pthread_sigmask(SIG_BLOCK, &sigmask, NULL);

  char buf[80];

  while (!done) {
    FD_ZERO(&fds);
    FD_SET(listen_sd, &fds);
    dout(20) << "accepter calling select" << dendl;
    int r = ::pselect(listen_sd+1, &fds, 0, &fds, 0, &sigempty);  // unblock SIGUSR1 inside select()
    dout(20) << "accepter select got " << r << dendl;
    
    if (done) break;

    // accept
    struct sockaddr_in addr;
    socklen_t slen = sizeof(addr);
    int sd = ::accept(listen_sd, (sockaddr*)&addr, &slen);
    if (sd >= 0) {
      errors = 0;
      opened_socket();
      dout(10) << "accepted incoming on sd " << sd << dendl;
      
      // disable Nagle algorithm?
      if (g_conf.ms_tcp_nodelay) {
	int flag = 1;
	int r = ::setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
	if (r < 0)
	  dout(0) << "accepter could't set TCP_NODELAY: " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      }
      
      rank->lock.Lock();
      if (rank->num_local > 0) {
	Pipe *p = new Pipe(rank, Pipe::STATE_ACCEPTING);
	p->sd = sd;
	p->start_reader();
	rank->pipes.insert(p);
      }
      rank->lock.Unlock();
    } else {
      dout(0) << "accepter no incoming connection?  sd = " << sd << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      if (++errors > 4)
	break;
    }
  }

  // unblock SIGUSR1
  pthread_sigmask(SIG_UNBLOCK, &sigmask, NULL);

  dout(20) << "accepter closing" << dendl;
  // don't close socket, in case we start up again?  blech.
  if (listen_sd >= 0) {
    ::close(listen_sd);
    listen_sd = -1;
    closed_socket();
  }
  dout(10) << "accepter stopping" << dendl;
  return 0;
}

void SimpleMessenger::Accepter::stop()
{
  done = true;
  dout(10) << "stop sending SIGUSR1" << dendl;
  this->kill(SIGUSR1);
  join();
  done = false;
}






/**********************************
 * Endpoint
 */

void SimpleMessenger::Endpoint::dispatch_entry()
{
  lock.Lock();
  while (!stop) {
    if (!dispatch_queue.empty()) {
      list<Message*> ls;

      // take highest priority message off the queue
      map<int, list<Message*> >::reverse_iterator p = dispatch_queue.rbegin();
      ls.push_back(p->second.front());
      p->second.pop_front();
      if (p->second.empty())
	dispatch_queue.erase(p->first);
      qlen--;

      lock.Unlock();
      {
        // deliver
        while (!ls.empty()) {
	  if (stop) {
	    dout(1) << "dispatch: stop=true, discarding " << ls.size() 
		    << " messages in dispatch queue" << dendl;
	    break;
	  }
          Message *m = ls.front();
          ls.pop_front();
	  if ((long)m == D_BAD_REMOTE_RESET) {
	    lock.Lock();
	    Connection *con = remote_reset_q.front();
	    remote_reset_q.pop_front();
	    lock.Unlock();
	    ms_deliver_handle_remote_reset(con);
	    con->put();
 	  } else if ((long)m == D_CONNECT) {
	    lock.Lock();
	    Connection *con = connect_q.front();
	    connect_q.pop_front();
	    lock.Unlock();
	    ms_deliver_handle_connect(con);
	    con->put();
 	  } else if ((long)m == D_BAD_RESET) {
	    lock.Lock();
	    Connection *con = reset_q.front();
	    reset_q.pop_front();
	    lock.Unlock();
	    ms_deliver_handle_reset(con);
	    con->put();
	  } else {
	    dout(1) << "<== " << m->get_source_inst()
		    << " " << m->get_seq()
		    << " ==== " << *m
		    << " ==== " << m->get_payload().length() << "+" << m->get_middle().length()
		    << "+" << m->get_data().length()
		    << " (" << m->get_footer().front_crc << " " << m->get_footer().middle_crc
		    << " " << m->get_footer().data_crc << ")"
		    << " " << m 
		    << dendl;
	    ms_deliver_dispatch(m);
	    dout(20) << "done calling dispatch on " << m << dendl;
	  }
        }
      }
      lock.Lock();
      continue;
    }
    cond.Wait(lock);
  }
  lock.Unlock();
  dout(15) << "dispatch: ending loop " << dendl;

  // deregister
  rank->unregister_entity(this);
  put();
}

void SimpleMessenger::Endpoint::ready()
{
  dout(10) << "ready " << get_myaddr() << dendl;
  assert(!dispatch_thread.is_started());
  get();
  dispatch_thread.create();
}


int SimpleMessenger::Endpoint::shutdown()
{
  dout(10) << "shutdown " << get_myaddr() << dendl;
  
  // stop my dispatch thread
  if (dispatch_thread.am_self()) {
    dout(10) << "shutdown i am dispatch, setting stop flag" << dendl;
    stop = true;
  } else {
    dout(10) << "shutdown i am not dispatch, setting stop flag and joining thread." << dendl;
    lock.Lock();
    stop = true;
    cond.Signal();
    lock.Unlock();
  }
  return 0;
}

void SimpleMessenger::Endpoint::suicide()
{
  dout(10) << "suicide " << get_myaddr() << dendl;
  shutdown();
  // hmm, or exit(0)?
}

void SimpleMessenger::Endpoint::prepare_dest(const entity_inst_t& inst)
{
  rank->lock.Lock();
  {
    if (rank->rank_pipe.count(inst.addr) == 0)
      rank->connect_rank(inst.addr, inst.name.type());
  }
  rank->lock.Unlock();
}

int SimpleMessenger::Endpoint::send_message(Message *m, entity_inst_t dest)
{
  // set envelope
  m->get_header().src = get_myinst();
  m->get_header().orig_src = m->get_header().src;

  if (!m->get_priority()) m->set_priority(get_default_send_priority());
 
  dout(1) << "--> " << dest.name << " " << dest.addr
          << " -- " << *m
    	  << " -- ?+" << m->get_data().length()
	  << " " << m 
	  << dendl;

  rank->submit_message(m, dest);

  return 0;
}

int SimpleMessenger::Endpoint::forward_message(Message *m, entity_inst_t dest)
{
  // set envelope
  m->get_header().src = get_myinst();

  if (!m->get_priority()) m->set_priority(get_default_send_priority());
 
  dout(1) << "**> " << dest.name << " " << dest.addr
          << " -- " << *m
    	  << " -- ?+" << m->get_data().length()
	  << " " << m 
          << dendl;

  rank->submit_message(m, dest);

  return 0;
}



int SimpleMessenger::Endpoint::lazy_send_message(Message *m, entity_inst_t dest)
{
  // set envelope
  m->get_header().src = get_myinst();
  m->get_header().orig_src = m->get_header().src;

  if (!m->get_priority()) m->set_priority(get_default_send_priority());
 
  dout(1) << "lazy "
	  << " --> " << dest.name << " " << dest.addr
          << " -- " << *m
    	  << " -- ?+" << m->get_data().length()
	  << " " << m 
          << dendl;

  rank->submit_message(m, dest, true);

  return 0;
}

int SimpleMessenger::Endpoint::send_keepalive(entity_inst_t dest)
{
  rank->send_keepalive(dest);
  return 0;
}



void SimpleMessenger::Endpoint::mark_down(entity_addr_t a)
{
  rank->mark_down(a);
}


entity_addr_t SimpleMessenger::Endpoint::get_myaddr()
{
  entity_addr_t a = rank->rank_addr;
  a.erank = my_rank;
  return a;  
}




/**************************************
 * Pipe
 */

#undef dout_prefix
#define dout_prefix _pipe_prefix()
ostream& SimpleMessenger::Pipe::_pipe_prefix() {
  return *_dout << dbeginl << pthread_self()
		<< " -- " << rank->rank_addr << " >> " << peer_addr << " pipe(" << this
		<< " sd=" << sd
		<< " pgs=" << peer_global_seq
		<< " cs=" << connect_seq
		<< " l=" << policy.lossy
		<< ").";
}

static int get_proto_version(int my_type, int peer_type, bool connect)
{
  // set reply protocol version
  if (peer_type == my_type) {
    // internal
    switch (my_type) {
    case CEPH_ENTITY_TYPE_OSD: return CEPH_OSD_PROTOCOL;
    case CEPH_ENTITY_TYPE_MDS: return CEPH_MDS_PROTOCOL;
    case CEPH_ENTITY_TYPE_MON: return CEPH_MON_PROTOCOL;
    }
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


int SimpleMessenger::Pipe::accept()
{
  dout(10) << "accept" << dendl;

  // my creater gave me sd via accept()
  assert(state == STATE_ACCEPTING);
  
  // announce myself.
  int rc = tcp_write(sd, CEPH_BANNER, strlen(CEPH_BANNER));
  if (rc < 0) {
    dout(10) << "accept couldn't write banner" << dendl;
    state = STATE_CLOSED;
    return -1;
  }

  // and my addr
  bufferlist addrs;
  ::encode(rank->rank_addr, addrs);

  // and peer's socket addr (they might not know their ip)
  entity_addr_t socket_addr;
  socklen_t len = sizeof(socket_addr.ss_addr());
  int r = ::getpeername(sd, (sockaddr*)&socket_addr.ss_addr(), &len);
  if (r < 0) {
    char buf[80];
    dout(0) << "accept failed to getpeername " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  ::encode(socket_addr, addrs);

  rc = tcp_write(sd, addrs.c_str(), addrs.length());
  if (rc < 0) {
    dout(10) << "accept couldn't write my+peer addr" << dendl;
    state = STATE_CLOSED;
    return -1;
  }

  dout(10) << "accept sd=" << sd << dendl;
  
  // identify peer
  char banner[strlen(CEPH_BANNER)+1];
  rc = tcp_read(sd, banner, strlen(CEPH_BANNER));
  if (rc < 0) {
    dout(10) << "accept couldn't read banner" << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  if (memcmp(banner, CEPH_BANNER, strlen(CEPH_BANNER))) {
    banner[strlen(CEPH_BANNER)] = 0;
    dout(1) << "accept peer sent bad banner '" << banner << "' (should be '" << CEPH_BANNER << "')" << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  bufferlist addrbl;
  {
    bufferptr tp(sizeof(peer_addr));
    addrbl.push_back(tp);
  }
  rc = tcp_read(sd, addrbl.c_str(), addrbl.length());
  if (rc < 0) {
    dout(10) << "accept couldn't read peer_addr" << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  {
    bufferlist::iterator ti = addrbl.begin();
    ::decode(peer_addr, ti);
  }

  dout(10) << "accept peer addr is " << peer_addr << dendl;
  if (peer_addr.is_blank_addr()) {
    // peer apparently doesn't know what ip they have; figure it out for them.
    int port = peer_addr.get_port();
    peer_addr.addr = socket_addr.addr;
    peer_addr.set_port(port);
    dout(0) << "accept peer addr is really " << peer_addr
	    << " (socket is " << socket_addr << ")" << dendl;
  }
  set_peer_addr(peer_addr);  // so that connection_state gets set up
  
  ceph_msg_connect connect;
  ceph_msg_connect_reply reply;
  Pipe *existing = 0;
  
  // this should roughly mirror pseudocode at
  //  http://ceph.newdream.net/wiki/Messaging_protocol

  while (1) {
    rc = tcp_read(sd, (char*)&connect, sizeof(connect));
    if (rc < 0) {
      dout(10) << "accept couldn't read connect" << dendl;
      goto fail_unlocked;
    }
    dout(20) << "accept got peer connect_seq " << connect.connect_seq
	     << " global_seq " << connect.global_seq
	     << dendl;
    
    rank->lock.Lock();

    // note peer's type, flags
    set_peer_type(connect.host_type);
    policy = rank->get_policy(connect.host_type);
    dout(10) << "accept of host_type " << connect.host_type
	     << ", policy.lossy=" << policy.lossy
	     << dendl;

    memset(&reply, 0, sizeof(reply));
    reply.protocol_version = get_proto_version(rank->my_type, peer_type, false);

    // mismatch?
    dout(10) << "accept my proto " << reply.protocol_version
	     << ", their proto " << connect.protocol_version << dendl;
    if (connect.protocol_version != reply.protocol_version) {
      reply.tag = CEPH_MSGR_TAG_BADPROTOVER;
      rank->lock.Unlock();
      goto reply;
    }
    
    // existing?
    if (rank->rank_pipe.count(peer_addr)) {
      existing = rank->rank_pipe[peer_addr];
      existing->lock.Lock();

      if (connect.global_seq < existing->peer_global_seq) {
	dout(10) << "accept existing " << existing << ".gseq " << existing->peer_global_seq
		 << " > " << connect.global_seq << ", RETRY_GLOBAL" << dendl;
	reply.tag = CEPH_MSGR_TAG_RETRY_GLOBAL;
	reply.global_seq = existing->peer_global_seq;  // so we can send it below..
	existing->lock.Unlock();
	rank->lock.Unlock();
	goto reply;
      } else {
	dout(10) << "accept existing " << existing << ".gseq " << existing->peer_global_seq
		 << " <= " << connect.global_seq << ", looks ok" << dendl;
      }
      
      if (existing->policy.lossy) {
	dout(-10) << "accept replacing existing (lossy) channel (new one lossy="
		  << policy.lossy << ")" << dendl;
	existing->was_session_reset();
	goto replace;
      }
      /*if (lossy_rx) {
	if (existing->state == STATE_STANDBY) {
	  dout(-10) << "accept incoming lossy connection, kicking outgoing lossless "
		    << existing << dendl;
	  existing->state = STATE_CONNECTING;
	  existing->cond.Signal();
	} else {
	  dout(-10) << "accept incoming lossy connection, our lossless " << existing
		    << " has state " << existing->state << ", doing nothing" << dendl;
	}
	existing->lock.Unlock();
	goto fail;
	}*/

      dout(-10) << "accept connect_seq " << connect.connect_seq
		<< " vs existing " << existing->connect_seq
		<< " state " << existing->state << dendl;

      if (connect.connect_seq < existing->connect_seq) {
	if (connect.connect_seq == 0) {
	  dout(-10) << "accept peer reset, then tried to connect to us, replacing" << dendl;
	  existing->was_session_reset(); // this resets out_queue, msg_ and connect_seq #'s
	  goto replace;
	} else {
	  // old attempt, or we sent READY but they didn't get it.
	  dout(10) << "accept existing " << existing << ".cseq " << existing->connect_seq
		   << " > " << connect.connect_seq << ", RETRY_SESSION" << dendl;
	  reply.tag = CEPH_MSGR_TAG_RETRY_SESSION;
	  reply.connect_seq = existing->connect_seq;  // so we can send it below..
	  existing->lock.Unlock();
	  rank->lock.Unlock();
	  goto reply;
	}
      }

      if (connect.connect_seq == existing->connect_seq) {
	// connection race?
	if (peer_addr < rank->rank_addr ||
	    existing->policy.server) {
	  // incoming wins
	  dout(10) << "accept connection race, existing " << existing << ".cseq " << existing->connect_seq
		   << " == " << connect.connect_seq << ", or we are server, replacing my attempt" << dendl;
	  assert(existing->state == STATE_CONNECTING ||
		 existing->state == STATE_STANDBY ||
		 existing->state == STATE_WAIT);
	  goto replace;
	} else {
	  // our existing outgoing wins
	  dout(10) << "accept connection race, existing " << existing << ".cseq " << existing->connect_seq
		   << " == " << connect.connect_seq << ", sending WAIT" << dendl;
	  assert(peer_addr > rank->rank_addr);
	  assert(existing->state == STATE_CONNECTING); // this will win
	  reply.tag = CEPH_MSGR_TAG_WAIT;
	  existing->lock.Unlock();
	  rank->lock.Unlock();
	  goto reply;
	}
      }

      assert(connect.connect_seq > existing->connect_seq);
      assert(connect.global_seq >= existing->peer_global_seq);
      if (existing->connect_seq == 0) {
	dout(0) << "accept we reset (peer sent cseq " << connect.connect_seq 
		 << ", " << existing << ".cseq = " << existing->connect_seq
		 << "), sending RESETSESSION" << dendl;
	reply.tag = CEPH_MSGR_TAG_RESETSESSION;
	rank->lock.Unlock();
	existing->lock.Unlock();
	goto reply;
      }

      // reconnect
      dout(10) << "accept peer sent cseq " << connect.connect_seq
	       << " > " << existing->connect_seq << dendl;
      goto replace;
    } // existing
    else if (connect.connect_seq > 0) {
      // we reset, and they are opening a new session
      dout(0) << "accept we reset (peer sent cseq " << connect.connect_seq << "), sending RESETSESSION" << dendl;
      rank->lock.Unlock();
      reply.tag = CEPH_MSGR_TAG_RESETSESSION;
      goto reply;
    } else {
      // new session
      dout(10) << "accept new session" << dendl;
      goto open;
    }
    assert(0);    

  reply:
    rc = tcp_write(sd, (char*)&reply, sizeof(reply));
    if (rc < 0)
      goto fail_unlocked;
  }
  
 replace:
  dout(10) << "accept replacing " << existing << dendl;
  existing->state = STATE_CLOSED;
  existing->cond.Signal();
  existing->reader_thread.kill(SIGUSR2);
  existing->unregister_pipe();
    
  // steal queue and out_seq
  existing->requeue_sent();
  out_seq = existing->out_seq;
  in_seq = existing->in_seq;
  dout(10) << "accept   out_seq " << out_seq << "  in_seq " << in_seq << dendl;
  for (map<int, list<Message*> >::iterator p = existing->q.begin();
       p != existing->q.end();
       p++)
    q[p->first].splice(q[p->first].begin(), p->second);
  
  existing->lock.Unlock();

 open:
  // open
  connect_seq = connect.connect_seq + 1;
  peer_global_seq = connect.global_seq;
  dout(10) << "accept success, connect_seq = " << connect_seq << ", sending READY" << dendl;

  // send READY reply
  reply.tag = CEPH_MSGR_TAG_READY;
  reply.global_seq = rank->get_global_seq();
  reply.connect_seq = connect_seq;
  reply.flags = 0;
  if (policy.lossy)
    reply.flags = reply.flags | CEPH_MSG_CONNECT_LOSSY;

  // ok!
  register_pipe();
  rank->lock.Unlock();

  rc = tcp_write(sd, (char*)&reply, sizeof(reply));
  if (rc < 0)
    goto fail;

  lock.Lock();
  if (state != STATE_CLOSED) {
    dout(10) << "accept starting writer, " << "state=" << state << dendl;
    start_writer();
  }
  dout(20) << "accept done" << dendl;
  lock.Unlock();
  return 0;   // success.


 fail:
  rank->lock.Unlock();
 fail_unlocked:
  lock.Lock();
  state = STATE_CLOSED;
  fault();
  lock.Unlock();
  return -1;
}

int SimpleMessenger::Pipe::connect()
{
  dout(10) << "connect " << connect_seq << dendl;
  assert(lock.is_locked());

  if (sd >= 0) {
    ::close(sd);
    sd = -1;
    closed_socket();
  }
  __u32 cseq = connect_seq;
  __u32 gseq = rank->get_global_seq();

  // stop reader thrad
  join_reader();

  lock.Unlock();
  
  char tag = -1;
  int rc;
  struct msghdr msg;
  struct iovec msgvec[2];
  int msglen;
  char banner[strlen(CEPH_BANNER)];
  entity_addr_t paddr;
  entity_addr_t peer_addr_for_me, socket_addr;
  bufferlist addrbl, myaddrbl;

  // create socket?
  sd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sd < 0) {
    char buf[80];
    dout(-1) << "connect couldn't created socket " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    assert(0);
    goto fail;
  }
  opened_socket();

  char buf[80];

  // connect!
  dout(10) << "connecting to " << peer_addr << dendl;
  rc = ::connect(sd, (sockaddr*)&peer_addr.addr, sizeof(peer_addr.addr));
  if (rc < 0) {
    dout(2) << "connect error " << peer_addr
	     << ", " << errno << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    goto fail;
  }

  // disable Nagle algorithm?
  if (g_conf.ms_tcp_nodelay) {
    int flag = 1;
    int r = ::setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
    if (r < 0) 
      dout(0) << "connect couldn't set TCP_NODELAY: " << strerror_r(errno, buf, sizeof(buf)) << dendl;
  }

  // verify banner
  // FIXME: this should be non-blocking, or in some other way verify the banner as we get it.
  rc = tcp_read(sd, (char*)&banner, strlen(CEPH_BANNER));
  if (rc < 0) {
    dout(2) << "connect couldn't read banner, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    goto fail;
  }
  if (memcmp(banner, CEPH_BANNER, strlen(CEPH_BANNER))) {
    dout(0) << "connect protocol error (bad banner) on peer " << paddr << dendl;
    goto fail;
  }

  memset(&msg, 0, sizeof(msg));
  msgvec[0].iov_base = banner;
  msgvec[0].iov_len = strlen(CEPH_BANNER);
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 1;
  msglen = msgvec[0].iov_len;
  if (do_sendmsg(sd, &msg, msglen)) {
    dout(2) << "connect couldn't write my banner, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    goto fail;
  }

  // identify peer
  {
    bufferptr p(sizeof(paddr) * 2);
    addrbl.push_back(p);
  }
  rc = tcp_read(sd, addrbl.c_str(), addrbl.length());
  if (rc < 0) {
    dout(2) << "connect couldn't read peer addrs, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    goto fail;
  }
  {
    bufferlist::iterator p = addrbl.begin();
    ::decode(paddr, p);
    ::decode(peer_addr_for_me, p);
  }

  dout(20) << "connect read peer addr " << paddr << " on socket " << sd << dendl;
  if (!peer_addr.is_local_to(paddr)) {
    if (paddr.is_blank_addr() &&
	peer_addr.get_port() == paddr.get_port() &&
	peer_addr.get_nonce() == paddr.get_nonce()) {
      dout(0) << "connect claims to be " 
	      << paddr << " not " << peer_addr << " - presumably this is the same node!" << dendl;
    } else {
      dout(0) << "connect claims to be " 
	      << paddr << " not " << peer_addr << " - wrong node!" << dendl;
      goto fail;
    }
  }

  dout(20) << "connect peer addr for me is " << peer_addr_for_me << dendl;

  if (rank->need_addr)
    rank->learned_addr(peer_addr_for_me);

  ::encode(rank->rank_addr, myaddrbl);

  memset(&msg, 0, sizeof(msg));
  msgvec[0].iov_base = myaddrbl.c_str();
  msgvec[0].iov_len = myaddrbl.length();
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 1;
  msglen = msgvec[0].iov_len;
  if (do_sendmsg(sd, &msg, msglen)) {
    dout(2) << "connect couldn't write my addr, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    goto fail;
  }
  dout(10) << "connect sent my addr " << rank->rank_addr << dendl;

  while (1) {
    ceph_msg_connect connect;
    connect.host_type = rank->my_type;
    connect.global_seq = gseq;
    connect.connect_seq = cseq;
    connect.protocol_version = get_proto_version(rank->my_type, peer_type, true);
    connect.flags = 0;
    if (policy.lossy)
      connect.flags |= CEPH_MSG_CONNECT_LOSSY;  // this is fyi, actually, server decides!
    memset(&msg, 0, sizeof(msg));
    msgvec[0].iov_base = (char*)&connect;
    msgvec[0].iov_len = sizeof(connect);
    msg.msg_iov = msgvec;
    msg.msg_iovlen = 1;
    msglen = msgvec[0].iov_len;

    dout(10) << "connect sending gseq=" << gseq << " cseq=" << cseq
	     << " proto=" << connect.protocol_version << dendl;
    if (do_sendmsg(sd, &msg, msglen)) {
      dout(2) << "connect couldn't write gseq, cseq, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      goto fail;
    }

    dout(20) << "connect wrote (self +) cseq, waiting for reply" << dendl;
    ceph_msg_connect_reply reply;
    if (tcp_read(sd, (char*)&reply, sizeof(reply)) < 0) {
      dout(2) << "connect read reply " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      goto fail;
    }
    dout(20) << "connect got reply tag " << (int)reply.tag
	     << " connect_seq " << reply.connect_seq
	     << " global_seq " << reply.global_seq
	     << " proto " << reply.protocol_version
	     << " flags " << (int)reply.flags
	     << dendl;

    lock.Lock();
    if (state != STATE_CONNECTING) {
      dout(0) << "connect got RESETSESSION but no longer connecting" << dendl;
      goto stop_locked;
    }

    if (reply.tag == CEPH_MSGR_TAG_BADPROTOVER) {
      dout(0) << "connect protocol version mismatch, my " << connect.protocol_version
	      << " != " << reply.protocol_version << dendl;
      goto fail_locked;
    }

    if (reply.tag == CEPH_MSGR_TAG_RESETSESSION) {
      dout(0) << "connect got RESETSESSION" << dendl;
      was_session_reset();
      cseq = 0;
      lock.Unlock();
      continue;
    }
    if (reply.tag == CEPH_MSGR_TAG_RETRY_GLOBAL) {
      gseq = rank->get_global_seq(reply.global_seq);
      dout(10) << "connect got RETRY_GLOBAL " << reply.global_seq
	       << " chose new " << gseq << dendl;
      lock.Unlock();
      continue;
    }
    if (reply.tag == CEPH_MSGR_TAG_RETRY_SESSION) {
      assert(reply.connect_seq > connect_seq);
      dout(10) << "connect got RETRY_SESSION " << connect_seq
	       << " -> " << reply.connect_seq << dendl;
      cseq = connect_seq = reply.connect_seq;
      lock.Unlock();
      continue;
    }

    if (reply.tag == CEPH_MSGR_TAG_WAIT) {
      dout(3) << "connect got WAIT (connection race)" << dendl;
      state = STATE_WAIT;
      goto stop_locked;
    }

    if (reply.tag == CEPH_MSGR_TAG_READY) {
      // hooray!
      peer_global_seq = reply.global_seq;
      policy.lossy = reply.flags & CEPH_MSG_CONNECT_LOSSY;
      state = STATE_OPEN;
      connect_seq = cseq + 1;
      assert(connect_seq == reply.connect_seq);
      backoff = utime_t();
      dout(20) << "connect success " << connect_seq << ", lossy = " << policy.lossy << dendl;

      for (unsigned i=0; i<rank->local.size(); i++) 
	if (rank->local[i])
	  rank->local[i]->queue_connect(connection_state->get());

      if (!reader_running) {
	dout(20) << "connect starting reader" << dendl;
	start_reader();
      }
      return 0;
    }
    
    // protocol error
    dout(0) << "connect got bad tag " << (int)tag << dendl;
    goto fail_locked;
  }

 fail:
  lock.Lock();
 fail_locked:
  if (state == STATE_CONNECTING)
    fault();
  else
    dout(3) << "connect fault, but state != connecting, stopping" << dendl;

 stop_locked:
  return -1;
}

void SimpleMessenger::Pipe::register_pipe()
{
  dout(10) << "register_pipe" << dendl;
  assert(rank->lock.is_locked());
  assert(rank->rank_pipe.count(peer_addr) == 0);
  rank->rank_pipe[peer_addr] = this;
}

void SimpleMessenger::Pipe::unregister_pipe()
{
  assert(rank->lock.is_locked());
  if (rank->rank_pipe.count(peer_addr) &&
      rank->rank_pipe[peer_addr] == this) {
    dout(10) << "unregister_pipe" << dendl;
    rank->rank_pipe.erase(peer_addr);
  } else {
    dout(10) << "unregister_pipe - not registered" << dendl;
  }
}


void SimpleMessenger::Pipe::requeue_sent()
{
  if (sent.empty())
    return;

  list<Message*>& rq = q[CEPH_MSG_PRIO_HIGHEST];
  while (!sent.empty()) {
    Message *m = sent.back();
    sent.pop_back();
    dout(10) << "requeue_sent " << *m << " for resend seq " << out_seq
	     << " (" << m->get_seq() << ")" << dendl;
    rq.push_front(m);
    out_seq--;
  }
}

void SimpleMessenger::Pipe::discard_queue()
{
  dout(10) << "discard_queue" << dendl;
  for (list<Message*>::iterator p = sent.begin(); p != sent.end(); p++)
    (*p)->put();
  sent.clear();
  for (map<int,list<Message*> >::iterator p = q.begin(); p != q.end(); p++)
    for (list<Message*>::iterator r = p->second.begin(); r != p->second.end(); r++)
      (*r)->put();
  q.clear();
}


void SimpleMessenger::Pipe::fault(bool onconnect, bool onread)
{
  assert(lock.is_locked());
  cond.Signal();

  if (onread && state == STATE_CONNECTING) {
    dout(10) << "fault already connecting, reader shutting down" << dendl;
    return;
  }
  
  char buf[80];
  if (!onconnect) dout(2) << "fault " << errno << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;

  if (state == STATE_CLOSED ||
      state == STATE_CLOSING) {
    dout(10) << "fault already closed|closing" << dendl;
    return;
  }

  if (sd >= 0) {
    ::close(sd);
    sd = -1;
    closed_socket();
  }

  // lossy channel?
  if (policy.lossy) {
    dout(10) << "fault on lossy channel, failing" << dendl;
    was_session_reset();
    fail();
    return;
  }

  // requeue sent items
  requeue_sent();

  if (!is_queued()) {
    if (state == STATE_CLOSING || onconnect) {
      dout(10) << "fault on connect, or already closing, and q empty: setting closed." << dendl;
      state = STATE_CLOSED;
    } else {
      dout(0) << "fault with nothing to send, going to standby" << dendl;
      state = STATE_STANDBY;
    }
    return;
  } 


  utime_t now = g_clock.now();
  if (state != STATE_CONNECTING) {
    if (!onconnect)
      dout(0) << "fault initiating reconnect" << dendl;
    connect_seq++;
    state = STATE_CONNECTING;
    backoff = utime_t();
  } else if (backoff == utime_t()) {
    if (!onconnect)
      dout(0) << "fault first fault" << dendl;
    backoff.set_from_double(g_conf.ms_initial_backoff);
  } else {
    dout(10) << "fault waiting " << backoff << dendl;
    cond.WaitInterval(lock, backoff);
    backoff += backoff;
    if (backoff > g_conf.ms_max_backoff)
      backoff.set_from_double(g_conf.ms_max_backoff);
    dout(10) << "fault done waiting or woke up" << dendl;
  }
}

void SimpleMessenger::Pipe::fail()
{
  derr(10) << "fail" << dendl;
  assert(lock.is_locked());

  stop();
  report_failures();

  for (unsigned i=0; i<rank->local.size(); i++) 
    if (rank->local[i])
      rank->local[i]->queue_reset(connection_state->get());

  // unregister
  lock.Unlock();
  rank->lock.Lock();
  unregister_pipe();
  rank->lock.Unlock();
  lock.Lock();
}

void SimpleMessenger::Pipe::was_session_reset()
{
  assert(lock.is_locked());

  dout(10) << "was_session_reset" << dendl;
  report_failures();
  for (unsigned i=0; i<rank->local.size(); i++) 
    if (rank->local[i])
      rank->local[i]->queue_remote_reset(connection_state->get());

  out_seq = 0;
  in_seq = 0;
  connect_seq = 0;
}

void SimpleMessenger::Pipe::report_failures()
{
  // report failures
  q[CEPH_MSG_PRIO_HIGHEST].splice(q[CEPH_MSG_PRIO_HIGHEST].begin(), sent);
  while (1) {
    Message *m = _get_next_outgoing();
    if (!m)
      break;
    m->put();
  }
}

void SimpleMessenger::Pipe::stop()
{
  dout(10) << "stop" << dendl;
  state = STATE_CLOSED;
  cond.Signal();
  if (sd >= 0) {
    ::close(sd);
    sd = -1;
  }
  if (reader_running)
    reader_thread.kill(SIGUSR2);
  if (writer_running)
    writer_thread.kill(SIGUSR2);
}


/* read msgs from socket.
 * also, server.
 */
void SimpleMessenger::Pipe::reader()
{
  if (state == STATE_ACCEPTING) 
    accept();

  lock.Lock();

  // loop.
  while (state != STATE_CLOSED &&
	 state != STATE_CONNECTING) {
    assert(lock.is_locked());

    // sleep if (re)connecting
    if (state == STATE_STANDBY) {
      dout(20) << "reader sleeping during reconnect|standby" << dendl;
      cond.Wait(lock);
      continue;
    }

    lock.Unlock();

    char buf[80];
    char tag = -1;
    dout(20) << "reader reading tag..." << dendl;
    int rc = tcp_read(sd, (char*)&tag, 1);
    if (rc < 0) {
      lock.Lock();
      dout(2) << "reader couldn't read tag, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      fault(false, true);
      continue;
    }

    if (tag == CEPH_MSGR_TAG_KEEPALIVE) {
      dout(20) << "reader got KEEPALIVE" << dendl;
      lock.Lock();
      continue;
    }

    // open ...
    if (tag == CEPH_MSGR_TAG_ACK) {
      dout(20) << "reader got ACK" << dendl;
      __le64 seq;
      int rc = tcp_read( sd, (char*)&seq, sizeof(seq));
      lock.Lock();
      if (rc < 0) {
	dout(2) << "reader couldn't read ack seq, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
	fault(false, true);
      } else if (state != STATE_CLOSED) {
	dout(15) << "reader got ack seq " << seq << dendl;
	// trim sent list
	while (!sent.empty() &&
	       sent.front()->get_seq() <= seq) {
	  Message *m = sent.front();
	  sent.pop_front();
	  dout(10) << "reader got ack seq " 
		    << seq << " >= " << m->get_seq() << " on " << m << " " << *m << dendl;
	  m->put();
	}
      }
      continue;
    }

    else if (tag == CEPH_MSGR_TAG_MSG) {
      dout(20) << "reader got MSG" << dendl;
      Message *m = read_message();

      lock.Lock();
      
      if (!m) {
	derr(2) << "reader read null message, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
	fault(false, true);
	continue;
      }

      if (state == STATE_CLOSED ||
	  state == STATE_CONNECTING)
	continue;

      m->set_connection(connection_state->get());

      // check received seq#
      if (m->get_seq() <= in_seq) {
	dout(-10) << "reader got old message "
		  << m->get_seq() << " <= " << in_seq << " " << m << " " << *m
		  << ", discarding" << dendl;
	delete m;
	continue;
      }
      in_seq++;

      if (!policy.lossy && in_seq != m->get_seq()) {
	dout(0) << "reader got bad seq " << m->get_seq() << " expected " << in_seq
		<< " for " << *m << " from " << m->get_source() << dendl;
	derr(0) << "reader got bad seq " << m->get_seq() << " expected " << in_seq
		<< " for " << *m << " from " << m->get_source() << dendl;
	assert(in_seq == m->get_seq()); // for now!
	fault(false, true);
	delete m;
	continue;
      }

      cond.Signal();  // wake up writer, to ack this
      lock.Unlock();
      
      dout(10) << "reader got message "
	       << m->get_seq() << " " << m << " " << *m
	       << dendl;
      
      // deliver
      Endpoint *entity = 0;
      
      rank->lock.Lock();
      {
	unsigned erank = m->get_header().dst_erank;
	if (erank < rank->max_local && rank->local[erank]) {
	  // find entity
	  entity = rank->local[erank];
	  entity->get();
	} else {
	  derr(0) << "reader got message " << *m << ", which isn't local" << dendl;
	}
      }
      rank->lock.Unlock();
      
      if (entity) {
	entity->queue_message(m);        // queue
	entity->put();
      }

      lock.Lock();
    } 
    
    else if (tag == CEPH_MSGR_TAG_CLOSE) {
      dout(20) << "reader got CLOSE" << dendl;
      lock.Lock();
      if (state == STATE_CLOSING)
	state = STATE_CLOSED;
      else
	state = STATE_CLOSING;
      cond.Signal();
      break;
    }
    else {
      dout(0) << "reader bad tag " << (int)tag << dendl;
      lock.Lock();
      fault(false, true);
    }
  }

 
  // reap?
  bool reap = false;
  reader_running = false;
  if (!writer_running)
    reap = true;

  lock.Unlock();

  if (reap) {
    dout(10) << "reader queueing for reap" << dendl;
    if (sd >= 0) {
      ::close(sd);
      sd = -1;
      closed_socket();
    }
    rank->lock.Lock();
    {
      rank->pipe_reap_queue.push_back(this);
      rank->wait_cond.Signal();
    }
    rank->lock.Unlock();
  }

  dout(10) << "reader done" << dendl;
}

/*
class FakeSocketError : public Context {
  int sd;
public:
  FakeSocketError(int s) : sd(s) {}
  void finish(int r) {
    cout << "faking socket error on " << sd << std::endl;
    ::close(sd);
  }
};
*/

/* write msgs to socket.
 * also, client.
 */
void SimpleMessenger::Pipe::writer()
{
  char buf[80];

  lock.Lock();

  while (state != STATE_CLOSED) {// && state != STATE_WAIT) {
    dout(10) << "writer: state = " << state << " policy.server=" << policy.server << dendl;

    // standby?
    if (is_queued() && state == STATE_STANDBY && !policy.server)
      state = STATE_CONNECTING;

    // connect?
    if (state == STATE_CONNECTING) {
      if (policy.server) {
	state = STATE_STANDBY;
      } else {
	connect();
	continue;
      }
    }
    
    if (state == STATE_CLOSING) {
      // write close tag
      dout(20) << "writer writing CLOSE tag" << dendl;
      char tag = CEPH_MSGR_TAG_CLOSE;
      state = STATE_CLOSED;
      lock.Unlock();
      if (sd) ::write(sd, &tag, 1);
      lock.Lock();
      continue;
    }

    if (state != STATE_CONNECTING && state != STATE_WAIT && state != STATE_STANDBY &&
	(is_queued() || in_seq > in_seq_acked)) {

      // keepalive?
      if (keepalive) {
	lock.Unlock();
	int rc = write_keepalive();
	lock.Lock();
	if (rc < 0) {
	  dout(2) << "writer couldn't write keepalive, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
	  fault();
 	  continue;
	}
	keepalive = false;
      }

      // send ack?
      if (in_seq > in_seq_acked) {
	int send_seq = in_seq;
	lock.Unlock();
	int rc = write_ack(send_seq);
	lock.Lock();
	if (rc < 0) {
	  dout(2) << "writer couldn't write ack, " << strerror_r(errno, buf, sizeof(buf)) << dendl;
	  fault();
 	  continue;
	}
	in_seq_acked = send_seq;
      }

      // grab outgoing message
      Message *m = _get_next_outgoing();
      if (m) {
	m->set_seq(++out_seq);
	sent.push_back(m); // move to sent list
	m->get();
	lock.Unlock();

        dout(20) << "writer encoding " << m->get_seq() << " " << m << " " << *m << dendl;

	// encode and copy out of *m
	m->encode();

        dout(20) << "writer sending " << m->get_seq() << " " << m << dendl;
	int rc = write_message(m);

	lock.Lock();
	if (rc < 0) {
          derr(1) << "writer error sending " << m << ", "
		  << errno << ": " << strerror_r(errno, buf, sizeof(buf)) << dendl;
	  fault();
        }
	m->put();
      }
      continue;
    }
    
    // wait
    dout(20) << "writer sleeping" << dendl;
    cond.Wait(lock);
  }
  
  dout(20) << "writer finishing" << dendl;

  // reap?
  bool reap = false;
  writer_running = false;
  if (!reader_running) reap = true;

  lock.Unlock();
  
  if (reap) {
    dout(10) << "writer queueing for reap" << dendl;
    if (sd >= 0) {
      ::close(sd);
      sd = -1;
      closed_socket();
    }
    rank->lock.Lock();
    {
      rank->pipe_reap_queue.push_back(this);
      rank->wait_cond.Signal();
    }
    rank->lock.Unlock();
  }

  dout(10) << "writer done" << dendl;
}


Message *SimpleMessenger::Pipe::read_message()
{
  // envelope
  //dout(10) << "receiver.read_message from sd " << sd  << dendl;
  
  ceph_msg_header header; 
  ceph_msg_footer footer;

  if (tcp_read( sd, (char*)&header, sizeof(header) ) < 0)
    return 0;

  dout(20) << "reader got envelope type=" << header.type
           << " src " << header.src
           << " front=" << header.front_len
	   << " data=" << header.data_len
	   << " off " << header.data_off
           << dendl;

  // verify header crc
  __u32 header_crc = crc32c_le(0, (unsigned char *)&header, sizeof(header) - sizeof(header.crc));
  if (header_crc != header.crc) {
    dout(0) << "reader got bad header crc " << header_crc << " != " << header.crc << dendl;
    return 0;
  }

  // ok, now it's safe to change the header..
  // munge source address?
  entity_addr_t srcaddr = header.src.addr;
  if (srcaddr.is_blank_addr()) {
    dout(10) << "reader munging src addr " << header.src << " to be " << peer_addr << dendl;
    ceph_entity_addr enc_peer_addr = peer_addr;
    header.orig_src.addr.in_addr = header.src.addr.in_addr = enc_peer_addr.in_addr;    
  }

  // read front
  bufferlist front;
  int front_len = header.front_len;
  if (front_len) {
    bufferptr bp = buffer::create(front_len);
    if (tcp_read( sd, bp.c_str(), front_len ) < 0) 
      return 0;
    front.push_back(bp);
    dout(20) << "reader got front " << front.length() << dendl;
  }

  // read middle
  bufferlist middle;
  int middle_len = header.middle_len;
  if (middle_len) {
    bufferptr bp = buffer::create(middle_len);
    if (tcp_read( sd, bp.c_str(), middle_len ) < 0) 
      return 0;
    middle.push_back(bp);
    dout(20) << "reader got middle " << middle.length() << dendl;
  }


  // read data
  bufferlist data;
  unsigned data_len = le32_to_cpu(header.data_len);
  unsigned data_off = le32_to_cpu(header.data_off);
  if (data_len) {
    int left = data_len;
    if (data_off & ~PAGE_MASK) {
      // head
      int head = MIN(PAGE_SIZE - (data_off & ~PAGE_MASK),
		     (unsigned)left);
      bufferptr bp = buffer::create(head);
      if (tcp_read( sd, bp.c_str(), head ) < 0) 
	return 0;
      data.push_back(bp);
      left -= head;
      dout(20) << "reader got data head " << head << dendl;
    }

    // middle
    int middle = left & PAGE_MASK;
    if (middle > 0) {
      bufferptr bp = buffer::create_page_aligned(middle);
      if (tcp_read( sd, bp.c_str(), middle ) < 0) 
	return 0;
      data.push_back(bp);
      left -= middle;
      dout(20) << "reader got data page-aligned middle " << middle << dendl;
    }

    if (left) {
      bufferptr bp = buffer::create(left);
      if (tcp_read( sd, bp.c_str(), left ) < 0) 
	return 0;
      data.push_back(bp);
      dout(20) << "reader got data tail " << left << dendl;
    }
  }

  // footer
  if (tcp_read(sd, (char*)&footer, sizeof(footer)) < 0) 
    return 0;
  
  int aborted = (footer.flags & CEPH_MSG_FOOTER_COMPLETE) == 0;
  dout(10) << "aborted = " << aborted << dendl;
  if (aborted) {
    dout(0) << "reader got " << front.length() << " + " << middle.length() << " + " << data.length()
	    << " byte message from " << header.src << ".. ABORTED" << dendl;
    // MEH FIXME 
    Message *m = new MGenericMessage(CEPH_MSG_PING);
    header.type = CEPH_MSG_PING;
    m->set_header(header);
    return m;
  }

  dout(20) << "reader got " << front.length() << " + " << middle.length() << " + " << data.length()
	   << " byte message from " << header.src << dendl;
  return decode_message(header, footer, front, middle, data);
}


int SimpleMessenger::Pipe::do_sendmsg(int sd, struct msghdr *msg, int len, bool more)
{
  char buf[80];

  while (len > 0) {
    if (0) { // sanity
      int l = 0;
      for (unsigned i=0; i<msg->msg_iovlen; i++)
	l += msg->msg_iov[i].iov_len;
      assert(l == len);
    }

    int r = ::sendmsg(sd, msg, more ? MSG_MORE : 0);
    if (r == 0) 
      dout(10) << "do_sendmsg hmm do_sendmsg got r==0!" << dendl;
    if (r < 0) { 
      dout(1) << "do_sendmsg error " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      return -1;
    }
    if (state == STATE_CLOSED) {
      dout(10) << "do_sendmsg oh look, state == CLOSED, giving up" << dendl;
      errno = EINTR;
      return -1; // close enough
    }

    if (0) {
      // hex dump
      struct iovec *v = msg->msg_iov;
      size_t left = r;
      size_t vpos = 0;
      dout(0) << "do_sendmsg wrote " << r << " bytes, hexdump:\n";
      int pos = 0;
      int col = 0;
      char buf[20];
      while (left > 0) {
	if (col == 0) {
	  sprintf(buf, "%05x : ", pos);
	  *_dout << buf;
	}
	sprintf(buf, " %02x", ((unsigned char*)v->iov_base)[vpos]);
	*_dout << buf;
	left--;
	if (!left)
	  break;
	vpos++;
	pos++;
	if (vpos == v->iov_len) {
	  v++;
	  vpos = 0;
	}	  
	col++;
	if (col == 16) {
	  *_dout << "\n";
	  col = 0;
	}
      }
      *_dout << dendl;
    }

    len -= r;
    if (len == 0) break;
    
    // hrmph.  trim r bytes off the front of our message.
    dout(20) << "do_sendmail short write did " << r << ", still have " << len << dendl;
    while (r > 0) {
      if (msg->msg_iov[0].iov_len <= (size_t)r) {
	// lose this whole item
	//dout(30) << "skipping " << msg->msg_iov[0].iov_len << ", " << (msg->msg_iovlen-1) << " v, " << r << " left" << dendl;
	r -= msg->msg_iov[0].iov_len;
	msg->msg_iov++;
	msg->msg_iovlen--;
      } else {
	// partial!
	//dout(30) << "adjusting " << msg->msg_iov[0].iov_len << ", " << msg->msg_iovlen << " v, " << r << " left" << dendl;
	msg->msg_iov[0].iov_base = (char *)msg->msg_iov[0].iov_base + r;
	msg->msg_iov[0].iov_len -= r;
	break;
      }
    }
  }
  return 0;
}


int SimpleMessenger::Pipe::write_ack(__u64 seq)
{
  dout(10) << "write_ack " << seq << dendl;

  char c = CEPH_MSGR_TAG_ACK;
  __le64 s;
  s = seq;

  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec msgvec[2];
  msgvec[0].iov_base = &c;
  msgvec[0].iov_len = 1;
  msgvec[1].iov_base = &s;
  msgvec[1].iov_len = sizeof(s);
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 2;
  
  if (do_sendmsg(sd, &msg, 1 + sizeof(s), true) < 0) 
    return -1;	
  return 0;
}

int SimpleMessenger::Pipe::write_keepalive()
{
  dout(10) << "write_keepalive" << dendl;

  char c = CEPH_MSGR_TAG_KEEPALIVE;

  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec msgvec[2];
  msgvec[0].iov_base = &c;
  msgvec[0].iov_len = 1;
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 1;
  
  if (do_sendmsg(sd, &msg, 1) < 0) 
    return -1;	
  return 0;
}


int SimpleMessenger::Pipe::write_message(Message *m)
{
  ceph_msg_header& header = m->get_header();
  ceph_msg_footer& footer = m->get_footer();

  // get envelope, buffers
  header.front_len = m->get_payload().length();
  header.middle_len = m->get_middle().length();
  header.data_len = m->get_data().length();
  footer.flags = CEPH_MSG_FOOTER_COMPLETE;
  m->calc_header_crc();

  bufferlist blist = m->get_payload();
  blist.append(m->get_middle());
  blist.append(m->get_data());
  
  dout(20)  << "write_message " << m << dendl;
  
  // set up msghdr and iovecs
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec msgvec[3 + blist.buffers().size()];  // conservative upper bound
  msg.msg_iov = msgvec;
  int msglen = 0;
  
  // send tag
  char tag = CEPH_MSGR_TAG_MSG;
  msgvec[msg.msg_iovlen].iov_base = &tag;
  msgvec[msg.msg_iovlen].iov_len = 1;
  msglen++;
  msg.msg_iovlen++;

  // send envelope
  msgvec[msg.msg_iovlen].iov_base = (char*)&header;
  msgvec[msg.msg_iovlen].iov_len = sizeof(header);
  msglen += sizeof(header);
  msg.msg_iovlen++;

  // payload (front+data)
  list<bufferptr>::const_iterator pb = blist.buffers().begin();
  int b_off = 0;  // carry-over buffer offset, if any
  int bl_pos = 0; // blist pos
  int left = blist.length();

  while (left > 0) {
    int donow = MIN(left, (int)pb->length()-b_off);
    if (donow == 0) {
      dout(0) << "donow = " << donow << " left " << left << " pb->length " << pb->length() << " b_off " << b_off << dendl;
    }
    assert(donow > 0);
    dout(30) << " bl_pos " << bl_pos << " b_off " << b_off
	     << " leftinchunk " << left
	     << " buffer len " << pb->length()
	     << " writing " << donow 
	     << dendl;
    
    if (msg.msg_iovlen >= IOV_MAX-2) {
      if (do_sendmsg(sd, &msg, msglen, true)) 
	return -1;	
      
      // and restart the iov
      msg.msg_iov = msgvec;
      msg.msg_iovlen = 0;
      msglen = 0;
    }
    
    msgvec[msg.msg_iovlen].iov_base = (void*)(pb->c_str()+b_off);
    msgvec[msg.msg_iovlen].iov_len = donow;
    msglen += donow;
    msg.msg_iovlen++;
    
    left -= donow;
    assert(left >= 0);
    b_off += donow;
    bl_pos += donow;
    if (left == 0) break;
    while (b_off == (int)pb->length()) {
      pb++;
      b_off = 0;
    }
  }
  assert(left == 0);

  // send footer
  msgvec[msg.msg_iovlen].iov_base = (void*)&footer;
  msgvec[msg.msg_iovlen].iov_len = sizeof(footer);
  msglen += sizeof(footer);
  msg.msg_iovlen++;

  // send
  if (do_sendmsg(sd, &msg, msglen)) 
    return -1;	

  return 0;
}


/********************************************
 * SimpleMessenger
 */
#undef dout_prefix
#define dout_prefix _prefix(this)


/*
 * note: assumes lock is held
 */
void SimpleMessenger::reaper()
{
  dout(10) << "reaper" << dendl;
  assert(lock.is_locked());

  while (!pipe_reap_queue.empty()) {
    Pipe *p = pipe_reap_queue.front();
    dout(10) << "reaper reaping pipe " << p << " " << p->get_peer_addr() << dendl;
    p->unregister_pipe();
    pipe_reap_queue.pop_front();
    assert(pipes.count(p));
    pipes.erase(p);
    p->join();
    p->discard_queue();
    dout(10) << "reaper reaped pipe " << p << " " << p->get_peer_addr() << dendl;
    assert(p->sd < 0);
    delete p;
    dout(10) << "reaper deleted pipe " << p << dendl;
  }
}


int SimpleMessenger::bind(int64_t force_nonce)
{
  lock.Lock();
  if (started) {
    dout(10) << "rank.bind already started" << dendl;
    lock.Unlock();
    return -1;
  }
  dout(10) << "rank.bind" << dendl;
  lock.Unlock();

  // bind to a socket
  return accepter.bind(force_nonce);
}


class C_Die : public Context {
public:
  void finish(int) {
    cerr << "die" << std::endl;
    exit(1);
  }
};

static void write_pid_file(int pid)
{
  if (!g_conf.pid_file)
    return;

  int fd = ::open(g_conf.pid_file, O_CREAT|O_TRUNC|O_WRONLY, 0644);
  if (fd >= 0) {
    char buf[20];
    int len = sprintf(buf, "%d\n", pid);
    ::write(fd, buf, len);
    ::close(fd);
  }
}

static void remove_pid_file()
{
  if (!g_conf.pid_file)
    return;

  // only remove it if it has OUR pid in it!
  int fd = ::open(g_conf.pid_file, O_RDONLY);
  if (fd >= 0) {
    char buf[20];
    ::read(fd, buf, 20);
    ::close(fd);
    int a = atoi(buf);

    if (a == getpid())
      ::unlink(g_conf.pid_file);
    else
      generic_dout(0) << "strange, pid file " << g_conf.pid_file 
	      << " has " << a << ", not expected " << getpid()
	      << dendl;
  }
}

int SimpleMessenger::start(bool nodaemon)
{
  // register at least one entity, first!
  assert(my_type >= 0); 

  lock.Lock();
  if (started) {
    dout(10) << "rank.start already started" << dendl;
    lock.Unlock();
    return 0;
  }

  if (!did_bind)
    rank_addr.nonce = getpid();

  dout(1) << "rank.start" << dendl;
  started = true;
  lock.Unlock();

  // daemonize?
  if (g_conf.daemonize && !nodaemon) {
    if (Thread::get_num_threads() > 0) {
      derr(0) << "rank.start BUG: there are " << Thread::get_num_threads()
	      << " already started that will now die!  call rank.start() sooner." 
	      << dendl;
    }
    dout(1) << "rank.start daemonizing" << dendl;

    if (1) {
      daemon(1, 0);
      write_pid_file(getpid());
    } else {
      pid_t pid = fork();
      if (pid) {
	// i am parent
	write_pid_file(pid);
	::close(0);
	::close(1);
	::close(2);
	_exit(0);
      }
    }
 
    if (g_conf.chdir && g_conf.chdir[0]) {
      ::mkdir(g_conf.chdir, 0700);
      ::chdir(g_conf.chdir);
    }

    _dout_rename_output_file();
  } else if (g_daemon) {
    write_pid_file(getpid());
  }

  // some debug hackery?
  if (g_conf.kill_after) 
    g_timer.add_event_after(g_conf.kill_after, new C_Die);

  // set noop handlers for SIGUSR2, SIGPIPE
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = noop_signal_handler;
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGUSR2, &sa, NULL);
  sigaction(SIGPIPE, &sa, NULL);  // mask SIGPIPE too.  FIXME: i'm quite certain this is a roundabout way to do that.

  // go!
  if (did_bind)
    accepter.start();
  return 0;
}


/* connect_rank
 * NOTE: assumes rank.lock held.
 */
SimpleMessenger::Pipe *SimpleMessenger::connect_rank(const entity_addr_t& addr, int type)
{
  assert(lock.is_locked());
  assert(addr != rank_addr);
  
  dout(10) << "connect_rank to " << addr << ", creating pipe and registering" << dendl;
  
  // create pipe
  Pipe *pipe = new Pipe(this, Pipe::STATE_CONNECTING);
  pipe->set_peer_type(type);
  pipe->set_peer_addr(addr);
  pipe->policy = get_policy(type);
  pipe->start_writer();
  pipe->register_pipe();
  pipes.insert(pipe);

  return pipe;
}








/* register_entity 
 */
SimpleMessenger::Endpoint *SimpleMessenger::register_entity(entity_name_t name)
{
  dout(10) << "register_entity " << name << dendl;
  lock.Lock();
  
  // create messenger
  int erank = max_local;
  Endpoint *msgr = new Endpoint(this, name, erank);

  // now i know my type.
  if (my_type >= 0)
    assert(my_type == name.type());
  else
    my_type = name.type();

  // add to directory
  max_local++;
  local.resize(max_local);
  stopped.resize(max_local);

  msgr->get();
  local[erank] = msgr;
  stopped[erank] = false;

  dout(10) << "register_entity " << name << " at " << msgr->get_myaddr() << dendl;

  num_local++;
  
  lock.Unlock();
  return msgr;
}


void SimpleMessenger::unregister_entity(Endpoint *msgr)
{
  lock.Lock();
  dout(10) << "unregister_entity " << msgr->get_myname() << dendl;
  
  // remove from local directory.
  assert(msgr->my_rank >= 0);
  assert(local[msgr->my_rank] == msgr);
  local[msgr->my_rank] = 0;
  stopped[msgr->my_rank] = true;
  num_local--;
  msgr->my_rank = -1;

  assert(msgr->nref.test() > 1);
  msgr->put();

  wait_cond.Signal();

  lock.Unlock();
}


void SimpleMessenger::submit_message(Message *m, const entity_inst_t& dest, bool lazy)
{
  const entity_addr_t& dest_addr = dest.addr;
  m->get_header().dst_erank = dest_addr.erank;

  assert(m->nref.test() == 0);

  // lookup
  entity_addr_t dest_proc_addr = dest_addr;
  dest_proc_addr.erank = 0;

  lock.Lock();
  {
    // local?
    if (rank_addr.is_local_to(dest_addr)) {
      if (dest_addr.get_erank() < max_local && local[dest_addr.get_erank()]) {
        // local
        dout(20) << "submit_message " << *m << " local" << dendl;
	local[dest_addr.get_erank()]->queue_message(m);
      } else {
        derr(0) << "submit_message " << *m << " " << dest_addr << " local but not in local map?  dropping." << dendl;
        //assert(0);  // hmpf, this is probably mds->mon beacon from newsyn.
	delete m;
      }
    }
    else {
      // remote.
      Pipe *pipe = 0;
      if (rank_pipe.count( dest_proc_addr )) {
        // connected?
        pipe = rank_pipe[ dest_proc_addr ];
	pipe->lock.Lock();
	if (pipe->state == Pipe::STATE_CLOSED) {
	  dout(20) << "submit_message " << *m << " remote, " << dest_addr << ", ignoring old closed pipe." << dendl;
	  pipe->unregister_pipe();
	  pipe->lock.Unlock();
	  pipe = 0;
	} else {
	  dout(20) << "submit_message " << *m << " remote, " << dest_addr << ", have pipe." << dendl;

	  pipe->_send(m);
	  pipe->lock.Unlock();
	}
      }
      if (!pipe) {
	if (lazy) {
	  dout(20) << "submit_message " << *m << " remote, " << dest_addr << ", lazy, dropping." << dendl;
	  delete m;
	} else {
	  dout(20) << "submit_message " << *m << " remote, " << dest_addr << ", new pipe." << dendl;
	  // not connected.
	  pipe = connect_rank(dest_proc_addr, dest.name.type());
	  pipe->send(m);
	}
      }
    }
  }

  lock.Unlock();
}

void SimpleMessenger::send_keepalive(const entity_inst_t& dest)
{
  const entity_addr_t dest_addr = dest.addr;
  entity_addr_t dest_proc_addr = dest_addr;
  lock.Lock();
  {
    // local?
    if (!rank_addr.is_local_to(dest_addr)) {
      // remote.
      Pipe *pipe = 0;
      if (rank_pipe.count( dest_proc_addr )) {
        // connected?
        pipe = rank_pipe[ dest_proc_addr ];
	pipe->lock.Lock();
	if (pipe->state == Pipe::STATE_CLOSED) {
	  dout(20) << "send_keepalive remote, " << dest_addr << ", ignoring old closed pipe." << dendl;
	  pipe->unregister_pipe();
	  pipe->lock.Unlock();
	  pipe = 0;
	} else {
	  dout(20) << "send_keepalive remote, " << dest_addr << ", have pipe." << dendl;
	  pipe->_send_keepalive();
	  pipe->lock.Unlock();
	}
      }
      if (!pipe) {
	dout(20) << "send_keepalive remote, " << dest_addr << ", new pipe." << dendl;
	// not connected.
	pipe = connect_rank(dest_proc_addr, dest.name.type());
	pipe->send_keepalive();
      }
    }
  }

  lock.Unlock();
}





void SimpleMessenger::wait()
{
  lock.Lock();
  while (1) {
    // reap dead pipes
    reaper();

    if (num_local == 0) {
      dout(10) << "wait: everything stopped" << dendl;
      break;   // everything stopped.
    } else {
      dout(10) << "wait: local still has " << local.size() << " items, waiting" << dendl;
    }
    
    wait_cond.Wait(lock);
  }
  lock.Unlock();
  
  // done!  clean up.
  if (did_bind) {
    dout(20) << "wait: stopping accepter thread" << dendl;
    accepter.stop();
    dout(20) << "wait: stopped accepter thread" << dendl;
  }

  // close+reap all pipes
  lock.Lock();
  {
    dout(10) << "wait: closing pipes" << dendl;
    list<Pipe*> toclose;
    for (hash_map<entity_addr_t,Pipe*>::iterator i = rank_pipe.begin();
         i != rank_pipe.end();
         i++)
      toclose.push_back(i->second);
    for (list<Pipe*>::iterator i = toclose.begin();
	 i != toclose.end();
	 i++) {
      (*i)->unregister_pipe();
      (*i)->lock.Lock();
      (*i)->stop();
      (*i)->lock.Unlock();
    }

    reaper();
    dout(10) << "wait: waiting for pipes " << pipes << " to close" << dendl;
    while (!pipes.empty()) {
      wait_cond.Wait(lock);
      reaper();
    }
  }
  lock.Unlock();

  dout(10) << "wait: done." << dendl;
  dout(1) << "shutdown complete." << dendl;
  remove_pid_file();
  started = false;
  did_bind = false;
  my_type = -1;
}




void SimpleMessenger::mark_down(entity_addr_t addr)
{
  lock.Lock();
  if (rank_pipe.count(addr)) {
    Pipe *p = rank_pipe[addr];
    dout(1) << "mark_down " << addr << " -- " << p << dendl;
    p->unregister_pipe();
    p->lock.Lock();
    p->stop();
    p->lock.Unlock();
  } else {
    dout(1) << "mark_down " << addr << " -- pipe dne" << dendl;
  }
  lock.Unlock();
}

void SimpleMessenger::learned_addr(entity_addr_t peer_addr_for_me)
{
  lock.Lock();
  int port = rank_addr.get_port();
  rank_addr.addr = peer_addr_for_me.addr;
  rank_addr.set_port(port);
  dout(1) << "learned my addr " << rank_addr << dendl;
  need_addr = false;
  lock.Unlock();
}
