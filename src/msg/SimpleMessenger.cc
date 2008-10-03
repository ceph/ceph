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

#define dout(l)  if (l<=g_conf.debug_ms) *_dout << dbeginl << g_clock.now() << " " << pthread_self() << " -- " << rank.rank_addr << " "
#define derr(l)  if (l<=g_conf.debug_ms) *_derr << dbeginl << g_clock.now() << " " << pthread_self() << " -- " << rank.rank_addr << " "



#include "tcp.cc"


Rank rank;

#ifdef DARWIN
sig_t old_sigint_handler = 0;
#else
sighandler_t old_sigint_handler = 0;
#endif

/********************************************
 * Accepter
 */

void simplemessenger_sigint(int r)
{
  rank.sigint();
  if (old_sigint_handler)
    old_sigint_handler(r);
}

void Rank::sigint()
{
  lock.Lock();
  derr(0) << "got control-c, exiting" << dendl;
  
  // force close listener socket
  if (accepter.listen_sd >= 0) 
    ::close(accepter.listen_sd);

  // force close all pipe sockets, too
  for (hash_map<entity_addr_t, Pipe*>::iterator p = rank_pipe.begin();
       p != rank_pipe.end();
       ++p) 
    p->second->force_close();

  lock.Unlock();
}



void noop_signal_handler(int s)
{
  //dout(0) << "blah_handler got " << s << dendl;
}

int Rank::Accepter::bind(int64_t force_nonce)
{
  // bind to a socket
  dout(10) << "accepter.bind" << dendl;
  
  // use whatever user specified (if anything)
  sockaddr_in listen_addr = g_my_addr.ipaddr;

  /* socket creation */
  listen_sd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_sd < 0) {
    derr(0) << "accepter.bind unable to create socket: "
	    << strerror(errno) << dendl;
    return -errno;
  }

  int on = 1;
  ::setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  
  /* bind to port */
  int rc = ::bind(listen_sd, (struct sockaddr *) &listen_addr, sizeof(listen_addr));
  if (rc < 0) {
    derr(0) << "accepter.bind unable to bind to " << g_my_addr.ipaddr
	    << ": " << strerror(errno) << dendl;
    return -errno;
  }

  // what port did we get?
  socklen_t llen = sizeof(listen_addr);
  getsockname(listen_sd, (sockaddr*)&listen_addr, &llen);
  
  dout(10) << "accepter.bind bound to " << listen_addr << dendl;

  // listen!
  rc = ::listen(listen_sd, 128);
  if (rc < 0) {
    derr(0) << "accepter.bind unable to listen on " << g_my_addr.ipaddr 
	    << ": " << strerror(errno) << dendl;
    return -errno;
  }
  
  rank.rank_addr = g_my_addr;
  if (rank.rank_addr != entity_addr_t())
    rank.need_addr = false;
  else 
    rank.need_addr = true;
  if (rank.rank_addr.get_port() == 0) {
    entity_addr_t tmp;
    tmp.ipaddr = listen_addr;
    rank.rank_addr.set_port(tmp.get_port());
    if (force_nonce >= 0)
      rank.rank_addr.nonce = force_nonce;
    else
      rank.rank_addr.nonce = getpid(); // FIXME: pid might not be best choice here.
  }
  rank.rank_addr.erank = 0;

  dout(1) << "accepter.bind rank_addr is " << rank.rank_addr 
	  << " need_addr=" << rank.need_addr
	  << dendl;
  return 0;
}

int Rank::Accepter::start()
{
  dout(1) << "accepter.start" << dendl;
  // set up signal handler
  //old_sigint_handler = signal(SIGINT, simplemessenger_sigint);

  // set a harmless handle for SIGUSR1 (we'll use it to stop the accepter)
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = noop_signal_handler;
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGUSR1, &sa, NULL);
  sigaction(SIGPIPE, &sa, NULL);  // mask SIGPIPE too.  FIXME: i'm quite certain this is a roundabout way to do that.

  // start thread
  create();

  return 0;
}

void *Rank::Accepter::entry()
{
  dout(10) << "accepter starting" << dendl;
  
  fd_set fds;
  while (!done) {
    FD_ZERO(&fds);
    FD_SET(listen_sd, &fds);
    dout(20) << "accepter calling select" << dendl;
    int r = ::select(listen_sd+1, &fds, 0, &fds, 0);
    dout(20) << "accepter select got " << r << dendl;
    
    if (done) break;

    // accept
    struct sockaddr_in addr;
    socklen_t slen = sizeof(addr);
    int sd = ::accept(listen_sd, (sockaddr*)&addr, &slen);
    if (sd >= 0) {
      dout(10) << "accepted incoming on sd " << sd << dendl;
      
      // disable Nagle algorithm?
      if (g_conf.ms_tcp_nodelay) {
	int flag = 1;
	int r = ::setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
	if (r < 0) 
	  dout(0) << "accepter could't set TCP_NODELAY: " << strerror(errno) << dendl;
      }
      
      rank.lock.Lock();
      if (rank.num_local > 0) {
	Pipe *p = new Pipe(Pipe::STATE_ACCEPTING);
	p->sd = sd;
	p->start_reader();
	rank.pipes.insert(p);
      }
      rank.lock.Unlock();
    } else {
      dout(10) << "accepter no incoming connection?  sd = " << sd << " errno " << errno << " " << strerror(errno) << dendl;
    }
  }

  dout(20) << "accepter closing" << dendl;
  // don't close socket, in case we start up again?  blech.
  if (listen_sd >= 0) {
    ::close(listen_sd);
    listen_sd = -1;
  }
  dout(10) << "accepter stopping" << dendl;
  return 0;
}

void Rank::Accepter::stop()
{
  done = true;
  dout(10) << "stop sending SIGUSR1" << dendl;
  this->kill(SIGUSR1);
  join();
  done = false;
}




/********************************************
 * Rank
 */


/*
 * note: assumes lock is held
 */
void Rank::reaper()
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
    dout(10) << "reaper reaped pipe " << p << " " << p->get_peer_addr() << dendl;
    delete p;
    dout(10) << "reaper deleted pipe " << p << dendl;
  }
}


int Rank::bind(int64_t force_nonce)
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

class C_Debug : public Context {
  public:
  void finish(int) {
    int size = &g_conf.debug_after - &g_conf.debug;
    memcpy((char*)&g_conf.debug, (char*)&g_debug_after_conf.debug, size);
    dout(0) << "debug_after flipping debug settings" << dendl;
  }
};

int Rank::start(bool nodaemon)
{
  lock.Lock();
  if (started) {
    dout(10) << "rank.start already started" << dendl;
    lock.Unlock();
    return 0;
  }

  dout(1) << "rank.start at " << rank_addr << dendl;
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
    daemon(1, 0);  /* fixme.. we should chdir(/) too! */
    rename_output_file();
  }

  // some debug hackery?
  if (g_conf.kill_after) 
    g_timer.add_event_after(g_conf.kill_after, new C_Die);
  if (g_conf.debug_after) 
    g_timer.add_event_after(g_conf.debug_after, new C_Debug);

  // go!
  accepter.start();
  return 0;
}


/* connect_rank
 * NOTE: assumes rank.lock held.
 */
Rank::Pipe *Rank::connect_rank(const entity_addr_t& addr, const Policy& p)
{
  assert(rank.lock.is_locked());
  assert(addr != rank.rank_addr);
  
  dout(10) << "connect_rank to " << addr << ", creating pipe and registering" << dendl;
  
  // create pipe
  Pipe *pipe = new Pipe(Pipe::STATE_CONNECTING);
  pipe->policy = p;
  pipe->peer_addr = addr;
  pipe->start_writer();
  pipe->register_pipe();
  pipes.insert(pipe);

  return pipe;
}








/* register_entity 
 */
Rank::EntityMessenger *Rank::register_entity(entity_name_t name)
{
  dout(10) << "register_entity " << name << dendl;
  lock.Lock();
  
  // create messenger
  int erank = max_local;
  EntityMessenger *msgr = new EntityMessenger(name, erank);

  // add to directory
  max_local++;
  local.resize(max_local);
  stopped.resize(max_local);

  local[erank] = msgr;
  stopped[erank] = false;
  msgr->_myinst.addr = rank_addr;
  if (msgr->_myinst.addr.ipaddr == entity_addr_t().ipaddr)
    msgr->need_addr = true;
  msgr->_myinst.addr.erank = erank;

  dout(10) << "register_entity " << name << " at " << msgr->_myinst.addr 
	   << " need_addr=" << need_addr
	   << dendl;

  num_local++;
  
  lock.Unlock();
  return msgr;
}


void Rank::unregister_entity(EntityMessenger *msgr)
{
  lock.Lock();
  dout(10) << "unregister_entity " << msgr->get_myname() << dendl;
  
  // remove from local directory.
  local[msgr->my_rank] = 0;
  stopped[msgr->my_rank] = true;
  num_local--;

  wait_cond.Signal();

  lock.Unlock();
}


void Rank::submit_message(Message *m, const entity_addr_t& dest_addr, bool lazy)
{
  const entity_name_t dest = m->get_dest();

  // lookup
  entity_addr_t dest_proc_addr = dest_addr;
  dest_proc_addr.erank = 0;

  lock.Lock();
  {
    // local?
    if (rank_addr.is_local_to(dest_addr)) {
      if (dest_addr.erank < max_local && local[dest_addr.erank]) {
        // local
        dout(20) << "submit_message " << *m << " dest " << dest << " local" << dendl;
	local[dest_addr.erank]->queue_message(m);
      } else {
        derr(0) << "submit_message " << *m << " dest " << dest << " " << dest_addr << " local but not in local map?  dropping." << dendl;
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
	  dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_addr << ", ignoring old closed pipe." << dendl;
	  pipe->unregister_pipe();
	  pipe->lock.Unlock();
	  pipe = 0;
	} else {
	  dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_addr << ", have pipe." << dendl;
	  pipe->_send(m);
	  pipe->lock.Unlock();
	}
      }
      if (!pipe) {
	if (lazy) {
	  dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_addr << ", lazy, dropping." << dendl;
	  delete m;
	} else {
	  dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_addr << ", new pipe." << dendl;
	  // not connected.
	  pipe = connect_rank(dest_proc_addr, policy_map[m->get_dest().type()]);
	  pipe->send(m);
	}
      }
    }
  }

  lock.Unlock();
}





void Rank::wait()
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
  dout(20) << "wait: stopping accepter thread" << dendl;
  accepter.stop();
  dout(20) << "wait: stopped accepter thread" << dendl;

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
      (*i)->dirty_close();
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
  started = false;
}






/**********************************
 * EntityMessenger
 */

void Rank::EntityMessenger::dispatch_entry()
{
  lock.Lock();
  while (!stop) {
    if (!dispatch_queue.empty() || !prio_dispatch_queue.empty()) {
      list<Message*> ls;
      if (!prio_dispatch_queue.empty()) {
	ls.swap(prio_dispatch_queue);
	pqlen = 0;
      } else {
	if (0) {
	  ls.swap(dispatch_queue);
	  qlen = 0;
	} else {
	  // limit how much low-prio stuff we grab, to avoid starving high-prio messages!
	  ls.push_back(dispatch_queue.front());
	  dispatch_queue.pop_front();
	  qlen--;
	}
      }

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
	  if ((long)m == BAD_REMOTE_RESET) {
	    lock.Lock();
	    entity_addr_t a = remote_reset_q.front().first;
	    entity_name_t n = remote_reset_q.front().second;
	    remote_reset_q.pop_front();
	    lock.Unlock();
	    get_dispatcher()->ms_handle_remote_reset(a, n);
 	  } else if ((long)m == BAD_RESET) {
	    lock.Lock();
	    entity_addr_t a = reset_q.front().first;
	    entity_name_t n = reset_q.front().second;
	    reset_q.pop_front();
	    lock.Unlock();
	    get_dispatcher()->ms_handle_reset(a, n);
	  } else if ((long)m == BAD_FAILED) {
	    lock.Lock();
	    m = failed_q.front().first;
	    entity_inst_t i = failed_q.front().second;
	    failed_q.pop_front();
	    lock.Unlock();
	    get_dispatcher()->ms_handle_failure(m, i);
	  } else {
	    dout(1) << m->get_dest() 
		    << " <== " << m->get_source_inst()
		    << " ==== " << *m
		    << " ==== " << m->get_payload().length() << "+" << m->get_data().length()
		    << " (" << m->front_crc << " " << m->data_crc << ")"
		    << " " << m 
		    << dendl;
	    dispatch(m);
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
  rank.unregister_entity(this);
}

void Rank::EntityMessenger::ready()
{
  dout(10) << "ready " << get_myaddr() << dendl;
  assert(!dispatch_thread.is_started());
  
  // start my dispatch thread
  dispatch_thread.create();
}


int Rank::EntityMessenger::shutdown()
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

void Rank::EntityMessenger::suicide()
{
  dout(10) << "suicide " << get_myaddr() << dendl;
  shutdown();
  // hmm, or exit(0)?
}

void Rank::EntityMessenger::prepare_dest(const entity_inst_t& inst)
{
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(inst.addr) == 0)
      rank.connect_rank(inst.addr, rank.policy_map[inst.name.type()]);
  }
  rank.lock.Unlock();
}

int Rank::EntityMessenger::send_message(Message *m, entity_inst_t dest)
{
  // set envelope
  m->set_source_inst(_myinst);
  m->set_orig_source_inst(_myinst);
  m->set_dest_inst(dest);
 
  dout(1) << m->get_source()
          << " --> " << dest.name << " " << dest.addr
          << " -- " << *m
	  << " -- " << m
          << dendl;

  rank.submit_message(m, dest.addr);

  return 0;
}

int Rank::EntityMessenger::forward_message(Message *m, entity_inst_t dest)
{
  // set envelope
  m->set_source_inst(_myinst);
  m->set_dest_inst(dest);
 
  dout(1) << m->get_source()
          << " **> " << dest.name << " " << dest.addr
          << " -- " << *m
	  << " -- " << m
          << dendl;

  rank.submit_message(m, dest.addr);

  return 0;
}



int Rank::EntityMessenger::lazy_send_message(Message *m, entity_inst_t dest)
{
  // set envelope
  m->set_source_inst(_myinst);
  m->set_orig_source_inst(_myinst);
  m->set_dest_inst(dest);
 
  dout(1) << "lazy " << m->get_source()
          << " --> " << dest.name << " " << dest.addr
          << " -- " << *m
	  << " -- " << m
          << dendl;

  rank.submit_message(m, dest.addr, true);

  return 0;
}



void Rank::EntityMessenger::reset_myname(entity_name_t newname)
{
  entity_name_t oldname = get_myname();
  dout(10) << "reset_myname " << oldname << " to " << newname << dendl;
  _set_myname(newname);
}


void Rank::EntityMessenger::mark_down(entity_addr_t a)
{
  rank.mark_down(a);
}

void Rank::mark_down(entity_addr_t addr)
{
  lock.Lock();
  if (rank_pipe.count(addr)) {
    Pipe *p = rank_pipe[addr];
    dout(2) << "mark_down " << addr << " -- " << p << dendl;
    p->lock.Lock();
    p->stop();
    p->lock.Unlock();
  } else {
    dout(2) << "mark_down " << addr << " -- pipe dne" << dendl;
  }
  lock.Unlock();
}





/**************************************
 * Pipe
 */

#undef dout
#undef derr
#define dout(l)  if (l<=g_conf.debug_ms) *_dout << dbeginl << g_clock.now() << " " << pthread_self() << " -- " << rank.rank_addr << " >> " << peer_addr << " pipe(" << this << ")."
#define derr(l)  if (l<=g_conf.debug_ms) *_derr << dbeginl << g_clock.now() << " " << pthread_self() << " -- " << rank.rank_addr << " >> " << peer_addr << " pipe(" << this << ")."

int Rank::Pipe::accept()
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
  rc = tcp_write(sd, (char*)&rank.rank_addr, sizeof(rank.rank_addr));
  if (rc < 0) {
    dout(10) << "accept couldn't write my addr" << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  dout(10) << "accept sd=" << sd << dendl;
  
  // identify peer
  char banner[strlen(CEPH_BANNER)];
  rc = tcp_read(sd, banner, strlen(CEPH_BANNER));
  if (rc < 0) {
    dout(10) << "accept couldn't read banner" << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  if (memcmp(banner, CEPH_BANNER, strlen(CEPH_BANNER))) {
    dout(10) << "accept peer sent bad banner" << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  rc = tcp_read(sd, (char*)&peer_addr, sizeof(peer_addr));
  if (rc < 0) {
    dout(10) << "accept couldn't read peer_addr" << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  dout(10) << "accept peer addr is " << peer_addr << dendl;
  if (peer_addr.ipaddr.sin_addr.s_addr == htonl(INADDR_ANY)) {
    // peer apparently doesn't know what ip they have; figure it out for them.
    entity_addr_t old_addr = peer_addr;
    socklen_t len = sizeof(peer_addr.ipaddr);
    int r = ::getpeername(sd, (sockaddr*)&peer_addr.ipaddr, &len);
    if (r < 0) {
      dout(0) << "accept failed to getpeername " << errno << " " << strerror(errno) << dendl;
      state = STATE_CLOSED;
      return -1;
    }
    peer_addr.ipaddr.sin_port = old_addr.ipaddr.sin_port;
    dout(2) << "accept peer says " << old_addr << ", socket says " << peer_addr << dendl;
  }
  
  ceph_msg_connect connect;
  Pipe *existing = 0;
  
  // this should roughly mirror pseudocode at
  //  http://ceph.newdream.net/wiki/Messaging_protocol

  while (1) {
    rc = tcp_read(sd, (char*)&connect, sizeof(connect));
    if (rc < 0) {
      dout(10) << "accept couldn't read connect" << dendl;
      goto fail;
    }
    dout(20) << "accept got peer connect_seq " << connect.connect_seq
	     << " global_seq " << connect.global_seq
	     << dendl;
    
    rank.lock.Lock();

    // existing?
    if (rank.rank_pipe.count(peer_addr)) {
      existing = rank.rank_pipe[peer_addr];
      existing->lock.Lock();

      if (connect.global_seq < existing->peer_global_seq) {
	dout(10) << "accept existing " << existing << ".gseq " << existing->peer_global_seq
		 << " > " << connect.global_seq << ", RETRY_GLOBAL" << dendl;
	__le32 gseq;
	gseq = existing->peer_global_seq;  // so we can send it below..
	existing->lock.Unlock();
	rank.lock.Unlock();
	char tag = CEPH_MSGR_TAG_RETRY_GLOBAL;
	if (tcp_write(sd, &tag, 1) < 0)
	  goto fail;
	if (tcp_write(sd, (char*)&gseq, sizeof(gseq)) < 0)
	  goto fail;
	continue;
      }
      
      if (existing->policy.is_lossy()) {
	dout(-10) << "accept replacing existing (lossy) channel" << dendl;
	existing->was_session_reset();
	goto replace;
      }

      if (connect.connect_seq < existing->connect_seq) {
	if (connect.connect_seq == 0) {
	  dout(-10) << "accept peer reset, then tried to connect to us, replacing" << dendl;
	  existing->was_session_reset(); // this resets out_queue, msg_ and connect_seq #'s
	  goto replace;
	} else {
	  // old attempt, or we sent READY but they didn't get it.
	  dout(10) << "accept existing " << existing << ".cseq " << existing->connect_seq
		   << " > " << connect.connect_seq << ", RETRY_SESSION" << dendl;
	  __le32 cseq;
	  cseq = existing->connect_seq;  // so we can send it below..
	  existing->lock.Unlock();
	  rank.lock.Unlock();
	  char tag = CEPH_MSGR_TAG_RETRY_SESSION;
	  if (tcp_write(sd, &tag, 1) < 0)
	    goto fail;
	  if (tcp_write(sd, (char*)&cseq, sizeof(cseq)) < 0)
	    goto fail;
	  continue;
	}
      }

      if (connect.connect_seq == existing->connect_seq) {
	// connection race
	if (peer_addr < rank.rank_addr) {
	  // incoming wins
	  dout(10) << "accept connection race, existing " << existing << ".cseq " << existing->connect_seq
		   << " == " << connect.connect_seq << ", replacing my attempt" << dendl;
	  assert(existing->state == STATE_CONNECTING ||
		 existing->state == STATE_STANDBY ||
		 existing->state == STATE_WAIT);
	  goto replace;
	} else {
	  // our existing outgoing wins
	  dout(10) << "accept connection race, existing " << existing << ".cseq " << existing->connect_seq
		   << " == " << connect.connect_seq << ", sending WAIT" << dendl;
	  assert(peer_addr > rank.rank_addr);
	  assert(existing->state == STATE_CONNECTING); // this will win
	  existing->lock.Unlock();
	  rank.lock.Unlock();
	  
	  char tag = CEPH_MSGR_TAG_WAIT;
	  if (tcp_write(sd, &tag, 1) < 0)
	    goto fail;
	  continue;	
	}
      }

      assert(connect.connect_seq > existing->connect_seq);
      assert(connect.global_seq >= existing->peer_global_seq);
      if (existing->connect_seq == 0) {
	dout(0) << "accept we reset (peer sent cseq " << connect.connect_seq 
		 << ", " << existing << ".cseq = " << existing->connect_seq
		 << "), sending RESETSESSION" << dendl;
	rank.lock.Unlock();
	existing->lock.Unlock();
	char tag = CEPH_MSGR_TAG_RESETSESSION;
	if (tcp_write(sd, &tag, 1) < 0)
	  goto fail;
	continue;	
      } else {
	// reconnect
	dout(10) << "accept peer sent cseq " << connect.connect_seq
		 << " > " << existing->connect_seq << dendl;
	goto replace;
      }
      assert(0);
    } // existing
    else if (connect.connect_seq > 0) {
      // we reset, and they are opening a new session
      dout(0) << "accept we reset (peer sent cseq " << connect.connect_seq << "), sending RESETSESSION" << dendl;
      rank.lock.Unlock();
      char tag = CEPH_MSGR_TAG_RESETSESSION;
      if (tcp_write(sd, &tag, 1) < 0)
	goto fail;
      continue;	
    } else {
      // new session
      dout(10) << "accept new session" << dendl;
      goto open;
    }
    assert(0);    
  }
  
 replace:
  dout(10) << "accept replacing " << existing << dendl;
  existing->state = STATE_CLOSED;
  existing->cond.Signal();
  existing->reader_thread.kill(SIGUSR1);
  existing->unregister_pipe();
    
  // steal queue and out_seq
  out_seq = existing->out_seq;
  if (!existing->sent.empty()) {
    out_seq = existing->sent.front()->get_seq()-1;
    q.splice(q.begin(), existing->sent);
  }
  q.splice(q.end(), existing->q);
  
  existing->lock.Unlock();

 open:
  // open
  register_pipe();
  rank.lock.Unlock();

  connect_seq = connect.connect_seq + 1;
  peer_global_seq = connect.global_seq;
  dout(10) << "accept success, connect_seq = " << connect_seq << ", sending READY" << dendl;

  // send READY
  { 
    char tag = CEPH_MSGR_TAG_READY;
    if (tcp_write(sd, &tag, 1) < 0) 
      goto fail;
  }

  if (state != STATE_CLOSED) {
    dout(10) << "accept starting writer, " << "state=" << state << dendl;
    start_writer();
  }
  dout(20) << "accept done" << dendl;
  return 0;   // success.


 fail:
  state = STATE_CLOSED;
  fault();
  return -1;
}

int Rank::Pipe::connect()
{
  dout(10) << "connect " << connect_seq << dendl;
  assert(lock.is_locked());

  if (sd >= 0) {
    ::close(sd);
    sd = -1;
  }
  __u32 cseq = connect_seq;
  __u32 gseq = rank.get_global_seq();

  lock.Unlock();
  
  int newsd;
  char tag = -1;
  int rc;
  struct sockaddr_in myAddr;
  struct msghdr msg;
  struct iovec msgvec[2];
  int msglen;
  char banner[strlen(CEPH_BANNER)];
  entity_addr_t paddr;

  // create socket?
  newsd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (newsd < 0) {
    dout(-1) << "connect couldn't created socket " << strerror(errno) << dendl;
    assert(0);
    goto fail;
  }
  
  // bind any port
  myAddr.sin_family = AF_INET;
  myAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  myAddr.sin_port = htons( 0 );    
  rc = ::bind(newsd, (struct sockaddr *) &myAddr, sizeof(myAddr));
  assert(rc>=0);

  // connect!
  dout(10) << "connecting to " << peer_addr.ipaddr << dendl;
  rc = ::connect(newsd, (sockaddr*)&peer_addr.ipaddr, sizeof(peer_addr.ipaddr));
  if (rc < 0) {
    dout(2) << "connect error " << peer_addr.ipaddr
	     << ", " << errno << ": " << strerror(errno) << dendl;
    goto fail;
  }

  // disable Nagle algorithm?
  if (g_conf.ms_tcp_nodelay) {
    int flag = 1;
    int r = ::setsockopt(newsd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
    if (r < 0) 
      dout(0) << "connect couldn't set TCP_NODELAY: " << strerror(errno) << dendl;
  }

  // verify banner
  // FIXME: this should be non-blocking, or in some other way verify the banner as we get it.
  rc = tcp_read(newsd, (char*)&banner, strlen(CEPH_BANNER));
  if (rc < 0) {
    dout(2) << "connect couldn't read banner, " << strerror(errno) << dendl;
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
  if (do_sendmsg(newsd, &msg, msglen)) {
    dout(2) << "connect couldn't write my banner, " << strerror(errno) << dendl;
    goto fail;
  }

  // identify peer
  rc = tcp_read(newsd, (char*)&paddr, sizeof(paddr));
  if (rc < 0) {
    dout(2) << "connect couldn't read peer addr, " << strerror(errno) << dendl;
    goto fail;
  }
  dout(20) << "connect read peer addr " << paddr << " on socket " << newsd << dendl;
  if (!peer_addr.is_local_to(paddr)) {
    if (paddr.ipaddr.sin_addr.s_addr == 0 &&
	peer_addr.ipaddr.sin_port == paddr.ipaddr.sin_port) {
      dout(0) << "connect claims to be " 
	      << paddr << " not " << peer_addr << " - presumably this is the same node!" << dendl;
    } else {
      dout(0) << "connect claims to be " 
	      << paddr << " not " << peer_addr << " - wrong node!" << dendl;
      goto fail;
    }
  }
  
  // identify myself
  memset(&msg, 0, sizeof(msg));
  msgvec[0].iov_base = (char*)&rank.rank_addr;
  msgvec[0].iov_len = sizeof(rank.rank_addr);
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 1;
  msglen = msgvec[0].iov_len;
  if (do_sendmsg(newsd, &msg, msglen)) {
    dout(2) << "connect couldn't write my addr, " << strerror(errno) << dendl;
    goto fail;
  }
  dout(10) << "connect sent my addr " << rank.rank_addr << dendl;

  while (1) {
    ceph_msg_connect connect;
    connect.global_seq = gseq;
    connect.connect_seq = cseq;
    memset(&msg, 0, sizeof(msg));
    msgvec[0].iov_base = (char*)&connect;
    msgvec[0].iov_len = sizeof(connect);
    msg.msg_iov = msgvec;
    msg.msg_iovlen = 1;
    msglen = msgvec[0].iov_len;

    dout(10) << "connect sending gseq=" << gseq << " cseq=" << cseq << dendl;
    if (do_sendmsg(newsd, &msg, msglen)) {
      dout(2) << "connect couldn't write gseq, cseq, " << strerror(errno) << dendl;
      goto fail;
    }
    dout(20) << "connect wrote (self +) cseq, waiting for tag" << dendl;
    if (tcp_read(newsd, &tag, 1) < 0) {
      dout(2) << "connect read tag, seq, " << strerror(errno) << dendl;
      goto fail;
    }
    dout(20) << "connect got tag " << (int)tag << dendl;

    if (tag == CEPH_MSGR_TAG_RESETSESSION) {
      lock.Lock();
      if (state != STATE_CONNECTING) {
	dout(0) << "connect got RESETSESSION but no longer connecting" << dendl;
	goto stop_locked;
      }

      dout(0) << "connect got RESETSESSION" << dendl;
      was_session_reset();
      cseq = 0;
      lock.Unlock();
      continue;
    }
    if (tag == CEPH_MSGR_TAG_RETRY_GLOBAL) {
      int rc = tcp_read(newsd, (char*)&gseq, sizeof(gseq));
      if (rc < 0) {
	dout(0) << "connect got RETRY_GLOBAL tag but couldn't read gseq" << dendl;
	goto fail;
      }
      lock.Lock();
      if (state != STATE_CONNECTING) {
	dout(0) << "connect got RETRY_GLOBAL, but connection race or something, failing" << dendl;
	goto stop_locked;
      }
      gseq = rank.get_global_seq(gseq);
      dout(10) << "connect got RETRY_GLOBAL " << gseq << dendl;
      lock.Unlock();
      continue;
    }
    if (tag == CEPH_MSGR_TAG_RETRY_SESSION) {
      int rc = tcp_read(newsd, (char*)&cseq, sizeof(cseq));
      if (rc < 0) {
	dout(0) << "connect got RETRY_SESSION tag but couldn't read cseq" << dendl;
	goto fail;
      }
      lock.Lock();
      if (state != STATE_CONNECTING) {
	dout(0) << "connect got RETRY_SESSION, but connection race or something, failing" << dendl;
	goto stop_locked;
      }
      assert(cseq > connect_seq);
      dout(10) << "connect got RETRY_SESSION " << connect_seq << " -> " << cseq << dendl;
      connect_seq = cseq;
      lock.Unlock();
      continue;
    }

    if (tag == CEPH_MSGR_TAG_WAIT) {
      lock.Lock();
      if (state == STATE_CONNECTING) {
	dout(3) << "connect got WAIT (connection race), will wait" << dendl;
	state = STATE_WAIT;
      } else {
	dout(3) << "connect got WAIT (connection race), and lo, the wait is already over" << dendl;
      }
      goto stop_locked;
    }

    if (tag == CEPH_MSGR_TAG_READY) {
      lock.Lock();
      if (state != STATE_CONNECTING) {
	dout(3) << "connect got READY but no longer connecting?" << dendl;
	goto stop_locked;
      }

      // hooray!
      state = STATE_OPEN;
      sd = newsd;
      connect_seq = cseq+1;
      first_fault = last_attempt = utime_t();
      dout(20) << "connect success " << connect_seq << dendl;

      if (!reader_running) {
	dout(20) << "connect starting reader" << dendl;
	start_reader();
      }
      return 0;
    }
    
    // protocol error
    dout(0) << "connect got bad tag " << (int)tag << dendl;
    goto fail;
  }

 fail:
  lock.Lock();
  if (state == STATE_CONNECTING)
    fault();
  else
    dout(3) << "connect fault, but state != connecting, stopping" << dendl;

 stop_locked:
  if (newsd >= 0) 
    ::close(newsd);
  return -1;
}

void Rank::Pipe::register_pipe()
{
  dout(10) << "register_pipe" << dendl;
  assert(rank.lock.is_locked());
  assert(rank.rank_pipe.count(peer_addr) == 0);
  rank.rank_pipe[peer_addr] = this;
}

void Rank::Pipe::unregister_pipe()
{
  assert(rank.lock.is_locked());
  if (rank.rank_pipe.count(peer_addr) &&
      rank.rank_pipe[peer_addr] == this) {
    dout(10) << "unregister_pipe" << dendl;
    rank.rank_pipe.erase(peer_addr);
  } else {
    dout(10) << "unregister_pipe - not registered" << dendl;
  }
}

void Rank::Pipe::fault(bool onconnect)
{
  assert(lock.is_locked());
  cond.Signal();

  if (!onconnect) dout(2) << "fault " << errno << ": " << strerror(errno) << dendl;

  if (state == STATE_CLOSED) {
    dout(10) << "fault already closed" << dendl;
    return;
  }

  if (sd >= 0)
    ::close(sd);
  sd = -1;

  // lossy channel?
  if (policy.is_lossy()) {
    dout(10) << "fault on lossy channel, failing" << dendl;
    fail();
    return;
  }

  if (q.empty()) {
    if (state == STATE_CLOSING || onconnect || policy.is_lossy()) {
      dout(10) << "fault on connect, or already closing, and q empty: setting closed." << dendl;
      state = STATE_CLOSED;
    } else {
      dout(0) << "fault nothing to send, going to standby" << dendl;
      state = STATE_STANDBY;
    }
    return;
  } 

  utime_t now = g_clock.now();
  if (state != STATE_CONNECTING) {
    if (!onconnect) dout(0) << "fault initiating reconnect" << dendl;
    connect_seq++;
    state = STATE_CONNECTING;
    first_fault = now;
  } else if (first_fault.sec() == 0) {
    if (!onconnect) dout(0) << "fault first fault" << dendl;
    first_fault = now;
  } else {
    utime_t failinterval = now - first_fault;
    utime_t retryinterval = now - last_attempt;
    if (!onconnect) dout(10) << "fault failure was " << failinterval 
			     << " ago, last attempt was at " << last_attempt
			     << ", " << retryinterval << " ago" << dendl;
    if (policy.fail_interval > 0 && failinterval > policy.fail_interval) {
      // give up
      dout(0) << "fault giving up" << dendl;
      fail();
    } else if (retryinterval < policy.retry_interval) {
      // wait
      now += (policy.retry_interval - retryinterval);
      dout(10) << "fault waiting until " << now << dendl;
      cond.WaitUntil(lock, now);
      dout(10) << "fault done waiting or woke up" << dendl;
    }
  }
  last_attempt = now;
}

void Rank::Pipe::fail()
{
  derr(10) << "fail" << dendl;
  assert(lock.is_locked());

  stop();
  report_failures();

  for (unsigned i=0; i<rank.local.size(); i++) 
    if (rank.local[i] && rank.local[i]->get_dispatcher())
      rank.local[i]->queue_reset(peer_addr, last_dest_name);
}

void Rank::Pipe::was_session_reset()
{
  dout(10) << "was_session_reset" << dendl;
  report_failures();
  for (unsigned i=0; i<rank.local.size(); i++) 
    if (rank.local[i] && rank.local[i]->get_dispatcher())
      rank.local[i]->queue_remote_reset(peer_addr, last_dest_name);

  // renumber outgoing seqs
  out_seq = 0;
  for (list<Message*>::iterator p = q.begin(); p != q.end(); p++)
    (*p)->set_seq(++out_seq);

  in_seq = 0;
  connect_seq = 0;
}

void Rank::Pipe::report_failures()
{
  // report failures
  q.splice(q.begin(), sent);
  while (!q.empty()) {
    Message *m = q.front();
    q.pop_front();

    if (policy.drop_msg_callback) {
      unsigned srcrank = m->get_source_inst().addr.erank;
      if (srcrank >= rank.max_local || rank.local[srcrank] == 0) {
	dout(1) << "fail on " << *m << ", srcrank " << srcrank << " dne, dropping" << dendl;
	delete m;
      } else if (rank.local[srcrank]->is_stopped()) {
	dout(1) << "fail on " << *m << ", dispatcher stopping, ignoring." << dendl;
	delete m;
      } else {
	dout(10) << "fail on " << *m << dendl;
	rank.local[srcrank]->queue_failure(m, m->get_dest_inst());
      }
    }
  }
}

void Rank::Pipe::stop()
{
  dout(10) << "stop" << dendl;
  assert(lock.is_locked());

  cond.Signal();
  state = STATE_CLOSED;
  if (sd >= 0)
    ::close(sd);
  sd = -1;

  // deactivate myself
  lock.Unlock();
  rank.lock.Lock();
  unregister_pipe();
  rank.lock.Unlock();
  lock.Lock();
}



void Rank::Pipe::dirty_close()
{
  dout(10) << "dirty_close" << dendl;
  lock.Lock();
  state = STATE_CLOSING;
  cond.Signal();
  dout(10) << "dirty_close sending SIGUSR1" << dendl;
  reader_thread.kill(SIGUSR1);
  lock.Unlock();
}


/* read msgs from socket.
 * also, server.
 */
void Rank::Pipe::reader()
{
  lock.Lock();

  if (state == STATE_ACCEPTING) 
    accept();

  // loop.
  while (state != STATE_CLOSED) {
    assert(lock.is_locked());

    // sleep if (re)connecting
    if (state == STATE_CONNECTING ||
	state == STATE_STANDBY) {
      dout(20) << "reader sleeping during reconnect|standby" << dendl;
      cond.Wait(lock);
      continue;
    }

    lock.Unlock();

    char tag = -1;
    dout(20) << "reader reading tag..." << dendl;
    int rc = tcp_read(sd, (char*)&tag, 1);
    if (rc < 0) {
      lock.Lock();
      dout(2) << "reader couldn't read tag, " << strerror(errno) << dendl;
      fault();
      continue;
    }

    // open ...
    if (tag == CEPH_MSGR_TAG_ACK) {
      dout(20) << "reader got ACK" << dendl;
      __u32 seq;
      int rc = tcp_read( sd, (char*)&seq, sizeof(seq));
      lock.Lock();
      if (rc < 0) {
	dout(2) << "reader couldn't read ack seq, " << strerror(errno) << dendl;
	fault();
      } else {
	dout(15) << "reader got ack seq " << seq << dendl;
	// trim sent list
	while (!sent.empty() &&
	       sent.front()->get_seq() <= seq) {
	  Message *m = sent.front();
	  sent.pop_front();
	  dout(10) << "reader got ack seq " 
		    << seq << " >= " << m->get_seq() << " on " << m << " " << *m << dendl;
	  delete m;
	}
      }
      continue;
    }

    else if (tag == CEPH_MSGR_TAG_MSG) {
      dout(20) << "reader got MSG" << dendl;
      Message *m = read_message();
      if (!m) {
	derr(2) << "reader read null message, " << strerror(errno) << dendl;
	lock.Lock();
	fault();
	continue;
      }

      // note received seq#
      lock.Lock();
      if (m->get_seq() <= in_seq) {
	dout(-10) << "reader got old message "
		  << m->get_seq() << " <= " << in_seq << " " << m << " " << *m
		  << " for " << m->get_dest() 
		  << ", discarding" << dendl;
	delete m;
	continue;
      }
      in_seq++;

      if (in_seq == 1) 
	policy = rank.policy_map[m->get_source().type()];  /* apply policy */

      if (!policy.is_lossy() && in_seq != m->get_seq()) {
	dout(0) << "reader got bad seq " << m->get_seq() << " expected " << in_seq
		<< " for " << *m << " from " << m->get_source() << dendl;
	derr(0) << "reader got bad seq " << m->get_seq() << " expected " << in_seq
		<< " for " << *m << " from " << m->get_source() << dendl;
	assert(in_seq == m->get_seq()); // for now!
	fault();
	delete m;
	continue;
      }

      cond.Signal();  // wake up writer, to ack this
      lock.Unlock();
      
      dout(10) << "reader got message "
	       << m->get_seq() << " " << m << " " << *m
	       << " for " << m->get_dest() << dendl;
      
      // deliver
      EntityMessenger *entity = 0;
      
      rank.lock.Lock();
      {
	unsigned erank = m->get_dest_inst().addr.erank;
	if (erank < rank.max_local && rank.local[erank]) {
	  // find entity
	  entity = rank.local[erank];

	  // first message?
	  if (entity->need_addr) {
	    entity->_set_myaddr(m->get_dest_inst().addr);
	    dout(2) << "reader entity addr is " << entity->get_myaddr() << dendl;
	    entity->need_addr = false;
	  }

	  if (rank.need_addr) {
	    rank.rank_addr = m->get_dest_inst().addr;
	    rank.rank_addr.erank = 0;
	    dout(2) << "reader rank_addr is " << rank.rank_addr << dendl;
	    rank.need_addr = false;
	  }

	} else {
	  derr(0) << "reader got message " << *m << " for " << m->get_dest() << ", which isn't local" << dendl;
	}
      }
      rank.lock.Unlock();
      
      if (entity)
	entity->queue_message(m);        // queue

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
      fault();
    }
  }

 
  // reap?
  bool reap = false;
  reader_running = false;
  if (!writer_running) reap = true;

  lock.Unlock();

  if (reap) {
    dout(10) << "reader queueing for reap" << dendl;
    if (sd >= 0) ::close(sd);
    rank.lock.Lock();
    {
      rank.pipe_reap_queue.push_back(this);
      rank.wait_cond.Signal();
    }
    rank.lock.Unlock();
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
void Rank::Pipe::writer()
{
  lock.Lock();

  while (state != STATE_CLOSED) { // && state != STATE_WAIT) {
    // standby?
    if (!q.empty() && state == STATE_STANDBY)
      state = STATE_CONNECTING;

    // connect?
    if (state == STATE_CONNECTING) {
      connect();
      continue;
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
	(!q.empty() || in_seq > in_seq_acked)) {

      // send ack?
      if (in_seq > in_seq_acked) {
	int send_seq = in_seq;
	lock.Unlock();
	int rc = write_ack(send_seq);
	lock.Lock();
	if (rc < 0) {
	  dout(2) << "writer couldn't write ack, " << strerror(errno) << dendl;
	  fault();
 	  continue;
	}
	in_seq_acked = send_seq;
      }

      // grab outgoing message
      if (!q.empty()) {
	Message *m = q.front();
	q.pop_front();
	m->set_seq(++out_seq);
	lock.Unlock();

        dout(20) << "writer encoding " << m->get_seq() << " " << m << " " << *m << dendl;

	// encode and copy out of *m
        if (m->empty_payload()) 
	  m->encode_payload();
	bufferlist payload, data;
	payload.claim(m->get_payload());
	data.claim(m->get_data());
	ceph_msg_header hdr = m->get_header();

	lock.Lock();
	sent.push_back(m); // move to sent list
	lock.Unlock();

        dout(20) << "writer sending " << m->get_seq() << " " << m << dendl;
	int rc = write_message(m, &hdr, payload, data);
	lock.Lock();
	
	if (rc < 0) {
          derr(1) << "writer error sending " << m << " to " << hdr.dst << ", "
		  << errno << ": " << strerror(errno) << dendl;
	  fault();
        }
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
    if (sd >= 0) ::close(sd);
    rank.lock.Lock();
    {
      rank.pipe_reap_queue.push_back(this);
      rank.wait_cond.Signal();
    }
    rank.lock.Unlock();
  }

  dout(10) << "writer done" << dendl;
}


Message *Rank::Pipe::read_message()
{
  // envelope
  //dout(10) << "receiver.read_message from sd " << sd  << dendl;
  
  ceph_msg_header header; 
  ceph_msg_footer footer;

  if (tcp_read( sd, (char*)&header, sizeof(header) ) < 0)
    return 0;
  
  dout(20) << "reader got envelope type=" << header.type
           << " src " << header.src << " dst " << header.dst
           << " front=" << header.front_len
	   << " data=" << header.data_len
	   << " off " << header.data_off
           << dendl;

  // verify header crc
  __u32 header_crc = crc32c_le(0, (unsigned char *)&header, sizeof(header) - sizeof(header.crc));
  if (header_crc != header.crc) {
    dout(0) << "reader got bad header crc " << header_crc << " != " << header_crc << dendl;
    return 0;
  }

  // ok, now it's safe to change the header..
  // munge source address?
  if (header.src.addr.ipaddr.sin_addr.s_addr == htonl(INADDR_ANY)) {
    dout(10) << "reader munging src addr " << header.src << " to be " << peer_addr << dendl;
    header.orig_src.addr.ipaddr = header.src.addr.ipaddr = peer_addr.ipaddr;
  }

  // read front
  bufferlist front;
  bufferptr bp;
  int front_len = header.front_len;
  if (front_len) {
    bp = buffer::create(front_len);
    if (tcp_read( sd, bp.c_str(), front_len ) < 0) 
      return 0;
    front.push_back(bp);
    dout(20) << "reader got front " << front.length() << dendl;
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
      bp = buffer::create(head);
      if (tcp_read( sd, bp.c_str(), head ) < 0) 
	return 0;
      data.push_back(bp);
      left -= head;
      dout(20) << "reader got data head " << head << dendl;
    }

    // middle
    int middle = left & PAGE_MASK;
    if (middle > 0) {
      bp = buffer::create_page_aligned(middle);
      if (tcp_read( sd, bp.c_str(), middle ) < 0) 
	return 0;
      data.push_back(bp);
      left -= middle;
      dout(20) << "reader got data page-aligned middle " << middle << dendl;
    }

    if (left) {
      bp = buffer::create(left);
      if (tcp_read( sd, bp.c_str(), left ) < 0) 
	return 0;
      data.push_back(bp);
      dout(20) << "reader got data tail " << left << dendl;
    }
  }

  // footer
  if (tcp_read(sd, (char*)&footer, sizeof(footer)) < 0) 
    return 0;
  
  dout(10) << "aborted = " << le32_to_cpu(footer.aborted) << dendl;
  if (le32_to_cpu(footer.aborted)) {
    dout(0) << "reader got " << front.length() << " + " << data.length()
	    << " byte message from " << header.src << ".. ABORTED" << dendl;
    // MEH FIXME 
    Message *m = new MGenericMessage(CEPH_MSG_PING);
    header.type = CEPH_MSG_PING;
    m->set_header(header);
    return m;
  }

  dout(20) << "reader got " << front.length() << " + " << data.length()
	   << " byte message from " << header.src << dendl;
  return decode_message(header, footer, front, data);
}


int Rank::Pipe::do_sendmsg(int sd, struct msghdr *msg, int len)
{
  while (len > 0) {
    if (0) { // sanity
      int l = 0;
      for (unsigned i=0; i<msg->msg_iovlen; i++)
	l += msg->msg_iov[i].iov_len;
      assert(l == len);
    }

    int r = ::sendmsg(sd, msg, 0);
    if (r == 0) 
      dout(10) << "do_sendmsg hmm do_sendmsg got r==0!" << dendl;
    if (r < 0) { 
      dout(1) << "do_sendmsg error " << strerror(errno) << dendl;
      return -1;
    }
    if (state == STATE_CLOSED) {
      dout(10) << "do_sendmsg oh look, state == CLOSED, giving up" << dendl;
      errno = EINTR;
      return -1; // close enough
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
	msg->msg_iov[0].iov_base = (void*)((long)msg->msg_iov[0].iov_base + r);
	msg->msg_iov[0].iov_len -= r;
	break;
      }
    }
  }
  return 0;
}


int Rank::Pipe::write_ack(unsigned seq)
{
  dout(10) << "write_ack " << seq << dendl;

  char c = CEPH_MSGR_TAG_ACK;
  __le32 s;
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
  
  if (do_sendmsg(sd, &msg, 5) < 0) 
    return -1;	
  return 0;
}


int Rank::Pipe::write_message(Message *m, ceph_msg_header *header, 
			      bufferlist &payload, bufferlist &data)
{
  struct ceph_msg_footer f;
  memset(&f, 0, sizeof(f));

  // get envelope, buffers
  header->front_len = payload.length();
  header->data_len = data.length();

  // calculate header, footer crc
  header->crc = crc32c_le(0, (unsigned char*)header, sizeof(*header) - sizeof(header->crc));
  f.front_crc = payload.crc32c(0);
  f.data_crc = data.crc32c(0);

  bufferlist blist = payload;
  blist.append(data);
  
  dout(20)  << "write_message " << m << " to " << header->dst << dendl;
  
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
  msgvec[msg.msg_iovlen].iov_base = (char*)header;
  msgvec[msg.msg_iovlen].iov_len = sizeof(*header);
  msglen += sizeof(*header);
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
      if (do_sendmsg(sd, &msg, msglen)) 
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

  // send data footer
  msgvec[msg.msg_iovlen].iov_base = (void*)&f;
  msgvec[msg.msg_iovlen].iov_len = sizeof(f);
  msglen += sizeof(f);
  msg.msg_iovlen++;

  // send
  if (do_sendmsg(sd, &msg, msglen)) 
    return -1;	

  return 0;
}


