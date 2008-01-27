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


sighandler_t old_sigint_handler = 0;


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
  if (accepter.listen_sd > 0) 
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

int Rank::Accepter::bind()
{
  // bind to a socket
  dout(10) << "accepter.bind" << dendl;
  
  char hostname[100];
  memset(hostname, 0, 100);
  gethostname(hostname, 100);
  dout(2) << "accepter.bind my hostname is " << hostname << dendl;

  // is there a .ceph_hosts file?
  if (g_conf.ms_hosts) {
    ifstream fh;
    fh.open(g_conf.ms_hosts);
    if (fh.is_open()) {
      while (1) {
	string line;
	getline(fh, line);
	if (fh.eof()) break;
	if (line[0] == '#' || line[0] == ';') continue;
	int ospace = line.find(" ");
	if (!ospace) continue;
	string host = line.substr(0, ospace);
	string addr = line.substr(ospace+1);
	dout(15) << g_conf.ms_hosts << ": host '" << host << "' -> '" << addr << "'" << dendl;
	if (host == hostname) {
	  parse_ip_port(addr.c_str(), g_my_addr);
	  dout(1) << g_conf.ms_hosts << ": my addr is " << g_my_addr << dendl;
	  break;
	}
      }
      fh.close();
    }
  }

  // use whatever user specified (if anything)
  sockaddr_in listen_addr = g_my_addr.v.ipaddr;

  /* socket creation */
  listen_sd = ::socket(AF_INET, SOCK_STREAM, 0);
  assert(listen_sd > 0);

  int on = 1;
  ::setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  
  /* bind to port */
  int rc = ::bind(listen_sd, (struct sockaddr *) &listen_addr, sizeof(listen_addr));
  if (rc < 0) 
    derr(0) << "accepter.bind unable to bind to " << g_my_addr.v.ipaddr << dendl;
  assert(rc >= 0);

  // what port did we get?
  socklen_t llen = sizeof(listen_addr);
  getsockname(listen_sd, (sockaddr*)&listen_addr, &llen);
  
  dout(10) << "accepter.bind bound to " << listen_addr << dendl;

  // listen!
  rc = ::listen(listen_sd, 128);
  assert(rc >= 0);
  
  // figure out my_addr
  if (g_my_addr != entity_addr_t()) {
    // user specified it, easy peasy.
    rank.rank_addr = g_my_addr;
  } else {
    // my IP is... HELP!
    struct hostent *myhostname = gethostbyname(hostname); 
    if (!myhostname) {
      derr(0) << "unable to resolve hostname '" << hostname 
	      << "', please specify your ip with --bind x.x.x.x" 
	      << dendl;
      exit(0);
    }
    
    // look up my hostname.
    listen_addr.sin_family = myhostname->h_addrtype;
    memcpy((char*)&listen_addr.sin_addr.s_addr, 
	   myhostname->h_addr_list[0], 
	   myhostname->h_length);
    rank.rank_addr.v.ipaddr = listen_addr;
    rank.rank_addr.set_port(0);
  }
  if (rank.rank_addr.get_port() == 0) {
    entity_addr_t tmp;
    tmp.v.ipaddr = listen_addr;
    rank.rank_addr.set_port(tmp.get_port());
    rank.rank_addr.v.nonce = getpid(); // FIXME: pid might not be best choice here.
  }
  rank.rank_addr.v.erank = 0;

  dout(1) << "accepter.bind rank_addr is " << rank.rank_addr << dendl;
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
    if (sd > 0) {
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
      dout(10) << "no incoming connection?" << dendl;
    }
  }

  dout(20) << "accepter closing" << dendl;
  if (listen_sd > 0) ::close(listen_sd);
  dout(10) << "accepter stopping" << dendl;
  return 0;
}

void Rank::Accepter::stop()
{
  done = true;
  dout(10) << "stop sending SIGUSR1" << dendl;
  this->kill(SIGUSR1);
  join();
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


int Rank::bind()
{
  lock.Lock();
  if (started) {
    dout(10) << "rank.bind already started" << dendl;
    lock.Unlock();
    return 0;
  }
  dout(10) << "rank.bind" << dendl;
  lock.Unlock();

  // bind to a socket
  return accepter.bind();
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

int Rank::start()
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
  if (g_conf.daemonize) {
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
  msgr->_myinst.addr.v.erank = erank;

  dout(10) << "register_entity " << name << " at " << msgr->_myinst.addr << dendl;

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


void Rank::submit_message(Message *m, const entity_addr_t& dest_addr)
{
  const entity_name_t dest = m->get_dest();

  // lookup
  entity_addr_t dest_proc_addr = dest_addr;
  dest_proc_addr.v.erank = 0;

  lock.Lock();
  {
    // local?
    if (ceph_entity_addr_is_local(dest_addr.v, rank_addr.v)) {
      if (dest_addr.v.erank < max_local && local[dest_addr.v.erank]) {
        // local
        dout(20) << "submit_message " << *m << " dest " << dest << " local" << dendl;
	local[dest_addr.v.erank]->queue_message(m);
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
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_addr << ", new pipe." << dendl;
        // not connected.
        pipe = connect_rank(dest_proc_addr, policy_map[m->get_dest().type()]);
	pipe->send(m);
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
		    << " ==== " << m 
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
  m->set_dest_inst(dest);
 
  dout(1) << m->get_source()
          << " --> " << dest.name << " " << dest.addr
          << " -- " << *m
	  << " -- " << m
          << dendl;

  rank.submit_message(m, dest.addr);

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
    dout(0) << "mark_down " << addr << " -- " << p << dendl;
    p->lock.Lock();
    p->stop();
    p->lock.Unlock();
  } else {
    dout(0) << "mark_down " << addr << " -- pipe dne" << dendl;
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

/*
 * we have to be careful about connection races:
 *  A initiates connection
 *  B initiates connection
 *  B accepts A's connection
 *  A rejects B's connection   (or vice-versa)
 * 
 * this is controlled by whether accept uses the new incoming socket
 * as the new pipe.  two cases:
 *  old         new(incoming)
 *  connecting  connecting   -> use socket initiated by lower address
 *  open        connecting 
 *   -> use new socket _only_ if connect_seq matches.  that is, the
 *      peer reconnected subsequent to the current open socket.  if
 *      connect_seq _doesn't_ match, it means that it is an 'old' attempt.
 */

int Rank::Pipe::accept()
{
  dout(10) << "accept" << dendl;

  // my creater gave me sd via accept()
  assert(state == STATE_ACCEPTING);
  
  // announce myself.
  int rc = tcp_write(sd, (char*)&rank.rank_addr, sizeof(rank.rank_addr));
  if (rc < 0) {
    dout(10) << "accept couldn't write my addr" << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  dout(10) << "accept sd=" << sd << dendl;
  
  // identify peer
  rc = tcp_read(sd, (char*)&peer_addr, sizeof(peer_addr));
  if (rc < 0) {
    dout(10) << "accept couldn't read peer addr" << dendl;
    state = STATE_CLOSED;
    return -1;
  }
  if (peer_addr.v.ipaddr.sin_addr.s_addr == htonl(INADDR_ANY)) {
    // peer apparently doesn't know what ip they have; figure it out for them.
    entity_addr_t old_addr = peer_addr;
    socklen_t len = sizeof(peer_addr.v.ipaddr);
    int r = ::getpeername(sd, (sockaddr*)&peer_addr.v.ipaddr, &len);
    if (r < 0) {
      dout(0) << "accept failed to getpeername " << errno << " " << strerror(errno) << dendl;
      state = STATE_CLOSED;
      return -1;
    }
    dout(2) << "accept peer says " << old_addr << ", socket says " << peer_addr << dendl;
  }

  __u32 cseq;
  rc = tcp_read(sd, (char*)&cseq, sizeof(cseq));
  if (rc < 0) {
    dout(10) << "accept couldn't read connect seq" << dendl;
    state = STATE_CLOSED;
    return -1;
  }

  dout(20) << "accept got connect_seq " << cseq << dendl;

  __u32 myseq = connect_seq = 1;
    
  // register pipe.
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_addr) == 0) {
      dout(10) << "accept new peer " << peer_addr << dendl;
      register_pipe();
    } else {
      // hmm!
      Pipe *other = rank.rank_pipe[peer_addr];
      other->lock.Lock();

      dout(10) << "accept got connect_seq " << cseq 
	       << ", existing pipe connect_seq " << other->connect_seq
	       << " state " << other->state
	       << dendl;

      // if open race, low addr's pipe "wins".
      // otherwise, look at connect_seq
      if ((other->state == STATE_CONNECTING && peer_addr < rank.rank_addr) ||
	  (other->state == STATE_OPEN && cseq == other->connect_seq)) {
	dout(10) << "accept already had pipe " << other
		 << ", but switching to this new one" << dendl;
	// switch to this new Pipe
	other->state = STATE_CLOSED;
	assert(q.empty());
	other->cond.Signal();
	other->unregister_pipe();
	register_pipe();

	// steal queue and out_seq
	out_seq = other->out_seq;
	if (!other->sent.empty()) {
	  out_seq = other->sent.front()->get_seq()-1;
	  q.splice(q.begin(), other->sent);
	}
	q.splice(q.end(), other->q);
      } 
      else {
	dout(10) << "accept already had pipe " << other
		 << ", closing this one" << dendl;
	myseq = other->connect_seq;
	state = STATE_CLOSED;
      }
      other->lock.Unlock();
    } 
  }
  rank.lock.Unlock();

  char tag;
  if (state == STATE_CLOSED) {
    dout(10) << "accept closed, sending REJECT tag" << dendl;
    tag = CEPH_MSGR_TAG_REJECT;
  } else {
    dout(10) << "accept sending READY tag" << dendl;
    tag = CEPH_MSGR_TAG_READY;
    state = STATE_OPEN;
    kick_reader_on_join = true;
  }

  if (tcp_write(sd, &tag, 1) < 0 ||
      tcp_write(sd, (char*)&myseq, sizeof(myseq)) < 0) {
    dout(2) << "accept couldn't send initial tag+seq: "
	    << strerror(errno) << dendl;
    fault();
  }

  if (state != STATE_CLOSED) {
    dout(10) << "accept starting writer, "
	     << "state=" << state << dendl;
    start_writer();
  }

  dout(20) << "accept done" << dendl;
  return 0;   // success.
}

int Rank::Pipe::connect()
{
  dout(10) << "connect " << connect_seq << dendl;
  assert(lock.is_locked());

  if (sd > 0) {
    ::close(sd);
    sd = 0;
  }
  __u32 cseq = connect_seq;
  __u32 rseq;

  lock.Unlock();

  int newsd;
  char tag = -1;
  int rc;
  struct sockaddr_in myAddr;
  entity_addr_t paddr;
  struct msghdr msg;
  struct iovec msgvec[2];
  int msglen;

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
  dout(10) << "connecting to " << peer_addr.v.ipaddr << dendl;
  rc = ::connect(newsd, (sockaddr*)&peer_addr.v.ipaddr, sizeof(peer_addr.v.ipaddr));
  if (rc < 0) {
    dout(2) << "connect error " << peer_addr.v.ipaddr
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

  // identify peer
  rc = tcp_read(newsd, (char*)&paddr, sizeof(paddr));
  if (rc < 0) {
    dout(2) << "connect couldn't read peer addr, " << strerror(errno) << dendl;
    goto fail;
  }
  dout(20) << "connect read peer addr " << paddr << dendl;
  if (!ceph_entity_addr_is_local(peer_addr.v, paddr.v)) {
    dout(0) << "connect peer identifies itself as " 
	    << paddr << "... wrong node!" << dendl;
    goto fail;
  }

  // identify myself, and send open seq
  memset(&msg, 0, sizeof(msg));
  msgvec[0].iov_base = (char*)&rank.rank_addr;
  msgvec[0].iov_len = sizeof(rank.rank_addr);
  msgvec[1].iov_base = (char*)&cseq;
  msgvec[1].iov_len = sizeof(cseq);
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 2;
  msglen = msgvec[0].iov_len + msgvec[1].iov_len;

  if (do_sendmsg(newsd, &msg, msglen)) {
    dout(2) << "connect couldn't write self, seq, " << strerror(errno) << dendl;
    goto fail;
  }

  dout(20) << "connect wrote self, seq, waiting for tag" << dendl;

  // wait for tag
  if (tcp_read(newsd, &tag, 1) < 0 ||
      tcp_read(newsd, (char*)&rseq, sizeof(rseq)) < 0) {
    dout(2) << "connect read tag, seq, " << strerror(errno) << dendl;
    goto fail;
  }

  dout(20) << "connect got initial tag " << (int)tag << " + seq " << rseq << dendl;

  lock.Lock();

  // FINISH
  if (state != STATE_CONNECTING) {
    dout(2) << "connect hmm, race durring connect(), not connecting anymore, failing" << dendl;
    goto fail_locked;  // hmm!
  }
  if (tag == CEPH_MSGR_TAG_REJECT) {
    if (connect_seq != rseq) {
      dout(0) << "connect got REJECT, old connect_seq was " << connect_seq
	      << ", taking new " << rseq << dendl;
      connect_seq = rseq;
    } else {
      dout(10) << "connect got REJECT, connection race (harmless), connect_seq=" << connect_seq << dendl;
    }
    goto fail_locked;
  }
  assert(tag == CEPH_MSGR_TAG_READY);
  state = STATE_OPEN;
  this->sd = newsd;
  connect_seq++;
  if (rseq != connect_seq) {
    dout(0) << "connect REMOTE RESET: my seq = " << connect_seq << ", remote seq = " << rseq << dendl;
    if (rseq < connect_seq) {
      connect_seq = rseq;
      report_failures();
      for (unsigned i=0; i<rank.local.size(); i++) 
	if (rank.local[i] && rank.local[i]->get_dispatcher())
	  rank.local[i]->queue_remote_reset(peer_addr, last_dest_name);
      // renumber outgoing seqs
      out_seq = 0;
      for (list<Message*>::iterator p = q.begin(); p != q.end(); p++)
	(*p)->set_seq(++out_seq);
      in_seq = 0;
    } else {
      dout(0) << "WTF" << dendl;
      assert(0);
    }
  }
  first_fault = last_attempt = utime_t();
  dout(20) << "connect success " << connect_seq << dendl;

  if (!reader_running) {
    dout(20) << "connect starting reader" << dendl;
    start_reader();
  }
  return 0;

 fail:
  lock.Lock();
 fail_locked:
  if (newsd > 0) ::close(newsd);
  fault(tag == CEPH_MSGR_TAG_REJECT); // quiet if reject (not socket error)
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
  if (q.empty()) {
    if (state == STATE_CLOSING || onconnect) {
      dout(10) << "fault on connect, or already closing, and q empty: setting closed." << dendl;
      state = STATE_CLOSED;
    } else {
      dout(0) << "fault nothing to send, going to standby" << dendl;
      state = STATE_STANDBY;
    }
    ::close(sd);
    sd = 0;
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

void Rank::Pipe::report_failures()
{
  // report failures
  q.splice(q.begin(), sent);
  while (!q.empty()) {
    Message *m = q.front();
    q.pop_front();

    if (policy.drop_msg_callback) {
      unsigned srcrank = m->get_source_inst().addr.v.erank;
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
  ::close(sd);
  sd = 0;

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
      assert(in_seq == m->get_seq());
      cond.Signal();  // wake up writer, to ack this
      lock.Unlock();
      
      dout(10) << "reader got message "
	       << m->get_seq() << " " << m << " " << *m
	       << " for " << m->get_dest() << dendl;
      
      // deliver
      EntityMessenger *entity = 0;
      
      rank.lock.Lock();
      {
	unsigned erank = m->get_dest_inst().addr.v.erank;
	if (erank < rank.max_local && rank.local[erank]) {
	  // find entity
	  entity = rank.local[erank];
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
      fault(true);  // treat as a fault; i.e. reconnect|close
      continue;
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
    if (sd > 0) ::close(sd);
    rank.lock.Lock();
    {
      rank.pipe_reap_queue.push_back(this);
      rank.wait_cond.Signal();
    }
    rank.lock.Unlock();
  }
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

  while (state != STATE_CLOSED) {
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
      char c = CEPH_MSGR_TAG_CLOSE;
      lock.Unlock();
      if (sd) ::write(sd, &c, 1);
      lock.Lock();
      state = STATE_CLOSED;
      continue;
    }

    if (state != STATE_CONNECTING &&
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
	sent.push_back(m); // move to sent list
	lock.Unlock();
        dout(20) << "writer sending " << m->get_seq() << " " << m << " " << *m << dendl;
        if (m->empty_payload()) 
	  m->encode_payload();
	int rc = write_message(m);
	lock.Lock();
	
	if (rc < 0) {
          derr(1) << "writer error sending " << *m << " to " << m->get_dest() << ", "
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
    if (sd > 0) ::close(sd);
    rank.lock.Lock();
    {
      rank.pipe_reap_queue.push_back(this);
      rank.wait_cond.Signal();
    }
    rank.lock.Unlock();
  }
}


Message *Rank::Pipe::read_message()
{
  // envelope
  //dout(10) << "receiver.read_message from sd " << sd  << dendl;
  
  ceph_msg_header env; 
  if (tcp_read( sd, (char*)&env, sizeof(env) ) < 0)
    return 0;
  
  dout(20) << "reader got envelope type=" << env.type 
           << " src " << env.src << " dst " << env.dst
           << " front=" << env.front_len 
	   << " data=" << env.data_len << " at " << env.data_off
           << dendl;
  
  if (env.src.addr.ipaddr.sin_addr.s_addr == htonl(INADDR_ANY)) {
    dout(10) << "reader munging src addr " << env.src << " to be " << peer_addr << dendl;
    env.src.addr.ipaddr = peer_addr.v.ipaddr;
  }

  // read front
  bufferlist front;
  bufferptr bp;
  if (env.front_len) {
    bp = buffer::create(env.front_len);
    if (tcp_read( sd, bp.c_str(), env.front_len ) < 0) 
      return 0;
    front.push_back(bp);
    dout(20) << "reader got front " << front.length() << dendl;
  }

  // read data
  bufferlist data;
  if (env.data_len) {
    int left = env.data_len;
    if (env.data_off & ~PAGE_MASK) {
      // head
      int head = MIN(PAGE_SIZE - (env.data_off & ~PAGE_MASK),
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
  
  // unmarshall message
  Message *m = decode_message(env, front, data);

  dout(20) << "reader got " << front.length() << " + " << data.length() << " byte message from " 
           << m->get_source() << dendl;
  
  return m;
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
  __u32 s = seq;/*cpu_to_le32(seq);*/

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


int Rank::Pipe::write_message(Message *m)
{
  // get envelope, buffers
  ceph_msg_header *env = &m->get_env();
  env->front_len = m->get_payload().length();
  env->data_len = m->get_data().length();

  bufferlist blist;
  blist.claim( m->get_payload() );
  blist.append( m->get_data() );
  
  dout(20)  << "write_message " << m << " " << *m << " to " << m->get_dest() << dendl;
  
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
  msgvec[msg.msg_iovlen].iov_base = (char*)env;
  msgvec[msg.msg_iovlen].iov_len = sizeof(*env);
  msglen += sizeof(*env);
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
    
    if (msg.msg_iovlen >= IOV_MAX-1) {
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
  
  // send
  if (do_sendmsg(sd, &msg, msglen)) 
    return -1;	

  return 0;
}


