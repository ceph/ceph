// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "config.h"

#include "messages/MGenericMessage.h"

//#include "messages/MFailure.h"

#include <netdb.h>


#undef dout
#define dout(l)  if (l<=g_conf.debug_ms) cout << g_clock.now() << " -- " << rank.my_addr << " "
#define derr(l)  if (l<=g_conf.debug_ms) cerr << g_clock.now() << " -- " << rank.my_addr << " "



#include "tcp.cc"


Rank rank;



/********************************************
 * Accepter
 */

void simplemessenger_sigint(int r)
{
  rank.sigint();
}

void Rank::sigint()
{
  lock.Lock();
  derr(0) << "got control-c, exiting" << endl;
  ::close(accepter.listen_sd);
  _exit(-1);
  lock.Unlock();
}




int Rank::Accepter::start()
{
  // bind to a socket
  dout(10) << "accepter.start binding to listen " << endl;
  
  // use whatever user specified..
  g_my_addr.make_addr(rank.listen_addr);

  /* socket creation */
  listen_sd = socket(AF_INET,SOCK_STREAM,0);
  assert(listen_sd > 0);
  
  /* bind to port */
  int rc = bind(listen_sd, (struct sockaddr *) &rank.listen_addr, sizeof(rank.listen_addr));
  if (rc < 0) 
    derr(0) << "accepter.start unable to bind to " << rank.listen_addr << endl;
  assert(rc >= 0);

  // what port did we get?
  socklen_t llen = sizeof(rank.listen_addr);
  getsockname(listen_sd, (sockaddr*)&rank.listen_addr, &llen);
  
  dout(10) << "accepter.start bound to " << rank.listen_addr << endl;

  // listen!
  rc = ::listen(listen_sd, 1000);
  assert(rc >= 0);
  
  // my address is...  HELP HELP HELP!
  char host[100];
  bzero(host, 100);
  gethostname(host, 100);
  //dout(10) << "accepter.start my hostname is " << host << endl;

  struct hostent *myhostname = gethostbyname( host ); 

  // figure out my_addr
  if (g_my_addr.port > 0) {
    // user specified it, easy peasy.
    rank.my_addr = g_my_addr;
  } else {
    // look up my hostname.  blech!  this sucks.
    rank.listen_addr.sin_family = myhostname->h_addrtype;
    memcpy((char *) &rank.listen_addr.sin_addr.s_addr, 
	   myhostname->h_addr_list[0], 
	   myhostname->h_length);

    // set up my_addr with a nonce
    rank.my_addr.set_addr(rank.listen_addr);
    rank.my_addr.nonce = getpid(); // FIXME: pid might not be best choice here.
  }

  dout(10) << "accepter.start my addr is " << rank.my_addr << endl;

  // set up signal handler
  signal(SIGINT, simplemessenger_sigint);

  // start thread
  create();

  return 0;
}

void *Rank::Accepter::entry()
{
  dout(10) << "accepter starting" << endl;

  while (!done) {
    // accept
    struct sockaddr_in addr;
    socklen_t slen = sizeof(addr);
    int sd = ::accept(listen_sd, (sockaddr*)&addr, &slen);
    if (sd > 0) {
      dout(10) << "accepted incoming on sd " << sd << endl;
      
      rank.lock.Lock();
      if (!rank.local.empty()) {
	Pipe *p = new Pipe(sd);
	rank.pipes.insert(p);
      }
      rank.lock.Unlock();
    } else {
      dout(10) << "no incoming connection?" << endl;
      break;
    }
  }

  return 0;
}



/**************************************
 * Pipe
 */

int Rank::Pipe::accept()
{
  // my creater gave me sd via accept()
  
  // announce myself.
  int rc = tcp_write(sd, (char*)&rank.my_addr, sizeof(rank.my_addr));
  if (rc < 0) {
    ::close(sd);
    done = true;
    return -1;
  }
  
  // identify peer
  rc = tcp_read(sd, (char*)&peer_addr, sizeof(peer_addr));
  if (rc < 0) {
    dout(10) << "pipe(? " << this << ").accept couldn't read peer inst" << endl;
    ::close(sd);
    done = true;
    return -1;
  }
  
  // create writer thread.
  writer_running = true;
  writer_thread.create();
  
  // register pipe.
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_addr) == 0) {
      // install a pipe!
      dout(10) << "pipe(" << peer_addr << ' ' << this << ").accept peer is " << peer_addr << endl;
      rank.rank_pipe[peer_addr] = this;
    } else {
      // low ranks' Pipes "win"
      if (peer_addr < rank.my_addr) {
	dout(10) << "pipe(" << peer_addr << ' ' << this << ").accept peer is " << peer_addr 
		 << ", already had pipe, but switching to this new one" << endl;
	// switch to this new Pipe
	rank.rank_pipe[peer_addr]->close();  // close old one
	rank.rank_pipe[peer_addr] = this;
      } else {
	dout(10) << "pipe(" << peer_addr << ' ' << this << ").accept peer is " << peer_addr 
		 << ", already had pipe, sticking with it" << endl;
      }
    }
  }
  rank.lock.Unlock();

  return 0;   // success.
}

int Rank::Pipe::connect()
{
  dout(10) << "pipe(" << peer_addr << ' ' << this << ").connect" << endl;

  // create socket?
  sd = socket(AF_INET,SOCK_STREAM,0);
  assert(sd > 0);
  
  // bind any port
  struct sockaddr_in myAddr;
  myAddr.sin_family = AF_INET;
  myAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  myAddr.sin_port = htons( 0 );    
  
  int rc = bind(sd, (struct sockaddr *) &myAddr, sizeof(myAddr));
  assert(rc>=0);

  // connect!
  tcpaddr_t tcpaddr;
  peer_addr.make_addr(tcpaddr);
  rc = ::connect(sd, (sockaddr*)&tcpaddr, sizeof(myAddr));
  if (rc < 0) {
    dout(10) << "connect error " << peer_addr
	     << ", " << errno << ": " << strerror(errno) << endl;
    return rc;
  }

  // identify peer
  entity_addr_t paddr;
  rc = tcp_read(sd, (char*)&paddr, sizeof(paddr));
  if (!rc) { // bool
    dout(10) << "pipe(" << peer_addr << ' ' << this << ").connect couldn't read peer addr" << endl;
    return -1;
  }
  if (peer_addr != paddr) {
    derr(0) << "pipe(" << peer_addr << ' ' << this << ").connect peer is " << paddr << ", wtf" << endl;
    assert(0);
    return -1;
  }

  // identify myself
  rc = tcp_write(sd, (char*)&rank.my_addr, sizeof(rank.my_addr));
  if (rc < 0) 
    return -1;
  
  // register pipe
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_addr) == 0) {
      dout(10) << "pipe(" << peer_addr << ' ' << this << ").connect registering pipe" << endl;
      rank.rank_pipe[peer_addr] = this;
    } else {
      // this is normal.
      dout(10) << "pipe(" << peer_addr << ' ' << this << ").connect pipe already registered." << endl;
    }
  }
  rank.lock.Unlock();

  // start reader
  reader_running = true;
  reader_thread.create();  
  
  return 0;
}


void Rank::Pipe::close()
{
  dout(10) << "pipe(" << peer_addr << ' ' << this << ").close" << endl;

  // unreg ourselves
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_addr) &&
        rank.rank_pipe[peer_addr] == this) {
      dout(10) << "pipe(" << peer_addr << ' ' << this
	       << ").close unregistering pipe" << endl;
      rank.rank_pipe.erase(peer_addr);
    }
  }
  rank.lock.Unlock();

  // queue close message?
  if (!need_to_send_close) {
    dout(10) << "pipe(" << peer_addr << ' ' << this
	     << ").close already closing/closed" << endl;
    return;
  }
  
  if (!writer_running) {
    dout(10) << "pipe(" << peer_addr << ' ' << this
	     << ").close not queueing MSG_CLOSE, no writer running" << endl;  
  } else {
    dout(10) << "pipe(" << peer_addr << ' ' << this
	     << ").close queueing MSG_CLOSE" << endl;
    lock.Lock();
    q.push_back(new MGenericMessage(MSG_CLOSE));
    cond.Signal();
    need_to_send_close = false;
    lock.Unlock();  
  }
}


/* read msgs from socket.
 * also, server.
 *
 */
void Rank::Pipe::reader()
{
  if (server) 
    accept();

  // loop.
  while (!done) {
    Message *m = read_message();
    if (!m || m->get_type() == 0) {
      if (m) {
	delete m;
	dout(10) << "pipe(" << peer_addr << ' ' << this << ").reader read MSG_CLOSE message" << endl;
	need_to_send_close = false;
      } else {
	derr(10) << "pipe(" << peer_addr << ' ' << this << ").reader read null message" << endl;
      }

      close();

      done = true;
      cond.Signal();  // wake up writer too.
      break;
    }

    dout(10) << "pipe(" << peer_addr << ' ' << this << ").reader got message for " << m->get_dest() << endl;

    EntityMessenger *entity = 0;

    rank.lock.Lock();
    {
      if (g_conf.ms_single_dispatch) {
	// submit to single dispatch queue
	rank._submit_single_dispatch(m);
      } else {
	if (rank.local.count(m->get_dest())) {
	  // find entity
	  entity = rank.local[m->get_dest()];
	} else {
	  entity = rank.find_unnamed(m->get_dest());
	  if (!entity) {
	    if (rank.stopped.count(m->get_dest())) {
	      // ignore it
	    } else {
	      derr(0) << "pipe(" << peer_addr << ' ' << this << ").reader got message " << *m << " for " << m->get_dest() << ", which isn't local" << endl;
	      assert(0);  // FIXME do this differently
	    }
	  }
	}
      }
    }
    rank.lock.Unlock();
    
    if (entity) 
      entity->queue_message(m);        // queue
  }

  
  // reap?
  bool reap = false;
  lock.Lock();
  {
    reader_running = false;
    if (!writer_running) reap = true;
  }
  lock.Unlock();

  if (reap) {
    dout(20) << "pipe(" << peer_addr << ' ' << this << ").reader queueing for reap" << endl;
    ::close(sd);
    rank.lock.Lock();
    {
      rank.pipe_reap_queue.push_back(this);
      rank.wait_cond.Signal();
    }
    rank.lock.Unlock();
  }
}


/* write msgs to socket.
 * also, client.
 */
void Rank::Pipe::writer()
{
  if (!server) {
    int rc = connect();
    if (rc < 0) {
      derr(1) << "pipe(" << peer_addr << ' ' << this << ").writer error connecting, " 
	      << errno << ": " << strerror(errno)
	      << endl;
      done = true;
      list<Message*> out;
      fail(out);
    }
  }

  // disable Nagle algorithm
  if (g_conf.ms_tcp_nodelay) {
    int flag = 1;
    int r = ::setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
    if (r < 0) 
      dout(0) << "pipe(" << peer_addr << ' ' << this << ").writer couldn't set TCP_NODELAY: " << strerror(errno) << endl;
  }

  // loop.
  lock.Lock();
  while (!q.empty() || !done) {
    
    if (!q.empty()) {
      dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer grabbing message(s)" << endl;
      
      // grab outgoing list
      list<Message*> out;
      out.swap(q);
      
      // drop lock while i send these
      lock.Unlock();
      
      while (!out.empty()) {
        Message *m = out.front();
        out.pop_front();

        dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer sending " << *m << endl;

        // stamp.
        m->set_source_addr(rank.my_addr);
        
        // marshall
        if (m->empty_payload())
          m->encode_payload();
        
        if (write_message(m) < 0) {
          // failed!
          derr(1) << "pipe(" << peer_addr << ' ' << this << ").writer error sending " << *m << " to " << m->get_dest()
		  << ", " << errno << ": " << strerror(errno)
		  << endl;
          out.push_front(m);
          fail(out);
          done = true;
          break;
        }

        // did i just send a close?
        if (m->get_type() == MSG_CLOSE) 
          done = true;

        // clean up
        delete m;
      }

      lock.Lock();
      continue;
    }
    
    // wait
    dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer sleeping" << endl;
    cond.Wait(lock);
  }
  lock.Unlock(); 
  
  dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer finishing" << endl;

  // reap?
  bool reap = false;
  lock.Lock();
  {
    writer_running = false;
    if (!reader_running) reap = true;
  }
  lock.Unlock();
  
  if (reap) {
    dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer queueing for reap" << endl;
    ::close(sd);
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
  //dout(10) << "receiver.read_message from sd " << sd  << endl;
  
  msg_envelope_t env; 
  if (!tcp_read( sd, (char*)&env, sizeof(env) )) {
    need_to_send_close = false;
    return 0;
  }
  
  dout(20) << "pipe(" << peer_addr << ' ' << this << ").reader got envelope type=" << env.type 
           << " src " << env.src << " dst " << env.dst
           << " nchunks=" << env.nchunks
           << endl;
  
  // payload
  bufferlist blist;

  if (env.nchunks > 0) {
    // read chunk-wise.
    for (int i=0; i<env.nchunks; i++) {
      size_t size;
      if (!tcp_read( sd, (char*)&size, sizeof(size) )) {
	need_to_send_close = false;
	return 0;
      }
      
      if (size == 0) continue;
      
      bufferptr bp(size);
      
      if (!tcp_read( sd, bp.c_str(), size )) {
	need_to_send_close = false;
	return 0;
      }
      
      blist.push_back(bp);
      
      dout(20) << "pipe(" << peer_addr << ' ' << this << ").reader got frag " << i << " of " << env.nchunks 
	       << " len " << bp.length() << endl;
    }
  } else {
    // read in one go
    int size = -env.nchunks;
    bufferptr bp(size);
    if (!tcp_read( sd, bp.c_str(), size )) {
      need_to_send_close = false;
      return 0;
    }
    blist.push_back(bp);
    dout(20) << "pipe(" << peer_addr << ' ' << this << ").reader got full payload len " 
	     << bp.length() << endl;
  }
  
  // unmarshall message
  size_t s = blist.length();
  Message *m = decode_message(env, blist);
  
  dout(20) << "pipe(" << peer_addr << ' ' << this << ").reader got " << s << " byte message from " 
           << m->get_source() << endl;
  
  return m;
}



int Rank::Pipe::write_message(Message *m)
{
  // get envelope, buffers
  msg_envelope_t *env = &m->get_envelope();
  bufferlist blist;
  blist.claim( m->get_payload() );
  
  dout(20)  << "pipe(" << peer_addr << ' ' << this << ").writer sending " << m << " " << *m 
            << " to " << m->get_dest()
            << endl;
  
#ifdef TCP_KEEP_CHUNKS
  // send chunk-wise
  env->nchunks = blist.buffers().size();

  // set up msghdr and iovecs
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec msgvec[1+2*blist.buffers().size()];
  msg.msg_iov = msgvec;
    
  // envelope
  msgvec[0].iov_base = (char*)env;
  msgvec[0].iov_len = sizeof(*env);
  msg.msg_iovlen++;
    
  // buffers
  size_t chunklen[blist.buffers().size()];
  int i = 0;
  for (list<bufferptr>::const_iterator it = blist.buffers().begin();
       it != blist.buffers().end();
       it++) {
    if ((*it).length() == 0) continue;  // skip blank buffer.
    
    // chunk len
    chunklen[i] = (*it).length();
    msgvec[msg.msg_iovlen].iov_base = &chunklen[i];
    msgvec[msg.msg_iovlen].iov_len = sizeof(chunklen[i]);
    msg.msg_iovlen++;

    // chunk content
    msgvec[msg.msg_iovlen].iov_base = (char*)(*it).c_str();
    msgvec[msg.msg_iovlen].iov_len = (*it).length();
    msg.msg_iovlen++;

    i++;
  }
 
  // send 
  int r = sendmsg(sd, &msg, 0);
  if (r < 0) { 
    assert(r == -1);
    derr(1) << "pipe(" << peer_addr << ' ' << this << ").writer error on sendmsg for " << *m
	    << " to " << m->get_dest() 
	    << ", " << strerror(errno)
	    << endl; 
    need_to_send_close = false;
    return -1;
  }

#else
  if (1) {
    // one big chunk
    env->nchunks = -blist.length();
    
    // set up msghdr and iovecs
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    struct iovec msgvec[1+blist.buffers().size()];
    msg.msg_iov = msgvec;
    
    // envelope
    msgvec[0].iov_base = (char*)env;
    msgvec[0].iov_len = sizeof(*env);
    msg.msg_iovlen++;
    
    // buffers
    for (list<bufferptr>::const_iterator it = blist.buffers().begin();
	 it != blist.buffers().end();
	 it++) {
      if ((*it).length() == 0) continue;  // skip blank buffer.
      msgvec[msg.msg_iovlen].iov_base = (char*)(*it).c_str();
      msgvec[msg.msg_iovlen].iov_len = (*it).length();
      msg.msg_iovlen++;
    }
    
    // send 
    int r = sendmsg(sd, &msg, 0);
    if (r < 0) { 
      assert(r == -1);
      derr(1) << "pipe(" << peer_addr << ' ' << this << ").writer error on sendmsg for " << *m
	      << " to " << m->get_dest() 
	      << ", " << strerror(errno)
	      << endl; 
      need_to_send_close = false;
      return -1;
    }
  } else {
    // OLD way.
    int size = blist.length();
    env->nchunks = -blist.length();

    // send envelope
    int r = tcp_write( sd, (char*)env, sizeof(*env), true );  // MORE
    if (r < 0) { 
      derr(1) << "pipe(" << peer_addr << ' ' << this << ").writer error sending envelope for " << *m
	      << " to " << m->get_dest() << endl; 
      need_to_send_close = false;
      return -1;
    }
    
    dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer data len is " << size << " in " << blist.buffers().size() << " buffers" << endl;
    
    for (list<bufferptr>::const_iterator it = blist.buffers().begin();
	 it != blist.buffers().end();
	 it++) {
      if ((*it).length() == 0) continue;  // blank buffer.
      r = tcp_write( sd, (char*)(*it).c_str(), (*it).length() );
      if (r < 0) { 
	derr(10) << "pipe(" << peer_addr << ' ' << this << ").writer error sending data megachunk for " << *m << " to " << m->get_dest() << " : len " << (*it).length() << endl; 
	need_to_send_close = false;
	return -1;
      }
    }
  }
#endif
  
  return 0;
}


void Rank::Pipe::fail(list<Message*>& out)
{
  derr(10) << "pipe(" << peer_addr << ' ' << this << ").fail" << endl;

  // FIXME: possible race before i reclaim lock here?
  
  // deactivate myself
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_addr) &&
        rank.rank_pipe[peer_addr] == this)
      rank.rank_pipe.erase(peer_addr);
  }
  rank.lock.Unlock();

  // what do i do about reader()?   FIXME

  // sort my messages by (source) dispatcher, dest.
  map<Dispatcher*, map<entity_name_t, list<Message*> > > by_dis;
  lock.Lock();
  {
    // include out at front of queue
    q.splice(q.begin(), out);  

    // sort
    while (!q.empty()) {
      if (q.front()->get_type() == MSG_CLOSE) {
        delete q.front();
      } 
      else if (rank.local.count(q.front()->get_source())) {
	EntityMessenger *mgr = rank.local[q.front()->get_source()];
        Dispatcher *dis = mgr->get_dispatcher();
	if (mgr->is_stopped()) {
	  // ignore.
	  dout(1) << "pipe(" << peer_addr << ' ' << this << ").fail on " << *q.front() << ", dispatcher stopping, ignoring." << endl;
	  delete q.front();
	} else {
	  by_dis[dis][q.front()->get_dest()].push_back(q.front());
	}
      } 
      else {
        // oh well.  sending entity musta just shut down?
        assert(0);
        delete q.front();
      }
      q.pop_front();
    }
  }
  lock.Unlock();

  // report failure(s) to dispatcher(s)
  for (map<Dispatcher*, map<entity_name_t, list<Message*> > >::iterator i = by_dis.begin();
       i != by_dis.end();
       ++i) 
    for (map<entity_name_t, list<Message*> >::iterator j = i->second.begin();
         j != i->second.end();
         ++j) 
      for (list<Message*>::iterator k = j->second.begin();
           k != j->second.end();
           ++k) {
	derr(1) << "pipe(" << peer_addr << ' ' << this << ").fail on " << **k << " to " << (*k)->get_dest_inst() << endl;
        i->first->ms_handle_failure(*k, (*k)->get_dest_inst());
      }
}






/********************************************
 * Rank
 */

Rank::Rank() : 
  single_dispatcher(this) {
  // default to any listen_addr
  memset((char*)&listen_addr, 0, sizeof(listen_addr));
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  listen_addr.sin_port = 0;
}
Rank::~Rank()
{
}

/*
void Rank::set_listen_addr(tcpaddr_t& a)
{
  dout(10) << "set_listen_addr " << a << endl;
  memcpy((char*)&listen_addr.sin_addr.s_addr, (char*)&a.sin_addr.s_addr, 4);
  listen_addr.sin_port = a.sin_port;
}
*/

void Rank::_submit_single_dispatch(Message *m)
{
  assert(lock.is_locked());

  if (local.count(m->get_dest()) &&
      local[m->get_dest()]->is_ready()) {
    rank.single_dispatch_queue.push_back(m);
    rank.single_dispatch_cond.Signal();
  } else {
    waiting_for_ready[m->get_dest()].push_back(m);
  }
}


void Rank::single_dispatcher_entry()
{
  lock.Lock();
  while (!single_dispatch_stop || !single_dispatch_queue.empty()) {
    if (!single_dispatch_queue.empty()) {
      list<Message*> ls;
      ls.swap(single_dispatch_queue);

      lock.Unlock();
      {
        while (!ls.empty()) {
          Message *m = ls.front();
          ls.pop_front();
          
          dout(1) << m->get_dest() 
		  << " <-- " << m->get_source_inst()
		  << " ---- " << *m
                  << " -- " << m 
                  << endl;
          
	  assert(local.count(m->get_dest()));
	  local[m->get_dest()]->dispatch(m);
	}
      }
      lock.Lock();
      continue;
    }
    single_dispatch_cond.Wait(lock);
  }
  lock.Unlock();
}


/*
 * note: assumes lock is held
 */
void Rank::reaper()
{
  dout(10) << "reaper" << endl;
  assert(lock.is_locked());

  while (!pipe_reap_queue.empty()) {
    Pipe *p = pipe_reap_queue.front();
    dout(10) << "reaper reaping pipe " << p->get_peer_addr() << endl;
    pipe_reap_queue.pop_front();
    assert(pipes.count(p));
    pipes.erase(p);
    p->join();
    dout(10) << "reaper reaped pipe " << p->get_peer_addr() << endl;
    delete p;
  }
}


int Rank::start_rank()
{
  dout(10) << "start_rank" << endl;

  // bind to a socket
  if (accepter.start() < 0) 
    return -1;

  // start single thread dispatcher?
  if (g_conf.ms_single_dispatch) {
    single_dispatch_stop = false;
    single_dispatcher.create();
  }

  lock.Lock();

  dout(1) << "start_rank at " << listen_addr << endl;

  lock.Unlock();
  return 0;
}



/* connect_rank
 * NOTE: assumes rank.lock held.
 */
Rank::Pipe *Rank::connect_rank(const entity_addr_t& addr)
{
  assert(rank.lock.is_locked());
  assert(addr != rank.my_addr);
  
  dout(10) << "connect_rank to " << addr << endl;
  
  // create pipe
  Pipe *pipe = new Pipe(addr);
  rank.rank_pipe[addr] = pipe;
  pipes.insert(pipe);

  return pipe;
}






Rank::EntityMessenger *Rank::find_unnamed(entity_name_t a)
{
  // find an unnamed local entity of the right type
  for (map<entity_name_t, EntityMessenger*>::iterator p = local.begin();
       p != local.end();
       ++p) {
    if (p->first.type() == a.type() && p->first.is_new()) 
      return p->second;
  }
  return 0;
}




/* register_entity 
 */
Rank::EntityMessenger *Rank::register_entity(entity_name_t name)
{
  dout(10) << "register_entity " << name << endl;
  lock.Lock();
  
  // create messenger
  EntityMessenger *msgr = new EntityMessenger(name);

  // add to directory
  assert(local.count(name) == 0);
  local[name] = msgr;
  
  lock.Unlock();
  return msgr;
}


void Rank::unregister_entity(EntityMessenger *msgr)
{
  lock.Lock();
  dout(10) << "unregister_entity " << msgr->get_myname() << endl;
  
  // remove from local directory.
  entity_name_t name = msgr->get_myname();
  assert(local.count(name));
  local.erase(name);
  
  stopped.insert(name);
  wait_cond.Signal();

  lock.Unlock();
}


void Rank::submit_message(Message *m, const entity_addr_t& dest_addr)
{
  const entity_name_t dest = m->get_dest();

  // lookup
  EntityMessenger *entity = 0;
  Pipe *pipe = 0;

  lock.Lock();
  {
    // local?
    if (dest_addr == my_addr) {
      if (local.count(dest)) {
        // local
        dout(20) << "submit_message " << *m << " dest " << dest << " local" << endl;
        if (g_conf.ms_single_dispatch) {
          _submit_single_dispatch(m);
        } else {
          entity = local[dest];
        }
      } else {
        derr(0) << "submit_message " << *m << " dest " << dest << " " << dest_addr << " local but not in local map?" << endl;
        //assert(0);  // hmpf, this is probably mds->mon beacon from newsyn.
      }
    }
    else {
      // remote.
      if (rank_pipe.count( dest_addr )) {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_addr << ", already connected." << endl;
        // connected.
        pipe = rank_pipe[ dest_addr ];
      } else {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_addr << ", connecting." << endl;
        // not connected.
        pipe = connect_rank( dest_addr );
      }
    }
  }
  lock.Unlock();
  
  // do it
  if (entity) {  
    // local!
    dout(20) << "submit_message " << *m << " dest " << dest << " local, queueing" << endl;
    entity->queue_message(m);
  } 
  else if (pipe) {
    // remote!
    dout(20) << "submit_message " << *m << " dest " << dest << " remote, sending" << endl;
    pipe->send(m);
  } 
}





void Rank::wait()
{
  lock.Lock();
  while (1) {
    // reap dead pipes
    reaper();

    if (local.empty()) {
      dout(10) << "wait: everything stopped" << endl;
      break;   // everything stopped.
    } else {
      dout(10) << "wait: local still has " << local.size() << " items, waiting" << endl;
    }
    
    wait_cond.Wait(lock);
  }
  lock.Unlock();
  
  // done!  clean up.

  //dout(10) << "wait: stopping accepter thread" << endl;
  //accepter.stop();

  // stop dispatch thread
  if (g_conf.ms_single_dispatch) {
    dout(10) << "wait: stopping dispatch thread" << endl;
    lock.Lock();
    single_dispatch_stop = true;
    single_dispatch_cond.Signal();
    lock.Unlock();
    single_dispatcher.join();
  }
  
  // reap pipes
  lock.Lock();
  {
    dout(10) << "wait: closing pipes" << endl;
    list<Pipe*> toclose;
    for (hash_map<entity_addr_t,Pipe*>::iterator i = rank_pipe.begin();
         i != rank_pipe.end();
         i++)
      toclose.push_back(i->second);
    for (list<Pipe*>::iterator i = toclose.begin();
	 i != toclose.end();
	 i++)
      (*i)->close();

    dout(10) << "wait: waiting for pipes " << pipes << " to close" << endl;
    while (!pipes.empty()) {
      wait_cond.Wait(lock);
      reaper();
    }
  }
  lock.Unlock();

  dout(10) << "wait: done." << endl;
}






/**********************************
 * EntityMessenger
 */

Rank::EntityMessenger::EntityMessenger(entity_name_t myaddr) :
  Messenger(myaddr),
  stop(false),
  dispatch_thread(this)
{
}
Rank::EntityMessenger::~EntityMessenger()
{
}

void Rank::EntityMessenger::dispatch_entry()
{
  lock.Lock();
  while (!stop) {
    if (!dispatch_queue.empty()) {
      list<Message*> ls;
      ls.swap(dispatch_queue);

      lock.Unlock();
      {
        // deliver
        while (!ls.empty()) {
	  if (stop) {
	    dout(1) << "dispatch: stop=true, discarding " << ls.size() 
		    << " messages in dispatch queue" << endl;
	    break;
	  }
          Message *m = ls.front();
          ls.pop_front();
          dout(1) << m->get_dest() 
		  << " <-- " << m->get_source_inst()
		  << " ---- " << *m
                  << " -- " << m 
                  << endl;
          dispatch(m);
        }
      }
      lock.Lock();
      continue;
    }
    cond.Wait(lock);
  }
  lock.Unlock();

  // deregister
  rank.unregister_entity(this);
}

void Rank::EntityMessenger::ready()
{
  dout(10) << "ready " << get_myaddr() << endl;

  if (g_conf.ms_single_dispatch) {
    rank.lock.Lock();
    if (rank.waiting_for_ready.count(get_myname())) {
      rank.single_dispatch_queue.splice(rank.single_dispatch_queue.end(),
                                        rank.waiting_for_ready[get_myname()]);
      rank.waiting_for_ready.erase(get_myname());
      rank.single_dispatch_cond.Signal();
    }
    rank.lock.Unlock();
  } else {
    // start my dispatch thread
    dispatch_thread.create();
  }
}


int Rank::EntityMessenger::shutdown()
{
  dout(10) << "shutdown " << get_myaddr() << endl;
  
  // stop my dispatch thread
  if (dispatch_thread.am_self()) {
    dout(1) << "shutdown i am dispatch, setting stop flag" << endl;
    stop = true;
  } else {
    dout(1) << "shutdown i am not dispatch, setting stop flag and joining thread." << endl;
    lock.Lock();
    stop = true;
    cond.Signal();
    lock.Unlock();
    dispatch_thread.join();
  }

  return 0;
}


void Rank::EntityMessenger::prepare_dest(const entity_addr_t& addr)
{
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(addr) == 0)
      rank.connect_rank(addr);
  }
  rank.lock.Unlock();
}

int Rank::EntityMessenger::send_message(Message *m, entity_inst_t dest,
					int port, int fromport)
{
  // set envelope
  m->set_source(get_myname(), fromport);
  m->set_source_addr(rank.my_addr);
  m->set_dest_inst(dest);
  m->set_dest_port(port);
 
  dout(1) << m->get_source()
          << " --> " << dest.name << " " << dest.addr
          << " -- " << *m
	  << " -- " << m
          << endl;

  rank.submit_message(m, dest.addr);

  return 0;
}



const entity_addr_t &Rank::EntityMessenger::get_myaddr()
{
  return rank.my_addr;
}


void Rank::EntityMessenger::reset_myname(entity_name_t newname)
{
  entity_name_t oldname = get_myname();
  dout(10) << "reset_myname " << oldname << " to " << newname << endl;

  rank.local.erase(oldname);
  rank.local[newname] = this;

  _set_myname(newname);
}




void Rank::EntityMessenger::mark_down(entity_addr_t a)
{
  rank.mark_down(a);
}

void Rank::mark_down(entity_addr_t addr)
{
  //if (my_rank == 0) return;   // ugh.. rank0 already handles this stuff in the namer
  lock.Lock();
  /*
  if (entity_map.count(a) &&
      entity_map[a] > inst) {
    dout(10) << "mark_down " << a << " inst " << inst << " < " << entity_map[a] << endl;
    derr(10) << "mark_down " << a << " inst " << inst << " < " << entity_map[a] << endl;
    // do nothing!
  } else {
    if (entity_map.count(a) == 0) {
      // don't know it
      dout(10) << "mark_down " << a << " inst " << inst << " ... unknown by me" << endl;
      derr(10) << "mark_down " << a << " inst " << inst << " ... unknown by me" << endl;
    } else {
      // know it
      assert(entity_map[a] <= inst);
      dout(10) << "mark_down " << a << " inst " << inst << endl;
      derr(10) << "mark_down " << a << " inst " << inst << endl;
      
      entity_map.erase(a);
      
      if (rank_pipe.count(inst)) {
	rank_pipe[inst]->close();
	rank_pipe.erase(inst);
      }
    }
  }
  */
  lock.Unlock();
}


