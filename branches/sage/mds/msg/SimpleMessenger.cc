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

#include "config.h"

#include "messages/MGenericMessage.h"

#include <netdb.h>

#include <iostream>
#include <fstream>

#define dout(l)  if (l<=g_conf.debug_ms) *_dout << dbeginl << g_clock.now() << " " << pthread_self() << " -- " << rank.my_addr << " "
#define derr(l)  if (l<=g_conf.debug_ms) *_derr << dbeginl << g_clock.now() << " " << pthread_self() << " -- " << rank.my_addr << " "



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

int Rank::Accepter::start()
{
  // bind to a socket
  dout(10) << "accepter.start" << dendl;
  
  char hostname[100];
  memset(hostname, 0, 100);
  gethostname(hostname, 100);
  dout(2) << "accepter.start my hostname is " << hostname << dendl;

  // is there a .ceph_hosts file?
  {
    ifstream fh;
    fh.open(".ceph_hosts");
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
	dout(15) << ".ceph_hosts: host '" << host << "' -> '" << addr << "'" << dendl;
	if (host == hostname) {
	  parse_ip_port(addr.c_str(), g_my_addr);
	  dout(1) << ".ceph_hosts: my addr is " << g_my_addr << dendl;
	  break;
	}
      }
      fh.close();
    }
  }

  // use whatever user specified (if anything)
  tcpaddr_t listen_addr;
  g_my_addr.make_addr(listen_addr);

  /* socket creation */
  listen_sd = socket(AF_INET,SOCK_STREAM,0);
  assert(listen_sd > 0);
  
  /* bind to port */
  int rc = bind(listen_sd, (struct sockaddr *) &listen_addr, sizeof(listen_addr));
  if (rc < 0) 
    derr(0) << "accepter.start unable to bind to " << listen_addr << dendl;
  assert(rc >= 0);

  // what port did we get?
  socklen_t llen = sizeof(listen_addr);
  getsockname(listen_sd, (sockaddr*)&listen_addr, &llen);
  
  dout(10) << "accepter.start bound to " << listen_addr << dendl;

  // listen!
  rc = ::listen(listen_sd, 1000);
  assert(rc >= 0);
  
  // figure out my_addr
  if (g_my_addr != entity_addr_t()) {
    // user specified it, easy peasy.
    rank.my_addr = g_my_addr;
  } else {
    // my IP is... HELP!
    struct hostent *myhostname = gethostbyname(hostname); 
    
    // look up my hostname.
    listen_addr.sin_family = myhostname->h_addrtype;
    memcpy((char*)&listen_addr.sin_addr.s_addr, 
	   myhostname->h_addr_list[0], 
	   myhostname->h_length);
    rank.my_addr.set_addr(listen_addr);
    rank.my_addr.v.port = 0;  // see below
  }
  if (rank.my_addr.v.port == 0) {
    entity_addr_t tmp;
    tmp.set_addr(listen_addr);
    rank.my_addr.v.port = tmp.v.port;
    rank.my_addr.v.nonce = getpid(); // FIXME: pid might not be best choice here.
  }

  dout(1) << "accepter.start my_addr is " << rank.my_addr << dendl;

  // set up signal handler
  //old_sigint_handler = signal(SIGINT, simplemessenger_sigint);

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
      
      rank.lock.Lock();
      if (!rank.local.empty()) {
	Pipe *p = new Pipe(sd);
	rank.pipes.insert(p);
      }
      rank.lock.Unlock();
    } else {
      dout(10) << "no incoming connection?" << dendl;
    }
  }

  dout(20) << "accepter closing" << dendl;
  ::close(listen_sd);
  dout(10) << "accepter stopping" << dendl;
  return 0;
}

void Rank::Accepter::stop()
{
  done = true;
  this->kill(SIGUSR1);
  join();
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
    dout(10) << "pipe(? " << this << ").accept couldn't read peer inst" << dendl;
    ::close(sd);
    done = true;
    return -1;
  }
    
  // register pipe.
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_addr) == 0) {
      // install as outgoing pipe!
      dout(10) << "pipe(" << peer_addr << ' ' << this << ").accept peer is " << peer_addr << dendl;
      rank.rank_pipe[peer_addr] = this;

      // create writer thread.
      writer_running = true;
      writer_thread.create();
    } else {
      // hrm, this may affect message delivery order.. keep both pipes!
      dout(10) << "pipe(" << peer_addr << ' ' << this << ").accept already have a pipe for this peer (" << rank.rank_pipe[peer_addr] << "), will receive on this pipe only" << dendl;

      // FIXME i could stop the receiver on the other pipe..
      
      /*
      // low ranks' Pipes "win"
      if (peer_addr < rank.my_addr) {
	dout(10) << "pipe(" << peer_addr << ' ' << this << ").accept peer is " << peer_addr 
		 << ", already had pipe, but switching to this new one" << dendl;
	// switch to this new Pipe
	rank.rank_pipe[peer_addr]->unregister();  // close old one
	rank.rank_pipe[peer_addr]->close();  // close old one
	rank.rank_pipe[peer_addr] = this;
      } else {
	dout(10) << "pipe(" << peer_addr << ' ' << this << ").accept peer is " << peer_addr 
		 << ", already had pipe, sticking with it" << dendl;
      }
      */      
    }
  }
  rank.lock.Unlock();

  return 0;   // success.
}

int Rank::Pipe::connect()
{
  dout(10) << "pipe(" << peer_addr << ' ' << this << ").connect" << dendl;

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
	     << ", " << errno << ": " << strerror(errno) << dendl;
    return rc;
  }

  // identify peer ..... FIXME
  entity_addr_t paddr;
  rc = tcp_read(sd, (char*)&paddr, sizeof(paddr));
  if (!rc) { // bool
    dout(10) << "pipe(" << peer_addr << ' ' << this << ").connect couldn't read peer addr" << dendl;
    return -1;
  }
  if (peer_addr != paddr) {
    dout(10) << "pipe(" << peer_addr << ' ' << this << ").connect peer identifies itself as " << paddr << ", wrong guy!" << dendl;
    ::close(sd);
    sd = 0;
    return -1;
  }

  // identify myself
  rc = tcp_write(sd, (char*)&rank.my_addr, sizeof(rank.my_addr));
  if (rc < 0) 
    return -1;
  
  // register pipe
  /*
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_addr) == 0) {
      dout(10) << "pipe(" << peer_addr << ' ' << this << ").connect registering pipe" << dendl;
      rank.rank_pipe[peer_addr] = this;
    } else {
      // this is normal.
      dout(10) << "pipe(" << peer_addr << ' ' << this << ").connect pipe already registered." << dendl;
    }
  }
  rank.lock.Unlock();
  */

  // start reader
  reader_running = true;
  reader_thread.create();  
  
  return 0;
}


void Rank::Pipe::unregister()
{
  assert(rank.lock.is_locked());
  if (rank.rank_pipe.count(peer_addr) &&
      rank.rank_pipe[peer_addr] == this) {
    dout(10) << "pipe(" << peer_addr << ' ' << this
	     << ").unregister" << dendl;
    rank.rank_pipe.erase(peer_addr);
  } else {
    dout(10) << "pipe(" << peer_addr << ' ' << this
	     << ").unregister - not registerd" << dendl;
  }
}

void Rank::Pipe::close()
{
  dout(10) << "pipe(" << peer_addr << ' ' << this << ").close" << dendl;

  // queue close message?
  if (!need_to_send_close) {
    dout(10) << "pipe(" << peer_addr << ' ' << this
	     << ").close already closing/closed" << dendl;
    return;
  }
  
  if (!writer_running) {
    dout(10) << "pipe(" << peer_addr << ' ' << this
	     << ").close not queueing MSG_CLOSE, no writer running" << dendl;  
  } else {
    dout(10) << "pipe(" << peer_addr << ' ' << this
	     << ").close queueing MSG_CLOSE" << dendl;
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
	dout(10) << "pipe(" << peer_addr << ' ' << this << ").reader read MSG_CLOSE message" << dendl;
	need_to_send_close = false;
      } else {
	derr(10) << "pipe(" << peer_addr << ' ' << this << ").reader read null message" << dendl;
      }

      rank.lock.Lock();
      unregister();
      rank.lock.Unlock();
      close();

      done = true;
      cond.Signal();  // wake up writer too.
      break;
    }

    dout(10) << "pipe(" << peer_addr << ' ' << this << ").reader got message " 
	     << m << " " << *m
	     << " for " << m->get_dest() << dendl;

    // deliver
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
	  if (entity) {
	    dout(3) << "pipe(" << peer_addr << ' ' << this << ").reader blessing " << m->get_dest() << dendl;
	    //entity->reset_myname(m->get_dest());
	    rank.local.erase(entity->get_myname());
	    rank.local[m->get_dest()] = entity;
	    entity->_set_myname(m->get_dest());

	  } else {
	    if (rank.stopped.count(m->get_dest())) {
	      // ignore it
	    } else {
	      derr(0) << "pipe(" << peer_addr << ' ' << this << ").reader got message " << *m << " for " << m->get_dest() << ", which isn't local" << dendl;
	      //assert(0);  // FIXME do this differently
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
    dout(20) << "pipe(" << peer_addr << ' ' << this << ").reader queueing for reap" << dendl;
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
	      << dendl;
      done = true;
      list<Message*> out;
      fail(out);
    }
  }

  // disable Nagle algorithm?
  if (g_conf.ms_tcp_nodelay) {
    int flag = 1;
    int r = ::setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
    if (r < 0) 
      dout(0) << "pipe(" << peer_addr << ' ' << this << ").writer couldn't set TCP_NODELAY: " << strerror(errno) << dendl;
  }

  // loop.
  lock.Lock();
  while (!q.empty() || !done) {
    
    if (!q.empty()) {
      dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer grabbing message(s)" << dendl;
      
      // grab outgoing list
      list<Message*> out;
      out.swap(q);
      
      // drop lock while i send these
      lock.Unlock();
      
      while (!out.empty()) {
        Message *m = out.front();
        out.pop_front();

        dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer sending " << m << " " << *m << dendl;

        // stamp.
        m->set_source_addr(rank.my_addr);
        
        // marshall
        if (m->empty_payload())
          m->encode_payload();
        
        if (write_message(m) < 0) {
          // failed!
          derr(1) << "pipe(" << peer_addr << ' ' << this << ").writer error sending " << *m << " to " << m->get_dest()
		  << ", " << errno << ": " << strerror(errno)
		  << dendl;
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
    dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer sleeping" << dendl;
    cond.Wait(lock);
  }
  lock.Unlock(); 
  
  dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer finishing" << dendl;

  // reap?
  bool reap = false;
  lock.Lock();
  {
    writer_running = false;
    if (!reader_running) reap = true;
  }
  lock.Unlock();
  
  if (reap) {
    dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer queueing for reap" << dendl;
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
  //dout(10) << "receiver.read_message from sd " << sd  << dendl;
  
  ceph_message_header env; 
  if (!tcp_read( sd, (char*)&env, sizeof(env) )) {
    need_to_send_close = false;
    return 0;
  }
  
  dout(20) << "pipe(" << peer_addr << ' ' << this << ").reader got envelope type=" << env.type 
           << " src " << env.src << " dst " << env.dst
           << " nchunks=" << env.nchunks
           << dendl;
  
  // payload
  bufferlist blist;
  int32_t pos = 0;
  list<int> chunk_at;
  for (unsigned i=0; i<env.nchunks; i++) {
    int32_t size;
    if (!tcp_read( sd, (char*)&size, sizeof(size) )) {
      need_to_send_close = false;
      return 0;
    }

    dout(30) << "decode chunk " << i << "/" << env.nchunks << " size " << size << dendl;

    if (pos) chunk_at.push_back(pos);
    pos += size;

    bufferptr bp;
    if (size % 4096 == 0) {
      dout(30) << "decoding page-aligned chunk of " << size << dendl;
      bp = buffer::create_page_aligned(size);
    } else {
      bp = buffer::create(size);
    }
    
    if (!tcp_read( sd, bp.c_str(), size )) {
      need_to_send_close = false;
      return 0;
    }
    
    blist.push_back(bp);
    
    dout(30) << "pipe(" << peer_addr << ' ' << this << ").reader got frag " << i << " of " << env.nchunks 
             << " len " << bp.length() << dendl;
  }
  
  // unmarshall message
  size_t s = blist.length();
  Message *m = decode_message(env, blist);

  m->set_chunk_payload_at(chunk_at);
  
  dout(20) << "pipe(" << peer_addr << ' ' << this << ").reader got " << s << " byte message from " 
           << m->get_source() << dendl;
  
  return m;
}


int Rank::Pipe::do_sendmsg(Message *m, struct msghdr *msg, int len)
{
  while (len > 0) {
    if (0) { // sanity
      int l = 0;
      for (unsigned i=0; i<msg->msg_iovlen; i++)
	l += msg->msg_iov[i].iov_len;
      assert(l == len);
    }

    int r = ::sendmsg(sd, msg, 0);
    if (r < 0) { 
      assert(r == -1);
      derr(1) << "pipe(" << peer_addr << ' ' << this << ").writer error on sendmsg for " << *m
	      << " to " << m->get_dest() 
	      << ", " << strerror(errno)
	      << dendl; 
      need_to_send_close = false;
      return -1;
    }
    len -= r;
    if (len == 0) break;
    
    // hrmph.  trim r bytes off the front of our message.
    dout(20) << "pipe(" << peer_addr << ' ' << this << ").writer partial sendmsg for " << *m
	    << " to " << m->get_dest()
	    << " did " << r << ", still have " << len
	    << dendl;
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


int Rank::Pipe::write_message(Message *m)
{
  // get envelope, buffers
  ceph_message_header *env = &m->get_envelope();
  bufferlist blist;
  blist.claim( m->get_payload() );
  
  // chunk out page aligned buffers? 
  if (blist.length() == 0) 
    env->nchunks = 0; 
  else {
    env->nchunks = 1 + m->get_chunk_payload_at().size();  // header + explicit chunk points
    if (!m->get_chunk_payload_at().empty())
      dout(20) << "chunking at " << m->get_chunk_payload_at()
	      << " in " << *m << " len " << blist.length()
	      << dendl;
  }

  dout(20)  << "pipe(" << peer_addr << ' ' << this << ").write_message " << m << " " << *m 
            << " to " << m->get_dest()
	    << " in " << env->nchunks
            << dendl;
  
  // set up msghdr and iovecs
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec msgvec[1 + blist.buffers().size() + env->nchunks*2];  // conservative upper bound
  msg.msg_iov = msgvec;
  int msglen = 0;
  
  // send envelope
  msgvec[0].iov_base = (char*)env;
  msgvec[0].iov_len = sizeof(*env);
  msglen += sizeof(*env);
  msg.msg_iovlen++;
  
  // payload
  list<bufferptr>::const_iterator pb = blist.buffers().begin();
  list<int>::const_iterator pc = m->get_chunk_payload_at().begin();
  int b_off = 0;  // carry-over buffer offset, if any
  int bl_pos = 0; // blist pos
  int nchunks = env->nchunks;
  int32_t chunksizes[nchunks];

  for (int curchunk=0; curchunk < nchunks; curchunk++) {
    // start a chunk
    int32_t size = blist.length() - bl_pos;
    if (pc != m->get_chunk_payload_at().end()) {
      assert(*pc > bl_pos);
      size = *pc - bl_pos;
      dout(30) << "pos " << bl_pos << " explicit chunk at " << *pc << " size " << size << " of " << blist.length() << dendl;
      pc++;
    }
    assert(size > 0);
    dout(30) << "chunk " << curchunk << " pos " << bl_pos << " size " << size << dendl;

    // chunk size
    chunksizes[curchunk] = size;
    msgvec[msg.msg_iovlen].iov_base = &chunksizes[curchunk];
    msgvec[msg.msg_iovlen].iov_len = sizeof(int32_t);
    msglen += sizeof(int32_t);
    msg.msg_iovlen++;

    // chunk contents
    int left = size;
    while (left > 0) {
      int donow = MIN(left, (int)pb->length()-b_off);
      assert(donow > 0);
      dout(30) << " bl_pos " << bl_pos << " b_off " << b_off
	      << " leftinchunk " << left
	      << " buffer len " << pb->length()
	      << " writing " << donow 
	      << dendl;

      if (msg.msg_iovlen >= IOV_MAX-1) {
	if (do_sendmsg(m, &msg, msglen)) 
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
      if (b_off != (int)pb->length()) 
	break;
      pb++;
      b_off = 0;
    }
    assert(left == 0);
  }
  assert(pb == blist.buffers().end());
  
  // send
  if (do_sendmsg(m, &msg, msglen)) 
    return -1;	

  return 0;
}


void Rank::Pipe::fail(list<Message*>& out)
{
  derr(10) << "pipe(" << peer_addr << ' ' << this << ").fail" << dendl;

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
	  dout(1) << "pipe(" << peer_addr << ' ' << this << ").fail on " << *q.front() << ", dispatcher stopping, ignoring." << dendl;
	  delete q.front();
	} else {
	  by_dis[dis][q.front()->get_dest()].push_back(q.front());
	}
      } 
      else {
        // oh well.  sending entity musta just shut down?
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
	derr(1) << "pipe(" << peer_addr << ' ' << this << ").fail on " << **k << " to " << (*k)->get_dest_inst() << dendl;
	if (i->first)
	  i->first->ms_handle_failure(*k, (*k)->get_dest_inst());
      }
}






/********************************************
 * Rank
 */

Rank::Rank() : 
  single_dispatcher(this),
  started(false) {
}
Rank::~Rank()
{
}

/*
void Rank::set_listen_addr(tcpaddr_t& a)
{
  dout(10) << "set_listen_addr " << a << dendl;
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
                  << dendl;
          
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
  dout(10) << "reaper" << dendl;
  assert(lock.is_locked());

  while (!pipe_reap_queue.empty()) {
    Pipe *p = pipe_reap_queue.front();
    dout(10) << "reaper reaping pipe " << p->get_peer_addr() << dendl;
    pipe_reap_queue.pop_front();
    assert(pipes.count(p));
    pipes.erase(p);
    p->join();
    dout(10) << "reaper reaped pipe " << p->get_peer_addr() << dendl;
    delete p;
  }
}


int Rank::start_rank()
{
  lock.Lock();
  if (started) {
    dout(10) << "start_rank already started" << dendl;
    lock.Unlock();
    return 0;
  }
  dout(10) << "start_rank" << dendl;
  lock.Unlock();

  // bind to a socket
  if (accepter.start() < 0) 
    return -1;

  // start single thread dispatcher?
  if (g_conf.ms_single_dispatch) {
    single_dispatch_stop = false;
    single_dispatcher.create();
  }

  lock.Lock();

  dout(1) << "start_rank at " << my_addr << dendl;
  started = true;
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
  
  dout(10) << "connect_rank to " << addr << ", creating pipe and registering" << dendl;
  
  // create pipe
  Pipe *pipe = new Pipe(addr);
  rank.rank_pipe[addr] = pipe;
  pipes.insert(pipe);

  // register
  rank.rank_pipe[addr] = pipe;

  return pipe;
}






Rank::EntityMessenger *Rank::find_unnamed(entity_name_t a)
{
  // find an unnamed (and _ready_) local entity of the right type
  for (map<entity_name_t, EntityMessenger*>::iterator p = local.begin();
       p != local.end();
       ++p) {
    if (p->first.type() == a.type() && p->first.is_new() &&
	p->second->is_ready()) 
      return p->second;
  }
  return 0;
}




/* register_entity 
 */
Rank::EntityMessenger *Rank::register_entity(entity_name_t name)
{
  dout(10) << "register_entity " << name << dendl;
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
  dout(10) << "unregister_entity " << msgr->get_myname() << dendl;
  
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
        dout(20) << "submit_message " << *m << " dest " << dest << " local" << dendl;
        if (g_conf.ms_single_dispatch) {
          _submit_single_dispatch(m);
        } else {
          entity = local[dest];
        }
      } else {
        derr(0) << "submit_message " << *m << " dest " << dest << " " << dest_addr << " local but not in local map?" << dendl;
        //assert(0);  // hmpf, this is probably mds->mon beacon from newsyn.
      }
    }
    else {
      // remote.
      if (rank_pipe.count( dest_addr )) {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_addr << ", already connected." << dendl;
        // connected.
        pipe = rank_pipe[ dest_addr ];
      } else {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_addr << ", connecting." << dendl;
        // not connected.
        pipe = connect_rank( dest_addr );
      }
    }
  }
  
  // do it
  if (entity) {  
    // local!
    dout(20) << "submit_message " << *m << " dest " << dest << " local, queueing" << dendl;
    entity->queue_message(m);
  } 
  else if (pipe) {
    // remote!
    dout(20) << "submit_message " << *m << " dest " << dest << " remote, sending" << dendl;
    pipe->send(m);
  } 

  lock.Unlock();
}





void Rank::wait()
{
  lock.Lock();
  while (1) {
    // reap dead pipes
    reaper();

    if (local.empty()) {
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

  // stop dispatch thread
  if (g_conf.ms_single_dispatch) {
    dout(10) << "wait: stopping dispatch thread" << dendl;
    lock.Lock();
    single_dispatch_stop = true;
    single_dispatch_cond.Signal();
    lock.Unlock();
    single_dispatcher.join();
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
      (*i)->unregister();
      (*i)->close();
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
          dout(1) << m->get_dest() 
		  << " <== " << m->get_source_inst()
		  << " ==== " << *m
                  << " ==== " << m 
                  << dendl;
          dispatch(m);
	  dout(20) << "done calling dispatch on " << m << dendl;
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
  dout(10) << "ready " << get_myaddr() << dendl;
  assert(!dispatch_thread.is_started());
  
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
          << dendl;

  rank.submit_message(m, dest.addr);

  return 0;
}

int Rank::EntityMessenger::send_first_message(Dispatcher *d,
					      Message *m, entity_inst_t dest,
					      int port, int fromport)
{
  /* hacky thing for csyn and newsyn:
   * set dispatcher (go active) AND set sender for this 
   * message while holding rank.lock.  this prevents any
   * races against incoming unnamed messages naming us before
   * we fire off our first message.  
   */
  rank.lock.Lock();
  set_dispatcher(d);

  // set envelope
  m->set_source(get_myname(), fromport);
  m->set_source_addr(rank.my_addr);
  m->set_dest_inst(dest);
  m->set_dest_port(port);
  rank.lock.Unlock();
 
  dout(1) << m->get_source()
          << " --> " << dest.name << " " << dest.addr
          << " -- " << *m
	  << " -- " << m
          << dendl;

  rank.submit_message(m, dest.addr);

  return 0;
}


const entity_addr_t &Rank::EntityMessenger::get_myaddr()
{
  return rank.my_addr;
}


void Rank::EntityMessenger::reset_myname(entity_name_t newname)
{
  rank.lock.Lock();
  {
    entity_name_t oldname = get_myname();
    dout(10) << "reset_myname " << oldname << " to " << newname << dendl;
    
    rank.local.erase(oldname);
    rank.local[newname] = this;
   
    _set_myname(newname);
  }
  rank.lock.Unlock();
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
    dout(10) << "mark_down " << a << " inst " << inst << " < " << entity_map[a] << dendl;
    derr(10) << "mark_down " << a << " inst " << inst << " < " << entity_map[a] << dendl;
    // do nothing!
  } else {
    if (entity_map.count(a) == 0) {
      // don't know it
      dout(10) << "mark_down " << a << " inst " << inst << " ... unknown by me" << dendl;
      derr(10) << "mark_down " << a << " inst " << inst << " ... unknown by me" << dendl;
    } else {
      // know it
      assert(entity_map[a] <= inst);
      dout(10) << "mark_down " << a << " inst " << inst << dendl;
      derr(10) << "mark_down " << a << " inst " << inst << dendl;
      
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


