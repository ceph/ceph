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

#include "config.h"

#include "messages/MGenericMessage.h"
#include "messages/MNSConnect.h"
#include "messages/MNSConnectAck.h"
#include "messages/MNSRegister.h"
#include "messages/MNSRegisterAck.h"
#include "messages/MNSLookup.h"
#include "messages/MNSLookupReply.h"
#include "messages/MNSFailure.h"

//#include "messages/MFailure.h"

#include <netdb.h>


#undef dout
#define dout(l)  if (l<=g_conf.debug_ms) cout << g_clock.now() << " -- " << rank.my_inst.addr << " "
#define derr(l)  if (l<=g_conf.debug_ms) cerr << g_clock.now() << " -- " << rank.my_inst.addr << " "



#include "tcp.cc"


Rank rank;



/********************************************
 * Accepter
 */

int Rank::Accepter::start()
{
  // bind to a socket
  dout(10) << "accepter.start binding to listen " << endl;
  
  /* socket creation */
  listen_sd = socket(AF_INET,SOCK_STREAM,0);
  assert(listen_sd > 0);
  
  /* bind to port */
  int rc = bind(listen_sd, (struct sockaddr *) &rank.listen_addr, sizeof(rank.listen_addr));
  if (rc < 0) 
    derr(0) << "accepter.start unable to bind to " << rank.listen_addr << endl;
  assert(rc >= 0);

  socklen_t llen = sizeof(rank.listen_addr);
  getsockname(listen_sd, (sockaddr*)&rank.listen_addr, &llen);
  
  int myport = rank.listen_addr.sin_port;

  // listen!
  rc = ::listen(listen_sd, 1000);
  assert(rc >= 0);

  //dout(10) << "accepter.start listening on " << myport << endl;
  
  // my address is...
  char host[100];
  bzero(host, 100);
  gethostname(host, 100);
  //dout(10) << "accepter.start my hostname is " << host << endl;

  struct hostent *myhostname = gethostbyname( host ); 

  struct sockaddr_in my_addr;  
  memset(&my_addr, 0, sizeof(my_addr));

  my_addr.sin_family = myhostname->h_addrtype;
  memcpy((char *) &my_addr.sin_addr.s_addr, 
         myhostname->h_addr_list[0], 
         myhostname->h_length);
  my_addr.sin_port = myport;
  
  rank.listen_addr = my_addr;
  
  dout(10) << "accepter.start listen addr is " << rank.listen_addr << endl;

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
      Pipe *p = new Pipe(sd);
      rank.pipes.insert(p);
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
  int rc = tcp_write(sd, (char*)&rank.my_inst, sizeof(rank.my_inst));
  if (rc < 0) {
    ::close(sd);
    done = true;
    return -1;
  }
  
  // identify peer
  rc = tcp_read(sd, (char*)&peer_inst, sizeof(peer_inst));
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
  if (peer_inst.rank >= 0) {
    rank.lock.Lock();
    {
      if (rank.rank_pipe.count(peer_inst.rank) == 0) {
        // install a pipe!
        dout(10) << "pipe(" << peer_inst << ' ' << this << ").accept peer is " << peer_inst << endl;
        rank.rank_pipe[peer_inst.rank] = this;
      } else {
        // low ranks' Pipes "win"
        if (peer_inst.rank < rank.my_inst.rank || 
            rank.my_inst.rank < 0) {
          dout(10) << "pipe(" << peer_inst << ' ' << this << ").accept peer is " << peer_inst 
                    << ", already had pipe, but switching to this new one" << endl;
          // switch to this new Pipe
          rank.rank_pipe[peer_inst.rank]->close();  // close old one
          rank.rank_pipe[peer_inst.rank] = this;
        } else {
          dout(10) << "pipe(" << peer_inst << ' ' << this << ").accept peer is " << peer_inst 
                    << ", already had pipe, sticking with it" << endl;
        }
      }
    }
    rank.lock.Unlock();
  } else {
    dout(10) << "pipe(" << peer_inst << ' ' << this << ").accept peer is unranked " << peer_inst << endl;
  }

  return 0;   // success.
}

int Rank::Pipe::connect()
{
  dout(10) << "pipe(" << peer_inst << ' ' << this << ").connect" << endl;

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
  rc = ::connect(sd, (sockaddr*)&peer_inst.addr, sizeof(myAddr));
  if (rc < 0) return rc;

  // identify peer
  entity_inst_t inst;
  rc = tcp_read(sd, (char*)&inst, sizeof(inst));
  if (inst.rank < 0) 
    inst = peer_inst;   // i know better than they do.
  if (peer_inst != inst && inst.rank > 0) {
    derr(0) << "pipe(" << peer_inst << ' ' << this << ").connect peer is " << inst << ", wtf" << endl;
    assert(0);
    return -1;
  }

  // identify myself
  rc = tcp_write(sd, (char*)&rank.my_inst, sizeof(rank.my_inst));
  if (rc < 0) 
    return -1;
  
  // register pipe
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_inst.rank) == 0) {
      dout(10) << "pipe(" << peer_inst << ' ' << this << ").connect registering pipe" << endl;
      rank.rank_pipe[peer_inst.rank] = this;
    } else {
      // this is normal.
      dout(10) << "pipe(" << peer_inst << ' ' << this << ").connect pipe already registered." << endl;
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
  if (sent_close) {
    dout(10) << "pipe(" << peer_inst << ' ' << this << ").close already closing" << endl;
    return;
  }
  dout(10) << "pipe(" << peer_inst << ' ' << this << ").close" << endl;

  // unreg ourselves
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_inst.rank) &&
        rank.rank_pipe[peer_inst.rank] == this) {
      dout(10) << "pipe(" << peer_inst << ' ' << this << ").close unregistering pipe" << endl;
      rank.rank_pipe.erase(peer_inst.rank);
    }
  }
  rank.lock.Unlock();

  // queue close message.
  if (socket_error) {
    dout(10) << "pipe(" << peer_inst << ' ' << this << ").close not queueing MSG_CLOSE, socket error" << endl;
  } else {
    dout(10) << "pipe(" << peer_inst << ' ' << this << ").close queueing MSG_CLOSE" << endl;
    lock.Lock();
    q.push_back(new MGenericMessage(MSG_CLOSE));
    cond.Signal();
    sent_close = true;
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
	dout(10) << "pipe(" << peer_inst << ' ' << this << ").reader read MSG_CLOSE message" << endl;
      } else {
	derr(10) << "pipe(" << peer_inst << ' ' << this << ").reader read null message" << endl;
      }

      if (!sent_close)
	close();

      done = true;
      cond.Signal();  // wake up writer too.
      break;
    }

    dout(10) << "pipe(" << peer_inst << ' ' << this << ").reader got message for " << m->get_dest() << endl;

    EntityMessenger *entity = 0;

    rank.lock.Lock();
    {
      if (rank.entity_map.count(m->get_source()) &&
          rank.entity_map[m->get_source()] > m->get_source_inst()) {
        derr(0) << "pipe(" << peer_inst << ' ' << this << ").reader source " << m->get_source() 
                << " inst " << m->get_source_inst() 
                << " > " << rank.entity_map[m->get_source()] 
                << ", WATCH OUT " << *m << endl;
        assert(0);
      }

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
	    derr(0) << "pipe(" << peer_inst << ' ' << this << ").reader got message " << *m << " for " << m->get_dest() << ", which isn't local" << endl;
	    assert(0);  // FIXME do this differently
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
    dout(20) << "pipe(" << peer_inst << ' ' << this << ").reader queueing for reap" << endl;
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
      derr(1) << "pipe(" << peer_inst << ' ' << this << ").writer error connecting" << endl;
      done = true;
      list<Message*> out;
      fail(out);
    }
  }

  // loop.
  lock.Lock();
  while (!q.empty() || !done) {
    
    if (!q.empty()) {
      dout(20) << "pipe(" << peer_inst << ' ' << this << ").writer grabbing message(s)" << endl;
      
      // grab outgoing list
      list<Message*> out;
      out.swap(q);
      
      // drop lock while i send these
      lock.Unlock();
      
      while (!out.empty()) {
        Message *m = out.front();
        out.pop_front();

        dout(20) << "pipe(" << peer_inst << ' ' << this << ").writer sending " << *m << endl;

        // stamp.
        m->set_source_inst(rank.my_inst);
        
        // marshall
        if (m->empty_payload())
          m->encode_payload();
        
        if (write_message(m) < 0) {
          // failed!
          derr(1) << "pipe(" << peer_inst << ' ' << this << ").writer error sending " << *m << " to " << m->get_dest() << endl;
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
    dout(20) << "pipe(" << peer_inst << ' ' << this << ").writer sleeping" << endl;
    cond.Wait(lock);
  }
  lock.Unlock(); 
  
  dout(20) << "pipe(" << peer_inst << ' ' << this << ").writer finishing" << endl;

  // reap?
  bool reap = false;
  lock.Lock();
  {
    writer_running = false;
    if (!reader_running) reap = true;
  }
  lock.Unlock();
  
  if (reap) {
    dout(20) << "pipe(" << peer_inst << ' ' << this << ").writer queueing for reap" << endl;
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
    socket_error = true;
    return 0;
  }
  
  dout(20) << "pipe(" << peer_inst << ' ' << this << ").reader got envelope type=" << env.type 
           << " src " << env.source << " dst " << env.dest
           << " nchunks=" << env.nchunks
           << endl;
  
  // payload
  bufferlist blist;
  for (int i=0; i<env.nchunks; i++) {
    int size;
    if (!tcp_read( sd, (char*)&size, sizeof(size) )) {
      socket_error = true;
      return 0;
    }
    
    if (size == 0) continue;

    bufferptr bp(size);
    
    if (!tcp_read( sd, bp.c_str(), size )) {
      socket_error = true;
      return 0;
    }
    
    blist.push_back(bp);
    
    dout(20) << "pipe(" << peer_inst << ' ' << this << ").reader got frag " << i << " of " << env.nchunks 
             << " len " << bp.length() << endl;
  }
  
  // unmarshall message
  size_t s = blist.length();
  Message *m = decode_message(env, blist);
  
  dout(20) << "pipe(" << peer_inst << ' ' << this << ").reader got " << s << " byte message from " 
           << m->get_source() << endl;
  
  return m;
}



int Rank::Pipe::write_message(Message *m)
{
  // get envelope, buffers
  msg_envelope_t *env = &m->get_envelope();
  bufferlist blist;
  blist.claim( m->get_payload() );
  
#ifdef TCP_KEEP_CHUNKS
  env->nchunks = blist.buffers().size();
#else
  env->nchunks = 1;
#endif

  dout(20)  << "pipe(" << peer_inst << ' ' << this << ").writer sending " << m << " " << *m 
            << " to " << m->get_dest()
            << endl;
  
  // send envelope
  int r = tcp_write( sd, (char*)env, sizeof(*env) );
  if (r < 0) { 
    derr(1) << "pipe(" << peer_inst << ' ' << this << ").writer error sending envelope for " << *m
             << " to " << m->get_dest() << endl; 
    socket_error = true;
    return -1;
  }

  // payload
#ifdef TCP_KEEP_CHUNKS
  // send chunk-wise
  int i = 0;
  for (list<bufferptr>::const_iterator it = blist.buffers().begin();
       it != blist.buffers().end();
       it++) {
    dout(10) << "pipe(" << peer_inst << ' ' << this << ").writer tcp_sending frag " << i << " len " << (*it).length() << endl;
    int size = (*it).length();
    r = tcp_write( sd, (char*)&size, sizeof(size) );
    if (r < 0) { 
      derr(10) << "pipe(" << peer_inst << ' ' << this << ").writer error sending chunk len for " << *m << " to " << m->get_dest() << endl; 
      socket_error = true;
      return -1;
    }
    r = tcp_write( sd, (*it).c_str(), size );
    if (r < 0) { 
      derr(10) << "pipe(" << peer_inst << ' ' << this << ").writer error sending data chunk for " << *m << " to " << m->get_dest() << endl; 
      socket_error = true;
      return -1;
    }
    i++;
  }
#else
  // one big chunk
  int size = blist.length();
  r = tcp_write( sd, (char*)&size, sizeof(size) );
  if (r < 0) { 
    derr(10) << "pipe(" << peer_inst << ' ' << this << ").writer error sending data len for " << *m << " to " << m->get_dest() << endl; 
    socket_error = true;
    return -1;
  }
  dout(20) << "pipe(" << peer_inst << ' ' << this << ").writer data len is " << size << " in " << blist.buffers().size() << " buffers" << endl;

  for (list<bufferptr>::const_iterator it = blist.buffers().begin();
       it != blist.buffers().end();
       it++) {
    if ((*it).length() == 0) continue;  // blank buffer.
    r = tcp_write( sd, (char*)(*it).c_str(), (*it).length() );
    if (r < 0) { 
      derr(10) << "pipe(" << peer_inst << ' ' << this << ").writer error sending data megachunk for " << *m << " to " << m->get_dest() << " : len " << (*it).length() << endl; 
      socket_error = true;
      return -1;
    }
  }
#endif
  
  return 0;
}


void Rank::Pipe::fail(list<Message*>& out)
{
  derr(10) << "pipe(" << peer_inst << ' ' << this << ").fail" << endl;

  // FIXME: possible race before i reclaim lock here?
  
  // deactivate myself
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(peer_inst.rank) &&
        rank.rank_pipe[peer_inst.rank] == this)
      rank.rank_pipe.erase(peer_inst.rank);
  }
  rank.lock.Unlock();

  // what do i do about reader()?   FIXME

  // sort my messages by (source) dispatcher, dest.
  map<Dispatcher*, map<msg_addr_t, list<Message*> > > by_dis;
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
	  dout(1) << "pipe(" << peer_inst << ' ' << this << ").fail on " << *q.front() << ", dispatcher stopping, ignoring." << endl;
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
  for (map<Dispatcher*, map<msg_addr_t, list<Message*> > >::iterator i = by_dis.begin();
       i != by_dis.end();
       ++i) 
    for (map<msg_addr_t, list<Message*> >::iterator j = i->second.begin();
         j != i->second.end();
         ++j) 
      for (list<Message*>::iterator k = j->second.begin();
           k != j->second.end();
           ++k) {
	derr(1) << "pipe(" << peer_inst << ' ' << this << ").fail on " << **k << " to " << j->first << " inst " << peer_inst << endl;
        i->first->ms_handle_failure(*k, j->first, peer_inst);
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

void Rank::set_listen_addr(tcpaddr_t& a)
{
  dout(10) << "set_listen_addr " << a << endl;
  memcpy((char*)&listen_addr.sin_addr.s_addr, (char*)&a.sin_addr.s_addr, 4);
  listen_addr.sin_port = a.sin_port;
}


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
		  << " <-- " << m->get_source() << " " << m->get_source_inst()
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
    dout(10) << "reaper reaping pipe " << p->get_peer_inst() << endl;
    pipe_reap_queue.pop_front();
    assert(pipes.count(p));
    pipes.erase(p);
    p->join();
    dout(10) << "reaper reaped pipe " << p->get_peer_inst() << endl;
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

  // my_inst
  my_inst.set_addr( listen_addr );

  dout(1) << "start_rank at " << my_inst << endl;

  lock.Unlock();
  return 0;
}



/* connect_rank
 * NOTE: assumes rank.lock held.
 */
Rank::Pipe *Rank::connect_rank(const entity_inst_t& inst)
{
  assert(rank.lock.is_locked());
  assert(inst != rank.my_inst);
  
  dout(10) << "connect_rank to " << inst << endl;
  
  // create pipe
  Pipe *pipe = new Pipe(inst);
  rank.rank_pipe[inst.rank] = pipe;
  pipes.insert(pipe);

  return pipe;
}





void Rank::show_dir()
{
  dout(10) << "show_dir ---" << endl;
  
  for (hash_map<msg_addr_t, entity_inst_t>::iterator i = entity_map.begin();
       i != entity_map.end();
       i++) {
    if (local.count(i->first)) {
      dout(10) << "show_dir entity_map " << i->first << " -> " << i->second << " local " << endl;
    } else {
      dout(10) << "show_dir entity_map " << i->first << " -> " << i->second << endl;
    }
  }
}

Rank::EntityMessenger *Rank::find_unnamed(msg_addr_t a)
{
  // find an unnamed local entity of the right type
  for (map<msg_addr_t, EntityMessenger*>::iterator p = local.begin();
       p != local.end();
       ++p) {
    if (p->first.type() == a.type() && p->first.is_new()) 
      return p->second;
  }
  return 0;
}




/* register_entity 
 */
Rank::EntityMessenger *Rank::register_entity(msg_addr_t addr)
{
  dout(10) << "register_entity " << addr << endl;
  lock.Lock();
  
  // create messenger
  EntityMessenger *msgr = new EntityMessenger(addr);

  // add to directory
  entity_map[addr] = my_inst;
  local[addr] = msgr;
  
  lock.Unlock();
  return msgr;
}


void Rank::unregister_entity(EntityMessenger *msgr)
{
  lock.Lock();
  dout(10) << "unregister_entity " << msgr->get_myaddr() << endl;
  
  // remove from local directory.
  assert(local.count(msgr->get_myaddr()));
  local.erase(msgr->get_myaddr());
  assert(entity_map.count(msgr->get_myaddr()));
  entity_map.erase(msgr->get_myaddr());

  wait_cond.Signal();

  lock.Unlock();
}


void Rank::submit_message(Message *m, const entity_inst_t& dest_inst)
{
  const msg_addr_t dest = m->get_dest();

  // lookup
  EntityMessenger *entity = 0;
  Pipe *pipe = 0;

  lock.Lock();
  {
    // local?
    if (dest_inst.rank == my_inst.rank) {
      if (local.count(dest)) {
        // local
        dout(20) << "submit_message " << *m << " dest " << dest << " local" << endl;
        if (g_conf.ms_single_dispatch) {
          _submit_single_dispatch(m);
        } else {
          entity = local[dest];
        }
      } else {
        derr(0) << "submit_message " << *m << " dest " << dest << " " << dest_inst << " local but not in local map?" << endl;
        assert(0);  // hmpf
      }
    }
    else {
      // remote.
      if (rank_pipe.count( dest_inst.rank )) {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_inst << ", already connected." << endl;
        // connected.
        pipe = rank_pipe[ dest_inst.rank ];
      } else {
        dout(20) << "submit_message " << *m << " dest " << dest << " remote, " << dest_inst << ", connecting." << endl;
        // not connected.
        pipe = connect_rank( dest_inst );
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
    }
    
    wait_cond.Wait(lock);
  }
  lock.Unlock();
  
  // done!  clean up.

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
    for (hash_map<__int64_t,Pipe*>::iterator i = rank_pipe.begin();
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

Rank::EntityMessenger::EntityMessenger(msg_addr_t myaddr) :
  Messenger(myaddr),
  stop(false),
  dispatch_thread(this)
{
  set_myinst(rank.my_inst);
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
          Message *m = ls.front();
          ls.pop_front();
          dout(1) << m->get_dest() 
		  << " <-- " << m->get_source() << " " << m->get_source_inst()
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
}

void Rank::EntityMessenger::ready()
{
  dout(10) << "ready " << get_myaddr() << endl;

  if (g_conf.ms_single_dispatch) {
    rank.lock.Lock();
    if (rank.waiting_for_ready.count(get_myaddr())) {
      rank.single_dispatch_queue.splice(rank.single_dispatch_queue.end(),
                                        rank.waiting_for_ready[get_myaddr()]);
      rank.waiting_for_ready.erase(get_myaddr());
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
  
  // deregister
  rank.unregister_entity(this);
  
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


void Rank::EntityMessenger::prepare_dest(const entity_inst_t& inst)
{
  rank.lock.Lock();
  {
    if (rank.rank_pipe.count(inst.rank) == 0)
      rank.connect_rank(inst);
  }
  rank.lock.Unlock();
}

int Rank::EntityMessenger::send_message(Message *m, msg_addr_t dest, entity_inst_t inst,
					int port, int fromport)
{
  // set envelope
  m->set_source(get_myaddr(), fromport);
  m->set_dest(dest, port);

  m->set_source_inst(rank.my_inst);
  
  dout(1) << m->get_source()
          << " --> " << m->get_dest() << " " << inst
          << " -- " << *m
	  << " -- " << m
          << endl;

  rank.submit_message(m, inst);

  return 0;
}


void Rank::EntityMessenger::reset_myaddr(msg_addr_t newaddr)
{
  msg_addr_t oldaddr = get_myaddr();
  dout(10) << "set_myaddr " << oldaddr << " to " << newaddr << endl;

  rank.entity_map.erase(oldaddr);
  rank.local.erase(oldaddr);
  rank.entity_map[newaddr] = rank.my_inst;
  rank.local[newaddr] = this;

  _set_myaddr(newaddr);
}




void Rank::EntityMessenger::mark_down(msg_addr_t a, entity_inst_t& i)
{
  assert(a != get_myaddr());
  rank.mark_down(a,i);
}

void Rank::mark_down(msg_addr_t a, entity_inst_t& inst)
{
  //if (my_rank == 0) return;   // ugh.. rank0 already handles this stuff in the namer
  lock.Lock();
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
      
      if (rank_pipe.count(inst.rank)) {
	rank_pipe[inst.rank]->close();
	rank_pipe.erase(inst.rank);
      }
    }
  }
  lock.Unlock();
}

void Rank::EntityMessenger::mark_up(msg_addr_t a, entity_inst_t& i)
{
  assert(a != get_myaddr());
  rank.mark_up(a, i);
}

void Rank::mark_up(msg_addr_t a, entity_inst_t& i)
{
  lock.Lock();
  {
    dout(10) << "mark_up " << a << " inst " << i << endl;
    derr(10) << "mark_up " << a << " inst " << i << endl;

    if (entity_map.count(a) == 0 ||
        entity_map[a] < i) {
      entity_map[a] = i;
      connect_rank(i);
    } else if (entity_map[a] == i) {
      dout(10) << "mark_up " << a << " inst " << i << " ... knew it" << endl;
      derr(10) << "mark_up " << a << " inst " << i << " ... knew it" << endl;
    } else {
      dout(-10) << "mark_up " << a << " inst " << i << " < " << entity_map[a] << endl;
      derr(-10) << "mark_up " << a << " inst " << i << " < " << entity_map[a] << endl;
    }

    //if (waiting_for_lookup.count(a))
    //lookup(a);
  }
  lock.Unlock();
}

