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

#ifndef __SIMPLEMESSENGER_H
#define __SIMPLEMESSENGER_H


#include <list>
#include <map>
using namespace std;
#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;


#include "include/types.h"

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

#include "Messenger.h"
#include "Message.h"
#include "tcp.h"



/* Rank - per-process
 */
class Rank {
public:
  void sigint();

private:
  class EntityMessenger;
  class Pipe;

  // incoming
  class Accepter : public Thread {
  public:
    bool done;

    int       listen_sd;
    
    Accepter() : done(false) {}
    
    void *entry();
    void stop();
    int start();
  } accepter;

  void sigint(int r);
  

  // pipe
  class Pipe {
  protected:
    int sd;
    bool done;
    entity_addr_t peer_addr;
    bool server;
    bool need_to_send_close;

    bool reader_running;
    bool writer_running;

    list<Message*> q;
    Mutex lock;
    Cond cond;
    
    int accept();   // server handshake
    int connect();  // client handshake
    void reader();
    void writer();

    Message *read_message();
    int write_message(Message *m);
    int do_sendmsg(Message *m, struct msghdr *msg, int len);
    void fail(list<Message*>& ls);

    // threads
    class Reader : public Thread {
      Pipe *pipe;
    public:
      Reader(Pipe *p) : pipe(p) {}
      void *entry() { pipe->reader(); return 0; }
    } reader_thread;
    friend class Reader;

    class Writer : public Thread {
      Pipe *pipe;
    public:
      Writer(Pipe *p) : pipe(p) {}
      void *entry() { pipe->writer(); return 0; }
    } writer_thread;
    friend class Writer;
    
  public:
    Pipe(int s) : sd(s),
		  done(false), server(true), 
		  need_to_send_close(true),
		  reader_running(false), writer_running(false),
		  reader_thread(this), writer_thread(this) {
      // server
      reader_running = true;
      reader_thread.create();
    }
    Pipe(const entity_addr_t &pi) : sd(0),
				    done(false), peer_addr(pi), server(false), 
				    need_to_send_close(true),
				    reader_running(false), writer_running(false),
				    reader_thread(this), writer_thread(this) {
      // client
      writer_running = true;
      writer_thread.create();
    }

    // public constructors
    static const Pipe& Server(int s);
    static const Pipe& Client(const entity_addr_t& pi);

    entity_addr_t& get_peer_addr() { return peer_addr; }

    void unregister();
    void close();
    void join() {
      if (writer_thread.is_started()) writer_thread.join();
      if (reader_thread.is_started()) reader_thread.join();
    }

    void send(Message *m) {
      lock.Lock();
      q.push_back(m);
      cond.Signal();
      lock.Unlock();
    }    
    void send(list<Message*>& ls) {
      lock.Lock();
      q.splice(q.end(), ls);
      cond.Signal();
      lock.Unlock();
    }

    void force_close() {
      ::close(sd);
    }
  };


  // messenger interface
  class EntityMessenger : public Messenger {
    Mutex lock;
    Cond cond;
    list<Message*> dispatch_queue;
    list<Message*> prio_dispatch_queue;
    bool stop;
    int qlen, pqlen;
    int my_rank;
    entity_addr_t my_addr;

    class DispatchThread : public Thread {
      EntityMessenger *m;
    public:
      DispatchThread(EntityMessenger *_m) : m(_m) {}
      void *entry() {
        m->dispatch_entry();
        return 0;
      }
    } dispatch_thread;
    void dispatch_entry();

    friend class Rank;

  public:
    void queue_message(Message *m) {
      // set recv stamp
      m->set_recv_stamp(g_clock.now());
      
      lock.Lock();
      if (m->get_source().is_mon()) {
	prio_dispatch_queue.push_back(m);
	pqlen++;
      } else {
	qlen++;
	dispatch_queue.push_back(m);
      }
      cond.Signal();
      lock.Unlock();
    }

  public:
    EntityMessenger(entity_name_t myaddr, int r) : 
      Messenger(myaddr),
      stop(false),
      qlen(0), pqlen(0),
      my_rank(r),
      dispatch_thread(this) { }
    ~EntityMessenger() {
      // join dispatch thread
      if (dispatch_thread.is_started())
	dispatch_thread.join();
    }

    void ready();
    bool is_stopped() { return stop; }

    void wait() {
      dispatch_thread.join();
    }
    
    const entity_addr_t &get_myaddr() { return my_addr; }

    int get_dispatch_queue_len() { return qlen + pqlen; }

    void reset_myname(entity_name_t m);

    int shutdown();
    void suicide();
    void prepare_dest(const entity_addr_t& addr);
    int send_message(Message *m, entity_inst_t dest,
		     int port=0, int fromport=0);
    int send_first_message(Dispatcher *d,
			   Message *m, entity_inst_t dest,
			   int port=0, int fromport=0);
    
    void mark_down(entity_addr_t a);
    void mark_up(entity_name_t a, entity_addr_t& i);
  };


  // Rank stuff
 public:
  Mutex lock;
  Cond  wait_cond;  // for wait()
  bool started;

  // where i listen
  entity_addr_t rank_addr;
  
  // local
  unsigned max_local, num_local;
  vector<EntityMessenger*> local;
  vector<bool>             stopped;
  
  // remote
  hash_map<entity_addr_t, Pipe*> rank_pipe;

  set<Pipe*>      pipes;
  list<Pipe*>     pipe_reap_queue;
        
  Pipe *connect_rank(const entity_addr_t& addr);

  const entity_addr_t &get_rank_addr() { return rank_addr; }

  void mark_down(entity_addr_t addr);

  void reaper();

public:
  Rank() : started(false),
	   max_local(0), num_local(0) { }
  ~Rank() { }

  //void set_listen_addr(tcpaddr_t& a);

  int start_rank();
  void wait();

  EntityMessenger *register_entity(entity_name_t addr);
  void rename_entity(EntityMessenger *ms, entity_name_t newaddr);
  void unregister_entity(EntityMessenger *ms);

  void submit_message(Message *m, const entity_addr_t& addr);  
  void prepare_dest(const entity_addr_t& addr);

  // create a new messenger
  EntityMessenger *new_entity(entity_name_t addr);

} ;



extern Rank rank;

#endif
