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
 
  class EntityMessenger;
  class Pipe;

  // incoming
  class Accepter : public Thread {
  public:
    bool done;

    tcpaddr_t listen_addr;
    int       listen_sd;
    
    Accepter() : done(false) {}
    
    void *entry();
    void stop() {
      done = true;
      ::close(listen_sd);
      join();
    }
    int start();
  } accepter;
  

  // pipe
  class Pipe {
  protected:
    int sd;
    bool done;
    entity_inst_t peer_inst;
    bool server;
    bool sent_close;
    bool socket_error;

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
		  sent_close(false), socket_error(false),
		  reader_running(false), writer_running(false),
		  reader_thread(this), writer_thread(this) {
      // server
      reader_running = true;
      reader_thread.create();
    }
    Pipe(const entity_inst_t &pi) : sd(0),
      done(false), peer_inst(pi), server(false), 
      sent_close(false),
      reader_running(false), writer_running(false),
      reader_thread(this), writer_thread(this) {
      // client
      writer_running = true;
      writer_thread.create();
    }

    // public constructors
    static const Pipe& Server(int s);
    static const Pipe& Client(const entity_inst_t& pi);

    entity_inst_t& get_peer_inst() { return peer_inst; }

    void close();
    void join() {
      writer_thread.join();
      reader_thread.join();
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
  };



  // messenger interface
  class EntityMessenger : public Messenger {
    Mutex lock;
    Cond cond;
    list<Message*> dispatch_queue;
    bool stop;

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

  public:
    void queue_message(Message *m) {
      lock.Lock();
      dispatch_queue.push_back(m);
      cond.Signal();
      lock.Unlock();
    }
    void queue_messages(list<Message*> ls) {
      lock.Lock();
      dispatch_queue.splice(dispatch_queue.end(), ls);
      cond.Signal();
      lock.Unlock();
    }

  public:
    EntityMessenger(msg_addr_t myaddr);
    ~EntityMessenger();

    void ready();
    bool is_stopped() { return stop; }

    void wait() {
      dispatch_thread.join();
    }
    
    void reset_myaddr(msg_addr_t m);

    void callback_kick() {} 
    int shutdown();
    void prepare_dest(const entity_inst_t& inst);
    int send_message(Message *m, msg_addr_t dest, entity_inst_t inst,
		     int port=0, int fromport=0);
    
    void mark_down(msg_addr_t a, entity_inst_t& i);
    void mark_up(msg_addr_t a, entity_inst_t& i);
  };


  class SingleDispatcher : public Thread {
    Rank *rank;
  public:
    SingleDispatcher(Rank *r) : rank(r) {}
    void *entry() {
      rank->single_dispatcher_entry();
      return 0;
    }
  } single_dispatcher;

  Cond            single_dispatch_cond;
  bool            single_dispatch_stop;
  list<Message*>  single_dispatch_queue;

  map<msg_addr_t, list<Message*> > waiting_for_ready;

  void single_dispatcher_entry();
  void _submit_single_dispatch(Message *m);


  // Rank stuff
 public:
  Mutex lock;
  Cond  wait_cond;  // for wait()
  
  // my instance
  entity_inst_t my_inst;
  
  // lookup
  hash_map<msg_addr_t, entity_inst_t> entity_map;
  hash_set<msg_addr_t>                entity_unstarted;

  // local
  map<msg_addr_t, EntityMessenger*> local;
  
  // remote
  hash_map<__int64_t, Pipe*> rank_pipe;

  set<Pipe*>      pipes;
  list<Pipe*>     pipe_reap_queue;
    
  void show_dir();
    
  Pipe *connect_rank(const entity_inst_t& inst);

  void mark_down(msg_addr_t addr, entity_inst_t& i);
  void mark_up(msg_addr_t addr, entity_inst_t& i);

  tcpaddr_t get_listen_addr() { return accepter.listen_addr; }

  void reaper();

  EntityMessenger *find_unnamed(msg_addr_t a);

public:
  Rank();
  ~Rank();

  int find_ns_addr(tcpaddr_t &tcpaddr);

  int start_rank();
  void wait();

  EntityMessenger *register_entity(msg_addr_t addr);
  void rename_entity(EntityMessenger *ms, msg_addr_t newaddr);
  void unregister_entity(EntityMessenger *ms);

  void submit_message(Message *m, const entity_inst_t& inst);  
  void prepare_dest(const entity_inst_t& inst);

  // create a new messenger
  EntityMessenger *new_entity(msg_addr_t addr);

} ;



extern Rank rank;

#endif
