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

#include "include/types.h"

#include <list>
#include <map>
using namespace std;
#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

#include "Messenger.h"
#include "Message.h"
#include "tcp.h"



/* Rank - per-process
 */
class SimpleMessenger {
public:
  struct Policy {
    bool lossy_tx;                // 
    bool server;
    float retry_interval;         // initial retry interval.  0 => fail immediately (lossy_tx=true)
    float fail_interval;          // before we call ms_handle_failure (lossy_tx=true)
    bool drop_msg_callback;
    bool fail_callback;
    bool remote_reset_callback;
    Policy() : 
      lossy_tx(false), server(false),
      retry_interval(g_conf.ms_retry_interval),
      fail_interval(g_conf.ms_fail_interval),
      drop_msg_callback(true),
      fail_callback(true),
      remote_reset_callback(true) {}

    Policy(bool tx, bool sr, float r, float f, bool dmc, bool fc, bool rrc) :
      lossy_tx(tx), server(sr),
      retry_interval(r), fail_interval(f),
      drop_msg_callback(dmc),
      fail_callback(fc),
      remote_reset_callback(rrc) {}

    // new
    static Policy stateful_server() { return Policy(false, true, g_conf.ms_retry_interval, 0,
						    true, true, true); }
    static Policy stateless_server() { return Policy(true, true, -1, -1,
						     true, true, true); }

    // old
    static Policy lossless() { return Policy(false, false,
					     g_conf.ms_retry_interval, 0,
					     true, true, true); }
    static Policy lossy_fail_after(float f) {
      return Policy(true, false,
		    MIN(g_conf.ms_retry_interval, f), f,
		    true, true, true);
    }
    static Policy lossy_fast_fail() { return Policy(true, false, -1, -1, true, true, true); }

    /*
    static Policy fast_fail() { return Policy(-1, -1, true, true, true); }
    static Policy fail_after(float f) { return Policy(MIN(g_conf.ms_retry_interval, f), f, true, true, true); }
    static Policy retry_forever() { return Policy(g_conf.ms_retry_interval, -1, false, true, true); }
    */
  };


public:
  void sigint();

private:
  class Endpoint;
  class Pipe;

  // incoming
  class Accepter : public Thread {
  public:
    SimpleMessenger *rank;
    bool done;
    int listen_sd;
    
    Accepter(SimpleMessenger *r) : rank(r), done(false), listen_sd(-1) {}
    
    void *entry();
    void stop();
    int bind(int64_t force_nonce);
    int start();
  } accepter;

  void sigint(int r);

  // pipe
  class Pipe {
  public:
    SimpleMessenger *rank;
    ostream& _pipe_prefix();

    enum {
      STATE_ACCEPTING,
      STATE_CONNECTING,
      STATE_OPEN,
      STATE_STANDBY,
      STATE_CLOSED,
      STATE_CLOSING,
      STATE_WAIT       // just wait for racing connection
    };

    int sd;
    int peer_type;
    entity_addr_t peer_addr;
    Policy policy;
    bool lossy_rx;
    
    Mutex lock;
    int state;

  protected:
    Connection *connection_state;

    utime_t first_fault;   // time of original failure
    utime_t last_attempt;  // time of last reconnect attempt

    bool reader_running;
    bool writer_running;

    map<int, list<Message*> > q;  // priority queue
    list<Message*> sent;
    Cond cond;
    bool keepalive;
    
    __u32 connect_seq, peer_global_seq;
    __u64 out_seq;
    __u64 in_seq, in_seq_acked;
    
    int accept();   // server handshake
    int connect();  // client handshake
    void reader();
    void writer();

    Message *read_message();
    int write_message(Message *m);
    int do_sendmsg(int sd, struct msghdr *msg, int len);
    int write_ack(__u64 s);
    int write_keepalive();

    void fault(bool silent=false, bool reader=false);
    void fail();

    void was_session_reset();

    void report_failures();

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
    Pipe(SimpleMessenger *r, int st) : 
      rank(r),
      sd(-1), peer_type(-1),
      lock("SimpleMessenger::Pipe::lock"),
      state(st), 
      connection_state(new Connection),
      reader_running(false), writer_running(false),
      keepalive(false),
      connect_seq(0), peer_global_seq(0),
      out_seq(0), in_seq(0), in_seq_acked(0),
      reader_thread(this), writer_thread(this) { }
    ~Pipe() {
      assert(q.empty());
      assert(sent.empty());
      connection_state->put();
    }


    void start_reader() {
      reader_running = true;
      reader_thread.create();
    }
    void start_writer() {
      writer_running = true;
      writer_thread.create();
    }
    void join_reader() {
      if (!reader_running)
	return;
      cond.Signal();
      reader_thread.kill(SIGUSR2);
      lock.Unlock();
      reader_thread.join();
      lock.Lock();
    }

    // public constructors
    static const Pipe& Server(int s);
    static const Pipe& Client(const entity_addr_t& pi);

    entity_addr_t& get_peer_addr() { return peer_addr; }

    __u32 get_out_seq() { return out_seq; }

    bool is_queued() { return !q.empty() || keepalive; }

    void register_pipe();
    void unregister_pipe();
    void join() {
      if (writer_thread.is_started()) writer_thread.join();
      if (reader_thread.is_started()) reader_thread.join();
    }
    void stop();

    void send(Message *m) {
      lock.Lock();
      _send(m);
      lock.Unlock();
    }    
    void _send(Message *m) {
      m->get();
      q[m->get_priority()].push_back(m);
      cond.Signal();
    }
    void send_keepalive() {
      lock.Lock();
      _send_keepalive();
      lock.Unlock();
    }    
    void _send_keepalive() {
      keepalive = true;
      cond.Signal();
    }
    Message *_get_next_outgoing() {
      Message *m = 0;
      while (!m && !q.empty()) {
	map<int, list<Message*> >::reverse_iterator p = q.rbegin();
	if (!p->second.empty()) {
	  m = p->second.front();
	  p->second.pop_front();
	}
	if (p->second.empty())
	  q.erase(p->first);
      }
      return m;
    }

    void requeue_sent();
    void discard_queue();

    void force_close() {
      if (sd >= 0) ::close(sd);
    }
  };


  // messenger interface
  class Endpoint : public Messenger {
    SimpleMessenger *rank;
    Mutex lock;
    Cond cond;
    map<int, list<Message*> > dispatch_queue;
    bool stop;
    int qlen;
    int my_rank;

    class DispatchThread : public Thread {
      Endpoint *m;
    public:
      DispatchThread(Endpoint *_m) : m(_m) {}
      void *entry() {
        m->dispatch_entry();
        return 0;
      }
    } dispatch_thread;
    void dispatch_entry();

    friend class SimpleMessenger;

  public:
    void queue_message(Message *m) {
      // set recv stamp
      m->set_recv_stamp(g_clock.now());

      assert(m->nref.test() == 0);

      lock.Lock();
      qlen++;
      dispatch_queue[m->get_priority()].push_back(m);
      cond.Signal();
      lock.Unlock();
    }

    enum { BAD_REMOTE_RESET, BAD_RESET, BAD_FAILED };
    list<entity_addr_t> remote_reset_q;
    list<entity_addr_t> reset_q;
    list<pair<Message*,entity_addr_t> > failed_q;

    void queue_remote_reset(entity_addr_t a) {
      lock.Lock();
      remote_reset_q.push_back(a);
      dispatch_queue[CEPH_MSG_PRIO_HIGHEST].push_back((Message*)BAD_REMOTE_RESET);
      cond.Signal();
      lock.Unlock();
    }
    void queue_reset(entity_addr_t a) {
      lock.Lock();
      reset_q.push_back(a);
      dispatch_queue[CEPH_MSG_PRIO_HIGHEST].push_back((Message*)BAD_RESET);
      cond.Signal();
      lock.Unlock();
    }
    void queue_failure(Message *m, entity_addr_t a) {
      lock.Lock();
      m->get();
      failed_q.push_back(pair<Message*,entity_addr_t>(m, a));
      dispatch_queue[CEPH_MSG_PRIO_HIGHEST].push_back((Message*)BAD_FAILED);
      cond.Signal();
      lock.Unlock();
    }

  public:
    Endpoint(SimpleMessenger *r, entity_name_t name, int rn) : 
      Messenger(name),
      rank(r),
      lock("SimpleMessenger::Endpoint::lock"),
      stop(false),
      qlen(0),
      my_rank(rn),
      dispatch_thread(this) { }
    ~Endpoint() { }

    void destroy() {
      // join dispatch thread
      if (dispatch_thread.is_started())
	dispatch_thread.join();

      Messenger::destroy();
    }

    void ready();
    bool is_stopped() { return stop; }

    void wait() {
      dispatch_thread.join();
    }
    
    int get_dispatch_queue_len() { return qlen; }

    entity_addr_t get_myaddr();


    int shutdown();
    void suicide();
    void prepare_dest(const entity_inst_t& inst);
    int send_message(Message *m, entity_inst_t dest);
    int forward_message(Message *m, entity_inst_t dest);
    int lazy_send_message(Message *m, entity_inst_t dest);
    int send_keepalive(entity_inst_t dest);

    void mark_down(entity_addr_t a);
    void mark_up(entity_name_t a, entity_addr_t& i);
  };


  // SimpleMessenger stuff
 public:
  Mutex lock;
  Cond  wait_cond;  // for wait()
  bool started;

  // where i listen
  bool need_addr;
  entity_addr_t rank_addr;
  
  // local
  unsigned max_local, num_local;
  vector<Endpoint*> local;
  vector<bool>             stopped;
  
  // remote
  hash_map<entity_addr_t, Pipe*> rank_pipe;
 
  int my_type;
  Policy default_policy;
  map<int, Policy> policy_map; // entity_name_t::type -> Policy

  set<Pipe*>      pipes;
  list<Pipe*>     pipe_reap_queue;
  
  Mutex global_seq_lock;
  __u32 global_seq;
      
  Pipe *connect_rank(const entity_addr_t& addr, int type);

  const entity_addr_t &get_rank_addr() { return rank_addr; }

  void mark_down(entity_addr_t addr);

  void reaper();

  Policy get_policy(int t) {
    if (policy_map.count(t))
      return policy_map[t];
    else
      return default_policy;
  }

public:
  SimpleMessenger() : accepter(this),
	   lock("SimpleMessenger::lock"), started(false), need_addr(true),
	   max_local(0), num_local(0),
	   my_type(-1),
	   global_seq_lock("SimpleMessenger::global_seq_lock"), global_seq(0) { }
  ~SimpleMessenger() { }

  //void set_listen_addr(tcpaddr_t& a);

  int bind(int64_t force_nonce = -1);
  int start(bool nodaemon = false);
  void wait();

  __u32 get_global_seq(__u32 old=0) {
    Mutex::Locker l(global_seq_lock);
    if (old > global_seq)
      global_seq = old;
    return ++global_seq;
  }

  void set_addr(entity_addr_t a) {
    rank_addr = a;
    need_addr = false;
  }

  Endpoint *register_entity(entity_name_t addr);
  void rename_entity(Endpoint *ms, entity_name_t newaddr);
  void unregister_entity(Endpoint *ms);

  void submit_message(Message *m, const entity_inst_t& addr, bool lazy=false);  
  void prepare_dest(const entity_inst_t& inst);
  void send_keepalive(const entity_inst_t& addr);  

  void learned_addr(entity_addr_t peer_addr_for_me);

  // create a new messenger
  Endpoint *new_entity(entity_name_t addr);

  void set_default_policy(Policy p) {
    default_policy = p;
  }
  void set_policy(int type, Policy p) {
    policy_map[type] = p;
  }
} ;

#endif
