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

#ifndef CEPH_SIMPLEMESSENGER_H
#define CEPH_SIMPLEMESSENGER_H

#include "include/types.h"
#include "include/xlist.h"

#include <list>
#include <map>
using namespace std;
#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;

#include "common/Mutex.h"
#include "include/atomic.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "common/Throttle.h"

#include "Messenger.h"
#include "Message.h"
#include "tcp.h"


/*
 * This class handles transmission and reception of messages. Generally
 * speaking, there are 2 major components:
 * 1) Pipe. Each network connection is handled through a pipe, which handles
 *    the input and output of each message.
 * 2) SimpleMessenger. It's the exterior class passed to the external 
 *    message handler and handles queuing and ordering of pipes. Each
 *    pipe maintains its own message ordering, but the SimpleMessenger
 *    decides what order pipes get to deliver messages in.
 *
 * This class should only be created on the heap, and it should be destroyed
 * via a call to destroy(). Making it on the stack or otherwise calling
 * the destructor will lead to badness.
 */

class SimpleMessenger : public Messenger {
public:
  /** @defgroup Accessors
   * @{
   */
  /**
   * Set the IP this SimpleMessenger is using. This is useful if it's unset
   * but another SimpleMessenger on the same interface has already learned its
   * IP. Of course, this function does not change the port, since the
   * SimpleMessenger always knows the correct setting for that.
   * If the SimpleMesssenger's IP is already set, this function is a no-op.
   *
   * @param addr The IP address to set internally.
   */
  void set_addr_unknowns(entity_addr_t& addr);
  /**
   * Retrieve the Connection for an endpoint.
   *
   * @param dest The endpoint you want to get a Connection for.
   * @return The requested Connection, as a pointer whose reference you own.
   */
  virtual Connection *get_connection(const entity_inst_t& dest);
  /** @} Accessors */

  /**
   * @defgroup Configuration functions
   * @{
   */
  /**
   * Set a policy which is applied to all peers who do not have a type-specific
   * Policy.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param p The Policy to apply.
   */
  void set_default_policy(Policy p) {
    assert(!started && !did_bind);
    default_policy = p;
  }
  /**
   * Set a policy which is applied to all peers of the given type.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param type The peer type this policy applies to.
   * @param p The policy to apply.
   */
  void set_policy(int type, Policy p) {
    assert(!started && !did_bind);
    policy_map[type] = p;
  }
  /**
   * Set a Throttler which is applied to all Messages from the given
   * type of peer.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param type The peer type this Throttler will apply to.
   * @param t The Throttler to apply. SimpleMessenger does not take
   * ownership of this pointer, but you must not destroy it before
   * you destroy SimpleMessenger.
   */
  void set_policy_throttler(int type, Throttle *t) {
    assert (!started && !did_bind);
    get_policy(type).throttler = t;
  }
  /**
   * Set the cluster protocol in use by this daemon.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param p The cluster protocol to use. Defined externally.
   */
  void set_cluster_protocol(int p) {
    assert(!started && !did_bind);
    cluster_protocol = p;
  }
  /** @} Configuration functions */
private:
  class Pipe;

  // incoming
  class Accepter : public Thread {
  public:
    SimpleMessenger *msgr;
    bool done;
    int listen_sd;
    
    Accepter(SimpleMessenger *r) : msgr(r), done(false), listen_sd(-1) {}
    
    void *entry();
    void stop();
    int bind(entity_addr_t &bind_addr, int avoid_port1=0, int avoid_port2=0);
    int rebind(int avoid_port);
    int start();
  } accepter;

  // pipe
  class Pipe : public RefCountedObject {
  public:
    SimpleMessenger *msgr;
    ostream& _pipe_prefix(std::ostream *_dout);

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
    
    Mutex pipe_lock;
    int state;

  protected:
    friend class SimpleMessenger;
    Connection *connection_state;

    utime_t backoff;         // backoff time

    bool reader_running, reader_joining;
    bool writer_running;

    map<int, list<Message*> > out_q;  // priority queue for outbound msgs
    map<int, list<Message*> > in_q; // and inbound ones
    int in_qlen;
    map<int, xlist<Pipe *>::item* > queue_items; // protected by pipe_lock AND q.lock
    list<Message*> sent;
    Cond cond;
    bool keepalive;
    bool halt_delivery; //if a pipe's queue is destroyed, stop adding to it
    bool close_on_empty;
    bool disposable;
    
    __u32 connect_seq, peer_global_seq;
    uint64_t out_seq;
    uint64_t in_seq, in_seq_acked;
    
    int accept();   // server handshake
    int connect();  // client handshake
    void reader();
    void writer();
    void unlock_maybe_reap();

    int read_message(Message **pm);
    int write_message(Message *m);
    /**
     * Write the given data (of length len) to the Pipe's socket. This function
     * will loop until all passed data has been written out.
     * If more is set, the function will optimize socket writes
     * for additional data (by passing the MSG_MORE flag, aka TCP_CORK).
     *
     * @param msg The msghdr to write out
     * @param len The length of the data in msg
     * @param more Should be set true if this is one part of a larger message
     * @return 0, or -1 on failure (unrecoverable -- close the socket).
     */
    int do_sendmsg(struct msghdr *msg, int len, bool more=false);
    int write_ack(uint64_t s);
    int write_keepalive();

    void fault(bool onconnect=false, bool reader=false);
    void fail();

    void was_session_reset();

    /* Clean up sent list */
    void handle_ack(uint64_t seq) {
      lsubdout(msgr->cct, ms, 15) << "reader got ack seq " << seq << dendl;
      // trim sent list
      while (!sent.empty() &&
          sent.front()->get_seq() <= seq) {
        Message *m = sent.front();
        sent.pop_front();
        lsubdout(msgr->cct, ms, 10) << "reader got ack seq "
            << seq << " >= " << m->get_seq() << " on " << m << " " << *m << dendl;
        m->put();
      }

      if (sent.empty() && close_on_empty) {
	// this is slightly hacky
	lsubdout(msgr->cct, ms, 10) << "reader got last ack, queue empty, closing" << dendl;
	policy.lossy = true;
	fault();
      }
    }

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
    Pipe(const Pipe& other);
    const Pipe& operator=(const Pipe& other);

    Pipe(SimpleMessenger *r, int st) : 
      msgr(r),
      sd(-1),
      peer_type(-1),
      pipe_lock("SimpleMessenger::Pipe::pipe_lock"),
      state(st), 
      connection_state(new Connection),
      reader_running(false), reader_joining(false), writer_running(false),
      in_qlen(0), keepalive(false), halt_delivery(false), 
      close_on_empty(false), disposable(false),
      connect_seq(0), peer_global_seq(0),
      out_seq(0), in_seq(0), in_seq_acked(0),
      reader_thread(this), writer_thread(this) {
      connection_state->pipe = get();
      msgr->timeout = msgr->cct->_conf->ms_tcp_read_timeout * 1000; //convert to ms
      if (msgr->timeout == 0)
        msgr->timeout = -1;
    }
    ~Pipe() {
      for (map<int, xlist<Pipe *>::item* >::iterator i = queue_items.begin();
	   i != queue_items.end();
	   ++i) {
	assert(!i->second->is_on_list());
	delete i->second;
      }
      assert(out_q.empty());
      assert(sent.empty());
      if (connection_state)
        connection_state->put();
    }


    void start_reader() {
      assert(pipe_lock.is_locked());
      assert(!reader_running);
      reader_running = true;
      reader_thread.create(msgr->cct->_conf->ms_rwthread_stack_bytes);
    }
    void start_writer() {
      assert(pipe_lock.is_locked());
      assert(!writer_running);
      writer_running = true;
      writer_thread.create(msgr->cct->_conf->ms_rwthread_stack_bytes);
    }
    void join_reader() {
      if (!reader_running)
	return;
      assert(!reader_joining);
      reader_joining = true;
      cond.Signal();
      pipe_lock.Unlock();
      reader_thread.join();
      pipe_lock.Lock();
      assert(reader_joining);
      reader_joining = false;
    }

    // public constructors
    static const Pipe& Server(int s);
    static const Pipe& Client(const entity_addr_t& pi);

    //we have two queue_received's to allow local signal delivery
    // via Message * (that doesn't actually point to a Message)
    void queue_received(Message *m, int priority);
    
    void queue_received(Message *m) {
      // this is just to make sure that a changeset is working
      // properly; if you start using the refcounting more and have
      // multiple people hanging on to a message, ditch the assert!
      assert(m->nref.read() == 1); 

      queue_received(m, m->get_priority());
    }

    __u32 get_out_seq() { return out_seq; }

    bool is_queued() { return !out_q.empty() || keepalive; }

    entity_addr_t& get_peer_addr() { return peer_addr; }

    void set_peer_addr(const entity_addr_t& a) {
      if (&peer_addr != &a)  // shut up valgrind
	peer_addr = a;
      connection_state->set_peer_addr(a);
    }
    void set_peer_type(int t) {
      peer_type = t;
      connection_state->set_peer_type(t);
    }

    void register_pipe();
    void unregister_pipe();
    void join() {
      if (writer_thread.is_started())
	writer_thread.join();
      if (reader_thread.is_started())
	reader_thread.join();
    }
    void stop();

    void send(Message *m) {
      pipe_lock.Lock();
      _send(m);
      pipe_lock.Unlock();
    }    
    void _send(Message *m) {
      out_q[m->get_priority()].push_back(m);
      cond.Signal();
    }
    void _send_keepalive() {
      keepalive = true;
      cond.Signal();
    }
    Message *_get_next_outgoing() {
      Message *m = 0;
      while (!m && !out_q.empty()) {
	map<int, list<Message*> >::reverse_iterator p = out_q.rbegin();
	if (!p->second.empty()) {
	  m = p->second.front();
	  p->second.pop_front();
	}
	if (p->second.empty())
	  out_q.erase(p->first);
      }
      return m;
    }

    /* Remove all messages from the sent queue. Add those with seq > max_acked
     * to the highest priority outgoing queue. */
    void requeue_sent(uint64_t max_acked=0);
    void discard_queue();

    void shutdown_socket() {
      if (sd >= 0)
        ::shutdown(sd, SHUT_RDWR);
    }
  };


  struct DispatchQueue {
    Mutex lock;
    Cond cond;
    bool stop;

    map<int, xlist<Pipe *>* > queued_pipes;
    map<int, xlist<Pipe *>::iterator> queued_pipe_iters;
    atomic_t qlen;
    
    enum { D_CONNECT = 0, D_BAD_REMOTE_RESET, D_BAD_RESET, D_NUM_CODES };
    list<Connection*> connect_q;
    list<Connection*> remote_reset_q;
    list<Connection*> reset_q;

    Pipe *local_pipe;
    void local_delivery(Message *m, int priority) {
      local_pipe->pipe_lock.Lock();
      if ((unsigned long)m > 10)
	m->set_connection(local_pipe->connection_state->get());
      local_pipe->queue_received(m, priority);
      local_pipe->pipe_lock.Unlock();
    }

    int get_queue_len() {
      return qlen.read();
    }
    
    void queue_connect(Connection *con) {
      lock.Lock();
      connect_q.push_back(con);
      lock.Unlock();
      local_delivery((Message*)D_CONNECT, CEPH_MSG_PRIO_HIGHEST);
    }
    void queue_remote_reset(Connection *con) {
      lock.Lock();
      remote_reset_q.push_back(con);
      lock.Unlock();
      local_delivery((Message*)D_BAD_REMOTE_RESET, CEPH_MSG_PRIO_HIGHEST);
    }
    void queue_reset(Connection *con) {
      lock.Lock();
      reset_q.push_back(con);
      lock.Unlock();
      local_delivery((Message*)D_BAD_RESET, CEPH_MSG_PRIO_HIGHEST);
    }

    DispatchQueue() :
      lock("SimpleMessenger::DispatchQeueu::lock"), 
      stop(false),
      qlen(0),
      local_pipe(NULL)
    {}
    ~DispatchQueue() {
      for (map< int, xlist<Pipe *>* >::iterator i = queued_pipes.begin();
	   i != queued_pipes.end();
	   ++i) {
	i->second->clear();
	delete i->second;
      }
    }
  } dispatch_queue;

  void dispatch_throttle_release(uint64_t msize);

  // SimpleMessenger stuff
  Mutex lock;
  Cond  wait_cond;  // for wait()
  bool did_bind;
  Throttle dispatch_throttler;

  // where i listen
  bool need_addr;
  uint64_t nonce;
  
  // local
  bool destination_stopped;
  
  // remote
  hash_map<entity_addr_t, Pipe*> rank_pipe;
 
  int my_type;


  // --- policy ---
  Policy default_policy;
  map<int, Policy> policy_map; // entity_name_t::type -> Policy

  // --- pipes ---
  set<Pipe*>      pipes;
  list<Pipe*>     pipe_reap_queue;

  Mutex global_seq_lock;
  __u32 global_seq;
public:
  Policy& get_policy(int t) {
    if (policy_map.count(t))
      return policy_map[t];
    else
      return default_policy;
  }

  Pipe *connect_rank(const entity_addr_t& addr, int type);
  virtual void mark_down(const entity_addr_t& addr);
  virtual void mark_down(Connection *con);
  virtual void mark_down_on_empty(Connection *con);
  virtual void mark_disposable(Connection *con);
  virtual void mark_down_all();

  // reaper
  class ReaperThread : public Thread {
    SimpleMessenger *msgr;
  public:
    ReaperThread(SimpleMessenger *m) : msgr(m) {}
    void *entry() {
      msgr->reaper_entry();
      return 0;
    }
  } reaper_thread;

  bool reaper_started, reaper_stop;
  Cond reaper_cond;

  void reaper_entry();
  void reaper();
  void queue_reap(Pipe *pipe);


  /***** Messenger-required functions  **********/
  int get_dispatch_queue_len() {
    return dispatch_queue.get_queue_len();
  }

  virtual void ready();
  virtual int shutdown();
  virtual void suicide();
  void prepare_dest(const entity_inst_t& inst);
  virtual int send_message(Message *m, const entity_inst_t& dest);
  virtual int send_message(Message *m, Connection *con);
  virtual int lazy_send_message(Message *m, const entity_inst_t& dest);
  virtual int lazy_send_message(Message *m, Connection *con) {
    return send_message(m, con);
  }

  /***********************/

private:
  class DispatchThread : public Thread {
    SimpleMessenger *msgr;
  public:
    DispatchThread(SimpleMessenger *_messenger) : msgr(_messenger) {}
    void *entry() {
      msgr->dispatch_entry();
      return 0;
    }
  } dispatch_thread;

  void dispatch_entry();

  SimpleMessenger *msgr; //hack to make dout macro work, will fix
  int timeout;
  
  /// internal cluster protocol version, if any, for talking to entities of the same type.
  int cluster_protocol;

  int get_proto_version(int peer_type, bool connect);

public:
  SimpleMessenger(CephContext *cct, entity_name_t name, string mname, uint64_t _nonce) :
    Messenger(cct, name, mname),
    accepter(this),
    lock("SimpleMessenger::lock"), did_bind(false),
    dispatch_throttler(cct, "dispatch_throttler", cct->_conf->ms_dispatch_throttle_bytes),
    need_addr(true),
    nonce(_nonce), destination_stopped(false), my_type(name.type()),
    global_seq_lock("SimpleMessenger::global_seq_lock"), global_seq(0),
    reaper_thread(this), reaper_started(false), reaper_stop(false), 
    dispatch_thread(this), msgr(this),
    timeout(0),
    cluster_protocol(0)
  {
    // for local dmsg delivery
    dispatch_queue.local_pipe = new Pipe(this, Pipe::STATE_OPEN);
    init_local_pipe();
  }
  virtual ~SimpleMessenger() {
    delete dispatch_queue.local_pipe;
  }

  int bind(entity_addr_t bind_addr);
  virtual int start();
  virtual void wait();

  int rebind(int avoid_port);

  __u32 get_global_seq(__u32 old=0) {
    Mutex::Locker l(global_seq_lock);
    if (old > global_seq)
      global_seq = old;
    return ++global_seq;
  }

  AuthAuthorizer *get_authorizer(int peer_type, bool force_new);
  bool verify_authorizer(Connection *con, int peer_type, int protocol, bufferlist& auth, bufferlist& auth_reply,
			 bool& isvalid);

  void submit_message(Message *m, const entity_addr_t& addr, int dest_type, bool lazy);
  void submit_message(Message *m, Pipe *pipe);
		      
  virtual int send_keepalive(const entity_inst_t& addr);
  virtual int send_keepalive(Connection *con);

  void learned_addr(const entity_addr_t& peer_addr_for_me);
  void init_local_pipe();

} ;

#endif
