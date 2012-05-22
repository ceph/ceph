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
 */

class SimpleMessenger : public Messenger {
  // First we have the public Messenger interface implementation...
public:
  /**
   * Initialize the SimpleMessenger!
   *
   * @param cct The CephContext to use
   * @param name The name to assign ourselves
   * _nonce A unique ID to use for this SimpleMessenger. It should not
   * be a value that will be repeated if the daemon restarts.
   */
  SimpleMessenger(CephContext *cct, entity_name_t name,
		  string mname, uint64_t _nonce) :
    Messenger(cct, name),
    accepter(this),
    reaper_thread(this),
    dispatch_thread(this),
    my_type(name.type()),
    nonce(_nonce),
    lock("SimpleMessenger::lock"), need_addr(true), did_bind(false),
    global_seq(0),
    destination_stopped(false),
    cluster_protocol(0),
    dispatch_throttler(cct, string("msgr_dispatch_throttler-") + mname, cct->_conf->ms_dispatch_throttle_bytes),
    reaper_started(false), reaper_stop(false),
    timeout(0),
    msgr(this)
  {
    pthread_spin_init(&global_seq_lock, PTHREAD_PROCESS_PRIVATE);
    // for local dmsg delivery
    dispatch_queue.local_pipe = new Pipe(this, Pipe::STATE_OPEN, NULL);
    init_local_pipe();
  }
  /**
   * Destroy the SimpleMessenger. Pretty simple since all the work is done
   * elsewhere.
   */
  virtual ~SimpleMessenger() {
    assert(destination_stopped); // we've been marked as stopped
    assert(!did_bind); // either we didn't bind or we shut down the Accepter
    assert(rank_pipe.empty()); // we don't have any running Pipes.
    assert(reaper_stop && !reaper_started); // the reaper thread is stopped
    delete dispatch_queue.local_pipe;
  }
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
   * Get the number of Messages which the SimpleMessenger has received
   * but not yet dispatched.
   * @return The length of the Dispatch queue.
   */
  int get_dispatch_queue_len() {
    return dispatch_queue.get_queue_len();
  }
  /** @} Accessors */

  /**
   * @defgroup Configuration functions
   * @{
   */
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
    assert(policy_map.count(type));
    policy_map[type].throttler = t;
  }
  /**
   * Bind the SimpleMessenger to a specific address. If bind_addr
   * is not completely filled in the system will use the
   * valid portions and cycle through the unset ones (eg, the port)
   * in an unspecified order.
   *
   * @param bind_addr The address to bind to.
   * @return 0 on success, or -1 if the SimpleMessenger is already running, or
   * -errno if an error is returned from a system call.
   */
  int bind(entity_addr_t bind_addr);
  /**
   * This function performs a full restart of the SimpleMessenger. It
   * calls mark_down_all() and binds to a new port. (If avoid_port
   * is set it additionally avoids that specific port.)
   *
   * @param avoid_port An additional port to avoid binding to.
   */
  int rebind(int avoid_port);
  /** @} Configuration functions */

  /**
   * @defgroup Startup/Shutdown
   * @{
   */
  /**
   * Start up the SimpleMessenger. Create worker threads as necessary.
   * @return 0
   */
  virtual int start();
  /**
   * Wait until the SimpleMessenger is ready to shut down (triggered by a
   * call to the shutdown() function), then handle
   * stopping its threads and cleaning up Pipes and various queues.
   * Once this function returns, the SimpleMessenger is fully shut down and
   * can be deleted.
   */
  virtual void wait();
  /**
   * Tell the SimpleMessenger to shut down. This function does not
   * complete the shutdown; it just triggers it.
   *
   * @return 0
   */
  virtual int shutdown();

  /** @} // Startup/Shutdown */

  /**
   * @defgroup Messaging
   * @{
   */
  /**
   * Queue the given Message for the given entity.
   * Success in this function does not guarantee Message delivery, only
   * success in queueing the Message. Other guarantees may be provided based
   * on the Connection policy associated with the dest.
   *
   * @param m The Message to send. The Messenger consumes a single reference
   * when you pass it in.
   * @param dest The entity to send the Message to.
   *
   * @return 0 on success, or -EINVAL if the dest's address is empty.
   */
  virtual int send_message(Message *m, const entity_inst_t& dest) {
    return _send_message(m, dest, false);
  }
  /**
   * Queue the given Message to send out on the given Connection.
   * Success in this function does not guarantee Message delivery, only
   * success in queueing the Message (or else a guaranteed-safe drop).
   * Other guarantees may be provided based on the Connection policy.
   *
   * @param m The Message to send. The Messenger consumes a single reference
   * when you pass it in.
   * @param con The Connection to send the Message out on.
   *
   * @return 0 on success.
   */
  virtual int send_message(Message *m, Connection *con) {
    return _send_message(m, con, false);
  }
  /**
   * Lazily queue the given Message for the given entity. Unlike with
   * send_message(), lazy_send_message() will not establish a
   * Connection if none exists, re-establish the connection if it
   * has broken, or queue the Message if the connection is broken.
   *
   * @param m The Message to send. The Messenger consumes a single reference
   * when you pass it in.
   * @param dest The entity to send the Message to.
   *
   * @return 0 on success, or -EINVAL if the dest's address is empty.
   */
  virtual int lazy_send_message(Message *m, const entity_inst_t& dest) {
    return _send_message(m, dest, true);
  }
  /**
   * Lazily queue the given Message for the given Connection.
   *
   * @param m The Message to send. The Messenger consumes a single reference
   * when you pass it in.
   * @param con The Connection to send the Message out on.
   *
   * @return 0.
   */
  virtual int lazy_send_message(Message *m, Connection *con) {
    return _send_message(m, con, true);
  }
  /** @} // Messaging */

  /**
   * @defgroup Connection Management
   * @{
   */
  /**
   * Get the Connection object associated with a given entity. If a
   * Connection does not exist, create one and establish a logical connection.
   * The caller owns a reference when this returns. Call ->put() when you're
   * done!
   *
   * @param dest The entity to get a connection for.
   * @return The requested Connection, as a pointer whose reference you own.
   */
  virtual Connection *get_connection(const entity_inst_t& dest);
  /**
   * Send a "keepalive" ping to the given dest, if it has a working Connection.
   * If the Messenger doesn't already have a Connection, or if the underlying
   * connection has broken, this function does nothing.
   *
   * @param dest The entity to send the keepalive to.
   * @return 0, or -EINVAL if we don't already have a Connection, or
   * -EPIPE if a Pipe for the dest doesn't exist.
   */
  virtual int send_keepalive(const entity_inst_t& addr);
  /**
   * Send a "keepalive" ping along the given Connection, if it's working.
   * If the underlying connection has broken, this function does nothing.
   *
   * @param dest The entity to send the keepalive to.
   * @return 0, or -EPIPE if the Connection doesn't have a running Pipe.
   */
  virtual int send_keepalive(Connection *con);
  /**
   * Mark down a Connection to a remote. This will cause us to
   * discard our outgoing queue for them, and if they try
   * to reconnect they will discard their queue when we
   * inform them of the session reset. If there is no
   * Connection to the given dest, it is a no-op.
   * It does not generate any notifications to the Dispatcher.
   *
   * @param a The address to mark down.
   */
  virtual void mark_down(const entity_addr_t& addr);
  /**
   * Mark down the given Connection. This will cause us to
   * discard its outgoing queue, and if the endpoint tries
   * to reconnect they will discard their queue when we
   * inform them of the session reset.
   * It does not generate any notifications to the Dispatcher.
   *
   * @param con The Connection to mark down.
   */
  virtual void mark_down(Connection *con);
  /**
   * Unlike mark_down, this function will try and deliver
   * all messages before ending the connection, and it will use
   * the Pipe's existing semantics to do so. Once the Messages
   * all been sent out (and acked, if using reliable delivery)
   * the Connection will be closed.
   * This function means that you will get standard delivery to endpoints,
   * and then the Connection will be cleaned up. It does not
   * generate any notifications to the Dispatcher.
   *
   * @param con The Connection to mark down.
   */
  virtual void mark_down_on_empty(Connection *con);
  /**
   * Mark a Connection as "disposable", setting it to lossy
   * (regardless of initial Policy). Unlike mark_down_on_empty()
   * this does not immediately close the Connection once
   * Messages have been delivered, so as long as there are no errors you can
   * continue to receive responses; but it will not attempt
   * to reconnect for message delivery or preserve your old
   * delivery semantics, either.
   * You can compose this with mark_down, in which case the Pipe
   * will make sure to send all Messages and wait for an ack before
   * closing, but if there's a failure it will simply shut down. It
   * does not generate any notifications to the Dispatcher.
   *
   * @param con The Connection to mark as disposable.
   */
  virtual void mark_disposable(Connection *con);
  /**
   * Mark all the existing Connections down. This is equivalent
   * to iterating over all Connections and calling mark_down()
   * on each.
   */
  virtual void mark_down_all();
  /** @} // Connection Management */
protected:
  /**
   * @defgroup Messenger Interfaces
   * @{
   */
  /**
   * Start up the DispatchQueue thread once we have somebody to dispatch to.
   */
  virtual void ready();
  /** @} // Messenger Interfaces */
private:
  /**
   * @defgroup Inner classes
   * @{
   */
  /**
   * If the SimpleMessenger binds to a specific address, the Accepter runs
   * and listens for incoming connections.
   */
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

  /**
   * The Pipe is the most complex SimpleMessenger component. It gets
   * two threads, one each for reading and writing on a socket it's handed
   * at creation time, and is responsible for everything that happens on
   * that socket. Besides message transmission, it's responsible for
   * propagating socket errors to the SimpleMessenger and then sticking
   * around in a state where it can provide enough data for the SimpleMessenger
   * to provide reliable Message delivery when it manages to reconnect.
   */
  class Pipe : public RefCountedObject {
    /**
     * The Reader thread handles all reads off the socket -- not just
     * Messages, but also acks and other protocol bits (excepting startup,
     * when the Writer does a couple of reads).
     * All the work is implemented in Pipe itself, of course.
     */
    class Reader : public Thread {
      Pipe *pipe;
    public:
      Reader(Pipe *p) : pipe(p) {}
      void *entry() { pipe->reader(); return 0; }
    } reader_thread;
    friend class Reader;

    /**
     * The Writer thread handles all writes to the socket (after startup).
     * All the work is implemented in Pipe itself, of course.
     */
    class Writer : public Thread {
      Pipe *pipe;
    public:
      Writer(Pipe *p) : pipe(p) {}
      void *entry() { pipe->writer(); return 0; }
    } writer_thread;
    friend class Writer;

  public:
    Pipe(SimpleMessenger *r, int st, Connection *con) :
      reader_thread(this), writer_thread(this),
      msgr(r),
      sd(-1),
      peer_type(-1),
      pipe_lock("SimpleMessenger::Pipe::pipe_lock"),
      state(st),
      connection_state(new Connection),
      reader_running(false), reader_joining(false), writer_running(false),
      in_qlen(0), keepalive(false), halt_delivery(false),
      close_on_empty(false),
      connect_seq(0), peer_global_seq(0),
      out_seq(0), in_seq(0), in_seq_acked(0) {
      if (con) {
        connection_state = con->get();
        connection_state->reset_pipe(this);
      } else {
        connection_state = new Connection();
        connection_state->pipe = get();
      }
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
        lsubdout(msgr->cct, ms, 10) << "reader got last ack, queue empty, closing" << dendl;
        stop();
      }
    }

    public:
    Pipe(const Pipe& other);
    const Pipe& operator=(const Pipe& other);

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

  /**
   * The DispatchQueue contains all the Pipes which have Messages
   * they want to be dispatched, carefully organized by Message priority
   * and permitted to deliver in a round-robin fashion.
   * See SimpleMessenger::dispatch_entry for details.
   */
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

  /**
   * A thread used to tear down Pipes when they're complete.
   */
  class ReaperThread : public Thread {
    SimpleMessenger *msgr;
  public:
    ReaperThread(SimpleMessenger *m) : msgr(m) {}
    void *entry() {
      msgr->reaper_entry();
      return 0;
    }
  } reaper_thread;

  /**
   * The DispatchThread runs dispatch_entry to empty out the dispatch_queue.
   */
  class DispatchThread : public Thread {
    SimpleMessenger *msgr;
  public:
    DispatchThread(SimpleMessenger *_messenger) : msgr(_messenger) {}
    void *entry() {
      msgr->dispatch_entry();
      return 0;
    }
  } dispatch_thread;

  /**
   * @} // Inner classes
   */

  /**
   * @defgroup Utility functions
   * @{
   */
  /**
   * Create a Pipe associated with the given entity (of the given type).
   * Initiate the connection. (This function returning does not guarantee
   * connection success.)
   *
   * @param addr The address of the entity to connect to.
   * @param type The peer type of the entity at the address.
   * @param con An existing Connection to associate with the new Pipe. If
   * NULL, it creates a new Connection.
   *
   * @return a pointer to the newly-created Pipe. Caller does not own a
   * reference; take one if you need it.
   */
  Pipe *connect_rank(const entity_addr_t& addr, int type, Connection *con);
  /**
   * Send a message, lazily or not.
   * This just glues [lazy_]send_message together and passes
   * the input on to submit_message.
   */
  int _send_message(Message *m, const entity_inst_t& dest, bool lazy);
  /**
   * Same as above, but for the Connection-based variants.
   */
  int _send_message(Message *m, Connection *con, bool lazy);
  /**
   * Queue up a Message for delivery to the entity specified
   * by addr and dest_type.
   * submit_message() is responsible for creating
   * new Pipes (and closing old ones) as necessary.
   *
   * @param m The Message to queue up. This function eats a reference.
   * @param con The existing Connection to use, or NULL if you don't know of one.
   * @param addr The address to send the Message to.
   * @param dest_type The peer type of the address we're sending to
   * @param lazy If true, do not establish or fix a Connection to send the Message;
   * just drop silently under failure.
   */
  void submit_message(Message *m, Connection *con,
                      const entity_addr_t& addr, int dest_type, bool lazy);
  /**
   * Look through the pipes in the pipe_reap_queue and tear them down.
   */
  void reaper();
  /**
   * @} // Utility functions
   */

  // SimpleMessenger stuff
  /// the peer type of our endpoint
  int my_type;
  /// approximately unique ID set by the Constructor for use in entity_addr_t
  uint64_t nonce;
  /// overall lock used for SimpleMessenger data structures
  Mutex lock;
  /// true, specifying we haven't learned our addr; set false when we find it.
  // maybe this should be protected by the lock?
  bool need_addr;
  /**
   *  false; set to true if the SimpleMessenger bound to a specific address;
   *  and set false again by Accepter::stop(). This isn't lock-protected
   *  since you shouldn't be able to race the only writers.
   */
  bool did_bind;
  /// counter for the global seq our connection protocol uses
  __u32 global_seq;
  /// lock to protect the global_seq
  pthread_spinlock_t global_seq_lock;

  /// flag set true when all the threads need to shut down
  bool destination_stopped;

  /// hash map of addresses to Pipes
  hash_map<entity_addr_t, Pipe*> rank_pipe;
  /// a set of all the Pipes we have which are somehow active
  set<Pipe*>      pipes;
  /// a list of Pipes we want to tear down
  list<Pipe*>     pipe_reap_queue;

  /// internal cluster protocol version, if any, for talking to entities of the same type.
  int cluster_protocol;
  /// the default Policy we use for Pipes
  Policy default_policy;
  /// map specifying different Policies for specific peer types
  map<int, Policy> policy_map; // entity_name_t::type -> Policy

  /// Throttle preventing us from building up a big backlog waiting for dispatch
  Throttle dispatch_throttler;

  bool reaper_started, reaper_stop;
  Cond reaper_cond;

  /// This Cond is slept on by wait() and signaled by dispatch_entry()
  Cond  wait_cond;

  int timeout;

  SimpleMessenger *msgr; //hack to make dout macro work, will fix

public:
  /**
   * @defgroup SimpleMessenger internals
   * @{
   */

  /**
   * This wraps ms_deliver_get_authorizer. We use it for Pipe.
   */
  AuthAuthorizer *get_authorizer(int peer_type, bool force_new);
  /**
   * This wraps ms_deliver_verify_authorizer; we use it for Pipe.
   */
  bool verify_authorizer(Connection *con, int peer_type, int protocol, bufferlist& auth, bufferlist& auth_reply,
                         bool& isvalid);
  /**
   * Increment the global sequence for this SimpleMessenger and return it.
   * This is for the connect protocol, although it doesn't hurt if somebody
   * else calls it.
   *
   * @return a global sequence ID that nobody else has seen.
   */
  __u32 get_global_seq(__u32 old=0) {
    pthread_spin_lock(&global_seq_lock);
    if (old > global_seq)
      global_seq = old;
    __u32 ret = ++global_seq;
    pthread_spin_unlock(&global_seq_lock);
    return ret;
  }
  /**
   * Get the protocol version we support for the given peer type: either
   * a peer protocol (if it matches our own), the protocol version for the
   * peer (if we're connecting), or our protocol version (if we're accepting).
   */
  int get_proto_version(int peer_type, bool connect);

  /**
   * Fill in the address and peer type for the local pipe, which
   * is used for delivering messages back to ourself (whether actual Messages
   * submitted by the endpoint, or interrupts we use to process things like
   * dead Pipes in a fair order).
   */
  void init_local_pipe();  /**
   * Tell the SimpleMessenger its full IP address.
   *
   * This is used by Pipes when connecting to other endpoints, and
   * probably shouldn't be called by anybody else.
   */
  void learned_addr(const entity_addr_t& peer_addr_for_me);

  /**
   * Get the Policy associated with a type of peer.
   * @param t The peer type to get the default policy for.
   *
   * @return A const Policy reference.
   */
  const Policy& get_policy(int t) {
    if (policy_map.count(t))
      return policy_map[t];
    else
      return default_policy;
  }

  /**
   * This function is used by the dispatch thread. It runs continuously
   * until dispatch_queue.stop is set to true, choosing what order the Pipes
   * get to deliver in, and sending out their chosen Message via the
   * ms_deliver_* functions.
   * It should really only by dispatch_thread calling this, in our
   * current implementation.
   */
  void dispatch_entry();
  /**
   * Release memory accounting back to the dispatch throttler.
   *
   * @param msize The amount of memory to release.
   */
  void dispatch_throttle_release(uint64_t msize);

  /**
   * This function is used by the reaper thread. As long as nobody
   * has set reaper_stop, it calls the reaper function, then
   * waits to be signaled when it needs to reap again (or when it needs
   * to stop).
   */
  void reaper_entry();
  /**
   * Add a pipe to the pipe_reap_queue, to be torn down on
   * the next call to reaper().
   * It should really only be the Pipe calling this, in our current
   * implementation.
   *
   * @param pipe A Pipe which has stopped its threads and is
   * ready to be torn down.
   */
  void queue_reap(Pipe *pipe);
  /**
   * @} // SimpleMessenger Internals
   */
} ;

#endif
