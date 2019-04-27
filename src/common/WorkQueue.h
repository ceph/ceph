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

#ifndef CEPH_WORKQUEUE_H
#define CEPH_WORKQUEUE_H

#ifdef WITH_SEASTAR
// for ObjectStore.h
struct ThreadPool {
  struct TPHandle {
  };
};

#else

#include <atomic>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "common/ceph_mutex.h"
#include "include/unordered_map.h"
#include "common/config_obs.h"
#include "common/HeartbeatMap.h"
#include "common/Thread.h"
#include "include/Context.h"

class CephContext;

/// Pool of threads that share work submitted to multiple work queues.
class ThreadPool : public md_config_obs_t {
  CephContext *cct;
  std::string name;
  std::string thread_name;
  std::string lockname;
  ceph::mutex _lock;
  ceph::condition_variable _cond;
  bool _stop;
  int _pause;
  int _draining;
  ceph::condition_variable _wait_cond;

public:
  class TPHandle {
    friend class ThreadPool;
    CephContext *cct;
    ceph::heartbeat_handle_d *hb;
    ceph::coarse_mono_clock::rep grace;
    ceph::coarse_mono_clock::rep suicide_grace;
  public:
    TPHandle(
      CephContext *cct,
      ceph::heartbeat_handle_d *hb,
      time_t grace,
      time_t suicide_grace)
      : cct(cct), hb(hb), grace(grace), suicide_grace(suicide_grace) {}
    void reset_tp_timeout();
    void suspend_tp_timeout();
  };
private:

  /// Basic interface to a work queue used by the worker threads.
  struct WorkQueue_ {
    std::string name;
    time_t timeout_interval, suicide_interval;
    WorkQueue_(std::string n, time_t ti, time_t sti)
      : name(std::move(n)), timeout_interval(ti), suicide_interval(sti)
    { }
    virtual ~WorkQueue_() {}
    /// Remove all work items from the queue.
    virtual void _clear() = 0;
    /// Check whether there is anything to do.
    virtual bool _empty() = 0;
    /// Get the next work item to process.
    virtual void *_void_dequeue() = 0;
    /** @brief Process the work item.
     * This function will be called several times in parallel
     * and must therefore be thread-safe. */
    virtual void _void_process(void *item, TPHandle &handle) = 0;
    /** @brief Synchronously finish processing a work item.
     * This function is called after _void_process with the global thread pool lock held,
     * so at most one copy will execute simultaneously for a given thread pool.
     * It can be used for non-thread-safe finalization. */
    virtual void _void_process_finish(void *) = 0;
  };

  // track thread pool size changes
  unsigned _num_threads;
  std::string _thread_num_option;
  const char **_conf_keys;

  const char **get_tracked_conf_keys() const override {
    return _conf_keys;
  }
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set <std::string> &changed) override;

public:
  /** @brief Work queue that processes several submitted items at once.
   * The queue will automatically add itself to the thread pool on construction
   * and remove itself on destruction. */
  template<class T>
  class BatchWorkQueue : public WorkQueue_ {
    ThreadPool *pool;

    virtual bool _enqueue(T *) = 0;
    virtual void _dequeue(T *) = 0;
    virtual void _dequeue(std::list<T*> *) = 0;
    virtual void _process_finish(const std::list<T*> &) {}

    // virtual methods from WorkQueue_ below
    void *_void_dequeue() override {
      std::list<T*> *out(new std::list<T*>);
      _dequeue(out);
      if (!out->empty()) {
	return (void *)out;
      } else {
	delete out;
	return 0;
      }
    }
    void _void_process(void *p, TPHandle &handle) override {
      _process(*((std::list<T*>*)p), handle);
    }
    void _void_process_finish(void *p) override {
      _process_finish(*(std::list<T*>*)p);
      delete (std::list<T*> *)p;
    }

  protected:
    virtual void _process(const std::list<T*> &items, TPHandle &handle) = 0;

  public:
    BatchWorkQueue(std::string n, time_t ti, time_t sti, ThreadPool* p)
      : WorkQueue_(std::move(n), ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~BatchWorkQueue() override {
      pool->remove_work_queue(this);
    }

    bool queue(T *item) {
      pool->_lock.lock();
      bool r = _enqueue(item);
      pool->_cond.notify_one();
      pool->_lock.unlock();
      return r;
    }
    void dequeue(T *item) {
      pool->_lock.lock();
      _dequeue(item);
      pool->_lock.unlock();
    }
    void clear() {
      pool->_lock.lock();
      _clear();
      pool->_lock.unlock();
    }

    void lock() {
      pool->lock();
    }
    void unlock() {
      pool->unlock();
    }
    void wake() {
      pool->wake();
    }
    void _wake() {
      pool->_wake();
    }
    void drain() {
      pool->drain(this);
    }

  };

  /** @brief Templated by-value work queue.
   * Skeleton implementation of a queue that processes items submitted by value.
   * This is useful if the items are single primitive values or very small objects
   * (a few bytes). The queue will automatically add itself to the thread pool on
   * construction and remove itself on destruction. */
  template<typename T, typename U = T>
  class WorkQueueVal : public WorkQueue_ {
    ceph::mutex _lock = ceph::make_mutex("WorkQueueVal::_lock");
    ThreadPool *pool;
    std::list<U> to_process;
    std::list<U> to_finish;
    virtual void _enqueue(T) = 0;
    virtual void _enqueue_front(T) = 0;
    bool _empty() override = 0;
    virtual U _dequeue() = 0;
    virtual void _process_finish(U) {}

    void *_void_dequeue() override {
      {
	std::lock_guard l(_lock);
	if (_empty())
	  return 0;
	U u = _dequeue();
	to_process.push_back(u);
      }
      return ((void*)1); // Not used
    }
    void _void_process(void *, TPHandle &handle) override {
      _lock.lock();
      ceph_assert(!to_process.empty());
      U u = to_process.front();
      to_process.pop_front();
      _lock.unlock();

      _process(u, handle);

      _lock.lock();
      to_finish.push_back(u);
      _lock.unlock();
    }

    void _void_process_finish(void *) override {
      _lock.lock();
      ceph_assert(!to_finish.empty());
      U u = to_finish.front();
      to_finish.pop_front();
      _lock.unlock();

      _process_finish(u);
    }

    void _clear() override {}

  public:
    WorkQueueVal(std::string n, time_t ti, time_t sti, ThreadPool *p)
      : WorkQueue_(std::move(n), ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~WorkQueueVal() override {
      pool->remove_work_queue(this);
    }
    void queue(T item) {
      std::lock_guard l(pool->_lock);
      _enqueue(item);
      pool->_cond.notify_one();
    }
    void queue_front(T item) {
      std::lock_guard l(pool->_lock);
      _enqueue_front(item);
      pool->_cond.notify_one();
    }
    void drain() {
      pool->drain(this);
    }
  protected:
    void lock() {
      pool->lock();
    }
    void unlock() {
      pool->unlock();
    }
    virtual void _process(U u, TPHandle &) = 0;
  };

  /** @brief Template by-pointer work queue.
   * Skeleton implementation of a queue that processes items of a given type submitted as pointers.
   * This is useful when the work item are large or include dynamically allocated memory. The queue
   * will automatically add itself to the thread pool on construction and remove itself on
   * destruction. */
  template<class T>
  class WorkQueue : public WorkQueue_ {
    ThreadPool *pool;
    
    /// Add a work item to the queue.
    virtual bool _enqueue(T *) = 0;
    /// Dequeue a previously submitted work item.
    virtual void _dequeue(T *) = 0;
    /// Dequeue a work item and return the original submitted pointer.
    virtual T *_dequeue() = 0;
    virtual void _process_finish(T *) {}

    // implementation of virtual methods from WorkQueue_
    void *_void_dequeue() override {
      return (void *)_dequeue();
    }
    void _void_process(void *p, TPHandle &handle) override {
      _process(static_cast<T *>(p), handle);
    }
    void _void_process_finish(void *p) override {
      _process_finish(static_cast<T *>(p));
    }

  protected:
    /// Process a work item. Called from the worker threads.
    virtual void _process(T *t, TPHandle &) = 0;

  public:
    WorkQueue(std::string n, time_t ti, time_t sti, ThreadPool* p)
      : WorkQueue_(std::move(n), ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~WorkQueue() override {
      pool->remove_work_queue(this);
    }
    
    bool queue(T *item) {
      pool->_lock.lock();
      bool r = _enqueue(item);
      pool->_cond.notify_one();
      pool->_lock.unlock();
      return r;
    }
    void dequeue(T *item) {
      pool->_lock.lock();
      _dequeue(item);
      pool->_lock.unlock();
    }
    void clear() {
      pool->_lock.lock();
      _clear();
      pool->_lock.unlock();
    }

    void lock() {
      pool->lock();
    }
    void unlock() {
      pool->unlock();
    }
    /// wake up the thread pool (without lock held)
    void wake() {
      pool->wake();
    }
    /// wake up the thread pool (with lock already held)
    void _wake() {
      pool->_wake();
    }
    void _wait() {
      pool->_wait();
    }
    void drain() {
      pool->drain(this);
    }

  };

  template<typename T>
  class PointerWQ : public WorkQueue_ {
  public:
    ~PointerWQ() override {
      m_pool->remove_work_queue(this);
      ceph_assert(m_processing == 0);
    }
    void drain() {
      {
        // if this queue is empty and not processing, don't wait for other
        // queues to finish processing
        std::lock_guard l(m_pool->_lock);
        if (m_processing == 0 && m_items.empty()) {
          return;
        }
      }
      m_pool->drain(this);
    }
    void queue(T *item) {
      std::lock_guard l(m_pool->_lock);
      m_items.push_back(item);
      m_pool->_cond.notify_one();
    }
    bool empty() {
      std::lock_guard l(m_pool->_lock);
      return _empty();
    }
  protected:
    PointerWQ(std::string n, time_t ti, time_t sti, ThreadPool* p)
      : WorkQueue_(std::move(n), ti, sti), m_pool(p), m_processing(0) {
    }
    void register_work_queue() {
      m_pool->add_work_queue(this);
    }
    void _clear() override {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      m_items.clear();
    }
    bool _empty() override {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      return m_items.empty();
    }
    void *_void_dequeue() override {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      if (m_items.empty()) {
        return NULL;
      }

      ++m_processing;
      T *item = m_items.front();
      m_items.pop_front();
      return item;
    }
    void _void_process(void *item, ThreadPool::TPHandle &handle) override {
      process(reinterpret_cast<T *>(item));
    }
    void _void_process_finish(void *item) override {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      ceph_assert(m_processing > 0);
      --m_processing;
    }

    virtual void process(T *item) = 0;
    void process_finish() {
      std::lock_guard locker(m_pool->_lock);
      _void_process_finish(nullptr);
    }

    T *front() {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      if (m_items.empty()) {
        return NULL;
      }
      return m_items.front();
    }
    void requeue_front(T *item) {
      std::lock_guard pool_locker(m_pool->_lock);
      _void_process_finish(nullptr);
      m_items.push_front(item);
    }
    void requeue_back(T *item) {
      std::lock_guard pool_locker(m_pool->_lock);
      _void_process_finish(nullptr);
      m_items.push_back(item);
    }
    void signal() {
      std::lock_guard pool_locker(m_pool->_lock);
      m_pool->_cond.notify_one();
    }
    ceph::mutex &get_pool_lock() {
      return m_pool->_lock;
    }
  private:
    ThreadPool *m_pool;
    std::list<T *> m_items;
    uint32_t m_processing;
  };
private:
  std::vector<WorkQueue_*> work_queues;
  int next_work_queue = 0;
 

  // threads
  struct WorkThread : public Thread {
    ThreadPool *pool;
    // cppcheck-suppress noExplicitConstructor
    WorkThread(ThreadPool *p) : pool(p) {}
    void *entry() override {
      pool->worker(this);
      return 0;
    }
  };
  
  std::set<WorkThread*> _threads;
  std::list<WorkThread*> _old_threads;  ///< need to be joined
  int processing;

  void start_threads();
  void join_old_threads();
  void worker(WorkThread *wt);

public:
  ThreadPool(CephContext *cct_, std::string nm, std::string tn, int n, const char *option = NULL);
  ~ThreadPool() override;

  /// return number of threads currently running
  int get_num_threads() {
    std::lock_guard l(_lock);
    return _num_threads;
  }
  
  /// assign a work queue to this thread pool
  void add_work_queue(WorkQueue_* wq) {
    std::lock_guard l(_lock);
    work_queues.push_back(wq);
  }
  /// remove a work queue from this thread pool
  void remove_work_queue(WorkQueue_* wq) {
    std::lock_guard l(_lock);
    unsigned i = 0;
    while (work_queues[i] != wq)
      i++;
    for (i++; i < work_queues.size(); i++) 
      work_queues[i-1] = work_queues[i];
    ceph_assert(i == work_queues.size());
    work_queues.resize(i-1);
  }

  /// take thread pool lock
  void lock() {
    _lock.lock();
  }
  /// release thread pool lock
  void unlock() {
    _lock.unlock();
  }

  /// wait for a kick on this thread pool
  void wait(ceph::condition_variable &c) {
    std::unique_lock l(_lock, std::adopt_lock);
    c.wait(l);
  }

  /// wake up a waiter (with lock already held)
  void _wake() {
    _cond.notify_all();
  }
  /// wake up a waiter (without lock held)
  void wake() {
    std::lock_guard l(_lock);
    _cond.notify_all();
  }
  void _wait() {
    std::unique_lock l(_lock, std::adopt_lock);
    _cond.wait(l);
  }

  /// start thread pool thread
  void start();
  /// stop thread pool thread
  void stop(bool clear_after=true);
  /// pause thread pool (if it not already paused)
  void pause();
  /// pause initiation of new work
  void pause_new();
  /// resume work in thread pool.  must match each pause() call 1:1 to resume.
  void unpause();
  /** @brief Wait until work completes.
   * If the parameter is NULL, blocks until all threads are idle.
   * If it is not NULL, blocks until the given work queue does not have
   * any items left to process. */
  void drain(WorkQueue_* wq = 0);
};

class GenContextWQ :
  public ThreadPool::WorkQueueVal<GenContext<ThreadPool::TPHandle&>*> {
  std::list<GenContext<ThreadPool::TPHandle&>*> _queue;
public:
  GenContextWQ(const std::string &name, time_t ti, ThreadPool *tp)
    : ThreadPool::WorkQueueVal<
      GenContext<ThreadPool::TPHandle&>*>(name, ti, ti*10, tp) {}
  
  void _enqueue(GenContext<ThreadPool::TPHandle&> *c) override {
    _queue.push_back(c);
  }
  void _enqueue_front(GenContext<ThreadPool::TPHandle&> *c) override {
    _queue.push_front(c);
  }
  bool _empty() override {
    return _queue.empty();
  }
  GenContext<ThreadPool::TPHandle&> *_dequeue() override {
    ceph_assert(!_queue.empty());
    GenContext<ThreadPool::TPHandle&> *c = _queue.front();
    _queue.pop_front();
    return c;
  }
  void _process(GenContext<ThreadPool::TPHandle&> *c,
		ThreadPool::TPHandle &tp) override {
    c->complete(tp);
  }
};

class C_QueueInWQ : public Context {
  GenContextWQ *wq;
  GenContext<ThreadPool::TPHandle&> *c;
public:
  C_QueueInWQ(GenContextWQ *wq, GenContext<ThreadPool::TPHandle &> *c)
    : wq(wq), c(c) {}
  void finish(int) override {
    wq->queue(c);
  }
};

/// Work queue that asynchronously completes contexts (executes callbacks).
/// @see Finisher
class ContextWQ : public ThreadPool::PointerWQ<Context> {
public:
  ContextWQ(const std::string &name, time_t ti, ThreadPool *tp)
    : ThreadPool::PointerWQ<Context>(name, ti, 0, tp) {
    this->register_work_queue();
  }

  void queue(Context *ctx, int result = 0) {
    if (result != 0) {
      std::lock_guard locker(m_lock);
      m_context_results[ctx] = result;
    }
    ThreadPool::PointerWQ<Context>::queue(ctx);
  }
protected:
  void _clear() override {
    ThreadPool::PointerWQ<Context>::_clear();

    std::lock_guard locker(m_lock);
    m_context_results.clear();
  }

  void process(Context *ctx) override {
    int result = 0;
    {
      std::lock_guard locker(m_lock);
      ceph::unordered_map<Context *, int>::iterator it =
        m_context_results.find(ctx);
      if (it != m_context_results.end()) {
        result = it->second;
        m_context_results.erase(it);
      }
    }
    ctx->complete(result);
  }
private:
  ceph::mutex m_lock = ceph::make_mutex("ContextWQ::m_lock");
  ceph::unordered_map<Context*, int> m_context_results;
};

class ShardedThreadPool {

  CephContext *cct;
  std::string name;
  std::string thread_name;
  std::string lockname;
  ceph::mutex shardedpool_lock;
  ceph::condition_variable shardedpool_cond;
  ceph::condition_variable wait_cond;
  uint32_t num_threads;

  std::atomic<bool> stop_threads = { false };
  std::atomic<bool> pause_threads = { false };
  std::atomic<bool> drain_threads = { false };

  uint32_t num_paused;
  uint32_t num_drained;

public:

  class BaseShardedWQ {
  
  public:
    time_t timeout_interval, suicide_interval;
    BaseShardedWQ(time_t ti, time_t sti):timeout_interval(ti), suicide_interval(sti) {}
    virtual ~BaseShardedWQ() {}

    virtual void _process(uint32_t thread_index, ceph::heartbeat_handle_d *hb ) = 0;
    virtual void return_waiting_threads() = 0;
    virtual void stop_return_waiting_threads() = 0;
    virtual bool is_shard_empty(uint32_t thread_index) = 0;
  };

  template <typename T>
  class ShardedWQ: public BaseShardedWQ {
  
    ShardedThreadPool* sharded_pool;

  protected:
    virtual void _enqueue(T&&) = 0;
    virtual void _enqueue_front(T&&) = 0;


  public:
    ShardedWQ(time_t ti, time_t sti, ShardedThreadPool* tp): BaseShardedWQ(ti, sti), 
                                                                 sharded_pool(tp) {
      tp->set_wq(this);
    }
    ~ShardedWQ() override {}

    void queue(T&& item) {
      _enqueue(std::move(item));
    }
    void queue_front(T&& item) {
      _enqueue_front(std::move(item));
    }
    void drain() {
      sharded_pool->drain();
    }
    
  };

private:

  BaseShardedWQ* wq;
  // threads
  struct WorkThreadSharded : public Thread {
    ShardedThreadPool *pool;
    uint32_t thread_index;
    WorkThreadSharded(ShardedThreadPool *p, uint32_t pthread_index): pool(p),
      thread_index(pthread_index) {}
    void *entry() override {
      pool->shardedthreadpool_worker(thread_index);
      return 0;
    }
  };

  std::vector<WorkThreadSharded*> threads_shardedpool;
  void start_threads();
  void shardedthreadpool_worker(uint32_t thread_index);
  void set_wq(BaseShardedWQ* swq) {
    wq = swq;
  }



public:

  ShardedThreadPool(CephContext *cct_, std::string nm, std::string tn, uint32_t pnum_threads);

  ~ShardedThreadPool(){};

  /// start thread pool thread
  void start();
  /// stop thread pool thread
  void stop();
  /// pause thread pool (if it not already paused)
  void pause();
  /// pause initiation of new work
  void pause_new();
  /// resume work in thread pool.  must match each pause() call 1:1 to resume.
  void unpause();
  /// wait for all work to complete
  void drain();

};

#endif

#endif
