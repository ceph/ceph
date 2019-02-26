// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_COROUTINE_H
#define CEPH_RGW_COROUTINE_H

#ifdef _ASSERT_H
#define NEED_ASSERT_H
#pragma push_macro("_ASSERT_H")
#endif

#include <boost/asio.hpp>
#include <boost/intrusive_ptr.hpp>

#ifdef NEED_ASSERT_H
#pragma pop_macro("_ASSERT_H")
#endif

#include "include/utime.h"
#include "common/RefCountedObj.h"
#include "common/debug.h"
#include "common/Timer.h"
#include "common/admin_socket.h"

#include "rgw_common.h"
#include <boost/asio/coroutine.hpp>

#include <atomic>

#define RGW_ASYNC_OPS_MGR_WINDOW 100

class RGWCoroutinesStack;
class RGWCoroutinesManager;
class RGWAioCompletionNotifier;

class RGWCompletionManager : public RefCountedObject {
  friend class RGWCoroutinesManager;

  CephContext *cct;

  struct io_completion {
    rgw_io_id io_id;
    void *user_info;
  };
  list<io_completion> complete_reqs;
  set<rgw_io_id> complete_reqs_set;
  using NotifierRef = boost::intrusive_ptr<RGWAioCompletionNotifier>;
  set<NotifierRef> cns;

  Mutex lock;
  Cond cond;

  SafeTimer timer;

  std::atomic<bool> going_down = { false };

  map<void *, void *> waiters;

  class WaitContext;

protected:
  void _wakeup(void *opaque);
  void _complete(RGWAioCompletionNotifier *cn, const rgw_io_id& io_id, void *user_info);
public:
  explicit RGWCompletionManager(CephContext *_cct);
  ~RGWCompletionManager() override;

  void complete(RGWAioCompletionNotifier *cn, const rgw_io_id& io_id, void *user_info);
  int get_next(io_completion *io);
  bool try_get_next(io_completion *io);

  void go_down();

  /*
   * wait for interval length to complete user_info
   */
  void wait_interval(void *opaque, const utime_t& interval, void *user_info);
  void wakeup(void *opaque);

  void register_completion_notifier(RGWAioCompletionNotifier *cn);
  void unregister_completion_notifier(RGWAioCompletionNotifier *cn);
};

/* a single use librados aio completion notifier that hooks into the RGWCompletionManager */
class RGWAioCompletionNotifier : public RefCountedObject {
  librados::AioCompletion *c;
  RGWCompletionManager *completion_mgr;
  rgw_io_id io_id;
  void *user_data;
  Mutex lock;
  bool registered;

public:
  RGWAioCompletionNotifier(RGWCompletionManager *_mgr, const rgw_io_id& _io_id, void *_user_data);
  ~RGWAioCompletionNotifier() override {
    c->release();
    lock.Lock();
    bool need_unregister = registered;
    if (registered) {
      completion_mgr->get();
    }
    registered = false;
    lock.Unlock();
    if (need_unregister) {
      completion_mgr->unregister_completion_notifier(this);
      completion_mgr->put();
    }
  }

  librados::AioCompletion *completion() {
    return c;
  }

  void unregister() {
    Mutex::Locker l(lock);
    if (!registered) {
      return;
    }
    registered = false;
  }

  void cb() {
    lock.Lock();
    if (!registered) {
      lock.Unlock();
      put();
      return;
    }
    completion_mgr->get();
    registered = false;
    lock.Unlock();
    completion_mgr->complete(this, io_id, user_data);
    completion_mgr->put();
    put();
  }
};

// completion notifier with opaque payload (ie a reference-counted pointer)
template <typename T>
class RGWAioCompletionNotifierWith : public RGWAioCompletionNotifier {
  T value;
public:
  RGWAioCompletionNotifierWith(RGWCompletionManager *mgr,
                               const rgw_io_id& io_id, void *user_data,
                               T value)
    : RGWAioCompletionNotifier(mgr, io_id, user_data), value(std::move(value))
  {}
};

struct RGWCoroutinesEnv {
  uint64_t run_context;
  RGWCoroutinesManager *manager;
  list<RGWCoroutinesStack *> *scheduled_stacks;
  RGWCoroutinesStack *stack;

  RGWCoroutinesEnv() : run_context(0), manager(NULL), scheduled_stacks(NULL), stack(NULL) {}
};

enum RGWCoroutineState {
  RGWCoroutine_Error = -2,
  RGWCoroutine_Done  = -1,
  RGWCoroutine_Run   =  0,
};

struct rgw_spawned_stacks {
  vector<RGWCoroutinesStack *> entries;

  rgw_spawned_stacks() {}

  void add_pending(RGWCoroutinesStack *s) {
    entries.push_back(s);
  }

  void inherit(rgw_spawned_stacks *source) {
    for (vector<RGWCoroutinesStack *>::iterator iter = source->entries.begin();
         iter != source->entries.end(); ++iter) {
      add_pending(*iter);
    }
    source->entries.clear();
  }
};



class RGWCoroutine : public RefCountedObject, public boost::asio::coroutine {
  friend class RGWCoroutinesStack;

  struct StatusItem {
    utime_t timestamp;
    string status;

    StatusItem(utime_t& t, const string& s) : timestamp(t), status(s) {}

    void dump(Formatter *f) const;
  };

#define MAX_COROUTINE_HISTORY 10

  struct Status {
    CephContext *cct;
    RWLock lock;
    int max_history;

    utime_t timestamp;
    stringstream status;

    explicit Status(CephContext *_cct) : cct(_cct), lock("RGWCoroutine::Status::lock"), max_history(MAX_COROUTINE_HISTORY) {}

    deque<StatusItem> history;

    stringstream& set_status();
  } status;

  stringstream description;

protected:
  bool _yield_ret;
  boost::asio::coroutine drain_cr;

  CephContext *cct;

  RGWCoroutinesStack *stack;
  int retcode;
  int state;

  rgw_spawned_stacks spawned;

  stringstream error_stream;

  int set_state(int s, int ret = 0) {
    retcode = ret;
    state = s;
    return ret;
  }
  int set_cr_error(int ret) {
    return set_state(RGWCoroutine_Error, ret);
  }
  int set_cr_done() {
    return set_state(RGWCoroutine_Done, 0);
  }
  void set_io_blocked(bool flag);

  void reset_description() {
    description.str(string());
  }

  stringstream& set_description() {
    return description;
  }
  stringstream& set_status() {
    return status.set_status();
  }

  stringstream& set_status(const string& s) {
    stringstream& status = set_status();
    status << s;
    return status;
  }

  virtual int operate_wrapper() {
    return operate();
  }
public:
  RGWCoroutine(CephContext *_cct) : status(_cct), _yield_ret(false), cct(_cct), stack(NULL), retcode(0), state(RGWCoroutine_Run) {}
  ~RGWCoroutine() override;

  virtual int operate() = 0;

  bool is_done() { return (state == RGWCoroutine_Done || state == RGWCoroutine_Error); }
  bool is_error() { return (state == RGWCoroutine_Error); }

  stringstream& log_error() { return error_stream; }
  string error_str() {
    return error_stream.str();
  }

  void set_retcode(int r) {
    retcode = r;
  }

  int get_ret_status() {
    return retcode;
  }

  void call(RGWCoroutine *op); /* call at the same stack we're in */
  RGWCoroutinesStack *spawn(RGWCoroutine *op, bool wait); /* execute on a different stack */
  bool collect(int *ret, RGWCoroutinesStack *skip_stack); /* returns true if needs to be called again */
  bool collect_next(int *ret, RGWCoroutinesStack **collected_stack = NULL); /* returns true if found a stack to collect */

  int wait(const utime_t& interval);
  bool drain_children(int num_cr_left, RGWCoroutinesStack *skip_stack = NULL); /* returns true if needed to be called again */
  void wakeup();
  void set_sleeping(bool flag); /* put in sleep, or wakeup from sleep */

  size_t num_spawned() {
    return spawned.entries.size();
  }

  void wait_for_child();

  virtual string to_str() const;

  RGWCoroutinesStack *get_stack() const {
    return stack;
  }

  RGWCoroutinesEnv *get_env() const;

  void dump(Formatter *f) const;

  void init_new_io(RGWIOProvider *io_provider); /* only links the default io id */

  int io_block(int ret = 0) {
    return io_block(ret, -1);
  }
  int io_block(int ret, int64_t io_id);
  int io_block(int ret, const rgw_io_id& io_id);
  void io_complete() {
    io_complete(rgw_io_id{});
  }
  void io_complete(const rgw_io_id& io_id);
};

ostream& operator<<(ostream& out, const RGWCoroutine& cr);

#define yield_until_true(x)     \
do {                            \
  do {                          \
    yield _yield_ret = x;       \
  } while (!_yield_ret);        \
  _yield_ret = false;           \
} while (0)

#define drain_all() \
  drain_cr = boost::asio::coroutine(); \
  yield_until_true(drain_children(0))

#define drain_all_but(n) \
  drain_cr = boost::asio::coroutine(); \
  yield_until_true(drain_children(n))

#define drain_all_but_stack(stack) \
  drain_cr = boost::asio::coroutine(); \
  yield_until_true(drain_children(1, stack))

template <class T>
class RGWConsumerCR : public RGWCoroutine {
  list<T> product;

public:
  explicit RGWConsumerCR(CephContext *_cct) : RGWCoroutine(_cct) {}

  bool has_product() {
    return !product.empty();
  }

  void wait_for_product() {
    if (!has_product()) {
      set_sleeping(true);
    }
  }

  bool consume(T *p) {
    if (product.empty()) {
      return false;
    }
    *p = product.front();
    product.pop_front();
    return true;
  }

  void receive(const T& p, bool wakeup = true);
  void receive(list<T>& l, bool wakeup = true);
};

class RGWCoroutinesStack : public RefCountedObject {
  friend class RGWCoroutine;
  friend class RGWCoroutinesManager;

  CephContext *cct;

  RGWCoroutinesManager *ops_mgr;

  list<RGWCoroutine *> ops;
  list<RGWCoroutine *>::iterator pos;

  rgw_spawned_stacks spawned;

  set<RGWCoroutinesStack *> blocked_by_stack;
  set<RGWCoroutinesStack *> blocking_stacks;

  map<int64_t, rgw_io_id> io_finish_ids;
  rgw_io_id io_blocked_id;

  bool done_flag;
  bool error_flag;
  bool blocked_flag;
  bool sleep_flag;
  bool interval_wait_flag;

  bool is_scheduled;

  bool is_waiting_for_child;

  int retcode;

  uint64_t run_count;

protected:
  RGWCoroutinesEnv *env;
  RGWCoroutinesStack *parent;

  RGWCoroutinesStack *spawn(RGWCoroutine *source_op, RGWCoroutine *next_op, bool wait);
  bool collect(RGWCoroutine *op, int *ret, RGWCoroutinesStack *skip_stack); /* returns true if needs to be called again */
  bool collect_next(RGWCoroutine *op, int *ret, RGWCoroutinesStack **collected_stack); /* returns true if found a stack to collect */
public:
  RGWCoroutinesStack(CephContext *_cct, RGWCoroutinesManager *_ops_mgr, RGWCoroutine *start = NULL);
  ~RGWCoroutinesStack() override;

  int operate(RGWCoroutinesEnv *env);

  bool is_done() {
    return done_flag;
  }
  bool is_error() {
    return error_flag;
  }
  bool is_blocked_by_stack() {
    return !blocked_by_stack.empty();
  }
  void set_io_blocked(bool flag) {
    blocked_flag = flag;
  }
  void set_io_blocked_id(const rgw_io_id& io_id) {
    io_blocked_id = io_id;
  }
  bool is_io_blocked() {
    return blocked_flag && !done_flag;
  }
  bool can_io_unblock(const rgw_io_id& io_id) {
    return ((io_blocked_id.id < 0) ||
            io_blocked_id.intersects(io_id));
  }
  bool try_io_unblock(const rgw_io_id& io_id);
  bool consume_io_finish(const rgw_io_id& io_id);
  void set_interval_wait(bool flag) {
    interval_wait_flag = flag;
  }
  bool is_interval_waiting() {
    return interval_wait_flag;
  }
  void set_sleeping(bool flag) {
    bool wakeup = sleep_flag & !flag;
    sleep_flag = flag;
    if (wakeup) {
      schedule();
    }
  }
  bool is_sleeping() {
    return sleep_flag;
  }
  void set_is_scheduled(bool flag) {
    is_scheduled = flag;
  }

  bool is_blocked() {
    return is_blocked_by_stack() || is_sleeping() ||
          is_io_blocked() || waiting_for_child() ;
  }

  void schedule();
  void _schedule();

  int get_ret_status() {
    return retcode;
  }

  string error_str();

  void call(RGWCoroutine *next_op);
  RGWCoroutinesStack *spawn(RGWCoroutine *next_op, bool wait);
  int unwind(int retcode);

  int wait(const utime_t& interval);
  void wakeup();
  void io_complete() {
    io_complete(rgw_io_id{});
  }
  void io_complete(const rgw_io_id& io_id);

  bool collect(int *ret, RGWCoroutinesStack *skip_stack); /* returns true if needs to be called again */

  void cancel();

  RGWAioCompletionNotifier *create_completion_notifier();
  template <typename T>
  RGWAioCompletionNotifier *create_completion_notifier(T value);
  RGWCompletionManager *get_completion_mgr();

  void set_blocked_by(RGWCoroutinesStack *s) {
    blocked_by_stack.insert(s);
    s->blocking_stacks.insert(this);
  }

  void set_wait_for_child(bool flag) {
    is_waiting_for_child = flag;
  }

  bool waiting_for_child() {
    return is_waiting_for_child;
  }

  bool unblock_stack(RGWCoroutinesStack **s);

  RGWCoroutinesEnv *get_env() const { return env; }

  void dump(Formatter *f) const;

  void init_new_io(RGWIOProvider *io_provider);
};

template <class T>
void RGWConsumerCR<T>::receive(list<T>& l, bool wakeup)
{
  product.splice(product.end(), l);
  if (wakeup) {
    set_sleeping(false);
  }
}


template <class T>
void RGWConsumerCR<T>::receive(const T& p, bool wakeup)
{
  product.push_back(p);
  if (wakeup) {
    set_sleeping(false);
  }
}

class RGWCoroutinesManagerRegistry : public RefCountedObject, public AdminSocketHook {
  CephContext *cct;

  set<RGWCoroutinesManager *> managers;
  RWLock lock;

  string admin_command;

public:
  explicit RGWCoroutinesManagerRegistry(CephContext *_cct) : cct(_cct), lock("RGWCoroutinesRegistry::lock") {}
  ~RGWCoroutinesManagerRegistry() override;

  void add(RGWCoroutinesManager *mgr);
  void remove(RGWCoroutinesManager *mgr);

  int hook_to_admin_command(const string& command);
  bool call(std::string_view command, const cmdmap_t& cmdmap,
            std::string_view format, bufferlist& out) override;

  void dump(Formatter *f) const;
};

class RGWCoroutinesManager {
  CephContext *cct;
  std::atomic<bool> going_down = { false };

  std::atomic<int64_t> run_context_count = { 0 };
  map<uint64_t, set<RGWCoroutinesStack *> > run_contexts;

  std::atomic<int64_t> max_io_id = { 0 };

  RWLock lock;

  RGWIOIDProvider io_id_provider;

  void handle_unblocked_stack(set<RGWCoroutinesStack *>& context_stacks, list<RGWCoroutinesStack *>& scheduled_stacks,
                              RGWCompletionManager::io_completion& io, int *waiting_count);
protected:
  RGWCompletionManager *completion_mgr;
  RGWCoroutinesManagerRegistry *cr_registry;

  int ops_window;

  string id;

  void put_completion_notifier(RGWAioCompletionNotifier *cn);
public:
  RGWCoroutinesManager(CephContext *_cct, RGWCoroutinesManagerRegistry *_cr_registry) : cct(_cct), lock("RGWCoroutinesManager::lock"),
                                                                                        cr_registry(_cr_registry), ops_window(RGW_ASYNC_OPS_MGR_WINDOW) {
    completion_mgr = new RGWCompletionManager(cct);
    if (cr_registry) {
      cr_registry->add(this);
    }
  }
  virtual ~RGWCoroutinesManager() {
    stop();
    completion_mgr->put();
    if (cr_registry) {
      cr_registry->remove(this);
    }
  }

  int run(list<RGWCoroutinesStack *>& ops);
  int run(RGWCoroutine *op);
  void stop() {
    bool expected = false;
    if (going_down.compare_exchange_strong(expected, true)) {
      completion_mgr->go_down();
    }
  }

  virtual void report_error(RGWCoroutinesStack *op);

  RGWAioCompletionNotifier *create_completion_notifier(RGWCoroutinesStack *stack);
  template <typename T>
  RGWAioCompletionNotifier *create_completion_notifier(RGWCoroutinesStack *stack, T value);
  RGWCompletionManager *get_completion_mgr() { return completion_mgr; }

  void schedule(RGWCoroutinesEnv *env, RGWCoroutinesStack *stack);
  void _schedule(RGWCoroutinesEnv *env, RGWCoroutinesStack *stack);
  RGWCoroutinesStack *allocate_stack();

  int64_t get_next_io_id();

  void set_sleeping(RGWCoroutine *cr, bool flag);
  void io_complete(RGWCoroutine *cr, const rgw_io_id& io_id);

  virtual string get_id();
  void dump(Formatter *f) const;

  RGWIOIDProvider& get_io_id_provider() {
    return io_id_provider;
  }
};

template <typename T>
RGWAioCompletionNotifier *RGWCoroutinesManager::create_completion_notifier(RGWCoroutinesStack *stack, T value)
{
  rgw_io_id io_id{get_next_io_id(), -1};
  RGWAioCompletionNotifier *cn = new RGWAioCompletionNotifierWith<T>(completion_mgr, io_id, (void *)stack, std::move(value));
  completion_mgr->register_completion_notifier(cn);
  return cn;
}

template <typename T>
RGWAioCompletionNotifier *RGWCoroutinesStack::create_completion_notifier(T value)
{
  return ops_mgr->create_completion_notifier(this, std::move(value));
}

class RGWSimpleCoroutine : public RGWCoroutine {
  bool called_cleanup;

  int operate() override;

  int state_init();
  int state_send_request();
  int state_request_complete();
  int state_all_complete();

  void call_cleanup();

public:
  RGWSimpleCoroutine(CephContext *_cct) : RGWCoroutine(_cct), called_cleanup(false) {}
  ~RGWSimpleCoroutine() override;

  virtual int init() { return 0; }
  virtual int send_request() = 0;
  virtual int request_complete() = 0;
  virtual int finish() { return 0; }
  virtual void request_cleanup() {}
};

#endif
