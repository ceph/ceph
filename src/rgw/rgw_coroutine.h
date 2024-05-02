// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#ifdef _ASSERT_H
#define NEED_ASSERT_H
#pragma push_macro("_ASSERT_H")
#endif

#include <boost/asio/coroutine.hpp>
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
#include "rgw_http_client_types.h"

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
  std::list<io_completion> complete_reqs;
  std::set<rgw_io_id> complete_reqs_set;
  using NotifierRef = boost::intrusive_ptr<RGWAioCompletionNotifier>;
  std::set<NotifierRef> cns;

  ceph::mutex lock = ceph::make_mutex("RGWCompletionManager::lock");
  ceph::condition_variable cond;

  SafeTimer timer;

  std::atomic<bool> going_down = { false };

  std::map<void *, void *> waiters;

  class WaitContext;

protected:
  void _wakeup(void *opaque);
  void _complete(RGWAioCompletionNotifier *cn, const rgw_io_id& io_id, void *user_info);
public:
  explicit RGWCompletionManager(CephContext *_cct);
  virtual ~RGWCompletionManager() override;

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
  ceph::mutex lock = ceph::make_mutex("RGWAioCompletionNotifier");
  bool registered;

public:
  RGWAioCompletionNotifier(RGWCompletionManager *_mgr, const rgw_io_id& _io_id, void *_user_data);
  virtual ~RGWAioCompletionNotifier() override {
    c->release();
    lock.lock();
    bool need_unregister = registered;
    if (registered) {
      completion_mgr->get();
    }
    registered = false;
    lock.unlock();
    if (need_unregister) {
      completion_mgr->unregister_completion_notifier(this);
      completion_mgr->put();
    }
  }

  librados::AioCompletion *completion() {
    return c;
  }

  void unregister() {
    std::lock_guard l{lock};
    if (!registered) {
      return;
    }
    registered = false;
  }

  void cb() {
    lock.lock();
    if (!registered) {
      lock.unlock();
      put();
      return;
    }
    completion_mgr->get();
    registered = false;
    lock.unlock();
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
  std::list<RGWCoroutinesStack *> *scheduled_stacks;
  RGWCoroutinesStack *stack;

  RGWCoroutinesEnv() : run_context(0), manager(NULL), scheduled_stacks(NULL), stack(NULL) {}
};

enum RGWCoroutineState {
  RGWCoroutine_Error = -2,
  RGWCoroutine_Done  = -1,
  RGWCoroutine_Run   =  0,
};

struct rgw_spawned_stacks {
  std::vector<RGWCoroutinesStack *> entries;

  rgw_spawned_stacks() {}

  void add_pending(RGWCoroutinesStack *s) {
    entries.push_back(s);
  }

  void inherit(rgw_spawned_stacks *source) {
    for (auto* entry : source->entries) {
      add_pending(entry);
    }
    source->entries.clear();
  }
};



class RGWCoroutine : public RefCountedObject, public boost::asio::coroutine {
  friend class RGWCoroutinesStack;

  struct StatusItem {
    utime_t timestamp;
    std::string status;

    StatusItem(utime_t& t, const std::string& s) : timestamp(t), status(s) {}

    void dump(Formatter *f) const;
  };

#define MAX_COROUTINE_HISTORY 10

  struct Status {
    CephContext *cct;
    ceph::shared_mutex lock =
      ceph::make_shared_mutex("RGWCoroutine::Status::lock");
    int max_history;

    utime_t timestamp;
    std::stringstream status;

    explicit Status(CephContext *_cct) : cct(_cct), max_history(MAX_COROUTINE_HISTORY) {}

    std::deque<StatusItem> history;

    std::stringstream& set_status();
  } status;

  std::stringstream description;

protected:
  bool _yield_ret;

  struct {
    boost::asio::coroutine cr;
    bool should_exit{false};
    int ret{0};

    void init() {
      cr = boost::asio::coroutine();
      should_exit = false;
      ret = 0;
    }
  } drain_status;

  CephContext *cct;

  RGWCoroutinesStack *stack;
  int retcode;
  int state;

  rgw_spawned_stacks spawned;

  std::stringstream error_stream;

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
    description.str(std::string());
  }

  std::stringstream& set_description() {
    return description;
  }
  std::stringstream& set_status() {
    return status.set_status();
  }

  std::stringstream& set_status(const std::string& s) {
    std::stringstream& status = set_status();
    status << s;
    return status;
  }

  virtual int operate_wrapper(const DoutPrefixProvider *dpp) {
    return operate(dpp);
  }
public:
  RGWCoroutine(CephContext *_cct) : status(_cct), _yield_ret(false), cct(_cct), stack(NULL), retcode(0), state(RGWCoroutine_Run) {}
  virtual ~RGWCoroutine() override;

  virtual int operate(const DoutPrefixProvider *dpp) = 0;

  bool is_done() { return (state == RGWCoroutine_Done || state == RGWCoroutine_Error); }
  bool is_error() { return (state == RGWCoroutine_Error); }

  std::stringstream& log_error() { return error_stream; }
  std::string error_str() {
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
  bool collect(int *ret, RGWCoroutinesStack *skip_stack, uint64_t *stack_id = nullptr); /* returns true if needs to be called again */
  bool collect_next(int *ret, RGWCoroutinesStack **collected_stack = NULL); /* returns true if found a stack to collect */

  int wait(const utime_t& interval);
  bool drain_children(int num_cr_left,
                      RGWCoroutinesStack *skip_stack = nullptr,
                      std::optional<std::function<void(uint64_t stack_id, int ret)> > cb = std::nullopt); /* returns true if needed to be called again,
                                                                                                             cb will be called on completion of every
                                                                                                             completion. */
  bool drain_children(int num_cr_left,
                      std::optional<std::function<int(uint64_t stack_id, int ret)> > cb); /* returns true if needed to be called again,
                                                                                             cb will be called on every completion, can filter errors.
                                                                                             A negative return value from cb means that current cr
                                                                                             will need to exit */
  void wakeup();
  void set_sleeping(bool flag); /* put in sleep, or wakeup from sleep */

  size_t num_spawned() {
    return spawned.entries.size();
  }

  void wait_for_child();

  virtual std::string to_str() const;

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

std::ostream& operator<<(std::ostream& out, const RGWCoroutine& cr);

#define yield_until_true(x)     \
do {                            \
  do {                          \
    yield _yield_ret = x;       \
  } while (!_yield_ret);        \
  _yield_ret = false;           \
} while (0)

#define drain_all() \
  drain_status.init(); \
  yield_until_true(drain_children(0))

#define drain_all_but(n) \
  drain_status.init(); \
  yield_until_true(drain_children(n))

#define drain_all_but_stack(stack) \
  drain_status.init(); \
  yield_until_true(drain_children(1, stack))

#define drain_all_but_stack_cb(stack, cb) \
  drain_status.init(); \
  yield_until_true(drain_children(1, stack, cb))

#define drain_with_cb(n, cb) \
  drain_status.init(); \
  yield_until_true(drain_children(n, cb)); \
  if (drain_status.should_exit) { \
    return set_cr_error(drain_status.ret); \
  }

#define drain_all_cb(cb) \
  drain_with_cb(0, cb)

#define yield_spawn_window(cr, n, cb) \
  do { \
    spawn(cr, false); \
    drain_with_cb(n, cb); /* this is guaranteed to yield */ \
  } while (0)



template <class T>
class RGWConsumerCR : public RGWCoroutine {
  std::list<T> product;

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
  void receive(std::list<T>& l, bool wakeup = true);
};

class RGWCoroutinesStack : public RefCountedObject {
  friend class RGWCoroutine;
  friend class RGWCoroutinesManager;

  CephContext *cct;

  int64_t id{-1};

  RGWCoroutinesManager *ops_mgr;

  std::list<RGWCoroutine *> ops;
  std::list<RGWCoroutine *>::iterator pos;

  rgw_spawned_stacks spawned;

  std::set<RGWCoroutinesStack *> blocked_by_stack;
  std::set<RGWCoroutinesStack *> blocking_stacks;

  std::map<int64_t, rgw_io_id> io_finish_ids;
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
  bool collect(RGWCoroutine *op, int *ret, RGWCoroutinesStack *skip_stack, uint64_t *stack_id); /* returns true if needs to be called again */
  bool collect_next(RGWCoroutine *op, int *ret, RGWCoroutinesStack **collected_stack); /* returns true if found a stack to collect */
public:
  RGWCoroutinesStack(CephContext *_cct, RGWCoroutinesManager *_ops_mgr, RGWCoroutine *start = NULL);
  virtual ~RGWCoroutinesStack() override;

  int64_t get_id() const {
    return id;
  }

  int operate(const DoutPrefixProvider *dpp, RGWCoroutinesEnv *env);

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

  std::string error_str();

  void call(RGWCoroutine *next_op);
  RGWCoroutinesStack *spawn(RGWCoroutine *next_op, bool wait);
  int unwind(int retcode);

  int wait(const utime_t& interval);
  void wakeup();
  void io_complete() {
    io_complete(rgw_io_id{});
  }
  void io_complete(const rgw_io_id& io_id);

  bool collect(int *ret, RGWCoroutinesStack *skip_stack, uint64_t *stack_id); /* returns true if needs to be called again */

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
void RGWConsumerCR<T>::receive(std::list<T>& l, bool wakeup)
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

  std::set<RGWCoroutinesManager *> managers;
  ceph::shared_mutex lock =
    ceph::make_shared_mutex("RGWCoroutinesRegistry::lock");

  std::string admin_command;

public:
  explicit RGWCoroutinesManagerRegistry(CephContext *_cct) : cct(_cct) {}
  virtual ~RGWCoroutinesManagerRegistry() override;

  void add(RGWCoroutinesManager *mgr);
  void remove(RGWCoroutinesManager *mgr);

  int hook_to_admin_command(const std::string& command);
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& ss,
	   bufferlist& out) override;

  void dump(Formatter *f) const;
};

class RGWCoroutinesManager {
  CephContext *cct;
  std::atomic<bool> going_down = { false };

  std::atomic<int64_t> run_context_count = { 0 };
  std::map<uint64_t, std::set<RGWCoroutinesStack *> > run_contexts;

  std::atomic<int64_t> max_io_id = { 0 };
  std::atomic<uint64_t> max_stack_id = { 0 };

  mutable ceph::shared_mutex lock =
    ceph::make_shared_mutex("RGWCoroutinesManager::lock");

  RGWIOIDProvider io_id_provider;

  void handle_unblocked_stack(std::set<RGWCoroutinesStack *>& context_stacks, std::list<RGWCoroutinesStack *>& scheduled_stacks,
                              RGWCompletionManager::io_completion& io, int *waiting_count, int *interval_wait_count);
protected:
  RGWCompletionManager *completion_mgr;
  RGWCoroutinesManagerRegistry *cr_registry;

  int ops_window;

  std::string id;

  void put_completion_notifier(RGWAioCompletionNotifier *cn);
public:
  RGWCoroutinesManager(CephContext *_cct, RGWCoroutinesManagerRegistry *_cr_registry) : cct(_cct),
                                                                                        cr_registry(_cr_registry), ops_window(RGW_ASYNC_OPS_MGR_WINDOW) {
    completion_mgr = new RGWCompletionManager(cct);
    if (cr_registry) {
      cr_registry->add(this);
    }
  }
  virtual ~RGWCoroutinesManager();

  int run(const DoutPrefixProvider *dpp, std::list<RGWCoroutinesStack *>& ops);
  int run(const DoutPrefixProvider *dpp, RGWCoroutine *op);
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
  uint64_t get_next_stack_id();

  void set_sleeping(RGWCoroutine *cr, bool flag);
  void io_complete(RGWCoroutine *cr, const rgw_io_id& io_id);

  virtual std::string get_id();
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
  const int max_eio_retries;

  int tries{0};
  int op_ret{0};

  int operate(const DoutPrefixProvider *dpp) override;

  int state_init();
  int state_send_request(const DoutPrefixProvider *dpp);
  int state_request_complete();
  int state_all_complete();

  void call_cleanup();

public:
  RGWSimpleCoroutine(CephContext *_cct) : RGWCoroutine(_cct), called_cleanup(false), max_eio_retries(1) {}
  RGWSimpleCoroutine(CephContext *_cct, const int _max_eio_retries) : RGWCoroutine(_cct), called_cleanup(false), max_eio_retries(_max_eio_retries) {}
  virtual ~RGWSimpleCoroutine() override;

  virtual int init() { return 0; }
  virtual int send_request(const DoutPrefixProvider *dpp) = 0;
  virtual int request_complete() = 0;
  virtual int finish() { return 0; }
  virtual void request_cleanup() {}
};
