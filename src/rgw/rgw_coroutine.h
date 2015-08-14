#ifndef CEPH_RGW_COROUTINE_H
#define CEPH_RGW_COROUTINE_H

#ifdef _ASSERT_H
#define NEED_ASSERT_H
#pragma push_macro("_ASSERT_H")
#endif

#include <boost/asio.hpp>
#include <boost/asio/coroutine.hpp>

#ifdef NEED_ASSERT_H
#pragma pop_macro("_ASSERT_H")
#endif

#include "rgw_http_client.h"

#include "common/RefCountedObj.h"
#include "common/debug.h"

#define RGW_ASYNC_OPS_MGR_WINDOW 16

class RGWCoroutinesStack;
class RGWCoroutinesManager;

/* a single use librados aio completion notifier that hooks into the RGWCompletionManager */
class RGWAioCompletionNotifier : public RefCountedObject {
  librados::AioCompletion *c;
  RGWCompletionManager *completion_mgr;
  void *user_data;

public:
  RGWAioCompletionNotifier(RGWCompletionManager *_mgr, void *_user_data);
  ~RGWAioCompletionNotifier() {
    c->release();
  }

  librados::AioCompletion *completion() {
    return c;
  }

  void cb() {
    completion_mgr->complete(user_data);
    put();
  }
};


struct RGWCoroutinesEnv {
  RGWCoroutinesManager *manager;
  list<RGWCoroutinesStack *> *stacks;
  RGWCoroutinesStack *stack;

  RGWCoroutinesEnv() : manager(NULL), stacks(NULL), stack(NULL) {}
};

enum RGWCoroutineState {
  RGWCoroutine_Error = -2,
  RGWCoroutine_Done  = -1,
  RGWCoroutine_Run   =  0,
};

class RGWCoroutine : public RefCountedObject, public boost::asio::coroutine {
  friend class RGWCoroutinesStack;

protected:
  CephContext *cct;

  RGWCoroutinesStack *stack;
  bool io_blocked; /* wait for an actual io to complete, will need manager to wait on event */
  bool sleep; /* was set to sleep manually will be awaken manually, e.g., in producer consumer scenario */
  int retcode;
  int state;

  stringstream error_stream;

  int set_state(int s, int ret = 0) {
    state = s;
    return ret;
  }
  void set_io_blocked(bool flag) { io_blocked = flag; }
  void set_sleeping(bool flag) { sleep = flag; }
  int io_block(int ret) {
    set_io_blocked(true);
    return ret;
  }

  void call(RGWCoroutine *op); /* call at the same stack we're in */
  void spawn(RGWCoroutine *op, bool wait); /* execute on a different stack */

public:
  RGWCoroutine(CephContext *_cct) : cct(_cct), stack(NULL), io_blocked(false), sleep(false), retcode(0), state(RGWCoroutine_Run) {}
  virtual ~RGWCoroutine() {}

  virtual int operate() = 0;

  bool is_done() { return (state == RGWCoroutine_Done || state == RGWCoroutine_Error); }
  bool is_error() { return (state == RGWCoroutine_Error); }

  stringstream& log_error() { return error_stream; }
  string error_str() {
    return error_stream.str();
  }

  bool is_io_blocked() { return io_blocked; }
  bool is_sleeping() { return sleep; }

  void set_retcode(int r) {
    retcode = r;
  }

  int get_ret_status() {
    return retcode;
  }
};

template <class T>
class RGWConsumerCR : public RGWCoroutine {
  list<T> product;

public:
  RGWConsumerCR(CephContext *_cct) : RGWCoroutine(_cct) {}

  bool has_product() {
    return product.empty();
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

  void receive(const T& p, bool wakeup = true) {
    product.push_back(p);
    if (wakeup) {
      set_sleeping(false);
    }
  }
};

class RGWCoroutinesStack : public RefCountedObject {
  friend class RGWCoroutine;

  CephContext *cct;

  RGWCoroutinesManager *ops_mgr;

  list<RGWCoroutine *> ops;
  list<RGWCoroutine *>::iterator pos;

  list<RGWCoroutinesStack *> spawned_stacks;

  set<RGWCoroutinesStack *> blocked_by_stack;
  set<RGWCoroutinesStack *> blocking_stacks;

  bool done_flag;
  bool error_flag;
  bool blocked_flag;
  bool sleep_flag;

  int retcode;

protected:
  RGWCoroutinesEnv *env;

public:
  RGWCoroutinesStack(CephContext *_cct, RGWCoroutinesManager *_ops_mgr, RGWCoroutine *start = NULL);

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
  bool is_io_blocked() {
    return blocked_flag || is_blocked_by_stack();
  }
  bool is_sleeping() {
    return sleep_flag;
  }

  int get_ret_status() {
    return retcode;
  }

  void set_io_blocked(bool flag);

  string error_str();

  int call(RGWCoroutine *next_op, int ret = 0);
  void spawn(RGWCoroutine *next_op, bool wait);
  int unwind(int retcode);

  int complete_spawned();

  RGWAioCompletionNotifier *create_completion_notifier();
  RGWCompletionManager *get_completion_mgr();

  void set_blocked_by(RGWCoroutinesStack *s) {
    blocked_by_stack.insert(s);
    s->blocking_stacks.insert(this);
  }

  bool unblock_stack(RGWCoroutinesStack **s);

  RGWCoroutinesEnv *get_env() { return env; }
};

class RGWCoroutinesManager {
  CephContext *cct;

  void handle_unblocked_stack(list<RGWCoroutinesStack *>& stacks, RGWCoroutinesStack *stack, int *waiting_count);
protected:
  RGWCompletionManager completion_mgr;

  int ops_window;

  void put_completion_notifier(RGWAioCompletionNotifier *cn);
public:
  RGWCoroutinesManager(CephContext *_cct) : cct(_cct), ops_window(RGW_ASYNC_OPS_MGR_WINDOW) {}
  virtual ~RGWCoroutinesManager() {}

  int run(list<RGWCoroutinesStack *>& ops);
  int run(RGWCoroutine *op);

  virtual void report_error(RGWCoroutinesStack *op);

  RGWAioCompletionNotifier *create_completion_notifier(RGWCoroutinesStack *stack);
  RGWCompletionManager *get_completion_mgr() { return &completion_mgr; }

  RGWCoroutinesStack *allocate_stack() {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(cct, this);
    stack->get();
    return stack;
  }
};

class RGWSimpleCoroutine : public RGWCoroutine {
  int operate();

  int state_init();
  int state_send_request();
  int state_request_complete();
  int state_all_complete();

public:
  RGWSimpleCoroutine(CephContext *_cct) : RGWCoroutine(_cct) {}

  virtual int init() { return 0; }
  virtual int send_request() = 0;
  virtual int request_complete() = 0;
  virtual int finish() { return 0; }

};

#endif
