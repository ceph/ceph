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

  RGWCoroutinesEnv *env;
  bool blocked;
  int retcode;
  int state;

  stringstream error_stream;

  list<RGWCoroutine *> spawned_ops;

  int set_state(int s, int ret = 0) {
    state = s;
    return ret;
  }
  void set_blocked(bool flag) { blocked = flag; }
  int block(int ret) {
    set_blocked(true);
    return ret;
  }

  int do_operate(RGWCoroutinesEnv *_env) {
    env = _env;
    return operate();
  }

  void call(RGWCoroutine *op);
  void spawn(RGWCoroutine *op, bool wait);

  int complete_spawned();

public:
  RGWCoroutine(CephContext *_cct) : cct(_cct), env(NULL), blocked(false), retcode(0), state(RGWCoroutine_Run) {}
  virtual ~RGWCoroutine() {}

  virtual int operate() = 0;

  bool is_done() { return (state == RGWCoroutine_Done || state == RGWCoroutine_Error); }
  bool is_error() { return (state == RGWCoroutine_Error); }

  stringstream& log_error() { return error_stream; }
  string error_str() {
    return error_stream.str();
  }

  bool is_blocked() { return blocked; }

  void set_retcode(int r) {
    retcode = r;
  }

  int get_ret_status() {
    return retcode;
  }
};

class RGWCoroutinesStack {
  CephContext *cct;

  RGWCoroutinesManager *ops_mgr;

  list<RGWCoroutine *> ops;
  list<RGWCoroutine *>::iterator pos;

  set<RGWCoroutinesStack *> blocked_by_stack;
  set<RGWCoroutinesStack *> blocking_stacks;


  bool done_flag;
  bool error_flag;
  bool blocked_flag;

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
  bool is_blocked() {
    return blocked_flag || is_blocked_by_stack();
  }

  void set_blocked(bool flag);

  string error_str();

  int call(RGWCoroutine *next_op, int ret = 0);
  int unwind(int retcode);

  RGWAioCompletionNotifier *create_completion_notifier();
  RGWCompletionManager *get_completion_mgr();

  void set_blocked_by(RGWCoroutinesStack *s) {
    blocked_by_stack.insert(s);
    s->blocking_stacks.insert(this);
  }

  bool unblock_stack(RGWCoroutinesStack **s);
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
    return new RGWCoroutinesStack(cct, this);
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
