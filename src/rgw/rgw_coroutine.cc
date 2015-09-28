

#include "rgw_coroutine.h"

#include <boost/asio/coroutine.hpp>
#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw


RGWCompletionManager::RGWCompletionManager(CephContext *_cct) : cct(_cct), lock("RGWCompletionManager::lock"),
                                            timer(cct, lock)
{
  timer.init();
}

RGWCompletionManager::~RGWCompletionManager()
{
  Mutex::Locker l(lock);
  timer.cancel_all_events();
  timer.shutdown();
}

void RGWCompletionManager::complete(void *user_info)
{
  Mutex::Locker l(lock);
  _complete(user_info);
}

void RGWCompletionManager::_complete(void *user_info)
{
  complete_reqs.push_back(user_info);
  cond.Signal();
}

int RGWCompletionManager::get_next(void **user_info)
{
  Mutex::Locker l(lock);
  while (complete_reqs.empty()) {
    cond.Wait(lock);
    if (going_down.read() != 0) {
      return -ECANCELED;
    }
  }
  *user_info = complete_reqs.front();
  complete_reqs.pop_front();
  return 0;
}

bool RGWCompletionManager::try_get_next(void **user_info)
{
  Mutex::Locker l(lock);
  if (complete_reqs.empty()) {
    return false;
  }
  *user_info = complete_reqs.front();
  complete_reqs.pop_front();
  return true;
}

void RGWCompletionManager::go_down()
{
  Mutex::Locker l(lock);
  going_down.set(1);
  cond.Signal();
}

void RGWCompletionManager::wait_interval(void *opaque, const utime_t& interval, void *user_info)
{
  Mutex::Locker l(lock);
  assert(waiters.find(opaque) == waiters.end());
  waiters[opaque] = user_info;
  timer.add_event_after(interval, new WaitContext(this, opaque));
}

void RGWCompletionManager::wakeup(void *opaque)
{
  Mutex::Locker l(lock);
  _wakeup(opaque);
}

void RGWCompletionManager::_wakeup(void *opaque)
{
  map<void *, void *>::iterator iter = waiters.find(opaque);
  if (iter != waiters.end()) {
    void *user_id = iter->second;
    waiters.erase(iter);
    _complete(user_id);
  }
}


void RGWCoroutine::set_io_blocked(bool flag) {
  stack->set_io_blocked(flag);
}

void RGWCoroutine::set_sleeping(bool flag) {
  stack->set_sleeping(flag);
}

int RGWCoroutine::io_block(int ret) {
  set_io_blocked(true);
  return ret;
}

RGWCoroutinesStack::RGWCoroutinesStack(CephContext *_cct, RGWCoroutinesManager *_ops_mgr, RGWCoroutine *start) : cct(_cct), ops_mgr(_ops_mgr),
                                                                                                         done_flag(false), error_flag(false), blocked_flag(false),
                                                                                                         sleep_flag(false), is_scheduled(false), is_waiting_for_child(false),
													 retcode(0),
													 env(NULL), parent(NULL)
{
  if (start) {
    ops.push_back(start);
  }
  pos = ops.begin();
}

int RGWCoroutinesStack::operate(RGWCoroutinesEnv *_env)
{
  env = _env;
  RGWCoroutine *op = *pos;
  op->stack = this;
  int r = op->operate();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: op->operate() returned r=" << r << dendl;
  }

  error_flag = op->is_error();

  if (op->is_done()) {
    int op_retcode = op->get_ret_status();
    r = unwind(r);
    op->put();
    done_flag = (pos == ops.end());
    if (done_flag) {
      retcode = op_retcode;
    }
    return r;
  }

  /* should r ever be negative at this point? */
  assert(r >= 0);

  return 0;
}

string RGWCoroutinesStack::error_str()
{
  if (pos != ops.end()) {
    return (*pos)->error_str();
  }
  return string();
}

int RGWCoroutinesStack::call(RGWCoroutine *next_op, int ret) {
  if (!next_op) {
    return ret;
  }
  ops.push_back(next_op);
  if (pos != ops.end()) {
    ++pos;
  } else {
    pos = ops.begin();
  }
  return ret;
}

void RGWCoroutinesStack::spawn(RGWCoroutine *source_op, RGWCoroutine *op, bool wait)
{
  if (!op) {
    return;
  }
  op->get();

  rgw_spawned_stacks *s = (source_op ? &source_op->spawned : &spawned);

  RGWCoroutinesStack *stack = env->manager->allocate_stack();
  s->add_pending(stack);
  stack->parent = this;

  stack->get(); /* we'll need to collect the stack */
  int r = stack->call(op, 0);
  assert(r == 0);

  env->stacks->push_back(stack);

  if (wait) {
    set_blocked_by(stack);
  }
}

void RGWCoroutinesStack::spawn(RGWCoroutine *op, bool wait)
{
  spawn(NULL, op, wait);
}

int RGWCoroutinesStack::wait(const utime_t& interval)
{
  RGWCompletionManager *completion_mgr = env->manager->get_completion_mgr();
  completion_mgr->wait_interval((void *)this, interval, (void *)this);
  set_io_blocked(true);
  return 0;
}

void RGWCoroutinesStack::wakeup()
{
  RGWCompletionManager *completion_mgr = env->manager->get_completion_mgr();
  completion_mgr->wakeup((void *)this);
}

int RGWCoroutinesStack::unwind(int retcode)
{
  rgw_spawned_stacks *src_spawned = &(*pos)->spawned;

  if (pos == ops.begin()) {
    spawned.inherit(src_spawned);
    pos = ops.end();
    return retcode;
  }

  --pos;
  ops.pop_back();
  RGWCoroutine *op = *pos;
  op->set_retcode(retcode);
  op->spawned.inherit(src_spawned);
  return 0;
}


bool RGWCoroutinesStack::collect(RGWCoroutine *op, int *ret) /* returns true if needs to be called again */
{
  rgw_spawned_stacks *s = (op ? &op->spawned : &spawned);
  *ret = 0;
  vector<RGWCoroutinesStack *> new_list;

  for (vector<RGWCoroutinesStack *>::iterator iter = s->entries.begin(); iter != s->entries.end(); ++iter) {
    RGWCoroutinesStack *stack = *iter;
    if (!stack->is_done()) {
      new_list.push_back(stack);
      continue;
    }
    int r = stack->get_ret_status();
    if (r < 0) {
      *ret = r;
    }

    stack->put();
  }

  s->entries.swap(new_list);
  return (!new_list.empty());
}

bool RGWCoroutinesStack::collect(int *ret) /* returns true if needs to be called again */
{
  return collect(NULL, ret);
}

static void _aio_completion_notifier_cb(librados::completion_t cb, void *arg);

static void _aio_completion_notifier_cb(librados::completion_t cb, void *arg)
{
  ((RGWAioCompletionNotifier *)arg)->cb();
}

RGWAioCompletionNotifier::RGWAioCompletionNotifier(RGWCompletionManager *_mgr, void *_user_data) : completion_mgr(_mgr), user_data(_user_data) {
  c = librados::Rados::aio_create_completion((void *)this, _aio_completion_notifier_cb, NULL);
}

RGWAioCompletionNotifier *RGWCoroutinesStack::create_completion_notifier()
{
  return ops_mgr->create_completion_notifier(this);
}

RGWCompletionManager *RGWCoroutinesStack::get_completion_mgr()
{
  return ops_mgr->get_completion_mgr();
}

bool RGWCoroutinesStack::unblock_stack(RGWCoroutinesStack **s)
{
  if (blocking_stacks.empty()) {
    return false;
  }

  set<RGWCoroutinesStack *>::iterator iter = blocking_stacks.begin();
  *s = *iter;
  blocking_stacks.erase(iter);
  (*s)->blocked_by_stack.erase(this);

  return true;
}

void RGWCoroutinesManager::report_error(RGWCoroutinesStack *op)
{
#warning need to have error logging infrastructure that logs on backend
  lderr(cct) << "ERROR: failed operation: " << op->error_str() << dendl;
}

void RGWCoroutinesManager::handle_unblocked_stack(list<RGWCoroutinesStack *>& stacks, RGWCoroutinesStack *stack, int *blocked_count)
{
  --(*blocked_count);
  stack->set_io_blocked(false);
  if (!stack->is_done()) {
    stacks.push_back(stack);
  } else {
    stack->put();
  }
}

int RGWCoroutinesManager::run(list<RGWCoroutinesStack *>& stacks)
{
  int blocked_count = 0;
  RGWCoroutinesEnv env;

  env.manager = this;
  env.stacks = &stacks;

  for (list<RGWCoroutinesStack *>::iterator iter = stacks.begin(); iter != stacks.end() && !going_down.read();) {
    RGWCoroutinesStack *stack = *iter;
    env.stack = stack;

    int ret = stack->operate(&env);
    stack->set_is_scheduled(false);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: stack->operate() returned ret=" << ret << dendl;
    }

    if (stack->is_error()) {
      report_error(stack);
    }

    if (stack->is_io_blocked()) {
      ldout(cct, 20) << __func__ << ":" << " stack=" << (void *)stack << " is io blocked" << dendl;
      blocked_count++;
    } else if (stack->is_blocked()) {
      /* do nothing, we'll re-add the stack when the blocking stack is done,
       * or when we're awaken
       */
      ldout(cct, 20) << __func__ << ":" << " stack=" << (void *)stack << " is_blocked_by_stack()=" << stack->is_blocked_by_stack()
	             << " is_sleeping=" << stack->is_sleeping() << " waiting_for_child()=" << stack->waiting_for_child() << dendl;
    } else if (stack->is_done()) {
      ldout(cct, 20) << __func__ << ":" << " stack=" << (void *)stack << " is done" << dendl;
      RGWCoroutinesStack *s;
      while (stack->unblock_stack(&s)) {
	if (!s->is_blocked_by_stack() && !s->is_done()) {
	  if (s->is_io_blocked()) {
	    blocked_count++;
	  } else {
	    s->schedule();
	  }
	}
      }
      if (stack->parent && stack->parent->waiting_for_child()) {
        stack->parent->set_wait_for_child(false);
        stack->parent->schedule();
      }
      stack->put();
    } else {
      stack->schedule();
    }

    RGWCoroutinesStack *blocked_stack;
    while (completion_mgr.try_get_next((void **)&blocked_stack)) {
      handle_unblocked_stack(stacks, blocked_stack, &blocked_count);
    }

    ++iter;
    stacks.pop_front();

    while (stacks.empty() && blocked_count > 0) {
      int ret = completion_mgr.get_next((void **)&blocked_stack);
      if (ret < 0) {
	ldout(cct, 0) << "ERROR: failed to clone shard, completion_mgr.get_next() returned ret=" << ret << dendl;
      }
      if (going_down.read() > 0) {
	ldout(cct, 5) << __func__ << "(): was stopped, exiting" << dendl;
	return 0;
      }
      handle_unblocked_stack(stacks, blocked_stack, &blocked_count);
      iter = stacks.begin();
    }

    if (iter == stacks.end()) {
      iter = stacks.begin();
    }
  }

  return 0;
}

int RGWCoroutinesManager::run(RGWCoroutine *op)
{
  if (!op) {
    return 0;
  }
  list<RGWCoroutinesStack *> stacks;
  RGWCoroutinesStack *stack = allocate_stack();
  op->get();
  int r = stack->call(op);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: stack->call() returned r=" << r << dendl;
    return r;
  }

  stack->schedule(&stacks);

  r = run(stacks);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: run(stacks) returned r=" << r << dendl;
  }

  r = op->get_ret_status();
  op->put();

  return r;
}

RGWAioCompletionNotifier *RGWCoroutinesManager::create_completion_notifier(RGWCoroutinesStack *stack)
{
  return new RGWAioCompletionNotifier(&completion_mgr, (void *)stack);
}

int RGWCoroutine::call(RGWCoroutine *op)
{
  int r = stack->call(op, 0);
  assert(r == 0);
  return 0;
}

void RGWCoroutine::spawn(RGWCoroutine *op, bool wait)
{
  stack->spawn(this, op, wait);
}

bool RGWCoroutine::collect(int *ret) /* returns true if needs to be called again */
{
  return stack->collect(this, ret);
}

int RGWCoroutine::wait(const utime_t& interval)
{
  return stack->wait(interval);
}

void RGWCoroutine::wait_for_child()
{
  /* should only wait for child if there is a child that is not done yet */
  for (vector<RGWCoroutinesStack *>::iterator iter = spawned.entries.begin(); iter != spawned.entries.end(); ++iter) {
    if (!(*iter)->is_done()) {
      stack->set_wait_for_child(true);
    }
  }
}

void RGWCoroutine::wakeup()
{
  stack->wakeup();
}

int RGWSimpleCoroutine::operate()
{
  int ret = 0;
  reenter(this) {
    yield return state_init();
    yield return state_send_request();
    yield return state_request_complete();
    yield return state_all_complete();
    while (stack->collect(&ret)) {
      yield;
    }
    return set_state(RGWCoroutine_Done, ret);
  }
  return 0;
}

int RGWSimpleCoroutine::state_init()
{
  int ret = init();
  if (ret < 0) {
    return set_state(RGWCoroutine_Error, ret);
  }
  return 0;
}

int RGWSimpleCoroutine::state_send_request()
{
  int ret = send_request();
  if (ret < 0) {
    return set_state(RGWCoroutine_Error, ret);
  }
  return io_block(0);
}

int RGWSimpleCoroutine::state_request_complete()
{
  int ret = request_complete();
  if (ret < 0) {
    return set_state(RGWCoroutine_Error, ret);
  }
  return 0;
}

int RGWSimpleCoroutine::state_all_complete()
{
  int ret = finish();
  if (ret < 0) {
    return set_state(RGWCoroutine_Error, ret);
  }
  return 0;
}


