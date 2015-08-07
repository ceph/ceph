

#include "rgw_coroutine.h"

#define dout_subsys ceph_subsys_rgw



RGWCoroutinesStack::RGWCoroutinesStack(CephContext *_cct, RGWCoroutinesManager *_ops_mgr, RGWCoroutine *start) : cct(_cct), ops_mgr(_ops_mgr),
                                                                                                         done_flag(false), error_flag(false), blocked_flag(false) {
  if (start) {
    ops.push_back(start);
  }
  pos = ops.begin();
}

int RGWCoroutinesStack::operate(RGWCoroutinesEnv *env)
{
  RGWCoroutine *op = *pos;
  int r = op->do_operate(env);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: op->operate() returned r=" << r << dendl;
  }

  error_flag = op->is_error();
  blocked_flag = op->is_blocked();

  if (op->is_done()) {
    op->put();
    r = unwind(r);
    done_flag = (pos == ops.end());
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
  ops.push_back(next_op);
  if (pos != ops.end()) {
    ++pos;
  } else {
    pos = ops.begin();
  }
  return ret;
}

int RGWCoroutinesStack::unwind(int retcode)
{
  if (pos == ops.begin()) {
    pos = ops.end();
    return retcode;
  }

  --pos;
  ops.pop_back();
  RGWCoroutine *op = *pos;
  op->set_retcode(retcode);
  return 0;
}

void RGWCoroutinesStack::set_blocked(bool flag)
{
  blocked_flag = flag;
  if (pos != ops.end()) {
    (*pos)->set_blocked(flag);
  }
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

void RGWCoroutinesManager::handle_unblocked_stack(list<RGWCoroutinesStack *>& stacks, RGWCoroutinesStack *stack, int *waiting_count)
{
  --(*waiting_count);
  stack->set_blocked(false);
  if (!stack->is_done()) {
    stacks.push_back(stack);
  } else {
    delete stack;
  }
}

int RGWCoroutinesManager::run(list<RGWCoroutinesStack *>& stacks)
{
  int waiting_count = 0;
  RGWCoroutinesEnv env;

  env.manager = this;
  env.stacks = &stacks;

  for (list<RGWCoroutinesStack *>::iterator iter = stacks.begin(); iter != stacks.end();) {
    RGWCoroutinesStack *stack = *iter;
    env.stack = stack;
    int ret = stack->operate(&env);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: stack->operate() returned ret=" << ret << dendl;
    }

    if (stack->is_error()) {
      report_error(stack);
    }

    if (stack->is_blocked_by_stack()) {
      /* do nothing, we'll re-add the stack when the blocking stack is done */
    } else if (stack->is_blocked()) {
      waiting_count++;
    } else if (stack->is_done()) {
      RGWCoroutinesStack *s;
      while (stack->unblock_stack(&s)) {
	if (!s->is_blocked_by_stack() && !s->is_done()) {
	  if (s->is_blocked()) {
	    waiting_count++;
	  } else {
	    stacks.push_back(s);
	  }
	}
      }
      delete stack;
    } else {
      stacks.push_back(stack);
    }

    RGWCoroutinesStack *blocked_stack;
    while (completion_mgr.try_get_next((void **)&blocked_stack)) {
      handle_unblocked_stack(stacks, blocked_stack, &waiting_count);
    }

    if (waiting_count >= ops_window) {
      int ret = completion_mgr.get_next((void **)&blocked_stack);
      if (ret < 0) {
	ldout(cct, 0) << "ERROR: failed to clone shard, completion_mgr.get_next() returned ret=" << ret << dendl;
      }
      handle_unblocked_stack(stacks, blocked_stack, &waiting_count);
    }

    ++iter;
    stacks.pop_front();
    while (iter == stacks.end() && waiting_count > 0) {
      int ret = completion_mgr.get_next((void **)&blocked_stack);
      if (ret < 0) {
	ldout(cct, 0) << "ERROR: failed to clone shard, completion_mgr.get_next() returned ret=" << ret << dendl;
      }
      handle_unblocked_stack(stacks, blocked_stack, &waiting_count);
      iter = stacks.begin();
    }
  }

  return 0;
}

int RGWCoroutinesManager::run(RGWCoroutine *op)
{
  list<RGWCoroutinesStack *> stacks;
  RGWCoroutinesStack *stack = allocate_stack();
  op->get();
  int r = stack->call(op);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: stack->call() returned r=" << r << dendl;
    return r;
  }

  stacks.push_back(stack);

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

void RGWCoroutine::call(RGWCoroutine *op)
{
  int r = env->stack->call(op, 0);
  assert(r == 0);
}

void RGWCoroutine::spawn(RGWCoroutine *op)
{
  RGWCoroutinesStack *stack = env->manager->allocate_stack();

  int r = stack->call(op, 0);
  assert(r == 0);

  env->stacks->push_back(stack);

  env->stack->set_blocked_by(stack);
}

int RGWSimpleCoroutine::operate()
{
  switch (state) {
    case Init:
      ldout(cct, 20) << __func__ << ": init request" << dendl;
      return state_init();
    case SendRequest:
      ldout(cct, 20) << __func__ << ": send request" << dendl;
      return state_send_request();
    case RequestComplete:
      ldout(cct, 20) << __func__ << ": request complete" << dendl;
      return state_request_complete();
    case AllComplete:
      ldout(cct, 20) << __func__ << ": all complete" << dendl;
      return state_all_complete();
    case Done:
      ldout(cct, 20) << __func__ << ": done" << dendl;
      break;
    case Error:
      ldout(cct, 20) << __func__ << ": error" << dendl;
      break;
  }

  return 0;
}

int RGWSimpleCoroutine::state_init()
{
  int ret = init();
  if (ret < 0) {
    return set_state(Error, ret);
  }
  return set_state(SendRequest);
}

int RGWSimpleCoroutine::state_send_request()
{
  int ret = send_request();
  if (ret < 0) {
    return set_state(Error, ret);
  }
  return yield(set_state(RequestComplete));
}

int RGWSimpleCoroutine::state_request_complete()
{
  int ret = request_complete();
  if (ret < 0) {
    return set_state(Error, ret);
  }
  return set_state(AllComplete);
}

int RGWSimpleCoroutine::state_all_complete()
{
  int ret = finish();
  if (ret < 0) {
    return set_state(Error, ret);
  }
  return set_state(Done);
}


