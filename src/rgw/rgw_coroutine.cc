// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "include/Context.h"
#include "common/ceph_json.h"
#include "rgw_coroutine.h"

// re-include our assert to clobber the system one; fix dout:
#include "include/ceph_assert.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

using namespace std;

class RGWCompletionManager::WaitContext : public Context {
  RGWCompletionManager *manager;
  void *opaque;
public:
  WaitContext(RGWCompletionManager *_cm, void *_opaque) : manager(_cm), opaque(_opaque) {}
  void finish(int r) override {
    manager->_wakeup(opaque);
  }
};

RGWCompletionManager::RGWCompletionManager(CephContext *_cct) : cct(_cct),
                                            timer(cct, lock)
{
  timer.init();
}

RGWCompletionManager::~RGWCompletionManager()
{
  std::lock_guard l{lock};
  timer.cancel_all_events();
  timer.shutdown();
}

void RGWCompletionManager::complete(RGWAioCompletionNotifier *cn, const rgw_io_id& io_id, void *user_info)
{
  std::lock_guard l{lock};
  _complete(cn, io_id, user_info);
}

void RGWCompletionManager::register_completion_notifier(RGWAioCompletionNotifier *cn)
{
  std::lock_guard l{lock};
  if (cn) {
    cns.insert(cn);
  }
}

void RGWCompletionManager::unregister_completion_notifier(RGWAioCompletionNotifier *cn)
{
  std::lock_guard l{lock};
  if (cn) {
    cns.erase(cn);
  }
}

void RGWCompletionManager::_complete(RGWAioCompletionNotifier *cn, const rgw_io_id& io_id, void *user_info)
{
  if (cn) {
    cns.erase(cn);
  }

  if (complete_reqs_set.find(io_id) != complete_reqs_set.end()) {
    /* already have completion for this io_id, don't allow multiple completions for it */
    return;
  }
  complete_reqs.push_back(io_completion{io_id, user_info});
  cond.notify_all();
}

int RGWCompletionManager::get_next(io_completion *io)
{
  std::unique_lock l{lock};
  while (complete_reqs.empty()) {
    if (going_down) {
      return -ECANCELED;
    }
    cond.wait(l);
  }
  *io = complete_reqs.front();
  complete_reqs_set.erase(io->io_id);
  complete_reqs.pop_front();
  return 0;
}

bool RGWCompletionManager::try_get_next(io_completion *io)
{
  std::lock_guard l{lock};
  if (complete_reqs.empty()) {
    return false;
  }
  *io = complete_reqs.front();
  complete_reqs_set.erase(io->io_id);
  complete_reqs.pop_front();
  return true;
}

void RGWCompletionManager::go_down()
{
  std::lock_guard l{lock};
  for (auto cn : cns) {
    cn->unregister();
  }
  going_down = true;
  cond.notify_all();
}

void RGWCompletionManager::wait_interval(void *opaque, const utime_t& interval, void *user_info)
{
  std::lock_guard l{lock};
  ceph_assert(waiters.find(opaque) == waiters.end());
  waiters[opaque] = user_info;
  timer.add_event_after(interval, new WaitContext(this, opaque));
}

void RGWCompletionManager::wakeup(void *opaque)
{
  std::lock_guard l{lock};
  _wakeup(opaque);
}

void RGWCompletionManager::_wakeup(void *opaque)
{
  map<void *, void *>::iterator iter = waiters.find(opaque);
  if (iter != waiters.end()) {
    void *user_id = iter->second;
    waiters.erase(iter);
    _complete(NULL, rgw_io_id{0, -1} /* no IO id */, user_id);
  }
}

RGWCoroutine::~RGWCoroutine() {
  for (auto stack : spawned.entries) {
    stack->put();
  }
}

void RGWCoroutine::init_new_io(RGWIOProvider *io_provider)
{
  ceph_assert(stack); // if there's no stack, io_provider won't be uninitialized
  stack->init_new_io(io_provider);
}

void RGWCoroutine::set_io_blocked(bool flag) {
  if (stack) {
    stack->set_io_blocked(flag);
  }
}

void RGWCoroutine::set_sleeping(bool flag) {
  if (stack) {
    stack->set_sleeping(flag);
  }
}

int RGWCoroutine::io_block(int ret, int64_t io_id) {
  return io_block(ret, rgw_io_id{io_id, -1});
}

int RGWCoroutine::io_block(int ret, const rgw_io_id& io_id) {
  if (!stack) {
    return 0;
  }
  if (stack->consume_io_finish(io_id)) {
    return 0;
  }
  set_io_blocked(true);
  stack->set_io_blocked_id(io_id);
  return ret;
}

void RGWCoroutine::io_complete(const rgw_io_id& io_id) {
  if (stack) {
    stack->io_complete(io_id);
  }
}

void RGWCoroutine::StatusItem::dump(Formatter *f) const {
  ::encode_json("timestamp", timestamp, f);
  ::encode_json("status", status, f);
}

stringstream& RGWCoroutine::Status::set_status()
{
  std::unique_lock l{lock};
  string s = status.str();
  status.str(string());
  if (!timestamp.is_zero()) {
    history.push_back(StatusItem(timestamp, s));
  }
  if (history.size() > (size_t)max_history) {
    history.pop_front();
  }
  timestamp = ceph_clock_now();

  return status;
}

RGWCoroutinesManager::~RGWCoroutinesManager() {
  stop();
  completion_mgr->put();
  if (cr_registry) {
    cr_registry->remove(this);
  }
}

int64_t RGWCoroutinesManager::get_next_io_id()
{
  return (int64_t)++max_io_id;
}

uint64_t RGWCoroutinesManager::get_next_stack_id() {
  return (uint64_t)++max_stack_id;
}

RGWCoroutinesStack::RGWCoroutinesStack(CephContext *_cct, RGWCoroutinesManager *_ops_mgr, RGWCoroutine *start) : cct(_cct), ops_mgr(_ops_mgr),
                                                                                                         done_flag(false), error_flag(false), blocked_flag(false),
                                                                                                         sleep_flag(false), interval_wait_flag(false), is_scheduled(false), is_waiting_for_child(false),
													 retcode(0), run_count(0),
													 env(NULL), parent(NULL)
{
  id = ops_mgr->get_next_stack_id();
  if (start) {
    ops.push_back(start);
  }
  pos = ops.begin();
}

RGWCoroutinesStack::~RGWCoroutinesStack()
{
  for (auto op : ops) {
    op->put();
  }

  for (auto stack : spawned.entries) {
    stack->put();
  }
}

int RGWCoroutinesStack::operate(const DoutPrefixProvider *dpp, RGWCoroutinesEnv *_env)
{
  env = _env;
  RGWCoroutine *op = *pos;
  op->stack = this;
  ldpp_dout(dpp, 20) << *op << ": operate()" << dendl;
  int r = op->operate_wrapper(dpp);
  if (r < 0) {
    ldpp_dout(dpp, 20) << *op << ": operate() returned r=" << r << dendl;
  }

  error_flag = op->is_error();

  if (op->is_done()) {
    int op_retcode = r;
    r = unwind(op_retcode);
    op->put();
    done_flag = (pos == ops.end());
    blocked_flag &= !done_flag;
    if (done_flag) {
      retcode = op_retcode;
    }
    return r;
  }

  /* should r ever be negative at this point? */
  ceph_assert(r >= 0);

  return 0;
}

string RGWCoroutinesStack::error_str()
{
  if (pos != ops.end()) {
    return (*pos)->error_str();
  }
  return string();
}

void RGWCoroutinesStack::call(RGWCoroutine *next_op) {
  if (!next_op) {
    return;
  }
  ops.push_back(next_op);
  if (pos != ops.end()) {
    ++pos;
  } else {
    pos = ops.begin();
  }
}

void RGWCoroutinesStack::schedule()
{
  env->manager->schedule(env, this);
}

void RGWCoroutinesStack::_schedule()
{
  env->manager->_schedule(env, this);
}

RGWCoroutinesStack *RGWCoroutinesStack::spawn(RGWCoroutine *source_op, RGWCoroutine *op, bool wait)
{
  if (!op) {
    return NULL;
  }

  rgw_spawned_stacks *s = (source_op ? &source_op->spawned : &spawned);

  RGWCoroutinesStack *stack = env->manager->allocate_stack();
  s->add_pending(stack);
  stack->parent = this;

  stack->get(); /* we'll need to collect the stack */
  stack->call(op);

  env->manager->schedule(env, stack);

  if (wait) {
    set_blocked_by(stack);
  }

  return stack;
}

RGWCoroutinesStack *RGWCoroutinesStack::spawn(RGWCoroutine *op, bool wait)
{
  return spawn(NULL, op, wait);
}

int RGWCoroutinesStack::wait(const utime_t& interval)
{
  RGWCompletionManager *completion_mgr = env->manager->get_completion_mgr();
  completion_mgr->wait_interval((void *)this, interval, (void *)this);
  set_io_blocked(true);
  set_interval_wait(true);
  return 0;
}

void RGWCoroutinesStack::wakeup()
{
  RGWCompletionManager *completion_mgr = env->manager->get_completion_mgr();
  completion_mgr->wakeup((void *)this);
}

void RGWCoroutinesStack::io_complete(const rgw_io_id& io_id)
{
  RGWCompletionManager *completion_mgr = env->manager->get_completion_mgr();
  completion_mgr->complete(nullptr, io_id, (void *)this);
}

int RGWCoroutinesStack::unwind(int retcode)
{
  rgw_spawned_stacks *src_spawned = &(*pos)->spawned;

  if (pos == ops.begin()) {
    ldout(cct, 15) << "stack " << (void *)this << " end" << dendl;
    spawned.inherit(src_spawned);
    ops.clear();
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

void RGWCoroutinesStack::cancel()
{
  while (!ops.empty()) {
    RGWCoroutine *op = *pos;
    unwind(-ECANCELED);
    op->put();
  }
  put();
}

bool RGWCoroutinesStack::collect(RGWCoroutine *op, int *ret, RGWCoroutinesStack *skip_stack, uint64_t *stack_id) /* returns true if needs to be called again */
{
  bool need_retry = false;
  rgw_spawned_stacks *s = (op ? &op->spawned : &spawned);
  *ret = 0;
  vector<RGWCoroutinesStack *> new_list;

  for (vector<RGWCoroutinesStack *>::iterator iter = s->entries.begin(); iter != s->entries.end(); ++iter) {
    RGWCoroutinesStack *stack = *iter;
    if (stack == skip_stack || !stack->is_done()) {
      new_list.push_back(stack);
      if (!stack->is_done()) {
        ldout(cct, 20) << "collect(): s=" << (void *)this << " stack=" << (void *)stack << " is still running" << dendl;
      } else if (stack == skip_stack) {
        ldout(cct, 20) << "collect(): s=" << (void *)this << " stack=" << (void *)stack << " explicitly skipping stack" << dendl;
      }
      continue;
    }
    if (stack_id) {
      *stack_id = stack->get_id();
    }
    int r = stack->get_ret_status();
    stack->put();
    if (r < 0) {
      *ret = r;
      ldout(cct, 20) << "collect(): s=" << (void *)this << " stack=" << (void *)stack << " encountered error (r=" << r << "), skipping next stacks" << dendl;
      new_list.insert(new_list.end(), ++iter, s->entries.end());
      need_retry = (iter != s->entries.end());
      break;
    }

    ldout(cct, 20) << "collect(): s=" << (void *)this << " stack=" << (void *)stack << " is complete" << dendl;
  }

  s->entries.swap(new_list);
  return need_retry;
}

bool RGWCoroutinesStack::collect_next(RGWCoroutine *op, int *ret, RGWCoroutinesStack **collected_stack) /* returns true if found a stack to collect */
{
  rgw_spawned_stacks *s = (op ? &op->spawned : &spawned);
  *ret = 0;

  if (collected_stack) {
    *collected_stack = NULL;
  }

  for (vector<RGWCoroutinesStack *>::iterator iter = s->entries.begin(); iter != s->entries.end(); ++iter) {
    RGWCoroutinesStack *stack = *iter;
    if (!stack->is_done()) {
      continue;
    }
    int r = stack->get_ret_status();
    if (r < 0) {
      *ret = r;
    }

    if (collected_stack) {
      *collected_stack = stack;
    }
    stack->put();

    s->entries.erase(iter);
    return true;
  }

  return false;
}

bool RGWCoroutinesStack::collect(int *ret, RGWCoroutinesStack *skip_stack, uint64_t  *stack_id) /* returns true if needs to be called again */
{
  return collect(NULL, ret, skip_stack, stack_id);
}

static void _aio_completion_notifier_cb(librados::completion_t cb, void *arg)
{
  (static_cast<RGWAioCompletionNotifier *>(arg))->cb();
}

RGWAioCompletionNotifier::RGWAioCompletionNotifier(RGWCompletionManager *_mgr, const rgw_io_id& _io_id, void *_user_data) : completion_mgr(_mgr),
                                                                         io_id(_io_id),
                                                                         user_data(_user_data), registered(true) {
  c = librados::Rados::aio_create_completion(this, _aio_completion_notifier_cb);
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
  if (!op) {
    return;
  }
  string err = op->error_str();
  if (err.empty()) {
    return;
  }
  lderr(cct) << "ERROR: failed operation: " << op->error_str() << dendl;
}

void RGWCoroutinesStack::dump(Formatter *f) const {
  stringstream ss;
  ss << (void *)this;
  ::encode_json("stack", ss.str(), f);
  ::encode_json("run_count", run_count, f);
  f->open_array_section("ops");
  for (auto& i : ops) {
    encode_json("op", *i, f);
  }
  f->close_section();
}

void RGWCoroutinesStack::init_new_io(RGWIOProvider *io_provider)
{
  io_provider->set_io_user_info((void *)this);
  io_provider->assign_io(env->manager->get_io_id_provider());
}

bool RGWCoroutinesStack::try_io_unblock(const rgw_io_id& io_id)
{
  if (!can_io_unblock(io_id)) {
    auto p = io_finish_ids.emplace(io_id.id, io_id);
    auto& iter = p.first;
    bool inserted = p.second;
    if (!inserted) { /* could not insert, entry already existed, add channel to completion mask */
      iter->second.channels |= io_id.channels;
    }
    return false;
  }

  return true;
}

bool RGWCoroutinesStack::consume_io_finish(const rgw_io_id& io_id)
{
  auto iter = io_finish_ids.find(io_id.id);
  if (iter == io_finish_ids.end()) {
    return false;
  }
  int finish_mask = iter->second.channels;
  bool found = (finish_mask & io_id.channels) != 0;

  finish_mask &= ~(finish_mask & io_id.channels);

  if (finish_mask == 0) {
    io_finish_ids.erase(iter);
  }
  return found;
}


void RGWCoroutinesManager::handle_unblocked_stack(set<RGWCoroutinesStack *>& context_stacks, list<RGWCoroutinesStack *>& scheduled_stacks,
                                                  RGWCompletionManager::io_completion& io, int *blocked_count, int *interval_wait_count)
{
  ceph_assert(ceph_mutex_is_wlocked(lock));
  RGWCoroutinesStack *stack = static_cast<RGWCoroutinesStack *>(io.user_info);
  if (context_stacks.find(stack) == context_stacks.end()) {
    return;
  }
  if (!stack->try_io_unblock(io.io_id)) {
    return;
  }
  if (stack->is_io_blocked()) {
    --(*blocked_count);
    stack->set_io_blocked(false);
    if (stack->is_interval_waiting()) {
      --(*interval_wait_count);
    }
  }
  stack->set_interval_wait(false);
  if (!stack->is_done()) {
    if (!stack->is_scheduled) {
      scheduled_stacks.push_back(stack);
      stack->set_is_scheduled(true);
    }
  } else {
    context_stacks.erase(stack);
    stack->put();
  }
}

void RGWCoroutinesManager::schedule(RGWCoroutinesEnv *env, RGWCoroutinesStack *stack)
{
  std::unique_lock wl{lock};
  _schedule(env, stack);
}

void RGWCoroutinesManager::_schedule(RGWCoroutinesEnv *env, RGWCoroutinesStack *stack)
{
  ceph_assert(ceph_mutex_is_wlocked(lock));
  if (!stack->is_scheduled) {
    env->scheduled_stacks->push_back(stack);
    stack->set_is_scheduled(true);
  }
  set<RGWCoroutinesStack *>& context_stacks = run_contexts[env->run_context];
  context_stacks.insert(stack);
}

void RGWCoroutinesManager::set_sleeping(RGWCoroutine *cr, bool flag)
{
  cr->set_sleeping(flag);
}

void RGWCoroutinesManager::io_complete(RGWCoroutine *cr, const rgw_io_id& io_id)
{
  cr->io_complete(io_id);
}

int RGWCoroutinesManager::run(const DoutPrefixProvider *dpp, list<RGWCoroutinesStack *>& stacks)
{
  int ret = 0;
  int blocked_count = 0;
  int interval_wait_count = 0;
  bool canceled = false; // set on going_down
  RGWCoroutinesEnv env;
  bool op_not_blocked;

  uint64_t run_context = ++run_context_count;

  lock.lock();
  set<RGWCoroutinesStack *>& context_stacks = run_contexts[run_context];
  list<RGWCoroutinesStack *> scheduled_stacks;
  for (auto& st : stacks) {
    context_stacks.insert(st);
    scheduled_stacks.push_back(st);
    st->set_is_scheduled(true);
  }
  env.run_context = run_context;
  env.manager = this;
  env.scheduled_stacks = &scheduled_stacks;

  for (list<RGWCoroutinesStack *>::iterator iter = scheduled_stacks.begin(); iter != scheduled_stacks.end() && !going_down;) {
    RGWCompletionManager::io_completion io;
    RGWCoroutinesStack *stack = *iter;
    ++iter;
    scheduled_stacks.pop_front();

    if (context_stacks.find(stack) == context_stacks.end()) {
      /* stack was probably schedule more than once due to IO, but was since complete */
      goto next;
    }
    env.stack = stack;

    lock.unlock();

    ret = stack->operate(dpp, &env);

    lock.lock();

    stack->set_is_scheduled(false);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "stack->operate() returned ret=" << ret << dendl;
    }

    if (stack->is_error()) {
      report_error(stack);
    }

    op_not_blocked = false;

    if (stack->is_io_blocked()) {
      ldout(cct, 20) << __func__ << ":" << " stack=" << (void *)stack << " is io blocked" << dendl;
      if (stack->is_interval_waiting()) {
        interval_wait_count++;
      }
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
            if (stack->is_interval_waiting()) {
              interval_wait_count++;
            }
	    blocked_count++;
	  } else {
	    s->_schedule();
	  }
	}
      }
      if (stack->parent && stack->parent->waiting_for_child()) {
        stack->parent->set_wait_for_child(false);
        stack->parent->_schedule();
      }
      context_stacks.erase(stack);
      stack->put();
      stack = NULL;
    } else {
      op_not_blocked = true;
      stack->run_count++;
      stack->_schedule();
    }

    if (!op_not_blocked && stack) {
      stack->run_count = 0;
    }

    while (completion_mgr->try_get_next(&io)) {
      handle_unblocked_stack(context_stacks, scheduled_stacks, io, &blocked_count, &interval_wait_count);
    }

    /*
     * only account blocked operations that are not in interval_wait, these are stacks that
     * were put on a wait without any real IO operations. While we mark these as io_blocked,
     * these aren't really waiting for IOs
     */
    while (blocked_count - interval_wait_count >= ops_window) {
      lock.unlock();
      ret = completion_mgr->get_next(&io);
      lock.lock();
      if (ret < 0) {
       ldout(cct, 5) << "completion_mgr.get_next() returned ret=" << ret << dendl;
      }
      handle_unblocked_stack(context_stacks, scheduled_stacks, io, &blocked_count, &interval_wait_count);
    }

next:
    while (scheduled_stacks.empty() && blocked_count > 0) {
      lock.unlock();
      ret = completion_mgr->get_next(&io);
      lock.lock();
      if (ret < 0) {
        ldout(cct, 5) << "completion_mgr.get_next() returned ret=" << ret << dendl;
      }
      if (going_down) {
	ldout(cct, 5) << __func__ << "(): was stopped, exiting" << dendl;
	ret = -ECANCELED;
        canceled = true;
        break;
      }
      handle_unblocked_stack(context_stacks, scheduled_stacks, io, &blocked_count, &interval_wait_count);
      iter = scheduled_stacks.begin();
    }
    if (canceled) {
      break;
    }

    if (iter == scheduled_stacks.end()) {
      iter = scheduled_stacks.begin();
    }
  }

  if (!context_stacks.empty() && !going_down) {
    JSONFormatter formatter(true);
    formatter.open_array_section("context_stacks");
    for (auto& s : context_stacks) {
      ::encode_json("entry", *s, &formatter);
    }
    formatter.close_section();
    lderr(cct) << __func__ << "(): ERROR: deadlock detected, dumping remaining coroutines:\n";
    formatter.flush(*_dout);
    *_dout << dendl;
    ceph_assert(context_stacks.empty() || going_down); // assert on deadlock
  }

  for (auto stack : context_stacks) {
    ldout(cct, 20) << "clearing stack on run() exit: stack=" << (void *)stack << " nref=" << stack->get_nref() << dendl;
    stack->cancel();
  }
  run_contexts.erase(run_context);
  lock.unlock();

  return ret;
}

int RGWCoroutinesManager::run(const DoutPrefixProvider *dpp, RGWCoroutine *op)
{
  if (!op) {
    return 0;
  }
  list<RGWCoroutinesStack *> stacks;
  RGWCoroutinesStack *stack = allocate_stack();
  op->get();
  stack->call(op);

  stacks.push_back(stack);

  int r = run(dpp, stacks);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "run(stacks) returned r=" << r << dendl;
  } else {
    r = op->get_ret_status();
  }
  op->put();

  return r;
}

RGWAioCompletionNotifier *RGWCoroutinesManager::create_completion_notifier(RGWCoroutinesStack *stack)
{
  rgw_io_id io_id{get_next_io_id(), -1};
  RGWAioCompletionNotifier *cn = new RGWAioCompletionNotifier(completion_mgr, io_id, (void *)stack);
  completion_mgr->register_completion_notifier(cn);
  return cn;
}

void RGWCoroutinesManager::dump(Formatter *f) const {
  std::shared_lock rl{lock};

  f->open_array_section("run_contexts");
  for (auto& i : run_contexts) {
    f->open_object_section("context");
    ::encode_json("id", i.first, f);
    f->open_array_section("entries");
    for (auto& s : i.second) {
      ::encode_json("entry", *s, f);
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
}

RGWCoroutinesStack *RGWCoroutinesManager::allocate_stack() {
  return new RGWCoroutinesStack(cct, this);
}

string RGWCoroutinesManager::get_id()
{
  if (!id.empty()) {
    return id;
  }
  stringstream ss;
  ss << (void *)this;
  return ss.str();
}

void RGWCoroutinesManagerRegistry::add(RGWCoroutinesManager *mgr)
{
  std::unique_lock wl{lock};
  if (managers.find(mgr) == managers.end()) {
    managers.insert(mgr);
    get();
  }
}

void RGWCoroutinesManagerRegistry::remove(RGWCoroutinesManager *mgr)
{
  std::unique_lock wl{lock};
  if (managers.find(mgr) != managers.end()) {
    managers.erase(mgr);
    put();
  }
}

RGWCoroutinesManagerRegistry::~RGWCoroutinesManagerRegistry()
{
  AdminSocket *admin_socket = cct->get_admin_socket();
  if (!admin_command.empty()) {
    admin_socket->unregister_commands(this);
  }
}

int RGWCoroutinesManagerRegistry::hook_to_admin_command(const string& command)
{
  AdminSocket *admin_socket = cct->get_admin_socket();
  if (!admin_command.empty()) {
    admin_socket->unregister_commands(this);
  }
  admin_command = command;
  int r = admin_socket->register_command(admin_command, this,
				     "dump current coroutines stack state");
  if (r < 0) {
    lderr(cct) << "ERROR: fail to register admin socket command (r=" << r << ")" << dendl;
    return r;
  }
  return 0;
}

int RGWCoroutinesManagerRegistry::call(std::string_view command,
				       const cmdmap_t& cmdmap,
				       const bufferlist&,
				       Formatter *f,
				       std::ostream& ss,
				       bufferlist& out) {
  std::shared_lock rl{lock};
  ::encode_json("cr_managers", *this, f);
  return 0;
}

void RGWCoroutinesManagerRegistry::dump(Formatter *f) const {
  f->open_array_section("coroutine_managers");
  for (auto m : managers) {
    ::encode_json("entry", *m, f);
  }
  f->close_section();
}

void RGWCoroutine::call(RGWCoroutine *op)
{
  if (op) {
    stack->call(op);
  } else {
    // the call()er expects this to set a retcode
    retcode = 0;
  }
}

RGWCoroutinesStack *RGWCoroutine::spawn(RGWCoroutine *op, bool wait)
{
  return stack->spawn(this, op, wait);
}

bool RGWCoroutine::collect(int *ret, RGWCoroutinesStack *skip_stack, uint64_t *stack_id) /* returns true if needs to be called again */
{
  return stack->collect(this, ret, skip_stack, stack_id);
}

bool RGWCoroutine::collect_next(int *ret, RGWCoroutinesStack **collected_stack) /* returns true if found a stack to collect */
{
  return stack->collect_next(this, ret, collected_stack);
}

int RGWCoroutine::wait(const utime_t& interval)
{
  return stack->wait(interval);
}

void RGWCoroutine::wait_for_child()
{
  /* should only wait for child if there is a child that is not done yet, and no complete children */
  if (spawned.entries.empty()) {
    return;
  }
  for (vector<RGWCoroutinesStack *>::iterator iter = spawned.entries.begin(); iter != spawned.entries.end(); ++iter) {
    if ((*iter)->is_done()) {
      return;
    }
  }
  stack->set_wait_for_child(true);
}

string RGWCoroutine::to_str() const
{
  return typeid(*this).name();
}

ostream& operator<<(ostream& out, const RGWCoroutine& cr)
{
  out << "cr:s=" << (void *)cr.get_stack() << ":op=" << (void *)&cr << ":" << typeid(cr).name();
  return out;
}

bool RGWCoroutine::drain_children(int num_cr_left,
                                  RGWCoroutinesStack *skip_stack,
                                  std::optional<std::function<void(uint64_t stack_id, int ret)> > cb)
{
  bool done = false;
  ceph_assert(num_cr_left >= 0);
  if (num_cr_left == 0 && skip_stack) {
    num_cr_left = 1;
  }
  reenter(&drain_status.cr) {
    while (num_spawned() > (size_t)num_cr_left) {
      yield wait_for_child();
      int ret;
      uint64_t stack_id;
      bool again = false;
      do {
        again = collect(&ret, skip_stack, &stack_id);
        if (ret < 0) {
            ldout(cct, 10) << "collect() returned ret=" << ret << dendl;
            /* we should have reported this error */
            log_error() << "ERROR: collect() returned error (ret=" << ret << ")";
        }
        if (cb) {
          (*cb)(stack_id, ret);
        }
      } while (again);
    }
    done = true;
  }
  return done;
}

bool RGWCoroutine::drain_children(int num_cr_left,
                                  std::optional<std::function<int(uint64_t stack_id, int ret)> > cb)
{
  bool done = false;
  ceph_assert(num_cr_left >= 0);

  reenter(&drain_status.cr) {
    while (num_spawned() > (size_t)num_cr_left) {
      yield wait_for_child();
      int ret;
      uint64_t stack_id;
      bool again = false;
      do {
        again = collect(&ret, nullptr, &stack_id);
        if (ret < 0) {
          ldout(cct, 10) << "collect() returned ret=" << ret << dendl;
          /* we should have reported this error */
          log_error() << "ERROR: collect() returned error (ret=" << ret << ")";
        }
        if (cb && !drain_status.should_exit) {
          int r = (*cb)(stack_id, ret);
          if (r < 0) {
            drain_status.ret = r;
            drain_status.should_exit = true;
            num_cr_left = 0; /* need to drain all */
          }
        }
      } while (again);
    }
    done = true;
  }
  return done;
}

void RGWCoroutine::wakeup()
{
  if (stack) {
    stack->wakeup();
  }
}

RGWCoroutinesEnv *RGWCoroutine::get_env() const
{
  return stack->get_env();
}

void RGWCoroutine::dump(Formatter *f) const {
  if (!description.str().empty()) {
    encode_json("description", description.str(), f);
  }
  encode_json("type", to_str(), f);
  if (!spawned.entries.empty()) {
    f->open_array_section("spawned");
    for (auto& i : spawned.entries) {
      char buf[32];
      snprintf(buf, sizeof(buf), "%p", (void *)i);
      encode_json("stack", string(buf), f);
    }
    f->close_section();
  }
  if (!status.history.empty()) {
    encode_json("history", status.history, f);
  }

  if (!status.status.str().empty()) {
    f->open_object_section("status");
    encode_json("status", status.status.str(), f);
    encode_json("timestamp", status.timestamp, f);
    f->close_section();
  }
}

RGWSimpleCoroutine::~RGWSimpleCoroutine()
{
  if (!called_cleanup) {
    request_cleanup();
  }
}

void RGWSimpleCoroutine::call_cleanup()
{
  called_cleanup = true;
  request_cleanup();
}

int RGWSimpleCoroutine::operate(const DoutPrefixProvider *dpp)
{
  int ret = 0;
  reenter(this) {
    yield return state_init();

    for (tries = 0; tries < max_eio_retries; tries++) {
      yield return state_send_request(dpp);
      yield return state_request_complete();

      if (op_ret == -EIO && tries < max_eio_retries - 1) {
        ldout(cct, 20) << "request IO error. retries=" << tries << dendl;
        continue;
      } else if (op_ret < 0) {
        call_cleanup();
        return set_state(RGWCoroutine_Error, op_ret);
      }
      break;
    }

    yield return state_all_complete();
    drain_all();
    call_cleanup();
    return set_state(RGWCoroutine_Done, ret);
  }
  return 0;
}

int RGWSimpleCoroutine::state_init()
{
  int ret = init();
  if (ret < 0) {
    call_cleanup();
    return set_state(RGWCoroutine_Error, ret);
  }
  return 0;
}

int RGWSimpleCoroutine::state_send_request(const DoutPrefixProvider *dpp)
{
  int ret = send_request(dpp);
  if (ret < 0) {
    call_cleanup();
    return set_state(RGWCoroutine_Error, ret);
  }
  return io_block(0);
}

int RGWSimpleCoroutine::state_request_complete()
{
  op_ret = request_complete();
  if (op_ret < 0 && op_ret != -EIO) {
    call_cleanup();
    return set_state(RGWCoroutine_Error, op_ret);
  }
  return 0;
}

int RGWSimpleCoroutine::state_all_complete()
{
  int ret = finish();
  if (ret < 0) {
    call_cleanup();
    return set_state(RGWCoroutine_Error, ret);
  }
  return 0;
}


