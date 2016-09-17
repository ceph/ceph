

#include "common/ceph_json.h"

#include "rgw_coroutine.h"
#include "rgw_boost_asio_yield.h"


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

void RGWCompletionManager::complete(RGWAioCompletionNotifier *cn, void *user_info)
{
  Mutex::Locker l(lock);
  _complete(cn, user_info);
}

void RGWCompletionManager::register_completion_notifier(RGWAioCompletionNotifier *cn)
{
  Mutex::Locker l(lock);
  if (cn) {
    cns.insert(cn);
    cn->get();
  }
}

void RGWCompletionManager::unregister_completion_notifier(RGWAioCompletionNotifier *cn)
{
  Mutex::Locker l(lock);
  if (cn) {
    cns.erase(cn);
    cn->put();
  }
}

void RGWCompletionManager::_complete(RGWAioCompletionNotifier *cn, void *user_info)
{
  if (cn) {
    cns.erase(cn);
    cn->put();
  }
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
  for (auto cn : cns) {
    cn->unregister();
  }
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
    _complete(NULL, user_id);
  }
}

RGWCoroutine::~RGWCoroutine() {
  for (auto stack : spawned.entries) {
    stack->put();
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

void RGWCoroutine::StatusItem::dump(Formatter *f) const {
  ::encode_json("timestamp", timestamp, f);
  ::encode_json("status", status, f);
}

stringstream& RGWCoroutine::Status::set_status()
{
  RWLock::WLocker l(lock);
  string s = status.str();
  status.str(string());
  if (!timestamp.is_zero()) {
    history.push_back(StatusItem(timestamp, s));
  }
  if (history.size() > (size_t)max_history) {
    history.pop_front();
  }
  timestamp = ceph_clock_now(cct);

  return status;
}

RGWCoroutinesStack::RGWCoroutinesStack(CephContext *_cct, RGWCoroutinesManager *_ops_mgr, RGWCoroutine *start) : cct(_cct), ops_mgr(_ops_mgr),
                                                                                                         done_flag(false), error_flag(false), blocked_flag(false),
                                                                                                         sleep_flag(false), interval_wait_flag(false), is_scheduled(false), is_waiting_for_child(false),
													 retcode(0), run_count(0),
													 env(NULL), parent(NULL)
{
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

int RGWCoroutinesStack::operate(RGWCoroutinesEnv *_env)
{
  env = _env;
  RGWCoroutine *op = *pos;
  op->stack = this;
  ldout(cct, 20) << *op << ": operate()" << dendl;
  int r = op->operate();
  if (r < 0) {
    ldout(cct, 20) << *op << ": operate() returned r=" << r << dendl;
  }

  error_flag = op->is_error();

  if (op->is_done()) {
    int op_retcode = r;
    r = unwind(op_retcode);
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

int RGWCoroutinesStack::unwind(int retcode)
{
  rgw_spawned_stacks *src_spawned = &(*pos)->spawned;

  if (pos == ops.begin()) {
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


bool RGWCoroutinesStack::collect(RGWCoroutine *op, int *ret, RGWCoroutinesStack *skip_stack) /* returns true if needs to be called again */
{
  bool done = true;
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
        ldout(cct, 20) << "collect(): s=" << (void *)this << " stack=" << (void *)stack << " explicitily skipping stack" << dendl;
      }
      continue;
    }
    int r = stack->get_ret_status();
    stack->put();
    if (r < 0) {
      *ret = r;
      ldout(cct, 20) << "collect(): s=" << (void *)this << " stack=" << (void *)stack << " encountered error (r=" << r << "), skipping next stacks" << dendl;
      new_list.insert(new_list.end(), ++iter, s->entries.end());
      done &= (iter != s->entries.end());
      break;
    }

    ldout(cct, 20) << "collect(): s=" << (void *)this << " stack=" << (void *)stack << " is complete" << dendl;
  }

  s->entries.swap(new_list);
  return (!done);
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

bool RGWCoroutinesStack::collect(int *ret, RGWCoroutinesStack *skip_stack) /* returns true if needs to be called again */
{
  return collect(NULL, ret, skip_stack);
}

static void _aio_completion_notifier_cb(librados::completion_t cb, void *arg);

static void _aio_completion_notifier_cb(librados::completion_t cb, void *arg)
{
  ((RGWAioCompletionNotifier *)arg)->cb();
}

RGWAioCompletionNotifier::RGWAioCompletionNotifier(RGWCompletionManager *_mgr, void *_user_data) : completion_mgr(_mgr),
                                                                         user_data(_user_data), lock("RGWAioCompletionNotifier"), registered(true) {
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

void RGWCoroutinesManager::handle_unblocked_stack(set<RGWCoroutinesStack *>& context_stacks, list<RGWCoroutinesStack *>& scheduled_stacks, RGWCoroutinesStack *stack, int *blocked_count)
{
  RWLock::WLocker wl(lock);
  --(*blocked_count);
  stack->set_io_blocked(false);
  stack->set_interval_wait(false);
  if (!stack->is_done()) {
    scheduled_stacks.push_back(stack);
  } else {
    RWLock::WLocker wl(lock);
    context_stacks.erase(stack);
    stack->put();
  }
}

void RGWCoroutinesManager::schedule(RGWCoroutinesEnv *env, RGWCoroutinesStack *stack)
{
  assert(lock.is_wlocked());
  env->scheduled_stacks->push_back(stack);
  set<RGWCoroutinesStack *>& context_stacks = run_contexts[env->run_context];
  context_stacks.insert(stack);
}

int RGWCoroutinesManager::run(list<RGWCoroutinesStack *>& stacks)
{
  int ret = 0;
  int blocked_count = 0;
  int interval_wait_count = 0;
  RGWCoroutinesEnv env;

  uint64_t run_context = run_context_count.inc();

  lock.get_write();
  set<RGWCoroutinesStack *>& context_stacks = run_contexts[run_context];
  list<RGWCoroutinesStack *> scheduled_stacks;
  for (auto& st : stacks) {
    context_stacks.insert(st);
    scheduled_stacks.push_back(st);
  }
  lock.unlock();

  env.run_context = run_context;
  env.manager = this;
  env.scheduled_stacks = &scheduled_stacks;

  for (list<RGWCoroutinesStack *>::iterator iter = scheduled_stacks.begin(); iter != scheduled_stacks.end() && !going_down.read();) {
    lock.get_write();

    RGWCoroutinesStack *stack = *iter;
    env.stack = stack;

    ret = stack->operate(&env);
    stack->set_is_scheduled(false);
    if (ret < 0) {
      ldout(cct, 20) << "stack->operate() returned ret=" << ret << dendl;
    }

    if (stack->is_error()) {
      report_error(stack);
    }

    bool op_not_blocked = false;

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
	    s->schedule();
	  }
	}
      }
      if (stack->parent && stack->parent->waiting_for_child()) {
        stack->parent->set_wait_for_child(false);
        stack->parent->schedule();
      }
      context_stacks.erase(stack);
      stack->put();
      stack = NULL;
    } else {
      op_not_blocked = true;
      stack->run_count++;
      stack->schedule();
    }

    if (!op_not_blocked && stack) {
      stack->run_count = 0;
    }

    lock.unlock();

    RGWCoroutinesStack *blocked_stack;
    while (completion_mgr->try_get_next((void **)&blocked_stack)) {
      handle_unblocked_stack(context_stacks, scheduled_stacks, blocked_stack, &blocked_count);
    }

    /*
     * only account blocked operations that are not in interval_wait, these are stacks that
     * were put on a wait without any real IO operations. While we mark these as io_blocked,
     * these aren't really waiting for IOs
     */
    while (blocked_count - interval_wait_count >= ops_window) {
      ret = completion_mgr->get_next((void **)&blocked_stack);
      if (ret < 0) {
       ldout(cct, 0) << "ERROR: failed to clone shard, completion_mgr.get_next() returned ret=" << ret << dendl;
      }
      handle_unblocked_stack(context_stacks, scheduled_stacks, blocked_stack, &blocked_count);
    }

    ++iter;
    scheduled_stacks.pop_front();


    while (scheduled_stacks.empty() && blocked_count > 0) {
      ret = completion_mgr->get_next((void **)&blocked_stack);
      if (ret < 0) {
	ldout(cct, 0) << "ERROR: failed to clone shard, completion_mgr.get_next() returned ret=" << ret << dendl;
      }
      if (going_down.read() > 0) {
	ldout(cct, 5) << __func__ << "(): was stopped, exiting" << dendl;
	ret = -ECANCELED;
        break;
      }
      handle_unblocked_stack(context_stacks, scheduled_stacks, blocked_stack, &blocked_count);
      iter = scheduled_stacks.begin();
    }
    if (ret == -ECANCELED) {
      break;
    }

    if (iter == scheduled_stacks.end()) {
      iter = scheduled_stacks.begin();
    }
  }

  lock.get_write();
  assert(context_stacks.empty() || going_down.read()); // assert on deadlock
  for (auto stack : context_stacks) {
    ldout(cct, 20) << "clearing stack on run() exit: stack=" << (void *)stack << " nref=" << stack->get_nref() << dendl;
    stack->put();
  }
  run_contexts.erase(run_context);
  lock.unlock();

  return ret;
}

int RGWCoroutinesManager::run(RGWCoroutine *op)
{
  if (!op) {
    return 0;
  }
  list<RGWCoroutinesStack *> stacks;
  RGWCoroutinesStack *stack = allocate_stack();
  op->get();
  stack->call(op);

  stack->schedule(&stacks);

  int r = run(stacks);
  if (r < 0) {
    ldout(cct, 20) << "run(stacks) returned r=" << r << dendl;
  } else {
    r = op->get_ret_status();
  }
  op->put();

  return r;
}

RGWAioCompletionNotifier *RGWCoroutinesManager::create_completion_notifier(RGWCoroutinesStack *stack)
{
  RGWAioCompletionNotifier *cn = new RGWAioCompletionNotifier(completion_mgr, (void *)stack);
  completion_mgr->register_completion_notifier(cn);
  return cn;
}

void RGWCoroutinesManager::dump(Formatter *f) const {
  RWLock::RLocker rl(lock);

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
  RWLock::WLocker wl(lock);
  if (managers.find(mgr) == managers.end()) {
    managers.insert(mgr);
    get();
  }
}

void RGWCoroutinesManagerRegistry::remove(RGWCoroutinesManager *mgr)
{
  RWLock::WLocker wl(lock);
  if (managers.find(mgr) != managers.end()) {
    managers.erase(mgr);
    put();
  }
}

RGWCoroutinesManagerRegistry::~RGWCoroutinesManagerRegistry()
{
  AdminSocket *admin_socket = cct->get_admin_socket();
  if (!admin_command.empty()) {
    admin_socket->unregister_command(admin_command);
  }
}

int RGWCoroutinesManagerRegistry::hook_to_admin_command(const string& command)
{
  AdminSocket *admin_socket = cct->get_admin_socket();
  if (!admin_command.empty()) {
    admin_socket->unregister_command(admin_command);
  }
  admin_command = command;
  int r = admin_socket->register_command(admin_command, admin_command, this,
				     "dump current coroutines stack state");
  if (r < 0) {
    lderr(cct) << "ERROR: fail to register admin socket command (r=" << r << ")" << dendl;
    return r;
  }
  return 0;
}

bool RGWCoroutinesManagerRegistry::call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {
  RWLock::RLocker rl(lock);
  stringstream ss;
  JSONFormatter f;
  ::encode_json("cr_managers", *this, &f);
  f.flush(ss);
  out.append(ss);
  return true;
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
  stack->call(op);
}

RGWCoroutinesStack *RGWCoroutine::spawn(RGWCoroutine *op, bool wait)
{
  return stack->spawn(this, op, wait);
}

bool RGWCoroutine::collect(int *ret, RGWCoroutinesStack *skip_stack) /* returns true if needs to be called again */
{
  return stack->collect(this, ret, skip_stack);
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

bool RGWCoroutine::drain_children(int num_cr_left, RGWCoroutinesStack *skip_stack)
{
  bool done = false;
  assert(num_cr_left >= 0);
  if (num_cr_left == 0 && skip_stack) {
    num_cr_left = 1;
  }
  reenter(&drain_cr) {
    while (num_spawned() > (size_t)num_cr_left) {
      yield wait_for_child();
      int ret;
      while (collect(&ret, skip_stack)) {
        if (ret < 0) {
          ldout(cct, 10) << "collect() returned ret=" << ret << dendl;
          /* we should have reported this error */
          log_error() << "ERROR: collect() returned error (ret=" << ret << ")";
        }
      }
    }
    done = true;
  }
  return done;
}

void RGWCoroutine::wakeup()
{
  stack->wakeup();
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

int RGWSimpleCoroutine::operate()
{
  int ret = 0;
  reenter(this) {
    yield return state_init();
    yield return state_send_request();
    yield return state_request_complete();
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

int RGWSimpleCoroutine::state_send_request()
{
  int ret = send_request();
  if (ret < 0) {
    call_cleanup();
    return set_state(RGWCoroutine_Error, ret);
  }
  return io_block(0);
}

int RGWSimpleCoroutine::state_request_complete()
{
  int ret = request_complete();
  if (ret < 0) {
    call_cleanup();
    return set_state(RGWCoroutine_Error, ret);
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


