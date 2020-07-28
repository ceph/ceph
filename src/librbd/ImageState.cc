// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ImageState.h"
#include "include/rbd/librbd.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "common/WorkQueue.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/OpenRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/image/SetSnapRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageState: " << this << " "

namespace librbd {

using util::create_async_context_callback;
using util::create_context_callback;

class ImageUpdateWatchers {
public:

  explicit ImageUpdateWatchers(CephContext *cct) : m_cct(cct),
    m_lock(ceph::make_mutex(util::unique_lock_name("librbd::ImageUpdateWatchers::m_lock", this))) {
  }

  ~ImageUpdateWatchers() {
    ceph_assert(m_watchers.empty());
    ceph_assert(m_in_flight.empty());
    ceph_assert(m_pending_unregister.empty());
    ceph_assert(m_on_shut_down_finish == nullptr);

    destroy_work_queue();
  }

  void flush(Context *on_finish) {
    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__ << dendl;
    {
      std::lock_guard locker{m_lock};
      if (!m_in_flight.empty()) {
	Context *ctx = new LambdaContext(
	  [this, on_finish](int r) {
	    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__
	                     << ": completing flush" << dendl;
	    on_finish->complete(r);
	  });
	m_work_queue->queue(ctx, 0);
	return;
      }
    }
    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__
		     << ": completing flush" << dendl;
    on_finish->complete(0);
  }

  void shut_down(Context *on_finish) {
    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__ << dendl;
    {
      std::lock_guard locker{m_lock};
      ceph_assert(m_on_shut_down_finish == nullptr);
      m_watchers.clear();
      if (!m_in_flight.empty()) {
	m_on_shut_down_finish = on_finish;
	return;
      }
    }
    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__
		     << ": completing shut down" << dendl;
    on_finish->complete(0);
  }

  void register_watcher(UpdateWatchCtx *watcher, uint64_t *handle) {
    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__ << ": watcher="
                     << watcher << dendl;

    std::lock_guard locker{m_lock};
    ceph_assert(m_on_shut_down_finish == nullptr);

    create_work_queue();

    *handle = m_next_handle++;
    m_watchers.insert(std::make_pair(*handle, watcher));
  }

  void unregister_watcher(uint64_t handle, Context *on_finish) {
    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__ << ": handle="
		     << handle << dendl;
    int r = 0;
    {
      std::lock_guard locker{m_lock};
      auto it = m_watchers.find(handle);
      if (it == m_watchers.end()) {
	r = -ENOENT;
      } else {
	if (m_in_flight.find(handle) != m_in_flight.end()) {
	  ceph_assert(m_pending_unregister.find(handle) == m_pending_unregister.end());
	  m_pending_unregister[handle] = on_finish;
	  on_finish = nullptr;
	}
	m_watchers.erase(it);
      }
    }

    if (on_finish) {
      ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__
		       << ": completing unregister" << dendl;
      on_finish->complete(r);
    }
  }

  void notify() {
    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__ << dendl;

    std::lock_guard locker{m_lock};
    for (auto it : m_watchers) {
      send_notify(it.first, it.second);
    }
  }

  void send_notify(uint64_t handle, UpdateWatchCtx *watcher) {
    ceph_assert(ceph_mutex_is_locked(m_lock));

    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__ << ": handle="
		     << handle << ", watcher=" << watcher << dendl;

    m_in_flight.insert(handle);

    Context *ctx = new LambdaContext(
      [this, handle, watcher](int r) {
	handle_notify(handle, watcher);
      });

    m_work_queue->queue(ctx, 0);
  }

  void handle_notify(uint64_t handle, UpdateWatchCtx *watcher) {

    ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__ << ": handle="
		     << handle << ", watcher=" << watcher << dendl;

    watcher->handle_notify();

    Context *on_unregister_finish = nullptr;
    Context *on_shut_down_finish = nullptr;

    {
      std::lock_guard locker{m_lock};

      auto in_flight_it = m_in_flight.find(handle);
      ceph_assert(in_flight_it != m_in_flight.end());
      m_in_flight.erase(in_flight_it);

      // If there is no more in flight notifications for this watcher
      // and it is pending unregister, complete it now.
      if (m_in_flight.find(handle) == m_in_flight.end()) {
	auto it = m_pending_unregister.find(handle);
	if (it != m_pending_unregister.end()) {
	  on_unregister_finish = it->second;
	  m_pending_unregister.erase(it);
	}
      }

      if (m_in_flight.empty()) {
	ceph_assert(m_pending_unregister.empty());
	if (m_on_shut_down_finish != nullptr) {
	  std::swap(m_on_shut_down_finish, on_shut_down_finish);
	}
      }
    }

    if (on_unregister_finish != nullptr) {
      ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__
		       << ": completing unregister" << dendl;
      on_unregister_finish->complete(0);
    }

    if (on_shut_down_finish != nullptr) {
      ldout(m_cct, 20) << "ImageUpdateWatchers::" << __func__
		       << ": completing shut down" << dendl;
      on_shut_down_finish->complete(0);
    }
  }

private:
  class ThreadPoolSingleton : public ThreadPool {
  public:
    explicit ThreadPoolSingleton(CephContext *cct)
      : ThreadPool(cct, "librbd::ImageUpdateWatchers::thread_pool", "tp_librbd",
		   1) {
      start();
    }
    ~ThreadPoolSingleton() override {
      stop();
    }
  };

  CephContext *m_cct;
  ceph::mutex m_lock;
  ContextWQ *m_work_queue = nullptr;
  std::map<uint64_t, UpdateWatchCtx*> m_watchers;
  uint64_t m_next_handle = 0;
  std::multiset<uint64_t> m_in_flight;
  std::map<uint64_t, Context*> m_pending_unregister;
  Context *m_on_shut_down_finish = nullptr;

  void create_work_queue() {
    if (m_work_queue != nullptr) {
      return;
    }
    auto& thread_pool = m_cct->lookup_or_create_singleton_object<
      ThreadPoolSingleton>("librbd::ImageUpdateWatchers::thread_pool",
			   false, m_cct);
    m_work_queue = new ContextWQ("librbd::ImageUpdateWatchers::work_queue",
				 m_cct->_conf.get_val<uint64_t>("rbd_op_thread_timeout"),
				 &thread_pool);
  }

  void destroy_work_queue() {
    if (m_work_queue == nullptr) {
      return;
    }
    m_work_queue->drain();
    delete m_work_queue;
  }
};

class QuiesceWatchers {
public:
  explicit QuiesceWatchers(CephContext *cct, asio::ContextWQ* work_queue)
    : m_cct(cct),
      m_work_queue(work_queue),
      m_lock(ceph::make_mutex(util::unique_lock_name(
        "librbd::QuiesceWatchers::m_lock", this))) {
  }

  ~QuiesceWatchers() {
    ceph_assert(m_pending_unregister.empty());
    ceph_assert(m_on_notify == nullptr);
  }

  void register_watcher(QuiesceWatchCtx *watcher, uint64_t *handle) {
    ldout(m_cct, 20) << "QuiesceWatchers::" << __func__ << ": watcher="
                     << watcher << dendl;

    std::lock_guard locker{m_lock};

    *handle = m_next_handle++;
    m_watchers[*handle] = watcher;
  }

  void unregister_watcher(uint64_t handle, Context *on_finish) {
    int r = 0;
    {
      std::lock_guard locker{m_lock};
      auto it = m_watchers.find(handle);
      if (it == m_watchers.end()) {
        r = -ENOENT;
      } else {
        if (m_on_notify != nullptr) {
          ceph_assert(!m_pending_unregister.count(handle));
          m_pending_unregister[handle] = on_finish;
          on_finish = nullptr;
        }
        m_watchers.erase(it);
      }
    }

    if (on_finish) {
      ldout(m_cct, 20) << "QuiesceWatchers::" << __func__
                       << ": completing unregister " << handle << dendl;
      on_finish->complete(r);
    }
  }

  void notify_quiesce(Context *on_finish) {
    std::lock_guard locker{m_lock};
    if (m_on_notify != nullptr) {
      m_pending_notify.push_back(on_finish);
      return;
    }

    notify(QUIESCE, on_finish);
  }

  void notify_unquiesce(Context *on_finish) {
    std::lock_guard locker{m_lock};

    notify(UNQUIESCE, on_finish);
  }

  void quiesce_complete(int r) {
    Context *on_notify = nullptr;
    {
      std::lock_guard locker{m_lock};
      ceph_assert(m_on_notify != nullptr);
      ceph_assert(m_handle_quiesce_cnt > 0);

      m_handle_quiesce_cnt--;

      if (m_handle_quiesce_cnt > 0) {
        return;
      }

      std::swap(on_notify, m_on_notify);
    }

    on_notify->complete(r);
  }

private:
  enum EventType {QUIESCE, UNQUIESCE};

  CephContext *m_cct;
  asio::ContextWQ *m_work_queue;

  ceph::mutex m_lock;
  std::map<uint64_t, QuiesceWatchCtx*> m_watchers;
  uint64_t m_next_handle = 0;
  Context *m_on_notify = nullptr;
  std::list<Context *> m_pending_notify;
  std::map<uint64_t, Context*> m_pending_unregister;
  uint64_t m_handle_quiesce_cnt = 0;

  void notify(EventType event_type, Context *on_finish) {
    ceph_assert(ceph_mutex_is_locked(m_lock));

    if (m_watchers.empty()) {
      m_work_queue->queue(on_finish);
      return;
    }

    ldout(m_cct, 20) << "QuiesceWatchers::" << __func__ << " event: "
                     << event_type << dendl;

    Context *ctx = nullptr;
    if (event_type == UNQUIESCE) {
      ctx = create_async_context_callback(
        m_work_queue, create_context_callback<
          QuiesceWatchers, &QuiesceWatchers::handle_notify_unquiesce>(this));
    }
    auto gather_ctx = new C_Gather(m_cct, ctx);

    ceph_assert(m_on_notify == nullptr);
    ceph_assert(m_handle_quiesce_cnt == 0);

    m_on_notify = on_finish;

    for (auto it : m_watchers) {
      send_notify(it.first, it.second, event_type, gather_ctx->new_sub());
    }

    gather_ctx->activate();
  }

  void send_notify(uint64_t handle, QuiesceWatchCtx *watcher,
                   EventType event_type, Context *on_finish) {
    auto ctx = new LambdaContext(
      [this, handle, watcher, event_type, on_finish](int) {
        ldout(m_cct, 20) << "QuiesceWatchers::" << __func__ << ": handle="
                         << handle << ", event_type=" << event_type << dendl;
        switch (event_type) {
        case QUIESCE:
          m_handle_quiesce_cnt++;
          watcher->handle_quiesce();
          break;
        case UNQUIESCE:
          watcher->handle_unquiesce();
          break;
        default:
          ceph_abort_msgf("invalid event_type %d", event_type);
        }

        on_finish->complete(0);
      });

    m_work_queue->queue(ctx);
  }

  void handle_notify_unquiesce(int r) {
    ldout(m_cct, 20) << "QuiesceWatchers::" << __func__ << ": r=" << r
                     << dendl;

    ceph_assert(r == 0);

    std::unique_lock locker{m_lock};

    if (!m_pending_unregister.empty()) {
      std::map<uint64_t, Context*> pending_unregister;
      std::swap(pending_unregister, m_pending_unregister);
      locker.unlock();
      for (auto &it : pending_unregister) {
        ldout(m_cct, 20) << "QuiesceWatchers::" << __func__
                         << ": completing unregister " << it.first << dendl;
        it.second->complete(0);
      }
      locker.lock();
    }

    Context *on_notify = nullptr;
    std::swap(on_notify, m_on_notify);

    if (!m_pending_notify.empty()) {
      auto on_finish = m_pending_notify.front();
      m_pending_notify.pop_front();
      notify(QUIESCE, on_finish);
    }

    locker.unlock();
    on_notify->complete(0);
  }
};

template <typename I>
ImageState<I>::ImageState(I *image_ctx)
  : m_image_ctx(image_ctx), m_state(STATE_UNINITIALIZED),
    m_lock(ceph::make_mutex(util::unique_lock_name("librbd::ImageState::m_lock", this))),
    m_last_refresh(0), m_refresh_seq(0),
    m_update_watchers(new ImageUpdateWatchers(image_ctx->cct)),
    m_quiesce_watchers(new QuiesceWatchers(
      image_ctx->cct, image_ctx->asio_engine->get_work_queue())) {
}

template <typename I>
ImageState<I>::~ImageState() {
  ceph_assert(m_state == STATE_UNINITIALIZED || m_state == STATE_CLOSED);
  delete m_update_watchers;
  delete m_quiesce_watchers;
}

template <typename I>
int ImageState<I>::open(uint64_t flags) {
  C_SaferCond ctx;
  open(flags, &ctx);

  int r = ctx.wait();
  if (r < 0) {
    delete m_image_ctx;
  }
  return r;
}

template <typename I>
void ImageState<I>::open(uint64_t flags, Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_lock.lock();
  ceph_assert(m_state == STATE_UNINITIALIZED);
  m_open_flags = flags;

  Action action(ACTION_TYPE_OPEN);
  action.refresh_seq = m_refresh_seq;

  execute_action_unlock(action, on_finish);
}

template <typename I>
int ImageState<I>::close() {
  C_SaferCond ctx;
  close(&ctx);

  int r = ctx.wait();
  delete m_image_ctx;
  return r;
}

template <typename I>
void ImageState<I>::close(Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_lock.lock();
  ceph_assert(!is_closed());

  Action action(ACTION_TYPE_CLOSE);
  action.refresh_seq = m_refresh_seq;
  execute_action_unlock(action, on_finish);
}

template <typename I>
void ImageState<I>::handle_update_notification() {
  std::lock_guard locker{m_lock};
  ++m_refresh_seq;

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": refresh_seq = " << m_refresh_seq << ", "
		 << "last_refresh = " << m_last_refresh << dendl;

  switch (m_state) {
  case STATE_UNINITIALIZED:
  case STATE_CLOSED:
  case STATE_OPENING:
  case STATE_CLOSING:
    ldout(cct, 5) << "dropping update notification to watchers" << dendl;
    return;
  default:
    break;
  }

  m_update_watchers->notify();
}

template <typename I>
bool ImageState<I>::is_refresh_required() const {
  std::lock_guard locker{m_lock};
  return (m_last_refresh != m_refresh_seq || find_pending_refresh() != nullptr);
}

template <typename I>
int ImageState<I>::refresh() {
  C_SaferCond refresh_ctx;
  refresh(&refresh_ctx);
  return refresh_ctx.wait();
}

template <typename I>
void ImageState<I>::refresh(Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_lock.lock();
  if (is_closed()) {
    m_lock.unlock();
    on_finish->complete(-ESHUTDOWN);
    return;
  }

  Action action(ACTION_TYPE_REFRESH);
  action.refresh_seq = m_refresh_seq;
  execute_action_unlock(action, on_finish);
}

template <typename I>
int ImageState<I>::refresh_if_required() {
  C_SaferCond ctx;
  {
    m_lock.lock();
    Action action(ACTION_TYPE_REFRESH);
    action.refresh_seq = m_refresh_seq;

    auto refresh_action = find_pending_refresh();
    if (refresh_action != nullptr) {
      // if a refresh is in-flight, delay until it is finished
      action = *refresh_action;
    } else if (m_last_refresh == m_refresh_seq) {
      m_lock.unlock();
      return 0;
    } else if (is_closed()) {
      m_lock.unlock();
      return -ESHUTDOWN;
    }

    execute_action_unlock(action, &ctx);
  }

  return ctx.wait();
}

template <typename I>
const typename ImageState<I>::Action *
ImageState<I>::find_pending_refresh() const {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  auto it = std::find_if(m_actions_contexts.rbegin(),
                         m_actions_contexts.rend(),
                         [](const ActionContexts& action_contexts) {
      return (action_contexts.first == ACTION_TYPE_REFRESH);
    });
  if (it != m_actions_contexts.rend()) {
    return &it->first;
  }
  return nullptr;
}

template <typename I>
void ImageState<I>::snap_set(uint64_t snap_id, Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": snap_id=" << snap_id << dendl;

  Action action(ACTION_TYPE_SET_SNAP);
  action.snap_id = snap_id;

  m_lock.lock();
  execute_action_unlock(action, on_finish);
}

template <typename I>
void ImageState<I>::prepare_lock(Context *on_ready) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << dendl;

  m_lock.lock();
  if (is_closed()) {
    m_lock.unlock();
    on_ready->complete(-ESHUTDOWN);
    return;
  }

  Action action(ACTION_TYPE_LOCK);
  action.on_ready = on_ready;
  execute_action_unlock(action, nullptr);
}

template <typename I>
void ImageState<I>::handle_prepare_lock_complete() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << dendl;

  m_lock.lock();
  if (m_state != STATE_PREPARING_LOCK) {
    m_lock.unlock();
    return;
  }

  complete_action_unlock(STATE_OPEN, 0);
}

template <typename I>
int ImageState<I>::register_update_watcher(UpdateWatchCtx *watcher,
					 uint64_t *handle) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_update_watchers->register_watcher(watcher, handle);

  ldout(cct, 20) << __func__ << ": handle=" << *handle << dendl;
  return 0;
}

template <typename I>
void ImageState<I>::unregister_update_watcher(uint64_t handle,
                                              Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": handle=" << handle << dendl;

  m_update_watchers->unregister_watcher(handle, on_finish);
}

template <typename I>
int ImageState<I>::unregister_update_watcher(uint64_t handle) {
  C_SaferCond ctx;
  unregister_update_watcher(handle, &ctx);
  return ctx.wait();
}

template <typename I>
void ImageState<I>::flush_update_watchers(Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_update_watchers->flush(on_finish);
}

template <typename I>
void ImageState<I>::shut_down_update_watchers(Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_update_watchers->shut_down(on_finish);
}

template <typename I>
bool ImageState<I>::is_transition_state() const {
  switch (m_state) {
  case STATE_UNINITIALIZED:
  case STATE_OPEN:
  case STATE_CLOSED:
    return false;
  case STATE_OPENING:
  case STATE_CLOSING:
  case STATE_REFRESHING:
  case STATE_SETTING_SNAP:
  case STATE_PREPARING_LOCK:
    break;
  }
  return true;
}

template <typename I>
bool ImageState<I>::is_closed() const {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  return ((m_state == STATE_CLOSED) ||
          (!m_actions_contexts.empty() &&
           m_actions_contexts.back().first.action_type == ACTION_TYPE_CLOSE));
}

template <typename I>
void ImageState<I>::append_context(const Action &action, Context *context) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  ActionContexts *action_contexts = nullptr;
  for (auto &action_ctxs : m_actions_contexts) {
    if (action == action_ctxs.first) {
      action_contexts = &action_ctxs;
      break;
    }
  }

  if (action_contexts == nullptr) {
    m_actions_contexts.push_back({action, {}});
    action_contexts = &m_actions_contexts.back();
  }

  if (context != nullptr) {
    action_contexts->second.push_back(context);
  }
}

template <typename I>
void ImageState<I>::execute_next_action_unlock() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  ceph_assert(!m_actions_contexts.empty());
  switch (m_actions_contexts.front().first.action_type) {
  case ACTION_TYPE_OPEN:
    send_open_unlock();
    return;
  case ACTION_TYPE_CLOSE:
    send_close_unlock();
    return;
  case ACTION_TYPE_REFRESH:
    send_refresh_unlock();
    return;
  case ACTION_TYPE_SET_SNAP:
    send_set_snap_unlock();
    return;
  case ACTION_TYPE_LOCK:
    send_prepare_lock_unlock();
    return;
  }
  ceph_abort();
}

template <typename I>
void ImageState<I>::execute_action_unlock(const Action &action,
                                          Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  append_context(action, on_finish);
  if (!is_transition_state()) {
    execute_next_action_unlock();
  } else {
    m_lock.unlock();
  }
}

template <typename I>
void ImageState<I>::complete_action_unlock(State next_state, int r) {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  ceph_assert(!m_actions_contexts.empty());

  ActionContexts action_contexts(std::move(m_actions_contexts.front()));
  m_actions_contexts.pop_front();

  m_state = next_state;
  m_lock.unlock();

  for (auto ctx : action_contexts.second) {
    ctx->complete(r);
  }

  if (next_state != STATE_UNINITIALIZED && next_state != STATE_CLOSED) {
    m_lock.lock();
    if (!is_transition_state() && !m_actions_contexts.empty()) {
      execute_next_action_unlock();
    } else {
      m_lock.unlock();
    }
  }
}

template <typename I>
void ImageState<I>::send_open_unlock() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_state = STATE_OPENING;

  Context *ctx = create_async_context_callback(
    *m_image_ctx, create_context_callback<
      ImageState<I>, &ImageState<I>::handle_open>(this));
  image::OpenRequest<I> *req = image::OpenRequest<I>::create(
    m_image_ctx, m_open_flags, ctx);

  m_lock.unlock();
  req->send();
}

template <typename I>
void ImageState<I>::handle_open(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
  }

  m_lock.lock();
  complete_action_unlock(r < 0 ? STATE_UNINITIALIZED : STATE_OPEN, r);
}

template <typename I>
void ImageState<I>::send_close_unlock() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_state = STATE_CLOSING;

  Context *ctx = create_context_callback<
    ImageState<I>, &ImageState<I>::handle_close>(this);
  image::CloseRequest<I> *req = image::CloseRequest<I>::create(
    m_image_ctx, ctx);

  m_lock.unlock();
  req->send();
}

template <typename I>
void ImageState<I>::handle_close(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "error occurred while closing image: " << cpp_strerror(r)
               << dendl;
  }

  m_lock.lock();
  complete_action_unlock(STATE_CLOSED, r);
}

template <typename I>
void ImageState<I>::send_refresh_unlock() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_state = STATE_REFRESHING;
  ceph_assert(!m_actions_contexts.empty());
  auto &action_context = m_actions_contexts.front().first;
  ceph_assert(action_context.action_type == ACTION_TYPE_REFRESH);

  Context *ctx = create_async_context_callback(
    *m_image_ctx, create_context_callback<
      ImageState<I>, &ImageState<I>::handle_refresh>(this));
  image::RefreshRequest<I> *req = image::RefreshRequest<I>::create(
    *m_image_ctx, false, false, ctx);

  m_lock.unlock();
  req->send();
}

template <typename I>
void ImageState<I>::handle_refresh(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  m_lock.lock();
  ceph_assert(!m_actions_contexts.empty());

  ActionContexts &action_contexts(m_actions_contexts.front());
  ceph_assert(action_contexts.first.action_type == ACTION_TYPE_REFRESH);
  ceph_assert(m_last_refresh <= action_contexts.first.refresh_seq);

  if (r == -ERESTART) {
    ldout(cct, 5) << "incomplete refresh: not updating sequence" << dendl;
    r = 0;
  } else {
    m_last_refresh = action_contexts.first.refresh_seq;
  }

  complete_action_unlock(STATE_OPEN, r);
}

template <typename I>
void ImageState<I>::send_set_snap_unlock() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  m_state = STATE_SETTING_SNAP;

  ceph_assert(!m_actions_contexts.empty());
  ActionContexts &action_contexts(m_actions_contexts.front());
  ceph_assert(action_contexts.first.action_type == ACTION_TYPE_SET_SNAP);

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "snap_id=" << action_contexts.first.snap_id << dendl;

  Context *ctx = create_async_context_callback(
    *m_image_ctx, create_context_callback<
      ImageState<I>, &ImageState<I>::handle_set_snap>(this));
  image::SetSnapRequest<I> *req = image::SetSnapRequest<I>::create(
    *m_image_ctx, action_contexts.first.snap_id, ctx);

  m_lock.unlock();
  req->send();
}

template <typename I>
void ImageState<I>::handle_set_snap(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << " r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to set snapshot: " << cpp_strerror(r) << dendl;
  }

  m_lock.lock();
  complete_action_unlock(STATE_OPEN, r);
}

template <typename I>
void ImageState<I>::send_prepare_lock_unlock() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));
  m_state = STATE_PREPARING_LOCK;

  ceph_assert(!m_actions_contexts.empty());
  ActionContexts &action_contexts(m_actions_contexts.front());
  ceph_assert(action_contexts.first.action_type == ACTION_TYPE_LOCK);

  Context *on_ready = action_contexts.first.on_ready;
  m_lock.unlock();

  if (on_ready == nullptr) {
    complete_action_unlock(STATE_OPEN, 0);
    return;
  }

  // wake up the lock handler now that its safe to proceed
  on_ready->complete(0);
}

template <typename I>
int ImageState<I>::register_quiesce_watcher(QuiesceWatchCtx *watcher,
                                                uint64_t *handle) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_quiesce_watchers->register_watcher(watcher, handle);

  ldout(cct, 20) << __func__ << ": handle=" << *handle << dendl;
  return 0;
}

template <typename I>
int ImageState<I>::unregister_quiesce_watcher(uint64_t handle) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": handle=" << handle << dendl;

  C_SaferCond ctx;
  m_quiesce_watchers->unregister_watcher(handle, &ctx);
  return ctx.wait();
}

template <typename I>
void ImageState<I>::notify_quiesce(Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_quiesce_watchers->notify_quiesce(on_finish);
}

template <typename I>
void ImageState<I>::notify_unquiesce(Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_quiesce_watchers->notify_unquiesce(on_finish);
}

template <typename I>
void ImageState<I>::quiesce_complete(int r) {
  m_quiesce_watchers->quiesce_complete(r);
}

} // namespace librbd

template class librbd::ImageState<librbd::ImageCtx>;
