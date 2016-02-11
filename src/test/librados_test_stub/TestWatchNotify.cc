// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestWatchNotify.h"
#include "include/Context.h"
#include "common/Finisher.h"
#include <boost/bind.hpp>
#include <boost/function.hpp>

namespace librados {

TestWatchNotify::TestWatchNotify(CephContext *cct, Finisher *finisher)
  : m_cct(cct), m_finisher(finisher), m_handle(), m_notify_id(),
    m_file_watcher_lock("librados::TestWatchNotify::m_file_watcher_lock"),
    m_pending_notifies(0) {
  m_cct->get();
}

TestWatchNotify::~TestWatchNotify() {
  m_cct->put();
}

TestWatchNotify::NotifyHandle::NotifyHandle()
  : lock("TestWatchNotify::NotifyHandle::lock") {
}

TestWatchNotify::Watcher::Watcher()
  : lock("TestWatchNotify::Watcher::lock") {
}

void TestWatchNotify::flush() {
  Mutex::Locker file_watcher_locker(m_file_watcher_lock);
  while (m_pending_notifies > 0) {
    m_file_watcher_cond.Wait(m_file_watcher_lock);
  }
}

int TestWatchNotify::list_watchers(const std::string& o,
                                   std::list<obj_watch_t> *out_watchers) {
  SharedWatcher watcher = get_watcher(o);
  RWLock::RLocker l(watcher->lock);

  out_watchers->clear();
  for (TestWatchNotify::WatchHandles::iterator it =
         watcher->watch_handles.begin();
       it != watcher->watch_handles.end(); ++it) {
    obj_watch_t obj;
    strcpy(obj.addr, ":/0");
    obj.watcher_id = static_cast<int64_t>(it->second.gid);
    obj.cookie = it->second.handle;
    obj.timeout_seconds = 30;
    out_watchers->push_back(obj);
  }
  return 0;
}

int TestWatchNotify::notify(const std::string& oid, bufferlist& bl,
                            uint64_t timeout_ms, bufferlist *pbl) {
  uint64_t notify_id;
  {
    Mutex::Locker file_watcher_locker(m_file_watcher_lock);
    ++m_pending_notifies;
    notify_id = ++m_notify_id;
  }

  SharedNotifyHandle notify_handle(new NotifyHandle());
  {
    SharedWatcher watcher = get_watcher(oid);
    RWLock::WLocker watcher_locker(watcher->lock);

    WatchHandles watch_handles = watcher->watch_handles;
    for (WatchHandles::iterator w_it = watch_handles.begin();
         w_it != watch_handles.end(); ++w_it) {
      WatchHandle &watch_handle = w_it->second;

      Mutex::Locker notify_handle_locker(notify_handle->lock);
      notify_handle->pending_watcher_ids.insert(std::make_pair(
        watch_handle.gid, watch_handle.handle));
    }

    watcher->notify_handles[notify_id] = notify_handle;

    FunctionContext *ctx = new FunctionContext(
      boost::bind(&TestWatchNotify::execute_notify, this, oid, bl, notify_id));
    m_finisher->queue(ctx);
  }

  {
    utime_t timeout;
    timeout.set_from_double(ceph_clock_now(m_cct) + (timeout_ms / 1000.0));

    Mutex::Locker notify_locker(notify_handle->lock);
    while (!notify_handle->pending_watcher_ids.empty()) {
      notify_handle->cond.WaitUntil(notify_handle->lock, timeout);
    }

    if (pbl != NULL) {
      ::encode(notify_handle->notify_responses, *pbl);
      ::encode(notify_handle->pending_watcher_ids, *pbl);
    }
  }

  SharedWatcher watcher = get_watcher(oid);
  Mutex::Locker file_watcher_locker(m_file_watcher_lock);
  {
    RWLock::WLocker watcher_locker(watcher->lock);

    watcher->notify_handles.erase(notify_id);
    if (watcher->watch_handles.empty() && watcher->notify_handles.empty()) {
      m_file_watchers.erase(oid);
    }
  }

  if (--m_pending_notifies == 0) {
    m_file_watcher_cond.Signal();
  }
  return 0;
}

void TestWatchNotify::notify_ack(const std::string& o, uint64_t notify_id,
                                 uint64_t handle, uint64_t gid,
                                 bufferlist& bl) {
  SharedWatcher watcher = get_watcher(o);

  RWLock::RLocker l(watcher->lock);
  NotifyHandles::iterator it = watcher->notify_handles.find(notify_id);
  if (it == watcher->notify_handles.end()) {
    return;
  }

  bufferlist response;
  response.append(bl);

  SharedNotifyHandle notify_handle = it->second;
  Mutex::Locker notify_handle_locker(notify_handle->lock);

  WatcherID watcher_id = std::make_pair(gid, handle);
  notify_handle->notify_responses[watcher_id] = response;
  notify_handle->pending_watcher_ids.erase(watcher_id);
  if (notify_handle->pending_watcher_ids.empty()) {
    notify_handle->cond.Signal();
  }
}

int TestWatchNotify::watch(const std::string& o, uint64_t gid,
                           uint64_t *handle, librados::WatchCtx *ctx,
                           librados::WatchCtx2 *ctx2) {
  SharedWatcher watcher = get_watcher(o);

  RWLock::WLocker l(watcher->lock);
  WatchHandle watch_handle;
  watch_handle.gid = gid;
  watch_handle.handle = ++m_handle;
  watch_handle.watch_ctx = ctx;
  watch_handle.watch_ctx2 = ctx2;
  watcher->watch_handles[watch_handle.handle] = watch_handle;

  *handle = watch_handle.handle;
  return 0;
}

int TestWatchNotify::unwatch(uint64_t handle) {
  Mutex::Locker l(m_file_watcher_lock);
  for (FileWatchers::iterator it = m_file_watchers.begin();
       it != m_file_watchers.end(); ++it) {
    SharedWatcher watcher = it->second;
    RWLock::WLocker watcher_locker(watcher->lock);

    WatchHandles::iterator w_it = watcher->watch_handles.find(handle);
    if (w_it != watcher->watch_handles.end()) {
      watcher->watch_handles.erase(w_it);
      if (watcher->watch_handles.empty() && watcher->notify_handles.empty()) {
        m_file_watchers.erase(it);
      }
      break;
    }
  }
  return 0;
}

TestWatchNotify::SharedWatcher TestWatchNotify::get_watcher(
    const std::string& oid) {
  Mutex::Locker l(m_file_watcher_lock);
  return _get_watcher(oid);
}

TestWatchNotify::SharedWatcher TestWatchNotify::_get_watcher(
    const std::string& oid) {
  SharedWatcher &watcher = m_file_watchers[oid];
  if (!watcher) {
    watcher.reset(new Watcher());
  }
  return watcher;
}

void TestWatchNotify::execute_notify(const std::string &oid,
                                     bufferlist &bl, uint64_t notify_id) {
  SharedWatcher watcher = get_watcher(oid);
  RWLock::RLocker watcher_locker(watcher->lock);
  WatchHandles &watch_handles = watcher->watch_handles;

  NotifyHandles::iterator n_it = watcher->notify_handles.find(notify_id);
  if (n_it == watcher->notify_handles.end()) {
    return;
  }

  SharedNotifyHandle notify_handle = n_it->second;
  Mutex::Locker notify_handle_locker(notify_handle->lock);

  WatcherIDs watcher_ids(notify_handle->pending_watcher_ids);
  for (WatcherIDs::iterator w_id_it = watcher_ids.begin();
       w_id_it != watcher_ids.end(); ++w_id_it) {
    WatcherID watcher_id = *w_id_it;
    WatchHandles::iterator w_it = watch_handles.find(watcher_id.second);
    if (w_it == watch_handles.end()) {
      notify_handle->pending_watcher_ids.erase(watcher_id);
    } else {
      WatchHandle &watch_handle = w_it->second;
      assert(watch_handle.gid == watcher_id.first);
      assert(watch_handle.handle == watcher_id.second);

      bufferlist notify_bl;
      notify_bl.append(bl);

      notify_handle->lock.Unlock();
      watcher->lock.put_read();
      if (watch_handle.watch_ctx2 != NULL) {
        watch_handle.watch_ctx2->handle_notify(notify_id, w_it->first, 0,
                                               notify_bl);
      } else if (watch_handle.watch_ctx != NULL) {
        watch_handle.watch_ctx->notify(0, 0, notify_bl);
      }
      watcher->lock.get_read();
      notify_handle->lock.Lock();

      if (watch_handle.watch_ctx2 == NULL) {
        // auto ack old-style watch/notify clients
        notify_handle->notify_responses[watcher_id] = bufferlist();
        notify_handle->pending_watcher_ids.erase(watcher_id);
      }
    }
  }

  if (notify_handle->pending_watcher_ids.empty()) {
    notify_handle->cond.Signal();
  }
}

} // namespace librados
