// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestWatchNotify.h"
#include "include/Context.h"
#include "common/Finisher.h"
#include <boost/bind.hpp>
#include <boost/function.hpp>

namespace librados {

TestWatchNotify::TestWatchNotify(CephContext *cct)
  : m_cct(cct), m_finisher(new Finisher(cct)), m_handle(), m_notify_id(),
    m_file_watcher_lock("librados::TestWatchNotify::m_file_watcher_lock"),
    m_pending_notifies(0) {
  m_cct->get();
  m_finisher->start();
}

TestWatchNotify::~TestWatchNotify() {
  m_finisher->stop();
  delete m_finisher;
  m_cct->put();
}

TestWatchNotify::NotifyHandle::NotifyHandle()
  : pbl(NULL), pending_responses(),
    lock("TestWatchNotify::NotifyHandle::lock") {
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
    obj.watcher_id = static_cast<int64_t>(it->second.instance_id);
    obj.cookie = it->second.handle;
    obj.timeout_seconds = 30;
    out_watchers->push_back(obj);
  }
  return 0;
}

void TestWatchNotify::aio_notify(const std::string& oid, bufferlist& bl,
                                 uint64_t timeout_ms, bufferlist *pbl,
                                 Context *on_notify) {
  SharedWatcher watcher = get_watcher(oid);
  RWLock::WLocker watcher_locker(watcher->lock);
  Mutex::Locker file_watcher_lock(m_file_watcher_lock);
  ++m_pending_notifies;
  uint64_t notify_id = ++m_notify_id;

  SharedNotifyHandle notify_handle(new NotifyHandle());
  notify_handle->pbl = pbl;

  watcher->notify_handles[notify_id] = notify_handle;

  FunctionContext *ctx = new FunctionContext(
      boost::bind(&TestWatchNotify::execute_notify, this,
                  oid, bl, notify_id, on_notify));
  m_finisher->queue(ctx);
}

int TestWatchNotify::notify(const std::string& oid, bufferlist& bl,
                            uint64_t timeout_ms, bufferlist *pbl) {
  C_SaferCond cond;
  aio_notify(oid, bl, timeout_ms, pbl, &cond);
  return cond.wait();
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
  Mutex::Locker l2(notify_handle->lock);
  --notify_handle->pending_responses;
  notify_handle->notify_responses[std::make_pair(gid, handle)] = response;
  notify_handle->cond.Signal();
}

int TestWatchNotify::watch(const std::string& o, uint64_t instance_id,
                           uint64_t *handle, librados::WatchCtx *ctx,
                           librados::WatchCtx2 *ctx2) {
  SharedWatcher watcher = get_watcher(o);

  RWLock::WLocker l(watcher->lock);
  WatchHandle watch_handle;
  watch_handle.instance_id = instance_id;
  watch_handle.handle = ++m_handle;
  watch_handle.watch_ctx = ctx;
  watch_handle.watch_ctx2 = ctx2;
  watcher->watch_handles[watch_handle.handle] = watch_handle;

  *handle = watch_handle.handle;
  return 0;
}

int TestWatchNotify::unwatch(uint64_t handle) {

  SharedWatcher watcher;
  {
    Mutex::Locker l(m_file_watcher_lock);
    for (FileWatchers::iterator it = m_file_watchers.begin();
         it != m_file_watchers.end(); ++it) {
      if (it->second->watch_handles.find(handle) !=
            it->second->watch_handles.end()) {
        watcher = it->second;
        break;
      }
    }
  }

  if (watcher) {
    RWLock::WLocker l(watcher->lock);
    watcher->watch_handles.erase(handle);
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
                                     bufferlist &bl, uint64_t notify_id,
                                     Context *on_notify) {
  WatchHandles watch_handles;
  SharedNotifyHandle notify_handle;

  {
    SharedWatcher watcher = get_watcher(oid);
    RWLock::RLocker l(watcher->lock);

    NotifyHandles::iterator n_it = watcher->notify_handles.find(notify_id);
    if (n_it == watcher->notify_handles.end()) {
      return;
    }

    watch_handles = watcher->watch_handles;
    notify_handle = n_it->second;
  }

  utime_t timeout;
  timeout.set_from_double(ceph_clock_now(m_cct) + 15);

  for (WatchHandles::iterator w_it = watch_handles.begin();
       w_it != watch_handles.end(); ++w_it) {
    WatchHandle &watch_handle = w_it->second;

    bufferlist notify_bl;
    notify_bl.append(bl);
    if (watch_handle.watch_ctx2 != NULL) {
      {
        Mutex::Locker l2(notify_handle->lock);
        ++notify_handle->pending_responses;
      }
      watch_handle.watch_ctx2->handle_notify(notify_id, w_it->first, 0,
                                             notify_bl);
    } else if (watch_handle.watch_ctx != NULL) {
      watch_handle.watch_ctx->notify(0, 0, notify_bl);
    }
  }

  {
    Mutex::Locker l2(notify_handle->lock);
    while (notify_handle->pending_responses > 0) {
      notify_handle->cond.WaitUntil(notify_handle->lock, timeout);
    }
    if (notify_handle->pbl != NULL) {
      ::encode(notify_handle->notify_responses, *notify_handle->pbl);
    }
  }

  on_notify->complete(0);

  Mutex::Locker file_watcher_locker(m_file_watcher_lock);
  if (--m_pending_notifies == 0) {
    m_file_watcher_cond.Signal();
  }
}

} // namespace librados
