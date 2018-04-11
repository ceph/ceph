// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestWatchNotify.h"
#include "include/Context.h"
#include "include/stringify.h"
#include "common/Finisher.h"
#include "test/librados_test_stub/TestRadosClient.h"
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include "include/assert.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "TestWatchNotify::" << __func__ << ": "

namespace librados {

std::ostream& operator<<(std::ostream& out,
			 const TestWatchNotify::WatcherID &watcher_id) {
  out << "(" << watcher_id.first << "," << watcher_id.second << ")";
  return out;
}

TestWatchNotify::TestWatchNotify()
  : m_lock("librados::TestWatchNotify::m_lock") {
}

void TestWatchNotify::flush(TestRadosClient *rados_client) {
  CephContext *cct = rados_client->cct();

  ldout(cct, 20) << "enter" << dendl;
  // block until we know no additional async notify callbacks will occur
  C_SaferCond ctx;
  m_async_op_tracker.wait_for_ops(&ctx);
  ctx.wait();
}

int TestWatchNotify::list_watchers(int64_t pool_id, const std::string& o,
                                   std::list<obj_watch_t> *out_watchers) {
  Mutex::Locker lock(m_lock);
  SharedWatcher watcher = get_watcher(pool_id, o);

  out_watchers->clear();
  for (TestWatchNotify::WatchHandles::iterator it =
         watcher->watch_handles.begin();
       it != watcher->watch_handles.end(); ++it) {
    obj_watch_t obj;
    strcpy(obj.addr, it->second.addr.c_str());
    obj.watcher_id = static_cast<int64_t>(it->second.gid);
    obj.cookie = it->second.handle;
    obj.timeout_seconds = 30;
    out_watchers->push_back(obj);
  }
  return 0;
}

void TestWatchNotify::aio_flush(TestRadosClient *rados_client,
                                Context *on_finish) {
  rados_client->get_aio_finisher()->queue(on_finish);
}

int TestWatchNotify::watch(TestRadosClient *rados_client, int64_t pool_id,
                           const std::string& o, uint64_t gid, uint64_t *handle,
                           librados::WatchCtx *ctx, librados::WatchCtx2 *ctx2) {
  C_SaferCond cond;
  aio_watch(rados_client, pool_id, o, gid, handle, ctx, ctx2, &cond);
  return cond.wait();
}

void TestWatchNotify::aio_watch(TestRadosClient *rados_client, int64_t pool_id,
                                const std::string& o, uint64_t gid,
                                uint64_t *handle,
                                librados::WatchCtx *watch_ctx,
                                librados::WatchCtx2 *watch_ctx2,
                                Context *on_finish) {
  auto ctx = new FunctionContext([=](int) {
      execute_watch(rados_client, pool_id, o, gid, handle, watch_ctx,
                    watch_ctx2, on_finish);
    });
  rados_client->get_aio_finisher()->queue(ctx);
}

int TestWatchNotify::unwatch(TestRadosClient *rados_client,
                             uint64_t handle) {
  C_SaferCond ctx;
  aio_unwatch(rados_client, handle, &ctx);
  return ctx.wait();
}

void TestWatchNotify::aio_unwatch(TestRadosClient *rados_client,
                                  uint64_t handle, Context *on_finish) {
  auto ctx = new FunctionContext([this, rados_client, handle, on_finish](int) {
      execute_unwatch(rados_client, handle, on_finish);
    });
  rados_client->get_aio_finisher()->queue(ctx);
}

void TestWatchNotify::aio_notify(TestRadosClient *rados_client, int64_t pool_id,
                                 const std::string& oid, const bufferlist& bl,
                                 uint64_t timeout_ms, bufferlist *pbl,
                                 Context *on_notify) {
  auto ctx = new FunctionContext([=](int) {
      execute_notify(rados_client, pool_id, oid, bl, pbl, on_notify);
    });
  rados_client->get_aio_finisher()->queue(ctx);
}

int TestWatchNotify::notify(TestRadosClient *rados_client, int64_t pool_id,
                            const std::string& oid, bufferlist& bl,
                            uint64_t timeout_ms, bufferlist *pbl) {
  C_SaferCond cond;
  aio_notify(rados_client, pool_id, oid, bl, timeout_ms, pbl, &cond);
  return cond.wait();
}

void TestWatchNotify::notify_ack(TestRadosClient *rados_client, int64_t pool_id,
                                 const std::string& o, uint64_t notify_id,
                                 uint64_t handle, uint64_t gid,
                                 bufferlist& bl) {
  CephContext *cct = rados_client->cct();
  ldout(cct, 20) << "notify_id=" << notify_id << ", handle=" << handle
		 << ", gid=" << gid << dendl;
  Mutex::Locker lock(m_lock);
  WatcherID watcher_id = std::make_pair(gid, handle);
  ack_notify(rados_client, pool_id, o, notify_id, watcher_id, bl);
  finish_notify(rados_client, pool_id, o, notify_id);
}

void TestWatchNotify::execute_watch(TestRadosClient *rados_client,
                                    int64_t pool_id, const std::string& o,
                                    uint64_t gid, uint64_t *handle,
                                    librados::WatchCtx *ctx,
                                    librados::WatchCtx2 *ctx2,
                                    Context* on_finish) {
  CephContext *cct = rados_client->cct();

  m_lock.Lock();
  SharedWatcher watcher = get_watcher(pool_id, o);

  WatchHandle watch_handle;
  watch_handle.rados_client = rados_client;
  watch_handle.addr = "127.0.0.1:0/" + stringify(rados_client->get_nonce());
  watch_handle.nonce = rados_client->get_nonce();
  watch_handle.gid = gid;
  watch_handle.handle = ++m_handle;
  watch_handle.watch_ctx = ctx;
  watch_handle.watch_ctx2 = ctx2;
  watcher->watch_handles[watch_handle.handle] = watch_handle;

  *handle = watch_handle.handle;

  ldout(cct, 20) << "oid=" << o << ", gid=" << gid << ": handle=" << *handle
	         << dendl;
  m_lock.Unlock();

  on_finish->complete(0);
}

void TestWatchNotify::execute_unwatch(TestRadosClient *rados_client,
                                      uint64_t handle, Context* on_finish) {
  CephContext *cct = rados_client->cct();

  ldout(cct, 20) << "handle=" << handle << dendl;
  {
    Mutex::Locker locker(m_lock);
    for (FileWatchers::iterator it = m_file_watchers.begin();
         it != m_file_watchers.end(); ++it) {
      SharedWatcher watcher = it->second;

      WatchHandles::iterator w_it = watcher->watch_handles.find(handle);
      if (w_it != watcher->watch_handles.end()) {
        watcher->watch_handles.erase(w_it);
        if (watcher->watch_handles.empty() && watcher->notify_handles.empty()) {
          m_file_watchers.erase(it);
        }
        break;
      }
    }
  }
  on_finish->complete(0);
}

TestWatchNotify::SharedWatcher TestWatchNotify::get_watcher(
    int64_t pool_id, const std::string& oid) {
  assert(m_lock.is_locked());
  SharedWatcher &watcher = m_file_watchers[{pool_id, oid}];
  if (!watcher) {
    watcher.reset(new Watcher());
  }
  return watcher;
}

void TestWatchNotify::execute_notify(TestRadosClient *rados_client,
                                     int64_t pool_id, const std::string &oid,
                                     const bufferlist &bl, bufferlist *pbl,
                                     Context *on_notify) {
  CephContext *cct = rados_client->cct();

  m_lock.Lock();
  uint64_t notify_id = ++m_notify_id;

  ldout(cct, 20) << "oid=" << oid << ": notify_id=" << notify_id << dendl;

  SharedWatcher watcher = get_watcher(pool_id, oid);

  SharedNotifyHandle notify_handle(new NotifyHandle());
  notify_handle->rados_client = rados_client;
  notify_handle->pbl = pbl;
  notify_handle->on_notify = on_notify;

  WatchHandles &watch_handles = watcher->watch_handles;
  for (auto &watch_handle_pair : watch_handles) {
    WatchHandle &watch_handle = watch_handle_pair.second;
    notify_handle->pending_watcher_ids.insert(std::make_pair(
      watch_handle.gid, watch_handle.handle));

    m_async_op_tracker.start_op();
    uint64_t notifier_id = rados_client->get_instance_id();
    watch_handle.rados_client->get_aio_finisher()->queue(new FunctionContext(
      [this, pool_id, oid, bl, notify_id, watch_handle, notifier_id](int r) {
        bufferlist notify_bl;
        notify_bl.append(bl);

        if (watch_handle.watch_ctx2 != NULL) {
          watch_handle.watch_ctx2->handle_notify(notify_id,
                                                 watch_handle.handle,
                                                 notifier_id, notify_bl);
        } else if (watch_handle.watch_ctx != NULL) {
          watch_handle.watch_ctx->notify(0, 0, notify_bl);

          // auto ack old-style watch/notify clients
          ack_notify(watch_handle.rados_client, pool_id, oid, notify_id,
                     {watch_handle.gid, watch_handle.handle}, bufferlist());
        }

        m_async_op_tracker.finish_op();
      }));
  }
  watcher->notify_handles[notify_id] = notify_handle;

  finish_notify(rados_client, pool_id, oid, notify_id);
  m_lock.Unlock();
}

void TestWatchNotify::ack_notify(TestRadosClient *rados_client, int64_t pool_id,
                                 const std::string &oid, uint64_t notify_id,
                                 const WatcherID &watcher_id,
                                 const bufferlist &bl) {
  CephContext *cct = rados_client->cct();

  ldout(cct, 20) << "oid=" << oid << ", notify_id=" << notify_id
		 << ", WatcherID=" << watcher_id << dendl;

  assert(m_lock.is_locked());
  SharedWatcher watcher = get_watcher(pool_id, oid);

  NotifyHandles::iterator it = watcher->notify_handles.find(notify_id);
  if (it == watcher->notify_handles.end()) {
    ldout(cct, 1) << "oid=" << oid << ", notify_id=" << notify_id
		  << ", WatcherID=" << watcher_id << ": not found" << dendl;
    return;
  }

  bufferlist response;
  response.append(bl);

  SharedNotifyHandle notify_handle = it->second;
  notify_handle->notify_responses[watcher_id] = response;
  notify_handle->pending_watcher_ids.erase(watcher_id);
}

void TestWatchNotify::finish_notify(TestRadosClient *rados_client,
                                    int64_t pool_id, const std::string &oid,
                                    uint64_t notify_id) {
  CephContext *cct = rados_client->cct();

  ldout(cct, 20) << "oid=" << oid << ", notify_id=" << notify_id << dendl;

  assert(m_lock.is_locked());
  SharedWatcher watcher = get_watcher(pool_id, oid);

  NotifyHandles::iterator it = watcher->notify_handles.find(notify_id);
  if (it == watcher->notify_handles.end()) {
    ldout(cct, 1) << "oid=" << oid << ", notify_id=" << notify_id
	          << ": not found" << dendl;
    return;
  }

  SharedNotifyHandle notify_handle = it->second;
  if (!notify_handle->pending_watcher_ids.empty()) {
    ldout(cct, 10) << "oid=" << oid << ", notify_id=" << notify_id
	           << ": pending watchers, returning" << dendl;
    return;
  }

  ldout(cct, 20) << "oid=" << oid << ", notify_id=" << notify_id
		 << ": completing" << dendl;

  if (notify_handle->pbl != NULL) {
    encode(notify_handle->notify_responses, *notify_handle->pbl);
    encode(notify_handle->pending_watcher_ids, *notify_handle->pbl);
  }

  notify_handle->rados_client->get_aio_finisher()->queue(
    notify_handle->on_notify, 0);
  watcher->notify_handles.erase(notify_id);
  if (watcher->watch_handles.empty() && watcher->notify_handles.empty()) {
    m_file_watchers.erase({pool_id, oid});
  }
}

void TestWatchNotify::blacklist(uint32_t nonce) {
  Mutex::Locker locker(m_lock);

  for (auto file_it = m_file_watchers.begin();
       file_it != m_file_watchers.end(); ) {
    auto &watcher = file_it->second;
    for (auto w_it = watcher->watch_handles.begin();
         w_it != watcher->watch_handles.end();) {
      if (w_it->second.nonce == nonce) {
        w_it = watcher->watch_handles.erase(w_it);
      } else {
        ++w_it;
      }
    }
    if (watcher->watch_handles.empty() && watcher->notify_handles.empty()) {
        file_it = m_file_watchers.erase(file_it);
    } else {
      ++file_it;
    }
  }
}

} // namespace librados
