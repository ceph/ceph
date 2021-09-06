// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LREM_WATCH_NOTIFY_H
#define CEPH_LREM_WATCH_NOTIFY_H

#include "include/rados/librados.hpp"
#include "common/AsyncOpTracker.h"
#include "common/ceph_mutex.h"
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <list>
#include <map>

class Finisher;

namespace librados {

class LRemCluster;
class LRemRadosClient;

class LRemWatchNotify : boost::noncopyable {
public:
  typedef std::pair<uint64_t, uint64_t> WatcherID;
  typedef std::set<WatcherID> WatcherIDs;
  typedef std::map<std::pair<uint64_t, uint64_t>, bufferlist> NotifyResponses;

  struct NotifyHandle {
    LRemRadosClient *rados_client = nullptr;
    WatcherIDs pending_watcher_ids;
    NotifyResponses notify_responses;
    bufferlist *pbl = nullptr;
    Context *on_notify = nullptr;
  };
  typedef boost::shared_ptr<NotifyHandle> SharedNotifyHandle;
  typedef std::map<uint64_t, SharedNotifyHandle> NotifyHandles;

  struct WatchHandle {
    LRemRadosClient *rados_client = nullptr;
    std::string addr;
    uint32_t nonce;
    uint64_t gid;
    uint64_t handle;
    librados::WatchCtx* watch_ctx;
    librados::WatchCtx2* watch_ctx2;
  };

  typedef std::map<uint64_t, WatchHandle> WatchHandles;

  struct ObjectHandler;
  typedef boost::shared_ptr<ObjectHandler> SharedObjectHandler;

  struct Watcher {
    Watcher(int64_t pool_id, const std::string& nspace, const std::string& oid)
      : pool_id(pool_id), nspace(nspace), oid(oid) {
    }

    int64_t pool_id;
    std::string nspace;
    std::string oid;

    SharedObjectHandler object_handler;
    WatchHandles watch_handles;
    NotifyHandles notify_handles;
  };
  typedef boost::shared_ptr<Watcher> SharedWatcher;

  LRemWatchNotify(LRemCluster* lrem_cluster);

  int list_watchers(int64_t pool_id, const std::string& nspace,
                    const std::string& o, std::list<obj_watch_t> *out_watchers);

  void aio_flush(LRemRadosClient *rados_client, Context *on_finish);
  void aio_watch(LRemRadosClient *rados_client, int64_t pool_id,
                 const std::string& nspace, const std::string& o, uint64_t gid,
                 uint64_t *handle, librados::WatchCtx *watch_ctx,
                 librados::WatchCtx2 *watch_ctx2, Context *on_finish);
  void aio_unwatch(LRemRadosClient *rados_client, uint64_t handle,
                   Context *on_finish);
  void aio_notify(LRemRadosClient *rados_client, int64_t pool_id,
                  const std::string& nspace, const std::string& oid,
                  const bufferlist& bl, uint64_t timeout_ms, bufferlist *pbl,
                  Context *on_notify);

  void flush(LRemRadosClient *rados_client);
  int notify(LRemRadosClient *rados_client, int64_t pool_id,
             const std::string& nspace, const std::string& o, bufferlist& bl,
             uint64_t timeout_ms, bufferlist *pbl);
  void notify_ack(LRemRadosClient *rados_client, int64_t pool_id,
                  const std::string& nspace, const std::string& o,
                  uint64_t notify_id, uint64_t handle, uint64_t gid,
                  bufferlist& bl);

  int watch(LRemRadosClient *rados_client, int64_t pool_id,
            const std::string& nspace, const std::string& o, uint64_t gid,
            uint64_t *handle, librados::WatchCtx *ctx,
            librados::WatchCtx2 *ctx2);
  int unwatch(LRemRadosClient *rados_client, uint64_t handle);

  void blocklist(uint32_t nonce);

private:
  typedef std::tuple<int64_t, std::string, std::string> PoolFile;
  typedef std::map<PoolFile, SharedWatcher> FileWatchers;

  LRemCluster *m_lrem_cluster;

  uint64_t m_handle = 0;
  uint64_t m_notify_id = 0;

  ceph::mutex m_lock =
    ceph::make_mutex("librados::LRemWatchNotify::m_lock");
  AsyncOpTracker m_async_op_tracker;

  FileWatchers	m_file_watchers;

  SharedWatcher get_watcher(int64_t pool_id, const std::string& nspace,
                            const std::string& oid);
  void maybe_remove_watcher(SharedWatcher shared_watcher);

  void execute_watch(LRemRadosClient *rados_client, int64_t pool_id,
                     const std::string& nspace, const std::string& o,
                     uint64_t gid, uint64_t *handle,
                     librados::WatchCtx *watch_ctx,
                     librados::WatchCtx2 *watch_ctx2,
                     Context *on_finish);
  void execute_unwatch(LRemRadosClient *rados_client, uint64_t handle,
                       Context *on_finish);

  void execute_notify(LRemRadosClient *rados_client, int64_t pool_id,
                      const std::string& nspace, const std::string &oid,
                      const bufferlist &bl, bufferlist *pbl,
                      Context *on_notify);
  void ack_notify(LRemRadosClient *rados_client, int64_t pool_id,
                  const std::string& nspace, const std::string &oid,
                  uint64_t notify_id, const WatcherID &watcher_id,
                  const bufferlist &bl);
  void finish_notify(LRemRadosClient *rados_client, int64_t pool_id,
                     const std::string& nspace, const std::string &oid,
                     uint64_t notify_id);

  void handle_object_removed(int64_t pool_id, const std::string& nspace,
                             const std::string& oid);
};

} // namespace librados

#endif // CEPH_LREM_WATCH_NOTIFY_H
