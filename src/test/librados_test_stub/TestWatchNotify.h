
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_WATCH_NOTIFY_H
#define CEPH_TEST_WATCH_NOTIFY_H

#include "include/rados/librados.hpp"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <list>
#include <map>

class CephContext;
class Cond;
class Finisher;

namespace librados {

class TestWatchNotify : boost::noncopyable {
public:
  typedef std::map<std::pair<uint64_t, uint64_t>, bufferlist> NotifyResponses;

  struct NotifyHandle {
    NotifyHandle();
    NotifyResponses notify_responses;
    bufferlist *pbl;
    size_t pending_responses;
    Mutex lock;
    Cond cond;
  };
  typedef boost::shared_ptr<NotifyHandle> SharedNotifyHandle;
  typedef std::map<uint64_t, SharedNotifyHandle> NotifyHandles;

  struct WatchHandle {
    uint64_t instance_id;
    uint64_t handle;
    librados::WatchCtx* watch_ctx;
    librados::WatchCtx2* watch_ctx2;
  };

  typedef std::map<uint64_t, WatchHandle> WatchHandles;

  struct Watcher {
    Watcher();
    WatchHandles watch_handles;
    NotifyHandles notify_handles;
    RWLock lock;
  };
  typedef boost::shared_ptr<Watcher> SharedWatcher;

  TestWatchNotify(CephContext *cct);
  ~TestWatchNotify();

  void flush();
  int list_watchers(const std::string& o,
                    std::list<obj_watch_t> *out_watchers);
  void aio_notify(const std::string& oid, bufferlist& bl, uint64_t timeout_ms,
                  bufferlist *pbl, Context *on_notify);
  int notify(const std::string& o, bufferlist& bl,
             uint64_t timeout_ms, bufferlist *pbl);
  void notify_ack(const std::string& o, uint64_t notify_id,
                  uint64_t handle, uint64_t gid, bufferlist& bl);
  int watch(const std::string& o, uint64_t instance_id, uint64_t *handle,
            librados::WatchCtx *ctx, librados::WatchCtx2 *ctx2);
  int unwatch(uint64_t handle);

private:

  typedef std::map<std::string, SharedWatcher> FileWatchers;

  CephContext *m_cct;
  Finisher *m_finisher;

  uint64_t m_handle;
  uint64_t m_notify_id;

  Mutex m_file_watcher_lock;
  Cond m_file_watcher_cond;
  uint64_t m_pending_notifies;

  FileWatchers	m_file_watchers;

  SharedWatcher get_watcher(const std::string& oid);
  SharedWatcher _get_watcher(const std::string& oid);
  void execute_notify(const std::string &oid, bufferlist &bl,
                      uint64_t notify_id, Context *on_notify);

};

} // namespace librados

#endif // CEPH_TEST_WATCH_NOTIFY_H
