// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_MIRROR_WATCHER_H
#define CEPHFS_MIRROR_MIRROR_WATCHER_H

#include <string_view>

#include "common/ceph_mutex.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "Watcher.h"
#include "Types.h"

class ContextWQ;
class Messenger;

namespace cephfs {
namespace mirror {

class FSMirror;

// watch for notifications via cephfs_mirror object (in metadata
// pool). this is used sending keepalived with keepalive payload
// being the rados instance address (used by the manager module
// to blocklist when needed).

class MirrorWatcher : public Watcher {
public:
  static MirrorWatcher *create(librados::IoCtx &ioctx, FSMirror *fs_mirror,
                               ErrorListener &elistener, ContextWQ *work_queue) {
    return new MirrorWatcher(ioctx, fs_mirror, elistener, work_queue);
  }

  MirrorWatcher(librados::IoCtx &ioctx, FSMirror *fs_mirror, ErrorListener &elistener,
                ContextWQ *work_queue);
  ~MirrorWatcher();

  void init(Context *on_finish);
  void shutdown(Context *on_finish);

  void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist& bl) override;
  void handle_rewatch_complete(int r) override;

  bool is_blocklisted() {
    std::scoped_lock locker(m_lock);
    return m_blocklisted;
  }

  bool is_failed() {
    std::scoped_lock locker(m_lock);
    return m_failed;
  }

private:
  librados::IoCtx &m_ioctx;
  FSMirror *m_fs_mirror;
  ErrorListener &m_elistener;
  ContextWQ *m_work_queue;

  ceph::mutex m_lock;
  std::string m_instance_id;

  Context *m_on_init_finish = nullptr;
  Context *m_on_shutdown_finish = nullptr;

  bool m_blocklisted = false;
  bool m_failed = false;

  void register_watcher();
  void handle_register_watcher(int r);

  void unregister_watcher();
  void handle_unregister_watcher(int r);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_MIRROR_WATCHER_H
