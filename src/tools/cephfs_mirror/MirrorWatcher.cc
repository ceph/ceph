// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "include/stringify.h"
#include "msg/Messenger.h"
#include "aio_utils.h"
#include "MirrorWatcher.h"
#include "FSMirror.h"
#include "Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::MirrorWatcher " << __func__

namespace cephfs {
namespace mirror {

MirrorWatcher::MirrorWatcher(librados::IoCtx &ioctx, FSMirror *fs_mirror,
                             ContextWQ *work_queue)
  : Watcher(ioctx, CEPHFS_MIRROR_OBJECT, work_queue),
    m_ioctx(ioctx),
    m_fs_mirror(fs_mirror),
    m_work_queue(work_queue),
    m_lock(ceph::make_mutex("cephfs::mirror::mirror_watcher")),
    m_instance_id(stringify(m_ioctx.get_instance_id())) {
}

MirrorWatcher::~MirrorWatcher() {
}

void MirrorWatcher::init(Context *on_finish) {
  dout(20) << dendl;

  {
    std::scoped_lock locker(m_lock);
    ceph_assert(m_on_init_finish == nullptr);
    m_on_init_finish = new LambdaContext([this, on_finish](int r) {
                                           on_finish->complete(r);
                                           if (m_on_shutdown_finish != nullptr) {
                                             m_on_shutdown_finish->complete(0);
                                           }
                                         });
  }

  register_watcher();
}

void MirrorWatcher::shutdown(Context *on_finish) {
  dout(20) << dendl;

  {
    std::scoped_lock locker(m_lock);
    ceph_assert(m_on_shutdown_finish == nullptr);
    if (m_on_init_finish != nullptr) {
      dout(10) << ": delaying shutdown -- init in progress" << dendl;
      m_on_shutdown_finish = new LambdaContext([this, on_finish](int r) {
                                                 m_on_shutdown_finish = nullptr;
                                                 shutdown(on_finish);
                                               });
      return;
    }

    m_on_shutdown_finish = on_finish;
  }

  unregister_watcher();
}

void MirrorWatcher::handle_notify(uint64_t notify_id, uint64_t handle,
                                  uint64_t notifier_id, bufferlist& bl) {
  dout(20) << dendl;

  JSONFormatter f;
  f.open_object_section("info");
  encode_json("addr", m_fs_mirror->get_instance_addr(), &f);
  f.close_section();

  bufferlist outbl;
  f.flush(outbl);
  acknowledge_notify(notify_id, handle, outbl);
}

void MirrorWatcher::handle_rewatch_complete(int r) {
  dout(5) << ": r=" << r << dendl;

  if (r == -EBLOCKLISTED) {
    dout(0) << ": client blocklisted" <<dendl;
    std::scoped_lock locker(m_lock);
    m_blocklisted = true;
    m_blocklisted_ts = ceph_clock_now();
  } else if (r == -ENOENT) {
    derr << ": mirroring object deleted" << dendl;
    m_failed = true;
    m_failed_ts = ceph_clock_now();
  } else if (r < 0) {
    derr << ": rewatch error: " << cpp_strerror(r) << dendl;
    m_failed = true;
    m_failed_ts = ceph_clock_now();
  }
}

void MirrorWatcher::register_watcher() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  Context *on_finish = new C_CallbackAdapter<
    MirrorWatcher, &MirrorWatcher::handle_register_watcher>(this);
  register_watch(on_finish);
}

void MirrorWatcher::handle_register_watcher(int r) {
  dout(20) << ": r=" << r << dendl;

  Context *on_init_finish = nullptr;
  {
    std::scoped_lock locker(m_lock);
    std::swap(on_init_finish, m_on_init_finish);
  }

  on_init_finish->complete(r);
}

void MirrorWatcher::unregister_watcher() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  Context *on_finish = new C_CallbackAdapter<
    MirrorWatcher, &MirrorWatcher::handle_unregister_watcher>(this);
  unregister_watch(new C_AsyncCallback<ContextWQ>(m_work_queue, on_finish));
}

void MirrorWatcher::handle_unregister_watcher(int r) {
  dout(20) << ": r=" << r << dendl;

  Context *on_shutdown_finish = nullptr;
  {
    std::scoped_lock locker(m_lock);
    std::swap(on_shutdown_finish, m_on_shutdown_finish);
  }

  on_shutdown_finish->complete(r);
}

} // namespace mirror
} // namespace cephfs
