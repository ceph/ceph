// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/cephfs/cls_cephfs_client.h"
#include "include/stringify.h"
#include "aio_utils.h"
#include "InstanceWatcher.h"
#include "Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::InstanceWatcher " << __func__

using namespace std;

namespace cephfs {
namespace mirror {

namespace {

std::string instance_oid(const std::string &instance_id) {
  return CEPHFS_MIRROR_OBJECT + "." + instance_id;
}

} // anonymous namespace

InstanceWatcher::InstanceWatcher(librados::IoCtx &ioctx,
                                 Listener &listener, ContextWQ *work_queue)
  : Watcher(ioctx, instance_oid(stringify(ioctx.get_instance_id())), work_queue),
    m_ioctx(ioctx),
    m_listener(listener),
    m_work_queue(work_queue),
    m_lock(ceph::make_mutex("cephfs::mirror::instance_watcher")) {
}

InstanceWatcher::~InstanceWatcher() {
}

void InstanceWatcher::init(Context *on_finish) {
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

  create_instance();
}

void InstanceWatcher::shutdown(Context *on_finish) {
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

void InstanceWatcher::handle_notify(uint64_t notify_id, uint64_t handle,
                                    uint64_t notifier_id, bufferlist& bl) {
  dout(20) << dendl;

  std::string dir_path;
  std::string mode;
  try {
    JSONDecoder jd(bl);
    JSONDecoder::decode_json("dir_path", dir_path, &jd.parser, true);
    JSONDecoder::decode_json("mode", mode, &jd.parser, true);
  } catch (const JSONDecoder::err &e) {
    derr << ": failed to decode notify json: " << e.what() << dendl;
  }

  dout(20) << ": notifier_id=" << notifier_id << ", dir_path=" << dir_path
           << ", mode=" << mode << dendl;

  if (mode == "acquire") {
    m_listener.acquire_directory(dir_path);
  } else if (mode == "release") {
    m_listener.release_directory(dir_path);
  } else {
    derr << ": unknown mode" << dendl;
  }

  bufferlist outbl;
  acknowledge_notify(notify_id, handle, outbl);
}

void InstanceWatcher::handle_rewatch_complete(int r) {
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

void InstanceWatcher::create_instance() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  librados::ObjectWriteOperation op;
  op.create(false);

  librados::AioCompletion *aio_comp =
    librados::Rados::aio_create_completion(
      this, &rados_callback<InstanceWatcher, &InstanceWatcher::handle_create_instance>);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

void InstanceWatcher::handle_create_instance(int r) {
  dout(20) << ": r=" << r << dendl;

  Context *on_init_finish = nullptr;
  {
    std::scoped_lock locker(m_lock);
    if (r < 0) {
      std::swap(on_init_finish, m_on_init_finish);
    }
  }

  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
    return;
  }

  register_watcher();
}

void InstanceWatcher::register_watcher() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  Context *on_finish = new C_CallbackAdapter<
    InstanceWatcher, &InstanceWatcher::handle_register_watcher>(this);
  register_watch(on_finish);
}

void InstanceWatcher::handle_register_watcher(int r) {
  dout(20) << ": r=" << r << dendl;

  Context *on_init_finish = nullptr;
  {
    std::scoped_lock locker(m_lock);
    if (r == 0) {
      std::swap(on_init_finish, m_on_init_finish);
    }
  }

  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
    return;
  }

  remove_instance();
}

void InstanceWatcher::unregister_watcher() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  Context *on_finish = new C_CallbackAdapter<
    InstanceWatcher, &InstanceWatcher::handle_unregister_watcher>(this);
  unregister_watch(new C_AsyncCallback<ContextWQ>(m_work_queue, on_finish));
}

void InstanceWatcher::handle_unregister_watcher(int r) {
  dout(20) << ": r=" << r << dendl;

  Context *on_shutdown_finish = nullptr;
  {
    std::scoped_lock locker(m_lock);
    if (r < 0) {
      std::swap(on_shutdown_finish, m_on_shutdown_finish);
    }
  }

  if (on_shutdown_finish != nullptr) {
    on_shutdown_finish->complete(r);
    return;
  }

  remove_instance();
}

void InstanceWatcher::remove_instance() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  librados::ObjectWriteOperation op;
  op.remove();

  librados::AioCompletion *aio_comp =
    librados::Rados::aio_create_completion(
      this, &rados_callback<InstanceWatcher, &InstanceWatcher::handle_remove_instance>);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

void InstanceWatcher::handle_remove_instance(int r) {
  dout(20) << ": r=" << r << dendl;

  Context *on_init_finish = nullptr;
  Context *on_shutdown_finish = nullptr;
  {
    std::scoped_lock locker(m_lock);
    std::swap(on_init_finish, m_on_init_finish);
    std::swap(on_shutdown_finish, m_on_shutdown_finish);
  }

  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
  }
  if (on_shutdown_finish != nullptr) {
    on_shutdown_finish->complete(r);
  }
}

} // namespace mirror
} // namespace cephfs
