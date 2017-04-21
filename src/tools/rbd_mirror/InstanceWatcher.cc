// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "InstanceWatcher.h"
#include "include/atomic.h"
#include "include/stringify.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ManagedLock.h"
#include "librbd/Utils.h"
#include "InstanceReplayer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::InstanceWatcher: "

namespace rbd {
namespace mirror {

using namespace instance_watcher;

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;
using librbd::util::unique_lock_name;

namespace {

struct C_GetInstances : public Context {
  std::vector<std::string> *instance_ids;
  Context *on_finish;
  bufferlist out_bl;

  C_GetInstances(std::vector<std::string> *instance_ids, Context *on_finish)
    : instance_ids(instance_ids), on_finish(on_finish) {
  }

  void finish(int r) override {
    dout(20) << "C_GetInstances: " << this << " " <<  __func__ << ": r=" << r
             << dendl;

    if (r == 0) {
      bufferlist::iterator it = out_bl.begin();
      r = librbd::cls_client::mirror_instances_list_finish(&it, instance_ids);
    } else if (r == -ENOENT) {
      r = 0;
    }
    on_finish->complete(r);
  }
};

template <typename I>
struct C_RemoveInstanceRequest : public Context {
  InstanceWatcher<I> instance_watcher;
  Context *on_finish;

  C_RemoveInstanceRequest(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                          const std::string &instance_id, Context *on_finish)
    : instance_watcher(io_ctx, work_queue, nullptr, instance_id),
      on_finish(on_finish) {
  }

  void send() {
    dout(20) << "C_RemoveInstanceRequest: " << this << " " << __func__ << dendl;

    instance_watcher.remove(this);
  }

  void finish(int r) override {
    dout(20) << "C_RemoveInstanceRequest: " << this << " " << __func__ << ": r="
             << r << dendl;
    assert(r == 0);

    on_finish->complete(r);
  }
};

} // anonymous namespace

template <typename I>
struct InstanceWatcher<I>::C_NotifyInstanceRequest : public Context {
  InstanceWatcher<I> *instance_watcher;
  librbd::watcher::Notifier notifier;
  std::string instance_id;
  uint64_t request_id;
  bufferlist bl;
  Context *on_finish;
  librbd::watcher::NotifyResponse response;
  atomic_t canceling;

  C_NotifyInstanceRequest(InstanceWatcher<I> *instance_watcher,
                          const std::string &instance_id, uint64_t request_id,
                          bufferlist &&bl, Context *on_finish)
    : instance_watcher(instance_watcher),
      notifier(instance_watcher->m_work_queue, instance_watcher->m_ioctx,
               RBD_MIRROR_INSTANCE_PREFIX + instance_id),
      instance_id(instance_id), request_id(request_id), bl(bl),
      on_finish(on_finish) {
    instance_watcher->m_notify_op_tracker.start_op();
    assert(instance_watcher->m_lock.is_locked());
    auto result = instance_watcher->m_notify_ops.insert(
        std::make_pair(instance_id, this)).second;
    assert(result);
  }

  void send() {
    dout(20) << "C_NotifyInstanceRequest: " << this << " " << __func__ << dendl;

    notifier.notify(bl, &response, this);
  }

  void cancel() {
    dout(20) << "C_NotifyInstanceRequest: " << this << " " << __func__ << dendl;

    canceling.set(1);
  }

  void finish(int r) override {
    dout(20) << "C_NotifyInstanceRequest: " << this << " " << __func__ << ": r="
             << r << dendl;

    if (r == 0 || r == -ETIMEDOUT) {
      bool found = false;
      for (auto &it : response.acks) {
        auto &bl = it.second;
        if (it.second.length() == 0) {
          dout(20) << "C_NotifyInstanceRequest: " << this << " " << __func__
                   << ": no payload in ack, ignoring" << dendl;
          continue;
        }
        try {
          auto iter = bl.begin();
          NotifyAckPayload ack;
          ::decode(ack, iter);
          if (ack.instance_id != instance_watcher->get_instance_id()) {
            derr << "C_NotifyInstanceRequest: " << this << " " << __func__
                 << ": ack instance_id (" << ack.instance_id << ") "
                 << "does not match, ignoring" << dendl;
            continue;
          }
          if (ack.request_id != request_id) {
            derr << "C_NotifyInstanceRequest: " << this << " " << __func__
                 << ": ack request_id (" << ack.request_id << ") "
                 << "does not match, ignoring" << dendl;
            continue;
          }
          r = ack.ret_val;
          found = true;
          break;
        } catch (const buffer::error &err) {
          derr << "C_NotifyInstanceRequest: " << this << " " << __func__
               << ": failed to decode ack: " << err.what() << dendl;
          continue;
        }
      }

      if (!found) {
        if (r == -ETIMEDOUT) {
          if (canceling.read()) {
            r = -ECANCELED;
          } else {
            derr << "C_NotifyInstanceRequest: " << this << " " << __func__
                 << ": resending after timeout" << dendl;
            send();
            return;
          }
        } else {
          r = -EINVAL;
        }
      }
    }

    instance_watcher->m_notify_op_tracker.finish_op();
    on_finish->complete(r);

    Mutex::Locker locker(instance_watcher->m_lock);
    auto result = instance_watcher->m_notify_ops.erase(
        std::make_pair(instance_id, this));
    assert(result > 0);
    delete this;
  }

  void complete(int r) override {
    finish(r);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::InstanceWatcher: " \
                           << this << " " << __func__ << ": "
template <typename I>
void InstanceWatcher<I>::get_instances(librados::IoCtx &io_ctx,
                                       std::vector<std::string> *instance_ids,
                                       Context *on_finish) {
  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_instances_list_start(&op);
  C_GetInstances *ctx = new C_GetInstances(instance_ids, on_finish);
  librados::AioCompletion *aio_comp = create_rados_callback(ctx);

  int r = io_ctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op, &ctx->out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::remove_instance(librados::IoCtx &io_ctx,
                                         ContextWQ *work_queue,
                                         const std::string &instance_id,
                                         Context *on_finish) {
  auto req = new C_RemoveInstanceRequest<I>(io_ctx, work_queue, instance_id,
                                            on_finish);
  req->send();
}

template <typename I>
InstanceWatcher<I> *InstanceWatcher<I>::create(
    librados::IoCtx &io_ctx, ContextWQ *work_queue,
    InstanceReplayer<I> *instance_replayer) {
  return new InstanceWatcher<I>(io_ctx, work_queue, instance_replayer,
                                stringify(io_ctx.get_instance_id()));
}

template <typename I>
InstanceWatcher<I>::InstanceWatcher(librados::IoCtx &io_ctx,
                                    ContextWQ *work_queue,
                                    InstanceReplayer<I> *instance_replayer,
                                    const std::string &instance_id)
  : Watcher(io_ctx, work_queue, RBD_MIRROR_INSTANCE_PREFIX + instance_id),
    m_instance_replayer(instance_replayer), m_instance_id(instance_id),
    m_lock(unique_lock_name("rbd::mirror::InstanceWatcher::m_lock", this)),
    m_instance_lock(librbd::ManagedLock<I>::create(
      m_ioctx, m_work_queue, m_oid, this, librbd::managed_lock::EXCLUSIVE, true,
      m_cct->_conf->rbd_blacklist_expire_seconds)) {
}

template <typename I>
InstanceWatcher<I>::~InstanceWatcher() {
  m_instance_lock->destroy();
}

template <typename I>
int InstanceWatcher<I>::init() {
  C_SaferCond init_ctx;
  init(&init_ctx);
  return init_ctx.wait();
}

template <typename I>
void InstanceWatcher<I>::init(Context *on_finish) {
  dout(20) << "instance_id=" << m_instance_id << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  register_instance();
}

template <typename I>
void InstanceWatcher<I>::shut_down() {
  C_SaferCond shut_down_ctx;
  shut_down(&shut_down_ctx);
  int r = shut_down_ctx.wait();
  assert(r == 0);
}

template <typename I>
void InstanceWatcher<I>::shut_down(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  release_lock();
}

template <typename I>
void InstanceWatcher<I>::remove(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;
  m_removing = true;

  get_instance_locker();
}

template <typename I>
void InstanceWatcher<I>::notify_image_acquire(
    const std::string &instance_id, const std::string &global_image_id,
    const std::string &peer_mirror_uuid, const std::string &peer_image_id,
  Context *on_notify_ack) {
  dout(20) << "instance_id=" << instance_id << ", global_image_id="
           << global_image_id << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_finish == nullptr);

  if (instance_id == m_instance_id) {
    handle_image_acquire(global_image_id, peer_mirror_uuid, peer_image_id,
                         on_notify_ack);
  } else {
    uint64_t request_id = ++m_request_seq;
    bufferlist bl;
    ::encode(NotifyMessage{ImageAcquirePayload{
        request_id, global_image_id, peer_mirror_uuid, peer_image_id}}, bl);
    auto req = new C_NotifyInstanceRequest(this, instance_id, request_id,
                                           std::move(bl), on_notify_ack);
    req->send();
  }
}

template <typename I>
void InstanceWatcher<I>::notify_image_release(
  const std::string &instance_id, const std::string &global_image_id,
  const std::string &peer_mirror_uuid, const std::string &peer_image_id,
  bool schedule_delete, Context *on_notify_ack) {
  dout(20) << "instance_id=" << instance_id << ", global_image_id="
           << global_image_id << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_finish == nullptr);

  if (instance_id == m_instance_id) {
    handle_image_release(global_image_id, peer_mirror_uuid, peer_image_id,
                         schedule_delete, on_notify_ack);
  } else {
    uint64_t request_id = ++m_request_seq;
    bufferlist bl;
    ::encode(NotifyMessage{ImageReleasePayload{
        request_id, global_image_id, peer_mirror_uuid, peer_image_id,
        schedule_delete}}, bl);
    auto req = new C_NotifyInstanceRequest(this, instance_id, request_id,
                                           std::move(bl), on_notify_ack);
    req->send();
  }
}

template <typename I>
void InstanceWatcher<I>::cancel_notify_requests(
    const std::string &instance_id) {
  dout(20) << "instance_id=" << instance_id << dendl;

  Mutex::Locker locker(m_lock);

  for (auto op : m_notify_ops) {
    if (op.first == instance_id) {
      op.second->cancel();
    }
  }
}


template <typename I>
void InstanceWatcher<I>::register_instance() {
  assert(m_lock.is_locked());

  dout(20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_instances_add(&op, m_instance_id);
  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_register_instance>(this);

  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_register_instance(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    if (r == 0) {
      create_instance_object();
      return;
    }

    derr << "error registering instance: " << cpp_strerror(r) << dendl;

    std::swap(on_finish, m_on_finish);
  }
  on_finish->complete(r);
}


template <typename I>
void InstanceWatcher<I>::create_instance_object() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  librados::ObjectWriteOperation op;
  op.create(true);

  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>,
    &InstanceWatcher<I>::handle_create_instance_object>(this);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_create_instance_object(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error creating " << m_oid << " object: " << cpp_strerror(r)
         << dendl;

    m_ret_val = r;
    unregister_instance();
    return;
  }

  register_watch();
}

template <typename I>
void InstanceWatcher<I>::register_watch() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_register_watch>(this));

  librbd::Watcher::register_watch(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_register_watch(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error registering instance watcher for " << m_oid << " object: "
         << cpp_strerror(r) << dendl;

    m_ret_val = r;
    remove_instance_object();
    return;
  }

  acquire_lock();
}

template <typename I>
void InstanceWatcher<I>::acquire_lock() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_acquire_lock>(this));

  m_instance_lock->acquire_lock(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_acquire_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    if (r < 0) {

      derr << "error acquiring instance lock: " << cpp_strerror(r) << dendl;

      m_ret_val = r;
      unregister_watch();
      return;
    }

    std::swap(on_finish, m_on_finish);
  }

  on_finish->complete(r);
}

template <typename I>
void InstanceWatcher<I>::release_lock() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_release_lock>(this));

  m_instance_lock->shut_down(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_release_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error releasing instance lock: " << cpp_strerror(r) << dendl;
  }

  unregister_watch();
}

template <typename I>
void InstanceWatcher<I>::unregister_watch() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      InstanceWatcher<I>, &InstanceWatcher<I>::handle_unregister_watch>(this));

  librbd::Watcher::unregister_watch(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_unregister_watch(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error unregistering instance watcher for " << m_oid << " object: "
         << cpp_strerror(r) << dendl;
  }

  Mutex::Locker locker(m_lock);
  remove_instance_object();
}

template <typename I>
void InstanceWatcher<I>::remove_instance_object() {
  assert(m_lock.is_locked());

  dout(20) << dendl;

  librados::ObjectWriteOperation op;
  op.remove();

  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>,
    &InstanceWatcher<I>::handle_remove_instance_object>(this);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_remove_instance_object(int r) {
  dout(20) << "r=" << r << dendl;

  if (m_removing && r == -ENOENT) {
    r = 0;
  }

  if (r < 0) {
    derr << "error removing " << m_oid << " object: " << cpp_strerror(r)
         << dendl;
  }

  Mutex::Locker locker(m_lock);
  unregister_instance();
}

template <typename I>
void InstanceWatcher<I>::unregister_instance() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_instances_remove(&op, m_instance_id);
  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_unregister_instance>(this);

  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_unregister_instance(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error unregistering instance: " << cpp_strerror(r) << dendl;
  }

  Mutex::Locker locker(m_lock);
  wait_for_notify_ops();
}

template <typename I>
void InstanceWatcher<I>::wait_for_notify_ops() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  for (auto op : m_notify_ops) {
    op.second->cancel();
  }

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_wait_for_notify_ops>(this));

  m_notify_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_wait_for_notify_ops(int r) {
  dout(20) << "r=" << r << dendl;

  assert(r == 0);

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    assert(m_notify_ops.empty());

    std::swap(on_finish, m_on_finish);
    r = m_ret_val;

    if (m_removing) {
      m_removing = false;
    }
  }
  on_finish->complete(r);
}

template <typename I>
void InstanceWatcher<I>::get_instance_locker() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_get_instance_locker>(this));

  m_instance_lock->get_locker(&m_instance_locker, ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_get_instance_locker(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    if (r != -ENOENT) {
      derr << "error retrieving instance locker: " << cpp_strerror(r) << dendl;
    }
    remove_instance_object();
    return;
  }

  break_instance_lock();
}

template <typename I>
void InstanceWatcher<I>::break_instance_lock() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_break_instance_lock>(this));

  m_instance_lock->break_lock(m_instance_locker, true, ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_break_instance_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    if (r != -ENOENT) {
      derr << "error breaking instance lock: " << cpp_strerror(r) << dendl;
    }
    remove_instance_object();
    return;
  }

  remove_instance_object();
}

template <typename I>
Context *InstanceWatcher<I>::prepare_request(const std::string &instance_id,
                                             uint64_t request_id,
                                             C_NotifyAck *on_notify_ack) {
  dout(20) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  Mutex::Locker locker(m_lock);

  Context *ctx = nullptr;
  Request request(instance_id, request_id);
  auto it = m_requests.find(request);

  if (it != m_requests.end()) {
    dout(20) << "duplicate for in-progress request" << dendl;
    delete it->on_notify_ack;
    m_requests.erase(it);
  } else {
    ctx = new FunctionContext(
      [this, instance_id, request_id] (int r) {
        C_NotifyAck *on_notify_ack = nullptr;
        {
          // update request state in the requests list
          Mutex::Locker locker(m_lock);
          Request request(instance_id, request_id);
          auto it = m_requests.find(request);
          assert(it != m_requests.end());
          on_notify_ack = it->on_notify_ack;
          m_requests.erase(it);
        }

        ::encode(NotifyAckPayload(instance_id, request_id, r),
                 on_notify_ack->out);
        on_notify_ack->complete(0);
      });
  }

  request.on_notify_ack = on_notify_ack;
  m_requests.insert(request);
  return ctx;
}

template <typename I>
void InstanceWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
                                       uint64_t notifier_id, bufferlist &bl) {
  dout(20) << "notify_id=" << notify_id << ", handle=" << handle << ", "
           << "notifier_id=" << notifier_id << dendl;

  auto ctx = new C_NotifyAck(this, notify_id, handle);

  NotifyMessage notify_message;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(notify_message, iter);
  } catch (const buffer::error &err) {
    derr << "error decoding image notification: " << err.what() << dendl;
    ctx->complete(0);
    return;
  }

  apply_visitor(HandlePayloadVisitor(this, stringify(notifier_id), ctx),
                notify_message.payload);
}

template <typename I>
void InstanceWatcher<I>::handle_image_acquire(
  const std::string &global_image_id, const std::string &peer_mirror_uuid,
  const std::string &peer_image_id, Context *on_finish) {
  dout(20) << "global_image_id=" << global_image_id << dendl;

  m_instance_replayer->acquire_image(global_image_id, peer_mirror_uuid,
                                     peer_image_id, on_finish);
}

template <typename I>
void InstanceWatcher<I>::handle_image_release(
  const std::string &global_image_id,  const std::string &peer_mirror_uuid,
  const std::string &peer_image_id, bool schedule_delete, Context *on_finish) {
  dout(20) << "global_image_id=" << global_image_id << dendl;

  m_instance_replayer->release_image(global_image_id, peer_mirror_uuid,
                                     peer_image_id, schedule_delete, on_finish);
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const std::string &instance_id,
                                        const ImageAcquirePayload &payload,
                                        C_NotifyAck *on_notify_ack) {
  dout(20) << "image_acquire: instance_id=" << instance_id << ", "
           << "request_id=" << payload.request_id << dendl;

  auto on_finish = prepare_request(instance_id, payload.request_id,
                                   on_notify_ack);
  if (on_finish != nullptr) {
    handle_image_acquire(payload.global_image_id, payload.peer_mirror_uuid,
                         payload.peer_image_id, on_finish);
  }
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const std::string &instance_id,
                                        const ImageReleasePayload &payload,
                                        C_NotifyAck *on_notify_ack) {
  dout(20) << "image_release: instance_id=" << instance_id << ", "
           << "request_id=" << payload.request_id << dendl;

  auto on_finish = prepare_request(instance_id, payload.request_id,
                                   on_notify_ack);
  if (on_finish != nullptr) {
    handle_image_release(payload.global_image_id, payload.peer_mirror_uuid,
                         payload.peer_image_id, payload.schedule_delete,
                         on_finish);
  }
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const std::string &instance_id,
                                        const UnknownPayload &payload,
                                        C_NotifyAck *on_notify_ack) {
  dout(20) << "unknown: instance_id=" << instance_id << dendl;

  on_notify_ack->complete(0);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::InstanceWatcher<librbd::ImageCtx>;
