// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "InstanceWatcher.h"
#include "include/stringify.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/AsioEngine.h"
#include "librbd/ManagedLock.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "InstanceReplayer.h"
#include "Throttler.h"
#include "common/Cond.h"

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
    dout(10) << "C_GetInstances: " << this << " " <<  __func__ << ": r=" << r
             << dendl;

    if (r == 0) {
      auto it = out_bl.cbegin();
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

  C_RemoveInstanceRequest(librados::IoCtx &io_ctx,
                          librbd::AsioEngine& asio_engine,
                          const std::string &instance_id, Context *on_finish)
    : instance_watcher(io_ctx, asio_engine, nullptr, nullptr, instance_id),
      on_finish(on_finish) {
  }

  void send() {
    dout(10) << "C_RemoveInstanceRequest: " << this << " " << __func__ << dendl;

    instance_watcher.remove(this);
  }

  void finish(int r) override {
    dout(10) << "C_RemoveInstanceRequest: " << this << " " << __func__ << ": r="
             << r << dendl;
    ceph_assert(r == 0);

    on_finish->complete(r);
  }
};

} // anonymous namespace

template <typename I>
struct InstanceWatcher<I>::C_NotifyInstanceRequest : public Context {
  InstanceWatcher<I> *instance_watcher;
  std::string instance_id;
  uint64_t request_id;
  bufferlist bl;
  Context *on_finish;
  bool send_to_leader;
  std::unique_ptr<librbd::watcher::Notifier> notifier;
  librbd::watcher::NotifyResponse response;
  bool canceling = false;

  C_NotifyInstanceRequest(InstanceWatcher<I> *instance_watcher,
                          const std::string &instance_id, uint64_t request_id,
                          bufferlist &&bl, Context *on_finish)
    : instance_watcher(instance_watcher), instance_id(instance_id),
      request_id(request_id), bl(bl), on_finish(on_finish),
      send_to_leader(instance_id.empty()) {
    dout(10) << "C_NotifyInstanceRequest: " << this << " " << __func__
             << ": instance_watcher=" << instance_watcher << ", instance_id="
             << instance_id << ", request_id=" << request_id << dendl;

    ceph_assert(ceph_mutex_is_locked(instance_watcher->m_lock));

    if (!send_to_leader) {
      ceph_assert((!instance_id.empty()));
      notifier.reset(new librbd::watcher::Notifier(
                         instance_watcher->m_work_queue,
                         instance_watcher->m_ioctx,
                         RBD_MIRROR_INSTANCE_PREFIX + instance_id));
    }

    instance_watcher->m_notify_op_tracker.start_op();
    auto result = instance_watcher->m_notify_ops.insert(
        std::make_pair(instance_id, this)).second;
    ceph_assert(result);
  }

  void send() {
    dout(10) << "C_NotifyInstanceRequest: " << this << " " << __func__ << dendl;

    ceph_assert(ceph_mutex_is_locked(instance_watcher->m_lock));

    if (canceling) {
      dout(10) << "C_NotifyInstanceRequest: " << this << " " << __func__
               << ": canceling" << dendl;
      instance_watcher->m_work_queue->queue(this, -ECANCELED);
      return;
    }

    if (send_to_leader) {
      if (instance_watcher->m_leader_instance_id.empty()) {
        dout(10) << "C_NotifyInstanceRequest: " << this << " " << __func__
                 << ": suspending" << dendl;
        instance_watcher->suspend_notify_request(this);
        return;
      }

      if (instance_watcher->m_leader_instance_id != instance_id) {
        auto count = instance_watcher->m_notify_ops.erase(
            std::make_pair(instance_id, this));
        ceph_assert(count > 0);

        instance_id = instance_watcher->m_leader_instance_id;

        auto result = instance_watcher->m_notify_ops.insert(
            std::make_pair(instance_id, this)).second;
        ceph_assert(result);

        notifier.reset(new librbd::watcher::Notifier(
                           instance_watcher->m_work_queue,
                           instance_watcher->m_ioctx,
                           RBD_MIRROR_INSTANCE_PREFIX + instance_id));
      }
    }

    dout(10) << "C_NotifyInstanceRequest: " << this << " " << __func__
             << ": sending to " << instance_id << dendl;
    notifier->notify(bl, &response, this);
  }

  void cancel() {
    dout(10) << "C_NotifyInstanceRequest: " << this << " " << __func__ << dendl;

    ceph_assert(ceph_mutex_is_locked(instance_watcher->m_lock));

    canceling = true;
    instance_watcher->unsuspend_notify_request(this);
  }

  void finish(int r) override {
    dout(10) << "C_NotifyInstanceRequest: " << this << " " << __func__ << ": r="
             << r << dendl;

    if (r == 0 || r == -ETIMEDOUT) {
      bool found = false;
      for (auto &it : response.acks) {
        auto &bl = it.second;
        if (it.second.length() == 0) {
          dout(5) << "C_NotifyInstanceRequest: " << this << " " << __func__
                  << ": no payload in ack, ignoring" << dendl;
          continue;
        }
        try {
          auto iter = bl.cbegin();
          NotifyAckPayload ack;
          decode(ack, iter);
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
          derr << "C_NotifyInstanceRequest: " << this << " " << __func__
               << ": resending after timeout" << dendl;
	  std::lock_guard locker{instance_watcher->m_lock};
          send();
          return;
        } else {
          r = -EINVAL;
        }
      } else {
        if (r == -ESTALE && send_to_leader) {
          derr << "C_NotifyInstanceRequest: " << this << " " << __func__
               << ": resending due to leader change" << dendl;
	  std::lock_guard locker{instance_watcher->m_lock};
          send();
          return;
        }
      }
    }

    on_finish->complete(r);

    {
      std::lock_guard locker{instance_watcher->m_lock};
      auto result = instance_watcher->m_notify_ops.erase(
        std::make_pair(instance_id, this));
      ceph_assert(result > 0);
      instance_watcher->m_notify_op_tracker.finish_op();
    }

    delete this;
  }

  void complete(int r) override {
    finish(r);
  }
};

template <typename I>
struct InstanceWatcher<I>::C_SyncRequest : public Context {
  InstanceWatcher<I> *instance_watcher;
  std::string sync_id;
  Context *on_start;
  Context *on_complete = nullptr;
  C_NotifyInstanceRequest *req = nullptr;

  C_SyncRequest(InstanceWatcher<I> *instance_watcher,
                const std::string &sync_id, Context *on_start)
    : instance_watcher(instance_watcher), sync_id(sync_id),
      on_start(on_start) {
    dout(10) << "C_SyncRequest: " << this << " " << __func__ << ": sync_id="
             << sync_id << dendl;
  }

  void finish(int r) override {
    dout(10) << "C_SyncRequest: " << this << " " << __func__ << ": r="
             << r << dendl;

    if (on_start != nullptr) {
      instance_watcher->handle_notify_sync_request(this, r);
    } else {
      instance_watcher->handle_notify_sync_complete(this, r);
      delete this;
    }
  }

  // called twice
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
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::remove_instance(librados::IoCtx &io_ctx,
                                         librbd::AsioEngine& asio_engine,
                                         const std::string &instance_id,
                                         Context *on_finish) {
  auto req = new C_RemoveInstanceRequest<I>(io_ctx, asio_engine, instance_id,
                                            on_finish);
  req->send();
}

template <typename I>
InstanceWatcher<I> *InstanceWatcher<I>::create(
    librados::IoCtx &io_ctx, librbd::AsioEngine& asio_engine,
    InstanceReplayer<I> *instance_replayer,
    Throttler<I> *image_sync_throttler) {
  return new InstanceWatcher<I>(io_ctx, asio_engine, instance_replayer,
                                image_sync_throttler,
                                stringify(io_ctx.get_instance_id()));
}

template <typename I>
InstanceWatcher<I>::InstanceWatcher(librados::IoCtx &io_ctx,
                                    librbd::AsioEngine& asio_engine,
                                    InstanceReplayer<I> *instance_replayer,
                                    Throttler<I> *image_sync_throttler,
                                    const std::string &instance_id)
  : Watcher(io_ctx, asio_engine.get_work_queue(),
            RBD_MIRROR_INSTANCE_PREFIX + instance_id),
    m_instance_replayer(instance_replayer),
    m_image_sync_throttler(image_sync_throttler), m_instance_id(instance_id),
    m_lock(ceph::make_mutex(
      unique_lock_name("rbd::mirror::InstanceWatcher::m_lock", this))),
    m_instance_lock(librbd::ManagedLock<I>::create(
      m_ioctx, asio_engine, m_oid, this, librbd::managed_lock::EXCLUSIVE, true,
      m_cct->_conf.get_val<uint64_t>("rbd_blacklist_expire_seconds"))) {
}

template <typename I>
InstanceWatcher<I>::~InstanceWatcher() {
  ceph_assert(m_requests.empty());
  ceph_assert(m_notify_ops.empty());
  ceph_assert(m_notify_op_tracker.empty());
  ceph_assert(m_suspended_ops.empty());
  ceph_assert(m_inflight_sync_reqs.empty());
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
  dout(10) << "instance_id=" << m_instance_id << dendl;

  std::lock_guard locker{m_lock};

  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  register_instance();
}

template <typename I>
void InstanceWatcher<I>::shut_down() {
  C_SaferCond shut_down_ctx;
  shut_down(&shut_down_ctx);
  int r = shut_down_ctx.wait();
  ceph_assert(r == 0);
}

template <typename I>
void InstanceWatcher<I>::shut_down(Context *on_finish) {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  release_lock();
}

template <typename I>
void InstanceWatcher<I>::remove(Context *on_finish) {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  get_instance_locker();
}

template <typename I>
void InstanceWatcher<I>::notify_image_acquire(
    const std::string &instance_id, const std::string &global_image_id,
    Context *on_notify_ack) {
  dout(10) << "instance_id=" << instance_id << ", global_image_id="
           << global_image_id << dendl;

  std::lock_guard locker{m_lock};

  ceph_assert(m_on_finish == nullptr);

  uint64_t request_id = ++m_request_seq;
  bufferlist bl;
  encode(NotifyMessage{ImageAcquirePayload{request_id, global_image_id}}, bl);
  auto req = new C_NotifyInstanceRequest(this, instance_id, request_id,
                                         std::move(bl), on_notify_ack);
  req->send();
}

template <typename I>
void InstanceWatcher<I>::notify_image_release(
    const std::string &instance_id, const std::string &global_image_id,
    Context *on_notify_ack) {
  dout(10) << "instance_id=" << instance_id << ", global_image_id="
           << global_image_id << dendl;

  std::lock_guard locker{m_lock};

  ceph_assert(m_on_finish == nullptr);

  uint64_t request_id = ++m_request_seq;
  bufferlist bl;
  encode(NotifyMessage{ImageReleasePayload{request_id, global_image_id}}, bl);
  auto req = new C_NotifyInstanceRequest(this, instance_id, request_id,
                                         std::move(bl), on_notify_ack);
  req->send();
}

template <typename I>
void InstanceWatcher<I>::notify_peer_image_removed(
    const std::string &instance_id, const std::string &global_image_id,
    const std::string &peer_mirror_uuid, Context *on_notify_ack) {
  dout(10) << "instance_id=" << instance_id << ", "
           << "global_image_id=" << global_image_id << ", "
           << "peer_mirror_uuid=" << peer_mirror_uuid << dendl;

  std::lock_guard locker{m_lock};
  ceph_assert(m_on_finish == nullptr);

  uint64_t request_id = ++m_request_seq;
  bufferlist bl;
  encode(NotifyMessage{PeerImageRemovedPayload{request_id, global_image_id,
                                               peer_mirror_uuid}}, bl);
  auto req = new C_NotifyInstanceRequest(this, instance_id, request_id,
                                         std::move(bl), on_notify_ack);
  req->send();
}

template <typename I>
void InstanceWatcher<I>::notify_sync_request(const std::string &sync_id,
                                             Context *on_sync_start) {
  dout(10) << "sync_id=" << sync_id << dendl;

  std::lock_guard locker{m_lock};

  ceph_assert(m_inflight_sync_reqs.count(sync_id) == 0);

  uint64_t request_id = ++m_request_seq;

  bufferlist bl;
  encode(NotifyMessage{SyncRequestPayload{request_id, sync_id}}, bl);

  auto sync_ctx = new C_SyncRequest(this, sync_id, on_sync_start);
  sync_ctx->req = new C_NotifyInstanceRequest(this, "", request_id,
                                              std::move(bl), sync_ctx);

  m_inflight_sync_reqs[sync_id] = sync_ctx;
  sync_ctx->req->send();
}

template <typename I>
bool InstanceWatcher<I>::cancel_sync_request(const std::string &sync_id) {
  dout(10) << "sync_id=" << sync_id << dendl;

  std::lock_guard locker{m_lock};

  auto it = m_inflight_sync_reqs.find(sync_id);
  if (it == m_inflight_sync_reqs.end()) {
    return false;
  }

  auto sync_ctx = it->second;

  if (sync_ctx->on_start == nullptr) {
    return false;
  }

  ceph_assert(sync_ctx->req != nullptr);
  sync_ctx->req->cancel();
  return true;
}

template <typename I>
void InstanceWatcher<I>::notify_sync_start(const std::string &instance_id,
                                           const std::string &sync_id) {
  dout(10) << "sync_id=" << sync_id << dendl;

  std::lock_guard locker{m_lock};

  uint64_t request_id = ++m_request_seq;

  bufferlist bl;
  encode(NotifyMessage{SyncStartPayload{request_id, sync_id}}, bl);

  auto ctx = new LambdaContext(
    [this, sync_id] (int r) {
      dout(10) << "finish: sync_id=" << sync_id << ", r=" << r << dendl;
      std::lock_guard locker{m_lock};
      if (r != -ESTALE && is_leader()) {
        m_image_sync_throttler->finish_op(m_ioctx.get_namespace(), sync_id);
      }
    });
  auto req = new C_NotifyInstanceRequest(this, instance_id, request_id,
                                         std::move(bl), ctx);
  req->send();
}

template <typename I>
void InstanceWatcher<I>::notify_sync_complete(const std::string &sync_id) {
  std::lock_guard locker{m_lock};
  notify_sync_complete(m_lock, sync_id);
}

template <typename I>
void InstanceWatcher<I>::notify_sync_complete(const ceph::mutex&,
                                              const std::string &sync_id) {
  dout(10) << "sync_id=" << sync_id << dendl;
  ceph_assert(ceph_mutex_is_locked(m_lock));

  auto it = m_inflight_sync_reqs.find(sync_id);
  ceph_assert(it != m_inflight_sync_reqs.end());

  auto sync_ctx = it->second;
  ceph_assert(sync_ctx->req == nullptr);

  m_inflight_sync_reqs.erase(it);
  m_work_queue->queue(sync_ctx, 0);
}

template <typename I>
void InstanceWatcher<I>::handle_notify_sync_request(C_SyncRequest *sync_ctx,
                                                    int r) {
  dout(10) << "sync_id=" << sync_ctx->sync_id << ", r=" << r << dendl;

  Context *on_start = nullptr;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(sync_ctx->req != nullptr);
    ceph_assert(sync_ctx->on_start != nullptr);

    if (sync_ctx->req->canceling) {
      r = -ECANCELED;
    }

    std::swap(sync_ctx->on_start, on_start);
    sync_ctx->req = nullptr;

    if (r == -ECANCELED) {
      notify_sync_complete(m_lock, sync_ctx->sync_id);
    }
  }

  on_start->complete(r == -ECANCELED ? r : 0);
}

template <typename I>
void InstanceWatcher<I>::handle_notify_sync_complete(C_SyncRequest *sync_ctx,
                                                     int r) {
  dout(10) << "sync_id=" << sync_ctx->sync_id << ", r=" << r << dendl;

  if (sync_ctx->on_complete != nullptr) {
    sync_ctx->on_complete->complete(r);
  }
}

template <typename I>
void InstanceWatcher<I>::handle_acquire_leader() {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  m_leader_instance_id = m_instance_id;
  unsuspend_notify_requests();
}

template <typename I>
void InstanceWatcher<I>::handle_release_leader() {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  m_leader_instance_id.clear();

  m_image_sync_throttler->drain(m_ioctx.get_namespace(), -ESTALE);
}

template <typename I>
void InstanceWatcher<I>::handle_update_leader(
  const std::string &leader_instance_id) {
  dout(10) << "leader_instance_id=" << leader_instance_id << dendl;

  std::lock_guard locker{m_lock};

  m_leader_instance_id = leader_instance_id;

  if (!m_leader_instance_id.empty()) {
    unsuspend_notify_requests();
  }
}

template <typename I>
void InstanceWatcher<I>::cancel_notify_requests(
    const std::string &instance_id) {
  dout(10) << "instance_id=" << instance_id << dendl;

  std::lock_guard locker{m_lock};

  for (auto op : m_notify_ops) {
    if (op.first == instance_id && !op.second->send_to_leader) {
      op.second->cancel();
    }
  }
}

template <typename I>
void InstanceWatcher<I>::register_instance() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_instances_add(&op, m_instance_id);
  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_register_instance>(this);

  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_register_instance(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    std::lock_guard locker{m_lock};

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
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  librados::ObjectWriteOperation op;
  op.create(true);

  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>,
    &InstanceWatcher<I>::handle_create_instance_object>(this);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_create_instance_object(int r) {
  dout(10) << "r=" << r << dendl;

  std::lock_guard locker{m_lock};

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
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_register_watch>(this));

  librbd::Watcher::register_watch(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_register_watch(int r) {
  dout(10) << "r=" << r << dendl;

  std::lock_guard locker{m_lock};

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
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_acquire_lock>(this));

  m_instance_lock->acquire_lock(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_acquire_lock(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    std::lock_guard locker{m_lock};

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
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_release_lock>(this));

  m_instance_lock->shut_down(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_release_lock(int r) {
  dout(10) << "r=" << r << dendl;

  std::lock_guard locker{m_lock};

  if (r < 0) {
    derr << "error releasing instance lock: " << cpp_strerror(r) << dendl;
  }

  unregister_watch();
}

template <typename I>
void InstanceWatcher<I>::unregister_watch() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      InstanceWatcher<I>, &InstanceWatcher<I>::handle_unregister_watch>(this));

  librbd::Watcher::unregister_watch(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_unregister_watch(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error unregistering instance watcher for " << m_oid << " object: "
         << cpp_strerror(r) << dendl;
  }

  std::lock_guard locker{m_lock};
  remove_instance_object();
}

template <typename I>
void InstanceWatcher<I>::remove_instance_object() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  op.remove();

  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>,
    &InstanceWatcher<I>::handle_remove_instance_object>(this);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_remove_instance_object(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    r = 0;
  }

  if (r < 0) {
    derr << "error removing " << m_oid << " object: " << cpp_strerror(r)
         << dendl;
  }

  std::lock_guard locker{m_lock};
  unregister_instance();
}

template <typename I>
void InstanceWatcher<I>::unregister_instance() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_instances_remove(&op, m_instance_id);
  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_unregister_instance>(this);

  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_unregister_instance(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error unregistering instance: " << cpp_strerror(r) << dendl;
  }

  std::lock_guard locker{m_lock};
  wait_for_notify_ops();
}

template <typename I>
void InstanceWatcher<I>::wait_for_notify_ops() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

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
  dout(10) << "r=" << r << dendl;

  ceph_assert(r == 0);

  Context *on_finish = nullptr;
  {
    std::lock_guard locker{m_lock};

    ceph_assert(m_notify_ops.empty());

    std::swap(on_finish, m_on_finish);
    r = m_ret_val;
  }
  on_finish->complete(r);
}

template <typename I>
void InstanceWatcher<I>::get_instance_locker() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_get_instance_locker>(this));

  m_instance_lock->get_locker(&m_instance_locker, ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_get_instance_locker(int r) {
  dout(10) << "r=" << r << dendl;

  std::lock_guard locker{m_lock};

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
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_break_instance_lock>(this));

  m_instance_lock->break_lock(m_instance_locker, true, ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_break_instance_lock(int r) {
  dout(10) << "r=" << r << dendl;

  std::lock_guard locker{m_lock};

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
void InstanceWatcher<I>::suspend_notify_request(C_NotifyInstanceRequest *req) {
  dout(10) << req << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  auto result = m_suspended_ops.insert(req).second;
  ceph_assert(result);
}

template <typename I>
bool InstanceWatcher<I>::unsuspend_notify_request(
  C_NotifyInstanceRequest *req) {
  dout(10) << req << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  auto result = m_suspended_ops.erase(req);
  if (result == 0) {
    return false;
  }

  req->send();
  return true;
}

template <typename I>
void InstanceWatcher<I>::unsuspend_notify_requests() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  std::set<C_NotifyInstanceRequest *> suspended_ops;
  std::swap(m_suspended_ops, suspended_ops);

  for (auto op : suspended_ops) {
    op->send();
  }
}

template <typename I>
Context *InstanceWatcher<I>::prepare_request(const std::string &instance_id,
                                             uint64_t request_id,
                                             C_NotifyAck *on_notify_ack) {
  dout(10) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  std::lock_guard locker{m_lock};

  Context *ctx = nullptr;
  Request request(instance_id, request_id);
  auto it = m_requests.find(request);

  if (it != m_requests.end()) {
    dout(10) << "duplicate for in-progress request" << dendl;
    delete it->on_notify_ack;
    m_requests.erase(it);
  } else {
    ctx = create_async_context_callback(
        m_work_queue, new LambdaContext(
            [this, instance_id, request_id] (int r) {
              complete_request(instance_id, request_id, r);
            }));
  }

  request.on_notify_ack = on_notify_ack;
  m_requests.insert(request);
  return ctx;
}

template <typename I>
void InstanceWatcher<I>::complete_request(const std::string &instance_id,
                                          uint64_t request_id, int r) {
  dout(10) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  C_NotifyAck *on_notify_ack;
  {
    std::lock_guard locker{m_lock};
    Request request(instance_id, request_id);
    auto it = m_requests.find(request);
    ceph_assert(it != m_requests.end());
    on_notify_ack = it->on_notify_ack;
    m_requests.erase(it);
  }

  encode(NotifyAckPayload(instance_id, request_id, r), on_notify_ack->out);
  on_notify_ack->complete(0);
}

template <typename I>
void InstanceWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
                                       uint64_t notifier_id, bufferlist &bl) {
  dout(10) << "notify_id=" << notify_id << ", handle=" << handle << ", "
           << "notifier_id=" << notifier_id << dendl;

  auto ctx = new C_NotifyAck(this, notify_id, handle);

  NotifyMessage notify_message;
  try {
    auto iter = bl.cbegin();
    decode(notify_message, iter);
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
    const std::string &global_image_id, Context *on_finish) {
  dout(10) << "global_image_id=" << global_image_id << dendl;

  auto ctx = new LambdaContext(
      [this, global_image_id, on_finish] (int r) {
        m_instance_replayer->acquire_image(this, global_image_id, on_finish);
        m_notify_op_tracker.finish_op();
      });

  m_notify_op_tracker.start_op();
  m_work_queue->queue(ctx, 0);
}

template <typename I>
void InstanceWatcher<I>::handle_image_release(
    const std::string &global_image_id, Context *on_finish) {
  dout(10) << "global_image_id=" << global_image_id << dendl;

  auto ctx = new LambdaContext(
      [this, global_image_id, on_finish] (int r) {
        m_instance_replayer->release_image(global_image_id, on_finish);
        m_notify_op_tracker.finish_op();
      });

  m_notify_op_tracker.start_op();
  m_work_queue->queue(ctx, 0);
}

template <typename I>
void InstanceWatcher<I>::handle_peer_image_removed(
    const std::string &global_image_id, const std::string &peer_mirror_uuid,
    Context *on_finish) {
  dout(10) << "global_image_id=" << global_image_id << ", "
           << "peer_mirror_uuid=" << peer_mirror_uuid << dendl;

  auto ctx = new LambdaContext(
      [this, peer_mirror_uuid, global_image_id, on_finish] (int r) {
        m_instance_replayer->remove_peer_image(global_image_id,
                                               peer_mirror_uuid, on_finish);
        m_notify_op_tracker.finish_op();
      });

  m_notify_op_tracker.start_op();
  m_work_queue->queue(ctx, 0);
}

template <typename I>
void InstanceWatcher<I>::handle_sync_request(const std::string &instance_id,
                                             const std::string &sync_id,
                                             Context *on_finish) {
  dout(10) << "instance_id=" << instance_id << ", sync_id=" << sync_id << dendl;

  std::lock_guard locker{m_lock};

  if (!is_leader()) {
    dout(10) << "sync request for non-leader" << dendl;
    m_work_queue->queue(on_finish, -ESTALE);
    return;
  }

  Context *on_start = create_async_context_callback(
    m_work_queue, new LambdaContext(
      [this, instance_id, sync_id, on_finish] (int r) {
        dout(10) << "handle_sync_request: finish: instance_id=" << instance_id
                 << ", sync_id=" << sync_id << ", r=" << r << dendl;
        if (r == 0) {
          notify_sync_start(instance_id, sync_id);
        }
        if (r == -ENOENT) {
          r = 0;
        }
        on_finish->complete(r);
      }));
  m_image_sync_throttler->start_op(m_ioctx.get_namespace(), sync_id, on_start);
}

template <typename I>
void InstanceWatcher<I>::handle_sync_start(const std::string &instance_id,
                                           const std::string &sync_id,
                                           Context *on_finish) {
  dout(10) << "instance_id=" << instance_id << ", sync_id=" << sync_id << dendl;

  std::lock_guard locker{m_lock};

  auto it = m_inflight_sync_reqs.find(sync_id);
  if (it == m_inflight_sync_reqs.end()) {
    dout(5) << "not found" << dendl;
    m_work_queue->queue(on_finish, 0);
    return;
  }

  auto sync_ctx = it->second;

  if (sync_ctx->on_complete != nullptr) {
    dout(5) << "duplicate request" << dendl;
    m_work_queue->queue(sync_ctx->on_complete, -ESTALE);
  }

  sync_ctx->on_complete = on_finish;
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const std::string &instance_id,
                                        const ImageAcquirePayload &payload,
                                        C_NotifyAck *on_notify_ack) {
  dout(10) << "image_acquire: instance_id=" << instance_id << ", "
           << "request_id=" << payload.request_id << dendl;

  auto on_finish = prepare_request(instance_id, payload.request_id,
                                   on_notify_ack);
  if (on_finish != nullptr) {
    handle_image_acquire(payload.global_image_id, on_finish);
  }
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const std::string &instance_id,
                                        const ImageReleasePayload &payload,
                                        C_NotifyAck *on_notify_ack) {
  dout(10) << "image_release: instance_id=" << instance_id << ", "
           << "request_id=" << payload.request_id << dendl;

  auto on_finish = prepare_request(instance_id, payload.request_id,
                                   on_notify_ack);
  if (on_finish != nullptr) {
    handle_image_release(payload.global_image_id, on_finish);
  }
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const std::string &instance_id,
                                        const PeerImageRemovedPayload &payload,
                                        C_NotifyAck *on_notify_ack) {
  dout(10) << "remove_peer_image: instance_id=" << instance_id << ", "
           << "request_id=" << payload.request_id << dendl;

  auto on_finish = prepare_request(instance_id, payload.request_id,
                                   on_notify_ack);
  if (on_finish != nullptr) {
    handle_peer_image_removed(payload.global_image_id, payload.peer_mirror_uuid,
                              on_finish);
  }
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const std::string &instance_id,
                                        const SyncRequestPayload &payload,
                                        C_NotifyAck *on_notify_ack) {
  dout(10) << "sync_request: instance_id=" << instance_id << ", "
           << "request_id=" << payload.request_id << dendl;

  auto on_finish = prepare_request(instance_id, payload.request_id,
                                   on_notify_ack);
  if (on_finish == nullptr) {
    return;
  }

  handle_sync_request(instance_id, payload.sync_id, on_finish);
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const std::string &instance_id,
                                        const SyncStartPayload &payload,
                                        C_NotifyAck *on_notify_ack) {
  dout(10) << "sync_start: instance_id=" << instance_id << ", "
           << "request_id=" << payload.request_id << dendl;

  auto on_finish = prepare_request(instance_id, payload.request_id,
                                   on_notify_ack);
  if (on_finish == nullptr) {
    return;
  }

  handle_sync_start(instance_id, payload.sync_id, on_finish);
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const std::string &instance_id,
                                        const UnknownPayload &payload,
                                        C_NotifyAck *on_notify_ack) {
  dout(5) << "unknown: instance_id=" << instance_id << dendl;

  on_notify_ack->complete(0);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::InstanceWatcher<librbd::ImageCtx>;
