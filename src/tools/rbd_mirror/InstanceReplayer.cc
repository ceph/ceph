// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "common/Timer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "ImageReplayer.h"
#include "InstanceReplayer.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::InstanceReplayer: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;

template <typename I>
InstanceReplayer<I>::InstanceReplayer(
    Threads<I> *threads, std::shared_ptr<ImageDeleter> image_deleter,
    RadosRef local_rados, const std::string &local_mirror_uuid,
    int64_t local_pool_id)
  : m_threads(threads), m_image_deleter(image_deleter),
    m_local_rados(local_rados), m_local_mirror_uuid(local_mirror_uuid),
    m_local_pool_id(local_pool_id),
    m_lock("rbd::mirror::InstanceReplayer " + stringify(local_pool_id)) {
}

template <typename I>
InstanceReplayer<I>::~InstanceReplayer() {
  assert(m_image_state_check_task == nullptr);
  assert(m_async_op_tracker.empty());
  assert(m_image_replayers.empty());
}

template <typename I>
int InstanceReplayer<I>::init() {
  C_SaferCond init_ctx;
  init(&init_ctx);
  return init_ctx.wait();
}

template <typename I>
void InstanceReplayer<I>::init(Context *on_finish) {
  dout(20) << dendl;

  Context *ctx = new FunctionContext(
    [this, on_finish] (int r) {
      {
        Mutex::Locker timer_locker(m_threads->timer_lock);
        schedule_image_state_check_task();
      }
      on_finish->complete(0);
    });

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void InstanceReplayer<I>::shut_down() {
  C_SaferCond shut_down_ctx;
  shut_down(&shut_down_ctx);
  int r = shut_down_ctx.wait();
  assert(r == 0);
}

template <typename I>
void InstanceReplayer<I>::shut_down(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_shut_down == nullptr);
  m_on_shut_down = on_finish;

  Context *ctx = new FunctionContext(
    [this] (int r) {
      cancel_image_state_check_task();
      wait_for_ops();
    });

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void InstanceReplayer<I>::add_peer(std::string mirror_uuid,
                                   librados::IoCtx io_ctx) {
  dout(20) << mirror_uuid << dendl;

  Mutex::Locker locker(m_lock);
  auto result = m_peers.insert(Peer(mirror_uuid, io_ctx)).second;
  assert(result);
}

template <typename I>
void InstanceReplayer<I>::remove_peer(std::string mirror_uuid) {
  dout(20) << mirror_uuid << dendl;

  Mutex::Locker locker(m_lock);
  auto result = m_peers.erase(Peer(mirror_uuid));
  assert(result > 0);
}

template <typename I>
void InstanceReplayer<I>::release_all(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, on_finish);
  for (auto it = m_image_replayers.begin(); it != m_image_replayers.end();
       it = m_image_replayers.erase(it)) {
    auto image_replayer = it->second;
    auto ctx = gather_ctx->new_sub();
    ctx = new FunctionContext(
      [image_replayer, ctx] (int r) {
        image_replayer->destroy();
        ctx->complete(0);
      });
    stop_image_replayer(image_replayer, ctx);
  }
  gather_ctx->activate();
}

template <typename I>
void InstanceReplayer<I>::acquire_image(InstanceWatcher<I> *instance_watcher,
                                        const std::string &global_image_id,
                                        const std::string &peer_mirror_uuid,
                                        const std::string &peer_image_id,
                                        Context *on_finish) {
  dout(20) << "global_image_id=" << global_image_id << ", peer_mirror_uuid="
           << peer_mirror_uuid << ", peer_image_id=" << peer_image_id << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_shut_down == nullptr);

  auto it = m_image_replayers.find(global_image_id);

  if (it == m_image_replayers.end()) {
    auto image_replayer = ImageReplayer<I>::create(
        m_threads, m_image_deleter, instance_watcher, m_local_rados,
        m_local_mirror_uuid, m_local_pool_id, global_image_id);

    dout(20) << global_image_id << ": creating replayer " << image_replayer
             << dendl;

    it = m_image_replayers.insert(std::make_pair(global_image_id,
                                                 image_replayer)).first;
  }

  auto image_replayer = it->second;
  if (!peer_mirror_uuid.empty()) {
    auto iter = m_peers.find(Peer(peer_mirror_uuid));
    assert(iter != m_peers.end());
    auto io_ctx = iter->io_ctx;

    image_replayer->add_remote_image(peer_mirror_uuid, peer_image_id, io_ctx);
  }
  start_image_replayer(image_replayer);

  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
void InstanceReplayer<I>::release_image(const std::string &global_image_id,
                                        const std::string &peer_mirror_uuid,
                                        const std::string &peer_image_id,
                                        bool schedule_delete,
                                        Context *on_finish) {
  dout(20) << "global_image_id=" << global_image_id << ", peer_mirror_uuid="
           << peer_mirror_uuid << ", peer_image_id=" << peer_image_id << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_shut_down == nullptr);

  auto it = m_image_replayers.find(global_image_id);

  if (it == m_image_replayers.end()) {
    dout(20) << global_image_id << ": not found" << dendl;
    m_threads->work_queue->queue(on_finish, 0);
    return;
  }

  auto image_replayer = it->second;
  if (!peer_mirror_uuid.empty()) {
    image_replayer->remove_remote_image(peer_mirror_uuid, peer_image_id,
					schedule_delete);
  }

  if (!image_replayer->remote_images_empty()) {
    dout(20) << global_image_id << ": still has peer images" << dendl;
    m_threads->work_queue->queue(on_finish, 0);
    return;
  }

  m_image_replayers.erase(it);

  on_finish = new FunctionContext(
    [image_replayer, on_finish] (int r) {
      image_replayer->destroy();
      on_finish->complete(0);
    });

  if (schedule_delete) {
    on_finish = new FunctionContext(
      [this, image_replayer, on_finish] (int r) {
        auto global_image_id = image_replayer->get_global_image_id();
        m_image_deleter->schedule_image_delete(
          m_local_rados, m_local_pool_id, global_image_id);
        on_finish->complete(0);
      });
  }

  stop_image_replayer(image_replayer, on_finish);
}

template <typename I>
void InstanceReplayer<I>::print_status(Formatter *f, stringstream *ss) {
  dout(20) << dendl;

  if (!f) {
    return;
  }

  Mutex::Locker locker(m_lock);

  f->open_array_section("image_replayers");
  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->print_status(f, ss);
  }
  f->close_section();
}

template <typename I>
void InstanceReplayer<I>::start()
{
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  m_manual_stop = false;

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->start(nullptr, true);
  }
}

template <typename I>
void InstanceReplayer<I>::stop()
{
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  m_manual_stop = true;

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->stop(nullptr, true);
  }
}

template <typename I>
void InstanceReplayer<I>::restart()
{
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  m_manual_stop = false;

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->restart();
  }
}

template <typename I>
void InstanceReplayer<I>::flush()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker locker(m_lock);

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;
    image_replayer->flush();
  }
}

template <typename I>
void InstanceReplayer<I>::start_image_replayer(
    ImageReplayer<I> *image_replayer) {
  assert(m_lock.is_locked());

  std::string global_image_id = image_replayer->get_global_image_id();
  dout(20) << "global_image_id=" << global_image_id << dendl;

  if (!image_replayer->is_stopped()) {
    return;
  } else if (image_replayer->is_blacklisted()) {
    derr << "blacklisted detected during image replay" << dendl;
    return;
  }

  FunctionContext *ctx = new FunctionContext(
    [this, global_image_id] (int r) {
      dout(20) << "image deleter result: r=" << r << ", "
               << "global_image_id=" << global_image_id << dendl;

      Mutex::Locker locker(m_lock);
      m_async_op_tracker.finish_op();

      if (r == -ESTALE || r == -ECANCELED) {
        return;
      }

      auto it = m_image_replayers.find(global_image_id);
      if (it == m_image_replayers.end()) {
        return;
      }

      auto image_replayer = it->second;
      if (r >= 0) {
        image_replayer->start(nullptr, false);
      } else {
        start_image_replayer(image_replayer);
      }
    });

  m_async_op_tracker.start_op();
  m_image_deleter->wait_for_scheduled_deletion(
    m_local_pool_id, image_replayer->get_global_image_id(), ctx, false);
}

template <typename I>
void InstanceReplayer<I>::start_image_replayers() {
  dout(20) << dendl;

  Context *ctx = new FunctionContext(
    [this] (int r) {
      Mutex::Locker locker(m_lock);
      m_async_op_tracker.finish_op();
      if (m_on_shut_down != nullptr) {
        return;
      }
      for (auto &it : m_image_replayers) {
        start_image_replayer(it.second);
      }
    });

  m_async_op_tracker.start_op();
  m_threads->work_queue->queue(ctx, 0);
}


template <typename I>
void InstanceReplayer<I>::stop_image_replayer(ImageReplayer<I> *image_replayer,
                                              Context *on_finish) {
  dout(20) << image_replayer << " global_image_id="
           << image_replayer->get_global_image_id() << ", on_finish="
           << on_finish << dendl;

  if (image_replayer->is_stopped()) {
    m_threads->work_queue->queue(on_finish, 0);
    return;
  }

  m_async_op_tracker.start_op();
  Context *ctx = create_async_context_callback(
    m_threads->work_queue, new FunctionContext(
      [this, image_replayer, on_finish] (int r) {
        stop_image_replayer(image_replayer, on_finish);
        m_async_op_tracker.finish_op();
      }));

  if (image_replayer->is_running()) {
    image_replayer->stop(ctx, false);
  } else {
    int after = 1;
    dout(20) << "scheduling image replayer " << image_replayer << " stop after "
             << after << " sec (task " << ctx << ")" << dendl;
    ctx = new FunctionContext(
      [this, after, ctx] (int r) {
        Mutex::Locker timer_locker(m_threads->timer_lock);
        m_threads->timer->add_event_after(after, ctx);
      });
    m_threads->work_queue->queue(ctx, 0);
  }
}

template <typename I>
void InstanceReplayer<I>::wait_for_ops() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    InstanceReplayer, &InstanceReplayer<I>::handle_wait_for_ops>(this);

  m_async_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void InstanceReplayer<I>::handle_wait_for_ops(int r) {
  dout(20) << "r=" << r << dendl;

  assert(r == 0);

  Mutex::Locker locker(m_lock);
  stop_image_replayers();
}

template <typename I>
void InstanceReplayer<I>::stop_image_replayers() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<InstanceReplayer<I>,
    &InstanceReplayer<I>::handle_stop_image_replayers>(this));

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);
  for (auto &it : m_image_replayers) {
    stop_image_replayer(it.second, gather_ctx->new_sub());
  }
  gather_ctx->activate();
}

template <typename I>
void InstanceReplayer<I>::handle_stop_image_replayers(int r) {
  dout(20) << "r=" << r << dendl;

  assert(r == 0);

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    for (auto &it : m_image_replayers) {
      assert(it.second->is_stopped());
      it.second->destroy();
    }
    m_image_replayers.clear();

    assert(m_on_shut_down != nullptr);
    std::swap(on_finish, m_on_shut_down);
  }
  on_finish->complete(r);
}

template <typename I>
void InstanceReplayer<I>::cancel_image_state_check_task() {
  Mutex::Locker timer_locker(m_threads->timer_lock);

  if (m_image_state_check_task == nullptr) {
    return;
  }

  dout(20) << m_image_state_check_task << dendl;
  bool canceled = m_threads->timer->cancel_event(m_image_state_check_task);
  assert(canceled);
  m_image_state_check_task = nullptr;
}

template <typename I>
void InstanceReplayer<I>::schedule_image_state_check_task() {
  assert(m_threads->timer_lock.is_locked());
  assert(m_image_state_check_task == nullptr);

  m_image_state_check_task = new FunctionContext(
    [this](int r) {
      assert(m_threads->timer_lock.is_locked());
      m_image_state_check_task = nullptr;
      schedule_image_state_check_task();
      start_image_replayers();
    });

  int after =
    max(1, g_ceph_context->_conf->rbd_mirror_image_state_check_interval);

  dout(20) << "scheduling image state check after " << after << " sec (task "
           << m_image_state_check_task << ")" << dendl;
  m_threads->timer->add_event_after(after, m_image_state_check_task);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::InstanceReplayer<librbd::ImageCtx>;
