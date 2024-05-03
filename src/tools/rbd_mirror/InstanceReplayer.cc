// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/Cond.h"
#include "common/Timer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "GroupReplayer.h"
#include "ImageReplayer.h"
#include "InstanceReplayer.h"
#include "ServiceDaemon.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::InstanceReplayer: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {

namespace {

const std::string SERVICE_DAEMON_IMAGE_ASSIGNED_COUNT_KEY("image_assigned_count");
const std::string SERVICE_DAEMON_IMAGE_WARNING_COUNT_KEY("image_warning_count");
const std::string SERVICE_DAEMON_IMAGE_ERROR_COUNT_KEY("image_error_count");

const std::string SERVICE_DAEMON_GROUP_ASSIGNED_COUNT_KEY("group_assigned_count");
const std::string SERVICE_DAEMON_GROUP_WARNING_COUNT_KEY("group_warning_count");
const std::string SERVICE_DAEMON_GROUP_ERROR_COUNT_KEY("group_error_count");

} // anonymous namespace

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;

template <typename I>
InstanceReplayer<I>::InstanceReplayer(
    librados::IoCtx &local_io_ctx, const std::string &local_mirror_uuid,
    Threads<I> *threads, ServiceDaemon<I>* service_daemon,
    MirrorStatusUpdater<I>* local_status_updater,
    journal::CacheManagerHandler *cache_manager_handler,
    PoolMetaCache* pool_meta_cache)
  : m_local_io_ctx(local_io_ctx), m_local_mirror_uuid(local_mirror_uuid),
    m_threads(threads), m_service_daemon(service_daemon),
    m_local_status_updater(local_status_updater),
    m_cache_manager_handler(cache_manager_handler),
    m_pool_meta_cache(pool_meta_cache),
    m_lock(ceph::make_mutex("rbd::mirror::InstanceReplayer " +
        stringify(local_io_ctx.get_id()))) {
}

template <typename I>
InstanceReplayer<I>::~InstanceReplayer() {
  ceph_assert(m_image_state_check_task == nullptr);
  ceph_assert(m_async_op_tracker.empty());
  ceph_assert(m_image_replayers.empty());
}

template <typename I>
bool InstanceReplayer<I>::is_blocklisted() const {
  std::lock_guard locker{m_lock};
  return m_blocklisted;
}

template <typename I>
int InstanceReplayer<I>::init() {
  C_SaferCond init_ctx;
  init(&init_ctx);
  return init_ctx.wait();
}

template <typename I>
void InstanceReplayer<I>::init(Context *on_finish) {
  dout(10) << dendl;

  Context *ctx = new LambdaContext(
    [this, on_finish] (int r) {
      {
        std::lock_guard timer_locker{m_threads->timer_lock};
        schedule_image_state_check_task();
        schedule_group_state_check_task();
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
  ceph_assert(r == 0);
}

template <typename I>
void InstanceReplayer<I>::shut_down(Context *on_finish) {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  ceph_assert(m_on_shut_down == nullptr);
  m_on_shut_down = on_finish;

  Context *ctx = new LambdaContext(
    [this] (int r) {
      cancel_group_state_check_task();
      cancel_image_state_check_task();
      wait_for_ops();
    });

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void InstanceReplayer<I>::add_peer(const Peer<I>& peer) {
  dout(10) << "peer=" << peer << dendl;

  std::lock_guard locker{m_lock};
  auto result = m_peers.insert(peer).second;
  ceph_assert(result);
}

template <typename I>
void InstanceReplayer<I>::release_all(Context *on_finish) {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, on_finish);
  dout(10) << "group_replayers: { " << m_group_replayers << " }" << dendl;
  for (auto it = m_group_replayers.begin(); it != m_group_replayers.end();
       it = m_group_replayers.erase(it)) {
    auto group_replayer = it->second;
    auto ctx = gather_ctx->new_sub();
    ctx = new LambdaContext(
      [group_replayer, ctx] (int r) {
        group_replayer->destroy();
        ctx->complete(0);
      });
    stop_group_replayer(group_replayer, ctx);
  }
  for (auto it = m_image_replayers.begin(); it != m_image_replayers.end();
       it = m_image_replayers.erase(it)) {
    auto image_replayer = it->second;
    auto ctx = gather_ctx->new_sub();
    ctx = new LambdaContext(
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
                                        Context *on_finish) {
  dout(10) << "global_image_id=" << global_image_id << dendl;

  std::lock_guard locker{m_lock};

  ceph_assert(m_on_shut_down == nullptr);

  auto it = m_image_replayers.find(global_image_id);
  if (it == m_image_replayers.end()) {
    auto image_replayer = ImageReplayer<I>::create(
        m_local_io_ctx, nullptr, m_local_mirror_uuid, global_image_id,
        m_threads, instance_watcher, m_local_status_updater,
        m_cache_manager_handler, m_pool_meta_cache);

    dout(10) << global_image_id << ": creating replayer " << image_replayer
             << dendl;

    it = m_image_replayers.insert(std::make_pair(global_image_id,
                                                 image_replayer)).first;

    // TODO only a single peer is currently supported
    ceph_assert(m_peers.size() == 1);
    auto peer = *m_peers.begin();
    image_replayer->add_peer(peer);
    start_image_replayer(image_replayer);
  } else {
    // A duplicate acquire notification implies (1) connection hiccup or
    // (2) new leader election. For the second case, restart the replayer to
    // detect if the image has been deleted while the leader was offline
    auto& image_replayer = it->second;
    image_replayer->set_finished(false);
    image_replayer->restart(new C_TrackedOp(m_async_op_tracker, nullptr));
  }

  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
void InstanceReplayer<I>::release_image(const std::string &global_image_id,
                                        Context *on_finish) {
  dout(10) << "global_image_id=" << global_image_id << dendl;

  std::lock_guard locker{m_lock};
  ceph_assert(m_on_shut_down == nullptr);

  auto it = m_image_replayers.find(global_image_id);
  if (it == m_image_replayers.end()) {
    dout(5) << global_image_id << ": not found" << dendl;
    m_threads->work_queue->queue(on_finish, 0);
    return;
  }

  auto image_replayer = it->second;
  m_image_replayers.erase(it);

  on_finish = new LambdaContext(
    [image_replayer, on_finish] (int r) {
      image_replayer->destroy();
      on_finish->complete(0);
    });
  stop_image_replayer(image_replayer, on_finish);
}

template <typename I>
void InstanceReplayer<I>::remove_peer_image(const std::string &global_image_id,
                                            const std::string &peer_mirror_uuid,
                                            Context *on_finish) {
  dout(10) << "global_image_id=" << global_image_id << ", "
           << "peer_mirror_uuid=" << peer_mirror_uuid << dendl;

  std::lock_guard locker{m_lock};
  ceph_assert(m_on_shut_down == nullptr);

  auto it = m_image_replayers.find(global_image_id);
  if (it != m_image_replayers.end()) {
    // TODO only a single peer is currently supported, therefore
    // we can just interrupt the current image replayer and
    // it will eventually detect that the peer image is missing and
    // determine if a delete propagation is required.
    auto image_replayer = it->second;
    image_replayer->restart(new C_TrackedOp(m_async_op_tracker, nullptr));
  }
  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
void InstanceReplayer<I>::acquire_group(InstanceWatcher<I> *instance_watcher,
                                        const std::string &global_group_id,
                                        Context *on_finish) {
  dout(10) << "global_group_id=" << global_group_id << dendl;

  std::lock_guard locker{m_lock};

  ceph_assert(m_on_shut_down == nullptr);

  auto it = m_group_replayers.find(global_group_id);
  if (it == m_group_replayers.end()) {
    auto group_replayer = GroupReplayer<I>::create(
        m_local_io_ctx, m_local_mirror_uuid, global_group_id,
        m_threads, instance_watcher, m_local_status_updater,
        m_cache_manager_handler, m_pool_meta_cache);

    dout(10) << global_group_id << ": creating replayer " << group_replayer
             << dendl;

    it = m_group_replayers.insert(std::make_pair(global_group_id,
                                                 group_replayer)).first;

    // TODO only a single peer is currently supported
    ceph_assert(m_peers.size() == 1);
    auto peer = *m_peers.begin();
    group_replayer->add_peer(peer);
    start_group_replayer(group_replayer);
  } else {
    // A duplicate acquire notification implies (1) connection hiccup or
    // (2) new leader election. For the second case, restart the replayer to
    // detect if the group has been deleted while the leader was offline
    auto& group_replayer = it->second;
    group_replayer->set_finished(false);
    group_replayer->restart(new C_TrackedOp(m_async_op_tracker, nullptr));
  }

  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
void InstanceReplayer<I>::release_group(const std::string &global_group_id,
                                        Context *on_finish) {
  dout(10) << "global_group_id=" << global_group_id << dendl;

  std::lock_guard locker{m_lock};
  ceph_assert(m_on_shut_down == nullptr);

  dout(10) << "group_replayers: { " << m_group_replayers << " }" << dendl;
  auto it = m_group_replayers.find(global_group_id);
  if (it == m_group_replayers.end()) {
    dout(5) << global_group_id << ": not found" << dendl;
    m_threads->work_queue->queue(on_finish, 0);
    return;
  }

  auto group_replayer = it->second;
  m_group_replayers.erase(it);

  on_finish = new LambdaContext(
    [group_replayer, on_finish] (int r) {
      group_replayer->destroy();
      on_finish->complete(0);
    });
  stop_group_replayer(group_replayer, on_finish);
}

template <typename I>
void InstanceReplayer<I>::remove_peer_group(const std::string &global_group_id,
                                            const std::string &peer_mirror_uuid,
                                            Context *on_finish) {
  dout(10) << "global_group_id=" << global_group_id << ", "
           << "peer_mirror_uuid=" << peer_mirror_uuid << dendl;

  std::lock_guard locker{m_lock};
  ceph_assert(m_on_shut_down == nullptr);

  auto it = m_group_replayers.find(global_group_id);
  if (it != m_group_replayers.end()) {
    // TODO only a single peer is currently supported, therefore
    // we can just interrupt the current group replayer and
    // it will eventually detect that the peer group is missing and
    // determine if a delete propagation is required.
    auto group_replayer = it->second;
    group_replayer->restart(new C_TrackedOp(m_async_op_tracker, nullptr));
  }
  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
void InstanceReplayer<I>::print_status(Formatter *f) {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  f->open_array_section("group_replayers");
  for (auto &[_, group_replayer] : m_group_replayers) {
    group_replayer->print_status(f);
  }
  f->close_section();
  f->open_array_section("image_replayers");
  for (auto &[_, image_replayer] : m_image_replayers) {
    image_replayer->print_status(f);
  }
  f->close_section();
}

template <typename I>
void InstanceReplayer<I>::start()
{
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  m_manual_stop = false;

  auto cct = static_cast<CephContext *>(m_local_io_ctx.cct());
  auto gather_ctx = new C_Gather(
    cct, new C_TrackedOp(m_async_op_tracker, nullptr));
  for (auto &[_, group_replayer] : m_group_replayers) {
    group_replayer->start(gather_ctx->new_sub(), true);
  }
  for (auto &[_, image_replayer] : m_image_replayers) {
    image_replayer->start(gather_ctx->new_sub(), true);
  }

  gather_ctx->activate();
}

template <typename I>
void InstanceReplayer<I>::stop()
{
  stop(nullptr);
}

template <typename I>
void InstanceReplayer<I>::stop(Context *on_finish)
{
  dout(10) << dendl;

  if (on_finish == nullptr) {
    on_finish = new C_TrackedOp(m_async_op_tracker, on_finish);
  } else {
    on_finish = new LambdaContext(
      [this, on_finish] (int r) {
        m_async_op_tracker.wait_for_ops(on_finish);
      });
  }

  auto cct = static_cast<CephContext *>(m_local_io_ctx.cct());
  auto gather_ctx = new C_Gather(cct, on_finish);
  {
    std::lock_guard locker{m_lock};

    m_manual_stop = true;

    for (auto &[_, group_replayer] : m_group_replayers) {
      group_replayer->stop(gather_ctx->new_sub(), true);
    }
    for (auto &[_, image_replayer] : m_image_replayers) {
      image_replayer->stop(gather_ctx->new_sub(), true);
    }
  }

  gather_ctx->activate();
}

template <typename I>
void InstanceReplayer<I>::restart()
{
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  m_manual_stop = false;

  for (auto &[_, group_replayer] : m_group_replayers) {
    group_replayer->restart(new C_TrackedOp(m_async_op_tracker, nullptr));
  }
  for (auto &[_, image_replayer] : m_image_replayers) {
    image_replayer->restart(new C_TrackedOp(m_async_op_tracker, nullptr));
  }
}

template <typename I>
void InstanceReplayer<I>::flush()
{
  dout(10) << dendl;

  std::lock_guard locker{m_lock};

  for (auto &[_, group_replayer] : m_group_replayers) {
    group_replayer->flush();
  }
  for (auto &[_, image_replayer] : m_image_replayers) {
    image_replayer->flush();
  }
}

template <typename I>
void InstanceReplayer<I>::start_image_replayer(
    ImageReplayer<I> *image_replayer) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  std::string global_image_id = image_replayer->get_global_image_id();
  if (!image_replayer->is_stopped()) {
    dout(10) << "image replayer is not stopped for global_image_id="
             << global_image_id << dendl;
    return;
  } else if (image_replayer->is_blocklisted()) {
    derr << "global_image_id=" << global_image_id << ": blocklisted detected "
         << "during image replay" << dendl;
    m_blocklisted = true;
    return;
  } else if (image_replayer->is_finished()) {
    // TODO temporary until policy integrated
    dout(5) << "removing image replayer for global_image_id="
            << global_image_id << dendl;
    m_image_replayers.erase(image_replayer->get_global_image_id());
    image_replayer->destroy();
    return;
  } else if (m_manual_stop) {
    dout(10) << "image replayer manually stopped for global_image_id="
             << global_image_id << dendl;
    return;
  }

  dout(10) << "global_image_id=" << global_image_id << dendl;
  image_replayer->start(new C_TrackedOp(m_async_op_tracker, nullptr), false);
}

template <typename I>
void InstanceReplayer<I>::queue_start_image_replayers() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    InstanceReplayer, &InstanceReplayer<I>::start_image_replayers>(this);
  m_async_op_tracker.start_op();
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void InstanceReplayer<I>::start_image_replayers(int r) {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};
  if (m_on_shut_down != nullptr) {
    m_async_op_tracker.finish_op();
    return;
  }

  uint64_t image_count = 0;
  uint64_t warning_count = 0;
  uint64_t error_count = 0;
  for (auto it = m_image_replayers.begin();
       it != m_image_replayers.end();) {
    auto current_it(it);
    ++it;

    ++image_count;
    auto health_state = current_it->second->get_health_state();
    if (health_state == image_replayer::HEALTH_STATE_WARNING) {
      ++warning_count;
    } else if (health_state == image_replayer::HEALTH_STATE_ERROR) {
      ++error_count;
    }

    start_image_replayer(current_it->second);
  }

  m_service_daemon->add_or_update_namespace_attribute(
    m_local_io_ctx.get_id(), m_local_io_ctx.get_namespace(),
    SERVICE_DAEMON_IMAGE_ASSIGNED_COUNT_KEY, image_count);
  m_service_daemon->add_or_update_namespace_attribute(
    m_local_io_ctx.get_id(), m_local_io_ctx.get_namespace(),
    SERVICE_DAEMON_IMAGE_WARNING_COUNT_KEY, warning_count);
  m_service_daemon->add_or_update_namespace_attribute(
    m_local_io_ctx.get_id(), m_local_io_ctx.get_namespace(),
    SERVICE_DAEMON_IMAGE_ERROR_COUNT_KEY, error_count);

  m_async_op_tracker.finish_op();
}

template <typename I>
void InstanceReplayer<I>::stop_image_replayer(ImageReplayer<I> *image_replayer,
                                              Context *on_finish) {
  dout(10) << image_replayer << " global_image_id="
           << image_replayer->get_global_image_id() << ", on_finish="
           << on_finish << dendl;

  if (image_replayer->is_stopped()) {
    m_threads->work_queue->queue(on_finish, 0);
    return;
  }

  m_async_op_tracker.start_op();
  Context *ctx = create_async_context_callback(
    m_threads->work_queue, new LambdaContext(
      [this, image_replayer, on_finish] (int r) {
        stop_image_replayer(image_replayer, on_finish);
        m_async_op_tracker.finish_op();
      }));

  if (image_replayer->is_running()) {
    image_replayer->stop(ctx, false);
  } else {
    int after = 1;
    dout(10) << "scheduling image replayer " << image_replayer << " stop after "
             << after << " sec (task " << ctx << ")" << dendl;
    ctx = new LambdaContext(
      [this, after, ctx] (int r) {
        std::lock_guard timer_locker{m_threads->timer_lock};
        m_threads->timer->add_event_after(after, ctx);
      });
    m_threads->work_queue->queue(ctx, 0);
  }
}

template <typename I>
void InstanceReplayer<I>::stop_image_replayers() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

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
  dout(10) << "r=" << r << dendl;

  ceph_assert(r == 0);

  Context *on_finish = nullptr;
  {
    std::lock_guard locker{m_lock};

    for (auto &it : m_image_replayers) {
      ceph_assert(it.second->is_stopped());
      it.second->destroy();
    }
    m_image_replayers.clear();

    std::swap(on_finish, m_on_shut_down);
  }
  if (on_finish) {
    on_finish->complete(r);
  }
}

template <typename I>
void InstanceReplayer<I>::cancel_image_state_check_task() {
  std::lock_guard timer_locker{m_threads->timer_lock};

  if (m_image_state_check_task == nullptr) {
    return;
  }

  dout(10) << m_image_state_check_task << dendl;
  bool canceled = m_threads->timer->cancel_event(m_image_state_check_task);
  ceph_assert(canceled);
  m_image_state_check_task = nullptr;
}

template <typename I>
void InstanceReplayer<I>::schedule_image_state_check_task() {
  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
  ceph_assert(m_image_state_check_task == nullptr);

  m_image_state_check_task = new LambdaContext(
    [this](int r) {
      ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
      m_image_state_check_task = nullptr;
      schedule_image_state_check_task();
      queue_start_image_replayers();
    });

  auto cct = static_cast<CephContext *>(m_local_io_ctx.cct());
  int after = cct->_conf.get_val<uint64_t>(
    "rbd_mirror_image_state_check_interval");

  dout(10) << "scheduling image state check after " << after << " sec (task "
           << m_image_state_check_task << ")" << dendl;
  m_threads->timer->add_event_after(after, m_image_state_check_task);
}

template <typename I>
void InstanceReplayer<I>::start_group_replayer(
    GroupReplayer<I> *group_replayer) {
  dout(10) << dendl;
  ceph_assert(ceph_mutex_is_locked(m_lock));

  std::string global_group_id = group_replayer->get_global_group_id();
  std::string group_name = group_replayer->get_name();
  std::string group_id;
  bool resync_requested = false;
  int r = librbd::cls_client::mirror_group_resync_get(&m_local_io_ctx,
                                                      global_group_id,
                                                      group_name,
                                                      &group_id);
  if (r < 0) {
    derr << "getting mirror group resync for global_group_id="
         << global_group_id << " failed: " << cpp_strerror(r) << dendl;
  } else if (r == 0) {
    if (group_id == group_replayer->get_local_group_id()) {
      resync_requested = true;
    }
  }
  if (!group_replayer->is_stopped()) {
    if (group_replayer->needs_restart() || resync_requested) {
      group_replayer->restart(new C_TrackedOp(m_async_op_tracker, nullptr),
                              resync_requested);
    } else {
      group_replayer->sync_group_names();
    }
    return;
  } else if (group_replayer->is_blocklisted()) {
    derr << "global_group_id=" << global_group_id << ": blocklisted detected "
         << "during group replay" << dendl;
    m_blocklisted = true;
    return;
  } else if (group_replayer->is_finished()) {
    // TODO temporary until policy integrated
    if (resync_requested) {
      resync_requested = false;
      r = librbd::cls_client::mirror_group_resync_remove(&m_local_io_ctx,
                                                         global_group_id,
                                                         group_name);
      if (r < 0) {
        derr << "removing mirror group resync for global_group_id="
             << global_group_id << " failed: " << cpp_strerror(r) << dendl;
      }
    } else {
      dout(5) << "removing group replayer for global_group_id="
              << global_group_id << dendl;
      m_group_replayers.erase(group_replayer->get_global_group_id());
      group_replayer->destroy();
      return;
    }
  } else if (m_manual_stop) {
    return;
  }
  dout(10) << "global_group_id=" << global_group_id << dendl;
  group_replayer->start(new C_TrackedOp(m_async_op_tracker, nullptr),
                        false, false, resync_requested);
}

template <typename I>
void InstanceReplayer<I>::queue_start_group_replayers() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    InstanceReplayer, &InstanceReplayer<I>::start_group_replayers>(this);
  m_async_op_tracker.start_op();
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void InstanceReplayer<I>::start_group_replayers(int r) {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};
  if (m_on_shut_down != nullptr) {
    m_async_op_tracker.finish_op();
    return;
  }

  uint64_t group_count = 0;
  uint64_t warning_count = 0;
  uint64_t error_count = 0;
  for (auto it = m_group_replayers.begin();
       it != m_group_replayers.end();) {
    auto current_it(it);
    ++it;

    ++group_count;
    auto health_state = current_it->second->get_health_state();
    if (health_state == image_replayer::HEALTH_STATE_WARNING) {
      ++warning_count;
    } else if (health_state == image_replayer::HEALTH_STATE_ERROR) {
      ++error_count;
    }

    start_group_replayer(current_it->second);
  }

  m_service_daemon->add_or_update_namespace_attribute(
    m_local_io_ctx.get_id(), m_local_io_ctx.get_namespace(),
    SERVICE_DAEMON_GROUP_ASSIGNED_COUNT_KEY, group_count);
  m_service_daemon->add_or_update_namespace_attribute(
    m_local_io_ctx.get_id(), m_local_io_ctx.get_namespace(),
    SERVICE_DAEMON_GROUP_WARNING_COUNT_KEY, warning_count);
  m_service_daemon->add_or_update_namespace_attribute(
    m_local_io_ctx.get_id(), m_local_io_ctx.get_namespace(),
    SERVICE_DAEMON_GROUP_ERROR_COUNT_KEY, error_count);

  m_async_op_tracker.finish_op();
}

template <typename I>
void InstanceReplayer<I>::stop_group_replayer(GroupReplayer<I> *group_replayer,
                                              Context *on_finish) {
  dout(10) << group_replayer << " global_group_id="
           << group_replayer->get_global_group_id() << ", on_finish="
           << on_finish << dendl;

  if (group_replayer->is_stopped()) {
    m_threads->work_queue->queue(on_finish, 0);
    return;
  }

  m_async_op_tracker.start_op();
  Context *ctx = create_async_context_callback(
    m_threads->work_queue, new LambdaContext(
      [this, group_replayer, on_finish] (int r) {
        stop_group_replayer(group_replayer, on_finish);
        m_async_op_tracker.finish_op();
      }));

  if (group_replayer->is_running()) {
    group_replayer->stop(ctx, false);
  } else {
    int after = 1;
    dout(10) << "scheduling group replayer " << group_replayer << " stop after "
             << after << " sec (task " << ctx << ")" << dendl;
    ctx = new LambdaContext(
      [this, after, ctx] (int r) {
        std::lock_guard timer_locker{m_threads->timer_lock};
        m_threads->timer->add_event_after(after, ctx);
      });
    m_threads->work_queue->queue(ctx, 0);
  }
}

template <typename I>
void InstanceReplayer<I>::stop_group_replayers() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  Context *ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<InstanceReplayer<I>,
    &InstanceReplayer<I>::handle_stop_group_replayers>(this));

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);
  for (auto &[_, group_replayer] : m_group_replayers) {
    stop_group_replayer(group_replayer, gather_ctx->new_sub());
  }
  gather_ctx->activate();
}

template <typename I>
void InstanceReplayer<I>::handle_stop_group_replayers(int r) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(r == 0);

  Context *on_finish = nullptr;
  {
    std::lock_guard locker{m_lock};

    for (auto &[_, group_replayer] : m_group_replayers) {
      ceph_assert(group_replayer->is_stopped());
      group_replayer->destroy();
    }
    m_group_replayers.clear();

    std::swap(on_finish, m_on_shut_down);
  }
  if (on_finish) {
    on_finish->complete(r);
  }
}

template <typename I>
void InstanceReplayer<I>::cancel_group_state_check_task() {
  std::lock_guard timer_locker{m_threads->timer_lock};

  if (m_group_state_check_task == nullptr) {
    return;
  }

  dout(10) << m_group_state_check_task << dendl;
  bool canceled = m_threads->timer->cancel_event(m_group_state_check_task);
  ceph_assert(canceled);
  m_group_state_check_task = nullptr;
}

template <typename I>
void InstanceReplayer<I>::schedule_group_state_check_task() {
  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
  ceph_assert(m_group_state_check_task == nullptr);

  m_group_state_check_task = new LambdaContext(
    [this](int r) {
      ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
      m_group_state_check_task = nullptr;
      schedule_group_state_check_task();
      queue_start_group_replayers();
    });

  auto cct = static_cast<CephContext *>(m_local_io_ctx.cct());
  int after = cct->_conf.get_val<uint64_t>(
    "rbd_mirror_image_state_check_interval");

  dout(10) << "scheduling group state check after " << after << " sec (task "
           << m_group_state_check_task << ")" << dendl;
  m_threads->timer->add_event_after(after, m_group_state_check_task);
}

template <typename I>
void InstanceReplayer<I>::wait_for_ops() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    InstanceReplayer, &InstanceReplayer<I>::handle_wait_for_ops>(this);

  m_async_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void InstanceReplayer<I>::handle_wait_for_ops(int r) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(r == 0);

  std::lock_guard locker{m_lock};
  stop_group_replayers();
  stop_image_replayers();
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::InstanceReplayer<librbd::ImageCtx>;
