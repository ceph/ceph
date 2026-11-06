// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/group/MutationLock.h"
#include "common/ceph_context.h"
#include "include/Context.h"
#include "librbd/AsioEngine.h"
#include "librbd/ManagedLock.h"
#include "librbd/Utils.h"
#include "librbd/Watcher.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include <boost/asio/steady_timer.hpp>
#include <memory>
#include <utility>

namespace librbd {
namespace group {
namespace {

class MutationLock {
  struct LockWatcher : Watcher {
    MutationLock& parent;

    LockWatcher(MutationLock& parent, librados::IoCtx& io_ctx,
                asio::ContextWQ* work_queue, const std::string& oid)
      : Watcher(io_ctx, work_queue, oid), parent(parent) {
    }

    void handle_notify(uint64_t notify_id, uint64_t handle,
                       uint64_t notifier_id, bufferlist& bl) override {
      bufferlist out;
      acknowledge_notify(notify_id, handle, out);
    }

    void handle_rewatch_complete(int r) override {
      if (r == 0 && parent.m_managed_lock->is_lock_owner()) {
        parent.m_managed_lock->reacquire_lock(new LambdaContext([](int) {}));
      }
    }
  };

public:
  MutationLock(librados::IoCtx& io_ctx, const std::string& group_id,
               std::shared_ptr<AsioEngine> engine)
    : m_ioctx(io_ctx), m_engine(std::move(engine)), m_group_id(group_id),
      m_watcher(*this, m_ioctx, m_engine->get_work_queue(),
                util::group_header_name(group_id)),
      m_retry_timer(m_engine->get_io_context()),
      m_managed_lock(ManagedLock<>::create(
        m_ioctx, *m_engine, m_watcher.get_oid(), &m_watcher,
        managed_lock::EXCLUSIVE, true,
        reinterpret_cast<CephContext*>(m_ioctx.cct())->_conf.get_val<uint64_t>(
          "rbd_blocklist_expire_seconds"))) {
  }

  void acquire(bool non_primary_only, bool wait_for_lock, Context* on_finish) {
    m_non_primary_only = non_primary_only;
    m_wait_for_lock = wait_for_lock;
    m_on_acquire = on_finish;
    m_watcher.register_watch(new LambdaContext([this](int r) {
      if (r < 0) {
        complete_acquire(r);
        return;
      }
      try_acquire();
    }));
  }

  void release(Context* on_finish) {
    m_managed_lock->shut_down(new LambdaContext([this, on_finish](int r) {
      if (m_watcher.is_unregistered()) {
        on_finish->complete(r);
        return;
      }
      m_watcher.unregister_watch(new LambdaContext([on_finish, r](int ret) {
        on_finish->complete(r < 0 ? r : ret);
      }));
    }));
  }

private:
  librados::IoCtx m_ioctx;
  std::shared_ptr<AsioEngine> m_engine;
  std::string m_group_id;
  LockWatcher m_watcher;
  boost::asio::steady_timer m_retry_timer;
  std::unique_ptr<ManagedLock<>> m_managed_lock;
  std::vector<cls::rbd::GroupSnapshot> m_snapshots;
  Context* m_on_acquire = nullptr;
  bool m_non_primary_only = false;
  bool m_wait_for_lock = false;

  void complete_acquire(int r) {
    auto on_finish = std::exchange(m_on_acquire, nullptr);
    on_finish->complete(r);
  }

  void try_acquire() {
    m_managed_lock->try_acquire_lock(new LambdaContext([this](int r) {
      if ((r == -EAGAIN || r == -EBUSY) && m_wait_for_lock) {
        // Only promotion waits. Replay must not hold up image shutdown while
        // promotion owns the lock and drains the old image watchers.
        m_retry_timer.expires_after(std::chrono::milliseconds(100));
        m_retry_timer.async_wait([this](auto ec) {
          if (ec) {
            complete_acquire(-ECANCELED);
          } else {
            try_acquire();
          }
        });
        return;
      }
      if (r < 0 || !m_non_primary_only) {
        complete_acquire(r);
        return;
      }
      auto req = ListSnapshotsRequest<>::create(
        m_ioctx, m_group_id, true, true, &m_snapshots,
        new LambdaContext([this](int ret) { handle_snapshots(ret); }));
      req->send();
    }));
  }

  void handle_snapshots(int r) {
    if (r == 0) {
      for (auto it = m_snapshots.rbegin(); it != m_snapshots.rend(); ++it) {
        auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
          &it->snapshot_namespace);
        if (ns == nullptr) {
          continue;
        }
        if (ns->is_orphan() ||
            ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
          r = -EAGAIN;
        }
        break;
      }
    }
    complete_acquire(r);
  }
};

} // anonymous namespace

void execute_mutation(librados::IoCtx& io_ctx, const std::string& group_id,
                      bool non_primary_only, bool wait_for_lock,
                      std::function<void(int, Context*)> action,
                      Context* on_finish) {
  auto engine = std::make_shared<AsioEngine>(io_ctx);
  auto lock = std::make_shared<MutationLock>(io_ctx, group_id, engine);
  lock->acquire(non_primary_only, wait_for_lock,
    new LambdaContext([lock, action, on_finish](int r) {
      action(r, new LambdaContext([lock, on_finish](int ret) {
        lock->release(new LambdaContext([lock, on_finish, ret](int release_r) {
          on_finish->complete(ret < 0 ? ret : release_r);
        }));
      }));
    }));
}

} // namespace group
} // namespace librbd
