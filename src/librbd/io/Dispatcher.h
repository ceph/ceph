// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_DISPATCHER_H
#define CEPH_LIBRBD_IO_DISPATCHER_H

#include "include/int_types.h"
#include "include/Context.h"
#include "common/ceph_mutex.h"
#include "common/dout.h"
#include "common/AsyncOpTracker.h"
#include "librbd/Utils.h"
#include "librbd/io/Types.h"
#include <map>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::Dispatcher: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename ImageCtxT, typename DispatchInterfaceT>
class Dispatcher : public DispatchInterfaceT {
public:
  typedef typename DispatchInterfaceT::Dispatch Dispatch;
  typedef typename DispatchInterfaceT::DispatchLayer DispatchLayer;
  typedef typename DispatchInterfaceT::DispatchSpec DispatchSpec;

  Dispatcher(ImageCtxT* image_ctx)
    : m_image_ctx(image_ctx),
      m_lock(ceph::make_shared_mutex(
        librbd::util::unique_lock_name("librbd::io::Dispatcher::lock",
				       this))) {
  }

  virtual ~Dispatcher() {
    ceph_assert(m_dispatches.empty());
  }

  void shut_down(Context* on_finish) override {
    auto cct = m_image_ctx->cct;
    ldout(cct, 5) << dendl;

    std::map<DispatchLayer, DispatchMeta> dispatches;
    {
      std::unique_lock locker{m_lock};
      std::swap(dispatches, m_dispatches);
    }

    for (auto it : dispatches) {
      shut_down_dispatch(it.second, &on_finish);
    }
    on_finish->complete(0);
  }

  void register_dispatch(Dispatch* dispatch) override {
    auto cct = m_image_ctx->cct;
    auto type = dispatch->get_dispatch_layer();
    ldout(cct, 5) << "dispatch_layer=" << type << dendl;

    std::unique_lock locker{m_lock};

    auto result = m_dispatches.insert(
      {type, {dispatch, new AsyncOpTracker()}});
    ceph_assert(result.second);
  }

  void shut_down_dispatch(DispatchLayer dispatch_layer,
                          Context* on_finish) override {
    auto cct = m_image_ctx->cct;
    ldout(cct, 5) << "dispatch_layer=" << dispatch_layer << dendl;

    DispatchMeta dispatch_meta;
    {
      std::unique_lock locker{m_lock};
      auto it = m_dispatches.find(dispatch_layer);
      ceph_assert(it != m_dispatches.end());

      dispatch_meta = it->second;
      m_dispatches.erase(it);
    }

    shut_down_dispatch(dispatch_meta, &on_finish);
    on_finish->complete(0);
  }

  void send(DispatchSpec* dispatch_spec) {
    auto cct = m_image_ctx->cct;
    ldout(cct, 20) << "dispatch_spec=" << dispatch_spec << dendl;

    auto dispatch_layer = dispatch_spec->dispatch_layer;

    // apply the IO request to all layers -- this method will be re-invoked
    // by the dispatch layer if continuing / restarting the IO
    while (true) {
      m_lock.lock_shared();
      dispatch_layer = dispatch_spec->dispatch_layer;
      auto it = m_dispatches.upper_bound(dispatch_layer);
      if (it == m_dispatches.end()) {
        // the request is complete if handled by all layers
        dispatch_spec->dispatch_result = DISPATCH_RESULT_COMPLETE;
        m_lock.unlock_shared();
        break;
      }

      auto& dispatch_meta = it->second;
      auto dispatch = dispatch_meta.dispatch;
      auto async_op_tracker = dispatch_meta.async_op_tracker;
      dispatch_spec->dispatch_result = DISPATCH_RESULT_INVALID;

      // prevent recursive locking back into the dispatcher while handling IO
      async_op_tracker->start_op();
      m_lock.unlock_shared();

      // advance to next layer in case we skip or continue
      dispatch_spec->dispatch_layer = dispatch->get_dispatch_layer();

      bool handled = send_dispatch(dispatch, dispatch_spec);
      async_op_tracker->finish_op();

      // handled ops will resume when the dispatch ctx is invoked
      if (handled) {
        return;
      }
    }

    // skipped through to the last layer
    dispatch_spec->dispatcher_ctx.complete(0);
  }

protected:
  struct DispatchMeta {
    Dispatch* dispatch = nullptr;
    AsyncOpTracker* async_op_tracker = nullptr;

    DispatchMeta() {
    }
    DispatchMeta(Dispatch* dispatch, AsyncOpTracker* async_op_tracker)
      : dispatch(dispatch), async_op_tracker(async_op_tracker) {
    }
  };

  ImageCtxT* m_image_ctx;

  ceph::shared_mutex m_lock;
  std::map<DispatchLayer, DispatchMeta> m_dispatches;

  virtual bool send_dispatch(Dispatch* dispatch,
                             DispatchSpec* dispatch_spec) = 0;

private:
  void shut_down_dispatch(DispatchMeta& dispatch_meta,
                          Context** on_finish) {
    auto dispatch = dispatch_meta.dispatch;
    auto async_op_tracker = dispatch_meta.async_op_tracker;

    auto ctx = *on_finish;
    ctx = new LambdaContext(
      [dispatch, async_op_tracker, ctx](int r) {
        delete dispatch;
        delete async_op_tracker;

        ctx->complete(r);
      });
    ctx = new LambdaContext([dispatch, ctx](int r) {
        dispatch->shut_down(ctx);
      });
    *on_finish = new LambdaContext([async_op_tracker, ctx](int r) {
        async_op_tracker->wait_for_ops(ctx);
      });
  }

};

} // namespace io
} // namespace librbd

#undef dout_subsys
#undef dout_prefix
#define dout_prefix *_dout

#endif // CEPH_LIBRBD_IO_DISPATCHER_H
