// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_DISPATCHER_H
#define CEPH_LIBRBD_IO_DISPATCHER_H

#include "include/int_types.h"
#include "include/Context.h"
#include "common/ceph_mutex.h"
#include "common/dout.h"
#include "common/AsyncOpTracker.h"
#include "common/Cond.h"
#include "librbd/Utils.h"
#include "librbd/io/DispatcherInterface.h"
#include "librbd/io/Types.h"
#include <map>
#include <ostream>

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
      m_op_tracker(new AsyncOpTracker()) {
  }

  virtual ~Dispatcher() {
    ceph_assert(m_dispatches.empty());
    delete m_op_tracker;
  }

  void shut_down_whilst_quiesced(Context* on_finish) {
    std::map<DispatchLayer, DispatchMeta> dispatches;
    {
      std::swap(dispatches, m_dispatches);
    }

    // add the callbacks for shutting down the dispatch layers to the on_finish callback
    for (auto it : dispatches) {
      shut_down_dispatch(it.second, &on_finish);
    }
  }

  // shutdown all layers in this dispatcher (image or object)
  void shut_down(Context* on_finish) override {
    auto cct = m_image_ctx->cct;
    ldout(cct, 5) << dendl;

    // The dispatch shutdown method will be called from the AsyncOpTracker when quiesced with the lock held  
    auto locked_ctx = new LambdaContext([this, on_finish]() {
      this->shut_down_whilst_quiesced(on_finish);
    });

    m_op_tracker->wait_for_ops(on_finish, locked_ctx);
  }

  void register_dispatch(Dispatch* dispatch) override {
    auto cct = m_image_ctx->cct;
    auto type = dispatch->get_dispatch_layer();
    ldout(cct, 5) << "dispatch_layer=" << type << dendl;

    C_SaferCond on_finish;

    // This will be called from the AsyncOpTracker when quiesced with the lock held  
    auto on_quiesced_locked_ctx = new LambdaContext([this, dispatch]() {
      auto type = dispatch->get_dispatch_layer();
      auto result = m_dispatches.insert({type, {dispatch}});
      ceph_assert(result.second);
    });

    // Wait for IO to quiesce then add layer
    m_op_tracker->wait_for_ops(&on_finish, on_quiesced_locked_ctx);

    // Resume thread once layer has been added
    on_finish.wait();
  }

  bool exists(DispatchLayer dispatch_layer) override {
    // register with the tracker to prevent m_dispatches from being changed under our feet.
    m_op_tracker->start_op();
    bool exists = m_dispatches.find(dispatch_layer) != m_dispatches.end();
    m_op_tracker->finish_op();
    return exists;
  }

  void shut_down_dispatch_whilst_quiesced(DispatchLayer dispatch_layer,
                                          Context* on_finish) {
    DispatchMeta dispatch_meta;
    auto it = m_dispatches.find(dispatch_layer);
    if (it == m_dispatches.end()) {
      return;
    }

    dispatch_meta = it->second;
    m_dispatches.erase(it);

    // add the callbacks for shutting down the dispatch layer to the on_finish callback
    shut_down_dispatch(dispatch_meta, &on_finish);
  }                            

  void shut_down_dispatch(DispatchLayer dispatch_layer,
                          Context* on_finish) override {
    auto cct = m_image_ctx->cct;
    ldout(cct, 5) << "dispatch_layer=" << dispatch_layer << dendl;

    // The dispatch shutdown method will be called from the AsyncOpTracker when quiesced with the lock held  
    auto locked_ctx = new LambdaContext([this, dispatch_layer, on_finish]() {
      this->shut_down_dispatch_whilst_quiesced(dispatch_layer, on_finish);
    });

    m_op_tracker->wait_for_ops(on_finish, locked_ctx);
  }

  void finished(DispatchSpec* dispatch_spec) {
    m_op_tracker->finish_op();
  }

  void send(DispatchSpec* dispatch_spec) {
    auto cct = m_image_ctx->cct;
    ldout(cct, 20) << "dispatch_spec=" << dispatch_spec << dendl;

    auto dispatch_layer = dispatch_spec->dispatch_layer;

    // If this is the first time into the send function then record this in the overall op_tracker
    if (dispatch_spec->dispatch_result == DISPATCH_RESULT_INIT) {
      m_op_tracker->start_op();
    }
    if (dispatch_spec->dispatch_result == DISPATCH_RESULT_REFRESH) {
      ldout(cct, 20) << "rstart. dispatch_spec=" << dispatch_spec << dendl;
      m_op_tracker->start_op();
    }

    // apply the IO request to all layers -- this method will be re-invoked
    // by the dispatch layer if continuing / restarting the IO
    while (true) {
      dispatch_layer = dispatch_spec->dispatch_layer;
      auto it = m_dispatches.upper_bound(dispatch_layer);
      if (it == m_dispatches.end()) {
        // the request is complete if handled by all layers
        dispatch_spec->dispatch_result = DISPATCH_RESULT_COMPLETE;
        break;
      }

      auto& dispatch_meta = it->second;
      auto dispatch = dispatch_meta.dispatch;
      dispatch_spec->dispatch_result = DISPATCH_RESULT_INVALID;

      // advance to next layer in case we skip or continue
      dispatch_spec->dispatch_layer = dispatch->get_dispatch_layer();

      bool handled = send_dispatch(dispatch, dispatch_spec);
      // This will end up in ImageDispatchSpec::C_Dispatcher::complete() where it will look at the dispatch_spec result.
      // If it is CONTINUE then it will call back into this function (for the next layer)
      // If it is COMPLETE then it will call finish - in ImageDispatchSpec this will just delete the dispatchSpec

      // Alternatively this will end up in ObjectDispatchSpec::C_Dispatcher::complete() where it will look at the dispatch_spec result.
      // If it is CONTINUE then it will call back into this function (for the next layer)
      // If it is COMPLETE then it will call finish - this will call the complete() method (and then delete the object dispatchSpec)
      //                                              the complete() method will call AioCompletion::complete which in turn will call AioCompletion::notify_callbacks_complete()
      //                                              this will call ImageDispatchSpec::C_Dispatcher::complete() which will delete the image dispatchSpec

      // handled ops will resume when the dispatch ctx is invoked
      if (handled) {

        // If this IO request was queued waiting for a refresh to complete then decrement the count of outstanding operations.
        // Once the refresh has completed the request will be resubmitted and tracked again.
        // This is required because if the refresh needs to add or remove dispatch layers then it can only do so when there are no 
        // IOs in flight.  IOs that are queued waiting for the refresh to complete are not in flight.
        if(dispatch_spec->dispatch_result == DISPATCH_RESULT_REFRESH) {
          ldout(cct, 20) << "rfinish. dispatch_spec=" << dispatch_spec << dendl;
          m_op_tracker->finish_op();
        }

        return;  
        // layer is processing/already processed this request.  It will either complete the request or call back into this send method for the next layer
        // If it completes the request it will call ImageDispatchSpec::C_Dispatcher::complete()
      } 
      // otherwise this layer processed the request synchronously (or had nothing to do) - go round again for the next layer
    }

    // skipped through to the last layer - call the C_Dispatcher.complete() function ourselves
    dispatch_spec->dispatcher_ctx.complete(0);
  }

protected:
  struct DispatchMeta {
    Dispatch* dispatch = nullptr;

    DispatchMeta() {
    }
    DispatchMeta(Dispatch* dispatch)
      : dispatch(dispatch) {
    }
  };

  ImageCtxT* m_image_ctx;
  AsyncOpTracker* m_op_tracker = nullptr;

  std::map<DispatchLayer, DispatchMeta> m_dispatches;

  virtual bool send_dispatch(Dispatch* dispatch,
                             DispatchSpec* dispatch_spec) = 0;

protected:
  struct C_LayerIterator : public Context {
    Dispatcher* dispatcher;
    Context* on_finish;
    DispatchLayer dispatch_layer;
    bool counted=false;

    C_LayerIterator(Dispatcher* dispatcher,
                    DispatchLayer start_layer,
                    Context* on_finish)
    : dispatcher(dispatcher), on_finish(on_finish), dispatch_layer(start_layer) {
    }

    // Called from Image/ObjectDispatcher::invalidate_cache and will also be called back asynchronously after a dispatcher has completed its work (invalidate cache)
    void complete(int r) override {

      // If this is the first time into the complete function then record this in the op_tracker
      if (!counted) {
        dispatcher->m_op_tracker->start_op();
        counted = true;
      }

      while (true) {
        auto it = dispatcher->m_dispatches.upper_bound(dispatch_layer);
        if (it == dispatcher->m_dispatches.end()) {
          dispatcher->m_op_tracker->finish_op();
          Context::complete(r); // this calls the finish() method below
          return;
        }

        auto& dispatch_meta = it->second;
        auto dispatch = dispatch_meta.dispatch;

        // next loop should start after current layer
        dispatch_layer = dispatch->get_dispatch_layer();

        auto handled = execute(dispatch, this);
        if (handled) {
          break;
        }
      }
    }

    void finish(int r) override {
      on_finish->complete(0);
    }
    virtual bool execute(Dispatch* dispatch,
                         Context* on_finish) = 0;
  };

  struct C_InvalidateCache : public C_LayerIterator {
    C_InvalidateCache(Dispatcher* dispatcher, DispatchLayer start_layer, Context* on_finish)
      : C_LayerIterator(dispatcher, start_layer, on_finish) {
    }

    bool execute(Dispatch* dispatch,
                 Context* on_finish) override {
      return dispatch->invalidate_cache(on_finish);
    }
  };

private:
  void shut_down_dispatch(DispatchMeta& dispatch_meta,
                          Context** on_finish) {
    auto dispatch = dispatch_meta.dispatch;

    auto ctx = *on_finish;
    ctx = new LambdaContext([dispatch, ctx](int r) {
        delete dispatch;
        ctx->complete(r);
      });
    *on_finish = new LambdaContext([dispatch, ctx](int r) {
        dispatch->shut_down(ctx);
      });
  }

};

} // namespace io
} // namespace librbd

#undef dout_subsys
#undef dout_prefix
#define dout_prefix *_dout

#endif // CEPH_LIBRBD_IO_DISPATCHER_H
