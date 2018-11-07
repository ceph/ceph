// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/LibrbdWriteback.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/Utils.h"
#include "librbd/cache/SharedReadOnlyObjectDispatch.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::SharedReadOnlyObjectDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace cache {

template <typename I>
SharedReadOnlyObjectDispatch<I>::SharedReadOnlyObjectDispatch(
    I* image_ctx) : m_image_ctx(image_ctx) {
}

template <typename I>
SharedReadOnlyObjectDispatch<I>::~SharedReadOnlyObjectDispatch() {
    delete m_object_store;
    delete m_cache_client;
}

// TODO if connect fails, init will return error to high layer.
template <typename I>
void SharedReadOnlyObjectDispatch<I>::init() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  if (m_image_ctx->parent != nullptr) {
    ldout(cct, 5) << "child image: skipping" << dendl;
    return;
  }

  ldout(cct, 5) << "parent image: setup SRO cache client" << dendl;

  std::string controller_path = ((CephContext*)cct)->_conf.get_val<std::string>("rbd_shared_cache_sock");
  m_cache_client = new ceph::immutable_obj_cache::CacheClient(controller_path.c_str(), m_image_ctx->cct);
  m_cache_client->run();

  int ret = m_cache_client->connect();
  if (ret < 0) {
    ldout(cct, 5) << "SRO cache client fail to connect with local controller: "
                  << "please start ceph-immutable-object-cache daemon"
		  << dendl;
  } else {
    ldout(cct, 5) << "SRO cache client to register volume "
                  << "name = " << m_image_ctx->name
                  << " on ceph-immutable-object-cache daemon"
                  << dendl;

    auto ctx = new FunctionContext([this](bool reg) {
      handle_register_volume(reg);
    });
    ret = m_cache_client->register_volume(m_image_ctx->data_ctx.get_pool_name(),
                                          m_image_ctx->name, m_image_ctx->size, ctx);

    if (ret >= 0) {
      // add ourself to the IO object dispatcher chain
      m_image_ctx->io_object_dispatcher->register_object_dispatch(this);
    }
  }
}

template <typename I>
bool SharedReadOnlyObjectDispatch<I>::read(
    const std::string &oid, uint64_t object_no, uint64_t object_off,
    uint64_t object_len, librados::snap_t snap_id, int op_flags,
    const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
    io::ExtentMap* extent_map, int* object_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;

  // if any session fails, later reads will go to rados
  if(!m_cache_client->is_session_work()) {
    ldout(cct, 5) << "SRO cache client session failed " << dendl;
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
    return false;
    // TODO(): fix domain socket error
  }

  auto ctx = new FunctionContext([this, oid, object_off, object_len,
    read_data, dispatch_result, on_dispatched](bool cache) {
    handle_read_cache(cache, oid, object_off, object_len,
                      read_data, dispatch_result, on_dispatched);
  });

  if (m_cache_client && m_cache_client->is_session_work() && m_object_store) {
    m_cache_client->lookup_object(m_image_ctx->data_ctx.get_pool_name(),
      m_image_ctx->name, oid, ctx);
  }
  return true;
}

template <typename I>
int SharedReadOnlyObjectDispatch<I>::handle_read_cache(
    bool cache, const std::string &oid, uint64_t read_off,
    uint64_t read_len, ceph::bufferlist* read_data,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  // try to read from parent image
  if (cache) {
    int r = m_object_store->read_object(oid, read_data, read_off, read_len, on_dispatched);
    if (r == read_len) {
      *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
      //TODO(): complete in syncfile
      on_dispatched->complete(r);
      ldout(cct, 20) << "read cache: " << *dispatch_result <<dendl;
      return true;
    }
  }

  // fall back to read rados
  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  on_dispatched->complete(0);
  ldout(cct, 20) << "read rados: " << *dispatch_result <<dendl;
  return false;

}

template <typename I>
int SharedReadOnlyObjectDispatch<I>::handle_register_volume(bool reg) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  if (reg) {
    ldout(cct, 20) << "SRO cache client open cache handler" << dendl;
    m_object_store = new SharedPersistentObjectCacher<I>(m_image_ctx, m_image_ctx->shared_cache_path);
  }
  return 0;
}

template <typename I>
void SharedReadOnlyObjectDispatch<I>::client_handle_request(std::string msg) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

}

} // namespace cache
} // namespace librbd

template class librbd::cache::SharedReadOnlyObjectDispatch<librbd::ImageCtx>;
