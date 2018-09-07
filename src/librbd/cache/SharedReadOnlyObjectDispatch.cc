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
    //TODO(): should we cover multi-leveled clone?
    ldout(cct, 5) << "child image: skipping SRO cache client" << dendl;
    return;
  }

  ldout(cct, 5) << "parent image: setup SRO cache client = " << dendl;

  std::string controller_path = ((CephContext*)cct)->_conf.get_val<std::string>("rbd_shared_cache_sock");
  m_cache_client = new ceph::immutable_obj_cache::CacheClient(controller_path.c_str(),
    ([&](std::string s){client_handle_request(s);}), m_image_ctx->cct);

  int ret = m_cache_client->connect();
  if (ret < 0) {
    ldout(cct, 5) << "SRO cache client fail to connect with local controller: "
                  << "please start rbd-cache daemon"
		  << dendl;
  } else {
    ldout(cct, 5) << "SRO cache client to register volume on rbd-cache daemon: "
                   << "name = " << m_image_ctx->id 
                   << dendl;

    ret = m_cache_client->register_volume(m_image_ctx->data_ctx.get_pool_name(),
                                          m_image_ctx->id, m_image_ctx->size);

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
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
    return true;
    // TODO(): fix domain socket error
  }

  auto ctx = new FunctionContext([this, oid, object_off, object_len,
    read_data, dispatch_result, on_dispatched](bool cache) {
    handle_read_cache(cache, oid, object_off, object_len,
                      read_data, dispatch_result, on_dispatched);
  });

  if (m_cache_client && m_cache_client->is_session_work() && m_object_store) {
    m_cache_client->lookup_object(m_image_ctx->data_ctx.get_pool_name(),
      m_image_ctx->id, oid, ctx);
  }
  return true;
}

template <typename I>
int SharedReadOnlyObjectDispatch<I>::handle_read_cache(
    bool cache, const std::string &oid, uint64_t object_off,
    uint64_t object_len, ceph::bufferlist* read_data,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  // try to read from parent image
  if (cache) {
    int r = m_object_store->read_object(oid, read_data, object_off, object_len, on_dispatched);
    //int r = object_len;
    if (r != 0) {
      *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
      //TODO(): complete in syncfile
      on_dispatched->complete(r);
      ldout(cct, 20) << "read cache: " << *dispatch_result <<dendl;
      return true;
    }
  } else {
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
    ldout(cct, 20) << "read rados: " << *dispatch_result <<dendl;
    return false;
  }
}
template <typename I>
void SharedReadOnlyObjectDispatch<I>::client_handle_request(std::string msg) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  ceph::immutable_obj_cache::rbdsc_req_type_t *io_ctx = (ceph::immutable_obj_cache::rbdsc_req_type_t*)(msg.c_str());

  switch (io_ctx->type) {
    case ceph::immutable_obj_cache::RBDSC_REGISTER_REPLY: {
      // open cache handler for volume
      ldout(cct, 20) << "SRO cache client open cache handler" << dendl;
      m_object_store = new SharedPersistentObjectCacher<I>(m_image_ctx, m_image_ctx->shared_cache_path);

      break;
    }
    case ceph::immutable_obj_cache::RBDSC_READ_REPLY: {
      ldout(cct, 20) << "SRO cache client start to read cache" << dendl;
      //TODO(): should call read here

      break;
    }
    case ceph::immutable_obj_cache::RBDSC_READ_RADOS: {
      ldout(cct, 20) << "SRO cache client start to read rados" << dendl;
      //TODO(): should call read here

      break;
    }
    default: ldout(cct, 20) << "nothing" << io_ctx->type <<dendl;
      break;

  }
}

} // namespace cache
} // namespace librbd

template class librbd::cache::SharedReadOnlyObjectDispatch<librbd::ImageCtx>;
