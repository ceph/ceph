// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/Utils.h"
#include "librbd/cache/ParentCacheObjectDispatch.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ParentCacheObjectDispatch: " \
                           << this << " " << __func__ << ": "

using namespace ceph::immutable_obj_cache;
using librbd::util::data_object_name;

namespace librbd {
namespace cache {

template <typename I>
ParentCacheObjectDispatch<I>::ParentCacheObjectDispatch(
    I* image_ctx) : m_image_ctx(image_ctx), m_cache_client(nullptr),
    m_initialized(false), m_object_store(nullptr), m_re_connecting(false) {
  std::string controller_path =
    ((CephContext*)(m_image_ctx->cct))->_conf.get_val<std::string>("immutable_object_cache_sock");
  m_cache_client = new CacheClient(controller_path.c_str(), m_image_ctx->cct);
}

template <typename I>
ParentCacheObjectDispatch<I>::~ParentCacheObjectDispatch() {
    delete m_object_store;
    delete m_cache_client;
}

template <typename I>
void ParentCacheObjectDispatch<I>::init() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  if (m_image_ctx->parent != nullptr) {
    ldout(cct, 5) << "child image: skipping" << dendl;
    return;
  }

  C_SaferCond* cond = new C_SaferCond();
  Context* create_session_ctx = new FunctionContext([cond](int ret) {
    cond->complete(0);
  });

  create_cache_session(create_session_ctx, false);
  cond->wait();

  m_image_ctx->io_object_dispatcher->register_object_dispatch(this);
  m_initialized = true;
}

template <typename I>
bool ParentCacheObjectDispatch<I>::read(
    uint64_t object_no, uint64_t object_off,
    uint64_t object_len, librados::snap_t snap_id, int op_flags,
    const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
    io::ExtentMap* extent_map, int* object_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;
  ceph_assert(m_initialized);
  string oid = data_object_name(m_image_ctx, object_no);

  /* if RO daemon still don't startup, or RO daemon crash,
   * or session have any error, try to re-connect daemon.*/
  if (!m_cache_client->is_session_work()) {
    if(!m_re_connecting.load()) {
      ldout(cct, 20) << "try to re-connct RO daemon. " << dendl;
      m_re_connecting.store(true);

      Context* on_finish = new FunctionContext([this](int ret) {
        m_re_connecting.store(false);
      });
      create_cache_session(on_finish, true);
    }

    ldout(cct, 5) << "session don't work, dispatch current request to lower object layer " << dendl;
    return false;
  }

  ceph_assert(m_cache_client->is_session_work());

  CacheGenContextURef ctx = make_gen_lambda_context<ObjectCacheRequest*,
                                     std::function<void(ObjectCacheRequest*)>>
   ([this, snap_id, read_data, dispatch_result, on_dispatched,
      oid, object_off, object_len](ObjectCacheRequest* ack) {
     handle_read_cache(ack, object_off, object_len, read_data,
                       dispatch_result, on_dispatched);
  });

  m_cache_client->lookup_object(m_image_ctx->data_ctx.get_namespace(),
                                m_image_ctx->data_ctx.get_id(),
                                (uint64_t)snap_id, oid, std::move(ctx));
  return true;
}

template <typename I>
void ParentCacheObjectDispatch<I>::handle_read_cache(
    ObjectCacheRequest* ack, uint64_t read_off,
    uint64_t read_len, ceph::bufferlist* read_data,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  if(ack->type != RBDSC_READ_REPLY) {
    // go back to read rados
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
    return;
  }

  ceph_assert(ack->type == RBDSC_READ_REPLY);
  std::string file_path = ((ObjectCacheReadReplyData*)ack)->cache_path;
  ceph_assert(file_path != "");

  // try to read from parent image cache
  int r = m_object_store->read_object(file_path, read_data, read_off, read_len, on_dispatched);
  if(r < 0) {
    // cache read error, fall back to read rados
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
  }

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  on_dispatched->complete(r);
}

template <typename I>
int ParentCacheObjectDispatch<I>::handle_register_client(bool reg) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  if (reg) {
    ldout(cct, 20) << "Parent cache open cache handler" << dendl;
    m_object_store = new SharedPersistentObjectCacher(m_image_ctx, m_image_ctx->shared_cache_path);
  }
  return 0;
}

template <typename I>
int ParentCacheObjectDispatch<I>::create_cache_session(Context* on_finish, bool is_reconnect) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  Context* register_ctx = new FunctionContext([this, cct, on_finish](int ret) {
    if (ret < 0) {
      ldout(cct, 20) << "Parent cache fail to register client." << dendl;
      handle_register_client(false);
      on_finish->complete(-1);
      return;
    }
    ceph_assert(m_cache_client->is_session_work());

    handle_register_client(true);
    on_finish->complete(0);
  });

  Context* connect_ctx = new FunctionContext(
    [this, cct, register_ctx](int ret) {
    if (ret < 0) {
      ldout(cct, 20) << "Parent cache fail to connect RO daeomn." << dendl;
      register_ctx->complete(-1);
      return;
    }

    ldout(cct, 20) << "Parent cache connected to RO daemon." << dendl;

    m_cache_client->register_client(register_ctx);
  });

  if (m_cache_client != nullptr && is_reconnect) {
    // CacheClient's destruction will cleanup all details on old session.
    delete m_cache_client;

    // create new CacheClient to connect RO daemon.
    std::string controller_path =
      ((CephContext*)(m_image_ctx->cct))->_conf.get_val<std::string>("immutable_object_cache_sock");
    m_cache_client = new CacheClient(controller_path.c_str(), m_image_ctx->cct);
  }

  m_cache_client->run();

  m_cache_client->connect(connect_ctx);
  return 0;
}

template <typename I>
ParentCacheObjectDispatch<I>::SharedPersistentObjectCacher::SharedPersistentObjectCacher (
   I *image_ctx, std::string cache_path)
  : m_image_ctx(image_ctx) {
  auto *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;
}

template <typename I>
ParentCacheObjectDispatch<I>::SharedPersistentObjectCacher::~SharedPersistentObjectCacher() {
}

template <typename I>
int ParentCacheObjectDispatch<I>::SharedPersistentObjectCacher::read_object(
        std::string file_path, ceph::bufferlist* read_data, uint64_t offset,
        uint64_t length, Context *on_finish) {

  auto *cct = m_image_ctx->cct;
  ldout(cct, 20) << "file path: " << file_path << dendl;

  std::string error;
  int ret = read_data->pread_file(file_path.c_str(), offset, length, &error);
  if (ret < 0) {
    ldout(cct, 5) << "read from file return error: " << error
                  << "file path= " << file_path
                  << dendl;
    return ret;
  }
  return read_data->length();
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ParentCacheObjectDispatch<librbd::ImageCtx>;
