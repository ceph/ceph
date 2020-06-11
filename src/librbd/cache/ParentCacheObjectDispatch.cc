// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"
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
    m_initialized(false), m_connecting(false) {
  ceph_assert(m_image_ctx->data_ctx.is_valid());
  std::string controller_path =
    ((CephContext*)(m_image_ctx->cct))->_conf.get_val<std::string>("immutable_object_cache_sock");
  m_cache_client = new CacheClient(controller_path.c_str(), m_image_ctx->cct);
}

template <typename I>
ParentCacheObjectDispatch<I>::~ParentCacheObjectDispatch() {
    delete m_cache_client;
}

template <typename I>
void ParentCacheObjectDispatch<I>::init(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  if (m_image_ctx->child == nullptr) {
    ldout(cct, 5) << "non-parent image: skipping" << dendl;
    if (on_finish != nullptr) {
      on_finish->complete(-EINVAL);
    }
    return;
  }

  Context* create_session_ctx = new LambdaContext([this, on_finish](int ret) {
    m_connecting.store(false);
    if (on_finish != nullptr) {
      on_finish->complete(ret);
    }
  });

  m_connecting.store(true);
  create_cache_session(create_session_ctx, false);

  m_image_ctx->io_object_dispatcher->register_dispatch(this);
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
   * or session occur any error, try to re-connect daemon.*/
  if (!m_cache_client->is_session_work()) {
    if (!m_connecting.exchange(true)) {
      /* Since we don't have a giant lock protecting the full re-connect process,
       * if thread A first passes the if (!m_cache_client->is_session_work()),
       * thread B could have also passed it and reconnected
       * before thread A resumes and executes if (!m_connecting.exchange(true)).
       * This will result in thread A re-connecting a working session.
       * So, we need to check if session is normal again. If session work,
       * we need set m_connecting to false. */
      if (!m_cache_client->is_session_work()) {
        Context* on_finish = new LambdaContext([this](int ret) {
          m_connecting.store(false);
        });
        create_cache_session(on_finish, true);
      } else {
        m_connecting.store(false);
      }
    }
    ldout(cct, 5) << "Parent cache try to re-connect to RO daemon. "
                  << "dispatch current request to lower object layer" << dendl;
    return false;
  }

  ceph_assert(m_cache_client->is_session_work());

  CacheGenContextURef ctx = make_gen_lambda_context<ObjectCacheRequest*,
                                     std::function<void(ObjectCacheRequest*)>>
   ([this, read_data, dispatch_result, on_dispatched,
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
     ObjectCacheRequest* ack, uint64_t read_off, uint64_t read_len,
     ceph::bufferlist* read_data, io::DispatchResult* dispatch_result,
     Context* on_dispatched) {
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
  int r = read_object(file_path, read_data, read_off, read_len, on_dispatched);
  if(r < 0) {
    // cache read error, fall back to read rados
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
    return;
  }

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  on_dispatched->complete(r);
}

template <typename I>
int ParentCacheObjectDispatch<I>::handle_register_client(bool reg) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  if (!reg) {
    lderr(cct) << "Parent cache register fails." << dendl;
  }
  return 0;
}

template <typename I>
int ParentCacheObjectDispatch<I>::create_cache_session(Context* on_finish, bool is_reconnect) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  Context* register_ctx = new LambdaContext([this, cct, on_finish](int ret) {
    if (ret < 0) {
      lderr(cct) << "Parent cache fail to register client." << dendl;
    } else {
      ceph_assert(m_cache_client->is_session_work());
    }
    handle_register_client(ret < 0 ? false : true);
    on_finish->complete(ret);
  });

  Context* connect_ctx = new LambdaContext(
    [this, cct, register_ctx](int ret) {
    if (ret < 0) {
      lderr(cct) << "Parent cache fail to connect RO daeomn." << dendl;
      register_ctx->complete(ret);
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
int ParentCacheObjectDispatch<I>::read_object(
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
