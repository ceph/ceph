// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoObjectDispatch.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/crypto/CryptoInterface.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::CryptoObjectDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::data_object_name;

template <typename I>
struct C_EncryptedObjectReadRequest : public Context {

    I* image_ctx;
    CryptoInterface* crypto;
    uint64_t object_no;
    uint64_t object_off;
    ceph::bufferlist* read_data;
    Context* onfinish;
    io::ReadResult::C_ObjectReadRequest* req_comp;
    io::ObjectDispatchSpec* req;

    C_EncryptedObjectReadRequest(
            I* image_ctx, CryptoInterface* crypto, uint64_t object_no,
            uint64_t object_off, uint64_t object_len, librados::snap_t snap_id,
            int op_flags, const ZTracer::Trace &parent_trace,
            ceph::bufferlist* read_data, int* object_dispatch_flags,
            Context** on_finish,
            Context* on_dispatched) : image_ctx(image_ctx),
                                      crypto(crypto),
                                      object_no(object_no),
                                      object_off(object_off),
                                      read_data(read_data),
                                      onfinish(on_dispatched) {
      *on_finish = util::create_context_callback<
              Context, &Context::complete>(*on_finish, crypto);

      auto aio_comp = io::AioCompletion::create_and_start(
              (Context*)this, util::get_image_ctx(image_ctx),
              io::AIO_TYPE_READ);
      aio_comp->read_result = io::ReadResult{read_data};
      aio_comp->set_request_count(1);

      auto req_comp = new io::ReadResult::C_ObjectReadRequest(
              aio_comp, object_off, object_len, {{0, object_len}});

      req = io::ObjectDispatchSpec::create_read(
              image_ctx, io::OBJECT_DISPATCH_LAYER_CRYPTO, object_no,
              {{object_off, object_len}}, snap_id, op_flags, parent_trace,
              &req_comp->bl, &req_comp->extent_map, nullptr, req_comp);
    }

    void send() {
      req->send();
    }

    void finish(int r) override {
      ldout(image_ctx->cct, 20) << "r=" << r << dendl;
      if (r > 0) {
        crypto->decrypt(
                std::move(*read_data),
                Striper::get_file_offset(
                        image_ctx->cct, &image_ctx->layout, object_no,
                        object_off));
      }
      onfinish->complete(r);
    }
};

template <typename I>
CryptoObjectDispatch<I>::CryptoObjectDispatch(
    I* image_ctx, CryptoInterface *crypto)
  : m_image_ctx(image_ctx), m_crypto(crypto) {
}

template <typename I>
void CryptoObjectDispatch<I>::init(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  // need to initialize m_crypto here using image header object

  m_image_ctx->io_object_dispatcher->register_dispatch(this);

  on_finish->complete(0);
}

template <typename I>
void CryptoObjectDispatch<I>::shut_down(Context* on_finish) {
  if (m_crypto != nullptr) {
    m_crypto->put();
    m_crypto = nullptr;
  }
  on_finish->complete(0);
}

template <typename I>
bool CryptoObjectDispatch<I>::read(
    uint64_t object_no, const io::Extents &extents,
    librados::snap_t snap_id, int op_flags, const ZTracer::Trace &parent_trace,
    ceph::bufferlist* read_data, io::Extents* extent_map, uint64_t* version,
    int* object_dispatch_flags, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << extents << dendl;
  ceph_assert(m_crypto != nullptr);

  if (version != nullptr || extents.size() != 1) {
    // there's currently no need to support multiple extents
    // as well as returning object version
    return false;
  }

  auto [object_off, object_len] = extents.front();

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  auto req = new C_EncryptedObjectReadRequest<I>(
          m_image_ctx, m_crypto, object_no, object_off, object_len, snap_id,
          op_flags, parent_trace, read_data, object_dispatch_flags, on_finish,
          on_dispatched);
  req->send();
  return true;
}

template <typename I>
bool CryptoObjectDispatch<I>::write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << data.length() << dendl;
  ceph_assert(m_crypto != nullptr);

  m_crypto->encrypt(
          std::move(data),
          Striper::get_file_offset(
                  m_image_ctx->cct, &m_image_ctx->layout, object_no,
                  object_off));
  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  on_dispatched->complete(0);
  return true;
}

template <typename I>
bool CryptoObjectDispatch<I>::write_same(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    io::LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;
  ceph_assert(m_crypto != nullptr);

  // convert to regular write
  io::LightweightObjectExtent extent(object_no, object_off, object_len, 0);
  extent.buffer_extents = std::move(buffer_extents);

  bufferlist ws_data;
  io::util::assemble_write_same_extent(extent, data, &ws_data, true);

  auto ctx = new LambdaContext(
      [on_finish_ctx=on_dispatched](int r) {
          on_finish_ctx->complete(r);
      });

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  auto req = io::ObjectDispatchSpec::create_write(
          m_image_ctx, io::OBJECT_DISPATCH_LAYER_NONE, object_no,
          object_off, std::move(ws_data), snapc, op_flags, 0,
          parent_trace, ctx);
  req->send();
  return true;
}

template <typename I>
bool CryptoObjectDispatch<I>::compare_and_write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
    ceph::bufferlist&& write_data, const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
    int* object_dispatch_flags, uint64_t* journal_tid,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << write_data.length()
                 << dendl;
  ceph_assert(m_crypto != nullptr);

  uint64_t image_offset = Striper::get_file_offset(
          m_image_ctx->cct, &m_image_ctx->layout, object_no, object_off);
  m_crypto->encrypt(std::move(cmp_data), image_offset);
  m_crypto->encrypt(std::move(write_data), image_offset);
  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  on_dispatched->complete(0);
  return true;
}

template <typename I>
bool CryptoObjectDispatch<I>::discard(
        uint64_t object_no, uint64_t object_off, uint64_t object_len,
        const ::SnapContext &snapc, int discard_flags,
        const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
        uint64_t* journal_tid, io::DispatchResult* dispatch_result,
        Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << object_len << dendl;
  ceph_assert(m_crypto != nullptr);

  // convert to write-same
  auto ctx = new LambdaContext(
      [on_finish_ctx=on_dispatched](int r) {
          on_finish_ctx->complete(r);
      });

  bufferlist bl;
  const int buffer_size = 4096;
  bl.append_zero(buffer_size);

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  auto req = io::ObjectDispatchSpec::create_write_same(
          m_image_ctx, io::OBJECT_DISPATCH_LAYER_NONE, object_no, object_off,
          object_len, {{0, buffer_size}}, std::move(bl), snapc,
          *object_dispatch_flags, 0, parent_trace, ctx);
  req->send();
  return true;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::CryptoObjectDispatch<librbd::ImageCtx>;
