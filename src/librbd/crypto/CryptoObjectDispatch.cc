// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoObjectDispatch.h"
#include "include/ceph_assert.h"
#include "include/neorados/RADOS.hpp"
#include "common/dout.h"
#include "osdc/Striper.h"
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

using librbd::util::create_context_callback;
using librbd::util::data_object_name;

template <typename I>
uint64_t get_file_offset(I* image_ctx, uint64_t object_no,
                         uint64_t object_off) {
  auto off = io::util::raw_to_area_offset(
      *image_ctx, Striper::get_file_offset(image_ctx->cct, &image_ctx->layout,
                                           object_no, object_off));
  ceph_assert(off.second == io::ImageArea::DATA);
  return off.first;
}

template <typename I>
struct C_AlignedObjectReadRequest : public Context {
    I* image_ctx;
    CryptoInterface* crypto;
    uint64_t object_no;
    io::ReadExtents* extents;
    IOContext io_context;
    const ZTracer::Trace parent_trace;
    uint64_t* version;
    Context* on_finish;
    io::ObjectDispatchSpec* req;
    bool disable_read_from_parent;

    C_AlignedObjectReadRequest(
            I* image_ctx, CryptoInterface* crypto,
            uint64_t object_no, io::ReadExtents* extents, IOContext io_context,
            int op_flags, int read_flags, const ZTracer::Trace &parent_trace,
            uint64_t* version, int* object_dispatch_flags,
            Context* on_dispatched
            ) : image_ctx(image_ctx), crypto(crypto), object_no(object_no),
                extents(extents), io_context(io_context),
                parent_trace(parent_trace), version(version),
                on_finish(on_dispatched) {
      disable_read_from_parent =
              ((read_flags & io::READ_FLAG_DISABLE_READ_FROM_PARENT) != 0);
      read_flags |= io::READ_FLAG_DISABLE_READ_FROM_PARENT;

      auto ctx = create_context_callback<
              C_AlignedObjectReadRequest<I>,
              &C_AlignedObjectReadRequest<I>::handle_read>(this);

      req = io::ObjectDispatchSpec::create_read(
              image_ctx, io::OBJECT_DISPATCH_LAYER_CRYPTO, object_no,
              extents, io_context, op_flags, read_flags, parent_trace,
              version, ctx);
    }

    void send() {
      req->send();
    }

    void finish(int r) override {
      ldout(image_ctx->cct, 20) << "aligned read r=" << r << dendl;
      on_finish->complete(r);
    }

    void handle_read(int r) {
      auto cct = image_ctx->cct;
      ldout(cct, 20) << "aligned read r=" << r << dendl;
      if (r >= 0) {
        r = 0;
        for (auto& extent: *extents) {
          auto crypto_ret = crypto->decrypt_aligned_extent(
              extent, get_file_offset(image_ctx, object_no, extent.offset));
          if (crypto_ret != 0) {
            ceph_assert(crypto_ret < 0);
            r = crypto_ret;
            break;
          }
          r += extent.length;
        }
      }

      if (r == -ENOENT && !disable_read_from_parent) {
        io::util::read_parent<I>(
                image_ctx, object_no, extents,
                io_context->get_read_snap(),
                parent_trace, this);
      } else {
        complete(r);
      }
    }
};

template <typename I>
struct C_UnalignedObjectReadRequest : public Context {
    CephContext* cct;
    io::ReadExtents* extents;
    Context* on_finish;
    io::ReadExtents aligned_extents;
    io::ObjectDispatchSpec* req;

    C_UnalignedObjectReadRequest(
            I* image_ctx, CryptoInterface* crypto,
            uint64_t object_no, io::ReadExtents* extents, IOContext io_context,
            int op_flags, int read_flags, const ZTracer::Trace &parent_trace,
            uint64_t* version, int* object_dispatch_flags,
            Context* on_dispatched) : cct(image_ctx->cct), extents(extents),
                                      on_finish(on_dispatched) {
      crypto->align_extents(*extents, &aligned_extents);

      // send the aligned read back to get decrypted
      req = io::ObjectDispatchSpec::create_read(
              image_ctx,
              io::util::get_previous_layer(io::OBJECT_DISPATCH_LAYER_CRYPTO),
              object_no, &aligned_extents, io_context, op_flags, read_flags,
              parent_trace, version, this);
    }

    void send() {
      req->send();
    }

    void remove_alignment_data() {
      for (uint64_t i = 0; i < extents->size(); ++i) {
        auto& extent = (*extents)[i];
        auto& aligned_extent = aligned_extents[i];
        if (aligned_extent.extent_map.empty()) {
          uint64_t cut_offset = extent.offset - aligned_extent.offset;
          int64_t padding_count =
                  cut_offset + extent.length - aligned_extent.bl.length();
          if (padding_count > 0) {
            aligned_extent.bl.append_zero(padding_count);
          }
          aligned_extent.bl.splice(cut_offset, extent.length, &extent.bl);
        } else {
          for (auto [off, len]: aligned_extent.extent_map) {
            ceph::bufferlist tmp;
            aligned_extent.bl.splice(0, len, &tmp);

            uint64_t bytes_to_skip = 0;
            if (off < extent.offset) {
              bytes_to_skip = extent.offset - off;
              if (len <= bytes_to_skip) {
                continue;
              }
              off += bytes_to_skip;
              len -= bytes_to_skip;
            }

            len = std::min(len, extent.offset + extent.length - off);
            if (len == 0) {
              continue;
            }

            if (len > 0) {
              tmp.splice(bytes_to_skip, len, &extent.bl);
              extent.extent_map.emplace_back(off, len);
            }
          }
        }
      }
    }

    void finish(int r) override {
      ldout(cct, 20) << "unaligned read r=" << r << dendl;
      if (r >= 0) {
        remove_alignment_data();

        r = 0;
        for (auto& extent: *extents) {
          r += extent.length;
        }
      }
      on_finish->complete(r);
    }
};

template <typename I>
struct C_UnalignedObjectWriteRequest : public Context {
    I* image_ctx;
    CryptoInterface* crypto;
    uint64_t object_no;
    uint64_t object_off;
    ceph::bufferlist data;
    ceph::bufferlist cmp_data;
    uint64_t* mismatch_offset;
    IOContext io_context;
    int op_flags;
    int write_flags;
    std::optional<uint64_t> assert_version;
    const ZTracer::Trace parent_trace;
    int* object_dispatch_flags;
    uint64_t* journal_tid;
    Context* on_finish;
    bool may_copyup;
    ceph::bufferlist aligned_data;
    io::ReadExtents extents;
    uint64_t version;
    C_UnalignedObjectReadRequest<I>* read_req;
    bool object_exists;

    C_UnalignedObjectWriteRequest(
            I* image_ctx, CryptoInterface* crypto,
            uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
            ceph::bufferlist&& cmp_data, uint64_t* mismatch_offset,
            IOContext io_context, int op_flags, int write_flags,
            std::optional<uint64_t> assert_version,
            const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
            uint64_t* journal_tid, Context* on_dispatched, bool may_copyup
            ) : image_ctx(image_ctx), crypto(crypto), object_no(object_no),
                object_off(object_off), data(data), cmp_data(cmp_data),
                mismatch_offset(mismatch_offset), io_context(io_context),
                op_flags(op_flags), write_flags(write_flags),
                assert_version(assert_version), parent_trace(parent_trace),
                object_dispatch_flags(object_dispatch_flags),
                journal_tid(journal_tid), on_finish(on_dispatched),
                may_copyup(may_copyup) {
      // build read extents
      auto [pre_align, post_align] = crypto->get_pre_and_post_align(
              object_off, data.length());
      if (pre_align != 0) {
        extents.emplace_back(object_off - pre_align, pre_align);
      }
      if (post_align != 0) {
        extents.emplace_back(object_off + data.length(), post_align);
      }
      if (cmp_data.length() != 0) {
        extents.emplace_back(object_off, cmp_data.length());
      }

      auto ctx = create_context_callback<
              C_UnalignedObjectWriteRequest<I>,
              &C_UnalignedObjectWriteRequest<I>::handle_read>(this);

      read_req = new C_UnalignedObjectReadRequest<I>(
              image_ctx, crypto, object_no, &extents, io_context,
              0, io::READ_FLAG_DISABLE_READ_FROM_PARENT, parent_trace,
              &version, 0, ctx);
    }

    void send() {
      read_req->send();
    }

    bool check_cmp_data() {
      if (cmp_data.length() == 0) {
        return true;
      }

      auto& cmp_extent = extents.back();
      io::util::unsparsify(image_ctx->cct, &cmp_extent.bl,
                           cmp_extent.extent_map, cmp_extent.offset,
                           cmp_extent.length);

      std::optional<uint64_t> found_mismatch = std::nullopt;

      auto it1 = cmp_data.cbegin();
      auto it2 = cmp_extent.bl.cbegin();
      for (uint64_t idx = 0; idx < cmp_data.length(); ++idx) {
        if (*it1 != *it2) {
          found_mismatch = std::make_optional(idx);
          break;
        }
        ++it1;
        ++it2;
      }

      extents.pop_back();

      if (found_mismatch.has_value()) {
        if (mismatch_offset != nullptr) {
          *mismatch_offset = found_mismatch.value();
        }
        complete(-EILSEQ);
        return false;
      }

      return true;
    }

    bool check_create_exclusive() {
      bool exclusive =
              ((write_flags & io::OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE) != 0);
      if (exclusive && object_exists) {
        complete(-EEXIST);
        return false;
      }
      return true;
    }

    bool check_version() {
      int r = 0;
      if (assert_version.has_value()) {
        if (!object_exists) {
          r = -ENOENT;
        } else if (assert_version.value() < version) {
          r = -ERANGE;
        } else if (assert_version.value() > version) {
          r = -EOVERFLOW;
        }
      }

      if (r != 0) {
        complete(r);
        return false;
      }
      return true;
    }

    void build_aligned_data() {
      auto [pre_align, post_align] = crypto->get_pre_and_post_align(
              object_off, data.length());
      if (pre_align != 0) {
        auto &extent = extents.front();
        io::util::unsparsify(image_ctx->cct, &extent.bl, extent.extent_map,
                             extent.offset, extent.length);
        extent.bl.splice(0, pre_align, &aligned_data);
      }
      aligned_data.append(data);
      if (post_align != 0) {
        auto &extent = extents.back();
        io::util::unsparsify(image_ctx->cct, &extent.bl, extent.extent_map,
                             extent.offset, extent.length);
        extent.bl.splice(0, post_align, &aligned_data);
      }
    }

    void handle_copyup(int r) {
      ldout(image_ctx->cct, 20) << "r=" << r << dendl;
      if (r < 0) {
        complete(r);
      } else {
        restart_request(false);
      }
    }

    void handle_read(int r) {
      ldout(image_ctx->cct, 20) << "unaligned write r=" << r << dendl;

      if (r == -ENOENT) {
        if (may_copyup) {
          auto ctx = create_context_callback<
                  C_UnalignedObjectWriteRequest<I>,
                  &C_UnalignedObjectWriteRequest<I>::handle_copyup>(this);
          if (io::util::trigger_copyup(
                  image_ctx, object_no, io_context, ctx)) {
            return;
          }
          delete ctx;
        }
        object_exists = false;
      } else if (r < 0) {
        complete(r);
        return;
      } else {
        object_exists = true;
      }

      if (!check_create_exclusive() || !check_version() || !check_cmp_data()) {
        return;
      }

      build_aligned_data();

      auto aligned_off = crypto->align(object_off, data.length()).first;
      auto new_write_flags = write_flags;
      auto new_assert_version = std::make_optional(version);
      if (!object_exists) {
        new_write_flags |=  io::OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE;
        new_assert_version = std::nullopt;
      }

      auto ctx = create_context_callback<
              C_UnalignedObjectWriteRequest<I>,
              &C_UnalignedObjectWriteRequest<I>::handle_write>(this);

      // send back aligned write back to get encrypted and committed
      auto write_req = io::ObjectDispatchSpec::create_write(
              image_ctx,
              io::util::get_previous_layer(io::OBJECT_DISPATCH_LAYER_CRYPTO),
              object_no, aligned_off, std::move(aligned_data), io_context,
              op_flags, new_write_flags, new_assert_version,
              journal_tid == nullptr ? 0 : *journal_tid, parent_trace, ctx);
      write_req->send();
    }

    void restart_request(bool may_copyup) {
      auto req = new C_UnalignedObjectWriteRequest<I>(
              image_ctx, crypto, object_no, object_off,
              std::move(data), std::move(cmp_data),
              mismatch_offset, io_context, op_flags, write_flags,
              assert_version, parent_trace,
              object_dispatch_flags, journal_tid, this, may_copyup);
      req->send();
    }

    void handle_write(int r) {
      ldout(image_ctx->cct, 20) << "r=" << r << dendl;
      bool exclusive = write_flags & io::OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE;
      bool restart = false;
      if (r == -ERANGE && !assert_version.has_value()) {
        restart = true;
      } else if (r == -EEXIST && !exclusive) {
        restart = true;
      }

      if (restart) {
        restart_request(may_copyup);
      } else {
        complete(r);
      }
    }

    void finish(int r) override {
      ldout(image_ctx->cct, 20) << "unaligned write r=" << r << dendl;
      on_finish->complete(r);
    }
};

template <typename I>
CryptoObjectDispatch<I>::CryptoObjectDispatch(
    I* image_ctx, CryptoInterface* crypto)
  : m_image_ctx(image_ctx), m_crypto(crypto) {
  m_data_offset_object_no = Striper::get_num_objects(image_ctx->layout,
                                                     crypto->get_data_offset());
}

template <typename I>
void CryptoObjectDispatch<I>::shut_down(Context* on_finish) {
  on_finish->complete(0);
}

template <typename I>
bool CryptoObjectDispatch<I>::read(
    uint64_t object_no, io::ReadExtents* extents, IOContext io_context,
    int op_flags, int read_flags, const ZTracer::Trace &parent_trace,
    uint64_t* version, int* object_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  if (object_no < m_data_offset_object_no) {
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << *extents << dendl;
  ceph_assert(m_crypto != nullptr);

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  if (m_crypto->is_aligned(*extents)) {
    auto req = new C_AlignedObjectReadRequest<I>(
            m_image_ctx, m_crypto, object_no, extents, io_context,
            op_flags, read_flags, parent_trace, version, object_dispatch_flags,
            on_dispatched);
    req->send();
  } else {
    auto req = new C_UnalignedObjectReadRequest<I>(
            m_image_ctx, m_crypto, object_no, extents, io_context,
            op_flags, read_flags, parent_trace, version, object_dispatch_flags,
            on_dispatched);
    req->send();
  }

  return true;
}

template <typename I>
bool CryptoObjectDispatch<I>::write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    IOContext io_context, int op_flags, int write_flags,
    std::optional<uint64_t> assert_version,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (object_no < m_data_offset_object_no) {
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << data.length() << dendl;
  ceph_assert(m_crypto != nullptr);

  if (m_crypto->is_aligned(object_off, data.length())) {
    auto r = m_crypto->encrypt(
        &data, get_file_offset(m_image_ctx, object_no, object_off));
    *dispatch_result = r == 0 ? io::DISPATCH_RESULT_CONTINUE
                              : io::DISPATCH_RESULT_COMPLETE;
    on_dispatched->complete(r);
  } else {
    *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
    auto req = new C_UnalignedObjectWriteRequest<I>(
            m_image_ctx, m_crypto, object_no, object_off, std::move(data), {},
            nullptr, io_context, op_flags, write_flags, assert_version,
            parent_trace, object_dispatch_flags, journal_tid, on_dispatched,
            true);
    req->send();
  }

  return true;
}

template <typename I>
bool CryptoObjectDispatch<I>::write_same(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    io::LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
    IOContext io_context, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (object_no < m_data_offset_object_no) {
    return false;
  }

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
          m_image_ctx,
          io::util::get_previous_layer(io::OBJECT_DISPATCH_LAYER_CRYPTO),
          object_no, object_off, std::move(ws_data), io_context, op_flags, 0,
          std::nullopt, 0, parent_trace, ctx);
  req->send();
  return true;
}

template <typename I>
bool CryptoObjectDispatch<I>::compare_and_write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
    ceph::bufferlist&& write_data, IOContext io_context, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
    int* object_dispatch_flags, uint64_t* journal_tid,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  if (object_no < m_data_offset_object_no) {
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << data_object_name(m_image_ctx, object_no) << " "
                 << object_off << "~" << write_data.length()
                 << dendl;
  ceph_assert(m_crypto != nullptr);

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  auto req = new C_UnalignedObjectWriteRequest<I>(
          m_image_ctx, m_crypto, object_no, object_off, std::move(write_data),
          std::move(cmp_data), mismatch_offset, io_context, op_flags, 0,
          std::nullopt, parent_trace, object_dispatch_flags, journal_tid,
          on_dispatched, true);
  req->send();

  return true;
}

template <typename I>
bool CryptoObjectDispatch<I>::discard(
        uint64_t object_no, uint64_t object_off, uint64_t object_len,
        IOContext io_context, int discard_flags,
        const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
        uint64_t* journal_tid, io::DispatchResult* dispatch_result,
        Context** on_finish, Context* on_dispatched) {
  if (object_no < m_data_offset_object_no) {
    return false;
  }

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
          m_image_ctx,
          io::util::get_previous_layer(io::OBJECT_DISPATCH_LAYER_CRYPTO),
          object_no, object_off, object_len, {{0, object_len}}, std::move(bl),
          io_context, *object_dispatch_flags, 0, parent_trace, ctx);
  req->send();
  return true;
}

template <typename I>
int CryptoObjectDispatch<I>::prepare_copyup(
        uint64_t object_no,
        io::SnapshotSparseBufferlist* snapshot_sparse_bufferlist) {
  if (object_no < m_data_offset_object_no) {
    return 0;
  }

  ceph::bufferlist current_bl;
  current_bl.append_zero(m_image_ctx->get_object_size());

  for (auto& [key, extent_map]: *snapshot_sparse_bufferlist) {
    // update current_bl with data from extent_map
    for (auto& extent : extent_map) {
      auto &sbe = extent.get_val();
      if (sbe.state == io::SPARSE_EXTENT_STATE_DATA) {
        current_bl.begin(extent.get_off()).copy_in(extent.get_len(), sbe.bl);
      } else if (sbe.state == io::SPARSE_EXTENT_STATE_ZEROED) {
        ceph::bufferlist zeros;
        zeros.append_zero(extent.get_len());
        current_bl.begin(extent.get_off()).copy_in(extent.get_len(), zeros);
      }
    }

    // encrypt
    io::SparseBufferlist encrypted_sparse_bufferlist;
    for (auto& extent : extent_map) {
      auto [aligned_off, aligned_len] = m_crypto->align(
              extent.get_off(), extent.get_len());

      auto [image_extents, _] = io::util::object_to_area_extents(
          m_image_ctx, object_no, {{aligned_off, aligned_len}});

      ceph::bufferlist encrypted_bl;
      uint64_t position = 0;
      for (auto [image_offset, image_length]: image_extents) {
        ceph::bufferlist aligned_bl;
        aligned_bl.substr_of(current_bl, aligned_off + position, image_length);
        aligned_bl.rebuild(); // to deep copy aligned_bl from current_bl
        position += image_length;

        auto r = m_crypto->encrypt(&aligned_bl, image_offset);
        if (r != 0) {
          return r;
        }

        encrypted_bl.append(aligned_bl);
      }

      encrypted_sparse_bufferlist.insert(
        aligned_off, aligned_len, {io::SPARSE_EXTENT_STATE_DATA, aligned_len,
                                   std::move(encrypted_bl)});
    }

    // replace original plaintext sparse bufferlist with encrypted one
    extent_map.clear();
    extent_map.insert(std::move(encrypted_sparse_bufferlist));
  }

  return 0;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::CryptoObjectDispatch<librbd::ImageCtx>;
