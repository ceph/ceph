// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/NBDStream.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"

#include <libnbd.h>

namespace librbd {
namespace migration {

namespace {

const std::string SERVER_KEY {"server"};
const std::string PORT_KEY {"port"};

int extent_cb(void* data, const char* metacontext, uint64_t offset,
              uint32_t* entries, size_t nr_entries, int* error) {
  auto sparse_extents = reinterpret_cast<io::SparseExtents*>(data);

  uint64_t length = 0;
  for (size_t i=0; i<nr_entries; i+=2) {
    length += entries[i];
  }
  auto state = io::SPARSE_EXTENT_STATE_DATA;
  if (nr_entries == 2) {
    if (entries[1] & (LIBNBD_STATE_HOLE | LIBNBD_STATE_ZERO)) {
      state = io::SPARSE_EXTENT_STATE_ZEROED;
    }
  }
  sparse_extents->insert(offset, length, {state, length});
  return 1;
}

} // anonymous namespace

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::NBDStream::ReadRequest: " \
                           << this << " " << __func__ << ": "

template <typename I>
struct NBDStream<I>::ReadRequest {
  NBDStream* nbd_stream;
  io::Extents byte_extents;
  bufferlist* data;
  Context* on_finish;
  size_t index = 0;

  ReadRequest(NBDStream* nbd_stream, io::Extents&& byte_extents,
              bufferlist* data, Context* on_finish)
    : nbd_stream(nbd_stream), byte_extents(std::move(byte_extents)),
      data(data), on_finish(on_finish) {
    auto cct = nbd_stream->m_cct;
    ldout(cct, 20) << dendl;
  }

  void send() {
    data->clear();
    read();
  }

  void read() {
    if (index >= byte_extents.size()) {
      finish(0);
      return;
    }

    auto cct = nbd_stream->m_cct;
    auto [byte_offset, byte_length] = byte_extents[index++];
    ldout(cct, 20) << "byte_offset=" << byte_offset << " byte_length="
                   << byte_length << dendl;

    auto ptr = buffer::ptr_node::create(buffer::create_small_page_aligned(
      byte_length));
    int rc = nbd_pread(nbd_stream->m_nbd, ptr->c_str(), byte_length,
                       byte_offset, 0);
    if (rc == -1) {
      rc = nbd_get_errno();
      lderr(cct) << "pread " << byte_offset << "~" << byte_length << ": "
                 << nbd_get_error() << " (errno = " << rc << ")"
                 << dendl;
      finish(rc);
      return;
    }

    data->push_back(std::move(ptr));
    boost::asio::post(nbd_stream->m_strand, [this] { read(); });
  }

  void finish(int r) {
    auto cct = nbd_stream->m_cct;
    ldout(cct, 20) << "r=" << r << dendl;

    if (r < 0) {
      data->clear();
    }

    on_finish->complete(r);
    delete this;
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::NBDStream::ListSparseExtentsRequest: " \
                           << this << " " << __func__ << ": "

template <typename I>
struct NBDStream<I>::ListSparseExtentsRequest {
  NBDStream* nbd_stream;
  io::Extents byte_extents;
  io::SparseExtents* sparse_extents;
  Context* on_finish;
  size_t index = 0;

  ListSparseExtentsRequest(NBDStream* nbd_stream, io::Extents&& byte_extents,
                           io::SparseExtents* sparse_extents, Context* on_finish)
    : nbd_stream(nbd_stream), byte_extents(std::move(byte_extents)),
      sparse_extents(sparse_extents), on_finish(on_finish) {
    auto cct = nbd_stream->m_cct;
    ldout(cct, 20) << dendl;
  }

  void send() {
    list_sparse_extents();
  }

  void list_sparse_extents() {
    if (index >= byte_extents.size()) {
      finish(0);
      return;
    }

    auto cct = nbd_stream->m_cct;
    auto [byte_offset, byte_length] = byte_extents[index++];
    ldout(cct, 20) << "byte_offset=" << byte_offset << " byte_length="
                   << byte_length << dendl;

    int rc = nbd_block_status(nbd_stream->m_nbd, byte_length, byte_offset,
                              {extent_cb, sparse_extents}, 0);
    if (rc == -1) {
      rc = nbd_get_errno();
      lderr(cct) << "block_status " << byte_offset << "~" << byte_length << ": "
                 << nbd_get_error() << " (errno = " << rc << ")"
                 << dendl;
      finish(rc);
      return;
    }

    boost::asio::post(nbd_stream->m_strand, [this] { list_sparse_extents(); });
  }

  void finish(int r) {
    auto cct = nbd_stream->m_cct;
    ldout(cct, 20) << "r=" << r << dendl;

    on_finish->complete(r);
    delete this;
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::NBDStream: " \
                           << this << " " << __func__ << ": "

template <typename I>
NBDStream<I>::NBDStream(I* image_ctx, const json_spirit::mObject& json_object)
  : m_cct(image_ctx->cct), m_asio_engine(image_ctx->asio_engine),
    m_json_object(json_object),
    m_strand(boost::asio::make_strand(*m_asio_engine)) {
}

template <typename I>
NBDStream<I>::~NBDStream() {
  if (m_nbd != nullptr) {
    nbd_close(m_nbd);
  }
}

template <typename I>
void NBDStream<I>::open(Context* on_finish) {
  int rc;

  auto& server_value = m_json_object[SERVER_KEY];
  if (server_value.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate '" << SERVER_KEY << "' key" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto& port_value = m_json_object[PORT_KEY];
  if (port_value.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate '" << PORT_KEY << "' key" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  const char *m_server = &(server_value.get_str())[0];
  const char *m_port = &(port_value.get_str())[0];

  m_nbd = nbd_create();
  if (m_nbd == nullptr) {
    lderr(m_cct) << "failed to create nbd object '" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  rc = nbd_add_meta_context(m_nbd, LIBNBD_CONTEXT_BASE_ALLOCATION);
  if (rc == -1) {
    lderr(m_cct) << "failed to add nbd meta context '" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  rc = nbd_connect_tcp(m_nbd, m_server, m_port);
  if (rc == -1) {
    rc = nbd_get_errno();
    lderr(m_cct) << "failed to connect to nbd server: " << nbd_get_error()
                 << " (errno=" << rc << ")" << dendl;
    on_finish->complete(rc);
    return;
  }

  ldout(m_cct, 20) << "server=" << m_server << ", "
                   << "port=" << m_port << dendl;

  on_finish->complete(0);
}

template <typename I>
void NBDStream<I>::close(Context* on_finish) {
  ldout(m_cct, 20) << dendl;

  if (m_nbd != nullptr) {
    nbd_close(m_nbd);
    m_nbd = nullptr;
  }

  on_finish->complete(0);
}

template <typename I>
void NBDStream<I>::get_size(uint64_t* size, Context* on_finish) {
  ldout(m_cct, 20) << dendl;

  *size = nbd_get_size(m_nbd);
  on_finish->complete(0);
}

template <typename I>
void NBDStream<I>::read(io::Extents&& byte_extents,
                        bufferlist* data,
                        Context* on_finish) {
  ldout(m_cct, 20) << byte_extents << dendl;
  auto ctx = new ReadRequest(this, std::move(byte_extents), data, on_finish);
  boost::asio::post(m_strand, [ctx] { ctx->send(); });
}

template <typename I>
void NBDStream<I>::list_sparse_extents(io::Extents&& byte_extents,
                                       io::SparseExtents* sparse_extents,
                                       Context* on_finish) {
  ldout(m_cct, 20) << byte_extents << dendl;
  auto ctx = new ListSparseExtentsRequest(this, std::move(byte_extents),
                                          sparse_extents, on_finish);
  boost::asio::post(m_strand, [ctx] { ctx->send(); });
}

} // namespace migration
} // namespace librbd

template class librbd::migration::NBDStream<librbd::ImageCtx>;
