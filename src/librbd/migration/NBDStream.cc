// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/NBDStream.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include <boost/asio/buffer.hpp>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/chrono.h>
#include <fmt/format.h>

#include <time.h>

namespace librbd {
namespace migration {

namespace {

const std::string SERVER_KEY {"server"};
const std::string PORT_KEY {"port"};

} // anonymous namespace

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::NBDStream: " << this \
                           << " " << __func__ << ": "

int check_extent(void *data, 
                 const char *metacontext,
                 uint64_t offset,
                 uint32_t *entries, size_t nr_entries, int *error) {
  io::SparseExtents* sparse_extents = (io::SparseExtents*)data;
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

template <typename I>
struct NBDStream<I>::ReadRequest {
  NBDStream*  nbd_stream;
  io::Extents byte_extents;
  bufferlist* data;
  Context* on_finish;

  ReadRequest(NBDStream* nbd_stream, io::Extents&& byte_extents,
              bufferlist* data, Context* on_finish)
    : nbd_stream(nbd_stream), byte_extents(std::move(byte_extents)),
      data(data), on_finish(on_finish) {
    auto cct = nbd_stream->m_cct;
    ldout(cct, 20) << dendl;
  }

  void send_read() {
    data->clear();
    read();
  }

  void read() {
    auto cct = nbd_stream->m_cct;  
    struct nbd_handle *nbd = nbd_stream->nbd;
    int rc=0;

    ldout(cct, 20) << "byte_extents=" << byte_extents << dendl;
    int extent = 0;
    for (auto [byte_offset, byte_length] : byte_extents) {
      ldout(cct, 20) << "byte_offset=" << byte_offset << dendl;
      ldout(cct, 20) << "byte_length=" << byte_length << dendl;
      auto ptr = buffer::ptr_node::create(buffer::create_small_page_aligned(
        byte_length));
      auto buffer = boost::asio::mutable_buffer(ptr->c_str(), byte_length);
      data->push_back(std::move(ptr));
      rc = nbd_pread(nbd, boost::asio::buffer_cast<void *>(buffer),
        byte_length, byte_offset, 0);
      if(rc == -1) {
        rc = nbd_get_errno();
        lderr(cct) << "nbd_pread: " << nbd_get_error()
                   << " (errno=" << rc << ")" 
                   <<  byte_offset << " " << byte_length << dendl;
        on_finish->complete(rc);
        return;
      }
      extent++;
    }

    finish(0);
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

template <typename I>
struct NBDStream<I>::ListRequest {
  NBDStream*  nbd_stream;
  io::Extents byte_extents;
  io::SparseExtents* sparse_extents;
  Context* on_finish;

  ListRequest(NBDStream* nbd_stream, io::Extents&& byte_extents,
              io::SparseExtents* sparse_extents, Context* on_finish)
    : nbd_stream(nbd_stream), byte_extents(std::move(byte_extents)),
      sparse_extents(sparse_extents), on_finish(on_finish) {
    auto cct = nbd_stream->m_cct;
    ldout(cct, 20) << dendl;
  }

  void send_list() {
    list();
  }

  void list() {
    auto cct = nbd_stream->m_cct;  
    struct nbd_handle *nbd = nbd_stream->nbd;
    int rc=0;

    ldout(cct, 20) << dendl;
    int extent = 0;
    unsigned int size = nbd_get_size(nbd);
    for (auto& [byte_offset, byte_length] : byte_extents) {
      if (byte_offset+byte_length > size) {
        // avoid getting request out of bounds warning
        byte_length = size - byte_offset;
      }
      rc = nbd_block_status(nbd, byte_length, byte_offset,
        (nbd_extent_callback) { .callback=check_extent,
                                .user_data=sparse_extents }, 0);
      if (rc == -1) {
        rc = nbd_get_errno();
        lderr(cct) << "nbd_block_status: " << nbd_get_error()
                   << " (errno=" << rc << ")" 
                   << byte_offset << " " << byte_length << dendl;
        on_finish->complete(rc);
        return;
      }
      extent++;
    }

    finish(0);
  }

  void finish(int r) {
    auto cct = nbd_stream->m_cct;
    ldout(cct, 20) << "r=" << r << dendl;

    on_finish->complete(r);
    delete this;
  }
};

template <typename I>
NBDStream<I>::NBDStream(I* image_ctx, const json_spirit::mObject& json_object)
  : m_image_ctx(image_ctx), m_cct(image_ctx->cct),
    m_asio_engine(image_ctx->asio_engine), m_json_object(json_object),
    m_strand(boost::asio::make_strand(*m_asio_engine)) {
}

template <typename I>
NBDStream<I>::~NBDStream() {
}

template <typename I>
void NBDStream<I>::open(Context* on_finish) {
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

  nbd = nbd_create();
  if (nbd == NULL) {
    lderr(m_cct) << "failed to create nbd object '" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }
  if (nbd_add_meta_context (nbd, LIBNBD_CONTEXT_BASE_ALLOCATION) == -1) {
    lderr(m_cct) << "failed to add nbd meta context '" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }
  if (nbd_connect_tcp(nbd, m_server, m_port) == -1) {
    auto rc = nbd_get_errno();
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

  if (nbd != NULL) {
    nbd_close(nbd);
  }
  on_finish->complete(0);
}

template <typename I>
void NBDStream<I>::get_size(uint64_t* size, Context* on_finish) {
  ldout(m_cct, 20) << dendl;

  *size = nbd_get_size(nbd);
  on_finish->complete(0);
}

template <typename I>
void NBDStream<I>::read(io::Extents&& byte_extents,
                        bufferlist* data,
                        Context* on_finish) {
  ldout(m_cct, 20) << byte_extents << dendl;
  auto ctx = new ReadRequest(this, std::move(byte_extents), data, on_finish);
  boost::asio::post(m_strand, [ctx]() { ctx->send_read(); });
}

template <typename I>
void NBDStream<I>::list_raw_snap(io::Extents&& image_extents,
                                 io::SparseExtents* sparse_extents, 
                                 Context* on_finish) {
  ldout(m_cct, 20) << image_extents << dendl;
  auto ctx = new ListRequest(this, std::move(image_extents), sparse_extents, on_finish);
  boost::asio::post(m_strand, [ctx]() { ctx->send_list(); });
}

} // namespace migration
} // namespace librbd

template class librbd::migration::NBDStream<librbd::ImageCtx>;
