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

const std::string URI_KEY{"uri"};

int from_nbd_errno(int rc) {
  // nbd_get_errno() needs a default/fallback error:
  // "Even when a call returns an error, nbd_get_errno() might return 0.
  // This does not mean there was no error. It means no additional errno
  // information is available for this error."
  return rc > 0 ? -rc : -EIO;
}

int extent_cb(void* data, const char* metacontext, uint64_t offset,
              uint32_t* entries, size_t nr_entries, int* error) {
  auto sparse_extents = reinterpret_cast<io::SparseExtents*>(data);

  // "[...] always check the metacontext field to ensure you are
  // receiving the data you expect."
  if (strcmp(metacontext, LIBNBD_CONTEXT_BASE_ALLOCATION) == 0) {
    for (size_t i = 0; i < nr_entries; i += 2) {
      auto length = entries[i];
      auto state = entries[i + 1];
      if (length > 0 && state & (LIBNBD_STATE_HOLE | LIBNBD_STATE_ZERO)) {
        sparse_extents->insert(offset, length,
                               {io::SPARSE_EXTENT_STATE_ZEROED, length});
      }
      offset += length;
    }
  }

  return 1;
}

} // anonymous namespace

template <typename>
class NBDClient {
public:
  static NBDClient* create() {
    return new NBDClient();
  }

  const char* get_error() {
    return nbd_get_error();
  }

  int get_errno() {
    return nbd_get_errno();
  }

  int init() {
    m_handle.reset(nbd_create());
    return m_handle != nullptr ? 0 : -1;
  }

  int add_meta_context(const char* name) {
    return nbd_add_meta_context(m_handle.get(), name);
  }

  int connect_uri(const char* uri) {
    return nbd_connect_uri(m_handle.get(), uri);
  }

  int64_t get_size() {
    return nbd_get_size(m_handle.get());
  }

  int pread(void* buf, size_t count, uint64_t offset, uint32_t flags) {
    return nbd_pread(m_handle.get(), buf, count, offset, flags);
  }

  int block_status(uint64_t count, uint64_t offset,
                   nbd_extent_callback extent_callback, uint32_t flags) {
    return nbd_block_status(m_handle.get(), count, offset, extent_callback,
                            flags);
  }

  int shutdown(uint32_t flags) {
    return nbd_shutdown(m_handle.get(), flags);
  }

private:
  struct nbd_handle_deleter {
    void operator()(nbd_handle* h) {
      nbd_close(h);
    }
  };
  std::unique_ptr<nbd_handle, nbd_handle_deleter> m_handle;
};

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

    auto& nbd_client = nbd_stream->m_nbd_client;
    auto ptr = buffer::ptr_node::create(buffer::create_small_page_aligned(
      byte_length));
    int rc = nbd_client->pread(ptr->c_str(), byte_length, byte_offset, 0);
    if (rc == -1) {
      rc = nbd_client->get_errno();
      lderr(cct) << "pread " << byte_offset << "~" << byte_length << ": "
                 << nbd_client->get_error() << " (errno = " << rc << ")"
                 << dendl;
      finish(from_nbd_errno(rc));
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

    // nbd_block_status() is specified to be really loose:
    // "The count parameter is a hint: the server may choose to
    // return less status, or the final block may extend beyond the
    // requested range. [...] It is possible for the extent function
    // to be called more times than you expect [...] It is also
    // possible that the extent function is not called at all, even
    // for metadata contexts that you requested."
    io::SparseExtents tmp_sparse_extents;
    tmp_sparse_extents.insert(byte_offset, byte_length,
                              {io::SPARSE_EXTENT_STATE_DATA, byte_length});

    auto& nbd_client = nbd_stream->m_nbd_client;
    int rc = nbd_client->block_status(byte_length, byte_offset,
                                      {extent_cb, &tmp_sparse_extents}, 0);
    if (rc == -1) {
      rc = nbd_client->get_errno();
      lderr(cct) << "block_status " << byte_offset << "~" << byte_length << ": "
                 << nbd_client->get_error() << " (errno = " << rc << ")"
                 << dendl;
      // don't propagate errors -- we are set up to list any missing
      // parts of the range as DATA if nbd_block_status() returns less
      // status or none at all
    }

    // trim the result in case more status was returned
    sparse_extents->insert(tmp_sparse_extents.intersect(byte_offset,
                                                        byte_length));

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
}

template <typename I>
void NBDStream<I>::open(Context* on_finish) {
  std::string uri;
  int rc;

  if (auto it = m_json_object.find(URI_KEY);
      it != m_json_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      uri = it->second.get_str();
    } else {
      lderr(m_cct) << "invalid URI" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  } else {
    lderr(m_cct) << "missing URI" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  ldout(m_cct, 10) << "uri=" << uri << dendl;

  m_nbd_client.reset(NBDClient<I>::create());
  rc = m_nbd_client->init();
  if (rc == -1) {
    rc = m_nbd_client->get_errno();
    lderr(m_cct) << "init: " << m_nbd_client->get_error()
                 << " (errno = " << rc << ")" << dendl;
    on_finish->complete(from_nbd_errno(rc));
    return;
  }

  rc = m_nbd_client->add_meta_context(LIBNBD_CONTEXT_BASE_ALLOCATION);
  if (rc == -1) {
    rc = m_nbd_client->get_errno();
    lderr(m_cct) << "add_meta_context: " << m_nbd_client->get_error()
                 << " (errno = " << rc << ")" << dendl;
    on_finish->complete(from_nbd_errno(rc));
    return;
  }

  rc = m_nbd_client->connect_uri(uri.c_str());
  if (rc == -1) {
    rc = m_nbd_client->get_errno();
    lderr(m_cct) << "connect_uri: " << m_nbd_client->get_error()
                 << " (errno = " << rc << ")" << dendl;
    on_finish->complete(from_nbd_errno(rc));
    return;
  }

  on_finish->complete(0);
}

template <typename I>
void NBDStream<I>::close(Context* on_finish) {
  ldout(m_cct, 20) << dendl;

  if (m_nbd_client != nullptr) {
    // send a graceful shutdown to the server
    // ignore errors -- we are read-only, also from the client's
    // POV there is no disadvantage to abruptly closing the socket
    // in nbd_close()
    m_nbd_client->shutdown(0);
    m_nbd_client.reset();
  }

  on_finish->complete(0);
}

template <typename I>
void NBDStream<I>::get_size(uint64_t* size, Context* on_finish) {
  ldout(m_cct, 20) << dendl;

  int64_t rc = m_nbd_client->get_size();
  if (rc == -1) {
    rc = m_nbd_client->get_errno();
    lderr(m_cct) << "get_size: " << m_nbd_client->get_error()
                 << " (errno = " << rc << ")" << dendl;
    on_finish->complete(from_nbd_errno(rc));
    return;
  }

  *size = rc;
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
