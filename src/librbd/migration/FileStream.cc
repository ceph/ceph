// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif // _LARGEFILE64_SOURCE

#include "librbd/migration/FileStream.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/asio/Utils.h"
#include <boost/asio/buffer.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <fcntl.h>
#include <unistd.h>

namespace librbd {
namespace migration {

namespace {

const std::string FILE_PATH {"file_path"};

} // anonymous namespace

#ifdef BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::FileStream::ReadRequest " \
                           << this << " " << __func__ << ": "

template <typename I>
struct FileStream<I>::ReadRequest {
  FileStream* file_stream;
  io::Extents byte_extents;
  bufferlist* data;
  Context* on_finish;
  size_t index = 0;

  ReadRequest(FileStream* file_stream, io::Extents&& byte_extents,
              bufferlist* data, Context* on_finish)
    : file_stream(file_stream), byte_extents(std::move(byte_extents)),
      data(data), on_finish(on_finish) {
    auto cct = file_stream->m_cct;
    ldout(cct, 20) << dendl;
  }

  void send() {
    data->clear();
    read();
  }

  void read() {
    auto cct = file_stream->m_cct;
    if (index >= byte_extents.size()) {
      finish(0);
      return;
    }

    auto& byte_extent = byte_extents[index++];
    ldout(cct, 20) << "byte_extent=" << byte_extent << dendl;

    auto ptr = buffer::ptr_node::create(buffer::create_small_page_aligned(
      byte_extent.second));
    auto buffer = boost::asio::mutable_buffer(
      ptr->c_str(), byte_extent.second);
    data->push_back(std::move(ptr));

    int r;
    auto offset = lseek64(file_stream->m_file_no, byte_extent.first, SEEK_SET);
    if (offset == -1) {
      r = -errno;
      lderr(cct) << "failed to seek file: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    boost::system::error_code ec;
    size_t bytes_read = boost::asio::read(
      *file_stream->m_stream_descriptor, std::move(buffer), ec);
    r = -ec.value();
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "failed to read from file stream: " << cpp_strerror(r)
                 << dendl;
      finish(r);
      return;
    } else if (bytes_read < byte_extent.second) {
      lderr(cct) << "failed to read " << byte_extent.second << " bytes from "
                 << "file stream" << dendl;
      finish(-ERANGE);
      return;
    }

    // re-queue the remainder of the read requests
    boost::asio::post(file_stream->m_strand, [this]() { read(); });
  }

  void finish(int r) {
    auto cct = file_stream->m_cct;
    ldout(cct, 20) << "r=" << r << dendl;

    if (r < 0) {
      data->clear();
    }

    on_finish->complete(r);
    delete this;
  }
};

#endif // BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::FileStream: " << this \
                           << " " << __func__ << ": "

template <typename I>
FileStream<I>::FileStream(I* image_ctx, const json_spirit::mObject& json_object)
  : m_cct(image_ctx->cct), m_asio_engine(image_ctx->asio_engine),
    m_json_object(json_object),
    m_strand(boost::asio::make_strand(*m_asio_engine)) {
}

template <typename I>
FileStream<I>::~FileStream() {
  if (m_file_no != -1) {
    ::close(m_file_no);
  }
}

#ifdef BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR

template <typename I>
void FileStream<I>::open(Context* on_finish) {
  auto& file_path_value = m_json_object[FILE_PATH];
  if (file_path_value.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate '" << FILE_PATH << "' key" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto& file_path = file_path_value.get_str();
  ldout(m_cct, 10) << "file_path=" << file_path << dendl;

  m_file_no = ::open(file_path.c_str(), O_RDONLY);
  if (m_file_no < 0) {
    int r = -errno;
    lderr(m_cct) << "failed to open file '" << file_path << "': "
                 << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  m_stream_descriptor = std::make_optional<
    boost::asio::posix::stream_descriptor>(m_strand, m_file_no);
  on_finish->complete(0);
}

template <typename I>
void FileStream<I>::close(Context* on_finish) {
  ldout(m_cct, 10) << dendl;

  m_stream_descriptor.reset();
  on_finish->complete(0);
}

template <typename I>
void FileStream<I>::get_size(uint64_t* size, Context* on_finish) {
  ldout(m_cct, 10) << dendl;

  // execute IO operations in a single strand to prevent seek races
  boost::asio::post(
    m_strand, [this, size, on_finish]() {
      auto offset = lseek64(m_file_no, 0, SEEK_END);
      if (offset == -1) {
        int r = -errno;
        lderr(m_cct) << "failed to seek to file end: " << cpp_strerror(r)
                     << dendl;
        on_finish->complete(r);
        return;
      }

      ldout(m_cct, 10) << "size=" << offset << dendl;
      *size = offset;
      on_finish->complete(0);
    });
}

template <typename I>
void FileStream<I>::read(io::Extents&& byte_extents, bufferlist* data,
                         Context* on_finish) {
  ldout(m_cct, 20) << byte_extents << dendl;

  auto ctx = new ReadRequest(this, std::move(byte_extents), data, on_finish);

  // execute IO operations in a single strand to prevent seek races
  boost::asio::post(m_strand, [ctx]() { ctx->send(); });
}

#else  // BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR

template <typename I>
void FileStream<I>::open(Context* on_finish) {
  on_finish->complete(-EIO);
}

template <typename I>
void FileStream<I>::close(Context* on_finish) {
  on_finish->complete(-EIO);
}

template <typename I>
void FileStream<I>::get_size(uint64_t* size, Context* on_finish) {
  on_finish->complete(-EIO);
}

template <typename I>
void FileStream<I>::read(io::Extents&& byte_extents, bufferlist* data,
                         Context* on_finish) {
  on_finish->complete(-EIO);
}

#endif // BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR

} // namespace migration
} // namespace librbd

template class librbd::migration::FileStream<librbd::ImageCtx>;
