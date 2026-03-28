// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "aio.h"
#include "common/buffer_sequence.h"

namespace rgw {

struct FileReadHandler {
  Aio* throttle = nullptr;
  AioResult& r;
  buffer::ptr buffer;
  void operator()(boost::system::error_code ec, size_t bytes) {
    r.result = -ec.value();
    buffer.set_length(bytes);
    r.data.append(std::move(buffer));
    throttle->put(r);
  }
};

Aio::OpFunc file_read_op(boost::asio::random_access_file& file,
                         uint64_t offset, uint64_t len)
{
  return [&file, offset, len] (Aio* aio, AioResult& r) mutable {
    buffer::ptr p = buffer::create(len);
    auto buffer = boost::asio::mutable_buffer{p.c_str(), p.length()};
    file.async_read_some_at(offset, buffer,
                            FileReadHandler{aio, r, std::move(p)});
  };
}


struct FileWriteHandler {
  Aio* throttle = nullptr;
  AioResult& r;
  buffer::list bl;
  void operator()(boost::system::error_code ec, size_t bytes) const {
    r.result = -ec.value();
    throttle->put(r);
  }
};

Aio::OpFunc file_write_op(boost::asio::random_access_file& file,
                          uint64_t offset, bufferlist bl)
{
  return [&file, offset, bl=std::move(bl)] (Aio* aio, AioResult& r) mutable {
    auto buffers = ceph::buffer::const_sequence{bl};
    file.async_write_some_at(offset, buffers,
                             FileWriteHandler{aio, r, std::move(bl)});
  };
}

} // namespace rgw
