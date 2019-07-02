// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Socket.h"
#include <seastar/core/polymorphic_temporary_buffer.hh>

#include "Errors.h"

namespace ceph::net {

namespace {

inline seastar::temporary_buffer<char> sharing_split(
  seastar::temporary_buffer<char>& buf,
  const size_t bytes)
{
  auto shared_part = buf.share(0, bytes);
  buf.trim_front(bytes);
  return shared_part;
}

} // anonymous namespace

void Socket::return_unused(buffer_t&& buf) {
  returned_rxbuf = std::move(buf);
}

seastar::temporary_buffer<char>
Socket::create(seastar::compat::polymorphic_allocator<char>* const allocator)
{
#if 0
  return seastar::make_temporary_buffer<char>(allocator, 8192);
#else

  if (!returned_rxbuf.empty()) {
    return std::move(returned_rxbuf);
  }

  logger().debug("{}: read_hint: .further_bytes={}, alignment: .base={}, .at={}",
                 __func__,
                 read_hint.further_bytes,
                 read_hint.alignment.base,
                 read_hint.alignment.at);

  auto ret = seastar::make_temporary_buffer<char>(allocator,
    read_hint.alignment.base + read_hint.further_bytes);

  if (read_hint.alignment.base) {
    const auto offset_in_buf = read_hint.alignment.base
      - p2phase<uintptr_t>(reinterpret_cast<uintptr_t>(ret.get()),
                           read_hint.alignment.base)
      - read_hint.alignment.at;
    ret.trim_front(offset_in_buf);
    ret.trim(read_hint.further_bytes);
  }

  hinted_rxbuf = ceph::buffer::create(ret.share());
  return ret;

  // TODO: implement prefetching for very small (under 4K) chunk sizes to not
  // hurt RADOS' reads while the POSIX stack is being used (and till it lacks
  // io_uring support).
#endif
}


seastar::future<bufferlist> Socket::read(const size_t bytes)
{
  r.remaining = bytes;
  r.sgl.clear();
  return seastar::do_until(
    [this] { return r.remaining == 0; },
    [this] {
      if (wrapping_rxbuf.empty()) {
        return in.read().then([this] (read_buffer_t&& new_wrapping_rxbuf) {
          if (new_wrapping_rxbuf.empty()) {
            throw std::system_error(make_error_code(error::read_eof));
          }
          wrapping_rxbuf = std::move(new_wrapping_rxbuf);
          return seastar::now();
        });
      }

      // That's the reason for `hinted_rxbuf` – the variant of `bl::append()`
      // doing the bptr fusion (see implementation). The nice side effect is
      // not instantiating `bufferptr` nor `raw_seastar_local_ptr`.
      // Be aware that an allocator is free to arrange chunk's control block
      // as it wants. In the consequence *adjacent* chunks can be produced by
      // different calls to e.g. `malloc()`, and thus they must be reclaimed
      // separately.
      //
      // Alternatives:
      //  * extend `seastar::temporary_buffer` to offer the same feature as
      //    `ceph::bufferptr` with its `get_raw()` method: verification that
      //    two adjacent instances are really spanning the same memory chunk
      //    (from memory allocator's PoV).
      //  * Ensure for all allocators that two separated chunks are never ever
      //    adjacent.
      const size_t round_size = std::min(r.remaining, wrapping_rxbuf.size());
      if (hinted_rxbuf.c_str() <= wrapping_rxbuf.get() &&
          hinted_rxbuf.end_c_str() >= wrapping_rxbuf.get() + wrapping_rxbuf.size()) {
        // yay, S* gave us back (a part of) buffer the ibf had produced.
        // In other words: the wrapping_rxbuf is *subset* of hinted_rxbuf.
        size_t offset = wrapping_rxbuf.get() - hinted_rxbuf.c_str();
        r.sgl.append(hinted_rxbuf, offset, round_size);
        wrapping_rxbuf.trim_front(round_size);
      } else {
        r.sgl.push_back(buffer::create(sharing_split(wrapping_rxbuf, round_size)));
      }
      r.remaining -= round_size;
      return seastar::now();
    }
  ).then([this] {
    return seastar::make_ready_future<ceph::bufferlist>(std::move(r.sgl));
  });
}

seastar::future<Socket::read_buffer_t> Socket::read_exactly(size_t bytes) {
  if (bytes <= wrapping_rxbuf.size()) {
    // oh, the cheap and straightforward case ::read_exactly() is really
    // intended for.
    return seastar::make_ready_future<seastar::temporary_buffer<char>>(
        sharing_split(wrapping_rxbuf, bytes));
  }

  r.remaining = bytes;
  r.contiguous_buffer = seastar::temporary_buffer<char>(bytes);
  return seastar::do_until(
    [this] { return r.remaining == 0; },
    [this] {
      if (wrapping_rxbuf.empty()) {
        return in.read().then([this] (read_buffer_t&& new_wrapping_rxbuf) {
          if (new_wrapping_rxbuf.empty()) {
            throw std::system_error(make_error_code(error::read_eof));
          }
          wrapping_rxbuf = std::move(new_wrapping_rxbuf);
          return seastar::now();
        });
      }

      const size_t round_size = std::min(r.remaining, wrapping_rxbuf.size());
      const size_t completed = r.contiguous_buffer.size() - r.remaining;
      std::copy(wrapping_rxbuf.get(), wrapping_rxbuf.get() + round_size,
                r.contiguous_buffer.get_write() + completed);
      r.remaining -= round_size;
      wrapping_rxbuf.trim_front(round_size);
      return seastar::now();
    }
  ).then([this] {
    return seastar::make_ready_future<seastar::temporary_buffer<char>>(
        std::move(r.contiguous_buffer));
  });
}

} // namespace ceph::net
