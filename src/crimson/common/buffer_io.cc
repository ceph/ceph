// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "buffer_io.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/do_with.hh>

#include "include/buffer.h"

namespace crimson {

seastar::future<> write_file(ceph::buffer::list&& bl,
                             seastar::sstring fn,
                             seastar::file_permissions permissions)
{
  const auto flags = (seastar::open_flags::wo |
                      seastar::open_flags::create |
                      seastar::open_flags::truncate);
  seastar::file_open_options foo;
  foo.create_permissions = permissions;
  return seastar::open_file_dma(fn, flags, foo).then(
    [bl=std::move(bl)](seastar::file f) {
    return seastar::make_file_output_stream(f).then(
      [bl=std::move(bl), f=std::move(f)](seastar::output_stream<char> out) {
      return seastar::do_with(std::move(out),
                              std::move(f),
                              std::move(bl),
                              [](seastar::output_stream<char>& out,
                                 seastar::file& f,
                                 ceph::buffer::list& bl) {
        return seastar::do_for_each(bl.buffers(), [&out](auto& buf) {
          return out.write(buf.c_str(), buf.length());
        }).then([&out] {
          return out.close();
        });
      });
    });
  });
}

seastar::future<seastar::temporary_buffer<char>>
read_file(const seastar::sstring fn)
{
  return seastar::open_file_dma(fn, seastar::open_flags::ro).then(
    [] (seastar::file f) {
    return f.size().then([f = std::move(f)](size_t s) {
      return seastar::do_with(seastar::make_file_input_stream(f),
			      [s](seastar::input_stream<char>& in) {
        return in.read_exactly(s);
      });
    });
  });
}

}
