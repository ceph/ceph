// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <algorithm>
#include <string>

#include "rgw_putobj.h"

namespace rgw::putobj {

/// Finalize a digest and return it as a hex-encoded string.
template <typename Digest>
std::string finalize(Digest& digest)
{
  unsigned char buf[Digest::digest_size];
  digest.Final(buf);

  std::string hex;
  hex.resize(Digest::digest_size * 2);

  auto out = hex.begin();
  std::for_each(std::begin(buf), std::end(buf),
      [&out] (unsigned char c) {
        constexpr auto table = std::string_view{"0123456789abcdef"};
        *out++ = table[c >> 4]; // high 4 bits
        *out++ = table[c & 0xf]; // low 4 bits
      });
  return hex;
}

/// A streaming data processor that performs inline hashing of incoming
/// bytes before forwarding them to the wrapped processor. When the processor
/// is flushed by calling process() with an empty buffer, the final sum is
/// copied to the given output string.
template <typename Digest>
class HashPipe : public putobj::Pipe {
  std::string& output;
  Digest digest;

 public:
  template <typename ...DigestArgs>
  HashPipe(sal::DataProcessor* next, std::string& output, DigestArgs&& ...args)
    : Pipe(next), output(output), digest(std::forward<DigestArgs>(args)...)
  {}

  int process(bufferlist&& data, uint64_t logical_offset) override
  {
    if (data.length() == 0) {
      // flush the pipe and finalize the digest
      output = finalize(digest);
    } else {
      // hash each buffer segment
      for (const auto& ptr : data.buffers()) {
        digest.Update(reinterpret_cast<const unsigned char*>(ptr.c_str()),
                      ptr.length());
      }
    }

    return Pipe::process(std::move(data), logical_offset);
  }
};

} // namespace rgw::putobj
