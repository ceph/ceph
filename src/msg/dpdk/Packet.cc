// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <algorithm>
#include <cctype>

#include "Packet.h"

constexpr size_t Packet::internal_data_size;
constexpr size_t Packet::default_nr_frags;

void Packet::linearize(size_t at_frag, size_t desired_size) {
  impl->unuse_internal_data();
  size_t nr_frags = 0;
  size_t accum_size = 0;
  while (accum_size < desired_size) {
    accum_size += impl->_frags[at_frag + nr_frags].size;
    ++nr_frags;
  }
  std::unique_ptr<char[]> new_frag{new char[accum_size]};
  auto p = new_frag.get();
  for (size_t i = 0; i < nr_frags; ++i) {
    auto& f = impl->_frags[at_frag + i];
    p = std::copy(f.base, f.base + f.size, p);
  }
  // collapse nr_frags into one fragment
  std::copy(impl->_frags + at_frag + nr_frags, impl->_frags + impl->_nr_frags,
            impl->_frags + at_frag + 1);
  impl->_nr_frags -= nr_frags - 1;
  impl->_frags[at_frag] = fragment{new_frag.get(), accum_size};
  if (at_frag == 0 && desired_size == len()) {
    // We can drop the old buffer safely
    auto x = std::move(impl->_deleter);
    auto buf = std::move(new_frag);
    impl->_deleter = make_deleter([buf] {});
  } else {
    auto del = std::move(impl->_deleter);
    auto buf = buf = std::move(new_frag);
    impl->_deleter = make_deleter([del, buf] {});
  }
}

class C_free_on_cpu : public EventCallback {
  deleter del;
  std::function<void()> cb;
 public:
  C_free_on_cpu(deleter &&d, std::function<void()> &&c):
      del(std::move(d)), cb(std::move(c)) {}
  void do_request(int fd) {
    // deleter needs to be moved from lambda capture to be destroyed here
    // otherwise deleter destructor will be called on a cpu that called
    // create_external_event when work_item is destroyed.
    deleter xxx(std::move(del));
    cb();
    delete this;
  }
};

Packet Packet::free_on_cpu(EventCenter *center, std::function<void()> cb)
{
  auto d = std::move(_impl->_deleter);
  auto cb = std::move(cb);
  // make new deleter that runs old deleter on an origin cpu
  _impl->_deleter = make_deleter(deleter(), [d, cpu, cb] () mutable {
    center->create_external_event(new C_free_on_cpu(std::move(d), std::move(cb)));
  });

  return Packet(impl::copy(_impl.get()));
}

std::ostream& operator<<(std::ostream& os, const Packet& p) {
  os << "Packet{";
  bool first = true;
  for (auto&& frag : p.fragments()) {
    if (!first) {
      os << ", ";
    }
    first = false;
    if (std::all_of(frag.base, frag.base + frag.size, [] (int c) { return c >= 9 && c <= 0x7f; })) {
      os << '"';
      for (auto p = frag.base; p != frag.base + frag.size; ++p) {
        auto c = *p;
        if (isprint(c)) {
          os << c;
        } else if (c == '\r') {
          os << "\\r";
        } else if (c == '\n') {
          os << "\\n";
        } else if (c == '\t') {
          os << "\\t";
        } else {
          uint8_t b = c;
          os << "\\x" << (b / 16) << (b % 16);
        }
      }
      os << '"';
    } else {
      os << "{";
      bool nfirst = true;
      for (auto p = frag.base; p != frag.base + frag.size; ++p) {
        if (!nfirst) {
          os << " ";
        }
        nfirst = false;
        uint8_t b = *p;
        os << sprint("%02x", unsigned(b));
      }
      os << "}";
    }
  }
  os << "}";
  return os;
}
