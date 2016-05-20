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


#ifndef CEPH_MSG_PACKET_H_
#define CEPH_MSG_PACKET_H_

#include <vector>
#include <algorithm>
#include <iosfwd>

#include "include/types.h"
#include "common/Tub.h"
#include "common/deleter.h"
#include "msg/async/Event.h"

#include "const.h"

struct fragment {
    char* base;
    size_t size;
};

struct offload_info {
  ip_protocol_num protocol = ip_protocol_num::unused;
  bool needs_csum = false;
  uint8_t ip_hdr_len = 20;
  uint8_t tcp_hdr_len = 20;
  uint8_t udp_hdr_len = 8;
  bool needs_ip_csum = false;
  bool reassembled = false;
  uint16_t tso_seg_size = 0;
  // HW stripped VLAN header (CPU order)
  Tub<uint16_t> vlan_tci;
};

// Zero-copy friendly packet class
//
// For implementing zero-copy, we need a flexible destructor that can
// destroy packet data in different ways: decrementing a reference count,
// or calling a free()-like function.
//
// Moreover, we need different destructors for each set of fragments within
// a single fragment. For example, a header and trailer might need delete[]
// to be called, while the internal data needs a reference count to be
// released.  Matters are complicated in that fragments can be split
// (due to virtual/physical translation).
//
// To implement this, we associate each packet with a single destructor,
// but allow composing a packet from another packet plus a fragment to
// be added, with its own destructor, causing the destructors to be chained.
//
// The downside is that the data needed for the destructor is duplicated,
// if it is already available in the fragment itself.
//
// As an optimization, when we allocate small fragments, we allocate some
// extra space, so prepending to the packet does not require extra
// allocations.  This is useful when adding headers.
//
class Packet {
  // enough for lots of headers, not quite two cache lines:
  static constexpr size_t internal_data_size = 128 - 16;
  static constexpr size_t default_nr_frags = 4;

  struct pseudo_vector {
    fragment* _start;
    fragment* _finish;
    pseudo_vector(fragment* start, size_t nr)
        : _start(start), _finish(_start + nr) {}
    fragment* begin() { return _start; }
    fragment* end() { return _finish; }
    fragment& operator[](size_t idx) { return _start[idx]; }
  };

  struct impl {
    // when destroyed, virtual destructor will reclaim resources
    deleter _deleter;
    unsigned _len = 0;
    uint16_t _nr_frags = 0;
    uint16_t _allocated_frags;
    offload_info _offload_info;
    Tub<uint32_t> rss_hash;
    char data[internal_data_size]; // only frags[0] may use
    unsigned headroom = internal_data_size; // in data
    // FIXME: share data/frags space

    fragment frags[];

    impl(size_t nr_frags = default_nr_frags);
    impl(const impl&) = delete;
    impl(fragment frag, size_t nr_frags = default_nr_frags);

    pseudo_vector fragments() { return { frags, _nr_frags }; }

    static std::unique_ptr<impl> allocate(size_t nr_frags) {
      nr_frags = MAX(nr_frags, default_nr_frags);
      return std::unique_ptr<impl>(new (nr_frags) impl(nr_frags));
    }

    static std::unique_ptr<impl> copy(impl* old, size_t nr) {
      auto n = allocate(nr);
      n->_deleter = std::move(old->_deleter);
      n->_len = old->_len;
      n->_nr_frags = old->_nr_frags;
      n->headroom = old->headroom;
      n->_offload_info = old->_offload_info;
      n->rss_hash.construct(old->rss_hash);
      std::copy(old->frags, old->frags + old->_nr_frags, n->frags);
      old->copy_internal_fragment_to(n.get());
      return std::move(n);
    }

    static std::unique_ptr<impl> copy(impl* old) {
      return copy(old, old->_nr_frags);
    }

    static std::unique_ptr<impl> allocate_if_needed(std::unique_ptr<impl> old, size_t extra_frags) {
      if (old->_allocated_frags >= old->_nr_frags + extra_frags) {
        return std::move(old);
      }
      return copy(old.get(), std::max<size_t>(old->_nr_frags + extra_frags, 2 * old->_nr_frags));
    }
    void* operator new(size_t size, size_t nr_frags = default_nr_frags) {
      assert(nr_frags == uint16_t(nr_frags));
      return ::operator new(size + nr_frags * sizeof(fragment));
    }
    // Matching the operator new above
    void operator delete(void* ptr, size_t nr_frags) {
      return ::operator delete(ptr);
    }
    // Since the above "placement delete" hides the global one, expose it
    void operator delete(void* ptr) {
      return ::operator delete(ptr);
    }

    bool using_internal_data() const {
      return _nr_frags
              && frags[0].base >= data
              && frags[0].base < data + internal_data_size;
    }

    void unuse_internal_data() {
      if (!using_internal_data()) {
        return;
      }
      auto buf = static_cast<char*>(::malloc(frags[0].size));
      if (!buf) {
        throw std::bad_alloc();
      }
      deleter d = make_free_deleter(buf);
      std::copy(frags[0].base, frags[0].base + frags[0].size, buf);
      frags[0].base = buf;
      _deleter.append(std::move(d));
      headroom = internal_data_size;
    }
    void copy_internal_fragment_to(impl* to) {
      if (!using_internal_data()) {
        return;
      }
      to->frags[0].base = to->data + headroom;
      std::copy(frags[0].base, frags[0].base + frags[0].size,
              to->frags[0].base);
    }
  };
  Packet(std::unique_ptr<impl>&& impl) : _impl(std::move(impl)) {}
  std::unique_ptr<impl> _impl;
public:
  static Packet from_static_data(const char* data, size_t len) {
    return {fragment{const_cast<char*>(data), len}, deleter()};
  }

  // build empty Packet
  Packet();
  // build empty Packet with nr_frags allocated
  Packet(size_t nr_frags);
  // move existing Packet
  Packet(Packet&& x) noexcept;
  // copy data into Packet
  Packet(const char* data, size_t len);
  // copy data into Packet
  Packet(fragment frag);
  // zero-copy single fragment
  Packet(fragment frag, deleter del);
  // zero-copy multiple fragments
  Packet(std::vector<fragment> frag, deleter del);
  // build Packet with iterator
  template <typename Iterator>
  Packet(Iterator begin, Iterator end, deleter del);
  // append fragment (copying new fragment)
  Packet(Packet&& x, fragment frag);
  // prepend fragment (copying new fragment, with header optimization)
  Packet(fragment frag, Packet&& x);
  // prepend fragment (zero-copy)
  Packet(fragment frag, deleter del, Packet&& x);
  // append fragment (zero-copy)
  Packet(Packet&& x, fragment frag, deleter d);
  // append deleter
  Packet(Packet&& x, deleter d);

  Packet& operator=(Packet&& x) {
    if (this != &x) {
      this->~Packet();
      new (this) Packet(std::move(x));
    }
    return *this;
  }

  unsigned len() const { return _impl->_len; }
  unsigned memory() const { return len() +  sizeof(Packet::impl); }

  fragment frag(unsigned idx) const { return _impl->frags[idx]; }
  fragment& frag(unsigned idx) { return _impl->frags[idx]; }

  unsigned nr_frags() const { return _impl->_nr_frags; }
  pseudo_vector fragments() const { return { _impl->frags, _impl->_nr_frags }; }
  fragment* fragment_array() const { return _impl->frags; }

  // share Packet data (reference counted, non COW)
  Packet share();
  Packet share(size_t offset, size_t len);

  void append(Packet&& p);

  void trim_front(size_t how_much);
  void trim_back(size_t how_much);

  // get a header pointer, linearizing if necessary
  template <typename Header>
  Header* get_header(size_t offset = 0);

  // get a header pointer, linearizing if necessary
  char* get_header(size_t offset, size_t size);

  // prepend a header (default-initializing it)
  template <typename Header>
  Header* prepend_header(size_t extra_size = 0);

  // prepend a header (uninitialized!)
  char* prepend_uninitialized_header(size_t size);

  Packet free_on_cpu(EventCenter *c, std::function<void()> cb = []{});

  void linearize() { return linearize(0, len()); }

  void reset() { _impl.reset(); }

  void reserve(int n_frags) {
    if (n_frags > _impl->_nr_frags) {
      auto extra = n_frags - _impl->_nr_frags;
      _impl = impl::allocate_if_needed(std::move(_impl), extra);
    }
  }
  Tub<uint32_t> rss_hash() {
    return _impl->rss_hash;
  }
  void set_rss_hash(uint32_t hash) {
    _impl->rss_hash.construct(hash);
  }
private:
  void linearize(size_t at_frag, size_t desired_size);
  bool allocate_headroom(size_t size);
public:
  class offload_info offload_info() const { return _impl->_offload_info; }
  class offload_info& offload_info_ref() { return _impl->_offload_info; }
  void set_offload_info(class offload_info oi) { _impl->_offload_info = oi; }
};

std::ostream& operator<<(std::ostream& os, const Packet& p);

inline Packet::Packet(Packet&& x) noexcept
    : _impl(std::move(x._impl)) {
}

inline Packet::impl::impl(size_t nr_frags)
    : _len(0), _allocated_frags(nr_frags) {
}

inline Packet::impl::impl(fragment frag, size_t nr_frags)
    : _len(frag.size), _allocated_frags(nr_frags) {
    assert(_allocated_frags > _nr_frags);
  if (frag.size <= internal_data_size) {
    headroom -= frag.size;
    frags[0] = { data + headroom, frag.size };
  } else {
    auto buf = static_cast<char*>(::malloc(frag.size));
    if (!buf) {
      throw std::bad_alloc();
    }
    deleter d = make_free_deleter(buf);
    frags[0] = { buf, frag.size };
    _deleter.append(std::move(d));
  }
  std::copy(frag.base, frag.base + frag.size, frags[0].base);
  ++_nr_frags;
}

inline Packet::Packet(): _impl(impl::allocate(1)) {
}

inline Packet::Packet(size_t nr_frags): _impl(impl::allocate(nr_frags)) {
}

inline Packet::Packet(fragment frag): _impl(new impl(frag)) {
}

inline Packet::Packet(const char* data, size_t size):
    Packet(fragment{const_cast<char*>(data), size}) {
}

inline Packet::Packet(fragment frag, deleter d)
    : _impl(impl::allocate(1)) {
  _impl->_deleter = std::move(d);
  _impl->frags[_impl->_nr_frags++] = frag;
  _impl->_len = frag.size;
}

inline Packet::Packet(std::vector<fragment> frag, deleter d)
    : _impl(impl::allocate(frag.size())) {
  _impl->_deleter = std::move(d);
  std::copy(frag.begin(), frag.end(), _impl->frags);
  _impl->_nr_frags = frag.size();
  _impl->_len = 0;
  for (auto&& f : _impl->fragments()) {
    _impl->_len += f.size;
  }
}

template <typename Iterator>
inline Packet::Packet(Iterator begin, Iterator end, deleter del) {
  unsigned nr_frags = 0, len = 0;
  nr_frags = std::distance(begin, end);
  std::for_each(begin, end, [&] (fragment& frag) { len += frag.size; });
  _impl = impl::allocate(nr_frags);
  _impl->_deleter = std::move(del);
  _impl->_len = len;
  _impl->_nr_frags = nr_frags;
  std::copy(begin, end, _impl->frags);
}

inline Packet::Packet(Packet&& x, fragment frag)
    : _impl(impl::allocate_if_needed(std::move(x._impl), 1)) {
  _impl->_len += frag.size;
  char* buf = new char[frag.size];
  std::copy(frag.base, frag.base + frag.size, buf);
  _impl->frags[_impl->_nr_frags++] = {buf, frag.size};
  _impl->_deleter = make_deleter(std::move(_impl->_deleter), [buf] {
    delete[] buf;
  });
}

inline bool Packet::allocate_headroom(size_t size) {
  if (_impl->headroom >= size) {
    _impl->_len += size;
    if (!_impl->using_internal_data()) {
      _impl = impl::allocate_if_needed(std::move(_impl), 1);
      std::copy_backward(_impl->frags, _impl->frags + _impl->_nr_frags,
              _impl->frags + _impl->_nr_frags + 1);
      _impl->frags[0] = { _impl->data + internal_data_size, 0 };
      ++_impl->_nr_frags;
    }
    _impl->headroom -= size;
    _impl->frags[0].base -= size;
    _impl->frags[0].size += size;
    return true;
  } else {
    return false;
  }
}


inline Packet::Packet(fragment frag, Packet&& x)
    : _impl(std::move(x._impl)) {
  // try to prepend into existing internal fragment
  if (allocate_headroom(frag.size)) {
    std::copy(frag.base, frag.base + frag.size, _impl->frags[0].base);
    return;
  } else {
    // didn't work out, allocate and copy
    _impl->unuse_internal_data();
    _impl = impl::allocate_if_needed(std::move(_impl), 1);
    _impl->_len += frag.size;
    char *buf = new char[frag.size];
    std::copy(frag.base, frag.base + frag.size, buf);
    std::copy_backward(_impl->frags, _impl->frags + _impl->_nr_frags,
            _impl->frags + _impl->_nr_frags + 1);
    ++_impl->_nr_frags;
    _impl->frags[0] = {buf, frag.size};
    _impl->_deleter = make_deleter(
            std::move(_impl->_deleter), [buf] { delete []buf; });
  }
}

inline Packet::Packet(Packet&& x, fragment frag, deleter d)
    : _impl(impl::allocate_if_needed(std::move(x._impl), 1)) {
  _impl->_len += frag.size;
  _impl->frags[_impl->_nr_frags++] = frag;
  d.append(std::move(_impl->_deleter));
  _impl->_deleter = std::move(d);
}

inline Packet::Packet(Packet&& x, deleter d): _impl(std::move(x._impl)) {
  _impl->_deleter.append(std::move(d));
}

inline void Packet::append(Packet&& p) {
  if (!_impl->_len) {
    *this = std::move(p);
    return;
  }
  _impl = impl::allocate_if_needed(std::move(_impl), p._impl->_nr_frags);
  _impl->_len += p._impl->_len;
  p._impl->unuse_internal_data();
  std::copy(p._impl->frags, p._impl->frags + p._impl->_nr_frags,
            _impl->frags + _impl->_nr_frags);
  _impl->_nr_frags += p._impl->_nr_frags;
  p._impl->_deleter.append(std::move(_impl->_deleter));
  _impl->_deleter = std::move(p._impl->_deleter);
}

inline char* Packet::get_header(size_t offset, size_t size) {
  if (offset + size > _impl->_len) {
    return nullptr;
  }
  size_t i = 0;
  while (i != _impl->_nr_frags && offset >= _impl->frags[i].size) {
    offset -= _impl->frags[i++].size;
  }
  if (i == _impl->_nr_frags) {
    return nullptr;
  }
  if (offset + size > _impl->frags[i].size) {
    linearize(i, offset + size);
  }
  return _impl->frags[i].base + offset;
}

template <typename Header>
inline Header* Packet::get_header(size_t offset) {
  return reinterpret_cast<Header*>(get_header(offset, sizeof(Header)));
}

inline void Packet::trim_front(size_t how_much) {
  assert(how_much <= _impl->_len);
  _impl->_len -= how_much;
  size_t i = 0;
  while (how_much && how_much >= _impl->frags[i].size) {
    how_much -= _impl->frags[i++].size;
  }
  std::copy(_impl->frags + i, _impl->frags + _impl->_nr_frags, _impl->frags);
  _impl->_nr_frags -= i;
  if (!_impl->using_internal_data()) {
    _impl->headroom = internal_data_size;
  }
  if (how_much) {
    if (_impl->using_internal_data()) {
      _impl->headroom += how_much;
    }
    _impl->frags[0].base += how_much;
    _impl->frags[0].size -= how_much;
  }
}

inline void Packet::trim_back(size_t how_much) {
  assert(how_much <= _impl->_len);
  _impl->_len -= how_much;
  size_t i = _impl->_nr_frags - 1;
  while (how_much && how_much >= _impl->frags[i].size) {
    how_much -= _impl->frags[i--].size;
  }
  _impl->_nr_frags = i + 1;
  if (how_much) {
    _impl->frags[i].size -= how_much;
    if (i == 0 && _impl->using_internal_data()) {
        _impl->headroom += how_much;
    }
  }
}

template <typename Header>
Header* Packet::prepend_header(size_t extra_size) {
  auto h = prepend_uninitialized_header(sizeof(Header) + extra_size);
  return new (h) Header{};
}

// prepend a header (uninitialized!)
inline char* Packet::prepend_uninitialized_header(size_t size) {
  if (!allocate_headroom(size)) {
    // didn't work out, allocate and copy
    _impl->unuse_internal_data();
    // try again, after unuse_internal_data we may have space after all
    if (!allocate_headroom(size)) {
      // failed
      _impl->_len += size;
      _impl = impl::allocate_if_needed(std::move(_impl), 1);
      char *buf = new char[size];
      std::copy_backward(_impl->frags, _impl->frags + _impl->_nr_frags,
              _impl->frags + _impl->_nr_frags + 1);
      ++_impl->_nr_frags;
      _impl->frags[0] = {buf, size};
      _impl->_deleter = make_deleter(std::move(_impl->_deleter),
              [buf] { delete []buf; });
    }
  }
  return _impl->frags[0].base;
}

inline Packet Packet::share() {
    return share(0, _impl->_len);
}

inline Packet Packet::share(size_t offset, size_t len) {
  _impl->unuse_internal_data(); // FIXME: eliminate?
  Packet n;
  n._impl = impl::allocate_if_needed(std::move(n._impl), _impl->_nr_frags);
  size_t idx = 0;
  while (offset > 0 && offset >= _impl->frags[idx].size) {
    offset -= _impl->frags[idx++].size;
  }
  while (n._impl->_len < len) {
    auto& f = _impl->frags[idx++];
    auto fsize = std::min(len - n._impl->_len, f.size - offset);
    n._impl->frags[n._impl->_nr_frags++] = { f.base + offset, fsize };
    n._impl->_len += fsize;
    offset = 0;
  }
  n._impl->_offload_info = _impl->_offload_info;
  assert(!n._impl->_deleter);
  n._impl->_deleter = _impl->_deleter.share();
  return n;
}

#endif /* CEPH_MSG_PACKET_H_ */
