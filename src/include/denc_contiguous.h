// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_INCLUDE_DENC_CONTIGUOUS_H
#define CEPH_INCLUDE_DENC_CONTIGUOUS_H

#include <cstddef>
#include <type_traits>

#include "include/buffer.h"
#include "include/denc.h"

namespace ceph {

/* A C struct the wire format carries as its raw bytes, the way
 * WRITE_RAW_ENCODER() encodes it, wrapped so that denc can place it in a
 * contiguous append alongside its neighbours. */
template<typename T>
struct raw_bytes_t {
  const T& v;
};

template<typename T>
raw_bytes_t<T> raw_bytes(const T& v)
{
  static_assert(std::is_trivially_copyable_v<T>);
  return raw_bytes_t<T>{v};
}

} // namespace ceph

template<typename T>
struct denc_traits<ceph::raw_bytes_t<T>> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = false;
  static void bound_encode(const ceph::raw_bytes_t<T>&, size_t& p) {
    p += sizeof(T);
  }
  static void encode(const ceph::raw_bytes_t<T>& o,
                     ceph::buffer::list::contiguous_appender& p) {
    p.append(reinterpret_cast<const char*>(&o.v), sizeof(T));
  }
};

namespace ceph {

/* Append several values back to back, bounds-checking the buffer once rather
 * than once per value.  Each is encoded exactly as encode() would encode it
 * on its own.  This is encode(std::tuple{...}) without the tuple, which would
 * copy every string, map and vector it holds.
 *
 * Meant for hot paths such as CInode::encode_inodestat(), where a bufferlist
 * append of a few bytes costs about as much as encoding the field did. */
template<typename... Ts>
void encode_contiguous(ceph::buffer::list& bl, const Ts&... vs)
{
  size_t len = 0;
  (denc(vs, len), ...);
  auto app = bl.get_contiguous_appender(len);
  (denc(vs, app), ...);
}

} // namespace ceph

#endif
