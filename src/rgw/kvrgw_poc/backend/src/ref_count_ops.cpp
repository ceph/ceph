// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "kv_store.hpp"
#include "object_value.hpp"
#include "ref_count.hpp"

namespace kvrgw {

void decrement_or_del_child_d(KvTransaction &tr, std::string_view d_key,
                              uint64_t object_size, bool shared)
{
  if (!shared) {
    tr.kv_del(d_key);
    return;
  }
  auto d_val = tr.kv_get(d_key);
  if (!d_val || !*d_val) {
    tr.kv_del(d_key);
    return;
  }
  const auto dref = read_d_ref_count(**d_val);
  if (!dref.shared || dref.ref_count <= 1) {
    tr.kv_del(d_key);
  }
  else {
    const auto data = d_data_portion(**d_val, object_size);
    tr.kv_put(d_key, write_d_with_ref(data, dref.ref_count - 1));
  }
}

} // namespace kvrgw
