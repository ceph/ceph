// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <boost/intrusive/list.hpp>
#include "rgw_common.h"

namespace librados {
class ObjectReadOperation;
class ObjectWriteOperation;
}
struct rgw_rados_ref;

namespace rgw::putobj {

struct Result {
  rgw_raw_obj obj;
  int result = 0;
};
struct ResultEntry : Result, boost::intrusive::list_base_hook<> {
  virtual ~ResultEntry() {}
};
// a list of polymorphic entries that frees them on destruction
template <typename T, typename ...Args>
struct OwningList : boost::intrusive::list<T, Args...> {
  OwningList() = default;
  ~OwningList() { this->clear_and_dispose(std::default_delete<T>{}); }
  OwningList(OwningList&&) = default;
  OwningList& operator=(OwningList&&) = default;
  OwningList(const OwningList&) = delete;
  OwningList& operator=(const OwningList&) = delete;
};
using ResultList = OwningList<ResultEntry>;

// returns the first error code or 0 if all succeeded
inline int check_for_errors(const ResultList& results) {
  for (auto& e : results) {
    if (e.result < 0) {
      return e.result;
    }
  }
  return 0;
}

// interface to submit async librados operations and wait on their completions.
// each call returns a list of results from prior completions
class Aio {
 public:
  virtual ~Aio() {}

  virtual ResultList submit(rgw_rados_ref& ref, const rgw_raw_obj& obj,
                            librados::ObjectReadOperation *op,
                            bufferlist *data, uint64_t cost) = 0;

  virtual ResultList submit(rgw_rados_ref& ref, const rgw_raw_obj& obj,
                            librados::ObjectWriteOperation *op,
                            uint64_t cost) = 0;

  // poll for any ready completions without waiting
  virtual ResultList poll() = 0;

  // return any ready completions. if there are none, wait for the next
  virtual ResultList wait() = 0;

  // wait for all outstanding completions and return their results
  virtual ResultList drain() = 0;
};

} // namespace rgw::putobj
