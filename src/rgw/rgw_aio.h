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
#include "services/svc_rados.h" // cant forward declare RGWSI_RADOS::Obj

namespace librados {
class ObjectReadOperation;
class ObjectWriteOperation;
}

namespace rgw {

struct AioResult {
  RGWSI_RADOS::Obj obj;
  uint64_t id = 0; // id allows caller to associate a result with its request
  bufferlist data; // result buffer for reads
  int result = 0;
};
struct AioResultEntry : AioResult, boost::intrusive::list_base_hook<> {
  virtual ~AioResultEntry() {}
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
using AioResultList = OwningList<AioResultEntry>;

// returns the first error code or 0 if all succeeded
inline int check_for_errors(const AioResultList& results) {
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

  virtual AioResultList submit(RGWSI_RADOS::Obj& obj,
                               librados::ObjectReadOperation *op,
                               uint64_t cost, uint64_t id) = 0;

  virtual AioResultList submit(RGWSI_RADOS::Obj& obj,
                               librados::ObjectWriteOperation *op,
                               uint64_t cost, uint64_t id) = 0;

  // poll for any ready completions without waiting
  virtual AioResultList poll() = 0;

  // return any ready completions. if there are none, wait for the next
  virtual AioResultList wait() = 0;

  // wait for all outstanding completions and return their results
  virtual AioResultList drain() = 0;
};

} // namespace rgw
