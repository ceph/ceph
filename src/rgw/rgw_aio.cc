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

#include <type_traits>
#include "include/rados/librados.hpp"

#include "rgw_aio.h"

namespace rgw {
namespace {
void cb(librados::completion_t, void* arg);
}
struct state {
  Aio* aio;
  librados::AioCompletion* c;

  state(Aio* aio, AioResult& r)
    : aio(aio),
      c(librados::Rados::aio_create_completion(&r, nullptr, &cb)) {}
};

namespace {
void cb(librados::completion_t, void* arg) {
  static_assert(sizeof(AioResult::user_data) >= sizeof(state));
  static_assert(std::is_trivially_destructible_v<state>);
  auto& r = *(static_cast<AioResult*>(arg));
  auto s = reinterpret_cast<state*>(&r.user_data);
  r.result = s->c->get_return_value();
  s->c->release();
  s->aio->put(r);
}

template<typename T,
	 bool read = std::is_same_v<std::decay_t<T>,
				    librados::ObjectReadOperation>,
	 typename = std::enable_if_t<std::is_base_of_v<librados::ObjectOperation,
						       std::decay_t<T>> &&
				     !std::is_lvalue_reference_v<T> &&
				     !std::is_const_v<T>>>
Aio::OpFunc aio_abstract(T&& op) {
  return [op = std::move(op)](Aio* aio, AioResult& r) mutable {
	   auto s = new (&r.user_data) state(aio, r);
	   if constexpr (read) {
	     r.result = r.obj.aio_operate(s->c, &op, &r.data);
	   } else {
	     r.result = r.obj.aio_operate(s->c, &op);
	   }
	   if (r.result < 0) {
	     s->c->release();
	     aio->put(r);
	   }
	 };
}
}

Aio::OpFunc Aio::librados_op(librados::ObjectReadOperation&& op) {
  return aio_abstract(std::move(op));
}
Aio::OpFunc Aio::librados_op(librados::ObjectWriteOperation&& op) {
  return aio_abstract(std::move(op));
}
}
