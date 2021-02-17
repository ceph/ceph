// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include "rgw_aio.h"

namespace ba = boost::asio;

namespace rgw {

namespace {

struct state {
  Aio* aio;
  AioResult& r;

  void operator ()(boost::system::error_code ec) {
    static_assert(sizeof(AioResult::user_data) >= sizeof(state));
    static_assert(std::is_trivially_destructible_v<state>);
    r.result = ec;
    aio->put(r);
  }

  state(Aio* aio, AioResult& r)
    : aio(aio), r(r) {}
};


template<typename Op>
Aio::OpFunc aio_abstract(Op&& op) {
  return [op = std::move(op)] (Aio* aio, AioResult& r) mutable {
      auto s = new (&r.user_data) state(aio, r);
      if constexpr (std::is_same_v<std::decay_t<Op>, nr::ReadOp>) {
	r.obj.operate(std::move(op), &r.data, std::ref(*s));
      } else {
	r.obj.operate(std::move(op), std::ref(*s));
      }
    };
}

struct Handler {
  Aio* throttle = nullptr;
  AioResult& r;
  // callback
  void operator()(boost::system::error_code ec) {
    r.result = ec;
    throttle->put(r);
  }
};

template<typename Op>
Aio::OpFunc aio_abstract(Op&& op, ba::io_context& context,
                         spawn::yield_context yield) {
  return [op = std::move(op), yield] (Aio* aio, AioResult& r) mutable {
      // arrange for the completion Handler to run on the yield_context's strand
      // executor so it can safely call back into Aio without locking
      using namespace boost::asio;
      async_completion<spawn::yield_context, void()> init(yield);
      auto ex = get_associated_executor(init.completion_handler);

      if constexpr (std::is_same_v<std::decay_t<Op>, nr::ReadOp>) {
	r.obj.operate(std::move(op), &r.data,
		      bind_executor(ex, Handler{aio, r}));
      } else {
	r.obj.operate(std::move(op),
		      bind_executor(ex, Handler{aio, r}));
      }
    };
}

template<typename Op>
Aio::OpFunc aio_abstract(Op&& op, optional_yield y) {
  static_assert(std::is_base_of_v<nr::Op, std::decay_t<Op>>);
  static_assert(!std::is_lvalue_reference_v<Op>);
  static_assert(!std::is_const_v<Op>);
  if (y) {
    return aio_abstract(std::move(op), y.get_io_context(),
                        y.get_yield_context());
  }
  return aio_abstract(std::move(op));
}

} // anonymous namespace

Aio::OpFunc Aio::rados_op(nr::ReadOp&& op,
			  optional_yield y) {
  return aio_abstract(std::move(op), y);
}
Aio::OpFunc Aio::rados_op(nr::WriteOp&& op,
			  optional_yield y) {
  return aio_abstract(std::move(op), y);
}

} // namespace rgw 
