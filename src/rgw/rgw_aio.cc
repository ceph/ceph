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
#include "include/rados/librados.hpp"
#include "librados/librados_asio.h"

#include "rgw_aio.h"
#include "rgw_d3n_cacherequest.h"
#include "rgw_cache_driver.h"

namespace rgw {

namespace {

void cb(librados::completion_t, void* arg);

struct state {
  Aio* aio;
  librados::IoCtx ctx;
  librados::AioCompletion* c;

  state(Aio* aio, librados::IoCtx ctx, AioResult& r)
    : aio(aio), ctx(std::move(ctx)),
    // coverity[ctor_dtor_leak:SUPPRESS]
      c(librados::Rados::aio_create_completion(&r, &cb)) {}
};

void cb(librados::completion_t, void* arg) {
  static_assert(sizeof(AioResult::user_data) >= sizeof(state));
  auto& r = *(static_cast<AioResult*>(arg));
  auto s = reinterpret_cast<state*>(&r.user_data);
  r.result = s->c->get_return_value();
  s->c->release();
  Aio* aio = s->aio;
  // manually destroy the state that was constructed with placement new
  s->~state();
  aio->put(r);
}

template <typename Op>
Aio::OpFunc aio_abstract(librados::IoCtx ctx, Op&& op, jspan_context* trace_ctx = nullptr) {
  return [ctx = std::move(ctx), op = std::move(op), trace_ctx] (Aio* aio, AioResult& r) mutable {
      constexpr bool read = std::is_same_v<std::decay_t<Op>, librados::ObjectReadOperation>;
      // use placement new to construct the rados state inside of user_data
      auto s = new (&r.user_data) state(aio, ctx, r);
      if constexpr (read) {
        (void)trace_ctx; // suppress unused trace_ctx warning. until we will support the read op trace
        r.result = ctx.aio_operate(r.obj.oid, s->c, &op, &r.data);
      } else {
        r.result = ctx.aio_operate(r.obj.oid, s->c, &op, 0, trace_ctx);
      }
      if (r.result < 0) {
        // cb() won't be called, so release everything here
        s->c->release();
        aio->put(r);
        s->~state();
      }
    };
}

struct Handler {
  Aio* throttle = nullptr;
  librados::IoCtx ctx;
  AioResult& r;
  // write callback
  void operator()(boost::system::error_code ec) const {
    r.result = -ec.value();
    throttle->put(r);
  }
  // read callback
  void operator()(boost::system::error_code ec, bufferlist bl) const {
    r.result = -ec.value();
    r.data = std::move(bl);
    throttle->put(r);
  }
};

template <typename Op>
Aio::OpFunc aio_abstract(librados::IoCtx ctx, Op&& op,
                         boost::asio::yield_context yield,
                         jspan_context* trace_ctx) {
  return [ctx = std::move(ctx), op = std::move(op), yield, trace_ctx] (Aio* aio, AioResult& r) mutable {
      // arrange for the completion Handler to run on the yield_context's strand
      // executor so it can safely call back into Aio without locking
      auto ex = yield.get_executor();

      librados::async_operate(yield, ctx, r.obj.oid, &op, 0, trace_ctx,
                              bind_executor(ex, Handler{aio, ctx, r}));
    };
}


Aio::OpFunc d3n_cache_aio_abstract(const DoutPrefixProvider *dpp, optional_yield y, off_t read_ofs, off_t read_len, std::string& cache_location) {
  return [dpp, y, read_ofs, read_len, cache_location] (Aio* aio, AioResult& r) mutable {
    // d3n data cache requires yield context (rgw_beast_enable_async=true)
    ceph_assert(y);
    auto c = std::make_unique<D3nL1CacheRequest>();
    lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: d3n_cache_aio_abstract(): libaio Read From Cache, oid=" << r.obj.oid << dendl;
    c->file_aio_read_abstract(dpp, y.get_yield_context(), cache_location, read_ofs, read_len, aio, r);
  };
}


template <typename Op>
Aio::OpFunc aio_abstract(librados::IoCtx ctx, Op&& op, optional_yield y, jspan_context *trace_ctx = nullptr) {
  static_assert(std::is_base_of_v<librados::ObjectOperation, std::decay_t<Op>>);
  static_assert(!std::is_lvalue_reference_v<Op>);
  static_assert(!std::is_const_v<Op>);
  if (y) {
    return aio_abstract(std::move(ctx), std::forward<Op>(op),
                        y.get_yield_context(), trace_ctx);
  }
  return aio_abstract(std::move(ctx), std::forward<Op>(op), trace_ctx);
}

} // anonymous namespace

Aio::OpFunc Aio::librados_op(librados::IoCtx ctx,
                             librados::ObjectReadOperation&& op,
                             optional_yield y) {
  return aio_abstract(std::move(ctx), std::move(op), y);
}
Aio::OpFunc Aio::librados_op(librados::IoCtx ctx,
                             librados::ObjectWriteOperation&& op,
                             optional_yield y, jspan_context *trace_ctx) {
  return aio_abstract(std::move(ctx), std::move(op), y, trace_ctx);
}

Aio::OpFunc Aio::d3n_cache_op(const DoutPrefixProvider *dpp, optional_yield y,
                              off_t read_ofs, off_t read_len, std::string& cache_location) {
  return d3n_cache_aio_abstract(dpp, y, read_ofs, read_len, cache_location);
}

} // namespace rgw
