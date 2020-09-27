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

namespace rgw {

namespace {

void cb(librados::completion_t, void* arg);

struct state {
  Aio* aio;
  librados::AioCompletion* c;

  state(Aio* aio, AioResult& r)
    : aio(aio),
      c(librados::Rados::aio_create_completion(&r, &cb)) {}
};

struct cache_state {
  Aio* aio;
  D3nL1CacheRequest* c;

  cache_state(Aio* aio, AioResult& r)
    : aio(aio) {}

  int submit_libaio_op(D3nL1CacheRequest* cc) {
    int ret = 0;
    if((ret = ::aio_read(cc->paiocb)) != 0) {
      return ret;
    }
    return ret;
  }
};

void cb(librados::completion_t, void* arg) {
  static_assert(sizeof(AioResult::user_data) >= sizeof(state));
  static_assert(std::is_trivially_destructible_v<state>);
  auto& r = *(static_cast<AioResult*>(arg));
  auto s = reinterpret_cast<state*>(&r.user_data);
  r.result = s->c->get_return_value();
  s->c->release();
  s->aio->put(r);
}

void cache_aio_cb(sigval_t sigval) {
  D3nL1CacheRequest* c = static_cast<D3nL1CacheRequest*>(sigval.sival_ptr);
  int status = c->status();
  if (status == ECANCELED) {
    c->r->result = -1;
    c->aio->put(*(c->r));
    return;
  } else if (status == 0) {
    c->finish();
    c->r->result = 0;
    c->aio->put(*(c->r));
  }
}

template <typename Op>
Aio::OpFunc aio_abstract(Op&& op) {
  return [op = std::move(op)] (Aio* aio, AioResult& r) mutable {
      constexpr bool read = std::is_same_v<std::decay_t<Op>, librados::ObjectReadOperation>;
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

struct Handler {
  Aio* throttle = nullptr;
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
Aio::OpFunc aio_abstract(Op&& op, boost::asio::io_context& context,
                         spawn::yield_context yield) {
  return [op = std::move(op), &context, yield] (Aio* aio, AioResult& r) mutable {
      // arrange for the completion Handler to run on the yield_context's strand
      // executor so it can safely call back into Aio without locking
      using namespace boost::asio;
      async_completion<spawn::yield_context, void()> init(yield);
      auto ex = get_associated_executor(init.completion_handler);

      auto& ref = r.obj.get_ref();
      librados::async_operate(context, ref.pool.ioctx(), ref.obj.oid, &op, 0,
                              bind_executor(ex, Handler{aio, r}));
    };
}


template <typename Op>
Aio::OpFunc cache_aio_abstract(Op&& op, off_t obj_ofs, off_t read_ofs, off_t read_len, std::string& location) {
  return [op = std::move(op), obj_ofs, read_ofs, read_len, location] (Aio* aio, AioResult& r) mutable{
    auto& ref = r.obj.get_ref();
    auto cs = new(&r.user_data) cache_state(aio, r);
    cs->c = new D3nL1CacheRequest();
    if (g_conf()->rgw_d3n_l1_libaio_read) {
      cs->c->prepare_libaio_op(ref.obj.oid, &r.data, read_len, obj_ofs, read_ofs, location, cache_aio_cb, aio, &r);
      int ret = cs->submit_libaio_op(cs->c);
      if(ret < 0) {
        r.result = -1;
        cs->aio->put(r);
      }
    } else {
      int ret = cs->c->execute_io_op(ref.obj.oid, &r.data, read_len, obj_ofs, read_ofs, location, cache_aio_cb, aio, &r);
      if(ret < 0) {
        r.result = -1;
        cs->aio->put(r);
      }
    }
  };
}


template <typename Op>
Aio::OpFunc aio_abstract(Op&& op, optional_yield y) {
  static_assert(std::is_base_of_v<librados::ObjectOperation, std::decay_t<Op>>);
  static_assert(!std::is_lvalue_reference_v<Op>);
  static_assert(!std::is_const_v<Op>);
  if (y) {
    return aio_abstract(std::forward<Op>(op), y.get_io_context(),
                        y.get_yield_context());
  }
  return aio_abstract(std::forward<Op>(op));
}

template <typename Op>
Aio::OpFunc cache_aio_abstract(Op&& op, optional_yield y, off_t obj_ofs,
                               off_t read_ofs, off_t read_len, std::string& location) {
  static_assert(std::is_base_of_v<librados::ObjectOperation, std::decay_t<Op>>);
  static_assert(!std::is_lvalue_reference_v<Op>);
  static_assert(!std::is_const_v<Op>);

  return cache_aio_abstract(std::forward<Op>(op), obj_ofs, read_ofs, read_len, location);
}

} // anonymous namespace

Aio::OpFunc Aio::librados_op(librados::ObjectReadOperation&& op,
                             optional_yield y) {
  return aio_abstract(std::move(op), y);
}
Aio::OpFunc Aio::librados_op(librados::ObjectWriteOperation&& op,
                             optional_yield y) {
  return aio_abstract(std::move(op), y);
}

Aio::OpFunc Aio::cache_op(librados::ObjectReadOperation&& op, optional_yield y,
                          off_t obj_ofs, off_t read_ofs, off_t read_len, std::string& location) {
  return cache_aio_abstract(std::move(op), y, obj_ofs, read_ofs, read_len, location);
}

} // namespace rgw
