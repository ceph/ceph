// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#pragma once

#include <boost/asio/async_result.hpp>
#include <boost/system/error_code.hpp>

#include "include/neorados/RADOS.hpp"

#include "include/buffer.h"
#include "include/encoding.h"

#include "common/async/bind_like.h"

#include "common/allocate_unique.h"

namespace neorados::cls {
namespace ba = boost::asio;
namespace bs = boost::system;
namespace ca = ceph::async;
namespace cb = ceph::buffer;
namespace nr = neorados;

namespace detail {
template<typename... Args>
struct is_tuple;

template<typename... Args>
struct is_tuple<std::tuple<Args...>> : std::true_type {};

template<typename T>
struct is_tuple<T> : public std::false_type {};

template<typename T>
inline constexpr bool is_tuple_v = is_tuple<T>::value;

template<typename T>
struct ExecDecodeCB {
  bs::error_code ec;
  T result;
  void operator()(bs::error_code e, const cb::list& r) {
    if (e) {
      ec = e;
      return;
    }
    try {
      auto p = r.begin();
      using ceph::decode;
      decode(result, p);
    } catch (const cb::error& err) {
      ec = err.code();
    }
  }
};

template<typename Handler, typename T, typename F>
class Disassembler {
  Handler handler;
  using allocator_type = boost::asio::associated_allocator_t<Handler>;
  using decoder_type = ExecDecodeCB<T>;
  using decoder_ptr = ceph::allocated_unique_ptr<decoder_type, allocator_type>;
  decoder_ptr decoder;
  F f;
public:
  Disassembler(Handler&& handler, decoder_ptr&& decoder, F&& f)
    : handler(std::move(handler)), decoder(std::move(decoder)),
      f(std::move(f)) {}

  void operator ()(bs::error_code ec) {
    if (!ec) {
      ec = decoder->ec;
    }
    auto reply = std::move(decoder->result);
    decoder.reset(); // free handler-allocated memory before dispatching

    if constexpr (is_tuple_v<std::invoke_result_t<F, T&&>>) {
      std::apply(std::move(handler),
                 std::tuple_cat(std::make_tuple(ec),
                                std::move(f)(std::move(reply))));
    } else {
      std::move(handler)(ec, std::move(f)(std::move(reply)));
    }
  }
};

template<typename... Args>
struct ExecDecSig;

template<typename T>
struct ExecDecSig<T> {
  using type = void(bs::error_code, T);
};

template<typename... Args>
struct ExecDecSig<std::tuple<Args...>> {
  using type = void(bs::error_code, Args...);
};

template<typename F, typename Rep>
using exec_dec_sig = typename ExecDecSig<std::invoke_result_t<F, Rep&&>>::type;
}


template<typename Rep, typename Req, typename F, typename CT>
auto exec(nr::RADOS& r, const nr::Object& o, const nr::IOContext& ioc,
          std::string_view cls, std::string_view method,
          const Req& req, F&& f, CT&& ct)
{
  ba::async_completion<CT, detail::exec_dec_sig<F, Rep>> init(ct);
  cb::list in;
  encode(req, in);
  ReadOp op;
  auto e = ba::get_associated_executor(init.completion_handler,
                                       r.get_executor());
  auto a = ba::get_associated_allocator(init.completion_handler);
  auto reply = ceph::allocate_unique<
    detail::ExecDecodeCB<Rep>>(a);

  op.exec(cls, method, in, std::ref(*reply));
  r.execute(o, ioc, std::move(op), nullptr,
            ca::bind_ea(e, a,
                        detail::Disassembler(std::move(init.completion_handler),
                                             std::move(reply),
                                             std::forward<F>(f))));
  return init.result.get();
}

template<typename Rep, typename F, typename CT>
auto exec(nr::RADOS& r, const nr::Object& o, const nr::IOContext& ioc,
          std::string_view cls, std::string_view method, F&& f, CT&& ct)
{
  ba::async_completion<CT, detail::exec_dec_sig<F, Rep>> init(ct);
  ReadOp op;
  auto e = ba::get_associated_executor(init.completion_handler,
                                       r.get_executor());
  auto a = ba::get_associated_allocator(init.completion_handler);
  auto reply = ceph::allocate_unique<
    detail::ExecDecodeCB<Rep>>(a);

  op.exec(cls, method, {}, std::ref(*reply));
  r.execute(o, ioc, std::move(op), nullptr,
            ca::bind_ea(e, a,
                        detail::Disassembler(std::move(init.completion_handler),
                                             std::move(reply),
                                             std::forward<F>(f))));
  return init.result.get();
}

template<typename Req, typename CT>
auto exec(nr::RADOS& r, const nr::Object& o, const nr::IOContext& ioc,
          std::string_view cls, std::string_view method,
          const Req& req, CT&& ct) {
  cb::list in;
  encode(req, in);
  WriteOp op;
  op.exec(cls, method, in);
  r.execute(o, ioc, std::move(op), std::forward<CT>(ct));
}
}
