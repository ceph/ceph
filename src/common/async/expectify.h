// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/system/error_code.hpp>

#include <memory>
#include <utility>
#include <type_traits>

#include "include/expected.hpp"

namespace ceph::async {
/// Wrapper type to force return of tl::expected
template<typename T>
struct expectify {
  T t;

  expectify(T t)
    : t(std::move(t)) {}
};
}

namespace boost::asio {
template<typename CompletionToken, typename ReturnType, typename... Args>
class async_result<ceph::async::expectify<CompletionToken>, ReturnType(Args...)>
{
  std::unique_ptr<boost::system::error_code> ec;
  async_result<CompletionToken, void(Args...)> r;

public:
  class completion_handler_type {
    friend async_result;

    std::unique_ptr<boost::system::error_code> ec =
      std::make_unique<boost::system::error_code>();
    typename async_result<CompletionToken,
			  void(Args...)>::completion_handler_type h;


  public:
    completion_handler_type(ceph::async::expectify<CompletionToken> e)
      : h(e.t[*ec]) {}

    template<typename... Args2>
    void operator ()(Args2&&... args) noexcept {
      h(std::forward<Args2>(args)...);
    }
  };
  using return_type =
    tl::expected<typename async_result<CompletionToken,
				       void(Args...)>::return_type,
		 boost::system::error_code>;
public:

  explicit async_result(completion_handler_type& h)
    : ec(std::move(h.ec)), r(h.h) {}

  return_type get() {
    if constexpr (std::is_void_v<decltype(r.get())>) {
      r.get();
      if (*ec)
	return tl::unexpected(*ec);
      else
	return {};
    } else {
      auto rv = r.get();
      if (*ec)
	return tl::unexpected(*ec);
      else
	return rv;
    }
  }
};
}
