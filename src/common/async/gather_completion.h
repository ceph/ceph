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
 * Foundation. See file COPYING.
 *
 */


#ifndef CEPH_COMMON_ASYNC_GATHER_COMPLETION_H
#define CEPH_COMMON_ASYNC_GATHER_COMPLETION_H

#include <cassert>
#include <type_traits>

#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/handler_invoke_hook.hpp>
#include <boost/asio/is_executor.hpp>
#include <boost/asio/strand.hpp>

namespace ceph::async {
template<typename...>
class gather_completion;

template<typename Executor, typename Completion, typename Accumulate,
	 typename... MinorArgs, typename... MajorArgs>
class gather_completion<Executor, Completion, Accumulate, void(MinorArgs...),
			void(MajorArgs...)> {
  static_assert(boost::asio::is_executor<Executor>::value);
  boost::asio::strand<Executor> strand;
  boost::asio::async_completion<Completion,
				void(MajorArgs...)> completion_major;
  Accumulate accumulate;
  static_assert(std::is_convertible_v<std::invoke_result_t<
		                        Accumulate, MajorArgs..., MinorArgs...>,
		  std::tuple<MajorArgs...>>);
  std::tuple<MajorArgs...> seed;
  int completions = 0;
  bool completed = false;

public:

  gather_completion(Executor ex, Completion completion_major,
		    Accumulate accumulate, std::tuple<MajorArgs...> seed)
    : strand(ex), completion_major(completion_major),
      accumulate(accumulate), seed(seed) {}

  gather_completion(const gather_completion&) = delete;
  gather_completion& operator =(const gather_completion&) = delete;
  gather_completion(gather_completion&&) = delete;
  gather_completion& operator =(gather_completion&&) = delete;

  auto completion_minor() {
    boost::asio::dispatch(strand, [this] {
				    assert(!completed);
				    ++completions; });

    return boost::asio::bind_executor(
      strand,
      [this](MinorArgs... args) {
	--completions;
	seed = std::apply(accumulate,
			  std::tuple_cat(std::move(seed),
					 std::forward_as_tuple(args...)));
	if (completions == 0 && completed)
	  std::apply(completion_major.completion_handler, seed);
      });
  }

  auto complete() {
    boost::asio::dispatch(
      strand,
      [this]() {
	completed = true;
	if (completions == 0)
	  std::apply(completion_major.completion_handler, seed);
      });
    if constexpr (std::is_void_v<decltype(completion_major.result.get())>) {
	completion_major.result.get();
    } else {
      return std::move(completion_major.result.get());
    }
  }
};

template<typename Executor, typename Completion, typename Accumulate,
	 typename... MinorArgs, typename MajorArg>
class gather_completion<Executor, Completion, Accumulate, void(MinorArgs...),
			void(MajorArg)> {
  static_assert(boost::asio::is_executor<Executor>::value);
  boost::asio::strand<Executor> strand;
  boost::asio::async_completion<Completion,
				void(MajorArg)> completion_major;
  Accumulate accumulate;
  static_assert(std::is_convertible_v<std::invoke_result_t<
		                        Accumulate, MajorArg, MinorArgs...>,
		  MajorArg>);
  MajorArg seed;
  int completions = 0;
  bool completed = false;

public:

  gather_completion(Executor ex, Completion completion_major,
		    Accumulate accumulate, MajorArg seed)
    : strand(ex), completion_major(completion_major),
      accumulate(accumulate), seed(seed) {}

  gather_completion(const gather_completion&) = delete;
  gather_completion& operator =(const gather_completion&) = delete;
  gather_completion(gather_completion&&) = delete;
  gather_completion& operator =(gather_completion&&) = delete;

  auto completion_minor() {
    boost::asio::dispatch(strand, [this] {
				    assert(!completed);
				    ++completions; });

    return boost::asio::bind_executor(
      strand,
      [this](MinorArgs... args) {
	--completions;
	seed = accumulate(std::move(seed), std::move(args)...);
	if (completions == 0 && completed)
	  completion_major.completion_handler(std::move(seed));
      });
  }

  auto complete() {
    boost::asio::dispatch(
      strand,
      [this]() {
	completed = true;
	if (completions == 0)
	  completion_major.completion_handler(std::move(seed));
      });
    if constexpr (std::is_void_v<decltype(completion_major.result.get())>) {
	completion_major.result.get();
    } else {
      return std::move(completion_major.result.get());
    }
  }
};

template<typename Executor, typename Completion, typename Accumulate,
	 typename... MinorArgs>
class gather_completion<Executor, Completion, Accumulate, void(MinorArgs...),
			void()> {
  static_assert(boost::asio::is_executor<Executor>::value);
  boost::asio::strand<Executor> strand;
  boost::asio::async_completion<Completion, void()> completion_major;
  Accumulate accumulate;
  static_assert(std::is_void_v<std::invoke_result_t<Accumulate, MinorArgs...>>);
  int completions = 0;
  bool completed = false;

public:

  gather_completion(Executor ex, Completion completion_major,
		    Accumulate accumulate)
    : strand(ex), completion_major(completion_major),
      accumulate(accumulate) {}

  gather_completion(const gather_completion&) = delete;
  gather_completion& operator =(const gather_completion&) = delete;
  gather_completion(gather_completion&&) = delete;
  gather_completion& operator =(gather_completion&&) = delete;

  auto completion_minor() {
    boost::asio::dispatch(strand, [this] {
				    assert(!completed);
				    ++completions; });

    return boost::asio::bind_executor(
      strand,
      [this](MinorArgs... args) {
	--completions;
	accumulate(std::move(args)...);
	if (completions == 0 && completed)
	  completion_major.completion_handler();
      });
  }

  auto complete() {
    boost::asio::dispatch(
      strand,
      [this]() {
	completed = true;
	if (completions == 0)
	  completion_major.completion_handler();
      });
    if constexpr (std::is_void_v<decltype(completion_major.result.get())>) {
	completion_major.result.get();
    } else {
      return std::move(completion_major.result.get());
    }
  }
};

template<typename Executor, typename Completion>
class gather_completion<Executor, Completion> {
  static_assert(boost::asio::is_executor<Executor>::value);
  boost::asio::strand<Executor> strand;
  boost::asio::async_completion<Completion, void()> completion_major;
  int completions = 0;
  bool completed = false;

public:

  gather_completion(Executor ex, Completion completion_major)
    : strand(ex), completion_major(completion_major) {}

  gather_completion(const gather_completion&) = delete;
  gather_completion& operator =(const gather_completion&) = delete;
  gather_completion(gather_completion&&) = delete;
  gather_completion& operator =(gather_completion&&) = delete;

  auto completion_minor() {
    assert(!completed);
    boost::asio::dispatch(strand, [this] { ++completions; });
    return boost::asio::bind_executor(
      strand,
      [this] {
	--completions;
	if (completions == 0 && completed)
	  completion_major.completion_handler();
      });
  }

  auto complete() {
    boost::asio::dispatch(
      strand,
      [this]() {
	completed = true;
	if (completions == 0)
	  completion_major.completion_handler();
      });
    if constexpr (std::is_void_v<decltype(completion_major.result.get())>) {
	completion_major.result.get();
    } else {
      return std::move(completion_major.result.get());
    }
  }
};

template<typename A, typename B = A>
class AnyTrue {
public:
  A operator ()(A&& a, B&& b) {
    if (bool(b))
      return std::move(b);

    return std::move(a);
  }
};

// For function pointers

template<typename Executor, typename Completion,
	 typename... MinorArgs, typename... MajorArgs>
auto make_gather_completion(
  Executor e, Completion c,
  std::tuple<MajorArgs...> (*a)(MajorArgs..., MinorArgs...),
  std::tuple<MajorArgs...> s = std::tuple<MajorArgs...>{}) {
  return gather_completion<Executor, Completion,
			   std::tuple<MajorArgs...> (*)(MajorArgs...,
							MinorArgs...),
			   void(MinorArgs...),
			   void(MajorArgs...)>(e, c, a, s);
}

template<typename Executor, typename Completion,
	 typename... MinorArgs, typename MajorArg>
auto make_gather_completion(Executor e, Completion c,
			    MajorArg (*a)(MajorArg, MinorArgs...),
			    MajorArg s = MajorArg{}) {
  return gather_completion<Executor, Completion,
			   MajorArg (*)(MajorArg, MinorArgs...),
			   void(MinorArgs...),
			   void(MajorArg)>(e, c, a, s);
}

template<typename Executor, typename Completion,
	 typename... MinorArgs>
auto make_gather_completion(Executor e, Completion c,
			    void (*a)(MinorArgs...)) {
  return gather_completion<Executor, Completion,
			   void (*)(MinorArgs...),
			   void(MinorArgs...), void()>(e, c, a);
}

// For functions other than pointers

template<typename MnA, typename Executor, typename Completion, typename Accumulate,
	 typename... MajorArgs>
auto make_gather_completion(
  Executor e, Completion c, Accumulate a, std::tuple<MajorArgs...> s) {
  return gather_completion<Executor, Completion, Accumulate,
			   MnA, void(MajorArgs...)>(e, c, a, s);
}

template<typename MnA, typename Executor, typename Completion,
	 typename Accumulate, typename MajorArg>
auto make_gather_completion(Executor e, Completion c, Accumulate a,
			    MajorArg s) {
  return gather_completion<Executor, Completion, Accumulate,
			   MnA, void(MajorArg)>(e, c, a, s);
}

template<typename MnA, typename Executor, typename Completion,
	 typename Accumulate>
auto make_gather_completion(Executor e, Completion c,
			    Accumulate a) {
  return gather_completion<Executor, Completion, Accumulate,
			   MnA, void()>(e, c, a);
}

// For the case with no accumulator, no arguments

template<typename Executor, typename Completion>
auto make_gather_completion(Executor e, Completion c) {
  return gather_completion<Executor, Completion>(e, c);
}
}

#endif // CEPH_COMMON_ASYNC_GATHER_COMPLETION_H
