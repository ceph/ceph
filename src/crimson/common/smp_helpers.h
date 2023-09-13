// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>

#include <seastar/core/smp.hh>

#include "crimson/common/errorator.h"
#include "crimson/common/utility.h"

namespace crimson {

using core_id_t = seastar::shard_id;
static constexpr core_id_t NULL_CORE = std::numeric_limits<core_id_t>::max();

auto submit_to(core_id_t core, auto &&f) {
  using ret_type = decltype(f());
  if constexpr (is_errorated_future_v<ret_type>) {
    auto ret = seastar::smp::submit_to(
      core,
      [f=std::move(f)]() mutable {
	return f().to_base();
      });
    return ret_type(std::move(ret));
  } else {
    return seastar::smp::submit_to(core, std::move(f));
  }
}

template <typename Obj, typename Method, typename... Args>
auto proxy_method_on_core(
  core_id_t core, Obj &obj, Method method, Args&&... args) {
  return crimson::submit_to(
    core,
    [&obj, method,
     arg_tuple=std::make_tuple(std::forward<Args>(args)...)]() mutable {
      return apply_method_to_tuple(obj, method, std::move(arg_tuple));
    });
}

/**
 * reactor_map_seq
 *
 * Invokes f on each reactor sequentially, Caller may assume that
 * f will not be invoked concurrently on multiple cores.
 */
template <typename F>
auto reactor_map_seq(F &&f) {
  using ret_type = decltype(f());
  if constexpr (is_errorated_future_v<ret_type>) {
    auto ret = crimson::do_for_each(
      seastar::smp::all_cpus().begin(),
      seastar::smp::all_cpus().end(),
      [f=std::move(f)](auto core) mutable {
	return seastar::smp::submit_to(
	  core,
	  [&f] {
	    return std::invoke(f);
	  });
      });
    return ret_type(ret);
  } else {
    return seastar::do_for_each(
      seastar::smp::all_cpus().begin(),
      seastar::smp::all_cpus().end(),
      [f=std::move(f)](auto core) mutable {
	return seastar::smp::submit_to(
	  core,
	  [&f] {
	    return std::invoke(f);
	  });
      });
  }
}

/**
 * sharded_map_seq
 *
 * Invokes f on each shard of t sequentially.  Caller may assume that
 * f will not be invoked concurrently on multiple cores.
 */
template <typename T, typename F>
auto sharded_map_seq(T &t, F &&f) {
  return reactor_map_seq(
    [&t, f=std::forward<F>(f)]() mutable {
      return std::invoke(f, t.local());
    });
}

}
