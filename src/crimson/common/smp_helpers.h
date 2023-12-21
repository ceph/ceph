// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <concepts>
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>

#include <seastar/core/shared_future.hh>
#include <seastar/core/smp.hh>

#include "common/likely.h"
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

enum class crosscore_type_t {
  ONE,   // from 1 to 1 core
  ONE_N, // from 1 to n cores
  N_ONE, // from n to 1 core
};

/**
 * smp_crosscore_ordering_t
 *
 * To preserve the event order from source to target core(s).
 */
template <crosscore_type_t CTypeValue>
class smp_crosscore_ordering_t {
  static constexpr bool IS_ONE = (CTypeValue == crosscore_type_t::ONE);
  static constexpr bool IS_ONE_N = (CTypeValue == crosscore_type_t::ONE_N);
  static constexpr bool IS_N_ONE = (CTypeValue == crosscore_type_t::N_ONE);
  static_assert(IS_ONE || IS_ONE_N || IS_N_ONE);

public:
  using seq_t = uint64_t;

  smp_crosscore_ordering_t() requires IS_ONE
    : out_seqs(0) { }

  smp_crosscore_ordering_t() requires (!IS_ONE)
    : out_seqs(seastar::smp::count, 0),
      in_controls(seastar::smp::count) {}

  ~smp_crosscore_ordering_t() = default;

  /*
   * Called by the original core to get the ordering sequence
   */

  seq_t prepare_submit() requires IS_ONE {
    return do_prepare_submit(out_seqs);
  }

  seq_t prepare_submit(core_id_t target_core) requires IS_ONE_N {
    return do_prepare_submit(out_seqs[target_core]);
  }

  seq_t prepare_submit() requires IS_N_ONE {
    return do_prepare_submit(out_seqs[seastar::this_shard_id()]);
  }

  /*
   * Called by the target core to preserve the ordering
   */

  seq_t get_in_seq() const requires IS_ONE {
    return in_controls.seq;
  }

  seq_t get_in_seq() const requires IS_ONE_N {
    return in_controls[seastar::this_shard_id()].seq;
  }

  seq_t get_in_seq(core_id_t source_core) const requires IS_N_ONE {
    return in_controls[source_core].seq;
  }

  bool proceed_or_wait(seq_t seq) requires IS_ONE {
    return in_controls.proceed_or_wait(seq);
  }

  bool proceed_or_wait(seq_t seq) requires IS_ONE_N {
    return in_controls[seastar::this_shard_id()].proceed_or_wait(seq);
  }

  bool proceed_or_wait(seq_t seq, core_id_t source_core) requires IS_N_ONE {
    return in_controls[source_core].proceed_or_wait(seq);
  }

  seastar::future<> wait(seq_t seq) requires IS_ONE {
    return in_controls.wait(seq);
  }

  seastar::future<> wait(seq_t seq) requires IS_ONE_N {
    return in_controls[seastar::this_shard_id()].wait(seq);
  }

  seastar::future<> wait(seq_t seq, core_id_t source_core) requires IS_N_ONE {
    return in_controls[source_core].wait(seq);
  }

  void reset_wait() requires IS_N_ONE {
    for (auto &in_control : in_controls) {
      in_control.reset_wait();
    }
  }

private:
  struct in_control_t {
    seq_t seq = 0;
    std::optional<seastar::shared_promise<>> pr_wait;

    bool proceed_or_wait(seq_t in_seq) {
      if (in_seq == seq + 1) {
        ++seq;
        reset_wait();
        return true;
      } else {
        return false;
      }
    }

    seastar::future<> wait(seq_t in_seq) {
      assert(in_seq != seq + 1);
      if (!pr_wait.has_value()) {
        pr_wait = seastar::shared_promise<>();
      }
      return pr_wait->get_shared_future();
    }

    void reset_wait() {
      if (unlikely(pr_wait.has_value())) {
        pr_wait->set_value();
        pr_wait = std::nullopt;
      }
    }
  };

  seq_t do_prepare_submit(seq_t &out_seq) {
    return ++out_seq;
  }

  std::conditional_t<
    IS_ONE,
    seq_t, std::vector<seq_t>
  > out_seqs;

  std::conditional_t<
    IS_ONE,
    in_control_t, std::vector<in_control_t>
  > in_controls;
};

} // namespace crimson
