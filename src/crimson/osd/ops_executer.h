// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <optional>
#include <type_traits>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "common/dout.h"
#include "crimson/net/Fwd.h"
#include "os/Transaction.h"
#include "osd/osd_types.h"
#include "osd/osd_internal_types.h"

#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osdmap_gate.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/exceptions.h"

#include "messages/MOSDOp.h"

class PGLSFilter;
class OSDOp;

namespace ceph::osd {
class OpsExecuter {
  // an operation can be divided into two stages: main and effect-exposing
  // one. The former is performed immediately on call to `do_osd_op()` while
  // the later on `submit_changes()` – after successfully processing main
  // stages of all involved operations. When any stage fails, none of all
  // scheduled effect-exposing stages will be executed.
  // when operation requires this division, `with_effect()` should be used.
  struct effect_t {
    virtual seastar::future<> execute() = 0;
    virtual ~effect_t() = default;
  };

  PGBackend::cached_os_t os;
  PG& pg;
  PGBackend& backend;
  Ref<MOSDOp> msg;
  ceph::os::Transaction txn;

  size_t num_read = 0;    ///< count read ops
  size_t num_write = 0;   ///< count update ops

  // this gizmo could be wrapped in std::optional for the sake of lazy
  // initialization. we don't need it for ops that doesn't have effect
  // TODO: verify the init overhead of chunked_fifo
  seastar::chunked_fifo<std::unique_ptr<effect_t>> op_effects;

  template <class Context, class MainFunc, class EffectFunc>
  auto with_effect(Context&& ctx, MainFunc&& main_func, EffectFunc&& effect_func);

  seastar::future<> do_op_call(class OSDOp& osd_op);

  template <class Func>
  auto do_const_op(Func&& f) {
    // TODO: pass backend as read-only
    return std::forward<Func>(f)(backend, std::as_const(*os));
  }

  template <class Func>
  auto do_read_op(Func&& f) {
    ++num_read;
    // TODO: pass backend as read-only
    return do_const_op(std::forward<Func>(f));
  }

  template <class Func>
  auto do_write_op(Func&& f) {
    ++num_write;
    return std::forward<Func>(f)(backend, *os, txn);
  }

  // PG operations are being provided with pg instead of os.
  template <class Func>
  auto do_pg_op(Func&& f) {
    return std::forward<Func>(f)(std::as_const(pg),
                                 std::as_const(msg->get_hobj().nspace));
  }

  seastar::future<> dont_do_legacy_op() {
    throw ceph::osd::operation_not_supported();
  }

public:
  OpsExecuter(PGBackend::cached_os_t os, PG& pg, Ref<MOSDOp> msg)
    : os(std::move(os)),
      pg(pg),
      backend(pg.get_backend()),
      msg(std::move(msg)) {
  }
  OpsExecuter(PG& pg, Ref<MOSDOp> msg)
    : OpsExecuter{PGBackend::cached_os_t{}, pg, std::move(msg)}
  {}

  seastar::future<> execute_osd_op(class OSDOp& osd_op);
  seastar::future<> execute_pg_op(class OSDOp& osd_op);

  template <typename Func>
  seastar::future<> submit_changes(Func&& f) &&;

  const auto& get_message() const {
    return *msg;
  }
};

template <class Context, class MainFunc, class EffectFunc>
auto OpsExecuter::with_effect(
  Context&& ctx,
  MainFunc&& main_func,
  EffectFunc&& effect_func)
{
  using context_t = std::decay_t<Context>;
  // the language offers implicit conversion to pointer-to-function for
  // lambda only when it's closureless
  static_assert(std::is_convertible_v<EffectFunc,
                                      seastar::future<> (*)(context_t&&)>,
                "with_effect function is not allowed to capture");
  struct task_t final : effect_t {
    context_t ctx;
    EffectFunc effect_func;

    task_t(Context&& ctx, EffectFunc&& effect_func)
       : ctx(std::move(ctx)), effect_func(std::move(effect_func)) {}
    seastar::future<> execute() final {
      return std::move(effect_func)(std::move(ctx));
    }
  };
  auto task =
    std::make_unique<task_t>(std::move(ctx), std::move(effect_func));
  auto& ctx_ref = task->ctx;
  op_effects.emplace_back(std::move(task));
  return std::forward<MainFunc>(main_func)(ctx_ref);
}

template <typename Func>
seastar::future<> OpsExecuter::submit_changes(Func&& f) && {
  return std::forward<Func>(f)(std::move(txn), std::move(os)).then(
    // NOTE: this lambda could be scheduled conditionally (if_then?)
    [this] {
      return seastar::do_until(
        [this] { return op_effects.empty(); },
        [this] {
          auto fut = op_effects.front()->execute();
          op_effects.pop_front();
          return fut;
        });
    });
}

} // namespace ceph::osd
