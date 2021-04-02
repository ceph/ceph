// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
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
#include "crimson/osd/object_context.h"

#include "crimson/common/errorator.h"
#include "crimson/common/interruptible_future.h"
#include "crimson/common/type_helpers.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osdmap_gate.h"

#include "crimson/osd/pg_backend.h"
#include "crimson/osd/exceptions.h"

#include "messages/MOSDOp.h"

class PG;
class PGLSFilter;
class OSDOp;

namespace crimson::osd {

// OpsExecuter -- a class for executing ops targeting a certain object.
class OpsExecuter {
  using call_errorator = crimson::errorator<
    crimson::stateful_ec,
    crimson::ct_error::enoent,
    crimson::ct_error::invarg,
    crimson::ct_error::permission_denied,
    crimson::ct_error::operation_not_supported,
    crimson::ct_error::input_output_error,
    crimson::ct_error::value_too_large>;
  using read_errorator = PGBackend::read_errorator;
  using write_ertr = PGBackend::write_ertr;
  using get_attr_errorator = PGBackend::get_attr_errorator;
  using watch_errorator = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::invarg,
    crimson::ct_error::not_connected,
    crimson::ct_error::timed_out>;

  using call_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      IOInterruptCondition, call_errorator>;
  using read_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      IOInterruptCondition, read_errorator>;
  using write_iertr =
    ::crimson::interruptible::interruptible_errorator<
      IOInterruptCondition, write_ertr>;
  using get_attr_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      IOInterruptCondition, get_attr_errorator>;
  using watch_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      IOInterruptCondition, watch_errorator>;

  template <typename Errorator, typename T = void>
  using interruptible_errorated_future =
    ::crimson::interruptible::interruptible_errorated_future<
      IOInterruptCondition, Errorator, T>;
  using interruptor =
    ::crimson::interruptible::interruptor<IOInterruptCondition>;
  template <typename T = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      IOInterruptCondition, T>;

public:
  // because OpsExecuter is pretty heavy-weight object we want to ensure
  // it's not copied nor even moved by accident. Performance is the sole
  // reason for prohibiting that.
  OpsExecuter(OpsExecuter&&) = delete;
  OpsExecuter(const OpsExecuter&) = delete;

  using osd_op_errorator = crimson::compound_errorator_t<
    call_errorator,
    read_errorator,
    write_ertr,
    get_attr_errorator,
    watch_errorator,
    PGBackend::stat_errorator>;
  using osd_op_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      IOInterruptCondition, osd_op_errorator>;

private:
  // an operation can be divided into two stages: main and effect-exposing
  // one. The former is performed immediately on call to `do_osd_op()` while
  // the later on `submit_changes()` – after successfully processing main
  // stages of all involved operations. When any stage fails, none of all
  // scheduled effect-exposing stages will be executed.
  // when operation requires this division, some variant of `with_effect()`
  // should be used.
  struct effect_t {
    virtual osd_op_errorator::future<> execute() = 0;
    virtual ~effect_t() = default;
  };

  ObjectContextRef obc;
  const OpInfo& op_info;
  const pg_pool_t& pool_info;  // for the sake of the ObjClass API
  PGBackend& backend;
  const MOSDOp& msg;
  std::optional<osd_op_params_t> osd_op_params;
  bool user_modify = false;
  ceph::os::Transaction txn;

  size_t num_read = 0;    ///< count read ops
  size_t num_write = 0;   ///< count update ops

  // this gizmo could be wrapped in std::optional for the sake of lazy
  // initialization. we don't need it for ops that doesn't have effect
  // TODO: verify the init overhead of chunked_fifo
  seastar::chunked_fifo<std::unique_ptr<effect_t>> op_effects;

  template <class Context, class MainFunc, class EffectFunc>
  auto with_effect_on_obc(
    Context&& ctx,
    MainFunc&& main_func,
    EffectFunc&& effect_func);

  call_ierrorator::future<> do_op_call(class OSDOp& osd_op);
  watch_ierrorator::future<> do_op_watch(
    class OSDOp& osd_op,
    class ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_watch_subop_watch(
    class OSDOp& osd_op,
    class ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_watch_subop_reconnect(
    class OSDOp& osd_op,
    class ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_watch_subop_unwatch(
    class OSDOp& osd_op,
    class ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_watch_subop_ping(
    class OSDOp& osd_op,
    class ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_notify(
    class OSDOp& osd_op,
    const class ObjectState& os);
  watch_ierrorator::future<> do_op_notify_ack(
    class OSDOp& osd_op,
    const class ObjectState& os);

  const hobject_t &get_target() const {
    return obc->obs.oi.soid;
  }

  template <class Func>
  auto do_const_op(Func&& f) {
    // TODO: pass backend as read-only
    return std::forward<Func>(f)(backend, std::as_const(obc->obs));
  }

  template <class Func>
  auto do_read_op(Func&& f) {
    ++num_read;
    // TODO: pass backend as read-only
    return do_const_op(std::forward<Func>(f));
  }

  template <class Func>
  auto do_write_op(Func&& f, bool um) {
    ++num_write;
    if (!osd_op_params) {
      osd_op_params.emplace();
    }
    user_modify = um;
    return std::forward<Func>(f)(backend, obc->obs, txn);
  }

  decltype(auto) dont_do_legacy_op() {
    return crimson::ct_error::operation_not_supported::make();
  }

public:
  OpsExecuter(ObjectContextRef obc,
              const OpInfo& op_info,
              const pg_pool_t& pool_info,
              PGBackend& backend,
              const MOSDOp& msg)
    : obc(std::move(obc)),
      op_info(op_info),
      pool_info(pool_info),
      backend(backend),
      msg(msg) {
  }

  interruptible_errorated_future<osd_op_errorator>
  execute_op(class OSDOp& osd_op);

  template <typename MutFunc>
  osd_op_ierrorator::future<> flush_changes(MutFunc&& mut_func) &&;

  const auto& get_message() const {
    return msg;
  }

  size_t get_processed_rw_ops_num() const {
    return num_read + num_write;
  }

  uint32_t get_pool_stripe_width() const {
    return pool_info.get_stripe_width();
  }

  bool has_seen_write() const {
    return num_write > 0;
  }
};

template <class Context, class MainFunc, class EffectFunc>
auto OpsExecuter::with_effect_on_obc(
  Context&& ctx,
  MainFunc&& main_func,
  EffectFunc&& effect_func)
{
  using context_t = std::decay_t<Context>;
  // the language offers implicit conversion to pointer-to-function for
  // lambda only when it's closureless. We enforce this restriction due
  // the fact that `flush_changes()` std::moves many executer's parts.
  using allowed_effect_func_t =
    seastar::future<> (*)(context_t&&, ObjectContextRef);
  static_assert(std::is_convertible_v<EffectFunc, allowed_effect_func_t>,
                "with_effect function is not allowed to capture");
  struct task_t final : effect_t {
    context_t ctx;
    EffectFunc effect_func;
    ObjectContextRef obc;

    task_t(Context&& ctx, EffectFunc&& effect_func, ObjectContextRef obc)
       : ctx(std::move(ctx)),
         effect_func(std::move(effect_func)),
         obc(std::move(obc)) {
    }
    osd_op_errorator::future<> execute() final {
      return std::move(effect_func)(std::move(ctx), std::move(obc));
    }
  };
  auto task =
    std::make_unique<task_t>(std::move(ctx), std::move(effect_func), obc);
  auto& ctx_ref = task->ctx;
  op_effects.emplace_back(std::move(task));
  return std::forward<MainFunc>(main_func)(ctx_ref);
}

template <typename MutFunc>
OpsExecuter::osd_op_ierrorator::future<> OpsExecuter::flush_changes(
  MutFunc&& mut_func) &&
{
  const bool want_mutate = !txn.empty();
  // osd_op_params are instantiated by every wr-like operation.
  assert(osd_op_params || !want_mutate);
  assert(obc);
  auto maybe_mutated = interruptor::make_interruptible(osd_op_errorator::now());
  if (want_mutate) {
    maybe_mutated = std::forward<MutFunc>(mut_func)(std::move(txn),
                                                    std::move(obc),
                                                    std::move(*osd_op_params),
                                                    user_modify);
  }
  if (__builtin_expect(op_effects.empty(), true)) {
    return maybe_mutated;
  } else {
    return maybe_mutated.safe_then_interruptible([this] {
      // let's do the cleaning of `op_effects` in destructor
      return interruptor::do_for_each(op_effects, [] (auto& op_effect) {
        return op_effect->execute();
      });
    });
  }
}

// PgOpsExecuter -- a class for executing ops targeting a certain PG.
class PgOpsExecuter {
  template <typename T = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      IOInterruptCondition, T>;

public:
  PgOpsExecuter(const PG& pg, const MOSDOp& msg)
    : pg(pg), nspace(msg.get_hobj().nspace) {
  }

  interruptible_future<> execute_op(class OSDOp& osd_op);

private:
  const PG& pg;
  const std::string& nspace;
};

} // namespace crimson::osd
