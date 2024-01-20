// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <type_traits>
#include <utility>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <fmt/os.h>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include "common/dout.h"
#include "common/map_cacher.hpp"
#include "common/static_ptr.h"
#include "messages/MOSDOp.h"
#include "os/Transaction.h"
#include "osd/osd_types.h"

#include "crimson/common/errorator.h"
#include "crimson/common/interruptible_future.h"
#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/shard_services.h"

struct ObjectState;
struct OSDOp;
class OSDriver;
class SnapMapper;

namespace crimson::osd {
class PG;

// OpsExecuter -- a class for executing ops targeting a certain object.
class OpsExecuter : public seastar::enable_lw_shared_from_this<OpsExecuter> {
  friend class SnapTrimObjSubEvent;

  using call_errorator = crimson::errorator<
    crimson::stateful_ec,
    crimson::ct_error::enoent,
    crimson::ct_error::eexist,
    crimson::ct_error::enospc,
    crimson::ct_error::edquot,
    crimson::ct_error::cmp_fail,
    crimson::ct_error::eagain,
    crimson::ct_error::invarg,
    crimson::ct_error::erange,
    crimson::ct_error::ecanceled,
    crimson::ct_error::enametoolong,
    crimson::ct_error::permission_denied,
    crimson::ct_error::operation_not_supported,
    crimson::ct_error::input_output_error,
    crimson::ct_error::value_too_large,
    crimson::ct_error::file_too_large>;
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
  // ExecutableMessage -- an interface class to allow using OpsExecuter
  // with other message types than just the `MOSDOp`. The type erasure
  // happens in the ctor of `OpsExecuter`.
  struct ExecutableMessage {
    virtual osd_reqid_t get_reqid() const = 0;
    virtual utime_t get_mtime() const = 0;
    virtual epoch_t get_map_epoch() const = 0;
    virtual entity_inst_t get_orig_source_inst() const = 0;
    virtual uint64_t get_features() const = 0;
    virtual bool has_flag(uint32_t flag) const = 0;
    virtual entity_name_t get_source() const = 0;
  };

  template <class ImplT>
  class ExecutableMessagePimpl final : ExecutableMessage {
    const ImplT* pimpl;
    // In crimson, conn is independently maintained outside Message.
    const crimson::net::ConnectionRef conn;
  public:
    ExecutableMessagePimpl(const ImplT* pimpl,
                           const crimson::net::ConnectionRef conn)
      : pimpl(pimpl), conn(conn) {
    }

    osd_reqid_t get_reqid() const final {
      return pimpl->get_reqid();
    }
    bool has_flag(uint32_t flag) const final {
      return pimpl->has_flag(flag);
    }
    utime_t get_mtime() const final {
      return pimpl->get_mtime();
    };
    epoch_t get_map_epoch() const final {
      return pimpl->get_map_epoch();
    }
    entity_inst_t get_orig_source_inst() const final {
      // We can't get the origin source address from the message
      // since (In Crimson) the connection is maintained
      // outside of the Message.
      return entity_inst_t(get_source(), conn->get_peer_addr());
    }
    entity_name_t get_source() const final {
      return pimpl->get_source();
    }
    uint64_t get_features() const final {
      return pimpl->get_features();
    }
  };

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

  object_stat_sum_t delta_stats;
private:
  // an operation can be divided into two stages: main and effect-exposing
  // one. The former is performed immediately on call to `do_osd_op()` while
  // the later on `submit_changes()` – after successfully processing main
  // stages of all involved operations. When any stage fails, none of all
  // scheduled effect-exposing stages will be executed.
  // when operation requires this division, some variant of `with_effect()`
  // should be used.
  struct effect_t {
    // an effect can affect PG, i.e. create a watch timeout
    virtual osd_op_errorator::future<> execute(Ref<PG> pg) = 0;
    virtual ~effect_t() = default;
  };

  Ref<PG> pg; // for the sake of object class
  ObjectContextRef obc;
  const OpInfo& op_info;
  using abstracted_msg_t =
    ceph::static_ptr<ExecutableMessage,
                     sizeof(ExecutableMessagePimpl<void>)>;
  abstracted_msg_t msg;
  crimson::net::ConnectionRef conn;
  std::optional<osd_op_params_t> osd_op_params;
  bool user_modify = false;
  ceph::os::Transaction txn;

  size_t num_read = 0;    ///< count read ops
  size_t num_write = 0;   ///< count update ops

  SnapContext snapc; // writer snap context
  struct CloningContext {
    SnapSet new_snapset;
    pg_log_entry_t log_entry;

    void apply_to(
      std::vector<pg_log_entry_t>& log_entries,
      ObjectContext& processed_obc) &&;
  };
  std::unique_ptr<CloningContext> cloning_ctx;


  /**
   * execute_clone
   *
   * If snapc contains a snap which occurred logically after the last write
   * seen by this object (see OpsExecutor::should_clone()), we first need
   * make a clone of the object at its current state.  execute_clone primes
   * txn with that clone operation and returns an
   * OpsExecutor::CloningContext which will allow us to fill in the corresponding
   * metadata and log_entries once the operations have been processed.
   *
   * Note that this strategy differs from classic, which instead performs this
   * work at the end and reorders the transaction.  See
   * PrimaryLogPG::make_writeable
   *
   * @param snapc [in] snapc for this operation (from the client if from the
   *                   client, from the pool otherwise)
   * @param initial_obs [in] objectstate for the object at operation start
   * @param initial_snapset [in] snapset for the object at operation start
   * @param backend [in,out] interface for generating mutations
   * @param txn [out] transaction for the operation
   */
  std::unique_ptr<CloningContext> execute_clone(
    const SnapContext& snapc,
    const ObjectState& initial_obs,
    const SnapSet& initial_snapset,
    PGBackend& backend,
    ceph::os::Transaction& txn);


  /**
   * should_clone
   *
   * Predicate returning whether a user write with snap context snapc
   * contains a snap which occurred prior to the most recent write
   * on the object reflected in initial_obc.
   *
   * @param initial_obc [in] obc for object to be mutated
   * @param snapc [in] snapc for this operation (from the client if from the
   *                   client, from the pool otherwise)
   */
  static bool should_clone(
    const ObjectContext& initial_obc,
    const SnapContext& snapc) {
    // clone?
    return initial_obc.obs.exists                       // both nominally and...
      && !initial_obc.obs.oi.is_whiteout()              // ... logically exists
      && snapc.snaps.size()                             // there are snaps
      && snapc.snaps[0] > initial_obc.ssc->snapset.seq; // existing obj is old
  }

  interruptible_future<std::vector<pg_log_entry_t>> flush_clone_metadata(
    std::vector<pg_log_entry_t>&& log_entries,
    SnapMapper& snap_mapper,
    OSDriver& osdriver,
    ceph::os::Transaction& txn);

  static interruptible_future<> snap_map_remove(
    const hobject_t& soid,
    SnapMapper& snap_mapper,
    OSDriver& osdriver,
    ceph::os::Transaction& txn);
  static interruptible_future<> snap_map_modify(
    const hobject_t& soid,
    const std::set<snapid_t>& snaps,
    SnapMapper& snap_mapper,
    OSDriver& osdriver,
    ceph::os::Transaction& txn);
  static interruptible_future<> snap_map_clone(
    const hobject_t& soid,
    const std::set<snapid_t>& snaps,
    SnapMapper& snap_mapper,
    OSDriver& osdriver,
    ceph::os::Transaction& txn);

  // this gizmo could be wrapped in std::optional for the sake of lazy
  // initialization. we don't need it for ops that doesn't have effect
  // TODO: verify the init overhead of chunked_fifo
  seastar::chunked_fifo<std::unique_ptr<effect_t>> op_effects;

  template <class Context, class MainFunc, class EffectFunc>
  auto with_effect_on_obc(
    Context&& ctx,
    MainFunc&& main_func,
    EffectFunc&& effect_func);

  call_ierrorator::future<> do_op_call(OSDOp& osd_op);
  watch_ierrorator::future<> do_op_watch(
    OSDOp& osd_op,
    ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_watch_subop_watch(
    OSDOp& osd_op,
    ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_watch_subop_reconnect(
    OSDOp& osd_op,
    ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_watch_subop_unwatch(
    OSDOp& osd_op,
    ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_watch_subop_ping(
    OSDOp& osd_op,
    ObjectState& os,
    ceph::os::Transaction& txn);
  watch_ierrorator::future<> do_op_list_watchers(
    OSDOp& osd_op,
    const ObjectState& os);
  watch_ierrorator::future<> do_op_notify(
    OSDOp& osd_op,
    const ObjectState& os);
  watch_ierrorator::future<> do_op_notify_ack(
    OSDOp& osd_op,
    const ObjectState& os);
  call_errorator::future<> do_assert_ver(
    OSDOp& osd_op,
    const ObjectState& os);

  using list_snaps_ertr = read_errorator::extend<
    crimson::ct_error::invarg>;
  using list_snaps_iertr = ::crimson::interruptible::interruptible_errorator<
    ::crimson::osd::IOInterruptCondition,
    list_snaps_ertr>;
  list_snaps_iertr::future<> do_list_snaps(
    OSDOp& osd_op,
    const ObjectState& os,
    const SnapSet& ss);

  template <class Func>
  auto do_const_op(Func&& f);

  template <class Func>
  auto do_read_op(Func&& f) {
    ++num_read;
    // TODO: pass backend as read-only
    return do_const_op(std::forward<Func>(f));
  }

  template <class Func>
  auto do_snapset_op(Func&& f) {
    ++num_read;
    return std::invoke(
      std::forward<Func>(f),
      std::as_const(obc->obs),
      std::as_const(obc->ssc->snapset));
  }

  enum class modified_by {
    user,
    sys,
  };

  template <class Func>
  auto do_write_op(Func&& f, modified_by m = modified_by::user);

  decltype(auto) dont_do_legacy_op() {
    return crimson::ct_error::operation_not_supported::make();
  }

  interruptible_errorated_future<osd_op_errorator>
  do_execute_op(OSDOp& osd_op);

  OpsExecuter(Ref<PG> pg,
              ObjectContextRef obc,
              const OpInfo& op_info,
              abstracted_msg_t&& msg,
              crimson::net::ConnectionRef conn,
              const SnapContext& snapc);

public:
  template <class MsgT>
  OpsExecuter(Ref<PG> pg,
              ObjectContextRef obc,
              const OpInfo& op_info,
              const MsgT& msg,
              crimson::net::ConnectionRef conn,
              const SnapContext& snapc)
    : OpsExecuter(
        std::move(pg),
        std::move(obc),
        op_info,
        abstracted_msg_t{
          std::in_place_type_t<ExecutableMessagePimpl<MsgT>>{},
          &msg,
          conn},
        conn,
        snapc) {
  }

  template <class Func>
  struct RollbackHelper;

  template <class Func>
  RollbackHelper<Func> create_rollbacker(Func&& func);

  interruptible_errorated_future<osd_op_errorator>
  execute_op(OSDOp& osd_op);

  using rep_op_fut_tuple =
    std::tuple<interruptible_future<>, osd_op_ierrorator::future<>>;
  using rep_op_fut_t =
    interruptible_future<rep_op_fut_tuple>;
  template <typename MutFunc>
  rep_op_fut_t flush_changes_n_do_ops_effects(
    const std::vector<OSDOp>& ops,
    SnapMapper& snap_mapper,
    OSDriver& osdriver,
    MutFunc&& mut_func) &&;
  std::vector<pg_log_entry_t> prepare_transaction(
    const std::vector<OSDOp>& ops);
  void fill_op_params_bump_pg_version();

  ObjectContextRef get_obc() const {
    return obc;
  }

  const object_info_t &get_object_info() const {
    return obc->obs.oi;
  }
  const hobject_t &get_target() const {
    return get_object_info().soid;
  }

  const auto& get_message() const {
    return *msg;
  }

  size_t get_processed_rw_ops_num() const {
    return num_read + num_write;
  }

  uint32_t get_pool_stripe_width() const;

  bool has_seen_write() const {
    return num_write > 0;
  }

  object_stat_sum_t& get_stats(){
    return delta_stats;
  }

  version_t get_last_user_version() const;

  ObjectContextRef prepare_clone(
    const hobject_t& coid);

  void apply_stats();
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
    seastar::future<> (*)(context_t&&, ObjectContextRef, Ref<PG>);
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
    osd_op_errorator::future<> execute(Ref<PG> pg) final {
      return std::move(effect_func)(std::move(ctx),
                                    std::move(obc),
                                    std::move(pg));
    }
  };
  auto task =
    std::make_unique<task_t>(std::move(ctx), std::move(effect_func), obc);
  auto& ctx_ref = task->ctx;
  op_effects.emplace_back(std::move(task));
  return std::forward<MainFunc>(main_func)(ctx_ref);
}

template <typename MutFunc>
OpsExecuter::rep_op_fut_t
OpsExecuter::flush_changes_n_do_ops_effects(
  const std::vector<OSDOp>& ops,
  SnapMapper& snap_mapper,
  OSDriver& osdriver,
  MutFunc&& mut_func) &&
{
  const bool want_mutate = !txn.empty();
  // osd_op_params are instantiated by every wr-like operation.
  assert(osd_op_params || !want_mutate);
  assert(obc);
  rep_op_fut_t maybe_mutated =
    interruptor::make_ready_future<rep_op_fut_tuple>(
	seastar::now(),
	interruptor::make_interruptible(osd_op_errorator::now()));
  if (cloning_ctx) {
    ceph_assert(want_mutate);
  }
  if (want_mutate) {
    if (user_modify) {
      osd_op_params->user_at_version = osd_op_params->at_version.version;
    }
    maybe_mutated = flush_clone_metadata(
      prepare_transaction(ops),
      snap_mapper,
      osdriver,
      txn
    ).then_interruptible([mut_func=std::move(mut_func),
                          this](auto&& log_entries) mutable {
      auto [submitted, all_completed] =
        std::forward<MutFunc>(mut_func)(std::move(txn),
                                        std::move(obc),
                                        std::move(*osd_op_params),
                                        std::move(log_entries));
      return interruptor::make_ready_future<rep_op_fut_tuple>(
	std::move(submitted),
	osd_op_ierrorator::future<>(std::move(all_completed)));
    });
  }
  apply_stats();

  if (__builtin_expect(op_effects.empty(), true)) {
    return maybe_mutated;
  } else {
    return maybe_mutated.then_unpack_interruptible(
      // need extra ref pg due to apply_stats() which can be executed after
      // informing snap mapper
      [this, pg=this->pg](auto&& submitted, auto&& all_completed) mutable {
      return interruptor::make_ready_future<rep_op_fut_tuple>(
	  std::move(submitted),
	  all_completed.safe_then_interruptible([this, pg=std::move(pg)] {
	    // let's do the cleaning of `op_effects` in destructor
	    return interruptor::do_for_each(op_effects,
	      [pg=std::move(pg)](auto& op_effect) {
	      return op_effect->execute(pg);
	    });
	  }));
    });
  }
}

template <class Func>
struct OpsExecuter::RollbackHelper {
  interruptible_future<> rollback_obc_if_modified(const std::error_code& e);
  ObjectContextRef get_obc() const {
    assert(ox);
    return ox->obc;
  }
  seastar::lw_shared_ptr<OpsExecuter> ox;
  Func func;
};

template <class Func>
inline OpsExecuter::RollbackHelper<Func>
OpsExecuter::create_rollbacker(Func&& func) {
  return {shared_from_this(), std::forward<Func>(func)};
}


template <class Func>
OpsExecuter::interruptible_future<>
OpsExecuter::RollbackHelper<Func>::rollback_obc_if_modified(
  const std::error_code& e)
{
  // Oops, an operation had failed. do_osd_ops() altogether with
  // OpsExecuter already dropped the ObjectStore::Transaction if
  // there was any. However, this is not enough to completely
  // rollback as we gave OpsExecuter the very single copy of `obc`
  // we maintain and we did it for both reading and writing.
  // Now all modifications must be reverted.
  //
  // Let's just reload from the store. Evicting from the shared
  // LRU would be tricky as next MOSDOp (the one at `get_obc`
  // phase) could actually already finished the lookup. Fortunately,
  // this is supposed to live on cold  paths, so performance is not
  // a concern -- simplicity wins.
  //
  // The conditional's purpose is to efficiently handle hot errors
  // which may appear as a result of e.g. CEPH_OSD_OP_CMPXATTR or
  // CEPH_OSD_OP_OMAP_CMP. These are read-like ops and clients
  // typically append them before any write. If OpsExecuter hasn't
  // seen any modifying operation, `obc` is supposed to be kept
  // unchanged.
  assert(ox);
  const auto need_rollback = ox->has_seen_write();
  crimson::get_logger(ceph_subsys_osd).debug(
    "{}: object {} got error {}, need_rollback={}",
    __func__,
    ox->obc->get_oid(),
    e,
    need_rollback);
  return need_rollback ? func(*ox->obc) : interruptor::now();
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

  interruptible_future<> execute_op(OSDOp& osd_op);

private:
  const PG& pg;
  const std::string& nspace;
};

} // namespace crimson::osd
