// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sharded.hh>

#include "crimson/osd/osd_connection_priv.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/pg_map.h"

namespace crimson::os {
  class FuturizedStore;
}

namespace crimson::osd {
/**
 * PGShardManager
 *
 * Manages all state required to partition PGs over seastar reactors
 * as well as state required to route messages to pgs. Mediates access to
 * shared resources required by PGs (objectstore, messenger, monclient,
 * etc)
 */
class PGShardManager {
  seastar::sharded<OSDSingletonState> &osd_singleton_state;
  seastar::sharded<ShardServices> &shard_services;
  seastar::sharded<PGShardMapping> &pg_to_shard_mapping;

#define FORWARD_CONST(FROM_METHOD, TO_METHOD, TARGET)		\
  template <typename... Args>					\
  auto FROM_METHOD(Args&&... args) const {			\
    return TARGET.TO_METHOD(std::forward<Args>(args)...);	\
  }

#define FORWARD(FROM_METHOD, TO_METHOD, TARGET)		\
  template <typename... Args>					\
  auto FROM_METHOD(Args&&... args) {				\
    return TARGET.TO_METHOD(std::forward<Args>(args)...);	\
  }

#define FORWARD_TO_OSD_SINGLETON(METHOD) \
  FORWARD(METHOD, METHOD, get_osd_singleton_state())

public:
  using cached_map_t = OSDMapService::cached_map_t;
  using local_cached_map_t = OSDMapService::local_cached_map_t;

  PGShardManager(
    seastar::sharded<OSDSingletonState> &osd_singleton_state,
    seastar::sharded<ShardServices> &shard_services,
    seastar::sharded<PGShardMapping> &pg_to_shard_mapping)
  : osd_singleton_state(osd_singleton_state),
    shard_services(shard_services),
    pg_to_shard_mapping(pg_to_shard_mapping) {}

  auto &get_osd_singleton_state() {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    return osd_singleton_state.local();
  }
  auto &get_osd_singleton_state() const {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    return osd_singleton_state.local();
  }
  auto &get_shard_services() {
    return shard_services.local();
  }
  auto &get_shard_services() const {
    return shard_services.local();
  }
  auto &get_local_state() { return get_shard_services().local_state; }
  auto &get_local_state() const { return get_shard_services().local_state; }
  auto &get_pg_to_shard_mapping() { return pg_to_shard_mapping.local(); }
  auto &get_pg_to_shard_mapping() const { return pg_to_shard_mapping.local(); }

  seastar::future<> update_map(local_cached_map_t &&map) {
    get_osd_singleton_state().update_map(
      make_local_shared_foreign(local_cached_map_t(map))
    );
    /* We need each core to get its own foreign_ptr<local_cached_map_t>.
     * foreign_ptr can't be cheaply copied, so we make one for each core
     * up front. */
    return seastar::do_with(
      std::vector<seastar::foreign_ptr<local_cached_map_t>>(),
      [this, map](auto &fmaps) {
	fmaps.resize(seastar::smp::count);
	for (auto &i: fmaps) {
	  i = seastar::foreign_ptr(map);
	}
	return shard_services.invoke_on_all(
	  [&fmaps](auto &local) mutable {
	    local.local_state.update_map(
	      make_local_shared_foreign(
		std::move(fmaps[seastar::this_shard_id()])
	      ));
	  });
      });
  }

  seastar::future<> stop_registries() {
    return shard_services.invoke_on_all([](auto &local) {
      return local.local_state.stop_registry();
    });
  }

  FORWARD_TO_OSD_SINGLETON(send_pg_created)

  // osd state forwards
  FORWARD(is_active, is_active, get_shard_services().local_state.osd_state)
  FORWARD(is_preboot, is_preboot, get_shard_services().local_state.osd_state)
  FORWARD(is_booting, is_booting, get_shard_services().local_state.osd_state)
  FORWARD(is_stopping, is_stopping, get_shard_services().local_state.osd_state)
  FORWARD(is_prestop, is_prestop, get_shard_services().local_state.osd_state)
  FORWARD(is_initializing, is_initializing, get_shard_services().local_state.osd_state)
  FORWARD(set_prestop, set_prestop, get_shard_services().local_state.osd_state)
  FORWARD(set_preboot, set_preboot, get_shard_services().local_state.osd_state)
  FORWARD(set_booting, set_booting, get_shard_services().local_state.osd_state)
  FORWARD(set_stopping, set_stopping, get_shard_services().local_state.osd_state)
  FORWARD(set_active, set_active, get_shard_services().local_state.osd_state)
  FORWARD(when_active, when_active, get_shard_services().local_state.osd_state)
  FORWARD_CONST(get_osd_state_string, to_string, get_shard_services().local_state.osd_state)

  FORWARD(got_map, got_map, get_shard_services().local_state.osdmap_gate)
  FORWARD(wait_for_map, wait_for_map, get_shard_services().local_state.osdmap_gate)

  // Metacoll
  FORWARD_TO_OSD_SINGLETON(init_meta_coll)
  FORWARD_TO_OSD_SINGLETON(get_meta_coll)

  // Core OSDMap methods
  FORWARD_TO_OSD_SINGLETON(get_local_map)
  FORWARD_TO_OSD_SINGLETON(load_map_bl)
  FORWARD_TO_OSD_SINGLETON(load_map_bls)
  FORWARD_TO_OSD_SINGLETON(store_maps)
  FORWARD_TO_OSD_SINGLETON(trim_maps)

  seastar::future<> set_up_epoch(epoch_t e);

  seastar::future<> set_superblock(OSDSuperblock superblock);

  template <typename F>
  auto with_remote_shard_state(core_id_t core, F &&f) {
    return shard_services.invoke_on(
      core, [f=std::move(f)](auto &target_shard_services) mutable {
	return std::invoke(
	  std::move(f), target_shard_services.local_state,
	  target_shard_services);
      });
  }

  template <typename T, typename F>
  auto process_ordered_op_remotely(
      OSDConnectionPriv::crosscore_ordering_t::seq_t cc_seq,
      ShardServices &target_shard_services,
      typename T::IRef &&op,
      F &&f) {
    auto &crosscore_ordering = get_osd_priv(&op->get_connection()).crosscore_ordering;
    if (crosscore_ordering.proceed_or_wait(cc_seq)) {
      return std::invoke(
        std::move(f),
        target_shard_services,
        std::move(op));
    } else {
      auto &logger = crimson::get_logger(ceph_subsys_osd);
      logger.debug("{} got {} at the remote pg, wait at {}",
                   *op, cc_seq, crosscore_ordering.get_in_seq());
      return crosscore_ordering.wait(cc_seq
      ).then([this, cc_seq, &target_shard_services,
              op=std::move(op), f=std::move(f)]() mutable {
        return this->template process_ordered_op_remotely<T>(
            cc_seq, target_shard_services, std::move(op), std::move(f));
      });
    }
  }

  template <typename T, typename F>
  auto with_remote_shard_state_and_op(
      core_id_t core,
      typename T::IRef &&op,
      F &&f) {
    ceph_assert(op->use_count() == 1);
    if (seastar::this_shard_id() == core) {
      auto &target_shard_services = shard_services.local();
      return std::invoke(
        std::move(f),
        target_shard_services,
        std::move(op));
    }
    // Note: the ordering in only preserved until f is invoked.
    auto &opref = *op;
    auto &crosscore_ordering = get_osd_priv(&opref.get_connection()).crosscore_ordering;
    auto cc_seq = crosscore_ordering.prepare_submit(core);
    auto &logger = crimson::get_logger(ceph_subsys_osd);
    logger.debug("{}: send {} to the remote pg core {}",
                 opref, cc_seq, core);
    return opref.get_handle().complete(
    ).then([&opref, this] {
      get_local_state().registry.remove_from_registry(opref);
      return opref.prepare_remote_submission();
    }).then([op=std::move(op), f=std::move(f), this, core, cc_seq
            ](auto f_conn) mutable {
      return shard_services.invoke_on(
        core,
        [this, cc_seq,
         f=std::move(f), op=std::move(op), f_conn=std::move(f_conn)
        ](auto &target_shard_services) mutable {
        op->finish_remote_submission(std::move(f_conn));
        target_shard_services.local_state.registry.add_to_registry(*op);
        return this->template process_ordered_op_remotely<T>(
            cc_seq, target_shard_services, std::move(op), std::move(f));
      });
    });
  }

  /// Runs opref on the appropriate core, creating the pg as necessary.
  template <typename T>
  seastar::future<> run_with_pg_maybe_create(
    typename T::IRef op,
    ShardServices &target_shard_services
  ) {
    static_assert(T::can_create());
    auto &logger = crimson::get_logger(ceph_subsys_osd);
    auto &opref = *op;
    return opref.template with_blocking_event<
      PGMap::PGCreationBlockingEvent
    >([&target_shard_services, &opref](auto &&trigger) {
      return target_shard_services.get_or_create_pg(
        std::move(trigger),
        opref.get_pgid(),
        std::move(opref.get_create_info())
      );
    }).safe_then([&logger, &target_shard_services, &opref](Ref<PG> pgref) {
      logger.debug("{}: have_pg", opref);
      return opref.with_pg(target_shard_services, pgref);
    }).handle_error(
      crimson::ct_error::ecanceled::handle([&logger, &opref](auto) {
        logger.debug("{}: pg creation canceled, dropping", opref);
        return seastar::now();
      })
    ).then([op=std::move(op)] {});
  }

  /// Runs opref on the appropriate core, waiting for pg as necessary
  template <typename T>
  seastar::future<> run_with_pg_maybe_wait(
    typename T::IRef op,
    ShardServices &target_shard_services
  ) {
    static_assert(!T::can_create());
    auto &logger = crimson::get_logger(ceph_subsys_osd);
    auto &opref = *op;
    return opref.template with_blocking_event<
      PGMap::PGCreationBlockingEvent
    >([&target_shard_services, &opref](auto &&trigger) {
      return target_shard_services.wait_for_pg(
        std::move(trigger), opref.get_pgid());
    }).safe_then([&logger, &target_shard_services, &opref](Ref<PG> pgref) {
      logger.debug("{}: have_pg", opref);
      return opref.with_pg(target_shard_services, pgref);
    }).handle_error(
      crimson::ct_error::ecanceled::handle([&logger, &opref](auto) {
        logger.debug("{}: pg creation canceled, dropping", opref);
        return seastar::now();
      })
    ).then([op=std::move(op)] {});
  }

  seastar::future<> load_pgs(crimson::os::FuturizedStore& store);
  seastar::future<> stop_pgs();

  seastar::future<std::map<pg_t, pg_stat_t>> get_pg_stats() const;

  /**
   * invoke_method_on_each_shard_seq
   *
   * Invokes shard_services method on each shard sequentially.
   */
  template <typename F, typename... Args>
  seastar::future<> invoke_on_each_shard_seq(
    F &&f) const {
    return sharded_map_seq(
      shard_services,
      [f=std::forward<F>(f)](const ShardServices &shard_services) mutable {
	return std::invoke(
	  f,
	  shard_services);
      });
  }

  /**
   * for_each_pg
   *
   * Invokes f on each pg sequentially.  Caller may rely on f not being
   * invoked concurrently on multiple cores.
   */
  template <typename F>
  seastar::future<> for_each_pg(F &&f) const {
    return invoke_on_each_shard_seq(
      [f=std::move(f)](const auto &local_service) mutable {
	for (auto &pg: local_service.local_state.pg_map.get_pgs()) {
	  std::apply(f, pg);
	}
	return seastar::now();
      });
  }

  /**
   * for_each_pgid
   *
   * Syncronously invokes f on each pgid
   */
  template <typename F>
  void for_each_pgid(F &&f) const {
    return get_pg_to_shard_mapping().for_each_pgid(
      std::forward<F>(f));
  }

  auto get_num_pgs() const {
    return get_pg_to_shard_mapping().get_num_pgs();
  }

  seastar::future<> broadcast_map_to_pgs(epoch_t epoch);

  template <typename F>
  auto with_pg(spg_t pgid, F &&f) {
    core_id_t core = get_pg_to_shard_mapping().get_pg_mapping(pgid);
    return with_remote_shard_state(
      core,
      [pgid, f=std::move(f)](auto &local_state, auto &local_service) mutable {
	return std::invoke(
	  std::move(f),
	  local_state.pg_map.get_pg(pgid));
      });
  }

  template <typename T, typename... Args>
  auto start_pg_operation(Args&&... args) {
    auto op = get_local_state().registry.create_operation<T>(
      std::forward<Args>(args)...);
    auto &logger = crimson::get_logger(ceph_subsys_osd);
    logger.debug("{}: starting {}", *op, __func__);

    auto &opref = *op;
    auto id = op->get_id();
    if constexpr (T::is_trackable) {
      op->template track_event<typename T::StartEvent>();
    }
    auto fut = opref.template enter_stage<>(
      opref.get_connection_pipeline().await_active
    ).then([this, &opref, &logger] {
      logger.debug("{}: start_pg_operation in await_active stage", opref);
      return get_shard_services().local_state.osd_state.when_active();
    }).then([&logger, &opref] {
      logger.debug("{}: start_pg_operation active, entering await_map", opref);
      return opref.template enter_stage<>(
	opref.get_connection_pipeline().await_map);
    }).then([this, &logger, &opref] {
      logger.debug("{}: start_pg_operation await_map stage", opref);
      using OSDMapBlockingEvent =
	OSD_OSDMapGate::OSDMapBlocker::BlockingEvent;
      return opref.template with_blocking_event<OSDMapBlockingEvent>(
	[this, &opref](auto &&trigger) {
	  std::ignore = this;
	  return get_shard_services().local_state.osdmap_gate.wait_for_map(
	      std::move(trigger),
	      opref.get_epoch(),
	      &get_shard_services());
      });
    }).then([&logger, &opref](auto epoch) {
      logger.debug("{}: got map {}, entering get_pg_mapping", opref, epoch);
      return opref.template enter_stage<>(
	opref.get_connection_pipeline().get_pg_mapping);
    }).then([this, &opref] {
      return get_pg_to_shard_mapping().get_or_create_pg_mapping(opref.get_pgid());
    }).then_wrapped([this, &logger, op=std::move(op)](auto fut) mutable {
      if (unlikely(fut.failed())) {
        logger.error("{}: failed before with_pg", *op);
        op->get_handle().exit();
        return seastar::make_exception_future<>(fut.get_exception());
      }

      auto core = fut.get();
      logger.debug("{}: can_create={}, target-core={}",
                   *op, T::can_create(), core);
      return this->template with_remote_shard_state_and_op<T>(
        core, std::move(op),
        [this](ShardServices &target_shard_services,
               typename T::IRef op) {
        auto &opref = *op;
        auto &logger = crimson::get_logger(ceph_subsys_osd);
        logger.debug("{}: entering create_or_wait_pg", opref);
        return opref.template enter_stage<>(
          opref.get_pershard_pipeline(target_shard_services).create_or_wait_pg
        ).then([this, &target_shard_services, op=std::move(op)]() mutable {
          if constexpr (T::can_create()) {
            return this->template run_with_pg_maybe_create<T>(
                std::move(op), target_shard_services);
          } else {
            return this->template run_with_pg_maybe_wait<T>(
                std::move(op), target_shard_services);
          }
        });
      });
    });
    return std::make_pair(id, std::move(fut));
  }

#undef FORWARD
#undef FORWARD_CONST
#undef FORWARD_TO_OSD_SINGLETON
};

}
