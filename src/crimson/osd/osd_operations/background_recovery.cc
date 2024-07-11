// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

#include "messages/MOSDOp.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/shard_services.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/osd_operations/background_recovery.h"

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::UrgentRecovery> {
    static std::tuple<> get_backends() {
      return {};
    }
  };

  template <>
  struct EventBackendRegistry<osd::PglogBasedRecovery> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

SET_SUBSYS(osd);

namespace crimson::osd {

template <class T>
BackgroundRecoveryT<T>::BackgroundRecoveryT(
  Ref<PG> pg,
  ShardServices &ss,
  epoch_t epoch_started,
  crimson::osd::scheduler::scheduler_class_t scheduler_class,
  float delay)
  : pg(pg),
    epoch_started(epoch_started),
    delay(delay),
    ss(ss),
    scheduler_class(scheduler_class)
{}

template <class T>
void BackgroundRecoveryT<T>::print(std::ostream &lhs) const
{
  lhs << "BackgroundRecovery(" << pg->get_pgid() << ")";
}

template <class T>
void BackgroundRecoveryT<T>::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg->get_pgid();
  f->open_object_section("recovery_detail");
  {
    // TODO pg->dump_recovery_state(f);
  }
  f->close_section();
}

template <class T>
seastar::future<> BackgroundRecoveryT<T>::start()
{
  typename T::IRef ref = static_cast<T*>(this);
  using interruptor = typename T::interruptor;

  LOG_PREFIX(BackgroundRecoveryT<T>::start);
  DEBUGDPPI("{}: start", *pg, *this);
  auto maybe_delay = seastar::now();
  if (delay) {
    maybe_delay = seastar::sleep(
      std::chrono::milliseconds(std::lround(delay * 1000)));
  }
  return maybe_delay.then([ref, this] {
    return this->template with_blocking_event<OperationThrottler::BlockingEvent>(
      [ref, this] (auto&& trigger) {
      return ss.with_throttle_while(
        std::move(trigger),
        this, get_scheduler_params(), [this] {
          return interruptor::with_interruption([this] {
            return do_recovery();
          }, [](std::exception_ptr) {
            return seastar::make_ready_future<bool>(false);
          }, pg);
        }).handle_exception_type([ref, this](const std::system_error& err) {
	  LOG_PREFIX(BackgroundRecoveryT<T>::start);
          if (err.code() == std::make_error_code(std::errc::interrupted)) {
            DEBUGDPPI("recovery interruped: {}", *pg, err.what());
            return seastar::now();
          }
          return seastar::make_exception_future<>(err);
        });
      });
  });
}

UrgentRecovery::UrgentRecovery(
    const hobject_t& soid,
    const eversion_t& need,
    Ref<PG> pg,
    ShardServices& ss,
    epoch_t epoch_started)
  : BackgroundRecoveryT{pg, ss, epoch_started,
                        crimson::osd::scheduler::scheduler_class_t::immediate},
    soid{soid}, need(need)
{
}

UrgentRecovery::interruptible_future<bool>
UrgentRecovery::do_recovery()
{
  LOG_PREFIX(UrgentRecovery::do_recovery);
  DEBUGDPPI("{}: {}", *pg, __func__, *this);
  if (pg->has_reset_since(epoch_started)) {
    return seastar::make_ready_future<bool>(false);
  }

  return pg->find_unfound(epoch_started
  ).then_interruptible([this] {
    return with_blocking_event<RecoveryBackend::RecoveryBlockingEvent,
			       interruptor>([this] (auto&& trigger) {
      return pg->get_recovery_handler()->recover_missing(trigger, soid, need);
    }).then_interruptible([] {
      return seastar::make_ready_future<bool>(false);
    });
  });
}

void UrgentRecovery::print(std::ostream &lhs) const
{
  lhs << "UrgentRecovery(" << pg->get_pgid() << ", "
    << soid << ", v" << need << ", epoch_started: "
    << epoch_started << ")";
}

void UrgentRecovery::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg->get_pgid();
  f->open_object_section("recovery_detail");
  {
    f->dump_stream("oid") << soid;
    f->dump_stream("version") << need;
  }
  f->close_section();
}

PglogBasedRecovery::PglogBasedRecovery(
  Ref<PG> pg,
  ShardServices &ss,
  const epoch_t epoch_started,
  float delay)
  : BackgroundRecoveryT(
      std::move(pg),
      ss,
      epoch_started,
      crimson::osd::scheduler::scheduler_class_t::background_recovery,
      delay)
{}

PglogBasedRecovery::interruptible_future<bool>
PglogBasedRecovery::do_recovery()
{
  if (pg->has_reset_since(epoch_started)) {
    return seastar::make_ready_future<bool>(false);
  }
  return pg->find_unfound(epoch_started
  ).then_interruptible([this] {
    return with_blocking_event<RecoveryBackend::RecoveryBlockingEvent,
			       interruptor>([this] (auto&& trigger) {
      return pg->get_recovery_handler()->start_recovery_ops(
	trigger,
	crimson::common::local_conf()->osd_recovery_max_single_start);
    });
  });
}

PGPeeringPipeline &BackfillRecovery::peering_pp(PG &pg)
{
  return pg.peering_request_pg_pipeline;
}

BackfillRecovery::interruptible_future<bool>
BackfillRecovery::do_recovery()
{
  LOG_PREFIX(BackfillRecovery::do_recovery);
  DEBUGDPPI("{}", *pg, __func__);

  if (pg->has_reset_since(epoch_started)) {
    DEBUGDPPI("{}: pg got reset since epoch_started={}",
		*pg, __func__, epoch_started);
    return seastar::make_ready_future<bool>(false);
  }
  // TODO: limits
  return enter_stage<interruptor>(
    // process_event() of our boost::statechart machine is non-reentrant.
    // with the backfill_pipeline we protect it from a second entry from
    // the implementation of BackfillListener.
    // additionally, this stage serves to synchronize with PeeringEvent.
    peering_pp(*pg).process
  ).then_interruptible([this] {
    pg->get_recovery_handler()->dispatch_backfill_event(std::move(evt));
    return handle.complete();
  }).then_interruptible([] {
    return seastar::make_ready_future<bool>(false);
  }).finally([this] {
    handle.exit();
  });
}

template class BackgroundRecoveryT<UrgentRecovery>;
template class BackgroundRecoveryT<PglogBasedRecovery>;
template class BackgroundRecoveryT<BackfillRecovery>;

} // namespace crimson::osd
