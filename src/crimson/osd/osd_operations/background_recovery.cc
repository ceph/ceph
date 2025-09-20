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
    return seastar::repeat([ref, this] {
      return interruptor::with_interruption([this] {
       return do_recovery();
      }, [](std::exception_ptr) {
       return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
      }, pg, epoch_started);
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

UrgentRecovery::interruptible_future<seastar::stop_iteration>
UrgentRecovery::do_recovery()
{
  LOG_PREFIX(UrgentRecovery::do_recovery);
  DEBUGDPPI("{}: {}", *pg, __func__, *this);
  if (pg->has_reset_since(epoch_started)) {
    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
  }

  return pg->find_unfound(epoch_started
  ).then_interruptible([this] {
    return with_blocking_event<RecoveryBackend::RecoveryBlockingEvent,
			       interruptor>([this] (auto&& trigger) {
      return pg->get_recovery_handler()->recover_missing(
	trigger, soid, need, false);
    }).then_interruptible([] {
      return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
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

PglogBasedRecovery::interruptible_future<seastar::stop_iteration>
PglogBasedRecovery::do_recovery()
{
  LOG_PREFIX(PglogBasedRecovery::do_recovery);
  DEBUGDPPI("{}: {}", *pg, __func__, *this);
  if (pg->has_reset_since(epoch_started)) {
    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
  }
  return pg->find_unfound(epoch_started
  ).then_interruptible([this] {
    return with_blocking_event<RecoveryBackend::RecoveryBlockingEvent,
			       interruptor>([this] (auto&& trigger) {
      return pg->get_recovery_handler()->start_recovery_ops(
	trigger,
	*this,
	crimson::common::local_conf()->osd_recovery_max_single_start);
    });
  });
}

template class BackgroundRecoveryT<UrgentRecovery>;
template class BackgroundRecoveryT<PglogBasedRecovery>;

} // namespace crimson::osd
