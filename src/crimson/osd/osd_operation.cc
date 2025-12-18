// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "osd_operation.h"
#include "common/Formatter.h"
#include "crimson/common/log.h"
#include "crimson/osd/osd_operations/client_request.h"

using namespace std::string_literals;
SET_SUBSYS(osd);

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using namespace crimson::osd::scheduler;
namespace crimson::osd {

void OSDOperationRegistry::do_stop()
{
  logger().info("OSDOperationRegistry::{}", __func__);
  // we need to decouple visiting the registry from destructing
  // ops because of the auto-unlink feature of boost::intrusive.
  // the list shouldn't change while iterating due to constrains
  // on iterator's validity.
  constexpr auto historic_reg_index =
    static_cast<size_t>(OperationTypeCode::historic_client_request);
  auto& historic_registry = get_registry<historic_reg_index>();
  std::vector<ClientRequest::ICRef> to_ref_down;
  std::transform(std::begin(historic_registry), std::end(historic_registry),
		 std::back_inserter(to_ref_down),
		 [] (const Operation& op) {
		   return ClientRequest::ICRef{
		     static_cast<const ClientRequest*>(&op),
		     /* add_ref= */ false
		   };
		 });
  // to_ref_down is going off
}

OSDOperationRegistry::OSDOperationRegistry()
  : OperationRegistryT(seastar::this_shard_id()) {}

static auto get_duration(const ClientRequest& client_request)
{
  // TODO: consider enhancing `CompletionEvent` with computing duration
  // once -- when it's enetered.
  return client_request.get_completed() - client_request.get_started();
}

void OSDOperationRegistry::put_historic(const ClientRequest& op)
{
  using crimson::common::local_conf;
  // unlink the op from the client request registry. this is a part of
  // the re-link procedure. finally it will be in historic/historic_slow registry.
  constexpr auto historic_reg_index =
    static_cast<size_t>(OperationTypeCode::historic_client_request);
  constexpr auto slow_historic_reg_index = 
    static_cast<size_t>(OperationTypeCode::historic_slow_client_request);

  if (get_duration(op) > local_conf()->osd_op_complaint_time) {
    auto& slow_historic_registry = get_registry<slow_historic_reg_index>();
    _put_historic(slow_historic_registry,
      op,
      local_conf()->osd_op_history_slow_op_size);
  } else {
    auto& historic_registry = get_registry<historic_reg_index>();
    _put_historic(historic_registry,
      op,
      local_conf()->osd_op_history_size);
  }
}

void OSDOperationRegistry::_put_historic(
  op_list& list,
  const class ClientRequest& op,
  uint64_t max)
{
  constexpr auto client_reg_index =
    static_cast<size_t>(OperationTypeCode::client_request);
  auto& client_registry = get_registry<client_reg_index>();

  // we only save the newest op
  list.splice(std::end(list), client_registry, client_registry.iterator_to(op));
  ClientRequest::ICRef(
      &op, /* add_ref= */true
    ).detach(); // yes, "leak" it for now!

  if (list.size() >= max) {
    auto old_op_ptr = &list.front();
    list.pop_front();
    const auto& old_op =
      static_cast<const ClientRequest&>(*old_op_ptr);
    // clear a previously "leaked" op
    ClientRequest::ICRef(&old_op, /* add_ref= */false);
  }
}

size_t OSDOperationRegistry::dump_historic_client_requests(ceph::Formatter* f) const
{
  const auto& historic_client_registry =
    get_registry<static_cast<size_t>(OperationTypeCode::historic_client_request)>(); //ClientRequest::type)>();
  f->open_object_section("op_history");
  f->dump_int("size", historic_client_registry.size());
  // TODO: f->dump_int("duration", history_duration.load());
  // the intrusive list is configured to not store the size
  size_t ops_count = 0;
  {
    f->open_array_section("ops");
    for (const auto& op : historic_client_registry) {
      op.dump(f);
      ++ops_count;
    }
    f->close_section();
  }
  f->close_section();
  return ops_count;
}

size_t OSDOperationRegistry::dump_slowest_historic_client_requests(ceph::Formatter* f) const
{
  const auto& slow_historic_client_registry =
    get_registry<static_cast<size_t>(OperationTypeCode::historic_slow_client_request)>(); //ClientRequest::type)>();
  f->open_object_section("op_history");
  f->dump_int("size", slow_historic_client_registry.size());
  // TODO: f->dump_int("duration", history_duration.load());
  // the intrusive list is configured to not store the size
  size_t ops_count = 0;
  {
    f->open_array_section("ops");
    for (const auto& op : slow_historic_client_registry) {
      op.dump(f);
      ++ops_count;
    }
    f->close_section();
  }
  f->close_section();
  return ops_count;
}

void OSDOperationRegistry::visit_ops_in_flight(std::function<void(const ClientRequest&)>&& visit)
{
  const auto& client_registry =
    get_registry<static_cast<size_t>(OperationTypeCode::client_request)>();
  auto it = std::begin(client_registry);
  for (; it != std::end(client_registry); ++it) {
    const auto& fastest_historic_op = static_cast<const ClientRequest&>(*it);
    visit(fastest_historic_op);
  }
}

void OperationThrottler::start()
{
  LOG_PREFIX(OperationThrottler::start);
  if (started) {
    DEBUG("OperationThrottler background task is already started, skipping.");
    return;
  }

  started=true;
  stopped=false;

  INFO("Starting OperationThrottler background task");
  bg_future.emplace(background_task());
  return;
}

OperationThrottler::OperationThrottler(ConfigProxy &conf)
{
  conf.add_observer(this);
}

void OperationThrottler::initialize_scheduler(CephContext *cct, ConfigProxy &conf, bool is_rotational, int whoami)
{
  scheduler = crimson::osd::scheduler::make_scheduler(cct, conf, whoami, seastar::smp::count,
            seastar::this_shard_id(), is_rotational, true);
  update_from_config(conf);
}

seastar::future<> OperationThrottler::background_task() {
  LOG_PREFIX(OperationThrottler::background_task);
  while (!stopped) {
    co_await cv.wait([this] {
      return (available() && !scheduler->empty()) || stopped;
    });

    // It might be possible as mclock scheduler can return a timestamp in double means
    // the work item is scheduled in the future, so in that case wait until
    // the returned timestamp in the dequeue response before retrying.
    while (available() && !scheduler->empty() && !stopped) {
      WorkItem work_item = scheduler->dequeue();
      if (auto when_ready = std::get_if<double>(&work_item)) {
        ceph::real_clock::time_point future_time = ceph::real_clock::from_double(*when_ready);
        auto now = ceph::real_clock::now();
        ceph_assert(future_time > now);
        auto wait_duration = std::chrono::duration_cast<std::chrono::milliseconds>(future_time - now);
        INFO("No items ready. Retrying in {} ms", wait_duration.count());
        co_await seastar::sleep(wait_duration);
        continue;
      }
      if (auto *item = std::get_if<crimson::osd::scheduler::item_t>(&work_item)) {
        DEBUG("Waking up a work item");
        item->wake.set_value();
        ++in_progress;
        --pending;
        DEBUG("Updated counters during background_task: in_progress={}, pending={}", in_progress, pending);
      } else {
        DEBUG("Unexpected variant in WorkItem — neither time nor item");
      }
    }
  }

  DEBUG("Background task exiting cleanly");
  co_return;
}

void OperationThrottler::wake() {
  // Attempt to wakeup pending operation if resources are available.
  // The mclock scheduler might return delay if item is not ready
  // to process
  cv.signal();
}

void OperationThrottler::release_throttle()
{
  LOG_PREFIX(OperationThrottler::release_throttle);
  ceph_assert(in_progress > 0);
  --in_progress;
  DEBUG("Updated counters during release_throttle: in_progress={}, pending={}",
        in_progress, pending);
  wake();
}

seastar::future<> OperationThrottler::acquire_throttle(
  crimson::osd::scheduler::params_t params)
{
  crimson::osd::scheduler::item_t item{params, seastar::promise<>()};
  auto fut = item.wake.get_future();
  scheduler->enqueue(std::move(item));
  ++pending;
  wake();
  return fut;
}

seastar::future<> OperationThrottler::stop()
{
  if (!started)
    co_return;

  stopped = true;
  cv.broadcast();

  if (bg_future && !bg_future->available()) {
    co_await std::move(*bg_future);
  }

  bg_future.reset();
  started = false;

  co_return;
}

void OperationThrottler::dump_detail(Formatter *f) const
{
  f->dump_unsigned("max_in_progress", max_in_progress);
  f->dump_unsigned("in_progress", in_progress);
  f->dump_unsigned("pending", pending);
  f->dump_unsigned("background_task started", started);
  f->dump_unsigned("background_task stopeed", stopped);

  f->open_object_section("scheduler");
  {
    scheduler->dump(*f);
  }
  f->close_section();
}

void OperationThrottler::update_from_config(const ConfigProxy &conf)
{
  max_in_progress = conf.get_val<uint64_t>("crimson_osd_scheduler_concurrency");
  wake();
}

std::vector<std::string> OperationThrottler::get_tracked_keys() const noexcept
{
  return {"crimson_osd_scheduler_concurrency"s};
}

void OperationThrottler::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed)
{
  update_from_config(conf);
}

}
