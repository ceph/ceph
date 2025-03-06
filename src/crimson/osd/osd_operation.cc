// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd_operation.h"
#include "common/Formatter.h"
#include "crimson/common/log.h"
#include "crimson/osd/osd_operations/client_request.h"

using namespace std::string_literals;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

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

OperationThrottler::OperationThrottler(ConfigProxy &conf)
  : scheduler(crimson::osd::scheduler::make_scheduler(conf))
{
  conf.add_observer(this);
  update_from_config(conf);
}

void OperationThrottler::wake()
{
  while ((!max_in_progress || in_progress < max_in_progress) &&
	 !scheduler->empty()) {
    auto item = scheduler->dequeue();
    item.wake.set_value();
    ++in_progress;
    --pending;
  }
}

void OperationThrottler::release_throttle()
{
  ceph_assert(in_progress > 0);
  --in_progress;
  wake();
}

seastar::future<> OperationThrottler::acquire_throttle(
  crimson::osd::scheduler::params_t params)
{
  crimson::osd::scheduler::item_t item{params, seastar::promise<>()};
  auto fut = item.wake.get_future();
  scheduler->enqueue(std::move(item));
  wake();
  return fut;
}

void OperationThrottler::dump_detail(Formatter *f) const
{
  f->dump_unsigned("max_in_progress", max_in_progress);
  f->dump_unsigned("in_progress", in_progress);
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
