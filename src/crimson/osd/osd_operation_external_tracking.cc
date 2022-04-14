// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/config.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/osd_operation_external_tracking.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

void HistoricBackend::handle(ClientRequest::CompletionEvent&,
                             const Operation& op)
{
  // early exit if the history is disabled
  using crimson::common::local_conf;
  if (!local_conf()->osd_op_history_size) {
    return;
  }

#ifdef NDEBUG
  const auto& client_request = static_cast<const ClientRequest&>(op);
#else
  const auto& client_request = dynamic_cast<const ClientRequest&>(op);
#endif
  auto& main_registry = client_request.osd.get_shard_services().registry;

  // unlink the op from the client request registry. this is a part of
  // the re-link procedure. finally it will be in historic registry.
  constexpr auto client_reg_index =
    static_cast<size_t>(OperationTypeCode::client_request);
  constexpr auto historic_reg_index =
    static_cast<size_t>(OperationTypeCode::historic_client_request);
  auto& client_registry = main_registry.get_registry<client_reg_index>();
  auto& historic_registry = main_registry.get_registry<historic_reg_index>();

  historic_registry.splice(std::end(historic_registry),
			   client_registry,
			   client_registry.iterator_to(client_request));
  ClientRequest::ICRef(
    &client_request, /* add_ref= */true
  ).detach(); // yes, "leak" it for now!

  // check whether the history size limit is not exceeded; if so, then
  // purge the oldest op.
  // NOTE: Operation uses the auto-unlink feature of boost::intrusive.
  // NOTE: the cleaning happens in OSDOperationRegistry::do_stop()
  if (historic_registry.size() > local_conf()->osd_op_history_size) {
    const auto& oldest_historic_op =
      static_cast<const ClientRequest&>(historic_registry.front());
    // clear a previously "leaked" op
    ClientRequest::ICRef(&oldest_historic_op, /* add_ref= */false);
  }
}

} // namespace crimson::osd
