// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd_operation.h"
#include "common/Formatter.h"
#include "crimson/common/log.h"
#include "crimson/osd/osd_operation_tracking.h"

namespace crimson::osd {

void HistoricBackend::handle(ClientRequest::DoneEvent&, const Operation& op)
{
  // TODO: static_cast for production builds
  const auto& client_request = dynamic_cast<const ClientRequest&>(op);
  auto& main_registry = client_request.osd.get_shard_services().registry;
  // create a historic op and release the smart pointer. It will be
  // re-acquired (via the historic registry) when it's the purge time.
  main_registry.create_operation<HistoricClientRequest>(
    ClientRequest::ICRef(&client_request, /* add_ref= */true)
  ).detach();

  const auto historic_op_registry_max_size =
    local_conf().get_val<std::size_t>("osd_op_history_size");

  // check whether the history size limit is not exceeded; if so, then
  // purge the oldest op.
  // NOTE: Operation uses the auto-unlink feature of boost::intrusive.
  constexpr auto historic_registry_index =
    static_cast<int>(OperationTypeCode::historic_client_request);
  const auto& historic_registry =
    main_registry.registries[historic_registry_index];
  if (historic_registry.size() > historic_op_registry_max_size) {
    const auto& oldest_historic_op =
      static_cast<const HistoricClientRequest&>(historic_registry.front());
    HistoricClientRequest::ICRef(&oldest_historic_op, /* add_ref= */false);
  }
}

}
