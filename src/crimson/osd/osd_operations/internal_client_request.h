// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/type_helpers.h"
#include "crimson/osd/object_context_loader.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_activation_blocker.h"

namespace crimson::osd {

class InternalClientRequest : public PhasedOperationT<InternalClientRequest> {
public:
  explicit InternalClientRequest(Ref<PG> pg);
  ~InternalClientRequest();

  // imposed by `ShardService::start_operation<T>(...)`.
  seastar::future<> start();

protected:
  virtual const hobject_t& get_target_oid() const = 0;
  virtual PG::do_osd_ops_params_t get_do_osd_ops_params() const = 0;
  virtual std::vector<OSDOp> create_osd_ops() = 0;

  const PG& get_pg() const {
    return *pg;
  }

private:
  friend OperationT<InternalClientRequest>;

  static constexpr OperationTypeCode type =
    OperationTypeCode::internal_client_request;

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;

  CommonPGPipeline& client_pp();

  InternalClientRequest::interruptible_future<> with_interruption();
  InternalClientRequest::interruptible_future<> do_process(
    crimson::osd::ObjectContextRef obc,
    std::vector<OSDOp> &osd_ops);

  Ref<PG> pg;
  epoch_t start_epoch;
  OpInfo op_info;
  std::optional<ObjectContextLoader::Orderer> obc_orderer;
  PipelineHandle handle;

public:
  PipelineHandle& get_handle() { return handle; }

  std::tuple<
    StartEvent,
    CommonOBCPipeline::Process::BlockingEvent,
    CommonOBCPipeline::WaitRepop::BlockingEvent,
    CompletionEvent
  > tracking_events;
};

} // namespace crimson::osd

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::InternalClientRequest> : fmt::ostream_formatter {};
#endif
