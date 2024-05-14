// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/osd_operations/common/pg_pipeline.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_activation_blocker.h"

namespace crimson::osd {

class InternalClientRequest : public PhasedOperationT<InternalClientRequest>,
                              private CommonClientRequest {
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

  seastar::future<> do_process();

  Ref<PG> pg;
  OpInfo op_info;
  PipelineHandle handle;

public:
  PipelineHandle& get_handle() { return handle; }

  std::tuple<
    StartEvent,
    CommonPGPipeline::WaitForActive::BlockingEvent,
    PGActivationBlocker::BlockingEvent,
    CommonPGPipeline::RecoverMissing::BlockingEvent,
    CommonPGPipeline::GetOBC::BlockingEvent,
    CommonPGPipeline::LockOBC::BlockingEvent,
    CommonPGPipeline::Process::BlockingEvent,
    CompletionEvent
  > tracking_events;
};

} // namespace crimson::osd

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::InternalClientRequest> : fmt::ostream_formatter {};
#endif
