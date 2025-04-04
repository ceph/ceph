// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDRepOpReply.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class ShardServices;

class OSD;
class PG;

class ReplicatedRequestReply final
  : public OperationT<ReplicatedRequestReply>,
    public RemoteOperation
{
public:
  static constexpr OperationTypeCode type =
    OperationTypeCode::replicated_request_reply;
  ReplicatedRequestReply(crimson::net::ConnectionRef&&, Ref<MOSDRepOpReply>&&);

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;

  spg_t get_pgid() const {
    return req->get_spg();
  }

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

  PipelineHandle &get_handle() { return handle; }
private:
  PipelineHandle handle;
  Ref<MOSDRepOpReply> req;
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::ReplicatedRequestReply> : fmt::ostream_formatter {};
#endif
