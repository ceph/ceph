// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

#include "msg/MessageRef.h"

#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"

namespace crimson::osd {

class OSD;
class PG;

using osd_id_t = int;

class CompoundPeeringRequest : public TrackableOperationT<CompoundPeeringRequest> {
  friend class LttngBackendCompoundPeering;
public:
  static constexpr OperationTypeCode type =
    OperationTypeCode::compound_peering_request;

  struct SubOpBlocker : crimson::BlockerT<SubOpBlocker> {
    static constexpr const char * type_name = "CompoundOpBlocker";

    std::vector<crimson::OperationRef> subops;
    SubOpBlocker(std::vector<crimson::OperationRef> &&subops)
      : subops(subops) {}

    virtual void dump_detail(Formatter *f) const {
      f->open_array_section("dependent_operations");
      {
        for (auto &i : subops) {
          i->dump_brief(f);
        }
      }
      f->close_section();
    }
  };

private:
  OSD &osd;
  crimson::net::ConnectionRef conn;
  Ref<Message> m;

public:
  CompoundPeeringRequest(
    OSD &osd, crimson::net::ConnectionRef conn, Ref<Message> m);

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;
  seastar::future<> start();

  std::tuple<
    StartEvent,
    SubOpBlocker::BlockingEvent,
    CompletionEvent
  > tracking_events;
};

}
