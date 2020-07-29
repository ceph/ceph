// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"

namespace crimson::osd {

class OSD;
class PG;

class RecoverySubRequest final : public OperationT<RecoverySubRequest> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::background_recovery_sub;

  RecoverySubRequest(OSD &osd, crimson::net::ConnectionRef conn, Ref<MOSDFastDispatchOp>&& m)
    : osd(osd), conn(conn), m(m) {}

  void print(std::ostream& out) const final
  {
    out << *m;
  }

  void dump_detail(Formatter *f) const final
  {
  }

  seastar::future<> start();
private:
  OSD& osd;
  crimson::net::ConnectionRef conn;
  Ref<MOSDFastDispatchOp> m;
};

}
