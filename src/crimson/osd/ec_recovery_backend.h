// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "crimson/common/type_helpers.h"
#include "crimson/os/futurized_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/recovery_backend.h"
#include "crimson/osd/shard_services.h"


#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGBackfillRemove.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGScan.h"
#include "osd/recovery_types.h"
#include "osd/osd_types.h"

namespace crimson::osd{
  class PG;
}

class PGBackend;

class ECRecoveryBackend : public RecoveryBackend {
public:
  ECRecoveryBackend(crimson::osd::PG& pg,
		    crimson::osd::ShardServices& shard_services,
		    crimson::os::CollectionRef coll,
		    PGBackend* backend)
    : RecoveryBackend(pg, shard_services, coll, backend)
  {}

  interruptible_future<> handle_recovery_op(
    Ref<MOSDFastDispatchOp> m,
    crimson::net::ConnectionXcoreRef conn) final;

  interruptible_future<> recover_object(
    const hobject_t& soid,
    eversion_t need) final;

  seastar::future<> on_stop() final {
    return seastar::now();
  }

private:
  interruptible_future<> handle_push(
    Ref<MOSDPGPush> m);
  interruptible_future<> handle_push_reply(
    Ref<MOSDPGPushReply> m);
};
