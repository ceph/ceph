// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <seastar/core/future.hh>
#include "erasure-code/ErasureCodeInterface.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "include/buffer_fwd.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "osd/ECCommon.h"
#include "osd/ECUtil.h"
#include "osd/osd_types.h"
#include "pg_backend.h"

namespace crimson::osd {

class PG;

class ECBackend : public PGBackend,
		  public ECCommon
{
  static ceph::ErasureCodeInterfaceRef create_ec_impl(
    const ec_profile_t& ec_profile);

public:
  ECBackend(pg_shard_t whoami,
	    CollectionRef coll,
	    crimson::osd::ShardServices& shard_services,
	    const ec_profile_t& ec_profile,
	    uint64_t stripe_width,
	    bool fast_read,
	    bool allows_ecoverwrites,
	    DoutPrefixProvider &dpp,
	    ECListener &eclistener);
  seastar::future<> stop() final {
    return seastar::now();
  }
  void on_actingset_changed(bool same_primary) final {}

  write_iertr::future<> handle_rep_write_op(
    Ref<MOSDECSubOpWrite>,
    crimson::osd::PG& pg);
  write_iertr::future<> handle_rep_write_reply(ECSubWriteReply&& op);
  ll_read_ierrorator::future<ECSubReadReply> handle_rep_read_op(ECSubRead&);
  ll_read_ierrorator::future<ECSubReadReply> handle_rep_read_op(Ref<MOSDECSubOpRead>);
  ll_read_ierrorator::future<> handle_rep_read_reply(ECSubReadReply& mop);
  ll_read_ierrorator::future<> handle_rep_read_reply(Ref<MOSDECSubOpReadReply>);

private:
  friend class ECRecoveryBackend;

  ll_read_ierrorator::future<ceph::bufferlist>
  _read(const hobject_t& hoid,
        uint64_t object_size,
        uint64_t off,
        uint64_t len,
        uint32_t flags) final;
  rep_op_fut_t
  submit_transaction(const std::set<pg_shard_t> &pg_shards,
		     crimson::osd::ObjectContextRef&& obc,
		     crimson::osd::ObjectContextRef&& new_clone,
		     ceph::os::Transaction&& txn,
		     osd_op_params_t&& req,
		     epoch_t min_epoch, epoch_t max_epoch,
		     std::vector<pg_log_entry_t>&& log_entries) final;
  seastar::future<> request_committed(const osd_reqid_t& reqid,
				       const eversion_t& version) final {
    return seastar::now();
  }

  write_iertr::future<> handle_sub_write(
    pg_shard_t from,
    ECSubWrite&& op,
    ECListener& pg);

  void handle_sub_write(
    pg_shard_t from,
    OpRequestRef msg,
    ECSubWrite &op,
    const ZTracer::Trace &trace,
    ECListener& eclistener) override;

  void handle_sub_read_n_reply(
    pg_shard_t from,
    ECSubRead &op,
    const ZTracer::Trace &trace) override;

  bool is_single_chunk(const hobject_t& obj, const ECSubRead& op);

  ll_read_errorator::future<ceph::bufferlist> maybe_chunked_read(
    const hobject_t& obj,
    const ECSubRead& op,
    std::uint64_t off,
    std::uint64_t size,
    std::uint32_t flags);

  void objects_read_and_reconstruct(
    const std::map<hobject_t, std::list<ec_align_t>> &reads,
    bool fast_read,
    uint64_t object_size,
    GenContextURef<ec_extents_t &&> &&func) override;

  void objects_read_and_reconstruct_for_rmw(
    std::map<hobject_t, read_request_t> &&to_read,
    GenContextURef<ec_extents_t&&> &&func) override;

  ceph::ErasureCodeInterfaceRef ec_impl;
  const ECUtil::stripe_info_t sinfo;

  const bool fast_read;
  const bool allows_ecoverwrites;

  ECCommon::ReadPipeline read_pipeline;
  ECCommon::RMWPipeline rmw_pipeline;
};

}
