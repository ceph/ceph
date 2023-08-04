// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <seastar/core/future.hh>
#include "erasure-code/ErasureCodeInterface.h"
#include "include/buffer_fwd.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "osd/ECUtil.h"
#include "osd/osd_types.h"
#include "pg_backend.h"

class ECBackend : public PGBackend
{
public:
  ECBackend(pg_shard_t whoami,
	    CollectionRef coll,
	    crimson::osd::ShardServices& shard_services,
	    const ec_profile_t& ec_profile,
	    uint64_t stripe_width,
	    bool fast_read,
	    bool allows_ecoverwrites,
	    DoutPrefixProvider &dpp);
  seastar::future<> stop() final {
    return seastar::now();
  }
  void on_actingset_changed(bool same_primary) final {}

  write_iertr::future<> handle_rep_write_op(Ref<MOSDECSubOpWrite>);
  write_iertr::future<> handle_rep_write_reply(Ref<MOSDECSubOpWriteReply>);
  ll_read_ierrorator::future<> handle_rep_read_op(Ref<MOSDECSubOpRead>);
  ll_read_ierrorator::future<> handle_rep_read_reply(Ref<MOSDECSubOpReadReply>);
private:
  ll_read_ierrorator::future<ceph::bufferlist>
  _read(const hobject_t& hoid, uint64_t off, uint64_t len, uint32_t flags) override;
  rep_op_fut_t
  _submit_transaction(std::set<pg_shard_t>&& pg_shards,
		      const hobject_t& hoid,
		      ceph::os::Transaction&& txn,
		      osd_op_params_t&& req,
		      epoch_t min_epoch, epoch_t max_epoch,
		      std::vector<pg_log_entry_t>&& log_entries) final;
  CollectionRef coll;
  seastar::future<> request_committed(const osd_reqid_t& reqid,
				       const eversion_t& version) final {
    return seastar::now();
  }

  bool is_single_chunk(const hobject_t& obj, const ECSubRead& op);

  ll_read_errorator::future<ceph::bufferlist> maybe_chunked_read(
    const hobject_t& obj,
    const ECSubRead& op,
    std::uint64_t off,
    std::uint64_t size,
    std::uint32_t flags);

  ceph::ErasureCodeInterfaceRef ec_impl;
  const ECUtil::stripe_info_t sinfo;

  const bool fast_read;
  const bool allows_ecoverwrites;
};
