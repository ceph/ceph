#include "ec_backend.h"

#include "crimson/osd/shard_services.h"

ECBackend::ECBackend(shard_id_t shard,
                     ECBackend::CollectionRef coll,
                     crimson::osd::ShardServices& shard_services,
                     const ec_profile_t&,
                     uint64_t)
  : PGBackend{shard, coll, &shard_services.get_store()}
{
  // todo
}

ECBackend::ll_read_errorator::future<ceph::bufferlist>
ECBackend::_read(const hobject_t& hoid,
                 const uint64_t off,
                 const uint64_t len,
                 const uint32_t flags)
{
  // todo
  return seastar::make_ready_future<bufferlist>();
}

seastar::future<crimson::osd::acked_peers_t>
ECBackend::_submit_transaction(std::set<pg_shard_t>&& pg_shards,
                               const hobject_t& hoid,
                               ceph::os::Transaction&& txn,
                               const osd_op_params_t& osd_op_p,
                               epoch_t min_epoch, epoch_t max_epoch,
			       std::vector<pg_log_entry_t>&& log_entries)
{
  // todo
  return seastar::make_ready_future<crimson::osd::acked_peers_t>();
}
