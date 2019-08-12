#include "ec_backend.h"

#include "crimson/os/cyan_collection.h"
#include "crimson/osd/shard_services.h"

ECBackend::ECBackend(shard_id_t shard,
                     ECBackend::CollectionRef coll,
                     ceph::osd::ShardServices& shard_services,
                     const ec_profile_t&,
                     uint64_t)
  : PGBackend{shard, coll, &shard_services.get_store()}
{
  // todo
}

seastar::future<bufferlist> ECBackend::_read(const hobject_t& hoid,
                                             uint64_t off,
                                             uint64_t len,
                                             uint32_t flags)
{
  // todo
  return seastar::make_ready_future<bufferlist>();
}

seastar::future<ceph::osd::acked_peers_t>
ECBackend::_submit_transaction(std::set<pg_shard_t>&& pg_shards,
                               const hobject_t& hoid,
                               ceph::os::Transaction&& txn,
                               osd_reqid_t req_id,
                               epoch_t min_epoch, epoch_t max_epoch,
                               eversion_t ver)
{
  // todo
  return seastar::make_ready_future<ceph::osd::acked_peers_t>();
}
