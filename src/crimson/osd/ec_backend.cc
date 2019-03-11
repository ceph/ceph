#include "ec_backend.h"
#include "crimson/os/cyan_collection.h"

ECBackend::ECBackend(shard_id_t shard,
                     ECBackend::CollectionRef coll,
                     ceph::os::CyanStore* store,
                     const ec_profile_t&,
                     uint64_t)
  : PGBackend{shard, coll, store}
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
