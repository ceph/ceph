#include "replicated_backend.h"

#include "crimson/os/cyan_collection.h"
#include "crimson/os/cyan_object.h"
#include "crimson/os/cyan_store.h"

ReplicatedBackend::ReplicatedBackend(shard_id_t shard,
                                     ReplicatedBackend::CollectionRef coll,
                                     ceph::os::CyanStore* store)
  : PGBackend{shard, coll, store}
{}

seastar::future<bufferlist> ReplicatedBackend::_read(const hobject_t& hoid,
                                                     uint64_t off,
                                                     uint64_t len,
                                                     uint32_t flags)
{
  return store->read(coll, ghobject_t{hoid}, off, len, flags);
}
