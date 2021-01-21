// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <typeinfo>
#include <vector>

#include <optional>
#include <seastar/core/future.hh>

#include "osd/osd_types.h"
#include "include/uuid.h"

#include "os/Transaction.h"
#include "crimson/os/futurized_store.h"
#include "transaction.h"
#include "wal.h"
#include "cache.h"

namespace crimson::os::poseidonstore {

/**
 *
 * PoseidonStore write processing
 *
 * 1) Thread architecture and lock domain
 *
 * Poseidonstore aims to split a partition into several partitions to maximize parallelism based on Crimson, 
 * but the number of partitionss is smaller than the number of PGs in an OSD (PGs >= Partitions). 
 * So, the reactor threads, which handle I/Os based on per-PG basis, inevitabely access to the partition 
 * at the same time. The following figure  shows the example.
 *
 *
 * (PG 1) reactor thread  (PG 2) reactor thread   (PG 3) reactor thread
 *      |                  |----------------- |
 *      ----------------------|
 *           V-------------|  V
 * ------------------------------------
 * | partition 1    || partition 2    |
 * | (PG 2, 3)      || (PG 1)         |
 * ====================================
 * | WAL            || WAL            |
 * ====================================
 * | Data partition || Data partition |
 * ------------------------------------
 * partion 1 means a sharded partition
 * 
 * ** Discussion
 * The best way to achieve high IOPS is that each sharded service (reactor thread) manages only 
 * a sharded partition. With this policy; each sharded partion handles pre-configured PGs,
 * if the number of PGs grows, the number of sharded parition can also increase. However, what happens if
 * the number of sharded services (reactor thread) grows or shrinks with the same PGs? 
 * Do we need to consider this case?
 * To be conservative, the following design assumes that we need a lock to protect two reactor thread
 * access a shared partition at a time, considering such case.
 * 
 *
 * As shown in the figure as above,
 * as a result, at least, a lock in each paritition is necessary to preserve the I/O ordering.
 * Even poseidonstore is desigend to obtain the parallelism by spliting a single partition,
 * tremendous I/Os headed for a partition can be coming because the environment we target is NVMe 
 * or more faster devices.
 * So, we need a careful thread and lock structure here. 
 * To minimize lock contention, we propose a lock-free structure on I/O processing;
 * each WAL manages a list to record incoming data sequentially with CAS operation 
 * as shown in the following figure.
 * (we refer to this paper: https://www.usenix.org/conference/fast18/presentation/son)
 *
 * 
 *      V--- reactor thread will insert a record by using CAS
 *
 * | record 1 | --CAS--> | record 2 | --CAS--> | record 3 | .. (in-memory)
 *      |
 *      |--- reactor thread issues a write request to WAL
 *      v
 *  || WAL ||  --flush-->  || Data partition ||
 *             ^
 *             flush can be done by reactor thread later
 *
 *
 * The record as above should be inserted first by reactor thread before the commit. After that, the reactor thread
 * completes a write operation to WAL (the state is committed). Then, the records need to be
 * flushed to the data partitions. 
 *
 * We introduce a cooperative control here to handle transaction with low overhead; the commit and flush 
 * can be done by the reactor thread itself.
 * Briefly, the reactor thread checks whether flush needs to be triggred after requesting submitting transaction to WAL.
 * If needed, it issues a flush operation. If not, it just skip the flush operation.
 * All these hehaviors can be done by an reactor thread with exploiting lock-free operations (e.g., insert and delete).
 *
 *
 * 2) Tasks in transaction processing (write)
 *
 * a. Reserve data partition area to make a room for the object when hanling both large and small write
 *
 * b. Build records from a transaction
 *
 * c. Write data to data partition first if the data is large
 *
 * d. Flush if ther are committed entries 
 *    e-1. Do post works asynchronously (e.g., flush and completion, which requires applied state if necessary)
 *    e-2. Remove the transaction from the list 
 *
 * e. Add the transaction to the (lock-free) list
 *
 * f. Submit the transaction to make it committed (after committing the data to WAL with holding the pg lock, poseidonstore yield control)
 *    f-1. Call callback to notify completion state to upper layer (e.g., ack to the client)
 *    f-2. Make the transaction's state in the list COMMIITED
 *
 *
 * The procedure as above runs in an reactor thread context with holding a PG lock, which means isolation is guaranteed.
 * a ~ d are executed in synchronous manner, but d ~ f can run in parallel like as below.
 *
 * do_transaction() {
 * 	a();
 * 	b();
 * 	c();
 * 	if(the list has committed item) d().then({ finish applied }) <- consumer of the lock-firee list
 * 	e(); <- producer of the lock-free list
 * 	f().then({ finish commit })
 * }
 *
 * With holding the PG lock, each reactor thread finishes the transaction write within its own contenxt; do_transaction 
 * will yield control after at least f() is completed. 
 * Also, without separated flush thread, each reactor thread try to flush committed item if necessary 
 * whenever do_transaction() is called.
 *
 * As a reuslt, each reactor thread can issue a write request in parallel as belows.
 *
 *    reactor thread 1  reactor thread 2  reactor thread 3
 *         |            |          |
 *         |            |          |
 *         v            v          v
 * WAL |  RECORD 1 | RECORD 2 | RECORD 3| ...
 *
 */

class PoseidonStore final : public FuturizedStore {
  uuid_d osd_fsid;

public:

  // for test
  PoseidonStore(const std::string& path);
  ~PoseidonStore() final;

  seastar::future<> stop() final;
  seastar::future<> mount() final;
  seastar::future<> umount() final;

  seastar::future<> mkfs(uuid_d new_osd_fsid) final;
  seastar::future<store_statfs_t> stat() const final;

  read_errorator::future<ceph::bufferlist> read(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    uint32_t op_flags = 0) final;
  read_errorator::future<ceph::bufferlist> readv(
    CollectionRef c,
    const ghobject_t& oid,
    interval_set<uint64_t>& m,
    uint32_t op_flags = 0) final;
  get_attr_errorator::future<ceph::bufferptr> get_attr(
    CollectionRef c,
    const ghobject_t& oid,
    std::string_view name) const final;
  get_attrs_ertr::future<attrs_t> get_attrs(
    CollectionRef c,
    const ghobject_t& oid) final;

  seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) final;

  read_errorator::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    const omap_keys_t& keys) final;

  /// Retrieves paged set of values > start (if present)
  read_errorator::future<std::tuple<bool, omap_values_t>> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) final; ///< @return <done, values> values.empty() iff done

  read_errorator::future<bufferlist> omap_get_header(
    CollectionRef c,
    const ghobject_t& oid) final;

  seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) const final;

  seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;
  seastar::future<CollectionRef> open_collection(const coll_t& cid) final;
  seastar::future<std::vector<coll_t>> list_collections() final;

  seastar::future<> do_transaction(
    CollectionRef ch,
    ceph::os::Transaction&& txn) final;

  seastar::future<OmapIteratorRef> get_omap_iterator(
    CollectionRef ch,
    const ghobject_t& oid) final;
  seastar::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef ch,
    const ghobject_t& oid,
    uint64_t off,
    uint64_t len) final;

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) final;
  seastar::future<std::tuple<int, std::string>> read_meta(const std::string& key) final;
  uuid_d get_fsid() const final;

  unsigned get_max_attr_name_length() const final {
    return 256;
  }
private:
  std::unique_ptr<DeviceManager> device_manager;
  WAL wal;
  Cache cache;
};

}
