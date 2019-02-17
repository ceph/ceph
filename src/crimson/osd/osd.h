// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include "include/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>

#include "crimson/mon/MonClient.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/osd/chained_dispatchers.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/state.h"
#include "crimson/osd/opqueue_item.h"
#include "crimson/osd/op_request.h"
#include "crimson/osd/session.h"
#include "crimson/osd/session.h"
#include "crimson/osd/pg.h"
#include "common/OpQueue.h"
#include "common/WeightedPriorityQueue.h"
#include "common/PrioritizedQueue.h"
#include "msg/Message.h"

#include "osd/OSDMap.h"

class MOSDMap;
class OSDMap;
class OSDMeta;
class PG;
class Heartbeat;
class OSD;

namespace ceph::net {
  class Messenger;
}

namespace ceph::os {
  class CyanStore;
  struct Collection;
  class Transaction;
}

template<typename T> using Ref = boost::intrusive_ptr<T>;
enum class io_queue {
  prioritized,
  weightedpriority,
};

class ContextQueue {
  list<Context *> q;
public:
  ContextQueue(){}

  void queue(list<Context *>& ls) {
    {
      if (q.empty()) {
	q.swap(ls);
      } else {
	q.insert(q.end(), ls.begin(), ls.end());
      }
    }

    ls.clear();
  }

  void swap(list<Context *>& ls) {
    ls.clear();
    if (!q.empty()) {
      q.swap(ls);
    }
  }

  bool empty() {
    return q.empty();
  }
};

struct OSDShardPGSlot {
  PGRef pg;                      ///< pg reference
  deque<OpQueueItem> to_process; ///< order items for this slot
  int num_running = 0;          ///< _process threads doing pg lookup/lock

  deque<OpQueueItem> waiting;   ///< waiting for pg (or map + pg)

  /// waiting for map (peering evt)
  map<epoch_t,deque<OpQueueItem>> waiting_peering;

  /// incremented by wake_pg_waiters; indicates racing _process threads
  /// should bail out (their op has been requeued)
  uint64_t requeue_seq = 0;

  /// waiting for split child to materialize in these epoch(s)
  set<epoch_t> waiting_for_split;

  epoch_t epoch = 0;
  boost::intrusive::set_member_hook<> pg_epoch_item;

  /// waiting for a merge (source or target) by this epoch
  epoch_t waiting_for_merge_epoch = 0;
};

struct OSDShard {
  const unsigned shard_id;
  OSD *osd;
  OSDMapRef shard_osdmap;
  unordered_map<spg_t,unique_ptr<OSDShardPGSlot>> pg_slots;
  /// priority queue
  std::unique_ptr<OpQueue<OpQueueItem, uint64_t>> pqueue;
  ContextQueue context_queue;

  void _enqueue_front(OpQueueItem&& item, unsigned cutoff) {
    unsigned priority = item.get_priority();
    unsigned cost = item.get_cost();
    if (priority >= cutoff)
      pqueue->enqueue_strict_front(
	item.get_owner(),
	priority, std::move(item));
    else
      pqueue->enqueue_front(
	item.get_owner(),
	priority, cost, std::move(item));
  }
  OSDShard(
    int id,
    OSD *osd,
    uint64_t max_tok_per_prio, uint64_t min_cost,
    io_queue opqueue)
    : shard_id(id),
      osd(osd),
      context_queue() {
    if (opqueue == io_queue::weightedpriority) {
      pqueue = std::make_unique<
	WeightedPriorityQueue<OpQueueItem,uint64_t>>(
	  max_tok_per_prio, min_cost);
    } else if (opqueue == io_queue::prioritized) {
      pqueue = std::make_unique<
	PrioritizedQueue<OpQueueItem,uint64_t>>(
	  max_tok_per_prio, min_cost);
    }
  }
};

class OSD : public ceph::net::Dispatcher,
	    private OSDMapService {
  seastar::gate gate;
  seastar::timer<seastar::lowres_clock> beacon_timer;
  const int whoami;
  
  // talk with osd
  std::unique_ptr<ceph::net::Messenger> cluster_msgr;
  // talk with client/mon/mgr
  std::unique_ptr<ceph::net::Messenger> public_msgr;
  ChainedDispatchers dispatchers;
  ceph::mon::Client monc;

  std::unique_ptr<Heartbeat> heartbeat;
  seastar::timer<seastar::lowres_clock> heartbeat_timer;

  // TODO: use LRU cache
  std::map<epoch_t, seastar::lw_shared_ptr<OSDMap>> osdmaps;
  std::map<epoch_t, bufferlist> map_bl_cache;
  seastar::lw_shared_ptr<OSDMap> osdmap;
  seastar::lw_shared_ptr<OSDMap> next_osdmap;
  // TODO: use a wrapper for ObjectStore
  std::unique_ptr<ceph::os::CyanStore> store;
  std::unique_ptr<OSDMeta> meta_coll;

  std::unordered_map<spg_t, Ref<PG>> pgs;
  OSDState state;

  /// _first_ epoch we were marked up (after this process started)
  epoch_t boot_epoch = 0;
  /// _most_recent_ epoch we were marked up
  epoch_t up_epoch = 0;
  //< epoch we last did a bind to new ip:ports
  epoch_t bind_epoch = 0;
  //< since when there is no more pending pg creates from mon
  epoch_t last_pg_create_epoch = 0;

  OSDSuperblock superblock;
  uint32_t num_shards = 1;
  std::unique_ptr<OSDShard> sdata;
  // Dispatcher methods
  seastar::future<> ms_dispatch(ceph::net::ConnectionRef conn, MessageRef m) override;
  seastar::future<> ms_handle_connect(ceph::net::ConnectionRef conn) override;
  seastar::future<> ms_handle_reset(ceph::net::ConnectionRef conn) override;
  seastar::future<> ms_handle_remote_reset(ceph::net::ConnectionRef conn) override;

public:
  OSD(int id, uint32_t nonce);
  ~OSD() override;
  const unsigned int op_prio_cutoff;
  seastar::future<> mkfs(uuid_d fsid);

  seastar::future<> start();
  seastar::future<> stop();

private:
  seastar::future<> start_boot();
  seastar::future<> _preboot(version_t newest_osdmap, version_t oldest_osdmap);
  seastar::future<> _send_boot();

  seastar::future<Ref<PG>> load_pg(spg_t pgid);
  seastar::future<> load_pgs();

  // OSDMapService methods
  seastar::future<seastar::lw_shared_ptr<OSDMap>> get_map(epoch_t e) override;
  seastar::lw_shared_ptr<OSDMap> get_map() const override;

  seastar::future<bufferlist> load_map_bl(epoch_t e);
  void store_map_bl(ceph::os::Transaction& t,
                    epoch_t e, bufferlist&& bl);
  seastar::future<> store_maps(ceph::os::Transaction& t,
                               epoch_t start, Ref<MOSDMap> m);
  seastar::future<> osdmap_subscribe(version_t epoch, bool force_request);

  void write_superblock(ceph::os::Transaction& t);
  seastar::future<> read_superblock();

  seastar::future<> handle_osd_map(ceph::net::ConnectionRef conn,
                                   Ref<MOSDMap> m);
  seastar::future<> committed_osd_maps(version_t first,
                                       version_t last,
                                       Ref<MOSDMap> m);
  bool should_restart() const;
  seastar::future<> restart();
  seastar::future<> shutdown();

  seastar::future<> send_beacon();
  void update_heartbeat_peers();
  
  seastar::future<> handle_osd_op(ceph::net::ConnectionRef conn, MessageRef m);
  seastar::future<> do_op_enqueue(ceph::net::ConnectionRef conn, OpRef op, MessageRef m);
  seastar::future<> do_op_dequeue();
  seastar::future<> handle_oncommits(list<Context*>& oncommits) {
    return seastar::parallel_for_each(oncommits.begin(),oncommits.end(),[] (auto commit){
      return seastar::now(); //return commit->complete(0); implement context class later.
    }); 
  }
  void enqueue_op(spg_t pg, OpRef op, epoch_t epoch);
  seastar::future<> dequeue_op(OpQueueItem qi, PGRef pg);
  seastar::future<bool> handle_pg_missing();
  seastar::future<>maybe_share_map(OpRef op);
  set<SessionRef> session_waiting_for_map;
  void dispatch_session_waiting(SessionRef session, seastar::lw_shared_ptr<OSDMap> osdmap);
  void register_session_waiting_on_map(SessionRef session) {
    session_waiting_for_map.insert(session);
  }
  void clear_session_waiting_on_map(SessionRef session) {
    session_waiting_for_map.erase(session);
  }

  void pre_publish_map(seastar::lw_shared_ptr<OSDMap> map) override{
    next_osdmap = std::move(map);
  }
  
  std::map<epoch_t, unsigned> map_reservations;
  seastar::lw_shared_ptr<OSDMap> get_nextmap_reserved() override{
    if (!next_osdmap)
      return seastar::lw_shared_ptr<OSDMap>();
    epoch_t e = next_osdmap->get_epoch();
    map<epoch_t, unsigned>::iterator i =
      map_reservations.insert(std::make_pair(e, 0)).first;
    i->second++;
    return next_osdmap;
  }
  void release_map(seastar::lw_shared_ptr<OSDMap> osdmap) override {
    map<epoch_t, unsigned>::iterator i =
      map_reservations.find(osdmap->get_epoch());
    ceph_assert(i != map_reservations.end());
    ceph_assert(i->second > 0);
    if (--(i->second) == 0) {
      map_reservations.erase(i);
    }
  }
  seastar::future<PGRef> handle_pg_create_info(const OSDMapRef& osdmap, const PGCreateInfo *info);
};
