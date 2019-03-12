// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <seastar/core/future.hh>

#include "osd/osd_types.h"

template<typename T> using Ref = boost::intrusive_ptr<T>;
class OSDMap;
class PGPeeringEvent;

namespace ceph::net {
  class Messenger;
}

namespace ceph::os {
  class CyanStore;
}

class PG : public boost::intrusive_ref_counter<
  PG,
  boost::thread_unsafe_counter>
{
  using ec_profile_t = std::map<std::string,std::string>;
  using cached_map_t = boost::local_shared_ptr<OSDMap>;

public:
  PG(spg_t pgid,
     pg_shard_t pg_shard,
     pg_pool_t&& pool,
     std::string&& name,
     ec_profile_t&& ec_profile,
     cached_map_t osdmap,
     ceph::net::Messenger& msgr);

  epoch_t get_osdmap_epoch() const;
  const pg_info_t& get_info() const;
  const pg_stat_t& get_stats() const;
  void clear_state(uint64_t mask);
  bool test_state(uint64_t mask) const;
  void set_state(uint64_t mask);
  const PastIntervals& get_past_intervals() const;
  pg_shard_t get_primary() const;
  bool is_primary() const;
  bool is_acting(pg_shard_t pg_shard) const;
  bool is_up(pg_shard_t pg_shard) const;
  pg_shard_t get_whoami() const;
  epoch_t get_last_peering_reset() const;
  void update_last_peering_reset();
  epoch_t get_need_up_thru() const;
  void update_need_up_thru(const OSDMap* o = nullptr);

  bool proc_replica_info(pg_shard_t from,
			 const pg_info_t& pg_info,
			 epoch_t send_epoch);
  void proc_replica_log(pg_shard_t from,
			const pg_info_t& pg_info,
			const pg_log_t& pg_log,
			const pg_missing_t& pg_missing);

  using peer_info_t = std::map<pg_shard_t, pg_info_t>;
  pg_shard_t find_best_info(const PG::peer_info_t& infos) const;
  enum class choose_acting_t {
    dont_change,
    should_change,
    pg_incomplete,
  };
  std::vector<int>
  calc_acting(pg_shard_t auth_shard,
	      const vector<int>& acting,
	      const map<pg_shard_t, pg_info_t>& all_info) const;
  std::pair<choose_acting_t, pg_shard_t> choose_acting();
  seastar::future<> read_state(ceph::os::CyanStore* store);

  // peering/recovery
  bool should_send_notify() const;
  pg_notify_t get_notify(epoch_t query_epoch) const;
  bool is_last_activated_peer(pg_shard_t peer);
  void clear_primary_state();

  seastar::future<> do_peering_event(std::unique_ptr<PGPeeringEvent> evt);
  seastar::future<> handle_advance_map(cached_map_t next_map);
  seastar::future<> handle_activate_map();

  void print(ostream& os) const;

private:
  void update_primary_state(const std::vector<int>& new_up,
			    int new_up_primary,
			    const std::vector<int>& new_acting,
			    int new_acting_primary);
private:
  const spg_t pgid;
  pg_shard_t whoami;
  pg_pool_t pool;

  epoch_t last_peering_reset = 0;
  epoch_t need_up_thru = 0;

  bool should_notify_primary = false;

  using pg_shard_set_t = std::set<pg_shard_t>;
  // peer_info    -- projected (updates _before_ replicas ack)
  peer_info_t peer_info; //< info from peers (stray or prior)
  pg_shard_set_t peer_activated;

  //< pg state
  pg_info_t info;
  //< last written info, for fast info persistence
  pg_info_t last_written_info;
  PastIntervals past_intervals;
  // primary state
  pg_shard_t primary, up_primary;
  std::vector<int> acting, up;
  pg_shard_set_t actingset, upset;
  pg_shard_set_t acting_recovery_backfill;
  std::vector<int> want_acting;

  cached_map_t osdmap;
  ceph::net::Messenger& msgr;
};

std::ostream& operator<<(std::ostream&, const PG& pg);
