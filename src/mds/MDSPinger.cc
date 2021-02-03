// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"

#include "mds/MDSRank.h"
#include "mds/MDSPinger.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.pinger " << __func__

MDSPinger::MDSPinger(MDSRank *mds)
  : mds(mds) {
}

void MDSPinger::send_ping(mds_rank_t rank, const entity_addrvec_t &addr) {
  dout(10) << ": rank=" << rank << dendl;

  std::scoped_lock locker(lock);
  auto [it, inserted] = ping_state_by_rank.emplace(rank, PingState());
  if (inserted) {
    dout(20) << ": init ping pong state for rank=" << rank << dendl;
  }

  auto &ping_state = it->second;
  auto last_seq = ping_state.last_seq++;

  ping_state.seq_time_map.emplace(last_seq, clock::now());

  dout(10) << ": sending ping with sequence=" << last_seq << " to rank="
           << rank << dendl;
  mds->send_message_mds(make_message<MMDSPing>(last_seq), addr);
}

bool MDSPinger::pong_received(mds_rank_t rank, version_t seq) {
  dout(10) << ": rank=" << rank << ", sequence=" << seq << dendl;

  std::scoped_lock locker(lock);
  auto it1 = ping_state_by_rank.find(rank);
  if (it1 == ping_state_by_rank.end()) {
    // this *might* just happen on mds failover when a non-rank-0 mds
    // acks backs a ping message from an earlier rank 0 mds to a newly
    // appointed rank 0 mds (possible?).
    // or when non rank 0 active MDSs begin sending metric updates before
    // rank 0 can start pinging it (although, that should resolve out soon).
    dout(10) << ": received pong from rank=" << rank << " to which ping was never"
             << " sent (ignoring...)." << dendl;
    return false;
  }

  auto &ping_state = it1->second;
  // find incoming seq timestamp for updation
  auto it2 = ping_state.seq_time_map.find(seq);
  if (it2 == ping_state.seq_time_map.end()) {
    // rank still bootstrapping
    dout(10) << ": pong received for unknown ping sequence " << seq
             << ", rank " << rank << " should catch up soon." << dendl;
    return false;
  }

  ping_state.last_acked_time = it2->second;
  ping_state.seq_time_map.erase(ping_state.seq_time_map.begin(), it2);

  return true;
}

void MDSPinger::reset_ping(mds_rank_t rank) {
  dout(10) << ": rank=" << rank << dendl;

  std::scoped_lock locker(lock);
  auto it = ping_state_by_rank.find(rank);
  if (it == ping_state_by_rank.end()) {
    dout(10) << ": rank=" << rank << " was never sent ping request." << dendl;
    return;
  }

  // remove the rank from ping state, send_ping() will init it
  // later when invoked.
  ping_state_by_rank.erase(it);
}

bool MDSPinger::is_rank_lagging(mds_rank_t rank) {
  dout(10) << ": rank=" << rank << dendl;

  std::scoped_lock locker(lock);
  auto it = ping_state_by_rank.find(rank);
  if (it == ping_state_by_rank.end()) {
    derr << ": rank=" << rank << " was never sent ping request." << dendl;
    return false;
  }

  auto now = clock::now();
  auto since = std::chrono::duration<double>(now - it->second.last_acked_time).count();
  if (since > g_conf().get_val<std::chrono::seconds>("mds_ping_grace").count()) {
    dout(5) << ": rank=" << rank << " is lagging a pong response (last ack time is "
            <<  it->second.last_acked_time << ")" << dendl;
    return true;
  }

  return false;
}
