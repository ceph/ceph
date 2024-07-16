/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "mds/QuiesceDbManager.h"
#include "common/debug.h"
#include "fmt/format.h"
#include "include/ceph_assert.h"
#include <algorithm>
#include <random>
#include <ranges>
#include <type_traits>
#include "boost/url.hpp"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_quiesce
#undef dout_prefix
#define dout_prefix *_dout << "quiesce.mgr." << membership.me << " <" << __func__ << "> "

#undef dout
#define dout(lvl)                                                        \
  do {                                                                   \
    auto subsys = ceph_subsys_mds;                                       \
    if ((dout_context)->_conf->subsys.should_gather(dout_subsys, lvl)) { \
      subsys = dout_subsys;                                              \
    }                                                                    \
  dout_impl(dout_context, ceph::dout::need_dynamic(subsys), lvl) dout_prefix

#undef dendl
#define dendl \
  dendl_impl; \
  }           \
  while (0)

#define dset(suffix) "[" << set_id << "@" << set.version << "] " << suffix
#define dsetroot(suffix) "[" << set_id << "@" << set.version << "," << root << "] " << suffix

const QuiesceInterface::PeerId QuiesceClusterMembership::INVALID_MEMBER = MDS_GID_NONE;

static QuiesceTimeInterval time_distance(QuiesceTimePoint lhs, QuiesceTimePoint rhs) {
  if (lhs > rhs) {
    return lhs - rhs;
  } else {
    return rhs - lhs;
  }
}

bool QuiesceDbManager::db_thread_has_work() const
{
  return db_thread_should_exit
      || pending_acks.size() > 0
      || pending_requests.size() > 0
      || pending_db_updates.size() > 0
      || (agent_callback.has_value() && agent_callback->if_newer < db.version())
      || (cluster_membership.has_value() && cluster_membership->epoch != membership.epoch);
}

void* QuiesceDbManager::quiesce_db_thread_main()
{
  std::unique_lock ls(submit_mutex);
  QuiesceTimeInterval next_event_at_age = QuiesceTimeInterval::max();
  QuiesceDbVersion last_acked = {0, 0};

  dout(5) << "Entering the main thread" << dendl;
  bool keep_working = true;
  while (keep_working) {
    // QuiesceTimeInterval::max() value of next_event_at_age
    // may cause an overflow in some stdlib implementations when calling
    // std::condition_variable::wait_for(ls, next_event_at_age - db_age).
    // The overflow can make the call timeout immediately,
    // resulting in a busy-loop.
    // The solution is to cap the wait duration to a value which can
    // certainly fit in whichever clock std library is using internally.
    const auto max_wait = std::chrono::duration_cast<QuiesceTimeInterval>(
        std::chrono::seconds(10)
    );

    while (!db_thread_has_work()) {
      auto db_age = db.get_age();
      if (next_event_at_age <= db_age) {
        break;
      }
      dout(20) << "db idle, age: " << db_age << " next_event_at_age: " << next_event_at_age << dendl;
      auto timeout = std::min(max_wait, next_event_at_age - db_age);
      submit_condition.wait_for(ls, timeout);
    }

    auto [is_member, should_exit] = membership_upkeep();
    keep_working = !should_exit;

    if (is_member) {
      decltype(pending_acks) acks(std::move(pending_acks));
      decltype(pending_requests) requests(std::move(pending_requests));
      decltype(pending_db_updates) db_updates(std::move(pending_db_updates));

      ls.unlock();

      if (membership.is_leader()) {
        const QuiesceTimeInterval bootstrap_delay = leader_bootstrap(std::move(db_updates));
        if (bootstrap_delay == QuiesceTimeInterval::zero()){
          // we're good to process things
          next_event_at_age = leader_upkeep(std::move(acks), std::move(requests));
        } else {
          // not yet there. Put the acks and requests back onto the queue and wait for updates
          // We should mark the next event age in case we get caught up in the sleep above
          next_event_at_age = db.get_age() + bootstrap_delay;
          ls.lock();
          while (!requests.empty()) {
            pending_requests.emplace_front(std::move(requests.back()));
            requests.pop_back();
          }
          while (!acks.empty()) {
            pending_acks.emplace_front(std::move(acks.back()));
            acks.pop_back();
          }
          if (pending_db_updates.empty()) {
            // we are waiting here because if requests/acks aren't empty
            // the code above will skip the sleep due to the `db_thread_has_work`
            // returning true, causing a busy-loop of the quiesce manager thread.
            // This sleep may be interrupted by the submit_condition, in which case
            // we will re-consider everything and may end up here again, but with a shorter
            // bootstrap_delay.
            dout(5) << "bootstrap: waiting for new peers with pending acks: " << pending_acks.size()
              << " requests: " << pending_requests.size()
              << ". Wait timeout: " << bootstrap_delay << dendl;
            submit_condition.wait_for(ls, bootstrap_delay);
          }
          continue;
        }
      } else {
        next_event_at_age = replica_upkeep(std::move(db_updates));
      }
    } else {
      ls.unlock();
      dout(15) << "not a cluster member, keeping idle " << dendl;
      next_event_at_age = QuiesceTimeInterval::max();
    }
  
    complete_requests();

    // by default, only send ack if the version has changed
    bool send_ack = last_acked != db.version();
    QuiesceMap quiesce_map(db.version());
    {
      std::lock_guard lc(agent_mutex);
      if (agent_callback) {
        if (agent_callback->if_newer < db.version()) {
          dout(20) << "notifying agent with db version " << db.version() << dendl;
          calculate_quiesce_map(quiesce_map);
          send_ack = agent_callback->notify(quiesce_map);
          agent_callback->if_newer = db.version();
        } else {
          send_ack = false;
        }
      } else {
        // by default, ack the db version and agree to whatever was sent
        // This means that a quiesce cluster member with an empty agent callback 
        // will cause roots to stay quiescing indefinitely
        dout(5) << "no agent callback registered, responding with an empty ack" << dendl;
      }
    }

    if (is_member && send_ack) {
      auto db_version = quiesce_map.db_version;
      dout(20) << "synchronous agent ack: " << quiesce_map << dendl;
      auto rc = membership.send_ack(std::move(quiesce_map));
      if (rc != 0) {
        dout(1) << "ERROR ("<< rc <<") when sending synchronous agent ack " 
        << quiesce_map << dendl;
      } else {
        last_acked = db_version;
      }
    }

    ls.lock();
  }

  ls.unlock();

  dout(5) << "Exiting the main thread" << dendl;

  return 0;
}

void QuiesceDbManager::update_membership(const QuiesceClusterMembership& new_membership, RequestContext* inject_request)
{
  std::unique_lock lock(submit_mutex);

  bool will_participate = new_membership.members.contains(new_membership.me);
  dout(20) << "will participate: " << std::boolalpha << will_participate << std::noboolalpha << dendl;

  if (will_participate && !quiesce_db_thread.is_started()) {
    // start the thread
    dout(5) << "starting the db mgr thread at epoch: " << new_membership.epoch << dendl;
    db_thread_should_exit = false;
    quiesce_db_thread.create("quiesce_db_mgr");
  } else if (quiesce_db_thread.is_started()) {
    submit_condition.notify_all();
  }

  if (inject_request) {
    if (will_participate || quiesce_db_thread.is_started()) {
      pending_requests.push_front(inject_request);
    } else {
      inject_request->complete(ENOTTY);
    }
  }

  if (will_participate) {
    cluster_membership = new_membership;
  } else {
    cluster_membership.reset();
    db_thread_should_clear_db = true;
  }

  std::lock_guard lc(agent_mutex);
  if (agent_callback) {
      agent_callback->if_newer = {0, 0};
  }
}

std::pair<QuiesceDbManager::IsMemberBool, QuiesceDbManager::ShouldExitBool>
QuiesceDbManager::membership_upkeep()
{
  if (db_thread_should_clear_db) {
    dout(5) << "a reset of the db has been requested" << dendl;
    db_thread_should_clear_db = false;
    membership.epoch = 0;
    // clear the peers to bootstrap from scratch if we are the leader
    peers.clear();
    // reset the db
    db.clear();
    // not clearing awaits and requests, they will be handled below
  }

  if (cluster_membership && cluster_membership->epoch == membership.epoch) {
    // no changes
    return {true, db_thread_should_exit};
  }

  bool was_leader = membership.epoch > 0 && membership.leader == membership.me;
  bool is_leader = cluster_membership && cluster_membership->leader == cluster_membership->me;
  if (cluster_membership) {
    dout(10) << "epoch:" << cluster_membership->epoch << " leader:" 
      << std::boolalpha << was_leader << "->" << is_leader << std::noboolalpha
      << " members:" << cluster_membership->members << dendl;
  } else {
    dout(10) << "not a member! was_leader: " << was_leader << dendl;
  }

  if (is_leader) {
    // remove peers that aren't present anymore
    for (auto peer_it = peers.begin(); peer_it != peers.end();) {
      if (cluster_membership->members.contains(peer_it->first)) {
        peer_it++;
      } else {
        peer_it = peers.erase(peer_it);
      }
    }
    // create empty info for new peers
    for (auto peer : cluster_membership->members) {
      peers.try_emplace(peer);
    }
    // update the db epoch
    db.epoch = cluster_membership->epoch;
  } else {
    peers.clear();
    // abort awaits with EINPROGRESS
    // the reason is that we don't really have a new version
    // of any of the sets, we just aren't authoritative anymore
    // hence, EINPROGRESS is a more appropriate response than, say, EINTR
    for (auto & [_, await_ctx]: awaits) {
      done_requests[await_ctx.req_ctx] = EINPROGRESS;
    }
    awaits.clear();
    // reject pending requests as not leader
    while (!pending_requests.empty()) {
      done_requests[pending_requests.front()] = ENOTTY;
      pending_requests.pop_front();
    }
  }

  if (cluster_membership) {
    membership = *cluster_membership;
    dout(15) << "Updated membership" << dendl;
  }

  return { cluster_membership.has_value(), db_thread_should_exit };
}

QuiesceTimeInterval QuiesceDbManager::replica_upkeep(decltype(pending_db_updates)&& db_updates)
{
  // as a replica, we only care about the latest update
  while (db_updates.size() > 1) {
    dout(10) << "skipping an older update from " << db_updates.front().origin << " version " << db_updates.front().db.db_version << dendl;
    db_updates.pop();
  }

  if (db_updates.empty()) {
    // no db updates, wait forever
    return QuiesceTimeInterval::max();
  }

  QuiesceDbListing &update = db_updates.back().db;

  if (update.db_version.set_version == 0) {
    // this is a call from the leader to upload our local db version
    update.sets = db.sets;
    update.db_version.set_version = db.set_version;
    update.db_age = db.get_age();
    membership.send_listing_to(membership.leader, std::move(update));
    return QuiesceTimeInterval::max();
  }

  auto time_zero = QuiesceClock::now() - update.db_age;
  if (time_distance(time_zero, db.time_zero) > std::chrono::seconds(1)) {
    dout(10) << "significant db_time_zero change to " << time_zero << " from " << db.time_zero << dendl;
  }
  db.time_zero = time_zero;
  db.epoch = update.db_version.epoch;

  if (db.set_version > update.db_version.set_version) {
    dout(3) << "got an older version of DB from the leader: " << update.db_version.set_version << " < " << db.set_version << dendl;
    dout(3) << "discarding the DB" << dendl;
    db.clear();
  } else {
    for (auto& [qs_id, qs] : update.sets) {
      db.sets.insert_or_assign(qs_id, std::move(qs));
    }
    db.set_version = update.db_version.set_version;
  }

  // wait forever
  return QuiesceTimeInterval::max();
}

QuiesceTimeInterval QuiesceDbManager::leader_bootstrap(decltype(pending_db_updates)&& db_updates)
{
  const QuiesceTimeInterval PEER_DISCOVERY_INTERVAL = std::chrono::seconds(1);
  QuiesceTimeInterval bootstrap_delay = PEER_DISCOVERY_INTERVAL;

  // check that we've heard from all peers in this epoch
  std::unordered_set<QuiesceInterface::PeerId> unknown_peers;
  for (auto&& [peer, info] : peers) {
    if (info.diff_map.db_version.epoch < membership.epoch && info.diff_map.db_version.set_version == 0) {
      if (peer != membership.me) {
        unknown_peers.insert(peer);
      }
    }
  }

  // only consider db submissions from unknown peers
  while (!unknown_peers.empty() && !db_updates.empty()) {
    auto &from = db_updates.front().origin;
    auto &update = db_updates.front().db;
    if (update.db_version.epoch == membership.epoch && unknown_peers.erase(from) > 0) {
      // see if this peer's version is newer than mine
      if (db.set_version < update.db_version.set_version) {
        dout(3) << "preferring version from peer " 
          << from << " (" << update.db_version 
          << ") over mine (" << db.version() << ")" 
          << " and incrementing it to collect acks" << dendl;
        db.time_zero = QuiesceClock::now() - update.db_age;
        db.set_version = update.db_version.set_version + 1;
        db.sets = update.sets;
      }
      // record that we've seen this peer;
      // set the epoch correctly but use set version 0 because it's not an ack yet.
      peers[from] =  PeerInfo {QuiesceMap({membership.epoch, 0}), QuiesceClock::now()};
    }
    db_updates.pop();
  }

  QuiesceTimePoint const now = QuiesceClock::now();
  for (auto & peer: unknown_peers) {
    PeerInfo & info = peers[peer];

    QuiesceTimePoint next_discovery = info.last_activity + PEER_DISCOVERY_INTERVAL;
    if (next_discovery < now) {
      // send a discovery request to unknown peers
      dout(5) << " sending a discovery request to " << peer << dendl;
      membership.send_listing_to(peer, QuiesceDbListing(membership.epoch));
      info.last_activity = now;
      next_discovery = info.last_activity + PEER_DISCOVERY_INTERVAL;
    }
    // next_discovery is >= now
    if (bootstrap_delay > next_discovery - now) {
      bootstrap_delay = (next_discovery - now);
    }
  }

  bool all_peers_known = unknown_peers.empty();
  
  if (!all_peers_known) {
    dout(10) << "unknown peers: " << unknown_peers << dendl;
  }

  // add some margin to hit the discovery interval for the earliest discovery.
  const QuiesceTimeInterval a_little_more = std::chrono::milliseconds(100);
  return all_peers_known ? QuiesceTimeInterval::zero() : (bootstrap_delay + a_little_more);
}

QuiesceTimeInterval QuiesceDbManager::leader_upkeep(decltype(pending_acks)&& acks, decltype(pending_requests)&& requests)
{
  // record peer acks
  while (!acks.empty()) {
    auto& [from, diff_map] = acks.front();
    leader_record_ack(from, std::move(diff_map));
    acks.pop_front();
  }

  // process requests
  while (!requests.empty()) {
    auto req_ctx = requests.front();
    int result = leader_process_request(req_ctx);
    if (result != EBUSY) {
      done_requests[req_ctx] = result;
    }
    requests.pop_front();
  }

  QuiesceTimeInterval next_db_event_at_age = leader_upkeep_db();
  QuiesceTimeInterval next_await_event_at_age = leader_upkeep_awaits();

  return std::min(next_db_event_at_age, next_await_event_at_age);
}

void QuiesceDbManager::complete_requests() {
  for (auto [req, res]: done_requests) {
    auto & r = req->response;
    r.clear();
    if (membership.leader == membership.me) {
      r.db_age = db.get_age();
      r.db_version = db.version();

      if (req->request.set_id) {
        Db::Sets::const_iterator it = db.sets.find(*req->request.set_id);
        if (it != db.sets.end()) {
          r.sets.emplace(*it);
        }
      } else if (req->request.is_query()) {
        for (auto && it : std::as_const(db.sets)) {
          r.sets.emplace(it);
        }
      }
    }

    // non-zero result codes are all errors
    dout(10) << "completing " << req->request << " with rc: " << -res << dendl;
    req->complete(-res);
  }
  done_requests.clear();
}

void QuiesceDbManager::leader_record_ack(QuiesceInterface::PeerId from, QuiesceMap&& diff_map)
{
  auto it = peers.find(from);

  if (it == peers.end()) {
    dout(5) << "unknown peer " << from << dendl;
    // ignore updates from unknown peers
    return;
  }

  auto & info = it->second;

  if (diff_map.db_version > db.version()) {
    dout(15) << "future version ack by peer " << from << " (" << diff_map.db_version << " > " << db.version() << ")" << dendl;
    if (diff_map.db_version.epoch > db.version().epoch && diff_map.db_version.set_version <= db.version().set_version) {
      dout(15) << "my epoch is behind, ignoring this until my membership is updated" << dendl;
    } else {
      dout(5) << "will send the peer a full DB" << dendl;
      info.clear();
    }
  } else {
    dout(20) << "ack " << diff_map << " from peer " << from << dendl;
    info.diff_map = std::move(diff_map);
    info.last_activity = QuiesceClock::now();
  }
}

static std::string random_hex_string() {
  std::mt19937 gen(std::random_device {} ());
  return fmt::format("{:x}", gen());
}

bool QuiesceDbManager::sanitize_roots(QuiesceDbRequest::Roots& roots)
{
  static const std::string file_scheme = "file";
  static const std::string inode_scheme = "inode";
  static const std::unordered_set<std::string> supported_schemes { file_scheme, inode_scheme };
  QuiesceDbRequest::Roots result;
  for (auto &root : roots) {
    auto parsed_uri = boost::urls::parse_uri_reference(root);
    if (!parsed_uri) {
      dout(2) << "Couldn't parse root '" << root << "' as URI (error: " << parsed_uri.error() << ")" << dendl;
      return false;
    }

    boost::url root_url = parsed_uri.value();
    root_url.normalize();

    if (!root_url.has_scheme()) {
      root_url.set_scheme(file_scheme);
    } else if (!supported_schemes.contains(root_url.scheme())) {
      dout(2) << "Unsupported root URL scheme '" << root_url.scheme() << "'" << dendl;
      return false;
    }

    if (root_url.has_authority()) {
      auto auth_str = root_url.authority().buffer();
      bool ok_remove = false;
      if (auth_str == membership.fs_name) {
        ok_remove = true;
      } else {
        try {
          ok_remove = std::stoll(auth_str) == membership.fs_id;
        } catch (...) { }
      }
      if (ok_remove) {
        // OK, but remove the authority for now
        // we may want to enforce it if we decide to keep a single database for all file systems
        dout(10) << "Removing the fs name or id '" << auth_str << "' from the root url authority section" << dendl;
        root_url.remove_authority();
      } else {
        dout(2) << "The root url '" << root_url.buffer() 
          << "' includes an authority section '" << auth_str 
          << "' which doesn't match the fs id (" << membership.fs_id 
          << ") or name ('" << membership.fs_name << "')" << dendl;
        return false;
      }
    }

    std::string sanitized_path;
    sanitized_path.reserve(root_url.path().size());
    // deal with the file path
    //  * make it absolute (start with a slash)
    //  * remove repeated slashes
    //  * remove the trailing slash
    bool skip_slash = true;
    for (auto&& c : root_url.path()) {
      if (c != '/' || !skip_slash) {
        sanitized_path.push_back(c);
      }
      skip_slash = c == '/';
    }

    if (sanitized_path.size() > 0 && sanitized_path.back() == '/') {
      sanitized_path.pop_back();
    }

    if (root_url.scheme() == file_scheme) {
      sanitized_path.insert(sanitized_path.begin(), '/');
    } else if (root_url.scheme() == inode_scheme) {
      uint64_t inodeno = 0;
      try {
        inodeno = std::stoull(sanitized_path);
      } catch (...) { }

      if (!inodeno || fmt::format("{}", inodeno) != sanitized_path) {
        dout(2) << "Root '" << root << "' does not encode a vaild inode number" << dendl;
        return false;
      }
    }

    root_url.set_path(sanitized_path);

    if (root_url.buffer() != root) {
      dout(10) << "Normalized root '" << root << "' to '" << root_url.buffer() << "'" << dendl;
    }
    result.insert(root_url.buffer());
  }
  roots.swap(result);
  return true;
}

int QuiesceDbManager::leader_process_request(RequestContext* req_ctx)
{
  QuiesceDbRequest &request = req_ctx->request;

  if (!request.is_valid()) {
    dout(2) << "rejecting an invalid request" << dendl;
    return EINVAL;
  }

  if (!sanitize_roots(request.roots)) {
    dout(2) << "failed to sanitize roots for a request" << dendl;
    return EINVAL;
  }

  dout(20) << request << dendl;

  const auto db_age = db.get_age();

  if (request.is_cancel_all()) {
    dout(3) << "WARNING: got a cancel all request" << dendl;
    // special case - reset all
    // this will only work on active sets
    for (auto &[set_id, set]: db.sets) {
      if (set.is_active()) {
        bool did_update = false;
        for (auto&& [_, member]: set.members) {
          did_update |= !member.excluded;
          member.excluded = true;
        }

        ceph_assert(did_update);
        ceph_assert(set.rstate.update(QS_CANCELED, db_age));
        set.version = db.set_version+1;
      }
    }
    return 0;
  }

  // figure out the set to update
  auto set_it = db.sets.end();

  if (request.set_id) {
    set_it = db.sets.find(*request.set_id);
  } else if (request.if_version > 0) {
    dout(2) << "can't expect a non-zero version (" << *request.if_version << ") for a new set" << dendl;
    return EINVAL;
  }

  if (set_it == db.sets.end()) {
    if (request.includes_roots() && request.if_version <= 0) {
      // such requests may introduce a new set
      if (!request.set_id) {
        // we should generate a unique set id
        QuiesceSetId new_set_id;
        do {
          new_set_id = random_hex_string();
        } while (db.sets.contains(new_set_id));
        // update the set_id in the request so that we can
        // later know which set got created
        request.set_id.emplace(std::move(new_set_id));
      }
      set_it = db.sets.emplace(*request.set_id, QuiesceSet()).first;
    } else if (request.is_mutating() || request.await) {
      ceph_assert(request.set_id.has_value());
      dout(2) << "coudn't find set with id '" << *request.set_id <<  "'" << dendl;
      return ENOENT;
    }
  }

  if (set_it != db.sets.end()) {
    auto& [set_id, set] = *set_it;

    int result = leader_update_set(*set_it, request);
    if (result != 0) {
      return result;
    }

    if (request.await) {
      // quiesce-await is only allowed for sets that are quiescing or quiesced.
      // this check may have a false negative for a quiesced set
      // that will be released in another request in the same batch
      // in that case, this await will be enqueued but then found and completed
      // with the same error in `leader_upkeep_awaits`
      if (set.rstate.state > QS_QUIESCED && !request.is_release()) {
        dout(2) << dset("can't quiesce-await a set in the state: ") << set.rstate.state << dendl;
        return EPERM;
      }

      auto expire_at_age = interval_saturate_add(db_age, *request.await);
      awaits.emplace(std::piecewise_construct,
          std::forward_as_tuple(set_id),
          std::forward_as_tuple(expire_at_age, req_ctx));
      // let the caller know that the request isn't done yet
      return EBUSY;
    }
  }

  // if we got here it must be a success
  return 0;
}

int QuiesceDbManager::leader_update_set(Db::Sets::value_type& set_it, const QuiesceDbRequest& request)
{
  auto & [set_id, set] = set_it;
  if (request.if_version && set.version != *request.if_version) {
    dout(10) << dset("is newer than requested (") << *request.if_version << ") " << dendl;
    return ESTALE;
  }

  if (!request.is_mutating()) {
    return 0;
  }

  bool did_update = false;
  bool did_update_roots = false;

  if (request.is_release()) {
    // the release command is allowed in states
    // quiesced, releasing, released
    switch (set.rstate.state) {
      case QS_QUIESCED:
        // we only update the state to RELEASING,
        // and not the age. This is to keep counting
        // towards the quiesce expiration.
        // TODO: this could be reconsidered, but would
        // then probably require an additional timestamp
        set.rstate.state = QS_RELEASING;
        did_update = true;
        dout(15) << dset("") << "updating state to: " << set.rstate.state << dendl;
      case QS_RELEASING:
      case QS_RELEASED:
        break;
      default:
        dout(2) << dset("can't release in the state: ") << set.rstate.state << dendl;
        return EPERM;
    }
  } else {
    const auto db_age = db.get_age();
    bool reset = false;

    if (!request.is_reset()) {
      // only active or new sets can be modified
      if (!set.is_active() && set.version > 0) {
        dout(2) << dset("rejecting modification in the terminal state: ") << set.rstate.state << dendl;
        return EPERM;
      } else if (request.includes_roots() && set.is_releasing()) {
        dout(2) << dset("rejecting new roots in the QS_RELEASING state") << dendl;
        return EPERM;
      }
    } else {
      // a reset request can be used to resurrect a set from whichever state it's in now
      if (set.rstate.state > QS_QUIESCED) {
        dout(5) << dset("reset back to a QUIESCING state") << dendl;
        did_update = set.rstate.update(QS_QUIESCING, db_age);
        ceph_assert(did_update);
        reset = true;
      }
    }

    if (request.timeout) {
      set.timeout = *request.timeout;
      did_update = true;
    }

    if (request.expiration) {
      set.expiration = *request.expiration;
      did_update = true;
    }

    size_t included_count = 0;
    QuiesceState min_member_state = QS__MAX;

    for (auto& [root, info] : set.members) {
      if (request.should_exclude(root)) {
        did_update_roots |= !info.excluded;
        info.excluded = true;
      } else if (!info.excluded) {
        included_count ++;

        QuiesceState effective_member_state;

        if (reset) {
          dout(5) << dsetroot("reset back to a QUIESCING state") << dendl;
          info.rstate.state = QS_QUIESCING;
          info.rstate.at_age = db_age;
          did_update_roots = true;
          effective_member_state = info.rstate.state;
        } else {
          QuiesceState min_reported_state;
          QuiesceState max_reported_state;
          size_t reporting_peers = check_peer_reports(set_id, set, root, info, min_reported_state, max_reported_state);

          if (reporting_peers == peers.size() && max_reported_state < QS__FAILURE) {
            effective_member_state = set.get_effective_member_state(min_reported_state);
          } else {
            effective_member_state = info.rstate.state;
          }
        }

        min_member_state = std::min(min_member_state, effective_member_state);
      }
    }

    if (request.includes_roots()) {
      for (auto const& root : request.roots) {
        auto const& [member_it, emplaced] = set.members.try_emplace(root, db_age);
        auto& [_, info] = *member_it;
        if (emplaced || info.excluded) {
          info.excluded = false;
          did_update_roots = true;
          included_count++;
          info.rstate = { QS_QUIESCING, db_age };
          min_member_state = std::min(min_member_state, QS_QUIESCING);
        }
      }
    }

    did_update |= did_update_roots;

    if (included_count == 0) {
      dout(20) << dset("cancelled due to 0 included members") << dendl;
      did_update = set.rstate.update(QS_CANCELED, db_age);
      ceph_assert(did_update);
    } else if (min_member_state < QS__MAX) {
      auto next_state = set.next_state(min_member_state);
      if (did_update |= set.rstate.update(next_state, db_age)) {
        dout(15) << dset("updated to match the min state of the remaining (") << included_count << ") members: " << set.rstate.state << dendl;
      }
    }
  }

  if (did_update) {
    dout(20) << dset("updating version from ") << set.version << " to " << db.set_version + 1 << dendl;
    set.version = db.set_version + 1;
    if (did_update_roots) {
      // any awaits pending on this set must be interrupted
      // NB! even though the set may be QUIESCED now, it could only
      // get there due to exclusion of quiescing roots, which is
      // not a vaild way to successfully await a set, hence EINTR
      // However, if the set had all roots removed then we
      // should respond in ECANCELED to notify that no more await
      // attempts will be permitted
      auto range = awaits.equal_range(set_id);
      int rc = EINTR;
      if (!set.is_active()) {
        ceph_assert(set.rstate.state == QS_CANCELED);
        rc = ECANCELED;
      }
      for (auto it = range.first; it != range.second; it++) {
        done_requests[it->second.req_ctx] = rc;
      }
      if (range.first != range.second) {
        dout(10) << dset("interrupting awaits with rc = ") << rc << " due to a change in members" << dendl;
      }
      awaits.erase(range.first, range.second);
    }
  }

  return 0;
}

QuiesceTimeInterval QuiesceDbManager::leader_upkeep_db()
{
  QuiesceTimeInterval next_event_at_age = QuiesceTimeInterval::max();
  QuiesceSetVersion max_set_version = db.set_version;

  struct PeerUpdate {
    QuiesceInterface::PeerId peer;
    PeerInfo& info;
    std::deque<std::reference_wrapper<Db::Sets::value_type>> set_refs;
    PeerUpdate(QuiesceInterface::PeerId peer, PeerInfo& info)
        : peer(peer)
        , info(info)
    {}

    QuiesceSetVersion known_set_version() const
    {
      return info.diff_map.db_version.set_version;
    }
  };

  // populate peer_updates with peers except me
  std::vector<PeerUpdate> peer_updates;
  for (auto& [peer, info]: peers) {
    // no need to replicate to myself
    if (peer != membership.me) {
      peer_updates.emplace_back(peer, info);
    }
  }

  for(auto & set_it: db.sets) {
    auto & [set_id, set] = set_it;
    auto next_set_event_at_age = leader_upkeep_set(set_it);

    max_set_version = std::max(max_set_version, set.version);
    next_event_at_age = std::min(next_event_at_age, next_set_event_at_age);

    for(auto & peer_update: peer_updates) {
      // update remote peers if their version is lower than this set's
      if (peer_update.known_set_version() < set.version) {
        peer_update.set_refs.emplace_back(set_it);
      }
    }
  }

  db.set_version = max_set_version;

  // update the peers
  const auto now = QuiesceClock::now();
  static const QuiesceTimeInterval PEER_REPEATED_UPDATE_INTERVAL = std::chrono::seconds(1);
  for (auto const & peer_update: peer_updates) {
    if (peer_update.info.last_sent_version == db.version()) {
      if (now < (peer_update.info.last_activity + PEER_REPEATED_UPDATE_INTERVAL)) {
        // don't spam the peer with the same version
        continue;
      }
      dout(5) << "repeated update of the peer " << peer_update.peer << " with version " << db.version() << dendl;
    }

    QuiesceDbListing listing;
    listing.db_age = db.get_age();
    listing.db_version = db.version();
    std::ranges::copy(peer_update.set_refs, std::inserter(listing.sets, listing.sets.end()));

    dout(20) << "updating peer " << peer_update.peer << " with " << peer_update.set_refs.size() 
      << " sets modified in db version range (" 
      << peer_update.known_set_version() << ".." << db.set_version << "]" << dendl;

    auto rc = membership.send_listing_to(peer_update.peer, std::move(listing));
    if (rc != 0) {
      dout(1) << "ERROR (" << rc << ") trying to replicate db version " 
        << db.set_version << " with " << peer_update.set_refs.size() 
        << " sets to the peer " << peer_update.peer << dendl;
    } else {
      peer_update.info.last_activity = now;
      peer_update.info.last_sent_version = db.version();
    }
  }

  return next_event_at_age;
}

QuiesceState QuiesceSet::next_state(QuiesceState min_member_state) const {
  ceph_assert(min_member_state > QS__INVALID);
  ceph_assert(rstate.state < QS__TERMINAL);

  if (is_releasing() && min_member_state == QS_QUIESCED) {
    // keep releasing
    return QS_RELEASING;
  }

  // otherwise, follow the member state
  return min_member_state;
}

size_t QuiesceDbManager::check_peer_reports(const QuiesceSetId& set_id, const QuiesceSet& set, const QuiesceRoot& root, const QuiesceSet::MemberInfo& member, QuiesceState& min_reported_state, QuiesceState& max_reported_state) {
  min_reported_state = QS__MAX;
  max_reported_state = QS__INVALID;

  size_t up_to_date_peers = 0;
  std::multimap<QuiesceState, std::pair<QuiesceInterface::PeerId, QuiesceDbVersion>> reporting_peers;

  for (auto& [peer, info] : peers) {
    // we consider the last bit of information we had from the peer
    auto dit = info.diff_map.roots.find(root);
    QuiesceState reported_state = set.get_requested_member_state();

    if (dit != info.diff_map.roots.end()) {
      // the peer has something to say about this root
      auto const& pr_state = dit->second;
      if (!pr_state.is_valid()) {
        dout(5) << dsetroot("ignoring an invalid peer state ") << pr_state.state << dendl;
        continue;
      }
      reported_state = pr_state.state;
    }

    // but we only consider the peer up to date given the version
    if (info.diff_map.db_version >= QuiesceDbVersion { membership.epoch, set.version }) {
      up_to_date_peers++;
    }

    // we keep track of reported states only if the peer actually said something
    // even if for an older version
    if (info.diff_map.db_version.set_version > 0) {
      reporting_peers.insert({ reported_state, { peer, info.diff_map.db_version } });
      min_reported_state = std::min(min_reported_state, reported_state);
      max_reported_state = std::max(max_reported_state, reported_state);
    }
  }

  if (min_reported_state == QS__MAX) {
    // this means that we had 0 eligible peer reports
    min_reported_state = set.get_requested_member_state();
    max_reported_state = set.get_requested_member_state();
  }

  dout(20) << dsetroot("")
           << "up_to_date_peers: " << up_to_date_peers
           << " min_reported_state: " << min_reported_state
           << " max_reported_state: " << max_reported_state
           << " peer_acks: " << reporting_peers
           << dendl;

  return up_to_date_peers;
}

QuiesceTimeInterval QuiesceDbManager::leader_upkeep_set(Db::Sets::value_type& set_it)
{
  auto& [set_id, set] = set_it;

  if (!set.is_active()) {
    return QuiesceTimeInterval::max();
  }

  QuiesceTimeInterval end_of_life = QuiesceTimeInterval::max();

  const auto db_age = db.get_age();
  // no quiescing could have started before the current db_age

  QuiesceState min_member_state = QS__MAX;
  size_t included_members = 0;
  // for each included member, apply recorded acks and check quiesce timeouts
  for (auto& [root, member] : set.members) {
    if (member.excluded) {
      continue;
    }
    included_members++;

    QuiesceState min_reported_state;
    QuiesceState max_reported_state;

    size_t reporting_peers = check_peer_reports(set_id, set, root, member, min_reported_state, max_reported_state);
    auto effective_state = set.get_effective_member_state(min_reported_state);

    if (max_reported_state >= QS__FAILURE) {
      // if at least one peer is reporting a failure state then move to it
      dout(5) << dsetroot("reported by at least one peer as: ") << max_reported_state << dendl;
      if (member.rstate.update(max_reported_state, db_age)) {
        dout(15) << dsetroot("updating member state to ") << member.rstate.state << dendl;
        set.version = db.set_version + 1;
      }
    } else if (effective_state < member.rstate.state) {
      // someone has reported a rollback state for the root
      dout(15) << dsetroot("reported by at least one peer as ") << min_reported_state << " vs. the expected " << member.rstate.state << dendl;
      if (member.rstate.update(effective_state, db_age)) {
        dout(15) << dsetroot("updating member state to ") << member.rstate.state << dendl;
        set.version = db.set_version + 1;
      }
    } else if (reporting_peers == peers.size()) {
      dout(20) << dsetroot("min reported state for all (") << reporting_peers << ") peers: " << min_reported_state 
          << ". Effective state: " << effective_state << dendl;
      if (member.rstate.update(effective_state, db_age)) {
        dout(15) << dsetroot("updating member state to ") << member.rstate.state << dendl;
        set.version = db.set_version + 1;
      }
    }

    if (member.is_quiescing()) {
      // the quiesce timeout applies in this case
      auto timeout_at_age = interval_saturate_add(member.rstate.at_age, set.timeout);
      if (timeout_at_age <= db_age) {
        // NB: deliberately not changing the member state
        dout(10) << dsetroot("detected a member quiesce timeout") << dendl;
        ceph_assert(set.rstate.update(QS_TIMEDOUT, db_age));
        set.version = db.set_version + 1;
        break;
      }
      end_of_life = std::min(end_of_life, timeout_at_age);
    } else if (member.is_failed()) {
      // if at least one member is in a failure state
      // then the set must receive it as well
      dout(5) << dsetroot("propagating the terminal member state to the set level: ") << member.rstate.state << dendl;
      ceph_assert(set.rstate.update(member.rstate.state, db_age));
      set.version = db.set_version + 1;
      break;
    }

    min_member_state = std::min(min_member_state, member.rstate.state);
  }

  if (!set.is_active()) {
    return QuiesceTimeInterval::max();
  }

  // we should have at least one included members to be active
  ceph_assert(included_members > 0);
  auto next_state = set.next_state(min_member_state);

  if (set.rstate.update(next_state, db_age)) {
    set.version = db.set_version + 1;
    dout(15) << dset("updated set state to match member reports: ") << set.rstate.state << dendl;
  }

  if (set.is_quiesced() || set.is_released()) {
    // any awaits pending on this set should be completed now,
    // before the set may enter a QS_EXPIRED state
    // due to a zero expiration timeout.
    // this could be used for barriers.
    auto range = awaits.equal_range(set_id);
    for (auto it = range.first; it != range.second; it++) {
      done_requests[it->second.req_ctx] = 0;
      if (set.is_quiesced()) {
        // since we've just completed a _quiesce_ await
        // we should also reset the recorded age of the QUIESCED state
        // to postpone the expiration time checked below
        set.rstate.at_age = db_age;
        set.version = db.set_version + 1;
        dout(20) << dset("reset quiesced state age upon successful await") << dendl;
      }
    }
    awaits.erase(range.first, range.second);
  }

  // check timeouts:
  if (set.is_quiescing()) {
    // sanity check that we haven't missed this before
    ceph_assert(end_of_life > db_age);
  } else if (set.is_active()) {
    auto expire_at_age = interval_saturate_add(set.rstate.at_age, set.expiration);
    if (expire_at_age <= db_age) {
      // we have expired
      ceph_assert(set.rstate.update(QS_EXPIRED, db_age));
      set.version = db.set_version + 1;
    } else {
      end_of_life = std::min(end_of_life, expire_at_age);
    }
  }

  return end_of_life;
}

QuiesceTimeInterval QuiesceDbManager::leader_upkeep_awaits()
{
  QuiesceTimeInterval next_event_at_age = QuiesceTimeInterval::max();
  for (auto it = awaits.begin(); it != awaits.end();) {
    auto & [set_id, actx] = *it;
    Db::Sets::const_iterator set_it = db.sets.find(set_id);
    QuiesceState set_state = QS__INVALID;

    int rc = db.get_age() >= actx.expire_at_age ? EINPROGRESS : EBUSY;

    if (set_it == db.sets.cend()) {
      rc = ENOENT;
    } else {
      auto const& set = set_it->second;
      set_state = set.rstate.state;
      switch(set_state) {
        case QS_CANCELED:
          rc = ECANCELED;
          break;
        case QS_EXPIRED:
        case QS_TIMEDOUT:
          rc = ETIMEDOUT;
          break;
        case QS_QUIESCED:
          rc = 0; // fallthrough
        case QS_QUIESCING:
          ceph_assert(!actx.req_ctx->request.is_release());
          break;
        case QS_RELEASED:
          rc = 0; // fallthrough
        case QS_RELEASING:
          if (!actx.req_ctx->request.is_release()) {
            // technically possible for a quiesce await
            // to get here if a concurrent release request
            // was submitted in the same batch;
            // see the corresponding check in
            // `leader_process_request`
            rc = EPERM;
          }
          break;
        case QS_FAILED:
          rc = EBADF;
          break;
        default: ceph_abort("unexpected quiesce set state");
      }
    }

    if (rc != EBUSY) {
      done_requests[actx.req_ctx] = rc;
      it = awaits.erase(it);
    } else {
      next_event_at_age = std::min(next_event_at_age, actx.expire_at_age);
      ++it;
    }
  }
  return next_event_at_age;
}

static QuiesceTimeInterval get_root_ttl(const QuiesceSet & set, const QuiesceSet::MemberInfo &member, QuiesceTimeInterval db_age) {

  QuiesceTimeInterval end_of_life = db_age;

  if (set.is_quiesced() || set.is_releasing()) {
    end_of_life = set.rstate.at_age + set.expiration;
  } else if (set.is_active()) {
    auto age = db_age; // taking the upper bound here
    if (member.is_quiescing()) {
      // we know that this member is on a timer
      age = member.rstate.at_age;
    }
    end_of_life = age + set.timeout; 
  }

  if (end_of_life > db_age) {
    return end_of_life - db_age;
  } else {
    return QuiesceTimeInterval::zero();
  }
}

void QuiesceDbManager::calculate_quiesce_map(QuiesceMap &map)
{
  map.roots.clear();
  map.db_version = db.version();
  auto db_age = db.get_age();

  for(auto & [set_id, set]: db.sets) {
    if (set.is_active()) {
      // we only report active sets;
      for(auto & [root, member]: set.members) {
        if (member.excluded) {
          continue;
        }

        // for a quiesce map, we want to report active roots as either QUIESCING or RELEASING
        // this is to make sure that clients always have a reason to report back and confirm
        // the quiesced state.
        auto requested = set.get_requested_member_state();
        auto ttl = get_root_ttl(set, member, db_age);
        auto root_it = map.roots.try_emplace(root, QuiesceMap::RootInfo { requested, ttl }).first;

        // the logic below resolves conditions when members representing the same root have different state/ttl
        // The state should be min, e.g. QUIESCING if at least one member is QUIESCING
        // The ttl should be large enough to cover all aggregated states, i.e. max
        root_it->second.state = std::min(root_it->second.state, requested);
        root_it->second.ttl = std::max(root_it->second.ttl, ttl);
      }
    }
  }
}
