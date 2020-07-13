// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <boost/lexical_cast.hpp>
#include "include/ceph_assert.h"  // lexical_cast includes system assert.h

#include <boost/config/warning_disable.hpp>
#include <boost/fusion/include/std_pair.hpp>
#include <boost/range/adaptor/reversed.hpp>

#include "MDSRank.h"
#include "Server.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Migrator.h"
#include "MDBalancer.h"
#include "InoTable.h"
#include "SnapClient.h"
#include "Mutation.h"
#include "MetricsHandler.h"
#include "cephfs_features.h"

#include "msg/Messenger.h"

#include "osdc/Objecter.h"

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/ESession.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"
#include "events/EPurged.h"

#include "include/stringify.h"
#include "include/filepath.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "include/compat.h"
#include "osd/OSDMap.h"

#include <errno.h>
#include <math.h>

#include <list>
#include <iostream>
#include <string_view>

#include "common/config.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".server "

class ServerContext : public MDSContext {
  protected:
  Server *server;
  MDSRank *get_mds() override
  {
    return server->mds;
  }

  public:
  explicit ServerContext(Server *s) : server(s) {
    ceph_assert(server != NULL);
  }
};

class Batch_Getattr_Lookup : public BatchOp {
protected:
  Server* server;
  ceph::ref_t<MDRequestImpl> mdr;
  std::vector<ceph::ref_t<MDRequestImpl>> batch_reqs;
  int res = 0;
public:
  Batch_Getattr_Lookup(Server* s, const ceph::ref_t<MDRequestImpl>& r)
    : server(s), mdr(r) {
    if (mdr->client_request->get_op() == CEPH_MDS_OP_LOOKUP)
      mdr->batch_op_map = &mdr->dn[0].back()->batch_ops;
    else
      mdr->batch_op_map = &mdr->in[0]->batch_ops;
  }
  void add_request(const ceph::ref_t<MDRequestImpl>& r) override {
    batch_reqs.push_back(r);
  }
  ceph::ref_t<MDRequestImpl> find_new_head() override {
    while (!batch_reqs.empty()) {
      auto r = std::move(batch_reqs.back());
      batch_reqs.pop_back();
      if (r->killed)
	continue;

      r->batch_op_map = mdr->batch_op_map;
      mdr->batch_op_map = nullptr;
      mdr = r;
      return mdr;
    }
    return nullptr;
  }
  void _forward(mds_rank_t t) override {
    MDCache* mdcache = server->mdcache;
    mdcache->mds->forward_message_mds(mdr->release_client_request(), t);
    mdr->set_mds_stamp(ceph_clock_now());
    for (auto& m : batch_reqs) {
      if (!m->killed)
	mdcache->request_forward(m, t);
    }
    batch_reqs.clear();
  }
  void _respond(int r) override {
    mdr->set_mds_stamp(ceph_clock_now());
    for (auto& m : batch_reqs) {
      if (!m->killed) {
	m->tracei = mdr->tracei;
	m->tracedn = mdr->tracedn;
	server->respond_to_request(m, r);
      }
    }
    batch_reqs.clear();
    server->reply_client_request(mdr, make_message<MClientReply>(*mdr->client_request, r));
  }
  void print(std::ostream& o) {
    o << "[batch front=" << *mdr << "]";
  }
};

class ServerLogContext : public MDSLogContextBase {
protected:
  Server *server;
  MDSRank *get_mds() override
  {
    return server->mds;
  }

  MDRequestRef mdr;
  void pre_finish(int r) override {
    if (mdr)
      mdr->mark_event("journal_committed: ");
  }
public:
  explicit ServerLogContext(Server *s) : server(s) {
    ceph_assert(server != NULL);
  }
  explicit ServerLogContext(Server *s, MDRequestRef& r) : server(s), mdr(r) {
    ceph_assert(server != NULL);
  }
};

void Server::create_logger()
{
  PerfCountersBuilder plb(g_ceph_context, "mds_server", l_mdss_first, l_mdss_last);

  plb.add_u64_counter(l_mdss_handle_client_request, "handle_client_request",
                      "Client requests", "hcr", PerfCountersBuilder::PRIO_INTERESTING);
  plb.add_u64_counter(l_mdss_handle_slave_request, "handle_slave_request",
                      "Slave requests", "hsr", PerfCountersBuilder::PRIO_INTERESTING);
  plb.add_u64_counter(l_mdss_handle_client_session,
                      "handle_client_session", "Client session messages", "hcs",
                      PerfCountersBuilder::PRIO_INTERESTING);
  plb.add_u64_counter(l_mdss_cap_revoke_eviction, "cap_revoke_eviction",
                      "Cap Revoke Client Eviction", "cre", PerfCountersBuilder::PRIO_INTERESTING);

  // fop latencies are useful
  plb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  plb.add_time_avg(l_mdss_req_lookuphash_latency, "req_lookuphash_latency",
                   "Request type lookup hash of inode latency");
  plb.add_time_avg(l_mdss_req_lookupino_latency, "req_lookupino_latency",
                   "Request type lookup inode latency");
  plb.add_time_avg(l_mdss_req_lookupparent_latency, "req_lookupparent_latency",
                   "Request type lookup parent latency");
  plb.add_time_avg(l_mdss_req_lookupname_latency, "req_lookupname_latency",
                   "Request type lookup name latency");
  plb.add_time_avg(l_mdss_req_lookup_latency, "req_lookup_latency",
                   "Request type lookup latency");
  plb.add_time_avg(l_mdss_req_lookupsnap_latency, "req_lookupsnap_latency",
                   "Request type lookup snapshot latency");
  plb.add_time_avg(l_mdss_req_getattr_latency, "req_getattr_latency",
                   "Request type get attribute latency");
  plb.add_time_avg(l_mdss_req_setattr_latency, "req_setattr_latency",
                   "Request type set attribute latency");
  plb.add_time_avg(l_mdss_req_setlayout_latency, "req_setlayout_latency",
                   "Request type set file layout latency");
  plb.add_time_avg(l_mdss_req_setdirlayout_latency, "req_setdirlayout_latency",
                   "Request type set directory layout latency");
  plb.add_time_avg(l_mdss_req_setxattr_latency, "req_setxattr_latency",
                   "Request type set extended attribute latency");
  plb.add_time_avg(l_mdss_req_rmxattr_latency, "req_rmxattr_latency",
                   "Request type remove extended attribute latency");
  plb.add_time_avg(l_mdss_req_readdir_latency, "req_readdir_latency",
                   "Request type read directory latency");
  plb.add_time_avg(l_mdss_req_setfilelock_latency, "req_setfilelock_latency",
                   "Request type set file lock latency");
  plb.add_time_avg(l_mdss_req_getfilelock_latency, "req_getfilelock_latency",
                   "Request type get file lock latency");
  plb.add_time_avg(l_mdss_req_create_latency, "req_create_latency",
                   "Request type create latency");
  plb.add_time_avg(l_mdss_req_open_latency, "req_open_latency",
                   "Request type open latency");
  plb.add_time_avg(l_mdss_req_mknod_latency, "req_mknod_latency",
                   "Request type make node latency");
  plb.add_time_avg(l_mdss_req_link_latency, "req_link_latency",
                   "Request type link latency");
  plb.add_time_avg(l_mdss_req_unlink_latency, "req_unlink_latency",
                   "Request type unlink latency");
  plb.add_time_avg(l_mdss_req_rmdir_latency, "req_rmdir_latency",
                   "Request type remove directory latency");
  plb.add_time_avg(l_mdss_req_rename_latency, "req_rename_latency",
                   "Request type rename latency");
  plb.add_time_avg(l_mdss_req_mkdir_latency, "req_mkdir_latency",
                   "Request type make directory latency");
  plb.add_time_avg(l_mdss_req_symlink_latency, "req_symlink_latency",
                   "Request type symbolic link latency");
  plb.add_time_avg(l_mdss_req_lssnap_latency, "req_lssnap_latency",
                   "Request type list snapshot latency");
  plb.add_time_avg(l_mdss_req_mksnap_latency, "req_mksnap_latency",
                   "Request type make snapshot latency");
  plb.add_time_avg(l_mdss_req_rmsnap_latency, "req_rmsnap_latency",
                   "Request type remove snapshot latency");
  plb.add_time_avg(l_mdss_req_renamesnap_latency, "req_renamesnap_latency",
                   "Request type rename snapshot latency");

  plb.set_prio_default(PerfCountersBuilder::PRIO_DEBUGONLY);
  plb.add_u64_counter(l_mdss_dispatch_client_request, "dispatch_client_request",
                      "Client requests dispatched");
  plb.add_u64_counter(l_mdss_dispatch_slave_request, "dispatch_server_request",
                      "Server requests dispatched");

  logger = plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

Server::Server(MDSRank *m, MetricsHandler *metrics_handler) :
  mds(m), 
  mdcache(mds->mdcache), mdlog(mds->mdlog),
  recall_throttle(g_conf().get_val<double>("mds_recall_max_decay_rate")),
  metrics_handler(metrics_handler)
{
  replay_unsafe_with_closed_session = g_conf().get_val<bool>("mds_replay_unsafe_with_closed_session");
  cap_revoke_eviction_timeout = g_conf().get_val<double>("mds_cap_revoke_eviction_timeout");
  max_snaps_per_dir = g_conf().get_val<uint64_t>("mds_max_snaps_per_dir");
  delegate_inos_pct = g_conf().get_val<uint64_t>("mds_client_delegate_inos_pct");
  supported_features = feature_bitset_t(CEPHFS_FEATURES_MDS_SUPPORTED);
}

void Server::dispatch(const cref_t<Message> &m)
{
  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_RECONNECT:
    handle_client_reconnect(ref_cast<MClientReconnect>(m));
    return;
  }

/*
 *In reconnect phase, client sent unsafe requests to mds before reconnect msg. Seting sessionclosed_isok will handle scenario like this:

1. In reconnect phase, client sent unsafe requests to mds.
2. It reached reconnect timeout. All sessions without sending reconnect msg in time, some of which may had sent unsafe requests, are marked as closed.
(Another situation is #31668, which will deny all client reconnect msg to speed up reboot).
3.So these unsafe request from session without sending reconnect msg in time or being denied could be handled in clientreplay phase.

*/
  bool sessionclosed_isok = replay_unsafe_with_closed_session;
  // active?
  // handle_slave_request()/handle_client_session() will wait if necessary
  if (m->get_type() == CEPH_MSG_CLIENT_REQUEST && !mds->is_active()) {
    const auto &req = ref_cast<MClientRequest>(m);
    if (mds->is_reconnect() || mds->get_want_state() == CEPH_MDS_STATE_RECONNECT) {
      Session *session = mds->get_session(req);
      if (!session || (!session->is_open() && !sessionclosed_isok)) {
	dout(5) << "session is closed, dropping " << req->get_reqid() << dendl;
	return;
      }
      bool queue_replay = false;
      if (req->is_replay() || req->is_async()) {
	dout(3) << "queuing replayed op" << dendl;
	queue_replay = true;
	if (req->head.ino &&
	    !session->have_completed_request(req->get_reqid().tid, nullptr)) {
	  mdcache->add_replay_ino_alloc(inodeno_t(req->head.ino));
	}
      } else if (req->get_retry_attempt()) {
	// process completed request in clientreplay stage. The completed request
	// might have created new file/directorie. This guarantees MDS sends a reply
	// to client before other request modifies the new file/directorie.
	if (session->have_completed_request(req->get_reqid().tid, NULL)) {
	  dout(3) << "queuing completed op" << dendl;
	  queue_replay = true;
	}
	// this request was created before the cap reconnect message, drop any embedded
	// cap releases.
	req->releases.clear();
      }
      if (queue_replay) {
	req->mark_queued_for_replay();
	mds->enqueue_replay(new C_MDS_RetryMessage(mds, m));
	return;
      }
    }

    bool wait_for_active = true;
    if (mds->is_stopping()) {
      wait_for_active = false;
    } else if (mds->is_clientreplay()) {
      if (req->is_queued_for_replay()) {
	wait_for_active = false;
      }
    }
    if (wait_for_active) {
      dout(3) << "not active yet, waiting" << dendl;
      mds->wait_for_active(new C_MDS_RetryMessage(mds, m));
      return;
    }
  }

  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_SESSION:
    handle_client_session(ref_cast<MClientSession>(m));
    return;
  case CEPH_MSG_CLIENT_REQUEST:
    handle_client_request(ref_cast<MClientRequest>(m));
    return;
  case CEPH_MSG_CLIENT_RECLAIM:
    handle_client_reclaim(ref_cast<MClientReclaim>(m));
    return;
  case MSG_MDS_SLAVE_REQUEST:
    handle_slave_request(ref_cast<MMDSSlaveRequest>(m));
    return;
  default:
    derr << "server unknown message " << m->get_type() << dendl;
    ceph_abort_msg("server unknown message");  
  }
}



// ----------------------------------------------------------
// SESSION management

class C_MDS_session_finish : public ServerLogContext {
  Session *session;
  uint64_t state_seq;
  bool open;
  version_t cmapv;
  interval_set<inodeno_t> inos;
  version_t inotablev;
  interval_set<inodeno_t> purge_inos;
  LogSegment *ls = nullptr;
  Context *fin;
public:
  C_MDS_session_finish(Server *srv, Session *se, uint64_t sseq, bool s, version_t mv, Context *fin_ = NULL) :
    ServerLogContext(srv), session(se), state_seq(sseq), open(s), cmapv(mv), inotablev(0), fin(fin_) { }
  C_MDS_session_finish(Server *srv, Session *se, uint64_t sseq, bool s, version_t mv, interval_set<inodeno_t> i, version_t iv, Context *fin_ = NULL) :
    ServerLogContext(srv), session(se), state_seq(sseq), open(s), cmapv(mv), inos(std::move(i)), inotablev(iv), fin(fin_) { }
  C_MDS_session_finish(Server *srv, Session *se, uint64_t sseq, bool s, version_t mv, interval_set<inodeno_t> i, version_t iv,
		       interval_set<inodeno_t> _purge_inos, LogSegment *_ls, Context *fin_ = NULL) :
    ServerLogContext(srv), session(se), state_seq(sseq), open(s), cmapv(mv), inos(std::move(i)), inotablev(iv), purge_inos(std::move(_purge_inos)), ls(_ls), fin(fin_){}
  void finish(int r) override {
    ceph_assert(r == 0);
    server->_session_logged(session, state_seq, open, cmapv, inos, inotablev, purge_inos, ls);
    if (fin) {
      fin->complete(r);
    }
  }
};

Session* Server::find_session_by_uuid(std::string_view uuid)
{
  Session* session = nullptr;
  for (auto& it : mds->sessionmap.get_sessions()) {
    auto& metadata = it.second->info.client_metadata;

    auto p = metadata.find("uuid");
    if (p == metadata.end() || p->second != uuid)
      continue;

    if (!session) {
      session = it.second;
    } else if (!session->reclaiming_from) {
      assert(it.second->reclaiming_from == session);
      session = it.second;
    } else {
      assert(session->reclaiming_from == it.second);
    }
  }
  return session;
}

void Server::reclaim_session(Session *session, const cref_t<MClientReclaim> &m)
{
  if (!session->is_open() && !session->is_stale()) {
    dout(10) << "session not open, dropping this req" << dendl;
    return;
  }

  auto reply = make_message<MClientReclaimReply>(0);
  if (m->get_uuid().empty()) {
    dout(10) << __func__ << " invalid message (no uuid)" << dendl;
    reply->set_result(-EINVAL);
    mds->send_message_client(reply, session);
    return;
  }

  unsigned flags = m->get_flags();
  if (flags != CEPH_RECLAIM_RESET) { // currently only support reset
    dout(10) << __func__ << " unsupported flags" << dendl;
    reply->set_result(-EOPNOTSUPP);
    mds->send_message_client(reply, session);
    return;
  }

  Session* target = find_session_by_uuid(m->get_uuid());
  if (target) {
    if (session->info.auth_name != target->info.auth_name) {
      dout(10) << __func__ << " session auth_name " << session->info.auth_name
	       << " != target auth_name " << target->info.auth_name << dendl;
      reply->set_result(-EPERM);
      mds->send_message_client(reply, session);
    }

    assert(!target->reclaiming_from);
    assert(!session->reclaiming_from);
    session->reclaiming_from = target;
    reply->set_addrs(entity_addrvec_t(target->info.inst.addr));
  }

  if (flags & CEPH_RECLAIM_RESET) {
    finish_reclaim_session(session, reply);
    return;
  }

  ceph_abort();
}

void Server::finish_reclaim_session(Session *session, const ref_t<MClientReclaimReply> &reply)
{
  Session *target = session->reclaiming_from;
  if (target) {
    session->reclaiming_from = nullptr;

    Context *send_reply;
    if (reply) {
      int64_t session_id = session->get_client().v;
      send_reply = new LambdaContext([this, session_id, reply](int r) {
	    assert(ceph_mutex_is_locked_by_me(mds->mds_lock));
	    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(session_id));
	    if (!session) {
	      return;
	    }
	    auto epoch = mds->objecter->with_osdmap([](const OSDMap &map){ return map.get_epoch(); });
	    reply->set_epoch(epoch);
	    mds->send_message_client(reply, session);
	  });
    } else {
      send_reply = nullptr;
    }

    bool blacklisted = mds->objecter->with_osdmap([target](const OSDMap &map) {
	  return map.is_blacklisted(target->info.inst.addr);
	});

    if (blacklisted || !g_conf()->mds_session_blacklist_on_evict) {
      kill_session(target, send_reply);
    } else {
      std::stringstream ss;
      mds->evict_client(target->get_client().v, false, true, ss, send_reply);
    }
  } else if (reply) {
    mds->send_message_client(reply, session);
  }
}

void Server::handle_client_reclaim(const cref_t<MClientReclaim> &m)
{
  Session *session = mds->get_session(m);
  dout(3) << __func__ <<  " " << *m << " from " << m->get_source() << dendl;
  assert(m->get_source().is_client()); // should _not_ come from an mds!

  if (!session) {
    dout(0) << " ignoring sessionless msg " << *m << dendl;
    return;
  }

  if (mds->get_state() < MDSMap::STATE_CLIENTREPLAY) {
    mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (m->get_flags() & MClientReclaim::FLAG_FINISH) {
    finish_reclaim_session(session);
  } else {
    reclaim_session(session, m);
  }
}

void Server::handle_client_session(const cref_t<MClientSession> &m)
{
  version_t pv;
  Session *session = mds->get_session(m);

  dout(3) << "handle_client_session " << *m << " from " << m->get_source() << dendl;
  ceph_assert(m->get_source().is_client()); // should _not_ come from an mds!

  if (!session) {
    dout(0) << " ignoring sessionless msg " << *m << dendl;
    auto reply = make_message<MClientSession>(CEPH_SESSION_REJECT);
    reply->metadata["error_string"] = "sessionless";
    mds->send_message(reply, m->get_connection());
    return;
  }

  if (m->get_op() == CEPH_SESSION_REQUEST_RENEWCAPS) {
    // always handle renewcaps (state >= MDSMap::STATE_RECONNECT)
  } else if (m->get_op() == CEPH_SESSION_REQUEST_CLOSE) {
    // close requests need to be handled when mds is active
    if (mds->get_state() < MDSMap::STATE_ACTIVE) {
      mds->wait_for_active(new C_MDS_RetryMessage(mds, m));
      return;
    }
  } else {
    if (mds->get_state() < MDSMap::STATE_CLIENTREPLAY) {
      mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
      return;
    }
  }

  if (logger)
    logger->inc(l_mdss_handle_client_session);

  uint64_t sseq = 0;
  switch (m->get_op()) {
  case CEPH_SESSION_REQUEST_OPEN:
    if (session->is_opening() ||
	session->is_open() ||
	session->is_stale() ||
	session->is_killing() ||
	terminating_sessions) {
      dout(10) << "currently open|opening|stale|killing, dropping this req" << dendl;
      return;
    }
    ceph_assert(session->is_closed() || session->is_closing());

    if (mds->is_stopping()) {
      dout(10) << "mds is stopping, dropping open req" << dendl;
      return;
    }

    {
      auto& addr = session->info.inst.addr;
      session->set_client_metadata(client_metadata_t(m->metadata, m->supported_features, m->metric_spec));
      auto& client_metadata = session->info.client_metadata;

      auto log_session_status = [this, m, session](std::string_view status, std::string_view err) {
        auto now = ceph_clock_now();
        auto throttle_elapsed = m->get_recv_complete_stamp() - m->get_throttle_stamp();
        auto elapsed = now - m->get_recv_stamp();
        CachedStackStringStream css;
        *css << "New client session:"
             << " addr=\"" <<  session->info.inst.addr << "\""
             << ",elapsed=" << elapsed
             << ",throttled=" << throttle_elapsed
             << ",status=\"" << status << "\"";
        if (!err.empty()) {
          *css << ",error=\"" << err << "\"";
        }
        const auto& metadata = session->info.client_metadata;
        if (auto it = metadata.find("root"); it != metadata.end()) {
          *css << ",root=\"" << it->second << "\"";
        }
        dout(2) << css->strv() << dendl;
      };

      auto send_reject_message = [this, &session, &log_session_status](std::string_view err_str) {
	auto m = make_message<MClientSession>(CEPH_SESSION_REJECT);
	if (session->info.has_feature(CEPHFS_FEATURE_MIMIC))
	  m->metadata["error_string"] = err_str;
	mds->send_message_client(m, session);
        log_session_status("REJECTED", err_str);
      };

      bool blacklisted = mds->objecter->with_osdmap(
	  [&addr](const OSDMap &osd_map) -> bool {
	    return osd_map.is_blacklisted(addr);
	  });

      if (blacklisted) {
	dout(10) << "rejecting blacklisted client " << addr << dendl;
	send_reject_message("blacklisted");
	session->clear();
	break;
      }

      if (client_metadata.features.empty())
	infer_supported_features(session, client_metadata);

      dout(20) << __func__ << " CEPH_SESSION_REQUEST_OPEN metadata entries:" << dendl;
      dout(20) << " features: '" << client_metadata.features << "'" << dendl;
      dout(20) << " metric specification: [" << client_metadata.metric_spec << "]" << dendl;
      for (const auto& p : client_metadata) {
	dout(20) << "  " << p.first << ": " << p.second << dendl;
      }

      feature_bitset_t missing_features = required_client_features;
      missing_features -= client_metadata.features;
      if (!missing_features.empty()) {
	stringstream ss;
	ss << "missing required features '" << missing_features << "'";
	send_reject_message(ss.str());
	mds->clog->warn() << "client session (" << session->info.inst
                          << ") lacks required features " << missing_features
                          << "; client supports " << client_metadata.features;
	session->clear();
	break;
      }

      // Special case for the 'root' metadata path; validate that the claimed
      // root is actually within the caps of the session
      if (auto it = client_metadata.find("root"); it != client_metadata.end()) {
	auto claimed_root = it->second;
	stringstream ss;
	bool denied = false;
	// claimed_root has a leading "/" which we strip before passing
	// into caps check
	if (claimed_root.empty() || claimed_root[0] != '/') {
	  denied = true;
	  ss << "invalue root '" << claimed_root << "'";
	} else if (!session->auth_caps.path_capable(claimed_root.substr(1))) {
	  denied = true;
	  ss << "non-allowable root '" << claimed_root << "'";
	}

	if (denied) {
	  // Tell the client we're rejecting their open
	  send_reject_message(ss.str());
	  mds->clog->warn() << "client session with " << ss.str()
			    << " denied (" << session->info.inst << ")";
	  session->clear();
	  break;
	}
      }

      if (auto it = client_metadata.find("uuid"); it != client_metadata.end()) {
	if (find_session_by_uuid(it->second)) {
	  send_reject_message("duplicated session uuid");
	  mds->clog->warn() << "client session with duplicated session uuid '"
			    << it->second << "' denied (" << session->info.inst << ")";
	  session->clear();
	  break;
	}
      }

      if (session->is_closed()) {
        mds->sessionmap.add_session(session);
      }

      pv = mds->sessionmap.mark_projected(session);
      sseq = mds->sessionmap.set_state(session, Session::STATE_OPENING);
      mds->sessionmap.touch_session(session);
      auto fin = new LambdaContext([log_session_status = std::move(log_session_status)](int r){
        ceph_assert(r == 0);
        log_session_status("ACCEPTED", "");
      });
      mdlog->start_submit_entry(new ESession(m->get_source_inst(), true, pv, client_metadata),
				new C_MDS_session_finish(this, session, sseq, true, pv, fin));
      mdlog->flush();
    }
    break;

  case CEPH_SESSION_REQUEST_RENEWCAPS:
    if (session->is_open() || session->is_stale()) {
      mds->sessionmap.touch_session(session);
      if (session->is_stale()) {
	mds->sessionmap.set_state(session, Session::STATE_OPEN);
	mds->locker->resume_stale_caps(session);
	mds->sessionmap.touch_session(session);
      }
      auto reply = make_message<MClientSession>(CEPH_SESSION_RENEWCAPS, m->get_seq());
      mds->send_message_client(reply, session);
    } else {
      dout(10) << "ignoring renewcaps on non open|stale session (" << session->get_state_name() << ")" << dendl;
    }
    break;
    
  case CEPH_SESSION_REQUEST_CLOSE:
    {
      if (session->is_closed() || 
	  session->is_closing() ||
	  session->is_killing()) {
	dout(10) << "already closed|closing|killing, dropping this req" << dendl;
	return;
      }
      if (session->is_importing()) {
	dout(10) << "ignoring close req on importing session" << dendl;
	return;
      }
      ceph_assert(session->is_open() || 
	     session->is_stale() || 
	     session->is_opening());
      if (m->get_seq() < session->get_push_seq()) {
	dout(10) << "old push seq " << m->get_seq() << " < " << session->get_push_seq() 
		 << ", dropping" << dendl;
	return;
      }
      // We are getting a seq that is higher than expected.
      // Handle the same as any other seqn error.
      //
      if (m->get_seq() != session->get_push_seq()) {
	dout(0) << "old push seq " << m->get_seq() << " != " << session->get_push_seq()
		<< ", BUGGY!" << dendl;
	mds->clog->warn() << "incorrect push seq " << m->get_seq() << " != "
			  << session->get_push_seq() << ", dropping" << " from client : " << session->get_human_name();
	return;
      }
      journal_close_session(session, Session::STATE_CLOSING, NULL);
    }
    break;

  case CEPH_SESSION_FLUSHMSG_ACK:
    finish_flush_session(session, m->get_seq());
    break;

  case CEPH_SESSION_REQUEST_FLUSH_MDLOG:
    if (mds->is_active())
      mdlog->flush();
    break;

  default:
    ceph_abort();
  }
}

void Server::flush_session(Session *session, MDSGatherBuilder *gather) {
  if (!session->is_open() ||
      !session->get_connection() ||
      !session->get_connection()->has_feature(CEPH_FEATURE_EXPORT_PEER)) {
    return;
  }

  version_t seq = session->wait_for_flush(gather->new_sub());
  mds->send_message_client(
    make_message<MClientSession>(CEPH_SESSION_FLUSHMSG, seq), session);
}

void Server::flush_client_sessions(set<client_t>& client_set, MDSGatherBuilder& gather)
{
  for (set<client_t>::iterator p = client_set.begin(); p != client_set.end(); ++p) {
    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(p->v));
    ceph_assert(session);
    flush_session(session, &gather);
  }
}

void Server::finish_flush_session(Session *session, version_t seq)
{
  MDSContext::vec finished;
  session->finish_flush(seq, finished);
  mds->queue_waiters(finished);
}

void Server::_session_logged(Session *session, uint64_t state_seq, bool open, version_t pv,
			     const interval_set<inodeno_t>& inos, version_t piv,
			     const interval_set<inodeno_t>& purge_inos, LogSegment *ls)
{
  dout(10) << "_session_logged " << session->info.inst
	   << " state_seq " << state_seq
	   << " " << (open ? "open":"close")
	   << " " << pv
	   << " purge_inos : " << purge_inos << dendl;
  
  if (NULL != ls) {
    dout(10)  << "_session_logged seq : " << ls->seq << dendl;
    if (purge_inos.size()){
      ls->purge_inodes.insert(purge_inos);
      mdcache->purge_inodes(purge_inos, ls);
    }
  }
  
  if (piv) {
    ceph_assert(session->is_closing() || session->is_killing() ||
	   session->is_opening()); // re-open closing session
    session->info.prealloc_inos.subtract(inos);
    session->delegated_inos.clear();
    mds->inotable->apply_release_ids(inos);
    ceph_assert(mds->inotable->get_version() == piv);
  }

  mds->sessionmap.mark_dirty(session);

  // apply
  if (session->get_state_seq() != state_seq) {
    dout(10) << " journaled state_seq " << state_seq << " != current " << session->get_state_seq()
	     << ", noop" << dendl;
    // close must have been canceled (by an import?), or any number of other things..
  } else if (open) {
    ceph_assert(session->is_opening());
    mds->sessionmap.set_state(session, Session::STATE_OPEN);
    mds->sessionmap.touch_session(session);
    metrics_handler->add_session(session);
    ceph_assert(session->get_connection());
    auto reply = make_message<MClientSession>(CEPH_SESSION_OPEN);
    if (session->info.has_feature(CEPHFS_FEATURE_MIMIC))
      reply->supported_features = supported_features;
    mds->send_message_client(reply, session);
    if (mdcache->is_readonly()) {
      auto m = make_message<MClientSession>(CEPH_SESSION_FORCE_RO);
      mds->send_message_client(m, session);
    }
  } else if (session->is_closing() ||
	     session->is_killing()) {
    // kill any lingering capabilities, leases, requests
    while (!session->caps.empty()) {
      Capability *cap = session->caps.front();
      CInode *in = cap->get_inode();
      dout(20) << " killing capability " << ccap_string(cap->issued()) << " on " << *in << dendl;
      mds->locker->remove_client_cap(in, cap, true);
    }
    while (!session->leases.empty()) {
      ClientLease *r = session->leases.front();
      CDentry *dn = static_cast<CDentry*>(r->parent);
      dout(20) << " killing client lease of " << *dn << dendl;
      dn->remove_client_lease(r, mds->locker);
    }
    if (client_reconnect_gather.erase(session->info.get_client())) {
      dout(20) << " removing client from reconnect set" << dendl;
      if (client_reconnect_gather.empty()) {
        dout(7) << " client " << session->info.inst << " was last reconnect, finishing" << dendl;
        reconnect_gather_finish();
      }
    }
    if (client_reclaim_gather.erase(session->info.get_client())) {
      dout(20) << " removing client from reclaim set" << dendl;
      if (client_reclaim_gather.empty()) {
        dout(7) << " client " << session->info.inst << " was last reclaimed, finishing" << dendl;
	mds->maybe_clientreplay_done();
      }
    }
    
    if (session->is_closing()) {
      // mark con disposable.  if there is a fault, we will get a
      // reset and clean it up.  if the client hasn't received the
      // CLOSE message yet, they will reconnect and get an
      // ms_handle_remote_reset() and realize they had in fact closed.
      // do this *before* sending the message to avoid a possible
      // race.
      if (session->get_connection()) {
        // Conditional because terminate_sessions will indiscrimately
        // put sessions in CLOSING whether they ever had a conn or not.
        session->get_connection()->mark_disposable();
      }

      // reset session
      mds->send_message_client(make_message<MClientSession>(CEPH_SESSION_CLOSE), session);
      mds->sessionmap.set_state(session, Session::STATE_CLOSED);
      session->clear();
      metrics_handler->remove_session(session);
      mds->sessionmap.remove_session(session);
    } else if (session->is_killing()) {
      // destroy session, close connection
      if (session->get_connection()) {
        session->get_connection()->mark_down();
        mds->sessionmap.set_state(session, Session::STATE_CLOSED);
        session->set_connection(nullptr);
      }
      metrics_handler->remove_session(session);
      mds->sessionmap.remove_session(session);
    } else {
      ceph_abort();
    }
  } else {
    ceph_abort();
  }
}

/**
 * Inject sessions from some source other than actual connections.
 *
 * For example:
 *  - sessions inferred from journal replay
 *  - sessions learned from other MDSs during rejoin
 *  - sessions learned from other MDSs during dir/caps migration
 *  - sessions learned from other MDSs during a cross-MDS rename
 */
version_t Server::prepare_force_open_sessions(map<client_t,entity_inst_t>& cm,
					      map<client_t,client_metadata_t>& cmm,
					      map<client_t, pair<Session*,uint64_t> >& smap)
{
  version_t pv = mds->sessionmap.get_projected();

  dout(10) << "prepare_force_open_sessions " << pv 
	   << " on " << cm.size() << " clients"
	   << dendl;

  mds->objecter->with_osdmap(
      [this, &cm, &cmm](const OSDMap &osd_map) {
	for (auto p = cm.begin(); p != cm.end(); ) {
	  if (osd_map.is_blacklisted(p->second.addr)) {
	    dout(10) << " ignoring blacklisted client." << p->first
		     << " (" <<  p->second.addr << ")" << dendl;
	    cmm.erase(p->first);
	    cm.erase(p++);
	  } else {
	    ++p;
	  }
	}
      });

  for (map<client_t,entity_inst_t>::iterator p = cm.begin(); p != cm.end(); ++p) {
    Session *session = mds->sessionmap.get_or_add_session(p->second);
    pv = mds->sessionmap.mark_projected(session);
    uint64_t sseq;
    if (session->is_closed() || 
	session->is_closing() ||
	session->is_killing()) {
      sseq = mds->sessionmap.set_state(session, Session::STATE_OPENING);
      auto q = cmm.find(p->first);
      if (q != cmm.end())
	session->info.client_metadata.merge(q->second);
    } else {
      ceph_assert(session->is_open() ||
	     session->is_opening() ||
	     session->is_stale());
      sseq = 0;
    }
    smap[p->first] = make_pair(session, sseq);
    session->inc_importing();
  }
  return pv;
}

void Server::finish_force_open_sessions(const map<client_t,pair<Session*,uint64_t> >& smap,
					bool dec_import)
{
  /*
   * FIXME: need to carefully consider the race conditions between a
   * client trying to close a session and an MDS doing an import
   * trying to force open a session...  
   */
  dout(10) << "finish_force_open_sessions on " << smap.size() << " clients,"
	   << " initial v " << mds->sessionmap.get_version() << dendl;

  for (auto &it : smap) {
    Session *session = it.second.first;
    uint64_t sseq = it.second.second;
    if (sseq > 0) {
      if (session->get_state_seq() != sseq) {
	dout(10) << "force_open_sessions skipping changed " << session->info.inst << dendl;
      } else {
	dout(10) << "force_open_sessions opened " << session->info.inst << dendl;
	mds->sessionmap.set_state(session, Session::STATE_OPEN);
	mds->sessionmap.touch_session(session);
        metrics_handler->add_session(session);

	auto reply = make_message<MClientSession>(CEPH_SESSION_OPEN);
	if (session->info.has_feature(CEPHFS_FEATURE_MIMIC))
	  reply->supported_features = supported_features;
	mds->send_message_client(reply, session);

	if (mdcache->is_readonly())
	  mds->send_message_client(make_message<MClientSession>(CEPH_SESSION_FORCE_RO), session);
      }
    } else {
      dout(10) << "force_open_sessions skipping already-open " << session->info.inst << dendl;
      ceph_assert(session->is_open() || session->is_stale());
    }

    if (dec_import) {
      session->dec_importing();
    }

    mds->sessionmap.mark_dirty(session);
  }

  dout(10) << __func__ << ": final v " << mds->sessionmap.get_version() << dendl;
}

class C_MDS_TerminatedSessions : public ServerContext {
  void finish(int r) override {
    server->terminating_sessions = false;
  }
  public:
  explicit C_MDS_TerminatedSessions(Server *s) : ServerContext(s) {}
};

void Server::terminate_sessions()
{
  dout(5) << "terminating all sessions..." << dendl;

  terminating_sessions = true;

  // kill them off.  clients will retry etc.
  set<Session*> sessions;
  mds->sessionmap.get_client_session_set(sessions);
  for (set<Session*>::const_iterator p = sessions.begin();
       p != sessions.end();
       ++p) {
    Session *session = *p;
    if (session->is_closing() ||
	session->is_killing() ||
	session->is_closed())
      continue;
    journal_close_session(session, Session::STATE_CLOSING, NULL);
  }

  mdlog->wait_for_safe(new C_MDS_TerminatedSessions(this));
}


void Server::find_idle_sessions()
{
  auto now = clock::now();
  auto last_cleared_laggy = mds->last_cleared_laggy();

  dout(10) << "find_idle_sessions. last cleared laggy state " << last_cleared_laggy << "s ago" << dendl;
  
  // timeout/stale
  //  (caps go stale, lease die)
  double queue_max_age = mds->get_dispatch_queue_max_age(ceph_clock_now());
  double cutoff = queue_max_age + mds->mdsmap->get_session_timeout();

  // don't kick clients if we've been laggy
  if (last_cleared_laggy < cutoff) {
    dout(10) << " last cleared laggy " << last_cleared_laggy << "s ago (< cutoff " << cutoff
	     << "), not marking any client stale" << dendl;
    return;
  }

  std::vector<Session*> to_evict;

  bool defer_session_stale = g_conf().get_val<bool>("mds_defer_session_stale");
  const auto sessions_p1 = mds->sessionmap.by_state.find(Session::STATE_OPEN);
  if (sessions_p1 != mds->sessionmap.by_state.end() && !sessions_p1->second->empty()) {
    std::vector<Session*> new_stale;

    for (auto session : *(sessions_p1->second)) {
      auto last_cap_renew_span = std::chrono::duration<double>(now - session->last_cap_renew).count();
      if (last_cap_renew_span < cutoff) {
	dout(20) << "laggiest active session is " << session->info.inst
		 << " and renewed caps recently (" << last_cap_renew_span << "s ago)" << dendl;
	break;
      }

      if (session->last_seen > session->last_cap_renew) {
	last_cap_renew_span = std::chrono::duration<double>(now - session->last_seen).count();
	if (last_cap_renew_span < cutoff) {
	  dout(20) << "laggiest active session is " << session->info.inst
		   << " and renewed caps recently (" << last_cap_renew_span << "s ago)" << dendl;
	  continue;
	}
      }

      if (last_cap_renew_span >= mds->mdsmap->get_session_autoclose()) {
	dout(20) << "evicting session " << session->info.inst << " since autoclose "
		    "has arrived" << dendl;
	// evict session without marking it stale
	to_evict.push_back(session);
	continue;
      }

      if (defer_session_stale &&
	  !session->is_any_flush_waiter() &&
	  !mds->locker->is_revoking_any_caps_from(session->get_client())) {
	dout(20) << "deferring marking session " << session->info.inst << " stale "
		    "since it holds no caps" << dendl;
	continue;
      }

      auto it = session->info.client_metadata.find("timeout");
      if (it != session->info.client_metadata.end()) {
	unsigned timeout = strtoul(it->second.c_str(), nullptr, 0);
	if (timeout == 0) {
	  dout(10) << "skipping session " << session->info.inst
		   << ", infinite timeout specified" << dendl;
	  continue;
	}
	double cutoff = queue_max_age + timeout;
	if  (last_cap_renew_span < cutoff) {
	  dout(10) << "skipping session " << session->info.inst
		   << ", timeout (" << timeout << ") specified"
		   << " and renewed caps recently (" << last_cap_renew_span << "s ago)" << dendl;
	  continue;
	}

	// do not go through stale, evict it directly.
	to_evict.push_back(session);
      } else {
	dout(10) << "new stale session " << session->info.inst
		 << " last renewed caps " << last_cap_renew_span << "s ago" << dendl;
	new_stale.push_back(session);
      }
    }

    for (auto session : new_stale) {
      mds->sessionmap.set_state(session, Session::STATE_STALE);
      if (mds->locker->revoke_stale_caps(session)) {
	mds->locker->remove_stale_leases(session);
	finish_flush_session(session, session->get_push_seq());
	auto m = make_message<MClientSession>(CEPH_SESSION_STALE, session->get_push_seq());
	mds->send_message_client(m, session);
      } else {
	to_evict.push_back(session);
      }
    }
  }

  // autoclose
  cutoff = queue_max_age + mds->mdsmap->get_session_autoclose();

  // Collect a list of sessions exceeding the autoclose threshold
  const auto sessions_p2 = mds->sessionmap.by_state.find(Session::STATE_STALE);
  if (sessions_p2 != mds->sessionmap.by_state.end() && !sessions_p2->second->empty()) {
    for (auto session : *(sessions_p2->second)) {
      assert(session->is_stale());
      auto last_cap_renew_span = std::chrono::duration<double>(now - session->last_cap_renew).count();
      if (last_cap_renew_span < cutoff) {
	dout(20) << "oldest stale session is " << session->info.inst
		 << " and recently renewed caps " << last_cap_renew_span << "s ago" << dendl;
	break;
      }
      to_evict.push_back(session);
    }
  }

  for (auto session: to_evict) {
    if (session->is_importing()) {
      dout(10) << "skipping session " << session->info.inst << ", it's being imported" << dendl;
      continue;
    }

    auto last_cap_renew_span = std::chrono::duration<double>(now - session->last_cap_renew).count();
    mds->clog->warn() << "evicting unresponsive client " << *session
		      << ", after " << last_cap_renew_span << " seconds";
    dout(10) << "autoclosing stale session " << session->info.inst
	     << " last renewed caps " << last_cap_renew_span << "s ago" << dendl;

    if (g_conf()->mds_session_blacklist_on_timeout) {
      std::stringstream ss;
      mds->evict_client(session->get_client().v, false, true, ss, nullptr);
    } else {
      kill_session(session, NULL);
    }
  }
}

void Server::evict_cap_revoke_non_responders() {
  if (!cap_revoke_eviction_timeout) {
    return;
  }

  auto&& to_evict = mds->locker->get_late_revoking_clients(cap_revoke_eviction_timeout);

  for (auto const &client: to_evict) {
    mds->clog->warn() << "client id " << client << " has not responded to"
                      << " cap revoke by MDS for over " << cap_revoke_eviction_timeout
                      << " seconds, evicting";
    dout(1) << __func__ << ": evicting cap revoke non-responder client id "
            << client << dendl;

    std::stringstream ss;
    bool evicted = mds->evict_client(client.v, false,
                                     g_conf()->mds_session_blacklist_on_evict,
                                     ss, nullptr);
    if (evicted && logger) {
      logger->inc(l_mdss_cap_revoke_eviction);
    }
  }
}

void Server::handle_conf_change(const std::set<std::string>& changed) {
  if (changed.count("mds_replay_unsafe_with_closed_session")) {
    replay_unsafe_with_closed_session = g_conf().get_val<bool>("mds_replay_unsafe_with_closed_session");
  }
  if (changed.count("mds_cap_revoke_eviction_timeout")) {
    cap_revoke_eviction_timeout = g_conf().get_val<double>("mds_cap_revoke_eviction_timeout");
    dout(20) << __func__ << " cap revoke eviction timeout changed to "
            << cap_revoke_eviction_timeout << dendl;
  }
  if (changed.count("mds_recall_max_decay_rate")) {
    recall_throttle = DecayCounter(g_conf().get_val<double>("mds_recall_max_decay_rate"));
  }
  if (changed.count("mds_max_snaps_per_dir")) {
    max_snaps_per_dir = g_conf().get_val<uint64_t>("mds_max_snaps_per_dir");
    dout(20) << __func__ << " max snapshots per directory changed to "
            << max_snaps_per_dir << dendl;
  }
  if (changed.count("mds_client_delegate_inos_pct")) {
    delegate_inos_pct = g_conf().get_val<uint64_t>("mds_client_delegate_inos_pct");
  }
}

/*
 * XXX bump in the interface here, not using an MDSContext here
 * because all the callers right now happen to use a SaferCond
 */
void Server::kill_session(Session *session, Context *on_safe, bool need_purge_inos)
{
  ceph_assert(ceph_mutex_is_locked_by_me(mds->mds_lock));

  if ((session->is_opening() ||
       session->is_open() ||
       session->is_stale()) &&
      !session->is_importing()) {
    dout(10) << "kill_session " << session << dendl;
    journal_close_session(session, Session::STATE_KILLING, on_safe, need_purge_inos);
  } else {
    dout(10) << "kill_session importing or already closing/killing " << session << dendl;
    if (session->is_closing() ||
	session->is_killing()) {
      if (on_safe)
	mdlog->wait_for_safe(new MDSInternalContextWrapper(mds, on_safe));
    } else {
      ceph_assert(session->is_closed() ||
		  session->is_importing());
      if (on_safe)
	on_safe->complete(0);
    }
  }
}

size_t Server::apply_blacklist(const std::set<entity_addr_t> &blacklist)
{
  bool prenautilus = mds->objecter->with_osdmap(
      [&](const OSDMap& o) {
	return o.require_osd_release < ceph_release_t::nautilus;
      });

  std::vector<Session*> victims;
  const auto& sessions = mds->sessionmap.get_sessions();
  for (const auto& p : sessions) {
    if (!p.first.is_client()) {
      // Do not apply OSDMap blacklist to MDS daemons, we find out
      // about their death via MDSMap.
      continue;
    }

    Session *s = p.second;
    auto inst_addr = s->info.inst.addr;
    // blacklist entries are always TYPE_ANY for nautilus+
    inst_addr.set_type(entity_addr_t::TYPE_ANY);
    if (blacklist.count(inst_addr)) {
      victims.push_back(s);
      continue;
    }
    if (prenautilus) {
      // ...except pre-nautilus, they were TYPE_LEGACY
      inst_addr.set_type(entity_addr_t::TYPE_LEGACY);
      if (blacklist.count(inst_addr)) {
	victims.push_back(s);
      }
    }
  }

  for (const auto& s : victims) {
    kill_session(s, nullptr);
  }

  dout(10) << "apply_blacklist: killed " << victims.size() << dendl;

  return victims.size();
}

void Server::journal_close_session(Session *session, int state, Context *on_safe, bool need_purge_inos)
{
  dout(10) << __func__ << " : "
	   << "("<< need_purge_inos << ")"
	   << session->info.inst
	   << "(" << session->info.prealloc_inos.size() << "|" << session->pending_prealloc_inos.size() << ")" << dendl;

  uint64_t sseq = mds->sessionmap.set_state(session, state);
  version_t pv = mds->sessionmap.mark_projected(session);
  version_t piv = 0;

  // release alloc and pending-alloc inos for this session
  // and wipe out session state, in case the session close aborts for some reason
  interval_set<inodeno_t> both;
  both.insert(session->pending_prealloc_inos);
  if (!need_purge_inos)
    both.insert(session->info.prealloc_inos);
  if (both.size()) {
    mds->inotable->project_release_ids(both);
    piv = mds->inotable->get_projected_version();
  } else
    piv = 0;
  
  if(need_purge_inos && session->info.prealloc_inos.size()) {
    dout(10) << "start purge indoes " << session->info.prealloc_inos << dendl;
    LogSegment* ls = mdlog->get_current_segment();
    LogEvent* e = new ESession(session->info.inst, false, pv, both, piv, session->info.prealloc_inos);
    MDSLogContextBase* c = new C_MDS_session_finish(this, session, sseq, false, pv, both, piv,
						    session->info.prealloc_inos, ls, on_safe);
    mdlog->start_submit_entry(e, c);
  } else {
    interval_set<inodeno_t> empty;
    LogEvent* e = new ESession(session->info.inst, false, pv, both, piv, empty);
    MDSLogContextBase* c = new C_MDS_session_finish(this, session, sseq, false, pv, both, piv, on_safe);
    mdlog->start_submit_entry(e, c);
  }
  mdlog->flush();

  // clean up requests, too
  while(!session->requests.empty()) {
    auto mdr = MDRequestRef(*session->requests.begin());
    mdcache->request_kill(mdr);
  }

  finish_flush_session(session, session->get_push_seq());
}

void Server::reconnect_clients(MDSContext *reconnect_done_)
{
  reconnect_done = reconnect_done_;

  auto now = clock::now();
  set<Session*> sessions;
  mds->sessionmap.get_client_session_set(sessions);
  for (auto session : sessions) {
    if (session->is_open()) {
      client_reconnect_gather.insert(session->get_client());
      session->set_reconnecting(true);
      session->last_cap_renew = now;
    }
  }

  if (client_reconnect_gather.empty()) {
    dout(7) << "reconnect_clients -- no sessions, doing nothing." << dendl;
    reconnect_gather_finish();
    return;
  }

  // clients will get the mdsmap and discover we're reconnecting via the monitor.
  
  reconnect_start = now;
  dout(1) << "reconnect_clients -- " << client_reconnect_gather.size() << " sessions" << dendl;
  mds->sessionmap.dump();
}

void Server::handle_client_reconnect(const cref_t<MClientReconnect> &m)
{
  dout(7) << "handle_client_reconnect " << m->get_source()
	  << (m->has_more() ? " (more)" : "") << dendl;
  client_t from = m->get_source().num();
  Session *session = mds->get_session(m);
  if (!session) {
    dout(0) << " ignoring sessionless msg " << *m << dendl;
    auto reply = make_message<MClientSession>(CEPH_SESSION_REJECT);
    reply->metadata["error_string"] = "sessionless";
    mds->send_message(reply, m->get_connection());
    return;
  }

  if (!session->is_open()) {
    dout(0) << " ignoring msg from not-open session" << *m << dendl;
    auto reply = make_message<MClientSession>(CEPH_SESSION_CLOSE);
    mds->send_message(reply, m->get_connection());
    return;
  }

  bool reconnect_all_deny = g_conf().get_val<bool>("mds_deny_all_reconnect");

  if (!mds->is_reconnect() && mds->get_want_state() == CEPH_MDS_STATE_RECONNECT) {
    dout(10) << " we're almost in reconnect state (mdsmap delivery race?); waiting" << dendl;
    mds->wait_for_reconnect(new C_MDS_RetryMessage(mds, m));
    return;
  }

  auto delay = std::chrono::duration<double>(clock::now() - reconnect_start).count();
  dout(10) << " reconnect_start " << reconnect_start << " delay " << delay << dendl;

  bool deny = false;
  if (reconnect_all_deny || !mds->is_reconnect() || mds->get_want_state() != CEPH_MDS_STATE_RECONNECT || reconnect_evicting) {
    // XXX maybe in the future we can do better than this?
    if (reconnect_all_deny) {
      dout(1) << "mds_deny_all_reconnect was set to speed up reboot phase, ignoring reconnect, sending close" << dendl;
    } else {
      dout(1) << "no longer in reconnect state, ignoring reconnect, sending close" << dendl;
    }
    mds->clog->info() << "denied reconnect attempt (mds is "
       << ceph_mds_state_name(mds->get_state())
       << ") from " << m->get_source_inst()
       << " after " << delay << " (allowed interval " << g_conf()->mds_reconnect_timeout << ")";
    deny = true;
  } else {
    std::string error_str;
    if (!session->is_open()) {
      error_str = "session is closed";
    } else if (mdcache->is_readonly()) {
      error_str = "mds is readonly";
    } else {
      if (session->info.client_metadata.features.empty())
	infer_supported_features(session,  session->info.client_metadata);

      feature_bitset_t missing_features = required_client_features;
      missing_features -= session->info.client_metadata.features;
      if (!missing_features.empty()) {
	stringstream ss;
	ss << "missing required features '" << missing_features << "'";
	error_str = ss.str();
      }
    }

    if (!error_str.empty()) {
      deny = true;
      dout(1) << " " << error_str << ", ignoring reconnect, sending close" << dendl;
      mds->clog->info() << "denied reconnect attempt from "
			<< m->get_source_inst() << " (" << error_str << ")";
    }
  }

  if (deny) {
    auto r = make_message<MClientSession>(CEPH_SESSION_CLOSE);
    mds->send_message_client(r, session);
    if (session->is_open()) {
      client_reconnect_denied.insert(session->get_client());
    }
    return;
  }

  if (!m->has_more()) {
    metrics_handler->add_session(session);
    // notify client of success with an OPEN
    auto reply = make_message<MClientSession>(CEPH_SESSION_OPEN);
    if (session->info.has_feature(CEPHFS_FEATURE_MIMIC))
      reply->supported_features = supported_features;
    mds->send_message_client(reply, session);
    mds->clog->debug() << "reconnect by " << session->info.inst << " after " << delay;
  }

  session->last_cap_renew = clock::now();
  
  // snaprealms
  for (const auto &r : m->realms) {
    CInode *in = mdcache->get_inode(inodeno_t(r.realm.ino));
    if (in && in->state_test(CInode::STATE_PURGING))
      continue;
    if (in) {
      if (in->snaprealm) {
	dout(15) << "open snaprealm (w inode) on " << *in << dendl;
      } else {
	// this can happen if we are non-auth or we rollback snaprealm
	dout(15) << "open snaprealm (null snaprealm) on " << *in << dendl;
      }
      mdcache->add_reconnected_snaprealm(from, inodeno_t(r.realm.ino), snapid_t(r.realm.seq));
    } else {
      dout(15) << "open snaprealm (w/o inode) on " << inodeno_t(r.realm.ino)
	       << " seq " << r.realm.seq << dendl;
      mdcache->add_reconnected_snaprealm(from, inodeno_t(r.realm.ino), snapid_t(r.realm.seq));
    }
  }

  // caps
  for (const auto &p : m->caps) {
    // make sure our last_cap_id is MAX over all issued caps
    if (p.second.capinfo.cap_id > mdcache->last_cap_id)
      mdcache->last_cap_id = p.second.capinfo.cap_id;
    
    CInode *in = mdcache->get_inode(p.first);
    if (in && in->state_test(CInode::STATE_PURGING))
      continue;
    if (in && in->is_auth()) {
      // we recovered it, and it's ours.  take note.
      dout(15) << "open cap realm " << inodeno_t(p.second.capinfo.snaprealm)
	       << " on " << *in << dendl;
      in->reconnect_cap(from, p.second, session);
      mdcache->add_reconnected_cap(from, p.first, p.second);
      recover_filelocks(in, p.second.flockbl, m->get_orig_source().num());
      continue;
    }
      
    if (in && !in->is_auth()) {
      // not mine.
      dout(10) << "non-auth " << *in << ", will pass off to authority" << dendl;
      // add to cap export list.
      mdcache->rejoin_export_caps(p.first, from, p.second,
				  in->authority().first, true);
    } else {
      // don't know if the inode is mine
      dout(10) << "missing ino " << p.first << ", will load later" << dendl;
      mdcache->rejoin_recovered_caps(p.first, from, p.second, MDS_RANK_NONE);
    }
  }

  reconnect_last_seen = clock::now();

  if (!m->has_more()) {
    mdcache->rejoin_recovered_client(session->get_client(), session->info.inst);

    // remove from gather set
    client_reconnect_gather.erase(from);
    session->set_reconnecting(false);
    if (client_reconnect_gather.empty())
      reconnect_gather_finish();
  }
}

void Server::infer_supported_features(Session *session, client_metadata_t& client_metadata)
{
  int supported = -1;
  auto it = client_metadata.find("ceph_version");
  if (it != client_metadata.end()) {
    // user space client
    if (it->second.compare(0, 16, "ceph version 12.") == 0)
      supported = CEPHFS_FEATURE_LUMINOUS;
    else if (session->get_connection()->has_feature(CEPH_FEATURE_FS_CHANGE_ATTR))
      supported = CEPHFS_FEATURE_KRAKEN;
  } else {
    it = client_metadata.find("kernel_version");
    if (it != client_metadata.end()) {
      // kernel client
      if (session->get_connection()->has_feature(CEPH_FEATURE_NEW_OSDOP_ENCODING))
	supported = CEPHFS_FEATURE_LUMINOUS;
    }
  }
  if (supported == -1 &&
      session->get_connection()->has_feature(CEPH_FEATURE_FS_FILE_LAYOUT_V2))
    supported = CEPHFS_FEATURE_JEWEL;

  if (supported >= 0) {
    unsigned long value = (1UL << (supported + 1)) - 1;
    client_metadata.features = feature_bitset_t(value);
    dout(10) << __func__ << " got '" << client_metadata.features << "'" << dendl;
  }
}

void Server::update_required_client_features()
{
  required_client_features = mds->mdsmap->get_required_client_features();
  dout(7) << "required_client_features: " << required_client_features << dendl;

  if (mds->get_state() >= MDSMap::STATE_RECONNECT) {
    set<Session*> sessions;
    mds->sessionmap.get_client_session_set(sessions);
    for (auto session : sessions) {
      feature_bitset_t missing_features = required_client_features;
      missing_features -= session->info.client_metadata.features;
      if (!missing_features.empty()) {
	bool blacklisted = mds->objecter->with_osdmap(
	    [session](const OSDMap &osd_map) -> bool {
	      return osd_map.is_blacklisted(session->info.inst.addr);
	    });
	if (blacklisted)
	  continue;

	mds->clog->warn() << "evicting session " << *session << ", missing required features '"
			  << missing_features << "'";
	std::stringstream ss;
	mds->evict_client(session->get_client().v, false,
			  g_conf()->mds_session_blacklist_on_evict, ss);
      }
    }
  }
}

void Server::reconnect_gather_finish()
{
  dout(7) << "reconnect_gather_finish.  failed on " << failed_reconnects << " clients" << dendl;
  ceph_assert(reconnect_done);

  if (!mds->snapclient->is_synced()) {
    // make sure snaptable cache is populated. snaprealms will be
    // extensively used in rejoin stage.
    dout(7) << " snaptable cache isn't synced, delaying state transition" << dendl;
    mds->snapclient->wait_for_sync(reconnect_done);
  } else {
    reconnect_done->complete(0);
  }
  reconnect_done = NULL;
}

void Server::reconnect_tick()
{
  bool reject_all_reconnect = false;
  if (reconnect_evicting) {
    dout(7) << "reconnect_tick: waiting for evictions" << dendl;
    return;
  }

  /*
   * Set mds_deny_all_reconnect to reject all the reconnect req ,
   * then load less meta information in rejoin phase. This will shorten reboot time.
   * Moreover, loading less meta increases the chance standby with less memory can failover.

   * Why not shorten reconnect period?
   * Clients may send unsafe or retry requests, which haven't been
   * completed before old mds stop, to new mds. These requests may
   * need to be processed during new mds's clientreplay phase,
   * see: #https://github.com/ceph/ceph/pull/29059.
   */
  bool reconnect_all_deny = g_conf().get_val<bool>("mds_deny_all_reconnect");
  if (client_reconnect_gather.empty())
    return;

  if (reconnect_all_deny && (client_reconnect_gather == client_reconnect_denied))
    reject_all_reconnect = true;
 
  auto now = clock::now();
  auto elapse1 = std::chrono::duration<double>(now - reconnect_start).count();
  if (elapse1 < g_conf()->mds_reconnect_timeout && !reject_all_reconnect)
    return;

  vector<Session*> remaining_sessions;
  remaining_sessions.reserve(client_reconnect_gather.size());
  for (auto c : client_reconnect_gather) {
    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(c.v));
    ceph_assert(session);
    remaining_sessions.push_back(session);
    // client re-sends cap flush messages before the reconnect message
    if (session->last_seen > reconnect_last_seen)
      reconnect_last_seen = session->last_seen;
  }

  auto elapse2 = std::chrono::duration<double>(now - reconnect_last_seen).count();
  if (elapse2 < g_conf()->mds_reconnect_timeout / 2 && !reject_all_reconnect) {
    dout(7) << "reconnect_tick: last seen " << elapse2
            << " seconds ago, extending reconnect interval" << dendl;
    return;
  }

  dout(7) << "reconnect timed out, " << remaining_sessions.size()
          << " clients have not reconnected in time" << dendl;

  // If we're doing blacklist evictions, use this to wait for them before
  // proceeding to reconnect_gather_finish
  MDSGatherBuilder gather(g_ceph_context);

  for (auto session : remaining_sessions) {
    // Keep sessions that have specified timeout. These sessions will prevent
    // mds from going to active. MDS goes to active after they all have been
    // killed or reclaimed.
    if (session->info.client_metadata.find("timeout") !=
	session->info.client_metadata.end()) {
      dout(1) << "reconnect keeps " << session->info.inst
	      << ", need to be reclaimed" << dendl;
      client_reclaim_gather.insert(session->get_client());
      continue;
    }

    dout(1) << "reconnect gives up on " << session->info.inst << dendl;

    mds->clog->warn() << "evicting unresponsive client " << *session
		      << ", after waiting " << elapse1
		      << " seconds during MDS startup";

    if (g_conf()->mds_session_blacklist_on_timeout) {
      std::stringstream ss;
      mds->evict_client(session->get_client().v, false, true, ss,
			gather.new_sub());
    } else {
      kill_session(session, NULL, true);
    }

    failed_reconnects++;
  }
  client_reconnect_gather.clear();
  client_reconnect_denied.clear();

  if (gather.has_subs()) {
    dout(1) << "reconnect will complete once clients are evicted" << dendl;
    gather.set_finisher(new MDSInternalContextWrapper(mds, new LambdaContext(
	    [this](int r){reconnect_gather_finish();})));
    gather.activate();
    reconnect_evicting = true;
  } else {
    reconnect_gather_finish();
  }
}

void Server::recover_filelocks(CInode *in, bufferlist locks, int64_t client)
{
  if (!locks.length()) return;
  int numlocks;
  ceph_filelock lock;
  auto p = locks.cbegin();
  decode(numlocks, p);
  for (int i = 0; i < numlocks; ++i) {
    decode(lock, p);
    lock.client = client;
    in->get_fcntl_lock_state()->held_locks.insert(pair<uint64_t, ceph_filelock>(lock.start, lock));
    ++in->get_fcntl_lock_state()->client_held_lock_counts[client];
  }
  decode(numlocks, p);
  for (int i = 0; i < numlocks; ++i) {
    decode(lock, p);
    lock.client = client;
    in->get_flock_lock_state()->held_locks.insert(pair<uint64_t, ceph_filelock> (lock.start, lock));
    ++in->get_flock_lock_state()->client_held_lock_counts[client];
  }
}

/**
 * Call this when the MDCache is oversized, to send requests to the clients
 * to trim some caps, and consequently unpin some inodes in the MDCache so
 * that it can trim too.
 */
std::pair<bool, uint64_t> Server::recall_client_state(MDSGatherBuilder* gather, RecallFlags flags)
{
  const auto now = clock::now();
  const bool steady = !!(flags&RecallFlags::STEADY);
  const bool enforce_max = !!(flags&RecallFlags::ENFORCE_MAX);
  const bool enforce_liveness = !!(flags&RecallFlags::ENFORCE_LIVENESS);
  const bool trim = !!(flags&RecallFlags::TRIM);

  const auto max_caps_per_client = g_conf().get_val<uint64_t>("mds_max_caps_per_client");
  const auto min_caps_per_client = g_conf().get_val<uint64_t>("mds_min_caps_per_client");
  const auto recall_global_max_decay_threshold = g_conf().get_val<Option::size_t>("mds_recall_global_max_decay_threshold");
  const auto recall_max_caps = g_conf().get_val<Option::size_t>("mds_recall_max_caps");
  const auto recall_max_decay_threshold = g_conf().get_val<Option::size_t>("mds_recall_max_decay_threshold");
  const auto cache_liveness_magnitude = g_conf().get_val<Option::size_t>("mds_session_cache_liveness_magnitude");

  dout(7) << __func__ << ":"
           << " min=" << min_caps_per_client
           << " max=" << max_caps_per_client
           << " total=" << Capability::count()
           << " flags=" << flags
           << dendl;

  /* trim caps of sessions with the most caps first */
  std::multimap<uint64_t, Session*> caps_session;
  auto f = [&caps_session, enforce_max, enforce_liveness, trim, max_caps_per_client, cache_liveness_magnitude](auto& s) {
    auto num_caps = s->caps.size();
    auto cache_liveness = s->get_session_cache_liveness();
    if (trim || (enforce_max && num_caps > max_caps_per_client) || (enforce_liveness && cache_liveness < (num_caps>>cache_liveness_magnitude))) {
      caps_session.emplace(std::piecewise_construct, std::forward_as_tuple(num_caps), std::forward_as_tuple(s));
    }
  };
  mds->sessionmap.get_client_sessions(std::move(f));

  std::pair<bool, uint64_t> result = {false, 0};
  auto& [throttled, caps_recalled] = result;
  last_recall_state = now;
  for (const auto& [num_caps, session] : boost::adaptors::reverse(caps_session)) {
    if (!session->is_open() ||
        !session->get_connection() ||
	!session->info.inst.name.is_client())
      continue;

    dout(10) << __func__ << ":"
             << " session " << session->info.inst
	     << " caps " << num_caps
	     << ", leases " << session->leases.size()
	     << dendl;

    uint64_t newlim;
    if (num_caps < recall_max_caps || (num_caps-recall_max_caps) < min_caps_per_client) {
      newlim = min_caps_per_client;
    } else {
      newlim = num_caps-recall_max_caps;
    }
    if (num_caps > newlim) {
      /* now limit the number of caps we recall at a time to prevent overloading ourselves */
      uint64_t recall = std::min<uint64_t>(recall_max_caps, num_caps-newlim);
      newlim = num_caps-recall;
      const uint64_t session_recall_throttle = session->get_recall_caps_throttle();
      const uint64_t session_recall_throttle2o = session->get_recall_caps_throttle2o();
      const uint64_t global_recall_throttle = recall_throttle.get();
      if (session_recall_throttle+recall > recall_max_decay_threshold) {
        dout(15) << "  session recall threshold (" << recall_max_decay_threshold << ") hit at " << session_recall_throttle << "; skipping!" << dendl;
        throttled = true;
        continue;
      } else if (session_recall_throttle2o+recall > recall_max_caps*2) {
        dout(15) << "  session recall 2nd-order threshold (" << 2*recall_max_caps << ") hit at " << session_recall_throttle2o << "; skipping!" << dendl;
        throttled = true;
        continue;
      } else if (global_recall_throttle+recall > recall_global_max_decay_threshold) {
        dout(15) << "  global recall threshold (" << recall_global_max_decay_threshold << ") hit at " << global_recall_throttle << "; skipping!" << dendl;
        throttled = true;
        break;
      }

      // now check if we've recalled caps recently and the client is unlikely to satisfy a new recall
      if (steady) {
        const auto session_recall = session->get_recall_caps();
        const auto session_release = session->get_release_caps();
        if (2*session_release < session_recall && 2*session_recall > recall_max_decay_threshold) {
          /* The session has been unable to keep up with the number of caps
           * recalled (by half); additionally, to prevent marking sessions
           * we've just begun to recall from, the session_recall counter
           * (decayed count of caps recently recalled) is **greater** than the
           * session threshold for the session's cap recall throttle.
           */
          dout(15) << "  2*session_release < session_recall"
                      " (2*" << session_release << " < " << session_recall << ") &&"
                      " 2*session_recall < recall_max_decay_threshold"
                      " (2*" << session_recall << " > " << recall_max_decay_threshold << ")"
                      " Skipping because we are unlikely to get more released." << dendl;
          continue;
        } else if (recall < recall_max_caps && 2*recall < session_recall) {
          /* The number of caps recalled is less than the number we *could*
           * recall (so there isn't much left to recall?) and the number of
           * caps is less than the current recall_caps counter (decayed count
           * of caps recently recalled).
           */
          dout(15) << "  2*recall < session_recall "
                      " (2*" << recall << " < " << session_recall << ") &&"
                      " recall < recall_max_caps (" << recall << " < " << recall_max_caps << ");"
                      " Skipping because we are unlikely to get more released." << dendl;
          continue;
        }
      }

      dout(7) << "  recalling " << recall << " caps; session_recall_throttle = " << session_recall_throttle << "; global_recall_throttle = " << global_recall_throttle << dendl;

      auto m = make_message<MClientSession>(CEPH_SESSION_RECALL_STATE);
      m->head.max_caps = newlim;
      mds->send_message_client(m, session);
      if (gather) {
        flush_session(session, gather);
      }
      caps_recalled += session->notify_recall_sent(newlim);
      recall_throttle.hit(recall);
    }
  }

  dout(7) << "recalled" << (throttled ? " (throttled)" : "") << " " << caps_recalled << " client caps." << dendl;

  return result;
}

void Server::force_clients_readonly()
{
  dout(10) << "force_clients_readonly" << dendl;
  set<Session*> sessions;
  mds->sessionmap.get_client_session_set(sessions);
  for (set<Session*>::const_iterator p = sessions.begin();
      p != sessions.end();
      ++p) {
    Session *session = *p;
    if (!session->info.inst.name.is_client() ||
	!(session->is_open() || session->is_stale()))
      continue;
    mds->send_message_client(make_message<MClientSession>(CEPH_SESSION_FORCE_RO), session);
  }
}

/*******
 * some generic stuff for finishing off requests
 */
void Server::journal_and_reply(MDRequestRef& mdr, CInode *in, CDentry *dn, LogEvent *le, MDSLogContextBase *fin)
{
  dout(10) << "journal_and_reply tracei " << in << " tracedn " << dn << dendl;
  ceph_assert(!mdr->has_completed);

  // note trace items for eventual reply.
  mdr->tracei = in;
  if (in)
    mdr->pin(in);

  mdr->tracedn = dn;
  if (dn)
    mdr->pin(dn);

  early_reply(mdr, in, dn);
  
  mdr->committing = true;
  submit_mdlog_entry(le, fin, mdr, __func__);
  
  if (mdr->client_request && mdr->client_request->is_queued_for_replay()) {
    if (mds->queue_one_replay()) {
      dout(10) << " queued next replay op" << dendl;
    } else {
      dout(10) << " journaled last replay op" << dendl;
    }
  } else if (mdr->did_early_reply)
    mds->locker->drop_rdlocks_for_early_reply(mdr.get());
  else
    mdlog->flush();
}

void Server::submit_mdlog_entry(LogEvent *le, MDSLogContextBase *fin, MDRequestRef& mdr,
                                std::string_view event)
{
  if (mdr) {
    string event_str("submit entry: ");
    event_str += event;
    mdr->mark_event(event_str);
  } 
  mdlog->submit_entry(le, fin);
}

/*
 * send response built from mdr contents and error code; clean up mdr
 */
void Server::respond_to_request(MDRequestRef& mdr, int r)
{
  if (mdr->client_request) {
    if (mdr->is_batch_head()) {
      dout(20) << __func__ << " batch head " << *mdr << dendl;
      mdr->release_batch_op()->respond(r);
    } else {
     reply_client_request(mdr, make_message<MClientReply>(*mdr->client_request, r));
    }
  } else if (mdr->internal_op > -1) {
    dout(10) << "respond_to_request on internal request " << mdr << dendl;
    if (!mdr->internal_op_finish)
      ceph_abort_msg("trying to respond to internal op without finisher");
    mdr->internal_op_finish->complete(r);
    mdcache->request_finish(mdr);
  }
}

// statistics mds req op number and latency 
void Server::perf_gather_op_latency(const cref_t<MClientRequest> &req, utime_t lat)
{
  int code = l_mdss_first;
  switch(req->get_op()) {
  case CEPH_MDS_OP_LOOKUPHASH:
    code = l_mdss_req_lookuphash_latency;
    break;
  case CEPH_MDS_OP_LOOKUPINO:
    code = l_mdss_req_lookupino_latency;
    break;
  case CEPH_MDS_OP_LOOKUPPARENT:
    code = l_mdss_req_lookupparent_latency;
    break;
  case CEPH_MDS_OP_LOOKUPNAME:
    code = l_mdss_req_lookupname_latency;
    break;
  case CEPH_MDS_OP_LOOKUP:
    code = l_mdss_req_lookup_latency;
    break;
  case CEPH_MDS_OP_LOOKUPSNAP:
    code = l_mdss_req_lookupsnap_latency;
    break;
  case CEPH_MDS_OP_GETATTR:
    code = l_mdss_req_getattr_latency;
    break;
  case CEPH_MDS_OP_SETATTR:
    code = l_mdss_req_setattr_latency;
    break;
  case CEPH_MDS_OP_SETLAYOUT:
    code = l_mdss_req_setlayout_latency;
    break;
  case CEPH_MDS_OP_SETDIRLAYOUT:
    code = l_mdss_req_setdirlayout_latency;
    break;
  case CEPH_MDS_OP_SETXATTR:
    code = l_mdss_req_setxattr_latency;
    break;
  case CEPH_MDS_OP_RMXATTR:
    code = l_mdss_req_rmxattr_latency;
    break;
  case CEPH_MDS_OP_READDIR:
    code = l_mdss_req_readdir_latency;
    break;
  case CEPH_MDS_OP_SETFILELOCK:
    code = l_mdss_req_setfilelock_latency;
    break;
  case CEPH_MDS_OP_GETFILELOCK:
    code = l_mdss_req_getfilelock_latency;
    break;
  case CEPH_MDS_OP_CREATE:
    code = l_mdss_req_create_latency;
    break;
  case CEPH_MDS_OP_OPEN:
    code = l_mdss_req_open_latency;
    break;
  case CEPH_MDS_OP_MKNOD:
    code = l_mdss_req_mknod_latency;
    break;
  case CEPH_MDS_OP_LINK:
    code = l_mdss_req_link_latency;
    break;
  case CEPH_MDS_OP_UNLINK:
    code = l_mdss_req_unlink_latency;
    break;
  case CEPH_MDS_OP_RMDIR:
    code = l_mdss_req_rmdir_latency;
    break;
  case CEPH_MDS_OP_RENAME:
    code = l_mdss_req_rename_latency;
    break;
  case CEPH_MDS_OP_MKDIR:
    code = l_mdss_req_mkdir_latency;
    break;
  case CEPH_MDS_OP_SYMLINK:
    code = l_mdss_req_symlink_latency;
    break;
  case CEPH_MDS_OP_LSSNAP:
    code = l_mdss_req_lssnap_latency;
    break;
  case CEPH_MDS_OP_MKSNAP:
    code = l_mdss_req_mksnap_latency;
    break;
  case CEPH_MDS_OP_RMSNAP:
    code = l_mdss_req_rmsnap_latency;
    break;
  case CEPH_MDS_OP_RENAMESNAP:
    code = l_mdss_req_renamesnap_latency;
    break;
  default: ceph_abort();
  }
  logger->tinc(code, lat);   
}

void Server::early_reply(MDRequestRef& mdr, CInode *tracei, CDentry *tracedn)
{
  if (!g_conf()->mds_early_reply)
    return;

  if (mdr->no_early_reply) {
    dout(10) << "early_reply - flag no_early_reply is set, not allowed." << dendl;
    return;
  }

  if (mdr->has_more() && mdr->more()->has_journaled_slaves) {
    dout(10) << "early_reply - there are journaled slaves, not allowed." << dendl;
    return; 
  }

  if (mdr->alloc_ino) {
    dout(10) << "early_reply - allocated ino, not allowed" << dendl;
    return;
  }

  const cref_t<MClientRequest> &req = mdr->client_request;
  entity_inst_t client_inst = req->get_source_inst();
  if (client_inst.name.is_mds())
    return;

  if (req->is_replay()) {
    dout(10) << " no early reply on replay op" << dendl;
    return;
  }


  auto reply = make_message<MClientReply>(*req, 0);
  reply->set_unsafe();

  // mark xlocks "done", indicating that we are exposing uncommitted changes.
  //
  //_rename_finish() does not send dentry link/unlink message to replicas.
  // so do not set xlocks on dentries "done", the xlocks prevent dentries
  // that have projected linkages from getting new replica.
  mds->locker->set_xlocks_done(mdr.get(), req->get_op() == CEPH_MDS_OP_RENAME);

  dout(10) << "early_reply " << reply->get_result() 
	   << " (" << cpp_strerror(reply->get_result())
	   << ") " << *req << dendl;

  if (tracei || tracedn) {
    if (tracei)
      mdr->cap_releases.erase(tracei->vino());
    if (tracedn)
      mdr->cap_releases.erase(tracedn->get_dir()->get_inode()->vino());

    set_trace_dist(reply, tracei, tracedn, mdr);
  }

  reply->set_extra_bl(mdr->reply_extra_bl);
  mds->send_message_client(reply, mdr->session);

  mdr->did_early_reply = true;

  mds->logger->inc(l_mds_reply);
  utime_t lat = ceph_clock_now() - req->get_recv_stamp();
  mds->logger->tinc(l_mds_reply_latency, lat);
  if (client_inst.name.is_client()) {
    mds->sessionmap.hit_session(mdr->session);
  }
  perf_gather_op_latency(req, lat);
  dout(20) << "lat " << lat << dendl;

  mdr->mark_event("early_replied");
}

/*
 * send given reply
 * include a trace to tracei
 * Clean up mdr
 */
void Server::reply_client_request(MDRequestRef& mdr, const ref_t<MClientReply> &reply)
{
  ceph_assert(mdr.get());
  const cref_t<MClientRequest> &req = mdr->client_request;
  
  dout(7) << "reply_client_request " << reply->get_result()
	   << " (" << cpp_strerror(reply->get_result())
	   << ") " << *req << dendl;

  mdr->mark_event("replying");

  Session *session = mdr->session;

  // note successful request in session map?
  //
  // setfilelock requests are special, they only modify states in MDS memory.
  // The states get lost when MDS fails. If Client re-send a completed
  // setfilelock request, it means that client did not receive corresponding
  // setfilelock reply.  So MDS should re-execute the setfilelock request.
  if (req->may_write() && req->get_op() != CEPH_MDS_OP_SETFILELOCK &&
      reply->get_result() == 0 && session) {
    inodeno_t created = mdr->alloc_ino ? mdr->alloc_ino : mdr->used_prealloc_ino;
    session->add_completed_request(mdr->reqid.tid, created);
    if (mdr->ls) {
      mdr->ls->touched_sessions.insert(session->info.inst.name);
    }
  }

  // give any preallocated inos to the session
  apply_allocated_inos(mdr, session);

  // get tracei/tracedn from mdr?
  CInode *tracei = mdr->tracei;
  CDentry *tracedn = mdr->tracedn;

  bool is_replay = mdr->client_request->is_replay();
  bool did_early_reply = mdr->did_early_reply;
  entity_inst_t client_inst = req->get_source_inst();

  if (!did_early_reply && !is_replay) {

    mds->logger->inc(l_mds_reply);
    utime_t lat = ceph_clock_now() - mdr->client_request->get_recv_stamp();
    mds->logger->tinc(l_mds_reply_latency, lat);
    if (session && client_inst.name.is_client()) {
      mds->sessionmap.hit_session(session);
    }
    perf_gather_op_latency(req, lat);
    dout(20) << "lat " << lat << dendl;
    
    if (tracei)
      mdr->cap_releases.erase(tracei->vino());
    if (tracedn)
      mdr->cap_releases.erase(tracedn->get_dir()->get_inode()->vino());
  }

  // drop non-rdlocks before replying, so that we can issue leases
  mdcache->request_drop_non_rdlocks(mdr);

  // reply at all?
  if (session && !client_inst.name.is_mds()) {
    // send reply.
    if (!did_early_reply &&   // don't issue leases if we sent an earlier reply already
	(tracei || tracedn)) {
      if (is_replay) {
	if (tracei)
	  mdcache->try_reconnect_cap(tracei, session);
      } else {
	// include metadata in reply
	set_trace_dist(reply, tracei, tracedn, mdr);
      }
    }

    // We can set the extra bl unconditionally: if it's already been sent in the
    // early_reply, set_extra_bl will have claimed it and reply_extra_bl is empty
    reply->set_extra_bl(mdr->reply_extra_bl);

    reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());
    mds->send_message_client(reply, session);
  }

  if (req->is_queued_for_replay() &&
      (mdr->has_completed || reply->get_result() < 0)) {
    if (reply->get_result() < 0) {
      int r = reply->get_result();
      derr << "reply_client_request: failed to replay " << *req
	   << " error " << r << " (" << cpp_strerror(r)  << ")" << dendl;
      mds->clog->warn() << "failed to replay " << req->get_reqid() << " error " << r;
    }
    mds->queue_one_replay();
  }

  // clean up request
  mdcache->request_finish(mdr);

  // take a closer look at tracei, if it happens to be a remote link
  if (tracei && 
      tracedn &&
      tracedn->get_projected_linkage()->is_remote()) {
    mdcache->eval_remote(tracedn);
  }
}

/*
 * pass inode OR dentry (not both, or we may get confused)
 *
 * trace is in reverse order (i.e. root inode comes last)
 */
void Server::set_trace_dist(const ref_t<MClientReply> &reply,
			    CInode *in, CDentry *dn,
			    MDRequestRef& mdr)
{
  // skip doing this for debugging purposes?
  if (g_conf()->mds_inject_traceless_reply_probability &&
      mdr->ls && !mdr->o_trunc &&
      (rand() % 10000 < g_conf()->mds_inject_traceless_reply_probability * 10000.0)) {
    dout(5) << "deliberately skipping trace for " << *reply << dendl;
    return;
  }

  // inode, dentry, dir, ..., inode
  bufferlist bl;
  mds_rank_t whoami = mds->get_nodeid();
  Session *session = mdr->session;
  snapid_t snapid = mdr->snapid;
  utime_t now = ceph_clock_now();

  dout(20) << "set_trace_dist snapid " << snapid << dendl;

  // realm
  if (snapid == CEPH_NOSNAP) {
    SnapRealm *realm;
    if (in)
      realm = in->find_snaprealm();
    else
      realm = dn->get_dir()->get_inode()->find_snaprealm();
    reply->snapbl = realm->get_snap_trace();
    dout(10) << "set_trace_dist snaprealm " << *realm << " len=" << reply->snapbl.length() << dendl;
  }

  // dir + dentry?
  if (dn) {
    reply->head.is_dentry = 1;
    CDir *dir = dn->get_dir();
    CInode *diri = dir->get_inode();

    diri->encode_inodestat(bl, session, NULL, snapid);
    dout(20) << "set_trace_dist added diri " << *diri << dendl;

#ifdef MDS_VERIFY_FRAGSTAT
    if (dir->is_complete())
      dir->verify_fragstat();
#endif
    DirStat ds;
    ds.frag = dir->get_frag();
    ds.auth = dir->get_dir_auth().first;
    if (dir->is_auth() && !mdcache->forward_all_reqs_to_auth())
      dir->get_dist_spec(ds.dist, whoami);

    dir->encode_dirstat(bl, session->info, ds);
    dout(20) << "set_trace_dist added dir  " << *dir << dendl;

    encode(dn->get_name(), bl);

    int lease_mask = 0;
    CDentry::linkage_t *dnl = dn->get_linkage(mdr->get_client(), mdr);
    if (dnl->is_primary()) {
      ceph_assert(dnl->get_inode() == in);
      lease_mask = CEPH_LEASE_PRIMARY_LINK;
    } else {
      if (dnl->is_remote())
	ceph_assert(dnl->get_remote_ino() == in->ino());
      else
	ceph_assert(!in);
    }
    mds->locker->issue_client_lease(dn, mdr, lease_mask, now, bl);
    dout(20) << "set_trace_dist added dn   " << snapid << " " << *dn << dendl;
  } else
    reply->head.is_dentry = 0;

  // inode
  if (in) {
    in->encode_inodestat(bl, session, NULL, snapid, 0, mdr->getattr_caps);
    dout(20) << "set_trace_dist added in   " << *in << dendl;
    reply->head.is_target = 1;
  } else
    reply->head.is_target = 0;

  reply->set_trace(bl);
}

void Server::handle_client_request(const cref_t<MClientRequest> &req)
{
  dout(4) << "handle_client_request " << *req << dendl;

  if (mds->logger)
    mds->logger->inc(l_mds_request);
  if (logger)
    logger->inc(l_mdss_handle_client_request);

  if (!mdcache->is_open()) {
    dout(5) << "waiting for root" << dendl;
    mdcache->wait_for_open(new C_MDS_RetryMessage(mds, req));
    return;
  }

  bool sessionclosed_isok = replay_unsafe_with_closed_session;
  // active session?
  Session *session = 0;
  if (req->get_source().is_client()) {
    session = mds->get_session(req);
    if (!session) {
      dout(5) << "no session for " << req->get_source() << ", dropping" << dendl;
    } else if ((session->is_closed() && (!mds->is_clientreplay() || !sessionclosed_isok)) ||
	       session->is_closing() ||
	       session->is_killing()) {
      dout(5) << "session closed|closing|killing, dropping" << dendl;
      session = NULL;
    }
    if (!session) {
      if (req->is_queued_for_replay())
	mds->queue_one_replay();
      return;
    }
  }

  // old mdsmap?
  if (req->get_mdsmap_epoch() < mds->mdsmap->get_epoch()) {
    // send it?  hrm, this isn't ideal; they may get a lot of copies if
    // they have a high request rate.
  }

  // completed request?
  bool has_completed = false;
  if (req->is_replay() || req->get_retry_attempt()) {
    ceph_assert(session);
    inodeno_t created;
    if (session->have_completed_request(req->get_reqid().tid, &created)) {
      has_completed = true;
      if (!session->is_open())
        return;
      // Don't send traceless reply if the completed request has created
      // new inode. Treat the request as lookup request instead.
      if (req->is_replay() ||
	  ((created == inodeno_t() || !mds->is_clientreplay()) &&
	   req->get_op() != CEPH_MDS_OP_OPEN &&
	   req->get_op() != CEPH_MDS_OP_CREATE)) {
	dout(5) << "already completed " << req->get_reqid() << dendl;
        auto reply = make_message<MClientReply>(*req, 0);
	if (created != inodeno_t()) {
	  bufferlist extra;
	  encode(created, extra);
	  reply->set_extra_bl(extra);
	}
        mds->send_message_client(reply, session);

	if (req->is_queued_for_replay())
	  mds->queue_one_replay();

	return;
      }
      if (req->get_op() != CEPH_MDS_OP_OPEN &&
	  req->get_op() != CEPH_MDS_OP_CREATE) {
	dout(10) << " completed request which created new inode " << created
		 << ", convert it to lookup request" << dendl;
	req->head.op = req->get_dentry_wanted() ? CEPH_MDS_OP_LOOKUP : CEPH_MDS_OP_GETATTR;
	req->head.args.getattr.mask = CEPH_STAT_CAP_INODE_ALL;
      }
    }
  }

  // trim completed_request list
  if (req->get_oldest_client_tid() > 0) {
    dout(15) << " oldest_client_tid=" << req->get_oldest_client_tid() << dendl;
    ceph_assert(session);
    if (session->trim_completed_requests(req->get_oldest_client_tid())) {
      // Sessions 'completed_requests' was dirtied, mark it to be
      // potentially flushed at segment expiry.
      mdlog->get_current_segment()->touched_sessions.insert(session->info.inst.name);

      if (session->get_num_trim_requests_warnings() > 0 &&
	  session->get_num_completed_requests() * 2 < g_conf()->mds_max_completed_requests)
	session->reset_num_trim_requests_warnings();
    } else {
      if (session->get_num_completed_requests() >=
	  (g_conf()->mds_max_completed_requests << session->get_num_trim_requests_warnings())) {
	session->inc_num_trim_requests_warnings();
	stringstream ss;
	ss << "client." << session->get_client() << " does not advance its oldest_client_tid ("
	   << req->get_oldest_client_tid() << "), "
	   << session->get_num_completed_requests()
	   << " completed requests recorded in session\n";
	mds->clog->warn() << ss.str();
	dout(20) << __func__ << " " << ss.str() << dendl;
      }
    }
  }

  // register + dispatch
  MDRequestRef mdr = mdcache->request_start(req);
  if (!mdr.get())
    return;

  if (session) {
    mdr->session = session;
    session->requests.push_back(&mdr->item_session_request);
  }

  if (has_completed)
    mdr->has_completed = true;

  // process embedded cap releases?
  //  (only if NOT replay!)
  if (!req->releases.empty() && req->get_source().is_client() && !req->is_replay()) {
    client_t client = req->get_source().num();
    for (const auto &r : req->releases) {
      mds->locker->process_request_cap_release(mdr, client, r.item, r.dname);
    }
    req->releases.clear();
  }

  dispatch_client_request(mdr);
  return;
}

void Server::handle_osd_map()
{
  /* Note that we check the OSDMAP_FULL flag directly rather than
   * using osdmap_full_flag(), because we want to know "is the flag set"
   * rather than "does the flag apply to us?" */
  mds->objecter->with_osdmap([this](const OSDMap& o) {
      auto pi = o.get_pg_pool(mds->mdsmap->get_metadata_pool());
      is_full = pi && pi->has_flag(pg_pool_t::FLAG_FULL);
      dout(7) << __func__ << ": full = " << is_full << " epoch = "
	      << o.get_epoch() << dendl;
    });
}

void Server::dispatch_client_request(MDRequestRef& mdr)
{
  // we shouldn't be waiting on anyone.
  ceph_assert(!mdr->has_more() || mdr->more()->waiting_on_slave.empty());

  if (mdr->killed) {
    dout(10) << "request " << *mdr << " was killed" << dendl;
    //if the mdr is a "batch_op" and it has followers, pick a follower as
    //the new "head of the batch ops" and go on processing the new one.
    if (mdr->is_batch_head()) {
      int mask = mdr->client_request->head.args.getattr.mask;
      auto it = mdr->batch_op_map->find(mask);
      auto new_batch_head = it->second->find_new_head();
      if (!new_batch_head) {
	mdr->batch_op_map->erase(it);
	return;
      }
      mdr = std::move(new_batch_head);
    } else {
      return;
    }
  } else if (mdr->aborted) {
    mdr->aborted = false;
    mdcache->request_kill(mdr);
    return;
  }

  const cref_t<MClientRequest> &req = mdr->client_request;

  if (logger) logger->inc(l_mdss_dispatch_client_request);

  dout(7) << "dispatch_client_request " << *req << dendl;

  if (req->may_write() && mdcache->is_readonly()) {
    dout(10) << " read-only FS" << dendl;
    respond_to_request(mdr, -EROFS);
    return;
  }
  if (mdr->has_more() && mdr->more()->slave_error) {
    dout(10) << " got error from slaves" << dendl;
    respond_to_request(mdr, mdr->more()->slave_error);
    return;
  }
  
  if (is_full) {
    if (req->get_op() == CEPH_MDS_OP_SETLAYOUT ||
        req->get_op() == CEPH_MDS_OP_SETDIRLAYOUT ||
        req->get_op() == CEPH_MDS_OP_SETLAYOUT ||
        req->get_op() == CEPH_MDS_OP_RMXATTR ||
        req->get_op() == CEPH_MDS_OP_SETXATTR ||
        req->get_op() == CEPH_MDS_OP_CREATE ||
        req->get_op() == CEPH_MDS_OP_SYMLINK ||
        req->get_op() == CEPH_MDS_OP_MKSNAP ||
	((req->get_op() == CEPH_MDS_OP_LINK ||
	  req->get_op() == CEPH_MDS_OP_RENAME) &&
	 (!mdr->has_more() || mdr->more()->witnessed.empty())) // haven't started slave request
	) {

      dout(20) << __func__ << ": full, responding ENOSPC to op " << ceph_mds_op_name(req->get_op()) << dendl;
      respond_to_request(mdr, -ENOSPC);
      return;
    } else {
      dout(20) << __func__ << ": full, permitting op " << ceph_mds_op_name(req->get_op()) << dendl;
    }
  }

  switch (req->get_op()) {
  case CEPH_MDS_OP_LOOKUPHASH:
  case CEPH_MDS_OP_LOOKUPINO:
    handle_client_lookup_ino(mdr, false, false);
    break;
  case CEPH_MDS_OP_LOOKUPPARENT:
    handle_client_lookup_ino(mdr, true, false);
    break;
  case CEPH_MDS_OP_LOOKUPNAME:
    handle_client_lookup_ino(mdr, false, true);
    break;

    // inodes ops.
  case CEPH_MDS_OP_LOOKUP:
    handle_client_getattr(mdr, true);
    break;

  case CEPH_MDS_OP_LOOKUPSNAP:
    // lookupsnap does not reference a CDentry; treat it as a getattr
  case CEPH_MDS_OP_GETATTR:
    handle_client_getattr(mdr, false);
    break;

  case CEPH_MDS_OP_SETATTR:
    handle_client_setattr(mdr);
    break;
  case CEPH_MDS_OP_SETLAYOUT:
    handle_client_setlayout(mdr);
    break;
  case CEPH_MDS_OP_SETDIRLAYOUT:
    handle_client_setdirlayout(mdr);
    break;
  case CEPH_MDS_OP_SETXATTR:
    handle_client_setxattr(mdr);
    break;
  case CEPH_MDS_OP_RMXATTR:
    handle_client_removexattr(mdr);
    break;

  case CEPH_MDS_OP_READDIR:
    handle_client_readdir(mdr);
    break;

  case CEPH_MDS_OP_SETFILELOCK:
    handle_client_file_setlock(mdr);
    break;

  case CEPH_MDS_OP_GETFILELOCK:
    handle_client_file_readlock(mdr);
    break;

    // funky.
  case CEPH_MDS_OP_CREATE:
    if (mdr->has_completed)
      handle_client_open(mdr);  // already created.. just open
    else
      handle_client_openc(mdr);
    break;

  case CEPH_MDS_OP_OPEN:
    handle_client_open(mdr);
    break;

    // namespace.
    // no prior locks.
  case CEPH_MDS_OP_MKNOD:
    handle_client_mknod(mdr);
    break;
  case CEPH_MDS_OP_LINK:
    handle_client_link(mdr);
    break;
  case CEPH_MDS_OP_UNLINK:
  case CEPH_MDS_OP_RMDIR:
    handle_client_unlink(mdr);
    break;
  case CEPH_MDS_OP_RENAME:
    handle_client_rename(mdr);
    break;
  case CEPH_MDS_OP_MKDIR:
    handle_client_mkdir(mdr);
    break;
  case CEPH_MDS_OP_SYMLINK:
    handle_client_symlink(mdr);
    break;


    // snaps
  case CEPH_MDS_OP_LSSNAP:
    handle_client_lssnap(mdr);
    break;
  case CEPH_MDS_OP_MKSNAP:
    handle_client_mksnap(mdr);
    break;
  case CEPH_MDS_OP_RMSNAP:
    handle_client_rmsnap(mdr);
    break;
  case CEPH_MDS_OP_RENAMESNAP:
    handle_client_renamesnap(mdr);
    break;

  default:
    dout(1) << " unknown client op " << req->get_op() << dendl;
    respond_to_request(mdr, -EOPNOTSUPP);
  }
}


// ---------------------------------------
// SLAVE REQUESTS

void Server::handle_slave_request(const cref_t<MMDSSlaveRequest> &m)
{
  dout(4) << "handle_slave_request " << m->get_reqid() << " from " << m->get_source() << dendl;
  mds_rank_t from = mds_rank_t(m->get_source().num());

  if (logger) logger->inc(l_mdss_handle_slave_request);

  // reply?
  if (m->is_reply())
    return handle_slave_request_reply(m);

  // the purpose of rename notify is enforcing causal message ordering. making sure
  // bystanders have received all messages from rename srcdn's auth MDS.
  if (m->get_op() == MMDSSlaveRequest::OP_RENAMENOTIFY) {
    auto reply = make_message<MMDSSlaveRequest>(m->get_reqid(), m->get_attempt(), MMDSSlaveRequest::OP_RENAMENOTIFYACK);
    mds->send_message(reply, m->get_connection());
    return;
  }

  CDentry *straydn = NULL;
  if (m->straybl.length() > 0) {
    mdcache->decode_replica_stray(straydn, m->straybl, from);
    ceph_assert(straydn);
    m->straybl.clear();
  }

  if (!mds->is_clientreplay() && !mds->is_active() && !mds->is_stopping()) {
    dout(3) << "not clientreplay|active yet, waiting" << dendl;
    mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
    return;
  }

  // am i a new slave?
  MDRequestRef mdr;
  if (mdcache->have_request(m->get_reqid())) {
    // existing?
    mdr = mdcache->request_get(m->get_reqid());

    // is my request newer?
    if (mdr->attempt > m->get_attempt()) {
      dout(10) << "local request " << *mdr << " attempt " << mdr->attempt << " > " << m->get_attempt()
	       << ", dropping " << *m << dendl;
      return;
    }

    if (mdr->attempt < m->get_attempt()) {
      // mine is old, close it out
      dout(10) << "local request " << *mdr << " attempt " << mdr->attempt << " < " << m->get_attempt()
	       << ", closing out" << dendl;
      mdcache->request_finish(mdr);
      mdr.reset();
    } else if (mdr->slave_to_mds != from) {
      dout(10) << "local request " << *mdr << " not slave to mds." << from << dendl;
      return;
    }

    // may get these while mdr->slave_request is non-null
    if (m->get_op() == MMDSSlaveRequest::OP_DROPLOCKS) {
      mds->locker->drop_locks(mdr.get());
      return;
    }
    if (m->get_op() == MMDSSlaveRequest::OP_FINISH) {
      if (m->is_abort()) {
	mdr->aborted = true;
	if (mdr->slave_request) {
	  // only abort on-going xlock, wrlock and auth pin
	  ceph_assert(!mdr->slave_did_prepare());
	} else {
	  mdcache->request_finish(mdr);
	}
      } else {
	if (m->inode_export.length() > 0)
	  mdr->more()->inode_import = m->inode_export;
	// finish off request.
	mdcache->request_finish(mdr);
      }
      return;
    }
  }
  if (!mdr.get()) {
    // new?
    if (m->get_op() == MMDSSlaveRequest::OP_FINISH) {
      dout(10) << "missing slave request for " << m->get_reqid() 
	       << " OP_FINISH, must have lost race with a forward" << dendl;
      return;
    }
    mdr = mdcache->request_start_slave(m->get_reqid(), m->get_attempt(), m);
    mdr->set_op_stamp(m->op_stamp);
  }
  ceph_assert(mdr->slave_request == 0);     // only one at a time, please!  

  if (straydn) {
    mdr->pin(straydn);
    mdr->straydn = straydn;
  }

  if (mds->is_clientreplay() && !mds->mdsmap->is_clientreplay(from) &&
      mdr->locks.empty()) {
    dout(3) << "not active yet, waiting" << dendl;
    mds->wait_for_active(new C_MDS_RetryMessage(mds, m));
    return;
  }

  mdr->reset_slave_request(m);
  
  dispatch_slave_request(mdr);
}

void Server::handle_slave_request_reply(const cref_t<MMDSSlaveRequest> &m)
{
  mds_rank_t from = mds_rank_t(m->get_source().num());
  
  if (!mds->is_clientreplay() && !mds->is_active() && !mds->is_stopping()) {
    metareqid_t r = m->get_reqid();
    if (!mdcache->have_uncommitted_leader(r, from)) {
      dout(10) << "handle_slave_request_reply ignoring slave reply from mds."
	       << from << " reqid " << r << dendl;
      return;
    }
    dout(3) << "not clientreplay|active yet, waiting" << dendl;
    mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (m->get_op() == MMDSSlaveRequest::OP_COMMITTED) {
    metareqid_t r = m->get_reqid();
    mdcache->committed_leader_slave(r, from);
    return;
  }

  MDRequestRef mdr = mdcache->request_get(m->get_reqid());
  if (m->get_attempt() != mdr->attempt) {
    dout(10) << "handle_slave_request_reply " << *mdr << " ignoring reply from other attempt "
	     << m->get_attempt() << dendl;
    return;
  }

  switch (m->get_op()) {
  case MMDSSlaveRequest::OP_XLOCKACK:
    {
      // identify lock, leader request
      SimpleLock *lock = mds->locker->get_lock(m->get_lock_type(),
					       m->get_object_info());
      mdr->more()->slaves.insert(from);
      lock->decode_locked_state(m->get_lock_data());
      dout(10) << "got remote xlock on " << *lock << " on " << *lock->get_parent() << dendl;
      mdr->emplace_lock(lock, MutationImpl::LockOp::XLOCK);
      mdr->finish_locking(lock);
      lock->get_xlock(mdr, mdr->get_client());

      ceph_assert(mdr->more()->waiting_on_slave.count(from));
      mdr->more()->waiting_on_slave.erase(from);
      ceph_assert(mdr->more()->waiting_on_slave.empty());
      mdcache->dispatch_request(mdr);
    }
    break;
    
  case MMDSSlaveRequest::OP_WRLOCKACK:
    {
      // identify lock, leader request
      SimpleLock *lock = mds->locker->get_lock(m->get_lock_type(),
					       m->get_object_info());
      mdr->more()->slaves.insert(from);
      dout(10) << "got remote wrlock on " << *lock << " on " << *lock->get_parent() << dendl;
      auto it = mdr->emplace_lock(lock, MutationImpl::LockOp::REMOTE_WRLOCK, from);
      ceph_assert(it->is_remote_wrlock());
      ceph_assert(it->wrlock_target == from);

      mdr->finish_locking(lock);

      ceph_assert(mdr->more()->waiting_on_slave.count(from));
      mdr->more()->waiting_on_slave.erase(from);
      ceph_assert(mdr->more()->waiting_on_slave.empty());
      mdcache->dispatch_request(mdr);
    }
    break;

  case MMDSSlaveRequest::OP_AUTHPINACK:
    handle_slave_auth_pin_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_LINKPREPACK:
    handle_slave_link_prep_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_RMDIRPREPACK:
    handle_slave_rmdir_prep_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_RENAMEPREPACK:
    handle_slave_rename_prep_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_RENAMENOTIFYACK:
    handle_slave_rename_notify_ack(mdr, m);
    break;

  default:
    ceph_abort();
  }
}

void Server::dispatch_slave_request(MDRequestRef& mdr)
{
  dout(7) << "dispatch_slave_request " << *mdr << " " << *mdr->slave_request << dendl;

  if (mdr->aborted) {
    dout(7) << " abort flag set, finishing" << dendl;
    mdcache->request_finish(mdr);
    return;
  }

  if (logger) logger->inc(l_mdss_dispatch_slave_request);

  int op = mdr->slave_request->get_op();
  switch (op) {
  case MMDSSlaveRequest::OP_XLOCK:
  case MMDSSlaveRequest::OP_WRLOCK:
    {
      // identify object
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());

      if (!lock) {
	dout(10) << "don't have object, dropping" << dendl;
	ceph_abort(); // can this happen, if we auth pinned properly.
      }
      if (op == MMDSSlaveRequest::OP_XLOCK && !lock->get_parent()->is_auth()) {
	dout(10) << "not auth for remote xlock attempt, dropping on " 
		 << *lock << " on " << *lock->get_parent() << dendl;
      } else {
	// use acquire_locks so that we get auth_pinning.
	MutationImpl::LockOpVec lov;
	for (const auto& p : mdr->locks) {
	  if (p.is_xlock())
	    lov.add_xlock(p.lock);
	  else if (p.is_wrlock())
	    lov.add_wrlock(p.lock);
	}

	int replycode = 0;
	switch (op) {
	case MMDSSlaveRequest::OP_XLOCK:
	  lov.add_xlock(lock);
	  replycode = MMDSSlaveRequest::OP_XLOCKACK;
	  break;
	case MMDSSlaveRequest::OP_WRLOCK:
	  lov.add_wrlock(lock);
	  replycode = MMDSSlaveRequest::OP_WRLOCKACK;
	  break;
	}
	
	if (!mds->locker->acquire_locks(mdr, lov))
	  return;
	
	// ack
	auto r = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, replycode);
	r->set_lock_type(lock->get_type());
	lock->get_parent()->set_object_info(r->get_object_info());
	if (replycode == MMDSSlaveRequest::OP_XLOCKACK)
	  lock->encode_locked_state(r->get_lock_data());
	mds->send_message(r, mdr->slave_request->get_connection());
      }

      // done.
      mdr->reset_slave_request();
    }
    break;

  case MMDSSlaveRequest::OP_UNXLOCK:
  case MMDSSlaveRequest::OP_UNWRLOCK:
    {  
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());
      ceph_assert(lock);
      auto it = mdr->locks.find(lock);
      ceph_assert(it != mdr->locks.end());
      bool need_issue = false;
      switch (op) {
      case MMDSSlaveRequest::OP_UNXLOCK:
	mds->locker->xlock_finish(it, mdr.get(), &need_issue);
	break;
      case MMDSSlaveRequest::OP_UNWRLOCK:
	mds->locker->wrlock_finish(it, mdr.get(), &need_issue);
	break;
      }
      if (need_issue)
	mds->locker->issue_caps(static_cast<CInode*>(lock->get_parent()));

      // done.  no ack necessary.
      mdr->reset_slave_request();
    }
    break;

  case MMDSSlaveRequest::OP_AUTHPIN:
    handle_slave_auth_pin(mdr);
    break;

  case MMDSSlaveRequest::OP_LINKPREP:
  case MMDSSlaveRequest::OP_UNLINKPREP:
    handle_slave_link_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_RMDIRPREP:
    handle_slave_rmdir_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_RENAMEPREP:
    handle_slave_rename_prep(mdr);
    break;

  default: 
    ceph_abort();
  }
}

void Server::handle_slave_auth_pin(MDRequestRef& mdr)
{
  dout(10) << "handle_slave_auth_pin " << *mdr << dendl;

  // build list of objects
  list<MDSCacheObject*> objects;
  CInode *auth_pin_freeze = NULL;
  bool nonblocking = mdr->slave_request->is_nonblocking();
  bool fail = false, wouldblock = false, readonly = false;
  ref_t<MMDSSlaveRequest> reply;

  if (mdcache->is_readonly()) {
    dout(10) << " read-only FS" << dendl;
    readonly = true;
    fail = true;
  }

  if (!fail) {
    for (const auto &oi : mdr->slave_request->get_authpins()) {
      MDSCacheObject *object = mdcache->get_object(oi);
      if (!object) {
	dout(10) << " don't have " << oi << dendl;
	fail = true;
	break;
      }

      objects.push_back(object);
      if (oi == mdr->slave_request->get_authpin_freeze())
	auth_pin_freeze = static_cast<CInode*>(object);
    }
  }
  
  // can we auth pin them?
  if (!fail) {
    for (const auto& obj : objects) {
      if (!obj->is_auth()) {
	dout(10) << " not auth for " << *obj << dendl;
	fail = true;
	break;
      }
      if (mdr->is_auth_pinned(obj))
	continue;
      if (!mdr->can_auth_pin(obj)) {
	if (nonblocking) {
	  dout(10) << " can't auth_pin (freezing?) " << *obj << " nonblocking" << dendl;
	  fail = true;
	  wouldblock = true;
	  break;
	}
	// wait
	dout(10) << " waiting for authpinnable on " << *obj << dendl;
	obj->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
	mdr->drop_local_auth_pins();

	mds->locker->notify_freeze_waiter(obj);
	goto blocked;
      }
    }
  }

  if (!fail) {
    /* freeze authpin wrong inode */
    if (mdr->has_more() && mdr->more()->is_freeze_authpin &&
	mdr->more()->rename_inode != auth_pin_freeze)
      mdr->unfreeze_auth_pin(true);

    /* handle_slave_rename_prep() call freeze_inode() to wait for all other operations
     * on the source inode to complete. This happens after all locks for the rename
     * operation are acquired. But to acquire locks, we need auth pin locks' parent
     * objects first. So there is an ABBA deadlock if someone auth pins the source inode
     * after locks are acquired and before Server::handle_slave_rename_prep() is called.
     * The solution is freeze the inode and prevent other MDRequests from getting new
     * auth pins.
     */
    if (auth_pin_freeze) {
      dout(10) << " freezing auth pin on " << *auth_pin_freeze << dendl;
      if (!mdr->freeze_auth_pin(auth_pin_freeze)) {
	auth_pin_freeze->add_waiter(CInode::WAIT_FROZEN, new C_MDS_RetryRequest(mdcache, mdr));
	mds->mdlog->flush();
	goto blocked;
      }
    }
  }

  reply = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_AUTHPINACK);

  if (fail) {
    mdr->drop_local_auth_pins();  // just in case
    if (readonly)
      reply->mark_error_rofs();
    if (wouldblock)
      reply->mark_error_wouldblock();
  } else {
    // auth pin!
    for (const auto& obj : objects) {
      dout(10) << "auth_pinning " << *obj << dendl;
      mdr->auth_pin(obj);
    }
    // return list of my auth_pins (if any)
    for (const auto &p : mdr->object_states) {
      if (!p.second.auth_pinned)
	continue;
      MDSCacheObjectInfo info;
      p.first->set_object_info(info);
      reply->get_authpins().push_back(info);
      if (p.first == (MDSCacheObject*)auth_pin_freeze)
	auth_pin_freeze->set_object_info(reply->get_authpin_freeze());
    }
  }

  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // clean up this request
  mdr->reset_slave_request();
  return;

blocked:
  if (mdr->slave_request->should_notify_blocking()) {
    reply = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_AUTHPINACK);
    reply->mark_req_blocked();
    mds->send_message_mds(reply, mdr->slave_to_mds);
    mdr->slave_request->clear_notify_blocking();
  }
  return;
}

void Server::handle_slave_auth_pin_ack(MDRequestRef& mdr, const cref_t<MMDSSlaveRequest> &ack)
{
  dout(10) << "handle_slave_auth_pin_ack on " << *mdr << " " << *ack << dendl;
  mds_rank_t from = mds_rank_t(ack->get_source().num());

  if (ack->is_req_blocked()) {
    mdr->disable_lock_cache();
    // slave auth pin is blocked, drop locks to avoid deadlock
    mds->locker->drop_locks(mdr.get(), nullptr);
    return;
  }

  // added auth pins?
  set<MDSCacheObject*> pinned;
  for (const auto &oi : ack->get_authpins()) {
    MDSCacheObject *object = mdcache->get_object(oi);
    ceph_assert(object);  // we pinned it
    dout(10) << " remote has pinned " << *object << dendl;
    mdr->set_remote_auth_pinned(object, from);
    if (oi == ack->get_authpin_freeze())
      mdr->set_remote_frozen_auth_pin(static_cast<CInode *>(object));
    pinned.insert(object);
  }

  // removed frozen auth pin ?
  if (mdr->more()->is_remote_frozen_authpin &&
      ack->get_authpin_freeze() == MDSCacheObjectInfo()) {
    auto stat_p = mdr->find_object_state(mdr->more()->rename_inode);
    ceph_assert(stat_p);
    if (stat_p->remote_auth_pinned == from) {
      mdr->more()->is_remote_frozen_authpin = false;
    }
  }

  // removed auth pins?
  for (auto& p : mdr->object_states) {
    if (p.second.remote_auth_pinned == MDS_RANK_NONE)
      continue;
    MDSCacheObject* object = p.first;
    if (p.second.remote_auth_pinned == from && pinned.count(object) == 0) {
      dout(10) << " remote has unpinned " << *object << dendl;
      mdr->_clear_remote_auth_pinned(p.second);
    }
  }

  // note slave
  mdr->more()->slaves.insert(from);

  // clear from waiting list
  auto ret = mdr->more()->waiting_on_slave.erase(from);
  ceph_assert(ret);

  if (ack->is_error_rofs()) {
    mdr->more()->slave_error = -EROFS;
  } else if (ack->is_error_wouldblock()) {
    mdr->more()->slave_error = -EWOULDBLOCK;
  }

  // go again?
  if (mdr->more()->waiting_on_slave.empty())
    mdcache->dispatch_request(mdr);
  else 
    dout(10) << "still waiting on slaves " << mdr->more()->waiting_on_slave << dendl;
}


// ---------------------------------------
// HELPERS


/**
 * check whether we are permitted to complete a request
 *
 * Check whether we have permission to perform the operation specified
 * by mask on the given inode, based on the capability in the mdr's
 * session.
 */
bool Server::check_access(MDRequestRef& mdr, CInode *in, unsigned mask)
{
  if (mdr->session) {
    int r = mdr->session->check_access(
      in, mask,
      mdr->client_request->get_caller_uid(),
      mdr->client_request->get_caller_gid(),
      &mdr->client_request->get_caller_gid_list(),
      mdr->client_request->head.args.setattr.uid,
      mdr->client_request->head.args.setattr.gid);
    if (r < 0) {
      respond_to_request(mdr, r);
      return false;
    }
  }
  return true;
}

/**
 * check whether fragment has reached maximum size
 *
 */
bool Server::check_fragment_space(MDRequestRef &mdr, CDir *in)
{
  const auto size = in->get_frag_size();
  if (size >= g_conf()->mds_bal_fragment_size_max) {
    dout(10) << "fragment " << *in << " size exceeds " << g_conf()->mds_bal_fragment_size_max << " (ENOSPC)" << dendl;
    respond_to_request(mdr, -ENOSPC);
    return false;
  }

  return true;
}

CDentry* Server::prepare_stray_dentry(MDRequestRef& mdr, CInode *in)
{
  string straydname;
  in->name_stray_dentry(straydname);

  CDentry *straydn = mdr->straydn;
  if (straydn) {
    ceph_assert(straydn->get_name() == straydname);
    return straydn;
  }
  CDir *straydir = mdcache->get_stray_dir(in);

  if (!mdr->client_request->is_replay() &&
      !check_fragment_space(mdr, straydir))
    return nullptr;

  straydn = straydir->lookup(straydname);
  if (!straydn) {
    if (straydir->is_frozen_dir()) {
      dout(10) << __func__ << ": " << *straydir << " is frozen, waiting" << dendl;
      mds->locker->drop_locks(mdr.get());
      mdr->drop_local_auth_pins();
      straydir->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      return nullptr;
    }
    straydn = straydir->add_null_dentry(straydname);
    straydn->mark_new();
  } else {
    ceph_assert(straydn->get_projected_linkage()->is_null());
  }

  straydn->state_set(CDentry::STATE_STRAY);
  mdr->straydn = straydn;
  mdr->pin(straydn);

  return straydn;
}

/** prepare_new_inode
 *
 * create a new inode.  set c/m/atime.  hit dir pop.
 */
CInode* Server::prepare_new_inode(MDRequestRef& mdr, CDir *dir, inodeno_t useino, unsigned mode,
				  file_layout_t *layout)
{
  CInode *in = new CInode(mdcache);
  
  // Server::prepare_force_open_sessions() can re-open session in closing
  // state. In that corner case, session's prealloc_inos are being freed.
  // To simplify the code, we disallow using/refilling session's prealloc_ino
  // while session is opening.
  bool allow_prealloc_inos = mdr->session->is_open();

  // assign ino
  if (allow_prealloc_inos && (mdr->used_prealloc_ino = in->inode.ino = mdr->session->take_ino(useino))) {
    mds->sessionmap.mark_projected(mdr->session);
    dout(10) << "prepare_new_inode used_prealloc " << mdr->used_prealloc_ino
	     << " (" << mdr->session->info.prealloc_inos
	     << ", " << mdr->session->info.prealloc_inos.size() << " left)"
	     << dendl;
  } else {
    mdr->alloc_ino = 
      in->inode.ino = mds->inotable->project_alloc_id(useino);
    dout(10) << "prepare_new_inode alloc " << mdr->alloc_ino << dendl;
  }

  if (useino && useino != in->inode.ino) {
    dout(0) << "WARNING: client specified " << useino << " and i allocated " << in->inode.ino << dendl;
    mds->clog->error() << mdr->client_request->get_source()
       << " specified ino " << useino
       << " but mds." << mds->get_nodeid() << " allocated " << in->inode.ino;
    //ceph_abort(); // just for now.
  }
    
  if (allow_prealloc_inos &&
      mdr->session->get_num_projected_prealloc_inos() < g_conf()->mds_client_prealloc_inos / 2) {
    int need = g_conf()->mds_client_prealloc_inos - mdr->session->get_num_projected_prealloc_inos();
    mds->inotable->project_alloc_ids(mdr->prealloc_inos, need);
    ceph_assert(mdr->prealloc_inos.size());  // or else fix projected increment semantics
    mdr->session->pending_prealloc_inos.insert(mdr->prealloc_inos);
    mds->sessionmap.mark_projected(mdr->session);
    dout(10) << "prepare_new_inode prealloc " << mdr->prealloc_inos << dendl;
  }

  in->inode.version = 1;
  in->inode.xattr_version = 1;
  in->inode.nlink = 1;   // FIXME

  in->inode.mode = mode;

  // FIPS zeroization audit 20191117: this memset is not security related.
  memset(&in->inode.dir_layout, 0, sizeof(in->inode.dir_layout));
  if (in->inode.is_dir()) {
    in->inode.dir_layout.dl_dir_hash = g_conf()->mds_default_dir_hash;
  } else if (layout) {
    in->inode.layout = *layout;
  } else {
    in->inode.layout = mdcache->default_file_layout;
  }

  in->inode.truncate_size = -1ull;  // not truncated, yet!
  in->inode.truncate_seq = 1; /* starting with 1, 0 is kept for no-truncation logic */

  CInode *diri = dir->get_inode();

  dout(10) << oct << " dir mode 0" << diri->inode.mode << " new mode 0" << mode << dec << dendl;

  if (diri->inode.mode & S_ISGID) {
    dout(10) << " dir is sticky" << dendl;
    in->inode.gid = diri->inode.gid;
    if (S_ISDIR(mode)) {
      dout(10) << " new dir also sticky" << dendl;      
      in->inode.mode |= S_ISGID;
    }
  } else 
    in->inode.gid = mdr->client_request->get_caller_gid();

  in->inode.uid = mdr->client_request->get_caller_uid();

  in->inode.btime = in->inode.ctime = in->inode.mtime = in->inode.atime =
    mdr->get_op_stamp();

  in->inode.change_attr = 0;

  const cref_t<MClientRequest> &req = mdr->client_request;
  if (req->get_data().length()) {
    auto p = req->get_data().cbegin();

    // xattrs on new inode?
    CInode::mempool_xattr_map xattrs;
    decode_noshare(xattrs, p);
    for (const auto &p : xattrs) {
      dout(10) << "prepare_new_inode setting xattr " << p.first << dendl;
      auto em = in->xattrs.emplace(std::piecewise_construct, std::forward_as_tuple(p.first), std::forward_as_tuple(p.second));
      if (!em.second)
        em.first->second = p.second;
    }
  }

  if (!mds->mdsmap->get_inline_data_enabled() ||
      !mdr->session->get_connection()->has_feature(CEPH_FEATURE_MDS_INLINE_DATA))
    in->inode.inline_data.version = CEPH_INLINE_NONE;

  mdcache->add_inode(in);  // add
  dout(10) << "prepare_new_inode " << *in << dendl;
  return in;
}

void Server::journal_allocated_inos(MDRequestRef& mdr, EMetaBlob *blob)
{
  dout(20) << "journal_allocated_inos sessionmapv " << mds->sessionmap.get_projected()
	   << " inotablev " << mds->inotable->get_projected_version()
	   << dendl;
  blob->set_ino_alloc(mdr->alloc_ino,
		      mdr->used_prealloc_ino,
		      mdr->prealloc_inos,
		      mdr->client_request->get_source(),
		      mds->sessionmap.get_projected(),
		      mds->inotable->get_projected_version());
}

void Server::apply_allocated_inos(MDRequestRef& mdr, Session *session)
{
  dout(10) << "apply_allocated_inos " << mdr->alloc_ino
	   << " / " << mdr->prealloc_inos
	   << " / " << mdr->used_prealloc_ino << dendl;

  if (mdr->alloc_ino) {
    mds->inotable->apply_alloc_id(mdr->alloc_ino);
  }
  if (mdr->prealloc_inos.size()) {
    ceph_assert(session);
    session->pending_prealloc_inos.subtract(mdr->prealloc_inos);
    session->info.prealloc_inos.insert(mdr->prealloc_inos);
    mds->sessionmap.mark_dirty(session, !mdr->used_prealloc_ino);
    mds->inotable->apply_alloc_ids(mdr->prealloc_inos);
  }
  if (mdr->used_prealloc_ino) {
    ceph_assert(session);
    session->info.used_inos.erase(mdr->used_prealloc_ino);
    mds->sessionmap.mark_dirty(session);
  }
}

class C_MDS_TryFindInode : public ServerContext {
  MDRequestRef mdr;
public:
  C_MDS_TryFindInode(Server *s, MDRequestRef& r) : ServerContext(s), mdr(r) {}
  void finish(int r) override {
    if (r == -ESTALE) // :( find_ino_peers failed
      server->respond_to_request(mdr, r);
    else
      server->dispatch_client_request(mdr);
  }
};

class CF_MDS_MDRContextFactory : public MDSContextFactory {
public:
  CF_MDS_MDRContextFactory(MDCache *cache, MDRequestRef &mdr, bool dl) :
    mdcache(cache), mdr(mdr), drop_locks(dl) {}
  MDSContext *build() {
    if (drop_locks) {
      mdcache->mds->locker->drop_locks(mdr.get(), nullptr);
      mdr->drop_local_auth_pins();
    }
    return new C_MDS_RetryRequest(mdcache, mdr);
  }
private:
  MDCache *mdcache;
  MDRequestRef mdr;
  bool drop_locks;
};

/* If this returns null, the request has been handled
 * as appropriate: forwarded on, or the client's been replied to */
CInode* Server::rdlock_path_pin_ref(MDRequestRef& mdr,
				    bool want_auth,
				    bool no_want_auth)
{
  const filepath& refpath = mdr->get_filepath();
  dout(10) << "rdlock_path_pin_ref " << *mdr << " " << refpath << dendl;

  if (mdr->locking_state & MutationImpl::PATH_LOCKED)
    return mdr->in[0];

  // traverse
  CF_MDS_MDRContextFactory cf(mdcache, mdr, true);
  int flags = 0;
  if (refpath.is_last_snap()) {
    if (!no_want_auth)
      want_auth = true;
  } else {
    flags |= MDS_TRAVERSE_RDLOCK_PATH | MDS_TRAVERSE_RDLOCK_SNAP;
  }
  if (want_auth)
    flags |= MDS_TRAVERSE_WANT_AUTH;
  int r = mdcache->path_traverse(mdr, cf, refpath, flags, &mdr->dn[0], &mdr->in[0]);
  if (r > 0)
    return nullptr; // delayed
  if (r < 0) {  // error
    if (r == -ENOENT && !mdr->dn[0].empty()) {
      if (mdr->client_request &&
	  mdr->client_request->get_dentry_wanted())
        mdr->tracedn = mdr->dn[0].back();
      respond_to_request(mdr, r);
    } else if (r == -ESTALE) {
      dout(10) << "FAIL on ESTALE but attempting recovery" << dendl;
      MDSContext *c = new C_MDS_TryFindInode(this, mdr);
      mdcache->find_ino_peers(refpath.get_ino(), c);
    } else {
      dout(10) << "FAIL on error " << r << dendl;
      respond_to_request(mdr, r);
    }
    return nullptr;
  }
  CInode *ref = mdr->in[0];
  dout(10) << "ref is " << *ref << dendl;

  if (want_auth) {
    // auth_pin?
    //   do NOT proceed if freezing, as cap release may defer in that case, and
    //   we could deadlock when we try to lock @ref.
    // if we're already auth_pinned, continue; the release has already been processed.
    if (ref->is_frozen() || ref->is_frozen_auth_pin() ||
	(ref->is_freezing() && !mdr->is_auth_pinned(ref))) {
      dout(7) << "waiting for !frozen/authpinnable on " << *ref << dendl;
      ref->add_waiter(CInode::WAIT_UNFREEZE, cf.build());
      if (mdr->is_any_remote_auth_pin())
	mds->locker->notify_freeze_waiter(ref);
      return 0;
    }
    mdr->auth_pin(ref);
  }

  // set and pin ref
  mdr->pin(ref);
  return ref;
}


/** rdlock_path_xlock_dentry
 * traverse path to the directory that could/would contain dentry.
 * make sure i am auth for that dentry, forward as necessary.
 * create null dentry in place (or use existing if okexist).
 * get rdlocks on traversed dentries, xlock on new dentry.
 */
CDentry* Server::rdlock_path_xlock_dentry(MDRequestRef& mdr,
					  bool create, bool okexist, bool want_layout)
{
  const filepath& refpath = mdr->get_filepath();
  dout(10) << "rdlock_path_xlock_dentry " << *mdr << " " << refpath << dendl;

  if (mdr->locking_state & MutationImpl::PATH_LOCKED)
    return mdr->dn[0].back();

  // figure parent dir vs dname
  if (refpath.depth() == 0) {
    dout(7) << "invalid path (zero length)" << dendl;
    respond_to_request(mdr, -EINVAL);
    return nullptr;
  }

  if (refpath.is_last_snap()) {
    respond_to_request(mdr, -EROFS);
    return nullptr;
  }

  if (refpath.is_last_dot_or_dotdot()) {
    dout(7) << "invalid path (last dot or dot_dot)" << dendl;
    if (create)
      respond_to_request(mdr, -EEXIST);
    else
      respond_to_request(mdr, -ENOTEMPTY);
    return nullptr;
  }

  // traverse to parent dir
  CF_MDS_MDRContextFactory cf(mdcache, mdr, true);
  int flags = MDS_TRAVERSE_RDLOCK_SNAP | MDS_TRAVERSE_RDLOCK_PATH |
	      MDS_TRAVERSE_WANT_DENTRY | MDS_TRAVERSE_XLOCK_DENTRY |
	      MDS_TRAVERSE_WANT_AUTH;
  if (refpath.depth() == 1 && !mdr->lock_cache_disabled)
    flags |= MDS_TRAVERSE_CHECK_LOCKCACHE;
  if (create)
    flags |= MDS_TRAVERSE_RDLOCK_AUTHLOCK;
  if (want_layout)
    flags |= MDS_TRAVERSE_WANT_DIRLAYOUT;
  int r = mdcache->path_traverse(mdr, cf, refpath, flags, &mdr->dn[0]);
  if (r > 0)
    return nullptr; // delayed
  if (r < 0) {
    if (r == -ESTALE) {
      dout(10) << "FAIL on ESTALE but attempting recovery" << dendl;
      mdcache->find_ino_peers(refpath.get_ino(), new C_MDS_TryFindInode(this, mdr));
      return nullptr;
    }
    respond_to_request(mdr, r);
    return nullptr;
  }

  CDentry *dn = mdr->dn[0].back();
  CDir *dir = dn->get_dir();
  CInode *diri = dir->get_inode();

  if (!mdr->reqid.name.is_mds()) {
    if (diri->is_system() && !diri->is_root()) {
      respond_to_request(mdr, -EROFS);
      return nullptr;
    }
  }

  if (!diri->is_base() && diri->get_projected_parent_dir()->inode->is_stray()) {
    respond_to_request(mdr, -ENOENT);
    return nullptr;
  }

  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  if (dnl->is_null()) {
    if (!create && okexist) {
      respond_to_request(mdr, -ENOENT);
      return nullptr;
    }

    snapid_t next_snap = mdcache->get_global_snaprealm()->get_newest_seq() + 1;
    dn->first = std::max(dn->first, next_snap);
  } else {
    if (!okexist) {
      respond_to_request(mdr, -EEXIST);
      return nullptr;
    }
    mdr->in[0] = dnl->get_inode();
  }

  return dn;
}

/** rdlock_two_paths_xlock_destdn
 * traverse two paths and lock the two paths in proper order.
 * The order of taking locks is:
 * 1. Lock directory inodes or dentries according to which trees they
 *    are under. Lock objects under fs root before objects under mdsdir.
 * 2. Lock directory inodes or dentries according to their depth, in
 *    ascending order.
 * 3. Lock directory inodes or dentries according to inode numbers or
 *    dentries' parent inode numbers, in ascending order.
 * 4. Lock dentries in the same directory in order of their keys.
 * 5. Lock non-directory inodes according to inode numbers, in ascending
 *    order.
 */
std::pair<CDentry*, CDentry*>
Server::rdlock_two_paths_xlock_destdn(MDRequestRef& mdr, bool xlock_srcdn)
{

  const filepath& refpath = mdr->get_filepath();
  const filepath& refpath2 = mdr->get_filepath2();

  dout(10) << "rdlock_two_paths_xlock_destdn " << *mdr << " " << refpath << " " << refpath2 << dendl;

  if (mdr->locking_state & MutationImpl::PATH_LOCKED)
    return std::make_pair(mdr->dn[0].back(), mdr->dn[1].back());

  if (refpath.depth() != 1 || refpath2.depth() != 1) {
    respond_to_request(mdr, -EINVAL);
    return std::pair<CDentry*, CDentry*>(nullptr, nullptr);
  }

  if (refpath.is_last_snap() || refpath2.is_last_snap()) {
    respond_to_request(mdr, -EROFS);
    return std::make_pair(nullptr, nullptr);
  }

  // traverse to parent dir
  CF_MDS_MDRContextFactory cf(mdcache, mdr, true);
  int flags = MDS_TRAVERSE_RDLOCK_SNAP |  MDS_TRAVERSE_WANT_DENTRY | MDS_TRAVERSE_WANT_AUTH;
  int r = mdcache->path_traverse(mdr, cf, refpath, flags, &mdr->dn[0]);
  if (r != 0) {
    if (r == -ESTALE) {
      dout(10) << "ESTALE on path, attempting recovery" << dendl;
      mdcache->find_ino_peers(refpath.get_ino(), new C_MDS_TryFindInode(this, mdr));
    } else if (r < 0) {
      respond_to_request(mdr, r);
    }
    return std::make_pair(nullptr, nullptr);
  }

  flags = MDS_TRAVERSE_RDLOCK_SNAP2 | MDS_TRAVERSE_WANT_DENTRY | MDS_TRAVERSE_DISCOVER;
  r = mdcache->path_traverse(mdr, cf, refpath2, flags, &mdr->dn[1]);
  if (r != 0) {
    if (r == -ESTALE) {
      dout(10) << "ESTALE on path2, attempting recovery" << dendl;
      mdcache->find_ino_peers(refpath2.get_ino(), new C_MDS_TryFindInode(this, mdr));
    } else if (r < 0) {
      respond_to_request(mdr, r);
    }
    return std::make_pair(nullptr, nullptr);
  }

  CDentry *srcdn = mdr->dn[1].back();
  CDir *srcdir = srcdn->get_dir();
  CDentry *destdn = mdr->dn[0].back();
  CDir *destdir = destdn->get_dir();

  if (!mdr->reqid.name.is_mds()) {
    if ((srcdir->get_inode()->is_system() && !srcdir->get_inode()->is_root()) ||
	(destdir->get_inode()->is_system() && !destdir->get_inode()->is_root())) {
      respond_to_request(mdr, -EROFS);
      return std::make_pair(nullptr, nullptr);
    }
  }

  if (!destdir->get_inode()->is_base() &&
      destdir->get_inode()->get_projected_parent_dir()->inode->is_stray()) {
    respond_to_request(mdr, -ENOENT);
    return std::make_pair(nullptr, nullptr);
  }

  MutationImpl::LockOpVec lov;
  if (srcdir->get_inode() == destdir->get_inode()) {
    lov.add_wrlock(&destdir->inode->filelock);
    lov.add_wrlock(&destdir->inode->nestlock);
    if (xlock_srcdn && srcdir != destdir) {
      mds_rank_t srcdir_auth = srcdir->authority().first;
      if (srcdir_auth != mds->get_nodeid()) {
	lov.add_remote_wrlock(&srcdir->inode->filelock, srcdir_auth);
	lov.add_remote_wrlock(&srcdir->inode->nestlock, srcdir_auth);
      }
    }

    if (srcdn->get_name() > destdn->get_name())
      lov.add_xlock(&destdn->lock);

    if (xlock_srcdn)
      lov.add_xlock(&srcdn->lock);
    else
      lov.add_rdlock(&srcdn->lock);

    if (srcdn->get_name() < destdn->get_name())
      lov.add_xlock(&destdn->lock);
  } else {
    int cmp = mdr->compare_paths();
    bool lock_destdir_first =
      (cmp < 0 || (cmp == 0 && destdir->ino() < srcdir->ino()));

    if (lock_destdir_first) {
      lov.add_wrlock(&destdir->inode->filelock);
      lov.add_wrlock(&destdir->inode->nestlock);
      lov.add_xlock(&destdn->lock);
    }

    if (xlock_srcdn) {
      mds_rank_t srcdir_auth = srcdir->authority().first;
      if (srcdir_auth == mds->get_nodeid()) {
	lov.add_wrlock(&srcdir->inode->filelock);
	lov.add_wrlock(&srcdir->inode->nestlock);
      } else {
	lov.add_remote_wrlock(&srcdir->inode->filelock, srcdir_auth);
	lov.add_remote_wrlock(&srcdir->inode->nestlock, srcdir_auth);
      }
      lov.add_xlock(&srcdn->lock);
    } else {
      lov.add_rdlock(&srcdn->lock);
    }

    if (!lock_destdir_first) {
      lov.add_wrlock(&destdir->inode->filelock);
      lov.add_wrlock(&destdir->inode->nestlock);
      lov.add_xlock(&destdn->lock);
    }
  }

  CInode *auth_pin_freeze = nullptr;
  // XXX any better way to do this?
  if (xlock_srcdn && !srcdn->is_auth()) {
    CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
    auth_pin_freeze = srcdnl->is_primary() ? srcdnl->get_inode() : nullptr;
  }
  if (!mds->locker->acquire_locks(mdr, lov, auth_pin_freeze))
    return std::make_pair(nullptr, nullptr);

  if (srcdn->get_projected_linkage()->is_null()) {
    respond_to_request(mdr, -ENOENT);
    return std::make_pair(nullptr, nullptr);
  }

  if (destdn->get_projected_linkage()->is_null()) {
    snapid_t next_snap = mdcache->get_global_snaprealm()->get_newest_seq() + 1;
    destdn->first = std::max(destdn->first, next_snap);
  }

  mdr->locking_state |= MutationImpl::PATH_LOCKED;

  return std::make_pair(destdn, srcdn);
}

/**
 * try_open_auth_dirfrag -- open dirfrag, or forward to dirfrag auth
 *
 * @param diri base inode
 * @param fg the exact frag we want
 * @param mdr request
 * @returns the pointer, or NULL if it had to be delayed (but mdr is taken care of)
 */
CDir* Server::try_open_auth_dirfrag(CInode *diri, frag_t fg, MDRequestRef& mdr)
{
  CDir *dir = diri->get_dirfrag(fg);

  if (dir) {
    // am i auth for the dirfrag?
    if (!dir->is_auth()) {
      mds_rank_t auth = dir->authority().first;
      dout(7) << "try_open_auth_dirfrag: not auth for " << *dir
	<< ", fw to mds." << auth << dendl;
      mdcache->request_forward(mdr, auth);
      return nullptr;
    }
  } else {
    // not open and inode not mine?
    if (!diri->is_auth()) {
      mds_rank_t inauth = diri->authority().first;
      dout(7) << "try_open_auth_dirfrag: not open, not inode auth, fw to mds." << inauth << dendl;
      mdcache->request_forward(mdr, inauth);
      return nullptr;
    }

    // not open and inode frozen?
    if (diri->is_frozen()) {
      dout(10) << "try_open_auth_dirfrag: dir inode is frozen, waiting " << *diri << dendl;
      ceph_assert(diri->get_parent_dir());
      diri->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      return nullptr;
    }

    // invent?
    dir = diri->get_or_open_dirfrag(mdcache, fg);
  }

  return dir;
}


// ===============================================================================
// STAT

void Server::handle_client_getattr(MDRequestRef& mdr, bool is_lookup)
{
  const cref_t<MClientRequest> &req = mdr->client_request;

  if (req->get_filepath().depth() == 0 && is_lookup) {
    // refpath can't be empty for lookup but it can for
    // getattr (we do getattr with empty refpath for mount of '/')
    respond_to_request(mdr, -EINVAL);
    return;
  }

  bool want_auth = false;
  int mask = req->head.args.getattr.mask;
  if (mask & CEPH_STAT_RSTAT)
    want_auth = true; // set want_auth for CEPH_STAT_RSTAT mask

  if (!mdr->is_batch_head() && mdr->can_batch()) {
    CF_MDS_MDRContextFactory cf(mdcache, mdr, false);
    int r = mdcache->path_traverse(mdr, cf, mdr->get_filepath(),
				   (want_auth ? MDS_TRAVERSE_WANT_AUTH : 0),
				   &mdr->dn[0], &mdr->in[0]);
    if (r > 0)
      return; // delayed

    if (r < 0) {
      // fall-thru. let rdlock_path_pin_ref() check again.
    } else if (is_lookup) {
      CDentry* dn = mdr->dn[0].back();
      mdr->pin(dn);
      auto em = dn->batch_ops.emplace(std::piecewise_construct, std::forward_as_tuple(mask), std::forward_as_tuple());
      if (em.second) {
	em.first->second = std::make_unique<Batch_Getattr_Lookup>(this, mdr);
      } else {
	dout(20) << __func__ << ": LOOKUP op, wait for previous same getattr ops to respond. " << *mdr << dendl;
	em.first->second->add_request(mdr);
	return;
      }
    } else {
      CInode *in = mdr->in[0];
      mdr->pin(in);
      auto em = in->batch_ops.emplace(std::piecewise_construct, std::forward_as_tuple(mask), std::forward_as_tuple());
      if (em.second) {
	em.first->second = std::make_unique<Batch_Getattr_Lookup>(this, mdr);
      } else {
	dout(20) << __func__ << ": GETATTR op, wait for previous same getattr ops to respond. " << *mdr << dendl;
	em.first->second->add_request(mdr);
	return;
      }
    }
  }

  CInode *ref = rdlock_path_pin_ref(mdr, want_auth, false);
  if (!ref)
    return;

  mdr->getattr_caps = mask;

  /*
   * if client currently holds the EXCL cap on a field, do not rdlock
   * it; client's stat() will result in valid info if _either_ EXCL
   * cap is held or MDS rdlocks and reads the value here.
   *
   * handling this case here is easier than weakening rdlock
   * semantics... that would cause problems elsewhere.
   */
  client_t client = mdr->get_client();
  int issued = 0;
  Capability *cap = ref->get_client_cap(client);
  if (cap && (mdr->snapid == CEPH_NOSNAP ||
	      mdr->snapid <= cap->client_follows))
    issued = cap->issued();

  // FIXME
  MutationImpl::LockOpVec lov;
  if ((mask & CEPH_CAP_LINK_SHARED) && !(issued & CEPH_CAP_LINK_EXCL))
    lov.add_rdlock(&ref->linklock);
  if ((mask & CEPH_CAP_AUTH_SHARED) && !(issued & CEPH_CAP_AUTH_EXCL))
    lov.add_rdlock(&ref->authlock);
  if ((mask & CEPH_CAP_XATTR_SHARED) && !(issued & CEPH_CAP_XATTR_EXCL))
    lov.add_rdlock(&ref->xattrlock);
  if ((mask & CEPH_CAP_FILE_SHARED) && !(issued & CEPH_CAP_FILE_EXCL)) {
    // Don't wait on unstable filelock if client is allowed to read file size.
    // This can reduce the response time of getattr in the case that multiple
    // clients do stat(2) and there are writers.
    // The downside of this optimization is that mds may not issue Fs caps along
    // with getattr reply. Client may need to send more getattr requests.
    if (mdr->is_rdlocked(&ref->filelock)) {
      lov.add_rdlock(&ref->filelock);
    } else if (ref->filelock.is_stable() ||
	       ref->filelock.get_num_wrlocks() > 0 ||
	       !ref->filelock.can_read(mdr->get_client())) {
      lov.add_rdlock(&ref->filelock);
      mdr->locking_state &= ~MutationImpl::ALL_LOCKED;
    }
  }

  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  if (!check_access(mdr, ref, MAY_READ))
    return;

  utime_t now = ceph_clock_now();
  mdr->set_mds_stamp(now);

  // note which caps are requested, so we return at least a snapshot
  // value for them.  (currently this matters for xattrs and inline data)
  mdr->getattr_caps = mask;

  mds->balancer->hit_inode(ref, META_POP_IRD, req->get_source().num());

  // reply
  dout(10) << "reply to stat on " << *req << dendl;
  mdr->tracei = ref;
  if (is_lookup)
    mdr->tracedn = mdr->dn[0].back();
  respond_to_request(mdr, 0);
}

struct C_MDS_LookupIno2 : public ServerContext {
  MDRequestRef mdr;
  C_MDS_LookupIno2(Server *s, MDRequestRef& r) : ServerContext(s), mdr(r) {}
  void finish(int r) override {
    server->_lookup_ino_2(mdr, r);
  }
};

/*
 * filepath:  ino
 */
void Server::handle_client_lookup_ino(MDRequestRef& mdr,
				      bool want_parent, bool want_dentry)
{
  const cref_t<MClientRequest> &req = mdr->client_request;

  if ((uint64_t)req->head.args.lookupino.snapid > 0)
    return _lookup_snap_ino(mdr);

  inodeno_t ino = req->get_filepath().get_ino();
  CInode *in = mdcache->get_inode(ino);
  if (in && in->state_test(CInode::STATE_PURGING)) {
    respond_to_request(mdr, -ESTALE);
    return;
  }
  if (!in) {
    mdcache->open_ino(ino, (int64_t)-1, new C_MDS_LookupIno2(this, mdr), false);
    return;
  }

  if (mdr && in->snaprealm && !in->snaprealm->have_past_parents_open() &&
      !in->snaprealm->open_parents(new C_MDS_RetryRequest(mdcache, mdr))) {
    return;
  }

  // check for nothing (not read or write); this still applies the
  // path check.
  if (!check_access(mdr, in, 0))
    return;

  CDentry *dn = in->get_projected_parent_dn();
  CInode *diri = dn ? dn->get_dir()->inode : NULL;

  MutationImpl::LockOpVec lov;
  if (dn && (want_parent || want_dentry)) {
    mdr->pin(dn);
    lov.add_rdlock(&dn->lock);
  }

  unsigned mask = req->head.args.lookupino.mask;
  if (mask) {
    Capability *cap = in->get_client_cap(mdr->get_client());
    int issued = 0;
    if (cap && (mdr->snapid == CEPH_NOSNAP || mdr->snapid <= cap->client_follows))
      issued = cap->issued();
    // FIXME
    // permission bits, ACL/security xattrs
    if ((mask & CEPH_CAP_AUTH_SHARED) && (issued & CEPH_CAP_AUTH_EXCL) == 0)
      lov.add_rdlock(&in->authlock);
    if ((mask & CEPH_CAP_XATTR_SHARED) && (issued & CEPH_CAP_XATTR_EXCL) == 0)
      lov.add_rdlock(&in->xattrlock);

    mdr->getattr_caps = mask;
  }

  if (!lov.empty()) {
    if (!mds->locker->acquire_locks(mdr, lov))
      return;

    if (diri != NULL) {
      // need read access to directory inode
      if (!check_access(mdr, diri, MAY_READ))
        return;
    }
  }

  if (want_parent) {
    if (in->is_base()) {
      respond_to_request(mdr, -EINVAL);
      return;
    }
    if (!diri || diri->is_stray()) {
      respond_to_request(mdr, -ESTALE);
      return;
    }
    dout(10) << "reply to lookup_parent " << *in << dendl;
    mdr->tracei = diri;
    respond_to_request(mdr, 0);
  } else {
    if (want_dentry) {
      inodeno_t dirino = req->get_filepath2().get_ino();
      if (!diri || (dirino != inodeno_t() && diri->ino() != dirino)) {
	respond_to_request(mdr, -ENOENT);
	return;
      }
      dout(10) << "reply to lookup_name " << *in << dendl;
    } else
      dout(10) << "reply to lookup_ino " << *in << dendl;

    mdr->tracei = in;
    if (want_dentry)
      mdr->tracedn = dn;
    respond_to_request(mdr, 0);
  }
}

void Server::_lookup_snap_ino(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;

  vinodeno_t vino;
  vino.ino = req->get_filepath().get_ino();
  vino.snapid = (__u64)req->head.args.lookupino.snapid;
  inodeno_t parent_ino = (__u64)req->head.args.lookupino.parent;
  __u32 hash = req->head.args.lookupino.hash;

  dout(7) << "lookup_snap_ino " << vino << " parent " << parent_ino << " hash " << hash << dendl;

  CInode *in = mdcache->lookup_snap_inode(vino);
  if (!in) {
    in = mdcache->get_inode(vino.ino);
    if (in) {
      if (in->state_test(CInode::STATE_PURGING) ||
	  !in->has_snap_data(vino.snapid)) {
	if (in->is_dir() || !parent_ino) {
	  respond_to_request(mdr, -ESTALE);
	  return;
	}
	in = NULL;
      }
    }
  }

  if (in) {
    dout(10) << "reply to lookup_snap_ino " << *in << dendl;
    mdr->snapid = vino.snapid;
    mdr->tracei = in;
    respond_to_request(mdr, 0);
    return;
  }

  CInode *diri = NULL;
  if (parent_ino) {
    diri = mdcache->get_inode(parent_ino);
    if (!diri) {
      mdcache->open_ino(parent_ino, mds->mdsmap->get_metadata_pool(), new C_MDS_LookupIno2(this, mdr));
      return;
    }

    if (!diri->is_dir()) {
      respond_to_request(mdr, -EINVAL);
      return;
    }

    MutationImpl::LockOpVec lov;
    lov.add_rdlock(&diri->dirfragtreelock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;

    frag_t frag = diri->dirfragtree[hash];
    CDir *dir = try_open_auth_dirfrag(diri, frag, mdr);
    if (!dir)
      return;

    if (!dir->is_complete()) {
      if (dir->is_frozen()) {
	mds->locker->drop_locks(mdr.get());
	mdr->drop_local_auth_pins();
	dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr), true);
      return;
    }

    respond_to_request(mdr, -ESTALE);
  } else {
    mdcache->open_ino(vino.ino, mds->mdsmap->get_metadata_pool(), new C_MDS_LookupIno2(this, mdr), false);
  }
}

void Server::_lookup_ino_2(MDRequestRef& mdr, int r)
{
  inodeno_t ino = mdr->client_request->get_filepath().get_ino();
  dout(10) << "_lookup_ino_2 " << mdr.get() << " ino " << ino << " r=" << r << dendl;

  // `r` is a rank if >=0, else an error code
  if (r >= 0) {
    mds_rank_t dest_rank(r);
    if (dest_rank == mds->get_nodeid())
      dispatch_client_request(mdr);
    else
      mdcache->request_forward(mdr, dest_rank);
    return;
  }

  // give up
  if (r == -ENOENT || r == -ENODATA)
    r = -ESTALE;
  respond_to_request(mdr, r);
}


/* This function takes responsibility for the passed mdr*/
void Server::handle_client_open(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  dout(7) << "open on " << req->get_filepath() << dendl;

  int flags = req->head.args.open.flags;
  int cmode = ceph_flags_to_mode(flags);
  if (cmode < 0) {
    respond_to_request(mdr, -EINVAL);
    return;
  }
  
  bool need_auth = !file_mode_is_readonly(cmode) ||
		   (flags & (CEPH_O_TRUNC | CEPH_O_DIRECTORY));

  if ((cmode & CEPH_FILE_MODE_WR) && mdcache->is_readonly()) {
    dout(7) << "read-only FS" << dendl;
    respond_to_request(mdr, -EROFS);
    return;
  }
  
  CInode *cur = rdlock_path_pin_ref(mdr, need_auth);
  if (!cur)
    return;

  if (cur->is_frozen() || cur->state_test(CInode::STATE_EXPORTINGCAPS)) {
    ceph_assert(!need_auth);
    mdr->locking_state &= ~(MutationImpl::PATH_LOCKED | MutationImpl::ALL_LOCKED);
    CInode *cur = rdlock_path_pin_ref(mdr, true);
    if (!cur)
      return;
  }

  if (!cur->inode.is_file()) {
    // can only open non-regular inode with mode FILE_MODE_PIN, at least for now.
    cmode = CEPH_FILE_MODE_PIN;
    // the inode is symlink and client wants to follow it, ignore the O_TRUNC flag.
    if (cur->inode.is_symlink() && !(flags & CEPH_O_NOFOLLOW))
      flags &= ~CEPH_O_TRUNC;
  }

  dout(10) << "open flags = " << flags
	   << ", filemode = " << cmode
	   << ", need_auth = " << need_auth
	   << dendl;
  
  // regular file?
  /*if (!cur->inode.is_file() && !cur->inode.is_dir()) {
    dout(7) << "not a file or dir " << *cur << dendl;
    respond_to_request(mdr, -ENXIO);                 // FIXME what error do we want?
    return;
    }*/
  if ((flags & CEPH_O_DIRECTORY) && !cur->inode.is_dir() && !cur->inode.is_symlink()) {
    dout(7) << "specified O_DIRECTORY on non-directory " << *cur << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }

  if ((flags & CEPH_O_TRUNC) && !cur->inode.is_file()) {
    dout(7) << "specified O_TRUNC on !(file|symlink) " << *cur << dendl;
    // we should return -EISDIR for directory, return -EINVAL for other non-regular
    respond_to_request(mdr, cur->inode.is_dir() ? -EISDIR : -EINVAL);
    return;
  }

  if (cur->inode.inline_data.version != CEPH_INLINE_NONE &&
      !mdr->session->get_connection()->has_feature(CEPH_FEATURE_MDS_INLINE_DATA)) {
    dout(7) << "old client cannot open inline data file " << *cur << dendl;
    respond_to_request(mdr, -EPERM);
    return;
  }
  
  // snapped data is read only
  if (mdr->snapid != CEPH_NOSNAP &&
      ((cmode & CEPH_FILE_MODE_WR) || req->may_write())) {
    dout(7) << "snap " << mdr->snapid << " is read-only " << *cur << dendl;
    respond_to_request(mdr, -EROFS);
    return;
  }

  MutationImpl::LockOpVec lov;

  unsigned mask = req->head.args.open.mask;
  if (mask) {
    Capability *cap = cur->get_client_cap(mdr->get_client());
    int issued = 0;
    if (cap && (mdr->snapid == CEPH_NOSNAP || mdr->snapid <= cap->client_follows))
      issued = cap->issued();
    // permission bits, ACL/security xattrs
    if ((mask & CEPH_CAP_AUTH_SHARED) && (issued & CEPH_CAP_AUTH_EXCL) == 0)
      lov.add_rdlock(&cur->authlock);
    if ((mask & CEPH_CAP_XATTR_SHARED) && (issued & CEPH_CAP_XATTR_EXCL) == 0)
      lov.add_rdlock(&cur->xattrlock);

    mdr->getattr_caps = mask;
  }

  // O_TRUNC
  if ((flags & CEPH_O_TRUNC) && !mdr->has_completed) {
    ceph_assert(cur->is_auth());

    lov.add_xlock(&cur->filelock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;

    if (!check_access(mdr, cur, MAY_WRITE))
      return;

    // wait for pending truncate?
    const auto pi = cur->get_projected_inode();
    if (pi->is_truncating()) {
      dout(10) << " waiting for pending truncate from " << pi->truncate_from
	       << " to " << pi->truncate_size << " to complete on " << *cur << dendl;
      mds->locker->drop_locks(mdr.get());
      mdr->drop_local_auth_pins();
      cur->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
    
    do_open_truncate(mdr, cmode);
    return;
  }

  // sync filelock if snapped.
  //  this makes us wait for writers to flushsnaps, ensuring we get accurate metadata,
  //  and that data itself is flushed so that we can read the snapped data off disk.
  if (mdr->snapid != CEPH_NOSNAP && !cur->is_dir()) {
    lov.add_rdlock(&cur->filelock);
  }

  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  mask = MAY_READ;
  if (cmode & CEPH_FILE_MODE_WR)
    mask |= MAY_WRITE;
  if (!check_access(mdr, cur, mask))
    return;

  utime_t now = ceph_clock_now();
  mdr->set_mds_stamp(now);

  if (cur->is_file() || cur->is_dir()) {
    if (mdr->snapid == CEPH_NOSNAP) {
      // register new cap
      Capability *cap = mds->locker->issue_new_caps(cur, cmode, mdr, nullptr);
      if (cap)
	dout(12) << "open issued caps " << ccap_string(cap->pending())
		 << " for " << req->get_source()
		 << " on " << *cur << dendl;
    } else {
      int caps = ceph_caps_for_mode(cmode);
      dout(12) << "open issued IMMUTABLE SNAP caps " << ccap_string(caps)
	       << " for " << req->get_source()
	       << " snapid " << mdr->snapid
	       << " on " << *cur << dendl;
      mdr->snap_caps = caps;
    }
  }

  // increase max_size?
  if (cmode & CEPH_FILE_MODE_WR)
    mds->locker->check_inode_max_size(cur);

  // make sure this inode gets into the journal
  if (cur->is_auth() && cur->last == CEPH_NOSNAP &&
      mdcache->open_file_table.should_log_open(cur)) {
    EOpen *le = new EOpen(mds->mdlog);
    mdlog->start_entry(le);
    le->add_clean_inode(cur);
    mdlog->submit_entry(le);
  }
  
  // hit pop
  if (cmode & CEPH_FILE_MODE_WR)
    mds->balancer->hit_inode(cur, META_POP_IWR);
  else
    mds->balancer->hit_inode(cur, META_POP_IRD,
			     mdr->client_request->get_source().num());

  CDentry *dn = 0;
  if (req->get_dentry_wanted()) {
    ceph_assert(mdr->dn[0].size());
    dn = mdr->dn[0].back();
  }

  mdr->tracei = cur;
  mdr->tracedn = dn;
  respond_to_request(mdr, 0);
}

class C_MDS_openc_finish : public ServerLogContext { 
  CDentry *dn;
  CInode *newi;
public:
  C_MDS_openc_finish(Server *s, MDRequestRef& r, CDentry *d, CInode *ni) :
    ServerLogContext(s, r), dn(d), newi(ni) {}
  void finish(int r) override {
    ceph_assert(r == 0);

    dn->pop_projected_linkage();

    // dirty inode, dn, dir
    newi->inode.version--;   // a bit hacky, see C_MDS_mknod_finish
    newi->mark_dirty(newi->inode.version+1, mdr->ls);
    newi->mark_dirty_parent(mdr->ls, true);

    mdr->apply();

    get_mds()->locker->share_inode_max_size(newi);

    MDRequestRef null_ref;
    get_mds()->mdcache->send_dentry_link(dn, null_ref);

    get_mds()->balancer->hit_inode(newi, META_POP_IWR);

    server->respond_to_request(mdr, 0);

    ceph_assert(g_conf()->mds_kill_openc_at != 1);
  }
};

/* This function takes responsibility for the passed mdr*/
void Server::handle_client_openc(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  client_t client = mdr->get_client();

  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << dendl;

  int cmode = ceph_flags_to_mode(req->head.args.open.flags);
  if (cmode < 0) {
    respond_to_request(mdr, -EINVAL);
    return;
  }

  bool excl = req->head.args.open.flags & CEPH_O_EXCL;
  CDentry *dn = rdlock_path_xlock_dentry(mdr, true, !excl, true);
  if (!dn)
    return;

  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  if (!excl && !dnl->is_null()) {
    // it existed.
    mds->locker->xlock_downgrade(&dn->lock, mdr.get());

    MutationImpl::LockOpVec lov;
    lov.add_rdlock(&dnl->get_inode()->snaplock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;

    handle_client_open(mdr);
    return;
  }

  ceph_assert(dnl->is_null());

  // set layout
  file_layout_t layout;
  if (mdr->dir_layout != file_layout_t())
    layout = mdr->dir_layout;
  else
    layout = mdcache->default_file_layout;

  // What kind of client caps are required to complete this operation
  uint64_t access = MAY_WRITE;

  const auto default_layout = layout;

  // fill in any special params from client
  if (req->head.args.open.stripe_unit)
    layout.stripe_unit = req->head.args.open.stripe_unit;
  if (req->head.args.open.stripe_count)
    layout.stripe_count = req->head.args.open.stripe_count;
  if (req->head.args.open.object_size)
    layout.object_size = req->head.args.open.object_size;
  if (req->get_connection()->has_feature(CEPH_FEATURE_CREATEPOOLID) &&
      (__s32)req->head.args.open.pool >= 0) {
    layout.pool_id = req->head.args.open.pool;

    // make sure we have as new a map as the client
    if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
      mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // If client doesn't have capability to modify layout pools, then
  // only permit this request if the requested pool matches what the
  // file would have inherited anyway from its parent.
  if (default_layout != layout) {
    access |= MAY_SET_VXATTR;
  }

  if (!layout.is_valid()) {
    dout(10) << " invalid initial file layout" << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }
  if (!mds->mdsmap->is_data_pool(layout.pool_id)) {
    dout(10) << " invalid data pool " << layout.pool_id << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }

  // created null dn.
  CDir *dir = dn->get_dir();
  CInode *diri = dir->get_inode();
  if (!check_access(mdr, diri, access))
    return;
  if (!check_fragment_space(mdr, dir))
    return;

  if (mdr->dn[0].size() == 1)
    mds->locker->create_lock_cache(mdr, diri, &mdr->dir_layout);

  // create inode.
  CInode *in = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino),
				 req->head.args.open.mode | S_IFREG, &layout);
  ceph_assert(in);

  // it's a file.
  dn->push_projected_linkage(in);

  in->inode.version = dn->pre_dirty();
  if (layout.pool_id != mdcache->default_file_layout.pool_id)
    in->inode.add_old_pool(mdcache->default_file_layout.pool_id);
  in->inode.update_backtrace();
  in->inode.rstat.rfiles = 1;

  SnapRealm *realm = diri->find_snaprealm();
  snapid_t follows = mdcache->get_global_snaprealm()->get_newest_seq();
  ceph_assert(follows >= realm->get_newest_seq());

  ceph_assert(dn->first == follows+1);
  in->first = dn->first;

  // do the open
  Capability *cap = mds->locker->issue_new_caps(in, cmode, mdr, realm);
  in->authlock.set_state(LOCK_EXCL);
  in->xattrlock.set_state(LOCK_EXCL);

  if (cap && (cmode & CEPH_FILE_MODE_WR)) {
    in->inode.client_ranges[client].range.first = 0;
    in->inode.client_ranges[client].range.last = in->inode.layout.stripe_unit;
    in->inode.client_ranges[client].follows = follows;
    cap->mark_clientwriteable();
  }
  
  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "openc");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);
  mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, in, true, true, true);

  // make sure this inode gets into the journal
  le->metablob.add_opened_ino(in->ino());

  C_MDS_openc_finish *fin = new C_MDS_openc_finish(this, mdr, dn, in);

  if (mdr->session->info.has_feature(CEPHFS_FEATURE_DELEG_INO)) {
    openc_response_t	ocresp;

    dout(10) << "adding created_ino and delegated_inos" << dendl;
    ocresp.created_ino = in->inode.ino;

    if (delegate_inos_pct && !req->is_queued_for_replay()) {
      // Try to delegate some prealloc_inos to the client, if it's down to half the max
      unsigned frac = 100 / delegate_inos_pct;
      if (mdr->session->delegated_inos.size() < (unsigned)g_conf()->mds_client_prealloc_inos / frac / 2)
	mdr->session->delegate_inos(g_conf()->mds_client_prealloc_inos / frac, ocresp.delegated_inos);
    }

    encode(ocresp, mdr->reply_extra_bl);
  } else if (mdr->client_request->get_connection()->has_feature(CEPH_FEATURE_REPLY_CREATE_INODE)) {
    dout(10) << "adding ino to reply to indicate inode was created" << dendl;
    // add the file created flag onto the reply if create_flags features is supported
    encode(in->inode.ino, mdr->reply_extra_bl);
  }

  journal_and_reply(mdr, in, dn, le, fin);

  // We hit_dir (via hit_inode) in our finish callback, but by then we might
  // have overshot the split size (multiple opencs in flight), so here is
  // an early chance to split the dir if this openc makes it oversized.
  mds->balancer->maybe_fragment(dir, false);
}



void Server::handle_client_readdir(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  client_t client = req->get_source().num();
  MutationImpl::LockOpVec lov;
  CInode *diri = rdlock_path_pin_ref(mdr, false, true);
  if (!diri) return;

  // it's a directory, right?
  if (!diri->is_dir()) {
    // not a dir
    dout(10) << "reply to " << *req << " readdir -ENOTDIR" << dendl;
    respond_to_request(mdr, -ENOTDIR);
    return;
  }

  lov.add_rdlock(&diri->filelock);
  lov.add_rdlock(&diri->dirfragtreelock);

  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  if (!check_access(mdr, diri, MAY_READ))
    return;

  // which frag?
  frag_t fg = (__u32)req->head.args.readdir.frag;
  unsigned req_flags = (__u32)req->head.args.readdir.flags;
  string offset_str = req->get_path2();

  __u32 offset_hash = 0;
  if (!offset_str.empty())
    offset_hash = ceph_frag_value(diri->hash_dentry_name(offset_str));
  else
    offset_hash = (__u32)req->head.args.readdir.offset_hash;

  dout(10) << " frag " << fg << " offset '" << offset_str << "'"
	   << " offset_hash " << offset_hash << " flags " << req_flags << dendl;

  // does the frag exist?
  if (diri->dirfragtree[fg.value()] != fg) {
    frag_t newfg;
    if (req_flags & CEPH_READDIR_REPLY_BITFLAGS) {
      if (fg.contains((unsigned)offset_hash)) {
	newfg = diri->dirfragtree[offset_hash];
      } else {
	// client actually wants next frag
	newfg = diri->dirfragtree[fg.value()];
      }
    } else {
      offset_str.clear();
      newfg = diri->dirfragtree[fg.value()];
    }
    dout(10) << " adjust frag " << fg << " -> " << newfg << " " << diri->dirfragtree << dendl;
    fg = newfg;
  }
  
  CDir *dir = try_open_auth_dirfrag(diri, fg, mdr);
  if (!dir) return;

  // ok!
  dout(10) << "handle_client_readdir on " << *dir << dendl;
  ceph_assert(dir->is_auth());

  if (!dir->is_complete()) {
    if (dir->is_frozen()) {
      dout(7) << "dir is frozen " << *dir << dendl;
      mds->locker->drop_locks(mdr.get());
      mdr->drop_local_auth_pins();
      dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
    // fetch
    dout(10) << " incomplete dir contents for readdir on " << *dir << ", fetching" << dendl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr), true);
    return;
  }

#ifdef MDS_VERIFY_FRAGSTAT
  dir->verify_fragstat();
#endif

  utime_t now = ceph_clock_now();
  mdr->set_mds_stamp(now);

  snapid_t snapid = mdr->snapid;
  dout(10) << "snapid " << snapid << dendl;

  SnapRealm *realm = diri->find_snaprealm();

  unsigned max = req->head.args.readdir.max_entries;
  if (!max)
    max = dir->get_num_any();  // whatever, something big.
  unsigned max_bytes = req->head.args.readdir.max_bytes;
  if (!max_bytes)
    // make sure at least one item can be encoded
    max_bytes = (512 << 10) + g_conf()->mds_max_xattr_pairs_size;

  // start final blob
  bufferlist dirbl;
  DirStat ds;
  ds.frag = dir->get_frag();
  ds.auth = dir->get_dir_auth().first;
  if (dir->is_auth() && !mdcache->forward_all_reqs_to_auth())
    dir->get_dist_spec(ds.dist, mds->get_nodeid());

  dir->encode_dirstat(dirbl, mdr->session->info, ds);

  // count bytes available.
  //  this isn't perfect, but we should capture the main variable/unbounded size items!
  int front_bytes = dirbl.length() + sizeof(__u32) + sizeof(__u8)*2;
  int bytes_left = max_bytes - front_bytes;
  bytes_left -= realm->get_snap_trace().length();

  // build dir contents
  bufferlist dnbl;
  __u32 numfiles = 0;
  bool start = !offset_hash && offset_str.empty();
  // skip all dns < dentry_key_t(snapid, offset_str, offset_hash)
  dentry_key_t skip_key(snapid, offset_str.c_str(), offset_hash);
  auto it = start ? dir->begin() : dir->lower_bound(skip_key);
  bool end = (it == dir->end());
  for (; !end && numfiles < max; end = (it == dir->end())) {
    CDentry *dn = it->second;
    ++it;

    if (dn->state_test(CDentry::STATE_PURGING))
      continue;

    bool dnp = dn->use_projected(client, mdr);
    CDentry::linkage_t *dnl = dnp ? dn->get_projected_linkage() : dn->get_linkage();

    if (dnl->is_null())
      continue;

    if (dn->last < snapid || dn->first > snapid) {
      dout(20) << "skipping non-overlapping snap " << *dn << dendl;
      continue;
    }

    if (!start) {
      dentry_key_t offset_key(dn->last, offset_str.c_str(), offset_hash);
      if (!(offset_key < dn->key()))
	continue;
    }

    CInode *in = dnl->get_inode();

    if (in && in->ino() == CEPH_INO_CEPH)
      continue;

    // remote link?
    // better for the MDS to do the work, if we think the client will stat any of these files.
    if (dnl->is_remote() && !in) {
      in = mdcache->get_inode(dnl->get_remote_ino());
      if (in) {
	dn->link_remote(dnl, in);
      } else if (dn->state_test(CDentry::STATE_BADREMOTEINO)) {
	dout(10) << "skipping bad remote ino on " << *dn << dendl;
	continue;
      } else {
	// touch everything i _do_ have
	for (auto &p : *dir) {
	  if (!p.second->get_linkage()->is_null())
	    mdcache->lru.lru_touch(p.second);
        }

	// already issued caps and leases, reply immediately.
	if (dnbl.length() > 0) {
	  mdcache->open_remote_dentry(dn, dnp, new C_MDSInternalNoop);
	  dout(10) << " open remote dentry after caps were issued, stopping at "
		   << dnbl.length() << " < " << bytes_left << dendl;
	  break;
	}

	mds->locker->drop_locks(mdr.get());
	mdr->drop_local_auth_pins();
	mdcache->open_remote_dentry(dn, dnp, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }
    }
    ceph_assert(in);

    if ((int)(dnbl.length() + dn->get_name().length() + sizeof(__u32) + sizeof(LeaseStat)) > bytes_left) {
      dout(10) << " ran out of room, stopping at " << dnbl.length() << " < " << bytes_left << dendl;
      break;
    }
    
    unsigned start_len = dnbl.length();

    // dentry
    dout(12) << "including    dn " << *dn << dendl;
    encode(dn->get_name(), dnbl);
    int lease_mask = dnl->is_primary() ? CEPH_LEASE_PRIMARY_LINK : 0;
    mds->locker->issue_client_lease(dn, mdr, lease_mask, now, dnbl);

    // inode
    dout(12) << "including inode " << *in << dendl;
    int r = in->encode_inodestat(dnbl, mdr->session, realm, snapid, bytes_left - (int)dnbl.length());
    if (r < 0) {
      // chop off dn->name, lease
      dout(10) << " ran out of room, stopping at " << start_len << " < " << bytes_left << dendl;
      bufferlist keep;
      keep.substr_of(dnbl, 0, start_len);
      dnbl.swap(keep);
      break;
    }
    ceph_assert(r >= 0);
    numfiles++;

    // touch dn
    mdcache->lru.lru_touch(dn);
  }
  
  __u16 flags = 0;
  if (end) {
    flags = CEPH_READDIR_FRAG_END;
    if (start)
      flags |= CEPH_READDIR_FRAG_COMPLETE; // FIXME: what purpose does this serve
  }
  // client only understand END and COMPLETE flags ?
  if (req_flags & CEPH_READDIR_REPLY_BITFLAGS) {
    flags |= CEPH_READDIR_HASH_ORDER | CEPH_READDIR_OFFSET_HASH;
  }
  
  // finish final blob
  encode(numfiles, dirbl);
  encode(flags, dirbl);
  dirbl.claim_append(dnbl);
  
  // yay, reply
  dout(10) << "reply to " << *req << " readdir num=" << numfiles
	   << " bytes=" << dirbl.length()
	   << " start=" << (int)start
	   << " end=" << (int)end
	   << dendl;
  mdr->reply_extra_bl = dirbl;

  // bump popularity.  NOTE: this doesn't quite capture it.
  mds->balancer->hit_dir(dir, META_POP_IRD, -1, numfiles);
  
  // reply
  mdr->tracei = diri;
  respond_to_request(mdr, 0);
}



// ===============================================================================
// INODE UPDATES


/* 
 * finisher for basic inode updates
 */
class C_MDS_inode_update_finish : public ServerLogContext {
  CInode *in;
  bool truncating_smaller, changed_ranges, new_realm;
public:
  C_MDS_inode_update_finish(Server *s, MDRequestRef& r, CInode *i,
			    bool sm=false, bool cr=false, bool nr=false) :
    ServerLogContext(s, r), in(i),
    truncating_smaller(sm), changed_ranges(cr), new_realm(nr) { }
  void finish(int r) override {
    ceph_assert(r == 0);

    // apply
    in->pop_and_dirty_projected_inode(mdr->ls);
    mdr->apply();

    MDSRank *mds = get_mds();

    // notify any clients
    if (truncating_smaller && in->inode.is_truncating()) {
      mds->locker->issue_truncate(in);
      mds->mdcache->truncate_inode(in, mdr->ls);
    }

    if (new_realm) {
      int op = CEPH_SNAP_OP_SPLIT;
      mds->mdcache->send_snap_update(in, 0, op);
      mds->mdcache->do_realm_invalidate_and_update_notify(in, op);
    }

    get_mds()->balancer->hit_inode(in, META_POP_IWR);

    server->respond_to_request(mdr, 0);

    if (changed_ranges)
      get_mds()->locker->share_inode_max_size(in);
  }
};

void Server::handle_client_file_setlock(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  MutationImpl::LockOpVec lov;

  // get the inode to operate on, and set up any locks needed for that
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur)
    return;

  lov.add_xlock(&cur->flocklock);
  /* acquire_locks will return true if it gets the locks. If it fails,
     it will redeliver this request at a later date, so drop the request.
   */
  if (!mds->locker->acquire_locks(mdr, lov)) {
    dout(10) << "handle_client_file_setlock could not get locks!" << dendl;
    return;
  }

  // copy the lock change into a ceph_filelock so we can store/apply it
  ceph_filelock set_lock;
  set_lock.start = req->head.args.filelock_change.start;
  set_lock.length = req->head.args.filelock_change.length;
  set_lock.client = req->get_orig_source().num();
  set_lock.owner = req->head.args.filelock_change.owner;
  set_lock.pid = req->head.args.filelock_change.pid;
  set_lock.type = req->head.args.filelock_change.type;
  bool will_wait = req->head.args.filelock_change.wait;

  dout(10) << "handle_client_file_setlock: " << set_lock << dendl;

  ceph_lock_state_t *lock_state = NULL;
  bool interrupt = false;

  // get the appropriate lock state
  switch (req->head.args.filelock_change.rule) {
  case CEPH_LOCK_FLOCK_INTR:
    interrupt = true;
    // fall-thru
  case CEPH_LOCK_FLOCK:
    lock_state = cur->get_flock_lock_state();
    break;

  case CEPH_LOCK_FCNTL_INTR:
    interrupt = true;
    // fall-thru
  case CEPH_LOCK_FCNTL:
    lock_state = cur->get_fcntl_lock_state();
    break;

  default:
    dout(10) << "got unknown lock type " << set_lock.type
	     << ", dropping request!" << dendl;
    respond_to_request(mdr, -EOPNOTSUPP);
    return;
  }

  dout(10) << " state prior to lock change: " << *lock_state << dendl;
  if (CEPH_LOCK_UNLOCK == set_lock.type) {
    list<ceph_filelock> activated_locks;
    MDSContext::vec waiters;
    if (lock_state->is_waiting(set_lock)) {
      dout(10) << " unlock removing waiting lock " << set_lock << dendl;
      lock_state->remove_waiting(set_lock);
      cur->take_waiting(CInode::WAIT_FLOCK, waiters);
    } else if (!interrupt) {
      dout(10) << " unlock attempt on " << set_lock << dendl;
      lock_state->remove_lock(set_lock, activated_locks);
      cur->take_waiting(CInode::WAIT_FLOCK, waiters);
    }
    mds->queue_waiters(waiters);

    respond_to_request(mdr, 0);
  } else {
    dout(10) << " lock attempt on " << set_lock << dendl;
    bool deadlock = false;
    if (mdr->more()->flock_was_waiting &&
	!lock_state->is_waiting(set_lock)) {
      dout(10) << " was waiting for lock but not anymore, must have been canceled " << set_lock << dendl;
      respond_to_request(mdr, -EINTR);
    } else if (!lock_state->add_lock(set_lock, will_wait, mdr->more()->flock_was_waiting, &deadlock)) {
      dout(10) << " it failed on this attempt" << dendl;
      // couldn't set lock right now
      if (deadlock) {
	respond_to_request(mdr, -EDEADLK);
      } else if (!will_wait) {
	respond_to_request(mdr, -EWOULDBLOCK);
      } else {
	dout(10) << " added to waiting list" << dendl;
	ceph_assert(lock_state->is_waiting(set_lock));
	mdr->more()->flock_was_waiting = true;
	mds->locker->drop_locks(mdr.get());
	mdr->drop_local_auth_pins();
	mdr->mark_event("failed to add lock, waiting");
	mdr->mark_nowarn();
	cur->add_waiter(CInode::WAIT_FLOCK, new C_MDS_RetryRequest(mdcache, mdr));
      }
    } else
      respond_to_request(mdr, 0);
  }
  dout(10) << " state after lock change: " << *lock_state << dendl;
}

void Server::handle_client_file_readlock(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  MutationImpl::LockOpVec lov;

  // get the inode to operate on, and set up any locks needed for that
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur)
    return;

  /* acquire_locks will return true if it gets the locks. If it fails,
     it will redeliver this request at a later date, so drop the request.
  */
  lov.add_rdlock(&cur->flocklock);
  if (!mds->locker->acquire_locks(mdr, lov)) {
    dout(10) << "handle_client_file_readlock could not get locks!" << dendl;
    return;
  }
  
  // copy the lock change into a ceph_filelock so we can store/apply it
  ceph_filelock checking_lock;
  checking_lock.start = req->head.args.filelock_change.start;
  checking_lock.length = req->head.args.filelock_change.length;
  checking_lock.client = req->get_orig_source().num();
  checking_lock.owner = req->head.args.filelock_change.owner;
  checking_lock.pid = req->head.args.filelock_change.pid;
  checking_lock.type = req->head.args.filelock_change.type;

  // get the appropriate lock state
  ceph_lock_state_t *lock_state = NULL;
  switch (req->head.args.filelock_change.rule) {
  case CEPH_LOCK_FLOCK:
    lock_state = cur->get_flock_lock_state();
    break;

  case CEPH_LOCK_FCNTL:
    lock_state = cur->get_fcntl_lock_state();
    break;

  default:
    dout(10) << "got unknown lock type " << checking_lock.type << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }
  lock_state->look_for_lock(checking_lock);

  bufferlist lock_bl;
  encode(checking_lock, lock_bl);

  mdr->reply_extra_bl = lock_bl;
  respond_to_request(mdr, 0);
}

void Server::handle_client_setattr(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  MutationImpl::LockOpVec lov;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  if (mdr->snapid != CEPH_NOSNAP) {
    respond_to_request(mdr, -EROFS);
    return;
  }
  if (cur->ino() < MDS_INO_SYSTEM_BASE && !cur->is_base()) {
    respond_to_request(mdr, -EPERM);
    return;
  }

  __u32 mask = req->head.args.setattr.mask;
  __u32 access_mask = MAY_WRITE;

  // xlock inode
  if (mask & (CEPH_SETATTR_MODE|CEPH_SETATTR_UID|CEPH_SETATTR_GID|CEPH_SETATTR_BTIME|CEPH_SETATTR_KILL_SGUID))
    lov.add_xlock(&cur->authlock);
  if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME|CEPH_SETATTR_SIZE))
    lov.add_xlock(&cur->filelock);
  if (mask & CEPH_SETATTR_CTIME)
    lov.add_wrlock(&cur->versionlock);

  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  if ((mask & CEPH_SETATTR_UID) && (cur->inode.uid != req->head.args.setattr.uid))
    access_mask |= MAY_CHOWN;

  if ((mask & CEPH_SETATTR_GID) && (cur->inode.gid != req->head.args.setattr.gid))
    access_mask |= MAY_CHGRP;

  if (!check_access(mdr, cur, access_mask))
    return;

  // trunc from bigger -> smaller?
  auto pip = cur->get_projected_inode();

  uint64_t old_size = std::max<uint64_t>(pip->size, req->head.args.setattr.old_size);

  // ENOSPC on growing file while full, but allow shrinks
  if (is_full && req->head.args.setattr.size > old_size) {
    dout(20) << __func__ << ": full, responding ENOSPC to setattr with larger size" << dendl;
    respond_to_request(mdr, -ENOSPC);
    return;
  }

  bool truncating_smaller = false;
  if (mask & CEPH_SETATTR_SIZE) {
    truncating_smaller = req->head.args.setattr.size < old_size;
    if (truncating_smaller && pip->is_truncating()) {
      dout(10) << " waiting for pending truncate from " << pip->truncate_from
	       << " to " << pip->truncate_size << " to complete on " << *cur << dendl;
      mds->locker->drop_locks(mdr.get());
      mdr->drop_local_auth_pins();
      cur->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  bool changed_ranges = false;

  // project update
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setattr");
  mdlog->start_entry(le);

  auto &pi = cur->project_inode();

  if (mask & CEPH_SETATTR_UID)
    pi.inode.uid = req->head.args.setattr.uid;
  if (mask & CEPH_SETATTR_GID)
    pi.inode.gid = req->head.args.setattr.gid;

  if (mask & CEPH_SETATTR_MODE)
    pi.inode.mode = (pi.inode.mode & ~07777) | (req->head.args.setattr.mode & 07777);
  else if ((mask & (CEPH_SETATTR_UID|CEPH_SETATTR_GID|CEPH_SETATTR_KILL_SGUID)) &&
	    S_ISREG(pi.inode.mode) &&
            (pi.inode.mode & (S_IXUSR|S_IXGRP|S_IXOTH))) {
    pi.inode.mode &= ~(S_ISUID|S_ISGID);
  }

  if (mask & CEPH_SETATTR_MTIME)
    pi.inode.mtime = req->head.args.setattr.mtime;
  if (mask & CEPH_SETATTR_ATIME)
    pi.inode.atime = req->head.args.setattr.atime;
  if (mask & CEPH_SETATTR_BTIME)
    pi.inode.btime = req->head.args.setattr.btime;
  if (mask & (CEPH_SETATTR_ATIME | CEPH_SETATTR_MTIME | CEPH_SETATTR_BTIME))
    pi.inode.time_warp_seq++;   // maybe not a timewarp, but still a serialization point.
  if (mask & CEPH_SETATTR_SIZE) {
    if (truncating_smaller) {
      pi.inode.truncate(old_size, req->head.args.setattr.size);
      le->metablob.add_truncate_start(cur->ino());
    } else {
      pi.inode.size = req->head.args.setattr.size;
      pi.inode.rstat.rbytes = pi.inode.size;
    }
    pi.inode.mtime = mdr->get_op_stamp();

    // adjust client's max_size?
    CInode::mempool_inode::client_range_map new_ranges;
    bool max_increased = false;
    mds->locker->calc_new_client_ranges(cur, pi.inode.size, true, &new_ranges, &max_increased);
    if (pi.inode.client_ranges != new_ranges) {
      dout(10) << " client_ranges " << pi.inode.client_ranges << " -> " << new_ranges << dendl;
      pi.inode.client_ranges = new_ranges;
      changed_ranges = true;
    }
  }

  pi.inode.version = cur->pre_dirty();
  pi.inode.ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode.rstat.rctime)
    pi.inode.rstat.rctime = mdr->get_op_stamp();
  pi.inode.change_attr++;

  // log + wait
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, cur);
  
  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(this, mdr, cur,
								   truncating_smaller, changed_ranges));

  // flush immediately if there are readers/writers waiting
  if (mdr->is_xlocked(&cur->filelock) &&
      (cur->get_caps_wanted() & (CEPH_CAP_FILE_RD|CEPH_CAP_FILE_WR)))
    mds->mdlog->flush();
}

/* Takes responsibility for mdr */
void Server::do_open_truncate(MDRequestRef& mdr, int cmode)
{
  CInode *in = mdr->in[0];
  client_t client = mdr->get_client();
  ceph_assert(in);

  dout(10) << "do_open_truncate " << *in << dendl;

  SnapRealm *realm = in->find_snaprealm();
  Capability *cap = mds->locker->issue_new_caps(in, cmode, mdr, realm);

  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "open_truncate");
  mdlog->start_entry(le);

  // prepare
  auto &pi = in->project_inode();
  pi.inode.version = in->pre_dirty();
  pi.inode.mtime = pi.inode.ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode.rstat.rctime)
    pi.inode.rstat.rctime = mdr->get_op_stamp();
  pi.inode.change_attr++;

  uint64_t old_size = std::max<uint64_t>(pi.inode.size, mdr->client_request->head.args.open.old_size);
  if (old_size > 0) {
    pi.inode.truncate(old_size, 0);
    le->metablob.add_truncate_start(in->ino());
  }

  bool changed_ranges = false;
  if (cap && (cmode & CEPH_FILE_MODE_WR)) {
    pi.inode.client_ranges[client].range.first = 0;
    pi.inode.client_ranges[client].range.last = pi.inode.get_layout_size_increment();
    pi.inode.client_ranges[client].follows = realm->get_newest_seq();
    changed_ranges = true;
    cap->mark_clientwriteable();
  }
  
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());

  mdcache->predirty_journal_parents(mdr, &le->metablob, in, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, in);
  
  // make sure ino gets into the journal
  le->metablob.add_opened_ino(in->ino());
  
  mdr->o_trunc = true;

  CDentry *dn = 0;
  if (mdr->client_request->get_dentry_wanted()) {
    ceph_assert(mdr->dn[0].size());
    dn = mdr->dn[0].back();
  }

  journal_and_reply(mdr, in, dn, le, new C_MDS_inode_update_finish(this, mdr, in, old_size > 0,
								   changed_ranges));
  // Although the `open` part can give an early reply, the truncation won't
  // happen until our EUpdate is persistent, to give the client a prompt
  // response we must also flush that event.
  mdlog->flush();
}


/* This function cleans up the passed mdr */
void Server::handle_client_setlayout(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  if (mdr->snapid != CEPH_NOSNAP) {
    respond_to_request(mdr, -EROFS);
    return;
  }
  if (!cur->is_file()) {
    respond_to_request(mdr, -EINVAL);
    return;
  }
  if (cur->get_projected_inode()->size ||
      cur->get_projected_inode()->truncate_seq > 1) {
    respond_to_request(mdr, -ENOTEMPTY);
    return;
  }

  // validate layout
  file_layout_t layout = cur->get_projected_inode()->layout;
  // save existing layout for later
  const auto old_layout = layout;

  int access = MAY_WRITE;

  if (req->head.args.setlayout.layout.fl_object_size > 0)
    layout.object_size = req->head.args.setlayout.layout.fl_object_size;
  if (req->head.args.setlayout.layout.fl_stripe_unit > 0)
    layout.stripe_unit = req->head.args.setlayout.layout.fl_stripe_unit;
  if (req->head.args.setlayout.layout.fl_stripe_count > 0)
    layout.stripe_count=req->head.args.setlayout.layout.fl_stripe_count;
  if (req->head.args.setlayout.layout.fl_pg_pool > 0) {
    layout.pool_id = req->head.args.setlayout.layout.fl_pg_pool;

    // make sure we have as new a map as the client
    if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
      mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // Don't permit layout modifications without 'p' caps
  if (layout != old_layout) {
    access |= MAY_SET_VXATTR;
  }

  if (!layout.is_valid()) {
    dout(10) << "bad layout" << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }
  if (!mds->mdsmap->is_data_pool(layout.pool_id)) {
    dout(10) << " invalid data pool " << layout.pool_id << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }

  MutationImpl::LockOpVec lov;
  lov.add_xlock(&cur->filelock);
  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  if (!check_access(mdr, cur, access))
    return;

  // project update
  auto &pi = cur->project_inode();
  pi.inode.layout = layout;
  // add the old pool to the inode
  pi.inode.add_old_pool(old_layout.pool_id);
  pi.inode.version = cur->pre_dirty();
  pi.inode.ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode.rstat.rctime)
    pi.inode.rstat.rctime = mdr->get_op_stamp();
  pi.inode.change_attr++;
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setlayout");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, cur);
  
  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(this, mdr, cur));
}

bool Server::xlock_policylock(MDRequestRef& mdr, CInode *in, bool want_layout, bool xlock_snaplock)
{
  if (mdr->locking_state & MutationImpl::ALL_LOCKED)
    return true;

  MutationImpl::LockOpVec lov;
  lov.add_xlock(&in->policylock);
  if (xlock_snaplock)
    lov.add_xlock(&in->snaplock);
  else
    lov.add_rdlock(&in->snaplock);
  if (!mds->locker->acquire_locks(mdr, lov))
    return false;

  if (want_layout && in->get_projected_inode()->has_layout()) {
    mdr->dir_layout = in->get_projected_inode()->layout;
    want_layout = false;
  }
  if (CDentry *pdn = in->get_projected_parent_dn(); pdn) {
    if (!mds->locker->try_rdlock_snap_layout(pdn->get_dir()->get_inode(), mdr, 0, want_layout))
      return false;
  }

  mdr->locking_state |= MutationImpl::ALL_LOCKED;
  return true;
}

CInode* Server::try_get_auth_inode(MDRequestRef& mdr, inodeno_t ino)
{
  CInode *in = mdcache->get_inode(ino);
  if (!in || in->state_test(CInode::STATE_PURGING)) {
    respond_to_request(mdr, -ESTALE);
    return nullptr;
  }
  if (!in->is_auth()) {
    mdcache->request_forward(mdr, in->authority().first);
    return nullptr;
  }

  return in;
}

void Server::handle_client_setdirlayout(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;

  // can't use rdlock_path_pin_ref because we need to xlock snaplock/policylock
  CInode *cur = try_get_auth_inode(mdr, req->get_filepath().get_ino());
  if (!cur)
    return;

  if (!cur->is_dir()) {
    respond_to_request(mdr, -ENOTDIR);
    return;
  }

  if (!xlock_policylock(mdr, cur, true))
    return;

  // validate layout
  const auto old_pi = cur->get_projected_inode();
  file_layout_t layout;
  if (old_pi->has_layout())
    layout = old_pi->layout;
  else if (mdr->dir_layout != file_layout_t())
    layout = mdr->dir_layout;
  else
    layout = mdcache->default_file_layout;

  // Level of access required to complete
  int access = MAY_WRITE;

  const auto old_layout = layout;

  if (req->head.args.setlayout.layout.fl_object_size > 0)
    layout.object_size = req->head.args.setlayout.layout.fl_object_size;
  if (req->head.args.setlayout.layout.fl_stripe_unit > 0)
    layout.stripe_unit = req->head.args.setlayout.layout.fl_stripe_unit;
  if (req->head.args.setlayout.layout.fl_stripe_count > 0)
    layout.stripe_count=req->head.args.setlayout.layout.fl_stripe_count;
  if (req->head.args.setlayout.layout.fl_pg_pool > 0) {
    layout.pool_id = req->head.args.setlayout.layout.fl_pg_pool;
    // make sure we have as new a map as the client
    if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
      mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }  
  }

  if (layout != old_layout) {
    access |= MAY_SET_VXATTR;
  }

  if (!layout.is_valid()) {
    dout(10) << "bad layout" << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }
  if (!mds->mdsmap->is_data_pool(layout.pool_id)) {
    dout(10) << " invalid data pool " << layout.pool_id << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }

  if (!check_access(mdr, cur, access))
    return;

  auto &pi = cur->project_inode();
  pi.inode.layout = layout;
  pi.inode.version = cur->pre_dirty();

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setlayout");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, cur);

  mdr->no_early_reply = true;
  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(this, mdr, cur));
}

// XATTRS

int Server::parse_layout_vxattr(string name, string value, const OSDMap& osdmap,
				file_layout_t *layout, bool validate)
{
  dout(20) << "parse_layout_vxattr name " << name << " value '" << value << "'" << dendl;
  try {
    if (name == "layout") {
      string::iterator begin = value.begin();
      string::iterator end = value.end();
      keys_and_values<string::iterator> p;    // create instance of parser
      std::map<string, string> m;             // map to receive results
      if (!qi::parse(begin, end, p, m)) {     // returns true if successful
	return -EINVAL;
      }
      string left(begin, end);
      dout(10) << " parsed " << m << " left '" << left << "'" << dendl;
      if (begin != end)
	return -EINVAL;
      for (map<string,string>::iterator q = m.begin(); q != m.end(); ++q) {
        // Skip validation on each attr, we do it once at the end (avoid
        // rejecting intermediate states if the overall result is ok)
	int r = parse_layout_vxattr(string("layout.") + q->first, q->second,
                                    osdmap, layout, false);
	if (r < 0)
	  return r;
      }
    } else if (name == "layout.object_size") {
      layout->object_size = boost::lexical_cast<unsigned>(value);
    } else if (name == "layout.stripe_unit") {
      layout->stripe_unit = boost::lexical_cast<unsigned>(value);
    } else if (name == "layout.stripe_count") {
      layout->stripe_count = boost::lexical_cast<unsigned>(value);
    } else if (name == "layout.pool") {
      try {
	layout->pool_id = boost::lexical_cast<unsigned>(value);
      } catch (boost::bad_lexical_cast const&) {
	int64_t pool = osdmap.lookup_pg_pool_name(value);
	if (pool < 0) {
	  dout(10) << " unknown pool " << value << dendl;
	  return -ENOENT;
	}
	layout->pool_id = pool;
      }
    } else if (name == "layout.pool_namespace") {
      layout->pool_ns = value;
    } else {
      dout(10) << " unknown layout vxattr " << name << dendl;
      return -EINVAL;
    }
  } catch (boost::bad_lexical_cast const&) {
    dout(10) << "bad vxattr value, unable to parse int for " << name << dendl;
    return -EINVAL;
  }

  if (validate && !layout->is_valid()) {
    dout(10) << "bad layout" << dendl;
    return -EINVAL;
  }
  if (!mds->mdsmap->is_data_pool(layout->pool_id)) {
    dout(10) << " invalid data pool " << layout->pool_id << dendl;
    return -EINVAL;
  }
  return 0;
}

int Server::parse_quota_vxattr(string name, string value, quota_info_t *quota)
{
  dout(20) << "parse_quota_vxattr name " << name << " value '" << value << "'" << dendl;
  try {
    if (name == "quota") {
      string::iterator begin = value.begin();
      string::iterator end = value.end();
      if (begin == end) {
	// keep quota unchanged. (for create_quota_realm())
	return 0;
      }
      keys_and_values<string::iterator> p;    // create instance of parser
      std::map<string, string> m;             // map to receive results
      if (!qi::parse(begin, end, p, m)) {     // returns true if successful
        return -EINVAL;
      }
      string left(begin, end);
      dout(10) << " parsed " << m << " left '" << left << "'" << dendl;
      if (begin != end)
        return -EINVAL;
      for (map<string,string>::iterator q = m.begin(); q != m.end(); ++q) {
        int r = parse_quota_vxattr(string("quota.") + q->first, q->second, quota);
        if (r < 0)
          return r;
      }
    } else if (name == "quota.max_bytes") {
      int64_t q = boost::lexical_cast<int64_t>(value);
      if (q < 0)
        return -EINVAL;
      quota->max_bytes = q;
    } else if (name == "quota.max_files") {
      int64_t q = boost::lexical_cast<int64_t>(value);
      if (q < 0)
        return -EINVAL;
      quota->max_files = q;
    } else {
      dout(10) << " unknown quota vxattr " << name << dendl;
      return -EINVAL;
    }
  } catch (boost::bad_lexical_cast const&) {
    dout(10) << "bad vxattr value, unable to parse int for " << name << dendl;
    return -EINVAL;
  }

  if (!quota->is_valid()) {
    dout(10) << "bad quota" << dendl;
    return -EINVAL;
  }
  return 0;
}

void Server::create_quota_realm(CInode *in)
{
  dout(10) << __func__ << " " << *in << dendl;

  auto req = make_message<MClientRequest>(CEPH_MDS_OP_SETXATTR);
  req->set_filepath(filepath(in->ino()));
  req->set_string2("ceph.quota");
  // empty vxattr value
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, in->authority().first);
}

/*
 * Verify that the file layout attribute carried by client
 * is well-formatted.
 * Return 0 on success, otherwise this function takes
 * responsibility for the passed mdr.
 */
int Server::check_layout_vxattr(MDRequestRef& mdr,
                                string name,
                                string value,
                                file_layout_t *layout)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  epoch_t epoch;
  int r;

  mds->objecter->with_osdmap([&](const OSDMap& osdmap) {
      r = parse_layout_vxattr(name, value, osdmap, layout);
      epoch = osdmap.get_epoch();
    });

  if (r == -ENOENT) {

    // we don't have the specified pool, make sure our map
    // is newer than or as new as the client.
    epoch_t req_epoch = req->get_osdmap_epoch();

    if (req_epoch > epoch) {

      // well, our map is older. consult mds.
      auto fin = new C_IO_Wrapper(mds, new C_MDS_RetryRequest(mdcache, mdr));

      mds->objecter->wait_for_map(req_epoch, lambdafy(fin));
      return r;
    } else if (req_epoch == 0 && !mdr->waited_for_osdmap) {

      // For compatibility with client w/ old code, we still need get the
      // latest map. One day if COMPACT_VERSION of MClientRequest >=3,
      // we can remove those code.
      mdr->waited_for_osdmap = true;
      mds->objecter->wait_for_latest_osdmap(std::ref(*new C_IO_Wrapper(
        mds, new C_MDS_RetryRequest(mdcache, mdr))));
      return r;
    }
  }

  if (r < 0) {

    if (r == -ENOENT)
      r = -EINVAL;

    respond_to_request(mdr, r);
    return r;
  }

  // all is well
  return 0;
}

void Server::handle_set_vxattr(MDRequestRef& mdr, CInode *cur)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  string name(req->get_path2());
  bufferlist bl = req->get_data();
  string value (bl.c_str(), bl.length());
  dout(10) << "handle_set_vxattr " << name
           << " val " << value.length()
           << " bytes on " << *cur
           << dendl;

  CInode::mempool_inode *pip = nullptr;
  string rest;

  if (!check_access(mdr, cur, MAY_SET_VXATTR)) {
    return;
  }

  bool new_realm = false;
  if (name.compare(0, 15, "ceph.dir.layout") == 0) {
    if (!cur->is_dir()) {
      respond_to_request(mdr, -EINVAL);
      return;
    }

    if (!xlock_policylock(mdr, cur, true))
      return;

    file_layout_t layout;
    if (cur->get_projected_inode()->has_layout())
      layout = cur->get_projected_inode()->layout;
    else if (mdr->dir_layout != file_layout_t())
      layout = mdr->dir_layout;
    else
      layout = mdcache->default_file_layout;

    rest = name.substr(name.find("layout"));
    if (check_layout_vxattr(mdr, rest, value, &layout) < 0)
      return;

    auto &pi = cur->project_inode();
    pi.inode.layout = layout;
    mdr->no_early_reply = true;
    pip = &pi.inode;
  } else if (name.compare(0, 16, "ceph.file.layout") == 0) {
    if (!cur->is_file()) {
      respond_to_request(mdr, -EINVAL);
      return;
    }
    if (cur->get_projected_inode()->size ||
        cur->get_projected_inode()->truncate_seq > 1) {
      respond_to_request(mdr, -ENOTEMPTY);
      return;
    }
    file_layout_t layout = cur->get_projected_inode()->layout;
    rest = name.substr(name.find("layout"));
    if (check_layout_vxattr(mdr, rest, value, &layout) < 0)
      return;

    MutationImpl::LockOpVec lov;
    lov.add_xlock(&cur->filelock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;

    auto &pi = cur->project_inode();
    int64_t old_pool = pi.inode.layout.pool_id;
    pi.inode.add_old_pool(old_pool);
    pi.inode.layout = layout;
    pip = &pi.inode;
  } else if (name.compare(0, 10, "ceph.quota") == 0) { 
    if (!cur->is_dir() || cur->is_root()) {
      respond_to_request(mdr, -EINVAL);
      return;
    }

    quota_info_t quota = cur->get_projected_inode()->quota;

    rest = name.substr(name.find("quota"));
    int r = parse_quota_vxattr(rest, value, &quota);
    if (r < 0) {
      respond_to_request(mdr, r);
      return;
    }

    if (quota.is_enable() && !cur->get_projected_srnode())
      new_realm = true;

    if (!xlock_policylock(mdr, cur, false, new_realm))
      return;

    auto &pi = cur->project_inode(false, new_realm);
    pi.inode.quota = quota;

    if (new_realm) {
      SnapRealm *realm = cur->find_snaprealm();
      auto seq = realm->get_newest_seq();
      auto &newsnap = *pi.snapnode;
      newsnap.created = seq;
      newsnap.seq = seq;
    }
    mdr->no_early_reply = true;
    pip = &pi.inode;

    client_t exclude_ct = mdr->get_client();
    mdcache->broadcast_quota_to_client(cur, exclude_ct, true);
  } else if (name == "ceph.dir.pin"sv) {
    if (!cur->is_dir() || cur->is_root()) {
      respond_to_request(mdr, -EINVAL);
      return;
    }

    mds_rank_t rank;
    try {
      rank = boost::lexical_cast<mds_rank_t>(value);
      if (rank < 0) rank = MDS_RANK_NONE;
    } catch (boost::bad_lexical_cast const&) {
      dout(10) << "bad vxattr value, unable to parse int for " << name << dendl;
      respond_to_request(mdr, -EINVAL);
      return;
    }

    if (!xlock_policylock(mdr, cur))
      return;

    auto &pi = cur->project_inode();
    cur->set_export_pin(rank);
    pip = &pi.inode;
  } else if (name == "ceph.dir.pin.random"sv) {
    if (!cur->is_dir() || cur->is_root()) {
      respond_to_request(mdr, -EINVAL);
      return;
    }

    double val;
    try {
      val = boost::lexical_cast<double>(value);
    } catch (boost::bad_lexical_cast const&) {
      dout(10) << "bad vxattr value, unable to parse float for " << name << dendl;
      respond_to_request(mdr, -EINVAL);
      return;
    }

    if (val < 0.0 || 1.0 < val) {
      respond_to_request(mdr, -EDOM);
      return;
    } else if (mdcache->export_ephemeral_random_max < val) {
      respond_to_request(mdr, -EINVAL);
      return;
    }

    if (!xlock_policylock(mdr, cur))
      return;

    auto &pi = cur->project_inode();
    cur->setxattr_ephemeral_rand(val);
    pip = &pi.inode;
  } else if (name == "ceph.dir.pin.distributed"sv) {
    if (!cur->is_dir() || cur->is_root()) {
      respond_to_request(mdr, -EINVAL);
      return;
    }

    bool val;
    try {
      val = boost::lexical_cast<bool>(value);
    } catch (boost::bad_lexical_cast const&) {
      dout(10) << "bad vxattr value, unable to parse bool for " << name << dendl;
      respond_to_request(mdr, -EINVAL);
      return;
    }

    if (!xlock_policylock(mdr, cur))
      return;

    auto &pi = cur->project_inode();
    cur->setxattr_ephemeral_dist(val);
    pip = &pi.inode;
  } else {
    dout(10) << " unknown vxattr " << name << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }

  pip->change_attr++;
  pip->ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pip->rstat.rctime)
    pip->rstat.rctime = mdr->get_op_stamp();
  pip->version = cur->pre_dirty();
  if (cur->is_file())
    pip->update_backtrace();

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "set vxattr layout");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, cur);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(this, mdr, cur,
								   false, false, new_realm));
  return;
}

void Server::handle_remove_vxattr(MDRequestRef& mdr, CInode *cur)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  string name(req->get_path2());

  dout(10) << __func__ << " " << name << " on " << *cur << dendl;

  if (name == "ceph.dir.layout") {
    if (!cur->is_dir()) {
      respond_to_request(mdr, -ENODATA);
      return;
    }
    if (cur->is_root()) {
      dout(10) << "can't remove layout policy on the root directory" << dendl;
      respond_to_request(mdr, -EINVAL);
      return;
    }

    if (!cur->get_projected_inode()->has_layout()) {
      respond_to_request(mdr, -ENODATA);
      return;
    }

    MutationImpl::LockOpVec lov;
    lov.add_xlock(&cur->policylock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;

    auto &pi = cur->project_inode();
    pi.inode.clear_layout();
    pi.inode.version = cur->pre_dirty();

    // log + wait
    mdr->ls = mdlog->get_current_segment();
    EUpdate *le = new EUpdate(mdlog, "remove dir layout vxattr");
    mdlog->start_entry(le);
    le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
    mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY);
    mdcache->journal_dirty_inode(mdr.get(), &le->metablob, cur);

    mdr->no_early_reply = true;
    journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(this, mdr, cur));
    return;
  } else if (name == "ceph.dir.layout.pool_namespace"
          || name == "ceph.file.layout.pool_namespace") {
    // Namespace is the only layout field that has a meaningful
    // null/none value (empty string, means default layout).  Is equivalent
    // to a setxattr with empty string: pass through the empty payload of
    // the rmxattr request to do this.
    handle_set_vxattr(mdr, cur);
    return;
  }

  respond_to_request(mdr, -ENODATA);
}

class C_MDS_inode_xattr_update_finish : public ServerLogContext {
  CInode *in;
public:

  C_MDS_inode_xattr_update_finish(Server *s, MDRequestRef& r, CInode *i) :
    ServerLogContext(s, r), in(i) { }
  void finish(int r) override {
    ceph_assert(r == 0);

    // apply
    in->pop_and_dirty_projected_inode(mdr->ls);
    
    mdr->apply();

    get_mds()->balancer->hit_inode(in, META_POP_IWR);

    server->respond_to_request(mdr, 0);
  }
};

void Server::handle_client_setxattr(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  string name(req->get_path2());

  // magic ceph.* namespace?
  if (name.compare(0, 5, "ceph.") == 0) {
    // can't use rdlock_path_pin_ref because we need to xlock snaplock/policylock
    CInode *cur = try_get_auth_inode(mdr, req->get_filepath().get_ino());
    if (!cur)
      return;

    handle_set_vxattr(mdr, cur);
    return;
  }

  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur)
    return;

  if (mdr->snapid != CEPH_NOSNAP) {
    respond_to_request(mdr, -EROFS);
    return;
  }

  int flags = req->head.args.setxattr.flags;

  MutationImpl::LockOpVec lov;
  lov.add_xlock(&cur->xattrlock);
  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  if (!check_access(mdr, cur, MAY_WRITE))
    return;

  auto pxattrs = cur->get_projected_xattrs();
  size_t len = req->get_data().length();
  size_t inc = len + name.length();

  // check xattrs kv pairs size
  size_t cur_xattrs_size = 0;
  for (const auto& p : *pxattrs) {
    if ((flags & CEPH_XATTR_REPLACE) && (name.compare(p.first) == 0)) {
      continue;
    }
    cur_xattrs_size += p.first.length() + p.second.length();
  }

  if (((cur_xattrs_size + inc) > g_conf()->mds_max_xattr_pairs_size)) {
    dout(10) << "xattr kv pairs size too big. cur_xattrs_size " 
             << cur_xattrs_size << ", inc " << inc << dendl;
    respond_to_request(mdr, -ENOSPC);
    return;
  }

  if ((flags & CEPH_XATTR_CREATE) && pxattrs->count(mempool::mds_co::string(name))) {
    dout(10) << "setxattr '" << name << "' XATTR_CREATE and EEXIST on " << *cur << dendl;
    respond_to_request(mdr, -EEXIST);
    return;
  }
  if ((flags & CEPH_XATTR_REPLACE) && !pxattrs->count(mempool::mds_co::string(name))) {
    dout(10) << "setxattr '" << name << "' XATTR_REPLACE and ENODATA on " << *cur << dendl;
    respond_to_request(mdr, -ENODATA);
    return;
  }

  dout(10) << "setxattr '" << name << "' len " << len << " on " << *cur << dendl;

  // project update
  auto &pi = cur->project_inode(true);
  pi.inode.version = cur->pre_dirty();
  pi.inode.ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode.rstat.rctime)
    pi.inode.rstat.rctime = mdr->get_op_stamp();
  pi.inode.change_attr++;
  pi.inode.xattr_version++;
  auto &px = *pi.xattrs;
  if ((flags & CEPH_XATTR_REMOVE)) {
    px.erase(mempool::mds_co::string(name));
  } else {
    bufferptr b = buffer::create(len);
    if (len)
      req->get_data().begin().copy(len, b.c_str());
    auto em = px.emplace(std::piecewise_construct, std::forward_as_tuple(mempool::mds_co::string(name)), std::forward_as_tuple(b));
    if (!em.second)
      em.first->second = b;
  }

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setxattr");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, cur);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(this, mdr, cur));
}

void Server::handle_client_removexattr(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  std::string name(req->get_path2());

  if (name.compare(0, 5, "ceph.") == 0) {
    // can't use rdlock_path_pin_ref because we need to xlock snaplock/policylock
    CInode *cur = try_get_auth_inode(mdr, req->get_filepath().get_ino());
    if (!cur)
      return;

    handle_remove_vxattr(mdr, cur);
    return;
  }

  CInode* cur = rdlock_path_pin_ref(mdr, true);
  if (!cur)
    return;

  if (mdr->snapid != CEPH_NOSNAP) {
    respond_to_request(mdr, -EROFS);
    return;
  }

  MutationImpl::LockOpVec lov;
  lov.add_xlock(&cur->xattrlock);
  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  auto pxattrs = cur->get_projected_xattrs();
  if (pxattrs->count(mempool::mds_co::string(name)) == 0) {
    dout(10) << "removexattr '" << name << "' and ENODATA on " << *cur << dendl;
    respond_to_request(mdr, -ENODATA);
    return;
  }

  dout(10) << "removexattr '" << name << "' on " << *cur << dendl;

  // project update
  auto &pi = cur->project_inode(true);
  auto &px = *pi.xattrs;
  pi.inode.version = cur->pre_dirty();
  pi.inode.ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode.rstat.rctime)
    pi.inode.rstat.rctime = mdr->get_op_stamp();
  pi.inode.change_attr++;
  pi.inode.xattr_version++;
  px.erase(mempool::mds_co::string(name));

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "removexattr");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, cur);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(this, mdr, cur));
}


// =================================================================
// DIRECTORY and NAMESPACE OPS


// ------------------------------------------------

// MKNOD

class C_MDS_mknod_finish : public ServerLogContext {
  CDentry *dn;
  CInode *newi;
public:
  C_MDS_mknod_finish(Server *s, MDRequestRef& r, CDentry *d, CInode *ni) :
    ServerLogContext(s, r), dn(d), newi(ni) {}
  void finish(int r) override {
    ceph_assert(r == 0);

    // link the inode
    dn->pop_projected_linkage();
    
    // be a bit hacky with the inode version, here.. we decrement it
    // just to keep mark_dirty() happen. (we didn't bother projecting
    // a new version of hte inode since it's just been created)
    newi->inode.version--; 
    newi->mark_dirty(newi->inode.version + 1, mdr->ls);
    newi->mark_dirty_parent(mdr->ls, true);

    // mkdir?
    if (newi->inode.is_dir()) { 
      CDir *dir = newi->get_dirfrag(frag_t());
      ceph_assert(dir);
      dir->fnode.version--;
      dir->mark_dirty(dir->fnode.version + 1, mdr->ls);
      dir->mark_new(mdr->ls);
    }

    mdr->apply();

    MDRequestRef null_ref;
    get_mds()->mdcache->send_dentry_link(dn, null_ref);

    if (newi->inode.is_file()) {
      get_mds()->locker->share_inode_max_size(newi);
    } else if (newi->inode.is_dir()) {
      // We do this now so that the linkages on the new directory are stable.
      newi->maybe_ephemeral_dist();
      newi->maybe_ephemeral_rand(true);
    }

    // hit pop
    get_mds()->balancer->hit_inode(newi, META_POP_IWR);

    // reply
    server->respond_to_request(mdr, 0);
  }
};


void Server::handle_client_mknod(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  client_t client = mdr->get_client();

  unsigned mode = req->head.args.mknod.mode;
  if ((mode & S_IFMT) == 0)
    mode |= S_IFREG;

  mdr->disable_lock_cache();
  CDentry *dn = rdlock_path_xlock_dentry(mdr, true, false, S_ISREG(mode));
  if (!dn)
    return;

  CDir *dir = dn->get_dir();
  CInode *diri = dir->get_inode();
  if (!check_access(mdr, diri, MAY_WRITE))
    return;
  if (!check_fragment_space(mdr, dn->get_dir()))
    return;

  // set layout
  file_layout_t layout;
  if (mdr->dir_layout != file_layout_t())
    layout = mdr->dir_layout;
  else
    layout = mdcache->default_file_layout;

  CInode *newi = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino), mode, &layout);
  ceph_assert(newi);

  dn->push_projected_linkage(newi);

  newi->inode.rdev = req->head.args.mknod.rdev;
  newi->inode.version = dn->pre_dirty();
  newi->inode.rstat.rfiles = 1;
  if (layout.pool_id != mdcache->default_file_layout.pool_id)
    newi->inode.add_old_pool(mdcache->default_file_layout.pool_id);
  newi->inode.update_backtrace();

  snapid_t follows = mdcache->get_global_snaprealm()->get_newest_seq();
  SnapRealm *realm = dn->get_dir()->inode->find_snaprealm();
  ceph_assert(follows >= realm->get_newest_seq());

  // if the client created a _regular_ file via MKNOD, it's highly likely they'll
  // want to write to it (e.g., if they are reexporting NFS)
  if (S_ISREG(newi->inode.mode)) {
    // issue a cap on the file
    int cmode = CEPH_FILE_MODE_RDWR;
    Capability *cap = mds->locker->issue_new_caps(newi, cmode, mdr, realm);
    if (cap) {
      cap->set_wanted(0);

      // put locks in excl mode
      newi->filelock.set_state(LOCK_EXCL);
      newi->authlock.set_state(LOCK_EXCL);
      newi->xattrlock.set_state(LOCK_EXCL);

      dout(15) << " setting a client_range too, since this is a regular file" << dendl;
      newi->inode.client_ranges[client].range.first = 0;
      newi->inode.client_ranges[client].range.last = newi->inode.layout.stripe_unit;
      newi->inode.client_ranges[client].follows = follows;
      cap->mark_clientwriteable();
    }
  }

  ceph_assert(dn->first == follows + 1);
  newi->first = dn->first;
    
  dout(10) << "mknod mode " << newi->inode.mode << " rdev " << newi->inode.rdev << dendl;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mknod");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);
  
  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(),
				    PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, newi, true, true, true);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(this, mdr, dn, newi));
  mds->balancer->maybe_fragment(dn->get_dir(), false);
}



// MKDIR
/* This function takes responsibility for the passed mdr*/
void Server::handle_client_mkdir(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;

  mdr->disable_lock_cache();
  CDentry *dn = rdlock_path_xlock_dentry(mdr, true);
  if (!dn)
    return;

  CDir *dir = dn->get_dir();
  CInode *diri = dir->get_inode();

  // mkdir check access
  if (!check_access(mdr, diri, MAY_WRITE))
    return;

  if (!check_fragment_space(mdr, dir))
    return;

  // new inode
  unsigned mode = req->head.args.mkdir.mode;
  mode &= ~S_IFMT;
  mode |= S_IFDIR;
  CInode *newi = prepare_new_inode(mdr, dir, inodeno_t(req->head.ino), mode);
  ceph_assert(newi);

  // it's a directory.
  dn->push_projected_linkage(newi);

  newi->inode.version = dn->pre_dirty();
  newi->inode.rstat.rsubdirs = 1;
  newi->inode.update_backtrace();

  snapid_t follows = mdcache->get_global_snaprealm()->get_newest_seq();
  SnapRealm *realm = dn->get_dir()->inode->find_snaprealm();
  ceph_assert(follows >= realm->get_newest_seq());

  dout(12) << " follows " << follows << dendl;
  ceph_assert(dn->first == follows + 1);
  newi->first = dn->first;

  // ...and that new dir is empty.
  CDir *newdir = newi->get_or_open_dirfrag(mdcache, frag_t());
  newdir->state_set(CDir::STATE_CREATING);
  newdir->mark_complete();
  newdir->fnode.version = newdir->pre_dirty();

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mkdir");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);
  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, newi, true, true);
  le->metablob.add_new_dir(newdir); // dirty AND complete AND new
  
  // issue a cap on the directory
  int cmode = CEPH_FILE_MODE_RDWR;
  Capability *cap = mds->locker->issue_new_caps(newi, cmode, mdr, realm);
  if (cap) {
    cap->set_wanted(0);

    // put locks in excl mode
    newi->filelock.set_state(LOCK_EXCL);
    newi->authlock.set_state(LOCK_EXCL);
    newi->xattrlock.set_state(LOCK_EXCL);
  }

  // make sure this inode gets into the journal
  le->metablob.add_opened_ino(newi->ino());

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(this, mdr, dn, newi));

  // We hit_dir (via hit_inode) in our finish callback, but by then we might
  // have overshot the split size (multiple mkdir in flight), so here is
  // an early chance to split the dir if this mkdir makes it oversized.
  mds->balancer->maybe_fragment(dir, false);
}


// SYMLINK

void Server::handle_client_symlink(MDRequestRef& mdr)
{
  mdr->disable_lock_cache();
  CDentry *dn = rdlock_path_xlock_dentry(mdr, true);
  if (!dn)
    return;

  CDir *dir = dn->get_dir();
  CInode *diri = dir->get_inode();

  if (!check_access(mdr, diri, MAY_WRITE))
    return;
  if (!check_fragment_space(mdr, dir))
    return;

  const cref_t<MClientRequest> &req = mdr->client_request;

  unsigned mode = S_IFLNK | 0777;
  CInode *newi = prepare_new_inode(mdr, dir, inodeno_t(req->head.ino), mode);
  ceph_assert(newi);

  // it's a symlink
  dn->push_projected_linkage(newi);

  newi->symlink = req->get_path2();
  newi->inode.size = newi->symlink.length();
  newi->inode.rstat.rbytes = newi->inode.size;
  newi->inode.rstat.rfiles = 1;
  newi->inode.version = dn->pre_dirty();
  newi->inode.update_backtrace();

  newi->first = dn->first;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "symlink");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);
  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, newi, true, true);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(this, mdr, dn, newi));
  mds->balancer->maybe_fragment(dir, false);
}





// LINK

void Server::handle_client_link(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;

  dout(7) << "handle_client_link " << req->get_filepath()
	  << " to " << req->get_filepath2()
	  << dendl;

  mdr->disable_lock_cache();

  CDentry *destdn;
  CInode *targeti;

  if (req->get_filepath2().depth() == 0) {
    targeti = mdcache->get_inode(req->get_filepath2().get_ino());
    if (!targeti) {
      dout(10) << "ESTALE on path2, attempting recovery" << dendl;
      mdcache->find_ino_peers(req->get_filepath2().get_ino(), new C_MDS_TryFindInode(this, mdr));
      return;
    }
    mdr->pin(targeti);

    if (!(mdr->locking_state & MutationImpl::SNAP2_LOCKED)) {
      CDentry *pdn = targeti->get_projected_parent_dn();
      if (!pdn) {
	dout(7) << "target has no parent dn, failing..." << dendl;
	respond_to_request(mdr, -EINVAL);
	return;
      }
      if (!mds->locker->try_rdlock_snap_layout(pdn->get_dir()->get_inode(), mdr, 1))
	return;
      mdr->locking_state |= MutationImpl::SNAP2_LOCKED;
    }

    destdn = rdlock_path_xlock_dentry(mdr, false);
    if (!destdn)
      return;

  } else {
    auto ret = rdlock_two_paths_xlock_destdn(mdr, false);
    destdn = ret.first;
    if (!destdn)
      return;

    if (!destdn->get_projected_linkage()->is_null()) {
      respond_to_request(mdr, -EEXIST);
      return;
    }

    targeti = ret.second->get_projected_linkage()->get_inode();
  }

  if (targeti->is_dir()) {
    dout(7) << "target is a dir, failing..." << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }

  CDir *dir = destdn->get_dir();
  dout(7) << "handle_client_link link " << destdn->get_name() << " in " << *dir << dendl;
  dout(7) << "target is " << *targeti << dendl;

  if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
    MutationImpl::LockOpVec lov;
    lov.add_xlock(&targeti->snaplock);
    lov.add_xlock(&targeti->linklock);

    if (!mds->locker->acquire_locks(mdr, lov))
      return;

    mdr->locking_state |= MutationImpl::ALL_LOCKED;
  }

  if (targeti->get_projected_inode()->nlink == 0) {
    dout(7) << "target has no link, failing..." << dendl;
    respond_to_request(mdr, -ENOENT);
  }

  if ((!mdr->has_more() || mdr->more()->witnessed.empty())) {
    if (!check_access(mdr, targeti, MAY_WRITE))
      return;

    if (!check_access(mdr, dir->get_inode(), MAY_WRITE))
      return;

    if (!check_fragment_space(mdr, dir))
      return;
  }

  // go!
  ceph_assert(g_conf()->mds_kill_link_at != 1);

  // local or remote?
  if (targeti->is_auth()) 
    _link_local(mdr, destdn, targeti);
  else 
    _link_remote(mdr, true, destdn, targeti);
  mds->balancer->maybe_fragment(dir, false);  
}


class C_MDS_link_local_finish : public ServerLogContext {
  CDentry *dn;
  CInode *targeti;
  version_t dnpv;
  version_t tipv;
  bool adjust_realm;
public:
  C_MDS_link_local_finish(Server *s, MDRequestRef& r, CDentry *d, CInode *ti,
			  version_t dnpv_, version_t tipv_, bool ar) :
    ServerLogContext(s, r), dn(d), targeti(ti),
    dnpv(dnpv_), tipv(tipv_), adjust_realm(ar) { }
  void finish(int r) override {
    ceph_assert(r == 0);
    server->_link_local_finish(mdr, dn, targeti, dnpv, tipv, adjust_realm);
  }
};


void Server::_link_local(MDRequestRef& mdr, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_local " << *dn << " to " << *targeti << dendl;

  mdr->ls = mdlog->get_current_segment();

  // predirty NEW dentry
  version_t dnpv = dn->pre_dirty();
  version_t tipv = targeti->pre_dirty();
  
  // project inode update
  auto &pi = targeti->project_inode();
  pi.inode.nlink++;
  pi.inode.ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode.rstat.rctime)
    pi.inode.rstat.rctime = mdr->get_op_stamp();
  pi.inode.change_attr++;
  pi.inode.version = tipv;

  bool adjust_realm = false;
  if (!targeti->is_projected_snaprealm_global()) {
    sr_t *newsnap = targeti->project_snaprealm();
    targeti->mark_snaprealm_global(newsnap);
    targeti->record_snaprealm_parent_dentry(newsnap, NULL, targeti->get_projected_parent_dn(), true);
    adjust_realm = true;
  }

  // log + wait
  EUpdate *le = new EUpdate(mdlog, "link_local");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, 1);      // new dn
  mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, 0, PREDIRTY_PRIMARY);           // targeti
  le->metablob.add_remote_dentry(dn, true, targeti->ino(), targeti->d_type());  // new remote
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, targeti);

  // do this after predirty_*, to avoid funky extra dnl arg
  dn->push_projected_linkage(targeti->ino(), targeti->d_type());

  journal_and_reply(mdr, targeti, dn, le,
		    new C_MDS_link_local_finish(this, mdr, dn, targeti, dnpv, tipv, adjust_realm));
}

void Server::_link_local_finish(MDRequestRef& mdr, CDentry *dn, CInode *targeti,
				version_t dnpv, version_t tipv, bool adjust_realm)
{
  dout(10) << "_link_local_finish " << *dn << " to " << *targeti << dendl;

  // link and unlock the NEW dentry
  CDentry::linkage_t *dnl = dn->pop_projected_linkage();
  if (!dnl->get_inode())
    dn->link_remote(dnl, targeti);
  dn->mark_dirty(dnpv, mdr->ls);

  // target inode
  targeti->pop_and_dirty_projected_inode(mdr->ls);

  mdr->apply();

  MDRequestRef null_ref;
  mdcache->send_dentry_link(dn, null_ref);

  if (adjust_realm) {
    int op = CEPH_SNAP_OP_SPLIT;
    mds->mdcache->send_snap_update(targeti, 0, op);
    mds->mdcache->do_realm_invalidate_and_update_notify(targeti, op);
  }

  // bump target popularity
  mds->balancer->hit_inode(targeti, META_POP_IWR);
  mds->balancer->hit_dir(dn->get_dir(), META_POP_IWR);

  // reply
  respond_to_request(mdr, 0);
}


// link / unlink remote

class C_MDS_link_remote_finish : public ServerLogContext {
  bool inc;
  CDentry *dn;
  CInode *targeti;
  version_t dpv;
public:
  C_MDS_link_remote_finish(Server *s, MDRequestRef& r, bool i, CDentry *d, CInode *ti) :
    ServerLogContext(s, r), inc(i), dn(d), targeti(ti),
    dpv(d->get_projected_version()) {}
  void finish(int r) override {
    ceph_assert(r == 0);
    server->_link_remote_finish(mdr, inc, dn, targeti, dpv);
  }
};

void Server::_link_remote(MDRequestRef& mdr, bool inc, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_remote " 
	   << (inc ? "link ":"unlink ")
	   << *dn << " to " << *targeti << dendl;

  // 1. send LinkPrepare to dest (journal nlink++ prepare)
  mds_rank_t linkauth = targeti->authority().first;
  if (mdr->more()->witnessed.count(linkauth) == 0) {
    if (mds->is_cluster_degraded() &&
	!mds->mdsmap->is_clientreplay_or_active_or_stopping(linkauth)) {
      dout(10) << " targeti auth mds." << linkauth << " is not active" << dendl;
      if (mdr->more()->waiting_on_slave.empty())
	mds->wait_for_active_peer(linkauth, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }

    dout(10) << " targeti auth must prepare nlink++/--" << dendl;
    int op;
    if (inc)
      op = MMDSSlaveRequest::OP_LINKPREP;
    else 
      op = MMDSSlaveRequest::OP_UNLINKPREP;
    auto req = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, op);
    targeti->set_object_info(req->get_object_info());
    req->op_stamp = mdr->get_op_stamp();
    if (auto& desti_srnode = mdr->more()->desti_srnode)
      encode(*desti_srnode, req->desti_snapbl);
    mds->send_message_mds(req, linkauth);

    ceph_assert(mdr->more()->waiting_on_slave.count(linkauth) == 0);
    mdr->more()->waiting_on_slave.insert(linkauth);
    return;
  }
  dout(10) << " targeti auth has prepared nlink++/--" << dendl;

  ceph_assert(g_conf()->mds_kill_link_at != 2);

  if (auto& desti_srnode = mdr->more()->desti_srnode) {
    delete desti_srnode;
    desti_srnode = NULL;
  }

  mdr->set_mds_stamp(ceph_clock_now());

  // add to event
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, inc ? "link_remote":"unlink_remote");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    mdcache->add_uncommitted_leader(mdr->reqid, mdr->ls, mdr->more()->witnessed);
  }

  if (inc) {
    dn->pre_dirty();
    mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, 1);
    le->metablob.add_remote_dentry(dn, true, targeti->ino(), targeti->d_type()); // new remote
    dn->push_projected_linkage(targeti->ino(), targeti->d_type());
  } else {
    dn->pre_dirty();
    mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, -1);
    mdcache->journal_cow_dentry(mdr.get(), &le->metablob, dn);
    le->metablob.add_null_dentry(dn, true);
    dn->push_projected_linkage();
  }

  journal_and_reply(mdr, (inc ? targeti : nullptr), dn, le,
		    new C_MDS_link_remote_finish(this, mdr, inc, dn, targeti));
}

void Server::_link_remote_finish(MDRequestRef& mdr, bool inc,
				 CDentry *dn, CInode *targeti,
				 version_t dpv)
{
  dout(10) << "_link_remote_finish "
	   << (inc ? "link ":"unlink ")
	   << *dn << " to " << *targeti << dendl;

  ceph_assert(g_conf()->mds_kill_link_at != 3);

  if (!mdr->more()->witnessed.empty())
    mdcache->logged_leader_update(mdr->reqid);

  if (inc) {
    // link the new dentry
    CDentry::linkage_t *dnl = dn->pop_projected_linkage();
    if (!dnl->get_inode())
      dn->link_remote(dnl, targeti);
    dn->mark_dirty(dpv, mdr->ls);
  } else {
    // unlink main dentry
    dn->get_dir()->unlink_inode(dn);
    dn->pop_projected_linkage();
    dn->mark_dirty(dn->get_projected_version(), mdr->ls);  // dirty old dentry
  }

  mdr->apply();

  MDRequestRef null_ref;
  if (inc)
    mdcache->send_dentry_link(dn, null_ref);
  else
    mdcache->send_dentry_unlink(dn, NULL, null_ref);
  
  // bump target popularity
  mds->balancer->hit_inode(targeti, META_POP_IWR);
  mds->balancer->hit_dir(dn->get_dir(), META_POP_IWR);

  // reply
  respond_to_request(mdr, 0);

  if (!inc)
    // removing a new dn?
    dn->get_dir()->try_remove_unlinked_dn(dn);
}


// remote linking/unlinking

class C_MDS_SlaveLinkPrep : public ServerLogContext {
  CInode *targeti;
  bool adjust_realm;
public:
  C_MDS_SlaveLinkPrep(Server *s, MDRequestRef& r, CInode *t, bool ar) :
    ServerLogContext(s, r), targeti(t), adjust_realm(ar) { }
  void finish(int r) override {
    ceph_assert(r == 0);
    server->_logged_slave_link(mdr, targeti, adjust_realm);
  }
};

class C_MDS_SlaveLinkCommit : public ServerContext {
  MDRequestRef mdr;
  CInode *targeti;
public:
  C_MDS_SlaveLinkCommit(Server *s, MDRequestRef& r, CInode *t) :
    ServerContext(s), mdr(r), targeti(t) { }
  void finish(int r) override {
    server->_commit_slave_link(mdr, r, targeti);
  }
};

void Server::handle_slave_link_prep(MDRequestRef& mdr)
{
  dout(10) << "handle_slave_link_prep " << *mdr 
	   << " on " << mdr->slave_request->get_object_info() 
	   << dendl;

  ceph_assert(g_conf()->mds_kill_link_at != 4);

  CInode *targeti = mdcache->get_inode(mdr->slave_request->get_object_info().ino);
  ceph_assert(targeti);
  dout(10) << "targeti " << *targeti << dendl;
  CDentry *dn = targeti->get_parent_dn();
  CDentry::linkage_t *dnl = dn->get_linkage();
  ceph_assert(dnl->is_primary());

  mdr->set_op_stamp(mdr->slave_request->op_stamp);

  mdr->auth_pin(targeti);

  //ceph_abort();  // test hack: make sure leader can handle a slave that fails to prepare...
  ceph_assert(g_conf()->mds_kill_link_at != 5);

  // journal it
  mdr->ls = mdlog->get_current_segment();
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_prep", mdr->reqid, mdr->slave_to_mds,
				      ESlaveUpdate::OP_PREPARE, ESlaveUpdate::LINK);
  mdlog->start_entry(le);

  auto &pi = dnl->get_inode()->project_inode();

  // update journaled target inode
  bool inc;
  bool adjust_realm = false;
  bool realm_projected = false;
  if (mdr->slave_request->get_op() == MMDSSlaveRequest::OP_LINKPREP) {
    inc = true;
    pi.inode.nlink++;
    if (!targeti->is_projected_snaprealm_global()) {
      sr_t *newsnap = targeti->project_snaprealm();
      targeti->mark_snaprealm_global(newsnap);
      targeti->record_snaprealm_parent_dentry(newsnap, NULL, targeti->get_projected_parent_dn(), true);
      adjust_realm = true;
      realm_projected = true;
    }
  } else {
    inc = false;
    pi.inode.nlink--;
    if (targeti->is_projected_snaprealm_global()) {
      ceph_assert(mdr->slave_request->desti_snapbl.length());
      auto p = mdr->slave_request->desti_snapbl.cbegin();

      sr_t *newsnap = targeti->project_snaprealm();
      decode(*newsnap, p);

      if (pi.inode.nlink == 0)
	ceph_assert(!newsnap->is_parent_global());

      realm_projected = true;
    } else {
      ceph_assert(mdr->slave_request->desti_snapbl.length() == 0);
    }
  }

  link_rollback rollback;
  rollback.reqid = mdr->reqid;
  rollback.ino = targeti->ino();
  rollback.old_ctime = targeti->inode.ctime;   // we hold versionlock xlock; no concorrent projections
  const fnode_t *pf = targeti->get_parent_dn()->get_dir()->get_projected_fnode();
  rollback.old_dir_mtime = pf->fragstat.mtime;
  rollback.old_dir_rctime = pf->rstat.rctime;
  rollback.was_inc = inc;
  if (realm_projected) {
    if (targeti->snaprealm) {
      encode(true, rollback.snapbl);
      targeti->encode_snap_blob(rollback.snapbl);
    } else {
      encode(false, rollback.snapbl);
    }
  }
  encode(rollback, le->rollback);
  mdr->more()->rollback_bl = le->rollback;

  pi.inode.ctime = mdr->get_op_stamp();
  pi.inode.version = targeti->pre_dirty();

  dout(10) << " projected inode " << pi.inode.ino << " v " << pi.inode.version << dendl;

  // commit case
  mdcache->predirty_journal_parents(mdr, &le->commit, dnl->get_inode(), 0, PREDIRTY_SHALLOW|PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr.get(), &le->commit, targeti);
  mdcache->add_uncommitted_slave(mdr->reqid, mdr->ls, mdr->slave_to_mds);

  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveLinkCommit(this, mdr, targeti);

  mdr->more()->slave_update_journaled = true;
  submit_mdlog_entry(le, new C_MDS_SlaveLinkPrep(this, mdr, targeti, adjust_realm),
                     mdr, __func__);
  mdlog->flush();
}

void Server::_logged_slave_link(MDRequestRef& mdr, CInode *targeti, bool adjust_realm)
{
  dout(10) << "_logged_slave_link " << *mdr
	   << " " << *targeti << dendl;

  ceph_assert(g_conf()->mds_kill_link_at != 6);

  // update the target
  targeti->pop_and_dirty_projected_inode(mdr->ls);
  mdr->apply();

  // hit pop
  mds->balancer->hit_inode(targeti, META_POP_IWR);

  // done.
  mdr->reset_slave_request();

  if (adjust_realm) {
    int op = CEPH_SNAP_OP_SPLIT;
    mds->mdcache->send_snap_update(targeti, 0, op);
    mds->mdcache->do_realm_invalidate_and_update_notify(targeti, op);
  }

  // ack
  if (!mdr->aborted) {
    auto reply = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_LINKPREPACK);
    mds->send_message_mds(reply, mdr->slave_to_mds);
  } else {
    dout(10) << " abort flag set, finishing" << dendl;
    mdcache->request_finish(mdr);
  }
}


struct C_MDS_CommittedSlave : public ServerLogContext {
  C_MDS_CommittedSlave(Server *s, MDRequestRef& m) : ServerLogContext(s, m) {}
  void finish(int r) override {
    server->_committed_slave(mdr);
  }
};

void Server::_commit_slave_link(MDRequestRef& mdr, int r, CInode *targeti)
{  
  dout(10) << "_commit_slave_link " << *mdr
	   << " r=" << r
	   << " " << *targeti << dendl;

  ceph_assert(g_conf()->mds_kill_link_at != 7);

  if (r == 0) {
    // drop our pins, etc.
    mdr->cleanup();

    // write a commit to the journal
    ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_commit", mdr->reqid, mdr->slave_to_mds,
					ESlaveUpdate::OP_COMMIT, ESlaveUpdate::LINK);
    mdlog->start_entry(le);
    submit_mdlog_entry(le, new C_MDS_CommittedSlave(this, mdr), mdr, __func__);
    mdlog->flush();
  } else {
    do_link_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr);
  }
}

void Server::_committed_slave(MDRequestRef& mdr)
{
  dout(10) << "_committed_slave " << *mdr << dendl;

  ceph_assert(g_conf()->mds_kill_link_at != 8);

  bool assert_exist = mdr->more()->slave_update_journaled;
  mdcache->finish_uncommitted_slave(mdr->reqid, assert_exist);
  auto req = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_COMMITTED);
  mds->send_message_mds(req, mdr->slave_to_mds);
  mdcache->request_finish(mdr);
}

struct C_MDS_LoggedLinkRollback : public ServerLogContext {
  MutationRef mut;
  map<client_t,ref_t<MClientSnap>> splits;
  C_MDS_LoggedLinkRollback(Server *s, MutationRef& m, MDRequestRef& r,
			   map<client_t,ref_t<MClientSnap>>&& _splits) :
    ServerLogContext(s, r), mut(m), splits(std::move(_splits)) {
  }
  void finish(int r) override {
    server->_link_rollback_finish(mut, mdr, splits);
  }
};

void Server::do_link_rollback(bufferlist &rbl, mds_rank_t leader, MDRequestRef& mdr)
{
  link_rollback rollback;
  auto p = rbl.cbegin();
  decode(rollback, p);

  dout(10) << "do_link_rollback on " << rollback.reqid 
	   << (rollback.was_inc ? " inc":" dec") 
	   << " ino " << rollback.ino
	   << dendl;

  ceph_assert(g_conf()->mds_kill_link_at != 9);

  mdcache->add_rollback(rollback.reqid, leader); // need to finish this update before resolve finishes
  ceph_assert(mdr || mds->is_resolve());

  MutationRef mut(new MutationImpl(nullptr, utime_t(), rollback.reqid));
  mut->ls = mds->mdlog->get_current_segment();

  CInode *in = mdcache->get_inode(rollback.ino);
  ceph_assert(in);
  dout(10) << " target is " << *in << dendl;
  ceph_assert(!in->is_projected());  // live slave request hold versionlock xlock.
  
  auto &pi = in->project_inode();
  pi.inode.version = in->pre_dirty();
  mut->add_projected_inode(in);

  // parent dir rctime
  CDir *parent = in->get_projected_parent_dn()->get_dir();
  fnode_t *pf = parent->project_fnode();
  mut->add_projected_fnode(parent);
  pf->version = parent->pre_dirty();
  if (pf->fragstat.mtime == pi.inode.ctime) {
    pf->fragstat.mtime = rollback.old_dir_mtime;
    if (pf->rstat.rctime == pi.inode.ctime)
      pf->rstat.rctime = rollback.old_dir_rctime;
    mut->add_updated_lock(&parent->get_inode()->filelock);
    mut->add_updated_lock(&parent->get_inode()->nestlock);
  }

  // inode
  pi.inode.ctime = rollback.old_ctime;
  if (rollback.was_inc)
    pi.inode.nlink--;
  else
    pi.inode.nlink++;

  map<client_t,ref_t<MClientSnap>> splits;
  if (rollback.snapbl.length() && in->snaprealm) {
    bool hadrealm;
    auto p = rollback.snapbl.cbegin();
    decode(hadrealm, p);
    if (hadrealm) {
      if (!mds->is_resolve()) {
	sr_t *new_srnode = new sr_t();
	decode(*new_srnode, p);
	in->project_snaprealm(new_srnode);
      } else {
	decode(in->snaprealm->srnode, p);
      }
    } else {
      SnapRealm *realm = parent->get_inode()->find_snaprealm();
      if (!mds->is_resolve())
	mdcache->prepare_realm_merge(in->snaprealm, realm, splits);
      in->project_snaprealm(NULL);
    }
  }

  // journal it
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_rollback", rollback.reqid, leader,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::LINK);
  mdlog->start_entry(le);
  le->commit.add_dir_context(parent);
  le->commit.add_dir(parent, true);
  le->commit.add_primary_dentry(in->get_projected_parent_dn(), 0, true);
  
  submit_mdlog_entry(le, new C_MDS_LoggedLinkRollback(this, mut, mdr, std::move(splits)),
                     mdr, __func__);
  mdlog->flush();
}

void Server::_link_rollback_finish(MutationRef& mut, MDRequestRef& mdr,
				   map<client_t,ref_t<MClientSnap>>& splits)
{
  dout(10) << "_link_rollback_finish" << dendl;

  ceph_assert(g_conf()->mds_kill_link_at != 10);

  mut->apply();

  if (!mds->is_resolve())
    mdcache->send_snaps(splits);

  if (mdr)
    mdcache->request_finish(mdr);

  mdcache->finish_rollback(mut->reqid, mdr);

  mut->cleanup();
}


void Server::handle_slave_link_prep_ack(MDRequestRef& mdr, const cref_t<MMDSSlaveRequest> &m)
{
  dout(10) << "handle_slave_link_prep_ack " << *mdr 
	   << " " << *m << dendl;
  mds_rank_t from = mds_rank_t(m->get_source().num());

  ceph_assert(g_conf()->mds_kill_link_at != 11);

  // note slave
  mdr->more()->slaves.insert(from);
  
  // witnessed!
  ceph_assert(mdr->more()->witnessed.count(from) == 0);
  mdr->more()->witnessed.insert(from);
  ceph_assert(!m->is_not_journaled());
  mdr->more()->has_journaled_slaves = true;
  
  // remove from waiting list
  ceph_assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  ceph_assert(mdr->more()->waiting_on_slave.empty());

  dispatch_client_request(mdr);  // go again!
}





// UNLINK

void Server::handle_client_unlink(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  client_t client = mdr->get_client();

  // rmdir or unlink?
  bool rmdir = (req->get_op() == CEPH_MDS_OP_RMDIR);

  if (rmdir)
    mdr->disable_lock_cache();
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, true);
  if (!dn)
    return;

  CDentry::linkage_t *dnl = dn->get_linkage(client, mdr);
  ceph_assert(!dnl->is_null());
  CInode *in = dnl->get_inode();

  if (rmdir) {
    dout(7) << "handle_client_rmdir on " << *dn << dendl;
  } else {
    dout(7) << "handle_client_unlink on " << *dn << dendl;
  }
  dout(7) << "dn links to " << *in << dendl;

  // rmdir vs is_dir 
  if (in->is_dir()) {
    if (rmdir) {
      // do empty directory checks
      if (_dir_is_nonempty_unlocked(mdr, in)) {
	respond_to_request(mdr, -ENOTEMPTY);
	return;
      }
    } else {
      dout(7) << "handle_client_unlink on dir " << *in << ", returning error" << dendl;
      respond_to_request(mdr, -EISDIR);
      return;
    }
  } else {
    if (rmdir) {
      // unlink
      dout(7) << "handle_client_rmdir on non-dir " << *in << ", returning error" << dendl;
      respond_to_request(mdr, -ENOTDIR);
      return;
    }
  }

  CInode *diri = dn->get_dir()->get_inode();
  if ((!mdr->has_more() || mdr->more()->witnessed.empty())) {
    if (!check_access(mdr, diri, MAY_WRITE))
      return;
  }

  // -- create stray dentry? --
  CDentry *straydn = NULL;
  if (dnl->is_primary()) {
    straydn = prepare_stray_dentry(mdr, dnl->get_inode());
    if (!straydn)
      return;
    dout(10) << " straydn is " << *straydn << dendl;
  } else if (mdr->straydn) {
    mdr->unpin(mdr->straydn);
    mdr->straydn = NULL;
  }

  // lock
  if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
    MutationImpl::LockOpVec lov;

    lov.add_xlock(&in->linklock);
    lov.add_xlock(&in->snaplock);
    if (in->is_dir())
      lov.add_rdlock(&in->filelock);   // to verify it's empty

    if (straydn) {
      lov.add_wrlock(&straydn->get_dir()->inode->filelock);
      lov.add_wrlock(&straydn->get_dir()->inode->nestlock);
      lov.add_xlock(&straydn->lock);
    }

    if (!mds->locker->acquire_locks(mdr, lov))
      return;

    mdr->locking_state |= MutationImpl::ALL_LOCKED;
  }

  if (in->is_dir() &&
      _dir_is_nonempty(mdr, in)) {
    respond_to_request(mdr, -ENOTEMPTY);
    return;
  }

  if (straydn)
    straydn->first = mdcache->get_global_snaprealm()->get_newest_seq() + 1;

  if (!mdr->more()->desti_srnode) {
    if (in->is_projected_snaprealm_global()) {
      sr_t *new_srnode = in->prepare_new_srnode(0);
      in->record_snaprealm_parent_dentry(new_srnode, NULL, dn, dnl->is_primary());
      // dropping the last linkage or dropping the last remote linkage,
      // detch the inode from global snaprealm
      auto nlink = in->get_projected_inode()->nlink;
      if (nlink == 1 ||
	  (nlink == 2 && !dnl->is_primary() &&
	   !in->get_projected_parent_dir()->inode->is_stray()))
	in->clear_snaprealm_global(new_srnode);
      mdr->more()->desti_srnode = new_srnode;
    } else if (dnl->is_primary()) {
      // prepare snaprealm blob for slave request
      SnapRealm *realm = in->find_snaprealm();
      snapid_t follows = realm->get_newest_seq();
      if (in->snaprealm || follows + 1 > in->get_oldest_snap()) {
	sr_t *new_srnode = in->prepare_new_srnode(follows);
	in->record_snaprealm_past_parent(new_srnode, straydn->get_dir()->inode->find_snaprealm());
	mdr->more()->desti_srnode = new_srnode;
      }
    }
  }

  // yay!
  if (in->is_dir() && in->has_subtree_root_dirfrag()) {
    // subtree root auths need to be witnesses
    set<mds_rank_t> witnesses;
    in->list_replicas(witnesses);
    dout(10) << " witnesses " << witnesses << ", have " << mdr->more()->witnessed << dendl;

    for (set<mds_rank_t>::iterator p = witnesses.begin();
	 p != witnesses.end();
	 ++p) {
      if (mdr->more()->witnessed.count(*p)) {
	dout(10) << " already witnessed by mds." << *p << dendl;
      } else if (mdr->more()->waiting_on_slave.count(*p)) {
	dout(10) << " already waiting on witness mds." << *p << dendl;      
      } else {
	if (!_rmdir_prepare_witness(mdr, *p, mdr->dn[0], straydn))
	  return;
      }
    }
    if (!mdr->more()->waiting_on_slave.empty())
      return;  // we're waiting for a witness.
  }

  if (!rmdir && dnl->is_primary() && mdr->dn[0].size() == 1)
    mds->locker->create_lock_cache(mdr, diri);

  // ok!
  if (dnl->is_remote() && !dnl->get_inode()->is_auth()) 
    _link_remote(mdr, false, dn, dnl->get_inode());
  else
    _unlink_local(mdr, dn, straydn);
}

class C_MDS_unlink_local_finish : public ServerLogContext {
  CDentry *dn;
  CDentry *straydn;
  version_t dnpv;  // deleted dentry
public:
  C_MDS_unlink_local_finish(Server *s, MDRequestRef& r, CDentry *d, CDentry *sd) :
    ServerLogContext(s, r), dn(d), straydn(sd),
    dnpv(d->get_projected_version()) {}
  void finish(int r) override {
    ceph_assert(r == 0);
    server->_unlink_local_finish(mdr, dn, straydn, dnpv);
  }
};

void Server::_unlink_local(MDRequestRef& mdr, CDentry *dn, CDentry *straydn)
{
  dout(10) << "_unlink_local " << *dn << dendl;

  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  CInode *in = dnl->get_inode();


  // ok, let's do it.
  mdr->ls = mdlog->get_current_segment();

  // prepare log entry
  EUpdate *le = new EUpdate(mdlog, "unlink_local");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    mdcache->add_uncommitted_leader(mdr->reqid, mdr->ls, mdr->more()->witnessed);
  }

  if (straydn) {
    ceph_assert(dnl->is_primary());
    straydn->push_projected_linkage(in);
  }

  // the unlinked dentry
  dn->pre_dirty();

  auto &pi = in->project_inode();
  {
    std::string t;
    dn->make_path_string(t, true);
    pi.inode.stray_prior_path = std::move(t);
  }
  pi.inode.version = in->pre_dirty();
  pi.inode.ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode.rstat.rctime)
    pi.inode.rstat.rctime = mdr->get_op_stamp();
  pi.inode.change_attr++;
  pi.inode.nlink--;
  if (pi.inode.nlink == 0)
    in->state_set(CInode::STATE_ORPHAN);

  if (mdr->more()->desti_srnode) {
    auto& desti_srnode = mdr->more()->desti_srnode;
    in->project_snaprealm(desti_srnode);
    desti_srnode = NULL;
  }

  if (straydn) {
    // will manually pop projected inode

    // primary link.  add stray dentry.
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, -1);
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, straydn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);

    pi.inode.update_backtrace();
    le->metablob.add_primary_dentry(straydn, in, true, true);
  } else {
    mdr->add_projected_inode(in);
    // remote link.  update remote inode.
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(), PREDIRTY_DIR, -1);
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, 0, PREDIRTY_PRIMARY);
    mdcache->journal_dirty_inode(mdr.get(), &le->metablob, in);
  }

  mdcache->journal_cow_dentry(mdr.get(), &le->metablob, dn);
  le->metablob.add_null_dentry(dn, true);

  if (in->is_dir()) {
    dout(10) << " noting renamed (unlinked) dir ino " << in->ino() << " in metablob" << dendl;
    le->metablob.renamed_dirino = in->ino();
  }

  dn->push_projected_linkage();

  if (straydn) {
    ceph_assert(in->first <= straydn->first);
    in->first = straydn->first;
  }

  if (in->is_dir()) {
    ceph_assert(straydn);
    mdcache->project_subtree_rename(in, dn->get_dir(), straydn->get_dir());
  }

  journal_and_reply(mdr, 0, dn, le, new C_MDS_unlink_local_finish(this, mdr, dn, straydn));
}

void Server::_unlink_local_finish(MDRequestRef& mdr,
				  CDentry *dn, CDentry *straydn,
				  version_t dnpv) 
{
  dout(10) << "_unlink_local_finish " << *dn << dendl;

  if (!mdr->more()->witnessed.empty())
    mdcache->logged_leader_update(mdr->reqid);

  CInode *strayin = NULL;
  bool hadrealm = false;
  if (straydn) {
    // if there is newly created snaprealm, need to split old snaprealm's
    // inodes_with_caps. So pop snaprealm before linkage changes.
    strayin = dn->get_linkage()->get_inode();
    hadrealm = strayin->snaprealm ? true : false;
    strayin->early_pop_projected_snaprealm();
  }

  // unlink main dentry
  dn->get_dir()->unlink_inode(dn);
  dn->pop_projected_linkage();

  // relink as stray?  (i.e. was primary link?)
  if (straydn) {
    dout(20) << " straydn is " << *straydn << dendl;
    straydn->pop_projected_linkage();

    strayin->pop_and_dirty_projected_inode(mdr->ls);

    mdcache->touch_dentry_bottom(straydn);
  }

  dn->mark_dirty(dnpv, mdr->ls);
  mdr->apply();
  
  mdcache->send_dentry_unlink(dn, straydn, mdr);
  
  if (straydn) {
    // update subtree map?
    if (strayin->is_dir())
      mdcache->adjust_subtree_after_rename(strayin, dn->get_dir(), true);

    if (strayin->snaprealm && !hadrealm)
      mdcache->do_realm_invalidate_and_update_notify(strayin, CEPH_SNAP_OP_SPLIT, false);
  }

  // bump pop
  mds->balancer->hit_dir(dn->get_dir(), META_POP_IWR);

  // reply
  respond_to_request(mdr, 0);
  
  // removing a new dn?
  dn->get_dir()->try_remove_unlinked_dn(dn);

  // clean up ?
  // respond_to_request() drops locks. So stray reintegration can race with us.
  if (straydn && !straydn->get_projected_linkage()->is_null()) {
    // Tip off the MDCache that this dentry is a stray that
    // might be elegible for purge.
    mdcache->notify_stray(straydn);
  }
}

bool Server::_rmdir_prepare_witness(MDRequestRef& mdr, mds_rank_t who, vector<CDentry*>& trace, CDentry *straydn)
{
  if (mds->is_cluster_degraded() &&
      !mds->mdsmap->is_clientreplay_or_active_or_stopping(who)) {
    dout(10) << "_rmdir_prepare_witness mds." << who << " is not active" << dendl;
    if (mdr->more()->waiting_on_slave.empty())
      mds->wait_for_active_peer(who, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
  
  dout(10) << "_rmdir_prepare_witness mds." << who << dendl;
  auto req = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_RMDIRPREP);
  req->srcdnpath = filepath(trace.front()->get_dir()->ino());
  for (auto dn : trace)
    req->srcdnpath.push_dentry(dn->get_name());
  mdcache->encode_replica_stray(straydn, who, req->straybl);
  if (mdr->more()->desti_srnode)
    encode(*mdr->more()->desti_srnode, req->desti_snapbl);

  req->op_stamp = mdr->get_op_stamp();
  mds->send_message_mds(req, who);
  
  ceph_assert(mdr->more()->waiting_on_slave.count(who) == 0);
  mdr->more()->waiting_on_slave.insert(who);
  return true;
}

struct C_MDS_SlaveRmdirPrep : public ServerLogContext {
  CDentry *dn, *straydn;
  C_MDS_SlaveRmdirPrep(Server *s, MDRequestRef& r, CDentry *d, CDentry *st)
    : ServerLogContext(s, r), dn(d), straydn(st) {}
  void finish(int r) override {
    server->_logged_slave_rmdir(mdr, dn, straydn);
  }
};

struct C_MDS_SlaveRmdirCommit : public ServerContext {
  MDRequestRef mdr;
  CDentry *straydn;
  C_MDS_SlaveRmdirCommit(Server *s, MDRequestRef& r, CDentry *sd)
    : ServerContext(s), mdr(r), straydn(sd) { }
  void finish(int r) override {
    server->_commit_slave_rmdir(mdr, r, straydn);
  }
};

void Server::handle_slave_rmdir_prep(MDRequestRef& mdr)
{
  dout(10) << "handle_slave_rmdir_prep " << *mdr 
	   << " " << mdr->slave_request->srcdnpath 
	   << " to " << mdr->slave_request->destdnpath
	   << dendl;

  vector<CDentry*> trace;
  filepath srcpath(mdr->slave_request->srcdnpath);
  dout(10) << " src " << srcpath << dendl;
  CInode *in;
  CF_MDS_MDRContextFactory cf(mdcache, mdr, false);
  int r = mdcache->path_traverse(mdr, cf, srcpath,
				 MDS_TRAVERSE_DISCOVER | MDS_TRAVERSE_PATH_LOCKED,
				 &trace, &in);
  if (r > 0) return;
  if (r == -ESTALE) {
    mdcache->find_ino_peers(srcpath.get_ino(), new C_MDS_RetryRequest(mdcache, mdr),
			    mdr->slave_to_mds, true);
    return;
  }
  ceph_assert(r == 0);
  CDentry *dn = trace.back();
  dout(10) << " dn " << *dn << dendl;
  mdr->pin(dn);

  ceph_assert(mdr->straydn);
  CDentry *straydn = mdr->straydn;
  dout(10) << " straydn " << *straydn << dendl;
  
  mdr->set_op_stamp(mdr->slave_request->op_stamp);

  rmdir_rollback rollback;
  rollback.reqid = mdr->reqid;
  rollback.src_dir = dn->get_dir()->dirfrag();
  rollback.src_dname = dn->get_name();
  rollback.dest_dir = straydn->get_dir()->dirfrag();
  rollback.dest_dname = straydn->get_name();
  if (mdr->slave_request->desti_snapbl.length()) {
    if (in->snaprealm) {
      encode(true, rollback.snapbl);
      in->encode_snap_blob(rollback.snapbl);
    } else {
      encode(false, rollback.snapbl);
    }
  }
  encode(rollback, mdr->more()->rollback_bl);
  // FIXME: rollback snaprealm
  dout(20) << " rollback is " << mdr->more()->rollback_bl.length() << " bytes" << dendl;

  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveRmdirCommit(this, mdr, straydn);

  straydn->push_projected_linkage(in);
  dn->push_projected_linkage();

  ceph_assert(straydn->first >= in->first);
  in->first = straydn->first;

  if (!in->has_subtree_root_dirfrag(mds->get_nodeid())) {
    dout(10) << " no auth subtree in " << *in << ", skipping journal" << dendl;
    _logged_slave_rmdir(mdr, dn, straydn);
    return;
  }

  mdr->ls = mdlog->get_current_segment();
  ESlaveUpdate *le =  new ESlaveUpdate(mdlog, "slave_rmdir", mdr->reqid, mdr->slave_to_mds,
				       ESlaveUpdate::OP_PREPARE, ESlaveUpdate::RMDIR);
  mdlog->start_entry(le);
  le->rollback = mdr->more()->rollback_bl;

  le->commit.add_dir_context(straydn->get_dir());
  le->commit.add_primary_dentry(straydn, in, true);
  // slave: no need to journal original dentry

  dout(10) << " noting renamed (unlinked) dir ino " << in->ino() << " in metablob" << dendl;
  le->commit.renamed_dirino = in->ino();

  mdcache->project_subtree_rename(in, dn->get_dir(), straydn->get_dir());
  mdcache->add_uncommitted_slave(mdr->reqid, mdr->ls, mdr->slave_to_mds);

  mdr->more()->slave_update_journaled = true;
  submit_mdlog_entry(le, new C_MDS_SlaveRmdirPrep(this, mdr, dn, straydn),
                     mdr, __func__);
  mdlog->flush();
}

void Server::_logged_slave_rmdir(MDRequestRef& mdr, CDentry *dn, CDentry *straydn)
{
  dout(10) << "_logged_slave_rmdir " << *mdr << " on " << *dn << dendl;
  CInode *in = dn->get_linkage()->get_inode();

  bool new_realm;
  if (mdr->slave_request->desti_snapbl.length()) {
    new_realm = !in->snaprealm;
    in->decode_snap_blob(mdr->slave_request->desti_snapbl);
    ceph_assert(in->snaprealm);
    ceph_assert(in->snaprealm->have_past_parents_open());
  } else {
    new_realm = false;
  }

  // update our cache now, so we are consistent with what is in the journal
  // when we journal a subtree map
  dn->get_dir()->unlink_inode(dn);
  straydn->pop_projected_linkage();
  dn->pop_projected_linkage();

  mdcache->adjust_subtree_after_rename(in, dn->get_dir(), mdr->more()->slave_update_journaled);

  if (new_realm)
      mdcache->do_realm_invalidate_and_update_notify(in, CEPH_SNAP_OP_SPLIT, false);

  // done.
  mdr->reset_slave_request();
  mdr->straydn = 0;

  if (!mdr->aborted) {
    auto reply = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_RMDIRPREPACK);
    if (!mdr->more()->slave_update_journaled)
      reply->mark_not_journaled();
    mds->send_message_mds(reply, mdr->slave_to_mds);
  } else {
    dout(10) << " abort flag set, finishing" << dendl;
    mdcache->request_finish(mdr);
  }
}

void Server::handle_slave_rmdir_prep_ack(MDRequestRef& mdr, const cref_t<MMDSSlaveRequest> &ack)
{
  dout(10) << "handle_slave_rmdir_prep_ack " << *mdr 
	   << " " << *ack << dendl;

  mds_rank_t from = mds_rank_t(ack->get_source().num());

  mdr->more()->slaves.insert(from);
  mdr->more()->witnessed.insert(from);
  if (!ack->is_not_journaled())
    mdr->more()->has_journaled_slaves = true;

  // remove from waiting list
  ceph_assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  if (mdr->more()->waiting_on_slave.empty())
    dispatch_client_request(mdr);  // go again!
  else 
    dout(10) << "still waiting on slaves " << mdr->more()->waiting_on_slave << dendl;
}

void Server::_commit_slave_rmdir(MDRequestRef& mdr, int r, CDentry *straydn)
{
  dout(10) << "_commit_slave_rmdir " << *mdr << " r=" << r << dendl;

  if (r == 0) {
    if (mdr->more()->slave_update_journaled) {
      CInode *strayin = straydn->get_projected_linkage()->get_inode();
      if (strayin && !strayin->snaprealm)
	mdcache->clear_dirty_bits_for_stray(strayin);
    }

    mdr->cleanup();

    if (mdr->more()->slave_update_journaled) {
      // write a commit to the journal
      ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rmdir_commit", mdr->reqid,
					  mdr->slave_to_mds, ESlaveUpdate::OP_COMMIT,
					  ESlaveUpdate::RMDIR);
      mdlog->start_entry(le);
      submit_mdlog_entry(le, new C_MDS_CommittedSlave(this, mdr), mdr, __func__);
      mdlog->flush();
    } else {
      _committed_slave(mdr);
    }
  } else {
    // abort
    do_rmdir_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr);
  }
}

struct C_MDS_LoggedRmdirRollback : public ServerLogContext {
  metareqid_t reqid;
  CDentry *dn;
  CDentry *straydn;
  C_MDS_LoggedRmdirRollback(Server *s, MDRequestRef& m, metareqid_t mr, CDentry *d, CDentry *st)
    : ServerLogContext(s, m), reqid(mr), dn(d), straydn(st) {}
  void finish(int r) override {
    server->_rmdir_rollback_finish(mdr, reqid, dn, straydn);
  }
};

void Server::do_rmdir_rollback(bufferlist &rbl, mds_rank_t leader, MDRequestRef& mdr)
{
  // unlink the other rollback methods, the rmdir rollback is only
  // needed to record the subtree changes in the journal for inode
  // replicas who are auth for empty dirfrags.  no actual changes to
  // the file system are taking place here, so there is no Mutation.

  rmdir_rollback rollback;
  auto p = rbl.cbegin();
  decode(rollback, p);
  
  dout(10) << "do_rmdir_rollback on " << rollback.reqid << dendl;
  mdcache->add_rollback(rollback.reqid, leader); // need to finish this update before resolve finishes
  ceph_assert(mdr || mds->is_resolve());

  CDir *dir = mdcache->get_dirfrag(rollback.src_dir);
  if (!dir)
    dir = mdcache->get_dirfrag(rollback.src_dir.ino, rollback.src_dname);
  ceph_assert(dir);
  CDentry *dn = dir->lookup(rollback.src_dname);
  ceph_assert(dn);
  dout(10) << " dn " << *dn << dendl;
  CDir *straydir = mdcache->get_dirfrag(rollback.dest_dir);
  ceph_assert(straydir);
  CDentry *straydn = straydir->lookup(rollback.dest_dname);
  ceph_assert(straydn);
  dout(10) << " straydn " << *straydn << dendl;
  CInode *in = straydn->get_linkage()->get_inode();

  dn->push_projected_linkage(in);
  straydn->push_projected_linkage();

  if (rollback.snapbl.length() && in->snaprealm) {
    bool hadrealm;
    auto p = rollback.snapbl.cbegin();
    decode(hadrealm, p);
    if (hadrealm) {
      decode(in->snaprealm->srnode, p);
    } else {
      in->snaprealm->merge_to(dir->get_inode()->find_snaprealm());
    }
  }

  if (mdr && !mdr->more()->slave_update_journaled) {
    ceph_assert(!in->has_subtree_root_dirfrag(mds->get_nodeid()));

    _rmdir_rollback_finish(mdr, rollback.reqid, dn, straydn);
    return;
  }


  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rmdir_rollback", rollback.reqid, leader,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::RMDIR);
  mdlog->start_entry(le);
  
  le->commit.add_dir_context(dn->get_dir());
  le->commit.add_primary_dentry(dn, in, true);
  // slave: no need to journal straydn
  
  dout(10) << " noting renamed (unlinked) dir ino " << in->ino() << " in metablob" << dendl;
  le->commit.renamed_dirino = in->ino();

  mdcache->project_subtree_rename(in, straydn->get_dir(), dn->get_dir());

  submit_mdlog_entry(le,
                     new C_MDS_LoggedRmdirRollback(this, mdr,rollback.reqid,
                                                   dn, straydn),
                     mdr, __func__);
  mdlog->flush();
}

void Server::_rmdir_rollback_finish(MDRequestRef& mdr, metareqid_t reqid, CDentry *dn, CDentry *straydn)
{
  dout(10) << "_rmdir_rollback_finish " << reqid << dendl;

  straydn->get_dir()->unlink_inode(straydn);
  dn->pop_projected_linkage();
  straydn->pop_projected_linkage();

  CInode *in = dn->get_linkage()->get_inode();
  mdcache->adjust_subtree_after_rename(in, straydn->get_dir(),
				       !mdr || mdr->more()->slave_update_journaled);

  if (mds->is_resolve()) {
    CDir *root = mdcache->get_subtree_root(straydn->get_dir());
    mdcache->try_trim_non_auth_subtree(root);
  }

  if (mdr)
    mdcache->request_finish(mdr);

  mdcache->finish_rollback(reqid, mdr);
}


/** _dir_is_nonempty[_unlocked]
 *
 * check if a directory is non-empty (i.e. we can rmdir it).
 *
 * the unlocked varient this is a fastpath check.  we can't really be
 * sure until we rdlock the filelock.
 */
bool Server::_dir_is_nonempty_unlocked(MDRequestRef& mdr, CInode *in)
{
  dout(10) << "dir_is_nonempty_unlocked " << *in << dendl;
  ceph_assert(in->is_auth());

  if (in->filelock.is_cached())
    return false; // there can be pending async create/unlink. don't know.
  if (in->snaprealm && in->snaprealm->srnode.snaps.size())
    return true; // in a snapshot!

  auto&& ls = in->get_dirfrags();
  for (const auto& dir : ls) {
    // is the frag obviously non-empty?
    if (dir->is_auth()) {
      if (dir->get_projected_fnode()->fragstat.size()) {
	dout(10) << "dir_is_nonempty_unlocked dirstat has " 
		 << dir->get_projected_fnode()->fragstat.size() << " items " << *dir << dendl;
	return true;
      }
    }
  }

  return false;
}

bool Server::_dir_is_nonempty(MDRequestRef& mdr, CInode *in)
{
  dout(10) << "dir_is_nonempty " << *in << dendl;
  ceph_assert(in->is_auth());
  ceph_assert(in->filelock.can_read(mdr->get_client()));

  frag_info_t dirstat;
  version_t dirstat_version = in->get_projected_inode()->dirstat.version;

  auto&& ls = in->get_dirfrags();
  for (const auto& dir : ls) {
    const fnode_t *pf = dir->get_projected_fnode();
    if (pf->fragstat.size()) {
      dout(10) << "dir_is_nonempty dirstat has "
	       << pf->fragstat.size() << " items " << *dir << dendl;
      return true;
    }

    if (pf->accounted_fragstat.version == dirstat_version)
      dirstat.add(pf->accounted_fragstat);
    else
      dirstat.add(pf->fragstat);
  }

  return dirstat.size() != in->get_projected_inode()->dirstat.size();
}


// ======================================================


class C_MDS_rename_finish : public ServerLogContext {
  CDentry *srcdn;
  CDentry *destdn;
  CDentry *straydn;
public:
  C_MDS_rename_finish(Server *s, MDRequestRef& r,
		      CDentry *sdn, CDentry *ddn, CDentry *stdn) :
    ServerLogContext(s, r),
    srcdn(sdn), destdn(ddn), straydn(stdn) { }
  void finish(int r) override {
    ceph_assert(r == 0);
    server->_rename_finish(mdr, srcdn, destdn, straydn);
  }
};


/** handle_client_rename
 *
 * rename leader is the destdn auth.  this is because cached inodes
 * must remain connected.  thus, any replica of srci, must also
 * replicate destdn, and possibly straydn, so that srci (and
 * destdn->inode) remain connected during the rename.
 *
 * to do this, we freeze srci, then leader (destdn auth) verifies that
 * all other nodes have also replciated destdn and straydn.  note that
 * destdn replicas need not also replicate srci.  this only works when 
 * destdn is leader.
 *
 * This function takes responsibility for the passed mdr.
 */
void Server::handle_client_rename(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  dout(7) << "handle_client_rename " << *req << dendl;

  filepath destpath = req->get_filepath();
  filepath srcpath = req->get_filepath2();
  if (srcpath.is_last_dot_or_dotdot() || destpath.is_last_dot_or_dotdot()) {
    respond_to_request(mdr, -EBUSY);
    return;
  }

  auto [destdn, srcdn] = rdlock_two_paths_xlock_destdn(mdr, true);
  if (!destdn)
    return;

  dout(10) << " destdn " << *destdn << dendl;
  CDir *destdir = destdn->get_dir();
  ceph_assert(destdir->is_auth());
  CDentry::linkage_t *destdnl = destdn->get_projected_linkage();

  dout(10) << " srcdn " << *srcdn << dendl;
  CDir *srcdir = srcdn->get_dir();
  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
  CInode *srci = srcdnl->get_inode();
  dout(10) << " srci " << *srci << dendl;

  // -- some sanity checks --
  if (destdn == srcdn) {
    dout(7) << "rename src=dest, noop" << dendl;
    respond_to_request(mdr, 0);
    return;
  }

  // dest a child of src?
  // e.g. mv /usr /usr/foo
  if (srci->is_dir() && srci->is_projected_ancestor_of(destdir->get_inode())) {
    dout(7) << "cannot rename item to be a child of itself" << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }

  // is this a stray migration, reintegration or merge? (sanity checks!)
  if (mdr->reqid.name.is_mds() &&
      !(MDS_INO_IS_STRAY(srcpath.get_ino()) &&
	MDS_INO_IS_STRAY(destpath.get_ino())) &&
      !(destdnl->is_remote() &&
	destdnl->get_remote_ino() == srci->ino())) {
    respond_to_request(mdr, -EINVAL);  // actually, this won't reply, but whatev.
    return;
  }

  CInode *oldin = 0;
  if (!destdnl->is_null()) {
    //dout(10) << "dest dn exists " << *destdn << dendl;
    oldin = mdcache->get_dentry_inode(destdn, mdr, true);
    if (!oldin) return;
    dout(10) << " oldin " << *oldin << dendl;

    // non-empty dir? do trivial fast unlocked check, do another check later with read locks
    if (oldin->is_dir() && _dir_is_nonempty_unlocked(mdr, oldin)) {
      respond_to_request(mdr, -ENOTEMPTY);
      return;
    }

    // mv /some/thing /to/some/existing_other_thing
    if (oldin->is_dir() && !srci->is_dir()) {
      respond_to_request(mdr, -EISDIR);
      return;
    }
    if (!oldin->is_dir() && srci->is_dir()) {
      respond_to_request(mdr, -ENOTDIR);
      return;
    }
    if (srci == oldin && !srcdir->inode->is_stray()) {
      respond_to_request(mdr, 0);  // no-op.  POSIX makes no sense.
      return;
    }
  }

  vector<CDentry*>& srctrace = mdr->dn[1];
  vector<CDentry*>& desttrace = mdr->dn[0];

  // src+dest traces _must_ share a common ancestor for locking to prevent orphans
  if (destpath.get_ino() != srcpath.get_ino() &&
      !(req->get_source().is_mds() &&
	MDS_INO_IS_STRAY(srcpath.get_ino()))) {  // <-- mds 'rename' out of stray dir is ok!
    CInode *srcbase = srctrace[0]->get_dir()->get_inode();
    CInode *destbase = desttrace[0]->get_dir()->get_inode();
    // ok, extend srctrace toward root until it is an ancestor of desttrace.
    while (srcbase != destbase &&
	   !srcbase->is_projected_ancestor_of(destbase)) {
      CDentry *pdn = srcbase->get_projected_parent_dn();
      srctrace.insert(srctrace.begin(), pdn);
      dout(10) << "rename prepending srctrace with " << *pdn << dendl;
      srcbase = pdn->get_dir()->get_inode();
    }

    // then, extend destpath until it shares the same parent inode as srcpath.
    while (destbase != srcbase) {
      CDentry *pdn = destbase->get_projected_parent_dn();
      desttrace.insert(desttrace.begin(), pdn);
      dout(10) << "rename prepending desttrace with " << *pdn << dendl;
      destbase = pdn->get_dir()->get_inode();
    }
    dout(10) << "rename src and dest traces now share common ancestor " << *destbase << dendl;
  }


  bool linkmerge = srcdnl->get_inode() == destdnl->get_inode();
  if (linkmerge)
    dout(10) << " this is a link merge" << dendl;

  // -- create stray dentry? --
  CDentry *straydn = NULL;
  if (destdnl->is_primary() && !linkmerge) {
    straydn = prepare_stray_dentry(mdr, destdnl->get_inode());
    if (!straydn)
      return;
    dout(10) << " straydn is " << *straydn << dendl;
  } else if (mdr->straydn) {
    mdr->unpin(mdr->straydn);
    mdr->straydn = NULL;
  }


  // -- locks --
  if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
    MutationImpl::LockOpVec lov;

    // we need to update srci's ctime.  xlock its least contended lock to do that...
    lov.add_xlock(&srci->linklock);
    lov.add_xlock(&srci->snaplock);

    if (oldin) {
      // xlock oldin (for nlink--)
      lov.add_xlock(&oldin->linklock);
      lov.add_xlock(&oldin->snaplock);
      if (oldin->is_dir()) {
	ceph_assert(srci->is_dir());
	lov.add_rdlock(&oldin->filelock);   // to verify it's empty

	// adjust locking order?
	int cmp = mdr->compare_paths();
	if (cmp < 0 || (cmp == 0 && oldin->ino() < srci->ino()))
	  std::reverse(lov.begin(), lov.end());
      } else {
	ceph_assert(!srci->is_dir());
	// adjust locking order;
	if (srci->ino() > oldin->ino())
	  std::reverse(lov.begin(), lov.end());
      }
    }

    // straydn?
    if (straydn) {
      lov.add_wrlock(&straydn->get_dir()->inode->filelock);
      lov.add_wrlock(&straydn->get_dir()->inode->nestlock);
      lov.add_xlock(&straydn->lock);
    }

    CInode *auth_pin_freeze = !srcdn->is_auth() && srcdnl->is_primary() ? srci : nullptr;
    if (!mds->locker->acquire_locks(mdr, lov, auth_pin_freeze))
      return;

    mdr->locking_state |= MutationImpl::ALL_LOCKED;
  }

  if (linkmerge)
    ceph_assert(srcdir->inode->is_stray() && srcdnl->is_primary() && destdnl->is_remote());

  if ((!mdr->has_more() || mdr->more()->witnessed.empty())) {
    if (!check_access(mdr, srcdir->get_inode(), MAY_WRITE))
      return;

    if (!check_access(mdr, destdn->get_dir()->get_inode(), MAY_WRITE))
      return;

    if (!check_fragment_space(mdr, destdn->get_dir()))
      return;

    if (!check_access(mdr, srci, MAY_WRITE))
      return;
  }

  // with read lock, really verify oldin is empty
  if (oldin &&
      oldin->is_dir() &&
      _dir_is_nonempty(mdr, oldin)) {
    respond_to_request(mdr, -ENOTEMPTY);
    return;
  }

  /* project_snaprealm_past_parent() will do this job
   *
  // moving between snaprealms?
  if (srcdnl->is_primary() && srci->is_multiversion() && !srci->snaprealm) {
    SnapRealm *srcrealm = srci->find_snaprealm();
    SnapRealm *destrealm = destdn->get_dir()->inode->find_snaprealm();
    if (srcrealm != destrealm &&
	(srcrealm->get_newest_seq() + 1 > srcdn->first ||
	 destrealm->get_newest_seq() + 1 > srcdn->first)) {
      dout(10) << " renaming between snaprealms, creating snaprealm for " << *srci << dendl;
      mdcache->snaprealm_create(mdr, srci);
      return;
    }
  }
  */

  ceph_assert(g_conf()->mds_kill_rename_at != 1);

  // -- open all srcdn inode frags, if any --
  // we need these open so that auth can properly delegate from inode to dirfrags
  // after the inode is _ours_.
  if (srcdnl->is_primary() && 
      !srcdn->is_auth() && 
      srci->is_dir()) {
    dout(10) << "srci is remote dir, setting stickydirs and opening all frags" << dendl;
    mdr->set_stickydirs(srci);

    frag_vec_t leaves;
    srci->dirfragtree.get_leaves(leaves);
    for (const auto& leaf : leaves) {
      CDir *dir = srci->get_dirfrag(leaf);
      if (!dir) {
	dout(10) << " opening " << leaf << " under " << *srci << dendl;
	mdcache->open_remote_dirfrag(srci, leaf, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }
    }
  }

  // -- prepare snaprealm ---

  if (linkmerge) {
    if (!mdr->more()->srci_srnode &&
	srci->get_projected_inode()->nlink == 1 &&
	srci->is_projected_snaprealm_global()) {
      sr_t *new_srnode = srci->prepare_new_srnode(0);
      srci->record_snaprealm_parent_dentry(new_srnode, NULL, destdn, false);

      srci->clear_snaprealm_global(new_srnode);
      mdr->more()->srci_srnode = new_srnode;
    }
  } else {
    if (oldin && !mdr->more()->desti_srnode) {
      if (oldin->is_projected_snaprealm_global()) {
	sr_t *new_srnode = oldin->prepare_new_srnode(0);
	oldin->record_snaprealm_parent_dentry(new_srnode, NULL, destdn, destdnl->is_primary());
	// dropping the last linkage or dropping the last remote linkage,
	// detch the inode from global snaprealm
	auto nlink = oldin->get_projected_inode()->nlink;
	if (nlink == 1 ||
	    (nlink == 2 && !destdnl->is_primary() &&
	     !oldin->get_projected_parent_dir()->inode->is_stray()))
	  oldin->clear_snaprealm_global(new_srnode);
	mdr->more()->desti_srnode = new_srnode;
      } else if (destdnl->is_primary()) {
	SnapRealm *dest_realm = destdir->inode->find_snaprealm();
	snapid_t follows = dest_realm->get_newest_seq();
	if (oldin->snaprealm || follows + 1 > oldin->get_oldest_snap()) {
	  sr_t *new_srnode = oldin->prepare_new_srnode(follows);
	  oldin->record_snaprealm_past_parent(new_srnode, straydn->get_dir()->inode->find_snaprealm());
	  mdr->more()->desti_srnode = new_srnode;
	}
      }
    }
    if (!mdr->more()->srci_srnode) {
      SnapRealm *dest_realm = destdir->inode->find_snaprealm();
      if (srci->is_projected_snaprealm_global()) {
	sr_t *new_srnode = srci->prepare_new_srnode(0);
	srci->record_snaprealm_parent_dentry(new_srnode, dest_realm, srcdn, srcdnl->is_primary());
	mdr->more()->srci_srnode = new_srnode;
      } else if (srcdnl->is_primary()) {
	SnapRealm *src_realm = srcdir->inode->find_snaprealm();
	snapid_t follows = src_realm->get_newest_seq();
	if (src_realm != dest_realm &&
	    (srci->snaprealm || follows + 1 > srci->get_oldest_snap())) {
	  sr_t *new_srnode = srci->prepare_new_srnode(follows);
	  srci->record_snaprealm_past_parent(new_srnode, dest_realm);
	  mdr->more()->srci_srnode = new_srnode;
	}
      }
    }
  }

  // -- prepare witnesses --

  /*
   * NOTE: we use _all_ replicas as witnesses.
   * this probably isn't totally necessary (esp for file renames),
   * but if/when we change that, we have to make sure rejoin is
   * sufficiently robust to handle strong rejoins from survivors
   * with totally wrong dentry->inode linkage.
   * (currently, it can ignore rename effects, because the resolve
   * stage will sort them out.)
   */
  set<mds_rank_t> witnesses = mdr->more()->extra_witnesses;
  if (srcdn->is_auth())
    srcdn->list_replicas(witnesses);
  else
    witnesses.insert(srcdn->authority().first);
  if (srcdnl->is_remote() && !srci->is_auth())
    witnesses.insert(srci->authority().first);
  destdn->list_replicas(witnesses);
  if (destdnl->is_remote() && !oldin->is_auth())
    witnesses.insert(oldin->authority().first);
  dout(10) << " witnesses " << witnesses << ", have " << mdr->more()->witnessed << dendl;

  if (!witnesses.empty()) {
    // Replicas can't see projected dentry linkages and will get confused.
    // We have taken snaplocks on ancestor inodes. Later rename/rmdir requests
    // can't project these inodes' linkages.
    bool need_flush = false;
    for (auto& dn : srctrace) {
      if (dn->is_projected()) {
	need_flush = true;
	break;
      }
    }
    if (!need_flush) {
      CDentry *dn = destdn;
      do {
	if (dn->is_projected()) {
	  need_flush = true;
	  break;
	}
	CInode *diri = dn->get_dir()->get_inode();
	dn = diri->get_projected_parent_dn();
      } while (dn);
    }
    if (need_flush) {
      mdlog->wait_for_safe(
	  new MDSInternalContextWrapper(mds,
	    new C_MDS_RetryRequest(mdcache, mdr)));
      mdlog->flush();
      return;
    }
  }

  // do srcdn auth last
  mds_rank_t last = MDS_RANK_NONE;
  if (!srcdn->is_auth()) {
    last = srcdn->authority().first;
    mdr->more()->srcdn_auth_mds = last;
    // ask auth of srci to mark srci as ambiguous auth if more than two MDS
    // are involved in the rename operation.
    if (srcdnl->is_primary() && !mdr->more()->is_ambiguous_auth) {
      dout(10) << " preparing ambiguous auth for srci" << dendl;
      ceph_assert(mdr->more()->is_remote_frozen_authpin);
      ceph_assert(mdr->more()->rename_inode == srci);
      _rename_prepare_witness(mdr, last, witnesses, srctrace, desttrace, straydn);
      return;
    }
  }
  
  for (set<mds_rank_t>::iterator p = witnesses.begin();
       p != witnesses.end();
       ++p) {
    if (*p == last) continue;  // do it last!
    if (mdr->more()->witnessed.count(*p)) {
      dout(10) << " already witnessed by mds." << *p << dendl;
    } else if (mdr->more()->waiting_on_slave.count(*p)) {
      dout(10) << " already waiting on witness mds." << *p << dendl;      
    } else {
      if (!_rename_prepare_witness(mdr, *p, witnesses, srctrace, desttrace, straydn))
	return;
    }
  }
  if (!mdr->more()->waiting_on_slave.empty())
    return;  // we're waiting for a witness.

  if (last != MDS_RANK_NONE && mdr->more()->witnessed.count(last) == 0) {
    dout(10) << " preparing last witness (srcdn auth)" << dendl;
    ceph_assert(mdr->more()->waiting_on_slave.count(last) == 0);
    _rename_prepare_witness(mdr, last, witnesses, srctrace, desttrace, straydn);
    return;
  }

  // test hack: bail after slave does prepare, so we can verify it's _live_ rollback.
  if (!mdr->more()->slaves.empty() && !srci->is_dir())
    ceph_assert(g_conf()->mds_kill_rename_at != 3);
  if (!mdr->more()->slaves.empty() && srci->is_dir())
    ceph_assert(g_conf()->mds_kill_rename_at != 4);

  // -- declare now --
  mdr->set_mds_stamp(ceph_clock_now());

  // -- prepare journal entry --
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "rename");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    
    mdcache->add_uncommitted_leader(mdr->reqid, mdr->ls, mdr->more()->witnessed);
    // no need to send frozen auth pin to recovring auth MDS of srci
    mdr->more()->is_remote_frozen_authpin = false;
  }
  
  _rename_prepare(mdr, &le->metablob, &le->client_map, srcdn, destdn, straydn);
  if (le->client_map.length())
    le->cmapv = mds->sessionmap.get_projected();

  // -- commit locally --
  C_MDS_rename_finish *fin = new C_MDS_rename_finish(this, mdr, srcdn, destdn, straydn);

  journal_and_reply(mdr, srci, destdn, le, fin);
  mds->balancer->maybe_fragment(destdn->get_dir(), false);
}


void Server::_rename_finish(MDRequestRef& mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_finish " << *mdr << dendl;

  if (!mdr->more()->witnessed.empty())
    mdcache->logged_leader_update(mdr->reqid);

  // apply
  _rename_apply(mdr, srcdn, destdn, straydn);

  mdcache->send_dentry_link(destdn, mdr);

  CDentry::linkage_t *destdnl = destdn->get_linkage();
  CInode *in = destdnl->get_inode();
  bool need_eval = mdr->more()->cap_imports.count(in);

  // test hack: test slave commit
  if (!mdr->more()->slaves.empty() && !in->is_dir())
    ceph_assert(g_conf()->mds_kill_rename_at != 5);
  if (!mdr->more()->slaves.empty() && in->is_dir())
    ceph_assert(g_conf()->mds_kill_rename_at != 6);
  
  // bump popularity
  mds->balancer->hit_dir(srcdn->get_dir(), META_POP_IWR);
  if (destdnl->is_remote() && in->is_auth())
    mds->balancer->hit_inode(in, META_POP_IWR);

  // did we import srci?  if so, explicitly ack that import that, before we unlock and reply.

  ceph_assert(g_conf()->mds_kill_rename_at != 7);

  // reply
  respond_to_request(mdr, 0);

  if (need_eval)
    mds->locker->eval(in, CEPH_CAP_LOCKS, true);

  // clean up?
  // respond_to_request() drops locks. So stray reintegration can race with us.
  if (straydn && !straydn->get_projected_linkage()->is_null()) {
    mdcache->notify_stray(straydn);
  }
}



// helpers

bool Server::_rename_prepare_witness(MDRequestRef& mdr, mds_rank_t who, set<mds_rank_t> &witnesse,
				     vector<CDentry*>& srctrace, vector<CDentry*>& dsttrace, CDentry *straydn)
{
  if (mds->is_cluster_degraded() &&
      !mds->mdsmap->is_clientreplay_or_active_or_stopping(who)) {
    dout(10) << "_rename_prepare_witness mds." << who << " is not active" << dendl;
    if (mdr->more()->waiting_on_slave.empty())
      mds->wait_for_active_peer(who, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }

  dout(10) << "_rename_prepare_witness mds." << who << dendl;
  auto req = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_RENAMEPREP);

  req->srcdnpath = filepath(srctrace.front()->get_dir()->ino());
  for (auto dn : srctrace)
    req->srcdnpath.push_dentry(dn->get_name());
  req->destdnpath = filepath(dsttrace.front()->get_dir()->ino());
  for (auto dn : dsttrace)
    req->destdnpath.push_dentry(dn->get_name());
  if (straydn)
    mdcache->encode_replica_stray(straydn, who, req->straybl);

  if (mdr->more()->srci_srnode)
    encode(*mdr->more()->srci_srnode, req->srci_snapbl);
  if (mdr->more()->desti_srnode)
    encode(*mdr->more()->desti_srnode, req->desti_snapbl);

  req->srcdn_auth = mdr->more()->srcdn_auth_mds;
  
  // srcdn auth will verify our current witness list is sufficient
  req->witnesses = witnesse;

  req->op_stamp = mdr->get_op_stamp();
  mds->send_message_mds(req, who);
  
  ceph_assert(mdr->more()->waiting_on_slave.count(who) == 0);
  mdr->more()->waiting_on_slave.insert(who);
  return true;
}

version_t Server::_rename_prepare_import(MDRequestRef& mdr, CDentry *srcdn, bufferlist *client_map_bl)
{
  version_t oldpv = mdr->more()->inode_import_v;

  CDentry::linkage_t *srcdnl = srcdn->get_linkage();

  /* import node */
  auto blp = mdr->more()->inode_import.cbegin();
	  
  // imported caps
  map<client_t,entity_inst_t> client_map;
  map<client_t, client_metadata_t> client_metadata_map;
  decode(client_map, blp);
  decode(client_metadata_map, blp);
  prepare_force_open_sessions(client_map, client_metadata_map,
			      mdr->more()->imported_session_map);
  encode(client_map, *client_map_bl, mds->mdsmap->get_up_features());
  encode(client_metadata_map, *client_map_bl);

  list<ScatterLock*> updated_scatterlocks;
  mdcache->migrator->decode_import_inode(srcdn, blp, srcdn->authority().first, mdr->ls,
					 mdr->more()->cap_imports, updated_scatterlocks);

  // hack: force back to !auth and clean, temporarily
  srcdnl->get_inode()->state_clear(CInode::STATE_AUTH);
  srcdnl->get_inode()->mark_clean();

  return oldpv;
}

bool Server::_need_force_journal(CInode *diri, bool empty)
{
  auto&& dirs = diri->get_dirfrags();

  bool force_journal = false;
  if (empty) {
    for (const auto& dir : dirs) {
      if (dir->is_subtree_root() && dir->get_dir_auth().first == mds->get_nodeid()) {
	dout(10) << " frag " << dir->get_frag() << " is auth subtree dirfrag, will force journal" << dendl;
	force_journal = true;
	break;
      } else
	dout(20) << " frag " << dir->get_frag() << " is not auth subtree dirfrag" << dendl;
    }
  } else {
    // see if any children of our frags are auth subtrees.
    std::vector<CDir*> subtrees;
    mdcache->get_subtrees(subtrees);
    dout(10) << " subtrees " << subtrees << " frags " << dirs << dendl;
    for (const auto& dir : dirs) {
      for (const auto& subtree : subtrees) {
	if (dir->contains(subtree)) {
	  if (subtree->get_dir_auth().first == mds->get_nodeid()) {
	    dout(10) << " frag " << dir->get_frag() << " contains (maybe) auth subtree, will force journal "
		     << *subtree << dendl;
	    force_journal = true;
	    break;
	  } else
	    dout(20) << " frag " << dir->get_frag() << " contains but isn't auth for " << *subtree << dendl;
	} else
	  dout(20) << " frag " << dir->get_frag() << " does not contain " << *subtree << dendl;
      }
      if (force_journal)
	break;
    }
  }
  return force_journal;
}

void Server::_rename_prepare(MDRequestRef& mdr,
			     EMetaBlob *metablob, bufferlist *client_map_bl,
			     CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_prepare " << *mdr << " " << *srcdn << " " << *destdn << dendl;
  if (straydn)
    dout(10) << " straydn " << *straydn << dendl;

  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
  CDentry::linkage_t *destdnl = destdn->get_projected_linkage();
  CInode *srci = srcdnl->get_inode();
  CInode *oldin = destdnl->get_inode();

  // primary+remote link merge?
  bool linkmerge = (srci == oldin);
  if (linkmerge)
    ceph_assert(srcdnl->is_primary() && destdnl->is_remote());
  bool silent = srcdn->get_dir()->inode->is_stray();

  bool force_journal_dest = false;
  if (srci->is_dir() && !destdn->is_auth()) {
    if (srci->is_auth()) {
      // if we are auth for srci and exporting it, force journal because journal replay needs
      // the source inode to create auth subtrees.
      dout(10) << " we are exporting srci, will force journal destdn" << dendl;
      force_journal_dest = true;
    } else
      force_journal_dest = _need_force_journal(srci, false);
  }

  bool force_journal_stray = false;
  if (oldin && oldin->is_dir() && straydn && !straydn->is_auth())
    force_journal_stray = _need_force_journal(oldin, true);

  if (linkmerge)
    dout(10) << " merging remote and primary links to the same inode" << dendl;
  if (silent)
    dout(10) << " reintegrating stray; will avoid changing nlink or dir mtime" << dendl;
  if (force_journal_dest)
    dout(10) << " forcing journal destdn because we (will) have auth subtrees nested beneath it" << dendl;
  if (force_journal_stray)
    dout(10) << " forcing journal straydn because we (will) have auth subtrees nested beneath it" << dendl;

  if (srci->is_dir() && (destdn->is_auth() || force_journal_dest)) {
    dout(10) << " noting renamed dir ino " << srci->ino() << " in metablob" << dendl;
    metablob->renamed_dirino = srci->ino();
  } else if (oldin && oldin->is_dir() && force_journal_stray) {
    dout(10) << " noting rename target dir " << oldin->ino() << " in metablob" << dendl;
    metablob->renamed_dirino = oldin->ino();
  }

  // prepare
  CInode::mempool_inode *spi = 0;    // renamed inode
  CInode::mempool_inode *tpi = 0;  // target/overwritten inode
  
  // target inode
  if (!linkmerge) {
    if (destdnl->is_primary()) {
      ceph_assert(straydn);  // moving to straydn.
      // link--, and move.
      if (destdn->is_auth()) {
	auto &pi= oldin->project_inode(); //project_snaprealm
	pi.inode.version = straydn->pre_dirty(pi.inode.version);
	pi.inode.update_backtrace();
        tpi = &pi.inode;
      }
      straydn->push_projected_linkage(oldin);
    } else if (destdnl->is_remote()) {
      // nlink-- targeti
      if (oldin->is_auth()) {
	auto &pi = oldin->project_inode();
	pi.inode.version = oldin->pre_dirty();
        tpi = &pi.inode;
      }
    }
  }

  // dest
  if (srcdnl->is_remote()) {
    if (!linkmerge) {
      // destdn
      if (destdn->is_auth())
	mdr->more()->pvmap[destdn] = destdn->pre_dirty();
      destdn->push_projected_linkage(srcdnl->get_remote_ino(), srcdnl->get_remote_d_type());
      // srci
      if (srci->is_auth()) {
	auto &pi = srci->project_inode();
	pi.inode.version = srci->pre_dirty();
        spi = &pi.inode;
      }
    } else {
      dout(10) << " will merge remote onto primary link" << dendl;
      if (destdn->is_auth()) {
	auto &pi = oldin->project_inode();
	pi.inode.version = mdr->more()->pvmap[destdn] = destdn->pre_dirty(oldin->inode.version);
        spi = &pi.inode;
      }
    }
  } else { // primary
    if (destdn->is_auth()) {
      version_t oldpv;
      if (srcdn->is_auth())
	oldpv = srci->get_projected_version();
      else {
	oldpv = _rename_prepare_import(mdr, srcdn, client_map_bl);

	// note which dirfrags have child subtrees in the journal
	// event, so that we can open those (as bounds) during replay.
	if (srci->is_dir()) {
	  auto&& ls = srci->get_dirfrags();
	  for (const auto& dir : ls) {
	    if (!dir->is_auth())
	      metablob->renamed_dir_frags.push_back(dir->get_frag());
	  }
	  dout(10) << " noting renamed dir open frags " << metablob->renamed_dir_frags << dendl;
	}
      }
      auto &pi = srci->project_inode(); // project snaprealm if srcdnl->is_primary
                                                 // & srcdnl->snaprealm
      pi.inode.version = mdr->more()->pvmap[destdn] = destdn->pre_dirty(oldpv);
      pi.inode.update_backtrace();
      spi = &pi.inode;
    }
    destdn->push_projected_linkage(srci);
  }

  // src
  if (srcdn->is_auth())
    mdr->more()->pvmap[srcdn] = srcdn->pre_dirty();
  srcdn->push_projected_linkage();  // push null linkage

  if (!silent) {
    if (spi) {
      spi->ctime = mdr->get_op_stamp();
      if (mdr->get_op_stamp() > spi->rstat.rctime)
	spi->rstat.rctime = mdr->get_op_stamp();
      spi->change_attr++;
      if (linkmerge)
	spi->nlink--;
    }
    if (tpi) {
      tpi->ctime = mdr->get_op_stamp();
      if (mdr->get_op_stamp() > tpi->rstat.rctime)
	tpi->rstat.rctime = mdr->get_op_stamp();
      tpi->change_attr++;
      {
        std::string t;
        destdn->make_path_string(t, true);
        tpi->stray_prior_path = std::move(t);
      }
      tpi->nlink--;
      if (tpi->nlink == 0)
	oldin->state_set(CInode::STATE_ORPHAN);
    }
  }

  // prepare nesting, mtime updates
  int predirty_dir = silent ? 0:PREDIRTY_DIR;
  
  // guarantee stray dir is processed first during journal replay. unlink the old inode,
  // then link the source inode to destdn
  if (destdnl->is_primary()) {
    ceph_assert(straydn);
    if (straydn->is_auth()) {
      metablob->add_dir_context(straydn->get_dir());
      metablob->add_dir(straydn->get_dir(), true);
    }
  }

  // sub off target
  if (destdn->is_auth() && !destdnl->is_null()) {
    mdcache->predirty_journal_parents(mdr, metablob, oldin, destdn->get_dir(),
				      (destdnl->is_primary() ? PREDIRTY_PRIMARY:0)|predirty_dir, -1);
    if (destdnl->is_primary()) {
      ceph_assert(straydn);
      mdcache->predirty_journal_parents(mdr, metablob, oldin, straydn->get_dir(),
					PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
    }
  }
  
  // move srcdn
  int predirty_primary = (srcdnl->is_primary() && srcdn->get_dir() != destdn->get_dir()) ? PREDIRTY_PRIMARY:0;
  int flags = predirty_dir | predirty_primary;
  if (srcdn->is_auth())
    mdcache->predirty_journal_parents(mdr, metablob, srci, srcdn->get_dir(), PREDIRTY_SHALLOW|flags, -1);
  if (destdn->is_auth())
    mdcache->predirty_journal_parents(mdr, metablob, srci, destdn->get_dir(), flags, 1);

  // add it all to the metablob
  // target inode
  if (!linkmerge) {
    if (destdnl->is_primary()) {
      ceph_assert(straydn);
      if (destdn->is_auth()) {
	// project snaprealm, too
	if (auto& desti_srnode = mdr->more()->desti_srnode) {
	  oldin->project_snaprealm(desti_srnode);
	  if (tpi->nlink == 0)
	    ceph_assert(!desti_srnode->is_parent_global());
	  desti_srnode = NULL;
	}
	straydn->first = mdcache->get_global_snaprealm()->get_newest_seq() + 1;
	metablob->add_primary_dentry(straydn, oldin, true, true);
      } else if (force_journal_stray) {
	dout(10) << " forced journaling straydn " << *straydn << dendl;
	metablob->add_dir_context(straydn->get_dir());
	metablob->add_primary_dentry(straydn, oldin, true);
      }
    } else if (destdnl->is_remote()) {
      if (oldin->is_auth()) {
	sr_t *new_srnode = NULL;
	if (mdr->slave_request) {
	  if (mdr->slave_request->desti_snapbl.length() > 0) {
	    new_srnode = new sr_t();
	    auto p = mdr->slave_request->desti_snapbl.cbegin();
	    decode(*new_srnode, p);
	  }
	} else if (auto& desti_srnode = mdr->more()->desti_srnode) {
	  new_srnode = desti_srnode;
	  desti_srnode = NULL;
	}
	if (new_srnode) {
	  oldin->project_snaprealm(new_srnode);
	  if (tpi->nlink == 0)
	    ceph_assert(!new_srnode->is_parent_global());
	}
	// auth for targeti
	metablob->add_dir_context(oldin->get_projected_parent_dir());
	mdcache->journal_cow_dentry(mdr.get(), metablob, oldin->get_projected_parent_dn(),
				    CEPH_NOSNAP, 0, destdnl);
	metablob->add_primary_dentry(oldin->get_projected_parent_dn(), oldin, true);
      }
    }
  }

  // dest
  if (srcdnl->is_remote()) {
    ceph_assert(!linkmerge);
    if (destdn->is_auth() && !destdnl->is_null())
      mdcache->journal_cow_dentry(mdr.get(), metablob, destdn, CEPH_NOSNAP, 0, destdnl);
    else
      destdn->first = mdcache->get_global_snaprealm()->get_newest_seq() + 1;

    if (destdn->is_auth())
      metablob->add_remote_dentry(destdn, true, srcdnl->get_remote_ino(), srcdnl->get_remote_d_type());

    if (srci->is_auth() ) { // it's remote
      if (mdr->slave_request) {
	if (mdr->slave_request->srci_snapbl.length() > 0) {
	  sr_t *new_srnode = new sr_t();
	  auto p = mdr->slave_request->srci_snapbl.cbegin();
	  decode(*new_srnode, p);
	  srci->project_snaprealm(new_srnode);
	}
      } else if (auto& srci_srnode = mdr->more()->srci_srnode) {
	srci->project_snaprealm(srci_srnode);
	srci_srnode = NULL;
      }

      CDentry *srci_pdn = srci->get_projected_parent_dn();
      metablob->add_dir_context(srci_pdn->get_dir());
      mdcache->journal_cow_dentry(mdr.get(), metablob, srci_pdn, CEPH_NOSNAP, 0, srcdnl);
      metablob->add_primary_dentry(srci_pdn, srci, true);
    }
  } else if (srcdnl->is_primary()) {
    // project snap parent update?
    if (destdn->is_auth()) {
      if (auto& srci_srnode = mdr->more()->srci_srnode) {
	srci->project_snaprealm(srci_srnode);
	srci_srnode = NULL;
      }
    }
    
    if (destdn->is_auth() && !destdnl->is_null())
      mdcache->journal_cow_dentry(mdr.get(), metablob, destdn, CEPH_NOSNAP, 0, destdnl);

    destdn->first = mdcache->get_global_snaprealm()->get_newest_seq() + 1;

    if (destdn->is_auth())
      metablob->add_primary_dentry(destdn, srci, true, true);
    else if (force_journal_dest) {
      dout(10) << " forced journaling destdn " << *destdn << dendl;
      metablob->add_dir_context(destdn->get_dir());
      metablob->add_primary_dentry(destdn, srci, true);
      if (srcdn->is_auth() && srci->is_dir()) {
	// journal new subtrees root dirfrags
	auto&& ls = srci->get_dirfrags();
	for (const auto& dir : ls) {
	  if (dir->is_auth())
	    metablob->add_dir(dir, true);
	}
      }
    }
  }
    
  // src
  if (srcdn->is_auth()) {
    dout(10) << " journaling srcdn " << *srcdn << dendl;
    mdcache->journal_cow_dentry(mdr.get(), metablob, srcdn, CEPH_NOSNAP, 0, srcdnl);
    // also journal the inode in case we need do slave rename rollback. It is Ok to add
    // both primary and NULL dentries. Because during journal replay, null dentry is
    // processed after primary dentry.
    if (srcdnl->is_primary() && !srci->is_dir() && !destdn->is_auth())
      metablob->add_primary_dentry(srcdn, srci, true);
    metablob->add_null_dentry(srcdn, true);
  } else
    dout(10) << " NOT journaling srcdn " << *srcdn << dendl;

  // make renamed inode first track the dn
  if (srcdnl->is_primary() && destdn->is_auth()) {
    ceph_assert(srci->first <= destdn->first);
    srci->first = destdn->first;
  }
  // make stray inode first track the straydn
  if (straydn && straydn->is_auth()) {
    ceph_assert(oldin->first <= straydn->first);
    oldin->first = straydn->first;
  }

  if (oldin && oldin->is_dir()) {
    ceph_assert(straydn);
    mdcache->project_subtree_rename(oldin, destdn->get_dir(), straydn->get_dir());
  }
  if (srci->is_dir())
    mdcache->project_subtree_rename(srci, srcdn->get_dir(), destdn->get_dir());

}


void Server::_rename_apply(MDRequestRef& mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_apply " << *mdr << " " << *srcdn << " " << *destdn << dendl;
  dout(10) << " pvs " << mdr->more()->pvmap << dendl;

  CDentry::linkage_t *srcdnl = srcdn->get_linkage();
  CDentry::linkage_t *destdnl = destdn->get_linkage();

  CInode *oldin = destdnl->get_inode();

  // primary+remote link merge?
  bool linkmerge = (srcdnl->get_inode() == oldin);
  if (linkmerge)
    ceph_assert(srcdnl->is_primary() || destdnl->is_remote());

  bool new_in_snaprealm = false;
  bool new_oldin_snaprealm = false;

  // target inode
  if (!linkmerge) {
    if (destdnl->is_primary()) {
      ceph_assert(straydn);
      dout(10) << "straydn is " << *straydn << dendl;

      // if there is newly created snaprealm, need to split old snaprealm's
      // inodes_with_caps. So pop snaprealm before linkage changes.
      if (destdn->is_auth()) {
	bool hadrealm = (oldin->snaprealm ? true : false);
	oldin->early_pop_projected_snaprealm();
	new_oldin_snaprealm = (oldin->snaprealm && !hadrealm);
      } else {
	ceph_assert(mdr->slave_request);
	if (mdr->slave_request->desti_snapbl.length()) {
	  new_oldin_snaprealm = !oldin->snaprealm;
	  oldin->decode_snap_blob(mdr->slave_request->desti_snapbl);
	  ceph_assert(oldin->snaprealm);
	  ceph_assert(oldin->snaprealm->have_past_parents_open());
	}
      }

      destdn->get_dir()->unlink_inode(destdn, false);

      straydn->pop_projected_linkage();
      if (mdr->is_slave() && !mdr->more()->slave_update_journaled)
	ceph_assert(!straydn->is_projected()); // no other projected

      // nlink-- targeti
      if (destdn->is_auth())
	oldin->pop_and_dirty_projected_inode(mdr->ls);

      mdcache->touch_dentry_bottom(straydn);  // drop dn as quickly as possible.
    } else if (destdnl->is_remote()) {
      destdn->get_dir()->unlink_inode(destdn, false);
      if (oldin->is_auth()) {
	oldin->pop_and_dirty_projected_inode(mdr->ls);
      } else if (mdr->slave_request) {
	if (mdr->slave_request->desti_snapbl.length() > 0) {
	  ceph_assert(oldin->snaprealm);
	  oldin->decode_snap_blob(mdr->slave_request->desti_snapbl);
	}
      } else if (auto& desti_srnode = mdr->more()->desti_srnode) {
	delete desti_srnode;
	desti_srnode = NULL;
      }
    }
  }

  // unlink src before we relink it at dest
  CInode *in = srcdnl->get_inode();
  ceph_assert(in);

  bool srcdn_was_remote = srcdnl->is_remote();
  if (!srcdn_was_remote) {
    // if there is newly created snaprealm, need to split old snaprealm's
    // inodes_with_caps. So pop snaprealm before linkage changes.
    if (destdn->is_auth()) {
      bool hadrealm = (in->snaprealm ? true : false);
      in->early_pop_projected_snaprealm();
      new_in_snaprealm = (in->snaprealm && !hadrealm);
    } else {
      ceph_assert(mdr->slave_request);
      if (mdr->slave_request->srci_snapbl.length()) {
	new_in_snaprealm = !in->snaprealm;
	in->decode_snap_blob(mdr->slave_request->srci_snapbl);
	ceph_assert(in->snaprealm);
	ceph_assert(in->snaprealm->have_past_parents_open());
      }
    }
  }

  srcdn->get_dir()->unlink_inode(srcdn);

  // dest
  if (srcdn_was_remote) {
    if (!linkmerge) {
      // destdn
      destdnl = destdn->pop_projected_linkage();
      if (mdr->is_slave() && !mdr->more()->slave_update_journaled)
	ceph_assert(!destdn->is_projected()); // no other projected

      destdn->link_remote(destdnl, in);
      if (destdn->is_auth())
	destdn->mark_dirty(mdr->more()->pvmap[destdn], mdr->ls);
      // in
      if (in->is_auth()) {
	in->pop_and_dirty_projected_inode(mdr->ls);
      } else if (mdr->slave_request) {
	if (mdr->slave_request->srci_snapbl.length() > 0) {
	  ceph_assert(in->snaprealm);
	  in->decode_snap_blob(mdr->slave_request->srci_snapbl);
	}
      } else if (auto& srci_srnode = mdr->more()->srci_srnode) {
	delete srci_srnode;
	srci_srnode = NULL;
      }
    } else {
      dout(10) << "merging remote onto primary link" << dendl;
      oldin->pop_and_dirty_projected_inode(mdr->ls);
    }
  } else { // primary
    if (linkmerge) {
      dout(10) << "merging primary onto remote link" << dendl;
      destdn->get_dir()->unlink_inode(destdn, false);
    }
    destdnl = destdn->pop_projected_linkage();
    if (mdr->is_slave() && !mdr->more()->slave_update_journaled)
      ceph_assert(!destdn->is_projected()); // no other projected

    // srcdn inode import?
    if (!srcdn->is_auth() && destdn->is_auth()) {
      ceph_assert(mdr->more()->inode_import.length() > 0);

      map<client_t,Capability::Import> imported_caps;
      
      // finish cap imports
      finish_force_open_sessions(mdr->more()->imported_session_map);
      if (mdr->more()->cap_imports.count(destdnl->get_inode())) {
	mdcache->migrator->finish_import_inode_caps(destdnl->get_inode(),
						    mdr->more()->srcdn_auth_mds, true,
						    mdr->more()->imported_session_map,
						    mdr->more()->cap_imports[destdnl->get_inode()],
						    imported_caps);
      }

      mdr->more()->inode_import.clear();
      encode(imported_caps, mdr->more()->inode_import);

      /* hack: add an auth pin for each xlock we hold. These were
       * remote xlocks previously but now they're local and
       * we're going to try and unpin when we xlock_finish. */

      for (auto i = mdr->locks.lower_bound(&destdnl->get_inode()->versionlock);
	   i !=  mdr->locks.end();
	   ++i) {
	SimpleLock *lock = i->lock;
	if (lock->get_parent() != destdnl->get_inode())
	  break;
	if (i->is_xlock() && !lock->is_locallock())
	  mds->locker->xlock_import(lock);
      }
      
      // hack: fix auth bit
      in->state_set(CInode::STATE_AUTH);

      mdr->clear_ambiguous_auth();
    }

    if (destdn->is_auth())
      in->pop_and_dirty_projected_inode(mdr->ls);
  }

  // src
  if (srcdn->is_auth())
    srcdn->mark_dirty(mdr->more()->pvmap[srcdn], mdr->ls);
  srcdn->pop_projected_linkage();
  if (mdr->is_slave() && !mdr->more()->slave_update_journaled)
    ceph_assert(!srcdn->is_projected()); // no other projected
  
  // apply remaining projected inodes (nested)
  mdr->apply();

  // update subtree map?
  if (destdnl->is_primary() && in->is_dir())
    mdcache->adjust_subtree_after_rename(in, srcdn->get_dir(), true);

  if (straydn && oldin->is_dir())
    mdcache->adjust_subtree_after_rename(oldin, destdn->get_dir(), true);

  if (new_oldin_snaprealm)
    mdcache->do_realm_invalidate_and_update_notify(oldin, CEPH_SNAP_OP_SPLIT, false);
  if (new_in_snaprealm)
    mdcache->do_realm_invalidate_and_update_notify(in, CEPH_SNAP_OP_SPLIT, true);

  // removing a new dn?
  if (srcdn->is_auth())
    srcdn->get_dir()->try_remove_unlinked_dn(srcdn);
}



// ------------
// SLAVE

class C_MDS_SlaveRenamePrep : public ServerLogContext {
  CDentry *srcdn, *destdn, *straydn;
public:
  C_MDS_SlaveRenamePrep(Server *s, MDRequestRef& m, CDentry *sr, CDentry *de, CDentry *st) :
    ServerLogContext(s, m), srcdn(sr), destdn(de), straydn(st) {}
  void finish(int r) override {
    server->_logged_slave_rename(mdr, srcdn, destdn, straydn);
  }
};

class C_MDS_SlaveRenameCommit : public ServerContext {
  MDRequestRef mdr;
  CDentry *srcdn, *destdn, *straydn;
public:
  C_MDS_SlaveRenameCommit(Server *s, MDRequestRef& m, CDentry *sr, CDentry *de, CDentry *st) :
    ServerContext(s), mdr(m), srcdn(sr), destdn(de), straydn(st) {}
  void finish(int r) override {
    server->_commit_slave_rename(mdr, r, srcdn, destdn, straydn);
  }
};

class C_MDS_SlaveRenameSessionsFlushed : public ServerContext {
  MDRequestRef mdr;
public:
  C_MDS_SlaveRenameSessionsFlushed(Server *s, MDRequestRef& r) :
    ServerContext(s), mdr(r) {}
  void finish(int r) override {
    server->_slave_rename_sessions_flushed(mdr);
  }
};

void Server::handle_slave_rename_prep(MDRequestRef& mdr)
{
  dout(10) << "handle_slave_rename_prep " << *mdr 
	   << " " << mdr->slave_request->srcdnpath 
	   << " to " << mdr->slave_request->destdnpath
	   << dendl;

  if (mdr->slave_request->is_interrupted()) {
    dout(10) << " slave request interrupted, sending noop reply" << dendl;
    auto reply = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_RENAMEPREPACK);
    reply->mark_interrupted();
    mds->send_message_mds(reply, mdr->slave_to_mds);
    mdr->reset_slave_request();
    return;
  }

  // discover destdn
  filepath destpath(mdr->slave_request->destdnpath);
  dout(10) << " dest " << destpath << dendl;
  vector<CDentry*> trace;
  CF_MDS_MDRContextFactory cf(mdcache, mdr, false);
  int r = mdcache->path_traverse(mdr, cf, destpath,
				 MDS_TRAVERSE_DISCOVER | MDS_TRAVERSE_PATH_LOCKED | MDS_TRAVERSE_WANT_DENTRY,
				 &trace);
  if (r > 0) return;
  if (r == -ESTALE) {
    mdcache->find_ino_peers(destpath.get_ino(), new C_MDS_RetryRequest(mdcache, mdr),
			    mdr->slave_to_mds, true);
    return;
  }
  ceph_assert(r == 0);  // we shouldn't get an error here!
      
  CDentry *destdn = trace.back();
  CDentry::linkage_t *destdnl = destdn->get_projected_linkage();
  dout(10) << " destdn " << *destdn << dendl;
  mdr->pin(destdn);
  
  // discover srcdn
  filepath srcpath(mdr->slave_request->srcdnpath);
  dout(10) << " src " << srcpath << dendl;
  CInode *srci = nullptr;
  r = mdcache->path_traverse(mdr, cf, srcpath,
			     MDS_TRAVERSE_DISCOVER | MDS_TRAVERSE_PATH_LOCKED,
			     &trace, &srci);
  if (r > 0) return;
  ceph_assert(r == 0);

  CDentry *srcdn = trace.back();
  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
  dout(10) << " srcdn " << *srcdn << dendl;
  mdr->pin(srcdn);
  mdr->pin(srci);

  // stray?
  bool linkmerge = srcdnl->get_inode() == destdnl->get_inode();
  if (linkmerge)
    ceph_assert(srcdnl->is_primary() && destdnl->is_remote());
  CDentry *straydn = mdr->straydn;
  if (destdnl->is_primary() && !linkmerge)
    ceph_assert(straydn);

  mdr->set_op_stamp(mdr->slave_request->op_stamp);
  mdr->more()->srcdn_auth_mds = srcdn->authority().first;

  // set up commit waiter (early, to clean up any freezing etc we do)
  if (!mdr->more()->slave_commit)
    mdr->more()->slave_commit = new C_MDS_SlaveRenameCommit(this, mdr, srcdn, destdn, straydn);

  // am i srcdn auth?
  if (srcdn->is_auth()) {
    set<mds_rank_t> srcdnrep;
    srcdn->list_replicas(srcdnrep);

    bool reply_witness = false;
    if (srcdnl->is_primary() && !srcdnl->get_inode()->state_test(CInode::STATE_AMBIGUOUSAUTH)) {
      // freeze?
      // we need this to
      //  - avoid conflicting lock state changes
      //  - avoid concurrent updates to the inode
      //     (this could also be accomplished with the versionlock)
      int allowance = 3; // 1 for the mdr auth_pin, 1 for the link lock, 1 for the snap lock
      dout(10) << " freezing srci " << *srcdnl->get_inode() << " with allowance " << allowance << dendl;
      bool frozen_inode = srcdnl->get_inode()->freeze_inode(allowance);

      // unfreeze auth pin after freezing the inode to avoid queueing waiters
      if (srcdnl->get_inode()->is_frozen_auth_pin())
	mdr->unfreeze_auth_pin();

      if (!frozen_inode) {
	srcdnl->get_inode()->add_waiter(CInode::WAIT_FROZEN, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }

      /*
       * set ambiguous auth for srci
       * NOTE: we don't worry about ambiguous cache expire as we do
       * with subtree migrations because all slaves will pin
       * srcdn->get_inode() for duration of this rename.
       */
      mdr->set_ambiguous_auth(srcdnl->get_inode());

      // just mark the source inode as ambiguous auth if more than two MDS are involved.
      // the leader will send another OP_RENAMEPREP slave request later.
      if (mdr->slave_request->witnesses.size() > 1) {
	dout(10) << " set srci ambiguous auth; providing srcdn replica list" << dendl;
	reply_witness = true;
      }

      // make sure bystanders have received all lock related messages
      for (set<mds_rank_t>::iterator p = srcdnrep.begin(); p != srcdnrep.end(); ++p) {
	if (*p == mdr->slave_to_mds ||
	    (mds->is_cluster_degraded() &&
	     !mds->mdsmap->is_clientreplay_or_active_or_stopping(*p)))
	  continue;
	auto notify = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_RENAMENOTIFY);
	mds->send_message_mds(notify, *p);
	mdr->more()->waiting_on_slave.insert(*p);
      }

      // make sure clients have received all cap related messages
      set<client_t> export_client_set;
      mdcache->migrator->get_export_client_set(srcdnl->get_inode(), export_client_set);

      MDSGatherBuilder gather(g_ceph_context);
      flush_client_sessions(export_client_set, gather);
      if (gather.has_subs()) {
	mdr->more()->waiting_on_slave.insert(MDS_RANK_NONE);
	gather.set_finisher(new C_MDS_SlaveRenameSessionsFlushed(this, mdr));
	gather.activate();
      }
    }

    // is witness list sufficient?
    for (set<mds_rank_t>::iterator p = srcdnrep.begin(); p != srcdnrep.end(); ++p) {
      if (*p == mdr->slave_to_mds ||
	  mdr->slave_request->witnesses.count(*p)) continue;
      dout(10) << " witness list insufficient; providing srcdn replica list" << dendl;
      reply_witness = true;
      break;
    }

    if (reply_witness) {
      ceph_assert(!srcdnrep.empty());
      auto reply = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_RENAMEPREPACK);
      reply->witnesses.swap(srcdnrep);
      mds->send_message_mds(reply, mdr->slave_to_mds);
      mdr->reset_slave_request();
      return;	
    }
    dout(10) << " witness list sufficient: includes all srcdn replicas" << dendl;
    if (!mdr->more()->waiting_on_slave.empty()) {
      dout(10) << " still waiting for rename notify acks from "
	       << mdr->more()->waiting_on_slave << dendl;
      return;
    }
  } else if (srcdnl->is_primary() && srcdn->authority() != destdn->authority()) {
    // set ambiguous auth for srci on witnesses
    mdr->set_ambiguous_auth(srcdnl->get_inode());
  }

  // encode everything we'd need to roll this back... basically, just the original state.
  rename_rollback rollback;
  
  rollback.reqid = mdr->reqid;
  
  rollback.orig_src.dirfrag = srcdn->get_dir()->dirfrag();
  rollback.orig_src.dirfrag_old_mtime = srcdn->get_dir()->get_projected_fnode()->fragstat.mtime;
  rollback.orig_src.dirfrag_old_rctime = srcdn->get_dir()->get_projected_fnode()->rstat.rctime;
  rollback.orig_src.dname = srcdn->get_name();
  if (srcdnl->is_primary())
    rollback.orig_src.ino = srcdnl->get_inode()->ino();
  else {
    ceph_assert(srcdnl->is_remote());
    rollback.orig_src.remote_ino = srcdnl->get_remote_ino();
    rollback.orig_src.remote_d_type = srcdnl->get_remote_d_type();
  }
  
  rollback.orig_dest.dirfrag = destdn->get_dir()->dirfrag();
  rollback.orig_dest.dirfrag_old_mtime = destdn->get_dir()->get_projected_fnode()->fragstat.mtime;
  rollback.orig_dest.dirfrag_old_rctime = destdn->get_dir()->get_projected_fnode()->rstat.rctime;
  rollback.orig_dest.dname = destdn->get_name();
  if (destdnl->is_primary())
    rollback.orig_dest.ino = destdnl->get_inode()->ino();
  else if (destdnl->is_remote()) {
    rollback.orig_dest.remote_ino = destdnl->get_remote_ino();
    rollback.orig_dest.remote_d_type = destdnl->get_remote_d_type();
  }
  
  if (straydn) {
    rollback.stray.dirfrag = straydn->get_dir()->dirfrag();
    rollback.stray.dirfrag_old_mtime = straydn->get_dir()->get_projected_fnode()->fragstat.mtime;
    rollback.stray.dirfrag_old_rctime = straydn->get_dir()->get_projected_fnode()->rstat.rctime;
    rollback.stray.dname = straydn->get_name();
  }
  if (mdr->slave_request->desti_snapbl.length()) {
    CInode *oldin = destdnl->get_inode();
    if (oldin->snaprealm) {
      encode(true, rollback.desti_snapbl);
      oldin->encode_snap_blob(rollback.desti_snapbl);
    } else {
      encode(false, rollback.desti_snapbl);
    }
  }
  if (mdr->slave_request->srci_snapbl.length()) {
    if (srci->snaprealm) {
      encode(true, rollback.srci_snapbl);
      srci->encode_snap_blob(rollback.srci_snapbl);
    } else {
      encode(false, rollback.srci_snapbl);
    }
  }
  encode(rollback, mdr->more()->rollback_bl);
  // FIXME: rollback snaprealm
  dout(20) << " rollback is " << mdr->more()->rollback_bl.length() << " bytes" << dendl;

  // journal.
  mdr->ls = mdlog->get_current_segment();
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_prep", mdr->reqid, mdr->slave_to_mds,
				      ESlaveUpdate::OP_PREPARE, ESlaveUpdate::RENAME);
  mdlog->start_entry(le);
  le->rollback = mdr->more()->rollback_bl;
  
  bufferlist blah;  // inode import data... obviously not used if we're the slave
  _rename_prepare(mdr, &le->commit, &blah, srcdn, destdn, straydn);

  if (le->commit.empty()) {
    dout(10) << " empty metablob, skipping journal" << dendl;
    mdlog->cancel_entry(le);
    mdr->ls = NULL;
    _logged_slave_rename(mdr, srcdn, destdn, straydn);
  } else {
    mdcache->add_uncommitted_slave(mdr->reqid, mdr->ls, mdr->slave_to_mds);
    mdr->more()->slave_update_journaled = true;
    submit_mdlog_entry(le, new C_MDS_SlaveRenamePrep(this, mdr, srcdn, destdn, straydn),
		       mdr, __func__);
    mdlog->flush();
  }
}

void Server::_logged_slave_rename(MDRequestRef& mdr,
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_logged_slave_rename " << *mdr << dendl;

  // prepare ack
  ref_t<MMDSSlaveRequest> reply;
  if (!mdr->aborted) {
    reply = make_message<MMDSSlaveRequest>(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_RENAMEPREPACK);
    if (!mdr->more()->slave_update_journaled)
      reply->mark_not_journaled();
  }

  CDentry::linkage_t *srcdnl = srcdn->get_linkage();
  //CDentry::linkage_t *straydnl = straydn ? straydn->get_linkage() : 0;

  // export srci?
  if (srcdn->is_auth() && srcdnl->is_primary()) {
    // set export bounds for CInode::encode_export()
    if (reply) {
      std::vector<CDir*> bounds;
      if (srcdnl->get_inode()->is_dir()) {
	srcdnl->get_inode()->get_dirfrags(bounds);
	for (const auto& bound : bounds) {
	  bound->state_set(CDir::STATE_EXPORTBOUND);
        }
      }

      map<client_t,entity_inst_t> exported_client_map;
      map<client_t, client_metadata_t> exported_client_metadata_map;
      bufferlist inodebl;
      mdcache->migrator->encode_export_inode(srcdnl->get_inode(), inodebl,
					     exported_client_map,
					     exported_client_metadata_map);

      for (const auto& bound : bounds) {
	bound->state_clear(CDir::STATE_EXPORTBOUND);
      }

      encode(exported_client_map, reply->inode_export, mds->mdsmap->get_up_features());
      encode(exported_client_metadata_map, reply->inode_export);
      reply->inode_export.claim_append(inodebl);
      reply->inode_export_v = srcdnl->get_inode()->inode.version;
    }

    // remove mdr auth pin
    mdr->auth_unpin(srcdnl->get_inode());
    mdr->more()->is_inode_exporter = true;

    if (srcdnl->get_inode()->is_dirty())
      srcdnl->get_inode()->mark_clean();

    dout(10) << " exported srci " << *srcdnl->get_inode() << dendl;
  }

  // apply
  _rename_apply(mdr, srcdn, destdn, straydn);   

  CDentry::linkage_t *destdnl = destdn->get_linkage();

  // bump popularity
  mds->balancer->hit_dir(srcdn->get_dir(), META_POP_IWR);
  if (destdnl->get_inode() && destdnl->get_inode()->is_auth())
    mds->balancer->hit_inode(destdnl->get_inode(), META_POP_IWR);

  // done.
  mdr->reset_slave_request();
  mdr->straydn = 0;

  if (reply) {
    mds->send_message_mds(reply, mdr->slave_to_mds);
  } else {
    ceph_assert(mdr->aborted);
    dout(10) << " abort flag set, finishing" << dendl;
    mdcache->request_finish(mdr);
  }
}

void Server::_commit_slave_rename(MDRequestRef& mdr, int r,
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_commit_slave_rename " << *mdr << " r=" << r << dendl;

  CInode *in = destdn->get_linkage()->get_inode();

  inodeno_t migrated_stray;
  if (srcdn->is_auth() && srcdn->get_dir()->inode->is_stray())
    migrated_stray = in->ino();

  MDSContext::vec finished;
  if (r == 0) {
    // unfreeze+singleauth inode
    //  hmm, do i really need to delay this?
    if (mdr->more()->is_inode_exporter) {
      // drop our pins
      // we exported, clear out any xlocks that we moved to another MDS

      for (auto i = mdr->locks.lower_bound(&in->versionlock);
	   i !=  mdr->locks.end(); ) {
	SimpleLock *lock = i->lock;
	if (lock->get_parent() != in)
	  break;
	// we only care about xlocks on the exported inode
	if (i->is_xlock() && !lock->is_locallock())
	  mds->locker->xlock_export(i++, mdr.get());
	else
	  ++i;
      }

      map<client_t,Capability::Import> peer_imported;
      auto bp = mdr->more()->inode_import.cbegin();
      decode(peer_imported, bp);

      dout(10) << " finishing inode export on " << *in << dendl;
      mdcache->migrator->finish_export_inode(in, mdr->slave_to_mds, peer_imported, finished);
      mds->queue_waiters(finished);   // this includes SINGLEAUTH waiters.

      // unfreeze
      ceph_assert(in->is_frozen_inode());
      in->unfreeze_inode(finished);
    }

    // singleauth
    if (mdr->more()->is_ambiguous_auth) {
      mdr->more()->rename_inode->clear_ambiguous_auth(finished);
      mdr->more()->is_ambiguous_auth = false;
    }

    if (straydn && mdr->more()->slave_update_journaled) {
      CInode *strayin = straydn->get_projected_linkage()->get_inode();
      if (strayin && !strayin->snaprealm)
	mdcache->clear_dirty_bits_for_stray(strayin);
    }

    mds->queue_waiters(finished);
    mdr->cleanup();

    if (mdr->more()->slave_update_journaled) {
      // write a commit to the journal
      ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_commit", mdr->reqid,
					  mdr->slave_to_mds, ESlaveUpdate::OP_COMMIT,
					  ESlaveUpdate::RENAME);
      mdlog->start_entry(le);
      submit_mdlog_entry(le, new C_MDS_CommittedSlave(this, mdr), mdr, __func__);
      mdlog->flush();
    } else {
      _committed_slave(mdr);
    }
  } else {

    // abort
    //  rollback_bl may be empty if we froze the inode but had to provide an expanded
    // witness list from the leader, and they failed before we tried prep again.
    if (mdr->more()->rollback_bl.length()) {
      if (mdr->more()->is_inode_exporter) {
	dout(10) << " reversing inode export of " << *in << dendl;
	in->abort_export();
      }
      if (mdcache->is_ambiguous_slave_update(mdr->reqid, mdr->slave_to_mds)) {
	mdcache->remove_ambiguous_slave_update(mdr->reqid, mdr->slave_to_mds);
	// rollback but preserve the slave request
	do_rename_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr, false);
	mdr->more()->rollback_bl.clear();
      } else
	do_rename_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr, true);
    } else {
      dout(10) << " rollback_bl empty, not rollback back rename (leader failed after getting extra witnesses?)" << dendl;
      // singleauth
      if (mdr->more()->is_ambiguous_auth) {
	if (srcdn->is_auth())
	  mdr->more()->rename_inode->unfreeze_inode(finished);

	mdr->more()->rename_inode->clear_ambiguous_auth(finished);
	mdr->more()->is_ambiguous_auth = false;
      }
      mds->queue_waiters(finished);
      mdcache->request_finish(mdr);
    }
  }

  if (migrated_stray && mds->is_stopping())
    mdcache->shutdown_export_stray_finish(migrated_stray);
}

void _rollback_repair_dir(MutationRef& mut, CDir *dir, rename_rollback::drec &r, utime_t ctime,
			  bool isdir, int linkunlink, nest_info_t &rstat)
{
  fnode_t *pf;
  pf = dir->project_fnode();
  mut->add_projected_fnode(dir);
  pf->version = dir->pre_dirty();

  if (isdir) {
    pf->fragstat.nsubdirs += linkunlink;
  } else {
    pf->fragstat.nfiles += linkunlink;
  }    
  if (r.ino) {
    pf->rstat.rbytes += linkunlink * rstat.rbytes;
    pf->rstat.rfiles += linkunlink * rstat.rfiles;
    pf->rstat.rsubdirs += linkunlink * rstat.rsubdirs;
    pf->rstat.rsnaps += linkunlink * rstat.rsnaps;
  }
  if (pf->fragstat.mtime == ctime) {
    pf->fragstat.mtime = r.dirfrag_old_mtime;
    if (pf->rstat.rctime == ctime)
      pf->rstat.rctime = r.dirfrag_old_rctime;
  }
  mut->add_updated_lock(&dir->get_inode()->filelock);
  mut->add_updated_lock(&dir->get_inode()->nestlock);
}

struct C_MDS_LoggedRenameRollback : public ServerLogContext {
  MutationRef mut;
  CDentry *srcdn;
  version_t srcdnpv;
  CDentry *destdn;
  CDentry *straydn;
  map<client_t,ref_t<MClientSnap>> splits[2];
  bool finish_mdr;
  C_MDS_LoggedRenameRollback(Server *s, MutationRef& m, MDRequestRef& r,
			     CDentry *sd, version_t pv, CDentry *dd, CDentry *st,
			     map<client_t,ref_t<MClientSnap>> _splits[2], bool f) :
    ServerLogContext(s, r), mut(m), srcdn(sd), srcdnpv(pv), destdn(dd),
    straydn(st), finish_mdr(f) {
      splits[0].swap(_splits[0]);
      splits[1].swap(_splits[1]);
    }
  void finish(int r) override {
    server->_rename_rollback_finish(mut, mdr, srcdn, srcdnpv,
				    destdn, straydn, splits, finish_mdr);
  }
};

void Server::do_rename_rollback(bufferlist &rbl, mds_rank_t leader, MDRequestRef& mdr,
				bool finish_mdr)
{
  rename_rollback rollback;
  auto p = rbl.cbegin();
  decode(rollback, p);

  dout(10) << "do_rename_rollback on " << rollback.reqid << dendl;
  // need to finish this update before sending resolve to claim the subtree
  mdcache->add_rollback(rollback.reqid, leader);

  MutationRef mut(new MutationImpl(nullptr, utime_t(), rollback.reqid));
  mut->ls = mds->mdlog->get_current_segment();

  CDentry *srcdn = NULL;
  CDir *srcdir = mdcache->get_dirfrag(rollback.orig_src.dirfrag);
  if (!srcdir)
    srcdir = mdcache->get_dirfrag(rollback.orig_src.dirfrag.ino, rollback.orig_src.dname);
  if (srcdir) {
    dout(10) << "  srcdir " << *srcdir << dendl;
    srcdn = srcdir->lookup(rollback.orig_src.dname);
    if (srcdn) {
      dout(10) << "   srcdn " << *srcdn << dendl;
      ceph_assert(srcdn->get_linkage()->is_null());
    } else
      dout(10) << "   srcdn not found" << dendl;
  } else
    dout(10) << "  srcdir not found" << dendl;

  CDentry *destdn = NULL;
  CDir *destdir = mdcache->get_dirfrag(rollback.orig_dest.dirfrag);
  if (!destdir)
    destdir = mdcache->get_dirfrag(rollback.orig_dest.dirfrag.ino, rollback.orig_dest.dname);
  if (destdir) {
    dout(10) << " destdir " << *destdir << dendl;
    destdn = destdir->lookup(rollback.orig_dest.dname);
    if (destdn)
      dout(10) << "  destdn " << *destdn << dendl;
    else
      dout(10) << "  destdn not found" << dendl;
  } else
    dout(10) << " destdir not found" << dendl;

  CInode *in = NULL;
  if (rollback.orig_src.ino) {
    in = mdcache->get_inode(rollback.orig_src.ino);
    if (in && in->is_dir())
      ceph_assert(srcdn && destdn);
  } else
    in = mdcache->get_inode(rollback.orig_src.remote_ino);

  CDir *straydir = NULL;
  CDentry *straydn = NULL;
  if (rollback.stray.dirfrag.ino) {
    straydir = mdcache->get_dirfrag(rollback.stray.dirfrag);
    if (straydir) {
      dout(10) << "straydir " << *straydir << dendl;
      straydn = straydir->lookup(rollback.stray.dname);
      if (straydn) {
	dout(10) << " straydn " << *straydn << dendl;
	ceph_assert(straydn->get_linkage()->is_primary());
      } else
	dout(10) << " straydn not found" << dendl;
    } else
      dout(10) << "straydir not found" << dendl;
  }

  CInode *target = NULL;
  if (rollback.orig_dest.ino) {
    target = mdcache->get_inode(rollback.orig_dest.ino);
    if (target)
      ceph_assert(destdn && straydn);
  } else if (rollback.orig_dest.remote_ino)
    target = mdcache->get_inode(rollback.orig_dest.remote_ino);

  // can't use is_auth() in the resolve stage
  mds_rank_t whoami = mds->get_nodeid();
  // slave
  ceph_assert(!destdn || destdn->authority().first != whoami);
  ceph_assert(!straydn || straydn->authority().first != whoami);

  bool force_journal_src = false;
  bool force_journal_dest = false;
  if (in && in->is_dir() && srcdn->authority().first != whoami)
    force_journal_src = _need_force_journal(in, false);
  if (in && target && target->is_dir())
    force_journal_dest = _need_force_journal(in, true);
  
  version_t srcdnpv = 0;
  // repair src
  if (srcdn) {
    if (srcdn->authority().first == whoami)
      srcdnpv = srcdn->pre_dirty();
    if (rollback.orig_src.ino) {
      ceph_assert(in);
      srcdn->push_projected_linkage(in);
    } else
      srcdn->push_projected_linkage(rollback.orig_src.remote_ino,
				    rollback.orig_src.remote_d_type);
  }

  map<client_t,ref_t<MClientSnap>> splits[2];

  CInode::mempool_inode *pip = nullptr;
  if (in) {
    bool projected;
    if (in->get_projected_parent_dn()->authority().first == whoami) {
      auto &pi = in->project_inode();
      pip = &pi.inode;
      mut->add_projected_inode(in);
      pip->version = in->pre_dirty();
      projected = true;
    } else {
      pip = in->get_projected_inode();
      projected = false;
    }
    if (pip->ctime == rollback.ctime)
      pip->ctime = rollback.orig_src.old_ctime;

    if (rollback.srci_snapbl.length() && in->snaprealm) {
      bool hadrealm;
      auto p = rollback.srci_snapbl.cbegin();
      decode(hadrealm, p);
      if (hadrealm) {
	if (projected && !mds->is_resolve()) {
	  sr_t *new_srnode = new sr_t();
	  decode(*new_srnode, p);
	  in->project_snaprealm(new_srnode);
	} else
	  decode(in->snaprealm->srnode, p);
      } else {
	SnapRealm *realm;
	if (rollback.orig_src.ino) {
	  ceph_assert(srcdir);
	  realm = srcdir->get_inode()->find_snaprealm();
	} else {
	  realm = in->snaprealm->parent;
	}
	if (!mds->is_resolve())
	  mdcache->prepare_realm_merge(in->snaprealm, realm, splits[0]);
	if (projected)
	  in->project_snaprealm(NULL);
	else
	  in->snaprealm->merge_to(realm);
      }
    }
  }

  if (srcdn && srcdn->authority().first == whoami) {
    nest_info_t blah;
    _rollback_repair_dir(mut, srcdir, rollback.orig_src, rollback.ctime,
			 in ? in->is_dir() : false, 1, pip ? pip->accounted_rstat : blah);
  }

  // repair dest
  if (destdn) {
    if (rollback.orig_dest.ino && target) {
      destdn->push_projected_linkage(target);
    } else if (rollback.orig_dest.remote_ino) {
      destdn->push_projected_linkage(rollback.orig_dest.remote_ino,
				     rollback.orig_dest.remote_d_type);
    } else {
      // the dentry will be trimmed soon, it's ok to have wrong linkage
      if (rollback.orig_dest.ino)
	ceph_assert(mds->is_resolve());
      destdn->push_projected_linkage();
    }
  }

  if (straydn)
    straydn->push_projected_linkage();

  if (target) {
    bool projected;
    CInode::mempool_inode *ti = nullptr;
    if (target->get_projected_parent_dn()->authority().first == whoami) {
      auto &pi = target->project_inode();
      ti = &pi.inode;
      mut->add_projected_inode(target);
      ti->version = target->pre_dirty();
      projected = true;
    } else {
      ti = target->get_projected_inode();
      projected = false;
    }
    if (ti->ctime == rollback.ctime)
      ti->ctime = rollback.orig_dest.old_ctime;
    if (MDS_INO_IS_STRAY(rollback.orig_src.dirfrag.ino)) {
      if (MDS_INO_IS_STRAY(rollback.orig_dest.dirfrag.ino))
	ceph_assert(!rollback.orig_dest.ino && !rollback.orig_dest.remote_ino);
      else
	ceph_assert(rollback.orig_dest.remote_ino &&
	       rollback.orig_dest.remote_ino == rollback.orig_src.ino);
    } else
      ti->nlink++;

    if (rollback.desti_snapbl.length() && target->snaprealm) {
      bool hadrealm;
      auto p = rollback.desti_snapbl.cbegin();
      decode(hadrealm, p);
      if (hadrealm) {
	if (projected && !mds->is_resolve()) {
	  sr_t *new_srnode = new sr_t();
	  decode(*new_srnode, p);
	  target->project_snaprealm(new_srnode);
	} else
	  decode(target->snaprealm->srnode, p);
      } else {
	SnapRealm *realm;
	if (rollback.orig_dest.ino) {
	  ceph_assert(destdir);
	  realm = destdir->get_inode()->find_snaprealm();
	} else {
	  realm = target->snaprealm->parent;
	}
	if (!mds->is_resolve())
	  mdcache->prepare_realm_merge(target->snaprealm, realm, splits[1]);
	if (projected)
	  target->project_snaprealm(NULL);
	else
	  target->snaprealm->merge_to(realm);
      }
    }
  }

  if (srcdn)
    dout(0) << " srcdn back to " << *srcdn << dendl;
  if (in)
    dout(0) << "  srci back to " << *in << dendl;
  if (destdn)
    dout(0) << " destdn back to " << *destdn << dendl;
  if (target)
    dout(0) << "  desti back to " << *target << dendl;
  
  // journal it
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_rollback", rollback.reqid, leader,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::RENAME);
  mdlog->start_entry(le);

  if (srcdn && (srcdn->authority().first == whoami || force_journal_src)) {
    le->commit.add_dir_context(srcdir);
    if (rollback.orig_src.ino)
      le->commit.add_primary_dentry(srcdn, 0, true);
    else
      le->commit.add_remote_dentry(srcdn, true);
  }

  if (!rollback.orig_src.ino && // remote linkage
      in && in->authority().first == whoami) {
    le->commit.add_dir_context(in->get_projected_parent_dir());
    le->commit.add_primary_dentry(in->get_projected_parent_dn(), in, true);
  }

  if (force_journal_dest) {
    ceph_assert(rollback.orig_dest.ino);
    le->commit.add_dir_context(destdir);
    le->commit.add_primary_dentry(destdn, 0, true);
  }

  // slave: no need to journal straydn

  if (target && target != in && target->authority().first == whoami) {
    ceph_assert(rollback.orig_dest.remote_ino);
    le->commit.add_dir_context(target->get_projected_parent_dir());
    le->commit.add_primary_dentry(target->get_projected_parent_dn(), target, true);
  }

  if (in && in->is_dir() && (srcdn->authority().first == whoami || force_journal_src)) {
    dout(10) << " noting renamed dir ino " << in->ino() << " in metablob" << dendl;
    le->commit.renamed_dirino = in->ino();
    if (srcdn->authority().first == whoami) {
      auto&& ls = in->get_dirfrags();
      for (const auto& dir : ls) {
	if (!dir->is_auth())
	  le->commit.renamed_dir_frags.push_back(dir->get_frag());
      }
      dout(10) << " noting renamed dir open frags " << le->commit.renamed_dir_frags << dendl;
    }
  } else if (force_journal_dest) {
    dout(10) << " noting rename target ino " << target->ino() << " in metablob" << dendl;
    le->commit.renamed_dirino = target->ino();
  }
  
  if (target && target->is_dir()) {
    ceph_assert(destdn);
    mdcache->project_subtree_rename(target, straydir, destdir);
  }

  if (in && in->is_dir()) {
    ceph_assert(srcdn);
    mdcache->project_subtree_rename(in, destdir, srcdir);
  }

  if (mdr && !mdr->more()->slave_update_journaled) {
    ceph_assert(le->commit.empty());
    mdlog->cancel_entry(le);
    mut->ls = NULL;
    _rename_rollback_finish(mut, mdr, srcdn, srcdnpv, destdn, straydn, splits, finish_mdr);
  } else {
    ceph_assert(!le->commit.empty());
    if (mdr)
      mdr->more()->slave_update_journaled = false;
    MDSLogContextBase *fin = new C_MDS_LoggedRenameRollback(this, mut, mdr,
							    srcdn, srcdnpv, destdn, straydn,
							    splits, finish_mdr);
    submit_mdlog_entry(le, fin, mdr, __func__);
    mdlog->flush();
  }
}

void Server::_rename_rollback_finish(MutationRef& mut, MDRequestRef& mdr, CDentry *srcdn,
				     version_t srcdnpv, CDentry *destdn, CDentry *straydn,
				     map<client_t,ref_t<MClientSnap>> splits[2], bool finish_mdr)
{
  dout(10) << "_rename_rollback_finish " << mut->reqid << dendl;

  if (straydn) {
    straydn->get_dir()->unlink_inode(straydn);
    straydn->pop_projected_linkage();
  }
  if (destdn) {
    destdn->get_dir()->unlink_inode(destdn);
    destdn->pop_projected_linkage();
  }
  if (srcdn) {
    srcdn->pop_projected_linkage();
    if (srcdn->authority().first == mds->get_nodeid()) {
      srcdn->mark_dirty(srcdnpv, mut->ls);
      if (srcdn->get_linkage()->is_primary())
	srcdn->get_linkage()->get_inode()->state_set(CInode::STATE_AUTH);
    }
  }

  mut->apply();

  if (srcdn && srcdn->get_linkage()->is_primary()) {
    CInode *in = srcdn->get_linkage()->get_inode();
    if (in && in->is_dir()) {
      ceph_assert(destdn);
      mdcache->adjust_subtree_after_rename(in, destdn->get_dir(), true);
    }
  }

  if (destdn) {
    CInode *oldin = destdn->get_linkage()->get_inode();
    // update subtree map?
    if (oldin && oldin->is_dir()) {
      ceph_assert(straydn);
      mdcache->adjust_subtree_after_rename(oldin, straydn->get_dir(), true);
    }
  }

  if (mds->is_resolve()) {
    CDir *root = NULL;
    if (straydn)
      root = mdcache->get_subtree_root(straydn->get_dir());
    else if (destdn)
      root = mdcache->get_subtree_root(destdn->get_dir());
    if (root)
      mdcache->try_trim_non_auth_subtree(root);
  } else {
    mdcache->send_snaps(splits[1]);
    mdcache->send_snaps(splits[0]);
  }

  if (mdr) {
    MDSContext::vec finished;
    if (mdr->more()->is_ambiguous_auth) {
      if (srcdn->is_auth())
	mdr->more()->rename_inode->unfreeze_inode(finished);

      mdr->more()->rename_inode->clear_ambiguous_auth(finished);
      mdr->more()->is_ambiguous_auth = false;
    }
    mds->queue_waiters(finished);
    if (finish_mdr || mdr->aborted)
      mdcache->request_finish(mdr);
    else
      mdr->more()->slave_rolling_back = false;
  }

  mdcache->finish_rollback(mut->reqid, mdr);

  mut->cleanup();
}

void Server::handle_slave_rename_prep_ack(MDRequestRef& mdr, const cref_t<MMDSSlaveRequest> &ack)
{
  dout(10) << "handle_slave_rename_prep_ack " << *mdr 
	   << " witnessed by " << ack->get_source()
	   << " " << *ack << dendl;
  mds_rank_t from = mds_rank_t(ack->get_source().num());

  // note slave
  mdr->more()->slaves.insert(from);
  if (mdr->more()->srcdn_auth_mds == from &&
      mdr->more()->is_remote_frozen_authpin &&
      !mdr->more()->is_ambiguous_auth) {
    mdr->set_ambiguous_auth(mdr->more()->rename_inode);
  }

  // witnessed?  or add extra witnesses?
  ceph_assert(mdr->more()->witnessed.count(from) == 0);
  if (ack->is_interrupted()) {
    dout(10) << " slave request interrupted, noop" << dendl;
  } else if (ack->witnesses.empty()) {
    mdr->more()->witnessed.insert(from);
    if (!ack->is_not_journaled())
      mdr->more()->has_journaled_slaves = true;
  } else {
    dout(10) << " extra witnesses (srcdn replicas) are " << ack->witnesses << dendl;
    mdr->more()->extra_witnesses = ack->witnesses;
    mdr->more()->extra_witnesses.erase(mds->get_nodeid());  // not me!
  }

  // srci import?
  if (ack->inode_export.length()) {
    dout(10) << " got srci import" << dendl;
    mdr->more()->inode_import.share(ack->inode_export);
    mdr->more()->inode_import_v = ack->inode_export_v;
  }

  // remove from waiting list
  ceph_assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  if (mdr->more()->waiting_on_slave.empty())
    dispatch_client_request(mdr);  // go again!
  else 
    dout(10) << "still waiting on slaves " << mdr->more()->waiting_on_slave << dendl;
}

void Server::handle_slave_rename_notify_ack(MDRequestRef& mdr, const cref_t<MMDSSlaveRequest> &ack)
{
  dout(10) << "handle_slave_rename_notify_ack " << *mdr << " from mds."
	   << ack->get_source() << dendl;
  ceph_assert(mdr->is_slave());
  mds_rank_t from = mds_rank_t(ack->get_source().num());

  if (mdr->more()->waiting_on_slave.count(from)) {
    mdr->more()->waiting_on_slave.erase(from);

    if (mdr->more()->waiting_on_slave.empty()) {
      if (mdr->slave_request)
	dispatch_slave_request(mdr);
    } else 
      dout(10) << " still waiting for rename notify acks from "
	       << mdr->more()->waiting_on_slave << dendl;
  }
}

void Server::_slave_rename_sessions_flushed(MDRequestRef& mdr)
{
  dout(10) << "_slave_rename_sessions_flushed " << *mdr << dendl;

  if (mdr->more()->waiting_on_slave.count(MDS_RANK_NONE)) {
    mdr->more()->waiting_on_slave.erase(MDS_RANK_NONE);

    if (mdr->more()->waiting_on_slave.empty()) {
      if (mdr->slave_request)
	dispatch_slave_request(mdr);
    } else
      dout(10) << " still waiting for rename notify acks from "
	<< mdr->more()->waiting_on_slave << dendl;
  }
}

// snaps
/* This function takes responsibility for the passed mdr*/
void Server::handle_client_lssnap(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;

  // traverse to path
  CInode *diri = try_get_auth_inode(mdr, req->get_filepath().get_ino());
  if (!diri)
    return;

  if (!diri->is_dir()) {
    respond_to_request(mdr, -ENOTDIR);
    return;
  }
  dout(10) << "lssnap on " << *diri << dendl;

  // lock snap
  if (!mds->locker->try_rdlock_snap_layout(diri, mdr))
    return;

  if (!check_access(mdr, diri, MAY_READ))
    return;

  SnapRealm *realm = diri->find_snaprealm();
  map<snapid_t,const SnapInfo*> infomap;
  realm->get_snap_info(infomap, diri->get_oldest_snap());

  unsigned max_entries = req->head.args.readdir.max_entries;
  if (!max_entries)
    max_entries = infomap.size();
  int max_bytes = req->head.args.readdir.max_bytes;
  if (!max_bytes)
    // make sure at least one item can be encoded
    max_bytes = (512 << 10) + g_conf()->mds_max_xattr_pairs_size;

  __u64 last_snapid = 0;
  string offset_str = req->get_path2();
  if (!offset_str.empty())
    last_snapid = realm->resolve_snapname(offset_str, diri->ino());

  //Empty DirStat
  bufferlist dirbl;
  static DirStat empty;
  CDir::encode_dirstat(dirbl, mdr->session->info, empty);

  max_bytes -= dirbl.length() - sizeof(__u32) + sizeof(__u8) * 2;

  __u32 num = 0;
  bufferlist dnbl;
  auto p = infomap.upper_bound(last_snapid);
  for (; p != infomap.end() && num < max_entries; ++p) {
    dout(10) << p->first << " -> " << *p->second << dendl;

    // actual
    string snap_name;
    if (p->second->ino == diri->ino())
      snap_name = p->second->name;
    else
      snap_name = p->second->get_long_name();

    unsigned start_len = dnbl.length();
    if (int(start_len + snap_name.length() + sizeof(__u32) + sizeof(LeaseStat)) > max_bytes)
      break;

    encode(snap_name, dnbl);
    //infinite lease
    LeaseStat e(CEPH_LEASE_VALID, -1, 0);
    mds->locker->encode_lease(dnbl, mdr->session->info, e);
    dout(20) << "encode_infinite_lease" << dendl;

    int r = diri->encode_inodestat(dnbl, mdr->session, realm, p->first, max_bytes - (int)dnbl.length());
    if (r < 0) {
      bufferlist keep;
      keep.substr_of(dnbl, 0, start_len);
      dnbl.swap(keep);
      break;
    }
    ++num;
  }

  encode(num, dirbl);
  __u16 flags = 0;
  if (p == infomap.end()) {
    flags = CEPH_READDIR_FRAG_END;
    if (last_snapid == 0)
      flags |= CEPH_READDIR_FRAG_COMPLETE;
  }
  encode(flags, dirbl);
  dirbl.claim_append(dnbl);
  
  mdr->reply_extra_bl = dirbl;
  mdr->tracei = diri;
  respond_to_request(mdr, 0);
}


// MKSNAP

struct C_MDS_mksnap_finish : public ServerLogContext {
  CInode *diri;
  SnapInfo info;
  C_MDS_mksnap_finish(Server *s, MDRequestRef& r, CInode *di, SnapInfo &i) :
    ServerLogContext(s, r), diri(di), info(i) {}
  void finish(int r) override {
    server->_mksnap_finish(mdr, diri, info);
  }
};

/* This function takes responsibility for the passed mdr*/
void Server::handle_client_mksnap(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  // make sure we have as new a map as the client
  if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
    mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }
  if (!mds->mdsmap->allows_snaps()) {
    // you can't make snapshots until you set an option right now
    respond_to_request(mdr, -EPERM);
    return;
  }

  CInode *diri = try_get_auth_inode(mdr, req->get_filepath().get_ino());
  if (!diri)
    return;

  // dir only
  if (!diri->is_dir()) {
    respond_to_request(mdr, -ENOTDIR);
    return;
  }
  if (diri->is_system() && !diri->is_root()) {
    // no snaps in system dirs (root is ok)
    respond_to_request(mdr, -EPERM);
    return;
  }
  
  std::string_view snapname = req->get_filepath().last_dentry();

  if (mdr->client_request->get_caller_uid() < g_conf()->mds_snap_min_uid || mdr->client_request->get_caller_uid() > g_conf()->mds_snap_max_uid) {
    dout(20) << "mksnap " << snapname << " on " << *diri << " denied to uid " << mdr->client_request->get_caller_uid() << dendl;
    respond_to_request(mdr, -EPERM);
    return;
  }
  
  dout(10) << "mksnap " << snapname << " on " << *diri << dendl;

  // lock snap
  if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
    MutationImpl::LockOpVec lov;
    lov.add_xlock(&diri->snaplock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;

    if (CDentry *pdn = diri->get_projected_parent_dn(); pdn) {
      if (!mds->locker->try_rdlock_snap_layout(pdn->get_dir()->get_inode(), mdr))
	return;
    }
    mdr->locking_state |= MutationImpl::ALL_LOCKED;
  }

  if (!check_access(mdr, diri, MAY_WRITE|MAY_SNAPSHOT))
    return;

  // check if we can create any more snapshots
  // we don't allow any more if we are already at or beyond the limit
  if (diri->snaprealm &&
      diri->snaprealm->get_snaps().size() >= max_snaps_per_dir) {
    respond_to_request(mdr, -EMLINK);
    return;
  }

  // make sure name is unique
  if (diri->snaprealm &&
      diri->snaprealm->exists(snapname)) {
    respond_to_request(mdr, -EEXIST);
    return;
  }
  if (snapname.length() == 0 ||
      snapname[0] == '_') {
    respond_to_request(mdr, -EINVAL);
    return;
  }

  // allocate a snapid
  if (!mdr->more()->stid) {
    // prepare an stid
    mds->snapclient->prepare_create(diri->ino(), snapname,
				    mdr->get_mds_stamp(),
				    &mdr->more()->stid, &mdr->more()->snapidbl,
				    new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  version_t stid = mdr->more()->stid;
  snapid_t snapid;
  auto p = mdr->more()->snapidbl.cbegin();
  decode(snapid, p);
  dout(10) << " stid " << stid << " snapid " << snapid << dendl;

  ceph_assert(mds->snapclient->get_cached_version() >= stid);

  // journal
  SnapInfo info;
  info.ino = diri->ino();
  info.snapid = snapid;
  info.name = snapname;
  info.stamp = mdr->get_op_stamp();

  auto &pi = diri->project_inode(false, true);
  pi.inode.ctime = info.stamp;
  if (info.stamp > pi.inode.rstat.rctime)
    pi.inode.rstat.rctime = info.stamp;
  pi.inode.rstat.rsnaps++;
  pi.inode.version = diri->pre_dirty();

  // project the snaprealm
  auto &newsnap = *pi.snapnode;
  newsnap.created = snapid;
  auto em = newsnap.snaps.emplace(std::piecewise_construct, std::forward_as_tuple(snapid), std::forward_as_tuple(info));
  if (!em.second)
    em.first->second = info;
  newsnap.seq = snapid;
  newsnap.last_created = snapid;

  // journal the inode changes
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mksnap");
  mdlog->start_entry(le);

  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  le->metablob.add_table_transaction(TABLE_SNAP, stid);
  mdcache->predirty_journal_parents(mdr, &le->metablob, diri, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, diri);

  // journal the snaprealm changes
  submit_mdlog_entry(le, new C_MDS_mksnap_finish(this, mdr, diri, info),
                     mdr, __func__);
  mdlog->flush();
}

void Server::_mksnap_finish(MDRequestRef& mdr, CInode *diri, SnapInfo &info)
{
  dout(10) << "_mksnap_finish " << *mdr << " " << info << dendl;

  int op = (diri->snaprealm? CEPH_SNAP_OP_CREATE : CEPH_SNAP_OP_SPLIT);

  diri->pop_and_dirty_projected_inode(mdr->ls);
  mdr->apply();

  mds->snapclient->commit(mdr->more()->stid, mdr->ls);

  // create snap
  dout(10) << "snaprealm now " << *diri->snaprealm << dendl;

  // notify other mds
  mdcache->send_snap_update(diri, mdr->more()->stid, op);

  mdcache->do_realm_invalidate_and_update_notify(diri, op);

  // yay
  mdr->in[0] = diri;
  mdr->snapid = info.snapid;
  mdr->tracei = diri;
  respond_to_request(mdr, 0);
}


// RMSNAP

struct C_MDS_rmsnap_finish : public ServerLogContext {
  CInode *diri;
  snapid_t snapid;
  C_MDS_rmsnap_finish(Server *s, MDRequestRef& r, CInode *di, snapid_t sn) :
    ServerLogContext(s, r), diri(di), snapid(sn) {}
  void finish(int r) override {
    server->_rmsnap_finish(mdr, diri, snapid);
  }
};

/* This function takes responsibility for the passed mdr*/
void Server::handle_client_rmsnap(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;

  CInode *diri = try_get_auth_inode(mdr, req->get_filepath().get_ino());
  if (!diri)
    return;

  if (!diri->is_dir()) {
    respond_to_request(mdr, -ENOTDIR);
    return;
  }

  std::string_view snapname = req->get_filepath().last_dentry();

  if (mdr->client_request->get_caller_uid() < g_conf()->mds_snap_min_uid || mdr->client_request->get_caller_uid() > g_conf()->mds_snap_max_uid) {
    dout(20) << "rmsnap " << snapname << " on " << *diri << " denied to uid " << mdr->client_request->get_caller_uid() << dendl;
    respond_to_request(mdr, -EPERM);
    return;
  }

  dout(10) << "rmsnap " << snapname << " on " << *diri << dendl;

  // does snap exist?
  if (snapname.length() == 0 || snapname[0] == '_') {
    respond_to_request(mdr, -EINVAL);   // can't prune a parent snap, currently.
    return;
  }
  if (!diri->snaprealm || !diri->snaprealm->exists(snapname)) {
    respond_to_request(mdr, -ENOENT);
    return;
  }
  snapid_t snapid = diri->snaprealm->resolve_snapname(snapname, diri->ino());
  dout(10) << " snapname " << snapname << " is " << snapid << dendl;

  if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
    MutationImpl::LockOpVec lov;
    lov.add_xlock(&diri->snaplock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;
    if (CDentry *pdn = diri->get_projected_parent_dn(); pdn) {
      if (!mds->locker->try_rdlock_snap_layout(pdn->get_dir()->get_inode(), mdr))
	return;
    }
    mdr->locking_state |= MutationImpl::ALL_LOCKED;
  }

  if (!check_access(mdr, diri, MAY_WRITE|MAY_SNAPSHOT))
    return;

  // prepare
  if (!mdr->more()->stid) {
    mds->snapclient->prepare_destroy(diri->ino(), snapid,
				     &mdr->more()->stid, &mdr->more()->snapidbl,
				     new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }
  version_t stid = mdr->more()->stid;
  auto p = mdr->more()->snapidbl.cbegin();
  snapid_t seq;
  decode(seq, p);  
  dout(10) << " stid is " << stid << ", seq is " << seq << dendl;

  ceph_assert(mds->snapclient->get_cached_version() >= stid);

  // journal
  auto &pi = diri->project_inode(false, true);
  pi.inode.version = diri->pre_dirty();
  pi.inode.ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode.rstat.rctime)
    pi.inode.rstat.rctime = mdr->get_op_stamp();
  pi.inode.rstat.rsnaps--;
  
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "rmsnap");
  mdlog->start_entry(le);
  
  // project the snaprealm
  auto &newnode = *pi.snapnode;
  newnode.snaps.erase(snapid);
  newnode.seq = seq;
  newnode.last_destroyed = seq;

  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  le->metablob.add_table_transaction(TABLE_SNAP, stid);
  mdcache->predirty_journal_parents(mdr, &le->metablob, diri, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, diri);

  submit_mdlog_entry(le, new C_MDS_rmsnap_finish(this, mdr, diri, snapid),
                     mdr, __func__);
  mdlog->flush();
}

void Server::_rmsnap_finish(MDRequestRef& mdr, CInode *diri, snapid_t snapid)
{
  dout(10) << "_rmsnap_finish " << *mdr << " " << snapid << dendl;
  snapid_t stid = mdr->more()->stid;
  auto p = mdr->more()->snapidbl.cbegin();
  snapid_t seq;
  decode(seq, p);  

  diri->pop_and_dirty_projected_inode(mdr->ls);
  mdr->apply();

  mds->snapclient->commit(stid, mdr->ls);

  dout(10) << "snaprealm now " << *diri->snaprealm << dendl;

  // notify other mds
  mdcache->send_snap_update(diri, mdr->more()->stid, CEPH_SNAP_OP_DESTROY);

  mdcache->do_realm_invalidate_and_update_notify(diri, CEPH_SNAP_OP_DESTROY);

  // yay
  mdr->in[0] = diri;
  respond_to_request(mdr, 0);

  // purge snapshot data
  if (diri->snaprealm->have_past_parents_open())
    diri->purge_stale_snap_data(diri->snaprealm->get_snaps());
}

struct C_MDS_renamesnap_finish : public ServerLogContext {
  CInode *diri;
  snapid_t snapid;
  C_MDS_renamesnap_finish(Server *s, MDRequestRef& r, CInode *di, snapid_t sn) :
    ServerLogContext(s, r), diri(di), snapid(sn) {}
  void finish(int r) override {
    server->_renamesnap_finish(mdr, diri, snapid);
  }
};

/* This function takes responsibility for the passed mdr*/
void Server::handle_client_renamesnap(MDRequestRef& mdr)
{
  const cref_t<MClientRequest> &req = mdr->client_request;
  if (req->get_filepath().get_ino() != req->get_filepath2().get_ino()) {
    respond_to_request(mdr, -EINVAL);
    return;
  }

  CInode *diri = try_get_auth_inode(mdr, req->get_filepath().get_ino());
  if (!diri)
    return;

  if (!diri->is_dir()) { // dir only
    respond_to_request(mdr, -ENOTDIR);
    return;
  }

  if (mdr->client_request->get_caller_uid() < g_conf()->mds_snap_min_uid ||
      mdr->client_request->get_caller_uid() > g_conf()->mds_snap_max_uid) {
    respond_to_request(mdr, -EPERM);
    return;
  }

  std::string_view dstname = req->get_filepath().last_dentry();
  std::string_view srcname = req->get_filepath2().last_dentry();
  dout(10) << "renamesnap " << srcname << "->" << dstname << " on " << *diri << dendl;

  if (srcname.length() == 0 || srcname[0] == '_') {
    respond_to_request(mdr, -EINVAL);   // can't rename a parent snap.
    return;
  }
  if (!diri->snaprealm || !diri->snaprealm->exists(srcname)) {
    respond_to_request(mdr, -ENOENT);
    return;
  }
  if (dstname.length() == 0 || dstname[0] == '_') {
    respond_to_request(mdr, -EINVAL);
    return;
  }
  if (diri->snaprealm->exists(dstname)) {
    respond_to_request(mdr, -EEXIST);
    return;
  }

  snapid_t snapid = diri->snaprealm->resolve_snapname(srcname, diri->ino());
  dout(10) << " snapname " << srcname << " is " << snapid << dendl;

  // lock snap
  if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
    MutationImpl::LockOpVec lov;
    lov.add_xlock(&diri->snaplock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;
    if (CDentry *pdn = diri->get_projected_parent_dn(); pdn) {
      if (!mds->locker->try_rdlock_snap_layout(pdn->get_dir()->get_inode(), mdr))
	return;
    }
    mdr->locking_state |= MutationImpl::ALL_LOCKED;
  }

  if (!check_access(mdr, diri, MAY_WRITE|MAY_SNAPSHOT))
    return;

    // prepare
  if (!mdr->more()->stid) {
    mds->snapclient->prepare_update(diri->ino(), snapid, dstname, utime_t(),
				    &mdr->more()->stid,
				    new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  version_t stid = mdr->more()->stid;
  dout(10) << " stid is " << stid << dendl;

  ceph_assert(mds->snapclient->get_cached_version() >= stid);

  // journal
  auto &pi = diri->project_inode(false, true);
  pi.inode.ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode.rstat.rctime)
     pi.inode.rstat.rctime = mdr->get_op_stamp();
  pi.inode.version = diri->pre_dirty();

  // project the snaprealm
  auto &newsnap = *pi.snapnode;
  auto it = newsnap.snaps.find(snapid);
  ceph_assert(it != newsnap.snaps.end());
  it->second.name = dstname;

  // journal the inode changes
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "renamesnap");
  mdlog->start_entry(le);

  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  le->metablob.add_table_transaction(TABLE_SNAP, stid);
  mdcache->predirty_journal_parents(mdr, &le->metablob, diri, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, diri);

  // journal the snaprealm changes
  submit_mdlog_entry(le, new C_MDS_renamesnap_finish(this, mdr, diri, snapid),
                     mdr, __func__);
  mdlog->flush();
}

void Server::_renamesnap_finish(MDRequestRef& mdr, CInode *diri, snapid_t snapid)
{
  dout(10) << "_renamesnap_finish " << *mdr << " " << snapid << dendl;

  diri->pop_and_dirty_projected_inode(mdr->ls);
  mdr->apply();

  mds->snapclient->commit(mdr->more()->stid, mdr->ls);

  dout(10) << "snaprealm now " << *diri->snaprealm << dendl;

  // notify other mds
  mdcache->send_snap_update(diri, mdr->more()->stid, CEPH_SNAP_OP_UPDATE);

  mdcache->do_realm_invalidate_and_update_notify(diri, CEPH_SNAP_OP_UPDATE);

  // yay
  mdr->in[0] = diri;
  mdr->tracei = diri;
  mdr->snapid = snapid;
  respond_to_request(mdr, 0);
}

/**
 * Return true if server is in state RECONNECT and this
 * client has not yet reconnected.
 */
bool Server::waiting_for_reconnect(client_t c) const
{
  return client_reconnect_gather.count(c) > 0;
}

void Server::dump_reconnect_status(Formatter *f) const
{
  f->open_object_section("reconnect_status");
  f->dump_stream("client_reconnect_gather") << client_reconnect_gather;
  f->close_section();
}
