// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "MDSReadPhase.h"

#include <algorithm>
#include <unordered_set>

#include "Beacon.h"
#include "CDentry.h"
#include "CDir.h"
#include "CInode.h"
#include "Locker.h"
#include "MDBalancer.h"
#include "MDCache.h"
#include "MDSRank.h"
#include "Server.h"
#include "SessionMap.h"
#include "SnapRealm.h"
#include "mdstypes.h"

#include "common/debug.h"
#include "messages/MClientReply.h"
#include "msg/Messenger.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".read_phase "

thread_local MDSReadPhaseCtx *MDSReadPhaseCtx::tl_ctx = nullptr;

uint64_t MDSReadPhaseCtx::alloc_cap_id()
{
  ceph_assert(next_cap_id < end_cap_id);
  return next_cap_id++;
}

MDSReadPhase::MDSReadPhase(MDSRank *m, unsigned nthreads) : mds(m)
{
  ctxs.resize(nthreads + 1);
  for (unsigned i = 0; i <= nthreads; i++)
    slots.push_back(std::make_unique<Slot>());
  for (unsigned i = 1; i <= nthreads; i++)
    threads.emplace_back(&MDSReadPhase::worker_entry, this, i);
}

MDSReadPhase::~MDSReadPhase()
{
  shutdown();
}

void MDSReadPhase::shutdown()
{
  {
    std::lock_guard l(pool_lock);
    if (stopping)
      return;
    stopping = true;
  }
  dout(1) << "stats: phases " << stats.phases << " queued " << stats.queued
          << " served " << stats.served << " fallback " << stats.fallback
          << " late " << stats.late
          << dendl;
  for (auto &s : slots)
    s->cv.notify_one();
  for (auto &t : threads)
    t.join();
  threads.clear();
  // the requests still queued are dropped; the clients resend them to the
  // next MDS
  pending.clear();
}

void MDSReadPhase::log_stats()
{
  if (stats.phases == logged_phases)
    return;
  logged_phases = stats.phases;
  dout(1) << "stats: phases " << stats.phases << " queued " << stats.queued
          << " served " << stats.served << " fallback " << stats.fallback
          << " late " << stats.late
          << dendl;
}

void MDSReadPhase::worker_entry(unsigned idx)
{
  ceph_pthread_setname("mds-read-phase");
  uint64_t seen = 0;
  Slot &slot = *slots[idx];
  std::unique_lock l(pool_lock);
  while (true) {
    slot.cv.wait(l, [&] { return stopping || slot.gen != seen; });
    if (stopping)
      return;
    seen = slot.gen;
    auto *fn = job;
    l.unlock();
    (*fn)(idx);
    l.lock();
    if (--running == 0)
      pool_done.notify_one();
  }
}

/* run fn(0..nw-1), fn(0) on the calling thread; wake only nw-1 workers */
void MDSReadPhase::run_parallel(const std::function<void(unsigned)> &fn,
                                unsigned nw)
{
  {
    std::lock_guard l(pool_lock);
    job = &fn;
    running = nw - 1;
    for (unsigned i = 1; i < nw; i++)
      slots[i]->gen++;
  }
  for (unsigned i = 1; i < nw; i++)
    slots[i]->cv.notify_one();
  fn(0);
  std::unique_lock l(pool_lock);
  pool_done.wait(l, [&] { return running == 0; });
  job = nullptr;
}

bool MDSReadPhase::is_candidate(const cref_t<Message> &m) const
{
  if (m->get_type() != CEPH_MSG_CLIENT_REQUEST)
    return false;
  const auto &req = ref_cast<MClientRequest>(m);
  if (!req->get_source().is_client() ||
      req->is_replay() || req->get_retry_attempt() || req->is_queued_for_replay())
    return false;
  // Cap releases carried by the request change cap and lock state; only
  // the normal path processes them.
  if (!req->releases.empty())
    return false;
  const filepath &fp = req->get_filepath();
  switch (req->get_op()) {
  case CEPH_MDS_OP_GETATTR:
    if (fp.depth() != 0)
      return false;
    break;
  case CEPH_MDS_OP_LOOKUP:
    if (fp.depth() != 1 || fp[0].empty())
      return false;
    break;
  default:
    return false;
  }
  return true;
}

bool MDSReadPhase::maybe_queue(const cref_t<Message> &m)
{
  if (!is_candidate(m))
    return false;

  // as MDSRankDispatcher::ms_dispatch() does
  auto priv = m->get_connection()->get_priv();
  if (auto *session = static_cast<Session*>(priv.get()))
    session->last_seen = Session::clock::now();

  pending.push_back(m);
  stats.queued++;
  // Wait for more only if the messenger has more queued behind this, and
  // enough to fill a batch worth splitting over threads.  With nothing
  // queued, the clients that would send the next request may all be
  // waiting for replies to the queued ones.  A smaller batch is served
  // inline anyway, so waiting would only delay it, and the message that
  // would end the wait may never come here: messages other dispatchers
  // take (MonClient) never reach ms_dispatch2().
  const size_t backlog = mds->messenger->get_dispatch_queue_len();
  if (pending.size() >= max_batch || backlog == 0 ||
      pending.size() + backlog < 2 * min_per_worker)
    flush();
  return true;
}

/* serial, mds_lock held: find the target of a request */
bool MDSReadPhase::resolve(Item &it)
{
  const auto &req = it.req;
  it.session = mds->get_session(req);
  if (!it.session || !it.session->is_open() || it.session->is_stale() ||
      !it.session->get_connection())
    return false;

  const filepath &fp = req->get_filepath();
  if (req->get_op() == CEPH_MDS_OP_GETATTR) {
    it.in = mds->mdcache->get_inode(fp.get_ino());
    return it.in != nullptr;
  }

  // LOOKUP
  CInode *diri = mds->mdcache->get_inode(fp.get_ino());
  if (!diri || !diri->is_dir())
    return false;
  std::string_view name = fp[0];
  CDir *dir = diri->get_dirfrag(diri->pick_dirfrag(name));
  if (!dir || !dir->is_auth())
    return false;
  CDentry *dn = dir->lookup(name);
  if (!dn || dn->is_projected())
    return false;
  CDentry::linkage_t *dnl = dn->get_linkage();
  if (!dnl->is_primary())
    return false;
  it.diri = diri;
  it.dir = dir;
  it.dn = dn;
  it.in = dnl->get_inode();
  return true;
}

/*
 * Could the normal path take every rdlock this request needs without
 * changing any lock state, and would it let the client read the inode?
 * Anything else, including errors, goes to the normal path.
 *
 * The locks must also be stable.  The normal path moves a lock that is
 * readable but unstable (a dentry lock still in XLOCKDONE after the mkdir
 * that created it, ...) on to a stable state when it drops its rdlock;
 * the read phase takes no rdlock, so it would leave such a lock where it
 * is, and e.g. never get to issue a lease on that dentry.
 */
static bool readable(const SimpleLock &l, client_t client)
{
  return l.is_stable() && l.can_rdlock(client);
}

bool MDSReadPhase::can_read(Item &it)
{
  CInode *in = it.in;
  const client_t client = it.session->get_client();

  if (!in->is_auth() || in->is_ambiguous_auth() ||
      in->is_frozen() || in->is_freezing() ||
      in->state_test(CInode::STATE_EXPORTINGCAPS) ||
      in->last != CEPH_NOSNAP || in->is_under_quarantine() ||
      in->quiescelock.is_xlocked())
    return false;
  if (!readable(in->snaplock, client))
    return false;

  int issued = 0;
  if (Capability *cap = in->get_client_cap(client))
    issued = cap->issued();
  const int mask = it.req->head.args.getattr.mask;
  if ((mask & CEPH_CAP_LINK_SHARED) && !(issued & CEPH_CAP_LINK_EXCL) &&
      !readable(in->linklock, client))
    return false;
  if ((mask & CEPH_CAP_AUTH_SHARED) && !(issued & CEPH_CAP_AUTH_EXCL) &&
      !readable(in->authlock, client))
    return false;
  if ((mask & CEPH_CAP_XATTR_SHARED) && !(issued & CEPH_CAP_XATTR_EXCL) &&
      !readable(in->xattrlock, client))
    return false;
  if ((mask & CEPH_CAP_FILE_SHARED) && !(issued & CEPH_CAP_FILE_EXCL) &&
      !readable(in->filelock, client))
    return false;

  if (it.dn) {
    // The parent goes into the reply without caps (another worker may be
    // serving a request on it), so a client with no cap on the parent
    // would never get one, and with no cap and no dentry lease it has to
    // look the name up again every time it walks this path.  Let the
    // normal path give it the cap, once.  The parent's caps can only be
    // read if no worker changes them, i.e. it is no target in this batch.
    if (!it.lease_ok)
      return false;
    Capability *dcap = it.diri->get_client_cap(client);
    if (!dcap)
      return false;
    // Likewise when the normal path would add caps to the client's cap on
    // the parent: it issues what the client wants or the inode likes, as
    // far as the locks allow, and that is how a client gets Fs back on a
    // directory whose filelock did not allow it when its cap was issued.
    // Without Fs and without a dentry lease (none is issued when the
    // filelock would allow Fs) it has to look every name up again.
    int would = (dcap->wanted() | it.diri->get_caps_liked()) &
      it.diri->get_caps_allowed_for_client(it.session, dcap,
                                           &*it.diri->get_inode());
    if (would & ~dcap->pending())
      return false;
    if (it.dir->is_frozen() || it.dir->is_freezing() ||
        !readable(it.diri->snaplock, client) ||
        !readable(it.dn->lock, client))
      return false;
  }

  const auto &req = it.req;
  int r = it.session->check_access(
    mds->mdsmap->get_fs_name(), in, MAY_READ,
    req->get_caller_uid(), req->get_caller_gid(),
    &req->get_caller_gid_list(),
    req->head.args.setattr.uid, req->head.args.setattr.gid, false);
  return r >= 0;
}

/* parallel, mds_lock held by the dispatch thread */
bool MDSReadPhase::serve(Item &it, MDSReadPhaseCtx &ctx)
{
  if (!can_read(it))
    return false;

  Session *session = it.session;
  auto reply = ceph::make_message<MClientReply>(*it.req, 0);

  SnapRealm *realm = it.in->find_snaprealm();
  {
    // SnapRealm builds its snap trace lazily
    std::lock_guard l(snap_lock);
    reply->snapbl = mds->server->get_snap_trace(session, realm);
  }

  bufferlist bl;
  if (it.dn) {
    reply->head.is_dentry = 1;
    // the parent may be another worker's target: no caps on it
    it.diri->encode_inodestat(bl, session, nullptr, CEPH_NOSNAP, 0, 0, true);
    DirStat ds;
    ds.frag = it.dir->get_frag();
    ds.auth = it.dir->get_dir_auth().first;
    if (it.dir->is_auth() && !mds->server->forward_all_requests_to_auth)
      it.dir->get_dist_spec(ds.dist, mds->get_nodeid());
    it.dir->encode_dirstat(bl, session->info, ds);
    encode(it.dn->get_name(), bl);
    // as Locker::issue_client_lease(); the lists it puts the lease on are
    // shared, so that part is done after the phase
    const client_t client = session->get_client();
    LeaseStat lstat;
    lstat.mask = 0;
    lstat.alternate_name = std::string(it.dn->get_alternate_name());
    if (it.lease_ok &&
        !it.diri->is_stray() &&
        !it.diri->filelock.can_lease(client) &&
        !(it.diri->get_client_cap_pending(client) &
          (CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_EXCL)) &&
        it.dn->lock.can_lease(client)) {
      ClientLease *l = it.dn->add_client_lease(session);
      const int pool = 1;
      utime_t ttl = ceph_clock_now();
      ttl += mds->mdcache->client_lease_durations[pool];
      ctx.leases.emplace_back(l, ttl);
      lstat.mask = CEPH_LEASE_VALID | CEPH_LEASE_PRIMARY_LINK;
      lstat.duration_ms =
        (uint32_t)(1000 * mds->mdcache->client_lease_durations[pool]);
      lstat.seq = ++l->seq;
    }
    mds->locker->encode_lease(bl, session->info, lstat);
  } else {
    reply->head.is_dentry = 0;
  }
  it.in->encode_inodestat(bl, session, nullptr, CEPH_NOSNAP, 0,
                          it.req->head.args.getattr.mask);
  reply->head.is_target = 1;
  reply->set_trace(bl);
  reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());

  utime_t lat = ceph_clock_now() - it.req->get_recv_stamp();
  mds->logger->inc(l_mds_reply);
  mds->logger->tinc(l_mds_reply_latency, lat);
  mds->server->perf_gather_op_latency(it.req, lat);

  mds->send_message_client(reply, session);

  ctx.hits.push_back(it.in);
  ctx.sessions.push_back(session);
  return true;
}

/* serial, mds_lock held: what the workers deferred */
void MDSReadPhase::apply(MDSReadPhaseCtx &ctx)
{
  for (CInode *in : ctx.first_caps)
    in->add_first_cap_shared();
  for (Capability *cap : ctx.new_caps) {
    cap->get_session()->touch_cap_bottom(cap);
    cap->get_inode()->containing_realm->add_cap(cap->get_client(), cap);
  }
  for (CInode *in : ctx.hits)
    mds->balancer->hit_inode(in, META_POP_IRD);
  for (Session *s : ctx.sessions)
    mds->sessionmap.hit_session(s);
  for (CDentry *dn : ctx.lease_pins)
    dn->get(CDentry::PIN_CLIENTLEASE);
  for (auto &[l, ttl] : ctx.leases) {
    l->session->touch_lease(l);
    mds->mdcache->touch_client_lease(l, 1, ttl);
  }
  ctx.clear();
}

void MDSReadPhase::flush()
{
  if (pending.empty())
    return;

  std::vector<cref_t<Message>> batch;
  batch.swap(pending);

  std::vector<Item> items;
  std::vector<cref_t<Message>> fallback;
  const bool usable = mds->is_active() && !mds->beacon.is_laggy() &&
    mds->waiting_for_nolaggy.empty() && mds->mdcache->is_open() &&
    !g_conf()->mds_inject_traceless_reply_probability;
  if (usable) {
    items.reserve(batch.size());
    for (auto &m : batch) {
      Item it;
      it.req = ref_cast<MClientRequest>(m);
      if (resolve(it))
        items.push_back(std::move(it));
    }
  }

  if (!items.empty()) {
    // a LOOKUP may read its parent's caps (dentry lease) only if no worker
    // changes them, i.e. the parent is no request's target in this batch
    std::unordered_set<CInode*> targets;
    for (auto &it : items)
      targets.insert(it.in);
    for (auto &it : items)
      if (it.diri)
        it.lease_ok = !targets.count(it.diri);

    const unsigned nworkers = std::clamp<unsigned>(
      items.size() / min_per_worker.load(), 1, ctxs.size());
    std::vector<std::vector<Item*>> parts(nworkers);
    for (auto &it : items)
      parts[it.in->ino().val % nworkers].push_back(&it);

    // each request adds at most one cap
    for (unsigned w = 0; w < nworkers; w++) {
      ctxs[w].next_cap_id = mds->mdcache->last_cap_id + 1;
      mds->mdcache->last_cap_id += parts[w].size();
      ctxs[w].end_cap_id = mds->mdcache->last_cap_id + 1;
      ctxs[w].lease_seq_lock = &lease_seq_lock;
    }

    auto work = [&](unsigned w) {
      MDSReadPhaseCtx::set_current(&ctxs[w]);
      for (Item *it : parts[w])
        it->served = serve(*it, ctxs[w]);
      MDSReadPhaseCtx::set_current(nullptr);
    };
    if (nworkers == 1)
      work(0);
    else
      run_parallel(work, nworkers);
    for (unsigned w = 0; w < nworkers; w++)
      apply(ctxs[w]);
    stats.phases++;
  }

  // the rest the normal way, in arrival order
  size_t i = 0, served = 0;
  for (auto &m : batch) {
    if (i < items.size() && items[i].req.get() == m.get()) {
      Item &it = items[i++];
      if (it.served) {
        // as Server::handle_client_request() does for every request
        mds->server->trim_completed_request_list(
          it.req->get_oldest_client_tid(), it.session);
        served++;
        continue;
      }
    }
    fallback.push_back(m);
  }
  if (served) {
    // the counters the normal path bumps for every request, which
    // ceph fs status and the balancer's request rate read
    mds->logger->inc(l_mds_request, served);
    mds->server->logger->inc(l_mdss_handle_client_request, served);
    mds->server->logger->inc(l_mdss_dispatch_client_request, served);
  }
  stats.served += served;
  stats.fallback += fallback.size();
  dout(20) << __func__ << " batch " << batch.size() << " served " << served
           << " fallback " << fallback.size() << dendl;

  for (auto &m : fallback) {
    mds->inc_dispatch_depth();
    mds->_dispatch(m, true);
    mds->dec_dispatch_depth();
  }
}
