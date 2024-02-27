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

#include "MDSRank.h"
#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "Migrator.h"
#include "Locker.h"
#include "Server.h"

#include "MDBalancer.h"
#include "MDLog.h"
#include "MDSMap.h"
#include "Mutation.h"

#include "include/filepath.h"
#include "common/likely.h"

#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/ESessions.h"

#include "msg/Messenger.h"

#include "messages/MClientCaps.h"

/*
 * this is what the dir->dir_auth values look like
 *
 *   dir_auth  authbits  
 * export
 *   me         me      - before
 *   me, me     me      - still me, but preparing for export
 *   me, them   me      - send MExportDir (peer is preparing)
 *   them, me   me      - journaled EExport
 *   them       them    - done
 *
 * import:
 *   them       them    - before
 *   me, them   me      - journaled EImportStart
 *   me         me      - done
 *
 * which implies:
 *  - auth bit is set if i am listed as first _or_ second dir_auth.
 */

#include "common/config.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".mig " << __func__ << " "

using namespace std;

class MigratorContext : public MDSContext {
protected:
  Migrator *mig;
  MDSRank *get_mds() override {
    return mig->mds;
  }
public:
  explicit MigratorContext(Migrator *mig_) : mig(mig_) {
    ceph_assert(mig != NULL);
  }
};

class MigratorLogContext : public MDSLogContextBase {
protected:
  Migrator *mig;
  MDSRank *get_mds() override {
    return mig->mds;
  }
public:
  explicit MigratorLogContext(Migrator *mig_) : mig(mig_) {
    ceph_assert(mig != NULL);
  }
};

void Migrator::dispatch(const cref_t<Message> &m)
{
  switch (m->get_type()) {
    // import
  case MSG_MDS_EXPORTDIRDISCOVER:
    handle_export_discover(ref_cast<MExportDirDiscover>(m));
    break;
  case MSG_MDS_EXPORTDIRPREP:
    handle_export_prep(ref_cast<MExportDirPrep>(m));
    break;
  case MSG_MDS_EXPORTDIR:
    if (unlikely(inject_session_race)) {
      dout(0) << "waiting for inject_session_race" << dendl;
      mds->wait_for_any_client_connection(new C_MDS_RetryMessage(mds, m));
    } else {
      handle_export_dir(ref_cast<MExportDir>(m));
    }
    break;
  case MSG_MDS_EXPORTDIRFINISH:
    handle_export_finish(ref_cast<MExportDirFinish>(m));
    break;
  case MSG_MDS_EXPORTDIRCANCEL:
    handle_export_cancel(ref_cast<MExportDirCancel>(m));
    break;

    // export 
  case MSG_MDS_EXPORTDIRDISCOVERACK:
    handle_export_discover_ack(ref_cast<MExportDirDiscoverAck>(m));
    break;
  case MSG_MDS_EXPORTDIRPREPACK:
    handle_export_prep_ack(ref_cast<MExportDirPrepAck>(m));
    break;
  case MSG_MDS_EXPORTDIRACK:
    handle_export_ack(ref_cast<MExportDirAck>(m));
    break;
  case MSG_MDS_EXPORTDIRNOTIFYACK:
    handle_export_notify_ack(ref_cast<MExportDirNotifyAck>(m));
    break;

    // export 3rd party (dir_auth adjustments)
  case MSG_MDS_EXPORTDIRNOTIFY:
    handle_export_notify(ref_cast<MExportDirNotify>(m));
    break;

    // caps
  case MSG_MDS_EXPORTCAPS:
    handle_export_caps(ref_cast<MExportCaps>(m));
    break;
  case MSG_MDS_EXPORTCAPSACK:
    handle_export_caps_ack(ref_cast<MExportCapsAck>(m));
    break;
  case MSG_MDS_GATHERCAPS:
    handle_gather_caps(ref_cast<MGatherCaps>(m));
    break;

  default:
    derr << "migrator unknown message " << m->get_type() << dendl;
    ceph_abort_msg("migrator unknown message");
  }
}

void Migrator::export_empty_import(CDir *dir)
{
  dout(7) << *dir << dendl;
  ceph_assert(dir->is_subtree_root());

  if (dir->inode->is_auth()) {
    dout(7) << " inode is auth" << dendl;
    return;
  }
  if (!dir->is_auth()) {
    dout(7) << " not auth" << dendl;
    return;
  }
  if (dir->is_freezing() || dir->is_frozen()) {
    dout(7) << " freezing or frozen" << dendl;
    return;
  }
  if (dir->get_num_head_items() > 0) {
    dout(7) << " not actually empty" << dendl;
    return;
  }
  if (dir->inode->is_root()) {
    dout(7) << " root" << dendl;
    return;
  }
  
  mds_rank_t dest = dir->inode->authority().first;
  //if (mds->is_shutting_down()) dest = 0;  // this is more efficient.
  
  dout(7) << " really empty, exporting to " << dest << dendl;
  assert (dest != mds->get_nodeid());
  
  dout(7) << "exporting to mds." << dest 
           << " empty import " << *dir << dendl;
  export_dir( dir, dest );
}

void Migrator::find_stale_export_freeze()
{
  utime_t now = ceph_clock_now();
  utime_t cutoff = now;
  cutoff -= g_conf()->mds_freeze_tree_timeout;


  /*
   * We could have situations like:
   *
   * - mds.0 authpins an item in subtree A
   * - mds.0 sends request to mds.1 to authpin an item in subtree B
   * - mds.0 freezes subtree A
   * - mds.1 authpins an item in subtree B
   * - mds.1 sends request to mds.0 to authpin an item in subtree A
   * - mds.1 freezes subtree B
   * - mds.1 receives the remote authpin request from mds.0
   *   (wait because subtree B is freezing)
   * - mds.0 receives the remote authpin request from mds.1
   *   (wait because subtree A is freezing)
   *
   *
   * - client request authpins items in subtree B
   * - freeze subtree B
   * - import subtree A which is parent of subtree B
   *   (authpins parent inode of subtree B, see CDir::set_dir_auth())
   * - freeze subtree A
   * - client request tries authpinning items in subtree A
   *   (wait because subtree A is freezing)
   */
  for (map<CDir*,export_state_t>::iterator p = export_state.begin();
       p != export_state.end(); ) {
    CDir* dir = p->first;
    export_state_t& stat = p->second;
    ++p;
    if (stat.state != EXPORT_DISCOVERING && stat.state != EXPORT_FREEZING)
      continue;
    ceph_assert(dir->freeze_tree_state);
    if (stat.last_cum_auth_pins != dir->freeze_tree_state->auth_pins) {
      stat.last_cum_auth_pins = dir->freeze_tree_state->auth_pins;
      stat.last_cum_auth_pins_change = now;
      continue;
    }
    if (stat.last_cum_auth_pins_change >= cutoff)
      continue;
    if (stat.num_remote_waiters > 0 ||
	(!dir->inode->is_root() && dir->get_parent_dir()->is_freezing())) {
      export_try_cancel(dir);
    }
  }
}

void Migrator::export_try_cancel(CDir *dir, bool notify_peer)
{
  dout(10) << *dir << dendl;

  map<CDir*,export_state_t>::iterator it = export_state.find(dir);
  ceph_assert(it != export_state.end());

  int state = it->second.state;
  switch (state) {
  case EXPORT_LOCKING:
    dout(10) << "export state=locking : dropping locks and removing auth_pin" << dendl;
    num_locking_exports--;
    it->second.state = EXPORT_CANCELLED;
    dir->auth_unpin(this);
    break;
  case EXPORT_DISCOVERING:
    dout(10) << "export state=discovering : canceling freeze and removing auth_pin" << dendl;
    it->second.state = EXPORT_CANCELLED;
    dir->unfreeze_tree();  // cancel the freeze
    dir->auth_unpin(this);
    if (notify_peer &&
	(!mds->is_cluster_degraded() ||
	 mds->mdsmap->is_clientreplay_or_active_or_stopping(it->second.peer))) // tell them.
      mds->send_message_mds(make_message<MExportDirCancel>(dir->dirfrag(),
							   it->second.tid),
			    it->second.peer);
    break;

  case EXPORT_FREEZING:
    dout(10) << "export state=freezing : canceling freeze" << dendl;
    it->second.state = EXPORT_CANCELLED;
    dir->unfreeze_tree();  // cancel the freeze
    if (dir->is_subtree_root())
      mdcache->try_subtree_merge(dir);
    if (notify_peer &&
	(!mds->is_cluster_degraded() ||
	 mds->mdsmap->is_clientreplay_or_active_or_stopping(it->second.peer))) // tell them.
      mds->send_message_mds(make_message<MExportDirCancel>(dir->dirfrag(),
							   it->second.tid),
			    it->second.peer);
    break;

    // NOTE: state order reversal, warning comes after prepping
  case EXPORT_WARNING:
    dout(10) << "export state=warning : unpinning bounds, unfreezing, notifying" << dendl;
    it->second.state = EXPORT_CANCELLING;
    // fall-thru

  case EXPORT_PREPPING:
    if (state != EXPORT_WARNING) {
      dout(10) << "export state=prepping : unpinning bounds, unfreezing" << dendl;
      it->second.state = EXPORT_CANCELLED;
    }

    {
      // unpin bounds
      set<CDir*> bounds;
      mdcache->get_subtree_bounds(dir, bounds);
      for (set<CDir*>::iterator q = bounds.begin();
          q != bounds.end();
          ++q) {
        CDir *bd = *q;
        bd->put(CDir::PIN_EXPORTBOUND);
        bd->state_clear(CDir::STATE_EXPORTBOUND);
      }
      if (state == EXPORT_WARNING) {
	// notify bystanders
	export_notify_abort(dir, it->second, bounds);
	// process delayed expires
	mdcache->process_delayed_expire(dir);
      }
    }
    dir->unfreeze_tree();
    mdcache->try_subtree_merge(dir);
    if (notify_peer &&
	(!mds->is_cluster_degraded() ||
	 mds->mdsmap->is_clientreplay_or_active_or_stopping(it->second.peer))) // tell them.
      mds->send_message_mds(make_message<MExportDirCancel>(dir->dirfrag(),
							   it->second.tid),
			    it->second.peer);
    break;

  case EXPORT_EXPORTING:
    dout(10) << "export state=exporting : reversing, and unfreezing" << dendl;
    it->second.state = EXPORT_CANCELLING;
    export_reverse(dir, it->second);
    break;

  case EXPORT_LOGGINGFINISH:
  case EXPORT_NOTIFYING:
    dout(10) << "export state=loggingfinish|notifying : ignoring dest failure, we were successful." << dendl;
    // leave export_state, don't clean up now.
    break;
  case EXPORT_CANCELLING:
    break;

  default:
    ceph_abort();
  }

  // finish clean-up?
  if (it->second.state == EXPORT_CANCELLING ||
      it->second.state == EXPORT_CANCELLED) {
    MutationRef mut;
    mut.swap(it->second.mut);

    if (it->second.state == EXPORT_CANCELLED) {
      export_cancel_finish(it);
    }

    // drop locks
    if (state == EXPORT_LOCKING || state == EXPORT_DISCOVERING) {
      MDRequestRef mdr = static_cast<MDRequestImpl*>(mut.get());
      ceph_assert(mdr);
      mdcache->request_kill(mdr);
    } else if (mut) {
      mds->locker->drop_locks(mut.get());
      mut->cleanup();
    }

    mdcache->show_subtrees();

    maybe_do_queued_export();
  }
}

void Migrator::export_cancel_finish(export_state_iterator& it)
{
  CDir *dir = it->first;
  bool unpin = (it->second.state == EXPORT_CANCELLING);
  auto parent = std::move(it->second.parent);

  total_exporting_size -= it->second.approx_size;
  export_state.erase(it);

  ceph_assert(dir->state_test(CDir::STATE_EXPORTING));
  dir->clear_exporting();

  if (unpin) {
    // pinned by Migrator::export_notify_abort()
    dir->auth_unpin(this);
  }
  // send pending import_maps?  (these need to go out when all exports have finished.)
  mdcache->maybe_send_pending_resolves();

  if (parent)
    child_export_finish(parent, false);
}

// ==========================================================
// mds failure handling

void Migrator::handle_mds_failure_or_stop(mds_rank_t who)
{
  dout(5) << who << dendl;

  // check my exports

  // first add an extra auth_pin on any freezes, so that canceling a
  // nested freeze doesn't complete one further up the hierarchy and
  // confuse the shit out of us.  we'll remove it after canceling the
  // freeze.  this way no freeze completions run before we want them
  // to.
  std::vector<CDir*> pinned_dirs;
  for (map<CDir*,export_state_t>::iterator p = export_state.begin();
       p != export_state.end();
       ++p) {
    if (p->second.state == EXPORT_FREEZING) {
      CDir *dir = p->first;
      dout(10) << "adding temp auth_pin on freezing " << *dir << dendl;
      dir->auth_pin(this);
      pinned_dirs.push_back(dir);
    }
  }

  map<CDir*,export_state_t>::iterator p = export_state.begin();
  while (p != export_state.end()) {
    map<CDir*,export_state_t>::iterator next = p;
    ++next;
    CDir *dir = p->first;
    
    // abort exports:
    //  - that are going to the failed node
    //  - that aren't frozen yet (to avoid auth_pin deadlock)
    //  - they havne't prepped yet (they may need to discover bounds to do that)
    if ((p->second.peer == who &&
	 p->second.state != EXPORT_CANCELLING) ||
	p->second.state == EXPORT_LOCKING ||
	p->second.state == EXPORT_DISCOVERING ||
	p->second.state == EXPORT_FREEZING ||
	p->second.state == EXPORT_PREPPING) {
      // the guy i'm exporting to failed, or we're just freezing.
      dout(10) << "cleaning up export state (" << p->second.state << ")"
	       << get_export_statename(p->second.state) << " of " << *dir << dendl;
      export_try_cancel(dir);
    } else if (p->second.peer != who) {
      // bystander failed.
      if (p->second.warning_ack_waiting.erase(who)) {
	if (p->second.state == EXPORT_WARNING) {
	  p->second.notify_ack_waiting.erase(who);   // they won't get a notify either.
	  // exporter waiting for warning acks, let's fake theirs.
	  dout(10) << "faking export_warning_ack from mds." << who
		   << " on " << *dir << " to mds." << p->second.peer
		   << dendl;
	  if (p->second.warning_ack_waiting.empty())
	    export_go(dir);
	}
      }
      if (p->second.notify_ack_waiting.erase(who)) {
	// exporter is waiting for notify acks, fake it
	dout(10) << "faking export_notify_ack from mds." << who
		 << " on " << *dir << " to mds." << p->second.peer
		 << dendl;
	if (p->second.state == EXPORT_NOTIFYING) {
	  if (p->second.notify_ack_waiting.empty())
	    export_finish(dir);
	} else if (p->second.state == EXPORT_CANCELLING) {
	  if (p->second.notify_ack_waiting.empty()) {
	    export_cancel_finish(p);
	  }
	}
      }
    }
    
    // next!
    p = next;
  }


  // check my imports
  map<dirfrag_t,import_state_t>::iterator q = import_state.begin();
  while (q != import_state.end()) {
    map<dirfrag_t,import_state_t>::iterator next = q;
    ++next;
    dirfrag_t df = q->first;
    CInode *diri = mdcache->get_inode(df.ino);
    CDir *dir = mdcache->get_dirfrag(df);

    if (q->second.peer == who) {
      if (dir)
	dout(10) << "cleaning up import state (" << q->second.state << ")"
		 << get_import_statename(q->second.state) << " of " << *dir << dendl;
      else
	dout(10) << "cleaning up import state (" << q->second.state << ")"
		 << get_import_statename(q->second.state) << " of " << df << dendl;

      switch (q->second.state) {
      case IMPORT_DISCOVERING:
	dout(10) << "import state=discovering : clearing state" << dendl;
	import_reverse_discovering(df);
	break;

      case IMPORT_DISCOVERED:
	ceph_assert(diri);
	dout(10) << "import state=discovered : unpinning inode " << *diri << dendl;
	import_reverse_discovered(df, diri);
	break;

      case IMPORT_PREPPING:
	ceph_assert(dir);
	dout(10) << "import state=prepping : unpinning base+bounds " << *dir << dendl;
	import_reverse_prepping(dir, q->second);
	break;

      case IMPORT_PREPPED:
	ceph_assert(dir);
	dout(10) << "import state=prepped : unpinning base+bounds, unfreezing " << *dir << dendl;
	{
	  set<CDir*> bounds;
	  mdcache->get_subtree_bounds(dir, bounds);
	  import_remove_pins(dir, bounds);
	  
	  // adjust auth back to the exporter
	  mdcache->adjust_subtree_auth(dir, q->second.peer);

	  // notify bystanders ; wait in aborting state
	  q->second.state = IMPORT_ABORTING;
	  import_notify_abort(dir, bounds);
	  ceph_assert(g_conf()->mds_kill_import_at != 10);
	}
	break;

      case IMPORT_LOGGINGSTART:
	ceph_assert(dir);
	dout(10) << "import state=loggingstart : reversing import on " << *dir << dendl;
	import_reverse(dir);
	break;

      case IMPORT_ACKING:
	ceph_assert(dir);
	// hrm.  make this an ambiguous import, and wait for exporter recovery to disambiguate
	dout(10) << "import state=acking : noting ambiguous import " << *dir << dendl;
	{
	  set<CDir*> bounds;
	  mdcache->get_subtree_bounds(dir, bounds);
	  mdcache->add_ambiguous_import(dir, bounds);
	}
	break;
	
      case IMPORT_FINISHING:
	ceph_assert(dir);
	dout(10) << "import state=finishing : finishing import on " << *dir << dendl;
	import_finish(dir, true);
	break;

      case IMPORT_ABORTING:
	ceph_assert(dir);
	dout(10) << "import state=aborting : ignoring repeat failure " << *dir << dendl;
	break;
      }
    } else {
      auto bystanders_entry = q->second.bystanders.find(who);
      if (bystanders_entry != q->second.bystanders.end()) {
	q->second.bystanders.erase(bystanders_entry);
	if (q->second.state == IMPORT_ABORTING) {
	  ceph_assert(dir);
	  dout(10) << "faking export_notify_ack from mds." << who
		   << " on aborting import " << *dir << " from mds." << q->second.peer
		   << dendl;
	  if (q->second.bystanders.empty())
	    import_reverse_unfreeze(dir);
	}
      }
    }

    // next!
    q = next;
  }

  for (const auto& dir : pinned_dirs) {
    dout(10) << "removing temp auth_pin on " << *dir << dendl;
    dir->auth_unpin(this);
  }  
}



void Migrator::show_importing()
{  
  dout(10) << dendl;
  for (map<dirfrag_t,import_state_t>::iterator p = import_state.begin();
       p != import_state.end();
       ++p) {
    CDir *dir = mdcache->get_dirfrag(p->first);
    if (dir) {
      dout(10) << " importing from " << p->second.peer
	       << ": (" << p->second.state << ") " << get_import_statename(p->second.state)
	       << " " << p->first << " " << *dir << dendl;
    } else {
      dout(10) << " importing from " << p->second.peer
	       << ": (" << p->second.state << ") " << get_import_statename(p->second.state)
	       << " " << p->first << dendl;
    }
  }
}

void Migrator::show_exporting() 
{
  dout(10) << dendl;
  for (const auto& [dir, state] : export_state) {
    dout(10) << " exporting to " << state.peer
	     << ": (" << state.state << ") " << get_export_statename(state.state)
	     << " " << dir->dirfrag() << " " << *dir << dendl;
  }
}



void Migrator::audit()
{
  if (!g_conf()->subsys.should_gather<ceph_subsys_mds, 5>())
    return;  // hrm.

  // import_state
  show_importing();
  for (map<dirfrag_t,import_state_t>::iterator p = import_state.begin();
       p != import_state.end();
       ++p) {
    if (p->second.state == IMPORT_DISCOVERING)
      continue;
    if (p->second.state == IMPORT_DISCOVERED) {
      CInode *in = mdcache->get_inode(p->first.ino);
      ceph_assert(in);
      continue;
    }
    CDir *dir = mdcache->get_dirfrag(p->first);
    ceph_assert(dir);
    if (p->second.state == IMPORT_PREPPING)
      continue;
    if (p->second.state == IMPORT_ABORTING) {
      ceph_assert(!dir->is_ambiguous_dir_auth());
      ceph_assert(dir->get_dir_auth().first != mds->get_nodeid());
      continue;
    }
    ceph_assert(dir->is_ambiguous_dir_auth());
    ceph_assert(dir->authority().first  == mds->get_nodeid() ||
	   dir->authority().second == mds->get_nodeid());
  }

  // export_state
  show_exporting();
  for (map<CDir*,export_state_t>::iterator p = export_state.begin();
       p != export_state.end();
       ++p) {
    CDir *dir = p->first;
    if (p->second.state == EXPORT_LOCKING ||
	p->second.state == EXPORT_DISCOVERING ||
	p->second.state == EXPORT_FREEZING ||
	p->second.state == EXPORT_CANCELLING)
      continue;
    ceph_assert(dir->is_ambiguous_dir_auth());
    ceph_assert(dir->authority().first  == mds->get_nodeid() ||
	   dir->authority().second == mds->get_nodeid());
  }

  // ambiguous+me subtrees should be importing|exporting

  // write me
}





// ==========================================================
// EXPORT

void Migrator::export_dir_nicely(CDir *dir, mds_rank_t dest)
{
  // enqueue
  dout(7) << *dir << " to " << dest << dendl;
  export_queue.push_back(pair<dirfrag_t,mds_rank_t>(dir->dirfrag(), dest));

  maybe_do_queued_export();
}

void Migrator::maybe_do_queued_export()
{
  static bool running;
  if (running)
    return;
  running = true;

  uint64_t max_total_size = max_export_size * 2;

  while (!export_queue.empty() &&
	 max_total_size > total_exporting_size &&
	 max_total_size - total_exporting_size >=
	 max_export_size * (num_locking_exports + 1)) {

    dirfrag_t df = export_queue.front().first;
    mds_rank_t dest = export_queue.front().second;
    export_queue.pop_front();
    
    CDir *dir = mdcache->get_dirfrag(df);
    if (!dir) continue;
    if (!dir->is_auth()) continue;

    dout(7) << "nicely exporting to mds." << dest << " " << *dir << dendl;

    export_dir(dir, dest);
  }

  running = false;
}




class C_MDC_ExportFreeze : public MigratorContext {
  CDir *dir;   // dir i'm exporting
  uint64_t tid;
public:
  C_MDC_ExportFreeze(Migrator *m, CDir *e, uint64_t t) :
    MigratorContext(m), dir(e), tid(t) {
    dir->get(CDir::PIN_PTRWAITER);
  }
  void finish(int r) override {
    if (r >= 0)
      mig->export_frozen(dir, tid);
    dir->put(CDir::PIN_PTRWAITER);
  }
};


bool Migrator::export_try_grab_locks(CDir *dir, MutationRef& mut)
{
  CInode *diri = dir->get_inode();

  if (!diri->filelock.can_wrlock(diri->get_loner()) ||
      !diri->nestlock.can_wrlock(diri->get_loner()))
    return false;

  MutationImpl::LockOpVec lov;

  set<CDir*> wouldbe_bounds;
  set<CInode*> bound_inodes;
  mdcache->get_wouldbe_subtree_bounds(dir, wouldbe_bounds);
  for (auto& bound : wouldbe_bounds)
    bound_inodes.insert(bound->get_inode());
  for (auto& in : bound_inodes)
    lov.add_rdlock(&in->dirfragtreelock);

  lov.add_rdlock(&diri->dirfragtreelock);

  CInode* in = diri;
  while (true) {
    lov.add_rdlock(&in->snaplock);
    CDentry* pdn = in->get_projected_parent_dn();
    if (!pdn)
      break;
    in = pdn->get_dir()->get_inode();
  }

  if (!mds->locker->rdlock_try_set(lov, mut))
    return false;

  mds->locker->wrlock_force(&diri->filelock, mut);
  mds->locker->wrlock_force(&diri->nestlock, mut);

  return true;
}


/** export_dir(dir, dest)
 * public method to initiate an export.
 * will fail if the directory is freezing, frozen, unpinnable, or root. 
 */
void Migrator::export_dir(CDir *dir, mds_rank_t dest)
{
  ceph_assert(dir->is_auth());
  ceph_assert(dest != mds->get_nodeid());
   
  CDir* parent = dir->inode->get_projected_parent_dir();
  if (!mds->is_stopping() && !dir->is_exportable(dest) && dir->get_num_head_items() > 0) {
    dout(7) << "Cannot export to mds." << dest << " " << *dir << ": dir is export pinned" << dendl;
    return;
  } else if (!(mds->is_active() || mds->is_stopping())) {
    dout(7) << "Cannot export to mds." << dest << " " << *dir << ": not active" << dendl;
    return;
  } else if (mdcache->is_readonly()) {
    dout(7) << "Cannot export to mds." << dest << " " << *dir << ": read-only FS, no exports for now" << dendl;
    return;
  } else if (!mds->mdsmap->is_active(dest)) {
    dout(7) << "Cannot export to mds." << dest << " " << *dir << ": destination not active" << dendl;
    return;
  } else if (mds->is_cluster_degraded()) {
    dout(7) << "Cannot export to mds." << dest << " " << *dir << ": cluster degraded" << dendl;
    return;
  } else if (dir->inode->is_system()) {
    dout(7) << "Cannot export to mds." << dest << " " << *dir << ": is a system directory" << dendl;
    return;
  } else if (dir->is_frozen() || dir->is_freezing()) {
    dout(7) << "Cannot export to mds." << dest << " " << *dir << ": is frozen" << dendl;
    return;
  } else if (dir->state_test(CDir::STATE_EXPORTING)) {
    dout(7) << "Cannot export to mds." << dest << " " << *dir << ": already exporting" << dendl;
    return;
  } else if (parent && parent->inode->is_stray()
             && parent->get_parent_dir()->ino() != MDS_INO_MDSDIR(dest)) {
    dout(7) << "Cannot export to mds." << dest << " " << *dir << ": in stray directory" << dendl;
    return;
  }

  if (unlikely(g_conf()->mds_thrash_exports)) {
    // create random subtree bound (which will not be exported)
    std::vector<CDir*> ls;
    for (auto p = dir->begin(); p != dir->end(); ++p) {
      auto dn = p->second;
      CDentry::linkage_t *dnl= dn->get_linkage();
      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	if (in->is_dir()) {
          auto&& dirs = in->get_nested_dirfrags();
          ls.insert(std::end(ls), std::begin(dirs), std::end(dirs));
        }
      }
    }
    if (ls.size() > 0) {
      int n = rand() % ls.size();
      auto p = ls.begin();
      while (n--) ++p;
      CDir *bd = *p;
      if (!(bd->is_frozen() || bd->is_freezing())) {
	ceph_assert(bd->is_auth());
	dir->state_set(CDir::STATE_AUXSUBTREE);
	mdcache->adjust_subtree_auth(dir, mds->get_nodeid());
	dout(7) << "create aux subtree " << *bd << " under " << *dir << dendl;
      }
    }
  }

  dout(4) << "Starting export to mds." << dest << " " << *dir << dendl;

  mds->hit_export_target(dest, -1);

  dir->auth_pin(this);
  dir->mark_exporting();

  MDRequestRef mdr = mdcache->request_start_internal(CEPH_MDS_OP_EXPORTDIR);
  mdr->more()->export_dir = dir;
  mdr->pin(dir);

  ceph_assert(export_state.count(dir) == 0);
  export_state_t& stat = export_state[dir];
  num_locking_exports++;
  stat.state = EXPORT_LOCKING;
  stat.peer = dest;
  stat.tid = mdr->reqid.tid;
  stat.mut = mdr;

  mdcache->dispatch_request(mdr);
}

/*
 * check if directory is too large to be export in whole. If it is,
 * choose some subdirs, whose total size is suitable.
 */
void Migrator::maybe_split_export(CDir* dir, uint64_t max_size, bool null_okay,
				  vector<pair<CDir*, size_t> >& results)
{
  static const unsigned frag_size = 800;
  static const unsigned inode_size = 1000;
  static const unsigned cap_size = 80;
  static const unsigned remote_size = 10;
  static const unsigned null_size = 1;

  // state for depth-first search
  struct LevelData {
    CDir *dir;
    CDir::dentry_key_map::iterator iter;
    size_t dirfrag_size = frag_size;
    size_t subdirs_size = 0;
    bool complete = true;
    vector<CDir*> siblings;
    vector<pair<CDir*, size_t> > subdirs;
    LevelData(const LevelData&) = default;
    LevelData(CDir *d) :
      dir(d), iter(d->begin()) {}
  };

  vector<LevelData> stack;
  stack.emplace_back(dir);

  size_t found_size = 0;
  size_t skipped_size = 0;

  for (;;) {
    auto& data = stack.back();
    CDir *cur = data.dir;
    auto& it = data.iter;
    auto& dirfrag_size = data.dirfrag_size;

    while(it != cur->end()) {
      CDentry *dn = it->second;
      ++it;

      dirfrag_size += dn->name.size();
      if (dn->get_linkage()->is_null()) {
	dirfrag_size += null_size;
	continue;
      }
      if (dn->get_linkage()->is_remote()) {
	dirfrag_size += remote_size;
	continue;
      }

      CInode *in = dn->get_linkage()->get_inode();
      dirfrag_size += inode_size;
      dirfrag_size += in->get_client_caps().size() * cap_size;

      if (in->is_dir()) {
	auto ls = in->get_nested_dirfrags();
	std::reverse(ls.begin(), ls.end());

	bool complete = true;
	for (auto p = ls.begin(); p != ls.end(); ) {
	  if ((*p)->state_test(CDir::STATE_EXPORTING) ||
	      (*p)->is_freezing_dir() || (*p)->is_frozen_dir()) {
	    complete = false;
	    p = ls.erase(p);
	  } else {
	    ++p;
	  }
	}
	if (!complete) {
	  // skip exporting dir's ancestors. because they can't get
	  // frozen (exporting dir's parent inode is auth pinned).
	  for (auto p = stack.rbegin(); p < stack.rend(); ++p) {
	    if (!p->complete)
	      break;
	    p->complete = false;
	  }
	}
	if (!ls.empty()) {
	  stack.emplace_back(ls.back());
	  ls.pop_back();
	  stack.back().siblings.swap(ls);
	  break;
	}
      }
    }
    // did above loop push new dirfrag into the stack?
    if (stack.back().dir != cur)
      continue;

    if (data.complete) {
      auto cur_size = data.subdirs_size + dirfrag_size;
      // we can do nothing with large dirfrag
      if (cur_size >= max_size && found_size * 2 > max_size)
	break;

      found_size += dirfrag_size;

      if (stack.size() > 1) {
	auto& parent = stack[stack.size() - 2];
	parent.subdirs.emplace_back(cur, cur_size);
	parent.subdirs_size += cur_size;
      }
    } else {
      // can't merge current dirfrag to its parent if there is skipped subdir
      results.insert(results.end(), data.subdirs.begin(), data.subdirs.end());
      skipped_size += dirfrag_size;
    }

    vector<CDir*> ls;
    ls.swap(data.siblings);

    stack.pop_back();
    if (stack.empty())
      break;

    if (found_size >= max_size)
      break;

    // next dirfrag
    if (!ls.empty()) {
      stack.emplace_back(ls.back());
      ls.pop_back();
      stack.back().siblings.swap(ls);
    }
  }

  for (auto& p : stack)
    results.insert(results.end(), p.subdirs.begin(), p.subdirs.end());

  if (results.empty() && (!skipped_size || !null_okay))
    results.emplace_back(dir, found_size + skipped_size);
}

class C_M_ExportDirWait : public MigratorContext {
  MDRequestRef mdr;
  int count;
public:
  C_M_ExportDirWait(Migrator *m, MDRequestRef mdr, int count)
    : MigratorContext(m), mdr(mdr), count(count) {}
  void finish(int r) override {
    mig->dispatch_export_dir(mdr, count);
  }
};

void Migrator::dispatch_export_dir(const MDRequestRef& mdr, int count)
{
  CDir *dir = mdr->more()->export_dir;
  dout(7) << *mdr << " " << *dir << dendl;

  map<CDir*,export_state_t>::iterator it = export_state.find(dir);
  if (it == export_state.end() || it->second.tid != mdr->reqid.tid) {
    // export must have aborted.
    dout(7) << "export must have aborted " << *mdr << dendl;
    ceph_assert(mdr->killed || mdr->aborted);
    if (mdr->aborted) {
      mdr->aborted = false;
      mdcache->request_kill(mdr);
    }
    return;
  }
  ceph_assert(it->second.state == EXPORT_LOCKING);

  if (mdr->more()->peer_error || dir->is_frozen() || dir->is_freezing()) {
    dout(7) << "wouldblock|freezing|frozen, canceling export" << dendl;
    export_try_cancel(dir);
    return;
  }

  mds_rank_t dest = it->second.peer;
  if (!mds->is_export_target(dest)) {
    dout(7) << "dest is not yet an export target" << dendl;
    if (count > 3) {
      dout(7) << "dest has not been added as export target after three MDSMap epochs, canceling export" << dendl;
      export_try_cancel(dir);
      return;
    }

    mds->locker->drop_locks(mdr.get());
    mdr->drop_local_auth_pins();

    mds->wait_for_mdsmap(mds->mdsmap->get_epoch(), new C_M_ExportDirWait(this, mdr, count+1));
    return;
  }

  if (!dir->inode->get_parent_dn()) {
    dout(7) << "waiting for dir to become stable before export: " << *dir << dendl;
    dir->add_waiter(CDir::WAIT_CREATED, new C_M_ExportDirWait(this, mdr, 1));
    return;
  }

  // locks?
  if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
    MutationImpl::LockOpVec lov;
    // If auth MDS of the subtree root inode is neither the exporter MDS
    // nor the importer MDS and it gathers subtree root's fragstat/neststat
    // while the subtree is exporting. It's possible that the exporter MDS
    // and the importer MDS both are auth MDS of the subtree root or both
    // are not auth MDS of the subtree root at the time they receive the
    // lock messages. So the auth MDS of the subtree root inode may get no
    // or duplicated fragstat/neststat for the subtree root dirfrag.
    lov.lock_scatter_gather(&dir->get_inode()->filelock);
    lov.lock_scatter_gather(&dir->get_inode()->nestlock);
    if (dir->get_inode()->is_auth()) {
      dir->get_inode()->filelock.set_scatter_wanted();
      dir->get_inode()->nestlock.set_scatter_wanted();
    }
    lov.add_rdlock(&dir->get_inode()->dirfragtreelock);

    if (!mds->locker->acquire_locks(mdr, lov, nullptr, {}, true)) {
      if (mdr->aborted)
	export_try_cancel(dir);
      return;
    }

    lov.clear();
    // bound dftlocks:
    // NOTE: We need to take an rdlock on bounding dirfrags during
    //  migration for a rather irritating reason: when we export the
    //  bound inode, we need to send scatterlock state for the dirfrags
    //  as well, so that the new auth also gets the correct info.  If we
    //  race with a refragment, this info is useless, as we can't
    //  redivvy it up.  And it's needed for the scatterlocks to work
    //  properly: when the auth is in a sync/lock state it keeps each
    //  dirfrag's portion in the local (auth OR replica) dirfrag.
    set<CDir*> wouldbe_bounds;
    set<CInode*> bound_inodes;
    mdcache->get_wouldbe_subtree_bounds(dir, wouldbe_bounds);
    for (auto& bound : wouldbe_bounds)
      bound_inodes.insert(bound->get_inode());
    for (auto& in : bound_inodes)
      lov.add_rdlock(&in->dirfragtreelock);

    if (!mds->locker->rdlock_try_set(lov, mdr))
      return;

    if (!mds->locker->try_rdlock_snap_layout(dir->get_inode(), mdr))
      return;

    mdr->locking_state |= MutationImpl::ALL_LOCKED;
  }

  ceph_assert(g_conf()->mds_kill_export_at != 1);

  auto parent = it->second.parent;

  vector<pair<CDir*, size_t> > results;
  maybe_split_export(dir, max_export_size, (bool)parent, results);

  if (results.size() == 1 && results.front().first == dir) {
    num_locking_exports--;
    it->second.state = EXPORT_DISCOVERING;
    // send ExportDirDiscover (ask target)
    filepath path;
    dir->inode->make_path(path);
    auto discover = make_message<MExportDirDiscover>(dir->dirfrag(), path,
						     mds->get_nodeid(),
						     it->second.tid);
    mds->send_message_mds(discover, dest);
    ceph_assert(g_conf()->mds_kill_export_at != 2);

    it->second.last_cum_auth_pins_change = ceph_clock_now();
    it->second.approx_size = results.front().second;
    total_exporting_size += it->second.approx_size;

    // start the freeze, but hold it up with an auth_pin.
    dir->freeze_tree();
    ceph_assert(dir->is_freezing_tree());
    dir->add_waiter(CDir::WAIT_FROZEN, new C_MDC_ExportFreeze(this, dir, it->second.tid));
    return;
  }

  if (parent) {
    parent->pending_children += results.size();
  } else {
    parent = std::make_shared<export_base_t>(dir->dirfrag(), dest,
					     results.size(), export_queue_gen);
  }

  if (results.empty()) {
    dout(7) << "subtree's children all are under exporting, retry rest parts of parent export "
	    << parent->dirfrag << dendl;
    parent->restart = true;
  } else {
    dout(7) << "subtree is too large, splitting it into: " <<  dendl;
  }

  for (auto& p : results) {
    CDir *sub = p.first;
    ceph_assert(sub != dir);
    dout(7) << " sub " << *sub << dendl;

    sub->auth_pin(this);
    sub->mark_exporting();

    MDRequestRef _mdr = mdcache->request_start_internal(CEPH_MDS_OP_EXPORTDIR);
    _mdr->more()->export_dir = sub;
    _mdr->pin(sub);

    ceph_assert(export_state.count(sub) == 0);
    auto& stat = export_state[sub];
    num_locking_exports++;
    stat.state = EXPORT_LOCKING;
    stat.peer = dest;
    stat.tid = _mdr->reqid.tid;
    stat.mut = _mdr;
    stat.parent = parent;
    mdcache->dispatch_request(_mdr);
  }

  // cancel the original one
  export_try_cancel(dir);
}

void Migrator::child_export_finish(std::shared_ptr<export_base_t>& parent, bool success)
{
  if (success)
    parent->restart = true;
  if (--parent->pending_children == 0) {
    if (parent->restart &&
	parent->export_queue_gen == export_queue_gen) {
      CDir *origin = mdcache->get_dirfrag(parent->dirfrag);
      if (origin && origin->is_auth()) {
	dout(7) << "child_export_finish requeue " << *origin << dendl;
	export_queue.emplace_front(origin->dirfrag(), parent->dest);
      }
    }
  }
}

/*
 * called on receipt of MExportDirDiscoverAck
 * the importer now has the directory's _inode_ in memory, and pinned.
 */
void Migrator::handle_export_discover_ack(const cref_t<MExportDirDiscoverAck> &m)
{
  CDir *dir = mdcache->get_dirfrag(m->get_dirfrag());
  mds_rank_t dest(m->get_source().num());
  ceph_assert(dir);
  
  dout(7) << "from " << m->get_source()
	  << " on " << *dir << dendl;

  mds->hit_export_target(dest, -1);

  map<CDir*,export_state_t>::iterator it = export_state.find(dir);
  if (it == export_state.end() ||
      it->second.tid != m->get_tid() ||
      it->second.peer != dest) {
    dout(7) << "must have aborted" << dendl;
  } else {
    ceph_assert(it->second.state == EXPORT_DISCOVERING);

    if (m->is_success()) {
      // release locks to avoid deadlock
      MDRequestRef mdr = static_cast<MDRequestImpl*>(it->second.mut.get());
      ceph_assert(mdr);
      mdcache->request_finish(mdr);
      it->second.mut.reset();
      // freeze the subtree
      it->second.state = EXPORT_FREEZING;
      dir->auth_unpin(this);
      ceph_assert(g_conf()->mds_kill_export_at != 3);

    } else {
      dout(7) << "peer failed to discover (not active?), canceling" << dendl;
      export_try_cancel(dir, false);
    }
  }
}

class C_M_ExportSessionsFlushed : public MigratorContext {
  CDir *dir;
  uint64_t tid;
public:
  C_M_ExportSessionsFlushed(Migrator *m, CDir *d, uint64_t t) :
    MigratorContext(m), dir(d), tid(t) {
    dir->get(CDir::PIN_PTRWAITER);
  }
  void finish(int r) override {
    mig->export_sessions_flushed(dir, tid);
    dir->put(CDir::PIN_PTRWAITER);
  }
};

void Migrator::export_sessions_flushed(CDir *dir, uint64_t tid)
{
  dout(7) << *dir << dendl;

  map<CDir*,export_state_t>::iterator it = export_state.find(dir);
  if (it == export_state.end() ||
      it->second.state == EXPORT_CANCELLING ||
      it->second.tid != tid) {
    // export must have aborted.
    dout(7) << "export must have aborted on " << dir << dendl;
    return;
  }

  ceph_assert(it->second.state == EXPORT_PREPPING || it->second.state == EXPORT_WARNING);
  ceph_assert(it->second.warning_ack_waiting.count(MDS_RANK_NONE) > 0);
  it->second.warning_ack_waiting.erase(MDS_RANK_NONE);
  if (it->second.state == EXPORT_WARNING && it->second.warning_ack_waiting.empty())
    export_go(dir);     // start export.
}

void Migrator::encode_export_prep_trace(bufferlist &final_bl, CDir *bound, 
                                        CDir *dir, export_state_t &es, 
                                        set<inodeno_t> &inodes_added, 
                                        set<dirfrag_t> &dirfrags_added)
{
  ENCODE_START(1, 1, final_bl);

  dout(7) << " started to encode dir " << *bound << dendl;
  CDir *cur = bound;
  bufferlist tracebl;
  char start = '-';
  
  while (1) {
    // don't repeat inodes
    if (inodes_added.count(cur->inode->ino()))
      break;
    inodes_added.insert(cur->inode->ino());

    // prepend dentry + inode
    ceph_assert(cur->inode->is_auth());
    bufferlist bl;
    mdcache->encode_replica_dentry(cur->inode->parent, es.peer, bl);
    dout(7) << "  added " << *cur->inode->parent << dendl;
    mdcache->encode_replica_inode(cur->inode, es.peer, bl, mds->mdsmap->get_up_features());
    dout(7) << "  added " << *cur->inode << dendl;
    bl.claim_append(tracebl);
    tracebl = std::move(bl);

    cur = cur->get_parent_dir();
    // don't repeat dirfrags
    if (dirfrags_added.count(cur->dirfrag()) || cur == dir) {
      start = 'd';  // start with dentry
      break;
    }
    dirfrags_added.insert(cur->dirfrag());

    // prepend dir
    mdcache->encode_replica_dir(cur, es.peer, bl);
    dout(7) << "  added " << *cur << dendl;
    bl.claim_append(tracebl);
    tracebl = std::move(bl);
    start = 'f';  // start with dirfrag
  }
  dirfrag_t df = cur->dirfrag();
  encode(df, final_bl);
  encode(start, final_bl);
  final_bl.claim_append(tracebl);
  
  ENCODE_FINISH(final_bl);
}

void Migrator::export_frozen(CDir *dir, uint64_t tid)
{
  dout(7) << *dir << dendl;

  map<CDir*,export_state_t>::iterator it = export_state.find(dir);
  if (it == export_state.end() || it->second.tid != tid) {
    dout(7) << "export must have aborted" << dendl;
    return;
  }

  ceph_assert(it->second.state == EXPORT_FREEZING);
  ceph_assert(dir->is_frozen_tree_root());

  it->second.mut = new MutationImpl();

  // ok, try to grab all my locks.
  CInode *diri = dir->get_inode();
  if ((diri->is_auth() && diri->is_frozen()) ||
      !export_try_grab_locks(dir, it->second.mut)) {
    dout(7) << "export_dir couldn't acquire all needed locks, failing. "
	    << *dir << dendl;
    export_try_cancel(dir);
    return;
  }

  if (diri->is_auth())
    it->second.mut->auth_pin(diri);

  mdcache->show_subtrees();

  // CDir::_freeze_tree() should have forced it into subtree.
  ceph_assert(dir->get_dir_auth() == mds_authority_t(mds->get_nodeid(), mds->get_nodeid()));
  // note the bounds.
  set<CDir*> bounds;
  mdcache->get_subtree_bounds(dir, bounds);

  // generate prep message, log entry.
  auto prep = make_message<MExportDirPrep>(dir->dirfrag(), it->second.tid);

  // include list of bystanders
  for (const auto &p : dir->get_replicas()) {
    if (p.first != it->second.peer) {
      dout(10) << "bystander mds." << p.first << dendl;
      prep->add_bystander(p.first);
    }
  }

  // include base dirfrag
  mdcache->encode_replica_dir(dir, it->second.peer, prep->basedir);
  
  /*
   * include spanning tree for all nested exports.
   * these need to be on the destination _before_ the final export so that
   * dir_auth updates on any nested exports are properly absorbed.
   * this includes inodes and dirfrags included in the subtree, but
   * only the inodes at the bounds.
   *
   * each trace is: df ('-' | ('f' dir | 'd') dentry inode (dir dentry inode)*)
   */
  set<inodeno_t> inodes_added;
  set<dirfrag_t> dirfrags_added;

  // check bounds
  for (auto &bound : bounds){
    // pin it.
    bound->get(CDir::PIN_EXPORTBOUND);
    bound->state_set(CDir::STATE_EXPORTBOUND);

    dout(7) << "  export bound " << *bound << dendl;
    prep->add_bound( bound->dirfrag() );
    
    bufferlist final_bl;
    encode_export_prep_trace(final_bl, bound, dir, it->second, inodes_added, dirfrags_added);
    prep->add_trace(final_bl);
  }

  // send.
  it->second.state = EXPORT_PREPPING;
  mds->send_message_mds(prep, it->second.peer);
  ceph_assert(g_conf()->mds_kill_export_at != 4);

  // make sure any new instantiations of caps are flushed out
  ceph_assert(it->second.warning_ack_waiting.empty());

  set<client_t> export_client_set;
  get_export_client_set(dir, export_client_set);

  MDSGatherBuilder gather(g_ceph_context);
  mds->server->flush_client_sessions(export_client_set, gather);
  if (gather.has_subs()) {
    it->second.warning_ack_waiting.insert(MDS_RANK_NONE);
    gather.set_finisher(new C_M_ExportSessionsFlushed(this, dir, it->second.tid));
    gather.activate();
  }
}

void Migrator::get_export_client_set(CDir *dir, set<client_t>& client_set)
{
  deque<CDir*> dfs;
  dfs.push_back(dir);
  while (!dfs.empty()) {
    CDir *dir = dfs.front();
    dfs.pop_front();
    for (auto& p : *dir) {
      CDentry *dn = p.second;
      if (!dn->get_linkage()->is_primary())
	continue;
      CInode *in = dn->get_linkage()->get_inode();
      if (in->is_dir()) {
	// directory?
	auto&& ls = in->get_dirfrags();
	for (auto& q : ls) {
	  if (!q->state_test(CDir::STATE_EXPORTBOUND)) {
	    // include nested dirfrag
	    ceph_assert(q->get_dir_auth().first == CDIR_AUTH_PARENT);
	    dfs.push_back(q); // it's ours, recurse (later)
	  }
	}
      }
      for (auto& q : in->get_client_caps()) {
	client_set.insert(q.first);
      }
    }
  }
}

void Migrator::get_export_client_set(CInode *in, set<client_t>& client_set)
{
  for (const auto &p : in->get_client_caps()) {
    client_set.insert(p.first);
  }
}

void Migrator::handle_export_prep_ack(const cref_t<MExportDirPrepAck> &m)
{
  CDir *dir = mdcache->get_dirfrag(m->get_dirfrag());
  mds_rank_t dest(m->get_source().num());
  ceph_assert(dir);

  dout(7) << *dir << dendl;

  mds->hit_export_target(dest, -1);

  map<CDir*,export_state_t>::iterator it = export_state.find(dir);
  if (it == export_state.end() ||
      it->second.tid != m->get_tid() ||
      it->second.peer != mds_rank_t(m->get_source().num())) {
    // export must have aborted.  
    dout(7) << "export must have aborted" << dendl;
    return;
  }
  ceph_assert(it->second.state == EXPORT_PREPPING);

  if (!m->is_success()) {
    dout(7) << "peer couldn't acquire all needed locks or wasn't active, canceling" << dendl;
    export_try_cancel(dir, false);
    return;
  }

  ceph_assert(g_conf()->mds_kill_export_at != 5);
  // send warnings
  set<CDir*> bounds;
  mdcache->get_subtree_bounds(dir, bounds);

  ceph_assert(it->second.warning_ack_waiting.empty() ||
         (it->second.warning_ack_waiting.size() == 1 &&
	  it->second.warning_ack_waiting.count(MDS_RANK_NONE) > 0));
  ceph_assert(it->second.notify_ack_waiting.empty());

  for (const auto &p : dir->get_replicas()) {
    if (p.first == it->second.peer) continue;
    if (mds->is_cluster_degraded() &&
	!mds->mdsmap->is_clientreplay_or_active_or_stopping(p.first))
      continue;  // only if active
    it->second.warning_ack_waiting.insert(p.first);
    it->second.notify_ack_waiting.insert(p.first);  // we'll eventually get a notifyack, too!

    auto notify = make_message<MExportDirNotify>(dir->dirfrag(), it->second.tid, true,
        mds_authority_t(mds->get_nodeid(),CDIR_AUTH_UNKNOWN),
        mds_authority_t(mds->get_nodeid(),it->second.peer));
    for (auto &cdir : bounds) {
      notify->get_bounds().push_back(cdir->dirfrag());
    }
    mds->send_message_mds(notify, p.first);
    
  }

  it->second.state = EXPORT_WARNING;

  ceph_assert(g_conf()->mds_kill_export_at != 6);
  // nobody to warn?
  if (it->second.warning_ack_waiting.empty())
    export_go(dir);  // start export.
}


class C_M_ExportGo : public MigratorContext {
  CDir *dir;
  uint64_t tid;
public:
  C_M_ExportGo(Migrator *m, CDir *d, uint64_t t) :
    MigratorContext(m), dir(d), tid(t) {
    dir->get(CDir::PIN_PTRWAITER);
  }
  void finish(int r) override {
    mig->export_go_synced(dir, tid);
    dir->put(CDir::PIN_PTRWAITER);
  }
};

void Migrator::export_go(CDir *dir)
{
  auto it = export_state.find(dir);
  ceph_assert(it != export_state.end());
  dout(7) << *dir << " to " << it->second.peer << dendl;

  // first sync log to flush out e.g. any cap imports
  mds->mdlog->wait_for_safe(new C_M_ExportGo(this, dir, it->second.tid));
  mds->mdlog->flush();
}

void Migrator::export_go_synced(CDir *dir, uint64_t tid)
{
  map<CDir*,export_state_t>::iterator it = export_state.find(dir);
  if (it == export_state.end() ||
      it->second.state == EXPORT_CANCELLING ||
      it->second.tid != tid) {
    // export must have aborted.  
    dout(7) << "export must have aborted on " << dir << dendl;
    return;
  }
  ceph_assert(it->second.state == EXPORT_WARNING);
  mds_rank_t dest = it->second.peer;

  dout(7) << *dir << " to " << dest << dendl;

  mdcache->show_subtrees();
  
  it->second.state = EXPORT_EXPORTING;
  ceph_assert(g_conf()->mds_kill_export_at != 7);

  ceph_assert(dir->is_frozen_tree_root());

  // set ambiguous auth
  mdcache->adjust_subtree_auth(dir, mds->get_nodeid(), dest);

  // take away the popularity we're sending.
  mds->balancer->subtract_export(dir);
  
  // fill export message with cache data
  auto req = make_message<MExportDir>(dir->dirfrag(), it->second.tid);
  map<client_t,entity_inst_t> exported_client_map;
  map<client_t,client_metadata_t> exported_client_metadata_map;
  uint64_t num_exported_inodes = 0;
  encode_export_dir(req->export_data, dir, // recur start point
                    exported_client_map, exported_client_metadata_map,
                    num_exported_inodes);
  encode(exported_client_map, req->client_map, mds->mdsmap->get_up_features());
  encode(exported_client_metadata_map, req->client_map);

  // add bounds to message
  set<CDir*> bounds;
  mdcache->get_subtree_bounds(dir, bounds);
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p)
    req->add_export((*p)->dirfrag());

  // send
  mds->send_message_mds(req, dest);
  ceph_assert(g_conf()->mds_kill_export_at != 8);

  mds->hit_export_target(dest, num_exported_inodes+1);

  // stats
  if (mds->logger) mds->logger->inc(l_mds_exported);
  if (mds->logger) mds->logger->inc(l_mds_exported_inodes, num_exported_inodes);

  mdcache->show_subtrees();
}


/** encode_export_inode
 * update our local state for this inode to export.
 * encode relevant state to be sent over the wire.
 * used by: encode_export_dir, file_rename (if foreign)
 *
 * FIXME: the separation between CInode.encode_export and these methods 
 * is pretty arbitrary and dumb.
 */
void Migrator::encode_export_inode(CInode *in, bufferlist& enc_state, 
				   map<client_t,entity_inst_t>& exported_client_map,
				   map<client_t,client_metadata_t>& exported_client_metadata_map)
{
  ENCODE_START(1, 1, enc_state);
  dout(7) << *in << dendl;
  ceph_assert(!in->is_replica(mds->get_nodeid()));

  encode(in->ino(), enc_state);
  encode(in->last, enc_state);
  in->encode_export(enc_state);

  // caps 
  encode_export_inode_caps(in, true, enc_state, exported_client_map, exported_client_metadata_map);
  ENCODE_FINISH(enc_state);
}

void Migrator::encode_export_inode_caps(CInode *in, bool auth_cap, bufferlist& bl,
					map<client_t,entity_inst_t>& exported_client_map,
					map<client_t,client_metadata_t>& exported_client_metadata_map)
{
  ENCODE_START(1, 1, bl);
  dout(20) << *in << dendl;
  // encode caps
  map<client_t,Capability::Export> cap_map;
  in->export_client_caps(cap_map);
  encode(cap_map, bl);
  if (auth_cap) {
    encode(in->get_mds_caps_wanted(), bl);

    in->state_set(CInode::STATE_EXPORTINGCAPS);
    in->get(CInode::PIN_EXPORTINGCAPS);
  }

  // make note of clients named by exported capabilities
  for (const auto &p : in->get_client_caps()) {
    if (exported_client_map.count(p.first))
      continue;
    Session *session =  mds->sessionmap.get_session(entity_name_t::CLIENT(p.first.v));
    exported_client_map[p.first] = session->info.inst;
    exported_client_metadata_map[p.first] = session->info.client_metadata;
  }
  ENCODE_FINISH(bl);
}

void Migrator::finish_export_inode_caps(CInode *in, mds_rank_t peer,
					map<client_t,Capability::Import>& peer_imported)
{
  dout(20) << *in << dendl;

  in->state_clear(CInode::STATE_EXPORTINGCAPS);
  in->put(CInode::PIN_EXPORTINGCAPS);

  // tell (all) clients about migrating caps.. 
  for (const auto &p : in->get_client_caps()) {
    const Capability *cap = &p.second;
    dout(7) << p.first
	    << " exported caps on " << *in << dendl;
    auto m = make_message<MClientCaps>(CEPH_CAP_OP_EXPORT, in->ino(), 0,
				       cap->get_cap_id(), cap->get_mseq(),
				       mds->get_osd_epoch_barrier());
    map<client_t,Capability::Import>::iterator q = peer_imported.find(p.first);
    ceph_assert(q != peer_imported.end());
    m->set_cap_peer(q->second.cap_id, q->second.issue_seq, q->second.mseq,
		    (q->second.cap_id > 0 ? peer : -1), 0);
    mds->send_message_client_counted(m, p.first);
  }
  in->clear_client_caps_after_export();
  mds->locker->eval(in, CEPH_CAP_LOCKS);
}

void Migrator::finish_export_inode(CInode *in, mds_rank_t peer,
				   map<client_t,Capability::Import>& peer_imported,
				   MDSContext::vec& finished)
{
  dout(12) << *in << dendl;

  // clean
  if (in->is_dirty())
    in->mark_clean();
  
  // clear/unpin cached_by (we're no longer the authority)
  in->clear_replica_map();
  
  // twiddle lock states for auth -> replica transition
  in->authlock.export_twiddle();
  in->linklock.export_twiddle();
  in->dirfragtreelock.export_twiddle();
  in->filelock.export_twiddle();
  in->nestlock.export_twiddle();
  in->xattrlock.export_twiddle();
  in->snaplock.export_twiddle();
  in->flocklock.export_twiddle();
  in->policylock.export_twiddle();
  in->quiescelock.export_twiddle();
  
  // mark auth
  ceph_assert(in->is_auth());
  in->state_clear(CInode::STATE_AUTH);
  in->replica_nonce = CInode::EXPORT_NONCE;
  
  in->clear_dirty_rstat();

  // no more auth subtree? clear scatter dirty
  if (!in->has_subtree_root_dirfrag(mds->get_nodeid()))
    in->clear_scatter_dirty();

  in->clear_dirty_parent();

  in->clear_clientwriteable();

  in->clear_file_locks();

  // waiters
  in->take_waiting(CInode::WAIT_ANY_MASK, finished);

  in->finish_export();
  
  finish_export_inode_caps(in, peer, peer_imported);
}

void Migrator::encode_export_dir(bufferlist& exportbl,
				CDir *dir,
				map<client_t,entity_inst_t>& exported_client_map,
				map<client_t,client_metadata_t>& exported_client_metadata_map,
                                uint64_t &num_exported)
{
  // This has to be declared before ENCODE_STARTED as it will need to be referenced after ENCODE_FINISH.
  std::vector<CDir*> subdirs;
  
  ENCODE_START(1, 1, exportbl);
  dout(7) << *dir << " " << dir->get_num_head_items() << " head items" << dendl;
  
  ceph_assert(dir->get_projected_version() == dir->get_version());

#ifdef MDS_VERIFY_FRAGSTAT
  if (dir->is_complete())
    dir->verify_fragstat();
#endif

  // dir 
  dirfrag_t df = dir->dirfrag();
  encode(df, exportbl);
  dir->encode_export(exportbl);
  
  __u32 nden = dir->items.size();
  encode(nden, exportbl);
  
  // dentries
  for (auto &p : *dir) {
    CDentry *dn = p.second;
    CInode *in = dn->get_linkage()->get_inode();

    num_exported++;
    
    // -- dentry
    dout(7) << " exporting " << *dn << dendl;
    
    // dn name
    encode(dn->get_name(), exportbl);
    encode(dn->last, exportbl);
    
    // state
    dn->encode_export(exportbl);
    
    // points to...
    
    // null dentry?
    if (dn->get_linkage()->is_null()) {
      exportbl.append("N", 1);  // null dentry
      continue;
    }
    
    if (dn->get_linkage()->is_remote()) {
      inodeno_t ino = dn->get_linkage()->get_remote_ino();
      unsigned char d_type = dn->get_linkage()->get_remote_d_type();
      auto& alternate_name = dn->alternate_name;
      // remote link
      CDentry::encode_remote(ino, d_type, alternate_name, exportbl);
      continue;
    }

    // primary link
    // -- inode
    exportbl.append("i", 1);    // inode dentry

    ENCODE_START(2, 1, exportbl);
    encode_export_inode(in, exportbl, exported_client_map, exported_client_metadata_map);  // encode, and (update state for) export
    encode(dn->alternate_name, exportbl);
    ENCODE_FINISH(exportbl);

    // directory?
    auto&& dfs = in->get_dirfrags();
    for (const auto& t : dfs) {
      if (!t->state_test(CDir::STATE_EXPORTBOUND)) {
	// include nested dirfrag
	ceph_assert(t->get_dir_auth().first == CDIR_AUTH_PARENT);
	subdirs.push_back(t);  // it's ours, recurse (later)
      }
    }
  }

  ENCODE_FINISH(exportbl);
  // subdirs
  for (const auto &dir : subdirs) {
    encode_export_dir(exportbl, dir, exported_client_map, exported_client_metadata_map, num_exported);
  }
}

void Migrator::finish_export_dir(CDir *dir, mds_rank_t peer,
				 map<inodeno_t,map<client_t,Capability::Import> >& peer_imported,
				 MDSContext::vec& finished, int *num_dentries)
{
  dout(10) << *dir << dendl;

  // release open_by 
  dir->clear_replica_map();

  // mark
  ceph_assert(dir->is_auth());
  dir->state_clear(CDir::STATE_AUTH);
  dir->remove_bloom();
  dir->replica_nonce = CDir::EXPORT_NONCE;

  if (dir->is_dirty())
    dir->mark_clean();

  // suck up all waiters
  dir->take_waiting(CDir::WAIT_ANY_MASK, finished);    // all dir waiters
  
  // pop
  dir->finish_export();

  // dentries
  std::vector<CDir*> subdirs;
  for (auto &p : *dir) {
    CDentry *dn = p.second;
    CInode *in = dn->get_linkage()->get_inode();

    // dentry
    dn->finish_export();

    // inode?
    if (dn->get_linkage()->is_primary()) {
      finish_export_inode(in, peer, peer_imported[in->ino()], finished);

      // subdirs?
      auto&& dirs = in->get_nested_dirfrags();
      subdirs.insert(std::end(subdirs), std::begin(dirs), std::end(dirs));
    }

    mdcache->touch_dentry_bottom(dn); // move dentry to tail of LRU
    ++(*num_dentries);
  }

  // subdirs
  for (const auto& dir : subdirs) {
    finish_export_dir(dir, peer, peer_imported, finished, num_dentries);
  }
}

class C_MDS_ExportFinishLogged : public MigratorLogContext {
  CDir *dir;
public:
  C_MDS_ExportFinishLogged(Migrator *m, CDir *d) : MigratorLogContext(m), dir(d) {}
  void finish(int r) override {
    mig->export_logged_finish(dir);
  }
};


/*
 * i should get an export_ack from the export target.
 */
void Migrator::handle_export_ack(const cref_t<MExportDirAck> &m)
{
  CDir *dir = mdcache->get_dirfrag(m->get_dirfrag());
  mds_rank_t dest(m->get_source().num());
  ceph_assert(dir);
  ceph_assert(dir->is_frozen_tree_root());  // i'm exporting!

  // yay!
  dout(7) << *dir << dendl;

  mds->hit_export_target(dest, -1);

  map<CDir*,export_state_t>::iterator it = export_state.find(dir);
  ceph_assert(it != export_state.end());
  ceph_assert(it->second.state == EXPORT_EXPORTING);
  ceph_assert(it->second.tid == m->get_tid());

  auto bp = m->imported_caps.cbegin();
  decode(it->second.peer_imported, bp);

  it->second.state = EXPORT_LOGGINGFINISH;
  ceph_assert(g_conf()->mds_kill_export_at != 9);
  set<CDir*> bounds;
  mdcache->get_subtree_bounds(dir, bounds);

  // log completion. 
  //  include export bounds, to ensure they're in the journal.
  EExport *le = new EExport(mds->mdlog, dir, it->second.peer);;

  le->metablob.add_dir_context(dir, EMetaBlob::TO_ROOT);
  le->metablob.add_dir(dir, false);
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bound = *p;
    le->get_bounds().insert(bound->dirfrag());
    le->metablob.add_dir_context(bound);
    le->metablob.add_dir(bound, false);
  }

  // list us second, them first.
  // this keeps authority().first in sync with subtree auth state in the journal.
  mdcache->adjust_subtree_auth(dir, it->second.peer, mds->get_nodeid());

  // log export completion, then finish (unfreeze, trigger finish context, etc.)
  mds->mdlog->submit_entry(le, new C_MDS_ExportFinishLogged(this, dir));
  mds->mdlog->flush();
  ceph_assert(g_conf()->mds_kill_export_at != 10);
}

void Migrator::export_notify_abort(CDir *dir, export_state_t& stat, set<CDir*>& bounds)
{
  dout(7) << *dir << dendl;

  ceph_assert(stat.state == EXPORT_CANCELLING);

  if (stat.notify_ack_waiting.empty()) {
    stat.state = EXPORT_CANCELLED;
    return;
  }

  dir->auth_pin(this);

  for (set<mds_rank_t>::iterator p = stat.notify_ack_waiting.begin();
       p != stat.notify_ack_waiting.end();
       ++p) {
    auto notify = make_message<MExportDirNotify>(dir->dirfrag(), stat.tid, true,
        pair<int,int>(mds->get_nodeid(), stat.peer),
        pair<int,int>(mds->get_nodeid(), CDIR_AUTH_UNKNOWN));
    for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirfrag());
    mds->send_message_mds(notify, *p);
  }
}

/*
 * this happens if the dest failes after i send the export data but before it is acked
 * that is, we don't know they safely received and logged it, so we reverse our changes
 * and go on.
 */
void Migrator::export_reverse(CDir *dir, export_state_t& stat)
{
  dout(7) << *dir << dendl;

  set<CInode*> to_eval;

  set<CDir*> bounds;
  mdcache->get_subtree_bounds(dir, bounds);

  // remove exporting pins
  std::deque<CDir*> rq;
  rq.push_back(dir);
  while (!rq.empty()) {
    CDir *t = rq.front(); 
    rq.pop_front();
    t->abort_export();
    for (auto &p : *t) {
      CDentry *dn = p.second;
      dn->abort_export();
      if (!dn->get_linkage()->is_primary())
	continue;
      CInode *in = dn->get_linkage()->get_inode();
      in->abort_export();
      if (in->state_test(CInode::STATE_EVALSTALECAPS)) {
	in->state_clear(CInode::STATE_EVALSTALECAPS);
	to_eval.insert(in);
      }
      if (in->is_dir()) {
        auto&& dirs = in->get_nested_dirfrags();
        for (const auto& dir : dirs) {
          rq.push_back(dir);
        }
      }
    }
  }
  
  // unpin bounds
  for (auto bd : bounds) {
    bd->put(CDir::PIN_EXPORTBOUND);
    bd->state_clear(CDir::STATE_EXPORTBOUND);
  }

  // notify bystanders
  export_notify_abort(dir, stat, bounds);

  // unfreeze tree, with possible subtree merge.
  mdcache->adjust_subtree_auth(dir, mds->get_nodeid(), mds->get_nodeid());

  // process delayed expires
  mdcache->process_delayed_expire(dir);

  dir->unfreeze_tree();
  mdcache->try_subtree_merge(dir);

  // revoke/resume stale caps
  for (auto in : to_eval) {
    bool need_issue = false;
    for (auto &p : in->client_caps) {
      Capability *cap = &p.second;
      if (!cap->is_stale()) {
	need_issue = true;
	break;
      }
    }
    if (need_issue &&
	(!in->is_auth() || !mds->locker->eval(in, CEPH_CAP_LOCKS)))
      mds->locker->issue_caps(in);
  }

  mdcache->show_cache();
}


/*
 * once i get the ack, and logged the EExportFinish(true),
 * send notifies (if any), otherwise go straight to finish.
 * 
 */
void Migrator::export_logged_finish(CDir *dir)
{
  dout(7) << *dir << dendl;

  export_state_t& stat = export_state[dir];

  // send notifies
  set<CDir*> bounds;
  mdcache->get_subtree_bounds(dir, bounds);

  for (set<mds_rank_t>::iterator p = stat.notify_ack_waiting.begin();
       p != stat.notify_ack_waiting.end();
       ++p) {
    auto notify = make_message<MExportDirNotify>(dir->dirfrag(), stat.tid, true,
        pair<int,int>(mds->get_nodeid(), stat.peer),
        pair<int,int>(stat.peer, CDIR_AUTH_UNKNOWN));

    for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirfrag());
    
    mds->send_message_mds(notify, *p);
  }

  // wait for notifyacks
  stat.state = EXPORT_NOTIFYING;
  ceph_assert(g_conf()->mds_kill_export_at != 11);
  
  // no notifies to wait for?
  if (stat.notify_ack_waiting.empty()) {
    export_finish(dir);  // skip notify/notify_ack stage.
  } else {
    // notify peer to send cap import messages to clients
    if (!mds->is_cluster_degraded() ||
	mds->mdsmap->is_clientreplay_or_active_or_stopping(stat.peer)) {
      mds->send_message_mds(make_message<MExportDirFinish>(dir->dirfrag(), false, stat.tid), stat.peer);
    } else {
      dout(7) << "not sending MExportDirFinish, dest has failed" << dendl;
    }
  }
}

/*
 * warning:
 *  i'll get an ack from each bystander.
 *  when i get them all, do the export.
 * notify:
 *  i'll get an ack from each bystander.
 *  when i get them all, unfreeze and send the finish.
 */
void Migrator::handle_export_notify_ack(const cref_t<MExportDirNotifyAck> &m)
{
  CDir *dir = mdcache->get_dirfrag(m->get_dirfrag());
  mds_rank_t dest(m->get_source().num());
  ceph_assert(dir);
  mds_rank_t from = mds_rank_t(m->get_source().num());

  mds->hit_export_target(dest, -1);

  auto export_state_entry = export_state.find(dir);
  if (export_state_entry != export_state.end()) {
    export_state_t& stat = export_state_entry->second;
    if (stat.state == EXPORT_WARNING &&
	stat.warning_ack_waiting.erase(from)) {
      // exporting. process warning.
      dout(7) << "from " << m->get_source()
	      << ": exporting, processing warning on " << *dir << dendl;
      if (stat.warning_ack_waiting.empty())
	export_go(dir);     // start export.
    } else if (stat.state == EXPORT_NOTIFYING &&
	       stat.notify_ack_waiting.erase(from)) {
      // exporting. process notify.
      dout(7) << "from " << m->get_source()
	      << ": exporting, processing notify on " << *dir << dendl;
      if (stat.notify_ack_waiting.empty())
	export_finish(dir);
    } else if (stat.state == EXPORT_CANCELLING &&
	       m->get_new_auth().second == CDIR_AUTH_UNKNOWN && // not warning ack
	       stat.notify_ack_waiting.erase(from)) {
      dout(7) << "from " << m->get_source()
	      << ": cancelling export, processing notify on " << *dir << dendl;
      if (stat.notify_ack_waiting.empty()) {
	export_cancel_finish(export_state_entry);
      }
    }
  }
  else {
    auto import_state_entry = import_state.find(dir->dirfrag());
    if (import_state_entry != import_state.end()) {
      import_state_t& stat = import_state_entry->second;
      if (stat.state == IMPORT_ABORTING) {
	// reversing import
	dout(7) << "from " << m->get_source()
	  << ": aborting import on " << *dir << dendl;
	ceph_assert(stat.bystanders.count(from));
	stat.bystanders.erase(from);
	if (stat.bystanders.empty())
	  import_reverse_unfreeze(dir);
      }
    }
  }
}

void Migrator::export_finish(CDir *dir)
{
  dout(3) << *dir << dendl;

  ceph_assert(g_conf()->mds_kill_export_at != 12);
  map<CDir*,export_state_t>::iterator it = export_state.find(dir);
  if (it == export_state.end()) {
    dout(7) << "target must have failed, not sending final commit message.  export succeeded anyway." << dendl;
    return;
  }

  // send finish/commit to new auth
  if (!mds->is_cluster_degraded() ||
      mds->mdsmap->is_clientreplay_or_active_or_stopping(it->second.peer)) {
    mds->send_message_mds(make_message<MExportDirFinish>(dir->dirfrag(), true, it->second.tid), it->second.peer);
  } else {
    dout(7) << "not sending MExportDirFinish last, dest has failed" << dendl;
  }
  ceph_assert(g_conf()->mds_kill_export_at != 13);
  
  // finish export (adjust local cache state)
  int num_dentries = 0;
  MDSContext::vec finished;
  finish_export_dir(dir, it->second.peer,
		    it->second.peer_imported, finished, &num_dentries);

  ceph_assert(!dir->is_auth());
  mdcache->adjust_subtree_auth(dir, it->second.peer);

  // unpin bounds
  set<CDir*> bounds;
  mdcache->get_subtree_bounds(dir, bounds);
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = *p;
    bd->put(CDir::PIN_EXPORTBOUND);
    bd->state_clear(CDir::STATE_EXPORTBOUND);
  }

  if (dir->state_test(CDir::STATE_AUXSUBTREE))
    dir->state_clear(CDir::STATE_AUXSUBTREE);

  // discard delayed expires
  mdcache->discard_delayed_expire(dir);

  dout(7) << "unfreezing" << dendl;

  // unfreeze tree, with possible subtree merge.
  //  (we do this _after_ removing EXPORTBOUND pins, to allow merges)
  dir->unfreeze_tree();
  mdcache->try_subtree_merge(dir);

  // no more auth subtree? clear scatter dirty
  if (!dir->get_inode()->is_auth() &&
      !dir->get_inode()->has_subtree_root_dirfrag(mds->get_nodeid())) {
    dir->get_inode()->clear_scatter_dirty();
    // wake up scatter_nudge waiters
    dir->get_inode()->take_waiting(CInode::WAIT_ANY_MASK, finished);
  }

  if (!finished.empty())
    mds->queue_waiters(finished);

  MutationRef mut = std::move(it->second.mut);
  auto parent = std::move(it->second.parent);
  // remove from exporting list, clean up state
  total_exporting_size -= it->second.approx_size;
  export_state.erase(it);

  ceph_assert(dir->state_test(CDir::STATE_EXPORTING));
  dir->clear_exporting();

  mdcache->show_subtrees();
  audit();

  mdcache->trim(num_dentries); // try trimming exported dentries

  // send pending import_maps?
  mdcache->maybe_send_pending_resolves();

  // drop locks, unpin path
  if (mut) {
    mds->locker->drop_locks(mut.get());
    mut->cleanup();
  }

  if (parent)
    child_export_finish(parent, true);

  maybe_do_queued_export();
}



class C_MDS_ExportDiscover : public MigratorContext {
public:
  C_MDS_ExportDiscover(Migrator *mig, const cref_t<MExportDirDiscover>& m) : MigratorContext(mig), m(m) {}
  void finish(int r) override {
    mig->handle_export_discover(m, true);
  }
private:
  cref_t<MExportDirDiscover> m;
};

class C_MDS_ExportDiscoverFactory : public MDSContextFactory {
public:
  C_MDS_ExportDiscoverFactory(Migrator *mig, cref_t<MExportDirDiscover> m) : mig(mig), m(m) {}
  MDSContext *build() {
    return new C_MDS_ExportDiscover(mig, m);
  }
private:
  Migrator *mig;
  cref_t<MExportDirDiscover> m;
};

// ==========================================================
// IMPORT

void Migrator::handle_export_discover(const cref_t<MExportDirDiscover> &m, bool started)
{
  mds_rank_t from = m->get_source_mds();
  ceph_assert(from != mds->get_nodeid());

  dout(7) << m->get_path() << dendl;

  // note import state
  dirfrag_t df = m->get_dirfrag();

  if (!mds->is_active()) {
    dout(7) << " not active, send NACK " << dendl;
    mds->send_message_mds(make_message<MExportDirDiscoverAck>(df, m->get_tid(), false), from);
    return;
  }

  // only start discovering on this message once.
  import_state_t *p_state;
  map<dirfrag_t,import_state_t>::iterator it = import_state.find(df);
  if (!started) {
    ceph_assert(it == import_state.end());
    p_state = &import_state[df];
    p_state->state = IMPORT_DISCOVERING;
    p_state->peer = from;
    p_state->tid = m->get_tid();
  } else {
    // am i retrying after ancient path_traverse results?
    if (it == import_state.end() ||
	it->second.peer != from ||
	it->second.tid != m->get_tid()) {
      dout(7) << " dropping obsolete message" << dendl;
      return;
    }
    ceph_assert(it->second.state == IMPORT_DISCOVERING);
    p_state = &it->second;
  }

  C_MDS_ExportDiscoverFactory cf(this, m);
  if (!mdcache->is_open()) {
    dout(10) << " waiting for root" << dendl;
    mds->mdcache->wait_for_open(cf.build());
    return;
  }

  ceph_assert(g_conf()->mds_kill_import_at != 1);

  // do we have it?
  CInode *in = mdcache->get_inode(m->get_dirfrag().ino);
  if (!in) {
    // must discover it!
    filepath fpath(m->get_path());
    vector<CDentry*> trace;
    MDRequestRef null_ref;
    int r = mdcache->path_traverse(null_ref, cf, fpath,
				   MDS_TRAVERSE_DISCOVER | MDS_TRAVERSE_PATH_LOCKED,
				   &trace);
    if (r > 0) return;
    if (r < 0) {
      dout(7) << "failed to discover or not dir " << m->get_path() << ", NAK" << dendl;
      ceph_abort();    // this shouldn't happen if the auth pins its path properly!!!!
    }

    ceph_abort(); // this shouldn't happen; the get_inode above would have succeeded.
  }

  // yay
  dout(7) << "have " << df << " inode " << *in << dendl;
  
  p_state->state = IMPORT_DISCOVERED;

  // pin inode in the cache (for now)
  ceph_assert(in->is_dir());
  in->get(CInode::PIN_IMPORTING);

  // reply
  dout(7) << " sending export_discover_ack on " << *in << dendl;
  mds->send_message_mds(make_message<MExportDirDiscoverAck>(df, m->get_tid()), p_state->peer);
  ceph_assert(g_conf()->mds_kill_import_at != 2);
}

void Migrator::import_reverse_discovering(dirfrag_t df)
{
  import_state.erase(df);
}

void Migrator::import_reverse_discovered(dirfrag_t df, CInode *diri)
{
  // unpin base
  diri->put(CInode::PIN_IMPORTING);
  import_state.erase(df);
}

void Migrator::import_reverse_prepping(CDir *dir, import_state_t& stat)
{
  set<CDir*> bounds;
  mdcache->map_dirfrag_set(stat.bound_ls, bounds);
  import_remove_pins(dir, bounds);
  import_reverse_final(dir);
}

void Migrator::handle_export_cancel(const cref_t<MExportDirCancel> &m)
{
  dout(7) << "on " << m->get_dirfrag() << dendl;
  dirfrag_t df = m->get_dirfrag();
  map<dirfrag_t,import_state_t>::iterator it = import_state.find(df);
  if (it == import_state.end()) {
    ceph_abort_msg("got export_cancel in weird state");
  } else if (it->second.state == IMPORT_DISCOVERING) {
    import_reverse_discovering(df);
  } else if (it->second.state == IMPORT_DISCOVERED) {
    CInode *in = mdcache->get_inode(df.ino);
    ceph_assert(in);
    import_reverse_discovered(df, in);
  } else if (it->second.state == IMPORT_PREPPING) {
    CDir *dir = mdcache->get_dirfrag(df);
    ceph_assert(dir);
    import_reverse_prepping(dir, it->second);
  } else if (it->second.state == IMPORT_PREPPED) {
    CDir *dir = mdcache->get_dirfrag(df);
    ceph_assert(dir);
    set<CDir*> bounds;
    mdcache->get_subtree_bounds(dir, bounds);
    import_remove_pins(dir, bounds);
    // adjust auth back to the exportor
    mdcache->adjust_subtree_auth(dir, it->second.peer);
    import_reverse_unfreeze(dir);
  } else {
    ceph_abort_msg("got export_cancel in weird state");
  }
}

class C_MDS_ExportPrep : public MigratorContext {
public:
  C_MDS_ExportPrep(Migrator *mig, const cref_t<MExportDirPrep>& m) : MigratorContext(mig), m(m) {}
  void finish(int r) override {
    mig->handle_export_prep(m, true);
  }
private:
  cref_t<MExportDirPrep> m;
};

class C_MDS_ExportPrepFactory : public MDSContextFactory {
public:
  C_MDS_ExportPrepFactory(Migrator *mig, cref_t<MExportDirPrep> m) : mig(mig), m(m) {}
  MDSContext *build() {
    return new C_MDS_ExportPrep(mig, m);
  }
private:
  Migrator *mig;
  cref_t<MExportDirPrep> m;
};

void Migrator::decode_export_prep_trace(bufferlist::const_iterator& blp, mds_rank_t oldauth, MDSContext::vec& finished)
{
  DECODE_START(1, blp);
  dirfrag_t df;
  decode(df, blp);
  char start;
  decode(start, blp);
  dout(10) << " trace from " << df << " start " << start << dendl;
  
  CDir *cur = nullptr;
  if (start == 'd') {
    cur = mdcache->get_dirfrag(df);
    ceph_assert(cur);
    dout(10) << "  had " << *cur << dendl;
  } else if (start == 'f') {
    CInode *in = mdcache->get_inode(df.ino);
    ceph_assert(in); 
    dout(10) << "  had " << *in << dendl; 
    mdcache->decode_replica_dir(cur, blp, in, oldauth, finished);
    dout(10) << "  added " << *cur << dendl;
  } else if (start == '-') {
    // nothing
  } else
    ceph_abort_msg("unrecognized start char");

  while (!blp.end()) {
    CDentry *dn = nullptr;
    mdcache->decode_replica_dentry(dn, blp, cur, finished);
    dout(10) << "  added " << *dn << dendl;
    CInode *in = nullptr;
    mdcache->decode_replica_inode(in, blp, dn, finished);
    dout(10) << "  added " << *in << dendl;
    if (blp.end())
      break;
    mdcache->decode_replica_dir(cur, blp, in, oldauth, finished);
    dout(10) << "  added " << *cur << dendl;
  }
  
  DECODE_FINISH(blp);
}

void Migrator::handle_export_prep(const cref_t<MExportDirPrep> &m, bool did_assim)
{
  mds_rank_t oldauth = mds_rank_t(m->get_source().num());
  ceph_assert(oldauth != mds->get_nodeid());

  CDir *dir;
  CInode *diri;
  MDSContext::vec finished;

  // assimilate root dir.
  map<dirfrag_t,import_state_t>::iterator it = import_state.find(m->get_dirfrag());
  if (!did_assim) {
    ceph_assert(it != import_state.end());
    ceph_assert(it->second.state == IMPORT_DISCOVERED);
    ceph_assert(it->second.peer == oldauth);
    diri = mdcache->get_inode(m->get_dirfrag().ino);
    ceph_assert(diri);
    auto p = m->basedir.cbegin();
    mdcache->decode_replica_dir(dir, p, diri, oldauth, finished);
    dout(7) << "on " << *dir << " (first pass)" << dendl;
  } else {
    if (it == import_state.end() ||
	it->second.peer != oldauth ||
	it->second.tid != m->get_tid()) {
      dout(7) << "obsolete message, dropping" << dendl;
      return;
    }
    ceph_assert(it->second.state == IMPORT_PREPPING);
    ceph_assert(it->second.peer == oldauth);

    dir = mdcache->get_dirfrag(m->get_dirfrag());
    ceph_assert(dir);
    dout(7) << "on " << *dir << " (subsequent pass)" << dendl;
    diri = dir->get_inode();
  }
  ceph_assert(dir->is_auth() == false);

  mdcache->show_subtrees();

  // build import bound map
  map<inodeno_t, fragset_t> import_bound_fragset;
  for (const auto &bound : m->get_bounds()) {
    dout(10) << " bound " << bound << dendl;
    import_bound_fragset[bound.ino].insert_raw(bound.frag);
  }
  // assimilate contents?
  if (!did_assim) {
    dout(7) << "doing assim on " << *dir << dendl;

    // change import state
    it->second.state = IMPORT_PREPPING;
    it->second.bound_ls = m->get_bounds();
    it->second.bystanders = m->get_bystanders();
    ceph_assert(g_conf()->mds_kill_import_at != 3);

    // bystander list
    dout(7) << "bystanders are " << it->second.bystanders << dendl;

    // move pin to dir
    diri->put(CInode::PIN_IMPORTING);
    dir->get(CDir::PIN_IMPORTING);  
    dir->state_set(CDir::STATE_IMPORTING);

    // assimilate traces to exports
    // each trace is: df ('-' | ('f' dir | 'd') dentry inode (dir dentry inode)*)
    for (const auto &bl : m->traces) {
      auto blp = bl.cbegin();
      decode_export_prep_trace(blp, oldauth, finished);
    }

    // make bound sticky
    for (map<inodeno_t,fragset_t>::iterator p = import_bound_fragset.begin();
	 p != import_bound_fragset.end();
	 ++p) {
      p->second.simplify();
      CInode *in = mdcache->get_inode(p->first);
      ceph_assert(in);
      in->get_stickydirs();
      dout(7) << " set stickydirs on bound inode " << *in << dendl;
    }

  } else {
    dout(7) << " not doing assim on " << *dir << dendl;
  }

  MDSGatherBuilder gather(g_ceph_context);

  if (!finished.empty())
    mds->queue_waiters(finished);


  bool success = true;
  if (mds->is_active()) {
    // open all bounds
    set<CDir*> import_bounds;
    for (map<inodeno_t,fragset_t>::iterator p = import_bound_fragset.begin();
	 p != import_bound_fragset.end();
	 ++p) {
      CInode *in = mdcache->get_inode(p->first);
      ceph_assert(in);

      // map fragset into a frag_t list, based on the inode fragtree
      frag_vec_t leaves;
      for (const auto& frag : p->second) {
	in->dirfragtree.get_leaves_under(frag, leaves);
      }
      dout(10) << " bound inode " << p->first << " fragset " << p->second << " maps to " << leaves << dendl;

      for (const auto& leaf : leaves) {
	CDir *bound = mdcache->get_dirfrag(dirfrag_t(p->first, leaf));
	if (!bound) {
	  dout(7) << "  opening bounding dirfrag " << leaf << " on " << *in << dendl;
	  mdcache->open_remote_dirfrag(in, leaf, gather.new_sub());
	  continue;
	}

	if (!bound->state_test(CDir::STATE_IMPORTBOUND)) {
	  dout(7) << "  pinning import bound " << *bound << dendl;
	  bound->get(CDir::PIN_IMPORTBOUND);
	  bound->state_set(CDir::STATE_IMPORTBOUND);
	} else {
	  dout(7) << "  already pinned import bound " << *bound << dendl;
	}
	import_bounds.insert(bound);
      }
    }

    if (gather.has_subs()) {
      C_MDS_ExportPrepFactory cf(this, m);
      gather.set_finisher(cf.build());
      gather.activate();
      return;
    }

    dout(7) << " all ready, noting auth and freezing import region" << dendl;

    if (!mdcache->is_readonly() &&
	// for pinning scatter gather. loner has a higher chance to get wrlock
	diri->filelock.can_wrlock(diri->get_loner()) &&
	diri->nestlock.can_wrlock(diri->get_loner())) {
      it->second.mut = new MutationImpl();
      // force some locks.  hacky.
      mds->locker->wrlock_force(&dir->inode->filelock, it->second.mut);
      mds->locker->wrlock_force(&dir->inode->nestlock, it->second.mut);

      // note that i am an ambiguous auth for this subtree.
      // specify bounds, since the exporter explicitly defines the region.
      mdcache->adjust_bounded_subtree_auth(dir, import_bounds,
					 pair<int,int>(oldauth, mds->get_nodeid()));
      mdcache->verify_subtree_bounds(dir, import_bounds);
      // freeze.
      dir->_freeze_tree();
      // note new state
      it->second.state = IMPORT_PREPPED;
    } else {
      dout(7) << " couldn't acquire all needed locks, failing. " << *dir << dendl;
      success = false;
    }
  } else {
    dout(7) << " not active, failing. " << *dir << dendl;
    success = false;
  }

  if (!success)
    import_reverse_prepping(dir, it->second);

  // ok!
  dout(7) << " sending export_prep_ack on " << *dir << dendl;
  mds->send_message(make_message<MExportDirPrepAck>(dir->dirfrag(), success, m->get_tid()), m->get_connection());

  ceph_assert(g_conf()->mds_kill_import_at != 4);
}




class C_MDS_ImportDirLoggedStart : public MigratorLogContext {
  dirfrag_t df;
  CDir *dir;
  mds_rank_t from;
public:
  map<client_t,pair<Session*,uint64_t> > imported_session_map;

  C_MDS_ImportDirLoggedStart(Migrator *m, CDir *d, mds_rank_t f) :
    MigratorLogContext(m), df(d->dirfrag()), dir(d), from(f) {
    dir->get(CDir::PIN_PTRWAITER);
  }
  void finish(int r) override {
    mig->import_logged_start(df, dir, from, imported_session_map);
    dir->put(CDir::PIN_PTRWAITER);
  }
};

void Migrator::handle_export_dir(const cref_t<MExportDir> &m)
{
  ceph_assert(g_conf()->mds_kill_import_at != 5);
  CDir *dir = mdcache->get_dirfrag(m->dirfrag);
  ceph_assert(dir);

  mds_rank_t oldauth = mds_rank_t(m->get_source().num());
  dout(7) << "importing " << *dir << " from " << oldauth << dendl;

  ceph_assert(!dir->is_auth());
  ceph_assert(dir->freeze_tree_state);
  
  map<dirfrag_t,import_state_t>::iterator it = import_state.find(m->dirfrag);
  ceph_assert(it != import_state.end());
  ceph_assert(it->second.state == IMPORT_PREPPED);
  ceph_assert(it->second.tid == m->get_tid());
  ceph_assert(it->second.peer == oldauth);

  if (!dir->get_inode()->dirfragtree.is_leaf(dir->get_frag()))
    dir->get_inode()->dirfragtree.force_to_leaf(g_ceph_context, dir->get_frag());

  mdcache->show_subtrees();

  C_MDS_ImportDirLoggedStart *onlogged = new C_MDS_ImportDirLoggedStart(this, dir, oldauth);

  // start the journal entry
  EImportStart *le = new EImportStart(mds->mdlog, dir->dirfrag(), m->bounds, oldauth);

  le->metablob.add_dir_context(dir);
  
  // adjust auth (list us _first_)
  mdcache->adjust_subtree_auth(dir, mds->get_nodeid(), oldauth);

  // new client sessions, open these after we journal
  // include imported sessions in EImportStart
  auto cmp = m->client_map.cbegin();
  map<client_t,entity_inst_t> client_map;
  map<client_t,client_metadata_t> client_metadata_map;
  decode(client_map, cmp);
  decode(client_metadata_map, cmp);
  ceph_assert(cmp.end());
  le->cmapv = mds->server->prepare_force_open_sessions(client_map, client_metadata_map,
						       onlogged->imported_session_map);
  encode(client_map, le->client_map, mds->mdsmap->get_up_features());
  encode(client_metadata_map, le->client_map);

  auto blp = m->export_data.cbegin();
  int num_imported_inodes = 0;
  while (!blp.end()) {
    decode_import_dir(blp,
                      oldauth, 
                      dir,                 // import root
                      le,
                      mds->mdlog->get_current_segment(),
                      it->second.peer_exports,
                      it->second.updated_scatterlocks,
                      num_imported_inodes);
  }
  dout(10) << " " << m->bounds.size() << " imported bounds" << dendl;
  
  // include bounds in EImportStart
  set<CDir*> import_bounds;
  for (const auto &bound : m->bounds) {
    CDir *bd = mdcache->get_dirfrag(bound);
    ceph_assert(bd);
    le->metablob.add_dir(bd, false);  // note that parent metadata is already in the event
    import_bounds.insert(bd);
  }
  mdcache->verify_subtree_bounds(dir, import_bounds);

  // adjust popularity
  mds->balancer->add_import(dir);

  dout(7) << "did " << *dir << dendl;

  // note state
  it->second.state = IMPORT_LOGGINGSTART;
  ceph_assert(g_conf()->mds_kill_import_at != 6);

  // log it
  mds->mdlog->submit_entry(le, onlogged);
  mds->mdlog->flush();

  // some stats
  if (mds->logger) {
    mds->logger->inc(l_mds_imported);
    mds->logger->inc(l_mds_imported_inodes, num_imported_inodes);
  }
}


/*
 * this is an import helper
 *  called by import_finish, and import_reverse and friends.
 */
void Migrator::import_remove_pins(CDir *dir, set<CDir*>& bounds)
{
  import_state_t& stat = import_state[dir->dirfrag()];
  // root
  dir->put(CDir::PIN_IMPORTING);
  dir->state_clear(CDir::STATE_IMPORTING);

  // bounding inodes
  set<inodeno_t> did;
  for (list<dirfrag_t>::iterator p = stat.bound_ls.begin();
       p != stat.bound_ls.end();
       ++p) {
    if (did.count(p->ino))
      continue;
    did.insert(p->ino);
    CInode *in = mdcache->get_inode(p->ino);
    ceph_assert(in);
    in->put_stickydirs();
  }

  if (stat.state == IMPORT_PREPPING) {
    for (auto bd : bounds) {
      if (bd->state_test(CDir::STATE_IMPORTBOUND)) {
	bd->put(CDir::PIN_IMPORTBOUND);
	bd->state_clear(CDir::STATE_IMPORTBOUND);
      }
    }
  } else if (stat.state >= IMPORT_PREPPED) {
    // bounding dirfrags
    for (auto bd : bounds) {
      ceph_assert(bd->state_test(CDir::STATE_IMPORTBOUND));
      bd->put(CDir::PIN_IMPORTBOUND);
      bd->state_clear(CDir::STATE_IMPORTBOUND);
    }
  }
}

class C_MDC_QueueContexts : public MigratorContext {
public:
  MDSContext::vec contexts;
  C_MDC_QueueContexts(Migrator *m) : MigratorContext(m) {}
  void finish(int r) override {
    // execute contexts immediately after 'this' context
    get_mds()->queue_waiters_front(contexts);
  }
};

/*
 * note: this does teh full work of reversing and import and cleaning up
 *  state.  
 * called by both handle_mds_failure and by handle_resolve (if we are
 *  a survivor coping with an exporter failure+recovery).
 */
void Migrator::import_reverse(CDir *dir)
{
  dout(7) << *dir << dendl;

  import_state_t& stat = import_state[dir->dirfrag()];
  stat.state = IMPORT_ABORTING;

  set<CDir*> bounds;
  mdcache->get_subtree_bounds(dir, bounds);

  // remove pins
  import_remove_pins(dir, bounds);

  // update auth, with possible subtree merge.
  ceph_assert(dir->is_subtree_root());
  if (mds->is_resolve())
    mdcache->trim_non_auth_subtree(dir);

  mdcache->adjust_subtree_auth(dir, stat.peer);

  auto fin = new C_MDC_QueueContexts(this);
  if (!dir->get_inode()->is_auth() &&
      !dir->get_inode()->has_subtree_root_dirfrag(mds->get_nodeid())) {
    dir->get_inode()->clear_scatter_dirty();
    // wake up scatter_nudge waiters
    dir->get_inode()->take_waiting(CInode::WAIT_ANY_MASK, fin->contexts);
  }

  int num_dentries = 0;
  // adjust auth bits.
  std::deque<CDir*> q;
  q.push_back(dir);
  while (!q.empty()) {
    CDir *cur = q.front();
    q.pop_front();
    
    // dir
    cur->abort_import();

    for (auto &p : *cur) {
      CDentry *dn = p.second;

      // dentry
      dn->clear_auth();
      dn->clear_replica_map();
      dn->set_replica_nonce(CDentry::EXPORT_NONCE);
      if (dn->is_dirty()) 
	dn->mark_clean();

      // inode?
      if (dn->get_linkage()->is_primary()) {
	CInode *in = dn->get_linkage()->get_inode();
	in->state_clear(CInode::STATE_AUTH);
	in->clear_replica_map();
	in->set_replica_nonce(CInode::EXPORT_NONCE);
	if (in->is_dirty()) 
	  in->mark_clean();
	in->clear_dirty_rstat();
	if (!in->has_subtree_root_dirfrag(mds->get_nodeid())) {
	  in->clear_scatter_dirty();
	  in->take_waiting(CInode::WAIT_ANY_MASK, fin->contexts);
	}

	in->clear_dirty_parent();

	in->clear_clientwriteable();
	in->state_clear(CInode::STATE_NEEDSRECOVER);

	in->authlock.clear_gather();
	in->linklock.clear_gather();
	in->dirfragtreelock.clear_gather();
	in->filelock.clear_gather();

	in->clear_file_locks();

	// non-bounding dir?
	auto&& dfs = in->get_dirfrags();
	for (const auto& dir : dfs) {
	  if (bounds.count(dir) == 0)
	    q.push_back(dir);
        }
      }

      mdcache->touch_dentry_bottom(dn); // move dentry to tail of LRU
      ++num_dentries;
    }
  }

  dir->add_waiter(CDir::WAIT_UNFREEZE, fin);

  if (stat.state == IMPORT_ACKING) {
    // remove imported caps
    for (map<CInode*,map<client_t,Capability::Export> >::iterator p = stat.peer_exports.begin();
	 p != stat.peer_exports.end();
	 ++p) {
      CInode *in = p->first;
      for (map<client_t,Capability::Export>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) {
	Capability *cap = in->get_client_cap(q->first);
	if (!cap) {
	  ceph_assert(!stat.session_map.count(q->first));
	  continue;
	}
	if (cap->is_importing())
	  in->remove_client_cap(q->first);
	else
	  cap->clear_clientwriteable();
      }
      in->put(CInode::PIN_IMPORTINGCAPS);
    }
    for (auto& p : stat.session_map) {
      Session *session = p.second.first;
      session->dec_importing();
    }
  }
	 
  // log our failure
  mds->mdlog->submit_entry(new EImportFinish(dir, false));	// log failure

  mdcache->trim(num_dentries); // try trimming dentries

  // notify bystanders; wait in aborting state
  import_notify_abort(dir, bounds);
}

void Migrator::import_notify_finish(CDir *dir, set<CDir*>& bounds)
{
  dout(7) << *dir << dendl;

  import_state_t& stat = import_state[dir->dirfrag()];
  for (set<mds_rank_t>::iterator p = stat.bystanders.begin();
       p != stat.bystanders.end();
       ++p) {
    auto notify = make_message<MExportDirNotify>(dir->dirfrag(), stat.tid, false,
        pair<int,int>(stat.peer, mds->get_nodeid()),
        pair<int,int>(mds->get_nodeid(), CDIR_AUTH_UNKNOWN));
    for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirfrag());
    mds->send_message_mds(notify, *p);
  }
}

void Migrator::import_notify_abort(CDir *dir, set<CDir*>& bounds)
{
  dout(7) << *dir << dendl;
  
  import_state_t& stat = import_state[dir->dirfrag()];
  for (set<mds_rank_t>::iterator p = stat.bystanders.begin();
       p != stat.bystanders.end(); ) {
    if (mds->is_cluster_degraded() &&
	!mds->mdsmap->is_clientreplay_or_active_or_stopping(*p)) {
      // this can happen if both exporter and bystander fail in the same mdsmap epoch
      stat.bystanders.erase(p++);
      continue;
    }
    auto notify = make_message<MExportDirNotify>(dir->dirfrag(), stat.tid, true,
        mds_authority_t(stat.peer, mds->get_nodeid()),
        mds_authority_t(stat.peer, CDIR_AUTH_UNKNOWN));
    for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirfrag());
    mds->send_message_mds(notify, *p);
    ++p;
  }
  if (stat.bystanders.empty()) {
    dout(7) << "no bystanders, finishing reverse now" << dendl;
    import_reverse_unfreeze(dir);
  } else {
    ceph_assert(g_conf()->mds_kill_import_at != 10);
  }
}

void Migrator::import_reverse_unfreeze(CDir *dir)
{
  dout(7) << *dir << dendl;
  ceph_assert(!dir->is_auth());
  mdcache->discard_delayed_expire(dir);
  dir->unfreeze_tree();
  if (dir->is_subtree_root())
    mdcache->try_subtree_merge(dir);
  import_reverse_final(dir);
}

void Migrator::import_reverse_final(CDir *dir) 
{
  dout(7) << *dir << dendl;

  // clean up
  map<dirfrag_t, import_state_t>::iterator it = import_state.find(dir->dirfrag());
  ceph_assert(it != import_state.end());

  MutationRef mut = it->second.mut;
  import_state.erase(it);

  // send pending import_maps?
  mdcache->maybe_send_pending_resolves();

  if (mut) {
    mds->locker->drop_locks(mut.get());
    mut->cleanup();
  }

  mdcache->show_subtrees();
  //audit();  // this fails, bc we munge up the subtree map during handle_import_map (resolve phase)
}




void Migrator::import_logged_start(dirfrag_t df, CDir *dir, mds_rank_t from,
				   map<client_t,pair<Session*,uint64_t> >& imported_session_map)
{
  dout(7) << *dir << dendl;

  map<dirfrag_t, import_state_t>::iterator it = import_state.find(dir->dirfrag());
  if (it == import_state.end() ||
      it->second.state != IMPORT_LOGGINGSTART) {
    dout(7) << "import " << df << " must have aborted" << dendl;
    mds->server->finish_force_open_sessions(imported_session_map);
    return;
  }

  // note state
  it->second.state = IMPORT_ACKING;

  ceph_assert(g_conf()->mds_kill_import_at != 7);

  // force open client sessions and finish cap import
  mds->server->finish_force_open_sessions(imported_session_map, false);
  
  map<inodeno_t,map<client_t,Capability::Import> > imported_caps;
  for (map<CInode*, map<client_t,Capability::Export> >::iterator p = it->second.peer_exports.begin();
       p != it->second.peer_exports.end();
       ++p) {
    // parameter 'peer' is NONE, delay sending cap import messages to client
    finish_import_inode_caps(p->first, MDS_RANK_NONE, true, imported_session_map,
			     p->second, imported_caps[p->first->ino()]);
  }

  it->second.session_map.swap(imported_session_map);
  
  // send notify's etc.
  dout(7) << "sending ack for " << *dir << " to old auth mds." << from << dendl;

  // test surviving observer of a failed migration that did not complete
  //assert(dir->replica_map.size() < 2 || mds->get_nodeid() != 0);

  auto ack = make_message<MExportDirAck>(dir->dirfrag(), it->second.tid);
  encode(imported_caps, ack->imported_caps);

  mds->send_message_mds(ack, from);
  ceph_assert(g_conf()->mds_kill_import_at != 8);

  mdcache->show_subtrees();
}

void Migrator::handle_export_finish(const cref_t<MExportDirFinish> &m)
{
  CDir *dir = mdcache->get_dirfrag(m->get_dirfrag());
  ceph_assert(dir);
  dout(7) << *dir << (m->is_last() ? " last" : "") << dendl;

  map<dirfrag_t,import_state_t>::iterator it = import_state.find(m->get_dirfrag());
  ceph_assert(it != import_state.end());
  ceph_assert(it->second.tid == m->get_tid());

  import_finish(dir, false, m->is_last());
}

void Migrator::import_finish(CDir *dir, bool notify, bool last)
{
  dout(7) << *dir << dendl;

  map<dirfrag_t,import_state_t>::iterator it = import_state.find(dir->dirfrag());
  ceph_assert(it != import_state.end());
  ceph_assert(it->second.state == IMPORT_ACKING || it->second.state == IMPORT_FINISHING);

  if (it->second.state == IMPORT_ACKING) {
    ceph_assert(dir->is_auth());
    mdcache->adjust_subtree_auth(dir, mds->get_nodeid(), mds->get_nodeid());
  }

  // log finish
  ceph_assert(g_conf()->mds_kill_import_at != 9);

  if (it->second.state == IMPORT_ACKING) {
    for (map<CInode*, map<client_t,Capability::Export> >::iterator p = it->second.peer_exports.begin();
	p != it->second.peer_exports.end();
	++p) {
      CInode *in = p->first;
      ceph_assert(in->is_auth());
      for (map<client_t,Capability::Export>::iterator q = p->second.begin();
	  q != p->second.end();
	  ++q) {
	auto r = it->second.session_map.find(q->first);
	if (r == it->second.session_map.end())
	  continue;

	Session *session = r->second.first;
	Capability *cap = in->get_client_cap(q->first);
	ceph_assert(cap);
	cap->merge(q->second, true);
	cap->clear_importing();
	mdcache->do_cap_import(session, in, cap, q->second.cap_id, q->second.seq,
				    q->second.mseq - 1, it->second.peer, CEPH_CAP_FLAG_AUTH);
      }
      p->second.clear();
      in->replica_caps_wanted = 0;
    }
    for (auto& p : it->second.session_map) {
      Session *session = p.second.first;
      session->dec_importing();
    }
  }

  if (!last) {
    ceph_assert(it->second.state == IMPORT_ACKING);
    it->second.state = IMPORT_FINISHING;
    return;
  }

  // remove pins
  set<CDir*> bounds;
  mdcache->get_subtree_bounds(dir, bounds);

  if (notify)
    import_notify_finish(dir, bounds);

  import_remove_pins(dir, bounds);

  map<CInode*, map<client_t,Capability::Export> > peer_exports;
  it->second.peer_exports.swap(peer_exports);

  // clear import state (we're done!)
  MutationRef mut = it->second.mut;
  import_state.erase(it);

  mds->mdlog->submit_entry(new EImportFinish(dir, true));

  // process delayed expires
  mdcache->process_delayed_expire(dir);

  // unfreeze tree, with possible subtree merge.
  dir->unfreeze_tree();
  mdcache->try_subtree_merge(dir);

  mdcache->show_subtrees();
  //audit();  // this fails, bc we munge up the subtree map during handle_import_map (resolve phase)

  if (mut) {
    mds->locker->drop_locks(mut.get());
    mut->cleanup();
  }

  // re-eval imported caps
  for (map<CInode*, map<client_t,Capability::Export> >::iterator p = peer_exports.begin();
       p != peer_exports.end();
       ++p) {
    if (p->first->is_auth())
      mds->locker->eval(p->first, CEPH_CAP_LOCKS, true);
    p->first->put(CInode::PIN_IMPORTINGCAPS);
  }

  // send pending import_maps?
  mdcache->maybe_send_pending_resolves();

  // did i just import mydir?
  if (dir->ino() == MDS_INO_MDSDIR(mds->get_nodeid()))
    mdcache->populate_mydir();

  // is it empty?
  if (dir->get_num_head_items() == 0 &&
      !dir->inode->is_auth()) {
    // reexport!
    export_empty_import(dir);
  }
}

void Migrator::decode_import_inode(CDentry *dn, bufferlist::const_iterator& blp,
				   mds_rank_t oldauth, LogSegment *ls,
				   map<CInode*, map<client_t,Capability::Export> >& peer_exports,
				   list<ScatterLock*>& updated_scatterlocks)
{ 
  CInode *in;
  bool added = false;
  DECODE_START(1, blp); 
  dout(15) << " on " << *dn << dendl;

  inodeno_t ino;
  snapid_t last;
  decode(ino, blp);
  decode(last, blp);

  in = mdcache->get_inode(ino, last);
  if (!in) {
    in = new CInode(mds->mdcache, true, 2, last);
    added = true;
  }

  // state after link  -- or not!  -sage
  in->decode_import(blp, ls);  // cap imports are noted for later action

  // caps
  decode_import_inode_caps(in, true, blp, peer_exports);

  DECODE_FINISH(blp);

  // add inode?
  if (added) {
    mdcache->add_inode(in);
    dout(10) << "added " << *in << dendl;
  } else {
    dout(10) << "  had " << *in << dendl;
  }

  // link before state  -- or not!  -sage
  if (dn->get_linkage()->get_inode() != in) {
    ceph_assert(!dn->get_linkage()->get_inode());
    dn->dir->link_primary_inode(dn, in);
  }

  if (in->is_dir())
    dn->dir->pop_lru_subdirs.push_back(&in->item_pop_lru);
 
  if (in->get_inode()->is_dirty_rstat())
    in->mark_dirty_rstat();

  if (!in->get_inode()->client_ranges.empty())
    in->mark_clientwriteable();
  
  // clear if dirtyscattered, since we're going to journal this
  //  but not until we _actually_ finish the import...
  if (in->filelock.is_dirty()) {
    updated_scatterlocks.push_back(&in->filelock);
    mds->locker->mark_updated_scatterlock(&in->filelock);
  }

  if (in->dirfragtreelock.is_dirty()) {
    updated_scatterlocks.push_back(&in->dirfragtreelock);
    mds->locker->mark_updated_scatterlock(&in->dirfragtreelock);
  }

  // adjust replica list
  //assert(!in->is_replica(oldauth));  // not true on failed export
  in->add_replica(oldauth, CInode::EXPORT_NONCE);
  if (in->is_replica(mds->get_nodeid()))
    in->remove_replica(mds->get_nodeid());

  if (in->snaplock.is_stable() &&
      in->snaplock.get_state() != LOCK_SYNC)
      mds->locker->try_eval(&in->snaplock, NULL);

  if (in->policylock.is_stable() &&
      in->policylock.get_state() != LOCK_SYNC)
      mds->locker->try_eval(&in->policylock, NULL);

  if (in->quiescelock.is_stable() &&
      in->quiescelock.get_state() != LOCK_SYNC)
      mds->locker->try_eval(&in->quiescelock, NULL);
}

void Migrator::decode_import_inode_caps(CInode *in, bool auth_cap,
					bufferlist::const_iterator &blp,
					map<CInode*, map<client_t,Capability::Export> >& peer_exports)
{
  DECODE_START(1, blp);
  map<client_t,Capability::Export> cap_map;
  decode(cap_map, blp);
  if (auth_cap) {
    mempool::mds_co::compact_map<int32_t,int32_t> mds_wanted;
    decode(mds_wanted, blp);
    mds_wanted.erase(mds->get_nodeid());
    in->set_mds_caps_wanted(mds_wanted);
  }
  if (!cap_map.empty() ||
      (auth_cap && (in->get_caps_wanted() & ~CEPH_CAP_PIN))) {
    peer_exports[in].swap(cap_map);
    in->get(CInode::PIN_IMPORTINGCAPS);
  }
  DECODE_FINISH(blp);
}

void Migrator::finish_import_inode_caps(CInode *in, mds_rank_t peer, bool auth_cap,
					const map<client_t,pair<Session*,uint64_t> >& session_map,
					const map<client_t,Capability::Export> &export_map,
					map<client_t,Capability::Import> &import_map)
{
  const auto& client_ranges = in->get_projected_inode()->client_ranges;
  auto r = client_ranges.cbegin();
  bool needs_recover = false;

  for (auto& it : export_map) {
    dout(10) << "for client." << it.first << " on " << *in << dendl;

    auto p = session_map.find(it.first);
    if (p == session_map.end()) {
      dout(10) << " no session for client." << it.first << dendl;
      (void)import_map[it.first];
      continue;
    }

    Session *session = p->second.first;

    Capability *cap = in->get_client_cap(it.first);
    if (!cap) {
      cap = in->add_client_cap(it.first, session);
      if (peer < 0)
	cap->mark_importing();
    }

    if (auth_cap) {
      while (r != client_ranges.cend() && r->first < it.first) {
	needs_recover = true;
	++r;
      }
      if (r != client_ranges.cend() && r->first == it.first) {
	cap->mark_clientwriteable();
	++r;
      }
    }

    // Always ask exporter mds to send cap export messages for auth caps.
    // For non-auth caps, ask exporter mds to send cap export messages to
    // clients who haven't opened sessions. The cap export messages will
    // make clients open sessions.
    if (auth_cap || !session->get_connection()) {
      Capability::Import& im = import_map[it.first];
      im.cap_id = cap->get_cap_id();
      im.mseq = auth_cap ? it.second.mseq : cap->get_mseq();
      im.issue_seq = cap->get_last_seq() + 1;
    }

    if (peer >= 0) {
      cap->merge(it.second, auth_cap);
      mdcache->do_cap_import(session, in, cap, it.second.cap_id,
				  it.second.seq, it.second.mseq - 1, peer,
				  auth_cap ? CEPH_CAP_FLAG_AUTH : CEPH_CAP_FLAG_RELEASE);
    }
  }

  if (auth_cap) {
    if (r != client_ranges.cend())
      needs_recover = true;
    if (needs_recover)
      in->state_set(CInode::STATE_NEEDSRECOVER);
  }

  if (peer >= 0) {
    in->replica_caps_wanted = 0;
    in->put(CInode::PIN_IMPORTINGCAPS);
  }
}

void Migrator::decode_import_dir(bufferlist::const_iterator& blp,
				mds_rank_t oldauth,
				CDir *import_root,
				EImportStart *le,
				LogSegment *ls,
				map<CInode*,map<client_t,Capability::Export> >& peer_exports,
				list<ScatterLock*>& updated_scatterlocks, int &num_imported)
{
  DECODE_START(1, blp);
  // set up dir
  dirfrag_t df;
  decode(df, blp);

  CInode *diri = mdcache->get_inode(df.ino);
  ceph_assert(diri);
  CDir *dir = diri->get_or_open_dirfrag(mds->mdcache, df.frag);
  ceph_assert(dir);
  
  dout(7) << *dir << dendl;

  if (!dir->freeze_tree_state) {
    ceph_assert(dir->get_version() == 0);
    dir->freeze_tree_state = import_root->freeze_tree_state;
  }

  // assimilate state
  dir->decode_import(blp, ls);

  // adjust replica list
  //assert(!dir->is_replica(oldauth));    // not true on failed export
  dir->add_replica(oldauth, CDir::EXPORT_NONCE);
  if (dir->is_replica(mds->get_nodeid()))
    dir->remove_replica(mds->get_nodeid());

  // add to journal entry
  if (le) 
    le->metablob.add_import_dir(dir);

  // take all waiters on this dir
  // NOTE: a pass of imported data is guaranteed to get all of my waiters because
  // a replica's presense in my cache implies/forces it's presense in authority's.
  MDSContext::vec waiters;
  dir->take_waiting(CDir::WAIT_ANY_MASK, waiters);
  for (auto c : waiters)
    dir->add_waiter(CDir::WAIT_UNFREEZE, c);  // UNFREEZE will get kicked both on success or failure
  
  dout(15) << "doing contents" << dendl;
  
  // contents
  __u32 nden;
  decode(nden, blp);
  
  for (; nden>0; nden--) {
    num_imported++;
    
    // dentry
    string dname;
    snapid_t last;
    decode(dname, blp);
    decode(last, blp);
    
    CDentry *dn = dir->lookup_exact_snap(dname, last);
    if (!dn)
      dn = dir->add_null_dentry(dname, 1, last);
    
    dn->decode_import(blp, ls);

    dn->add_replica(oldauth, CDentry::EXPORT_NONCE);
    if (dn->is_replica(mds->get_nodeid()))
      dn->remove_replica(mds->get_nodeid());

    // dentry lock in unreadable state can block path traverse
    if (dn->lock.get_state() != LOCK_SYNC)
      mds->locker->try_eval(&dn->lock, NULL);

    dout(15) << " got " << *dn << dendl;
    
    // points to...
    char icode;
    decode(icode, blp);
    
    if (icode == 'N') {
      // null dentry
      ceph_assert(dn->get_linkage()->is_null());  
      
      // fall thru
    }
    else if (icode == 'L' || icode == 'l') {
      // remote link
      inodeno_t ino;
      unsigned char d_type;
      mempool::mds_co::string alternate_name;

      CDentry::decode_remote(icode, ino, d_type, alternate_name, blp);

      if (dn->get_linkage()->is_remote()) {
	ceph_assert(dn->get_linkage()->get_remote_ino() == ino);
        ceph_assert(dn->get_alternate_name() == alternate_name);
      } else {
	dir->link_remote_inode(dn, ino, d_type);
        dn->set_alternate_name(std::move(alternate_name));
      }
    }
    else if (icode == 'I' || icode == 'i') {
      // inode
      ceph_assert(le);
      if (icode == 'i') {
        DECODE_START(2, blp);
        decode_import_inode(dn, blp, oldauth, ls,
                            peer_exports, updated_scatterlocks);
        ceph_assert(!dn->is_projected());
        decode(dn->alternate_name, blp);
        DECODE_FINISH(blp);
      } else {
        decode_import_inode(dn, blp, oldauth, ls,
                            peer_exports, updated_scatterlocks);
      }
    }
    
    // add dentry to journal entry
    if (le)
      le->metablob.add_import_dentry(dn);
  }
  
#ifdef MDS_VERIFY_FRAGSTAT
  if (dir->is_complete())
    dir->verify_fragstat();
#endif

  dir->inode->maybe_export_pin();

  dout(7) << " done " << *dir << dendl;
  DECODE_FINISH(blp);
}





// authority bystander

void Migrator::handle_export_notify(const cref_t<MExportDirNotify> &m)
{
  if (!(mds->is_clientreplay() || mds->is_active() || mds->is_stopping())) {
    return;
  }

  CDir *dir = mdcache->get_dirfrag(m->get_dirfrag());

  mds_rank_t from = mds_rank_t(m->get_source().num());
  mds_authority_t old_auth = m->get_old_auth();
  mds_authority_t new_auth = m->get_new_auth();
  
  if (!dir) {
    dout(7) << old_auth << " -> " << new_auth
	    << " on missing dir " << m->get_dirfrag() << dendl;
  } else if (dir->authority() != old_auth) {
    dout(7) << "old_auth was " << dir->authority() 
	    << " != " << old_auth << " -> " << new_auth
	    << " on " << *dir << dendl;
  } else {
    dout(7) << old_auth << " -> " << new_auth
	    << " on " << *dir << dendl;
    // adjust auth
    set<CDir*> have;
    mdcache->map_dirfrag_set(m->get_bounds(), have);
    mdcache->adjust_bounded_subtree_auth(dir, have, new_auth);
    
    // induce a merge?
    mdcache->try_subtree_merge(dir);
  }
  
  // send ack
  if (m->wants_ack()) {
    mds->send_message_mds(make_message<MExportDirNotifyAck>(m->get_dirfrag(), m->get_tid(), m->get_new_auth()), from);
  } else {
    // aborted.  no ack.
    dout(7) << "no ack requested" << dendl;
  }
}

/** cap exports **/
void Migrator::export_caps(CInode *in)
{
  mds_rank_t dest = in->authority().first;
  dout(7) << "to mds." << dest << " " << *in << dendl;

  ceph_assert(in->is_any_caps());
  ceph_assert(!in->is_auth());
  ceph_assert(!in->is_ambiguous_auth());
  ceph_assert(!in->state_test(CInode::STATE_EXPORTINGCAPS));

  auto ex = make_message<MExportCaps>();
  ex->ino = in->ino();

  encode_export_inode_caps(in, false, ex->cap_bl, ex->client_map, ex->client_metadata_map);

  mds->send_message_mds(ex, dest);
}

void Migrator::handle_export_caps_ack(const cref_t<MExportCapsAck> &ack)
{
  mds_rank_t from = ack->get_source().num();
  CInode *in = mdcache->get_inode(ack->ino);
  if (in) {
    ceph_assert(!in->is_auth());

    dout(10) << *ack << " from "
	     << ack->get_source() << " on " << *in << dendl;

    map<client_t,Capability::Import> imported_caps;
    map<client_t,uint64_t> caps_ids;
    auto blp = ack->cap_bl.cbegin();
    decode(imported_caps, blp);
    decode(caps_ids, blp);

    for (auto& it : imported_caps) {
      Capability *cap = in->get_client_cap(it.first);
      if (!cap || cap->get_cap_id() != caps_ids.at(it.first))
	continue;

      dout(7) << " telling client." << it.first
	      << " exported caps on " << *in << dendl;
      auto m = make_message<MClientCaps>(CEPH_CAP_OP_EXPORT, in->ino(), 0,
				       cap->get_cap_id(), cap->get_mseq(),
				       mds->get_osd_epoch_barrier());
      m->set_cap_peer(it.second.cap_id, it.second.issue_seq, it.second.mseq, from, 0);
      mds->send_message_client_counted(m, it.first);

      in->remove_client_cap(it.first);
    }

    mds->locker->request_inode_file_caps(in);
    mds->locker->try_eval(in, CEPH_CAP_LOCKS);
  }
}

void Migrator::handle_gather_caps(const cref_t<MGatherCaps> &m)
{
  CInode *in = mdcache->get_inode(m->ino);
  if (!in)
    return;

  dout(10) << *m << " from " << m->get_source()
           << " on " << *in << dendl;

  if (in->is_any_caps() &&
      !in->is_auth() &&
      !in->is_ambiguous_auth() &&
      !in->state_test(CInode::STATE_EXPORTINGCAPS))
    export_caps(in);
}

class C_M_LoggedImportCaps : public MigratorLogContext {
  CInode *in;
  mds_rank_t from;
public:
  map<client_t,pair<Session*,uint64_t> > imported_session_map;
  map<CInode*, map<client_t,Capability::Export> > peer_exports;

  C_M_LoggedImportCaps(Migrator *m, CInode *i, mds_rank_t f) : MigratorLogContext(m), in(i), from(f) {}
  void finish(int r) override {
    mig->logged_import_caps(in, from, imported_session_map, peer_exports);
  }  
};

void Migrator::handle_export_caps(const cref_t<MExportCaps> &ex)
{
  dout(10) << *ex << " from " << ex->get_source() << dendl;
  CInode *in = mdcache->get_inode(ex->ino);
  
  ceph_assert(in);
  ceph_assert(in->is_auth());

  // FIXME
  if (!in->can_auth_pin()) {
    return;
  }

  in->auth_pin(this);

  map<client_t,entity_inst_t> client_map{ex->client_map};
  map<client_t,client_metadata_t> client_metadata_map{ex->client_metadata_map};

  C_M_LoggedImportCaps *finish = new C_M_LoggedImportCaps(
      this, in, mds_rank_t(ex->get_source().num()));

  version_t pv = mds->server->prepare_force_open_sessions(client_map, client_metadata_map,
							  finish->imported_session_map);
  // decode new caps
  auto blp = ex->cap_bl.cbegin();
  decode_import_inode_caps(in, false, blp, finish->peer_exports);
  ceph_assert(!finish->peer_exports.empty());   // thus, inode is pinned.

  // journal open client sessions
  ESessions *le = new ESessions(pv, std::move(client_map),
				std::move(client_metadata_map));
  mds->mdlog->submit_entry(le, finish);
  mds->mdlog->flush();
}


void Migrator::logged_import_caps(CInode *in, 
				  mds_rank_t from,
				  map<client_t,pair<Session*,uint64_t> >& imported_session_map,
				  map<CInode*, map<client_t,Capability::Export> >& peer_exports)
{
  dout(10) << *in << dendl;
  // see export_go() vs export_go_synced()
  ceph_assert(in->is_auth());

  // force open client sessions and finish cap import
  mds->server->finish_force_open_sessions(imported_session_map);

  auto it = peer_exports.find(in);
  ceph_assert(it != peer_exports.end());

  // clients will release caps from the exporter when they receive the cap import message.
  map<client_t,Capability::Import> imported_caps;
  finish_import_inode_caps(in, from, false, imported_session_map, it->second, imported_caps);
  mds->locker->eval(in, CEPH_CAP_LOCKS, true);

  if (!imported_caps.empty()) {
    auto ack = make_message<MExportCapsAck>(in->ino());
    map<client_t,uint64_t> peer_caps_ids;
    for (auto &p : imported_caps )
      peer_caps_ids[p.first] = it->second.at(p.first).cap_id;

    encode(imported_caps, ack->cap_bl);
    encode(peer_caps_ids, ack->cap_bl);
    mds->send_message_mds(ack, from);
  }

  in->auth_unpin(this);
}

Migrator::Migrator(MDSRank *m, MDCache *c) : mds(m), mdcache(c) {
  max_export_size = g_conf().get_val<Option::size_t>("mds_max_export_size");
  inject_session_race = g_conf().get_val<bool>("mds_inject_migrator_session_race");
}

void Migrator::handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map)
{
  if (changed.count("mds_max_export_size"))
    max_export_size = g_conf().get_val<Option::size_t>("mds_max_export_size");
  if (changed.count("mds_inject_migrator_session_race")) {
    inject_session_race = g_conf().get_val<bool>("mds_inject_migrator_session_race");
    dout(0) << "mds_inject_migrator_session_race is " << inject_session_race << dendl;
  }
}
