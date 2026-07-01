// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
#include <vector>

#include "ClientCaps.h"
#include "ClientCaps_impl.h"
#include "Client.h"
#include "Inode.h"
#include "Fh.h"
#include "MetaSession.h"
#include "UserPerm.h"
#include "ClientSnapRealm.h"
#include "Dentry.h"
#include "messages/MClientCaps.h"
#include "messages/MClientSession.h"
#if defined(__linux__)
#include "FSCrypt.h"
#endif
#define dout_subsys ceph_subsys_client

ClientCaps::ClientCaps(Client *client, CephContext *cct)
  : client(client), cct(cct),
    last_cap_renew(ceph::coarse_mono_clock::now()),
    caps_release_delay(cct->_conf.get_val<std::chrono::seconds>("client_caps_release_delay"))
{}

ClientCaps::~ClientCaps() = default;

void ClientCaps::inc_pinned_icaps(){ std::scoped_lock l(caps_lock); client->inc_pinned_icaps(); }
void ClientCaps::dec_pinned_icaps(uint64_t nr){ std::scoped_lock l(caps_lock); client->dec_pinned_icaps(nr); }
void ClientCaps::set_cap_epoch_barrier(epoch_t e){ std::scoped_lock l(caps_lock); cap_epoch_barrier=e; }
epoch_t ClientCaps::get_cap_epoch_barrier() const { std::scoped_lock l(caps_lock); return cap_epoch_barrier; }
int ClientCaps::get_num_flushing_caps() const { std::scoped_lock l(caps_lock); return num_flushing_caps; }
void ClientCaps::inc_num_flushing_caps(){ std::scoped_lock l(caps_lock); num_flushing_caps++; }
void ClientCaps::dec_num_flushing_caps(){ std::scoped_lock l(caps_lock); num_flushing_caps--; }
ceph_tid_t ClientCaps::get_last_flush_tid() const { std::scoped_lock l(caps_lock); return last_flush_tid; }
ceph_tid_t ClientCaps::allocate_flush_tid(){ std::scoped_lock l(caps_lock); return ++last_flush_tid; }


void ClientCaps::get_cap_ref(Inode *in, int cap)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client->client_lock));
  if ((cap & CEPH_CAP_FILE_BUFFER) &&
      in->cap_refs[CEPH_CAP_FILE_BUFFER] == 0) {
    ldout(cct, 5) << __func__ << " got first FILE_BUFFER ref on " << *in << dendl;
    in->iget();
  }
  if ((cap & CEPH_CAP_FILE_CACHE) &&
      in->cap_refs[CEPH_CAP_FILE_CACHE] == 0) {
    ldout(cct, 5) << __func__ << " got first FILE_CACHE ref on " << *in << dendl;
    in->iget();
  }
  in->get_cap_ref(cap);
}


void ClientCaps::put_cap_ref(Inode *in, int cap)
{
  int last = in->put_cap_ref(cap);
  if (last) {
    int put_nref = 0;
    int drop = last & ~in->caps_issued();
    if (in->snapid == CEPH_NOSNAP) {
      if ((last & CEPH_CAP_FILE_WR) &&
	  !in->cap_snaps.empty() &&
	  in->cap_snaps.rbegin()->second.writing) {
	ldout(cct, 10) << __func__ << " finishing pending cap_snap on " << *in << dendl;
	in->cap_snaps.rbegin()->second.writing = 0;
	finish_cap_snap(in, in->cap_snaps.rbegin()->second, get_caps_used(in));
	ldout(cct, 10) << __func__ << " calling signal_caps_inode" << dendl;
	signal_caps_inode(in);  // wake up blocked sync writers
      }
      if (last & CEPH_CAP_FILE_BUFFER) {
	for (auto &p : in->cap_snaps)
	  p.second.dirty_data = 0;
	// _flushed runs on client_finisher.  Defer fsync advancers so we do not
	// run them inline and starve C_Write_Finisher::finish_io.
	client->signal_deferred_context_list(in->waitfor_commit);
	ldout(cct, 5) << __func__ << " dropped last FILE_BUFFER ref on " << *in << dendl;
        if (!in->is_write_delegated()) {
          ++put_nref;
        }

	if (!in->cap_snaps.empty()) {
	  flush_snaps(in);
	}
      }
    }
    if (last & CEPH_CAP_FILE_CACHE) {
      ldout(cct, 5) << __func__ << " dropped last FILE_CACHE ref on " << *in << dendl;
      ++put_nref;

      ldout(cct, 10) << __func__ << " calling signal_caps_inode" << dendl;
      signal_caps_inode(in);
    }
    if (drop)
      check_caps(in, 0);
    if (put_nref)
      client->put_inode(in, put_nref);
  }
}

// get caps for a given file handle -- the inode should have @need caps
// issued by the mds and @want caps not revoked (or not under revocation).
// this routine blocks till the cap requirement is satisfied. also account
// (track) for capability hit when required (when cap requirement succeedes).

int ClientCaps::try_get_caps(Fh *fh, int need, int want, int *phave)
{
  Inode *in = fh->inode.get();

  int r = client->check_pool_perm(in, need);
  if (r < 0)
    return r;

  int file_wanted = in->caps_file_wanted();
  if ((file_wanted & need) != need)
    return -EAGAIN;

  if ((fh->mode & CEPH_FILE_MODE_WR) && fh->gen != client->fd_gen)
    return -EAGAIN;

  if ((in->flags & I_ERROR_FILELOCK) && fh->has_any_filelocks())
    return -EIO;

  int implemented;
  int have = in->caps_issued(&implemented);
  if ((have & need) != need)
    return -EAGAIN;

  int revoking = implemented & ~have;
  if ((revoking & want) != 0)
    return -EAGAIN;

  *phave = need | (have & want);
  in->get_cap_ref(need);
  client->cap_hit();
  return 0;
}

int ClientCaps::get_caps(Fh *fh, int need, int want, int *phave, loff_t endoff)
{
  Inode *in = fh->inode.get();

  int r = client->check_pool_perm(in, need);
  if (r < 0)
    return r;

  while (1) {
    int file_wanted = in->caps_file_wanted();
    if ((file_wanted & need) != need) {
      ldout(cct, 10) << "get_caps " << *in << " need " << ccap_string(need)
		     << " file_wanted " << ccap_string(file_wanted) << ", EBADF "
		     << dendl;
      return -EBADF;
    }

    if ((fh->mode & CEPH_FILE_MODE_WR) && fh->gen != client->fd_gen)
      return -EBADF;

    if ((in->flags & I_ERROR_FILELOCK) && fh->has_any_filelocks())
      return -EIO;

    int implemented;
    int have = in->caps_issued(&implemented);

    bool waitfor_caps = false;
    bool waitfor_commit = false;

    if (have & need & CEPH_CAP_FILE_WR) {
      if (endoff > 0) {
	 if ((endoff >= (loff_t)in->max_size ||
	      endoff > (loff_t)(in->size << 1)) &&
	     endoff > (loff_t)in->wanted_max_size) {
           ldout(cct, 10) << "wanted_max_size " << in->wanted_max_size << " -> " << endoff << dendl;
           uint64_t want = endoff;
#if defined(__linux__)
           if (in->fscrypt_auth.size()) {
             want = fscrypt_block_start(endoff + FSCRYPT_BLOCK_SIZE - 1);
	   }
#endif
	   in->wanted_max_size = want;
	 }
	 if (in->wanted_max_size > in->max_size &&
	     in->wanted_max_size > in->requested_max_size)
	   check_caps(in, 0);
      }

      if (endoff >= 0 && endoff > (loff_t)in->max_size) {
	ldout(cct, 10) << "waiting on max_size, endoff " << endoff << " max_size " << in->max_size << " on " << *in << dendl;
	waitfor_caps = true;
      }
      if (!in->cap_snaps.empty()) {
	if (in->cap_snaps.rbegin()->second.writing) {
	  ldout(cct, 10) << "waiting on cap_snap write to complete" << dendl;
	  waitfor_caps = true;
	}
	for (auto &p : in->cap_snaps) {
	  if (p.second.dirty_data) {
	    waitfor_commit = true;
	    break;
	  }
        }
	if (waitfor_commit) {
	  client->_flush_cap_snap_buffer(in);
	  ldout(cct, 10) << "waiting for WRBUFFER to get dropped" << dendl;
	}
      }
    }

    if (!waitfor_caps && !waitfor_commit) {
      if ((have & need) == need) {
	int revoking = implemented & ~have;
	ldout(cct, 10) << "get_caps " << *in << " have " << ccap_string(have)
		 << " need " << ccap_string(need) << " want " << ccap_string(want)
		 << " revoking " << ccap_string(revoking)
		 << dendl;
	if ((revoking & want) == 0) {
	  *phave = need | (have & want);
	  in->get_cap_ref(need);
	  client->cap_hit();
	  return 0;
	}
      }
      ldout(cct, 10) << "waiting for caps " << *in << " need " << ccap_string(need) << " want " << ccap_string(want) << dendl;
      waitfor_caps = true;
    }

    if ((need & CEPH_CAP_FILE_WR) &&
        ((in->auth_cap && in->auth_cap->session->readonly)
        // (is locked)
#if defined(__linux__)
        || (in->is_fscrypt_enabled() && client->is_inode_locked(in) && client->fscrypt_as)
#endif
       ))
      return -EROFS;

    if (in->flags & I_CAP_DROPPED) {
      int mds_wanted = in->caps_mds_wanted();
      if ((mds_wanted & need) != need) {
	int ret = client->_renew_caps(in);
	if (ret < 0)
	  return ret;
	continue;
      }
      if (!(file_wanted & ~mds_wanted))
	in->flags &= ~I_CAP_DROPPED;
    }

    if (waitfor_caps)
      client->wait_on_context_list(in->waitfor_caps);
    else if (waitfor_commit)
      client->wait_on_context_list(in->waitfor_commit);
  }
}


int ClientCaps::get_caps_used(Inode *in)
{
  unsigned used = in->caps_used();
  if (!(used & CEPH_CAP_FILE_CACHE) &&
      !client->objectcacher_set_is_empty(in))
    used |= CEPH_CAP_FILE_CACHE;
  return used;
}


void ClientCaps::cap_delay_requeue(Inode *in)
{
  std::scoped_lock lock(caps_lock);
  ldout(cct, 10) << __func__ << " on " << *in << dendl;

  in->hold_caps_until = ceph::coarse_mono_clock::now() + caps_release_delay;
  delayed_list.push_back(&in->delay_cap_item);
}

void ClientCaps::purge_delayed_list()
{
  std::vector<Inode*> inodes;
  {
    std::scoped_lock lock(caps_lock);
    while (!delayed_list.empty()) {
      inodes.push_back(delayed_list.front());
      delayed_list.pop_front();
    }
  }
  for (Inode *in : inodes) {
    in->delay_cap_item.remove_myself();
    int extra = in->get_nref() - 1;
    if (extra > 0) {
      client->put_inode(in, extra);
    } else if (in->get_nref() > 0) {
      client->put_inode(in);
    }
  }
  client->delay_put_inodes();
}


void ClientCaps::send_cap(Inode *in, MetaSession *session, Cap *cap,
		      int flags, int used, int want, int retain,
		      int flush, ceph_tid_t flush_tid)
{
  int held = cap->issued | cap->implemented;
  int revoking = cap->implemented & ~cap->issued;
  retain &= ~revoking;
  int dropping = cap->issued & ~retain;
  int op = CEPH_CAP_OP_UPDATE;

  ldout(cct, 10) << __func__ << " " << *in
	   << " mds." << session->mds_num << " seq " << cap->seq
	   << " used " << ccap_string(used)
	   << " want " << ccap_string(want)
	   << " flush " << ccap_string(flush)
	   << " retain " << ccap_string(retain)
	   << " held "<< ccap_string(held)
	   << " revoking " << ccap_string(revoking)
	   << " dropping " << ccap_string(dropping)
	   << dendl;

  if (cct->_conf->client_inject_release_failure && revoking) {
    const int would_have_issued = cap->issued & retain;
    const int would_have_implemented = cap->implemented & (cap->issued | used);
    // Simulated bug:
    //  - tell the server we think issued is whatever they issued plus whatever we implemented
    //  - leave what we have implemented in place
    ldout(cct, 20) << __func__ << " injecting failure to release caps" << dendl;
    cap->issued = cap->issued | cap->implemented;

    // Make an exception for revoking xattr caps: we are injecting
    // failure to release other caps, but allow xattr because client
    // will block on xattr ops if it can't release these to MDS (#9800)
    const int xattr_mask = CEPH_CAP_XATTR_SHARED | CEPH_CAP_XATTR_EXCL;
    cap->issued ^= xattr_mask & revoking;
    cap->implemented ^= xattr_mask & revoking;

    ldout(cct, 20) << __func__ << " issued " << ccap_string(cap->issued) << " vs " << ccap_string(would_have_issued) << dendl;
    ldout(cct, 20) << __func__ << " implemented " << ccap_string(cap->implemented) << " vs " << ccap_string(would_have_implemented) << dendl;
  } else {
    // Normal behaviour
    cap->issued &= retain;
    cap->implemented &= cap->issued | used;
  }

  snapid_t follows = 0;

  if (flush)
    follows = in->snaprealm->get_snap_context().seq;

  auto m = make_message<MClientCaps>(op,
				   in->ino,
				   0,
				   cap->cap_id, cap->seq,
				   cap->implemented,
				   want,
				   flush,
				   cap->mseq,
                                   cap->issue_seq,
                                   get_cap_epoch_barrier());
  /*
   * Since the setattr will check the cephx mds auth access before
   * buffering the changes, so it makes no sense any more to let
   * the cap update to check the access in MDS again.
   *
   * For new clients with old MDSs that doesn't support
   * CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK we will force the session
   * to be readonly if root_squash is enabled as a workaround.
   */
  m->caller_uid = -1;
  m->caller_gid = -1;

  m->set_tid(flush_tid);

  m->head.uid = in->uid;
  m->head.gid = in->gid;
  m->head.mode = in->mode;

  m->head.nlink = in->nlink;

  if (flush & CEPH_CAP_XATTR_EXCL) {
    encode(in->xattrs, m->xattrbl);
    m->head.xattr_version = in->xattr_version;
  }

  m->size = in->size;
  m->max_size = in->max_size;
  m->truncate_seq = in->truncate_seq;
  m->truncate_size = in->truncate_size;
  m->mtime = in->mtime;
  m->atime = in->atime;
  m->ctime = in->ctime;
  m->btime = in->btime;
  m->time_warp_seq = in->time_warp_seq;
  m->change_attr = in->change_attr;
  m->fscrypt_auth = in->fscrypt_auth;
  m->fscrypt_file = in->fscrypt_file;

  if (!(flags & MClientCaps::FLAG_PENDING_CAPSNAP) &&
      !in->cap_snaps.empty() &&
      in->cap_snaps.rbegin()->second.flush_tid == 0)
    flags |= MClientCaps::FLAG_PENDING_CAPSNAP;
  m->flags = flags;

  if (flush & CEPH_CAP_FILE_WR) {
    m->inline_version = in->inline_version;
    m->inline_data = in->inline_data;
  }

  in->reported_size = in->size;
  m->set_snap_follows(follows);
  cap->wanted = want;
  if (cap == in->auth_cap) {
    if (want & CEPH_CAP_ANY_FILE_WR) {
      m->set_max_size(in->wanted_max_size);
      in->requested_max_size = in->wanted_max_size;
      ldout(cct, 15) << "auth cap, requesting max_size " << in->requested_max_size << dendl;
    } else {
      in->requested_max_size = 0;
      ldout(cct, 15) << "auth cap, reset requested_max_size due to not wanting any file write cap" << dendl;
    }
  }

  if (!session->flushing_caps_tids.empty())
    m->set_oldest_flush_tid(*session->flushing_caps_tids.begin());

  session->con->send_message2(std::move(m));
}


bool ClientCaps::is_max_size_approaching(Inode *in)
{
  /* mds will adjust max size according to the reported size */
  if (in->flushing_caps & CEPH_CAP_FILE_WR)
    return false;
  if (in->size >= in->max_size)
    return true;
  /* half of previous max_size increment has been used */
  if (in->max_size > in->reported_size &&
      (in->size << 1) >= in->max_size + in->reported_size)
    return true;
  return false;
}

int ClientCaps::adjust_caps_used_for_lazyio(int used, int issued, int implemented)
{
  if (!(used & (CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER)))
    return used;
  if (!(implemented & CEPH_CAP_FILE_LAZYIO))
    return used;

  if (issued & CEPH_CAP_FILE_LAZYIO) {
    if (!(issued & CEPH_CAP_FILE_CACHE)) {
      used &= ~CEPH_CAP_FILE_CACHE;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
    if (!(issued & CEPH_CAP_FILE_BUFFER)) {
      used &= ~CEPH_CAP_FILE_BUFFER;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
  } else {
    if (!(implemented & CEPH_CAP_FILE_CACHE)) {
      used &= ~CEPH_CAP_FILE_CACHE;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
    if (!(implemented & CEPH_CAP_FILE_BUFFER)) {
      used &= ~CEPH_CAP_FILE_BUFFER;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
  }
  return used;
}

/**
 * check_caps
 *
 * Examine currently used and wanted versus held caps. Release, flush or ack
 * revoked caps to the MDS as appropriate.
 *
 * @param in the inode to check
 * @param flags flags to apply to cap check
 */
void ClientCaps::check_caps(const InodeRef& in, unsigned flags)
{
  unsigned wanted = in->caps_wanted();
  unsigned used = get_caps_used(in.get());
  unsigned cap_used;

  int implemented;
  int issued = in->caps_issued(&implemented);
  int revoking = implemented & ~issued;

  int orig_used = used;
  used = adjust_caps_used_for_lazyio(used, issued, implemented);

  int retain = wanted | used | CEPH_CAP_PIN;
  if (!client->is_unmounting() && in->nlink > 0) {
    if (wanted) {
      retain |= CEPH_CAP_ANY;
    } else if (in->is_dir() &&
	       (issued & CEPH_CAP_FILE_SHARED) &&
	       (in->flags & I_COMPLETE)) {
      // we do this here because we don't want to drop to Fs (and then
      // drop the Fs if we do a create!) if that alone makes us send lookups
      // to the MDS. Doing it in in->caps_wanted() has knock-on effects elsewhere
      wanted = CEPH_CAP_ANY_SHARED | CEPH_CAP_FILE_EXCL;
      retain |= wanted;
    } else {
      retain |= CEPH_CAP_ANY_SHARED;
      // keep RD only if we didn't have the file open RW,
      // because then the mds would revoke it anyway to
      // journal max_size=0.
      if (in->max_size == 0)
	retain |= CEPH_CAP_ANY_RD;
    }
  }

  ldout(cct, 10) << __func__ << " on " << *in
	   << " wanted " << ccap_string(wanted)
	   << " used " << ccap_string(used)
	   << " issued " << ccap_string(issued)
	   << " revoking " << ccap_string(revoking)
	   << " flags=" << flags
	   << dendl;

  if (in->snapid != CEPH_NOSNAP)
    return; //snap caps last forever, can't write

  if (in->caps.empty())
    return;   // guard if at end of func

  if (!(orig_used & CEPH_CAP_FILE_BUFFER) &&
      (revoking & used & (CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO))) {
    if (client->_release(in.get()))
      used &= ~(CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO);
  }

  for (auto &[mds, cap] : in->caps) {
    auto session = client->mds_sessions.at(mds);

    cap_used = used;
    if (in->auth_cap && &cap != in->auth_cap)
      cap_used &= ~in->auth_cap->issued;

    revoking = cap.implemented & ~cap.issued;

    ldout(cct, 10) << " cap mds." << mds
	     << " issued " << ccap_string(cap.issued)
	     << " implemented " << ccap_string(cap.implemented)
	     << " revoking " << ccap_string(revoking) << dendl;

    if (in->wanted_max_size > in->max_size &&
	in->wanted_max_size > in->requested_max_size &&
	&cap == in->auth_cap)
      goto ack;

    /* approaching file_max? */
    if ((cap.issued & CEPH_CAP_FILE_WR) &&
	&cap == in->auth_cap &&
	is_max_size_approaching(in.get())) {
      ldout(cct, 10) << "size " << in->size << " approaching max_size " << in->max_size
		     << ", reported " << in->reported_size << dendl;
      goto ack;
    }

    /* completed revocation? */
    if (revoking && (revoking & cap_used) == 0) {
      ldout(cct, 10) << "completed revocation of " << ccap_string(cap.implemented & ~cap.issued) << dendl;
      goto ack;
    }

    /* want more caps from mds? */
    if (wanted & ~(cap.wanted | cap.issued))
      goto ack;

    if (!revoking && client->is_unmounting() && (cap_used == 0))
      goto ack;

    if ((cap.issued & ~retain) == 0 && // and we don't have anything we wouldn't like
	!in->dirty_caps)               // and we have no dirty caps
      continue;

    if (!(flags & CHECK_CAPS_NODELAY)) {
      ldout(cct, 10) << "delaying cap release" << dendl;
      cap_delay_requeue(in.get());
      continue;
    }

  ack:
    if (&cap == in->auth_cap) {
      if (in->flags & I_KICK_FLUSH) {
	ldout(cct, 20) << " reflushing caps (check_caps) on " << *in
		       << " to mds." << mds << dendl;
	kick_flushing_caps(in.get(), session.get());
      }
      if (!in->cap_snaps.empty() &&
	  in->cap_snaps.rbegin()->second.flush_tid == 0)
	flush_snaps(in.get());
    }

    int flushing;
    int msg_flags = 0;
    ceph_tid_t flush_tid;
    if (in->auth_cap == &cap && in->dirty_caps) {
      flushing = mark_caps_flushing(in.get(), &flush_tid);
      if (flags & CHECK_CAPS_SYNCHRONOUS)
	msg_flags |= MClientCaps::FLAG_SYNC;
    } else {
      flushing = 0;
      flush_tid = 0;
    }

    in->delay_cap_item.remove_myself();
    send_cap(in.get(), session.get(), &cap, msg_flags, cap_used, wanted, retain,
	     flushing, flush_tid);
  }
}



void ClientCaps::queue_cap_snap(Inode *in, const SnapContext& old_snapc)
{
  int used = get_caps_used(in);
  int dirty = in->caps_dirty();
  ldout(cct, 10) << __func__ << " " << *in << " snapc " << old_snapc << " used " << ccap_string(used) << dendl;

  if (in->cap_snaps.size() &&
      in->cap_snaps.rbegin()->second.writing) {
    ldout(cct, 10) << __func__ << " already have pending cap_snap on " << *in << dendl;
    return;
  } else if (dirty || (used & CEPH_CAP_FILE_WR)) {
    const auto &capsnapem = in->cap_snaps.emplace(std::piecewise_construct, std::make_tuple(old_snapc.seq), std::make_tuple(in));
    ceph_assert(capsnapem.second); /* element inserted */
    CapSnap &capsnap = capsnapem.first->second;
    capsnap.context = old_snapc;
    capsnap.issued = in->caps_issued();
    capsnap.dirty = dirty;

    capsnap.dirty_data = (used & CEPH_CAP_FILE_BUFFER);

    capsnap.uid = in->uid;
    capsnap.gid = in->gid;
    capsnap.mode = in->mode;
    capsnap.btime = in->btime;
    capsnap.xattrs = in->xattrs;
    capsnap.xattr_version = in->xattr_version;

    if (used & CEPH_CAP_FILE_WR) {
      ldout(cct, 10) << __func__ << " WR used on " << *in << dendl;
      capsnap.writing = 1;
    } else {
      finish_cap_snap(in, capsnap, used);
    }
  } else {
    ldout(cct, 10) << __func__ << " not dirty|writing on " << *in << dendl;
  }
}


void ClientCaps::finish_cap_snap(Inode *in, CapSnap &capsnap, int used)
{
  ldout(cct, 10) << __func__ << " " << *in << " capsnap " << (void *)&capsnap << " used " << ccap_string(used) << dendl;
  capsnap.size = in->size;
  capsnap.fscrypt_auth = in->fscrypt_auth;
  capsnap.fscrypt_file = in->fscrypt_file;
  capsnap.mtime = in->mtime;
  capsnap.atime = in->atime;
  capsnap.ctime = in->ctime;
  capsnap.time_warp_seq = in->time_warp_seq;
  capsnap.change_attr = in->change_attr;
  capsnap.dirty |= in->caps_dirty();

  if (capsnap.dirty & CEPH_CAP_FILE_WR) {
    capsnap.inline_data = in->inline_data;
    capsnap.inline_version = in->inline_version;
  }

  if (used & CEPH_CAP_FILE_BUFFER) {
    ldout(cct, 10) << __func__ << " " << *in << " cap_snap " << &capsnap << " used " << used
	     << " WRBUFFER, trigger to flush dirty buffer" << dendl;

    /* trigger to flush the buffer */
    client->_flush_cap_snap_buffer(in);
  } else {
    capsnap.dirty_data = 0;
    flush_snaps(in);
  }
}


void ClientCaps::send_flush_snap(Inode *in, MetaSession *session,
			     snapid_t follows, CapSnap& capsnap)
{
  auto m = make_message<MClientCaps>(CEPH_CAP_OP_FLUSHSNAP,
				     in->ino, in->snaprealm->ino, 0,
				     in->auth_cap->mseq, get_cap_epoch_barrier());
  /*
   * Since the setattr will check the cephx mds auth access before
   * buffering the changes, so it makes no sense any more to let
   * the cap update to check the access in MDS again.
   */
  m->caller_uid = -1;
  m->caller_gid = -1;

  m->set_client_tid(capsnap.flush_tid);
  m->head.snap_follows = follows;

  m->head.caps = capsnap.issued;
  m->head.dirty = capsnap.dirty;

  m->head.uid = capsnap.uid;
  m->head.gid = capsnap.gid;
  m->head.mode = capsnap.mode;
  m->btime = capsnap.btime;

  m->size = capsnap.size;

  m->head.xattr_version = capsnap.xattr_version;
  encode(capsnap.xattrs, m->xattrbl);

  m->fscrypt_file = capsnap.fscrypt_auth;
  m->fscrypt_file = capsnap.fscrypt_file;
  m->ctime = capsnap.ctime;
  m->btime = capsnap.btime;
  m->mtime = capsnap.mtime;
  m->atime = capsnap.atime;
  m->time_warp_seq = capsnap.time_warp_seq;
  m->change_attr = capsnap.change_attr;

  if (capsnap.dirty & CEPH_CAP_FILE_WR) {
    m->inline_version = in->inline_version;
    m->inline_data = in->inline_data;
  }

  ceph_assert(!session->flushing_caps_tids.empty());
  m->set_oldest_flush_tid(*session->flushing_caps_tids.begin());

  session->con->send_message2(std::move(m));
}


void ClientCaps::signal_caps_inode_sync(Inode *in)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client->client_lock));

  // Nonblocking fsync advancers may re-queue themselves on
  // waitfor_caps_pending while we are finishing waitfor_caps.  A single
  // signal/swap can leave those waiters in waitfor_caps without running
  // them until the next cap flush ack (which may never come).
  do {
    client->signal_deferred_context_list(in->waitfor_caps);
    if (in->waitfor_caps_pending.empty())
      break;
    std::swap(in->waitfor_caps, in->waitfor_caps_pending);
  } while (true);
}

void ClientCaps::signal_caps_inode(Inode *in)
{
  // handle_cap_flush_ack runs on client_finisher with client_lock held.
  // Waking cap waiters via finish_contexts() runs each fsync advancer inline
  // and starves C_Write_Finisher::finish_io on the same finisher queue.
  signal_caps_inode_sync(in);
}

void ClientCaps::flush_snaps(Inode *in)
{
  ldout(cct, 10) << "flush_snaps on " << *in << dendl;
  ceph_assert(in->cap_snaps.size());

  // pick auth mds
  ceph_assert(in->auth_cap);
  MetaSession *session = in->auth_cap->session;

  for (auto &p : in->cap_snaps) {
    CapSnap &capsnap = p.second;
    // only do new flush
    if (capsnap.flush_tid > 0)
      continue;

    ldout(cct, 10) << "flush_snaps mds." << session->mds_num
	     << " follows " << p.first
	     << " size " << capsnap.size
	     << " mtime " << capsnap.mtime
	     << " dirty_data=" << capsnap.dirty_data
	     << " writing=" << capsnap.writing
	     << " on " << *in << dendl;
    if (capsnap.dirty_data || capsnap.writing)
      break;

    capsnap.flush_tid = allocate_flush_tid();
    session->flushing_caps_tids.insert(capsnap.flush_tid);
    in->flushing_cap_tids[capsnap.flush_tid] = 0;
    if (!in->flushing_cap_item.is_on_list())
      session->flushing_caps.push_back(&in->flushing_cap_item);

    send_flush_snap(in, session, p.first, capsnap);
  }
}


void ClientCaps::check_cap_issue(Inode *in, unsigned issued)
{
  unsigned had = in->caps_issued();

  if ((issued & CEPH_CAP_FILE_CACHE) &&
      !(had & CEPH_CAP_FILE_CACHE))
    in->cache_gen++;

  if ((issued & CEPH_CAP_FILE_SHARED) !=
      (had & CEPH_CAP_FILE_SHARED)) {
    if (issued & CEPH_CAP_FILE_SHARED)
      in->shared_gen++;
    if (in->is_dir())
      client->clear_dir_complete_and_ordered(in, true);
  }
}


void ClientCaps::add_update_cap(Inode *in, MetaSession *mds_session, uint64_t cap_id,
			    unsigned issued, unsigned wanted, unsigned seq, unsigned mseq,
			    inodeno_t realm, int flags, const UserPerm& cap_perms)
{
  if (!in->is_any_caps()) {
    ceph_assert(in->snaprealm == 0);
    in->snaprealm = client->get_snap_realm(realm);
    in->snaprealm->inodes_with_caps.push_back(&in->snaprealm_item);
    ldout(cct, 15) << __func__ << " first one, opened snaprealm " << in->snaprealm << dendl;
  } else {
    ceph_assert(in->snaprealm);
    if ((flags & CEPH_CAP_FLAG_AUTH) &&
	realm != inodeno_t(-1) && in->snaprealm->ino != realm) {
      in->snaprealm_item.remove_myself();
      auto oldrealm = in->snaprealm;
      in->snaprealm = client->get_snap_realm(realm);
      in->snaprealm->inodes_with_caps.push_back(&in->snaprealm_item);
      client->put_snap_realm(oldrealm);
    }
  }

  mds_rank_t mds = mds_session->mds_num;
  const auto &capem = in->caps.emplace(std::piecewise_construct, std::forward_as_tuple(mds), std::forward_as_tuple(*in, mds_session));
  Cap &cap = capem.first->second;
  if (!capem.second) {
    if (cap.gen < mds_session->cap_gen)
      cap.issued = cap.implemented = CEPH_CAP_PIN;

    /*
     * auth mds of the inode changed. we received the cap export
     * message, but still haven't received the cap import message.
     * handle_cap_export() updated the new auth MDS' cap.
     *
     * "ceph_seq_cmp(seq, cap->seq) <= 0" means we are processing
     * a message that was send before the cap import message. So
     * don't remove caps.
     */
    if (ceph_seq_cmp(seq, cap.seq) <= 0) {
      if (&cap != in->auth_cap)
         ldout(cct, 0) << "WARNING: " <<  "inode " << *in << " caps on mds." << mds << " != auth_cap." << dendl;

      ceph_assert(cap.cap_id == cap_id);
      seq = cap.seq;
      mseq = cap.mseq;
      issued |= cap.issued;
      flags |= CEPH_CAP_FLAG_AUTH;
    }
  } else {
    inc_pinned_icaps();
  }

  check_cap_issue(in, issued);

  if (flags & CEPH_CAP_FLAG_AUTH) {
    if (in->auth_cap != &cap &&
        (!in->auth_cap || ceph_seq_cmp(in->auth_cap->mseq, mseq) < 0)) {
      if (in->auth_cap) {
        if (in->flushing_cap_item.is_on_list()) {
          ldout(cct, 10) << __func__ << " changing auth cap: "
                         << "add myself to new auth MDS' flushing caps list" << dendl;
          adjust_session_flushing_caps(in, in->auth_cap->session, mds_session);
        }
        if (in->dirty_cap_item.is_on_list()) {
          ldout(cct, 10) << __func__ << " changing auth cap: "
                         << "add myself to new auth MDS' dirty caps list" << dendl;
          mds_session->get_dirty_list().push_back(&in->dirty_cap_item);
        }
      }

      in->auth_cap = &cap;
    }
  }

  unsigned old_caps = cap.issued;
  cap.cap_id = cap_id;
  cap.issued = issued;
  cap.implemented |= issued;
  if (ceph_seq_cmp(mseq, cap.mseq) > 0)
    cap.wanted = wanted;
  else
    cap.wanted |= wanted;
  cap.seq = seq;
  cap.issue_seq = seq;
  cap.mseq = mseq;
  cap.gen = mds_session->cap_gen;
  cap.latest_perms = cap_perms;
  ldout(cct, 10) << __func__ << " issued " << ccap_string(old_caps) << " -> " << ccap_string(cap.issued)
	   << " from mds." << mds
	   << " on " << *in
	   << dendl;

  if ((issued & ~old_caps) && in->auth_cap == &cap) {
    // non-auth MDS is revoking the newly grant caps ?
    for (auto &p : in->caps) {
      if (&p.second == &cap)
	continue;
      if (p.second.implemented & ~p.second.issued & issued) {
	check_caps(in, CHECK_CAPS_NODELAY);
	break;
      }
    }
  }

  if (issued & ~old_caps) {
    ldout(cct, 10) << __func__ << " calling signal_caps_inode" << dendl;
    signal_caps_inode(in);
  }
}


void ClientCaps::remove_cap(Cap *cap, bool queue_release)
{
  auto &in = cap->inode;
  MetaSession *session = cap->session;
  mds_rank_t mds = cap->session->mds_num;

  ldout(cct, 10) << __func__ << " mds." << mds << " on " << in << dendl;
  
  if (queue_release) {
    session->enqueue_cap_release(
      in.ino,
      cap->cap_id,
      cap->issue_seq,
      cap->mseq,
      get_cap_epoch_barrier());
  } else {
    dec_pinned_icaps();
  }


  if (in.auth_cap == cap) {
    if (in.flushing_cap_item.is_on_list()) {
      ldout(cct, 10) << " removing myself from flushing_cap list" << dendl;
      in.flushing_cap_item.remove_myself();
    }
    in.auth_cap = NULL;
  }
  size_t n = in.caps.erase(mds);
  ceph_assert(n == 1);
  cap = nullptr;

  if (!in.is_any_caps()) {
    ldout(cct, 15) << __func__ << " last one, closing snaprealm " << in.snaprealm << dendl;
    in.snaprealm_item.remove_myself();
    client->put_snap_realm(in.snaprealm);
    in.snaprealm = 0;
  }
}


void ClientCaps::remove_all_caps(Inode *in, bool queue_release)
{
  while (!in->caps.empty())
    remove_cap(&in->caps.begin()->second, queue_release);
}


void ClientCaps::remove_session_caps(MetaSession *s, int err)
{
  ldout(cct, 10) << __func__ << " mds." << s->mds_num << dendl;

  while (s->caps.size()) {
    Cap *cap = *s->caps.begin();
    InodeRef in(&cap->inode);
    bool dirty_caps = false;
    if (in->auth_cap == cap) {
      dirty_caps = in->dirty_caps | in->flushing_caps;
      in->wanted_max_size = 0;
      in->requested_max_size = 0;
      if (in->has_any_filelocks())
	in->flags |= I_ERROR_FILELOCK;
    }
    auto caps = cap->implemented;
    if (cap->wanted | cap->issued)
      in->flags |= I_CAP_DROPPED;
    remove_cap(cap, false);
    in->cap_snaps.clear();
    if (dirty_caps) {
      lderr(cct) << __func__ << " still has dirty|flushing caps on " << *in << dendl;
      if (in->flushing_caps) {
	dec_num_flushing_caps();
	in->flushing_cap_tids.clear();
      }
      in->flushing_caps = 0;
      in->mark_caps_clean();
      client->put_inode(in.get());
    }
    caps &= CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER;
    if (caps && !in->caps_issued_mask(caps, true)) {
      if (err == -EBLOCKLISTED) {
	if (in->oset.dirty_or_tx) {
	  lderr(cct) << __func__ << " still has dirty data on " << *in << dendl;
	  in->set_async_err(err);
	}
	client->objectcacher_purge_set(in.get());
      } else {
	client->objectcacher_release_set(in.get());
      }
      client->_schedule_invalidate_callback(in.get(), 0, 0);
    }

    ldout(cct, 10) << __func__ << " calling signal_caps_inode" << dendl;
    signal_caps_inode(in.get());
  }
  s->flushing_caps_tids.clear();
  client->sync_cond.notify_all();
}

void ClientCaps::trim_caps(MetaSession *s, uint64_t max)
{
  mds_rank_t mds = s->mds_num;
  size_t caps_size = s->caps.size();
  ldout(cct, 10) << __func__ << " mds." << mds << " max " << max 
    << " caps " << caps_size << dendl;

  uint64_t trimmed = 0;
  auto p = s->caps.begin();
  std::set<Dentry *> to_trim; /* this avoids caps other than the one we're
                               * looking at from getting deleted during traversal. */
  while ((caps_size - trimmed) > max && !p.end()) {
    Cap *cap = *p;
    InodeRef in(&cap->inode);

    // Increment p early because it will be invalidated if cap
    // is deleted inside remove_cap
    ++p;

    if (in->dirty_caps || in->cap_snaps.size())
      cap_delay_requeue(in.get());

    if (in->caps.size() > 1 && cap != in->auth_cap) {
      int mine = cap->issued | cap->implemented;
      int oissued = in->auth_cap ? in->auth_cap->issued : 0;
      // disposable non-auth cap
      if (!(get_caps_used(in.get()) & ~oissued & mine)) {
	ldout(cct, 20) << " removing unused, unneeded non-auth cap on " << *in << dendl;
	cap = (remove_cap(cap, true), nullptr);
	trimmed++;
      }
    } else {
      ldout(cct, 20) << " trying to trim dentries for " << *in << dendl;
      client->_trim_negative_child_dentries(in);
      bool all = true;
      auto q = in->dentries.begin();
      while (q != in->dentries.end()) {
        Dentry *dn = *q;
        ++q;
	if (dn->lru_is_expireable()) {
	  if (client->can_invalidate_dentries &&
	      dn->dir->parent_inode->ino == CEPH_INO_ROOT) {
	    // Only issue one of these per DN for inodes in root: handle
	    // others more efficiently by calling for root-child DNs at
	    // the end of this function.
	    client->_schedule_invalidate_dentry_callback(dn, true);
	  }
          ldout(cct, 20) << " queueing dentry for trimming: " << dn->name << dendl;
          to_trim.insert(dn);
        } else {
          ldout(cct, 20) << "  not expirable: " << dn->name << dendl;
	  all = false;
        }
      }
      if (in->ll_ref == 1 && in->ino != CEPH_INO_ROOT) {
         client->_schedule_ino_release_callback(in.get());
      }
      if (all && in->ino != CEPH_INO_ROOT) {
        ldout(cct, 20) << __func__ << " counting as trimmed: " << *in << dendl;
	if (!in->dirty_caps && !in->cap_snaps.size())
	  trimmed++;
      }
    }
  }
  ldout(cct, 20) << " trimming queued dentries: " << dendl;
  for (const auto &dn : to_trim) {
    client->trim_dentry(dn);
  }
  to_trim.clear();

  caps_size = s->caps.size();
  if (caps_size > (size_t)max)
    client->_invalidate_kernel_dcache();
}


int ClientCaps::mark_caps_flushing(Inode *in, ceph_tid_t* ptid)
{
  MetaSession *session = in->auth_cap->session;

  int flushing = in->dirty_caps;
  ceph_assert(flushing);

  ceph_tid_t flush_tid = allocate_flush_tid();
  in->flushing_cap_tids[flush_tid] = flushing;

  if (!in->flushing_caps) {
    ldout(cct, 10) << __func__ << " " << ccap_string(flushing) << " " << *in << dendl;
    inc_num_flushing_caps();
  } else {
    ldout(cct, 10) << __func__ << " (more) " << ccap_string(flushing) << " " << *in << dendl;
  }

  in->flushing_caps |= flushing;
  in->mark_caps_clean();
 
  if (!in->flushing_cap_item.is_on_list())
    session->flushing_caps.push_back(&in->flushing_cap_item);
  session->flushing_caps_tids.insert(flush_tid);

  *ptid = flush_tid;
  return flushing;
}


void ClientCaps::adjust_session_flushing_caps(Inode *in, MetaSession *old_s,  MetaSession *new_s)
{
  for (auto &p : in->cap_snaps) {
    CapSnap &capsnap = p.second;
    if (capsnap.flush_tid > 0) {
      old_s->flushing_caps_tids.erase(capsnap.flush_tid);
      new_s->flushing_caps_tids.insert(capsnap.flush_tid);
    }
  }
  for (map<ceph_tid_t, int>::iterator it = in->flushing_cap_tids.begin();
       it != in->flushing_cap_tids.end();
       ++it) {
    old_s->flushing_caps_tids.erase(it->first);
    new_s->flushing_caps_tids.insert(it->first);
  }
  new_s->flushing_caps.push_back(&in->flushing_cap_item);
}

/*
 * Flush all the dirty caps back to the MDS. Because the callers
 * generally wait on the result of this function (syncfs and umount
 * cases), we set CHECK_CAPS_SYNCHRONOUS on the last check_caps call.
 */

void ClientCaps::flush_caps_sync()
{
  ldout(cct, 10) << __func__ << dendl;
  for (auto &q : client->mds_sessions) {
    auto s = q.second;
    // Snapshot dirty inodes before flushing: nested check_caps() or cap
    // waiters may mark_caps_clean() other inodes still referenced by the
    // xlist iterator and corrupt ++p (cur->_list assert).
    std::vector<Inode*> dirty;
    dirty.reserve(s->dirty_list.size());
    for (auto p = s->dirty_list.begin(); !p.end(); ++p)
      dirty.push_back(*p);

    for (unsigned i = 0; i < dirty.size(); ++i) {
      unsigned flags = CHECK_CAPS_NODELAY;
      if (i + 1 == dirty.size())
        flags |= CHECK_CAPS_SYNCHRONOUS;
      check_caps(dirty[i], flags);
    }
  }
}


void ClientCaps::wait_sync_caps(Inode *in, ceph_tid_t want)
{
  while (in->flushing_caps) {
    map<ceph_tid_t, int>::iterator it = in->flushing_cap_tids.begin();
    ceph_assert(it != in->flushing_cap_tids.end());
    if (it->first > want)
      break;
    ldout(cct, 10) << __func__ << " on " << *in << " flushing "
		   << ccap_string(it->second) << " want " << want
		   << " last " << it->first << dendl;
    client->wait_on_context_list(in->waitfor_caps);
  }
}


void ClientCaps::wait_sync_caps(ceph_tid_t want)
{
 retry:
  ldout(cct, 10) << __func__ << " want " << want  << " (last is " << get_last_flush_tid() << ", "
	   << get_num_flushing_caps() << " total flushing)" << dendl;
  for (auto &p : client->mds_sessions) {
    auto s = p.second;
    if (s->flushing_caps_tids.empty())
	continue;
    ceph_tid_t oldest_tid = *s->flushing_caps_tids.begin();
    if (oldest_tid <= want) {
      ldout(cct, 10) << " waiting on mds." << p.first << " tid " << oldest_tid
		     << " (want " << want << ")" << dendl;
      std::unique_lock l{client->client_lock, std::adopt_lock};
      client->sync_cond.wait(l);
      l.release();
      goto retry;
    }
  }
}


void ClientCaps::kick_flushing_caps(Inode *in, MetaSession *session)
{
  in->flags &= ~I_KICK_FLUSH;

  Cap *cap = in->auth_cap;
  ceph_assert(cap->session == session);

  ceph_tid_t last_snap_flush = 0;
  for (auto p = in->flushing_cap_tids.rbegin();
       p != in->flushing_cap_tids.rend();
       ++p) {
    if (!p->second) {
      last_snap_flush = p->first;
      break;
    }
  }

  int wanted = in->caps_wanted();
  int used = get_caps_used(in) | in->caps_dirty();
  auto it = in->cap_snaps.begin();
  for (auto& p : in->flushing_cap_tids) {
    if (p.second) {
      int msg_flags = p.first < last_snap_flush ? MClientCaps::FLAG_PENDING_CAPSNAP : 0;
      send_cap(in, session, cap, msg_flags, used, wanted, (cap->issued | cap->implemented),
	       p.second, p.first);
    } else {
      ceph_assert(it != in->cap_snaps.end());
      ceph_assert(it->second.flush_tid == p.first);
      send_flush_snap(in, session, it->first, it->second);
      ++it;
    }
  }
}


void ClientCaps::kick_flushing_caps(MetaSession *session)
{
  mds_rank_t mds = session->mds_num;
  ldout(cct, 10) << __func__ << " mds." << mds << dendl;

  for (xlist<Inode*>::iterator p = session->flushing_caps.begin(); !p.end(); ++p) {
    Inode *in = *p;
    if (in->flags & I_KICK_FLUSH) {
      ldout(cct, 20) << " reflushing caps on " << *in << " to mds." << mds << dendl;
      kick_flushing_caps(in, session);
    }
  }
}


void ClientCaps::early_kick_flushing_caps(MetaSession *session)
{
  for (xlist<Inode*>::iterator p = session->flushing_caps.begin(); !p.end(); ++p) {
    Inode *in = *p;
    Cap *cap = in->auth_cap;
    ceph_assert(cap);

    // if flushing caps were revoked, we re-send the cap flush in client reconnect
    // stage. This guarantees that MDS processes the cap flush message before issuing
    // the flushing caps to other client.
    if ((in->flushing_caps & in->auth_cap->issued) == in->flushing_caps) {
      in->flags |= I_KICK_FLUSH;
      continue;
    }

    ldout(cct, 20) << " reflushing caps (early_kick) on " << *in
		   << " to mds." << session->mds_num << dendl;
    // send_reconnect() also will reset these sequence numbers. make sure
    // sequence numbers in cap flush message match later reconnect message.
    cap->seq = 0;
    cap->issue_seq = 0;
    cap->mseq = 0;
    cap->issued = cap->implemented;

    kick_flushing_caps(in, session);
  }
}


void ClientCaps::flush_cap_releases()
{
  uint64_t nr_caps = 0;

  // send any cap releases
  for (auto &p : client->mds_sessions) {
    auto session = p.second;
    if (session->release && client->mdsmap->is_clientreplay_or_active_or_stopping(
            p.first)) {
      nr_caps += session->release->caps.size();
      for (const auto &cap: session->release->caps) {
        ldout(cct, 10) << __func__ << " removing " << static_cast<inodeno_t>(cap.ino) <<
        " from subvolume tracker" << dendl;
        client->subvolume_tracker->remove_inode(static_cast<inodeno_t>(cap.ino));
      }
      if (cct->_conf->client_inject_release_failure) {
        ldout(cct, 20) << __func__ << " injecting failure to send cap release message" << dendl;
      } else {
        session->con->send_message2(std::move(session->release));
      }
      session->release.reset();
    }
  }

  if (nr_caps > 0) {
    dec_pinned_icaps(nr_caps);
  }
}


void ClientCaps::renew_and_flush_cap_releases()
{
  ceph_assert(client->client_lock.is_locked_by_me());

  if (!client->mount_aborted && client->mdsmap->get_epoch()) {
    // renew caps?
    auto el = ceph::coarse_mono_clock::now() - last_cap_renew;
    if (unlikely(utime_t(el) > client->mdsmap->get_session_timeout() / 3.0))
      renew_caps();

    flush_cap_releases();
  }
}


void ClientCaps::renew_caps()
{
  ldout(cct, 10) << "renew_caps()" << dendl;
  last_cap_renew = ceph::coarse_mono_clock::now();

  for (auto &p : client->mds_sessions) {
    ldout(cct, 15) << "renew_caps requesting from mds." << p.first << dendl;
    if (client->mdsmap->get_state(p.first) >= MDSMap::STATE_REJOIN)
      renew_caps(p.second.get());
  }
}


void ClientCaps::renew_caps(MetaSession *session)
{
  ldout(cct, 10) << "renew_caps mds." << session->mds_num << dendl;
  session->last_cap_renew_request = ceph_clock_now();
  uint64_t seq = ++session->cap_renew_seq;
  auto m = make_message<MClientSession>(CEPH_SESSION_REQUEST_RENEWCAPS, seq);
  m->oldest_client_tid = client->oldest_tid;
  session->con->send_message2(std::move(m));
}


// ===============================================================
// high level (POSIXy) interface

