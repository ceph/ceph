#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "Server.h"
#include "Locker.h"
#include "Mutation.h"

#include "MDLog.h"
#include "SessionMap.h"
#include "events/ESubtreeMap.h"

#include "include/filepath.h"
#include "messages/MClientRequest.h"

#include "osdc/Objecter.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".cache "

MDCache::MDCache(MDSRank *_mds) :
  mds(_mds), server(_mds->server), locker(_mds->locker),
  inode_map_lock("MDCache::inode_map_lock"),
  replay_undef_inodes(member_offset(CInode, item_dirty_parent)),
  metadata_pool(-1),
  log_segments_lock("MDCache::log_segments_lock"),
  request_map_lock("MDCache::request_map_lock"),
  rename_dir_mutex("MDCache::rename_dir_mutex"),
  open_inode_mutex("MDCache::open_inode_mutex")
{
}

void MDCache::add_inode(CInode *in)
{
  assert(in->ino() != inodeno_t());
  inode_map_lock.Lock();
  assert(inode_map.count(in->vino()) == 0);
  inode_map[in->vino()] = in;
  if (in->ino() < MDS_INO_SYSTEM_BASE) {
    if (in->ino() == MDS_INO_ROOT) {
      root = in;
    } else if (in->ino() == MDS_INO_MDSDIR(mds->get_nodeid())) {
      myin = in;
    } else if (in->is_stray()) {
      if (MDS_INO_STRAY_OWNER(in->ino()) == mds->get_nodeid()) {
	strays[MDS_INO_STRAY_INDEX(in->ino())] = in;
      }
    }
  }
  inode_map_lock.Unlock();
}

void MDCache::remove_inode(CInode *in)
{
  assert(in->get_num_ref() == 0);
  assert(in->state_test(CInode::STATE_FREEING));

  inode_map_lock.Lock();
  auto it = inode_map.find(in->vino());
  assert(it != inode_map.end());
  inode_map.erase(it);
  inode_map_lock.Unlock();
  delete in;
}

void MDCache::remove_inode_recursive(CInode *in)
{
  in->mutex_assert_locked_by_me();
  dout(10) << "remove_inode_recursive " << *in << dendl;
  list<CDir*> ls;
  in->get_dirfrags(ls);

  while (!ls.empty()) {
    CDirRef subdir = ls.front();
    ls.pop_front();

    dout(10) << " removing dirfrag " << *subdir << dendl;
    while (!subdir->empty()) {
      CDentry *dn = subdir->begin()->second;
      const CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_primary()) {
	CInode* other_in = dnl->get_inode();
	other_in->mutex_lock();
	subdir->unlink_inode(dn);
	remove_inode_recursive(other_in);
      } else if (dnl->is_remote()) {
	subdir->unlink_inode(dn);
      }
      subdir->remove_dentry(dn);
    }
    in->close_dirfrag(subdir->get_frag());
  }

  in->state_set(CInode::STATE_FREEING);
  in->mutex_lock();
  remove_inode(in);
}

CInodeRef MDCache::get_inode(const vinodeno_t &vino)
{
  CInodeRef ref;
  Mutex::Locker l(inode_map_lock);
  auto it = inode_map.find(vino);
  if (it != inode_map.end()) {
    ref = it->second;
    if (ref->state_test(CInode::STATE_FREEING))
      ref.reset();
  }
  return ref;
}

CDirRef MDCache::get_dirfrag(const dirfrag_t &df) {
  CInodeRef in = get_inode(df.ino);
  if (!in)
    return NULL;
  return in->get_dirfrag(df.frag);
}

CInodeRef MDCache::create_system_inode(inodeno_t ino, int mode)
{
  CInodeRef in = new CInode(this);
  inode_t* pi = in->__get_inode();

  pi->ino = ino;
  pi->version = 0;
  pi->xattr_version = 1;
  pi->mode = 0500 | mode;
  pi->size = 0;
  pi->ctime = pi->mtime = ceph_clock_now(g_ceph_context);
  pi->nlink = 1;
  pi->truncate_size = -1ull;

  memset(&pi->dir_layout, 0, sizeof(pi->dir_layout));
  if (pi->is_dir()) {
    pi->dir_layout.dl_dir_hash = g_conf->mds_default_dir_hash;
    ++pi->rstat.rsubdirs;
  } else {
    pi->layout = default_file_layout;
    ++pi->rstat.rfiles;
  }
  pi->accounted_rstat = pi->rstat;

  if (in->is_base()) {
  }

  add_inode(in.get());
  return in;
}

void MDCache::create_empty_hierarchy(MDSGather *gather)
{
  // create root inode 
  create_system_inode(MDS_INO_ROOT, S_IFDIR|0755);
  if (!gather) {
    // create undef inode for log replay
    return;
  }

  LogSegment *ls = mds->mdlog->_get_current_segment();

  root->mutex_lock();
  CDirRef rootdir = root->get_or_open_dirfrag(frag_t());
  rootdir->__set_version(1);

  rootdir->mark_complete();
  rootdir->mark_dirty(rootdir->pre_dirty(), ls);
  rootdir->commit(gather->new_sub());

  root->mark_dirty(root->pre_dirty(), ls);
  root->store(gather->new_sub());

  root->mutex_unlock();
}

void MDCache::create_mydir_hierarchy(MDSGather *gather)
{
  // create mds dir
  create_system_inode(MDS_INO_MDSDIR(mds->get_nodeid()), S_IFDIR);
  if (!gather) {
    // create undef inode for log replay
    return;
  }

  myin->mutex_lock();

  CDirRef mydir = myin->get_or_open_dirfrag(frag_t());
  mydir->__set_version(1);
  fnode_t *pf = mydir->__get_fnode();

  LogSegment *ls = mds->mdlog->_get_current_segment();

  // stray dir
  for (int i = 0; i < NUM_STRAY; ++i) {
    CInodeRef stray = create_system_inode(MDS_INO_STRAY(mds->get_nodeid(), i), S_IFDIR);
    stringstream name;
    name << "stray" << i;

    stray->mutex_lock();
    mydir->add_primary_dentry(name.str(), stray.get());

    CDirRef straydir = stray->get_or_open_dirfrag(frag_t());
    pf->rstat.add(stray->get_inode()->rstat);
    pf->fragstat.nsubdirs++;

    straydir->mark_complete();
    straydir->mark_dirty(straydir->pre_dirty(), ls);
    straydir->commit(gather->new_sub());

    stray->mark_dirty(stray->pre_dirty(), ls);
    stray->_mark_dirty_parent(ls, true);
    stray->store_backtrace(gather->new_sub());
    stray->mutex_unlock();
  }

  pf->accounted_fragstat = pf->fragstat;
  pf->accounted_rstat = pf->rstat;

  inode_t *pi = myin->__get_inode();
  pi->dirstat = pf->fragstat;
  pi->rstat = pf->rstat;
  pi->rstat.rsubdirs++;
  pi->accounted_rstat = pi->rstat;

  mydir->mark_complete();
  mydir->mark_dirty(mydir->pre_dirty(), ls);
  mydir->commit(gather->new_sub());

  myin->mark_dirty(myin->pre_dirty(), ls);
  myin->store(gather->new_sub());

  myin->mutex_unlock();
}

class C_MDS_OpenRootMydir : public MDSContextBase {
  MDCache *mdcache;
  MDSRank* get_mds() { return mdcache->mds; }
public:
  explicit C_MDS_OpenRootMydir(MDCache *c) : mdcache(c) {}
  void finish(int r) {
    if (r < 0) {
      // If we can't open root, something disastrous has happened: mark
      // this rank damaged for operator intervention.  Note that
      // it is not okay to call suicide() here because we are in
      // a Finisher callback.
      get_mds()->damaged();
      assert(0);  // damaged should never return
      return;
    }
    mdcache->open_root_and_mydir();
  }
};

void MDCache::open_root_and_mydir()
{
  assert(root);
  assert(myin);

  MDSGatherBuilder gather(g_ceph_context);
  if (root->get_version() == 0) {
    root->mutex_lock();
    root->fetch(gather.new_sub());
    root->mutex_unlock();
  }

  if (myin->get_version() == 0) {
    myin->mutex_lock();
    myin->fetch(gather.new_sub());
    myin->mutex_unlock();
  } else {
    populate_mydir(gather);
  }

  if (gather.has_subs()) {
    gather.set_finisher(new C_MDS_OpenRootMydir(this));
    gather.activate();
  } else {
    dout(10) << "open_root_and_mydir done" << dendl;
  }
}

void MDCache::populate_mydir(MDSGatherBuilder& gather)
{
  assert(myin);

  myin->mutex_lock();
  CDirRef mydir = myin->get_or_open_dirfrag(frag_t());

  dout(10) << "populate_mydir " << *mydir << dendl;

  if (!mydir->is_complete()) {
    mydir->fetch(gather.new_sub());
    myin->mutex_unlock();
    return;
  }

/*
  FIXME:
  if (mydir->get_version() == 0 && mydir->state_test(CDir::STATE_BADFRAG)) {
    // A missing dirfrag, we will recreate it.  Before that, we must dirty
    // it before dirtying any of the strays we create within it.
    mds->clog->warn() << "fragment " << mydir->dirfrag() << " was unreadable, "
      "recreating it now";
    LogSegment *ls = mds->mdlog->get_current_segment();
    mydir->state_clear(CDir::STATE_BADFRAG);
    mydir->mark_complete();
    mydir->mark_dirty(mydir->pre_dirty(), ls);
  }
*/

  // open or create stray
  for (int i = 0; i < NUM_STRAY; ++i) {
    stringstream name;
    name << "stray" << i;
    CDentryRef straydn = mydir->lookup(name.str());
 
    // allow for older fs's with stray instead of stray0
    assert(straydn);
    assert(strays[i]);

    myin->mutex_unlock();
    strays[i]->mutex_lock();

    // we make multiple passes through this method; make sure we only pin each stray once.
    if (!strays[i]->state_test(CInode::STATE_STRAYPINNED)) {
      strays[i]->get(CInode::PIN_STRAY);
      strays[i]->state_set(CInode::STATE_STRAYPINNED);
      //strays[i]->get_stickydirs();
    }
    dout(20) << " stray num " << i << " is " << *strays[i] << dendl;

    // open all frags
    list<frag_t> ls;
    strays[i]->dirfragtree.get_leaves(ls);
    for (auto fg : ls) {
      CDirRef dir = strays[i]->get_or_open_dirfrag(fg);

      // DamageTable applies special handling to strays: it will
      // have damaged() us out if one is damaged.
      // assert(!dir->state_test(CDir::STATE_BADFRAG));

      if (dir->get_version() == 0)
        dir->fetch(gather.new_sub());
    }

    strays[i]->mutex_unlock();
    myin->mutex_lock();
  }

  myin->mutex_unlock();

  // okay!
  //dout(10) << "populate_mydir done" << dendl;
}

bool MDCache::trim_inode(CDentry *dn, CInode *in)
{
  if (!in->mutex_trylock()) {
    return false;
  }

  if (in->get_num_ref() > 0) {
    in->mutex_unlock();
    return false;
  }
  /*
  if (in->is_dir()) {
  }
  */
  in->state_set(CInode::STATE_FREEING);

  if (dn)
    dn->get_dir()->unlink_inode(dn);

  in->mutex_unlock();

  remove_inode(in);
  // caller remove inode
  return true;
}

bool MDCache::trim_dentry(CDentry *dn)
{
  assert(dn->get_dir()->get_num_ref() > 0);
  CDirRef dir = dn->get_dir();

  dir->get_inode()->mutex_lock();
  if (dn->get_num_ref() > 0) {
  }

  const CDentry::linkage_t *dnl = dn->get_linkage();
  CInode *in = dnl->get_inode();
  if (dnl->is_primary()) {
    if (!trim_inode(dn, in)) {

    }
  } else if (dnl->is_remote()) {

  } else {

  }

  dir->remove_dentry(dn);
  dir->get_inode()->mutex_unlock();

  return true;
}

int MDCache::path_traverse(const MDRequestRef& mdr, const filepath& path,
			   vector<CDentryRef> *pdnvec, CInodeRef *pin)
{
  if (pdnvec)
    pdnvec->clear();
  if (pin)
    pin->reset();

  CInodeRef cur = get_inode(path.get_ino());
  if (!cur) {
    return -ESTALE;
  }

  int err = 0;
  for (unsigned depth = 0; depth < path.depth(); ++depth) {
    if (!cur->is_dir()) {
      return -ENOTDIR;
    }

    const string& dname = path[depth];

    cur->mutex_lock();
    frag_t fg = cur->pick_dirfrag(dname);
    CDirRef curdir = cur->get_or_open_dirfrag(fg);
    assert(curdir);

    CDentryRef dn = curdir->lookup(dname);
    CInodeRef in;
    if (dn) {
      const CDentry::linkage_t* dnl = dn->get_projected_linkage();
      if (dnl->is_null()) {
	dn->mutex_lock();
	if (!dn->lock.can_read(mdr->get_client()) &&
	    dn->lock.is_xlocked() && dn->lock.get_xlock_by() != mdr) {
	  dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mds, mdr));
	  err = 1;
	} else {
	  err = -ENOENT;
	}
	dn->mutex_unlock();
      } else {
	in = dnl->get_inode();
	if (dnl->is_remote() && !in) {
	  in = get_inode(dnl->get_remote_ino());
	  if (!in) {
	    open_remote_dentry(dnl->get_remote_ino(), dnl->get_remote_d_type(),
			       new C_MDS_RetryRequest(mds, mdr));
	    err = 1;
	  }
	}
      }
    } else if (!curdir->is_complete() &&
	       !(curdir->has_bloom() && !curdir->is_in_bloom(dname))) {
      curdir->fetch(new C_MDS_RetryRequest(mds, mdr));
      err = 1;
    } else {
      if (pdnvec) {
	if (depth == path.depth() - 1) {
	  dn = curdir->add_null_dentry(path[depth]);
	} else {
	  assert(dn = NULL);
	}
      }
      err = -ENOENT;
    }
    cur->mutex_unlock();

    if (pdnvec) {
      if (dn) {
	pdnvec->push_back(dn);
      } else {
	pdnvec->clear();
      }
    }
    if (err)
      break;
    cur.swap(in);
  }
  if (!err && pin)
    pin->swap(cur);

  return err;
}

CDentryRef MDCache::get_or_create_stray_dentry(CInode *in)
{
  string straydname;
  in->name_stray_dentry(straydname);

  CInodeRef& strayi = strays[0]; 
  strayi->mutex_lock();

  frag_t fg = strayi->pick_dirfrag(straydname);
  CDirRef straydir = strayi->get_dirfrag(fg);
  assert(straydir);
  CDentryRef straydn = straydir->lookup(straydname);
  if (!straydn) {
    straydn = straydir->add_null_dentry(straydname);
  } else {
    assert(straydn->get_projected_linkage()->is_null());
  }

  strayi->mutex_unlock();
  return straydn;
}

CInodeRef MDCache::replay_invent_inode(inodeno_t ino)
{
  CInodeRef in = new CInode(this);
  inode_t *pi = in->__get_inode();
  pi->ino = ino;
  pi->mode = S_IFDIR|0755;
  in->state_set(CInode::STATE_REPLAYUNDEF);
  replay_undef_inodes.push_back(&in->item_dirty_parent);
  add_inode(in.get());
  dout(10) << "replay_invent_inode " << *in << dendl;
  return in;
}

void MDCache::open_replay_undef_inodes(MDSContextBase *fin)
{
  dout(10) << "open_replay_undef_inodes" << dendl;
  list<inodeno_t> inos;
  for (auto p = replay_undef_inodes.begin(); !p.end(); ++p)
    inos.push_back((*p)->ino());

  MDSGatherBuilder gather(g_ceph_context, fin);
  for (auto ino : inos)
    open_inode(ino, metadata_pool, gather.new_sub());

  assert(gather.has_subs());
  gather.activate();
}

void MDCache::init_layouts()
{
  metadata_pool = mds->get_metadata_pool();
  default_file_layout = file_layout_t::get_default();
  default_file_layout.pool_id = mds->mdsmap->get_first_data_pool();
  default_log_layout = file_layout_t::get_default();
  default_log_layout.pool_id = metadata_pool;
}

MDRequestRef MDCache::request_start(MClientRequest *req)
{
  // register new client request
  MDRequestImpl::Params params;
  params.reqid = req->get_reqid();
  params.attempt = req->get_num_fwd();
  params.client_req = req;

  MDRequestRef mdr(new MDRequestImpl(params));
  mdr->set_op_stamp(req->get_stamp());


  request_map_lock.Lock();
  assert(request_map.count(params.reqid) == 0);
  request_map[params.reqid] = mdr;
  request_map_lock.Unlock();
  dout(7) << "request_start " << *mdr << dendl;
  return mdr;
}

MDRequestRef MDCache::request_get(metareqid_t rid)
{
  MDRequestRef mdr;
  request_map_lock.Lock();
  auto p = request_map.find(rid);
  assert(p != request_map.end());
  mdr = p->second;
  request_map_lock.Unlock();
  dout(7) << "request_get " << rid << " " << *mdr << dendl;
  return mdr;
}

void MDCache::dispatch_request(const MDRequestRef& mdr)
{
  Mutex::Locker l(mdr->dispatch_mutex);
  if (mdr->killed) {
    dout(10) << "request " << *mdr << " was killed" << dendl;
    return;
  }
  if (mdr->client_request) {
    server->dispatch_client_request(mdr);
  } else {
    assert(0);
  }
}

void MDCache::request_finish(const MDRequestRef& mdr)
{
  dout(7) << "request_finish " << *mdr << dendl;

  request_cleanup(mdr);
}

void MDCache::request_cleanup(const MDRequestRef& mdr)
{
  dout(15) << "request_cleanup " << *mdr << dendl;

  locker->drop_locks(mdr);

  mdr->cleanup();

  if (mdr->session) {
    mdr->session->mutex_lock();
    mdr->item_session_request.remove_myself();
    mdr->session->mutex_unlock();
    mdr->session = NULL;
  }

  request_map_lock.Lock();
  auto p = request_map.find(mdr->reqid);
  assert(p != request_map.end());
  request_map.erase(p);
  request_map_lock.Unlock();
}

void MDCache::request_kill(const MDRequestRef& mdr)
{
  mdr->dispatch_mutex.Lock();
  mdr->killed = true;
  if (mdr->is_committing()) {
    dout(10) << "request_kill " << *mdr << " -- already committing, no-op" << dendl;
  } else {
    dout(10) << "request_kill " << *mdr << dendl;
    request_cleanup(mdr);
  }
  mdr->dispatch_mutex.Unlock();
}

void MDCache::lock_parents_for_linkunlink(const MDRequestRef& mdr, CInode *in,
					  CDentry *dn, bool apply)
{
  for (;;) {
    CDentryRef parent_dn;
    CInode *parent_dn_diri;
    if (apply) 
      parent_dn = in->get_lock_parent_dn();
    else
      parent_dn = in->get_lock_projected_parent_dn();

    parent_dn_diri = parent_dn->get_dir_inode();
    if (parent_dn_diri == dn->get_dir_inode()) {
      mdr->add_locked_object(parent_dn_diri);
      return;
    }

    bool done = false;
    if (dn->get_dir_inode()->is_stray()) {
      dn->get_dir_inode()->mutex_lock();
      done = true;
    } else if (dn->get_dir_inode()->mutex_trylock()) {
      done = true;
    }
    if (done) {
      mdr->add_locked_object(parent_dn_diri);
      mdr->add_locked_object(dn->get_dir_inode());
      return;
    }

    parent_dn_diri->mutex_unlock();

    bool primary_first = false;
    bool hold_rename_mutex = false;
    if (!parent_dn_diri->is_stray()) {
      rename_dir_mutex.Lock();
      hold_rename_mutex = true;
      if (dn->get_dir_inode()->is_projected_ancestor_of(parent_dn_diri)) {
	// primary_first = false;
      } else if (parent_dn_diri->is_projected_ancestor_of(dn->get_dir_inode())) {
	primary_first = true;
      } else if (parent_dn_diri->is_lt(dn->get_dir_inode())) {
	primary_first = true;
      }
    }

    if (primary_first) {
      parent_dn_diri->mutex_lock();
      dn->get_dir_inode()->mutex_lock();
    } else {
      dn->get_dir_inode()->mutex_lock();
      parent_dn_diri->mutex_lock();
    }

    if (hold_rename_mutex)
      rename_dir_mutex.Unlock();

    const CDentry::linkage_t *dnl;
    if (apply)
      dnl = parent_dn->get_linkage();
    else
      dnl = parent_dn->get_projected_linkage();
    if (dnl->is_primary() && dnl->get_inode() == in) {
      mdr->add_locked_object(parent_dn_diri);
      mdr->add_locked_object(dn->get_dir_inode());
      break;
    }

    parent_dn_diri->mutex_unlock();
    dn->get_dir_inode()->mutex_unlock();
  }
}

int MDCache::lock_parents_for_rename(const MDRequestRef& mdr, CInode *srci, CInode *oldin,
				     CDentry *srcdn, CDentry *destdn, bool apply)
{
  struct lock_object_lt {
    bool operator()(CInode* l, CInode* r) const {
      if (l == r)
	return false;
      if (r->is_stray()) {
	if (!l->is_stray())
	  return true;
	return l < r;
      }
      if (l->is_stray())
	return false;
      if (l->is_projected_ancestor_of(r))
	return true;
      if (r->is_projected_ancestor_of(l))
	return false;
      return l->is_lt(r);
    }
  };

  bool rename_dir = srci->is_dir();
  for (;;) {
    CDentryRef srci_parent_dn;
    CDentryRef oldin_parent_dn;

    srci->mutex_lock();
    if (apply)
      srci_parent_dn = srci->get_parent_dn();
    else
      srci_parent_dn = srci->get_projected_parent_dn();
    srci->mutex_unlock();

    if (oldin) {
      oldin->mutex_lock();
      if (apply)
	oldin_parent_dn = oldin->get_parent_dn();
      else
	oldin_parent_dn = oldin->get_projected_parent_dn();
      oldin->mutex_unlock();
    }

    rename_dir_mutex.Lock();
    if (rename_dir && !apply) {
      if (srci->is_projected_ancestor_of(destdn->get_dir_inode())) {
	rename_dir_mutex.Unlock();
	return -EINVAL;
      }
    }

    std::set<CInode*, lock_object_lt> sorted;
    sorted.insert(srcdn->get_dir_inode());
    sorted.insert(destdn->get_dir_inode());
    sorted.insert(srci_parent_dn->get_dir_inode());
    if (oldin_parent_dn)
      sorted.insert(oldin_parent_dn->get_dir_inode());

    for (auto p : sorted)
      p->mutex_lock();

    if (!rename_dir)
      rename_dir_mutex.Unlock();

    const CDentry::linkage_t *srci_dnl, *oldin_dnl = NULL;
    if (apply) {
      srci_dnl = srci_parent_dn->get_linkage();
      if (oldin_parent_dn)
	oldin_dnl = oldin_parent_dn->get_linkage();
    } else {
      srci_dnl = srci_parent_dn->get_projected_linkage();
      if (oldin_parent_dn)
	oldin_dnl = oldin_parent_dn->get_projected_linkage();
    }

    if (srci_dnl->is_primary() && srci_dnl->get_inode() == srci &&
	(!oldin_parent_dn ||
	 (oldin_dnl->is_primary() && oldin_dnl->get_inode() == oldin))) {
      for (auto p : sorted)
	mdr->add_locked_object(p);
      mdr->hold_rename_dir_mutex = rename_dir;
      return 0;
    }

    if (rename_dir)
      rename_dir_mutex.Unlock();

    for (auto p : sorted)
      p->mutex_unlock();
  }
}

void MDCache::lock_objects_for_update(const MutationRef& mut, CInode *in, bool apply)
{
  if (in->is_base()) {
    mut->lock_object(in);
    return;
  }

  CDentryRef dn;
  if (apply)
    dn = in->get_lock_parent_dn();
  else
    dn = in->get_lock_projected_parent_dn();
  mut->add_locked_object(dn->get_dir_inode());
  mut->lock_object(in);
}

void MDCache::project_rstat_inode_to_frag(CInode *in, CDir *dir, int linkunlink)
{
  fnode_t *pf = dir->__get_projected_fnode();
  inode_t *pi = in->__get_projected_inode();

  nest_info_t delta;
  if (linkunlink == 0) {
    delta.add(pi->rstat);
    delta.sub(pi->accounted_rstat);
  } else if (linkunlink < 0) {
    delta.sub(pi->accounted_rstat);
  } else {
    delta.add(pi->rstat);
  }

  pi->accounted_rstat = pi->rstat;
  pf->rstat.add(delta);
}

void MDCache::project_rstat_frag_to_inode(const fnode_t *pf, inode_t *pi)
{
  nest_info_t delta = pf->rstat;
  delta.sub(pf->accounted_rstat);

  pi->rstat.add(delta);
}

void MDCache::predirty_journal_parents(const MutationRef& mut, EMetaBlob *blob,
				       CInode *in, CDir *parent,
				       int flags, int linkunlink)
{
  bool parent_dn = flags & PREDIRTY_PRIMARY;
  bool update_parent_mtime = flags & PREDIRTY_DIR;

  // make sure stamp is set
  if (mut->get_mds_stamp() == utime_t())
    mut->set_mds_stamp(ceph_clock_now(g_ceph_context));

  in->mutex_assert_locked_by_me();

  if (in->is_base())
    return;
  
  if (!parent)
    parent = in->get_projected_parent_dn()->get_dir();

  parent->get_inode()->mutex_assert_locked_by_me();

  if (flags == 0 && linkunlink == 0) {
    // blob->add_dir_context(parent);
    return;
  }	

  list<CInode*> lsi;
  bool first = true;
  CInode *cur = in;
  while (parent) {
    CInode *pin = parent->get_inode();

    mut->add_projected_fnode(parent, first);
    fnode_t *pf = parent->project_fnode();
    pf->version = parent->pre_dirty();

    if (update_parent_mtime || linkunlink) {
      assert(mut->wrlocks.count(&pin->filelock));
      assert(mut->wrlocks.count(&pin->nestlock));

      // update stale fragstat/rstat?
      parent->resync_accounted_fragstat(pf);
      parent->resync_accounted_rstat(pf);

      if (update_parent_mtime) {
	pf->fragstat.mtime = mut->get_op_stamp();
	if (pf->fragstat.mtime > pf->rstat.rctime)
	  pf->rstat.rctime = pf->fragstat.mtime;
      }
      if (linkunlink) {
	if (cur->is_dir())
	  pf->fragstat.nsubdirs += linkunlink;
	else
	  pf->fragstat.nfiles += linkunlink;
      }
    }

    bool stop = false;
    // rstat
    if (!parent_dn) {
      stop = true;
      // don't update parent this pass
    } else if (!linkunlink && !(pin->nestlock.can_wrlock(-1) &&
				pin->versionlock.can_wrlock())) {
      stop = true;
      cur->mark_dirty_rstat();
    } else {
      if (linkunlink)
	assert(mut->wrlocks.count(&pin->nestlock));

      if (mut->wrlocks.count(&pin->nestlock) == 0) {
	locker->wrlock_force(&pin->nestlock, mut);
      }

      parent->resync_accounted_rstat(pf);
      project_rstat_inode_to_frag(cur, parent, linkunlink);
      cur->clear_dirty_rstat();
    }


    CDentry *parentdn = NULL; 
    if (!stop && !pin->is_base()) {
      parentdn = pin->get_projected_parent_dn();
      if (parentdn->get_dir_inode()->mutex_trylock())
	mut->add_locked_object(parentdn->get_dir_inode());
      else
	stop = true;
    }
    if (!stop && !mut->wrlocks.count(&pin->nestlock) &&
	(!pin->versionlock.can_wrlock() ||
	 !locker->wrlock_start(&pin->nestlock, mut))) {
      stop = true;
    }

    if (stop) {
      int mask = CEPH_LOCK_INEST;
      if (update_parent_mtime || linkunlink)
	mask |= CEPH_LOCK_IFILE;
      mut->add_updated_lock(pin, mask);
      break;
    }

    assert(mut->wrlocks.count(&pin->nestlock));
    if (!mut->wrlocks.count(&pin->versionlock))
      locker->local_wrlock_grab(&pin->versionlock, mut);

    lsi.push_front(pin);

    mut->add_projected_inode(pin, false);
    inode_t *pi = pin->project_inode();
    pi->version = pin->pre_dirty();

    if (update_parent_mtime || linkunlink) {
      bool touched_mtime = false;
      pi->dirstat.add_delta(pf->fragstat, pf->accounted_fragstat, touched_mtime);
      pf->accounted_fragstat = pf->fragstat;
      if (touched_mtime)
	pi->mtime = pi->ctime = pi->dirstat.mtime;
    }

    // frag rstat -> inode rstat
    project_rstat_frag_to_inode(pf, pi);
    pf->accounted_rstat = pf->rstat;

    if (pin->is_base())
      break;

    cur = pin;
    parent = parentdn->get_dir();

    linkunlink = 0;
    first = false;
    parent_dn = true;
    update_parent_mtime = false;
  }

  for (auto in : lsi) {
    journal_dirty_inode(mut, blob, in);
  }
}

void MDCache::journal_dirty_inode(const MutationRef& mut, EMetaBlob *metablob, CInode *in)
{
  assert(mut->is_object_locked(in));
  if (in->is_base()) {
    metablob->add_root(true, in, in->get_projected_inode());
  } else {
    CDentry *dn = in->get_projected_parent_dn();
    assert(mut->is_object_locked(dn->get_dir_inode()));
    metablob->add_primary_dentry(dn, in, true);
  }
}

// -------------------------------------------------------------------------------
// Open inode by inode number

class C_MDC_OI_BacktraceFetched : public MDSAsyncContextBase {
protected:
  MDCache *mdcache;
  inodeno_t ino;
  MDSRank* get_mds() { return mdcache->mds; };
public:
  bufferlist bl;
  C_MDC_OI_BacktraceFetched(MDCache *c, inodeno_t i) : mdcache(c), ino(i) { }
  void finish(int r) {
    mdcache->_open_inode_backtrace_fetched(ino, bl, r);
  }
};

struct C_MDC_OI_LookupDentry : public MDSContextBase {
  MDCache *mdcache;
  inodeno_t ino;
  MDSRank* get_mds() { return mdcache->mds; };
public:
  C_MDC_OI_LookupDentry(MDCache *c, inodeno_t i) : mdcache(c), ino(i) {}
  void finish(int r) {
    assert(r >= 0);
    mdcache->_open_inode_lookup_dentry(ino);
  }
};

void MDCache::fetch_backtrace(inodeno_t ino, int64_t pool, bufferlist& bl,
			      MDSAsyncContextBase *fin)
{
  object_t oid = CInode::get_object_name(ino, frag_t(), "");
  mds->objecter->getxattr(oid, object_locator_t(pool), "parent", CEPH_NOSNAP, &bl, 0, fin);
}

void MDCache::_open_inode_backtrace_fetched(inodeno_t ino, bufferlist& bl, int err)
{
  dout(10) << "_open_ino_backtrace_fetched ino " << ino << " errno " << err << dendl;

  open_inode_mutex.Lock();

  auto p = opening_inodes.find(ino);
  assert(p != opening_inodes.end());
  open_inode_info_t& info = p->second;

  CInodeRef in = get_inode(ino);
  if (in && !in->state_test(CInode::STATE_REPLAYUNDEF)) {
    dout(10) << " found cached " << *in << dendl;
    _open_inode_finish(ino, info, 0);
    return;
  }

  inode_backtrace_t backtrace;
  if (err == 0) {
    ::decode(backtrace, bl);
    if (backtrace.pool != info.pool && backtrace.pool != -1) {
      dout(10) << " old object in pool " << info.pool
               << ", retrying pool " << backtrace.pool << dendl;
      info.pool = backtrace.pool;
      open_inode_mutex.Unlock();

      C_MDC_OI_BacktraceFetched *fin = new C_MDC_OI_BacktraceFetched(this, ino);
      fetch_backtrace(ino, backtrace.pool, fin->bl, fin);
      return;
    }
  } else if (err == -ENOENT) {
    if (info.pool != metadata_pool) {
      dout(10) << " no object in pool " << info.pool
               << ", retrying pool " << metadata_pool << dendl;
      info.pool = metadata_pool;
      open_inode_mutex.Unlock();

      C_MDC_OI_BacktraceFetched *fin = new C_MDC_OI_BacktraceFetched(this, ino);
      fetch_backtrace(ino, metadata_pool, fin->bl, fin);
      return;
    }
  }
  if (err == 0) {
    if (backtrace.ancestors.empty()) {
      dout(10) << " got empty backtrace " << dendl;
      err = -EIO;
    } else if (!info.ancestors.empty()) {
      if (info.ancestors[0] == backtrace.ancestors[0]) {
        dout(10) << " got same parents " << info.ancestors[0] << " 2 times" << dendl;
        err = -EIO;
      }
    }
  }
  if (err < 0) {
    dout(10) << " failed to open ino " << ino << dendl;
    _open_inode_finish(ino, info, err);
    return;
  }

  dout(10) << " got backtrace " << backtrace << dendl;
  info.ancestors = backtrace.ancestors;
  int64_t pool = info.pool;
  open_inode_mutex.Unlock();

  _open_inode_lookup_dentry(ino, pool, backtrace.ancestors[0]);
  return;
}

void MDCache::_open_inode_lookup_dentry(inodeno_t ino)
{
  dout(10) << "_open_ino_lookup_dentry ino " << ino << dendl;
  open_inode_mutex.Lock();

  auto p = opening_inodes.find(ino);
  assert(p != opening_inodes.end());
  open_inode_info_t& info = p->second;

  CInodeRef in = get_inode(ino);
  if (in && !in->state_test(CInode::STATE_REPLAYUNDEF)) {
    dout(10) << " found cached " << *in << dendl;
    _open_inode_finish(ino, info, 0);
    return;
  }

  inode_backpointer_t parent = info.ancestors[0];
  int64_t pool = info.pool;
  open_inode_mutex.Unlock();

  _open_inode_lookup_dentry(ino, pool, parent);
}

void MDCache::_open_inode_lookup_dentry(inodeno_t ino, int64_t pool, inode_backpointer_t& parent)
{ 
  CInodeRef diri = get_inode(parent.dirino);
  if (!diri || diri->state_test(CInode::STATE_REPLAYUNDEF)) {
    open_inode(parent.dirino, metadata_pool, new C_MDC_OI_LookupDentry(this, ino));
    return;
  }

  int err;
  if (diri->is_dir()) {
    err = -ENOENT;
    diri->mutex_lock();
    const string &dname = parent.dname;
    frag_t fg = diri->pick_dirfrag(dname);
    CDirRef dir = diri->get_or_open_dirfrag(fg);

    CDentryRef dn = dir->lookup(dname);
    if (dn) {
      const CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_primary() && dnl->get_inode()->ino() == ino) {
	if (dnl->get_inode()->state_test(CInode::STATE_REPLAYUNDEF)) {
	  dir->fetch(new C_MDC_OI_LookupDentry(this, ino));
	  err = 1;
	} else {
	  err = 0;
	}
      }
    } else if (!dir->is_complete() &&
	       !(dir->has_bloom() && !dir->is_in_bloom(dname))) {
      dir->fetch(new C_MDC_OI_LookupDentry(this, ino));
      err = 1;
    }
    diri->mutex_unlock();
  } else {
    err = -ENOTDIR;
  }

  if (err > 0)
    return;

  if (err == 0) {
    open_inode_mutex.Lock();
    auto p = opening_inodes.find(ino);
    assert(p != opening_inodes.end());
    dout(10) << " found cached ino " << ino << dendl;
    _open_inode_finish(ino, p->second, 0);
    return;
  }

  dout(10) << " lookup err " << err << ", check backtrace again"  << dendl;
  C_MDC_OI_BacktraceFetched *fin = new C_MDC_OI_BacktraceFetched(this, ino);
  fetch_backtrace(ino, pool, fin->bl, fin);
}

void MDCache::_open_inode_finish(inodeno_t ino, open_inode_info_t& info, int ret)
{
  assert(open_inode_mutex.is_locked_by_me());
  dout(10) << "_open_inode_finish ino " << ino << " ret " << ret << dendl;

  list<MDSContextBase*> waiters;
  waiters.swap(info.waiters);
  opening_inodes.erase(ino);

  open_inode_mutex.Unlock();

  finish_contexts(g_ceph_context, waiters, ret);
}

void MDCache::open_inode(inodeno_t ino, int64_t pool, MDSContextBase* fin)
{
  dout(10) << "_open_inode ino " << ino << " pool " << pool << dendl;
  bool is_new = false;
  open_inode_mutex.Lock();
  auto p = opening_inodes.find(ino);
  if (p != opening_inodes.end()) {
    p->second.waiters.push_back(fin);
  } else {
    is_new = true;
    if (pool < 0)
      pool = default_file_layout.pool_id;
    open_inode_info_t& oi = opening_inodes[ino];
    oi.pool = pool;
    if (fin)
      oi.waiters.push_back(fin);
  }
  open_inode_mutex.Unlock();
  if (is_new) {
    C_MDC_OI_BacktraceFetched *fin = new C_MDC_OI_BacktraceFetched(this, ino);
    fetch_backtrace(ino, pool, fin->bl, fin);
  }
}

void MDCache::open_remote_dentry(inodeno_t ino, uint8_t d_type , MDSContextBase* fin) {
  int64_t pool = (d_type == DT_DIR) ? metadata_pool : -1;
  open_inode(ino, pool, fin);
}

ESubtreeMap *MDCache::create_subtree_map()
{
  ESubtreeMap *le = new ESubtreeMap();
  mds->mdlog->_start_entry(le);
  return le;
}


