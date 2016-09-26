#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "Locker.h"
#include "Mutation.h"

#include "events/EMetaBlob.h"

#include "osdc/Objecter.h"
#include "include/stringify.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.dir(" << dirfrag() << ") "

class CDirContext : public MDSAsyncContextBase
{
protected:
  CDirRef dir;
  MDSRank* get_mds() { return dir->mdcache->mds; }

public:
  explicit CDirContext(CDir *d) : dir(d) {
    assert(dir != NULL);
    set_finisher(get_mds()->finisher);
  }
};

ostream& operator<<(ostream& out, const CDir& dir)
{
  bool locked = dir.get_inode()->mutex_is_locked_by_me();
  bool need_unlock = false;
  if (!locked && dir.get_inode()->mutex_trylock()) {
    locked = true;
    need_unlock = true;
  }

  out << "[dir " << dir.dirfrag();
  out << " v" << dir.get_version();
  out << " pv" << dir.get_projected_version();
  out << " state=" << hex << dir.get_state() << dec;

  if (locked) {
    string path;
    dir.get_inode()->make_string(path);
    out << " " << path;

    const fnode_t *of = dir.get_fnode();    
    // fragstat
    out << " " << of->fragstat;
    if (!(of->fragstat == of->accounted_fragstat))
      out << "/" << of->accounted_fragstat;

    // rstat
    out << " " << of->rstat;
    if (!(of->rstat == of->accounted_rstat))
      out << "/" << of->accounted_rstat;
  } else {
    out << " (unlocked)...";
  }

  out << " ref=" << dir.get_num_ref();
  out << " " << &dir;
  out << "]";

  if (need_unlock)
    dir.get_inode()->mutex_unlock();

  return out;
}

CDir::CDir(CInode *in) :
  CObject("CDir"), mdcache(in->mdcache), inode(in),
  projected_version(0),
  committing_version(0), committed_version(0),
  dirty_dentries(member_offset(CDentry, item_dir_dirty)),
  num_head_items(0), num_head_null(0),
  dirty_rstat_inodes(member_offset(CInode, item_dirty_rstat)),
  bloom(NULL)
{
}

CDir::~CDir()
{
  remove_bloom();
}

dirfrag_t CDir::dirfrag() const
{
  return dirfrag_t(get_inode()->ino(), get_frag());
}

void CDir::make_string(std::string& s) const
{
  s = "dir(" + stringify(dirfrag()) + ")";
}

bool CDir::is_lt(const CObject *r) const
{
  return dirfrag() < (static_cast<const CDir*>(r))->dirfrag();
}

fnode_t *CDir::project_fnode()
{
  inode->mutex_assert_locked_by_me();
  const fnode_t *p = get_projected_fnode();
  projected_fnode.push_back(*p);
  dout(10) << "project_fnode " << &projected_fnode.back() << dendl;
  return &projected_fnode.back();
}

void CDir::pop_and_dirty_projected_fnode(LogSegment *ls)
{
  inode->mutex_assert_locked_by_me();
  assert(!projected_fnode.empty());
  const fnode_t *pf = &projected_fnode.front(); 
  dout(10) << "pop_and_dirty_projected_fnode " << pf << " v " << pf->version << dendl;
  assert(pf->version > get_version());
  assert(pf->version <= projected_version);

  fnode = *pf;
  projected_fnode.pop_front();
  _mark_dirty(ls);
}

version_t CDir::pre_dirty(version_t min)
{ 
  inode->mutex_assert_locked_by_me();
  if (min > projected_version)
    projected_version = min;
  ++projected_version;
  dout(10) << "pre_dirty " << projected_version << dendl;
  return projected_version;
}

void CDir::mark_dirty(version_t pv, LogSegment *ls)
{
  assert(get_version() < pv);
  assert(pv <= projected_version);
  fnode.version = pv;
  _mark_dirty(ls);
}

void CDir::_mark_dirty(LogSegment *ls)
{
  inode->mutex_assert_locked_by_me();
  if (!state_test(STATE_DIRTY)) {
    dout(10) << "mark_dirty (was clean) " << *this << " version " << get_version() << dendl;
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
    assert(ls);
  } else {
    dout(10) << "mark_dirty (already dirty) " << *this << " version " << get_version() << dendl;
  }

  if (ls) {
    mdcache->lock_log_segments();
    ls->dirty_dirfrags.push_back(&item_dirty);
    if (committed_version == 0 && !is_new()) {
      state_set(STATE_NEW);
      ls->new_dirfrags.push_back(&item_new);
    }
    mdcache->unlock_log_segments();
  }
}

void CDir::mark_clean()
{
  dout(10) << "mark_clean " << *this << " version " << get_version() << dendl;
  if (state_test(STATE_DIRTY)) {
    mdcache->lock_log_segments();
    item_dirty.remove_myself();
    item_new.remove_myself();
    mdcache->unlock_log_segments();

    state_clear(STATE_DIRTY|STATE_NEW);
    put(PIN_DIRTY);
  }
}

void CDir::add_dirty_dentry(CDentry *dn)
{
  inode->mutex_assert_locked_by_me();
  dirty_dentries.push_back(&dn->item_dir_dirty);
}

void CDir::remove_dirty_dentry(CDentry *dn)
{
  inode->mutex_assert_locked_by_me();
  dn->item_dir_dirty.remove_myself();
}

void CDir::clear_new()
{
  if (state_test(STATE_NEW)) {
    mdcache->lock_log_segments();
    item_new.remove_myself();
    mdcache->unlock_log_segments();
    state_clear(STATE_DIRTY);
  }
}

void CDir::link_remote_inode(CDentry *dn, inodeno_t ino, uint8_t d_type)
{
//  dout(12) << "link_remote_inode " << *dn << " remote " << ino << dendl;
  dn->link_inode_work(ino, d_type);
  num_head_items++;
  num_head_null--;
}

void CDir::link_primary_inode(CDentry *dn, CInode *in)
{ 
//  dout(12) << "link_primary_inode " << *dn << " " << *in << dendl;
  dn->link_inode_work(in);
  num_head_items++;
  num_head_null--;
}

void CDir::unlink_inode(CDentry *dn)
{
//  dout(12) << "unlink_inode " << *dn << dendl;
  dn->unlink_inode_work();
  num_head_items--;
  num_head_null++;
}

CDentryRef CDir::add_null_dentry(const string& dname)
{
  inode->mutex_assert_locked_by_me();
  CDentryRef dn = new CDentry(this, dname);

  if (items.empty())
    get(PIN_CHILD);
  else
    assert(items.count(dn->get_key()) == 0);
  items[dn->get_key()] = dn.get();

  num_head_null++;

  return dn;
}

CDentryRef CDir::add_primary_dentry(const string& dname, CInode *in)
{
  inode->mutex_assert_locked_by_me();
  CDentryRef dn = new CDentry(this, dname);

  if (items.empty()) 
    get(PIN_CHILD);
  else
    assert(items.count(dn->get_key()) == 0);
  items[dn->get_key()] = dn.get();

  num_head_items++;

  dn->link_inode_work(in);
  return dn;
}

CDentryRef CDir::add_remote_dentry(const string& dname, inodeno_t ino, uint8_t d_type)
{
  inode->mutex_assert_locked_by_me();
  CDentryRef dn = new CDentry(this, dname);

  if (items.empty()) 
    get(PIN_CHILD);
  else
    assert(items.count(dn->get_key()) == 0);
  items[dn->get_key()] = dn.get();

  num_head_items++;

  dn->link_inode_work(ino, d_type);
  return dn;
}

void CDir::remove_dentry(CDentry *dn)
{
  inode->mutex_assert_locked_by_me();

  assert(dn->get_linkage()->is_null());

  // remove from list
  auto it = items.find(dn->get_key());
  assert(it != items.end());
  items.erase(it);

  num_head_null--;

  if (items.empty())
    put(PIN_CHILD);
}

CDentry* CDir::__lookup(const char *name, snapid_t snap)
{
  assert(snap == CEPH_NOSNAP);
  dout(20) << "lookup (" << snap << ", '" << name << "')" << dendl;
  auto it = items.lower_bound(dentry_key_t(snap, name));
  if (it == items.end())
    return 0;
  if (it->second->get_name() == name) {
    dout(20) << "  hit -> " << it->first << dendl;
    return it->second;
  }
  dout(20) << "  miss -> " << it->first << dendl;
  return 0;
}

void CDir::encode_dirstat(bufferlist& bl, mds_rank_t whoami)
{
  frag_t frag;
  mds_rank_t auth = whoami;
  std::set<mds_rank_t> dist;
  ::encode(frag, bl);
  ::encode(auth, bl);
  ::encode(dist, bl);
}

void CDir::resync_accounted_fragstat(fnode_t *pf)
{
  const inode_t *pi = inode->get_projected_inode();
  if (pf->accounted_fragstat.version != pi->dirstat.version) {
    pf->fragstat.version = pi->dirstat.version;
    dout(10) << "resync_accounted_fragstat " << pf->accounted_fragstat << " -> " << pf->fragstat << dendl;
    pf->accounted_fragstat = pf->fragstat;
  }
}

/*
 * resync rstat and accounted_rstat with inode
 */
void CDir::resync_accounted_rstat(fnode_t *pf)
{
  const inode_t *pi = inode->get_projected_inode();
  if (pf->accounted_rstat.version != pi->rstat.version) {
    pf->rstat.version = pi->rstat.version;
    dout(10) << "resync_accounted_rstat " << pf->accounted_rstat << " -> " << pf->rstat << dendl;
    pf->accounted_rstat = pf->rstat;
  }
}

void CDir::add_dirty_rstat_inode(CInode *in)
{
  inode->mutex_assert_locked_by_me();
  dirty_rstat_inodes.push_back(&in->item_dirty_rstat);
  mdcache->locker->mark_updated_scatterlock(&inode->nestlock);
}

void CDir::remove_dirty_rstat_inode(CInode *in)
{
  inode->mutex_assert_locked_by_me();
  in->item_dirty_rstat.remove_myself();
}

void CDir::assimilate_dirty_rstat_inodes(const MutationRef& mut)
{ 
  inode->mutex_assert_locked_by_me();
  dout(10) << "assimilate_dirty_rstat_inodes" << dendl;
  for (elist<CInode*>::iterator p = dirty_rstat_inodes.begin_use_current();
      !p.end(); ++p) {
    CInode *in = *p;

    in->mutex_lock();

    assert(in->get_projected_parent_dn()->get_dir() == this);

    mut->pin(in);
    mut->add_projected_inode(in, true);
    inode_t *pi = in->project_inode();
    pi->version = in->pre_dirty();

    mdcache->project_rstat_inode_to_frag(in, this, 0);

    in->mutex_unlock();
  }
  state_set(STATE_ASSIMRSTAT);
  dout(10) << "assimilate_dirty_rstat_inodes done" << dendl;
}

void CDir::assimilate_dirty_rstat_inodes_finish(const MutationRef& mut, EMetaBlob *blob)
{
  inode->mutex_assert_locked_by_me();
  if (!state_test(STATE_ASSIMRSTAT))
    return;

  state_clear(STATE_ASSIMRSTAT);
  dout(10) << "assimilate_dirty_rstat_inodes_finish" << dendl;

  elist<CInode*>::iterator p = dirty_rstat_inodes.begin_use_current();
  while (!p.end()) {
    CInode *in = *p;
    ++p;

    in->mutex_lock();
    in->clear_dirty_rstat();
    blob->add_primary_dentry(in->get_projected_parent_dn(), in, true);
    in->mutex_unlock();
  }

  assert(dirty_rstat_inodes.empty());
}

// -----------------------
// COMMIT

/**
 * commit
 *
 * @param want - min version i want committed
 * @param c - callback for completion
 */
void CDir::commit(MDSContextBase *c, int op_prio)
{
  inode->mutex_assert_locked_by_me();

  dout(10) << "commit on " << *this << dendl;

  version_t want = get_version();
  assert(want >= committed_version);

  if (committed_version == want) {
    dout(10) << "already committed " << committed_version << " == " << want << dendl;
    assert(!state_test(STATE_COMMITTING));
    mdcache->mds->queue_context(c);
    return;
  }

#if 0
  if (inode->inode.nlink == 0 && !inode->snaprealm) {
    dout(7) << "commit dirfrag for unlinked directory, mark clean" << dendl;
    try_remove_dentries_for_stray();
    if (c)
      cache->mds->queue_waiter(c);
    return;
  }
#endif

  // auth_pin on first waiter
  if (waiting_for_commit.empty())
    get(PIN_COMMITTING);
  if (c)
    waiting_for_commit[want].push_back(c);
  else
    waiting_for_commit[want].size();

  // alrady committed an older version?
  if (committing_version > committed_version) {
    dout(10) << "already committing older " << committing_version << ", waiting for that to finish" << dendl;
    assert(state_test(STATE_COMMITTING));
    return;
  }

  // commit.
  committing_version = want;

  // mark committing (if not already)
  assert(!state_test(STATE_COMMITTING));
  dout(10) << "marking committing" << dendl;
  state_set(STATE_COMMITTING);

  // ok.
  _omap_commit(op_prio);
}

class C_Dir_Committed : public CDirContext {
  version_t version;
public:
  C_Dir_Committed(CDir *d, version_t v) : CDirContext(d), version(v) { }
  void finish(int r) {
    dir->_committed(r, version);
  }
};

object_t CDir::get_ondisk_object() const {
  dirfrag_t df = dirfrag();
  return file_object_t(df.ino, df.frag);
}

/**
 * Flush out the modified dentries in this dir. Keep the bufferlist
 * below max_write_size;
 */
void CDir::_omap_commit(int op_prio)
{
  dout(10) << "_omap_commit" << dendl;

  unsigned max_write_size = /* FIXME: mdcache->max_dir_commit_size; */ 128 * 1024 * 1024;
  unsigned write_size = 0;

  if (op_prio < 0)
    op_prio = CEPH_MSG_PRIO_DEFAULT;


  set<string> to_remove;
  map<string, bufferlist> to_set;

  C_GatherBuilder gather(g_ceph_context, new C_Dir_Committed(this, get_version()));

  SnapContext snapc;
  object_t oid = get_ondisk_object();
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  for (auto p = dirty_dentries.begin(); !p.end(); ) {
    CDentry *dn = *p;
    ++p;

    string key;
    dn->get_key().encode(key);

    assert(dn->is_dirty());

    if (dn->get_linkage()->is_null()) {
      dout(10) << " rm " << dn->get_name() << " " << *dn << dendl;
      write_size += key.length();
      to_remove.insert(key);
    } else {
      dout(10) << " set " << dn->get_name() << " " << *dn << dendl;
      bufferlist dnbl;
      _encode_dentry(dn, dnbl);
      write_size += key.length() + dnbl.length();
      to_set[key].swap(dnbl);
    }

    if (write_size >= max_write_size) {
      ObjectOperation op;
      op.priority = op_prio;

      // don't create new dirfrag blindly
      if (!is_new())
	op.stat(NULL, (ceph::real_time*) NULL, NULL);

      if (!to_set.empty())
	op.omap_set(to_set);
      if (!to_remove.empty())
	op.omap_rm_keys(to_remove);

      mdcache->mds->objecter->mutate(oid, oloc, op, snapc,
				   ceph::real_clock::now(g_ceph_context),
				   0, NULL, gather.new_sub());

      write_size = 0;
      to_set.clear();
      to_remove.clear();
    }
  }

  ObjectOperation op;
  op.priority = op_prio;

  // don't create new dirfrag blindly
  if (!is_new())
    op.stat(NULL, (ceph::real_time*)NULL, NULL);

  /*
   * save the header at the last moment.. If we were to send it off before other
   * updates, but die before sending them all, we'd think that the on-disk state
   * was fully committed even though it wasn't! However, since the messages are
   * strictly ordered between the MDS and the OSD, and since messages to a given
   * PG are strictly ordered, if we simply send the message containing the header
   * off last, we cannot get our header into an incorrect state.
   */
  bufferlist header;
  ::encode(fnode, header);
  op.omap_set_header(header);

  if (!to_set.empty())
    op.omap_set(to_set);
  if (!to_remove.empty())
    op.omap_rm_keys(to_remove);

  mdcache->mds->objecter->mutate(oid, oloc, op, snapc,
		  		 ceph::real_clock::now(g_ceph_context),
				 0, NULL, gather.new_sub());

  gather.activate();
}

void CDir::_encode_dentry(CDentry *dn, bufferlist& bl)
{
  // clear dentry NEW flag, if any.  we can no longer silently drop it.
  dn->clear_new();

  ::encode(dn->first, bl);

  // primary or remote?
  if (dn->get_linkage()->is_remote()) {
    inodeno_t ino = dn->get_linkage()->get_remote_ino();
    unsigned char d_type = dn->get_linkage()->get_remote_d_type();
    dout(14) << " pos " << bl.length() << " dn '" << dn->get_name() << "' remote ino " << ino << dendl;
    
    // marker, name, ino
    bl.append('L');         // remote link
    ::encode(ino, bl);
    ::encode(d_type, bl);
  } else if (dn->get_linkage()->is_primary()) {
    // primary link
    CInode *in = dn->get_linkage()->get_inode();
    assert(in);
    
    dout(14) << " pos " << bl.length() << " dn '" << dn->get_name() << "' inode " << *in << dendl;
    
    // marker, name, inode, [symlink string]
    bl.append('I');         // inode

    /*
    if (in->is_multiversion()) {
      if (!in->snaprealm) {
	if (snaps)
	  in->purge_stale_snap_data(*snaps);
      } else if (in->snaprealm->have_past_parents_open()) {
	in->purge_stale_snap_data(in->snaprealm->get_snaps());
      }
    }
    */

    in->encode_bare(bl, mdcache->mds->mdsmap->get_up_features());
  } else {
    assert(0);
  }
}


/**
 * _committed
 *
 * @param v version i just committed
 */
void CDir::_committed(int r, version_t v)
{
  if (r < 0) {
#if 0
    // the directory could be partly purged during MDS failover
    if (r == -ENOENT && committed_version == 0 &&
	inode->inode.nlink == 0 && inode->snaprealm) {
      inode->state_set(CInode::STATE_MISSINGOBJS);
      r = 0;
    }
#endif
    if (r < 0) {
      dout(1) << "commit error " << r << " v " << v << dendl;
      mdcache->mds->clog->error() << "failed to commit dir " << dirfrag() << " object,"
	      			  << " errno " << r << "\n";
      mdcache->mds->handle_write_error(r);
      return;
    }
  }

  inode->mutex_lock();

  dout(10) << "_committed v " << v << " on " << *this << dendl;

  bool stray = inode->is_stray();

  // take note.
  assert(v > committed_version);
  assert(v <= committing_version);
  committed_version = v;

  // _all_ commits done?
  if (committing_version == committed_version) 
    state_clear(CDir::STATE_COMMITTING);
  
  // dir clean?
  if (committed_version == get_version()) {
    mark_clean();
  } else {
    // _any_ commit, even if we've been redirtied, means we're no longer new.
    clear_new();
  }

  // dentries clean?
  for (auto p = dirty_dentries.begin(); !p.end(); ) {
    CDentry *dn = *p;
    ++p;
    
    assert(dn->is_dirty());
    // inode?
    if (dn->get_linkage()->is_primary()) {
      CInodeRef in = dn->get_linkage()->get_inode();
      assert(in);

      in->mutex_lock();
      assert(in->is_dirty());
      if (committed_version >= in->get_version()) {
	dout(15) << " dir " << committed_version << " >= inode " << in->get_version() << " now clean " << *in << dendl;
	in->mark_clean();
      }
      in->mutex_unlock();
    }

    // dentry
    if (committed_version >= dn->get_version()) {
      dout(15) << " dir " << committed_version << " >= dn " << dn->get_version() << " now clean " << *dn << dendl;
      dn->mark_clean();

      // drop clean null stray dentries immediately
      if (stray && 
	  dn->get_num_ref() == 0 &&
	  !dn->is_projected() &&
	  dn->get_linkage()->is_null())
	remove_dentry(dn);
    } 
  }

  // finishers?

  std::list<MDSContextBase*> finished;
  bool were_waiting = !waiting_for_commit.empty();
  for (auto p = waiting_for_commit.begin(); p != waiting_for_commit.end(); ) {
    auto n = p;
    ++n;
    if (p->first > committed_version) {
      if (!state_test(STATE_COMMITTING)) {
	dout(10) << " there are waiters for " << p->first << ", committing again" << dendl;
	commit(NULL, -1);
      }
      break;
    }

    finished.splice(finished.begin(), p->second);
    waiting_for_commit.erase(p);
    p = n;
  } 

#if 0
  // try drop dentries in this dirfrag if it's about to be purged
  if (inode->inode.nlink == 0 && inode->snaprealm)
    cache->maybe_eval_stray(inode, true);
#endif

  // unpin if we kicked the last waiter.
  if (were_waiting && waiting_for_commit.empty())
    put(PIN_COMMITTING);

  inode->mutex_unlock();

  finish_contexts(g_ceph_context, finished, 0);
}


void CDir::fetch(MDSContextBase *c)
{
  inode->mutex_assert_locked_by_me();
  dout(10) << "fetch " << " on " << *this << dendl;

  assert(!is_complete());

  add_waiter(WAIT_COMPLETE, c);

  if (state_test(CDir::STATE_FETCHING)) {
    dout(7) << "already fetching; waiting" << dendl;
    return;
  }

  state_set(CDir::STATE_FETCHING);
  get(PIN_FETCHING);

  // if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_fetch);

  std::set<dentry_key_t> empty;
  _omap_fetch(NULL, empty);
}

class C_Dir_Fetched : public CDirContext {
  MDSContextBase *fin;
public:
  bufferlist hdrbl;
  map<string, bufferlist> omap;
  int ret1, ret2;

  C_Dir_Fetched(CDir *d, MDSContextBase *f) :
    CDirContext(d), fin(f), ret1(0), ret2(0) { }
  void finish(int r) {
    if (r >= 0) r = ret1;
    if (r >= 0) r = ret2;
    dir->_omap_fetched(r, hdrbl, omap, !fin);
    if (fin)
      fin->complete(r);
  }
};

void CDir::_omap_fetch(MDSContextBase *c, const std::set<dentry_key_t>& keys)
{
  C_Dir_Fetched *fin = new C_Dir_Fetched(this, c);
  object_t oid = get_ondisk_object();
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());
  ObjectOperation rd;
  rd.omap_get_header(&fin->hdrbl, &fin->ret1);
  if (keys.empty()) {
    assert(!c);
    rd.omap_get_vals("", "", (uint64_t)-1, &fin->omap, &fin->ret2);
  } else {
    assert(c);
    std::set<std::string> str_keys;
    for (auto p = keys.begin(); p != keys.end(); ++p) {
      string str;
      p->encode(str);
      str_keys.insert(str);
    }
    rd.omap_get_vals_by_keys(str_keys, &fin->omap, &fin->ret2);
  }
#if 0
  // check the correctness of backtrace
  if (g_conf->mds_verify_backtrace > 0 && frag == frag_t()) {
    rd.getxattr("parent", &fin->btbl, &fin->ret3);
    rd.set_last_op_flags(CEPH_OSD_OP_FLAG_FAILOK);
  } else {
    fin->ret3 = -ECANCELED;
  }
#endif
  mdcache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, NULL, 0, fin);
}


CDentryRef CDir::_load_dentry(const std::string &dname, const snapid_t last,
			      bufferlist &bl, const int pos)
{
  bufferlist::iterator q = bl.begin();

  snapid_t first;
  ::decode(first, q);

  // marker
  char type;
  ::decode(type, q);

  dout(20) << "_fetched pos " << pos << " marker '" << type << "' dname '" << dname
           << " [" << first << "," << last << "]"
           << dendl;

  CDentryRef dn = lookup(dname);
  if (type == 'L') {
    // hard link
    inodeno_t ino;
    unsigned char d_type;
    ::decode(ino, q);
    ::decode(d_type, q);

    if (dn) {
      if (dn->get_linkage()->get_inode() == 0) {
        dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
      } else {
        dout(12) << "_fetched  had dentry " << *dn << dendl;
      }
    } else {
      // (remote) link
      dn = add_remote_dentry(dname, ino, d_type);

#if 0
      // link to inode?
      CInode *in = cache->get_inode(ino);   // we may or may not have it.
      if (in) {
        dn->link_remote(dn->get_linkage(), in);
        dout(12) << "_fetched  got remote link " << ino << " which we have " << *in << dendl;
      } else {
        dout(12) << "_fetched  got remote link " << ino << " (dont' have it)" << dendl;
      }
#endif
    }
  }
  else if (type == 'I') {
    if (dn) {
      if (dn->get_linkage()->is_null()) {
        dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
      } else {
        dout(12) << "_fetched  had dentry " << *dn << dendl;
      }
    } else {
      // add inode
      CInode* newi = new CInode(mdcache);

      try {
	newi->decode_bare(q);
      } catch (buffer::error &err) {
	delete newi;
	throw err;
      }

      CInodeRef in = mdcache->get_inode(newi->ino(), last);
      if (!in) {
	in = newi;
	in->mutex_lock();

	dn = add_primary_dentry(dname, in.get()); // link

	if (in->get_inode()->is_dirty_rstat())
	  in->mark_dirty_rstat();

	mdcache->add_inode(in.get()); // add

	in->mutex_unlock();

	/*
	if (inode->is_stray()) {
	  cache->notify_stray_loaded(dn);
	}
	*/
      } else {
	LogChannelRef clog = mdcache->mds->clog;
	string dirpath, inopath;
	this->make_string(dirpath);
	in->mutex_lock();
	in->make_string(inopath);
	in->mutex_unlock();
	dout(0) << "_fetched  badness: got (but i already had) " << *in << dendl;
	clog->error() << "loaded dup inode " << newi->vino()
		      << " v" << newi->get_inode()->version << " at " << dirpath << "/" << dname
		      << ", but inode " << in->vino() << " v" << in->get_inode()->version
		      << " already exists at " << inopath << "\n";
	delete newi;
	return dn;
      }
    }
  } else {
    std::ostringstream oss;
    oss << "Invalid tag char '" << type << "' pos " << pos;
    throw buffer::malformed_input(oss.str());
  }

  return dn;
}

void CDir::_omap_fetched(int r, bufferlist& hdrbl, map<string, bufferlist>& omap,
			 bool complete)
{
  LogChannelRef clog = mdcache->mds->clog;
  dout(10) << "_fetched header " << hdrbl.length() << " bytes "
           << omap.size() << " keys for " << *this << dendl;

  assert(r == 0 || r == -ENOENT || r == -ENODATA);

  if (hdrbl.length() == 0) {
    dout(0) << "_fetched missing object for " << *this << dendl;
    clog->error() << "dir " << dirfrag() << " object missing on disk; some files may be lost\n";

    go_bad(complete);
    return;
  }

  fnode_t got_fnode;
  {
    bufferlist::iterator p = hdrbl.begin();
    try {
      ::decode(got_fnode, p);
    } catch (const buffer::error &err) {
      derr << "Corrupt fnode in dirfrag " << dirfrag() << ": " << err << dendl;
      clog->warn() << "Corrupt fnode header in " << dirfrag() << ": " << err;
      go_bad(complete);
      return;
    }
    if (!p.end()) {
      clog->warn() << "header buffer of dir " << dirfrag() << " has "
		   << hdrbl.length() - p.get_off() << " extra bytes\n";
      go_bad(complete);
      return;
    }
  }
  dout(10) << "_fetched version " << got_fnode.version << dendl;

  inode->mutex_lock();

  // take the loaded fnode?
  // only if we are a fresh CDir* with no prior state.
  if (get_version() == 0) {
    assert(!is_projected());
    assert(!state_test(STATE_COMMITTING));
    fnode = got_fnode;
    projected_version = committing_version = committed_version = got_fnode.version;
  }

  unsigned pos = omap.size() - 1;
  for (auto p = omap.rbegin(); p != omap.rend(); ++p, --pos) {
    string dname;
    snapid_t last;
    dentry_key_t::decode_helper(p->first, dname, last);

    CDentryRef dn;
    try {
      dn = _load_dentry(dname, last, p->second, pos);
    } catch (const buffer::error &err) {
      clog->warn() << "Corrupt dentry '" << dname << "' in dir frag "
		   << dirfrag() << ": " << err;

      // Remember that this dentry is damaged.  Subsequent operations
      // that try to act directly on it will get their EIOs, but this
      // dirfrag as a whole will continue to look okay (minus the
      // mysteriously-missing dentry)
      go_bad_dentry(last, dname);

      // Anyone who was WAIT_DENTRY for this guy will get kicked
      // to RetryRequest, and hit the DamageTable-interrogating path.
      // Stats will now be bogus because we will think we're complete,
      // but have 1 or more missing dentries.
      continue;
    }

#if 0
    if (dn && (wanted_items.count(dname) > 0 || !complete)) {
      dout(10) << " touching wanted dn " << *dn << dendl;
      inode->mdcache->touch_dentry(dn);
    }
#endif

    /** clean underwater item?
     * Underwater item is something that is dirty in our cache from
     * journal replay, but was previously flushed to disk before the
     * mds failed.
     *
     * We only do this is committed_version == 0. that implies either
     * - this is a fetch after from a clean/empty CDir is created
     *   (and has no effect, since the dn won't exist); or
     * - this is a fetch after _recovery_, which is what we're worried 
     *   about.  Items that are marked dirty from the journal should be
     *   marked clean if they appear on disk.
     */
    if (committed_version == 0 &&
	dn && dn->is_dirty() &&
	dn->get_version() <= got_fnode.version) {
      dout(10) << "_fetched  had underwater dentry " << *dn << ", marking clean" << dendl;
      dn->mark_clean();

      if (dn->get_linkage()->is_primary()) {
	CInodeRef in = dn->get_linkage()->get_inode();
	in->mutex_lock();
        assert(in->get_version() <= got_fnode.version);
        dout(10) << "_fetched  had underwater inode " << *in << ", marking clean" << dendl;
        in->mark_clean();
	in->mutex_unlock();
      }
    }
  }

  //cache->mds->logger->inc("newin", num_new_inodes_loaded);

  std::list<MDSContextBase*> finished;
  // mark complete, !fetching
  if (complete) {
    assert(state_test(STATE_FETCHING));
    state_clear(STATE_FETCHING);
    mark_complete();
    take_waiting(WAIT_COMPLETE, finished);
    put(PIN_FETCHING);
  }

  inode->mutex_unlock();

  finish_contexts(g_ceph_context, finished, 0);
}

void CDir::mark_complete() {
  state_set(STATE_COMPLETE);
  remove_bloom();
}

void CDir::add_to_bloom(CDentry *dn)
{
  assert(dn->last == CEPH_NOSNAP);
  if (!bloom) {
    /* not create bloom filter for incomplete dir that was added by log replay */
    if (!is_complete())
      return;
    unsigned size = num_head_items;
    if (size < 100)
      size = 100;
    bloom = new bloom_filter(size, 1.0 / size, 0);
  }
  /* This size and false positive probability is completely random.*/
  bloom->insert(dn->get_name().c_str(), dn->get_name().size());
}

bool CDir::is_in_bloom(const string& name)
{
  if (!bloom)
    return false;
  return bloom->contains(name.c_str(), name.size());
}

void CDir::remove_bloom()
{
  delete bloom;
  bloom = NULL;
}

void CDir::first_get()
{
  inode->get(CInode::PIN_DIRFRAG);
}

void CDir::last_put()
{
  inode->put(CInode::PIN_DIRFRAG);
}

void intrusive_ptr_add_ref(CDir *o)
{
  o->get(CObject::PIN_INTRUSIVEPTR);
}
void intrusive_ptr_release(CDir *o)
{
  o->put(CObject::PIN_INTRUSIVEPTR);
}
