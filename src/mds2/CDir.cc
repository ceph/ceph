#include "MDCache.h"
#include "Locker.h"
#include "CDir.h"
#include "CInode.h"
#include "CDentry.h"

#include "messages/MClientRequest.h"
#undef dout_prefix
#define dout_prefix *_dout << "dir(" << inode->ino() << ") "

#define dout_subsys ceph_subsys_mds

ostream& operator<<(ostream& out, const CDir& dir)
{
  return out;
}

CDir::CDir(CInode *i) :
  CObject("CDir"), mdcache(i->mdcache), inode(i),
  dirty_rstat_inodes(member_offset(CInode, dirty_rstat_item))
{
}

void CDir::first_get()
{
  inode->get(CInode::PIN_DIRFRAG);
}

void CDir::last_put()
{
  inode->put(CInode::PIN_DIRFRAG);
}

dirfrag_t CDir::dirfrag() const
{
  return dirfrag_t(get_inode()->ino(), get_frag());
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
  inode->mutex_assert_locked_by_me();
  assert(get_version() < pv);
  assert(pv <= projected_version);
  fnode.version = pv;
  _mark_dirty(ls);
}

void CDir::_mark_dirty(LogSegment *ls)
{
  if (!state_test(STATE_DIRTY)) {
    dout(10) << "mark_dirty (was clean) " << *this << " version " << get_version() << dendl;
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
 //   assert(ls);
  } else {
    dout(10) << "mark_dirty (already dirty) " << *this << " version " << get_version() << dendl;
  }
  /*
  if (ls) {
    ls->dirty_dirfrags.push_back(&item_dirty);
    if (committed_version == 0 && !item_new.is_on_list())
      ls->new_dirfrags.push_back(&item_new);
  }
  */
}

void CDir::mark_clean()
{
  dout(10) << "mark_clean " << *this << " version " << get_version() << dendl;
  if (state_test(STATE_DIRTY)) {
//    item_dirty.remove_myself();
//    item_new.remove_myself();

    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);
  }
}

void CDir::link_remote_inode(CDentry *dn, inodeno_t ino, uint8_t d_type)
{
//  dout(12) << "link_remote_inode " << *dn << " remote " << ino << dendl;
  dn->link_inode_work(ino, d_type);
}

void CDir::link_primary_inode(CDentry *dn, CInode *in)
{ 
//  dout(12) << "link_primary_inode " << *dn << " " << *in << dendl;
  dn->link_inode_work(in);
}

void CDir::unlink_inode(CDentry *dn)
{
//  dout(12) << "unlink_inode " << *dn << dendl;
  dn->unlink_inode_work();
}

CDentryRef CDir::add_null_dentry(const string& dname, snapid_t first, snapid_t last)
{
  inode->mutex_assert_locked_by_me();
  CDentryRef dn = new CDentry(this, dname, first, last);

  if (items.empty())
    get(PIN_CHILD);
  else
    assert(items.count(dn->get_key()) == 0);
  items[dn->get_key()] = dn.get();

  return dn;
}

CDentryRef CDir::add_primary_dentry(const string& dname, CInode *in,
				    snapid_t first, snapid_t last)
{
  inode->mutex_assert_locked_by_me();
  CDentryRef dn = new CDentry(this, dname, first, last);

  if (items.empty()) 
    get(PIN_CHILD);
  else
    assert(items.count(dn->get_key()) == 0);
  items[dn->get_key()] = dn.get();

  in->mutex_lock();
  dn->link_inode_work(in);
  in->mutex_unlock();
  return dn;
}

CDentryRef CDir::add_remote_dentry(const string& dname, inodeno_t ino, uint8_t d_type,
				   snapid_t first, snapid_t last)
{
  inode->mutex_assert_locked_by_me();
  CDentryRef dn = new CDentry(this, dname, first, last);

  if (items.empty()) 
    get(PIN_CHILD);
  else
    assert(items.count(dn->get_key()) == 0);
  items[dn->get_key()] = dn.get();

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

  if (items.empty())
    put(PIN_CHILD);
}

CDentry* CDir::__lookup(const char *name, snapid_t snap)
{
  dout(20) << "lookup (" << snap << ", '" << name << "')" << dendl;
  auto it = items.lower_bound(dentry_key_t(snap, name));
  if (it == items.end())
    return 0;
  if (it->second->get_name() == name &&
      it->second->get_first() <= snap) {
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
  in->get(CInode::PIN_DIRTYRSTAT);
  dirty_rstat_inodes.push_back(&in->dirty_rstat_item);
  mdcache->locker->mark_updated_scatterlock(&inode->nestlock);
}

void CDir::remove_dirty_rstat_inode(CInode *in)
{
  inode->mutex_assert_locked_by_me();
  in->dirty_rstat_item.remove_myself();
  in->put(CInode::PIN_DIRTYRSTAT);
}

void CDir::assimilate_dirty_rstat_inodes(MutationRef& mut)
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

void CDir::assimilate_dirty_rstat_inodes_finish(MutationRef& mut, EMetaBlob *blob)
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
    //blob->add_primary_dentry(in->get_projected_parent_dn(), in, true);
    in->mutex_unlock();
  }

  /*
  if (!dirty_rstat_inodes.empty())
    mdcache->locker->mark_updated_scatterlock(&inode->nestlock);
  */
}
