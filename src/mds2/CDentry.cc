#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "SessionMap.h"
#include "LogSegment.h"
#include "DentryLease.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << get_dir_inode()->mdcache->mds->get_nodeid() << ".cache.dentry(" << dir->dirfrag() << " " << name << ") "

snapid_t CDentry::first(2);
snapid_t CDentry::last(CEPH_NOSNAP);

LockType CDentry::lock_type(CEPH_LOCK_DN);
LockType CDentry::versionlock_type(CEPH_LOCK_DVERSION);

ostream& operator<<(ostream& out, const CDentry& dn)
{
  bool locked = dn.mutex_is_locked_by_me();
  bool need_unlock = false;
  if (!locked && dn.mutex_trylock()) {
    locked = true;
    need_unlock = true;
  }
  bool dir_locked = dn.get_dir_inode()->mutex_is_locked_by_me();
  bool need_unlock_dir = false;
  if (!dir_locked && dn.get_dir_inode()->mutex_trylock()) {
    dir_locked = true;
    need_unlock_dir = true;
  }

  string path;
  dn.make_string(path);
  out << "[dentry " << path;
  out << " state=" << hex << dn.get_state() << dec;

  if (dir_locked) {
    const CDentry::linkage_t *dnl = dn.get_linkage();
    if (dnl->is_null()) out << " NULL";
    if (dnl->is_remote()) out << " REMOTE(" << dnl->get_remote_ino() << ")";
    out << " inode=" << dnl->get_inode();

  } else {
    out << " (dir unlocked)...";
  }

  if (locked) {
    if (!dn.lock.is_sync_and_unlocked())
      out << " " << dn.lock;
    if (!dn.versionlock.is_sync_and_unlocked())
      out << " " << dn.versionlock;
  } else {
    out << " (unlocked)...";
  }

  out << " ref=" << dn.get_num_ref();
  out << " " << &dn;
  out << "]";

  if (need_unlock_dir)
    dn.get_dir_inode()->mutex_unlock();
  if (need_unlock)
    dn.mutex_unlock();
  return out;
}

void CDentry::print(ostream& out)
{
  out << *this;
}

CDentry::CDentry(CDir *d, const std::string &n, __u32 h) :
  CObject("CDentry"), name(n), hash(h), dir(d),
  lock(this, &lock_type),
  versionlock(this, &versionlock_type)
{
}

CInode* CDentry::get_dir_inode() const
{
  return get_dir()->get_inode();
}

void CDentry::make_string(std::string& s) const
{
  get_dir()->make_string(s);
  s += "/";
  s += name;
}

bool CDentry::is_lt(const CObject *r) const
{
  const CDentry *o = static_cast<const CDentry*>(r);
  if ((get_dir_inode()->ino() < o->get_dir_inode()->ino()) ||
      (get_dir_inode()->ino() == o->get_dir_inode()->ino() &&
       get_key() < o->get_key()))
    return true;
  return false;
}

const CDentry::linkage_t* CDentry::get_linkage(client_t client,
					       const MutationRef& mut) const
{
  Mutex::Locker l(mutex);
  if (lock.can_read_projected(client) || lock.get_xlock_by() == mut)
    return get_projected_linkage();
  return get_linkage();
}

CDentry::linkage_t* CDentry::_project_linkage()
{
  // dirfrag must be locked
  get_dir_inode()->mutex_assert_locked_by_me();
  projected_linkages.push_back(linkage_t());
  return &projected_linkages.back();
}

void CDentry::push_projected_linkage(CInode *in)
{
  // dirty rstat tracking is in the projected plane
  bool dirty_rstat = in->is_dirty_rstat();
  if (dirty_rstat)
    in->clear_dirty_rstat();

  _project_linkage()->set_inode(in);
  in->push_projected_parent(this);

  if (dirty_rstat)
    in->mark_dirty_rstat();
}

void CDentry::pop_projected_linkage()
{
  get_dir_inode()->mutex_assert_locked_by_me();
  assert(!projected_linkages.empty());
  linkage_t& n = projected_linkages.front();

  if (n.get_remote_ino()) {
    dir->link_remote_inode(this, n.get_remote_ino(), n.get_remote_d_type());
    if (n.get_inode()) {
//      linkage.inode = n.inode;
//      linkage.inode->add_remote_parent(this);
    }
  } else if (n.get_inode()) {
    CDentry* dn = n.get_inode()->pop_projected_parent();
    assert(dn == this);
    dir->link_primary_inode(this, n.get_inode());
  }

  projected_linkages.pop_front();
}

void CDentry::link_inode_work(inodeno_t ino, uint8_t d_type)
{
  get_dir_inode()->mutex_assert_locked_by_me();
  assert(linkage.is_null());
  linkage.set_remote(ino, d_type);
}

void CDentry::link_inode_work(CInode *in)
{
  get_dir_inode()->mutex_assert_locked_by_me();
  in->mutex_assert_locked_by_me();

  assert(linkage.is_null());
  linkage.set_inode(in); 
  in->set_primary_parent(this);

  if (in->get_num_ref())
    get(CDentry::PIN_INODEPIN);
}

void CDentry::unlink_inode_work()
{
  get_dir_inode()->mutex_assert_locked_by_me();
  assert(!linkage.is_null());

  CInode *in = linkage.get_inode();
  if (linkage.is_remote()) {
    // remote
    linkage.set_remote(0, 0);
    if (in) {
      in->remove_remote_parent(this);
      linkage.set_inode(NULL);
    }
    state_clear(STATE_BADREMOTEINO);
  } else {
    // primary
    assert(linkage.is_primary());
    in->mutex_assert_locked_by_me();

    // unpin dentry?
    if (in->get_num_ref())
      put(CDentry::PIN_INODEPIN);

    // detach inode
    in->remove_primary_parent(this);
    linkage.set_inode(NULL);
  }
}

void CDentry::link_remote(const CDentry::linkage_t *dnl, CInodeRef& in)
{
  assert(dnl->is_remote());
  assert(in->ino() == dnl->get_remote_ino());

  if (dnl == &linkage) {
    in->mutex_lock();
    in->add_remote_parent(this);
    linkage.set_inode(in.get());
    in->mutex_unlock();
  }
}

version_t CDentry::pre_dirty(version_t min)
{ 
  projected_version = dir->pre_dirty(min);
  dout(10) << " pre_dirty " << *this << dendl;
  return projected_version;
}

void CDentry::_mark_dirty(LogSegment *ls)
{ 
  // state+pin
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
    dir->add_dirty_dentry(this);
    assert(ls);
  }
  if (ls) {
    dir->mdcache->lock_log_segments();
    ls->dirty_dentries.push_back(&item_dirty);
    dir->mdcache->unlock_log_segments();
  }
}

void CDentry::mark_dirty(version_t pv, LogSegment *ls)
{ 
  dout(10) << " mark_dirty " << *this << dendl;

  // i now live in this new dir version
  assert(pv <= projected_version);
  version = pv;
  _mark_dirty(ls);

  // mark dir too
  dir->mark_dirty(pv, ls);
}

void CDentry::mark_clean()
{
  dout(10) << " mark_clean " << *this << dendl;
  assert(is_dirty());

  dir->mdcache->lock_log_segments();
  item_dirty.remove_myself();
  dir->mdcache->unlock_log_segments();

  dir->remove_dirty_dentry(this);

  // state+pin
  state_clear(STATE_DIRTY | STATE_NEW);
  put(PIN_DIRTY);
}

void CDentry::mark_new()
{
  dout(10) << " mark_new " << *this << dendl;
  state_set(STATE_NEW);
}

void CDentry::clear_new()
{
  dout(10) << " clear_new " << *this << dendl;
  state_clear(STATE_NEW);
}

DentryLease *CDentry::add_client_lease(Session *session, utime_t ttl)
{
  mutex_assert_locked_by_me();
  client_t client = session->get_client();
  DentryLease *l;

  bool is_new = false;
  auto p = client_leases.find(client);
  if (p != client_leases.end()) {
    l = p->second;
  } else {
    dout(20) << "add_client_lease client." << client << " on " << lock << dendl;
    if (client_leases.empty())
      get(PIN_CLIENTLEASE);
    l = new DentryLease(this, session);
    lock.get_client_lease();
    client_leases[client] = l;
    is_new = true;
  }
  session->mutex_lock();
  if (is_new)
    l->seq = ++session->lease_seq;
  l->ttl = ttl;
  session->touch_lease(l);
  session->mutex_unlock();
  return l;
}

void CDentry::remove_client_lease(Session *session)
{
  mutex_assert_locked_by_me();
  client_t client = session->get_client();
  dout(20) << "remove_client_lease client." << client << " on " << lock << dendl;

  auto p = client_leases.find(client);
  assert(p != client_leases.end());
  DentryLease *l = p->second;

  lock.put_client_lease();

  session->mutex_lock();
  l->item_session_lease.remove_myself();
  session->mutex_unlock();

  client_leases.erase(p);
  delete l;

  if (client_leases.empty())
    put(PIN_CLIENTLEASE);
}

void CDentry::first_get()
{
  lru_pin();
}
void CDentry::last_put()
{
  lru_unpin();
  CInode *diri = get_dir_inode();
  if (get_linkage()->is_null() &&
      (diri->is_stray() || diri->state_test(CInode::STATE_ORPHAN))) {
    diri->mdcache->touch_dentry_bottom(this);
  } else if (diri->is_stray()) {
    diri->mdcache->eval_stray(this);
  }
}

void intrusive_ptr_add_ref(CDentry *o)
{
  o->get(CObject::PIN_INTRUSIVEPTR);
}
void intrusive_ptr_release(CDentry *o)
{
  o->put(CObject::PIN_INTRUSIVEPTR);
}
