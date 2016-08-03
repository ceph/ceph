#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDSDaemon.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "ino(" << inode.ino << ") "

ostream& operator<<(ostream& out, const CInode& in)
{
  return out;
}

void CInode::first_get()
{
  Mutex::Locker l(mutex);
  if (parent)
    parent->get(CDentry::PIN_INODEPIN);
}
void CInode::last_put()
{
  Mutex::Locker l(mutex);
  if (parent)
    parent->put(CDentry::PIN_INODEPIN);
}

CDirRef CInode::get_or_open_dirfrag(frag_t fg)
{
  CDirRef ref = get_dirfrag(fg);
  if (!ref) {
    assert(fg == frag_t());
    dir = new CDir(this);
    dir->__set_version(1);
    ref = dir;
  }
  return ref;
}

inode_t *CInode::project_inode(std::map<string, bufferptr> **ppx)
{
  mutex_assert_locked_by_me();
  const xattr_map_t *px = NULL;
  if (projected_nodes.empty()) {
    if (ppx)
      px = get_xattrs();
    projected_nodes.push_back(projected_inode_t(inode, px));
    if (ppx)
      *ppx = &projected_nodes.back().xattrs;
  } else {
    const inode_t *pi = get_projected_inode();
    if (ppx)
      px = get_projected_xattrs();
    projected_nodes.push_back(projected_inode_t(*pi, px));
    if (ppx)
      *ppx = &projected_nodes.back().xattrs;
  }
  dout(10) << "project_inode " << &projected_nodes.back() << dendl;
  return &projected_nodes.back().inode;
}

void CInode::pop_and_dirty_projected_inode(LogSegment *ls)
{
  mutex_assert_locked_by_me();
  assert(!projected_nodes.empty());

  projected_inode_t *pi = &projected_nodes.front();
  dout(10) << "pop_and_dirty_projected_inode " << pi << dendl;

  mark_dirty(pi->inode.version, ls);
  inode = pi->inode;

  if (pi->xattrs_projected)
    xattrs.swap(pi->xattrs);

  projected_nodes.pop_front();
}

version_t CInode::pre_dirty()
{
  mutex_assert_locked_by_me();
  version_t pv;
  CDentry* parent_dn = get_projected_parent_dn();
  if (parent_dn) {
    pv = parent_dn->pre_dirty(get_projected_version());
    dout(10) << "pre_dirty " << pv << " (current v " << inode.version << ")" << dendl;
  } else {
    assert(is_base());
    pv = get_projected_version() + 1;
  }
  return pv;
}

void CInode::_mark_dirty(LogSegment *ls)
{
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
//    assert(ls);
  }

  /*
  if (ls)
    ls->dirty_inodes.push_back(&item_dirty);
    */
}

void CInode::mark_dirty(version_t pv, LogSegment *ls) {
  mutex_assert_locked_by_me();
  dout(10) << "mark_dirty " << *this << dendl;

  // touch my private version
  assert(inode.version < pv);
  inode.version = pv;
  _mark_dirty(ls);

  // mark dentry too
  if (parent)
    parent->mark_dirty(pv, ls);
}


void CInode::mark_clean()
{
  dout(10) << " mark_clean " << *this << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);

    // remove myself from ls dirty list
//    item_dirty.remove_myself();
  }
}

CDentryRef CInode::get_lock_parent_dn()
{
  for (;;) {
    CDentryRef dn;
    mutex_lock();
    dn = get_parent_dn();
    bool done = dn->get_dir_inode()->mutex_trylock();
    mutex_unlock();
    if (done)
      return dn;

    dn->get_dir_inode()->mutex_lock();
    const CDentry::linkage_t* dnl = dn->get_linkage();
    if (!dnl->is_primary() || dnl->get_inode() != this) {
      dn->get_dir_inode()->mutex_unlock();
      continue;
    }
    return dn;
  }
}

CDentryRef CInode::get_lock_projected_parent_dn()
{
  for (;;) {
    CDentryRef dn;
    mutex_lock();
    dn = get_projected_parent_dn();
    bool done = dn->get_dir_inode()->mutex_trylock();
    mutex_unlock();
    if (done)
      return dn;

    dn->get_dir_inode()->mutex_lock();
    const CDentry::linkage_t* dnl = dn->get_projected_linkage();
    if (!dnl->is_primary() || dnl->get_inode() != this) {
      dn->get_dir_inode()->mutex_unlock();
      continue;
    }
    return dn;
  }
}

bool CInode::is_projected_ancestor_of(CInode *other) const
{
  // caller should hold MDCache::rename_dir_mutex
  while (other) {
    if (other == this)
      return true;
    other->mutex_lock();
    CDentry *parent_dn = other->get_projected_parent_dn();
    other->mutex_unlock();
    if (!parent_dn)
      break;
    other = parent_dn->get_dir_inode();
  }
  return false;
}

int CInode::encode_inodestat(bufferlist& bl, Session *session,
			     unsigned max_bytes)
{
  mutex_assert_locked_by_me();
  assert(session->connection);
  
  snapid_t snapid = CEPH_NOSNAP;
  // pick a version!
  const inode_t *pi = get_projected_inode();

  // "fake" a version that is old (stable) version, +1 if projected.
  version_t version = (get_inode()->version * 2) + is_projected();

  file_layout_t layout = pi->layout;
  // max_size is min of projected, actual
  uint64_t max_size = 0;

  // xattr
  bufferlist xbl;
  version_t xattr_version;
  {
    ::encode(*get_projected_xattrs(), xbl);
    xattr_version = pi->xattr_version;
  }

  // inline data
  version_t inline_version = CEPH_INLINE_NONE;
  bufferlist inline_data;

  if (max_bytes) {
    // FIXME
  }

  // encode caps
  struct ceph_mds_reply_cap ecap;
  ecap.cap_id = 0;
  ecap.caps = 0;
  ecap.seq = 0;
  ecap.mseq = 0;
  ecap.realm = 0;
  ecap.wanted = 0;
  ecap.flags = CEPH_CAP_FLAG_AUTH;

  /*
   * note: encoding matches MClientReply::InodeStat
   */
  ::encode(pi->ino, bl);
  ::encode(snapid, bl);
  ::encode(pi->rdev, bl);
  ::encode(version, bl);
  ::encode(xattr_version, bl);

  ::encode(ecap, bl);
  {
    ceph_file_layout legacy_layout;
    layout.to_legacy(&legacy_layout);
    ::encode(legacy_layout, bl);
  }
  ::encode(pi->ctime, bl);
  ::encode(pi->mtime, bl);
  ::encode(pi->atime, bl);
  ::encode(pi->time_warp_seq, bl);
  ::encode(pi->size, bl);
  ::encode(max_size, bl);
  ::encode(pi->truncate_size, bl);
  ::encode(pi->truncate_seq, bl);

  ::encode(pi->mode, bl);
  ::encode((uint32_t)pi->uid, bl);
  ::encode((uint32_t)pi->gid, bl);

  ::encode(pi->nlink, bl);

  ::encode(pi->dirstat.nfiles, bl);
  ::encode(pi->dirstat.nsubdirs, bl);
  ::encode(pi->rstat.rbytes, bl);
  ::encode(pi->rstat.rfiles, bl);
  ::encode(pi->rstat.rsubdirs, bl);
  ::encode(pi->rstat.rctime, bl);

  {
    fragtree_t dirfragtree;
    dirfragtree.encode(bl);
  }

  ::encode(symlink, bl);

  if (session->connection->has_feature(CEPH_FEATURE_DIRLAYOUTHASH)) {
    ::encode(pi->dir_layout, bl);
  }

  ::encode(xbl, bl);

  if (session->connection->has_feature(CEPH_FEATURE_MDS_INLINE_DATA)) {
    ::encode(inline_version, bl);
    ::encode(inline_data, bl);
  }

  if (session->connection->has_feature(CEPH_FEATURE_MDS_QUOTA)) {
    ::encode(pi->quota, bl);
  }

  if (session->connection->has_feature(CEPH_FEATURE_FS_FILE_LAYOUT_V2)) {
    ::encode(layout.pool_ns, bl);
  }

  return 0;
}

void CInode::name_stray_dentry(string& dname) const
{
  char s[20];
  snprintf(s, sizeof(s), "%llx", (unsigned long long)inode.ino.val);
  dname = s;
}
