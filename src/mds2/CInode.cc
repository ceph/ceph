#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "Locker.h"
#include "Mutation.h"
#include "MDLog.h"
#include "SessionMap.h"

#include "osdc/Objecter.h"

#include "messages/MClientCaps.h"

#include "events/EUpdate.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.inode(" << vino() << ") "

class CInodeIOContext : public MDSAsyncContextBase
{
protected:
  CInodeRef in;
  MDSRank *get_mds() { return in->mdcache->mds; }
public:
  explicit CInodeIOContext(CInode *in_) : in(in_) {
    assert(in != NULL);
  }
};

class CInodeLogContext : public MDSLogContextBase
{
protected:
  CInodeRef in;
  MDSRank *get_mds() {return in->mdcache->mds;}
public:
  explicit CInodeLogContext(CInode *in_) : in(in_) {
    assert(in != NULL);
  }
};

// crap
snapid_t CInode::first(2);
snapid_t CInode::last(CEPH_NOSNAP);
snapid_t CInode::oldest_snap(CEPH_NOSNAP);
fragtree_t CInode::dirfragtree;
compact_map<snapid_t, old_inode_t> CInode::old_inodes;

LockType CInode::versionlock_type(CEPH_LOCK_IVERSION);
LockType CInode::authlock_type(CEPH_LOCK_IAUTH);
LockType CInode::linklock_type(CEPH_LOCK_ILINK);
LockType CInode::dirfragtreelock_type(CEPH_LOCK_IDFT);
LockType CInode::filelock_type(CEPH_LOCK_IFILE);
LockType CInode::xattrlock_type(CEPH_LOCK_IXATTR);
LockType CInode::snaplock_type(CEPH_LOCK_ISNAP);
LockType CInode::nestlock_type(CEPH_LOCK_INEST);
LockType CInode::flocklock_type(CEPH_LOCK_IFLOCK);
LockType CInode::policylock_type(CEPH_LOCK_IPOLICY);

ostream& operator<<(ostream& out, const CInode& in)
{
  bool locked = in.mutex_is_locked_by_me();
  bool need_unlock = false;
  if (!locked && in.mutex_trylock()) {
    locked = true;
    need_unlock = true;
  }

  out << "[inode " << in.ino();
  out << " v" << in.get_version();
  out << " state=" << hex << in.get_state() << dec;

  if (locked) {
    string path;
    in.make_string(path);
    out << " " << path;

    const inode_t *oi = in.get_inode();
    if (in.is_dir()) {
      out << " " << oi->dirstat;
    } else {
      out << " s=" << oi->size;
      if (oi->nlink != 1)
	out << " nl=" << oi->nlink;
    }

    // rstat
    out << " " << oi->rstat;
    if (!(oi->rstat == oi->accounted_rstat))
      out << "/" << oi->accounted_rstat;

    // locks
    if (!in.authlock.is_sync_and_unlocked())
      out << " " << in.authlock;
    if (!in.linklock.is_sync_and_unlocked())
      out << " " << in.linklock;
    if (in.is_dir()) {
      if (!in.dirfragtreelock.is_sync_and_unlocked())
	out << " " << in.dirfragtreelock;
      if (!in.snaplock.is_sync_and_unlocked())
	out << " " << in.snaplock;
      if (!in.nestlock.is_sync_and_unlocked())
	out << " " << in.nestlock;
      if (!in.policylock.is_sync_and_unlocked())
	out << " " << in.policylock;
    } else  {
      if (!in.flocklock.is_sync_and_unlocked())
	out << " " << in.flocklock;
    }
    if (!in.filelock.is_sync_and_unlocked())
      out << " " << in.filelock;
    if (!in.xattrlock.is_sync_and_unlocked())
      out << " " << in.xattrlock;
    if (!in.versionlock.is_sync_and_unlocked())
      out << " " << in.versionlock;

    // hack: spit out crap on which clients have caps
    if (oi->client_ranges.size())
      out << " cr=" << oi->client_ranges;

    if (!in.get_client_caps().empty()) {
      out << " caps={";
      for (auto it = in.get_client_caps().begin();
	  it != in.get_client_caps().end();
	  ++it) {
	if (it != in.get_client_caps().begin()) out << ",";
	out << it->first << "="
	  << ccap_string(it->second->pending());
	if (it->second->issued() != it->second->pending())
	  out << "/" << ccap_string(it->second->issued());
	out << "/" << ccap_string(it->second->wanted())
	  << "@" << it->second->get_last_sent();
      }
      out << "}";
      if (in.get_loner() >= 0 || in.get_wanted_loner() >= 0) {
	out << ",l=" << in.get_loner();
	if (in.get_loner() != in.get_wanted_loner())
	  out << "(" << in.get_wanted_loner() << ")";
      }
    }
  } else {
    out << " (unlocked) ...";   
  }

  out << " ref=" << in.get_num_ref();
  out << " " << &in;
  out << "]";

  if (need_unlock)
    in.mutex_unlock();
  return out;
}

CInode::CInode(MDCache *_mdcache) :
  CObject("CInode"), mdcache(_mdcache),
  latest_projected_xattrs(NULL),
  parent(NULL),
  versionlock(this, &versionlock_type),
  authlock(this, &authlock_type),
  linklock(this, &linklock_type),
  dirfragtreelock(this, &dirfragtreelock_type),
  filelock(this, &filelock_type),
  xattrlock(this, &xattrlock_type),
  snaplock(this, &snaplock_type),
  nestlock(this, &nestlock_type),
  flocklock(this, &flocklock_type),
  policylock(this, &policylock_type),
  loner_cap(-1), want_loner_cap(-1),
  want_max_size(0)
{

}

void CInode::make_string(string& s) const
{
  mutex_assert_locked_by_me();
  if (is_base()) {
    if (is_mdsdir()) {
      char buf[40];
      uint64_t mds = (uint64_t)ino() - MDS_INO_MDSDIR_OFFSET;
      snprintf(buf, sizeof(buf), "~mds%" PRId64 "/", mds);
      s = buf;
    } else {
      s = "/";
    }
  } else {
    CDentry *dn = get_parent_dn();
    if (dn) {
      dn->make_string(s);
    } else {
      char buf[40];
      snprintf(buf, sizeof(buf), "#%" PRIx64, (uint64_t)ino());
      s = buf;
    }
    if (is_dir())
      s += "/";
  }
}

void CInode::name_stray_dentry(string& dname) const
{
  char s[20];
  snprintf(s, sizeof(s), "%llx", (unsigned long long)inode.ino.val);
  dname = s;
}

void CInode::get_dirfrags(list<CDir*>& ls)
{
  mutex_assert_locked_by_me();
  for (auto p = dirfrags.begin(); p != dirfrags.end(); ++p)
    ls.push_back(p->second);
}

CDirRef CInode::get_or_open_dirfrag(frag_t fg)
{
  CDirRef ref = get_dirfrag(fg);
  if (!ref) {
    assert(fg == frag_t());
    CDir *dir = new CDir(this);
    dirfrags[fg] = dir;
    ref = dir;
  }
  return ref;
}

void CInode::close_dirfrag(frag_t fg)
{
  mutex_assert_locked_by_me();
  auto p = dirfrags.find(fg);
  assert(p != dirfrags.end());
  CDir *dir = p->second;
  assert(dir->get_num_ref() == 0);
  delete dir;
  dirfrags.erase(p);
}

inode_t *CInode::project_inode(std::map<string, bufferptr> **ppx)
{
  mutex_assert_locked_by_me();
  const xattr_map_t *px = NULL;
  if (projected_nodes.empty()) {
    if (ppx)
      px = get_xattrs();
    projected_nodes.push_back(projected_inode_t(inode, px));
  } else {
    const inode_t *pi = get_projected_inode();
    if (ppx)
      px = get_projected_xattrs();
    projected_nodes.push_back(projected_inode_t(*pi, px));
  }
  if (ppx)
    *ppx = latest_projected_xattrs = &projected_nodes.back().xattrs;

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
  if (pi->inode.is_backtrace_updated())
    _mark_dirty_parent(ls, pi->inode.layout.pool_id != inode.layout.pool_id);

  inode = pi->inode;

  if (pi->xattrs_projected) {
    if (latest_projected_xattrs == &pi->xattrs)
      latest_projected_xattrs = NULL;
    xattrs.swap(pi->xattrs);
  }

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
    assert(ls);
  }

  if (ls) {
    mdcache->lock_log_segments();
    ls->dirty_inodes.push_back(&item_dirty);
    mdcache->unlock_log_segments();
  }
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
    // remove myself from ls dirty list
    mdcache->lock_log_segments();
    item_dirty.remove_myself();
    mdcache->unlock_log_segments();

    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);
  }
}

void CInode::_mark_dirty_parent(LogSegment *ls, bool dirty_pool)
{
  if (!state_test(STATE_DIRTYPARENT)) {
    dout(10) << "mark_dirty_parent" << dendl;
    state_set(STATE_DIRTYPARENT);
    get(PIN_DIRTYPARENT);
    assert(ls);
  }
  if (dirty_pool)
    state_set(STATE_DIRTYPOOL);
  if (ls) {
    mdcache->lock_log_segments();
    ls->dirty_parent_inodes.push_back(&item_dirty_parent);
    mdcache->unlock_log_segments();
  }
}

void CInode::clear_dirty_parent()
{
  if (state_test(STATE_DIRTYPARENT)) {
    dout(10) << "clear_dirty_parent" << dendl;
    mdcache->lock_log_segments();
    item_dirty_parent.remove_myself();
    mdcache->unlock_log_segments();

    state_clear(STATE_DIRTYPARENT|STATE_DIRTYPOOL);
    put(PIN_DIRTYPARENT);
  }
}

void CInode::clear_replay_undefined()
{
  if (state_test(STATE_REPLAYUNDEF)) {
    dout(10) << "clear_replay_undef" << dendl;
    mdcache->lock_log_segments();
    item_dirty_parent.remove_myself();
    mdcache->unlock_log_segments();
    state_clear(STATE_REPLAYUNDEF);
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
			     unsigned max_bytes, int getattr_wants)
{
  client_t client = session->get_client();
  assert(session->connection);

  snapid_t snapid = CEPH_NOSNAP;
  
  // pick a version!
  const inode_t *oi = get_inode();
  const inode_t *pi = get_projected_inode();
  const xattr_map_t *pxattrs = 0;
  
  // "fake" a version that is old (stable) version, +1 if projected.
  version_t version = (oi->version * 2) + is_projected();

  Capability *cap = get_client_cap(client);
  bool pfile = filelock.is_xlocked_by_client(client) || get_loner() == client;
  bool pauth = authlock.is_xlocked_by_client(client) || get_loner() == client;
  bool plink = linklock.is_xlocked_by_client(client) || get_loner() == client;
  bool pxattr = xattrlock.is_xlocked_by_client(client) || get_loner() == client;
  bool plocal = versionlock.get_last_wrlock_client() == client;
  bool ppolicy = policylock.is_xlocked_by_client(client) || get_loner()==client;
  
  const inode_t *any_i = (pfile|pauth|plink|pxattr|plocal) ? pi : oi;
  
  dout(20) << " pfile " << pfile << " pauth " << pauth
	   << " plink " << plink << " pxattr " << pxattr
	   << " plocal " << plocal << " ctime " << any_i->ctime
	   << dendl;

  // file
  const inode_t *file_i = pfile ? pi:oi;
  file_layout_t layout;
  if (is_dir()) {
    layout = (ppolicy ? pi : oi)->layout;
  } else {
    layout = file_i->layout;
  }

  // max_size is min of projected, actual
  uint64_t max_size;
  {
    auto p = oi->client_ranges.find(client);
    auto q = pi->client_ranges.find(client);
    if (p == oi->client_ranges.end() ||
	q == pi->client_ranges.end())
      max_size = 0;
    else
      max_size = MIN(p->second.range.last, q->second.range.last);
  }

  // inline data
  version_t inline_version = CEPH_INLINE_NONE;
  bufferlist inline_data;
#if 0
  if (file_i->inline_data.version == CEPH_INLINE_NONE) {
    inline_version = CEPH_INLINE_NONE;
  } else if (!cap ||
	     cap->client_inline_version < file_i->inline_data.version ||
	     (getattr_wants & CEPH_CAP_FILE_RD)) { // client requests inline data
    inline_version = file_i->inline_data.version;
    if (file_i->inline_data.length() > 0)
      inline_data = file_i->inline_data.get_data();
  }
#endif

  // nest (do same as file... :/)
  if (cap) {
    cap->last_rbytes = file_i->rstat.rbytes;
    cap->last_rsize = file_i->rstat.rsize();
  }

  // auth
  const inode_t *auth_i = pauth ? pi:oi;

  // link
  const inode_t *link_i = plink ? pi:oi;
  
  // xattr
  const inode_t *xattr_i = pxattr ? pi:oi;

  // xattr
  bufferlist xbl;
  version_t xattr_version;
  if (!cap || cap->client_xattr_version < xattr_i->xattr_version ||
      (getattr_wants & CEPH_CAP_XATTR_SHARED)) { // client requests xattrs
    if (!pxattrs)
      pxattrs = pxattr ? get_projected_xattrs() : &xattrs;
    ::encode(*pxattrs, xbl);
    xattr_version = xattr_i->xattr_version;
  } else {
    xattr_version = 0;
  }
  
  // do we have room?
  if (max_bytes) {
    unsigned bytes = 8 + 8 + 4 + 8 + 8 + sizeof(ceph_mds_reply_cap) +
      sizeof(struct ceph_file_layout) + 4 + layout.pool_ns.size() +
      sizeof(struct ceph_timespec) * 3 +
      4 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 4 +
      8 + 8 + 8 + 8 + 8 + sizeof(struct ceph_timespec) +
      4;
    bytes += sizeof(__u32);
    bytes += (sizeof(__u32) + sizeof(__u32)) * 0 /*  dirfragtree._splits.size() */;
    bytes += sizeof(__u32) + symlink.length();
    bytes += sizeof(__u32) + xbl.length();
    bytes += sizeof(version_t) + sizeof(__u32) + inline_data.length();
    if (bytes > max_bytes)
      return -ENOSPC;
  }


  // encode caps
  if (!cap) {
    // add a new cap
    cap = add_client_cap(session);
    if (is_auth()) {
      if (choose_ideal_loner() >= 0)
        try_set_loner();
      else if (get_wanted_loner() < 0)
        try_drop_loner();
    }
  }

  int likes = get_caps_liked();
  int allowed = get_caps_allowed_for_client(session, file_i);
  int issue = (cap->wanted() | likes) & allowed;
  cap->issue_norevoke(issue);
  issue = cap->pending();
  cap->set_last_issue();
  cap->set_last_issue_stamp(ceph_clock_now(g_ceph_context));
  cap->clear_new();

  struct ceph_mds_reply_cap ecap;
  ecap.caps = issue;
  ecap.wanted = cap->wanted();
  ecap.cap_id = cap->get_cap_id();
  ecap.seq = cap->get_last_seq();
  dout(10) << "encode_inodestat issuing " << ccap_string(issue)
           << " seq " << cap->get_last_seq() << dendl;
  ecap.mseq = cap->get_mseq();
  ecap.realm = CEPH_INO_ROOT;

  ecap.flags = CEPH_CAP_FLAG_AUTH;
  dout(10) << "encode_inodestat caps " << ccap_string(ecap.caps)
	   << " seq " << ecap.seq << " mseq " << ecap.mseq
	   << " xattrv " << xattr_version << " len " << xbl.length()
	   << dendl;

  if (inline_data.length() && cap) {
    if ((cap->pending() | getattr_wants) & CEPH_CAP_FILE_SHARED) {
      dout(10) << "including inline version " << inline_version << dendl;
      cap->client_inline_version = inline_version;
    } else {
      dout(10) << "dropping inline version " << inline_version << dendl;
      inline_version = 0;
      inline_data.clear();
    }
  }

  // include those xattrs?
  if (xbl.length() && cap) {
    if ((cap->pending() | getattr_wants) & CEPH_CAP_XATTR_SHARED) {
      dout(10) << "including xattrs version " << xattr_i->xattr_version << dendl;
      cap->client_xattr_version = xattr_i->xattr_version;
    } else {
      dout(10) << "dropping xattrs version " << xattr_i->xattr_version << dendl;
      xbl.clear(); // no xattrs .. XXX what's this about?!?
      xattr_version = 0;
    }
  }

  /*
   * note: encoding matches MClientReply::InodeStat
   */
  ::encode(oi->ino, bl);
  ::encode(snapid, bl);
  ::encode(oi->rdev, bl);
  ::encode(version, bl);

  ::encode(xattr_version, bl);

  ::encode(ecap, bl);
  {
    ceph_file_layout legacy_layout;
    layout.to_legacy(&legacy_layout);
    ::encode(legacy_layout, bl);
  }
  ::encode(any_i->ctime, bl);
  ::encode(file_i->mtime, bl);
  ::encode(file_i->atime, bl);
  ::encode(file_i->time_warp_seq, bl);
  ::encode(file_i->size, bl);
  ::encode(max_size, bl);
  ::encode(file_i->truncate_size, bl);
  ::encode(file_i->truncate_seq, bl);

  ::encode(auth_i->mode, bl);
  ::encode((uint32_t)auth_i->uid, bl);
  ::encode((uint32_t)auth_i->gid, bl);

  ::encode(link_i->nlink, bl);

  ::encode(file_i->dirstat.nfiles, bl);
  ::encode(file_i->dirstat.nsubdirs, bl);
  ::encode(file_i->rstat.rbytes, bl);
  ::encode(file_i->rstat.rfiles, bl);
  ::encode(file_i->rstat.rsubdirs, bl);
  ::encode(file_i->rstat.rctime, bl);

  {
    fragtree_t empty;
    empty.encode(bl);
  }
  //dirfragtree.encode(bl);

  ::encode(symlink, bl);
  if (session->connection->has_feature(CEPH_FEATURE_DIRLAYOUTHASH)) {
    ::encode(file_i->dir_layout, bl);
  }
  ::encode(xbl, bl);
  if (session->connection->has_feature(CEPH_FEATURE_MDS_INLINE_DATA)) {
    ::encode(inline_version, bl);
    ::encode(inline_data, bl);
  }
  if (session->connection->has_feature(CEPH_FEATURE_MDS_QUOTA)) {
    const inode_t *policy_i = ppolicy ? pi : oi;
    ::encode(policy_i->quota, bl);
  }
  if (session->connection->has_feature(CEPH_FEATURE_FS_FILE_LAYOUT_V2)) {
    ::encode(layout.pool_ns, bl);
  }

  return 0;
}


void CInode::encode_cap_message(MClientCaps *m, Capability *cap)
{
  assert(cap);

  client_t client = cap->get_client();

  bool pfile = filelock.is_xlocked_by_client(client) || (cap->issued() & CEPH_CAP_FILE_EXCL);
  bool pauth = authlock.is_xlocked_by_client(client);
  bool plink = linklock.is_xlocked_by_client(client);
  bool pxattr = xattrlock.is_xlocked_by_client(client);

  const inode_t *oi = get_inode();
  const inode_t *pi = get_projected_inode();
  const inode_t *i = (pfile|pauth|plink|pxattr) ? pi : oi;

  dout(20) << "encode_cap_message pfile " << pfile
           << " pauth " << pauth << " plink " << plink << " pxattr " << pxattr
           << " ctime " << i->ctime << dendl;

  i = pfile ? pi:oi;
  m->layout = i->layout;
  m->size = i->size;
  m->truncate_seq = i->truncate_seq;
  m->truncate_size = i->truncate_size;
  m->mtime = i->mtime;
  m->atime = i->atime;
  m->ctime = i->ctime;
  m->time_warp_seq = i->time_warp_seq;

  m->inline_version = CEPH_INLINE_NONE;
  /*
  if (cap->client_inline_version < i->inline_data.version) {
    m->inline_version = cap->client_inline_version = i->inline_data.version;
    if (i->inline_data.length() > 0)
      m->inline_data = i->inline_data.get_data();
  } else {
    m->inline_version = 0;
  }
  */

  // max_size is min of projected, actual.
  uint64_t oldms = 0, newms = 0;
  {
    auto p = oi->client_ranges.find(client);
    if (p != oi->client_ranges.end())
      oldms = p->second.range.last;
    auto q = pi->client_ranges.find(client);
    if (p != pi->client_ranges.end())
      newms = q->second.range.last;
  }
  m->max_size = MIN(oldms, newms);

  i = pauth ? pi:oi;
  m->head.mode = i->mode;
  m->head.uid = i->uid;
  m->head.gid = i->gid;

  i = plink ? pi:oi;
  m->head.nlink = i->nlink;

  i = pxattr ? pi:oi;
  const xattr_map_t *ix = pxattr ? get_projected_xattrs() : &xattrs;
  if ((cap->pending() & CEPH_CAP_XATTR_SHARED) &&
      i->xattr_version > cap->client_xattr_version) {
    dout(10) << "    including xattrs v " << i->xattr_version << dendl;
    ::encode(*ix, m->xattrbl);
    m->head.xattr_version = i->xattr_version;
    cap->client_xattr_version = i->xattr_version;
  }
}

int CInode::get_caps_liked() const
{
  if (is_dir())
    return CEPH_CAP_PIN | CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_SHARED;  // but not, say, FILE_RD|WR|WRBUFFER
  else
    return CEPH_CAP_ANY & ~CEPH_CAP_FILE_LAZYIO;
}

int CInode::get_caps_allowed_ever() const
{
  int allowed;
  if (is_dir())
    allowed = CEPH_CAP_PIN | CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_SHARED;
  else
    allowed = CEPH_CAP_ANY;
  return allowed &
    (CEPH_CAP_PIN |
     (filelock.gcaps_allowed_ever() << filelock.get_cap_shift()) |
     (authlock.gcaps_allowed_ever() << authlock.get_cap_shift()) |
     (xattrlock.gcaps_allowed_ever() << xattrlock.get_cap_shift()) |
     (linklock.gcaps_allowed_ever() << linklock.get_cap_shift()));
}

int CInode::get_caps_allowed_by_type(int type) const
{
  return
    CEPH_CAP_PIN |
    (filelock.gcaps_allowed(type) << filelock.get_cap_shift()) |
    (authlock.gcaps_allowed(type) << authlock.get_cap_shift()) |
    (xattrlock.gcaps_allowed(type) << xattrlock.get_cap_shift()) |
    (linklock.gcaps_allowed(type) << linklock.get_cap_shift());
}

int CInode::get_caps_careful() const
{
  return
    (filelock.gcaps_careful() << filelock.get_cap_shift()) |
    (authlock.gcaps_careful() << authlock.get_cap_shift()) |
    (xattrlock.gcaps_careful() << xattrlock.get_cap_shift()) |
    (linklock.gcaps_careful() << linklock.get_cap_shift());
}

int CInode::get_xlocker_mask(client_t client) const
{
  return
    (filelock.gcaps_xlocker_mask(client) << filelock.get_cap_shift()) |
    (authlock.gcaps_xlocker_mask(client) << authlock.get_cap_shift()) |
    (xattrlock.gcaps_xlocker_mask(client) << xattrlock.get_cap_shift()) |
    (linklock.gcaps_xlocker_mask(client) << linklock.get_cap_shift());
}

int CInode::get_caps_allowed_for_client(Session *session, const inode_t *file_i) const
{
  client_t client = session->info.inst.name.num();
  int allowed;
  if (client == get_loner()) {
    // as the loner, we get the loner_caps AND any xlocker_caps for things we have xlocked
    allowed =
      get_caps_allowed_by_type(CAP_LONER) |
      (get_caps_allowed_by_type(CAP_XLOCKER) & get_xlocker_mask(client));
  } else {
    allowed = get_caps_allowed_by_type(CAP_ANY);
  }

  if (!is_dir()) {
    if ((file_i->inline_data.version != CEPH_INLINE_NONE &&
         !session->connection->has_feature(CEPH_FEATURE_MDS_INLINE_DATA)) ||
        (!file_i->layout.pool_ns.empty() &&
         !session->connection->has_feature(CEPH_FEATURE_FS_FILE_LAYOUT_V2)))
      allowed &= ~(CEPH_CAP_FILE_RD | CEPH_CAP_FILE_WR);
  }
  return allowed;
}

// caps issued, wanted
int CInode::get_caps_issued(int *ploner, int *pother, int *pxlocker,
                            int shift, int mask)
{ 
  int c = 0;
  int loner = 0, other = 0, xlocker = 0;
  if (!is_auth()) {
    loner_cap = -1;
  }
  
  for (map<client_t,Capability*>::const_iterator it = client_caps.begin();
       it != client_caps.end();
       ++it) {
    int i = it->second->issued();
    c |= i;
    if (it->first == loner_cap)
      loner |= i;
    else
      other |= i;
    xlocker |= get_xlocker_mask(it->first) & i;
  }
  if (ploner) *ploner = (loner >> shift) & mask;
  if (pother) *pother = (other >> shift) & mask; 
  if (pxlocker) *pxlocker = (xlocker >> shift) & mask;
  return (c >> shift) & mask;
}

bool CInode::is_any_caps_wanted() const
{
  for (map<client_t,Capability*>::const_iterator it = client_caps.begin();
       it != client_caps.end();
       ++it)
    if (it->second->wanted())
      return true;
  return false;
}

int CInode::get_caps_wanted(int *ploner, int *pother, int shift, int mask) const
{
  int w = 0;
  int loner = 0, other = 0;
  for (map<client_t,Capability*>::const_iterator it = client_caps.begin();
       it != client_caps.end();
       ++it) {
    if (!it->second->is_stale()) {
      int t = it->second->wanted();
      w |= t;
      if (it->first == loner_cap)
        loner |= t;
      else
        other |= t;
    }
    //cout << " get_caps_wanted client " << it->first << " " << cap_string(it->second.wanted()) << endl;
  }
  if (ploner) *ploner = (loner >> shift) & mask;
  if (pother) *pother = (other >> shift) & mask;
  return (w >> shift) & mask;
}

bool CInode::issued_caps_need_gather(SimpleLock *lock)
{
  int loner_issued, other_issued, xlocker_issued;
  get_caps_issued(&loner_issued, &other_issued, &xlocker_issued,
                  lock->get_cap_shift(), lock->get_cap_mask());
  if ((loner_issued & ~lock->gcaps_allowed(CAP_LONER)) ||
      (other_issued & ~lock->gcaps_allowed(CAP_ANY)) ||
      (xlocker_issued & ~lock->gcaps_allowed(CAP_XLOCKER)))
    return true;
  return false;
}

client_t CInode::calc_ideal_loner()
{
  int n = 0;
  client_t loner = -1;
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
       it != client_caps.end();
       ++it)
    if (!it->second->is_stale() &&
        (it->second->wanted() & (CEPH_CAP_ANY_WR|CEPH_CAP_FILE_WR|CEPH_CAP_FILE_RD))) {
      if (n)
        return -1;
      n++;
      loner = it->first;
    }
  return loner;
}

client_t CInode::choose_ideal_loner()
{
  want_loner_cap = calc_ideal_loner();
  return want_loner_cap;
}

void CInode::set_loner_cap(client_t l)
{
  loner_cap = l;
  authlock.set_excl_client(loner_cap);
  filelock.set_excl_client(loner_cap);
  linklock.set_excl_client(loner_cap);
  xattrlock.set_excl_client(loner_cap);
}

bool CInode::try_set_loner()
{
  assert(want_loner_cap >= 0);
  if (loner_cap >= 0 && loner_cap != want_loner_cap)
    return false;
  set_loner_cap(want_loner_cap);
  return true;
}

bool CInode::try_drop_loner()
{
  if (loner_cap < 0)
    return true;

  int other_allowed = get_caps_allowed_by_type(CAP_ANY);
  Capability *cap = get_client_cap(loner_cap);
  if (!cap ||
      (cap->issued() & ~other_allowed) == 0) {
    set_loner_cap(-1);
    return true;
  }
  return false;
}

void CInode::start_scatter(ScatterLock *lock)
{
  mutex_assert_locked_by_me();
  dout(10) << "start_scatter " << *lock << " on " << *this << dendl;

  for (auto p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    frag_t fg = p->first;
    CDir *dir = p->second;
    dout(20) << fg << " " << *dir << dendl;
    switch (lock->get_type()) {
      case CEPH_LOCK_IFILE:
      case CEPH_LOCK_INEST:
	finish_scatter_update(lock, dir);
	break;
      case CEPH_LOCK_IDFT:
	//dir->state_clear(CDir::STATE_DIRTYDFT);
	break;
    }
  }
}

void CInode::__frag_update_finish(CDir *dir, const MutationRef& mut)
{
  mut->wait_committing();
  dout(10) << "__finish_frag_update on " << *dir << dendl;

  mut->apply();
  mut->cleanup();
}


class C_Inode_FragUpdate : public CInodeLogContext {
  CDir *dir;
  MutationRef mut;
  void finish(int r) {
    in->__frag_update_finish(dir, mut);
  }
public:
  C_Inode_FragUpdate(CInode *i, CDir *d, const MutationRef& m)
    : CInodeLogContext(i), dir(d), mut(m) {}
};

void CInode::finish_scatter_update(ScatterLock *lock, CDir *dir)
{ 
  const inode_t *pi = get_projected_inode();
  frag_t fg = dir->get_frag();

  version_t inode_version;
  version_t dir_accounted_version;
  switch (lock->get_type()) {
  case CEPH_LOCK_IFILE:
    inode_version = pi->dirstat.version;
    dir_accounted_version = dir->get_projected_fnode()->accounted_fragstat.version;
    break;
  case CEPH_LOCK_INEST: 
    inode_version = pi->rstat.version;
    dir_accounted_version = dir->get_projected_fnode()->accounted_rstat.version;
    break;
  default:
    assert(0); 
  }

  if (dir->get_version() == 0) {
    dout(10) << "finish_scatter_update " << fg << " not loaded, marking " << *lock << " stale " << *dir << dendl;
  } else {
    if (dir_accounted_version != inode_version) {
      dout(10) << "finish_scatter_update " << fg << " journaling accounted scatterstat update v" << inode_version << dendl;

      MutationRef mut(new MutationImpl);

      fnode_t *pf = dir->project_fnode();
      pf->version = dir->pre_dirty();

      const char *ename = 0;
      switch (lock->get_type()) {
      case CEPH_LOCK_IFILE:
	pf->fragstat.version = pi->dirstat.version;
	pf->accounted_fragstat = pf->fragstat;
	ename = "lock ifile accounted scatter stat update";
	break;
      case CEPH_LOCK_INEST: 
	pf->rstat.version = pi->rstat.version;
	pf->accounted_rstat = pf->rstat;
	ename = "lock inest accounted scatter stat update";
	break;
      default:
	assert(0);
      }

      mut->add_projected_fnode(dir, false);
      mut->pin(dir);

      EUpdate *le = new EUpdate(NULL, ename);
      le->metablob.add_dir(dir, true);

      MDLog *mdlog = mdcache->mds->mdlog;
      mdlog->start_entry(le);
      mut->ls = mdlog->get_current_segment();
      mdlog->submit_entry(le, new C_Inode_FragUpdate(this, dir, mut));

      mut->start_committing();
    } else {
      dout(10) << "finish_scatter_update " << fg << " accounted " << *lock
	       << " scatter stat unchanged at v" << dir_accounted_version << dendl;
    }
  }
}

void CInode::finish_scatter_gather_update(int type, const MutationRef& mut)
{
  dout(10) << "finish_scatter_gather_update " << type << " on " << *this << dendl;

  switch (type) {
  case CEPH_LOCK_IFILE:
    {
      inode_t *pi = __get_projected_inode();

      bool touched_mtime = false;
      dout(20) << "  orig dirstat " << pi->dirstat << dendl;
      pi->dirstat.version++;
      for (auto p = dirfrags.begin(); p != dirfrags.end(); ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;

	bool update = dir->get_version() > 0;
	const fnode_t *pf = update ? dir->project_fnode() : dir->get_projected_fnode();

	if (pf->accounted_fragstat.version == pi->dirstat.version - 1) {
	  dout(20) << fg << "           fragstat " << pf->fragstat << dendl;
	  dout(20) << fg << " accounted_fragstat " << pf->accounted_fragstat << dendl;
	  pi->dirstat.add_delta(pf->fragstat, pf->accounted_fragstat, touched_mtime);
	} else {
	  dout(20) << fg << " skipping STALE accounted_fragstat " << pf->accounted_fragstat << dendl;
	}

	if (update) {
	  mut->pin(dir);
	  mut->add_projected_fnode(dir, true);
	  fnode_t *pf = dir->__get_projected_fnode();
	  pf->version = dir->pre_dirty();

	  pf->fragstat.version = pi->dirstat.version;
	  pf->accounted_fragstat = pf->fragstat;
	  dout(10) << fg << " updated accounted_fragstat " << pf->fragstat << " on " << *dir << dendl;
	}
      }
      if (touched_mtime)
	pi->mtime = pi->ctime = pi->dirstat.mtime;
      dout(20) << " final dirstat " << pi->dirstat << dendl;
    }
    break;

  case CEPH_LOCK_INEST:
    {
      inode_t *pi = __get_projected_inode();

      dout(20) << "  orig rstat " << pi->rstat << dendl;
      pi->rstat.version++;
      for (auto p = dirfrags.begin(); p != dirfrags.end(); ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;

	bool update = dir->get_version() > 0;
	const fnode_t *pf = update ? dir->project_fnode() : dir->get_projected_fnode();

	if (pf->accounted_rstat.version == pi->rstat.version-1) {
	  // only pull this frag's dirty rstat inodes into the frag if
	  // the frag is non-stale and updateable.  if it's stale,
	  // that info will just get thrown out!
	  if (update)
	    dir->assimilate_dirty_rstat_inodes(mut);

	  dout(20) << fg << "           rstat " << pf->rstat << dendl;
	  dout(20) << fg << " accounted_rstat " << pf->accounted_rstat << dendl;
	  mdcache->project_rstat_frag_to_inode(pf, pi);
	} else {
	  dout(20) << fg << " skipping STALE accounted_rstat " << pf->accounted_rstat << dendl;
	}
	if (update) {
	  mut->pin(dir);
	  mut->add_projected_fnode(dir, true);
	  fnode_t *pf = dir->__get_projected_fnode();
	  pf->version = dir->pre_dirty();

	  pf->rstat.version = pi->rstat.version;
	  pf->accounted_rstat = pf->rstat;
	  dout(10) << fg << " updated accounted_rstat " << pf->rstat << " on " << *dir << dendl;
	}
      }
      dout(20) << " final rstat " << pi->rstat << dendl;
    }
    break;

  case CEPH_LOCK_IDFT:
    break;

  default:
    assert(0);
  }
}

void CInode::finish_scatter_gather_update_accounted(int type, const MutationRef& mut,
						    EMetaBlob *metablob)
{
  dout(10) << "finish_scatter_gather_update_accounted " << type << " on " << *this << dendl;
  assert(is_auth());

  for (auto p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    CDir *dir = p->second;
    if (dir->get_version() == 0)
      continue;
    
    if (type == CEPH_LOCK_IDFT)
      continue;  // nothing to do.

    dout(10) << " journaling updated frag accounted_ on " << *dir << dendl;
    assert(dir->is_projected());
    metablob->add_dir(dir, true);

    if (type == CEPH_LOCK_INEST)
      dir->assimilate_dirty_rstat_inodes_finish(mut, metablob);
  }
}

void CInode::mark_dirty_rstat()
{
  mutex_assert_locked_by_me();
  if (!state_test(STATE_DIRTYRSTAT)) {
    dout(10) << "mark_dirty_rstat" << dendl;
    state_set(STATE_DIRTYRSTAT);
    get(PIN_DIRTYRSTAT);
    CDentry *dn = get_projected_parent_dn();
    dn->get_dir()->add_dirty_rstat_inode(this);
  }
}
void CInode::clear_dirty_rstat()
{
  mutex_assert_locked_by_me();
  if (state_test(STATE_DIRTYRSTAT)) {
    dout(10) << "clear_dirty_rstat" << dendl;
    CDentry *dn = get_projected_parent_dn();
    dn->get_dir()->remove_dirty_rstat_inode(this);
    state_clear(STATE_DIRTYRSTAT);
    put(PIN_DIRTYRSTAT);
  }
}

void CInode::clear_dirty_scattered(int type)
{
  mutex_assert_locked_by_me();
  dout(10) << "clear_dirty_scattered " << type << " on " << *this << dendl;
  mdcache->lock_log_segments();
  switch (type) {
    case CEPH_LOCK_IFILE:
      item_dirty_dirfrag_dir.remove_myself();
      break;
    case CEPH_LOCK_INEST:
      item_dirty_dirfrag_nest.remove_myself();
      break;
    case CEPH_LOCK_IDFT:
//      item_dirty_dirfrag_dirfragtree.remove_myself();
      break;
    default:
      assert(0);
  }
  mdcache->unlock_log_segments();
}

void CInode::mark_dirty_scattered(int type, LogSegment *ls)
{
  mutex_assert_locked_by_me();
  dout(10) << "mark_dirty_scattered " << type << " on " << *this << dendl;
  mdcache->lock_log_segments();
  switch (type) {
    case CEPH_LOCK_IFILE:
      ls->dirty_dirfrag_dir.push_back(&item_dirty_dirfrag_dir);
      break;
    case CEPH_LOCK_INEST:
      ls->dirty_dirfrag_nest.push_back(&item_dirty_dirfrag_nest);
      break;
    case CEPH_LOCK_IDFT:
//      ls->dirty_dirfrag_dirfragtree.push_back(&tem_dirty_dirfrag_dirfragtree);
    default:
      assert(0);
  }
  mdcache->unlock_log_segments();
}

void CInode::mark_updated_scatterlocks(int mask, LogSegment *ls)
{
  dout(10) << "mark_updated_scatterlocks " << mask << " on " << *this << dendl;
  if (mask & CEPH_LOCK_IFILE)
    mdcache->locker->mark_updated_scatterlock(&filelock, ls);
  if (mask & CEPH_LOCK_INEST)
    mdcache->locker->mark_updated_scatterlock(&nestlock, ls);
}

Capability *CInode::add_client_cap(Session *session)
{
  mutex_assert_locked_by_me();

  if (client_caps.empty()) {
    get(PIN_CAPS);
/*
    if (conrealm)
      containing_realm = conrealm;
    else
      containing_realm = find_snaprealm();
    containing_realm->inodes_with_caps.push_back(&item_caps);
    dout(10) << "add_client_cap first cap, joining realm " << *containing_realm << dendl;
*/
  }

/*
  mdcache->num_caps++;
  if (client_caps.empty())
    mdcache->num_inodes_with_caps++;
*/

  Capability *cap = new Capability(this, session, mdcache->get_new_cap_id());
  assert(client_caps.count(session->get_client()) == 0);
  client_caps[session->get_client()] = cap;

  session->mutex_lock();
  session->touch_cap(cap);
  if (session->is_stale())
    cap->mark_stale();
  session->mutex_unlock();

  cap->client_follows = 1;

//  containing_realm->add_cap(client, cap);

  return cap;
}

void CInode::remove_client_cap(Session *session)
{
  mutex_assert_locked_by_me();
  client_t client = session->get_client();
  auto p = client_caps.find(client);
  assert(p != client_caps.end());
  Capability *cap = p->second;;

  session->mutex_lock();
  cap->item_session_caps.remove_myself();
  session->mutex_unlock();
/*
  cap->item_revoking_caps.remove_myself();
  cap->item_client_revoking_caps.remove_myself();
  containing_realm->remove_cap(client, cap);
*/

  if (client == loner_cap)
    loner_cap = -1;

  client_caps.erase(p);
  delete cap;
  if (client_caps.empty()) {
    put(PIN_CAPS);
//    dout(10) << "remove_client_cap last cap, leaving realm " << *containing_realm << dendl;
//    item_caps.remove_myself();
//    containing_realm = NULL;
  }
//  mdcache->num_caps--;

  //clean up advisory locks
/*
  bool fcntl_removed = fcntl_locks ? fcntl_locks->remove_all_from(client) : false;
  bool flock_removed = flock_locks ? flock_locks->remove_all_from(client) : false;
  if (fcntl_removed || flock_removed) {
    list<MDSContextBase*> waiters;
    take_waiting(CInode::WAIT_FLOCK, waiters);
    mdcache->mds->queue_waiters(waiters);
  }
*/
}

Capability *CInode::reconnect_cap(const cap_reconnect_t& icr, Session *session)
{
  mutex_assert_locked_by_me();
  Capability *cap = get_client_cap(session->get_client());
  if (cap) {
    // FIXME?
    cap->merge(icr.capinfo.wanted, icr.capinfo.issued);
  } else {
    cap = add_client_cap(session);
    cap->set_cap_id(icr.capinfo.cap_id);
    cap->set_wanted(icr.capinfo.wanted);
    cap->issue_norevoke(icr.capinfo.issued);
    cap->reset_seq();
  }
  cap->set_last_issue_stamp(ceph_clock_now(g_ceph_context));
  return cap;
}

void CInode::choose_lock_state(SimpleLock *lock, int allissued)
{
  int shift = lock->get_cap_shift();
  int issued = (allissued >> shift) & lock->get_cap_mask();
  if (is_auth()) {
    if (lock->is_xlocked()) {
      // do nothing here
    } else if (lock->get_state() != LOCK_MIX) {
      if (issued & (CEPH_CAP_GEXCL | CEPH_CAP_GBUFFER))
        lock->set_state(LOCK_EXCL);
      else if (issued & CEPH_CAP_GWR)
        lock->set_state(LOCK_MIX);
      else if (lock->is_dirty())
	lock->set_state(LOCK_LOCK);
      else
        lock->set_state(LOCK_SYNC);
    }
  } else {
    // our states have already been chosen during rejoin.
    if (lock->is_xlocked())
      assert(lock->get_state() == LOCK_LOCK);
  }
}

void CInode::choose_lock_states(int dirty_caps)
{
  int issued = get_caps_issued() | dirty_caps;
  if (is_auth() && (issued & (CEPH_CAP_ANY_EXCL|CEPH_CAP_ANY_WR)) &&
      choose_ideal_loner() >= 0)
    try_set_loner();
  choose_lock_state(&filelock, issued);
  choose_lock_state(&nestlock, issued);
  choose_lock_state(&dirfragtreelock, issued);
  choose_lock_state(&authlock, issued);
  choose_lock_state(&xattrlock, issued);
  choose_lock_state(&linklock, issued);
}

void CInode::clone(const CInode *other)
{
  inode = other->inode;
  symlink = other->symlink;
  xattrs= other->xattrs;
}

void CInode::encode_bare(bufferlist &bl, uint64_t features)
{
  ::encode(inode, bl, features);
  if (is_symlink())
    ::encode(symlink, bl);
  ::encode(dirfragtree, bl);
  ::encode(xattrs, bl);
  ::encode(bufferlist(), bl); // snapbl
  ::encode(old_inodes, bl, features);
}

void CInode::encode(bufferlist &bl, uint64_t features)
{
  ENCODE_START(4, 4, bl);
  encode_bare(bl, features);
  ENCODE_FINISH(bl);
}

void CInode::decode_bare(bufferlist::iterator &bl, __u8 struct_v)
{
  ::decode(inode, bl);
  if (is_symlink())
    ::decode(symlink, bl);
  {
    fragtree_t tmp;
    ::decode(tmp, bl);
    assert(tmp.empty());
  }
  ::decode(xattrs, bl);
  {
    bufferlist snap_blob;
    ::decode(snap_blob, bl);
    assert(snap_blob.length() == 0);
  }
  {
    map<snapid_t, old_inode_t> tmp;
    ::decode(tmp, bl);
    assert(tmp.empty());
  }
}

void CInode::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  decode_bare(bl, struct_v);
  DECODE_FINISH(bl);
}

struct C_Inode_Stored : public CInodeIOContext {
  version_t version;
  MDSContextBase *fin;
  C_Inode_Stored(CInode *i, version_t v, MDSContextBase *f) :
    CInodeIOContext(i), version(v), fin(f) {}
  void finish(int r) {
    in->_stored(r, version, fin);
  }
};

object_t CInode::get_object_name(inodeno_t ino, frag_t fg, const char *suffix)
{
  char n[60];
  snprintf(n, sizeof(n), "%llx.%08llx%s", (long long unsigned)ino, (long long unsigned)fg, suffix ? suffix : "");
  return object_t(n);
}

void CInode::store(MDSContextBase *fin, int op_prio)
{
  mutex_assert_locked_by_me();
  dout(10) << "store " << get_version() << dendl;
  assert(is_base());

  if (op_prio < 0)
    op_prio = CEPH_MSG_PRIO_DEFAULT;

  // encode
  bufferlist bl;
  string magic = CEPH_FS_ONDISK_MAGIC;
  ::encode(magic, bl);
  encode(bl, mdcache->mds->mdsmap->get_up_features());

  // write it.
  SnapContext snapc;
  ObjectOperation m;
  m.write_full(bl);

  object_t oid = CInode::get_object_name(ino(), frag_t(), ".inode");
  object_locator_t oloc(mdcache->get_metadata_pool());

  mdcache->mds->objecter->mutate(oid, oloc, m, snapc, ceph::real_clock::now(g_ceph_context),
				 0, NULL, new C_Inode_Stored(this, get_version(), fin));
}

void CInode::_stored(int r, version_t v, MDSContextBase *fin)
{
  if (r < 0) {
    derr << "store error " << r << " v " << v << " on " << *this << dendl;
    mdcache->mds->clog->error() << "failed to store ino " << ino() << " object,"
      << " errno " << r << "\n";
    mdcache->mds->handle_write_error(r);
    fin->complete(r);
    return;
  }

  mutex_lock();
  dout(10) << "_stored " << v << " on " << *this << dendl;
  if (v >= get_version())
    mark_clean();
  mutex_unlock();

  fin->complete(0);
}

struct C_Inode_Fetched : public CInodeIOContext {
  bufferlist bl;
  MDSContextBase *fin;
  C_Inode_Fetched(CInode *i, MDSContextBase *f) :
    CInodeIOContext(i), fin(f) {}
  void finish(int r) {
    in->_fetched(r, bl, fin);
  }
};

void CInode::fetch(MDSContextBase *fin)
{
  mutex_assert_locked_by_me();
  dout(10) << "fetch" << dendl;

  C_Inode_Fetched *c = new C_Inode_Fetched(this, fin);

  object_t oid = CInode::get_object_name(ino(), frag_t(), ".inode");
  object_locator_t oloc(mdcache->get_metadata_pool());
  mdcache->mds->objecter->read(oid, oloc, 0, 0, CEPH_NOSNAP, &c->bl, 0, c);
}

void CInode::_fetched(int r, bufferlist& bl, MDSContextBase *fin)
{
  if (r < 0) {
    derr << "fetch error " << r << " on " << *this << dendl;
    mdcache->mds->clog->error() << "failed to fetch ino " << ino() << " object,"
      << " errno " << r << "\n";
    fin->complete(r);
    return;
  }

  int err = -EINVAL;
  mutex_lock();
  // Attempt decode
  try {
    bufferlist::iterator p = bl.begin();
    string magic;
    ::decode(magic, p);
    dout(10) << " magic is '" << magic << "' (expecting '"
	     << CEPH_FS_ONDISK_MAGIC << "')" << dendl;
    if (magic != CEPH_FS_ONDISK_MAGIC) {
      dout(0) << "on disk magic '" << magic << "' != my magic '" << CEPH_FS_ONDISK_MAGIC
	      << "'" << dendl;
    } else {
      decode(p);
      err = 0;
      dout(10) << "_fetched " << *this << dendl;
    }
  } catch (buffer::error &err) {
    derr << "Corrupt inode 0x" << std::hex << ino() << std::dec
	 << ": " << err << dendl;
    return;
  }
  mutex_unlock();
  fin->complete(err);
}

int64_t CInode::get_backtrace_pool() const
{
  if (is_dir()) {
    return mdcache->get_metadata_pool();
  } else {
    // Files are required to have an explicit layout that specifies
    // a pool
    assert(inode.layout.pool_id != -1);
    return inode.layout.pool_id;
  }
}

void CInode::build_backtrace(int64_t pool, inode_backtrace_t& bt)
{
  bt.ino = ino();
  bt.ancestors.clear();
  bt.pool = pool;

  CDentry *pdn = get_parent_dn();
  assert(pdn);
  CInode *diri = pdn->get_dir_inode();
  bt.ancestors.push_back(inode_backpointer_t(diri->ino(), pdn->name, get_version()));
  for (auto p = inode.old_pools.begin(); p != inode.old_pools.end(); ++p) {
    // don't add our own pool id to old_pools to avoid looping (e.g. setlayout 0, 1, 0)
    if (*p != pool)
      bt.old_pools.insert(*p);
  }
}

struct C_Inode_BacktraceStored : public CInodeIOContext {
  version_t version;
  MDSContextBase *fin;
  C_Inode_BacktraceStored(CInode *i, version_t v, MDSContextBase *f) :
    CInodeIOContext(i), version(v), fin(f) {}
  void finish(int r) {
    in->_backtrace_stored(r, version, fin);
  }
};

void CInode::store_backtrace(MDSContextBase *fin, int op_prio)
{
  mutex_assert_locked_by_me();
  dout(10) << "store_backtrace on " << *this << dendl;
  assert(is_dirty_parent());

  if (op_prio < 0)
    op_prio = CEPH_MSG_PRIO_DEFAULT;

  const int64_t pool = get_backtrace_pool();
  inode_backtrace_t bt;
  build_backtrace(pool, bt);
  bufferlist parent_bl;
  ::encode(bt, parent_bl);

  ObjectOperation op;
  op.priority = op_prio;
  op.create(false);
  op.setxattr("parent", parent_bl);

  /*
  bufferlist layout_bl;
  ::encode(inode.layout, layout_bl, mdcache->mds->mdsmap->get_up_features());
  op.setxattr("layout", layout_bl);
  */

  SnapContext snapc;
  object_t oid = get_object_name(ino(), frag_t(), "");
  object_locator_t oloc(pool);
  Context *fin2 = new C_Inode_BacktraceStored(this, inode.backtrace_version, fin);

  if (!state_test(STATE_DIRTYPOOL) || inode.old_pools.empty()) {
    dout(20) << __func__ << ": no dirtypool or no old pools" << dendl;
    mdcache->mds->objecter->mutate(oid, oloc, op, snapc,
                                   ceph::real_clock::now(g_ceph_context),
                                   0, NULL, fin2);
    return;
  }

  C_GatherBuilder gather(g_ceph_context, fin2);
  mdcache->mds->objecter->mutate(oid, oloc, op, snapc,
                                 ceph::real_clock::now(g_ceph_context),
                                 0, NULL, gather.new_sub());

  // In the case where DIRTYPOOL is set, we update all old pools backtraces
  // such that anyone reading them will see the new pool ID in
  // inode_backtrace_t::pool and go read everything else from there.
  for (compact_set<int64_t>::iterator p = inode.old_pools.begin();
       p != inode.old_pools.end();
       ++p) {
    if (*p == pool)
      continue;
    dout(20) << __func__ << ": updating old pool " << *p << dendl;

    ObjectOperation op;
    op.priority = op_prio;
    op.create(false);
    op.setxattr("parent", parent_bl);

    object_locator_t oloc(*p);
    mdcache->mds->objecter->mutate(oid, oloc, op, snapc,
                                   ceph::real_clock::now(g_ceph_context),
                                   0, NULL, gather.new_sub());
  }
  gather.activate();
}

void CInode::_backtrace_stored(int r, version_t v, MDSContextBase *fin)
{
  if (r < 0) {
    dout(1) << "store backtrace error " << r << " v " << v << dendl;
    mdcache->mds->clog->error() << "failed to store backtrace on ino "
				<< ino() << " object, errno " << r << "\n";
    mdcache->mds->handle_write_error(r);
    if (fin)
      fin->complete(r);
    return;
  }

  dout(10) << "_backtrace_stored v " << v <<  dendl;

  mutex_lock();
  if (v == get_inode()->backtrace_version)
    clear_dirty_parent();
  mutex_unlock();
  if (fin)
    fin->complete(0);
}

void CInode::first_get(bool locked)
{
  if (!locked)
    mutex_lock();
  if (parent)
    parent->get(CDentry::PIN_INODEPIN);
  if (!locked)
    mutex_unlock();
}

void CInode::last_put()
{
  Mutex::Locker l(mutex);
  if (parent)
    parent->put(CDentry::PIN_INODEPIN);
}

void intrusive_ptr_add_ref(CInode *o)
{
  o->get(CObject::PIN_INTRUSIVEPTR);
}
void intrusive_ptr_release(CInode *o)
{
  o->put(CObject::PIN_INTRUSIVEPTR);
}
