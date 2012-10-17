
#include "MetaSession.h"
#include "Inode.h"
#include "Dentry.h"
#include "Dir.h"

ostream& operator<<(ostream &out, Inode &in)
{
  out << in.vino() << "("
      << "ref=" << in._ref
      << " cap_refs=" << in.cap_refs
      << " open=" << in.open_by_mode
      << " mode=" << oct << in.mode << dec
      << " size=" << in.size
      << " mtime=" << in.mtime
      << " caps=" << ccap_string(in.caps_issued());
  if (!in.caps.empty()) {
    out << "(";
    for (map<int,Cap*>::iterator p = in.caps.begin(); p != in.caps.end(); ++p) {
      if (p != in.caps.begin())
	out << ',';
      out << p->first << '=' << ccap_string(p->second->issued);
    }
    out << ")";
  }
  if (in.dirty_caps)
    out << " dirty_caps=" << ccap_string(in.dirty_caps);
  if (in.flushing_caps)
    out << " flushing_caps=" << ccap_string(in.flushing_caps);

  if (in.flags & I_COMPLETE)
    out << " COMPLETE";

  if (in.is_file())
    out << " " << in.oset;

  if (!in.dn_set.empty())
    out << " parents=" << in.dn_set;

  out << ' ' << &in << ")";
  return out;
}


void Inode::make_long_path(filepath& p)
{
  if (!dn_set.empty()) {
    assert((*dn_set.begin())->dir && (*dn_set.begin())->dir->parent_inode);
    (*dn_set.begin())->dir->parent_inode->make_long_path(p);
    p.push_dentry((*dn_set.begin())->name);
  } else if (snapdir_parent) {
    snapdir_parent->make_nosnap_relative_path(p);
    string empty;
    p.push_dentry(empty);
  } else
    p = filepath(ino);
}

/*
 * make a filepath suitable for an mds request:
 *  - if we are non-snapped/live, the ino is sufficient, e.g. #1234
 *  - if we are snapped, make filepath relative to first non-snapped parent.
 */
void Inode::make_nosnap_relative_path(filepath& p)
{
  if (snapid == CEPH_NOSNAP) {
    p = filepath(ino);
  } else if (snapdir_parent) {
    snapdir_parent->make_nosnap_relative_path(p);
    string empty;
    p.push_dentry(empty);
  } else if (!dn_set.empty()) {
    assert((*dn_set.begin())->dir && (*dn_set.begin())->dir->parent_inode);
    (*dn_set.begin())->dir->parent_inode->make_nosnap_relative_path(p);
    p.push_dentry((*dn_set.begin())->name);
  } else {
    p = filepath(ino);
  }
}

void Inode::get_open_ref(int mode)
{
  open_by_mode[mode]++;
}

bool Inode::put_open_ref(int mode)
{
  //cout << "open_by_mode[" << mode << "] " << open_by_mode[mode] << " -> " << (open_by_mode[mode]-1) << std::endl;
  if (--open_by_mode[mode] == 0)
    return true;
  return false;
}

void Inode::get_cap_ref(int cap)
{
  int n = 0;
  while (cap) {
    if (cap & 1) {
      int c = 1 << n;
      cap_refs[c]++;
      //cout << "inode " << *this << " get " << cap_string(c) << " " << (cap_refs[c]-1) << " -> " << cap_refs[c] << std::endl;
    }
    cap >>= 1;
    n++;
  }
}

bool Inode::put_cap_ref(int cap)
{
  // if cap is always a single bit (which it seems to be)
  // all this logic is equivalent to:
  // if (--cap_refs[c]) return false; else return true;
  bool last = false;
  int n = 0;
  while (cap) {
    if (cap & 1) {
      int c = 1 << n;
      if (--cap_refs[c] == 0)
	last = true;      
      //cout << "inode " << *this << " put " << cap_string(c) << " " << (cap_refs[c]+1) << " -> " << cap_refs[c] << std::endl;
    }
    cap >>= 1;
    n++;
  }
  return last;
}

bool Inode::is_any_caps()
{
  return caps.size() || exporting_mds >= 0;
}

bool Inode::cap_is_valid(Cap* cap) 
{
  /*cout << "cap_gen     " << cap->session-> cap_gen << std::endl
    << "session gen " << cap->gen << std::endl
    << "cap expire  " << cap->session->cap_ttl << std::endl
    << "cur time    " << ceph_clock_now(cct) << std::endl;*/
  if ((cap->session->cap_gen <= cap->gen)
      && (ceph_clock_now(cct) < cap->session->cap_ttl)) {
    return true;
  }
  //if we make it here, the capabilities aren't up-to-date
  cap->session->was_stale = true;
  return true;
}

int Inode::caps_issued(int *implemented)
{
  int c = exporting_issued | snap_caps;
  int i = 0;
  for (map<int,Cap*>::iterator it = caps.begin();
       it != caps.end();
       it++)
    if (cap_is_valid(it->second)) {
      c |= it->second->issued;
      i |= it->second->implemented;
    }
  if (implemented)
    *implemented = i;
  return c;
}

void Inode::touch_cap(Cap *cap)
{
  // move to back of LRU
  cap->session->caps.push_back(&cap->cap_item);
}

void Inode::try_touch_cap(int mds)
{
  if (caps.count(mds))
    touch_cap(caps[mds]);
}

bool Inode::caps_issued_mask(unsigned mask)
{
  int c = exporting_issued | snap_caps;
  if ((c & mask) == mask)
    return true;
  // prefer auth cap
  if (auth_cap &&
      cap_is_valid(auth_cap) &&
      (auth_cap->issued & mask) == mask) {
    touch_cap(auth_cap);
    return true;
  }
  // try any cap
  for (map<int,Cap*>::iterator it = caps.begin();
       it != caps.end();
       it++) {
    if (cap_is_valid(it->second)) {
      if ((it->second->issued & mask) == mask) {
	touch_cap(it->second);
	return true;
      }
      c |= it->second->issued;
    }
  }
  if ((c & mask) == mask) {
    // bah.. touch them all
    for (map<int,Cap*>::iterator it = caps.begin();
	 it != caps.end();
	 it++)
      touch_cap(it->second);
    return true;
  }
  return false;
}

int Inode::caps_used()
{
  int w = 0;
  for (map<int,int>::iterator p = cap_refs.begin();
       p != cap_refs.end();
       p++)
    if (p->second)
      w |= p->first;
  return w;
}

int Inode::caps_file_wanted()
{
  int want = 0;
  for (map<int,int>::iterator p = open_by_mode.begin();
       p != open_by_mode.end();
       p++)
    if (p->second)
      want |= ceph_caps_for_mode(p->first);
  return want;
}

int Inode::caps_wanted()
{
  int want = caps_file_wanted() | caps_used();
  if (want & CEPH_CAP_FILE_BUFFER)
    want |= CEPH_CAP_FILE_EXCL;
  return want;
}

int Inode::caps_dirty()
{
  return dirty_caps | flushing_caps;
}

bool Inode::have_valid_size()
{
  // RD+RDCACHE or WR+WRBUFFER => valid size
  if (caps_issued() & (CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_EXCL))
    return true;
  return false;
}

// open Dir for an inode.  if it's not open, allocated it (and pin dentry in memory).
Dir *Inode::open_dir()
{
  if (!dir) {
    dir = new Dir(this);
    lsubdout(cct, mds, 15) << "open_dir " << dir << " on " << this << dendl;
    assert(dn_set.size() < 2); // dirs can't be hard-linked
    if (!dn_set.empty())
      (*dn_set.begin())->get();      // pin dentry
    get();                  // pin inode
  }
  return dir;
}

bool Inode::check_mode(uid_t ruid, gid_t rgid, gid_t *sgids, int sgids_count, uint32_t rflags)
{
  unsigned mflags = rflags & O_ACCMODE;
  unsigned fmode = 0;

  if ((mflags & O_WRONLY) == O_WRONLY)
      fmode |= 2;
  if ((mflags & O_RDONLY) == O_RDONLY)
      fmode |= 4;
  if ((mflags & O_RDWR) == O_RDWR)
      fmode |= 6;

  // if uid is owner, owner entry determines access
  if (uid == ruid) {
    fmode = fmode << 6;
  } else if (gid == rgid) {
    // if a gid or sgid matches the owning group, group entry determines access
    fmode = fmode << 3;
  } else {
    int i = 0;
    for (; i < sgids_count; ++i) {
      if (sgids[i] == gid) {
        fmode = fmode << 3;
	break;
      }
    }
  }

  return (mode & fmode) == fmode;
}
