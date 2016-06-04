// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Client.h"
#include "Inode.h"
#include "Dentry.h"
#include "Dir.h"
#include "MetaSession.h"
#include "ClientSnapRealm.h"
#include "UserGroups.h"

ostream& operator<<(ostream &out, const Inode &in)
{
  out << in.vino() << "("
      << "faked_ino=" << in.faked_ino
      << " ref=" << in._ref
      << " ll_ref=" << in.ll_ref
      << " cap_refs=" << in.cap_refs
      << " open=" << in.open_by_mode
      << " mode=" << oct << in.mode << dec
      << " size=" << in.size << "/" << in.max_size
      << " mtime=" << in.mtime
      << " caps=" << ccap_string(in.caps_issued());
  if (!in.caps.empty()) {
    out << "(";
    for (auto p = in.caps.begin(); p != in.caps.end(); ++p) {
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

  if (in.is_dir() && in.has_dir_layout())
    out << " has_dir_layout";

  if (in.quota.is_enable())
    out << " " << in.quota;

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

int Inode::put_cap_ref(int cap)
{
  // if cap is always a single bit (which it seems to be)
  // all this logic is equivalent to:
  // if (--cap_refs[c]) return false; else return true;
  int last = 0;
  int n = 0;
  while (cap) {
    if (cap & 1) {
      int c = 1 << n;
      if (cap_refs[c] <= 0) {
	lderr(client->cct) << "put_cap_ref " << ccap_string(c) << " went negative on " << *this << dendl;
	assert(cap_refs[c] > 0);
      }
      if (--cap_refs[c] == 0)
        last |= c;
      //cout << "inode " << *this << " put " << cap_string(c) << " " << (cap_refs[c]+1) << " -> " << cap_refs[c] << std::endl;
    }
    cap >>= 1;
    n++;
  }
  return last;
}

bool Inode::is_any_caps()
{
  return !caps.empty() || snap_caps;
}

bool Inode::cap_is_valid(Cap* cap) const
{
  /*cout << "cap_gen     " << cap->session-> cap_gen << std::endl
    << "session gen " << cap->gen << std::endl
    << "cap expire  " << cap->session->cap_ttl << std::endl
    << "cur time    " << ceph_clock_now(cct) << std::endl;*/
  if ((cap->session->cap_gen <= cap->gen)
      && (ceph_clock_now(client->cct) < cap->session->cap_ttl)) {
    return true;
  }
  return false;
}

int Inode::caps_issued(int *implemented) const
{
  int c = snap_caps;
  int i = 0;
  for (map<mds_rank_t,Cap*>::const_iterator it = caps.begin();
       it != caps.end();
       ++it)
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

void Inode::try_touch_cap(mds_rank_t mds)
{
  if (caps.count(mds))
    touch_cap(caps[mds]);
}

bool Inode::caps_issued_mask(unsigned mask)
{
  int c = snap_caps;
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
  for (map<mds_rank_t,Cap*>::iterator it = caps.begin();
       it != caps.end();
       ++it) {
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
    for (map<mds_rank_t,Cap*>::iterator it = caps.begin();
	 it != caps.end();
	 ++it)
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
       ++p)
    if (p->second)
      w |= p->first;
  return w;
}

int Inode::caps_file_wanted()
{
  int want = 0;
  for (map<int,int>::iterator p = open_by_mode.begin();
       p != open_by_mode.end();
       ++p)
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
    lsubdout(client->cct, client, 15) << "open_dir " << dir << " on " << this << dendl;
    assert(dn_set.size() < 2); // dirs can't be hard-linked
    if (!dn_set.empty())
      (*dn_set.begin())->get();      // pin dentry
    get();                  // pin inode
  }
  return dir;
}

bool Inode::check_mode(uid_t ruid, UserGroups& groups, unsigned want)
{
  if (uid == ruid) {
    // if uid is owner, owner entry determines access
    want = want << 6;
  } else if (groups.is_in(gid)) {
    // if a gid or sgid matches the owning group, group entry determines access
    want = want << 3;
  }

  return (mode & want) == want;
}

void Inode::get() {
  _ref++;
  lsubdout(client->cct, client, 15) << "inode.get on " << this << " " <<  ino << '.' << snapid
				    << " now " << _ref << dendl;
}

//private method to put a reference; see Client::put_inode()
int Inode::_put(int n) {
  _ref -= n;
  lsubdout(client->cct, client, 15) << "inode.put on " << this << " " << ino << '.' << snapid
				    << " now " << _ref << dendl;
  assert(_ref >= 0);
  return _ref;
}


void Inode::dump(Formatter *f) const
{
  f->dump_stream("ino") << ino;
  f->dump_stream("snapid") << snapid;
  if (rdev)
    f->dump_unsigned("rdev", rdev);
  f->dump_stream("ctime") << ctime;
  f->dump_stream("mode") << '0' << std::oct << mode << std::dec;
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  f->dump_unsigned("nlink", nlink);

  f->dump_int("size", size);
  f->dump_int("max_size", max_size);
  f->dump_int("truncate_seq", truncate_seq);
  f->dump_int("truncate_size", truncate_size);
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_int("time_warp_seq", time_warp_seq);

  f->dump_object("layout", layout);
  if (is_dir()) {
    f->open_object_section("dir_layout");
    ::dump(dir_layout, f);
    f->close_section();

    /* FIXME when wip-mds-encoding is merged ***
    f->open_object_section("dir_stat");
    dirstat.dump(f);
    f->close_section();

    f->open_object_section("rstat");
    rstat.dump(f);
    f->close_section();
    */
  }

  f->dump_unsigned("version", version);
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_unsigned("flags", flags);

  if (is_dir()) {
    if (!dir_contacts.empty()) {
      f->open_object_section("dir_contants");
      for (set<int>::iterator p = dir_contacts.begin(); p != dir_contacts.end(); ++p)
	f->dump_int("mds", *p);
      f->close_section();
    }
    f->dump_int("dir_hashed", (int)dir_hashed);
    f->dump_int("dir_replicated", (int)dir_replicated);
  }

  f->open_array_section("caps");
  for (map<mds_rank_t,Cap*>::const_iterator p = caps.begin(); p != caps.end(); ++p) {
    f->open_object_section("cap");
    f->dump_int("mds", p->first);
    if (p->second == auth_cap)
      f->dump_int("auth", 1);
    p->second->dump(f);
    f->close_section();
  }
  f->close_section();
  if (auth_cap)
    f->dump_int("auth_cap", auth_cap->session->mds_num);

  f->dump_stream("dirty_caps") << ccap_string(dirty_caps);
  if (flushing_caps) {
    f->dump_stream("flushings_caps") << ccap_string(flushing_caps);
    f->open_object_section("flushing_cap_tid");
    for (map<ceph_tid_t, int>::const_iterator p = flushing_cap_tids.begin();
	 p != flushing_cap_tids.end();
	 ++p) {
      string n(ccap_string(p->second));
      f->dump_unsigned(n.c_str(), p->first);
    }
    f->close_section();
  }
  f->dump_int("shared_gen", shared_gen);
  f->dump_int("cache_gen", cache_gen);
  if (snap_caps) {
    f->dump_int("snap_caps", snap_caps);
    f->dump_int("snap_cap_refs", snap_cap_refs);
  }

  f->dump_stream("hold_caps_until") << hold_caps_until;

  if (snaprealm) {
    f->open_object_section("snaprealm");
    snaprealm->dump(f);
    f->close_section();
  }
  if (!cap_snaps.empty()) {
    for (map<snapid_t,CapSnap*>::const_iterator p = cap_snaps.begin(); p != cap_snaps.end(); ++p) {
      f->open_object_section("cap_snap");
      f->dump_stream("follows") << p->first;
      p->second->dump(f);
      f->close_section();
    }
  }

  // open
  if (!open_by_mode.empty()) {
    f->open_array_section("open_by_mode");
    for (map<int,int>::const_iterator p = open_by_mode.begin(); p != open_by_mode.end(); ++p) {
      f->open_object_section("ref");
      f->dump_unsigned("mode", p->first);
      f->dump_unsigned("refs", p->second);
      f->close_section();
    }
    f->close_section();
  }
  if (!cap_refs.empty()) {
    f->open_array_section("cap_refs");
    for (map<int,int>::const_iterator p = cap_refs.begin(); p != cap_refs.end(); ++p) {
      f->open_object_section("cap_ref");
      f->dump_stream("cap") << ccap_string(p->first);
      f->dump_int("refs", p->second);
      f->close_section();
    }
    f->close_section();
  }

  f->dump_unsigned("reported_size", reported_size);
  if (wanted_max_size != max_size)
    f->dump_unsigned("wanted_max_size", wanted_max_size);
  if (requested_max_size != max_size)
    f->dump_unsigned("requested_max_size", requested_max_size);

  f->dump_int("ref", _ref);
  f->dump_int("ll_ref", ll_ref);

  if (!dn_set.empty()) {
    f->open_array_section("parents");
    for (set<Dentry*>::const_iterator p = dn_set.begin(); p != dn_set.end(); ++p) {
      f->open_object_section("dentry");
      f->dump_stream("dir_ino") << (*p)->dir->parent_inode->ino;
      f->dump_string("name", (*p)->name);
      f->close_section();
    }
    f->close_section();
  }
}

void Cap::dump(Formatter *f) const
{
  f->dump_int("mds", session->mds_num);
  f->dump_stream("ino") << inode->ino;
  f->dump_unsigned("cap_id", cap_id);
  f->dump_stream("issued") << ccap_string(issued);
  if (implemented != issued)
    f->dump_stream("implemented") << ccap_string(implemented);
  f->dump_stream("wanted") << ccap_string(wanted);
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("issue_seq", issue_seq);
  f->dump_unsigned("mseq", mseq);
  f->dump_unsigned("gen", gen);
}

void CapSnap::dump(Formatter *f) const
{
  f->dump_stream("ino") << in->ino;
  f->dump_stream("issued") << ccap_string(issued);
  f->dump_stream("dirty") << ccap_string(dirty);
  f->dump_unsigned("size", size);
  f->dump_stream("ctime") << ctime;
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_int("time_warp_seq", time_warp_seq);
  f->dump_stream("mode") << '0' << std::oct << mode << std::dec;
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  if (!xattrs.empty()) {
    f->open_object_section("xattr_lens");
    for (map<string,bufferptr>::const_iterator p = xattrs.begin(); p != xattrs.end(); ++p)
      f->dump_int(p->first.c_str(), p->second.length());
    f->close_section();
  }
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_int("writing", (int)writing);
  f->dump_int("dirty_data", (int)dirty_data);
  f->dump_unsigned("flush_tid", flush_tid);
}
