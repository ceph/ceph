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



#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"
#include "MDCache.h"
#include "Locker.h"

#include "osdc/Objecter.h"

#include "snap.h"

#include "LogSegment.h"

#include "common/Clock.h"

#include "messages/MLock.h"
#include "messages/MClientCaps.h"

#include <string>
#include <stdio.h>

#include "config.h"

#define DOUT_SUBSYS mds
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "mds" << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") "


boost::pool<> CInode::pool(sizeof(CInode));
boost::pool<> Capability::pool(sizeof(Capability));

LockType CInode::versionlock_type(CEPH_LOCK_IVERSION);
LockType CInode::authlock_type(CEPH_LOCK_IAUTH);
LockType CInode::linklock_type(CEPH_LOCK_ILINK);
LockType CInode::dirfragtreelock_type(CEPH_LOCK_IDFT);
LockType CInode::filelock_type(CEPH_LOCK_IFILE);
LockType CInode::xattrlock_type(CEPH_LOCK_IXATTR);
LockType CInode::snaplock_type(CEPH_LOCK_ISNAP);
LockType CInode::nestlock_type(CEPH_LOCK_INEST);

//int cinode_pins[CINODE_NUM_PINS];  // counts
ostream& CInode::print_db_line_prefix(ostream& out)
{
  return out << g_clock.now() << " mds" << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") ";
}



ostream& operator<<(ostream& out, CInode& in)
{
  string path;
  in.make_path_string_projected(path);

  out << "[inode " << in.inode.ino;
  out << " [" 
      << (in.is_multiversion() ? "...":"")
      << in.first << "," << in.last << "]";
  out << " " << path << (in.is_dir() ? "/":"");

  if (in.is_auth()) {
    out << " auth";
    if (in.is_replicated()) 
      out << in.get_replicas();
  } else {
    pair<int,int> a = in.authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
    out << "." << in.get_replica_nonce();
  }

  if (in.is_symlink())
    out << " symlink='" << in.symlink << "'";
  if (in.is_dir() && !in.dirfragtree.empty())
    out << " " << in.dirfragtree;
  
  out << " v" << in.get_version();
  if (in.get_projected_version() > in.get_version())
    out << " pv" << in.get_projected_version();

  if (in.is_auth_pinned()) {
    out << " ap=" << in.get_num_auth_pins();
#ifdef MDS_AUTHPIN_SET
    out << "(" << in.auth_pin_set << ")";
#endif
  }

  if (in.snaprealm)
    out << " snaprealm=" << in.snaprealm;

  if (in.state_test(CInode::STATE_AMBIGUOUSAUTH)) out << " AMBIGAUTH";
  if (in.state_test(CInode::STATE_NEEDSRECOVER)) out << " needsrecover";
  if (in.state_test(CInode::STATE_RECOVERING)) out << " recovering";
  if (in.state_test(CInode::STATE_DIRTYPARENT)) out << " dirtyparent";
  if (in.is_freezing_inode()) out << " FREEZING=" << in.auth_pin_freeze_allowance;
  if (in.is_frozen_inode()) out << " FROZEN";

  if (in.get_nested_anchors())
    out << " na=" << in.get_nested_anchors();

  if (in.inode.is_dir()) {
    out << " " << in.inode.dirstat;
    out << " ds=" << in.inode.dirstat.size() << "=" 
	<< in.inode.dirstat.nfiles << "+" << in.inode.dirstat.nsubdirs;
    //if (in.inode.dirstat.version > 10000) out << " BADDIRSTAT";
  } else {
    out << " s=" << in.inode.size;
    out << " nl=" << in.inode.nlink;
  }

  out << " rb=" << in.inode.rstat.rbytes;
  if (in.inode.rstat.rbytes != in.inode.accounted_rstat.rbytes)
    out << "/" << in.inode.accounted_rstat.rbytes;
  if (in.is_projected()) 
    out << "(" << in.get_projected_inode()->rstat.rbytes
	<< "/" << in.get_projected_inode()->accounted_rstat.rbytes << ")";

  out << " rf=" << in.inode.rstat.rfiles;
  if (in.inode.rstat.rfiles != in.inode.accounted_rstat.rfiles)
    out << "/" << in.inode.accounted_rstat.rfiles;
  if (in.is_projected()) 
    out << "(" << in.get_projected_inode()->rstat.rfiles
	<< "/" << in.get_projected_inode()->accounted_rstat.rfiles << ")";

  out << " rd=" << in.inode.rstat.rsubdirs;
  if (in.inode.rstat.rsubdirs != in.inode.accounted_rstat.rsubdirs)
    out << "/" << in.inode.accounted_rstat.rsubdirs;
  if (in.is_projected()) 
    out << "(" << in.get_projected_inode()->rstat.rsubdirs
	<< "/" << in.get_projected_inode()->accounted_rstat.rsubdirs << ")";

  // locks
  out << " " << in.authlock;
  out << " " << in.linklock;
  if (in.inode.is_dir()) {
    out << " " << in.dirfragtreelock;
    out << " " << in.snaplock;
    out << " " << in.nestlock;
  }
  out << " " << in.filelock;
  out << " " << in.xattrlock;
  out << " " << in.versionlock;

  // hack: spit out crap on which clients have caps
  if (in.inode.client_ranges.size())
    out << " cr=" << in.inode.client_ranges;

  if (!in.get_client_caps().empty()) {
    out << " caps={";
    for (map<client_t,Capability*>::iterator it = in.get_client_caps().begin();
         it != in.get_client_caps().end();
         it++) {
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

  if (in.get_num_ref()) {
    out << " |";
    in.print_pin_set(out);
  }

  out << " " << &in;
  out << "]";
  return out;
}


void CInode::print(ostream& out)
{
  out << *this;
}


inode_t *CInode::project_inode(map<string,bufferptr> *px) 
{
  if (projected_inode.empty()) {
    projected_inode.push_back(new inode_t(inode));
    if (px)
      *px = xattrs;
  } else {
    projected_inode.push_back(new inode_t(*projected_inode.back()));
    if (px)
      *px = *get_projected_xattrs();
  }
  projected_xattrs.push_back(px);
  dout(15) << "project_inode " << projected_inode.back() << dendl;
  return projected_inode.back();
}

void CInode::pop_and_dirty_projected_inode(LogSegment *ls) 
{
  assert(!projected_inode.empty());
  dout(15) << "pop_and_dirty_projected_inode " << projected_inode.front()
	   << " v" << projected_inode.front()->version << dendl;
  mark_dirty(projected_inode.front()->version, ls);
  inode = *projected_inode.front();
  delete projected_inode.front();

  map<string,bufferptr> *px = projected_xattrs.front();
  if (px) {
    xattrs = *px;
    delete px;
  }

  projected_inode.pop_front();
  projected_xattrs.pop_front();
}


// ====== CInode =======

// dirfrags

frag_t CInode::pick_dirfrag(const nstring& dn)
{
  if (dirfragtree.empty())
    return frag_t();          // avoid the string hash if we can.

  __u32 h = ceph_str_hash_linux(dn.data(), dn.length());
  return dirfragtree[h];
}

void CInode::get_dirfrags_under(frag_t fg, list<CDir*>& ls)
{
  list<frag_t> fglist;
  dirfragtree.get_leaves_under(fg, fglist);
  for (list<frag_t>::iterator p = fglist.begin();
       p != fglist.end();
       ++p) 
    if (dirfrags.count(*p))
      ls.push_back(dirfrags[*p]);
}

CDir *CInode::get_approx_dirfrag(frag_t fg)
{
  CDir *dir = get_dirfrag(fg);
  if (dir) return dir;

  // find a child?
  list<CDir*> ls;
  get_dirfrags_under(fg, ls);
  if (!ls.empty()) 
    return ls.front();

  // try parents?
  while (1) {
    fg = fg.parent();
    dir = get_dirfrag(fg);
    if (dir) return dir;
  }
}	

void CInode::get_dirfrags(list<CDir*>& ls) 
{
  // all dirfrags
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    ls.push_back(p->second);
}
void CInode::get_nested_dirfrags(list<CDir*>& ls) 
{  
  // dirfrags in same subtree
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (!p->second->is_subtree_root())
      ls.push_back(p->second);
}
void CInode::get_subtree_dirfrags(list<CDir*>& ls) 
{ 
  // dirfrags that are roots of new subtrees
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (p->second->is_subtree_root())
      ls.push_back(p->second);
}


CDir *CInode::get_or_open_dirfrag(MDCache *mdcache, frag_t fg)
{
  assert(is_dir());

  // have it?
  CDir *dir = get_dirfrag(fg);
  if (!dir) {
    // create it.
    assert(is_auth());
    dir = new CDir(this, fg, mdcache, true);
    add_dirfrag(dir);
  }
  return dir;
}

CDir *CInode::add_dirfrag(CDir *dir)
{
  assert(dirfrags.count(dir->dirfrag().frag) == 0);
  dirfrags[dir->dirfrag().frag] = dir;

  if (stickydir_ref > 0) {
    dir->state_set(CDir::STATE_STICKY);
    dir->get(CDir::PIN_STICKY);
  }

  return dir;
}

void CInode::close_dirfrag(frag_t fg)
{
  dout(14) << "close_dirfrag " << fg << dendl;
  assert(dirfrags.count(fg));
  
  CDir *dir = dirfrags[fg];
  dir->remove_null_dentries();
  
  // clear dirty flag
  if (dir->is_dirty())
    dir->mark_clean();
  
  if (stickydir_ref > 0) {
    dir->state_clear(CDir::STATE_STICKY);
    dir->put(CDir::PIN_STICKY);
  }
  
  // dump any remaining dentries, for debugging purposes
  for (CDir::map_t::iterator p = dir->items.begin();
       p != dir->items.end();
       ++p) 
    dout(14) << "close_dirfrag LEFTOVER dn " << *p->second << dendl;

  assert(dir->get_num_ref() == 0);
  delete dir;
  dirfrags.erase(fg);
}

void CInode::close_dirfrags()
{
  while (!dirfrags.empty()) 
    close_dirfrag(dirfrags.begin()->first);
}

bool CInode::has_subtree_root_dirfrag()
{
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (p->second->is_subtree_root())
      return true;
  return false;
}


void CInode::get_stickydirs()
{
  if (stickydir_ref == 0) {
    get(PIN_STICKYDIRS);
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      p->second->state_set(CDir::STATE_STICKY);
      p->second->get(CDir::PIN_STICKY);
    }
  }
  stickydir_ref++;
}

void CInode::put_stickydirs()
{
  assert(stickydir_ref > 0);
  stickydir_ref--;
  if (stickydir_ref == 0) {
    put(PIN_STICKYDIRS);
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      p->second->state_clear(CDir::STATE_STICKY);
      p->second->put(CDir::PIN_STICKY);
    }
  }
}





// pins

void CInode::first_get()
{
  // pin my dentry?
  if (parent) 
    parent->get(CDentry::PIN_INODEPIN);
}

void CInode::last_put() 
{
  // unpin my dentry?
  if (parent) 
    parent->put(CDentry::PIN_INODEPIN);
}

void CInode::add_remote_parent(CDentry *p) 
{
  if (remote_parents.empty())
    get(PIN_REMOTEPARENT);
  remote_parents.insert(p);
}
void CInode::remove_remote_parent(CDentry *p) 
{
  remote_parents.erase(p);
  if (remote_parents.empty())
    put(PIN_REMOTEPARENT);
}




CDir *CInode::get_parent_dir()
{
  if (parent)
    return parent->dir;
  return NULL;
}
CInode *CInode::get_parent_inode() 
{
  if (parent) 
    return parent->dir->inode;
  return NULL;
}

bool CInode::is_projected_ancestor_of(CInode *other)
{
  while (other) {
    if (other == this)
      return true;
    if (!other->get_projected_parent_dn())
      break;
    other = other->get_projected_parent_dn()->get_dir()->get_inode();
  }
  return false;
}

void CInode::make_path_string(string& s, bool force, CDentry *use_parent)
{
  if (!force)
    use_parent = parent;

  if (use_parent) {
    use_parent->make_path_string(s);
  } 
  else if (is_root()) {
    s = "";  // root
  } 
  else if (is_mdsdir()) {
    char t[30];
    sprintf(t, "~mds%d", (int)ino() - MDS_INO_MDSDIR_OFFSET);
    s = t;
  }
  else {
    char n[20];
    snprintf(n, sizeof(n), "#%llx", (unsigned long long)(ino()));
    s += n;
  }
}
void CInode::make_path_string_projected(string& s)
{
  make_path_string(s);
  
  if (projected_parent.size()) {
    string q;
    q.swap(s);
    s = "{" + q;
    for (list<CDentry*>::iterator p = projected_parent.begin();
	 p != projected_parent.end();
	 p++) {
      string q;
      make_path_string(q, true, *p);
      s += " ";
      s += q;
    }
    s += "}";
  }
}

void CInode::make_path(filepath& fp)
{
  if (parent) 
    parent->make_path(fp);
  else
    fp = filepath(ino());
}

void CInode::make_anchor_trace(vector<Anchor>& trace)
{
  if (get_projected_parent_dn())
    get_projected_parent_dn()->make_anchor_trace(trace, this);
  else 
    assert(is_base());
}

void CInode::name_stray_dentry(string& dname)
{
  char s[20];
  snprintf(s, sizeof(s), "%llx", (unsigned long long)inode.ino.val);
  dname = s;
}


Capability *CInode::add_client_cap(client_t client, Session *session, SnapRealm *conrealm)
{
  if (client_caps.empty()) {
    get(PIN_CAPS);
    if (conrealm)
      containing_realm = conrealm;
    else
      containing_realm = find_snaprealm();
    containing_realm->inodes_with_caps.push_back(&item_caps);
    dout(10) << "add_client_cap first cap, joining realm " << *containing_realm << dendl;
  }

  mdcache->num_caps++;
  if (client_caps.empty())
    mdcache->num_inodes_with_caps++;
  
  assert(client_caps.count(client) == 0);
  Capability *cap = client_caps[client] = new Capability(this, ++mdcache->last_cap_id, client);
  if (session)
    session->add_cap(cap);
  
  cap->client_follows = first-1;
  
  containing_realm->add_cap(client, cap);
  
  return cap;
}

void CInode::remove_client_cap(client_t client)
{
  assert(client_caps.count(client) == 1);
  Capability *cap = client_caps[client];
  
  cap->item_session_caps.remove_myself();
  containing_realm->remove_cap(client, cap);
  
  if (client == loner_cap)
    loner_cap = -1;

  delete cap;
  client_caps.erase(client);
  if (client_caps.empty()) {
    dout(10) << "remove_client_cap last cap, leaving realm " << *containing_realm << dendl;
    put(PIN_CAPS);
    item_caps.remove_myself();
    containing_realm = NULL;
    item_open_file.remove_myself();  // unpin logsegment
    mdcache->num_inodes_with_caps--;
  }
  mdcache->num_caps--;
}

void CInode::move_to_realm(SnapRealm *realm)
{
  dout(10) << "move_to_realm joining realm " << *realm
	   << ", leaving realm " << *containing_realm << dendl;
  for (map<client_t,Capability*>::iterator q = client_caps.begin();
       q != client_caps.end();
       q++) {
    containing_realm->remove_cap(q->first, q->second);
    realm->add_cap(q->first, q->second);
  }
  item_caps.remove_myself();
  realm->inodes_with_caps.push_back(&item_caps);
  containing_realm = realm;
}


version_t CInode::pre_dirty()
{
  version_t pv; 
  if (parent || projected_parent.size()) {
    pv = get_projected_parent_dn()->pre_dirty(get_projected_version());
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
  
  // move myself to this segment's dirty list
  if (ls) 
    ls->dirty_inodes.push_back(&item_dirty);
}

void CInode::mark_dirty(version_t pv, LogSegment *ls) {
  
  dout(10) << "mark_dirty " << *this << dendl;

  /*
    NOTE: I may already be dirty, but this fn _still_ needs to be called so that
    the directory is (perhaps newly) dirtied, and so that parent_dir_version is 
    updated below.
  */
  
  // only auth can get dirty.  "dirty" async data in replicas is relative to
  // filelock state, not the dirty flag.
  assert(is_auth());
  
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
    item_dirty.remove_myself();
  }
}    


// --------------
// per-inode storage
// (currently for root inode only)

struct C_Inode_Stored : public Context {
  CInode *in;
  version_t version;
  Context *fin;
  C_Inode_Stored(CInode *i, version_t v, Context *f) : in(i), version(v), fin(f) {}
  void finish(int r) {
    in->_stored(version, fin);
  }
};

void CInode::store(Context *fin)
{
  dout(10) << "store " << get_version() << dendl;
  assert(is_base());

  bufferlist bl;
  nstring magic = CEPH_FS_ONDISK_MAGIC;
  ::encode(magic, bl);
  encode_store(bl);

  // write it.
  SnapContext snapc;
  ObjectOperation m;
  m.setxattr("inode", bl);

  char n[30];
  snprintf(n, sizeof(n), "%llx.%08llx", (long long unsigned)ino(), (long long unsigned)frag_t());
  object_t oid(n);
  OSDMap *osdmap = mdcache->mds->objecter->osdmap;
  ceph_object_layout ol = osdmap->make_object_layout(oid,
						     mdcache->mds->mdsmap->get_metadata_pg_pool());

  mdcache->mds->objecter->mutate(oid, ol, m, snapc, g_clock.now(), 0,
				 NULL, new C_Inode_Stored(this, get_version(), fin) );
}

void CInode::_stored(version_t v, Context *fin)
{
  dout(10) << "_stored " << v << " " << *this << dendl;
  if (v == get_projected_version())
    mark_clean();

  fin->finish(0);
  delete fin;
}

struct C_Inode_Fetched : public Context {
  CInode *in;
  bufferlist bl;
  Context *fin;
  C_Inode_Fetched(CInode *i, Context *f) : in(i), fin(f) {}
  void finish(int r) {
    in->_fetched(bl, fin);
  }
};

void CInode::fetch(Context *fin)
{
  dout(10) << "fetch" << dendl;

  C_Inode_Fetched *c = new C_Inode_Fetched(this, fin);
  char n[30];
  snprintf(n, sizeof(n), "%llx.%08llx", (long long unsigned)ino(), (long long unsigned)frag_t());
  object_t oid(n);

  ObjectOperation rd;
  rd.getxattr("inode");

  OSDMap *osdmap = mdcache->mds->objecter->osdmap;
  ceph_object_layout ol = osdmap->make_object_layout(oid,
						     mdcache->mds->mdsmap->get_metadata_pg_pool());

  mdcache->mds->objecter->read(oid, ol, rd, CEPH_NOSNAP, &c->bl, 0, c );
}

void CInode::_fetched(bufferlist& bl, Context *fin)
{
  dout(10) << "_fetched" << dendl;
  bufferlist::iterator p = bl.begin();
  nstring magic;
  ::decode(magic, p);
  dout(10) << " magic is '" << magic << "' (expecting '" << CEPH_FS_ONDISK_MAGIC << "')" << dendl;
  if (magic != CEPH_FS_ONDISK_MAGIC) {
    dout(0) << "on disk magic '" << magic << "' != my magic '" << CEPH_FS_ONDISK_MAGIC
	    << "'" << dendl;
    fin->finish(-EINVAL);
  } else {
    decode_store(p);
    dout(10) << "_fetched " << *this << dendl;
    fin->finish(0);
  }
  delete fin;
}



// ------------------
// parent dir

struct C_Inode_StoredParent : public Context {
  CInode *in;
  version_t version;
  Context *fin;
  C_Inode_StoredParent(CInode *i, version_t v, Context *f) : in(i), version(v), fin(f) {}
  void finish(int r) {
    in->_stored_parent(version, fin);
  }
};

void CInode::encode_parent_mutation(ObjectOperation& m)
{
  string path;
  make_path_string(path);
  m.setxattr("path", path);

  CDentry *pdn = get_parent_dn();
  if (pdn) {
    bufferlist parent(32 + pdn->name.length());
    uint64_t ino = pdn->get_dir()->get_inode()->ino();
    __u8 v = 1;
    ::encode(v, parent);
    ::encode(inode.version, parent);
    ::encode(ino, parent);
    ::encode(pdn->name, parent);
    m.setxattr("parent", parent);
  }
}

void CInode::store_parent(Context *fin)
{
  dout(10) << "store_parent" << dendl;
  
  ObjectOperation m;
  encode_parent_mutation(m);

  // write it.
  SnapContext snapc;

  char n[30];
  snprintf(n, sizeof(n), "%llx.%08llx", (long long unsigned)ino(), (long long unsigned)frag_t());
  object_t oid(n);
  OSDMap *osdmap = mdcache->mds->objecter->osdmap;
  ceph_object_layout ol = osdmap->make_object_layout(oid,
						     mdcache->mds->mdsmap->get_metadata_pg_pool());

  mdcache->mds->objecter->mutate(oid, ol, m, snapc, g_clock.now(), 0,
				 NULL, new C_Inode_StoredParent(this, inode.last_renamed_version, fin) );

}

void CInode::_stored_parent(version_t v, Context *fin)
{
  if (v == inode.last_renamed_version) {
    dout(10) << "stored_parent committed v" << v << ", removing from list" << dendl;
    item_renamed_file.remove_myself();
    state_clear(STATE_DIRTYPARENT);
  } else {
    dout(10) << "stored_parent committed v" << v << " < " << inode.last_renamed_version
	     << ", renamed again, not removing from list" << dendl;
  }
  if (fin) {
    fin->finish(0);
    delete fin;
  }
}


// ------------------
// locking

void CInode::set_object_info(MDSCacheObjectInfo &info)
{
  info.ino = ino();
  info.snapid = last;
}

void CInode::encode_lock_state(int type, bufferlist& bl)
{
  ::encode(first, bl);

  switch (type) {
  case CEPH_LOCK_IAUTH:
    ::encode(inode.ctime, bl);
    ::encode(inode.mode, bl);
    ::encode(inode.uid, bl);
    ::encode(inode.gid, bl);  
    break;
    
  case CEPH_LOCK_ILINK:
    ::encode(inode.ctime, bl);
    ::encode(inode.nlink, bl);
    ::encode(inode.anchored, bl);
    break;
    
  case CEPH_LOCK_IDFT:
    {
      // encode the raw tree
      ::encode(dirfragtree, bl);

      // also specify which frags are mine
      set<frag_t> myfrags;
      list<CDir*> dfls;
      get_dirfrags(dfls);
      for (list<CDir*>::iterator p = dfls.begin(); p != dfls.end(); ++p) 
	if ((*p)->is_auth()) {
	  frag_t fg = (*p)->get_frag();
	  myfrags.insert(fg);
	}
      ::encode(myfrags, bl);
    }
    break;
    
  case CEPH_LOCK_IFILE:
    if (is_auth()) {
      ::encode(inode.layout, bl);
      ::encode(inode.size, bl);
      ::encode(inode.mtime, bl);
      ::encode(inode.atime, bl);
      ::encode(inode.time_warp_seq, bl);
      ::encode(inode.client_ranges, bl);
    }

    {
      dout(15) << "encode_lock_state inode.dirstat is " << inode.dirstat << dendl;
      ::encode(inode.dirstat, bl);  // only meaningful if i am auth.
      bufferlist tmp;
      __u32 n = 0;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	if (is_auth() || dir->is_auth()) {
	  dout(15) << fg << " " << *dir << dendl;
	  dout(20) << fg << "           fragstat " << dir->fnode.fragstat << dendl;
	  dout(20) << fg << " accounted_fragstat " << dir->fnode.accounted_fragstat << dendl;
	  ::encode(fg, tmp);
	  ::encode(dir->first, tmp);
	  ::encode(dir->fnode.fragstat, tmp);
	  ::encode(dir->fnode.accounted_fragstat, tmp);
	  n++;
	}
      }
      ::encode(n, bl);
      bl.claim_append(tmp);
    }
    break;

  case CEPH_LOCK_INEST:
    {
      dout(15) << "encode_lock_state inode.rstat is " << inode.rstat << dendl;
      ::encode(inode.rstat, bl);  // only meaningful if i am auth.
      bufferlist tmp;
      __u32 n = 0;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	if (is_auth() || dir->is_auth()) {
	  dout(10) << fg << " " << *dir << dendl;
	  dout(10) << fg << " " << dir->fnode.rstat << dendl;
	  dout(10) << fg << " " << dir->fnode.rstat << dendl;
	  dout(10) << fg << " " << dir->dirty_old_rstat << dendl;
	  ::encode(fg, tmp);
	  ::encode(dir->first, tmp);
	  ::encode(dir->fnode.rstat, tmp);
	  ::encode(dir->fnode.accounted_rstat, tmp);
	  ::encode(dir->dirty_old_rstat, tmp);
	  n++;
	}
      }
      ::encode(n, bl);
      bl.claim_append(tmp);
    }
    break;
    
  case CEPH_LOCK_IXATTR:
    ::encode(xattrs, bl);
    break;

  case CEPH_LOCK_ISNAP:
    encode_snap(bl);
    break;

  
  default:
    assert(0);
  }
}

void CInode::decode_lock_state(int type, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  utime_t tm;

  snapid_t newfirst;
  ::decode(newfirst, p);

  if (!is_auth() && newfirst != first) {
    dout(10) << "decode_lock_state first " << first << " -> " << newfirst << dendl;
    assert(newfirst > first);
    first = newfirst;
  }

  switch (type) {
  case CEPH_LOCK_IAUTH:
    ::decode(tm, p);
    if (inode.ctime < tm) inode.ctime = tm;
    ::decode(inode.mode, p);
    ::decode(inode.uid, p);
    ::decode(inode.gid, p);
    break;

  case CEPH_LOCK_ILINK:
    ::decode(tm, p);
    if (inode.ctime < tm) inode.ctime = tm;
    ::decode(inode.nlink, p);
    {
      bool was_anchored = inode.anchored;
      ::decode(inode.anchored, p);
      if (parent && was_anchored != inode.anchored)
	parent->adjust_nested_anchors((int)inode.anchored - (int)was_anchored);
    }
    break;

  case CEPH_LOCK_IDFT:
    {
      fragtree_t temp;
      ::decode(temp, p);
      set<frag_t> authfrags;
      ::decode(authfrags, p);
      if (is_auth()) {
	// auth.  believe replica's auth frags only.
	for (set<frag_t>::iterator p = authfrags.begin(); p != authfrags.end(); ++p)
	  if (!dirfragtree.is_leaf(*p)) {
	    dout(10) << " forcing frag " << *p << " to leaf (split|merge)" << dendl;
	    dirfragtree.force_to_leaf(*p);
	    dirfragtreelock.mark_dirty();  // ok bc we're auth and caller will handle
	  }
      } else {
	// replica.  take the new tree, BUT make sure any open
	//  dirfrags remain leaves (they may have split _after_ this
	//  dft was scattered, or we may still be be waiting on the
	//  notify from the auth)
	dirfragtree.swap(temp);
	for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	     p != dirfrags.end();
	     p++)
	  if (!dirfragtree.is_leaf(p->first)) {
	    dout(10) << " forcing open dirfrag " << p->first << " to leaf (racing with split|merge)" << dendl;
	    dirfragtree.force_to_leaf(p->first);
	  }
      }
    }
    break;

  case CEPH_LOCK_IFILE:
    if (!is_auth()) {
      ::decode(inode.layout, p);
      ::decode(inode.size, p);
      ::decode(inode.mtime, p);
      ::decode(inode.atime, p);
      ::decode(inode.time_warp_seq, p);
      ::decode(inode.client_ranges, p);
    }

    {
      frag_info_t dirstat;
      ::decode(dirstat, p);
      if (!is_auth()) {
	dout(10) << " taking inode dirstat " << dirstat << " for " << *this << dendl;
	inode.dirstat = dirstat;    // take inode summation if replica
      }
      __u32 n;
      ::decode(n, p);
      dout(10) << " ...got " << n << " fragstats on " << *this << dendl;
      while (n--) {
	frag_t fg;
	snapid_t fgfirst;
	frag_info_t fragstat;
	frag_info_t accounted_fragstat;
	::decode(fg, p);
	::decode(fgfirst, p);
	::decode(fragstat, p);
	::decode(accounted_fragstat, p);
	dout(10) << fg << " [" << fgfirst << ",head] " << dendl;
	dout(10) << fg << "           fragstat " << fragstat << dendl;
	dout(20) << fg << " accounted_fragstat " << accounted_fragstat << dendl;

	CDir *dir = get_dirfrag(fg);
	if (is_auth()) {
	  assert(dir);                // i am auth; i had better have this dir open
	  dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		   << " on " << *dir << dendl;
	  dir->first = fgfirst;
	  dir->fnode.fragstat = fragstat;
	  dir->fnode.accounted_fragstat = accounted_fragstat;
	  dir->first = fgfirst;
	  if (!(fragstat == accounted_fragstat)) {
	    dout(10) << fg << " setting filelock updated flag" << dendl;
	    filelock.mark_dirty();  // ok bc we're auth and caller will handle
	  }
	} else {
	  if (dir && dir->is_auth()) {
	    dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		     << " on " << *dir << dendl;
	    dir->first = fgfirst;

	    dout(10) << fg << " setting accounted_fragstat and setting dirty bit" << dendl;
	    fnode_t *pf = dir->get_projected_fnode();
	    pf->accounted_fragstat = fragstat;
	    pf->fragstat.version = fragstat.version;
	    assert(pf->fragstat == fragstat);
	    dir->_set_dirty_flag();	    // bit of a hack
	  }
	}
      }
    }
    break;

  case CEPH_LOCK_INEST:
    {
      nest_info_t rstat;
      ::decode(rstat, p);
      if (!is_auth()) {
	dout(10) << " taking inode rstat " << rstat << " for " << *this << dendl;
	inode.rstat = rstat;    // take inode summation if replica
      }
      __u32 n;
      ::decode(n, p);
      while (n--) {
	frag_t fg;
	snapid_t fgfirst;
	nest_info_t rstat;
	nest_info_t accounted_rstat;
	map<snapid_t,old_rstat_t> dirty_old_rstat;
	::decode(fg, p);
	::decode(fgfirst, p);
	::decode(rstat, p);
	::decode(accounted_rstat, p);
	::decode(dirty_old_rstat, p);
	dout(10) << fg << " [" << fgfirst << ",head]" << dendl;
	dout(10) << fg << "               rstat " << rstat << dendl;
	dout(10) << fg << "     accounted_rstat " << accounted_rstat << dendl;
	dout(10) << fg << "     dirty_old_rstat " << dirty_old_rstat << dendl;

	CDir *dir = get_dirfrag(fg);
	if (is_auth()) {
	  assert(dir);                // i am auth; i had better have this dir open
	  dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		   << " on " << *dir << dendl;
	  dir->first = fgfirst;
	  dir->fnode.rstat = rstat;
	  dir->fnode.accounted_rstat = accounted_rstat;
	  dir->dirty_old_rstat.swap(dirty_old_rstat);
	  if (!(rstat == accounted_rstat) || dir->dirty_old_rstat.size()) {
	    dout(10) << fg << " setting nestlock updated flag" << dendl;
	    nestlock.mark_dirty();  // ok bc we're auth and caller will handle
	  }
	} else {
	  if (dir && dir->is_auth()) {
	    dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		     << " on " << *dir << dendl;
	    dir->first = fgfirst;
	    
	    dout(10) << fg << " resetting accounted_rstat and setting dirty bit" << dendl;
	    fnode_t *pf = dir->get_projected_fnode();
	    pf->accounted_rstat = rstat;
	    pf->rstat.version = rstat.version;
	    dir->dirty_old_rstat.clear();
	    dir->_set_dirty_flag();	    // bit of a hack, FIXME?
	  }
	}
      }
    }
    break;

  case CEPH_LOCK_IXATTR:
    ::decode(xattrs, p);
    break;

  case CEPH_LOCK_ISNAP:
    {
      snapid_t seq = 0;
      if (snaprealm)
	seq = snaprealm->seq;
      decode_snap(p);
      if (snaprealm && snaprealm->seq != seq)
	mdcache->do_realm_invalidate_and_update_notify(this, seq ? CEPH_SNAP_OP_UPDATE:CEPH_SNAP_OP_SPLIT);
    }
    break;

  default:
    assert(0);
  }
}

void CInode::clear_dirty_scattered(int type)
{
  dout(10) << "clear_dirty_scattered " << type << " on " << *this << dendl;
  switch (type) {
  case CEPH_LOCK_IFILE:
    item_dirty_dirfrag_dir.remove_myself();
    break;

  case CEPH_LOCK_INEST:
    item_dirty_dirfrag_nest.remove_myself();
    break;

  case CEPH_LOCK_IDFT:
    item_dirty_dirfrag_dirfragtree.remove_myself();
    break;

  default:
    assert(0);
  }
}


void CInode::finish_scatter_gather_update(int type)
{
  dout(10) << "finish_scatter_gather_update " << type << " on " << *this << dendl;
  assert(is_auth());

  switch (type) {
  case CEPH_LOCK_IFILE:
    {
      // adjust summation
      assert(is_auth());
      inode_t *pi = get_projected_inode();

      bool touched_mtime = false;
      dout(20) << "  orig dirstat " << pi->dirstat << dendl;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   p++) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;
	fnode_t *pf = dir->get_projected_fnode();
	if (pf->accounted_fragstat.version == pi->dirstat.version) {
	  dout(20) << fg << "           fragstat " << pf->fragstat << dendl;
	  dout(20) << fg << " accounted_fragstat " << pf->accounted_fragstat << dendl;
	  pi->dirstat.take_diff(pf->fragstat, pf->accounted_fragstat, touched_mtime);
	} else {
	  dout(20) << fg << " skipping OLD accounted_fragstat " << pf->accounted_fragstat << dendl;
	  pf->accounted_fragstat = pf->fragstat;
	}
	pf->fragstat.version = pf->accounted_fragstat.version = pi->dirstat.version + 1;
      }
      if (touched_mtime)
	pi->mtime = pi->ctime = pi->dirstat.mtime;
      pi->dirstat.version++;
      dout(20) << " final dirstat " << pi->dirstat << dendl;
      assert(pi->dirstat.size() >= 0);
      assert(pi->dirstat.nfiles >= 0);
      assert(pi->dirstat.nsubdirs >= 0);
    }
    break;

  case CEPH_LOCK_INEST:
    {
      // adjust summation
      assert(is_auth());
      inode_t *pi = get_projected_inode();
      dout(20) << "  orig rstat " << pi->rstat << dendl;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   p++) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;
	fnode_t *pf = dir->get_projected_fnode();
	if (pf->accounted_rstat.version == pi->rstat.version) {
	  dout(20) << fg << "           rstat " << pf->rstat << dendl;
	  dout(20) << fg << " accounted_rstat " << pf->accounted_rstat << dendl;
	  dout(20) << fg << " dirty_old_rstat " << dir->dirty_old_rstat << dendl;
	  mdcache->project_rstat_frag_to_inode(pf->rstat, pf->accounted_rstat,
					       dir->first, CEPH_NOSNAP, this, true);
	  for (map<snapid_t,old_rstat_t>::iterator q = dir->dirty_old_rstat.begin();
	       q != dir->dirty_old_rstat.end();
	       q++)
	    mdcache->project_rstat_frag_to_inode(q->second.rstat, q->second.accounted_rstat,
						 q->second.first, q->first, this, true);
	} else {
	  dout(20) << fg << " skipping OLD accounted_rstat " << pf->accounted_rstat << dendl;
	  pf->accounted_rstat = pf->rstat;
	}
	dir->dirty_old_rstat.clear();
	pf->rstat.version = pf->accounted_rstat.version = pi->rstat.version + 1;
      }
      pi->rstat.version++;
      dout(20) << " final rstat " << pi->rstat << dendl;

      //assert(pi->rstat.rfiles >= 0);
      if (pi->rstat.rfiles < 0) {
	stringstream ss;
	ss << "rfiles underflow " << pi->rstat.rfiles << " on " << *this;
	mdcache->mds->logclient.log(LOG_ERROR, ss);
	pi->rstat.rfiles = 0;
      }

      //assert(pi->rstat.rsubdirs >= 0);
      if (pi->rstat.rsubdirs < 0) {
	stringstream ss;
	ss << "rsubdirs underflow " << pi->rstat.rfiles << " on " << *this;
	mdcache->mds->logclient.log(LOG_ERROR, ss);
	pi->rstat.rsubdirs = 0;
      }
    }
    break;

  case CEPH_LOCK_IDFT:
    break;

  default:
    assert(0);
  }
}


// waiting

bool CInode::is_frozen()
{
  if (is_frozen_inode()) return true;
  if (parent && parent->dir->is_frozen()) return true;
  return false;
}

bool CInode::is_frozen_dir()
{
  if (parent && parent->dir->is_frozen_dir()) return true;
  return false;
}

bool CInode::is_freezing()
{
  if (is_freezing_inode()) return true;
  if (parent && parent->dir->is_freezing()) return true;
  return false;
}

void CInode::add_waiter(uint64_t tag, Context *c) 
{
  dout(10) << "add_waiter tag " << tag 
	   << " !ambig " << !state_test(STATE_AMBIGUOUSAUTH)
	   << " !frozen " << !is_frozen_inode()
	   << " !freezing " << !is_freezing_inode()
	   << dendl;
  // wait on the directory?
  //  make sure its not the inode that is explicitly ambiguous|freezing|frozen
  if (((tag & WAIT_SINGLEAUTH) && !state_test(STATE_AMBIGUOUSAUTH)) ||
      ((tag & WAIT_UNFREEZE) && !is_frozen_inode() && !is_freezing_inode())) {
    parent->dir->add_waiter(tag, c);
    return;
  }
  MDSCacheObject::add_waiter(tag, c);
}

bool CInode::freeze_inode(int auth_pin_allowance)
{
  assert(auth_pin_allowance > 0);  // otherwise we need to adjust parent's nested_auth_pins
  assert(auth_pins >= auth_pin_allowance);
  if (auth_pins > auth_pin_allowance) {
    dout(10) << "freeze_inode - waiting for auth_pins to drop to " << auth_pin_allowance << dendl;
    auth_pin_freeze_allowance = auth_pin_allowance;
    get(PIN_FREEZING);
    state_set(STATE_FREEZING);
    return false;
  }

  dout(10) << "freeze_inode - frozen" << dendl;
  assert(auth_pins == auth_pin_allowance);
  get(PIN_FROZEN);
  state_set(STATE_FROZEN);
  return true;
}

void CInode::unfreeze_inode(list<Context*>& finished) 
{
  dout(10) << "unfreeze_inode" << dendl;
  if (state_test(STATE_FREEZING)) {
    state_clear(STATE_FREEZING);
    put(PIN_FREEZING);
  } else if (state_test(STATE_FROZEN)) {
    state_clear(STATE_FROZEN);
    put(PIN_FROZEN);
  } else 
    assert(0);
  take_waiting(WAIT_UNFREEZE, finished);
}


// auth_pins
bool CInode::can_auth_pin() {
  if (is_freezing_inode() || is_frozen_inode()) return false;
  if (parent)
    return parent->can_auth_pin();
  return true;
}

void CInode::auth_pin(void *by) 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by << " on " << *this
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  
  if (parent)
    parent->adjust_nested_auth_pins(1, 1);
}

void CInode::auth_unpin(void *by) 
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  assert(auth_pin_set.count(by));
  auth_pin_set.erase(auth_pin_set.find(by));
#endif

  if (auth_pins == 0)
    put(PIN_AUTHPIN);
  
  dout(10) << "auth_unpin by " << by << " on " << *this
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  
  assert(auth_pins >= 0);

  if (parent)
    parent->adjust_nested_auth_pins(-1, -1);

  if (is_freezing_inode() &&
      auth_pins == auth_pin_freeze_allowance) {
    dout(10) << "auth_unpin freezing!" << dendl;
    get(PIN_FROZEN);
    put(PIN_FREEZING);
    state_clear(STATE_FREEZING);
    state_set(STATE_FROZEN);
    finish_waiting(WAIT_FROZEN);
  }  
}

void CInode::adjust_nested_auth_pins(int a)
{
  assert(a);
  nested_auth_pins += a;
  dout(35) << "adjust_nested_auth_pins by " << a
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  assert(nested_auth_pins >= 0);

  if (parent)
    parent->adjust_nested_auth_pins(a, 0);
}

void CInode::adjust_nested_anchors(int by)
{
  assert(by);
  nested_anchors += by;
  dout(20) << "adjust_nested_anchors by " << by << " -> " << nested_anchors << dendl;
  assert(nested_anchors >= 0);
  if (parent)
    parent->adjust_nested_anchors(by);
}

// authority

pair<int,int> CInode::authority() 
{
  if (inode_auth.first >= 0) 
    return inode_auth;

  if (parent)
    return parent->dir->authority();

  return CDIR_AUTH_UNDEF;
}


// SNAP

snapid_t CInode::get_oldest_snap()
{
  snapid_t t = CEPH_NOSNAP;
  if (!old_inodes.empty())
    t = old_inodes.begin()->second.first;
  return MIN(t, first);
}


old_inode_t& CInode::cow_old_inode(snapid_t follows, inode_t *pi)
{
  assert(follows >= first);

  old_inode_t &old = old_inodes[follows];
  old.first = first;
  old.inode = *pi;
  old.xattrs = xattrs;
  
  if (!(old.inode.rstat == old.inode.accounted_rstat))
    dirty_old_rstats.insert(follows);
  
  first = follows+1;

  dout(10) << "cow_old_inode to [" << old.first << "," << follows << "] on "
	   << *this << dendl;

  return old;
}

void CInode::pre_cow_old_inode()
{
  snapid_t follows = find_snaprealm()->get_newest_seq();
  if (first <= follows)
    cow_old_inode(follows, get_projected_inode());
}

void CInode::purge_stale_snap_data(const set<snapid_t>& snaps)
{
  dout(10) << "purge_stale_snap_data " << snaps << dendl;

  if (old_inodes.empty())
    return;

  map<snapid_t,old_inode_t>::iterator p = old_inodes.begin();
  while (p != old_inodes.end()) {
    set<snapid_t>::const_iterator q = snaps.lower_bound(p->second.first);
    if (q == snaps.end() || *q > p->first) {
      dout(10) << " purging old_inode [" << p->second.first << "," << p->first << "]" << dendl;
      old_inodes.erase(p++);
    } else
      p++;
  }
}

/*
 * pick/create an old_inode that we can write into.
 */
/*map<snapid_t,old_inode_t>::iterator CInode::pick_dirty_old_inode(snapid_t last)
{
  dout(10) << "pick_dirty_old_inode last " << last << dendl;
  SnapRealm *realm = find_snaprealm();
  dout(10) << " realm " << *realm << dendl;
  const set<snapid_t>& snaps = realm->get_snaps();
  dout(10) << " snaps " << snaps << dendl;
  
  //snapid_t snap = snaps.lower_bound(last);


  
}
*/

void CInode::open_snaprealm(bool nosplit)
{
  if (!snaprealm) {
    SnapRealm *parent = find_snaprealm();
    snaprealm = new SnapRealm(mdcache, this);
    if (parent) {
      dout(10) << "open_snaprealm " << snaprealm
	       << " parent is " << parent
	       << dendl;
      dout(30) << " siblings are " << parent->open_children << dendl;
      snaprealm->parent = parent;
      if (!nosplit)
	parent->split_at(snaprealm);
      parent->open_children.insert(snaprealm);
    }
  }
}
void CInode::close_snaprealm(bool nojoin)
{
  if (snaprealm) {
    dout(15) << "close_snaprealm " << *snaprealm << dendl;
    snaprealm->close_parents();
    if (snaprealm->parent) {
      snaprealm->parent->open_children.erase(snaprealm);
      //if (!nojoin)
      //snaprealm->parent->join(snaprealm);
    }
    delete snaprealm;
    snaprealm = 0;
  }
}

SnapRealm *CInode::find_snaprealm()
{
  CInode *cur = this;
  while (!cur->snaprealm) {
    if (cur->get_parent_dn())
      cur = cur->get_parent_dn()->get_dir()->get_inode();
    else if (get_projected_parent_dn())
      cur = cur->get_projected_parent_dn()->get_dir()->get_inode();
    else
      break;
  }
  return cur->snaprealm;
}

void CInode::encode_snap_blob(bufferlist &snapbl)
{
  if (snaprealm) {
    ::encode(*snaprealm, snapbl);
    dout(20) << "encode_snap_blob " << *snaprealm << dendl;
  }
}
void CInode::decode_snap_blob(bufferlist& snapbl) 
{
  if (snapbl.length()) {
    open_snaprealm();
    bufferlist::iterator p = snapbl.begin();
    ::decode(*snaprealm, p);
    dout(20) << "decode_snap_blob " << *snaprealm << dendl;
  }
}


bool CInode::encode_inodestat(bufferlist& bl, Session *session,
			      SnapRealm *realm,
			      snapid_t snapid)
{
  int client = session->inst.name.num();
  assert(snapid);
  
  bool valid = true;

  // do not issue caps if inode differs from readdir snaprealm
  bool no_caps = (realm && snaprealm && realm != snaprealm);
  if (no_caps)
    dout(20) << "encode_inodestat realm=" << realm << " snaprealm " << snaprealm
	     << " no_caps=" << no_caps << dendl;

  // pick a version!
  inode_t *oi = &inode;
  inode_t *pi = get_projected_inode();

  map<string, bufferptr> *pxattrs = 0;

  if (snapid != CEPH_NOSNAP && is_multiversion()) {

    // for now at least, old_inodes is only defined/valid on the auth
    if (!is_auth())
      valid = false;

    map<snapid_t,old_inode_t>::iterator p = old_inodes.lower_bound(snapid);
    if (p != old_inodes.end()) {
      dout(15) << "encode_inodestat snapid " << snapid
	       << " to old_inode [" << p->second.first << "," << p->first << "]" 
	       << " " << p->second.inode.rstat
	       << dendl;
      assert(p->second.first <= snapid && snapid <= p->first);
      pi = oi = &p->second.inode;
      pxattrs = &p->second.xattrs;
    }
  }
  
  /*
   * note: encoding matches struct ceph_client_reply_inode
   */
  struct ceph_mds_reply_inode e;
  memset(&e, 0, sizeof(e));
  e.ino = oi->ino;
  e.snapid = snapid;  // 0 -> NOSNAP
  e.rdev = oi->rdev;

  // "fake" a version that is old (stable) version, +1 if projected.
  e.version = (oi->version * 2) + is_projected();


  Capability *cap = get_client_cap(client);
  bool pfile = filelock.is_xlocked_by_client(client) || get_loner() == client;
  //(cap && (cap->issued() & CEPH_CAP_FILE_EXCL));
  bool pauth = authlock.is_xlocked_by_client(client);
  bool plink = linklock.is_xlocked_by_client(client);
  bool pxattr = xattrlock.is_xlocked_by_client(client);

  bool plocal = versionlock.get_last_wrlock_client() == client;
  
  inode_t *i = (pfile|pauth|plink|pxattr|plocal) ? pi : oi;
  i->ctime.encode_timeval(&e.ctime);
  
  dout(20) << " pfile " << pfile << " pauth " << pauth << " plink " << plink << " pxattr " << pxattr
	   << " plocal " << plocal
	   << " ctime " << i->ctime
	   << " valid=" << valid << dendl;

  // file
  i = pfile ? pi:oi;
  e.layout = i->layout;
  e.size = i->size;
  e.truncate_seq = i->truncate_seq;
  e.truncate_size = i->truncate_size;
  i->mtime.encode_timeval(&e.mtime);
  i->atime.encode_timeval(&e.atime);
  e.time_warp_seq = i->time_warp_seq;

  // max_size is min of projected, actual
  e.max_size = MIN(oi->client_ranges.count(client) ? oi->client_ranges[client].last : 0,
		   pi->client_ranges.count(client) ? pi->client_ranges[client].last : 0);

  e.files = i->dirstat.nfiles;
  e.subdirs = i->dirstat.nsubdirs;

  // nest (do same as file... :/)
  i->rstat.rctime.encode_timeval(&e.rctime);
  e.rbytes = i->rstat.rbytes;
  e.rfiles = i->rstat.rfiles;
  e.rsubdirs = i->rstat.rsubdirs;

  // auth
  i = pauth ? pi:oi;
  e.mode = i->mode;
  e.uid = i->uid;
  e.gid = i->gid;

  // link
  i = plink ? pi:oi;
  e.nlink = i->nlink;
  
  e.fragtree.nsplits = dirfragtree._splits.size();

  // xattr
  i = pxattr ? pi:oi;
  bool had_latest_xattrs = cap && (cap->issued() & CEPH_CAP_XATTR_SHARED) &&
    cap->client_xattr_version == i->xattr_version;
  
  // encode caps
  if (snapid != CEPH_NOSNAP) {
    /*
     * snapped inodes (files or dirs) only get read-only caps.  if
     * there is a Capability on the inode (either it's a dir, or it's
     * a file with a flushing cap), then limit caps issued to what is
     * already on teh cap.
     *
     * do NOT adjust cap issued state, because the client always
     * tracks caps per-snap and the mds does either per-interval or
     * multiversion.
     */
    e.cap.caps = valid ? get_caps_allowed_by_type(CAP_ANY) : CEPH_STAT_CAP_INODE;
    if (cap)
      e.cap.caps = e.cap.caps & cap->issued();
    e.cap.seq = 0;
    e.cap.mseq = 0;
    e.cap.realm = 0;
  } else {
    if (!no_caps && valid && !cap) {
      // add a new cap
      cap = add_client_cap(client, session, find_snaprealm());
      if (is_auth()) {
	if (choose_ideal_loner() >= 0)
	  try_set_loner();
	else if (get_wanted_loner() < 0)
	  try_drop_loner();
      }
    }

    if (cap && valid) {
      int likes = get_caps_liked();
      int allowed = get_caps_allowed_for_client(client);
      int issue = (cap->wanted() | likes) & allowed;
      cap->issue_norevoke(issue);
      issue = cap->pending();
      cap->set_last_issue();
      cap->set_last_issue_stamp(g_clock.recent_now());
      e.cap.caps = issue;
      e.cap.wanted = cap->wanted();
      e.cap.cap_id = cap->get_cap_id();
      e.cap.seq = cap->get_last_seq();
      dout(10) << "encode_inodestat issueing " << ccap_string(issue) << " seq " << cap->get_last_seq() << dendl;
      e.cap.mseq = cap->get_mseq();
      e.cap.realm = find_snaprealm()->inode->ino();
    } else {
      e.cap.caps = 0;
      e.cap.seq = 0;
      e.cap.mseq = 0;
      e.cap.realm = 0;
      e.cap.wanted = 0;
    }
  }
  e.cap.flags = is_auth() ? CEPH_CAP_FLAG_AUTH:0;
  dout(10) << "encode_inodestat caps " << ccap_string(e.cap.caps)
	   << " seq " << e.cap.seq
	   << " mseq " << e.cap.mseq << dendl;

  // xattr
  bufferlist xbl;
  e.xattr_version = i->xattr_version;
  if (!had_latest_xattrs &&
      cap &&
      (cap->pending() & CEPH_CAP_XATTR_SHARED)) {
    
    if (!pxattrs)
      pxattrs = pxattr ? get_projected_xattrs() : &xattrs;

    ::encode(*pxattrs, xbl);
    if (cap)
      cap->client_xattr_version = i->xattr_version;
    dout(10) << "including xattrs version " << i->xattr_version << dendl;
  }

  // encode
  ::encode(e, bl);
  for (map<frag_t,int32_t>::iterator p = dirfragtree._splits.begin();
       p != dirfragtree._splits.end();
       p++) {
    ::encode(p->first, bl);
    ::encode(p->second, bl);
  }
  ::encode(symlink, bl);
  ::encode(xbl, bl);

  return valid;
}

void CInode::encode_cap_message(MClientCaps *m, Capability *cap)
{
  client_t client = cap->get_client();

  bool pfile = filelock.is_xlocked_by_client(client) ||
    (cap && (cap->issued() & CEPH_CAP_FILE_EXCL));
  bool pauth = authlock.is_xlocked_by_client(client);
  bool plink = linklock.is_xlocked_by_client(client);
  bool pxattr = xattrlock.is_xlocked_by_client(client);
 
  inode_t *oi = &inode;
  inode_t *pi = get_projected_inode();
  inode_t *i = (pfile|pauth|plink|pxattr) ? pi : oi;
  i->ctime.encode_timeval(&m->head.ctime);
  
  dout(20) << "encode_cap_message pfile " << pfile
	   << " pauth " << pauth << " plink " << plink << " pxattr " << pxattr
	   << " ctime " << i->ctime << dendl;

  i = pfile ? pi:oi;
  m->head.layout = i->layout;
  m->head.size = i->size;
  m->head.truncate_seq = i->truncate_seq;
  m->head.truncate_size = i->truncate_size;
  i->mtime.encode_timeval(&m->head.mtime);
  i->atime.encode_timeval(&m->head.atime);
  m->head.time_warp_seq = i->time_warp_seq;

  // max_size is min of projected, actual.
  uint64_t oldms = oi->client_ranges.count(client) ? oi->client_ranges[client].last : 0;
  uint64_t newms = pi->client_ranges.count(client) ? pi->client_ranges[client].last : 0;
  m->head.max_size = MIN(oldms, newms);

  i = pauth ? pi:oi;
  m->head.mode = i->mode;
  m->head.uid = i->uid;
  m->head.gid = i->gid;

  i = plink ? pi:oi;
  m->head.nlink = i->nlink;

  i = pxattr ? pi:oi;
  map<string,bufferptr> *ix = pxattr ? get_projected_xattrs() : &xattrs;
  if ((cap->pending() & CEPH_CAP_XATTR_SHARED) &&
      i->xattr_version > cap->client_xattr_version) {
    dout(10) << "    including xattrs v " << i->xattr_version << dendl;
    ::encode(*ix, m->xattrbl);
    m->head.xattr_version = i->xattr_version;
    cap->client_xattr_version = i->xattr_version;
  }
}



void CInode::_encode_base(bufferlist& bl)
{
  ::encode(first, bl);
  ::encode(inode, bl);
  ::encode(symlink, bl);
  ::encode(dirfragtree, bl);
  ::encode(xattrs, bl);
  ::encode(old_inodes, bl);
  encode_snap(bl);
}
void CInode::_decode_base(bufferlist::iterator& p)
{
  ::decode(first, p);
  bool was_anchored = inode.anchored;
  ::decode(inode, p);
  if (parent && was_anchored != inode.anchored)
    parent->adjust_nested_anchors((int)inode.anchored - (int)was_anchored);

  ::decode(symlink, p);
  ::decode(dirfragtree, p);
  ::decode(xattrs, p);
  ::decode(old_inodes, p);
  decode_snap(p);
}

void CInode::_encode_locks_full(bufferlist& bl)
{
  ::encode(authlock, bl);
  ::encode(linklock, bl);
  ::encode(dirfragtreelock, bl);
  ::encode(filelock, bl);
  ::encode(xattrlock, bl);
  ::encode(snaplock, bl);
  ::encode(nestlock, bl);
}
void CInode::_decode_locks_full(bufferlist::iterator& p)
{
  ::decode(authlock, p);
  ::decode(linklock, p);
  ::decode(dirfragtreelock, p);
  ::decode(filelock, p);
  ::decode(xattrlock, p);
  ::decode(snaplock, p);
  ::decode(nestlock, p);
}

void CInode::_encode_locks_state_for_replica(bufferlist& bl)
{
  authlock.encode_state_for_replica(bl);
  linklock.encode_state_for_replica(bl);
  dirfragtreelock.encode_state_for_replica(bl);
  filelock.encode_state_for_replica(bl);
  nestlock.encode_state_for_replica(bl);
  xattrlock.encode_state_for_replica(bl);
  snaplock.encode_state_for_replica(bl);
}
void CInode::_decode_locks_state(bufferlist::iterator& p, bool is_new)
{
  authlock.decode_state(p, is_new);
  linklock.decode_state(p, is_new);
  dirfragtreelock.decode_state(p, is_new);
  filelock.decode_state(p, is_new);
  nestlock.decode_state(p, is_new);
  xattrlock.decode_state(p, is_new);
  snaplock.decode_state(p, is_new);
}
void CInode::_decode_locks_rejoin(bufferlist::iterator& p, list<Context*>& waiters)
{
  authlock.decode_state_rejoin(p, waiters);
  linklock.decode_state_rejoin(p, waiters);
  dirfragtreelock.decode_state_rejoin(p, waiters);
  filelock.decode_state_rejoin(p, waiters);
  nestlock.decode_state_rejoin(p, waiters);
  xattrlock.decode_state_rejoin(p, waiters);
  snaplock.decode_state_rejoin(p, waiters);
}


// IMPORT/EXPORT

void CInode::encode_export(bufferlist& bl)
{
  __u8 struct_v = 2;
  ::encode(struct_v, bl);
  _encode_base(bl);

  bool dirty = is_dirty();
  ::encode(dirty, bl);

  ::encode(pop, bl);

  ::encode(replica_map, bl);

  // include scatterlock info for any bounding CDirs
  bufferlist bounding;
  if (inode.is_dir())
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      CDir *dir = p->second;
      if (dir->state_test(CDir::STATE_EXPORTBOUND)) {
	::encode(p->first, bounding);
	::encode(dir->fnode.fragstat, bounding);
	::encode(dir->fnode.accounted_fragstat, bounding);
	::encode(dir->fnode.rstat, bounding);
	::encode(dir->fnode.accounted_rstat, bounding);
	dout(10) << " encoded fragstat/rstat info for " << *dir << dendl;
      }
    }
  ::encode(bounding, bl);

  _encode_locks_full(bl);
  get(PIN_TEMPEXPORTING);
}

void CInode::finish_export(utime_t now)
{
  pop.zero(now);

  // just in case!
  //dirlock.clear_updated();

  loner_cap = -1;

  put(PIN_TEMPEXPORTING);
}

void CInode::decode_import(bufferlist::iterator& p,
			   LogSegment *ls)
{
  __u8 struct_v;
  ::decode(struct_v, p);

  _decode_base(p);

  bool dirty;
  ::decode(dirty, p);
  if (dirty) 
    _mark_dirty(ls);

  ::decode(pop, p);

  ::decode(replica_map, p);
  if (!replica_map.empty())
    get(PIN_REPLICATED);

  if (struct_v >= 2) {
    // decode fragstat info on bounding cdirs
    bufferlist bounding;
    ::decode(bounding, p);
    bufferlist::iterator q = bounding.begin();
    while (!q.end()) {
      frag_t fg;
      ::decode(fg, q);
      CDir *dir = get_dirfrag(fg);
      assert(dir);  // we should have all bounds open
      if (!dir->is_auth()) {
	::decode(dir->fnode.fragstat, q);
	::decode(dir->fnode.accounted_fragstat, q);
	::decode(dir->fnode.rstat, q);
	::decode(dir->fnode.accounted_rstat, q);
	dout(10) << " took fragstat/rstat info for " << *dir << dendl;
      } else {
	dout(10) << " skipped fragstat/rstat info for " << *dir << dendl;
	frag_info_t f;
	nest_info_t n;
	::decode(f, q);
	::decode(f, q);
	::decode(n, q);
	::decode(n, q);
      }
    }
  }

  _decode_locks_full(p);
}
