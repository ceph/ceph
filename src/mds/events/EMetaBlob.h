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

#ifndef CEPH_MDS_EMETABLOB_H
#define CEPH_MDS_EMETABLOB_H

#include <stdlib.h>

#include "../CInode.h"
#include "../CDir.h"
#include "../CDentry.h"
#include "../LogSegment.h"

#include "include/interval_set.h"

class MDSRank;
class MDLog;
class LogSegment;
struct MDSlaveUpdate;

/*
 * a bunch of metadata in the journal
 */

/* notes:
 *
 * - make sure you adjust the inode.version for any modified inode you
 *   journal.  CDir and CDentry maintain a projected_version, but CInode
 *   doesn't, since the journaled inode usually has to be modifed 
 *   manually anyway (to delay the change in the MDS's cache until after
 *   it is journaled).
 *
 */


class EMetaBlob {

public:
  /* fullbit - a regular dentry + inode
   *
   * We encode this one a bit weirdly, just because (also, it's marginally faster
   * on multiple encodes, which I think can happen):
   * Encode a bufferlist on struct creation with all data members, without a struct_v.
   * When encode is called, encode struct_v and then append the bufferlist.
   * Decode straight into the appropriate variables.
   *
   * So, if you add members, encode them in the constructor and then change
   * the struct_v in the encode function!
   */
  struct fullbit {
    static const int STATE_DIRTY =	 (1<<0);
    static const int STATE_DIRTYPARENT = (1<<1);
    static const int STATE_DIRTYPOOL   = (1<<2);
    typedef compact_map<snapid_t, old_inode_t> old_inodes_t;
    string  dn;         // dentry
    snapid_t dnfirst, dnlast;
    version_t dnv;
    inode_t inode;      // if it's not
    fragtree_t dirfragtree;
    map<string,bufferptr> xattrs;
    string symlink;
    snapid_t oldest_snap;
    bufferlist snapbl;
    __u8 state;
    old_inodes_t old_inodes;

    fullbit(const fullbit& o);
    const fullbit& operator=(const fullbit& o);

    fullbit(const string& d, snapid_t df, snapid_t dl, 
	    version_t v, const inode_t& i, const fragtree_t &dft, 
	    const map<string,bufferptr> &xa, const string& sym,
	    snapid_t os, const bufferlist &sbl, __u8 st,
	    const old_inodes_t *oi = NULL) :
      dn(d), dnfirst(df), dnlast(dl), dnv(v), inode(i), xattrs(xa),
      oldest_snap(os), state(st)
    {
      if (i.is_symlink())
	symlink = sym;
      if (i.is_dir())
	dirfragtree = dft;
      if (oi)
	old_inodes = *oi;
      snapbl = sbl;
    }
    fullbit(bufferlist::iterator &p) {
      decode(p);
    }
    fullbit() {}
    ~fullbit() {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator &bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<EMetaBlob::fullbit*>& ls);

    void update_inode(MDSRank *mds, CInode *in);
    bool is_dirty() const { return (state & STATE_DIRTY); }
    bool is_dirty_parent() const { return (state & STATE_DIRTYPARENT); }
    bool is_dirty_pool() const { return (state & STATE_DIRTYPOOL); }

    void print(ostream& out) const {
      out << " fullbit dn " << dn << " [" << dnfirst << "," << dnlast << "] dnv " << dnv
	  << " inode " << inode.ino
	  << " state=" << state << std::endl;
    }
    string state_string() const {
      string state_string;
      bool marked_already = false;
      if (is_dirty()) {
	state_string.append("dirty");
	marked_already = true;
      }
      if (is_dirty_parent()) {
	state_string.append(marked_already ? "+dirty_parent" : "dirty_parent");
	if (is_dirty_pool())
	  state_string.append("+dirty_pool");
      }
      return state_string;
    }
  };
  WRITE_CLASS_ENCODER(fullbit)
  
  /* remotebit - a dentry + remote inode link (i.e. just an ino)
   */
  struct remotebit {
    string dn;
    snapid_t dnfirst, dnlast;
    version_t dnv;
    inodeno_t ino;
    unsigned char d_type;
    bool dirty;

    remotebit(const string& d, snapid_t df, snapid_t dl, version_t v, inodeno_t i, unsigned char dt, bool dr) : 
      dn(d), dnfirst(df), dnlast(dl), dnv(v), ino(i), d_type(dt), dirty(dr) { }
    remotebit(bufferlist::iterator &p) { decode(p); }
    remotebit(): dnfirst(0), dnlast(0), dnv(0), ino(0),
	d_type('\0'), dirty(false) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator &bl);
    void print(ostream& out) const {
      out << " remotebit dn " << dn << " [" << dnfirst << "," << dnlast << "] dnv " << dnv
	  << " ino " << ino
	  << " dirty=" << dirty << std::endl;
    }
    void dump(Formatter *f) const;
    static void generate_test_instances(list<remotebit*>& ls);
  };
  WRITE_CLASS_ENCODER(remotebit)

  /*
   * nullbit - a null dentry
   */
  struct nullbit {
    string dn;
    snapid_t dnfirst, dnlast;
    version_t dnv;
    bool dirty;

    nullbit(const string& d, snapid_t df, snapid_t dl, version_t v, bool dr) : 
      dn(d), dnfirst(df), dnlast(dl), dnv(v), dirty(dr) { }
    nullbit(bufferlist::iterator &p) { decode(p); }
    nullbit(): dnfirst(0), dnlast(0), dnv(0), dirty(false) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator &bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<nullbit*>& ls);
    void print(ostream& out) {
      out << " nullbit dn " << dn << " [" << dnfirst << "," << dnlast << "] dnv " << dnv
	  << " dirty=" << dirty << std::endl;
    }
  };
  WRITE_CLASS_ENCODER(nullbit)


  /* dirlump - contains metadata for any dir we have contents for.
   */
public:
  struct dirlump {
    static const int STATE_COMPLETE =    (1<<1);
    static const int STATE_DIRTY =       (1<<2);  // dirty due to THIS journal item, that is!
    static const int STATE_NEW =         (1<<3);  // new directory
    static const int STATE_IMPORTING =	 (1<<4);  // importing directory
    static const int STATE_DIRTYDFT =	 (1<<5);  // dirty dirfragtree

    //version_t  dirv;
    fnode_t fnode;
    __u32 state;
    __u32 nfull, nremote, nnull;

  private:
    mutable bufferlist dnbl;
    mutable bool dn_decoded;
    mutable list<ceph::shared_ptr<fullbit> > dfull;
    mutable list<remotebit> dremote;
    mutable list<nullbit> dnull;

  public:
    dirlump() : state(0), nfull(0), nremote(0), nnull(0), dn_decoded(true) { }
    
    bool is_complete() const { return state & STATE_COMPLETE; }
    void mark_complete() { state |= STATE_COMPLETE; }
    bool is_dirty() const { return state & STATE_DIRTY; }
    void mark_dirty() { state |= STATE_DIRTY; }
    bool is_new() const { return state & STATE_NEW; }
    void mark_new() { state |= STATE_NEW; }
    bool is_importing() { return state & STATE_IMPORTING; }
    void mark_importing() { state |= STATE_IMPORTING; }
    bool is_dirty_dft() { return state & STATE_DIRTYDFT; }
    void mark_dirty_dft() { state |= STATE_DIRTYDFT; }

    const list<ceph::shared_ptr<fullbit> > &get_dfull()   const { return dfull; }
    const list<remotebit>                  &get_dremote() const { return dremote; }
    const list<nullbit>                    &get_dnull()   const { return dnull; }

    void add_dnull(nullbit const &n)                   { dnull.push_back(n); };
    void add_dfull(ceph::shared_ptr<fullbit> const &p) { dfull.push_back(p); };
    void add_dremote(remotebit const &r)               { dremote.push_back(r); };

    void print(dirfrag_t dirfrag, ostream& out) {
      out << "dirlump " << dirfrag << " v " << fnode.version
	  << " state " << state
	  << " num " << nfull << "/" << nremote << "/" << nnull
	  << std::endl;
      _decode_bits();
      for (list<ceph::shared_ptr<fullbit> >::iterator p = dfull.begin(); p != dfull.end(); ++p)
	(*p)->print(out);
      for (list<remotebit>::iterator p = dremote.begin(); p != dremote.end(); ++p)
	p->print(out);
      for (list<nullbit>::iterator p = dnull.begin(); p != dnull.end(); ++p)
	p->print(out);
    }

    string state_string() const {
      string state_string;
      bool marked_already = false;
      if (is_complete()) {
	state_string.append("complete");
	marked_already = true;
      }
      if (is_dirty()) {
	state_string.append(marked_already ? "+dirty" : "dirty");
	marked_already = true;
      }
      if (is_new()) {
	state_string.append(marked_already ? "+new" : "new");
      }
      return state_string;
    }

    // if this changes, update the versioning in encode for it!
    void _encode_bits() const {
      if (!dn_decoded) return;
      ::encode(dfull, dnbl);
      ::encode(dremote, dnbl);
      ::encode(dnull, dnbl);
    }
    void _decode_bits() const { 
      if (dn_decoded) return;
      bufferlist::iterator p = dnbl.begin();
      ::decode(dfull, p);
      ::decode(dremote, p);
      ::decode(dnull, p);
      dn_decoded = true;
    }

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator &bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<dirlump*>& ls);
  };
  WRITE_CLASS_ENCODER(dirlump)

  // my lumps.  preserve the order we added them in a list.
  list<dirfrag_t>         lump_order;
  map<dirfrag_t, dirlump> lump_map;
  list<ceph::shared_ptr<fullbit> > roots;
public:
  list<pair<__u8,version_t> > table_tids;  // tableclient transactions

  inodeno_t opened_ino;
public:
  inodeno_t renamed_dirino;
  list<frag_t> renamed_dir_frags;
private:
  
  // ino (pre)allocation.  may involve both inotable AND session state.
  version_t inotablev, sessionmapv;
  inodeno_t allocated_ino;            // inotable
  interval_set<inodeno_t> preallocated_inos; // inotable + session
  inodeno_t used_preallocated_ino;    //            session
  entity_name_t client_name;          //            session

  // inodes i've truncated
  list<inodeno_t> truncate_start;        // start truncate 
  map<inodeno_t, log_segment_seq_t> truncate_finish;  // finished truncate (started in segment blah)

public:
  vector<inodeno_t> destroyed_inodes;
private:

  // idempotent op(s)
  list<pair<metareqid_t,uint64_t> > client_reqs;
  list<pair<metareqid_t,uint64_t> > client_flushes;

 public:
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void get_inodes(std::set<inodeno_t> &inodes) const;
  void get_paths(std::vector<std::string> &paths) const;
  void get_dentries(std::map<dirfrag_t, std::set<std::string> > &dentries) const;
  entity_name_t get_client_name() const {return client_name;}

  void dump(Formatter *f) const;
  static void generate_test_instances(list<EMetaBlob*>& ls);
  // soft stateadd
  uint64_t last_subtree_map;
  uint64_t event_seq;

  // for replay, in certain cases
  //LogSegment *_segment;

  EMetaBlob(MDLog *mdl = 0);  // defined in journal.cc
  ~EMetaBlob() { }

  void print(ostream& out) {
    for (list<dirfrag_t>::iterator p = lump_order.begin();
	 p != lump_order.end();
	 ++p) {
      lump_map[*p].print(*p, out);
    }
  }

  void add_client_req(metareqid_t r, uint64_t tid=0) {
    client_reqs.push_back(pair<metareqid_t,uint64_t>(r, tid));
  }
  void add_client_flush(metareqid_t r, uint64_t tid=0) {
    client_flushes.push_back(pair<metareqid_t,uint64_t>(r, tid));
  }

  void add_table_transaction(int table, version_t tid) {
    table_tids.push_back(pair<__u8, version_t>(table, tid));
  }

  void add_opened_ino(inodeno_t ino) {
    assert(!opened_ino);
    opened_ino = ino;
  }

  void set_ino_alloc(inodeno_t alloc,
		     inodeno_t used_prealloc,
		     interval_set<inodeno_t>& prealloc,
		     entity_name_t client,
		     version_t sv, version_t iv) {
    allocated_ino = alloc;
    used_preallocated_ino = used_prealloc;
    preallocated_inos = prealloc;
    client_name = client;
    sessionmapv = sv;
    inotablev = iv;
  }

  void add_truncate_start(inodeno_t ino) {
    truncate_start.push_back(ino);
  }
  void add_truncate_finish(inodeno_t ino, uint64_t segoff) {
    truncate_finish[ino] = segoff;
  }
  
  bool rewrite_truncate_finish(MDSRank const *mds, std::map<uint64_t, uint64_t> const &old_to_new);

  void add_destroyed_inode(inodeno_t ino) {
    destroyed_inodes.push_back(ino);
  }
  
  void add_null_dentry(CDentry *dn, bool dirty) {
    add_null_dentry(add_dir(dn->get_dir(), false), dn, dirty);
  }
  void add_null_dentry(dirlump& lump, CDentry *dn, bool dirty) {
    // add the dir
    lump.nnull++;
    lump.add_dnull(nullbit(dn->get_name(), 
			   dn->first, dn->last,
			   dn->get_projected_version(), 
			   dirty));
  }

  void add_remote_dentry(CDentry *dn, bool dirty) {
    add_remote_dentry(add_dir(dn->get_dir(), false), dn, dirty, 0, 0);
  }
  void add_remote_dentry(CDentry *dn, bool dirty, inodeno_t rino, int rdt) {
    add_remote_dentry(add_dir(dn->get_dir(), false), dn, dirty, rino, rdt);
  }
  void add_remote_dentry(dirlump& lump, CDentry *dn, bool dirty, 
			 inodeno_t rino=0, unsigned char rdt=0) {
    if (!rino) {
      rino = dn->get_projected_linkage()->get_remote_ino();
      rdt = dn->get_projected_linkage()->get_remote_d_type();
    }
    lump.nremote++;
    lump.add_dremote(remotebit(dn->get_name(), 
			       dn->first, dn->last,
                               dn->get_projected_version(), 
                               rino, rdt,
                               dirty));
  }

  // return remote pointer to to-be-journaled inode
  void add_primary_dentry(CDentry *dn, CInode *in, bool dirty,
			  bool dirty_parent=false, bool dirty_pool=false) {
    __u8 state = 0;
    if (dirty) state |= fullbit::STATE_DIRTY;
    if (dirty_parent) state |= fullbit::STATE_DIRTYPARENT;
    if (dirty_pool) state |= fullbit::STATE_DIRTYPOOL;
    add_primary_dentry(add_dir(dn->get_dir(), false), dn, in, state);
  }
  void add_primary_dentry(dirlump& lump, CDentry *dn, CInode *in, __u8 state) {
    if (!in) 
      in = dn->get_projected_linkage()->get_inode();

    // make note of where this inode was last journaled
    in->last_journaled = event_seq;
    //cout << "journaling " << in->inode.ino << " at " << my_offset << std::endl;

    const inode_t *pi = in->get_projected_inode();
    if ((state & fullbit::STATE_DIRTY) && pi->is_backtrace_updated())
      state |= fullbit::STATE_DIRTYPARENT;

    bufferlist snapbl;
    const sr_t *sr = in->get_projected_srnode();
    if (sr)
      sr->encode(snapbl);

    lump.nfull++;
    lump.add_dfull(ceph::shared_ptr<fullbit>(new fullbit(dn->get_name(),
							 dn->first, dn->last,
							 dn->get_projected_version(),
							 *pi, in->dirfragtree,
							 *in->get_projected_xattrs(),
							 in->symlink,
							 in->oldest_snap, snapbl,
							 state, &in->old_inodes)));
  }

  // convenience: primary or remote?  figure it out.
  void add_dentry(CDentry *dn, bool dirty) {
    dirlump& lump = add_dir(dn->get_dir(), false);
    add_dentry(lump, dn, dirty, false, false);
  }
  void add_import_dentry(CDentry *dn) {
    bool dirty_parent = false;
    bool dirty_pool = false;
    if (dn->get_linkage()->is_primary()) {
      dirty_parent = dn->get_linkage()->get_inode()->is_dirty_parent();
      dirty_pool = dn->get_linkage()->get_inode()->is_dirty_pool();
    }
    dirlump& lump = add_dir(dn->get_dir(), false);
    add_dentry(lump, dn, dn->is_dirty(), dirty_parent, dirty_pool);
  }
  void add_dentry(dirlump& lump, CDentry *dn, bool dirty, bool dirty_parent, bool dirty_pool) {
    // primary or remote
    if (dn->get_projected_linkage()->is_remote()) {
      add_remote_dentry(dn, dirty);
      return;
    } else if (dn->get_projected_linkage()->is_null()) {
      add_null_dentry(dn, dirty);
      return;
    }
    assert(dn->get_projected_linkage()->is_primary());
    add_primary_dentry(dn, 0, dirty, dirty_parent, dirty_pool);
  }

  void add_root(bool dirty, CInode *in, const inode_t *pi=0, fragtree_t *pdft=0, bufferlist *psnapbl=0,
		    map<string,bufferptr> *px=0) {
    in->last_journaled = event_seq;
    //cout << "journaling " << in->inode.ino << " at " << my_offset << std::endl;

    if (!pi) pi = in->get_projected_inode();
    if (!pdft) pdft = &in->dirfragtree;
    if (!px) px = in->get_projected_xattrs();

    bufferlist snapbl;
    if (psnapbl)
      snapbl = *psnapbl;
    else
      in->encode_snap_blob(snapbl);

    for (list<ceph::shared_ptr<fullbit> >::iterator p = roots.begin(); p != roots.end(); ++p) {
      if ((*p)->inode.ino == in->ino()) {
	roots.erase(p);
	break;
      }
    }

    string empty;
    roots.push_back(ceph::shared_ptr<fullbit>(new fullbit(empty, in->first, in->last, 0, *pi,
							  *pdft, *px, in->symlink,
							  in->oldest_snap, snapbl,
							  dirty ? fullbit::STATE_DIRTY : 0,
							  &in->old_inodes)));
  }
  
  dirlump& add_dir(CDir *dir, bool dirty, bool complete=false) {
    return add_dir(dir->dirfrag(), dir->get_projected_fnode(), dir->get_projected_version(),
		   dirty, complete);
  }
  dirlump& add_new_dir(CDir *dir) {
    return add_dir(dir->dirfrag(), dir->get_projected_fnode(), dir->get_projected_version(),
		   true, true, true); // dirty AND complete AND new
  }
  dirlump& add_import_dir(CDir *dir) {
    // dirty=false would be okay in some cases
    return add_dir(dir->dirfrag(), dir->get_projected_fnode(), dir->get_projected_version(),
		   dir->is_dirty(), dir->is_complete(), false, true, dir->is_dirty_dft());
  }
  dirlump& add_fragmented_dir(CDir *dir, bool dirty, bool dirtydft) {
    return add_dir(dir->dirfrag(), dir->get_projected_fnode(), dir->get_projected_version(),
		   dirty, false, false, false, dirtydft);
  }
  dirlump& add_dir(dirfrag_t df, const fnode_t *pf, version_t pv, bool dirty,
		   bool complete=false, bool isnew=false,
		   bool importing=false, bool dirty_dft=false) {
    if (lump_map.count(df) == 0)
      lump_order.push_back(df);

    dirlump& l = lump_map[df];
    l.fnode = *pf;
    l.fnode.version = pv;
    if (complete) l.mark_complete();
    if (dirty) l.mark_dirty();
    if (isnew) l.mark_new();
    if (importing) l.mark_importing();
    if (dirty_dft) l.mark_dirty_dft();
    return l;
  }
  
  static const int TO_AUTH_SUBTREE_ROOT = 0;  // default.
  static const int TO_ROOT = 1;
  
  void add_dir_context(CDir *dir, int mode = TO_AUTH_SUBTREE_ROOT);

  bool empty() {
    return roots.empty() && lump_order.empty() && table_tids.empty() &&
	   truncate_start.empty() && truncate_finish.empty() &&
	   destroyed_inodes.empty() && client_reqs.empty() &&
	   opened_ino == 0 && inotablev == 0 && sessionmapv == 0;
  }

  void print(ostream& out) const {
    out << "[metablob";
    if (!lump_order.empty()) 
      out << " " << lump_order.front() << ", " << lump_map.size() << " dirs";
    if (!table_tids.empty())
      out << " table_tids=" << table_tids;
    if (allocated_ino || preallocated_inos.size()) {
      if (allocated_ino)
	out << " alloc_ino=" << allocated_ino;
      if (preallocated_inos.size())
	out << " prealloc_ino=" << preallocated_inos;
      if (used_preallocated_ino)
	out << " used_prealloc_ino=" << used_preallocated_ino;
      out << " v" << inotablev;
    }
    out << "]";
  }

  void update_segment(LogSegment *ls);
  void replay(MDSRank *mds, LogSegment *ls, MDSlaveUpdate *su=NULL);
};
WRITE_CLASS_ENCODER(EMetaBlob)
WRITE_CLASS_ENCODER(EMetaBlob::fullbit)
WRITE_CLASS_ENCODER(EMetaBlob::remotebit)
WRITE_CLASS_ENCODER(EMetaBlob::nullbit)
WRITE_CLASS_ENCODER(EMetaBlob::dirlump)

inline ostream& operator<<(ostream& out, const EMetaBlob& t) {
  t.print(out);
  return out;
}

#endif
