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

#ifndef __MDS_EMETABLOB_H
#define __MDS_EMETABLOB_H

#include <stdlib.h>
#include <string>
using std::string;

#include "../CInode.h"
#include "../CDir.h"
#include "../CDentry.h"

#include "include/triple.h"

class MDS;
class MDLog;
class LogSegment;

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
   */
  struct fullbit {
    string  dn;         // dentry
    version_t dnv;
    inode_t inode;      // if it's not
    fragtree_t dirfragtree;
    map<string,bufferptr> xattrs;
    string symlink;
    bool dirty;

    fullbit(const string& d, version_t v, inode_t& i, fragtree_t &dft, map<string,bufferptr> &xa, string& sym, bool dr) :
      dn(d), dnv(v), inode(i), dirfragtree(dft), xattrs(xa), symlink(sym), dirty(dr) { }
    fullbit(bufferlist::iterator &p) { decode(p); }
    fullbit() {}

    void encode(bufferlist& bl) const {
      ::encode(dn, bl);
      ::encode(dnv, bl);
      ::encode(inode, bl);
      ::encode(dirfragtree, bl);
      ::encode(xattrs, bl);
      if (inode.is_symlink())
	::encode(symlink, bl);
      ::encode(dirty, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(dn, bl);
      ::decode(dnv, bl);
      ::decode(inode, bl);
      ::decode(dirfragtree, bl);
      ::decode(xattrs, bl);
      if (inode.is_symlink())
	::decode(symlink, bl);
      ::decode(dirty, bl);
    }
    void print(ostream& out) {
      out << " fullbit dn " << dn << " dnv " << dnv
	  << " inode " << inode.ino
	  << " dirty=" << dirty << std::endl;
    }
  };
  WRITE_CLASS_ENCODER(fullbit)
  
  /* remotebit - a dentry + remote inode link (i.e. just an ino)
   */
  struct remotebit {
    string dn;
    version_t dnv;
    inodeno_t ino;
    unsigned char d_type;
    bool dirty;

    remotebit(const string& d, version_t v, inodeno_t i, unsigned char dt, bool dr) : 
      dn(d), dnv(v), ino(i), d_type(dt), dirty(dr) { }
    remotebit(bufferlist::iterator &p) { decode(p); }
    remotebit() {}

    void encode(bufferlist& bl) const {
      ::encode(dn, bl);
      ::encode(dnv, bl);
      ::encode(ino, bl);
      ::encode(d_type, bl);
      ::encode(dirty, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(dn, bl);
      ::decode(dnv, bl);
      ::decode(ino, bl);
      ::decode(d_type, bl);
      ::decode(dirty, bl);
    }
    void print(ostream& out) {
      out << " remotebit dn " << dn << " dnv " << dnv
	  << " ino " << ino
	  << " dirty=" << dirty << std::endl;
    }
  };
  WRITE_CLASS_ENCODER(remotebit)

  /*
   * nullbit - a null dentry
   */
  struct nullbit {
    string dn;
    version_t dnv;
    bool dirty;

    nullbit(const string& d, version_t v, bool dr) : dn(d), dnv(v), dirty(dr) { }
    nullbit(bufferlist::iterator &p) { decode(p); }
    nullbit() {}

    void encode(bufferlist& bl) const {
      ::encode(dn, bl);
      ::encode(dnv, bl);
      ::encode(dirty, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(dn, bl);
      ::decode(dnv, bl);
      ::decode(dirty, bl);
    }
    void print(ostream& out) {
      out << " nullbit dn " << dn << " dnv " << dnv
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

    //version_t  dirv;
    fnode_t fnode;
    __u32 state;
    __u32 nfull, nremote, nnull;

  private:
    mutable bufferlist dnbl;
    bool dn_decoded;
    list<fullbit>   dfull;
    list<remotebit> dremote;
    list<nullbit>   dnull;

  public:
    dirlump() : state(0), nfull(0), nremote(0), nnull(0), dn_decoded(true) { }
    
    bool is_complete() { return state & STATE_COMPLETE; }
    void mark_complete() { state |= STATE_COMPLETE; }
    bool is_dirty() { return state & STATE_DIRTY; }
    void mark_dirty() { state |= STATE_DIRTY; }

    list<fullbit>   &get_dfull()   { return dfull; }
    list<remotebit> &get_dremote() { return dremote; }
    list<nullbit>   &get_dnull()   { return dnull; }

    void print(dirfrag_t dirfrag, ostream& out) {
      out << "dirlump " << dirfrag << " v " << fnode.version
	  << " state " << state
	  << " num " << nfull << "/" << nremote << "/" << nnull
	  << std::endl;
      _decode_bits();
      for (list<fullbit>::iterator p = dfull.begin(); p != dfull.end(); ++p)
	p->print(out);
      for (list<remotebit>::iterator p = dremote.begin(); p != dremote.end(); ++p)
	p->print(out);
      for (list<nullbit>::iterator p = dnull.begin(); p != dnull.end(); ++p)
	p->print(out);
    }

    void _encode_bits() const {
      ::encode(dfull, dnbl);
      ::encode(dremote, dnbl);
      ::encode(dnull, dnbl);
    }
    void _decode_bits() { 
      if (dn_decoded) return;
      bufferlist::iterator p = dnbl.begin();
      ::decode(dfull, p);
      ::decode(dremote, p);
      ::decode(dnull, p);
      dn_decoded = true;
    }

    void encode(bufferlist& bl) const {
      ::encode(fnode, bl);
      ::encode(state, bl);
      ::encode(nfull, bl);
      ::encode(nremote, bl);
      ::encode(nnull, bl);
      _encode_bits();
      ::encode(dnbl, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(fnode, bl);
      ::decode(state, bl);
      ::decode(nfull, bl);
      ::decode(nremote, bl);
      ::decode(nnull, bl);
      ::decode(dnbl, bl);
      dn_decoded = false;      // don't decode bits unless we need them.
    }
  };
  WRITE_CLASS_ENCODER(dirlump)

private:
  // my lumps.  preserve the order we added them in a list.
  list<dirfrag_t>         lump_order;
  map<dirfrag_t, dirlump> lump_map;

  // anchor transactions included in this update.
  list<version_t>         atids;

  // inode dirlocks (scatterlocks) i've touched.
  //map<inodeno_t, utime_t> dirty_inode_mtimes;

  // ino's i've allocated
  list<inodeno_t> allocated_inos;
  version_t alloc_tablev;

  // inodes i've destroyed.
  list< triple<inodeno_t,uint64_t,uint64_t> > truncated_inodes;

  // idempotent op(s)
  list<metareqid_t> client_reqs;

 public:
  void encode(bufferlist& bl) const {
    ::encode(lump_order, bl);
    ::encode(lump_map, bl);
    ::encode(atids, bl);
    //::encode(dirty_inode_mtimes, bl);
    ::encode(allocated_inos, bl);
    if (!allocated_inos.empty())
      ::encode(alloc_tablev, bl);
    ::encode(truncated_inodes, bl);
    ::encode(client_reqs, bl);
  } 
  void decode(bufferlist::iterator &bl) {
    ::decode(lump_order, bl);
    ::decode(lump_map, bl);
    ::decode(atids, bl);
    //::decode(dirty_inode_mtimes, bl);
    ::decode(allocated_inos, bl);
    if (!allocated_inos.empty())
      ::decode(alloc_tablev, bl);
    ::decode(truncated_inodes, bl);
    ::decode(client_reqs, bl);
  }


  // soft state
  off_t last_subtree_map;
  off_t my_offset;

  // for replay, in certain cases
  LogSegment *_segment;

  EMetaBlob() : last_subtree_map(0), my_offset(0), _segment(0) { }
  EMetaBlob(MDLog *mdl);  // defined in journal.cc

  void print(ostream& out) {
    for (list<dirfrag_t>::iterator p = lump_order.begin();
	 p != lump_order.end();
	 ++p) {
      lump_map[*p].print(*p, out);
    }
  }

  void add_client_req(metareqid_t r) {
    client_reqs.push_back(r);
  }

  void add_anchor_transaction(version_t atid) {
    atids.push_back(atid);
  }  

  /*
  void add_dirtied_inode_mtime(inodeno_t ino, utime_t ctime) {
    dirty_inode_mtimes[ino] = ctime;
  }
  */

  void add_allocated_ino(inodeno_t ino, version_t tablev) {
    allocated_inos.push_back(ino);
    alloc_tablev = tablev;
  }

  void add_inode_truncate(inodeno_t ino, uint64_t newsize, uint64_t oldsize) {
    truncated_inodes.push_back(triple<inodeno_t,uint64_t,uint64_t>(ino, newsize, oldsize));
  }
  
  void add_null_dentry(CDentry *dn, bool dirty) {
    add_null_dentry(add_dir(dn->get_dir(), false), dn, dirty);
  }
  void add_null_dentry(dirlump& lump, CDentry *dn, bool dirty) {
    // add the dir
    lump.nnull++;
    if (dirty)
      lump.get_dnull().push_front(nullbit(dn->get_name(), 
					  dn->get_projected_version(), 
					  dirty));
    else
      lump.get_dnull().push_back(nullbit(dn->get_name(), 
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
      rino = dn->get_remote_ino();
      rdt = dn->get_remote_d_type();
    }
    lump.nremote++;
    if (dirty)
      lump.get_dremote().push_front(remotebit(dn->get_name(), 
					      dn->get_projected_version(), 
					      rino, rdt,
					      dirty));
    else
      lump.get_dremote().push_back(remotebit(dn->get_name(), 
					     dn->get_projected_version(), 
					     rino, rdt,
					     dirty));
  }

  // return remote pointer to to-be-journaled inode
  inode_t *add_primary_dentry(CDentry *dn, bool dirty, 
			      CInode *in=0, inode_t *pi=0, fragtree_t *pdft=0) {
    return add_primary_dentry(add_dir(dn->get_dir(), false),
			      dn, dirty, in, pi, pdft);
  }
  inode_t *add_primary_dentry(dirlump& lump, CDentry *dn, bool dirty, 
			      CInode *in=0, inode_t *pi=0, fragtree_t *pdft=0) {
    if (!in) 
      in = dn->get_inode();

    // make note of where this inode was last journaled
    in->last_journaled = my_offset;
    //cout << "journaling " << in->inode.ino << " at " << my_offset << std::endl;

    lump.nfull++;
    if (dirty) {
      lump.get_dfull().push_front(fullbit(dn->get_name(), 
					  dn->get_projected_version(), 
					  in->inode, in->dirfragtree, in->xattrs, in->symlink, 
					  dirty));
      if (pi) lump.get_dfull().front().inode = *pi;
      return &lump.get_dfull().front().inode;
    } else {
      lump.get_dfull().push_back(fullbit(dn->get_name(), 
					 dn->get_projected_version(),
					 in->inode, in->dirfragtree, in->xattrs, in->symlink, 
					 dirty));
      if (pi) lump.get_dfull().back().inode = *pi;
      return &lump.get_dfull().back().inode;
    }
  }

  // convenience: primary or remote?  figure it out.
  inode_t *add_dentry(CDentry *dn, bool dirty) {
    dirlump& lump = add_dir(dn->get_dir(), false);
    return add_dentry(lump, dn, dirty);
  }
  inode_t *add_dentry(dirlump& lump, CDentry *dn, bool dirty) {
    // primary or remote
    if (dn->is_remote()) {
      add_remote_dentry(dn, dirty);
      return 0;
    } else if (dn->is_null()) {
      add_null_dentry(dn, dirty);
      return 0;
    }
    assert(dn->is_primary());
    return add_primary_dentry(dn, dirty);
  }

  
  dirlump& add_dir(CDir *dir, bool dirty, bool complete=false) {
    return add_dir(dir->dirfrag(), dir->get_projected_fnode(), dir->get_projected_version(),
		   dirty, complete);
  }
  dirlump& add_dir(dirfrag_t df, fnode_t *pf, version_t pv, bool dirty, bool complete=false) {
    if (lump_map.count(df) == 0) {
      lump_order.push_back(df);
      lump_map[df].fnode = *pf;
      lump_map[df].fnode.version = pv;
    }
    dirlump& l = lump_map[df];
    if (complete) l.mark_complete();
    if (dirty) l.mark_dirty();
    return l;
  }
  
  static const int TO_AUTH_SUBTREE_ROOT = 0;  // default.
  static const int TO_ROOT = 1;
  
  void add_dir_context(CDir *dir, int mode = TO_AUTH_SUBTREE_ROOT) {
    // already have this dir?  (we must always add in order)
    if (lump_map.count(dir->dirfrag())) 
      return;

    if (mode == TO_AUTH_SUBTREE_ROOT) {
      //return;  // hack: for comparison purposes.. what if NO context?

      // subtree root?
      if (dir->is_subtree_root() && dir->is_auth())
	return;

      // was the inode journaled since the last subtree_map?
      if (//false &&  // for benchmarking
	  last_subtree_map &&
	  dir->inode->last_journaled >= last_subtree_map) {
	/*
	cout << " inode " << dir->inode->inode.ino 
	     << " last journaled at " << dir->inode->last_journaled
	     << " and last_subtree_map is " << last_subtree_map 
	     << std::endl;
	*/
	return;
      }
    }
    
    // stop at root/stray
    CInode *diri = dir->get_inode();
    if (!diri->get_parent_dn())
      return;

    // journaled?

    // add parent dn
    CDentry *parent = diri->get_projected_parent_dn();
    add_dir_context(parent->get_dir(), mode);
    add_dentry(parent, false);
  }



 
  void print(ostream& out) const {
    out << "[metablob";
    if (!lump_order.empty()) 
      out << " " << lump_order.front() << ", " << lump_map.size() << " dirs";
    if (!atids.empty())
      out << " atids=" << atids;
    if (!allocated_inos.empty())
      out << " inos=" << allocated_inos << " v" << alloc_tablev;
    out << "]";
  }

  void update_segment(LogSegment *ls);
  void replay(MDS *mds, LogSegment *ls=0);
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
