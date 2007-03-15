// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
using namespace std;

#include "../CInode.h"
#include "../CDir.h"
#include "../CDentry.h"


class MDS;

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

  /* fullbit - a regular dentry + inode
   */
  struct fullbit {
    string  dn;         // dentry
    version_t dnv;
    inode_t inode;      // if it's not
    string  symlink;
    bool dirty;

    fullbit(const string& d, version_t v, inode_t& i, bool dr) : dn(d), dnv(v), inode(i), dirty(dr) { }
    fullbit(const string& d, version_t v, inode_t& i, string& sym, bool dr) : dn(d), dnv(v), inode(i), symlink(sym), dirty(dr) { }
    fullbit(bufferlist& bl, int& off) { _decode(bl, off); }
    void _encode(bufferlist& bl) {
      ::_encode(dn, bl);
      bl.append((char*)&dnv, sizeof(dnv));
      bl.append((char*)&inode, sizeof(inode));
      if (inode.is_symlink())
	::_encode(symlink, bl);
      bl.append((char*)&dirty, sizeof(dirty));
    }
    void _decode(bufferlist& bl, int& off) {
      ::_decode(dn, bl, off);
      bl.copy(off, sizeof(dnv), (char*)&dnv);
      off += sizeof(dnv);
      bl.copy(off, sizeof(inode), (char*)&inode);  
      off += sizeof(inode);
      if (inode.is_symlink())
	::_decode(symlink, bl, off);
      bl.copy(off, sizeof(dirty), (char*)&dirty);
      off += sizeof(dirty);
    }
  };
  
  /* remotebit - a dentry + remote inode link (i.e. just an ino)
   */
  struct remotebit {
    string dn;
    version_t dnv;
    inodeno_t ino;
    bool dirty;

    remotebit(const string& d, version_t v, inodeno_t i, bool dr) : dn(d), dnv(v), ino(i), dirty(dr) { }
    remotebit(bufferlist& bl, int& off) { _decode(bl, off); }
    void _encode(bufferlist& bl) {
      ::_encode(dn, bl);
      bl.append((char*)&dnv, sizeof(dnv));
      bl.append((char*)&ino, sizeof(ino));
      bl.append((char*)&dirty, sizeof(dirty));
    }
    void _decode(bufferlist& bl, int& off) {
      ::_decode(dn, bl, off);
      bl.copy(off, sizeof(dnv), (char*)&dnv);
      off += sizeof(dnv);
      bl.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);
      bl.copy(off, sizeof(dirty), (char*)&dirty);
      off += sizeof(dirty);
    }
  };

  /*
   * nullbit - a null dentry
   */
  struct nullbit {
    string dn;
    version_t dnv;
    bool dirty;
    nullbit(const string& d, version_t v, bool dr) : dn(d), dnv(v), dirty(dr) { }
    nullbit(bufferlist& bl, int& off) { _decode(bl, off); }
    void _encode(bufferlist& bl) {
      ::_encode(dn, bl);
      bl.append((char*)&dnv, sizeof(dnv));
      bl.append((char*)&dirty, sizeof(dirty));
    }
    void _decode(bufferlist& bl, int& off) {
      ::_decode(dn, bl, off);
      bl.copy(off, sizeof(dnv), (char*)&dnv);
      off += sizeof(dnv);
      bl.copy(off, sizeof(dirty), (char*)&dirty);
      off += sizeof(dirty);
    }
  };


  /* dirlump - contains metadata for any dir we have contents for.
   */
  struct dirlump {
    static const int STATE_COMPLETE = (1<<1);
    static const int STATE_DIRTY =    (1<<2);  // dirty due to THIS journal item, that is!

    dirfrag_t  dirfrag;
    version_t  dirv;
    int state;
    int nfull, nremote, nnull;
    bufferlist bfull, bremote, bnull;

  private:
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

    void _encode_bits() {
      for (list<fullbit>::iterator p = dfull.begin(); p != dfull.end(); ++p)
	p->_encode(bfull);
      for (list<remotebit>::iterator p = dremote.begin(); p != dremote.end(); ++p)
	p->_encode(bremote);
      for (list<nullbit>::iterator p = dnull.begin(); p != dnull.end(); ++p)
	p->_encode(bnull);
    }
    void _decode_bits() { 
      if (dn_decoded) return;
      int off = 0;
      for (int i=0; i<nfull; i++) 
	dfull.push_back(fullbit(bfull, off));
      off = 0;
      for (int i=0; i<nremote; i++) 
	dremote.push_back(remotebit(bremote, off));
      off = 0;
      for (int i=0; i<nnull; i++) 
	dnull.push_back(nullbit(bnull, off));
      dn_decoded = true;
    }

    void _encode(bufferlist& bl) {
      bl.append((char*)&dirfrag, sizeof(dirfrag));
      bl.append((char*)&dirv, sizeof(dirv));
      bl.append((char*)&state, sizeof(state));
      bl.append((char*)&nfull, sizeof(nfull));
      bl.append((char*)&nremote, sizeof(nremote));
      bl.append((char*)&nnull, sizeof(nnull));
      _encode_bits();
      ::_encode(bfull, bl);
      ::_encode(bremote, bl);
      ::_encode(bnull, bl);
    }
    void _decode(bufferlist& bl, int& off) {
      bl.copy(off, sizeof(dirfrag), (char*)&dirfrag);  off += sizeof(dirfrag);
      bl.copy(off, sizeof(dirv), (char*)&dirv);  off += sizeof(dirv);
      bl.copy(off, sizeof(state), (char*)&state);  off += sizeof(state);
      bl.copy(off, sizeof(nfull), (char*)&nfull);  off += sizeof(nfull);
      bl.copy(off, sizeof(nremote), (char*)&nremote);  off += sizeof(nremote);
      bl.copy(off, sizeof(nnull), (char*)&nnull);  off += sizeof(nnull);
      ::_decode(bfull, bl, off);
      ::_decode(bremote, bl, off);
      ::_decode(bnull, bl, off);
      // don't decode bits unless we need them.
      dn_decoded = false;
    }
  };
  
  // my lumps.  preserve the order we added them in a list.
  list<dirfrag_t>         lump_order;
  map<dirfrag_t, dirlump> lump_map;

  // anchor transactions included in this update.
  list<version_t>         atids;

  // inodes i've destroyed.
  list<inode_t>           destroyed_inodes;

 public:

  void add_anchor_transaction(version_t atid) {
    atids.push_back(atid);
  }  

  void add_destroyed_inode(const inode_t& inode) {
    destroyed_inodes.push_back(inode);
  }
  
  void add_null_dentry(CDentry *dn, bool dirty) {
    dirlump& lump = add_dir(dn->get_dir(), false);

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

  // return remote pointer to to-be-journaled inode iff it's a normal (non-remote) dentry
  inode_t *add_dentry(CDentry *dn, bool dirty, CInode *in=0) {
    CDir *dir = dn->get_dir();
    if (!in) in = dn->get_inode();

    // add the dir
    dirlump& lump = add_dir(dir, false);

    // add the dirbit
    if (dn->is_remote()) {
      lump.nremote++;
      if (dirty)
	lump.get_dremote().push_front(remotebit(dn->get_name(), 
						dn->get_projected_version(), 
						dn->get_remote_ino(), 
						dirty));
      else
	lump.get_dremote().push_back(remotebit(dn->get_name(), 
					       dn->get_projected_version(), 
					       dn->get_remote_ino(), 
					       dirty));
    } 
    else if (!in) {
      add_null_dentry(dn, dirty);
    }
    else {
      lump.nfull++;
      if (dirty) {
	lump.get_dfull().push_front(fullbit(dn->get_name(), 
					    dn->get_projected_version(), 
					    in->inode, in->symlink, 
					    dirty));
	return &lump.get_dfull().front().inode;
      } else {
	lump.get_dfull().push_back(fullbit(dn->get_name(), 
					   dn->get_projected_version(),
					   in->inode, in->symlink, 
					   dirty));
	return &lump.get_dfull().back().inode;
      }
    }
    return 0;
  }
  
  dirlump& add_dir(CDir *dir, bool dirty) {
    dirfrag_t df = dir->dirfrag();
    if (lump_map.count(df) == 0) {
      lump_order.push_back(df);
      lump_map[df].dirv = dir->get_projected_version();
    }
    dirlump& l = lump_map[df];
    if (dir->is_complete()) l.mark_complete();
    if (dirty) l.mark_dirty();
    return l;
  }

  void add_dir_context(CDir *dir, bool toroot=false) {
    // already have this dir?  (we must always add in order)
    if (lump_map.count(dir->dirfrag())) 
      return;

    CInode *diri = dir->get_inode();
    if (!toroot && dir->is_subtree_root() && dir->is_auth())
      return;  // stop at subtree root
    if (!dir->get_inode()->get_parent_dn())
      return;

    CDentry *parent = diri->get_parent_dn();
    add_dir_context(parent->get_dir(), toroot);
    add_dentry(parent, false);
  }


  // encoding

  void _encode(bufferlist& bl) {
    int n = lump_map.size();
    bl.append((char*)&n, sizeof(n));
    for (list<dirfrag_t>::iterator i = lump_order.begin();
	 i != lump_order.end();
	 ++i) {
      bl.append((char*)&(*i), sizeof(*i));
      lump_map[*i]._encode(bl);
    }
    ::_encode(atids, bl);
    ::_encode(destroyed_inodes, bl);
  } 
  void _decode(bufferlist& bl, int& off) {
    int n;
    bl.copy(off, sizeof(n), (char*)&n);  
    off += sizeof(n);
    for (int i=0; i<n; i++) {
      dirfrag_t dirfrag;
      bl.copy(off, sizeof(dirfrag), (char*)&dirfrag);
      off += sizeof(dirfrag);
      lump_order.push_back(dirfrag);
      lump_map[dirfrag]._decode(bl, off);
    }
    ::_decode(atids, bl, off);
    ::_decode(destroyed_inodes, bl, off);
  }
  
  void print(ostream& out) const {
    out << "[metablob";
    if (!lump_order.empty()) 
      out << lump_order.front() << ", " << lump_map.size() << " dirs";
    if (!atids.empty())
      out << " atids " << atids;
    out << "]";
  }

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
};

inline ostream& operator<<(ostream& out, const EMetaBlob& t) {
  t.print(out);
  return out;
}

#endif
