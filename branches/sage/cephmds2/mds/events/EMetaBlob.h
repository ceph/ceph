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
    fullbit(const string& d, inode_t& i) : dn(d), inode(i) { }
    fullbit(const string& d, inode_t& i, string& sym) : dn(d), inode(i), symlink(sym) { }
    fullbit(bufferlist& bl, int& off) { _decode(bl, off); }
    void _encode(bufferlist& bl) {
      ::_encode(dn, bl);
      bl.append((char*)&inode, sizeof(inode));
      if (inode.is_symlink())
	::_encode(symlink, bl);
    }
    void _decode(bufferlist& bl, int& off) {
      ::_decode(dn, bl, off);
      bl.copy(off, sizeof(inode), (char*)&inode);  
      off += sizeof(inode);
      if (inode.is_symlink())
	::_decode(symlink, bl, off);
    }
  };
  
  /* remotebit - a dentry + remote inode link (i.e. just an ino)
   */
  struct remotebit {
    string dn;
    version_t dnv;
    inodeno_t ino;

    remotebit(const string& d, version_t v, inodeno_t i) : dn(d), dnv(v), ino(i) { }
    remotebit(bufferlist& bl, int& off) { _decode(bl, off); }
    void _encode(bufferlist& bl) {
      ::_encode(dn, bl);
      bl.append((char*)&dnv, sizeof(dnv));
      bl.append((char*)&ino, sizeof(ino));
    }
    void _decode(bufferlist& bl, int& off) {
      ::_decode(dn, bl, off);
      bl.copy(off, sizeof(dnv), (char*)&dnv);
      off += sizeof(dnv);
      bl.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);
    }
  };

  /*
   * nullbit - a null dentry
   */
  struct nullbit {
    string dn;
    version_t dnv;
    nullbit(const string& d, version_t v) : dn(d), dnv(v) { }
    nullbit(bufferlist& bl, int& off) { _decode(bl, off); }
    void _encode(bufferlist& bl) {
      ::_encode(dn, bl);
      bl.append((char*)&dnv, sizeof(dnv));
    }
    void _decode(bufferlist& bl, int& off) {
      ::_decode(dn, bl, off);
      bl.copy(off, sizeof(dnv), (char*)&dnv);
      off += sizeof(dnv);
    }
  };


  /* dirlump - contains metadata for any dir we have contents for.
   */
  struct dirlump {
    static const int STATE_IMPORT =   (1<<0);
    static const int STATE_COMPLETE = (1<<1);

    dirslice_t dirslice;
    version_t  dirv;
    int state;
    int nfull, nremote, nnull;
    bufferlist bfull, bremote, bnull;
    list<fullbit>   dfull;
    list<remotebit> dremote;
    list<nullbit>   dnull;
    
    dirlump() : state(0), nfull(0), nremote(0), nnull(0) { }
    
    bool is_import() { return state & STATE_IMPORT; }
    void mark_import() { state |= STATE_IMPORT; }
    bool is_complete() { return state & STATE_COMPLETE; }
    void mark_complete() { state |= STATE_COMPLETE; }

    void _encode_bits() {
      for (list<fullbit>::iterator p = dfull.begin(); p != dfull.end(); ++p)
	p->_encode(bfull);
      for (list<remotebit>::iterator p = dremote.begin(); p != dremote.end(); ++p)
	p->_encode(bremote);
      for (list<nullbit>::iterator p = dnull.begin(); p != dnull.end(); ++p)
	p->_encode(bnull);
    }
    void _decode_bits() { 
      int off = 0;
      for (int i=0; i<nfull; i++) 
	dfull.push_back(fullbit(bfull, off));
      off = 0;
      for (int i=0; i<nremote; i++) 
	dremote.push_back(remotebit(bremote, off));
      off = 0;
      for (int i=0; i<nnull; i++) 
	dnull.push_back(nullbit(bnull, off));
    }

    void _encode(bufferlist& bl) {
      bl.append((char*)&dirslice, sizeof(dirslice));
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
      bl.copy(off, sizeof(dirslice), (char*)&dirslice);  off += sizeof(dirslice);
      bl.copy(off, sizeof(dirv), (char*)&dirv);  off += sizeof(dirv);
      bl.copy(off, sizeof(state), (char*)&state);  off += sizeof(state);
      bl.copy(off, sizeof(nfull), (char*)&nfull);  off += sizeof(nfull);
      bl.copy(off, sizeof(nremote), (char*)&nremote);  off += sizeof(nremote);
      bl.copy(off, sizeof(nnull), (char*)&nnull);  off += sizeof(nnull);
      ::_decode(bfull, bl, off);
      ::_decode(bremote, bl, off);
      ::_decode(bnull, bl, off);
      // don't decode bits unless we need them.
    }
  };
  
  // my lumps.  preserve the order we added them in a list.
  list<inodeno_t>         lump_order;
  map<inodeno_t, dirlump> lump_map;

 public:
  
  // remote pointer to to-be-journaled inode iff it's a normal (non-remote) dentry
  inode_t *add_dentry(CDentry *dn, CInode *in=0) {
    CDir *dir = dn->get_dir();
    if (!in) in = dn->get_inode();

    // add the dir
    dirlump& lump = add_dir(dir);

    // add the dirbit
    if (dn->is_remote()) {
      lump.dremote.push_back(remotebit(dn->get_name(), dn->get_projected_version(), dn->get_remote_ino()));
      lump.nremote++;
    } 
    else if (!in) {
      lump.dnull.push_back(nullbit(dn->get_name(), dn->get_projected_version()));
      lump.nnull++;
    }
    else {
      lump.dfull.push_back(fullbit(dn->get_name(), in->inode, in->symlink));
      lump.nfull++;
      return &lump.dfull.back().inode;
    }
    return 0;
  }
  
  void add_dir_context(CDir *dir, bool toroot=false) {
    // already have this dir?  (we must always add in order)
    if (lump_map.count(dir->ino())) 
      return;

    CInode *diri = dir->get_inode();
    if (!toroot && 
	(dir->is_import() || dir->is_hashed()))
      return;  // stop at import point
    if (!dir->get_inode()->get_parent_dn())
      return;

    CDentry *parent = diri->get_parent_dn();
    add_dir_context(parent->get_dir(), toroot);
    add_dentry(parent);
  }

  dirlump& add_dir(CDir *dir) {
    if (lump_map.count(dir->ino()) == 0) {
      lump_order.push_back(dir->ino());
      lump_map[dir->ino()].dirv = dir->get_projected_version();
    }
    dirlump& l = lump_map[dir->ino()];
    if (dir->is_complete()) l.mark_complete();
    if (dir->is_import()) l.mark_import();
    return l;
  }
    
  void _encode(bufferlist& bl) {
    int n = lump_map.size();
    bl.append((char*)&n, sizeof(n));
    for (list<inodeno_t>::iterator i = lump_order.begin();
	 i != lump_order.end();
	 ++i) {
      bl.append((char*)&(*i), sizeof(*i));
      lump_map[*i]._encode(bl);
    }
  } 
  void _decode(bufferlist& bl, int& off) {
    int n;
    bl.copy(off, sizeof(n), (char*)&n);  
    off += sizeof(n);
    for (int i=0; i<n; i++) {
      inodeno_t dirino;
      bl.copy(off, sizeof(dirino), (char*)&dirino);
      off += sizeof(dirino);
      lump_order.push_back(dirino);
      lump_map[dirino]._decode(bl, off);
    }
  }
  
  void print(ostream& out) const {
    out << "[metablob " << lump_order.front()
	<< ", " << lump_map.size() << " dirs]";
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
