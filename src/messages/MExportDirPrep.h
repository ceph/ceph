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


#ifndef __MEXPORTDIRPREP_H
#define __MEXPORTDIRPREP_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirPrep : public Message {
  dirfrag_t dirfrag;

  /* nested export discover payload.
     not all inodes will have dirs; they may require a separate discover.
     dentries are the links to each inode.
     dirs map includes base dir (ino)
  */
  list<dirfrag_t>                bounds;

  list<CInodeDiscover*>          inodes;
  list<CDentryDiscover*>         dentries;
  map<inodeno_t,dirfrag_t>       inode_dirfrag;
  map<inodeno_t,string>          inode_dentry;

  map<inodeno_t,list<frag_t> >   frags_by_ino;
  map<dirfrag_t,CDirDiscover*>   dirfrags;

  set<int>                       bystanders;

  bool b_did_assim;

 public:
  dirfrag_t get_dirfrag() { return dirfrag; }
  list<dirfrag_t>& get_bounds() { return bounds; }
  list<CInodeDiscover*>& get_inodes() { return inodes; }
  list<CDentryDiscover*>& get_dentries() { return dentries; }
  list<frag_t>& get_inode_dirfrags(inodeno_t ino) { 
    return frags_by_ino[ino];
  }
  dirfrag_t get_containing_dirfrag(inodeno_t ino) {
    return inode_dirfrag[ino];
  }
  string& get_dentry(inodeno_t ino) {
    return inode_dentry[ino];
  }
  bool have_dirfrag(dirfrag_t df) {
    return dirfrags.count(df);
  }
  CDirDiscover* get_dirfrag_discover(dirfrag_t df) {
    return dirfrags[df];
  }
  set<int> &get_bystanders() { return bystanders; }

  bool did_assim() { return b_did_assim; }
  void mark_assim() { b_did_assim = true; }

  MExportDirPrep() {
    b_did_assim = false;
  }
  MExportDirPrep(dirfrag_t df) : 
    Message(MSG_MDS_EXPORTDIRPREP),
    dirfrag(df),
    b_did_assim(false) { }
  ~MExportDirPrep() {
    for (list<CInodeDiscover*>::iterator iit = inodes.begin();
         iit != inodes.end();
         iit++)
      delete *iit;
    for (list<CDentryDiscover*>::iterator p = dentries.begin();
         p != dentries.end();
         p++)
      delete *p;
    for (map<dirfrag_t,CDirDiscover*>::iterator dit = dirfrags.begin();
         dit != dirfrags.end();
         dit++) 
      delete dit->second;
  }


  const char *get_type_name() { return "ExP"; }
  void print(ostream& o) {
    o << "export_prep(" << dirfrag << ")";
  }

  void add_export(dirfrag_t df) {
    bounds.push_back( df );
  }
  void add_inode(dirfrag_t df, const string& name, CDentryDiscover *dn, CInodeDiscover *in) {
    inodes.push_back(in);
    dentries.push_back(dn);
    inode_dirfrag[in->get_ino()] = df;
    inode_dentry[in->get_ino()] = name;
  }
  void add_dirfrag(CDirDiscover *dir) {
    dirfrags[dir->get_dirfrag()] = dir;
    frags_by_ino[dir->get_dirfrag().ino].push_back(dir->get_dirfrag().frag);
  }
  void add_bystander(int who) {
    bystanders.insert(who);
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(dirfrag), (char*)&dirfrag);
    off += sizeof(dirfrag);
    
    ::_decode(bounds, payload, off);
    
    // inodes
    int ni;
    payload.copy(off, sizeof(int), (char*)&ni);
    off += sizeof(int);
    for (int i=0; i<ni; i++) {
      // inode
      CInodeDiscover *in = new CInodeDiscover;
      in->_decode(payload, off);
      inodes.push_back(in);

      // dentry
      CDentryDiscover *dn = new CDentryDiscover;
      dn->_decode(payload, off);
      dentries.push_back(dn);
      
      // dentry
      string d;
      _decode(d, payload, off);
      inode_dentry[in->get_ino()] = d;
      
      // dir ino
      dirfrag_t df;
      payload.copy(off, sizeof(df), (char*)&df);
      off += sizeof(df);
      inode_dirfrag[in->get_ino()] = df;

      // child frags
      ::_decode(frags_by_ino[in->get_ino()], payload, off);
    }

    // dirs
    int nd;
    payload.copy(off, sizeof(int), (char*)&nd);
    off += sizeof(int);
    for (int i=0; i<nd; i++) {
      CDirDiscover *dir = new CDirDiscover;
      dir->_decode(payload, off);
      dirfrags[dir->get_dirfrag()] = dir;
    }
    
    ::_decode(bystanders, payload, off);
  }

  virtual void encode_payload() {
    payload.append((char*)&dirfrag, sizeof(dirfrag));

    ::_encode(bounds, payload);

    // inodes
    int ni = inodes.size();
    payload.append((char*)&ni, sizeof(int));
    list<CDentryDiscover*>::iterator dit = dentries.begin();
    list<CInodeDiscover*>::iterator iit = inodes.begin();
    while (iit != inodes.end()) {
      (*iit)->_encode(payload);
      (*dit)->_encode(payload);

      // dentry name
      _encode(inode_dentry[(*iit)->get_ino()], payload);

      // dir ino
      dirfrag_t df = inode_dirfrag[(*iit)->get_ino()];
      payload.append((char*)&df, sizeof(df));

      // child frags
      ::_encode(frags_by_ino[(*iit)->get_ino()], payload);

      iit++;
      dit++;
    }

    // dirs
    int nd = dirfrags.size();
    payload.append((char*)&nd, sizeof(int));
    for (map<dirfrag_t,CDirDiscover*>::iterator dit = dirfrags.begin();
         dit != dirfrags.end();
         dit++)
      dit->second->_encode(payload);

    ::_encode(bystanders, payload);
  }
};

#endif
