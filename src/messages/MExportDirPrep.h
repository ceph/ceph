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
  map<inodeno_t,nstring>          inode_dentry;

  map<inodeno_t,list<frag_t> >   frags_by_ino;
  map<dirfrag_t,CDirDiscover*>   dirfrags;

  set<__s32>                       bystanders;

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
  nstring& get_dentry(inodeno_t ino) {
    return inode_dentry[ino];
  }
  bool have_dirfrag(dirfrag_t df) {
    return dirfrags.count(df);
  }
  CDirDiscover* get_dirfrag_discover(dirfrag_t df) {
    return dirfrags[df];
  }
  set<__s32> &get_bystanders() { return bystanders; }

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
  void add_inode(dirfrag_t df, const nstring& name, CDentryDiscover *dn, CInodeDiscover *in) {
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
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
    ::decode(bounds, p);
    ::decode(inodes, p);
    ::decode(dentries, p);
    ::decode(inode_dirfrag, p);
    ::decode(inode_dentry, p);
    ::decode(frags_by_ino, p);
    ::decode(dirfrags, p);
    ::decode(bystanders, p);
  }

  virtual void encode_payload() {
    ::encode(dirfrag, payload);
    ::encode(bounds, payload);
    ::encode(inodes, payload);
    ::encode(dentries, payload);
    ::encode(inode_dirfrag, payload);
    ::encode(inode_dentry, payload);
    ::encode(frags_by_ino, payload);
    ::encode(dirfrags, payload);
    ::encode(bystanders, payload);
  }
};

#endif
