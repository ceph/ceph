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


#ifndef __MEXPORTDIRPREP_H
#define __MEXPORTDIRPREP_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirPrep : public Message {
  inodeno_t ino;

  /* nested export discover payload.
     not all inodes will have dirs; they may require a separate discover.
     dentries are the links to each inode.
     dirs map includes base dir (ino)
  */
  list<inodeno_t>                exports;

  list<CInodeDiscover*>          inodes;
  map<inodeno_t,inodeno_t>       inode_dirino;
  map<inodeno_t,string>          inode_dentry;

  map<inodeno_t,CDirDiscover*>   dirs;

  bool b_did_assim;

 public:
  inodeno_t get_ino() { return ino; }
  list<inodeno_t>& get_exports() { return exports; }
  list<CInodeDiscover*>& get_inodes() { return inodes; }
  inodeno_t get_containing_dirino(inodeno_t ino) {
    return inode_dirino[ino];
  }
  string& get_dentry(inodeno_t ino) {
    return inode_dentry[ino];
  }
  bool have_dir(inodeno_t ino) {
    return dirs.count(ino);
  }
  CDirDiscover* get_dir(inodeno_t ino) {
    return dirs[ino];
  }

  bool did_assim() { return b_did_assim; }
  void mark_assim() { b_did_assim = true; }

  MExportDirPrep() {
    b_did_assim = false;
  }
  MExportDirPrep(CInode *in) : 
    Message(MSG_MDS_EXPORTDIRPREP) {
    ino = in->ino();
    b_did_assim = false;
  }
  ~MExportDirPrep() {
    for (list<CInodeDiscover*>::iterator iit = inodes.begin();
         iit != inodes.end();
         iit++)
      delete *iit;
    for (map<inodeno_t,CDirDiscover*>::iterator dit = dirs.begin();
         dit != dirs.end();
         dit++) 
      delete dit->second;
  }


  virtual char *get_type_name() { return "ExP"; }




  void add_export(inodeno_t dirino) {
    exports.push_back( dirino );
  }
  void add_inode(inodeno_t dirino, const string& dentry, CInodeDiscover *in) {
    inodes.push_back(in);
    inode_dirino.insert(pair<inodeno_t, inodeno_t>(in->get_ino(), dirino));
    inode_dentry.insert(pair<inodeno_t, string>(in->get_ino(), dentry));
  }
  void add_dir(CDirDiscover *dir) {
    dirs.insert(pair<inodeno_t, CDirDiscover*>(dir->get_ino(), dir));
  }


  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    
    // exports
    int ne;
    payload.copy(off, sizeof(int), (char*)&ne);
    off += sizeof(int);
    for (int i=0; i<ne; i++) {
      inodeno_t ino;
      payload.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);
      exports.push_back(ino);
    }

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
      string d;
      _decode(d, payload, off);
      inode_dentry[in->get_ino()] = d;
      
      // dir ino
      inodeno_t dino;
      payload.copy(off, sizeof(dino), (char*)&dino);
      off += sizeof(dino);
      inode_dirino[in->get_ino()] = dino;
    }

    // dirs
    int nd;
    payload.copy(off, sizeof(int), (char*)&nd);
    off += sizeof(int);
    for (int i=0; i<nd; i++) {
      CDirDiscover *dir = new CDirDiscover;
      dir->_decode(payload, off);
      dirs[dir->get_ino()] = dir;
    }
  }

  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));

    // exports
    int ne = exports.size();
    payload.append((char*)&ne, sizeof(int));
    for (list<inodeno_t>::iterator it = exports.begin();
         it != exports.end();
         it++) {
      inodeno_t ino = *it;
      payload.append((char*)&ino, sizeof(ino));
    }

    // inodes
    int ni = inodes.size();
    payload.append((char*)&ni, sizeof(int));
    for (list<CInodeDiscover*>::iterator iit = inodes.begin();
         iit != inodes.end();
         iit++) {
      (*iit)->_encode(payload);
      
      // dentry
      _encode(inode_dentry[(*iit)->get_ino()], payload);

      // dir ino
      inodeno_t ino = inode_dirino[(*iit)->get_ino()];
      payload.append((char*)&ino, sizeof(ino));
    }

    // dirs
    int nd = dirs.size();
    payload.append((char*)&nd, sizeof(int));
    for (map<inodeno_t,CDirDiscover*>::iterator dit = dirs.begin();
         dit != dirs.end();
         dit++)
      dit->second->_encode(payload);
  }
};

#endif
