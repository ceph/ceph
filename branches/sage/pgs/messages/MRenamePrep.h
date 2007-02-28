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


#ifndef __MRENAMEPREP_H
#define __MRENAMEPREP_H

class MRenamePrep : public Message {
  inodeno_t srcdirino;
  string srcname;
  string srcpath;
  inodeno_t destdirino;
  string destname;
  string destpath;
  int initiator;
  int srcauth;

 public:
  int get_initiator() { return initiator; }
  inodeno_t get_srcdirino() { return srcdirino; }
  string& get_srcname() { return srcname; }
  string& get_srcpath() { return srcpath; }
  int get_srcauth() { return srcauth; }
  inodeno_t get_destdirino() { return destdirino; }
  string& get_destname() { return destname; }
  string& get_destpath() { return destpath; }

  MRenamePrep() {}
  MRenamePrep(int initiator,
              inodeno_t srcdirino,
              const string& srcname,
              const string& srcpath,
              inodeno_t destdirino,
              const string& destname,
              const string& destpath,
              int srcauth) :
    Message(MSG_MDS_RENAMEPREP) {
    this->initiator = initiator;
    this->srcdirino = srcdirino;
    this->srcname = srcname;
    this->srcpath = srcpath;
    this->destdirino = destdirino;
    this->destname = destname;
    this->destpath = destpath;
    this->srcauth = srcauth;
  }
  virtual char *get_type_name() { return "RnP";}
  
  virtual void decode_payload(crope& s, int& off) {
    s.copy(off, sizeof(initiator), (char*)&initiator);
    off += sizeof(initiator);
    s.copy(off, sizeof(srcdirino), (char*)&srcdirino);
    off += sizeof(srcdirino);
    s.copy(off, sizeof(destdirino), (char*)&destdirino);
    off += sizeof(destdirino);
    _unrope(srcname, s, off);
    _unrope(srcpath, s, off);
    _unrope(destname, s, off);
    _unrope(destpath, s, off);
    s.copy(off, sizeof(srcauth), (char*)&srcauth);
    off += sizeof(srcauth);
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&initiator,sizeof(initiator));
    s.append((char*)&srcdirino,sizeof(srcdirino));
    s.append((char*)&destdirino,sizeof(destdirino));
    _rope(srcname, s);
    _rope(srcpath, s);
    _rope(destname, s);
    _rope(destpath, s);
    s.append((char*)&srcauth, sizeof(srcauth));
  }
};

#endif
