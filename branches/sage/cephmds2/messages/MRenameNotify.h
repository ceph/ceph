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


#ifndef __MRENAMENOTIFY_H
#define __MRENAMENOTIFY_H

class MRenameNotify : public Message {
  inodeno_t ino;
  inodeno_t srcdirino;
  string srcname;
  inodeno_t destdirino;
  string destname;
  string destdirpath;
  int srcauth;

 public:
  inodeno_t get_ino() { return ino; }
  inodeno_t get_srcdirino() { return srcdirino; }
  string& get_srcname() { return srcname; }
  inodeno_t get_destdirino() { return destdirino; }
  string& get_destname() { return destname; }
  string& get_destdirpath() { return destdirpath; }
  int get_srcauth() { return srcauth; }

  MRenameNotify() {}
  MRenameNotify(inodeno_t ino,
                inodeno_t srcdirino,
                const string& srcname,
                inodeno_t destdirino,
                const string& destdirpath,
                const string& destname,
                int srcauth
                ) :
    Message(MSG_MDS_RENAMENOTIFY) {
    this->ino = ino;
    this->srcdirino = srcdirino;
    this->srcname = srcname;
    this->destdirino = destdirino;
    this->destname = destname;
    this->destdirpath = destdirpath;
    this->srcauth = srcauth;
  }
  virtual char *get_type_name() { return "Rnot";}
  
  virtual void decode_payload(crope& s, int& off) {
    s.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    s.copy(off, sizeof(srcdirino), (char*)&srcdirino);
    off += sizeof(srcdirino);
    s.copy(off, sizeof(destdirino), (char*)&destdirino);
    off += sizeof(destdirino);
    _unrope(srcname, s, off);
    _unrope(destname, s, off);
    _unrope(destdirpath, s, off);
    s.copy(off, sizeof(srcauth), (char*)&srcauth);
    off += sizeof(srcauth);
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&ino,sizeof(ino));
    s.append((char*)&srcdirino,sizeof(srcdirino));
    s.append((char*)&destdirino,sizeof(destdirino));
    _rope(srcname, s);
    _rope(destname, s);
    _rope(destdirpath, s);
    s.append((char*)&srcauth, sizeof(srcauth));
  }
};

#endif
