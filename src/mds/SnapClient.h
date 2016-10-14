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

#ifndef CEPH_SNAPCLIENT_H
#define CEPH_SNAPCLIENT_H

#include "MDSTableClient.h"
#include "snap.h"

class MDSInternalContextBase;
class MDSRank;
class LogSegment;

class SnapClient : public MDSTableClient {
public:
  explicit SnapClient(MDSRank *m) : MDSTableClient(m, TABLE_SNAP) {}

  void resend_queries() {}
  void handle_query_result(MMDSTableRequest *m) {}

  void prepare_create(inodeno_t dirino, const string& name, utime_t stamp,
		      version_t *pstid, bufferlist *pbl, MDSInternalContextBase *onfinish) {
    bufferlist bl;
    __u32 op = TABLE_OP_CREATE;
    ::encode(op, bl);
    ::encode(dirino, bl);
    ::encode(name, bl);
    ::encode(stamp, bl);
    _prepare(bl, pstid, pbl, onfinish);
  }

  void prepare_create_realm(inodeno_t ino, version_t *pstid, bufferlist *pbl, MDSInternalContextBase *onfinish) {
    bufferlist bl;
    __u32 op = TABLE_OP_CREATE;
    ::encode(op, bl);
    ::encode(ino, bl);
    _prepare(bl, pstid, pbl, onfinish);
  }

  void prepare_destroy(inodeno_t ino, snapid_t snapid, version_t *pstid, bufferlist *pbl, MDSInternalContextBase *onfinish) {
    bufferlist bl;
    __u32 op = TABLE_OP_DESTROY;
    ::encode(op, bl);
    ::encode(ino, bl);
    ::encode(snapid, bl);
    _prepare(bl, pstid, pbl, onfinish);
  }

  void prepare_update(inodeno_t ino, snapid_t snapid, const string& name, utime_t stamp,
		      version_t *pstid, bufferlist *pbl, MDSInternalContextBase *onfinish) {
    bufferlist bl;
    __u32 op = TABLE_OP_UPDATE;
    ::encode(op, bl);
    ::encode(ino, bl);
    ::encode(snapid, bl);
    ::encode(name, bl);
    ::encode(stamp, bl);
    _prepare(bl, pstid, pbl, onfinish);
  }
};

#endif
