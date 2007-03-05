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

#ifndef __MDS_EMOUNT_H
#define __MDS_EMOUNT_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"

class EMount : public LogEvent {
 protected:
  entity_inst_t client_inst;
  bool  mounted;    // mount or unmount
  version_t cmapv;  // client map version

 public:
  EMount() : LogEvent(EVENT_MOUNT) { }
  EMount(entity_inst_t inst, bool m, version_t v) :
    LogEvent(EVENT_MOUNT),
    client_inst(inst),
    mounted(m),
    cmapv(v) {
  }
  
  void encode_payload(bufferlist& bl) {
    bl.append((char*)&client_inst, sizeof(client_inst));
    bl.append((char*)&mounted, sizeof(mounted));
    bl.append((char*)&cmapv, sizeof(cmapv));
  }
  void decode_payload(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(client_inst), (char*)&client_inst);
    off += sizeof(client_inst);
    bl.copy(off, sizeof(mounted), (char*)&mounted);
    off += sizeof(mounted);
    bl.copy(off, sizeof(cmapv), (char*)&cmapv);
    off += sizeof(cmapv);
  }


  void print(ostream& out) {
    if (mounted)
      out << "EMount " << client_inst << " mount cmapv " << cmapv;
    else
      out << "EMount " << client_inst << " unmount cmapv " << cmapv;
  }
  

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
  
};

#endif
