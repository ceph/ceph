// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGR_MGRSESSION_H
#define CEPH_MGR_MGRSESSION_H

#include "common/RefCountedObj.h"
#include "common/entity_name.h"
#include "msg/msg_types.h"
#include "mon/MonCap.h"


/**
 * Session state associated with the Connection.
 */
struct MgrSession : public RefCountedObject {
  uint64_t global_id = 0;
  EntityName entity_name;
  entity_inst_t inst;

  int osd_id = -1;  ///< osd id (if an osd)

  // mon caps are suitably generic for mgr
  MonCap caps;

  MgrSession(CephContext *cct) : RefCountedObject(cct, 0) {}
  ~MgrSession() override {}
};

typedef boost::intrusive_ptr<MgrSession> MgrSessionRef;


#endif
