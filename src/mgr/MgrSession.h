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
struct MgrSession : public RefCountedObjectInstanceSafe<MgrSession> {
  uint64_t global_id = 0;
  EntityName entity_name;
  entity_inst_t inst;

  int osd_id = -1;  ///< osd id (if an osd)

  // mon caps are suitably generic for mgr
  MonCap caps;

  std::set<std::string> declared_types;

  const entity_addr_t& get_peer_addr() {
    return inst.addr;
  }

private:
  friend factory;
  explicit MgrSession(CephContext *cct) : RefCountedObjectInstanceSafe<MgrSession>(cct) {}
  ~MgrSession() override = default;
};

using MgrSessionRef = MgrSession::ref;


#endif
