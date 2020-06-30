// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "msg/Connection.h"
#include "msg/Messenger.h"


bool Connection::is_blackhole() const {
  auto& conf = msgr->cct->_conf;
  return ((conf->ms_blackhole_mon && peer_type == CEPH_ENTITY_TYPE_MON) ||
      (conf->ms_blackhole_osd && peer_type == CEPH_ENTITY_TYPE_OSD) ||
      (conf->ms_blackhole_mds && peer_type == CEPH_ENTITY_TYPE_MDS) ||
      (conf->ms_blackhole_client && peer_type == CEPH_ENTITY_TYPE_CLIENT));
}
