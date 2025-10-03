// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "msg/Connection.h"
#include "msg/Messenger.h"


bool Connection::is_blackhole() const {
  auto& conf = msgr->cct->_conf;

  switch (peer_type) {
  case CEPH_ENTITY_TYPE_MON:
    return conf->ms_blackhole_mon;
  case CEPH_ENTITY_TYPE_OSD:
    return conf->ms_blackhole_osd;
  case CEPH_ENTITY_TYPE_MDS:
    return conf->ms_blackhole_mds;
  case CEPH_ENTITY_TYPE_CLIENT:
    return conf->ms_blackhole_client;
  default:
    return false;
  }
}
