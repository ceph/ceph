// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "msg/Connection.h"
#include "msg/Message.h"
#include "msg/Messenger.h"

Connection::~Connection() = default;

int Connection::send_message(Message* _m) {
  auto m = ceph::ref_t<Message>(_m, false); /* consume ref */
  return send_msg(std::move(m));
}

int Connection::send_message2(const MessageRef& m) {
  return send_msg(MessageRef(m));
}

int Connection::send_message2(MessageRef&& m) {
  return send_msg(std::move(m));
}

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
