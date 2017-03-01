// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBOSD_DISPATCHER_H
#define CEPH_LIBOSD_DISPATCHER_H

#include <atomic>
#include "msg/Dispatcher.h"

class CephContext;
class OSD;

namespace ceph
{
namespace osd
{

class Dispatcher : public ::Dispatcher {
public:
  struct OnReply {
    virtual ~OnReply() {}
    virtual void on_reply(Message *m) {};
  };

private:
  Messenger *ms;
  ConnectionRef conn;
  std::atomic<ceph_tid_t> next_tid;

public:
//  bool ms_can_fast_dispatch_any() const { return true; }
//  bool ms_can_fast_dispatch(Message *m) const { return true; }
  
  virtual bool ms_handle_refused(Connection *con) {return true;}
  
  Dispatcher(CephContext *cct, Messenger *ms, ConnectionRef conn);

  void send_request(Message *m, OnReply *c);

  bool ms_dispatch(Message *m);

  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
};

} // namespace osd
} // namespace ceph

#endif // CEPH_LIBOSD_DISPATCHER_H
