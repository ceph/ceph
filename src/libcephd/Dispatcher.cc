// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "msg/Messenger.h"

#include "Dispatcher.h"


#define dout_subsys ceph_subsys_osd

#undef dout_prefix
#define dout_prefix (*_dout << "libosd ")

namespace ceph
{
namespace osd
{

Dispatcher::Dispatcher(CephContext *cct, Messenger *ms, ConnectionRef conn)
  : ::Dispatcher(cct),
    ms(ms),
    conn(conn),
    next_tid(0)
{
}

void Dispatcher::send_request(Message *m, OnReply *c)
{
  const ceph_tid_t tid = next_tid++;
  m->set_tid(tid);

  m->libosd_context = c;

  // send to server messenger
//  conn.get()->send_message(m);
  ms->send_message(m, conn.get());
}

bool Dispatcher::ms_dispatch(Message *m)
{
  ldout(cct, 10) << "ms_dispatch " << *m << dendl;

  assert(m->libosd_context);
  OnReply *c = reinterpret_cast<OnReply*>(m->libosd_context);
  if (c == nullptr)
    return false;

  c->on_reply(m);
  return true;
}

} // namespace osd
} // namespace ceph
