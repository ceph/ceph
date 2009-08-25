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

#include "msg/SimpleMessenger.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonMap.h"
#include "messages/MClientMount.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"

#include "include/nstring.h"

#include "messages/MClientMountAck.h"
#include "messages/MClientUnmount.h"
#include "common/ConfUtils.h"

#include "MonClient.h"
#include "MonMap.h"

#include "auth/Auth.h"

#include "config.h"


#define DOUT_SUBSYS monc
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "monclient: "


/*
 * build an initial monmap with any known monitor
 * addresses.
 */
int MonClient::build_initial_monmap()
{
  // file?
  if (g_conf.monmap) {
    const char *monmap_fn = g_conf.monmap;
    int r = monmap.read(monmap_fn);
    if (r >= 0)
      return 0;
    cerr << "unable to read monmap from " << monmap_fn << ": " << strerror(errno) << std::endl;
  }

  // -m foo?
  if (g_conf.mon_host) {
    vector<entity_addr_t> addrs;
    if (parse_ip_port_vec(g_conf.mon_host, addrs)) {
      for (unsigned i=0; i<addrs.size(); i++) {
	entity_inst_t inst;
	inst.name = entity_name_t::MON(i);
	inst.addr = addrs[i];
	monmap.add_mon(inst);
      }
      return 0;
    }
    cerr << "unable to parse addrs in '" << g_conf.mon_host << "'" << std::endl;
  }

  // config file?
  ConfFile a(g_conf.conf);
  ConfFile b("ceph.conf");
  ConfFile *c = 0;
  
  if (a.parse())
    c = &a;
  else if (b.parse())
    c = &b;
  if (c) {
    static string monstr;
    for (int i=0; i<15; i++) {
      char monname[20];
      char *val = 0;
      sprintf(monname, "mon%d", i);
      c->read(monname, "mon addr", &val, 0);
      if (!val || !val[0])
	break;
      
      entity_inst_t inst;
      if (!parse_ip_port(val, inst.addr)) {
	cerr << "unable to parse conf's addr for " << monname << " (" << val << ")" << std::endl;
	continue;
      }
      inst.name = entity_name_t::MON(monmap.mon_inst.size());
      monmap.add_mon(inst);
    }
    if (monmap.size())
      return 0;
    cerr << "unable to find any monitors in conf" << std::endl;
    return -EINVAL;
  }

  cerr << "please specify monitors via -m monaddr or -c ceph.conf" << std::endl;
  return -ENOENT;
}

int MonClient::get_monmap()
{
  dout(10) << "get_monmap" << dendl;
  Mutex::Locker l(monc_lock);
  
  SimpleMessenger *rank = NULL; 
  bool temp_msgr = false;
  if (!messenger) {
    rank = new SimpleMessenger;
    rank->bind();
    messenger = rank->register_entity(entity_name_t::CLIENT(-1));
    messenger->set_dispatcher(this);
    rank->start(true);  // do not daemonize!
    temp_msgr = true; 
  }
  
  int attempt = 10;
  int i = 0;

  srand(getpid());
  dout(10) << "have " << monmap.epoch << dendl;
    
  while (monmap.epoch == 0) {
    i = rand() % monmap.mon_inst.size();
    dout(10) << "querying " << monmap.mon_inst[i] << dendl;
    messenger->send_message(new MMonGetMap, monmap.mon_inst[i]);
    
    if (--attempt == 0)
      break;
    
    utime_t interval(1, 0);
    map_cond.WaitInterval(monc_lock, interval);
  }
  
  if (temp_msgr) {
    messenger->shutdown();
    rank->wait();
    messenger->destroy();
    messenger = 0;
  }

  if (monmap.epoch)
    return 0;
  return -1;
}


bool MonClient::dispatch_impl(Message *m)
{
  dout(10) << "dispatch " << *m << dendl;

  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    handle_monmap((MMonMap*)m);
    return true;

  case CEPH_MSG_CLIENT_MOUNT_ACK:
    handle_mount_ack((MClientMountAck*)m);
    return true;

  case CEPH_MSG_CLIENT_UNMOUNT:
    handle_unmount((MClientUnmount*)m);
    return true;
  }

  return false;
}

void MonClient::handle_monmap(MMonMap *m)
{
  dout(10) << "handle_monmap " << *m << dendl;
  monc_lock.Lock();
  bufferlist::iterator p = m->monmapbl.begin();
  ::decode(monmap, p);
  map_cond.Signal();
  monc_lock.Unlock();
  delete m;
}



// -------------------
// MOUNT

void MonClient::_try_mount(double timeout)
{
  dout(10) << "_try_mount" << dendl;
  int mon = monmap.pick_mon();
  dout(2) << "sending client_mount to mon" << mon << dendl;
  messenger->set_dispatcher(this);
  messenger->send_message(new MClientMount, monmap.get_inst(mon));

  // schedule timeout?
  assert(mount_timeout_event == 0);
  mount_timeout_event = new C_MountTimeout(this, timeout);
  timer.add_event_after(timeout, mount_timeout_event);
}

void MonClient::_mount_timeout(double timeout)
{
  dout(10) << "_mount_timeout" << dendl;
  mount_timeout_event = 0;
  _try_mount(timeout);
}

int MonClient::mount(double mount_timeout)
{
  Mutex::Locker lock(monc_lock);

  if (mounted) {
    dout(5) << "already mounted" << dendl;;
    return 0;
  }

  // only first mounter does the work
  bool itsme = false;
  if (!mounters) {
    itsme = true;
    _try_mount(mount_timeout);
  } else {
    dout(5) << "additional mounter" << dendl;
  }
  mounters++;

  while (signed_ticket.length() == 0 ||
	 (!itsme && !mounted))       // non-doers wait a little longer
    mount_cond.Wait(monc_lock);

  if (!itsme) {
    dout(5) << "additional mounter returning" << dendl;
    assert(mounted);
    return 0;
  }

  // finish.
  timer.cancel_event(mount_timeout_event);
  mount_timeout_event = 0;

  mounted = true;
  mount_cond.SignalAll(); // wake up non-doers

  return 0;
}

void MonClient::handle_mount_ack(MClientMountAck* m)
{
  dout(10) << "handle_mount_ack " << *m << dendl;

  // monmap
  bufferlist::iterator p = m->monmap_bl.begin();
  ::decode(monmap, p);

  // ticket
  signed_ticket = m->signed_ticket;

  messenger->reset_myname(m->get_dest());

  mount_cond.Signal();

  delete m;
}


// UNMOUNT
int MonClient::unmount()
{
  Mutex::Locker lock(monc_lock);

  // fixme: this should retry and time out too

  int mon = monmap.pick_mon();
  dout(2) << "sending client_unmount to mon" << mon << dendl;
  messenger->send_message(new MClientUnmount, monmap.get_inst(mon));
  
  while (mounted)
    mount_cond.Wait(monc_lock);

  dout(2) << "unmounted." << dendl;
  return 0;
}

void MonClient::handle_unmount(Message* m)
{
  Mutex::Locker lock(monc_lock);
  mounted = false;
  mount_cond.Signal();
  delete m;
}


// ---------

void MonClient::send_mon_message(Message *m, bool newmon)
{
  Mutex::Locker l(monc_lock);
  int mon = monmap.pick_mon(newmon);
  messenger->send_message(m, monmap.mon_inst[mon]);
}

void MonClient::pick_new_mon()
{
  Mutex::Locker l(monc_lock);
  int oldmon = monmap.pick_mon();
  messenger->mark_down(monmap.get_inst(oldmon).addr);
  monmap.pick_mon(true);
}

void MonClient::_try_do_op(MonClientOpCtx& ctx, double timeout)
{
  dout(10) << "_try_do_op" << dendl;
  int mon = monmap.pick_mon();
  dout(2) << "sending client_mount to mon" << mon << dendl;

  MAuth *msg = new MAuth;
  if (!msg)
    return;

  bufferlist& bl = msg->get_auth_payload();

  EntityName name;
  entity_addr_t my_addr;

  build_authenticate_request(name, my_addr, bl);
  
  messenger->set_dispatcher(this);
  messenger->send_message(msg, monmap.get_inst(mon));

  // schedule timeout?
  assert(ctx.timeout_event == 0);
  ctx.timeout_event = new C_OpTimeout(this, &ctx, timeout);
  timer.add_event_after(timeout, ctx.timeout_event);
}

/*
  get ticket-granting-ticket for this principle
*/
int MonClient::do_op(MonClientOpCtx& ctx, double timeout)
{
  Mutex::Locker lock(monc_lock);

  if (ctx.done) {
    dout(5) << "op already done" << dendl;;
    return 0;
  }

  // only the first does the work
  bool itsme = false;
  if (!ctx.num_waiters) {
    itsme = true;
    _try_do_op(ctx, timeout);
  } else {
    dout(5) << "additional get_tgt" << dendl;
  }
  ctx.num_waiters++;

  while (!ctx.got_data ||
	 (!itsme && !ctx.done)) // non-doers wait a little longer
	ctx.cond.Wait(monc_lock);

  if (!itsme) {
    dout(5) << "additional get_tgt returning" << dendl;
    assert(ctx.got_data);
    return 0;
  }

  // finish.
  timer.cancel_event(ctx.timeout_event);
  ctx.timeout_event = 0;

  ctx.got_data = true;
  ctx.cond.SignalAll(); // wake up non-doers

  return 0;
}

void MonClient::_op_timeout(MonClientOpCtx& ctx, double timeout)
{
  dout(10) << "_op_timeout" << dendl;
  ctx.timeout_event = 0;
  _try_do_op(ctx, timeout);
}

