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
#include "auth/AuthProtocol.h"

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
  MonClientOpHandler *op_handler;
  dout(10) << "dispatch " << *m << dendl;

  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    handle_monmap((MMonMap*)m);
    return true;

  case CEPH_MSG_CLIENT_MOUNT_ACK:
    op_handler = &mount_handler;
    break;

  case CEPH_MSG_CLIENT_UNMOUNT:
    op_handler = &unmount_handler;
    break;

  case CEPH_MSG_AUTH_REPLY:
    op_handler = cur_auth_handler;
    break;

  default:
    return false;
  }

  op_handler->handle_response(m);

  delete m;

  return true;
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

int MonClient::mount(double mount_timeout)
{
  int ret = mount_handler.do_op(mount_timeout);

  dout(0) << "mount ret=" << ret << dendl;

  return ret;
}

int MonClient::unmount(double timeout)
{
  return unmount_handler.do_op(timeout);
}

int MonClient::authorize(uint32_t want_keys, double mount_timeout)
{
  Mutex::Locker l(auth_lock);
  int ret;

  auth_client_handler.set_request_keys(want_keys);

  do {
    MonClientAuthHandler h(this);

    cur_auth_handler = &h;

    int err = h.do_op(mount_timeout);
    if (err < 0)
      return err;

    ret =  h.get_result();
    dout(0) << "auth ret=" << ret << dendl;
  } while (ret == -EAGAIN);

  return ret;
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

void MonClient::MonClientOpHandler::_try_do_op(double timeout)
{
  dout(10) << "_try_do_op" << dendl;
  int mon = client->monmap.pick_mon();
  dout(2) << "sending client_mount to mon" << mon << dendl;

  Message *msg = build_request();
  
  client->messenger->set_dispatcher(client);
  client->messenger->send_message(msg, client->monmap.get_inst(mon));

  // schedule timeout?
  assert(timeout_event == 0);
  timeout_event = new C_OpTimeout(this, timeout);
  timer.add_event_after(timeout, timeout_event);
}

int MonClient::MonClientOpHandler::do_op(double timeout)
{
  Mutex::Locker lock(op_lock);

  if (done) {
    dout(5) << "op already done" << dendl;;
    return 0;
  }
  // only the first does the work
  bool itsme = false;
  if (!num_waiters) {
    itsme = true;
    _try_do_op(timeout);
  } else {
    dout(5) << "additional doer" << dendl;
  }
  num_waiters++;

  while (!got_response() ||
	 (!itsme && !done)) { // non-doers wait a little longer
	cond.Wait(op_lock);
  }
  num_waiters--;

  if (!itsme) {
    dout(5) << "additional returning" << dendl;
    assert(got_response());
    return 0;
  }

  // finish.
  timer.cancel_event(timeout_event);
  timeout_event = 0;

  done = true;

  cond.SignalAll(); // wake up non-doers


  return 0;
}

void MonClient::MonClientOpHandler::_op_timeout(double timeout)
{
  dout(10) << "_op_timeout" << dendl;
  timeout_event = 0;
  _try_do_op(timeout);
}

// -------------------
// MOUNT
Message *MonClient::MonClientMountHandler::build_request()
{
  return new MClientMount;
}

void MonClient::MonClientMountHandler::handle_response(Message *response)
{
  MClientMountAck* m = (MClientMountAck *)response;
  dout(10) << "handle_mount_ack " << *m << dendl;

  // monmap
  bufferlist::iterator p = m->monmap_bl.begin();
  ::decode(client->monmap, p);

  client->messenger->reset_myname(m->get_dest());

  response_flag = true;

  cond.Signal();
}

Message *MonClient::MonClientUnmountHandler::build_request()
{
  return new MClientUnmount;
}

// -------------------
// UNMOUNT
void MonClient::MonClientUnmountHandler::handle_response(Message *response)
{
  Mutex::Locker lock(op_lock);
  client->mounted = false;
  got_ack = true;
  cond.Signal();
}

// -------------------
// AUTH
Message *MonClient::MonClientAuthHandler::build_request()
{
  MAuth *msg = new MAuth;
  if (!msg)
    return NULL;
  bufferlist& bl = msg->get_auth_payload();

  if (client->auth_client_handler.generate_request(bl) < 0) {
    delete msg;
    return NULL;
  }

  return msg;
}

void MonClient::MonClientAuthHandler::handle_response(Message *response)
{
  MAuthReply* m = (MAuthReply *)response;
  Mutex::Locker lock(op_lock);

  dout(0) << "ret=" << m->result << dendl;

  last_result = (int)m->result;

  client->auth_client_handler.handle_response(m->result, m->result_bl);

  cond.Signal();
  return; /* FIXME */
}

