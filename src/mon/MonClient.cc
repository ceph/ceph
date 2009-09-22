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
#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"
#include "messages/MAuthRotating.h"
#include "common/ConfUtils.h"

#include "MonClient.h"
#include "MonMap.h"

#include "auth/Auth.h"
#include "auth/AuthProtocol.h"
#include "auth/KeysServer.h"

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
    char buf[80];
    cerr << "unable to read monmap from " << monmap_fn << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
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
  
  want_monmap = true;
  if (cur_mon < 0)
    _reopen_session();

  while (want_monmap)
    map_cond.Wait(monc_lock);

  dout(10) << "get_monmap done" << dendl;

  return 0;
}

int MonClient::get_monmap_privately()
{
  dout(10) << "get_monmap_privately" << dendl;
  Mutex::Locker l(monc_lock);
  
  SimpleMessenger *rank = NULL; 
  bool temp_msgr = false;
  if (!messenger) {
    rank = new SimpleMessenger;
    rank->bind();
    rank->set_policy(entity_name_t::TYPE_MON, SimpleMessenger::Policy::lossy_fast_fail());
    messenger = rank->register_entity(entity_name_t::CLIENT(-1));
    messenger->add_dispatcher_head(this);
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


bool MonClient::ms_dispatch(Message *m)
{
  dout(10) << "dispatch " << *m << dendl;

  if (my_addr == entity_addr_t())
    my_addr = messenger->get_myaddr();

  dout(0) << "dispatch type=" << m->get_type() << dendl;

  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    dout(0) << "CEPH_MSG_MON_MAP begin" << dendl;
    handle_monmap((MMonMap*)m);
    dout(0) << "CEPH_MSG_MON_MAP end" << dendl;
    return true;

  case CEPH_MSG_CLIENT_MOUNT_ACK:
    handle_mount_ack((MClientMountAck*)m);
    return true;

  case CEPH_MSG_AUTH_REPLY:
    handle_auth((MAuthReply*)m);
    return true;

  case CEPH_MSG_MON_SUBSCRIBE_ACK:
    handle_subscribe_ack((MMonSubscribeAck*)m);
    return true;

  case MSG_AUTH_ROTATING:
    handle_auth_rotating_response((MAuthRotating*)m);
    return true;
  }


  return false;
}

void MonClient::handle_monmap(MMonMap *m)
{
  dout(10) << "handle_monmap " << *m << dendl;
  monc_lock.Lock();

  _finish_hunting();

  bufferlist::iterator p = m->monmapbl.begin();
  ::decode(monmap, p);

  _sub_got("monmap", monmap.get_epoch());

  map_cond.Signal();
  want_monmap = false;

  monc_lock.Unlock();
  delete m;
}

// ----------------------
// mount

void MonClient::_send_mount()
{
  dout(10) << "_send_mount" << dendl;
  _send_mon_message(new MClientMount);
  mount_started = g_clock.now();
}

void MonClient::init()
{
  dout(10) << "init" << dendl;
  messenger->add_dispatcher_head(this);

  Mutex::Locker l(monc_lock);
  timer.add_event_after(10.0, new C_Tick(this));
}

void MonClient::shutdown()
{
  timer.cancel_all_events();
}

int MonClient::mount(double mount_timeout)
{
  Mutex::Locker lock(monc_lock);

  if (clientid >= 0) {
    dout(5) << "already mounted" << dendl;;
    return 0;
  }

  // only first mounter does the work
  mounting++;
  if (mounting == 1) {
    if (cur_mon < 0)
      _reopen_session();
    else
      _send_mount();
  }
  while (clientid < 0 && !mount_err)
    mount_cond.Wait(monc_lock);
  mounting--;

  if (clientid >= 0) {
    dout(5) << "mount success, client" << clientid << dendl;

    _sub_want("monmap", monmap.get_epoch());
  }

  return mount_err;
}

void MonClient::handle_mount_ack(MClientMountAck* m)
{
  dout(10) << "handle_mount_ack " << *m << dendl;

  _finish_hunting();

  // monmap
  bufferlist::iterator p = m->monmap_bl.begin();
  ::decode(monmap, p);

  clientid = m->client;
  messenger->set_myname(entity_name_t::CLIENT(m->client.v));

  mount_cond.SignalAll();
  delete m;
}

void MonClient::handle_auth(MAuthReply *m)
{
  int ret = auth.handle_response((MAuthReply*)m);
  delete m;

  if (ret == -EAGAIN) {
    auth.send_session_request(this, &auth_handler, 30.0);
  } else {
    state = MC_STATE_AUTHENTICATED;
    _reopen_session();
  }
}


int MonClient::authenticate(double timeout)
{
  Mutex::Locker lock(monc_lock);

  auth_timeout = timeout;

  return auth.start_session(this, timeout);
}

// ---------

void MonClient::_send_mon_message(Message *m)
{
  assert(cur_mon >= 0);
  messenger->send_message(m, monmap.mon_inst[cur_mon]);
}

void MonClient::send_message(Message *m)
{
  _send_mon_message(m);
}

void MonClient::_pick_new_mon()
{
  if (cur_mon >= 0)
    messenger->mark_down(monmap.get_inst(cur_mon).addr);
  cur_mon = monmap.pick_mon(true);
  dout(10) << "_pick_new_mon picked mon" << cur_mon << dendl;
}

void MonClient::_reopen_session()
{
  dout(10) << "_reopen_session" << dendl;
  _pick_new_mon();

  dout(0) << "_reopen_session 0" << dendl;
  if (state == MC_STATE_NONE) {
    state = MC_STATE_AUTHENTICATING;
    auth.send_session_request(this, &auth_handler, 30.0);
    return;
  }

  if (state == MC_STATE_AUTHENTICATING)
    return;

  if (keyring && keyring->need_rotating_secrets())
    _start_auth_rotating(KEY_ROTATE_TIME);
  dout(0) << "_reopen_session 2" << dendl;

  if (mounting)
    _send_mount();
  if (!sub_have.empty())
    _renew_subs();
  if (!mounting && sub_have.empty()) {
    _send_mon_message(new MMonGetMap);
  }
}

bool MonClient::ms_handle_reset(Connection *con, const entity_addr_t& peer)
{
  dout(10) << "ms_handle_reset " << peer << dendl;
  if (hunting)
    return true;

  dout(0) << "hunting for new mon" << dendl;
  hunting = true;
  _reopen_session();
  return false;
}

void MonClient::_finish_hunting()
{
  if (hunting) {
    dout(0) << "found mon" << cur_mon << dendl; 
    hunting = false;
  }
}

void MonClient::tick()
{
  dout(10) << "tick" << dendl;

  if (keyring && keyring->need_rotating_secrets()) {
    dout(0) << "MonClient::tick: need rotating secret" << dendl;
    _start_auth_rotating(KEY_ROTATE_TIME);
  }

  if (hunting) {
    dout(0) << "continuing hunt" << dendl;
    auth.start_session(this, 30.0);
    _reopen_session();
  } else {
    // just renew as needed
    utime_t now = g_clock.now();
    if (now > sub_renew_after)
      _renew_subs();

    int oldmon = monmap.pick_mon();
    messenger->send_keepalive(monmap.mon_inst[oldmon]);
  }

  auth.tick();

  timer.add_event_after(10.0, new C_Tick(this));
}


// ---------

void MonClient::_renew_subs()
{
  if (sub_have.empty()) {
    dout(10) << "renew_subs - empty" << dendl;
    return;
  }

  dout(10) << "renew_subs" << dendl;
  if (cur_mon < 0)
    _reopen_session();
  else {
    if (sub_renew_sent == utime_t())
      sub_renew_sent = g_clock.now();

    MMonSubscribe *m = new MMonSubscribe;
    m->what = sub_have;
    _send_mon_message(m);
  }
}

void MonClient::handle_subscribe_ack(MMonSubscribeAck *m)
{
  _finish_hunting();

  if (sub_renew_sent != utime_t()) {
    sub_renew_after = sub_renew_sent;
    sub_renew_after += m->interval / 2.0;
    dout(10) << "handle_subscribe_ack sent " << sub_renew_sent << " renew after " << sub_renew_after << dendl;
    sub_renew_sent = utime_t();
  } else {
    dout(10) << "handle_subscribe_ack sent " << sub_renew_sent << ", ignoring" << dendl;
  }

  delete m;
}

int MonClient::authorize(double timeout)
{
  return auth.authorize(CEPHX_PRINCIPAL_MON, timeout);
}

int MonClient::_start_auth_rotating(double timeout)
{
  if (entity_name.entity_type != CEPHX_PRINCIPAL_OSD)
    return 0;

  MAuthRotating *m = new MAuthRotating();
  if (!m)
    return -ENOMEM;

  m->entity_name = entity_name;

  _send_mon_message(m);

  auth_timeout_event = new C_AuthRotatingTimeout(this, timeout);
  if (!auth_timeout_event)
    return -ENOMEM;
  timer.add_event_after(timeout, auth_timeout_event);

  dout(0) << "MonClient::_start_auth_rotating waiting" << dendl;
  auth_cond.Wait(monc_lock);
  dout(0) << "MonClient::_start_auth_rotating wait ended" << dendl;

  if (auth_got_timeout) {
    dout(0) << "MonClient::_start_auth_rotating got timeout" << dendl;
    return -ETIMEDOUT;
  }

  if (auth_timeout_event) {
    timer.cancel_event(auth_timeout_event);
    auth_timeout_event = NULL;
  }

  return 0;
}

void MonClient::handle_auth_rotating_response(MAuthRotating *m)
{
  Mutex::Locker l(monc_lock);

  if (auth_got_timeout)
    return;

  auth_cond.Signal();

  assert(keyring);

  dout(0) << "MonClient::handle_auth_rotating_response got_response status=" << m->status << " length=" << m->response_bl.length() << dendl;

  if (!m->status) {
    RotatingSecrets secrets;
    CryptoKey secret_key;
    keyring->get_master(secret_key);
    bufferlist::iterator iter = m->response_bl.begin();
    if (decode_decrypt(secrets, secret_key, iter) == 0) {
      keyring->set_rotating(secrets);
    } else {
      derr(0) << "could not set rotating key: decode_decrypt failed" << dendl;
    }
  }
}

