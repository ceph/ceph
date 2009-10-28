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

// TODO: missing run() method, which creates the two main timers, refreshTimer and readTimer

#include "Monitor.h"

#include "osd/OSDMap.h"

#include "MonitorStore.h"

#include "msg/Messenger.h"

#include "messages/PaxosServiceMessage.h"
#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonObserve.h"
#include "messages/MMonObserveNotify.h"

#include "messages/MMonPaxos.h"
#include "messages/MClass.h"
#include "messages/MRoute.h"

#include "messages/MClientMountAck.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"

#include "messages/MAuthReply.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "MonmapMonitor.h"
#include "ClientMonitor.h"
#include "PGMonitor.h"
#include "LogMonitor.h"
#include "ClassMonitor.h"
#include "AuthMonitor.h"

#include "osd/OSDMap.h"

#include "config.h"

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(this)
static ostream& _prefix(Monitor *mon) {
  return *_dout << dbeginl
		<< "mon" << mon->whoami
		<< (mon->is_starting() ?
		    (const char*)"(starting)" : 
		    (mon->is_leader() ?
		     (const char*)"(leader)" :
		     (mon->is_peon() ? 
		      (const char*)"(peon)" : 
		      (const char*)"(?\?)")))
		<< " ";
}

Monitor::Monitor(int w, MonitorStore *s, Messenger *m, MonMap *map) :
  whoami(w), 
  messenger(m),
  lock("Monitor::lock"),
  monmap(map),
  logclient(messenger, monmap),
  timer(lock), tick_timer(0),
  store(s),
  
  state(STATE_STARTING), stopping(false),
  
  elector(this, w),
  mon_epoch(0), 
  leader(0),

  paxos(PAXOS_NUM), paxos_service(PAXOS_NUM)
{
  paxos_service[PAXOS_MDSMAP] = new MDSMonitor(this, add_paxos(PAXOS_MDSMAP));
  paxos_service[PAXOS_MONMAP] = new MonmapMonitor(this, add_paxos(PAXOS_MONMAP));
  paxos_service[PAXOS_OSDMAP] = new OSDMonitor(this, add_paxos(PAXOS_OSDMAP));
  paxos_service[PAXOS_CLIENTMAP] = new ClientMonitor(this, add_paxos(PAXOS_CLIENTMAP));
  paxos_service[PAXOS_PGMAP] = new PGMonitor(this, add_paxos(PAXOS_PGMAP));
  paxos_service[PAXOS_LOG] = new LogMonitor(this, add_paxos(PAXOS_LOG));
  paxos_service[PAXOS_CLASS] = new ClassMonitor(this, add_paxos(PAXOS_CLASS));
  paxos_service[PAXOS_AUTH] = new AuthMonitor(this, add_paxos(PAXOS_AUTH));
}

Paxos *Monitor::add_paxos(int type)
{
  Paxos *p = new Paxos(this, whoami, type);
  paxos[type] = p;
  return p;
}

Monitor::~Monitor()
{
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    delete *p;
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    delete *p;
  if (messenger)
    messenger->destroy();
}

void Monitor::init()
{
  lock.Lock();
  
  dout(1) << "init fsid " << monmap->fsid << dendl;
  
  // init paxos
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->init();

  for (vector<PaxosService*>::iterator ps = paxos_service.begin(); ps != paxos_service.end(); ps++)
    (*ps)->init();

  logclient.set_synchronous(true);
  
  // i'm ready!
  messenger->add_dispatcher_tail(this);
  messenger->add_dispatcher_head(&logclient);
  
  // start ticker
  reset_tick();
  
  // call election?
  if (monmap->size() > 1) {
    call_election();
  } else {
    // we're standalone.
    set<int> q;
    q.insert(whoami);
    win_election(1, q);
  }
  
  lock.Unlock();
}

void Monitor::shutdown()
{
  dout(1) << "shutdown" << dendl;
  
  elector.shutdown();
  
  // clean up
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->shutdown();

  // cancel all events
  cancel_tick();
  timer.cancel_all();
  timer.join();  

  // die.
  messenger->shutdown();
}


void Monitor::call_election(bool is_new)
{
  if (monmap->size() == 1) return;
  
  if (is_new) {
    stringstream ss;
    ss << "mon" << whoami << " calling new monitor election";
    logclient.log(LOG_INFO, ss);
  }

  dout(10) << "call_election" << dendl;
  state = STATE_STARTING;
  
  // tell paxos
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->election_starting();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_starting();

  // call a new election
  elector.call_election();
}

void Monitor::win_election(epoch_t epoch, set<int>& active) 
{
  state = STATE_LEADER;
  leader = whoami;
  mon_epoch = epoch;
  quorum = active;
  dout(10) << "win_election, epoch " << mon_epoch << " quorum is " << quorum << dendl;

  stringstream ss;
  ss << "mon" << whoami << " won leader election with quorum " << quorum;
  logclient.log(LOG_INFO, ss);
  
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->leader_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_finished();

  resend_routed_requests();
} 

void Monitor::lose_election(epoch_t epoch, set<int> &q, int l) 
{
  state = STATE_PEON;
  mon_epoch = epoch;
  leader = l;
  quorum = q;
  dout(10) << "lose_election, epoch " << mon_epoch << " leader is mon" << leader
	   << " quorum is " << quorum << dendl;
  
  for (vector<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->peon_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_finished();

  resend_routed_requests();
}

void Monitor::handle_command(MMonCommand *m)
{
  if (ceph_fsid_compare(&m->fsid, &monmap->fsid)) {
    dout(0) << "handle_command on fsid " << m->fsid << " != " << monmap->fsid << dendl;
    reply_command(m, -EPERM, "wrong fsid", 0);
    return;
  }

  dout(0) << "handle_command " << *m << dendl;
  bufferlist rdata;
  string rs;
  int r = -EINVAL;
  if (!m->cmd.empty()) {
      dout(0) << "m->cmd[0]=" << m->cmd[0] << dendl;
    if (m->cmd[0] == "mds") {
      mdsmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "osd") {
      osdmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "pg") {
      pgmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "client") {
      clientmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "mon") {
      monmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "stop") {
      shutdown();
      reply_command(m, 0, "stopping", 0);
      return;
    }
    if (m->cmd[0] == "stop_cluster") {
      stop_cluster();
      reply_command(m, 0, "initiating cluster shutdown", 0);
      return;
    }

    if (m->cmd[0] == "_injectargs") {
      parse_config_option_string(m->cmd[1]);
      return;
    } 
    if (m->cmd[0] == "class") {
      classmon()->dispatch(m);
      return;
    }
    if (m->cmd[0] == "auth") {
      authmon()->dispatch(m);
      return;
    }
    rs = "unrecognized subsystem";
  } else 
    rs = "no command";

  reply_command(m, r, rs, rdata, 0);
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs, version_t version)
{
  bufferlist rdata;
  reply_command(m, rc, rs, rdata, version);
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs, bufferlist& rdata, version_t version)
{
  MMonCommandAck *reply = new MMonCommandAck(m->cmd, rc, rs, version);
  reply->set_data(rdata);
  send_reply(m, reply);
  delete m;
}


// ------------------------
// request/reply routing
//
// a client/mds/osd will connect to a random monitor.  we need to forward any
// messages requiring state updates to the leader, and then route any replies
// back via the correct monitor and back to them.  (the monitor will not
// initiate any connections.)

void Monitor::forward_request_leader(PaxosServiceMessage *req)
{
  int mon = get_leader();
  Session *session = 0;
  if (req->get_connection())
    session = (Session *)req->get_connection()->get_priv();
  if (req->session_mon >= 0) {
    dout(10) << "forward_request won't double fwd request " << *req << dendl;
    delete req;
  } else if (session && !session->closed) {
    RoutedRequest *rr = new RoutedRequest;
    rr->tid = ++routed_request_tid;

    dout(10) << "forward_request " << rr->tid << " request " << *req << dendl;

    encode_message(req, rr->request_bl);
    rr->session = (Session *)session->get();
    routed_requests[rr->tid] = rr;

    session->routed_request_tids.insert(rr->tid);
    
    dout(10) << " noting that i mon" << whoami << " own this requests's session" << dendl;
    req->session_mon = whoami;
    req->session_mon_tid = rr->tid;
    req->clear_payload();
    
    messenger->forward_message(req, monmap->get_inst(mon));
  } else {
    dout(10) << "forward_request no session for request " << *req << dendl;
    delete req;
  }
}

void Monitor::try_send_message(Message *m, entity_inst_t to)
{
  dout(10) << "try_send_message " << *m << " to " << to << dendl;

  bufferlist bl;
  encode_message(m, bl);

  messenger->send_message(m, to);

  for (int i=0; i<(int)monmap->size(); i++) {
    if (i != whoami)
      messenger->send_message(new MRoute(0, bl, to),
			      monmap->get_inst(i));
  }
}

void Monitor::send_reply(PaxosServiceMessage *req, Message *reply, entity_inst_t to)
{
  if (req->session_mon >= 0) {
    if (req->session_mon < (int)monmap->size()) {
      dout(15) << "send_reply routing reply to " << to << " via mon" << req->session_mon
	       << " for request " << *req << dendl;
      messenger->send_message(new MRoute(req->session_mon_tid, reply, to),
			      monmap->get_inst(req->session_mon));
    } else
      dout(2) << "send_reply mon" << req->session_mon << " dne, dropping reply " << *reply
	      << " to " << *req << " for " << to << dendl;
  } else {
    messenger->send_message(reply, to);
  }
}

void Monitor::handle_route(MRoute *m)
{
  dout(10) << "handle_route " << *m->msg << " to " << m->dest << dendl;
  
  // look it up
  if (routed_requests.count(m->session_mon_tid)) {
    RoutedRequest *rr = routed_requests[m->session_mon_tid];
    messenger->send_message(m->msg, rr->session->inst);
    m->msg = NULL;
    routed_requests.erase(m->session_mon_tid);
    rr->session->routed_request_tids.insert(rr->tid);
    delete rr;
  } else {
    dout(10) << " don't have routed request tid " << m->session_mon_tid
	     << ", trying to send anyway" << dendl;
    messenger->send_message(m->msg, m->dest);
    m->msg = NULL;
  }
  delete m;
}

void Monitor::resend_routed_requests()
{
  dout(10) << "resend_routed_requests" << dendl;
  int mon = get_leader();
  for (map<__u64, RoutedRequest*>::iterator p = routed_requests.begin();
       p != routed_requests.end();
       p++) {
    RoutedRequest *rr = p->second;

    bufferlist::iterator q = rr->request_bl.begin();
    PaxosServiceMessage *req = (PaxosServiceMessage *)decode_message(q);

    dout(10) << " resend to mon" << mon << " tid " << rr->tid << " " << *req << dendl;
    req->session_mon = whoami;
    req->session_mon_tid = rr->tid;
    req->clear_payload();
    messenger->forward_message(req, monmap->get_inst(mon));
  }  
}

void Monitor::remove_session(Session *s)
{
  dout(10) << "remove_session " << s << " " << s->inst << dendl;
  for (set<__u64>::iterator p = s->routed_request_tids.begin();
       p != s->routed_request_tids.end();
       p++) {
    if (routed_requests.count(*p)) {
      RoutedRequest *rr = routed_requests[*p];
      dout(10) << " dropping routed request " << rr->tid << dendl;
      delete rr;
      routed_requests.erase(*p);
    }
  }
  session_map.remove_session(s);
}


void Monitor::handle_observe(MMonObserve *m)
{
  dout(10) << "handle_observe " << *m << " from " << m->get_source_inst() << dendl;
  if (m->machine_id >= PAXOS_NUM) {
    dout(0) << "register_observer: bad monitor id: " << m->machine_id << dendl;
  } else {
    paxos[m->machine_id]->register_observer(m->get_orig_source_inst(), m->ver);
  }
  messenger->send_message(m, m->get_orig_source_inst());
}


void Monitor::inject_args(const entity_inst_t& inst, vector<string>& args, version_t version)
{
  dout(10) << "inject_args " << inst << " " << args << dendl;
  MMonCommand *c = new MMonCommand(monmap->fsid, version);
  c->cmd = args;
  try_send_message(c, inst);
}




void Monitor::stop_cluster()
{
  dout(0) << "stop_cluster -- initiating shutdown" << dendl;
  stopping = true;
  mdsmon()->do_stop();
}


bool Monitor::ms_dispatch(Message *m)
{
  bool ret = true;
  lock.Lock();

  Connection *connection = m->get_connection();
  Session *s = NULL;
  bool reuse_caps = false;
  MonCaps caps;
  EntityName entity_name;
  bool src_is_mon;

  if (connection) {
    s = (Session *)connection->get_priv();
    if (s && s->closed) {
      caps = s->caps;
      reuse_caps = true;
      s->put();
      s = NULL;
    }
    if (!s) {
      s = session_map.new_session(m->get_source_inst());
      m->get_connection()->set_priv(s->get());
      dout(10) << "ms_dispatch new session " << s << " for " << s->inst << dendl;

      if (!s->inst.name.is_mon()) {
	// set an initial timeout here, so we will trim this session even if they don't
	// do anything.
	s->until = g_clock.now();
	s->until += g_conf.mon_subscribe_interval;
      }
      if (reuse_caps)
        s->caps = caps;
    } else {
      dout(20) << "ms_dispatch existing session " << s << " for " << s->inst << dendl;
    }
    if (s->auth_handler) {
      entity_name = s->auth_handler->get_entity_name();
    }
  }
  src_is_mon = (connection && connection->get_peer_type() & CEPH_ENTITY_TYPE_MON);
#define ALLOW_CAPS(service_id, allow_caps) \
do { \
  if (src_is_mon) \
    break; \
  if (s && ((int)s->caps.get_caps(service_id) & (allow_caps)) != (allow_caps)) { \
    dout(0) << "filtered out request due to caps " \
           << " allowing=" << #allow_caps << " message=" << *m << dendl; \
    delete m; \
    goto out; \
  } \
} while (0)

#define ALLOW_MESSAGES_FROM(peers) \
do { \
  if ((connection && connection->get_peer_type() & (peers | CEPH_ENTITY_TYPE_MON)) == 0) { \
    dout(0) << "filtered out request, peer=" << connection->get_peer_type() \
           << " allowing=" << #peers << " message=" << *m << dendl; \
    delete m; \
    goto out; \
  } \
} while (0)

#define EXIT_NOT_ADMIN \
do { \
  if (!entity_name.is_admin()) { \
    dout(0) << "filtered out request (not admin), peer=" << connection->get_peer_type() \
           << " entity_name=" << entity_name.to_str() << " message=" << *m << dendl; \
    delete m; \
    goto out; \
  } \
} while (0)

#define IS_NOT_ADMIN (!src_is_mon) && (!entity_name.is_admin())

  {
    switch (m->get_type()) {
      
    case MSG_ROUTE:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);
      handle_route((MRoute*)m);
      break;

      // misc
    case CEPH_MSG_MON_GET_MAP:
      /* public.. no need for checks */
      handle_mon_get_map((MMonGetMap*)m);
      break;

    case MSG_MON_COMMAND:
      if (IS_NOT_ADMIN) {
        string rs="Access denied";
        reply_command((MMonCommand *)m, -EACCES, rs, 0);
        EXIT_NOT_ADMIN;
      }
      handle_command((MMonCommand*)m);
      break;

    case CEPH_MSG_MON_SUBSCRIBE:
      /* FIXME: check what's being subscribed, filter accordingly */
      handle_subscribe((MMonSubscribe*)m);
      break;

      // OSDs
    case MSG_OSD_FAILURE:
    case MSG_OSD_BOOT:
    case MSG_OSD_IN:
    case MSG_OSD_OUT:
    case MSG_OSD_ALIVE:
    case MSG_OSD_PGTEMP:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_OSD);
      ALLOW_CAPS(PAXOS_OSDMAP, MON_CAP_R);
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

    case MSG_REMOVE_SNAPS:
      ALLOW_CAPS(PAXOS_OSDMAP, MON_CAP_RW);
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // MDSs
    case MSG_MDS_BEACON:
    case MSG_MDS_OFFLOAD_TARGETS:
      ALLOW_CAPS(PAXOS_MDSMAP, MON_CAP_RW);
      paxos_service[PAXOS_MDSMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // auth
    case CEPH_MSG_AUTH:
      /* no need to check caps here */
      paxos_service[PAXOS_AUTH]->dispatch((PaxosServiceMessage*)m);
      break;

      // clients
    case CEPH_MSG_CLIENT_MOUNT:
      ALLOW_CAPS(PAXOS_CLIENTMAP, MON_CAP_RW);
      paxos_service[PAXOS_CLIENTMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // pg
    case CEPH_MSG_STATFS:
    case MSG_PGSTATS:
    case MSG_GETPOOLSTATS:
      ALLOW_CAPS(PAXOS_CLIENTMAP, MON_CAP_R);
      paxos_service[PAXOS_PGMAP]->dispatch((PaxosServiceMessage*)m);
      break;

    case MSG_POOLOP:
      ALLOW_CAPS(PAXOS_OSDMAP, MON_CAP_RX);
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // log
    case MSG_LOG:
      ALLOW_CAPS(PAXOS_LOG, MON_CAP_RW);
      paxos_service[PAXOS_LOG]->dispatch((PaxosServiceMessage*)m);
      break;

      // paxos
    case MSG_MON_PAXOS:
      {
        ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);

	MMonPaxos *pm = (MMonPaxos*)m;

	// sanitize
	if (pm->epoch > mon_epoch) 
	  call_election();
	if (pm->epoch != mon_epoch) {
	  delete pm;
	  break;
	}

	// send it to the right paxos instance
	assert(pm->machine_id < PAXOS_NUM);
	Paxos *p = paxos[pm->machine_id];
	p->dispatch((PaxosServiceMessage*)m);

	// make sure service finds out about any state changes
	if (p->is_active())
	  paxos_service[p->machine_id]->update_from_paxos();
      }
      break;

    case MSG_MON_OBSERVE:
      if (IS_NOT_ADMIN) {
        EXIT_NOT_ADMIN;
      }
      handle_observe((MMonObserve *)m);
      break;

      // elector messages
    case MSG_MON_ELECTION:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);
      elector.dispatch(m);
      break;

    case MSG_CLASS:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_OSD);
      handle_class((MClass *)m);
      break;

    default:
      ret = false;
    }
  }
out:
  if (s) {
    s->put();
  }
  lock.Unlock();

  return ret;
}

void Monitor::handle_subscribe(MMonSubscribe *m)
{
  dout(10) << "handle_subscribe " << *m << dendl;
  
  bool reply = false;

  Session *s = (Session *)m->get_connection()->get_priv();

  s->until = g_clock.now();
  s->until += g_conf.mon_subscribe_interval;
  for (map<nstring,ceph_mon_subscribe_item>::iterator p = m->what.begin();
       p != m->what.end();
       p++) {
    if (!p->second.onetime)
      reply = true;
    session_map.add_update_sub(s, p->first, p->second.have, p->second.onetime);
    if (p->first == "mdsmap") {
      if ((int)s->caps.get_caps(PAXOS_MDSMAP) & (MON_CAP_R)) {
        mdsmon()->check_sub(s->sub_map["mdsmap"]);
      }
    } else if (p->first == "osdmap") {
      if ((int)s->caps.get_caps(PAXOS_OSDMAP) & (MON_CAP_R)) {
        osdmon()->check_sub(s->sub_map["osdmap"]);
      }
    } else if (p->first == "monmap") {
      check_sub(s->sub_map["monmap"]);
    }
  }

  // ???

  if (reply)
    messenger->send_message(new MMonSubscribeAck(monmap->get_fsid(), (int)g_conf.mon_subscribe_interval),
			    m->get_source_inst());

  s->put();
  delete m;
}

bool Monitor::ms_handle_reset(Connection *con)
{
  // ignore lossless monitor sessions
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
    return false;

  Session *s = (Session *)con->get_priv();
  if (!s)
    return false;

  Mutex::Locker l(lock);

  dout(10) << "reset/close on session " << s->inst << dendl;
  remove_session(s);
  s->put();
    
  // remove from connection, too.
  con->set_priv(NULL);
  return true;
}

void Monitor::check_subs()
{
  nstring type = "monmap";
  xlist<Subscription*>::iterator p = session_map.subs[type].begin();
  while (!p.end()) {
    Subscription *sub = *p;
    ++p;
    check_sub(sub);
  }
}

void Monitor::check_sub(Subscription *sub)
{
  if (sub->last < monmap->get_epoch()) {
    send_latest_monmap(sub->session->inst);
    if (sub->onetime)
      session_map.remove_sub(sub);
    else
      sub->last = monmap->get_epoch();
  }
}


// -----

void Monitor::send_latest_monmap(entity_inst_t i)
{
  bufferlist bl;
  monmap->encode(bl);
  messenger->send_message(new MMonMap(bl), i);
}

void Monitor::handle_mon_get_map(MMonGetMap *m)
{
  dout(10) << "handle_mon_get_map" << dendl;
  send_latest_monmap(m->get_orig_source_inst());
  delete m;
}







/************ TICK ***************/

class C_Mon_Tick : public Context {
  Monitor *mon;
public:
  C_Mon_Tick(Monitor *m) : mon(m) {}
  void finish(int r) {
    mon->tick();
  }
};

void Monitor::cancel_tick()
{
  if (tick_timer) timer.cancel_event(tick_timer);
}

void Monitor::reset_tick()
{
  cancel_tick();
  tick_timer = new C_Mon_Tick(this);
  timer.add_event_after(g_conf.mon_tick_interval, tick_timer);
}


void Monitor::tick()
{
  tick_timer = 0;

  _dout_check_log();

  // ok go.
  dout(11) << "tick" << dendl;
  
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->tick();
  
  // trim sessions
  utime_t now = g_clock.now();
  xlist<Session*>::iterator p = session_map.sessions.begin();
  while (!p.end()) {
    Session *s = *p;
    ++p;
    
    // don't trim monitors
    if (s->inst.name.is_mon())
      continue; 

    if (!s->until.is_zero() && s->until < now) {
      dout(10) << " trimming session " << s->inst
	       << " (until " << s->until << " < now " << now << ")" << dendl;
      messenger->mark_down(s->inst.addr);
      remove_session(s);
    }
  }

  // next tick!
  reset_tick();
}





/*
 * this is the closest thing to a traditional 'mkfs' for ceph.
 * initialize the monitor state machines to their initial values.
 */
int Monitor::mkfs(bufferlist& osdmapbl)
{
  // create it
  int err = store->mkfs();
  if (err < 0) {
    char buf[80];
    cerr << "error " << err << " " << strerror_r(err, buf, sizeof(buf)) << std::endl;
    exit(1);
  }
  
  store->put_int(whoami, "whoami", 0);

  bufferlist magicbl;
  magicbl.append(CEPH_MON_ONDISK_MAGIC);
  magicbl.append("\n");
  store->put_bl_ss(magicbl, "magic", 0);

  bufferlist monmapbl;
  monmap->encode(monmapbl);
  store->put_bl_sn(monmapbl, "monmap", monmap->epoch);  

  // latest, too.. but make this conform to paxos stash latest format
  bufferlist latest;
  version_t v = monmap->get_epoch();
  ::encode(v, latest);
  ::encode(monmapbl, latest);
  store->put_bl_ss(latest, "monmap", "latest");

  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++) {
    PaxosService *svc = *p;
    bufferlist bl;
    dout(10) << "initializing " << svc->get_machine_name() << dendl;
    svc->paxos->init();
    svc->create_pending();
    if (svc->paxos->machine_id == PAXOS_OSDMAP)
      svc->create_initial(osdmapbl);
    else
      svc->create_initial(bl);
    // commit to paxos
    svc->encode_pending(bl);
    store->put_bl_sn(bl, svc->get_machine_name(), 1);
    store->put_int(1, svc->get_machine_name(), "last_committed");
  }

  return 0;
}

void Monitor::handle_class(MClass *m)
{
  switch (m->action) {
    case CLASS_SET:
    case CLASS_GET:
      classmon()->handle_request(m);
      break;
    case CLASS_RESPONSE:
      dout(0) << "got a class response (" << *m << ") ???" << dendl;
      break;
    default:
      dout(0) << "got an unknown class message (" << *m << ") ???" << dendl;
      assert(0);
      break;
  }
}

bool Monitor::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new)
{
  CephXServiceTicketInfo auth_ticket_info;

  CephXSessionAuthInfo info;
  int ret;
  uint32_t service_id = dest_type;

  dout(0) << "ms_get_authorizer service_id=" << service_id << dendl;

  if (service_id != CEPH_ENTITY_TYPE_MON) {
    ret = key_server.build_session_auth_info(service_id, auth_ticket_info, info);
    if (ret < 0) {
      return false;
    }
  } else {
    EntityName name;
    name.entity_type = CEPH_ENTITY_TYPE_MON;

    CryptoKey secret;
    if (!key_server.get_secret(name, secret)) {
      dout(0) << "couldn't get secret for mon service!" << dendl;
      stringstream ss;
      key_server.list_secrets(ss);
      dout(0) << ss.str() << dendl;
      return false;
    }
    /* mon to mon authentication uses the private monitor shared key and not the
       rotating key */
    ret = key_server.build_session_auth_info(service_id, auth_ticket_info, info, secret, (uint64_t)-1);
    if (ret < 0) {
      return false;
    }
    dout(0) << "built session auth_info for use with mon" << dendl;

  }

  bufferlist ticket_data;
  ret = cephx_build_service_ticket(info, ticket_data);
  if (ret < 0)
    return false;

  dout(0) << "built service ticket" << dendl;
  bufferlist::iterator iter = ticket_data.begin();
  CephXTicketHandler handler;
  ::decode(handler.ticket, iter);

  handler.service_id = service_id;
  handler.session_key = info.session_key;

  *authorizer = handler.build_authorizer();
  
  return true;
}

bool Monitor::ms_verify_authorizer(Connection *con, int peer_type,
				   int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
				   bool& isvalid)
{
  dout(0) << "Monitor::verify_authorizer start" << dendl;

  if (protocol != CEPH_AUTH_CEPHX)
    return false;

  bufferlist::iterator iter = authorizer_data.begin();
  CephXServiceTicketInfo auth_ticket_info;


  isvalid = true;

  if (!authorizer_data.length())
    return true; /* we're not picky */

  int ret = cephx_verify_authorizer(key_server, iter, auth_ticket_info, authorizer_reply);
  dout(0) << "Monitor::verify_authorizer returns " << ret << dendl;

  isvalid = (ret >= 0);

  return true;
};

