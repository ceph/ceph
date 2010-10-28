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

#include "MDS.h"
#include "MDCache.h"
#include "SessionMap.h"
#include "osdc/Filer.h"

#include "config.h"

#define DOUT_SUBSYS mds
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "mds" << mds->get_nodeid() << ".sessionmap "


void SessionMap::dump()
{
  dout(10) << "dump" << dendl;
  for (hash_map<entity_name_t,Session*>::iterator p = session_map.begin();
       p != session_map.end();
       ++p) 
    dout(10) << p->first << " " << p->second
	     << " state " << p->second->get_state_name()
	     << " completed " << p->second->completed_requests
	     << " prealloc_inos " << p->second->prealloc_inos
	     << " used_ions " << p->second->used_inos
	     << dendl;
}


// ----------------
// LOAD


object_t SessionMap::get_object_name()
{
  char s[30];
  snprintf(s, sizeof(s), "mds%d_sessionmap", mds->whoami);
  return object_t(s);
}

class C_SM_Load : public Context {
  SessionMap *sessionmap;
public:
  bufferlist bl;
  C_SM_Load(SessionMap *cm) : sessionmap(cm) {}
  void finish(int r) {
    sessionmap->_load_finish(r, bl);
  }
};

void SessionMap::load(Context *onload)
{
  dout(10) << "load" << dendl;

  if (onload)
    waiting_for_load.push_back(onload);
  
  C_SM_Load *c = new C_SM_Load(this);
  object_t oid = get_object_name();
  object_locator_t oloc(mds->mdsmap->get_metadata_pg_pool());
  mds->objecter->read_full(oid, oloc, CEPH_NOSNAP, &c->bl, 0, c);
}

void SessionMap::_load_finish(int r, bufferlist &bl)
{ 
  bufferlist::iterator blp = bl.begin();
  dump();
  decode(blp);  // note: this sets last_cap_renew = now()
  dout(10) << "_load_finish v " << version 
	   << ", " << session_map.size() << " sessions, "
	   << bl.length() << " bytes"
	   << dendl;
  projected = committing = committed = version;
  dump();
  finish_contexts(waiting_for_load);
}


// ----------------
// SAVE

class C_SM_Save : public Context {
  SessionMap *sessionmap;
  version_t version;
public:
  C_SM_Save(SessionMap *cm, version_t v) : sessionmap(cm), version(v) {}
  void finish(int r) {
	sessionmap->_save_finish(version);
  }
};

void SessionMap::save(Context *onsave, version_t needv)
{
  dout(10) << "save needv " << needv << ", v " << version << dendl;
 
  if (needv && committing >= needv) {
    assert(committing > committed);
    commit_waiters[committing].push_back(onsave);
    return;
  }

  commit_waiters[version].push_back(onsave);
  
  bufferlist bl;
  
  encode(bl);
  committing = version;
  SnapContext snapc;
  object_t oid = get_object_name();
  object_locator_t oloc(mds->mdsmap->get_metadata_pg_pool());

  mds->objecter->write_full(oid, oloc,
			    snapc,
			    bl, g_clock.now(), 0,
			    NULL, new C_SM_Save(this, version));
}

void SessionMap::_save_finish(version_t v)
{
  dout(10) << "_save_finish v" << v << dendl;
  committed = v;

  finish_contexts(commit_waiters[v]);
  commit_waiters.erase(v);
}


// -------------------

void SessionMap::encode(bufferlist& bl)
{
  uint64_t pre = -1;     // for 0.19 compatibility; we forgot an encoding prefix.
  ::encode(pre, bl);

  __u8 struct_v = 2;
  ::encode(struct_v, bl);

  ::encode(version, bl);

  for (hash_map<entity_name_t,Session*>::iterator p = session_map.begin(); 
       p != session_map.end(); 
       ++p) 
    if (p->second->is_open() ||
	p->second->is_closing() ||
	p->second->is_stale() ||
	p->second->is_killing()) {
      ::encode(p->first, bl);
      p->second->encode(bl);
    }
}

void SessionMap::decode(bufferlist::iterator& p)
{
  utime_t now = g_clock.now();
  uint64_t pre;
  ::decode(pre, p);
  if (pre == (uint64_t)-1) {
    __u8 struct_v;
    ::decode(struct_v, p);
    assert(struct_v == 2);

    ::decode(version, p);

    while (!p.end()) {
      entity_inst_t inst;
      ::decode(inst.name, p);
      Session *s = get_or_add_session(inst);
      if (s->is_closed())
	set_state(s, Session::STATE_OPEN);
      s->decode(p);
    }

  } else {
    // --- old format ----
    version = pre;

    // this is a meaningless upper bound.  can be ignored.
    __u32 n;
    ::decode(n, p);
    
    while (n-- && !p.end()) {
      bufferlist::iterator p2 = p;
      Session *s = new Session;
      s->decode(p);
      if (session_map.count(s->inst.name)) {
	// eager client connected too fast!  aie.
	dout(10) << " already had session for " << s->inst.name << ", recovering" << dendl;
	entity_name_t n = s->inst.name;
	delete s;
	s = session_map[n];
	p = p2;
	s->decode(p);
      } else {
	session_map[s->inst.name] = s;
      }
      set_state(s, Session::STATE_OPEN);
      s->last_cap_renew = now;
    }
  }
}
