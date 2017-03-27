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

#include "MDSRank.h"
#include "MDCache.h"
#include "Mutation.h"
#include "SessionMap.h"
#include "osdc/Filer.h"
#include "common/Finisher.h"

#include "common/config.h"
#include "common/errno.h"
#include "include/assert.h"
#include "include/stringify.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << rank << ".sessionmap "


class SessionMapIOContext : public MDSIOContextBase
{
  protected:
    SessionMap *sessionmap;
    MDSRank *get_mds() {return sessionmap->mds;}
  public:
    explicit SessionMapIOContext(SessionMap *sessionmap_) : sessionmap(sessionmap_) {
      assert(sessionmap != NULL);
    }
};



void SessionMap::dump()
{
  dout(10) << "dump" << dendl;
  for (ceph::unordered_map<entity_name_t,Session*>::iterator p = session_map.begin();
       p != session_map.end();
       ++p) 
    dout(10) << p->first << " " << p->second
	     << " state " << p->second->get_state_name()
	     << " completed " << p->second->info.completed_requests
	     << " prealloc_inos " << p->second->info.prealloc_inos
	     << " used_inos " << p->second->info.used_inos
	     << dendl;
}


// ----------------
// LOAD


object_t SessionMap::get_object_name()
{
  char s[30];
  snprintf(s, sizeof(s), "mds%d_sessionmap", int(mds->get_nodeid()));
  return object_t(s);
}

class C_IO_SM_Load : public SessionMapIOContext {
public:
  const bool first;  //< Am I the initial (header) load?
  int header_r;  //< Return value from OMAP header read
  int values_r;  //< Return value from OMAP value read
  bufferlist header_bl;
  std::map<std::string, bufferlist> session_vals;

  C_IO_SM_Load(SessionMap *cm, const bool f)
    : SessionMapIOContext(cm), first(f), header_r(0), values_r(0) {}

  void finish(int r) {
    sessionmap->_load_finish(r, header_r, values_r, first, header_bl, session_vals);
  }
};


/**
 * Decode OMAP header.  Call this once when loading.
 */
void SessionMapStore::decode_header(
      bufferlist &header_bl)
{
  bufferlist::iterator q = header_bl.begin();
  DECODE_START(1, q)
  ::decode(version, q);
  DECODE_FINISH(q);
}

void SessionMapStore::encode_header(
    bufferlist *header_bl)
{
  ENCODE_START(1, 1, *header_bl);
  ::encode(version, *header_bl);
  ENCODE_FINISH(*header_bl);
}

/**
 * Decode and insert some serialized OMAP values.  Call this
 * repeatedly to insert batched loads.
 */
void SessionMapStore::decode_values(std::map<std::string, bufferlist> &session_vals)
{
  for (std::map<std::string, bufferlist>::iterator i = session_vals.begin();
       i != session_vals.end(); ++i) {

    entity_inst_t inst;

    bool parsed = inst.name.parse(i->first);
    if (!parsed) {
      derr << "Corrupt entity name '" << i->first << "' in sessionmap" << dendl;
      throw buffer::malformed_input("Corrupt entity name in sessionmap");
    }

    Session *s = get_or_add_session(inst);
    if (s->is_closed())
      s->set_state(Session::STATE_OPEN);
    bufferlist::iterator q = i->second.begin();
    s->decode(q);
  }
}

/**
 * An OMAP read finished.
 */
void SessionMap::_load_finish(
    int operation_r,
    int header_r,
    int values_r,
    bool first,
    bufferlist &header_bl,
    std::map<std::string, bufferlist> &session_vals)
{
  if (operation_r < 0) {
    derr << "_load_finish got " << cpp_strerror(operation_r) << dendl;
    mds->clog->error() << "error reading sessionmap '" << get_object_name()
                       << "' " << operation_r << " ("
                       << cpp_strerror(operation_r) << ")";
    mds->damaged();
    assert(0);  // Should be unreachable because damaged() calls respawn()
  }

  // Decode header
  if (first) {
    if (header_r != 0) {
      derr << __func__ << ": header error: " << cpp_strerror(header_r) << dendl;
      mds->clog->error() << "error reading sessionmap header "
                         << header_r << " (" << cpp_strerror(header_r) << ")";
      mds->damaged();
      assert(0);  // Should be unreachable because damaged() calls respawn()
    }

    if(header_bl.length() == 0) {
      dout(4) << __func__ << ": header missing, loading legacy..." << dendl;
      load_legacy();
      return;
    }

    try {
      decode_header(header_bl);
    } catch (buffer::error &e) {
      mds->clog->error() << "corrupt sessionmap header: " << e.what();
      mds->damaged();
      assert(0);  // Should be unreachable because damaged() calls respawn()
    }
    dout(10) << __func__ << " loaded version " << version << dendl;
  }

  if (values_r != 0) {
    derr << __func__ << ": error reading values: "
      << cpp_strerror(values_r) << dendl;
    mds->clog->error() << "error reading sessionmap values: " 
                       << values_r << " (" << cpp_strerror(values_r) << ")";
    mds->damaged();
    assert(0);  // Should be unreachable because damaged() calls respawn()
  }

  // Decode session_vals
  try {
    decode_values(session_vals);
  } catch (buffer::error &e) {
    mds->clog->error() << "corrupt sessionmap values: " << e.what();
    mds->damaged();
    assert(0);  // Should be unreachable because damaged() calls respawn()
  }

  if (session_vals.size() == g_conf->mds_sessionmap_keys_per_op) {
    // Issue another read if we're not at the end of the omap
    const std::string last_key = session_vals.rbegin()->first;
    dout(10) << __func__ << ": continue omap load from '"
             << last_key << "'" << dendl;
    object_t oid = get_object_name();
    object_locator_t oloc(mds->mdsmap->get_metadata_pool());
    C_IO_SM_Load *c = new C_IO_SM_Load(this, false);
    ObjectOperation op;
    op.omap_get_vals(last_key, "", g_conf->mds_sessionmap_keys_per_op,
        &c->session_vals, &c->values_r);
    mds->objecter->read(oid, oloc, op, CEPH_NOSNAP, NULL, 0,
        new C_OnFinisher(c, mds->finisher));
  } else {
    // I/O is complete.  Update `by_state`
    dout(10) << __func__ << ": omap load complete" << dendl;
    for (ceph::unordered_map<entity_name_t, Session*>::iterator i = session_map.begin();
         i != session_map.end(); ++i) {
      Session *s = i->second;
      if (by_state.count(s->get_state()) == 0)
        by_state[s->get_state()] = new xlist<Session*>;
      by_state[s->get_state()]->push_back(&s->item_session_list);
    }

    // Population is complete.  Trigger load waiters.
    dout(10) << __func__ << ": v " << version 
	   << ", " << session_map.size() << " sessions" << dendl;
    projected = committing = committed = version;
    dump();
    finish_contexts(g_ceph_context, waiting_for_load);
  }
}

/**
 * Populate session state from OMAP records in this
 * rank's sessionmap object.
 */
void SessionMap::load(MDSInternalContextBase *onload)
{
  dout(10) << "load" << dendl;

  if (onload)
    waiting_for_load.push_back(onload);
  
  C_IO_SM_Load *c = new C_IO_SM_Load(this, true);
  object_t oid = get_object_name();
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());

  ObjectOperation op;
  op.omap_get_header(&c->header_bl, &c->header_r);
  op.omap_get_vals("", "", g_conf->mds_sessionmap_keys_per_op,
      &c->session_vals, &c->values_r);

  mds->objecter->read(oid, oloc, op, CEPH_NOSNAP, NULL, 0, new C_OnFinisher(c, mds->finisher));
}

class C_IO_SM_LoadLegacy : public SessionMapIOContext {
public:
  bufferlist bl;
  explicit C_IO_SM_LoadLegacy(SessionMap *cm) : SessionMapIOContext(cm) {}
  void finish(int r) {
    sessionmap->_load_legacy_finish(r, bl);
  }
};


/**
 * Load legacy (object data blob) SessionMap format, assuming
 * that waiting_for_load has already been populated with
 * the relevant completion.  This is the fallback if we do not
 * find an OMAP header when attempting to load normally.
 */
void SessionMap::load_legacy()
{
  dout(10) << __func__ << dendl;

  C_IO_SM_LoadLegacy *c = new C_IO_SM_LoadLegacy(this);
  object_t oid = get_object_name();
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());

  mds->objecter->read_full(oid, oloc, CEPH_NOSNAP, &c->bl, 0,
			   new C_OnFinisher(c, mds->finisher));
}

void SessionMap::_load_legacy_finish(int r, bufferlist &bl)
{ 
  bufferlist::iterator blp = bl.begin();
  if (r < 0) {
    derr << "_load_finish got " << cpp_strerror(r) << dendl;
    assert(0 == "failed to load sessionmap");
  }
  dump();
  decode_legacy(blp);  // note: this sets last_cap_renew = now()
  dout(10) << "_load_finish v " << version 
	   << ", " << session_map.size() << " sessions, "
	   << bl.length() << " bytes"
	   << dendl;
  projected = committing = committed = version;
  dump();

  // Mark all sessions dirty, so that on next save() we will write
  // a complete OMAP version of the data loaded from the legacy format
  for (ceph::unordered_map<entity_name_t, Session*>::iterator i = session_map.begin();
       i != session_map.end(); ++i) {
    // Don't use mark_dirty because on this occasion we want to ignore the
    // keys_per_op limit and do one big write (upgrade must be atomic)
    dirty_sessions.insert(i->first);
  }
  loaded_legacy = true;

  finish_contexts(g_ceph_context, waiting_for_load);
}


// ----------------
// SAVE

class C_IO_SM_Save : public SessionMapIOContext {
  version_t version;
public:
  C_IO_SM_Save(SessionMap *cm, version_t v) : SessionMapIOContext(cm), version(v) {}
  void finish(int r) {
    assert(r == 0);
    sessionmap->_save_finish(version);
  }
};

void SessionMap::save(MDSInternalContextBase *onsave, version_t needv)
{
  dout(10) << __func__ << ": needv " << needv << ", v " << version << dendl;
 
  if (needv && committing >= needv) {
    assert(committing > committed);
    commit_waiters[committing].push_back(onsave);
    return;
  }

  commit_waiters[version].push_back(onsave);

  committing = version;
  SnapContext snapc;
  object_t oid = get_object_name();
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());

  ObjectOperation op;

  /* Compose OSD OMAP transaction for full write */
  bufferlist header_bl;
  encode_header(&header_bl);
  op.omap_set_header(header_bl);

  /* If we loaded a legacy sessionmap, then erase the old data.  If
   * an old-versioned MDS tries to read it, it'll fail out safely
   * with an end_of_buffer exception */
  if (loaded_legacy) {
    dout(4) << __func__ << " erasing legacy sessionmap" << dendl;
    op.truncate(0);
    loaded_legacy = false;  // only need to truncate once.
  }

  dout(20) << " updating keys:" << dendl;
  map<string, bufferlist> to_set;
  for(std::set<entity_name_t>::iterator i = dirty_sessions.begin();
      i != dirty_sessions.end(); ++i) {
    const entity_name_t name = *i;
    Session *session = session_map[name];

    if (session->is_open() ||
	session->is_closing() ||
	session->is_stale() ||
	session->is_killing()) {
      dout(20) << "  " << name << dendl;
      // Serialize K
      std::ostringstream k;
      k << name;

      // Serialize V
      bufferlist bl;
      session->info.encode(bl);

      // Add to RADOS op
      to_set[k.str()] = bl;

      session->clear_dirty_completed_requests();
    } else {
      dout(20) << "  " << name << " (ignoring)" << dendl;
    }
  }
  if (!to_set.empty()) {
    op.omap_set(to_set);
  }

  dout(20) << " removing keys:" << dendl;
  set<string> to_remove;
  for(std::set<entity_name_t>::const_iterator i = null_sessions.begin();
      i != null_sessions.end(); ++i) {
    dout(20) << "  " << *i << dendl;
    std::ostringstream k;
    k << *i;
    to_remove.insert(k.str());
  }
  if (!to_remove.empty()) {
    op.omap_rm_keys(to_remove);
  }

  dirty_sessions.clear();
  null_sessions.clear();

  mds->objecter->mutate(oid, oloc, op, snapc,
			ceph::real_clock::now(g_ceph_context),
      0, NULL, new C_OnFinisher(new C_IO_SM_Save(this, version),
				mds->finisher));
}

void SessionMap::_save_finish(version_t v)
{
  dout(10) << "_save_finish v" << v << dendl;
  committed = v;

  finish_contexts(g_ceph_context, commit_waiters[v]);
  commit_waiters.erase(v);
}


/**
 * Deserialize sessions, and update by_state index
 */
void SessionMap::decode_legacy(bufferlist::iterator &p)
{
  // Populate `sessions`
  SessionMapStore::decode_legacy(p);

  // Update `by_state`
  for (ceph::unordered_map<entity_name_t, Session*>::iterator i = session_map.begin();
       i != session_map.end(); ++i) {
    Session *s = i->second;
    if (by_state.count(s->get_state()) == 0)
      by_state[s->get_state()] = new xlist<Session*>;
    by_state[s->get_state()]->push_back(&s->item_session_list);
  }
}

uint64_t SessionMap::set_state(Session *session, int s) {
  if (session->state != s) {
    session->set_state(s);
    if (by_state.count(s) == 0)
      by_state[s] = new xlist<Session*>;
    by_state[s]->push_back(&session->item_session_list);
  }
  return session->get_state_seq();
}

void SessionMapStore::decode_legacy(bufferlist::iterator& p)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  uint64_t pre;
  ::decode(pre, p);
  if (pre == (uint64_t)-1) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, p);
    assert(struct_v >= 2);
    
    ::decode(version, p);
    
    while (!p.end()) {
      entity_inst_t inst;
      ::decode(inst.name, p);
      Session *s = get_or_add_session(inst);
      if (s->is_closed())
        s->set_state(Session::STATE_OPEN);
      s->decode(p);
    }

    DECODE_FINISH(p);
  } else {
    // --- old format ----
    version = pre;

    // this is a meaningless upper bound.  can be ignored.
    __u32 n;
    ::decode(n, p);
    
    while (n-- && !p.end()) {
      bufferlist::iterator p2 = p;
      Session *s = new Session;
      s->info.decode(p);
      if (session_map.count(s->info.inst.name)) {
	// eager client connected too fast!  aie.
	dout(10) << " already had session for " << s->info.inst.name << ", recovering" << dendl;
	entity_name_t n = s->info.inst.name;
	delete s;
	s = session_map[n];
	p = p2;
	s->info.decode(p);
      } else {
	session_map[s->info.inst.name] = s;
      }
      s->set_state(Session::STATE_OPEN);
      s->last_cap_renew = now;
    }
  }
}

void SessionMapStore::dump(Formatter *f) const
{
  f->open_array_section("Sessions");
  for (ceph::unordered_map<entity_name_t,Session*>::const_iterator p = session_map.begin();
       p != session_map.end();
       ++p)  {
    f->open_object_section("Session");
    f->open_object_section("entity name");
    p->first.dump(f);
    f->close_section(); // entity name
    f->dump_string("state", p->second->get_state_name());
    f->open_object_section("Session info");
    p->second->info.dump(f);
    f->close_section(); // Session info
    f->close_section(); // Session
  }
  f->close_section(); // Sessions
}

void SessionMapStore::generate_test_instances(list<SessionMapStore*>& ls)
{
  // pretty boring for now
  ls.push_back(new SessionMapStore());
}

void SessionMap::wipe()
{
  dout(1) << "wipe start" << dendl;
  dump();
  while (!session_map.empty()) {
    Session *s = session_map.begin()->second;
    remove_session(s);
  }
  version = ++projected;
  dout(1) << "wipe result" << dendl;
  dump();
  dout(1) << "wipe done" << dendl;
}

void SessionMap::wipe_ino_prealloc()
{
  for (ceph::unordered_map<entity_name_t,Session*>::iterator p = session_map.begin(); 
       p != session_map.end(); 
       ++p) {
    p->second->pending_prealloc_inos.clear();
    p->second->info.prealloc_inos.clear();
    p->second->info.used_inos.clear();
  }
  projected = ++version;
}

void SessionMap::add_session(Session *s)
{
  dout(10) << __func__ << " s=" << s << " name=" << s->info.inst.name << dendl;

  assert(session_map.count(s->info.inst.name) == 0);
  session_map[s->info.inst.name] = s;
  if (by_state.count(s->state) == 0)
    by_state[s->state] = new xlist<Session*>;
  by_state[s->state]->push_back(&s->item_session_list);
  s->get();
}

void SessionMap::remove_session(Session *s)
{
  dout(10) << __func__ << " s=" << s << " name=" << s->info.inst.name << dendl;

  s->trim_completed_requests(0);
  s->item_session_list.remove_myself();
  session_map.erase(s->info.inst.name);
  if (dirty_sessions.count(s->info.inst.name)) {
    dirty_sessions.erase(s->info.inst.name);
  }
  null_sessions.insert(s->info.inst.name);
  s->put();
}

void SessionMap::touch_session(Session *session)
{
  dout(10) << __func__ << " s=" << session << " name=" << session->info.inst.name << dendl;

  // Move to the back of the session list for this state (should
  // already be on a list courtesy of add_session and set_state)
  assert(session->item_session_list.is_on_list());
  if (by_state.count(session->state) == 0)
    by_state[session->state] = new xlist<Session*>;
  by_state[session->state]->push_back(&session->item_session_list);

  session->last_cap_renew = ceph_clock_now(g_ceph_context);
}

void SessionMap::_mark_dirty(Session *s)
{
  if (dirty_sessions.size() >= g_conf->mds_sessionmap_keys_per_op) {
    // Pre-empt the usual save() call from journal segment trim, in
    // order to avoid building up an oversized OMAP update operation
    // from too many sessions modified at once
    save(new C_MDSInternalNoop, version);
  }

  dirty_sessions.insert(s->info.inst.name);
}

void SessionMap::mark_dirty(Session *s)
{
  dout(20) << __func__ << " s=" << s << " name=" << s->info.inst.name
    << " v=" << version << dendl;

  _mark_dirty(s);
  version++;
  s->pop_pv(version);
}

void SessionMap::replay_dirty_session(Session *s)
{
  dout(20) << __func__ << " s=" << s << " name=" << s->info.inst.name
    << " v=" << version << dendl;

  _mark_dirty(s);

  replay_advance_version();
}

void SessionMap::replay_advance_version()
{
  version++;
  projected = version;
}

version_t SessionMap::mark_projected(Session *s)
{
  dout(20) << __func__ << " s=" << s << " name=" << s->info.inst.name
    << " pv=" << projected << " -> " << projected + 1 << dendl;
  ++projected;
  s->push_pv(projected);
  return projected;
}


class C_IO_SM_Save_One : public SessionMapIOContext {
  MDSInternalContextBase *on_safe;
public:
  C_IO_SM_Save_One(SessionMap *cm, MDSInternalContextBase *on_safe_)
    : SessionMapIOContext(cm), on_safe(on_safe_) {}
  void finish(int r) {
    if (r != 0) {
      get_mds()->handle_write_error(r);
    } else {
      on_safe->complete(r);
    }
  }
};


void SessionMap::save_if_dirty(const std::set<entity_name_t> &tgt_sessions,
                               MDSGatherBuilder *gather_bld)
{
  assert(gather_bld != NULL);

  std::vector<entity_name_t> write_sessions;

  // Decide which sessions require a write
  for (std::set<entity_name_t>::iterator i = tgt_sessions.begin();
       i != tgt_sessions.end(); ++i) {
    const entity_name_t &session_id = *i;

    if (session_map.count(session_id) == 0) {
      // Session isn't around any more, never mind.
      continue;
    }

    Session *session = session_map[session_id];
    if (!session->has_dirty_completed_requests()) {
      // Session hasn't had completed_requests
      // modified since last write, no need to
      // write it now.
      continue;
    }

    if (dirty_sessions.count(session_id) > 0) {
      // Session is already dirtied, will be written, no
      // need to pre-empt that.
      continue;
    }
    // Okay, passed all our checks, now we write
    // this session out.  The version we write
    // into the OMAP may now be higher-versioned
    // than the version in the header, but that's
    // okay because it's never a problem to have
    // an overly-fresh copy of a session.
    write_sessions.push_back(*i);
  }

  dout(4) << __func__ << ": writing " << write_sessions.size() << dendl;

  // Batch writes into mds_sessionmap_keys_per_op
  const uint32_t kpo = g_conf->mds_sessionmap_keys_per_op;
  map<string, bufferlist> to_set;
  for (uint32_t i = 0; i < write_sessions.size(); ++i) {
    // Start a new write transaction?
    if (i % g_conf->mds_sessionmap_keys_per_op == 0) {
      to_set.clear();
    }

    const entity_name_t &session_id = write_sessions[i];
    Session *session = session_map[session_id];
    session->clear_dirty_completed_requests();

    // Serialize K
    std::ostringstream k;
    k << session_id;

    // Serialize V
    bufferlist bl;
    session->info.encode(bl);

    // Add to RADOS op
    to_set[k.str()] = bl;

    // Complete this write transaction?
    if (i == write_sessions.size() - 1
        || i % kpo == kpo - 1) {
      ObjectOperation op;
      op.omap_set(to_set);

      SnapContext snapc;
      object_t oid = get_object_name();
      object_locator_t oloc(mds->mdsmap->get_metadata_pool());
      MDSInternalContextBase *on_safe = gather_bld->new_sub();
      mds->objecter->mutate(oid, oloc, op, snapc,
			    ceph::real_clock::now(g_ceph_context),
			    0, NULL, new C_OnFinisher(
			      new C_IO_SM_Save_One(this, on_safe),
			      mds->finisher));
    }
  }
}

// =================
// Session

#undef dout_prefix
#define dout_prefix *_dout << "Session "

/**
 * Calculate the length of the `requests` member list,
 * because elist does not have a size() method.
 *
 * O(N) runtime.  This would be const, but elist doesn't
 * have const iterators.
 */
size_t Session::get_request_count()
{
  size_t result = 0;

  elist<MDRequestImpl*>::iterator p = requests.begin(
      member_offset(MDRequestImpl, item_session_request));
  while (!p.end()) {
    ++result;
    ++p;
  }

  return result;
}

/**
 * Capped in response to a CEPH_MSG_CLIENT_CAPRELEASE message,
 * with n_caps equal to the number of caps that were released
 * in the message.  Used to update state about how many caps a
 * client has released since it was last instructed to RECALL_STATE.
 */
void Session::notify_cap_release(size_t n_caps)
{
  if (!recalled_at.is_zero()) {
    recall_release_count += n_caps;
    if (recall_release_count >= recall_count)
      clear_recalled_at();
  }
}

/**
 * Called when a CEPH_MSG_CLIENT_SESSION->CEPH_SESSION_RECALL_STATE
 * message is sent to the client.  Update our recall-related state
 * in order to generate health metrics if the session doesn't see
 * a commensurate number of calls to ::notify_cap_release
 */
void Session::notify_recall_sent(int const new_limit)
{
  if (recalled_at.is_zero()) {
    // Entering recall phase, set up counters so we can later
    // judge whether the client has respected the recall request
    recalled_at = last_recall_sent = ceph_clock_now(g_ceph_context);
    assert (new_limit < caps.size());  // Behaviour of Server::recall_client_state
    recall_count = caps.size() - new_limit;
    recall_release_count = 0;
  } else {
    last_recall_sent = ceph_clock_now(g_ceph_context);
  }
}

void Session::clear_recalled_at()
{
  recalled_at = last_recall_sent = utime_t();
  recall_count = 0;
  recall_release_count = 0;
}

void Session::set_client_metadata(map<string, string> const &meta)
{
  info.client_metadata = meta;

  _update_human_name();
}

/**
 * Use client metadata to generate a somewhat-friendlier
 * name for the client than its session ID.
 *
 * This is *not* guaranteed to be unique, and any machine
 * consumers of session-related output should always use
 * the session ID as a primary capacity and use this only
 * as a presentation hint.
 */
void Session::_update_human_name()
{
  if (info.client_metadata.count("hostname")) {
    // Happy path, refer to clients by hostname
    human_name = info.client_metadata["hostname"];
    if (!info.auth_name.has_default_id()) {
      // When a non-default entity ID is set by the user, assume they
      // would like to see it in references to the client, if it's
      // reasonable short.  Limit the length because we don't want
      // to put e.g. uuid-generated names into a "human readable"
      // rendering.
      const int arbitrarily_short = 16;
      if (info.auth_name.get_id().size() < arbitrarily_short) {
        human_name += std::string(":") + info.auth_name.get_id();
      }
    }
  } else {
    // Fallback, refer to clients by ID e.g. client.4567
    human_name = stringify(info.inst.name.num());
  }
}

void Session::decode(bufferlist::iterator &p)
{
  info.decode(p);

  _update_human_name();
}

int Session::check_access(CInode *in, unsigned mask,
			  int caller_uid, int caller_gid,
			  int new_uid, int new_gid)
{
  string path;
  CInode *diri = NULL;
  if (!in->is_base())
    diri = in->get_projected_parent_dn()->get_dir()->get_inode();
  if (diri && diri->is_stray()){
    path = in->get_projected_inode()->stray_prior_path;
    dout(20) << __func__ << " stray_prior_path " << path << dendl;
  } else {
    in->make_path_string(path, true);
    dout(20) << __func__ << " path " << path << dendl;
  }
  if (path.length())
    path = path.substr(1);    // drop leading /

  if (in->inode.is_dir() &&
      in->inode.has_layout() &&
      in->inode.layout.pool_ns.length() &&
      !connection->has_feature(CEPH_FEATURE_FS_FILE_LAYOUT_V2)) {
    dout(10) << __func__ << " client doesn't support FS_FILE_LAYOUT_V2" << dendl;
    return -EIO;
  }

  if (!auth_caps.is_capable(path, in->inode.uid, in->inode.gid, in->inode.mode,
			    caller_uid, caller_gid, mask,
			    new_uid, new_gid)) {
    return -EACCES;
  }
  return 0;
}

int SessionFilter::parse(
    const std::vector<std::string> &args,
    std::stringstream *ss)
{
  assert(ss != NULL);

  for (const auto &s : args) {
    dout(20) << __func__ << " parsing filter '" << s << "'" << dendl;

    auto eq = s.find("=");
    if (eq == std::string::npos || eq == s.size()) {
      *ss << "Invalid filter '" << s << "'";
      return -EINVAL;
    }

    // Keys that start with this are to be taken as referring
    // to freeform client metadata fields.
    const std::string metadata_prefix("client_metadata.");

    auto k = s.substr(0, eq);
    auto v = s.substr(eq + 1);

    dout(20) << __func__ << " parsed k='" << k << "', v='" << v << "'" << dendl;

    if (k.compare(0, metadata_prefix.size(), metadata_prefix) == 0
        && k.size() > metadata_prefix.size()) {
      // Filter on arbitrary metadata key (no fixed schema for this,
      // so anything after the dot is a valid field to filter on)
      auto metadata_key = k.substr(metadata_prefix.size());
      metadata.insert(std::make_pair(metadata_key, v));
    } else if (k == "auth_name") {
      // Filter on client entity name
      auth_name = v;
    } else if (k == "state") {
      state = v;
    } else if (k == "id") {
      std::string err;
      id = strict_strtoll(v.c_str(), 10, &err);
      if (!err.empty()) {
        *ss << err;
        return -EINVAL;
      }
    } else if (k == "reconnecting") {

      /**
       * Strict boolean parser.  Allow true/false/0/1.
       * Anything else is -EINVAL.
       */
      auto is_true = [](const std::string &bstr, bool *out) -> bool
      {
        assert(out != nullptr);

        if (bstr == "true" || bstr == "1") {
          *out = true;
          return 0;
        } else if (bstr == "false" || bstr == "0") {
          *out = false;
          return 0;
        } else {
          return -EINVAL;
        }
      };

      bool bval;
      int r = is_true(v, &bval);
      if (r == 0) {
        set_reconnecting(bval);
      } else {
        *ss << "Invalid boolean value '" << v << "'";
        return -EINVAL;
      }
    } else {
      *ss << "Invalid filter key '" << k << "'";
      return -EINVAL;
    }
  }

  return 0;
}

bool SessionFilter::match(
    const Session &session,
    std::function<bool(client_t)> is_reconnecting) const
{
  for (auto m : metadata) {
    auto k = m.first;
    auto v = m.second;
    if (session.info.client_metadata.count(k) == 0) {
      return false;
    }
    if (session.info.client_metadata.at(k) != v) {
      return false;
    }
  }

  if (!auth_name.empty() && auth_name != session.info.auth_name.get_id()) {
    return false;
  }

  if (!state.empty() && state != session.get_state_name()) {
    return false;
  }

  if (id != 0 && id != session.info.inst.name.num()) {
    return false;
  }

  if (reconnecting.first) {
    const bool am_reconnecting = is_reconnecting(session.info.inst.name.num());
    if (reconnecting.second != am_reconnecting) {
      return false;
    }
  }

  return true;
}

std::ostream& operator<<(std::ostream &out, const Session &s)
{
 if (s.get_human_name() == stringify(s.info.inst.name.num())) {
   out << s.get_human_name();
 } else {
   out << s.get_human_name() << " (" << std::dec << s.info.inst.name.num() << ")";
 }
 return out;
}

