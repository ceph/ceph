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

#include "MDSTable.h"

#include "MDS.h"
#include "MDLog.h"

#include "osdc/Filer.h"

#include "include/types.h"

#include "common/config.h"
#include "include/assert.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << (mds ? mds->get_nodeid() : -1) << "." << table_name << ": "


class C_MT_Save : public Context {
  MDSTable *ida;
  version_t version;
public:
  C_MT_Save(MDSTable *i, version_t v) : ida(i), version(v) {}
  void finish(int r) {
    ida->save_2(r, version);
  }
};

void MDSTable::save(Context *onfinish, version_t v)
{
  if (v > 0 && v <= committing_version) {
    dout(10) << "save v " << version << " - already saving "
	     << committing_version << " >= needed " << v << dendl;
    waitfor_save[v].push_back(onfinish);
    return;
  }
  
  dout(10) << "save v " << version << dendl;
  assert(is_active());
  
  bufferlist bl;
  ::encode(version, bl);
  encode_state(bl);

  committing_version = version;

  if (onfinish)
    waitfor_save[version].push_back(onfinish);

  // write (async)
  SnapContext snapc;
  object_t oid = get_object_name();
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());
  mds->objecter->write_full(oid, oloc,
			    snapc,
			    bl, ceph_clock_now(g_ceph_context), 0,
			    NULL, new C_MT_Save(this, version));
}

void MDSTable::save_2(int r, version_t v)
{
  dout(10) << "save_2 v " << v << dendl;
  if (r == -EBLACKLISTED) {
    mds->suicide();
    return;
  }
  if (r < 0) {
    dout(10) << "save_2 could not write table: " << r << dendl;
    assert(r >= 0);
  }
  assert(r >= 0);
  committed_version = v;
  
  list<Context*> ls;
  while (!waitfor_save.empty()) {
    if (waitfor_save.begin()->first > v) break;
    ls.splice(ls.end(), waitfor_save.begin()->second);
    waitfor_save.erase(waitfor_save.begin());
  }
  finish_contexts(g_ceph_context, ls,0);
}


void MDSTable::reset()
{
  reset_state();
  state = STATE_ACTIVE;
}



// -----------------------

class C_MT_Load : public Context {
public:
  MDSTable *ida;
  Context *onfinish;
  bufferlist bl;
  C_MT_Load(MDSTable *i, Context *o) : ida(i), onfinish(o) {}
  void finish(int r) {
    ida->load_2(r, bl, onfinish);
  }
};

object_t MDSTable::get_object_name()
{
  char n[50];
  if (per_mds)
    snprintf(n, sizeof(n), "mds%d_%s", mds->whoami, table_name);
  else
    snprintf(n, sizeof(n), "mds_%s", table_name);
  return object_t(n);
}

void MDSTable::load(Context *onfinish)
{ 
  dout(10) << "load" << dendl;

  assert(is_undef());
  state = STATE_OPENING;

  C_MT_Load *c = new C_MT_Load(this, onfinish);
  object_t oid = get_object_name();
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());
  mds->objecter->read_full(oid, oloc, CEPH_NOSNAP, &c->bl, 0, c);
}

void MDSTable::load_2(int r, bufferlist& bl, Context *onfinish)
{
  assert(is_opening());
  state = STATE_ACTIVE;
  if (r == -EBLACKLISTED) {
    mds->suicide();
    return;
  }
  if (r < 0) {
    derr << "load_2 could not read table: " << r << dendl;
    assert(r >= 0);
  }

  dout(10) << "load_2 got " << bl.length() << " bytes" << dendl;
  bufferlist::iterator p = bl.begin();
  ::decode(version, p);
  projected_version = committed_version = version;
  dout(10) << "load_2 loaded v" << version << dendl;
  decode_state(p);

  if (onfinish) {
    onfinish->complete(0);
  }
}
