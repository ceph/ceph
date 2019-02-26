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

#include "MDSRank.h"
#include "MDLog.h"

#include "osdc/Filer.h"

#include "include/types.h"

#include "common/config.h"
#include "common/errno.h"
#include "common/Finisher.h"

#include "include/ceph_assert.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << rank << "." << table_name << ": "


class MDSTableIOContext : public MDSIOContextBase
{
  protected:
    MDSTable *ida;
    MDSRank *get_mds() override {return ida->mds;}
  public:
    explicit MDSTableIOContext(MDSTable *ida_) : ida(ida_) {
      ceph_assert(ida != NULL);
    }
};


class C_IO_MT_Save : public MDSTableIOContext {
  version_t version;
public:
  C_IO_MT_Save(MDSTable *i, version_t v) : MDSTableIOContext(i), version(v) {}
  void finish(int r) override {
    ida->save_2(r, version);
  }
  void print(ostream& out) const override {
    out << "table_save(" << ida->table_name << ")";
  }
};

void MDSTable::save(MDSContext *onfinish, version_t v)
{
  if (v > 0 && v <= committing_version) {
    dout(10) << "save v " << version << " - already saving "
	     << committing_version << " >= needed " << v << dendl;
    if (onfinish)
      waitfor_save[v].push_back(onfinish);
    return;
  }
  
  dout(10) << "save v " << version << dendl;
  ceph_assert(is_active());
  
  bufferlist bl;
  encode(version, bl);
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
			    bl, ceph::real_clock::now(), 0,
			    new C_OnFinisher(new C_IO_MT_Save(this, version),
					     mds->finisher));
}

void MDSTable::save_2(int r, version_t v)
{
  if (r < 0) {
    dout(1) << "save error " << r << " v " << v << dendl;
    mds->clog->error() << "failed to store table " << table_name << " object,"
		       << " errno " << r;
    mds->handle_write_error(r);
    return;
  }

  dout(10) << "save_2 v " << v << dendl;
  committed_version = v;
  
  MDSContext::vec ls;
  while (!waitfor_save.empty()) {
    auto it = waitfor_save.begin();
    if (it->first > v) break;
    auto& v = it->second;
    ls.insert(ls.end(), v.begin(), v.end());
    waitfor_save.erase(it);
  }
  finish_contexts(g_ceph_context, ls, 0);
}


void MDSTable::reset()
{
  reset_state();
  projected_version = version;
  state = STATE_ACTIVE;
}



// -----------------------

class C_IO_MT_Load : public MDSTableIOContext {
public:
  Context *onfinish;
  bufferlist bl;
  C_IO_MT_Load(MDSTable *i, Context *o) : MDSTableIOContext(i), onfinish(o) {}
  void finish(int r) override {
    ida->load_2(r, bl, onfinish);
  }
  void print(ostream& out) const override {
    out << "table_load(" << ida->table_name << ")";
  }
};

object_t MDSTable::get_object_name() const
{
  char n[50];
  if (per_mds)
    snprintf(n, sizeof(n), "mds%d_%s", int(rank), table_name.c_str());
  else
    snprintf(n, sizeof(n), "mds_%s", table_name.c_str());
  return object_t(n);
}

void MDSTable::load(MDSContext *onfinish)
{ 
  dout(10) << "load" << dendl;

  ceph_assert(is_undef());
  state = STATE_OPENING;

  C_IO_MT_Load *c = new C_IO_MT_Load(this, onfinish);
  object_t oid = get_object_name();
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());
  mds->objecter->read_full(oid, oloc, CEPH_NOSNAP, &c->bl, 0,
			   new C_OnFinisher(c, mds->finisher));
}

void MDSTable::load_2(int r, bufferlist& bl, Context *onfinish)
{
  ceph_assert(is_opening());
  state = STATE_ACTIVE;
  if (r == -EBLACKLISTED) {
    mds->respawn();
    return;
  }
  if (r < 0) {
    derr << "load_2 could not read table: " << r << dendl;
    mds->clog->error() << "error reading table object '" << get_object_name()
                       << "' " << r << " (" << cpp_strerror(r) << ")";
    mds->damaged();
    ceph_assert(r >= 0);  // Should be unreachable because damaged() calls respawn()
  }

  dout(10) << "load_2 got " << bl.length() << " bytes" << dendl;
  auto p = bl.cbegin();

  try {
    decode(version, p);
    projected_version = committed_version = version;
    dout(10) << "load_2 loaded v" << version << dendl;
    decode_state(p);
  } catch (buffer::error &e) {
    mds->clog->error() << "error decoding table object '" << get_object_name()
                       << "': " << e.what();
    mds->damaged();
    ceph_assert(r >= 0);  // Should be unreachable because damaged() calls respawn()
  }

  if (onfinish) {
    onfinish->complete(0);
  }
}
