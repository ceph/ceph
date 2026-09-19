// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <functional>

#include "common/debug.h"

#include "common/Finisher.h"
#include "common/errno.h" // for cpp_strerror()
#include "include/ceph_assert.h"
#include "osdc/Objecter.h"

#include "MDSContext.h"
#include "MDSRank.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << rank << "." << table_name << ": "

using namespace std;

namespace {
std::function<void(Context*)> g_mds_table_write_hook;
MDSTableTestAccess::TestIO* g_mds_table_test_io = nullptr;

ceph::fair_mutex&
mds_table_lock(MDSRank* mds)
{
  if (g_mds_table_test_io) {
    ceph_assert(g_mds_table_test_io->lock);
    return *g_mds_table_test_io->lock;
  }
  ceph_assert(mds);
  return mds->mds_lock;
}

Finisher*
mds_table_finisher(MDSRank* mds)
{
  if (g_mds_table_test_io) {
    ceph_assert(g_mds_table_test_io->finisher);
    return g_mds_table_test_io->finisher;
  }
  ceph_assert(mds);
  return mds->finisher;
}

int64_t
mds_table_pool(MDSRank* mds)
{
  if (g_mds_table_test_io)
    return g_mds_table_test_io->pool;
  ceph_assert(mds);
  return mds->get_metadata_pool();
}
} // namespace

void
MDSTableTestAccess::set_write_hook(write_hook_t hook)
{
  g_mds_table_write_hook = std::move(hook);
}

void
MDSTableTestAccess::clear_write_hook()
{
  g_mds_table_write_hook = nullptr;
}

void
MDSTableTestAccess::set_test_io(TestIO* io)
{
  g_mds_table_test_io = io;
}

void
MDSTableTestAccess::clear_test_io()
{
  g_mds_table_test_io = nullptr;
}

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
  auto& lock = mds_table_lock(mds);
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  if (v > 0 && v <= committing_version) {
    dout(10) << "save v " << version << " - already saving "
	     << committing_version << " >= needed " << v << dendl;
    if (onfinish)
      waitfor_save[v].push_back(onfinish);
    return;
  }

  // A prior write_full may still be in flight (we drop mds_lock around the
  // Objecter submit below). Do not start another RADOS write until it completes.
  if (committing_version > committed_version) {
    dout(10) << "save v " << version << " - deferring, write in flight for "
             << committing_version << dendl;
    if (onfinish)
      waitfor_save[version].push_back(onfinish);
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
  object_locator_t oloc(mds_table_pool(mds));
  Context* fin = new C_OnFinisher(
      new C_IO_MT_Save(this, version), mds_table_finisher(mds));

  // Objecter may block in _throttle_op. MDLog::log_trim_upkeep holds
  // mds_lock across try_expire -> save; do not keep mds_lock while waiting
  // on the throttle or the whole MDS stalls (dispatch/asok blocked).
  lock.unlock();
  if (g_mds_table_write_hook) {
    g_mds_table_write_hook(fin);
  } else {
    ceph_assert(mds);
    mds->objecter->write_full(
        oid, oloc, snapc, bl, ceph::real_clock::now(), 0, fin);
  }
  lock.lock();
}

void MDSTable::save_2(int r, version_t v)
{
  ceph_assert(ceph_mutex_is_locked_by_me(mds->mds_lock));

  if (r < 0) {
    dout(1) << "save error " << r << " v " << v << dendl;
    mds->clog->error() << "failed to store table " << table_name << " object,"
		       << " errno " << r;
    mds->handle_write_error(r);
    return;
  }

  dout(10) << "save_2 v " << v << dendl;
  if (v >= committed_version)
    committed_version = v;

  MDSContext::vec ls;
  while (!waitfor_save.empty()) {
    auto it = waitfor_save.begin();
    if (it->first > v) break;
    auto& contexts = it->second;
    ls.insert(ls.end(), contexts.begin(), contexts.end());
    waitfor_save.erase(it);
  }
  finish_contexts(g_ceph_context, ls, 0);

  // Table may have advanced while mds_lock was dropped around write_full.
  if (version > committed_version) {
    save(nullptr, version);
  }
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
  object_locator_t oloc(mds->get_metadata_pool());
  mds->objecter->read_full(oid, oloc, CEPH_NOSNAP, &c->bl, 0,
			   new C_OnFinisher(c, mds->finisher));
}

void MDSTable::load_2(int r, bufferlist& bl, Context *onfinish)
{
  ceph_assert(is_opening());
  state = STATE_ACTIVE;
  if (r == -EBLOCKLISTED) {
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
