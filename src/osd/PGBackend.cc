// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "PGBackend.h"
#include "OSD.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, PGBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

// -- ObjectModDesc --
struct RollbackVisitor : public ObjectModDesc::Visitor {
  const hobject_t &hoid;
  PGBackend *pg;
  ObjectStore::Transaction t;
  RollbackVisitor(
    const hobject_t &hoid,
    PGBackend *pg) : hoid(hoid), pg(pg) {}
  void append(uint64_t old_size) {
    ObjectStore::Transaction temp;
    pg->rollback_append(hoid, old_size, &temp);
    temp.append(t);
    temp.swap(t);
  }
  void setattrs(map<string, boost::optional<bufferlist> > &attrs) {
    ObjectStore::Transaction temp;
    pg->rollback_setattrs(hoid, attrs, &temp);
    temp.append(t);
    temp.swap(t);
  }
  void rmobject(version_t old_version) {
    ObjectStore::Transaction temp;
    pg->rollback_stash(hoid, old_version, &temp);
    temp.append(t);
    temp.swap(t);
  }
  void create() {
    ObjectStore::Transaction temp;
    pg->rollback_create(hoid, &temp);
    temp.append(t);
    temp.swap(t);
  }
  void update_snaps(set<snapid_t> &snaps) {
    // pass
  }
};

void PGBackend::rollback(
  const hobject_t &hoid,
  const ObjectModDesc &desc,
  ObjectStore::Transaction *t)
{
  assert(desc.can_rollback());
  RollbackVisitor vis(hoid, this);
  desc.visit(&vis);
  t->append(vis.t);
}


void PGBackend::rollback_setattrs(
  const hobject_t &hoid,
  map<string, boost::optional<bufferlist> > &old_attrs,
  ObjectStore::Transaction *t) {
  map<string, bufferlist> to_set;
  set<string> to_remove;
  for (map<string, boost::optional<bufferlist> >::iterator i = old_attrs.begin();
       i != old_attrs.end();
       ++i) {
    if (i->second) {
      to_set[i->first] = i->second.get();
    } else {
      t->rmattr(coll, hoid, i->first);
    }
  }
  t->setattrs(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    to_set);
}

void PGBackend::rollback_append(
  const hobject_t &hoid,
  uint64_t old_size,
  ObjectStore::Transaction *t) {
  t->truncate(coll, hoid, old_size);
}

void PGBackend::rollback_stash(
  const hobject_t &hoid,
  version_t old_version,
  ObjectStore::Transaction *t) {
  t->remove(coll, hoid);
  t->collection_move_rename(
    coll,
    ghobject_t(hoid, old_version, 0),
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
}

void PGBackend::rollback_create(
  const hobject_t &hoid,
  ObjectStore::Transaction *t) {
  t->remove(coll, hoid);
}

void PGBackend::trim_stashed_object(
  const hobject_t &hoid,
  version_t old_version,
  ObjectStore::Transaction *t) {
  t->remove(coll, ghobject_t(hoid, old_version, 0));
}
