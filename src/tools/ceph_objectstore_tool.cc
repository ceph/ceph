// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/scoped_ptr.hpp>

#include <stdlib.h>

#include "common/Formatter.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"

#include "os/ObjectStore.h"
#include "os/filestore/FileJournal.h"
#ifdef HAVE_LIBFUSE
#include "os/FuseStore.h"
#endif

#include "osd/PGLog.h"
#include "osd/OSD.h"
#include "osd/PG.h"

#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_reader.h"

#include "ceph_objectstore_tool.h"
#include "include/compat.h"

namespace po = boost::program_options;
using namespace std;

#ifdef INTERNAL_TEST
CompatSet get_test_compat_set() {
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_PGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_OLOC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_HOBJECTPOOL);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BIGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG);
#ifdef INTERNAL_TEST2
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_SNAPMAPPER);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_SHARDS);
#endif
  return CompatSet(ceph_osd_feature_compat, ceph_osd_feature_ro_compat,
		   ceph_osd_feature_incompat);
}
#endif

const ssize_t max_read = 1024 * 1024;
const int fd_none = INT_MIN;
bool outistty;
bool dry_run = false;

struct action_on_object_t {
  virtual ~action_on_object_t() {}
  virtual int call(ObjectStore *store, coll_t coll, ghobject_t &ghobj, object_info_t &oi) = 0;
};

int _action_on_all_objects_in_pg(ObjectStore *store, coll_t coll, action_on_object_t &action, bool debug)
{
  unsigned LIST_AT_A_TIME = 100;
  ghobject_t next;
  while (!next.is_max()) {
    vector<ghobject_t> list;
    int r = store->collection_list(
				   coll,
				   next,
				   ghobject_t::get_max(),
				   true,
				   LIST_AT_A_TIME,
				   &list,
				   &next);
    if (r < 0) {
      cerr << "Error listing collection: " << coll << ", "
	   << cpp_strerror(r) << std::endl;
      return r;
    }
    for (vector<ghobject_t>::iterator obj = list.begin();
	 obj != list.end();
	 ++obj) {
      if (obj->is_pgmeta())
	continue;
      object_info_t oi;
      if (coll != coll_t::meta()) {
        bufferlist attr;
        r = store->getattr(coll, *obj, OI_ATTR, attr);
        if (r < 0) {
	  cerr << "Error getting attr on : " << make_pair(coll, *obj) << ", "
	       << cpp_strerror(r) << std::endl;
	  continue;
        }
        bufferlist::iterator bp = attr.begin();
        try {
	  ::decode(oi, bp);
        } catch (...) {
	  r = -EINVAL;
	  cerr << "Error getting attr on : " << make_pair(coll, *obj) << ", "
	       << cpp_strerror(r) << std::endl;
	  continue;
        }
      }
      r = action.call(store, coll, *obj, oi);
      if (r < 0)
	return r;
    }
  }
  return 0;
}

int action_on_all_objects_in_pg(ObjectStore *store, string pgidstr, action_on_object_t &action, bool debug)
{
  spg_t pgid;
  // Scan collections in case this is an ec pool but no shard specified
  unsigned scanned = 0;
  int r = 0;
  vector<coll_t> colls_to_check;
  vector<coll_t> candidates;
  r = store->list_collections(candidates);
  if (r < 0) {
    cerr << "Error listing collections: " << cpp_strerror(r) << std::endl;
    return r;
  }
  pgid.parse(pgidstr.c_str());
  for (vector<coll_t>::iterator i = candidates.begin();
       i != candidates.end();
       ++i) {
    spg_t cand_pgid;
    if (!i->is_pg(&cand_pgid))
      continue;

    // If an exact match or treat no shard as any shard
    if (cand_pgid == pgid ||
        (pgid.is_no_shard() && pgid.pgid == cand_pgid.pgid)) {
      colls_to_check.push_back(*i);
    }
  }

  if (debug)
    cerr << colls_to_check.size() << " pgs to scan" << std::endl;
  for (vector<coll_t>::iterator i = colls_to_check.begin();
       i != colls_to_check.end();
       ++i, ++scanned) {
    if (debug)
      cerr << "Scanning " << *i << ", " << scanned << "/"
	   << colls_to_check.size() << " completed" << std::endl;
    r = _action_on_all_objects_in_pg(store, *i, action, debug);
    if (r < 0)
      break;
  }
  return r;
}

int action_on_all_objects_in_exact_pg(ObjectStore *store, coll_t coll, action_on_object_t &action, bool debug)
{
  int r = _action_on_all_objects_in_pg(store, coll, action, debug);
  return r;
}

int _action_on_all_objects(ObjectStore *store, action_on_object_t &action, bool debug)
{
  unsigned scanned = 0;
  int r = 0;
  vector<coll_t> colls_to_check;
  vector<coll_t> candidates;
  r = store->list_collections(candidates);
  if (r < 0) {
    cerr << "Error listing collections: " << cpp_strerror(r) << std::endl;
    return r;
  }
  for (vector<coll_t>::iterator i = candidates.begin();
       i != candidates.end();
       ++i) {
    if (i->is_pg()) {
      colls_to_check.push_back(*i);
    }
  }

  if (debug)
    cerr << colls_to_check.size() << " pgs to scan" << std::endl;
  for (vector<coll_t>::iterator i = colls_to_check.begin();
       i != colls_to_check.end();
       ++i, ++scanned) {
    if (debug)
      cerr << "Scanning " << *i << ", " << scanned << "/"
	   << colls_to_check.size() << " completed" << std::endl;
    r = _action_on_all_objects_in_pg(store, *i, action, debug);
    if (r < 0)
      return r;
  }
  return 0;
}

int action_on_all_objects(ObjectStore *store, action_on_object_t &action, bool debug)
{
  int r = _action_on_all_objects(store, action, debug);
  return r;
}

struct pgid_object_list {
  list<pair<coll_t, ghobject_t> > _objects;

  void insert(coll_t coll, ghobject_t &ghobj) {
    _objects.push_back(make_pair(coll, ghobj));
  }

  void dump(Formatter *f, bool human_readable) const {
    if (!human_readable)
      f->open_array_section("pgid_objects");
    for (list<pair<coll_t, ghobject_t> >::const_iterator i = _objects.begin();
	 i != _objects.end();
	 ++i) {
      f->open_array_section("pgid_object");
      spg_t pgid;
      bool is_pg = i->first.is_pg(&pgid);
      if (is_pg)
        f->dump_string("pgid", stringify(pgid));
      if (!is_pg || !human_readable)
        f->dump_string("coll", i->first.to_str());
      f->open_object_section("ghobject");
      i->second.dump(f);
      f->close_section();
      f->close_section();
      if (human_readable) {
        f->flush(cout);
        cout << std::endl;
      }
    }
    if (!human_readable) {
      f->close_section();
      f->flush(cout);
      cout << std::endl;
    }
  }
};

struct lookup_ghobject : public action_on_object_t {
  pgid_object_list _objects;
  const string _name;
  bool _need_snapset;

  lookup_ghobject(const string& name, bool need_snapset = false) : _name(name),
		  _need_snapset(need_snapset) { }

  virtual int call(ObjectStore *store, coll_t coll, ghobject_t &ghobj, object_info_t &oi) {
    if (_need_snapset && !ghobj.hobj.has_snapset())
      return 0;
    if (_name.length() == 0 || ghobj.hobj.oid.name == _name)
      _objects.insert(coll, ghobj);
    return 0;
  }

  int size() const {
    return _objects._objects.size();
  }

  pair<coll_t, ghobject_t> pop() {
     pair<coll_t, ghobject_t> front = _objects._objects.front();
     _objects._objects.pop_front();
     return front;
  }

  void dump(Formatter *f, bool human_readable) const {
    _objects.dump(f, human_readable);
  }
};

ghobject_t infos_oid = OSD::make_infos_oid();
ghobject_t log_oid;
ghobject_t biginfo_oid;

int file_fd = fd_none;
bool debug = false;
super_header sh;
uint64_t testalign;

// Convert non-printable characters to '\###'
static void cleanbin(string &str)
{
  bool cleaned = false;
  string clean;

  for (string::iterator it = str.begin(); it != str.end(); ++it) {
    if (!isprint(*it)) {
      clean.push_back('\\');
      clean.push_back('0' + ((*it >> 6) & 7));
      clean.push_back('0' + ((*it >> 3) & 7));
      clean.push_back('0' + (*it & 7));
      cleaned = true;
    } else {
      clean.push_back(*it);
    }
  }

  if (cleaned)
    str = clean;
  return;
}

static int get_fd_data(int fd, bufferlist &bl)
{
  uint64_t total = 0;
  do {
    ssize_t bytes = bl.read_fd(fd, max_read);
    if (bytes < 0) {
      cerr << "read_fd error " << cpp_strerror(bytes) << std::endl;
      return bytes;
    }

    if (bytes == 0)
      break;

    total += bytes;
  } while(true);

  assert(bl.length() == total);
  return 0;
}

void myexit(int ret)
{
  if (g_ceph_context)
    g_ceph_context->put();
  exit(ret);
}

int get_log(ObjectStore *fs, __u8 struct_ver,
   coll_t coll, spg_t pgid, const pg_info_t &info,
   PGLog::IndexedLog &log, pg_missing_t &missing,
   map<eversion_t, hobject_t> &divergent_priors)
{
  try {
    ostringstream oss;
    assert(struct_ver > 0);
    PGLog::read_log(fs, coll,
		    struct_ver >= 8 ? coll : coll_t::meta(),
		    struct_ver >= 8 ? pgid.make_pgmeta_oid() : log_oid,
		    info, divergent_priors, log, missing, oss);
    if (debug && oss.str().size())
      cerr << oss.str() << std::endl;
  }
  catch (const buffer::error &e) {
    cerr << "read_log threw exception error " << e.what() << std::endl;
    return -EFAULT;
  }
  return 0;
}

void dump_log(Formatter *formatter, ostream &out, pg_log_t &log,
      pg_missing_t &missing, map<eversion_t, hobject_t> &divergent_priors)
{
  formatter->open_object_section("op_log");
  formatter->open_object_section("pg_log_t");
  log.dump(formatter);
  formatter->close_section();
  formatter->flush(out);
  formatter->open_object_section("pg_missing_t");
  missing.dump(formatter);
  formatter->close_section();
  formatter->flush(out);
  formatter->open_object_section("map");
  formatter->open_array_section("divergent_priors");
  for (map<eversion_t, hobject_t>::iterator it = divergent_priors.begin();
       it != divergent_priors.end(); ++ it) {
      formatter->open_object_section("item");
      formatter->dump_stream("eversion") << it->first;
      formatter->dump_stream("hobject") << it->second;
      formatter->close_section();
  }
  formatter->close_section();
  formatter->close_section();
  formatter->close_section();
  formatter->flush(out);
}

//Based on part of OSD::load_pgs()
int finish_remove_pgs(ObjectStore *store)
{
  vector<coll_t> ls;
  int r = store->list_collections(ls);
  if (r < 0) {
    cerr << "finish_remove_pgs: failed to list pgs: " << cpp_strerror(r)
      << std::endl;
    return r;
  }

  for (vector<coll_t>::iterator it = ls.begin();
       it != ls.end();
       ++it) {
    spg_t pgid;

    if (it->is_temp(&pgid) ||
       (it->is_pg(&pgid) && PG::_has_removal_flag(store, pgid))) {
      cout << "finish_remove_pgs " << *it << " removing " << pgid << std::endl;
      OSD::recursive_remove_collection(store, pgid, *it);
      continue;
    }

    //cout << "finish_remove_pgs ignoring unrecognized " << *it << std::endl;
  }
  return 0;
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

int mark_pg_for_removal(ObjectStore *fs, spg_t pgid, ObjectStore::Transaction *t)
{
  pg_info_t info(pgid);
  coll_t coll(pgid);
  ghobject_t pgmeta_oid(info.pgid.make_pgmeta_oid());

  bufferlist bl;
  epoch_t map_epoch = 0;
  int r = PG::peek_map_epoch(fs, pgid, &map_epoch, &bl);
  if (r < 0)
    cerr << __func__ << " warning: peek_map_epoch reported error" << std::endl;
  map<epoch_t,pg_interval_t> past_intervals;
  __u8 struct_v;
  r = PG::read_info(fs, pgid, coll, bl, info, past_intervals, struct_v);
  if (r < 0) {
    cerr << __func__ << " error on read_info " << cpp_strerror(r) << std::endl;
    return r;
  }
  if (struct_v < 8) {
    // old xattr
    cout << "setting legacy 'remove' xattr flag" << std::endl;
    bufferlist one;
    one.append('1');
    t->collection_setattr(coll, "remove", one);
    cout << "remove " << coll_t::meta() << " " << log_oid << std::endl;
    t->remove(coll_t::meta(), log_oid);
    cout << "remove " << coll_t::meta() << " " << biginfo_oid << std::endl;
    t->remove(coll_t::meta(), biginfo_oid);
  } else {
    // new omap key
    cout << "setting '_remove' omap key" << std::endl;
    map<string,bufferlist> values;
    ::encode((char)1, values["_remove"]);
    t->omap_setkeys(coll, pgmeta_oid, values);
  }
  return 0;
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

int initiate_new_remove_pg(ObjectStore *store, spg_t r_pgid,
			   ObjectStore::Sequencer &osr)
{
  if (!dry_run)
    finish_remove_pgs(store);
  if (!store->collection_exists(coll_t(r_pgid)))
    return -ENOENT;

  cout << " marking collection for removal" << std::endl;
  if (dry_run)
    return 0;
  ObjectStore::Transaction rmt;
  int r = mark_pg_for_removal(store, r_pgid, &rmt);
  if (r < 0) {
    return r;
  }
  store->apply_transaction(&osr, std::move(rmt));
  finish_remove_pgs(store);
  return r;
}

int write_info(ObjectStore::Transaction &t, epoch_t epoch, pg_info_t &info,
    map<epoch_t,pg_interval_t> &past_intervals)
{
  //Empty for this
  coll_t coll(info.pgid);
  ghobject_t pgmeta_oid(info.pgid.make_pgmeta_oid());
  map<string,bufferlist> km;
  int ret = PG::_prepare_write_info(
    &km, epoch,
    info, coll,
    past_intervals,
    pgmeta_oid,
    true, true);
  if (ret) cerr << "Failed to write info" << std::endl;
  t.omap_setkeys(coll, pgmeta_oid, km);
  return ret;
}

int write_pg(ObjectStore::Transaction &t, epoch_t epoch, pg_info_t &info,
    pg_log_t &log, map<epoch_t,pg_interval_t> &past_intervals,
    map<eversion_t, hobject_t> &divergent_priors)
{
  int ret = write_info(t, epoch, info, past_intervals);
  if (ret)
    return ret;
  coll_t coll(info.pgid);
  map<string,bufferlist> km;
  PGLog::write_log(t, &km, log, coll, info.pgid.make_pgmeta_oid(), divergent_priors, true);
  t.omap_setkeys(coll, info.pgid.make_pgmeta_oid(), km);
  return 0;
}

const int OMAP_BATCH_SIZE = 25;
void get_omap_batch(ObjectMap::ObjectMapIterator &iter, map<string, bufferlist> &oset)
{
  oset.clear();
  for (int count = OMAP_BATCH_SIZE; count && iter->valid(); --count, iter->next()) {
    oset.insert(pair<string, bufferlist>(iter->key(), iter->value()));
  }
}

int ObjectStoreTool::export_file(ObjectStore *store, coll_t cid, ghobject_t &obj)
{
  struct stat st;
  mysize_t total;
  footer ft;

  int ret = store->stat(cid, obj, &st);
  if (ret < 0)
    return ret;

  cerr << "Read " << obj << std::endl;

  total = st.st_size;
  if (debug)
    cerr << "size=" << total << std::endl;

  object_begin objb(obj);

  {
    bufferptr bp;
    bufferlist bl;
    ret = store->getattr(cid, obj, OI_ATTR, bp);
    if (ret < 0) {
      cerr << "getattr failure object_info " << ret << std::endl;
      return ret;
    }
    bl.push_back(bp);
    decode(objb.oi, bl);
    if (debug)
      cerr << "object_info: " << objb.oi << std::endl;
  }

  // XXX: Should we be checking for WHITEOUT or LOST in objb.oi.flags and skip?

  ret = write_section(TYPE_OBJECT_BEGIN, objb, file_fd);
  if (ret < 0)
    return ret;

  uint64_t offset = 0;
  bufferlist rawdatabl;
  while(total > 0) {
    rawdatabl.clear();
    mysize_t len = max_read;
    if (len > total)
      len = total;

    ret = store->read(cid, obj, offset, len, rawdatabl);
    if (ret < 0)
      return ret;
    if (ret == 0)
      return -EINVAL;

    data_section dblock(offset, len, rawdatabl);
    if (debug)
      cerr << "data section offset=" << offset << " len=" << len << std::endl;

    total -= ret;
    offset += ret;

    ret = write_section(TYPE_DATA, dblock, file_fd);
    if (ret) return ret;
  }

  //Handle attrs for this object
  map<string,bufferptr> aset;
  ret = store->getattrs(cid, obj, aset);
  if (ret) return ret;
  attr_section as(aset);
  ret = write_section(TYPE_ATTRS, as, file_fd);
  if (ret)
    return ret;

  if (debug) {
    cerr << "attrs size " << aset.size() << std::endl;
  }

  //Handle omap information
  bufferlist hdrbuf;
  ret = store->omap_get_header(cid, obj, &hdrbuf, true);
  if (ret < 0) {
    cerr << "omap_get_header: " << cpp_strerror(ret) << std::endl;
    return ret;
  }

  omap_hdr_section ohs(hdrbuf);
  ret = write_section(TYPE_OMAP_HDR, ohs, file_fd);
  if (ret)
    return ret;

  ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(cid, obj);
  if (!iter) {
    ret = -ENOENT;
    cerr << "omap_get_iterator: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  iter->seek_to_first();
  int mapcount = 0;
  map<string, bufferlist> out;
  while(iter->valid()) {
    get_omap_batch(iter, out);

    if (out.empty()) break;

    mapcount += out.size();
    omap_section oms(out);
    ret = write_section(TYPE_OMAP, oms, file_fd);
    if (ret)
      return ret;
  }
  if (debug)
    cerr << "omap map size " << mapcount << std::endl;

  ret = write_simple(TYPE_OBJECT_END, file_fd);
  if (ret)
    return ret;

  return 0;
}

int ObjectStoreTool::export_files(ObjectStore *store, coll_t coll)
{
  ghobject_t next;

  while (!next.is_max()) {
    vector<ghobject_t> objects;
    int r = store->collection_list(coll, next, ghobject_t::get_max(), true, 300,
      &objects, &next);
    if (r < 0)
      return r;
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      assert(!i->hobj.is_meta());
      if (i->is_pgmeta() || i->hobj.is_temp()) {
	continue;
      }
      r = export_file(store, coll, *i);
      if (r < 0)
        return r;
    }
  }
  return 0;
}

int set_inc_osdmap(ObjectStore *store, epoch_t e, bufferlist& bl, bool force,
		   ObjectStore::Sequencer &osr) {
  OSDMap::Incremental inc;
  bufferlist::iterator it = bl.begin();
  inc.decode(it);
  if (e == 0) {
    e = inc.epoch;
  } else if (e != inc.epoch) {
    cerr << "incremental.epoch mismatch: "
	 << inc.epoch << " != " << e << std::endl;
    if (force) {
      cerr << "But will continue anyway." << std::endl;
    } else {
      return -EINVAL;
    }
  }
  const ghobject_t inc_oid = OSD::get_inc_osdmap_pobject_name(e);
  if (!store->exists(coll_t::meta(), inc_oid)) {
    cerr << "inc-osdmap (" << inc_oid << ") does not exist." << std::endl;
    if (!force) {
      return -ENOENT;
    }
    cout << "Creating a new epoch." << std::endl;
  }
  if (dry_run)
    return 0;
  ObjectStore::Transaction t;
  t.write(coll_t::meta(), inc_oid, 0, bl.length(), bl);
  t.truncate(coll_t::meta(), inc_oid, bl.length());
  int ret = store->apply_transaction(&osr, std::move(t));
  if (ret) {
    cerr << "Failed to set inc-osdmap (" << inc_oid << "): " << ret << std::endl;
  } else {
    cout << "Wrote inc-osdmap." << inc.epoch << std::endl;
  }
  return ret;
}

int get_inc_osdmap(ObjectStore *store, epoch_t e, bufferlist& bl)
{
  if (store->read(coll_t::meta(),
		  OSD::get_inc_osdmap_pobject_name(e),
		  0, 0, bl) < 0) {
    return -ENOENT;
  }
  return 0;
}

int set_osdmap(ObjectStore *store, epoch_t e, bufferlist& bl, bool force,
	       ObjectStore::Sequencer &osr) {
  OSDMap osdmap;
  osdmap.decode(bl);
  if (e == 0) {
    e = osdmap.get_epoch();
  } else if (e != osdmap.get_epoch()) {
    cerr << "osdmap.epoch mismatch: "
	 << e << " != " << osdmap.get_epoch() << std::endl;
    if (force) {
      cerr << "But will continue anyway." << std::endl;
    } else {
      return -EINVAL;
    }
  }
  const ghobject_t full_oid = OSD::get_osdmap_pobject_name(e);
  if (!store->exists(coll_t::meta(), full_oid)) {
    cerr << "osdmap (" << full_oid << ") does not exist." << std::endl;
    if (!force) {
      return -ENOENT;
    }
    cout << "Creating a new epoch." << std::endl;
  }
  if (dry_run)
    return 0;
  ObjectStore::Transaction t;
  t.write(coll_t::meta(), full_oid, 0, bl.length(), bl);
  t.truncate(coll_t::meta(), full_oid, bl.length());
  int ret = store->apply_transaction(&osr, std::move(t));
  if (ret) {
    cerr << "Failed to set osdmap (" << full_oid << "): " << ret << std::endl;
  } else {
    cout << "Wrote osdmap." << osdmap.get_epoch() << std::endl;
  }
  return ret;
}

int get_osdmap(ObjectStore *store, epoch_t e, OSDMap &osdmap, bufferlist& bl)
{
  bool found = store->read(
      coll_t::meta(), OSD::get_osdmap_pobject_name(e), 0, 0, bl) >= 0;
  if (!found) {
    cerr << "Can't find OSDMap for pg epoch " << e << std::endl;
    return -ENOENT;
  }
  osdmap.decode(bl);
  if (debug)
    cerr << osdmap << std::endl;
  return 0;
}

int add_osdmap(ObjectStore *store, metadata_section &ms)
{
  return get_osdmap(store, ms.map_epoch, ms.osdmap, ms.osdmap_bl);
}

int ObjectStoreTool::do_export(ObjectStore *fs, coll_t coll, spg_t pgid,
    pg_info_t &info, epoch_t map_epoch, __u8 struct_ver,
    const OSDSuperblock& superblock,
    map<epoch_t,pg_interval_t> &past_intervals)
{
  PGLog::IndexedLog log;
  pg_missing_t missing;
  map<eversion_t, hobject_t> divergent_priors;

  cerr << "Exporting " << pgid << std::endl;

  int ret = get_log(fs, struct_ver, coll, pgid, info, log, missing,
                    divergent_priors);
  if (ret > 0)
      return ret;

  if (debug) {
    Formatter *formatter = Formatter::create("json-pretty");
    assert(formatter);
    dump_log(formatter, cerr, log, missing, divergent_priors);
    delete formatter;
  }
  write_super();

  pg_begin pgb(pgid, superblock);
  // Special case: If replicated pg don't require the importing OSD to have shard feature
  if (pgid.is_no_shard()) {
    pgb.superblock.compat_features.incompat.remove(CEPH_OSD_FEATURE_INCOMPAT_SHARDS);
  }
  ret = write_section(TYPE_PG_BEGIN, pgb, file_fd);
  if (ret)
    return ret;

  // The metadata_section is now before files, so import can detect
  // errors and abort without wasting time.
  metadata_section ms(struct_ver, map_epoch, info, log, past_intervals, divergent_priors);
  ret = add_osdmap(fs, ms);
  if (ret)
    return ret;
  ret = write_section(TYPE_PG_METADATA, ms, file_fd);
  if (ret)
    return ret;

  ret = export_files(fs, coll);
  if (ret) {
    cerr << "export_files error " << ret << std::endl;
    return ret;
  }

  ret = write_simple(TYPE_PG_END, file_fd);
  if (ret)
    return ret;

  return 0;
}

int get_data(ObjectStore *store, coll_t coll, ghobject_t hoid,
    ObjectStore::Transaction *t, bufferlist &bl)
{
  bufferlist::iterator ebliter = bl.begin();
  data_section ds;
  ds.decode(ebliter);

  if (debug)
    cerr << "\tdata: offset " << ds.offset << " len " << ds.len << std::endl;
  t->write(coll, hoid, ds.offset, ds.len,  ds.databl);
  return 0;
}

int get_attrs(ObjectStore *store, coll_t coll, ghobject_t hoid,
    ObjectStore::Transaction *t, bufferlist &bl,
    OSDriver &driver, SnapMapper &snap_mapper)
{
  bufferlist::iterator ebliter = bl.begin();
  attr_section as;
  as.decode(ebliter);

  if (debug)
    cerr << "\tattrs: len " << as.data.size() << std::endl;
  t->setattrs(coll, hoid, as.data);

  // This could have been handled in the caller if we didn't need to
  // support exports that didn't include object_info_t in object_begin.
  if (hoid.hobj.snap < CEPH_MAXSNAP && hoid.generation == ghobject_t::NO_GEN) {
    map<string,bufferlist>::iterator mi = as.data.find(OI_ATTR);
    if (mi != as.data.end()) {
      object_info_t oi(mi->second);

      if (debug)
        cerr << "object_info " << oi << std::endl;

      OSDriver::OSTransaction _t(driver.get_transaction(t));
      set<snapid_t> oi_snaps(oi.snaps.begin(), oi.snaps.end());
      snap_mapper.add_oid(hoid.hobj, oi_snaps, &_t);
    }
  }

  return 0;
}

int get_omap_hdr(ObjectStore *store, coll_t coll, ghobject_t hoid,
    ObjectStore::Transaction *t, bufferlist &bl)
{
  bufferlist::iterator ebliter = bl.begin();
  omap_hdr_section oh;
  oh.decode(ebliter);

  if (debug)
    cerr << "\tomap header: " << string(oh.hdr.c_str(), oh.hdr.length())
      << std::endl;
  t->omap_setheader(coll, hoid, oh.hdr);
  return 0;
}

int get_omap(ObjectStore *store, coll_t coll, ghobject_t hoid,
    ObjectStore::Transaction *t, bufferlist &bl)
{
  bufferlist::iterator ebliter = bl.begin();
  omap_section os;
  os.decode(ebliter);

  if (debug)
    cerr << "\tomap: size " << os.omap.size() << std::endl;
  t->omap_setkeys(coll, hoid, os.omap);
  return 0;
}

int ObjectStoreTool::get_object(ObjectStore *store, coll_t coll,
				bufferlist &bl, OSDMap &curmap,
				bool *skipped_objects,
				ObjectStore::Sequencer &osr)
{
  ObjectStore::Transaction tran;
  ObjectStore::Transaction *t = &tran;
  bufferlist::iterator ebliter = bl.begin();
  object_begin ob;
  ob.decode(ebliter);
  OSDriver driver(
    store,
    coll_t(),
    OSD::make_snapmapper_oid());
  spg_t pg;
  coll.is_pg_prefix(&pg);
  SnapMapper mapper(&driver, 0, 0, 0, pg.shard);

  if (ob.hoid.hobj.is_temp()) {
    cerr << "ERROR: Export contains temporary object '" << ob.hoid << "'" << std::endl;
    return -EFAULT;
  }
  assert(g_ceph_context);
  if (ob.hoid.hobj.nspace != g_ceph_context->_conf->osd_hit_set_namespace) {
    object_t oid = ob.hoid.hobj.oid;
    object_locator_t loc(ob.hoid.hobj);
    pg_t raw_pgid = curmap.object_locator_to_pg(oid, loc);
    pg_t pgid = curmap.raw_pg_to_pg(raw_pgid);

    spg_t coll_pgid;
    if (coll.is_pg(&coll_pgid) == false) {
      cerr << "INTERNAL ERROR: Bad collection during import" << std::endl;
      return -EFAULT;
    }
    if (coll_pgid.shard != ob.hoid.shard_id) {
      cerr << "INTERNAL ERROR: Importing shard " << coll_pgid.shard
        << " but object shard is " << ob.hoid.shard_id << std::endl;
      return -EFAULT;
    }

    if (coll_pgid.pgid != pgid) {
      cerr << "Skipping object '" << ob.hoid << "' which belongs in pg " << pgid << std::endl;
      *skipped_objects = true;
      skip_object(bl);
      return 0;
    }
  }

  if (!dry_run)
    t->touch(coll, ob.hoid);

  cout << "Write " << ob.hoid << std::endl;

  bufferlist ebl;
  bool done = false;
  while(!done) {
    sectiontype_t type;
    int ret = read_section(&type, &ebl);
    if (ret)
      return ret;

    //cout << "\tdo_object: Section type " << hex << type << dec << std::endl;
    //cout << "\t\tsection size " << ebl.length() << std::endl;
    if (type >= END_OF_TYPES) {
      cout << "Skipping unknown object section type" << std::endl;
      continue;
    }
    switch(type) {
    case TYPE_DATA:
      if (dry_run) break;
      ret = get_data(store, coll, ob.hoid, t, ebl);
      if (ret) return ret;
      break;
    case TYPE_ATTRS:
      if (dry_run) break;
      ret = get_attrs(store, coll, ob.hoid, t, ebl, driver, mapper);
      if (ret) return ret;
      break;
    case TYPE_OMAP_HDR:
      if (dry_run) break;
      ret = get_omap_hdr(store, coll, ob.hoid, t, ebl);
      if (ret) return ret;
      break;
    case TYPE_OMAP:
      if (dry_run) break;
      ret = get_omap(store, coll, ob.hoid, t, ebl);
      if (ret) return ret;
      break;
    case TYPE_OBJECT_END:
      done = true;
      break;
    default:
      cerr << "Unknown section type " << type << std::endl;
      return -EFAULT;
    }
  }
  if (!dry_run)
    store->apply_transaction(&osr, std::move(*t));
  return 0;
}

int get_pg_metadata(ObjectStore *store, bufferlist &bl, metadata_section &ms,
    const OSDSuperblock& sb, OSDMap& curmap, spg_t pgid)
{
  bufferlist::iterator ebliter = bl.begin();
  ms.decode(ebliter);
  spg_t old_pgid = ms.info.pgid;
  ms.info.pgid = pgid;

#if DIAGNOSTIC
  Formatter *formatter = new JSONFormatter(true);
  cout << "export pgid " << old_pgid << std::endl;
  cout << "struct_v " << (int)ms.struct_ver << std::endl;
  cout << "map epoch " << ms.map_epoch << std::endl;

  formatter->open_object_section("importing OSDMap");
  ms.osdmap.dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;

  cout << "osd current epoch " << sb.current_epoch << std::endl;
  formatter->open_object_section("current OSDMap");
  curmap.dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;

  formatter->open_object_section("info");
  ms.info.dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;

  formatter->open_object_section("log");
  ms.log.dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;

  formatter->open_array_section("divergent_priors");
  for (map<eversion_t, hobject_t>::iterator it = ms.divergent_priors.begin();
       it != ms.divergent_priors.end(); ++ it) {
      formatter->open_object_section("item");
      formatter->dump_stream("eversion") << it->first;
      formatter->dump_stream("hobject") << it->second;
      formatter->close_section();
  }
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
#endif

  if (ms.osdmap.get_epoch() != 0 && ms.map_epoch != ms.osdmap.get_epoch()) {
    cerr << "FATAL: Invalid OSDMap epoch in export data" << std::endl;
    return -EFAULT;
  }

  if (ms.map_epoch > sb.current_epoch) {
    cerr << "ERROR: Export PG's map_epoch " << ms.map_epoch << " > OSD's epoch " << sb.current_epoch << std::endl;
    cerr << "The OSD you are using is older than the exported PG" << std::endl;
    cerr << "Either use another OSD or join selected OSD to cluster to update it first" << std::endl;
    return -EINVAL;
  }

  // Pool verified to exist for call to get_pg_num().
  unsigned new_pg_num = curmap.get_pg_num(pgid.pgid.pool());

  if (pgid.pgid.ps() >= new_pg_num) {
    cerr << "Illegal pgid, the seed is larger than current pg_num" << std::endl;
    return -EINVAL;
  }

  // Old exports didn't include OSDMap, see if we have a copy locally
  if (ms.osdmap.get_epoch() == 0) {
    OSDMap findmap;
    bufferlist findmap_bl;
    int ret = get_osdmap(store, ms.map_epoch, findmap, findmap_bl);
    if (ret == 0) {
      ms.osdmap = findmap;
    } else {
      cerr << "WARNING: No OSDMap in old export,"
           " some objects may be ignored due to a split" << std::endl;
    }
  }

  // Make sure old_pg_num is 0 in the unusual case that OSDMap not in export
  // nor can we find a local copy.
  unsigned old_pg_num = 0;
  if (ms.osdmap.get_epoch() != 0)
    old_pg_num = ms.osdmap.get_pg_num(pgid.pgid.pool());

  if (debug) {
    cerr << "old_pg_num " << old_pg_num << std::endl;
    cerr << "new_pg_num " << new_pg_num << std::endl;
    cerr << ms.osdmap << std::endl;
    cerr << curmap << std::endl;
  }

  // If we have managed to have a good OSDMap we can do these checks
  if (old_pg_num) {
    if (old_pgid.pgid.ps() >= old_pg_num) {
      cerr << "FATAL: pgid invalid for original map epoch" << std::endl;
      return -EFAULT;
    }
    if (pgid.pgid.ps() >= old_pg_num) {
      cout << "NOTICE: Post split pgid specified" << std::endl;
    } else {
      spg_t parent(pgid);
      if (parent.is_split(old_pg_num, new_pg_num, NULL)) {
            cerr << "WARNING: Split occurred, some objects may be ignored" << std::endl;
      }
    }
  }

  if (debug) {
    cerr << "Import pgid " << ms.info.pgid << std::endl;
    cerr << "Clearing past_intervals " << ms.past_intervals << std::endl;
    cerr << "Zero same_interval_since " << ms.info.history.same_interval_since << std::endl;
  }

  // Let osd recompute past_intervals and same_interval_since
  ms.past_intervals.clear();
  ms.info.history.same_interval_since =  0;

  if (debug)
    cerr << "Changing pg epoch " << ms.map_epoch << " to " << sb.current_epoch << std::endl;

  ms.map_epoch = sb.current_epoch;

  return 0;
}

typedef map<eversion_t, hobject_t> divergent_priors_t;

// out: pg_log_t that only has entries that apply to import_pgid using curmap
// reject: Entries rejected from "in" are in the reject.log.  Other fields not set.
void filter_divergent_priors(spg_t import_pgid, const OSDMap &curmap,
  const string &hit_set_namespace, const divergent_priors_t &in,
  divergent_priors_t &out, divergent_priors_t &reject)
{
  out.clear();
  reject.clear();

  for (divergent_priors_t::const_iterator i = in.begin();
       i != in.end(); ++i) {

    // Reject divergent priors for temporary objects
    if (i->second.is_temp()) {
      reject.insert(*i);
      continue;
    }

    if (i->second.nspace != hit_set_namespace) {
      object_t oid = i->second.oid;
      object_locator_t loc(i->second);
      pg_t raw_pgid = curmap.object_locator_to_pg(oid, loc);
      pg_t pgid = curmap.raw_pg_to_pg(raw_pgid);

      if (import_pgid.pgid == pgid) {
        out.insert(*i);
      } else {
        reject.insert(*i);
      }
    } else {
      out.insert(*i);
    }
  }
}

int ObjectStoreTool::do_import(ObjectStore *store, OSDSuperblock& sb,
			       bool force, std::string pgidstr,
			       ObjectStore::Sequencer &osr)
{
  bufferlist ebl;
  pg_info_t info;
  PGLog::IndexedLog log;
  bool skipped_objects = false;

  if (!dry_run)
    finish_remove_pgs(store);

  int ret = read_super();
  if (ret)
    return ret;

  if (sh.magic != super_header::super_magic) {
    cerr << "Invalid magic number" << std::endl;
    return -EFAULT;
  }

  if (sh.version > super_header::super_ver) {
    cerr << "Can't handle export format version=" << sh.version << std::endl;
    return -EINVAL;
  }

  //First section must be TYPE_PG_BEGIN
  sectiontype_t type;
  ret = read_section(&type, &ebl);
  if (ret)
    return ret;
  if (type == TYPE_POOL_BEGIN) {
    cerr << "Pool exports cannot be imported into a PG" << std::endl;
    return -EINVAL;
  } else if (type != TYPE_PG_BEGIN) {
    cerr << "Invalid first section type " << type << std::endl;
    return -EFAULT;
  }

  bufferlist::iterator ebliter = ebl.begin();
  pg_begin pgb;
  pgb.decode(ebliter);
  spg_t pgid = pgb.pgid;
  spg_t orig_pgid = pgid;

  if (pgidstr.length()) {
    spg_t user_pgid;

    bool ok = user_pgid.parse(pgidstr.c_str());
    // This succeeded in main() already
    assert(ok);
    if (pgid != user_pgid) {
      if (pgid.pool() != user_pgid.pool()) {
        cerr << "Can't specify a different pgid pool, must be " << pgid.pool() << std::endl;
        return -EINVAL;
      }
      if (pgid.is_no_shard() && !user_pgid.is_no_shard()) {
        cerr << "Can't specify a sharded pgid with a non-sharded export" << std::endl;
        return -EINVAL;
      }
      // Get shard from export information if not specified
      if (!pgid.is_no_shard() && user_pgid.is_no_shard()) {
        user_pgid.shard = pgid.shard;
      }
      if (pgid.shard != user_pgid.shard) {
        cerr << "Can't specify a different shard, must be " << pgid.shard << std::endl;
        return -EINVAL;
      }
      pgid = user_pgid;
    }
  }

  if (!pgb.superblock.cluster_fsid.is_zero()
      && pgb.superblock.cluster_fsid != sb.cluster_fsid) {
    cerr << "Export came from different cluster with fsid "
         << pgb.superblock.cluster_fsid << std::endl;
    return -EINVAL;
  }

  if (debug) {
    cerr << "Exported features: " << pgb.superblock.compat_features << std::endl;
  }

  // Special case: Old export has SHARDS incompat feature on replicated pg, remove it
  if (pgid.is_no_shard())
    pgb.superblock.compat_features.incompat.remove(CEPH_OSD_FEATURE_INCOMPAT_SHARDS);

  if (sb.compat_features.compare(pgb.superblock.compat_features) == -1) {
    CompatSet unsupported = sb.compat_features.unsupported(pgb.superblock.compat_features);

    cerr << "Export has incompatible features set " << unsupported << std::endl;

    // Let them import if they specify the --force option
    if (!force)
        return 11;  // Positive return means exit status
  }

  // Don't import if pool no longer exists
  OSDMap curmap;
  bufferlist bl;
  ret = get_osdmap(store, sb.current_epoch, curmap, bl);
  if (ret) {
    cerr << "Can't find local OSDMap" << std::endl;
    return ret;
  }
  if (!curmap.have_pg_pool(pgid.pgid.m_pool)) {
    cerr << "Pool " << pgid.pgid.m_pool << " no longer exists" << std::endl;
    // Special exit code for this error, used by test code
    return 10;  // Positive return means exit status
  }

  ghobject_t pgmeta_oid = pgid.make_pgmeta_oid();
  log_oid = OSD::make_pg_log_oid(pgid);
  biginfo_oid = OSD::make_pg_biginfo_oid(pgid);

  //Check for PG already present.
  coll_t coll(pgid);
  if (store->collection_exists(coll)) {
    cerr << "pgid " << pgid << " already exists" << std::endl;
    return -EEXIST;
  }

  if (!dry_run) {
    ObjectStore::Transaction t;
    PG::_create(t, pgid,
		pgid.get_split_bits(curmap.get_pg_pool(pgid.pool())->get_pg_num()));
    PG::_init(t, pgid, NULL);

    // mark this coll for removal until we're done
    map<string,bufferlist> values;
    ::encode((char)1, values["_remove"]);
    t.omap_setkeys(coll, pgid.make_pgmeta_oid(), values);

    store->apply_transaction(&osr, std::move(t));
  }

  cout << "Importing pgid " << pgid;
  if (orig_pgid != pgid) {
    cout << " exported as " << orig_pgid;
  }
  cout << std::endl;

  bool done = false;
  bool found_metadata = false;
  metadata_section ms;
  while(!done) {
    ret = read_section(&type, &ebl);
    if (ret)
      return ret;

    //cout << "do_import: Section type " << hex << type << dec << std::endl;
    if (type >= END_OF_TYPES) {
      cout << "Skipping unknown section type" << std::endl;
      continue;
    }
    switch(type) {
    case TYPE_OBJECT_BEGIN:
      ret = get_object(store, coll, ebl, curmap, &skipped_objects, osr);
      if (ret) return ret;
      break;
    case TYPE_PG_METADATA:
      ret = get_pg_metadata(store, ebl, ms, sb, curmap, pgid);
      if (ret) return ret;
      found_metadata = true;
      break;
    case TYPE_PG_END:
      done = true;
      break;
    default:
      cerr << "Unknown section type " << type << std::endl;
      return -EFAULT;
    }
  }

  if (!found_metadata) {
    cerr << "Missing metadata section" << std::endl;
    return -EFAULT;
  }

  ObjectStore::Transaction t;
  if (!dry_run) {
    pg_log_t newlog, reject;
    pg_log_t::filter_log(pgid, curmap, g_ceph_context->_conf->osd_hit_set_namespace,
      ms.log, newlog, reject);
    if (debug) {
      for (list<pg_log_entry_t>::iterator i = newlog.log.begin();
           i != newlog.log.end(); ++i)
        cerr << "Keeping log entry " << *i << std::endl;
      for (list<pg_log_entry_t>::iterator i = reject.log.begin();
           i != reject.log.end(); ++i)
        cerr << "Skipping log entry " << *i << std::endl;
    }

    divergent_priors_t newdp, rejectdp;
    filter_divergent_priors(pgid, curmap, g_ceph_context->_conf->osd_hit_set_namespace,
      ms.divergent_priors, newdp, rejectdp);
    ms.divergent_priors = newdp;
    if (debug) {
      for (divergent_priors_t::iterator i = newdp.begin();
           i != newdp.end(); ++i)
        cerr << "Keeping divergent_prior " << *i << std::endl;
      for (divergent_priors_t::iterator i = rejectdp.begin();
           i != rejectdp.end(); ++i)
        cerr << "Skipping divergent_prior " << *i << std::endl;
    }

    if (debug) {
      pg_missing_t missing;
      Formatter *formatter = Formatter::create("json-pretty");
      dump_log(formatter, cerr, newlog, missing, ms.divergent_priors);
      delete formatter;
    }

    // Just like a split invalidate stats since the object count is changed
    if (skipped_objects)
      ms.info.stats.stats_invalid = true;

    ret = write_pg(t, ms.map_epoch, ms.info, newlog, ms.past_intervals, ms.divergent_priors);
    if (ret) return ret;
  }

  // done, clear removal flag
  if (debug)
    cerr << "done, clearing removal flag" << std::endl;

  if (!dry_run) {
    set<string> remove;
    remove.insert("_remove");
    t.omap_rmkeys(coll, pgid.make_pgmeta_oid(), remove);
    store->apply_transaction(&osr, std::move(t));
  }

  return 0;
}

int do_list(ObjectStore *store, string pgidstr, string object,
	    Formatter *formatter, bool debug, bool human_readable, bool head)
{
  int r;
  lookup_ghobject lookup(object, head);
  if (pgidstr.length() > 0) {
    r = action_on_all_objects_in_pg(store, pgidstr, lookup, debug);
  } else {
    r = action_on_all_objects(store, lookup, debug);
  }
  if (r)
    return r;
  lookup.dump(formatter, human_readable);
  formatter->flush(cout);
  return 0;
}

int do_meta(ObjectStore *store, string object, Formatter *formatter, bool debug, bool human_readable)
{
  int r;
  lookup_ghobject lookup(object);
  r = action_on_all_objects_in_exact_pg(store, coll_t::meta(), lookup, debug);
  if (r)
    return r;
  lookup.dump(formatter, human_readable);
  formatter->flush(cout);
  return 0;
}

int do_remove_object(ObjectStore *store, coll_t coll,
		     ghobject_t &ghobj,
		     ObjectStore::Sequencer &osr)
{
  spg_t pg;
  coll.is_pg_prefix(&pg);
  OSDriver driver(
    store,
    coll_t(),
    OSD::make_snapmapper_oid());
  SnapMapper mapper(&driver, 0, 0, 0, pg.shard);
  struct stat st;

  int r = store->stat(coll, ghobj, &st);
  if (r < 0) {
    cerr << "remove: " << cpp_strerror(r) << std::endl;
    return r;
  }

  cout << "remove " << ghobj << std::endl;
  if (dry_run)
    return 0;
  ObjectStore::Transaction t;
  OSDriver::OSTransaction _t(driver.get_transaction(&t));
  r = mapper.remove_oid(ghobj.hobj, &_t);
  if (r < 0 && r != -ENOENT) {
    cerr << "remove_oid returned " << cpp_strerror(r) << std::endl;
    return r;
  }

  t.remove(coll, ghobj);

  store->apply_transaction(&osr, std::move(t));
  return 0;
}

int do_list_attrs(ObjectStore *store, coll_t coll, ghobject_t &ghobj)
{
  map<string,bufferptr> aset;
  int r = store->getattrs(coll, ghobj, aset);
  if (r < 0) {
    cerr << "getattrs: " << cpp_strerror(r) << std::endl;
    return r;
  }

  for (map<string,bufferptr>::iterator i = aset.begin();i != aset.end(); ++i) {
    string key(i->first);
    if (outistty)
      cleanbin(key);
    cout << key << std::endl;
  }
  return 0;
}

int do_list_omap(ObjectStore *store, coll_t coll, ghobject_t &ghobj)
{
  ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(coll, ghobj);
  if (!iter) {
    cerr << "omap_get_iterator: " << cpp_strerror(ENOENT) << std::endl;
    return -ENOENT;
  }
  iter->seek_to_first();
  map<string, bufferlist> oset;
  while(iter->valid()) {
    get_omap_batch(iter, oset);

    for (map<string,bufferlist>::iterator i = oset.begin();i != oset.end(); ++i) {
      string key(i->first);
      if (outistty)
        cleanbin(key);
      cout << key << std::endl;
    }
  }
  return 0;
}

int do_get_bytes(ObjectStore *store, coll_t coll, ghobject_t &ghobj, int fd)
{
  struct stat st;
  mysize_t total;

  int ret = store->stat(coll, ghobj, &st);
  if (ret < 0) {
    cerr << "get-bytes: " << cpp_strerror(ret) << std::endl;
    return ret;
  }

  total = st.st_size;
  if (debug)
    cerr << "size=" << total << std::endl;

  uint64_t offset = 0;
  bufferlist rawdatabl;
  while(total > 0) {
    rawdatabl.clear();
    mysize_t len = max_read;
    if (len > total)
      len = total;

    ret = store->read(coll, ghobj, offset, len, rawdatabl);
    if (ret < 0)
      return ret;
    if (ret == 0)
      return -EINVAL;

    if (debug)
      cerr << "data section offset=" << offset << " len=" << len << std::endl;

    total -= ret;
    offset += ret;

    ret = write(fd, rawdatabl.c_str(), ret);
    if (ret == -1) {
      perror("write");
      return -errno;
    }
  }

  return 0;
}

int do_set_bytes(ObjectStore *store, coll_t coll,
		 ghobject_t &ghobj, int fd,
		 ObjectStore::Sequencer &osr)
{
  ObjectStore::Transaction tran;
  ObjectStore::Transaction *t = &tran;

  if (debug)
    cerr << "Write " << ghobj << std::endl;

  if (!dry_run) {
    t->touch(coll, ghobj);
    t->truncate(coll, ghobj, 0);
  }

  uint64_t offset = 0;
  bufferlist rawdatabl;
  do {
    rawdatabl.clear();
    ssize_t bytes = rawdatabl.read_fd(fd, max_read);
    if (bytes < 0) {
      cerr << "read_fd error " << cpp_strerror(bytes) << std::endl;
      return bytes;
    }

    if (bytes == 0)
      break;

    if (debug)
      cerr << "\tdata: offset " << offset << " bytes " << bytes << std::endl;
    if (!dry_run)
      t->write(coll, ghobj, offset, bytes,  rawdatabl);

    offset += bytes;
    // XXX: Should we apply_transaction() every once in a while for very large files
  } while(true);

  if (!dry_run)
    store->apply_transaction(&osr, std::move(*t));
  return 0;
}

int do_get_attr(ObjectStore *store, coll_t coll, ghobject_t &ghobj, string key)
{
  bufferptr bp;

  int r = store->getattr(coll, ghobj, key.c_str(), bp);
  if (r < 0) {
    cerr << "getattr: " << cpp_strerror(r) << std::endl;
    return r;
  }

  string value(bp.c_str(), bp.length());
  if (outistty) {
    cleanbin(value);
    value.push_back('\n');
  }
  cout << value;

  return 0;
}

int do_set_attr(ObjectStore *store, coll_t coll,
		ghobject_t &ghobj, string key, int fd,
		ObjectStore::Sequencer &osr)
{
  ObjectStore::Transaction tran;
  ObjectStore::Transaction *t = &tran;
  bufferlist bl;

  if (debug)
    cerr << "Setattr " << ghobj << std::endl;

  int ret = get_fd_data(fd, bl);
  if (ret < 0)
    return ret;

  if (dry_run)
    return 0;

  t->touch(coll, ghobj);

  t->setattr(coll, ghobj, key,  bl);

  store->apply_transaction(&osr, std::move(*t));
  return 0;
}

int do_rm_attr(ObjectStore *store, coll_t coll,
	       ghobject_t &ghobj, string key,
	       ObjectStore::Sequencer &osr)
{
  ObjectStore::Transaction tran;
  ObjectStore::Transaction *t = &tran;

  if (debug)
    cerr << "Rmattr " << ghobj << std::endl;

  if (dry_run)
    return 0;

  t->rmattr(coll, ghobj, key);

  store->apply_transaction(&osr, std::move(*t));
  return 0;
}

int do_get_omap(ObjectStore *store, coll_t coll, ghobject_t &ghobj, string key)
{
  set<string> keys;
  map<string, bufferlist> out;

  keys.insert(key);

  int r = store->omap_get_values(coll, ghobj, keys, &out);
  if (r < 0) {
    cerr << "omap_get_values: " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (out.empty()) {
    cerr << "Key not found" << std::endl;
    return -ENOENT;
  }

  assert(out.size() == 1);

  bufferlist bl = out.begin()->second;
  string value(bl.c_str(), bl.length());
  if (outistty) {
    cleanbin(value);
    value.push_back('\n');
  }
  cout << value;

  return 0;
}

int do_set_omap(ObjectStore *store, coll_t coll,
		ghobject_t &ghobj, string key, int fd,
		ObjectStore::Sequencer &osr)
{
  ObjectStore::Transaction tran;
  ObjectStore::Transaction *t = &tran;
  map<string, bufferlist> attrset;
  bufferlist valbl;

  if (debug)
    cerr << "Set_omap " << ghobj << std::endl;

  int ret = get_fd_data(fd, valbl);
  if (ret < 0)
    return ret;

  attrset.insert(pair<string, bufferlist>(key, valbl));

  if (dry_run)
    return 0;

  t->touch(coll, ghobj);

  t->omap_setkeys(coll, ghobj, attrset);

  store->apply_transaction(&osr, std::move(*t));
  return 0;
}

int do_rm_omap(ObjectStore *store, coll_t coll,
	       ghobject_t &ghobj, string key,
	       ObjectStore::Sequencer &osr)
{
  ObjectStore::Transaction tran;
  ObjectStore::Transaction *t = &tran;
  set<string> keys;

  keys.insert(key);

  if (debug)
    cerr << "Rm_omap " << ghobj << std::endl;

  if (dry_run)
    return 0;

  t->omap_rmkeys(coll, ghobj, keys);

  store->apply_transaction(&osr, std::move(*t));
  return 0;
}

int do_get_omaphdr(ObjectStore *store, coll_t coll, ghobject_t &ghobj)
{
  bufferlist hdrbl;

  int r = store->omap_get_header(coll, ghobj, &hdrbl, true);
  if (r < 0) {
    cerr << "omap_get_header: " << cpp_strerror(r) << std::endl;
    return r;
  }

  string header(hdrbl.c_str(), hdrbl.length());
  if (outistty) {
    cleanbin(header);
    header.push_back('\n');
  }
  cout << header;

  return 0;
}

int do_set_omaphdr(ObjectStore *store, coll_t coll,
		   ghobject_t &ghobj, int fd,
		   ObjectStore::Sequencer &osr)
{
  ObjectStore::Transaction tran;
  ObjectStore::Transaction *t = &tran;
  bufferlist hdrbl;

  if (debug)
    cerr << "Omap_setheader " << ghobj << std::endl;

  int ret = get_fd_data(fd, hdrbl);
  if (ret)
    return ret;

  if (dry_run)
    return 0;

  t->touch(coll, ghobj);

  t->omap_setheader(coll, ghobj, hdrbl);

  store->apply_transaction(&osr, std::move(*t));
  return 0;
}

struct do_fix_lost : public action_on_object_t {
  ObjectStore::Sequencer *osr;

  explicit do_fix_lost(ObjectStore::Sequencer *_osr) : osr(_osr) {}

  virtual int call(ObjectStore *store, coll_t coll,
		   ghobject_t &ghobj, object_info_t &oi) {
    if (oi.is_lost()) {
      cout << coll << "/" << ghobj << " is lost";
      if (!dry_run)
        cout << ", fixing";
      cout << std::endl;
      if (dry_run)
        return 0;
      oi.clear_flag(object_info_t::FLAG_LOST);
      bufferlist bl;
      ::encode(oi, bl);
      ObjectStore::Transaction t;
      t.setattr(coll, ghobj, OI_ATTR, bl);
      int r = store->apply_transaction(osr, std::move(t));
      if (r < 0) {
	cerr << "Error getting fixing attr on : " << make_pair(coll, ghobj)
	     << ", "
	     << cpp_strerror(r) << std::endl;
	return r;
      }
    }
    return 0;
  }
};

int get_snapset(ObjectStore *store, coll_t coll, ghobject_t &ghobj, SnapSet &ss, bool silent = false)
{
  bufferlist attr;
  int r = store->getattr(coll, ghobj, SS_ATTR, attr);
  if (r < 0) {
    if (!silent)
      cerr << "Error getting snapset on : " << make_pair(coll, ghobj) << ", "
	   << cpp_strerror(r) << std::endl;
    return r;
  }
  bufferlist::iterator bp = attr.begin();
  try {
    ::decode(ss, bp);
  } catch (...) {
    r = -EINVAL;
    cerr << "Error decoding snapset on : " << make_pair(coll, ghobj) << ", "
         << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

int print_obj_info(ObjectStore *store, coll_t coll, ghobject_t &ghobj, Formatter* formatter)
{
  int r = 0;
  formatter->open_object_section("obj");
  formatter->open_object_section("id");
  ghobj.dump(formatter);
  formatter->close_section();

  bufferlist attr;
  int gr = store->getattr(coll, ghobj, OI_ATTR, attr);
  if (gr < 0) {
    r = gr;
    cerr << "Error getting attr on : " << make_pair(coll, ghobj) << ", "
       << cpp_strerror(r) << std::endl;
  } else {
    object_info_t oi;
    bufferlist::iterator bp = attr.begin();
    try {
      ::decode(oi, bp);
      formatter->open_object_section("info");
      oi.dump(formatter);
      formatter->close_section();
    } catch (...) {
      r = -EINVAL;
      cerr << "Error decoding attr on : " << make_pair(coll, ghobj) << ", "
           << cpp_strerror(r) << std::endl;
    }
  }
  struct stat st;
  int sr =  store->stat(coll, ghobj, &st, true);
  if (sr < 0) {
    r = sr;
    cerr << "Error stat on : " << make_pair(coll, ghobj) << ", "
         << cpp_strerror(r) << std::endl;
  } else {
    formatter->open_object_section("stat");
    formatter->dump_int("size", st.st_size);
    formatter->dump_int("blksize", st.st_blksize);
    formatter->dump_int("blocks", st.st_blocks);
    formatter->dump_int("nlink", st.st_nlink);
    formatter->close_section();
  }

  if (ghobj.hobj.has_snapset()) {
    SnapSet ss;
    int snr = get_snapset(store, coll, ghobj, ss);
    if (snr < 0) {
      r = snr;
    } else {
      formatter->open_object_section("SnapSet");
      ss.dump(formatter);
      formatter->close_section();
    }
  }
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
  return r;
}

int set_size(ObjectStore *store, coll_t coll, ghobject_t &ghobj, uint64_t setsize, Formatter* formatter,
	     ObjectStore::Sequencer &osr)
{
  if (ghobj.hobj.is_snapdir()) {
    cerr << "Can't set the size of a snapdir" << std::endl;
    return -EINVAL;
  }
  bufferlist attr;
  int r = store->getattr(coll, ghobj, OI_ATTR, attr);
  if (r < 0) {
    cerr << "Error getting attr on : " << make_pair(coll, ghobj) << ", "
       << cpp_strerror(r) << std::endl;
    return r;
  }
  object_info_t oi;
  bufferlist::iterator bp = attr.begin();
  try {
    ::decode(oi, bp);
  } catch (...) {
    r = -EINVAL;
    cerr << "Error getting attr on : " << make_pair(coll, ghobj) << ", "
         << cpp_strerror(r) << std::endl;
    return r;
  }
  struct stat st;
  r =  store->stat(coll, ghobj, &st, true);
  if (r < 0) {
    cerr << "Error stat on : " << make_pair(coll, ghobj) << ", "
         << cpp_strerror(r) << std::endl;
  }
  ghobject_t head(ghobj);
  SnapSet ss;
  bool found_head = true;
  map<snapid_t, uint64_t>::iterator csi;
  bool is_snap = ghobj.hobj.is_snap();
  if (is_snap) {
    head.hobj = head.hobj.get_head();
    r = get_snapset(store, coll, head, ss, true);
    if (r < 0 && r != -ENOENT) {
      // Requested get_snapset() silent, so if not -ENOENT show error
      cerr << "Error getting snapset on : " << make_pair(coll, head) << ", "
	   << cpp_strerror(r) << std::endl;
      return r;
    }
    if (r == -ENOENT) {
      head.hobj = head.hobj.get_snapdir();
      r = get_snapset(store, coll, head, ss);
      if (r < 0)
        return r;
      found_head = false;
    } else {
      found_head = true;
    }
    csi = ss.clone_size.find(ghobj.hobj.snap);
    if (csi == ss.clone_size.end()) {
      cerr << "SnapSet is missing clone_size for snap " << ghobj.hobj.snap << std::endl;
      return -EINVAL;
    }
  }
  if ((uint64_t)st.st_size == setsize && oi.size == setsize
       && (!is_snap || csi->second == setsize)) {
    cout << "Size of object is already " << setsize << std::endl;
    return 0;
  }
  cout << "Setting size to " << setsize << ", stat size " << st.st_size
       << ", obj info size " << oi.size;
  if (is_snap) {
    cout << ", " << (found_head ? "head" : "snapdir")
	 << " clone_size " << csi->second;
    csi->second = setsize;
  }
  cout << std::endl;
  if (!dry_run) {
    attr.clear();
    oi.size = setsize;
    ::encode(oi, attr);
    ObjectStore::Transaction t;
    t.setattr(coll, ghobj, OI_ATTR, attr);
    t.truncate(coll, ghobj, setsize);
    if (is_snap) {
      bufferlist snapattr;
      snapattr.clear();
      ::encode(ss, snapattr);
      t.setattr(coll, head, SS_ATTR, snapattr);
    }
    r = store->apply_transaction(&osr, std::move(t));
    if (r < 0) {
      cerr << "Error writing object info: " << make_pair(coll, ghobj) << ", "
         << cpp_strerror(r) << std::endl;
      return r;
    }
  }
  return 0;
}

int clear_snapset(ObjectStore *store, coll_t coll, ghobject_t &ghobj,
                  string arg, ObjectStore::Sequencer &osr)
{
  SnapSet ss;
  int ret = get_snapset(store, coll, ghobj, ss);
  if (ret < 0)
    return ret;

  // Use "head" to set head_exists incorrectly
  if (arg == "corrupt" || arg == "head")
    ss.head_exists = !ghobj.hobj.is_head();
  else if (ss.head_exists != ghobj.hobj.is_head()) {
    cerr << "Correcting head_exists, set to "
         << (ghobj.hobj.is_head() ? "true" : "false") << std::endl;
    ss.head_exists = ghobj.hobj.is_head();
  }
  // Use "corrupt" to clear entire SnapSet
  // Use "seq" to just corrupt SnapSet.seq
  if (arg == "corrupt" || arg == "seq")
    ss.seq = 0;
  // Use "snaps" to just clear SnapSet.snaps
  if (arg == "corrupt" || arg == "snaps")
    ss.snaps.clear();
  // By default just clear clone, clone_overlap and clone_size
  if (arg == "corrupt")
    arg = "";
  if (arg == "" || arg == "clones")
    ss.clones.clear();
  if (arg == "" || arg == "clone_overlap")
    ss.clone_overlap.clear();
  if (arg == "" || arg == "clone_size")
    ss.clone_size.clear();
  // Break all clone sizes by adding 1
  if (arg == "size") {
    for (map<snapid_t, uint64_t>::iterator i = ss.clone_size.begin();
         i != ss.clone_size.end(); ++i)
      ++(i->second);
  }

  if (!dry_run) {
    bufferlist bl;
    ::encode(ss, bl);
    ObjectStore::Transaction t;
    t.setattr(coll, ghobj, SS_ATTR, bl);
    int r = store->apply_transaction(&osr, std::move(t));
    if (r < 0) {
      cerr << "Error setting snapset on : " << make_pair(coll, ghobj) << ", "
	   << cpp_strerror(r) << std::endl;
      return r;
    }
  }
  return 0;
}

vector<snapid_t>::iterator find(vector<snapid_t> &v, snapid_t clid)
{
  return std::find(v.begin(), v.end(), clid);
}

map<snapid_t, interval_set<uint64_t> >::iterator
find(map<snapid_t, interval_set<uint64_t> > &m, snapid_t clid)
{
  return m.find(clid);
}

map<snapid_t, uint64_t>::iterator find(map<snapid_t, uint64_t> &m,
				       snapid_t clid)
{
  return m.find(clid);
}

template<class T>
int remove_from(T &mv, string name, snapid_t cloneid, bool force)
{
  typename T::iterator i = find(mv, cloneid);
  if (i != mv.end()) {
    mv.erase(i);
  } else {
    cerr << "Clone " << cloneid << " doesn't exist in " << name;
    if (force) {
      cerr << " (ignored)" << std::endl;
      return 0;
    }
    cerr << std::endl;
    return -EINVAL;
  }
  return 0;
}

int remove_clone(ObjectStore *store, coll_t coll, ghobject_t &ghobj, snapid_t cloneid, bool force,
		     ObjectStore::Sequencer &osr)
{
  // XXX: Don't allow this if in a cache tier or former cache tier
  // bool allow_incomplete_clones() const {
  // 	return cache_mode != CACHEMODE_NONE || has_flag(FLAG_INCOMPLETE_CLONES);

  SnapSet snapset;
  int ret = get_snapset(store, coll, ghobj, snapset);
  if (ret < 0)
    return ret;

  // Derived from trim_object()
  // ...from snapset
  vector<snapid_t>::iterator p;
  for (p = snapset.clones.begin(); p != snapset.clones.end(); ++p)
    if (*p == cloneid)
      break;
  if (p == snapset.clones.end()) {
    cerr << "Clone " << cloneid << " not present";
    return -ENOENT;
  }
  if (p != snapset.clones.begin()) {
    // not the oldest... merge overlap into next older clone
    vector<snapid_t>::iterator n = p - 1;
    hobject_t prev_coid = ghobj.hobj;
    prev_coid.snap = *n;
    //bool adjust_prev_bytes = is_present_clone(prev_coid);

    //if (adjust_prev_bytes)
    //  ctx->delta_stats.num_bytes -= snapset.get_clone_bytes(*n);

    snapset.clone_overlap[*n].intersection_of(
	snapset.clone_overlap[*p]);

    //if (adjust_prev_bytes)
    //  ctx->delta_stats.num_bytes += snapset.get_clone_bytes(*n);
  }

  ret = remove_from(snapset.clones, "clones", cloneid, force);
  if (ret) return ret;
  ret = remove_from(snapset.clone_overlap, "clone_overlap", cloneid, force);
  if (ret) return ret;
  ret = remove_from(snapset.clone_size, "clone_size", cloneid, force);
  if (ret) return ret;

  if (dry_run)
    return 0;

  bufferlist bl;
  ::encode(snapset, bl);
  ObjectStore::Transaction t;
  t.setattr(coll, ghobj, SS_ATTR, bl);
  int r = store->apply_transaction(&osr, std::move(t));
  if (r < 0) {
    cerr << "Error setting snapset on : " << make_pair(coll, ghobj) << ", "
	 << cpp_strerror(r) << std::endl;
    return r;
  }
  cout << "Removal of clone " << cloneid << " complete" << std::endl;
  cout << "Use pg repair after OSD restarted to correct stat information" << std::endl;
  return 0;
}

void usage(po::options_description &desc)
{
    cerr << std::endl;
    cerr << desc << std::endl;
    cerr << std::endl;
    cerr << "Positional syntax:" << std::endl;
    cerr << std::endl;
    cerr << "ceph-objectstore-tool ... <object> (get|set)-bytes [file]" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> set-(attr|omap) <key> [file]" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> (get|rm)-(attr|omap) <key>" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> get-omaphdr" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> set-omaphdr [file]" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> list-attrs" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> list-omap" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> remove" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> dump" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> set-size" << std::endl;
    cerr << "ceph-objectstore-tool ... <object> remove-clone-metadata <cloneid>" << std::endl;
    cerr << std::endl;
    cerr << "<object> can be a JSON object description as displayed" << std::endl;
    cerr << "by --op list." << std::endl;
    cerr << "<object> can be an object name which will be looked up in all" << std::endl;
    cerr << "the OSD's PGs." << std::endl;
    cerr << "<object> can be the empty string ('') which with a provided pgid " << std::endl;
    cerr << "specifies the pgmeta object" << std::endl;
    cerr << std::endl;
    cerr << "The optional [file] argument will read stdin or write stdout" << std::endl;
    cerr << "if not specified or if '-' specified." << std::endl;
}

bool ends_with(const string& check, const string& ending)
{
    return check.size() >= ending.size() && check.rfind(ending) == (check.size() - ending.size());
}

// Based on FileStore::dump_journal(), set-up enough to only dump
int mydump_journal(Formatter *f, string journalpath, bool m_journal_dio)
{
  int r;

  if (!journalpath.length())
    return -EINVAL;

  FileJournal *journal = new FileJournal(uuid_d(), NULL, NULL, journalpath.c_str(), m_journal_dio);
  r = journal->_fdump(*f, false);
  delete journal;
  return r;
}

int main(int argc, char **argv)
{
  string dpath, jpath, pgidstr, op, file, mountpoint, object, objcmd, arg1, arg2, type, format;
  spg_t pgid;
  unsigned epoch = 0;
  ghobject_t ghobj;
  bool human_readable;
  bool force;
  Formatter *formatter;
  bool head;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("type", po::value<string>(&type),
     "Arg is one of [filestore (default), memstore]")
    ("data-path", po::value<string>(&dpath),
     "path to object store, mandatory")
    ("journal-path", po::value<string>(&jpath),
     "path to journal, mandatory for filestore type")
    ("pgid", po::value<string>(&pgidstr),
     "PG id, mandatory for info, log, remove, export, rm-past-intervals, mark-complete")
    ("op", po::value<string>(&op),
     "Arg is one of [info, log, remove, mkfs, fsck, fuse, export, import, list, fix-lost, list-pgs, rm-past-intervals, dump-journal, dump-super, meta-list, "
	 "get-osdmap, set-osdmap, get-inc-osdmap, set-inc-osdmap, mark-complete]")
    ("epoch", po::value<unsigned>(&epoch),
     "epoch# for get-osdmap and get-inc-osdmap, the current epoch in use if not specified")
    ("file", po::value<string>(&file),
     "path of file to export, import, get-osdmap, set-osdmap, get-inc-osdmap or set-inc-osdmap")
    ("mountpoint", po::value<string>(&mountpoint),
     "fuse mountpoint")
    ("format", po::value<string>(&format)->default_value("json-pretty"),
     "Output format which may be json, json-pretty, xml, xml-pretty")
    ("debug", "Enable diagnostic output to stderr")
    ("force", "Ignore some types of errors and proceed with operation - USE WITH CAUTION: CORRUPTION POSSIBLE NOW OR IN THE FUTURE")
    ("skip-journal-replay", "Disable journal replay")
    ("skip-mount-omap", "Disable mounting of omap")
    ("head", "Find head/snapdir when searching for objects by name")
    ("dry-run", "Don't modify the objectstore")
    ;

  po::options_description positional("Positional options");
  positional.add_options()
    ("object", po::value<string>(&object), "'' for pgmeta_oid, object name or ghobject in json")
    ("objcmd", po::value<string>(&objcmd), "command [(get|set)-bytes, (get|set|rm)-(attr|omap), (get|set)-omaphdr, list-attrs, list-omap, remove]")
    ("arg1", po::value<string>(&arg1), "arg1 based on cmd")
    ("arg2", po::value<string>(&arg2), "arg2 based on cmd")
    ("test-align", po::value<uint64_t>(&testalign)->default_value(0), "hidden align option for testing")
    ;

  po::options_description all("All options");
  all.add(desc).add(positional);

  po::positional_options_description pd;
  pd.add("object", 1).add("objcmd", 1).add("arg1", 1).add("arg2", 1);

  vector<string> ceph_option_strings;
  po::variables_map vm;
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(all).allow_unregistered().positional(pd).run();
    po::store( parsed, vm);
    po::notify(vm);
    ceph_option_strings = po::collect_unrecognized(parsed.options,
						   po::include_positional);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    myexit(1);
  }

  if (vm.count("help")) {
    usage(desc);
    myexit(1);
  }

  if (!vm.count("debug")) {
    debug = false;
  } else {
    debug = true;
  }

  if (!vm.count("force")) {
    force = false;
  } else {
    force = true;
  }

  if (vm.count("dry-run"))
    dry_run = true;
  osflagbits_t flags = 0;
  if (dry_run || vm.count("skip-journal-replay"))
    flags |= SKIP_JOURNAL_REPLAY;
  if (vm.count("skip-mount-omap"))
    flags |= SKIP_MOUNT_OMAP;

  head = (vm.count("head") > 0);

  vector<const char *> ceph_options;
  env_to_vec(ceph_options);
  ceph_options.reserve(ceph_options.size() + ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/type", dpath.c_str());
  int fd = ::open(fn, O_RDONLY);
  if (fd >= 0) {
    bufferlist bl;
    bl.read_fd(fd, 64);
    if (bl.length()) {
      string dp_type = string(bl.c_str(), bl.length() - 1);  // drop \n
      if (vm.count("type") && dp_type != "" && type != dp_type)
        cerr << "WARNING: Ignoring type \"" << type << "\" - found data-path type \""
             << dp_type << "\"" << std::endl;
      type = dp_type;
      //cout << "object store type is " << type << std::endl;
    }
    ::close(fd);
  }
  if (!vm.count("type") && type == "") {
    type = "filestore";
  }
  if (!vm.count("data-path") &&
     !(op == "dump-journal" && type == "filestore")) {
    cerr << "Must provide --data-path" << std::endl;
    usage(desc);
    myexit(1);
  }
  if (type == "filestore" && !vm.count("journal-path")) {
    jpath = dpath + "/journal";
  }
  if (!vm.count("op") && !vm.count("object")) {
    cerr << "Must provide --op or object command..." << std::endl;
    usage(desc);
    myexit(1);
  }
  if (op != "list" &&
      vm.count("op") && vm.count("object")) {
    cerr << "Can't specify both --op and object command syntax" << std::endl;
    usage(desc);
    myexit(1);
  }
  if (op != "list" && vm.count("object") && !vm.count("objcmd")) {
    cerr << "Invalid syntax, missing command" << std::endl;
    usage(desc);
    myexit(1);
  }
  if (op == "fuse" && mountpoint.length() == 0) {
    cerr << "Missing fuse mountpoint" << std::endl;
    usage(desc);
    myexit(1);
  }
  outistty = isatty(STDOUT_FILENO);

  file_fd = fd_none;
  if ((op == "export" || op == "get-osdmap" || op == "get-inc-osdmap") && !dry_run) {
    if (!vm.count("file") || file == "-") {
      if (outistty) {
        cerr << "stdout is a tty and no --file filename specified" << std::endl;
        myexit(1);
      }
      file_fd = STDOUT_FILENO;
    } else {
      file_fd = open(file.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
    }
  } else if (op == "import" || op == "set-osdmap" || op == "set-inc-osdmap") {
    if (!vm.count("file") || file == "-") {
      if (isatty(STDIN_FILENO)) {
        cerr << "stdin is a tty and no --file filename specified" << std::endl;
        myexit(1);
      }
      file_fd = STDIN_FILENO;
    } else {
      file_fd = open(file.c_str(), O_RDONLY);
    }
  }

  ObjectStoreTool tool = ObjectStoreTool(file_fd, dry_run);

  if (vm.count("file") && file_fd == fd_none && !dry_run) {
    cerr << "--file option only applies to import, export, "
	 << "get-osdmap, set-osdmap, get-inc-osdmap or set-inc-osdmap" << std::endl;
    myexit(1);
  }

  if (file_fd != fd_none && file_fd < 0) {
    string err = string("file: ") + file;
    perror(err.c_str());
    myexit(1);
  }

  global_init(
    NULL, ceph_options, CEPH_ENTITY_TYPE_OSD,
    CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
    //CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_conf = g_ceph_context->_conf;
  if (debug) {
    g_conf->set_val_or_die("log_to_stderr", "true");
    g_conf->set_val_or_die("err_to_stderr", "true");
  }
  g_conf->apply_changes(NULL);

  // Special list handling.  Treating pretty_format as human readable,
  // with one object per line and not an enclosing array.
  human_readable = ends_with(format, "-pretty");
  if ((op == "list" || op == "meta-list") && human_readable) {
    // Remove -pretty from end of format which we know is there
    format = format.substr(0, format.size() - strlen("-pretty"));
  }

  formatter = Formatter::create(format);
  if (formatter == NULL) {
    cerr << "unrecognized format: " << format << std::endl;
    myexit(1);
  }

  // Special handling for filestore journal, so we can dump it without mounting
  if (op == "dump-journal" && type == "filestore") {
    int ret = mydump_journal(formatter, jpath, g_conf->journal_dio);
    if (ret < 0) {
      cerr << "journal-path: " << jpath << ": "
	   << cpp_strerror(ret) << std::endl;
      myexit(1);
    }
    formatter->flush(cout);
    myexit(0);
  }

  //Verify that data-path really exists
  struct stat st;
  if (::stat(dpath.c_str(), &st) == -1) {
    string err = string("data-path: ") + dpath;
    perror(err.c_str());
    myexit(1);
  }

  if (pgidstr.length() && !pgid.parse(pgidstr.c_str())) {
    cerr << "Invalid pgid '" << pgidstr << "' specified" << std::endl;
    myexit(1);
  }

  ObjectStore *fs = ObjectStore::create(g_ceph_context, type, dpath, jpath, flags);
  if (fs == NULL) {
    cerr << "Unable to create store of type " << type << std::endl;
    myexit(1);
  }

  if (op == "fsck") {
    int r = fs->fsck();
    if (r < 0) {
      cerr << "fsck failed: " << cpp_strerror(r) << std::endl;
      myexit(1);
    }
    if (r > 0) {
      cerr << "fsck found " << r << " errors" << std::endl;
      myexit(1);
    }
    cout << "fsck found no errors" << std::endl;
    exit(0);
  }
  if (op == "mkfs") {
    int r = fs->mkfs();
    if (r < 0) {
      cerr << "fsck failed: " << cpp_strerror(r) << std::endl;
      myexit(1);
    }
    myexit(0);
  }

  ObjectStore::Sequencer *osr = new ObjectStore::Sequencer(__func__);
  int ret = fs->mount();
  if (ret < 0) {
    if (ret == -EBUSY) {
      cerr << "OSD has the store locked" << std::endl;
    } else {
      cerr << "Mount failed with '" << cpp_strerror(ret) << "'" << std::endl;
    }
    myexit(1);
  }

  if (op == "fuse") {
#ifdef HAVE_LIBFUSE
    FuseStore fuse(fs, mountpoint);
    cout << "mounting fuse at " << mountpoint << " ..." << std::endl;
    int r = fuse.main();
    if (r < 0) {
      cerr << "failed to mount fuse: " << cpp_strerror(r) << std::endl;
      myexit(1);
    }
#else
    cerr << "fuse support not enabled" << std::endl;
#endif
    myexit(0);
  }

  vector<coll_t> ls;
  vector<coll_t>::iterator it;
  CompatSet supported;

#ifdef INTERNAL_TEST
  supported = get_test_compat_set();
#else
  supported = OSD::get_osd_compat_set();
#endif

  bufferlist bl;
  OSDSuperblock superblock;
  bufferlist::iterator p;
  ret = fs->read(coll_t::meta(), OSD_SUPERBLOCK_GOBJECT, 0, 0, bl);
  if (ret < 0) {
    cerr << "Failure to read OSD superblock: " << cpp_strerror(ret) << std::endl;
    goto out;
  }

  p = bl.begin();
  ::decode(superblock, p);

  if (debug) {
    cerr << "Cluster fsid=" << superblock.cluster_fsid << std::endl;
  }

  if (debug) {
    cerr << "Supported features: " << supported << std::endl;
    cerr << "On-disk features: " << superblock.compat_features << std::endl;
  }
  if (supported.compare(superblock.compat_features) == -1) {
    CompatSet unsupported = supported.unsupported(superblock.compat_features);
    cerr << "On-disk OSD incompatible features set "
      << unsupported << std::endl;
    ret = -EINVAL;
    goto out;
  }

  if (op != "list" && vm.count("object")) {
    // Special case: Create pgmeta_oid if empty string specified
    // This can't conflict with any actual object names.
    if (object == "") {
      ghobj = pgid.make_pgmeta_oid();
    } else {
    json_spirit::Value v;
    try {
      if (!json_spirit::read(object, v)) {
        // Special: Need head/snapdir so set even if user didn't specify
        if (vm.count("objcmd") && (objcmd == "remove-clone-metadata"))
	  head = true;
	lookup_ghobject lookup(object, head);
	if (action_on_all_objects(fs, lookup, debug)) {
	  throw std::runtime_error("Internal error");
	} else {
	  if (lookup.size() != 1) {
	    stringstream ss;
	    if (lookup.size() == 0)
	      ss << "No object id '" << object << "' found or invalid JSON specified";
	    else
	      ss << "Found " << lookup.size() << " objects with id '" << object
		 << "', please use a JSON spec from --op list instead";
	    throw std::runtime_error(ss.str());
	  }
	  pair<coll_t, ghobject_t> found = lookup.pop();
	  pgidstr = found.first.to_str();
	  pgid.parse(pgidstr.c_str());
	  ghobj = found.second;
	}
      } else {
	stringstream ss;
	if (pgidstr.length() == 0 && v.type() != json_spirit::array_type) {
	  ss << "Without --pgid the object '" << object
	     << "' must be a JSON array";
	  throw std::runtime_error(ss.str());
	}
	if (v.type() == json_spirit::array_type) {
	  json_spirit::Array array = v.get_array();
	  if (array.size() != 2) {
	    ss << "Object '" << object
	       << "' must be a JSON array with 2 elements";
	    throw std::runtime_error(ss.str());
	  }
	  vector<json_spirit::Value>::iterator i = array.begin();
	  //if (i == array.end() || i->type() != json_spirit::str_type) {
	  if (i->type() != json_spirit::str_type) {
	    ss << "Object '" << object
	       << "' must be a JSON array with the first element a string";
	    throw std::runtime_error(ss.str());
	  }
	  string object_pgidstr = i->get_str();
          if (object_pgidstr != "meta") {
	    spg_t object_pgid;
	    object_pgid.parse(object_pgidstr.c_str());
	    if (pgidstr.length() > 0) {
	      if (object_pgid != pgid) {
	        ss << "object '" << object
		   << "' has a pgid different from the --pgid="
		   << pgidstr << " option";
	        throw std::runtime_error(ss.str());
	      }
	    } else {
	      pgidstr = object_pgidstr;
	      pgid = object_pgid;
	    }
          } else {
            pgidstr = object_pgidstr;
          }
	  ++i;
	  v = *i;
	}
	try {
	  ghobj.decode(v);
	} catch (std::runtime_error& e) {
	  ss << "Decode object JSON error: " << e.what();
	  throw std::runtime_error(ss.str());
	}
        if (pgidstr != "meta" && (uint64_t)pgid.pgid.m_pool != (uint64_t)ghobj.hobj.pool) {
          cerr << "Object pool and pgid pool don't match" << std::endl;
          ret = 1;
          goto out;
        }
      }
    } catch (std::runtime_error& e) {
      cerr << e.what() << std::endl;
      ret = 1;
      goto out;
    }
    }
  }

  // The ops which require --pgid option are checked here and
  // mentioned in the usage for --pgid.
  if ((op == "info" || op == "log" || op == "remove" || op == "export"
      || op == "rm-past-intervals" || op == "mark-complete") &&
      pgidstr.length() == 0) {
    cerr << "Must provide pgid" << std::endl;
    usage(desc);
    ret = 1;
    goto out;
  }

  if (op == "import") {

    try {
      ret = tool.do_import(fs, superblock, force, pgidstr, *osr);
    }
    catch (const buffer::error &e) {
      cerr << "do_import threw exception error " << e.what() << std::endl;
      ret = -EFAULT;
    }
    if (ret == -EFAULT) {
      cerr << "Corrupt input for import" << std::endl;
    }
    if (ret == 0)
      cout << "Import successful" << std::endl;
    goto out;
  } else if (op == "dump-journal-mount") {
    // Undocumented feature to dump journal with mounted fs
    // This doesn't support the format option, but it uses the
    // ObjectStore::dump_journal() and mounts to get replay to run.
    ret = fs->dump_journal(cout);
    if (ret) {
      if (ret == -EOPNOTSUPP) {
        cerr << "Object store type \"" << type << "\" doesn't support journal dump" << std::endl;
      } else {
        cerr << "Journal dump failed with error " << cpp_strerror(ret) << std::endl;
      }
    }
    goto out;
  } else if (op == "get-osdmap") {
    bufferlist bl;
    OSDMap osdmap;
    if (epoch == 0) {
      epoch = superblock.current_epoch;
    }
    ret = get_osdmap(fs, epoch, osdmap, bl);
    if (ret) {
      cerr << "Failed to get osdmap#" << epoch << ": "
	   << cpp_strerror(ret) << std::endl;
      goto out;
    }
    ret = bl.write_fd(file_fd);
    if (ret) {
      cerr << "Failed to write to " << file << ": " << cpp_strerror(ret) << std::endl;
    } else {
      cout << "osdmap#" << epoch << " exported." << std::endl;
    }
    goto out;
  } else if (op == "set-osdmap") {
    bufferlist bl;
    ret = get_fd_data(file_fd, bl);
    if (ret < 0) {
      cerr << "Failed to read osdmap " << cpp_strerror(ret) << std::endl;
    } else {
      ret = set_osdmap(fs, epoch, bl, force, *osr);
    }
    goto out;
  } else if (op == "get-inc-osdmap") {
    bufferlist bl;
    if (epoch == 0) {
      epoch = superblock.current_epoch;
    }
    ret = get_inc_osdmap(fs, epoch, bl);
    if (ret < 0) {
      cerr << "Failed to get incremental osdmap# " << epoch << ": "
	   << cpp_strerror(ret) << std::endl;
      goto out;
    }
    ret = bl.write_fd(file_fd);
    if (ret) {
      cerr << "Failed to write to " << file << ": " << cpp_strerror(ret) << std::endl;
    } else {
      cout << "inc-osdmap#" << epoch << " exported." << std::endl;
    }
    goto out;
  } else if (op == "set-inc-osdmap") {
    bufferlist bl;
    ret = get_fd_data(file_fd, bl);
    if (ret < 0) {
      cerr << "Failed to read incremental osdmap  " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      ret = set_inc_osdmap(fs, epoch, bl, force, *osr);
    }
    goto out;
  }

  log_oid = OSD::make_pg_log_oid(pgid);
  biginfo_oid = OSD::make_pg_biginfo_oid(pgid);

  if (op == "remove") {
    ret = initiate_new_remove_pg(fs, pgid, *osr);
    if (ret < 0) {
      cerr << "PG '" << pgid << "' not found" << std::endl;
      goto out;
    }
    cout << "Remove successful" << std::endl;
    goto out;
  }

  if (op == "fix-lost") {
    boost::scoped_ptr<action_on_object_t> action;
    action.reset(new do_fix_lost(osr));
    if (pgidstr.length())
      ret = action_on_all_objects_in_exact_pg(fs, coll_t(pgid), *action, debug);
    else
      ret = action_on_all_objects(fs, *action, debug);
    goto out;
  }

  if (op == "list") {
    ret = do_list(fs, pgidstr, object, formatter, debug, human_readable, head);
    if (ret < 0) {
      cerr << "do_list failed: " << cpp_strerror(ret) << std::endl;
    }
    goto out;
  }

  if (op == "dump-super") {
    formatter->open_object_section("superblock");
    superblock.dump(formatter);
    formatter->close_section();
    formatter->flush(cout);
    cout << std::endl;
    goto out;
  }

  if (op == "meta-list") {
    ret = do_meta(fs, object, formatter, debug, human_readable);
    if (ret < 0) {
      cerr << "do_meta failed: " << cpp_strerror(ret) << std::endl;
    }
    goto out;
  }

  ret = fs->list_collections(ls);
  if (ret < 0) {
    cerr << "failed to list pgs: " << cpp_strerror(ret) << std::endl;
    goto out;
  }

  if (debug && op == "list-pgs")
    cout << "Performing list-pgs operation" << std::endl;

  // Find pg
  for (it = ls.begin(); it != ls.end(); ++it) {
    spg_t tmppgid;

    if (pgidstr == "meta") {
      if (it->to_str() == "meta")
        break;
      else
        continue;
    }

    if (!it->is_pg(&tmppgid)) {
      continue;
    }

    if (it->is_temp(&tmppgid)) {
      continue;
    }

    if (op != "list-pgs" && tmppgid != pgid) {
      continue;
    }

    if (op != "list-pgs") {
      //Found!
      break;
    }

    cout << tmppgid << std::endl;
  }

  if (op == "list-pgs") {
    ret = 0;
    goto out;
  }

  // If not an object command nor any of the ops handled below, then output this usage
  // before complaining about a bad pgid
  if (!vm.count("objcmd") && op != "export" && op != "info" && op != "log" && op != "rm-past-intervals" && op != "mark-complete") {
    cerr << "Must provide --op (info, log, remove, mkfs, fsck, export, import, list, fix-lost, list-pgs, rm-past-intervals, dump-journal, dump-super, meta-list, "
      "get-osdmap, set-osdmap, get-inc-osdmap, set-inc-osdmap, mark-complete)"
	 << std::endl;
    usage(desc);
    ret = 1;
    goto out;
  }
  epoch_t map_epoch;
// The following code for export, info, log require omap or !skip-mount-omap
  if (it != ls.end()) {

    coll_t coll = *it;

    if (vm.count("objcmd")) {
      ret = 0;
      if (objcmd == "remove") {
        ret = do_remove_object(fs, coll, ghobj, *osr);
        goto out;
      } else if (objcmd == "list-attrs") {
        ret = do_list_attrs(fs, coll, ghobj);
        goto out;
      } else if (objcmd == "list-omap") {
        ret = do_list_omap(fs, coll, ghobj);
        goto out;
      } else if (objcmd == "get-bytes" || objcmd == "set-bytes") {
        if (objcmd == "get-bytes") {
          int fd;
          if (vm.count("arg1") == 0 || arg1 == "-") {
            fd = STDOUT_FILENO;
	  } else {
            fd = open(arg1.c_str(), O_WRONLY|O_TRUNC|O_CREAT|O_EXCL|O_LARGEFILE, 0666);
            if (fd == -1) {
              cerr << "open " << arg1 << " " << cpp_strerror(errno) << std::endl;
              ret = 1;
              goto out;
            }
          }
          ret = do_get_bytes(fs, coll, ghobj, fd);
          if (fd != STDOUT_FILENO)
            close(fd);
        } else {
          int fd;
          if (vm.count("arg1") == 0 || arg1 == "-") {
            // Since read_fd() doesn't handle ^D from a tty stdin, don't allow it.
            if (isatty(STDIN_FILENO)) {
                cerr << "stdin is a tty and no file specified" << std::endl;
                ret = 1;
                goto out;
            }
            fd = STDIN_FILENO;
	  } else {
            fd = open(arg1.c_str(), O_RDONLY|O_LARGEFILE, 0666);
            if (fd == -1) {
              cerr << "open " << arg1 << " " << cpp_strerror(errno) << std::endl;
              ret = 1;
              goto out;
            }
          }
          ret = do_set_bytes(fs, coll, ghobj, fd, *osr);
          if (fd != STDIN_FILENO)
            close(fd);
        }
        goto out;
      } else if (objcmd == "get-attr") {
	if (vm.count("arg1") == 0) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	ret = do_get_attr(fs, coll, ghobj, arg1);
        goto out;
      } else if (objcmd == "set-attr") {
	if (vm.count("arg1") == 0) {
	  usage(desc);
          ret = 1;
        }

	int fd;
	if (vm.count("arg2") == 0 || arg2 == "-") {
          // Since read_fd() doesn't handle ^D from a tty stdin, don't allow it.
          if (isatty(STDIN_FILENO)) {
            cerr << "stdin is a tty and no file specified" << std::endl;
            ret = 1;
            goto out;
          }
	  fd = STDIN_FILENO;
	} else {
	  fd = open(arg2.c_str(), O_RDONLY|O_LARGEFILE, 0666);
	  if (fd == -1) {
	    cerr << "open " << arg2 << " " << cpp_strerror(errno) << std::endl;
	    ret = 1;
	    goto out;
	  }
	}
	ret = do_set_attr(fs, coll, ghobj, arg1, fd, *osr);
	if (fd != STDIN_FILENO)
	  close(fd);
        goto out;
      } else if (objcmd == "rm-attr") {
	if (vm.count("arg1") == 0) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	ret = do_rm_attr(fs, coll, ghobj, arg1, *osr);
        goto out;
      } else if (objcmd == "get-omap") {
	if (vm.count("arg1") == 0) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	ret = do_get_omap(fs, coll, ghobj, arg1);
        goto out;
      } else if (objcmd == "set-omap") {
	if (vm.count("arg1") == 0) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	int fd;
	if (vm.count("arg2") == 0 || arg2 == "-") {
          // Since read_fd() doesn't handle ^D from a tty stdin, don't allow it.
          if (isatty(STDIN_FILENO)) {
            cerr << "stdin is a tty and no file specified" << std::endl;
            ret = 1;
            goto out;
          }
	  fd = STDIN_FILENO;
	} else {
	  fd = open(arg2.c_str(), O_RDONLY|O_LARGEFILE, 0666);
	  if (fd == -1) {
	    cerr << "open " << arg2 << " " << cpp_strerror(errno) << std::endl;
	    ret = 1;
	    goto out;
	  }
	}
	ret = do_set_omap(fs, coll, ghobj, arg1, fd, *osr);
	if (fd != STDIN_FILENO)
	  close(fd);
        goto out;
      } else if (objcmd == "rm-omap") {
	if (vm.count("arg1") == 0) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	ret = do_rm_omap(fs, coll, ghobj, arg1, *osr);
        goto out;
      } else if (objcmd == "get-omaphdr") {
	if (vm.count("arg1")) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	ret = do_get_omaphdr(fs, coll, ghobj);
        goto out;
      } else if (objcmd == "set-omaphdr") {
        // Extra arg
	if (vm.count("arg2")) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	int fd;
	if (vm.count("arg1") == 0 || arg1 == "-") {
          // Since read_fd() doesn't handle ^D from a tty stdin, don't allow it.
          if (isatty(STDIN_FILENO)) {
            cerr << "stdin is a tty and no file specified" << std::endl;
            ret = 1;
            goto out;
          }
	  fd = STDIN_FILENO;
	} else {
	  fd = open(arg1.c_str(), O_RDONLY|O_LARGEFILE, 0666);
	  if (fd == -1) {
	    cerr << "open " << arg1 << " " << cpp_strerror(errno) << std::endl;
	    ret = 1;
	    goto out;
	  }
	}
	ret = do_set_omaphdr(fs, coll, ghobj, fd, *osr);
	if (fd != STDIN_FILENO)
	  close(fd);
        goto out;
      } else if (objcmd == "dump") {
	// There should not be any other arguments
	if (vm.count("arg1") || vm.count("arg2")) {
	  usage(desc);
	  ret = 1;
	  goto out;
	}
	ret = print_obj_info(fs, coll, ghobj, formatter);
	goto out;
      } else if (objcmd == "set-size") {
        // Extra arg
	if (vm.count("arg1") == 0 || vm.count("arg2")) {
	  usage(desc);
          ret = 1;
          goto out;
        }
        if (arg1.length() == 0 || !isdigit(arg1.c_str()[0])) {
	  cerr << "Invalid size '" << arg1 << "' specified" << std::endl;
	  ret = 1;
	  goto out;
	}
	uint64_t size = atoll(arg1.c_str());
	ret = set_size(fs, coll, ghobj, size, formatter, *osr);
	goto out;
      } else if (objcmd == "clear-snapset") {
        // UNDOCUMENTED: For testing zap SnapSet
        // IGNORE extra args since not in usage anyway
	if (!ghobj.hobj.has_snapset()) {
	  cerr << "'" << objcmd << "' requires a head or snapdir object" << std::endl;
	  ret = 1;
	  goto out;
	}
        ret = clear_snapset(fs, coll, ghobj, arg1, *osr);
        goto out;
      } else if (objcmd == "remove-clone-metadata") {
        // Extra arg
	if (vm.count("arg1") == 0 || vm.count("arg2")) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	if (!ghobj.hobj.has_snapset()) {
	  cerr << "'" << objcmd << "' requires a head or snapdir object" << std::endl;
	  ret = 1;
	  goto out;
	}
        if (arg1.length() == 0 || !isdigit(arg1.c_str()[0])) {
	  cerr << "Invalid cloneid '" << arg1 << "' specified" << std::endl;
	  ret = 1;
	  goto out;
	}
        snapid_t cloneid = atoi(arg1.c_str());
	ret = remove_clone(fs, coll, ghobj, cloneid, force, *osr);
	goto out;
      }
      cerr << "Unknown object command '" << objcmd << "'" << std::endl;
      usage(desc);
      ret = 1;
      goto out;
    }

    bufferlist bl;
    map_epoch = 0;
    ret = PG::peek_map_epoch(fs, pgid, &map_epoch, &bl);
    if (ret < 0)
      cerr << "peek_map_epoch reports error" << std::endl;
    if (debug)
      cerr << "map_epoch " << map_epoch << std::endl;

    pg_info_t info(pgid);
    map<epoch_t,pg_interval_t> past_intervals;
    __u8 struct_ver;
    ret = PG::read_info(fs, pgid, coll, bl, info, past_intervals,
		      struct_ver);
    if (ret < 0) {
      cerr << "read_info error " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    if (struct_ver < PG::compat_struct_v) {
      cerr << "PG is too old to upgrade, use older Ceph version" << std::endl;
      ret = -EFAULT;
      goto out;
    }
    if (debug)
      cerr << "struct_v " << (int)struct_ver << std::endl;

    if (op == "export") {
      ret = tool.do_export(fs, coll, pgid, info, map_epoch, struct_ver, superblock, past_intervals);
      if (ret == 0)
        cerr << "Export successful" << std::endl;
    } else if (op == "info") {
      formatter->open_object_section("info");
      info.dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    } else if (op == "log") {
      PGLog::IndexedLog log;
      pg_missing_t missing;
      map<eversion_t, hobject_t> divergent_priors;
      ret = get_log(fs, struct_ver, coll, pgid, info, log, missing,
                    divergent_priors);
      if (ret < 0)
          goto out;

      dump_log(formatter, cout, log, missing, divergent_priors);
    } else if (op == "rm-past-intervals") {
      ObjectStore::Transaction tran;
      ObjectStore::Transaction *t = &tran;

      if (struct_ver != PG::cur_struct_v) {
        cerr << "Can't remove past-intervals, version mismatch " << (int)struct_ver
          << " (pg)  != " << (int)PG::cur_struct_v << " (tool)"
          << std::endl;
        ret = -EFAULT;
        goto out;
      }

      cout << "Remove past-intervals " << past_intervals << std::endl;

      past_intervals.clear();
      if (dry_run) {
        ret = 0;
        goto out;
      }
      ret = write_info(*t, map_epoch, info, past_intervals);

      if (ret == 0) {
        fs->apply_transaction(osr, std::move(*t));
        cout << "Removal succeeded" << std::endl;
      }
    } else if (op == "mark-complete") {
      ObjectStore::Transaction tran;
      ObjectStore::Transaction *t = &tran;

      if (struct_ver != PG::cur_struct_v) {
	cerr << "Can't mark-complete, version mismatch " << (int)struct_ver
	     << " (pg)  != " << (int)PG::cur_struct_v << " (tool)"
	     << std::endl;
	ret = 1;
	goto out;
      }

      cout << "Marking complete " << std::endl;

      info.last_update = eversion_t(superblock.current_epoch, info.last_update.version + 1);
      info.last_backfill = hobject_t::get_max();
      info.last_epoch_started = superblock.current_epoch;
      info.history.last_epoch_started = superblock.current_epoch;
      info.history.last_epoch_clean = superblock.current_epoch;
      past_intervals.clear();

      if (!dry_run) {
	ret = write_info(*t, map_epoch, info, past_intervals);
	if (ret != 0)
	  goto out;
	fs->apply_transaction(osr, std::move(*t));
      }
      cout << "Marking complete succeeded" << std::endl;
    } else {
      assert(!"Should have already checked for valid --op");
    }
  } else {
    cerr << "PG '" << pgid << "' not found" << std::endl;
    ret = -ENOENT;
  }

out:
  int r = fs->umount();
  delete osr;
  if (r < 0) {
    cerr << "umount failed: " << cpp_strerror(r) << std::endl;
    // If no previous error, then use umount() error
    if (ret == 0)
      ret = r;
  }

  if (dry_run) {
    // Export output can go to stdout, so put this message on stderr
    if (op == "export")
      cerr << "dry-run: Nothing changed" << std::endl;
    else
      cout << "dry-run: Nothing changed" << std::endl;
  }

  if (ret < 0)
    ret = 1;
  myexit(ret);
}
