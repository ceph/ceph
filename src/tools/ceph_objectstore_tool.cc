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
#include "os/FileStore.h"
#include "os/FileJournal.h"

#include "osd/PGLog.h"
#include "osd/OSD.h"
#include "osd/PG.h"

#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_reader.h"

#include "include/rados/librados.hpp"

namespace po = boost::program_options;
using namespace std;

static coll_t META_COLL("meta");

enum {
    TYPE_NONE = 0,
    TYPE_PG_BEGIN,
    TYPE_PG_END,
    TYPE_OBJECT_BEGIN,
    TYPE_OBJECT_END,
    TYPE_DATA,
    TYPE_ATTRS,
    TYPE_OMAP_HDR,
    TYPE_OMAP,
    TYPE_PG_METADATA,
    END_OF_TYPES,	//Keep at the end
};

//#define INTERNAL_TEST
//#define INTERNAL_TEST2
//#define INTERNAL_TEST3

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

typedef uint8_t sectiontype_t;
typedef uint32_t mymagic_t;
typedef int64_t mysize_t;
const ssize_t max_read = 1024 * 1024;
const uint16_t shortmagic = 0xffce;	//goes into stream as "ceff"
//endmagic goes into stream as "ceff ffec"
const mymagic_t endmagic = (0xecff << 16) | shortmagic;
const int fd_none = INT_MIN;
bool outistty;
bool dry_run = false;

//The first FIXED_LENGTH bytes are a fixed
//portion of the export output.  This includes the overall
//version number, and size of header and footer.
//THIS STRUCTURE CAN ONLY BE APPENDED TO.  If it needs to expand,
//the version can be bumped and then anything
//can be added to the export format.
struct super_header {
  static const uint32_t super_magic = (shortmagic << 16) | shortmagic;
  // ver = 1, Initial version
  // ver = 2, Add OSDSuperblock to pg_begin
  static const uint32_t super_ver = 2;
  static const uint32_t FIXED_LENGTH = 16;
  uint32_t magic;
  uint32_t version;
  uint32_t header_size;
  uint32_t footer_size;

  super_header() : magic(0), version(0), header_size(0), footer_size(0) { }
  int read_super();

  void encode(bufferlist& bl) const {
    ::encode(magic, bl);
    ::encode(version, bl);
    ::encode(header_size, bl);
    ::encode(footer_size, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(magic, bl);
    ::decode(version, bl);
    ::decode(header_size, bl);
    ::decode(footer_size, bl);
  }
};

struct header {
  sectiontype_t type;
  mysize_t size;
  header(sectiontype_t type, mysize_t size) :
    type(type), size(size) { }
  header(): type(0), size(0) { }

  int get_header();

  void encode(bufferlist& bl) const {
    uint32_t debug_type = (type << 24) | (type << 16) | shortmagic;
    ENCODE_START(1, 1, bl);
    ::encode(debug_type, bl);
    ::encode(size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    uint32_t debug_type;
    DECODE_START(1, bl);
    ::decode(debug_type, bl);
    type = debug_type >> 24;
    ::decode(size, bl);
    DECODE_FINISH(bl);
  }
};

struct footer {
  mymagic_t magic;
  footer() : magic(endmagic) { }

  int get_footer();

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(magic, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(magic, bl);
    DECODE_FINISH(bl);
  }
};

struct pg_begin {
  spg_t pgid;
  OSDSuperblock superblock;

  pg_begin(spg_t pg, const OSDSuperblock& sb):
    pgid(pg), superblock(sb) { }
  pg_begin() { }

  void encode(bufferlist& bl) const {
    // If superblock doesn't include CEPH_FS_FEATURE_INCOMPAT_SHARDS then
    // shard will be NO_SHARD for a replicated pool.  This means
    // that we allow the decode by struct_v 2.
    ENCODE_START(3, 2, bl);
    ::encode(pgid.pgid, bl);
    ::encode(superblock, bl);
    ::encode(pgid.shard, bl);
    ENCODE_FINISH(bl);
  }
  // NOTE: New super_ver prevents decode from ver 1
  void decode(bufferlist::iterator& bl) {
    DECODE_START(3, bl);
    ::decode(pgid.pgid, bl);
    if (struct_v > 1) {
      ::decode(superblock, bl);
    }
    if (struct_v > 2) {
      ::decode(pgid.shard, bl);
    } else {
      pgid.shard = shard_id_t::NO_SHARD;
    }
    DECODE_FINISH(bl);
  }
};

struct object_begin {
  ghobject_t hoid;

  // Duplicate what is in the OI_ATTR so we have it at the start
  // of object processing.
  object_info_t oi;

  object_begin(const ghobject_t &hoid): hoid(hoid) { }
  object_begin() { }

  // If superblock doesn't include CEPH_FS_FEATURE_INCOMPAT_SHARDS then
  // generation will be NO_GEN, shard_id will be NO_SHARD for a replicated
  // pool.  This means we will allow the decode by struct_v 1.
  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    ::encode(hoid.hobj, bl);
    ::encode(hoid.generation, bl);
    ::encode(hoid.shard_id, bl);
    ::encode(oi, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(3, bl);
    ::decode(hoid.hobj, bl);
    if (struct_v > 1) {
      ::decode(hoid.generation, bl);
      ::decode(hoid.shard_id, bl);
    } else {
      hoid.generation = ghobject_t::NO_GEN;
      hoid.shard_id = shard_id_t::NO_SHARD;
    }
    if (struct_v > 2) {
      ::decode(oi, bl);
    }
    DECODE_FINISH(bl);
  }
};

struct data_section {
  uint64_t offset;
  uint64_t len;
  bufferlist databl;
  data_section(uint64_t offset, uint64_t len, bufferlist bl):
     offset(offset), len(len), databl(bl) { }
  data_section(): offset(0), len(0) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(offset, bl);
    ::encode(len, bl);
    ::encode(databl, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(offset, bl);
    ::decode(len, bl);
    ::decode(databl, bl);
    DECODE_FINISH(bl);
  }
};

struct attr_section {
  map<string,bufferptr> data;
  attr_section(const map<string,bufferptr> &data) : data(data) { }
  attr_section() { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }
};

struct omap_hdr_section {
  bufferlist hdr;
  omap_hdr_section(bufferlist hdr) : hdr(hdr) { }
  omap_hdr_section() { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(hdr, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(hdr, bl);
    DECODE_FINISH(bl);
  }
};

struct omap_section {
  map<string, bufferlist> omap;
  omap_section(const map<string, bufferlist> &omap) :
    omap(omap) { }
  omap_section() { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(omap, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(omap, bl);
    DECODE_FINISH(bl);
  }
};

struct metadata_section {
  // struct_ver is the on-disk version of original pg
  __u8 struct_ver;  // for reference
  epoch_t map_epoch;
  pg_info_t info;
  pg_log_t log;
  map<epoch_t,pg_interval_t> past_intervals;
  OSDMap osdmap;
  bufferlist osdmap_bl;  // Used in lieu of encoding osdmap due to crc checking
  map<eversion_t, hobject_t> divergent_priors;

  metadata_section(__u8 struct_ver, epoch_t map_epoch, const pg_info_t &info,
		   const pg_log_t &log, map<epoch_t,pg_interval_t> &past_intervals,
		   map<eversion_t, hobject_t> &divergent_priors)
    : struct_ver(struct_ver),
      map_epoch(map_epoch),
      info(info),
      log(log),
      past_intervals(past_intervals),
      divergent_priors(divergent_priors) { }
  metadata_section()
    : struct_ver(0),
      map_epoch(0) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    ::encode(struct_ver, bl);
    ::encode(map_epoch, bl);
    ::encode(info, bl);
    ::encode(log, bl);
    ::encode(past_intervals, bl);
    // Equivalent to osdmap.encode(bl, features); but
    // preserving exact layout for CRC checking.
    bl.append(osdmap_bl);
    ::encode(divergent_priors, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(4, bl);
    ::decode(struct_ver, bl);
    ::decode(map_epoch, bl);
    ::decode(info, bl);
    ::decode(log, bl);
    if (struct_v > 1) {
      ::decode(past_intervals, bl);
    } else {
      cout << "NOTICE: Older export without past_intervals" << std::endl;
    }
    if (struct_v > 2) {
      osdmap.decode(bl);
    } else {
      cout << "WARNING: Older export without OSDMap information" << std::endl;
    }
    if (struct_v > 3) {
      ::decode(divergent_priors, bl);
    }
    DECODE_FINISH(bl);
  }
};

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
    int r = store->collection_list_partial(
				       coll,
				       next,
				       LIST_AT_A_TIME,
				       LIST_AT_A_TIME,
				       0,
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
      if (coll != META_COLL) {
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
    snapid_t snap;
    if (!i->is_pg(cand_pgid, snap))
      continue;
    if (snap != CEPH_NOSNAP)
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
  store->sync_and_flush();
  return r;
}

int action_on_all_objects_in_exact_pg(ObjectStore *store, coll_t coll, action_on_object_t &action, bool debug)
{
  int r = _action_on_all_objects_in_pg(store, coll, action, debug);
  store->sync_and_flush();
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
    spg_t pgid;
    snapid_t snap;
    if (i->is_pg(pgid, snap)) {
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
  store->sync_and_flush();
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
      snapid_t snap;
      spg_t pgid;
      bool is_pg = i->first.is_pg(pgid, snap);
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

hobject_t infos_oid = OSD::make_infos_oid();
ghobject_t log_oid;
hobject_t biginfo_oid;

int file_fd = fd_none;
bool debug = false;
super_header sh;
uint64_t testalign;

template <typename T>
int write_section(sectiontype_t type, const T& obj, int fd) {
  if (dry_run)
    return 0;
  bufferlist blhdr, bl, blftr;
  obj.encode(bl);
  header hdr(type, bl.length());
  hdr.encode(blhdr);
  footer ft;
  ft.encode(blftr);

  int ret = blhdr.write_fd(fd);
  if (ret) return ret;
  ret = bl.write_fd(fd);
  if (ret) return ret;
  ret = blftr.write_fd(fd);
  return ret;
}

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

int write_simple(sectiontype_t type, int fd)
{
  if (dry_run)
    return 0;
  bufferlist hbl;

  header hdr(type, 0);
  hdr.encode(hbl);
  return hbl.write_fd(fd);
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

static void invalid_filestore_path(string &path)
{
  cerr << "Invalid filestore path specified: " << path << "\n";
  myexit(1);
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
		    struct_ver >= 8 ? coll : META_COLL,
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

//Based on RemoveWQ::_process()
void remove_coll(ObjectStore *store, const coll_t &coll)
{
  spg_t pg;
  coll.is_pg_prefix(pg);
  OSDriver driver(
    store,
    coll_t(),
    OSD::make_snapmapper_oid());
  SnapMapper mapper(&driver, 0, 0, 0, pg.shard);

  ghobject_t next;
  int r = 0;
  int64_t num = 0;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  cout << "remove_coll " << coll << std::endl;
  while (!next.is_max()) {
    vector<ghobject_t> objects;
    r = store->collection_list_partial(coll, next, 200, 300, 0,
      &objects, &next);
    if (r < 0)
      goto out;
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i, ++num) {

      OSDriver::OSTransaction _t(driver.get_transaction(t));
      cout << "remove " << *i << std::endl;
      int r = mapper.remove_oid(i->hobj, &_t);
      if (r != 0 && r != -ENOENT) {
        assert(0);
      }

      t->remove(coll, *i);
      if (num >= 30) {
        store->apply_transaction(*t);
        delete t;
        t = new ObjectStore::Transaction;
        num = 0;
      }
    }
  }
  t->remove_collection(coll);
  store->apply_transaction(*t);
out:
  delete t;
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

    if (it->is_temp(pgid)) {
      cout << "finish_remove_pgs " << *it << " clearing temp" << std::endl;
      OSD::recursive_remove_collection(store, *it);
      continue;
    }

    uint64_t seq;
    snapid_t snap;
    if (it->is_removal(&seq, &pgid) || (it->is_pg(pgid, snap) &&
	PG::_has_removal_flag(store, pgid))) {
      cout << "finish_remove_pgs removing " << *it
	   << " pgid is " << pgid << std::endl;
      remove_coll(store, *it);
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
  epoch_t pg_epoch = 0;
  int r = PG::peek_map_epoch(fs, pgid, &pg_epoch, &bl);
  if (r < 0)
    cerr << __func__ << " warning: peek_map_epoch fails" << std::endl;

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
    cout << "remove " << META_COLL << " " << log_oid.hobj.oid << std::endl;
    t->remove(META_COLL, log_oid);
    cout << "remove " << META_COLL << " " << biginfo_oid.oid << std::endl;
    t->remove(META_COLL, biginfo_oid);
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

int initiate_new_remove_pg(ObjectStore *store, spg_t r_pgid)
{
  if (!dry_run)
    finish_remove_pgs(store);
  if (!store->collection_exists(coll_t(r_pgid)))
    return -ENOENT;

  cout << " marking collection for removal" << std::endl;
  if (dry_run)
    return 0;
  ObjectStore::Transaction *rmt = new ObjectStore::Transaction;
  int r = mark_pg_for_removal(store, r_pgid, rmt);
  if (r < 0) {
    delete rmt;
    return r;
  }
  store->apply_transaction(*rmt);
  finish_remove_pgs(store);
  return r;
}

int header::get_header()
{
  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, sh.header_size);
  if ((size_t)bytes != sh.header_size) {
    cerr << "Unexpected EOF" << std::endl;
    return -EFAULT;
  }

  decode(ebliter);

  return 0;
}

int footer::get_footer()
{
  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, sh.footer_size);
  if ((size_t)bytes != sh.footer_size) {
    cerr << "Unexpected EOF" << std::endl;
    return -EFAULT;
  }

  decode(ebliter);

  if (magic != endmagic) {
    cerr << "Bad footer magic" << std::endl;
    return -EFAULT;
  }

  return 0;
}

int write_info(ObjectStore::Transaction &t, epoch_t epoch, pg_info_t &info,
    map<epoch_t,pg_interval_t> &past_intervals)
{
  //Empty for this
  coll_t coll(info.pgid);
  ghobject_t pgmeta_oid(info.pgid.make_pgmeta_oid());
  int ret = PG::_write_info(t, epoch,
    info, coll,
    past_intervals,
    pgmeta_oid,
    true);
  if (ret) cerr << "Failed to write info" << std::endl;
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
  PGLog::write_log(t, log, coll, info.pgid.make_pgmeta_oid(), divergent_priors);
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

int export_file(ObjectStore *store, coll_t cid, ghobject_t &obj)
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

int export_files(ObjectStore *store, coll_t coll)
{
  ghobject_t next;

  while (!next.is_max()) {
    vector<ghobject_t> objects;
    int r = store->collection_list_partial(coll, next, 200, 300, 0,
      &objects, &next);
    if (r < 0)
      return r;
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
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

int set_inc_osdmap(ObjectStore *store, epoch_t e, bufferlist& bl, bool force) {
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
  if (!store->exists(META_COLL, inc_oid)) {
    cerr << "inc-osdmap (" << inc_oid << ") does not exist." << std::endl;
    if (!force) {
      return -ENOENT;
    }
    cout << "Creating a new epoch." << std::endl;
  }
  if (dry_run)
    return 0;
  ObjectStore::Transaction t;
  t.write(META_COLL, inc_oid, 0, bl.length(), bl);
  t.truncate(META_COLL, inc_oid, bl.length());
  int ret = store->apply_transaction(t);
  if (ret) {
    cerr << "Failed to set inc-osdmap (" << inc_oid << "): " << ret << std::endl;
  } else {
    cout << "Wrote inc-osdmap." << inc.epoch << std::endl;
  }
  return ret;
}

int get_inc_osdmap(ObjectStore *store, epoch_t e, bufferlist& bl)
{
  if (store->read(META_COLL,
		  OSD::get_inc_osdmap_pobject_name(e),
		  0, 0, bl) < 0) {
    return -ENOENT;
  }
  return 0;
}

int set_osdmap(ObjectStore *store, epoch_t e, bufferlist& bl, bool force) {
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
  if (!store->exists(META_COLL, full_oid)) {
    cerr << "osdmap (" << full_oid << ") does not exist." << std::endl;
    if (!force) {
      return -ENOENT;
    }
    cout << "Creating a new epoch." << std::endl;
  }
  if (dry_run)
    return 0;
  ObjectStore::Transaction t;
  t.write(META_COLL, full_oid, 0, bl.length(), bl);
  t.truncate(META_COLL, full_oid, bl.length());
  int ret = store->apply_transaction(t);
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
      META_COLL, OSD::get_osdmap_pobject_name(e), 0, 0, bl) >= 0;
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

//Write super_header with its fixed 16 byte length
void write_super()
{
  if (dry_run)
    return;
  bufferlist superbl;
  super_header sh;
  footer ft;

  header hdr(TYPE_NONE, 0);
  hdr.encode(superbl);

  sh.magic = super_header::super_magic;
  sh.version = super_header::super_ver;
  sh.header_size = superbl.length();
  superbl.clear();
  ft.encode(superbl);
  sh.footer_size = superbl.length();
  superbl.clear();

  sh.encode(superbl);
  assert(super_header::FIXED_LENGTH == superbl.length());
  superbl.write_fd(file_fd);
}

int do_export(ObjectStore *fs, coll_t coll, spg_t pgid, pg_info_t &info,
    epoch_t map_epoch, __u8 struct_ver, const OSDSuperblock& superblock,
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

int super_header::read_super()
{
  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, super_header::FIXED_LENGTH);
  if ((size_t)bytes != super_header::FIXED_LENGTH) {
    cerr << "Unexpected EOF" << std::endl;
    return -EFAULT;
  }

  decode(ebliter);

  return 0;
}

int read_section(int fd, sectiontype_t *type, bufferlist *bl)
{
  header hdr;
  ssize_t bytes;

  int ret = hdr.get_header();
  if (ret)
    return ret;

  *type = hdr.type;

  bl->clear();
  bytes = bl->read_fd(fd, hdr.size);
  if (bytes != hdr.size) {
    cerr << "Unexpected EOF" << std::endl;
    return -EFAULT;
  }

  if (hdr.size > 0) {
    footer ft;
    ret = ft.get_footer();
    if (ret)
      return ret;
  }

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
    map<string,bufferptr>::iterator mi = as.data.find(OI_ATTR);
    if (mi != as.data.end()) {
      bufferlist attr_bl;
      attr_bl.push_back(mi->second);
      object_info_t oi(attr_bl);

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

int skip_object(bufferlist &bl)
{
  bufferlist::iterator ebliter = bl.begin();
  bufferlist ebl;
  bool done = false;
  while(!done) {
    sectiontype_t type;
    int ret = read_section(file_fd, &type, &ebl);
    if (ret)
      return ret;

    ebliter = ebl.begin();
    if (type >= END_OF_TYPES) {
      cout << "Skipping unknown object section type" << std::endl;
      continue;
    }
    switch(type) {
    case TYPE_DATA:
    case TYPE_ATTRS:
    case TYPE_OMAP_HDR:
    case TYPE_OMAP:
#ifdef DIAGNOSTIC
      cerr << "Skip type " << (int)type << std::endl;
#endif
      break;
    case TYPE_OBJECT_END:
      done = true;
      break;
    default:
      return -EFAULT;
    }
  }
  return 0;
}

int get_object_rados(librados::IoCtx &ioctx, bufferlist &bl, bool no_overwrite)
{
  bufferlist::iterator ebliter = bl.begin();
  object_begin ob;
  ob.decode(ebliter);
  map<string,bufferptr>::iterator i;
  bufferlist abl;
  bool skipping;

  data_section ds;
  attr_section as;
  omap_hdr_section oh;
  omap_section os;

  assert(g_ceph_context);
  if (ob.hoid.hobj.nspace == g_ceph_context->_conf->osd_hit_set_namespace) {
    cout << "Skipping internal object " << ob.hoid << std::endl;
    skip_object(bl);
    return 0;
  }

  if (!ob.hoid.hobj.is_head()) {
    cout << "Skipping non-head for " << ob.hoid << std::endl;
    skip_object(bl);
    return 0;
  }

  ioctx.set_namespace(ob.hoid.hobj.get_namespace());

  string msg("Write");
  skipping = false;
  if (dry_run) {
    uint64_t psize;
    time_t pmtime;
    int ret = ioctx.stat(ob.hoid.hobj.oid.name, &psize, &pmtime);
    if (ret == 0) {
      if (no_overwrite)
        // Could set skipping, but dry-run doesn't change anything either
        msg = "Skipping existing";
      else
        msg = "***Overwrite***";
    }
  } else {
    int ret = ioctx.create(ob.hoid.hobj.oid.name, true);
    if (ret && ret != -EEXIST) {
      cerr << "create failed: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    if (ret == -EEXIST) {
      if (no_overwrite) {
        msg = "Skipping existing";
        skipping = true;
      } else {
        msg = "***Overwrite***";
        ret = ioctx.remove(ob.hoid.hobj.oid.name);
        if (ret < 0) {
          cerr << "remove failed: " << cpp_strerror(ret) << std::endl;
          return ret;
        }
        ret = ioctx.create(ob.hoid.hobj.oid.name, true);
        // If object re-appeared after removal, let's just skip it
        if (ret == -EEXIST) {
          skipping = true;
          msg = "Skipping in-use object";
          ret = 0;
        }
        if (ret < 0) {
          cerr << "create failed: " << cpp_strerror(ret) << std::endl;
          return ret;
        }
      }
    }
  }

  cout << msg << " " << ob.hoid << std::endl;

  bool need_align = false;
  uint64_t alignment = 0;
  if (testalign) {
    need_align = true;
    alignment = testalign;
  } else {
    if ((need_align = ioctx.pool_requires_alignment()))
      alignment = ioctx.pool_required_alignment();
  }

  if (debug && need_align)
    cerr << "alignment = " << alignment << std::endl;

  bufferlist ebl, databl;
  uint64_t in_offset = 0, out_offset = 0;
  bool done = false;
  while(!done) {
    sectiontype_t type;
    int ret = read_section(file_fd, &type, &ebl);
    if (ret)
      return ret;

    ebliter = ebl.begin();
    //cout << "\tdo_object: Section type " << hex << type << dec << std::endl;
    //cout << "\t\tsection size " << ebl.length() << std::endl;
    if (type >= END_OF_TYPES) {
      cout << "Skipping unknown object section type" << std::endl;
      continue;
    }
    switch(type) {
    case TYPE_DATA:
      ds.decode(ebliter);
      if (debug)
        cerr << "\tdata: offset " << ds.offset << " len " << ds.len << std::endl;
      if (need_align) {
        if (ds.offset != in_offset) {
          cerr << "Discontiguous object data in export" << std::endl;
          return -EFAULT;
        }
        assert(ds.databl.length() == ds.len);
        databl.claim_append(ds.databl);
        in_offset += ds.len;
        if (databl.length() >= alignment) {
          uint64_t rndlen = uint64_t(databl.length() / alignment) * alignment;
          if (debug) cerr << "write offset=" << out_offset << " len=" << rndlen << std::endl;
          if (!dry_run && !skipping) {
            ret = ioctx.write(ob.hoid.hobj.oid.name, databl, rndlen, out_offset);
            if (ret) {
              cerr << "write failed: " << cpp_strerror(ret) << std::endl;
              return ret;
            }
          }
          out_offset += rndlen;
          bufferlist n;
          if (databl.length() > rndlen) {
            assert(databl.length() - rndlen < alignment);
	    n.substr_of(databl, rndlen, databl.length() - rndlen);
          }
          databl = n;
        }
        break;
      }
      if (!dry_run && !skipping) {
        ret = ioctx.write(ob.hoid.hobj.oid.name, ds.databl, ds.len, ds.offset);
        if (ret) {
          cerr << "write failed: " << cpp_strerror(ret) << std::endl;
          return ret;
        }
      }
      break;
    case TYPE_ATTRS:
      as.decode(ebliter);

      if (debug)
        cerr << "\tattrs: len " << as.data.size() << std::endl;
      if (dry_run || skipping)
        break;
      for (i = as.data.begin(); i != as.data.end(); ++i) {
        // The user xattrs that we want all begin with "_" with length > 1.
        // Drop key "_" and all attributes that do not start with '_'
        if (i->first == "_" || i->first[0] != '_')
          continue;
        abl.clear();
        abl.push_front(i->second);
        ret = ioctx.setxattr(ob.hoid.hobj.oid.name, i->first.substr(1).c_str(), abl);
        if (ret) {
          cerr << "setxattr failed: " << cpp_strerror(ret) << std::endl;
          if (ret != -EOPNOTSUPP)
            return ret;
        }
      }
      break;
    case TYPE_OMAP_HDR:
      oh.decode(ebliter);

      if (debug)
        cerr << "\tomap header: " << string(oh.hdr.c_str(), oh.hdr.length())
          << std::endl;
      if (dry_run || skipping)
        break;
      ret = ioctx.omap_set_header(ob.hoid.hobj.oid.name, oh.hdr);
      if (ret) {
        cerr << "omap_set_header failed: " << cpp_strerror(ret) << std::endl;
        if (ret != -EOPNOTSUPP)
          return ret;
      }
      break;
    case TYPE_OMAP:
      os.decode(ebliter);

      if (debug)
        cerr << "\tomap: size " << os.omap.size() << std::endl;
      if (dry_run || skipping)
        break;
      ret = ioctx.omap_set(ob.hoid.hobj.oid.name, os.omap);
      if (ret) {
        cerr << "omap_set failed: " << cpp_strerror(ret) << std::endl;
        if (ret != -EOPNOTSUPP)
          return ret;
      }
      break;
    case TYPE_OBJECT_END:
      done = true;
      if (need_align && databl.length() > 0) {
        assert(databl.length() < alignment);
        if (debug) cerr << "END write offset=" << out_offset << " len=" << databl.length() << std::endl;
        if (dry_run || skipping)
          break;
        ret = ioctx.write(ob.hoid.hobj.oid.name, databl, databl.length(), out_offset);
        if (ret) {
           cerr << "write failed: " << cpp_strerror(ret) << std::endl;
          return ret;
        }
      }
      break;
    default:
      return -EFAULT;
    }
  }
  return 0;
}

int get_object(ObjectStore *store, coll_t coll, bufferlist &bl, OSDMap &curmap,
               bool *skipped_objects)
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
  coll.is_pg_prefix(pg);
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
    snapid_t coll_snap;
    if (coll.is_pg(coll_pgid, coll_snap) == false) {
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
    int ret = read_section(file_fd, &type, &ebl);
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
    store->apply_transaction(*t);
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

int do_import_rados(string pool, bool no_overwrite)
{
  bufferlist ebl;
  pg_info_t info;
  PGLog::IndexedLog log;

  int ret = sh.read_super();
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
  ret = read_section(file_fd, &type, &ebl);
  if (ret)
    return ret;
  if (type != TYPE_PG_BEGIN) {
    cerr << "Invalid first section type " << type << std::endl;
    return -EFAULT;
  }

  bufferlist::iterator ebliter = ebl.begin();
  pg_begin pgb;
  pgb.decode(ebliter);
  spg_t pgid = pgb.pgid;

  if (!pgid.is_no_shard()) {
    cerr << "Importing Erasure Coded shard is not supported" << std::endl;
    myexit(1);
  }

  if (debug) {
    cerr << "Exported features: " << pgb.superblock.compat_features << std::endl;
  }

  // XXX: How to check export features?
#if 0
  if (sb.compat_features.compare(pgb.superblock.compat_features) == -1) {
    cerr << "Export has incompatible features set "
      << pgb.superblock.compat_features << std::endl;
    return -EINVAL;
  }
#endif

  librados::IoCtx ioctx;
  librados::Rados cluster;

  char *id = getenv("CEPH_CLIENT_ID");
  if (id) cerr << "Client id is: " << id << std::endl;
  ret = cluster.init(id);
  if (ret) {
    cerr << "Error " << ret << " in cluster.init" << std::endl;
    return ret;
  }
  ret = cluster.conf_read_file(NULL);
  if (ret) {
    cerr << "Error " << ret << " in cluster.conf_read_file" << std::endl;
    return ret;
  }
  ret = cluster.conf_parse_env(NULL);
  if (ret) {
    cerr << "Error " << ret << " in cluster.conf_read_env" << std::endl;
    return ret;
  }
  cluster.connect();

  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  if (ret < 0) {
    cerr << "ioctx_create " << pool << " failed with " << ret << std::endl;
    return ret;
  }

  cout << "Importing from pgid " << pgid << std::endl;

  bool done = false;
  bool found_metadata = false;
  metadata_section ms;
  while(!done) {
    ret = read_section(file_fd, &type, &ebl);
    if (ret)
      return ret;

    //cout << "do_import: Section type " << hex << type << dec << std::endl;
    if (type >= END_OF_TYPES) {
      cout << "Skipping unknown section type" << std::endl;
      continue;
    }
    switch(type) {
    case TYPE_OBJECT_BEGIN:
      ret = get_object_rados(ioctx, ebl, no_overwrite);
      if (ret) return ret;
      break;
    case TYPE_PG_METADATA:
      if (debug)
        cout << "Don't care about the old metadata" << std::endl;
      found_metadata = true;
      break;
    case TYPE_PG_END:
      done = true;
      break;
    default:
      return -EFAULT;
    }
  }

  if (!found_metadata) {
    cerr << "Missing metadata section, ignored" << std::endl;
  }

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

int do_import(ObjectStore *store, OSDSuperblock& sb, bool force, string pgidstr)
{
  bufferlist ebl;
  pg_info_t info;
  PGLog::IndexedLog log;
  bool skipped_objects = false;

  if (!dry_run)
    finish_remove_pgs(store);

  int ret = sh.read_super();
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
  ret = read_section(file_fd, &type, &ebl);
  if (ret)
    return ret;
  if (type != TYPE_PG_BEGIN) {
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

    // If shards setting the issue, then inform user what they can do about it.
    if (unsupported.incompat.contains(CEPH_OSD_FEATURE_INCOMPAT_SHARDS)) {
      cerr << std::endl;
      cerr << "OSD requires sharding to be enabled" << std::endl;
      cerr << std::endl;
      cerr << "If you wish to import, first do 'ceph-objectstore-tool...--op set-allow-sharded-objects'" << std::endl;
      return -EINVAL;
    }
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
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    PG::_create(*t, pgid);
    PG::_init(*t, pgid, NULL);

    // mark this coll for removal until we're done
    map<string,bufferlist> values;
    ::encode((char)1, values["_remove"]);
    t->omap_setkeys(coll, pgid.make_pgmeta_oid(), values);

    store->apply_transaction(*t);
    delete t;
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
    ret = read_section(file_fd, &type, &ebl);
    if (ret)
      return ret;

    //cout << "do_import: Section type " << hex << type << dec << std::endl;
    if (type >= END_OF_TYPES) {
      cout << "Skipping unknown section type" << std::endl;
      continue;
    }
    switch(type) {
    case TYPE_OBJECT_BEGIN:
      ret = get_object(store, coll, ebl, curmap, &skipped_objects);
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
    store->apply_transaction(t);
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
  r = action_on_all_objects_in_exact_pg(store, META_COLL, lookup, debug);
  if (r)
    return r;
  lookup.dump(formatter, human_readable);
  formatter->flush(cout);
  return 0;
}

int do_remove_object(ObjectStore *store, coll_t coll, ghobject_t &ghobj)
{
  spg_t pg;
  coll.is_pg_prefix(pg);
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
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  OSDriver::OSTransaction _t(driver.get_transaction(t));
  r = mapper.remove_oid(ghobj.hobj, &_t);
  if (r < 0 && r != -ENOENT) {
    cerr << "remove_oid returned " << cpp_strerror(r) << std::endl;
    return r;
  }

  t->remove(coll, ghobj);

  store->apply_transaction(*t);
  delete t;
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

int do_set_bytes(ObjectStore *store, coll_t coll, ghobject_t &ghobj, int fd)
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
    store->apply_transaction(*t);
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

int do_set_attr(ObjectStore *store, coll_t coll, ghobject_t &ghobj, string key, int fd)
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

  store->apply_transaction(*t);
  return 0;
}

int do_rm_attr(ObjectStore *store, coll_t coll, ghobject_t &ghobj, string key)
{
  ObjectStore::Transaction tran;
  ObjectStore::Transaction *t = &tran;

  if (debug)
    cerr << "Rmattr " << ghobj << std::endl;

  if (dry_run)
    return 0;

  t->rmattr(coll, ghobj, key);

  store->apply_transaction(*t);
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

int do_set_omap(ObjectStore *store, coll_t coll, ghobject_t &ghobj, string key, int fd)
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

  store->apply_transaction(*t);
  return 0;
}

int do_rm_omap(ObjectStore *store, coll_t coll, ghobject_t &ghobj, string key)
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

  store->apply_transaction(*t);
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

int do_set_omaphdr(ObjectStore *store, coll_t coll, ghobject_t &ghobj, int fd)
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

  store->apply_transaction(*t);
  return 0;
}

struct do_fix_lost : public action_on_object_t {
  virtual int call(ObjectStore *store, coll_t coll, ghobject_t &ghobj, object_info_t &oi) {
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
      int r = store->apply_transaction(t);
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

int set_size(ObjectStore *store, coll_t coll, ghobject_t &ghobj, uint64_t setsize, Formatter* formatter)
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
    r = store->apply_transaction(t);
    if (r < 0) {
      cerr << "Error writing object info: " << make_pair(coll, ghobj) << ", "
         << cpp_strerror(r) << std::endl;
      return r;
    }
  }
  return 0;
}

int clear_snapset(ObjectStore *store, coll_t coll, ghobject_t &ghobj,
                  string arg)
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
    int r = store->apply_transaction(t);
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

int remove_clone(ObjectStore *store, coll_t coll, ghobject_t &ghobj, snapid_t cloneid, bool force)
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
  int r = store->apply_transaction(t);
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
    cerr << "ceph-objectstore-tool import-rados <pool> [file]" << std::endl;
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
  string dpath, jpath, pgidstr, op, file, object, objcmd, arg1, arg2, type, format;
  spg_t pgid;
  unsigned epoch = 0;
  ghobject_t ghobj;
  bool human_readable, no_overwrite;
  bool force;
  Formatter *formatter;
  bool head;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("type", po::value<string>(&type),
     "Arg is one of [filestore (default), memstore, keyvaluestore]")
    ("data-path", po::value<string>(&dpath),
     "path to object store, mandatory")
    ("journal-path", po::value<string>(&jpath),
     "path to journal, mandatory for filestore type")
    ("pgid", po::value<string>(&pgidstr),
     "PG id, mandatory for info, log, remove, export, rm-past-intervals, mark-complete")
    ("op", po::value<string>(&op),
     "Arg is one of [info, log, remove, export, import, list, fix-lost, list-pgs, rm-past-intervals, set-allow-sharded-objects, dump-journal, dump-super, meta-list, "
	 "get-osdmap, set-osdmap, get-inc-osdmap, set-inc-osdmap, mark-complete]")
    ("epoch", po::value<unsigned>(&epoch),
     "epoch# for get-osdmap and get-inc-osdmap, the current epoch in use if not specified")
    ("file", po::value<string>(&file),
     "path of file to export, import, get-osdmap, set-osdmap, get-inc-osdmap or set-inc-osdmap")
    ("format", po::value<string>(&format)->default_value("json-pretty"),
     "Output format which may be json, json-pretty, xml, xml-pretty")
    ("debug", "Enable diagnostic output to stderr")
    ("force", "Ignore some types of errors and proceed with operation - USE WITH CAUTION: CORRUPTION POSSIBLE NOW OR IN THE FUTURE")
    ("skip-journal-replay", "Disable journal replay")
    ("skip-mount-omap", "Disable mounting of omap")
    ("head", "Find head/snapdir when searching for objects by name")
    ("dry-run", "Don't modify the objectstore")
    ("no-overwrite", "For import-rados don't overwrite existing files")
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

  no_overwrite = false;
  if (vm.count("no-overwrite"))
    no_overwrite = true;
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

  // Handle completely different operation "import-rados"
  if (object == "import-rados") {
    if (vm.count("objcmd") == 0) {
      cerr << "ceph-objectstore-tool import-rados <pool> [file]" << std::endl;
      myexit(1);
    }

    string pool = objcmd;
    // positional argument takes precendence, but accept
    // --file option too
    if (!vm.count("arg1")) {
      if (!vm.count("file"))
        arg1 = "-";
      else
        arg1 = file;
    }
    if (arg1 == "-") {
      if (isatty(STDIN_FILENO)) {
        cerr << "stdin is a tty and no file specified" << std::endl;
        myexit(1);
      }
      file_fd = STDIN_FILENO;
    } else {
      file_fd = open(arg1.c_str(), O_RDONLY);
      if (file_fd < 0) {
        perror("open");
	myexit(1);
      }
    }

    global_init(NULL, ceph_options, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
    common_init_finish(g_ceph_context);

    int ret = do_import_rados(pool, no_overwrite);
    if (ret == 0)
      cout << "Import successful" << std::endl;
    myexit(ret != 0);
  }

  if (!vm.count("type")) {
    type = "filestore";
  }
  if (!vm.count("data-path") &&
     !(op == "dump-journal" && type == "filestore")) {
    cerr << "Must provide --data-path" << std::endl;
    usage(desc);
    myexit(1);
  }
  if (type == "filestore" && !vm.count("journal-path")) {
    cerr << "Must provide --journal-path" << std::endl;
    usage(desc);
    myexit(1);
  }
  if (!vm.count("op") && !vm.count("object")) {
    cerr << "Must provide --op or object command..." << std::endl;
    usage(desc);
    myexit(1);
  }
  if (op != "list" && vm.count("op") && vm.count("object")) {
    cerr << "Can't specify both --op and object command syntax" << std::endl;
    usage(desc);
    myexit(1);
  }
  if (op != "list" && vm.count("object") && !vm.count("objcmd")) {
    cerr << "Invalid syntax, missing command" << std::endl;
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
  //Verify data data-path really is a filestore
  if (type == "filestore") {
    if (!S_ISDIR(st.st_mode)) {
      invalid_filestore_path(dpath);
    }
    string check = dpath + "/whoami";
    if (::stat(check.c_str(), &st) == -1) {
       perror("whoami");
       invalid_filestore_path(dpath);
    }
    if (!S_ISREG(st.st_mode)) {
      invalid_filestore_path(dpath);
    }
    check = dpath + "/current";
    if (::stat(check.c_str(), &st) == -1) {
       perror("current");
       invalid_filestore_path(dpath);
    }
    if (!S_ISDIR(st.st_mode)) {
      invalid_filestore_path(dpath);
    }
  }

  if (pgidstr.length() && !pgid.parse(pgidstr.c_str())) {
    cerr << "Invalid pgid '" << pgidstr << "' specified" << std::endl;
    myexit(1);
  }

  ObjectStore *fs = ObjectStore::create(g_ceph_context, type, dpath, jpath, flags);
  if (fs == NULL) {
    cerr << "Must provide --type (filestore, memstore, keyvaluestore)" << std::endl;
    if (type == "keyvaluestore") {
      cerr << "Add \"keyvaluestore\" to "
           << "enable_experimental_unrecoverable_data_corrupting_features"
           << std::endl;
    }
    myexit(1);
  }

  int ret = fs->mount();
  if (ret < 0) {
    if (ret == -EBUSY) {
      cerr << "OSD has the store locked" << std::endl;
    } else {
      cerr << "Mount failed with '" << cpp_strerror(ret) << "'" << std::endl;
    }
    myexit(1);
  }

  bool fs_sharded_objects = fs->get_allow_sharded_objects();

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
  ret = fs->read(META_COLL, OSD_SUPERBLOCK_POBJECT, 0, 0, bl);
  if (ret < 0) {
    cerr << "Failure to read OSD superblock: " << cpp_strerror(ret) << std::endl;
    goto out;
  }

  p = bl.begin();
  ::decode(superblock, p);

  if (debug) {
    cerr << "Cluster fsid=" << superblock.cluster_fsid << std::endl;
  }

#ifdef INTERNAL_TEST2
  fs->set_allow_sharded_objects();
  assert(fs->get_allow_sharded_objects());
  fs_sharded_objects = true;
  superblock.compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_SHARDS);
#endif

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

  if (op == "set-allow-sharded-objects") {
    // This could only happen if we backport changes to an older release
    if (!supported.incompat.contains(CEPH_OSD_FEATURE_INCOMPAT_SHARDS)) {
      cerr << "Can't enable sharded objects in this release" << std::endl;
      ret = 1;
      goto out;
    }
    if (superblock.compat_features.incompat.contains(CEPH_OSD_FEATURE_INCOMPAT_SHARDS) &&
        fs_sharded_objects) {
      cerr << "Sharded objects already fully enabled" << std::endl;
      ret = 0;
      goto out;
    }
    OSDMap curmap;
    bufferlist bl;
    ret = get_osdmap(fs, superblock.current_epoch, curmap, bl);
    if (ret) {
        cerr << "Can't find local OSDMap" << std::endl;
        goto out;
    }

    // Based on OSDMonitor::check_cluster_features()
    // XXX: The up state of osds in the last map isn't
    // as important from a non-running osd.  I'm using
    // get_all_osds() instead.  An osd which was never
    // upgraded and never removed would be flagged here.
    stringstream unsupported_ss;
    int unsupported_count = 0;
    uint64_t features = CEPH_FEATURE_OSD_ERASURE_CODES;
    set<int32_t> all_osds;
    curmap.get_all_osds(all_osds);
    for (set<int32_t>::iterator it = all_osds.begin();
         it != all_osds.end(); ++it) {
        const osd_xinfo_t &xi = curmap.get_xinfo(*it);
#ifdef INTERNAL_TEST3
        // Force one of the OSDs to not have support for erasure codes
        if (unsupported_count == 0)
            ((osd_xinfo_t &)xi).features &= ~features;
#endif
        if ((xi.features & features) != features) {
            if (unsupported_count > 0)
                unsupported_ss << ", ";
            unsupported_ss << "osd." << *it;
            unsupported_count ++;
        }
    }

    if (unsupported_count > 0) {
        cerr << "ERASURE_CODES feature unsupported by: "
           << unsupported_ss.str() << std::endl;
        ret = 1;
        goto out;
    }

    if (!dry_run) {
      superblock.compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_SHARDS);
      ObjectStore::Transaction t;
      bl.clear();
      ::encode(superblock, bl);
      t.write(META_COLL, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl);
      ret = fs->apply_transaction(t);
      if (ret < 0) {
        cerr << "Error writing OSD superblock: " << cpp_strerror(ret) << std::endl;
        goto out;
      }

      fs->set_allow_sharded_objects();
    }
    cout << "Enabled on-disk sharded objects" << std::endl;

    ret = 0;
    goto out;
  }

  // If there was a crash as an OSD was transitioning to sharded objects
  // and hadn't completed a set_allow_sharded_objects().
  // This utility does not want to attempt to finish that transition.
  if (superblock.compat_features.incompat.contains(CEPH_OSD_FEATURE_INCOMPAT_SHARDS) != fs_sharded_objects) {
    // An OSD should never have call set_allow_sharded_objects() before
    // updating its own OSD features.
    if (fs_sharded_objects)
      cerr << "FileStore sharded but OSD not set, Corruption?" << std::endl;
    else
      cerr << "Found incomplete transition to sharded objects" << std::endl;
    cerr << std::endl;
    cerr << "Use --op set-allow-sharded-objects to repair" << std::endl;
    ret = -EINVAL;
    goto out;
  }

  if (op == "import") {

    try {
      ret = do_import(fs, superblock, force, pgidstr);
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
      ret = set_osdmap(fs, epoch, bl, force);
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
      ret = set_inc_osdmap(fs, epoch, bl, force);
    }
    goto out;
  }

  log_oid = OSD::make_pg_log_oid(pgid);
  biginfo_oid = OSD::make_pg_biginfo_oid(pgid);

  if (op == "remove") {
    ret = initiate_new_remove_pg(fs, pgid);
    if (ret < 0) {
      cerr << "PG '" << pgid << "' not found" << std::endl;
      goto out;
    }
    cout << "Remove successful" << std::endl;
    goto out;
  }

  if (op == "fix-lost") {
    boost::scoped_ptr<action_on_object_t> action;
    action.reset(new do_fix_lost());
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
    snapid_t snap;
    spg_t tmppgid;

    if (pgidstr == "meta") {
      if (it->to_str() == "meta")
        break;
      else
        continue;
    }

    if (!it->is_pg(tmppgid, snap)) {
      continue;
    }

    if (it->is_temp(tmppgid)) {
      continue;
    }

    if (op != "list-pgs" && tmppgid != pgid) {
      continue;
    }
    if (snap != CEPH_NOSNAP) {
      if (debug)
        cerr << "skipping snapped dir " << *it
	       << " (pg " << pgid << " snap " << snap << ")" << std::endl;
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
    cerr << "Must provide --op (info, log, remove, export, import, list, fix-lost, list-pgs, rm-past-intervals, set-allow-sharded-objects, dump-journal, dump-super, meta-list, "
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
        ret = do_remove_object(fs, coll, ghobj);
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
          ret = do_set_bytes(fs, coll, ghobj, fd);
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
	ret = do_set_attr(fs, coll, ghobj, arg1, fd);
	if (fd != STDIN_FILENO)
	  close(fd);
        goto out;
      } else if (objcmd == "rm-attr") {
	if (vm.count("arg1") == 0) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	ret = do_rm_attr(fs, coll, ghobj, arg1);
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
	ret = do_set_omap(fs, coll, ghobj, arg1, fd);
	if (fd != STDIN_FILENO)
	  close(fd);
        goto out;
      } else if (objcmd == "rm-omap") {
	if (vm.count("arg1") == 0) {
	  usage(desc);
          ret = 1;
          goto out;
        }
	ret = do_rm_omap(fs, coll, ghobj, arg1);
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
	ret = do_set_omaphdr(fs, coll, ghobj, fd);
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
	ret = set_size(fs, coll, ghobj, size, formatter);
	goto out;
      } else if (objcmd == "clear-snapset") {
        // UNDOCUMENTED: For testing zap SnapSet
        // IGNORE extra args since not in usage anyway
	if (!ghobj.hobj.has_snapset()) {
	  cerr << "'" << objcmd << "' requires a head or snapdir object" << std::endl;
	  ret = 1;
	  goto out;
	}
        ret = clear_snapset(fs, coll, ghobj, arg1);
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
	ret = remove_clone(fs, coll, ghobj, cloneid, force);
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
      cerr << "peek_map_epoch returns an error" << std::endl;

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
      ret = do_export(fs, coll, pgid, info, map_epoch, struct_ver, superblock, past_intervals);
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
        fs->apply_transaction(*t);
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
	fs->apply_transaction(*t);
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
