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

#include <stdlib.h>

#include "common/Formatter.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"

#include "os/ObjectStore.h"
#include "os/FileStore.h"

#include "osd/PGLog.h"
#include "osd/OSD.h"

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

//The first FIXED_LENGTH bytes are a fixed
//portion of the export output.  This includes the overall
//version number, and size of header and footer.
//THIS STRUCTURE CAN ONLY BE APPENDED TO.  If it needs to expand,
//the version can be bumped and then anything
//can be added to the export format.
struct super_header {
  static const uint32_t super_magic = (shortmagic << 16) | shortmagic;
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
  object_begin(const ghobject_t &hoid): hoid(hoid) { }
  object_begin() { }

  // If superblock doesn't include CEPH_FS_FEATURE_INCOMPAT_SHARDS then
  // generation will be NO_GEN, shard_id will be NO_SHARD for a replicated
  // pool.  This means we will allow the decode by struct_v 1.
  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(hoid.hobj, bl);
    ::encode(hoid.generation, bl);
    ::encode(hoid.shard_id, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(hoid.hobj, bl);
    if (struct_v > 1) {
      ::decode(hoid.generation, bl);
      ::decode(hoid.shard_id, bl);
    } else {
      hoid.generation = ghobject_t::NO_GEN;
      hoid.shard_id = shard_id_t::NO_SHARD;
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
  __u8 struct_ver;
  epoch_t map_epoch;
  pg_info_t info;
  pg_log_t log;
  map<epoch_t,pg_interval_t> past_intervals;

  metadata_section(__u8 struct_ver, epoch_t map_epoch, const pg_info_t &info,
		   const pg_log_t &log, map<epoch_t,pg_interval_t> &past_intervals)
    : struct_ver(struct_ver),
      map_epoch(map_epoch),
      info(info),
      log(log),
      past_intervals(past_intervals) { }
  metadata_section()
    : struct_ver(0),
      map_epoch(0) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(struct_ver, bl);
    ::encode(map_epoch, bl);
    ::encode(info, bl);
    ::encode(log, bl);
    ::encode(past_intervals, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(struct_ver, bl);
    ::decode(map_epoch, bl);
    ::decode(info, bl);
    ::decode(log, bl);
    if (struct_v > 1) {
      ::decode(past_intervals, bl);
    } else {
      cout << "NOTICE: Older export without past_intervals" << std::endl;
    }
    DECODE_FINISH(bl);
  }
};

hobject_t infos_oid = OSD::make_infos_oid();
hobject_t biginfo_oid, log_oid;

int file_fd = fd_none;
bool debug = false;
super_header sh;
uint64_t testalign;

template <typename T>
int write_section(sectiontype_t type, const T& obj, int fd) {
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
      cerr << "read_fd error " << cpp_strerror(-bytes) << std::endl;
      return 1;
    }

    if (bytes == 0)
      break;

    total += bytes;
  } while(true);

  assert(bl.length() == total);
  return 0;
}

static void invalid_filestore_path(string &path)
{
  cerr << "Invalid filestore path specified: " << path << "\n";
  exit(1);
}

int get_log(ObjectStore *fs, coll_t coll, spg_t pgid, const pg_info_t &info,
   PGLog::IndexedLog &log, pg_missing_t &missing)
{
  map<eversion_t, hobject_t> divergent_priors;
  try {
    ostringstream oss;
    PGLog::read_log(fs, coll, log_oid, info, divergent_priors, log, missing, oss);
    if (debug && oss.str().size())
      cerr << oss.str() << std::endl;
  }
  catch (const buffer::error &e) {
    cerr << "read_log threw exception error " << e.what() << std::endl;
    return 1;
  }
  return 0;
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
    cerr << "finish_remove_pgs: failed to list pgs: " << cpp_strerror(-r)
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
    coll_t coll(pgid);
    char val;
    if (it->is_removal(&seq, &pgid) ||
	store->collection_getattr(coll, "remove", &val, 1) == 1) {
      cout << "finish_remove_pgs removing " << *it
	   << " pgid is " << pgid << std::endl;
      remove_coll(store, *it);
      continue;
    }

    //cout << "finish_remove_pgs ignoring unrecognized " << *it << std::endl;
  }
  return 0;
}

int initiate_new_remove_pg(ObjectStore *store, spg_t r_pgid)
{
  ObjectStore::Transaction *rmt = new ObjectStore::Transaction;

  if (store->collection_exists(coll_t(r_pgid))) {
    cout << " marking collection for removal" << std::endl;
    bufferlist one;
    one.append('1');
    rmt->collection_setattr(coll_t(r_pgid), "remove", one);
  } else {
    delete rmt;
    return ENOENT;
  }

  cout << "remove " << META_COLL << " " << log_oid.oid << std::endl;
  rmt->remove(META_COLL, log_oid);
  cout << "remove " << META_COLL << " " << biginfo_oid.oid << std::endl;
  rmt->remove(META_COLL, biginfo_oid);

  store->apply_transaction(*rmt);

  return 0;
}

int header::get_header()
{
  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, sh.header_size);
  if ((size_t)bytes != sh.header_size) {
    cerr << "Unexpected EOF" << std::endl;
    return EFAULT;
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
    return EFAULT;
  }

  decode(ebliter);

  if (magic != endmagic) {
    cerr << "Bad footer magic" << std::endl;
    return EFAULT;
  }

  return 0;
}

int write_info(ObjectStore::Transaction &t, epoch_t epoch, pg_info_t &info,
    __u8 struct_ver, map<epoch_t,pg_interval_t> &past_intervals)
{
  //Empty for this
  interval_set<snapid_t> snap_collections; // obsolete
  coll_t coll(info.pgid);

  int ret = PG::_write_info(t, epoch,
    info, coll,
    past_intervals,
    snap_collections,
    infos_oid,
    struct_ver,
    true, true);
  if (ret < 0) ret = -ret;
  if (ret) cerr << "Failed to write info" << std::endl;
  return ret;
}

void write_log(ObjectStore::Transaction &t, pg_log_t &log)
{
  map<eversion_t, hobject_t> divergent_priors;
  PGLog::write_log(t, log, log_oid, divergent_priors);
}

int write_pg(ObjectStore::Transaction &t, epoch_t epoch, pg_info_t &info,
    pg_log_t &log, __u8 struct_ver, map<epoch_t,pg_interval_t> &past_intervals)
{
  int ret = write_info(t, epoch, info, struct_ver, past_intervals);
  if (ret) return ret;
  write_log(t, log);
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
    cerr << "omap_get_header: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  omap_hdr_section ohs(hdrbuf);
  ret = write_section(TYPE_OMAP_HDR, ohs, file_fd);
  if (ret)
    return ret;

  ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(cid, obj);
  if (!iter) {
    ret = -ENOENT;
    cerr << "omap_get_iterator: " << cpp_strerror(-ret) << std::endl;
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
      r = export_file(store, coll, *i);
      if (r < 0)
        return r;
    }
  }
  return 0;
}

int get_osdmap(ObjectStore *store, epoch_t e, OSDMap &osdmap)
{
  bufferlist bl;
  bool found = store->read(
      META_COLL, OSD::get_osdmap_pobject_name(e), 0, 0, bl) >= 0;
  if (!found) {
    cerr << "Can't find OSDMap for pg epoch " << e << std::endl;
    return ENOENT;
  }
  osdmap.decode(bl);
  if (debug)
    cerr << osdmap << std::endl;
  return 0;
}

//Write super_header with its fixed 16 byte length
void write_super()
{
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

  cerr << "Exporting " << pgid << std::endl;

  int ret = get_log(fs, coll, pgid, info, log, missing);
  if (ret > 0)
      return ret;

  write_super();

  pg_begin pgb(pgid, superblock);
  // Special case: If replicated pg don't require the importing OSD to have shard feature
  if (pgid.is_no_shard()) {
    pgb.superblock.compat_features.incompat.remove(CEPH_OSD_FEATURE_INCOMPAT_SHARDS);
  }
  ret = write_section(TYPE_PG_BEGIN, pgb, file_fd);
  if (ret)
    return ret;

  ret = export_files(fs, coll);
  if (ret) {
    cerr << "export_files error " << ret << std::endl;
    return ret;
  }

  metadata_section ms(struct_ver, map_epoch, info, log, past_intervals);
  ret = write_section(TYPE_PG_METADATA, ms, file_fd);
  if (ret)
    return ret;

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
    return EFAULT;
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
    return EFAULT;
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

int get_object_rados(librados::IoCtx &ioctx, bufferlist &bl)
{
  bufferlist::iterator ebliter = bl.begin();
  object_begin ob;
  ob.decode(ebliter);
  map<string,bufferptr>::iterator i;
  bufferlist abl;

  data_section ds;
  attr_section as;
  omap_hdr_section oh;
  omap_section os;

  if (!ob.hoid.hobj.is_head()) {
    cout << "Skipping non-head for " << ob.hoid << std::endl;
  }

  ioctx.set_namespace(ob.hoid.hobj.get_namespace());

  string msg("Write");
  int ret = ioctx.create(ob.hoid.hobj.oid.name, true);
  if (ret && ret != -EEXIST) {
    cerr << "create failed: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  if (ret == -EEXIST) {
    msg = "***Overwrite***";
    ret = ioctx.remove(ob.hoid.hobj.oid.name);
    if (ret < 0) {
      cerr << "remove failed: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    ret = ioctx.create(ob.hoid.hobj.oid.name, true);
    if (ret < 0) {
      cerr << "create failed: " << cpp_strerror(ret) << std::endl;
      return ret;
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
          return EFAULT;
        }
        assert(ds.databl.length() == ds.len);
        databl.claim_append(ds.databl);
        in_offset += ds.len;
        if (databl.length() >= alignment) {
          uint64_t rndlen = uint64_t(databl.length() / alignment) * alignment;
          if (debug) cerr << "write offset=" << out_offset << " len=" << rndlen << std::endl;
          ret = ioctx.write(ob.hoid.hobj.oid.name, databl, rndlen, out_offset);
          if (ret) {
            cerr << "write failed: " << cpp_strerror(ret) << std::endl;
            return ret;
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
      ret = ioctx.write(ob.hoid.hobj.oid.name, ds.databl, ds.len, ds.offset);
      if (ret) {
        cerr << "write failed: " << cpp_strerror(ret) << std::endl;
        return ret;
      }
      break;
    case TYPE_ATTRS:
      as.decode(ebliter);

      if (debug)
        cerr << "\tattrs: len " << as.data.size() << std::endl;
      for (i = as.data.begin(); i != as.data.end(); ++i) {
        if (i->first == "_" || i->first == "snapset")
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
      ret = ioctx.omap_set(ob.hoid.hobj.oid.name, os.omap);
      if (ret) {
        cerr << "omap_set failed: " << cpp_strerror(ret) << std::endl;
        if (ret != -EOPNOTSUPP)
          return ret;
      }
      break;
    case TYPE_OBJECT_END:
      if (need_align && databl.length() > 0) {
        assert(databl.length() < alignment);
        if (debug) cerr << "END write offset=" << out_offset << " len=" << databl.length() << std::endl;
        ret = ioctx.write(ob.hoid.hobj.oid.name, databl, databl.length(), out_offset);
        if (ret) {
           cerr << "write failed: " << cpp_strerror(ret) << std::endl;
          return ret;
        }
      }
      done = true;
      break;
    default:
      return EFAULT;
    }
  }
  return 0;
}

int get_object(ObjectStore *store, coll_t coll, bufferlist &bl)
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
      ret = get_data(store, coll, ob.hoid, t, ebl);
      if (ret) return ret;
      break;
    case TYPE_ATTRS:
      ret = get_attrs(store, coll, ob.hoid, t, ebl, driver, mapper);
      if (ret) return ret;
      break;
    case TYPE_OMAP_HDR:
      ret = get_omap_hdr(store, coll, ob.hoid, t, ebl);
      if (ret) return ret;
      break;
    case TYPE_OMAP:
      ret = get_omap(store, coll, ob.hoid, t, ebl);
      if (ret) return ret;
      break;
    case TYPE_OBJECT_END:
      done = true;
      break;
    default:
      return EFAULT;
    }
  }
  store->apply_transaction(*t);
  return 0;
}

int get_pg_metadata(ObjectStore *store, coll_t coll, bufferlist &bl)
{
  ObjectStore::Transaction tran;
  ObjectStore::Transaction *t = &tran;
  bufferlist::iterator ebliter = bl.begin();
  metadata_section ms;
  ms.decode(ebliter);

#if DIAGNOSTIC
  Formatter *formatter = new JSONFormatter(true);
  cout << "struct_v " << (int)ms.struct_ver << std::endl;
  cout << "epoch " << ms.map_epoch << std::endl;
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
#endif

  int ret = write_pg(*t, ms.map_epoch, ms.info, ms.log, ms.struct_ver, ms.past_intervals);
  if (ret) return ret;

  store->apply_transaction(*t);

  return 0;
}

int do_import_rados(string pool)
{
  bufferlist ebl;
  pg_info_t info;
  PGLog::IndexedLog log;

  int ret = sh.read_super();
  if (ret)
    return ret;

  if (sh.magic != super_header::super_magic) {
    cerr << "Invalid magic number" << std::endl;
    return EFAULT;
  }

  if (sh.version > super_header::super_ver) {
    cerr << "Can't handle export format version=" << sh.version << std::endl;
    return EINVAL;
  }

  //First section must be TYPE_PG_BEGIN
  sectiontype_t type;
  ret = read_section(file_fd, &type, &ebl);
  if (ret)
    return ret;
  if (type != TYPE_PG_BEGIN) {
    return EFAULT;
  }

  bufferlist::iterator ebliter = ebl.begin();
  pg_begin pgb;
  pgb.decode(ebliter);
  spg_t pgid = pgb.pgid;

  if (!pgid.is_no_shard()) {
    cerr << "Importing Erasure Coded shard is not supported" << std::endl;
    exit(1);
  }

  if (debug) {
    cerr << "Exported features: " << pgb.superblock.compat_features << std::endl;
  }

  // XXX: How to check export features?
#if 0
  if (sb.compat_features.compare(pgb.superblock.compat_features) == -1) {
    cerr << "Export has incompatible features set "
      << pgb.superblock.compat_features << std::endl;
    return 1;
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
      ret = get_object_rados(ioctx, ebl);
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
      return EFAULT;
    }
  }

  if (!found_metadata) {
    cerr << "Missing metadata section, ignored" << std::endl;
  }

  return 0;
}

int do_import(ObjectStore *store, OSDSuperblock& sb)
{
  bufferlist ebl;
  pg_info_t info;
  PGLog::IndexedLog log;

  finish_remove_pgs(store);

  int ret = sh.read_super();
  if (ret)
    return ret;

  if (sh.magic != super_header::super_magic) {
    cerr << "Invalid magic number" << std::endl;
    return EFAULT;
  }

  if (sh.version > super_header::super_ver) {
    cerr << "Can't handle export format version=" << sh.version << std::endl;
    return EINVAL;
  }

  //First section must be TYPE_PG_BEGIN
  sectiontype_t type;
  ret = read_section(file_fd, &type, &ebl);
  if (ret)
    return ret;
  if (type != TYPE_PG_BEGIN) {
    return EFAULT;
  }

  bufferlist::iterator ebliter = ebl.begin();
  pg_begin pgb;
  pgb.decode(ebliter);
  spg_t pgid = pgb.pgid;

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
      cerr << "If you wish to import, first do 'ceph_objectstore_tool...--op set-allow-sharded-objects'" << std::endl;
    }
    return 1;
  }

  log_oid = OSD::make_pg_log_oid(pgid);
  biginfo_oid = OSD::make_pg_biginfo_oid(pgid);

  //Check for PG already present.
  coll_t coll(pgid);
  if (store->collection_exists(coll)) {
    cerr << "pgid " << pgid << " already exists" << std::endl;
    return 1;
  }

  // mark this coll for removal until we are done
  bufferlist one;
  one.append('1');
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  t->create_collection(coll);
  t->collection_setattr(coll, "remove", one);
  store->apply_transaction(*t);
  delete t;

  cout << "Importing pgid " << pgid << std::endl;

  bool done = false;
  bool found_metadata = false;
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
      ret = get_object(store, coll, ebl);
      if (ret) return ret;
      break;
    case TYPE_PG_METADATA:
      ret = get_pg_metadata(store, coll, ebl);
      if (ret) return ret;
      found_metadata = true;
      break;
    case TYPE_PG_END:
      done = true;
      break;
    default:
      return EFAULT;
    }
  }

  if (!found_metadata) {
    cerr << "Missing metadata section" << std::endl;
    return EFAULT;
  }

  // done, clear removal flag
  cout << "done, clearing remove flag" << std::endl;
  t = new ObjectStore::Transaction;
  t->collection_rmattr(coll, "remove");
  store->apply_transaction(*t);
  delete t;

  return 0;
}

int do_list(ObjectStore *store, coll_t coll, Formatter *formatter)
{
  ghobject_t next;
  while (!next.is_max()) {
    vector<ghobject_t> objects;
    int r = store->collection_list_partial(coll, next, 200, 300, 0,
      &objects, &next);
    if (r < 0)
      return r;
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end(); ++i) {

      formatter->open_object_section("list");
      i->dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    }
  }
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
    cerr << "remove: " << cpp_strerror(-r) << std::endl;
    return r;
  }

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  OSDriver::OSTransaction _t(driver.get_transaction(t));
  cout << "remove " << ghobj << std::endl;
  r = mapper.remove_oid(ghobj.hobj, &_t);
  if (r != 0 && r != -ENOENT) {
    cerr << "remove_oid returned " << cpp_strerror(-r) << std::endl;
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
    cerr << "getattrs: " << cpp_strerror(-r) << std::endl;
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
    cerr << "get-bytes: " << cpp_strerror(-ret) << std::endl;
    return 1;
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
      return 1;
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

  t->touch(coll, ghobj);
  t->truncate(coll, ghobj, 0);

  uint64_t offset = 0;
  bufferlist rawdatabl;
  do {
    rawdatabl.clear();
    ssize_t bytes = rawdatabl.read_fd(fd, max_read);
    if (bytes < 0) {
      cerr << "read_fd error " << cpp_strerror(-bytes) << std::endl;
      return 1;
    }

    if (bytes == 0)
      break;

    if (debug)
      cerr << "\tdata: offset " << offset << " bytes " << bytes << std::endl;
    t->write(coll, ghobj, offset, bytes,  rawdatabl);

    offset += bytes;
    // XXX: Should we apply_transaction() every once in a while for very large files
  } while(true);

  store->apply_transaction(*t);
  return 0;
}

int do_get_attr(ObjectStore *store, coll_t coll, ghobject_t &ghobj, string key)
{
  bufferptr bp;

  int r = store->getattr(coll, ghobj, key.c_str(), bp);
  if (r < 0) {
    cerr << "getattr: " << cpp_strerror(-r) << std::endl;
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

  if (get_fd_data(fd, bl))
    return 1;

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
    cerr << "omap_get_values: " << cpp_strerror(-r) << std::endl;
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

  if (get_fd_data(fd, valbl))
    return 1;

  attrset.insert(pair<string, bufferlist>(key, valbl));

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

  t->omap_rmkeys(coll, ghobj, keys);

  store->apply_transaction(*t);
  return 0;
}

int do_get_omaphdr(ObjectStore *store, coll_t coll, ghobject_t &ghobj)
{
  bufferlist hdrbl;

  int r = store->omap_get_header(coll, ghobj, &hdrbl, true);
  if (r < 0) {
    cerr << "omap_get_header: " << cpp_strerror(-r) << std::endl;
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

  if (get_fd_data(fd, hdrbl))
    return 1;

  t->touch(coll, ghobj);

  t->omap_setheader(coll, ghobj, hdrbl);

  store->apply_transaction(*t);
  return 0;
}

void usage(po::options_description &desc)
{
    cerr << std::endl;
    cerr << desc << std::endl;
    cerr << std::endl;
    cerr << "Positional syntax:" << std::endl;
    cerr << std::endl;
    cerr << "(requires --data, --journal (for filestore type) and --pgid to be specified)" << std::endl;
    cerr << "(optional [file] argument will read stdin or write stdout if not specified or if '-' specified)" << std::endl;
    cerr << "ceph_objectstore_tool ... <object> (get|set)-bytes [file]" << std::endl;
    cerr << "ceph_objectstore_tool ... <object> (set-(attr|omap) <key> [file]" << std::endl;
    cerr << "ceph_objectstore_tool ... <object> (set-omaphdr) [file]" << std::endl;
    cerr << "ceph_objectstore_tool ... <object> (get|rm)-(attr|omap) <key>" << std::endl;
    cerr << "ceph_objectstore_tool ... <object> (get-omaphdr)" << std::endl;
    cerr << "ceph_objectstore_tool ... <object> list-attrs" << std::endl;
    cerr << "ceph_objectstore_tool ... <object> list-omap" << std::endl;
    cerr << "ceph_objectstore_tool ... <object> remove" << std::endl;
    cerr << std::endl;
    cerr << "ceph_objectstore_tool import-rados <pool> [file]" << std::endl;
    cerr << std::endl;
    exit(1);
}

int main(int argc, char **argv)
{
  string dpath, jpath, pgidstr, op, file, object, objcmd, arg1, arg2, type;
  ghobject_t ghobj;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("type", po::value<string>(&type),
     "Arg is one of [filestore (default), memstore, keyvaluestore-dev]")
    ("data-path", po::value<string>(&dpath),
     "path to object store, mandatory")
    ("journal-path", po::value<string>(&jpath),
     "path to journal, mandatory for filestore type")
    ("pgid", po::value<string>(&pgidstr),
     "PG id, mandatory except for import, list-lost, fix-lost")
    ("op", po::value<string>(&op),
     "Arg is one of [info, log, remove, export, import, list, list-lost, fix-lost, list-pgs, rm-past-intervals, set-allow-sharded-objects]")
    ("file", po::value<string>(&file),
     "path of file to export or import")
    ("debug", "Enable diagnostic output to stderr")
    ("skip-journal-replay", "Disable journal replay")
    ("skip-mount-omap", "Disable mounting of omap")
    ;

  po::options_description positional("Positional options");
  positional.add_options()
    ("object", po::value<string>(&object), "ghobject in json")
    ("objcmd", po::value<string>(&objcmd), "command [(get|set)-bytes, (get|set|rm)-(attr|omap), list-attrs, list-omap, remove]")
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
    return 1;
  }

  if (vm.count("help")) {
    usage(desc);
  }

  if (!vm.count("debug")) {
    debug = false;
  } else {
    debug = true;
  }

  // Handle completely different operation "import-rados"
  if (object == "import-rados") {
    if (vm.count("objcmd") == 0) {
      cerr << "ceph_objectstore_tool import-rados <pool> [file]" << std::endl;
      exit(1);
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
        exit(1);
      }
      file_fd = STDIN_FILENO;
    } else {
      file_fd = open(arg1.c_str(), O_RDONLY);
      if (file_fd < 0) {
        perror("open");
        return 1;
      }
    }
    int ret = do_import_rados(pool);
    if (ret == 0)
      cout << "Import successful" << std::endl;
    return ret != 0;
  }

  if (!vm.count("data-path")) {
    cerr << "Must provide --data-path" << std::endl;
    usage(desc);
  }
  if (!vm.count("type")) {
    type = "filestore";
  }
  if (type == "filestore" && !vm.count("journal-path")) {
    cerr << "Must provide --journal-path" << std::endl;
    usage(desc);
  }
  if (vm.count("object") && !vm.count("objcmd")) {
    cerr << "Invalid syntax, missing command" << std::endl;
    usage(desc);
  }
  if (!vm.count("op") && !(vm.count("object") && vm.count("objcmd"))) {
    cerr << "Must provide --op or object command..." << std::endl;
    usage(desc);
  }
  if (vm.count("op") && vm.count("object")) {
    cerr << "Can't specify both --op and object command syntax" << std::endl;
    usage(desc);
  }
  if (op != "import" && op != "list-lost" && op != "fix-lost"
      && op != "list-pgs"  && op != "set-allow-sharded-objects" && !vm.count("pgid")) {
    cerr << "Must provide pgid" << std::endl;
    usage(desc);
  }

  if (vm.count("object")) {
    json_spirit::Value v;
    try {
      if (!json_spirit::read(object, v))
        throw std::runtime_error("bad json");
      ghobj.decode(v);
    } catch (std::runtime_error& e) {
      cerr << "error parsing offset: " << e.what() << std::endl;
      exit(1);
    }
  }

  outistty = isatty(STDOUT_FILENO);

  file_fd = fd_none;
  if (op == "export") {
    if (!vm.count("file") || file == "-") {
      if (outistty) {
        cerr << "stdout is a tty and no --file filename specified" << std::endl;
        exit(1);
      }
      file_fd = STDOUT_FILENO;
    } else {
      file_fd = open(file.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
    }
  } else if (op == "import") {
    if (!vm.count("file") || file == "-") {
      if (isatty(STDIN_FILENO)) {
        cerr << "stdin is a tty and no --file filename specified" << std::endl;
        exit(1);
      }
      file_fd = STDIN_FILENO;
    } else {
      file_fd = open(file.c_str(), O_RDONLY);
    }
  }

  if (vm.count("file") && file_fd == fd_none) {
    cerr << "--file option only applies to import or export" << std::endl;
    return 1;
  }

  if (file_fd != fd_none && file_fd < 0) {
    perror("open");
    return 1;
  }

  if (dpath.length() == 0) {
    cerr << "Invalid params" << std::endl;
    return 1;
  }

  if (op == "import" && pgidstr.length()) {
    cerr << "--pgid option invalid with import" << std::endl;
    return 1;
  }

  vector<const char *> ceph_options;
  env_to_vec(ceph_options);
  ceph_options.reserve(ceph_options.size() + ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  osflagbits_t flags = 0;
  if (vm.count("skip-journal-replay"))
    flags |= SKIP_JOURNAL_REPLAY;
  if (vm.count("skip-mount-omap"))
    flags |= SKIP_MOUNT_OMAP;

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

  //Verify that data-path really exists
  struct stat st;
  if (::stat(dpath.c_str(), &st) == -1) {
     perror("data-path");
     exit(1);
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

  spg_t pgid;
  if (pgidstr.length() && !pgid.parse(pgidstr.c_str())) {
    cerr << "Invalid pgid '" << pgidstr << "' specified" << std::endl;
    return 1;
  }

  ObjectStore *fs = ObjectStore::create(g_ceph_context, type, dpath, jpath, flags);
  if (fs == NULL) {
    cerr << "Must provide --type (filestore, memstore, keyvaluestore-dev)" << std::endl;
    exit(1);
  }

  int r = fs->mount();
  if (r < 0) {
    if (r == -EBUSY) {
      cerr << "OSD has the store locked" << std::endl;
    } else {
      cerr << "Mount failed with '" << cpp_strerror(-r) << "'" << std::endl;
    }
    return 1;
  }

  bool fs_sharded_objects = fs->get_allow_sharded_objects();

  int ret = 0;
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
  r = fs->read(META_COLL, OSD_SUPERBLOCK_POBJECT, 0, 0, bl);
  if (r < 0) {
    cerr << "Failure to read OSD superblock error= " << r << std::endl;
    goto out;
  }

  p = bl.begin();
  ::decode(superblock, p);

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
    cerr << "On-disk OSD incompatible features set "
      << superblock.compat_features << std::endl;
    ret = EINVAL;
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
    ret = get_osdmap(fs, superblock.current_epoch, curmap);
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

    superblock.compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_SHARDS);
    ObjectStore::Transaction t;
    bufferlist bl;
    ::encode(superblock, bl);
    t.write(META_COLL, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl);
    r = fs->apply_transaction(t);
    if (r < 0) {
      cerr << "Error writing OSD superblock: " << cpp_strerror(r) << std::endl;
      ret = 1;
      goto out;
    }

    fs->set_allow_sharded_objects();

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
    ret = EINVAL;
    goto out;
  }

  if (op == "import") {

    try {
      ret = do_import(fs, superblock);
    }
    catch (const buffer::error &e) {
      cerr << "do_import threw exception error " << e.what() << std::endl;
      ret = EFAULT;
    }
    if (ret == EFAULT) {
      cerr << "Corrupt input for import" << std::endl;
    }
    if (ret == 0)
      cout << "Import successful" << std::endl;
    goto out;
  }

  log_oid = OSD::make_pg_log_oid(pgid);
  biginfo_oid = OSD::make_pg_biginfo_oid(pgid);

  if (op == "remove") {
    finish_remove_pgs(fs);
    int r = initiate_new_remove_pg(fs, pgid);
    if (r) {
      cerr << "PG '" << pgid << "' not found" << std::endl;
      ret = 1;
      goto out;
    }
    finish_remove_pgs(fs);
    cout << "Remove successful" << std::endl;
    goto out;
  }

  if (op == "list-lost" || op == "fix-lost") {
    unsigned LIST_AT_A_TIME = 100;
    unsigned scanned = 0;
    int r = 0;
    vector<coll_t> colls_to_check;
    if (pgidstr.length()) {
      colls_to_check.push_back(coll_t(pgid));
    } else {
      vector<coll_t> candidates;
      r = fs->list_collections(candidates);
      if (r < 0) {
        cerr << "Error listing collections: " << cpp_strerror(r) << std::endl;
        goto UMOUNT;
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
    }

    cout << colls_to_check.size() << " pgs to scan" << std::endl;
    for (vector<coll_t>::iterator i = colls_to_check.begin();
         i != colls_to_check.end();
         ++i, ++scanned) {
      cout << "Scanning " << *i << ", " << scanned << "/"
           << colls_to_check.size() << " completed" << std::endl;
      ghobject_t next;
      while (!next.is_max()) {
        vector<ghobject_t> list;
        r = fs->collection_list_partial(
          *i,
          next,
          LIST_AT_A_TIME,
          LIST_AT_A_TIME,
          CEPH_NOSNAP,
          &list,
          &next);
        if (r < 0) {
          cerr << "Error listing collection: " << *i << ", "
               << cpp_strerror(r) << std::endl;
          goto UMOUNT;
        }
        for (vector<ghobject_t>::iterator obj = list.begin();
             obj != list.end();
             ++obj) {
          bufferlist attr;
          r = fs->getattr(*i, *obj, OI_ATTR, attr);
          if (r < 0) {
            cerr << "Error getting attr on : " << make_pair(*i, *obj) << ", "
                 << cpp_strerror(r) << std::endl;
            goto UMOUNT;
          }
          object_info_t oi;
          bufferlist::iterator bp = attr.begin();
          try {
            ::decode(oi, bp);
          } catch (...) {
            r = -EINVAL;
            cerr << "Error getting attr on : " << make_pair(*i, *obj) << ", "
                 << cpp_strerror(r) << std::endl;
            goto UMOUNT;
          }
          if (oi.is_lost()) {
            if (op == "list-lost") {
              cout << *i << "/" << *obj << " is lost" << std::endl;
            }
            if (op == "fix-lost") {
              cout << *i << "/" << *obj << " is lost, fixing" << std::endl;
              oi.clear_flag(object_info_t::FLAG_LOST);
              bufferlist bl2;
              ::encode(oi, bl2);
              ObjectStore::Transaction t;
              t.setattr(*i, *obj, OI_ATTR, bl2);
              r = fs->apply_transaction(t);
              if (r < 0) {
                cerr << "Error getting fixing attr on : " << make_pair(*i, *obj)
                     << ", "
                     << cpp_strerror(r) << std::endl;
                goto UMOUNT;
              }
            }
          }
        }
      }
    }
    cout << "Completed" << std::endl;

   UMOUNT:
    fs->sync_and_flush();
    ret = r;
    goto out;
  }

  r = fs->list_collections(ls);
  if (r < 0) {
    cerr << "failed to list pgs: " << cpp_strerror(-r) << std::endl;
    ret = 1;
    goto out;
  }

  if (debug && op == "list-pgs")
    cout << "Performing list-pgs operation" << std::endl;

  // Find pg
  for (it = ls.begin(); it != ls.end(); ++it) {
    snapid_t snap;
    spg_t tmppgid;

    if (!it->is_pg(tmppgid, snap)) {
      continue;
    }

    if (it->is_temp(tmppgid)) {
      continue;
    }

    if (op != "list-pgs" && tmppgid != pgid) {
      continue;
    }
    if (snap != CEPH_NOSNAP && debug) {
      cout << "skipping snapped dir " << *it
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

  epoch_t map_epoch;
// The following code for export, info, log require omap or !skip-mount-omap
  if (it != ls.end()) {

    coll_t coll = *it;

    if (vm.count("objcmd")) {
      ret = 0;
      if (objcmd == "remove") {
        int r = do_remove_object(fs, coll, ghobj);
        if (r) {
          ret = 1;
        }
        goto out;
      } else if (objcmd == "list-attrs") {
        int r = do_list_attrs(fs, coll, ghobj);
        if (r) {
          ret = 1;
        }
        goto out;
      } else if (objcmd == "list-omap") {
        int r = do_list_omap(fs, coll, ghobj);
        if (r) {
          ret = 1;
        }
        goto out;
      } else if (objcmd == "get-bytes" || objcmd == "set-bytes") {
        int r;
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
          r = do_get_bytes(fs, coll, ghobj, fd);
          if (fd != STDOUT_FILENO)
            close(fd);
        } else {
          int fd;
          if (vm.count("arg1") == 0 || arg1 == "-") {
            fd = STDIN_FILENO;
	  } else {
            fd = open(arg1.c_str(), O_RDONLY|O_LARGEFILE, 0666);
            if (fd == -1) {
              cerr << "open " << arg1 << " " << cpp_strerror(errno) << std::endl;
              ret = 1;
              goto out;
            }
          }
          r = do_set_bytes(fs, coll, ghobj, fd);
          if (fd != STDIN_FILENO)
            close(fd);
        }
        if (r)
          ret = 1;
        goto out;
      } else if (objcmd == "get-attr") {
	if (vm.count("arg1") == 0)
	  usage(desc);
	r = do_get_attr(fs, coll, ghobj, arg1);
	if (r)
	  ret = 1;
        goto out;
      } else if (objcmd == "set-attr") {
	if (vm.count("arg1") == 0)
	  usage(desc);

	int fd;
	if (vm.count("arg2") == 0 || arg2 == "-") {
	  fd = STDIN_FILENO;
	} else {
	  fd = open(arg2.c_str(), O_RDONLY|O_LARGEFILE, 0666);
	  if (fd == -1) {
	    cerr << "open " << arg2 << " " << cpp_strerror(errno) << std::endl;
	    ret = 1;
	    goto out;
	  }
	}
	r = do_set_attr(fs, coll, ghobj, arg1, fd);
	if (fd != STDIN_FILENO)
	  close(fd);
	if (r)
	  ret = 1;
        goto out;
      } else if (objcmd == "rm-attr") {
	if (vm.count("arg1") == 0)
	  usage(desc);
	r = do_rm_attr(fs, coll, ghobj, arg1);
	if (r)
	  ret = 1;
        goto out;
      } else if (objcmd == "get-omap") {
	if (vm.count("arg1") == 0)
	  usage(desc);
	r = do_get_omap(fs, coll, ghobj, arg1);
	if (r)
	  ret = 1;
        goto out;
      } else if (objcmd == "set-omap") {
	if (vm.count("arg1") == 0)
	  usage(desc);

	int fd;
	if (vm.count("arg2") == 0 || arg2 == "-") {
	  fd = STDIN_FILENO;
	} else {
	  fd = open(arg2.c_str(), O_RDONLY|O_LARGEFILE, 0666);
	  if (fd == -1) {
	    cerr << "open " << arg2 << " " << cpp_strerror(errno) << std::endl;
	    ret = 1;
	    goto out;
	  }
	}
	r = do_set_omap(fs, coll, ghobj, arg1, fd);
	if (fd != STDIN_FILENO)
	  close(fd);
	if (r)
	  ret = 1;
        goto out;
      } else if (objcmd == "rm-omap") {
	if (vm.count("arg1") == 0)
	  usage(desc);
	r = do_rm_omap(fs, coll, ghobj, arg1);
	if (r)
	  ret = 1;
        goto out;
      } else if (objcmd == "get-omaphdr") {
	if (vm.count("arg1"))
	  usage(desc);
	r = do_get_omaphdr(fs, coll, ghobj);
	if (r)
	  ret = 1;
        goto out;
      } else if (objcmd == "set-omaphdr") {
        // Extra arg
	if (vm.count("arg2"))
	  usage(desc);
	int fd;
	if (vm.count("arg1") == 0 || arg1 == "-") {
	  fd = STDIN_FILENO;
	} else {
	  fd = open(arg1.c_str(), O_RDONLY|O_LARGEFILE, 0666);
	  if (fd == -1) {
	    cerr << "open " << arg1 << " " << cpp_strerror(errno) << std::endl;
	    ret = 1;
	    goto out;
	  }
	}
	r = do_set_omaphdr(fs, coll, ghobj, fd);
	if (fd != STDIN_FILENO)
	  close(fd);
	if (r)
	  ret = 1;
        goto out;
      }
      cerr << "Unknown object command '" << objcmd << "'" << std::endl;
      usage(desc);
    }

    if (op == "list") {
      Formatter *formatter = new JSONFormatter(false);
      r = do_list(fs, coll, formatter);
      if (r) {
        cerr << "do_list failed with " << r << std::endl;
        ret = 1;
      }
      goto out;
    }

    Formatter *formatter = new JSONFormatter(true);
    bufferlist bl;
    map_epoch = PG::peek_map_epoch(fs, coll, infos_oid, &bl);
    if (debug)
      cerr << "map_epoch " << map_epoch << std::endl;

    pg_info_t info(pgid);
    map<epoch_t,pg_interval_t> past_intervals;
    hobject_t biginfo_oid = OSD::make_pg_biginfo_oid(pgid);
    interval_set<snapid_t> snap_collections;

    __u8 struct_ver;
    r = PG::read_info(fs, coll, bl, info, past_intervals, biginfo_oid,
      infos_oid, snap_collections, struct_ver);
    if (r < 0) {
      cerr << "read_info error " << cpp_strerror(-r) << std::endl;
      ret = 1;
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
      ret = get_log(fs, coll, pgid, info, log, missing);
      if (ret > 0)
          goto out;

      formatter->open_object_section("log");
      log.dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
      formatter->open_object_section("missing");
      missing.dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    } else if (op == "rm-past-intervals") {
      ObjectStore::Transaction tran;
      ObjectStore::Transaction *t = &tran;

      cout << "Remove past-intervals " << past_intervals << std::endl;

      past_intervals.clear();
      ret = write_info(*t, map_epoch, info, struct_ver, past_intervals);

      if (ret == 0) {
        fs->apply_transaction(*t);
        cout << "Removal succeeded" << std::endl;
      }
    } else {
      cerr << "Must provide --op (info, log, remove, export, import, list, list-lost, fix-lost, list-pgs, rm-past-intervals)"
	<< std::endl;
      usage(desc);
    }
  } else {
    cerr << "PG '" << pgid << "' not found" << std::endl;
    ret = 1;
  }

out:
  if (fs->umount() < 0) {
    cerr << "umount failed" << std::endl;
    return 1;
  }

  return (ret != 0);
}
