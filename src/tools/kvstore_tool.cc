// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "kvstore_tool.h"

#include <charconv>
#include <cstring>
#include <iostream>
#include <set>
#include <utility>

#include "common/Formatter.h"
#include "common/errno.h"
#include "common/hobject.h"
#include "common/pretty_binary.h"
#include "common/sharedptr_registry.hpp"
#include "common/url_escape.h"
#include "include/Context.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/object.h"
#include "kv/KeyValueDB.h"
#include "kv/KeyValueHistogram.h"
#include "os/bluestore/bluestore_types.h"
#include "osd/OSDMap.h"
#include "osd/osd_types_fmt.h"


using namespace std;

StoreTool::StoreTool(const string& type,
		     const string& path,
		     bool to_repair,
		     bool need_stats)
    : store_path(path)
{

  if (need_stats) {
    g_conf()->rocksdb_perf = true;
    g_conf()->rocksdb_collect_compaction_stats = true;
  }

  if (type == "bluestore-kv") {
#ifdef WITH_BLUESTORE
    if (load_bluestore(path, to_repair) != 0)
      exit(1);
#else
    cerr << "bluestore not compiled in" << std::endl;
    exit(1);
#endif
  } else {
    auto db_ptr = KeyValueDB::create(g_ceph_context, type, path);
    if (!to_repair) {
      if (int r = db_ptr->open(std::cerr); r < 0) {
	cerr << "failed to open type " << type << " path " << path << ": "
	     << cpp_strerror(r) << std::endl;
	exit(1);
      }
    }
    db.reset(db_ptr);
  }
}

int StoreTool::load_bluestore(const string& path, bool to_repair)
{
  auto bluestore = new BlueStore(g_ceph_context, path);
  KeyValueDB* db_ptr;
  int r = bluestore->open_db_environment(&db_ptr, to_repair);
  if (r < 0) {
    return -EINVAL;
  }
  db = decltype(db){db_ptr, Deleter(bluestore)};
  return 0;
}

uint32_t StoreTool::traverse(const string& prefix,
			     const bool do_crc,
			     const bool do_value_dump,
			     ostream* out)
{
  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();

  if (prefix.empty())
    iter->seek_to_first();
  else
    iter->seek_to_first(prefix);

  uint32_t crc = -1;

  while (iter->valid()) {
    pair<string, string> rk = iter->raw_key();
    if (!prefix.empty() && (rk.first != prefix))
      break;

    if (out)
      *out << url_escape(rk.first) << "\t" << url_escape(rk.second);
    if (out)
      *out << "[" << rk.first << "]\t[" << rk.second << "]\n";
    if (do_crc) {
      bufferlist bl;
      bl.append(rk.first);
      bl.append(rk.second);
      bl.append(iter->value());

      crc = bl.crc32c(crc);
      if (out) {
	*out << "\t" << bl.crc32c(0);
      }
    }
    if (out)
      *out << std::endl;
    if (out && do_value_dump) {
      bufferptr bp = iter->value_as_ptr();
      bufferlist value;
      value.append(bp);
      ostringstream os;
      value.hexdump(os);
      std::cout << os.str() << std::endl;
      *out << os.str() << std::endl;
    }
    iter->next();
  }

  return crc;
}

static bool snapmapper_entry(std::string k)
{
  return k.find("SNA_") != string::npos || k.find("OBJ_") != string::npos ||
	 k.find("MAP_") != string::npos;
}
using ceph::decode;

void hobject_t::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  if (struct_v >= 1)
    decode(key, bl);
  decode(oid, bl);
  decode(snap, bl);
  decode(hash, bl);
  if (struct_v >= 2)
    decode(max, bl);
  else
    max = false;
  if (struct_v >= 4) {
    decode(nspace, bl);
    decode(pool, bl);
    // for compat with hammer, which did not handle the transition
    // from pool -1 -> pool INT64_MIN for MIN properly.  this object
    // name looks a bit like a pgmeta object for the meta collection,
    // but those do not ever exist (and is_pgmeta() pool >= 0).
    if (pool == -1 && snap == 0 && hash == 0 && !max && oid.name.empty()) {
      pool = INT64_MIN;
      ceph_assert(is_min());
    }

    // for compatibility with some earlier verisons which might encoded
    // a non-canonical max object
    if (max) {
      *this = hobject_t::get_max();
    }
  }
  DECODE_FINISH(bl);
  build_hash_cache();
}

struct object_snaps {
  hobject_t oid;
  std::set<snapid_t> snaps;
  object_snaps(hobject_t oid, const std::set<snapid_t>& snaps)
      : oid(oid)
      , snaps(snaps)
  {}
  object_snaps() {}
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p)
  {
    DECODE_START(1, p);
    decode(oid, p);
    __u32 n;
    decode(n, p);
    snaps.clear();
    while (n--) {
      uint64_t v;
      decode(v, p);
      snaps.insert(v);
    }
    DECODE_FINISH(p);
  }
};


struct Mapping {
  snapid_t snap;
  hobject_t hoid;
  explicit Mapping(const std::pair<snapid_t, hobject_t>& in)
      : snap(in.first)
      , hoid(in.second)
  {}
  Mapping() : snap(0) {}
  void encode(ceph::buffer::list& bl) const
  {
    ENCODE_START(1, 1, bl);
    encode(snap, bl);
    encode(hoid, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(snap, bl);
    decode(hoid, bl);
    DECODE_FINISH(bl);
  }
};

static void decode_sna_key(string key, std::ostream* o)
{
  // two possible formats (w./o the shard id):
  //   "SNA_2_0000000000000001_0000000000000002.347FB131.7.xxxx"
  //   "SNA_2_0000000000000001_.a_0000000000000002.347FB131.7.xxxx";
  // ZZ SNA_ =========== SNA_2_0000000000000001_.1_0000000000000002.A3F603B4.1.eeee..
  int plid{0};
  long unsigned int snpid;
  long int shardid{-2};
  int64_t snap;
  char oname[1024];
  unsigned int hash{0};
  long unsigned int pool;

  *o << "\nZZ SNA_ =========== " << key << std::endl;

  auto n = sscanf(key.c_str(),
		  "SNA_%d_%016X_%lx.%8X.%lx.%s",
		  &plid,
		  &snpid,
		  &pool,
		  &hash,
		  &snap,
		  oname);
  if (n != 6) {
    // we do have shard-id to parse
    n = sscanf(key.c_str(),
	       "SNA_%d_%016X_.%lX_%lx.%8X.%lx.%s",
	       &plid,
	       &snpid,
	       &shardid,
	       &pool,
	       &hash,
	       &snap,
	       oname);

    if (n != 7) {
      *o << "invalid SNA key " << n << std::endl;
      return;
    }
  }
  *o << fmt::format(
    "ZZ sna-rec: pid:{} snpid:{} shardid:{:x} [pool:{} hash:{:x} snap:{} nm:{}]\n",
    plid,
    snpid,
    shardid,
    pool,
    hash,
    snap,
    oname);
}

// ZZ OBJ_ =========== OBJ_.1_0000000000000002.A3F603B4.1.eeee..
// ZZ obj-rec: shardid:-ffffffff [pool:2 hash:a3f603b4 clone:1 nm:eeee..]

static void decode_obj_key(string key, std::ostream* o)
{
  // two possible formats (w./o the shard id):
  //   "OBJ_0000000000000002.347FB131.7.xxxx"
  //   "OBJ_.a_0000000000000002.347FB131.7.xxxx";
  long int shardid{-2};
  int64_t clone;
  char oname[1024];
  unsigned int hash{0};
  long unsigned int pool;

  *o << "\nZZ OBJ_ =========== " << key << std::endl;

  auto n =
    sscanf(key.c_str(), "OBJ_%lx.%8X.%lx.%s", &pool, &hash, &clone, oname);
  if (n != 4) {
    // we do have shard-id to parse
    n = sscanf(key.c_str(),
	       "OBJ_.%lx_%lx.%8X.%lx.%s",
	       &shardid,
	       &pool,
	       &hash,
	       &clone,
	       oname);

    if (n != 5) {
      *o << "invalid OBJ key " << n << std::endl;
      return;
    }
  }
  *o << fmt::format("ZZ obj-rec: shardid:{:x} [pool:{} hash:{:x} clone:{} nm:{}]\n",
		    shardid,
		    pool,
		    hash,
		    clone,
		    oname);
}


std::pair<snapid_t, hobject_t> from_raw(
  const pair<std::string, bufferlist>& image)
{
  using ceph::decode;
  bufferlist bl(image.second);
  auto bp = bl.cbegin();

  Mapping m;
  m.decode(bp);

  return std::make_pair(m.snap, m.hoid);
}


static void parse_mp_kv(string k,
			ceph::bufferlist v,
			std::ostream* o,
			std::ofstream* bo)
{
  if (k.find("SNA_") != string::npos) {
    // k: "SNA_"
    //   + ("%lld" % poolid)
    //   + "_"
    //   + ("%016x" % snapid)
    //   + "_"
    //   + (".%x" % shard_id)
    //   + "_"
    //   + hobject_t::to_str() ("%llx.%8x.%lx.name...." % pool, hash, snap)
    // -> SnapMapping::Mapping { snap, hoid }


    decode_sna_key(k.substr(k.find("SNA")), o);

    auto [snap, hoid] = from_raw(make_pair("", v));
    /* for the IDE */ hobject_t ho = hoid;

    *o << "\n///sna// key: " << k << " -//- snap: " << snap.val
       << " hobject:" << ho << " p:" << ho.pool << std::endl;

    // and the grepable version:
    *o << fmt::format("ZZ sna-recv snap:{} hoid:{}\n", snap.val, ho);

  } else if (k.find("OBJ_") != string::npos) {
    //  "OBJ_" +
    //   + (".%x" % shard_id)
    //   + hobject_t::to_str()
    //    -> SnapMapper::object_snaps { oid, set<snapid_t> }

    decode_obj_key(k.substr(k.find("OBJ")), o);
    auto bp = v.cbegin();
    object_snaps os;
    try {
      os.decode(bp);
    } catch (...) {
      *o << "decode failed" << std::endl;
    }

    *o << "\n///obj// key: " << k << " -//- oid: " << os.oid << std::endl;
    *o << "\tsnaps -> " << os.snaps << std::endl;

    // and the grepable version:
    *o << fmt::format("ZZ obj-recv hoid:{} snaps:{}\n", os.oid, os.snaps);

  } else if (k.find("MAP_") != string::npos) {
    //   "MAP_"
    //   + ("%016x" % snapid)
    //   + "_"
    //   + (".%x" % shard_id)
    //   + "_"
    //   + hobject_t::to_str() ("%llx.%8x.%lx.name...." % pool, hash, snap)
    //   -> SnapMapping::Mapping { snap, hoid }

  } else {
  }
}


bufferlist StoreTool::mapmapper(const string& known_prfx, bool do_value_dump)
{
  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();

  if (known_prfx.empty())
    iter->seek_to_first();
  else
    iter->seek_to_first(known_prfx);

  bufferlist ret;

  while (iter->valid()) {
    // pair<string, string> rk = iter->raw_key();
    auto [prefix, key] = iter->raw_key();
    if (!known_prfx.empty() && (prefix != known_prfx))
      break;

    // related to the snap mapper?
    if (!snapmapper_entry(key)) {
      std::cout << "NOT RLVNT [" << key << "]\n";
      iter->next();
      continue;
    }

    // if (out)
    std::cout << "[" << key << "]\n";
    if (do_value_dump) {
      bufferptr bp = iter->value_as_ptr();
      bufferlist this_value;
      this_value.append(bp);

      ostringstream os1;
      parse_mp_kv(key, this_value, &os1, nullptr);
      std::cout << os1.str() << std::endl;

      ostringstream os;
      this_value.hexdump(os);
      std::cout << os.str() << std::endl;
      ret.append(bp);
    }
    iter->next();
  }

  return ret;
}


void StoreTool::corrupt_snaps(string keypart)
{
  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();
  iter->seek_to_first("p");

  bufferlist ret;

  while (iter->valid()) {
    pair<string, string> rk = iter->raw_key();

    // is this the one we wish to corrupt?
    if ((rk.second.find("SNA_") != string::npos) &&
	(rk.second.find(keypart) != string::npos)) {
      /*

	      the key should look like this:
	      [something]SNA_[poolid]_[?]_[?].[hobject]

	      truncate at the 3rd underscore.

      */


      // get the value
      bufferptr bp = iter->value_as_ptr();
      bufferlist bl;
      bl.append(bp);

      // better do RE here, but for now:
      auto mod_s = rk.second;
      mod_s.resize(
	mod_s.find("_", mod_s.find("_", mod_s.find("SNA_") + 4) + 1) + 1);
      set(rk.first, mod_s, bl);
      rm(rk.first, rk.second);
      break;
    }
    iter->next();
  }
}


// TODO remove the duplicate code here (the whole function...)
static int decode_sna_key_snap(string key, std::ostream* o)
{
  // two possible formats (w./o the shard id):
  //   "SNA_2_0000000000000001_0000000000000002.347FB131.7.xxxx"
  //   "SNA_2_0000000000000001_.a_0000000000000002.347FB131.7.xxxx";
  // ZZ SNA_ =========== SNA_2_0000000000000001_.1_0000000000000002.A3F603B4.1.eeee..
  int plid{0};
  long unsigned int snpid;
  long int shardid{-2};
  int64_t clone;
  char oname[1024];
  unsigned int hash{0};
  long unsigned int pool;

  *o << "\nZZ SNA_ =========== " << key << std::endl;

  auto n = sscanf(key.c_str(),
		  "SNA_%d_%016X_%lx.%8X.%lx.%s",
		  &plid,
		  &snpid,
		  &pool,
		  &hash,
		  &clone,
		  oname);
  if (n != 6) {
    // we do have shard-id to parse
    n = sscanf(key.c_str(),
	       "SNA_%d_%016X_.%lX_%lx.%8X.%lx.%s",
	       &plid,
	       &snpid,
	       &shardid,
	       &pool,
	       &hash,
	       &clone,
	       oname);

    if (n != 7) {
      *o << "invalid SNA key " << n << std::endl;
      return 999;
    }
  }
  *o << fmt::format(
    "ZZ sna-rec: pid:{} snpid:{} shardid:{:x} [pool:{} hash:{:x} clone:{} nm:{}]\n",
    plid,
    snpid,
    shardid,
    pool,
    hash,
    clone,
    oname);

  return snpid;
}

void StoreTool::corrupt_snap_v2(string keypart, int snp)
{
  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();
  iter->seek_to_first("p");

  bufferlist ret;

  while (iter->valid()) {
    auto [prefix, key] = iter->raw_key();

    // is this the one we wish to corrupt?
    if ((key.find("SNA_") != string::npos) &&
	(key.find(keypart) != string::npos)) {
      /*

	      the key should look like this:
	      [something]SNA_[poolid]_[?]_[?].[hobject]

	      truncate at the 3rd underscore.

      */
      ostringstream os1;
      auto ksnap = decode_sna_key_snap(key.substr(key.find("SNA")), &os1);
      std::cout << os1.str() << std::endl;
      if (ksnap != snp) {
        iter->next();
        continue;
        }

      // get the value
      bufferptr bp = iter->value_as_ptr();
      bufferlist bl;
      bl.append(bp);

      // better do RE here, but for now:
      auto mod_s = key;
      mod_s.resize(
	mod_s.find("_", mod_s.find("_", mod_s.find("SNA_") + 4) + 1) + 1);
      set(prefix, mod_s, bl);
      rm(prefix, key);
      break;
    }
    iter->next();
  }
}

void StoreTool::corrupt_obj_entries(string keypart)
{
  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();
  iter->seek_to_first("p");

  bufferlist ret;

  while (iter->valid()) {
    pair<string, string> rk = iter->raw_key();

    // is this the one we wish to corrupt?
    if ((rk.second.find("OBJ_") != string::npos) &&
	(rk.second.find(keypart) != string::npos)) {

      std::cout << "corrupting OBJ_ entry " << rk.second << std::endl;
      // get the value
      bufferptr bp = iter->value_as_ptr();
      bufferlist bl;
      bl.append(bp);

      auto mod_s = rk.second;
      mod_s.resize(mod_s.find("OBJ_") + 3);
      set(rk.first, mod_s, bl);
      rm(rk.first, rk.second);
      break;
    }
    iter->next();
  }
}


void StoreTool::list(const string& prefix,
		     const bool do_crc,
		     const bool do_value_dump)
{
  traverse(prefix, do_crc, do_value_dump, &std::cout);
}

bool StoreTool::exists(const string& prefix)
{
  ceph_assert(!prefix.empty());
  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();
  iter->seek_to_first(prefix);
  return (iter->valid() && (iter->raw_key().first == prefix));
}

bool StoreTool::exists(const string& prefix, const string& key)
{
  ceph_assert(!prefix.empty());

  if (key.empty()) {
    return exists(prefix);
  }
  bool exists = false;
  get(prefix, key, exists);
  return exists;
}

bufferlist StoreTool::get(const string& prefix, const string& key, bool& exists)
{
  ceph_assert(!prefix.empty() && !key.empty());

  map<string, bufferlist> result;
  std::set<std::string> keys;
  keys.insert(key);
  db->get(prefix, keys, &result);

  if (result.count(key) > 0) {
    exists = true;
    return result[key];
  } else {
    exists = false;
    return bufferlist();
  }
}

uint64_t StoreTool::get_size()
{
  map<string, uint64_t> extras;
  uint64_t s = db->get_estimated_size(extras);
  for (auto& [name, size] : extras) {
    std::cout << name << " - " << size << std::endl;
  }
  std::cout << "total: " << s << std::endl;
  return s;
}

bool StoreTool::set(const string& prefix, const string& key, bufferlist& val)
{
  ceph_assert(!prefix.empty());
  ceph_assert(!key.empty());
  ceph_assert(val.length() > 0);

  KeyValueDB::Transaction tx = db->get_transaction();
  tx->set(prefix, key, val);
  int ret = db->submit_transaction_sync(tx);

  return (ret == 0);
}

bool StoreTool::rm(const string& prefix, const string& key)
{
  ceph_assert(!prefix.empty());
  ceph_assert(!key.empty());

  KeyValueDB::Transaction tx = db->get_transaction();
  tx->rmkey(prefix, key);
  int ret = db->submit_transaction_sync(tx);

  return (ret == 0);
}

bool StoreTool::rm_prefix(const string& prefix)
{
  ceph_assert(!prefix.empty());

  KeyValueDB::Transaction tx = db->get_transaction();
  tx->rmkeys_by_prefix(prefix);
  int ret = db->submit_transaction_sync(tx);

  return (ret == 0);
}

void StoreTool::print_summary(const uint64_t total_keys,
			      const uint64_t total_size,
			      const uint64_t total_txs,
			      const string& store_path,
			      const string& other_path,
			      const int duration) const
{
  std::cout << "summary:" << std::endl;
  std::cout << "  copied " << total_keys << " keys" << std::endl;
  std::cout << "  used " << total_txs << " transactions" << std::endl;
  std::cout << "  total size " << byte_u_t(total_size) << std::endl;
  std::cout << "  from '" << store_path << "' to '" << other_path << "'"
	    << std::endl;
  std::cout << "  duration " << duration << " seconds" << std::endl;
}

int StoreTool::print_stats() const
{
  ostringstream ostr;
  Formatter* f = Formatter::create("json-pretty", "json-pretty", "json-pretty");
  int ret = -1;
  if (g_conf()->rocksdb_perf) {
    db->get_statistics(f);
    ostr << "db_statistics ";
    f->flush(ostr);
    ret = 0;
  } else {
    ostr << "db_statistics not enabled";
    f->flush(ostr);
  }
  std::cout << ostr.str() << std::endl;
  delete f;
  return ret;
}

// Itrerates through the db and collects the stats
int StoreTool::build_size_histogram(const string& prefix0) const
{
  ostringstream ostr;
  Formatter* f = Formatter::create("json-pretty", "json-pretty", "json-pretty");

  const size_t MAX_PREFIX = 256;
  uint64_t num[MAX_PREFIX] = {0};

  size_t max_key_size = 0, max_value_size = 0;
  uint64_t total_key_size = 0, total_value_size = 0;
  size_t key_size = 0, value_size = 0;
  KeyValueHistogram hist;

  auto start = coarse_mono_clock::now();

  auto iter = db->get_iterator(prefix0, KeyValueDB::ITERATOR_NOCACHE);
  iter->seek_to_first();
  while (iter->valid()) {
    pair<string, string> key(iter->raw_key());
    key_size = key.first.size() + key.second.size();
    value_size = iter->value().length();
    hist.value_hist[hist.get_value_slab(value_size)]++;
    max_key_size = std::max(max_key_size, key_size);
    max_value_size = std::max(max_value_size, value_size);
    total_key_size += key_size;
    total_value_size += value_size;


    unsigned prefix = key.first[0];
    ceph_assert(prefix < MAX_PREFIX);
    num[prefix]++;
    hist.update_hist_entry(hist.key_hist, key.first, key_size, value_size);
    iter->next();
  }

  ceph::timespan duration = coarse_mono_clock::now() - start;
  f->open_object_section("rocksdb_key_value_stats");
  for (size_t i = 0; i < MAX_PREFIX; ++i) {
    if (num[i]) {
      string key = "Records for prefix: ";
      key += pretty_binary_string(string(1, char(i)));
      f->dump_unsigned(key, num[i]);
    }
  }
  f->dump_unsigned("max_key_size", max_key_size);
  f->dump_unsigned("max_value_size", max_value_size);
  f->dump_unsigned("total_key_size", total_key_size);
  f->dump_unsigned("total_value_size", total_value_size);
  hist.dump(f);
  f->close_section();

  f->flush(ostr);
  delete f;

  std::cout << ostr.str() << std::endl;
  std::cout << __func__ << " finished in " << duration << " seconds"
	    << std::endl;
  return 0;
}

int StoreTool::copy_store_to(const string& type,
			     const string& other_path,
			     const int num_keys_per_tx,
			     const string& other_type)
{
  if (num_keys_per_tx <= 0) {
    std::cerr << "must specify a number of keys/tx > 0" << std::endl;
    return -EINVAL;
  }

  // open or create a leveldb store at @p other_path
  boost::scoped_ptr<KeyValueDB> other;
  KeyValueDB* other_ptr =
    KeyValueDB::create(g_ceph_context, other_type, other_path);
  if (int err = other_ptr->create_and_open(std::cerr); err < 0) {
    return err;
  }
  other.reset(other_ptr);

  KeyValueDB::WholeSpaceIterator it = db->get_wholespace_iterator();
  it->seek_to_first();
  uint64_t total_keys = 0;
  uint64_t total_size = 0;
  uint64_t total_txs = 0;

  auto duration = [start = coarse_mono_clock::now()] {
    const auto now = coarse_mono_clock::now();
    auto seconds = std::chrono::duration<double>(now - start);
    return seconds.count();
  };

  do {
    int num_keys = 0;

    KeyValueDB::Transaction tx = other->get_transaction();

    while (it->valid() && num_keys < num_keys_per_tx) {
      auto [prefix, key] = it->raw_key();
      bufferlist v = it->value();
      tx->set(prefix, key, v);

      num_keys++;
      total_size += v.length();

      it->next();
    }

    total_txs++;
    total_keys += num_keys;

    if (num_keys > 0)
      other->submit_transaction_sync(tx);

    std::cout << "ts = " << duration() << "s, copied " << total_keys
	      << " keys so far (" << byte_u_t(total_size) << ")" << std::endl;

  } while (it->valid());

  print_summary(total_keys,
		total_size,
		total_txs,
		store_path,
		other_path,
		duration());

  return 0;
}

void StoreTool::compact()
{
  db->compact();
}

void StoreTool::compact_prefix(const string& prefix)
{
  db->compact_prefix(prefix);
}

void StoreTool::compact_range(const string& prefix,
			      const string& start,
			      const string& end)
{
  db->compact_range(prefix, start, end);
}

int StoreTool::destructive_repair()
{
  return db->repair(std::cout);
}
