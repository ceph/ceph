// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "kvstore_tool.h"

#include <iostream>

#include "common/errno.h"
#include "common/url_escape.h"
#include "common/pretty_binary.h"
#include "include/buffer.h"
#include "kv/KeyValueDB.h"
#include "kv/KeyValueHistogram.h"

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
    KeyValueDB *db_ptr;
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
                             ostream *out)
{
  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();

  if (prefix.empty())
    iter->seek_to_first();
  else
    iter->seek_to_first(prefix);

  uint32_t crc = -1;

  while (iter->valid()) {
    pair<string,string> rk = iter->raw_key();
    if (!prefix.empty() && (rk.first != prefix))
      break;

    if (out)
      *out << url_escape(rk.first) << "\t" << url_escape(rk.second);
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
    }
    iter->next();
  }

  return crc;
}

void StoreTool::list(const string& prefix, const bool do_crc,
                     const bool do_value_dump)
{
  traverse(prefix, do_crc, do_value_dump,& std::cout);
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

bufferlist StoreTool::get(const string& prefix,
			  const string& key,
			  bool& exists)
{
  ceph_assert(!prefix.empty() && !key.empty());

  map<string,bufferlist> result;
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
  map<string,uint64_t> extras;
  uint64_t s = db->get_estimated_size(extras);
  for (auto& [name, size] : extras) {
    std::cout << name << " - " << size << std::endl;
  }
  std::cout << "total: " << s << std::endl;
  return s;
}

bool StoreTool::set(const string &prefix, const string &key, bufferlist &val)
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

void StoreTool::print_summary(const uint64_t total_keys, const uint64_t total_size,
                              const uint64_t total_txs, const string& store_path,
                              const string& other_path, const int duration) const
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
  std::cout <<  ostr.str() << std::endl;
  delete f;
  return ret;
}

//Itrerates through the db and collects the stats
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
  std::cout << __func__ << " finished in " << duration << " seconds" << std::endl;
  return 0;
}

int StoreTool::copy_store_to(const string& type, const string& other_path,
                             const int num_keys_per_tx,
                             const string& other_type)
{
  if (num_keys_per_tx <= 0) {
    std::cerr << "must specify a number of keys/tx > 0" << std::endl;
    return -EINVAL;
  }

  // open or create a RocksDB store at @p other_path
  boost::scoped_ptr<KeyValueDB> other;
  KeyValueDB *other_ptr = KeyValueDB::create(g_ceph_context,
					     other_type,
					     other_path);
  if (int err = other_ptr->create_and_open(std::cerr); err < 0) {
    return err;
  }
  other.reset(other_ptr);

  KeyValueDB::WholeSpaceIterator it = db->get_wholespace_iterator();
  it->seek_to_first();
  uint64_t total_keys = 0;
  uint64_t total_size = 0;
  uint64_t total_txs = 0;

  auto duration = [start=coarse_mono_clock::now()] {
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
              << " keys so far (" << byte_u_t(total_size) << ")"
              << std::endl;

  } while (it->valid());

  print_summary(total_keys, total_size, total_txs, store_path, other_path,
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
