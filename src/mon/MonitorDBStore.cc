// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/

#include "MonitorDBStore.h"

#include <sstream>

#include "common/blkdev.h"
#include "common/Clock.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/version.h"
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "include/encoding_list.h"
#include "include/encoding_string.h"
#include "global/global_context.h" // for g_conf()

std::string MonitorDBStore::get_devname() {
  char devname[4096] = {0}, partition[4096];
  get_device_by_path(path.c_str(), partition, devname,
                     sizeof(devname));
  return devname;
}

void MonitorDBStore::Op::encode(ceph::buffer::list& encode_bl) const {
  ENCODE_START(2, 1, encode_bl);
  encode(type, encode_bl);
  encode(prefix, encode_bl);
  encode(key, encode_bl);
  encode(bl, encode_bl);
  encode(endkey, encode_bl);
  ENCODE_FINISH(encode_bl);
}

void MonitorDBStore::Op::decode(ceph::buffer::list::const_iterator& decode_bl) {
  DECODE_START(2, decode_bl);
  decode(type, decode_bl);
  decode(prefix, decode_bl);
  decode(key, decode_bl);
  decode(bl, decode_bl);
  if (struct_v >= 2)
    decode(endkey, decode_bl);
  DECODE_FINISH(decode_bl);
}

void MonitorDBStore::Op::dump(ceph::Formatter *f) const {
  f->dump_int("type", type);
  f->dump_string("prefix", prefix);
  f->dump_string("key", key);
  if (endkey.length()) {
    f->dump_string("endkey", endkey);
  }
}

std::list<MonitorDBStore::Op> MonitorDBStore::Op::generate_test_instances() {
  std::list<Op> ls;
  ls.emplace_back();
  // we get coverage here from the Transaction instances
  return ls;
}

void MonitorDBStore::Transaction::put(const std::string& prefix, const std::string& key, const ceph::buffer::list& bl) {
  ops.push_back(Op(OP_PUT, prefix, key, bl));
  ++keys;
  bytes += ops.back().approx_size();
}

void MonitorDBStore::Transaction::put(const std::string& prefix, version_t ver, const ceph::buffer::list& bl) {
  std::ostringstream os;
  os << ver;
  put(prefix, os.str(), bl);
}

void MonitorDBStore::Transaction::put(const std::string& prefix, const std::string& key, version_t ver) {
  using ceph::encode;
  ceph::buffer::list bl;
  encode(ver, bl);
  put(prefix, key, bl);
}

void MonitorDBStore::Transaction::erase(const std::string& prefix, const std::string& key) {
  ops.push_back(Op(OP_ERASE, prefix, key));
  ++keys;
  bytes += ops.back().approx_size();
}

void MonitorDBStore::Transaction::erase(const std::string& prefix, version_t ver) {
  std::ostringstream os;
  os << ver;
  erase(prefix, os.str());
}

void MonitorDBStore::Transaction::erase_range(const std::string& prefix, const std::string& begin,
                                              const std::string& end) {
  ops.push_back(Op(OP_ERASE_RANGE, prefix, begin, end));
  ++keys;
  bytes += ops.back().approx_size();
}

void MonitorDBStore::Transaction::compact_prefix(const std::string& prefix) {
  ops.push_back(Op(OP_COMPACT, prefix, {}));
}

void MonitorDBStore::Transaction::compact_range(const std::string& prefix, const std::string& start,
                                                const std::string& end) {
  ops.push_back(Op(OP_COMPACT, prefix, start, end));
}

void MonitorDBStore::Transaction::encode(ceph::buffer::list& bl) const {
  ENCODE_START(2, 1, bl);
  encode(ops, bl);
  encode(bytes, bl);
  encode(keys, bl);
  ENCODE_FINISH(bl);
}

void MonitorDBStore::Transaction::decode(ceph::buffer::list::const_iterator& bl) {
  DECODE_START(2, bl);
  decode(ops, bl);
  if (struct_v >= 2) {
    decode(bytes, bl);
    decode(keys, bl);
  }
  DECODE_FINISH(bl);
}

std::list<MonitorDBStore::Transaction> MonitorDBStore::Transaction::generate_test_instances() {
  std::list<Transaction> ls;
  ls.emplace_back();
  ls.emplace_back();
  ceph::buffer::list bl;
  bl.append("value");
  ls.back().put("prefix", "key", bl);
  ls.back().erase("prefix2", "key2");
  ls.back().erase_range("prefix3", "key3", "key4");
  ls.back().compact_prefix("prefix3");
  ls.back().compact_range("prefix4", "from", "to");
  return ls;
}

void MonitorDBStore::Transaction::append(TransactionRef other) {
  ops.splice(ops.end(), other->ops);
  keys += other->keys;
  bytes += other->bytes;
}

void MonitorDBStore::Transaction::append_from_encoded(ceph::buffer::list& bl) {
  auto other(std::make_shared<Transaction>());
  auto it = bl.cbegin();
  other->decode(it);
  append(other);
}

void MonitorDBStore::Transaction::dump(ceph::Formatter *f, bool dump_val) const {
  f->open_object_section("transaction");
  f->open_array_section("ops");
  int op_num = 0;
  for (auto it = ops.begin(); it != ops.end(); ++it) {
    const Op& op = *it;
    f->open_object_section("op");
    f->dump_int("op_num", op_num++);
    switch (op.type) {
    case OP_PUT:
    {
      f->dump_string("type", "PUT");
      f->dump_string("prefix", op.prefix);
      f->dump_string("key", op.key);
      f->dump_unsigned("length", op.bl.length());
      if (dump_val) {
        std::ostringstream os;
        op.bl.hexdump(os);
        f->dump_string("bl", os.str());
      }
    }
    break;
    case OP_ERASE:
    {
      f->dump_string("type", "ERASE");
      f->dump_string("prefix", op.prefix);
      f->dump_string("key", op.key);
    }
    break;
    case OP_ERASE_RANGE:
    {
      f->dump_string("type", "ERASE_RANGE");
      f->dump_string("prefix", op.prefix);
      f->dump_string("start", op.key);
      f->dump_string("end", op.endkey);
    }
    break;
    case OP_COMPACT:
    {
      f->dump_string("type", "COMPACT");
      f->dump_string("prefix", op.prefix);
      f->dump_string("start", op.key);
      f->dump_string("end", op.endkey);
    }
    break;
    default:
    {
      f->dump_string("type", "unknown");
      f->dump_unsigned("op_code", op.type);
      break;
    }
    }
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("num_keys", keys);
  f->dump_unsigned("num_bytes", bytes);
  f->close_section();
}

int MonitorDBStore::apply_transaction(MonitorDBStore::TransactionRef t) {
  KeyValueDB::Transaction dbt = db->get_transaction();

  if (do_dump) {
    if (!g_conf()->mon_debug_dump_json) {
      ceph::buffer::list bl;
      t->encode(bl);
      bl.write_fd(dump_fd_binary);
    } else {
      t->dump(&dump_fmt, true);
      dump_fmt.flush(dump_fd_json);
      dump_fd_json.flush();
    }
  }

  std::list<std::pair<std::string, std::pair<std::string,std::string>>> compact;
  for (auto it = t->ops.begin(); it != t->ops.end(); ++it) {
    const Op& op = *it;
    switch (op.type) {
    case Transaction::OP_PUT:
      dbt->set(op.prefix, op.key, op.bl);
      break;
    case Transaction::OP_ERASE:
      dbt->rmkey(op.prefix, op.key);
      break;
    case Transaction::OP_ERASE_RANGE:
      dbt->rm_range_keys(op.prefix, op.key, op.endkey);
      break;
    case Transaction::OP_COMPACT:
      compact.push_back(make_pair(op.prefix, make_pair(op.key, op.endkey)));
      break;
    default:
      derr << __func__ << " unknown op type " << op.type << dendl;
      ceph_abort();
      break;
    }
  }
  int r = db->submit_transaction_sync(dbt);
  if (r >= 0) {
    while (!compact.empty()) {
      if (compact.front().second.first == std::string() &&
          compact.front().second.second == std::string())
        db->compact_prefix_async(compact.front().first);
      else
        db->compact_range_async(compact.front().first, compact.front().second.first, compact.front().second.second);
      compact.pop_front();
    }
  } else {
    ceph_abort_msg("failed to write to db");
  }
  return r;
}

struct MonitorDBStore::C_DoTransaction : public Context {
  MonitorDBStore *store;
  MonitorDBStore::TransactionRef t;
  Context *oncommit;
  C_DoTransaction(MonitorDBStore *s, MonitorDBStore::TransactionRef t,
                  Context *f)
    : store(s), t(t), oncommit(f)
    {}
  void finish(int r) override {
    /* The store serializes writes.  Each transaction is handled
     * sequentially by the io_work Finisher.  If a transaction takes longer
     * to apply its state to permanent storage, then no other transaction
     * will be handled meanwhile.
     *
     * We will now randomly inject random delays.  We can safely sleep prior
     * to applying the transaction as it won't break the model.
     */
    double delay_prob = g_conf()->mon_inject_transaction_delay_probability;
    if (delay_prob && (rand() % 10000 < delay_prob * 10000.0)) {
      utime_t delay;
      double delay_max = g_conf()->mon_inject_transaction_delay_max;
      delay.set_from_double(delay_max * (double)(rand() % 10000) / 10000.0);
      lsubdout(g_ceph_context, mon, 1)
        << "apply_transaction will be delayed for " << delay
        << " seconds" << dendl;
      delay.sleep();
    }
    int ret = store->apply_transaction(t);
    oncommit->complete(ret);
  }
};

void MonitorDBStore::queue_transaction(MonitorDBStore::TransactionRef t,
                                       Context *oncommit) {
  io_work.queue(new C_DoTransaction(this, t, oncommit));
}

__u32 MonitorDBStore::StoreIteratorImpl::crc() {
  if (g_conf()->mon_sync_debug)
    return crc_bl.crc32c(0);
  return 0;
}

void MonitorDBStore::WholeStoreIteratorImpl::get_chunk_tx(TransactionRef tx, uint64_t max_bytes,
                                                          uint64_t max_keys) {
  using ceph::encode;
  ceph_assert(done == false);
  ceph_assert(iter->valid() == true);

  while (iter->valid()) {
    std::string prefix(iter->raw_key().first);
    std::string key(iter->raw_key().second);
    if (sync_prefixes.count(prefix)) {
      ceph::buffer::list value = iter->value();
      if (tx->empty() ||
          (tx->get_bytes() + value.length() + key.size() +
           prefix.size() < max_bytes &&
           tx->get_keys() < max_keys)) {
        // NOTE: putting every key in a separate transaction is
        // questionable as far as efficiency goes
        auto tmp(std::make_shared<Transaction>());
        tmp->put(prefix, key, value);
        tx->append(tmp);
        if (g_conf()->mon_sync_debug) {
          encode(prefix, crc_bl);
          encode(key, crc_bl);
          encode(value, crc_bl);
        }
      } else {
        last_key.first = prefix;
        last_key.second = key;
        return;
      }
    }
    iter->next();
  }
  ceph_assert(iter->valid() == false);
  done = true;
}

std::pair<std::string,std::string> MonitorDBStore::WholeStoreIteratorImpl::get_next_key() {
  ceph_assert(iter->valid());

  for (; iter->valid(); iter->next()) {
    std::pair<std::string,std::string> r = iter->raw_key();
    if (sync_prefixes.count(r.first) > 0) {
      iter->next();
      return r;
    }
  }
  return std::pair<std::string,std::string>();
}

MonitorDBStore::Synchronizer MonitorDBStore::get_synchronizer(std::pair<std::string,std::string> &key,
                              std::set<std::string> &prefixes) {
  KeyValueDB::WholeSpaceIterator iter;
  iter = db->get_wholespace_iterator();

  if (!key.first.empty() && !key.second.empty())
    iter->upper_bound(key.first, key.second);
  else
    iter->seek_to_first();

  return std::shared_ptr<StoreIteratorImpl>(
    new WholeStoreIteratorImpl(iter, prefixes)
    );
}

KeyValueDB::Iterator MonitorDBStore::get_iterator(const std::string &prefix) {
  ceph_assert(!prefix.empty());
  KeyValueDB::Iterator iter = db->get_iterator(prefix);
  iter->seek_to_first();
  return iter;
}

KeyValueDB::WholeSpaceIterator MonitorDBStore::get_iterator() {
  KeyValueDB::WholeSpaceIterator iter;
  iter = db->get_wholespace_iterator();
  iter->seek_to_first();
  return iter;
}

int MonitorDBStore::get(const std::string& prefix, const std::string& key, ceph::buffer::list& bl) {
  ceph_assert(bl.length() == 0);
  return db->get(prefix, key, &bl);
}

int MonitorDBStore::get(const std::string& prefix, const version_t ver, ceph::buffer::list& bl) {
  std::ostringstream os;
  os << ver;
  return get(prefix, os.str(), bl);
}

version_t MonitorDBStore::get(const std::string& prefix, const std::string& key) {
  using ceph::decode;
  ceph::buffer::list bl;
  int err = get(prefix, key, bl);
  if (err < 0) {
    if (err == -ENOENT) // if key doesn't exist, assume its value is 0
      return 0;
    // we're not expecting any other negative return value, and we can't
    // just return a negative value if we're returning a version_t
    generic_dout(0) << "MonitorDBStore::get() error obtaining"
                    << " (" << prefix << ":" << key << "): "
                    << cpp_strerror(err) << dendl;
    ceph_abort_msg("error obtaining key");
  }

  ceph_assert(bl.length());
  version_t ver;
  auto p = bl.cbegin();
  decode(ver, p);
  return ver;
}

bool MonitorDBStore::exists(const std::string& prefix, const std::string& key) {
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  int err = it->lower_bound(key);
  if (err < 0)
    return false;

  return (it->valid() && it->key() == key);
}

bool MonitorDBStore::exists(const std::string& prefix, version_t ver) {
  std::ostringstream os;
  os << ver;
  return exists(prefix, os.str());
}

std::string MonitorDBStore::combine_strings(const std::string& prefix, const std::string& value) {
  std::string out = prefix;
  out.push_back('_');
  out.append(value);
  return out;
}

std::string MonitorDBStore::combine_strings(const std::string& prefix, const version_t ver) {
  std::ostringstream os;
  os << ver;
  return combine_strings(prefix, os.str());
}

int MonitorDBStore::clear_key(const std::string& prefix, const std::string& key) {
  ceph_assert(!prefix.empty());
  ceph_assert(!key.empty());
  KeyValueDB::Transaction dbt = db->get_transaction();
  dbt->rmkey(prefix, key);
  return db->submit_transaction_sync(dbt);
}

void MonitorDBStore::clear(std::set<std::string>& prefixes) {
  KeyValueDB::Transaction dbt = db->get_transaction();

  for (auto iter = prefixes.begin(); iter != prefixes.end(); ++iter) {
    dbt->rmkeys_by_prefix((*iter));
  }
  int r = db->submit_transaction_sync(dbt);
  ceph_assert(r >= 0);
}

void MonitorDBStore::_open(const std::string& kv_type) {
  std::string full_path = get_store_path(path);

  KeyValueDB *db_ptr = KeyValueDB::create(g_ceph_context,
      				    kv_type,
      				    full_path);
  if (!db_ptr) {
    derr << __func__ << " error initializing "
         << kv_type << " db back storage in "
         << full_path << dendl;
    ceph_abort_msg("MonitorDBStore: error initializing keyvaluedb back storage");
  }
  db.reset(db_ptr);

  if (g_conf()->mon_debug_dump_transactions) {
    if (!g_conf()->mon_debug_dump_json) {
      dump_fd_binary = ::open(
        g_conf()->mon_debug_dump_location.c_str(),
        O_CREAT|O_APPEND|O_WRONLY|O_CLOEXEC, 0644);
      if (dump_fd_binary < 0) {
        dump_fd_binary = -errno;
        derr << "Could not open log file, got "
             << cpp_strerror(dump_fd_binary) << dendl;
      }
    } else {
      dump_fmt.reset();
      dump_fmt.open_array_section("dump");
      dump_fd_json.open(g_conf()->mon_debug_dump_location.c_str());
    }
    do_dump = true;
  }
  if (kv_type == "rocksdb")
    db->init(g_conf()->mon_rocksdb_options);
  else
    db->init();
}

int MonitorDBStore::open(std::ostream &out) {
  std::string kv_type;
  int r = read_meta("kv_backend", &kv_type);
  if (r < 0 || kv_type.empty()) {
    // assume old monitors that did not mark the type were RocksDB.
    kv_type = "rocksdb";
    r = write_meta("kv_backend", kv_type);
    if (r < 0)
      return r;
  }
  _open(kv_type);
  r = db->open(out);
  if (r < 0)
    return r;

  // Monitors are few in number, so the resource cost of exposing
  // very detailed stats is low: ramp up the priority of all the
  // KV store's perf counters.  Do this after open, because backend may
  // not have constructed PerfCounters earlier.
  if (db->get_perf_counters()) {
    db->get_perf_counters()->set_prio_adjust(
        PerfCountersBuilder::PRIO_USEFUL - PerfCountersBuilder::PRIO_DEBUGONLY);
  }

  io_work.start();
  is_open = true;
  return 0;
}

int MonitorDBStore::create_and_open(std::ostream &out) {
  int r = write_meta("ceph_version_when_created", pretty_version_to_str());
  if (r < 0)
    return r;

  std::ostringstream created_at;
  utime_t now = ceph_clock_now();
  now.gmtime(created_at);
  r = write_meta("created_at", created_at.str());
  if (r < 0)
    return r;

  // record the type before open
  std::string kv_type;
  r = read_meta("kv_backend", &kv_type);
  if (r < 0) {
    kv_type = g_conf()->mon_keyvaluedb;
    r = write_meta("kv_backend", kv_type);
    if (r < 0)
      return r;
  }
  _open(kv_type);
  r = db->create_and_open(out);
  if (r < 0)
    return r;
  io_work.start();
  is_open = true;
  return 0;
}

void MonitorDBStore::close() {
  // there should be no work queued!
  ceph_assert(io_work.is_empty());
  io_work.stop();
  is_open = false;
  db.reset(NULL);
}

KeyValueDB::BackupStats MonitorDBStore::backup() {
  auto backup_path = g_conf().get_val<std::string>("mon_backup_path");
  auto stats = db->backup(backup_path);
  if (!stats.error) {
    // Stash the mon keyring alongside the rocksdb backup, keyed by
    // backup id, so a restore of an older version is paired with the
    // keyring of that vintage. The [mon.] secret can rotate; using a
    // single fixed filename would break authentication after restore.
    std::error_code ec;
    auto dest = backup_path + "/keyring." + std::to_string(stats.id);
    std::filesystem::copy_file(
      path + "/keyring", dest,
      std::filesystem::copy_options::overwrite_existing, ec);
    if (!ec) {
      std::filesystem::permissions(dest,
                                   std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
                                   std::filesystem::perm_options::replace, ec);
    }
    if (ec) {
      // Best-effort: a rocksdb backup without a stashed keyring is still
      // valid; the operator can supply a keyring out-of-band on restore.
      derr << __func__ << " failed to stash keyring at "
           << dest << ": " << ec.message() << dendl;
    }
  }
  return stats;
}

KeyValueDB::BackupCleanupStats MonitorDBStore::backup_cleanup() {
  auto backup_path = g_conf().get_val<std::string>("mon_backup_path");
  auto stats = db->backup_cleanup(
    backup_path,
    g_conf().get_val<uint64_t>("mon_backup_keep_last"),
    g_conf().get_val<uint64_t>("mon_backup_keep_hourly"),
    g_conf().get_val<uint64_t>("mon_backup_keep_daily"));
  if (stats.error) {
    return stats;
  }
  // Remove keyring.<id> files for backup ids the kv layer just dropped.
  std::string kv_type;
  if (read_meta("kv_backend", &kv_type) < 0 || kv_type.empty()) {
    kv_type = "rocksdb";
  }
  std::set<uint64_t> surviving;
  auto remaining = KeyValueDB::list_backups(g_ceph_context, kv_type, backup_path);
  if (!remaining) {
    return stats;
  }
  for (const auto& b : *remaining) {
    surviving.insert(b.id);
  }
  std::error_code ec;
  for (auto it = std::filesystem::directory_iterator(backup_path, ec);
       it != std::filesystem::directory_iterator();
       it.increment(ec)) {
    auto name = it->path().filename().string();
    if (name.compare(0, 8, "keyring.") != 0) {
      continue;
    }
    std::string idstr = name.substr(8);
    std::string parse_err;
    long long id = strict_strtoll(idstr.c_str(), 10, &parse_err);
    if (!parse_err.empty() || id < 0) {
      continue;
    }
    if (surviving.count(static_cast<uint64_t>(id))) {
      continue;
    }
    std::error_code rm_ec;
    std::filesystem::remove(it->path(), rm_ec);
  }
  return stats;
}

std::optional<std::vector<KeyValueDB::BackupStats>> MonitorDBStore::list_backups(
  CephContext *cct, const std::string &path, const std::string &backup_path) {
  std::string kv_type;
  int r = read_meta_path("kv_backend", &kv_type, path);
  if (r < 0 || kv_type.empty()) {
    // Disaster recovery: mon_data may be empty or absent. We only ship
    // a rocksdb kv backend today, so default to it for enumeration.
    kv_type = "rocksdb";
  }
  return KeyValueDB::list_backups(cct, kv_type, backup_path);
}

bool MonitorDBStore::restore_backup(CephContext *cct, const std::string &path,
                                    const std::string &backup_path,
                                    const std::optional<uint32_t> &version) {
  std::string kv_type;
  int r = read_meta_path("kv_backend", &kv_type, path);
  if (r < 0 || kv_type.empty()) {
    // Disaster recovery: mon_data is empty or freshly initialized, so
    // there is no kv_backend marker. Default to rocksdb and stamp the
    // file back so the subsequent open() finds it.
    kv_type = "rocksdb";
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
      lderr(cct) << __func__ << " failed to create " << path
                 << ": " << ec.message() << dendl;
      return false;
    }
    std::filesystem::permissions(path,
      std::filesystem::perms::owner_all,
      std::filesystem::perm_options::replace, ec);
    const std::string v = kv_type + "\n";
    if (safe_write_file(path.c_str(), "kv_backend",
                        v.c_str(), v.length(), 0600) < 0) {
      lderr(cct) << __func__ << " failed to write kv_backend in "
                 << path << dendl;
      return false;
    }
  }
  std::string store_path = get_store_path(path);

  // Resolve "latest" up front so we know which versioned keyring to
  // rehydrate alongside the rocksdb restore. Pick by BackupEngine id
  // (monotonic per rocksdb) rather than timestamp, so a clock skew
  // between backups cannot make the default restore pick a stale one.
  uint32_t resolved_version;
  if (version) {
    resolved_version = *version;
  } else {
    auto backups = KeyValueDB::list_backups(cct, kv_type, backup_path);
    if (!backups || backups->empty()) {
      lderr(cct) << __func__ << " no backups found at " << backup_path << dendl;
      return false;
    }
    resolved_version = std::max_element(
      backups->begin(), backups->end(),
      [](const auto& a, const auto& b) { return a.id < b.id; })->id;
  }

  if (!KeyValueDB::restore_backup(cct, kv_type, store_path, backup_path,
                                  resolved_version)) {
    return false;
  }

  // Rehydrate the matching keyring (skipped silently if the operator
  // keeps the keyring out-of-band).
  std::error_code ec;
  auto keyring_src = backup_path + "/keyring." + std::to_string(resolved_version);
  if (std::filesystem::exists(keyring_src, ec)) {
    std::filesystem::copy_file(
      keyring_src,
      path + "/keyring",
      std::filesystem::copy_options::overwrite_existing,
      ec);
    if (ec) {
      lderr(cct) << __func__ << " failed to restore keyring from "
                 << keyring_src << ": " << ec.message() << dendl;
      return false;
    }
  }

  // The mon store holds auth, config-key and dm-crypt secrets;
  // tighten everything we just restored to owner-only.
  std::filesystem::permissions(path,
    std::filesystem::perms::owner_all,
    std::filesystem::perm_options::replace, ec);
  for (auto it = std::filesystem::recursive_directory_iterator(path, ec);
       it != std::filesystem::recursive_directory_iterator();
       it.increment(ec)) {
    std::error_code ec_chmod;
    auto perms = it->is_directory(ec_chmod)
      ? std::filesystem::perms::owner_all
      : (std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
    std::filesystem::permissions(it->path(), perms,
      std::filesystem::perm_options::replace, ec_chmod);
    if (ec_chmod) {
      lderr(cct) << __func__ << " failed to chmod " << it->path()
                 << ": " << ec_chmod.message() << dendl;
    }
  }
  return true;
}

int MonitorDBStore::write_meta(const std::string& key,
                               const std::string& value) const {
  std::string v = value;
  v += "\n";
  int r = safe_write_file(path.c_str(), key.c_str(),
                          v.c_str(), v.length(),
                          0600);
  if (r < 0)
    return r;
  return 0;
}

int MonitorDBStore::read_meta_path(const std::string& key,
                                   std::string *value,
                                   const std::string& path) {
  char buf[4096];
  int r = safe_read_file(path.c_str(), key.c_str(),
                         buf, sizeof(buf));
  if (r <= 0)
    return r;
  // drop trailing newlines
  while (r && isspace(buf[r-1])) {
    --r;
  }
  *value = std::string(buf, r);
  return 0;
}

MonitorDBStore::MonitorDBStore(const std::string& path)
  : path(path),
    db(0),
    do_dump(false),
    dump_fd_binary(-1),
    dump_fmt(true),
    io_work(g_ceph_context, "monstore", "fn_monstore"),
    is_open(false) {
}

MonitorDBStore::~MonitorDBStore() {
  ceph_assert(!is_open);
  if (do_dump) {
    if (!g_conf()->mon_debug_dump_json) {
      ::close(dump_fd_binary);
    } else {
      dump_fmt.close_section();
      dump_fmt.flush(dump_fd_json);
      dump_fd_json.flush();
      dump_fd_json.close();
    }
  }
}
