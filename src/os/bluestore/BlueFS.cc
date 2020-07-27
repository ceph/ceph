// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "boost/algorithm/string.hpp" 
#include "bluestore_common.h"
#include "BlueFS.h"

#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "Allocator.h"
#include "include/ceph_assert.h"
#include "common/admin_socket.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluefs
#undef dout_prefix
#define dout_prefix *_dout << "bluefs "
using TOPNSPC::common::cmd_getval;

using std::byte;
using std::list;
using std::make_pair;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::to_string;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;


MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::File, bluefs_file, bluefs);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::Dir, bluefs_dir, bluefs);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::FileWriter, bluefs_file_writer, bluefs_file_writer);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::FileReaderBuffer,
			      bluefs_file_reader_buffer, bluefs_file_reader);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::FileReader, bluefs_file_reader, bluefs_file_reader);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::FileLock, bluefs_file_lock, bluefs);

static void wal_discard_cb(void *priv, void* priv2) {
  BlueFS *bluefs = static_cast<BlueFS*>(priv);
  interval_set<uint64_t> *tmp = static_cast<interval_set<uint64_t>*>(priv2);
  bluefs->handle_discard(BlueFS::BDEV_WAL, *tmp);
}

static void db_discard_cb(void *priv, void* priv2) {
  BlueFS *bluefs = static_cast<BlueFS*>(priv);
  interval_set<uint64_t> *tmp = static_cast<interval_set<uint64_t>*>(priv2);
  bluefs->handle_discard(BlueFS::BDEV_DB, *tmp);
}

static void slow_discard_cb(void *priv, void* priv2) {
  BlueFS *bluefs = static_cast<BlueFS*>(priv);
  interval_set<uint64_t> *tmp = static_cast<interval_set<uint64_t>*>(priv2);
  bluefs->handle_discard(BlueFS::BDEV_SLOW, *tmp);
}

class BlueFS::SocketHook : public AdminSocketHook {
  BlueFS* bluefs;
public:
  static BlueFS::SocketHook* create(BlueFS* bluefs)
  {
    BlueFS::SocketHook* hook = nullptr;
    AdminSocket* admin_socket = bluefs->cct->get_admin_socket();
    if (admin_socket) {
      hook = new BlueFS::SocketHook(bluefs);
      int r = admin_socket->register_command("bluestore bluefs available "
                                             "name=alloc_size,type=CephInt,req=false",
                                             hook,
                                             "Report available space for bluefs. "
                                             "If alloc_size set, make simulation.");
      if (r != 0) {
        ldout(bluefs->cct, 1) << __func__ << " cannot register SocketHook" << dendl;
        delete hook;
        hook = nullptr;
      } else {
        r = admin_socket->register_command("bluefs stats",
                                           hook,
                                           "Dump internal statistics for bluefs."
                                           "");
        ceph_assert(r == 0);
	r = admin_socket->register_command("bluefs files list", hook,
					   "print files in bluefs");
	ceph_assert(r == 0);
      }
    }
    return hook;
  }

  ~SocketHook() {
    AdminSocket* admin_socket = bluefs->cct->get_admin_socket();
    admin_socket->unregister_commands(this);
  }
private:
  SocketHook(BlueFS* bluefs) :
    bluefs(bluefs) {}
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    if (command == "bluestore bluefs available") {
      int64_t alloc_size = 0;
      cmd_getval(cmdmap, "alloc_size", alloc_size);
      if ((alloc_size & (alloc_size - 1)) != 0) {
	errss << "Invalid allocation size:'" << alloc_size << std::endl;
	return -EINVAL;
      }
      if (alloc_size == 0)
	alloc_size = bluefs->cct->_conf->bluefs_alloc_size;
      f->open_object_section("bluefs_available_space");
      for (unsigned dev = BDEV_WAL; dev <= BDEV_SLOW; dev++) {
	if (bluefs->bdev[dev]) {
	  f->open_object_section("dev");
	  f->dump_string("device", bluefs->get_device_name(dev));
	  ceph_assert(bluefs->alloc[dev]);
	  f->dump_int("free", bluefs->alloc[dev]->get_free());
	  f->close_section();
	}
      }
      size_t extra_space = 0;
      if (bluefs->slow_dev_expander) {
	extra_space = bluefs->slow_dev_expander->available_freespace(alloc_size);
      }
      f->dump_int("available_from_bluestore", extra_space);
      f->close_section();
    } else if (command == "bluefs stats") {
      std::stringstream ss;
      bluefs->dump_block_extents(ss);
      bluefs->dump_volume_selector(ss);
      out.append(ss);
    } else if (command == "bluefs files list") {
      const char* devnames[3] = {"wal","db","slow"};
      std::lock_guard l(bluefs->lock);
      f->open_array_section("files");
      for (auto &d : bluefs->dir_map) {
        std::string dir = d.first;
        for (auto &r : d.second->file_map) {
          f->open_object_section("file");
          f->dump_string("name", (dir + "/" + r.first).c_str());
          std::vector<size_t> sizes;
          sizes.resize(bluefs->bdev.size());
          for(auto& i : r.second->fnode.extents) {
            sizes[i.bdev] += i.length;
          }
          for (size_t i = 0; i < sizes.size(); i++) {
            if (sizes[i]>0) {
	      if (i < sizeof(devnames) / sizeof(*devnames))
		f->dump_int(devnames[i], sizes[i]);
	      else
		f->dump_int(("dev-"+to_string(i)).c_str(), sizes[i]);
	    }
          }
          f->close_section();
        }
      }
      f->close_section();
      f->flush(out);
    } else {
      errss << "Invalid command" << std::endl;
      return -ENOSYS;
    }
    return 0;
  }
};

BlueFS::BlueFS(CephContext* cct)
  : cct(cct),
    bdev(MAX_BDEV),
    ioc(MAX_BDEV),
    block_all(MAX_BDEV)
{
  discard_cb[BDEV_WAL] = wal_discard_cb;
  discard_cb[BDEV_DB] = db_discard_cb;
  discard_cb[BDEV_SLOW] = slow_discard_cb;
  asok_hook = SocketHook::create(this);
}

BlueFS::~BlueFS()
{
  delete asok_hook;
  for (auto p : ioc) {
    if (p)
      p->aio_wait();
  }
  for (auto p : bdev) {
    if (p) {
      p->close();
      delete p;
    }
  }
  for (auto p : ioc) {
    delete p;
  }
}

void BlueFS::_init_logger()
{
  PerfCountersBuilder b(cct, "bluefs",
                        l_bluefs_first, l_bluefs_last);
  b.add_u64_counter(l_bluefs_gift_bytes, "gift_bytes",
		    "Bytes gifted from BlueStore", NULL, 0, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_reclaim_bytes, "reclaim_bytes",
		    "Bytes reclaimed by BlueStore", NULL, 0, unit_t(UNIT_BYTES));
  b.add_u64(l_bluefs_db_total_bytes, "db_total_bytes",
	    "Total bytes (main db device)",
	    "b", PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64(l_bluefs_db_used_bytes, "db_used_bytes",
	    "Used bytes (main db device)",
	    "u", PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64(l_bluefs_wal_total_bytes, "wal_total_bytes",
	    "Total bytes (wal device)",
	    "walb", PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64(l_bluefs_wal_used_bytes, "wal_used_bytes",
	    "Used bytes (wal device)",
	    "walu", PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64(l_bluefs_slow_total_bytes, "slow_total_bytes",
	    "Total bytes (slow device)",
	    "slob", PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64(l_bluefs_slow_used_bytes, "slow_used_bytes",
	    "Used bytes (slow device)",
	    "slou", PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64(l_bluefs_num_files, "num_files", "File count",
	    "f", PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64(l_bluefs_log_bytes, "log_bytes", "Size of the metadata log",
	    "jlen", PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_log_compactions, "log_compactions",
		    "Compactions of the metadata log");
  b.add_u64_counter(l_bluefs_logged_bytes, "logged_bytes",
		    "Bytes written to the metadata log", "j",
		    PerfCountersBuilder::PRIO_CRITICAL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_files_written_wal, "files_written_wal",
		    "Files written to WAL");
  b.add_u64_counter(l_bluefs_files_written_sst, "files_written_sst",
		    "Files written to SSTs");
  b.add_u64_counter(l_bluefs_bytes_written_wal, "bytes_written_wal",
		    "Bytes written to WAL", "wal",
		    PerfCountersBuilder::PRIO_CRITICAL);
  b.add_u64_counter(l_bluefs_bytes_written_sst, "bytes_written_sst",
		    "Bytes written to SSTs", "sst",
		    PerfCountersBuilder::PRIO_CRITICAL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_bytes_written_slow, "bytes_written_slow",
		    "Bytes written to WAL/SSTs at slow device", NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_max_bytes_wal, "max_bytes_wal",
		    "Maximum bytes allocated from WAL");
  b.add_u64_counter(l_bluefs_max_bytes_db, "max_bytes_db",
		    "Maximum bytes allocated from DB");
  b.add_u64_counter(l_bluefs_max_bytes_slow, "max_bytes_slow",
		    "Maximum bytes allocated from SLOW");

  b.add_u64_counter(l_bluefs_read_random_count, "read_random_count",
		    "random read requests processed");
  b.add_u64_counter(l_bluefs_read_random_bytes, "read_random_bytes",
		    "Bytes requested in random read mode", NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_random_disk_count, "read_random_disk_count",
		    "random reads requests going to disk");
  b.add_u64_counter(l_bluefs_read_random_disk_bytes, "read_random_disk_bytes",
		    "Bytes read from disk in random read mode", NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_random_buffer_count, "read_random_buffer_count",
		    "random read requests processed using prefetch buffer");
  b.add_u64_counter(l_bluefs_read_random_buffer_bytes, "read_random_buffer_bytes",
		    "Bytes read from prefetch buffer in random read mode", NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));

  b.add_u64_counter(l_bluefs_read_count, "read_count",
		    "buffered read requests processed");
  b.add_u64_counter(l_bluefs_read_bytes, "read_bytes",
		    "Bytes requested in buffered read mode", NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));

  b.add_u64_counter(l_bluefs_read_prefetch_count, "read_prefetch_count",
		    "prefetch read requests processed");
  b.add_u64_counter(l_bluefs_read_prefetch_bytes, "read_prefetch_bytes",
		    "Bytes requested in prefetch read mode", NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));

  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
}

void BlueFS::_shutdown_logger()
{
  cct->get_perfcounters_collection()->remove(logger);
  delete logger;
}

void BlueFS::_update_logger_stats()
{
  // we must be holding the lock
  logger->set(l_bluefs_num_files, file_map.size());
  logger->set(l_bluefs_log_bytes, log_writer->file->fnode.size);

  if (alloc[BDEV_WAL]) {
    logger->set(l_bluefs_wal_total_bytes, block_all[BDEV_WAL].size());
    logger->set(l_bluefs_wal_used_bytes,
		block_all[BDEV_WAL].size() - alloc[BDEV_WAL]->get_free());
  }
  if (alloc[BDEV_DB]) {
    logger->set(l_bluefs_db_total_bytes, block_all[BDEV_DB].size());
    logger->set(l_bluefs_db_used_bytes,
		block_all[BDEV_DB].size() - alloc[BDEV_DB]->get_free());
  }
  if (alloc[BDEV_SLOW]) {
    logger->set(l_bluefs_slow_total_bytes, block_all[BDEV_SLOW].size());
    logger->set(l_bluefs_slow_used_bytes,
		block_all[BDEV_SLOW].size() - alloc[BDEV_SLOW]->get_free());
  }
}

int BlueFS::add_block_device(unsigned id, const string& path, bool trim,
			     bool shared_with_bluestore)
{
  dout(10) << __func__ << " bdev " << id << " path " << path << dendl;
  ceph_assert(id < bdev.size());
  ceph_assert(bdev[id] == NULL);
  BlockDevice *b = BlockDevice::create(cct, path, NULL, NULL,
				       discard_cb[id], static_cast<void*>(this));
  if (shared_with_bluestore) {
    b->set_no_exclusive_lock();
  }
  int r = b->open(path);
  if (r < 0) {
    delete b;
    return r;
  }
  if (trim) {
    b->discard(0, b->get_size());
  }

  dout(1) << __func__ << " bdev " << id << " path " << path
	  << " size " << byte_u_t(b->get_size()) << dendl;
  bdev[id] = b;
  ioc[id] = new IOContext(cct, NULL);
  return 0;
}

bool BlueFS::bdev_support_label(unsigned id)
{
  ceph_assert(id < bdev.size());
  ceph_assert(bdev[id]);
  return bdev[id]->supported_bdev_label();
}

uint64_t BlueFS::get_block_device_size(unsigned id)
{
  if (id < bdev.size() && bdev[id])
    return bdev[id]->get_size();
  return 0;
}

void BlueFS::_add_block_extent(unsigned id, uint64_t offset, uint64_t length,
                               bool skip)
{
  dout(1) << __func__ << " bdev " << id
	  << " 0x" << std::hex << offset << "~" << length << std::dec
	  << " skip " << skip
	  << dendl;

  ceph_assert(id < bdev.size());
  ceph_assert(bdev[id]);
  ceph_assert(bdev[id]->get_size() >= offset + length);
  block_all[id].insert(offset, length);

  if (id < alloc.size() && alloc[id]) {
    if (!skip)
      log_t.op_alloc_add(id, offset, length);

    alloc[id]->init_add_free(offset, length);
  }

  if (logger)
    logger->inc(l_bluefs_gift_bytes, length);
  dout(10) << __func__ << " done" << dendl;
}

int BlueFS::reclaim_blocks(unsigned id, uint64_t want,
			   PExtentVector *extents)
{
  std::unique_lock l(lock);
  dout(1) << __func__ << " bdev " << id
          << " want 0x" << std::hex << want << std::dec << dendl;
  ceph_assert(id < alloc.size());
  ceph_assert(alloc[id]);
  int64_t got = 0;

  interval_set<uint64_t> granular;
  while (want > 0 && !block_unused_too_granular[id].empty()) {
    auto p = block_unused_too_granular[id].begin();
    dout(20) << __func__ << " unused " << (int)id << ":"
	     << std::hex << p.get_start() << "~" << p.get_len() << dendl;
    extents->push_back({p.get_start(), p.get_len()});
    granular.insert(p.get_start(), p.get_len());
    if (want >= p.get_len()) {
      want -= p.get_len();
    } else {
      want = 0;
    }
    got += p.get_len();
    block_unused_too_granular[id].erase(p);
  }

  if (want > 0) {
    got += alloc[id]->allocate(want, alloc_size[id], 0, extents);
    ceph_assert(got != 0);
    if (got < 0) {
      derr << __func__ << " failed to allocate space to return to bluestore"
	   << dendl;
      alloc[id]->dump();
      block_unused_too_granular[id].insert(granular);
      return got;
    }

    for (auto& p : *extents) {
      block_all[id].erase(p.offset, p.length);
      log_t.op_alloc_rm(id, p.offset, p.length);
    }

    flush_bdev();
    int r = _flush_and_sync_log(l);
    ceph_assert(r == 0);
  }

  logger->inc(l_bluefs_reclaim_bytes, got);
  dout(1) << __func__ << " bdev " << id << " want 0x" << std::hex << want
	  << " got " << *extents << dendl;
  return 0;
}

void BlueFS::handle_discard(unsigned id, interval_set<uint64_t>& to_release)
{
  dout(10) << __func__ << " bdev " << id << dendl;
  ceph_assert(alloc[id]);
  alloc[id]->release(to_release);
}

uint64_t BlueFS::get_used()
{
  std::lock_guard l(lock);
  uint64_t used = 0;
  for (unsigned id = 0; id < MAX_BDEV; ++id) {
    if (alloc[id]) {
      used += block_all[id].size() - alloc[id]->get_free();
    }
  }
  return used;
}

uint64_t BlueFS::get_total(unsigned id)
{
  std::lock_guard l(lock);
  ceph_assert(id < block_all.size());
  return block_all[id].size();
}

uint64_t BlueFS::get_free(unsigned id)
{
  std::lock_guard l(lock);
  ceph_assert(id < alloc.size());
  return alloc[id]->get_free();
}

void BlueFS::dump_perf_counters(Formatter *f)
{
  f->open_object_section("bluefs_perf_counters");
  logger->dump_formatted(f,0);
  f->close_section();
}

void BlueFS::dump_block_extents(ostream& out)
{
  for (unsigned i = 0; i < MAX_BDEV; ++i) {
    if (!bdev[i]) {
      continue;
    }
    auto owned = get_total(i);
    auto free = get_free(i);

    out << i << " : device size 0x" << std::hex << bdev[i]->get_size()
        << " : own 0x" << block_all[i]
        << " = 0x" << owned
        << " : using 0x" << owned - free
	<< std::dec << "(" << byte_u_t(owned - free) << ")";
    if (i == _get_slow_device_id()) {
      ceph_assert(slow_dev_expander);
      ceph_assert(alloc[i]);
      free = slow_dev_expander->available_freespace(alloc_size[i]);
      out << std::hex
          << " : bluestore has 0x" << free
          << std::dec << "(" << byte_u_t(free) << ") available";
    }
    out << "\n";
  }
}

void BlueFS::get_usage(vector<pair<uint64_t,uint64_t>> *usage)
{
  std::lock_guard l(lock);
  usage->resize(bdev.size());
  for (unsigned id = 0; id < bdev.size(); ++id) {
    if (!bdev[id]) {
      (*usage)[id] = make_pair(0, 0);
      continue;
    }
    (*usage)[id].first = alloc[id]->get_free();
    (*usage)[id].second = block_all[id].size();
    uint64_t used =
      (block_all[id].size() - (*usage)[id].first) * 100 / block_all[id].size();
    dout(10) << __func__ << " bdev " << id
	     << " free " << (*usage)[id].first
	     << " (" << byte_u_t((*usage)[id].first) << ")"
	     << " / " << (*usage)[id].second
	     << " (" << byte_u_t((*usage)[id].second) << ")"
	     << ", used " << used << "%"
	     << dendl;
  }
}

int BlueFS::get_block_extents(unsigned id, interval_set<uint64_t> *extents)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " bdev " << id << dendl;
  if (id >= block_all.size())
    return -EINVAL;
  *extents = block_all[id];
  return 0;
}

int BlueFS::mkfs(uuid_d osd_uuid, const bluefs_layout_t& layout)
{
  std::unique_lock l(lock);
  dout(1) << __func__
	  << " osd_uuid " << osd_uuid
	  << dendl;

  // set volume selector if not provided before/outside
  if (vselector == nullptr) {
    vselector.reset(
      new OriginalVolumeSelector(
        get_block_device_size(BlueFS::BDEV_WAL) * 95 / 100,
        get_block_device_size(BlueFS::BDEV_DB) * 95 / 100,
        get_block_device_size(BlueFS::BDEV_SLOW) * 95 / 100));
  }

  _init_alloc();
  _init_logger();

  super.version = 1;
  super.block_size = bdev[BDEV_DB]->get_block_size();
  super.osd_uuid = osd_uuid;
  super.uuid.generate_random();
  dout(1) << __func__ << " uuid " << super.uuid << dendl;

  // init log
  FileRef log_file = ceph::make_ref<File>();
  log_file->fnode.ino = 1;
  log_file->vselector_hint = vselector->get_hint_for_log();
  int r = _allocate(
    vselector->select_prefer_bdev(log_file->vselector_hint),
    cct->_conf->bluefs_max_log_runway,
    &log_file->fnode);
  vselector->add_usage(log_file->vselector_hint, log_file->fnode);
  ceph_assert(r == 0);
  log_writer = _create_writer(log_file);

  // initial txn
  log_t.op_init();
  for (unsigned bdev = 0; bdev < MAX_BDEV; ++bdev) {
    interval_set<uint64_t>& p = block_all[bdev];
    if (p.empty())
      continue;
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      dout(20) << __func__ << " op_alloc_add " << bdev << " 0x"
               << std::hex << q.get_start() << "~" << q.get_len() << std::dec
               << dendl;
      log_t.op_alloc_add(bdev, q.get_start(), q.get_len());
    }
  }
  _flush_and_sync_log(l);

  // write supers
  super.log_fnode = log_file->fnode;
  super.memorized_layout = layout;
  _write_super(BDEV_DB);
  flush_bdev();

  // clean up
  super = bluefs_super_t();
  _close_writer(log_writer);
  log_writer = NULL;
  block_all.clear();
  vselector.reset(nullptr);
  _stop_alloc();
  _shutdown_logger();

  dout(10) << __func__ << " success" << dendl;
  return 0;
}

void BlueFS::_init_alloc()
{
  dout(20) << __func__ << dendl;
  alloc.resize(MAX_BDEV);
  alloc_size.resize(MAX_BDEV, 0);
  pending_release.resize(MAX_BDEV);
  block_unused_too_granular.resize(MAX_BDEV);

  if (bdev[BDEV_WAL]) {
    alloc_size[BDEV_WAL] = cct->_conf->bluefs_alloc_size;
  }
  if (bdev[BDEV_SLOW]) {
    alloc_size[BDEV_DB] = cct->_conf->bluefs_alloc_size;
    alloc_size[BDEV_SLOW] = cct->_conf->bluefs_shared_alloc_size;
  } else {
    alloc_size[BDEV_DB] = cct->_conf->bluefs_shared_alloc_size;
  }
  // new wal and db devices are never shared
  if (bdev[BDEV_NEWWAL]) {
    alloc_size[BDEV_NEWWAL] = cct->_conf->bluefs_alloc_size;
  }
  if (bdev[BDEV_NEWDB]) {
    alloc_size[BDEV_NEWDB] = cct->_conf->bluefs_alloc_size;
  }

  for (unsigned id = 0; id < bdev.size(); ++id) {
    if (!bdev[id]) {
      continue;
    }
    ceph_assert(bdev[id]->get_size());
    std::string name = "bluefs-";
    const char* devnames[] = {"wal","db","slow"};
    if (id <= BDEV_SLOW)
      name += devnames[id];
    else
      name += to_string(uintptr_t(this));
    ceph_assert(alloc_size[id]);
    dout(1) << __func__ << " id " << id
	     << " alloc_size 0x" << std::hex << alloc_size[id]
	     << " size 0x" << bdev[id]->get_size() << std::dec << dendl;
    alloc[id] = Allocator::create(cct, cct->_conf->bluefs_allocator,
				  bdev[id]->get_size(),
				  alloc_size[id], name);
    interval_set<uint64_t>& p = block_all[id];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      alloc[id]->init_add_free(q.get_start(), q.get_len());
    }
  }
}

void BlueFS::_stop_alloc()
{
  dout(20) << __func__ << dendl;
  for (auto p : bdev) {
    if (p)
      p->discard_drain();
  }

  for (auto p : alloc) {
    if (p != nullptr)  {
      p->shutdown();
      delete p;
    }
  }
  alloc.clear();
  block_unused_too_granular.clear();
}

int BlueFS::mount()
{
  dout(1) << __func__ << dendl;

  int r = _open_super();
  if (r < 0) {
    derr << __func__ << " failed to open super: " << cpp_strerror(r) << dendl;
    goto out;
  }

  // set volume selector if not provided before/outside
  if (vselector == nullptr) {
    vselector.reset(
      new OriginalVolumeSelector(
        get_block_device_size(BlueFS::BDEV_WAL) * 95 / 100,
        get_block_device_size(BlueFS::BDEV_DB) * 95 / 100,
        get_block_device_size(BlueFS::BDEV_SLOW) * 95 / 100));
  }

  block_all.clear();
  block_all.resize(MAX_BDEV);
  _init_alloc();
  _init_logger();

  r = _replay(false, false);
  if (r < 0) {
    derr << __func__ << " failed to replay log: " << cpp_strerror(r) << dendl;
    _stop_alloc();
    goto out;
  }

  // init freelist
  for (auto& p : file_map) {
    dout(30) << __func__ << " noting alloc for " << p.second->fnode << dendl;
    for (auto& q : p.second->fnode.extents) {
      alloc[q.bdev]->init_rm_free(q.offset, q.length);
    }
  }

  // set up the log for future writes
  log_writer = _create_writer(_get_file(1));
  ceph_assert(log_writer->file->fnode.ino == 1);
  log_writer->pos = log_writer->file->fnode.size;
  dout(10) << __func__ << " log write pos set to 0x"
           << std::hex << log_writer->pos << std::dec
           << dendl;

  return 0;

 out:
  super = bluefs_super_t();
  return r;
}

int BlueFS::maybe_verify_layout(const bluefs_layout_t& layout) const
{
  if (super.memorized_layout) {
    if (layout == *super.memorized_layout) {
      dout(10) << __func__ << " bluefs layout verified positively" << dendl;
    } else {
      derr << __func__ << " memorized layout doesn't fit current one" << dendl;
      return -EIO;
    }
  } else {
    dout(10) << __func__ << " no memorized_layout in bluefs superblock"
             << dendl;
  }

  return 0;
}

void BlueFS::umount(bool avoid_compact)
{
  dout(1) << __func__ << dendl;

  sync_metadata(avoid_compact);

  _close_writer(log_writer);
  log_writer = NULL;

  vselector.reset(nullptr);
  _stop_alloc();
  file_map.clear();
  dir_map.clear();
  super = bluefs_super_t();
  log_t.clear();
  _shutdown_logger();
}

int BlueFS::prepare_new_device(int id, const bluefs_layout_t& layout)
{
  dout(1) << __func__ << dendl;

  if(id == BDEV_NEWDB) {
    int new_log_dev_cur = BDEV_WAL;
    int new_log_dev_next = BDEV_WAL;
    if (!bdev[BDEV_WAL]) {
      new_log_dev_cur = BDEV_NEWDB;
      new_log_dev_next = BDEV_DB;
    }
    _rewrite_log_and_layout_sync(false,
      BDEV_NEWDB,
      new_log_dev_cur,
      new_log_dev_next,
      RENAME_DB2SLOW,
      layout);
    //}
  } else if(id == BDEV_NEWWAL) {
    _rewrite_log_and_layout_sync(false,
      BDEV_DB,
      BDEV_NEWWAL,
      BDEV_WAL,
      REMOVE_WAL,
      layout);
  } else {
    assert(false);
  }
  return 0;
}

void BlueFS::collect_metadata(map<string,string> *pm, unsigned skip_bdev_id)
{
  if (skip_bdev_id != BDEV_DB && bdev[BDEV_DB])
    bdev[BDEV_DB]->collect_metadata("bluefs_db_", pm);
  if (bdev[BDEV_WAL])
    bdev[BDEV_WAL]->collect_metadata("bluefs_wal_", pm);
}

void BlueFS::get_devices(set<string> *ls)
{
  for (unsigned i = 0; i < MAX_BDEV; ++i) {
    if (bdev[i]) {
      bdev[i]->get_devices(ls);
    }
  }
}

int BlueFS::fsck()
{
  std::lock_guard l(lock);
  dout(1) << __func__ << dendl;
  // hrm, i think we check everything on mount...
  return 0;
}

int BlueFS::_write_super(int dev)
{
  // build superblock
  bufferlist bl;
  encode(super, bl);
  uint32_t crc = bl.crc32c(-1);
  encode(crc, bl);
  dout(10) << __func__ << " super block length(encoded): " << bl.length() << dendl;
  dout(10) << __func__ << " superblock " << super.version << dendl;
  dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
  ceph_assert_always(bl.length() <= get_super_length());
  bl.append_zero(get_super_length() - bl.length());

  bdev[dev]->write(get_super_offset(), bl, false, WRITE_LIFE_SHORT);
  dout(20) << __func__ << " v " << super.version
           << " crc 0x" << std::hex << crc
           << " offset 0x" << get_super_offset() << std::dec
           << dendl;
  return 0;
}

int BlueFS::_open_super()
{
  dout(10) << __func__ << dendl;

  bufferlist bl;
  uint32_t expected_crc, crc;
  int r;

  // always the second block
  r = bdev[BDEV_DB]->read(get_super_offset(), get_super_length(),
			  &bl, ioc[BDEV_DB], false);
  if (r < 0)
    return r;

  auto p = bl.cbegin();
  decode(super, p);
  {
    bufferlist t;
    t.substr_of(bl, 0, p.get_off());
    crc = t.crc32c(-1);
  }
  decode(expected_crc, p);
  if (crc != expected_crc) {
    derr << __func__ << " bad crc on superblock, expected 0x"
         << std::hex << expected_crc << " != actual 0x" << crc << std::dec
         << dendl;
    return -EIO;
  }
  dout(10) << __func__ << " superblock " << super.version << dendl;
  dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
  return 0;
}

int BlueFS::_check_new_allocations(const bluefs_fnode_t& fnode,
  size_t dev_count,
  boost::dynamic_bitset<uint64_t>* owned_blocks,
  boost::dynamic_bitset<uint64_t>* used_blocks)
{
  auto& fnode_extents = fnode.extents;
  for (auto e : fnode_extents) {
    auto id = e.bdev;
    bool fail = false;
    ceph_assert(id < dev_count);
    apply_for_bitset_range(e.offset, e.length, alloc_size[id], owned_blocks[id],
      [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
        if (!bs.test(pos)) {
          fail = true;
        }
      }
    );
    if (fail) {
      derr << __func__ << " invalid extent " << int(id)
        << ": 0x" << std::hex << e.offset << "~" << e.length
        << std::dec
        << ": wasn't given but allocated for ino " << fnode.ino
        << dendl;
      return -EFAULT;
    }

    apply_for_bitset_range(e.offset, e.length, alloc_size[id], used_blocks[id],
      [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
        if (bs.test(pos)) {
          fail = true;
        }
        bs.set(pos);
      }
    );
    if (fail) {
      derr << __func__ << " invalid extent " << int(e.bdev)
        << ": 0x" << std::hex << e.offset << "~" << e.length
        << std::dec << ": duplicate reference, ino " << fnode.ino
        << dendl;
      return -EFAULT;
    }
  }
  return 0;
}

int BlueFS::_adjust_granularity(
  __u8 id, uint64_t *offset, uint64_t *length, bool alloc)
{
  const char *op = alloc ? "op_alloc_add" : "op_alloc_rm";
  auto oldo = *offset;
  auto oldl = *length;
  if (*offset & (alloc_size[id] - 1)) {
    *offset &= ~(alloc_size[id] - 1);
    *offset += alloc_size[id];
    if (*length > *offset - oldo) {
      if (alloc) {
	block_unused_too_granular[id].insert(oldo, *offset - oldo);
      } else {
	block_unused_too_granular[id].erase(oldo, *offset - oldo);
      }
      *length -= (*offset - oldo);
    } else {
      if (alloc) {
	block_unused_too_granular[id].insert(oldo, *length);
      } else {
	block_unused_too_granular[id].erase(oldo, *length);
      }
      *length = 0;
    }
  }
  if (*length & (alloc_size[id] - 1)) {
    *length &= ~(alloc_size[id] - 1);
    if (alloc) {
      block_unused_too_granular[id].insert(
	*offset + *length,
	oldo + oldl - *offset - *length);
    } else {
      block_unused_too_granular[id].erase(
	*offset + *length,
	oldo + oldl - *offset - *length);
    }
  }
  if (oldo != *offset || oldl != *length) {
    dout(10) << __func__ << " " << op << " "
	     << (int)id << ":" << std::hex << oldo << "~" << oldl
	     << " -> " << (int)id << ":" << *offset << "~" << *length << dendl;
  }
  return 0;
}

int BlueFS::_verify_alloc_granularity(
  __u8 id, uint64_t offset, uint64_t length, const char *op)
{
  if ((offset & (alloc_size[id] - 1)) ||
      (length & (alloc_size[id] - 1))) {
    derr << __func__ << " " << op << " of " << (int)id
	 << ":0x" << std::hex << offset << "~" << length << std::dec
	 << " does not align to alloc_size 0x"
	 << std::hex << alloc_size[id] << std::dec << dendl;
    // be helpful
    auto need = alloc_size[id];
    while (need && ((offset & (need - 1)) ||
		    (length & (need - 1)))) {
      need >>= 1;
    }
    if (need) {
      const char *which;
      if (id == BDEV_SLOW ||
	  (id == BDEV_DB && !bdev[BDEV_SLOW])) {
	which = "bluefs_shared_alloc_size";
      } else {
	which = "bluefs_alloc_size";
      }
      derr << "work-around by setting " << which << " = " << need
	   << " for this OSD" << dendl;
    }
    return -EFAULT;
  }
  return 0;
}

int BlueFS::_replay(bool noop, bool to_stdout)
{
  dout(10) << __func__ << (noop ? " NO-OP" : "") << dendl;
  ino_last = 1;  // by the log
  log_seq = 0;

  FileRef log_file;
  log_file = _get_file(1);

  // sanity check
  for (auto& a : block_unused_too_granular) {
    ceph_assert(a.empty());
  }

  log_file->fnode = super.log_fnode;
  if (!noop) {
    log_file->vselector_hint =
      vselector->get_hint_for_log();
  } else {
    // do not use fnode from superblock in 'noop' mode - log_file's one should
    // be fine and up-to-date
    ceph_assert(log_file->fnode.ino == 1);
    ceph_assert(log_file->fnode.extents.size() != 0);
  }
  dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
  if (unlikely(to_stdout)) {
    std::cout << " log_fnode " << super.log_fnode << std::endl;
  } 

  FileReader *log_reader = new FileReader(
    log_file, cct->_conf->bluefs_max_prefetch,
    false,  // !random
    true);  // ignore eof

  bool seen_recs = false;

  boost::dynamic_bitset<uint64_t> used_blocks[MAX_BDEV];
  boost::dynamic_bitset<uint64_t> owned_blocks[MAX_BDEV];

  if (!noop) {
    if (cct->_conf->bluefs_log_replay_check_allocations) {
      for (size_t i = 0; i < MAX_BDEV; ++i) {
	if (alloc_size[i] != 0 && bdev[i] != nullptr) {
	  used_blocks[i].resize(round_up_to(bdev[i]->get_size(), alloc_size[i]) / alloc_size[i]);
	  owned_blocks[i].resize(round_up_to(bdev[i]->get_size(), alloc_size[i]) / alloc_size[i]);
	}
      }
    }
  }
  
  bool first_log_check = true;
  
  while (true) {
    ceph_assert((log_reader->buf.pos & ~super.block_mask()) == 0);
    uint64_t pos = log_reader->buf.pos;
    uint64_t read_pos = pos;
    bufferlist bl;
    {
      int r = _read(log_reader, read_pos, super.block_size,
		    &bl, NULL);
      if (r != (int)super.block_size && cct->_conf->bluefs_replay_recovery) {
	r += do_replay_recovery_read(log_reader, pos, read_pos + r, super.block_size - r, &bl);
      }
      assert(r == (int)super.block_size);
      read_pos += r;
    }
    uint64_t more = 0;
    uint64_t seq;
    uuid_d uuid;
    {
      auto p = bl.cbegin();
      __u8 a, b;
      uint32_t len;
      decode(a, p);
      decode(b, p);
      decode(len, p);
      decode(uuid, p);
      decode(seq, p);
      if (len + 6 > bl.length()) {
	more = round_up_to(len + 6 - bl.length(), super.block_size);
      }
    }
    if (uuid != super.uuid) {
      if (seen_recs) {
	dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
		 << ": stop: uuid " << uuid << " != super.uuid " << super.uuid
		 << dendl;
      } else {
	derr << __func__ << " 0x" << std::hex << pos << std::dec
		 << ": stop: uuid " << uuid << " != super.uuid " << super.uuid
		 << ", block dump: \n";
	bufferlist t;
	t.substr_of(bl, 0, super.block_size);
	t.hexdump(*_dout);
	*_dout << dendl;
      }
      break;
    }
    if (seq != log_seq + 1) {
      if (seen_recs) {
	dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
		 << ": stop: seq " << seq << " != expected " << log_seq + 1
		 << dendl;;
      } else {
	derr << __func__ << " 0x" << std::hex << pos << std::dec
	     << ": stop: seq " << seq << " != expected " << log_seq + 1
	     << dendl;;
      }
      break;
    }
    if (more) {
      dout(20) << __func__ << " need 0x" << std::hex << more << std::dec
               << " more bytes" << dendl;
      bufferlist t;
      int r = _read(log_reader, read_pos, more, &t, NULL);
      if (r < (int)more) {
	dout(10) << __func__ << " 0x" << std::hex << pos
                 << ": stop: len is 0x" << bl.length() + more << std::dec
                 << ", which is past eof" << dendl;
	if (cct->_conf->bluefs_replay_recovery) {
	  //try to search for more data
	  r += do_replay_recovery_read(log_reader, pos, read_pos + r, more - r, &t);
	  if (r < (int)more) {
	    //in normal mode we must read r==more, for recovery it is too strict
	    break;
	  }
	}
      }
      ceph_assert(r == (int)more);
      bl.claim_append(t);
      read_pos += r;
    }
    seen_recs = true;
    bluefs_transaction_t t;
    try {
      auto p = bl.cbegin();
      decode(t, p);
    }
    catch (ceph::buffer::error& e) {
      derr << __func__ << " 0x" << std::hex << pos << std::dec
           << ": stop: failed to decode: " << e.what()
           << dendl;
      delete log_reader;
      return -EIO;
    }
    ceph_assert(seq == t.seq);
    dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
             << ": " << t << dendl;
    if (unlikely(to_stdout)) {
      std::cout << " 0x" << std::hex << pos << std::dec
                << ": " << t << std::endl;
    }

    auto p = t.op_bl.cbegin();
    while (!p.end()) {
      __u8 op;
      decode(op, p);
      switch (op) {

      case bluefs_transaction_t::OP_INIT:
	dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                 << ":  op_init" << dendl;
        if (unlikely(to_stdout)) {
          std::cout << " 0x" << std::hex << pos << std::dec
                    << ":  op_init" << std::endl;
        }

	ceph_assert(t.seq == 1);
	break;

      case bluefs_transaction_t::OP_JUMP:
        {
	  uint64_t next_seq;
	  uint64_t offset;
	  decode(next_seq, p);
	  decode(offset, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
		   << ":  op_jump seq " << next_seq
		   << " offset 0x" << std::hex << offset << std::dec << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_jump seq " << next_seq
                      << " offset 0x" << std::hex << offset << std::dec
                      << std::endl;
          }

	  ceph_assert(next_seq >= log_seq);
	  log_seq = next_seq - 1; // we will increment it below
	  uint64_t skip = offset - read_pos;
	  if (skip) {
	    bufferlist junk;
	    int r = _read(log_reader, read_pos, skip, &junk,
			  NULL);
	    if (r != (int)skip) {
	      dout(10) << __func__ << " 0x" << std::hex << read_pos
		       << ": stop: failed to skip to " << offset
		       << std::dec << dendl;
	      ceph_abort_msg("problem with op_jump");
	    }
	  }
	}
	break;

      case bluefs_transaction_t::OP_JUMP_SEQ:
        {
	  uint64_t next_seq;
	  decode(next_seq, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_jump_seq " << next_seq << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_jump_seq " << next_seq << std::endl;
          }

	  ceph_assert(next_seq >= log_seq);
	  log_seq = next_seq - 1; // we will increment it below
	}
	break;

      case bluefs_transaction_t::OP_ALLOC_ADD:
        {
	  __u8 id;
	  uint64_t offset, length;
	  decode(id, p);
	  decode(offset, p);
	  decode(length, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_alloc_add " << " " << (int)id
                   << ":0x" << std::hex << offset << "~" << length << std::dec
                   << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_alloc_add " << " " << (int)id
                      << ":0x" << std::hex << offset << "~" << length << std::dec
                      << std::endl;
          }
	  if (!noop) {
	    block_all[id].insert(offset, length);
	    _adjust_granularity(id, &offset, &length, true);
	    if (length) {
	      alloc[id]->init_add_free(offset, length);
	    }

            if (cct->_conf->bluefs_log_replay_check_allocations) {
              bool fail = false;
              apply_for_bitset_range(offset, length, alloc_size[id], owned_blocks[id],
                [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
                  if (bs.test(pos)) {
                    fail = true;
                  } else {
                    bs.set(pos);
                  }
                }
              );
              if (fail) {
                derr << __func__ << " invalid extent " << (int)id
                  << ": 0x" << std::hex << offset << "~" << length
                  << std::dec << ": already given" << dendl;
                return -EFAULT;
              }
              apply_for_bitset_range(offset, length, alloc_size[id], used_blocks[id],
                [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
                  if (bs.test(pos)) {
                    fail = true;
                  }
                }
              );
              if (fail) {
                derr << __func__ << " invalid extent " << int(id)
                  << ": 0x" << std::hex << offset << "~" << length
                  << std::dec << ": already in use" << dendl;
                return -EFAULT;
              }
            }
	  }
	}
	break;

      case bluefs_transaction_t::OP_ALLOC_RM:
        {
	  __u8 id;
	  uint64_t offset, length;
	  decode(id, p);
	  decode(offset, p);
	  decode(length, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_alloc_rm " << " " << (int)id
                   << ":0x" << std::hex << offset << "~" << length << std::dec
                   << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_alloc_rm " << " " << (int)id
                      << ":0x" << std::hex << offset << "~" << length << std::dec
                      << std::endl;
          }
	  if (!noop) {
	    block_all[id].erase(offset, length);
	    _adjust_granularity(id, &offset, &length, false);
	    if (length) {
	      alloc[id]->init_rm_free(offset, length);
	    }
            if (cct->_conf->bluefs_log_replay_check_allocations) {
              bool fail = false;
              apply_for_bitset_range(offset, length, alloc_size[id], owned_blocks[id],
                [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
                  if (!bs.test(pos)) {
                    fail = true;
                  } else {
                    bs.reset(pos);
                  }
                }
              );
              if (fail) {
                derr << __func__ << " invalid extent " << int(id)
                  << ": 0x" << std::hex << offset << "~" << length
                  << std::dec << ": wasn't given" << dendl;
                return -EFAULT;
              }

              apply_for_bitset_range(offset, length, alloc_size[id], used_blocks[id],
                [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
                  if (bs.test(pos)) {
                    fail = true;
                  }
                }
              );
              if (fail) {
                derr << __func__ << " invalid extent " << (int)id
                   << ": 0x" << std::hex << offset << "~" << length
                  << std::dec << ": still in use" << dendl;
                return -EFAULT;
              }
            }
          }
	}
	break;

      case bluefs_transaction_t::OP_DIR_LINK:
        {
	  string dirname, filename;
	  uint64_t ino;
	  decode(dirname, p);
	  decode(filename, p);
	  decode(ino, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_dir_link " << " " << dirname << "/" << filename
                   << " to " << ino
		   << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_dir_link " << " " << dirname << "/" << filename
                      << " to " << ino
                      << std::endl;
          }

	  if (!noop) {
	    FileRef file = _get_file(ino);
	    ceph_assert(file->fnode.ino);
	    map<string,DirRef>::iterator q = dir_map.find(dirname);
	    ceph_assert(q != dir_map.end());
	    map<string,FileRef>::iterator r = q->second->file_map.find(filename);
	    ceph_assert(r == q->second->file_map.end());

            vselector->sub_usage(file->vselector_hint, file->fnode);
            file->vselector_hint =
              vselector->get_hint_by_dir(dirname);
            vselector->add_usage(file->vselector_hint, file->fnode);

	    q->second->file_map[filename] = file;
	    ++file->refs;
	  }
	}
	break;

      case bluefs_transaction_t::OP_DIR_UNLINK:
        {
	  string dirname, filename;
	  decode(dirname, p);
	  decode(filename, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_dir_unlink " << " " << dirname << "/" << filename
                   << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_dir_unlink " << " " << dirname << "/" << filename
                      << std::endl;
          }
 
	  if (!noop) {
	    map<string,DirRef>::iterator q = dir_map.find(dirname);
	    ceph_assert(q != dir_map.end());
	    map<string,FileRef>::iterator r = q->second->file_map.find(filename);
	    ceph_assert(r != q->second->file_map.end());
            ceph_assert(r->second->refs > 0); 
	    --r->second->refs;
	    q->second->file_map.erase(r);
	  }
	}
	break;

      case bluefs_transaction_t::OP_DIR_CREATE:
        {
	  string dirname;
	  decode(dirname, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_dir_create " << dirname << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_dir_create " << dirname << std::endl;
          }

	  if (!noop) {
	    map<string,DirRef>::iterator q = dir_map.find(dirname);
	    ceph_assert(q == dir_map.end());
	    dir_map[dirname] = ceph::make_ref<Dir>();
	  }
	}
	break;

      case bluefs_transaction_t::OP_DIR_REMOVE:
        {
	  string dirname;
	  decode(dirname, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_dir_remove " << dirname << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_dir_remove " << dirname << std::endl;
          }

	  if (!noop) {
	    map<string,DirRef>::iterator q = dir_map.find(dirname);
	    ceph_assert(q != dir_map.end());
	    ceph_assert(q->second->file_map.empty());
	    dir_map.erase(q);
	  }
	}
	break;

      case bluefs_transaction_t::OP_FILE_UPDATE:
        {
	  bluefs_fnode_t fnode;
	  decode(fnode, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_file_update " << " " << fnode << " " << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_file_update " << " " << fnode << std::endl;
          }
          if (!noop) {
	    FileRef f = _get_file(fnode.ino);
            if (cct->_conf->bluefs_log_replay_check_allocations) {
              // check initial log layout
              if (first_log_check) {
                first_log_check = false;
                int r = _check_new_allocations(log_file->fnode,
                  MAX_BDEV, owned_blocks, used_blocks);
                if (r < 0) {
                  return r;
                }
              }
            
              auto& fnode_extents = f->fnode.extents;
              for (auto e : fnode_extents) {
                auto id = e.bdev;
		if (int r = _verify_alloc_granularity(id, e.offset, e.length,
						      "OP_FILE_UPDATE"); r < 0) {
		  return r;
		}
                apply_for_bitset_range(e.offset, e.length, alloc_size[id],
				       used_blocks[id],
                  [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
                    ceph_assert(bs.test(pos));
                    bs.reset(pos);
                  }
                );
              }
            }

            if (fnode.ino != 1) {
              vselector->sub_usage(f->vselector_hint, f->fnode);
            }
            f->fnode = fnode;
            if (fnode.ino != 1) {
              vselector->add_usage(f->vselector_hint, f->fnode);
            }

	    if (fnode.ino > ino_last) {
	      ino_last = fnode.ino;
	    }
            if (cct->_conf->bluefs_log_replay_check_allocations) {
              int r = _check_new_allocations(f->fnode,
                MAX_BDEV, owned_blocks, used_blocks);
              if (r < 0) {
                return r;
              }
            }
	  }
        }
	break;

      case bluefs_transaction_t::OP_FILE_REMOVE:
        {
	  uint64_t ino;
	  decode(ino, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_file_remove " << ino << dendl;
          if (unlikely(to_stdout)) {
            std::cout << " 0x" << std::hex << pos << std::dec
                      << ":  op_file_remove " << ino << std::endl;
          }

          if (!noop) {
            auto p = file_map.find(ino);
            ceph_assert(p != file_map.end());
            vselector->sub_usage(p->second->vselector_hint, p->second->fnode);
            if (cct->_conf->bluefs_log_replay_check_allocations) {
              auto& fnode_extents = p->second->fnode.extents;
              for (auto e : fnode_extents) {
                auto id = e.bdev;
                bool fail = false;
                apply_for_bitset_range(e.offset, e.length, alloc_size[id], owned_blocks[id],
                  [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
                    if (!bs.test(pos)) {
                      fail = true;
                    }
                  }
                );
                if (fail) {
                  derr << __func__ << " invalid extent " << int(id)
                    << ": 0x" << std::hex << e.offset << "~" << e.length
                    << std::dec
                    << ": wasn't given but is allocated for removed ino " << ino
                    << dendl;
                  return -EFAULT;
                }

                apply_for_bitset_range(e.offset, e.length, alloc_size[id], used_blocks[id],
                  [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
                    if (!bs.test(pos)) {
                      fail = true;
                    }
                    bs.reset(pos);
                  }
                );
                if (fail) {
                  derr << __func__ << " invalid extent " << int(id)
                    << ": 0x" << std::hex << e.offset << "~" << e.length
                    << std::dec
                    << ": not in use but is allocated for removed ino " << ino
                    << dendl;
                  return -EFAULT;
                }
              }
            }
            file_map.erase(p);
          }
        }
	break;

      default:
	derr << __func__ << " 0x" << std::hex << pos << std::dec
             << ": stop: unrecognized op " << (int)op << dendl;
	delete log_reader;
        return -EIO;
      }
    }
    ceph_assert(p.end());

    // we successfully replayed the transaction; bump the seq and log size
    ++log_seq;
    log_file->fnode.size = log_reader->buf.pos;
  }
  if (!noop) {
    vselector->add_usage(log_file->vselector_hint, log_file->fnode);
  }
  if (!noop && first_log_check &&
        cct->_conf->bluefs_log_replay_check_allocations) {
    int r = _check_new_allocations(log_file->fnode,
      MAX_BDEV, owned_blocks, used_blocks);
    if (r < 0) {
      return r;
    }
  }

  dout(10) << __func__ << " log file size was 0x"
           << std::hex << log_file->fnode.size << std::dec << dendl;
  if (unlikely(to_stdout)) {
    std::cout << " log file size was 0x"
              << std::hex << log_file->fnode.size << std::dec << std::endl;
  }

  delete log_reader;

  if (!noop) {
    // verify file link counts are all >0
    for (auto& p : file_map) {
      if (p.second->refs == 0 &&
	  p.second->fnode.ino > 1) {
	derr << __func__ << " file with link count 0: " << p.second->fnode
	     << dendl;
	return -EIO;
      }
    }
  }

  for (unsigned id = 0; id < block_unused_too_granular.size(); ++id) {
    dout(10) << __func__ << " block_unused_too_granular " << id << ": "
	     << block_unused_too_granular[id] << dendl;
  }
  dout(10) << __func__ << " done" << dendl;
  return 0;
}

int BlueFS::log_dump()
{
  // only dump log file's content
  ceph_assert(log_writer == nullptr && "cannot log_dump on mounted BlueFS");
  int r = _open_super();
  if (r < 0) {
    derr << __func__ << " failed to open super: " << cpp_strerror(r) << dendl;
    return r;
  }
  _init_logger();
  r = _replay(true, true);
  if (r < 0) {
    derr << __func__ << " failed to replay log: " << cpp_strerror(r) << dendl;
  }
  _shutdown_logger();
  super = bluefs_super_t();
  return r;
}

int BlueFS::device_migrate_to_existing(
  CephContext *cct,
  const set<int>& devs_source,
  int dev_target,
  const bluefs_layout_t& layout)
{
  vector<byte> buf;
  bool buffered = cct->_conf->bluefs_buffered_io;

  dout(10) << __func__ << " devs_source " << devs_source
	   << " dev_target " << dev_target << dendl;
  assert(dev_target < (int)MAX_BDEV);

  int flags = 0;
  flags |= devs_source.count(BDEV_DB) ?
    (REMOVE_DB | RENAME_SLOW2DB) : 0;
  flags |= devs_source.count(BDEV_WAL) ? REMOVE_WAL : 0;
  int dev_target_new = dev_target;

  // Slow device without separate DB one is addressed via BDEV_DB
  // Hence need renaming.
  if ((flags & REMOVE_DB) && dev_target == BDEV_SLOW) {
    dev_target_new = BDEV_DB;
    dout(0) << __func__ << " super to be written to " << dev_target << dendl;
  }

  for (auto& [ino, file_ref] : file_map) {
    //do not copy log
    if (file_ref->fnode.ino == 1) {
      continue;
    }
    dout(10) << __func__ << " " << ino << " " << file_ref->fnode << dendl;

    auto& fnode_extents = file_ref->fnode.extents;

    bool rewrite = std::any_of(
      fnode_extents.begin(),
      fnode_extents.end(),
      [=](auto& ext) {
	return ext.bdev != dev_target && devs_source.count(ext.bdev);
      });
    if (rewrite) {
      dout(10) << __func__ << "  migrating" << dendl;

      // read entire file
      bufferlist bl;
      for (auto old_ext : fnode_extents) {
	buf.resize(old_ext.length);
	int r = bdev[old_ext.bdev]->read_random(
	  old_ext.offset,
	  old_ext.length,
	  (char*)&buf.at(0),
	  buffered);
	if (r != 0) {
	  derr << __func__ << " failed to read 0x" << std::hex
	       << old_ext.offset << "~" << old_ext.length << std::dec
	       << " from " << (int)dev_target << dendl;
	  return -EIO;
	}
	bl.append((char*)&buf[0], old_ext.length);
      }

      // write entire file
      PExtentVector extents;
      auto l = _allocate_without_fallback(dev_target, bl.length(), &extents);
      if (l < 0) {
	derr << __func__ << " unable to allocate len 0x" << std::hex
	     << bl.length() << std::dec << " from " << (int)dev_target
	     << ": " << cpp_strerror(l) << dendl;
	return -ENOSPC;
      }

      uint64_t off = 0;
      for (auto& i : extents) {
	bufferlist cur;
	uint64_t cur_len = std::min<uint64_t>(i.length, bl.length() - off);
	ceph_assert(cur_len > 0);
	cur.substr_of(bl, off, cur_len);
	int r = bdev[dev_target]->write(i.offset, cur, buffered);
	ceph_assert(r == 0);
	off += cur_len;
      }

      // release old extents
      for (auto old_ext : fnode_extents) {
	PExtentVector to_release;
	to_release.emplace_back(old_ext.offset, old_ext.length);
	alloc[old_ext.bdev]->release(to_release);
      }

      // update fnode
      fnode_extents.clear();
      for (auto& i : extents) {
	fnode_extents.emplace_back(dev_target_new, i.offset, i.length);
      }
    } else {
      for (auto& ext : fnode_extents) {
	if (dev_target != dev_target_new && ext.bdev == dev_target) {
	  dout(20) << __func__ << "  " << " ... adjusting extent 0x"
		   << std::hex << ext.offset << std::dec
		   << " bdev " << dev_target << " -> " << dev_target_new
		   << dendl;
	  ext.bdev = dev_target_new;
	}
      }
    }
  }
  // new logging device in the current naming scheme
  int new_log_dev_cur = bdev[BDEV_WAL] ?
    BDEV_WAL :
    bdev[BDEV_DB] ? BDEV_DB : BDEV_SLOW;

  // new logging device in new naming scheme
  int new_log_dev_next = new_log_dev_cur;

  if (devs_source.count(new_log_dev_cur)) {
    // SLOW device is addressed via BDEV_DB too hence either WAL or DB
    new_log_dev_next = (flags & REMOVE_WAL) || !bdev[BDEV_WAL] ?
      BDEV_DB :
      BDEV_WAL;

    dout(0) << __func__ << " log moved from " << new_log_dev_cur
      << " to " << new_log_dev_next << dendl;

    new_log_dev_cur =
      (flags & REMOVE_DB) && new_log_dev_next == BDEV_DB ?
        BDEV_SLOW :
        new_log_dev_next;
  }

  _rewrite_log_and_layout_sync(
    false,
    (flags & REMOVE_DB) ? BDEV_SLOW : BDEV_DB,
    new_log_dev_cur,
    new_log_dev_next,
    flags,
    layout);
  return 0;
}

int BlueFS::device_migrate_to_new(
  CephContext *cct,
  const set<int>& devs_source,
  int dev_target,
  const bluefs_layout_t& layout)
{
  vector<byte> buf;
  bool buffered = cct->_conf->bluefs_buffered_io;

  dout(10) << __func__ << " devs_source " << devs_source
	   << " dev_target " << dev_target << dendl;
  assert(dev_target == (int)BDEV_NEWDB || (int)BDEV_NEWWAL);

  int flags = 0;

  flags |= devs_source.count(BDEV_DB) ?
    (!bdev[BDEV_SLOW] ? RENAME_DB2SLOW: REMOVE_DB) :
    0;
  flags |= devs_source.count(BDEV_WAL) ? REMOVE_WAL : 0;
  int dev_target_new = dev_target; //FIXME: remove, makes no sense

  for (auto& p : file_map) {
    //do not copy log
    if (p.second->fnode.ino == 1) {
      continue;
    }
    dout(10) << __func__ << " " << p.first << " " << p.second->fnode << dendl;

    auto& fnode_extents = p.second->fnode.extents;

    bool rewrite = false;
    for (auto ext_it = fnode_extents.begin();
	 ext_it != p.second->fnode.extents.end();
	 ++ext_it) {
      if (ext_it->bdev != dev_target && devs_source.count(ext_it->bdev)) {
	rewrite = true;
	break;
      }
    }
    if (rewrite) {
      dout(10) << __func__ << "  migrating" << dendl;

      // read entire file
      bufferlist bl;
      for (auto old_ext : fnode_extents) {
	buf.resize(old_ext.length);
	int r = bdev[old_ext.bdev]->read_random(
	  old_ext.offset,
	  old_ext.length,
	  (char*)&buf.at(0),
	  buffered);
	if (r != 0) {
	  derr << __func__ << " failed to read 0x" << std::hex
	       << old_ext.offset << "~" << old_ext.length << std::dec
	       << " from " << (int)dev_target << dendl;
	  return -EIO;
	}
	bl.append((char*)&buf[0], old_ext.length);
      }

      // write entire file
      PExtentVector extents;
      auto l = _allocate_without_fallback(dev_target, bl.length(), &extents);
      if (l < 0) {
	derr << __func__ << " unable to allocate len 0x" << std::hex
	     << bl.length() << std::dec << " from " << (int)dev_target
	     << ": " << cpp_strerror(l) << dendl;
	return -ENOSPC;
      }

      uint64_t off = 0;
      for (auto& i : extents) {
	bufferlist cur;
	uint64_t cur_len = std::min<uint64_t>(i.length, bl.length() - off);
	ceph_assert(cur_len > 0);
	cur.substr_of(bl, off, cur_len);
	int r = bdev[dev_target]->write(i.offset, cur, buffered);
	ceph_assert(r == 0);
	off += cur_len;
      }

      // release old extents
      for (auto old_ext : fnode_extents) {
	PExtentVector to_release;
	to_release.emplace_back(old_ext.offset, old_ext.length);
	alloc[old_ext.bdev]->release(to_release);
      }

      // update fnode
      fnode_extents.clear();
      for (auto& i : extents) {
	fnode_extents.emplace_back(dev_target_new, i.offset, i.length);
      }
    }
  }
  // new logging device in the current naming scheme
  int new_log_dev_cur =
    bdev[BDEV_NEWWAL] ?
      BDEV_NEWWAL :
      bdev[BDEV_WAL] && !(flags & REMOVE_WAL) ?
        BDEV_WAL :
	bdev[BDEV_NEWDB] ?
	  BDEV_NEWDB :
	  bdev[BDEV_DB] && !(flags & REMOVE_DB)?
	    BDEV_DB :
	    BDEV_SLOW;

  // new logging device in new naming scheme
  int new_log_dev_next =
    new_log_dev_cur == BDEV_NEWWAL ?
      BDEV_WAL :
      new_log_dev_cur == BDEV_NEWDB ?
	BDEV_DB :
        new_log_dev_cur;

  int super_dev =
    dev_target == BDEV_NEWDB ?
      BDEV_NEWDB :
      bdev[BDEV_DB] ?
        BDEV_DB :
	BDEV_SLOW;

  _rewrite_log_and_layout_sync(
    false,
    super_dev,
    new_log_dev_cur,
    new_log_dev_next,
    flags,
    layout);
  return 0;
}

BlueFS::FileRef BlueFS::_get_file(uint64_t ino)
{
  auto p = file_map.find(ino);
  if (p == file_map.end()) {
    FileRef f = ceph::make_ref<File>();
    file_map[ino] = f;
    dout(30) << __func__ << " ino " << ino << " = " << f
	     << " (new)" << dendl;
    return f;
  } else {
    dout(30) << __func__ << " ino " << ino << " = " << p->second << dendl;
    return p->second;
  }
}

void BlueFS::_drop_link(FileRef file)
{
  dout(20) << __func__ << " had refs " << file->refs
	   << " on " << file->fnode << dendl;
  ceph_assert(file->refs > 0);
  --file->refs;
  if (file->refs == 0) {
    dout(20) << __func__ << " destroying " << file->fnode << dendl;
    ceph_assert(file->num_reading.load() == 0);
    vselector->sub_usage(file->vselector_hint, file->fnode);
    log_t.op_file_remove(file->fnode.ino);
    for (auto& r : file->fnode.extents) {
      pending_release[r.bdev].insert(r.offset, r.length);
    }
    file_map.erase(file->fnode.ino);
    file->deleted = true;

    if (file->dirty_seq) {
      ceph_assert(file->dirty_seq > log_seq_stable);
      ceph_assert(dirty_files.count(file->dirty_seq));
      auto it = dirty_files[file->dirty_seq].iterator_to(*file);
      dirty_files[file->dirty_seq].erase(it);
      file->dirty_seq = 0;
    }
  }
}

int64_t BlueFS::_read_random(
  FileReader *h,         ///< [in] read from here
  uint64_t off,          ///< [in] offset
  uint64_t len,          ///< [in] this many bytes
  char *out)             ///< [out] copy it here
{
  auto* buf = &h->buf;

  int64_t ret = 0;
  dout(10) << __func__ << " h " << h
           << " 0x" << std::hex << off << "~" << len << std::dec
	   << " from " << h->file->fnode << dendl;

  ++h->file->num_reading;

  if (!h->ignore_eof &&
      off + len > h->file->fnode.size) {
    if (off > h->file->fnode.size)
      len = 0;
    else
      len = h->file->fnode.size - off;
    dout(20) << __func__ << " reaching (or past) eof, len clipped to 0x"
	     << std::hex << len << std::dec << dendl;
  }
  logger->inc(l_bluefs_read_random_count, 1);
  logger->inc(l_bluefs_read_random_bytes, len);

  std::shared_lock s_lock(h->lock);
  buf->bl.reassign_to_mempool(mempool::mempool_bluefs_file_reader);
  while (len > 0) {
    if (off < buf->bl_off || off >= buf->get_buf_end()) {
      s_lock.unlock();
      uint64_t x_off = 0;
      auto p = h->file->fnode.seek(off, &x_off);
      ceph_assert(p != h->file->fnode.extents.end());
      uint64_t l = std::min(p->length - x_off, len);
      //hard cap to 1GB
      l = std::min(l, uint64_t(1) << 30);
      dout(20) << __func__ << " read random 0x"
	       << std::hex << x_off << "~" << l << std::dec
	       << " of " << *p << dendl;
      int r = bdev[p->bdev]->read_random(p->offset + x_off, l, out,
					 cct->_conf->bluefs_buffered_io);
      ceph_assert(r == 0);
      off += l;
      len -= l;
      ret += l;
      out += l;

      logger->inc(l_bluefs_read_random_disk_count, 1);
      logger->inc(l_bluefs_read_random_disk_bytes, l);
      if (len > 0) {
	s_lock.lock();
      }
    } else {
      auto left = buf->get_buf_remaining(off);
      int64_t r = std::min(len, left);
      logger->inc(l_bluefs_read_random_buffer_count, 1);
      logger->inc(l_bluefs_read_random_buffer_bytes, r);
      dout(20) << __func__ << " left 0x" << std::hex << left
	      << " 0x" << off << "~" << len << std::dec
	      << dendl;

      // NOTE: h->bl is normally a contiguous buffer so c_str() is free.
      memcpy(out, buf->bl.c_str() + off - buf->bl_off, r);
      out += r;

      dout(30) << __func__ << " result chunk (0x"
	       << std::hex << r << std::dec << " bytes):\n";
      bufferlist t;
      t.substr_of(buf->bl, off - buf->bl_off, r);
      t.hexdump(*_dout);
      *_dout << dendl;

      off += r;
      len -= r;
      ret += r;
      buf->pos += r;
    }
  }
  dout(20) << __func__ << " got " << ret << dendl;
  --h->file->num_reading;
  return ret;
}

int64_t BlueFS::_read(
  FileReader *h,         ///< [in] read from here
  uint64_t off,          ///< [in] offset
  size_t len,            ///< [in] this many bytes
  bufferlist *outbl,     ///< [out] optional: reference the result here
  char *out)             ///< [out] optional: or copy it here
{
  FileReaderBuffer *buf = &(h->buf);

  bool prefetch = !outbl && !out;
  dout(10) << __func__ << " h " << h
           << " 0x" << std::hex << off << "~" << len << std::dec
	   << " from " << h->file->fnode
	   << (prefetch ? " prefetch" : "")
	   << dendl;

  ++h->file->num_reading;

  if (!h->ignore_eof &&
      off + len > h->file->fnode.size) {
    if (off > h->file->fnode.size)
      len = 0;
    else
      len = h->file->fnode.size - off;
    dout(20) << __func__ << " reaching (or past) eof, len clipped to 0x"
	     << std::hex << len << std::dec << dendl;
  }
  logger->inc(l_bluefs_read_count, 1);
  logger->inc(l_bluefs_read_bytes, len);
  if (prefetch) {
    logger->inc(l_bluefs_read_prefetch_count, 1);
    logger->inc(l_bluefs_read_prefetch_bytes, len);
  }

  if (outbl)
    outbl->clear();

  int64_t ret = 0;
  std::shared_lock s_lock(h->lock);
  while (len > 0) {
    size_t left;
    if (off < buf->bl_off || off >= buf->get_buf_end()) {
      s_lock.unlock();
      std::unique_lock u_lock(h->lock);
      buf->bl.reassign_to_mempool(mempool::mempool_bluefs_file_reader);
      if (off < buf->bl_off || off >= buf->get_buf_end()) {
        // if precondition hasn't changed during locking upgrade.
        buf->bl.clear();
        buf->bl_off = off & super.block_mask();
        uint64_t x_off = 0;
        auto p = h->file->fnode.seek(buf->bl_off, &x_off);
	if (p == h->file->fnode.extents.end()) {
	  dout(5) << __func__ << " reading less then required "
		  << ret << "<" << ret + len << dendl;
	  break;
	}

        uint64_t want = round_up_to(len + (off & ~super.block_mask()),
				    super.block_size);
        want = std::max(want, buf->max_prefetch);
        uint64_t l = std::min(p->length - x_off, want);
        //hard cap to 1GB
	l = std::min(l, uint64_t(1) << 30);
        uint64_t eof_offset = round_up_to(h->file->fnode.size, super.block_size);
        if (!h->ignore_eof &&
	    buf->bl_off + l > eof_offset) {
	  l = eof_offset - buf->bl_off;
        }
        dout(20) << __func__ << " fetching 0x"
                 << std::hex << x_off << "~" << l << std::dec
                 << " of " << *p << dendl;
        int r = bdev[p->bdev]->read(p->offset + x_off, l, &buf->bl, ioc[p->bdev],
				    cct->_conf->bluefs_buffered_io);
        ceph_assert(r == 0);
      }
      u_lock.unlock();
      s_lock.lock();
      // we should recheck if buffer is valid after lock downgrade
      continue; 
    }
    left = buf->get_buf_remaining(off);
    dout(20) << __func__ << " left 0x" << std::hex << left
             << " len 0x" << len << std::dec << dendl;

    int64_t r = std::min(len, left);
    if (outbl) {
      bufferlist t;
      t.substr_of(buf->bl, off - buf->bl_off, r);
      outbl->claim_append(t);
    }
    if (out) {
      // NOTE: h->bl is normally a contiguous buffer so c_str() is free.
      memcpy(out, buf->bl.c_str() + off - buf->bl_off, r);
      out += r;
    }

    dout(30) << __func__ << " result chunk (0x"
             << std::hex << r << std::dec << " bytes):\n";
    bufferlist t;
    t.substr_of(buf->bl, off - buf->bl_off, r);
    t.hexdump(*_dout);
    *_dout << dendl;

    off += r;
    len -= r;
    ret += r;
    buf->pos += r;
  }

  dout(20) << __func__ << " got " << ret << dendl;
  ceph_assert(!outbl || (int)outbl->length() == ret);
  --h->file->num_reading;
  return ret;
}

void BlueFS::_invalidate_cache(FileRef f, uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " file " << f->fnode
	   << " 0x" << std::hex << offset << "~" << length << std::dec
           << dendl;
  if (offset & ~super.block_mask()) {
    offset &= super.block_mask();
    length = round_up_to(length, super.block_size);
  }
  uint64_t x_off = 0;
  auto p = f->fnode.seek(offset, &x_off);
  while (length > 0 && p != f->fnode.extents.end()) {
    uint64_t x_len = std::min(p->length - x_off, length);
    bdev[p->bdev]->invalidate_cache(p->offset + x_off, x_len);
    dout(20) << __func__  << " 0x" << std::hex << x_off << "~" << x_len
             << std:: dec << " of " << *p << dendl;
    offset += x_len;
    length -= x_len;
  }
}

uint64_t BlueFS::_estimate_log_size()
{
  int avg_dir_size = 40;  // fixme
  int avg_file_size = 12;
  uint64_t size = 4096 * 2;
  size += file_map.size() * (1 + sizeof(bluefs_fnode_t));
  for (auto& p : block_all)
    size += p.num_intervals() * (1 + 1 + sizeof(uint64_t) * 2);
  size += dir_map.size() + (1 + avg_dir_size);
  size += file_map.size() * (1 + avg_dir_size + avg_file_size);
  return round_up_to(size, super.block_size);
}

void BlueFS::compact_log()
{
  std::unique_lock<ceph::mutex> l(lock);
  if (!cct->_conf->bluefs_replay_recovery_disable_compact) {
    if (cct->_conf->bluefs_compact_log_sync) {
      _compact_log_sync();
    } else {
      _compact_log_async(l);
    }
  }
}

bool BlueFS::_should_compact_log()
{
  uint64_t current = log_writer->file->fnode.size;
  uint64_t expected = _estimate_log_size();
  float ratio = (float)current / (float)expected;
  dout(10) << __func__ << " current 0x" << std::hex << current
	   << " expected " << expected << std::dec
	   << " ratio " << ratio
	   << (new_log ? " (async compaction in progress)" : "")
	   << dendl;
  if (new_log ||
      current < cct->_conf->bluefs_log_compact_min_size ||
      ratio < cct->_conf->bluefs_log_compact_min_ratio) {
    return false;
  }
  return true;
}

void BlueFS::_compact_log_dump_metadata(bluefs_transaction_t *t,
					int flags)
{
  t->seq = 1;
  t->uuid = super.uuid;
  dout(20) << __func__ << " op_init" << dendl;

  t->op_init();
  for (unsigned bdev = 0; bdev < MAX_BDEV; ++bdev) {
    interval_set<uint64_t>& p = block_all[bdev];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      auto bdev_new = bdev;
      if ((flags & REMOVE_WAL) && bdev == BDEV_WAL) {
	continue;
      }
      if ((flags & REMOVE_DB) && bdev == BDEV_DB) {
	continue;
      }
      if ((flags & RENAME_SLOW2DB) && bdev == BDEV_SLOW) {
	bdev_new = BDEV_DB;
      }
      if ((flags & RENAME_DB2SLOW) && bdev == BDEV_DB) {
	bdev_new = BDEV_SLOW;
      }
      if (bdev == BDEV_NEWDB) {
	// REMOVE_DB xor RENAME_DB
	ceph_assert(!(flags & REMOVE_DB) != !(flags & RENAME_DB2SLOW));
	ceph_assert(!(flags & RENAME_SLOW2DB));
	bdev_new = BDEV_DB;
      }
      if (bdev == BDEV_NEWWAL) {
	ceph_assert(flags & REMOVE_WAL);
	bdev_new = BDEV_WAL;
      }
      dout(20) << __func__ << " op_alloc_add " << bdev_new << " 0x"
               << std::hex << q.get_start() << "~" << q.get_len() << std::dec
               << dendl;
      t->op_alloc_add(bdev_new, q.get_start(), q.get_len());
    }
  }
  for (auto& [ino, file_ref] : file_map) {
    if (ino == 1)
      continue;
    ceph_assert(ino > 1);

    for(auto& e : file_ref->fnode.extents) {
      auto bdev = e.bdev;
      auto bdev_new = bdev;
      ceph_assert(!((flags & REMOVE_WAL) && bdev == BDEV_WAL));
      if ((flags & RENAME_SLOW2DB) && bdev == BDEV_SLOW) {
	bdev_new = BDEV_DB;
      }
      if ((flags & RENAME_DB2SLOW) && bdev == BDEV_DB) {
	bdev_new = BDEV_SLOW;
      }
      if (bdev == BDEV_NEWDB) {
	// REMOVE_DB xor RENAME_DB
	ceph_assert(!(flags & REMOVE_DB) != !(flags & RENAME_DB2SLOW));
	ceph_assert(!(flags & RENAME_SLOW2DB));
	bdev_new = BDEV_DB;
      }
      if (bdev == BDEV_NEWWAL) {
	ceph_assert(flags & REMOVE_WAL);
	bdev_new = BDEV_WAL;
      }
      e.bdev = bdev_new;
    }
    dout(20) << __func__ << " op_file_update " << file_ref->fnode << dendl;
    t->op_file_update(file_ref->fnode);
  }
  for (auto& [path, dir_ref] : dir_map) {
    dout(20) << __func__ << " op_dir_create " << path << dendl;
    t->op_dir_create(path);
    for (auto& [fname, file_ref] : dir_ref->file_map) {
      dout(20) << __func__ << " op_dir_link " << path << "/" << fname
	       << " to " << file_ref->fnode.ino << dendl;
      t->op_dir_link(path, fname, file_ref->fnode.ino);
    }
  }
}

void BlueFS::_compact_log_sync()
{
  dout(10) << __func__ << dendl;
  auto prefer_bdev =
    vselector->select_prefer_bdev(log_writer->file->vselector_hint);
  _rewrite_log_and_layout_sync(true,
    BDEV_DB,
    prefer_bdev,
    prefer_bdev,
    0,
    super.memorized_layout);
  logger->inc(l_bluefs_log_compactions);
}

void BlueFS::_rewrite_log_and_layout_sync(bool allocate_with_fallback,
					  int super_dev,
					  int log_dev,
					  int log_dev_new,
					  int flags,
					  std::optional<bluefs_layout_t> layout)
{
  File *log_file = log_writer->file.get();

  // clear out log (be careful who calls us!!!)
  log_t.clear();

  dout(20) << __func__ << " super_dev:" << super_dev
                       << " log_dev:" << log_dev
                       << " log_dev_new:" << log_dev_new
		       << " flags:" << flags
		       << dendl;
  bluefs_transaction_t t;
  _compact_log_dump_metadata(&t, flags);

  dout(20) << __func__ << " op_jump_seq " << log_seq << dendl;
  t.op_jump_seq(log_seq);

  bufferlist bl;
  encode(t, bl);
  _pad_bl(bl);

  uint64_t need = bl.length() + cct->_conf->bluefs_max_log_runway;
  dout(20) << __func__ << " need " << need << dendl;

  bluefs_fnode_t old_fnode;
  int r;
  log_file->fnode.swap_extents(old_fnode);
  if (allocate_with_fallback) {
    r = _allocate(log_dev, need, &log_file->fnode);
    ceph_assert(r == 0);
  } else {
    PExtentVector extents;
    r = _allocate_without_fallback(log_dev,
			       need,
			       &extents);
    ceph_assert(r == 0);
    for (auto& p : extents) {
      log_file->fnode.append_extent(
	bluefs_extent_t(log_dev, p.offset, p.length));
    }
  }

  _close_writer(log_writer);

  log_file->fnode.size = bl.length();
  vselector->sub_usage(log_file->vselector_hint, old_fnode);
  vselector->add_usage(log_file->vselector_hint, log_file->fnode);

  log_writer = _create_writer(log_file);
  log_writer->append(bl);
  r = _flush(log_writer, true);
  ceph_assert(r == 0);
#ifdef HAVE_LIBAIO
  if (!cct->_conf->bluefs_sync_write) {
    list<aio_t> completed_ios;
    _claim_completed_aios(log_writer, &completed_ios);
    wait_for_aio(log_writer);
    completed_ios.clear();
  }
#endif
  flush_bdev();

  super.memorized_layout = layout;
  super.log_fnode = log_file->fnode;
  // rename device if needed
  if (log_dev != log_dev_new) {
    dout(10) << __func__ << " renaming log extents to " << log_dev_new << dendl;
    for (auto& p : super.log_fnode.extents) {
      p.bdev = log_dev_new;
    }
  }
  dout(10) << __func__ << " writing super, log fnode: " << super.log_fnode << dendl;

  ++super.version;
  _write_super(super_dev);
  flush_bdev();

  dout(10) << __func__ << " release old log extents " << old_fnode.extents << dendl;
  for (auto& r : old_fnode.extents) {
    pending_release[r.bdev].insert(r.offset, r.length);
  }
}

/*
 * 1. Allocate a new extent to continue the log, and then log an event
 * that jumps the log write position to the new extent.  At this point, the
 * old extent(s) won't be written to, and reflect everything to compact.
 * New events will be written to the new region that we'll keep.
 *
 * 2. While still holding the lock, encode a bufferlist that dumps all of the
 * in-memory fnodes and names.  This will become the new beginning of the
 * log.  The last event will jump to the log continuation extent from #1.
 *
 * 3. Queue a write to a new extent for the new beginnging of the log.
 *
 * 4. Drop lock and wait
 *
 * 5. Retake the lock.
 *
 * 6. Update the log_fnode to splice in the new beginning.
 *
 * 7. Write the new superblock.
 *
 * 8. Release the old log space.  Clean up.
 */
void BlueFS::_compact_log_async(std::unique_lock<ceph::mutex>& l)
{
  dout(10) << __func__ << dendl;
  File *log_file = log_writer->file.get();
  ceph_assert(!new_log);
  ceph_assert(!new_log_writer);

  // create a new log [writer] so that we know compaction is in progress
  // (see _should_compact_log)
  new_log = ceph::make_ref<File>();
  new_log->fnode.ino = 0;   // so that _flush_range won't try to log the fnode

  // 0. wait for any racing flushes to complete.  (We do not want to block
  // in _flush_sync_log with jump_to set or else a racing thread might flush
  // our entries and our jump_to update won't be correct.)
  while (log_flushing) {
    dout(10) << __func__ << " log is currently flushing, waiting" << dendl;
    log_cond.wait(l);
  }

  vselector->sub_usage(log_file->vselector_hint, log_file->fnode);

  // 1. allocate new log space and jump to it.
  old_log_jump_to = log_file->fnode.get_allocated();
  dout(10) << __func__ << " old_log_jump_to 0x" << std::hex << old_log_jump_to
           << " need 0x" << (old_log_jump_to + cct->_conf->bluefs_max_log_runway) << std::dec << dendl;
  int r = _allocate(vselector->select_prefer_bdev(log_file->vselector_hint),
		    cct->_conf->bluefs_max_log_runway,
                    &log_file->fnode);
  ceph_assert(r == 0);
  //adjust usage as flush below will need it
  vselector->add_usage(log_file->vselector_hint, log_file->fnode);
  dout(10) << __func__ << " log extents " << log_file->fnode.extents << dendl;

  // update the log file change and log a jump to the offset where we want to
  // write the new entries
  log_t.op_file_update(log_file->fnode);
  log_t.op_jump(log_seq, old_log_jump_to);

  flush_bdev();  // FIXME?

  _flush_and_sync_log(l, 0, old_log_jump_to);

  // 2. prepare compacted log
  bluefs_transaction_t t;
  //avoid record two times in log_t and _compact_log_dump_metadata.
  log_t.clear();
  _compact_log_dump_metadata(&t, 0);

  uint64_t max_alloc_size = std::max(alloc_size[BDEV_WAL],
				     std::max(alloc_size[BDEV_DB],
					      alloc_size[BDEV_SLOW]));

  // conservative estimate for final encoded size
  new_log_jump_to = round_up_to(t.op_bl.length() + super.block_size * 2,
                                max_alloc_size);
  t.op_jump(log_seq, new_log_jump_to);

  // allocate
  //FIXME: check if we want DB here?
  r = _allocate(BlueFS::BDEV_DB, new_log_jump_to,
                    &new_log->fnode);
  ceph_assert(r == 0);

  // we might have some more ops in log_t due to _allocate call
  t.claim_ops(log_t);

  bufferlist bl;
  encode(t, bl);
  _pad_bl(bl);

  dout(10) << __func__ << " new_log_jump_to 0x" << std::hex << new_log_jump_to
	   << std::dec << dendl;

  new_log_writer = _create_writer(new_log);
  new_log_writer->append(bl);

  // 3. flush
  r = _flush(new_log_writer, true);
  ceph_assert(r == 0);

  // 4. wait
  _flush_bdev_safely(new_log_writer);

  // 5. update our log fnode
  // discard first old_log_jump_to extents

  dout(10) << __func__ << " remove 0x" << std::hex << old_log_jump_to << std::dec
	   << " of " << log_file->fnode.extents << dendl;
  uint64_t discarded = 0;
  mempool::bluefs::vector<bluefs_extent_t> old_extents;
  while (discarded < old_log_jump_to) {
    ceph_assert(!log_file->fnode.extents.empty());
    bluefs_extent_t& e = log_file->fnode.extents.front();
    bluefs_extent_t temp = e;
    if (discarded + e.length <= old_log_jump_to) {
      dout(10) << __func__ << " remove old log extent " << e << dendl;
      discarded += e.length;
      log_file->fnode.pop_front_extent();
    } else {
      dout(10) << __func__ << " remove front of old log extent " << e << dendl;
      uint64_t drop = old_log_jump_to - discarded;
      temp.length = drop;
      e.offset += drop;
      e.length -= drop;
      discarded += drop;
      dout(10) << __func__ << "   kept " << e << " removed " << temp << dendl;
    }
    old_extents.push_back(temp);
  }
  auto from = log_file->fnode.extents.begin();
  auto to = log_file->fnode.extents.end();
  while (from != to) {
    new_log->fnode.append_extent(*from);
    ++from;
  }

  vselector->sub_usage(log_file->vselector_hint, log_file->fnode);

  // clear the extents from old log file, they are added to new log
  log_file->fnode.clear_extents();
  // swap the log files. New log file is the log file now.
  new_log->fnode.swap_extents(log_file->fnode);

  log_writer->pos = log_writer->file->fnode.size =
    log_writer->pos - old_log_jump_to + new_log_jump_to;

  vselector->add_usage(log_file->vselector_hint, log_file->fnode);

  // 6. write the super block to reflect the changes
  dout(10) << __func__ << " writing super" << dendl;
  super.log_fnode = log_file->fnode;
  ++super.version;
  _write_super(BDEV_DB);

  lock.unlock();
  flush_bdev();
  lock.lock();

  // 7. release old space
  dout(10) << __func__ << " release old log extents " << old_extents << dendl;
  for (auto& r : old_extents) {
    pending_release[r.bdev].insert(r.offset, r.length);
  }

  // delete the new log, remove from the dirty files list
  _close_writer(new_log_writer);
  if (new_log->dirty_seq) {
    ceph_assert(dirty_files.count(new_log->dirty_seq));
    auto it = dirty_files[new_log->dirty_seq].iterator_to(*new_log);
    dirty_files[new_log->dirty_seq].erase(it);
  }
  new_log_writer = nullptr;
  new_log = nullptr;
  log_cond.notify_all();

  dout(10) << __func__ << " log extents " << log_file->fnode.extents << dendl;
  logger->inc(l_bluefs_log_compactions);
}

void BlueFS::_pad_bl(bufferlist& bl)
{
  uint64_t partial = bl.length() % super.block_size;
  if (partial) {
    dout(10) << __func__ << " padding with 0x" << std::hex
	     << super.block_size - partial << " zeros" << std::dec << dendl;
    bl.append_zero(super.block_size - partial);
  }
}


int BlueFS::_flush_and_sync_log(std::unique_lock<ceph::mutex>& l,
				uint64_t want_seq,
				uint64_t jump_to)
{
  while (log_flushing) {
    dout(10) << __func__ << " want_seq " << want_seq
	     << " log is currently flushing, waiting" << dendl;
    ceph_assert(!jump_to);
    log_cond.wait(l);
  }
  if (want_seq && want_seq <= log_seq_stable) {
    dout(10) << __func__ << " want_seq " << want_seq << " <= log_seq_stable "
	     << log_seq_stable << ", done" << dendl;
    ceph_assert(!jump_to);
    return 0;
  }
  if (log_t.empty() && dirty_files.empty()) {
    dout(10) << __func__ << " want_seq " << want_seq
	     << " " << log_t << " not dirty, dirty_files empty, no-op" << dendl;
    ceph_assert(!jump_to);
    return 0;
  }

  vector<interval_set<uint64_t>> to_release(pending_release.size());
  to_release.swap(pending_release);

  uint64_t seq = log_t.seq = ++log_seq;
  ceph_assert(want_seq == 0 || want_seq <= seq);
  log_t.uuid = super.uuid;

  // log dirty files
  auto lsi = dirty_files.find(seq);
  if (lsi != dirty_files.end()) {
    dout(20) << __func__ << " " << lsi->second.size() << " dirty_files" << dendl;
    for (auto &f : lsi->second) {
      dout(20) << __func__ << "   op_file_update " << f.fnode << dendl;
      log_t.op_file_update(f.fnode);
    }
  }

  dout(10) << __func__ << " " << log_t << dendl;
  ceph_assert(!log_t.empty());

  // allocate some more space (before we run out)?
  int64_t runway = log_writer->file->fnode.get_allocated() -
    log_writer->get_effective_write_pos();
  bool just_expanded_log = false;
  if (runway < (int64_t)cct->_conf->bluefs_min_log_runway) {
    dout(10) << __func__ << " allocating more log runway (0x"
	     << std::hex << runway << std::dec  << " remaining)" << dendl;
    while (new_log_writer) {
      dout(10) << __func__ << " waiting for async compaction" << dendl;
      log_cond.wait(l);
    }
    vselector->sub_usage(log_writer->file->vselector_hint, log_writer->file->fnode);
    int r = _allocate(
      vselector->select_prefer_bdev(log_writer->file->vselector_hint),
      cct->_conf->bluefs_max_log_runway,
      &log_writer->file->fnode);
    ceph_assert(r == 0);
    vselector->add_usage(log_writer->file->vselector_hint, log_writer->file->fnode);
    log_t.op_file_update(log_writer->file->fnode);
    just_expanded_log = true;
  }

  bufferlist bl;
  bl.reserve(super.block_size);
  encode(log_t, bl);
  // pad to block boundary
  size_t realign = super.block_size - (bl.length() % super.block_size);
  if (realign && realign != super.block_size)
    bl.append_zero(realign);

  logger->inc(l_bluefs_logged_bytes, bl.length());

  if (just_expanded_log) {
    ceph_assert(bl.length() <= runway); // if we write this, we will have an unrecoverable data loss
  }

  log_writer->append(bl);

  log_t.clear();
  log_t.seq = 0;  // just so debug output is less confusing
  log_flushing = true;

  int r = _flush(log_writer, true);
  ceph_assert(r == 0);

  if (jump_to) {
    dout(10) << __func__ << " jumping log offset from 0x" << std::hex
	     << log_writer->pos << " -> 0x" << jump_to << std::dec << dendl;
    log_writer->pos = jump_to;
    vselector->sub_usage(log_writer->file->vselector_hint, log_writer->file->fnode.size);
    log_writer->file->fnode.size = jump_to;
    vselector->add_usage(log_writer->file->vselector_hint, log_writer->file->fnode.size);
  }

  _flush_bdev_safely(log_writer);

  log_flushing = false;
  log_cond.notify_all();

  // clean dirty files
  if (seq > log_seq_stable) {
    log_seq_stable = seq;
    dout(20) << __func__ << " log_seq_stable " << log_seq_stable << dendl;

    auto p = dirty_files.begin();
    while (p != dirty_files.end()) {
      if (p->first > log_seq_stable) {
        dout(20) << __func__ << " done cleaning up dirty files" << dendl;
        break;
      }

      auto l = p->second.begin();
      while (l != p->second.end()) {
        File *file = &*l;
        ceph_assert(file->dirty_seq > 0);
        ceph_assert(file->dirty_seq <= log_seq_stable);
        dout(20) << __func__ << " cleaned file " << file->fnode << dendl;
        file->dirty_seq = 0;
        p->second.erase(l++);
      }

      ceph_assert(p->second.empty());
      dirty_files.erase(p++);
    }
  } else {
    dout(20) << __func__ << " log_seq_stable " << log_seq_stable
             << " already >= out seq " << seq
             << ", we lost a race against another log flush, done" << dendl;
  }

  for (unsigned i = 0; i < to_release.size(); ++i) {
    if (!to_release[i].empty()) {
      /* OK, now we have the guarantee alloc[i] won't be null. */
      int r = 0;
      if (cct->_conf->bdev_enable_discard && cct->_conf->bdev_async_discard) {
	r = bdev[i]->queue_discard(to_release[i]);
	if (r == 0)
	  continue;
      } else if (cct->_conf->bdev_enable_discard) {
	for (auto p = to_release[i].begin(); p != to_release[i].end(); ++p) {
	  bdev[i]->discard(p.get_start(), p.get_len());
	}
      }
      alloc[i]->release(to_release[i]);
    }
  }

  _update_logger_stats();

  return 0;
}

int BlueFS::_flush_range(FileWriter *h, uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " " << h << " pos 0x" << std::hex << h->pos
	   << " 0x" << offset << "~" << length << std::dec
	   << " to " << h->file->fnode << dendl;
  ceph_assert(!h->file->deleted);
  ceph_assert(h->file->num_readers.load() == 0);

  h->buffer_appender.flush();

  bool buffered;
  if (h->file->fnode.ino == 1)
    buffered = false;
  else
    buffered = cct->_conf->bluefs_buffered_io;

  if (offset + length <= h->pos)
    return 0;
  if (offset < h->pos) {
    length -= h->pos - offset;
    offset = h->pos;
    dout(10) << " still need 0x"
             << std::hex << offset << "~" << length << std::dec
             << dendl;
  }
  ceph_assert(offset <= h->file->fnode.size);

  uint64_t allocated = h->file->fnode.get_allocated();
  vselector->sub_usage(h->file->vselector_hint, h->file->fnode);
  // do not bother to dirty the file if we are overwriting
  // previously allocated extents.
  bool must_dirty = false;
  if (allocated < offset + length) {
    // we should never run out of log space here; see the min runway check
    // in _flush_and_sync_log.
    ceph_assert(h->file->fnode.ino != 1);
    int r = _allocate(vselector->select_prefer_bdev(h->file->vselector_hint),
		      offset + length - allocated,
		      &h->file->fnode);
    if (r < 0) {
      derr << __func__ << " allocated: 0x" << std::hex << allocated
           << " offset: 0x" << offset << " length: 0x" << length << std::dec
           << dendl;
      vselector->add_usage(h->file->vselector_hint, h->file->fnode); // undo
      ceph_abort_msg("bluefs enospc");
      return r;
    }
    must_dirty = true;
  }
  if (h->file->fnode.size < offset + length) {
    h->file->fnode.size = offset + length;
    if (h->file->fnode.ino > 1) {
      // we do not need to dirty the log file (or it's compacting
      // replacement) when the file size changes because replay is
      // smart enough to discover it on its own.
      must_dirty = true;
    }
  }
  if (must_dirty) {
    h->file->fnode.mtime = ceph_clock_now();
    ceph_assert(h->file->fnode.ino >= 1);
    if (h->file->dirty_seq == 0) {
      h->file->dirty_seq = log_seq + 1;
      dirty_files[h->file->dirty_seq].push_back(*h->file);
      dout(20) << __func__ << " dirty_seq = " << log_seq + 1
	       << " (was clean)" << dendl;
    } else {
      if (h->file->dirty_seq != log_seq + 1) {
        // need re-dirty, erase from list first
        ceph_assert(dirty_files.count(h->file->dirty_seq));
        auto it = dirty_files[h->file->dirty_seq].iterator_to(*h->file);
        dirty_files[h->file->dirty_seq].erase(it);
        h->file->dirty_seq = log_seq + 1;
        dirty_files[h->file->dirty_seq].push_back(*h->file);
        dout(20) << __func__ << " dirty_seq = " << log_seq + 1
                 << " (was " << h->file->dirty_seq << ")" << dendl;
      } else {
        dout(20) << __func__ << " dirty_seq = " << log_seq + 1
                 << " (unchanged, do nothing) " << dendl;
      }
    }
  }
  dout(20) << __func__ << " file now " << h->file->fnode << dendl;

  uint64_t x_off = 0;
  auto p = h->file->fnode.seek(offset, &x_off);
  ceph_assert(p != h->file->fnode.extents.end());
  dout(20) << __func__ << " in " << *p << " x_off 0x"
           << std::hex << x_off << std::dec << dendl;

  unsigned partial = x_off & ~super.block_mask();
  bufferlist bl;
  if (partial) {
    dout(20) << __func__ << " using partial tail 0x"
             << std::hex << partial << std::dec << dendl;
    ceph_assert(h->tail_block.length() == partial);
    bl.claim_append_piecewise(h->tail_block);
    x_off -= partial;
    offset -= partial;
    length += partial;
    dout(20) << __func__ << " waiting for previous aio to complete" << dendl;
    for (auto p : h->iocv) {
      if (p) {
	p->aio_wait();
      }
    }
  }
  if (length == partial + h->buffer.length()) {
    /* in case of inital allocation and need to zero, limited flush is unacceptable */
    bl.claim_append_piecewise(h->buffer);
  } else {
    bufferlist t;
    h->buffer.splice(0, length, &t);
    bl.claim_append_piecewise(t);
    t.substr_of(h->buffer, length, h->buffer.length() - length);
    h->buffer.swap(t);
    dout(20) << " leaving 0x" << std::hex << h->buffer.length() << std::dec
             << " unflushed" << dendl;
  }
  ceph_assert(bl.length() == length);

  h->pos = offset + length;

  unsigned tail = bl.length() & ~super.block_mask();
  if (tail) {
    dout(20) << __func__ << " caching tail of 0x"
             << std::hex << tail
             << " and padding block with 0x" << (super.block_size - tail)
             << std::dec << dendl;
    h->tail_block.substr_of(bl, bl.length() - tail, tail);
    bl.append_zero(super.block_size - tail);
    length += super.block_size - tail;
  } else {
    h->tail_block.clear();
  }
  ceph_assert(bl.length() == length);

  switch (h->writer_type) {
  case WRITER_WAL:
    logger->inc(l_bluefs_bytes_written_wal, length);
    break;
  case WRITER_SST:
    logger->inc(l_bluefs_bytes_written_sst, length);
    break;
  }

  dout(30) << "dump:\n";
  bl.hexdump(*_dout);
  *_dout << dendl;

  uint64_t bloff = 0;
  uint64_t bytes_written_slow = 0;
  while (length > 0) {
    uint64_t x_len = std::min(p->length - x_off, length);
    bufferlist t;
    t.substr_of(bl, bloff, x_len);
    if (cct->_conf->bluefs_sync_write) {
      bdev[p->bdev]->write(p->offset + x_off, t, buffered, h->write_hint);
    } else {
      bdev[p->bdev]->aio_write(p->offset + x_off, t, h->iocv[p->bdev], buffered, h->write_hint);
    }
    h->dirty_devs[p->bdev] = true;
    if (p->bdev == BDEV_SLOW) {
      bytes_written_slow += t.length();
    }

    bloff += x_len;
    length -= x_len;
    ++p;
    x_off = 0;
  }
  if (bytes_written_slow) {
    logger->inc(l_bluefs_bytes_written_slow, bytes_written_slow);
  }
  for (unsigned i = 0; i < MAX_BDEV; ++i) {
    if (bdev[i]) {
      if (h->iocv[i] && h->iocv[i]->has_pending_aios()) {
        bdev[i]->aio_submit(h->iocv[i]);
      }
    }
  }
  vselector->add_usage(h->file->vselector_hint, h->file->fnode);
  dout(20) << __func__ << " h " << h << " pos now 0x"
           << std::hex << h->pos << std::dec << dendl;
  return 0;
}

#ifdef HAVE_LIBAIO
// we need to retire old completed aios so they don't stick around in
// memory indefinitely (along with their bufferlist refs).
void BlueFS::_claim_completed_aios(FileWriter *h, list<aio_t> *ls)
{
  for (auto p : h->iocv) {
    if (p) {
      ls->splice(ls->end(), p->running_aios);
    }
  }
  dout(10) << __func__ << " got " << ls->size() << " aios" << dendl;
}

void BlueFS::wait_for_aio(FileWriter *h)
{
  // NOTE: this is safe to call without a lock, as long as our reference is
  // stable.
  utime_t start;
  lgeneric_subdout(cct, bluefs, 10) << __func__;
  start = ceph_clock_now();
  *_dout << " " << h << dendl;
  for (auto p : h->iocv) {
    if (p) {
      p->aio_wait();
    }
  }
  dout(10) << __func__ << " " << h << " done in " << (ceph_clock_now() - start) << dendl;
}
#endif

int BlueFS::_flush(FileWriter *h, bool force, std::unique_lock<ceph::mutex>& l)
{
  bool flushed = false;
  int r = _flush(h, force, &flushed);
  if (r == 0 && flushed) {
    _maybe_compact_log(l);
  }
  return r;
}

int BlueFS::_flush(FileWriter *h, bool force, bool *flushed)
{
  h->buffer_appender.flush();
  uint64_t length = h->buffer.length();
  uint64_t offset = h->pos;
  if (flushed) {
    *flushed = false;
  }
  if (!force &&
      length < cct->_conf->bluefs_min_flush_size) {
    dout(10) << __func__ << " " << h << " ignoring, length " << length
	     << " < min_flush_size " << cct->_conf->bluefs_min_flush_size
	     << dendl;
    return 0;
  }
  if (length == 0) {
    dout(10) << __func__ << " " << h << " no dirty data on "
	     << h->file->fnode << dendl;
    return 0;
  }
  dout(10) << __func__ << " " << h << " 0x"
           << std::hex << offset << "~" << length << std::dec
	   << " to " << h->file->fnode << dendl;
  ceph_assert(h->pos <= h->file->fnode.size);
  int r = _flush_range(h, offset, length);
  if (flushed) {
    *flushed = true;
  }
  return r;
}

int BlueFS::_truncate(FileWriter *h, uint64_t offset)
{
  dout(10) << __func__ << " 0x" << std::hex << offset << std::dec
           << " file " << h->file->fnode << dendl;
  if (h->file->deleted) {
    dout(10) << __func__ << "  deleted, no-op" << dendl;
    return 0;
  }

  // we never truncate internal log files
  ceph_assert(h->file->fnode.ino > 1);

  h->buffer_appender.flush();

  // truncate off unflushed data?
  if (h->pos < offset &&
      h->pos + h->buffer.length() > offset) {
    bufferlist t;
    dout(20) << __func__ << " tossing out last " << offset - h->pos
	     << " unflushed bytes" << dendl;
    t.substr_of(h->buffer, 0, offset - h->pos);
    h->buffer.swap(t);
    ceph_abort_msg("actually this shouldn't happen");
  }
  if (h->buffer.length()) {
    int r = _flush(h, true);
    if (r < 0)
      return r;
  }
  if (offset == h->file->fnode.size) {
    return 0;  // no-op!
  }
  if (offset > h->file->fnode.size) {
    ceph_abort_msg("truncate up not supported");
  }
  ceph_assert(h->file->fnode.size >= offset);
  vselector->sub_usage(h->file->vselector_hint, h->file->fnode.size);
  h->file->fnode.size = offset;
  vselector->add_usage(h->file->vselector_hint, h->file->fnode.size);
  log_t.op_file_update(h->file->fnode);
  return 0;
}

int BlueFS::_fsync(FileWriter *h, std::unique_lock<ceph::mutex>& l)
{
  dout(10) << __func__ << " " << h << " " << h->file->fnode << dendl;
  int r = _flush(h, true);
  if (r < 0)
     return r;
  uint64_t old_dirty_seq = h->file->dirty_seq;

  _flush_bdev_safely(h);

  if (old_dirty_seq) {
    uint64_t s = log_seq;
    dout(20) << __func__ << " file metadata was dirty (" << old_dirty_seq
	     << ") on " << h->file->fnode << ", flushing log" << dendl;
    _flush_and_sync_log(l, old_dirty_seq);
    ceph_assert(h->file->dirty_seq == 0 ||  // cleaned
	   h->file->dirty_seq > s);    // or redirtied by someone else
  }
  return 0;
}

void BlueFS::_flush_bdev_safely(FileWriter *h)
{
  std::array<bool, MAX_BDEV> flush_devs = h->dirty_devs;
  h->dirty_devs.fill(false);
#ifdef HAVE_LIBAIO
  if (!cct->_conf->bluefs_sync_write) {
    list<aio_t> completed_ios;
    _claim_completed_aios(h, &completed_ios);
    lock.unlock();
    wait_for_aio(h);
    completed_ios.clear();
    flush_bdev(flush_devs);
    lock.lock();
  } else
#endif
  {
    lock.unlock();
    flush_bdev(flush_devs);
    lock.lock();
  }
}

void BlueFS::flush_bdev(std::array<bool, MAX_BDEV>& dirty_bdevs)
{
  // NOTE: this is safe to call without a lock.
  dout(20) << __func__ << dendl;
  for (unsigned i = 0; i < MAX_BDEV; i++) {
    if (dirty_bdevs[i])
      bdev[i]->flush();
  }
}

void BlueFS::flush_bdev()
{
  // NOTE: this is safe to call without a lock.
  dout(20) << __func__ << dendl;
  for (unsigned i = 0; i < MAX_BDEV; i++) {
    // alloc space from BDEV_SLOW is unexpected.
    // So most cases we don't alloc from BDEV_SLOW and so avoiding flush not-used device.
    if (bdev[i] && ((i != BDEV_SLOW) || (block_all[i].size() - alloc[i]->get_free()))) {
      bdev[i]->flush();
    }
  }
}

const char* BlueFS::get_device_name(unsigned id)
{
  if (id >= MAX_BDEV) return "BDEV_INV";
  const char* names[] = {"BDEV_WAL", "BDEV_DB", "BDEV_SLOW", "BDEV_NEWWAL", "BDEV_NEWDB"};
  return names[id];
}

int BlueFS::_expand_slow_device(uint64_t need, PExtentVector& extents)
{
  int r = -ENOSPC;
  if (slow_dev_expander) {
    auto id = _get_slow_device_id();
    auto min_alloc_size = alloc_size[id];
    ceph_assert(id <= alloc.size() && alloc[id]);
    auto min_need = round_up_to(need, min_alloc_size);
    need = std::max(need,
      slow_dev_expander->get_recommended_expansion_delta(
        alloc[id]->get_free(), block_all[id].size()));

    need = round_up_to(need, min_alloc_size);
    dout(10) << __func__ << " expanding slow device by 0x"
             << std::hex << need << std::dec
	     << dendl;
    r = slow_dev_expander->allocate_freespace(min_need, need, extents);
  }
  return r;
}

int BlueFS::_allocate_without_fallback(uint8_t id, uint64_t len,
		      PExtentVector* extents)
{
  dout(10) << __func__ << " len 0x" << std::hex << len << std::dec
           << " from " << (int)id << dendl;
  assert(id < alloc.size());
  if (!alloc[id]) {
    return -ENOENT;
  }
  extents->reserve(4);  // 4 should be (more than) enough for most allocations
  uint64_t min_alloc_size = alloc_size[id];
  uint64_t left = round_up_to(len, min_alloc_size);
  int64_t alloc_len = alloc[id]->allocate(left, min_alloc_size, 0, extents);
  if (alloc_len < 0 || alloc_len < (int64_t)left) {
    if (alloc_len > 0) {
      alloc[id]->release(*extents);
    }
    if (bdev[id])
      derr << __func__ << " failed to allocate 0x" << std::hex << left
	   << " on bdev " << (int)id
	   << ", free 0x" << alloc[id]->get_free() << std::dec << dendl;
    else
      derr << __func__ << " failed to allocate 0x" << std::hex << left
	   << " on bdev " << (int)id << ", dne" << std::dec << dendl;
    if (alloc[id])
      alloc[id]->dump();
    return -ENOSPC;
  }

  return 0;
}

int BlueFS::_allocate(uint8_t id, uint64_t len,
		      bluefs_fnode_t* node)
{
  dout(10) << __func__ << " len 0x" << std::hex << len << std::dec
           << " from " << (int)id << dendl;
  ceph_assert(id < alloc.size());
  int64_t alloc_len = 0;
  PExtentVector extents;
  uint64_t hint = 0;
  if (alloc[id]) {
    if (!node->extents.empty() && node->extents.back().bdev == id) {
      hint = node->extents.back().end();
    }   
    extents.reserve(4);  // 4 should be (more than) enough for most allocations
    alloc_len = alloc[id]->allocate(round_up_to(len, alloc_size[id]),
				    alloc_size[id], hint, &extents);
  }
  if (!alloc[id] ||
      alloc_len < 0 ||
      alloc_len < (int64_t)round_up_to(len, alloc_size[id])) {
    if (alloc_len > 0) {
      alloc[id]->release(extents);
    }
    if (id != BDEV_SLOW) {
      if (bdev[id]) {
	dout(1) << __func__ << " failed to allocate 0x" << std::hex << len
		<< " on bdev " << (int)id
		<< ", free 0x" << alloc[id]->get_free()
		<< "; fallback to bdev " << (int)id + 1
		<< std::dec << dendl;
      }
      return _allocate(id + 1, len, node);
    }
    dout(1) << __func__ << " unable to allocate 0x" << std::hex << len
	    << " on bdev " << (int)id << ", free 0x"
	    << (alloc[id] ? alloc[id]->get_free() : (uint64_t)-1)
	    << "; fallback to slow device expander "
	    << std::dec << dendl;
    extents.clear();
    if (_expand_slow_device(len, extents) == 0) {
      id = _get_slow_device_id();
      for (auto& e : extents) {
	_add_block_extent(id, e.offset, e.length);
      }
      extents.clear();
      auto* last_alloc = alloc[id];
      ceph_assert(last_alloc);
      // try again
      alloc_len = last_alloc->allocate(round_up_to(len, alloc_size[id]),
				       alloc_size[id], hint, &extents);
      if (alloc_len < 0 || alloc_len < (int64_t)len) {
	if (alloc_len > 0) {
	  last_alloc->release(extents);
	}
	derr << __func__ << " failed to allocate 0x" << std::hex << len
	      << " on bdev " << (int)id
	      << ", free 0x" << last_alloc->get_free() << std::dec << dendl;
        return -ENOSPC;
      }
    } else {
      derr << __func__ << " failed to expand slow device to fit +0x"
	   << std::hex << len << std::dec
	   << dendl;
      return -ENOSPC;
    }
  } else {
    uint64_t total_allocated =
      block_all[id].size() - alloc[id]->get_free();
    if (max_bytes[id] < total_allocated) {
      logger->set(max_bytes_pcounters[id], total_allocated);
      max_bytes[id] = total_allocated;
    }
  }

  for (auto& p : extents) {
    node->append_extent(bluefs_extent_t(id, p.offset, p.length));
  }
   
  return 0;
}

int BlueFS::_preallocate(FileRef f, uint64_t off, uint64_t len)
{
  dout(10) << __func__ << " file " << f->fnode << " 0x"
	   << std::hex << off << "~" << len << std::dec << dendl;
  if (f->deleted) {
    dout(10) << __func__ << "  deleted, no-op" << dendl;
    return 0;
  }
  ceph_assert(f->fnode.ino > 1);
  uint64_t allocated = f->fnode.get_allocated();
  if (off + len > allocated) {
    uint64_t want = off + len - allocated;
    vselector->sub_usage(f->vselector_hint, f->fnode);

    int r = _allocate(vselector->select_prefer_bdev(f->vselector_hint),
      want,
      &f->fnode);
    vselector->add_usage(f->vselector_hint, f->fnode);
    if (r < 0)
      return r;
    log_t.op_file_update(f->fnode);
  }
  return 0;
}

void BlueFS::sync_metadata(bool avoid_compact)
{
  std::unique_lock l(lock);
  if (log_t.empty() && dirty_files.empty()) {
    dout(10) << __func__ << " - no pending log events" << dendl;
  } else {
    utime_t start;
    lgeneric_subdout(cct, bluefs, 10) << __func__;
    start = ceph_clock_now();
    *_dout <<  dendl;
    flush_bdev(); // FIXME?
    _flush_and_sync_log(l);
    dout(10) << __func__ << " done in " << (ceph_clock_now() - start) << dendl;
  }

  if (!avoid_compact) {
    _maybe_compact_log(l);
  }
}

void BlueFS::_maybe_compact_log(std::unique_lock<ceph::mutex>& l)
{
  if (!cct->_conf->bluefs_replay_recovery_disable_compact &&
      _should_compact_log()) {
    if (cct->_conf->bluefs_compact_log_sync) {
      _compact_log_sync();
    } else {
      _compact_log_async(l);
    }
  }
}

int BlueFS::open_for_write(
  const string& dirname,
  const string& filename,
  FileWriter **h,
  bool overwrite)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  DirRef dir;
  if (p == dir_map.end()) {
    // implicitly create the dir
    dout(20) << __func__ << "  dir " << dirname
	     << " does not exist" << dendl;
    return -ENOENT;
  } else {
    dir = p->second;
  }

  FileRef file;
  bool create = false;
  bool truncate = false;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    if (overwrite) {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " does not exist" << dendl;
      return -ENOENT;
    }
    file = ceph::make_ref<File>();
    file->fnode.ino = ++ino_last;
    file_map[ino_last] = file;
    dir->file_map[filename] = file;
    ++file->refs;
    create = true;
  } else {
    // overwrite existing file?
    file = q->second;
    if (overwrite) {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " already exists, overwrite in place" << dendl;
    } else {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " already exists, truncate + overwrite" << dendl;
      vselector->sub_usage(file->vselector_hint, file->fnode);
      file->fnode.size = 0;
      for (auto& p : file->fnode.extents) {
	pending_release[p.bdev].insert(p.offset, p.length);
      }
      truncate = true;

      file->fnode.clear_extents();
    }
  }
  ceph_assert(file->fnode.ino > 1);

  file->fnode.mtime = ceph_clock_now();
  file->vselector_hint = vselector->get_hint_by_dir(dirname);
  if (create || truncate) {
    vselector->add_usage(file->vselector_hint, file->fnode); // update file count
  }

  dout(20) << __func__ << " mapping " << dirname << "/" << filename
	   << " vsel_hint " << file->vselector_hint
	   << dendl;

  log_t.op_file_update(file->fnode);
  if (create)
    log_t.op_dir_link(dirname, filename, file->fnode.ino);

  *h = _create_writer(file);

  if (boost::algorithm::ends_with(filename, ".log")) {
    (*h)->writer_type = BlueFS::WRITER_WAL;
    if (logger && !overwrite) {
      logger->inc(l_bluefs_files_written_wal);
    }
  } else if (boost::algorithm::ends_with(filename, ".sst")) {
    (*h)->writer_type = BlueFS::WRITER_SST;
    if (logger) {
      logger->inc(l_bluefs_files_written_sst);
    }
  }

  dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
  return 0;
}

BlueFS::FileWriter *BlueFS::_create_writer(FileRef f)
{
  FileWriter *w = new FileWriter(f);
  for (unsigned i = 0; i < MAX_BDEV; ++i) {
    if (bdev[i]) {
      w->iocv[i] = new IOContext(cct, NULL);
    }
  }
  return w;
}

void BlueFS::_close_writer(FileWriter *h)
{
  dout(10) << __func__ << " " << h << " type " << h->writer_type << dendl;
  h->buffer.reassign_to_mempool(mempool::mempool_bluefs_file_writer);
  for (unsigned i=0; i<MAX_BDEV; ++i) {
    if (bdev[i]) {
      if (h->iocv[i]) {
	h->iocv[i]->aio_wait();
	bdev[i]->queue_reap_ioc(h->iocv[i]);
      }
    }
  }
  delete h;
}

int BlueFS::open_for_read(
  const string& dirname,
  const string& filename,
  FileReader **h,
  bool random)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename
	   << (random ? " (random)":" (sequential)") << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;

  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second.get();

  *h = new FileReader(file, random ? 4096 : cct->_conf->bluefs_max_prefetch,
		      random, false);
  dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
  return 0;
}

int BlueFS::rename(
  const string& old_dirname, const string& old_filename,
  const string& new_dirname, const string& new_filename)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << old_dirname << "/" << old_filename
	   << " -> " << new_dirname << "/" << new_filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(old_dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << old_dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef old_dir = p->second;
  map<string,FileRef>::iterator q = old_dir->file_map.find(old_filename);
  if (q == old_dir->file_map.end()) {
    dout(20) << __func__ << " dir " << old_dirname << " (" << old_dir
	     << ") file " << old_filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  FileRef file = q->second;

  p = dir_map.find(new_dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << new_dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef new_dir = p->second;
  q = new_dir->file_map.find(new_filename);
  if (q != new_dir->file_map.end()) {
    dout(20) << __func__ << " dir " << new_dirname << " (" << old_dir
	     << ") file " << new_filename
	     << " already exists, unlinking" << dendl;
    ceph_assert(q->second != file);
    log_t.op_dir_unlink(new_dirname, new_filename);
    _drop_link(q->second);
  }

  dout(10) << __func__ << " " << new_dirname << "/" << new_filename << " "
	   << " " << file->fnode << dendl;

  new_dir->file_map[new_filename] = file;
  old_dir->file_map.erase(old_filename);

  log_t.op_dir_link(new_dirname, new_filename, file->fnode.ino);
  log_t.op_dir_unlink(old_dirname, old_filename);
  return 0;
}

int BlueFS::mkdir(const string& dirname)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p != dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " exists" << dendl;
    return -EEXIST;
  }
  dir_map[dirname] = ceph::make_ref<Dir>();
  log_t.op_dir_create(dirname);
  return 0;
}

int BlueFS::rmdir(const string& dirname)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " does not exist" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  if (!dir->file_map.empty()) {
    dout(20) << __func__ << " dir " << dirname << " not empty" << dendl;
    return -ENOTEMPTY;
  }
  dir_map.erase(dirname);
  log_t.op_dir_remove(dirname);
  return 0;
}

bool BlueFS::dir_exists(const string& dirname)
{
  std::lock_guard l(lock);
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  bool exists = p != dir_map.end();
  dout(10) << __func__ << " " << dirname << " = " << (int)exists << dendl;
  return exists;
}

int BlueFS::stat(const string& dirname, const string& filename,
		 uint64_t *size, utime_t *mtime)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second.get();
  dout(10) << __func__ << " " << dirname << "/" << filename
	   << " " << file->fnode << dendl;
  if (size)
    *size = file->fnode.size;
  if (mtime)
    *mtime = file->fnode.mtime;
  return 0;
}

int BlueFS::lock_file(const string& dirname, const string& filename,
		      FileLock **plock)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  FileRef file;
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found, creating" << dendl;
    file = ceph::make_ref<File>();
    file->fnode.ino = ++ino_last;
    file->fnode.mtime = ceph_clock_now();
    file_map[ino_last] = file;
    dir->file_map[filename] = file;
    ++file->refs;
    log_t.op_file_update(file->fnode);
    log_t.op_dir_link(dirname, filename, file->fnode.ino);
  } else {
    file = q->second;
    if (file->locked) {
      dout(10) << __func__ << " already locked" << dendl;
      return -ENOLCK;
    }
  }
  file->locked = true;
  *plock = new FileLock(file);
  dout(10) << __func__ << " locked " << file->fnode
	   << " with " << *plock << dendl;
  return 0;
}

int BlueFS::unlock_file(FileLock *fl)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << fl << " on " << fl->file->fnode << dendl;
  ceph_assert(fl->file->locked);
  fl->file->locked = false;
  delete fl;
  return 0;
}

int BlueFS::readdir(const string& dirname, vector<string> *ls)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  if (dirname.empty()) {
    // list dirs
    ls->reserve(dir_map.size() + 2);
    for (auto& q : dir_map) {
      ls->push_back(q.first);
    }
  } else {
    // list files in dir
    map<string,DirRef>::iterator p = dir_map.find(dirname);
    if (p == dir_map.end()) {
      dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
      return -ENOENT;
    }
    DirRef dir = p->second;
    ls->reserve(dir->file_map.size() + 2);
    for (auto& q : dir->file_map) {
      ls->push_back(q.first);
    }
  }
  ls->push_back(".");
  ls->push_back("..");
  return 0;
}

int BlueFS::unlink(const string& dirname, const string& filename)
{
  std::lock_guard l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " file " << dirname << "/" << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  FileRef file = q->second;
  if (file->locked) {
    dout(20) << __func__ << " file " << dirname << "/" << filename
             << " is locked" << dendl;
    return -EBUSY;
  }
  dir->file_map.erase(filename);
  log_t.op_dir_unlink(dirname, filename);
  _drop_link(file);
  return 0;
}

bool BlueFS::wal_is_rotational()
{
  if (bdev[BDEV_WAL]) {
    return bdev[BDEV_WAL]->is_rotational();
  } else if (bdev[BDEV_DB]) {
    return bdev[BDEV_DB]->is_rotational();
  }
  return bdev[BDEV_SLOW]->is_rotational();
}

/*
  Algorithm.
  do_replay_recovery_read is used when bluefs log abruptly ends, but it seems that more data should be there.
  Idea is to search disk for definiton of extents that will be accompanied with bluefs log in future,
  and try if using it will produce healthy bluefs transaction.
  We encode already known bluefs log extents and search disk for these bytes.
  When we find it, we decode following bytes as extent.
  We read that whole extent and then check if merged with existing log part gives a proper bluefs transaction.
 */
int BlueFS::do_replay_recovery_read(FileReader *log_reader,
				    size_t replay_pos,
				    size_t read_offset,
				    size_t read_len,
				    bufferlist* bl) {
  dout(1) << __func__ << " replay_pos=0x" << std::hex << replay_pos <<
    " needs 0x" << read_offset << "~" << read_len << std::dec << dendl;

  bluefs_fnode_t& log_fnode = log_reader->file->fnode;
  bufferlist bin_extents;
  ::encode(log_fnode.extents, bin_extents);
  dout(2) << __func__ << " log file encoded extents length = " << bin_extents.length() << dendl;

  // cannot process if too small to effectively search
  ceph_assert(bin_extents.length() >= 32);
  bufferlist last_32;
  last_32.substr_of(bin_extents, bin_extents.length() - 32, 32);

  //read fixed part from replay_pos to end of bluefs_log extents
  bufferlist fixed;
  uint64_t e_off = 0;
  auto e = log_fnode.seek(replay_pos, &e_off);
  ceph_assert(e != log_fnode.extents.end());
  int r = bdev[e->bdev]->read(e->offset + e_off, e->length - e_off, &fixed, ioc[e->bdev],
				  cct->_conf->bluefs_buffered_io);
  ceph_assert(r == 0);
  //capture dev of last good extent
  uint8_t last_e_dev = e->bdev;
  uint64_t last_e_off = e->offset;
  ++e;
  while (e != log_fnode.extents.end()) {
    r = bdev[e->bdev]->read(e->offset, e->length, &fixed, ioc[e->bdev],
				  cct->_conf->bluefs_buffered_io);
    ceph_assert(r == 0);
    last_e_dev = e->bdev;
    ++e;
  }
  ceph_assert(replay_pos + fixed.length() == read_offset);

  dout(2) << __func__ << " valid data in log = " << fixed.length() << dendl;

  struct compare {
    bool operator()(const bluefs_extent_t& a, const bluefs_extent_t& b) const {
      if (a.bdev < b.bdev) return true;
      if (a.offset < b.offset) return true;
      return a.length < b.length;
    }
  };
  std::set<bluefs_extent_t, compare> extents_rejected;
  for (int dcnt = 0; dcnt < 3; dcnt++) {
    uint8_t dev = (last_e_dev + dcnt) % MAX_BDEV;
    if (bdev[dev] == nullptr) continue;
    dout(2) << __func__ << " processing " << get_device_name(dev) << dendl;
    interval_set<uint64_t> disk_regions;
    disk_regions.insert(0, bdev[dev]->get_size());
    for (auto f : file_map) {
      auto& e = f.second->fnode.extents;
      for (auto& p : e) {
	if (p.bdev == dev) {
	  disk_regions.erase(p.offset, p.length);
	}
      }
    }
    size_t disk_regions_count = disk_regions.num_intervals();
    dout(5) << __func__ << " " << disk_regions_count << " regions to scan on " << get_device_name(dev) << dendl;

    auto reg = disk_regions.lower_bound(last_e_off);
    //for all except first, start from beginning
    last_e_off = 0;
    if (reg == disk_regions.end()) {
      reg = disk_regions.begin();
    }
    const uint64_t chunk_size = 4 * 1024 * 1024;
    const uint64_t page_size = 4096;
    const uint64_t max_extent_size = 16;
    uint64_t overlay_size = last_32.length() + max_extent_size;
    for (size_t i = 0; i < disk_regions_count; reg++, i++) {
      if (reg == disk_regions.end()) {
	reg = disk_regions.begin();
      }
      uint64_t pos = reg.get_start();
      uint64_t len = reg.get_len();

      std::unique_ptr<char[]> raw_data_p{new char[page_size + chunk_size]};
      char* raw_data = raw_data_p.get();
      memset(raw_data, 0, page_size);

      while (len > last_32.length()) {
	uint64_t chunk_len = len > chunk_size ? chunk_size : len;
	dout(5) << __func__ << " read "
		<< get_device_name(dev) << ":0x" << std::hex << pos << "+" << chunk_len << std::dec << dendl;
	r = bdev[dev]->read_random(pos, chunk_len, raw_data + page_size, cct->_conf->bluefs_buffered_io);
	ceph_assert(r == 0);

	//search for fixed_last_32
	char* chunk_b = raw_data + page_size;
	char* chunk_e = chunk_b + chunk_len;

	char* search_b = chunk_b - overlay_size;
	char* search_e = chunk_e;

	for (char* sp = search_b; ; sp += last_32.length()) {
	  sp = (char*)memmem(sp, search_e - sp, last_32.c_str(), last_32.length());
	  if (sp == nullptr) {
	    break;
	  }

	  char* n = sp + last_32.length();
	  dout(5) << __func__ << " checking location 0x" << std::hex << pos + (n - chunk_b) << std::dec << dendl;
	  bufferlist test;
	  test.append(n, std::min<size_t>(max_extent_size, chunk_e - n));
	  bluefs_extent_t ne;
	  try {
	    bufferlist::const_iterator p = test.begin();
	    ::decode(ne, p);
	  } catch (buffer::error& e) {
	    continue;
	  }
	  if (extents_rejected.count(ne) != 0) {
	    dout(5) << __func__ << " extent " << ne << " already refected" <<dendl;
	    continue;
	  }
	  //insert as rejected already. if we succeed, it wouldn't make difference.
	  extents_rejected.insert(ne);

	  if (ne.bdev >= MAX_BDEV ||
	      bdev[ne.bdev] == nullptr ||
	      ne.length > 16 * 1024 * 1024 ||
	      (ne.length & 4095) != 0 ||
	      ne.offset + ne.length > bdev[ne.bdev]->get_size() ||
	      (ne.offset & 4095) != 0) {
	    dout(5) << __func__ << " refusing extent " << ne << dendl;
	    continue;
	  }
	  dout(5) << __func__ << " checking extent " << ne << dendl;

	  //read candidate extent - whole
	  bufferlist candidate;
	  candidate.append(fixed);
	  r = bdev[ne.bdev]->read(ne.offset, ne.length, &candidate, ioc[ne.bdev],
				cct->_conf->bluefs_buffered_io);
	  ceph_assert(r == 0);

	  //check if transaction & crc is ok
	  bluefs_transaction_t t;
	  try {
	    bufferlist::const_iterator p = candidate.begin();
	    ::decode(t, p);
	  }
	  catch (buffer::error& e) {
	    dout(5) << __func__ << " failed match" << dendl;
	    continue;
	  }

	  //success, it seems a probable candidate
	  uint64_t l = std::min<uint64_t>(ne.length, read_len);
	  //trim to required size
	  bufferlist requested_read;
	  requested_read.substr_of(candidate, fixed.length(), l);
	  bl->append(requested_read);
	  dout(5) << __func__ << " successful extension of log " << l << "/" << read_len << dendl;
	  log_fnode.append_extent(ne);
	  log_fnode.recalc_allocated();
	  log_reader->buf.pos += l;
	  return l;
	}
	//save overlay for next search
	memcpy(search_b, chunk_e - overlay_size, overlay_size);
	pos += chunk_len;
	len -= chunk_len;
      }
    }
  }
  return 0;
}

void BlueFS::debug_inject_duplicate_gift(unsigned id,
  uint64_t offset,
  uint64_t len)
{
  dout(0) << __func__ << dendl;
  if (id < alloc.size() && alloc[id]) {
    alloc[id]->init_add_free(offset, len);
  }
}

// ===============================================
// OriginalVolumeSelector

void* OriginalVolumeSelector::get_hint_for_log() const {
  return reinterpret_cast<void*>(BlueFS::BDEV_WAL);
}
void* OriginalVolumeSelector::get_hint_by_dir(const string& dirname) const {
  uint8_t res = BlueFS::BDEV_DB;
  if (dirname.length() > 5) {
    // the "db.slow" and "db.wal" directory names are hard-coded at
    // match up with bluestore.  the slow device is always the second
    // one (when a dedicated block.db device is present and used at
    // bdev 0).  the wal device is always last.
    if (boost::algorithm::ends_with(dirname, ".slow")) {
      res = BlueFS::BDEV_SLOW;
    }
    else if (boost::algorithm::ends_with(dirname, ".wal")) {
      res = BlueFS::BDEV_WAL;
    }
  }
  return reinterpret_cast<void*>(res);
}

uint8_t OriginalVolumeSelector::select_prefer_bdev(void* hint)
{
  return (uint8_t)(reinterpret_cast<uint64_t>(hint));
}

void OriginalVolumeSelector::get_paths(const std::string& base, paths& res) const
{
  res.emplace_back(base, db_total);
  res.emplace_back(base + ".slow", slow_total);
}

#undef dout_prefix
#define dout_prefix *_dout << "OriginalVolumeSelector: "

void OriginalVolumeSelector::dump(ostream& sout) {
  sout<< "wal_total:" << wal_total
    << ", db_total:" << db_total
    << ", slow_total:" << slow_total
    << std::endl;
}
