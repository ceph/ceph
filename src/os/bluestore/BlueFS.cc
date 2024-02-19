// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <chrono>
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
using std::map;
using std::ostream;
using std::set;
using std::string;
using std::to_string;
using std::vector;
using std::chrono::seconds;

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
      int r = admin_socket->register_command("bluestore bluefs device info "
                                             "name=alloc_size,type=CephInt,req=false",
                                             hook,
                                             "Shows space report for bluefs devices. "
                                             "This also includes an estimation for space "
                                             "available to bluefs at main device. "
                                             "alloc_size, if set, specifies the custom bluefs "
                                             "allocation unit size for the estimation above.");
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
	r = admin_socket->register_command("bluefs debug_inject_read_zeros", hook,
					   "Injects 8K zeros into next BlueFS read. Debug only.");
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
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    if (command == "bluestore bluefs device info") {
      int64_t alloc_size = 0;
      cmd_getval(cmdmap, "alloc_size", alloc_size);
      if ((alloc_size & (alloc_size - 1)) != 0) {
	errss << "Invalid allocation size:'" << alloc_size << std::endl;
	return -EINVAL;
      }
      if (alloc_size == 0)
	alloc_size = bluefs->cct->_conf->bluefs_shared_alloc_size;
      f->open_object_section("bluefs_device_info");
      for (unsigned dev = BDEV_WAL; dev <= BDEV_SLOW; dev++) {
	if (bluefs->bdev[dev]) {
	  f->open_object_section("dev");
	  f->dump_string("device", bluefs->get_device_name(dev));
	  ceph_assert(bluefs->alloc[dev]);
          auto total = bluefs->get_total(dev);
          auto free = bluefs->get_free(dev);
          auto used = bluefs->get_used(dev);

          f->dump_int("total", total);
          f->dump_int("free", free);
          f->dump_int("bluefs_used", used);
          if (bluefs->is_shared_alloc(dev)) {
            size_t avail = bluefs->probe_alloc_avail(dev, alloc_size);
            f->dump_int("bluefs max available", avail);
          }
          f->close_section();
        }
      }

      f->close_section();
    } else if (command == "bluefs stats") {
      std::stringstream ss;
      bluefs->dump_block_extents(ss);
      bluefs->dump_volume_selector(ss);
      out.append(ss);
    } else if (command == "bluefs files list") {
      const char* devnames[3] = {"wal","db","slow"};
      std::lock_guard l(bluefs->nodes.lock);
      f->open_array_section("files");
      for (auto &d : bluefs->nodes.dir_map) {
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
    } else if (command == "bluefs debug_inject_read_zeros") {
      bluefs->inject_read_zeros++;
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
    block_reserved(MAX_BDEV),
    alloc(MAX_BDEV),
    alloc_size(MAX_BDEV, 0)
{
  dirty.pending_release.resize(MAX_BDEV);
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
  b.add_u64_counter(l_bluefs_log_write_count, "log_write_count",
		    "Write op count to the metadata log");
  b.add_u64_counter(l_bluefs_logged_bytes, "logged_bytes",
		    "Bytes written to the metadata log",
		    "j",
		    PerfCountersBuilder::PRIO_CRITICAL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_files_written_wal, "files_written_wal",
		    "Files written to WAL");
  b.add_u64_counter(l_bluefs_files_written_sst, "files_written_sst",
		    "Files written to SSTs");
  b.add_u64_counter(l_bluefs_write_count_wal, "write_count_wal",
		    "Write op count to WAL");
  b.add_u64_counter(l_bluefs_write_count_sst, "write_count_sst",
		    "Write op count to SSTs");
  b.add_u64_counter(l_bluefs_bytes_written_wal, "bytes_written_wal",
		    "Bytes written to WAL",
		    "walb",
		    PerfCountersBuilder::PRIO_CRITICAL);
  b.add_u64_counter(l_bluefs_bytes_written_sst, "bytes_written_sst",
		    "Bytes written to SSTs",
		    "sstb",
		    PerfCountersBuilder::PRIO_CRITICAL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_bytes_written_slow, "bytes_written_slow",
		    "Bytes written to WAL/SSTs at slow device",
		    "slwb",
		    PerfCountersBuilder::PRIO_CRITICAL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_max_bytes_wal, "max_bytes_wal",
		    "Maximum bytes allocated from WAL",
		    "mxwb",
		    PerfCountersBuilder::PRIO_INTERESTING,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_max_bytes_db, "max_bytes_db",
		    "Maximum bytes allocated from DB",
		    "mxdb",
		    PerfCountersBuilder::PRIO_INTERESTING,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_max_bytes_slow, "max_bytes_slow",
		    "Maximum bytes allocated from SLOW",
		    "mxwb",
		    PerfCountersBuilder::PRIO_INTERESTING,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_slow_alloc_unit, "alloc_unit_slow",
		    "Allocation unit size (in bytes) for primary/shared device",
		    "ausb",
		    PerfCountersBuilder::PRIO_CRITICAL,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_db_alloc_unit, "alloc_unit_db",
		    "Allocation unit size (in bytes) for standalone DB device",
		    "audb",
		    PerfCountersBuilder::PRIO_CRITICAL,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_wal_alloc_unit, "alloc_unit_wal",
		    "Allocation unit size (in bytes) for standalone WAL device",
		    "auwb",
		    PerfCountersBuilder::PRIO_CRITICAL,
		    unit_t(UNIT_BYTES));
  b.add_time_avg   (l_bluefs_read_random_lat, "read_random_lat",
                    "Average bluefs read_random latency",
                    "rdrt",
                    PerfCountersBuilder::PRIO_INTERESTING);
  b.add_u64_counter(l_bluefs_read_random_count, "read_random_count",
		    "random read requests processed",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluefs_read_random_bytes, "read_random_bytes",
		    "Bytes requested in random read mode",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_random_disk_count, "read_random_disk_count",
		    "random reads requests going to disk",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluefs_read_random_disk_bytes, "read_random_disk_bytes",
		    "Bytes read from disk in random read mode",
		    "rrb",
		    PerfCountersBuilder::PRIO_INTERESTING,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_random_disk_bytes_wal, "read_random_disk_bytes_wal",
		    "random reads requests going to WAL disk",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_random_disk_bytes_db, "read_random_disk_bytes_db",
		    "random reads requests going to DB disk",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_random_disk_bytes_slow, "read_random_disk_bytes_slow",
		    "random reads requests going to main disk",
		    "rrsb",
		    PerfCountersBuilder::PRIO_INTERESTING,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_random_buffer_count, "read_random_buffer_count",
		    "random read requests processed using prefetch buffer",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluefs_read_random_buffer_bytes, "read_random_buffer_bytes",
		    "Bytes read from prefetch buffer in random read mode",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_time_avg   (l_bluefs_read_lat, "read_lat",
                    "Average bluefs read latency",
                    "rd_t",
                    PerfCountersBuilder::PRIO_INTERESTING);
  b.add_u64_counter(l_bluefs_read_count, "read_count",
		    "buffered read requests processed",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluefs_read_bytes, "read_bytes",
		    "Bytes requested in buffered read mode",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_disk_count, "read_disk_count",
		    "buffered reads requests going to disk",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluefs_read_disk_bytes, "read_disk_bytes",
		    "Bytes read in buffered mode from disk",
		    "rb",
		    PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_disk_bytes_wal, "read_disk_bytes_wal",
		    "reads requests going to WAL disk",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_disk_bytes_db, "read_disk_bytes_db",
		    "reads requests going to DB disk",
		    NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_disk_bytes_slow, "read_disk_bytes_slow",
		    "reads requests going to main disk",
		    "rsb",
		    PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_read_prefetch_count, "read_prefetch_count",
		    "prefetch read requests processed",
		     NULL,
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluefs_read_prefetch_bytes, "read_prefetch_bytes",
		    "Bytes requested in prefetch read mode",
		     NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluefs_write_count, "write_count",
		    "Write requests processed");
  b.add_u64_counter(l_bluefs_write_disk_count, "write_disk_count",
		    "Write requests sent to disk");
  b.add_u64_counter(l_bluefs_write_bytes, "write_bytes",
		    "Bytes written", NULL,
		    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_time_avg   (l_bluefs_compaction_lat, "compact_lat",
                    "Average bluefs log compaction latency",
                    "c__t",
                    PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time_avg   (l_bluefs_compaction_lock_lat, "compact_lock_lat",
                    "Average lock duration while compacting bluefs log",
                    "c_lt",
                    PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time_avg   (l_bluefs_fsync_lat, "fsync_lat",
                    "Average bluefs fsync latency",
                    "fs_t",
                    PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time_avg   (l_bluefs_flush_lat, "flush_lat",
                    "Average bluefs flush latency",
                    "fl_t",
                    PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time_avg   (l_bluefs_unlink_lat, "unlink_lat",
                    "Average bluefs unlink latency",
                    "unlt",
                    PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time_avg   (l_bluefs_truncate_lat, "truncate_lat",
                    "Average bluefs truncate latency",
                    "trnt",
                    PerfCountersBuilder::PRIO_INTERESTING);
  b.add_u64_counter(l_bluefs_alloc_shared_dev_fallbacks, "alloc_slow_fallback",
		    "Amount of allocations that required fallback to "
                    " slow/shared device",
		     "asdf",
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluefs_alloc_shared_size_fallbacks, "alloc_slow_size_fallback",
		    "Amount of allocations that required fallback to shared device's "
                    "regular unit size",
		     "assf",
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64(l_bluefs_read_zeros_candidate, "read_zeros_candidate",
	    "How many times bluefs read found page with all 0s");
  b.add_u64(l_bluefs_read_zeros_errors, "read_zeros_errors",
	    "How many times bluefs read found transient page with all 0s");
  b.add_time_avg(l_bluefs_wal_alloc_lat, "wal_alloc_lat",
                "Average bluefs wal allocate latency",
                "bwal",
                PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluefs_db_alloc_lat, "db_alloc_lat",
                "Average bluefs db allocate latency",
                "bdal",
                PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluefs_slow_alloc_lat, "slow_alloc_lat",
                "Average allocation latency for primary/shared device",
                "bsal",
                PerfCountersBuilder::PRIO_USEFUL);
  b.add_time(l_bluefs_wal_alloc_max_lat, "alloc_wal_max_lat",
             "Max allocation latency for wal device",
             "awxt",
             PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time(l_bluefs_db_alloc_max_lat, "alloc_db_max_lat",
             "Max allocation latency for db device",
             "adxt",
             PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time(l_bluefs_slow_alloc_max_lat, "alloc_slow_max_lat",
             "Max allocation latency for primary/shared device",
             "asxt",
             PerfCountersBuilder::PRIO_INTERESTING);

  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
}

void BlueFS::_shutdown_logger()
{
  cct->get_perfcounters_collection()->remove(logger);
  delete logger;
  logger = nullptr;
}

void BlueFS::_update_logger_stats()
{
  if (alloc[BDEV_WAL]) {
    logger->set(l_bluefs_wal_total_bytes, _get_total(BDEV_WAL));
    logger->set(l_bluefs_wal_used_bytes, _get_used(BDEV_WAL));
  }
  if (alloc[BDEV_DB]) {
    logger->set(l_bluefs_db_total_bytes, _get_total(BDEV_DB));
    logger->set(l_bluefs_db_used_bytes, _get_used(BDEV_DB));
  }
  if (alloc[BDEV_SLOW]) {
    logger->set(l_bluefs_slow_total_bytes, _get_total(BDEV_SLOW));
    logger->set(l_bluefs_slow_used_bytes, _get_used(BDEV_SLOW));
  }
}

int BlueFS::add_block_device(unsigned id, const string& path, bool trim,
                             bluefs_shared_alloc_context_t* _shared_alloc)
{
  uint64_t reserved;
  switch(id) {
    case BDEV_WAL:
    case BDEV_NEWWAL:
      reserved = BDEV_LABEL_BLOCK_SIZE;
      break;
    case BDEV_DB:
    case BDEV_NEWDB:
      reserved = DB_SUPER_RESERVED;
      break;
    case BDEV_SLOW:
      reserved = 0;
      break;
    default:
      ceph_assert(false);
  }
  dout(10) << __func__ << " bdev " << id << " path " << path << " "
           << " reserved " << reserved << dendl;
  ceph_assert(id < bdev.size());
  ceph_assert(bdev[id] == NULL);
  BlockDevice *b = BlockDevice::create(cct, path, NULL, NULL,
				       discard_cb[id], static_cast<void*>(this));
  block_reserved[id] = reserved;
  if (_shared_alloc) {
    b->set_no_exclusive_lock();
  }
  int r = b->open(path);
  if (r < 0) {
    delete b;
    return r;
  }
  if (trim) {
    interval_set<uint64_t> whole_device;
    whole_device.insert(0, b->get_size());
    b->try_discard(whole_device, false);
  }

  dout(1) << __func__ << " bdev " << id << " path " << path
	  << " size " << byte_u_t(b->get_size()) << dendl;
  bdev[id] = b;
  ioc[id] = new IOContext(cct, NULL);
  if (_shared_alloc) {
    ceph_assert(!shared_alloc);
    shared_alloc = _shared_alloc;
    alloc[id] = shared_alloc->a;
    shared_alloc_id = id;
  }
  return 0;
}

bool BlueFS::bdev_support_label(unsigned id)
{
  ceph_assert(id < bdev.size());
  ceph_assert(bdev[id]);
  return bdev[id]->supported_bdev_label();
}

uint64_t BlueFS::get_block_device_size(unsigned id) const
{
  if (id < bdev.size() && bdev[id])
    return bdev[id]->get_size();
  return 0;
}

void BlueFS::handle_discard(unsigned id, interval_set<uint64_t>& to_release)
{
  dout(10) << __func__ << " bdev " << id << dendl;
  ceph_assert(alloc[id]);
  alloc[id]->release(to_release);
  if (is_shared_alloc(id)) {
    shared_alloc->bluefs_used -= to_release.size();
  }
}

uint64_t BlueFS::get_used()
{
  uint64_t used = 0;
  for (unsigned id = 0; id < MAX_BDEV; ++id) {
    used += _get_used(id);
  }
  return used;
}

uint64_t BlueFS::_get_used(unsigned id) const
{
  uint64_t used = 0;
  if (!alloc[id])
     return 0;

  if (is_shared_alloc(id)) {
    used = shared_alloc->bluefs_used;
  } else {
    used = _get_total(id) - alloc[id]->get_free();
  }
  return used;
}

uint64_t BlueFS::get_used(unsigned id)
{
  ceph_assert(id < alloc.size());
  ceph_assert(alloc[id]);
  return _get_used(id);
}

uint64_t BlueFS::_get_total(unsigned id) const
{
  ceph_assert(id < bdev.size());
  ceph_assert(id < block_reserved.size());
  return get_block_device_size(id) - block_reserved[id];
}

uint64_t BlueFS::get_total(unsigned id)
{
  return _get_total(id);
}

uint64_t BlueFS::get_free(unsigned id)
{
  ceph_assert(id < alloc.size());
  return alloc[id]->get_free();
}

void BlueFS::dump_perf_counters(Formatter *f)
{
  f->open_object_section("bluefs_perf_counters");
  logger->dump_formatted(f, false, false);
  f->close_section();
}

void BlueFS::dump_block_extents(ostream& out)
{
  for (unsigned i = 0; i < MAX_BDEV; ++i) {
    if (!bdev[i]) {
      continue;
    }
    auto total = get_total(i);
    auto free = get_free(i);

    out << i << " : device size 0x" << std::hex << total
        << " : using 0x" << total - free
	<< std::dec << "(" << byte_u_t(total - free) << ")";
    out << "\n";
  }
}

void BlueFS::foreach_block_extents(
  unsigned id,
  std::function<void(uint64_t, uint32_t)> fn)
{
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " bdev " << id << dendl;
  ceph_assert(id < alloc.size());
  for (auto& p : nodes.file_map) {
    for (auto& q : p.second->fnode.extents) {
      if (q.bdev == id) {
        fn(q.offset, q.length);
      }
    }
  }
}

int BlueFS::mkfs(uuid_d osd_uuid, const bluefs_layout_t& layout)
{
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

  _init_logger();
  _init_alloc();

  super.version = 0;
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
    0,
    &log_file->fnode);
  vselector->add_usage(log_file->vselector_hint, log_file->fnode);
  ceph_assert(r == 0);
  log.writer = _create_writer(log_file);

  // initial txn
  ceph_assert(log.seq_live == 1);
  log.t.seq = 1;
  log.t.op_init();
  _flush_and_sync_log_LD();

  // write supers
  super.log_fnode = log_file->fnode;
  super.memorized_layout = layout;
  _write_super(BDEV_DB);
  _flush_bdev();

  // clean up
  super = bluefs_super_t();
  _close_writer(log.writer);
  log.writer = NULL;
  vselector.reset(nullptr);
  _stop_alloc();
  _shutdown_logger();
  if (shared_alloc) {
    ceph_assert(shared_alloc->need_init);
    shared_alloc->need_init = false;
  }

  dout(10) << __func__ << " success" << dendl;
  return 0;
}

void BlueFS::_init_alloc()
{
  dout(20) << __func__ << dendl;

  size_t wal_alloc_size = 0;
  if (bdev[BDEV_WAL]) {
    wal_alloc_size = cct->_conf->bluefs_alloc_size;
    alloc_size[BDEV_WAL] = wal_alloc_size;
  }
  logger->set(l_bluefs_wal_alloc_unit, wal_alloc_size);


  uint64_t shared_alloc_size = cct->_conf->bluefs_shared_alloc_size;
  if (shared_alloc && shared_alloc->a) {
    uint64_t unit = shared_alloc->a->get_block_size();
    shared_alloc_size = std::max(
      unit,
      shared_alloc_size);
    ceph_assert(0 == p2phase(shared_alloc_size, unit));
  }
  if (bdev[BDEV_SLOW]) {
    alloc_size[BDEV_DB] = cct->_conf->bluefs_alloc_size;
    alloc_size[BDEV_SLOW] = shared_alloc_size;
  } else {
    alloc_size[BDEV_DB] = shared_alloc_size;
    alloc_size[BDEV_SLOW] = 0;
  }
  logger->set(l_bluefs_db_alloc_unit, alloc_size[BDEV_DB]);
  logger->set(l_bluefs_slow_alloc_unit, alloc_size[BDEV_SLOW]);
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
    if (is_shared_alloc(id)) {
      dout(1) << __func__ << " shared, id " << id << std::hex
              << ", capacity 0x" << bdev[id]->get_size()
              << ", block size 0x" << alloc_size[id]
              << std::dec << dendl;
    } else {
      ceph_assert(alloc_size[id]);
      std::string name = "bluefs-";
      const char* devnames[] = { "wal","db","slow" };
      if (id <= BDEV_SLOW)
        name += devnames[id];
      else
        name += to_string(uintptr_t(this));
      dout(1) << __func__ << " new, id " << id << std::hex
              << ", allocator name " << name
              << ", allocator type " << cct->_conf->bluefs_allocator
              << ", capacity 0x" << bdev[id]->get_size()
              << ", reserved 0x" << block_reserved[id]
              << ", block size 0x" << alloc_size[id]
              << std::dec << dendl;
      alloc[id] = Allocator::create(cct, cct->_conf->bluefs_allocator,
				    bdev[id]->get_size(),
				    alloc_size[id],
				    name);
      alloc[id]->init_add_free(
        block_reserved[id],
        _get_total(id));
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

  for (size_t i = 0; i < alloc.size(); ++i) {
    if (alloc[i] && !is_shared_alloc(i)) {
      alloc[i]->shutdown();
      delete alloc[i];
      alloc[i] = nullptr;
    }
  }
}

int BlueFS::_read_and_check(uint8_t ndev, uint64_t off, uint64_t len,
			    ceph::buffer::list *pbl, IOContext *ioc, bool buffered)
{
  dout(10) << __func__ << " dev " << int(ndev)
           << ": 0x" << std::hex << off << "~" << len << std::dec
	   << (buffered ? " buffered" : "")
	   << dendl;
  int r;
  bufferlist bl;
  r = _bdev_read(ndev, off, len, &bl, ioc, buffered);
  if (r != 0) {
    return r;
  }
  uint64_t block_size = bdev[ndev]->get_block_size();
  if (inject_read_zeros) {
    if (len >= block_size * 2) {
      derr << __func__ << " injecting error, zeros at "
	   << int(ndev) << ": 0x" << std::hex << (off + len / 2)
	   << "~" << (block_size * 2) << std::dec << dendl;
      //use beginning, replace 8K in the middle with zeros, use tail
      bufferlist temp;
      bl.splice(0, len / 2 - block_size, &temp);
      temp.append(buffer::create(block_size * 2, 0));
      bl.splice(block_size * 2, len / 2 - block_size, &temp);
      bl = temp;
      inject_read_zeros--;
    }
  }
  //make a check if there is a block with all 0
  uint64_t to_check_len = len;
  uint64_t skip = p2nphase(off, block_size);
  if (skip >= to_check_len) {
    return r;
  }
  auto it = bl.begin(skip);
  to_check_len -= skip;
  bool all_zeros = false;
  while (all_zeros == false && to_check_len >= block_size) {
    // checking 0s step
    unsigned block_left = block_size;
    unsigned avail;
    const char* data;
    all_zeros = true;
    while (all_zeros && block_left > 0) {
      avail = it.get_ptr_and_advance(block_left, &data);
      block_left -= avail;
      all_zeros = mem_is_zero(data, avail);
    }
    // skipping step
    while (block_left > 0) {
      avail = it.get_ptr_and_advance(block_left, &data);
      block_left -= avail;
    }
    to_check_len -= block_size;
  }
  if (all_zeros) {
    logger->inc(l_bluefs_read_zeros_candidate, 1);
    bufferlist bl_reread;
    r = _bdev_read(ndev, off, len, &bl_reread, ioc, buffered);
    if (r != 0) {
      return r;
    }
    // check if both read gave the same
    if (!bl.contents_equal(bl_reread)) {
      // report problems to log, but continue, maybe it will be good now...
      derr << __func__ << " initial read of " << int(ndev)
	   << ": 0x" << std::hex << off << "~" << len
	   << std::dec << ": different then re-read " << dendl;
      logger->inc(l_bluefs_read_zeros_errors, 1);
    }
    // use second read will be better if is different
    pbl->append(bl_reread);
  } else {
    pbl->append(bl);
  }
  return r;
}

int BlueFS::_read_random_and_check(
  uint8_t ndev, uint64_t off, uint64_t len, char *buf, bool buffered)
{
  dout(10) << __func__ << " dev " << int(ndev)
           << ": 0x" << std::hex << off << "~" << len << std::dec
	   << (buffered ? " buffered" : "")
	   << dendl;
  int r;
  r = _bdev_read_random(ndev, off, len, buf, buffered);
  if (r != 0) {
    return r;
  }
  uint64_t block_size = bdev[ndev]->get_block_size();
  if (inject_read_zeros) {
    if (len >= block_size * 2) {
      derr << __func__ << " injecting error, zeros at "
	   << int(ndev) << ": 0x" << std::hex << (off + len / 2)
	   << "~" << (block_size * 2) << std::dec << dendl;
      //zero middle 8K
      memset(buf + len / 2 - block_size, 0, block_size * 2);
      inject_read_zeros--;
    }
  }
  //make a check if there is a block with all 0
  uint64_t to_check_len = len;
  const char* data = buf;
  uint64_t skip = p2nphase(off, block_size);
  if (skip >= to_check_len) {
    return r;
  }
  to_check_len -= skip;
  data += skip;

  bool all_zeros = false;
  while (all_zeros == false && to_check_len >= block_size) {
    if (mem_is_zero(data, block_size)) {
      // at least one block is all zeros
      all_zeros = true;
      break;
    }
    data += block_size;
    to_check_len -= block_size;
  }
  if (all_zeros) {
    logger->inc(l_bluefs_read_zeros_candidate, 1);
    std::unique_ptr<char[]> data_reread(new char[len]);
    r = _bdev_read_random(ndev, off, len, &data_reread[0], buffered);
    if (r != 0) {
      return r;
    }
    // check if both read gave the same
    if (memcmp(buf, &data_reread[0], len) != 0) {
      derr << __func__ << " initial read of " << int(ndev)
	   << ": 0x" << std::hex << off << "~" << len
	   << std::dec << ": different then re-read " << dendl;
      logger->inc(l_bluefs_read_zeros_errors, 1);
      // second read is probably better
      memcpy(buf, &data_reread[0], len);
    }
  }
  return r;
}

int BlueFS::_bdev_read(uint8_t ndev, uint64_t off, uint64_t len,
  ceph::buffer::list* pbl, IOContext* ioc, bool buffered)
{
  int cnt = 0;
  switch (ndev) {
    case BDEV_WAL: cnt = l_bluefs_read_disk_bytes_wal; break;
    case BDEV_DB: cnt = l_bluefs_read_disk_bytes_db; break;
    case BDEV_SLOW: cnt = l_bluefs_read_disk_bytes_slow; break;

  }
  if (cnt) {
    logger->inc(cnt, len);
  }
  return bdev[ndev]->read(off, len, pbl, ioc, buffered);
}

int BlueFS::_bdev_read_random(uint8_t ndev, uint64_t off, uint64_t len,
  char* buf, bool buffered)
{
  int cnt = 0;
  switch (ndev) {
    case BDEV_WAL: cnt = l_bluefs_read_random_disk_bytes_wal; break;
    case BDEV_DB: cnt = l_bluefs_read_random_disk_bytes_db; break;
    case BDEV_SLOW: cnt = l_bluefs_read_random_disk_bytes_slow; break;
  }
  if (cnt) {
    logger->inc(cnt, len);
  }
  return bdev[ndev]->read_random(off, len, buf, buffered);
}

int BlueFS::mount()
{
  dout(1) << __func__ << dendl;

  _init_logger();
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

  _init_alloc();

  r = _replay(false, false);
  if (r < 0) {
    derr << __func__ << " failed to replay log: " << cpp_strerror(r) << dendl;
    _stop_alloc();
    goto out;
  }

  // init freelist
  for (auto& p : nodes.file_map) {
    dout(30) << __func__ << " noting alloc for " << p.second->fnode << dendl;
    for (auto& q : p.second->fnode.extents) {
      bool is_shared = is_shared_alloc(q.bdev);
      ceph_assert(!is_shared || (is_shared && shared_alloc));
      if (is_shared && shared_alloc->need_init && shared_alloc->a) {
        shared_alloc->bluefs_used += q.length;
        alloc[q.bdev]->init_rm_free(q.offset, q.length);
      } else if (!is_shared) {
        alloc[q.bdev]->init_rm_free(q.offset, q.length);
      }
    }
  }
  if (shared_alloc) {
    shared_alloc->need_init = false;
    dout(1) << __func__ << " shared_bdev_used = "
            << shared_alloc->bluefs_used << dendl;
  } else {
    dout(1) << __func__ << " shared bdev not used"
            << dendl;
  }

  // set up the log for future writes
  log.writer = _create_writer(_get_file(1));
  ceph_assert(log.writer->file->fnode.ino == 1);
  log.writer->pos = log.writer->file->fnode.size;
  log.writer->file->fnode.reset_delta();
  dout(10) << __func__ << " log write pos set to 0x"
           << std::hex << log.writer->pos << std::dec
           << dendl;
  // update log size
  logger->set(l_bluefs_log_bytes, log.writer->file->fnode.size);
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
  if (cct->_conf->bluefs_check_volume_selector_on_umount) {
    _check_vselector_LNF();
  }
  _close_writer(log.writer);
  log.writer = NULL;
  log.t.clear();

  vselector.reset(nullptr);
  _stop_alloc();
  nodes.file_map.clear();
  nodes.dir_map.clear();
  super = bluefs_super_t();
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
    _rewrite_log_and_layout_sync_LNF_LD(false,
      BDEV_NEWDB,
      new_log_dev_cur,
      new_log_dev_next,
      RENAME_DB2SLOW,
      layout);
  } else if(id == BDEV_NEWWAL) {
    _rewrite_log_and_layout_sync_LNF_LD(false,
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
  dout(1) << __func__ << dendl;
  // hrm, i think we check everything on mount...
  return 0;
}

int BlueFS::_write_super(int dev)
{
  ++super.version;
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
  r = _bdev_read(BDEV_DB, get_super_offset(), get_super_length(),
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

int BlueFS::_check_allocations(const bluefs_fnode_t& fnode,
  boost::dynamic_bitset<uint64_t>* used_blocks,
  bool is_alloc, //true when allocating, false when deallocating
  const char* op_name)
{
  auto& fnode_extents = fnode.extents;
  for (auto e : fnode_extents) {
    auto id = e.bdev;
    bool fail = false;
    ceph_assert(id < MAX_BDEV);
    ceph_assert(bdev[id]);
    // let's use minimal allocation unit we can have
    auto alloc_unit = bdev[id]->get_block_size();

    if (int r = _verify_alloc_granularity(id, e.offset, e.length,
                                          alloc_unit,
					  op_name); r < 0) {
      return r;
    }

    apply_for_bitset_range(e.offset, e.length, alloc_unit, used_blocks[id],
      [&](uint64_t pos, boost::dynamic_bitset<uint64_t> &bs) {
	if (is_alloc == bs.test(pos)) {
	  fail = true;
	} else {
	  bs.flip(pos);
	}
      }
    );
    if (fail) {
      derr << __func__ << " " << op_name << " invalid extent " << int(e.bdev)
        << ": 0x" << std::hex << e.offset << "~" << e.length << std::dec
	<< (is_alloc == true ?
	    ": duplicate reference, ino " : ": double free, ino ")
	<< fnode.ino << dendl;
      return -EFAULT;
    }
  }
  return 0;
}

int BlueFS::_verify_alloc_granularity(
  __u8 id, uint64_t offset, uint64_t length, uint64_t alloc_unit, const char *op)
{
  if ((offset & (alloc_unit - 1)) ||
      (length & (alloc_unit - 1))) {
    derr << __func__ << " " << op << " of " << (int)id
	 << ":0x" << std::hex << offset << "~" << length << std::dec
	 << " does not align to alloc_size 0x"
	 << std::hex << alloc_unit << std::dec << dendl;
    return -EFAULT;
  }
  return 0;
}

int BlueFS::_replay(bool noop, bool to_stdout)
{
  dout(10) << __func__ << (noop ? " NO-OP" : "") << dendl;
  ino_last = 1;  // by the log
  uint64_t log_seq = 0;

  FileRef log_file;
  log_file = _get_file(1);

  log_file->fnode = super.log_fnode;
  if (!noop) {
    log_file->vselector_hint =
      vselector->get_hint_for_log();
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

  if (!noop) {
    if (cct->_conf->bluefs_log_replay_check_allocations) {
      for (size_t i = 0; i < MAX_BDEV; ++i) {
	if (bdev[i] != nullptr) {
          // let's use minimal allocation unit we can have
          auto au = bdev[i]->get_block_size();
          //hmm... on 32TB/4K drive this would take 1GB RAM!!!
	  used_blocks[i].resize(round_up_to(bdev[i]->get_size(), au) / au);
	}
      }
      // check initial log layout
      int r = _check_allocations(log_file->fnode,
				 used_blocks, true, "Log from super");
      if (r < 0) {
	return r;
      }
    }
  }
  
  while (true) {
    ceph_assert((log_reader->buf.pos & ~super.block_mask()) == 0);
    uint64_t pos = log_reader->buf.pos;
    uint64_t read_pos = pos;
    bufferlist bl;
    {
      int r = _read(log_reader, read_pos, super.block_size,
		    &bl, NULL);
      if (r != (int)super.block_size && cct->_conf->bluefs_replay_recovery) {
	r += _do_replay_recovery_read(log_reader, pos, read_pos + r, super.block_size - r, &bl);
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
	  r += _do_replay_recovery_read(log_reader, pos, read_pos + r, more - r, &t);
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
    bluefs_transaction_t t;
    try {
      auto p = bl.cbegin();
      decode(t, p);
      seen_recs = true;
    }
    catch (ceph::buffer::error& e) {
      // Multi-block transactions might be incomplete due to unexpected
      // power off. Hence let's treat that as a regular stop condition.
      if (seen_recs && more) {
        dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
                 << ": stop: failed to decode: " << e.what()
                 << dendl;
      } else {
        derr << __func__ << " 0x" << std::hex << pos << std::dec
             << ": stop: failed to decode: " << e.what()
             << dendl;
        delete log_reader;
        return -EIO;
      }
      break;
    }
    ceph_assert(seq == t.seq);
    dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
             << ": " << t << dendl;
    if (unlikely(to_stdout)) {
      std::cout << " 0x" << std::hex << pos << std::dec
                << ": " << t << std::endl;
    }

    auto p = t.op_bl.cbegin();
    auto pos0 = pos;
    while (!p.end()) {
      pos = pos0 + p.get_off();
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

	  ceph_assert(next_seq > log_seq);
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

	  ceph_assert(next_seq > log_seq);
	  log_seq = next_seq - 1; // we will increment it below
	}
	break;

      case bluefs_transaction_t::OP_ALLOC_ADD:
	// LEGACY, do nothing but read params
        {
          __u8 id;
          uint64_t offset, length;
          decode(id, p);
          decode(offset, p);
          decode(length, p);
        }
	break;

      case bluefs_transaction_t::OP_ALLOC_RM:
	// LEGACY, do nothing but read params
        {
          __u8 id;
          uint64_t offset, length;
          decode(id, p);
          decode(offset, p);
          decode(length, p);
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
	    map<string,DirRef>::iterator q = nodes.dir_map.find(dirname);
	    ceph_assert(q != nodes.dir_map.end());
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
	    map<string,DirRef>::iterator q = nodes.dir_map.find(dirname);
	    ceph_assert(q != nodes.dir_map.end());
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
	    map<string,DirRef>::iterator q = nodes.dir_map.find(dirname);
	    ceph_assert(q == nodes.dir_map.end());
	    nodes.dir_map[dirname] = ceph::make_ref<Dir>();
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
	    map<string,DirRef>::iterator q = nodes.dir_map.find(dirname);
	    ceph_assert(q != nodes.dir_map.end());
	    ceph_assert(q->second->file_map.empty());
	    nodes.dir_map.erase(q);
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
              int r = _check_allocations(f->fnode,
		used_blocks, false, "OP_FILE_UPDATE");
              if (r < 0) {
                return r;
              }
            }
            if (fnode.ino != 1) {
              vselector->sub_usage(f->vselector_hint, f->fnode);
	      vselector->add_usage(f->vselector_hint, fnode);
	    }
            f->fnode = fnode;

	    if (fnode.ino > ino_last) {
	      ino_last = fnode.ino;
	    }
            if (cct->_conf->bluefs_log_replay_check_allocations) {
              int r = _check_allocations(f->fnode,
		used_blocks, true, "OP_FILE_UPDATE");
              if (r < 0) {
                return r;
              }
            }
	  } else if (noop && fnode.ino == 1) {
	    FileRef f = _get_file(fnode.ino);
	    f->fnode = fnode;
	  }
        }
	break;
      case bluefs_transaction_t::OP_FILE_UPDATE_INC:
	{
	  bluefs_fnode_delta_t delta;
	  decode(delta, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
	    << ":  op_file_update_inc " << " " << delta << " " << dendl;
	  if (unlikely(to_stdout)) {
	    std::cout << " 0x" << std::hex << pos << std::dec
	      << ":  op_file_update_inc " << " " << delta << std::endl;
	  }
	  if (!noop) {
	    FileRef f = _get_file(delta.ino);
	    bluefs_fnode_t& fnode = f->fnode;
	    if (delta.offset != fnode.allocated) {
	      derr << __func__ << " invalid op_file_update_inc, new extents miss end of file"
		   << " fnode=" << fnode
		   << " delta=" << delta
		   << dendl;
	      ceph_assert(delta.offset == fnode.allocated);
	    }
	    if (cct->_conf->bluefs_log_replay_check_allocations) {
              int r = _check_allocations(fnode,
		used_blocks, false, "OP_FILE_UPDATE_INC");
              if (r < 0) {
                return r;
              }
            }

	    fnode.ino = delta.ino;
	    fnode.mtime = delta.mtime;
	    if (fnode.ino != 1) {
	      vselector->sub_usage(f->vselector_hint, fnode);
	    }
	    fnode.size = delta.size;
	    fnode.claim_extents(delta.extents);
	    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
		     << ":  op_file_update_inc produced " << " " << fnode << " " << dendl;

	    if (fnode.ino != 1) {
	      vselector->add_usage(f->vselector_hint, fnode);
	    }

	    if (fnode.ino > ino_last) {
	      ino_last = fnode.ino;
	    }
	    if (cct->_conf->bluefs_log_replay_check_allocations) {
              int r = _check_allocations(f->fnode,
		used_blocks, true, "OP_FILE_UPDATE_INC");
              if (r < 0) {
                return r;
              }
	    }
	  } else if (noop && delta.ino == 1) {
	    // we need to track bluefs log, even in noop mode
	    FileRef f = _get_file(1);
	    bluefs_fnode_t& fnode = f->fnode;
	    fnode.ino = delta.ino;
	    fnode.mtime = delta.mtime;
	    fnode.size = delta.size;
	    fnode.claim_extents(delta.extents);
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
            auto p = nodes.file_map.find(ino);
            ceph_assert(p != nodes.file_map.end());
            vselector->sub_usage(p->second->vselector_hint, p->second->fnode);
            if (cct->_conf->bluefs_log_replay_check_allocations) {
	      int r = _check_allocations(p->second->fnode,
		used_blocks, false, "OP_FILE_REMOVE");
              if (r < 0) {
		return r;
              }
            }
            nodes.file_map.erase(p);
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
    log.seq_live = log_seq + 1;
    dirty.seq_live = log_seq + 1;
    log.t.seq = log.seq_live;
    dirty.seq_stable = log_seq;
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
    for (auto& p : nodes.file_map) {
      if (p.second->refs == 0 &&
	  p.second->fnode.ino > 1) {
	derr << __func__ << " file with link count 0: " << p.second->fnode
	     << dendl;
	return -EIO;
      }
    }
  }
  // reflect file count in logger
  logger->set(l_bluefs_num_files, nodes.file_map.size());

  dout(10) << __func__ << " done" << dendl;
  return 0;
}

int BlueFS::log_dump()
{
  // only dump log file's content
  ceph_assert(log.writer == nullptr && "cannot log_dump on mounted BlueFS");
  _init_logger();
  int r = _open_super();
  if (r < 0) {
    derr << __func__ << " failed to open super: " << cpp_strerror(r) << dendl;
    return r;
  }
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

  for (auto& [ino, file_ref] : nodes.file_map) {
    //do not copy log
    if (ino == 1) {
      continue;
    }
    dout(10) << __func__ << " " << ino << " " << file_ref->fnode << dendl;

    vselector->sub_usage(file_ref->vselector_hint, file_ref->fnode);

    bool rewrite = std::any_of(
      file_ref->fnode.extents.begin(),
      file_ref->fnode.extents.end(),
      [=](auto& ext) {
	return ext.bdev != dev_target && devs_source.count(ext.bdev);
      });
    if (rewrite) {
      dout(10) << __func__ << "  migrating" << dendl;
      bluefs_fnode_t old_fnode;
      old_fnode.swap_extents(file_ref->fnode);
      auto& old_fnode_extents = old_fnode.extents;
      // read entire file
      bufferlist bl;
      for (const auto &old_ext : old_fnode_extents) {
	buf.resize(old_ext.length);
	int r = _bdev_read_random(old_ext.bdev,
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
      auto l = _allocate(dev_target, bl.length(), 0,
        &file_ref->fnode, nullptr, 0, false);
      if (l < 0) {
	derr << __func__ << " unable to allocate len 0x" << std::hex
	     << bl.length() << std::dec << " from " << (int)dev_target
	     << ": " << cpp_strerror(l) << dendl;
	return -ENOSPC;
      }

      uint64_t off = 0;
      for (auto& i : file_ref->fnode.extents) {
	bufferlist cur;
	uint64_t cur_len = std::min<uint64_t>(i.length, bl.length() - off);
	ceph_assert(cur_len > 0);
	cur.substr_of(bl, off, cur_len);
	int r = bdev[dev_target]->write(i.offset, cur, buffered);
	ceph_assert(r == 0);
	off += cur_len;
      }

      // release old extents
      for (const auto &old_ext : old_fnode_extents) {
	PExtentVector to_release;
	to_release.emplace_back(old_ext.offset, old_ext.length);
	alloc[old_ext.bdev]->release(to_release);
        if (is_shared_alloc(old_ext.bdev)) {
          shared_alloc->bluefs_used -= to_release.size();
        }
      }

      // update fnode
      for (auto& i : file_ref->fnode.extents) {
	i.bdev = dev_target_new;
      }
    } else {
      for (auto& ext : file_ref->fnode.extents) {
	if (dev_target != dev_target_new && ext.bdev == dev_target) {
	  dout(20) << __func__ << "  " << " ... adjusting extent 0x"
		   << std::hex << ext.offset << std::dec
		   << " bdev " << dev_target << " -> " << dev_target_new
		   << dendl;
	  ext.bdev = dev_target_new;
	}
      }
    }
    vselector->add_usage(file_ref->vselector_hint, file_ref->fnode);
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

  _rewrite_log_and_layout_sync_LNF_LD(
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
  assert(dev_target == (int)BDEV_NEWDB || dev_target == (int)BDEV_NEWWAL);

  int flags = 0;

  flags |= devs_source.count(BDEV_DB) ?
    (!bdev[BDEV_SLOW] ? RENAME_DB2SLOW: REMOVE_DB) :
    0;
  flags |= devs_source.count(BDEV_WAL) ? REMOVE_WAL : 0;
  int dev_target_new = dev_target; //FIXME: remove, makes no sense

  for (auto& [ino, file_ref] : nodes.file_map) {
    //do not copy log
    if (ino == 1) {
      continue;
    }
    dout(10) << __func__ << " " << ino << " " << file_ref->fnode << dendl;

    vselector->sub_usage(file_ref->vselector_hint, file_ref->fnode);

    bool rewrite = std::any_of(
      file_ref->fnode.extents.begin(),
      file_ref->fnode.extents.end(),
      [=](auto& ext) {
	return ext.bdev != dev_target && devs_source.count(ext.bdev);
      });
    if (rewrite) {
      dout(10) << __func__ << "  migrating" << dendl;
      bluefs_fnode_t old_fnode;
      old_fnode.swap_extents(file_ref->fnode);
      auto& old_fnode_extents = old_fnode.extents;
      // read entire file
      bufferlist bl;
      for (const auto &old_ext : old_fnode_extents) {
	buf.resize(old_ext.length);
	int r = _bdev_read_random(old_ext.bdev,
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
      auto l = _allocate(dev_target, bl.length(), 0,
        &file_ref->fnode, nullptr, 0, false);
      if (l < 0) {
	derr << __func__ << " unable to allocate len 0x" << std::hex
	     << bl.length() << std::dec << " from " << (int)dev_target
	     << ": " << cpp_strerror(l) << dendl;
	return -ENOSPC;
      }

      uint64_t off = 0;
      for (auto& i : file_ref->fnode.extents) {
	bufferlist cur;
	uint64_t cur_len = std::min<uint64_t>(i.length, bl.length() - off);
	ceph_assert(cur_len > 0);
	cur.substr_of(bl, off, cur_len);
	int r = bdev[dev_target]->write(i.offset, cur, buffered);
	ceph_assert(r == 0);
	off += cur_len;
      }

      // release old extents
      for (const auto &old_ext : old_fnode_extents) {
	PExtentVector to_release;
	to_release.emplace_back(old_ext.offset, old_ext.length);
	alloc[old_ext.bdev]->release(to_release);
        if (is_shared_alloc(old_ext.bdev)) {
          shared_alloc->bluefs_used -= to_release.size();
        }
      }

      // update fnode
      for (auto& i : file_ref->fnode.extents) {
	i.bdev = dev_target_new;
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

  _rewrite_log_and_layout_sync_LNF_LD(
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
  auto p = nodes.file_map.find(ino);
  if (p == nodes.file_map.end()) {
    FileRef f = ceph::make_ref<File>();
    nodes.file_map[ino] = f;
    // track files count in logger
    logger->set(l_bluefs_num_files, nodes.file_map.size());
    dout(30) << __func__ << " ino " << ino << " = " << f
	     << " (new)" << dendl;
    return f;
  } else {
    dout(30) << __func__ << " ino " << ino << " = " << p->second << dendl;
    return p->second;
  }
}


/**
To modify fnode both FileWriter::lock and File::lock must be obtained.
The special case is when we modify bluefs log (ino 1) or
we are compacting log (ino 0).

In any case it is enough to hold File::lock to be sure fnode will not be modified.
*/
struct lock_fnode_print {
  BlueFS::FileRef file;
  lock_fnode_print(BlueFS::FileRef file) : file(file) {};
};
std::ostream& operator<<(std::ostream& out, const lock_fnode_print& to_lock) {
  std::lock_guard l(to_lock.file->lock);
  out << to_lock.file->fnode;
  return out;
}

void BlueFS::_drop_link_D(FileRef file)
{
  dout(20) << __func__ << " had refs " << file->refs
	   << " on " << lock_fnode_print(file) << dendl;
  ceph_assert(file->refs > 0);
  ceph_assert(ceph_mutex_is_locked(log.lock));
  ceph_assert(ceph_mutex_is_locked(nodes.lock));

  --file->refs;
  if (file->refs == 0) {
    dout(20) << __func__ << " destroying " << file->fnode << dendl;
    ceph_assert(file->num_reading.load() == 0);
    vselector->sub_usage(file->vselector_hint, file->fnode);
    log.t.op_file_remove(file->fnode.ino);
    nodes.file_map.erase(file->fnode.ino);
    logger->set(l_bluefs_num_files, nodes.file_map.size());
    file->deleted = true;

    std::lock_guard dl(dirty.lock);
    for (auto& r : file->fnode.extents) {
      dirty.pending_release[r.bdev].insert(r.offset, r.length);
    }
    if (file->dirty_seq > dirty.seq_stable) {
      // retract request to serialize changes
      ceph_assert(dirty.files.count(file->dirty_seq));
      auto it = dirty.files[file->dirty_seq].iterator_to(*file);
      dirty.files[file->dirty_seq].erase(it);
      file->dirty_seq = dirty.seq_stable;
    }
  }
}

int64_t BlueFS::_read_random(
  FileReader *h,         ///< [in] read from here
  uint64_t off,          ///< [in] offset
  uint64_t len,          ///< [in] this many bytes
  char *out)             ///< [out] copy it here
{
  auto t0 = mono_clock::now();
  auto* buf = &h->buf;

  int64_t ret = 0;
  dout(10) << __func__ << " h " << h
           << " 0x" << std::hex << off << "~" << len << std::dec
	   << " from " << lock_fnode_print(h->file) << dendl;

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
      int r;
      if (!cct->_conf->bluefs_check_for_zeros) {
	r = _bdev_read_random(p->bdev, p->offset + x_off, l, out,
			      cct->_conf->bluefs_buffered_io);
      } else {
	r = _read_random_and_check(p->bdev, p->offset + x_off, l, out,
			cct->_conf->bluefs_buffered_io);
      }
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

      auto p = buf->bl.begin();
      p.seek(off - buf->bl_off);
      p.copy(r, out);
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
  dout(20) << __func__ << std::hex
           << " got 0x" << ret
           << std::dec  << dendl;
  --h->file->num_reading;
  logger->tinc(l_bluefs_read_random_lat, mono_clock::now() - t0);
  return ret;
}

int64_t BlueFS::_read(
  FileReader *h,         ///< [in] read from here
  uint64_t off,          ///< [in] offset
  size_t len,            ///< [in] this many bytes
  bufferlist *outbl,     ///< [out] optional: reference the result here
  char *out)             ///< [out] optional: or copy it here
{
  auto t0 = mono_clock::now();
  FileReaderBuffer *buf = &(h->buf);

  bool prefetch = !outbl && !out;
  dout(10) << __func__ << " h " << h
           << " 0x" << std::hex << off << "~" << len << std::dec
	   << " from " << lock_fnode_print(h->file)
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
	int r;
	// when reading BlueFS log (only happens on startup) use non-buffered io
	// it makes it in sync with logic in _flush_range()
	bool use_buffered_io = h->file->fnode.ino == 1 ? false : cct->_conf->bluefs_buffered_io;
	if (!cct->_conf->bluefs_check_for_zeros) {
	  r = _bdev_read(p->bdev, p->offset + x_off, l, &buf->bl, ioc[p->bdev],
			 use_buffered_io);
	} else {
	  r = _read_and_check(
	    p->bdev, p->offset + x_off, l, &buf->bl, ioc[p->bdev],
	    use_buffered_io);
	}
	logger->inc(l_bluefs_read_disk_count, 1);
	logger->inc(l_bluefs_read_disk_bytes, l);

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
      auto p = buf->bl.begin();
      p.seek(off - buf->bl_off);
      p.copy(r, out);
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

  dout(20) << __func__ << std::hex
           << " got 0x" << ret
           << std::dec  << dendl;
  ceph_assert(!outbl || (int)outbl->length() == ret);
  --h->file->num_reading;
  logger->tinc(l_bluefs_read_lat, mono_clock::now() - t0);
  return ret;
}

void BlueFS::invalidate_cache(FileRef f, uint64_t offset, uint64_t length)
{
  std::lock_guard l(f->lock);
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


uint64_t BlueFS::_estimate_transaction_size(bluefs_transaction_t* t)
{
  uint64_t max_alloc_size = std::max(alloc_size[BDEV_WAL],
				     std::max(alloc_size[BDEV_DB],
					      alloc_size[BDEV_SLOW]));

  // conservative estimate for final encoded size
  return round_up_to(t->op_bl.length() + super.block_size * 2, max_alloc_size);
}

uint64_t BlueFS::_make_initial_transaction(uint64_t start_seq,
                                           bluefs_fnode_t& fnode,
                                           uint64_t expected_final_size,
                                           bufferlist* out)
{
  bluefs_transaction_t t0;
  t0.seq = start_seq;
  t0.uuid = super.uuid;
  t0.op_init();
  t0.op_file_update_inc(fnode);
  t0.op_jump(start_seq, expected_final_size); // this is a fixed size op,
                                              // hence it's valid with fake
                                              // params for overall txc size
                                              // estimation
  if (!out) {
    return _estimate_transaction_size(&t0);
  }

  ceph_assert(expected_final_size > 0);
  out->reserve(expected_final_size);
  encode(t0, *out);
  // make sure we're not wrong aboth the size
  ceph_assert(out->length() <= expected_final_size);
  _pad_bl(*out, expected_final_size);
  return expected_final_size;
}

uint64_t BlueFS::_estimate_log_size_N()
{
  std::lock_guard nl(nodes.lock);
  int avg_dir_size = 40;  // fixme
  int avg_file_size = 12;
  uint64_t size = 4096 * 2;
  size += nodes.file_map.size() * (1 + sizeof(bluefs_fnode_t));
  size += nodes.dir_map.size() + (1 + avg_dir_size);
  size += nodes.file_map.size() * (1 + avg_dir_size + avg_file_size);
  return round_up_to(size, super.block_size);
}

void BlueFS::compact_log()/*_LNF_LD_NF_D*/
{
  if (!cct->_conf->bluefs_replay_recovery_disable_compact) {
    if (cct->_conf->bluefs_compact_log_sync) {
      _compact_log_sync_LNF_LD();
    } else {
      _compact_log_async_LD_LNF_D();
    }
  }
}

bool BlueFS::_should_start_compact_log_L_N()
{
  if (log_is_compacting.load() == true) {
    // compaction is already running
    return false;
  }
  uint64_t current;
  {
    std::lock_guard ll(log.lock);
    current = log.writer->file->fnode.size;
  }
  uint64_t expected = _estimate_log_size_N();
  float ratio = (float)current / (float)expected;
  dout(10) << __func__ << " current 0x" << std::hex << current
	   << " expected " << expected << std::dec
	   << " ratio " << ratio
	   << dendl;
  if (current < cct->_conf->bluefs_log_compact_min_size ||
      ratio < cct->_conf->bluefs_log_compact_min_ratio) {
    return false;
  }
  return true;
}

void BlueFS::_compact_log_dump_metadata_NF(uint64_t start_seq,
                                        bluefs_transaction_t *t,
					int bdev_update_flags,
                                        uint64_t capture_before_seq)
{
  dout(20) << __func__ << dendl;
  t->seq = start_seq;
  t->uuid = super.uuid;

  std::lock_guard nl(nodes.lock);

  for (auto& [ino, file_ref] : nodes.file_map) {
    if (ino == 1)
      continue;
    ceph_assert(ino > 1);
    std::lock_guard fl(file_ref->lock);
    if (bdev_update_flags) {
      for(auto& e : file_ref->fnode.extents) {
        auto bdev = e.bdev;
        auto bdev_new = bdev;
        ceph_assert(!((bdev_update_flags & REMOVE_WAL) && bdev == BDEV_WAL));
        if ((bdev_update_flags & RENAME_SLOW2DB) && bdev == BDEV_SLOW) {
	  bdev_new = BDEV_DB;
        }
        if ((bdev_update_flags & RENAME_DB2SLOW) && bdev == BDEV_DB) {
	  bdev_new = BDEV_SLOW;
        }
        if (bdev == BDEV_NEWDB) {
	  // REMOVE_DB xor RENAME_DB
	  ceph_assert(!(bdev_update_flags & REMOVE_DB) != !(bdev_update_flags & RENAME_DB2SLOW));
	  ceph_assert(!(bdev_update_flags & RENAME_SLOW2DB));
	  bdev_new = BDEV_DB;
        }
        if (bdev == BDEV_NEWWAL) {
	  ceph_assert(bdev_update_flags & REMOVE_WAL);
	  bdev_new = BDEV_WAL;
        }
        e.bdev = bdev_new;
      }
    }
    if (capture_before_seq == 0 || file_ref->dirty_seq < capture_before_seq) {
      dout(20) << __func__ << " op_file_update " << file_ref->fnode << dendl;
    } else {
      dout(20) << __func__ << " op_file_update just modified, dirty_seq="
               << file_ref->dirty_seq << " " << file_ref->fnode << dendl;
    }
    t->op_file_update(file_ref->fnode);
  }
  for (auto& [path, dir_ref] : nodes.dir_map) {
    dout(20) << __func__ << " op_dir_create " << path << dendl;
    t->op_dir_create(path);
    for (auto& [fname, file_ref] : dir_ref->file_map) {
      dout(20) << __func__ << " op_dir_link " << path << "/" << fname
	       << " to " << file_ref->fnode.ino << dendl;
      t->op_dir_link(path, fname, file_ref->fnode.ino);
    }
  }
}

void BlueFS::_compact_log_sync_LNF_LD()
{
  dout(10) << __func__ << dendl;
  uint8_t prefer_bdev;
  {
    std::lock_guard ll(log.lock);
    prefer_bdev =
      vselector->select_prefer_bdev(log.writer->file->vselector_hint);
  }
  _rewrite_log_and_layout_sync_LNF_LD(true,
    BDEV_DB,
    prefer_bdev,
    prefer_bdev,
    0,
    super.memorized_layout);
  logger->inc(l_bluefs_log_compactions);
}

/*
 * SYNC LOG COMPACTION
 *
 * 0. Lock the log completely through the whole procedure
 *
 * 1. Build new log. It will include log's starter and compacted metadata
 *    body. Jump op appended to the starter will link the pieces together.
 *
 * 2. Write out new log's content
 *
 * 3. Write out new superblock. This includes relevant device layout update.
 *
 * 4. Finalization. Old space release.
 */

void BlueFS::_rewrite_log_and_layout_sync_LNF_LD(bool permit_dev_fallback,
					 int super_dev,
					 int log_dev,
					 int log_dev_new,
					 int flags,
					 std::optional<bluefs_layout_t> layout)
{
  // we substitute log_dev with log_dev_new for new allocations below
  // and permitting fallback allocations prevents such a substitution
  ceph_assert((permit_dev_fallback && log_dev == log_dev_new) ||
              !permit_dev_fallback);

  dout(10) << __func__ << " super_dev:" << super_dev
                       << " log_dev:" << log_dev
                       << " log_dev_new:" << log_dev_new
		       << " flags:" << flags
		       << " seq:" << log.seq_live
		       << dendl;
  utime_t mtime = ceph_clock_now();
  uint64_t starter_seq = 1;

  // Part 0.
  // Lock the log totally till the end of the procedure
  std::lock_guard ll(log.lock);
  auto t0 = mono_clock::now();

  File *log_file = log.writer->file.get();
  // log.t.seq is always set to current live seq
  ceph_assert(log.t.seq == log.seq_live);
  // Capturing entire state. Dump anything that has been stored there.
  log.t.clear();
  log.t.seq = log.seq_live;
  // From now on, no changes to log.t are permitted until we finish rewriting log.
  // Can allow dirty to remain dirty - log.seq_live will not change.

  //
  // Part 1.
  // Build new log starter and compacted metadata body
  // 1.1. Build full compacted meta transaction.
  //      Encode a bluefs transaction that dumps all of the in-memory fnodes
  //      and names.
  //      This might be pretty large and its allocation map can exceed
  //      superblock size. Hence instead we'll need log starter part which
  //      goes to superblock and refers that new meta through op_update_inc.
  // 1.2.  Allocate space for the above transaction
  //       using its size estimation.
  // 1.3.  Allocate the space required for the starter part of the new log.
  //       It should be small enough to fit into superblock.
  // 1.4   Building new log persistent fnode representation which will
  //       finally land to disk.
  //       Depending on input parameters we might need to perform device ids
  //       rename - runtime and persistent replicas should be different when we
  //       are in the device migration process.
  // 1.5   Store starter fnode to run-time superblock, to be written out later.
  //       It doesn't contain compacted meta to fit relevant alocation map into
  //       superblock.
  // 1.6   Proceed building new log persistent fnode representation.
  //       Will add log tail with compacted meta extents from 1.1.
  //       Device rename applied as well
  //
  // 1.7.  Encode new log fnode starter,
  //       It will include op_init, new log's op_update_inc
  //       and jump to the compacted meta transaction beginning.
  //       Superblock will reference this starter part
  //
  // 1.8.  Encode compacted meta transaction,
  //       extend the transaction with a jump to proper sequence no
  //


  // 1.1 Build full compacted meta transaction
  bluefs_transaction_t compacted_meta_t;
  _compact_log_dump_metadata_NF(starter_seq + 1, &compacted_meta_t, flags, 0);

  // 1.2 Allocate the space required for the compacted meta transaction
  uint64_t compacted_meta_need =
    _estimate_transaction_size(&compacted_meta_t) +
      cct->_conf->bluefs_max_log_runway;

  dout(20) << __func__ << " compacted_meta_need " << compacted_meta_need << dendl;

  bluefs_fnode_t fnode_tail;
  int r = _allocate(log_dev, compacted_meta_need, 0, &fnode_tail, nullptr, 0,
    permit_dev_fallback);
  ceph_assert(r == 0);


  // 1.3 Allocate the space required for the starter part of the new log.
  // estimate new log fnode size to be referenced from superblock
  // hence use dummy fnode and jump parameters
  uint64_t starter_need = _make_initial_transaction(starter_seq, fnode_tail, 0, nullptr);

  bluefs_fnode_t fnode_starter(log_file->fnode.ino, 0, mtime);
  r = _allocate(log_dev, starter_need, 0, &fnode_starter, nullptr, 0,
    permit_dev_fallback);
  ceph_assert(r == 0);

  // 1.4 Building starter fnode
  bluefs_fnode_t fnode_persistent(fnode_starter.ino, 0, mtime);
  for (auto p : fnode_starter.extents) {
    // rename device if needed - this is possible when fallback allocations
    // are prohibited only. Which means every extent is targeted to the same
    // device and we can unconditionally update them.
    if (log_dev != log_dev_new) {
      dout(10) << __func__ << " renaming log extents to "
               << log_dev_new << dendl;
      p.bdev = log_dev_new;
    }
    fnode_persistent.append_extent(p);
  }

  // 1.5 Store starter fnode to run-time superblock, to be written out later
  super.log_fnode = fnode_persistent;

  // 1.6 Proceed building new log persistent fnode representation
  // we'll build incremental update starting from this point
  fnode_persistent.reset_delta();
  for (auto p : fnode_tail.extents) {
    // rename device if needed - this is possible when fallback allocations
    // are prohibited only. Which means every extent is targeted to the same
    // device and we can unconditionally update them.
    if (log_dev != log_dev_new) {
      dout(10) << __func__ << " renaming log extents to "
               << log_dev_new << dendl;
      p.bdev = log_dev_new;
    }
    fnode_persistent.append_extent(p);
  }

  // 1.7 Encode new log fnode
  // This will flush incremental part of fnode_persistent only.
  bufferlist starter_bl;
  _make_initial_transaction(starter_seq, fnode_persistent, starter_need, &starter_bl);

  // 1.8 Encode compacted meta transaction
  dout(20) << __func__ << " op_jump_seq " << log.seq_live << dendl;
  // hopefully "compact_meta_need" estimation provides enough extra space
  // for this op, assert below if not
  compacted_meta_t.op_jump_seq(log.seq_live);

  bufferlist compacted_meta_bl;
  encode(compacted_meta_t, compacted_meta_bl);
  _pad_bl(compacted_meta_bl);
  ceph_assert(compacted_meta_bl.length() <= compacted_meta_need);

  //
  // Part 2
  // Write out new log's content
  // 2.1. Build the full runtime new log's fnode
  //
  // 2.2. Write out new log's
  //
  // 2.3. Do flush and wait for completion through flush_bdev()
  //
  // 2.4. Finalize log update
  //      Update all sequence numbers
  //

  // 2.1 Build the full runtime new log's fnode
  bluefs_fnode_t old_log_fnode;
  old_log_fnode.swap(fnode_starter);
  old_log_fnode.clone_extents(fnode_tail);
  old_log_fnode.reset_delta();
  log_file->fnode.swap(old_log_fnode);

  // 2.2 Write out new log's content
  // Get rid off old writer
  _close_writer(log.writer);
  // Make new log writer and stage new log's content writing
  log.writer = _create_writer(log_file);
  log.writer->append(starter_bl);
  log.writer->append(compacted_meta_bl);

  // 2.3 Do flush and wait for completion through flush_bdev()
  _flush_special(log.writer);
#ifdef HAVE_LIBAIO
  if (!cct->_conf->bluefs_sync_write) {
    list<aio_t> completed_ios;
    _claim_completed_aios(log.writer, &completed_ios);
    _wait_for_aio(log.writer);
    completed_ios.clear();
  }
#endif
  _flush_bdev();

  // 2.4 Finalize log update
  ++log.seq_live;
  dirty.seq_live = log.seq_live;
  log.t.seq = log.seq_live;
  vselector->sub_usage(log_file->vselector_hint, old_log_fnode);
  vselector->add_usage(log_file->vselector_hint, log_file->fnode);

  // Part 3.
  // Write out new superblock to reflect all the changes.
  //

  super.memorized_layout = layout;
  _write_super(super_dev);
  _flush_bdev();

  // we're mostly done
  dout(10) << __func__ << " log extents " << log_file->fnode.extents << dendl;
  logger->inc(l_bluefs_log_compactions);

  // Part 4
  // Finalization. Release old space.
  //
  {
    dout(10) << __func__
             << " release old log extents " << old_log_fnode.extents
             << dendl;
    std::lock_guard dl(dirty.lock);
    for (auto& r : old_log_fnode.extents) {
      dirty.pending_release[r.bdev].insert(r.offset, r.length);
    }
  }
  logger->tinc(l_bluefs_compaction_lock_lat, mono_clock::now() - t0);
}

/*
 * ASYNC LOG COMPACTION
 *
 * 0. Lock the log and forbid its extension. The former covers just
 *    a part of the below procedure while the latter spans over it
 *    completely.
 * 1. Allocate a new extent to continue the log, and then log an event
 *    that jumps the log write position to the new extent.  At this point, the
 *    old extent(s) won't be written to, and reflect everything to compact.
 *    New events will be written to the new region that we'll keep.
 *    The latter will finally become new log tail on compaction completion.
 *
 * 2. Build new log. It will include log's starter, compacted metadata
 *    body and the above tail. Jump ops appended to the starter and meta body
 *    will link the pieces togather. Log's lock is releases in the mid of the
 *    process to permit parallel access to it.
 *
 * 3. Write out new log's content.
 *
 * 4. Write out new superblock to reflect all the changes.
 *
 * 5. Apply new log fnode, log is locked for a while.
 *
 * 6. Finalization. Clean up, old space release and total unlocking.
 */

void BlueFS::_compact_log_async_LD_LNF_D() //also locks FW for new_writer
{
  dout(10) << __func__ << dendl;
  utime_t mtime = ceph_clock_now();
  uint64_t starter_seq = 1;
  uint64_t old_log_jump_to = 0;

  // Part 0.
  // Lock the log and forbid its expansion and other compactions

  // lock log's run-time structures for a while
  log.lock.lock();

  // Extend log in case of having a big transaction waiting before starting compaction.
  _maybe_extend_log();

  // only one compaction allowed at one time
  bool old_is_comp = std::atomic_exchange(&log_is_compacting, true);
  if (old_is_comp) {
    dout(10) << __func__ << " ongoing" <<dendl;
    log.lock.unlock();
    return;
  }
  auto t0 = mono_clock::now();

  // Part 1.
  // Prepare current log for jumping into it.
  // 1. Allocate extent
  // 2. Update op to log
  // 3. Jump op to log
  // During that, no one else can write to log, otherwise we risk jumping backwards.
  // We need to sync log, because we are injecting discontinuity, and writer is not prepared for that.

  //signal _extend_log that expansion of log is temporary inacceptable
  bool old_forbidden = atomic_exchange(&log_forbidden_to_expand, true);
  ceph_assert(old_forbidden == false);

  //
  // Part 1.
  // Prepare current log for jumping into it.
  // 1.1. Allocate extent
  // 1.2. Save log's fnode extents and add new extents
  // 1.3. Update op to log
  // 1.4. Jump op to log
  // During that, no one else can write to log, otherwise we risk jumping backwards.
  // We need to sync log, because we are injecting discontinuity, and writer is not prepared for that.

  // 1.1 allocate new log extents and store them at fnode_tail
  File *log_file = log.writer->file.get();

  old_log_jump_to = log_file->fnode.get_allocated();
  bluefs_fnode_t fnode_tail;
  dout(10) << __func__ << " old_log_jump_to 0x" << std::hex << old_log_jump_to
           << " need 0x" << cct->_conf->bluefs_max_log_runway << std::dec << dendl;
  int r = _allocate(vselector->select_prefer_bdev(log_file->vselector_hint),
		    cct->_conf->bluefs_max_log_runway,
                    0,
                    &fnode_tail);
  ceph_assert(r == 0);

  // 1.2 save log's fnode extents and add new extents
  bluefs_fnode_t old_log_fnode(log_file->fnode);
  log_file->fnode.clone_extents(fnode_tail);
  //adjust usage as flush below will need it
  vselector->sub_usage(log_file->vselector_hint, old_log_fnode);
  vselector->add_usage(log_file->vselector_hint, log_file->fnode);
  dout(10) << __func__ << " log extents " << log_file->fnode.extents << dendl;

  // 1.3 update the log file change and log a jump to the offset where we want to
  // write the new entries
  log.t.op_file_update_inc(log_file->fnode);

  // 1.4 jump to new position should mean next seq
  log.t.op_jump(log.seq_live + 1, old_log_jump_to);
  uint64_t seq_now = log.seq_live;
  // we need to flush all bdev because we will be streaming all dirty files to log
  // TODO - think - if _flush_and_sync_log_jump will not add dirty files nor release pending allocations
  // then flush_bdev() will not be necessary
  _flush_bdev();
  _flush_and_sync_log_jump_D(old_log_jump_to);

  //
  // Part 2.
  // Build new log starter and compacted metadata body
  // 2.1.  Build full compacted meta transaction.
  //       While still holding the lock, encode a bluefs transaction
  //       that dumps all of the in-memory fnodes and names.
  //       This might be pretty large and its allocation map can exceed
  //       superblock size. Hence instead we'll need log starter part which
  //       goes to superblock and refers that new meta through op_update_inc.
  // 2.2.  After releasing the lock allocate space for the above transaction
  //       using its size estimation.
  //       Then build tailing list of extents which consists of these
  //       newly allocated extents followed by ones from Part 1.
  // 2.3.  Allocate the space required for the starter part of the new log.
  //       It should be small enough to fit into superblock.
  //       Effectively we start building new log fnode here.
  // 2.4.  Store starter fnode to run-time superblock, to be written out later
  // 2.5.  Finalize new log's fnode building
  //       This will include log's starter and tailing extents built at 2.2
  // 2.6.  Encode new log fnode starter,
  //       It will include op_init, new log's op_update_inc
  //       and jump to the compacted meta transaction beginning.
  //       Superblock will reference this starter part
  // 2.7.  Encode compacted meta transaction,
  //       extend the transaction with a jump to the log tail from 1.1 before
  //       encoding.
  //

  // 2.1 Build full compacted meta transaction
  bluefs_transaction_t compacted_meta_t;
  _compact_log_dump_metadata_NF(starter_seq + 1, &compacted_meta_t, 0, seq_now);

  // now state is captured to compacted_meta_t,
  // current log can be used to write to,
  //ops in log will be continuation of captured state
  logger->tinc(l_bluefs_compaction_lock_lat, mono_clock::now() - t0);
  log.lock.unlock();

  // 2.2 Allocate the space required for the compacted meta transaction
  uint64_t compacted_meta_need = _estimate_transaction_size(&compacted_meta_t);
  dout(20) << __func__ << " compacted_meta_need " << compacted_meta_need
           << dendl;
  {
    bluefs_fnode_t fnode_pre_tail;
    // do allocate
    r = _allocate(vselector->select_prefer_bdev(log_file->vselector_hint),
                  compacted_meta_need,
                  0,
                  &fnode_pre_tail);
    ceph_assert(r == 0);
    // build trailing list of extents in fnode_tail,
    // this will include newly allocated extents for compacted meta
    // and aux extents allocated at step 1.1
    fnode_pre_tail.claim_extents(fnode_tail.extents);
    fnode_tail.swap_extents(fnode_pre_tail);
  }

  // 2.3 Allocate the space required for the starter part of the new log.
  // Start building New log fnode
  FileRef new_log = nullptr;
  new_log = ceph::make_ref<File>();
  new_log->fnode.ino = log_file->fnode.ino;
  new_log->fnode.mtime = mtime;
  // Estimate the required space
  uint64_t starter_need =
    _make_initial_transaction(starter_seq, fnode_tail, 0, nullptr);
  // and now allocate and store at new_log_fnode
  r = _allocate(vselector->select_prefer_bdev(log_file->vselector_hint),
                starter_need,
                0,
                &new_log->fnode);
  ceph_assert(r == 0);

  // 2.4 Store starter fnode to run-time superblock, to be written out later
  super.log_fnode = new_log->fnode;

  // 2.5 Finalize new log's fnode building
  // start collecting new log fnode updates (to make op_update_inc later)
  // since this point. This will include compacted meta from 2.2 and aux
  // extents from 1.1.
  new_log->fnode.reset_delta();
  new_log->fnode.claim_extents(fnode_tail.extents);

  // 2.6 Encode new log fnode
  bufferlist starter_bl;
  _make_initial_transaction(starter_seq, new_log->fnode, starter_need,
    &starter_bl);

  // 2.7 Encode compacted meta transaction,
  dout(20) << __func__
           << " new_log jump seq " << seq_now
           << std::hex << " offset 0x" << starter_need + compacted_meta_need
	   << std::dec << dendl;
  // Extent compacted_meta transaction with a just to new log tail.
  // Hopefully "compact_meta_need" estimation provides enough extra space
  // for this new jump, assert below if not
  compacted_meta_t.op_jump(seq_now, starter_need + compacted_meta_need);
  // Now do encodeing and padding
  bufferlist compacted_meta_bl;
  compacted_meta_bl.reserve(compacted_meta_need);
  encode(compacted_meta_t, compacted_meta_bl);
  ceph_assert(compacted_meta_bl.length() <= compacted_meta_need);
  _pad_bl(compacted_meta_bl, compacted_meta_need);

  //
  // Part 3.
  // Write out new log's content
  // 3.1 Stage new log's content writing
  // 3.2 Do flush and wait for completion through flush_bdev()
  //

  // 3.1 Stage new log's content writing
  // Make new log writer and append bufferlists to write out.
  FileWriter *new_log_writer = _create_writer(new_log);
  // And append all new log's bufferlists to write out.
  new_log_writer->append(starter_bl);
  new_log_writer->append(compacted_meta_bl);

  // 3.2. flush and wait
  _flush_special(new_log_writer);
  _flush_bdev(new_log_writer, false); // do not check log.lock is locked

  // Part 4.
  // Write out new superblock to reflect all the changes.
  //

  _write_super(BDEV_DB);
  _flush_bdev();

  // Part 5.
  // Apply new log fnode
  //

  // we need to acquire log's lock back at this point
  log.lock.lock();
  // Reconstruct actual log object from the new one.
  vselector->sub_usage(log_file->vselector_hint, log_file->fnode);
  log_file->fnode.size =
    log.writer->pos - old_log_jump_to + starter_need + compacted_meta_need;
  log_file->fnode.mtime = std::max(mtime, log_file->fnode.mtime);
  log_file->fnode.swap_extents(new_log->fnode);
  // update log's writer
  log.writer->pos = log.writer->file->fnode.size;
  vselector->add_usage(log_file->vselector_hint, log_file->fnode);
  // and unlock
  log.lock.unlock();

  // we're mostly done
  dout(10) << __func__ << " log extents " << log_file->fnode.extents << dendl;
  logger->inc(l_bluefs_log_compactions);

  //Part 6.
  // Finalization
  // 6.1 Permit log's extension, forbidden at step 0.
  //
  // 6.2 Release the new log writer
  //
  // 6.3 Release old space
  //
  // 6.4. Enable other compactions
  //

  // 6.1 Permit log's extension, forbidden at step 0.
  old_forbidden = atomic_exchange(&log_forbidden_to_expand, false);
  ceph_assert(old_forbidden == true);
  //to wake up if someone was in need of expanding log
  log_cond.notify_all();

  // 6.2 Release the new log writer
  _close_writer(new_log_writer);
  new_log_writer = nullptr;
  new_log = nullptr;

  // 6.3 Release old space
  {
    dout(10) << __func__
             << " release old log extents " << old_log_fnode.extents
             << dendl;
    std::lock_guard dl(dirty.lock);
    for (auto& r : old_log_fnode.extents) {
      dirty.pending_release[r.bdev].insert(r.offset, r.length);
    }
  }

  // 6.4. Enable other compactions
  old_is_comp = atomic_exchange(&log_is_compacting, false);
  ceph_assert(old_is_comp);
}

void BlueFS::_pad_bl(bufferlist& bl, uint64_t pad_size)
{
  pad_size = std::max(pad_size, uint64_t(super.block_size));
  uint64_t partial = bl.length() % pad_size;
  if (partial) {
    dout(10) << __func__ << " padding with 0x" << std::hex
	     << pad_size - partial << " zeros" << std::dec << dendl;
    bl.append_zero(pad_size - partial);
  }
}


// Returns log seq that was live before advance.
uint64_t BlueFS::_log_advance_seq()
{
  ceph_assert(ceph_mutex_is_locked(dirty.lock));
  ceph_assert(ceph_mutex_is_locked(log.lock));
  //acquire new seq
  // this will became seq_stable once we write
  ceph_assert(dirty.seq_stable < dirty.seq_live);
  ceph_assert(log.t.seq == log.seq_live);
  uint64_t seq = log.seq_live;
  log.t.uuid = super.uuid;

  ++dirty.seq_live;
  ++log.seq_live;
  ceph_assert(dirty.seq_live == log.seq_live);
  return seq;
}


// Adds to log.t file modifications mentioned in `dirty.files`.
// Note: some bluefs ops may have already been stored in log.t transaction.
void BlueFS::_consume_dirty(uint64_t seq)
{
  ceph_assert(ceph_mutex_is_locked(dirty.lock));
  ceph_assert(ceph_mutex_is_locked(log.lock));

  // log dirty files
  // we just incremented log_seq. It is now illegal to add to dirty.files[log_seq]
  auto lsi = dirty.files.find(seq);
  if (lsi != dirty.files.end()) {
    dout(20) << __func__ << " " << lsi->second.size() << " dirty.files" << dendl;
    for (auto &f : lsi->second) {
      // fnode here is protected indirectly
      // the only path that adds to dirty.files goes from _fsync()
      // _fsync() is executed under writer lock,
      // and does not exit until syncing log is done
      dout(20) << __func__ << "   op_file_update_inc " << f.fnode << dendl;
      log.t.op_file_update_inc(f.fnode);
    }
  }
}

int64_t BlueFS::_maybe_extend_log() {
  uint64_t runway = log.writer->file->fnode.get_allocated() - log.writer->get_effective_write_pos();
  // increasing the size of the log involves adding a OP_FILE_UPDATE_INC which its size will 
  // increase with respect the number of extents. bluefs_min_log_runway should cover the max size 
  // a log can get.
  // inject new allocation in case log is too big
  size_t expected_log_size = 0;
  log.t.bound_encode(expected_log_size);
  if (expected_log_size + cct->_conf->bluefs_min_log_runway > runway) {
    _extend_log(expected_log_size + cct->_conf->bluefs_max_log_runway);
  } else if (runway < cct->_conf->bluefs_min_log_runway) {
    _extend_log(cct->_conf->bluefs_max_log_runway);
  }
  runway = log.writer->file->fnode.get_allocated() - log.writer->get_effective_write_pos();
  return runway;
}

void BlueFS::_extend_log(uint64_t amount) {
  ceph_assert(ceph_mutex_is_locked(log.lock));
  std::unique_lock<ceph::mutex> ll(log.lock, std::adopt_lock);
  while (log_forbidden_to_expand.load() == true) {
    log_cond.wait(ll);
  }
  ll.release();
  uint64_t allocated_before_extension = log.writer->file->fnode.get_allocated();
  amount = round_up_to(amount, super.block_size);
  int r = _allocate(
      vselector->select_prefer_bdev(log.writer->file->vselector_hint),
      amount,
      0,
      &log.writer->file->fnode,
      [&](const bluefs_extent_t& e) {
        vselector->add_usage(log.writer->file->vselector_hint, e);
      });
  ceph_assert(r == 0);
  dout(10) << "extended log by 0x" << std::hex << amount << " bytes " << dendl;

  bluefs_transaction_t log_extend_transaction;
  log_extend_transaction.seq = log.t.seq;
  log_extend_transaction.uuid = log.t.uuid;
  log_extend_transaction.op_file_update_inc(log.writer->file->fnode);

  bufferlist bl;
  bl.reserve(super.block_size);
  encode(log_extend_transaction, bl);
  _pad_bl(bl, super.block_size);
  log.writer->append(bl);
  ceph_assert(allocated_before_extension >= log.writer->get_effective_write_pos());

  // before sync_core we advance the seq
  {
    std::unique_lock<ceph::mutex> l(dirty.lock);
    dirty.seq_live++;
    log.seq_live++;
    log.t.seq++;
  }
}

void BlueFS::_flush_and_sync_log_core()
{
  ceph_assert(ceph_mutex_is_locked(log.lock));
  dout(10) << __func__ << " " << log.t << dendl;


  bufferlist bl;
  bl.reserve(super.block_size);
  encode(log.t, bl);
  // pad to block boundary
  size_t realign = super.block_size - (bl.length() % super.block_size);
  if (realign && realign != super.block_size)
    bl.append_zero(realign);

  logger->inc(l_bluefs_log_write_count, 1);
  logger->inc(l_bluefs_logged_bytes, bl.length());

  uint64_t runway = log.writer->file->fnode.get_allocated() - log.writer->get_effective_write_pos();
  // ensure runway is big enough, this should be taken care of by _maybe_extend_log,
  // but let's keep this here just in case.
  ceph_assert(bl.length() <= runway); 


  log.writer->append(bl);

  // prepare log for new transactions
  log.t.clear();
  log.t.seq = log.seq_live;

  uint64_t new_data = _flush_special(log.writer);
  vselector->add_usage(log.writer->file->vselector_hint, new_data);
}

// Clears dirty.files up to (including) seq_stable.
void BlueFS::_clear_dirty_set_stable_D(uint64_t seq)
{
  std::lock_guard dl(dirty.lock);

  // clean dirty files
  if (seq > dirty.seq_stable) {
    dirty.seq_stable = seq;
    dout(20) << __func__ << " seq_stable " << dirty.seq_stable << dendl;

    // undirty all files that were already streamed to log
    auto p = dirty.files.begin();
    while (p != dirty.files.end()) {
      if (p->first > dirty.seq_stable) {
        dout(20) << __func__ << " done cleaning up dirty files" << dendl;
        break;
      }

      auto l = p->second.begin();
      while (l != p->second.end()) {
        File *file = &*l;
        ceph_assert(file->dirty_seq <= dirty.seq_stable);
        dout(20) << __func__ << " cleaned file " << file->fnode.ino << dendl;
        file->dirty_seq = dirty.seq_stable;
        p->second.erase(l++);
      }

      ceph_assert(p->second.empty());
      dirty.files.erase(p++);
    }
  } else {
    dout(20) << __func__ << " seq_stable " << dirty.seq_stable
             << " already >= out seq " << seq
             << ", we lost a race against another log flush, done" << dendl;
  }
}

void BlueFS::_release_pending_allocations(vector<interval_set<uint64_t>>& to_release)
{
  for (unsigned i = 0; i < to_release.size(); ++i) {
    if (to_release[i].empty()) {
        continue;
    }
    /* OK, now we have the guarantee alloc[i] won't be null. */

    bool discard_queued = bdev[i]->try_discard(to_release[i]);
    if (!discard_queued) {
      alloc[i]->release(to_release[i]);
      if (is_shared_alloc(i)) {
        shared_alloc->bluefs_used -= to_release[i].size();
      }
    }
  }
}

int BlueFS::_flush_and_sync_log_LD(uint64_t want_seq)
{
  log.lock.lock();
  dirty.lock.lock();
  if (want_seq && want_seq <= dirty.seq_stable) {
    dout(10) << __func__ << " want_seq " << want_seq << " <= seq_stable "
      << dirty.seq_stable << ", done" << dendl;
    dirty.lock.unlock();
    log.lock.unlock();
    return 0;
  }
  
  ceph_assert(want_seq == 0 || want_seq <= dirty.seq_live); // illegal to request seq that was not created yet
  uint64_t seq =_log_advance_seq();
  _consume_dirty(seq);
  vector<interval_set<uint64_t>> to_release(dirty.pending_release.size());
  to_release.swap(dirty.pending_release);
  dirty.lock.unlock();

  _maybe_extend_log();
  _flush_and_sync_log_core();
  _flush_bdev(log.writer);
  logger->set(l_bluefs_log_bytes, log.writer->file->fnode.size);
  //now log.lock is no longer needed
  log.lock.unlock();

  _clear_dirty_set_stable_D(seq);
  _release_pending_allocations(to_release);

  _update_logger_stats();
  return 0;
}

// Flushes log and immediately adjusts log_writer pos.
int BlueFS::_flush_and_sync_log_jump_D(uint64_t jump_to)
{
  ceph_assert(ceph_mutex_is_locked(log.lock));

  ceph_assert(jump_to);
  // we synchronize writing to log, by lock to log.lock

  dirty.lock.lock();
  uint64_t seq =_log_advance_seq();
  _consume_dirty(seq);
  vector<interval_set<uint64_t>> to_release(dirty.pending_release.size());
  to_release.swap(dirty.pending_release);
  dirty.lock.unlock();
  _flush_and_sync_log_core();

  dout(10) << __func__ << " jumping log offset from 0x" << std::hex
	   << log.writer->pos << " -> 0x" << jump_to << std::dec << dendl;
  log.writer->pos = jump_to;
  vselector->sub_usage(log.writer->file->vselector_hint, log.writer->file->fnode.size);
  log.writer->file->fnode.size = jump_to;
  vselector->add_usage(log.writer->file->vselector_hint, log.writer->file->fnode.size);

  _flush_bdev(log.writer);

  _clear_dirty_set_stable_D(seq);
  _release_pending_allocations(to_release);

  logger->set(l_bluefs_log_bytes, log.writer->file->fnode.size);
  _update_logger_stats();
  return 0;
}

ceph::bufferlist BlueFS::FileWriter::flush_buffer(
  CephContext* const cct,
  const bool partial,
  const unsigned length,
  const bluefs_super_t& super)
{
  ceph_assert(ceph_mutex_is_locked(this->lock) || file->fnode.ino <= 1);
  ceph::bufferlist bl;
  if (partial) {
    tail_block.splice(0, tail_block.length(), &bl);
  }
  const auto remaining_len = length - bl.length();
  buffer.splice(0, remaining_len, &bl);
  if (buffer.length()) {
    dout(20) << " leaving 0x" << std::hex << buffer.length() << std::dec
             << " unflushed" << dendl;
  }
  if (const unsigned tail = bl.length() & ~super.block_mask(); tail) {
    const auto padding_len = super.block_size - tail;
    dout(20) << __func__ << " caching tail of 0x"
             << std::hex << tail
             << " and padding block with 0x" << padding_len
             << " buffer.length() " << buffer.length()
             << std::dec << dendl;
    // We need to go through the `buffer_appender` to get a chance to
    // preserve in-memory contiguity and not mess with the alignment.
    // Otherwise a costly rebuild could happen in e.g. `KernelDevice`.
    buffer_appender.append_zero(padding_len);
    buffer.splice(buffer.length() - padding_len, padding_len, &bl);
    // Deep copy the tail here. This allows to avoid costlier copy on
    // bufferlist rebuild in e.g. `KernelDevice` and minimizes number
    // of memory allocations.
    // The alternative approach would be to place the entire tail and
    // padding on a dedicated, 4 KB long memory chunk. This shouldn't
    // trigger the rebuild while still being less expensive.
    buffer_appender.substr_of(bl, bl.length() - padding_len - tail, tail);
    buffer.splice(buffer.length() - tail, tail, &tail_block);
  } else {
    tail_block.clear();
  }
  return bl;
}

int BlueFS::_signal_dirty_to_log_D(FileWriter *h)
{
  ceph_assert(ceph_mutex_is_locked(h->lock));
  std::lock_guard dl(dirty.lock);
  if (h->file->deleted) {
    dout(10) << __func__ << "  deleted, no-op" << dendl;
    return 0;
  }

  h->file->fnode.mtime = ceph_clock_now();
  ceph_assert(h->file->fnode.ino >= 1);
  if (h->file->dirty_seq <= dirty.seq_stable) {
    h->file->dirty_seq = dirty.seq_live;
    dirty.files[h->file->dirty_seq].push_back(*h->file);
    dout(20) << __func__ << " dirty_seq = " << dirty.seq_live
	     << " (was clean)" << dendl;
  } else {
    if (h->file->dirty_seq != dirty.seq_live) {
      // need re-dirty, erase from list first
      ceph_assert(dirty.files.count(h->file->dirty_seq));
      auto it = dirty.files[h->file->dirty_seq].iterator_to(*h->file);
      dirty.files[h->file->dirty_seq].erase(it);
      h->file->dirty_seq = dirty.seq_live;
      dirty.files[h->file->dirty_seq].push_back(*h->file);
      dout(20) << __func__ << " dirty_seq = " << dirty.seq_live
	       << " (was " << h->file->dirty_seq << ")" << dendl;
    } else {
      dout(20) << __func__ << " dirty_seq = " << dirty.seq_live
	       << " (unchanged, do nothing) " << dendl;
    }
  }
  return 0;
}

void BlueFS::flush_range(FileWriter *h, uint64_t offset, uint64_t length)/*_WF*/
{
  _maybe_check_vselector_LNF();
  std::unique_lock hl(h->lock);
  _flush_range_F(h, offset, length);
}

int BlueFS::_flush_range_F(FileWriter *h, uint64_t offset, uint64_t length)
{
  auto t0 = mono_clock::now();
  ceph_assert(ceph_mutex_is_locked(h->lock));
  ceph_assert(h->file->num_readers.load() == 0);
  ceph_assert(h->file->fnode.ino > 1);

  dout(10) << __func__ << " " << h << " pos 0x" << std::hex << h->pos
	   << " 0x" << offset << "~" << length << std::dec
	   << " to " << h->file->fnode
	   << " hint " << h->file->vselector_hint
           << dendl;
  if (h->file->deleted) {
    dout(10) << __func__ << "  deleted, no-op" << dendl;
    return 0;
  }

  bool buffered = cct->_conf->bluefs_buffered_io;

  if (offset + length <= h->pos)
    return 0;
  if (offset < h->pos) {
    length -= h->pos - offset;
    offset = h->pos;
    dout(10) << " still need 0x"
             << std::hex << offset << "~" << length << std::dec
             << dendl;
  }
  std::lock_guard file_lock(h->file->lock);
  ceph_assert(offset <= h->file->fnode.size);

  uint64_t allocated = h->file->fnode.get_allocated();
  // do not bother to dirty the file if we are overwriting
  // previously allocated extents.
  if (allocated < offset + length) {
    // we should never run out of log space here; see the min runway check
    // in _flush_and_sync_log.
    int r = _allocate(vselector->select_prefer_bdev(h->file->vselector_hint),
		      offset + length - allocated,
                      0,
		      &h->file->fnode,
		      [&](const bluefs_extent_t& e) {
		        vselector->add_usage(h->file->vselector_hint, e);
	              });
    if (r < 0) {
      derr << __func__ << " allocated: 0x" << std::hex << allocated
           << " offset: 0x" << offset << " length: 0x" << length << std::dec
           << dendl;
      ceph_abort_msg("bluefs enospc");
      return r;
    }
    h->file->is_dirty = true;
  }
  if (h->file->fnode.size < offset + length) {
    vselector->add_usage(h->file->vselector_hint, offset + length - h->file->fnode.size);
    h->file->fnode.size = offset + length;
    h->file->is_dirty = true;
  }
  dout(20) << __func__ << " file now, unflushed " << h->file->fnode << dendl;
  int res = _flush_data(h, offset, length, buffered);
  logger->tinc(l_bluefs_flush_lat, mono_clock::now() - t0);
  return res;
}

int BlueFS::_flush_data(FileWriter *h, uint64_t offset, uint64_t length, bool buffered)
{
  if (h->file->fnode.ino > 1) {
    ceph_assert(ceph_mutex_is_locked(h->lock));
    ceph_assert(ceph_mutex_is_locked(h->file->lock));
  }
  uint64_t x_off = 0;
  auto p = h->file->fnode.seek(offset, &x_off);
  ceph_assert(p != h->file->fnode.extents.end());
  dout(20) << __func__ << " in " << *p << " x_off 0x"
           << std::hex << x_off << std::dec << dendl;

  unsigned partial = x_off & ~super.block_mask();
  if (partial) {
    dout(20) << __func__ << " using partial tail 0x"
             << std::hex << partial << std::dec << dendl;
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

  auto bl = h->flush_buffer(cct, partial, length, super);
  ceph_assert(bl.length() >= length);
  h->pos = offset + length;
  length = bl.length();

  logger->inc(l_bluefs_write_count, 1);
  logger->inc(l_bluefs_write_bytes, length);

  switch (h->writer_type) {
  case WRITER_WAL:
    logger->inc(l_bluefs_write_count_wal, 1);
    logger->inc(l_bluefs_bytes_written_wal, length);
    break;
  case WRITER_SST:
    logger->inc(l_bluefs_write_count_sst, 1);
    logger->inc(l_bluefs_bytes_written_sst, length);
    break;
  }

  dout(30) << "dump:\n";
  bl.hexdump(*_dout);
  *_dout << dendl;

  uint64_t bloff = 0;
  uint64_t bytes_written_slow = 0;
  while (length > 0) {
    logger->inc(l_bluefs_write_disk_count, 1);

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

void BlueFS::_wait_for_aio(FileWriter *h)
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

void BlueFS::append_try_flush(FileWriter *h, const char* buf, size_t len)/*_WF_LNF_NF_LD_D*/
{
  bool flushed_sum = false;
  {
    std::unique_lock hl(h->lock);
    size_t max_size = 1ull << 30; // cap to 1GB
    while (len > 0) {
      bool need_flush = true;
      auto l0 = h->get_buffer_length();
      if (l0 < max_size) {
	size_t l = std::min(len, max_size - l0);
	h->append(buf, l);
	buf += l;
	len -= l;
	need_flush = h->get_buffer_length() >= cct->_conf->bluefs_min_flush_size;
      }
      if (need_flush) {
	bool flushed = false;
	int r = _flush_F(h, true, &flushed);
	ceph_assert(r == 0);
	flushed_sum |= flushed;
	// make sure we've made any progress with flush hence the
	// loop doesn't iterate forever
	ceph_assert(h->get_buffer_length() < max_size);
      }
    }
  }
  if (flushed_sum) {
    _maybe_compact_log_LNF_NF_LD_D();
  }
}

void BlueFS::flush(FileWriter *h, bool force)/*_WF_LNF_NF_LD_D*/
{
  bool flushed = false;
  int r;
  {
    std::unique_lock hl(h->lock);
    r = _flush_F(h, force, &flushed);
    ceph_assert(r == 0);
  }
  if (r == 0 && flushed) {
    _maybe_compact_log_LNF_NF_LD_D();
  }
}

int BlueFS::_flush_F(FileWriter *h, bool force, bool *flushed)
{
  ceph_assert(ceph_mutex_is_locked(h->lock));
  uint64_t length = h->get_buffer_length();
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
  int r = _flush_range_F(h, offset, length);
  if (flushed) {
    *flushed = true;
  }
  return r;
}

// Flush for bluefs special files.
// Does not add extents to h.
// Does not mark h as dirty.
// we do not need to dirty the log file (or it's compacting
// replacement) when the file size changes because replay is
// smart enough to discover it on its own.
uint64_t BlueFS::_flush_special(FileWriter *h)
{
  ceph_assert(h->file->fnode.ino <= 1);
  uint64_t length = h->get_buffer_length();
  uint64_t offset = h->pos;
  uint64_t new_data = 0;
  ceph_assert(length + offset <= h->file->fnode.get_allocated());
  if (h->file->fnode.size < offset + length) {
    new_data = offset + length - h->file->fnode.size;
    h->file->fnode.size = offset + length;
  }
  _flush_data(h, offset, length, false);
  return new_data;
}

int BlueFS::truncate(FileWriter *h, uint64_t offset)/*_WF_L*/
{
  auto t0 = mono_clock::now();
  std::lock_guard hl(h->lock);
  dout(10) << __func__ << " 0x" << std::hex << offset << std::dec
           << " file " << h->file->fnode << dendl;
  if (h->file->deleted) {
    dout(10) << __func__ << "  deleted, no-op" << dendl;
    return 0;
  }

  // we never truncate internal log files
  ceph_assert(h->file->fnode.ino > 1);

  // truncate off unflushed data?
  if (h->pos < offset &&
      h->pos + h->get_buffer_length() > offset) {
    dout(20) << __func__ << " tossing out last " << offset - h->pos
	     << " unflushed bytes" << dendl;
    ceph_abort_msg("actually this shouldn't happen");
  }
  if (h->get_buffer_length()) {
    int r = _flush_F(h, true);
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
  _flush_bdev(h);

  std::lock_guard ll(log.lock);
  vselector->sub_usage(h->file->vselector_hint, h->file->fnode.size - offset);
  h->file->fnode.size = offset;
  h->file->is_dirty = true;
  log.t.op_file_update_inc(h->file->fnode);
  logger->tinc(l_bluefs_truncate_lat, mono_clock::now() - t0);
  return 0;
}

int BlueFS::fsync(FileWriter *h)/*_WF_WD_WLD_WLNF_WNF*/
{
  auto t0 = mono_clock::now();
  _maybe_check_vselector_LNF();
  std::unique_lock hl(h->lock);
  uint64_t old_dirty_seq = 0;
  {
    dout(10) << __func__ << " " << h << " " << h->file->fnode
             << " dirty " << h->file->is_dirty << dendl;
    int r = _flush_F(h, true);
    if (r < 0)
      return r;
    _flush_bdev(h);
    if (h->file->is_dirty) {
      _signal_dirty_to_log_D(h);
      h->file->is_dirty = false;
    }
    {
      std::lock_guard dl(dirty.lock);
      if (dirty.seq_stable < h->file->dirty_seq) {
	old_dirty_seq = h->file->dirty_seq;
	dout(20) << __func__ << " file metadata was dirty (" << old_dirty_seq
		 << ") on " << h->file->fnode << ", flushing log" << dendl;
      }
    }
  }
  if (old_dirty_seq) {
    _flush_and_sync_log_LD(old_dirty_seq);
  }
  _maybe_compact_log_LNF_NF_LD_D();
  logger->tinc(l_bluefs_fsync_lat, mono_clock::now() - t0);
  return 0;
}

// be careful - either h->file->lock or log.lock must be taken
void BlueFS::_flush_bdev(FileWriter *h, bool check_mutext_locked)
{
  if (check_mutext_locked) {
    if (h->file->fnode.ino > 1) {
      ceph_assert(ceph_mutex_is_locked(h->lock));
    } else if (h->file->fnode.ino == 1) {
      ceph_assert(ceph_mutex_is_locked(log.lock));
    }
  }
  std::array<bool, MAX_BDEV> flush_devs = h->dirty_devs;
  h->dirty_devs.fill(false);
#ifdef HAVE_LIBAIO
  if (!cct->_conf->bluefs_sync_write) {
    list<aio_t> completed_ios;
    _claim_completed_aios(h, &completed_ios);
    _wait_for_aio(h);
    completed_ios.clear();
  }
#endif
  _flush_bdev(flush_devs);
}

void BlueFS::_flush_bdev(std::array<bool, MAX_BDEV>& dirty_bdevs)
{
  // NOTE: this is safe to call without a lock.
  dout(20) << __func__ << dendl;
  for (unsigned i = 0; i < MAX_BDEV; i++) {
    if (dirty_bdevs[i])
      bdev[i]->flush();
  }
}

void BlueFS::_flush_bdev()
{
  // NOTE: this is safe to call without a lock.
  dout(20) << __func__ << dendl;
  for (unsigned i = 0; i < MAX_BDEV; i++) {
    // alloc space from BDEV_SLOW is unexpected.
    // So most cases we don't alloc from BDEV_SLOW and so avoiding flush not-used device.
    if (bdev[i] && (i != BDEV_SLOW || _get_used(i))) {
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

void BlueFS::_update_allocate_stats(uint8_t id, const ceph::timespan& d)
{
  switch(id) {
    case BDEV_SLOW:
      logger->tinc(l_bluefs_slow_alloc_lat, d);
      if (d > max_alloc_lat[id]) {
        logger->tset(l_bluefs_slow_alloc_max_lat, utime_t(d));
        max_alloc_lat[id] = d;
      }
      break;
    case BDEV_DB:
      logger->tinc(l_bluefs_db_alloc_lat, d);
      if (d > max_alloc_lat[id]) {
        logger->tset(l_bluefs_db_alloc_max_lat, utime_t(d));
        max_alloc_lat[id] = d;
      }
      break;
    case BDEV_WAL:
      logger->tinc(l_bluefs_wal_alloc_lat, d);
      if (d > max_alloc_lat[id]) {
        logger->tset(l_bluefs_wal_alloc_max_lat, utime_t(d));
        max_alloc_lat[id] = d;
      }
      break;
  }
}

int BlueFS::_allocate(uint8_t id, uint64_t len,
		      uint64_t alloc_unit,
		      bluefs_fnode_t* node,
                      update_fn_t cb,
                      size_t alloc_attempts,
                      bool permit_dev_fallback)
{
  dout(10) << __func__ << " len 0x" << std::hex << len
           << " au 0x" << alloc_unit
           << std::dec << " from " << (int)id
           << " cooldown " << cooldown_deadline
           << dendl;
  ceph_assert(id < alloc.size());
  int64_t alloc_len = 0;
  PExtentVector extents;
  uint64_t hint = 0;
  int64_t need = len;
  bool shared = is_shared_alloc(id);
  auto shared_unit = shared_alloc ? shared_alloc->alloc_unit : 0;
  bool was_cooldown = false;
  if (alloc[id]) {
    if (!alloc_unit) {
      alloc_unit = alloc_size[id];
    }
    // do not attempt shared_allocator with bluefs alloc unit
    // when cooling down, fallback to slow dev alloc unit.
    if (shared && alloc_unit != shared_unit) {
       if (duration_cast<seconds>(real_clock::now().time_since_epoch()).count() <
           cooldown_deadline) {
         logger->inc(l_bluefs_alloc_shared_size_fallbacks);
         alloc_unit = shared_unit;
         was_cooldown = true;
       } else if (cooldown_deadline.fetch_and(0)) {
         // we might get false cooldown_deadline reset at this point
         // but that's mostly harmless.
         dout(1) << __func__ << " shared allocation cooldown period elapsed"
                 << dendl;
       }
    }
    need = round_up_to(len, alloc_unit);
    if (!node->extents.empty() && node->extents.back().bdev == id) {
      hint = node->extents.back().end();
    }   
    ++alloc_attempts;
    extents.reserve(4);  // 4 should be (more than) enough for most allocations
    auto t0 = mono_clock::now();
    alloc_len = alloc[id]->allocate(need, alloc_unit, hint, &extents);
    _update_allocate_stats(id, mono_clock::now() - t0);
  }
  if (alloc_len < 0 || alloc_len < need) {
    if (alloc[id]) {
      if (alloc_len > 0) {
        alloc[id]->release(extents);
      }
      if (!was_cooldown && shared) {
        auto delay_s = cct->_conf->bluefs_failed_shared_alloc_cooldown;
        cooldown_deadline = delay_s +
          duration_cast<seconds>(real_clock::now().time_since_epoch()).count();
        dout(1) << __func__ << " shared allocation cooldown set for "
                << delay_s << "s"
                << dendl;
      }
      dout(1) << __func__ << " unable to allocate 0x" << std::hex << need
	      << " on bdev " << (int)id
              << ", allocator name " << alloc[id]->get_name()
              << ", allocator type " << alloc[id]->get_type()
              << ", capacity 0x" << alloc[id]->get_capacity()
              << ", block size 0x" << alloc[id]->get_block_size()
              << ", alloc unit 0x" << alloc_unit
              << ", free 0x" << alloc[id]->get_free()
              << ", fragmentation " << alloc[id]->get_fragmentation()
              << ", allocated 0x" << (alloc_len > 0 ? alloc_len : 0)
	      << std::dec << dendl;
    } else {
      dout(20) << __func__ << " alloc-id not set on index="<< (int)id
               << " unable to allocate 0x" << std::hex << need
	       << " on bdev " << (int)id << std::dec << dendl;
    }
    if (alloc[id] && shared && alloc_unit != shared_unit) {
      alloc_unit = shared_unit;
      dout(20) << __func__ << " fallback to bdev "
	       << (int)id
               << " with alloc unit 0x" << std::hex << alloc_unit
               << std::dec << dendl;
      logger->inc(l_bluefs_alloc_shared_size_fallbacks);
      return _allocate(id,
                       len,
                       alloc_unit,
                       node,
		       cb,
                       alloc_attempts,
                       permit_dev_fallback);
    } else if (permit_dev_fallback && id != BDEV_SLOW && alloc[id + 1]) {
      dout(20) << __func__ << " fallback to bdev "
	       << (int)id + 1
	       << dendl;
      if (alloc_attempts > 0 && is_shared_alloc(id + 1)) {
        logger->inc(l_bluefs_alloc_shared_dev_fallbacks);
      }
      return _allocate(id + 1,
                       len,
                       0, // back to default alloc unit
                       node,
		       cb,
                       alloc_attempts,
                       permit_dev_fallback);
    } else {
      derr << __func__ << " allocation failed, needed 0x" << std::hex << need
           << dendl;
    }
    return -ENOSPC;
  } else {
    uint64_t used = _get_used(id);
    if (max_bytes[id] < used) {
      logger->set(max_bytes_pcounters[id], used);
      max_bytes[id] = used;
    }
    if (shared) {
      shared_alloc->bluefs_used += alloc_len;
    }
  }

  for (auto& p : extents) {
    bluefs_extent_t e(id, p.offset, p.length);
    node->append_extent(e);
    if (cb) {
      cb(e);
    }
  }
  return 0;
}

int BlueFS::preallocate(FileRef f, uint64_t off, uint64_t len)/*_LF*/
{
  std::lock_guard ll(log.lock);
  std::lock_guard fl(f->lock);
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

    int r = _allocate(vselector->select_prefer_bdev(f->vselector_hint),
      want,
      0,
      &f->fnode,
      [&](const bluefs_extent_t& e) {
	vselector->add_usage(f->vselector_hint, e);
      });
    if (r < 0)
      return r;

    log.t.op_file_update_inc(f->fnode);
  }
  return 0;
}

void BlueFS::sync_metadata(bool avoid_compact)/*_LNF_NF_LD_D*/
{
  bool can_skip_flush;
  {
    std::lock_guard ll(log.lock);
    std::lock_guard dl(dirty.lock);
    can_skip_flush = log.t.empty() && dirty.files.empty();
  }
  if (can_skip_flush) {
    dout(10) << __func__ << " - no pending log events" << dendl;
  } else {
    utime_t start;
    lgeneric_subdout(cct, bluefs, 10) << __func__;
    start = ceph_clock_now();
    *_dout <<  dendl;
    _flush_bdev(); // FIXME?
    _flush_and_sync_log_LD();
    dout(10) << __func__ << " done in " << (ceph_clock_now() - start) << dendl;
  }

  if (!avoid_compact) {
    _maybe_compact_log_LNF_NF_LD_D();
  }
}

void BlueFS::_maybe_compact_log_LNF_NF_LD_D()
{
  if (!cct->_conf->bluefs_replay_recovery_disable_compact &&
      _should_start_compact_log_L_N()) {
    auto t0 = mono_clock::now();
    if (cct->_conf->bluefs_compact_log_sync) {
      _compact_log_sync_LNF_LD();
    } else {
      _compact_log_async_LD_LNF_D();
    }
    logger->tinc(l_bluefs_compaction_lat, mono_clock::now() - t0);
  }
}

int BlueFS::open_for_write(
  std::string_view dirname,
  std::string_view filename,
  FileWriter **h,
  bool overwrite)/*_LND*/
{
  _maybe_check_vselector_LNF();
  FileRef file;
  bool create = false;
  mempool::bluefs::vector<bluefs_extent_t> pending_release_extents;
  {
  std::lock_guard ll(log.lock);
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = nodes.dir_map.find(dirname);
  DirRef dir;
  if (p == nodes.dir_map.end()) {
    // implicitly create the dir
    dout(20) << __func__ << "  dir " << dirname
	     << " does not exist" << dendl;
    return -ENOENT;
  } else {
    dir = p->second;
  }

  map<string,FileRef>::iterator q = dir->file_map.lower_bound(filename);
  if (q == dir->file_map.end() || q->first != filename) {
    if (overwrite) {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " does not exist" << dendl;
      return -ENOENT;
    }
    file = ceph::make_ref<File>();
    file->fnode.ino = ++ino_last;
    file->vselector_hint = vselector->get_hint_by_dir(dirname);
    nodes.file_map[ino_last] = file;
    dir->file_map.emplace_hint(q, string{filename}, file);
    ++file->refs;
    create = true;
    vselector->add_usage(file->vselector_hint, file->fnode.size, true); // update file count
    logger->set(l_bluefs_num_files, nodes.file_map.size());
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
      vselector->add_usage(file->vselector_hint, file->fnode.size, true); // restore file count
      pending_release_extents.swap(file->fnode.extents);

      file->fnode.clear_extents();
    }
  }
  ceph_assert(file->fnode.ino > 1);

  file->fnode.mtime = ceph_clock_now();
  dout(20) << __func__ << " mapping " << dirname << "/" << filename
	   << " vsel_hint " << file->vselector_hint
	   << dendl;

  log.t.op_file_update(file->fnode);
  if (create)
    log.t.op_dir_link(dirname, filename, file->fnode.ino);

  std::lock_guard dl(dirty.lock);
  for (auto& p : pending_release_extents) {
    dirty.pending_release[p.bdev].insert(p.offset, p.length);
  }
  }
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

void BlueFS::_drain_writer(FileWriter *h)
{
  dout(10) << __func__ << " " << h << " type " << h->writer_type << dendl;
  //h->buffer.reassign_to_mempool(mempool::mempool_bluefs_file_writer);
  for (unsigned i=0; i<MAX_BDEV; ++i) {
    if (bdev[i]) {
      if (h->iocv[i]) {
	h->iocv[i]->aio_wait();
	delete h->iocv[i];
      }
    }
  }
  // sanity
  if (h->file->fnode.size >= (1ull << 30)) {
    dout(10) << __func__ << " file is unexpectedly large:" << h->file->fnode << dendl;
  }
}

void BlueFS::_close_writer(FileWriter *h)
{
  _drain_writer(h);
  delete h;
}
void BlueFS::close_writer(FileWriter *h)
{
  {
    std::lock_guard l(h->lock);
    _drain_writer(h);
  }
  delete h;
}

uint64_t BlueFS::debug_get_dirty_seq(FileWriter *h)
{
  std::lock_guard l(h->lock);
  return h->file->dirty_seq;
}

bool BlueFS::debug_get_is_dev_dirty(FileWriter *h, uint8_t dev)
{
  std::lock_guard l(h->lock);
  return h->dirty_devs[dev];
}

int BlueFS::open_for_read(
  std::string_view dirname,
  std::string_view filename,
  FileReader **h,
  bool random)/*_N*/
{
  _maybe_check_vselector_LNF();
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << dirname << "/" << filename
	   << (random ? " (random)":" (sequential)") << dendl;
  map<string,DirRef>::iterator p = nodes.dir_map.find(dirname);
  if (p == nodes.dir_map.end()) {
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
  std::string_view old_dirname, std::string_view old_filename,
  std::string_view new_dirname, std::string_view new_filename)/*_LND*/
{
  std::lock_guard ll(log.lock);
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << old_dirname << "/" << old_filename
	   << " -> " << new_dirname << "/" << new_filename << dendl;
  map<string,DirRef>::iterator p = nodes.dir_map.find(old_dirname);
  if (p == nodes.dir_map.end()) {
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

  p = nodes.dir_map.find(new_dirname);
  if (p == nodes.dir_map.end()) {
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
    log.t.op_dir_unlink(new_dirname, new_filename);
    _drop_link_D(q->second);
  }

  dout(10) << __func__ << " " << new_dirname << "/" << new_filename << " "
	   << " " << file->fnode << dendl;

  new_dir->file_map[string{new_filename}] = file;
  old_dir->file_map.erase(string{old_filename});

  log.t.op_dir_link(new_dirname, new_filename, file->fnode.ino);
  log.t.op_dir_unlink(old_dirname, old_filename);
  return 0;
}

int BlueFS::mkdir(std::string_view dirname)/*_LN*/
{
  std::lock_guard ll(log.lock);
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,DirRef>::iterator p = nodes.dir_map.find(dirname);
  if (p != nodes.dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " exists" << dendl;
    return -EEXIST;
  }
  nodes.dir_map[string{dirname}] = ceph::make_ref<Dir>();
  log.t.op_dir_create(dirname);
  return 0;
}

int BlueFS::rmdir(std::string_view dirname)/*_LN*/
{
  std::lock_guard ll(log.lock);
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << dirname << dendl;
  auto p = nodes.dir_map.find(dirname);
  if (p == nodes.dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " does not exist" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  if (!dir->file_map.empty()) {
    dout(20) << __func__ << " dir " << dirname << " not empty" << dendl;
    return -ENOTEMPTY;
  }
  nodes.dir_map.erase(p);
  log.t.op_dir_remove(dirname);
  return 0;
}

bool BlueFS::dir_exists(std::string_view dirname)/*_N*/
{
  std::lock_guard nl(nodes.lock);
  map<string,DirRef>::iterator p = nodes.dir_map.find(dirname);
  bool exists = p != nodes.dir_map.end();
  dout(10) << __func__ << " " << dirname << " = " << (int)exists << dendl;
  return exists;
}

int BlueFS::stat(std::string_view dirname, std::string_view filename,
		 uint64_t *size, utime_t *mtime)/*_N*/
{
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = nodes.dir_map.find(dirname);
  if (p == nodes.dir_map.end()) {
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

int BlueFS::lock_file(std::string_view dirname, std::string_view filename,
		      FileLock **plock)/*_LN*/
{
  std::lock_guard ll(log.lock);
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = nodes.dir_map.find(dirname);
  if (p == nodes.dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  auto q = dir->file_map.lower_bound(filename);
  FileRef file;
  if (q == dir->file_map.end() || q->first != filename) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found, creating" << dendl;
    file = ceph::make_ref<File>();
    file->fnode.ino = ++ino_last;
    file->fnode.mtime = ceph_clock_now();
    nodes.file_map[ino_last] = file;
    dir->file_map.emplace_hint(q, string{filename}, file);
    logger->set(l_bluefs_num_files, nodes.file_map.size());
    ++file->refs;
    log.t.op_file_update(file->fnode);
    log.t.op_dir_link(dirname, filename, file->fnode.ino);
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

int BlueFS::unlock_file(FileLock *fl)/*_N*/
{
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << fl << " on " << fl->file->fnode << dendl;
  ceph_assert(fl->file->locked);
  fl->file->locked = false;
  delete fl;
  return 0;
}

int BlueFS::readdir(std::string_view dirname, vector<string> *ls)/*_N*/
{
  // dirname may contain a trailing /
  if (!dirname.empty() && dirname.back() == '/') {
    dirname.remove_suffix(1);
  }
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << dirname << dendl;
  if (dirname.empty()) {
    // list dirs
    ls->reserve(nodes.dir_map.size() + 2);
    for (auto& q : nodes.dir_map) {
      ls->push_back(q.first);
    }
  } else {
    // list files in dir
    map<string,DirRef>::iterator p = nodes.dir_map.find(dirname);
    if (p == nodes.dir_map.end()) {
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

int BlueFS::unlink(std::string_view dirname, std::string_view filename)/*_LND*/
{
  auto t0 = mono_clock::now();
  std::lock_guard ll(log.lock);
  std::lock_guard nl(nodes.lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = nodes.dir_map.find(dirname);
  if (p == nodes.dir_map.end()) {
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
  dir->file_map.erase(q);
  log.t.op_dir_unlink(dirname, filename);
  _drop_link_D(file);
  logger->tinc(l_bluefs_unlink_lat, mono_clock::now() - t0);

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

bool BlueFS::db_is_rotational()
{
  if (bdev[BDEV_DB]) {
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
int BlueFS::_do_replay_recovery_read(FileReader *log_reader,
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
  int r = _bdev_read(e->bdev, e->offset + e_off, e->length - e_off, &fixed, ioc[e->bdev],
		     cct->_conf->bluefs_buffered_io);
  ceph_assert(r == 0);
  //capture dev of last good extent
  uint8_t last_e_dev = e->bdev;
  uint64_t last_e_off = e->offset;
  ++e;
  while (e != log_fnode.extents.end()) {
    r = _bdev_read(e->bdev, e->offset, e->length, &fixed, ioc[e->bdev],
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
    for (auto f : nodes.file_map) {
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
		<< get_device_name(dev) << ":0x" << std::hex << pos << "+" << chunk_len
		<< std::dec << dendl;
	r = _bdev_read_random(dev, pos, chunk_len,
	  raw_data + page_size, cct->_conf->bluefs_buffered_io);
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
	  r = _bdev_read(ne.bdev, ne.offset, ne.length, &candidate, ioc[ne.bdev],
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

void BlueFS::_check_vselector_LNF() {
  BlueFSVolumeSelector* vs = vselector->clone_empty();
  if (!vs) {
    return;
  }
  std::lock_guard ll(log.lock);
  std::lock_guard nl(nodes.lock);
  // Checking vselector is under log, nodes and file(s) locks,
  // so any modification of vselector must be under at least one of those locks.
  for (auto& f : nodes.file_map) {
    f.second->lock.lock();
    vs->add_usage(f.second->vselector_hint, f.second->fnode);
  }
  bool res = vselector->compare(vs);
  if (!res) {
    dout(0) << "Current:";
    vselector->dump(*_dout);
    *_dout << dendl;
    dout(0) << "Expected:";
    vs->dump(*_dout);
    *_dout << dendl;
  }
  ceph_assert(res);
  for (auto& f : nodes.file_map) {
    f.second->lock.unlock();
  }
  delete vs;
}

size_t BlueFS::probe_alloc_avail(int dev, uint64_t alloc_size)
{
  size_t total = 0;
  auto iterated_allocation = [&](size_t off, size_t len) {
    //only count in size that is alloc_size aligned
    size_t dist_to_alignment;
    size_t offset_in_block = off & (alloc_size - 1);
    if (offset_in_block == 0)
      dist_to_alignment = 0;
    else
      dist_to_alignment = alloc_size - offset_in_block;
    if (dist_to_alignment >= len)
      return;
    len -= dist_to_alignment;
    total += p2align(len, alloc_size);
  };
  if (alloc[dev]) {
    alloc[dev]->foreach(iterated_allocation);
  }
  return total;
}
// ===============================================
// OriginalVolumeSelector

void* OriginalVolumeSelector::get_hint_for_log() const {
  return reinterpret_cast<void*>(BlueFS::BDEV_WAL);
}
void* OriginalVolumeSelector::get_hint_by_dir(std::string_view dirname) const {
  uint8_t res = BlueFS::BDEV_DB;
  if (dirname.length() > 5) {
    // the "db.slow" and "db.wal" directory names are hard-coded at
    // match up with bluestore.  the slow device is always the second
    // one (when a dedicated block.db device is present and used at
    // bdev 0).  the wal device is always last.
    if (boost::algorithm::ends_with(dirname, ".slow") && slow_total) {
      res = BlueFS::BDEV_SLOW;
    } else if (boost::algorithm::ends_with(dirname, ".wal") && wal_total) {
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
  res.emplace_back(base + ".slow",
    slow_total ? slow_total : db_total); // use fake non-zero value if needed to
                                         // avoid RocksDB complains
}

#undef dout_prefix
#define dout_prefix *_dout << "OriginalVolumeSelector: "

void OriginalVolumeSelector::dump(ostream& sout) {
  sout<< "wal_total:" << wal_total
    << ", db_total:" << db_total
    << ", slow_total:" << slow_total
    << std::endl;
}

// ===============================================
// FitToFastVolumeSelector

void FitToFastVolumeSelector::get_paths(const std::string& base, paths& res) const {
  res.emplace_back(base, 1);  // size of the last db_path has no effect
}
