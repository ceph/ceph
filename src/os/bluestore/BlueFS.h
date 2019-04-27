// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_BLUEFS_H
#define CEPH_OS_BLUESTORE_BLUEFS_H

#include <atomic>
#include <mutex>

#include "bluefs_types.h"
#include "common/RefCountedObj.h"
#include "BlockDevice.h"

#include "boost/intrusive/list.hpp"
#include <boost/intrusive_ptr.hpp>

class PerfCounters;

class Allocator;

enum {
  l_bluefs_first = 732600,
  l_bluefs_gift_bytes,
  l_bluefs_reclaim_bytes,
  l_bluefs_db_total_bytes,
  l_bluefs_db_used_bytes,
  l_bluefs_wal_total_bytes,
  l_bluefs_wal_used_bytes,
  l_bluefs_slow_total_bytes,
  l_bluefs_slow_used_bytes,
  l_bluefs_num_files,
  l_bluefs_log_bytes,
  l_bluefs_log_compactions,
  l_bluefs_logged_bytes,
  l_bluefs_files_written_wal,
  l_bluefs_files_written_sst,
  l_bluefs_bytes_written_wal,
  l_bluefs_bytes_written_sst,
  l_bluefs_bytes_written_slow,
  l_bluefs_max_bytes_wal,
  l_bluefs_max_bytes_db,
  l_bluefs_max_bytes_slow,
  l_bluefs_last,
};

class BlueFSDeviceExpander {
protected:
  ~BlueFSDeviceExpander() {}
public:
  virtual uint64_t get_recommended_expansion_delta(uint64_t bluefs_free,
    uint64_t bluefs_total) = 0;
  virtual int allocate_freespace(
    uint64_t min_size,
    uint64_t size,
    PExtentVector& extents) = 0;
};

class BlueFS {
public:
  CephContext* cct;
  static constexpr unsigned MAX_BDEV = 5;
  static constexpr unsigned BDEV_WAL = 0;
  static constexpr unsigned BDEV_DB = 1;
  static constexpr unsigned BDEV_SLOW = 2;
  static constexpr unsigned BDEV_NEWWAL = 3;
  static constexpr unsigned BDEV_NEWDB = 4;

  enum {
    WRITER_UNKNOWN,
    WRITER_WAL,
    WRITER_SST,
  };

  struct File : public RefCountedObject {
    MEMPOOL_CLASS_HELPERS();

    bluefs_fnode_t fnode;
    int refs;
    uint64_t dirty_seq;
    bool locked;
    bool deleted;
    boost::intrusive::list_member_hook<> dirty_item;

    std::atomic_int num_readers, num_writers;
    std::atomic_int num_reading;

    File()
      : RefCountedObject(NULL, 0),
	refs(0),
	dirty_seq(0),
	locked(false),
	deleted(false),
	num_readers(0),
	num_writers(0),
	num_reading(0)
      {}
    ~File() override {
      ceph_assert(num_readers.load() == 0);
      ceph_assert(num_writers.load() == 0);
      ceph_assert(num_reading.load() == 0);
      ceph_assert(!locked);
    }

    friend void intrusive_ptr_add_ref(File *f) {
      f->get();
    }
    friend void intrusive_ptr_release(File *f) {
      f->put();
    }
  };
  typedef boost::intrusive_ptr<File> FileRef;

  typedef boost::intrusive::list<
      File,
      boost::intrusive::member_hook<
        File,
	boost::intrusive::list_member_hook<>,
	&File::dirty_item> > dirty_file_list_t;

  struct Dir : public RefCountedObject {
    MEMPOOL_CLASS_HELPERS();

    mempool::bluefs::map<string,FileRef> file_map;

    Dir() : RefCountedObject(NULL, 0) {}

    friend void intrusive_ptr_add_ref(Dir *d) {
      d->get();
    }
    friend void intrusive_ptr_release(Dir *d) {
      d->put();
    }
  };
  typedef boost::intrusive_ptr<Dir> DirRef;

  struct FileWriter {
    MEMPOOL_CLASS_HELPERS();

    FileRef file;
    uint64_t pos;           ///< start offset for buffer
    bufferlist buffer;      ///< new data to write (at end of file)
    bufferlist tail_block;  ///< existing partial block at end of file, if any
    bufferlist::page_aligned_appender buffer_appender;  //< for const char* only
    int writer_type = 0;    ///< WRITER_*
    int write_hint = WRITE_LIFE_NOT_SET;

    ceph::mutex lock = ceph::make_mutex("BlueFS::FileWriter::lock");
    std::array<IOContext*,MAX_BDEV> iocv; ///< for each bdev
    std::array<bool, MAX_BDEV> dirty_devs;

    FileWriter(FileRef f)
      : file(f),
	pos(0),
	buffer_appender(buffer.get_page_aligned_appender(
			  g_conf()->bluefs_alloc_size / CEPH_PAGE_SIZE)) {
      ++file->num_writers;
      iocv.fill(nullptr);
      dirty_devs.fill(false);
      if (f->fnode.ino == 1) {
	write_hint = WRITE_LIFE_MEDIUM;
      }
    }
    // NOTE: caller must call BlueFS::close_writer()
    ~FileWriter() {
      --file->num_writers;
    }

    // note: BlueRocksEnv uses this append exclusively, so it's safe
    // to use buffer_appender exclusively here (e.g., it's notion of
    // offset will remain accurate).
    void append(const char *buf, size_t len) {
      buffer_appender.append(buf, len);
    }

    // note: used internally only, for ino 1 or 0.
    void append(bufferlist& bl) {
      buffer.claim_append(bl);
    }

    uint64_t get_effective_write_pos() {
      buffer_appender.flush();
      return pos + buffer.length();
    }
  };

  struct FileReaderBuffer {
    MEMPOOL_CLASS_HELPERS();

    uint64_t bl_off;        ///< prefetch buffer logical offset
    bufferlist bl;          ///< prefetch buffer
    uint64_t pos;           ///< current logical offset
    uint64_t max_prefetch;  ///< max allowed prefetch

    explicit FileReaderBuffer(uint64_t mpf)
      : bl_off(0),
	pos(0),
	max_prefetch(mpf) {}

    uint64_t get_buf_end() {
      return bl_off + bl.length();
    }
    uint64_t get_buf_remaining(uint64_t p) {
      if (p >= bl_off && p < bl_off + bl.length())
	return bl_off + bl.length() - p;
      return 0;
    }

    void skip(size_t n) {
      pos += n;
    }
    void seek(uint64_t offset) {
      pos = offset;
    }
  };

  struct FileReader {
    MEMPOOL_CLASS_HELPERS();

    FileRef file;
    FileReaderBuffer buf;
    bool random;
    bool ignore_eof;        ///< used when reading our log file

    FileReader(FileRef f, uint64_t mpf, bool rand, bool ie)
      : file(f),
	buf(mpf),
	random(rand),
	ignore_eof(ie) {
      ++file->num_readers;
    }
    ~FileReader() {
      --file->num_readers;
    }
  };

  struct FileLock {
    MEMPOOL_CLASS_HELPERS();

    FileRef file;
    explicit FileLock(FileRef f) : file(f) {}
  };

private:
  ceph::mutex lock = ceph::make_mutex("BlueFS::lock");

  PerfCounters *logger = nullptr;

  uint64_t max_bytes[MAX_BDEV] = {0};
  uint64_t max_bytes_pcounters[MAX_BDEV] = {
    l_bluefs_max_bytes_wal,
    l_bluefs_max_bytes_db,
    l_bluefs_max_bytes_slow,
  };

  // cache
  mempool::bluefs::map<string, DirRef> dir_map;              ///< dirname -> Dir
  mempool::bluefs::unordered_map<uint64_t,FileRef> file_map; ///< ino -> File

  // map of dirty files, files of same dirty_seq are grouped into list.
  map<uint64_t, dirty_file_list_t> dirty_files;

  bluefs_super_t super;        ///< latest superblock (as last written)
  uint64_t ino_last = 0;       ///< last assigned ino (this one is in use)
  uint64_t log_seq = 0;        ///< last used log seq (by current pending log_t)
  uint64_t log_seq_stable = 0; ///< last stable/synced log seq
  FileWriter *log_writer = 0;  ///< writer for the log
  bluefs_transaction_t log_t;  ///< pending, unwritten log transaction
  bool log_flushing = false;   ///< true while flushing the log
  ceph::condition_variable log_cond;

  uint64_t new_log_jump_to = 0;
  uint64_t old_log_jump_to = 0;
  FileRef new_log = nullptr;
  FileWriter *new_log_writer = nullptr;

  /*
   * There are up to 3 block devices:
   *
   *  BDEV_DB   db/      - the primary db device
   *  BDEV_WAL  db.wal/  - a small, fast device, specifically for the WAL
   *  BDEV_SLOW db.slow/ - a big, slow device, to spill over to as BDEV_DB fills
   */
  vector<BlockDevice*> bdev;                  ///< block devices we can use
  vector<IOContext*> ioc;                     ///< IOContexts for bdevs
  vector<interval_set<uint64_t> > block_all;  ///< extents in bdev we own
  vector<Allocator*> alloc;                   ///< allocators for bdevs
  vector<interval_set<uint64_t>> pending_release; ///< extents to release

  BlockDevice::aio_callback_t discard_cb[3]; //discard callbacks for each dev

  BlueFSDeviceExpander* slow_dev_expander = nullptr;

  void _init_logger();
  void _shutdown_logger();
  void _update_logger_stats();

  void _init_alloc();
  void _stop_alloc();

  void _pad_bl(bufferlist& bl);  ///< pad bufferlist to block size w/ zeros

  FileRef _get_file(uint64_t ino);
  void _drop_link(FileRef f);

  int _get_slow_device_id() { return bdev[BDEV_SLOW] ? BDEV_SLOW : BDEV_DB; }
  int _expand_slow_device(uint64_t min_size, PExtentVector& extents);
  int _allocate(uint8_t bdev, uint64_t len,
		bluefs_fnode_t* node);
  int _allocate_without_fallback(uint8_t id, uint64_t len,
				 PExtentVector* extents);

  int _flush_range(FileWriter *h, uint64_t offset, uint64_t length);
  int _flush(FileWriter *h, bool force);
  int _fsync(FileWriter *h, std::unique_lock<ceph::mutex>& l);

#ifdef HAVE_LIBAIO
  void _claim_completed_aios(FileWriter *h, list<aio_t> *ls);
  void wait_for_aio(FileWriter *h);  // safe to call without a lock
#endif

  int _flush_and_sync_log(std::unique_lock<ceph::mutex>& l,
			  uint64_t want_seq = 0,
			  uint64_t jump_to = 0);
  uint64_t _estimate_log_size();
  bool _should_compact_log();

  enum {
    REMOVE_DB = 1,
    REMOVE_WAL = 2,
    RENAME_SLOW2DB = 4,
    RENAME_DB2SLOW = 8,
  };
  void _compact_log_dump_metadata(bluefs_transaction_t *t,
				  int flags);
  void _compact_log_sync();
  void _compact_log_async(std::unique_lock<ceph::mutex>& l);

  void _rewrite_log_sync(bool allocate_with_fallback,
			 int super_dev,
			 int log_dev,
			 int new_log_dev,
			 int flags);

  //void _aio_finish(void *priv);

  void _flush_bdev_safely(FileWriter *h);
  void flush_bdev();  // this is safe to call without a lock
  void flush_bdev(std::array<bool, MAX_BDEV>& dirty_bdevs);  // this is safe to call without a lock

  int _preallocate(FileRef f, uint64_t off, uint64_t len);
  int _truncate(FileWriter *h, uint64_t off);

  int _read(
    FileReader *h,   ///< [in] read from here
    FileReaderBuffer *buf, ///< [in] reader state
    uint64_t offset, ///< [in] offset
    size_t len,      ///< [in] this many bytes
    bufferlist *outbl,   ///< [out] optional: reference the result here
    char *out);      ///< [out] optional: or copy it here
  int _read_random(
    FileReader *h,   ///< [in] read from here
    uint64_t offset, ///< [in] offset
    size_t len,      ///< [in] this many bytes
    char *out);      ///< [out] optional: or copy it here

  void _invalidate_cache(FileRef f, uint64_t offset, uint64_t length);

  int _open_super();
  int _write_super(int dev);
  int _replay(bool noop, bool to_stdout = false); ///< replay journal

  FileWriter *_create_writer(FileRef f);
  void _close_writer(FileWriter *h);

  // always put the super in the second 4k block.  FIXME should this be
  // block size independent?
  unsigned get_super_offset() {
    return 4096;
  }
  unsigned get_super_length() {
    return 4096;
  }

  void _add_block_extent(unsigned bdev, uint64_t offset, uint64_t len);

public:
  BlueFS(CephContext* cct);
  ~BlueFS();

  // the super is always stored on bdev 0
  int mkfs(uuid_d osd_uuid);
  int mount();
  void umount();
  int prepare_new_device(int id);
  
  int log_dump();

  void collect_metadata(map<string,string> *pm, unsigned skip_bdev_id);
  void get_devices(set<string> *ls);
  int fsck();

  int device_migrate_to_new(
    CephContext *cct,
    const set<int>& devs_source,
    int dev_target);
  int device_migrate_to_existing(
    CephContext *cct,
    const set<int>& devs_source,
    int dev_target);

  uint64_t get_used();
  uint64_t get_total(unsigned id);
  uint64_t get_free(unsigned id);
  void get_usage(vector<pair<uint64_t,uint64_t>> *usage); // [<free,total> ...]
  void dump_perf_counters(Formatter *f);

  void dump_block_extents(ostream& out);

  /// get current extents that we own for given block device
  int get_block_extents(unsigned id, interval_set<uint64_t> *extents);

  int open_for_write(
    const string& dir,
    const string& file,
    FileWriter **h,
    bool overwrite);

  int open_for_read(
    const string& dir,
    const string& file,
    FileReader **h,
    bool random = false);

  void close_writer(FileWriter *h) {
    std::lock_guard l(lock);
    _close_writer(h);
  }

  int rename(const string& old_dir, const string& old_file,
	     const string& new_dir, const string& new_file);

  int readdir(const string& dirname, vector<string> *ls);

  int unlink(const string& dirname, const string& filename);
  int mkdir(const string& dirname);
  int rmdir(const string& dirname);
  bool wal_is_rotational();

  bool dir_exists(const string& dirname);
  int stat(const string& dirname, const string& filename,
	   uint64_t *size, utime_t *mtime);

  int lock_file(const string& dirname, const string& filename, FileLock **p);
  int unlock_file(FileLock *l);

  void flush_log();
  void compact_log();

  /// sync any uncommitted state to disk
  void sync_metadata();

  void set_slow_device_expander(BlueFSDeviceExpander* a) {
    slow_dev_expander = a;
  }
  int add_block_device(unsigned bdev, const string& path, bool trim,
		       bool shared_with_bluestore=false);
  bool bdev_support_label(unsigned id);
  uint64_t get_block_device_size(unsigned bdev);

  /// gift more block space
  void add_block_extent(unsigned bdev, uint64_t offset, uint64_t len) {
    std::unique_lock l(lock);
    _add_block_extent(bdev, offset, len);
    int r = _flush_and_sync_log(l);
    ceph_assert(r == 0);
  }

  /// reclaim block space
  int reclaim_blocks(unsigned bdev, uint64_t want,
		     PExtentVector *extents);

  // handler for discard event
  void handle_discard(unsigned dev, interval_set<uint64_t>& to_release);

  void flush(FileWriter *h) {
    std::lock_guard l(lock);
    _flush(h, false);
  }
  void flush_range(FileWriter *h, uint64_t offset, uint64_t length) {
    std::lock_guard l(lock);
    _flush_range(h, offset, length);
  }
  int fsync(FileWriter *h) {
    std::unique_lock l(lock);
    return _fsync(h, l);
  }
  int read(FileReader *h, FileReaderBuffer *buf, uint64_t offset, size_t len,
	   bufferlist *outbl, char *out) {
    // no need to hold the global lock here; we only touch h and
    // h->file, and read vs write or delete is already protected (via
    // atomics and asserts).
    return _read(h, buf, offset, len, outbl, out);
  }
  int read_random(FileReader *h, uint64_t offset, size_t len,
		  char *out) {
    // no need to hold the global lock here; we only touch h and
    // h->file, and read vs write or delete is already protected (via
    // atomics and asserts).
    return _read_random(h, offset, len, out);
  }
  void invalidate_cache(FileRef f, uint64_t offset, uint64_t len) {
    std::lock_guard l(lock);
    _invalidate_cache(f, offset, len);
  }
  int preallocate(FileRef f, uint64_t offset, uint64_t len) {
    std::lock_guard l(lock);
    return _preallocate(f, offset, len);
  }
  int truncate(FileWriter *h, uint64_t offset) {
    std::lock_guard l(lock);
    return _truncate(h, offset);
  }

};

#endif
