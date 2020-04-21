// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_BLUEFS_H
#define CEPH_OS_BLUESTORE_BLUEFS_H

#include <atomic>
#include <mutex>

#include "bluefs_types.h"
#include "blk/BlockDevice.h"

#include "common/RefCountedObj.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "include/common_fwd.h"

#include "boost/intrusive/list.hpp"
#include "boost/dynamic_bitset.hpp"

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
  l_bluefs_read_random_count,
  l_bluefs_read_random_bytes,
  l_bluefs_read_random_disk_count,
  l_bluefs_read_random_disk_bytes,
  l_bluefs_read_random_buffer_count,
  l_bluefs_read_random_buffer_bytes,
  l_bluefs_read_count,
  l_bluefs_read_bytes,
  l_bluefs_read_prefetch_count,
  l_bluefs_read_prefetch_bytes,

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
  /** Reports amount of space that can be transferred to BlueFS.
   * This gives either current state, when alloc_size is currently used
   * BlueFS's size, or simulation when alloc_size is different.
   * @params
   * alloc_size - allocation unit size to check
   */
  virtual uint64_t available_freespace(uint64_t alloc_size) = 0;
};

class BlueFSVolumeSelector {
public:
  typedef std::vector<std::pair<std::string, uint64_t>> paths;

  virtual ~BlueFSVolumeSelector() {
  }
  virtual void* get_hint_for_log() const = 0;
  virtual void* get_hint_by_dir(const std::string& dirname) const = 0;

  virtual void add_usage(void* file_hint, const bluefs_fnode_t& fnode) = 0;
  virtual void sub_usage(void* file_hint, const bluefs_fnode_t& fnode) = 0;
  virtual void add_usage(void* file_hint, uint64_t fsize) = 0;
  virtual void sub_usage(void* file_hint, uint64_t fsize) = 0;
  virtual uint8_t select_prefer_bdev(void* hint) = 0;
  virtual void get_paths(const std::string& base, paths& res) const = 0;
  virtual void dump(std::ostream& sout) = 0;
};
class BlueFS;

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

    void* vselector_hint = nullptr;

  private:
    FRIEND_MAKE_REF(File);
    File()
      :
	refs(0),
	dirty_seq(0),
	locked(false),
	deleted(false),
	num_readers(0),
	num_writers(0),
	num_reading(0),
        vselector_hint(nullptr)
      {}
    ~File() override {
      ceph_assert(num_readers.load() == 0);
      ceph_assert(num_writers.load() == 0);
      ceph_assert(num_reading.load() == 0);
      ceph_assert(!locked);
    }
  };
  using FileRef = ceph::ref_t<File>;

  typedef boost::intrusive::list<
      File,
      boost::intrusive::member_hook<
        File,
	boost::intrusive::list_member_hook<>,
	&File::dirty_item> > dirty_file_list_t;

  struct Dir : public RefCountedObject {
    MEMPOOL_CLASS_HELPERS();

    mempool::bluefs::map<std::string,FileRef> file_map;

  private:
    FRIEND_MAKE_REF(Dir);
    Dir() = default;
  };
  using DirRef = ceph::ref_t<Dir>;

  struct FileWriter {
    MEMPOOL_CLASS_HELPERS();

    FileRef file;
    uint64_t pos = 0;       ///< start offset for buffer
    ceph::buffer::list buffer;      ///< new data to write (at end of file)
    ceph::buffer::list tail_block;  ///< existing partial block at end of file, if any
    ceph::buffer::list::page_aligned_appender buffer_appender;  //< for const char* only
    int writer_type = 0;    ///< WRITER_*
    int write_hint = WRITE_LIFE_NOT_SET;

    ceph::mutex lock = ceph::make_mutex("BlueFS::FileWriter::lock");
    std::array<IOContext*,MAX_BDEV> iocv; ///< for each bdev
    std::array<bool, MAX_BDEV> dirty_devs;

    FileWriter(FileRef f)
      : file(std::move(f)),
	buffer_appender(buffer.get_page_aligned_appender(
			  g_conf()->bluefs_alloc_size / CEPH_PAGE_SIZE)) {
      ++file->num_writers;
      iocv.fill(nullptr);
      dirty_devs.fill(false);
      if (file->fnode.ino == 1) {
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
    void append(ceph::buffer::list& bl) {
      buffer.claim_append(bl);
    }

    uint64_t get_effective_write_pos() {
      buffer_appender.flush();
      return pos + buffer.length();
    }
  };

  struct FileReaderBuffer {
    MEMPOOL_CLASS_HELPERS();

    uint64_t bl_off = 0;    ///< prefetch buffer logical offset
    ceph::buffer::list bl;          ///< prefetch buffer
    uint64_t pos = 0;       ///< current logical offset
    uint64_t max_prefetch;  ///< max allowed prefetch

    explicit FileReaderBuffer(uint64_t mpf)
      : max_prefetch(mpf) {}

    uint64_t get_buf_end() const {
      return bl_off + bl.length();
    }
    uint64_t get_buf_remaining(uint64_t p) const {
      if (p >= bl_off && p < bl_off + bl.length())
	return bl_off + bl.length() - p;
      return 0;
    }

    void skip(size_t n) {
      pos += n;
    }

    // For the sake of simplicity, we invalidate completed rather than
    // for the provided extent
    void invalidate_cache(uint64_t offset, uint64_t length) {
      if (offset >= bl_off && offset < get_buf_end()) {
	bl.clear();
	bl_off = 0;
      }
    }
  };

  struct FileReader {
    MEMPOOL_CLASS_HELPERS();

    FileRef file;
    FileReaderBuffer buf;
    bool random;
    bool ignore_eof;        ///< used when reading our log file

    ceph::shared_mutex lock {
     ceph::make_shared_mutex(std::string(), false, false, false)
    };


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
    explicit FileLock(FileRef f) : file(std::move(f)) {}
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
  mempool::bluefs::map<std::string, DirRef> dir_map;              ///< dirname -> Dir
  mempool::bluefs::unordered_map<uint64_t,FileRef> file_map; ///< ino -> File

  // map of dirty files, files of same dirty_seq are grouped into list.
  std::map<uint64_t, dirty_file_list_t> dirty_files;

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
  std::vector<BlockDevice*> bdev;                  ///< block devices we can use
  std::vector<IOContext*> ioc;                     ///< IOContexts for bdevs
  std::vector<interval_set<uint64_t> > block_all;  ///< extents in bdev we own
  std::vector<Allocator*> alloc;                   ///< allocators for bdevs
  std::vector<uint64_t> alloc_size;                ///< alloc size for each device
  std::vector<interval_set<uint64_t>> pending_release; ///< extents to release
  std::vector<interval_set<uint64_t>> block_unused_too_granular;

  BlockDevice::aio_callback_t discard_cb[3]; //discard callbacks for each dev

  BlueFSDeviceExpander* slow_dev_expander = nullptr;
  std::unique_ptr<BlueFSVolumeSelector> vselector;

  class SocketHook;
  SocketHook* asok_hook = nullptr;

  void _init_logger();
  void _shutdown_logger();
  void _update_logger_stats();

  void _init_alloc();
  void _stop_alloc();

  void _pad_bl(ceph::buffer::list& bl);  ///< pad ceph::buffer::list to block size w/ zeros

  FileRef _get_file(uint64_t ino);
  void _drop_link(FileRef f);

  unsigned _get_slow_device_id() {
    return bdev[BDEV_SLOW] ? BDEV_SLOW : BDEV_DB;
  }
  const char* get_device_name(unsigned id);
  int _expand_slow_device(uint64_t min_size, PExtentVector& extents);
  int _allocate(uint8_t bdev, uint64_t len,
		bluefs_fnode_t* node);
  int _allocate_without_fallback(uint8_t id, uint64_t len,
				 PExtentVector* extents);

  int _flush_range(FileWriter *h, uint64_t offset, uint64_t length);
  int _flush(FileWriter *h, bool force);
  int _fsync(FileWriter *h, std::unique_lock<ceph::mutex>& l);

#ifdef HAVE_LIBAIO
  void _claim_completed_aios(FileWriter *h, std::list<aio_t> *ls);
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

  void _rewrite_log_and_layout_sync(bool allocate_with_fallback,
				    int super_dev,
				    int log_dev,
				    int new_log_dev,
				    int flags,
				    std::optional<bluefs_layout_t> layout);

  //void _aio_finish(void *priv);

  void _flush_bdev_safely(FileWriter *h);
  void flush_bdev();  // this is safe to call without a lock
  void flush_bdev(std::array<bool, MAX_BDEV>& dirty_bdevs);  // this is safe to call without a lock

  int _preallocate(FileRef f, uint64_t off, uint64_t len);
  int _truncate(FileWriter *h, uint64_t off);

  int64_t _read(
    FileReader *h,   ///< [in] read from here
    uint64_t offset, ///< [in] offset
    size_t len,      ///< [in] this many bytes
    ceph::buffer::list *outbl,   ///< [out] optional: reference the result here
    char *out);      ///< [out] optional: or copy it here
  int64_t _read_random(
    FileReader *h,   ///< [in] read from here
    uint64_t offset, ///< [in] offset
    uint64_t len,    ///< [in] this many bytes
    char *out);      ///< [out] optional: or copy it here

  void _invalidate_cache(FileRef f, uint64_t offset, uint64_t length);

  int _open_super();
  int _write_super(int dev);
  int _check_new_allocations(const bluefs_fnode_t& fnode,
    size_t dev_count,
    boost::dynamic_bitset<uint64_t>* owned_blocks,
    boost::dynamic_bitset<uint64_t>* used_blocks);
  int _verify_alloc_granularity(
    __u8 id, uint64_t offset, uint64_t length,
    const char *op);
  int _adjust_granularity(
    __u8 id, uint64_t *offset, uint64_t *length, bool alloc);
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

  void _add_block_extent(unsigned bdev, uint64_t offset, uint64_t len,
                         bool skip=false);

public:
  BlueFS(CephContext* cct);
  ~BlueFS();

  // the super is always stored on bdev 0
  int mkfs(uuid_d osd_uuid, const bluefs_layout_t& layout);
  int mount();
  int maybe_verify_layout(const bluefs_layout_t& layout) const;
  void umount(bool avoid_compact = false);
  int prepare_new_device(int id, const bluefs_layout_t& layout);
  
  int log_dump();

  void collect_metadata(std::map<std::string,std::string> *pm, unsigned skip_bdev_id);
  void get_devices(std::set<std::string> *ls);
  uint64_t get_alloc_size(int id) {
    return alloc_size[id];
  }
  int fsck();

  int device_migrate_to_new(
    CephContext *cct,
    const std::set<int>& devs_source,
    int dev_target,
    const bluefs_layout_t& layout);
  int device_migrate_to_existing(
    CephContext *cct,
    const std::set<int>& devs_source,
    int dev_target,
    const bluefs_layout_t& layout);

  uint64_t get_used();
  uint64_t get_total(unsigned id);
  uint64_t get_free(unsigned id);
  void get_usage(std::vector<std::pair<uint64_t,uint64_t>> *usage); // [<free,total> ...]
  void dump_perf_counters(ceph::Formatter *f);

  void dump_block_extents(std::ostream& out);

  /// get current extents that we own for given block device
  int get_block_extents(unsigned id, interval_set<uint64_t> *extents);

  int open_for_write(
    const std::string& dir,
    const std::string& file,
    FileWriter **h,
    bool overwrite);

  int open_for_read(
    const std::string& dir,
    const std::string& file,
    FileReader **h,
    bool random = false);

  void close_writer(FileWriter *h) {
    std::lock_guard l(lock);
    _close_writer(h);
  }

  int rename(const std::string& old_dir, const std::string& old_file,
	     const std::string& new_dir, const std::string& new_file);

  int readdir(const std::string& dirname, std::vector<std::string> *ls);

  int unlink(const std::string& dirname, const std::string& filename);
  int mkdir(const std::string& dirname);
  int rmdir(const std::string& dirname);
  bool wal_is_rotational();

  bool dir_exists(const std::string& dirname);
  int stat(const std::string& dirname, const std::string& filename,
	   uint64_t *size, utime_t *mtime);

  int lock_file(const std::string& dirname, const std::string& filename, FileLock **p);
  int unlock_file(FileLock *l);

  void compact_log();

  /// sync any uncommitted state to disk
  void sync_metadata(bool avoid_compact);

  void set_slow_device_expander(BlueFSDeviceExpander* a) {
    slow_dev_expander = a;
  }
  void set_volume_selector(BlueFSVolumeSelector* s) {
    vselector.reset(s);
  }
  void dump_volume_selector(std::ostream& sout) {
    vselector->dump(sout);
  }
  void get_vselector_paths(const std::string& base,
                           BlueFSVolumeSelector::paths& res) const {
    return vselector->get_paths(base, res);
  }

  int add_block_device(unsigned bdev, const std::string& path, bool trim,
		       bool shared_with_bluestore=false);
  bool bdev_support_label(unsigned id);
  uint64_t get_block_device_size(unsigned bdev);

  /// gift more block space
  void add_block_extent(unsigned bdev, uint64_t offset, uint64_t len,
                        bool skip=false) {
    std::unique_lock l(lock);
    _add_block_extent(bdev, offset, len, skip);
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
  int64_t read(FileReader *h, uint64_t offset, size_t len,
	   ceph::buffer::list *outbl, char *out) {
    // no need to hold the global lock here; we only touch h and
    // h->file, and read vs write or delete is already protected (via
    // atomics and asserts).
    return _read(h, offset, len, outbl, out);
  }
  int64_t read_random(FileReader *h, uint64_t offset, size_t len,
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

  /// test purpose methods
  void debug_inject_duplicate_gift(unsigned bdev, uint64_t offset, uint64_t len);
  const PerfCounters* get_perf_counters() const {
    return logger;
  }
};

class OriginalVolumeSelector : public BlueFSVolumeSelector {
  uint64_t wal_total;
  uint64_t db_total;
  uint64_t slow_total;

public:
  OriginalVolumeSelector(
    uint64_t _wal_total,
    uint64_t _db_total,
    uint64_t _slow_total)
    : wal_total(_wal_total), db_total(_db_total), slow_total(_slow_total) {}

  void* get_hint_for_log() const override;
  void* get_hint_by_dir(const std::string& dirname) const override;

  void add_usage(void* hint, const bluefs_fnode_t& fnode) override {
    // do nothing
    return;
  }
  void sub_usage(void* hint, const bluefs_fnode_t& fnode) override {
    // do nothing
    return;
  }
  void add_usage(void* hint, uint64_t fsize) override {
    // do nothing
    return;
  }
  void sub_usage(void* hint, uint64_t fsize) override {
    // do nothing
    return;
  }

  uint8_t select_prefer_bdev(void* hint) override;
  void get_paths(const std::string& base, paths& res) const override;
  void dump(std::ostream& sout) override;
};

#endif
