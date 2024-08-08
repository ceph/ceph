// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_BLUEFS_H
#define CEPH_OS_BLUESTORE_BLUEFS_H

#include <atomic>
#include <mutex>
#include <limits>

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
  l_bluefs_db_total_bytes,
  l_bluefs_db_used_bytes,
  l_bluefs_wal_total_bytes,
  l_bluefs_wal_used_bytes,
  l_bluefs_slow_total_bytes,
  l_bluefs_slow_used_bytes,
  l_bluefs_num_files,
  l_bluefs_log_bytes,
  l_bluefs_log_compactions,
  l_bluefs_log_write_count,
  l_bluefs_logged_bytes,
  l_bluefs_files_written_wal,
  l_bluefs_files_written_sst,
  l_bluefs_write_count_wal,
  l_bluefs_write_count_sst,
  l_bluefs_bytes_written_wal,
  l_bluefs_bytes_written_sst,
  l_bluefs_bytes_written_slow,
  l_bluefs_max_bytes_wal,
  l_bluefs_max_bytes_db,
  l_bluefs_max_bytes_slow,
  l_bluefs_main_alloc_unit,
  l_bluefs_db_alloc_unit,
  l_bluefs_wal_alloc_unit,
  l_bluefs_read_random_count,
  l_bluefs_read_random_bytes,
  l_bluefs_read_random_disk_count,
  l_bluefs_read_random_disk_bytes,
  l_bluefs_read_random_disk_bytes_wal,
  l_bluefs_read_random_disk_bytes_db,
  l_bluefs_read_random_disk_bytes_slow,
  l_bluefs_read_random_buffer_count,
  l_bluefs_read_random_buffer_bytes,
  l_bluefs_read_count,
  l_bluefs_read_bytes,
  l_bluefs_read_disk_count,
  l_bluefs_read_disk_bytes,
  l_bluefs_read_disk_bytes_wal,
  l_bluefs_read_disk_bytes_db,
  l_bluefs_read_disk_bytes_slow,
  l_bluefs_read_prefetch_count,
  l_bluefs_read_prefetch_bytes,
  l_bluefs_write_count,
  l_bluefs_write_disk_count,
  l_bluefs_write_bytes,
  l_bluefs_compaction_lat,
  l_bluefs_compaction_lock_lat,
  l_bluefs_alloc_shared_dev_fallbacks,
  l_bluefs_alloc_shared_size_fallbacks,
  l_bluefs_read_zeros_candidate,
  l_bluefs_read_zeros_errors,
  l_bluefs_last,
};

class BlueFSVolumeSelector {
public:
  typedef std::vector<std::pair<std::string, uint64_t>> paths;

  virtual ~BlueFSVolumeSelector() {
  }
  /**
  *  Method to learn a hint (aka logic level discriminator)  specific for
  *  BlueFS log
  *
  */
  virtual void* get_hint_for_log() const = 0;
  /**
  *  Method to learn a hint (aka logic level discriminator) provided directory
  *  bound to.
  *
  */
  virtual void* get_hint_by_dir(std::string_view dirname) const = 0;

  /**
  *  Increments stats for a given logical level using provided fnode as a delta,
  *  Parameters:
  *    hint: logical level discriminator
  *    fnode: fnode metadata to be used as a complex delta value:
  *           (+1 file count, +file size, +all the extents)
  *
  */
  void add_usage(void* hint, const bluefs_fnode_t& fnode) {
    for (auto& e : fnode.extents) {
      add_usage(hint, e);
    }
    add_usage(hint, fnode.size, true);
  }
  /**
  *  Decrements stats for a given logical level using provided fnode as a delta
  *  Parameters:
  *    hint: logical level discriminator
  *    fnode: fnode metadata to be used as a complex delta value:
  *           (-1 file count, -file size, -all the extents)
  *
  */
  void sub_usage(void* hint, const bluefs_fnode_t& fnode) {
    for (auto& e : fnode.extents) {
      sub_usage(hint, e);
    }
    sub_usage(hint, fnode.size, true);
  }
  /**
  *  Increments stats for a given logical level using provided extent as a delta,
  *  Parameters:
  *    hint: logical level discriminator
  *    extent: bluefs extent to be used as a complex delta value:
  *           (.bdev determines physical location, +length)
  *
  */
  virtual void add_usage(void* hint, const bluefs_extent_t& extent) = 0;
  /**
  *  Decrements stats for a given logical level using provided extent as a delta,
  *  Parameters:
  *    hint: logical level discriminator
  *    extent: bluefs extent to be used as a complex delta value:
  *           (.bdev determines physical location, -length)
  *
  */
  virtual void sub_usage(void* hint, const bluefs_extent_t& extent) = 0;
  /**
  *  Increments files count and overall files size for a given logical level
  *  Parameters:
  *    hint: logical level discriminator
  *    fsize: delta value for file size
  *    upd_files: whether or not to increment file count
  *
  */
  virtual void add_usage(void* hint, uint64_t fsize, bool upd_files = false) = 0;
  /**
  *  Decrements files count and overall files size for a given logical level
  *  Parameters:
  *    hint: logical level discriminator
  *    fsize: delta value for file size
  *    upd_files: whether or not to decrement file count
  *
  */
  virtual void sub_usage(void* hint, uint64_t fsize, bool upd_files = false) = 0;

  /**
  *  Determines preferred physical device for the given logical level
  *  Parameters:
  *    hint: logical level discriminator
  *
  */
  virtual uint8_t select_prefer_bdev(void* hint) = 0;
  /**
  *  Builds path set for RocksDB to use
  *  Parameters:
  *    base: path's root
  *
  */
  virtual void get_paths(const std::string& base, paths& res) const = 0;
  /**
  *  Dumps VSelector's state
  *
  */
  virtual void dump(std::ostream& sout) = 0;

  /* used for sanity checking of vselector */
  virtual BlueFSVolumeSelector* clone_empty() const { return nullptr; }
  virtual bool compare(BlueFSVolumeSelector* other) { return true; };
};

struct bluefs_shared_alloc_context_t {
  bool need_init = false;
  Allocator* a = nullptr;
  uint64_t alloc_unit = 0;

  std::atomic<uint64_t> bluefs_used = 0;

  void set(Allocator* _a, uint64_t _au) {
    a = _a;
    alloc_unit = _au;
    need_init = true;
    bluefs_used = 0;
  }
  void reset() {
    a = nullptr;
    alloc_unit = 0;
  }
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
    bool is_dirty;
    boost::intrusive::list_member_hook<> dirty_item;

    std::atomic_int num_readers, num_writers;
    std::atomic_int num_reading;

    void* vselector_hint = nullptr;
    /* lock protects fnode and other the parts that can be modified during read & write operations.
       Does not protect values that are fixed
       Does not need to be taken when doing one-time operations:
       _replay, device_migrate_to_existing, device_migrate_to_new */
    ceph::mutex lock = ceph::make_mutex("BlueFS::File::lock");

  private:
    FRIEND_MAKE_REF(File);
    File()
      :
	refs(0),
	dirty_seq(0),
	locked(false),
	deleted(false),
	is_dirty(false),
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

    mempool::bluefs::map<std::string, FileRef, std::less<>> file_map;

  private:
    FRIEND_MAKE_REF(Dir);
    Dir() = default;
  };
  using DirRef = ceph::ref_t<Dir>;

  struct FileWriter {
    MEMPOOL_CLASS_HELPERS();

    FileRef file;
    uint64_t pos = 0;       ///< start offset for buffer
  private:
    ceph::buffer::list buffer;      ///< new data to write (at end of file)
    ceph::buffer::list tail_block;  ///< existing partial block at end of file, if any
  public:
    unsigned get_buffer_length() const {
      return buffer.length();
    }
    ceph::bufferlist flush_buffer(
      CephContext* cct,
      const bool partial,
      const unsigned length,
      const bluefs_super_t& super);
    ceph::buffer::list::page_aligned_appender buffer_appender;  //< for const char* only
  public:
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
    // to use buffer_appender exclusively here (e.g., its notion of
    // offset will remain accurate).
    void append(const char *buf, size_t len) {
      uint64_t l0 = get_buffer_length();
      ceph_assert(l0 + len <= std::numeric_limits<unsigned>::max());
      buffer_appender.append(buf, len);
    }

    void append(const std::byte *buf, size_t len) {
      // allow callers to use byte type instead of char* as we simply pass byte array
      append((const char*)buf, len);
    }

    // note: used internally only, for ino 1 or 0.
    void append(ceph::buffer::list& bl) {
      uint64_t l0 = get_buffer_length();
      ceph_assert(l0 + bl.length() <= std::numeric_limits<unsigned>::max());
      buffer.claim_append(bl);
    }

    void append_zero(size_t len) {
      uint64_t l0 = get_buffer_length();
      ceph_assert(l0 + len <= std::numeric_limits<unsigned>::max());
      buffer_appender.append_zero(len);
    }

    uint64_t get_effective_write_pos() {
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
  PerfCounters *logger = nullptr;

  uint64_t max_bytes[MAX_BDEV] = {0};
  uint64_t max_bytes_pcounters[MAX_BDEV] = {
    l_bluefs_max_bytes_wal,
    l_bluefs_max_bytes_db,
    l_bluefs_max_bytes_slow,
    l_bluefs_max_bytes_wal,
    l_bluefs_max_bytes_db,
  };

  // cache
  struct {
    ceph::mutex lock = ceph::make_mutex("BlueFS::nodes.lock");
    mempool::bluefs::map<std::string, DirRef, std::less<>> dir_map; ///< dirname -> Dir
    mempool::bluefs::unordered_map<uint64_t, FileRef> file_map;     ///< ino -> File
  } nodes;

  bluefs_super_t super;        ///< latest superblock (as last written)
  uint64_t ino_last = 0;       ///< last assigned ino (this one is in use)

  struct {
    ceph::mutex lock = ceph::make_mutex("BlueFS::log.lock");
    uint64_t seq_live = 1;   //seq that log is currently writing to; mirrors dirty.seq_live
    FileWriter *writer = 0;
    bluefs_transaction_t t;
  } log;

  struct {
    ceph::mutex lock = ceph::make_mutex("BlueFS::dirty.lock");
    uint64_t seq_stable = 0; //seq that is now stable on disk
    uint64_t seq_live = 1;   //seq that is ongoing and dirty files will be written to
    // map of dirty files, files of same dirty_seq are grouped into list.
    std::map<uint64_t, dirty_file_list_t> files;
    std::vector<interval_set<uint64_t>> pending_release; ///< extents to release
    // TODO: it should be examined what makes pending_release immune to
    // eras in a way similar to dirty_files. Hints:
    // 1) we have actually only 2 eras: log_seq and log_seq+1
    // 2) we usually not remove extents from files. And when we do, we force log-syncing.
  } dirty;

  ceph::condition_variable log_cond;                             ///< used for state control between log flush / log compaction
  std::atomic<bool> log_is_compacting{false};                    ///< signals that bluefs log is already ongoing compaction
  std::atomic<bool> log_forbidden_to_expand{false};              ///< used to signal that async compaction is in state
                                                                 ///  that prohibits expansion of bluefs log
  /*
   * There are up to 3 block devices:
   *
   *  BDEV_DB   db/      - the primary db device
   *  BDEV_WAL  db.wal/  - a small, fast device, specifically for the WAL
   *  BDEV_SLOW db.slow/ - a big, slow device, to spill over to as BDEV_DB fills
   */
  std::vector<BlockDevice*> bdev;                  ///< block devices we can use
  std::vector<IOContext*> ioc;                     ///< IOContexts for bdevs
  std::vector<uint64_t> block_reserved;            ///< starting reserve extent per device
  std::vector<Allocator*> alloc;                   ///< allocators for bdevs
  std::vector<uint64_t> alloc_size;                ///< alloc size for each device

  //std::vector<interval_set<uint64_t>> block_unused_too_granular;

  BlockDevice::aio_callback_t discard_cb[3]; //discard callbacks for each dev

  std::unique_ptr<BlueFSVolumeSelector> vselector;

  bluefs_shared_alloc_context_t* shared_alloc = nullptr;
  unsigned shared_alloc_id = unsigned(-1);
  inline bool is_shared_alloc(unsigned id) const {
    return id == shared_alloc_id;
  }
  std::atomic<int64_t> cooldown_deadline = 0;

  class SocketHook;
  SocketHook* asok_hook = nullptr;
  // used to trigger zeros into read (debug / verify)
  std::atomic<uint64_t> inject_read_zeros{0};

  void _init_logger();
  void _shutdown_logger();
  void _update_logger_stats();

  void _init_alloc();
  void _stop_alloc();

  ///< pad ceph::buffer::list to max(block size, pad_size) w/ zeros
  void _pad_bl(ceph::buffer::list& bl, uint64_t pad_size = 0);

  uint64_t _get_used(unsigned id) const;
  uint64_t _get_total(unsigned id) const;


  FileRef _get_file(uint64_t ino);
  void _drop_link_D(FileRef f);

  unsigned _get_slow_device_id() {
    return bdev[BDEV_SLOW] ? BDEV_SLOW : BDEV_DB;
  }
  const char* get_device_name(unsigned id);

  typedef std::function<void(const bluefs_extent_t)> update_fn_t;
  int _allocate(uint8_t bdev, uint64_t len,
                uint64_t alloc_unit,
		bluefs_fnode_t* node,
                update_fn_t cb = nullptr,
                size_t alloc_attempts = 0,
                bool permit_dev_fallback = true);

  /* signal replay log to include h->file in nearest log flush */
  int _signal_dirty_to_log_D(FileWriter *h);
  int _flush_range_F(FileWriter *h, uint64_t offset, uint64_t length);
  int _flush_data(FileWriter *h, uint64_t offset, uint64_t length, bool buffered);
  int _flush_F(FileWriter *h, bool force, bool *flushed = nullptr);
  uint64_t _flush_special(FileWriter *h);
  int _fsync(FileWriter *h);

#ifdef HAVE_LIBAIO
  void _claim_completed_aios(FileWriter *h, std::list<aio_t> *ls);
  void _wait_for_aio(FileWriter *h);  // safe to call without a lock
#endif

  int64_t _maybe_extend_log();
  void _extend_log();
  uint64_t _log_advance_seq();
  void _consume_dirty(uint64_t seq);
  void _clear_dirty_set_stable_D(uint64_t seq_stable);
  void _release_pending_allocations(std::vector<interval_set<uint64_t>>& to_release);

  void _flush_and_sync_log_core(int64_t available_runway);
  int _flush_and_sync_log_jump_D(uint64_t jump_to,
			       int64_t available_runway);
  int _flush_and_sync_log_LD(uint64_t want_seq = 0);

  uint64_t _estimate_transaction_size(bluefs_transaction_t* t);
  uint64_t _make_initial_transaction(uint64_t start_seq,
                                     bluefs_fnode_t& fnode,
                                     uint64_t expected_final_size,
                                     bufferlist* out);
  uint64_t _estimate_log_size_N();
  bool _should_start_compact_log_L_N();

  enum {
    REMOVE_DB = 1,
    REMOVE_WAL = 2,
    RENAME_SLOW2DB = 4,
    RENAME_DB2SLOW = 8,
  };
  void _compact_log_dump_metadata_NF(uint64_t start_seq,
                                     bluefs_transaction_t *t,
				     int flags,
				     uint64_t capture_before_seq);

  void _compact_log_sync_LNF_LD();
  void _compact_log_async_LD_LNF_D();

  void _rewrite_log_and_layout_sync_LNF_LD(bool permit_dev_fallback,
				    int super_dev,
				    int log_dev,
				    int new_log_dev,
				    int flags,
				    std::optional<bluefs_layout_t> layout);

  //void _aio_finish(void *priv);

  void _flush_bdev(FileWriter *h, bool check_mutex_locked = true);
  void _flush_bdev();  // this is safe to call without a lock
  void _flush_bdev(std::array<bool, MAX_BDEV>& dirty_bdevs);  // this is safe to call without a lock

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

  int _open_super();
  int _write_super(int dev);
  int _check_allocations(const bluefs_fnode_t& fnode,
    boost::dynamic_bitset<uint64_t>* used_blocks,
    bool is_alloc, //true when allocating, false when deallocating
    const char* op_name);
  int _verify_alloc_granularity(
    __u8 id, uint64_t offset, uint64_t length,
    uint64_t alloc_unit,
    const char *op);
  int _replay(bool noop, bool to_stdout = false); ///< replay journal

  FileWriter *_create_writer(FileRef f);
  void _drain_writer(FileWriter *h);
  void _close_writer(FileWriter *h);

  // always put the super in the second 4k block.  FIXME should this be
  // block size independent?
  unsigned get_super_offset() {
    return 4096;
  }
  unsigned get_super_length() {
    return 4096;
  }
  void _maybe_check_vselector_LNF() {
    if (cct->_conf->bluefs_check_volume_selector_often) {
      _check_vselector_LNF();
    }
  }
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
  uint64_t get_used(unsigned id);
  void dump_perf_counters(ceph::Formatter *f);

  void dump_block_extents(std::ostream& out);

  /// get current extents that we own for given block device
  void foreach_block_extents(
    unsigned id,
    std::function<void(uint64_t, uint32_t)> cb);

  int open_for_write(
    std::string_view dir,
    std::string_view file,
    FileWriter **h,
    bool overwrite);

  int open_for_read(
    std::string_view dir,
    std::string_view file,
    FileReader **h,
    bool random = false);

  // data added after last fsync() is lost
  void close_writer(FileWriter *h);

  int rename(std::string_view old_dir, std::string_view old_file,
	     std::string_view new_dir, std::string_view new_file);

  int readdir(std::string_view dirname, std::vector<std::string> *ls);

  int unlink(std::string_view dirname, std::string_view filename);
  int mkdir(std::string_view dirname);
  int rmdir(std::string_view dirname);
  bool wal_is_rotational();
  bool db_is_rotational();

  bool dir_exists(std::string_view dirname);
  int stat(std::string_view dirname, std::string_view filename,
	   uint64_t *size, utime_t *mtime);

  int lock_file(std::string_view dirname, std::string_view filename, FileLock **p);
  int unlock_file(FileLock *l);

  void compact_log();

  /// sync any uncommitted state to disk
  void sync_metadata(bool avoid_compact);

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
                       uint64_t reserved,
		       bluefs_shared_alloc_context_t* _shared_alloc = nullptr);
  bool bdev_support_label(unsigned id);
  uint64_t get_block_device_size(unsigned bdev) const;

  // handler for discard event
  void handle_discard(unsigned dev, interval_set<uint64_t>& to_release);

  void flush(FileWriter *h, bool force = false);

  void append_try_flush(FileWriter *h, const char* buf, size_t len);
  void flush_range(FileWriter *h, uint64_t offset, uint64_t length);
  int fsync(FileWriter *h);
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
  void invalidate_cache(FileRef f, uint64_t offset, uint64_t len);
  int preallocate(FileRef f, uint64_t offset, uint64_t len);
  int truncate(FileWriter *h, uint64_t offset);

  size_t probe_alloc_avail(int dev, uint64_t alloc_size);

  /// test purpose methods
  const PerfCounters* get_perf_counters() const {
    return logger;
  }
  uint64_t debug_get_dirty_seq(FileWriter *h);
  bool debug_get_is_dev_dirty(FileWriter *h, uint8_t dev);

private:
  // Wrappers for BlockDevice::read(...) and BlockDevice::read_random(...)
  // They are used for checking if read values are all 0, and reread if so.
  int _read_and_check(uint8_t ndev, uint64_t off, uint64_t len,
	   ceph::buffer::list *pbl, IOContext *ioc, bool buffered);
  int _read_random_and_check(uint8_t ndev, uint64_t off, uint64_t len, char *buf, bool buffered);

  int _bdev_read(uint8_t ndev, uint64_t off, uint64_t len,
    ceph::buffer::list* pbl, IOContext* ioc, bool buffered);
  int _bdev_read_random(uint8_t ndev, uint64_t off, uint64_t len, char* buf, bool buffered);

  /// test and compact log, if necessary
  void _maybe_compact_log_LNF_NF_LD_D();
  int _do_replay_recovery_read(FileReader *log,
			       size_t log_pos,
			       size_t read_offset,
			       size_t read_len,
			       bufferlist* bl);
  void _check_vselector_LNF();
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
  void* get_hint_by_dir(std::string_view dirname) const override;

  void add_usage(void* hint, const bluefs_extent_t& extent) override {
    // do nothing
    return;
  }
  void sub_usage(void* hint, const bluefs_extent_t& extent) override {
    // do nothing
    return;
  }
  void add_usage(void*, uint64_t, bool) override {
    // do nothing
    return;
  }
  void sub_usage(void*, uint64_t, bool) override {
    // do nothing
    return;
  }

  uint8_t select_prefer_bdev(void* hint) override;
  void get_paths(const std::string& base, paths& res) const override;
  void dump(std::ostream& sout) override;
};

class FitToFastVolumeSelector : public OriginalVolumeSelector {
public:
  FitToFastVolumeSelector(
    uint64_t _wal_total,
    uint64_t _db_total,
    uint64_t _slow_total)
    : OriginalVolumeSelector(_wal_total, _db_total, _slow_total) {}

  void get_paths(const std::string& base, paths& res) const override;
};
/**
 * Directional graph of locks.
 * Vertices - Locks. Edges (directed) - locking progression.
 * Edge A->B exist if last taken lock was A and next taken lock is B.
 * 
 * Row represents last lock taken.
 * Column represents next lock taken.
 *
 *     >        | W | L | N | D | F
 * -------------|---|---|---|---|---
 * FileWriter W |   | > | > | > | >
 * log        L |       | > | > | >
 * nodes      N |           | > | >
 * dirty      D |           |   | >
 * File       F |
 * 
 * Claim: Deadlock is possible IFF graph contains cycles.
 */
#endif
