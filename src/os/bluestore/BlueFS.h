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
  l_bluefs_db_free_bytes,
  l_bluefs_wal_total_bytes,
  l_bluefs_wal_free_bytes,
  l_bluefs_slow_total_bytes,
  l_bluefs_slow_free_bytes,
  l_bluefs_num_files,
  l_bluefs_log_bytes,
  l_bluefs_log_compactions,
  l_bluefs_logged_bytes,
  l_bluefs_last,
};

class BlueFS {
public:
  static constexpr unsigned MAX_BDEV = 3;
  static constexpr unsigned BDEV_WAL = 0;
  static constexpr unsigned BDEV_DB = 1;
  static constexpr unsigned BDEV_SLOW = 2;

  struct File : public RefCountedObject {
    bluefs_fnode_t fnode;
    int refs;
    bool dirty;
    bool locked;
    bool deleted;
    boost::intrusive::list_member_hook<> dirty_item;

    std::atomic_int num_readers, num_writers;
    std::atomic_int num_reading;

    File()
      : RefCountedObject(NULL, 0),
	refs(0),
	dirty(false),
	locked(false),
	deleted(false),
	num_readers(0),
	num_writers(0),
	num_reading(0)
      {}
    ~File() {
      assert(num_readers.load() == 0);
      assert(num_writers.load() == 0);
      assert(num_reading.load() == 0);
      assert(!locked);
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
    map<string,FileRef> file_map;

    friend void intrusive_ptr_add_ref(Dir *d) {
      d->get();
    }
    friend void intrusive_ptr_release(Dir *d) {
      d->put();
    }
  };
  typedef boost::intrusive_ptr<Dir> DirRef;

  struct FileWriter {
    FileRef file;
    uint64_t pos;           ///< start offset for buffer
    bufferlist buffer;      ///< new data to write (at end of file)
    bufferlist tail_block;  ///< existing partial block at end of file, if any

    std::mutex lock;
    std::array<IOContext*,MAX_BDEV> iocv; ///< for each bdev

    FileWriter(FileRef f)
      : file(f),
	pos(0) {
      ++file->num_writers;
    }
    // NOTE: caller must call BlueFS::close_writer()
    ~FileWriter() {
      --file->num_writers;
    }
    void append(const char *buf, size_t len) {
      buffer.append(buf, len);
    }
    void append(bufferlist& bl) {
      buffer.claim_append(bl);
    }
    void append(bufferptr& bp) {
      buffer.append(bp);
    }
  };

  struct FileReaderBuffer {
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
    FileRef file;
    explicit FileLock(FileRef f) : file(f) {}
  };

private:
  std::mutex lock;

  PerfCounters *logger;

  // cache
  map<string, DirRef> dir_map;                    ///< dirname -> Dir
  ceph::unordered_map<uint64_t,FileRef> file_map; ///< ino -> File
  dirty_file_list_t dirty_files;                  ///< list of dirty files

  bluefs_super_t super;       ///< latest superblock (as last written)
  uint64_t ino_last;          ///< last assigned ino (this one is in use)
  uint64_t log_seq;           ///< last used log seq (by current pending log_t)
  FileWriter *log_writer;     ///< writer for the log
  bluefs_transaction_t log_t; ///< pending, unwritten log transaction

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
  vector<uint64_t> block_total;               ///< sum of block_all
  vector<Allocator*> alloc;                   ///< allocators for bdevs

  void _init_logger();
  void _shutdown_logger();
  void _update_logger_stats();

  void _init_alloc();
  void _stop_alloc();

  void _pad_bl(bufferlist& bl);  ///< pad bufferlist to block size w/ zeros

  FileRef _get_file(uint64_t ino);
  void _drop_link(FileRef f);

  int _allocate(unsigned bdev, uint64_t len, vector<bluefs_extent_t> *ev);
  int _flush_range(FileWriter *h, uint64_t offset, uint64_t length);
  int _flush(FileWriter *h, bool force);
  void _flush_wait(FileWriter *h);
  void _fsync(FileWriter *h);

  int _flush_log();
  uint64_t _estimate_log_size();
  void _maybe_compact_log();
  void _compact_log();

  //void _aio_finish(void *priv);

  void _flush_bdev();

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
  int _write_super();
  int _replay(); ///< replay journal

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

public:
  BlueFS();
  ~BlueFS();

  // the super is always stored on bdev 0
  int mkfs(uuid_d osd_uuid);
  int mount();
  void umount();

  int fsck();

  uint64_t get_total(unsigned id);
  uint64_t get_free(unsigned id);
  void get_usage(vector<pair<uint64_t,uint64_t>> *usage); // [<free,total> ...]

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
    std::lock_guard<std::mutex> l(lock);
    _close_writer(h);
  }

  int rename(const string& old_dir, const string& old_file,
	     const string& new_dir, const string& new_file);

  int readdir(const string& dirname, vector<string> *ls);

  int unlink(const string& dirname, const string& filename);
  int mkdir(const string& dirname);
  int rmdir(const string& dirname);

  bool dir_exists(const string& dirname);
  int stat(const string& dirname, const string& filename,
	   uint64_t *size, utime_t *mtime);

  int lock_file(const string& dirname, const string& filename, FileLock **p);
  int unlock_file(FileLock *l);

  /// sync any uncommitted state to disk
  int sync();

  void sync_metadata();

  /// compact metadata
  int compact();

  int add_block_device(unsigned bdev, string path);
  uint64_t get_block_device_size(unsigned bdev);

  /// gift more block space
  void add_block_extent(unsigned bdev, uint64_t offset, uint64_t len);

  /// reclaim block space
  int reclaim_blocks(unsigned bdev, uint64_t want,
		     uint64_t *offset, uint32_t *length);

  void flush(FileWriter *h) {
    std::lock_guard<std::mutex> l(lock);
    _flush(h, false);
  }
  void flush_range(FileWriter *h, uint64_t offset, uint64_t length) {
    std::lock_guard<std::mutex> l(lock);
    _flush_range(h, offset, length);
  }
  void fsync(FileWriter *h) {
    std::lock_guard<std::mutex> l(lock);
    _fsync(h);
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
    std::lock_guard<std::mutex> l(lock);
    _invalidate_cache(f, offset, len);
  }
  int preallocate(FileRef f, uint64_t offset, uint64_t len) {
    std::lock_guard<std::mutex> l(lock);
    return _preallocate(f, offset, len);
  }
  int truncate(FileWriter *h, uint64_t offset) {
    std::lock_guard<std::mutex> l(lock);
    return _truncate(h, offset);
  }

};

#endif
