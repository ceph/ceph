// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifdef WITH_SEASTAR
#include "crimson/os/alienstore/alien_store.h"
#endif

#include "BlueRocksEnv.h"
#include "BlueFS.h"
#include "include/stringify.h"
#include "kv/RocksDBStore.h"
#include "string.h"

rocksdb::Status err_to_status(int r)
{
  switch (r) {
  case 0:
    return rocksdb::Status::OK();
  case -ENOENT:
    return rocksdb::Status::NotFound(rocksdb::Status::kNone);
  case -EINVAL:
    return rocksdb::Status::InvalidArgument(rocksdb::Status::kNone);
  case -EIO:
  case -EEXIST:
    return rocksdb::Status::IOError(rocksdb::Status::kNone);
  case -ENOLCK:
    return rocksdb::Status::IOError(strerror(r));
  default:
    // FIXME :(
    ceph_abort_msg("unrecognized error code");
    return rocksdb::Status::NotSupported(rocksdb::Status::kNone);
  }
}

// A file abstraction for reading sequentially through a file
class BlueRocksSequentialFile : public rocksdb::SequentialFile {
  BlueFS *fs;
  BlueFS::FileReader *h;
 public:
  BlueRocksSequentialFile(BlueFS *fs, BlueFS::FileReader *h) : fs(fs), h(h) {}
  ~BlueRocksSequentialFile() override {
    delete h;
  }

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  rocksdb::Status Read(size_t n, rocksdb::Slice* result, char* scratch) override {
    int64_t r = fs->read(h, h->buf.pos, n, NULL, scratch);
    ceph_assert(r >= 0);
    *result = rocksdb::Slice(scratch, r);
    return rocksdb::Status::OK();
  }

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  rocksdb::Status Skip(uint64_t n) override {
    h->buf.skip(n);
    return rocksdb::Status::OK();
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
    h->buf.invalidate_cache(offset, length);
    fs->invalidate_cache(h->file, offset, length);
    return rocksdb::Status::OK();
  }
};

// A file abstraction for randomly reading the contents of a file.
class BlueRocksRandomAccessFile : public rocksdb::RandomAccessFile {
  BlueFS *fs;
  BlueFS::FileReader *h;
 public:
  BlueRocksRandomAccessFile(BlueFS *fs, BlueFS::FileReader *h) : fs(fs), h(h) {}
  ~BlueRocksRandomAccessFile() override {
    delete h;
  }

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  rocksdb::Status Read(uint64_t offset, size_t n, rocksdb::Slice* result,
		       char* scratch) const override {
    int64_t r = fs->read_random(h, offset, n, scratch);
    ceph_assert(r >= 0);
    *result = rocksdb::Slice(scratch, r);
    return rocksdb::Status::OK();
  }

  // Tries to get an unique ID for this file that will be the same each time
  // the file is opened (and will stay the same while the file is open).
  // Furthermore, it tries to make this ID at most "max_size" bytes. If such an
  // ID can be created this function returns the length of the ID and places it
  // in "id"; otherwise, this function returns 0, in which case "id"
  // may not have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to eachother by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return snprintf(id, max_size, "%016llx",
		    (unsigned long long)h->file->fnode.ino);
  };

  // Readahead the file starting from offset by n bytes for caching.
  rocksdb::Status Prefetch(uint64_t offset, size_t n) override {
    fs->read(h, offset, n, nullptr, nullptr);
    return rocksdb::Status::OK();
  }

  //enum AccessPattern { NORMAL, RANDOM, SEQUENTIAL, WILLNEED, DONTNEED };

  void Hint(AccessPattern pattern) override {
    if (pattern == RANDOM)
      h->buf.max_prefetch = 4096;
    else if (pattern == SEQUENTIAL)
      h->buf.max_prefetch = fs->cct->_conf->bluefs_max_prefetch;
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
    h->buf.invalidate_cache(offset, length);
    fs->invalidate_cache(h->file, offset, length);
    return rocksdb::Status::OK();
  }
};


// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class BlueRocksWritableFile : public rocksdb::WritableFile {
  BlueFS *fs;
  BlueFS::FileWriter *h;
 public:
  BlueRocksWritableFile(BlueFS *fs, BlueFS::FileWriter *h) : fs(fs), h(h) {}
  ~BlueRocksWritableFile() override {
    fs->close_writer(h);
  }

  // Indicates if the class makes use of unbuffered I/O
  /*bool UseOSBuffer() const {
    return true;
    }*/

  // This is needed when you want to allocate
  // AlignedBuffer for use with file I/O classes
  // Used for unbuffered file I/O when UseOSBuffer() returns false
  /*size_t GetRequiredBufferAlignment() const {
    return c_DefaultPageSize;
    }*/

  rocksdb::Status Append(const rocksdb::Slice& data) override {
    h->append(data.data(), data.size());
    return rocksdb::Status::OK();
  }

  // Positioned write for unbuffered access default forward
  // to simple append as most of the tests are buffered by default
  rocksdb::Status PositionedAppend(
    const rocksdb::Slice& /* data */,
    uint64_t /* offset */) override {
    return rocksdb::Status::NotSupported();
  }

  // Truncate is necessary to trim the file to the correct size
  // before closing. It is not always possible to keep track of the file
  // size due to whole pages writes. The behavior is undefined if called
  // with other writes to follow.
  rocksdb::Status Truncate(uint64_t size) override {
    // we mirror the posix env, which does nothing here; instead, it
    // truncates to the final size on close.  whatever!
    return rocksdb::Status::OK();
    //int r = fs->truncate(h, size);
    //  return err_to_status(r);
  }

  rocksdb::Status Close() override {
    fs->flush(h, true);

    // mimic posix env, here.  shrug.
    size_t block_size;
    size_t last_allocated_block;
    GetPreallocationStatus(&block_size, &last_allocated_block);
    if (last_allocated_block > 0) {
      int r = fs->truncate(h, h->pos);
      if (r < 0)
	return err_to_status(r);
    }

    return rocksdb::Status::OK();
  }

  rocksdb::Status Flush() override {
    fs->flush(h);
    return rocksdb::Status::OK();
  }

  rocksdb::Status Sync() override { // sync data
    fs->fsync(h);
    return rocksdb::Status::OK();
  }

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  bool IsSyncThreadSafe() const override {
    return true;
  }

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  bool UseDirectIO() const {
    return false;
  }

  void SetWriteLifeTimeHint(rocksdb::Env::WriteLifeTimeHint hint) override {
    h->write_hint = (const int)hint;
  }

  /*
   * Get the size of valid data in the file.
   */
  uint64_t GetFileSize() override {
    return h->file->fnode.size + h->buffer.length();;
  }

  // For documentation, refer to RandomAccessFile::GetUniqueId()
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return snprintf(id, max_size, "%016llx",
		    (unsigned long long)h->file->fnode.ino);
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
    fs->fsync(h);
    fs->invalidate_cache(h->file, offset, length);
    return rocksdb::Status::OK();
  }

  using rocksdb::WritableFile::RangeSync;
  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  rocksdb::Status RangeSync(off_t offset, off_t nbytes) {
    // round down to page boundaries
    int partial = offset & 4095;
    offset -= partial;
    nbytes += partial;
    nbytes &= ~4095;
    if (nbytes)
      fs->flush_range(h, offset, nbytes);
    return rocksdb::Status::OK();
  }

 protected:
  using rocksdb::WritableFile::Allocate;
  /*
   * Pre-allocate space for a file.
   */
  rocksdb::Status Allocate(off_t offset, off_t len) {
    int r = fs->preallocate(h->file, offset, len);
    return err_to_status(r);
  }
};


// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class BlueRocksDirectory : public rocksdb::Directory {
  BlueFS *fs;
 public:
  explicit BlueRocksDirectory(BlueFS *f) : fs(f) {}

  // Fsync directory. Can be called concurrently from multiple threads.
  rocksdb::Status Fsync() override {
    // it is sufficient to flush the log.
    fs->sync_metadata(false);
    return rocksdb::Status::OK();
  }
};

// Identifies a locked file.
class BlueRocksFileLock : public rocksdb::FileLock {
 public:
  BlueFS *fs;
  BlueFS::FileLock *lock;
  BlueRocksFileLock(BlueFS *fs, BlueFS::FileLock *l) : fs(fs), lock(l) { }
  ~BlueRocksFileLock() override {
  }
};


// --------------------
// --- BlueRocksEnv ---
// --------------------

BlueRocksEnv::BlueRocksEnv(BlueFS *f)
  : EnvWrapper(Env::Default()),  // forward most of it to POSIX
    fs(f)
{

}

rocksdb::Status BlueRocksEnv::NewSequentialFile(
  const std::string& fname,
  std::unique_ptr<rocksdb::SequentialFile>* result,
  const rocksdb::EnvOptions& options)
{
  if (fname[0] == '/')
    return target()->NewSequentialFile(fname, result, options);
  std::string dir, file;
  split(fname, &dir, &file);
  BlueFS::FileReader *h;
  int r = fs->open_for_read(dir, file, &h, false);
  if (r < 0)
    return err_to_status(r);
  result->reset(new BlueRocksSequentialFile(fs, h));
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::NewRandomAccessFile(
  const std::string& fname,
  std::unique_ptr<rocksdb::RandomAccessFile>* result,
  const rocksdb::EnvOptions& options)
{
  std::string dir, file;
  split(fname, &dir, &file);
  BlueFS::FileReader *h;
  int r = fs->open_for_read(dir, file, &h, true);
  if (r < 0)
    return err_to_status(r);
  result->reset(new BlueRocksRandomAccessFile(fs, h));
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::NewWritableFile(
  const std::string& fname,
  std::unique_ptr<rocksdb::WritableFile>* result,
  const rocksdb::EnvOptions& options)
{
  std::string dir, file;
  split(fname, &dir, &file);
  BlueFS::FileWriter *h;
  int r = fs->open_for_write(dir, file, &h, false);
  if (r < 0)
    return err_to_status(r);
  result->reset(new BlueRocksWritableFile(fs, h));
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::ReuseWritableFile(
  const std::string& new_fname,
  const std::string& old_fname,
  std::unique_ptr<rocksdb::WritableFile>* result,
  const rocksdb::EnvOptions& options)
{
  std::string old_dir, old_file;
  split(old_fname, &old_dir, &old_file);
  std::string new_dir, new_file;
  split(new_fname, &new_dir, &new_file);

  int r = fs->rename(old_dir, old_file, new_dir, new_file);
  if (r < 0)
    return err_to_status(r);

  BlueFS::FileWriter *h;
  r = fs->open_for_write(new_dir, new_file, &h, true);
  if (r < 0)
    return err_to_status(r);
  result->reset(new BlueRocksWritableFile(fs, h));
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::NewDirectory(
  const std::string& name,
  std::unique_ptr<rocksdb::Directory>* result)
{
  if (!fs->dir_exists(name))
    return rocksdb::Status::NotFound(name, strerror(ENOENT));
  result->reset(new BlueRocksDirectory(fs));
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::FileExists(const std::string& fname)
{
  if (fname[0] == '/')
    return target()->FileExists(fname);
  std::string dir, file;
  split(fname, &dir, &file);
  if (fs->stat(dir, file, NULL, NULL) == 0)
    return rocksdb::Status::OK();
  return err_to_status(-ENOENT);
}

rocksdb::Status BlueRocksEnv::GetChildren(
  const std::string& dir,
  std::vector<std::string>* result)
{
  result->clear();
  int r = fs->readdir(dir, result);
  if (r < 0)
    return rocksdb::Status::NotFound(dir, strerror(ENOENT));//    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::DeleteFile(const std::string& fname)
{
  std::string dir, file;
  split(fname, &dir, &file);
  int r = fs->unlink(dir, file);
  if (r < 0)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::CreateDir(const std::string& dirname)
{
  int r = fs->mkdir(dirname);
  if (r < 0)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::CreateDirIfMissing(const std::string& dirname)
{
  int r = fs->mkdir(dirname);
  if (r < 0 && r != -EEXIST)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::DeleteDir(const std::string& dirname)
{
  int r = fs->rmdir(dirname);
  if (r < 0)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::GetFileSize(
  const std::string& fname,
  uint64_t* file_size)
{
  std::string dir, file;
  split(fname, &dir, &file);
  int r = fs->stat(dir, file, file_size, NULL);
  if (r < 0)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::GetFileModificationTime(const std::string& fname,
						      uint64_t* file_mtime)
{
  std::string dir, file;
  split(fname, &dir, &file);
  utime_t mtime;
  int r = fs->stat(dir, file, NULL, &mtime);
  if (r < 0)
    return err_to_status(r);
  *file_mtime = mtime.sec();
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::RenameFile(
  const std::string& src,
  const std::string& target)
{
  std::string old_dir, old_file;
  split(src, &old_dir, &old_file);
  std::string new_dir, new_file;
  split(target, &new_dir, &new_file);

  int r = fs->rename(old_dir, old_file, new_dir, new_file);
  if (r < 0)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::LinkFile(
  const std::string& src,
  const std::string& target)
{
  ceph_abort();
}

rocksdb::Status BlueRocksEnv::AreFilesSame(
  const std::string& first,
  const std::string& second, bool* res)
{
  for (auto& path : {first, second}) {
    if (fs->dir_exists(path)) {
      continue;
    }
    std::string dir, file;
    split(path, &dir, &file);
    int r = fs->stat(dir, file, nullptr, nullptr);
    if (!r) {
      continue;
    } else if (r == -ENOENT) {
      return rocksdb::Status::NotFound("AreFilesSame", path);
    } else {
      return err_to_status(r);
    }
  }
  *res = (first == second);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::LockFile(
  const std::string& fname,
  rocksdb::FileLock** lock)
{
  std::string dir, file;
  split(fname, &dir, &file);
  BlueFS::FileLock *l = NULL;
  int r = fs->lock_file(dir, file, &l);
  if (r < 0)
    return err_to_status(r);
  *lock = new BlueRocksFileLock(fs, l);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::UnlockFile(rocksdb::FileLock* lock)
{
  BlueRocksFileLock *l = static_cast<BlueRocksFileLock*>(lock);
  int r = fs->unlock_file(l->lock);
  if (r < 0)
    return err_to_status(r);
  delete lock;
  lock = nullptr;
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::GetAbsolutePath(
  const std::string& db_path,
  std::string* output_path)
{
  // this is a lie...
  *output_path = "/" + db_path;
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::NewLogger(
  const std::string& fname,
  std::shared_ptr<rocksdb::Logger>* result)
{
  // ignore the filename :)
  result->reset(create_rocksdb_ceph_logger());
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::GetTestDirectory(std::string* path)
{
  static int foo = 0;
  *path = "temp_" + stringify(++foo);
  return rocksdb::Status::OK();
}

void BlueRocksEnv::StartThread(void(*function)(void* arg), void* arg)
{
#ifdef WITH_SEASTAR
  crimson::os::AlienStore::configure_thread_memory();
#endif
  base_t::StartThread(function, arg);
}
