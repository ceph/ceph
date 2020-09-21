// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_BLUEROCKSENV_H
#define CEPH_OS_BLUESTORE_BLUEROCKSENV_H

#include <memory>
#include <string>

#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/env_mirror.h"

#include "include/ceph_assert.h"
#include "kv/RocksDBStore.h"

class BlueFS;

class BlueRocksEnv : public rocksdb::EnvWrapper {
  using base_t = EnvWrapper;

  void split(const std::string &fn, std::string *dir, std::string *file) {
    size_t slash = fn.rfind('/');
    *file = fn.substr(slash + 1);
    while (slash && fn[slash-1] == '/')
      --slash;
    *dir = fn.substr(0, slash);
  }

public:
  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure, stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  rocksdb::Status NewSequentialFile(
    const std::string& fname,
    std::unique_ptr<rocksdb::SequentialFile>* result,
    const rocksdb::EnvOptions& options) override;

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure, stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  rocksdb::Status NewRandomAccessFile(
    const std::string& fname,
    std::unique_ptr<rocksdb::RandomAccessFile>* result,
    const rocksdb::EnvOptions& options) override;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure, stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  rocksdb::Status NewWritableFile(
    const std::string& fname,
    std::unique_ptr<rocksdb::WritableFile>* result,
    const rocksdb::EnvOptions& options) override;

  // Reuse an existing file by renaming it and opening it as writable.
  rocksdb::Status ReuseWritableFile(
    const std::string& fname,
    const std::string& old_fname,
    std::unique_ptr<rocksdb::WritableFile>* result,
    const rocksdb::EnvOptions& options) override;

  // Create an object that represents a directory. Will fail if directory
  // doesn't exist. If the directory exists, it will open the directory
  // and create a new Directory object.
  //
  // On success, stores a pointer to the new Directory in
  // *result and returns OK. On failure stores nullptr in *result and
  // returns non-OK.
  rocksdb::Status NewDirectory(
    const std::string& name,
    std::unique_ptr<rocksdb::Directory>* result) override;

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  rocksdb::Status FileExists(const std::string& fname) override;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  rocksdb::Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) override;

  // Delete the named file.
  rocksdb::Status DeleteFile(const std::string& fname) override;

  // Create the specified directory. Returns error if directory exists.
  rocksdb::Status CreateDir(const std::string& dirname) override;

  // Create directory if missing. Return Ok if it exists, or successful in
  // Creating.
  rocksdb::Status CreateDirIfMissing(const std::string& dirname) override;

  // Delete the specified directory.
  rocksdb::Status DeleteDir(const std::string& dirname) override;

  // Store the size of fname in *file_size.
  rocksdb::Status GetFileSize(const std::string& fname, uint64_t* file_size) override;

  // Store the last modification time of fname in *file_mtime.
  rocksdb::Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) override;
  // Rename file src to target.
  rocksdb::Status RenameFile(const std::string& src,
                            const std::string& target) override;
  // Hard Link file src to target.
  rocksdb::Status LinkFile(const std::string& src, const std::string& target) override;

  // Tell if two files are identical
  rocksdb::Status AreFilesSame(const std::string& first,
			       const std::string& second, bool* res) override;

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  rocksdb::Status LockFile(const std::string& fname, rocksdb::FileLock** lock) override;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  rocksdb::Status UnlockFile(rocksdb::FileLock* lock) override;

  // *path is set to a temporary directory that can be used for testing. It may
  // or may not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  rocksdb::Status GetTestDirectory(std::string* path) override;

  // Create and return a log file for storing informational messages.
  rocksdb::Status NewLogger(
    const std::string& fname,
    std::shared_ptr<rocksdb::Logger>* result) override;

  // Get full directory name for this db.
  rocksdb::Status GetAbsolutePath(const std::string& db_path,
      std::string* output_path) override;

  // Start new thread taking care about Seastar's allocator init.
  void StartThread(void(*function)(void* arg), void* arg) override;

  explicit BlueRocksEnv(BlueFS *f);
private:
  BlueFS *fs;
};

#endif
