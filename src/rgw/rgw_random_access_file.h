// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

/*
 * RGWRandomAccessFile: an arrow::io::RandomAccessFile backed by a single RGW
 * object, read through the C++ SAL API (rgw::sal::Object::ReadOp).
 *
 * This lets Arrow consumers that need random access -- most notably the
 * Parquet reader (parquet::arrow::OpenFile) -- read objects in-process
 *
 *   auto file = RGWRandomAccessFile::Open(dpp, y, bucket, key);
 *   auto reader = parquet::arrow::OpenFile(*file, arrow::default_memory_pool());
 *
 * Both ReadAt() overloads are implemented directly on top of
 * ReadOp::iterate(), with a ReadOp created per call, so they share no state
 * besides the object and may be called from several threads at once (the
 * parquet reader does that with threaded column reads and pre-buffering).
 * The sequential Read()/Seek()/Tell()/Peek() API is layered on top of
 * ReadAt() and, as with every Arrow InputStream, is not thread-safe.
 *
 * The optional_yield given to Open() is captured and reused for every SAL
 * call.  Arrow may invoke ReadAt() from its own I/O thread pool (pre-buffering
 * or threaded column reads), where a yield context borrowed from a beast
 * coroutine must not be used.  Callers that run inside a request should either
 * open the file with null_yield or disable threaded reads
 * (ArrowReaderProperties::set_use_threads(false), set_pre_buffer(false)).
 *
 * Instances are always owned by a std::shared_ptr: Arrow's readers take the
 * file by shared_ptr, and arrow::io::FileInterface derives from
 * std::enable_shared_from_this (the default ReadAsync() relies on it).
 */

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include "common/async/yield_context.h"
#include "common/dout.h"
#include "rgw_sal.h"

class RGWRandomAccessFile : public arrow::io::RandomAccessFile {
public:
  /**
   * Open an object for reading.  Loads the object state (size, attributes);
   * no read op is created until data is actually read.  The returned file
   * owns @a obj; the bucket that @a obj was obtained from must outlive the
   * file.
   *
   * Fails with an IOError whose message carries the errno text if the object
   * does not exist or cannot be read.
   */
  static arrow::Result<std::shared_ptr<RGWRandomAccessFile>> Open(
      const DoutPrefixProvider* dpp, optional_yield y,
      std::unique_ptr<rgw::sal::Object> obj);

  /**
   * Convenience overload: get @a key from an already loaded @a bucket and
   * open it.  @a bucket must outlive the returned file.
   */
  static arrow::Result<std::shared_ptr<RGWRandomAccessFile>> Open(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Bucket* bucket, const rgw_obj_key& key);

  ~RGWRandomAccessFile() override;

  // arrow::io::FileInterface
  arrow::Status Close() override;
  arrow::Result<int64_t> Tell() const override;
  bool closed() const override;

  // arrow::io::Readable (sequential, uses the stream position)
  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

  // arrow::io::InputStream
  arrow::Result<std::string_view> Peek(int64_t nbytes) override;
  bool supports_zero_copy() const override { return false; }
  arrow::Result<std::shared_ptr<const arrow::KeyValueMetadata>>
      ReadMetadata() override;

  // arrow::io::Seekable
  arrow::Status Seek(int64_t position) override;

  // arrow::io::RandomAccessFile (position-independent, thread-safe)
  arrow::Result<int64_t> GetSize() override;
  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes,
                                void* out) override;
  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                       int64_t nbytes) override;

  /** Name of the backing object, for logging and error messages. */
  const std::string& name() const { return name_; }

private:
  RGWRandomAccessFile(const DoutPrefixProvider* dpp, optional_yield y,
                      std::unique_ptr<rgw::sal::Object> obj);

  arrow::Status load_object_state();
  void build_metadata();
  arrow::Status check_open() const;

  const DoutPrefixProvider* const dpp_;
  const optional_yield y_;

  // the bucket obj_ belongs to is owned by the caller
  std::unique_ptr<rgw::sal::Object> obj_;
  std::string name_;

  int64_t size_ = 0;
  rgw::sal::Attrs attrs_;
  std::shared_ptr<const arrow::KeyValueMetadata> metadata_;

  std::atomic<int64_t> pos_{0};
  std::atomic<bool> closed_{false};

  // backing store for Peek(); valid until the next call on this stream
  std::string peek_buf_;
};
