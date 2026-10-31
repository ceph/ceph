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
 * RGWOutputStream: an arrow::io::OutputStream that writes a single RGW object
 * through the C++ SAL API (rgw::sal::Writer, the atomic object writer).
 *
 * This is the write-side counterpart of RGWRandomAccessFile.  It lets Arrow
 * producers that write sequentially -- most notably the Parquet writer --
 * store objects in-process, without going out over HTTP:
 *
 *   auto sink = RGWOutputStream::Open(dpp, y, driver, bucket, key);
 *   parquet::arrow::WriteTable(table, pool, *sink, row_group_size);
 *   (*sink)->Close();   // the object becomes visible here
 *
 * Writes are accumulated in a bufferlist and handed to the SAL writer in
 * chunks (see Open()'s chunk_size), so the stream never holds more than one
 * chunk of unflushed data.  The data is only made visible by Close(), which
 * flushes the tail, computes the ETag, and completes the write; this is the
 * same all-or-nothing semantics as an S3 PUT.  Abort(), or destroying the
 * stream without closing it, discards the object (note that the rados driver
 * leaves the already written tail stripes behind, as it does for an
 * interrupted PUT).
 *
 * Tell() is exact at all times (it counts bytes accepted by Write()), which
 * the Parquet writer relies on for its row group and footer offsets.  After
 * Close(), size() and etag() describe the stored object.
 *
 * As with every Arrow OutputStream, instances are not thread-safe.  The
 * optional_yield given to Open() is captured and reused for every SAL call,
 * so it must stay valid for the lifetime of the stream.
 *
 * Instances are always owned by a std::shared_ptr: Arrow's writers take the
 * sink by shared_ptr, and arrow::io::FileInterface derives from
 * std::enable_shared_from_this.
 */

#pragma once

#include <memory>
#include <string>

#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include "common/async/yield_context.h"
#include "common/ceph_crypto.h"
#include "common/dout.h"
#include "include/buffer.h"
#include "rgw_sal.h"

class RGWOutputStream : public arrow::io::OutputStream {
public:
  /// default size of the chunks handed to the SAL writer
  static constexpr int64_t kDefaultChunkSize = 4 << 20;

  /**
   * Open an object for writing.  @a driver is the SAL driver that @a obj's
   * bucket was loaded from; it provides the writer.  The returned stream owns
   * @a obj; the bucket that @a obj was obtained from must outlive the stream.
   * Any existing object with the same key is replaced when the stream is
   * closed.
   *
   * @a attrs are stored on the object as given (e.g. RGW_ATTR_ACL,
   * RGW_ATTR_CONTENT_TYPE or RGW_ATTR_META_PREFIX user metadata); the ETag
   * attribute is always set by the stream from the written data.  The stream
   * adds no other attributes: an object without RGW_ATTR_ACL is treated by
   * RGW as owned by the bucket owner, but a PUT request always stores one.
   * @a unique_tag is the operation tag of the write.  It becomes the object's
   * ID tag (RGW_ATTR_ID_TAG), which the rados driver uses to detect
   * concurrent writers; a PUT request uses its request id.  When empty, a tag
   * is generated from a new request id.
   * @a chunk_size is the amount of buffered data that triggers a write to
   * the SAL writer; it must be positive.
   */
  static arrow::Result<std::shared_ptr<RGWOutputStream>> Open(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Driver* driver, std::unique_ptr<rgw::sal::Object> obj,
      rgw::sal::Attrs attrs = {}, std::string unique_tag = {},
      int64_t chunk_size = kDefaultChunkSize);

  /**
   * Convenience overload: get @a key from an already loaded @a bucket and
   * open it for writing.  @a bucket must outlive the returned stream.
   */
  static arrow::Result<std::shared_ptr<RGWOutputStream>> Open(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Driver* driver, rgw::sal::Bucket* bucket,
      const rgw_obj_key& key, rgw::sal::Attrs attrs = {},
      std::string unique_tag = {}, int64_t chunk_size = kDefaultChunkSize);

  /// Discards the object if the stream was not closed.
  ~RGWOutputStream() override;

  // arrow::io::FileInterface
  arrow::Status Close() override;
  arrow::Status Abort() override;
  arrow::Result<int64_t> Tell() const override;
  bool closed() const override;

  // arrow::io::Writable
  arrow::Status Write(const void* data, int64_t nbytes) override;
  arrow::Status Flush() override;
  // keep the Buffer and string_view overloads visible
  using arrow::io::Writable::Write;

  /** Name of the backing object, for logging and error messages. */
  const std::string& name() const { return name_; }

  /**
   * Bytes written so far.  Unlike Tell(), this stays available after Close(),
   * so a caller can record the object's size and etag() together.
   */
  int64_t size() const { return pos_; }

  /** ETag of the written object; set once Close() succeeded. */
  const std::string& etag() const { return etag_; }

  /** Operation tag of the write, as given to Open() or generated. */
  const std::string& unique_tag() const { return unique_tag_; }

private:
  RGWOutputStream(const DoutPrefixProvider* dpp, optional_yield y,
                  std::unique_ptr<rgw::sal::Object> obj, rgw::sal::Attrs attrs,
                  std::string unique_tag, int64_t chunk_size);

  arrow::Status prepare(rgw::sal::Driver* driver);
  arrow::Status check_open() const;
  // hand the buffered data to the SAL writer; final=true also tells the
  // writer that no more data will follow
  arrow::Status flush_pending(bool final);

  const DoutPrefixProvider* const dpp_;
  const optional_yield y_;
  const int64_t chunk_size_;

  // the bucket obj_ belongs to is owned by the caller
  std::unique_ptr<rgw::sal::Object> obj_;
  std::unique_ptr<rgw::sal::Writer> writer_;
  std::string name_;
  rgw::sal::Attrs attrs_;
  std::string unique_tag_;

  ceph::crypto::MD5 hash_;
  std::string etag_;

  ceph::bufferlist pending_;   // written but not yet handed to writer_
  int64_t flushed_ = 0;        // bytes handed to writer_ so far
  int64_t pos_ = 0;            // bytes accepted by Write() so far
  bool closed_ = false;
};
