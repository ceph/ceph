// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <ctime>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace arrow {
class Schema;
namespace io { class OutputStream; }
}

namespace rgw::inventory {

// One row of an inventory report == one object version in the bucket.
struct InventoryEntry {
  std::string bucket;
  std::string key;
  std::optional<std::string> version_id;
  std::optional<bool> is_latest;
  std::optional<bool> is_delete_marker;
  std::optional<int64_t> size;
  std::optional<int64_t> last_modified_ms;   // epoch millis
  std::optional<std::string> etag;
  std::optional<std::string> storage_class;
  std::optional<bool> is_multipart_uploaded;
  std::optional<std::string> replication_status;
  std::optional<std::string> encryption_status;
  std::optional<int64_t> object_lock_retain_until_date_ms;  // epoch millis
  std::optional<std::string> object_lock_mode;
  std::optional<std::string> object_lock_legal_hold_status;
  std::optional<std::string> intelligent_tiering_access_tier;
  std::optional<std::string> bucket_key_status;
};

// Which optional columns the inventory configuration selected.
struct FieldSelection {
  bool version_id = false;
  bool is_latest = false;
  bool is_delete_marker = false;
  bool size = true;
  bool last_modified = true;
  bool etag = true;
  bool storage_class = true;
  bool is_multipart_uploaded = false;
  bool replication_status = false;
  bool encryption_status = false;
  bool object_lock_retain_until_date = false;
  bool object_lock_mode = false;
  bool object_lock_legal_hold_status = false;
  bool intelligent_tiering_access_tier = false;
  bool bucket_key_status = false;
};

// Builds the Arrow schema matching AWS S3 Inventory Parquet output.
// Validated against real AWS-generated inventory (17 columns).
std::shared_ptr<arrow::Schema> build_inventory_schema(const FieldSelection& sel);

// Streams inventory rows into a Parquet file written to `sink`.
//
// The sink decides where the bytes go: an RGWOutputStream writes straight
// into an RGW object, a BufferOutputStream into memory (tests), a
// FileOutputStream into a local file.
//
// close() finishes the Parquet footer and then closes the sink; for an
// RGWOutputStream that is the moment the object becomes visible. If the
// writer is destroyed without a successful close(), or abort() is called,
// the sink is aborted instead, so a failed run never publishes a partial
// report.
//
// Encoding runs on the calling thread only (Arrow's thread pool is not
// used): the inventory runner executes inside the lifecycle worker's
// coroutine, and RGWOutputStream issues SAL writes with that coroutine's
// yield context, which must not be used from another thread.
class ParquetInventoryWriter {
public:
  // Throws std::runtime_error if the Parquet writer can't be opened.
  ParquetInventoryWriter(std::shared_ptr<arrow::io::OutputStream> sink,
                         const FieldSelection& sel,
                         int64_t row_group_size = 10000);
  ~ParquetInventoryWriter();

  ParquetInventoryWriter(const ParquetInventoryWriter&) = delete;
  ParquetInventoryWriter& operator=(const ParquetInventoryWriter&) = delete;

  // Buffer one entry; flushes a row group when row_group_size is reached.
  void append(const InventoryEntry& e);

  // Flush remaining rows, write the footer and close the sink.
  // Throws std::runtime_error on failure (the sink is aborted first).
  void close();

  // Discard the report: the sink is aborted, nothing is published.
  void abort() noexcept;

  int64_t rows_written() const { return total_rows_; }

private:
  void flush_row_group();

  struct Impl;
  std::unique_ptr<Impl> impl_;
  FieldSelection sel_;
  int64_t row_group_size_;
  int64_t total_rows_ = 0;
  bool finished_ = false;   // closed or aborted
};

} // namespace rgw::inventory
