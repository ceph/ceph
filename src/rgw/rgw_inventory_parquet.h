// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <ctime>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

namespace arrow {
class Schema;
namespace io { class OutputStream; }
}

namespace rgw::inventory {

// one row per object version
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
  // replication, encryption, object lock, tiering and bucket key status
  // are not in the bucket index entry. their columns are written as nulls
};

struct FieldSelection {
  bool version_id = false;
  bool is_latest = false;
  bool is_delete_marker = false;
  bool size = false;
  bool last_modified = false;
  bool etag = false;
  bool storage_class = false;
  bool is_multipart_uploaded = false;
  bool replication_status = false;
  bool encryption_status = false;
  bool object_lock_retain_until_date = false;
  bool object_lock_mode = false;
  bool object_lock_legal_hold_status = false;
  bool intelligent_tiering_access_tier = false;
  bool bucket_key_status = false;
};

// column names and types follow the AWS S3 Inventory parquet schema
std::shared_ptr<arrow::Schema> build_inventory_schema(const FieldSelection& sel);

// Writes inventory rows as parquet into the given sink. If the writer is
// destroyed without a successful close(), or close() fails, the sink is
// aborted.
//
// Arrow threads are disabled: this runs in the lc worker coroutine and
// RGWOutputStream must only be used with that coroutine's yield context.
class ParquetInventoryWriter {
public:
  static arrow::Result<std::unique_ptr<ParquetInventoryWriter>> Open(
      std::shared_ptr<arrow::io::OutputStream> sink,
      const FieldSelection& sel,
      int64_t row_group_size = 10000);
  ~ParquetInventoryWriter();

  ParquetInventoryWriter(const ParquetInventoryWriter&) = delete;
  ParquetInventoryWriter& operator=(const ParquetInventoryWriter&) = delete;

  arrow::Status append(const InventoryEntry& e);

  // writes the footer and closes the sink
  arrow::Status close();

  void abort() noexcept;

  int64_t rows_written() const { return total_rows_; }

private:
  ParquetInventoryWriter(const FieldSelection& sel, int64_t row_group_size);
  arrow::Status flush_row_group();

  enum class State { Open, Closed, Aborted };

  struct Impl;
  std::unique_ptr<Impl> impl_;
  FieldSelection sel_;
  int64_t row_group_size_;
  int64_t total_rows_ = 0;
  State state_ = State::Open;
};

} // namespace rgw::inventory
