// -*- mode:C++; tab-width:8; c-basic-offset:2 -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <ctime>

namespace arrow { class Schema; }

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
};

// Which optional fields the inventory configuration selected.
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
};

// Builds the Arrow schema matching AWS S3 Inventory parquet output.
std::shared_ptr<arrow::Schema> build_inventory_schema(const FieldSelection& sel);

class ParquetInventoryWriter {
public:
  // Opens `path` for writing. Throws std::runtime_error on failure.
  ParquetInventoryWriter(const std::string& path, const FieldSelection& sel,
                         int64_t row_group_size = 10000);
  ~ParquetInventoryWriter();

  ParquetInventoryWriter(const ParquetInventoryWriter&) = delete;
  ParquetInventoryWriter& operator=(const ParquetInventoryWriter&) = delete;

  // Buffer one entry; flushes a row group when row_group_size is reached.
  void append(const InventoryEntry& e);

  // Flush remaining rows and finalize the file footer. Must be called.
  void close();

  int64_t rows_written() const { return total_rows_; }

private:
  void flush_row_group();

  struct Impl;
  std::unique_ptr<Impl> impl_;
  FieldSelection sel_;
  int64_t row_group_size_;
  int64_t total_rows_ = 0;
  bool closed_ = false;
};

} // namespace rgw::inventory