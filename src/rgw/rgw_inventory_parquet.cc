// -*- mode:C++; tab-width:8; c-basic-offset:2 -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_inventory_parquet.h"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

#include <stdexcept>

namespace rgw::inventory {

std::shared_ptr<arrow::Schema> build_inventory_schema(const FieldSelection& sel)
{
  std::vector<std::shared_ptr<arrow::Field>> fields;
  // bucket and key are always present and required
  fields.push_back(arrow::field("bucket", arrow::utf8(), /*nullable=*/false));
  fields.push_back(arrow::field("key", arrow::utf8(), /*nullable=*/false));

  if (sel.version_id)
    fields.push_back(arrow::field("version_id", arrow::utf8()));
  if (sel.is_latest)
    fields.push_back(arrow::field("is_latest", arrow::boolean()));
  if (sel.is_delete_marker)
    fields.push_back(arrow::field("is_delete_marker", arrow::boolean()));
  if (sel.size)
    fields.push_back(arrow::field("size", arrow::int64()));
  if (sel.last_modified)
    fields.push_back(arrow::field(
      "last_modified_date", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")));
  if (sel.etag)
    fields.push_back(arrow::field("e_tag", arrow::utf8()));
  if (sel.storage_class)
    fields.push_back(arrow::field("storage_class", arrow::utf8()));
  if (sel.is_multipart_uploaded)
    fields.push_back(arrow::field("is_multipart_uploaded", arrow::boolean()));
  if (sel.replication_status)
    fields.push_back(arrow::field("replication_status", arrow::utf8()));
  if (sel.encryption_status)
    fields.push_back(arrow::field("encryption_status", arrow::utf8()));

  return arrow::schema(std::move(fields));
}

struct ParquetInventoryWriter::Impl {
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::io::FileOutputStream> sink;
  std::unique_ptr<parquet::arrow::FileWriter> writer;

  // column builders (only the selected ones are used)
  arrow::StringBuilder bucket, key, version_id, etag, storage_class,
                       replication_status, encryption_status;
  arrow::BooleanBuilder is_latest, is_delete_marker, is_multipart;
  arrow::Int64Builder size;
  std::unique_ptr<arrow::TimestampBuilder> last_modified;
  int64_t pending = 0;
};

ParquetInventoryWriter::ParquetInventoryWriter(
  const std::string& path, const FieldSelection& sel, int64_t row_group_size)
  : impl_(std::make_unique<Impl>()), sel_(sel), row_group_size_(row_group_size)
{
  impl_->schema = build_inventory_schema(sel_);

  auto sink_res = arrow::io::FileOutputStream::Open(path);
  if (!sink_res.ok()) {
    throw std::runtime_error("open failed: " + sink_res.status().ToString());
  }
  impl_->sink = *sink_res;

  auto props = parquet::WriterProperties::Builder()
                 .compression(parquet::Compression::SNAPPY)
                 ->build();
  auto writer_res = parquet::arrow::FileWriter::Open(
      *impl_->schema, arrow::default_memory_pool(), impl_->sink, props);
  if (!writer_res.ok()) {
    throw std::runtime_error("writer open failed: " +
                             writer_res.status().ToString());
  }
  impl_->writer = std::move(*writer_res);

  if (sel_.last_modified) {
    impl_->last_modified = std::make_unique<arrow::TimestampBuilder>(
        arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"),
        arrow::default_memory_pool());
  }
}

ParquetInventoryWriter::~ParquetInventoryWriter()
{
  if (!closed_) {
    try { close(); } catch (...) {}
  }
}

namespace {
template <typename Builder, typename V>
void append_opt(Builder& b, const std::optional<V>& v)
{
  arrow::Status s = v ? b.Append(*v) : b.AppendNull();
  if (!s.ok()) throw std::runtime_error(s.ToString());
}
} // anonymous namespace

void ParquetInventoryWriter::append(const InventoryEntry& e)
{
  auto& im = *impl_;
  if (auto s = im.bucket.Append(e.bucket); !s.ok())
    throw std::runtime_error(s.ToString());
  if (auto s = im.key.Append(e.key); !s.ok())
    throw std::runtime_error(s.ToString());

  if (sel_.version_id)        append_opt(im.version_id, e.version_id);
  if (sel_.is_latest)         append_opt(im.is_latest, e.is_latest);
  if (sel_.is_delete_marker)  append_opt(im.is_delete_marker, e.is_delete_marker);
  if (sel_.size)              append_opt(im.size, e.size);
  if (sel_.last_modified)     append_opt(*im.last_modified, e.last_modified_ms);
  if (sel_.etag)              append_opt(im.etag, e.etag);
  if (sel_.storage_class)     append_opt(im.storage_class, e.storage_class);
  if (sel_.is_multipart_uploaded)
                              append_opt(im.is_multipart, e.is_multipart_uploaded);
  if (sel_.replication_status)
                              append_opt(im.replication_status, e.replication_status);
  if (sel_.encryption_status)
                              append_opt(im.encryption_status, e.encryption_status);

  ++im.pending;
  ++total_rows_;
  if (im.pending >= row_group_size_) {
    flush_row_group();
  }
}

void ParquetInventoryWriter::flush_row_group()
{
  auto& im = *impl_;
  if (im.pending == 0) return;

  std::vector<std::shared_ptr<arrow::Array>> cols;
  auto finish = [&cols](auto& builder) {
    std::shared_ptr<arrow::Array> arr;
    auto s = builder.Finish(&arr);
    if (!s.ok()) throw std::runtime_error(s.ToString());
    cols.push_back(std::move(arr));
  };

  finish(im.bucket);
  finish(im.key);
  if (sel_.version_id)            finish(im.version_id);
  if (sel_.is_latest)             finish(im.is_latest);
  if (sel_.is_delete_marker)      finish(im.is_delete_marker);
  if (sel_.size)                  finish(im.size);
  if (sel_.last_modified)         finish(*im.last_modified);
  if (sel_.etag)                  finish(im.etag);
  if (sel_.storage_class)         finish(im.storage_class);
  if (sel_.is_multipart_uploaded) finish(im.is_multipart);
  if (sel_.replication_status)    finish(im.replication_status);
  if (sel_.encryption_status)     finish(im.encryption_status);

  auto table = arrow::Table::Make(im.schema, cols, im.pending);
  auto s = im.writer->WriteTable(*table, im.pending);
  if (!s.ok()) throw std::runtime_error(s.ToString());
  im.pending = 0;
}

void ParquetInventoryWriter::close()
{
  if (closed_) return;
  flush_row_group();
  auto s = impl_->writer->Close();
  if (!s.ok()) throw std::runtime_error(s.ToString());
  closed_ = true;
}

} // namespace rgw::inventory