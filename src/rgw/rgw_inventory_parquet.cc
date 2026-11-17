// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_inventory_parquet.h"

#include <arrow/api.h>
#include <arrow/io/interfaces.h>
#include <parquet/arrow/writer.h>

#include <map>
#include <string_view>
#include <tuple>

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
  if (sel.object_lock_retain_until_date)
    fields.push_back(arrow::field(
      "object_lock_retain_until_date",
      arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")));
  if (sel.object_lock_mode)
    fields.push_back(arrow::field("object_lock_mode", arrow::utf8()));
  if (sel.object_lock_legal_hold_status)
    fields.push_back(arrow::field("object_lock_legal_hold_status", arrow::utf8()));
  if (sel.intelligent_tiering_access_tier)
    fields.push_back(arrow::field("intelligent_tiering_access_tier", arrow::utf8()));
  if (sel.bucket_key_status)
    fields.push_back(arrow::field("bucket_key_status", arrow::utf8()));

  return arrow::schema(std::move(fields));
}

struct ParquetInventoryWriter::Impl {
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::io::OutputStream> sink;
  std::unique_ptr<parquet::arrow::FileWriter> writer;

  arrow::StringBuilder bucket, key, version_id, etag, storage_class;
  arrow::BooleanBuilder is_latest, is_delete_marker, is_multipart_uploaded;
  arrow::Int64Builder size;
  arrow::TimestampBuilder last_modified{
      arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"),
      arrow::default_memory_pool()};
  int64_t pending = 0;

  // the columns filled from InventoryEntry, by column name. any other
  // column in the schema is written as nulls
  std::map<std::string_view, arrow::ArrayBuilder*> builders() {
    return {
      {"bucket", &bucket},
      {"key", &key},
      {"version_id", &version_id},
      {"is_latest", &is_latest},
      {"is_delete_marker", &is_delete_marker},
      {"size", &size},
      {"last_modified_date", &last_modified},
      {"e_tag", &etag},
      {"storage_class", &storage_class},
      {"is_multipart_uploaded", &is_multipart_uploaded},
    };
  }
};

ParquetInventoryWriter::ParquetInventoryWriter(const FieldSelection& sel,
                                               int64_t row_group_size)
  : impl_(std::make_unique<Impl>()), sel_(sel), row_group_size_(row_group_size)
{
}

arrow::Result<std::unique_ptr<ParquetInventoryWriter>>
ParquetInventoryWriter::Open(std::shared_ptr<arrow::io::OutputStream> sink,
                             const FieldSelection& sel,
                             int64_t row_group_size)
{
  if (!sink) {
    return arrow::Status::Invalid("parquet inventory writer: null sink");
  }
  std::unique_ptr<ParquetInventoryWriter> w(
      new ParquetInventoryWriter(sel, row_group_size));
  auto& im = *w->impl_;
  im.sink = std::move(sink);
  im.schema = build_inventory_schema(sel);

  auto props = parquet::WriterProperties::Builder()
                 .compression(parquet::Compression::SNAPPY)
                 ->build();
  // see the comment on ParquetInventoryWriter
  auto arrow_props = parquet::ArrowWriterProperties::Builder()
                       .set_use_threads(false)
                       ->build();
  // on failure the destructor aborts the sink
  ARROW_ASSIGN_OR_RAISE(im.writer, parquet::arrow::FileWriter::Open(
      *im.schema, arrow::default_memory_pool(), im.sink, props, arrow_props));
  return w;
}

ParquetInventoryWriter::~ParquetInventoryWriter()
{
  abort();
}

namespace {
template <typename Builder, typename V>
arrow::Status append_opt(Builder& b, const std::optional<V>& v)
{
  return v ? b.Append(*v) : b.AppendNull();
}
} // anonymous namespace

arrow::Status ParquetInventoryWriter::append(const InventoryEntry& e)
{
  if (state_ != State::Open) {
    return arrow::Status::Invalid("parquet inventory writer is not open");
  }
  auto& im = *impl_;
  ARROW_RETURN_NOT_OK(im.bucket.Append(e.bucket));
  ARROW_RETURN_NOT_OK(im.key.Append(e.key));
  if (sel_.version_id) {
    ARROW_RETURN_NOT_OK(append_opt(im.version_id, e.version_id));
  }
  if (sel_.is_latest) {
    ARROW_RETURN_NOT_OK(append_opt(im.is_latest, e.is_latest));
  }
  if (sel_.is_delete_marker) {
    ARROW_RETURN_NOT_OK(append_opt(im.is_delete_marker, e.is_delete_marker));
  }
  if (sel_.size) {
    ARROW_RETURN_NOT_OK(append_opt(im.size, e.size));
  }
  if (sel_.last_modified) {
    ARROW_RETURN_NOT_OK(append_opt(im.last_modified, e.last_modified_ms));
  }
  if (sel_.etag) {
    ARROW_RETURN_NOT_OK(append_opt(im.etag, e.etag));
  }
  if (sel_.storage_class) {
    ARROW_RETURN_NOT_OK(append_opt(im.storage_class, e.storage_class));
  }
  if (sel_.is_multipart_uploaded) {
    ARROW_RETURN_NOT_OK(append_opt(im.is_multipart_uploaded,
                                   e.is_multipart_uploaded));
  }

  ++im.pending;
  ++total_rows_;
  if (im.pending >= row_group_size_) {
    return flush_row_group();
  }
  return arrow::Status::OK();
}

arrow::Status ParquetInventoryWriter::flush_row_group()
{
  auto& im = *impl_;
  if (im.pending == 0) {
    return arrow::Status::OK();
  }

  const auto builders = im.builders();
  std::vector<std::shared_ptr<arrow::Array>> cols;
  for (const auto& field : im.schema->fields()) {
    std::shared_ptr<arrow::Array> arr;
    if (auto b = builders.find(field->name()); b != builders.end()) {
      ARROW_RETURN_NOT_OK(b->second->Finish(&arr));
    } else {
      ARROW_ASSIGN_OR_RAISE(arr, arrow::MakeArrayOfNull(field->type(),
                                                        im.pending));
    }
    cols.push_back(std::move(arr));
  }

  auto table = arrow::Table::Make(im.schema, cols, im.pending);
  ARROW_RETURN_NOT_OK(im.writer->WriteTable(*table, im.pending));
  im.pending = 0;
  return arrow::Status::OK();
}

arrow::Status ParquetInventoryWriter::close()
{
  switch (state_) {
  case State::Closed:
    return arrow::Status::OK();
  case State::Aborted:
    return arrow::Status::Invalid("parquet inventory writer was aborted");
  case State::Open:
    break;
  }
  auto s = flush_row_group();
  if (s.ok()) {
    s = impl_->writer->Close();  // footer only, the sink stays open
  }
  if (!s.ok()) {
    abort();
    return s;
  }
  state_ = State::Closed;
  return impl_->sink->Close();
}

void ParquetInventoryWriter::abort() noexcept
{
  if (state_ != State::Open) {
    return;
  }
  state_ = State::Aborted;
  if (impl_->sink) {
    std::ignore = impl_->sink->Abort();
  }
}

} // namespace rgw::inventory
