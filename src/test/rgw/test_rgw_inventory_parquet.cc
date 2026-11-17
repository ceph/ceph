// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <catch2/catch_all.hpp>

#include "rgw/rgw_inventory_parquet.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>

#include <memory>
#include <string>

using namespace rgw::inventory;

namespace {

std::shared_ptr<arrow::io::BufferOutputStream> memory_sink()
{
  auto res = arrow::io::BufferOutputStream::Create();
  REQUIRE(res.ok());
  return *res;
}

std::shared_ptr<arrow::Table> read_back(const std::shared_ptr<arrow::Buffer>& buf)
{
  auto input = std::make_shared<arrow::io::BufferReader>(buf);

  parquet::arrow::FileReaderBuilder builder;
  auto st = builder.Open(input);
  REQUIRE(st.ok());

  std::unique_ptr<parquet::arrow::FileReader> reader;
  st = builder.Build(&reader);
  REQUIRE(st.ok());
  REQUIRE(reader != nullptr);

  std::shared_ptr<arrow::Table> table;
  st = reader->ReadTable(&table);
  REQUIRE(st.ok());
  REQUIRE(table != nullptr);
  return table;
}

// Bytes the writer produced. BufferOutputStream::Finish() also closes the
// stream, so only call it on a sink the writer did not close.
std::shared_ptr<arrow::Buffer> contents(
    const std::shared_ptr<arrow::io::BufferOutputStream>& sink)
{
  auto res = sink->Finish();
  REQUIRE(res.ok());
  return *res;
}

// Wraps a BufferOutputStream and records how the writer finished it.
class TrackingSink : public arrow::io::OutputStream {
public:
  TrackingSink() : inner_(memory_sink()) {}

  arrow::Status Close() override { closed_by_close = true; return inner_->Close(); }
  arrow::Status Abort() override { aborted = true; return inner_->Abort(); }
  arrow::Result<int64_t> Tell() const override { return inner_->Tell(); }
  bool closed() const override { return inner_->closed(); }
  arrow::Status Write(const void* data, int64_t nbytes) override {
    return inner_->Write(data, nbytes);
  }
  using arrow::io::Writable::Write;

  std::shared_ptr<arrow::Buffer> buffer() { return contents(inner_); }

  bool closed_by_close = false;
  bool aborted = false;

private:
  std::shared_ptr<arrow::io::BufferOutputStream> inner_;
};

// the columns sample_entry() fills
FieldSelection basic_fields()
{
  FieldSelection sel;
  sel.size = sel.last_modified = sel.etag = sel.storage_class = true;
  return sel;
}

std::unique_ptr<ParquetInventoryWriter> open_writer(
    std::shared_ptr<arrow::io::OutputStream> sink,
    const FieldSelection& sel = basic_fields(),
    int64_t row_group_size = 10000)
{
  auto res = ParquetInventoryWriter::Open(std::move(sink), sel, row_group_size);
  REQUIRE(res.ok());
  return std::move(*res);
}

InventoryEntry sample_entry(int i)
{
  InventoryEntry e;
  e.bucket = "test-bucket";
  e.key = "prefix/object-" + std::to_string(i);
  e.size = 1024 * i;
  e.last_modified_ms = 1720000000000LL + i * 1000;
  e.etag = "d41d8cd98f00b204e9800998ecf8427e";
  e.storage_class = "STANDARD";
  return e;
}

} // anonymous namespace

TEST_CASE("schema contains required and selected fields", "[inventory][schema]")
{
  auto schema = build_inventory_schema(basic_fields());

  REQUIRE(schema->num_fields() == 6);
  CHECK(schema->field(0)->name() == "bucket");
  CHECK(schema->field(0)->type()->id() == arrow::Type::STRING);
  CHECK_FALSE(schema->field(0)->nullable());
  CHECK(schema->field(1)->name() == "key");
  CHECK(schema->GetFieldByName("size") != nullptr);
  CHECK(schema->GetFieldByName("last_modified_date") != nullptr);
  CHECK(schema->GetFieldByName("e_tag") != nullptr);
  CHECK(schema->GetFieldByName("storage_class") != nullptr);
}

TEST_CASE("versioned-bucket schema includes version fields", "[inventory][schema]")
{
  FieldSelection sel;
  sel.version_id = true;
  sel.is_latest = true;
  sel.is_delete_marker = true;
  auto schema = build_inventory_schema(sel);
  CHECK(schema->GetFieldByName("version_id") != nullptr);
  CHECK(schema->GetFieldByName("is_latest") != nullptr);
  CHECK(schema->GetFieldByName("is_delete_marker") != nullptr);
}

TEST_CASE("no optional fields gives bucket and key only", "[inventory][schema]")
{
  CHECK(build_inventory_schema(FieldSelection{})->num_fields() == 2);
}

TEST_CASE("all optional fields produce the 17-column AWS schema", "[inventory][schema][aws]")
{
  FieldSelection sel = basic_fields();
  sel.version_id = sel.is_latest = sel.is_delete_marker = true;
  sel.is_multipart_uploaded = sel.replication_status = sel.encryption_status = true;
  sel.object_lock_retain_until_date = sel.object_lock_mode = true;
  sel.object_lock_legal_hold_status = true;
  sel.intelligent_tiering_access_tier = sel.bucket_key_status = true;
  auto schema = build_inventory_schema(sel);

  const std::vector<std::string> expected = {
    "bucket", "key", "version_id", "is_latest", "is_delete_marker", "size",
    "last_modified_date", "e_tag", "storage_class", "is_multipart_uploaded",
    "replication_status", "encryption_status", "object_lock_retain_until_date",
    "object_lock_mode", "object_lock_legal_hold_status",
    "intelligent_tiering_access_tier", "bucket_key_status"
  };
  REQUIRE(schema->num_fields() == static_cast<int>(expected.size()));
  for (size_t i = 0; i < expected.size(); ++i) {
    CHECK(schema->field(i)->name() == expected[i]);
  }
}

TEST_CASE("write a small inventory", "[inventory][write]")
{
  auto sink = memory_sink();
  auto w = open_writer(sink);
  for (int i = 0; i < 10; ++i) {
    REQUIRE(w->append(sample_entry(i)).ok());
  }
  REQUIRE(w->close().ok());
  CHECK(w->rows_written() == 10);
  CHECK(contents(sink)->size() > 0);
}

TEST_CASE("write and read back a small inventory", "[inventory][roundtrip]")
{
  auto sink = memory_sink();
  {
    auto w = open_writer(sink);
    for (int i = 0; i < 100; ++i) {
      REQUIRE(w->append(sample_entry(i)).ok());
    }
    REQUIRE(w->close().ok());
    CHECK(w->rows_written() == 100);
  }

  auto table = read_back(contents(sink));
  REQUIRE(table != nullptr);
  CHECK(table->num_rows() == 100);
  CHECK(table->num_columns() == 6);

  int key_idx = table->schema()->GetFieldIndex("key");
  REQUIRE(key_idx >= 0);
  auto flat_res = arrow::Concatenate(table->column(key_idx)->chunks());
  REQUIRE(flat_res.ok());
  auto key_col = std::static_pointer_cast<arrow::StringArray>(*flat_res);
  CHECK(key_col->GetString(0) == "prefix/object-0");
  CHECK(key_col->GetString(99) == "prefix/object-99");
}

TEST_CASE("multiple row groups read back in order", "[inventory][roundtrip]")
{
  auto sink = memory_sink();
  {
    auto w = open_writer(sink, basic_fields(), /*row_group_size=*/7);
    for (int i = 0; i < 50; ++i) {
      REQUIRE(w->append(sample_entry(i)).ok());
    }
    REQUIRE(w->close().ok());
  }

  auto table = read_back(contents(sink));
  CHECK(table->num_rows() == 50);
}

TEST_CASE("null optional values are preserved", "[inventory][nulls]")
{
  auto sink = memory_sink();
  {
    auto w = open_writer(sink);
    InventoryEntry e;
    e.bucket = "b";
    e.key = "k";
    REQUIRE(w->append(e).ok());
    REQUIRE(w->close().ok());
  }

  auto table = read_back(contents(sink));
  REQUIRE(table != nullptr);
  int size_idx = table->schema()->GetFieldIndex("size");
  REQUIRE(size_idx >= 0);
  CHECK(table->column(size_idx)->null_count() == 1);
}

TEST_CASE("close() closes the sink", "[inventory][sink]")
{
  auto sink = std::make_shared<TrackingSink>();
  {
    auto w = open_writer(sink);
    REQUIRE(w->append(sample_entry(1)).ok());
    REQUIRE(w->close().ok());
    CHECK(sink->closed());
    CHECK(w->close().ok());  // idempotent
  }
  CHECK(sink->closed_by_close);
  CHECK_FALSE(sink->aborted);  // destructor after close() is a no-op
  CHECK(read_back(sink->buffer())->num_rows() == 1);
}

TEST_CASE("destroying the writer without close() aborts the sink",
          "[inventory][sink]")
{
  auto sink = std::make_shared<TrackingSink>();
  {
    auto w = open_writer(sink);
    REQUIRE(w->append(sample_entry(1)).ok());
  }
  CHECK(sink->aborted);
  CHECK_FALSE(sink->closed_by_close);
}

TEST_CASE("abort() discards the report", "[inventory][sink]")
{
  auto sink = std::make_shared<TrackingSink>();
  auto w = open_writer(sink);
  REQUIRE(w->append(sample_entry(1)).ok());
  w->abort();
  CHECK(sink->aborted);
  CHECK_FALSE(w->append(sample_entry(2)).ok());
  CHECK_FALSE(w->close().ok());
  CHECK_FALSE(sink->closed_by_close);
}

TEST_CASE("a null sink is rejected", "[inventory][sink]")
{
  CHECK_FALSE(ParquetInventoryWriter::Open(nullptr, FieldSelection{}).ok());
}

TEST_CASE("columns not in the bucket index are written as typed nulls",
          "[inventory][nulls]")
{
  FieldSelection sel = basic_fields();
  sel.replication_status = sel.object_lock_retain_until_date = true;
  auto sink = memory_sink();
  {
    auto w = open_writer(sink, sel, /*row_group_size=*/2);
    for (int i = 0; i < 3; ++i) {  // two row groups
      REQUIRE(w->append(sample_entry(i)).ok());
    }
    REQUIRE(w->close().ok());
  }

  auto table = read_back(contents(sink));
  REQUIRE(table->num_rows() == 3);
  auto replication = table->GetColumnByName("replication_status");
  REQUIRE(replication != nullptr);
  CHECK(replication->type()->id() == arrow::Type::STRING);
  CHECK(replication->null_count() == 3);
  auto retain = table->GetColumnByName("object_lock_retain_until_date");
  REQUIRE(retain != nullptr);
  CHECK(retain->type()->id() == arrow::Type::TIMESTAMP);
  CHECK(retain->null_count() == 3);
  CHECK(table->GetColumnByName("size")->null_count() == 0);
}
