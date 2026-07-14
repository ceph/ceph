// -*- mode:C++; tab-width:8; c-basic-offset:2 -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <catch2/catch_all.hpp>

#include "rgw/rgw_inventory_parquet.h"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>

#include <filesystem>
#include <string>

using namespace rgw::inventory;
namespace fs = std::filesystem;

namespace {

struct TempFile {
  std::string path;
  TempFile() {
    path = (fs::temp_directory_path() /
            ("rgw_inv_test_" + std::to_string(::getpid()) + "_" +
             std::to_string(rand()) + ".parquet")).string();
  }
  ~TempFile() { std::error_code ec; fs::remove(path, ec); }
};

std::shared_ptr<arrow::Table> read_back(const std::string& path)
{
  auto infile_res = arrow::io::ReadableFile::Open(path);
  REQUIRE(infile_res.ok());

  parquet::arrow::FileReaderBuilder builder;
  auto st = builder.Open(*infile_res);
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

InventoryEntry sample_entry(int i)
{
  InventoryEntry e;
  e.bucket = "test-bucket";
  e.key    = "prefix/object-" + std::to_string(i);
  e.size   = 1024 * i;
  e.last_modified_ms = 1720000000000LL + i * 1000;
  e.etag         = "\"d41d8cd98f00b204e9800998ecf8427e\"";
  e.storage_class = "STANDARD";
  return e;
}

} // anonymous namespace

// -----------------------------------------------------------------------
TEST_CASE("schema contains required and selected fields", "[inventory][schema]")
{
  FieldSelection sel; // defaults: size, last_modified, etag, storage_class
  auto schema = build_inventory_schema(sel);

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

// -----------------------------------------------------------------------
TEST_CASE("versioned-bucket schema includes version fields", "[inventory][schema]")
{
  FieldSelection sel;
  sel.version_id       = true;
  sel.is_latest        = true;
  sel.is_delete_marker = true;
  auto schema = build_inventory_schema(sel);
  CHECK(schema->GetFieldByName("version_id") != nullptr);
  CHECK(schema->GetFieldByName("is_latest") != nullptr);
  CHECK(schema->GetFieldByName("is_delete_marker") != nullptr);
}

// -----------------------------------------------------------------------
TEST_CASE("write a small inventory without crash", "[inventory][write]")
{
  TempFile tmp;
  FieldSelection sel;

  ParquetInventoryWriter w(tmp.path, sel);
  for (int i = 0; i < 10; ++i) {
    w.append(sample_entry(i));
  }
  // close() must not throw
  REQUIRE_NOTHROW(w.close());
  CHECK(w.rows_written() == 10);

  // file must exist and be non-empty
  std::error_code ec;
  auto sz = fs::file_size(tmp.path, ec);
  REQUIRE(!ec);
  CHECK(sz > 0);
}

// -----------------------------------------------------------------------
TEST_CASE("write and read back a small inventory", "[inventory][roundtrip]")
{
  TempFile tmp;
  FieldSelection sel;

  {
    ParquetInventoryWriter w(tmp.path, sel);
    for (int i = 0; i < 100; ++i) w.append(sample_entry(i));
    REQUIRE_NOTHROW(w.close());
    CHECK(w.rows_written() == 100);
  }

  auto table = read_back(tmp.path);
  REQUIRE(table != nullptr);
  CHECK(table->num_rows() == 100);
  CHECK(table->num_columns() == 6);

  // spot-check key column
  int key_idx = table->schema()->GetFieldIndex("key");
  REQUIRE(key_idx >= 0);
  auto key_col = std::static_pointer_cast<arrow::StringArray>(
      table->column(key_idx)->chunk(0));
  CHECK(key_col->GetString(0)  == "prefix/object-0");
  CHECK(key_col->GetString(99) == "prefix/object-99");
}

// -----------------------------------------------------------------------
TEST_CASE("null optional values are preserved", "[inventory][nulls]")
{
  TempFile tmp;
  FieldSelection sel;

  {
    ParquetInventoryWriter w(tmp.path, sel);
    InventoryEntry e;
    e.bucket = "b";
    e.key    = "k";
    // all optionals left empty — size, etag, storage_class will be null
    REQUIRE_NOTHROW(w.append(e));
    REQUIRE_NOTHROW(w.close());
  }

  auto table = read_back(tmp.path);
  REQUIRE(table != nullptr);
  int size_idx = table->schema()->GetFieldIndex("size");
  REQUIRE(size_idx >= 0);
  CHECK(table->column(size_idx)->null_count() == 1);
}
