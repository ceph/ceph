// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <catch2/catch_all.hpp>

#include "rgw/rgw_inventory_manifest.h"

#include <filesystem>
#include <fstream>
#include <string>

using namespace rgw::inventory;
namespace fs = std::filesystem;

namespace {

struct TempTextFile {
  std::string path;
  explicit TempTextFile(const std::string& content) {
    path = (fs::temp_directory_path() /
            ("rgw_inv_manifest_test_" + std::to_string(::getpid()) + "_" +
             std::to_string(rand()) + ".txt")).string();
    std::ofstream f(path, std::ios::binary);
    f << content;
  }
  ~TempTextFile() { std::error_code ec; fs::remove(path, ec); }
};

FieldSelection all_fields()
{
  FieldSelection s;
  s.version_id = s.is_latest = s.is_delete_marker = true;
  s.size = s.last_modified = s.etag = s.storage_class = true;
  s.is_multipart_uploaded = s.replication_status = s.encryption_status = true;
  s.object_lock_retain_until_date = s.object_lock_mode = true;
  s.object_lock_legal_hold_status = true;
  s.intelligent_tiering_access_tier = s.bucket_key_status = true;
  return s;
}

} // anonymous namespace

TEST_CASE("md5_of_string matches RFC 1321 test vectors", "[inventory][manifest][md5]")
{
  CHECK(md5_of_string("") == "d41d8cd98f00b204e9800998ecf8427e");
  CHECK(md5_of_string("abc") == "900150983cd24fb0d6963f7d28e17f72");
}

TEST_CASE("md5_of_file matches md5_of_string", "[inventory][manifest][md5]")
{
  const std::string content = "hello inventory";
  TempTextFile f(content);
  CHECK(md5_of_file(f.path) == md5_of_string(content));
  CHECK(md5_of_file("/nonexistent/path.parquet") == "");
}

TEST_CASE("build_file_schema matches the real AWS fileSchema", "[inventory][manifest][aws]")
{
  // Exact string from the AWS-generated fixture (src/test/rgw/fixtures/manifest.json)
  const std::string expected =
    "message s3.inventory {"
    "  required binary bucket (STRING);"
    "  required binary key (STRING);"
    "  optional binary version_id (STRING);"
    "  optional boolean is_latest;"
    "  optional boolean is_delete_marker;"
    "  optional int64 size;"
    "  optional int64 last_modified_date (TIMESTAMP(MILLIS,true));"
    "  optional binary e_tag (STRING);"
    "  optional binary storage_class (STRING);"
    "  optional boolean is_multipart_uploaded;"
    "  optional binary replication_status (STRING);"
    "  optional binary encryption_status (STRING);"
    "  optional int64 object_lock_retain_until_date (TIMESTAMP(MILLIS,true));"
    "  optional binary object_lock_mode (STRING);"
    "  optional binary object_lock_legal_hold_status (STRING);"
    "  optional binary intelligent_tiering_access_tier (STRING);"
    "  optional binary bucket_key_status (STRING);"
    "}";
  CHECK(build_file_schema(all_fields()) == expected);
}

TEST_CASE("build_file_schema omits unselected fields", "[inventory][manifest]")
{
  FieldSelection s;
  s.size = s.last_modified = s.etag = s.storage_class = false;
  CHECK(build_file_schema(s) ==
        "message s3.inventory {"
        "  required binary bucket (STRING);"
        "  required binary key (STRING);"
        "}");
}

TEST_CASE("run timestamp and folder formats", "[inventory][manifest][keys]")
{
  const auto ts = inventory_run_timestamp();
  REQUIRE(ts.size() == 17);
  CHECK(ts[10] == 'T');
  CHECK(ts[16] == 'Z');

  // 2026-01-01 13:45:00 UTC -> day folder, time zeroed
  CHECK(inventory_run_folder(1767275100) == "2026-01-01T00-00Z");
}

TEST_CASE("data key uses the flat data/ layout with unique names", "[inventory][manifest][keys]")
{
  const auto k1 = inventory_data_key("inv", "src", "cfg");
  const auto k2 = inventory_data_key("inv", "src", "cfg");
  CHECK(k1.rfind("inv/src/cfg/data/", 0) == 0);
  CHECK(k1.substr(k1.size() - 8) == ".parquet");
  CHECK(k1 != k2);
  CHECK(inventory_data_key("", "src", "cfg").rfind("src/cfg/data/", 0) == 0);
}

TEST_CASE("manifest and checksum keys nest under the run folder", "[inventory][manifest][keys]")
{
  const auto mk = inventory_manifest_key("inv", "src", "cfg", "2026-01-01T00-00Z");
  CHECK(mk == "inv/src/cfg/2026-01-01T00-00Z/manifest.json");
  CHECK(inventory_checksum_key(mk) == "inv/src/cfg/2026-01-01T00-00Z/manifest.checksum");
  CHECK(inventory_checksum_key("inv/src/cfg/data/x.parquet") ==
        "inv/src/cfg/data/x.parquet");
}

TEST_CASE("Manifest::to_json is deterministic and uses the given timestamp", "[inventory][manifest][json]")
{
  Manifest m;
  m.source_bucket = "rgw-inv-src-hst0052";
  m.destination_bucket_arn = "arn:aws:s3:::rgw-inv-dst-hst0052";
  m.creation_timestamp_ms = 1786323600000LL;
  m.file_schema = build_file_schema(all_fields());
  m.files.push_back({"inv/rgw-inv-src-hst0052/full-inventory/data/"
                     "04fa16ce-6b89-49d7-83ad-f3424793577b.parquet",
                     4550, "35659a742aaba880f539821997e8fb27"});

  const auto json = m.to_json();
  CHECK(json == m.to_json());
  CHECK(json.find("\"creationTimestamp\": \"1786323600000\"") != std::string::npos);
  CHECK(json.find("\"sourceBucket\": \"rgw-inv-src-hst0052\"") != std::string::npos);
  CHECK(json.find("\"version\": \"2016-11-30\"") != std::string::npos);
  CHECK(json.find("\"size\": 4550") != std::string::npos);
}

TEST_CASE("Manifest::to_json separates multiple files correctly", "[inventory][manifest][json]")
{
  Manifest m;
  m.creation_timestamp_ms = 1;
  m.files.push_back({"key1", 1, "c1"});
  m.files.push_back({"key2", 2, "c2"});
  const auto json = m.to_json();
  CHECK(json.find("\"c1\"\n    },") != std::string::npos);
  CHECK(json.find("\"c2\"\n    },") == std::string::npos);
}
