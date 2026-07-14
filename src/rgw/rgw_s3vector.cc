// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_s3vector.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "common/dout.h"
#include "common/ceph_context.h"
#include <arrow/type_fwd.h>
#include <fmt/format.h>
#include "lancedb.h"
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <algorithm>
#include <charconv>
#include <cmath>
#include <set>
#include "rgw_s3vector_background.h"
#include "rgw_s3vector_filter.h"
#include "rgw/rgw_sal.h"

#ifdef WITH_RADOSGW_LANCEDB
#include "lancedb_rgw_store.h"
#endif

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {

  struct LanceDBSessionConnHandle {
    std::shared_ptr<const LanceDBSession> session_keepalive;
    LanceDBConnection* conn = nullptr;

    explicit operator bool() const {
      return conn != nullptr;
    }
  };

  struct LanceDBSessionTableHandle {
    LanceDBSessionConnHandle conn_handle;
    LanceDBTable* table = nullptr;

    explicit operator bool() const {
      return table != nullptr;
    }
  };

  // convert LanceDBError to linux error codes
  int lancedb_error_to_errno(LanceDBError err) {
    switch (err) {
      case LANCEDB_SUCCESS:
        return 0;
      case LANCEDB_INVALID_ARGUMENT:
      case LANCEDB_INVALID_TABLE_NAME:
      case LANCEDB_INVALID_INPUT:
      case LANCEDB_SCHEMA:
      case LANCEDB_ARROW:
        return -EINVAL;
      case LANCEDB_TABLE_NOT_FOUND:
      case LANCEDB_DATABASE_NOT_FOUND:
      case LANCEDB_INDEX_NOT_FOUND:
      case LANCEDB_EMBEDDING_FUNCTION_NOT_FOUND:
        return -ENOENT;
      case LANCEDB_DATABASE_ALREADY_EXISTS:
      case LANCEDB_TABLE_ALREADY_EXISTS:
        return -EEXIST;
      case LANCEDB_NOT_SUPPORTED:
        return -EOPNOTSUPP;
      case LANCEDB_RETRY:
        return -EAGAIN;
      case LANCEDB_TIMEOUT:
        return -EBUSY;
      case LANCEDB_CREATE_DIR:
      case LANCEDB_RUNTIME:
      case LANCEDB_OBJECT_STORE:
      case LANCEDB_LANCE:
      case LANCEDB_HTTP:
      case LANCEDB_OTHER:
      case LANCEDB_NAMESPACE:
      case LANCEDB_UNKNOWN:
        return -EIO;
    }
    return -EIO;
  }

  // Create a LanceDB session with RGW SAL provider.
  // Returns a new session on success, nullptr on failure.
  LanceDBSession* create_sal_session(const DoutPrefixProvider* dpp,
      rgw::sal::Driver* driver,
      const void* options) {
#ifdef WITH_RADOSGW_LANCEDB
    if (!driver) {
      ldpp_dout(dpp, 1) << "ERROR: SAL backend requires a valid driver" << dendl;
      return nullptr;
    }

    LanceDBObjectStoreRegistry* registry = lancedb_registry_new();
    if (!registry) {
      ldpp_dout(dpp, 1) << "ERROR: failed to create LanceDB registry" << dendl;
      return nullptr;
    }

    LanceDBObjectStoreProvider* provider = rgw_lancedb_create_provider(driver, dpp);
    if (!provider) {
      ldpp_dout(dpp, 1) << "ERROR: failed to create RGW LanceDB provider" << dendl;
      lancedb_registry_free(registry);
      return nullptr;
    }

    char* reg_error = nullptr;
    if (lancedb_registry_insert_provider(registry, "rgw", provider, &reg_error) != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: failed to insert provider: "
                        << (reg_error ? reg_error : "unknown") << dendl;
      if (reg_error)
        lancedb_free_string(reg_error);
      lancedb_registry_free(registry);
      return nullptr;
    }

    LanceDBSession* session = lancedb_session_new_with_registry(
        static_cast<const LanceDBSessionOptions*>(options), registry);
    if (!session) {
      ldpp_dout(dpp, 1) << "ERROR: failed to create SAL session" << dendl;
      lancedb_registry_free(registry);
    }
    return session;
#else
    ldpp_dout(dpp, 1) << "ERROR: no LanceDB SAL backend support" << dendl;
    return nullptr;
#endif
  }

  // utility functions for connection creation and opening table

  LanceDBConnection* connect(DoutPrefixProvider* dpp, const std::string& vector_bucket_name) {
    CephContext* cct = dpp->get_cct();
    const auto& conf = cct->_conf;
    const std::string backend_str = conf.get_val<std::string>("rgw_s3vector_backend");
    BackendType backend_type;
    if (int ret = get_backend_type(backend_str, backend_type); ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector unrecognized backend type: " << backend_str << dendl;
      return nullptr;
    }

    std::string uri;
    LanceDBConnectBuilder* builder = nullptr;

    if (is_local_backend(backend_type)) {
      // Local filesystem backend (default)
      const std::string local_path = conf.get_val<std::string>("rgw_s3vector_local_path");
      if (local_path.empty()) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector local backend requires "
                          << "rgw_s3vector_local_path to be configured" << dendl;
        return nullptr;
      }
      uri = fmt::format("{}/{}", local_path, vector_bucket_name);
      builder = lancedb_connect(uri.c_str());
      if (!builder) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create connection builder for: " << uri << dendl;
        return nullptr;
      }
      ldpp_dout(dpp, 10) << "INFO: s3vector connecting to local backend: " << uri << dendl;
    } else if (is_sal_backend(backend_type)) {
      // SAL backend requires a regular S3 bucket with the same name as the
      // vector bucket to exist before vector operations.
      rgw::sal::Driver* driver = rgw::s3vector::get_driver();
      // TODO: disable cache for short-lived sessions once LanceDB supports it
      // (currently 0 = default cache size, no way to disable)
      LanceDBSession* session = create_sal_session(dpp, driver);
      if (!session) {
        return nullptr;
      }

      uri = fmt::format("rgw://{}/", vector_bucket_name);
      builder = lancedb_connect(uri.c_str());
      if (!builder) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create connection builder for: " << uri << dendl;
        lancedb_session_free(session);
        return nullptr;
      }

      LanceDBConnectBuilder* new_builder = lancedb_connect_builder_session(builder, session);
      if (!new_builder) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to attach session to connection builder" << dendl;
        lancedb_connect_builder_free(builder);
        lancedb_session_free(session);
        return nullptr;
      }
      builder = new_builder;

      ldpp_dout(dpp, 10) << "INFO: s3vector connecting to SAL backend: " << uri << dendl;
    } else { // S3 backend

      // Use vector bucket name directly as the S3 bucket name.
      // A regular S3 bucket with the same name as the vector bucket must exist
      // at the backend.
      uri = fmt::format("s3://{}/", vector_bucket_name);
      builder = lancedb_connect(uri.c_str());
      if (!builder) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create connection builder for: " << uri << dendl;
        return nullptr;
      }

      const std::string s3_endpoint = conf.get_val<std::string>("rgw_s3vector_s3_endpoint");
      const std::string s3_region = conf.get_val<std::string>("rgw_s3vector_s3_region");
      const bool s3_allow_http = conf.get_val<bool>("rgw_s3vector_s3_allow_http");

      // set storage options
      auto set_storage_option = [&](const char* key, const char* value) -> bool {
        LanceDBConnectBuilder* new_builder = lancedb_connect_builder_storage_option(builder, key, value);
        if (!new_builder) {
          ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set storage option: " << key << dendl;
          lancedb_connect_builder_free(builder);
          builder = nullptr;
          return false;
        }
        builder = new_builder;
        return true;
      };

      if (!s3_endpoint.empty()) {
        if (!set_storage_option("endpoint", s3_endpoint.c_str())) {
          return nullptr;
        }
      }

      if (!s3_region.empty()) {
        if (!set_storage_option("aws_region", s3_region.c_str())) {
          return nullptr;
        }
      }

      // TODO: get credentials..
      // for now, when credentials are not set, underlying LanceDB S3_ObjectStore
      // provider falls back to the standard AWS credential provider chain
      // (env vars, instance profile, ~/.aws/credentials, etc.)

      if (s3_allow_http) {
        if (!set_storage_option("allow_http", "true")) {
          return nullptr;
        }
      }

      ldpp_dout(dpp, 10) << "INFO: s3vector connecting to S3 backend: " << uri
                         << " endpoint=" << s3_endpoint << " region=" << s3_region << dendl;
    }

    LanceDBConnection* conn = lancedb_connect_builder_execute(builder);
    if (!conn) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to connect to: " << uri << dendl;
    }
    return conn;
  }

  LanceDBSessionConnHandle connect_with_session_handle(DoutPrefixProvider* dpp, const std::string& vector_bucket_name) {
    CephContext* cct = dpp->get_cct();
    const auto& conf = cct->_conf;
    const std::string backend_str = conf.get_val<std::string>("rgw_s3vector_backend");
    BackendType backend_type;
    if (int ret = get_backend_type(backend_str, backend_type); ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector unrecognized backend type: " << backend_str << dendl;
      return {};
    }

    // Try to get session from pool
    auto session_sp = rgw::s3vector::get_session(dpp, vector_bucket_name);
    if (!session_sp) {
      // Session not in pool - trigger async creation and fall back to connect without session
      rgw::s3vector::notify_session_create(dpp, vector_bucket_name);
      return LanceDBSessionConnHandle{
        .conn = connect(dpp, vector_bucket_name)
      };
    }

    // Build URI based on backend type
    std::string uri;
    if (is_local_backend(backend_type)) {
      const std::string local_path = conf.get_val<std::string>("rgw_s3vector_local_path");
      uri = fmt::format("{}/{}", local_path, vector_bucket_name);
    } else if (is_sal_backend(backend_type)) {
      uri = fmt::format("rgw://{}/", vector_bucket_name);
    } else {
      uri = fmt::format("s3://{}/", vector_bucket_name);
    }

    LanceDBConnectBuilder* builder = lancedb_connect(uri.c_str());
    if (!builder) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create connection builder for: " << uri << dendl;
      return LanceDBSessionConnHandle{
        .conn = connect(dpp, vector_bucket_name)
      };
    }

    builder = lancedb_connect_builder_session(builder, session_sp.get());
    LanceDBConnection* conn = lancedb_connect_builder_execute(builder);
    if (!conn) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to connect using session to: " << uri << " falling back to connect without session" << dendl;
      return LanceDBSessionConnHandle{
        .conn = connect(dpp, vector_bucket_name)
      };
    }

    return LanceDBSessionConnHandle{
      .session_keepalive = std::move(session_sp),
      .conn = conn
    };
  }

  LanceDBTable* open_table(DoutPrefixProvider* dpp, const std::string& vector_bucket_name, const std::string& index_name) {
    LanceDBConnection* conn = connect(dpp, vector_bucket_name);
    if (!conn) {
      return nullptr;
    }
    LanceDBTable* table = lancedb_connection_open_table(conn, index_name.c_str());
    if (!table) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to open index: " << index_name << " in: " << vector_bucket_name << dendl;
      lancedb_connection_free(conn);
      return nullptr;
    }
    return table;
  }

  LanceDBSessionTableHandle open_table_with_session_handle(DoutPrefixProvider* dpp, const std::string& vector_bucket_name, const std::string& index_name) {
    auto conn_handle = connect_with_session_handle(dpp, vector_bucket_name);
    if (!conn_handle) {
      return {};
    }
    LanceDBTable* table = lancedb_connection_open_table(conn_handle.conn, index_name.c_str());
    if (!table) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to open index: " << index_name << " in: " << vector_bucket_name << dendl;
      lancedb_connection_free(conn_handle.conn);
      return {};
    }
    return LanceDBSessionTableHandle{
      .conn_handle = std::move(conn_handle),
      .table = table
    };
  }
  
  // get creation time from the first version of a table
  // returns 0 on failure
  uint64_t get_table_creation_time(const LanceDBTable* table, DoutPrefixProvider* dpp) {
    LanceDBVersion* versions = nullptr;
    size_t count = 0;
    char* error_message = nullptr;
    if (const auto result = lancedb_table_list_versions(table, &versions, nullptr, &count, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to list table versions: " << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return 0;
    }
    if (count == 0) {
      lancedb_free_versions(versions, count);
      return 0;
    }
    const uint64_t creation_time = versions[0].timestamp_seconds;
    lancedb_free_versions(versions, count);
    return creation_time;
  }

  // utility functions for JSON encoding/decoding

  template <typename T>
  void decode_from_chars(T& val, std::string_view sv) {
    const char* start = sv.data();
    const char* end = start + sv.length();

    const auto result = std::from_chars(start, end, val);

    if (result.ec == std::errc::invalid_argument) {
      throw JSONDecoder::err("failed to parse number");
    }
    if (result.ec == std::errc::result_out_of_range) {
      throw JSONDecoder::err("out of range number");
    }
    if (result.ptr != end) {
      throw JSONDecoder::err("trailing characters after number");
    }
  }

  void decode_json(const char* field_name, VectorData& data, JSONObj* obj) {
    data.clear();
    auto it = obj->find(field_name);
    if (it.end()) {
      throw JSONDecoder::err(std::string("missing field: ") + field_name);
    }
    auto arr_it = (*it)->find("float32");
    for (auto value_it = (*arr_it)->find_first(); !value_it.end(); ++value_it) {
      float value;
      decode_from_chars(value, (*value_it)->get_data());
      data.push_back(value);
    }
  }

  void encode_json(const char* field_name, const VectorData& data, ceph::Formatter* f) {
    f->open_object_section(field_name);
    f->open_array_section("float32");
    for (auto value : data) {
      f->dump_float("", value);
    }
    f->close_section();
    f->close_section();
  }

  void verify_name(const std::string& name) {
    if (name.length() < 3 || name.length() > 63) {
      throw JSONDecoder::err(fmt::format("'{}' length ({}) must be between 3 and 63 characters", name, name.length()));
    }
  }

  void decode_name_or_arn(const char* name_field, const char* arn_field, std::string& name, boost::optional<rgw::ARN>& arn, JSONObj* obj) {
    std::string str_arn;
    JSONDecoder::decode_json(arn_field, str_arn, obj);
    JSONDecoder::decode_json(name_field, name, obj);
    if (str_arn.empty() && name.empty()) {
      throw JSONDecoder::err(fmt::format("either {} or {} must be specified", name_field, arn_field));
    }
    if (!str_arn.empty() && !name.empty()) {
      throw JSONDecoder::err(fmt::format("only one of {} and {} must be specified", name_field, arn_field));
    }
    if (!str_arn.empty()) {
      arn = rgw::ARN::parse(str_arn);
      if (!arn) throw JSONDecoder::err(fmt::format("failed to parse ARN {}", str_arn));
    }
  }

  void decode_vector_bucket_name(std::string& vector_bucket_name, boost::optional<rgw::ARN>& arn, JSONObj* obj) {
    decode_name_or_arn("vectorBucketName", "vectorBucketArn", vector_bucket_name, arn, obj);
    if (arn) {
      ceph_assert(vector_bucket_name.empty());
      constexpr std::string_view prefix = "bucket/";
      std::string_view resource{arn->resource};
      if (!resource.starts_with(prefix)) {
        throw JSONDecoder::err(
            fmt::format("invalid vector bucket ARN. expected: 'bucket/<bucket_name>' got: {}", resource));
      }
      vector_bucket_name = resource.substr(prefix.size());
    }
    verify_name(vector_bucket_name);
  }

  void decode_index_name(std::string& vector_bucket_name, std::string& index_name, JSONObj* obj) {
    boost::optional<rgw::ARN> arn;
    decode_name_or_arn("indexName", "indexArn", index_name, arn, obj);
    if (arn) {
      ceph_assert(index_name.empty());
      constexpr std::string_view bucket_prefix = "bucket/";
      constexpr std::string_view index_prefix = "/index/";
      std::string_view resource{arn->resource};
      if (!resource.starts_with(bucket_prefix)) {
        throw JSONDecoder::err(
            fmt::format("invalid index ARN. expected: 'bucket/<bucket_name>/index/<index_name>' got: {}", resource));
      }
      // Remove "bucket/" prefix
      resource.remove_prefix(bucket_prefix.size());
      // Find "/index/" separator
      auto index_pos = resource.find(index_prefix);
      if (index_pos == std::string_view::npos) {
        throw JSONDecoder::err(
            fmt::format("invalid index ARN. expected: 'bucket/<bucket_name>/index/<index_name>' got: {}", resource));
      }
      vector_bucket_name = resource.substr(0, index_pos);
      index_name = resource.substr(index_pos + index_prefix.size());
    } else {
      JSONDecoder::decode_json("vectorBucketName", vector_bucket_name, obj, true);
    }
    verify_name(index_name);
    verify_name(vector_bucket_name);
  }

  template<typename T>
  void log_configuration(DoutPrefixProvider* dpp, const std::string& op_name, const T& configuration) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector " << op_name << " with: " << ss.str() << dendl;
  }

  // index operations: create, delete, get, list
  //////////////////////////////////////////////

  // create index

  static constexpr const char* distance_metric_key[] = {"distance_metric"};

  const char* distance_metric_to_string(DistanceMetric metric) {
    switch (metric) {
      case DistanceMetric::COSINE: return "cosine";
      case DistanceMetric::EUCLIDEAN: return "euclidean";
      case DistanceMetric::UNKNOWN: return "unknown";
    }
    return "unknown";
  }

  DistanceMetric string_to_distance_metric(const std::string& str) {
    if (str == "cosine") {
      return DistanceMetric::COSINE;
    } else if (str == "euclidean") {
      return DistanceMetric::EUCLIDEAN;
    }
    return DistanceMetric::UNKNOWN;
  }

  int set_table_distance_metric(const LanceDBTable* table, DistanceMetric metric, DoutPrefixProvider* dpp) {
    const char* value = distance_metric_to_string(metric);
    char* error_message = nullptr;
    if (const auto result = lancedb_table_set_metadata(table, distance_metric_key, &value, 1, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set distance_metric metadata: " << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return lancedb_error_to_errno(result);
    }
    return 0;
  }

  void filterable_metadata_key_t::dump(ceph::Formatter* f) const {
    ::encode_json("name", name, f);
    switch (type) {
      case FilterableMetadataType::STRING: ::encode_json("type", "String", f); break;
      case FilterableMetadataType::NUMBER: ::encode_json("type", "Number", f); break;
      case FilterableMetadataType::BOOLEAN: ::encode_json("type", "Boolean", f); break;
      case FilterableMetadataType::STRING_LIST: ::encode_json("type", "StringList", f); break;
      case FilterableMetadataType::NUMBER_LIST: ::encode_json("type", "NumberList", f); break;
      case FilterableMetadataType::BOOLEAN_LIST: ::encode_json("type", "BooleanList", f); break;
    }
    ::encode_json("mustExist", must_exist, f);
  }

  void filterable_metadata_key_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("name", name, obj, true);
    std::string type_str;
    JSONDecoder::decode_json("type", type_str, obj);
    if (type_str.empty() || type_str == "String") {
      type = FilterableMetadataType::STRING;
    } else if (type_str == "Number") {
      type = FilterableMetadataType::NUMBER;
    } else if (type_str == "Boolean") {
      type = FilterableMetadataType::BOOLEAN;
    } else if (type_str == "StringList") {
      type = FilterableMetadataType::STRING_LIST;
    } else if (type_str == "NumberList") {
      type = FilterableMetadataType::NUMBER_LIST;
    } else if (type_str == "BooleanList") {
      type = FilterableMetadataType::BOOLEAN_LIST;
    } else {
      throw JSONDecoder::err(fmt::format("invalid filterable metadata type: '{}'. Must be String, Number, Boolean, StringList, NumberList, or BooleanList", type_str));
    }
    JSONDecoder::decode_json("mustExist", must_exist, obj);
  }

  static constexpr const char* nonfilterable_metadata_key[] = {"nonfilterable_metadata"};

  int set_nonfilterable_metadata(const LanceDBTable* table, const std::vector<std::string>& keys, DoutPrefixProvider* dpp) {
    if (keys.empty()) {
      return 0;
    }
    JSONFormatter f;
    f.open_object_section("");
    ::encode_json("keys", keys, &f);
    f.close_section();
    std::stringstream ss;
    f.flush(ss);
    const auto json_str = ss.str();
    const char* value = json_str.c_str();
    char* error_message = nullptr;
    if (const auto result = lancedb_table_set_metadata(table, nonfilterable_metadata_key, &value, 1, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set " << nonfilterable_metadata_key <<
        " metadata: " << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return lancedb_error_to_errno(result);
    }
    return 0;
  }

  int get_nonfilterable_metadata(const LanceDBTable* table, DoutPrefixProvider* dpp, std::vector<std::string>& non_filterable_metadata_keys) {
    char** keys_out = nullptr;
    char** values_out = nullptr;
    size_t count = 0;
    char* error_message = nullptr;
    if (const auto result = lancedb_table_get_metadata(table, nonfilterable_metadata_key, 1, &keys_out, &values_out, &count, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to get " << nonfilterable_metadata_key <<
        "  metadata: " << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return lancedb_error_to_errno(result);
    }
    if (count > 0) {
      JSONParser parser;
      if (!parser.parse(values_out[0], strlen(values_out[0]))) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to parse nonfilterable metadata JSON" << dendl;
        lancedb_free_metadata(keys_out, values_out, count);
        return -EINVAL;
      }
      try {
        JSONDecoder::decode_json("keys", non_filterable_metadata_keys, &parser);
      } catch (const JSONDecoder::err& e) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to decode nonfilterable metadata JSON: " << e.what() << dendl;
        lancedb_free_metadata(keys_out, values_out, count);
        return -EINVAL;
      }
      lancedb_free_metadata(keys_out, values_out, count);
    }
    return 0;
  }


  DistanceMetric get_distance_metric(const LanceDBTable* table, DoutPrefixProvider* dpp) {
    char** keys_out = nullptr;
    char** values_out = nullptr;
    size_t count = 0;
    char* error_message = nullptr;
    if (const auto result = lancedb_table_get_metadata(table, distance_metric_key, 1, &keys_out, &values_out, &count, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to get distance_metric metadata: " << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return DistanceMetric::UNKNOWN;
    }
    DistanceMetric metric = DistanceMetric::UNKNOWN;
    if (count > 0) {
      metric = string_to_distance_metric(values_out[0]);
    }
    lancedb_free_metadata(keys_out, values_out, count);
    return metric;
  }

  void create_index_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("dataType", data_type, f);
    ::encode_json("dimension", dimension, f);
    ::encode_json("distanceMetric", distance_metric_to_string(distance_metric), f);
    ::encode_json("indexName", index_name, f);
    f->open_object_section("metadataConfiguration");
    ::encode_json("nonFilterableMetadataKeys", non_filterable_metadata_keys, f);
    ::encode_json("filterableMetadataKeys", filterable_metadata_keys, f);
    f->close_section();
    if (vector_bucket_arn) {
      ::encode_json("vectorBucketArn", vector_bucket_arn->to_string(), f);
    } else {
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->close_section();
  }

  void create_index_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("dataType", data_type, obj, true);
    if (data_type != "float32") {
      throw JSONDecoder::err(fmt::format("invalid dataType: {}. Only 'float32' is supported.", data_type));
    }
    JSONDecoder::decode_json("dimension", dimension, obj, true);
    if (dimension < 1 || dimension > 4096) {
      throw JSONDecoder::err(fmt::format("dimension must be between 1 and 4096, got {}", dimension));
    }
    std::string metric_str;
    JSONDecoder::decode_json("distanceMetric", metric_str, obj, true);
    distance_metric = string_to_distance_metric(metric_str);
    if (distance_metric == DistanceMetric::UNKNOWN) {
      throw JSONDecoder::err("invalid distanceMetric: " + metric_str);
    }
    JSONDecoder::decode_json("indexName", index_name, obj, true);
    auto md_it = obj->find("metadataConfiguration");
    if (!md_it.end()) {
      JSONDecoder::decode_json("nonFilterableMetadataKeys", non_filterable_metadata_keys, *md_it);
      JSONDecoder::decode_json("filterableMetadataKeys", filterable_metadata_keys, *md_it);
    }
    decode_vector_bucket_name(vector_bucket_name, vector_bucket_arn, obj);
  }

  void create_index_reply_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("indexArn", index_arn, f);
    f->close_section();
  }

  static constexpr const char* data_field = "data";
  static const std::string data_field_str{data_field};;
  static constexpr const char* key_field = "key";
  static const std::string key_field_str{key_field};;
  static constexpr const char* metadata_field = "metadata";
  static const std::string metadata_field_str{metadata_field};;
  static constexpr const char* distance_field = "_distance";
  static const std::string distance_field_str{distance_field};;
  static constexpr const char* key_columns[] = {key_field};
  static constexpr const char* data_columns[] = {data_field};
  static constexpr const char* table_columns[] = {key_field, data_field};
  static constexpr const char* table_columns_with_metadata[] = {key_field, data_field, metadata_field};
  static constexpr const char* key_and_metadata_columns[] = {key_field, metadata_field};
  static constexpr int num_key_columns = 1;

  std::pair<const char* const*, unsigned long> get_select_columns(bool return_data, bool return_metadata) {
    if (return_data && return_metadata) {
      return {table_columns_with_metadata, 3};
    } else if (return_data) {
      return {table_columns, 2};
    } else if (return_metadata) {
      return {key_and_metadata_columns, 2};
    }
    return {key_columns, 1};
  }

  std::shared_ptr<arrow::DataType> filterable_type_to_arrow(FilterableMetadataType type) {
    switch (type) {
      case FilterableMetadataType::STRING: return arrow::utf8();
      case FilterableMetadataType::NUMBER: return arrow::float64();
      case FilterableMetadataType::BOOLEAN: return arrow::boolean();
      case FilterableMetadataType::STRING_LIST: return arrow::list(arrow::utf8());
      case FilterableMetadataType::NUMBER_LIST: return arrow::list(arrow::float64());
      case FilterableMetadataType::BOOLEAN_LIST: return arrow::list(arrow::boolean());
    }
    return arrow::utf8();
  }

  std::optional<FilterableMetadataType> arrow_to_filterable_type(const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
      case arrow::Type::STRING:
        return FilterableMetadataType::STRING;
      case arrow::Type::DOUBLE:
        return FilterableMetadataType::NUMBER;
      case arrow::Type::BOOL:
        return FilterableMetadataType::BOOLEAN;
      case arrow::Type::LIST: {
        const auto& value_type = std::static_pointer_cast<arrow::ListType>(type)->value_type();
        switch (value_type->id()) {
          case arrow::Type::STRING:
            return FilterableMetadataType::STRING_LIST;
          case arrow::Type::DOUBLE:
            return FilterableMetadataType::NUMBER_LIST;
          case arrow::Type::BOOL:
            return FilterableMetadataType::BOOLEAN_LIST;
          default:
            return std::nullopt;
        }
      }
      default:
        return std::nullopt;
    }
  }

  std::vector<filterable_metadata_key_t> get_filterable_keys_from_schema(const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<filterable_metadata_key_t> keys;
    for (const auto& field : schema->fields()) {
      const auto& name = field->name();
      if (name == key_field || name == data_field || name == metadata_field || name.starts_with('_')) {
        continue;
      }
      if (const auto type = arrow_to_filterable_type(field->type()); type.has_value()) {
        keys.push_back({name, *type, !field->nullable()});
      }
    }
    return keys;
  }

  int create_table_schema(unsigned int dimension, const std::vector<filterable_metadata_key_t>& filterable_keys, DoutPrefixProvider* dpp, ArrowSchema* c_schema) {
    arrow::FieldVector fields = {
      arrow::field(key_field, arrow::utf8()),
      arrow::field(data_field, arrow::fixed_size_list(arrow::float32(), dimension)),
      arrow::field(metadata_field, arrow::utf8())
    };
    for (const auto& fk : filterable_keys) {
      fields.push_back(arrow::field(fk.name, filterable_type_to_arrow(fk.type), !fk.must_exist));
    }
    const auto schema = arrow::schema(fields);
    if (const auto status = arrow::ExportSchema(*schema, c_schema); !status.ok()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to export schema to C ABI: " << status.ToString() << dendl;
      return -EINVAL;
    }
    return 0;
  }

  int get_vector_dimension(const std::string& index_name, const std::shared_ptr<arrow::Schema>& schema, DoutPrefixProvider* dpp, unsigned int& dimension) {
    auto data_f = schema->GetFieldByName(data_field_str);
    if (!data_f) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector schema missing " << data_field_str << " field for index: " << index_name << dendl;
      return -EINVAL;
    }
    if (data_f->type()->id() != arrow::Type::FIXED_SIZE_LIST) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector " << data_field_str << " field is not a FixedSizeList for index: " << index_name << dendl;
      return -EINVAL;
    }
    dimension = std::static_pointer_cast<arrow::FixedSizeListType>(data_f->type())->list_size();
    return 0;
  }

  int import_table_schema(const std::string& index_name, LanceDBTable* table, DoutPrefixProvider* dpp, std::shared_ptr<arrow::Schema>& schema) {
    struct ArrowSchema* c_schema_ptr = nullptr;
    char* error_message = nullptr;
    if (const LanceDBError result = lancedb_table_arrow_schema(
          table,
          reinterpret_cast<FFI_ArrowSchema**>(&c_schema_ptr),
          &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to get schema for index: " << index_name
                        << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      return lancedb_error_to_errno(result);
    }
    auto imported = arrow::ImportSchema(c_schema_ptr);
    if (!imported.ok()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to import schema for index: " << index_name
                        << ". error: " << imported.status().ToString() << dendl;
      lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
      return -EINVAL;
    }
    schema = *imported;
    lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
    return 0;
  }

  int get_vector_dimension(const std::string& index_name, LanceDBTable* table, DoutPrefixProvider* dpp, unsigned int& dimension) {
    std::shared_ptr<arrow::Schema> schema;
    if (int ret = import_table_schema(index_name, table, dpp, schema); ret < 0) {
      return ret;
    }
    return get_vector_dimension(index_name, schema, dpp, dimension);
  }

  int create_index(const create_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y, std::vector<validation_error_t>& errors) {
    log_configuration(dpp, "CreateIndex", configuration);
    LanceDBConnection* conn = connect(dpp, configuration.vector_bucket_name);
    if (!conn) {
      return -EIO;
    }

    // validate metadata key names
    for (unsigned int i = 0; i < configuration.filterable_metadata_keys.size(); ++i) {
      const auto& name = configuration.filterable_metadata_keys[i].name;
      if (name.starts_with('_')) {
        errors.push_back({fmt::format("metadataConfiguration.filterableMetadataKeys[{}].name", i),
            fmt::format("'{}' must not start with an underscore", name)});
        break;
      }
      if (name.find('.') != std::string::npos) {
        errors.push_back({fmt::format("metadataConfiguration.filterableMetadataKeys[{}].name", i),
            fmt::format("'{}' must not contain '.'", name)});
        break;
      }
    }
    if (!errors.empty()) {
      lancedb_connection_free(conn);
      return -EINVAL;
    }
    for (unsigned int i = 0; i < configuration.non_filterable_metadata_keys.size(); ++i) {
      const auto& name = configuration.non_filterable_metadata_keys[i];
      if (name.find('.') != std::string::npos) {
        errors.push_back({fmt::format("metadataConfiguration.nonFilterableMetadataKeys[{}]", i),
            fmt::format("'{}' must not contain '.'", name)});
        break;
      }
    }
    if (!errors.empty()) {
      lancedb_connection_free(conn);
      return -EINVAL;
    }

    // verify no overlap between filterable and non-filterable metadata keys
    if (!configuration.non_filterable_metadata_keys.empty() && !configuration.filterable_metadata_keys.empty()) {
      std::set<std::string> nonfilterable_names(
          configuration.non_filterable_metadata_keys.begin(),
          configuration.non_filterable_metadata_keys.end());
      for (unsigned int i = 0; i < configuration.filterable_metadata_keys.size(); ++i) {
        const auto& name = configuration.filterable_metadata_keys[i].name;
        if (nonfilterable_names.count(name)) {
          errors.push_back({fmt::format("metadataConfiguration.filterableMetadataKeys[{}].name", i),
              fmt::format("'{}' appears in both filterable and non-filterable metadata keys", name)});
        }
      }
      if (!errors.empty()) {
        lancedb_connection_free(conn);
        return -EINVAL;
      }
    }

    struct ArrowSchema c_schema;
    if (int ret = create_table_schema(configuration.dimension, configuration.filterable_metadata_keys, dpp, &c_schema); ret < 0) {
      lancedb_connection_free(conn);
      return ret;
    }
    char* error_message = nullptr;
    LanceDBTable* table = nullptr;
    if (const LanceDBError result = lancedb_table_create(conn, configuration.index_name.c_str(),
          reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
          nullptr, &table, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector creating index: " << configuration.index_name << ", lancedb error code: " << result << ", error: " << error_message << dendl;
      if (result == LANCEDB_SCHEMA || result == LANCEDB_INVALID_INPUT || result == LANCEDB_ARROW || result == LANCEDB_LANCE) {
        errors.push_back({"metadataConfiguration.filterableMetadataKeys", error_message});
        lancedb_free_string(error_message);
        lancedb_connection_free(conn);
        return -EINVAL;
      }
      lancedb_free_string(error_message);
      lancedb_connection_free(conn);
      return lancedb_error_to_errno(result);
    }
    // create the main index on the table (vector index will be created only after vectors are added)
    const LanceDBScalarIndexConfig scalar_config = {
      .replace = 1,                    // replace existing index
      .force_update_statistics = 0     // don't force update statistics
    };
    if (const LanceDBError result = lancedb_table_create_scalar_index(
          table, key_columns, num_key_columns, LANCEDB_INDEX_BTREE, &scalar_config, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector creating scalar index: " << configuration.index_name << "on 'key' columns, error: " << error_message << dendl;
      lancedb_table_free(table);
      lancedb_free_string(error_message);
      lancedb_connection_free(conn);
      return lancedb_error_to_errno(result);
    }
    if (int ret = set_table_distance_metric(table, configuration.distance_metric, dpp); ret < 0) {
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return ret;
    }
    if (int ret = set_nonfilterable_metadata(table, configuration.non_filterable_metadata_keys, dpp); ret < 0) {
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return ret;
    }
    lancedb_table_free(table);
    lancedb_connection_free(conn);
    return 0;
  }

  // delete index

  void delete_index_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (index_arn) {
      ::encode_json("indexArn", index_arn->to_string(), f);
    } else {
      ::encode_json("indexName", index_name, f);
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->close_section();
  }

  void delete_index_t::decode_json(JSONObj* obj) {
    decode_index_name(vector_bucket_name, index_name, obj);
  }

  int delete_index(const delete_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "DeleteIndex", configuration);
    LanceDBConnection* conn = connect(dpp, configuration.vector_bucket_name);
    if (!conn) {
      return -EIO;
    }
    char* error_message;
    if (const LanceDBError result = lancedb_connection_drop_table(conn,
          configuration.index_name.c_str(), nullptr, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to delete index: " << configuration.index_name << ". error: " << error_message  << dendl;
      lancedb_free_string(error_message);
      lancedb_connection_free(conn);
      return lancedb_error_to_errno(result);
    }
    // we are not failing the operation if we cannot notify the background process on index removal
    notify_index_remove(dpp, configuration.vector_bucket_name, configuration.index_name);
    return 0;
  }

  // get index

  void get_index_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (index_arn) {
      ::encode_json("indexArn", index_arn->to_string(), f);
    } else {
      ::encode_json("indexName", index_name, f);
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->close_section();
  }

  void get_index_t::decode_json(JSONObj* obj) {
    decode_index_name(vector_bucket_name, index_name, obj);
  }

  void get_index_reply_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    f->open_object_section("index");
    ::encode_json("creationTime", creation_time, f);
    ::encode_json("dataType", data_type, f);
    ::encode_json("dimension", dimension, f);
    ::encode_json("distanceMetric", distance_metric_to_string(distance_metric), f);
    ::encode_json("indexArn", index_arn, f);
    ::encode_json("indexName", index_name, f);
    f->open_object_section("metadataConfiguration");
    ::encode_json("nonFilterableMetadataKeys", non_filterable_metadata_keys, f);
    ::encode_json("filterableMetadataKeys", filterable_metadata_keys, f);
    f->close_section();
    ::encode_json("vectorBucketName", vector_bucket_name, f);
    f->close_section();
    f->close_section();
  }


  int get_index(const get_index_t& configuration, const std::string& region, const std::string& account, DoutPrefixProvider* dpp, optional_yield y, get_index_reply_t& reply) {
    log_configuration(dpp, "GetIndex", configuration);
    LanceDBConnection* conn = connect(dpp, configuration.vector_bucket_name);
    if (!conn) {
      return -EIO;
    }
    LanceDBTable* table = lancedb_connection_open_table(conn, configuration.index_name.c_str());
    if (table == nullptr) {
      lancedb_connection_free(conn);
      return -ENOENT;
    }

    std::shared_ptr<arrow::Schema> schema;
    if (int ret = import_table_schema(configuration.index_name, table, dpp, schema); ret < 0) {
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return ret;
    }
    reply.dimension = 0;
    if (int ret = get_vector_dimension(configuration.index_name, schema, dpp, reply.dimension); ret < 0) {
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return ret;
    }
    if (int ret = get_nonfilterable_metadata(table, dpp, reply.non_filterable_metadata_keys); ret < 0) {
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return ret;
    }
    reply.filterable_metadata_keys = get_filterable_keys_from_schema(schema);

    reply.data_type = "float32";
    reply.distance_metric = get_distance_metric(table, dpp);
    reply.index_name = configuration.index_name;
    reply.vector_bucket_name = configuration.vector_bucket_name;

    if (configuration.index_arn) {
      reply.index_arn = configuration.index_arn->to_string();
    } else {
      reply.index_arn = index_arn(
            region,
            account,
            configuration.vector_bucket_name,
            configuration.index_name).to_string();
    }

    reply.creation_time = get_table_creation_time(table, dpp);
    lancedb_table_free(table);
    lancedb_connection_free(conn);
    return 0;
  }

  // list indexes

  void list_indexes_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("maxResults", max_results, f);
    ::encode_json("nextToken", next_token, f);
    ::encode_json("prefix", prefix, f);
    if (vector_bucket_arn) {
      ::encode_json("vectorBucketArn", vector_bucket_arn->to_string(), f);
    } else {
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->close_section();
  }

  void list_indexes_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("maxResults", max_results, default_max_results, obj);
    JSONDecoder::decode_json("nextToken", next_token, obj);
    JSONDecoder::decode_json("prefix", prefix, obj);
    decode_vector_bucket_name(vector_bucket_name, vector_bucket_arn, obj);

    if (max_results < 1 || max_results > 500) {
      throw JSONDecoder::err(fmt::format("maxResults must be between 1 and 500, got {}", max_results));
    }

    if (!next_token.empty() && (next_token.length() < 1 || next_token.length() > 512)) {
      throw JSONDecoder::err(fmt::format("nextToken length must be between 1 and 512, got {}", next_token.length()));
    }

    if (!prefix.empty() && (prefix.length() < 1 || prefix.length() > 63)) {
      throw JSONDecoder::err(fmt::format("prefix length must be between 1 and 63, got {}", prefix.length()));
    }
  }

  void index_summary_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("creationTime", creation_time, f);
    ::encode_json("indexArn", index_arn, f);
    ::encode_json("indexName", index_name, f);
    ::encode_json("vectorBucketName", vector_bucket_name, f);
    f->close_section();
  }

  void index_summary_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("creationTime", creation_time, obj);
    JSONDecoder::decode_json("indexArn", index_arn, obj);
    JSONDecoder::decode_json("indexName", index_name, obj);
    JSONDecoder::decode_json("vectorBucketName", vector_bucket_name, obj);
  }

  void list_indexes_reply_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    f->open_array_section("indexes");
    for (const auto& index : indexes) {
      index.dump(f);
    }
    f->close_section();
    ::encode_json("nextToken", next_token, f);
    f->close_section();
  }

  int list_indexes(const list_indexes_t& configuration, DoutPrefixProvider* dpp, optional_yield y, list_indexes_reply_t& reply) {
    log_configuration(dpp, "ListIndexes", configuration);
    LanceDBConnection* conn = connect(dpp, configuration.vector_bucket_name);
    if (!conn) {
      return -EIO;
    }
    LanceDBTableNamesBuilder* builder = lancedb_connection_table_names_builder(conn);
    if (!builder) {
      lancedb_connection_free(conn);
      return -EIO;
    }
    if (builder = lancedb_table_names_builder_limit(builder, configuration.max_results); !builder) {
      lancedb_connection_free(conn);
      return -EIO;
    }
    if (builder = lancedb_table_names_builder_start_after(builder, configuration.next_token.c_str()); !builder) {
      lancedb_connection_free(conn);
      return -EIO;
    }
    char** table_names;
    size_t name_count;
    char* error_message;
    if (const LanceDBError result = lancedb_table_names_builder_execute(builder, &table_names, &name_count, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector listing indexes of: " << configuration.vector_bucket_name << ", error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_connection_free(conn);
      return lancedb_error_to_errno(result);
    }
    const bool has_prefix = configuration.prefix.empty();
    ldpp_dout(dpp, 20) << "INFO: s3vector listing: " << configuration.vector_bucket_name << ", found: " << name_count << " indexes" << dendl;
    for (size_t i = 0; i < name_count; i++) {
      // TODO: once/if this is resolved: https://github.com/lancedb/lancedb/issues/2895 we can prefix filtering at the builder level
      std::string_view name{table_names[i]};
      if (has_prefix) {
        if (!name.starts_with(configuration.prefix)) {
          ldpp_dout(dpp, 20) << "INFO: on s3vector listing, index: " << name << " is is filterted out" << dendl;
          continue;
        }
      }
      ceph_assert(configuration.vector_bucket_arn);
      uint64_t creation_time = 0;
      LanceDBTable* table = lancedb_connection_open_table(conn, table_names[i]);
      if (table) {
        creation_time = get_table_creation_time(table, dpp);
        lancedb_table_free(table);
      }
      reply.indexes.emplace_back(
          creation_time,
          index_arn(
            configuration.vector_bucket_arn->region,
            configuration.vector_bucket_arn->account,
            configuration.vector_bucket_name,
            name).to_string(),
          std::string(name),
          configuration.vector_bucket_name
        );
        ldpp_dout(dpp, 20) << "INFO: on s3vector listing, index: " << name << " is added to list" << dendl;
    }
    if (name_count > 0) {
      reply.next_token = table_names[name_count-1];
    }
    lancedb_free_table_names(table_names, name_count);
    lancedb_connection_free(conn);
    return 0;
  }

  // vector bucket  operations: create, delete, get, list, put/get/delete policy
  //////////////////////////////////////////////////////////////////////////////

  // delete vector bucket

  void delete_vector_bucket_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (vector_bucket_arn) {
      ::encode_json("vectorBucketArn", vector_bucket_arn->to_string(), f);
    } else {
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->close_section();
  }

  void delete_vector_bucket_t::decode_json(JSONObj* obj) {
    decode_vector_bucket_name(vector_bucket_name, vector_bucket_arn, obj);
  }

  int delete_vector_bucket(const delete_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "DeleteVectorBucket", configuration);
    LanceDBConnection* conn = connect(dpp, configuration.vector_bucket_name);
    if (!conn) {
      return -EIO;
    }
    // force delete TODO: fail deletion if tables exists, unless a "force" flag is used
    char* error_message;
    if (LanceDBError err = lancedb_connection_drop_all_tables(conn, nullptr, &error_message); err != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: failed to drop content of s3vector bucket in: " << configuration.vector_bucket_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_connection_free(conn);
      return -EIO;
    }
    ldpp_dout(dpp, 20) << "INFO: deleting in-memory session (if it exists) for bucket: " << configuration.vector_bucket_name << dendl;
    rgw::s3vector::notify_session_delete(dpp, configuration.vector_bucket_name);
    lancedb_connection_free(conn);
    return 0;
  }

  // delete vector bucket policy

  void delete_vector_bucket_policy_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (vector_bucket_arn) {
      ::encode_json("vectorBucketArn", vector_bucket_arn->to_string(), f);
    } else {
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->close_section();
  }

  void delete_vector_bucket_policy_t::decode_json(JSONObj* obj) {
    decode_vector_bucket_name(vector_bucket_name, vector_bucket_arn, obj);
  }

  int delete_vector_bucket_policy(const delete_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "DeleteVectorBucketPolicy", configuration);
    // policy TODO: implement
    return 0;
  }

  // create vector bucket

  void create_vector_bucket_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("vectorBucketName", vector_bucket_name, f);
    f->close_section();
  }

  void create_vector_bucket_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("vectorBucketName", vector_bucket_name, obj, true);
    verify_name(vector_bucket_name);
  }

  void create_vector_bucket_reply_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("vectorBucketArn", vector_bucket_arn, f);
    f->close_section();
  }

  int create_vector_bucket(const create_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "CreateVectorBucket", configuration);
    auto conn_handle = connect_with_session_handle(dpp, configuration.vector_bucket_name);
    if (!conn_handle) {
      return -EIO;
    }
    LanceDBConnection* conn = conn_handle.conn;
    // verify connectivity by listing table names
    char** table_names;
    size_t name_count;
    char* error_message;
    if (const LanceDBError result = lancedb_connection_table_names(conn, &table_names, &name_count, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to verify connectivity for: " << configuration.vector_bucket_name << ", error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_connection_free(conn);
      return lancedb_error_to_errno(result);
    }
    lancedb_free_table_names(table_names, name_count);
    lancedb_connection_free(conn);
    return 0;
  }

  // get vector bucket

  void get_vector_bucket_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (vector_bucket_arn) {
      ::encode_json("vectorBucketArn", vector_bucket_arn->to_string(), f);
    } else {
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->close_section();
  }

  void get_vector_bucket_t::decode_json(JSONObj* obj) {
    decode_vector_bucket_name(vector_bucket_name, vector_bucket_arn, obj);
  }

  // list vector buckets

  void list_vector_buckets_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("maxResults", max_results, f);
    ::encode_json("nextToken", next_token, f);
    ::encode_json("prefix", prefix, f);
    f->close_section();
  }

  void list_vector_buckets_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("maxResults", max_results, default_max_results, obj);
    JSONDecoder::decode_json("nextToken", next_token, obj);
    JSONDecoder::decode_json("prefix", prefix, obj);

    if (max_results < 1 || max_results > 500) {
      throw JSONDecoder::err(fmt::format("maxResults must be between 1 and 500, got {}", max_results));
    }

    if (!next_token.empty() && (next_token.length() < 1 || next_token.length() > 512)) {
      throw JSONDecoder::err(fmt::format("nextToken length must be between 1 and 512, got {}", next_token.length()));
    }

    if (!prefix.empty() && (prefix.length() < 1 || prefix.length() > 63)) {
      throw JSONDecoder::err(fmt::format("prefix length must be between 1 and 63, got {}", prefix.length()));
    }
  }

  // put vector bucket policy

  void put_vector_bucket_policy_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("policy", policy, f);
    if (vector_bucket_arn) {
      ::encode_json("vectorBucketArn", vector_bucket_arn->to_string(), f);
    } else {
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->close_section();
  }

  void put_vector_bucket_policy_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("policy", policy, obj, true);
    decode_vector_bucket_name(vector_bucket_name, vector_bucket_arn, obj);

    if (policy.empty()) {
      throw JSONDecoder::err("policy must be specified and cannot be empty");
    }
    // policy TODO: validate JSON
  }

  // get vector bucket policy

  void get_vector_bucket_policy_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (vector_bucket_arn) {
      ::encode_json("vectorBucketArn", vector_bucket_arn->to_string(), f);
    } else {
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->close_section();
  }

  void get_vector_bucket_policy_t::decode_json(JSONObj* obj) {
    decode_vector_bucket_name(vector_bucket_name, vector_bucket_arn, obj);
  }

  // vector operations: put/get/delete/query
  //////////////////////////////////////////

  // put vectors

  void vector_item_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (distance) {
      f->dump_float("distance", *distance);
    }
    ::encode_json("key", key, f);
    if (data) {
      rgw::s3vector::encode_json("data", *data, f);
    }
    if (!metadata.empty()) {
      ::encode_json("metadata", metadata, f);
    }
    f->close_section();
  }

  void vector_item_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("key", key, obj, true);
    if (key.empty()) {
      throw JSONDecoder::err("vector key must be specified");
    }
    data.emplace();
    rgw::s3vector::decode_json("data", *data, obj);
    if (data->empty()) {
      throw JSONDecoder::err("vector data cannot be empty");
    }
    JSONDecoder::decode_json("metadata", metadata, obj);
  }

  void put_vectors_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (index_arn) {
      ::encode_json("indexArn", index_arn->to_string(), f);
    } else {
      ::encode_json("indexName", index_name, f);
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    f->open_array_section("vectors");
    for (const auto& vector : vectors) {
      vector.dump(f);
    }
    f->close_section();
    f->close_section();
  }

  void put_vectors_t::decode_json(JSONObj* obj) {
    decode_index_name(vector_bucket_name, index_name, obj);
    JSONDecoder::decode_json("vectors", vectors, obj, true);

    if (vectors.empty() or vectors.size() > 500) {
      throw JSONDecoder::err(fmt::format("vectors array must contain 1-500 items, got {}", vectors.size()));
    }
  }

  int put_vectors(const put_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y, std::vector<validation_error_t>& errors) {
    log_configuration(dpp, "PutVectors", configuration);
    auto table_handle = open_table_with_session_handle(dpp, configuration.vector_bucket_name, configuration.index_name);
    if (!table_handle) {
      return -EIO;
    }
    LanceDBTable* table = table_handle.table;
    LanceDBConnection* conn = table_handle.conn_handle.conn;
    if (configuration.vectors.empty()) {
      ldpp_dout(dpp, 10) << "WARNING: s3vector no vectors provided" << dendl;
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return 0;
    }

    // get the schema, dimension, and filterable keys from the table
    std::shared_ptr<arrow::Schema> schema;
    if (int ret = import_table_schema(configuration.index_name, table, dpp, schema); ret < 0) {
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return ret;
    }
    unsigned int dimension = 0;
    if (int ret = get_vector_dimension(configuration.index_name, schema, dpp, dimension); ret < 0) {
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return ret;
    }
    const auto filterable_keys = get_filterable_keys_from_schema(schema);

    arrow::StringBuilder key_builder;
    arrow::FloatBuilder float_builder;
    arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
        std::make_unique<arrow::FloatBuilder>(),
        dimension);
    arrow::StringBuilder metadata_builder;

    // create builders for filterable columns
    struct FilterableBuilder {
      FilterableMetadataType type;
      std::string name;
      bool must_exist;
      std::unique_ptr<arrow::ArrayBuilder> builder;
    };
    std::vector<FilterableBuilder> filterable_builders;
    for (const auto& fk : filterable_keys) {
      FilterableBuilder fb;
      fb.type = fk.type;
      fb.name = fk.name;
      fb.must_exist = fk.must_exist;
      switch (fk.type) {
        case FilterableMetadataType::STRING:
          fb.builder = std::make_unique<arrow::StringBuilder>();
          break;
        case FilterableMetadataType::NUMBER:
          fb.builder = std::make_unique<arrow::DoubleBuilder>();
          break;
        case FilterableMetadataType::BOOLEAN:
          fb.builder = std::make_unique<arrow::BooleanBuilder>();
          break;
        case FilterableMetadataType::STRING_LIST:
          fb.builder = std::make_unique<arrow::ListBuilder>(
              arrow::default_memory_pool(), std::make_unique<arrow::StringBuilder>());
          break;
        case FilterableMetadataType::NUMBER_LIST:
          fb.builder = std::make_unique<arrow::ListBuilder>(
              arrow::default_memory_pool(), std::make_unique<arrow::DoubleBuilder>());
          break;
        case FilterableMetadataType::BOOLEAN_LIST:
          fb.builder = std::make_unique<arrow::ListBuilder>(
              arrow::default_memory_pool(), std::make_unique<arrow::BooleanBuilder>());
          break;
      }
      filterable_builders.push_back(std::move(fb));
    }

    unsigned int num_rows = 0;
    for (size_t vi = 0; vi < configuration.vectors.size(); ++vi) {
      const auto& vector = configuration.vectors[vi];
      // validate key
      if (vector.key.empty()) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector vector with empty key at index " << vi << dendl;
        errors.push_back({fmt::format("vectors[{}].key", vi), "must not be empty"});
        break;
      }
      // validate data
      if (!vector.data) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector vector with no data, key: " << vector.key << dendl;
        errors.push_back({fmt::format("vectors[{}].data", vi), "missing data"});
        break;
      }
      // validate data dimension
      if (vector.data->size() != dimension) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector vector dimension mismatch, expected "
          << dimension << " got " << vector.data->size() << " for key: " << vector.key << dendl;
        errors.push_back({fmt::format("vectors[{}].data", vi),
          fmt::format("expected dimension {} but got {}", dimension, vector.data->size())});
        break;
      }
      // validate metadata JSON if exists
      const bool has_metadata = !vector.metadata.empty();
      JSONParser parser;
      if (has_metadata && !parser.parse(vector.metadata.c_str(), vector.metadata.size())) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector invalid metadata JSON for key: " << vector.key << dendl;
        errors.push_back({fmt::format("vectors[{}].metadata", vi), "invalid JSON"});
        break;
      }
      if (has_metadata) {
        bool invalid_field = false;
        for (auto it = parser.find_first(); !it.end(); ++it) {
          auto* field = *it;
          const auto& name = field->get_name();
          if (name.find('.') != std::string::npos) {
            ldpp_dout(dpp, 1) << "ERROR: s3vector metadata field name '" << name << "' must not contain '.' in key: " << vector.key << dendl;
            errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, name), "field name must not contain '.'"});
            invalid_field = true;
            break;
          }
          const auto& dv = field->get_data_val();
          if (!dv.quoted && dv.str == "null") {
            ldpp_dout(dpp, 1) << "ERROR: s3vector null metadata value for field '" << name << "' in key: " << vector.key << dendl;
            errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, name), "null values are not supported"});
            invalid_field = true;
            break;
          }
        }
        if (invalid_field) break;
      }
      // add key
      key_builder.Append(vector.key).ok();
      // add data
      auto* float_list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
      for (const auto& value : vector.data.value()) {
        float_list_builder->Append(value).ok();
      }
      data_builder.Append().ok();
      // add metadata
      if (has_metadata) {
        metadata_builder.Append(vector.metadata).ok();
      } else {
        metadata_builder.AppendNull().ok();
      }
      // add filterable metadata columns
      if (!filterable_builders.empty() && !has_metadata) {
        for (auto& fb : filterable_builders) {
          if (fb.must_exist) {
            errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, fb.name), "field is required"});
            break;
          }
          fb.builder->AppendNull().ok();
        }
        if (!errors.empty()) break;
        ++num_rows;
        continue;
      }
      if (has_metadata && !filterable_builders.empty()) {
        for (auto& fb : filterable_builders) {
          bool is_list_type = false;
          switch (fb.type) {
            case FilterableMetadataType::STRING:
            case FilterableMetadataType::NUMBER:
            case FilterableMetadataType::BOOLEAN:
              break;
            case FilterableMetadataType::STRING_LIST:
            case FilterableMetadataType::NUMBER_LIST:
            case FilterableMetadataType::BOOLEAN_LIST:
              is_list_type = true;
              break;
          }
          auto* field_obj = parser.find_obj(fb.name);
          if (!field_obj) {
            if (fb.must_exist) {
              errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, fb.name), "field is required"});
              break;
            }
            fb.builder->AppendNull().ok();
            continue;
          }
          if (is_list_type != field_obj->is_array()) {
            // column/field type mismatch with JSON value type
            errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, fb.name), "invalid type"});
            break;
          }
          std::vector<std::string> values;
          std::string value_str;
          try {
            if (is_list_type) {
              decode_json_obj(values, field_obj);
            } else {
              decode_json_obj(value_str, field_obj);
            }
          } catch (const JSONDecoder::err& e) {
            ldpp_dout(dpp, 1) << "ERROR: s3vector failed to decode metadata field '"
              << fb.name << "' for key: " << vector.key << ". error: " << e.what() << dendl;
            errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, fb.name), "invalid type"});
            break;
          }
          switch (fb.type) {
            case FilterableMetadataType::STRING:
              // anything can go into a string column
              static_cast<arrow::StringBuilder*>(fb.builder.get())->Append(value_str).ok();
              break;
            case FilterableMetadataType::NUMBER:
              try {
                double val;
                decode_from_chars(val, value_str);
                static_cast<arrow::DoubleBuilder*>(fb.builder.get())->Append(val).ok();
              } catch (const JSONDecoder::err& err) {
                ldpp_dout(dpp, 1) << "ERROR: s3vector filterable metadata field '"
                  << fb.name << "' for key: " << vector.key << " expected number but got:"  << value_str
                  << ". error: " << err.what() << dendl;
                errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, fb.name), err.what()});
              }
              break;
            case FilterableMetadataType::BOOLEAN:
              if (value_str == "true") {
                static_cast<arrow::BooleanBuilder*>(fb.builder.get())->Append(true).ok();
              } else if (value_str == "false") {
                static_cast<arrow::BooleanBuilder*>(fb.builder.get())->Append(false).ok();
              } else {
                ldpp_dout(dpp, 1) << "ERROR: s3vector filterable metadata field '"
                  << fb.name << "' for key: " << vector.key << " expected boolean but got: " << value_str << dendl;
                errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, fb.name), "expected boolean"});
              }
              break;
            case FilterableMetadataType::STRING_LIST: {
              auto* list_builder = static_cast<arrow::ListBuilder*>(fb.builder.get());
              auto* value_builder = static_cast<arrow::StringBuilder*>(list_builder->value_builder());
              list_builder->Append().ok();
              for (const auto& v : values) {
                value_builder->Append(v).ok();
              }
              break;
            }
            case FilterableMetadataType::NUMBER_LIST: {
              auto* list_builder = static_cast<arrow::ListBuilder*>(fb.builder.get());
              auto* value_builder = static_cast<arrow::DoubleBuilder*>(list_builder->value_builder());
              list_builder->Append().ok();
              for (const auto& v : values) {
                try {
                  double val;
                  decode_from_chars(val, v);
                  value_builder->Append(val).ok();
                } catch (const JSONDecoder::err& err) {
                  ldpp_dout(dpp, 1) << "ERROR: s3vector filterable metadata field '"
                    << fb.name << "' for key: " << vector.key << " expected number in list but got: " << v
                    << ". error: " << err.what() << dendl;
                  errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, fb.name), err.what()});
                  break;
                }
              }
              break;
            }
            case FilterableMetadataType::BOOLEAN_LIST: {
              auto* list_builder = static_cast<arrow::ListBuilder*>(fb.builder.get());
              auto* value_builder = static_cast<arrow::BooleanBuilder*>(list_builder->value_builder());
              list_builder->Append().ok();
              for (const auto& v : values) {
                if (v == "true") {
                  value_builder->Append(true).ok();
                } else if (v == "false") {
                  value_builder->Append(false).ok();
                } else {
                  ldpp_dout(dpp, 1) << "ERROR: s3vector filterable metadata field '"
                    << fb.name << "' for key: " << vector.key << " expected boolean in list but got: " << v << dendl;
                  errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, fb.name), "expected boolean"});
                  break;
                }
              }
              break;
            }
          }
          if (!errors.empty()) break;
        }
      }
      if (!errors.empty()) break;
      ++num_rows;
    }

    if (!errors.empty()) {
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return -EINVAL;
    }

    std::shared_ptr<arrow::Array> key_array, data_array, metadata_array;
    key_builder.Finish(&key_array).ok();
    data_builder.Finish(&data_array).ok();
    metadata_builder.Finish(&metadata_array).ok();

    arrow::ArrayVector arrays = {key_array, data_array, metadata_array};
    for (auto& fb : filterable_builders) {
      std::shared_ptr<arrow::Array> arr;
      fb.builder->Finish(&arr).ok();
      arrays.push_back(arr);
    }

    auto record_batch = arrow::RecordBatch::Make(schema, num_rows, arrays);
    ldpp_dout(dpp, 20) << "INFO: s3vector created record batch with " << num_rows << " rows" << dendl;
    struct ArrowArray c_array = {};
    struct ArrowSchema c_schema = {};
    if (const auto status = arrow::ExportRecordBatch(*record_batch, &c_array, &c_schema); !status.ok()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to export record batch to C ABI: " << status.ToString() << dendl;
      if (c_schema.release) c_schema.release(&c_schema);
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return -EINVAL;
    }

    LanceDBRecordBatchReader* batch_reader = nullptr;
    char* reader_error_message = nullptr;
    if (const auto reader_result = lancedb_record_batch_reader_from_arrow(
        reinterpret_cast<FFI_ArrowArray*>(&c_array),
        reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
        &batch_reader,
        &reader_error_message); reader_result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create record batch reader from arrow arrays: "
          << (reader_error_message ? reader_error_message : "unknown") << dendl;
      lancedb_free_string(reader_error_message);
      if (c_array.release) c_array.release(&c_array);
      if (c_schema.release) c_schema.release(&c_schema);
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return -EINVAL;
    }

    // upsert data into table: update existing keys, insert new ones
    ldpp_dout(dpp, 10) << "INFO: s3vector attempting to upsert " << num_rows << " vectors with dimension " << dimension << dendl;
    const LanceDBMergeInsertConfig merge_config = {
      .when_matched_update_all = 1,
      .when_not_matched_insert_all = 1
    };
    char* error_message;
    const LanceDBError result = lancedb_table_merge_insert(
          table,
          batch_reader,
          key_columns,
          num_key_columns,
          &merge_config,
          &error_message);

    if (result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to upsert record batch to index. error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return lancedb_error_to_errno(result);
    }
    // we are not failing the operation if we cannot notify the background process on index update
    notify_index_update(dpp, configuration.vector_bucket_name, configuration.index_name);
    lancedb_table_free(table);
    lancedb_connection_free(conn);
    return 0;
  }

  // get vectors

  void get_vectors_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (index_arn) {
      ::encode_json("indexArn", index_arn->to_string(), f);
    } else {
      ::encode_json("indexName", index_name, f);
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    ::encode_json("keys", keys, f);
    ::encode_json("returnData", return_data, f);
    ::encode_json("returnMetadata", return_metadata, f);
    f->close_section();
  }

  void get_vectors_t::decode_json(JSONObj* obj) {
    decode_index_name(vector_bucket_name, index_name, obj);
    JSONDecoder::decode_json("keys", keys, obj, true);
    JSONDecoder::decode_json("returnData", return_data, obj);
    JSONDecoder::decode_json("returnMetadata", return_metadata, obj);

    if (keys.empty() || keys.size() > 100) {
      throw JSONDecoder::err(fmt::format("keys array must contain 1-100 items, got {}", keys.size()));
    }

    for (const auto& key : keys) {
      if (key.empty() || key.length() > 1024) {
        throw JSONDecoder::err(fmt::format("each key must be 1-1024 characters long, got key of length {}", key.length()));
      }
    }
  }

  void get_vectors_reply_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    f->open_array_section("vectors");
    for (const auto& vector : vectors) {
      vector.dump(f);
    }
    f->close_section();
    f->close_section();
  }

  int populate_vectors_from_arrow(
      DoutPrefixProvider* dpp,
      struct ArrowArray** c_arrays_ptr,
      struct ArrowSchema* c_schema_ptr,
      std::vector<vector_item_t>& vectors,
      const std::string& index_name,
      bool use_data,
      bool use_distance,
      bool vector_query,
      bool use_metadata,
      const bool* matches = nullptr) {
    if (auto schema = arrow::ImportSchema(c_schema_ptr); schema.ok()) {
      if (auto array = arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(*c_arrays_ptr), *schema); array.ok()) {
        const auto& record_batch = *array;
        const auto num_columns = static_cast<unsigned int>(record_batch->num_columns());
        for (auto row = 0U; row < record_batch->num_rows(); row++) {
          if (matches && !matches[row]) continue;
          vector_item_t vector_item;
          if (use_data) vector_item.data.emplace();
          for (auto col = 0U; col < num_columns; col++) {
            auto column = record_batch->column(col);
            auto field = record_batch->schema()->field(col);

            if (field->name() == key_field_str) {
              const auto key_array = std::static_pointer_cast<arrow::StringArray>(column);
              if (!key_array->IsNull(row)) {
                vector_item.key = key_array->GetString(row);
              } else {
                vector_item.key = "";
              }
            } else if (field->name() == data_field_str && vector_item.data) {
              const auto data_array = std::static_pointer_cast<arrow::FixedSizeListArray>(column);
              if (!data_array->IsNull(row)) {
                const auto values = std::static_pointer_cast<arrow::FloatArray>(data_array->values());
                const auto start = data_array->value_offset(row);
                const auto length = data_array->value_length();
                for (auto i = 0; i < length; i++) {
                  vector_item.data->push_back(values->Value(start + i));
                }
              } else {
                ldpp_dout(dpp, 5) << "WARNING: s3vector got no data in record batch for index: " << index_name <<dendl;
              }
            } else if (field->name() == distance_field_str) {
              if (!use_distance) continue;
              const auto distance_array = std::static_pointer_cast<arrow::FloatArray>(column);
              if (!distance_array->IsNull(row)) {
                vector_item.distance = distance_array->Value(row);
              } else {
                ldpp_dout(dpp, 5) << "WARNING: s3vector got no distance in record batch for index: " << index_name <<dendl;
              }
            } else if (field->name() == metadata_field_str) {
              if (!use_metadata) continue;
              const auto metadata_array = std::static_pointer_cast<arrow::StringArray>(column);
              if (!metadata_array->IsNull(row)) {
                vector_item.metadata = metadata_array->GetString(row);
              }
            } else {
              ldpp_dout(dpp, 5) << "WARNING: s3vector got unknown field: " << field->name() <<
                " in record batch for index: " << index_name <<dendl;
              continue;
            }
          }
          vectors.push_back(vector_item);
        }
      } else {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to import record batch from arrow arrays for index: " <<
          index_name << ". error: " << array.status().ToString() << dendl;
        lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
        return -EINVAL;
      }
    } else {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to import schema from arrow C ABI for index: " <<
        index_name << ". error: " << schema.status().ToString() << dendl;
      return -EINVAL;
    }

    lancedb_free_arrow_arrays(reinterpret_cast<FFI_ArrowArray**>(c_arrays_ptr), 1);
    lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
    return 0;
  }

  int populate_vectors_from_query(
      DoutPrefixProvider* dpp,
      LanceDBQueryResult* query_result,
      std::vector<vector_item_t>& vectors,
      const std::string& index_name,
      bool use_data,
      bool use_distance,
      bool vector_query,
      bool use_metadata) {
    // distance can be used only with vector queries
    ceph_assert(!use_distance || vector_query);
    struct ArrowArray** c_arrays_ptr = nullptr;
    struct ArrowSchema* c_schema_ptr = nullptr;
    size_t count_out;
    char* error_message;
    if (const LanceDBError result = lancedb_query_result_to_arrow(
          query_result,
          reinterpret_cast<FFI_ArrowArray***>(&c_arrays_ptr),
          reinterpret_cast<FFI_ArrowSchema**>(&c_schema_ptr),
          &count_out,
          &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to convert query result to arrow arrays for index: " << index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      return lancedb_error_to_errno(result);
    }
    if (count_out == 0) {
      // no results
      lancedb_free_arrow_arrays(reinterpret_cast<FFI_ArrowArray**>(c_arrays_ptr), 1);
      lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
      return 0;
    }
    return populate_vectors_from_arrow(dpp, c_arrays_ptr, c_schema_ptr, vectors, index_name, use_data, use_distance, vector_query, use_metadata);
  }

  int get_vectors(const get_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y, get_vectors_reply_t& reply) {
    log_configuration(dpp, "GetVectors", configuration);
    auto table_handle = open_table_with_session_handle(dpp, configuration.vector_bucket_name, configuration.index_name);
    if (!table_handle) {
      return -EIO;
    }
    LanceDBTable* table = table_handle.table;
    LanceDBConnection* conn = table_handle.conn_handle.conn;

    LanceDBQuery* query = lancedb_query_new(table);
    if (!query) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create query for index: " << configuration.index_name << dendl;
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return -EIO;
    }

    char* error_message;
    {
      const auto [columns, count] = get_select_columns(configuration.return_data, configuration.return_metadata);
      if (const LanceDBError result = lancedb_query_select(query, columns, count, &error_message) ; result != LANCEDB_SUCCESS) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set select columns for query on index: " << configuration.index_name << ". error: " << error_message << dendl;
        lancedb_free_string(error_message);
        lancedb_query_free(query);
        lancedb_table_free(table);
        lancedb_connection_free(conn);
        return lancedb_error_to_errno(result);
      }
    }

    // build where filter for keys
    std::ostringstream oss;
    const auto keys_size = configuration.keys.size();
    for (size_t i = 0; i < keys_size; ++i) {
      oss << "key = \"" << configuration.keys[i] << "\"";
      if (i < keys_size - 1) {
        oss << " OR ";
      }
    }

    if (const LanceDBError result = lancedb_query_where_filter(query, oss.str().c_str(), &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set where filter for query on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_query_free(query);
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return lancedb_error_to_errno(result);
    }

    LanceDBQueryResult* query_result = lancedb_query_execute(query);
    if (!query_result) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to execute query on index: " << configuration.index_name << dendl;
      lancedb_query_free(query);
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return -EIO;
    }

    auto ret = populate_vectors_from_query(dpp, query_result, reply.vectors, configuration.index_name, configuration.return_data, false, false, configuration.return_metadata);
    lancedb_table_free(table);
    lancedb_connection_free(conn);
    return ret;
  }

  // list vectors

  void list_vectors_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (index_arn) {
      ::encode_json("indexArn", index_arn->to_string(), f);
    } else {
      ::encode_json("indexName", index_name, f);
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    ::encode_json("maxResults", max_results, f);
    ::encode_json("nextToken", offset, f);
    ::encode_json("returnData", return_data, f);
    ::encode_json("returnMetadata", return_metadata, f);
    if (segment_count > 0) {
      ::encode_json("segmentCount", segment_count, f);
      ::encode_json("segmentIndex", segment_index, f);
    }
    f->close_section();
  }

  void list_vectors_t::decode_json(JSONObj* obj) {
    decode_index_name(vector_bucket_name, index_name, obj);
    JSONDecoder::decode_json("maxResults", max_results, default_max_results, obj);
    std::string next_token;
    JSONDecoder::decode_json("nextToken", next_token, obj);
    JSONDecoder::decode_json("returnData", return_data, obj);
    JSONDecoder::decode_json("returnMetadata", return_metadata, obj);
    JSONDecoder::decode_json("segmentCount", segment_count, obj);
    JSONDecoder::decode_json("segmentIndex", segment_index, obj);

    if (max_results < 1 || max_results > 1000) {
      throw JSONDecoder::err(fmt::format("maxResults must be between 1 and 1000, got {}", max_results));
    }

    // according to the AWS spec the "nextToken" should be a string
    // however, in lancedb, fetching all vectors in pages is done using offsets
    /* if (!next_token.empty() && (next_token.length() < 1 || next_token.length() > 2048)) {
      throw JSONDecoder::err(fmt::format("nextToken length must be between 1 and 2048, got {}", next_token.length()));
    }*/

    if (!next_token.empty()) {
      decode_from_chars(offset, next_token);
    }

    if (segment_count > 0) {
      if (segment_count < 1 || segment_count > 16) {
        throw JSONDecoder::err(fmt::format("segmentCount must be between 1 and 16, got {}", segment_count));
      }
      if (segment_index >= segment_count) {
        throw JSONDecoder::err(fmt::format("segmentIndex must be between 0 and segmentCount-1 ({}), got {}", segment_count - 1, segment_index));
      }
    } else if (segment_index > 0) {
      throw JSONDecoder::err("segmentIndex requires segmentCount to be specified");
    }
  }

  void list_vectors_reply_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("nextToken", next_token, f);
    f->open_array_section("vectors");
    for (const auto& vector : vectors) {
      vector.dump(f);
    }
    f->close_section();
    f->close_section();
  }

  int list_vectors(const list_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y, list_vectors_reply_t& reply) {
    log_configuration(dpp, "ListVectors", configuration);
    LanceDBTable* table = open_table(dpp, configuration.vector_bucket_name, configuration.index_name);
    if (!table) {
      return -EIO;
    }

    LanceDBQuery* query = lancedb_query_new(table);
    if (!query) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create query for index: " << configuration.index_name << dendl;
      lancedb_table_free(table);
      return -EIO;
    }

    char* error_message;
    {
      const auto [columns, count] = get_select_columns(configuration.return_data, configuration.return_metadata);
      if (const LanceDBError result = lancedb_query_select(query, columns, count, &error_message) ; result != LANCEDB_SUCCESS) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set select columns for query on index: " << configuration.index_name << ". error: " << error_message << dendl;
        lancedb_free_string(error_message);
        lancedb_query_free(query);
        lancedb_table_free(table);
        return lancedb_error_to_errno(result);
      }
    }

    if (const LanceDBError result = lancedb_query_limit(query, configuration.max_results, &error_message) ; result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set query limit on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_query_free(query);
      lancedb_table_free(table);
      return lancedb_error_to_errno(result);
    }

    if (configuration.offset > 0) {
      if (const LanceDBError result = lancedb_query_offset(query, configuration.offset, &error_message) ; result != LANCEDB_SUCCESS) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set query offset on index: " << configuration.index_name << ". error: " << error_message << dendl;
        lancedb_free_string(error_message);
        lancedb_query_free(query);
        lancedb_table_free(table);
        return lancedb_error_to_errno(result);
      }
    }

    LanceDBQueryResult* query_result = lancedb_query_execute(query);
    if (!query_result) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to execute query on index: " << configuration.index_name << dendl;
      lancedb_query_free(query);
      lancedb_table_free(table);
      return -EIO;
    }

    int ret;
    if (ret = populate_vectors_from_query(dpp, query_result, reply.vectors, configuration.index_name, configuration.return_data, false, false, configuration.return_metadata); ret == 0) {
      const auto total_row_count = lancedb_table_count_rows(table);
      const auto next_offset = reply.vectors.size() + configuration.offset;
      if (next_offset < total_row_count) {
        reply.next_token = std::to_string(next_offset);
      }
    }
    lancedb_table_free(table);
    return ret;
  }

  // delete vectors

  void delete_vectors_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (index_arn) {
      ::encode_json("indexArn", index_arn->to_string(), f);
    } else {
      ::encode_json("indexName", index_name, f);
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    ::encode_json("keys", keys, f);
    f->close_section();
  }

  void delete_vectors_t::decode_json(JSONObj* obj) {
    decode_index_name(vector_bucket_name, index_name, obj);
    JSONDecoder::decode_json("keys", keys, obj, true);

    if (keys.empty() || keys.size() > 500) {
      throw JSONDecoder::err(fmt::format("keys array must contain 1-500 items, got {}", keys.size()));
    }

    for (const auto& key : keys) {
      if (key.empty() || key.length() > 1024) {
        throw JSONDecoder::err(fmt::format("each key must be 1-1024 characters long, got key of length {}", key.length()));
      }
    }
  }

  int delete_vectors(const delete_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "DeleteVectors", configuration);
    auto table_handle = open_table_with_session_handle(dpp, configuration.vector_bucket_name, configuration.index_name);
    if (!table_handle) {
      return -EIO;
    }
    LanceDBTable* table = table_handle.table;
    LanceDBConnection* conn = table_handle.conn_handle.conn;
    // build datafusion expression: key IN ("k1", "k2", ...)
    std::vector<LanceDBExpr*> key_exprs(configuration.keys.size());
    for (size_t i = 0; i < configuration.keys.size(); ++i) {
      key_exprs[i] = lancedb_expr_literal_string(configuration.keys[i].c_str());
      if (!key_exprs[i]) {
        for (size_t j = 0; j < i; ++j) {
          lancedb_expr_free(key_exprs[j]);
        }
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create literal expression for key: " << configuration.keys[i] << dendl;
        lancedb_table_free(table);
        lancedb_connection_free(conn);
        return -EINVAL;
      }
    }
    char* error_message = nullptr;
    LanceDBExpr* in_expr = lancedb_expr_in_list(
        lancedb_expr_column(key_field),
        key_exprs.data(),
        key_exprs.size(),
        false,
        &error_message);
    if (!in_expr) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to build delete expression for index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return -EINVAL;
    }
    LanceDBError result = lancedb_table_df_delete(table, in_expr, &error_message);
    if (result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to delete vectors from index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
    }
    lancedb_table_free(table);
    lancedb_connection_free(conn);
    return lancedb_error_to_errno(result);
  }

  // query vectors

  void query_vectors_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    if (!filter.empty()) {
      ::encode_json("filter", filter, f);
    }
    if (index_arn) {
      ::encode_json("indexArn", index_arn->to_string(), f);
    } else {
      ::encode_json("indexName", index_name, f);
      ::encode_json("vectorBucketName", vector_bucket_name, f);
    }
    rgw::s3vector::encode_json("queryVector", query_vector, f);
    ::encode_json("returnDistance", return_distance, f);
    ::encode_json("returnMetadata", return_metadata, f);
    ::encode_json("topK", top_k, f);
    ::encode_json("postFiltering", post_filtering, f);
    f->close_section();
  }

  void query_vectors_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("filter", filter, obj);
    decode_index_name(vector_bucket_name, index_name, obj);
    rgw::s3vector::decode_json("queryVector", query_vector, obj);
    JSONDecoder::decode_json("returnDistance", return_distance, obj);
    JSONDecoder::decode_json("returnMetadata", return_metadata, obj);
    JSONDecoder::decode_json("topK", top_k, obj, true);
    JSONDecoder::decode_json("postFiltering", post_filtering, obj);

    if (top_k < 1) {
      throw JSONDecoder::err(fmt::format("topK must be at least 1, got {}", top_k));
    }

    if (query_vector.empty()) {
      throw JSONDecoder::err("queryVector cannot be empty");
    }

  }

  void query_vectors_reply_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("distanceMetric", distance_metric_to_string(distance_metric), f);
    f->open_array_section("vectors");
    for (const auto& vector : vectors) {
      vector.dump(f);
    }
    f->close_section();
    f->close_section();
  }

  int query_vectors(const query_vectors_t& configuration, std::optional<JSONParser>& filter, DoutPrefixProvider* dpp, optional_yield y, query_vectors_reply_t& reply, std::vector<validation_error_t>& errors) {
    log_configuration(dpp, "QueryVectors", configuration);
    auto table_handle = open_table_with_session_handle(dpp, configuration.vector_bucket_name, configuration.index_name);
    if (!table_handle) {
      return -EIO;
    }
    LanceDBTable* table = table_handle.table;
    LanceDBConnection* conn = table_handle.conn_handle.conn;

    std::shared_ptr<arrow::Schema> schema;
    if (int ret = import_table_schema(configuration.index_name, table, dpp, schema); ret < 0) {
      lancedb_table_free(table);
      return ret;
    }

    unsigned int table_dimension;
    if (int ret = get_vector_dimension(configuration.index_name, schema, dpp, table_dimension); ret < 0) {
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return ret;
    }

    const auto query_dimension = configuration.query_vector.size();
    if (table_dimension != query_dimension) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector query vector dimension (" << query_dimension << ") does not match index: " <<
        configuration.index_name << " vector dimension (" << table_dimension << ")" << dendl;
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return -EINVAL;
    }

    LanceDBVectorQuery* query = lancedb_vector_query_new(table, configuration.query_vector.data(), query_dimension);
    if (!query) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create vector query for index: " << configuration.index_name << dendl;
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return -EIO;
    }

    char* error_message;

    // parse filter before setting up select columns, since a JSON filter
    // requires the metadata column to be included in the query results
    LanceDBExpr* json_filter_expr = nullptr;
    if (filter) {
      const auto filterable_keys = configuration.post_filtering
          ? std::vector<filterable_metadata_key_t>{}
          : get_filterable_keys_from_schema(schema);
      std::vector<std::string> nonfilterable_keys;
      if (int ret = get_nonfilterable_metadata(table, dpp, nonfilterable_keys); ret < 0) {
        lancedb_vector_query_free(query);
        lancedb_table_free(table);
        lancedb_connection_free(conn);
        return ret;
      }
      auto filter_exprs = build_filter_expr(*filter, filterable_keys, nonfilterable_keys, dpp, errors);
      if (!filter_exprs) {
        lancedb_vector_query_free(query);
        lancedb_table_free(table);
        lancedb_connection_free(conn);
        return -EINVAL;
      }
      if (filter_exprs->column_expr) {
        if (const LanceDBError result = lancedb_vector_query_df_filter(query, filter_exprs->column_expr, &error_message); result != LANCEDB_SUCCESS) {
          ldpp_dout(dpp, 1) << "ERROR: s3vector failed to apply column filter for vector query on index: " << configuration.index_name << ". error: " << error_message << dendl;
          lancedb_free_string(error_message);
          lancedb_expr_free(filter_exprs->json_expr);
          lancedb_vector_query_free(query);
          lancedb_table_free(table);
          lancedb_connection_free(conn);
          return lancedb_error_to_errno(result);
        }
      }
      json_filter_expr = filter_exprs->json_expr;
    }

    const bool need_metadata = configuration.return_metadata || json_filter_expr;
    {
      const auto num_columns = need_metadata ? 2UL : 1UL;
      const auto* columns = need_metadata ? key_and_metadata_columns : key_columns;
      if (const LanceDBError result = lancedb_vector_query_select(query, columns, num_columns, &error_message) ; result != LANCEDB_SUCCESS) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set select columns for vector query on index: " << configuration.index_name << ". error: " << error_message << dendl;
        lancedb_free_string(error_message);
        lancedb_expr_free(json_filter_expr);
        lancedb_vector_query_free(query);
        lancedb_table_free(table);
        lancedb_connection_free(conn);
        return lancedb_error_to_errno(result);
      }
    }

    if (const LanceDBError result = lancedb_vector_query_column(query, data_field, &error_message) ; result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set select columns for vector query on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_expr_free(json_filter_expr);
      lancedb_vector_query_free(query);
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return lancedb_error_to_errno(result);
    }

    const auto effective_top_k = json_filter_expr
        ? static_cast<unsigned int>(std::lround(configuration.top_k * dpp->get_cct()->_conf->rgw_s3vector_topk_post_filter_factor))
        : configuration.top_k;
    if (const LanceDBError result = lancedb_vector_query_limit(query, effective_top_k, &error_message) ; result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set top-k for vector query on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_expr_free(json_filter_expr);
      lancedb_vector_query_free(query);
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return lancedb_error_to_errno(result);
    }

    // execute consumes query regardless of success/failure
    LanceDBQueryResult* query_result = lancedb_vector_query_execute(query);
    if (!query_result) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to execute query on index: " << configuration.index_name << dendl;
      lancedb_expr_free(json_filter_expr);
      lancedb_table_free(table);
      lancedb_connection_free(conn);
      return -EIO;
    }

    int ret;
    if (json_filter_expr) {
      struct ArrowArray** c_arrays_ptr = nullptr;
      struct ArrowSchema* c_schema_ptr = nullptr;
      size_t count_out;
      if (const LanceDBError result = lancedb_query_result_to_arrow(
            query_result,
            reinterpret_cast<FFI_ArrowArray***>(&c_arrays_ptr),
            reinterpret_cast<FFI_ArrowSchema**>(&c_schema_ptr),
            &count_out,
            &error_message); result != LANCEDB_SUCCESS) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector failed to convert query result to arrow arrays for index: " << configuration.index_name << ". error: " << error_message << dendl;
        lancedb_free_string(error_message);
        lancedb_expr_free(json_filter_expr);
        lancedb_table_free(table);
        return lancedb_error_to_errno(result);
      }

      bool* matches = nullptr;
      size_t match_count = 0;
      if (count_out > 0) {
        if (const LanceDBError result = lancedb_json_matches(
              reinterpret_cast<FFI_ArrowArray**>(c_arrays_ptr),
              reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr),
              count_out,
              json_filter_expr,
              &matches,
              &match_count,
              &error_message); result != LANCEDB_SUCCESS) {
          ldpp_dout(dpp, 1) << "ERROR: s3vector failed to apply JSON metadata filter for index: " << configuration.index_name << ". error: " << error_message << dendl;
          lancedb_free_string(error_message);
          lancedb_free_arrow_arrays(reinterpret_cast<FFI_ArrowArray**>(c_arrays_ptr), count_out);
          lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
          lancedb_table_free(table);
          return lancedb_error_to_errno(result);
        }
      }

      if (count_out == 0) {
        lancedb_free_arrow_arrays(reinterpret_cast<FFI_ArrowArray**>(c_arrays_ptr), count_out);
        lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
        lancedb_expr_free(json_filter_expr);
        ret = 0;
      } else {
        const bool need_distance = configuration.return_distance || (effective_top_k > configuration.top_k);
        ret = populate_vectors_from_arrow(dpp, c_arrays_ptr, c_schema_ptr, reply.vectors, configuration.index_name,
            false, need_distance, true, configuration.return_metadata, matches);
        if (ret == 0 && reply.vectors.size() > configuration.top_k) {
          // if we received more than k vectors (due to using the factor when post filtering)
          // we return the top k ones based on distance
          std::sort(reply.vectors.begin(), reply.vectors.end(),
              [](const vector_item_t& a, const vector_item_t& b) {
                return *a.distance < *b.distance;
              });
          reply.vectors.resize(configuration.top_k);
          if (!configuration.return_distance) {
            // if distance was not asked by the client
            // we remove it from the reply
            for (auto& v : reply.vectors) v.distance.reset();
          }
        }
      }
      lancedb_free_json_matches(matches);
    } else {
      ret = populate_vectors_from_query(dpp, query_result, reply.vectors, configuration.index_name, false, configuration.return_distance, true, configuration.return_metadata);
    }

    reply.distance_metric = get_distance_metric(table, dpp);
    lancedb_table_free(table);
    lancedb_connection_free(conn);
    return ret;
  }

}
