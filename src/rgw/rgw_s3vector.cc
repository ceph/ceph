// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_s3vector.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "common/dout.h"
#include <arrow/type_fwd.h>
#include <fmt/format.h>
#include "lancedb.h"
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <charconv>

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {

  // convert LanceDBError to linux error codes
  int lancedb_error_to_errno(LanceDBError err) {
    switch (err) {
      case LANCEDB_SUCCESS:
        return 0;
      case LANCEDB_INVALID_ARGUMENT:
      case LANCEDB_INVALID_TABLE_NAME:
      case LANCEDB_INVALID_INPUT:
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
      case LANCEDB_SCHEMA:
      case LANCEDB_RUNTIME:
      case LANCEDB_OBJECT_STORE:
      case LANCEDB_LANCE:
      case LANCEDB_HTTP:
      case LANCEDB_ARROW:
      case LANCEDB_OTHER:
      case LANCEDB_UNKNOWN:
        return -EIO;
    }
    return -EIO;
  }

  // utility functions for connection creation and opening table

  LanceDBConnection* connect(DoutPrefixProvider* dpp, const std::string& vector_bucket_name) {
    const auto dbname = fmt::format("/tmp/lancedb/{}", vector_bucket_name);
    LanceDBConnectBuilder* builder = lancedb_connect(dbname.c_str());
    LanceDBConnection* conn = lancedb_connect_builder_execute(builder);
    if (!conn) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to connect to: " << dbname << dendl;
      lancedb_connect_builder_free(builder);
      return nullptr;
    }
    return conn;
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

  // utility functions for JSON encoding/decoding

  void decode_json_obj(float& val, JSONObj *obj) {
    std::string_view s = obj->get_data();
    const char* start = s.data();
    const char* end = start + s.length();

    const auto result = std::from_chars(start, end, val);

    if (result.ec == std::errc::invalid_argument) {
      throw JSONDecoder::err("failed to parse number");
    }

    if (result.ec == std::errc::result_out_of_range) {
      throw JSONDecoder::err("out of range number");
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
      decode_json_obj(value, *value_it);
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

  void decode_json(const char* field_name, DistanceMetric& metric, JSONObj* obj, bool mandatory) {
    std::string metric_str;
    JSONDecoder::decode_json(field_name, metric_str, obj, mandatory);
    if (metric_str == "cosine") {
      metric = DistanceMetric::COSINE;
    } else if (metric_str == "euclidean") {
      metric = DistanceMetric::EUCLIDEAN;
    } else {
      throw JSONDecoder::err("invalid distanceMetric: " + metric_str);
    }
  }

  void encode_json(const char* field_name, const DistanceMetric& metric, ceph::Formatter* f) {
    switch (metric) {
      case DistanceMetric::COSINE:
        ::encode_json(field_name, "cosine", f);
        return;
      case DistanceMetric::EUCLIDEAN:
        ::encode_json(field_name, "euclidean", f);
        return;
    }
    ::encode_json(field_name, "unknown", f);
  }

  void create_index_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    ::encode_json("dataType", data_type, f);
    ::encode_json("dimension", dimension, f);
    rgw::s3vector::encode_json("distanceMetric", distance_metric, f);
    ::encode_json("indexName", index_name, f);
    f->open_object_section("metadataConfiguration");
    ::encode_json("nonFilterableMetadataKeys", non_filterable_metadata_keys, f);
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
    rgw::s3vector::decode_json("distanceMetric", distance_metric, obj, true);
    JSONDecoder::decode_json("indexName", index_name, obj, true);
    auto md_it = obj->find("metadataConfiguration");
    if (!md_it.end()) {
      JSONDecoder::decode_json("nonFilterableMetadataKeys", non_filterable_metadata_keys, *md_it);
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
  static constexpr const char* distance_field = "_distance";
  static const std::string distance_field_str{distance_field};;
  static constexpr const char* key_columns[] = {key_field};
  static constexpr const char* data_columns[] = {data_field};
  static constexpr const char* table_columns[] = {key_field, data_field};
  static constexpr int num_key_columns = 1;
  static constexpr int num_table_columns = 2;

  int create_table_schema(unsigned int dimension, DoutPrefixProvider* dpp, ArrowSchema* c_schema) {
    const auto schema = arrow::schema(
        {
          arrow::field(key_field, arrow::utf8()),
          arrow::field(data_field, arrow::fixed_size_list(arrow::float32(), dimension))
        }
      );
    if (const auto status = arrow::ExportSchema(*schema, c_schema); !status.ok()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to export schema to C ABI: " << status.ToString() << dendl;
      return -EINVAL;
    }
    return 0;
  }

  int create_table_schema(unsigned int dimension, DoutPrefixProvider* dpp, ArrowSchema* c_schema, const std::shared_ptr<arrow::Schema>& schema) {
    if (const auto status = arrow::ExportSchema(*schema, c_schema); !status.ok()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to export schema to C ABI: " << status.ToString() << dendl;
      return -EINVAL;
    }
    return 0;
  }

  int create_index(const create_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "CreateIndex", configuration);
    LanceDBConnection* conn = connect(dpp, configuration.vector_bucket_name);
    if (!conn) {
      return -EIO;
    }
    // TODO: build the arrow schema when creating the table and store the C ABI schema
    // as an attrribute of the vectore bucket
    struct ArrowSchema c_schema;
    if (int ret = create_table_schema(configuration.dimension, dpp, &c_schema); ret < 0) {
      lancedb_connection_free(conn);
      return ret;
    }
    char* error_message;
    LanceDBTable* table = nullptr;
    if (const LanceDBError result = lancedb_table_create(conn, configuration.index_name.c_str(),
          reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
          nullptr, &table, &error_message); result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector creating index: " << configuration.index_name << ", error: " << error_message << dendl;
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
    rgw::s3vector::encode_json("distanceMetric", distance_metric, f);
    ::encode_json("indexArn", index_arn, f);
    ::encode_json("indexName", index_name, f);
    f->open_object_section("metadataConfiguration");
    ::encode_json("nonFilterableMetadataKeys", non_filterable_metadata_keys, f);
    f->close_section();
    ::encode_json("vectorBucketName", vector_bucket_name, f);
    f->close_section();
    f->close_section();
  }

  int get_vector_dimension(const std::string& index_name, LanceDBTable* table, DoutPrefixProvider* dpp, unsigned int& dimension) {
    // Get the Arrow schema from the table
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

    // Import the schema to Arrow C++
    const auto schema = arrow::ImportSchema(c_schema_ptr);
    if (!schema.ok()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to import schema for index: " << index_name
                        << ". error: " << schema.status().ToString() << dendl;
      lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
      return -EINVAL;
    }

    // Extract dimension from the "data" field
    auto data_field = schema->get()->GetFieldByName(data_field_str);
    if (!data_field) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector schema missing " << data_field_str << " field for index: " << index_name << dendl;
      lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
      return -EINVAL;
    }

    if (data_field->type()->id() != arrow::Type::FIXED_SIZE_LIST) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector " << data_field_str << "  field is not a FixedSizeList for index: " << index_name << dendl;
      lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
      return -EINVAL;
    }

    auto fixed_size_list_type = std::static_pointer_cast<arrow::FixedSizeListType>(data_field->type());
    dimension = fixed_size_list_type->list_size();
    lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
    return 0;
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

    if (int ret = get_vector_dimension(configuration.index_name, table, dpp, reply.dimension); ret < 0) {
      lancedb_connection_free(conn);
      lancedb_table_free(table);
      return ret;
    }

    reply.data_type = "float32";
    reply.distance_metric = DistanceMetric::COSINE; // TODO: store and retrieve from table metadata
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

    reply.creation_time = 0; // TODO: get from table metadata
    // reply.non_filterable_metadata_keys - empty for now, TODO: store and retrieve from table metadata
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
      reply.indexes.emplace_back(
          0, // TODO: creation time
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
    // TODO: do we want to drop all tables or return error if tables exist?
    char* error_message;
    if (LanceDBError err = lancedb_connection_drop_all_tables(conn, nullptr, &error_message); err != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: failed to drop content of s3vector bucket in: " << configuration.vector_bucket_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_connection_free(conn);
      return -EIO;
    }
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
    // TODO: implement in RGW only
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
    LanceDBConnection* conn = connect(dpp, configuration.vector_bucket_name);
    if (!conn) {
      return -EIO;
    }
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

  int get_vector_bucket(const get_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "GetVectorBucket", configuration);
    // TODO: implement in RGW
    return 0;
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

  int list_vector_buckets(const list_vector_buckets_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "ListVectorBuckets", configuration);
    // TODO: implement in RGW only
    return 0;
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
    // TODO: validate JSON policy
  }

  int put_vector_bucket_policy(const put_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "PutVectorBucketPolicy", configuration);
    // TODO: implement in RGW only
    return 0;
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

  int get_vector_bucket_policy(const get_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "GetVectorBucketPolicy", configuration);
    // TODO: implement in RGW only
    return 0;
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

  int put_vectors(const put_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    log_configuration(dpp, "PutVectors", configuration);
    LanceDBTable* table = open_table(dpp, configuration.vector_bucket_name, configuration.index_name);
    if (!table) {
      return -EIO;
    }
    // Detect dimension from the first vector in the request
    unsigned int dimension = 0;
    if (!configuration.vectors.empty()) {
      dimension = configuration.vectors[0].data->size();
      ldpp_dout(dpp, 20) << "INFO: s3vector detected dimension " << dimension << " from first vector" << dendl;
    } else {
      ldpp_dout(dpp, 10) << "WARNING: s3vector no vectors provided" << dendl;
      lancedb_table_free(table);
      return 0;
    }
    // TODO: index conf should be an attribute of the vector bucket with index/table name as key
    // and should include the dimension
    // TODO: build the arrow schema when creating the table and store the C ABI schema
    // as an attribute of the vector bucket
    struct ArrowSchema c_schema;
    const std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {
          arrow::field(key_field, arrow::utf8()),
          arrow::field(data_field, arrow::fixed_size_list(arrow::float32(), dimension))
        }
      );
    if (int ret = create_table_schema(dimension, dpp, &c_schema, schema); ret < 0) {
      lancedb_table_free(table);
      return ret;
    }

    arrow::StringBuilder key_builder;
    arrow::FloatBuilder float_builder;
    arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
        std::make_unique<arrow::FloatBuilder>(),
        dimension);
    // TODO: metadata configuration should also be taken from the index configuration
    unsigned int num_rows = 0;
    for (const auto& vector : configuration.vectors) {
      if (!vector.data) {
        ldpp_dout(dpp, 5) << "WARNING: s3vector skipping vector with no data" << dendl;
        continue;
      }
      if (vector.data->size() != dimension) {
        ldpp_dout(dpp, 5) << "WARNING: s3vector vector dimension mismatch, expected "
          << dimension << " got " << vector.data->size() <<
          ". skip vector with key: " << vector.key << dendl;
        continue;
      }
      if (vector.key.empty()) {
        ldpp_dout(dpp, 5) << "WARNING: s3vector skipping vector with empty key" << dendl;
        continue;
      }
      // TODO: check if metadata is allowed based on index config
      // key column
      key_builder.Append(vector.key).ok();
      // data column
      auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
      for (const auto & value : vector.data.value()) {
        list_builder->Append(value).ok();
      }
      data_builder.Append().ok();
      ++num_rows;
    }

    if (num_rows == 0) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector no valid vectors to insert" << dendl;
      lancedb_table_free(table);
      return -EINVAL;
    }

    std::shared_ptr<arrow::Array> key_array, data_array;
    key_builder.Finish(&key_array).ok();
    data_builder.Finish(&data_array).ok();

    auto record_batch = arrow::RecordBatch::Make(schema, num_rows, {key_array, data_array});
    ldpp_dout(dpp, 20) << "INFO: s3vector created record batch with " << num_rows << " rows" << dendl;
    struct ArrowArray c_array;
    if (const auto status = arrow::ExportRecordBatch(*record_batch, &c_array, &c_schema); !status.ok()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to export record batch to C ABI: " << status.ToString() << dendl;
      // Clean up Arrow C ABI schema on error
      if (c_schema.release) c_schema.release(&c_schema);
      lancedb_table_free(table);
      return -EINVAL;
    }

    auto batch_reader = lancedb_record_batch_reader_from_arrow(
        reinterpret_cast<FFI_ArrowArray*>(&c_array),
        reinterpret_cast<FFI_ArrowSchema*>(&c_schema));
    if (!batch_reader) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create record batch reader from arrow arrays" << dendl;
      // Clean up Arrow C ABI structures
      if (c_array.release) c_array.release(&c_array);
      if (c_schema.release) c_schema.release(&c_schema);
      lancedb_table_free(table);
      return -EINVAL;
    }

    // add data to table using simple insert instead of merge_insert to avoid SQL parser issues
    ldpp_dout(dpp, 10) << "INFO: s3vector attempting to add " << num_rows << " vectors with dimension " << dimension << dendl;
    char* error_message;
    const LanceDBError result = lancedb_table_add(
          table,
          batch_reader,
          &error_message);

    if (result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to write record batch to index. error: " << error_message << dendl;
      lancedb_record_batch_reader_free(batch_reader);
      lancedb_free_string(error_message);
      lancedb_table_free(table);
      return lancedb_error_to_errno(result);
    }
    lancedb_table_free(table);
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
      bool vector_query) {
    unsigned long num_columns = 1;
    if (use_data) ++num_columns;
    if (vector_query) ++num_columns;
    if (auto schema = arrow::ImportSchema(c_schema_ptr); schema.ok()) {
      if (auto array = arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(*c_arrays_ptr), *schema); array.ok()) {
        const auto& record_batch = *array;
        // return by rows instead of columns
        for (auto row = 0U; row < record_batch->num_rows(); row++) {
          const auto record_num_columns = static_cast<unsigned int>(record_batch->num_columns());
          if (record_num_columns != num_columns) {
            ldpp_dout(dpp, 1) << "ERROR: s3vector got invalid number of columns in record batch for index: " <<
              index_name << ". got: " << record_num_columns << " expected: " << num_columns << dendl;
              lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
              return -EINVAL;
          }
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
      bool vector_query) {
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
    return populate_vectors_from_arrow(dpp, c_arrays_ptr, c_schema_ptr, vectors, index_name, use_data, use_distance, vector_query);
  }

  int get_vectors(const get_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y, get_vectors_reply_t& reply) {
    log_configuration(dpp, "GetVectors", configuration);
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
    const unsigned long num_columns = (configuration.return_data ? 2 : 1);
    // TODO support metadata
    if (const LanceDBError result = lancedb_query_select(query, table_columns, num_columns, &error_message) ; result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set select columns for query on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_query_free(query);
      lancedb_table_free(table);
      return lancedb_error_to_errno(result);
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
      return lancedb_error_to_errno(result);
    }

    LanceDBQueryResult* query_result = lancedb_query_execute(query);
    if (!query_result) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to execute query on index: " << configuration.index_name << dendl;
      lancedb_query_free(query);
      lancedb_table_free(table);
      return -EIO;
    }

    auto ret = populate_vectors_from_query(dpp, query_result, reply.vectors, configuration.index_name, configuration.return_data, false, false);
    lancedb_table_free(table);
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
      const char* start = next_token.data();
      const char* end = start + next_token.length();
      const auto result = std::from_chars(start, end, offset);

      if (result.ec == std::errc::invalid_argument) {
        throw JSONDecoder::err("failed to parse next token as offset");
      }

      if (result.ec == std::errc::result_out_of_range) {
        throw JSONDecoder::err("out of range offset in next token");
      }
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
    const unsigned long num_columns = (configuration.return_data ? 2 : 1);
    // TODO support metadata
    if (const LanceDBError result = lancedb_query_select(query, table_columns, num_columns, &error_message) ; result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set select columns for query on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_query_free(query);
      lancedb_table_free(table);
      return lancedb_error_to_errno(result);
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
    if (ret = populate_vectors_from_query(dpp, query_result, reply.vectors, configuration.index_name, configuration.return_data, false, false); ret == 0) {
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
    LanceDBTable* table = open_table(dpp, configuration.vector_bucket_name, configuration.index_name);
    if (!table) {
      return -EIO;
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
    char* error_message;
    LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);
    if (result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set select columns for query on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
    }
    lancedb_table_free(table);
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
    f->close_section();
  }

  void query_vectors_t::decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("filter", filter, obj);
    decode_index_name(vector_bucket_name, index_name, obj);
    rgw::s3vector::decode_json("queryVector", query_vector, obj);
    JSONDecoder::decode_json("returnDistance", return_distance, obj);
    JSONDecoder::decode_json("returnMetadata", return_metadata, obj);
    JSONDecoder::decode_json("topK", top_k, obj, true);

    if (top_k < 1) {
      throw JSONDecoder::err(fmt::format("topK must be at least 1, got {}", top_k));
    }

    if (query_vector.empty()) {
      throw JSONDecoder::err("queryVector cannot be empty");
    }

    // TODO: validate filter
  }

  void query_vectors_reply_t::dump(ceph::Formatter* f) const {
    f->open_object_section("");
    rgw::s3vector::encode_json("distanceMetric", distance_metric, f);
    f->open_array_section("vectors");
    for (const auto& vector : vectors) {
      vector.dump(f);
    }
    f->close_section();
    f->close_section();
  }

  int query_vectors(const query_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y, query_vectors_reply_t& reply) {
    log_configuration(dpp, "QueryVectors", configuration);
    LanceDBTable* table = open_table(dpp, configuration.vector_bucket_name, configuration.index_name);
    if (!table) {
      return -EIO;
    }

    unsigned int table_dimension;
    if (int ret = get_vector_dimension(configuration.index_name, table, dpp, table_dimension); ret < 0) {
      lancedb_table_free(table);
      return ret;
    }

    const auto query_dimension = configuration.query_vector.size();
    if (table_dimension != query_dimension) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector query vector dimension (" << query_dimension << ") does not match index: " <<
        configuration.index_name << " vector dimension (" << table_dimension << ")" << dendl;
      return -EINVAL;
    }

    LanceDBVectorQuery* query = lancedb_vector_query_new(table, configuration.query_vector.data(), query_dimension);
    if (!query) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to create vector query for index: " << configuration.index_name << dendl;
      lancedb_table_free(table);
      return -EIO;
    }

    char* error_message;
    constexpr auto num_columns = 1;
    // TODO support metadata
    if (const LanceDBError result = lancedb_vector_query_select(query, key_columns, num_columns, &error_message) ; result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set select columns for vector query on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_vector_query_free(query);
      lancedb_table_free(table);
      return lancedb_error_to_errno(result);
    }

    if (const LanceDBError result = lancedb_vector_query_column(query, data_field, &error_message) ; result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set select columns for vector query on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_vector_query_free(query);
      lancedb_table_free(table);
      return lancedb_error_to_errno(result);
    }

    if (const LanceDBError result = lancedb_vector_query_limit(query, configuration.top_k, &error_message) ; result != LANCEDB_SUCCESS) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to set top-k for vector query on index: " << configuration.index_name << ". error: " << error_message << dendl;
      lancedb_free_string(error_message);
      lancedb_vector_query_free(query);
      lancedb_table_free(table);
      return lancedb_error_to_errno(result);
    }

    LanceDBQueryResult* query_result = lancedb_vector_query_execute(query);
    if (!query_result) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector failed to execute query on index: " << configuration.index_name << dendl;
      lancedb_vector_query_free(query);
      lancedb_table_free(table);
      return -EIO;
    }

    int ret = populate_vectors_from_query(dpp, query_result, reply.vectors, configuration.index_name, false, configuration.return_distance, true);
    reply.distance_metric = DistanceMetric::COSINE; // TODO: store and retrieve from table metadata
    lancedb_table_free(table);
    return ret;
  }

}

