// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_s3vector.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "common/dout.h"
#include <fmt/format.h>

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {


// utility functions for JSON encoding/decoding

void decode_json_obj(float& val, JSONObj *obj) {
  std::string s = obj->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtof(start, &p);

  /* Check for various possible errors */

 if ((errno == ERANGE && val == HUGE_VAL) ||
     (errno != 0 && val == 0)) {
   throw JSONDecoder::err("failed to number");
 }

 if (p == start) {
   throw JSONDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw JSONDecoder::err("failed to parse number");
   }
   p++;
 }
}

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

void create_index_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("dataType", data_type, f);
  ::encode_json("dimension", dimension, f);
  rgw::s3vector::encode_json("distanceMetric", distance_metric, f);
  ::encode_json("indexName", index_name, f);
  f->open_object_section("metadataConfiguration");
  ::encode_json("nonFilterableMetadataKeys", non_filterable_metadata_keys, f);
  f->close_section();
  ::encode_json("vectorBucketArn", vector_bucket_arn, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void decode_name(const char* name_field, std::string& name, JSONObj* obj) {
  JSONDecoder::decode_json(name_field, name, obj, true);
  if (name.length() < 3 || name.length() > 63) {
    throw JSONDecoder::err(fmt::format("{} length must be between 3 and 63 characters, got {}", name_field, name.length()));
  }
}

void decode_name_or_arn(const char* name_field, const char* arn_field, std::string& name, std::string& arn, JSONObj* obj) {
  JSONDecoder::decode_json(arn_field, arn, obj);
  JSONDecoder::decode_json(name_field, name, obj);
  if (arn.empty() && name.empty()) {
    throw JSONDecoder::err(fmt::format("either {} or {} must be specified", name_field, arn_field));
  }
  if (!name.empty() && (name.length() < 3 || name.length() > 63)) {
    throw JSONDecoder::err(fmt::format("{} length must be between 3 and 63 characters, got {}", name_field, name.length()));
  }
  //TODO: validate ARN
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
  decode_name_or_arn("vectorBucketName", "vectorBucketArn", vector_bucket_name, vector_bucket_arn, obj);
}

void create_vector_bucket_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void create_vector_bucket_t::decode_json(JSONObj* obj) {
  decode_name("vectorBucketName", vector_bucket_name, obj);
}

int create_index(const create_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector CreateIndex with: " << ss.str() << dendl;
    return 0;
}

void delete_index_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("indexArn", index_arn, f);
  ::encode_json("indexName", index_name, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void delete_index_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("indexName", "indexArn", index_name, index_arn, obj);
  decode_name("vectorBucketName", vector_bucket_name, obj);
}

void delete_vector_bucket_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("vectorBucketArn", vector_bucket_arn, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void delete_vector_bucket_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("vectorBucketName", "vectorBucketArn", vector_bucket_name, vector_bucket_arn, obj);
}

void delete_vector_bucket_policy_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void delete_vector_bucket_policy_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("vectorBucketName", "vectorBucketArn", vector_bucket_name, vector_bucket_arn, obj);
}

int create_vector_bucket(const create_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector CreateVectorBucket with: " << ss.str() << dendl;
    return 0;
}

int delete_index(const delete_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector DeleteIndex with: " << ss.str() << dendl;
    return 0;
}

int delete_vector_bucket(const delete_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector DeleteVectorBucket with: " << ss.str() << dendl;
    return 0;
}

void vector_item_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("key", key, f);
  rgw::s3vector::encode_json("data", data, f);
  ::encode_json("metadata", metadata, f);
  f->close_section();
}

void vector_item_t::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("key", key, obj, true);
  if (key.empty()) {
    throw JSONDecoder::err("vector key must be specified");
  }

  rgw::s3vector::decode_json("data", data, obj);
  if (data.empty()) {
    throw JSONDecoder::err("vector data cannot be empty");
  }

  JSONDecoder::decode_json("metadata", metadata, obj);

}

void put_vectors_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("indexArn", index_arn, f);
  ::encode_json("indexName", index_name, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->open_array_section("vectors");
  for (const auto& vector : vectors) {
    vector.dump(f);
  }
  f->close_section();
  f->close_section();
}

void put_vectors_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("indexName", "indexArn", index_name, index_arn, obj);
  decode_name("vectorBucketName", vector_bucket_name, obj);
  JSONDecoder::decode_json("vectors", vectors, obj, true);

  if (vectors.empty() or vectors.size() > 500) {
    throw JSONDecoder::err(fmt::format("vectors array must contain 1-500 items, got {}", vectors.size()));
  }
}

int delete_vector_bucket_policy(const delete_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector DeleteVectorBucketPolicy with: " << ss.str() << dendl;
    return 0;
}

void get_vectors_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("indexArn", index_arn, f);
  ::encode_json("indexName", index_name, f);
  ::encode_json("keys", keys, f);
  ::encode_json("returnData", return_data, f);
  ::encode_json("returnMetadata", return_metadata, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void get_vectors_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("indexName", "indexArn", index_name, index_arn, obj);
  JSONDecoder::decode_json("keys", keys, obj, true);
  JSONDecoder::decode_json("returnData", return_data, obj);
  JSONDecoder::decode_json("returnMetadata", return_metadata, obj);
  decode_name("vectorBucketName", vector_bucket_name, obj);

  if (keys.empty() || keys.size() > 100) {
    throw JSONDecoder::err(fmt::format("keys array must contain 1-100 items, got {}", keys.size()));
  }

  for (const auto& key : keys) {
    if (key.empty() || key.length() > 1024) {
      throw JSONDecoder::err(fmt::format("each key must be 1-1024 characters long, got key of length {}", key.length()));
    }
  }
}

int put_vectors(const put_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector PutVectors with: " << ss.str() << dendl;
    return 0;
}

void list_vectors_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("indexArn", index_arn, f);
  ::encode_json("indexName", index_name, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  ::encode_json("maxResults", max_results, f);
  ::encode_json("nextToken", next_token, f);
  ::encode_json("returnData", return_data, f);
  ::encode_json("returnMetadata", return_metadata, f);
  if (segment_count > 0) {
    ::encode_json("segmentCount", segment_count, f);
    ::encode_json("segmentIndex", segment_index, f);
  }
  f->close_section();
}

void list_vectors_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("indexName", "indexArn", index_name, index_arn, obj);
  decode_name("vectorBucketName", vector_bucket_name, obj);
  JSONDecoder::decode_json("maxResults", max_results, obj);
  JSONDecoder::decode_json("nextToken", next_token, obj);
  JSONDecoder::decode_json("returnData", return_data, obj);
  JSONDecoder::decode_json("returnMetadata", return_metadata, obj);
  JSONDecoder::decode_json("segmentCount", segment_count, obj);
  JSONDecoder::decode_json("segmentIndex", segment_index, obj);

  if (max_results < 1 || max_results > 1000) {
    throw JSONDecoder::err(fmt::format("maxResults must be between 1 and 1000, got {}", max_results));
  }

  if (!next_token.empty() && (next_token.length() < 1 || next_token.length() > 2048)) {
    throw JSONDecoder::err(fmt::format("nextToken length must be between 1 and 2048, got {}", next_token.length()));
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

int get_vectors(const get_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector GetVectors with: " << ss.str() << dendl;
    return 0;
}

void list_vector_buckets_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("maxResults", max_results, f);
  ::encode_json("nextToken", next_token, f);
  ::encode_json("prefix", prefix, f);
  f->close_section();
}

void list_vector_buckets_t::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("maxResults", max_results, obj);
  JSONDecoder::decode_json("nextToken", next_token, obj);
  JSONDecoder::decode_json("prefix", prefix, obj);

  if (max_results < 1 || max_results > 1000) {
    throw JSONDecoder::err(fmt::format("maxResults must be between 1 and 1000, got {}", max_results));
  }

  if (!next_token.empty() && (next_token.length() < 1 || next_token.length() > 2048)) {
    throw JSONDecoder::err(fmt::format("nextToken length must be between 1 and 2048, got {}", next_token.length()));
  }

  if (!prefix.empty() && (prefix.length() < 1 || prefix.length() > 63)) {
    throw JSONDecoder::err(fmt::format("prefix length must be between 1 and 63, got {}", prefix.length()));
  }
}

int list_vectors(const list_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector ListVectors with: " << ss.str() << dendl;
    return 0;
}

void get_vector_bucket_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("vectorBucketArn", vector_bucket_arn, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void get_vector_bucket_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("vectorBucketName", "vectorBucketArn", vector_bucket_name, vector_bucket_arn, obj);
}

int list_vector_buckets(const list_vector_buckets_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector ListVectorBuckets with: " << ss.str() << dendl;
    return 0;
}

void get_index_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("indexArn", index_arn, f);
  ::encode_json("indexName", index_name, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void get_index_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("indexName", "indexArn", index_name, index_arn, obj);
  decode_name("vectorBucketName", vector_bucket_name, obj);
}

int get_vector_bucket(const get_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector GetVectorBucket with: " << ss.str() << dendl;
    return 0;
}

void list_indexes_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("maxResults", max_results, f);
  ::encode_json("nextToken", next_token, f);
  ::encode_json("prefix", prefix, f);
  ::encode_json("vectorBucketArn", vector_bucket_arn, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void list_indexes_t::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("maxResults", max_results, obj);
  JSONDecoder::decode_json("nextToken", next_token, obj);
  JSONDecoder::decode_json("prefix", prefix, obj);
  decode_name_or_arn("vectorBucketName", "vectorBucketArn", vector_bucket_name, vector_bucket_arn, obj);

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

int get_index(const get_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector GetIndex with: " << ss.str() << dendl;
    return 0;
}

void put_vector_bucket_policy_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("policy", policy, f);
  ::encode_json("vectorBucketArn", vector_bucket_arn, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void put_vector_bucket_policy_t::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("policy", policy, obj, true);
  decode_name_or_arn("vectorBucketName", "vectorBucketArn", vector_bucket_name, vector_bucket_arn, obj);

  if (policy.empty()) {
    throw JSONDecoder::err("policy must be specified and cannot be empty");
  }
  // TODO: validate JSON policy
}

int list_indexes(const list_indexes_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector ListIndexes with: " << ss.str() << dendl;
    return 0;
}

void get_vector_bucket_policy_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("vectorBucketArn", vector_bucket_arn, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void get_vector_bucket_policy_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("vectorBucketName", "vectorBucketArn", vector_bucket_name, vector_bucket_arn, obj);
}

int put_vector_bucket_policy(const put_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector PutVectorBucketPolicy with: " << ss.str() << dendl;
    return 0;
}

int get_vector_bucket_policy(const get_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector GetVectorBucketPolicy with: " << ss.str() << dendl;
    return 0;
}

void delete_vectors_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  ::encode_json("indexArn", index_arn, f);
  ::encode_json("indexName", index_name, f);
  ::encode_json("keys", keys, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void delete_vectors_t::decode_json(JSONObj* obj) {
  decode_name_or_arn("indexName", "indexArn", index_name, index_arn, obj);
  JSONDecoder::decode_json("keys", keys, obj, true);
  decode_name("vectorBucketName", vector_bucket_name, obj);

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
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector DeleteVectors with: " << ss.str() << dendl;
    return 0;
}

void query_vectors_t::dump(ceph::Formatter* f) const {
  f->open_object_section("");
  if (!filter.empty()) {
    ::encode_json("filter", filter, f);
  }
  ::encode_json("indexArn", index_arn, f);
  ::encode_json("indexName", index_name, f);
  rgw::s3vector::encode_json("queryVector", query_vector, f);
  ::encode_json("returnDistance", return_distance, f);
  ::encode_json("returnMetadata", return_metadata, f);
  ::encode_json("topK", top_k, f);
  ::encode_json("vectorBucketName", vector_bucket_name, f);
  f->close_section();
}

void query_vectors_t::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("filter", filter, obj);
  decode_name_or_arn("indexName", "indexArn", index_name, index_arn, obj);
  rgw::s3vector::decode_json("queryVector", query_vector, obj);
  JSONDecoder::decode_json("returnDistance", return_distance, obj);
  JSONDecoder::decode_json("returnMetadata", return_metadata, obj);
  JSONDecoder::decode_json("topK", top_k, obj, true);
  decode_name("vectorBucketName", vector_bucket_name, obj);

  if (top_k < 1) {
    throw JSONDecoder::err(fmt::format("topK must be at least 1, got {}", top_k));
  }

  if (query_vector.empty()) {
    throw JSONDecoder::err("queryVector cannot be empty");
  }

  // TODO: validate filter
}

int query_vectors(const query_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y) {
    JSONFormatter f;
    configuration.dump(&f);
    std::stringstream ss;
    f.flush(ss);
    ldpp_dout(dpp, 20) << "INFO: executing s3vector QueryVectors with: " << ss.str() << dendl;
    return 0;
}

}

