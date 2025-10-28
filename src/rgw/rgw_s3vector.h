// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>
#include <vector>
#include "include/encoding.h"
#include "common/async/yield_context.h"

namespace ceph {
class Formatter;
}
class JSONObj;
class DoutPrefixProvider;

namespace rgw::s3vector {
enum class DistanceMetric {
  COSINE,
  EUCLIDEAN,
};

/*
  {
    "dataType": "string",
    "dimension": number,
    "distanceMetric": "string",
    "indexName": "string",
    "metadataConfiguration": {
      "nonFilterableMetadataKeys": [ "string" ]
    },
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct create_index_t {
  std::string data_type;
  unsigned int dimension; /* 1 - 4096 */
  DistanceMetric distance_metric;
  std::string index_name;
  std::vector<std::string> non_filterable_metadata_keys;
  std::string vector_bucket_arn;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(create_index_t)

/*
  {
    "vectorBucketName": "string"
  }
*/
struct create_vector_bucket_t {
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(create_vector_bucket_t)

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string"
  }
*/
struct delete_index_t {
  std::string index_arn;
  std::string index_name;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(delete_index_t)

/*
  {
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct delete_vector_bucket_t {
  std::string vector_bucket_arn;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(delete_vector_bucket_t)

/*
  {
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct delete_vector_bucket_policy_t {
  std::string vector_bucket_arn;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(delete_vector_bucket_policy_t)


using VectorData = std::vector<float>;
/*
  {
    "key": "string",
    "data": {"float32": [float]},
    "metadata": {}
  }
*/
struct vector_item_t {
  std::string key;
  VectorData data;
  std::string metadata; // JSON string

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(vector_item_t)

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string",
    "vectors": [vector_item_t]
  }
*/
struct put_vectors_t {
  std::string index_arn;
  std::string index_name;
  std::string vector_bucket_name;
  std::vector<vector_item_t> vectors;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(put_vectors_t)

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "keys": ["string"],
    "returnData": boolean,
    "returnMetadata": boolean,
    "vectorBucketName": "string"
  }
*/
struct get_vectors_t {
  std::string index_arn;
  std::string index_name;
  std::vector<std::string> keys;
  bool return_data = false;
  bool return_metadata = false;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(get_vectors_t)

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string",
    "maxResults": number,
    "nextToken": "string",
    "returnData": boolean,
    "returnMetadata": boolean,
    "segmentCount": number,
    "segmentIndex": number
  }
*/
struct list_vectors_t {
  std::string index_arn;
  std::string index_name;
  std::string vector_bucket_name;
  static constexpr unsigned int default_max_results = 500;
  unsigned int max_results = default_max_results;
  std::string next_token;
  bool return_data = false;
  bool return_metadata = false;
  unsigned int segment_count = 0;
  unsigned int segment_index = 0;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(list_vectors_t)

/*
  {
    "maxResults": number,
    "nextToken": "string",
    "prefix": "string"
  }
*/
struct list_vector_buckets_t {
  static constexpr unsigned int default_max_results = 500;
  unsigned int max_results = default_max_results;
  std::string next_token;
  std::string prefix;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(list_vector_buckets_t)

/*
  {
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct get_vector_bucket_t {
  std::string vector_bucket_arn;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(get_vector_bucket_t)

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string"
  }
*/
struct get_index_t {
  std::string index_arn;
  std::string index_name;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(get_index_t)

/*
  {
    "maxResults": number,
    "nextToken": "string",
    "prefix": "string",
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct list_indexes_t {
  static constexpr unsigned int default_max_results = 500;
  unsigned int max_results = default_max_results;
  std::string next_token;
  std::string prefix;
  std::string vector_bucket_arn;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(list_indexes_t)

/*
  {
    "policy": "string",
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct put_vector_bucket_policy_t {
  std::string policy;
  std::string vector_bucket_arn;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(put_vector_bucket_policy_t)

/*
  {
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct get_vector_bucket_policy_t {
  std::string vector_bucket_arn;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(get_vector_bucket_policy_t)

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "keys": ["string"],
    "vectorBucketName": "string"
  }
*/
struct delete_vectors_t {
  std::string index_arn;
  std::string index_name;
  std::vector<std::string> keys;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(delete_vectors_t)

/*
  {
    "filter": {},
    "indexArn": "string",
    "indexName": "string",
    "queryVector": {"float32": [float]},
    "returnDistance": boolean,
    "returnMetadata": boolean,
    "topK": number,
    "vectorBucketName": "string"
  }
*/
struct query_vectors_t {
  std::string filter; // JSON string
  std::string index_arn;
  std::string index_name;
  VectorData query_vector;
  bool return_distance = false;
  bool return_metadata = false;
  unsigned int top_k;
  std::string vector_bucket_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // TODO
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    // TODO
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(query_vectors_t)

int create_index(const create_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int create_vector_bucket(const create_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int delete_index(const delete_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int delete_vector_bucket(const delete_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int delete_vector_bucket_policy(const delete_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int put_vectors(const put_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int get_vectors(const get_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int list_vectors(const list_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int list_vector_buckets(const list_vector_buckets_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int get_vector_bucket(const get_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int get_index(const get_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int list_indexes(const list_indexes_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int put_vector_bucket_policy(const put_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int get_vector_bucket_policy(const get_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int delete_vectors(const delete_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int query_vectors(const query_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y);

}

