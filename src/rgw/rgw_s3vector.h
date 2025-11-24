// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>
#include <vector>
#include "include/encoding.h"
#include "rgw_arn.h"
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
  boost::optional<rgw::ARN> vector_bucket_arn;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct create_index_reply_t {
  std::string index_arn;

  void dump(ceph::Formatter* f) const;
};

/*
  {
    "vectorBucketName": "string"
  }
*/
struct create_vector_bucket_t {
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct create_vector_bucket_reply_t {
  std::string vector_bucket_arn;

  void dump(ceph::Formatter* f) const;
};

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string"
  }
*/
struct delete_index_t {
  boost::optional<rgw::ARN> index_arn;
  std::string index_name;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct delete_vector_bucket_t {
  boost::optional<rgw::ARN> vector_bucket_arn;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct delete_vector_bucket_policy_t {
  boost::optional<rgw::ARN> vector_bucket_arn;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

using VectorData = std::vector<float>;
/*
  {
    "distance": "float",
    "key": "string",
    "data": {"float32": ["float"]},
    "metadata": {}
  }
*/
struct vector_item_t {
  std::optional<float> distance;
  std::string key;
  std::optional<VectorData> data;
  std::string metadata; // JSON string

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string",
    "vectors": [vector_item_t]
  }
*/
struct put_vectors_t {
  boost::optional<rgw::ARN> index_arn;
  std::string index_name;
  std::string vector_bucket_name;
  std::vector<vector_item_t> vectors;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string"
    "keys": ["string"],
    "returnData": boolean,
    "returnMetadata": boolean,
  }
*/
struct get_vectors_t {
  boost::optional<rgw::ARN> index_arn;
  std::string index_name;
  std::string vector_bucket_name;
  std::vector<std::string> keys;
  bool return_data = false;
  bool return_metadata = false;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "vectors": [vector_item_t]
  }
*/
struct get_vectors_reply_t {
  std::vector<vector_item_t> vectors;

  void dump(ceph::Formatter* f) const;
};

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
  boost::optional<rgw::ARN> index_arn;
  std::string index_name;
  std::string vector_bucket_name;
  static constexpr unsigned int default_max_results = 500;
  unsigned int max_results = default_max_results;
  unsigned int offset = 0; // from nextToken
  bool return_data = false;
  bool return_metadata = false;
  unsigned int segment_count = 0;
  unsigned int segment_index = 0;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "nextToken": "string",
    "vectors": [vector_item_t]
  }
*/
struct list_vectors_reply_t {
  std::string next_token;
  std::vector<vector_item_t> vectors;

  void dump(ceph::Formatter* f) const;
};

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

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct get_vector_bucket_t {
  boost::optional<rgw::ARN> vector_bucket_arn;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string"
  }
*/
struct get_index_t {
  boost::optional<rgw::ARN> index_arn;
  std::string index_name;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "index": {
      "creationTime": number,
      "dataType": "string",
      "dimension": number,
      "distanceMetric": "string",
      "indexArn": "string",
      "indexName": "string",
      "metadataConfiguration": {
         "nonFilterableMetadataKeys": [ "string" ]
      },
      "vectorBucketName": "string"
    }
  }
 */
struct get_index_reply_t {
  unsigned int creation_time;
  std::string data_type;
  unsigned int dimension; /* 1 - 4096 */
  DistanceMetric distance_metric;
  std::string index_arn;
  std::string index_name;
  std::vector<std::string> non_filterable_metadata_keys;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
};

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
  boost::optional<rgw::ARN> vector_bucket_arn;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
     "creationTime": number,
     "indexArn": "string",
     "indexName": "string",
     "vectorBucketName": "string"
  }
 */
struct index_summary_t {
  unsigned int creation_time;
  std::string index_arn;
  std::string index_name;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "indexes": [
      {
         "creationTime": number,
         "indexArn": "string",
         "indexName": "string",
         "vectorBucketName": "string"
      }
    ],
    "nextToken": "string"
  }
*/
struct list_indexes_reply_t {
  std::vector<index_summary_t> indexes;
  std::string next_token;

  void dump(ceph::Formatter* f) const;
};

/*
  {
    "policy": "string",
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct put_vector_bucket_policy_t {
  std::string policy;
  boost::optional<rgw::ARN> vector_bucket_arn;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "vectorBucketArn": "string",
    "vectorBucketName": "string"
  }
*/
struct get_vector_bucket_policy_t {
  boost::optional<rgw::ARN> vector_bucket_arn;
  std::string vector_bucket_name;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string"
    "keys": ["string"],
  }
*/
struct delete_vectors_t {
  boost::optional<rgw::ARN> index_arn;
  std::string index_name;
  std::string vector_bucket_name;
  std::vector<std::string> keys;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
  {
    "filter": {},
    "indexArn": "string",
    "indexName": "string",
    "vectorBucketName": "string"
    "queryVector": {"float32": [float]},
    "returnDistance": boolean,
    "returnMetadata": boolean,
    "topK": number,
  }
*/
struct query_vectors_t {
  std::string filter; // JSON string
  boost::optional<rgw::ARN> index_arn;
  std::string index_name;
  std::string vector_bucket_name;
  VectorData query_vector;
  bool return_distance = false;
  bool return_metadata = false;
  unsigned int top_k;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

/*
{
  "distanceMetric": "string",
  "vectors": [vector_item_t]
}
*/

struct query_vectors_reply_t {
  DistanceMetric distance_metric;
  std::vector<vector_item_t> vectors;

  void dump(ceph::Formatter* f) const;
};

inline rgw::ARN index_arn(const std::string& zonegroup, const std::string& account, const std::string& vector_bucket_name, std::string_view index_name) {
  return rgw::ARN(rgw::Partition::aws,
      rgw::Service::s3vectors,
      zonegroup,
      account,
      fmt::format("bucket/{}/index/{}", vector_bucket_name, index_name)
    );
}

inline rgw::ARN vector_bucket_arn(const std::string& zonegroup, const std::string& account, const std::string& vector_bucket_name) {
  return rgw::ARN(rgw::Partition::aws,
      rgw::Service::s3vectors,
      zonegroup,
      account,
      fmt::format("bucket/{}", vector_bucket_name)
    );
}

int create_index(const create_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int create_vector_bucket(const create_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int delete_index(const delete_index_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int delete_vector_bucket(const delete_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int delete_vector_bucket_policy(const delete_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int put_vectors(const put_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int get_vectors(const get_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y, get_vectors_reply_t& reply);
int list_vectors(const list_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y, list_vectors_reply_t& reply);
int list_vector_buckets(const list_vector_buckets_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int get_vector_bucket(const get_vector_bucket_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int get_index(const get_index_t& configuration, const std::string& region, const std::string& account, DoutPrefixProvider* dpp, optional_yield y, get_index_reply_t& reply);
int list_indexes(const list_indexes_t& configuration, DoutPrefixProvider* dpp, optional_yield y, list_indexes_reply_t& reply);
int put_vector_bucket_policy(const put_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int get_vector_bucket_policy(const get_vector_bucket_policy_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int delete_vectors(const delete_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y);
int query_vectors(const query_vectors_t& configuration, DoutPrefixProvider* dpp, optional_yield y, query_vectors_reply_t& reply);

}

