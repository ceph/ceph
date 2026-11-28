// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

// parsing of the JSON payload of the s3vector PutVectors request.
// the payload holds the vectors, and may be large. so, it is parsed in a single
// pass, directly into the arrow arrays that are written to the index, without
// building a tree of JSON objects, or an intermediate representation of the vectors.
// the structure of the payload is fixed and known, and anything else is an error:
// a payload that is not valid JSON fails the parsing, and anything else that is
// invalid is kept as a validation error, to be sent to the client.

#include "rgw_s3vector.h"
#include "common/dout.h"
#include "common/ceph_context.h"
#include <arrow/api.h>
#include <fmt/format.h>
#include <glaze/json.hpp>
#include <algorithm>
#include <cmath>

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {

namespace {

  // values that are skipped are verified to be valid JSON
  struct json_opts_t : glz::opts {
    bool validate_skipped = true;
  };
  constexpr json_opts_t json_opts{};

  // the functions below use the JSON reader of glaze on a null terminated buffer.
  // "it" points to the next character to read, and errors are set in the context

  void skip_ws(glz::context& ctx, const char*& it, const char* end) {
    glz::skip_ws<json_opts>(ctx, it, end);
  }

  void skip_value(glz::context& ctx, const char*& it, const char* end) {
    glz::skip_value<glz::JSON>::op<json_opts>(ctx, it, end);
  }

  template<typename T>
  void parse_value(T& value, glz::context& ctx, const char*& it, const char* end) {
    glz::parse<glz::JSON>::op<json_opts>(value, ctx, it, end);
  }

  // parse a JSON object. on_field() is called for each of its fields, after the
  // name of the field was read into "name", with "it" pointing to the value of
  // the field. on_field() has to read the value, or to set an error
  template<typename OnField>
  void parse_object(std::string& name, glz::context& ctx, const char*& it, const char* end, OnField&& on_field) {
    skip_ws(ctx, it, end);
    if (glz::match<'{'>(ctx, it)) {
      return;
    }
    skip_ws(ctx, it, end);
    if (*it == '}') {
      ++it;
      return;
    }
    while (true) {
      parse_value(name, ctx, it, end);
      if (bool(ctx.error)) {
        return;
      }
      skip_ws(ctx, it, end);
      if (glz::match<':'>(ctx, it)) {
        return;
      }
      skip_ws(ctx, it, end);
      on_field();
      if (bool(ctx.error)) {
        return;
      }
      skip_ws(ctx, it, end);
      if (*it != ',') {
        break;
      }
      ++it;
      skip_ws(ctx, it, end);
    }
    glz::match<'}'>(ctx, it);
  }

  // parse a JSON array. on_item() is called for each of its items, with "it"
  // pointing to the item. on_item() has to read the item, or to set an error
  template<typename OnItem>
  void parse_array(glz::context& ctx, const char*& it, const char* end, OnItem&& on_item) {
    skip_ws(ctx, it, end);
    if (glz::match<'['>(ctx, it)) {
      return;
    }
    skip_ws(ctx, it, end);
    if (*it == ']') {
      ++it;
      return;
    }
    while (true) {
      on_item();
      if (bool(ctx.error)) {
        return;
      }
      skip_ws(ctx, it, end);
      if (*it != ',') {
        break;
      }
      ++it;
      skip_ws(ctx, it, end);
    }
    glz::match<']'>(ctx, it);
  }

  bool check(const arrow::Status& status, glz::context& ctx) {
    if (!status.ok()) [[unlikely]] {
      ctx.error = glz::error_code::constraint_violated;
      ctx.custom_error_message = "failed to add value to arrow array";
      return false;
    }
    return true;
  }

  // once a vector is found to be invalid, the vectors are not converted anymore
  bool is_invalid(const put_vectors_data_t& data) {
    return !data.errors.empty() || data.dimension_mismatch;
  }

  // add a validation error. only the first one is kept
  void add_error(put_vectors_data_t& data, std::string path, std::string message) {
    if (!is_invalid(data)) {
      data.errors.push_back({std::move(path), std::move(message)});
    }
  }

  // add a validation error for the value that "it" points to, and skip the value.
  // the value still has to be valid JSON, otherwise an error is set in the context
  void invalid_value(put_vectors_data_t& data, std::string path, std::string message,
      glz::context& ctx, const char*& it, const char* end) {
    add_error(data, std::move(path), std::move(message));
    skip_value(ctx, it, end);
  }

  // parse the values of a vector: [float, ...] into the array of values.
  // the first vector sets the dimension, the other vectors must match it
  void parse_vector_values(arrow::FloatBuilder& values, put_vectors_data_t& data,
      glz::context& ctx, const char*& it, const char* end) {
    const auto path = [&data] { return fmt::format("vectors[{}].data", data.num_vectors); };
    if (*it != '[') {
      invalid_value(data, path(), "float32 must be an array of numbers", ctx, it, end);
      return;
    }
    const char* const start = it;
    const bool first = (data.dimension == 0);
    // the values of the vectors that follow the first one are added without checking for space
    if (!first && !check(values.Reserve(data.dimension), ctx)) {
      return;
    }
    size_t count = 0;
    parse_array(ctx, it, end, [&] {
      float value;
      parse_value(value, ctx, it, end);
      if (bool(ctx.error)) {
        return;
      }
      if (!std::isfinite(value)) {
        ctx.error = glz::error_code::parse_number_failure;
        return;
      }
      if (first) {
        check(values.Append(value), ctx);
      } else if (count < data.dimension) {
        values.UnsafeAppend(value);
      }
      ++count;
    });
    if (ctx.error == glz::error_code::constraint_violated) {
      return;
    }
    if (bool(ctx.error)) {
      // the array is either not valid JSON, or holds values that are not float numbers
      ctx.error = glz::error_code::none;
      ctx.custom_error_message = {};
      it = start;
      invalid_value(data, path(), "float32 must be an array of numbers", ctx, it, end);
      return;
    }
    if (count == 0) {
      add_error(data, path(), "must not be empty");
    } else if (first) {
      data.dimension = count;
    } else if (count != data.dimension) {
      // reported once the dimension of the index is known
      data.dimension_mismatch.emplace(data.num_vectors, count);
    }
  }

  // verify the top level fields of the metadata object of a vector, without storing their values.
  // "it" points to the opening brace of the object, and is moved past the closing one.
  // returns false if the JSON is malformed, in which case the error is set in the context,
  // or if the metadata is invalid, in which case a validation error is added.
  // "field_names" is reused between the vectors, to avoid allocations
  bool validate_metadata(std::vector<std::string>& field_names, put_vectors_data_t& data,
      glz::context& ctx, const char*& it, const char* end) {
    const auto vi = data.num_vectors;
    ++it;
    skip_ws(ctx, it, end);
    if (*it == '}') {
      ++it;
      return true;
    }
    size_t num_fields = 0;
    while (true) {
      if (field_names.size() == num_fields) {
        field_names.emplace_back();
      }
      auto& name = field_names[num_fields];
      parse_value(name, ctx, it, end);
      if (bool(ctx.error)) {
        return false;
      }
      const auto names_end = field_names.begin() + num_fields;
      if (std::find(field_names.begin(), names_end, name) != names_end) {
        data.errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, name), "field name must not be duplicated"});
        return false;
      }
      if (++num_fields > max_metadata_keys) {
        data.errors.push_back({fmt::format("vectors[{}].metadata", vi),
            fmt::format("must not have more than {} fields", max_metadata_keys)});
        return false;
      }
      if (const auto result = validate_metadata_key_name(name); !result) {
        data.errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, name), fmt::format("field name {}", result.error())});
        return false;
      }
      skip_ws(ctx, it, end);
      if (glz::match<':'>(ctx, it)) {
        return false;
      }
      skip_ws(ctx, it, end);
      if (*it == 'n') {
        data.errors.push_back({fmt::format("vectors[{}].metadata.{}", vi, name), "null values are not supported"});
        return false;
      }
      skip_value(ctx, it, end);
      if (bool(ctx.error)) {
        return false;
      }
      skip_ws(ctx, it, end);
      if (*it != ',') {
        break;
      }
      ++it;
      skip_ws(ctx, it, end);
    }
    return !glz::match<'}'>(ctx, it);
  }

  // parse the metadata of a vector into the array of metadata.
  // the metadata is either a JSON object, or a string that holds a JSON object.
  // "metadata_str" and "field_names" are reused between the vectors, to avoid allocations
  void parse_vector_metadata(arrow::StringBuilder& metadata, put_vectors_data_t& data,
      std::string& metadata_str, std::vector<std::string>& field_names,
      glz::context& ctx, const char*& it, const char* end) {
    const auto vi = data.num_vectors;
    std::string_view value;
    if (*it == '{') {
      const char* const start = it;
      if (!validate_metadata(field_names, data, ctx, it, end)) {
        if (!bool(ctx.error)) {
          // the metadata is invalid, and was only partially read
          it = start;
          skip_value(ctx, it, end);
        }
        return;
      }
      value = std::string_view(start, it - start);
    } else if (*it == '"') {
      parse_value(metadata_str, ctx, it, end);
      if (bool(ctx.error)) {
        return;
      }
      if (metadata_str.empty()) {
        check(metadata.AppendNull(), ctx);
        return;
      }
      // malformed JSON inside the string does not make the payload malformed
      glz::context metadata_ctx{};
      const char* metadata_it = metadata_str.c_str();
      const char* const metadata_end = metadata_it + metadata_str.size();
      skip_ws(metadata_ctx, metadata_it, metadata_end);
      bool valid = (*metadata_it == '{');
      if (valid) {
        valid = validate_metadata(field_names, data, metadata_ctx, metadata_it, metadata_end);
        if (is_invalid(data)) {
          return;
        }
      }
      if (valid) {
        skip_ws(metadata_ctx, metadata_it, metadata_end);
        valid = (metadata_it == metadata_end);
      }
      if (!valid) {
        data.errors.push_back({fmt::format("vectors[{}].metadata", vi), "invalid JSON"});
        return;
      }
      value = metadata_str;
    } else {
      skip_value(ctx, it, end);
      data.errors.push_back({fmt::format("vectors[{}].metadata", vi), "invalid JSON"});
      return;
    }
    if (value.size() > max_metadata_size) {
      data.errors.push_back({fmt::format("vectors[{}].metadata", vi),
          fmt::format("size ({}) must not exceed {} bytes", value.size(), max_metadata_size)});
      return;
    }
    check(metadata.Append(value), ctx);
  }

  // parse a vector: {"key": "string", "data": {"float32": [float, ...]}, "metadata": {...}}
  // into the arrays. fields may come in any order, and the metadata is optional.
  // "name", "key", "metadata_str" and "field_names" are reused between the vectors, to avoid allocations
  void parse_vector(arrow::StringBuilder& keys, arrow::FloatBuilder& values, arrow::StringBuilder& metadata,
      put_vectors_data_t& data,
      std::string& name, std::string& key, std::string& metadata_str, std::vector<std::string>& field_names,
      glz::context& ctx, const char*& it, const char* end) {
    const auto vi = data.num_vectors;
    if (*it != '{') {
      invalid_value(data, fmt::format("vectors[{}]", vi), "must be an object", ctx, it, end);
      return;
    }
    bool has_key = false;
    bool has_data = false;
    bool has_values = false;
    bool has_metadata = false;
    // a field cannot be repeated, as it would be added twice to its array
    const auto unexpected = [](bool duplicate) {
      return duplicate ? "field must not be duplicated" : "unknown field";
    };
    parse_object(name, ctx, it, end, [&] {
      if (name == "key" && !has_key) {
        has_key = true;
        if (*it != '"') {
          invalid_value(data, fmt::format("vectors[{}].key", vi), "must be a string", ctx, it, end);
          return;
        }
        if (is_invalid(data)) {
          skip_value(ctx, it, end);
          return;
        }
        parse_value(key, ctx, it, end);
        if (bool(ctx.error)) {
          return;
        }
        if (key.empty()) {
          add_error(data, fmt::format("vectors[{}].key", vi), "must not be empty");
          return;
        }
        check(keys.Append(key), ctx);
      } else if (name == "data" && !has_data) {
        has_data = true;
        if (*it != '{') {
          invalid_value(data, fmt::format("vectors[{}].data", vi), "must be an object", ctx, it, end);
          return;
        }
        parse_object(name, ctx, it, end, [&] {
          if (name != "float32" || has_values) {
            invalid_value(data, fmt::format("vectors[{}].data.{}", vi, name), unexpected(name == "float32"), ctx, it, end);
            return;
          }
          has_values = true;
          if (is_invalid(data)) {
            skip_value(ctx, it, end);
            return;
          }
          parse_vector_values(values, data, ctx, it, end);
        });
      } else if (name == "metadata" && !has_metadata) {
        has_metadata = true;
        if (is_invalid(data)) {
          skip_value(ctx, it, end);
          return;
        }
        parse_vector_metadata(metadata, data, metadata_str, field_names, ctx, it, end);
      } else {
        invalid_value(data, fmt::format("vectors[{}].{}", vi, name),
            unexpected(name == "key" || name == "data" || name == "metadata"), ctx, it, end);
      }
    });
    if (bool(ctx.error) || is_invalid(data)) {
      return;
    }
    if (!has_key) {
      add_error(data, fmt::format("vectors[{}].key", vi), "must not be empty");
      return;
    }
    if (!has_values) {
      add_error(data, fmt::format("vectors[{}].data", vi), "missing data");
      return;
    }
    if (!has_metadata && !check(metadata.AppendNull(), ctx)) {
      return;
    }
    ++data.num_rows;
  }

} // anonymous namespace

  int parse_put_vectors(const DoutPrefixProvider* dpp, std::string_view payload, put_vectors_t& request) {
    const size_t max_vectors = dpp->get_cct()->_conf->rgw_s3vector_max_put_vectors;
    auto& data = request.data;
    data = {};

    // the builders of the arrays that are filled while parsing
    arrow::StringBuilder keys;
    arrow::FloatBuilder values;
    arrow::StringBuilder metadata;
    // strings that are reused while parsing, to avoid allocations
    std::string name;
    std::string key;
    std::string metadata_str;
    std::vector<std::string> field_names;

    std::string index_arn;
    request.index_arn = boost::none;
    request.index_name.clear();
    request.vector_bucket_name.clear();
    bool has_vectors = false;

    glz::context ctx{};
    const char* it = payload.data();
    const char* const end = it + payload.size();
    parse_object(name, ctx, it, end, [&] {
      if (name == "vectors" && !has_vectors) {
        has_vectors = true;
        if (*it != '[') {
          invalid_value(data, "vectors", "must be an array", ctx, it, end);
          return;
        }
        parse_array(ctx, it, end, [&] {
          if (is_invalid(data) || data.num_vectors >= max_vectors) {
            // nothing is going to be written, only the structure of the vector is verified
            skip_value(ctx, it, end);
          } else {
            parse_vector(keys, values, metadata, data, name, key, metadata_str, field_names, ctx, it, end);
          }
          if (!bool(ctx.error)) {
            ++data.num_vectors;
          }
        });
      } else if (name == "indexName") {
        parse_value(request.index_name, ctx, it, end);
      } else if (name == "vectorBucketName") {
        parse_value(request.vector_bucket_name, ctx, it, end);
      } else if (name == "indexArn") {
        parse_value(index_arn, ctx, it, end);
      } else {
        invalid_value(data, name, name == "vectors" ? "field must not be duplicated" : "unknown field", ctx, it, end);
      }
    });
    if (!bool(ctx.error)) {
      check(keys.Finish(&data.keys), ctx) &&
        check(values.Finish(&data.values), ctx) &&
        check(metadata.Finish(&data.metadata), ctx);
    }
    if (bool(ctx.error)) {
      ldpp_dout(dpp, 1) << "ERROR: failed to parse JSON s3vector PutVectors payload: "
        << glz::format_error(glz::error_ctx{size_t(it - payload.data()), ctx.error, ctx.custom_error_message}, payload) << dendl;
      data = {};
      return -EINVAL;
    }
    // the number of vectors is verified in put_vectors()
    return resolve_index_name(dpp, index_arn, request.vector_bucket_name, request.index_name, request.index_arn);
  }

} // namespace rgw::s3vector
