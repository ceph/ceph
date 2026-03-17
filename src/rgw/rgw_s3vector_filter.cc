// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_s3vector_filter.h"
#include "common/ceph_json.h"
#include "common/dout.h"
#include "lancedb.h"
#include <charconv>
#include <fmt/format.h>

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {

  static constexpr const char* metadata_field = "metadata";

  const filterable_metadata_key_t* find_filterable_key(
      const std::string& field_name,
      const std::vector<filterable_metadata_key_t>& filterable_keys) {
    for (const auto& fk : filterable_keys) {
      if (fk.name == field_name) return &fk;
    }
    return nullptr;
  }

  bool is_nonfilterable_key(
      const std::string& field_name,
      const std::vector<std::string>& nonfilterable_keys) {
    for (const auto& nfk : nonfilterable_keys) {
      if (nfk == field_name) return true;
    }
    return false;
  }

  LanceDBExpr* build_literal_expr(JSONObj* value_obj, FilterableMetadataType type,
      const std::string& field_name, DoutPrefixProvider* dpp,
      std::vector<validation_error_t>& errors) {
    const auto& dv = value_obj->get_data_val();
    switch (type) {
      case FilterableMetadataType::STRING:
        return lancedb_expr_literal_string(dv.str.c_str());
      case FilterableMetadataType::NUMBER: {
        double val;
        const auto [ptr, ec] = std::from_chars(dv.str.data(), dv.str.data() + dv.str.size(), val);
        if (ec != std::errc()) {
          ldpp_dout(dpp, 1) << "ERROR: s3vector filter: invalid number value '" << dv.str << "'" << dendl;
          errors.push_back({"filter", fmt::format("invalid number value '{}' for field '{}'", dv.str, field_name)});
          return nullptr;
        }
        return lancedb_expr_literal_f64(val);
      }
      case FilterableMetadataType::BOOLEAN:
        if (dv.str == "true") return lancedb_expr_literal_bool(true);
        if (dv.str == "false") return lancedb_expr_literal_bool(false);
        ldpp_dout(dpp, 1) << "ERROR: s3vector filter: invalid boolean value '" << dv.str << "'" << dendl;
        errors.push_back({"filter", fmt::format("invalid boolean value '{}' for field '{}'", dv.str, field_name)});
        return nullptr;
      default:
        ldpp_dout(dpp, 1) << "ERROR: s3vector filter: filtering not supported on list-type columns" << dendl;
        errors.push_back({"filter", fmt::format("filtering not supported on list-type metadata key '{}'", field_name)});
        return nullptr;
    }
  }

  enum class JsonValueType { STRING, NUMBER, BOOLEAN };

  std::optional<JsonValueType> infer_value_type(JSONObj* value_obj) {
    if (value_obj->is_object() || value_obj->is_array()) return std::nullopt;
    const auto& dv = value_obj->get_data_val();
    if (dv.quoted) return JsonValueType::STRING;
    if (dv.str == "true" || dv.str == "false") return JsonValueType::BOOLEAN;
    if (dv.str == "null") return std::nullopt;
    return JsonValueType::NUMBER;
  }

  LanceDBExpr* build_json_field_expr(const std::string& field_name, JsonValueType vtype) {
    const char* path = field_name.c_str();
    auto* col = lancedb_expr_column(metadata_field);
    switch (vtype) {
      case JsonValueType::STRING: return lancedb_expr_json_get_str(col, &path, 1);
      case JsonValueType::NUMBER: return lancedb_expr_json_get_float(col, &path, 1);
      case JsonValueType::BOOLEAN: return lancedb_expr_json_get_bool(col, &path, 1);
    }
    return nullptr;
  }

  LanceDBExpr* build_json_literal_expr(JSONObj* value_obj,
      const std::string& field_name, DoutPrefixProvider* dpp,
      std::vector<validation_error_t>& errors) {
    const auto& dv = value_obj->get_data_val();
    if (dv.quoted) return lancedb_expr_literal_string(dv.str.c_str());
    if (dv.str == "true") return lancedb_expr_literal_bool(true);
    if (dv.str == "false") return lancedb_expr_literal_bool(false);
    double val;
    auto [ptr, ec] = std::from_chars(dv.str.data(), dv.str.data() + dv.str.size(), val);
    if (ec == std::errc()) return lancedb_expr_literal_f64(val);
    ldpp_dout(dpp, 1) << "ERROR: s3vector filter: invalid literal value '" << dv.str << "'" << dendl;
    errors.push_back({"filter", fmt::format("invalid value '{}' for field '{}'", dv.str, field_name)});
    return nullptr;
  }

  static const auto invalid_binary_op = static_cast<LanceDBBinaryOp>(-1);

  LanceDBBinaryOp s3vector_to_lance_op(const std::string& op) {
    if (op == "$eq") return LANCEDB_BINARY_OP_EQ;
    if (op == "$ne") return LANCEDB_BINARY_OP_NOT_EQ;
    if (op == "$gt") return LANCEDB_BINARY_OP_GT;
    if (op == "$gte") return LANCEDB_BINARY_OP_GT_EQ;
    if (op == "$lt") return LANCEDB_BINARY_OP_LT;
    if (op == "$lte") return LANCEDB_BINARY_OP_LT_EQ;
    return invalid_binary_op;
  }

  LanceDBExpr* build_exists_expr(
      const std::string& field_name,
      JSONObj* value_obj,
      const filterable_metadata_key_t* fk,
      DoutPrefixProvider* dpp,
      std::vector<validation_error_t>& errors) {
    bool exists;
    const auto& dv = value_obj->get_data_val();
    if (dv.quoted || value_obj->is_object() || value_obj->is_array()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector filter: $exists operator requires a boolean value for field '" << field_name << "'" << dendl;
      errors.push_back({"filter", fmt::format("$exists operator requires a boolean value for field '{}'", field_name)});
      return nullptr;
    }
    if (dv.str == "true") exists = true;
    else if (dv.str == "false") exists = false;
    else {
      ldpp_dout(dpp, 1) << "ERROR: s3vector filter: $exists operator requires a boolean value for field '" << field_name << "'" << dendl;
      errors.push_back({"filter", fmt::format("$exists operator requires a boolean value for field '{}'", field_name)});
      return nullptr;
    }
    if (fk) {
      if (fk->must_exist) {
        // if column must exist, we can create a const boolean expression
        return lancedb_expr_literal_bool(exists);
      }
      auto* col = lancedb_expr_column(fk->name.c_str());
      // build an expression that checks if the column is null or not
      return exists ? lancedb_expr_is_not_null(col) : lancedb_expr_is_null(col);
    }
    const char* path = field_name.c_str();
    auto* col = lancedb_expr_column(metadata_field);
    auto* contains = lancedb_expr_json_contains(col, &path, 1);
    // build an expression that checks if the metadata JSON has this key
    return exists ? contains : lancedb_expr_not(contains);
  }

  LanceDBExpr* build_list_expr(
      const std::string& field_name,
      bool negated,
      JSONObj* value_obj,
      const filterable_metadata_key_t* fk,
      DoutPrefixProvider* dpp,
      std::vector<validation_error_t>& errors) {
    if (!value_obj->is_array()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector filter: $" << (negated ? "nin" : "in") << " requires an array of values for field '" << field_name << "'" << dendl;
      errors.push_back({"filter", fmt::format("${} requires an array of values for field '{}'", negated ? "nin" : "in", field_name)});
      return nullptr;
    }
    std::vector<LanceDBExpr*> list_exprs;
    JsonValueType vtype{}; // will be initialized by the first element if field is not a column
    for (auto it = value_obj->find_first(); !it.end(); ++it) {
      auto* elem = *it;
      if (!fk) {
        auto elem_type = infer_value_type(elem);
        if (!elem_type) {
          ldpp_dout(dpp, 1) << "ERROR: s3vector filter: unsupported value type for field '" << field_name << "'" << dendl;
          errors.push_back({"filter", fmt::format("unsupported value type for field '{}'", field_name)});
          for (auto* e : list_exprs) lancedb_expr_free(e);
          return nullptr;
        }
        if (list_exprs.empty()) {
          vtype = *elem_type;
        } else if (*elem_type != vtype) {
          ldpp_dout(dpp, 1) << "ERROR: s3vector filter: mixed types in list for field '" << field_name << "'" << dendl;
          errors.push_back({"filter", fmt::format("mixed types in list for field '{}'", field_name)});
          for (auto* e : list_exprs) lancedb_expr_free(e);
          return nullptr;
        }
      }
      auto* elem_expr = fk ? build_literal_expr(elem, fk->type, field_name, dpp, errors) : build_json_literal_expr(elem, field_name, dpp, errors);
      if (!elem_expr) {
        for (auto* e : list_exprs) lancedb_expr_free(e);
        return nullptr;
      }
      list_exprs.push_back(elem_expr);
    }
    if (list_exprs.empty()) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector filter: empty list for field '" << field_name << "'" << dendl;
      errors.push_back({"filter", fmt::format("empty list for field '{}'", field_name)});
      return nullptr;
    }

    LanceDBExpr* field_expr;
    if (!fk) {
      field_expr = build_json_field_expr(field_name, vtype);
    } else {
      field_expr = lancedb_expr_column(fk->name.c_str());
    }

    char* error_message = nullptr;
    auto* result = lancedb_expr_in_list(field_expr, list_exprs.data(), list_exprs.size(), negated, &error_message);
    if (!result) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector filter: failed to build list expression: " << (error_message ? error_message : "unknown") << dendl;
      errors.push_back({"filter", fmt::format("failed to build list expression for field '{}'", field_name)});
      lancedb_free_string(error_message);
    }
    return result;
  }

  LanceDBExpr* build_binary_expr(
      const std::string& field_name,
      const std::string& op,
      JSONObj* value_obj,
      const filterable_metadata_key_t* fk,
      DoutPrefixProvider* dpp,
      std::vector<validation_error_t>& errors) {
    auto binary_op = s3vector_to_lance_op(op);
    if (binary_op == invalid_binary_op) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector filter: unknown operator '" << op << "'" << dendl;
      errors.push_back({"filter", fmt::format("unknown filter operator '{}'", op)});
      return nullptr;
    }

    LanceDBExpr* field_expr;
    LanceDBExpr* literal_expr;
    if (fk) {
      field_expr = lancedb_expr_column(fk->name.c_str());
      literal_expr = build_literal_expr(value_obj, fk->type, field_name, dpp, errors);
    } else {
      auto vtype = infer_value_type(value_obj);
      if (!vtype) {
        ldpp_dout(dpp, 1) << "ERROR: s3vector filter: unsupported value type for field '" << field_name << "'" << dendl;
        errors.push_back({"filter", fmt::format("unsupported value type for field '{}'", field_name)});
        return nullptr;
      }
      field_expr = build_json_field_expr(field_name, *vtype);
      literal_expr = build_json_literal_expr(value_obj, field_name, dpp, errors);
    }
    if (!literal_expr) {
      lancedb_expr_free(field_expr);
      return nullptr;
    }
    return lancedb_expr_binary(field_expr, binary_op, literal_expr);
  }

  LanceDBExpr* build_op_expr(
      const std::string& field_name,
      const std::string& op,
      JSONObj* value_obj,
      const filterable_metadata_key_t* fk,
      DoutPrefixProvider* dpp,
      std::vector<validation_error_t>& errors) {
    if (op == "$exists") return build_exists_expr(field_name, value_obj, fk, dpp, errors);
    if (op == "$in" || op == "$nin") return build_list_expr(field_name, (op == "$nin"), value_obj, fk, dpp, errors);
    return build_binary_expr(field_name, op, value_obj, fk, dpp, errors);
  }

  void free_filter_exprs(FilterExprs& fe) {
    lancedb_expr_free(fe.column_expr);
    lancedb_expr_free(fe.json_expr);
    fe.column_expr = nullptr;
    fe.json_expr = nullptr;
  }

  void combine_filter_exprs_and(FilterExprs& combined, FilterExprs& other) {
    if (other.column_expr) {
      combined.column_expr = combined.column_expr
          ? lancedb_expr_and(combined.column_expr, other.column_expr)
          : other.column_expr;
    }
    if (other.json_expr) {
      combined.json_expr = combined.json_expr
          ? lancedb_expr_and(combined.json_expr, other.json_expr)
          : other.json_expr;
    }
  }

  std::optional<FilterExprs> build_field_expr(
      const std::string& field_name,
      JSONObj* value_obj,
      const std::vector<filterable_metadata_key_t>& filterable_keys,
      const std::vector<std::string>& nonfilterable_keys,
      DoutPrefixProvider* dpp,
      std::vector<validation_error_t>& errors) {
    if (is_nonfilterable_key(field_name, nonfilterable_keys)) {
      ldpp_dout(dpp, 1) << "ERROR: s3vector filter: cannot filter on non-filterable metadata key '" << field_name << "'" << dendl;
      errors.push_back({"filter", fmt::format("cannot filter on non-filterable metadata key '{}'", field_name)});
      return std::nullopt;
    }

    const auto* fk = find_filterable_key(field_name, filterable_keys);
    const bool is_column = (fk != nullptr);

    LanceDBExpr* combined = nullptr;
    if (value_obj->is_object()) {
      for (auto it = value_obj->find_first(); !it.end(); ++it) {
        auto* op_obj = *it;
        auto* cmp_expr = build_op_expr(field_name, op_obj->get_name(), op_obj, fk, dpp, errors);
        if (!cmp_expr) {
          lancedb_expr_free(combined);
          return std::nullopt;
        }
        // implicit AND between multiple operators on the same field. e.g. {"age": {"$gt": 18, "$lt": 65}} is treated as
        // {"$and": [{"age": {"$gt": 18}}, {"age": {"$lt": 65}}]}
        combined = combined ? lancedb_expr_and(combined, cmp_expr) : cmp_expr;
      }
    } else if (value_obj->is_array()) {
      // implicit eqality with an array is not permitted
      ldpp_dout(dpp, 1) << "ERROR: s3vector filter: cannot use implicit equality with an array value for field '" << field_name << "'" << dendl;
      errors.push_back({"filter", fmt::format("cannot use implicit equality with an array value for field '{}'", field_name)});
      return std::nullopt;
    } else {
      // implicit equality if value is not an object. e.g. {"color": "red"} is treated as {"color": {"$eq": "red"}}
      combined = build_op_expr(field_name, "$eq", value_obj, fk, dpp, errors);
    }
    if (!combined) return std::nullopt;

    FilterExprs result;
    if (is_column) {
      result.column_expr = combined;
    } else {
      result.json_expr = combined;
    }
    return result;
  }

  std::optional<FilterExprs> build_filter_expr(
      JSONObj& obj,
      const std::vector<filterable_metadata_key_t>& filterable_keys,
      const std::vector<std::string>& nonfilterable_keys,
      DoutPrefixProvider* dpp,
      std::vector<validation_error_t>& errors) {
    FilterExprs combined;
    for (auto it = obj.find_first(); !it.end(); ++it) {
      auto* child = *it;
      const auto& name = child->get_name();

      if (name == "$and" || name == "$or") {
        if (!child->is_array()) {
          ldpp_dout(dpp, 1) << "ERROR: s3vector filter: " << name << " requires an array of conditions" << dendl;
          errors.push_back({"filter", fmt::format("{} requires an array of conditions", name)});
          free_filter_exprs(combined);
          return std::nullopt;
        }
        // top level logical operators
        if (child->find_first().end()) {
          ldpp_dout(dpp, 1) << "ERROR: s3vector filter: " << name << " requires a non-empty array of conditions" << dendl;
          errors.push_back({"filter", fmt::format("{} requires a non-empty array of conditions", name)});
          free_filter_exprs(combined);
          return std::nullopt;
        }
        FilterExprs logical;
        bool first = true;
        bool has_column = false;
        bool has_json = false;
        for (auto arr_it = child->find_first(); !arr_it.end(); ++arr_it) {
          if (!(**arr_it).is_object()) {
            ldpp_dout(dpp, 1) << "ERROR: s3vector filter: " << name << " array elements must be filter objects" << dendl;
            errors.push_back({"filter", fmt::format("{} array elements must be filter objects", name)});
            free_filter_exprs(logical);
            free_filter_exprs(combined);
            return std::nullopt;
          }
          auto sub = build_filter_expr(**arr_it, filterable_keys, nonfilterable_keys, dpp, errors);
          if (!sub) {
            free_filter_exprs(logical);
            free_filter_exprs(combined);
            return std::nullopt;
          }
          has_column = has_column || sub->column_expr;
          has_json = has_json || sub->json_expr;

          if (name == "$or" && has_column && has_json) {
            ldpp_dout(dpp, 1) << "ERROR: s3vector filter: $or cannot mix filterable column and JSON metadata conditions. use postFiltering=true to treat all conditions as JSON metadata conditions" << dendl;
            errors.push_back({"filter", "$or cannot mix filterable column and JSON metadata conditions. use postFiltering=true to treat all conditions as JSON metadata conditions"});
            free_filter_exprs(*sub);
            free_filter_exprs(logical);
            free_filter_exprs(combined);
            return std::nullopt;
          }

          if (first) {
            logical = *sub;
            first = false;
          } else if (name == "$and") {
            combine_filter_exprs_and(logical, *sub);
          } else {
            // all children are either column expressions of JSON expressions (validated above)
            if (sub->column_expr) {
              logical.column_expr = lancedb_expr_or(logical.column_expr, sub->column_expr);
            }
            if (sub->json_expr) {
              logical.json_expr = lancedb_expr_or(logical.json_expr, sub->json_expr);
            }
          }
        }
        combine_filter_exprs_and(combined, logical);
      } else {
        // top level field expression
        auto field = build_field_expr(name, child, filterable_keys, nonfilterable_keys, dpp, errors);
        if (!field) {
          free_filter_exprs(combined);
          return std::nullopt;
        }
        combine_filter_exprs_and(combined, *field);
      }
    }
    return combined;
  }

}
