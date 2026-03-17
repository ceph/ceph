// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "gtest/gtest.h"
#include "rgw_s3vector_filter.h"
#include "common/ceph_json.h"
#include "common/dout.h"
#include "lancedb.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"

#define dout_subsys ceph_subsys_rgw

using namespace rgw::s3vector;

class S3VectorFilterTest : public ::testing::Test {
protected:
  NoDoutPrefix no_dpp{g_ceph_context, dout_subsys};
  DoutPrefixProvider* dpp = &no_dpp;
  std::vector<rgw::s3vector::validation_error_t> errors;

  // parse a JSON string into a JSONParser and call build_filter_expr
  std::optional<FilterExprs> build(
      const std::string& json,
      const std::vector<filterable_metadata_key_t>& filterable_keys = {},
      const std::vector<std::string>& nonfilterable_keys = {}) {
    errors.clear();
    JSONParser parser;
    EXPECT_TRUE(parser.parse(json.c_str(), json.size()));
    return build_filter_expr(parser, filterable_keys, nonfilterable_keys, dpp, errors);
  }

  void free_exprs(FilterExprs& fe) {
    lancedb_expr_free(fe.column_expr);
    lancedb_expr_free(fe.json_expr);
  }
};

// ---- implicit $eq ----

TEST_F(S3VectorFilterTest, ImplicitEqOnColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre": "rock"})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, ImplicitEqOnJson) {
  auto result = build(R"({"color": "red"})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

// ---- explicit operators on columns ----

TEST_F(S3VectorFilterTest, ExplicitEqOnColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre": {"$eq": "rock"}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, NumericRangeOnColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"year", FilterableMetadataType::NUMBER, false}};
  auto result = build(R"({"year": {"$gt": 2019, "$lt": 2026}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, BooleanEqOnColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"active", FilterableMetadataType::BOOLEAN, false}};
  auto result = build(R"({"active": {"$eq": true}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

// ---- explicit operators on JSON metadata ----

TEST_F(S3VectorFilterTest, ExplicitEqOnJson) {
  auto result = build(R"({"color": {"$eq": "red"}})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, NumericRangeOnJson) {
  auto result = build(R"({"score": {"$gte": 0.5, "$lte": 1.0}})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, NotEqOnJson) {
  auto result = build(R"({"color": {"$ne": "blue"}})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

// ---- $exists ----

TEST_F(S3VectorFilterTest, ExistsOnNullableColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre": {"$exists": true}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, ExistsTrueOnNonNullableColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"year", FilterableMetadataType::NUMBER, false}};
  auto result = build(R"({"year": {"$exists": true}})", keys);
  ASSERT_TRUE(result.has_value());
  // for non-nullable column, $exists returns a constant boolean expression
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, ExistsFalseOnNonNullableColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"year", FilterableMetadataType::NUMBER, false}};
  auto result = build(R"({"year": {"$exists": false}})", keys);
  ASSERT_TRUE(result.has_value());
  // for non-nullable column, $exists: false returns a constant false expression
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, ExistsOnJson) {
  auto result = build(R"({"color": {"$exists": true}})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, NotExistsOnJson) {
  auto result = build(R"({"color": {"$exists": false}})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, ExistsWithQuotedTrueRejected) {
  auto result = build(R"({"color": {"$exists": "true"}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ExistsWithNumberRejected) {
  auto result = build(R"({"color": {"$exists": 1}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ExistsWithEmptyValueRejected) {
  auto result = build(R"({"color": {"$exists": ""}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

// ---- $in / $nin ----

TEST_F(S3VectorFilterTest, InOnColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre": {"$in": ["rock", "jazz"]}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, NinOnColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre": {"$nin": ["pop"]}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, InOnJson) {
  auto result = build(R"({"color": {"$in": ["red", "blue"]}})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, InNumericOnJson) {
  auto result = build(R"({"score": {"$in": [1, 2, 3]}})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

// ---- $and ----

TEST_F(S3VectorFilterTest, AndColumnsOnly) {
  std::vector<filterable_metadata_key_t> keys = {
    {"genre", FilterableMetadataType::STRING, false},
    {"year", FilterableMetadataType::NUMBER, false},
  };
  auto result = build(R"({"$and": [{"genre": "rock"}, {"year": {"$gt": 2020}}]})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, AndJsonOnly) {
  auto result = build(R"({"$and": [{"color": "red"}, {"size": {"$gt": 10}}]})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, AndMixedColumnAndJson) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"$and": [{"genre": "rock"}, {"color": "red"}]})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

// ---- implicit AND (top-level fields) ----

TEST_F(S3VectorFilterTest, ImplicitAndMixedColumnAndJson) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre": "rock", "color": "red"})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

// ---- $or ----

TEST_F(S3VectorFilterTest, OrColumnsOnly) {
  std::vector<filterable_metadata_key_t> keys = {
    {"genre", FilterableMetadataType::STRING, false},
    {"year", FilterableMetadataType::NUMBER, false},
  };
  auto result = build(R"({"$or": [{"genre": "rock"}, {"year": {"$gt": 2020}}]})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, OrJsonOnly) {
  auto result = build(R"({"$or": [{"color": "red"}, {"size": {"$gt": 10}}]})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, OrColumnsNestedInAndWithJson) {
  // $or is column-only, combined via top-level $and with a JSON field — valid
  std::vector<filterable_metadata_key_t> keys = {
    {"genre", FilterableMetadataType::STRING, false},
    {"year", FilterableMetadataType::NUMBER, false},
  };
  auto result = build(
      R"({"$and": [{"$or": [{"genre": "rock"}, {"year": {"$gt": 2020}}]}, {"color": "red"}]})",
      keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, OrJsonNestedInAndWithColumn) {
  // $or is JSON-only, combined via top-level $and with a column field — valid
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(
      R"({"$and": [{"$or": [{"color": "red"}, {"size": {"$gt": 10}}]}, {"genre": "rock"}]})",
      keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, OrInsideOrAllColumns) {
  // nested $or within $or, all column fields — valid
  std::vector<filterable_metadata_key_t> keys = {
    {"genre", FilterableMetadataType::STRING, false},
    {"year", FilterableMetadataType::NUMBER, false},
    {"active", FilterableMetadataType::BOOLEAN, false},
  };
  auto result = build(
      R"({"$or": [{"genre": "rock"}, {"$or": [{"year": {"$gt": 2020}}, {"active": true}]}]})",
      keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, OrInsideOrAllJson) {
  // nested $or within $or, all JSON fields — valid
  auto result = build(
      R"({"$or": [{"color": "red"}, {"$or": [{"size": {"$gt": 10}}, {"weight": 5}]}]})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

// ---- complex expressions ----

TEST_F(S3VectorFilterTest, MultipleOperatorsOnSameField) {
  std::vector<filterable_metadata_key_t> keys = {{"year", FilterableMetadataType::NUMBER, false}};
  auto result = build(R"({"year": {"$gte": 2000, "$lte": 2025}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, DeeplyNested) {
  // $and [ $or [ $and [ field, field ], field ], field ]
  // mixes column and JSON at different levels, all connected by $and at the top
  std::vector<filterable_metadata_key_t> keys = {
    {"genre", FilterableMetadataType::STRING, false},
    {"year", FilterableMetadataType::NUMBER, false},
  };
  auto result = build(
      R"({
        "$and": [
          {"$or": [
            {"$and": [
              {"genre": {"$in": ["rock", "jazz"]}},
              {"genre": {"$ne": "blues"}}
            ]},
            {"genre": {"$exists": true}}
          ]},
          {"year": {"$gte": 2000, "$lte": 2025}},
          {"color": {"$eq": "red"}}
        ]
      })",
      keys);
  ASSERT_TRUE(result.has_value());
  // genre and year go to column_expr, color goes to json_expr
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, AllSixComparisonOps) {
  // verify all comparison operators produce valid expressions on JSON fields
  for (const auto& op : {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"}) {
    std::string json = R"({"score": {")" + std::string(op) + R"(": 42}})";
    auto result = build(json);
    ASSERT_TRUE(result.has_value()) << "failed for operator " << op;
    EXPECT_NE(result->json_expr, nullptr) << "null expr for operator " << op;
    free_exprs(*result);
  }
}

// ---- error cases ----

TEST_F(S3VectorFilterTest, NonfilterableKeyRejected) {
  std::vector<std::string> nonfilterable = {"secret"};
  auto result = build(R"({"secret": "value"})", {}, nonfilterable);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, MixedOrRejected) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"$or": [{"genre": "rock"}, {"color": "red"}]})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, MixedOrNestedViaAndRejected) {
  // $or child is a $and that returns both column_expr and json_expr — makes $or mixed
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(
      R"({"$or": [{"genre": "rock"}, {"$and": [{"genre": "jazz"}, {"color": "blue"}]}]})",
      keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, MixedOrNestedViaOrRejected) {
  // outer $or has one column child and one nested $or with JSON — mix detected across children
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(
      R"({"$or": [{"genre": "rock"}, {"$or": [{"color": "red"}, {"color": "blue"}]}]})",
      keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, MixedOrDeeplyNestedRejected) {
  // mix appears 3 levels deep: $and -> $or -> $and produces mixed FilterExprs for the $or
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(
      R"({"$and": [{"$or": [{"genre": "rock"}, {"$and": [{"color": "red"}, {"genre": "jazz"}]}]}]})",
      keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, UnknownOperatorRejected) {
  auto result = build(R"({"color": {"$regex": "r.*"}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, EmptyOrArrayRejected) {
  auto result = build(R"({"$or": []})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, EmptyAndArrayRejected) {
  auto result = build(R"({"$and": []})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, InvalidBooleanValueRejected) {
  std::vector<filterable_metadata_key_t> keys = {{"active", FilterableMetadataType::BOOLEAN, false}};
  auto result = build(R"({"active": {"$eq": "yes"}})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, InvalidNumberValueRejected) {
  std::vector<filterable_metadata_key_t> keys = {{"year", FilterableMetadataType::NUMBER, false}};
  auto result = build(R"({"year": {"$eq": "not_a_number"}})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, OrWithObjectValueRejected) {
  auto result = build(R"({"$or": {"$eq": {"field": "value"}}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, OrWithScalarValueRejected) {
  auto result = build(R"({"$or": "value"})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, OrWithScalarArrayRejected) {
  auto result = build(R"({"$or": [1, 2, 3]})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, AndWithObjectValueRejected) {
  auto result = build(R"({"$and": {"genre": "rock"}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, AndWithScalarValueRejected) {
  auto result = build(R"({"$and": 42})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, InWithObjectValueRejected) {
  auto result = build(R"({"color": {"$in": {"field": "value"}}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, InWithScalarValueRejected) {
  auto result = build(R"({"color": {"$in": "red"}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, NinWithObjectValueRejected) {
  auto result = build(R"({"color": {"$nin": {"field": "value"}}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, EmptyInListRejected) {
  auto result = build(R"({"color": {"$in": []}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, MixedTypesInListRejected) {
  auto result = build(R"({"color": {"$in": ["red", 42]}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, NullImplicitEqRejected) {
  auto result = build(R"({"color": null})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, NullExplicitEqRejected) {
  auto result = build(R"({"color": {"$eq": null}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, NullNeRejected) {
  auto result = build(R"({"color": {"$ne": null}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, NullInListElementRejected) {
  auto result = build(R"({"color": {"$in": [null, "red"]}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, NullGtRejected) {
  auto result = build(R"({"score": {"$gt": null}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ListTypeFilteringRejected) {
  std::vector<filterable_metadata_key_t> keys = {{"tags", FilterableMetadataType::STRING_LIST, false}};
  auto result = build(R"({"tags": {"$eq": "foo"}})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ArrayValueInJsonEqRejected) {
  // array value in $eq on a JSON field — not supported yet
  auto result = build(R"({"tags": {"$eq": ["a", "b"]}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ObjectValueInJsonEqRejected) {
  // object value rejected by infer_value_type
  auto result = build(R"({"color": {"$eq": {"nested": "value"}}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ObjectValueInJsonNeRejected) {
  auto result = build(R"({"color": {"$ne": {"nested": "value"}}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ObjectValueInJsonInListRejected) {
  auto result = build(R"({"color": {"$in": [{"nested": "value"}]}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

int main(int argc, char** argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                          CODE_ENVIRONMENT_UTILITY,
                          CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
