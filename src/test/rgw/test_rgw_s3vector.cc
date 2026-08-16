// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "gtest/gtest.h"
#include "rgw_s3vector.h"
#include "rgw_s3vector_filter.h"
#include "common/ceph_json.h"
#include "common/dout.h"
#include "lancedb.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include <arrow/api.h>
#include <arrow/c/bridge.h>

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

// ---- empty filter ----

TEST_F(S3VectorFilterTest, EmptyFilterMatchesAll) {
  // an empty filter has no conditions, so no expression is built and every vector matches
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
}

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

// ---- value conversion on columns ----
// the declared type of the column decides how the value is read, so the JSON type
// of the value in the filter does not have to match it

TEST_F(S3VectorFilterTest, QuotedNumberOnNumberColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"year", FilterableMetadataType::NUMBER, false}};
  auto result = build(R"({"year": {"$eq": "2021"}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, NumberOnStringColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre": {"$eq": 42}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, QuotedBooleanOnBooleanColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"active", FilterableMetadataType::BOOLEAN, false}};
  auto result = build(R"({"active": {"$eq": "true"}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
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

TEST_F(S3VectorFilterTest, ExistsOnListColumn) {
  // $exists is the only operator supported on a list-type column
  std::vector<filterable_metadata_key_t> keys = {{"tags", FilterableMetadataType::STRING_LIST, false}};
  auto result = build(R"({"tags": {"$exists": true}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
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

TEST_F(S3VectorFilterTest, MixedTypesInListOnColumnAccepted) {
  // unlike a JSON metadata field, list elements of a column are converted to the
  // declared type of the column, so a mixed-type list is accepted
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre": {"$in": ["rock", 42]}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, QuotedNumbersInListOnColumnAccepted) {
  std::vector<filterable_metadata_key_t> keys = {{"year", FilterableMetadataType::NUMBER, false}};
  auto result = build(R"({"year": {"$in": ["2020", 2021]}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, InvalidNumberInListOnColumnRejected) {
  // a list element that cannot be converted to the column type is still rejected
  std::vector<filterable_metadata_key_t> keys = {{"year", FilterableMetadataType::NUMBER, false}};
  auto result = build(R"({"year": {"$in": [2020, "recent"]}})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
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

TEST_F(S3VectorFilterTest, DotInFieldNameRejected) {
  // a metadata key may not contain a '.', so such a field could never be matched
  auto result = build(R"({"user.name": "alice"})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, DotInFieldNameOnColumnRejected) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre.name": {"$eq": "rock"}})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, DotInFieldNameInsideLogicalRejected) {
  auto result = build(R"({"$and": [{"color": "red"}, {"user.name": "alice"}]})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, UnderscoreFieldNameRejected) {
  // a metadata key may not contain a '.', so such a field could never be matched
  auto result = build(R"({"_user": "alice"})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, UnderscoreFieldNameOnColumnRejected) {
  std::vector<filterable_metadata_key_t> keys = {{"_genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"_genre": {"$eq": "rock"}})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, UnderscoreFieldNameInsideLogicalRejected) {
  auto result = build(R"({"$and": [{"color": "red"}, {"_user": "alice"}]})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}
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
  // "$or" here is a field name, since its value is not an array of conditions.
  // the filter is rejected because an object is not a valid value for "$eq"
  auto result = build(R"({"$or": {"$eq": {"field": "value"}}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, OrWithScalarValueIsAFieldName) {
  // "$or" is a logical operator only when its value is an array of conditions.
  // with a scalar value it is a metadata key named "$or", matched with implicit
  // equality, so that such a key remains filterable
  auto result = build(R"({"$or": "value"})");
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
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

TEST_F(S3VectorFilterTest, AndWithScalarValueIsAFieldName) {
  auto result = build(R"({"$and": 42})");
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
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

TEST_F(S3VectorFilterTest, ListTypeInListRejected) {
  // $in goes through a different path than the comparison operators, and is
  // rejected on a list-type column as well
  std::vector<filterable_metadata_key_t> keys = {{"tags", FilterableMetadataType::STRING_LIST, false}};
  auto result = build(R"({"tags": {"$in": ["live", "remaster"]}})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ArrayValueInJsonEqRejected) {
  // array value in $eq on a JSON field — not supported yet
  auto result = build(R"({"tags": {"$eq": ["a", "b"]}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ImplicitEqWithArrayRejected) {
  // implicit equality with an array value is not permitted
  auto result = build(R"({"tags": ["a", "b"]})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ImplicitEqWithArrayOnColumnRejected) {
  std::vector<filterable_metadata_key_t> keys = {{"genre", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"genre": ["rock", "jazz"]})", keys);
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

TEST_F(S3VectorFilterTest, ObjectValueOnEveryColumnTypeRejected) {
  // the check comes before the column type is looked at
  for (const auto type : {FilterableMetadataType::STRING, FilterableMetadataType::NUMBER,
                          FilterableMetadataType::BOOLEAN}) {
    const std::vector<filterable_metadata_key_t> keys = {{"f", type, false}};
    for (const auto op : {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"}) {
      auto result = build(fmt::format(R"({{"f": {{"{}": {{"nested": 1}}}}}})", op), keys);
      EXPECT_FALSE(result.has_value());
      EXPECT_FALSE(errors.empty());
    }
  }
}


// decode a QueryVectors request. "vectorBucketName", "indexName" and "topK"
// are always supplied, since they  are validated before "queryVector" is decoded
// and the tests below are about
static void decode_query(const std::string& query_vector, query_vectors_t& req) {
  const auto json = fmt::format(
      R"({{"vectorBucketName": "bkt", "indexName": "idx", "topK": 3, "queryVector": {}}})",
      query_vector);
  JSONParser parser;
  ASSERT_TRUE(parser.parse(json.c_str(), json.size()));
  req.decode_json(&parser);
}

TEST(S3VectorDecodeTest, QueryVectorValid) {
  query_vectors_t req;
  ASSERT_NO_THROW(decode_query(R"({"float32": [1.0, 2.0, 3.0]})", req));
  ASSERT_EQ(req.query_vector.size(), 3);
  EXPECT_FLOAT_EQ(req.query_vector[0], 1.0);
  EXPECT_FLOAT_EQ(req.query_vector[2], 3.0);
}

// an empty "queryVector" object leaves the "float32" lookup at the end
// iterator, which used to be dereferenced unconditionally
TEST(S3VectorDecodeTest, QueryVectorMissingFloat32) {
  query_vectors_t req;
  EXPECT_THROW(decode_query("{}", req), JSONDecoder::err);
}

TEST(S3VectorDecodeTest, QueryVectorFloat32NotAnArray) {
  query_vectors_t req;
  EXPECT_THROW(decode_query(R"({"float32": 5})", req), JSONDecoder::err);
}

TEST(S3VectorDecodeTest, QueryVectorEmptyArray) {
  query_vectors_t req;
  EXPECT_THROW(decode_query(R"({"float32": []})", req), JSONDecoder::err);
}

TEST(S3VectorDecodeTest, QueryVectorNonNumericElement) {
  query_vectors_t req;
  EXPECT_THROW(decode_query(R"({"float32": ["abc"]})", req), JSONDecoder::err);
}

TEST(S3VectorDecodeTest, QueryVectorMissingAltogether) {
  query_vectors_t req;
  const std::string json = R"({"vectorBucketName": "bkt", "indexName": "idx", "topK": 3})";
  JSONParser parser;
  ASSERT_TRUE(parser.parse(json.c_str(), json.size()));
  EXPECT_THROW(req.decode_json(&parser), JSONDecoder::err);
}

// ---- metadata key name validation ----
//

TEST(S3VectorKeyNameTest, ValidNames) {
  for (const auto& name : {"genre", "underscore_is_ok", "my key", " genre ", "my-key",
                           "1genre", "select", "café", "日本語", "MyKey",
                           "$eq", "$and", "$or", "'genre'",
                           "my\"key", "\"genre\""}) {
    EXPECT_FALSE(validate_metadata_key_name(name).has_value()) << "name: " << name;
    EXPECT_FALSE(validate_declared_metadata_key_name(name).has_value()) << "name: " << name;
  }
}

TEST(S3VectorKeyNameTest, EmptyNameRejected) {
  EXPECT_TRUE(validate_metadata_key_name("").has_value());
  EXPECT_TRUE(validate_declared_metadata_key_name("").has_value());
}

TEST(S3VectorKeyNameTest, LengthValidation) {
  const auto too_long = std::string(max_metadata_key_name_length + 1, 'k');
  const auto longest = std::string(max_metadata_key_name_length, 'k');
  EXPECT_TRUE(validate_declared_metadata_key_name(too_long).has_value());
  EXPECT_FALSE(validate_metadata_key_name(too_long).has_value());
  EXPECT_FALSE(validate_declared_metadata_key_name(longest).has_value());
}

TEST(S3VectorKeyNameTest, UnderscorePrefixRejected) {
  for (const auto& name : {"_key", "_data", "_metadata", "_distance", "_", "_anything"}) {
    EXPECT_TRUE(validate_metadata_key_name(name).has_value()) << "name: " << name;
    EXPECT_TRUE(validate_declared_metadata_key_name(name).has_value()) << "name: " << name;
  }
}

TEST(S3VectorKeyNameTest, DotRejected) {
  EXPECT_TRUE(validate_metadata_key_name("user.name").has_value());
  EXPECT_TRUE(validate_metadata_key_name(".").has_value());
  EXPECT_TRUE(validate_declared_metadata_key_name("user.name").has_value());
  EXPECT_TRUE(validate_declared_metadata_key_name(".").has_value());
}

// a name holding a backtick is rejected only when declared at CreateIndex,
// since only a declared key may become a column, and lance fails to write a
// column whose name holds a backtick anywhere - wrapped, leading, trailing or
// inside. as a key of a metadata document the name is fine, since that is
// never a column
TEST(S3VectorKeyNameTest, BackticksRejectedForDeclaredNamesOnly) {
  for (const auto& name : {"`genre`", "``", "`my key`", "`a`b`",
                           "my`key", "`lead", "trail`", "`"}) {
    EXPECT_TRUE(validate_declared_metadata_key_name(name).has_value()) << "name: " << name;
    EXPECT_FALSE(validate_metadata_key_name(name).has_value()) << "name: " << name;
  }
  // the other quote characters are handled by quoting the column name in the
  // filter expression, and need no rule
  for (const auto& name : {"\"genre\"", "'genre'"}) {
    EXPECT_FALSE(validate_declared_metadata_key_name(name).has_value()) << "name: " << name;
  }
}

// ---- column name quoting ----

TEST(S3VectorColumnNameTest, QuoteColumnName) {
  EXPECT_EQ(quote_column_name("genre"), "\"genre\"");
  EXPECT_EQ(quote_column_name("Genre"), "\"Genre\"");
  EXPECT_EQ(quote_column_name("my key"), "\"my key\"");
  EXPECT_EQ(quote_column_name(" genre "), "\" genre \"");
  EXPECT_EQ(quote_column_name("`genre`"), "\"`genre`\"");
  EXPECT_EQ(quote_column_name("'genre'"), "\"'genre'\"");
  EXPECT_EQ(quote_column_name("ge`nre"), "\"ge`nre\"");
  EXPECT_EQ(quote_column_name("ge'nre"), "\"ge'nre\"");
  EXPECT_EQ(quote_column_name("'ge'nre'"), "\"'ge'nre'\"");
  EXPECT_EQ(quote_column_name("`ge`nre`"), "\"`ge`nre`\"");
  EXPECT_EQ(quote_column_name(""), "\"\"");
  // an interior double quote is escaped by doubling it
  EXPECT_EQ(quote_column_name("my\"key"), "\"my\"\"key\"");
  EXPECT_EQ(quote_column_name("\"genre\""), "\"\"\"genre\"\"\"");
}

// evaluate '<column> = "x"' over a single row batch whose only column is named
// <name> and holds "x". returns true if datafusion resolved the column
// reference against the schema and the row matched. this is what a filter on a
// filterable metadata key does at query time, minus the table
static bool column_filter_matches(const std::string& name, const std::string& expr_name) {
  auto schema = arrow::schema({arrow::field(name, arrow::utf8())});
  arrow::StringBuilder builder;
  EXPECT_TRUE(builder.Append("x").ok());
  std::shared_ptr<arrow::Array> array;
  EXPECT_TRUE(builder.Finish(&array).ok());
  const auto batch = arrow::RecordBatch::Make(schema, 1, {array});

  struct ArrowArray c_array = {};
  struct ArrowSchema c_schema = {};
  EXPECT_TRUE(arrow::ExportRecordBatch(*batch, &c_array, &c_schema).ok());

  auto* column = lancedb_expr_column(expr_name.c_str());
  auto* value = lancedb_expr_literal_string("x");
  auto* expr = lancedb_expr_binary(column, LANCEDB_BINARY_OP_EQ, value);

  struct ArrowArray* arrays[1] = {&c_array};
  bool* matches = nullptr;
  size_t count = 0;
  char* error_message = nullptr;
  const auto result = lancedb_json_matches(
      reinterpret_cast<FFI_ArrowArray**>(arrays),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
      1, expr, &matches, &count, &error_message);

  const bool matched = (result == LANCEDB_SUCCESS && count == 1 && matches[0]);
  lancedb_free_json_matches(matches);
  lancedb_free_string(error_message);
  if (c_array.release) c_array.release(&c_array);
  if (c_schema.release) c_schema.release(&c_schema);
  return matched;
}

TEST(S3VectorColumnNameTest, QuotingColumnNames) {
  // a name that parses as a SQL identifier is normalized: lowercased, stripped
  // of its surrounding whitespace, or unwrapped when it is quoted with '"' or
  // '`', which are the identifier quote characters of the dialect
  const auto names_that_must_be_quoted = {"Genre", "MyKey", " genre ", "\"genre\"",
                                          "`genre`"};
  for (const auto& name : names_that_must_be_quoted) {
    EXPECT_TRUE(column_filter_matches(name, quote_column_name(name))) << "name: " << name;
    // make sure that quoting is needed
    EXPECT_FALSE(column_filter_matches(name, name)) << "name: " << name;
  }
  // a name that does not parse as an identifier is used as it is. note that a
  // single quote never delimits an identifier, only a string literal, so a name
  // wrapped in single quotes belongs here and not above
  const auto other_names = {"my key", "my\"key", "my'key", "'genre'", "café",
                            "日本語", "my-key", "1genre", "select", "$or"};
  for (const auto& name : other_names) {
    // make sure that quoting does not break names
    EXPECT_TRUE(column_filter_matches(name, quote_column_name(name))) << "name: " << name;
    // make sure that quoting is not needed
    EXPECT_TRUE(column_filter_matches(name, name)) << "name: " << name;
  }
}

// ---- metadata keys named after a filter operator ----
//
// "$and" and "$or" are logical operators only when their value is an array of
// conditions. every other name at a field position is a metadata key name, so
// that a key named after an operator stays filterable

TEST_F(S3VectorFilterTest, EveryOperatorNameWorksAsAFieldName) {
  for (const auto& name : {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte",
                           "$in", "$nin", "$exists", "$and", "$or"}) {
    auto result = build(fmt::format(R"({{"{}": "example"}})", name));
    ASSERT_TRUE(result.has_value()) << "name: " << name;
    EXPECT_NE(result->json_expr, nullptr) << "name: " << name;
    EXPECT_TRUE(errors.empty());
    free_exprs(*result);
  }
}

TEST_F(S3VectorFilterTest, OperatorNameAsAFilterableColumn) {
  std::vector<filterable_metadata_key_t> keys = {{"$or", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"$or": {"$eq": "example"}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, MixedOperatorAndFieldName) {
  // the same name in both roles in one filter: the outer "$or" takes an array
  // so it is the operator, the inner one takes an object so it is a field name
  auto result = build(R"({"$or": [{"$or": {"$eq": "example"}}, {"color": "red"}]})");
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->json_expr, nullptr);
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, SameOperatorAndFieldName) {
  // outer "$eq" is field name and inner "$eq" is operator
  auto result = build(R"({"$eq": {"$eq": "example"}})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, DifferentOperatorAndFieldName) {
  // "$eq" is the field name and "$ne" is the operator
  auto result = build(R"({"$eq": {"$ne": "example"}})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->column_expr, nullptr);
  EXPECT_NE(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, ObjectValueForAnOperatorRejected) {
  // the value of an operator (inner "$eq") must be a scalar
  // in this case it is an object and therefore rejected
  auto result = build(R"({"$eq": {"$eq": {"$ne": "example"}}})");
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, SameOperatorAndColumnName) {
  // the same, on a filterable metadata key named "$eq"
  const std::vector<filterable_metadata_key_t> keys = {{"$eq", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"$eq": {"$eq": "example"}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);

  // a different operator on the same column
  result = build(R"({"$eq": {"$ne": "example"}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, ObjectValueForAnOperatorOnColumnRejected) {
  const std::vector<filterable_metadata_key_t> keys = {{"$eq", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"$eq": {"$eq": {"$ne": "example"}}})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ArrayValueForAnOperatorOnColumnRejected) {
  const std::vector<filterable_metadata_key_t> keys = {{"$in", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"$in": {"$eq": ["rock", "jazz"]}})", keys);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(errors.empty());
}

TEST_F(S3VectorFilterTest, ElementInListOnColumn) {
  // "$in" as the name of a filterable column, with the "$in" operator applied to it
  const std::vector<filterable_metadata_key_t> keys = {{"$in", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"$in": {"$in": ["rock", "pop"]}})", keys);
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result->column_expr, nullptr);
  EXPECT_EQ(result->json_expr, nullptr);
  EXPECT_TRUE(errors.empty());
  free_exprs(*result);
}

TEST_F(S3VectorFilterTest, ObjectElementInListOnColumnRejected) {
  const std::vector<filterable_metadata_key_t> keys = {{"$in", FilterableMetadataType::STRING, false}};
  auto result = build(R"({"$in": {"$in": ["rock", {"nested": "value"}]}})", keys);
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
