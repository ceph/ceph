// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include <catch2/catch_test_macros.hpp>

#include "rgw/fdb/query.h"
#include "rgw/fdb/fdb.h"

#include <algorithm>
#include <array>
#include <compare>
#include <concepts>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace core = ceph::libfdb::interval;
namespace q = ceph::libfdb::query;
namespace lfdb = ceph::libfdb;
using namespace std::literals;

struct borrowed_key final
{
 std::string_view bytes;

 explicit constexpr borrowed_key(const std::string_view value) : bytes(value)
 {}
 borrowed_key(const borrowed_key&) = delete;

 friend constexpr std::string_view libfdb_key_view(const borrowed_key& key) { return key.bytes; }
};

static_assert(lfdb::concepts::libfdb_key<borrowed_key>);
static_assert(!std::copy_constructible<borrowed_key>);
static_assert(noexcept(q::is_empty(std::declval<const q::interval&>())));

struct integer_domain final
{
 using value_type = int;
 static constexpr auto compare(const int lhs, const int rhs) noexcept { return lhs <=> rhs; }
};

using integer_query = core::query<integer_domain>;
using integer_set = core::interval_set<integer_domain>;

template <typename LhsT, typename RhsT>
concept set_assignable = requires(LhsT&& lhs, RhsT&& rhs) {
 { std::forward<LhsT>(lhs) |= std::forward<RhsT>(rhs) } -> std::same_as<q::interval_set&>;
 { std::forward<LhsT>(lhs) -= std::forward<RhsT>(rhs) } -> std::same_as<q::interval_set&>;
 { std::forward<LhsT>(lhs) ^= std::forward<RhsT>(rhs) } -> std::same_as<q::interval_set&>;
 { std::forward<LhsT>(lhs) &= std::forward<RhsT>(rhs) } -> std::same_as<q::interval_set&>;
};

static_assert(set_assignable<q::interval_set&, const q::interval&>);
static_assert(set_assignable<q::interval_set&, const q::interval_set&>);
static_assert(!set_assignable<const q::interval_set&, q::interval>);
static_assert(!set_assignable<q::interval_set, q::interval>);
static_assert(!set_assignable<q::interval&, q::interval>);
static_assert(!set_assignable<q::interval_set&, integer_query>);
static_assert(!q::expression<integer_query>);
static_assert(q::configured_expression<q::interval_set>);
static_assert(core::expression<q::interval_set>);

struct malformed_wrapper final
{
 using domain_type = integer_domain;
 using interval_expression_type = int;
 int expression;
};
static_assert(!core::expression<malformed_wrapper>);
static_assert(!q::byte_interval_expression<q::configured<q::interval>>);

using byte_query = core::query<q::byte_string_domain>;
static_assert(std::same_as<decltype(core::after<q::byte_string_domain>("a")), byte_query>);
static_assert(std::same_as<decltype(core::before<q::byte_string_domain>("a")), byte_query>);
static_assert(std::same_as<decltype(core::at_or_after<q::byte_string_domain>("a")), byte_query>);
static_assert(std::same_as<decltype(core::at_or_before<q::byte_string_domain>("a")), byte_query>);
static_assert(std::same_as<decltype(core::between<q::byte_string_domain>("a", "b")), byte_query>);
static_assert(std::same_as<decltype(core::closed_between<q::byte_string_domain>("a", "b")), byte_query>);
static_assert(std::same_as<decltype(core::open_between<q::byte_string_domain>("a", "b")), byte_query>);
static_assert(std::same_as<decltype(core::prefix_starting_at<q::byte_string_domain>("a", "ab")), byte_query>);
static_assert(std::same_as<decltype(core::prefix_starting_after<q::byte_string_domain>("a", "ab")), byte_query>);

static_assert([] {
 integer_set selected {integer_query::closed(0, 10)};
 selected -= integer_query::open(2, 8);
 selected |= integer_query::singleton(5);
 selected ^= selected;
 return selected.empty();
}());

static_assert([] {
 q::interval_set selected {q::with_options(q::between("a", "z"), {.result_limit = 7})};
 selected -= q::between("m", "n");
 selected |= q::at("m");
 q::assign_complement(selected);
 return q::contains(selected, "mm") && !q::contains(selected, "a") &&
        !q::contains(selected, "\xFF"sv) && 7 == selected.options.result_limit;
}());

// Five atoms partition the entire mathematical universe. Every union of these
// atoms has a unique five-bit oracle, and each atom has its own witness key.
// The oracle builds maximal runs independently of the library's range algebra.
struct integer_model final
{
 using set_type = integer_set;
 static constexpr unsigned universe = 31;
 static constexpr std::array<int, 5> witnesses {-1, 0, 1, 2, 3};

 static auto atoms()
 {
  return std::array {
   core::before<integer_domain>(0), integer_query::singleton(0), integer_query::open(0, 2),
   integer_query::singleton(2), core::after<integer_domain>(2)};
 }

 static auto expected(const unsigned mask)
 {
  const auto cells = atoms();
  std::vector<integer_query> out;
  for (unsigned first = 0; 5 > first;) {
   if (0 == (mask & (1u << first))) {
    ++first;
    continue;
   }
   auto last = first;
   while (5 > last + 1 && 0 != (mask & (1u << (last + 1)))) {
    ++last;
   }
   out.push_back(integer_query::between(cells[first].lower(), cells[last].upper()));
   first = last + 1;
  }
  return out;
 }

 static set_type make(const unsigned mask)
 {
  set_type out;
  for (const auto& cell : expected(mask)) {
   out |= cell;
  }
  return out;
 }

 static bool matches(const core::expression auto& expr, const unsigned mask)
 {
  return expected(mask) == core::compile_intervals(expr);
 }

 static bool member(const core::expression auto& expr, const int key)
 {
  return core::contains(expr, key);
 }
};

struct fdb_model final
{
 using set_type = q::interval_set;
 static constexpr unsigned universe = 31;
 static constexpr std::array<std::string_view, 5> witnesses {"", "a", "a\0"sv, "b", "c"};

 static auto atoms()
 {
  return std::array {q::before("a"), q::at("a"), q::open_between("a", "b"),
                     q::at("b"), q::after("b")};
 }

 static auto expected(const unsigned mask)
 {
  const auto cells = atoms();
  std::vector<q::interval> out;
  for (unsigned first = 0; 5 > first;) {
   if (0 == (mask & (1u << first))) {
    ++first;
    continue;
   }
   auto last = first;
   while (5 > last + 1 && 0 != (mask & (1u << (last + 1)))) {
    ++last;
   }
   out.emplace_back(cells[first].begin_key, cells[last].end_key,
                    cells[first].begin_inclusive, cells[last].end_inclusive);
   first = last + 1;
  }
  return out;
 }

 static set_type make(const unsigned mask)
 {
  set_type out;
  for (const auto& cell : expected(mask)) {
   out |= cell;
  }
  return out;
 }

 static bool matches(const q::expression auto& expr, const unsigned mask)
 {
  return expected(mask) == q::compile_intervals(expr);
 }

 static bool member(const q::expression auto& expr, const std::string_view key)
 {
  return q::contains(expr, key);
 }
};

template <typename ModelT>
void check_algebra()
{
 std::vector<typename ModelT::set_type> sets;
 for (unsigned mask = 0; ModelT::universe >= mask; ++mask) {
  sets.push_back(ModelT::make(mask));
  REQUIRE(ModelT::matches(sets.back(), mask));
 }
 const auto& empty = sets.front();
 const auto& universe = sets.back();

 for (unsigned a = 0; ModelT::universe >= a; ++a) {
  const auto& x = sets[a];
  CAPTURE(a);
  CHECK(ModelT::matches(~x, ModelT::universe ^ a));
  CHECK(ModelT::matches(~~x, a));
  CHECK(ModelT::matches(x & universe, a));
  CHECK(ModelT::matches(x | empty, a));
  CHECK(ModelT::matches(x - x, 0));
  CHECK(ModelT::matches(x ^ x, 0));
  CHECK(ModelT::matches(x & ~x, 0));
  CHECK(ModelT::matches(x | ~x, ModelT::universe));

  for (unsigned b = 0; ModelT::universe >= b; ++b) {
   const auto& y = sets[b];
   CAPTURE(b);
   const auto intersection = a & b;
   const auto union_ = a | b;
   const auto difference = a & ~b;
   const auto xor_ = a ^ b;

   CHECK(ModelT::matches(x & y, intersection));
   CHECK(ModelT::matches(y & x, intersection));
   CHECK(ModelT::matches(x | y, union_));
   CHECK(ModelT::matches(y | x, union_));
   CHECK(ModelT::matches(x - y, difference));
   CHECK(ModelT::matches(x ^ y, xor_));
   CHECK(ModelT::matches(y ^ x, xor_));
   CHECK(ModelT::matches(~(x | y), ModelT::universe ^ union_));
   CHECK(ModelT::matches(~x & ~y, ModelT::universe ^ union_));
   CHECK(ModelT::matches(~(x & y), ModelT::universe ^ intersection));
   CHECK(ModelT::matches(~x | ~y, ModelT::universe ^ intersection));
   CHECK(ModelT::matches(x & (x | y), a));
   CHECK(ModelT::matches(x | (x & y), a));
   CHECK(ModelT::matches(x - y, a & (ModelT::universe ^ b)));
   CHECK(ModelT::matches((x - y) | (y - x), xor_));
   CHECK(ModelT::matches((x ^ y) ^ y, a));

   auto assigned = x;
   assigned &= y;
   CHECK(ModelT::matches(assigned, intersection));
   assigned = x;
   assigned |= y;
   CHECK(ModelT::matches(assigned, union_));
   assigned = x;
   assigned -= y;
   CHECK(ModelT::matches(assigned, difference));
   assigned = x;
   assigned ^= y;
   CHECK(ModelT::matches(assigned, xor_));

   for (unsigned key = 0; ModelT::witnesses.size() > key; ++key) {
    CHECK((0 != (xor_ & (1u << key))) == ModelT::member(x ^ y, ModelT::witnesses[key]));
   }

   for (unsigned c = 0; ModelT::universe >= c; ++c) {
    const auto& z = sets[c];
    CAPTURE(c);
    CHECK(ModelT::matches((x & y) & z, a & b & c));
    CHECK(ModelT::matches(x & (y & z), a & b & c));
    CHECK(ModelT::matches((x | y) | z, a | b | c));
    CHECK(ModelT::matches(x | (y | z), a | b | c));
    CHECK(ModelT::matches((x ^ y) ^ z, a ^ b ^ c));
    CHECK(ModelT::matches(x ^ (y ^ z), a ^ b ^ c));
    CHECK(ModelT::matches(x & (y | z), a & (b | c)));
    CHECK(ModelT::matches((x & y) | (x & z), a & (b | c)));
    CHECK(ModelT::matches(x | (y & z), a | (b & c)));
    CHECK(ModelT::matches((x | y) & (x | z), a | (b & c)));
    CHECK(ModelT::matches(x & (y ^ z), a & (b ^ c)));
    CHECK(ModelT::matches((x & y) ^ (x & z), a & (b ^ c)));
    CHECK(ModelT::matches((x - y) - z, a & ~(b | c)));
    CHECK(ModelT::matches(x - (y | z), a & ~(b | c)));
   }
  }
 }
}

TEST_CASE("core operators obey the full five-atom Boolean algebra", "[fdb][algebra][laws]")
{
 check_algebra<integer_model>();
}

TEST_CASE("FDB operators obey the full five-atom Boolean algebra", "[fdb][algebra][laws]")
{
 check_algebra<fdb_model>();
}

template <typename ModelT>
void check_nested_expressions()
{
 std::minstd_rand random{0x5e71};
 for (unsigned sample = 0; 512 > sample; ++sample) {
  const unsigned a = random() & ModelT::universe, b = random() & ModelT::universe;
  const unsigned c = random() & ModelT::universe, d = random() & ModelT::universe;
  const unsigned e = random() & ModelT::universe, f = random() & ModelT::universe;
  const auto x = ModelT::make(a), y = ModelT::make(b), z = ModelT::make(c);
  const auto u = ModelT::make(d), v = ModelT::make(e), w = ModelT::make(f);
  const auto expected = (ModelT::universe ^ (a ^ (b | c))) & ~((d & e) | (ModelT::universe ^ f));
  const auto expr = ~(x ^ (y | z)) - ((u & v) | ~w);
  CHECK(ModelT::matches(expr, expected));
  auto assigned = x;
  assigned ^= y | z;
  if constexpr (std::same_as<ModelT, integer_model>) {
   core::assign_complement(assigned);
  }
  if constexpr (std::same_as<ModelT, fdb_model>) {
   q::assign_complement(assigned);
  }
  assigned -= (u & v) | ~w;
  CHECK(ModelT::matches(assigned, expected));
 }
}

TEST_CASE("deep mixed expressions retain all operands and normalization", "[fdb][algebra][nested]")
{
 check_nested_expressions<integer_model>();
 check_nested_expressions<fdb_model>();
}

TEST_CASE("FDB options and universe survive operator dispatch and mutation", "[fdb][algebra]")
{
 const q::query_options options {.result_limit = 7, .target_bytes = 256,
   .reverse_order = true, .streaming_mode = FDB_STREAMING_MODE_SMALL};
 const auto left = q::with_options(q::between("a", "m"), options);
 const auto right = q::between("m", "z");
 const auto verify = [&](const q::expression auto& expr, const q::query_options expected) {
  CHECK(expected == q::detail::options_of(expr));
  q::for_each_interval(expr, [&](const q::interval& piece) { CHECK(expected == piece.options); });
 };

 verify(left | right, options);
 verify(right | left, options);
 verify(left & right, options);
 verify(left ^ right, options);
 verify(right ^ left, options);
 verify(left - right, options);
 verify(right - left, {});
 verify(~left, options);

 q::interval_set selected {left};
 selected ^= selected;
 verify(selected, options);
 CHECK(q::is_empty_expression(selected));
 q::assign_complement(selected);
 verify(selected, options);
 CHECK(q::contains(selected, ""));
 CHECK_FALSE(q::contains(selected, "\xFF"sv));

 const auto conflict = q::with_options(right, {.result_limit = 9});
 const auto original = q::compile_intervals(selected);
 CHECK_THROWS_AS(selected |= conflict, lfdb::libfdb_exception);
 CHECK_THROWS_AS(selected &= conflict, lfdb::libfdb_exception);
 CHECK_THROWS_AS(selected ^= conflict, lfdb::libfdb_exception);
 CHECK(original == q::compile_intervals(selected));
 CHECK(options == selected.options);
 selected -= conflict;
 verify(selected, options);

 const q::interval_set outer {left};
 const q::interval_set inner {q::with_options(q::between("b", "c"), {.result_limit = 9})};
 CHECK(q::intersects(outer, inner));
 CHECK_FALSE(q::is_disjoint(outer, inner));
 CHECK(q::encloses(outer, inner));

 using byte_query = core::query<q::byte_string_domain>;
 const auto algebraic = byte_query::universal();
 const auto pure_complement = ~byte_query::empty();
 CHECK(core::contains(pure_complement, "\xFF"s));
 for (const auto& expr : {q::interval_set{left | algebraic}, q::interval_set{algebraic | left},
                          q::interval_set{left & algebraic}, q::interval_set{algebraic & left},
                          q::interval_set{left ^ algebraic}, q::interval_set{algebraic ^ left}}) {
  verify(expr, options);
  CHECK_FALSE(q::contains(expr, "\xFF"sv));
 }
 const q::interval_set bounded {algebraic};
 CHECK(q::compile_intervals(bounded) == std::vector {q::universal()});

 q::interval_set flipped {q::at("a") | q::at("z")};
 flipped ^= q::between("", "m");
 CHECK(q::contains(flipped, "z"));
 q::interval_set relative {q::between("", "m") - (q::at("a") | q::at("z"))};
 CHECK_FALSE(q::contains(relative, "z"));
}

TEST_CASE("named mutators share the operators and retain reference semantics", "[fdb][algebra]")
{
 q::interval_set selected {q::between("a", "z")};
 const auto cut = q::between("m", "n");
 CHECK(std::addressof(selected) == std::addressof(q::assign_difference(selected, cut)));
 CHECK(2 == q::interval_count(selected));
 CHECK(std::addressof(selected) == std::addressof(q::assign_set_union(selected, cut)));
 CHECK(1 == q::interval_count(selected));
 CHECK(std::addressof(selected) == std::addressof(q::assign_intersection(selected, cut)));
 CHECK(q::compile_intervals(selected) == std::vector {cut});
 CHECK(std::addressof(selected) == std::addressof(q::assign_symmetric_difference(selected, selected)));
 CHECK(q::is_empty_expression(selected));
 CHECK(std::addressof(selected) == std::addressof(q::assign_complement(selected)));
 CHECK(q::compile_intervals(selected) == std::vector {q::universal()});

 selected &= selected;
 selected |= selected;
 selected -= selected;
 CHECK(q::is_empty_expression(selected));
 selected |= q::at("a");
 selected ^= std::move(selected);
 CHECK(q::is_empty_expression(selected));
 selected = ~selected;
 CHECK(q::compile_intervals(selected) == std::vector {q::universal()});

 const auto owned = q::prefix("record/") ^ q::prefix("record/private/");
 selected = q::interval_set{owned};
 CHECK(q::contains(selected, "record/a"));
 CHECK_FALSE(q::contains(selected, "record/private/a"));
 const auto fixed = q::compile_intervals<2>(owned);
 CHECK(2 == fixed.size());
 CHECK_THROWS_AS(q::compile_intervals<1>(owned), lfdb::libfdb_exception);
}

TEST_CASE("query key adaptation borrows ADL providers", "[fdb][algebra][keys]")
{
 const borrowed_key lower {"a"}, upper {"z"}, middle {"m"};
 const auto selection = q::between(lower, upper);
 CHECK(q::contains(selection, lower));
 CHECK(q::contains(selection, middle));
 CHECK(!q::contains(selection, upper));
 CHECK(q::contains(q::prefix(borrowed_key{"object/"}), borrowed_key{"object/name"}));
 CHECK(!q::contains(q::prefix(borrowed_key{"object/"}), borrowed_key{"neighbor/name"}));
}

TEST_CASE("scalar selector consumers share normalization", "[fdb][algebra][bounds]")
{
 const std::array keys {""sv, "\0"sv, "a"sv, "z"sv, "\xFF"sv, "\xFF\0"sv};
 for (const auto lower : keys) {
  for (const auto upper : keys) {
   for (const bool include_lower : {false, true}) {
    for (const bool include_upper : {false, true}) {
     auto selection = q::empty();
     selection.begin_key = lower;
     selection.end_key = upper;
     selection.begin_inclusive = include_lower;
     selection.end_inclusive = include_upper;
     selection.options = {.result_limit = 3, .reverse_order = true};
     const auto original = selection;
     const auto reserved = upper.starts_with('\xFF');
     const auto clipped_upper = reserved ? "\xFF"sv : upper;
     const auto empty = lower > clipped_upper ||
       (lower == clipped_upper && !(include_lower && include_upper && !reserved));
     CHECK(empty == q::is_empty(selection));
     CHECK((empty ? 0 : 1) == q::interval_count(selection));
     const auto exported = q::compile_intervals(selection);
     const auto executable = lfdb::detail::intervals(selection);
     CHECK(empty == exported.empty());
     CHECK(std::ranges::equal(exported, executable));
     CHECK(original == selection);
     if (!empty) {
      CHECK(clipped_upper == executable.front().end_key);
      CHECK((include_upper && !reserved) == executable.front().end_inclusive);
      CHECK(original.options == executable.front().options);
     }
    }
   }
  }
 }
}

TEST_CASE("all byte endpoint combinations follow independent membership", "[fdb][algebra][bounds]")
{
 const std::array<std::string, 4> endpoints {"", "a", "b", "\xFF"};
 std::vector<q::interval> ranges;
 for (const auto& lower : endpoints) {
  for (const auto& upper : endpoints) {
   for (const bool includes_lower : {false, true}) {
    for (const bool includes_upper : {false, true}) {
     ranges.emplace_back(lower, upper, includes_lower, includes_upper);
    }
   }
  }
 }
 const auto member = [](const q::interval& x, const std::string_view key) {
  return "\xFF"sv > key &&
         (key > x.begin_key || (key == x.begin_key && x.begin_inclusive)) &&
         (key < x.end_key || (key == x.end_key && x.end_inclusive));
 };

 for (const auto& lhs : ranges) {
  for (const auto& rhs : ranges) {
   for (const auto key : {""sv, "\0"sv, "a"sv, "a\0"sv, "b"sv, "b\0"sv, "\xFE"sv, "\xFF"sv}) {
    const bool a = member(lhs, key), b = member(rhs, key);
    CHECK((a && b) == q::contains(lhs & rhs, key));
    CHECK((a || b) == q::contains(lhs | rhs, key));
    CHECK((a && !b) == q::contains(lhs - rhs, key));
    CHECK((a != b) == q::contains(lhs ^ rhs, key));
    CHECK(("\xFF"sv > key && !a) == q::contains(~lhs, key));
   }
  }
 }
}

TEST_CASE("mixed core geometry emits only nonempty ordinary FDB ranges", "[fdb][algebra][bounds]")
{
 using byte_query = core::query<q::byte_string_domain>;
 const auto outside = byte_query::between(core::boundary<q::byte_string_domain>::open("\xFF"),
                                          core::boundary<q::byte_string_domain>::positive_infinity());
 REQUIRE(core::contains(outside, "\xFF\0"s));
 const auto inside = q::at("a");
 CHECK(q::compile_intervals(inside | outside) == std::vector {inside});
 CHECK(q::compile_intervals(outside | inside) == std::vector {inside});
 CHECK(q::compile_intervals(inside ^ outside) == std::vector {inside});
 CHECK(q::is_empty_expression(outside & q::universal()));
 CHECK(q::compile_intervals(q::empty() | outside).empty());
 CHECK(q::compile_intervals(q::empty() & byte_query::universal()).empty());
 CHECK(q::compile_intervals(q::empty() | core::before<q::byte_string_domain>(""s)).empty());
}

struct move_only_interval final
{
 using domain_type = q::byte_string_domain;
 std::string lower_key, upper_key;
 static inline int moves = 0;

 move_only_interval(std::string lower, std::string upper)
  : lower_key(std::move(lower)), upper_key(std::move(upper)) {}
 move_only_interval(const move_only_interval&) = delete;
 move_only_interval(move_only_interval&& other) noexcept
  : lower_key(std::move(other.lower_key)), upper_key(std::move(other.upper_key)) { ++moves; }

 auto lower() const { return core::boundary_ref<domain_type>::closed(lower_key); }
 auto upper() const { return core::boundary_ref<domain_type>::open(upper_key); }
};

TEST_CASE("mutation borrows providers and xor owns each rvalue once", "[fdb][algebra][ownership]")
{
 move_only_interval left {std::string(256, 'a'), std::string(256, 'm')};
 move_only_interval right {std::string(256, 'm'), std::string(256, 'z')};
 q::interval_set selected {left};
 selected |= right;
 CHECK(1 == q::interval_count(selected));
 CHECK(std::string(256, 'a') == left.lower_key);
 CHECK(std::string(256, 'z') == right.upper_key);
 CHECK(q::intersects(selected, right));
 CHECK(q::encloses(selected, right));

 move_only_interval::moves = 0;
 const auto owned = core::symmetric_difference(std::move(left), std::move(right));
 CHECK(2 == move_only_interval::moves);
 CHECK(core::contains(owned, std::string(256, 'n')));
 selected = q::interval_set{owned};
 CHECK(1 == q::interval_count(selected));

 struct sink final {
  int count = 0;
  sink() = default;
  sink(const sink&) = delete;
  void operator()(const q::interval&) { ++count; }
 } emit;
 q::for_each_interval(selected, emit);
 CHECK(1 == emit.count);

 auto configured = q::with_options(std::move(selected), {.result_limit = 11});
 static_assert(std::same_as<decltype(configured), q::interval_set>);
 configured &= q::starting_at(q::universal(), std::string(256, 'b'));
 CHECK(11 == configured.options.result_limit);

 struct owning_interval final {
  using domain_type = q::byte_string_domain;
  std::string lower_key, upper_key;
  auto lower() const { return core::boundary<domain_type>::closed(lower_key); }
  auto upper() const { return core::boundary<domain_type>::open(upper_key); }
 };
 const q::interval_set materialized {
  owning_interval{std::string(256, 'a'), std::string(256, 'z')}};
 CHECK(q::contains(materialized, std::string(256, 'm')));
 CHECK_FALSE(q::contains(materialized, std::string(256, 'z')));

 struct borrowed_set final {
  using domain_type = q::byte_string_domain;
  using interval_expression_type = core::interval_set<domain_type>;
  const interval_expression_type& expression;
  q::query_options options;
 };
 q::interval_set aliased {q::between("a", "z")};
 const borrowed_set view {aliased.expression, aliased.options};
 aliased ^= view;
 CHECK(q::is_empty_expression(aliased));
 aliased |= q::at("b");
 aliased -= view;
 CHECK(q::is_empty_expression(aliased));
}

struct injected_failure final {};

struct checked_key final
{
 int value;
 static inline int copies_left = -1;

 checked_key() = delete;
 explicit checked_key(const int x) : value(x) {}
 checked_key(const checked_key& other) : value(other.value)
 {
  if (0 == copies_left) {
   throw injected_failure{};
  }
  if (0 < copies_left) {
   --copies_left;
  }
 }
 checked_key(checked_key&&) = default;

 checked_key& operator=(const checked_key&) = default;
 checked_key& operator=(checked_key&&) = default;
};

struct checked_domain final
{
 using value_type = checked_key;
 static inline int comparisons_left = -1;

 static auto compare(const checked_key& lhs, const checked_key& rhs)
 {
  if (0 == comparisons_left) {
   throw injected_failure{};
  }
  if (0 < comparisons_left) {
   --comparisons_left;
  }
  return lhs.value <=> rhs.value;
 }
};

// Provider functions must not participate in this internal helper lookup:
void canonical_intervals(const core::interval_set<checked_domain>&) = delete;

TEST_CASE("canonical predicates and interval counting do not copy keys", "[fdb][algebra][ownership]")
{
 using checked_query = core::query<checked_domain>;
 using checked_set = core::interval_set<checked_domain>;
 const auto outer_interval = checked_query::closed(checked_key{0}, checked_key{10});
 const auto inner_interval = checked_query::open(checked_key{2}, checked_key{8});
 const checked_set outer {outer_interval};
 const checked_set inner {inner_interval};
 const checked_set separate {checked_query::closed(checked_key{20}, checked_key{30})};
 const checked_set empty;
 const auto difference = core::difference(outer_interval, inner_interval);

 struct copy_budget_reset final
 {
  ~copy_budget_reset() { checked_key::copies_left = -1; }
 } reset;

 checked_key::copies_left = 0;

 CHECK(core::intersects(outer, inner));
 CHECK_FALSE(core::intersects(outer, separate));
 CHECK(core::encloses(outer, inner));
 CHECK_FALSE(core::encloses(inner, outer));
 CHECK(core::encloses(outer, empty));
 CHECK_FALSE(core::encloses(empty, inner));
 CHECK(core::encloses(outer_interval, inner_interval));
 CHECK_FALSE(core::encloses(inner_interval, outer_interval));
 CHECK(2 == core::interval_count(difference));
 CHECK_FALSE(core::is_empty_expression(difference));
}

TEST_CASE("core mutators publish only after all copies and comparisons succeed", "[fdb][algebra][failure]")
{
 using checked_query = core::query<checked_domain>;
 using checked_set = core::interval_set<checked_domain>;
 const checked_set original {checked_query::closed(checked_key{0}, checked_key{10})};
 const checked_set rhs {checked_query::open(checked_key{2}, checked_key{8})};
 const auto describe = [](const checked_set& x) {
  std::vector<std::array<int, 4>> out;
  for (const auto& piece : x) {
   out.push_back({piece.lower().finite_key().value, piece.upper().finite_key().value,
                  piece.lower().inclusive(), piece.upper().inclusive()});
  }
  return out;
 };
 using operation = checked_set& (*)(checked_set&, const checked_set&);
 const std::array<operation, 5> operations {
  [](checked_set& x, const checked_set& y) -> checked_set& { return core::assign_intersection(x, y); },
  [](checked_set& x, const checked_set& y) -> checked_set& { return core::assign_set_union(x, y); },
  [](checked_set& x, const checked_set& y) -> checked_set& { return core::assign_difference(x, y); },
  [](checked_set& x, const checked_set& y) -> checked_set& { return core::assign_symmetric_difference(x, y); },
  [](checked_set& x, const checked_set&) -> checked_set& { return core::assign_complement(x); }
 };

 for (const auto mutate : operations) {
  for (const bool fail_copies : {false, true}) {
   bool caught = false, succeeded = false;
   for (int budget = 0; 512 > budget && !succeeded; ++budget) {
    auto selected = original;
    checked_key::copies_left = fail_copies ? budget : -1;
    checked_domain::comparisons_left = fail_copies ? -1 : budget;
    try
     {
     mutate(selected, rhs);
     succeeded = true;
     }
    catch (const injected_failure&)
     {
     caught = true;
     CHECK(describe(original) == describe(selected));
     CHECK(describe(original) == std::vector {std::array {0, 10, 1, 1}});
     CHECK(describe(rhs) == std::vector {std::array {2, 8, 0, 0}});
     }
    checked_key::copies_left = -1;
    checked_domain::comparisons_left = -1;
   }
   CHECK(caught);
   CHECK(succeeded);
  }
 }
}

struct immutable_key final
{
 const int value;
 immutable_key() = delete;
 explicit constexpr immutable_key(const int x) : value(x) {}
};

struct immutable_domain final
{
 using value_type = immutable_key;
 static constexpr auto compare(const immutable_key& lhs, const immutable_key& rhs)
 {
  return lhs.value <=> rhs.value;
 }
};

struct immutable_model final
{
 using query = core::query<immutable_domain>;
 using set_type = core::interval_set<immutable_domain>;
 static constexpr auto universe = integer_model::universe;
 static constexpr auto witnesses = integer_model::witnesses;

 static set_type make(const unsigned mask)
 {
  const auto convert = [](const auto& bound) {
   using boundary = core::boundary<immutable_domain>;
   if (core::boundary_kind::negative_infinity == bound.kind()) {
    return boundary::negative_infinity();
   }
   if (core::boundary_kind::positive_infinity == bound.kind()) {
    return boundary::positive_infinity();
   }
   return bound.inclusive() ? boundary::closed(immutable_key{bound.finite_key()})
                            : boundary::open(immutable_key{bound.finite_key()});
  };
  set_type out;
  for (const auto& piece : integer_model::expected(mask)) {
   out |= query::between(convert(piece.lower()), convert(piece.upper()));
  }
  return out;
 }

 static bool matches(const core::expression auto& expr, const unsigned mask)
 {
  const auto same_bound = [](const auto& expected, const auto& actual) {
   return expected.kind() == actual.kind() && expected.inclusive() == actual.inclusive() &&
          (!expected.finite() || expected.finite_key() == actual.finite_key().value);
  };
  return std::ranges::equal(integer_model::expected(mask), core::compile_intervals(expr),
   [&same_bound](const auto& expected, const auto& actual) {
    return same_bound(expected.lower(), actual.lower()) && same_bound(expected.upper(), actual.upper());
   });
 }

 static bool member(const core::expression auto& expr, const int key)
 {
  return core::contains(expr, immutable_key{key});
 }
};

TEST_CASE("immutable keys obey the full five-atom Boolean algebra", "[fdb][algebra][laws][immutable]")
{
 check_algebra<immutable_model>();
}

template <typename ExprT>
concept fixed_compilable = requires(const ExprT& expr) { core::compile_intervals<2>(expr); };

static_assert(core::ordered_domain<immutable_domain>);
static_assert(!std::assignable_from<immutable_key&, immutable_key>);
static_assert(!fixed_compilable<core::query<immutable_domain>>);
static_assert(fixed_compilable<integer_query>);

TEST_CASE("dynamic algebra supports immutable keys without default construction or equality", "[fdb][algebra][immutable]")
{
 using query = core::query<immutable_domain>;
 using set = core::interval_set<immutable_domain>;
 const set lhs {query::closed(immutable_key{0}, immutable_key{10})};
 const set rhs {query::open(immutable_key{2}, immutable_key{8})};
 const std::array results {set{lhs & rhs}, set{lhs | rhs}, set{lhs - rhs}, set{lhs ^ rhs}, set{~lhs}};
 for (int key = -1; 12 > key; ++key) {
  const bool a = 0 <= key && 10 >= key, b = 2 < key && 8 > key;
  const std::array expected {a && b, a || b, a && !b, a != b, !a};
  for (unsigned index = 0; results.size() > index; ++index) {
   CHECK(expected[index] == core::contains(results[index], immutable_key{key}));
  }
 }
 auto assigned = lhs;
 assigned ^= rhs;
 assigned |= rhs;
 assigned &= lhs;
 assigned -= rhs;
 assigned = ~assigned;
 core::assign_complement(assigned);
 CHECK(2 == assigned.size());
 CHECK(core::contains(assigned, immutable_key{1}));
 CHECK_FALSE(core::contains(assigned, immutable_key{5}));
}

TEST_CASE("fixed storage publishes its size after successful assignment", "[fdb][algebra][failure]")
{
 struct stored_interval final {
  using domain_type = integer_domain;
  int value = 0;
  static auto lower() { return core::boundary<domain_type>::closed(0); }
  static auto upper() { return core::boundary<domain_type>::closed(1); }
  stored_interval& operator=(const stored_interval& other)
  {
   if (0 > other.value) {
    throw injected_failure{};
   }
   value = other.value;
   return *this;
  }
 };
 core::static_interval_set<integer_domain, 2, stored_interval> out;
 out.push_back(stored_interval{3});
 CHECK_THROWS_AS(out.push_back(stored_interval{-1}), injected_failure);
 CHECK(1 == out.size());
 CHECK(3 == out[0].value);
 out.push_back(stored_interval{9});
 CHECK(2 == out.size());
 CHECK(9 == out[1].value);
 CHECK_THROWS_AS(out.push_back(stored_interval{11}), std::length_error);
}

} // anonymous namespace
