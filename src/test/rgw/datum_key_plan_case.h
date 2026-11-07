// Shared by runtime coverage and the independent constexpr check.
#ifndef CEPH_TEST_RGW_DATUM_KEY_PLAN_CASE_H
 #define CEPH_TEST_RGW_DATUM_KEY_PLAN_CASE_H

#include <datum/key.h>
#include <datum/plan/query.h>

#include <cstdint>
#include <limits>
#include <utility>

#include <string>
#include <string_view>
#include <tuple>

namespace datum_test {

namespace datum = ::ceph::salad::datum;

constexpr bool key_plan()
{
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    const auto segment = datum::key::fixed_width_hexadecimal_segment(maximum);
    const auto parent = datum::key::compose("neutral", std::string_view {"a\0b", 3});
    const auto child = parent / "leaf";
    const auto decoded = datum::key::decode<std::string, std::string, std::string>(child);
    const auto plan =
        datum::plan::with_control(datum::plan::make_query(datum::key::prefix(parent)), 4);
    const auto singleton = datum::key::at(child);
    const auto partial = datum::key::prefix(parent, "le");

    return datum::key::decimal_segment(maximum) == "18446744073709551615" and
           datum::key::decode_fixed_width_hexadecimal_segment<std::uint64_t>(
               std::string_view {segment}) == maximum and
           decoded and std::get<1>(*decoded) == std::string_view {"a\0b", 3} and
           datum::plan::is_resolved(plan) and datum::plan::control_count(plan) == 1 and
           datum::interval::contains(plan.intervals, datum::key::key_view(child)) and
           datum::interval::contains(singleton, datum::key::key_view(child)) and
           datum::interval::contains(partial, datum::key::key_view(child));
}

// Move/copy ownership and finite/infinite transitions must also work without framework headers.
constexpr bool key_query_ownership()
{
    using domain = datum::key::byte_string_domain;
    using query = datum::interval::query<domain>;

    constexpr auto text = std::string_view {
        "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk"};

    for (const auto length : {0, 1, 15, 16, 80}) {
        const auto lower = datum::compat::string_from_view(text.substr(0, length));
        auto upper = lower;
        upper += 'z';
        auto source = query::closed_open(lower, upper);
        const auto copied = source;
        const auto first = datum::interval::closed_bound<domain>(std::string(lower));
        const auto last = datum::interval::open_bound<domain>(std::string(upper));
        const auto named = datum::interval::between<domain>(lower, upper);
        if (first != copied.lower() || last != copied.upper() || named != copied) {
            return false;
        }

        auto moved = std::move(source);

        if (moved != copied) {
            return false;
        }

        auto assigned = query::universal();
        assigned = std::move(moved);
        if (assigned != copied) {
            return false;
        }

        assigned = query::universal();
        if (!datum::interval::is_below_all(assigned.lower()) ||
            !datum::interval::is_above_all(assigned.upper())) {
            return false;
        }

        assigned = copied;
        auto& same = assigned;
        assigned = std::move(same);
        assigned = copied;
        if (assigned != copied) {
            return false;
        }
    }

    const auto maximum = datum::compat::string_from_view("\xFF\xFF\xFF");
    const auto unbounded = query::prefix(maximum);
    const auto all = query::prefix(std::string {});

    return unbounded.lower().inclusive() and unbounded.lower().key() == maximum and
           datum::interval::is_above_all(unbounded.upper()) and all == query::universal();
}

} // datum_test

#endif // CEPH_TEST_RGW_DATUM_KEY_PLAN_CASE_H
