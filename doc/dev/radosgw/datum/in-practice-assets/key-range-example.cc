// Select names under a prefix, after one marker and before another.
#include <datum/key.h>

#include <array>
#include <iostream>
#include <string_view>

namespace datum = ::ceph::salad::datum;

int main()
{
    using namespace std::string_view_literals;

    const std::array names {
        "notes/01"sv, "photos/10"sv, "photos/30"sv,
        "photos/70"sv, "photos/90"sv, "videos/01"sv
    };
    auto selected = datum::key::byte_query::prefix("photos/");
    selected = datum::key::starting_after(selected, "photos/20");
    selected = datum::key::ending_before(selected, "photos/80");

    for (const auto name : names) {
        if (datum::interval::contains(selected, name)) {
            std::cout << name << '\n';
        }
    }
}
