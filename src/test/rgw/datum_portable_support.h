// Shared neutral fixtures for portable and reflected Datum tests.
#ifndef CEPH_TEST_RGW_DATUM_PORTABLE_SUPPORT_H
 #define CEPH_TEST_RGW_DATUM_PORTABLE_SUPPORT_H

#include <compare>
#include <functional>
#include <ranges>
#include <utility>

namespace {

template <typename ViewT, typename ConsumerT>
constexpr void consume_cuts(const ViewT& cuts, ConsumerT&& consumer)
{
    for (auto&& cut : cuts) {
        std::invoke(consumer, std::forward<decltype(cut)>(cut));
    }
}

template <typename T>
struct mechanics_domain
{
    using value_type = T;

public:
    static constexpr std::strong_ordering compare(const T& left, const T& right)
    {
        return left <=> right;
    }

};

} // anonymous namespace

#endif // CEPH_TEST_RGW_DATUM_PORTABLE_SUPPORT_H
