// Fragment-to-fragment transfer, capacity admission, and physical write footprints.
#include "datum/bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <ranges>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace datum = ceph::salad::datum;

namespace {

template <typename ByteT, std::size_t Extent>
auto partition_bytes(const std::span<ByteT, Extent> bytes, const unsigned mask)
{
    std::vector<std::span<ByteT>> blocks {bytes.first(0)};
    std::size_t first = 0;
    for (std::size_t last = 1; last < bytes.size(); ++last) {
        if (mask & (1U << (last - 1))) {
            blocks.push_back(bytes.subspan(first, last - first));
            blocks.push_back(bytes.subspan(last, 0));
            first = last;
        }
    }
    blocks.push_back(bytes.subspan(first));
    blocks.push_back(bytes.last(0));

    return blocks;
}

constexpr bool constant_transfer()
{
    const char source[2][3] {{'a', 'b', 'c'}, {'d', 'e', 'f'}};
    std::array<std::byte, 2> first {};
    std::array<std::byte, 5> last {};
    last.fill(std::byte {'x'});
    const std::array<std::span<std::byte>, 3> destination {
        std::span {first}, std::span {last}.first<2>(), std::span {last}.last<3>()
    };
    const auto copied = datum::scatter_bytes(source, destination);

    return 6 == copied and first == std::array {std::byte {'a'}, std::byte {'b'}} and
           last == std::array {std::byte {'c'}, std::byte {'d'}, std::byte {'e'},
                               std::byte {'f'}, std::byte {'x'}};
}

static_assert(constant_transfer());

template <typename SourceT, typename DestinationT>
concept can_scatter = requires (SourceT&& source, DestinationT&& destination) {
    datum::scatter_bytes(std::forward<SourceT>(source), std::forward<DestinationT>(destination));
};

using destination_blocks = std::array<std::array<char, 2>, 2>;
using input_blocks = decltype(std::views::istream<std::string>(std::declval<std::istringstream&>()));

static_assert(!can_scatter<input_blocks&, destination_blocks&>);
static_assert(!can_scatter<std::array<std::string_view, 2>&, const destination_blocks&>);

} // anonymous namespace

TEST_CASE("fragment transfer matches a flat-byte oracle across independent boundaries")
{
    const std::array bytes {'a', '\0', 'b', 'c', static_cast<char>(0xFF), 'd'};
    for (unsigned source_mask = 0; source_mask != 32; ++source_mask) {
        const auto source = partition_bytes(std::span<const char> {bytes}, source_mask);
        for (unsigned destination_mask = 0; destination_mask != 128; ++destination_mask) {
            std::array<char, 8> output {};
            output.fill('x');
            const auto destination = partition_bytes(std::span {output}, destination_mask);

            CHECK(6 == datum::scatter_bytes(source, destination));
            CHECK(std::ranges::equal(output | std::views::take(6), bytes));
            CHECK('x' == output[6]);
            CHECK('x' == output[7]);
        }
    }
    CHECK(constant_transfer());
}

TEST_CASE("fragment transfer refuses insufficient capacity before writing")
{
    const std::array source {std::string_view {"abc"}, std::string_view {"de"}};
    std::array<char, 4> output {'x', 'x', 'x', 'x'};
    const std::array destination {std::span {output}.first(2), std::span {output}.last(2)};

    CHECK_THROWS_AS(datum::scatter_bytes(source, destination), std::length_error);
    CHECK(output == std::array {'x', 'x', 'x', 'x'});
}

TEST_CASE("generated fragment descriptors are retained once per transfer pass")
{
    unsigned source_reads = 0;
    unsigned destination_reads = 0;
    auto source = std::views::iota(0, 2) | std::views::transform([&](const int index) {
        ++source_reads;

        return std::string(80, static_cast<char>('a' + index));
    });
    std::array<std::array<char, 37>, 6> storage;
    for (auto& block : storage) {
        block.fill('x');
    }
    auto destination = storage | std::views::transform([&](auto& block) {
        ++destination_reads;

        return std::span {block};
    });

    CHECK(160 == datum::scatter_bytes(source, destination));
    CHECK(4 == source_reads);
    CHECK(11 == destination_reads);
    const auto expected = std::string(80, 'a') + std::string(80, 'b') + std::string(62, 'x');
    CHECK(std::ranges::equal(storage | std::views::join, expected));
}

TEST_CASE("fragment transfer supports move-only mutable metadata and borrowed destination blocks")
{
    struct block final
    {
        std::array<char, 2> bytes;

    public:
        block(const char first, const char last) : bytes {first, last}
        {}

        block(const block&) = delete;
        block(block&&) = default;

    public:
        auto begin() { return bytes.begin(); }
        auto end() { return bytes.end(); }

    };

    STATIC_REQUIRE_FALSE(std::default_initializable<block>);
    STATIC_REQUIRE_FALSE(datum::byte_block<const block>);
    unsigned observations = 0;
    auto source = std::views::iota(0, 2) | std::views::transform([&](const int index) {
        ++observations;

        return block {static_cast<char>('a' + 2 * index), static_cast<char>('b' + 2 * index)};
    });
    std::array destination {block {'x', 'x'}, block {'x', 'x'}, block {'x', 'x'}};

    CHECK(4 == datum::scatter_bytes(source, destination));
    CHECK(4 == observations);
    CHECK(destination[0].bytes == std::array {'a', 'b'});
    CHECK(destination[1].bytes == std::array {'c', 'd'});
    CHECK(destination[2].bytes == std::array {'x', 'x'});

    char raw_destination[3][2] {{'x', 'x'}, {'x', 'x'}, {'x', 'x'}};
    CHECK(4 == datum::scatter_bytes(source, raw_destination));
    CHECK(std::ranges::equal(raw_destination[0], std::array {'a', 'b'}));
    CHECK(std::ranges::equal(raw_destination[1], std::array {'c', 'd'}));
    CHECK(std::ranges::equal(raw_destination[2], std::array {'x', 'x'}));
}

TEST_CASE("rvalue-reference fragments retain their original owners during byte copying")
{
    std::array owners {std::string {"ab"}, std::string {"cd"}};
    auto source = owners | std::views::transform([](auto& block) -> auto&& { return std::move(block); });
    std::array<std::array<char, 2>, 2> destination {};

    CHECK(4 == datum::scatter_bytes(source, destination));
    CHECK(owners == std::array {std::string {"ab"}, std::string {"cd"}});
    CHECK(destination == std::array {std::array {'a', 'b'}, std::array {'c', 'd'}});
}

TEST_CASE("empty byte transfers do not inspect destination fragments")
{
    std::array<char, 2> storage {'x', 'x'};
    const std::array blocks {std::span {storage}};
    auto destination = blocks | std::views::transform([](const auto block) {
        FAIL("empty transfer inspected destination");

        return block;
    });
    const std::array<std::span<const char>, 2> source {};

    CHECK(0 == datum::scatter_bytes(source, destination));
    CHECK(0 == datum::scatter_bytes(std::span<const char> {}, destination));
    CHECK(storage == std::array {'x', 'x'});
}

TEST_CASE("source observation failure leaves only the copied destination prefix")
{
    const std::array blocks {std::string_view {"ab"}, std::string_view {"cd"}};
    unsigned observations = 0;
    auto source = blocks | std::views::transform([&](const auto block) {
        if (++observations == 4) {
            throw std::runtime_error {"source observation failed"};
        }

        return block;
    });
    std::array<char, 4> storage {'x', 'x', 'x', 'x'};
    const std::array destination {std::span {storage}.first(2), std::span {storage}.last(2)};

    CHECK_THROWS_AS(datum::scatter_bytes(source, destination), std::runtime_error);
    CHECK(4 == observations);
    CHECK(storage == std::array {'a', 'b', 'x', 'x'});
}

TEST_CASE("destination observation failure preserves the completed fragment prefix")
{
    const std::array source {std::string_view {"abcd"}};
    std::array<char, 4> storage {'x', 'x', 'x', 'x'};
    const std::array blocks {std::span {storage}.first(2), std::span {storage}.last(2)};
    unsigned observations = 0;
    auto destination = blocks | std::views::transform([&](const auto block) {
        if (++observations == 4) {
            throw std::runtime_error {"destination observation failed"};
        }

        return block;
    });

    CHECK_THROWS_AS(datum::scatter_bytes(source, destination), std::runtime_error);
    CHECK(4 == observations);
    CHECK(storage == std::array {'a', 'b', 'x', 'x'});
}

TEST_CASE("fragment transfer preserves volatile byte access across different byte spellings")
{
    std::array<volatile unsigned char, 4> source {1, 2, 3, 255};
    std::array<volatile char, 5> output {'x', 'x', 'x', 'x', 'x'};
    const std::array blocks {std::span<const volatile unsigned char> {source.data(), 1},
                             std::span<const volatile unsigned char> {source.data() + 1, 3}};
    const std::array destination {std::span<volatile char> {output.data(), 2},
                                  std::span<volatile char> {output.data() + 2, 3}};

    CHECK(4 == datum::scatter_bytes(blocks, destination));
    CHECK(1 == static_cast<unsigned char>(output[0]));
    CHECK(2 == static_cast<unsigned char>(output[1]));
    CHECK(3 == static_cast<unsigned char>(output[2]));
    CHECK(255 == static_cast<unsigned char>(output[3]));
    CHECK('x' == output[4]);
}
