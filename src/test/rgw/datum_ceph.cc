// Native byte ownership and the ordinary Ceph declaration dialect.
#include <ceph-datum.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <iterator>
#include <utility>

#include <algorithm>
#include <ranges>

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;

namespace {

template <typename OutputT>
concept native_copy_output = requires(const ceph::buffer::list& source, OutputT&& output) {
    datum::copy(source, datum::byte_extent {}, std::forward<OutputT>(output));
    datum::copy(source, std::uint64_t {}, std::forward<OutputT>(output));
    datum::copy(source, std::forward<OutputT>(output));
};

static_assert(native_copy_output<std::array<char, 4>&>);
static_assert(native_copy_output<std::span<char>>);
static_assert(!native_copy_output<std::array<char, 4>>);
static_assert(!native_copy_output<const std::array<char, 4>&>);
static_assert(!native_copy_output<std::array<unsigned, 4>&>);

auto fragmented_input()
{
    ::ceph::buffer::list result;
    result.append(::ceph::buffer::copy("ab", 2));
    result.append(::ceph::buffer::copy("cdef", 4));

    return result;
}

} // anonymous namespace

TEST_CASE("Ceph copy and slice distinguish payload ownership")
{
    auto source = fragmented_input();
    auto independent = datum::copy(source, {1, 4});
    auto shared = datum::slice(source, {1, 4});
    source.mut_buffers().front().c_str()[1] = 'X';

    CHECK(independent.to_str() == "bcde");
    CHECK(shared.to_str() == "Xcde");
    source.clear();
    CHECK(shared.to_str() == "Xcde");
    CHECK(independent.to_str() == "bcde");
}

TEST_CASE("Ceph byte vocabulary supplies standard fragments and checked copying")
{
    const auto source = fragmented_input();
    auto blocks = datum::fragments(source);
    STATIC_REQUIRE(std::ranges::forward_range<decltype(blocks)>);
    STATIC_REQUIRE_FALSE(std::ranges::borrowed_range<decltype(blocks)>);
    auto bytes = blocks | std::views::join;
    STATIC_REQUIRE(std::ranges::input_range<decltype(bytes)>);
    std::string flattened;
    std::ranges::copy(bytes, std::back_inserter(flattened));
    CHECK(flattened == "abcdef");

    std::array<std::byte, 4> output {};
    datum::copy(source, 1, output);
    CHECK(output ==
          std::array {std::byte {'b'}, std::byte {'c'}, std::byte {'d'}, std::byte {'e'}});
    CHECK_THROWS_AS(datum::copy(source, datum::byte_extent {5, 2}, output), std::out_of_range);
    CHECK(output.front() == std::byte {'b'});
    CHECK_THROWS_AS(datum::copy(source, datum::byte_extent {0, 5}, output), std::length_error);
    CHECK(datum::slice(source, {6, 0}).length() == 0);
    CHECK_THROWS_AS(datum::slice(source, {7, 0}), std::out_of_range);
    volatile unsigned char observed[2] {};
    datum::copy(source, 2, observed);
    CHECK(observed[0] == 'c');
    CHECK(observed[1] == 'd');
}

TEST_CASE("Ceph whole-source copies return the written prefix and preserve capacity failures")
{
    auto source = fragmented_input();
    std::array<char, 8> output {'?', '?', '?', '?', '?', '?', '!', '!'};
    const auto written = datum::copy(source, output);
    CHECK(std::string_view {written.data(), written.size()} == "abcdef");
    CHECK(written.data() == output.data());
    CHECK(output[6] == '!');
    std::array<char, 3> short_output {'?', '?', '?'};
    CHECK_THROWS_AS(datum::copy(source, short_output), std::length_error);
    CHECK(short_output == std::array {'?', '?', '?'});
    source.clear();
    CHECK(datum::copy(source, output).empty());
    CHECK(output[0] == 'a');
}

TEST_CASE("Ceph contiguous reads borrow one fragment and gather multiple fragments")
{
    const auto source = fragmented_input();
    auto input = source.cbegin();
    std::array<char, 2> scratch {'?', '?'};
    const auto first = datum::read_contiguous(input, scratch);
    CHECK(std::string_view {first.data(), first.size()} == "ab");
    CHECK(first.data() != scratch.data());
    input = source.cbegin(1);
    const auto gathered = datum::read_contiguous(input, scratch);
    CHECK(std::string_view {gathered.data(), gathered.size()} == "bc");
    CHECK(gathered.data() == scratch.data());
    CHECK(input.get_remaining() == 3);
    std::array<char, 4> too_large {};
    CHECK_THROWS_AS(datum::read_contiguous(input, too_large), ::ceph::buffer::end_of_buffer);
    CHECK(input.get_remaining() == 3);
}

TEST_CASE("Ceph fragment views safely expose empty native descriptors")
{
    auto source = fragmented_input();
    source.push_back(ceph::buffer::ptr_node::create(ceph::buffer::ptr {}));
    source.push_back(ceph::buffer::create(0));
    auto blocks = datum::fragments(source);
    CHECK(std::ranges::count_if(blocks, [](const auto block) { return block.empty(); }) == 2);
    std::string flattened;
    std::ranges::copy(blocks | std::views::join, std::back_inserter(flattened));
    CHECK(flattened == "abcdef");
}
