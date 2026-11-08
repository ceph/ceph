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
#include <new>
#include <utility>

#include <algorithm>
#include <ranges>

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;

namespace {

// An ADL decoder accepts this value without a default constructor, copy, or move.
struct immobile_record final
{
    std::uint32_t first;
    std::uint32_t second;

public:
    explicit immobile_record(std::uint32_t first, std::uint32_t second)
        : first(first), second(second)
    {}
    immobile_record(const immobile_record&) = delete;
    immobile_record(immobile_record&&) = delete;
    immobile_record& operator=(const immobile_record&) = delete;
    immobile_record& operator=(immobile_record&&) = delete;

public:
    friend void decode(immobile_record& value, ::ceph::buffer::list::const_iterator& input)
    {
        ::ceph::decode(value.first, input);
        ::ceph::decode(value.second, input);
    }
};

struct exceptional_record final
{
    bool allocation;

    friend void decode(exceptional_record& value, ::ceph::buffer::list::const_iterator&)
    {
        if (value.allocation) {
            throw std::bad_alloc {};
        }
        throw std::runtime_error {"decoder failure"};
    }
};

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

TEST_CASE("Ceph fallible decoding accepts native values and ADL without moving records")
{
    ::ceph::buffer::list bytes;
    ::ceph::encode(std::uint32_t {19}, bytes);
    ::ceph::encode(std::uint32_t {23}, bytes);
    immobile_record record {1, 2};
    CHECK(datum::try_decode(record, bytes));
    CHECK(record.first == 19);
    CHECK(record.second == 23);

    auto input = bytes.cbegin();
    std::uint32_t first = 0;
    CHECK(datum::try_decode(first, input));
    CHECK(first == 19);
    CHECK(input.get_remaining() == sizeof(std::uint32_t));
    CHECK(datum::try_decode(first, input));
    CHECK(first == 23);
    CHECK(input.end());
}

TEST_CASE("Ceph fallible decoding retains native partial mutation and cursor progress")
{
    ::ceph::buffer::list bytes;
    ::ceph::encode(std::uint32_t {19}, bytes);
    immobile_record record {1, 2};
    auto input = bytes.cbegin();
    CHECK_FALSE(datum::try_decode(record, input));
    CHECK(record.first == 19);
    CHECK(record.second == 2);
    CHECK(input.end());
    CHECK_FALSE(datum::try_decode(record, bytes));
}

TEST_CASE("Ceph fallible decoding propagates allocation and other decoder exceptions")
{
    const ::ceph::buffer::list bytes;
    exceptional_record value {true};
    CHECK_THROWS_AS(datum::try_decode(value, bytes), std::bad_alloc);
    auto input = bytes.cbegin();
    CHECK_THROWS_AS(datum::try_decode(value, input), std::bad_alloc);
    value.allocation = false;
    CHECK_THROWS_AS(datum::try_decode(value, bytes), std::runtime_error);
    CHECK_THROWS_AS(datum::try_decode(value, input), std::runtime_error);
}

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
