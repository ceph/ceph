// Explicit schema codecs retain byte order, prefix effects and complete-message checks.
#include <datum/codec/zpp.h>
#include <datum/record_definition.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <string>
#include <system_error>
#include <vector>

namespace datum = ::ceph::salad::datum;
namespace codec_test {

struct bad_field { int value; };
#define ROW_FIELDS(Field) \
    Field(std::uint32_t, id) \
    Field(std::uint16_t, revision) \
    Field(std::string, payload)
#define SCALAR_FIELDS(Field) Field(std::uint32_t, first) Field(std::uint32_t, last)
#define FAILING_FIELDS(Field) \
    Field(std::uint32_t, first) \
    Field(bad_field, bad) \
    Field(std::uint32_t, last)
#define EMPTY_FIELDS(Field)
#define LOCAL_ENTRY_FIELDS(Field) \
    Field(std::string, key) \
    Field(std::uint64_t, version, 99) \
    Field(std::string, value) \
    Field(std::function<void()>, on_change)
#if defined(__cpp_lib_reflection)
struct row { std::uint32_t id; std::uint16_t revision; std::string payload; };
struct scalar { std::uint32_t first; std::uint32_t last; };
struct failing { std::uint32_t first; bad_field bad; std::uint32_t last; };
struct empty_row {};
struct local_entry
{
    std::string key;
    std::uint64_t version = 99;
    std::string value;
    std::function<void()> on_change;
};
#else
DATUM_RECORD(row, ROW_FIELDS);
DATUM_RECORD(scalar, SCALAR_FIELDS);
DATUM_RECORD(failing, FAILING_FIELDS);
DATUM_RECORD(empty_row, EMPTY_FIELDS);
DATUM_RECORD(local_entry, LOCAL_ENTRY_FIELDS);
#endif
#undef ROW_FIELDS
#undef SCALAR_FIELDS
#undef FAILING_FIELDS
#undef EMPTY_FIELDS
#undef LOCAL_ENTRY_FIELDS
inline unsigned bad_calls = 0;
inline bool throw_bad = false;

bool equal(const row& left, const row& right)
{
    return left.id == right.id and left.revision == right.revision and left.payload == right.payload;
}

} // codec_test

namespace ceph::salad::datum {

template <>
struct binary_schema<codec_test::row>
    : checked_binary_schema<codec_test::row, &codec_test::row::payload,
                            &codec_test::row::revision, &codec_test::row::id>
{};
template <>
struct binary_field_codec<codec_test::bad_field>
{
    static zpp::bits::errc apply(auto&, auto&)
    {
        ++codec_test::bad_calls;
        if (codec_test::throw_bad) {
            throw std::system_error {std::make_error_code(std::errc::permission_denied)};
        }
        return std::errc::permission_denied;
    }
};

} // ceph::salad::datum

namespace codec_test {

template <typename T>
requires datum::binary_record<T>
auto serialize(const T&) -> zpp::bits::members<datum::binary_member_count<T>>;

template <typename ArchiveT, typename T>
requires datum::binary_record<T>
constexpr auto serialize(ArchiveT& archive, T& value)
{
    return datum::serialize_schema(archive, value, typename datum::binary_schema<std::remove_cvref_t<T>>::fields {});
}

using wire = datum::binary_schema_t<row>;
using scalar_wire = datum::binary_schema_t<scalar>;
static_assert(datum::serializable<row>);
static_assert(!datum::serializable<scalar>);
static_assert(std::same_as<decltype(datum::checked_members<2>(std::declval<const scalar&>())), zpp::bits::members<2>>);
static_assert([] {
    const scalar source {17, 23};
    std::array<std::uint8_t, 16> storage {};
    const auto bytes = datum::to_bytes<scalar_wire>(source, storage);
    const auto decoded = datum::decode_bounded<scalar_wire, 16>(bytes, 16);
    return bytes.size() == 8 and decoded.first == 17 and decoded.last == 23;
}());

struct archive
{
    std::vector<std::uint32_t>& observed;
    zpp::bits::errc operator()(std::uint32_t value) { observed.push_back(value); return {}; }
};

} // codec_test

#include <catch2/catch_test_macros.hpp>

TEST_CASE("schema codecs retain explicit and enrolled wire bytes", "[datum][codec]")
{
    using namespace codec_test;
    const row source {42, 7, "payload"};
    datum::binary_buffer expected;
    zpp::bits::out output {expected};
    output(source.id, source.revision, source.payload).or_throw();
    const auto bytes = datum::to_bytes<wire>(source);
    CHECK(bytes == expected);
    CHECK(equal(datum::from_bytes<wire>(bytes), source));
    expected.clear();
    output.reset();
    output(source.payload, source.revision, source.id).or_throw();
    CHECK(datum::to_bytes(source) == expected);
    CHECK(equal(datum::from_bytes<row>(expected), source));
    CHECK(expected != bytes);
}

TEST_CASE("schema codec storage adapters preserve caller storage and input limits", "[datum][codec]")
{
    using namespace codec_test;
    const row source {42, 7, std::string(104, 'x')};
    const auto expected = datum::to_bytes<wire>(source);
    std::array<std::uint8_t, 512> storage;
    storage.fill(0xAA);
    const auto written = datum::to_bytes<wire>(source, storage);
    CHECK(std::ranges::equal(written, expected));
    CHECK(0xAA == storage[written.size()]);
    datum::binary_buffer reusable {1, 2, 3};
    datum::to_bytes<wire>(source, reusable);
    CHECK(reusable == expected);
    std::array<std::uint8_t, 512> staging {};
    std::array<std::uint8_t, 512> scattered {};
    const std::array fragments {std::span {scattered}.first(3),
        std::span {scattered}.subspan(3, expected.size() - 3)};
    CHECK(expected.size() == datum::to_bytes<wire>(source, fragments, staging));
    CHECK(equal(datum::from_bytes<wire>(fragments, staging), source));
    CHECK((equal(datum::decode_bounded<wire, 128>(expected, 512), source)));
    CHECK_THROWS_AS((datum::decode_bounded<wire, 16>(expected, 512)), std::runtime_error);
    CHECK_THROWS_AS((datum::decode_bounded<wire, 128>(expected, 1)), std::length_error);
    storage.fill(0xAA);
    CHECK_THROWS_AS(datum::to_bytes<wire>(source, std::span {storage}.first(3)), std::runtime_error);
    CHECK(0xAA == storage[3]);
}

TEST_CASE("schema codecs reject incomplete messages and retain documented prefix effects", "[datum][codec]")
{
    using namespace codec_test;
    const row source {42, 7, "payload"};
    const auto bytes = datum::to_bytes<wire>(source);
    auto damaged = bytes;
    damaged.push_back(0);
    CHECK_THROWS_AS(datum::from_bytes<wire>(damaged), std::runtime_error);
    damaged = bytes;
    damaged.pop_back();
    CHECK_THROWS_AS(datum::from_bytes<wire>(damaged), std::runtime_error);
    row target {1, 99, "old"};
    CHECK_THROWS_AS(datum::from_bytes<wire>(std::span {bytes}.first(4), target), std::runtime_error);
    CHECK(42 == target.id);
    CHECK(99 == target.revision);
    CHECK("old" == target.payload);
}

TEST_CASE("codec field errors stop before later fields and preserve explicit repeated profiles",
          "[datum][codec]")
{
    using namespace codec_test;
    std::vector<std::uint32_t> observed;
    archive output {observed};
    const failing source {11, {0}, 29};
    using failed_wire = datum::binary_fields<&failing::first, &failing::bad, &failing::last>;
    bad_calls = 0;
    throw_bad = false;
    const auto result = datum::serialize_schema(output, source, failed_wire {});
    CHECK(std::errc::permission_denied == static_cast<std::errc>(result));
    CHECK(observed == std::vector<std::uint32_t> {11});
    CHECK(1 == bad_calls);
    observed.clear();
    throw_bad = true;
    CHECK_THROWS_AS(datum::serialize_schema(output, source, failed_wire {}), std::system_error);
    throw_bad = false;
    CHECK(observed == std::vector<std::uint32_t> {11});
    CHECK(2 == bad_calls);
    observed.clear();
    const scalar repeated {17, 23};
    using repeated_wire = datum::binary_fields<&scalar::first, &scalar::first>;
    static_assert(!datum::binary_schema_type<repeated_wire>);
    datum::serialize_schema(output, repeated, repeated_wire {}).or_throw();
    CHECK(observed == std::vector<std::uint32_t> {17, 17});
    using empty_wire = datum::binary_schema_t<empty_row>;
    const auto empty = datum::to_bytes<empty_wire>(empty_row {});
    CHECK(empty.empty());
    CHECK_NOTHROW(datum::from_bytes<empty_wire>(empty));
}

TEST_CASE("selected wire fields preserve local defaults and existing callbacks", "[datum][codec]")
{
    using codec_test::local_entry;
    using wire = datum::binary_fields<&local_entry::value, &local_entry::key>;
    unsigned calls = 0;
    const local_entry source {"key", 7, "payload", [&calls] { ++calls; }};
    datum::binary_buffer expected;
    zpp::bits::out output {expected};
    output(source.value, source.key).or_throw();
    const auto bytes = datum::to_bytes<wire>(source);
    CHECK(bytes == expected);
    const auto candidate = datum::from_bytes<wire>(bytes);
    CHECK(candidate.key == source.key);
    CHECK(candidate.value == source.value);
    CHECK(99 == candidate.version);
    CHECK_FALSE(candidate.on_change);
    CHECK(0 == calls);

    local_entry target {"old", 42, "old", [&calls] { ++calls; }};
    datum::from_bytes<wire>(bytes, target);
    CHECK(target.key == source.key);
    CHECK(target.value == source.value);
    CHECK(42 == target.version);
    target.on_change();
    CHECK(1 == calls);
}

namespace codec_range_test {

struct storage final
{
    std::array<unsigned char, 8> bytes {};
};
auto begin(storage& value) { return value.bytes.data(); }
auto end(storage& value) { return value.bytes.data() + value.bytes.size(); }
auto begin(const storage& value) { return value.bytes.data(); }
auto end(const storage& value) { return value.bytes.data() + value.bytes.size(); }

template <typename StorageT>
concept can_encode = requires (const codec_test::scalar& value, StorageT&& bytes) {
    datum::to_bytes<codec_test::scalar_wire>(value, std::forward<StorageT>(bytes));
};
static_assert(can_encode<storage&>);
static_assert(!can_encode<storage>);
static_assert(!can_encode<const storage&>);
static_assert(!can_encode<std::span<const char>>);

template <typename FragmentsT>
concept can_encode_fragments = requires (const codec_test::scalar& value, FragmentsT&& output, storage& staging) {
    datum::to_bytes<codec_test::scalar_wire>(value, std::forward<FragmentsT>(output), staging);
    datum::to_bytes(value, std::forward<FragmentsT>(output), staging);
};

using writable_fragments = std::array<std::span<char>, 2>;
using readonly_fragments = std::array<std::span<const char>, 2>;
static_assert(can_encode_fragments<writable_fragments&>);
static_assert(can_encode_fragments<const writable_fragments&>);
static_assert(!can_encode_fragments<readonly_fragments&>);
static_assert(!can_encode_fragments<const readonly_fragments&>);
static_assert(!can_encode_fragments<const std::array<std::array<char, 4>, 2>&>);

} // codec_range_test

TEST_CASE("contiguous codec ranges preserve output ownership", "[datum][codec]")
{
    using namespace codec_test;
    const scalar source {17, 23};
    codec_range_test::storage storage;
    const auto written = datum::to_bytes<scalar_wire>(source, storage);
    CHECK(written.data() == storage.bytes.data());
    CHECK(written.size() == 8);
    const auto& input = storage;
    auto decoded = datum::from_bytes<scalar_wire>(input);
    CHECK(decoded.first == 17);
    CHECK(decoded.last == 23);
    decoded = {};
    datum::from_bytes<scalar_wire>(input, decoded);
    CHECK(decoded.first == 17);
    CHECK(decoded.last == 23);
    const auto bounded = datum::decode_bounded<scalar_wire, 16>(input, 8);
    CHECK(bounded.first == 17);
    CHECK(bounded.last == 23);
    CHECK_THROWS_AS((datum::decode_bounded<scalar_wire, 16>(input, 7)), std::length_error);

    std::vector<unsigned char> growable(1);
    datum::to_bytes<scalar_wire>(source, growable);
    CHECK(std::ranges::equal(growable, written));
    CHECK(datum::from_bytes<scalar_wire>(std::move(growable)).last == 23);

    std::array<unsigned char, 8> scattered {};
    const std::array fragments {std::span {scattered}.first(3), std::span {scattered}.subspan(3)};
    std::vector<unsigned char> staging(12, 0xAA);
    CHECK(datum::to_bytes<scalar_wire>(source, fragments, staging) == 8);
    CHECK(staging.size() == 12);
    CHECK(staging[8] == 0xAA);
    CHECK(datum::from_bytes<scalar_wire>(fragments, staging).first == 17);
    CHECK(staging.size() == 12);
    CHECK(staging[8] == 0xAA);
}
