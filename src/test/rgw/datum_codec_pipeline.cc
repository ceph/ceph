// Preserve the native string/codec/display constexpr contract in temporary C++23 too.
#include <datum/record.h>
#include <datum/codec/zpp.h>
#include <datum/render/display.h>
#include <datum/render/record.h>

#include <cstdint>
#include <type_traits>

#include <iterator>
#include <string>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;

namespace codec_pipeline_test {

#if defined(__cpp_lib_reflection)
struct record
{
    std::string name;
    std::uint64_t version;
};
#else
 #define PIPELINE_FIELDS(Field) Field(std::string, name) Field(std::uint64_t, version)
 DATUM_RECORD(record, PIPELINE_FIELDS);
 #undef PIPELINE_FIELDS
#endif

} // codec_pipeline_test

namespace ceph::salad::datum {

template <>
struct binary_schema<codec_pipeline_test::record>
    : checked_binary_schema<codec_pipeline_test::record,
                            &codec_pipeline_test::record::name,
                            &codec_pipeline_test::record::version>
{};

} // ceph::salad::datum

namespace ceph::salad::datum::render {

template <>
struct display_schema<codec_pipeline_test::record>
#if defined(__cpp_lib_reflection)
    : reflected_display_schema<^^codec_pipeline_test::record, 2>
#else
    : declared_display_schema<codec_pipeline_test::record, 2>
#endif
{
    static constexpr std::string_view name = "record";
};

} // ceph::salad::datum::render

namespace codec_pipeline_test {

template <typename T>
requires datum::binary_record<T>
auto serialize(const T&) -> zpp::bits::members<datum::binary_member_count<T>>;

template <typename ArchiveT, typename T>
requires datum::binary_record<T>
constexpr auto serialize(ArchiveT& archive, T& value)
{
    return datum::serialize_schema(
        archive, value, typename datum::binary_schema<std::remove_cvref_t<T>>::fields {});
}

constexpr bool round_trip_and_display()
{
    record value {"constant data", 41};
    auto bytes = datum::to_bytes(value);
    auto decoded = datum::from_bytes<record>(bytes);
    if (decoded.name != value.name || decoded.version != value.version) {
        return false;
    }
    record restored {};
    datum::to_bytes(value, bytes);
    datum::from_bytes(bytes, restored);
    std::string text;
    datum::render::format_to(std::back_inserter(text), restored);

    return restored.name == value.name and restored.version == value.version and
           text == "record{name=\"constant data\", version=41}";
}

static_assert(round_trip_and_display());

} // codec_pipeline_test

TEST_CASE("codec and diagnostics preserve the constexpr owning-string pipeline", "[datum][codec]")
{
    CHECK(codec_pipeline_test::round_trip_and_display());
}
