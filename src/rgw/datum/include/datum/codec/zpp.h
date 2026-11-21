// Datum: Explicit zpp codec adaptation and byte convenience endpoints.
// An optional codec layer above reflected schema facts.
//
// Please keep this layer independent of native buffers, record enrollment and application
// field representations.
// Put domain-specific behavior in consumer profiles or adapters.

#ifndef CEPH_RGW_DATUM_CODEC_ZPP_H
 #define CEPH_RGW_DATUM_CODEC_ZPP_H

#include "datum/bytes.h"
#include "datum/record.h"
#include "zpp_bits.h"

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include <stdexcept>
#include <system_error>
#include <tuple>
#include <utility>

#include <span>
#include <vector>

#include <iterator>
#include <ranges>

namespace ceph::salad::datum::serialization_detail {

template <typename ArchiveT>
inline constexpr bool output_archive =
    std::remove_cvref_t<ArchiveT>::kind() == zpp::bits::kind::out;

inline constexpr zpp::bits::errc success {};

// The optional codec supports a subset of Datum's byte spellings.
template <typename T>
concept byte_range = byte_block<T> and
                     zpp::bits::concepts::byte_type<std::ranges::range_value_t<T>>;

} // ceph::salad::datum::serialization_detail

namespace ceph::salad::datum {

// zpp's C++26 member expansion does not enforce its declared member count.
// Keep the approved count explicit so a record change requires wire review.
// Its ADL member hooks are unevaluated; a checked declaration needs no body.
template <std::size_t Count, typename RecordT>
consteval auto checked_members(const RecordT&)
{
    static_assert(
        serialization_detail::valid_member_count<RecordT, Count>(),
        "Serialized record must retain its approved direct aggregate members; review wire compatibility");

    return zpp::bits::members<Count> {};
}

template <typename T>
concept binary_record = requires { typename binary_schema<std::remove_cvref_t<T>>::fields; };

template <typename T>
requires binary_record<T>
inline constexpr std::size_t binary_member_count =
    binary_schema<std::remove_cvref_t<T>>::fields::size;

// Field representation is selected explicitly by a consumer profile.
template <typename T>
struct binary_field_codec
{
    static constexpr auto apply(auto& archive, auto& field) { return archive(field); }

};

template <typename ArchiveT, typename FieldT>
constexpr auto serialize_field(ArchiveT& archive, FieldT& field)
{
    return binary_field_codec<std::remove_cvref_t<FieldT>>::apply(archive, field);
}

// The C++23 fold replaces the native C++26 expansion statement below.
// It stops at the first error and borrows the archive and source.
#if !defined(__cpp_lib_reflection)
namespace serialization_detail {

template <auto ...Member, typename ArchiveT, typename ValueT>
constexpr zpp::bits::errc serialize_members(ArchiveT& archive, ValueT& value)
{
    // Native auto return deduction requires this same concrete error type.
    static_assert((std::same_as<decltype(serialize_field(archive, value.*Member)),
                                zpp::bits::errc> and ...),
                  "A binary field codec must return zpp::bits::errc");
    auto result = success;
    (void)((!zpp::bits::failure(result = serialize_field(archive, value.*Member))) and ...);

    return result;
}

} // serialization_detail
#endif

template <typename ArchiveT, typename ValueT, auto ...Member>
constexpr auto serialize_schema(ArchiveT& archive, ValueT& value, binary_fields<Member...>)
{
    constexpr bool inspecting =
        std::same_as<std::remove_cvref_t<ArchiveT>,
                     zpp::bits::traits::visitor<std::remove_cvref_t<ValueT>>>;
    // Type inspection must recognize the explicit wire schema too, rather
    // than trying to decompose private members of nested chrono values:
    if constexpr (inspecting) {
        return archive(value.*Member...);
    }

    if constexpr (!inspecting) {
#if defined(__cpp_lib_reflection)
        static constexpr auto members = std::tuple {Member...};
        template for (constexpr auto member : members) {
            if (const auto result = serialize_field(archive, value.*member);
                zpp::bits::failure(result)) {
                return result;
            }
        }

        return serialization_detail::success;
#else
        return serialization_detail::serialize_members<Member...>(archive, value);
#endif
    }
}

} // ceph::salad::datum

namespace ceph::salad::datum::serialization_detail {

template <binary_schema_type SchemaT, typename ValueT>
struct schema_reference final
{
    ValueT& value;
};

template <binary_schema_type SchemaT, typename ValueT>
auto serialize(const schema_reference<SchemaT, ValueT>&)
    -> zpp::bits::members<SchemaT::fields::size>;

template <typename ArchiveT, binary_schema_type SchemaT, typename ValueT>
constexpr auto serialize(ArchiveT& archive, const schema_reference<SchemaT, ValueT>& source)
{
    constexpr bool inspecting =
        std::same_as<std::remove_cvref_t<ArchiveT>,
                     zpp::bits::traits::visitor<schema_reference<SchemaT, ValueT>>>;
    if constexpr (inspecting) {
        return [&archive, &source]<auto ...Member>(binary_fields<Member...>) {
            return archive(source.value.*Member...);
        }(typename SchemaT::fields {});
    }

    if constexpr (!inspecting) {
        return serialize_schema(archive, source.value, typename SchemaT::fields {});
    }
}

template <binary_schema_type SchemaT, typename ValueT>
constexpr auto bind_schema(ValueT& value)
{
    return schema_reference<SchemaT, ValueT> {value};
}

template <typename T>
struct schema_value
{
    using type = T;
};

template <binary_schema_type SchemaT>
struct schema_value<SchemaT>
{
    using type = typename SchemaT::record_type;
};

template <typename T>
using schema_value_t = typename schema_value<T>::type;

// Complete-message endpoints share the codec's error code and reject trailing bytes.
template <typename ArchiveT, typename ValueT>
constexpr void decode_complete(ArchiveT& archive, ValueT& value, const std::size_t size)
{
    archive(value).or_throw();
    if (size != archive.position()) [[unlikely]] {
        throw std::system_error {std::make_error_code(std::errc::bad_message),
                                 "binary input contains trailing data"};
    }
}

} // ceph::salad::datum::serialization_detail

namespace ceph::salad::datum {

using binary_buffer = std::vector<std::uint8_t>;
using binary_view = std::span<const std::uint8_t>;

template <typename BufferT>
concept binary_output_buffer =
    serialization_detail::byte_range<BufferT> and
    requires (BufferT& buffer, const std::size_t size) {
        buffer.clear();
        buffer.resize(size);
    };

// Construction requirements for the value-returning convenience API. Codec
// enrollment and successful representation decoding remain separate properties.
template <typename T>
concept candidate_value = std::default_initializable<T> and std::move_constructible<T> and
                          !std::is_const_v<T> and !std::is_volatile_v<T>;

// Serializable is an explicit enrolled schema property, not merely the ability
// of the underlying codec to inspect an aggregate:
template <typename T>
concept serializable = requires(const std::remove_cvref_t<T>& value) { serialize(value); };

template <typename T, binary_output_buffer BufferT>
constexpr void to_bytes(const T& value, BufferT& out_data)
{
    out_data.clear();

    zpp::bits::out out(out_data);
    out(value).or_throw();
}

// Fixed initialized storage avoids resize/zero-fill. Overflow throws and may
// leave a written prefix; no bytes outside the span are touched.
template <typename T, writable_byte_output StorageT>
requires (!binary_output_buffer<StorageT>) and serialization_detail::byte_range<StorageT>
constexpr auto to_bytes(const T& value, StorageT&& storage)
{
    auto destination = std::span {storage};
    zpp::bits::out out(destination);
    out(value).or_throw();

    return destination.first(out.position());
}

// zpp requires contiguous storage. Fragment adaptation deliberately exposes
// caller staging and its extra copy rather than hiding temporary allocation.
template <typename T, writable_byte_fragments FragmentsT, writable_byte_output StorageT>
requires serialization_detail::byte_range<StorageT>
constexpr std::size_t to_bytes(const T& value, FragmentsT&& destination, StorageT&& staging)
{
    return ceph::salad::datum::scatter_bytes(
        ceph::salad::datum::to_bytes(value, std::span {staging}),
        std::forward<FragmentsT>(destination));
}

template <typename T>
constexpr binary_buffer to_bytes(const T& value)
{
    binary_buffer out_data;
    ceph::salad::datum::to_bytes(value, out_data);

    return out_data;
}

template <binary_schema_type SchemaT, binary_output_buffer BufferT>
constexpr void to_bytes(const typename SchemaT::record_type& value, BufferT& output)
{
    const auto selected = serialization_detail::bind_schema<SchemaT>(value);
    ceph::salad::datum::to_bytes(selected, output);
}

template <binary_schema_type SchemaT, writable_byte_output StorageT>
requires (!binary_output_buffer<StorageT>) and serialization_detail::byte_range<StorageT>
constexpr auto to_bytes(const typename SchemaT::record_type& value, StorageT&& storage)
{
    const auto selected = serialization_detail::bind_schema<SchemaT>(value);

    return ceph::salad::datum::to_bytes(selected, std::forward<StorageT>(storage));
}

template <binary_schema_type SchemaT, writable_byte_fragments FragmentsT,
          writable_byte_output StorageT>
requires serialization_detail::byte_range<StorageT>
constexpr std::size_t to_bytes(const typename SchemaT::record_type& value, FragmentsT&& destination, StorageT&& staging)
{
    const auto selected = serialization_detail::bind_schema<SchemaT>(value);

    return ceph::salad::datum::to_bytes(selected, std::forward<FragmentsT>(destination),
                                       std::forward<StorageT>(staging));
}

template <binary_schema_type SchemaT>
constexpr binary_buffer to_bytes(const typename SchemaT::record_type& value)
{
    binary_buffer output;
    ceph::salad::datum::to_bytes<SchemaT>(value, output);

    return output;
}

template <typename T, serialization_detail::byte_range BytesT>
constexpr void from_bytes(BytesT&& bytes, T& value)
{
    auto source = std::span {bytes};
    zpp::bits::in in(source);
    serialization_detail::decode_complete(in, value, source.size());
}

template <binary_schema_type SchemaT, serialization_detail::byte_range BytesT>
constexpr void from_bytes(BytesT&& bytes, typename SchemaT::record_type& value)
{
    auto selected = serialization_detail::bind_schema<SchemaT>(value);
    ceph::salad::datum::from_bytes(std::forward<BytesT>(bytes), selected);
}

// Fresh records retain ordinary defaults for omitted fields. Decoding into an
// existing record changes only selected fields, with the existing prefix effects.
template <typename T, serialization_detail::byte_range BytesT>
requires candidate_value<serialization_detail::schema_value_t<T>>
constexpr serialization_detail::schema_value_t<T> from_bytes(BytesT&& bytes)
{
    serialization_detail::schema_value_t<T> value {};
    ceph::salad::datum::from_bytes<T>(std::forward<BytesT>(bytes), value);

    return value;
}

// The codec limit is per resized container, not a total object-memory budget.
// Byte order follows the same selected schema or enrollment used by to_bytes().
// A candidate is returned only after complete representation decoding.
template <typename T, std::size_t MaximumContainerBytes, serialization_detail::byte_range BytesT>
requires candidate_value<serialization_detail::schema_value_t<T>>
constexpr serialization_detail::schema_value_t<T> decode_bounded(BytesT&& bytes, std::size_t maximum_input_bytes)
{
    if (std::ranges::size(bytes) > maximum_input_bytes) {
        throw std::length_error {"binary input exceeds its byte limit"};
    }
    serialization_detail::schema_value_t<T> candidate {};
    auto source = std::span {bytes};
    zpp::bits::in in(source, zpp::bits::alloc_limit<MaximumContainerBytes> {});
    if constexpr (binary_schema_type<T>) {
        auto selected = serialization_detail::bind_schema<T>(candidate);
        serialization_detail::decode_complete(in, selected, source.size());
    }

    if constexpr (!binary_schema_type<T>) {
        serialization_detail::decode_complete(in, candidate, source.size());
    }

    return candidate;
}

template <typename T, byte_fragments FragmentsT, writable_byte_output StorageT>
requires serialization_detail::byte_range<StorageT> and
         candidate_value<serialization_detail::schema_value_t<T>>
constexpr serialization_detail::schema_value_t<T> from_bytes(FragmentsT&& source, StorageT&& staging)
{
    return ceph::salad::datum::from_bytes<T>(
        ceph::salad::datum::copy(std::forward<FragmentsT>(source), staging));
}

} // ceph::salad::datum

#endif // CEPH_RGW_DATUM_CODEC_ZPP_H
