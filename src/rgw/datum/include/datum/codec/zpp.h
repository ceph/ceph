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
        static constexpr auto members = std::tuple {Member...};
        template for (constexpr auto member : members) {
            if (const auto result = serialize_field(archive, value.*member);
                zpp::bits::failure(result)) {
                return result;
            }
        }

        return serialization_detail::success;
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

} // ceph::salad::datum::serialization_detail

namespace ceph::salad::datum::render {

using binary_buffer = std::vector<std::uint8_t>;
using binary_view = std::span<const std::uint8_t>;

template <typename BufferT>
concept binary_output_buffer =
    byte_block<BufferT> && zpp::bits::concepts::byte_type<std::ranges::range_value_t<BufferT>> &&
    requires (BufferT& buffer, const std::size_t size) {
        buffer.clear();
        buffer.resize(size);
    };

// Construction requirements for the value-returning convenience API. Codec
// enrollment and successful representation decoding remain separate properties.
template <typename T>
concept candidate_value = std::default_initializable<T> && std::move_constructible<T> &&
                          !std::is_const_v<T> && !std::is_volatile_v<T>;

// Serializable is an explicit enrolled schema property, not merely the ability
// of the underlying codec to inspect an aggregate:
template <typename T>
concept serializable = requires(const std::remove_cvref_t<T>& value) { serialize(value); };

template <typename T, binary_output_buffer BufferT>
constexpr void to_bytes(const T& value, BufferT& out_data)
try {
    out_data.clear();

    zpp::bits::out out(out_data);
    out(value).or_throw();
} catch (const std::system_error& e) {
    throw std::runtime_error {e.what()};
}

// Fixed initialized storage avoids resize/zero-fill. Overflow throws and may
// leave a written prefix; no bytes outside the span are touched.
template <typename T, byte_value ByteT, std::size_t Extent>
requires (!std::is_const_v<ByteT>) && zpp::bits::concepts::byte_type<ByteT>
constexpr std::span<ByteT> to_bytes(const T& value, std::span<ByteT, Extent> storage)
try {
    zpp::bits::out out(storage);
    out(value).or_throw();

    return storage.first(out.position());
} catch (const std::system_error& e) {
    throw std::runtime_error {e.what()};
}

// zpp requires contiguous storage. Fragment adaptation deliberately exposes
// caller staging and its extra copy rather than hiding temporary allocation.
template <typename T, byte_fragments FragmentsT, byte_value ByteT, std::size_t Extent>
requires (!std::is_const_v<ByteT>) && zpp::bits::concepts::byte_type<ByteT>
constexpr std::size_t to_bytes(const T& value, FragmentsT&& destination,
                               std::span<ByteT, Extent> staging)
{
    return scatter_bytes(to_bytes(value, staging), std::forward<FragmentsT>(destination));
}

template <typename T>
constexpr binary_buffer to_bytes(const T& value)
{
    binary_buffer out_data;
    to_bytes(value, out_data);

    return out_data;
}

template <binary_schema_type SchemaT, binary_output_buffer BufferT>
constexpr void to_bytes(const typename SchemaT::record_type& value, BufferT& output)
{
    const auto selected = serialization_detail::bind_schema<SchemaT>(value);
    to_bytes(selected, output);
}

template <binary_schema_type SchemaT, byte_value ByteT, std::size_t Extent>
requires (!std::is_const_v<ByteT>) && zpp::bits::concepts::byte_type<ByteT>
constexpr std::span<ByteT> to_bytes(const typename SchemaT::record_type& value,
                                    std::span<ByteT, Extent> storage)
{
    const auto selected = serialization_detail::bind_schema<SchemaT>(value);

    return to_bytes(selected, storage);
}

template <binary_schema_type SchemaT, byte_fragments FragmentsT, byte_value ByteT,
          std::size_t Extent>
requires (!std::is_const_v<ByteT>) && zpp::bits::concepts::byte_type<ByteT>
constexpr std::size_t to_bytes(const typename SchemaT::record_type& value, FragmentsT&& destination,
                               std::span<ByteT, Extent> staging)
{
    return scatter_bytes(to_bytes<SchemaT>(value, staging), std::forward<FragmentsT>(destination));
}

template <binary_schema_type SchemaT>
constexpr binary_buffer to_bytes(const typename SchemaT::record_type& value)
{
    binary_buffer output;
    to_bytes<SchemaT>(value, output);

    return output;
}

template <typename T, zpp::bits::concepts::byte_type ByteT, std::size_t Extent>
constexpr void from_bytes(const std::span<ByteT, Extent> bytes, T& value)
try {
    zpp::bits::in in(bytes);
    in(value).or_throw();

    if (std::size(bytes) != in.position()) [[unlikely]] {
        throw std::runtime_error {"binary input contains trailing data"};
    }
} catch (const std::system_error& e) {
    throw std::runtime_error {e.what()};
}

template <typename T>
constexpr void from_bytes(binary_view bytes, T& value)
{
    from_bytes<T, const std::uint8_t, std::dynamic_extent>(bytes, value);
}

template <binary_schema_type SchemaT, std::ranges::contiguous_range BytesT>
requires std::ranges::sized_range<BytesT> &&
         zpp::bits::concepts::byte_type<std::ranges::range_value_t<BytesT>>
constexpr void from_bytes(const BytesT& bytes, typename SchemaT::record_type& value)
{
    auto selected = serialization_detail::bind_schema<SchemaT>(value);
    from_bytes(std::span {bytes}, selected);
}

// Fresh records retain ordinary defaults for omitted fields. Decoding into an
// existing record changes only selected fields, with the existing prefix effects.
template <typename T, zpp::bits::concepts::byte_type ByteT, std::size_t Extent>
requires candidate_value<serialization_detail::schema_value_t<T>>
constexpr serialization_detail::schema_value_t<T> from_bytes(std::span<ByteT, Extent> bytes)
{
    serialization_detail::schema_value_t<T> value {};
    from_bytes<T>(bytes, value);

    return value;
}

template <typename T>
requires candidate_value<serialization_detail::schema_value_t<T>>
constexpr serialization_detail::schema_value_t<T> from_bytes(const binary_view bytes)
{
    return from_bytes<T, const std::uint8_t, std::dynamic_extent>(bytes);
}

// The codec limit is per resized container, not a total object-memory budget.
// Byte order follows the same selected schema or enrollment used by to_bytes().
// A candidate is returned only after complete representation decoding.
template <typename T, std::size_t MaximumContainerBytes, zpp::bits::concepts::byte_type ByteT,
          std::size_t Extent>
requires candidate_value<serialization_detail::schema_value_t<T>>
constexpr serialization_detail::schema_value_t<T> decode_bounded(std::span<ByteT, Extent> bytes,
                                                                 std::size_t maximum_input_bytes)
try {
    if (std::size(bytes) > maximum_input_bytes) {
        throw std::length_error {"binary input exceeds its byte limit"};
    }
    serialization_detail::schema_value_t<T> candidate {};
    zpp::bits::in in(bytes, zpp::bits::alloc_limit<MaximumContainerBytes> {});
    if constexpr (binary_schema_type<T>) {
        auto selected = serialization_detail::bind_schema<T>(candidate);
        in(selected).or_throw();
    }

    if constexpr (!binary_schema_type<T>) {
        in(candidate).or_throw();
    }
    if (std::size(bytes) != in.position()) {
        throw std::runtime_error {"binary input contains trailing data"};
    }

    return candidate;
} catch (const std::system_error& e) {
    throw std::runtime_error {e.what()};
}

template <typename T, std::size_t MaximumContainerBytes>
requires candidate_value<serialization_detail::schema_value_t<T>>
constexpr serialization_detail::schema_value_t<T> decode_bounded(binary_view bytes,
                                                                 std::size_t maximum_input_bytes)
{
    return decode_bounded<T, MaximumContainerBytes, const std::uint8_t, std::dynamic_extent>(
        bytes, maximum_input_bytes);
}

template <typename T, byte_fragments FragmentsT, zpp::bits::concepts::byte_type ByteT,
          std::size_t Extent>
requires (!std::is_const_v<ByteT>) && candidate_value<serialization_detail::schema_value_t<T>>
constexpr serialization_detail::schema_value_t<T> from_bytes(const FragmentsT& source,
                                                             std::span<ByteT, Extent> staging)
{
    return from_bytes<T>(gather_bytes(source, staging));
}

} // ceph::salad::datum::render

#endif // CEPH_RGW_DATUM_CODEC_ZPP_H
