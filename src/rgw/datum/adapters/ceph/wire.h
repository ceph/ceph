// Ceph native framing and metadata adapters over Datum/zpp.
// This consumer adapter requires C++26; native buffers stay outside Datum's pure core.
#ifndef CEPH_COMMON_DATUM_WIRE_H
 #define CEPH_COMMON_DATUM_WIRE_H

#include "buffer_cursor.h"
#include "common/container_concepts.h"
#include "datum/codec/zpp.h"
#include "include/encoding.h"
#include "include/compat.h"

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

#include <algorithm>
#include <array>
#include <functional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace ceph::datum_wire {

namespace datum = ::ceph::salad::datum;
using byte_buffer = ::ceph::buffer::list;

constexpr std::uint32_t checked_count(const std::uint64_t size)
{
    if (size > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error {"Ceph wire length exceeds uint32"};
    }

    return static_cast<std::uint32_t>(size);
}

template <typename T>
std::uint8_t checked_byte(const T value)
{
    if constexpr (std::is_enum_v<T>) {
        return checked_byte(std::to_underlying(value));
    }

    if constexpr (!std::is_enum_v<T>) {
        if (!std::in_range<std::uint8_t>(value)) {
            throw std::length_error {"Ceph wire value exceeds uint8"};
        }

        return static_cast<std::uint8_t>(value);
    }
}

template <typename T>
struct scalar_record final
{
    T value;
};

using frame_header = ::ceph::encoding_detail::struct_header;
using header_fields =
    datum::binary_fields<&frame_header::v, &frame_header::compat, &frame_header::len>;

inline constexpr auto growing_metadata = std::true_type {};

// Native timestamps encode seconds and nanoseconds as uint32 fields.
inline constexpr auto encoded_time_size = 2 * sizeof(std::uint32_t);

// Keep success local when the archive's throwing helper is outlined.
[[gnu::always_inline]] inline void throw_if_error(const zpp::bits::errc result)
{
    if (zpp::bits::failure(result)) [[unlikely]] {
        result.or_throw();
    }
}

// Observe the selected field codecs using the same size4b metadata contract.
// Other representations can retain the explicit-size writer overload.
namespace detail {

struct metadata_size final
{
    std::uint64_t bytes = 0;

    static constexpr auto kind() { return zpp::bits::kind::out; }

    static constexpr std::uint64_t field_size(const std::unsigned_integral auto& value)
    {
        return sizeof(value);
    }

    template <typename T>
    requires std::same_as<T, std::string_view> || std::same_as<T, std::string>
    static constexpr std::uint64_t field_size(const T& value)
    {
        return sizeof(std::uint32_t) + static_cast<std::uint64_t>(checked_count(value.size()));
    }

    constexpr zpp::bits::errc operator()(const auto& ...values)
    {
        ((bytes += field_size(values)), ...);

        return {};
    }

};

} // detail

template <typename RecordT, auto ...Member>
constexpr std::uint64_t encoded_size(datum::binary_fields<Member...> selected, const RecordT& value)
{
    detail::metadata_size archive;
    const auto result = datum::serialize_schema(archive, value, selected);
    if (zpp::bits::failure(result)) {
        result.or_throw();
    }

    return archive.bytes;
}

static_assert(encoded_size(header_fields {}, frame_header {}) ==
              ::ceph::encoding_detail::struct_header_len());

class writer final
{
    byte_buffer& output;

private:
    void copy_payload(const byte_buffer& value, const unsigned offset, const unsigned size)
    {
        ensure(size);
        auto storage CEPH_INDETERMINATE = output.get_contiguous_appender(size);
        auto first = value.cbegin(offset);
        first.copy(size, storage.get_pos_add(size));
    }

public:
    explicit writer(byte_buffer& output) : output(output)
    {}

public:
    void ensure(const std::uint64_t additional) const
    {
        if (additional > std::numeric_limits<std::uint32_t>::max() - output.length()) {
            throw std::length_error {"Ceph message exceeds uint32"};
        }
    }

    template <typename RecordT, auto ...Member>
    void fields(datum::binary_fields<Member...> selected, const RecordT& value,
                const std::uint64_t size)
    {
        ensure(size);
        auto storage CEPH_INDETERMINATE = output.get_contiguous_appender(size);
        std::span bytes {storage.get_pos_add(size), static_cast<std::size_t>(size)};
        zpp::bits::out archive {bytes, zpp::bits::size4b {}, zpp::bits::endian::little {}};
        throw_if_error(datum::serialize_schema(archive, value, selected));
    }

    template <typename RecordT, auto ...Member>
    void fields(datum::binary_fields<Member...> selected, const RecordT& value)
    {
        fields(selected, value, encoded_size(selected, value));
    }

    // Unframed entry streams need append growth once their first arena fills.
    template <typename RecordT, auto ...Member>
    void fields(std::true_type, datum::binary_fields<Member...> selected, const RecordT& value)
    {
        const auto size = encoded_size(selected, value);
        ensure(size);
        auto storage = output.append_hole(static_cast<unsigned>(size));
        std::span bytes {storage.c_str(), static_cast<std::size_t>(size)};
        zpp::bits::out archive {bytes, zpp::bits::size4b {}, zpp::bits::endian::little {}};
        throw_if_error(datum::serialize_schema(archive, value, selected));
    }

    template <typename T>
    void scalar(const T value)
    {
        using record = scalar_record<T>;

        fields(datum::binary_fields<&record::value> {}, record {value}, sizeof(T));
    }

    void payload(const byte_buffer& value)
    {
        ensure(value.length());
        output.append(value);
    }

    void payload(const byte_buffer& value, const unsigned offset, const unsigned size)
    {
        if (0 == size) {
            return;
        }
        if (size <= output.get_append_buffer_unused_tail_length()) {
            copy_payload(value, offset, size);

            return;
        }
        ensure(size);
        auto first = value.cbegin(offset);
        first.copy(size, output);
    }

    void text(const std::string_view value)
    {
        using record = scalar_record<std::string_view>;

        fields(datum::binary_fields<&record::value> {}, record {value});
    }

    // A native byte chain can carry a string without an owning string temporary:
    void text(const byte_buffer& value)
    {
        scalar(static_cast<std::uint32_t>(value.length()));
        ensure(value.length());
        auto storage CEPH_INDETERMINATE = output.get_contiguous_appender(value.length());
        auto target = storage.get_pos_add(value.length());
        for (const auto& fragment : value.buffers()) {
            const auto size = fragment.length();
            if (0 == size) {
                continue;
            }
            // Match native string copying without a per-fragment memcpy dispatch.
            if (1 == size) {
                *target++ = *fragment.c_str();
                continue;
            }
            std::char_traits<char>::copy(target, fragment.c_str(), size);
            target += size;
        }
    }

    // Keep established nested native codecs while borrowing their owning records:
    template <typename T>
    void native(const T& value)
    {
        using ::ceph::encode;
        encode(value, output);
    }

    template <typename BytesT>
    requires datum::byte_block<const BytesT>
    void payload(const BytesT& value)
    {
        const auto bytes = std::as_bytes(std::span {std::ranges::data(value), std::ranges::size(value)});
        ensure(bytes.size());
        output.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    }

    // Metadata-only frames need neither a streaming header nor a backpatch.
    template <typename RecordT, auto ...Member>
    void frame(datum::binary_fields<Member...> selected, const std::uint8_t version,
               const RecordT& value)
    {
        constexpr std::uint64_t header_size = ::ceph::encoding_detail::struct_header_len();
        const auto body_size = checked_count(encoded_size(selected, value));
        const auto size = header_size + body_size;
        ensure(size);
        auto storage CEPH_INDETERMINATE = output.get_contiguous_appender(size);
        std::span bytes {storage.get_pos_add(size), static_cast<std::size_t>(size)};
        zpp::bits::out archive {bytes, zpp::bits::size4b {}, zpp::bits::endian::little {}};
        const frame_header header {version, 1, body_size};
        throw_if_error(datum::serialize_schema(archive, header, header_fields {}));
        throw_if_error(datum::serialize_schema(archive, value, selected));
    }

    template <typename BodyT>
    void frame(const std::uint8_t version, const std::uint8_t compatible, BodyT&& body)
    {
        constexpr auto header_size = ::ceph::encoding_detail::struct_header_len();
        ensure(header_size);
        auto header = output.append_hole(header_size);
        const auto start = output.length();
        std::invoke(std::forward<BodyT>(body));
        const frame_header value {version, compatible, output.length() - start};
        std::span bytes {header.c_str(), header_size};
        zpp::bits::out archive {bytes, zpp::bits::endian::little {}};
        throw_if_error(datum::serialize_schema(archive, value, header_fields {}));
    }

    template <typename BodyT>
    void frame(const std::uint8_t version, BodyT&& body)
    {
        frame(version, 1, std::forward<BodyT>(body));
    }

};

// Build one owning frame from fields written by the supplied body.
template <typename BodyT>
byte_buffer encode_frame(const std::uint8_t version, const std::uint8_t compatible, BodyT&& body)
{
    byte_buffer output;
    writer archive {output};
    archive.frame(version, compatible, [&] { std::invoke(std::forward<BodyT>(body), archive); });

    return output;
}

template <typename BodyT>
byte_buffer encode_frame(const std::uint8_t version, BodyT&& body)
{
    return encode_frame(version, 1, std::forward<BodyT>(body));
}

template <typename CursorT>
class basic_reader final
{
    CursorT& input;

    // Bytes after this frame stay with the input.
    unsigned trailing_bytes = 0;

public:
    explicit basic_reader(CursorT& input)
        : input(input)
    {}

private:
    basic_reader(CursorT& input, const unsigned trailing_bytes)
        : input(input), trailing_bytes(trailing_bytes)
    {}

public:
    unsigned remaining() const noexcept { return input.get_remaining() - trailing_bytes; }

    void require(const std::size_t size) const
    {
        if (size > remaining()) {
            throw ::ceph::buffer::malformed_input {"Ceph field exceeds its frame"};
        }
    }

    // Minimum is the least encoded bytes per element across all accepted leaf versions,
    // not sizeof(T) or the current encoder's size. Reserve tail bytes for following fields.
    // This rejects impossible counts before allocation; each leaf still validates its own body.
    template <unsigned Minimum>
    std::uint32_t sequence_size(const unsigned tail = 0)
    {
        static_assert(Minimum > 0);
        const auto size = scalar<std::uint32_t>();
        require(tail);
        if (size > (remaining() - tail) / Minimum) {
            throw ::ceph::buffer::malformed_input {"Ceph count exceeds frame capacity"};
        }

        return size;
    }

    template <std::size_t Size, typename RecordT, auto ...Member>
    void fields(datum::binary_fields<Member...> selected, RecordT& value)
    {
        require(Size);
        std::array<char, Size> storage CEPH_INDETERMINATE;
        const auto view = ::ceph::buffer::read_contiguous(input, storage).template first<Size>();
        zpp::bits::in archive {view, zpp::bits::endian::little {}};
        throw_if_error(datum::serialize_schema(archive, value, selected));
    }

    template <std::unsigned_integral T>
    T scalar()
    {
        using record = scalar_record<T>;

        record value CEPH_INDETERMINATE;
        fields<sizeof(T)>(datum::binary_fields<&record::value> {}, value);

        return value.value;
    }

    void text(std::string& value)
    {
        payload(value);
    }

    void text(::ceph::buffer::ptr& value)
    {
        const auto size = scalar<std::uint32_t>();
        require(size);
        if (0 == size) {
            value = ::ceph::buffer::ptr {};

            return;
        }
        input.copy_shallow(size, value);
    }

    std::string text()
    {
        std::string value CEPH_INDETERMINATE;
        text(value);

        return value;
    }

    void skip(const unsigned size)
    {
        require(size);
        input += size;
    }

    void skip_text() { skip(scalar<std::uint32_t>()); }

    bool text_equals(const std::string_view value)
    {
        const auto size = scalar<std::uint32_t>();
        require(size);
        if (size != value.size()) {
            input += size;

            return false;
        }
        bool equal = true;
        for (unsigned offset = 0; offset < size;) {
            const char *bytes = nullptr;
            const auto count = input.get_ptr_and_advance(size - offset, &bytes);
            equal = equal && std::ranges::equal(std::span {bytes, count},
                                                value.substr(offset, count));
            offset += count;
        }

        return equal;
    }

    template <typename BytesT>
    requires std::same_as<BytesT, std::string> || std::same_as<BytesT, byte_buffer>
    void payload(BytesT& value)
    {
        const auto size = scalar<std::uint32_t>();
        require(size);
        value.clear();
        input.copy(size, value);
    }

    // Validate the count before allocating unpublished sequence storage:
    template <unsigned Minimum, typename EntriesT, typename DecodeT>
    requires ::ceph::concepts::has_emplace_back<EntriesT>
    EntriesT sequence(DecodeT&& decode, const unsigned tail = 0)
    {
        const auto count = sequence_size<Minimum>(tail);
        EntriesT entries;
        ::ceph::util::maybe_reserve(entries, count);
        for (std::uint32_t index = 0; index < count; ++index) {
            std::invoke(decode, *this, entries.emplace_back());
        }

        return entries;
    }

    // Nested native codecs retain their wire rules; validate their consumed
    // bytes against this envelope before advancing its cursor:
    template <typename T>
    void native(T& value)
    {
        using ::ceph::decode;

        auto cursor = input;
        decode(value, cursor);
        require(input.get_remaining() - cursor.get_remaining());
        input = cursor;
    }

    template <unsigned Minimum, typename EntriesT>
    requires ::ceph::concepts::has_emplace_back<EntriesT>
    EntriesT native(const unsigned tail = 0)
    {
        return sequence<Minimum, EntriesT>(
            [](auto& fields, auto& value) { fields.native(value); }, tail);
    }

    template <typename BodyT>
    decltype(auto) frame(const std::uint8_t supported, BodyT&& body)
    {
        frame_header header CEPH_INDETERMINATE;
        fields<::ceph::encoding_detail::struct_header_len()>(header_fields {}, header);
        if (header.compat > supported) {
            throw ::ceph::buffer::malformed_input {"Ceph frame needs a newer decoder"};
        }
        require(header.len);
        const auto trailing = input.get_remaining() - header.len;
        basic_reader fields {input, trailing};
        auto decode_body = [&body, &fields, &header]() -> decltype(auto) {
            if constexpr (std::invocable<BodyT, basic_reader&, std::uint8_t, std::uint8_t>) {
                return std::invoke(std::forward<BodyT>(body), fields, header.v, header.compat);
            } else {
                return std::invoke(std::forward<BodyT>(body), fields, header.v);
            }
        };
        if constexpr (std::is_void_v<decltype(decode_body())>) {
            decode_body();
            input += input.get_remaining() - trailing;
        } else {
            decltype(auto) result = decode_body();
            input += input.get_remaining() - trailing;

            return result;
        }
    }

    // Older formats can have only a version byte. Their field profiles
    // determine where the unframed message ends:
    template <typename BodyT>
    decltype(auto) frame(const std::uint8_t supported, const std::uint8_t first_framed,
                         BodyT&& body)
    {
        require(1);
        auto cursor = input;
        std::uint8_t version CEPH_INDETERMINATE;
        ::ceph::decode(version, cursor);
        if (version >= first_framed) {
            return frame(supported, std::forward<BodyT>(body));
        }
        input = cursor;

        return std::invoke(std::forward<BodyT>(body), *this, version);
    }

    // Reuse encoded bytes only when decoding and re-encoding would preserve them.
    // Require the exact version, compat 1, and no unconsumed frame tail; compatible
    // extensions use the ordinary decode path so their extra bytes are not forwarded:
    template <typename BodyT>
    bool canonical_frame(const std::uint8_t version, BodyT&& body)
    {
        frame_header header CEPH_INDETERMINATE;
        fields<::ceph::encoding_detail::struct_header_len()>(header_fields {}, header);
        require(header.len);
        if (version != header.v || 1 != header.compat) {
            return false;
        }
        basic_reader fields {input, input.get_remaining() - header.len};

        return std::invoke(std::forward<BodyT>(body), fields) && 0 == fields.remaining();
    }

};

using reader = basic_reader<byte_buffer::const_iterator>;

// Publish the decoded record and its advanced input cursor only after decoding succeeds.
template <typename T, typename DecodeT>
[[gnu::always_inline]] inline void decode_record(T& value, byte_buffer::const_iterator& input,
                                                 DecodeT&& decode)
{
    static_assert(std::is_nothrow_move_assignable_v<T>);
    auto cursor = input;
    T candidate;
    std::invoke(std::forward<DecodeT>(decode), cursor, candidate);
    value = std::move(candidate);
    input = cursor;
}

template <std::uint8_t Supported, std::uint8_t FirstFramed = 0, typename T, typename DecodeT>
[[gnu::always_inline]] inline void decode_record(T& value, byte_buffer::const_iterator& input,
                                                 DecodeT&& decode)
{
    decode_record(value, input, [&decode](auto& cursor, auto& candidate) {
        reader archive {cursor};
        auto body = [&decode, &candidate](reader& fields, const std::uint8_t version) {
            std::invoke(std::forward<DecodeT>(decode), fields, candidate, version);
        };
        if constexpr (0 == FirstFramed) {
            archive.frame(Supported, body);
        }

        if constexpr (0 != FirstFramed) {
            archive.frame(Supported, FirstFramed, body);
        }
    });
}

} // ceph::datum_wire

#endif // CEPH_COMMON_DATUM_WIRE_H
