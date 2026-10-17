// RGW CLS: Shared native framing and metadata adapters over Datum/zpp.
// Native buffers and wire bounds belong here, never in Datum's pure core.
#ifndef CEPH_CLS_RGW_DATUM_WIRE_H
 #define CEPH_CLS_RGW_DATUM_WIRE_H

#include "datum/codec/zpp.h"
#include "include/buffer.h"

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

namespace cls::rgw::wire {

namespace datum = ::ceph::salad::datum;
using byte_buffer = ::ceph::buffer::list;

inline std::uint32_t checked_count(const std::uint64_t size)
{
    if (size > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error {"RGW CLS wire length exceeds uint32"};
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
            throw std::length_error {"RGW CLS wire value exceeds uint8"};
        }

        return static_cast<std::uint8_t>(value);
    }
}

template <typename T>
struct scalar_record final
{
    T value;
};

struct frame_header final
{
    std::uint8_t version;
    std::uint8_t compatible;

    std::uint32_t length;
};

using header_fields = datum::binary_fields<&frame_header::version,
    &frame_header::compatible, &frame_header::length>;

class writer final
{
    byte_buffer& output;

    void copy_payload(const byte_buffer& value, const unsigned offset, const unsigned size)
    {
        ensure(size);
        auto storage = output.get_contiguous_appender(size);
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
            throw std::length_error {"RGW CLS message exceeds uint32"};
        }
    }

    template <typename RecordT, auto ...Member>
    void fields(const std::size_t size, const RecordT& value, datum::binary_fields<Member...> selected)
    {
        ensure(size);
        auto storage = output.get_contiguous_appender(size);
        std::span bytes {storage.get_pos_add(size), size};
        zpp::bits::out archive {bytes, zpp::bits::size4b {}, zpp::bits::endian::little {}};
        datum::serialize_schema(archive, value, selected).or_throw();
    }

    template <typename T>
    void scalar(const T value)
    {
        using record = scalar_record<T>;

        fields(sizeof(T), record {value}, datum::binary_fields<&record::value> {});
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
        const auto size = checked_count(value.size());
        using record = scalar_record<std::string_view>;

        fields(4ull + size, record {value}, datum::binary_fields<&record::value> {});
    }

    // A native byte chain can carry a string without an owning string temporary:
    void text(const byte_buffer& value)
    {
        scalar(static_cast<std::uint32_t>(value.length()));
        ensure(value.length());
        auto storage = output.get_contiguous_appender(value.length());
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

    // Keep established nested RGW codecs while borrowing their owning records:
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

    template <typename BodyT>
    void frame(const std::uint8_t version, const std::uint8_t compatible, BodyT&& body)
    {
        ensure(6);
        auto header = output.append_hole(6);
        const auto start = output.length();
        std::invoke(std::forward<BodyT>(body));
        const frame_header value {version, compatible, output.length() - start};
        std::span bytes {header.c_str(), std::size_t {6}};
        zpp::bits::out archive {bytes, zpp::bits::endian::little {}};
        datum::serialize_schema(archive, value, header_fields {}).or_throw();
    }

    template <typename BodyT>
    void frame(const std::uint8_t version, BodyT&& body)
    {
        frame(version, 1, std::forward<BodyT>(body));
    }

};

class reader final
{
    byte_buffer::const_iterator& input;

    unsigned limit;

public:
    explicit reader(byte_buffer::const_iterator& input)
        : input(input), limit(input.get_off() + input.get_remaining())
    {}

private:
    reader(byte_buffer::const_iterator& input, const unsigned limit)
        : input(input), limit(limit)
    {}

public:
    unsigned remaining() const noexcept { return limit - input.get_off(); }

    void require(const std::size_t size) const
    {
        if (size > remaining()) {
            throw ::ceph::buffer::malformed_input {"RGW CLS field exceeds its frame"};
        }
    }

    template <unsigned Minimum>
    std::uint32_t sequence_size(const unsigned tail = 0)
    {
        static_assert(Minimum > 0);
        const auto size = scalar<std::uint32_t>();
        require(tail);
        if (size > (remaining() - tail) / Minimum) {
            throw ::ceph::buffer::malformed_input {"RGW CLS count exceeds frame capacity"};
        }

        return size;
    }

    template <std::size_t Size, typename RecordT, auto ...Member>
    void fields(RecordT& value, datum::binary_fields<Member...> selected)
    {
        require(Size);
        std::array<char, Size> storage;
        const char *bytes = nullptr;
        const auto first = input.get_ptr_and_advance(Size, &bytes);
        if (first < Size) {
            std::copy_n(bytes, first, storage.data());
            input.copy(Size - first, storage.data() + first);
            bytes = storage.data();
        }
        std::span view {bytes, Size};
        zpp::bits::in archive {view, zpp::bits::endian::little {}};
        datum::serialize_schema(archive, value, selected).or_throw();
    }

    template <std::unsigned_integral T>
    T scalar()
    {
        using record = scalar_record<T>;
        record value;
        fields<sizeof(T)>(value, datum::binary_fields<&record::value> {});

        return value.value;
    }

    void text(std::string& value)
    {
        const auto size = scalar<std::uint32_t>();
        require(size);
        value.clear();
        input.copy(size, value);
    }

    std::string text()
    {
        std::string value;
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
            equal = equal && std::ranges::equal(std::span {bytes, count}, value.substr(offset, count));
            offset += count;
        }

        return equal;
    }

    void payload(byte_buffer& value)
    {
        const auto size = scalar<std::uint32_t>();
        require(size);
        value.clear();
        input.copy(size, value);
    }

    // Nested native codecs retain their wire rules; validate their consumed
    // bytes against this envelope before advancing its cursor:
    template <typename T>
    void native(T& value)
    {
        using ::ceph::decode;

        auto cursor = input;
        decode(value, cursor);
        require(cursor.get_off() - input.get_off());
        input = cursor;
    }

    template <typename BodyT>
    decltype(auto) frame(const std::uint8_t supported, BodyT&& body)
    {
        frame_header header;
        fields<6>(header, header_fields {});
        if (header.compatible > supported) {
            throw ::ceph::buffer::malformed_input {"RGW CLS frame needs a newer decoder"};
        }
        require(header.length);
        const auto finish = input.get_off() + header.length;
        reader fields {input, finish};
        decltype(auto) result = std::invoke(std::forward<BodyT>(body), fields, header.version);
        input += finish - input.get_off();

        return result;
    }

    // Prepare/complete versions before the framed format have only a version
    // byte. Their field profiles determine where the old message ends:
    template <typename BodyT>
    decltype(auto) frame(const std::uint8_t supported, const std::uint8_t first_framed,
                         BodyT&& body)
    {
        require(1);
        auto cursor = input;
        std::uint8_t version;
        ::ceph::decode(version, cursor);
        if (version >= first_framed) {
            return frame(supported, std::forward<BodyT>(body));
        }
        input = cursor;

        return std::invoke(std::forward<BodyT>(body), *this, version);
    }

    // Encoded forwarding requires exactly the representation an owning
    // decoder would emit. Compatible extensions use the ordinary decode path:
    template <typename BodyT>
    bool canonical_frame(const std::uint8_t version, BodyT&& body)
    {
        frame_header header;
        fields<6>(header, header_fields {});
        require(header.length);
        if (version != header.version || 1 != header.compatible) {
            return false;
        }
        reader fields {input, input.get_off() + header.length};

        return std::invoke(std::forward<BodyT>(body), fields) && 0 == fields.remaining();
    }

};

// Publish an owning request and its native cursor only after its frame succeeds.
template <std::uint8_t Supported, std::uint8_t FirstFramed = 0, typename T, typename DecodeT>
[[gnu::always_inline]] inline void decode_record(T& value, byte_buffer::const_iterator& input, const DecodeT decode)
{
    static_assert(std::is_nothrow_move_assignable_v<T>);
    auto cursor = input;
    T candidate;
    reader archive {cursor};
    auto body = [&](reader& fields, const std::uint8_t version) {
        std::invoke(decode, fields, candidate, version);

        return 0;
    };
    if constexpr (0 == FirstFramed) {
        archive.frame(Supported, body);
    }

    if constexpr (0 != FirstFramed) {
        archive.frame(Supported, FirstFramed, body);
    }
    value = std::move(candidate);
    input = cursor;
}

struct key_fields final
{
    std::string_view name;
    std::string_view instance;
};

inline void encode_key(writer& archive, const std::string_view name, const std::string_view instance)
{
    checked_count(name.size());
    checked_count(instance.size());
    archive.frame(1, [&] {
        archive.fields(checked_count(8ull + name.size() + instance.size()), key_fields {name, instance},
            datum::binary_fields<&key_fields::name, &key_fields::instance> {});
    });
}

template <typename KeyT, auto Name, auto Instance>
KeyT& decode_key(reader& archive, KeyT& value, datum::binary_fields<Name, Instance>)
{
    return archive.frame(1, [&](auto& fields, std::uint8_t) -> KeyT& {
        fields.text(value.*Name);
        fields.text(value.*Instance);

        return value;
    });
}

inline auto decode_key(reader& archive)
{
    using key = std::pair<std::string, std::string>;

    key value;
    decode_key(archive, value, datum::binary_fields<&key::first, &key::second> {});

    return value;
}

} // cls::rgw::wire

#endif // CEPH_CLS_RGW_DATUM_WIRE_H
