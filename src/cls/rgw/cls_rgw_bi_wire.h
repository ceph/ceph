// RGW CLS: BI listing wire profiles, independent of client/module record types.
// Datum selects fields, zpp writes metadata, and Ceph retains opaque fragments.
// Native framing and buffer ownership stay here, outside Datum's pure core.
#ifndef CEPH_CLS_RGW_BI_WIRE_H
 #define CEPH_CLS_RGW_BI_WIRE_H

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

namespace cls::rgw::bi_wire
{
namespace datum = ::ceph::salad::datum;
using byte_buffer = ::ceph::buffer::list;

namespace detail
{
inline std::uint32_t count(const std::uint64_t size)
{
    if (size > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error {"RGW CLS wire length exceeds uint32"};
    }

    return static_cast<std::uint32_t>(size);
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

    template <typename BytesT>
    requires datum::byte_block<const BytesT>
    void payload(const BytesT& value)
    {
        const auto bytes = std::as_bytes(std::span {std::ranges::data(value), std::ranges::size(value)});
        ensure(bytes.size());
        output.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    }

    template <typename BodyT>
    void frame(const std::uint8_t version, BodyT&& body)
    {
        ensure(6);
        auto header = output.append_hole(6);
        const auto start = output.length();
        std::invoke(std::forward<BodyT>(body));
        const frame_header value {version, 1, output.length() - start};
        std::array<char, 6> bytes;
        zpp::bits::out archive {bytes, zpp::bits::endian::little {}};
        datum::serialize_schema(archive, value, header_fields {}).or_throw();
        header.copy_in(bytes.size(), bytes.data());
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

    std::string text()
    {
        const auto size = scalar<std::uint32_t>();
        require(size);
        std::string value;
        input.copy(size, value);

        return value;
    }

    byte_buffer payload()
    {
        const auto size = scalar<std::uint32_t>();
        require(size);
        byte_buffer value;
        input.copy(size, value);

        return value;
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
        auto result = std::invoke(std::forward<BodyT>(body), fields, header.version);
        input += finish - input.get_off();

        return result;
    }

};

template <typename BytesT>
std::size_t payload_size(const BytesT& value)
{
    if constexpr (std::same_as<std::remove_cvref_t<BytesT>, byte_buffer>) {
        return value.length();
    }

    if constexpr (!std::same_as<std::remove_cvref_t<BytesT>, byte_buffer>) {
        return std::ranges::size(value);
    }
}

struct request_prefix final
{
    std::uint32_t maximum;

    std::string_view filter;
    std::string_view marker;

    std::uint8_t reshard;
};

struct entry_prefix final
{
    std::uint8_t kind;
    std::string_view key;

    std::uint32_t length;
};

} // detail

inline byte_buffer encode_request(const std::uint32_t maximum, const std::string_view filter,
                              const std::string_view marker, const bool reshard)
{
    detail::count(filter.size());
    detail::count(marker.size());
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(2, [&] {
        using prefix = detail::request_prefix;

        archive.fields(detail::count(13ull + filter.size() + marker.size()),
            prefix {maximum, filter, marker, static_cast<std::uint8_t>(reshard)},
            datum::binary_fields<&prefix::maximum, &prefix::filter, &prefix::marker, &prefix::reshard> {});
    });

    return output;
}

template <typename FactoryT>
auto decode_request(byte_buffer::const_iterator& input, FactoryT&& factory)
{
    detail::reader archive {input};

    return archive.frame(2, [&](auto& fields, const std::uint8_t version) {
        const auto maximum = fields.template scalar<std::uint32_t>();
        auto filter = fields.text();
        auto marker = fields.text();
        const bool reshard = version >= 2 && 0 != fields.template scalar<std::uint8_t>();

        return std::invoke(std::forward<FactoryT>(factory), maximum,
                           std::move(filter), std::move(marker), reshard);
    });
}

namespace detail
{
template <typename EntryT, auto Kind, auto Key, auto Payload>
void encode_entry(writer& archive, const EntryT& value,
                   datum::binary_fields<Kind, Key, Payload>)
{
    const auto& data = datum::member_lens<Payload> {}(value);
    const std::string_view key = datum::member_lens<Key> {}(value);
    count(key.size());
    const auto length = count(payload_size(data));
    const auto kind = datum::member_lens<Kind> {}(value);
    const auto raw_kind = [&] {
        if constexpr (std::is_enum_v<decltype(kind)>) {
            return std::to_underlying(kind);
        } else {
            return kind;
        }
    }();
    if (!std::in_range<std::uint8_t>(raw_kind)) {
        throw std::length_error {"RGW CLS entry kind exceeds uint8"};
    }
    archive.frame(1, [&] {
        using prefix = entry_prefix;

        archive.fields(count(9ull + key.size()),
            prefix {static_cast<std::uint8_t>(raw_kind), key, length},
            datum::binary_fields<&prefix::kind, &prefix::key, &prefix::length> {});
        archive.payload(data);
    });
}

template <typename FactoryT>
auto decode_entry(reader& archive, FactoryT&& factory)
{
    return archive.frame(1, [&](auto& fields, std::uint8_t) {
        const auto kind = fields.template scalar<std::uint8_t>();
        auto key = fields.text();
        auto data = fields.payload();

        return std::invoke(std::forward<FactoryT>(factory), kind, std::move(key), std::move(data));
    });
}

} // detail

// Factories take ownership of the key and native payload references. A factory
// must retain those owners rather than return views into its temporary arguments.
template <typename FactoryT>
auto decode_entry(byte_buffer::const_iterator& input, FactoryT&& factory)
{
    detail::reader archive {input};

    return detail::decode_entry(archive, std::forward<FactoryT>(factory));
}

template <typename EntriesT, auto ...Member>
requires std::ranges::input_range<const EntriesT> && std::ranges::sized_range<const EntriesT>
byte_buffer encode_reply(const EntriesT& entries, const bool truncated, datum::binary_fields<Member...> selected)
{
    const auto size = detail::count(std::ranges::size(entries));
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(1, [&] {
        archive.scalar(size);
        for (const auto& entry : entries) {
            detail::encode_entry(archive, entry, selected);
        }
        archive.scalar(static_cast<std::uint8_t>(truncated));
    });

    return output;
}

template <typename EntriesT, typename FactoryT>
auto decode_reply(byte_buffer::const_iterator& input, FactoryT&& factory)
{
    detail::reader archive {input};

    return archive.frame(1, [&](auto& fields, std::uint8_t) {
        const auto size = fields.template scalar<std::uint32_t>();
        // A framed entry needs at least 6+1+4+4 bytes; the final flag needs one.
        fields.require(1);
        if (size > (fields.remaining() - 1) / 15) {
            throw ::ceph::buffer::malformed_input {"RGW CLS entry count exceeds frame capacity"};
        }
        EntriesT entries;
        if constexpr (requires { entries.reserve(size); }) {
            entries.reserve(size);
        }
        for (std::uint32_t index = 0; index < size; ++index) {
            entries.emplace_back(detail::decode_entry(fields, factory));
        }
        const bool truncated = 0 != fields.template scalar<std::uint8_t>();

        return std::pair {std::move(entries), truncated};
    });
}

} // cls::rgw::bi_wire

#endif // CEPH_CLS_RGW_BI_WIRE_H
