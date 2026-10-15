// RGW CLS: BI listing wire profiles, independent of client/module record types.
// Datum selects fields, zpp writes metadata, and Ceph retains opaque fragments.
// Common native framing and buffer ownership stay in the RGW adapter.
#ifndef CEPH_CLS_RGW_BI_WIRE_H
 #define CEPH_CLS_RGW_BI_WIRE_H

#include "cls_rgw_datum_wire.h"

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include <functional>
#include <ranges>
#include <string>
#include <string_view>

namespace cls::rgw::bi_wire
{
namespace datum = ::ceph::salad::datum;
using byte_buffer = ::ceph::buffer::list;

namespace detail
{
using ::cls::rgw::wire::checked_count;
using ::cls::rgw::wire::reader;
using ::cls::rgw::wire::writer;

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
    detail::checked_count(filter.size());
    detail::checked_count(marker.size());
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(2, [&] {
        using prefix = detail::request_prefix;

        archive.fields(detail::checked_count(13ull + filter.size() + marker.size()),
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
    checked_count(key.size());
    const auto length = checked_count(payload_size(data));
    const auto kind = ::cls::rgw::wire::checked_byte(datum::member_lens<Kind> {}(value));
    archive.frame(1, [&] {
        using prefix = entry_prefix;

        archive.fields(checked_count(9ull + key.size()),
            prefix {kind, key, length},
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
    const auto size = detail::checked_count(std::ranges::size(entries));
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
        // Each entry needs at least 15 bytes; reserve the final flag byte.
        const auto size = fields.template sequence_size<15>(1);
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
