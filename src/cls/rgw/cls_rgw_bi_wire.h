// RGW CLS: BI wire profiles, independent of client/module record types.
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

namespace cls::rgw::bi_wire {

namespace datum = ::ceph::salad::datum;
using byte_buffer = ::ceph::buffer::list;

namespace detail {

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

// BI get and OLH use the same object-key frame, followed by operation fields.
inline byte_buffer encode_get_request(const std::string_view name, const std::string_view instance,
                                      const std::uint8_t kind)
{
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(1, [&] {
        ::cls::rgw::wire::encode_key(archive, name, instance);
        archive.scalar(kind);
    });

    return output;
}

template <typename KeyT, auto ...Member>
std::uint8_t decode_get_request(byte_buffer::const_iterator& input, KeyT& key,
                                datum::binary_fields<Member...> selected)
{
    detail::reader archive {input};

    return archive.frame(1, [&](auto& fields, std::uint8_t) {
        ::cls::rgw::wire::decode_key(fields, key, selected);

        return fields.template scalar<std::uint8_t>();
    });
}

namespace detail {

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

template <typename EntryT, auto Kind, auto Key, auto Payload>
EntryT& decode_entry(reader& archive, EntryT& value,
                     datum::binary_fields<Kind, Key, Payload>)
{
    return archive.frame(1, [&](auto& fields, std::uint8_t) -> EntryT& {
        auto& kind = value.*Kind;
        kind = static_cast<std::remove_cvref_t<decltype(kind)>>(fields.template scalar<std::uint8_t>());
        fields.text(value.*Key);
        fields.payload(value.*Payload);

        return value;
    });
}

} // detail

// Fill unpublished candidates; the binding commits them after decoding succeeds.
// Selected payload fields retain native owners beyond the input envelope.
template <typename EntryT, auto ...Member>
EntryT& decode_entry(byte_buffer::const_iterator& input, EntryT& value,
                     datum::binary_fields<Member...> selected)
{
    detail::reader archive {input};

    return detail::decode_entry(archive, value, selected);
}

// A single-entry message wraps the same entry profile in its own native frame.
template <typename EntryT, auto ...Member>
byte_buffer encode_entry_message(const EntryT& value, datum::binary_fields<Member...> selected)
{
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(1, [&] {
        detail::encode_entry(archive, value, selected);
    });

    return output;
}

template <typename EntryT, auto ...Member>
EntryT& decode_entry_message(byte_buffer::const_iterator& input, EntryT& value,
                             datum::binary_fields<Member...> selected)
{
    detail::reader archive {input};

    return archive.frame(1, [&](auto& fields, std::uint8_t) -> EntryT& {
        return detail::decode_entry(fields, value, selected);
    });
}

// List replies and batch requests share a counted entry sequence and flag.
// The binding interprets the flag as truncation or an overwrite check.
template <typename EntriesT, auto ...Member>
requires std::ranges::input_range<const EntriesT> && std::ranges::sized_range<const EntriesT>
byte_buffer encode_entries(const EntriesT& entries, const bool flag, datum::binary_fields<Member...> selected)
{
    const auto size = detail::checked_count(std::ranges::size(entries));
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(1, [&] {
        archive.scalar(size);
        for (const auto& entry : entries) {
            detail::encode_entry(archive, entry, selected);
        }
        archive.scalar(static_cast<std::uint8_t>(flag));
    });

    return output;
}

template <typename EntriesT, auto ...Member>
auto decode_entries(byte_buffer::const_iterator& input, datum::binary_fields<Member...> selected)
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
            detail::decode_entry(fields, entries.emplace_back(), selected);
        }
        const bool flag = 0 != fields.template scalar<std::uint8_t>();

        return std::pair {std::move(entries), flag};
    });
}

} // cls::rgw::bi_wire

#endif // CEPH_CLS_RGW_BI_WIRE_H
