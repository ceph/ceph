// RGW CLS: BI wire profiles, independent of client/module record types.
// Datum selects fields, zpp writes metadata, and Ceph retains opaque fragments.
// Common native framing and buffer ownership stay in the RGW adapter.
#ifndef CEPH_CLS_RGW_BI_WIRE_H
 #define CEPH_CLS_RGW_BI_WIRE_H

#include "rgw/datum/adapters/rgw/key.h"

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include <ranges>
#include <string>
#include <string_view>

namespace cls::rgw::bi_wire {

namespace datum = ::ceph::salad::datum;
using byte_buffer = ::ceph::buffer::list;

namespace detail {

using ::ceph::datum_wire::checked_count;
using ::ceph::datum_wire::reader;
using ::ceph::datum_wire::writer;

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

struct empty_entries final
{
    std::uint32_t count;
    std::uint8_t flag;
};

struct entry_prefix final
{
    std::uint8_t kind;
    std::string_view key;

    std::uint32_t length;
};

constexpr auto entry_prefix_fields = datum::binary_fields<&entry_prefix::kind,
    &entry_prefix::key, &entry_prefix::length> {};

} // detail

template <std::uint8_t Version = 2>
byte_buffer encode_request(const std::uint32_t maximum, const std::string_view filter,
                                  const std::string_view marker, const bool reshard)
{
    byte_buffer output;
    detail::writer archive {output};
    using prefix = detail::request_prefix;

    archive.frame(datum::binary_fields<&prefix::maximum, &prefix::filter, &prefix::marker, &prefix::reshard> {},
                  Version, prefix {maximum, filter, marker, static_cast<std::uint8_t>(reshard)});

    return output;
}

template <typename RequestT, auto Maximum, auto Filter, auto Marker, auto Reshard>
std::uint8_t decode_request(datum::binary_fields<Maximum, Filter, Marker, Reshard>,
                    byte_buffer::const_iterator& input, RequestT& value)
{
    std::uint8_t decoded_version = 0;
    ::ceph::datum_wire::decode_record<2>(value, input, [&decoded_version](auto& fields, auto& candidate, const std::uint8_t version) {
        candidate.*Maximum = fields.template scalar<std::uint32_t>();
        fields.text(candidate.*Filter);
        fields.text(candidate.*Marker);
        candidate.*Reshard = version >= 2 && 0 != fields.template scalar<std::uint8_t>();
        decoded_version = version;
    });

    return decoded_version;
}

// BI get and OLH use the same object-key frame, followed by operation fields.
inline byte_buffer encode_get_request(const std::string_view name, const std::string_view instance,
                                      const std::uint8_t kind)
{
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(1, [&] {
        ::rgw::datum_wire::encode_key(archive, name, instance);
        archive.scalar(kind);
    });

    return output;
}

template <typename KeyT, auto ...Member>
std::uint8_t decode_get_request(datum::binary_fields<Member...> selected, byte_buffer::const_iterator& input, KeyT& key)
{
    detail::reader archive {input};

    return archive.frame(1, [&](auto& fields, std::uint8_t) {
        ::rgw::datum_wire::decode_key(selected, fields, key);

        return fields.template scalar<std::uint8_t>();
    });
}

namespace detail {

template <std::uint8_t Version = 1, typename EntryT, auto Kind, auto Key, auto Payload>
void encode_entry(datum::binary_fields<Kind, Key, Payload>, writer& archive, const EntryT& value)
{
    const auto& data = datum::member_lens<Payload> {}(value);
    const std::string_view key = datum::member_lens<Key> {}(value);
    const auto length = checked_count(payload_size(data));
    const auto kind = ::ceph::datum_wire::checked_byte(datum::member_lens<Kind> {}(value));
    using prefix = entry_prefix;

    if constexpr (1 == Version) {
        archive.frame(1, [&] {
            archive.fields(entry_prefix_fields, prefix {kind, key, length});
            archive.payload(data);
        });
    }

    if constexpr (2 == Version) {
        archive.fields(::ceph::datum_wire::growing_metadata, entry_prefix_fields, prefix {kind, key, length});
        archive.payload(data);
    }
}

template <std::uint8_t Version = 1, typename EntryT, auto Kind, auto Key, auto Payload>
void decode_entry(datum::binary_fields<Kind, Key, Payload>, reader& archive, EntryT& value)
{
    const auto body = [&](auto& fields) {
        auto& kind = value.*Kind;
        kind = static_cast<std::remove_cvref_t<decltype(kind)>>(fields.template scalar<std::uint8_t>());
        fields.text(value.*Key);
        fields.payload(value.*Payload);
    };

    if constexpr (1 == Version) {
        archive.frame(1, [&](auto& fields, std::uint8_t) { body(fields); });
    }

    if constexpr (2 == Version) {
        body(archive);
    }
}

} // detail

// Fill unpublished candidates; the binding commits them after decoding succeeds.
// Payload fields own copied bytes or retain the input's buffer owners.
template <typename EntryT, auto ...Member>
void decode_entry(datum::binary_fields<Member...> selected,
                  byte_buffer::const_iterator& input, EntryT& value)
{
    detail::reader archive {input};

    detail::decode_entry(selected, archive, value);
}

// A single-entry message wraps the same entry profile in its own native frame.
template <typename EntryT, auto ...Member>
byte_buffer encode_entry_message(datum::binary_fields<Member...> selected, const EntryT& value)
{
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(1, [&] {
        detail::encode_entry(selected, archive, value);
    });

    return output;
}

template <typename EntryT, auto ...Member>
void decode_entry_message(datum::binary_fields<Member...> selected,
                          byte_buffer::const_iterator& input, EntryT& value)
{
    detail::reader archive {input};

    archive.frame(1, [&](auto& fields, std::uint8_t) {
        detail::decode_entry(selected, fields, value);
    });
}

// List replies and batch requests share a counted entry sequence and flag.
// The binding interprets the flag as truncation or an overwrite check.
template <std::uint8_t Version = 1, typename EntriesT, auto ...Member>
requires std::ranges::input_range<const EntriesT> && std::ranges::sized_range<const EntriesT>
byte_buffer encode_entries(datum::binary_fields<Member...> selected, const EntriesT& entries, const bool flag)
{
    static_assert(1 == Version || 2 == Version);
    const auto size = detail::checked_count(std::ranges::size(entries));
    byte_buffer output;
    detail::writer archive {output};
    if (0 == size) {
        using body = detail::empty_entries;

        archive.frame(datum::binary_fields<&body::count, &body::flag> {},
                      1, body {size, static_cast<std::uint8_t>(flag)});

        return output;
    }
    archive.frame(Version, Version, [&] {
        archive.scalar(size);
        for (const auto& entry : entries) {
            detail::encode_entry<Version>(selected, archive, entry);
        }
        archive.scalar(static_cast<std::uint8_t>(flag));
    });

    return output;
}

template <typename EntriesT, std::uint8_t Supported = 1, auto ...Member>
auto decode_entries(datum::binary_fields<Member...> selected, byte_buffer::const_iterator& input)
{
    detail::reader archive {input};

    return archive.frame(Supported, [&](auto& fields, const std::uint8_t version, const std::uint8_t compatible) {
        const auto body = [&]<std::uint8_t Version>() {
            // Entry metadata is kind, key, and payload length; only v1 frames each entry.
            constexpr auto minimum_entry_size =
                ::ceph::datum_wire::encoded_size(
                    detail::entry_prefix_fields, detail::entry_prefix {}) +
                (1 == Version ? ::ceph::encoding_detail::struct_header_len() : 0);
            auto entries = fields.template sequence<minimum_entry_size, EntriesT>(
                [selected](auto& fields, auto& entry) { detail::decode_entry<Version>(selected, fields, entry); }, 1);
            const bool flag = 0 != fields.template scalar<std::uint8_t>();

            return std::pair {std::move(entries), flag};
        };
        // Compatibility, rather than version alone, selects the compact layout. A newer
        // compat-1 reply still has framed entries and may only append outer-frame fields.
        if constexpr (2 <= Supported) {
            if (2 <= compatible) {
                if (version < 2) {
                    throw ::ceph::buffer::malformed_input {"RGW CLS compact reply needs version two"};
                }
                return body.template operator()<2>();
            }
        }

        return body.template operator()<1>();
    });
}

} // cls::rgw::bi_wire

#endif // CEPH_CLS_RGW_BI_WIRE_H
