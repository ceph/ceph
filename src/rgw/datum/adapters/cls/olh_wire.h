// RGW CLS: OLH framing with independent record and container bindings.
#ifndef CEPH_CLS_RGW_OLH_WIRE_H
 #define CEPH_CLS_RGW_OLH_WIRE_H

#include "rgw/datum/adapters/rgw/key.h"

#include <cstdint>
#include <utility>

#include <functional>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>

namespace cls::rgw::olh_wire {

namespace datum = ::ceph::salad::datum;
using byte_buffer = ::ceph::buffer::list;

namespace detail {

using ::ceph::datum_wire::checked_count;
using ::rgw::datum_wire::decode_key;
using ::rgw::datum_wire::encode_key;
using ::ceph::datum_wire::reader;
using ::ceph::datum_wire::writer;

struct request_tail final
{
    std::uint64_t marker;
    std::string_view tag;

    std::uint8_t stales;
};

struct event_prefix final
{
    std::uint64_t epoch;
    std::uint8_t operation;

    std::string_view tag;
};

constexpr auto event_prefix_fields = datum::binary_fields<
    &event_prefix::epoch, &event_prefix::operation, &event_prefix::tag> {};

// An epoch group starts with its epoch and the count of its events.
inline constexpr auto minimum_group_size = sizeof(std::uint64_t) + sizeof(std::uint32_t);
// An event frame contains epoch/action/tag, a nested object key, and a delete-marker flag.
inline constexpr auto minimum_event_size = ::ceph::encoding_detail::struct_header_len() +
    ::ceph::datum_wire::encoded_size(event_prefix_fields, event_prefix {}) +
    ::rgw::datum_wire::minimum_key_size + sizeof(std::uint8_t);

template <typename EntryT, auto Epoch, auto Operation, auto Tag, auto Key, auto Deleted,
          auto Name, auto Instance>
void encode_event(datum::binary_fields<Epoch, Operation, Tag, Key, Deleted>,
                  datum::binary_fields<Name, Instance>, writer& archive, const EntryT& value)
{
    const std::string_view tag = datum::member_lens<Tag> {}(value);
    archive.frame(1, [&] [[gnu::noinline]] {
        using prefix = event_prefix;

        archive.fields(event_prefix_fields,
                       prefix {datum::member_lens<Epoch> {}(value),
                               ::ceph::datum_wire::checked_byte(datum::member_lens<Operation> {}(value)), tag});
        const auto& key = datum::member_lens<Key> {}(value);
        encode_key(archive, datum::member_lens<Name> {}(key), datum::member_lens<Instance> {}(key));
        archive.scalar(static_cast<std::uint8_t>(datum::member_lens<Deleted> {}(value)));
    });
}

template <typename EntryT, auto Epoch, auto Operation, auto Tag, auto Key, auto Deleted,
          auto ...KeyMember>
void decode_event(datum::binary_fields<Epoch, Operation, Tag, Key, Deleted>,
                  datum::binary_fields<KeyMember...> key_fields, reader& archive, EntryT& value)
{
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        value.*Epoch = fields.template scalar<std::uint64_t>();
        auto& operation = value.*Operation;
        // Admit the entire native uint8 wire domain; replay decides which actions it supports.
        operation = static_cast<std::remove_cvref_t<decltype(operation)>>(fields.template scalar<std::uint8_t>());
        fields.text(value.*Tag);
        decode_key(key_fields, fields, value.*Key);
        value.*Deleted = 0 != fields.template scalar<std::uint8_t>();
    });
}

} // detail

inline byte_buffer encode_request(const std::string_view name, const std::string_view instance,
                                  const std::uint64_t marker, const std::string_view tag, const bool stales)
{
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(2, [&] {
        detail::encode_key(archive, name, instance);
        using tail = detail::request_tail;

        archive.fields(datum::binary_fields<&tail::marker, &tail::tag, &tail::stales> {},
                       tail {marker, tag, static_cast<std::uint8_t>(stales)});
    });

    return output;
}

template <typename TagT = std::string, typename FactoryT>
auto decode_request(byte_buffer::const_iterator& input, FactoryT&& factory)
{
    detail::reader archive {input};

    return archive.frame(2, [&](auto& fields, const std::uint8_t version) {
        auto [name, instance] = detail::decode_key(fields);
        const auto marker = fields.template scalar<std::uint64_t>();
        TagT tag CEPH_INDETERMINATE;
        fields.text(tag);
        // Version 1 predates the stale-event flag and retains its false default.
        const bool stales = version >= 2 && 0 != fields.template scalar<std::uint8_t>();

        return std::invoke(std::forward<FactoryT>(factory), std::move(name),
                           std::move(instance), marker, std::move(tag), stales);
    });
}

// Filtering is applied to an already paginated range. Empty epoch groups are
// retained with all events when requested by the native get_stales policy.
// Filtering must not refill the page or recompute truncated: even an empty filtered
// reply can report that more stored epoch groups remain.
template <std::ranges::forward_range GroupsT, auto ...EventMember, auto ...KeyMember, typename IncludeT>
byte_buffer encode_reply(datum::binary_fields<EventMember...> event_fields,
                         datum::binary_fields<KeyMember...> key_fields,
                         const GroupsT& groups, const bool truncated,
                         IncludeT&& include, const bool include_all)
{
    const auto included_count = [&](const auto& entries) {
        return detail::checked_count(include_all ? std::ranges::size(entries)
                                                 : std::ranges::count_if(entries, std::ref(include)));
    };
    const auto group_count = detail::checked_count(include_all ? std::ranges::distance(groups)
        : std::ranges::count_if(groups, [&](const auto& group) { return 0 != included_count(group.second); }));
    byte_buffer output;
    detail::writer archive {output};
    archive.frame(1, [&] {
        archive.scalar(group_count);
        for (const auto& [epoch, entries] : groups) {
            const auto size = included_count(entries);
            if (!include_all && 0 == size) {
                continue;
            }
            archive.scalar(epoch);
            archive.scalar(size);
            for (const auto& entry : entries) {
                if (include_all || std::invoke(include, entry)) {
                    detail::encode_event(event_fields, key_fields, archive, entry);
                }
            }
        }
        archive.scalar(static_cast<std::uint8_t>(truncated));
    });

    return output;
}

// The insertion binding determines local associative semantics (such as
// first-wins map insertion) without enrolling a native type in Datum.
template <typename GroupsT, typename EntriesT, auto ...EventMember, auto ...KeyMember, typename InsertT>
auto decode_reply(datum::binary_fields<EventMember...> event_fields,
                  datum::binary_fields<KeyMember...> key_fields,
                  byte_buffer::const_iterator& input, InsertT&& insert)
{
    detail::reader archive {input};

    return archive.frame(1, [&](auto& fields, std::uint8_t) {
        const auto size = fields.template sequence_size<detail::minimum_group_size>(1);
        GroupsT groups;
        ::ceph::util::maybe_reserve(groups, size);
        for (std::uint32_t index = 0; index < size; ++index) {
            const auto epoch = fields.template scalar<std::uint64_t>();
            auto entries = fields.template sequence<detail::minimum_event_size, EntriesT>(
                [event_fields, key_fields](auto& fields, auto& entry) {
                    detail::decode_event(event_fields, key_fields, fields, entry);
                });
            std::invoke(insert, groups, epoch, std::move(entries));
        }
        const bool truncated = 0 != fields.template scalar<std::uint8_t>();

        return std::pair {std::move(groups), truncated};
    });
}

} // cls::rgw::olh_wire

#endif // CEPH_CLS_RGW_OLH_WIRE_H
