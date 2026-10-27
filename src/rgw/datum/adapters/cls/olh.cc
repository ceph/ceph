// RGW CLS: Bind OLH wire fields to native maps or independent vector records.
#include "olh.h"
#include "olh_wire.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <algorithm>
#include <cerrno>
#include <ranges>
#include <span>
#include <type_traits>
#include <utility>

namespace cls::rgw::olh_datum {

namespace {

namespace datum = ::ceph::salad::datum;
using native_entry = ::rgw_bucket_olh_log_entry;
using native_key = ::cls_rgw_obj_key;
using client_entry = ::cls::rgw::olh::event;
using client_key = ::cls::rgw::olh::object_key;

// Build ordered member lenses, providing compile-time mapping of Ceph data members into Datum's binary codec:
constexpr auto native_event_fields = datum::binary_fields<&native_entry::epoch, &native_entry::op,
    &native_entry::op_tag, &native_entry::key, &native_entry::delete_marker> {};
constexpr auto native_key_fields = datum::binary_fields<&native_key::name, &native_key::instance> {};
constexpr auto client_event_fields = datum::binary_fields<&client_entry::epoch, &client_entry::operation,
    &client_entry::tag, &client_entry::object, &client_entry::delete_marker> {};
constexpr auto client_key_fields = datum::binary_fields<&client_key::name, &client_key::instance> {};

// The unread span is the active suffix; next_fragment points past accounted-for fragments.
// Empty spans load the next fragment lazily. The input owns all bytes.
// basic_reader checks each read/skip against its frame before advancing this cursor.
// SingleFragment is selected only for one nonempty buffer, so its spans need no fragment walk.
template <bool SingleFragment>
class fragment_cursor final
{
    ::ceph::buffer::list::buffers_t::const_iterator next_fragment;
    ::ceph::buffer::list::buffers_t::const_iterator fragments_end;

    std::span<const char> unread;

    unsigned offset = 0;
    unsigned input_size;

public:
    explicit fragment_cursor(const ::ceph::buffer::list& input)
        : input_size(input.length())
    {
        if constexpr (SingleFragment) {
            const auto& fragment = input.buffers().front();
            unread = {fragment.c_str(), fragment.length()};
        } else {
            next_fragment = input.buffers().begin();
            fragments_end = input.buffers().end();
        }
    }

public:
    unsigned get_offset() const noexcept { return offset; }
    unsigned get_remaining() const noexcept { return input_size - offset; }

    std::size_t get_ptr_and_advance(const std::size_t wanted, const char **bytes)
    {
        if constexpr (!SingleFragment) {
            while (unread.empty() && next_fragment != fragments_end) {
                const auto& fragment = *next_fragment++;
                if (fragment.length()) {
                    unread = {fragment.c_str(), fragment.length()};
                }
            }
            if (unread.empty() && wanted) {
                throw ::ceph::buffer::end_of_buffer {};
            }
        }
        const auto size = SingleFragment ? wanted : std::min(wanted, unread.size());
        *bytes = unread.data();
        unread = unread.subspan(size);
        offset += size;

        return size;
    }

    fragment_cursor& operator+=(const unsigned size)
    {
        if constexpr (SingleFragment) {
            unread = unread.subspan(size);
        } else {
            auto remaining = size;
            const auto step = std::min<std::size_t>(remaining, unread.size());
            unread = unread.subspan(step);
            remaining -= step;
            // Skip whole fragments by length; only the final suffix needs a byte span.
            while (remaining) {
                if (next_fragment == fragments_end) {
                    throw ::ceph::buffer::end_of_buffer {};
                }
                const auto& fragment = *next_fragment++;
                if (remaining < fragment.length()) {
                    unread = {fragment.c_str() + remaining, fragment.length() - remaining};
                    remaining = 0;
                } else {
                    remaining -= fragment.length();
                }
            }
        }
        offset += size;

        return *this;
    }

};

// Forward only the exact native v1 frames: compatible extensions would be discarded by
// decode/re-encode, so copying them verbatim would change the reply's wire representation.
bool canonical_key(auto& archive)
{
    return archive.canonical_frame(1, [](auto& fields) {
        fields.skip_text();
        fields.skip_text();

        return true;
    });
}

bool canonical_event(auto& archive)
{
    return archive.canonical_frame(1, [](auto& fields) {
        // Skip the event epoch and action; their original bytes are forwarded.
        fields.skip(sizeof(std::uint64_t) + sizeof(std::uint8_t));
        fields.skip_text();
        if (!canonical_key(fields)) {
            return false;
        }

        // Native bool decoding normalizes every nonzero byte to true; only 0/1 survive unchanged.
        return fields.template scalar<std::uint8_t>() <= 1;
    });
}

// The stored rgw_bucket_olh_entry nests the same epoch/event sequence as the read reply.
// Reuse that sequence only after validating the whole stored frame, including unselected groups
// and the tag after the log. A full page cannot short-circuit validation or tag comparison.
template <bool SingleFragment>
std::optional<int> try_encode_stored_reply(
    std::bool_constant<SingleFragment>, const ::ceph::buffer::list& input,
    const std::uint64_t marker, const std::string_view tag, ::ceph::buffer::list& output)
{
    fragment_cursor<SingleFragment> cursor {input};
    ::ceph::datum_wire::basic_reader archive {cursor};
    unsigned lower = 0;
    unsigned upper = 0;
    std::uint32_t selected = 0;
    bool truncated = false;
    bool matches = false;
    const bool canonical = archive.canonical_frame(1, [&](auto& fields) {
        if (!canonical_key(fields)) {
            return false;
        }
        // The stored head's delete-marker and epoch are not reply fields.
        fields.skip(sizeof(std::uint8_t) + sizeof(std::uint64_t));
        // Reserve the tag's length prefix and the exists/pending_removal flags.
        constexpr auto tail_size = sizeof(std::uint32_t) + 2 * sizeof(std::uint8_t);
        const auto count = fields.template sequence_size<
            ::cls::rgw::olh_wire::detail::minimum_group_size>(tail_size);
        std::uint64_t previous = 0;
        for (std::uint32_t index = 0; index < count; ++index) {
            const auto start = cursor.get_offset();
            const auto epoch = fields.template scalar<std::uint64_t>();
            if (0 != index && epoch <= previous) {
                return false; // Native map decoding orders and deduplicates groups.
            }
            previous = epoch;
            const auto events = fields.template sequence_size<
                ::cls::rgw::olh_wire::detail::minimum_event_size>();
            for (std::uint32_t item = 0; item < events; ++item) {
                if (!canonical_event(fields)) {
                    return false;
                }
            }
            if (marker < epoch) {
                if (max_page_epochs == selected) {
                    truncated = true;
                    continue;
                }
                if (0 == selected) {
                    lower = start;
                }
                upper = cursor.get_offset();
                ++selected;
            }
        }
        matches = fields.text_equals(tag);
        // Stored exists/pending-removal flags are not reply fields.
        fields.skip(2 * sizeof(std::uint8_t));

        return true;
    });
    if (!canonical) {
        return std::nullopt;
    }
    if (!matches) {
        return -ECANCELED;
    }

    // Rebuild the reply envelope around the selected encoded epoch groups.
    ::ceph::buffer::list candidate;
    ::ceph::datum_wire::writer writer {candidate};
    writer.frame(1, [&] {
        writer.scalar(selected);
        writer.payload(input, lower, upper - lower);
        writer.scalar(static_cast<std::uint8_t>(truncated));
    });
    output.claim_append(candidate);

    return 0;
}

} // anonymous namespace

::ceph::buffer::list encode_request(const ::cls_rgw_obj_key& key, const std::uint64_t marker,
                                    const std::string_view tag, const bool stales)
{
    return ::cls::rgw::olh_wire::encode_request(key.name, key.instance, marker, tag, stales);
}

void decode(request& value, ::ceph::buffer::list::const_iterator& input)
{
    ::ceph::datum_wire::decode_record(value, input, [](auto& cursor, auto& candidate) {
        ::cls::rgw::olh_wire::decode_request<::ceph::buffer::ptr>(cursor,
            [&candidate](std::string&& name, std::string&& instance, const std::uint64_t marker,
                         ::ceph::buffer::ptr&& tag, const bool stales) {
                candidate.olh.name = std::move(name);
                candidate.olh.instance = std::move(instance);
                candidate.tag_bytes = std::move(tag);
                candidate.ver_marker = marker;
                candidate.get_stales = stales;
            });
    });
}

void encode_reply(const log_page groups, const bool stales, const bool truncated, ::ceph::buffer::list& output)
{
    auto candidate = ::cls::rgw::olh_wire::encode_reply(native_event_fields, native_key_fields,
        groups, truncated,
        [](const native_entry& value) { return ::CLS_RGW_OLH_OP_STALE != value.op; }, stales);
    output.claim_append(candidate);
}

std::optional<int> try_encode_stored_reply(const ::ceph::buffer::list& input,
                                           const std::uint64_t marker, const std::string_view tag,
                                           ::ceph::buffer::list& output)
try {
    // Compare payload size with the buffer-node metadata that forwarding may retain.
    // This is an owner-overhead heuristic, not a wire-size limit: dense chains fall back to
    // the owning decoder rather than retaining many owners for very little payload.
    const auto owner_bytes = std::uint64_t {input.get_num_buffers()} * sizeof(::ceph::buffer::ptr_node);
    if (input.length() < owner_bytes) {
        return std::nullopt;
    }
    if (1 == input.get_num_buffers() && input.length()) {
        return try_encode_stored_reply(std::true_type {}, input, marker, tag, output);
    }

    return try_encode_stored_reply(std::false_type {}, input, marker, tag, output);
} catch (const ::ceph::buffer::error&) {
    // Let the established stored-record decoder retain native error behavior.
    return std::nullopt;
}

void decode_reply(const ::ceph::buffer::list& input, ::rgw_cls_read_olh_log_ret& value)
{
    using groups_type = decltype(::rgw_cls_read_olh_log_ret::log);

    auto cursor = input.cbegin();
    auto [groups, truncated] = ::cls::rgw::olh_wire::decode_reply<groups_type, groups_type::mapped_type>(
        native_event_fields, native_key_fields, cursor,
        [](auto& groups, const std::uint64_t epoch, auto&& entries) {
            groups.try_emplace(epoch, std::move(entries));
        });
    value.log = std::move(groups);
    value.is_truncated = truncated;
}

void decode_reply(const ::ceph::buffer::list& input, ::cls::rgw::olh::reply& value)
{
    using groups_type = decltype(::cls::rgw::olh::reply::epochs);
    using entries_type = ::cls::rgw::olh::epoch_group::second_type;

    auto cursor = input.cbegin();
    auto [groups, truncated] = ::cls::rgw::olh_wire::decode_reply<groups_type, entries_type>(
        client_event_fields, client_key_fields, cursor,
        [](auto& groups, const std::uint64_t epoch, auto&& entries) {
            groups.emplace_back(epoch, std::move(entries));
        });
    // Native map decoding orders epochs and keeps the first value for a
    // duplicate key. Normal ascending responses avoid the sorting pass.
    const auto epoch = &::cls::rgw::olh::epoch_group::first;
    if (!std::ranges::is_sorted(groups, {}, epoch)) {
        std::ranges::stable_sort(groups, {}, epoch);
    }
    const auto duplicates = std::ranges::unique(groups, {}, epoch);
    groups.erase(duplicates.begin(), duplicates.end());
    value = {std::move(groups), truncated};
}

} // cls::rgw::olh_datum
