// RGW CLS: Bind native client/module records to the same BI wire profile.
// Opaque payloads retain native backing storage; only metadata uses zpp spans.
// Native types and completion ownership never enter Datum's pure mechanisms.
#include "cls_rgw_bi_datum.h"
#include "cls_rgw_bi_wire.h"
#include "cls_rgw_write_datum.h"

#include <utility>

namespace cls::rgw::bi_datum
{
namespace datum = ::ceph::salad::datum;

namespace
{
using native_entry = ::rgw_cls_bi_entry;
constexpr auto native_entry_fields = datum::binary_fields<&native_entry::type, &native_entry::idx, &native_entry::data> {};
constexpr auto module_entry_fields = datum::binary_fields<&entry::type, &entry::idx, &entry::data> {};

} // anonymous namespace

::ceph::buffer::list encode_request(const std::uint32_t maximum, const std::string_view filter,
                                    const std::string_view marker, const bool reshard)
{
    return ::cls::rgw::bi_wire::encode_request(maximum, filter, marker, reshard);
}

::ceph::buffer::list encode_get_request(const ::cls_rgw_obj_key& key, const ::BIIndexType type)
{
    return ::cls::rgw::bi_wire::encode_get_request(key.name, key.instance, std::to_underlying(type));
}

void decode(get_request& value, ::ceph::buffer::list::const_iterator& input)
{
    using key = ::cls_rgw_obj_key;

    auto cursor = input;
    get_request candidate;
    candidate.type = static_cast<::BIIndexType>(::cls::rgw::bi_wire::decode_get_request(
        cursor, candidate.key, datum::binary_fields<&key::name, &key::instance> {}));
    value = std::move(candidate);
    input = cursor;
}

void encode_get_reply(const entry& value, ::ceph::buffer::list& output)
{
    auto candidate = ::cls::rgw::bi_wire::encode_entry_message(value, module_entry_fields);
    output.claim_append(candidate);
}

::rgw_cls_bi_entry decode_get_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    ::rgw_cls_bi_entry candidate;
    ::cls::rgw::bi_wire::decode_entry_message(cursor, candidate, native_entry_fields);

    return candidate;
}

void decode(request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    auto candidate = ::cls::rgw::bi_wire::decode_request(cursor,
        [](const std::uint32_t maximum, std::string&& filter, std::string&& marker, const bool reshard) {
            return request {maximum, std::move(filter), std::move(marker), reshard};
        });
    value = std::move(candidate);
    input = cursor;
}

void decode(entry& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    entry candidate;
    ::cls::rgw::bi_wire::decode_entry(cursor, candidate, module_entry_fields);
    value = std::move(candidate);
    input = cursor;
}

void decode(put_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    auto [entries, check_existing] = ::cls::rgw::bi_wire::decode_entries<std::vector<entry>>(cursor, module_entry_fields);
    value = {std::move(entries), check_existing};
    input = cursor;
}

void decode(entry_put_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    entry_put_request candidate;
    ::cls::rgw::bi_wire::decode_entry_message(cursor, candidate.item, module_entry_fields);
    value = std::move(candidate);
    input = cursor;
}

::ceph::buffer::list encode_put_entry(const ::rgw_cls_bi_entry& value)
{
    return ::cls::rgw::bi_wire::encode_entry_message(value, native_entry_fields);
}

::ceph::buffer::list encode_put_request(const std::vector<::rgw_cls_bi_entry>& entries,
                                        const bool check_existing)
{
    return ::cls::rgw::bi_wire::encode_entries(entries, check_existing, native_entry_fields);
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    auto candidate = ::cls::rgw::bi_wire::encode_entries(value.entries, value.is_truncated, module_entry_fields);
    output.claim_append(candidate);
}

template <typename EntriesT>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();

    return ::cls::rgw::bi_wire::decode_entries<EntriesT>(cursor, native_entry_fields);
}

template std::pair<std::vector<::rgw_cls_bi_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);
template std::pair<std::list<::rgw_cls_bi_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bi_datum

namespace cls::rgw::write_datum {

namespace {

namespace datum = ::ceph::salad::datum;
using ::cls::rgw::wire::writer;

struct prepare_prefix final
{
    std::uint8_t operation;
    std::string_view tag;
    std::string_view locator;

    std::uint8_t log;
};

struct complete_prefix final
{
    std::uint8_t operation;
    std::uint64_t epoch;
};

} // anonymous namespace

::ceph::buffer::list encode_prepare(const ::RGWModifyOp operation, const std::string_view tag,
                                    const ::cls_rgw_obj_key& key, const std::string_view locator)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(7, 5, [&] {
        using prefix = prepare_prefix;

        archive.fields(::cls::rgw::wire::checked_count(10ull + tag.size() + locator.size()),
            prefix {static_cast<std::uint8_t>(operation), tag, locator, 0},
            datum::binary_fields<&prefix::operation, &prefix::tag, &prefix::locator, &prefix::log> {});
        ::cls::rgw::wire::encode_key(archive, key.name, key.instance);
        archive.scalar(std::uint16_t {0});
        archive.scalar(std::uint32_t {0}); // Empty compatibility zone set.
    });

    return output;
}

::ceph::buffer::list encode_complete(const ::RGWModifyOp operation, const std::string_view tag,
                                     const ::rgw_bucket_entry_ver& version,
                                     const ::cls_rgw_obj_key& key,
                                     const ::rgw_bucket_dir_entry_meta& metadata,
                                     const std::list<::cls_rgw_obj_key> *removed,
                                     const bool log, const std::uint16_t flags,
                                     const ::rgw_zone_set *zones, const std::string_view locator)
{
    const std::list<::cls_rgw_obj_key> empty_keys;
    const ::rgw_zone_set empty_zones;
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(9, 7, [&] {
        using prefix = complete_prefix;

        archive.fields(9, prefix {static_cast<std::uint8_t>(operation), version.epoch},
            datum::binary_fields<&prefix::operation, &prefix::epoch> {});
        archive.native(metadata);
        archive.text(tag);
        archive.text(locator);
        archive.native(removed ? *removed : empty_keys);
        archive.native(version);
        archive.scalar(static_cast<std::uint8_t>(log));
        ::cls::rgw::wire::encode_key(archive, key.name, key.instance);
        archive.scalar(flags);
        archive.native(zones ? *zones : empty_zones);
    });

    return output;
}

::ceph::buffer::list encode_link(const ::cls_rgw_obj_key& key,
                                 const ::ceph::buffer::list& tag, const bool deleted,
                                 const std::string_view operation_tag,
                                 const ::rgw_bucket_dir_entry_meta *metadata,
                                 const std::uint64_t epoch, const ::ceph::real_time unmodified,
                                 const bool precise, const bool log, const ::rgw_zone_set& zones)
{
    const ::rgw_bucket_dir_entry_meta empty_metadata;
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(5, [&] {
        ::cls::rgw::wire::encode_key(archive, key.name, key.instance);
        archive.text(tag);
        archive.scalar(static_cast<std::uint8_t>(deleted));
        archive.text(operation_tag);
        archive.native(metadata ? *metadata : empty_metadata);
        archive.scalar(epoch);
        archive.scalar(static_cast<std::uint8_t>(log));
        archive.scalar(std::uint16_t {0}); // The existing public link builder has no flags argument.
        archive.scalar(static_cast<std::uint64_t>(::ceph::real_clock::to_time_t(unmodified)));
        archive.native(unmodified);
        archive.scalar(static_cast<std::uint8_t>(precise));
        archive.native(zones);
    });

    return output;
}

::ceph::buffer::list encode_trim(const ::cls_rgw_obj_key& key, const std::uint64_t version,
                                 const std::string_view tag)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] {
        ::cls::rgw::wire::encode_key(archive, key.name, key.instance);
        archive.scalar(version);
        archive.text(tag);
    });

    return output;
}

} // cls::rgw::write_datum
