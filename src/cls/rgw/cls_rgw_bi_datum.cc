// RGW CLS: Bind native client/module records to the same BI wire profile.
// Opaque payloads retain native backing storage; only metadata uses zpp spans.
// Native types and completion ownership never enter Datum's pure mechanisms.
#include "cls_rgw_bi_datum.h"
#include "cls_rgw_bi_wire.h"
#include "cls_rgw_bilog_datum.h"
#include "cls_rgw_bucket_datum.h"
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

namespace cls::rgw::bilog_datum {

using ::cls::rgw::wire::reader;
using ::cls::rgw::wire::writer;

void decode(list_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    list_request candidate;
    reader archive {cursor};
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        fields.text(candidate.marker);
        candidate.max = fields.template scalar<std::uint32_t>();

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void decode(trim_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    trim_request candidate;
    reader archive {cursor};
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        fields.text(candidate.start_marker);
        fields.text(candidate.end_marker);

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

::ceph::buffer::list encode_list(const std::string_view marker, const std::uint32_t maximum)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] {
        archive.text(marker);
        archive.scalar(maximum);
    });

    return output;
}

::ceph::buffer::list encode_trim(const std::string_view start, const std::string_view end)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] {
        archive.text(start);
        archive.text(end);
    });

    return output;
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    ::ceph::buffer::list candidate;
    writer archive {candidate};
    archive.frame(1, [&] {
        archive.scalar(::cls::rgw::wire::checked_count(value.entries.size()));
        for (const auto& entry : value.entries) {
            archive.native(entry);
        }
        archive.scalar(static_cast<std::uint8_t>(value.truncated));
    });
    output.claim_append(candidate);
}

template <typename EntriesT>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    reader archive {cursor};

    return archive.frame(1, [&](auto& fields, std::uint8_t) {
        // Even the shortest native entry needs its six-byte frame header.
        const auto count = fields.template sequence_size<6>(1);
        EntriesT entries;
        if constexpr (requires { entries.reserve(count); }) {
            entries.reserve(count);
        }
        for (std::uint32_t index = 0; index < count; ++index) {
            fields.native(entries.emplace_back());
        }
        const bool truncated = 0 != fields.template scalar<std::uint8_t>();

        return std::pair {std::move(entries), truncated};
    });
}

template std::pair<std::vector<::rgw_bi_log_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);
template std::pair<std::list<::rgw_bi_log_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bilog_datum

namespace cls::rgw::bucket_datum {

using ::cls::rgw::wire::reader;
using ::cls::rgw::wire::writer;

::ceph::buffer::list encode_request(const ::cls_rgw_obj_key& start, const std::string_view prefix,
                                    const std::string_view delimiter, const std::uint32_t maximum,
                                    const bool versions)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(6, 4, [&] {
        archive.scalar(maximum);
        archive.text(prefix);
        ::cls::rgw::wire::encode_key(archive, start.name, start.instance);
        archive.scalar(static_cast<std::uint8_t>(versions));
        archive.text(delimiter);
    });

    return output;
}

void decode(request& value, ::ceph::buffer::list::const_iterator& input)
{
    using key = ::cls_rgw_obj_key;
    namespace datum = ::ceph::salad::datum;

    auto cursor = input;
    request candidate;
    reader archive {cursor};
    archive.frame(6, 2, [&](auto& fields, const std::uint8_t version) {
        if (version < 4) {
            fields.text(candidate.start_obj.name);
        }
        candidate.num_entries = fields.template scalar<std::uint32_t>();
        if (version >= 3) {
            fields.text(candidate.filter_prefix);
        }
        if (version >= 4) {
            ::cls::rgw::wire::decode_key(fields, candidate.start_obj,
                datum::binary_fields<&key::name, &key::instance> {});
        }
        if (version >= 5) {
            candidate.list_versions = 0 != fields.template scalar<std::uint8_t>();
        }
        if (version >= 6) {
            fields.text(candidate.delimiter);
        }

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    ::ceph::buffer::list candidate;
    writer archive {candidate};
    archive.frame(4, 2, [&] {
        archive.frame(2, 2, [&] {
            archive.native(value.dir.header);
            archive.scalar(::cls::rgw::wire::checked_count(value.dir.m.size()));
            for (const auto& [key, entry] : value.dir.m) {
                archive.text(key);
                archive.native(entry);
            }
        });
        archive.scalar(static_cast<std::uint8_t>(value.is_truncated));
        ::cls::rgw::wire::encode_key(archive, value.marker.name, value.marker.instance);
    });
    output.claim_append(candidate);
}

template <typename EntriesT>
requires ::ceph::concepts::has_subscript_operator<EntriesT>
list_reply<EntriesT> decode_reply(const ::ceph::buffer::list& input)
{
    using key = ::cls_rgw_obj_key;
    namespace datum = ::ceph::salad::datum;

    auto cursor = input.cbegin();
    list_reply<EntriesT> candidate;
    reader archive {cursor};
    archive.frame(4, 2, [&](auto& fields, const std::uint8_t version) {
        fields.frame(2, 2, [&](auto& directory_fields, std::uint8_t) {
            directory_fields.native(candidate.dir.header);
            // A string count plus even an unframed entry needs five bytes.
            const auto count = directory_fields.template sequence_size<5>();
            auto& entries = candidate.dir.m;
            if constexpr (requires { entries.reserve(count); }) {
                entries.reserve(count);
            }
            for (std::uint32_t index = 0; index < count; ++index) {
                auto name = directory_fields.text();
                // Decode into an existing value on duplicates. Old leaf versions
                // can leave previously decoded fields unchanged, just as native.
                directory_fields.native(entries[std::move(name)]);
            }

            return 0;
        });
        candidate.is_truncated = 0 != fields.template scalar<std::uint8_t>();
        candidate.cls_filtered = version >= 3;
        if (version >= 4) {
            ::cls::rgw::wire::decode_key(fields, candidate.marker,
                datum::binary_fields<&key::name, &key::instance> {});
        }

        return 0;
    });

    return candidate;
}

template list_reply<flat_entries> decode_reply(const ::ceph::buffer::list& input);
template list_reply<std::map<std::string, ::rgw_bucket_dir_entry>>
decode_reply(const ::ceph::buffer::list& input);

namespace {

stat_associations decode_stats(reader& fields)
{
    static_assert(std::same_as<std::underlying_type_t<::RGWObjCategory>, std::uint8_t>);
    const auto count = fields.sequence_size<26>();
    stat_associations result;
    constexpr auto categories = 1u + std::numeric_limits<std::uint8_t>::max();
    result.reserve(std::min(count, categories));
    for (std::uint32_t index = 0; index < count; ++index) {
        const auto category = static_cast<::RGWObjCategory>(fields.scalar<std::uint8_t>());
        ::rgw_bucket_category_stats stats;
        fields.native(stats);
        const auto position = std::ranges::lower_bound(result, category, {}, &stat_association::first);
        if (position == result.end() || position->first != category) {
            result.emplace(position, category, std::move(stats));
        }
    }

    return result;
}

} // anonymous namespace

::ceph::buffer::list encode_stats(const bool absolute, const ::rgw_bucket_dir_stats& stats,
                                  const ::rgw_bucket_dir_stats *dec_stats)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(2, [&] {
        archive.scalar(static_cast<std::uint8_t>(absolute));
        archive.native(stats);
        if (dec_stats) {
            archive.native(*dec_stats);

            return;
        }
        archive.scalar(std::uint32_t {0});
    });

    return output;
}

::ceph::buffer::list encode_timeout(const std::uint64_t timeout)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] { archive.scalar(timeout); });

    return output;
}

void decode(stats_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    stats_request candidate;
    reader archive {cursor};
    archive.frame(2, [&](auto& fields, const std::uint8_t version) {
        candidate.absolute = 0 != fields.template scalar<std::uint8_t>();
        candidate.stats = decode_stats(fields);
        if (version >= 2) {
            candidate.dec_stats = decode_stats(fields);
        }

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void decode(timeout_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    timeout_request candidate;
    reader archive {cursor};
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        candidate.tag_timeout = fields.template scalar<std::uint64_t>();

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void encode(const check_reply& value, ::ceph::buffer::list& output)
{
    ::ceph::buffer::list candidate;
    writer archive {candidate};
    archive.frame(1, [&] {
        archive.native(value.existing_header);
        archive.native(value.calculated_header);
    });
    output.claim_append(candidate);
}

check_reply decode_check_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    check_reply candidate;
    reader archive {cursor};
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        fields.native(candidate.existing_header);
        fields.native(candidate.calculated_header);

        return 0;
    });

    return candidate;
}

} // cls::rgw::bucket_datum

namespace cls::rgw::write_datum {

namespace {

namespace datum = ::ceph::salad::datum;
using ::cls::rgw::wire::reader;
using ::cls::rgw::wire::writer;
using key = ::cls_rgw_obj_key;
constexpr auto key_fields = datum::binary_fields<&key::name, &key::instance> {};

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

void decode(prepare_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    prepare_request candidate;
    reader archive {cursor};
    archive.frame(7, 3, [&](auto& fields, const std::uint8_t version) {
        candidate.op = static_cast<::RGWModifyOp>(fields.template scalar<std::uint8_t>());
        if (version < 5) {
            fields.text(candidate.key.name);
        }
        fields.text(candidate.tag);
        if (version >= 2) {
            fields.text(candidate.locator);
        }
        if (version >= 4) {
            candidate.log_op = 0 != fields.template scalar<std::uint8_t>();
        }
        if (version >= 5) {
            ::cls::rgw::wire::decode_key(fields, candidate.key, key_fields);
        }
        if (version >= 6) {
            candidate.bilog_flags = fields.template scalar<std::uint16_t>();
        }
        if (version >= 7) {
            fields.native(candidate.zones_trace);
        }

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void decode(complete_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    complete_request candidate;
    reader archive {cursor};
    archive.frame(9, 3, [&](auto& fields, const std::uint8_t version) {
        candidate.op = static_cast<::RGWModifyOp>(fields.template scalar<std::uint8_t>());
        if (version < 7) {
            fields.text(candidate.key.name);
        }
        candidate.ver.epoch = fields.template scalar<std::uint64_t>();
        fields.native(candidate.meta);
        fields.text(candidate.op_tag);
        if (version >= 2) {
            fields.text(candidate.locator);
        }
        const bool names_only = version >= 4 && version < 7;
        const auto count = names_only ? fields.template sequence_size<4>()
                                      : fields.template sequence_size<14>();
        candidate.remove_objs.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) {
            auto& removed = candidate.remove_objs.emplace_back();
            if (names_only) {
                fields.text(removed.name);
            }
            if (!names_only) {
                ::cls::rgw::wire::decode_key(fields, removed, key_fields);
            }
        }
        if (version >= 5) {
            fields.native(candidate.ver);
        }
        if (version < 5) {
            candidate.ver.pool = -1;
        }
        if (version >= 6) {
            candidate.log_op = 0 != fields.template scalar<std::uint8_t>();
        }
        if (version >= 7) {
            ::cls::rgw::wire::decode_key(fields, candidate.key, key_fields);
        }
        if (version >= 8) {
            candidate.bilog_flags = fields.template scalar<std::uint16_t>();
        }
        if (version >= 9) {
            fields.native(candidate.zones_trace);
        }

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void decode(link_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    link_request candidate;
    reader archive {cursor};
    archive.frame(5, [&](auto& fields, const std::uint8_t version) {
        ::cls::rgw::wire::decode_key(fields, candidate.key, key_fields);
        fields.text(candidate.olh_tag);
        candidate.delete_marker = 0 != fields.template scalar<std::uint8_t>();
        fields.text(candidate.op_tag);
        fields.native(candidate.meta);
        candidate.olh_epoch = fields.template scalar<std::uint64_t>();
        candidate.log_op = 0 != fields.template scalar<std::uint8_t>();
        candidate.bilog_flags = fields.template scalar<std::uint16_t>();
        if (version >= 2) {
            const auto seconds = fields.template scalar<std::uint64_t>();
            if (version == 2) {
                candidate.unmod_since = ::ceph::real_clock::from_time_t(static_cast<time_t>(seconds));
            }
        }
        if (version >= 3) {
            fields.native(candidate.unmod_since);
        }
        if (version >= 4) {
            candidate.high_precision_time = 0 != fields.template scalar<std::uint8_t>();
        }
        if (version >= 5) {
            fields.native(candidate.zones_trace);
        }

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void decode(trim_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    trim_request candidate;
    reader archive {cursor};
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        ::cls::rgw::wire::decode_key(fields, candidate.olh, key_fields);
        candidate.ver = fields.template scalar<std::uint64_t>();
        fields.text(candidate.olh_tag);

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void decode(unlink_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    unlink_request candidate;
    reader archive {cursor};
    archive.frame(3, [&](auto& fields, const std::uint8_t version) {
        ::cls::rgw::wire::decode_key(fields, candidate.key, key_fields);
        fields.text(candidate.op_tag);
        candidate.olh_epoch = fields.template scalar<std::uint64_t>();
        candidate.log_op = 0 != fields.template scalar<std::uint8_t>();
        candidate.bilog_flags = fields.template scalar<std::uint16_t>();
        if (version >= 2) {
            fields.text(candidate.olh_tag);
        }
        if (version >= 3) {
            fields.native(candidate.zones_trace);
        }

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void decode(clear_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    clear_request candidate;
    reader archive {cursor};
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        ::cls::rgw::wire::decode_key(fields, candidate.key, key_fields);
        fields.text(candidate.olh_tag);

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

void decode(refresh_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    refresh_request candidate;
    reader archive {cursor};
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        ::cls::rgw::wire::decode_key(fields, candidate.key, key_fields);

        return 0;
    });
    value = std::move(candidate);
    input = cursor;
}

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

::ceph::buffer::list encode_unlink(const ::cls_rgw_obj_key& key, const std::string_view operation_tag,
                                   const std::string_view tag, const std::uint64_t epoch,
                                   const bool log, const std::uint16_t flags, const ::rgw_zone_set& zones)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(3, [&] {
        ::cls::rgw::wire::encode_key(archive, key.name, key.instance);
        archive.text(operation_tag);
        archive.scalar(epoch);
        archive.scalar(static_cast<std::uint8_t>(log));
        archive.scalar(flags);
        archive.text(tag);
        archive.native(zones);
    });

    return output;
}

::ceph::buffer::list encode_clear(const ::cls_rgw_obj_key& key, const std::string_view tag)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] {
        ::cls::rgw::wire::encode_key(archive, key.name, key.instance);
        archive.text(tag);
    });

    return output;
}

::ceph::buffer::list encode_refresh(const ::cls_rgw_obj_key& key)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] {
        ::cls::rgw::wire::encode_key(archive, key.name, key.instance);
    });

    return output;
}

} // cls::rgw::write_datum
