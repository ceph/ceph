// RGW CLS: Bind independent client/module records to established wire profiles.
// Opaque payloads retain native backing storage; only metadata uses zpp spans.
// Native types and completion ownership never enter Datum's pure mechanisms.
#include "bi.h"
#include "bi_wire.h"
#include "bilog.h"
#include "bucket.h"
#include "lc.h"
#include "multipart.h"
#include "object.h"
#include "reshard.h"
#include "usage.h"
#include "write.h"

#include <bit>
#include <utility>

namespace cls::rgw::bi_datum {

namespace datum = ::ceph::salad::datum;
using ::ceph::datum_wire::decode_record;

namespace {

using native_entry = ::rgw_cls_bi_entry;

constexpr auto native_entry_fields = datum::binary_fields<&native_entry::type, &native_entry::idx, &native_entry::data> {};
constexpr auto module_entry_fields = datum::binary_fields<&entry::type, &entry::idx, &entry::data> {};

} // anonymous namespace

::ceph::buffer::list encode_request(const std::uint32_t maximum, const std::string_view filter,
                                    const std::string_view marker, const bool reshard)
{
    return ::cls::rgw::bi_wire::encode_request(maximum, filter, marker, reshard);
}

::ceph::buffer::list encode_request(const reply_format format, const std::uint32_t maximum,
                                    const std::string_view filter, const std::string_view marker, const bool reshard)
{
    if (reply_format::compact == format) {
        // Old modules accept this compatible request and return framed replies.
        return ::cls::rgw::bi_wire::encode_request<3>(maximum, filter, marker, reshard);
    }

    return encode_request(maximum, filter, marker, reshard);
}

::ceph::buffer::list encode_get_request(const ::cls_rgw_obj_key& key, const ::BIIndexType type)
{
    return ::cls::rgw::bi_wire::encode_get_request(key.name, key.instance, std::to_underlying(type));
}

void decode(get_request& value, ::ceph::buffer::list::const_iterator& input)
{
    using key = ::cls_rgw_obj_key;

    decode_record(value, input, [](auto& cursor, auto& candidate) {
        candidate.type = static_cast<::BIIndexType>(::cls::rgw::bi_wire::decode_get_request(
            datum::binary_fields<&key::name, &key::instance> {}, cursor, candidate.key));
    });
}

void encode(const entry& value, ::ceph::buffer::list& output)
{
    auto candidate = ::cls::rgw::bi_wire::encode_entry_message(module_entry_fields, value);
    output.claim_append(candidate);
}

::rgw_cls_bi_entry decode_get_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    ::rgw_cls_bi_entry candidate;
    ::cls::rgw::bi_wire::decode_entry_message(native_entry_fields, cursor, candidate);

    return candidate;
}

void decode(request& value, ::ceph::buffer::list::const_iterator& input)
{
    const auto version = ::cls::rgw::bi_wire::decode_request(
        datum::binary_fields<&request::max, &request::name_filter, &request::marker, &request::reshardlog> {},
        input, value);
    value.format = version >= 3 ? reply_format::compact : reply_format::framed;
}

void decode(entry& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record(value, input, [](auto& cursor, auto& candidate) {
        ::cls::rgw::bi_wire::decode_entry(module_entry_fields, cursor, candidate);
    });
}

void decode(put_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record(value, input, [](auto& cursor, auto& candidate) {
        auto [entries, check_existing] = ::cls::rgw::bi_wire::decode_entries<std::vector<entry>>(
            module_entry_fields, cursor);
        candidate.entries = std::move(entries);
        candidate.check_existing = check_existing;
    });
}

void decode(entry_put_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record(value, input, [](auto& cursor, auto& candidate) {
        ::cls::rgw::bi_wire::decode_entry_message(module_entry_fields, cursor, candidate.item);
    });
}

::ceph::buffer::list encode_put_entry(const ::rgw_cls_bi_entry& value)
{
    return ::cls::rgw::bi_wire::encode_entry_message(native_entry_fields, value);
}

::ceph::buffer::list encode_put_request(const std::vector<::rgw_cls_bi_entry>& entries,
                                        const bool check_existing)
{
    return ::cls::rgw::bi_wire::encode_entries(native_entry_fields, entries, check_existing);
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    auto candidate = ::cls::rgw::bi_wire::encode_entries(module_entry_fields, value.entries, value.is_truncated);
    output.claim_append(candidate);
}

void encode(const reply_format format, const reply& value, ::ceph::buffer::list& output)
{
    if (reply_format::compact == format) {
        auto candidate = ::cls::rgw::bi_wire::encode_entries<2>(module_entry_fields, value.entries, value.is_truncated);
        output.claim_append(candidate);

        return;
    }
    encode(value, output);
}

template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    std::vector<::rgw_cls_bi_entry>, std::list<::rgw_cls_bi_entry>>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();

    return ::cls::rgw::bi_wire::decode_entries<EntriesT, 2>(native_entry_fields, cursor);
}

template std::pair<std::vector<::rgw_cls_bi_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);
template std::pair<std::list<::rgw_cls_bi_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bi_datum

namespace cls::rgw::bilog_datum {

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::reader;
using ::ceph::datum_wire::encode_frame;

void decode(list_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.text(candidate.marker);
        candidate.max = fields.template scalar<std::uint32_t>();
    });
}

void decode(trim_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.text(candidate.start_marker);
        fields.text(candidate.end_marker);
    });
}

::ceph::buffer::list encode_list(const std::string_view marker, const std::uint32_t maximum)
{
    return encode_frame(1, [&](auto& archive) {
        archive.text(marker);
        archive.scalar(maximum);
    });
}

::ceph::buffer::list encode_trim(const std::string_view start, const std::string_view end)
{
    return encode_frame(1, [&](auto& archive) {
        archive.text(start);
        archive.text(end);
    });
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    auto candidate = encode_frame(1, [&](auto& archive) {
        archive.scalar(::ceph::datum_wire::checked_count(value.entries.size()));
        for (const auto& entry : value.entries) {
            archive.native(entry);
        }
        archive.scalar(static_cast<std::uint8_t>(value.truncated));
    });
    output.claim_append(candidate);
}

template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    std::vector<::rgw_bi_log_entry>, std::list<::rgw_bi_log_entry>>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    reader archive {cursor};

    return archive.frame(1, [&](auto& fields, std::uint8_t) {
        auto entries = fields.template native<
            ::ceph::encoding_detail::struct_header_len(), EntriesT>(1);
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

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::reader;
using ::ceph::datum_wire::encode_frame;

::ceph::buffer::list encode_request(const ::cls_rgw_obj_key& start, const std::string_view prefix,
                                    const std::string_view delimiter, const std::uint32_t maximum,
                                    const bool versions)
{
    return encode_frame(6, 4, [&](auto& archive) {
        archive.scalar(maximum);
        archive.text(prefix);
        ::rgw::datum_wire::encode_key(archive, start.name, start.instance);
        archive.scalar(static_cast<std::uint8_t>(versions));
        archive.text(delimiter);
    });
}

void decode(request& value, ::ceph::buffer::list::const_iterator& input)
{
    using key = ::cls_rgw_obj_key;
    namespace datum = ::ceph::salad::datum;

    decode_record<6, 2>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        if (version < 4) {
            fields.text(candidate.start_obj.name);
        }
        candidate.num_entries = fields.template scalar<std::uint32_t>();
        if (version >= 3) {
            fields.text(candidate.filter_prefix);
        }
        if (version >= 4) {
            ::rgw::datum_wire::decode_key(datum::binary_fields<&key::name, &key::instance> {}, fields, candidate.start_obj);
        }
        if (version >= 5) {
            candidate.list_versions = 0 != fields.template scalar<std::uint8_t>();
        }
        if (version >= 6) {
            fields.text(candidate.delimiter);
        }
    });
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    auto candidate = encode_frame(4, 2, [&](auto& archive) {
        archive.frame(2, 2, [&] {
            archive.native(value.dir.header);
            archive.scalar(::ceph::datum_wire::checked_count(value.dir.m.size()));
            for (const auto& [key, entry] : value.dir.m) {
                archive.text(key);
                archive.native(entry);
            }
        });
        archive.scalar(static_cast<std::uint8_t>(value.is_truncated));
        ::rgw::datum_wire::encode_key(archive, value.marker.name, value.marker.instance);
    });
    output.claim_append(candidate);
}

template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    flat_entries, std::map<std::string, ::rgw_bucket_dir_entry>>
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
            // A row needs its key-length prefix and at least the entry's version byte.
            const auto count = directory_fields.template sequence_size<
                sizeof(std::uint32_t) + sizeof(std::uint8_t)>();
            auto& entries = candidate.dir.m;
            ::ceph::util::maybe_reserve(entries, count);
            for (std::uint32_t index = 0; index < count; ++index) {
                auto name = directory_fields.text();
                // Decode into an existing value on duplicates. Old leaf versions
                // can leave previously decoded fields unchanged, just as native.
                directory_fields.native(entries[std::move(name)]);
            }
        });
        candidate.is_truncated = 0 != fields.template scalar<std::uint8_t>();
        candidate.cls_filtered = version >= 3;
        if (version >= 4) {
            ::rgw::datum_wire::decode_key(datum::binary_fields<&key::name, &key::instance> {}, fields, candidate.marker);
        }
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
    // Stats v1 is unframed: version byte, total_size, total_size_rounded, and num_entries.
    constexpr auto minimum_stats_size = sizeof(std::uint8_t) + 3 * sizeof(std::uint64_t);
    const auto count = fields.sequence_size<sizeof(::RGWObjCategory) + minimum_stats_size>();
    stat_associations result;
    // First-wins insertion can retain at most the 256 distinct uint8 category values.
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
    return encode_frame(2, [&](auto& archive) {
        archive.scalar(static_cast<std::uint8_t>(absolute));
        archive.native(stats);
        if (dec_stats) {
            archive.native(*dec_stats);

            return;
        }
        archive.scalar(std::uint32_t {0});
    });
}

::ceph::buffer::list encode_timeout(const std::uint64_t timeout)
{
    return encode_frame(1, [&](auto& archive) { archive.scalar(timeout); });
}

void decode(stats_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<2>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        candidate.absolute = 0 != fields.template scalar<std::uint8_t>();
        candidate.stats = decode_stats(fields);
        if (version >= 2) {
            candidate.dec_stats = decode_stats(fields);
        }
    });
}

void decode(timeout_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        candidate.tag_timeout = fields.template scalar<std::uint64_t>();
    });
}

void encode(const check_reply& value, ::ceph::buffer::list& output)
{
    auto candidate = encode_frame(1, [&](auto& archive) {
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
    });

    return candidate;
}

} // cls::rgw::bucket_datum

namespace cls::rgw::write_datum {

namespace {

namespace datum = ::ceph::salad::datum;
using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::reader;
using ::ceph::datum_wire::encode_frame;
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
    decode_record<7, 3>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
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
            ::rgw::datum_wire::decode_key(key_fields, fields, candidate.key);
        }
        if (version >= 6) {
            candidate.bilog_flags = fields.template scalar<std::uint16_t>();
        }
        if (version >= 7) {
            fields.native(candidate.zones_trace);
        }
    });
}

void decode(complete_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<9, 3>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
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
        const auto count = names_only ? fields.template sequence_size<sizeof(std::uint32_t)>()
            : fields.template sequence_size<::rgw::datum_wire::minimum_key_size>();
        candidate.remove_objs.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) {
            auto& removed = candidate.remove_objs.emplace_back();
            if (names_only) {
                fields.text(removed.name);
            }
            if (!names_only) {
                ::rgw::datum_wire::decode_key(key_fields, fields, removed);
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
            ::rgw::datum_wire::decode_key(key_fields, fields, candidate.key);
        }
        if (version >= 8) {
            candidate.bilog_flags = fields.template scalar<std::uint16_t>();
        }
        if (version >= 9) {
            fields.native(candidate.zones_trace);
        }
    });
}

void decode(link_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<5>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        ::rgw::datum_wire::decode_key(key_fields, fields, candidate.key);
        fields.text(candidate.olh_tag);
        candidate.delete_marker = 0 != fields.template scalar<std::uint8_t>();
        fields.text(candidate.op_tag);
        fields.native(candidate.meta);
        candidate.olh_epoch = fields.template scalar<std::uint64_t>();
        candidate.log_op = 0 != fields.template scalar<std::uint8_t>();
        candidate.bilog_flags = fields.template scalar<std::uint16_t>();
        if (version >= 2) {
            // Version 3 added the native timestamp after the old seconds field;
            // the old field remains on the wire but only version 2 uses its value.
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
    });
}

void decode(trim_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        ::rgw::datum_wire::decode_key(key_fields, fields, candidate.olh);
        candidate.ver = fields.template scalar<std::uint64_t>();
        fields.text(candidate.olh_tag);
    });
}

void decode(unlink_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<3>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        ::rgw::datum_wire::decode_key(key_fields, fields, candidate.key);
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
    });
}

void decode(clear_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        ::rgw::datum_wire::decode_key(key_fields, fields, candidate.key);
        fields.text(candidate.olh_tag);
    });
}

void decode(refresh_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        ::rgw::datum_wire::decode_key(key_fields, fields, candidate.key);
    });
}

::ceph::buffer::list encode_prepare(const ::RGWModifyOp operation, const std::string_view tag,
                                    const ::cls_rgw_obj_key& key, const std::string_view locator)
{
    return encode_frame(7, 5, [&](auto& archive) {
        using prefix = prepare_prefix;

        archive.fields(datum::binary_fields<&prefix::operation, &prefix::tag, &prefix::locator, &prefix::log> {},
                       prefix {static_cast<std::uint8_t>(operation), tag, locator, 0});
        ::rgw::datum_wire::encode_key(archive, key.name, key.instance);
        archive.scalar(std::uint16_t {0});
        archive.scalar(std::uint32_t {0}); // Empty compatibility zone set.
    });
}

::ceph::buffer::list encode(const complete_view& value)
{
    const std::list<::cls_rgw_obj_key> empty_keys;
    const ::rgw_zone_set empty_zones;

    return encode_frame(9, 7, [&](auto& archive) {
        using prefix = complete_prefix;

        archive.fields(datum::binary_fields<&prefix::operation, &prefix::epoch> {},
                       prefix {static_cast<std::uint8_t>(value.operation), value.version.epoch});
        archive.native(value.metadata);
        archive.text(value.operation_tag);
        archive.text(value.locator);
        archive.native(value.removed_keys ? *value.removed_keys : empty_keys);
        archive.native(value.version);
        archive.scalar(static_cast<std::uint8_t>(value.log));
        ::rgw::datum_wire::encode_key(archive, value.key.name, value.key.instance);
        archive.scalar(value.bilog_flags);
        archive.native(value.zones_trace ? *value.zones_trace : empty_zones);
    });
}

::ceph::buffer::list encode(const link_view& value)
{
    const ::rgw_bucket_dir_entry_meta empty_metadata;

    return encode_frame(5, [&](auto& archive) {
        ::rgw::datum_wire::encode_key(archive, value.key.name, value.key.instance);
        archive.text(value.olh_tag);
        archive.scalar(static_cast<std::uint8_t>(value.delete_marker));
        archive.text(value.operation_tag);
        archive.native(value.metadata ? *value.metadata : empty_metadata);
        archive.scalar(value.olh_epoch);
        archive.scalar(static_cast<std::uint8_t>(value.log));
        archive.scalar(std::uint16_t {0}); // The existing public link builder has no flags argument.
        archive.scalar(static_cast<std::uint64_t>(
            ::ceph::real_clock::to_time_t(value.unmodified_since)));
        archive.native(value.unmodified_since);
        archive.scalar(static_cast<std::uint8_t>(value.high_precision_time));
        archive.native(value.zones_trace);
    });
}

::ceph::buffer::list encode_trim(const ::cls_rgw_obj_key& key, const std::uint64_t version,
                                 const std::string_view tag)
{
    return encode_frame(1, [&](auto& archive) {
        ::rgw::datum_wire::encode_key(archive, key.name, key.instance);
        archive.scalar(version);
        archive.text(tag);
    });
}

::ceph::buffer::list encode_unlink(const ::cls_rgw_obj_key& key, const std::string_view operation_tag,
                                   const std::string_view tag, const std::uint64_t epoch,
                                   const bool log, const std::uint16_t flags, const ::rgw_zone_set& zones)
{
    return encode_frame(3, [&](auto& archive) {
        ::rgw::datum_wire::encode_key(archive, key.name, key.instance);
        archive.text(operation_tag);
        archive.scalar(epoch);
        archive.scalar(static_cast<std::uint8_t>(log));
        archive.scalar(flags);
        archive.text(tag);
        archive.native(zones);
    });
}

::ceph::buffer::list encode_clear(const ::cls_rgw_obj_key& key, const std::string_view tag)
{
    return encode_frame(1, [&](auto& archive) {
        ::rgw::datum_wire::encode_key(archive, key.name, key.instance);
        archive.text(tag);
    });
}

::ceph::buffer::list encode_refresh(const ::cls_rgw_obj_key& key)
{
    return encode_frame(1, [&](auto& archive) {
        ::rgw::datum_wire::encode_key(archive, key.name, key.instance);
    });
}

} // cls::rgw::write_datum

namespace cls::rgw::object_datum {

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::encode_frame;
using ::ceph::datum_wire::writer;

::ceph::buffer::list encode_remove(const std::list<std::string>& prefixes)
{
    ::ceph::buffer::list output;
    writer archive {output};
    if (prefixes.empty()) {
        using record = ::ceph::datum_wire::scalar_record<std::uint32_t>;

        archive.frame(::ceph::salad::datum::binary_fields<&record::value> {}, 1, record {0});

        return output;
    }
    archive.frame(1, [&] { archive.native(prefixes); });

    return output;
}

::ceph::buffer::list encode_version(const std::string_view attribute)
{
    return encode_frame(1, [&](auto& archive) { archive.text(attribute); });
}

::ceph::buffer::list encode_prefix(const std::string_view prefix, const bool fail_if_exist)
{
    return encode_frame(1, [&](auto& archive) {
        archive.text(prefix);
        archive.scalar(static_cast<std::uint8_t>(fail_if_exist));
    });
}

::ceph::buffer::list encode_mtime(const ::ceph::real_time mtime, const bool precise,
                                  const std::uint8_t type)
{
    return encode_frame(2, [&](auto& archive) {
        archive.native(mtime);
        archive.scalar(type);
        archive.scalar(static_cast<std::uint8_t>(precise));
    });
}

void decode(remove_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        candidate.keep_attr_prefixes = fields.template sequence<4, decltype(candidate.keep_attr_prefixes)>(
            [](auto& fields, auto& prefix) { fields.text(prefix); });
    });
}

void decode(version_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.text(candidate.attr);
    });
}

void decode(prefix_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.text(candidate.check_prefix);
        candidate.fail_if_exist = 0 != fields.template scalar<std::uint8_t>();
    });
}

void decode(mtime_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<2>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        fields.native(candidate.mtime);
        candidate.type = fields.template scalar<std::uint8_t>();
        if (version >= 2) {
            candidate.high_precision_time = 0 != fields.template scalar<std::uint8_t>();
        }
    });
}

} // cls::rgw::object_datum

namespace cls::rgw::usage_datum {

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::reader;
using ::ceph::datum_wire::encode_frame;

// v1: owner/bucket length prefixes, epoch, and total_usage's bytes_sent, bytes_received,
// ops, and successful_ops; usage_map, payer, and s3select_usage were added later.
constexpr auto minimum_entry_size = ::ceph::encoding_detail::struct_header_len() +
    2 * sizeof(std::uint32_t) + 5 * sizeof(std::uint64_t);

::ceph::buffer::list encode_add(const std::vector<::rgw_usage_log_entry>& entries)
{
    return encode_frame(2, [&](auto& archive) {
        archive.frame(1, [&] { archive.native(entries); });
        archive.scalar(std::uint32_t {0}); // The public builder leaves the unused user empty.
    });
}

::ceph::buffer::list encode_read(const std::string_view owner, const std::string_view bucket,
                                 const std::uint64_t start, const std::uint64_t finish,
                                 const std::uint32_t maximum, const std::string_view marker)
{
    return encode_frame(2, [&](auto& archive) {
        archive.scalar(start);
        archive.scalar(finish);
        archive.text(owner);
        archive.text(marker);
        archive.scalar(maximum);
        archive.text(bucket);
    });
}

::ceph::buffer::list encode_trim(const std::string_view user, const std::string_view bucket,
                                 const std::uint64_t start, const std::uint64_t finish)
{
    return encode_frame(3, 2, [&](auto& archive) {
        archive.scalar(start);
        archive.scalar(finish);
        archive.text(user);
        archive.text(bucket);
    });
}

void decode(add_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<2>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        fields.frame(1, [&](auto& sequence, std::uint8_t) {
            candidate.info.entries = sequence.template native<
                minimum_entry_size, decltype(candidate.info.entries)>();
        });
        if (version >= 2) {
            fields.skip_text(); // The native handler never observes this user field.
        }
    });
}

void decode(read_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<2>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        candidate.start_epoch = fields.template scalar<std::uint64_t>();
        candidate.end_epoch = fields.template scalar<std::uint64_t>();
        fields.text(candidate.owner);
        fields.text(candidate.iter);
        candidate.max_entries = fields.template scalar<std::uint32_t>();
        if (version >= 2) {
            fields.text(candidate.bucket);
        }
    });
}

void decode(trim_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<3>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        candidate.start_epoch = fields.template scalar<std::uint64_t>();
        candidate.end_epoch = fields.template scalar<std::uint64_t>();
        fields.text(candidate.user);
        if (version >= 3) {
            fields.text(candidate.bucket);
        }
    });
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    auto candidate = encode_frame(1, [&](auto& archive) {
        archive.native(value.usage);
        archive.scalar(static_cast<std::uint8_t>(value.truncated));
        archive.text(value.next_iter);
    });
    output.claim_append(candidate);
}

template <typename EntriesT>
requires ::ceph::concepts::same_as_any<
    EntriesT, native_entries,
    ::boost::container::flat_map<::rgw_user_bucket, ::rgw_usage_log_entry>>
read_reply<EntriesT> decode_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    read_reply<EntriesT> candidate;
    reader archive {cursor};
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        // Association keys contain user and bucket strings, each with a length prefix.
        constexpr auto minimum_key_size =
            ::ceph::encoding_detail::struct_header_len() + 2 * sizeof(std::uint32_t);
        // The reply ends with truncated and the next_iter string.
        constexpr auto tail_size = sizeof(std::uint8_t) + sizeof(std::uint32_t);
        const auto count = fields.template sequence_size<
            minimum_key_size + minimum_entry_size>(tail_size);
        auto& entries = candidate.usage;
        ::ceph::util::maybe_reserve(entries, count);
        for (std::uint32_t index = 0; index < count; ++index) {
            ::rgw_user_bucket key;
            ::rgw_usage_log_entry entry;
            fields.native(key);
            fields.native(entry);
            entries.try_emplace(std::move(key), std::move(entry));
        }
        candidate.truncated = 0 != fields.template scalar<std::uint8_t>();
        fields.text(candidate.next_iter);
    });

    return candidate;
}

template read_reply<native_entries> decode_reply(const ::ceph::buffer::list& input);
template read_reply<::boost::container::flat_map<::rgw_user_bucket, ::rgw_usage_log_entry>>
decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::usage_datum

namespace cls::rgw::reshard_datum {

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::reader;
using ::ceph::datum_wire::encode_frame;

::ceph::buffer::list encode_add(const ::cls_rgw_reshard_entry& entry, const bool create_only)
{
    return encode_frame(2, [&](auto& archive) {
        archive.native(entry);
        archive.scalar(static_cast<std::uint8_t>(create_only));
    });
}

::ceph::buffer::list encode_entry(const ::cls_rgw_reshard_entry& entry)
{
    return encode_frame(1, [&](auto& archive) { archive.native(entry); });
}

::ceph::buffer::list encode_list(const std::string_view marker, const std::uint32_t maximum)
{
    return encode_frame(1, [&](auto& archive) {
        archive.scalar(maximum);
        archive.text(marker);
    });
}

::ceph::buffer::list encode_remove(const ::cls_rgw_reshard_entry& entry)
{
    return encode_frame(1, [&](auto& archive) {
        archive.text(entry.tenant);
        archive.text(entry.bucket_name);
        archive.text(entry.bucket_id);
    });
}

::ceph::buffer::list encode_instance(const ::cls_rgw_bucket_instance_entry& entry)
{
    return encode_frame(1, [&](auto& archive) { archive.native(entry); });
}

::ceph::buffer::list encode_empty()
{
    return encode_frame(1, [](auto&) {});
}

::ceph::buffer::list encode_guard(const int error)
{
    static_assert(sizeof(int) == sizeof(std::int32_t));
    return encode_frame(1, [&](auto& archive) {
        archive.scalar(std::bit_cast<std::uint32_t>(error));
    });
}

void decode(add_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<2>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        fields.native(candidate.entry);
        if (version >= 2) {
            candidate.create_only = 0 != fields.template scalar<std::uint8_t>();
        }
    });
}

void decode(entry_message& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.native(candidate.entry);
    });
}

void decode(list_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        candidate.max = fields.template scalar<std::uint32_t>();
        fields.text(candidate.marker);
    });
}

void decode(remove_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.text(candidate.tenant);
        fields.text(candidate.bucket_name);
        fields.text(candidate.bucket_id);
    });
}

void decode(instance_message& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.native(candidate.entry);
    });
}

void decode(empty_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto&, auto&, std::uint8_t) {});
}

void decode(guard_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        candidate.ret_err = std::bit_cast<std::int32_t>(fields.template scalar<std::uint32_t>());
    });
}

void encode(const entry_message& value, ::ceph::buffer::list& output)
{
    auto candidate = encode_entry(value.entry);
    output.claim_append(candidate);
}

void encode(const instance_message& value, ::ceph::buffer::list& output)
{
    auto candidate = encode_instance(value.entry);
    output.claim_append(candidate);
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    auto candidate = encode_frame(1, [&](auto& archive) {
        archive.native(value.entries);
        archive.scalar(static_cast<std::uint8_t>(value.is_truncated));
    });
    output.claim_append(candidate);
}

::cls_rgw_reshard_entry decode_entry(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    entry_message candidate;
    decode(candidate, cursor);

    return std::move(candidate.entry);
}

::cls_rgw_bucket_instance_entry decode_instance(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    instance_message candidate;
    decode(candidate, cursor);

    return candidate.entry;
}

template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    std::vector<::cls_rgw_reshard_entry>, std::list<::cls_rgw_reshard_entry>>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    reader archive {cursor};
    return archive.frame(1, [&](auto& fields, std::uint8_t) {
        // v2 has tenant, bucket_name, and bucket_id strings; v1 also has new_instance_id.
        // v3 adds initiator, so v2 is the shortest accepted entry.
        constexpr auto minimum_entry_size = ::ceph::encoding_detail::struct_header_len() +
            ::ceph::datum_wire::encoded_time_size + 3 * sizeof(std::uint32_t) +
            sizeof(::cls_rgw_reshard_entry::old_num_shards) +
            sizeof(::cls_rgw_reshard_entry::new_num_shards);
        auto entries = fields.template native<minimum_entry_size, EntriesT>(1);
        const bool truncated = 0 != fields.template scalar<std::uint8_t>();

        return std::pair {std::move(entries), truncated};
    });
}

template std::pair<std::vector<::cls_rgw_reshard_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);
template std::pair<std::list<::cls_rgw_reshard_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::reshard_datum

namespace cls::rgw::lc_datum {

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::encode_frame;

::ceph::buffer::list encode(const std::string_view marker)
{
    return encode_frame(1, [&](auto& archive) { archive.text(marker); });
}

::ceph::buffer::list encode(const ::cls_rgw_lc_entry& entry)
{
    return encode_frame(2, 2, [&](auto& archive) { archive.native(entry); });
}

::ceph::buffer::list encode(const ::cls_rgw_lc_obj_head& head)
{
    return encode_frame(1, [&](auto& archive) { archive.native(head); });
}

::ceph::buffer::list encode(const std::string_view marker, const std::uint32_t maximum)
{
    return encode_frame(3, [&](auto& archive) {
        archive.text(marker);
        archive.scalar(maximum);
    });
}

void decode(marker_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.text(candidate.marker);
    });
}

void decode(entry_message& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<2>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        if (version < 2) {
            fields.text(candidate.entry.bucket);
            candidate.entry.status = fields.template scalar<std::uint32_t>();

            return;
        }
        fields.native(candidate.entry);
    });
}

void decode(head_message& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.native(candidate.head);
    });
}

void decode(list_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<3>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        candidate.compat_v = version;
        fields.text(candidate.marker);
        candidate.max_entries = fields.template scalar<std::uint32_t>();
    });
}

void encode(const entry_message& value, ::ceph::buffer::list& output)
{
    auto candidate = encode(value.entry);
    output.claim_append(candidate);
}

void encode(const head_message& value, ::ceph::buffer::list& output)
{
    auto candidate = encode(value.head);
    output.claim_append(candidate);
}

void encode(const list_reply& value, ::ceph::buffer::list& output)
{
    // The request version selects the reply shape: through v2 it is a bucket/status map,
    // while v3 carries full entries. Keep first-wins map insertion for the older shape.
    auto candidate = encode_frame(value.compat_v, [&](auto& archive) {
        if (value.compat_v <= 2) {
            std::map<std::string_view, int> old_entries;
            for (const auto& entry : value.entries) {
                old_entries.emplace(entry.bucket, static_cast<int>(entry.status));
            }
            archive.native(old_entries);
        }
        if (value.compat_v > 2) {
            archive.native(value.entries);
        }
        // Native v0/1 encoders also emit this flag, although those decoders skip it.
        archive.scalar(static_cast<std::uint8_t>(value.is_truncated));
    });
    output.claim_append(candidate);
}

} // cls::rgw::lc_datum

namespace cls::rgw::multipart_datum {

::ceph::buffer::list encode_update(const std::string_view part_key, const ::RGWUploadPartInfo& info)
{
    // The established client copies info into a fresh operation. Manifest's
    // custom copy omits head_placement_rule; preserve that normalization while
    // borrowing the other fields. Borrowing the raw info would change the wire.
    const ::RGWObjManifest manifest {info.manifest};
    ::ceph::buffer::list output;
    ::ceph::datum_wire::writer archive {output};
    archive.frame(1, [&] {
        archive.text(part_key);
        archive.frame(7, 2, [&] {
            archive.scalar(info.num);
            archive.scalar(info.size);
            archive.text(info.etag);
            archive.native(info.modified);
            archive.native(manifest);
            archive.native(info.cs_info);
            archive.scalar(info.accounted_size);
            archive.native(info.past_prefixes);
            archive.native(info.cksum);
            archive.text(info.crypt_salt);
        });
    });

    return output;
}

void decode(update_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    ::ceph::datum_wire::reader archive {cursor};
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        fields.text(value.part_key);
        fields.native(value.info);
    });
    input = cursor;
}

} // cls::rgw::multipart_datum
