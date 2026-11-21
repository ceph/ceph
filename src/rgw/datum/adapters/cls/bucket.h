// RGW CLS: Bucket directory messages independent of owning native op/res types.
// Associative bindings preserve sorted keys and native last-wins decoding.
#ifndef CEPH_CLS_RGW_BUCKET_DATUM_H
 #define CEPH_CLS_RGW_BUCKET_DATUM_H

#include "bucket_types.h"
#include "common/container_concepts.h"

#include <cstdint>
#include <utility>

#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::bucket_datum {

struct request final
{
    cls_rgw_obj_key start_obj;
    std::string filter_prefix;
    std::string delimiter;

    std::uint32_t num_entries = 0;
    bool list_versions = false;
};

using stat_association = std::pair<RGWObjCategory, rgw_bucket_category_stats>;
using stat_associations = std::vector<stat_association>;

struct stats_request final
{
    stat_associations stats;
    stat_associations dec_stats;

    bool absolute = false;
};

struct timeout_request final
{
    std::uint64_t tag_timeout = 0;
};

struct check_reply final
{
    rgw_bucket_dir_header existing_header;
    rgw_bucket_dir_header calculated_header;
};

struct check_page_request final
{
    rgw_cls_index_cursor cursor;
    std::uint32_t max_rows = RGW_CLS_INDEX_PAGE_ROWS;
    std::uint64_t max_bytes = RGW_CLS_INDEX_PAGE_BYTES;
};

struct check_page_reply final
{
    rgw_cls_index_cursor cursor;
    rgw_bucket_dir_header existing_header;
    rgw_bucket_dir_stats stats;
};

struct rebuild_request final
{
    rgw_cls_index_cursor cursor;
    rgw_bucket_dir_stats stats;
};

ceph::buffer::list encode_stats(bool absolute, const rgw_bucket_dir_stats& stats,
                                  const rgw_bucket_dir_stats *dec_stats);
ceph::buffer::list encode_timeout(std::uint64_t timeout);
void decode(stats_request& value, ceph::buffer::list::const_iterator& input);
void decode(timeout_request& value, ceph::buffer::list::const_iterator& input);
void encode(const check_reply& value, ceph::buffer::list& output);
check_reply decode_check_reply(const ceph::buffer::list& input);

ceph::buffer::list encode_check(const rgw_cls_index_cursor& cursor,
                                  std::uint32_t max_rows, std::uint64_t max_bytes);
void decode(check_page_request& value, ceph::buffer::list::const_iterator& input);
void encode(const check_page_reply& value, ceph::buffer::list& output);
check_page_reply decode_check_page(const ceph::buffer::list& input);
ceph::buffer::list encode_rebuild(const rgw_cls_index_cursor& cursor,
                                    const rgw_bucket_dir_stats& stats);
void decode(rebuild_request& value, ceph::buffer::list::const_iterator& input);

ceph::buffer::list encode_request(const cls_rgw_obj_key& start, std::string_view prefix,
                                    std::string_view delimiter, std::uint32_t maximum, bool versions);
void decode(request& value, ceph::buffer::list::const_iterator& input);
void encode(const reply& value, ceph::buffer::list& output);

// Flat-map and std::map instantiations share subscript/last-wins semantics.
template <typename EntriesT>
requires ceph::concepts::same_as_any<EntriesT,
                                    flat_entries, std::map<std::string, rgw_bucket_dir_entry>>
list_reply<EntriesT> decode_reply(const ceph::buffer::list& input);

// Validate the complete listing while retaining native partial header updates on errors.
void decode_header_reply(const ceph::buffer::list& input, rgw_bucket_dir_header& header);

} // cls::rgw::bucket_datum

#endif // CEPH_CLS_RGW_BUCKET_DATUM_H
