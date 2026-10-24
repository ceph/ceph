// RGW CLS: Bucket directory messages independent of owning native op/res types.
// Associative bindings preserve sorted keys and native last-wins decoding.
#ifndef CEPH_CLS_RGW_BUCKET_DATUM_H
 #define CEPH_CLS_RGW_BUCKET_DATUM_H

#include "cls/rgw/cls_rgw_types.h"
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
    ::cls_rgw_obj_key start_obj;
    std::string filter_prefix;
    std::string delimiter;

    std::uint32_t num_entries = 0;
    bool list_versions = false;
};

template <typename EntriesT>
struct directory final
{
    ::rgw_bucket_dir_header header;

    EntriesT m;
};

template <typename EntriesT>
struct list_reply final
{
    directory<EntriesT> dir;
    ::cls_rgw_obj_key marker;

    bool is_truncated = false;
    bool cls_filtered = true;
};

using flat_entries = ::boost::container::flat_map<std::string, ::rgw_bucket_dir_entry>;
using reply = list_reply<flat_entries>;

using stat_association = std::pair<::RGWObjCategory, ::rgw_bucket_category_stats>;
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
    ::rgw_bucket_dir_header existing_header;
    ::rgw_bucket_dir_header calculated_header;
};

::ceph::buffer::list encode_stats(bool absolute, const ::rgw_bucket_dir_stats& stats,
                                  const ::rgw_bucket_dir_stats *dec_stats);
::ceph::buffer::list encode_timeout(std::uint64_t timeout);
void decode(stats_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(timeout_request& value, ::ceph::buffer::list::const_iterator& input);
void encode(const check_reply& value, ::ceph::buffer::list& output);
check_reply decode_check_reply(const ::ceph::buffer::list& input);

::ceph::buffer::list encode_request(const ::cls_rgw_obj_key& start, std::string_view prefix,
                                    std::string_view delimiter, std::uint32_t maximum, bool versions);
void decode(request& value, ::ceph::buffer::list::const_iterator& input);
void encode(const reply& value, ::ceph::buffer::list& output);

// Flat-map and std::map instantiations share subscript/last-wins semantics.
template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    flat_entries, std::map<std::string, ::rgw_bucket_dir_entry>>
list_reply<EntriesT> decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bucket_datum

#endif // CEPH_CLS_RGW_BUCKET_DATUM_H
