// RGW CLS: Bucket directory messages independent of owning native op/res types.
// Associative bindings preserve sorted keys and native last-wins decoding.
#ifndef CEPH_CLS_RGW_BUCKET_DATUM_H
 #define CEPH_CLS_RGW_BUCKET_DATUM_H

#include "cls/rgw/cls_rgw_types.h"
#include "common/container_concepts.h"

#include <cstdint>

#include <string>
#include <string_view>

namespace cls::rgw::bucket_datum {

struct request final {
    ::cls_rgw_obj_key start_obj;
    std::string filter_prefix;
    std::string delimiter;

    std::uint32_t num_entries = 0;
    bool list_versions = false;
};

template <typename EntriesT>
struct directory final {
    ::rgw_bucket_dir_header header;
    EntriesT m;
};

template <typename EntriesT>
struct list_reply final {
    directory<EntriesT> dir;
    ::cls_rgw_obj_key marker;

    bool is_truncated = false;
    bool cls_filtered = true;
};

using flat_entries = ::boost::container::flat_map<std::string, ::rgw_bucket_dir_entry>;
using reply = list_reply<flat_entries>;

::ceph::buffer::list encode_request(const ::cls_rgw_obj_key& start, std::string_view prefix,
                                    std::string_view delimiter, std::uint32_t maximum, bool versions);
void decode(request& value, ::ceph::buffer::list::const_iterator& input);
void encode(const reply& value, ::ceph::buffer::list& output);

// Flat-map and std::map instantiations share subscript/last-wins semantics.
template <typename EntriesT>
requires ::ceph::concepts::has_subscript_operator<EntriesT>
list_reply<EntriesT> decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bucket_datum

#endif // CEPH_CLS_RGW_BUCKET_DATUM_H
