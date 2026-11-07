// RGW CLS: Independent bucket-index log records and native client bindings.
// The counted entry encoding permits module vectors and public client lists.
#ifndef CEPH_CLS_RGW_BILOG_DATUM_H
 #define CEPH_CLS_RGW_BILOG_DATUM_H

#include "cls/rgw/cls_rgw_types.h"
#include "common/container_concepts.h"

#include <cstdint>
#include <utility>

#include <list>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::bilog_datum {

struct list_request final
{
    std::string marker;

    std::uint32_t max = 0;
};

struct trim_request final
{
    std::string start_marker;
    std::string end_marker;
};

struct reply final
{
    std::vector<::rgw_bi_log_entry> entries;

    bool truncated = false;
};

void decode(list_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(trim_request& value, ::ceph::buffer::list::const_iterator& input);
void encode(const reply& value, ::ceph::buffer::list& output);

::ceph::buffer::list encode_list(std::string_view marker, std::uint32_t maximum);
::ceph::buffer::list encode_trim(std::string_view start, std::string_view end);

// Native log-entry codecs remain leaf bindings; the outer record is independent.
template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    std::vector<::rgw_bi_log_entry>, std::list<::rgw_bi_log_entry>>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bilog_datum

#endif // CEPH_CLS_RGW_BILOG_DATUM_H
