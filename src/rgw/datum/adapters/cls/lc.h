// RGW CLS: Lifecycle records independent of native public operation wrappers.
#ifndef CEPH_CLS_RGW_LC_DATUM_H
 #define CEPH_CLS_RGW_LC_DATUM_H

#include "cls/rgw/cls_rgw_types.h"

#include <cstdint>

#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::lc_datum {

struct marker_request final
{
    std::string marker;
};

struct entry_message final
{
    ::cls_rgw_lc_entry entry;
};

struct head_message final
{
    ::cls_rgw_lc_obj_head head;
};

struct list_request final
{
    std::string marker;
    std::uint32_t max_entries = 0;
    std::uint8_t compat_v = 3;
};

struct list_reply final
{
    std::vector<::cls_rgw_lc_entry> entries;
    bool is_truncated = false;
    std::uint8_t compat_v = 3;
};

::ceph::buffer::list encode(std::string_view marker);
::ceph::buffer::list encode(const ::cls_rgw_lc_entry& entry);
::ceph::buffer::list encode(const ::cls_rgw_lc_obj_head& head);
::ceph::buffer::list encode(std::string_view marker, std::uint32_t maximum);
void encode(const entry_message& value, ::ceph::buffer::list& output);
void encode(const head_message& value, ::ceph::buffer::list& output);
void encode(const list_reply& value, ::ceph::buffer::list& output);

void decode(marker_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(entry_message& value, ::ceph::buffer::list::const_iterator& input);
void decode(head_message& value, ::ceph::buffer::list::const_iterator& input);
void decode(list_request& value, ::ceph::buffer::list::const_iterator& input);

} // cls::rgw::lc_datum

#endif // CEPH_CLS_RGW_LC_DATUM_H
