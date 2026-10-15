// RGW CLS: C++23 boundary for the private Datum OLH implementation.
#ifndef CEPH_CLS_RGW_OLH_DATUM_H
 #define CEPH_CLS_RGW_OLH_DATUM_H

#include "cls_rgw_ops.h"
#include "cls_rgw_olh_types.h"

#include <cstdint>

#include <string>
#include <string_view>

namespace cls::rgw::olh_datum
{
struct request final
{
    ::cls_rgw_obj_key olh;
    std::string olh_tag;

    std::uint64_t ver_marker = 0;
    bool get_stales = false;
};

using stored_log = decltype(::rgw_bucket_olh_entry::pending_log);

::ceph::buffer::list encode_request(const ::cls_rgw_obj_key& key, std::uint64_t marker,
                                    std::string_view tag, bool stales);
void decode(request& value, ::ceph::buffer::list::const_iterator& input);
void encode_reply(stored_log::const_iterator lower, stored_log::const_iterator upper, bool stales,
                  bool truncated, ::ceph::buffer::list& output);
void decode_reply(const ::ceph::buffer::list& input, ::rgw_cls_read_olh_log_ret& value);
void decode_reply(const ::ceph::buffer::list& input, ::cls::rgw::olh::reply& value);

} // cls::rgw::olh_datum

#endif // CEPH_CLS_RGW_OLH_DATUM_H
