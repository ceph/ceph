// RGW CLS: C++23 boundary for the private Datum OLH implementation.
#ifndef CEPH_CLS_RGW_OLH_DATUM_H
 #define CEPH_CLS_RGW_OLH_DATUM_H

#include "cls_rgw_ops.h"
#include "cls_rgw_olh_types.h"
#include "include/buffer.h"

#include <cstdint>

#include <optional>
#include <ranges>
#include <string_view>

namespace cls::rgw::olh_datum {

struct request final
{
    ::cls_rgw_obj_key olh;

    std::uint64_t ver_marker = 0;
    bool get_stales = false;

private:
    // Keep the tag's backing bytes alive without exposing mutable input.
    ::ceph::buffer::ptr tag_bytes;

    friend void decode(request&, ::ceph::buffer::list::const_iterator&);

public:
    std::string_view tag() const noexcept
    {
        return {tag_bytes.length() ? tag_bytes.c_str() : "", tag_bytes.length()};
    }

};

using stored_log = decltype(::rgw_bucket_olh_entry::pending_log);
using log_page = std::ranges::subrange<stored_log::const_iterator, stored_log::const_iterator,
                                      std::ranges::subrange_kind::sized>;

::ceph::buffer::list encode_request(const ::cls_rgw_obj_key& key, std::uint64_t marker,
                                    std::string_view tag, bool stales);
void decode(request& value, ::ceph::buffer::list::const_iterator& input);
void encode_reply(log_page groups, bool stales, bool truncated, ::ceph::buffer::list& output);
// True-stales reads can retain canonical encoded events. An empty optional
// requests native decoding; otherwise the status is success or tag mismatch.
std::optional<int> encode_stored_reply(const ::ceph::buffer::list& input, std::uint64_t marker,
                                       std::string_view tag, ::ceph::buffer::list& output);
void decode_reply(const ::ceph::buffer::list& input, ::rgw_cls_read_olh_log_ret& value);
void decode_reply(const ::ceph::buffer::list& input, ::cls::rgw::olh::reply& value);

} // cls::rgw::olh_datum

#endif // CEPH_CLS_RGW_OLH_DATUM_H
