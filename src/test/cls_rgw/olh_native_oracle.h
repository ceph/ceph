// RGW CLS: C++23 native codecs used independently of the Datum wire binding.
#ifndef CEPH_TEST_CLS_RGW_OLH_NATIVE_ORACLE_H
 #define CEPH_TEST_CLS_RGW_OLH_NATIVE_ORACLE_H

#include "rgw/datum/adapters/cls/olh_types.h"
#include "include/buffer.h"

#include <cstdint>

#include <string>

namespace rgw_olh_oracle {

struct request final
{
    ::cls::rgw::olh::object_key key;
    std::string tag;

    std::uint64_t marker;
    bool stales;
};

::ceph::buffer::list encode_request(const request& value, std::uint8_t version = 2, bool extensions = false);
request decode_request(const ::ceph::buffer::list& bytes);
::ceph::buffer::list encode_reply(const ::cls::rgw::olh::reply& value, bool extensions = false);
::cls::rgw::olh::reply decode_reply(const ::ceph::buffer::list& bytes);

} // rgw_olh_oracle

#endif // CEPH_TEST_CLS_RGW_OLH_NATIVE_ORACLE_H
