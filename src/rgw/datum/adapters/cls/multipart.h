// RGW CLS: Multipart records independent of native public operation wrappers.
#ifndef CEPH_CLS_RGW_MULTIPART_DATUM_H
 #define CEPH_CLS_RGW_MULTIPART_DATUM_H

#include "rgw/rgw_basic_types.h"

#include <string>
#include <string_view>

namespace cls::rgw::multipart_datum {

struct update_request final
{
    std::string part_key;
    ::RGWUploadPartInfo info;
};

::ceph::buffer::list encode_update(std::string_view part_key, const ::RGWUploadPartInfo& info);
// Decode into a local request in place. Native Manifest copies change encoded
// fields, so publishing a candidate through copy/move assignment is incorrect.
// The cursor commits after success; the value may be partial after an error.
void decode(update_request& value, ::ceph::buffer::list::const_iterator& input);

} // cls::rgw::multipart_datum

#endif // CEPH_CLS_RGW_MULTIPART_DATUM_H
