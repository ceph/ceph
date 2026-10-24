// Shared native encoding and physical-fragment fixtures for RGW-owned CLS tests.
#ifndef CEPH_TEST_CLS_RGW_WIRE_TEST_H
 #define CEPH_TEST_CLS_RGW_WIRE_TEST_H

#include "include/encoding.h"

#include <cstddef>
#include <algorithm>

namespace rgw_cls_test {

inline ::ceph::buffer::list canonical(const auto& value)
{
    using ::ceph::encode;

    ::ceph::buffer::list output;
    encode(value, output);

    return output;
}

inline ::ceph::buffer::list fragmented(const ::ceph::buffer::list& input, const unsigned stride = 1)
{
    ::ceph::buffer::list output;
    const auto storage = input.to_str();
    for (std::size_t offset = 0; offset < storage.size(); offset += stride) {
        const auto size = std::min<std::size_t>(stride, storage.size() - offset);
        output.append(::ceph::buffer::copy(storage.data() + offset, size));
    }

    return output;
}

} // rgw_cls_test

#endif // CEPH_TEST_CLS_RGW_WIRE_TEST_H
