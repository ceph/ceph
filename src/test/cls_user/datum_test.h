// RGW user CLS fixtures shared by independent wire and actual-boundary tests.
#ifndef CEPH_TEST_CLS_USER_DATUM_TEST_H
 #define CEPH_TEST_CLS_USER_DATUM_TEST_H

#include "test/cls_rgw/wire_test.h"
#include "cls/user/cls_user_ops.h"

namespace user_cls_test {

using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;

inline auto bucket_entry(const std::string& name = "bucket")
{
    ::cls_user_bucket_entry value;
    value.bucket.name = name;
    value.bucket.marker = std::string(80, 'm');
    value.bucket.bucket_id = "bucket-id";
    value.bucket.placement_id = "placement";
    value.size = 37;
    value.size_rounded = 4096;
    value.count = 3;
    value.creation_time = ::ceph::real_time {std::chrono::seconds {123456}};

    return value;
}

inline auto resource(const std::string& name = "Resource", const std::string& path = "/path/")
{
    ::cls_user_account_resource value;
    value.name = name;
    value.path = path;
    value.metadata.append("opaque\0metadata", 15);

    return value;
}

} // user_cls_test

#endif // CEPH_TEST_CLS_USER_DATUM_TEST_H
