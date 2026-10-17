// RGW CLS: Shared registration/loader boundary for actual-module tests.
#ifndef CEPH_TEST_CLS_RGW_MODULE_LOADER_H
 #define CEPH_TEST_CLS_RGW_MODULE_LOADER_H

#include "objclass/objclass.h"

#include <string_view>

namespace rgw_cls_test {

struct registered_method final
{
    ::cls_method_cxx_call_t call;
    int flags;
};

registered_method get_method(std::string_view name);

} // rgw_cls_test

#endif // CEPH_TEST_CLS_RGW_MODULE_LOADER_H
