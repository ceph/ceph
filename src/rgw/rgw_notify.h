// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include "common/ceph_time.h"
#include "rgw_notify_event_type.h"

// forward declarations
class CephContext;
namespace rgw::sal {
    class RGWRadosStore;
}
class RGWRados;
class req_state;

namespace rgw::notify {

// publish notification
int publish(const req_state* s, 
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        rgw::sal::RGWRadosStore* store);

}

