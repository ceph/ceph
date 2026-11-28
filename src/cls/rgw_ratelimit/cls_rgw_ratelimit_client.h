#ifndef CEPH_CLS_RGW_RATELIMIT_CLIENT_H
#define CEPH_CLS_RGW_RATELIMIT_CLIENT_H

#include "cls/rgw_ratelimit/cls_rgw_ratelimit_ops.h"
#include "include/rados/librados.hpp"
#include "rgw_ratelimit_core.h"

namespace cls::rgw::ratelimit {

int consume(librados::IoCtx* ioctx,
            const std::string& oid,
            const std::string& key,
            OpType op_type,
            const RGWRateLimitInfo& info,
            ceph::timespan ts,
            int64_t interval,
            int64_t* delay);

int giveback(librados::IoCtx* ioctx,
             const std::string& oid,
             const std::string& key,
             OpType op_type);

int decrease_bytes(librados::IoCtx* ioctx,
                   const std::string& oid,
                   const std::string& key,
                   bool is_read,
                   int64_t amount,
                   const RGWRateLimitInfo& info);

} // namespace cls::rgw::ratelimit

#endif
