#ifndef RADOSGW_ADMIN_H 
 #define RADOSGW_ADMIN_H 1

/*
 * Copyright (C) 2024 IBM
*/

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <cerrno>
#include <string>
#include <sstream>
#include <optional>
#include <iostream>

extern "C" {
#include <liboath/oath.h>
}

#include <fmt/format.h>

#include "auth/Crypto.h"
#include "compressor/Compressor.h"

#include "common/async/context_pool.h"

#include "common/armor.h"
#include "common/ceph_json.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/fault_injector.h"

#include "include/util.h"

#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/2pc_queue/cls_2pc_queue_types.h"
#include "cls/2pc_queue/cls_2pc_queue_client.h"

#include "include/utime.h"
#include "include/str_list.h"

#include "rgw_user.h"
#include "rgw_otp.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_datalog.h"
#include "rgw_lc.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "rgw_orphan.h"
#include "rgw_sync.h"
#include "rgw_trim_bilog.h"
#include "rgw_trim_datalog.h"
#include "rgw_trim_mdlog.h"
#include "rgw_data_sync.h"
#include "rgw_rest_conn.h"
#include "rgw_realm_watcher.h"
#include "rgw_role.h"
#include "rgw_reshard.h"
#include "rgw_http_client_curl.h"
#include "rgw_zone.h"
#include "rgw_pubsub.h"
#include "rgw_bucket_sync.h"
#include "rgw_sync_checkpoint.h"
#include "rgw_lua.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_data_access.h"
#include "rgw_account.h"
#include "rgw_bucket_logging.h"

#include "services/svc_sync_modules.h"
#include "services/svc_cls.h"
#include "services/svc_bilog_rados.h"
#include "services/svc_mdlog.h"
#include "services/svc_user.h"
#include "services/svc_zone.h"

#include "driver/rados/rgw_bucket.h"
#include "driver/rados/rgw_sal_rados.h"

#endif
