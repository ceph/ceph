#include <iostream>
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/lock/cls_lock_client.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_rados.h"
#include "include/types.h"
#include "include/atomic.h"
#include "auth/Crypto.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "common/Cond.h"
#include "common/Clock.h"
#include "common/Thread.h"
#include "rgw_bl.h"

using namespace librados;

static string bl_index_lock_name = "bl_process";

void RGWBucketloggingConfiguration::add_rule(BLRule *rule)
{
    if (rule != NULL) {
        blrule[string("Status")] = string("Enabled");
        blrule[string("TargetBucket")] = rule->get_targetbucket();
        blrule[string("TargetPrefix")] = rule->get_prefix();
    }else {
        blrule[string("Status")] = string("Disabled");
    }
}

