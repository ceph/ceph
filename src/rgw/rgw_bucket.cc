#include <errno.h>

#include <string>

#include "common/errno.h"
#include "rgw_access.h"

#include "rgw_bucket.h"
#include "rgw_tools.h"

static rgw_bucket pi_buckets(BUCKETS_POOL_NAME);

static string pool_name_prefix = "p";
