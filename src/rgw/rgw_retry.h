#pragma once

#include "rgw_sal.h"

// So! Now and then when we try to update bucket information, the
// bucket has changed during the course of the operation. (Or we have
// a cache consistency problem that Watch/Notify isn't ruling out
// completely.)
//
// When this happens, we need to update the bucket info and try
// again. We have, however, to try the right *part* again.  We can't
// simply re-send, since that will obliterate the previous update.
//
// Thus, callers of this function should include everything that
// merges information to be changed into the bucket information as
// well as the call to set it.
//
// The called function must return an integer, negative on error. In
// general, they should just return op_ret.
namespace {
template<typename F> /* XXX was getting an error binding the lambda to
		      * const, also reference F--how big an issue is
		      * taking F by value? */
int retry_raced_bucket_write(const DoutPrefixProvider *dpp, rgw::sal::Bucket* b,  F f /* XXX fu2? */) {
  auto r = f();
  for (auto i = 0u; i < 15u && r == -ECANCELED; ++i) {
    r = b->try_refresh_info(dpp, nullptr);
    if (r >= 0) {
      r = f();
    }
  }
  return r;
}
}
