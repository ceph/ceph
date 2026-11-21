/*
 * Example live patch for ceph-mon, see doc/dev/livepatch.rst: a replacement
 * for OSDMonitor::_check_remove_pool() that refuses every pool removal.
 * qa/standalone/misc/livepatch.sh applies and reverts it.
 *
 *   build-patch.sh <build dir> lp_mon_block_pool_rm.cc ceph-mon \
 *     mon/OSDMonitor.cc '^_ZN10OSDMonitor18_check_remove_pool' \
 *     lp_OSDMonitor_check_remove_pool
 */
#include "mon/OSDMonitor.h"

extern "C" int lp_OSDMonitor_check_remove_pool(OSDMonitor *self,
                                               int64_t pool_id,
                                               const pg_pool_t& pool,
                                               std::ostream *ss)
{
  *ss << "pool '" << self->osdmap.get_pool_name(pool_id)
      << "': pool deletion is disabled by a live patch";
  return -EPERM;
}
