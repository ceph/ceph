#ifndef CEPH_PERF_COUNTER_ENTRY_H
#define CEPH_PERF_COUNTER_ENTRY_H

#include <stdint.h>

/** Counter kind tags for ceph_perf_counter_entry::type. */
#define CEPH_PERF_KIND_U64  0  /**< integer counter/gauge                    */
#define CEPH_PERF_KIND_TIME 1  /**< time average; value stored in nanoseconds */

/** Maximum length of a counter name, including the NUL terminator. */
#define CEPH_PERF_NAME_LEN 32

/**
 * One entry in the perf counters array.
 * Plain-old-data: value, type, and inline name string.
 */
struct ceph_perf_counter_entry {
  int64_t value;                   /**< counter value; ns for TIME counters  */
  uint8_t type;                    /**< CEPH_PERF_KIND_U64 or _TIME          */
  uint8_t reserved[7];
  char    name[CEPH_PERF_NAME_LEN];/**< NUL-terminated counter name          */
};

#define CEPH_PERF_COUNTERS_MAX 256

#endif 