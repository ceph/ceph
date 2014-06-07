
#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER pg

#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "./pg.tp.h"

#if !defined(PG_TP_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define PG_TP_H

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(pg, queue_op,
    TP_ARGS(
        // osd_reqid_t
        uint8_t,  type,
        int64_t,  num,
        uint64_t, tid,
        int32_t,  inc),
    TP_FIELDS(
        ctf_integer(uint8_t, type, type)
        ctf_integer(int64_t, num, num)
        ctf_integer(uint64_t, tid, tid)
        ctf_integer(int32_t, inc, inc)
    )
)

#endif /* PG_TP_H */

#include <lttng/tracepoint-event.h>
