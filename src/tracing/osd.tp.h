
#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER osd

#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "./osd.tp.h"
#undef TRACEPOINT_INCLUDE_FILE
#define TRACEPOINT_INCLUDE_FILE ./osd.tp.h

#if !defined(OSD_TP_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define OSD_TP_H

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(osd, prepare_tx_enter,
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

TRACEPOINT_EVENT(osd, prepare_tx_exit,
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

TRACEPOINT_EVENT(osd, ms_fast_dispatch,
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

TRACEPOINT_EVENT(osd, opwq_process_start,
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

TRACEPOINT_EVENT(osd, opwq_process_finish,
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

#endif /* OSD_TP_H */

#include <lttng/tracepoint-event.h>
