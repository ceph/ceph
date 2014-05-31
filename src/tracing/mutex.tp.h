
#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER mutex

#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "./mutex.tp.h"

#if !defined(MUTEX_TP_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define MUTEX_TP_H

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(mutex, lock_enter,
    TP_ARGS(
        const void *, addr,
        const char *, name),
    TP_FIELDS(
        ctf_integer_hex(unsigned long, addr, addr)
        ctf_string(name, name)
    )
)

TRACEPOINT_EVENT(mutex, lock_exit,
    TP_ARGS(
        const void *, addr,
        const char *, name),
    TP_FIELDS(
        ctf_integer_hex(unsigned long, addr, addr)
        ctf_string(name, name)
    )
)

TRACEPOINT_EVENT(mutex, unlock,
    TP_ARGS(
        const void *, addr,
        const char *, name),
    TP_FIELDS(
        ctf_integer_hex(unsigned long, addr, addr)
        ctf_string(name, name)
    )
)

#endif /* MUTEX_TP_H */

#include <lttng/tracepoint-event.h>
