#include "acconfig.h"

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/cyg_profile.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#endif

void __cyg_profile_func_enter(void *this_fn, void *call_site)
    __attribute__((no_instrument_function));

void __cyg_profile_func_exit(void *this_fn, void *call_site)
    __attribute__((no_instrument_function));


void __cyg_profile_func_enter(void *this_fn, void *call_site)
{
#ifdef WITH_LTTNG
    tracepoint(lttng_ust_cyg_profile, func_entry, this_fn, call_site);
#endif
}

void __cyg_profile_func_exit(void *this_fn, void *call_site)
{
#ifdef WITH_LTTNG
    tracepoint(lttng_ust_cyg_profile, func_exit, this_fn, call_site);
#endif
}

