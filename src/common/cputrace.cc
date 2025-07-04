#include "cputrace.h"
#include "common/Formatter.h"

#include <linux/perf_event.h>
#include <asm/unistd.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <thread>
#include <atomic>

#define PROFILE_ASSERT(x) if (!(x)) { fprintf(stderr, "Assert failed %s:%d\n", __FILE__, __LINE__); exit(1); }

static thread_local uint64_t thread_id_hash;
static thread_local bool thread_id_initialized;
static cputrace_profiler g_profiler;
static HW_ctx* active_contexts[CPUTRACE_MAX_ANCHORS][CPUTRACE_MAX_THREADS] = {{nullptr}};

static long perf_event_open(struct perf_event_attr* hw_event, pid_t pid,
                           int cpu, int group_fd, unsigned long flags) {
    return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
}

static Arena* arena_create(size_t size) {
    void* start = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    PROFILE_ASSERT(start != MAP_FAILED);
    ArenaRegion* region = (ArenaRegion*)malloc(sizeof(ArenaRegion));
    region->start = start;
    region->end = (char*)start + size;
    region->current = start;
    region->next = nullptr;
    Arena* a = (Arena*)malloc(sizeof(Arena));
    a->region = region;
    return a;
}

static void* arena_alloc(Arena* arena, size_t size) {
    if ((char*)arena->region->current + size > (char*)arena->region->end) {
        fprintf(stderr, "Arena allocation failed: insufficient space for %zu bytes\n", size);
        return nullptr;
    }
    void* ptr = arena->region->current;
    arena->region->current = (char*)arena->region->current + size;
    return ptr;
}

static void arena_reset(Arena* arena) {
    arena->region->current = arena->region->start;
}

static void arena_destroy(Arena* arena) {
    munmap(arena->region->start, (char*)arena->region->end - (char*)arena->region->start);
    free(arena->region);
    free(arena);
}

static uint64_t get_thread_id() {
    if (!thread_id_initialized) {
        uint64_t tid = pthread_self();
        for (int i = 0; i < 8; i++)
            tid = (tid << 7) ^ (tid >> 3);
        thread_id_hash = tid % CPUTRACE_MAX_THREADS;
        thread_id_initialized = true;
    }
    return thread_id_hash;
}

static void HW_init(struct HW_ctx* ctx, struct HW_conf* conf) {
    ctx->fd_swi = -1;
    ctx->fd_cyc = -1;
    ctx->fd_cmiss = -1;
    ctx->fd_bmiss = -1;
    ctx->fd_ins = -1;
    ctx->conf = *conf;
}

static void HW_start(struct HW_ctx* ctx) {
    struct perf_event_attr pe;

    if (ctx->conf.capture_swi) {
        if (ctx->fd_swi == -1) {
            memset(&pe, 0, sizeof(pe));
            pe.size = sizeof(pe);
            pe.type = PERF_TYPE_SOFTWARE;
            pe.config = PERF_COUNT_SW_CONTEXT_SWITCHES;
            pe.disabled = 1;
            ctx->fd_swi = perf_event_open(&pe, gettid(), -1, -1, 0);
            if (ctx->fd_swi != -1) {
                ioctl(ctx->fd_swi, PERF_EVENT_IOC_RESET, 0);
                ioctl(ctx->fd_swi, PERF_EVENT_IOC_ENABLE, 0);
            } else {
                ctx->conf.capture_swi = false;
                fprintf(stderr, "Failed to open perf event for SWI: %s\n", strerror(errno));
            }
        } else {
            ioctl(ctx->fd_swi, PERF_EVENT_IOC_RESET, 0);
        }
    }

    if (ctx->conf.capture_cyc) {
        if (ctx->fd_cyc == -1) {
            memset(&pe, 0, sizeof(pe));
            pe.size = sizeof(pe);
            pe.type = PERF_TYPE_HARDWARE;
            pe.config = PERF_COUNT_HW_CPU_CYCLES;
            pe.disabled = 1;
            pe.exclude_kernel = 1;
            pe.exclude_hv = 1;
            ctx->fd_cyc = perf_event_open(&pe, gettid(), -1, -1, 0);
            if (ctx->fd_cyc != -1) {
                ioctl(ctx->fd_cyc, PERF_EVENT_IOC_RESET, 0);
                ioctl(ctx->fd_cyc, PERF_EVENT_IOC_ENABLE, 0);
            } else {
                ctx->conf.capture_cyc = false;
                fprintf(stderr, "Failed to open perf event for CYC: %s\n", strerror(errno));
            }
        } else {
            ioctl(ctx->fd_cyc, PERF_EVENT_IOC_RESET, 0);
        }
    }

    if (ctx->conf.capture_cmiss) {
        if (ctx->fd_cmiss == -1) {
            memset(&pe, 0, sizeof(pe));
            pe.size = sizeof(pe);
            pe.type = PERF_TYPE_HARDWARE;
            pe.config = PERF_COUNT_HW_CACHE_MISSES;
            pe.disabled = 1;
            pe.exclude_kernel = 1;
            pe.exclude_hv = 1;
            ctx->fd_cmiss = perf_event_open(&pe, gettid(), -1, -1, 0);
            if (ctx->fd_cmiss != -1) {
                ioctl(ctx->fd_cmiss, PERF_EVENT_IOC_RESET, 0);
                ioctl(ctx->fd_cmiss, PERF_EVENT_IOC_ENABLE, 0);
            } else {
                ctx->conf.capture_cmiss = false;
                fprintf(stderr, "Failed to open perf event for CMISS: %s\n", strerror(errno));
            }
        } else {
            ioctl(ctx->fd_cmiss, PERF_EVENT_IOC_RESET, 0);
        }
    }

    if (ctx->conf.capture_bmiss) {
        if (ctx->fd_bmiss == -1) {
            memset(&pe, 0, sizeof(pe));
            pe.size = sizeof(pe);
            pe.type = PERF_TYPE_HARDWARE;
            pe.config = PERF_COUNT_HW_BRANCH_MISSES;
            pe.disabled = 1;
            pe.exclude_kernel = 1;
            pe.exclude_hv = 1;
            ctx->fd_bmiss = perf_event_open(&pe, gettid(), -1, -1, 0);
            if (ctx->fd_bmiss != -1) {
                ioctl(ctx->fd_bmiss, PERF_EVENT_IOC_RESET, 0);
                ioctl(ctx->fd_bmiss, PERF_EVENT_IOC_ENABLE, 0);
            } else {
                ctx->conf.capture_bmiss = false;
                fprintf(stderr, "Failed to open perf event for BMISS: %s\n", strerror(errno));
            }
        } else {
            ioctl(ctx->fd_bmiss, PERF_EVENT_IOC_RESET, 0);
        }
    }

    if (ctx->conf.capture_ins) {
        if (ctx->fd_ins == -1) {
            memset(&pe, 0, sizeof(pe));
            pe.size = sizeof(pe);
            pe.type = PERF_TYPE_HARDWARE;
            pe.config = PERF_COUNT_HW_INSTRUCTIONS;
            pe.disabled = 1;
            pe.exclude_kernel = 1;
            pe.exclude_hv = 1;
            ctx->fd_ins = perf_event_open(&pe, gettid(), -1, -1, 0);
            if (ctx->fd_ins != -1) {
                ioctl(ctx->fd_ins, PERF_EVENT_IOC_RESET, 0);
                ioctl(ctx->fd_ins, PERF_EVENT_IOC_ENABLE, 0);
            } else {
                ctx->conf.capture_ins = false;
                fprintf(stderr, "Failed to open perf event for INS: %s\n", strerror(errno));
            }
        } else {
            ioctl(ctx->fd_ins, PERF_EVENT_IOC_RESET, 0);
        }
    }
}

static void HW_clean(struct HW_ctx* ctx) {
    if (ctx->conf.capture_swi && ctx->fd_swi != -1) {
        ioctl(ctx->fd_swi, PERF_EVENT_IOC_DISABLE, 0);
        close(ctx->fd_swi);
        ctx->fd_swi = -1;
    }
    if (ctx->conf.capture_cyc && ctx->fd_cyc != -1) {
        ioctl(ctx->fd_cyc, PERF_EVENT_IOC_DISABLE, 0);
        close(ctx->fd_cyc);
        ctx->fd_cyc = -1;
    }
    if (ctx->conf.capture_cmiss && ctx->fd_cmiss != -1) {
        ioctl(ctx->fd_cmiss, PERF_EVENT_IOC_DISABLE, 0);
        close(ctx->fd_cmiss);
        ctx->fd_cmiss = -1;
    }
    if (ctx->conf.capture_bmiss && ctx->fd_bmiss != -1) {
        ioctl(ctx->fd_bmiss, PERF_EVENT_IOC_DISABLE, 0);
        close(ctx->fd_bmiss);
        ctx->fd_bmiss = -1;
    }
    if (ctx->conf.capture_ins && ctx->fd_ins != -1) {
        ioctl(ctx->fd_ins, PERF_EVENT_IOC_DISABLE, 0);
        close(ctx->fd_ins);
        ctx->fd_ins = -1;
    }
}

static void collect_metrics(struct HW_ctx* ctx, cputrace_anchor* anchor, uint64_t tid) {
    pthread_mutex_lock(&anchor->mutex[tid]);
    auto* arena = anchor->thread_arena[tid];

    if (ctx->conf.capture_swi && ctx->fd_swi != -1) {
        long long value = 0;
        if (read(ctx->fd_swi, &value, sizeof(long long)) != -1) {
            auto* r = (cputrace_anchor_result*)arena_alloc(arena, sizeof(cputrace_anchor_result));
            r->type = CPUTRACE_RESULT_SWI;
            r->value = value;
            ioctl(ctx->fd_swi, PERF_EVENT_IOC_RESET, 0);
        } else {
            fprintf(stderr, "Failed to read perf event for SWI: %s\n", strerror(errno));
        }
    }
    if (ctx->conf.capture_cyc && ctx->fd_cyc != -1) {
        long long value = 0;
        if (read(ctx->fd_cyc, &value, sizeof(long long)) != -1) {
            auto* r = (cputrace_anchor_result*)arena_alloc(arena, sizeof(cputrace_anchor_result));
            r->type = CPUTRACE_RESULT_CYC;
            r->value = value;
            ioctl(ctx->fd_cyc, PERF_EVENT_IOC_RESET, 0);
        } else {
            fprintf(stderr, "Failed to read perf event for CYC: %s\n", strerror(errno));
        }
    }
    if (ctx->conf.capture_cmiss && ctx->fd_cmiss != -1) {
        long long value = 0;
        if (read(ctx->fd_cmiss, &value, sizeof(long long)) != -1) {
            auto* r = (cputrace_anchor_result*)arena_alloc(arena, sizeof(cputrace_anchor_result));
            r->type = CPUTRACE_RESULT_CMISS;
            r->value = value;
            ioctl(ctx->fd_cmiss, PERF_EVENT_IOC_RESET, 0);
        } else {
            fprintf(stderr, "Failed to read perf event for CMISS: %s\n", strerror(errno));
        }
    }
    if (ctx->conf.capture_bmiss && ctx->fd_bmiss != -1) {
        long long value = 0;
        if (read(ctx->fd_bmiss, &value, sizeof(long long)) != -1) {
            auto* r = (cputrace_anchor_result*)arena_alloc(arena, sizeof(cputrace_anchor_result));
            r->type = CPUTRACE_RESULT_BMISS;
            r->value = value;
            ioctl(ctx->fd_bmiss, PERF_EVENT_IOC_RESET, 0);
        } else {
            fprintf(stderr, "Failed to read perf event for BMISS: %s\n", strerror(errno));
        }
    }
    if (ctx->conf.capture_ins && ctx->fd_ins != -1) {
        long long value = 0;
        if (read(ctx->fd_ins, &value, sizeof(long long)) != -1) {
            auto* r = (cputrace_anchor_result*)arena_alloc(arena, sizeof(cputrace_anchor_result));
            r->type = CPUTRACE_RESULT_INS;
            r->value = value;
            ioctl(ctx->fd_ins, PERF_EVENT_IOC_RESET, 0);
        } else {
            fprintf(stderr, "Failed to read perf event for INS: %s\n", strerror(errno));
        }
    }
    pthread_mutex_unlock(&anchor->mutex[tid]);
}

static void aggregate_thread_results(cputrace_anchor* anchor, uint64_t tid) {
    pthread_mutex_lock(&anchor->mutex[tid]);
    auto* arena = anchor->thread_arena[tid];
    auto* arr = (cputrace_anchor_result*)arena->region->start;
    size_t count = ((char*)arena->region->current - (char*)arena->region->start) / sizeof(arr[0]);
    for (size_t i = 0; i < count; ++i) {
        if (arr[i].type == CPUTRACE_RESULT_CALL_COUNT && arr[i].value == 1) {
            anchor->call_count += arr[i].value;
        } else {
            anchor->global_sum[arr[i].type] += arr[i].value;
        }
    }
    arena_reset(arena);
    pthread_mutex_unlock(&anchor->mutex[tid]);
}

HW_profile::HW_profile(const char* function, uint64_t index, uint64_t flags)
    : function(function), index(index), flags(flags) {
    if (index >= CPUTRACE_MAX_ANCHORS || !g_profiler.profiling)
        return;

    g_profiler.anchors[index].name = function;
    g_profiler.anchors[index].flags = flags;
    uint64_t tid = get_thread_id();

    pthread_mutex_lock(&g_profiler.anchors[index].mutex[tid]);
    auto* a = g_profiler.anchors[index].thread_arena[tid];
    auto* r = (cputrace_anchor_result*)arena_alloc(a, sizeof(cputrace_anchor_result));
    r->type = CPUTRACE_RESULT_CALL_COUNT;
    r->value = 1;
    pthread_mutex_unlock(&g_profiler.anchors[index].mutex[tid]);

    struct HW_conf conf = {0};
    if (flags & HW_PROFILE_SWI) conf.capture_swi = true;
    if (flags & HW_PROFILE_CYC) conf.capture_cyc = true;
    if (flags & HW_PROFILE_CMISS) conf.capture_cmiss = true;
    if (flags & HW_PROFILE_BMISS) conf.capture_bmiss = true;
    if (flags & HW_PROFILE_INS) conf.capture_ins = true;

    HW_init(&ctx, &conf);
    HW_start(&ctx);

    pthread_mutex_lock(&g_profiler.global_lock);
    active_contexts[index][tid] = &ctx;
    pthread_mutex_unlock(&g_profiler.global_lock);
}

HW_profile::~HW_profile() {
    if (!g_profiler.profiling || index >= CPUTRACE_MAX_ANCHORS)
        return;

    uint64_t tid = get_thread_id();
    pthread_mutex_lock(&g_profiler.global_lock);
    collect_metrics(&ctx, &g_profiler.anchors[index], tid);
    aggregate_thread_results(&g_profiler.anchors[index], tid);
    HW_clean(&ctx);
    active_contexts[index][tid] = nullptr;
    pthread_mutex_unlock(&g_profiler.global_lock);
}

void cputrace_start(ceph::Formatter* f) {
    pthread_mutex_lock(&g_profiler.global_lock);
    if (g_profiler.profiling) {
        f->open_object_section("cputrace_start");
        f->dump_format("status", "Profiling already active");
        f->close_section();
        pthread_mutex_unlock(&g_profiler.global_lock);
        return;
    }
    g_profiler.profiling = true;
    f->open_object_section("cputrace_start");
    f->dump_format("status", "Profiling started");
    f->close_section();
    pthread_mutex_unlock(&g_profiler.global_lock);
}

void cputrace_stop(ceph::Formatter* f) {
    pthread_mutex_lock(&g_profiler.global_lock);
    if (!g_profiler.profiling) {
        f->open_object_section("cputrace_stop");
        f->dump_format("status", "Profiling not active");
        f->close_section();
        pthread_mutex_unlock(&g_profiler.global_lock);
        return;
    }
    g_profiler.profiling = false;
    pthread_mutex_unlock(&g_profiler.global_lock);
    f->open_object_section("cputrace_stop");
    f->dump_format("status", "Profiling stopped");
    f->close_section();
}

void cputrace_reset(ceph::Formatter* f) {
    pthread_mutex_lock(&g_profiler.global_lock);
    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        if (!g_profiler.anchors[i].name) continue;
        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            pthread_mutex_lock(&g_profiler.anchors[i].mutex[j]);
            arena_reset(g_profiler.anchors[i].thread_arena[j]);
            active_contexts[i][j] = nullptr;
            pthread_mutex_unlock(&g_profiler.anchors[i].mutex[j]);
        }
        g_profiler.anchors[i].call_count = 0;
        for (int t = 0; t < CPUTRACE_RESULT_COUNT; ++t) {
            g_profiler.anchors[i].global_sum[t] = 0;
        }
    }
    f->open_object_section("cputrace_reset");
    f->dump_format("status", "Counters reset");
    f->close_section();
    pthread_mutex_unlock(&g_profiler.global_lock);
}

void cputrace_dump(ceph::Formatter* f, const std::string& logger, const std::string& counter) {
    pthread_mutex_lock(&g_profiler.global_lock);
    f->open_object_section("cputrace");
    const char* names[CPUTRACE_RESULT_COUNT - 1] = {"context_switches", "cycles", "cache_misses", "branch_misses", "instructions"};
    bool dumped = false;

    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        if (!g_profiler.anchors[i].name || (!logger.empty() && g_profiler.anchors[i].name != logger)) continue;

        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (active_contexts[i][j]) {
                collect_metrics(active_contexts[i][j], &g_profiler.anchors[i], j);
            }
            aggregate_thread_results(&g_profiler.anchors[i], j);
        }

        f->open_object_section(g_profiler.anchors[i].name);
        if (g_profiler.anchors[i].call_count) {
            f->dump_unsigned("call_count", g_profiler.anchors[i].call_count);
        }
        for (int t = 0; t < CPUTRACE_RESULT_COUNT - 1; ++t) {
            if (!(g_profiler.anchors[i].flags & (1ULL << t))) continue;
            if (counter.empty() || names[t] == counter) {
                f->dump_unsigned(names[t], g_profiler.anchors[i].global_sum[t]);
                if (g_profiler.anchors[i].call_count) {
                    f->dump_float(std::string("avg_") + names[t], (double)g_profiler.anchors[i].global_sum[t] / g_profiler.anchors[i].call_count);
                }
            }
        }
        f->close_section();
        dumped = true;
    }

    f->dump_format("status", dumped ? "Profiling data dumped" : "No profiling data available");
    f->close_section();
    pthread_mutex_unlock(&g_profiler.global_lock);
}

__attribute__((constructor)) static void cputrace_init() {
    g_profiler.anchors = (cputrace_anchor*)calloc(CPUTRACE_MAX_ANCHORS, sizeof(cputrace_anchor));
    if (!g_profiler.anchors) {
        fprintf(stderr, "Failed to allocate memory for profiler anchors: %s\n", strerror(errno));
        exit(1);
    }
    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (pthread_mutex_init(&g_profiler.anchors[i].mutex[j], nullptr) != 0) {
                fprintf(stderr, "Failed to initialize mutex for anchor %d, thread %d: %s\n", i, j, strerror(errno));
            }
            g_profiler.anchors[i].thread_arena[j] = arena_create(4 * 1024 * 1024);
        }
    }
    if (pthread_mutex_init(&g_profiler.global_lock, nullptr) != 0) {
        fprintf(stderr, "Failed to initialize global mutex: %s\n", strerror(errno));
    }
}

__attribute__((destructor)) static void cputrace_fini() {
    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (pthread_mutex_destroy(&g_profiler.anchors[i].mutex[j]) != 0) {
                fprintf(stderr, "Failed to destroy mutex for anchor %d, thread %d: %s\n", i, j, strerror(errno));
            }
            arena_destroy(g_profiler.anchors[i].thread_arena[j]);
        }
    }
    if (pthread_mutex_destroy(&g_profiler.global_lock) != 0) {
        fprintf(stderr, "Failed to destroy global mutex: %s\n", strerror(errno));
    }
    free(g_profiler.anchors);
}