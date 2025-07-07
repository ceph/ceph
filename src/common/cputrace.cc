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

#define PROFILE_ASSERT(x) if (!(x)) { fprintf(stderr, "Assert failed %s:%d\n", __FILE__, __LINE__); exit(1); }

static thread_local uint64_t thread_id_hash;
static thread_local bool thread_id_initialized;
static cputrace_profiler g_profiler;
static HW_ctx* active_contexts[CPUTRACE_MAX_ANCHORS][CPUTRACE_MAX_THREADS] = {{nullptr}};

struct read_format {
    uint64_t nr;
    struct {
        uint64_t value;
        uint64_t id;
    } values[];
};

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

static void setup_perf_event(struct perf_event_attr* pe, uint32_t type, uint64_t config) {
    memset(pe, 0, sizeof(*pe));
    pe->size = sizeof(*pe);
    pe->type = type;
    pe->config = config;
    pe->disabled = 1;
    pe->exclude_kernel = 0;
    pe->exclude_hv = 1;
    pe->read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
    if (type != PERF_TYPE_SOFTWARE) {
        pe->exclude_kernel = 1;
    }
}

static void open_perf_fd(int& fd, uint64_t& id, struct perf_event_attr* pe, const char* name, int group_fd) {
    fd = perf_event_open(pe, gettid(), -1, group_fd, 0);
    if (fd != -1) {
        ioctl(fd, PERF_EVENT_IOC_ID, &id);
        ioctl(fd, PERF_EVENT_IOC_RESET, 0);
    } else {    
        fprintf(stderr, "Failed to open perf event for %s: %s\n", name, strerror(errno));
        id = 0;
    }
}

static void close_perf_fd(int& fd) {
    if (fd != -1) {
        ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
        close(fd);
        fd = -1;
    }
}

static void HW_init(HW_ctx* ctx, HW_conf* conf) {
    *ctx = { -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, *conf };
}

static void HW_start(HW_ctx* ctx) {
    struct perf_event_attr pe;
    int parent_fd = -1;

    if (ctx->conf.capture_swi) {
        setup_perf_event(&pe, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES);
        open_perf_fd(ctx->fd_swi, ctx->id_swi, &pe, "SWI", -1);
        parent_fd = ctx->fd_swi;
    }
    else if (ctx->conf.capture_cyc) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
        open_perf_fd(ctx->fd_cyc, ctx->id_cyc, &pe, "CYC", -1);
        parent_fd = ctx->fd_cyc;
    } else if (ctx->conf.capture_cmiss) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES);
        open_perf_fd(ctx->fd_cmiss, ctx->id_cmiss, &pe, "CMISS", -1);
        parent_fd = ctx->fd_cmiss;
    } else if (ctx->conf.capture_bmiss) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);
        open_perf_fd(ctx->fd_bmiss, ctx->id_bmiss, &pe, "BMISS", -1);
        parent_fd = ctx->fd_bmiss;
    } else if (ctx->conf.capture_ins) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
        open_perf_fd(ctx->fd_ins, ctx->id_ins, &pe, "INS", -1);
        parent_fd = ctx->fd_ins;
    }

    ctx->parent_fd = parent_fd;

    if (ctx->conf.capture_swi && ctx->fd_swi == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES);
        open_perf_fd(ctx->fd_swi, ctx->id_swi, &pe, "SWI", parent_fd);
    }
    if (ctx->conf.capture_cyc && ctx->fd_cyc == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
        open_perf_fd(ctx->fd_cyc, ctx->id_cyc, &pe, "CYC", parent_fd);
    }
    if (ctx->conf.capture_cmiss && ctx->fd_cmiss == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES);
        open_perf_fd(ctx->fd_cmiss, ctx->id_cmiss, &pe, "CMISS", parent_fd);
    }
    if (ctx->conf.capture_bmiss && ctx->fd_bmiss == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);
        open_perf_fd(ctx->fd_bmiss, ctx->id_bmiss, &pe, "BMISS", parent_fd);
    }
    if (ctx->conf.capture_ins && ctx->fd_ins == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
        open_perf_fd(ctx->fd_ins, ctx->id_ins, &pe, "INS", parent_fd);
    }

    if (parent_fd != -1) {
        ioctl(parent_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
        ioctl(parent_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
    }
}

static void HW_clean(HW_ctx* ctx) {
    close_perf_fd(ctx->fd_swi);
    close_perf_fd(ctx->fd_cyc);
    close_perf_fd(ctx->fd_cmiss);
    close_perf_fd(ctx->fd_bmiss);
    close_perf_fd(ctx->fd_ins);
    close_perf_fd(ctx->parent_fd);
}

static void read_perf_event(HW_ctx* ctx, cputrace_anchor* anchor, uint64_t tid) {
    pthread_mutex_lock(&anchor->mutex[tid]);
    Arena* arena = anchor->thread_arena[tid];
    if (ctx->parent_fd != -1) {
        char buf[256];
        struct read_format* rf = (struct read_format*)buf;

        if (read(ctx->parent_fd, buf, sizeof(buf)) > 0) {
            for (uint64_t i = 0; i < rf->nr; i++) {
                cputrace_result_type type;
                if (rf->values[i].id == ctx->id_swi) type = CPUTRACE_RESULT_SWI;
                else if (rf->values[i].id == ctx->id_cyc) type = CPUTRACE_RESULT_CYC;
                else if (rf->values[i].id == ctx->id_cmiss) type = CPUTRACE_RESULT_CMISS;
                else if (rf->values[i].id == ctx->id_bmiss) type = CPUTRACE_RESULT_BMISS;
                else if (rf->values[i].id == ctx->id_ins) type = CPUTRACE_RESULT_INS;
                else continue;
                auto* r = (cputrace_anchor_result*)arena_alloc(arena, sizeof(cputrace_anchor_result));
                r->type = type;
                r->value = rf->values[i].value;
            }
            ioctl(ctx->parent_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
        } else {
            fprintf(stderr, "Failed to read perf events: %s\n", strerror(errno));
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
            anchor->call_count += 1;
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

    cputrace_anchor& anchor = g_profiler.anchors[index];
    anchor.name = function;
    anchor.flags = flags;
    uint64_t tid = get_thread_id();

    pthread_mutex_lock(&anchor.mutex[tid]);
    auto* a = anchor.thread_arena[tid];
    auto* r = (cputrace_anchor_result*)arena_alloc(a, sizeof(cputrace_anchor_result));
    if (r) {
        r->type = CPUTRACE_RESULT_CALL_COUNT;
        r->value = 1;
    }
    pthread_mutex_unlock(&anchor.mutex[tid]);

    HW_conf conf = {0};
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
    read_perf_event(&ctx, &g_profiler.anchors[index], tid);
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
    bool dumped = false;

    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        const auto& anchor = g_profiler.anchors[i];
        if (!anchor.name || (!logger.empty() && anchor.name != logger)) {
            continue;
        }

        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (active_contexts[i][j]) {
                read_perf_event(active_contexts[i][j], &g_profiler.anchors[i], j);
            }
            aggregate_thread_results(&g_profiler.anchors[i], j);
        }

        f->open_object_section(anchor.name);
        f->dump_unsigned("call_count", anchor.call_count);

        if (anchor.flags & HW_PROFILE_SWI && (counter.empty() || counter == "context_switches")) {
            f->dump_unsigned("context_switches", anchor.global_sum[CPUTRACE_RESULT_SWI]);
            if (anchor.call_count)
                f->dump_float("avg_context_switches", (double)anchor.global_sum[CPUTRACE_RESULT_SWI] / anchor.call_count);
        }
        if (anchor.flags & HW_PROFILE_CYC && (counter.empty() || counter == "cpu_cycles")) {
            f->dump_unsigned("cpu_cycles", anchor.global_sum[CPUTRACE_RESULT_CYC]);
            if (anchor.call_count)
                f->dump_float("avg_cpu_cycles", (double)anchor.global_sum[CPUTRACE_RESULT_CYC] / anchor.call_count);
        }
        if (anchor.flags & HW_PROFILE_CMISS && (counter.empty() || counter == "cache_misses")) {
            f->dump_unsigned("cache_misses", anchor.global_sum[CPUTRACE_RESULT_CMISS]);
            if (anchor.call_count)
                f->dump_float("avg_cache_misses", (double)anchor.global_sum[CPUTRACE_RESULT_CMISS] / anchor.call_count);
        }
        if (anchor.flags & HW_PROFILE_BMISS && (counter.empty() || counter == "branch_misses")) {
            f->dump_unsigned("branch_misses", anchor.global_sum[CPUTRACE_RESULT_BMISS]);
            if (anchor.call_count)
                f->dump_float("avg_branch_misses", (double)anchor.global_sum[CPUTRACE_RESULT_BMISS] / anchor.call_count);
        }
        if (anchor.flags & HW_PROFILE_INS && (counter.empty() || counter == "instructions")) {
            f->dump_unsigned("instructions", anchor.global_sum[CPUTRACE_RESULT_INS]);
            if (anchor.call_count)
                f->dump_float("avg_instructions", (double)anchor.global_sum[CPUTRACE_RESULT_INS] / anchor.call_count);
        }
        f->close_section();
        dumped = true;
    }

    f->dump_format("status", dumped ? "Profiling data dumped" : "No profiling data available");
    f->close_section();
    pthread_mutex_unlock(&g_profiler.global_lock);
}

void cputrace_print_to_stringstream(std::stringstream& ss) {
    pthread_mutex_lock(&g_profiler.global_lock);
    ss << "cputrace:\n";
    bool dumped = false;

    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        const auto& anchor = g_profiler.anchors[i];
        if (!anchor.name) {
            continue;
        }

        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (active_contexts[i][j]) {
                read_perf_event(active_contexts[i][j], &g_profiler.anchors[i], j);
            }
            aggregate_thread_results(&g_profiler.anchors[i], j);
        }

        ss << "  " << anchor.name << ":\n";
        ss << "    call_count: " << anchor.call_count << "\n";

        if (anchor.flags & HW_PROFILE_SWI) {
            ss << "    context_switches: " << anchor.global_sum[CPUTRACE_RESULT_SWI];
            if (anchor.call_count) {
                ss << "\n    avg_context_switches: " << (double)anchor.global_sum[CPUTRACE_RESULT_SWI] / anchor.call_count;
            }
            ss << "\n";
        }
        if (anchor.flags & HW_PROFILE_CYC) {
            ss << "    cpu_cycles: " << anchor.global_sum[CPUTRACE_RESULT_CYC];
            if (anchor.call_count) {
                ss << "\n    avg_cpu_cycles: " << (double)anchor.global_sum[CPUTRACE_RESULT_CYC] / anchor.call_count;
            }
            ss << "\n";
        }
        if (anchor.flags & HW_PROFILE_CMISS) {
            ss << "    cache_misses: " << anchor.global_sum[CPUTRACE_RESULT_CMISS];
            if (anchor.call_count) {
                ss << "\n    avg_cache_misses: " << (double)anchor.global_sum[CPUTRACE_RESULT_CMISS] / anchor.call_count;
            }
            ss << "\n";
        }
        if (anchor.flags & HW_PROFILE_BMISS) {
            ss << "    branch_misses: " << anchor.global_sum[CPUTRACE_RESULT_BMISS];
            if (anchor.call_count) {
                ss << "\n    avg_branch_misses: " << (double)anchor.global_sum[CPUTRACE_RESULT_BMISS] / anchor.call_count;
            }
            ss << "\n";
        }
        if (anchor.flags & HW_PROFILE_INS) {
            ss << "    instructions: " << anchor.global_sum[CPUTRACE_RESULT_INS];
            if (anchor.call_count) {
                ss << "\n    avg_instructions: " << (double)anchor.global_sum[CPUTRACE_RESULT_INS] / anchor.call_count;
            }
            ss << "\n";
        }
        dumped = true;
    }

    ss << "status: " << (dumped ? "Profiling data dumped" : "No profiling data available") << "\n";
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