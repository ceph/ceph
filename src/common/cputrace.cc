/*
 * CpuTrace: lightweight hardware performance counter profiling
 *
 * Implementation details.
 *
 * See detailed documentation and usage examples in:
 *   doc/dev/cputrace.rst
 *
 * This file contains the low-level implementation of CpuTrace,
 * including perf_event setup, context management, and RAII
 * profiling helpers.
 */

#include "cputrace.h"

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

static int thread_next_id = 0;
static std::mutex thread_id_mtx;
static thread_local int thread_id_local = -1;
static cputrace_profiler g_profiler;
static std::unordered_map<std::string, measurement_t> g_named_measurements;
static std::mutex g_named_measurements_lock;
static std::unordered_map<std::string, int> name_to_id;
static int next_id = 0;
static std::mutex name_id_mtx;

int register_anchor(const char* name) {
    std::lock_guard<std::mutex> lock(name_id_mtx);
    auto it = name_to_id.find(name);
    ceph_assert(it == name_to_id.end());
    int id = next_id++;
    name_to_id[name] = id;
    return id;
}

struct read_format {
    uint64_t nr;
    struct values {
        uint64_t value;
        uint64_t id;
    } values[];
};

static long perf_event_open(struct perf_event_attr* hw_event, pid_t pid,
                           int cpu, int group_fd, unsigned long flags) {
    return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
}

inline int get_thread_id() {
    if (thread_id_local == -1) {
        std::lock_guard<std::mutex> lck(thread_id_mtx);
        thread_id_local = thread_next_id++ % CPUTRACE_MAX_THREADS;
    }
    return thread_id_local;
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

HW_ctx HW_ctx_empty = {
    -1, -1, -1, -1, -1, -1,
     0,  0,  0,  0,  0
};

void HW_init(HW_ctx* ctx, cputrace_flags flags) {
    struct perf_event_attr pe;
    int parent_fd = -1;

    if (flags & HW_PROFILE_SWI) {
        setup_perf_event(&pe, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES);
        open_perf_fd(ctx->fd_swi, ctx->id_swi, &pe, "SWI", -1);
        parent_fd = ctx->fd_swi;
    } else if (flags & HW_PROFILE_CYC) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
        open_perf_fd(ctx->fd_cyc, ctx->id_cyc, &pe, "CYC", -1);
        parent_fd = ctx->fd_cyc;
    } else if (flags & HW_PROFILE_INS) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
        open_perf_fd(ctx->fd_ins, ctx->id_ins, &pe, "INS", -1);
        parent_fd = ctx->fd_ins;
    } else if (flags & HW_PROFILE_CMISS) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES);
        open_perf_fd(ctx->fd_cmiss, ctx->id_cmiss, &pe, "CMISS", -1);
        parent_fd = ctx->fd_cmiss;
    } else if (flags & HW_PROFILE_BMISS) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);
        open_perf_fd(ctx->fd_bmiss, ctx->id_bmiss, &pe, "BMISS", -1);
        parent_fd = ctx->fd_bmiss;
    }

    ctx->parent_fd = parent_fd;

    if (flags & HW_PROFILE_SWI && ctx->fd_swi == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES);
        open_perf_fd(ctx->fd_swi, ctx->id_swi, &pe, "SWI", parent_fd);
    }
    if (flags & HW_PROFILE_CYC && ctx->fd_cyc == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
        open_perf_fd(ctx->fd_cyc, ctx->id_cyc, &pe, "CYC", parent_fd);
    }
    if (flags & HW_PROFILE_CMISS && ctx->fd_cmiss == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES);
        open_perf_fd(ctx->fd_cmiss, ctx->id_cmiss, &pe, "CMISS", parent_fd);
    }
    if (flags & HW_PROFILE_BMISS && ctx->fd_bmiss == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);
        open_perf_fd(ctx->fd_bmiss, ctx->id_bmiss, &pe, "BMISS", parent_fd);
    }
    if (flags & HW_PROFILE_INS && ctx->fd_ins == -1 && parent_fd != -1) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
        open_perf_fd(ctx->fd_ins, ctx->id_ins, &pe, "INS", parent_fd);
    }

    if (parent_fd != -1) {
        ioctl(parent_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
        ioctl(parent_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
    }
}

void HW_clean(HW_ctx* ctx) {
    close_perf_fd(ctx->fd_swi);
    close_perf_fd(ctx->fd_cyc);
    close_perf_fd(ctx->fd_cmiss);
    close_perf_fd(ctx->fd_bmiss);
    close_perf_fd(ctx->fd_ins);
}

void HW_read(HW_ctx* ctx, sample_t* measure) {
    if (ctx->parent_fd == -1) {
        return;
    }
    static constexpr uint64_t MAX_COUNTERS = 5;
    static constexpr size_t BUFFER_SIZE =
        sizeof(read_format) + MAX_COUNTERS * sizeof(struct read_format::values);
    char buf[BUFFER_SIZE];

    struct read_format* rf = (struct read_format*)buf;
    if (read(ctx->parent_fd, buf, sizeof(buf)) > 0) {
        for (uint64_t i = 0; i < rf->nr; i++) {
            if (rf->values[i].id == ctx->id_swi) {
                measure->swi = rf->values[i].value;
            } else if (rf->values[i].id == ctx->id_cyc) {
                measure->cyc = rf->values[i].value;
            } else if (rf->values[i].id == ctx->id_cmiss) {
                measure->cmiss = rf->values[i].value;
            } else if (rf->values[i].id == ctx->id_bmiss) {
                measure->bmiss = rf->values[i].value;
            } else if (rf->values[i].id == ctx->id_ins) {
                measure->ins = rf->values[i].value;
            }
        }
    }
}

static void collect_samples(sample_t* start, sample_t* end, cputrace_anchor* anchor) {
    sample_t elapsed = *end - *start;
    anchor->global_results.sample(elapsed);
}

HW_profile::HW_profile(const char* function, uint64_t index, cputrace_flags flags)
    : function(function), index(index), flags(flags) {
    if (!g_profiler.profiling.load()) {
        return;
    }
    ceph_assert(index < CPUTRACE_MAX_ANCHORS);
    uint64_t tid = get_thread_id();
    cputrace_anchor& anchor = g_profiler.anchors[index];
    pthread_mutex_lock(&anchor.lock);
    anchor.name = function;
    anchor.flags = flags;

    anchor.global_results.call_count += 1;

    if (anchor.active_contexts[tid] == nullptr) {
        ctx = &anchor.per_thread_ctx[tid];
        *ctx = HW_ctx_empty;
        HW_init(ctx, flags);
        anchor.active_contexts[tid] = ctx;
    } else {
        ctx = anchor.active_contexts[tid];
    }
    anchor.nest_level[tid]++;
    if (anchor.nest_level[tid] == 1) {
        HW_read(ctx, &anchor.start[tid]);
    }
    anchor.is_capturing[tid] = true;
    pthread_mutex_unlock(&anchor.lock);
}

HW_profile::~HW_profile() {
    if (!g_profiler.profiling.load()) {
        return;
    }
    ceph_assert(index < CPUTRACE_MAX_ANCHORS);
    cputrace_anchor& anchor = g_profiler.anchors[index];
    uint64_t tid = get_thread_id();
    pthread_mutex_lock(&anchor.lock);
    anchor.nest_level[tid]--;
    if (anchor.nest_level[tid] == 0) {
        HW_read(ctx, &anchor.end[tid]);
        collect_samples(&anchor.start[tid], &anchor.end[tid], &anchor);
        anchor.start[tid] = anchor.end[tid];
        anchor.is_capturing[tid] = false;
    }
    pthread_mutex_unlock(&anchor.lock);
}

measurement_t* get_named_measurement(const std::string& name) {
    std::lock_guard<std::mutex> g(g_named_measurements_lock);
    return &g_named_measurements[name];
}

HW_named_guard::HW_named_guard(const char* name, HW_ctx* ctx)
    : name(name),
    guard(ctx, get_named_measurement(name))
{
}

HW_named_guard::~HW_named_guard() {
}

void cputrace_start(ceph::Formatter* f) {
    if (g_profiler.profiling.load()) {
        if (f) {
            f->open_object_section("cputrace_start");
            f->dump_format("status", "Profiling already active");
            f->close_section();
        }
        return;
    }
    g_profiler.profiling = true;
    if (f) {
        f->open_object_section("cputrace_start");
        f->dump_format("status", "Profiling started");
        f->close_section();
    }
}

void cputrace_stop(ceph::Formatter* f) {
    if (!g_profiler.profiling.load()) {
        if (f) {
            f->open_object_section("cputrace_stop");
            f->dump_format("status", "Profiling not active");
            f->close_section();
        }
        return;
    }
    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        cputrace_anchor& anchor = g_profiler.anchors[i];
        if (!anchor.name) {
            continue;
        }
        pthread_mutex_lock(&anchor.lock);
        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (anchor.is_capturing[j]) {
                HW_read(anchor.active_contexts[j], &anchor.end[j]);
                collect_samples(&anchor.start[j], &anchor.end[j], &anchor);
                anchor.start[j] = anchor.end[j];
                anchor.is_capturing[j] = false;
            }
        }
        pthread_mutex_unlock(&anchor.lock);
    }
    g_profiler.profiling = false;
    if (f) {
        f->open_object_section("cputrace_stop");
        f->dump_format("status", "Profiling stopped");
        f->close_section();
    }
}

void cputrace_reset(ceph::Formatter* f) {
    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        if (!g_profiler.anchors[i].name) continue;
        pthread_mutex_lock(&g_profiler.anchors[i].lock);
        g_profiler.anchors[i].global_results.reset();
        pthread_mutex_unlock(&g_profiler.anchors[i].lock);
    }
    if (f) {
        f->open_object_section("cputrace_reset");
        f->dump_format("status", "Counters reset");
        f->close_section();
    }
}

void cputrace_dump(ceph::Formatter* f, const std::string& logger, const std::string& counter) {
    f->open_object_section("cputrace");
    bool dumped = false;

    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        cputrace_anchor& anchor = g_profiler.anchors[i];
        if (!anchor.name || (!logger.empty() && anchor.name != logger)) {
            continue;
        }

        pthread_mutex_lock(&anchor.lock);
        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (anchor.is_capturing[j] && g_profiler.profiling.load()) {
                HW_read(anchor.active_contexts[j], &anchor.end[j]);
                collect_samples(&anchor.start[j], &anchor.end[j], &anchor);
                anchor.start[j] = anchor.end[j];
            }
        }
        pthread_mutex_unlock(&anchor.lock);

        f->open_object_section(anchor.name);
        anchor.global_results.dump(f, anchor.flags, counter);
        f->close_section();
        dumped = true;
    }

    f->dump_format("status", dumped ? "Profiling data dumped" : "No profiling data available");
    f->close_section();
}

void cputrace_print_to_stringstream(std::stringstream& ss) {
    ss << "cputrace:\n";
    bool dumped = false;

    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        cputrace_anchor& anchor = g_profiler.anchors[i];
        if (!anchor.name) {
            continue;
        }

        pthread_mutex_lock(&anchor.lock);
        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (anchor.is_capturing[j] && g_profiler.profiling.load()) {
                HW_read(anchor.active_contexts[j], &anchor.end[j]);
                collect_samples(&anchor.start[j], &anchor.end[j], &anchor);
                anchor.start[j] = anchor.end[j];
            }
        }
        pthread_mutex_unlock(&anchor.lock);

        ss << "  " << anchor.name << ":\n";
        anchor.global_results.dump_to_stringstream(ss, anchor.flags);
        dumped = true;
    }

    ss << "status: " << (dumped ? "Profiling data dumped" : "No profiling data available") << "\n";
}

__attribute__((constructor)) static void cputrace_init() {
    g_profiler.anchors = (cputrace_anchor*)calloc(CPUTRACE_MAX_ANCHORS, sizeof(cputrace_anchor));
    if (!g_profiler.anchors) {
        fprintf(stderr, "Failed to allocate memory for profiler anchors: %s\n", strerror(errno));
        exit(1);
    }
    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        if (pthread_mutex_init(&g_profiler.anchors[i].lock, nullptr) != 0) {
            fprintf(stderr, "Failed to initialize mutex for anchor %d: %s\n", i, strerror(errno));
            exit(1);
        }

    }
}

__attribute__((destructor)) static void cputrace_fini() {
    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        cputrace_anchor& anchor = g_profiler.anchors[i];
        pthread_mutex_lock(&anchor.lock);
        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (anchor.active_contexts[j] != nullptr) {
                HW_clean(&anchor.per_thread_ctx[j]);
                anchor.active_contexts[j] = nullptr;
                anchor.is_capturing[j] = false;
            }
        }
        pthread_mutex_unlock(&anchor.lock);
        if (pthread_mutex_destroy(&g_profiler.anchors[i].lock) != 0) {
            fprintf(stderr, "Failed to destroy mutex for anchor %d: %s\n", i, strerror(errno));
        }
    }
    free(g_profiler.anchors);
    g_profiler.anchors = nullptr;
}