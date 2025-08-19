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
#include <cstring>
#include <thread>

#define PROFILE_ASSERT(x) if (!(x)) { fprintf(stderr, "Assert failed %s:%d\n", __FILE__, __LINE__); exit(1); }

static thread_local uint64_t thread_id_hash;
static thread_local bool thread_id_initialized;
static cputrace_profiler g_profiler;

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

void HW_init(HW_ctx* ctx, uint64_t flags) {
    struct perf_event_attr pe;
    int parent_fd = -1;

    if (flags & HW_PROFILE_SWI) {
        setup_perf_event(&pe, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES);
        open_perf_fd(ctx->fd_swi, ctx->id_swi, &pe, "SWI", -1);
        parent_fd = ctx->fd_swi;
    }
    else if (flags & HW_PROFILE_CYC) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
        open_perf_fd(ctx->fd_cyc, ctx->id_cyc, &pe, "CYC", -1);
        parent_fd = ctx->fd_cyc;
    } else if (flags & HW_PROFILE_CMISS) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES);
        open_perf_fd(ctx->fd_cmiss, ctx->id_cmiss, &pe, "CMISS", -1);
        parent_fd = ctx->fd_cmiss;
    } else if (flags & HW_PROFILE_BMISS) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);
        open_perf_fd(ctx->fd_bmiss, ctx->id_bmiss, &pe, "BMISS", -1);
        parent_fd = ctx->fd_bmiss;
    } else if (flags & HW_PROFILE_INS) {
        setup_perf_event(&pe, PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
        open_perf_fd(ctx->fd_ins, ctx->id_ins, &pe, "INS", -1);
        parent_fd = ctx->fd_ins;
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
    close_perf_fd(ctx->parent_fd);
}

void HW_read(HW_ctx* ctx, sample_t* measure) {
    if (ctx->parent_fd == -1) {
        return;
    }
    char buf[256];
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
    if (end->swi) {
        anchor->global_results.swi += end->swi - start->swi;
    }
    if (end->cyc) {
        anchor->global_results.cyc += end->cyc - start->cyc;
    }
    if (end->cmiss) {
        anchor->global_results.cmiss += end->cmiss - start->cmiss;
    }
    if (end->bmiss) {
        anchor->global_results.bmiss += end->bmiss - start->bmiss;
    }
    if (end->ins) {
        anchor->global_results.ins += end->ins - start->ins;
    }
}

HW_profile::HW_profile(const char* function, uint64_t index, uint64_t flags)
    : function(function), index(index), flags(flags) {
    if (index >= CPUTRACE_MAX_ANCHORS || !g_profiler.profiling)
        return;

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
    if (!g_profiler.profiling || index >= CPUTRACE_MAX_ANCHORS)
        return;

    cputrace_anchor& anchor = g_profiler.anchors[index];
    uint64_t tid = get_thread_id();

    pthread_mutex_lock(&anchor.lock);
    anchor.nest_level[tid]--;
    if (anchor.nest_level[tid] == 0) {
        HW_read(ctx, &anchor.end[tid]);
        collect_samples(&anchor.start[tid], &anchor.end[tid], &anchor);
        std::memcpy(&anchor.start[tid], &anchor.end[tid], sizeof(anchor.start[tid]));
        anchor.is_capturing[tid] = false;
    }
    pthread_mutex_unlock(&anchor.lock);
}

void cputrace_start() {
    pthread_mutex_lock(&g_profiler.global_lock);
    if (g_profiler.profiling) {
        pthread_mutex_unlock(&g_profiler.global_lock);
        return;
    }
    g_profiler.profiling = true;
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

void cputrace_stop() {
    pthread_mutex_lock(&g_profiler.global_lock);
    if (!g_profiler.profiling) {
        pthread_mutex_unlock(&g_profiler.global_lock);
        return;
    }
    g_profiler.profiling = false;
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

void cputrace_reset() {
    pthread_mutex_lock(&g_profiler.global_lock);
    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        if (!g_profiler.anchors[i].name) continue;
        pthread_mutex_lock(&g_profiler.anchors[i].lock);
        g_profiler.anchors[i].global_results = results{};
        pthread_mutex_unlock(&g_profiler.anchors[i].lock);
    }
    pthread_mutex_unlock(&g_profiler.global_lock);
}

void cputrace_reset(ceph::Formatter* f) {
    pthread_mutex_lock(&g_profiler.global_lock);
    for (int i = 0; i < CPUTRACE_MAX_ANCHORS; ++i) {
        if (!g_profiler.anchors[i].name) continue;
        pthread_mutex_lock(&g_profiler.anchors[i].lock);
        g_profiler.anchors[i].global_results = results{};
        pthread_mutex_unlock(&g_profiler.anchors[i].lock);
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
        cputrace_anchor& anchor = g_profiler.anchors[i];
        if (!anchor.name || (!logger.empty() && anchor.name != logger)) {
            continue;
        }

        pthread_mutex_lock(&anchor.lock);
        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (anchor.is_capturing[j]) {
                HW_read(anchor.active_contexts[j], &anchor.end[j]);
                collect_samples(&anchor.start[j], &anchor.end[j], &anchor);
                std::memcpy(&anchor.start[j], &anchor.end[j], sizeof(anchor.start[j]));
            }
        }
        pthread_mutex_unlock(&anchor.lock);

        f->open_object_section(anchor.name);
        f->dump_unsigned("call_count", anchor.global_results.call_count);

        if (anchor.flags & HW_PROFILE_SWI && (counter.empty() || counter == "context_switches")) {
            f->dump_unsigned("context_switches", anchor.global_results.swi);
            if (anchor.global_results.call_count)
                f->dump_float("avg_context_switches", (double)anchor.global_results.swi / anchor.global_results.call_count);
        }
        if (anchor.flags & HW_PROFILE_CYC && (counter.empty() || counter == "cpu_cycles")) {
            f->dump_unsigned("cpu_cycles", anchor.global_results.cyc);
            if (anchor.global_results.call_count)
                f->dump_float("avg_cpu_cycles", (double)anchor.global_results.cyc / anchor.global_results.call_count);
        }
        if (anchor.flags & HW_PROFILE_CMISS && (counter.empty() || counter == "cache_misses")) {
            f->dump_unsigned("cache_misses", anchor.global_results.cmiss);
            if (anchor.global_results.call_count)
                f->dump_float("avg_cache_misses", (double)anchor.global_results.cmiss / anchor.global_results.call_count);
        }
        if (anchor.flags & HW_PROFILE_BMISS && (counter.empty() || counter == "branch_misses")) {
            f->dump_unsigned("branch_misses", anchor.global_results.bmiss);
            if (anchor.global_results.call_count)
                f->dump_float("avg_branch_misses", (double)anchor.global_results.bmiss / anchor.global_results.call_count);
        }
        if (anchor.flags & HW_PROFILE_INS && (counter.empty() || counter == "instructions")) {
            f->dump_unsigned("instructions", anchor.global_results.ins);
            if (anchor.global_results.call_count)
                f->dump_float("avg_instructions", (double)anchor.global_results.ins / anchor.global_results.call_count);
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
        cputrace_anchor& anchor = g_profiler.anchors[i];
        if (!anchor.name) {
            continue;
        }

        pthread_mutex_lock(&anchor.lock);
        for (int j = 0; j < CPUTRACE_MAX_THREADS; ++j) {
            if (anchor.is_capturing[j]) {
                HW_read(anchor.active_contexts[j], &anchor.end[j]);
                collect_samples(&anchor.start[j], &anchor.end[j], &anchor);
                std::memcpy(&anchor.start[j], &anchor.end[j], sizeof(anchor.start[j]));
            }
        }
        pthread_mutex_unlock(&anchor.lock);

        ss << "  " << anchor.name << ":\n";
        ss << "    call_count: " << anchor.global_results.call_count << "\n";

        if (anchor.flags & HW_PROFILE_SWI) {
            ss << "    context_switches: " << anchor.global_results.swi;
            if (anchor.global_results.call_count) {
                ss << "\n    avg_context_switches: " << (double)anchor.global_results.swi / anchor.global_results.call_count;
            }
            ss << "\n";
        }
        if (anchor.flags & HW_PROFILE_CYC) {
            ss << "    cpu_cycles: " << anchor.global_results.cyc;
            if (anchor.global_results.call_count) {
                ss << "\n    avg_cpu_cycles: " << (double)anchor.global_results.cyc / anchor.global_results.call_count;
            }
            ss << "\n";
        }
        if (anchor.flags & HW_PROFILE_CMISS) {
            ss << "    cache_misses: " << anchor.global_results.cmiss;
            if (anchor.global_results.call_count) {
                ss << "\n    avg_cache_misses: " << (double)anchor.global_results.cmiss / anchor.global_results.call_count;
            }
            ss << "\n";
        }
        if (anchor.flags & HW_PROFILE_BMISS) {
            ss << "    branch_misses: " << anchor.global_results.bmiss;
            if (anchor.global_results.call_count) {
                ss << "\n    avg_branch_misses: " << (double)anchor.global_results.bmiss / anchor.global_results.call_count;
            }
            ss << "\n";
        }
        if (anchor.flags & HW_PROFILE_INS) {
            ss << "    instructions: " << anchor.global_results.ins;
            if (anchor.global_results.call_count) {
                ss << "\n    avg_instructions: " << (double)anchor.global_results.ins / anchor.global_results.call_count;
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
        if (pthread_mutex_init(&g_profiler.anchors[i].lock, nullptr) != 0) {
            fprintf(stderr, "Failed to initialize mutex for anchor %d: %s\n", i, strerror(errno));
            exit(1);
        }

    }
    if (pthread_mutex_init(&g_profiler.global_lock, nullptr) != 0) {
        fprintf(stderr, "Failed to initialize global mutex: %s\n", strerror(errno));
        exit(1);
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
    if (pthread_mutex_destroy(&g_profiler.global_lock) != 0) {
        fprintf(stderr, "Failed to destroy global mutex: %s\n", strerror(errno));
    }
    free(g_profiler.anchors);
    g_profiler.anchors = nullptr;
}