#pragma once
#include <pthread.h>
#include <stdint.h>
#include <string>
#include "common/Formatter.h"

#define CPUTRACE_MAX_ANCHORS 128
#define CPUTRACE_MAX_THREADS 64

enum cputrace_result_type {
    CPUTRACE_RESULT_SWI = 0,
    CPUTRACE_RESULT_CYC,
    CPUTRACE_RESULT_CMISS,
    CPUTRACE_RESULT_BMISS,
    CPUTRACE_RESULT_INS,
    CPUTRACE_RESULT_CALL_COUNT,
    CPUTRACE_RESULT_COUNT
};

enum cputrace_flags {
    HW_PROFILE_SWI   = (1ULL << CPUTRACE_RESULT_SWI),
    HW_PROFILE_CYC   = (1ULL << CPUTRACE_RESULT_CYC),
    HW_PROFILE_CMISS = (1ULL << CPUTRACE_RESULT_CMISS),
    HW_PROFILE_BMISS = (1ULL << CPUTRACE_RESULT_BMISS),
    HW_PROFILE_INS   = (1ULL << CPUTRACE_RESULT_INS),
};

#define HWProfileFunctionF(var, name, flags) HW_profile var(name, __COUNTER__ + 1, flags)

struct cputrace_anchor_result {
    cputrace_result_type type;
    uint64_t value;
};

struct ArenaRegion {
    void* start;
    void* end;
    void* current;
    ArenaRegion* next;
};

struct Arena {
    ArenaRegion* region;
};

struct cputrace_anchor {
    const char* name;
    pthread_mutex_t mutex[CPUTRACE_MAX_THREADS];
    Arena* thread_arena[CPUTRACE_MAX_THREADS];
    uint64_t global_sum[CPUTRACE_RESULT_COUNT];
    uint64_t call_count;
    uint64_t flags;
};

struct cputrace_profiler {
    cputrace_anchor* anchors;
    bool profiling;
    pthread_mutex_t global_lock;
};

struct HW_conf {
    bool capture_swi;
    bool capture_cyc;
    bool capture_cmiss;
    bool capture_bmiss;
    bool capture_ins;
};

struct HW_ctx {
    int parent_fd;
    int fd_swi;
    int fd_cyc;
    int fd_cmiss;
    int fd_bmiss;
    int fd_ins;
    uint64_t id_swi;
    uint64_t id_cyc;
    uint64_t id_cmiss;
    uint64_t id_bmiss;
    uint64_t id_ins;
    struct HW_conf conf;
};

class HW_profile {
public:
    HW_profile(const char* function, uint64_t index, uint64_t flags);
    ~HW_profile();

private:
    const char* function;
    uint64_t index;
    uint64_t flags;
    struct HW_ctx ctx;
};

void cputrace_start(ceph::Formatter* f);
void cputrace_stop(ceph::Formatter* f);
void cputrace_reset(ceph::Formatter* f);
void cputrace_dump(ceph::Formatter* f, const std::string& logger = "", const std::string& counter = "");
void cputrace_print_to_stringstream(std::stringstream& ss);
void cputrace_flush_thread_start();
void cputrace_flush_thread_stop();