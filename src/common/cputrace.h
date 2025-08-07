#pragma once
#include <pthread.h>
#include <stdint.h>
#include <string>
#include "common/Formatter.h"

#define CPUTRACE_MAX_ANCHORS 10
#define CPUTRACE_MAX_THREADS 64

enum cputrace_flags {
    HW_PROFILE_SWI   = (1ULL << 0),
    HW_PROFILE_CYC   = (1ULL << 1),
    HW_PROFILE_CMISS = (1ULL << 2),
    HW_PROFILE_BMISS = (1ULL << 3),
    HW_PROFILE_INS   = (1ULL << 4),
};

#define HWProfileFunctionF(var, name, flags) HW_profile var(name, __COUNTER__ + 1, flags)

struct results {
    uint64_t call_count;
    uint64_t swi;
    uint64_t cyc;
    uint64_t cmiss;
    uint64_t bmiss;
    uint64_t ins;
};

struct cputrace_anchor {
    const char* name;
    pthread_mutex_t lock;
    results global_results;
    uint64_t flags;
};

struct cputrace_profiler {
    cputrace_anchor* anchors;
    bool profiling;
    pthread_mutex_t global_lock;
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
};

constexpr HW_ctx HW_ctx_empty = {
    -1, -1, -1, -1, -1, -1,
    0,  0,  0,  0,  0
};

struct sample_t {
    uint64_t swi  = 0;
    uint64_t cyc  = 0;
    uint64_t cmiss = 0;
    uint64_t bmiss = 0;
    uint64_t ins  = 0;
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

void HW_init(HW_ctx* ctx, uint64_t flags);
void HW_read(HW_ctx* ctx, sample_t* mesaure);
void HW_clean(HW_ctx* ctx);

void cputrace_start();
void cputrace_stop();
void cputrace_reset();
void cputrace_start(ceph::Formatter* f);
void cputrace_stop(ceph::Formatter* f);
void cputrace_reset(ceph::Formatter* f);
void cputrace_dump(ceph::Formatter* f, const std::string& logger = "", const std::string& counter = "");
void cputrace_print_to_stringstream(std::stringstream& ss);