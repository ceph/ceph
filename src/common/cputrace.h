#ifndef CPUTRACE_H
#define CPUTRACE_H

/*
 * CpuTrace: lightweight hardware performance counter profiling
 *
 * See detailed documentation and usage examples in:
 *   doc/dev/cputrace.rst
 *
 * This header provides the public interface for CpuTrace,
 * including profiling helpers (HW_profile, HW_guard),
 * measurement structures, and low-level initialization routines.
 */

#include <pthread.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <mutex>
#include <sstream>
#include <atomic>

#include "common/Formatter.h"
#include "include/ceph_assert.h"

#define CPUTRACE_MAX_ANCHORS 10
#define CPUTRACE_MAX_THREADS 64

enum cputrace_flags {
    HW_PROFILE_SWI   = (1ULL << 0),
    HW_PROFILE_CYC   = (1ULL << 1),
    HW_PROFILE_CMISS = (1ULL << 2),
    HW_PROFILE_BMISS = (1ULL << 3),
    HW_PROFILE_INS   = (1ULL << 4),
};

inline cputrace_flags operator|(cputrace_flags a, cputrace_flags b) {
    return static_cast<cputrace_flags>(
        static_cast<uint64_t>(a) | static_cast<uint64_t>(b));
}

inline cputrace_flags operator&(cputrace_flags a, cputrace_flags b) {
    return static_cast<cputrace_flags>(
        static_cast<uint64_t>(a) & static_cast<uint64_t>(b));
}

int register_anchor(const char* name);

#define HWProfileFunctionF(var, name, flags) \
  static int var##_id = register_anchor(name); \
  HW_profile var(name, var##_id, flags)

struct sample_t {
    uint64_t swi  = 0;
    uint64_t cyc  = 0;
    uint64_t cmiss = 0;
    uint64_t bmiss = 0;
    uint64_t ins  = 0;

    void operator=(const sample_t& other) {
        swi = other.swi;
        cyc = other.cyc;
        cmiss = other.cmiss;
        bmiss = other.bmiss;
        ins = other.ins;
    }

    sample_t operator-(const sample_t& other) const {
        sample_t result;
        result.swi = swi - other.swi;
        result.cyc = cyc - other.cyc;
        result.cmiss = cmiss - other.cmiss;
        result.bmiss = bmiss - other.bmiss;
        result.ins = ins - other.ins;
        return result;
    }
};

struct measurement_t {
    uint64_t call_count = 0;
    uint64_t sample_count = 0;
    uint64_t sum_swi = 0, sum_cyc = 0, sum_cmiss = 0, sum_bmiss = 0, sum_ins = 0;
    uint64_t non_zero_swi_count = 0;
    uint64_t zero_swi_count = 0;

    void sample(const sample_t& s) {
        sample_count += 1;
        if (s.swi > 0) {
            sum_swi += s.swi;
            non_zero_swi_count += 1;
        }
        if (s.swi == 0) {
            zero_swi_count += 1;
        }
        sum_cyc += s.cyc;
        sum_cmiss += s.cmiss;
        sum_bmiss += s.bmiss;
        sum_ins += s.ins;
    }

    void reset() {
        call_count = 0;
        sample_count = 0;
        non_zero_swi_count = 0;
        zero_swi_count = 0;
        sum_swi = sum_cyc = sum_cmiss = sum_bmiss = sum_ins = 0;
    }

    void dump(ceph::Formatter* f, cputrace_flags flags, const std::string& counter = "") const {
        f->open_object_section("metrics");
        f->dump_unsigned("sample_count", sample_count);
        if (flags & HW_PROFILE_SWI) {
            f->open_object_section("context_switches");
            f->dump_unsigned("non_zero_count", non_zero_swi_count);
            f->dump_unsigned("zero_count", zero_swi_count);
            f->dump_unsigned("total", sum_swi);
            if (sample_count) {
                f->dump_float("avg", (double)sum_swi / sample_count);
            }
            f->close_section();
        }

        auto dump_counter = [&](const std::string& name, uint64_t sum) {
            f->open_object_section(name.c_str());
            f->dump_unsigned("total", sum);
            if (sample_count) {
                f->dump_float("avg", static_cast<double>(sum) / sample_count);
            }
            f->close_section();
        };

        if (flags & HW_PROFILE_CYC && (counter.empty() || counter == "cpu_cycles"))
            dump_counter("cpu_cycles", sum_cyc);

        if (flags & HW_PROFILE_CMISS && (counter.empty() || counter == "cache_misses"))
            dump_counter("cache_misses", sum_cmiss);

        if (flags & HW_PROFILE_BMISS && (counter.empty() || counter == "branch_misses"))
            dump_counter("branch_misses", sum_bmiss);

        if (flags & HW_PROFILE_INS && (counter.empty() || counter == "instructions"))
            dump_counter("instructions", sum_ins);

        f->close_section();
    }

    void dump_to_stringstream(std::stringstream& ss, cputrace_flags flags) const {
        ss << "sample_count: " << sample_count << "\n";
        if (flags & HW_PROFILE_SWI) {
            ss << "\ncontext_switches:\n";
            ss << "    non_zero_count: " << non_zero_swi_count << "\n";
            ss << "    zero_count: " << zero_swi_count << "\n";
            ss << "    total: " << sum_swi << "\n";
            if (sample_count) {
                ss << "    avg  : " << (double)sum_swi / sample_count << "\n";
            }
        }

        auto dump_counter = [&](const std::string& name, uint64_t sum) {
            ss << name << ":\n";
            ss << "    total: " << sum << "\n";
            if (sample_count) {
                ss << "    avg  : " << (double)sum / sample_count << "\n";
            }
        };

        if (flags & HW_PROFILE_CYC)
            dump_counter("cpu_cycles", sum_cyc);

        if (flags & HW_PROFILE_CMISS)
            dump_counter("cache_misses", sum_cmiss);

        if (flags & HW_PROFILE_BMISS)
            dump_counter("branch_misses", sum_bmiss);

        if (flags & HW_PROFILE_INS)
            dump_counter("instructions", sum_ins);
    }
};

struct HW_ctx {
    int parent_fd = -1;
    int fd_swi = -1;
    int fd_cyc = -1;
    int fd_cmiss = -1;
    int fd_bmiss = -1;
    int fd_ins = -1;
    uint64_t id_swi = 0;
    uint64_t id_cyc = 0;
    uint64_t id_cmiss = 0;
    uint64_t id_bmiss = 0;
    uint64_t id_ins = 0;
};

extern HW_ctx HW_ctx_empty;

struct cputrace_anchor {
    const char* name = nullptr;
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    measurement_t global_results{};
    cputrace_flags flags = static_cast<cputrace_flags>(0);
    HW_ctx per_thread_ctx[CPUTRACE_MAX_THREADS]{};
    HW_ctx* active_contexts[CPUTRACE_MAX_THREADS] = {nullptr};
    sample_t start[CPUTRACE_MAX_THREADS]{};
    sample_t end[CPUTRACE_MAX_THREADS]{};
    bool is_capturing[CPUTRACE_MAX_THREADS] = {false};
    uint32_t nest_level[CPUTRACE_MAX_THREADS] = {0};
};

struct cputrace_profiler {
    cputrace_anchor* anchors = nullptr;
    std::atomic<bool> profiling{false};
};

class HW_profile {
public:
    HW_profile(const char* function, uint64_t index, cputrace_flags flags);
    ~HW_profile();

private:
    const char* function;
    uint64_t index;
    cputrace_flags flags;
    struct HW_ctx* ctx;
};

void HW_init(HW_ctx* ctx, cputrace_flags flags);
void HW_read(HW_ctx* ctx, sample_t* measure);
void HW_clean(HW_ctx* ctx);

class HW_guard {
public:
    HW_guard(HW_ctx* ctx, measurement_t* out_measurement)
        : ctx(ctx), meas(out_measurement) {
        if (ctx && meas) {
            HW_read(ctx, &start);
        }
    }
    ~HW_guard() {
        if (ctx && meas) {
            HW_read(ctx, &end);
            sample_t elapsed = end - start;
            meas->sample(elapsed);
        }
    }
private:
    HW_ctx* ctx{nullptr};
    measurement_t* meas{nullptr};
    sample_t start{}, end{};
};

class HW_named_guard {
public:
    HW_named_guard(const char* name, HW_ctx* ctx = nullptr);
    ~HW_named_guard();

private:
    const char* name = nullptr;
    HW_guard guard;
};

measurement_t* get_named_measurement(const std::string& name);

void cputrace_start(ceph::Formatter* f = nullptr);
void cputrace_stop(ceph::Formatter* f = nullptr);
void cputrace_reset(ceph::Formatter* f = nullptr);
void cputrace_dump(ceph::Formatter* f, const std::string& logger = "", const std::string& counter = "");
void cputrace_print_to_stringstream(std::stringstream& ss);

#endif
