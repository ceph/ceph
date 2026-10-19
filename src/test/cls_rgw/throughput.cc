// Direct live CLS throughput companion. Preparation and verification are untimed.
#include "cls/rgw/cls_rgw_client.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <time.h>
#include <unistd.h>

namespace {

using bytes = ::ceph::buffer::list;
using clock_type = std::chrono::steady_clock;

void require(const bool condition, const std::string_view message)
{
    if (!condition) {
        throw std::runtime_error(std::string {message});
    }
}

void success(const int status)
{
    if (status < 0) {
        throw std::runtime_error("Ceph status " + std::to_string(status));
    }
}

template <typename T>
T number(const std::string_view text)
{
    T result {};
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
    require(parsed.ec == std::errc {} && parsed.ptr == text.data() + text.size(), "Invalid numeric argument");
    return result;
}

std::uint64_t cpu_ns()
{
    timespec value {};
    require(0 == clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &value), "Client CPU clock unavailable");
    return std::uint64_t(value.tv_sec) * 1000000000 + value.tv_nsec;
}

std::uint64_t osd_ticks()
{
    const char *pid = std::getenv("BENCH_OSD_PID");
    require(pid && std::string_view {pid}.find_first_not_of("0123456789") == std::string_view::npos,
            "BENCH_OSD_PID must identify the measured local OSD");
    std::ifstream file(std::string {"/proc/"} + pid + "/stat");
    std::string line;
    std::getline(file, line);
    const auto closing = line.rfind(')');
    require(closing != std::string::npos, "OSD process stat unavailable");
    std::istringstream fields(line.substr(closing + 2));
    std::string value;
    std::uint64_t ticks = 0;
    for (unsigned field = 0; field <= 12; ++field) {
        require(bool(fields >> value), "Invalid OSD process stat");
        if (field == 11 || field == 12) {
            ticks += number<std::uint64_t>(value);
        }
    }
    return ticks;
}

struct sample final
{
    std::uint64_t operations = 0;
    std::array<std::uint64_t, 200001> latency_us {};
    std::vector<std::uint64_t> slow_latency_us;
    std::exception_ptr failure;
};

template <typename ActionT>
void measure(const std::string_view name, const unsigned entries, const unsigned depth,
             const double seconds, librados::IoCtx& io, ActionT action)
{
    std::vector<librados::IoCtx> contexts(depth);
    for (auto& context : contexts) {
        context.dup(io);
    }
    std::vector<sample> samples(depth);
    std::vector<std::thread> workers;
    workers.reserve(depth);
    std::barrier ready(depth + 1);
    std::atomic<bool> failed = false;
    clock_type::time_point deadline;
    std::exception_ptr coordinator_failure;
    try {
        for (unsigned worker = 0; worker < depth; ++worker) {
            workers.emplace_back([&, worker] {
                try {
                    for (unsigned call = 0; call < 8; ++call) {
                        action(contexts[worker], worker);
                    }
                } catch (...) {
                    samples[worker].failure = std::current_exception();
                    failed = true;
                }
                ready.arrive_and_wait();
                ready.arrive_and_wait();
                try {
                    while (!failed && clock_type::now() < deadline) {
                        const auto started = clock_type::now();
                        action(contexts[worker], worker);
                        const auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                            clock_type::now() - started).count();
                        ++samples[worker].operations;
                        if (std::uint64_t(latency) < samples[worker].latency_us.size()) {
                            ++samples[worker].latency_us[latency];
                        } else {
                            samples[worker].slow_latency_us.push_back(latency);
                        }
                    }
                } catch (...) {
                    samples[worker].failure = std::current_exception();
                    failed = true;
                }
            });
        }
    } catch (...) {
        coordinator_failure = std::current_exception();
        failed = true;
        for (auto missing = workers.size(); missing < depth; ++missing) {
            ready.arrive_and_drop();
        }
    }
    // Workers finish identical warmups before the CPU snapshots and timed release.
    ready.arrive_and_wait();
    std::uint64_t before_osd = 0, before_cpu = 0;
    try {
        before_osd = osd_ticks();
        before_cpu = cpu_ns();
    } catch (...) {
        coordinator_failure = std::current_exception();
        failed = true;
    }
    const auto started = clock_type::now();
    deadline = started + std::chrono::duration_cast<clock_type::duration>(std::chrono::duration<double>(seconds));
    ready.arrive_and_wait();
    for (auto& worker : workers) {
        worker.join();
    }
    if (coordinator_failure) {
        std::rethrow_exception(coordinator_failure);
    }
    const double elapsed = std::chrono::duration<double>(clock_type::now() - started).count();
    const auto client_cpu = cpu_ns() - before_cpu;
    const double osd_cpu = double(osd_ticks() - before_osd) * 1e9 / sysconf(_SC_CLK_TCK);
    std::uint64_t operations = 0;
    std::array<std::uint64_t, 200001> latencies {};
    std::vector<std::uint64_t> slow_latencies;
    for (const auto& item : samples) {
        if (item.failure) {
            std::rethrow_exception(item.failure);
        }
        operations += item.operations;
        for (std::size_t bin = 0; bin < latencies.size(); ++bin) {
            latencies[bin] += item.latency_us[bin];
        }
        slow_latencies.insert(slow_latencies.end(), item.slow_latency_us.begin(),
                              item.slow_latency_us.end());
    }
    require(operations > 0, "No completed measured requests");
    std::ranges::sort(slow_latencies);
    auto percentile = [&](const double fraction) -> std::uint64_t {
        const auto target = std::uint64_t(std::ceil(fraction * operations));
        std::uint64_t count = 0;
        for (std::size_t bin = 0; bin < latencies.size(); ++bin) {
            count += latencies[bin];
            if (count >= target) {
                return bin;
            }
        }
        return slow_latencies.at(target - count - 1);
    };
    std::cout << name << ',' << depth << ',' << entries << ',' << operations << ','
              << std::fixed << std::setprecision(6) << elapsed << ',' << operations / elapsed << ','
              << entries * operations / elapsed << ',' << client_cpu / double(operations) << ','
              << osd_cpu / operations << ',' << percentile(.50) << ',' << percentile(.99) << ','
              << percentile(1.0) << ',' << slow_latencies.size() << '\n';
    std::cout.flush();
}

struct objects final
{
    librados::IoCtx& io;
    std::vector<std::string> names;

public:
    void create(const std::string& name)
    {
        names.push_back(name);
        bytes empty;
        success(io.write_full(name, empty));
    }

    void remove_all()
    {
        for (const auto& name : names) {
            success(io.remove(name));
        }
        names.clear();
    }

    ~objects()
    {
        for (const auto& name : names) {
            if (const int status = io.remove(name); status < 0 && status != -ENOENT) {
                std::cerr << "Owned object cleanup failed: " << name << " status " << status << '\n';
            }
        }
    }

};

rgw_cls_bi_entry entry(const unsigned index, const bool fragmented)
{
    rgw_bucket_dir_entry value;
    value.key.name = "key-" + std::to_string(index);
    value.exists = true;
    value.meta.category = RGWObjCategory::Main;
    value.meta.size = 64;
    value.meta.accounted_size = 64;
    rgw_cls_bi_entry result {BIIndexType::Plain, value.key.name, {}};
    ::encode(value, result.data);
    if (fragmented) {
        for (unsigned fragment = 0; fragment < 8; ++fragment) {
            ::ceph::buffer::ptr part {::ceph::buffer::create(512)};
            part.zero();
            result.data.append(std::move(part));
        }
    }
    return result;
}

void verify_values(librados::IoCtx& io, const std::string& oid,
                   const std::vector<rgw_cls_bi_entry>& expected)
{
    std::map<std::string, bytes> actual;
    bool more = true;
    success(io.omap_get_vals2(oid, "", "", 1001, &actual, &more));
    require(!more && actual.size() == expected.size(), "Stored BI cardinality mismatch");
    for (const auto& row : expected) {
        require(actual.at(row.idx).contents_equal(row.data), "Stored BI bytes mismatch");
    }
}
} // anonymous namespace

int main(const int argc, char *argv[]) try
{
    constexpr auto usage = "Usage: ceph_perf_cls_rgw CONF POOL SECONDS DEPTH NAMESPACE\n"
        "Seven cases: native-stat; BI list 1/1000; OLH read 1/1000; single BI put; batch64 BI put.\n"
        "SECONDS is per case (0..30); DEPTH is 1..8. BENCH_OSD_PID supplies local OSD CPU.\n";
    if (argc == 2 && std::string_view {argv[1]} == "--help") {
        std::cout << usage;
        return 0;
    }
    require(argc == 6, usage);
    const double seconds = number<double>(argv[3]);
    const auto depth = number<unsigned>(argv[4]);
    require(std::isfinite(seconds) && seconds > 0 && seconds <= 30 && depth > 0 && depth <= 8,
            "Seconds must be >0 and <=30; depth must be 1..8");
    require(!std::string_view {argv[5]}.empty(), "Use a unique owned namespace");
    librados::Rados cluster;
    success(cluster.init("admin"));
    success(cluster.conf_read_file(argv[1]));
    success(cluster.connect());
    librados::IoCtx io;
    success(cluster.ioctx_create(argv[2], io));
    io.set_namespace(argv[5]);
    require(io.nobjects_begin() == io.nobjects_end(), "Use an empty, uniquely owned namespace");
    objects owned {io, {}};
    std::cout << "case,depth,entries_per_operation,operations,seconds,ops_per_second,entries_per_second,"
        "client_cpu_ns_per_op,osd_cpu_ns_per_op,p50_us,p99_us,max_us,histogram_slow_count\n";
    owned.create("native-stat");
    measure("native-stat", 1, depth, seconds, io, [](auto& context, unsigned) {
        std::uint64_t size;
        time_t modified;
        success(context.stat("native-stat", &size, &modified));
        require(0 == size, "Control object length mismatch");
    });
    for (const unsigned count : {1u, 1000u}) {
        const auto oid = "bi-list-" + std::to_string(count);
        owned.create(oid);
        std::map<std::string, bytes> values;
        for (unsigned index = 0; index < count; ++index) {
            auto row = entry(index, false);
            values.emplace(std::move(row.idx), std::move(row.data));
        }
        success(io.omap_set(oid, values));
        auto list = [&](auto& context) {
            std::list<rgw_cls_bi_entry> rows;
            bool more = true;
            success(cls_rgw_bi_list(context, oid, "", "", 1000, &rows, &more));
            require(rows.size() == count && !more, "BI reply cardinality mismatch");
            return rows;
        };
        const auto rows = list(io);
        for (const auto& row : rows) {
            require(values.at(row.idx).contents_equal(row.data), "BI fixture bytes mismatch");
        }
        measure(oid, count, depth, seconds, io, [&](auto& context, unsigned) { list(context); });
    }
    for (const unsigned count : {1u, 1000u}) {
        auto oid = "olh-read-" + std::to_string(count);
        owned.create(oid);
        rgw_bucket_olh_entry stored;
        stored.key.name = "object";
        stored.tag = "tag";
        for (unsigned epoch = 1; epoch <= count; ++epoch) {
            auto& event = stored.pending_log[epoch].emplace_back();
            event.epoch = epoch;
            event.op = CLS_RGW_OLH_OP_LINK_OLH;
            event.key.name = std::string(count == 1 ? 12 : 128, 'n');
            event.key.instance = "instance";
            event.op_tag = "operation";
        }
        bytes encoded;
        ::encode(stored, encoded);
        success(io.omap_set(oid, {{std::string {"\x80" "1001_object"}, encoded}}));
        auto read_log = [&](auto& context) {
            rgw_cls_read_olh_log_ret result;
            success(cls_rgw_get_olh_log(context, oid, stored.key, 0, stored.tag, result));
            require(result.log.size() == count && !result.is_truncated, "OLH reply cardinality mismatch");
            return result;
        };
        const auto result = read_log(io);
        for (const auto& [epoch, events] : result.log) {
            require(events.size() == 1 && events.front().epoch == epoch &&
                    events.front().key == stored.pending_log.at(epoch).front().key,
                    "OLH fixture content mismatch");
        }
        measure(oid, count, depth, seconds, io, [&](auto& context, unsigned) { read_log(context); });
    }
    const std::vector<rgw_cls_bi_entry> single {entry(0, true)};
    std::vector<rgw_cls_bi_entry> batch;
    for (unsigned index = 0; index < 64; ++index) {
        batch.push_back(entry(index, true));
    }
    for (const bool multiple : {false, true}) {
        std::vector<std::string> targets;
        for (unsigned worker = 0; worker < depth; ++worker) {
            targets.push_back(std::string {multiple ? "batch-" : "single-"} + std::to_string(worker));
            owned.create(targets.back());
        }
        const auto& rows = multiple ? batch : single;
        auto put = [&](auto& context, const unsigned worker) {
            librados::ObjectWriteOperation operation;
            if (multiple) {
                cls_rgw_bi_put_entries(operation, rows, true);
            } else {
                cls_rgw_bi_put(operation, targets[worker], rows.front());
            }
            success(context.operate(targets[worker], &operation));
        };
        for (unsigned worker = 0; worker < depth; ++worker) {
            put(io, worker);
            verify_values(io, targets[worker], rows);
        }
        measure(multiple ? "bi-put-batch64-fragmented" : "bi-put-single-fragmented",
                rows.size(), depth, seconds, io, put);
        for (const auto& target : targets) {
            verify_values(io, target, rows);
            bytes header;
            success(io.omap_get_header(target, &header));
            if (!multiple) {
                require(0 == header.length(), "Single put changed bucket header");
                continue;
            }
            rgw_bucket_dir_header value;
            auto cursor = header.cbegin();
            ::decode(value, cursor);
            const auto& stats = value.stats.at(RGWObjCategory::Main);
            require(stats.num_entries == rows.size() && stats.total_size == 64 * rows.size(),
                    "Batch overwrite accounting mismatch");
        }
    }
    owned.remove_all();
    std::cerr << "Verified seven live cases and removed all owned objects.\n";
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
