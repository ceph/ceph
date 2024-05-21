#include "rgw_ratelimit.h"
#include "rgw_common.h"
#include "random"
#include <cstdlib>
#include <string>
#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/steady_timer.hpp>
#include <spawn/spawn.hpp>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <boost/program_options.hpp>


using Executor = boost::asio::io_context::executor_type;
std::uniform_int_distribution<unsigned int> dist(0, 1);
std::random_device rd;
std::default_random_engine rng{rd()};
std::uniform_int_distribution<unsigned long long> disttenant(2, 100000000);
struct client_info {
    uint64_t accepted = 0;
    uint64_t rejected = 0;
    uint64_t ops = 0;
    uint64_t bytes = 0;
    uint64_t num_retries = 0;
    std::string tenant;
};

struct parameters {
    int64_t req_size = 1;
    int64_t backend_bandwidth = 1;
    size_t wait_between_retries_ms = 1;
    int num_clients = 1;
};
std::shared_ptr<std::vector<client_info>> ds = std::make_shared<std::vector<client_info>>(std::vector<client_info>());

std::string method[2] = {"PUT", "GET"};
void simulate_transfer(client_info& it, const RGWRateLimitInfo* info, std::shared_ptr<RateLimiter> ratelimit, const parameters& params, spawn::yield_context& yield, boost::asio::io_context& ioctx)
{
    auto dout = DoutPrefix(g_ceph_context, ceph_subsys_rgw, "rate limiter: ");
    boost::asio::steady_timer timer(ioctx);
    int rw = 0; // will always use PUT method as there is no difference
    std::string methodop(method[rw]);
    auto req_size = params.req_size;
    auto backend_bandwidth = params.backend_bandwidth;
// the 4 * 1024 * 1024 is the RGW default we are sending in a typical environment
    while (req_size) {
        if (req_size <= backend_bandwidth) {
            while (req_size > 0) {
                if(req_size > 4*1024*1024) {
                    ratelimit->decrease_bytes(methodop.c_str(),it.tenant, 4*1024*1024, info);
                    it.bytes += 4*1024*1024;
                    req_size = req_size - 4*1024*1024;
                }
                else {
                    ratelimit->decrease_bytes(methodop.c_str(),it.tenant, req_size, info);
                    req_size = 0;
                }
            }
        } else {
                int64_t total_bytes = 0;
                while (req_size > 0) {
                if (req_size >= 4*1024*1024) {
                    if (total_bytes >= backend_bandwidth)
                    {
                        timer.expires_after(std::chrono::seconds(1));
                        timer.async_wait(yield);
                        total_bytes = 0;
                    }
                    ratelimit->decrease_bytes(methodop.c_str(),it.tenant, 4*1024*1024, info);
                    it.bytes += 4*1024*1024;
                    req_size = req_size - 4*1024*1024;
                    total_bytes += 4*1024*1024;
                }
                else {
                    ratelimit->decrease_bytes(methodop.c_str(),it.tenant, req_size, info);
                    it.bytes += req_size;
                    total_bytes += req_size;
                    req_size = 0;
                }
            }
        }
    }
}
bool simulate_request(client_info& it, const RGWRateLimitInfo& info, std::shared_ptr<RateLimiter> ratelimit)
{
    boost::asio::io_context context;
    auto time = ceph::coarse_real_clock::now();
    int rw = 0; // will always use PUT method as there is no different
    std::string methodop = method[rw];
    auto dout = DoutPrefix(g_ceph_context, ceph_subsys_rgw, "rate limiter: ");
    bool to_fail = ratelimit->should_rate_limit(methodop.c_str(), it.tenant, time, &info);
    if(to_fail)
    {
        it.rejected++;
        it.ops++;
        return true;
    }
    it.accepted++;
    return false;
}
void simulate_client(client_info& it, const RGWRateLimitInfo& info, std::shared_ptr<RateLimiter> ratelimit, const parameters& params, spawn::yield_context& ctx, bool& to_run, boost::asio::io_context& ioctx)
{
    for (;;)
    {
                    bool to_retry = simulate_request(it, info, ratelimit);
                    while (to_retry && to_run)
                    {
                        if (params.wait_between_retries_ms)
                        {
                            boost::asio::steady_timer timer(ioctx);
                            timer.expires_after(std::chrono::milliseconds(params.wait_between_retries_ms));
                            timer.async_wait(ctx);
                        }
                        to_retry = simulate_request(it, info, ratelimit);
                    }
                    if (!to_run)
                    {
                        return;
                    }
                    simulate_transfer(it, &info, ratelimit, params, ctx, ioctx);
    }
}
void simulate_clients(boost::asio::io_context& context, std::string tenant, const RGWRateLimitInfo& info, std::shared_ptr<RateLimiter> ratelimit, const parameters& params, bool& to_run)
{
    for (int i = 0; i < params.num_clients; i++)
    {
        auto& it = ds->emplace_back(client_info());
        it.tenant = tenant;
        int x = ds->size() - 1;
        spawn::spawn(context,
                [&to_run ,x, ratelimit, info, params, &context](spawn::yield_context ctx)
                {
                    auto& it = ds.get()->operator[](x);
                    simulate_client(it, info, ratelimit, params, ctx, to_run, context);
                });
    }
}
int main(int argc, char **argv)
{
    int num_ratelimit_classes = 1;
    int64_t ops_limit = 1;
    int64_t bw_limit = 1;
    int thread_count = 512;
    int runtime = 60;
    parameters params;
    try
    {
        using namespace boost::program_options;
        options_description desc{"Options"};
        desc.add_options()
        ("help,h", "Help screen")
        ("num_ratelimit_classes", value<int>()->default_value(1), "how many ratelimit tenants")
        ("request_size", value<int64_t>()->default_value(1), "what is the request size we are testing if 0, it will be randomized")
        ("backend_bandwidth", value<int64_t>()->default_value(1), "what is the backend bandwidth, so there will be wait between decrease_bytes")
        ("wait_between_retries_ms", value<size_t>()->default_value(1), "time in seconds to wait between retries")
        ("ops_limit", value<int64_t>()->default_value(1), "ops limit for the tenants")
        ("bw_limit", value<int64_t>()->default_value(1), "bytes per second limit")
        ("threads", value<int>()->default_value(512), "server's threads count")
        ("runtime", value<int>()->default_value(60), "For how many seconds the test will run")
        ("num_clients", value<int>()->default_value(1), "number of clients per tenant to run");
        variables_map vm;
        store(parse_command_line(argc, argv, desc), vm);
        if (vm.count("help")) {
            std::cout << desc << std::endl;
            return EXIT_SUCCESS;
        }
        num_ratelimit_classes = vm["num_ratelimit_classes"].as<int>();
        params.req_size = vm["request_size"].as<int64_t>();
        params.backend_bandwidth = vm["backend_bandwidth"].as<int64_t>();
        params.wait_between_retries_ms = vm["wait_between_retries_ms"].as<size_t>();
        params.num_clients = vm["num_clients"].as<int>();
        ops_limit = vm["ops_limit"].as<int64_t>();
        bw_limit = vm["bw_limit"].as<int64_t>();
        thread_count = vm["threads"].as<int>();
        runtime = vm["runtime"].as<int>();
    }
    catch (const boost::program_options::error &ex)
    {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    RGWRateLimitInfo info;
    info.enabled = true;
    info.max_read_bytes = bw_limit;
    info.max_write_bytes = bw_limit;
    info.max_read_ops = ops_limit;
    info.max_write_ops = ops_limit;
    std::unique_ptr<CephContext> cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_ANY);
    if (!g_ceph_context)
    {
        g_ceph_context = cct.get();
    }
    std::shared_ptr<ActiveRateLimiter> ratelimit(new ActiveRateLimiter(g_ceph_context));
    ratelimit->start();
    std::vector<std::thread> threads;
    using Executor = boost::asio::io_context::executor_type;
    std::optional<boost::asio::executor_work_guard<Executor>> work;
    threads.reserve(thread_count);
    boost::asio::io_context context;
    boost::asio::io_context stopme;
    work.emplace(boost::asio::make_work_guard(context));
    // server execution
    for (int i = 0; i < thread_count; i++) {
      threads.emplace_back([&]() noexcept {
        context.run();
      });
    }
    //client execution
    bool to_run = true;
    ds->reserve(num_ratelimit_classes*params.num_clients);
    for (int i = 0; i < num_ratelimit_classes; i++)
    {
        unsigned long long tenantid = disttenant(rng);
        std::string tenantuser = "uuser" + std::to_string(tenantid);
        simulate_clients(context, tenantuser, info, ratelimit->get_active(), params, to_run);
    }
    boost::asio::steady_timer timer_runtime(stopme);
    timer_runtime.expires_after(std::chrono::seconds(runtime));
    timer_runtime.wait();
    work.reset();
    context.stop();
    to_run = false;

    for (auto& i : threads)
    {
        i.join();
    }
    std::unordered_map<std::string,client_info> metrics_by_tenant;
    for(auto& i : *ds.get())
    {
        auto it = metrics_by_tenant.emplace(i.tenant, client_info()).first;
        std::cout << i.accepted << std::endl;
        it->second.accepted += i.accepted;
        it->second.rejected += i.rejected;
    }
    // TODO sum the results by tenant
    for(auto& i : metrics_by_tenant)
    {
        std::cout << "Tenant is: " << i.first << std::endl;
        std::cout << "Simulator finished accepted  sum : " << i.second.accepted << std::endl;
        std::cout << "Simulator finished rejected  sum : " << i.second.rejected << std::endl;
    }

    return 0;
}
