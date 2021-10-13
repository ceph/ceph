#include "rgw_qos_tbb.h"
#include "random"
#include <cstdlib>
#include <string>
#include <future>
#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <boost/program_options.hpp>
#include <boost/bind.hpp>

using Executor = boost::asio::io_context::executor_type;
std::uniform_int_distribution<unsigned int> dist(0, 1);
std::random_device rd;
std::default_random_engine rng{rd()};
std::uniform_int_distribution<unsigned long long> disttenant(2, 100000000);
std::mutex lock;
struct client_info {
    uint64_t accepted = 0;
    uint64_t rejected = 0;
    uint64_t ops = 0;
    uint64_t bytes = 0;
    uint64_t num_retries = 0;
    std::string tenant;
};

struct req_info {
    size_t req_size = 1;
    size_t backend_bandwidth = 1;
    size_t wait_between_retries_ms = 1;
};
std::shared_ptr<std::vector<client_info>> ds = std::make_shared<std::vector<client_info>>(std::vector<client_info>()); 

std::string method[2] = {"PUT", "GET"};
// This function is simulating a single client request









/*
bool load_datastructure(std::shared_ptr<QosDatastruct> qos, std::string tenant, int req_size, int backend_bandwidth,  int ops_limit, int bw_limit, 
                        boost::asio::steady_timer& timer, boost::asio::yield_context ctx, const RGWQoSInfo& info)
{
    using namespace std::chrono_literals;
    int rw = 0; // will always use PUT method as there is no different
    std::string methodop = method[rw];
    auto time = std::chrono::system_clock::now();
    auto x = qos->increase_entry(methodop.c_str(), tenant, time, info);
    if(x)
    {
        stats->second.rejected++;
        stats->second.ops++;
        qos->decrease_concurrent_ops(methodop.c_str(),tenant);
        return true;
    }
    stats->second.accepted++;
    stats->second.bytes += req_size;
    stats->second.ops++;
// the 4 * 1024 * 1024 is the RGW default we are sending in a typical environment
    while (req_size) {
        if(req_size <= backend_bandwidth) {
            for(;req_size > 0;) {
                if(req_size > 4*1024*1024) {
                    qos->increase_bw(methodop.c_str(),tenant, 4*1024*1024, info);
                    req_size = req_size - 4*1024*1024;
                }
                else {
                    qos->increase_bw(methodop.c_str(),tenant, req_size, info);
                    req_size = 0;
                }
            }
        } else {
                uint64_t total_bytes = 0;
                for(;req_size > 0;) {
                if(req_size >= 4*1024*1024) {
                    if(total_bytes >= backend_bandwidth)
                    {
                        timer.expires_after(std::chrono::seconds(1));
                        timer.async_wait(ctx);
                        total_bytes = 0;
                    }
                    qos->increase_bw(methodop.c_str(),tenant, 4*1024*1024, info);
                    req_size = req_size - 4*1024*1024;
                    total_bytes += 4*1024*1024;
                }
                else {
                    qos->increase_bw(methodop.c_str(),tenant, req_size, info);
                    total_bytes += req_size;
                    req_size = 0;
                }
            }
        }
    }
    qos->decrease_concurrent_ops(methodop.c_str(),tenant);
    auto end = std::chrono::system_clock::now();
    return false;
}



















*/
void stop(boost::asio::io_context* context, std::optional<boost::asio::executor_work_guard<Executor>>& work)
{
    std::cerr << "stopping" << std::endl;
    work.reset();
    context->reset();
    context->stop();
}
void simulate_transfer(boost::asio::io_context& context, client_info& it, const RGWQoSInfo& info, std::shared_ptr<QosDatastruct> qos, const req_info& params)
{
    boost::asio::steady_timer timer(context);
    int rw = 0; // will always use PUT method as there is no different
    std::string methodop = method[rw];
    auto req_size = params.req_size;
    auto backend_bandwidth = params.backend_bandwidth;
// the 4 * 1024 * 1024 is the RGW default we are sending in a typical environment
    while (req_size) {
        if(req_size <= backend_bandwidth) {
            for(;req_size > 0;) {
                if(req_size > 4*1024*1024) {
                    qos->increase_bw(methodop.c_str(),it.tenant, 4*1024*1024, info);
                    it.bytes += 4*1024*1024;
                    req_size = req_size - 4*1024*1024;
                }
                else {
                    qos->increase_bw(methodop.c_str(),it.tenant, req_size, info);
                    req_size = 0;
                }
            }
        } else {
                uint64_t total_bytes = 0;
                for(;req_size > 0;) {
                if(req_size >= 4*1024*1024) {
                    if(total_bytes >= backend_bandwidth)
                    {
                        timer.expires_after(std::chrono::seconds(1));
                        timer.wait();
                        total_bytes = 0;
                    }
                    qos->increase_bw(methodop.c_str(),it.tenant, 4*1024*1024, info);
                    it.bytes += 4*1024*1024;
                    req_size = req_size - 4*1024*1024;
                    total_bytes += 4*1024*1024;
                }
                else {
                    qos->increase_bw(methodop.c_str(),it.tenant, req_size, info);
                    it.bytes += req_size;
                    total_bytes += req_size;
                    req_size = 0;
                }
            }
        }
    }
}
bool simulate_request(boost::asio::io_context& context, client_info& it, const RGWQoSInfo& info, std::shared_ptr<QosDatastruct> qos)
{
    auto time = std::chrono::system_clock::now();
    int rw = 0; // will always use PUT method as there is no different
    std::string methodop = method[rw];
    auto to_fail = qos->increase_entry(methodop.c_str(), it.tenant, time, info);
    if(to_fail)
    {
        it.rejected++;
        it.ops++;
        qos->decrease_concurrent_ops(methodop.c_str(),it.tenant);
        return true;
    }
    it.accepted++;
    return false;
}
void simulate_client(boost::asio::io_context& context, client_info& it, const RGWQoSInfo& info, std::shared_ptr<QosDatastruct> qos, const req_info& params)
{
    for(;;)
    {
        boost::asio::spawn(context,
                [&context,&it, qos, info, params](boost::asio::yield_context ctx)
                {
                    bool to_retry = simulate_request(context, it, info, qos);
                    while(to_retry)
                    {
                        boost::asio::steady_timer timer(context);
                        timer.expires_after(std::chrono::milliseconds(params.wait_between_retries_ms));
                        timer.async_wait(ctx);
                        to_retry = simulate_request(context, it, info, qos);
                    }
                    simulate_transfer(context, it, info, qos, params);
                    qos->decrease_concurrent_ops(method[0].c_str(),it.tenant); //simulate PUT request
                });
    }
}
void simulate_clients(boost::asio::io_context& context, std::string tenant, const RGWQoSInfo& info, std::shared_ptr<QosDatastruct> qos, const req_info& params)
{
    for(;;)
    {
        boost::asio::spawn(context,
                [&context, tenant, qos, info, params](boost::asio::yield_context ctx)
                {
                    auto& it = ds->emplace_back(client_info());
                    it.tenant = tenant;
                    simulate_client(context, it, info, qos, params);
                });
    }
}
int main(int argc, char **argv)
{
    int num_qos_classes = 1;
    int ops_limit = 1;
    int bw_limit = 1;
    int thread_count = 512;
    int runtime = 60;
    req_info params;
    try
    {
        using namespace boost::program_options;
        options_description desc{"Options"};
        desc.add_options()
        ("help,h", "Help screen")
        ("num_qos_classes", value<int>()->default_value(1), "how many qos tenants")
        ("request_size", value<size_t>()->default_value(1), "what is the request size we are testing if 0, it will be randomized")
        ("backend_bandwidth", value<size_t>()->default_value(1), "what is the backend bandwidth, so there will be wait between increase_bw")
        ("wait_between_retries_ms", value<size_t>()->default_value(1), "time in seconds to wait between retries")
        ("ops_limit", value<int>()->default_value(1), "ops limit for the tenants")
        ("bw_limit", value<int>()->default_value(1), "bytes per second limit")
        ("threads", value<int>()->default_value(512), "server's threads count")
        ("runtime", value<int>()->default_value(60), "For how many seconds the test will run");
        variables_map vm;
        store(parse_command_line(argc, argv, desc), vm);
        if (vm.count("help")) {
            std::cout << desc << std::endl;
            return EXIT_SUCCESS;
        }
        num_qos_classes = vm["num_qos_classes"].as<int>();
        params.req_size = vm["request_size"].as<size_t>();
        params.backend_bandwidth = vm["backend_bandwidth"].as<size_t>();
        params.wait_between_retries_ms = vm["wait_between_retries_ms"].as<size_t>();
        ops_limit = vm["ops_limit"].as<int>();
        bw_limit = vm["bw_limit"].as<int>();
        thread_count = vm["threads"].as<int>();
        runtime = vm["runtime"].as<int>();
    }
    catch (const boost::program_options::error &ex)
    {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    RGWQoSInfo info;
    info.enabled = true;
    info.max_read_bytes = bw_limit;
    info.max_write_bytes = bw_limit;
    info.max_read_ops = ops_limit;
    info.max_write_ops = ops_limit;

    std::shared_ptr<QosActiveDatastruct> qos(new QosActiveDatastruct());
    qos->start();
    std::vector<std::thread> threads;
    using Executor = boost::asio::io_context::executor_type;
    std::optional<boost::asio::executor_work_guard<Executor>> work;
    threads.reserve(thread_count);
    boost::asio::io_context context;
    work.emplace(boost::asio::make_work_guard(context));
    // server execution
    for (int i = 0; i < thread_count; i++) {
      threads.emplace_back([&]() noexcept {
        context.run();
      });
    }
    //client execution
    for(int i = 0; i < num_qos_classes; i++)
    {
        unsigned long long tenantid = disttenant(rng);
        std::string tenantuser = "uuser" + std::to_string(tenantid);
            boost::asio::spawn(context,
                [&context,tenantuser,info, qos, params](boost::asio::yield_context ctx) {
                    for(;;) {
                        simulate_clients(context, tenantuser, info, qos->get_active(), params);
                    }
                });
    }
    boost::asio::steady_timer timer_runtime(context);
    timer_runtime.expires_after(std::chrono::seconds(runtime));
    timer_runtime.async_wait(boost::bind(stop, &context, work));

    for(auto& i : threads)
        if (i.joinable())
        {
            i.join();
        }
        
    // TODO sum the results by tenant
    std::cerr << "Simulator finished" << std::endl;
    return 0;

}