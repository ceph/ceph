#include "rgw/rgw_ratelimit.h"
#include "rgw/rgw_common.h"
#include "random"
#include <cstdlib>
#include <string>
#include <chrono>
#include <boost/program_options.hpp>
int main(int argc, char **argv)
{
    int num_qos_classes = 1;
    try
    {
        using namespace boost::program_options;
        options_description desc{"Options"};
        desc.add_options()
        ("help,h", "Help screen")
        ("num_qos_classes", value<int>()->default_value(1), "how many qos tenants");
        variables_map vm;
        store(parse_command_line(argc, argv, desc), vm);
        if (vm.count("help")) {
            std::cout << desc << std::endl;
            return EXIT_SUCCESS;
        }
        num_qos_classes = vm["num_qos_classes"].as<int>();
    }
    catch (const boost::program_options::error &ex)
    {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    RGWRateLimitInfo info;
    info.enabled = true;
    info.max_read_bytes = 0;
    info.max_write_bytes = 0;
    info.max_read_ops = 0;
    info.max_write_ops = 0;
    std::unique_ptr<CephContext> cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_ANY);
    if (!g_ceph_context)
    {
        g_ceph_context = cct.get();
    }
    std::shared_ptr<ActiveRateLimiter> ratelimit(new ActiveRateLimiter(g_ceph_context));
    ratelimit->start();
    auto dout = DoutPrefix(g_ceph_context, ceph_subsys_rgw, "rate limiter: ");
    for(int i = 0; i < num_qos_classes; i++)
    {
        std::string tenant = "uuser" + std::to_string(i);
        auto time = ceph::coarse_real_clock::now();
        ratelimit->get_active()->should_rate_limit("PUT", tenant, time, &info);
    }

}
