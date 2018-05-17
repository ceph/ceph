#include "librados.hpp"


void client(uint32_t client_id, uint64_t resv, uint64_t wght, uint64_t limt) {

    Rados rados; //

    rados_qos_profile_t profile = rados.qos_profile_create(resv, wght, limt);
    assert(profile);

    uint64_t profile_id = rados.qos_profile_get_id(profile);

    std::cout << "client id " << client_id << " has qos profile id " <<
        profile_id << std::endl;

    
    
    int rv = rados.qos_profile_release(profile);
    assert(0 == rv);
}



int main(int argc, char* argv[]) {
    int rv = 0;
    Rados cluster;

    std::string pool_name = "dmclock_client_profile_test";

    
    rv = cluster.pool_create(pool_name);

    client();

    cluster.shutdown();
}
