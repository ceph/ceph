This tool has been built to evaluate the correctness of the rate limiting algorithm

Compile it:
g++ -pthread -ltbb -lboost_coroutine -lboost_system -lboost_program_options -std=c++2a simulator.cc -o simulator

How to use it:

Options:
  -h [ --help ]                      Help screen
  --num_reqs arg (=1)                how many requests per qos tenant
  --num_qos_classes arg (=1)         how many qos tenants
  --request_size arg (=1)            what is the request size we are testing if
                                     0, it will be randomized
  --backend_bandwidth arg (=1)       what is the backend bandwidth, so there 
                                     will be wait between increase_bw
  --num_of_retires arg (=1)          how many retries before fail
  --wait_between_retries_ms arg (=1) time in seconds to wait between retries
  --ops_limit arg (=1)               ops limit for the tenants
  --bw_limit arg (=1)                bytes per second limit
