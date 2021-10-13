#pragma once
#include <chrono>
#include <thread>
#include <condition_variable>
#include <iostream>
#include <unordered_map>
#include <atomic>
#include <string.h>

struct RGWQoSInfo {
  int64_t max_write_ops;
  int64_t max_read_ops;
  int64_t max_write_bytes;
  int64_t max_read_bytes;
  bool enabled = false;
  RGWQoSInfo()
    : max_write_ops(0), max_read_ops(0), max_write_bytes(0), max_read_bytes(0)  {}
};
class QosDatastruct {
  enum {
    RGW_QOS_OPS,
    RGW_QOS_CONCURR,
    RGW_QOS_BW,
  };
  struct qos_entry {
    // 3 is the enum size
    std::atomic_int64_t read[3] = {0};
    std::atomic_int64_t write[3] = {0};
    int64_t ts = 0;
    std::mutex ts_lock;
  };
  const size_t map_size = 2000000; // will create it with the closest upper prime number
  std::mutex insert_lock;
  std::condition_variable *cv;
  std::atomic_bool *replacing;
  typedef std::unordered_map<std::string, qos_entry> hash_map;
  hash_map qos_entries{map_size};
  bool is_read_op(const char *method) {
    if((strcmp(method,"GET") == 0) || (strcmp(method,"HEAD") == 0))
      return true;
    return false;
  }
    // find or create an entry, and return its iterator
  auto find_or_create(const std::string& key) {
    insert_lock.lock();
    if(qos_entries.size() > 0.9 * map_size && *replacing == false)
    {
        *replacing = true;
        cv->notify_all();
    }
    auto ret = qos_entries.emplace(std::piecewise_construct,
                               std::forward_as_tuple(key),
                               std::forward_as_tuple()).first;
    insert_lock.unlock();
    return ret;
  }

  bool increase_entry_read(hash_map::iterator& it, int64_t ops_limit, int64_t bw_limit) {
    int64_t ops = it->second.read[RGW_QOS_OPS];
    int64_t concur = ++it->second.read[RGW_QOS_CONCURR];
    int64_t bw = it->second.read[RGW_QOS_BW];
    //check if tenants did not reach their bw or ops limits and that the limits are not 0 (which is unlimited)
    if(((ops - 1 < 0 ||
      concur > ops_limit) && (ops_limit > 0)) ||
      (bw < 0 && bw_limit > 0))
      return true;
    // we don't want to reduce ops' tokens if we've rejected it.
    --it->second.read[RGW_QOS_OPS];
    return false;
  }
  
  bool increase_tokens(hash_map::iterator& it, int64_t curr_timestamp,
                       RGWQoSInfo& tenant) 
  {
    it->second.ts_lock.lock();
    int64_t read_ops = (tenant.max_read_ops * ((double)(curr_timestamp - it->second.ts) / std::nano::den));
    int64_t read_bw = (tenant.max_read_bytes * ((double)(curr_timestamp - it->second.ts) / std::nano::den));
    int64_t write_ops = (tenant.max_write_ops * ((double)(curr_timestamp - it->second.ts) / std::nano::den));
    int64_t write_bw = (tenant.max_write_bytes * ((double)(curr_timestamp - it->second.ts) / std::nano::den));
    it->second.ts = curr_timestamp;
    
    it->second.read[RGW_QOS_OPS] = std::min(tenant.max_read_ops, read_ops + it->second.read[RGW_QOS_OPS]);
    it->second.read[RGW_QOS_BW] = std::min(tenant.max_read_bytes, read_bw + it->second.read[RGW_QOS_BW]);
    
    it->second.write[RGW_QOS_OPS] = std::min(tenant.max_write_ops, write_ops + it->second.write[RGW_QOS_OPS]);
    it->second.write[RGW_QOS_BW] = std::min(tenant.max_write_bytes, write_bw + it->second.write[RGW_QOS_BW]);
    it->second.ts_lock.unlock();


    return true;
  }

  bool increase_entry_write(hash_map::iterator& it, int64_t ops_limit, int64_t bw_limit) {
    int64_t ops = it->second.write[RGW_QOS_OPS];
    int64_t concur = ++it->second.write[RGW_QOS_CONCURR];
    int64_t bw = it->second.write[RGW_QOS_BW];
    //check if tenants did not reach their bw or ops limits and that the limits are not 0 (which is unlimited)
    if(((ops - 1 < 0 ||
      concur > ops_limit) && (ops_limit > 0)) ||
      (bw < 0 && bw_limit > 0))
      return true;

    // we don't want to reduce ops' tokens if we've rejected it.
    --it->second.write[RGW_QOS_OPS];
    return false;
  }
  public:
    QosDatastruct(const QosDatastruct&) = delete;
    QosDatastruct& operator =(const QosDatastruct&) = delete;
    QosDatastruct(QosDatastruct&&) = delete;
    QosDatastruct& operator =(QosDatastruct&&) = delete;
    QosDatastruct() {};
    QosDatastruct(std::atomic_bool *replacing, std::condition_variable *cv) {
      this->replacing = replacing;
      this->cv = cv;
    };
    bool increase_entry(const char *method, std::string tenant, std::chrono::time_point<std::chrono::system_clock> curr_timestamp, RGWQoSInfo qos_info) {
      if(tenant.empty() || tenant.length() == 1 || !qos_info.enabled)
        return false;
      bool is_read = is_read_op(method);
      // it is possible that some requests will have the same ts, because we are using coarse real time
      int64_t ts = curr_timestamp.time_since_epoch().count();
      auto it = find_or_create(tenant);
      increase_tokens(it, ts, qos_info);
      if(is_read)
        return increase_entry_read(it, qos_info.max_read_ops, qos_info.max_read_bytes);
      return increase_entry_write(it, qos_info.max_write_ops, qos_info.max_write_bytes);
    }
    bool decrease_concurrent_ops(const char *method, std::string tenant) {
      if(tenant.empty() || tenant.length() == 1)
        return false;
      bool is_read = is_read_op(method);
      auto it = find_or_create(tenant);
      if (is_read && it->second.read[RGW_QOS_CONCURR] > 0)
        it->second.read[RGW_QOS_CONCURR]--;
      else if (!is_read && it->second.write[RGW_QOS_CONCURR] > 0) {
        it->second.write[RGW_QOS_CONCURR]--;
      }
      std::cout << "ratelimit read concurrency: " <<  it->second.read[RGW_QOS_CONCURR] <<
                  " write concurrency: " << it->second.write[RGW_QOS_CONCURR] << std::endl;
      return true;
    }
    bool increase_bw(const char *method, std::string tenant, int64_t amount, RGWQoSInfo& info) {
      if(tenant.empty() || tenant.length() == 1 || !info.enabled)
        return false;
      bool is_read = is_read_op(method);
      auto it = find_or_create(tenant);
      // we don't want the tenant to be with higher debt than 60 seconds of its limit
      if (is_read)
        it->second.read[RGW_QOS_BW] = std::max(it->second.read[RGW_QOS_BW] - amount,info.max_read_bytes * -60);
      else {
        it->second.write[RGW_QOS_BW] = std::max(it->second.write[RGW_QOS_BW] - amount,info.max_write_bytes * -60);
      }
      std::cout << "rate limiting read bytes is: " << it->second.read[RGW_QOS_BW] <<
                  " write bytes is: " <<  it->second.write[RGW_QOS_BW] << std::endl;
      std::cout << "rate limiting tenant is: " << tenant << " bytes added: " << amount << 
      " method is: " << method << std::endl;
      return true;
    }
    void clear() {
      qos_entries.clear();
    }
};
// This class purpose is to hold 2 QosDatastruct instances, one active and one passive.
// once the active has reached the watermark for clearing it will call the replace_active() thread using cv
// The replace_active will clear the previous QosDatastruct after all requests to it has been done (use_count() > 1)
// In the meanwhile new requests will come into the newer active
class QosActiveDatastruct {
  std::atomic_uint8_t stopped = {false};
  std::condition_variable cv;
  std::mutex cv_m;
  std::thread runner;
  std::atomic_bool replacing = false;
  std::atomic_uint8_t current_active = 0;
  std::shared_ptr<QosDatastruct> qos[2];
  void replace_active() {
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lk(cv_m);
    while(!stopped) {
      cv.wait(lk);
      current_active = current_active ^ 1;
      std::cout << "replacing active qos data structure" << std::endl;
      while (qos[(current_active ^ 1)].use_count() > 1 && !stopped) {
        if (cv.wait_for(lk, 1min) != std::cv_status::timeout && stopped)
          return;
      }
      if (stopped)
        return;
      std::cout << "clearing passive qos data structure" << std::endl;
      qos[(current_active ^ 1)]->clear();
      replacing = false;
    }
  }
  public:
    QosActiveDatastruct(const QosActiveDatastruct&) = delete;
    QosActiveDatastruct& operator =(const QosActiveDatastruct&) = delete;
    QosActiveDatastruct(QosActiveDatastruct&&) = delete;
    QosActiveDatastruct& operator =(QosActiveDatastruct&&) = delete;
    QosActiveDatastruct() {
      qos[0] = std::shared_ptr<QosDatastruct>(new QosDatastruct(&replacing,&cv));
      qos[1] = std::shared_ptr<QosDatastruct>(new QosDatastruct(&replacing,&cv));
    }
    ~QosActiveDatastruct() {
      std::cout << "stopping ratelimit_gc thread" << std::endl;
      stopped = true;
      cv.notify_all();
      if(runner.joinable()) {
        runner.join();
      }
    }
    std::shared_ptr<QosDatastruct> get_active() {
      return qos[current_active];
    }
    void start() {
      std::cout << "starting ratelimit_gc thread" << std::endl;
      runner = std::thread(&QosActiveDatastruct::replace_active, this);
    }
};