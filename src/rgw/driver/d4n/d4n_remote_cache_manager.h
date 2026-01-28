#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/system/detail/errc.hpp>

#include <aio.h>
#include "rgw_common.h"
#include "rgw_sal_d4n.h"

namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;

inline std::string get_resource(std::string& bucket_name, std::string& oid) {
  return fmt::format("{}{}{}", bucket_name, "/", oid);
}

class RemoteGetCB : public RGWHTTPStreamRWRequest::ReceiveCB {
public:
  bufferlist *in_bl;
  RemoteGetCB(bufferlist* _bl): in_bl(_bl) {}
  int handle_data(bufferlist& bl, bool *pause) override {
    this->in_bl->append(bl);
    return 0;
  }
};

class RemoteCachePut {
  public:
    struct RemoteCachePutOp {
      std::string bucket_name;
      std::string oid;
      uint64_t offset;
      uint64_t len;
      std::string version;
      rgw_user bucket_owner;
      std::string remote_addr;
      uint64_t obj_size;
    };
    RemoteCachePut(rgw::sal::Driver* driver, RemoteCachePutOp& op) : driver(driver), op(op) {}
    virtual ~RemoteCachePut() = default; 

    int init(CephContext* cct, const DoutPrefixProvider* dpp);
    int send_request(const DoutPrefixProvider* dpp, bufferlist& bl, optional_yield& y);
    int complete_request(const DoutPrefixProvider* dpp, optional_yield& y);
    int send_and_complete_request(const DoutPrefixProvider* dpp, bufferlist& bl, optional_yield& y);

  private:
    rgw::sal::Driver* driver;
    RemoteCachePutOp op;
    std::unique_ptr<RGWRESTStreamRWRequest> sender;
    bufferlist in_bl;
    std::unique_ptr<RemoteGetCB> cb;
};

class RemoteCachePutBatch {
private:
  size_t max_in_flight;
  rgw::sal::Driver* driver;
  CephContext* cct;

  struct PutResult {
    std::unique_ptr<RemoteCachePut> put_op;
    std::string key;
    int status = -EINPROGRESS;
    RemoteCachePut::RemoteCachePutOp op_info;
  };

  std::deque<PutResult> in_flight;
  std::vector<PutResult> completed;

public:
  RemoteCachePutBatch(rgw::sal::Driver* driver, CephContext* cct, size_t max)
    : max_in_flight(max), driver(driver), cct(cct) {}

  int send(const DoutPrefixProvider* dpp,
          optional_yield y,
          RemoteCachePut::RemoteCachePutOp& op,
          bufferlist& bl);
  int complete_next(const DoutPrefixProvider* dpp, optional_yield y);
  int finish_all(const DoutPrefixProvider* dpp, optional_yield y);

  const std::vector<PutResult>& get_results() const {
    return completed;
  }

  void clear_results() {
    completed.clear();
  }

  std::vector<RemoteCachePut::RemoteCachePutOp> get_failed_ops() const {
    std::vector<RemoteCachePut::RemoteCachePutOp> failed;
    for (const auto& r : completed) {
      if (r.status < 0) {
        failed.push_back(r.op_info);
      }
    }
    return failed;
  }

  bool has_errors() const {
    return std::any_of(completed.begin(), completed.end(),
                      [](const PutResult& r) { return r.status < 0; });
  }

  size_t get_in_flight_count() const { return in_flight.size(); }
  size_t get_completed_count() const { return completed.size(); }
};

} } // namespace rgw::d4n
