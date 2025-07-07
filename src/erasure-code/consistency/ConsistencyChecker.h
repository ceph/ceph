#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/program_options.hpp>
#include "librados/librados_asio.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include "Pool.h"
#include "ECReader.h"
#include "RadosCommands.h"
#include "ECEncoder.h"
#include "ECEncoderSwitch.h"

#define dout_context g_ceph_context

namespace ceph {
namespace consistency {
class ConsistencyCheckResult {
  private:
    std::string oid;
    std::string error_message;
    bool result;

  public:
    std::string get_oid() const { return oid; }
    std::string get_error_message() const { return error_message; }
    bool get_result() const { return result; }
    ConsistencyCheckResult(std::string oid,
                           std::string error_message,
                           bool result) :
      oid(oid),
      error_message(error_message),
      result(result) {}
};

class ConsistencyChecker {
  private:
    librados::Rados& rados;
    boost::asio::io_context& asio;
    ceph::consistency::ECReader reader;
    ceph::consistency::RadosCommands commands;
    ceph::consistency::Pool pool;
    ceph::consistency::ECEncoderSwitch encoder;
    std::vector<ConsistencyCheckResult> results;
    bool buffers_match(const bufferlist& b1, const bufferlist& b2);
    std::pair<bufferlist, bufferlist> split_data_and_parity(const std::string& oid,
                                                            const bufferlist& read,
                                                            int k, int m,
                                                            bool is_optimized);

  public:
    ConsistencyChecker(librados::Rados& rados,
                        boost::asio::io_context& asio,
                        const std::string& pool_name,
                        int stripe_unit);
    void queue_ec_read(Read read);
    bool check_object_consistency(const std::string& oid,
                                  const bufferlist& inbl);
    void print_results(std::ostream& out);
    void clear_results();
    bool single_read_and_check_consistency(const std::string& oid,
                                           int block_size,
                                           int offset,
                                           int length);
};
}
}